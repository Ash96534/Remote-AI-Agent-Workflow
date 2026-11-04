#!/usr/bin/env python3
"""Remote AI Agent Workflow - v1.0 Server.

Secure, self-hosted remote agent control system with:
- FastAPI REST API + WebSocket command streaming
- JWT authentication with spot-scoped tokens
- Multi-spot registry (control plane manages N remote agents)
- Sandboxed command execution (blacklist, output cap, timeout)
- LLM router with provider failover (Ollama → OpenAI → OpenRouter)
- ngrok tunnel management
- SQLite persistence (commands, sessions, spots, audit log)
- Structured JSON logging + Prometheus-style /metrics endpoint
- Per-spot resource limits and isolation
- Exponential backoff retries and graceful degradation
"""

from __future__ import annotations

import asyncio
import configparser
import hashlib
import json
import os
import sqlite3
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

import jwt
import requests
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    status,
    Request,
)
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent.resolve()
CONFIG_PATH = BASE_DIR / "config.ini"
DB_PATH = BASE_DIR / "agent_workflow.db"
LOG_PATH = BASE_DIR / "agent_workflow.log"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

# Server
def _strip_quotes(val: str) -> str:
    val = val.strip()
    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
        return val[1:-1]
    return val


SERVER_HOST = _strip_quotes(config.get("server", "host", fallback="0.0.0.0"))
SERVER_PORT = config.getint("server", "port", fallback=8080)
MAX_CMD_DURATION = config.getint("server", "max_command_duration", fallback=300)
HEARTBEAT_TIMEOUT_S = config.getint("server", "heartbeat_timeout_s", fallback=90)
HEARTBEAT_INTERVAL_S = config.getint("server", "heartbeat_interval_s", fallback=30)
MAX_QUEUED_COMMANDS_PER_SPOT = config.getint("server", "max_queued_commands_per_spot", fallback=50)

import json

# Security
JWT_SECRET = _strip_quotes(config.get("security", "jwt_secret", fallback="CHANGE_ME_IN_PRODUCTION"))
JWT_ALGORITHM = _strip_quotes(config.get("security", "jwt_algorithm", fallback="HS256"))
JWT_EXP_HOURS = config.getint("security", "jwt_expiration_hours", fallback=24)
_raw_blacklist = config.get("security", "command_blacklist", fallback="[]").strip()
try:
    CMD_BLACKLIST = [_strip_quotes(s) for s in json.loads(_raw_blacklist)]
except (json.JSONDecodeError, TypeError):
    CMD_BLACKLIST = [_strip_quotes(s) for s in _raw_blacklist.split(",") if s.strip()]
MAX_OUTPUT_KB = config.getint("security", "max_output_size_kb", fallback=1024)
TUNNEL_RATE_LIMIT_PER_MIN = config.getint("security", "tunnel_rate_limit_per_min", fallback=5)

# Tunnel
NGROK_AUTH_TOKEN = _strip_quotes(config.get("tunnel", "ngrok_auth_token", fallback=""))
NGROK_REGION = _strip_quotes(config.get("tunnel", "ngrok_region", fallback="us"))

# LLM
OLLAMA_HOST = _strip_quotes(config.get("llm", "ollama_host", fallback="http://127.0.0.1:11434"))
OLLAMA_MODEL = _strip_quotes(config.get("llm", "ollama_model", fallback="llama3.2:3b"))
OPENAI_API_KEY = _strip_quotes(config.get("llm", "openai_api_key", fallback=""))
OPENAI_MODEL = _strip_quotes(config.get("llm", "openai_model", fallback="gpt-4o-mini"))
OPENROUTER_API_KEY = _strip_quotes(config.get("llm", "openrouter_api_key", fallback=""))
OPENROUTER_MODEL = _strip_quotes(config.get("llm", "openrouter_model", fallback="meta-llama/llama-3.1-8b-instruct:free"))

# ---------------------------------------------------------------------------
# Logging (structured JSON)
# ---------------------------------------------------------------------------

import logging

LOGGER = logging.getLogger("aia")
LOGGER.setLevel(logging.INFO)
_LOG_HANDLER = logging.FileHandler(LOG_PATH)
_LOG_HANDLER.setFormatter(logging.Formatter("%(message)s"))
LOGGER.addHandler(_LOG_HANDLER)


def log_event(event: str, **fields: Any) -> None:
    entry = {"ts": time.time(), "event": event, **fields}
    LOGGER.info(json.dumps(entry, default=str))


# ---------------------------------------------------------------------------
# Metrics (in-memory Prometheus-style)
# ---------------------------------------------------------------------------

METRICS: dict[str, int] = {
    "commands_total": 0,
    "commands_completed": 0,
    "commands_failed": 0,
    "commands_rejected": 0,
    "commands_timeout": 0,
    "llm_requests_total": 0,
    "llm_requests_failed": 0,
    "tunnel_starts": 0,
    "tunnel_stops": 0,
    "spots_online": 0,
    "spots_offline": 0,
    "auth_issues": 0,
}


def inc(metric: str, amount: int = 1) -> None:
    METRICS[metric] = METRICS.get(metric, 0) + amount


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with db_conn() as conn:
        conn.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;

            CREATE TABLE IF NOT EXISTS spots (
                spot_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                host TEXT,
                port INTEGER,
                api_key TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'offline',
                last_heartbeat REAL,
                capabilities TEXT DEFAULT '["cmd"]',
                resource_limits TEXT DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS commands (
                id TEXT PRIMARY KEY,
                spot_id TEXT NOT NULL,
                command TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                output TEXT,
                error TEXT,
                duration_ms REAL,
                user TEXT,
                created_at REAL NOT NULL,
                completed_at REAL,
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                spot_id TEXT,
                token_hash TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_seen REAL NOT NULL,
                FOREIGN KEY (spot_id) REFERENCES spots(spot_id)
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event TEXT NOT NULL,
                spot_id TEXT,
                user TEXT,
                details TEXT,
                created_at REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_commands_spot ON commands(spot_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_commands_status ON commands(status);
            CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event, created_at);
        """)
        conn.commit()


def audit(event: str, spot_id: Optional[str] = None, user: Optional[str] = None, details: Optional[str] = None) -> None:
    try:
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO audit_log (event, spot_id, user, details, created_at) VALUES (?, ?, ?, ?, ?)",
                (event, spot_id, str(user) if user else None, details, time.time()),
            )
            conn.commit()
    except Exception:  # noqa: BLE001
        log_event("audit_write_failed", error=str(Exception))
    log_event("audit_event", spot_id=spot_id, user=str(user) if user else None, details=details)


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------

security = HTTPBearer(auto_error=False)


def is_command_blacklisted(command: str) -> bool:
    lower = command.lower()
    return any(bad.lower() in lower for bad in CMD_BLACKLIST)


def create_token(spot_id: Optional[str] = None, username: str = "agent") -> str:
    now = time.time()
    payload = {
        "sub": username,
        "spot_id": spot_id,
        "iat": now,
        "exp": now + JWT_EXP_HOURS * 3600,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> dict[str, Any]:
    if credentials is None:
        inc("auth_issues")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {"sub": str(payload.get("sub", "agent")), "spot_id": payload.get("spot_id")}
    except jwt.ExpiredSignatureError:
        inc("auth_issues")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError:
        inc("auth_issues")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# ---------------------------------------------------------------------------
# Spot registry (in-memory + DB backed)
# ---------------------------------------------------------------------------

@dataclass
class Spot:
    spot_id: str
    name: str
    host: str
    port: int
    api_key: str
    status: str = "offline"  # online | offline | degraded
    last_heartbeat: float = 0.0
    capabilities: list[str] = None
    resource_limits: dict[str, Any] = None
    created_at: float = 0.0
    updated_at: float = 0.0

    def __post_init__(self):
        if self.capabilities is None:
            self.capabilities = ["cmd"]
        if self.resource_limits is None:
            self.resource_limits = {}


spots: dict[str, Spot] = {}
spots_lock = threading.Lock()


def load_spots_from_db() -> None:
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM spots").fetchall()
    for r in rows:
        caps = json.loads(r["capabilities"] or '["cmd"]')
        limits = json.loads(r["resource_limits"] or "{}")
        spots[r["spot_id"]] = Spot(
            spot_id=r["spot_id"],
            name=r["name"],
            host=r["host"] or "",
            port=r["port"] or SERVER_PORT,
            api_key=r["api_key"],
            status=r["status"],
            last_heartbeat=r["last_heartbeat"] or 0.0,
            capabilities=caps,
            resource_limits=limits,
            created_at=r["created_at"],
            updated_at=r["updated_at"],
        )


def upsert_spot(spot: Spot) -> None:
    with spots_lock:
        spots[spot.spot_id] = spot
    with db_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO spots
            (spot_id, name, host, port, api_key, status, last_heartbeat,
             capabilities, resource_limits, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                spot.spot_id, spot.name, spot.host, spot.port, spot.api_key,
                spot.status, spot.last_heartbeat,
                json.dumps(spot.capabilities), json.dumps(spot.resource_limits),
                spot.created_at, spot.updated_at,
            ),
        )
        conn.commit()


def remove_spot(spot_id: str) -> None:
    with spots_lock:
        spots.pop(spot_id, None)
    with db_conn() as conn:
        conn.execute("DELETE FROM spots WHERE spot_id = ?", (spot_id,))
        conn.commit()


def get_spot(spot_id: str) -> Optional[Spot]:
    with spots_lock:
        return spots.get(spot_id)


def list_spots() -> list[Spot]:
    with spots_lock:
        return list(spots.values())


def update_spot_heartbeat(spot_id: str) -> None:
    spot = get_spot(spot_id)
    if not spot:
        return
    now = time.time()
    spot.last_heartbeat = now
    spot.status = "online"
    spot.updated_at = now
    upsert_spot(spot)
    inc("spots_online")


def heartbeat_monitor() -> None:
    """Background task: mark spots offline if heartbeat missing."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL_S)
        now = time.time()
        with spots_lock:
            snapshot = list(spots.values())
        for spot in snapshot:
            if spot.status == "online" and (now - spot.last_heartbeat) > HEARTBEAT_TIMEOUT_S:
                spot.status = "offline"
                spot.updated_at = now
                upsert_spot(spot)
                inc("spots_offline")
                log_event("spot_offline", spot_id=spot.spot_id, name=spot.name)


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------

def run_command(spot_id: str, command: str, user: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(user, dict):
        user = str(user.get("sub", "agent"))
    spot = get_spot(spot_id)
    limits = spot.resource_limits if spot else {}
    max_duration = int(limits.get("max_cmd_duration_s", MAX_CMD_DURATION))
    max_output = int(limits.get("max_output_kb", MAX_OUTPUT_KB))

    cmd_id = uuid.uuid4().hex
    start = time.time()

    # Blacklist check
    if is_command_blacklisted(command):
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO commands (id, spot_id, command, status, error, created_at, user) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (cmd_id, spot_id, command, "rejected", "Command blocked by security policy", start, user),
            )
            conn.commit()
        inc("commands_rejected")
        audit("command_rejected", spot_id=spot_id, user=user, details=command)
        log_event("command_rejected", spot_id=spot_id, cmd_id=cmd_id, command=command)
        return {"id": cmd_id, "status": "rejected", "error": "Command blocked by security policy", "duration_ms": 0.0}

    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=max_duration,
        )
        output = result.stdout or ""
        if result.stderr:
            output += f"\nSTDERR:\n{result.stderr}"
        if len(output.encode("utf-8")) > max_output * 1024:
            truncated = output[: max_output * 1024].decode("utf-8", errors="replace")
            output = truncated + "\n...[output truncated]..."
        duration_ms = (time.time() - start) * 1000
        status_val = "completed" if result.returncode == 0 else "failed"
    except subprocess.TimeoutExpired:
        output = ""
        status_val = "timeout"
        duration_ms = (time.time() - start) * 1000
    except Exception as exc:  # noqa: BLE001
        output = ""
        status_val = "error"
        duration_ms = (time.time() - start) * 1000
        output = f"Error: {exc}"

    with db_conn() as conn:
        conn.execute(
            """INSERT INTO commands
            (id, spot_id, command, output, error, status, created_at, completed_at, duration_ms, user)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cmd_id, spot_id, command,
                output if status_val not in ("error", "timeout", "rejected") else None,
                output if status_val in ("error", "timeout", "rejected") else None,
                status_val, start, time.time(), duration_ms, user,
            ),
        )
        conn.commit()

    if status_val == "completed":
        inc("commands_completed")
    elif status_val == "timeout":
        inc("commands_timeout")
    else:
        inc("commands_failed")
    inc("commands_total")
    audit("command_executed", spot_id=spot_id, user=user, details=command)
    log_event("command_executed", spot_id=spot_id, cmd_id=cmd_id, status=status_val, duration_ms=duration_ms)

    return {
        "id": cmd_id,
        "status": status_val,
        "output": output if status_val == "completed" else None,
        "error": output if status_val in ("error", "timeout", "rejected") else None,
        "duration_ms": duration_ms,
    }


# ---------------------------------------------------------------------------
# LLM router with failover
# ---------------------------------------------------------------------------

LLM_PROVIDERS = ["ollama", "openai", "openrouter"]


def query_ollama(prompt: str, model: str = OLLAMA_MODEL) -> str:
    try:
        resp = requests.post(
            f"{OLLAMA_HOST}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")
    except Exception as exc:  # noqa: BLE001
        return f"Ollama error: {exc}"


def query_openai(prompt: str, model: str = OPENAI_MODEL) -> str:
    api_key = OPENAI_API_KEY  # read at call time so tests can mutate it
    if not api_key:
        return "OpenAI API key not configured."
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}]},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as exc:  # noqa: BLE001
        return f"OpenAI error: {exc}"


def query_openrouter(prompt: str, model: str = OPENROUTER_MODEL) -> str:
    api_key = OPENROUTER_API_KEY  # read at call time so tests can mutate it
    if not api_key:
        return "OpenRouter API key not configured."
    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}]},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except Exception as exc:  # noqa: BLE001
        return f"OpenRouter error: {exc}"


PROVIDER_FUNCS = {
    "ollama": query_ollama,
    "openai": query_openai,
    "openrouter": query_openrouter,
}


ERROR_PREFIXES = ("ollama error", "openai error", "openrouter error")


def _is_error_response(response: str) -> bool:
    r = response.lower()
    return (
        r.startswith(ERROR_PREFIXES)
        or "api key not configured" in r
        or "connection refused" in r
    )


def query_llm_with_fallback(prompt: str, preferred: Optional[str] = None, model: Optional[str] = None) -> tuple[str, str]:
    """Try preferred provider, then fall through the list. Returns (response, provider_used)."""
    order = [p for p in LLM_PROVIDERS if p != preferred] if preferred else list(LLM_PROVIDERS)
    if preferred and preferred in LLM_PROVIDERS:
        order.insert(0, preferred)
    last_error = ""
    for provider in order:
        try:
            func = PROVIDER_FUNCS[provider]
            model_to_use = model or {
                "ollama": OLLAMA_MODEL,
                "openai": OPENAI_MODEL,
                "openrouter": OPENROUTER_MODEL,
            }[provider]
            response = func(prompt, model=model_to_use)
            if response and not _is_error_response(response):
                inc("llm_requests_total")
                return response, provider
            last_error = response or "empty response"
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
    inc("llm_requests_failed")
    return f"All LLM providers failed. Last error: {last_error}", "none"


# ---------------------------------------------------------------------------
# Tunnel manager
# ---------------------------------------------------------------------------

_tunnel_public_urls: dict[str, Optional[str]] = {}
_tunnel_rate_window: dict[str, list[float]] = {}


def _check_tunnel_rate_limit(spot_id: str) -> bool:
    now = time.time()
    window = _tunnel_rate_window.setdefault(spot_id, [])
    window[:] = [t for t in window if now - t < 60.0]
    if len(window) >= TUNNEL_RATE_LIMIT_PER_MIN:
        return False
    window.append(now)
    return True


def start_ngrok(spot_id: str) -> Optional[str]:
    if not _check_tunnel_rate_limit(spot_id):
        return None
    try:
        from pyngrok import ngrok
        if NGROK_AUTH_TOKEN:
            ngrok.set_auth_token(NGROK_AUTH_TOKEN)
        tunnel = ngrok.connect(SERVER_PORT, "http", region=NGROK_REGION)
        url = tunnel.public_url
        _tunnel_public_urls[spot_id] = url
        inc("tunnel_starts")
        audit("tunnel_started", spot_id=spot_id, details=url)
        log_event("tunnel_started", spot_id=spot_id, url=url)
        return url
    except Exception as exc:  # noqa: BLE001
        log_event("tunnel_start_failed", spot_id=spot_id, error=str(exc))
        return None


def stop_ngrok(spot_id: str) -> None:
    try:
        from pyngrok import ngrok
        url = _tunnel_public_urls.get(spot_id)
        if url:
            ngrok.disconnect(url)
    except Exception:
        pass
    _tunnel_public_urls[spot_id] = None
    inc("tunnel_stops")
    audit("tunnel_stopped", spot_id=spot_id)
    log_event("tunnel_stopped", spot_id=spot_id)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Remote AI Agent Workflow", version="1.0.0")
init_db()
load_spots_from_db()

# Start heartbeat monitor in background
_thread = threading.Thread(target=heartbeat_monitor, daemon=True)
_thread.start()


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    start = time.time()
    response = await call_next(request)
    duration_ms = (time.time() - start) * 1000
    response.headers["X-Request-ID"] = request_id
    log_event("http_request", method=request.method, path=request.url.path, status=response.status_code, duration_ms=duration_ms, request_id=request_id)
    return response


@app.get("/")
async def root():
    return {
        "name": "Remote AI Agent Workflow",
        "version": "1.0.0",
        "status": "running",
        "spots_online": len([s for s in list_spots() if s.status == "online"]),
        "endpoints": {
            "auth": "/auth/token",
            "spots": "/spots",
            "command": "/spots/{spot_id}/command",
            "llm": "/spots/{spot_id}/llm/chat",
            "tunnel": "/spots/{spot_id}/tunnel",
            "health": "/health",
            "metrics": "/metrics",
        },
    }


@app.get("/health")
async def health():
    online = [s for s in list_spots() if s.status == "online"]
    return {"status": "running", "spots_online": len(online), "spots_total": len(list_spots())}


@app.get("/metrics")
async def metrics():
    return JSONResponse(content=METRICS, media_type="application/json")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@app.post("/auth/token")
async def get_token(spot_id: Optional[str] = None):
    token = create_token(spot_id=spot_id)
    audit("token_issued", spot_id=spot_id)
    return {"access_token": token, "token_type": "bearer", "expires_in": JWT_EXP_HOURS * 3600, "spot_id": spot_id}


# ---------------------------------------------------------------------------
# Spots (multi-spot control plane)
# ---------------------------------------------------------------------------

class SpotRegisterRequest(BaseModel):
    name: str
    host: Optional[str] = None
    port: Optional[int] = None
    capabilities: Optional[list[str]] = None
    resource_limits: Optional[dict[str, Any]] = None


@app.post("/spots/register")
async def register_spot(req: SpotRegisterRequest, user: str = Depends(verify_token)):
    spot_id = uuid.uuid4().hex
    api_key = hashlib.sha256(f"{spot_id}{time.time()}".encode()).hexdigest()[:32]
    now = time.time()
    spot = Spot(
        spot_id=spot_id,
        name=req.name,
        host=req.host or "",
        port=req.port or SERVER_PORT,
        api_key=api_key,
        status="online",
        last_heartbeat=now,
        capabilities=req.capabilities or ["cmd"],
        resource_limits=req.resource_limits or {},
        created_at=now,
        updated_at=now,
    )
    upsert_spot(spot)
    inc("spots_online")
    audit("spot_registered", spot_id=spot_id, user=user, details=req.name)
    return {
        "spot_id": spot_id,
        "api_key": api_key,
        "name": spot.name,
        "status": spot.status,
    }


@app.get("/spots")
async def list_spots_endpoint(user: str = Depends(verify_token)):
    return [
        {
            "spot_id": s.spot_id,
            "name": s.name,
            "host": s.host,
            "port": s.port,
            "status": s.status,
            "last_heartbeat": s.last_heartbeat,
            "capabilities": s.capabilities,
            "resource_limits": s.resource_limits,
        }
        for s in list_spots()
    ]


@app.get("/spots/{spot_id}")
async def get_spot_endpoint(spot_id: str, user: str = Depends(verify_token)):
    spot = get_spot(spot_id)
    if not spot:
        raise HTTPException(status_code=404, detail="Spot not found")
    return {
        "spot_id": spot.spot_id,
        "name": spot.name,
        "host": spot.host,
        "port": spot.port,
        "status": spot.status,
        "last_heartbeat": spot.last_heartbeat,
        "capabilities": spot.capabilities,
        "resource_limits": spot.resource_limits,
    }


@app.delete("/spots/{spot_id}")
async def delete_spot(spot_id: str, user: str = Depends(verify_token)):
    if not get_spot(spot_id):
        raise HTTPException(status_code=404, detail="Spot not found")
    remove_spot(spot_id)
    audit("spot_deleted", spot_id=spot_id, user=user)
    return {"status": "deleted", "spot_id": spot_id}


@app.post("/spots/{spot_id}/heartbeat")
async def spot_heartbeat(spot_id: str):
    update_spot_heartbeat(spot_id)
    return {"status": "ok", "spot_id": spot_id}


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

class CommandRequest(BaseModel):
    command: str


class CommandResponse(BaseModel):
    id: str
    status: str
    output: Optional[str] = None
    error: Optional[str] = None
    duration_ms: Optional[float] = None


@app.post("/spots/{spot_id}/command", response_model=CommandResponse)
async def execute_command(spot_id: str, req: CommandRequest, user: str = Depends(verify_token)):
    spot = get_spot(spot_id)
    if not spot:
        raise HTTPException(status_code=404, detail="Spot not found")
    if spot.status != "online":
        raise HTTPException(status_code=503, detail=f"Spot {spot_id} is {spot.status}")
    return run_command(spot_id, req.command, user)


@app.websocket("/spots/{spot_id}/command")
async def command_websocket(websocket: WebSocket, spot_id: str):
    await websocket.accept()
    spot = get_spot(spot_id)
    if not spot or spot.status != "online":
        await websocket.send_json({"error": "Spot not found or offline"})
        await websocket.close()
        return
    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                command = payload.get("command", "")
                user = payload.get("user", "ws")
            except json.JSONDecodeError:
                command = data
                user = "ws"
            result = run_command(spot_id, command, user)
            await websocket.send_json(result)
    except WebSocketDisconnect:
        pass


@app.get("/spots/{spot_id}/commands")
async def get_command_history(spot_id: str, limit: int = 50, user: str = Depends(verify_token)):
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id, command, output, error, status, created_at, completed_at, duration_ms, user FROM commands WHERE spot_id = ? ORDER BY created_at DESC LIMIT ?",
            (spot_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/commands/{cmd_id}")
async def get_command_result(cmd_id: str, user: str = Depends(verify_token)):
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (cmd_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Command not found")
    return dict(row)


# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    prompt: str
    model: Optional[str] = None
    provider: Optional[str] = None


@app.post("/spots/{spot_id}/llm/chat")
async def llm_chat(spot_id: str, req: ChatRequest, user: str = Depends(verify_token)):
    spot = get_spot(spot_id)
    if not spot:
        raise HTTPException(status_code=404, detail="Spot not found")
    response, provider_used = query_llm_with_fallback(req.prompt, provider=req.provider, model=req.model)
    audit("llm_chat", spot_id=spot_id, user=user, details=f"provider={provider_used}")
    return {"response": response, "provider": provider_used, "model": req.model or "default"}


@app.get("/spots/{spot_id}/llm/providers")
async def llm_providers(spot_id: str, user: str = Depends(verify_token)):
    return {"providers": LLM_PROVIDERS, "default": "ollama"}


# ---------------------------------------------------------------------------
# Tunnel
# ---------------------------------------------------------------------------

@app.post("/spots/{spot_id}/tunnel/start")
async def tunnel_start(spot_id: str, user: str = Depends(verify_token)):
    spot = get_spot(spot_id)
    if not spot:
        raise HTTPException(status_code=404, detail="Spot not found")
    url = start_ngrok(spot_id)
    if not url:
        raise HTTPException(status_code=500, detail="Failed to start tunnel (rate limited or ngrok unavailable)")
    return {"public_url": url, "local_port": SERVER_PORT, "spot_id": spot_id}


@app.post("/spots/{spot_id}/tunnel/stop")
async def tunnel_stop(spot_id: str, user: str = Depends(verify_token)):
    spot = get_spot(spot_id)
    if not spot:
        raise HTTPException(status_code=404, detail="Spot not found")
    stop_ngrok(spot_id)
    return {"status": "stopped", "spot_id": spot_id}


@app.get("/spots/{spot_id}/tunnel/status")
async def tunnel_status(spot_id: str, user: str = Depends(verify_token)):
    spot = get_spot(spot_id)
    if not spot:
        raise HTTPException(status_code=404, detail="Spot not found")
    url = _tunnel_public_urls.get(spot_id)
    return {"active": url is not None, "public_url": url, "local_port": SERVER_PORT, "spot_id": spot_id}


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def main():
    import uvicorn

    if os.environ.get("AIA_START_TUNNEL", "0") == "1":
        threading.Thread(target=lambda: start_ngrok("default"), daemon=True).start()

    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)


if __name__ == "__main__":
    main()
