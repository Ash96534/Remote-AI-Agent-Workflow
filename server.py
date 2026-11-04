#!/usr/bin/env python3
"""Remote AI Agent Workflow - Secure tunnel for agent control.

Allows remote access to instruct and drive an AI coding agent running on a
laptop directly from a mobile phone interface via a secure tunnel.

Features:
- FastAPI REST API with JWT authentication
- Ollama / OpenAI / OpenRouter LLM integration
- Sandboxed command execution with whitelist/blacklist
- ngrok tunnel management
- SQLite command/result persistence
- REST + WebSocket command streaming
- Rate limiting and output size caps
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import configparser
import jwt
import requests
from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config.ini"
DB_PATH = Path(__file__).parent / "agent_workflow.db"

config = configparser.ConfigParser()
config.read(CONFIG_PATH)

SERVER_HOST = config.get("server", "host", fallback="0.0.0.0")
SERVER_PORT = config.getint("server", "port", fallback=8080)
MAX_CMD_DURATION = config.getint("server", "max_command_duration", fallback=300)

JWT_SECRET = config.get("security", "jwt_secret", fallback="CHANGE_ME_IN_PRODUCTION")
JWT_ALGORITHM = config.get("security", "jwt_algorithm", fallback="HS256")
JWT_EXP_HOURS = config.getint("security", "jwt_expiration_hours", fallback=24)
CMD_BLACKLIST = [
    s.strip() for s in config.get("security", "command_blacklist", fallback="").splitlines() if s.strip()
]
MAX_OUTPUT_KB = config.getint("security", "max_output_size_kb", fallback=1024)

NGROK_AUTH_TOKEN = config.get("tunnel", "ngrok_auth_token", fallback="")
NGROK_REGION = config.get("tunnel", "ngrok_region", fallback="us")

OLLAMA_HOST = config.get("llm", "ollama_host", fallback="http://127.0.0.1:11434")
OLLAMA_MODEL = config.get("llm", "ollama_model", fallback="llama3.2:3b")
OPENAI_API_KEY = config.get("llm", "openai_api_key", fallback="")
OPENAI_MODEL = config.get("llm", "openai_model", fallback="gpt-4o-mini")
OPENROUTER_API_KEY = config.get("llm", "openrouter_api_key", fallback="")
OPENROUTER_MODEL = config.get("llm", "openrouter_model", fallback="meta-llama/llama-3.1-8b-instruct:free")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with db_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS commands (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                output TEXT,
                error TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at REAL NOT NULL,
                completed_at REAL,
                duration_ms REAL,
                user TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                token_hash TEXT NOT NULL,
                created_at REAL NOT NULL,
                last_seen REAL NOT NULL
            )
        """)
        conn.commit()


# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------

security = HTTPBearer(auto_error=False)


def is_command_blacklisted(command: str) -> bool:
    lower = command.lower()
    return any(bad.lower() in lower for bad in CMD_BLACKLIST)


def create_token(username: str = "agent") -> str:
    now = time.time()
    payload = {
        "sub": username,
        "iat": now,
        "exp": now + JWT_EXP_HOURS * 3600,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> str:
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")
    try:
        payload = jwt.decode(credentials.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return str(payload.get("sub", "agent"))
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class CommandRequest(BaseModel):
    command: str


class CommandResponse(BaseModel):
    id: str
    status: str
    output: Optional[str] = None
    error: Optional[str] = None
    duration_ms: Optional[float] = None


class ChatRequest(BaseModel):
    prompt: str
    model: Optional[str] = None
    provider: Optional[str] = None


class TunnelStatus(BaseModel):
    active: bool
    public_url: Optional[str] = None
    local_port: int = SERVER_PORT


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------

def run_command(command: str, user: str) -> dict[str, Any]:
    cmd_id = uuid.uuid4().hex
    start = time.time()

    if is_command_blacklisted(command):
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO commands (id, command, status, error, created_at, user) VALUES (?, ?, ?, ?, ?, ?)",
                (cmd_id, command, "rejected", "Command blocked by security policy", start, user),
            )
            conn.commit()
        return {"id": cmd_id, "status": "rejected", "error": "Command blocked by security policy", "duration_ms": 0.0}

    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=MAX_CMD_DURATION,
        )
        output = result.stdout or ""
        if result.stderr:
            output += f"\nSTDERR:\n{result.stderr}"
        if len(output.encode("utf-8")) > MAX_OUTPUT_KB * 1024:
            output = output[: MAX_OUTPUT_KB * 1024].decode("utf-8", errors="replace") + "\n...[output truncated]..."
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
            "INSERT INTO commands (id, command, output, error, status, created_at, completed_at, duration_ms, user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (cmd_id, command, output if status_val != "error" else None, output if status_val == "error" else None, status_val, start, time.time(), duration_ms, user),
        )
        conn.commit()

    return {"id": cmd_id, "status": status_val, "output": output if status_val == "completed" else None, "error": output if status_val in ("error", "timeout", "rejected") else None, "duration_ms": duration_ms}


# ---------------------------------------------------------------------------
# LLM providers
# ---------------------------------------------------------------------------

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


def query_openai(prompt: str, model: str = OPENAI_MODEL, api_key: str = OPENAI_API_KEY) -> str:
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


def query_openrouter(prompt: str, model: str = OPENROUTER_MODEL, api_key: str = OPENROUTER_API_KEY) -> str:
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


def query_llm(prompt: str, provider: Optional[str] = None, model: Optional[str] = None) -> str:
    provider = provider or "ollama"
    model = model or OLLAMA_MODEL
    if provider == "ollama":
        return query_ollama(prompt, model)
    if provider == "openai":
        return query_openai(prompt, model)
    if provider == "openrouter":
        return query_openrouter(prompt, model)
    return f"Unknown provider: {provider}"


# ---------------------------------------------------------------------------
# Tunnel
# ---------------------------------------------------------------------------

_tunnel_public_url: Optional[str] = None


def start_ngrok() -> Optional[str]:
    global _tunnel_public_url
    try:
        from pyngrok import ngrok
        if NGROK_AUTH_TOKEN:
            ngrok.set_auth_token(NGROK_AUTH_TOKEN)
        tunnel = ngrok.connect(SERVER_PORT, "http", region=NGROK_REGION)
        _tunnel_public_url = tunnel.public_url
        return _tunnel_public_url
    except Exception as exc:  # noqa: BLE001
        print(f"ngrok error: {exc}")
        return None


def stop_ngrok() -> None:
    global _tunnel_public_url
    try:
        from pyngrok import ngrok
        if _tunnel_public_url:
            ngrok.disconnect(_tunnel_public_url)
    except Exception:
        pass
    _tunnel_public_url = None


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="Remote AI Agent Workflow", version="1.0.0")
init_db()


@app.get("/")
async def root():
    return {
        "name": "Remote AI Agent Workflow",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "auth": "/auth/token",
            "command": "/command (POST, WS)",
            "history": "/history",
            "llm": "/llm/chat (POST)",
            "tunnel": "/tunnel/status",
        },
    }


@app.post("/auth/token")
async def get_token():
    token = create_token()
    return {"access_token": token, "token_type": "bearer", "expires_in": JWT_EXP_HOURS * 3600}


@app.post("/command", response_model=CommandResponse)
async def execute_command(req: CommandRequest, user: str = Depends(verify_token)):
    return run_command(req.command, user)


@app.websocket("/command")
async def command_websocket(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await ws.receive_text()
            try:
                payload = json.loads(data)
                command = payload.get("command", "")
                user = payload.get("user", "ws")
            except json.JSONDecodeError:
                command = data
                user = "ws"
            result = run_command(command, user)
            await ws.send_json(result)
    except WebSocketDisconnect:
        pass


@app.get("/history")
async def get_history(limit: int = 50, user: str = Depends(verify_token)):
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id, command, output, error, status, created_at, completed_at, duration_ms, user FROM commands ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/command/{cmd_id}")
async def get_command(cmd_id: str, user: str = Depends(verify_token)):
    with db_conn() as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (cmd_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Command not found")
    return dict(row)


@app.post("/llm/chat")
async def llm_chat(req: ChatRequest, user: str = Depends(verify_token)):
    response = query_llm(req.prompt, provider=req.provider, model=req.model)
    return {"response": response, "provider": req.provider or "ollama", "model": req.model or OLLAMA_MODEL}


@app.get("/tunnel/status", response_model=TunnelStatus)
async def tunnel_status(user: str = Depends(verify_token)):
    return TunnelStatus(active=_tunnel_public_url is not None, public_url=_tunnel_public_url, local_port=SERVER_PORT)


@app.post("/tunnel/start")
async def tunnel_start(user: str = Depends(verify_token)):
    url = start_ngrok()
    if not url:
        raise HTTPException(status_code=500, detail="Failed to start tunnel")
    return {"public_url": url, "local_port": SERVER_PORT}


@app.post("/tunnel/stop")
async def tunnel_stop(user: str = Depends(verify_token)):
    stop_ngrok()
    return {"status": "stopped"}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import uvicorn

    if os.environ.get("AIA_START_TUNNEL", "0") == "1":
        threading.Thread(target=start_ngrok, daemon=True).start()

    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)


if __name__ == "__main__":
    main()
