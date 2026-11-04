# Remote AI Agent Workflow — Product & Architecture Specification

## 1. Overview

**Name:** Remote AI Agent Workflow  
**Type:** Self-hosted remote agent control system  
**Purpose:** Execute shell commands, query LLMs, and manage ngrok tunnels on a remote laptop from any client (mobile, laptop, web) via an authenticated FastAPI REST API + WebSocket, with a browser UI and CLI client.

## 2. Goals (MVP → v1.0)

| Area | Goal |
|------|------|
| Auth | JWT bearer auth, rotating secrets, per-session tokens |
| Command execution | Sandboxed subprocess with blacklist, output cap, timeout |
| LLM integration | Ollama (local), OpenAI, OpenRouter with provider failover |
| Tunnel management | ngrok start/stop/status with public URL display |
| Persistence | SQLite command history + session registry |
| Multi-spot | Support multiple remote agent instances (spots) from one control plane |
| Isolation | Per-spot process sandboxing, resource limits, network isolation |
| UI | Browser UI (Google Stitch-inspired) + CLI client |
| Reliability | Retries, exponential backoff, health checks, graceful degradation |
| Observability | Structured logging, metrics endpoint, request tracing |

## 3. Non-Goals (v1.0)

- No user registration / multi-tenancy beyond spots
- No file transfer (beyond command stdout)
- No persistent agent state beyond command history
- No end-to-end encryption of tunnel payload beyond TLS (ngrok handles)
- No Docker containerization (deferred)

## 4. Architecture

```
┌─────────────────┐     WSS/HTTPS     ┌──────────────────────────────┐
│  Control Client │◄──────────────────►│   ngrok (optional tunnel)    │
│  (browser/CLI)  │                    └──────────────┬───────────────┘
└─────────────────┘                                   │ localhost:8080
                                                      ▼
                                              ┌─────────────────┐
                                              │ FastAPI Server  │
                                              │  (server.py)    │
                                              │                 │
                                              ├─ Auth (JWT)     │
                                              ├─ Command Exec   │
                                              ├─ LLM Router     │
                                              ├─ Tunnel Mgr     │
                                              ├─ Spot Registry  │
                                              └─ SQLite History │
                                                      │
                                              ┌─────────────────┴─────────────────┐
                                              │         OS Sandbox Layer          │
                                              │  (per-spot subprocess limits)     │
                                              └───────────────────────────────────┘
```

### 4.1 Layers

| Layer | Responsibility |
|-------|---------------|
| Transport | FastAPI HTTP + WebSocket, CORS, rate limiting |
| Auth | JWT issue/verify, bearer enforcement, spot-scoped tokens |
| Spot Manager | Register, list, heartbeat, route commands to specific spot |
| Command Executor | Subprocess sandbox, blacklist, timeout, output cap, streaming |
| LLM Router | Provider abstraction (Ollama → OpenAI → OpenRouter failover) |
| Tunnel Manager | ngrok lifecycle, public URL tracking |
| Persistence | SQLite: commands, sessions, spots, audit log |
| UI | frontend.html (Stitch-inspired), CLI client (client.py) |

## 5. Data Model

### Spot
```json
{
  "spot_id": "uuid",
  "name": "laptop-home",
  "host": "192.168.1.42",
  "port": 8080,
  "api_key": "sk-...",
  "status": "online|offline|degraded",
  "last_heartbeat": "ISO8601",
  "capabilities": ["cmd","llm","tunnel"],
  "resource_limits": {"max_output_kb": 1024, "max_cmd_duration_s": 300},
  "created_at": "ISO8601"
}
```

### Command Record
```json
{
  "id": "uuid",
  "spot_id": "uuid",
  "command": "ls -la",
  "status": "queued|running|completed|failed|timeout|rejected",
  "output": "...",
  "error": "...",
  "duration_ms": 1234,
  "user": "agent",
  "created_at": "ISO8601",
  "completed_at": "ISO8601"
}
```

### LLM Session
```json
{
  "session_id": "uuid",
  "spot_id": "uuid",
  "provider": "ollama|openai|openrouter",
  "model": "llama3.2:3b",
  "messages": [{"role": "...", "content": "..."}],
  "created_at": "ISO8601"
}
```

## 6. API Surface

### Auth
- `POST /auth/token` — issue JWT
- `GET /auth/verify` — validate token

### Spots (Multi-spot control plane)
- `POST /spots/register` — register a new spot agent
- `GET /spots` — list all spots with status
- `GET /spots/{spot_id}` — get spot detail
- `DELETE /spots/{spot_id}` — deregister spot
- `POST /spots/{spot_id}/heartbeat` — spot reports alive

### Commands
- `POST /spots/{spot_id}/command` — execute command on spot
- `WS /spots/{spot_id}/command` — streaming command output
- `GET /spots/{spot_id}/commands` — history for spot
- `GET /commands/{cmd_id}` — single command result

### LLM
- `POST /spots/{spot_id}/llm/chat` — chat with LLM on spot
- `GET /spots/{spot_id}/llm/providers` — available providers

### Tunnel
- `POST /spots/{spot_id}/tunnel/start` — start ngrok
- `POST /spots/{spot_id}/tunnel/stop` — stop ngrok
- `GET /spots/{spot_id}/tunnel/status` — tunnel state

### System
- `GET /health` — server health
- `GET /metrics` — Prometheus-style metrics

## 7. Multi-Spot Support

The server acts as a **control plane** that can manage N remote spot agents. Each spot:
- Runs a lightweight spot agent (subset of server.py)
- Registers with control plane via heartbeat
- Exposes its own API; control plane proxies or routes to it
- Control plane maintains spot registry, routes commands by spot_id

### Spot Registration Flow
1. Spot agent starts, generates spot_id + api_key
2. Spot POSTs `/spots/register` to control plane with capabilities
3. Control plane stores spot, marks online
4. Spot sends heartbeat every 30s
5. Control plane marks offline after 90s silence

## 8. Isolation Systems

| Isolation | Mechanism |
|-----------|-----------|
| Process | `subprocess.run` with `timeout`, no shell=True where possible; shell=True only for user commands with strict validation |
| Resource | `max_output_kb`, `max_cmd_duration_s` per spot config |
| Command | Blacklist regex: `rm -rf /`, `mkfs`, `dd if=`, `> /dev/sda`, `shutdown`, `reboot`, `init 0`, `init 6`, `chmod -R /` |
| Network | Spot-to-control-plane mTLS optional; ngrok TLS for public tunnel |
| Auth | JWT per spot; tokens scoped to spot_id |

## 9. Fallbacks & Resilience

| Failure | Fallback |
|---------|----------|
| LLM provider down | Auto-failover: Ollama → OpenAI → OpenRouter → static canned response |
| ngrok unavailable | Return local `http://host:port` for LAN access; warn user |
| Spot offline | Queue commands (max 50 per spot), deliver on reconnect |
| DB locked | SQLite WAL mode, retry with exponential backoff (3 attempts) |
| Auth token expired | 401 → client re-auth via `/auth/token` |
| WebSocket disconnect | Auto-reconnect with last_event_id; resume stream |

## 10. Observability

- Structured JSON logs (`logging.Formatter` with `json.dumps`)
- `/metrics` endpoint: `commands_total`, `commands_failed`, `llm_requests_total`, `tunnel_starts`, `spots_online`
- Request ID middleware (X-Request-ID)
- Audit log: every command, auth event, tunnel action logged to SQLite + file

## 11. UI Requirements (Google Stitch-Inspired)

- Dark theme, glassmorphism cards
- Spot selector dropdown (multi-spot)
- Command terminal with streaming output
- AI chat panel with provider selector
- Tunnel status card with public URL copy button
- Real-time status pill (online/offline/degraded)
- Command history table
- Responsive grid layout
- Token management (get, paste, clear)

## 12. Verification Matrix

| Test | Command | Expected |
|------|---------|----------|
| Auth | `POST /auth/token` | Returns JWT |
| Auth | request without token | 401 |
| Command | `POST /command` `ls` | 200, output contains directory listing |
| Command blacklist | `rm -rf /` | 200, status=rejected |
| LLM Ollama | `POST /llm/chat` provider=ollama | non-empty response |
| LLM failover | Ollama down, OpenAI up | response from OpenAI |
| Tunnel start | `POST /tunnel/start` | returns public_url |
| Tunnel stop | `POST /tunnel/stop` | status=stopped |
| Spot register | `POST /spots/register` | returns spot_id |
| Spot list | `GET /spots` | array of spots |
| Health | `GET /health` | status=running |
| WS command | connect `/command` | receive streaming output |

## 13. Risks & Mitigation

| Risk | Mitigation |
|------|------------|
| Credential theft | JWT short expiry (24h), blacklist sensitive commands, no secret logging |
| Command injection | Blacklist + output cap + timeout |
| Resource exhaustion | Per-spot limits, max 50 queued commands |
| ngrok abuse | Rate limit tunnel start (5/min per spot) |
| Single point of failure | Spot agents can operate standalone; control plane is optional orchestrator |

## 14. Implementation Phases

| Phase | Deliverable |
|-------|-------------|
| 1 | Core server + auth + command exec + SQLite |
| 2 | Multi-spot registry + heartbeat + routing |
| 3 | LLM router + provider failover |
| 4 | Tunnel manager (ngrok) |
| 5 | WebSocket streaming commands |
| 6 | Stitch-inspired UI (frontend.html) |
| 7 | CLI client (client.py) parity |
| 8 | Tests (pytest), verification matrix, CI |
