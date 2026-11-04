# Remote AI Agent Workflow

A secure, self-hosted remote agent control system. Run commands on a laptop
from a mobile phone or remote machine through an authenticated FastAPI API,
with optional ngrok tunneling, LLM chat (Ollama / OpenAI / OpenRouter), and
full command history.

## Features

- **REST API + WebSocket** command execution with JWT authentication
- **Sandboxed commands** with blacklist (rm -rf, shutdown, reboot, etc.)
- **Output size caps** to prevent runaway output
- **ngrok tunnel** management — expose localhost to the internet securely
- **LLM chat** — Ollama (local), OpenAI, OpenRouter
- **Command history** persisted in SQLite
- **Web UI** — `frontend.html` for browser-based control
- **CLI client** — `client.py` for terminal-driven operation

## Architecture

```
Mobile / Remote Client
        │ HTTPS/WSS
        ▼
┌─────────────────────────────┐
│   ngrok tunnel (optional)   │
└─────────────────────────────┘
        │ localhost:8080
┌─────────────────────────────┐
│   FastAPI server (server.py) │
│   ├── JWT auth              │
│   ├── Command executor      │
│   ├── LLM clients           │
│   └── SQLite history        │
└─────────────────────────────┘
```

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the server

```bash
python server.py
```

The API is available at `http://localhost:8080`.

### 3. Get a token

```bash
python client.py token
```

### 4. Run a command

```bash
python client.py run "ls -la"
```

### 5. Chat with the LLM

```bash
python client.py chat "Explain how to reverse a linked list in Python."
```

### 6. Start a public tunnel (ngrok)

```bash
python client.py tunnel start
# then share the returned public URL
```

### 7. Use the Web UI

Open `frontend.html` in a browser, get a token, paste it into the UI, and
interact with the agent through a graphical interface.

## Configuration

Edit `config.ini` to change:

- Server host/port
- JWT secret and expiration
- Command blacklist
- ngrok auth token and region
- LLM provider (ollama / openai / openrouter) and model

## Security

- All mutating endpoints require a JWT bearer token.
- Commands matching the blacklist (e.g. `rm -rf /`, `shutdown`, `reboot`) are rejected before execution.
- Output is capped (default 1024 KB) to prevent memory exhaustion.
- Never commit `config.ini` with real API keys or JWT secrets.

## License

MIT © 2026 Ashish Jadhav
