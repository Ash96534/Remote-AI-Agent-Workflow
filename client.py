#!/usr/bin/env python3
"""Remote AI Agent Workflow client.

Command-line client for interacting with the Remote AI Agent Workflow server.
Supports command execution, LLM chat, tunnel management, and history retrieval.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests

DEFAULT_BASE = "http://localhost:8080"


class AIAClient:
    def __init__(self, base_url: str = DEFAULT_BASE, token: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def get_token(self) -> str:
        resp = requests.post(f"{self.base_url}/auth/token", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        self.token = data["access_token"]
        return self.token

    def run_command(self, command: str) -> dict:
        if not self.token:
            self.get_token()
        resp = requests.post(
            f"{self.base_url}/command",
            headers=self._headers(),
            json={"command": command},
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json()

    def chat(self, prompt: str, provider: str | None = None) -> str:
        if not self.token:
            self.get_token()
        resp = requests.post(
            f"{self.base_url}/llm/chat",
            headers=self._headers(),
            json={"prompt": prompt, "provider": provider},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("response", "")

    def tunnel_status(self) -> dict:
        if not self.token:
            self.get_token()
        resp = requests.get(f"{self.base_url}/tunnel/status", headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def tunnel_start(self) -> dict:
        if not self.token:
            self.get_token()
        resp = requests.post(f"{self.base_url}/tunnel/start", headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()

    def tunnel_stop(self) -> dict:
        if not self.token:
            self.get_token()
        resp = requests.post(f"{self.base_url}/tunnel/stop", headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()

    def history(self, limit: int = 50) -> list[dict]:
        if not self.token:
            self.get_token()
        resp = requests.get(f"{self.base_url}/history?limit={limit}", headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Remote AI Agent Workflow client")
    sub = parser.add_subparsers(dest="command", required=True)

    p_token = sub.add_parser("token", help="Obtain a bearer token")
    p_token.add_argument("--base", default=DEFAULT_BASE)

    p_cmd = sub.add_parser("run", help="Run a command on the agent")
    p_cmd.add_argument("command")
    p_cmd.add_argument("--base", default=DEFAULT_BASE)
    p_cmd.add_argument("--token")

    p_chat = sub.add_parser("chat", help="Chat with the LLM")
    p_chat.add_argument("prompt")
    p_chat.add_argument("--provider", default=None)
    p_chat.add_argument("--base", default=DEFAULT_BASE)
    p_chat.add_argument("--token")

    p_tunnel = sub.add_parser("tunnel", help="Tunnel management")
    p_tunnel.add_argument("action", choices=["status", "start", "stop"])
    p_tunnel.add_argument("--base", default=DEFAULT_BASE)
    p_tunnel.add_argument("--token")

    p_hist = sub.add_parser("history", help="Show command history")
    p_hist.add_argument("--limit", type=int, default=50)
    p_hist.add_argument("--base", default=DEFAULT_BASE)
    p_hist.add_argument("--token")

    args = parser.parse_args()

    client = AIAClient(args.base, getattr(args, "token", None))

    if args.command == "token":
        print(client.get_token())
    elif args.command == "run":
        result = client.run_command(args.command)
        print(json.dumps(result, indent=2))
    elif args.command == "chat":
        print(client.chat(args.prompt, args.provider))
    elif args.command == "tunnel":
        if args.action == "status":
            print(json.dumps(client.tunnel_status(), indent=2))
        elif args.action == "start":
            print(json.dumps(client.tunnel_start(), indent=2))
        elif args.action == "stop":
            print(json.dumps(client.tunnel_stop(), indent=2))
    elif args.command == "history":
        for row in client.history(args.limit):
            print(f"[{row['status']}] {row['command']}")


if __name__ == "__main__":
    main()
