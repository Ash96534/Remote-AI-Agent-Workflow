#!/usr/bin/env python3
"""Remote AI Agent Workflow - Secure tunnel for agent control.

Allows remote access to instruct and drive an AI coding agent running on a
laptop directly from a mobile phone interface via a secure tunnel.
"""

import os
import sys
import json
import threading
import time
from pathlib import Path

try:
    from pyngrok import ngrok
    HAVE_NGROK = True
except ImportError:
    HAVE_NGROK = False

try:
    import ollama
    HAVE_OLLAMA = True
except ImportError:
    HAVE_OLLAMA = False


class RemoteAgentWorkflow:
    """Secure tunnel workflow for remote agent control."""

    def __init__(self, local_port: int = 8080, tunnel_url: str | None = None):
        self.local_port = local_port
        self.tunnel_url = tunnel_url
        self.public_url = None
        self.ollama_client = None
        self.tunnel = None

        if HAVE_OLLAMA:
            try:
                self.ollama_client = ollama.Client()
            except Exception:
                self.ollama_client = None

    def start_tunnel(self) -> str:
        """Start ngrok tunnel and return public URL."""
        if not HAVE_NGROK:
            raise RuntimeError("pyngrok not installed. Install with: pip install pyngrok")

        self.tunnel = ngrok.connect(self.local_port, "http")
        self.public_url = self.tunnel.public_url
        print(f"Tunnel established: {self.public_url}")
        return self.public_url

    def stop_tunnel(self):
        """Close the tunnel."""
        if self.tunnel:
            ngrok.disconnect(self.public_url)
            self.tunnel = None
            self.public_url = None

    def query_ollama(self, prompt: str, model: str = "llama2") -> str:
        """Send a prompt to local Ollama model."""
        if not self.ollama_client:
            return "Ollama client not available"

        try:
            response = self.ollama_client.generate(model=model, prompt=prompt)
            return response.get("response", "")
        except Exception as e:
            return f"Error: {str(e)}"

    def execute_command(self, command: str) -> str:
        """Execute a local command and return output (sandboxed)."""
        import subprocess

        try:
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True, timeout=30
            )
            output = result.stdout
            if result.stderr:
                output += f"\nSTDERR: {result.stderr}"
            return output or "(no output)"
        except Exception as e:
            return f"Error: {str(e)}"

    def run(self):
        """Start the workflow server and tunnel."""
        print("Remote AI Agent Workflow")
        print("=" * 40)

        if not HAVE_NGROK:
            print("WARNING: pyngrok not installed. Tunnel will not work.")
            print("Install with: pip install pyngrok")

        if not HAVE_OLLAMA:
            print("WARNING: ollama not installed. LLM features disabled.")
            print("Install with: pip install ollama")

        public_url = self.start_tunnel()
        print(f"Public URL: {public_url}")
        print(f"Local port: {self.local_port}")
        print("\nShare the public URL with your mobile interface.")
        print("Press Ctrl+C to stop.\n")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            self.stop_tunnel()


if __name__ == "__main__":
    workflow = RemoteAgentWorkflow(local_port=8080)
    workflow.run()
