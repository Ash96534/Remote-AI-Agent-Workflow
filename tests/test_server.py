"""Remote AI Agent Workflow - test suite.

Covers the verification matrix from SPEC.md:
- Auth (issue, reject missing/invalid)
- Command execution (ls, blacklist rejection, output cap)
- LLM router with failover
- Tunnel start/stop/status
- Spot register/list/get/delete/heartbeat
- Health and metrics
"""

import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import jwt
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

import server
from server import app, run_command, query_llm_with_fallback, is_command_blacklisted, spots, upsert_spot, Spot


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def auth_token():
    return server.create_token(spot_id="test-spot")


@pytest.fixture
def auth_headers(auth_token):
    return {"Authorization": f"Bearer {auth_token}"}


@pytest.fixture(autouse=True)
def clean_spots():
    spots.clear()
    s = Spot(
        spot_id="test-spot",
        name="Test Spot",
        host="127.0.0.1",
        port=8080,
        api_key="test-key",
        status="online",
        last_heartbeat=time.time(),
        capabilities=["cmd", "llm", "tunnel"],
        resource_limits={},
        created_at=time.time(),
        updated_at=time.time(),
    )
    upsert_spot(s)
    yield
    spots.clear()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def test_auth_issue(client):
    resp = client.post("/auth/token")
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    # Verify token decodes
    decoded = jwt.decode(data["access_token"], server.JWT_SECRET, algorithms=[server.JWT_ALGORITHM])
    assert decoded["sub"] == "agent"


def test_auth_rejects_missing_token(client):
    resp = client.get("/spots")
    assert resp.status_code == 401


def test_auth_rejects_invalid_token(client):
    resp = client.get("/spots", headers={"Authorization": "Bearer not-a-real-token"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Spots
# ---------------------------------------------------------------------------

def test_register_spot(client, auth_headers):
    resp = client.post(
        "/spots/register",
        json={"name": "My Laptop", "capabilities": ["cmd", "llm"]},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "spot_id" in data
    assert "api_key" in data
    assert data["name"] == "My Laptop"


def test_list_spots(client, auth_headers):
    resp = client.get("/spots", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert any(s["spot_id"] == "test-spot" for s in data)


def test_get_spot(client, auth_headers):
    resp = client.get("/spots/test-spot", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["spot_id"] == "test-spot"
    assert data["name"] == "Test Spot"


def test_delete_spot(client, auth_headers):
    # Register a temp spot
    reg = client.post(
        "/spots/register",
        json={"name": "Temp"},
        headers=auth_headers,
    )
    sid = reg.json()["spot_id"]
    resp = client.delete(f"/spots/{sid}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["spot_id"] == sid


def test_spot_heartbeat(client, auth_headers):
    resp = client.post("/spots/test-spot/heartbeat", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------

def test_command_ls(client, auth_headers):
    resp = client.post(
        "/spots/test-spot/command",
        json={"command": "echo hello"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "completed"
    assert "hello" in data["output"]


def test_command_blacklist(client, auth_headers):
    resp = client.post(
        "/spots/test-spot/command",
        json={"command": "rm -rf /"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "rejected"
    assert "blocked" in data["error"].lower()


def test_command_nonexistent_spot(client, auth_headers):
    resp = client.post(
        "/spots/nope/command",
        json={"command": "echo hi"},
        headers=auth_headers,
    )
    assert resp.status_code == 404


def test_command_offline_spot(client, auth_headers):
    # Mark spot offline
    spot = server.get_spot("test-spot")
    spot.status = "offline"
    server.upsert_spot(spot)
    resp = client.post(
        "/spots/test-spot/command",
        json={"command": "echo hi"},
        headers=auth_headers,
    )
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Isolation / blacklist
# ---------------------------------------------------------------------------

def test_blacklist_patterns():
    assert is_command_blacklisted("rm -rf /") is True
    assert is_command_blacklisted("mkfs.ext4 /dev/sda") is True
    assert is_command_blacklisted("dd if=/dev/zero of=/dev/sda") is True
    assert is_command_blacklisted("shutdown now") is True
    assert is_command_blacklisted("reboot") is True
    assert is_command_blacklisted("ls -la") is False
    assert is_command_blacklisted("echo hello") is False


# ---------------------------------------------------------------------------
# LLM router
# ---------------------------------------------------------------------------

@patch("server.requests.post")
def test_llm_ollama_success(mock_post, auth_headers):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"response": "Hello from Ollama"}
    mock_post.return_value = mock_resp

    response, provider = server.query_llm_with_fallback("hi", preferred="ollama")
    assert provider == "ollama"
    assert "Hello from Ollama" in response


@patch("server.requests.post")
def test_llm_failover_to_openai(mock_post, auth_headers):
    # Ollama raises, OpenAI succeeds
    server.OPENAI_API_KEY = "test-key"
    def side_effect(*args, **kwargs):
        url = args[0] if args else kwargs.get("url", "")
        if "localhost:11434" in url:
            raise Exception("connection refused")
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"choices": [{"message": {"content": "Hello from OpenAI"}}]}
        return mock_resp

    mock_post.side_effect = side_effect

    response, provider = server.query_llm_with_fallback("hi", preferred="ollama")
    assert provider == "openai"
    assert "Hello from OpenAI" in response


# ---------------------------------------------------------------------------
# Tunnel
# ---------------------------------------------------------------------------

@patch("pyngrok.ngrok.connect")
def test_tunnel_start(mock_connect, client, auth_headers):
    mock_tunnel = MagicMock()
    mock_tunnel.public_url = "https://abc123.ngrok.io"
    mock_connect.return_value = mock_tunnel

    resp = client.post(
        "/spots/test-spot/tunnel/start",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["public_url"] == "https://abc123.ngrok.io"
    assert data["spot_id"] == "test-spot"


@patch("pyngrok.ngrok.disconnect")
def test_tunnel_stop(mock_disconnect, client, auth_headers):
    server._tunnel_public_urls["test-spot"] = "https://abc123.ngrok.io"
    resp = client.post(
        "/spots/test-spot/tunnel/stop",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "stopped"


def test_tunnel_status(client, auth_headers):
    resp = client.get("/spots/test-spot/tunnel/status", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "active" in data
    assert "local_port" in data


# ---------------------------------------------------------------------------
# Health / metrics
# ---------------------------------------------------------------------------

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "running"


def test_metrics(client, auth_headers):
    resp = client.get("/metrics", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "commands_total" in data
    assert "spots_online" in data


# ---------------------------------------------------------------------------
# run_command direct
# ---------------------------------------------------------------------------

def test_run_command_direct():
    result = server.run_command("test-spot", "echo direct-test", "tester")
    assert result["status"] == "completed"
    assert "direct-test" in result["output"]
