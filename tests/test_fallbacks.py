"""Remote AI Agent Workflow - fallbacks, isolation, and multi-spot tests."""

import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

import server
from server import app, Spot, upsert_spot, spots, run_command, query_llm_with_fallback


@pytest.fixture(autouse=True)
def clean_spots():
    spots.clear()
    s = Spot(
        spot_id="spot-a",
        name="Spot A",
        host="127.0.0.1",
        port=8080,
        api_key="key-a",
        status="online",
        last_heartbeat=time.time(),
        capabilities=["cmd", "llm", "tunnel"],
        resource_limits={"max_cmd_duration_s": 10, "max_output_kb": 64},
        created_at=time.time(),
        updated_at=time.time(),
    )
    upsert_spot(s)
    s2 = Spot(
        spot_id="spot-b",
        name="Spot B",
        host="127.0.0.1",
        port=8081,
        api_key="key-b",
        status="offline",
        last_heartbeat=0.0,
        capabilities=["cmd"],
        resource_limits={},
        created_at=time.time(),
        updated_at=time.time(),
    )
    upsert_spot(s2)
    yield
    spots.clear()


# ---------------------------------------------------------------------------
# Multi-spot isolation
# ---------------------------------------------------------------------------

def test_spot_isolation_separate_state():
    """Each spot maintains independent state; commands on spot-a don't affect spot-b."""
    assert server.get_spot("spot-a").status == "online"
    assert server.get_spot("spot-b").status == "offline"

    # Register a third spot
    s3 = Spot(
        spot_id="spot-c",
        name="Spot C",
        host="10.0.0.3",
        port=8082,
        api_key="key-c",
        status="online",
        last_heartbeat=time.time(),
        capabilities=["llm"],
        resource_limits={},
        created_at=time.time(),
        updated_at=time.time(),
    )
    server.upsert_spot(s3)
    assert server.get_spot("spot-c").capabilities == ["llm"]
    assert server.get_spot("spot-a").capabilities == ["cmd", "llm", "tunnel"]


def test_resource_limits_applied_per_spot():
    spot_a = server.get_spot("spot-a")
    assert spot_a.resource_limits["max_output_kb"] == 64
    spot_b = server.get_spot("spot-b")
    assert spot_b.resource_limits == {}


# ---------------------------------------------------------------------------
# Fallbacks: LLM provider chain
# ---------------------------------------------------------------------------

@patch("server.requests.post")
def test_llm_full_failover_chain(mock_post):
    """All providers fail -> returns error message, provider='none'."""
    server.OPENAI_API_KEY = ""
    server.OPENROUTER_API_KEY = ""
    mock_post.side_effect = Exception("network down")

    response, provider = server.query_llm_with_fallback("hello", preferred="ollama")
    assert provider == "none"
    assert "All LLM providers failed" in response


@patch("server.requests.post")
def test_llm_preferred_then_failover(mock_post):
    """Preferred provider succeeds, no failover needed."""
    server.OPENROUTER_API_KEY = "test-key"
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"choices": [{"message": {"content": "preferred answer"}}]}
    mock_post.return_value = mock_resp

    response, provider = server.query_llm_with_fallback("hello", preferred="openrouter")
    assert provider == "openrouter"
    assert "preferred answer" in response


# ---------------------------------------------------------------------------
# Tunnel fallback: ngrok unavailable -> local URL
# ---------------------------------------------------------------------------

@patch("pyngrok.ngrok.connect", side_effect=ImportError("no pyngrok"))
def test_tunnel_start_no_pyngrok(mock_connect):
    token = server.create_token(spot_id="spot-a")
    client = TestClient(server.app)
    resp = client.post(
        "/spots/spot-a/tunnel/start",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 500


# ---------------------------------------------------------------------------
# Heartbeat -> offline transition
# ---------------------------------------------------------------------------

def test_heartbeat_marks_online():
    spot = server.get_spot("spot-b")
    assert spot.status == "offline"
    server.update_spot_heartbeat("spot-b")
    spot = server.get_spot("spot-b")
    assert spot.status == "online"


# ---------------------------------------------------------------------------
# Output cap isolation
# ---------------------------------------------------------------------------

def test_output_cap_respected():
    spot = server.get_spot("spot-a")
    # max_output_kb = 64 -> small cap. Use a command that produces long output.
    result = server.run_command("spot-a", "python -c \"print('x'*200000)\"", "tester")
    if result["output"]:
        assert len(result["output"].encode("utf-8")) <= 64 * 1024 + 100
