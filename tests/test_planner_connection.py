import asyncio

import httpx

from civ6_workflow.codex_planner import CodexPlannerConfig
from civ6_workflow.planner_connection import (
    planner_connection_status,
    probe_planner_connection,
)


class _Client:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get(self, url, *, headers):
        assert url == "https://api.openai.com/v1/models/test-model"
        assert headers["Authorization"] == "Bearer test-key"
        return httpx.Response(
            200,
            headers={"x-request-id": "req_model_probe"},
            json={"id": "test-model", "object": "model"},
        )


def test_status_does_not_expose_secret(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "secret-value")
    status = planner_connection_status(
        CodexPlannerConfig(backend="responses", model="test-model")
    )

    assert status["configured"] is True
    assert status["credential_present"] is True
    assert status["secret_exposed_to_browser"] is False
    assert "secret-value" not in repr(status)


def test_responses_probe_retrieves_configured_model(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        "civ6_workflow.planner_connection.httpx.AsyncClient", _Client
    )

    result = asyncio.run(
        probe_planner_connection(
            CodexPlannerConfig(backend="responses", model="test-model")
        )
    )

    assert result["ok"] is True
    assert result["http_status"] == 200
    assert result["request_id"] == "req_model_probe"
    assert result["connection_owner"] == "frontend_via_local_backend"
    assert result["secret_exposed_to_browser"] is False


def test_responses_probe_fails_without_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    result = asyncio.run(
        probe_planner_connection(
            CodexPlannerConfig(backend="responses", model="test-model")
        )
    )

    assert result["ok"] is False
    assert "OPENAI_API_KEY" in result["error"]
