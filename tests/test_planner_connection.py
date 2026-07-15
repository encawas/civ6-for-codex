import asyncio
import json

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

    async def post(self, url, *, headers, content):
        assert url == "https://api.openai.com/v1/responses"
        assert headers["Authorization"] == "Bearer test-key"
        payload = json.loads(content)
        assert payload["model"] == "test-model"
        assert payload["text"]["format"]["type"] == "json_schema"
        return httpx.Response(
            200,
            headers={"x-request-id": "req_model_probe"},
            json={
                "model": "test-model",
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {"type": "output_text", "text": '{"ok":true}'}
                        ],
                    }
                ],
            },
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


def test_status_reads_key_from_configured_auth_file(monkeypatch, tmp_path):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(
        json.dumps({"OPENAI_API_KEY": "file-secret"}), encoding="utf-8"
    )

    status = planner_connection_status(
        CodexPlannerConfig(
            backend="responses", model="test-model", api_key_file=auth_file
        )
    )

    assert status["configured"] is True
    assert status["credential_source"].startswith("file:")
    assert "file-secret" not in repr(status)


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


class _HtmlClient(_Client):
    async def post(self, url, *, headers, content):
        return httpx.Response(
            200,
            headers={
                "x-request-id": "req_html",
                "content-type": "text/html; charset=utf-8",
            },
            text="<html>not an API response</html>",
        )


def test_responses_probe_rejects_html_success_page(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        "civ6_workflow.planner_connection.httpx.AsyncClient", _HtmlClient
    )

    result = asyncio.run(
        probe_planner_connection(
            CodexPlannerConfig(backend="responses", model="test-model")
        )
    )

    assert result["ok"] is False
    assert result["http_status"] == 200
    assert "non-JSON" in result["error"]
