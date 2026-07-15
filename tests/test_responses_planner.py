import asyncio
import json

import httpx

from civ6_workflow.codex_planner import CodexPlannerConfig, PlannerError, SYSTEM_INSTRUCTIONS
from civ6_workflow.models import AgentRequest, ExecutionMode, PlanBundle
from civ6_workflow.responses_planner import ResponsesPlanner


class _Stream:
    def __init__(self, response: httpx.Response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _Client:
    def __init__(self, *args, **kwargs):
        self.payload = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def stream(self, method, url, *, headers, content):
        payload = json.loads(content)
        assert method == "POST"
        assert url == "https://api.openai.com/v1/responses"
        assert "tools" not in payload
        assert "tool_choice" not in payload
        assert payload["store"] is False
        assert payload["text"]["format"]["type"] == "json_schema"
        assert payload["text"]["format"]["strict"] is False
        plan = PlanBundle(summary="compact response plan").model_dump_json()
        body = {
            "id": "resp_test",
            "output": [
                {
                    "type": "message",
                    "content": [{"type": "output_text", "text": plan}],
                }
            ],
        }
        response = httpx.Response(
            200,
            headers={"x-request-id": "req_http_123"},
            content=json.dumps(body).encode("utf-8"),
        )
        return _Stream(response)


def test_responses_planner_returns_schema_plan_and_diagnostics(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr("civ6_workflow.responses_planner.httpx.AsyncClient", _Client)
    planner = ResponsesPlanner(
        CodexPlannerConfig(
            backend="responses",
            model="test-model",
            reasoning_effort="low",
        ),
        SYSTEM_INSTRUCTIONS,
        PlannerError,
    )
    request = AgentRequest(
        turn=5,
        execution_mode=ExecutionMode.CONFIRM,
        trigger_events=[],
        relevant_state={"turn": 5},
        constraints={"max_tasks": 2},
    )

    bundle = asyncio.run(planner.plan(request))

    assert bundle.summary == "compact response plan"
    assert planner.last_diagnostics is not None
    assert planner.last_diagnostics["backend"] == "responses"
    assert planner.last_diagnostics["request_id"] == "req_http_123"
    assert planner.last_diagnostics["http_status"] == 200
    assert planner.last_diagnostics["request_bytes"] > 0
    assert planner.last_diagnostics["response_bytes"] > 0
    assert planner.last_diagnostics["connect_and_headers_seconds"] is not None
    assert planner.last_diagnostics["first_byte_seconds"] is not None
    assert planner.last_diagnostics["completion_seconds"] is not None


def test_responses_planner_fails_fast_without_api_key(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    planner = ResponsesPlanner(
        CodexPlannerConfig(backend="responses", model="test-model"),
        SYSTEM_INSTRUCTIONS,
        PlannerError,
    )
    request = AgentRequest(
        turn=5,
        execution_mode=ExecutionMode.CONFIRM,
        trigger_events=[],
        constraints={"max_tasks": 1},
    )

    try:
        asyncio.run(planner.plan(request))
    except PlannerError as exc:
        assert "OPENAI_API_KEY" in str(exc)
    else:
        raise AssertionError("missing API key must fail before an HTTP request")
