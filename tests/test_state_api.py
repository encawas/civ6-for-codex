import asyncio

import httpx
import pytest

from civ6_workflow.state_api import Civ6StateApi, StateApiConfig


def test_state_api_retries_startup_503_until_snapshot_is_ready(monkeypatch):
    class Client:
        def __init__(self):
            self.calls = 0

        async def get(self, path, **kwargs):
            self.calls += 1
            request = httpx.Request("GET", f"http://state.test{path}")
            if self.calls == 1:
                return httpx.Response(503, request=request)
            return httpx.Response(
                200,
                request=request,
                json={"overview": {"turn": 109}},
            )

    async def no_wait(_seconds):
        return None

    api = Civ6StateApi(
        StateApiConfig(
            base_url="http://state.test",
            startup_retry_seconds=1.0,
        )
    )
    client = Client()
    api.client = client
    monkeypatch.setattr(asyncio, "sleep", no_wait)

    result = asyncio.run(api.get("/api/workflow/snapshot"))

    assert result == {"overview": {"turn": 109}}
    assert client.calls == 2


def test_state_api_uses_short_runtime_budget_after_startup(monkeypatch):
    class Client:
        def __init__(self):
            self.responses = [200, 503]
            self.timeouts = []

        async def get(self, path, **kwargs):
            self.timeouts.append(kwargs["timeout"])
            status_code = self.responses.pop(0)
            request = httpx.Request("GET", f"http://state.test{path}")
            return httpx.Response(status_code, request=request, json={})

    async def no_wait(_seconds):
        return None

    api = Civ6StateApi(
        StateApiConfig(
            base_url="http://state.test",
            timeout_seconds=10.0,
            runtime_timeout_seconds=2.0,
            startup_retry_seconds=0.0,
            runtime_retry_seconds=0.0,
        )
    )
    client = Client()
    api.client = client
    monkeypatch.setattr(asyncio, "sleep", no_wait)

    assert asyncio.run(api.get("/ready")) == {}
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(api.get("/api/workflow/snapshot"))

    assert client.timeouts == [10.0, 2.0]
    assert api.availability_epoch == 1


def test_state_api_connect_timeout_is_bounded_and_marks_reconnect(monkeypatch):
    class Client:
        def __init__(self):
            self.calls = 0

        async def get(self, path, **kwargs):
            self.calls += 1
            if self.calls == 1:
                request = httpx.Request("GET", f"http://state.test{path}")
                return httpx.Response(200, request=request, json={})
            raise httpx.ConnectTimeout("timed out")

    async def no_wait(_seconds):
        return None

    api = Civ6StateApi(
        StateApiConfig(
            base_url="http://state.test",
            startup_retry_seconds=0.0,
            runtime_retry_seconds=0.0,
            runtime_timeout_seconds=0.25,
        )
    )
    api.client = Client()
    monkeypatch.setattr(asyncio, "sleep", no_wait)

    assert asyncio.run(api.get("/ready")) == {}
    with pytest.raises(httpx.ConnectTimeout):
        asyncio.run(api.get("/api/workflow/snapshot"))

    assert api.client.calls == 2
    assert api.availability_epoch == 1


def test_state_api_does_not_swallow_cancellation():
    class Client:
        async def get(self, path, **kwargs):
            raise asyncio.CancelledError

    api = Civ6StateApi(
        StateApiConfig(
            base_url="http://state.test",
            startup_retry_seconds=30.0,
        )
    )
    api.client = Client()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(api.get("/api/workflow/snapshot"))
