import asyncio

import httpx

from civ6_workflow.state_api import Civ6StateApi, StateApiConfig


def test_state_api_retries_startup_503_until_snapshot_is_ready(monkeypatch):
    class Client:
        def __init__(self):
            self.calls = 0

        async def get(self, path):
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
