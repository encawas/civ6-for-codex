from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

import httpx

from .models import AgentRequest, PlanBundle

log = logging.getLogger(__name__)


@dataclass(slots=True)
class PlannerHttpDiagnostics:
    backend: str = "responses"
    request_bytes: int = 0
    connect_and_headers_seconds: float | None = None
    first_byte_seconds: float | None = None
    completion_seconds: float | None = None
    request_id: str | None = None
    http_status: int | None = None
    response_bytes: int = 0
    error_body: str | None = None


class ResponsesPlanner:
    """Low-overhead runtime planner using the OpenAI Responses API directly."""

    def __init__(self, config: Any, system_instructions: str, error_type: type[Exception]):
        self.config = config
        self.system_instructions = system_instructions
        self.error_type = error_type
        self.last_diagnostics: dict[str, Any] | None = None

    async def plan(self, request: AgentRequest) -> PlanBundle:
        api_key = os.environ.get(self.config.api_key_env)
        if not api_key:
            raise self.error_type(
                f"Responses API planner requires environment variable "
                f"{self.config.api_key_env}"
            )
        if not self.config.model:
            raise self.error_type(
                "Responses API planner requires codex.model in config.toml"
            )

        request_text = request.model_dump_json(exclude_none=True)
        payload: dict[str, Any] = {
            "model": self.config.model,
            "instructions": self.system_instructions,
            "input": request_text,
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "civ6_plan_bundle",
                    "schema": PlanBundle.model_json_schema(),
                    # Pydantic defaults intentionally remain optional in the remote
                    # schema. The returned object is validated strictly and with
                    # extra="forbid" locally before any task is persisted.
                    "strict": False,
                },
            },
            "store": False,
        }
        if self.config.reasoning_effort:
            payload["reasoning"] = {"effort": self.config.reasoning_effort}
        raw_request = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        diagnostics = PlannerHttpDiagnostics(request_bytes=len(raw_request))
        started = time.perf_counter()
        timeout = httpx.Timeout(
            connect=self.config.connect_timeout_seconds,
            read=self.config.read_timeout_seconds,
            write=self.config.write_timeout_seconds,
            pool=self.config.pool_timeout_seconds,
        )
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        url = f"{self.config.api_base_url.rstrip('/')}/responses"

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:

                async def perform() -> tuple[httpx.Response, bytes]:
                    async with client.stream(
                        "POST", url, headers=headers, content=raw_request
                    ) as response:
                        diagnostics.connect_and_headers_seconds = (
                            time.perf_counter() - started
                        )
                        diagnostics.request_id = response.headers.get("x-request-id")
                        diagnostics.http_status = response.status_code
                        chunks: list[bytes] = []
                        async for chunk in response.aiter_bytes():
                            if diagnostics.first_byte_seconds is None:
                                diagnostics.first_byte_seconds = (
                                    time.perf_counter() - started
                                )
                            chunks.append(chunk)
                        return response, b"".join(chunks)

                response, body = await asyncio.wait_for(
                    perform(), timeout=self.config.timeout_seconds
                )
        except TimeoutError as exc:
            diagnostics.completion_seconds = time.perf_counter() - started
            diagnostics.error_body = "total planner timeout"
            self._record(diagnostics)
            raise self.error_type(
                "Responses API planning timed out after "
                f"{self.config.timeout_seconds}s; diagnostics={self.last_diagnostics}"
            ) from exc
        except httpx.HTTPError as exc:
            diagnostics.completion_seconds = time.perf_counter() - started
            diagnostics.error_body = str(exc)
            self._record(diagnostics)
            raise self.error_type(
                f"Responses API transport failed: {exc}; "
                f"diagnostics={self.last_diagnostics}"
            ) from exc

        diagnostics.completion_seconds = time.perf_counter() - started
        diagnostics.response_bytes = len(body)
        if response.status_code >= 400:
            diagnostics.error_body = body.decode("utf-8", errors="replace")[-4000:]
            self._record(diagnostics)
            raise self.error_type(
                f"Responses API returned HTTP {response.status_code}; "
                f"request_id={diagnostics.request_id}; body={diagnostics.error_body!r}"
            )

        try:
            response_json = json.loads(body)
        except json.JSONDecodeError as exc:
            diagnostics.error_body = body.decode("utf-8", errors="replace")[-4000:]
            self._record(diagnostics)
            raise self.error_type(
                f"Responses API returned invalid JSON; request_id={diagnostics.request_id}"
            ) from exc

        output_text = _extract_output_text(response_json)
        if not output_text:
            diagnostics.error_body = json.dumps(
                response_json, ensure_ascii=False, separators=(",", ":")
            )[-4000:]
            self._record(diagnostics)
            raise self.error_type(
                "Responses API completed without output_text; "
                f"request_id={diagnostics.request_id}"
            )

        try:
            bundle = PlanBundle.model_validate_json(output_text)
        except Exception as exc:
            diagnostics.error_body = output_text[-4000:]
            self._record(diagnostics)
            raise self.error_type(
                "Responses API returned an invalid PlanBundle; "
                f"request_id={diagnostics.request_id}: {exc}"
            ) from exc

        max_tasks = int(request.constraints.get("max_tasks", 8))
        if len(bundle.tasks) > max_tasks:
            diagnostics.error_body = (
                f"planner returned {len(bundle.tasks)} tasks with max_tasks={max_tasks}"
            )
            self._record(diagnostics)
            raise self.error_type(diagnostics.error_body)

        self._record(diagnostics)
        return bundle

    def _record(self, diagnostics: PlannerHttpDiagnostics) -> None:
        self.last_diagnostics = asdict(diagnostics)
        log.info(
            "civ6 planner HTTP diagnostics: %s",
            json.dumps(self.last_diagnostics, ensure_ascii=False, separators=(",", ":")),
        )


def _extract_output_text(response: dict[str, Any]) -> str:
    for item in response.get("output", []) or []:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            if isinstance(content, dict) and content.get("type") == "output_text":
                text = content.get("text")
                if isinstance(text, str):
                    return text.strip()
    return ""
