from __future__ import annotations

import asyncio
import json
from enum import StrEnum
from typing import Any, Iterable

from .ports import ReadOnlyGameQueryPort
from .workflow_protocol import (
    MAX_INFORMATION_QUERIES_PER_ROUND,
    MAX_INFORMATION_ROUND_BYTES,
    READ_ONLY_QUERY_SPECS,
    InformationRequest,
    WorkflowProtocolError,
    validate_information_request,
)


class InformationQueryFailureCategory(StrEnum):
    TOOL_UNAVAILABLE = "tool_unavailable"
    TRANSIENT_CONNECTION = "transient_connection"
    TIMEOUT = "timeout"
    QUERY_REJECTED = "query_rejected"
    MALFORMED_RESULT = "malformed_result"
    RESULT_TOO_LARGE = "result_too_large"
    ROUND_BUDGET_EXCEEDED = "round_budget_exceeded"


class InformationQueryError(RuntimeError):
    pass


class InformationQueryRouter:
    """Executes one bounded batch over the read-only MCP query surface."""

    def __init__(
        self,
        game: ReadOnlyGameQueryPort,
        *,
        max_queries: int = MAX_INFORMATION_QUERIES_PER_ROUND,
        query_timeout_seconds: float = 10.0,
        transient_attempts: int = 2,
    ):
        if max_queries != MAX_INFORMATION_QUERIES_PER_ROUND:
            raise ValueError("information query limit is a protocol constant")
        self.game = game
        self.max_queries = MAX_INFORMATION_QUERIES_PER_ROUND
        self.query_timeout_seconds = max(0.01, query_timeout_seconds)
        self.transient_attempts = max(1, transient_attempts)

    async def execute(
        self,
        requests: Iterable[InformationRequest],
        *,
        available_tools: set[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        rows = [validate_information_request(request) for request in requests]
        if len(rows) > self.max_queries:
            raise InformationQueryError(
                f"information query batch exceeds limit {self.max_queries}"
            )
        request_ids = [request.request_id for request in rows]
        if any(request_id is None for request_id in request_ids):
            raise WorkflowProtocolError(
                "information requests must be materialized before execution"
            )
        if len(request_ids) != len(set(request_ids)):
            raise WorkflowProtocolError("duplicate information request_id")

        if available_tools is None:
            list_tools = getattr(self.game, "list_tools", None)
            available_tools = (
                set(READ_ONLY_QUERY_SPECS)
                if list_tools is None
                else set(await list_tools())
            )

        query = getattr(self.game, "query_tool", None)
        if query is None:
            raise InformationQueryError(
                "configured game port does not support focused read-only queries"
            )

        results: dict[str, dict[str, Any]] = {}
        for request in rows:
            assert request.request_id is not None
            common = {
                "information_request_id": request.request_id,
                "event_dedupe_key": request.event_dedupe_key,
                "query_type": request.query_type,
                "tool_name": request.tool_name,
                "arguments": request.arguments,
                "purpose": request.purpose,
            }
            if request.tool_name not in available_tools:
                results[request.request_id] = {
                    **common,
                    "status": "FAILED",
                    "failure_category": (
                        InformationQueryFailureCategory.TOOL_UNAVAILABLE.value
                    ),
                    "message": (
                        f"civ6-mcp is missing read-only query tool: {request.tool_name}"
                    ),
                    "attempt_count": 0,
                }
                continue
            results[request.request_id] = await self._execute_one(
                request, common, query
            )

        return self._enforce_round_budget(results)

    async def _execute_one(self, request, common, query) -> dict[str, Any]:
        failure_category = InformationQueryFailureCategory.QUERY_REJECTED
        message = ""
        for attempt_number in range(1, self.transient_attempts + 1):
            try:
                raw = await asyncio.wait_for(
                    query(request.tool_name, request.arguments),
                    timeout=self.query_timeout_seconds,
                )
                if raw is None:
                    return {
                        **common,
                        "status": "FAILED",
                        "failure_category": (
                            InformationQueryFailureCategory.QUERY_REJECTED.value
                        ),
                        "message": "information query returned no result",
                        "attempt_count": attempt_number,
                    }
                result, truncated = self._normalize_result(request.tool_name, raw)
            except asyncio.CancelledError:
                raise
            except (TimeoutError, asyncio.TimeoutError) as exc:
                failure_category = InformationQueryFailureCategory.TIMEOUT
                message = str(exc) or "information query timed out"
            except (ConnectionError, OSError) as exc:
                failure_category = InformationQueryFailureCategory.TRANSIENT_CONNECTION
                message = str(exc)
            except (TypeError, ValueError) as exc:
                category = (
                    InformationQueryFailureCategory.RESULT_TOO_LARGE
                    if "exceeds" in str(exc)
                    else InformationQueryFailureCategory.MALFORMED_RESULT
                )
                return {
                    **common,
                    "status": "FAILED",
                    "failure_category": category.value,
                    "message": str(exc),
                    "attempt_count": attempt_number,
                }
            except Exception as exc:
                return {
                    **common,
                    "status": "FAILED",
                    "failure_category": (
                        InformationQueryFailureCategory.QUERY_REJECTED.value
                    ),
                    "message": str(exc),
                    "attempt_count": attempt_number,
                }
            else:
                return {
                    **common,
                    "status": "SUCCEEDED",
                    "result": result,
                    "truncated": truncated,
                    "attempt_count": attempt_number,
                }
            if attempt_number == self.transient_attempts:
                break
            await asyncio.sleep(0)

        return {
            **common,
            "status": "FAILED",
            "failure_category": failure_category.value,
            "message": message,
            "attempt_count": self.transient_attempts,
        }

    @staticmethod
    def _stable_item_key(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def _normalize_collections(cls, value: Any, *, max_items: int) -> tuple[Any, bool]:
        truncated = False
        if isinstance(value, dict):
            normalized: dict[str, Any] = {}
            for key in sorted(value):
                if not isinstance(key, str):
                    raise TypeError(
                        "information query result object keys must be strings"
                    )
                normalized[key], child_truncated = cls._normalize_collections(
                    value[key], max_items=max_items
                )
                truncated = truncated or child_truncated
            return normalized, truncated
        if isinstance(value, list):
            rows = list(value)
            stable_fields = ("id", "name", "city_id", "unit_id", "player_id")
            if rows and all(isinstance(item, dict) for item in rows):
                if any(all(field in item for item in rows) for field in stable_fields):
                    rows.sort(key=cls._stable_item_key)
            if len(rows) > max_items:
                rows = rows[:max_items]
                truncated = True
            normalized_rows = []
            for item in rows:
                normalized, child_truncated = cls._normalize_collections(
                    item, max_items=max_items
                )
                normalized_rows.append(normalized)
                truncated = truncated or child_truncated
            return normalized_rows, truncated
        if value is None or isinstance(value, (str, int, float, bool)):
            return value, False
        raise TypeError(
            f"information query result is not canonical JSON: {type(value).__name__}"
        )

    @classmethod
    def _normalize_result(cls, tool_name: str, raw: Any) -> tuple[Any, bool]:
        spec = READ_ONLY_QUERY_SPECS[tool_name]
        normalized, truncated = cls._normalize_collections(
            raw, max_items=spec.max_result_items
        )
        encoded = json.dumps(
            normalized,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(encoded) > spec.max_result_bytes:
            raise ValueError(
                f"information query result exceeds {spec.max_result_bytes} bytes"
            )
        return normalized, truncated

    @classmethod
    def _enforce_round_budget(
        cls, results: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        bounded = {key: results[key] for key in sorted(results)}
        while (
            len(
                json.dumps(
                    bounded,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8")
            )
            > MAX_INFORMATION_ROUND_BYTES
        ):
            candidates = [
                key
                for key, value in bounded.items()
                if value.get("status") == "SUCCEEDED"
            ]
            if not candidates:
                raise InformationQueryError(
                    "information query failure metadata exceeds round budget"
                )
            key = max(
                candidates,
                key=lambda candidate: len(
                    cls._stable_item_key(bounded[candidate]).encode("utf-8")
                ),
            )
            original = bounded[key]
            bounded[key] = {
                name: value
                for name, value in original.items()
                if name not in {"result", "truncated"}
            }
            bounded[key].update(
                {
                    "status": "FAILED",
                    "failure_category": (
                        InformationQueryFailureCategory.ROUND_BUDGET_EXCEEDED.value
                    ),
                    "message": "result omitted because the information round budget was exceeded",
                }
            )
        return bounded
