from __future__ import annotations

import json
from typing import Any, Iterable

from .models import GameEvent
from .workflow_protocol import InformationRequest, validate_information_request


class InformationQueryError(RuntimeError):
    pass


class InformationQueryRouter:
    """Executes a small, read-only MCP query surface for focused planning."""

    def __init__(self, game: Any, *, max_queries: int = 8):
        self.game = game
        self.max_queries = max(1, max_queries)

    def prefetch(self, events: Iterable[GameEvent]) -> list[InformationRequest]:
        requests: list[InformationRequest] = []
        seen: set[str] = set()
        for event in events:
            request: InformationRequest | None = None
            if event.event_type == "settler_site_selection_required":
                unit_id = event.entity_id
                if unit_id is not None:
                    request = InformationRequest(
                        event_dedupe_key=event.dedupe_key,
                        query_type="settler_select_site",
                        tool_name="get_settle_advisor",
                        arguments={"unit_id": unit_id},
                        purpose="Rank valid settlement sites near the blocking settler.",
                    )
            elif event.event_type == "unit_promotion_required":
                unit_id = event.entity_id
                if unit_id is not None:
                    request = InformationRequest(
                        event_dedupe_key=event.dedupe_key,
                        query_type="unit_promotion_options",
                        tool_name="get_unit_promotions",
                        arguments={"unit_id": unit_id},
                        purpose="List legal promotion choices for the blocking unit.",
                    )
            if request is None:
                continue
            key = self._semantic_key(request)
            if key in seen:
                continue
            seen.add(key)
            requests.append(request)
            if len(requests) >= self.max_queries:
                break
        return requests

    async def execute(
        self, requests: Iterable[InformationRequest]
    ) -> dict[str, dict[str, Any]]:
        rows = list(requests)
        if len(rows) > self.max_queries:
            raise InformationQueryError(
                f"information query batch exceeds limit {self.max_queries}"
            )
        results: dict[str, dict[str, Any]] = {}
        for request in rows:
            validate_information_request(request)
            query = getattr(self.game, "query_tool", None)
            if query is None:
                raise InformationQueryError(
                    "configured game port does not support focused read-only queries"
                )
            try:
                raw = await query(request.tool_name, request.arguments)
            except Exception as exc:
                raise InformationQueryError(
                    f"information query {request.request_id} ({request.tool_name}) failed: {exc}"
                ) from exc
            results[request.request_id] = {
                "event_dedupe_key": request.event_dedupe_key,
                "query_type": request.query_type,
                "tool_name": request.tool_name,
                "arguments": request.arguments,
                "purpose": request.purpose,
                "result": raw,
            }
        return results

    @staticmethod
    def _semantic_key(request: InformationRequest) -> str:
        return json.dumps(
            {"tool": request.tool_name, "arguments": request.arguments},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
