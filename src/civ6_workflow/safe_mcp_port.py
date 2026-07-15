from __future__ import annotations

from typing import Any

from .mcp_port import Civ6GamePort as BaseCiv6GamePort
from .workflow_protocol import READ_ONLY_QUERY_SPECS, InformationRequest, validate_information_request


class SafeCiv6GamePort(BaseCiv6GamePort):
    """Fetch unit rows only when needed and expose a read-only query surface."""

    async def read_snapshot(self, *, include_units: bool = False):
        snapshot = await super().read_snapshot(include_units=include_units)
        if snapshot.units is not None or not self._has_unit_blocker(snapshot.blockers):
            return snapshot
        units = await self.state_api.get("/api/units")
        return snapshot.model_copy(update={"units": units})

    async def query_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any:
        if name not in READ_ONLY_QUERY_SPECS:
            raise RuntimeError(f"read-only workflow query is not allowed: {name}")
        request = InformationRequest(
            event_dedupe_key="internal-query-validation",
            query_type=name,
            tool_name=name,
            arguments=arguments or {},
            purpose="Validate and execute a focused read-only workflow query.",
        )
        validate_information_request(request)
        available = await self.list_tools()
        if name not in available:
            raise RuntimeError(f"civ6-mcp is missing read-only query tool: {name}")
        return await self.client.call_tool(name, arguments or {})

    @staticmethod
    def _has_unit_blocker(blockers) -> bool:
        return any(
            isinstance(blocker, dict)
            and str(blocker.get("blocking_type", "")) == "ENDTURN_BLOCKING_UNITS"
            for blocker in blockers
        )
