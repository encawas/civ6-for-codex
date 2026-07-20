from __future__ import annotations

from typing import Any

from .actions import ActionValidationError, resolve_action
from .mcp_port import (
    Civ6GamePort as BaseCiv6GamePort,
    McpToolRejectedError,
)
from .models import ActionResult, MutationDeliveryStatus, StoredTask
from .workflow_protocol import (
    READ_ONLY_QUERY_SPECS,
    InformationRequest,
    validate_information_request,
)


class SafeCiv6GamePort(BaseCiv6GamePort):
    """Fetch unit rows only when needed and expose a read-only query surface."""

    async def read_snapshot(self, *, include_units: bool = False):
        snapshot = await super().read_snapshot(include_units=include_units)
        if snapshot.units is not None or not self._has_unit_blocker(snapshot.blockers):
            return snapshot
        units = await self.state_api.get("/api/units")
        return snapshot.model_copy(update={"units": units})

    async def execute_task(self, task: StoredTask) -> ActionResult:
        try:
            tool_name, arguments = resolve_action(task, self.allowed_tools)
        except ActionValidationError as exc:
            return ActionResult(
                success=False,
                blocked=True,
                message=str(exc),
                delivery_status=MutationDeliveryStatus.PROVEN_NOT_SENT,
            )
        try:
            raw = await self.client.call_tool(tool_name, arguments)
        except McpToolRejectedError as exc:
            return ActionResult(
                success=False,
                blocked=True,
                message=str(exc),
                details={
                    "tool_name": exc.tool_name,
                    "error_type": type(exc).__name__,
                    "rejection_code": exc.rejection_code,
                },
                delivery_status=MutationDeliveryStatus.EXPLICITLY_REJECTED,
            )
        except Exception as exc:
            return ActionResult(
                success=False,
                message=f"{type(exc).__name__}: {exc}",
                details={
                    "tool_name": tool_name,
                    "error_type": type(exc).__name__,
                },
                delivery_status=MutationDeliveryStatus.UNKNOWN,
            )
        return self._normalize_action_result(raw)

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
