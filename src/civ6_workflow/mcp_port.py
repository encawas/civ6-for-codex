from __future__ import annotations

import json
import os
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any, Protocol

from .actions import ActionValidationError, resolve_action
from .models import ActionResult, RuntimeSnapshot, StoredTask
from .state_api import Civ6StateApi


@dataclass(slots=True)
class McpServerConfig:
    command: str = "civ-mcp"
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)


class GamePort(Protocol):
    call_count: int

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot: ...

    async def execute_task(self, task: StoredTask) -> ActionResult: ...

    async def end_turn(self) -> ActionResult: ...

    async def list_tools(self) -> set[str]: ...


class Civ6McpClient:
    def __init__(self, config: McpServerConfig):
        self.config = config
        self._stack: AsyncExitStack | None = None
        self.session: Any | None = None
        self.call_count = 0

    async def __aenter__(self) -> "Civ6McpClient":
        try:
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "The optional mcp package is required for a live Civ6 connection"
            ) from exc
        self._stack = AsyncExitStack()
        params = StdioServerParameters(
            command=self.config.command,
            args=self.config.args,
            env={**os.environ, **self.config.env},
        )
        read, write = await self._stack.enter_async_context(stdio_client(params))
        self.session = await self._stack.enter_async_context(ClientSession(read, write))
        await self.session.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._stack is not None:
            await self._stack.aclose()
        self.session = None
        self._stack = None

    def _require_session(self) -> Any:
        if self.session is None:
            raise RuntimeError("Civ6 MCP client is not connected")
        return self.session

    async def list_tools(self) -> set[str]:
        result = await self._require_session().list_tools()
        return {tool.name for tool in result.tools}

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        self.call_count += 1
        result = await self._require_session().call_tool(name, arguments=arguments or {})
        is_error = bool(
            getattr(result, "isError", False) or getattr(result, "is_error", False)
        )
        if is_error:
            text = self._extract_text(result.content)
            raise RuntimeError(text or f"MCP tool {name} returned an error")
        structured = getattr(result, "structuredContent", None)
        if structured is None:
            structured = getattr(result, "structured_content", None)
        if structured is not None:
            return structured
        text = self._extract_text(result.content)
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"text": text}

    @staticmethod
    def _extract_text(content: list[Any]) -> str:
        parts: list[str] = []
        for block in content:
            text = getattr(block, "text", None)
            if text is not None:
                parts.append(str(text))
        return "\n".join(parts).strip()


class Civ6GamePort:
    """Structured HTTP reads plus deterministic MCP actions."""

    def __init__(
        self,
        client: Civ6McpClient,
        state_api: Civ6StateApi,
        *,
        allowed_tools: set[str],
    ):
        self.client = client
        self.state_api = state_api
        self.allowed_tools = allowed_tools

    @property
    def call_count(self) -> int:
        return self.client.call_count + self.state_api.call_count

    async def list_tools(self) -> set[str]:
        return await self.client.list_tools()

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        snapshot_path = (
            "/api/workflow/snapshot?include_units=true"
            if include_units
            else "/api/workflow/snapshot?include_units=false"
        )
        bundled = await self.state_api.get_optional(snapshot_path)
        if isinstance(bundled, dict):
            overview = bundled.get("overview", {})
            tech_civics = bundled.get("tech_civics", {})
            cities = bundled.get("cities", [])
            units = bundled.get("units")
            identity = bundled.get("identity")
            notifications = bundled.get("notifications", [])
            end_turn_blockers = bundled.get("end_turn_blockers", [])
            diplomacy = bundled.get("pending_diplomacy", [])
            trades = bundled.get("pending_trades", [])
        else:
            overview = await self.state_api.get("/api/overview")
            tech_civics = await self.state_api.get_optional("/api/tech-civics")
            cities = await self.state_api.get("/api/cities")
            units = await self.state_api.get("/api/units") if include_units else None
            identity = await self.state_api.get_optional("/api/identity")
            notifications = await self.state_api.get_optional("/api/notifications")
            end_turn_blockers = await self.state_api.get_optional("/api/end-turn-blockers")
            diplomacy = await self.state_api.get_optional("/api/pending-diplomacy")
            trades = await self.state_api.get_optional("/api/pending-trades")

            # Stock upstream lacks the workflow endpoints. Text fallbacks keep
            # blocker detection usable, but core overview/city/unit logic stays JSON.
            if notifications is None:
                notifications = await self.client.call_tool("get_notifications")
            if diplomacy is None:
                diplomacy = await self.client.call_tool("get_pending_diplomacy")
            if trades is None:
                trades = await self.client.call_tool("get_pending_trades")
            if end_turn_blockers is None:
                end_turn_blockers = []
            if tech_civics is None:
                tech_civics = {}

        turn = self._find_int(overview, ("turn", "turn_number", "current_turn"))
        if turn is None:
            raise RuntimeError("structured overview did not expose a turn number")

        if isinstance(identity, dict) and identity.get("seed") is not None:
            game_id = f"{identity.get('civ', overview.get('civ_name', 'unknown'))}:{identity['seed']}"
        else:
            civ = str(overview.get("civ_name", overview.get("civilization", "unknown")))
            leader = str(overview.get("leader_name", overview.get("leader", "unknown")))
            player_id = str(overview.get("player_id", "0"))
            game_id = f"fallback:{civ}:{leader}:{player_id}"

        blockers: list[dict[str, Any]] = []
        if self._has_actionable(notifications):
            blockers.append({"type": "notifications", "data": notifications})
        if self._has_actionable(diplomacy):
            blockers.append({"type": "pending_diplomacy", "data": diplomacy})
        if self._has_actionable(trades):
            blockers.append({"type": "pending_trades", "data": trades})
        if isinstance(end_turn_blockers, list):
            for blocker in end_turn_blockers:
                if not isinstance(blocker, dict):
                    continue
                blockers.append(
                    {
                        "type": "end_turn_blocker",
                        "blocking_type": blocker.get("blocking_type", "UNKNOWN"),
                        "message": blocker.get("message", ""),
                    }
                )
        missing_production = self._cities_without_production(cities)
        if missing_production:
            blockers.append(
                {"type": "city_no_production", "city_ids": missing_production}
            )

        return RuntimeSnapshot(
            turn=turn,
            game_id=game_id,
            overview=self._ensure_dict(overview),
            tech_civics=(
                tech_civics if isinstance(tech_civics, (dict, list)) else {}
            ),
            notifications=notifications,
            diplomacy=diplomacy,
            trades=trades,
            cities=cities,
            units=units,
            blockers=blockers,
        )

    async def execute_task(self, task: StoredTask) -> ActionResult:
        try:
            tool_name, arguments = resolve_action(task, self.allowed_tools)
        except ActionValidationError as exc:
            return ActionResult(success=False, blocked=True, message=str(exc))
        try:
            raw = await self.client.call_tool(tool_name, arguments)
        except Exception as exc:
            return ActionResult(success=False, message=str(exc))
        return self._normalize_action_result(raw)

    async def end_turn(self) -> ActionResult:
        if "end_turn" not in self.allowed_tools:
            return ActionResult(success=False, blocked=True, message="end_turn is not allowed")
        try:
            raw = await self.client.call_tool("end_turn")
        except Exception as exc:
            return ActionResult(success=False, message=str(exc))
        return self._normalize_action_result(raw)

    @staticmethod
    def _normalize_action_result(raw: Any) -> ActionResult:
        if isinstance(raw, dict):
            text = str(raw.get("text", "")).strip()
            lowered = text.lower()
            textual_error = lowered.startswith("error:") or lowered.startswith("failed:")
            if raw.get("success") is False or raw.get("error") or textual_error:
                return ActionResult(
                    success=False,
                    blocked=bool(raw.get("blocked") or raw.get("blocker")),
                    message=str(
                        raw.get("error") or raw.get("message") or text or "action failed"
                    ),
                    details=raw,
                )
            return ActionResult(
                success=True,
                blocked=False,
                message=str(raw.get("message") or text or "ok"),
                details=raw,
            )
        return ActionResult(success=True, message="ok", details={"raw": raw})

    @classmethod
    def _find_int(cls, value: Any, keys: tuple[str, ...]) -> int | None:
        found = cls._find(value, keys)
        if isinstance(found, bool):
            return None
        if isinstance(found, int):
            return found
        if isinstance(found, str) and found.isdigit():
            return int(found)
        return None

    @classmethod
    def _find(cls, value: Any, keys: tuple[str, ...]) -> Any:
        if isinstance(value, dict):
            for key in keys:
                if key in value:
                    return value[key]
            for child in value.values():
                result = cls._find(child, keys)
                if result is not None:
                    return result
        elif isinstance(value, list):
            for child in value:
                result = cls._find(child, keys)
                if result is not None:
                    return result
        return None

    @staticmethod
    def _has_actionable(value: Any) -> bool:
        if value is None or value == {} or value == []:
            return False
        if isinstance(value, list):
            if value and all(isinstance(item, dict) for item in value):
                if any("is_action_required" in item for item in value):
                    return any(bool(item.get("is_action_required")) for item in value)
            return bool(value)
        if isinstance(value, dict):
            if "text" in value:
                text = str(value["text"]).strip().lower()
                negative_markers = (
                    "no active notifications",
                    "no pending",
                    "no open diplomacy",
                    "no trade offers",
                    "none.",
                )
                return bool(text) and not any(marker in text for marker in negative_markers)
            for key in (
                "pending",
                "sessions",
                "offers",
                "action_required",
                "actionRequired",
                "items",
                "notifications",
            ):
                if key in value:
                    candidate = value[key]
                    if isinstance(candidate, list) and key in {"items", "notifications"}:
                        actionable = [
                            item
                            for item in candidate
                            if not isinstance(item, dict)
                            or item.get("is_action_required")
                            or item.get("action_required")
                            or item.get("actionRequired")
                            or item.get("blocking")
                        ]
                        return bool(actionable)
                    return bool(candidate)
            if value.get("count") == 0:
                return False
        return bool(value)

    @staticmethod
    def _cities_without_production(cities: Any) -> list[str]:
        if isinstance(cities, dict):
            candidates = cities.get("cities", cities.get("items", []))
        else:
            candidates = cities
        if not isinstance(candidates, list):
            return []
        missing: list[str] = []
        for city in candidates:
            if not isinstance(city, dict):
                continue
            production = city.get("currently_building", city.get("producing"))
            if production in (None, "", "NONE", "none", "nothing", {}, []):
                city_id = city.get("city_id", city.get("id"))
                if city_id is not None:
                    missing.append(str(city_id))
        return missing

    @staticmethod
    def _ensure_dict(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {"value": value}
