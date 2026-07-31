from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .actions import ActionValidationError, resolve_action
from .domain.observations import SlotState, normalize_slot
from .models import (
    ActionResult,
    MutationDeliveryStatus,
    RuntimeSnapshot,
    TurnActionExecution,
)
from .ports import (
    BoundedGamePort as BoundedGamePort,
    GamePort as GamePort,
    MutationBudget as MutationBudget,
    MutationBudgetExceeded as MutationBudgetExceeded,
)
from .state_api import Civ6StateApi
from .workflow_protocol import (
    READ_ONLY_QUERY_SPECS,
    InformationRequest,
    validate_information_request,
)


@dataclass(slots=True)
class McpServerConfig:
    command: str = "civ-mcp"
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)


_EMPTY_REFLECTIONS_RESPONSE = re.compile(
    r"^Empty reflections: (?:tactical|strategic|tooling|planning|hypothesis)"
    r"(?:, (?:tactical|strategic|tooling|planning|hypothesis))*\. "
    r"Provide non-empty entries for all 5 fields: tactical, strategic, tooling, "
    r"planning, hypothesis\.$"
)
_CANNOT_END_TURN_RESPONSE = re.compile(r"^Cannot end turn: .+$", re.DOTALL)
_TURN_PAUSED_RESPONSE = re.compile(r"^Turn paused(?: —| -|:).+$", re.DOTALL)


class McpToolRejectedError(RuntimeError):
    """The MCP server received the request and explicitly rejected it."""

    def __init__(
        self,
        tool_name: str,
        message: str,
        *,
        rejection_code: str | None = None,
    ):
        super().__init__(message)
        self.tool_name = tool_name
        self.rejection_code = rejection_code


def _windows_civ_mcp_process_ids() -> set[int]:
    if sys.platform != "win32":
        return set()
    command = (
        "$OutputEncoding=[Console]::OutputEncoding="
        "[Text.UTF8Encoding]::new();"
        "$rows=@(Get-CimInstance Win32_Process | Where-Object { "
        "($_.Name -like 'python*.exe' -or $_.Name -like 'civ-mcp*.exe') "
        "-and $_.CommandLine -match "
        "'(?i)(-m\\s+civ_mcp|civ-mcp(?:\\.exe)?)' } | "
        "Select-Object -ExpandProperty ProcessId);"
        "$rows | ConvertTo-Json -Compress"
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError("unable to inspect existing civ6-mcp sidecar processes")
    if not result.stdout.strip():
        return set()
    payload = json.loads(result.stdout)
    values = payload if isinstance(payload, list) else [payload]
    return {int(value) for value in values}


def _terminate_windows_civ_mcp_processes(process_ids: set[int]) -> None:
    remaining = set(process_ids) & _windows_civ_mcp_process_ids()
    for process_id in sorted(remaining):
        subprocess.run(
            ["taskkill", "/PID", str(process_id), "/T", "/F"],
            capture_output=True,
            timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
    if process_ids & _windows_civ_mcp_process_ids():
        raise RuntimeError("unable to terminate owned civ6-mcp sidecar processes")


async def _wait_for_windows_civ_mcp_process_ids() -> set[int]:
    for _ in range(20):
        process_ids = await asyncio.to_thread(_windows_civ_mcp_process_ids)
        if process_ids:
            return process_ids
        await asyncio.sleep(0.1)
    return set()


class _McpSidecarGuard:
    def __init__(
        self,
        identity: str,
        *,
        state_directory: Path | None = None,
        windows: bool | None = None,
    ):
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
        directory = state_directory or Path(tempfile.gettempdir())
        self.lock_path = directory / f"civ6-workflow-mcp-{digest}.lock"
        self.owner_path = directory / f"civ6-workflow-mcp-{digest}.json"
        self.windows = sys.platform == "win32" if windows is None else windows
        self._handle: Any | None = None
        self._owned_process_ids: set[int] = set()

    def __enter__(self) -> "_McpSidecarGuard":
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle: Any | None = None
        try:
            handle = self.lock_path.open("a+b")
            handle.seek(0)
            if handle.read(1) != b"\0":
                handle.seek(0)
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if handle is not None:
                handle.close()
            raise RuntimeError("another Civ6 MCP sidecar owner is active") from exc
        self._handle = handle
        try:
            self._reclaim_stale_owner()
        except BaseException:
            self.close()
            raise
        return self

    def _reclaim_stale_owner(self) -> None:
        if not self.windows:
            return
        existing = _windows_civ_mcp_process_ids()
        recorded: set[int] = set()
        if self.owner_path.exists():
            try:
                payload = json.loads(self.owner_path.read_text(encoding="utf-8"))
                recorded = {int(value) for value in payload.get("process_ids", [])}
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    "civ6-mcp sidecar ownership record is invalid"
                ) from exc
        reclaimable = existing & recorded
        if reclaimable:
            _terminate_windows_civ_mcp_processes(reclaimable)
            existing = _windows_civ_mcp_process_ids()
        if existing:
            raise RuntimeError(
                "an unowned civ6-mcp sidecar is already running; stop it before "
                "starting the Workflow Runtime"
            )
        self.owner_path.unlink(missing_ok=True)

    def record_started(self, process_ids: set[int]) -> None:
        if not self.windows:
            return
        if not process_ids:
            raise RuntimeError("started civ6-mcp sidecar process was not discoverable")
        self._owned_process_ids = set(process_ids)
        temporary = self.owner_path.with_suffix(f".{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(
                {"owner_pid": os.getpid(), "process_ids": sorted(process_ids)},
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        temporary.replace(self.owner_path)

    def close(self) -> None:
        error: BaseException | None = None
        try:
            if self.windows and self._owned_process_ids:
                _terminate_windows_civ_mcp_processes(self._owned_process_ids)
            self.owner_path.unlink(missing_ok=True)
        except BaseException as exc:
            error = exc
        finally:
            handle, self._handle = self._handle, None
            if handle is not None:
                try:
                    handle.seek(0)
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()
        if error is not None:
            raise error

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class Civ6McpClient:
    def __init__(self, config: McpServerConfig):
        self.config = config
        self._stack: AsyncExitStack | None = None
        self._sidecar_guard: _McpSidecarGuard | None = None
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
        identity = "\0".join((self.config.command, *self.config.args))
        self._sidecar_guard = _McpSidecarGuard(identity)
        self._sidecar_guard.__enter__()
        self._stack = AsyncExitStack()
        params = StdioServerParameters(
            command=self.config.command,
            args=self.config.args,
            env={**os.environ, **self.config.env},
        )
        try:
            read, write = await self._stack.enter_async_context(stdio_client(params))
            if sys.platform == "win32":
                self._sidecar_guard.record_started(
                    await _wait_for_windows_civ_mcp_process_ids()
                )
            self.session = await self._stack.enter_async_context(
                ClientSession(read, write)
            )
            await self.session.initialize()
            return self
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if self._stack is not None:
                await self._stack.aclose()
        finally:
            self.session = None
            self._stack = None
            guard, self._sidecar_guard = self._sidecar_guard, None
            if guard is not None:
                guard.close()

    def _require_session(self) -> Any:
        if self.session is None:
            raise RuntimeError("Civ6 MCP client is not connected")
        return self.session

    async def list_tools(self) -> set[str]:
        result = await self._require_session().list_tools()
        return {tool.name for tool in result.tools}

    async def call_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any:
        self.call_count += 1
        result = await self._require_session().call_tool(
            name, arguments=arguments or {}
        )
        is_error = bool(
            getattr(result, "isError", False) or getattr(result, "is_error", False)
        )
        if is_error:
            text = self._extract_text(result.content)
            raise McpToolRejectedError(
                name,
                text or f"MCP tool {name} returned an error",
                rejection_code="mcp_is_error",
            )
        structured = getattr(result, "structuredContent", None)
        if structured is None:
            structured = getattr(result, "structured_content", None)
        if structured is not None:
            return structured
        text = self._extract_text(result.content)
        rejection_code = self._semantic_rejection_code(name, text)
        if rejection_code is not None:
            raise McpToolRejectedError(name, text, rejection_code=rejection_code)
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"text": text}

    @staticmethod
    def _semantic_rejection_code(tool_name: str, text: str) -> str | None:
        """Recognize documented upstream business outcomes at the MCP boundary.

        civ-mcp currently returns these end-turn validation outcomes as successful
        MCP envelopes containing a textual result. Full response grammars keep
        this adapter logic separate from transport exception handling.
        """

        if tool_name != "end_turn" or not text:
            return None
        if _EMPTY_REFLECTIONS_RESPONSE.fullmatch(text):
            return "end_turn_reflections_required"
        if _CANNOT_END_TURN_RESPONSE.fullmatch(text):
            return "end_turn_blocked"
        if _TURN_PAUSED_RESPONSE.fullmatch(text):
            return "end_turn_paused"
        return None

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
            end_turn_blockers = await self.state_api.get_optional(
                "/api/end-turn-blockers"
            )
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

        snapshot = RuntimeSnapshot(
            turn=turn,
            game_id=game_id,
            overview=self._ensure_dict(overview),
            tech_civics=(tech_civics if isinstance(tech_civics, (dict, list)) else {}),
            notifications=notifications,
            diplomacy=diplomacy,
            trades=trades,
            cities=cities,
            units=units,
            blockers=blockers,
        )
        if snapshot.units is not None or not self._has_unit_blocker(snapshot.blockers):
            return snapshot
        units = await self.state_api.get("/api/units")
        return snapshot.model_copy(update={"units": units})

    async def execute_task(self, task: TurnActionExecution) -> ActionResult:
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

    async def end_turn(self, reflections: dict[str, str]) -> ActionResult:
        if "end_turn" not in self.allowed_tools:
            return ActionResult(
                success=False,
                blocked=True,
                message="end_turn is not allowed",
                delivery_status=MutationDeliveryStatus.PROVEN_NOT_SENT,
            )
        try:
            raw = await self.client.call_tool("end_turn", reflections)
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
                message=str(exc),
                details={"error_type": type(exc).__name__},
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
    def _has_unit_blocker(blockers: Any) -> bool:
        return any(
            isinstance(blocker, dict)
            and str(blocker.get("blocking_type", "")) == "ENDTURN_BLOCKING_UNITS"
            for blocker in blockers
        )

    @staticmethod
    def _normalize_action_result(raw: Any) -> ActionResult:
        if isinstance(raw, dict):
            text = str(raw.get("text") or raw.get("result") or "").strip()
            if text.startswith("Cannot connect to Civ 6"):
                return ActionResult(
                    success=False,
                    blocked=True,
                    message=text,
                    details=raw,
                    delivery_status=MutationDeliveryStatus.PROVEN_NOT_SENT,
                )
            if raw.get("success") is False or raw.get("error"):
                return ActionResult(
                    success=False,
                    blocked=bool(raw.get("blocked") or raw.get("blocker")),
                    message=str(
                        raw.get("error")
                        or raw.get("message")
                        or text
                        or "action failed"
                    ),
                    details=raw,
                    delivery_status=MutationDeliveryStatus.EXPLICITLY_REJECTED,
                )
            return ActionResult(
                success=True,
                blocked=False,
                message=str(raw.get("message") or text or "ok"),
                details=raw,
                delivery_status=MutationDeliveryStatus.ACKNOWLEDGED,
            )
        return ActionResult(
            success=True,
            message="ok",
            details={"raw": raw},
            delivery_status=MutationDeliveryStatus.ACKNOWLEDGED,
        )

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
                return bool(text) and not any(
                    marker in text for marker in negative_markers
                )
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
                    if isinstance(candidate, list) and key in {
                        "items",
                        "notifications",
                    }:
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
            if normalize_slot(production).state is SlotState.EMPTY:
                city_id = city.get("city_id", city.get("id"))
                if city_id is not None:
                    missing.append(str(city_id))
        return missing

    @staticmethod
    def _ensure_dict(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {"value": value}
