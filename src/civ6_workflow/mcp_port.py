from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
import socket
import tempfile
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from enum import StrEnum
from uuid import uuid4
from pathlib import Path
from typing import Any

from .actions import ActionValidationError, PreparedAction, prepare_action
from .domain.observations import SlotState, normalize_slot
from .mutation_protocol import (
    DeliveryCategory,
    McpToolEnvelope,
    MutationToolAdapter,
)
from .models import (
    ActionResult,
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
from .workflow_protocol import READ_ONLY_QUERY_SPECS


@dataclass(slots=True)
class McpServerConfig:
    command: str = "civ-mcp"
    args: list[str] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    mutation_timeout_seconds: float = 30.0

    startup_timeout_seconds: float = 30.0
    shutdown_timeout_seconds: float = 10.0
    list_tools_timeout_seconds: float = 10.0
    read_query_timeout_seconds: float = 30.0


_EMPTY_REFLECTIONS_RESPONSE = re.compile(
    r"^Empty reflections: (?:tactical|strategic|tooling|planning|hypothesis)"
    r"(?:, (?:tactical|strategic|tooling|planning|hypothesis))*\. "
    r"Provide non-empty entries for all 5 fields: tactical, strategic, tooling, "
    r"planning, hypothesis\.$"
)
_CANNOT_END_TURN_RESPONSE = re.compile(r"^Cannot end turn: .+$", re.DOTALL)
_TURN_PAUSED_RESPONSE = re.compile(r"^Turn paused(?: —| -|:).+$", re.DOTALL)


class McpClientNotConnectedError(RuntimeError):
    pass


class McpMutationTimeoutError(TimeoutError):
    pass


class McpMutationTransportError(ConnectionError):
    pass


def _is_transport_failure(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionError, EOFError)):
        return True
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "broken pipe",
            "connection closed",
            "connection lost",
            "session closed",
            "stream closed",
            "end of file",
        )
    )


@dataclass(frozen=True, slots=True)
class _WindowsProcessIdentity:
    process_id: int
    parent_process_id: int
    creation_time: str
    executable_path: str
    command_line_hash: str

    def to_json(self) -> dict[str, object]:
        return {
            "process_id": self.process_id,
            "parent_process_id": self.parent_process_id,
            "creation_time": self.creation_time,
            "executable_path": self.executable_path,
            "command_line_hash": self.command_line_hash,
        }

    @classmethod
    def from_json(cls, value: object) -> "_WindowsProcessIdentity":
        if not isinstance(value, dict):
            raise ValueError("process identity must be an object")
        return cls(
            process_id=int(value["process_id"]),
            parent_process_id=int(value["parent_process_id"]),
            creation_time=str(value["creation_time"]),
            executable_path=str(value["executable_path"]),
            command_line_hash=str(value["command_line_hash"]),
        )


def _is_civ_mcp_process(
    *,
    name: str,
    executable_path: str,
    command_line: str,
) -> bool:
    executable_name = Path(executable_path or name).name.casefold()
    if executable_name in {"civ-mcp", "civ-mcp.exe"}:
        return True
    if not executable_name.startswith(("python", "pythonw")):
        return False
    return (
        re.search(
            r"(?:^|\s)-m\s+[\"']?civ_mcp[\"']?(?:\s|$)",
            command_line,
            flags=re.IGNORECASE,
        )
        is not None
    )


def _windows_civ_mcp_processes() -> dict[int, _WindowsProcessIdentity]:
    if sys.platform != "win32":
        return {}
    command = (
        "$OutputEncoding=[Console]::OutputEncoding="
        "[Text.UTF8Encoding]::new();"
        "@(Get-CimInstance Win32_Process | Where-Object { "
        "$_.Name -like 'python*.exe' -or $_.Name -like 'civ-mcp*.exe' } | "
        "Select-Object ProcessId,ParentProcessId,CreationDate,"
        "ExecutablePath,Name,CommandLine) | ConvertTo-Json -Compress"
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
        return {}
    payload = json.loads(result.stdout)
    rows = payload if isinstance(payload, list) else [payload]
    processes: dict[int, _WindowsProcessIdentity] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("Name") or "")
        executable_path = str(row.get("ExecutablePath") or "")
        command_line = str(row.get("CommandLine") or "")
        if not _is_civ_mcp_process(
            name=name,
            executable_path=executable_path,
            command_line=command_line,
        ):
            continue
        process_id = int(row["ProcessId"])
        processes[process_id] = _WindowsProcessIdentity(
            process_id=process_id,
            parent_process_id=int(row.get("ParentProcessId") or 0),
            creation_time=str(row.get("CreationDate") or ""),
            executable_path=os.path.normcase(os.path.abspath(executable_path))
            if executable_path
            else name.casefold(),
            command_line_hash=hashlib.sha256(
                " ".join(command_line.split()).casefold().encode("utf-8")
            ).hexdigest(),
        )
    return processes


def _terminate_windows_sidecar_processes(
    expected: dict[int, _WindowsProcessIdentity],
) -> None:
    current = _windows_civ_mcp_processes()
    mismatched = {
        process_id
        for process_id, identity in expected.items()
        if process_id in current and current[process_id] != identity
    }
    if mismatched:
        raise RuntimeError(
            "recorded civ6-mcp PID identity changed; refusing automatic termination"
        )
    for process_id in sorted(set(expected) & set(current)):
        subprocess.run(
            ["taskkill", "/PID", str(process_id), "/T", "/F"],
            capture_output=True,
            timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
    remaining = _windows_civ_mcp_processes()
    if set(expected) & set(remaining):
        raise RuntimeError("unable to terminate owned civ6-mcp sidecar processes")


async def _wait_for_new_windows_civ_mcp_processes(
    baseline: set[int],
) -> dict[int, _WindowsProcessIdentity]:
    for _ in range(20):
        processes = await asyncio.to_thread(_windows_civ_mcp_processes)
        started = {
            process_id: identity
            for process_id, identity in processes.items()
            if process_id not in baseline and identity.parent_process_id == os.getpid()
        }
        if started:
            return started
        await asyncio.sleep(0.1)
    return {}


def _state_api_port_is_open() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 8000), timeout=0.25):
            return True
    except OSError:
        return False


class _McpSidecarGuard:
    def __init__(
        self,
        identity: str,
        *,
        state_directory: Path | None = None,
        windows: bool | None = None,
    ):
        self.identity_hash = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        directory = state_directory or Path(tempfile.gettempdir())
        self.lock_path = directory / "civ6-workflow-mcp-sidecar.lock"
        self.owner_path = directory / "civ6-workflow-mcp-sidecar.json"
        self.windows = sys.platform == "win32" if windows is None else windows
        self._handle: Any | None = None
        self._baseline_process_ids: set[int] = set()
        self._owned_processes: dict[int, _WindowsProcessIdentity] = {}

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
            self.close(preserve_owner_record=True)
            raise
        return self

    def _reclaim_stale_owner(self) -> None:
        if not self.windows:
            if sys.platform != "win32" and _state_api_port_is_open():
                raise RuntimeError(
                    "an unowned Civ6 State API is already listening on port 8000"
                )
            self.owner_path.unlink(missing_ok=True)
            return
        existing = _windows_civ_mcp_processes()
        recorded: dict[int, _WindowsProcessIdentity] = {}
        if self.owner_path.exists() and existing:
            try:
                payload = json.loads(self.owner_path.read_text(encoding="utf-8"))
                if payload.get("schema_version") != "civ6-sidecar-owner/v2":
                    raise ValueError("unsupported owner record version")
                recorded = {
                    identity.process_id: identity
                    for identity in (
                        _WindowsProcessIdentity.from_json(value)
                        for value in payload.get("processes", [])
                    )
                }
            except (
                OSError,
                ValueError,
                TypeError,
                KeyError,
                json.JSONDecodeError,
            ) as exc:
                raise RuntimeError(
                    "civ6-mcp sidecar ownership record is invalid"
                ) from exc
        reclaimable = {
            process_id: identity
            for process_id, identity in recorded.items()
            if existing.get(process_id) == identity
        }
        if reclaimable:
            _terminate_windows_sidecar_processes(reclaimable)
            existing = _windows_civ_mcp_processes()
        if existing:
            raise RuntimeError(
                "an unowned civ6-mcp sidecar is already running; stop it before "
                "starting the Workflow Runtime"
            )
        self.owner_path.unlink(missing_ok=True)
        self._baseline_process_ids = set(existing)

    def record_started(self, processes: dict[int, _WindowsProcessIdentity]) -> None:
        if not self.windows:
            return
        owned = {
            process_id: identity
            for process_id, identity in processes.items()
            if process_id not in self._baseline_process_ids
            and identity.parent_process_id == os.getpid()
        }
        if not owned:
            raise RuntimeError(
                "started civ6-mcp sidecar process was not attributable to this Runtime"
            )
        self._owned_processes = owned
        temporary = self.owner_path.with_suffix(f".{os.getpid()}.tmp")
        payload = {
            "schema_version": "civ6-sidecar-owner/v2",
            "lease_id": uuid4().hex,
            "runtime_pid": os.getpid(),
            "guard_identity": self.identity_hash,
            "processes": [
                identity.to_json()
                for identity in sorted(owned.values(), key=lambda item: item.process_id)
            ],
        }
        temporary.write_text(
            json.dumps(payload, separators=(",", ":")),
            encoding="utf-8",
        )
        temporary.replace(self.owner_path)

    def close(self, *, preserve_owner_record: bool = False) -> None:
        error: BaseException | None = None
        try:
            if self.windows and self._owned_processes:
                _terminate_windows_sidecar_processes(self._owned_processes)
            if not preserve_owner_record:
                self.owner_path.unlink(missing_ok=True)
            self._owned_processes = {}
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

    async def aclose(self) -> None:
        await asyncio.to_thread(self.close)

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class McpClientState(StrEnum):
    NEW = "NEW"
    STARTING = "STARTING"
    READY = "READY"
    BROKEN = "BROKEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class Civ6McpClient:
    def __init__(self, config: McpServerConfig):
        self.config = config
        self._stack: AsyncExitStack | None = None
        self._sidecar_guard: _McpSidecarGuard | None = None
        self.session: Any | None = None
        self.call_count = 0
        self.state = McpClientState.NEW
        self.session_generation = 0
        self.list_tools_count = 0
        self.read_query_count = 0
        self.mutation_count = 0
        self.mutation_seconds = 0.0
        self.mutation_timeout_count = 0
        self.reconnect_count = 0
        self.session_broken = False

    async def __aenter__(self) -> "Civ6McpClient":
        if self.state not in {McpClientState.NEW, McpClientState.CLOSED}:
            raise RuntimeError(f"Civ6 MCP client cannot start from {self.state}")
        self.state = McpClientState.STARTING
        try:
            async with asyncio.timeout(self.config.startup_timeout_seconds):
                return await self._start()
        except TimeoutError as exc:
            self.state = McpClientState.CLOSED
            raise TimeoutError(
                "Civ6 MCP sidecar startup exceeded "
                f"{self.config.startup_timeout_seconds:g} seconds"
            ) from exc

        except BaseException:
            if self.state is McpClientState.STARTING:
                self.state = McpClientState.CLOSED
            raise

    async def _start(self) -> "Civ6McpClient":

        try:
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "The optional mcp package is required for a live Civ6 connection"
            ) from exc
        identity = "\0".join((self.config.command, *self.config.args))
        self._sidecar_guard = _McpSidecarGuard(identity)
        await asyncio.to_thread(self._sidecar_guard.__enter__)
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
                    await _wait_for_new_windows_civ_mcp_processes(
                        self._sidecar_guard._baseline_process_ids
                    )
                )
            self.session = await self._stack.enter_async_context(
                ClientSession(read, write)
            )
            await self.session.initialize()
            self.session_broken = False
            self.session_generation += 1
            self.state = McpClientState.READY
            return self
        except BaseException as exc:
            try:
                await self.__aexit__(None, None, None)
            except BaseException as cleanup_error:
                exc.add_note(f"Civ6 MCP startup cleanup failed: {cleanup_error!r}")
            raise

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if (
            self.state is McpClientState.CLOSED
            and self._stack is None
            and self._sidecar_guard is None
        ):
            return
        self.state = McpClientState.CLOSING
        stack, self._stack = self._stack, None
        guard, self._sidecar_guard = self._sidecar_guard, None
        close_task: asyncio.Task[None] | None = None
        errors: list[BaseException] = []
        try:
            if stack is not None:
                close_task = asyncio.create_task(stack.aclose())
                done, _pending = await asyncio.wait(
                    {close_task},
                    timeout=self.config.shutdown_timeout_seconds,
                )
                if done:
                    try:
                        close_task.result()
                    except BaseException as close_error:
                        errors.append(close_error)
                else:
                    close_task.cancel()
            if guard is not None:
                try:
                    await guard.aclose()
                except BaseException as guard_error:
                    errors.append(guard_error)
            if close_task is not None and not close_task.done():
                done, _pending = await asyncio.wait({close_task}, timeout=1.0)
                if done:
                    await asyncio.gather(close_task, return_exceptions=True)
                else:
                    close_task.add_done_callback(
                        lambda task: task.exception() if not task.cancelled() else None
                    )
        finally:
            self.session = None
            self.session_broken = False
            self.state = McpClientState.CLOSED
        if errors:
            raise BaseExceptionGroup("Civ6 MCP sidecar cleanup failed", errors)

    def _mark_broken(self) -> None:
        self.session_broken = True
        self.state = McpClientState.BROKEN

    def _require_session(self) -> Any:
        if (
            self.session is None
            or self.session_broken
            or self.state is not McpClientState.READY
        ):
            raise McpClientNotConnectedError(
                "Civ6 MCP client is not connected to a healthy Session"
            )
        return self.session

    async def list_tools(self) -> set[str]:
        session = self._require_session()
        self.call_count += 1
        self.list_tools_count += 1
        try:
            async with asyncio.timeout(self.config.list_tools_timeout_seconds):
                result = await session.list_tools()
        except BaseException as exc:
            if isinstance(
                exc, (TimeoutError, asyncio.CancelledError)
            ) or _is_transport_failure(exc):
                self._mark_broken()
            raise
        return {tool.name for tool in result.tools}

    async def call_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any:
        session = self._require_session()
        self.call_count += 1
        self.read_query_count += 1
        try:
            async with asyncio.timeout(self.config.read_query_timeout_seconds):
                result = await session.call_tool(name, arguments=arguments or {})
        except BaseException as exc:
            if isinstance(
                exc, (TimeoutError, asyncio.CancelledError)
            ) or _is_transport_failure(exc):
                self._mark_broken()
            raise
        envelope = McpToolEnvelope.from_sdk_result(name, result)
        if envelope.is_error:
            raise RuntimeError(
                envelope.text_content or f"MCP query tool {name} returned an error"
            )
        if envelope.structured_content is not None:
            return envelope.structured_content
        if not envelope.text_content:
            return {}
        try:
            return json.loads(envelope.text_content)
        except json.JSONDecodeError:
            return {"text": envelope.text_content}

    async def call_mutation_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
    ) -> McpToolEnvelope:
        session = self._require_session()
        self.call_count += 1
        self.mutation_count += 1
        started = asyncio.get_running_loop().time()
        try:
            async with asyncio.timeout(self.config.mutation_timeout_seconds):
                result = await session.call_tool(name, arguments=arguments or {})
        except TimeoutError as exc:
            self.mutation_timeout_count += 1
            self._mark_broken()
            raise McpMutationTimeoutError(
                f"MCP mutation {name} exceeded "
                f"{self.config.mutation_timeout_seconds:g} seconds"
            ) from exc
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                self._mark_broken()
                raise
            if _is_transport_failure(exc):
                self._mark_broken()
                raise McpMutationTransportError(f"{type(exc).__name__}: {exc}") from exc
            raise
        finally:
            self.mutation_seconds += asyncio.get_running_loop().time() - started
        return McpToolEnvelope.from_sdk_result(name, result)

    async def reconnect_if_broken(self) -> bool:
        if not self.session_broken and self.state is not McpClientState.BROKEN:
            return False
        await self.__aexit__(None, None, None)
        await self.__aenter__()
        self.reconnect_count += 1
        return True


class SnapshotCapability(StrEnum):
    UNKNOWN = "UNKNOWN"
    BUNDLED = "BUNDLED"
    LEGACY = "LEGACY"


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
        self._mutation_adapter = MutationToolAdapter()
        self._snapshot_capability = SnapshotCapability.UNKNOWN
        self._snapshot_capability_epoch = self._availability_epoch()

    @property
    def call_count(self) -> int:
        return self.client.call_count + self.state_api.call_count

    @property
    def tool_surface_epoch(self) -> int:
        return int(getattr(self.client, "session_generation", id(self.client.session)))

    async def list_tools(self) -> set[str]:
        return await self.client.list_tools()

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        self._refresh_snapshot_capability()
        if self._snapshot_capability is not SnapshotCapability.LEGACY:
            snapshot_path = (
                "/api/workflow/snapshot?include_units=true"
                if include_units
                else "/api/workflow/snapshot?include_units=false"
            )
            bundled = await self.state_api.get_optional(snapshot_path)
            if isinstance(bundled, dict):
                self._snapshot_capability = SnapshotCapability.BUNDLED
                return await self._upgrade_units_if_required(
                    self._read_bundled_snapshot(bundled, include_units=include_units),
                    include_units=include_units,
                )
            if bundled is not None:
                raise RuntimeError("structured workflow snapshot must be an object")
            self._snapshot_capability = SnapshotCapability.LEGACY
        return await self._upgrade_units_if_required(
            await self._read_legacy_snapshot(include_units=include_units),
            include_units=include_units,
        )

    def _availability_epoch(self) -> int:
        return int(getattr(self.state_api, "availability_epoch", 0))

    def _refresh_snapshot_capability(self) -> None:
        epoch = self._availability_epoch()
        if epoch != self._snapshot_capability_epoch:
            self._snapshot_capability = SnapshotCapability.UNKNOWN
            self._snapshot_capability_epoch = epoch

    async def _upgrade_units_if_required(
        self,
        snapshot: RuntimeSnapshot,
        *,
        include_units: bool,
    ) -> RuntimeSnapshot:
        if snapshot.units is not None or not self._has_unit_blocker(snapshot.blockers):
            return snapshot
        if self._snapshot_capability is SnapshotCapability.BUNDLED:
            if include_units:
                raise RuntimeError(
                    "structured workflow snapshot omitted requested unit details"
                )
            return await self.read_snapshot(include_units=True)
        units = await self.state_api.get("/api/units")
        self._validate_collection(units, "units")
        overview = await self.state_api.get("/api/overview")
        identity = await self.state_api.get_optional("/api/identity")
        if (
            self._extract_turn(overview) != snapshot.turn
            or self._resolve_game_session_id(identity, overview) != snapshot.game_id
        ):
            return await self._read_legacy_snapshot(include_units=True)
        return snapshot.model_copy(update={"units": units})

    def _read_bundled_snapshot(
        self,
        bundled: dict[str, Any],
        *,
        include_units: bool,
    ) -> RuntimeSnapshot:
        if "overview" not in bundled:
            raise RuntimeError("structured workflow snapshot omitted overview")
        overview = bundled["overview"]
        self._validate_mapping(overview, "overview")
        tech_civics_loaded = "tech_civics" in bundled
        cities_loaded = "cities" in bundled
        notifications_loaded = "notifications" in bundled
        diplomacy_loaded = "pending_diplomacy" in bundled
        trades_loaded = "pending_trades" in bundled
        end_turn_blockers_loaded = "end_turn_blockers" in bundled
        cities = self._collection_or_empty(
            bundled.get("cities"), cities_loaded, "cities"
        )
        notifications = self._collection_or_empty(
            bundled.get("notifications"), notifications_loaded, "notifications"
        )
        diplomacy = self._collection_or_empty(
            bundled.get("pending_diplomacy"), diplomacy_loaded, "pending_diplomacy"
        )
        trades = self._collection_or_empty(
            bundled.get("pending_trades"), trades_loaded, "pending_trades"
        )
        end_turn_blockers = self._list_or_empty(
            bundled.get("end_turn_blockers"),
            end_turn_blockers_loaded,
            "end_turn_blockers",
        )
        tech_civics = bundled.get("tech_civics", {})
        self._validate_mapping(tech_civics, "tech_civics")
        identity = bundled.get("identity")
        self._validate_optional_mapping(identity, "identity")
        units = bundled.get("units") if include_units and "units" in bundled else None
        if units is not None:
            self._validate_collection(units, "units")
        return self._build_runtime_snapshot(
            overview=overview,
            tech_civics=tech_civics,
            cities=cities,
            units=units,
            identity=identity,
            notifications=notifications,
            diplomacy=diplomacy,
            trades=trades,
            end_turn_blockers=end_turn_blockers,
            tech_civics_loaded=tech_civics_loaded,
            cities_loaded=cities_loaded,
            notifications_loaded=notifications_loaded,
            diplomacy_loaded=diplomacy_loaded,
            trades_loaded=trades_loaded,
            end_turn_blockers_loaded=end_turn_blockers_loaded,
        )

    async def _read_legacy_snapshot(self, *, include_units: bool) -> RuntimeSnapshot:
        for _ in range(2):
            overview_before = await self.state_api.get("/api/overview")
            identity_before = await self.state_api.get_optional("/api/identity")
            self._validate_mapping(overview_before, "overview")
            self._validate_optional_mapping(identity_before, "identity")
            self._extract_turn(overview_before)
            (
                tech_civics,
                cities,
                units,
                notifications,
                end_turn_blockers,
                diplomacy,
                trades,
            ) = await asyncio.gather(
                self.state_api.get_optional("/api/tech-civics"),
                self.state_api.get("/api/cities"),
                self.state_api.get("/api/units") if include_units else self._none(),
                self.state_api.get_optional("/api/notifications"),
                self.state_api.get_optional("/api/end-turn-blockers"),
                self.state_api.get_optional("/api/pending-diplomacy"),
                self.state_api.get_optional("/api/pending-trades"),
            )
            tech_civics_loaded = tech_civics is not None
            notifications_loaded = notifications is not None
            diplomacy_loaded = diplomacy is not None
            trades_loaded = trades is not None
            end_turn_blockers_loaded = end_turn_blockers is not None
            if notifications is None:
                notifications = await self.client.call_tool("get_notifications")
                notifications_loaded = notifications is not None
            if diplomacy is None:
                diplomacy = await self.client.call_tool("get_pending_diplomacy")
                diplomacy_loaded = diplomacy is not None
            if trades is None:
                trades = await self.client.call_tool("get_pending_trades")
                trades_loaded = trades is not None
            overview_after = await self.state_api.get("/api/overview")
            identity_after = await self.state_api.get_optional("/api/identity")
            self._validate_mapping(overview_after, "overview")
            self._validate_optional_mapping(identity_after, "identity")
            if not self._same_legacy_session(
                overview_before,
                identity_before,
                overview_after,
                identity_after,
            ):
                continue
            self._validate_collection(cities, "cities")
            self._validate_optional_mapping(tech_civics, "tech_civics")
            self._validate_optional_collection(notifications, "notifications")
            self._validate_optional_collection(diplomacy, "pending_diplomacy")
            self._validate_optional_collection(trades, "pending_trades")
            self._validate_optional_list(end_turn_blockers, "end_turn_blockers")
            if units is not None:
                self._validate_collection(units, "units")
            return self._build_runtime_snapshot(
                overview=overview_after,
                tech_civics=tech_civics or {},
                cities=cities,
                units=units,
                identity=identity_after
                if identity_after is not None
                else identity_before,
                notifications=notifications or [],
                diplomacy=diplomacy or [],
                trades=trades or [],
                end_turn_blockers=end_turn_blockers or [],
                tech_civics_loaded=tech_civics_loaded,
                cities_loaded=True,
                notifications_loaded=notifications_loaded,
                diplomacy_loaded=diplomacy_loaded,
                trades_loaded=trades_loaded,
                end_turn_blockers_loaded=end_turn_blockers_loaded,
            )
        raise RuntimeError("legacy snapshot changed while its fields were being read")

    @staticmethod
    async def _none() -> None:
        return None

    def _build_runtime_snapshot(
        self,
        *,
        overview: dict[str, Any],
        tech_civics: dict[str, Any],
        cities: dict[str, Any] | list[Any],
        units: dict[str, Any] | list[Any] | None,
        identity: dict[str, Any] | None,
        notifications: dict[str, Any] | list[Any],
        diplomacy: dict[str, Any] | list[Any],
        trades: dict[str, Any] | list[Any],
        end_turn_blockers: list[Any],
        tech_civics_loaded: bool,
        cities_loaded: bool,
        notifications_loaded: bool,
        diplomacy_loaded: bool,
        trades_loaded: bool,
        end_turn_blockers_loaded: bool,
    ) -> RuntimeSnapshot:
        missing_production, production_loaded = self._cities_without_production(
            cities,
            loaded=cities_loaded,
        )
        blockers: list[dict[str, Any]] = []
        if notifications_loaded and self._has_actionable(notifications):
            blockers.append({"type": "notifications", "data": notifications})
        if diplomacy_loaded and self._has_actionable(diplomacy):
            blockers.append({"type": "pending_diplomacy", "data": diplomacy})
        if trades_loaded and self._has_actionable(trades):
            blockers.append({"type": "pending_trades", "data": trades})
        if end_turn_blockers_loaded:
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
        if missing_production:
            blockers.append(
                {"type": "city_no_production", "city_ids": missing_production}
            )
        return RuntimeSnapshot(
            turn=self._extract_turn(overview),
            game_id=self._resolve_game_session_id(identity, overview),
            overview=overview,
            tech_civics=tech_civics,
            notifications=notifications,
            diplomacy=diplomacy,
            trades=trades,
            cities=cities,
            units=units,
            blockers=blockers,
            tech_civics_loaded=tech_civics_loaded,
            cities_loaded=cities_loaded,
            notifications_loaded=notifications_loaded,
            diplomacy_loaded=diplomacy_loaded,
            trades_loaded=trades_loaded,
            end_turn_blockers_loaded=end_turn_blockers_loaded,
            blockers_loaded=(
                cities_loaded
                and production_loaded
                and notifications_loaded
                and diplomacy_loaded
                and trades_loaded
                and end_turn_blockers_loaded
            ),
        )

    @staticmethod
    def _collection_or_empty(
        value: Any, loaded: bool, name: str
    ) -> dict[str, Any] | list[Any]:
        if not loaded:
            return []
        Civ6GamePort._validate_collection(value, name)
        return value

    @staticmethod
    def _list_or_empty(value: Any, loaded: bool, name: str) -> list[Any]:
        if not loaded:
            return []
        Civ6GamePort._validate_optional_list(value, name)
        return value

    @staticmethod
    def _validate_mapping(value: Any, name: str) -> None:
        if not isinstance(value, dict):
            raise RuntimeError(f"structured {name} must be an object")

    @staticmethod
    def _validate_optional_mapping(value: Any, name: str) -> None:
        if value is not None:
            Civ6GamePort._validate_mapping(value, name)

    @staticmethod
    def _validate_collection(value: Any, name: str) -> None:
        if not isinstance(value, (dict, list)):
            raise RuntimeError(f"structured {name} must be a list or object")

    @staticmethod
    def _validate_optional_collection(value: Any, name: str) -> None:
        if value is not None:
            Civ6GamePort._validate_collection(value, name)

    @staticmethod
    def _validate_optional_list(value: Any, name: str) -> None:
        if not isinstance(value, list):
            raise RuntimeError(f"structured {name} must be a list")

    def _same_legacy_session(
        self,
        overview_before: dict[str, Any],
        identity_before: dict[str, Any] | None,
        overview_after: dict[str, Any],
        identity_after: dict[str, Any] | None,
    ) -> bool:
        return self._extract_turn(overview_before) == self._extract_turn(
            overview_after
        ) and self._resolve_game_session_id(
            identity_before, overview_before
        ) == self._resolve_game_session_id(identity_after, overview_after)

    @staticmethod
    def _extract_turn(overview: dict[str, Any]) -> int:
        for key in ("turn", "turn_number", "current_turn"):
            value = overview.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, int) and value >= 0:
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        raise RuntimeError("structured overview did not expose a turn number")

    @classmethod
    def _resolve_game_session_id(
        cls,
        identity: dict[str, Any] | None,
        overview: dict[str, Any],
    ) -> str:
        identity = identity or {}
        civ = str(
            identity.get("civ")
            or overview.get("civ_name")
            or overview.get("civilization")
            or "unknown"
        )
        player_id = str(identity.get("player_id") or overview.get("player_id") or "0")
        sources = (identity, overview)
        for key in (
            "game_uuid",
            "game_id",
            "save_id",
            "save_identity",
            "map_seed",
            "game_seed",
            "seed",
        ):
            for source in sources:
                value = source.get(key)
                if isinstance(value, bool) or value is None:
                    continue
                if isinstance(value, (str, int)) and str(value).strip():
                    if key == "seed":
                        return f"{civ}:{value}"
                    payload = json.dumps(
                        {
                            "key": key,
                            "value": str(value).strip(),
                            "civ": civ,
                            "player_id": player_id,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    return f"session:{key}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"
        raise RuntimeError(
            "structured state did not expose a stable game session identity"
        )

    def preflight_mutation(self, tool_name: str) -> None:
        if tool_name not in self.allowed_tools:
            raise ActionValidationError(f"tool is not allowed: {tool_name}")
        if getattr(self.client, "session_broken", False):
            raise McpClientNotConnectedError(
                "Civ6 MCP mutation session is marked broken"
            )
        if hasattr(self.client, "session") and self.client.session is None:
            raise McpClientNotConnectedError("Civ6 MCP client is not connected")

    async def recover_mutation_session(self) -> bool:
        reconnect = getattr(self.client, "reconnect_if_broken", None)
        if reconnect is None:
            return False
        recovered = await reconnect()
        if recovered:
            self._snapshot_capability = SnapshotCapability.UNKNOWN
        return bool(recovered)

    @property
    def call_metrics(self) -> dict[str, float | int]:
        return {
            "state_api_call_count": self.state_api.call_count,
            "mcp_list_tools_count": getattr(self.client, "list_tools_count", 0),
            "mcp_read_query_count": getattr(self.client, "read_query_count", 0),
            "mcp_mutation_count": getattr(self.client, "mutation_count", 0),
            "mcp_mutation_seconds": getattr(self.client, "mutation_seconds", 0.0),
            "mcp_timeout_count": getattr(self.client, "mutation_timeout_count", 0),
            "mcp_reconnect_count": getattr(self.client, "reconnect_count", 0),
        }

    async def execute_task(self, task: TurnActionExecution) -> ActionResult:
        try:
            prepared = prepare_action(task, self.allowed_tools)
            self.preflight_mutation(prepared.tool_name)
        except (ActionValidationError, McpClientNotConnectedError) as exc:
            category = (
                DeliveryCategory.GAME_UNAVAILABLE
                if isinstance(exc, McpClientNotConnectedError)
                else DeliveryCategory.LOCAL_REJECTED
            )
            return self._mutation_adapter.local_failure(
                tool_name=task.action_type,
                category=category,
                message=str(exc),
                exception_type=type(exc).__name__,
            )
        return await self.execute_prepared_action(prepared, task)

    async def execute_prepared_action(
        self,
        prepared: PreparedAction,
        task: TurnActionExecution,
    ) -> ActionResult:
        if prepared.action_type != task.action_type:
            return self._mutation_adapter.local_failure(
                tool_name=prepared.tool_name,
                category=DeliveryCategory.LOCAL_REJECTED,
                message="prepared action type does not match task",
            )
        try:
            self.preflight_mutation(prepared.tool_name)
        except (ActionValidationError, McpClientNotConnectedError) as exc:
            category = (
                DeliveryCategory.GAME_UNAVAILABLE
                if isinstance(exc, McpClientNotConnectedError)
                else DeliveryCategory.LOCAL_REJECTED
            )
            return self._mutation_adapter.local_failure(
                tool_name=prepared.tool_name,
                category=category,
                message=str(exc),
                exception_type=type(exc).__name__,
            )
        return await self._invoke_mutation_tool(
            prepared.tool_name,
            dict(prepared.normalized_arguments),
        )

    async def end_turn(self, reflections: dict[str, str]) -> ActionResult:
        try:
            self.preflight_mutation("end_turn")
        except (ActionValidationError, McpClientNotConnectedError) as exc:
            category = (
                DeliveryCategory.GAME_UNAVAILABLE
                if isinstance(exc, McpClientNotConnectedError)
                else DeliveryCategory.LOCAL_REJECTED
            )
            return self._mutation_adapter.local_failure(
                tool_name="end_turn",
                category=category,
                message=str(exc),
                exception_type=type(exc).__name__,
            )
        return await self._invoke_mutation_tool("end_turn", reflections)

    async def _invoke_mutation_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> ActionResult:
        try:
            call_mutation = getattr(self.client, "call_mutation_tool", None)
            raw = (
                await call_mutation(tool_name, arguments)
                if call_mutation is not None
                else await self.client.call_tool(tool_name, arguments)
            )
        except McpClientNotConnectedError as exc:
            return self._mutation_adapter.local_failure(
                tool_name=tool_name,
                category=DeliveryCategory.GAME_UNAVAILABLE,
                message=str(exc),
                exception_type=type(exc).__name__,
            )
        except McpMutationTimeoutError as exc:
            return self._mutation_adapter.local_failure(
                tool_name=tool_name,
                category=DeliveryCategory.MCP_CALL_TIMEOUT,
                message=str(exc),
                exception_type=type(exc).__name__,
            )
        except McpMutationTransportError as exc:
            return self._mutation_adapter.local_failure(
                tool_name=tool_name,
                category=DeliveryCategory.MCP_TRANSPORT_ERROR,
                message=str(exc),
                exception_type=type(exc).__name__,
            )
        except Exception as exc:
            return self._mutation_adapter.local_failure(
                tool_name=tool_name,
                category=DeliveryCategory.TOOL_EXECUTION_ERROR,
                message=f"{type(exc).__name__}: {exc}",
                exception_type=type(exc).__name__,
            )
        envelope = McpToolEnvelope.from_legacy_payload(tool_name, raw)
        return self._mutation_adapter.interpret(envelope)

    async def query_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any:
        if name not in READ_ONLY_QUERY_SPECS:
            raise RuntimeError(f"read-only workflow query is not allowed: {name}")
        return await self.client.call_tool(name, arguments or {})

    @staticmethod
    def _normalize_action_result(
        raw: Any,
        *,
        tool_name: str = "unit_action",
    ) -> ActionResult:
        return MutationToolAdapter().interpret(
            McpToolEnvelope.from_legacy_payload(tool_name, raw)
        )

    @staticmethod
    def _has_unit_blocker(blockers: Any) -> bool:
        return any(
            isinstance(blocker, dict)
            and str(blocker.get("blocking_type", "")) == "ENDTURN_BLOCKING_UNITS"
            for blocker in blockers
        )

    @staticmethod
    def _has_actionable(value: Any) -> bool:
        if value is None or value == {} or value == []:
            return False
        if isinstance(value, list):
            return any(Civ6GamePort._is_actionable_item(item) for item in value)
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
                    return Civ6GamePort._has_actionable(value[key])
            if value.get("count") == 0:
                return False
        return bool(value)

    @staticmethod
    def _is_actionable_item(value: Any) -> bool:
        if not isinstance(value, dict):
            return bool(value)
        flags = (
            "is_action_required",
            "action_required",
            "actionRequired",
            "blocking",
        )
        present = [key for key in flags if key in value]
        return any(bool(value[key]) for key in present) if present else bool(value)

    @staticmethod
    def _cities_without_production(
        cities: Any,
        *,
        loaded: bool,
    ) -> tuple[list[str], bool]:
        if not loaded:
            return [], False
        if isinstance(cities, dict):
            if "cities" in cities:
                candidates = cities["cities"]
            elif "items" in cities:
                candidates = cities["items"]
            else:
                return [], False
        else:
            candidates = cities
        if not isinstance(candidates, list):
            return [], False
        missing: list[str] = []
        production_loaded = True
        for city in candidates:
            if not isinstance(city, dict):
                production_loaded = False
                continue
            field_loaded = "currently_building" in city or "producing" in city
            if not field_loaded:
                production_loaded = False
                continue
            production = city.get("currently_building", city.get("producing"))
            if normalize_slot(production, loaded=True).state is SlotState.EMPTY:
                city_id = city.get("city_id", city.get("id"))
                if city_id is not None:
                    missing.append(str(city_id))
        return missing, production_loaded
