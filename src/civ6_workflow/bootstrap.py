from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .actions import canonical_action_types
from .batch_executor import BatchExecutor
from .codex_planner import CodexPlanner
from .conditions import ConditionEvaluator
from .config import AppConfig
from .runtime import RuntimeConfig, RuntimeServices, WorkflowRuntime
from .gate import EventGate, GateConfig
from .mcp_port import Civ6GamePort, Civ6McpClient
from .planner_lifecycle import PlannerLifecycleCoordinator, PlannerLifecycleRuntime
from .ports import GamePort, Planner, ReadOnlyGameQueryPortView
from .models import ExecutionMode, RuntimeSnapshot
from .replay import (
    RecordingGamePort,
    RecordingPlanner,
    ReplayEngineSettings,
    ReplayGamePort,
    ReplayPlanner,
    SnapshotRecording,
)
from .state_api import Civ6StateApi
from .store import WorkflowStore
from .strategic_workflow import StrategicWorkflowCoordinator
from .turn_compiler import TurnCompiler
from .web_ui import ControlPanelHTTPServer, ControlPanelState
from .workflow_queries import InformationQueryRouter


@dataclass(frozen=True, slots=True)
class RuntimeComposition:
    """The explicit object graph used by every workflow runtime entry point."""

    store: WorkflowStore
    game: GamePort
    planner: Planner
    runtime: WorkflowRuntime


@dataclass(frozen=True, slots=True)
class ControlPanelComposition:
    store: WorkflowStore
    control: ControlPanelState
    server: ControlPanelHTTPServer


def resolve_database_path(config: AppConfig, config_path: str | Path) -> Path:
    path = Path(config.runtime.database_path)
    if path.is_absolute():
        return path
    return Path(config_path).parent / path


def build_store(config: AppConfig, config_path: str | Path) -> WorkflowStore:
    return WorkflowStore(
        resolve_database_path(config, config_path),
        enable_phase1c_decisions=True,
    )


def build_runtime_services(runtime: WorkflowRuntime) -> RuntimeServices:
    """Build the one application-service graph used by every Runtime entry point."""

    conditions = ConditionEvaluator()
    read_only_game = ReadOnlyGameQueryPortView(runtime.game)
    information_queries = InformationQueryRouter(read_only_game)
    planner_lifecycle = PlannerLifecycleCoordinator(
        PlannerLifecycleRuntime(
            store=runtime.store,
            game=read_only_game,
            planner=runtime.planner,
            config=runtime.config,
            conditions=conditions,
            information_queries=information_queries,
            now=lambda: runtime._now(),
            monotonic=lambda: runtime._monotonic(),
            checkpoint=lambda name: runtime._checkpoint(name),
            observation_id=lambda: runtime._active_observation_id,
            human_wait_context=lambda snapshot: runtime._human_wait_context(snapshot),
            available_tools=lambda: runtime._available_tools or set(),
        )
    )
    return RuntimeServices(
        gate=EventGate(
            runtime.store,
            GateConfig(
                default_cooldown_turns=max(
                    0, int(runtime.config.default_cooldown_turns)
                )
            ),
        ),
        conditions=conditions,
        batch_executor=BatchExecutor(
            store=runtime.store,
            game=runtime.game,
            conditions=conditions,
            allowed_action_types=runtime.config.allowed_action_types,
            allowed_tools=runtime.config.allowed_tools,
            verification_attempts=runtime.config.verification_attempts,
            now=lambda: runtime._now(),
            monotonic=lambda: runtime._monotonic(),
            checkpoint=lambda name: runtime._checkpoint(name),
        ),
        information_queries=information_queries,
        strategic_workflow=StrategicWorkflowCoordinator(
            store=runtime.store,
            planner_lifecycle=planner_lifecycle,
            turn_compiler=TurnCompiler(),
        ),
    )


def compose_runtime(
    *,
    store: WorkflowStore,
    game: GamePort,
    planner: Planner,
    runtime_config: RuntimeConfig | None = None,
    clock: Any | None = None,
    crash_injector: Any | None = None,
) -> RuntimeComposition:
    runtime = WorkflowRuntime(
        store=store,
        game=game,
        planner=planner,
        config=runtime_config,
        clock=clock,
        crash_injector=crash_injector,
        service_factory=build_runtime_services,
    )
    return RuntimeComposition(store=store, game=game, planner=planner, runtime=runtime)


def compose_live_runtime(
    config: AppConfig,
    config_path: str | Path,
    client: Civ6McpClient,
    state_api: Civ6StateApi,
    *,
    store: WorkflowStore | None = None,
    planner_base_directory: str | Path | None = None,
) -> RuntimeComposition:
    store = store or build_store(config, config_path)
    game = Civ6GamePort(
        client,
        state_api,
        allowed_tools=set(config.safety.allowed_tools),
    )
    planner = CodexPlanner(config.codex_config(planner_base_directory))
    return compose_runtime(
        store=store,
        game=game,
        planner=planner,
        runtime_config=config.runtime_config(),
    )


@asynccontextmanager
async def open_live_runtime(
    config: AppConfig,
    config_path: str | Path,
    *,
    store: WorkflowStore | None = None,
    planner_base_directory: str | Path | None = None,
) -> AsyncIterator[RuntimeComposition]:
    async with Civ6McpClient(config.mcp_config()) as client:
        async with Civ6StateApi(config.state_api_config()) as state_api:
            yield compose_live_runtime(
                config,
                config_path,
                client,
                state_api,
                store=store,
                planner_base_directory=planner_base_directory,
            )


def compose_recording_runtime(
    config: AppConfig,
    config_path: str | Path,
    client: Civ6McpClient,
    state_api: Civ6StateApi,
    recording: SnapshotRecording,
    *,
    store: WorkflowStore | None = None,
    on_first_snapshot: Callable[[RuntimeSnapshot], None] | None = None,
) -> RuntimeComposition:
    live = compose_live_runtime(
        config,
        config_path,
        client,
        state_api,
        store=store,
    )
    game = RecordingGamePort(
        live.game,
        recording,
        on_first_snapshot=on_first_snapshot,
    )
    planner = RecordingPlanner(live.planner, recording)
    return compose_runtime(
        store=live.store,
        game=game,
        planner=planner,
        runtime_config=config.runtime_config(),
    )


def replay_runtime_config(
    recording: SnapshotRecording,
    *,
    auto_end_turn: bool,
) -> RuntimeConfig:
    settings: ReplayEngineSettings | None = recording.engine_settings
    action_types = (
        set(settings.allowed_action_types)
        if settings
        else set(canonical_action_types())
    )
    auto_action_types = (
        set(settings.auto_action_types) if settings else set(canonical_action_types())
    )
    return RuntimeConfig(
        execution_mode=settings.execution_mode if settings else ExecutionMode.AUTO,
        auto_end_turn=auto_end_turn,
        max_agent_calls_per_turn=(settings.max_agent_calls_per_turn if settings else 1),
        repeated_failure_threshold=(
            settings.repeated_failure_threshold if settings else 2
        ),
        verification_attempts=settings.verification_attempts if settings else 3,
        verification_delay_seconds=(
            settings.verification_delay_seconds if settings else 0.25
        ),
        auto_action_types=auto_action_types,
        allowed_action_types=action_types,
        allowed_tools=set(settings.allowed_tools) if settings else set(recording.tools),
    )


def compose_replay_runtime(
    recording: SnapshotRecording,
    database: str | Path,
    *,
    auto_end_turn: bool = False,
) -> RuntimeComposition:
    if not recording.frames:
        raise ValueError("recording contains no snapshot frames")
    store = WorkflowStore(database, enable_phase1c_decisions=True)
    if recording.store_state is not None:
        store.import_replay_state(recording.store_state)
    config = replay_runtime_config(recording, auto_end_turn=auto_end_turn)
    if recording.seed_plans:
        raise ValueError(
            "legacy replay seed plans require migration to persisted "
            "StrategicContract and TurnActionGraph state"
        )
    game = ReplayGamePort(recording)
    planner = ReplayPlanner(recording)
    return compose_runtime(
        store=store,
        game=game,
        planner=planner,
        runtime_config=config,
    )


def compose_control_panel(
    config: AppConfig,
    config_path: str | Path,
    *,
    address: tuple[str, int],
    run_tick_callback: Callable[[], Any],
) -> ControlPanelComposition:
    store = build_store(config, config_path)
    control = ControlPanelState(
        config=config,
        store=store,
        run_tick_callback=run_tick_callback,
    )
    server = ControlPanelHTTPServer(address, control)
    return ControlPanelComposition(store=store, control=control, server=server)
