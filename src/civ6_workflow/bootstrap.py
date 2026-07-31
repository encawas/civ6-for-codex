from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .actions import ACTION_REGISTRY
from .batch_executor import BatchExecutor
from .codex_planner import CodexPlanner
from .conditions import ConditionEvaluator
from .config import AppConfig
from .engine import EngineConfig, RuntimeServices, WorkflowEngine
from .gate import EventGate, GateConfig
from .mcp_port import Civ6GamePort, Civ6McpClient
from .planner_lifecycle import PlannerLifecycleCoordinator, PlannerLifecycleRuntime
from .ports import GamePort, Planner
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
from .turn_compiler import TurnCompiler
from .web_ui import ControlPanelHTTPServer, ControlPanelState
from .workflow_queries import InformationQueryRouter


@dataclass(frozen=True, slots=True)
class RuntimeComposition:
    """The explicit object graph used by every workflow runtime entry point."""

    store: WorkflowStore
    game: GamePort
    planner: Planner
    engine: WorkflowEngine


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


def build_runtime_services(engine: WorkflowEngine) -> RuntimeServices:
    """Build the one application-service graph used by every Runtime entry point."""

    conditions = ConditionEvaluator()
    information_queries = InformationQueryRouter(engine.game)
    return RuntimeServices(
        gate=EventGate(
            engine.store,
            GateConfig(
                default_cooldown_turns=max(0, int(engine.config.default_cooldown_turns))
            ),
        ),
        conditions=conditions,
        batch_executor=BatchExecutor(
            store=engine.store,
            game=engine.game,
            conditions=conditions,
            verification_attempts=engine.config.verification_attempts,
            now=lambda: engine._now(),
            monotonic=lambda: engine._monotonic(),
            checkpoint=lambda name: engine._checkpoint(name),
        ),
        turn_compiler=TurnCompiler(),
        information_queries=information_queries,
        planner_lifecycle=PlannerLifecycleCoordinator(
            PlannerLifecycleRuntime(
                store=engine.store,
                game=engine.game,
                planner=engine.planner,
                config=engine.config,
                conditions=conditions,
                information_queries=information_queries,
                now=lambda: engine._now(),
                monotonic=lambda: engine._monotonic(),
                checkpoint=lambda name: engine._checkpoint(name),
                observation_id=lambda: engine._active_observation_id,
                human_wait_context=lambda snapshot: engine._human_wait_context(
                    snapshot
                ),
            )
        ),
    )


def compose_runtime(
    *,
    store: WorkflowStore,
    game: GamePort,
    planner: Planner,
    engine_config: EngineConfig | None = None,
    clock: Any | None = None,
    crash_injector: Any | None = None,
) -> RuntimeComposition:
    engine = WorkflowEngine(
        store=store,
        game=game,
        planner=planner,
        config=engine_config,
        clock=clock,
        crash_injector=crash_injector,
        service_factory=build_runtime_services,
    )
    return RuntimeComposition(store=store, game=game, planner=planner, engine=engine)


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
        engine_config=config.engine_config(),
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
        engine_config=config.engine_config(),
    )


def replay_engine_config(
    recording: SnapshotRecording,
    *,
    auto_end_turn: bool,
) -> EngineConfig:
    settings: ReplayEngineSettings | None = recording.engine_settings
    action_types = (
        set(settings.allowed_action_types) if settings else set(ACTION_REGISTRY)
    )
    auto_action_types = (
        set(settings.auto_action_types) if settings else set(ACTION_REGISTRY)
    )
    return EngineConfig(
        execution_mode=settings.execution_mode if settings else ExecutionMode.AUTO,
        auto_end_turn=auto_end_turn,
        max_agent_calls_per_turn=(settings.max_agent_calls_per_turn if settings else 1),
        repeated_failure_threshold=(
            settings.repeated_failure_threshold if settings else 2
        ),
        verification_attempts=settings.verification_attempts if settings else 3,
        auto_action_types=auto_action_types,
        allowed_action_types=action_types,
        allowed_tools=set(settings.allowed_tools) if settings else set(recording.tools),
        verification_delay_seconds=0,
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
    config = replay_engine_config(recording, auto_end_turn=auto_end_turn)
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
        engine_config=config,
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
