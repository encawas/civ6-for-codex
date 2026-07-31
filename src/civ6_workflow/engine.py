from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO
from uuid import uuid4

from .actions import (
    ACTION_REGISTRY,
    action_argument_contracts,
)
from .agent_projection import project_agent_context
from .batch_executor import BatchExecutor, ExecutionTransition
from .conditions import ConditionEvaluator, extract_known_entities
from .domain import (
    ActionAttempt,
    AttemptStatus,
    AwaitingHumanTick,
    NoSafeActionTick,
    PlannerRequest,
    PlannerRequestStatus,
    ProviderAttemptStatus,
    RuntimeState,
    STRATEGIC_PROPOSAL_TARGET_KINDS,
    StrategicProposalWaitErrorTick,
    StrategicProposalWaitResumedTick,
    StrategicRequestWaitErrorTick,
    StrategicRequestWaitResumedTick,
    SystemErrorTick,
    TickOutcomeKind,
    validate_workflow_tick,
)
from .events import events_from_snapshot
from .gate import EventGate, GateConfig
from .ports import (
    GamePort,
    MutationBudget,
    Planner,
    WorkflowStorePort,
)
from .models import (
    EventLevel,
    ExecutionMode,
    GameEvent,
    MutationDeliveryStatus,
    RiskLevel,
    RuntimeSnapshot,
    StoredTask,
    TaskStatus,
    TickResult,
)
from .observation_normalization import (
    NormalizedRuntimeObservation,
    normalize_runtime_snapshot,
)
from .planner_lifecycle import PlannerLifecycleCoordinator
from .recovery import recover_turn_rewind
from .turn_compiler import TurnCompiler
from .validation import (
    PlanValidationContext,
    action_entity_type_contracts,
    condition_contracts,
    entity_id_argument_contracts,
    validate_plan_bundle,
)
from .workflow_protocol import (
    LEASE_CONDITION_TYPES,
    WorkflowAgentRequest as AgentRequest,
    WorkflowPlanBundle as PlanBundle,
    WorkflowTickMetrics as TickMetrics,
    information_tool_argument_contracts,
    validate_event_resolution_contract,
)
from .workflow_queries import InformationQueryRouter


END_TURN_AUTHORIZATION_PROJECTION_VERSION = "end-turn-authorization/v1"
HUMAN_WAIT_PROJECTION_VERSION = "human-wait-observation/v1"
_TRANSIENT_HTTP = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class EngineConfig:
    execution_mode: ExecutionMode = ExecutionMode.CONFIRM
    auto_end_turn: bool = False
    max_agent_calls_per_turn: int = 1
    repeated_failure_threshold: int = 2
    default_cooldown_turns: int = 2
    verification_attempts: int = 3
    verification_delay_seconds: float = 0.25
    auto_action_types: set[str] = field(default_factory=lambda: set(ACTION_REGISTRY))
    allowed_action_types: set[str] = field(default_factory=lambda: set(ACTION_REGISTRY))
    allowed_tools: set[str] = field(
        default_factory=lambda: {
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
        }
    )


@dataclass(slots=True)
class _TickContext:
    tick_id: str
    started_at: datetime
    started_monotonic: float
    call_count_before: int
    metrics: TickMetrics
    budget: MutationBudget
    starting_state: RuntimeState = RuntimeState.OBSERVING
    observation_ids: list[str] = field(default_factory=list)
    resuming_human_wait: bool = False


class InjectedCrashBoundary(RuntimeError):
    """Crash-injection signal that must escape the Tick error boundary."""


class FatalTickPersistenceError(RuntimeError):
    """The runtime could not persist even a SYSTEM_ERROR audit Tick."""


class _TickFileLock:
    """User-global non-blocking lock for one complete workflow Tick."""

    def __init__(self, lock_path: Path | None = None):
        self.path = lock_path or (Path.home() / ".civ6-workflow" / "runtime.tick.lock")
        self.handle: BinaryIO | None = None

    def __enter__(self) -> "_TickFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError(
                "another civ6-workflow process is already executing a game tick "
                f"under this user account ({self.path})"
            ) from exc
        self.handle = handle
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        handle = self.handle
        self.handle = None
        if handle is None:
            return
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


class WorkflowEngine:
    """Canonical bounded runtime; TickResult is only a compatibility envelope."""

    def __init__(
        self,
        *,
        store: WorkflowStorePort,
        game: GamePort,
        planner: Planner,
        config: EngineConfig | None = None,
        clock: Any | None = None,
        crash_injector: Any | None = None,
    ):
        self.store = store
        self.game = game
        self.planner = planner
        self.config = config or EngineConfig()
        self.clock = clock
        self.crash_injector = crash_injector
        self.gate = EventGate(
            store,
            GateConfig(
                default_cooldown_turns=max(0, int(self.config.default_cooldown_turns))
            ),
        )
        self.conditions = ConditionEvaluator()
        self.batch_executor = BatchExecutor(
            store=self.store,
            game=self.game,
            conditions=self.conditions,
            verification_attempts=self.config.verification_attempts,
            now=self._now,
            monotonic=self._monotonic,
            checkpoint=self._checkpoint,
        )
        self.turn_compiler = TurnCompiler()
        self.information_queries = InformationQueryRouter(self.game)
        self.planner_lifecycle = PlannerLifecycleCoordinator(self)
        self._available_tools: set[str] | None = None
        self._active_observation_id: str | None = None

    def request_end_turn_retry(self, game_id: str, turn: int) -> None:
        """Persist explicit authorization to retry the latest rejected end turn."""
        attempt = self.store.latest_attempt_for_task(game_id, f"end_turn:{turn}")
        if attempt is None or not self._is_explicit_end_turn_rejection(attempt):
            raise ValueError(
                "the current turn has no explicitly rejected end-turn attempt"
            )
        self.store.set_meta(self._end_turn_retry_key(attempt), True)

    async def tick(self) -> TickResult:
        with _TickFileLock():
            self.store.prepare_execution_mode(self.config.execution_mode)
            result = await self._tick_once()
            game_id = self.store.get_meta("last_game_id")
            no_safe_action = (
                isinstance(result.workflow_tick, dict)
                and result.workflow_tick.get("outcome") == "NO_SAFE_ACTION"
            )
            if (
                isinstance(game_id, str)
                and not result.paused
                and not result.turn_ended
                and not result.agent_invoked
                and any(event.blocking for event in result.events)
                and (
                    no_safe_action
                    or self.store.agent_called_for_turn(game_id, result.turn)
                )
                and self.store.active_turn_action_graph(game_id) is None
            ):
                result.paused = True
                result.pause_reason = (
                    "A blocking workflow event remains after this turn's planning "
                    "call and no executable recovery task exists; human review is "
                    "required."
                )
            if isinstance(game_id, str) and self._uncertain_tasks(game_id):
                result.paused = True
                if not result.pause_reason:
                    result.pause_reason = (
                        "An irreversible action has an uncertain commit outcome; "
                        "reconcile the live game state before retrying."
                    )
            return result

    async def _tick_once(self) -> TickResult:
        ctx = _TickContext(
            tick_id=f"tick_{uuid4().hex}",
            started_at=self._now(),
            started_monotonic=self._monotonic(),
            call_count_before=self.game.call_count,
            metrics=TickMetrics(),
            budget=MutationBudget(),
        )
        try:
            return await self._run_tick(ctx)
        except InjectedCrashBoundary:
            raise
        except Exception as exc:
            if ctx.budget.used:
                raise
            return self._system_error(ctx, exc)

    async def _run_tick(self, ctx: _TickContext) -> TickResult:
        raw = await self._read_snapshot(ctx.metrics, include_units=False)
        observation = self._normalize_snapshot(raw, ctx.metrics)
        snapshot = observation.snapshot
        observation_id = self._observation_id(observation)
        self._active_observation_id = observation_id
        ctx.observation_ids.append(observation_id)
        ctx.starting_state = self.store.load_runtime_state(snapshot.game_id)
        self.store.set_meta("last_game_id", snapshot.game_id)
        self.store.set_meta("last_observed_turn", snapshot.turn)
        await self._verify_tool_surface()

        unresolved = self.store.unresolved_action_attempt(snapshot.game_id)
        if unresolved is not None:
            unresolved_task = self.store.get_task(snapshot.game_id, unresolved.task_id)
            if (
                unresolved_task is not None
                and unresolved_task.entity_type in {"unit", "builder"}
                and snapshot.units is None
            ):
                raw = await self._read_snapshot(ctx.metrics, include_units=True)
                observation = self._normalize_snapshot(raw, ctx.metrics)
                snapshot = observation.snapshot
                observation_id = self._observation_id(observation)
                self._active_observation_id = observation_id
                ctx.observation_ids.append(observation_id)
            transition = self.batch_executor.reconcile(
                observation,
                unresolved,
                source_observation_id=observation_id,
                metrics=ctx.metrics,
            )
            return self._finish_execution_transition(
                ctx,
                snapshot,
                transition,
            )

        if ctx.starting_state is RuntimeState.AWAITING_HUMAN:
            wait = self.store.human_wait_context(snapshot.game_id) or {}
            if (
                wait.get("wait_kind") == "strategic_contract_proposal_ready"
                and wait.get("resume_policy") == "explicit_only"
            ):
                if self.store.phase1c_decisions_enabled:
                    return self._finish(
                        ctx,
                        snapshot,
                        AwaitingHumanTick,
                        blocking_reason=str(
                            wait.get("blocking_reason")
                            or "research Proposal requires APPROVE or REJECT"
                        ),
                    )
                if wait.get("resume_requested") is True:
                    return self._finish(
                        ctx,
                        snapshot,
                        StrategicProposalWaitResumedTick,
                        resume_request_id=wait.get("resume_request_id"),
                        proposal_ready_tick_id=wait.get("proposal_ready_tick_id"),
                        planner_request_id=wait.get("planner_request_id"),
                        proposal_id=wait.get("proposal_id"),
                        target_kind=wait.get("target_kind"),
                        expected_base_revision=wait.get("expected_base_revision"),
                        resume_reason="explicit_user_resume",
                    )
            if wait.get("wait_kind") == "strategic_request_terminated":
                if wait.get("resume_requested") is True:
                    return self._finish(
                        ctx,
                        snapshot,
                        StrategicRequestWaitResumedTick,
                        planner_request_id=wait.get("planner_request_id"),
                        terminal_tick_id=wait.get("terminal_tick_id"),
                        terminal_status=wait.get("terminal_status"),
                        resume_reason="explicit_user_resume",
                        resumed_at=datetime.fromisoformat(
                            str(wait.get("resume_requested_at")).replace("Z", "+00:00")
                        ),
                    )
                return self._finish(
                    ctx,
                    snapshot,
                    AwaitingHumanTick,
                    blocking_reason=str(
                        wait.get("blocking_reason") or "human review is required"
                    ),
                )
            if wait.get("requires_unit_details") is True and snapshot.units is None:
                raw = await self._read_snapshot(ctx.metrics, include_units=True)
                observation = self._normalize_snapshot(raw, ctx.metrics)
                snapshot = observation.snapshot
                observation_id = self._observation_id(observation)
                self._active_observation_id = observation_id
                ctx.observation_ids.append(observation_id)
            resume_reason = self._human_wait_resume_reason(observation)
            if resume_reason is None:
                return self._finish(
                    ctx,
                    snapshot,
                    AwaitingHumanTick,
                    blocking_reason=str(
                        wait.get("blocking_reason") or "human review is required"
                    ),
                )
            ctx.resuming_human_wait = True
        active_execution = self.store.active_execution_missions(snapshot.game_id)
        active_contract = self.store.get_active_strategic_contract(snapshot.game_id)
        execution_missions = () if active_execution is None else active_execution[1]
        if (
            active_execution is not None
            and active_contract is not None
            and active_execution[0] != active_contract
        ):
            raise RuntimeError(
                "execution Mission projection disagrees with active Contract"
            )
        owned_execution_scopes = (
            set()
            if active_contract is None
            else set(active_contract.authority_scope_set.mission_graph_scopes)
        )
        need_units = (
            observation.canonical.unit_summary.detail_required
            or "settler" in owned_execution_scopes
            or "tactical_emergency" in owned_execution_scopes
        )
        if need_units and snapshot.units is None:
            raw = await self._read_snapshot(ctx.metrics, include_units=True)
            observation = self._normalize_snapshot(raw, ctx.metrics)
            snapshot = observation.snapshot
            observation_id = self._observation_id(observation)
            self._active_observation_id = observation_id
            ctx.observation_ids.append(observation_id)

        mission_repair_tick = await self.planner_lifecycle.advance_mission_repair(
            ctx, observation
        )
        if mission_repair_tick is not None:
            return mission_repair_tick

        authoritative_mission_events: list[GameEvent] = []
        if active_contract is not None:
            for mission in execution_missions:
                scope = mission.scope
                if scope in {"settler", "city_roles", "tactical_emergency"}:
                    unavailable_target = self.turn_compiler.unavailable_target(
                        observation.canonical, mission
                    )
                    if unavailable_target is None:
                        continue
                    authoritative_mission_events.append(
                        GameEvent(
                            event_type=f"{scope}_mission_target_unavailable",
                            turn=snapshot.turn,
                            entity_type=scope,
                            entity_id=unavailable_target,
                            level=EventLevel.L3,
                            risk=RiskLevel.MEDIUM,
                            blocking=True,
                            payload={
                                "contract_id": active_contract.contract_id,
                                "contract_revision": active_contract.revision,
                                "mission_id": mission.mission_id,
                                "mission_revision": mission.mission_revision,
                                "target": unavailable_target,
                            },
                            dedupe_key=(
                                f"{scope}_mission_target_unavailable:"
                                f"{active_contract.revision}:"
                                f"{mission.mission_revision}:{unavailable_target}"
                            ),
                        )
                    )
                    continue
                target_key = "technology" if scope == "research" else "civic"
                available_ids = (
                    observation.canonical.progression.available_research_ids
                    if scope == "research"
                    else observation.canonical.progression.available_civic_ids
                )
                unavailable_target = self.turn_compiler.unavailable_target(
                    observation.canonical, mission
                )
                if unavailable_target is None:
                    continue
                available = sorted(item.value for item in available_ids)
                authoritative_mission_events.append(
                    GameEvent(
                        event_type=f"{scope}_mission_target_unavailable",
                        turn=snapshot.turn,
                        entity_type=scope,
                        entity_id=unavailable_target,
                        level=EventLevel.L3,
                        risk=RiskLevel.MEDIUM,
                        blocking=True,
                        payload={
                            "contract_id": active_contract.contract_id,
                            "contract_revision": active_contract.revision,
                            "mission_id": mission.mission_id,
                            "mission_revision": mission.mission_revision,
                            target_key: unavailable_target,
                            "available": available,
                        },
                        dedupe_key=(
                            f"{scope}_mission_target_unavailable:"
                            f"{active_contract.revision}:{mission.mission_revision}:"
                            f"{unavailable_target}"
                        ),
                    )
                )
        existing_graph_state = self.store.active_turn_action_graph(snapshot.game_id)
        reusable_graph = (
            active_contract is not None
            and bool(execution_missions)
            and existing_graph_state is not None
            and existing_graph_state[0].turn_number == snapshot.turn
            and existing_graph_state[0].source_observation_projection_hash
            == observation.canonical.projection_hash
            and existing_graph_state[0].source_contract_id
            == active_contract.contract_id
            and existing_graph_state[0].source_contract_revision
            == active_contract.revision
        )
        turn_compilation = (
            None
            if active_contract is None or not execution_missions or reusable_graph
            else self.turn_compiler.compile_missions(
                observation.canonical,
                active_contract,
                execution_missions,
                mode=self.config.execution_mode,
                auto_action_types=self.config.auto_action_types,
                compiled_at=observation.canonical.observed_at,
            )
        )
        snapshot_events = events_from_snapshot(snapshot)
        current_events = [
            *authoritative_mission_events,
            *snapshot_events,
        ]
        diplomacy_trade_human_events = [
            event
            for event in current_events
            if event.event_type
            in {"pending_diplomacy", "pending_trade_offer", "war_posture_required"}
        ]
        tactical_unavailable_events = [
            event
            for event in authoritative_mission_events
            if event.event_type == "tactical_emergency_mission_target_unavailable"
        ]
        if "city_roles" in owned_execution_scopes:
            current_events = [
                event
                for event in current_events
                if event.event_type
                not in {"city_role_required", "invalid_city_plan_item"}
            ]
        if "settler" in owned_execution_scopes:
            current_events = [
                event
                for event in current_events
                if event.event_type != "settler_site_selection_required"
            ]
        if "diplomacy_trade" in owned_execution_scopes:
            current_events = [
                event
                for event in current_events
                if event.event_type
                not in {
                    "pending_diplomacy",
                    "pending_trade_offer",
                    "war_posture_required",
                }
            ]
        if "tactical_emergency" in owned_execution_scopes:
            current_events = [
                event
                for event in current_events
                if event.event_type
                not in {
                    "tactical_attack_opportunity",
                    "emergency_defense_required",
                    "emergency_response_window",
                    "tactical_emergency_mission_target_unavailable",
                }
            ]
        if turn_compilation is not None:
            self.store.activate_turn_action_graph(
                turn_compilation.graph,
                turn_compilation.nodes,
                activated_at=observation.canonical.observed_at,
            )
        if "diplomacy_trade" in owned_execution_scopes and diplomacy_trade_human_events:
            gate = self.gate.ingest(snapshot.game_id, diplomacy_trade_human_events)
            human_wait = TickResult(
                turn=snapshot.turn,
                metrics=ctx.metrics,
                events=gate.emitted,
                paused=True,
                pause_reason=(
                    "Diplomacy and trade responses require explicit human review."
                ),
            )
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                compatibility=human_wait,
                blocking_reason=human_wait.pause_reason,
            )
        if (
            "tactical_emergency" in owned_execution_scopes
            and tactical_unavailable_events
        ):
            gate = self.gate.ingest(snapshot.game_id, tactical_unavailable_events)
            human_wait = TickResult(
                turn=snapshot.turn,
                metrics=ctx.metrics,
                events=gate.emitted,
                paused=True,
                pause_reason=(
                    "The active tactical/emergency Mission is no longer safely "
                    "executable and requires explicit human review."
                ),
            )
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                compatibility=human_wait,
                blocking_reason=human_wait.pause_reason,
            )

        due_tasks = [
            *self.store.due_turn_action_nodes(
                snapshot.game_id,
                snapshot.turn,
                source_observation_id=observation_id,
            )
        ]
        due_tasks.sort(key=lambda task: (task.due_turn, task.task_id))
        if (
            due_tasks
            and snapshot.units is None
            and any(task.entity_type in {"unit", "builder"} for task in due_tasks)
        ):
            raw = await self._read_snapshot(ctx.metrics, include_units=True)
            observation = self._normalize_snapshot(raw, ctx.metrics)
            snapshot = observation.snapshot
            observation_id = self._observation_id(observation)
            self._active_observation_id = observation_id
            ctx.observation_ids.append(observation_id)

        execution = await self.batch_executor.advance(
            observation,
            source_observation_id=observation_id,
            mode=self.config.execution_mode,
            available_tools=self._available_tools or set(),
            metrics=ctx.metrics,
            budget=ctx.budget,
        )
        if execution is not None:
            return self._finish_execution_transition(ctx, snapshot, execution)

        rewind_event = recover_turn_rewind(self.store, snapshot)
        events = [] if rewind_event is None else [rewind_event]
        events.extend(authoritative_mission_events)
        events.extend(snapshot_events)
        if "diplomacy_trade" in owned_execution_scopes:
            events = [
                event
                for event in events
                if event.event_type
                not in {
                    "pending_diplomacy",
                    "pending_trade_offer",
                    "war_posture_required",
                }
            ]
        if "tactical_emergency" in owned_execution_scopes:
            events = [
                event
                for event in events
                if event.event_type
                not in {
                    "tactical_attack_opportunity",
                    "emergency_defense_required",
                    "emergency_response_window",
                    "tactical_emergency_mission_target_unavailable",
                }
            ]
        if "settler" in owned_execution_scopes:
            events = [
                event
                for event in events
                if event.event_type != "settler_site_selection_required"
            ]
        gate = self.gate.ingest(snapshot.game_id, events)
        compat = TickResult(
            turn=snapshot.turn, metrics=ctx.metrics, events=gate.emitted
        )
        agent_events = list(gate.agent_events)
        agent_events.extend(
            event
            for event in gate.by_level[EventLevel.L2]
            if event.blocking and event not in agent_events
        )
        agent_events, planning_tick = await self._advance_decision_runtime(
            ctx,
            observation,
            agent_events,
            compat,
            current_events=events,
        )
        if planning_tick is not None:
            return planning_tick
        if agent_events:
            compat.paused = True
            compat.pause_reason = (
                "A blocking event has no current MissionGraph projection; "
                "explicit migration or human review is required."
            )
        if compat.paused:
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                compatibility=compat,
                blocking_reason=compat.pause_reason or "human review is required",
            )
        end_turn_suppression = self._end_turn_rejection_suppression(observation)
        if self._may_end_turn(snapshot, compat):
            if end_turn_suppression is not None:
                return self._finish(
                    ctx,
                    snapshot,
                    NoSafeActionTick,
                    compatibility=compat,
                    blocking_reason=end_turn_suppression,
                )
            transition = await self.batch_executor.send_end_turn(
                observation,
                source_observation_id=observation_id,
                authorization_projection_version=(
                    END_TURN_AUTHORIZATION_PROJECTION_VERSION
                ),
                authorization_projection_hash=self._end_turn_authorization_hash(
                    observation
                ),
                reflections=self._end_turn_reflections(observation),
                metrics=ctx.metrics,
                budget=ctx.budget,
            )
            return self._finish_execution_transition(ctx, snapshot, transition)
        if any(event.blocking for event in compat.events):
            compat.paused = True
            compat.pause_reason = "a blocking decision has no safe automatic resolution"
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                compatibility=compat,
                blocking_reason=compat.pause_reason,
            )
        return self._finish(
            ctx,
            snapshot,
            NoSafeActionTick,
            compatibility=compat,
            blocking_reason="no safe action is available",
        )

    def _finish(
        self,
        ctx: _TickContext,
        snapshot: RuntimeSnapshot,
        tick_type: type,
        *,
        compatibility: TickResult | None = None,
        executed_task_ids: list[str] | None = None,
        failed_task_ids: list[str] | None = None,
        blocked_task_ids: list[str] | None = None,
        turn_ended: bool = False,
        attempt_update: ActionAttempt | None = None,
        task_status: TaskStatus | None = None,
        task_error: str | None = None,
        runtime_active_attempt_id: str | None = None,
        **fields: Any,
    ) -> TickResult:
        completed = self._now()
        ctx.metrics.mcp_call_count = self.game.call_count - ctx.call_count_before
        ctx.metrics.mutation_count = ctx.budget.used
        ctx.metrics.total_seconds = self._monotonic() - ctx.started_monotonic
        common = {
            "tick_id": ctx.tick_id,
            "game_session_id": snapshot.game_id,
            "turn_number": snapshot.turn,
            "starting_runtime_state": ctx.starting_state,
            "observation_ids": tuple(ctx.observation_ids),
            "started_at": ctx.started_at,
            "completed_at": completed,
            "metrics": ctx.metrics.model_dump(mode="json"),
        }
        tick = validate_workflow_tick(tick_type(**common, **fields))
        human_wait_context = None
        if isinstance(
            tick,
            (
                AwaitingHumanTick,
                StrategicProposalWaitErrorTick,
                StrategicRequestWaitErrorTick,
            ),
        ):
            existing_wait = self.store.human_wait_context(snapshot.game_id)
            if existing_wait is not None and (
                (
                    existing_wait.get("wait_kind")
                    == "strategic_contract_proposal_ready"
                    and existing_wait.get("resume_policy") == "explicit_only"
                )
                or existing_wait.get("wait_kind") == "strategic_request_terminated"
            ):
                human_wait_context = dict(existing_wait)
            else:
                human_wait_context = self._human_wait_context(snapshot)
            human_wait_context["blocking_reason"] = tick.blocking_reason

        if isinstance(
            tick, (StrategicProposalWaitResumedTick, StrategicRequestWaitResumedTick)
        ):
            if attempt_update is not None:
                raise ValueError("Proposal wait resume cannot update an ActionAttempt")
            self.store.persist_phase4_tick(tick, human_wait_context=None)
        elif attempt_update is None:
            self.store.persist_tick_and_runtime_state(
                tick,
                active_attempt_id=runtime_active_attempt_id,
                checkpoint=self._checkpoint,
                human_wait_context=human_wait_context,
            )
        elif attempt_update.status is AttemptStatus.SUCCEEDED:
            if attempt_update.action_type == "end_turn":
                self.store.finalize_turn_transition(
                    attempt_update, tick, checkpoint=self._checkpoint
                )
            else:
                self.store.finalize_attempt_success(
                    attempt_update, tick, checkpoint=self._checkpoint
                )
        elif attempt_update.status is AttemptStatus.REJECTED_BEFORE_SEND:
            self.store.recover_prepared_attempt(
                attempt_update, tick, checkpoint=self._checkpoint
            )
        elif attempt_update.status is AttemptStatus.FAILED:
            if attempt_update.action_type == "end_turn":
                self.store.persist_tick_and_runtime_state(
                    tick,
                    attempt=attempt_update,
                    active_attempt_id=None,
                    checkpoint=self._checkpoint,
                    attempt_checkpoint="after_attempt_failed_update",
                    human_wait_context=human_wait_context,
                )
            else:
                self.store.finalize_attempt_failure(
                    attempt_update,
                    tick,
                    task_error=task_error,
                    checkpoint=self._checkpoint,
                )
        else:
            self.store.persist_tick_and_runtime_state(
                tick,
                active_attempt_id=attempt_update.action_attempt_id,
                attempt=attempt_update,
                task_status=task_status,
                task_error=task_error,
                checkpoint=self._checkpoint,
                human_wait_context=human_wait_context,
            )

        result = compatibility or TickResult(turn=snapshot.turn, metrics=ctx.metrics)
        result.metrics = ctx.metrics
        result.tick_id = tick.tick_id
        result.runtime_state = tick.ending_runtime_state.value
        result.workflow_tick = tick.model_dump(mode="json")
        result.turn_ended = turn_ended
        if executed_task_ids:
            result.executed_task_ids.extend(executed_task_ids)
        if failed_task_ids:
            result.failed_task_ids.extend(failed_task_ids)
        if blocked_task_ids:
            result.blocked_task_ids.extend(blocked_task_ids)
        if isinstance(
            tick,
            (
                AwaitingHumanTick,
                StrategicProposalWaitErrorTick,
                StrategicRequestWaitErrorTick,
                SystemErrorTick,
            ),
        ):
            result.paused = True
            result.pause_reason = tick.blocking_reason
        return result

    def _finish_execution_transition(
        self,
        ctx: _TickContext,
        snapshot: RuntimeSnapshot,
        transition: ExecutionTransition,
    ) -> TickResult:
        return self._finish(
            ctx,
            snapshot,
            transition.tick_type,
            executed_task_ids=list(transition.executed_task_ids),
            failed_task_ids=list(transition.failed_task_ids),
            blocked_task_ids=list(transition.blocked_task_ids),
            turn_ended=transition.turn_ended,
            attempt_update=transition.attempt_update,
            task_status=transition.task_status,
            task_error=transition.task_error,
            **transition.fields,
        )

    @staticmethod
    def _observation_id(observation: NormalizedRuntimeObservation) -> str:
        return observation.canonical.observation_id

    def _system_error(
        self,
        ctx: _TickContext,
        error: Exception,
    ) -> TickResult:
        category = type(error).__name__
        summary = " ".join(str(error).split())[:500] or category
        try:
            game_id = str(self.store.get_meta("last_game_id", "runtime:unknown"))
            turn = int(self.store.get_meta("last_observed_turn", 0) or 0)
            ctx.starting_state = self.store.load_runtime_state(game_id)
            active_attempt = self.store.unresolved_action_attempt(game_id)
            active_attempt_id = (
                None if active_attempt is None else active_attempt.action_attempt_id
            )
            wait = self.store.human_wait_context(game_id)
            if not ctx.observation_ids:
                ctx.observation_ids.append(f"error_obs_{uuid4().hex}")
            snapshot = RuntimeSnapshot(
                turn=max(0, turn),
                game_id=game_id,
                overview={"turn": max(0, turn)},
            )
            if (
                ctx.starting_state is RuntimeState.AWAITING_HUMAN
                and isinstance(wait, dict)
                and wait.get("wait_kind") == "strategic_contract_proposal_ready"
                and wait.get("resume_policy") == "explicit_only"
            ):
                proposal_ready_tick_id = wait.get("proposal_ready_tick_id")
                if not isinstance(proposal_ready_tick_id, str):
                    ready_ticks = [
                        tick
                        for tick in self.store.list_workflow_ticks(game_id)
                        if tick.outcome is TickOutcomeKind.STRATEGIC_PROPOSAL_READY
                        and getattr(tick, "proposal_id", None)
                        == wait.get("proposal_id")
                    ]
                    if len(ready_ticks) != 1:
                        raise ValueError(
                            "Proposal wait has no unique Proposal-ready Tick"
                        )
                    proposal_ready_tick_id = ready_ticks[0].tick_id
                return self._finish(
                    ctx,
                    snapshot,
                    StrategicProposalWaitErrorTick,
                    blocking_reason=(
                        "workflow Tick failed while Proposal wait remains active"
                    ),
                    error_category=category,
                    diagnostic_summary=summary,
                    proposal_ready_tick_id=proposal_ready_tick_id,
                    planner_request_id=wait.get("planner_request_id"),
                    proposal_id=wait.get("proposal_id"),
                    target_kind=wait.get("target_kind"),
                    expected_base_revision=wait.get("expected_base_revision"),
                    runtime_active_attempt_id=active_attempt_id,
                )
            if (
                ctx.starting_state is RuntimeState.AWAITING_HUMAN
                and isinstance(wait, dict)
                and wait.get("wait_kind") == "strategic_request_terminated"
            ):
                return self._finish(
                    ctx,
                    snapshot,
                    StrategicRequestWaitErrorTick,
                    blocking_reason=(
                        "workflow Tick failed while strategic Request wait remains active"
                    ),
                    error_category=category,
                    diagnostic_summary=summary,
                    planner_request_id=wait.get("planner_request_id"),
                    terminal_tick_id=wait.get("terminal_tick_id"),
                    terminal_status=wait.get("terminal_status"),
                    failure_category=wait.get("failure_category"),
                    runtime_active_attempt_id=active_attempt_id,
                )
            return self._finish(
                ctx,
                snapshot,
                SystemErrorTick,
                blocking_reason="workflow Tick failed before mutation",
                error_category=category,
                diagnostic_summary=summary,
                action_attempt_id=active_attempt_id,
                runtime_active_attempt_id=active_attempt_id,
            )
        except InjectedCrashBoundary:
            raise
        except Exception as persistence_error:
            raise FatalTickPersistenceError(
                "workflow Tick failed and SYSTEM_ERROR audit persistence "
                f"also failed ({type(persistence_error).__name__})"
            ) from persistence_error

    def _checkpoint(self, name: str) -> None:
        if self.crash_injector is None:
            return
        try:
            self.crash_injector.checkpoint(name)
        except Exception as exc:
            raise InjectedCrashBoundary(str(exc)) from exc

    def _now(self) -> datetime:
        if self.clock is not None:
            return self.clock.now()
        return datetime.now(UTC)

    def _monotonic(self) -> float:
        if self.clock is not None:
            return float(self.clock.monotonic())
        return time.perf_counter()

    async def _pre_route_decision_runtime(self, ctx, observation, current_events):
        compatibility = TickResult(
            turn=observation.snapshot.turn,
            metrics=ctx.metrics,
            events=[],
        )
        return self.planner_lifecycle.validate_before_routing(
            ctx, observation, current_events, compatibility
        )

    async def _advance_decision_runtime(
        self,
        ctx,
        observation,
        agent_events,
        compatibility,
        current_events=None,
    ):
        return await self.planner_lifecycle.advance(
            ctx,
            observation,
            agent_events,
            compatibility,
            current_events=current_events,
        )

    async def _invoke_planner(
        self,
        snapshot,
        agent_events,
        result: TickResult,
        metrics: TickMetrics,
    ) -> None:
        if self.store.unresolved_action_attempt(snapshot.game_id) is not None:
            return
        backoff = self._active_backoff()
        if backoff is not None:
            result.paused = True
            result.pause_reason = (
                "Planner provider is in transient backoff for "
                f"{backoff['remaining_seconds']:.1f}s: {backoff.get('category')}"
            )
            return

        request = self._build_agent_request(snapshot, agent_events)
        result.planner_request_id = request.request_id
        planner_started = time.perf_counter()
        current_request = request
        bundle: PlanBundle | None = None
        result.agent_invoked = True

        try:
            prefetched = self.information_queries.prefetch(agent_events)
            if prefetched:
                prefetched_results = await self.information_queries.execute(prefetched)
                metrics.information_query_count += len(prefetched_results)
                request = request.model_copy(
                    update={"information_results": prefetched_results}
                )
                current_request = request
                self.store.set_meta(
                    "last_information_results",
                    {
                        "turn": snapshot.turn,
                        "phase": "prefetch",
                        "results": prefetched_results,
                    },
                )

            bundle = await self._plan_once(request, metrics)
            self._validate_planner_bundle(
                bundle,
                request,
                snapshot,
                agent_events,
                allow_information_requests=True,
            )

            if getattr(bundle, "information_requests", []):
                self.store.set_meta(
                    "last_information_phase_bundle",
                    bundle.model_dump(mode="json"),
                )
                focused_results = await self.information_queries.execute(
                    bundle.information_requests
                )
                metrics.information_query_count += len(focused_results)
                combined = dict(getattr(request, "information_results", {}))
                combined.update(focused_results)
                payload = request.model_dump(mode="python")
                payload.update(
                    {
                        "request_id": f"req_{uuid4().hex}",
                        "information_results": combined,
                        "constraints": {
                            **request.constraints,
                            "planning_phase": "final",
                            "allow_information_requests": False,
                        },
                    }
                )
                current_request = AgentRequest.model_validate(payload)
                self.store.set_meta(
                    "last_information_results",
                    {
                        "turn": snapshot.turn,
                        "phase": "planner_requested",
                        "results": combined,
                    },
                )
                bundle = await self._plan_once(current_request, metrics)

            self._validate_planner_bundle(
                bundle,
                current_request,
                snapshot,
                agent_events,
                allow_information_requests=False,
            )

            self.store.save_plan_bundle(
                snapshot.game_id,
                snapshot.turn,
                bundle,
                mode=self.config.execution_mode,
                auto_action_types=self.config.auto_action_types,
                observation_id=self._active_observation_id,
            )
            self.store.set_meta(
                "last_event_resolutions",
                {
                    "turn": snapshot.turn,
                    "plan_id": bundle.plan_id,
                    "resolutions": [
                        item.model_dump(mode="json")
                        for item in getattr(bundle, "event_resolutions", [])
                    ],
                },
            )
            self.store.mark_events_sent_to_agent(
                snapshot.game_id,
                [event.dedupe_key for event in agent_events],
                snapshot.turn,
            )
            result.plan_id = bundle.plan_id
            if bundle.requires_human_review:
                result.paused = True
                result.pause_reason = "Planner requested human review"

            self.store.record_agent_run(
                snapshot.game_id,
                current_request,
                response=bundle,
                success=True,
                error=None,
                duration_seconds=time.perf_counter() - planner_started,
            )
            self._clear_backoff()
        except Exception as exc:
            failure = self._classify_planner_failure(exc)
            result.paused = True
            result.pause_reason = (
                f"Agent planning failed [{failure['category']}]: "
                f"{failure['final_error']}"
            )
            self.store.record_agent_run(
                snapshot.game_id,
                current_request,
                response=bundle,
                success=False,
                error=json.dumps(failure, ensure_ascii=False, separators=(",", ":")),
                duration_seconds=time.perf_counter() - planner_started,
            )
            if failure["transient"]:
                self._set_backoff(failure)
        finally:
            diagnostics = getattr(self.planner, "last_diagnostics", None)
            if isinstance(diagnostics, dict):
                self.store.set_meta("last_planner_diagnostics", diagnostics)
            metrics.agent_seconds = time.perf_counter() - planner_started

    async def _plan_once(
        self, request: AgentRequest, metrics: TickMetrics
    ) -> PlanBundle:
        metrics.agent_attempt_count += 1
        # Backward-compatible metric now means attempted planner calls, not only
        # successful calls.
        metrics.agent_call_count = metrics.agent_attempt_count
        bundle = await self.planner.plan(request)
        metrics.agent_success_count += 1
        return bundle

    def _validate_planner_bundle(
        self,
        bundle: PlanBundle,
        request: AgentRequest,
        snapshot,
        agent_events,
        *,
        allow_information_requests: bool,
    ) -> None:
        max_tasks = int(request.constraints.get("max_tasks", 8))
        validate_plan_bundle(
            bundle,
            PlanValidationContext(
                current_turn=snapshot.turn,
                allowed_action_types=set(
                    request.constraints.get(
                        "allowed_action_types", self.config.allowed_action_types
                    )
                ),
                known_entities=extract_known_entities(snapshot),
                max_tasks=max_tasks,
            ),
        )
        known_task_ids = {
            task.task_id for task in self.store.list_tasks(snapshot.game_id)
        }
        validate_event_resolution_contract(
            PlanBundle.model_validate(bundle.model_dump(mode="python")),
            agent_events,
            known_task_ids=known_task_ids,
            allow_information_requests=allow_information_requests,
        )

    def _active_backoff(
        self, request: "PlannerRequest | None" = None
    ) -> dict[str, Any] | None:
        if (
            request is not None
            and request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
        ):
            if request.status is not PlannerRequestStatus.BACKOFF:
                return None
            if request.next_retry_at is None:
                raise ValueError("strategic BACKOFF request requires next_retry_at")
            remaining = (request.next_retry_at - self._now()).total_seconds()
            if remaining <= 0:
                return None
            attempts = self.store.list_provider_attempts(request.planner_request_id)
            return {
                "category": request.failure_category,
                "failure_count": sum(
                    attempt.status is ProviderAttemptStatus.FAILED
                    for attempt in attempts
                ),
                "until": request.next_retry_at.isoformat(),
                "remaining_seconds": remaining,
            }
        value = self.store.get_meta("planner_provider_backoff")
        if not isinstance(value, dict):
            return None
        until = float(value.get("until_epoch", 0) or 0)
        remaining = until - time.time()
        if remaining <= 0:
            return None
        return {**value, "remaining_seconds": remaining}

    def _classify_planner_failure(self, exc: Exception) -> dict[str, Any]:
        diagnostics = getattr(self.planner, "last_diagnostics", None)
        if not isinstance(diagnostics, dict):
            diagnostics = {}
        status = diagnostics.get("http_status")
        try:
            status = None if status is None else int(status)
        except (TypeError, ValueError):
            status = None
        text = str(exc)
        lowered = text.lower()
        transient = status in _TRANSIENT_HTTP or any(
            marker in lowered
            for marker in (
                "timeout",
                "timed out",
                "transport failed",
                "connection reset",
                "temporarily unavailable",
            )
        )
        if transient:
            category = "transient_provider_failure"
        elif status in {401, 403}:
            category = "authentication_failure"
        elif status == 404:
            category = "model_or_endpoint_not_found"
        elif "planbundle" in lowered or "event resolution" in lowered:
            category = "planner_contract_failure"
        else:
            category = "planner_failure"
        return {
            "category": category,
            "transient": transient,
            "provider": diagnostics.get("backend", "unknown"),
            "http_status": status,
            "request_id": diagnostics.get("request_id"),
            "retry_count": diagnostics.get("attempt_count", 0),
            "final_error": text[-1000:],
        }

    def _set_backoff(self, failure: dict[str, Any]) -> None:
        count = int(self.store.get_meta("planner_transient_failure_count", 0) or 0) + 1
        delay = min(120.0, 5.0 * (2 ** min(count - 1, 5)))
        self.store.set_meta("planner_transient_failure_count", count)
        self.store.set_meta(
            "planner_provider_backoff",
            {
                **failure,
                "failure_count": count,
                "delay_seconds": delay,
                "until_epoch": time.time() + delay,
            },
        )

    def _clear_backoff(self) -> None:
        self.store.set_meta("planner_transient_failure_count", 0)
        self.store.set_meta("planner_provider_backoff", {})

    @staticmethod
    def _normalize_snapshot(
        snapshot: RuntimeSnapshot, metrics: TickMetrics
    ) -> NormalizedRuntimeObservation:
        started = time.perf_counter()
        observation = normalize_runtime_snapshot(snapshot)
        metrics.normalization_seconds += time.perf_counter() - started
        return observation

    async def _read_snapshot(
        self, metrics: TickMetrics, *, include_units: bool
    ) -> RuntimeSnapshot:
        started = self._monotonic()
        snapshot = await self.game.read_snapshot(include_units=include_units)
        metrics.state_query_seconds += self._monotonic() - started
        return snapshot

    async def _verify_tool_surface(self) -> None:
        if self._available_tools is not None:
            return
        self._available_tools = await self.game.list_tools()
        fallback_queries = {
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
        }
        missing = (self.config.allowed_tools | fallback_queries) - self._available_tools
        if missing:
            raise RuntimeError(f"civ6-mcp is missing required tools: {sorted(missing)}")

    def _suppress_recoverable_blockers(
        self, events: list[GameEvent], retrying_tasks: list[StoredTask]
    ) -> list[GameEvent]:
        production_city_ids = {
            str(task.entity_id)
            for task in retrying_tasks
            if task.action_type == "city_set_production"
        }
        has_research = any(
            task.action_type == "set_research" for task in retrying_tasks
        )
        has_civic = any(task.action_type == "set_civic" for task in retrying_tasks)
        retained: list[GameEvent] = []
        for event in events:
            if (
                event.event_type == "city_no_production"
                and str(event.entity_id) in production_city_ids
            ):
                continue
            if event.event_type == "end_turn_blocker":
                kind = str(event.payload.get("blocking_type", ""))
                if production_city_ids and kind == "ENDTURN_BLOCKING_PRODUCTION":
                    continue
                if has_research and kind == "ENDTURN_BLOCKING_RESEARCH":
                    continue
                if has_civic and kind == "ENDTURN_BLOCKING_CIVIC":
                    continue
            retained.append(event)

        game_id = self.store.get_meta("last_game_id")
        if not isinstance(game_id, str):
            return retained
        active_statuses = {
            TaskStatus.PENDING,
            TaskStatus.READY,
            TaskStatus.RUNNING,
            TaskStatus.AWAITING_CONFIRMATION,
        }
        unit_actions = {
            "unit_move",
            "builder_improve",
            "unit_heal",
            "unit_fortify",
            "unit_skip",
        }
        active_tasks = self.store.list_tasks(game_id)
        has_unit_recovery = any(
            task.action_type in unit_actions and task.status in active_statuses
            for task in active_tasks
        )
        has_settler_recovery = any(
            task.action_type == "unit_found_city" and task.status in active_statuses
            for task in active_tasks
        )
        if not (has_unit_recovery or has_settler_recovery):
            return retained
        return [
            event
            for event in retained
            if not (
                event.event_type == "end_turn_blocker"
                and str(event.payload.get("blocking_type", ""))
                == "ENDTURN_BLOCKING_UNITS"
            )
        ]

    def _build_agent_request(
        self, snapshot: RuntimeSnapshot, events: list[GameEvent]
    ) -> AgentRequest:
        context = self.store.current_context(snapshot.game_id)
        active_contract = self.store.get_active_strategic_contract(snapshot.game_id)
        owned_scopes = (
            set()
            if active_contract is None
            else set(active_contract.authority_scope_set.mission_graph_scopes)
        )
        research_authoritative = "research" in owned_scopes
        civic_authoritative = "civic" in owned_scopes
        opening_authoritative = "opening_strategy" in owned_scopes
        settler_authoritative = "settler" in owned_scopes
        city_roles_authoritative = "city_roles" in owned_scopes
        if research_authoritative or civic_authoritative:
            context = dict(context)
            strategy = context.get("strategy")
            if isinstance(strategy, dict):
                strategy = dict(strategy)
                if research_authoritative:
                    for key in (
                        "research",
                        "research_queue",
                        "tech",
                        "tech_queue",
                        "technology",
                    ):
                        strategy.pop(key, None)
                if civic_authoritative:
                    strategy.pop("civic_queue", None)
                context["strategy"] = strategy
        relevant_state, relevant_plans, max_tasks = project_agent_context(
            snapshot, events, context
        )
        if city_roles_authoritative:
            relevant_plans = dict(relevant_plans)
            relevant_plans.pop("cities", None)
        allowed_action_types = set(self.config.allowed_action_types)
        if research_authoritative:
            allowed_action_types.discard("set_research")
        if civic_authoritative:
            allowed_action_types.discard("set_civic")
        if settler_authoritative:
            allowed_action_types.difference_update({"unit_move", "unit_found_city"})
        if city_roles_authoritative:
            allowed_action_types.discard("city_set_production")
        argument_contracts = action_argument_contracts(allowed_action_types)
        entity_type_contracts = action_entity_type_contracts(allowed_action_types)
        entity_id_contracts = entity_id_argument_contracts(entity_type_contracts)
        required_condition_contracts = condition_contracts(allowed_action_types)
        information_contracts = information_tool_argument_contracts()
        strategy_queue_fields = {}
        if not civic_authoritative:
            strategy_queue_fields["civic_queue"] = "ordered CIVIC_* type names"
        if not research_authoritative:
            strategy_queue_fields["research_queue"] = "ordered TECH_* type names"
        return AgentRequest(
            turn=snapshot.turn,
            execution_mode=self.config.execution_mode,
            trigger_events=events,
            current_strategy=context.get("strategy", {}),
            current_plans=relevant_plans,
            relevant_state=relevant_state,
            constraints={
                "allowed_action_types": sorted(allowed_action_types),
                "action_argument_contracts": argument_contracts,
                "action_entity_types": entity_type_contracts,
                "entity_id_arguments": entity_id_contracts,
                "condition_contracts": required_condition_contracts,
                "supported_condition_types": [
                    "turn_at_least",
                    "turn_equals",
                    "no_blocker_type",
                    "field_equals",
                    "field_in",
                    "entity_exists",
                    "city_production_equals",
                    "city_has_no_production",
                    "research_unselected",
                    "research_available",
                    "research_equals",
                    "civic_unselected",
                    "civic_available",
                    "civic_equals",
                    "unit_at",
                    "unit_has_moves",
                    "unit_no_moves",
                    "unit_has_build_charge",
                    "unit_build_charges_equals",
                    "unit_can_improve",
                ],
                "supported_lease_condition_types": sorted(LEASE_CONDITION_TYPES),
                "strategy_queue_fields": strategy_queue_fields,
                "strategy_updates_allowed": not opening_authoritative,
                "task_postconditions_required": True,
                "max_tasks": max_tasks,
                "max_agent_calls_this_turn": self.config.max_agent_calls_per_turn,
                "forbidden_domains": [
                    "declare_war",
                    "peace",
                    "diplomacy_accept",
                    "world_congress",
                    "policy_rebuild",
                    "city_capture",
                    "city_placement",
                    "purchase",
                ],
                "event_resolution_required": True,
                "planning_phase": "initial",
                "allow_information_requests": True,
                "allowed_information_tools": list(information_contracts),
                "information_tool_arguments": information_contracts,
            },
        )

    def _uncertain_tasks(self, game_id: str) -> list[StoredTask]:
        return self.store.list_tasks(game_id, statuses=[TaskStatus.UNCERTAIN])

    def _may_end_turn(self, snapshot: RuntimeSnapshot, result: TickResult) -> bool:
        if self.config.execution_mode is ExecutionMode.READONLY:
            return False
        if not self.config.auto_end_turn or result.paused or result.agent_invoked:
            return False
        if self.store.unresolved_action_attempt(snapshot.game_id) is not None:
            return False
        if snapshot.blockers or any(event.blocking for event in result.events):
            return False
        blocking = [
            TaskStatus.READY,
            TaskStatus.RUNNING,
            TaskStatus.VERIFYING,
            TaskStatus.UNCERTAIN,
            TaskStatus.AWAITING_CONFIRMATION,
        ]
        return not any(
            task.due_turn <= snapshot.turn
            and (task.expires_turn is None or task.expires_turn >= snapshot.turn)
            for task in self.store.list_tasks(snapshot.game_id, statuses=blocking)
        )

    def _human_wait_resume_reason(
        self, observation: NormalizedRuntimeObservation
    ) -> str | None:
        """Return the durable trigger that permits one human-wait re-evaluation."""

        context = self.store.human_wait_context(observation.snapshot.game_id)
        if context is None:
            # Legacy records predate a durable comparison baseline. Reconcile once
            # from the fresh observation and immediately write a v1 baseline.
            return "legacy human wait has no durable comparison baseline"
        if context.get("wait_kind") == "strategic_contract_proposal_ready":
            if context.get("resume_requested") is True:
                return "explicit user resume was requested"
            return None
        if context.get("resume_requested") is True:
            return "explicit user resume was requested"
        if (
            context.get("execution_mode") != ExecutionMode.AUTO.value
            and self.config.execution_mode is ExecutionMode.AUTO
        ):
            return "execution mode changed to auto"
        if context.get("observation_projection_hash") != self._human_wait_hash(
            observation.snapshot
        ):
            return "current normalized observation materially changed"
        return None

    def _human_wait_context(self, snapshot: RuntimeSnapshot) -> dict[str, Any]:
        return {
            "version": "human-wait/v1",
            "execution_mode": self.config.execution_mode.value,
            "observation_projection_version": HUMAN_WAIT_PROJECTION_VERSION,
            "observation_projection_hash": self._human_wait_hash(snapshot),
            "requires_unit_details": snapshot.units is not None,
            "resume_requested": False,
        }

    @staticmethod
    def _human_wait_hash(snapshot: RuntimeSnapshot) -> str:
        """Hash normalized facts that can remove or materially change a wait."""

        projection = {
            "version": HUMAN_WAIT_PROJECTION_VERSION,
            "game_id": snapshot.game_id,
            "turn": snapshot.turn,
            "cities": snapshot.cities,
            "tech_civics": snapshot.tech_civics,
            "units": snapshot.units,
            "blockers": snapshot.blockers,
            "notifications": snapshot.notifications,
            "diplomacy": snapshot.diplomacy,
            "trades": snapshot.trades,
        }
        encoded = json.dumps(
            projection,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _end_turn_reflections(
        observation: NormalizedRuntimeObservation,
    ) -> dict[str, str]:
        """Build the upstream diary fields from current normalized facts only."""

        canonical = observation.canonical
        snapshot = observation.snapshot
        city_count = len(canonical.cities)
        production = (
            ", ".join(
                f"{city.entity_id.value}:{city.production.value or city.production.state.value}"
                for city in canonical.cities
            )
            or "none"
        )
        unit_summary = canonical.unit_summary
        research = (
            canonical.progression.current_research.value
            or canonical.progression.current_research.state.value
        )
        civic = (
            canonical.progression.current_civic.value
            or canonical.progression.current_civic.state.value
        )
        return {
            "tactical": (
                f"Turn {snapshot.turn}: {city_count} city(s), production {production}; "
                f"{len(unit_summary.actionable_unit_ids)} actionable unit(s)."
            ),
            "strategic": (
                f"Research {research}; civic {civic}; {city_count} city(s) are active."
            ),
            "tooling": "No tool errors observed; current end-turn checks passed.",
            "planning": (
                "After transition, reobserve and continue approved work or route new blockers."
            ),
            "hypothesis": (
                "If no new mandatory blocker appears, current research and production remain valid next turn."
            ),
        }

    def _end_turn_rejection_suppression(
        self, observation: NormalizedRuntimeObservation
    ) -> str | None:
        snapshot = observation.snapshot
        attempt = self.store.latest_attempt_for_task(
            snapshot.game_id, f"end_turn:{snapshot.turn}"
        )
        if attempt is None or not self._is_explicit_end_turn_rejection(attempt):
            return None
        if self.store.get_meta(self._end_turn_retry_key(attempt), False):
            return None

        arguments = attempt.normalized_arguments
        rejected_hash = arguments.get("authorization_projection_hash")
        projection_version = arguments.get("authorization_projection_version")
        if (
            projection_version != END_TURN_AUTHORIZATION_PROJECTION_VERSION
            or not isinstance(rejected_hash, str)
            or not rejected_hash
        ):
            return (
                "end turn remains suppressed because the current-turn rejection "
                "lacks a comparable authorization projection; explicit retry is required "
                f"({attempt.action_attempt_id})"
            )

        current_hash = self._end_turn_authorization_hash(observation)
        if current_hash != rejected_hash:
            return None
        return (
            "end turn remains suppressed because the current-turn attempt was "
            f"explicitly rejected and authorization state is unchanged "
            f"({attempt.action_attempt_id})"
        )

    @staticmethod
    def _is_explicit_end_turn_rejection(attempt: ActionAttempt) -> bool:
        return (
            attempt.action_type == "end_turn"
            and attempt.status is AttemptStatus.FAILED
            and attempt.transport_result is not None
            and attempt.transport_result.get("delivery_status")
            == MutationDeliveryStatus.EXPLICITLY_REJECTED.value
        )

    @staticmethod
    def _end_turn_retry_key(attempt: ActionAttempt) -> str:
        return f"end_turn_explicit_retry:{attempt.action_attempt_id}"

    @staticmethod
    def _end_turn_authorization_hash(
        observation: NormalizedRuntimeObservation,
    ) -> str:
        canonical = observation.canonical
        snapshot = observation.snapshot
        projection = {
            "version": END_TURN_AUTHORIZATION_PROJECTION_VERSION,
            "normalization_version": canonical.normalization_version,
            "cities": sorted(
                (
                    {
                        "city_id": city.entity_id.value,
                        "production_state": city.production.state.value,
                        "production_value": city.production.value,
                    }
                    for city in canonical.cities
                ),
                key=lambda row: row["city_id"],
            ),
            "progression": {
                "research_state": canonical.progression.current_research.state.value,
                "research_value": canonical.progression.current_research.value,
                "civic_state": canonical.progression.current_civic.state.value,
                "civic_value": canonical.progression.current_civic.value,
            },
            "units": (
                None
                if canonical.units is None
                else sorted(
                    (
                        {
                            "unit_id": unit.entity_id.value,
                            "unit_type": unit.unit_type,
                            "action_state": unit.action_state.value,
                            "moves_remaining": unit.moves_remaining,
                            "x": unit.values.get("x"),
                            "y": unit.values.get("y"),
                            "needs_promotion": unit.values.get("needs_promotion"),
                        }
                        for unit in canonical.units
                    ),
                    key=lambda row: row["unit_id"],
                )
            ),
            "blockers": sorted(
                (blocker.model_dump(mode="json") for blocker in canonical.blockers),
                key=lambda row: json.dumps(row, sort_keys=True, default=str),
            ),
            "notifications": snapshot.notifications,
            "diplomacy": snapshot.diplomacy,
            "trades": snapshot.trades,
        }
        encoded = json.dumps(
            projection,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()
