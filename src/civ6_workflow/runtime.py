from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, Callable
from uuid import uuid4

from .actions import (
    canonical_action_types,
)
from .batch_executor import BatchExecutor, ExecutionTransition
from .conditions import ConditionEvaluator
from .domain import (
    ActionAttempt,
    AttemptStatus,
    AwaitingHumanTick,
    AwaitingApprovalTick,
    NoSafeActionTick,
    RuntimeState,
    StrategicProposalWaitErrorTick,
    StrategicProposalWaitResumedTick,
    StrategicRequestWaitErrorTick,
    StrategicRequestWaitResumedTick,
    SystemErrorTick,
    TickOutcomeKind,
    validate_workflow_tick,
)
from .events import events_from_observation
from .gate import EventGate
from .ports import (
    GamePort,
    MutationBudget,
    Planner,
    WorkflowStorePort,
)
from .models import (
    EventLevel,
    ExecutionMode,
    MutationDeliveryStatus,
    RuntimeSnapshot,
    TurnActionExecution,
    TaskStatus,
    TickResult,
)
from .observation_normalization import (
    NormalizedRuntimeObservation,
    normalize_runtime_snapshot,
)
from .recovery import recover_turn_rewind
from .runtime_errors import FatalTickPersistenceError, InjectedCrashBoundary
from .strategic_workflow import StrategicWorkflowCoordinator
from .workflow_protocol import (
    WorkflowTickMetrics as TickMetrics,
)
from .workflow_queries import InformationQueryRouter


END_TURN_AUTHORIZATION_PROJECTION_VERSION = "end-turn-authorization/v1"
HUMAN_WAIT_PROJECTION_VERSION = "human-wait-observation/v1"
_TRANSIENT_HTTP = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class RuntimeConfig:
    execution_mode: ExecutionMode = ExecutionMode.CONFIRM
    auto_end_turn: bool = False
    max_new_planner_requests_per_turn: int | None = None
    max_agent_calls_per_turn: int = 1
    max_provider_attempts_per_turn: int = 6
    max_provider_attempts_per_logical_request: int = 6
    max_abandoned_provider_attempts_per_request: int = 1
    max_information_rounds_per_request: int = 1
    max_turn_seconds: float = 300.0
    mcp_mutation_timeout_seconds: float = 30.0
    repeated_failure_threshold: int = 2
    default_cooldown_turns: int = 2
    verification_attempts: int = 3
    verification_delay_seconds: float = 0.25
    auto_action_types: set[str] = field(
        default_factory=lambda: set(canonical_action_types())
    )
    allowed_action_types: set[str] = field(
        default_factory=lambda: set(canonical_action_types())
    )
    allowed_tools: set[str] = field(
        default_factory=lambda: {
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
        }
    )

    def __post_init__(self) -> None:
        if (
            self.max_new_planner_requests_per_turn is not None
            and self.max_new_planner_requests_per_turn < 0
        ):
            raise ValueError("max_new_planner_requests_per_turn must be non-negative")
        for name in (
            "max_agent_calls_per_turn",
            "max_provider_attempts_per_turn",
            "max_provider_attempts_per_logical_request",
            "max_abandoned_provider_attempts_per_request",
            "max_information_rounds_per_request",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.max_turn_seconds <= 0:
            raise ValueError("max_turn_seconds must be positive")
        if self.mcp_mutation_timeout_seconds <= 0:
            raise ValueError("mcp_mutation_timeout_seconds must be positive")
        if self.mcp_mutation_timeout_seconds >= self.max_turn_seconds:
            raise ValueError(
                "mcp_mutation_timeout_seconds must be less than max_turn_seconds"
            )
        canonical = set(canonical_action_types())
        unknown_allowed = self.allowed_action_types - canonical
        if unknown_allowed:
            raise ValueError(
                "allowed_action_types contain non-canonical actions: "
                f"{sorted(unknown_allowed)}"
            )
        if not self.auto_action_types <= self.allowed_action_types:
            extra = sorted(self.auto_action_types - self.allowed_action_types)
            raise ValueError(
                "auto_action_types must be a subset of allowed_action_types; "
                f"extra={extra}"
            )

    @property
    def new_planner_request_limit(self) -> int:
        if self.max_new_planner_requests_per_turn is not None:
            return self.max_new_planner_requests_per_turn
        return self.max_agent_calls_per_turn


@dataclass(frozen=True, slots=True)
class RuntimeServices:
    """Application services supplied by the canonical composition root."""

    gate: EventGate
    conditions: ConditionEvaluator
    batch_executor: BatchExecutor
    information_queries: InformationQueryRouter
    strategic_workflow: StrategicWorkflowCoordinator


@dataclass(slots=True)
class _TickContext:
    tick_id: str
    started_at: datetime
    started_monotonic: float
    call_count_before: int
    external_counts_before: dict[str, float | int]
    metrics: TickMetrics
    budget: MutationBudget
    starting_state: RuntimeState = RuntimeState.OBSERVING
    observation_ids: list[str] = field(default_factory=list)
    resuming_human_wait: bool = False


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


class WorkflowRuntime:
    """Canonical bounded runtime; TickResult is only a compatibility envelope."""

    def __init__(
        self,
        *,
        store: WorkflowStorePort,
        game: GamePort,
        planner: Planner,
        config: RuntimeConfig | None = None,
        clock: Any | None = None,
        crash_injector: Any | None = None,
        service_factory: Callable[[WorkflowRuntime], RuntimeServices],
    ):
        self.store = store
        self.game = game
        self.planner = planner
        self.config = config or RuntimeConfig()
        self.clock = clock
        self.crash_injector = crash_injector
        self._available_tools: set[str] | None = None
        self._available_tools_epoch: object | None = None
        self._active_observation_id: str | None = None
        services = service_factory(self)
        self.gate = services.gate
        self.conditions = services.conditions
        self.batch_executor = services.batch_executor
        self.information_queries = services.information_queries
        self.strategic_workflow = services.strategic_workflow

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
            result = await asyncio.wait_for(
                self._tick_once(),
                timeout=self.config.max_turn_seconds,
            )
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
            external_counts_before=self._external_call_metrics(),
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
        observation = self._normalize_snapshot(
            raw, ctx.metrics, observed_at=ctx.started_at
        )
        snapshot = observation.snapshot
        observation_id = self._observation_id(observation)
        self._active_observation_id = observation_id
        ctx.observation_ids.append(observation_id)
        ctx.starting_state = self.store.load_runtime_state(snapshot.game_id)
        previous_game_id = self.store.get_meta("last_game_id")
        previous_turn = self.store.get_meta("last_observed_turn")
        rewind_pending = (
            previous_game_id == snapshot.game_id
            and isinstance(previous_turn, int)
            and snapshot.turn < previous_turn
        )
        if not rewind_pending:
            self.store.set_meta("last_game_id", snapshot.game_id)
            self.store.set_meta("last_observed_turn", snapshot.turn)
        unresolved = self.store.unresolved_action_attempt(snapshot.game_id)
        if rewind_pending and unresolved is not None:
            reason = (
                "the game turn rewound while an action outcome is unresolved; "
                "the old timeline cannot be used for verification"
            )
            wait = self.store.human_wait_context(snapshot.game_id) or {}
            if (
                ctx.starting_state is RuntimeState.AWAITING_HUMAN
                and wait.get("wait_kind") == "turn_rewind_with_unresolved_attempt"
                and wait.get("action_attempt_id") == unresolved.action_attempt_id
            ):
                return self._held_result(
                    ctx, snapshot, state=RuntimeState.AWAITING_HUMAN, reason=reason
                )
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                blocking_reason=reason,
                human_wait_context_override={
                    "version": "human-wait/v1",
                    "wait_kind": "turn_rewind_with_unresolved_attempt",
                    "resume_policy": "explicit_only",
                    "action_attempt_id": unresolved.action_attempt_id,
                    "resume_requested": False,
                },
            )
        if unresolved is not None:
            unresolved_task = self.store.get_task(snapshot.game_id, unresolved.task_id)
            if (
                unresolved_task is not None
                and unresolved_task.entity_type in {"unit", "builder"}
                and snapshot.units is None
            ):
                raw = await self._read_snapshot(ctx.metrics, include_units=True)
                observation = self._normalize_snapshot(
                    raw, ctx.metrics, observed_at=ctx.started_at
                )
                snapshot = observation.snapshot
                observation_id = self._observation_id(observation)
                self._active_observation_id = observation_id
                ctx.observation_ids.append(observation_id)
            if (
                getattr(unresolved, "status", None) is AttemptStatus.UNCERTAIN
                and unresolved.verification_count >= self.config.verification_attempts
                and ctx.starting_state is RuntimeState.AWAITING_HUMAN
                and getattr(unresolved, "last_verification_projection_hash", None)
                == observation.canonical.projection_hash
            ):
                return self._held_result(
                    ctx,
                    snapshot,
                    state=RuntimeState.AWAITING_HUMAN,
                    reason=(
                        "action verification remains inconclusive; a materially new "
                        "Observation or explicit reconciliation is required"
                    ),
                )
            if (
                getattr(unresolved, "last_verification_projection_hash", None)
                == observation.canonical.projection_hash
                and getattr(unresolved, "verified_at", None) is not None
                and self.config.verification_delay_seconds > 0
                and self._now()
                < unresolved.verified_at
                + timedelta(seconds=self.config.verification_delay_seconds)
            ):
                self._finalize_metrics(ctx)
                return TickResult(
                    turn=snapshot.turn,
                    metrics=ctx.metrics,
                    runtime_state=ctx.starting_state.value,
                )
            self.store.save_normalized_observation(observation.canonical)
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
        if ctx.starting_state in {RuntimeState.SYSTEM_ERROR, RuntimeState.PAUSED}:
            return self._held_result(
                ctx,
                snapshot,
                state=ctx.starting_state,
                reason=f"runtime is halted in {ctx.starting_state.value}",
            )

        active_request = self.store.active_planner_request(snapshot.game_id)
        if (
            active_request is not None
            and active_request.status.value == "BACKOFF"
            and active_request.next_retry_at is not None
            and active_request.target.kind.value == "STRATEGIC_CONTRACT_CREATION"
            and self.store.get_active_strategic_contract(snapshot.game_id) is None
            and (
                active_request.input_projection.get(
                    "source_observation_projection_hash"
                )
                in {None, observation.canonical.projection_hash}
            )
            and active_request.next_retry_at > self._now()
        ):
            return self._held_result(
                ctx,
                snapshot,
                state=RuntimeState.PLANNER_BACKOFF,
                reason=f"planner backoff until {active_request.next_retry_at.isoformat()}",
            )

        if ctx.starting_state is RuntimeState.AWAITING_APPROVAL:
            active_graph = self.store.active_turn_action_graph(snapshot.game_id)
            active_contract = self.store.get_active_strategic_contract(snapshot.game_id)
            if active_graph is not None:
                graph, tasks = active_graph
                awaiting = [
                    task
                    for task in tasks
                    if task.status is TaskStatus.AWAITING_CONFIRMATION
                ]
                graph_is_fresh = (
                    graph.turn_number == snapshot.turn
                    and graph.source_observation_projection_hash
                    == observation.canonical.projection_hash
                    and active_contract is not None
                    and graph.source_contract_revision == active_contract.revision
                )
                if awaiting and graph_is_fresh:
                    return self._held_result(
                        ctx,
                        snapshot,
                        state=RuntimeState.AWAITING_APPROVAL,
                        reason="task approval is required",
                    )

        recover_session = getattr(self.game, "recover_mutation_session", None)
        if recover_session is not None:
            await recover_session()
        await self._verify_tool_surface()

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
                observation = self._normalize_snapshot(
                    raw, ctx.metrics, observed_at=ctx.started_at
                )
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
        need_units = (
            observation.canonical.unit_summary.detail_required
            or self.strategic_workflow.requires_unit_details(snapshot.game_id)
        )
        if need_units and snapshot.units is None:
            raw = await self._read_snapshot(ctx.metrics, include_units=True)
            observation = self._normalize_snapshot(
                raw, ctx.metrics, observed_at=ctx.started_at
            )
            snapshot = observation.snapshot
            observation_id = self._observation_id(observation)
            self._active_observation_id = observation_id
            ctx.observation_ids.append(observation_id)

        projection = await self.strategic_workflow.prepare_projection(
            ctx,
            observation,
            snapshot_events=tuple(events_from_observation(observation.canonical)),
            mode=self.config.execution_mode,
            auto_action_types=self.config.auto_action_types,
            allowed_action_types=self.config.allowed_action_types,
        )
        if projection.lifecycle_tick is not None:
            return projection.lifecycle_tick
        if projection.human_wait_reason is not None:
            gate = self.gate.ingest(
                snapshot.game_id, list(projection.human_wait_events)
            )
            human_wait = TickResult(
                turn=snapshot.turn,
                metrics=ctx.metrics,
                events=gate.emitted,
                paused=True,
                pause_reason=projection.human_wait_reason,
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
            observation = self._normalize_snapshot(
                raw, ctx.metrics, observed_at=ctx.started_at
            )
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

        rewind_event = recover_turn_rewind(
            self.store,
            snapshot,
            previous_game_id=previous_game_id,
            previous_turn=previous_turn,
            recovered_at=observation.canonical.observed_at,
        )
        events = [] if rewind_event is None else [rewind_event]
        events.extend(projection.current_events)
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
        agent_events, planning_tick = await self.strategic_workflow.advance_planning(
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
        if self._may_end_turn(
            snapshot,
            compat,
            pending_repair_scopes=projection.pending_repair_scopes,
        ):
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
        human_wait_context_override: Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> TickResult:
        completed = self._now()
        self._finalize_metrics(ctx)
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
        human_wait_context = (
            None
            if human_wait_context_override is None
            else dict(human_wait_context_override)
        )
        if isinstance(
            tick,
            (
                AwaitingHumanTick,
                StrategicProposalWaitErrorTick,
                StrategicRequestWaitErrorTick,
            ),
        ):
            existing_wait = (
                None
                if human_wait_context is not None
                else self.store.human_wait_context(snapshot.game_id)
            )
            if existing_wait is not None and (
                (
                    existing_wait.get("wait_kind")
                    == "strategic_contract_proposal_ready"
                    and existing_wait.get("resume_policy") == "explicit_only"
                )
                or existing_wait.get("wait_kind") == "strategic_request_terminated"
            ):
                human_wait_context = dict(existing_wait)
            elif human_wait_context is None:
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
                AwaitingApprovalTick,
                StrategicProposalWaitErrorTick,
                StrategicRequestWaitErrorTick,
                SystemErrorTick,
            ),
        ):
            result.paused = True
            result.pause_reason = tick.blocking_reason
        return result

    def _held_result(
        self,
        ctx: _TickContext,
        snapshot: RuntimeSnapshot,
        *,
        state: RuntimeState,
        reason: str,
    ) -> TickResult:
        self._finalize_metrics(ctx)
        return TickResult(
            turn=snapshot.turn,
            paused=True,
            pause_reason=reason,
            metrics=ctx.metrics,
            runtime_state=state.value,
        )

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

    def _normalize_snapshot(
        self,
        snapshot: RuntimeSnapshot,
        metrics: TickMetrics,
        *,
        observed_at: datetime,
    ) -> NormalizedRuntimeObservation:
        started = time.perf_counter()
        observation = normalize_runtime_snapshot(snapshot, observed_at=observed_at)
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
        epoch = getattr(self.game, "tool_surface_epoch", None)
        if self._available_tools is not None and epoch == self._available_tools_epoch:
            return
        self._available_tools = await self.game.list_tools()
        self._available_tools_epoch = epoch
        fallback_queries = {
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
        }
        missing = fallback_queries - self._available_tools
        if missing:
            raise RuntimeError(f"civ6-mcp is missing required tools: {sorted(missing)}")

    def _finalize_metrics(self, ctx: _TickContext) -> None:
        external_counts = self._external_call_metrics()
        for name in (
            "state_api_call_count",
            "mcp_list_tools_count",
            "mcp_read_query_count",
            "mcp_mutation_count",
            "mcp_timeout_count",
            "mcp_reconnect_count",
        ):
            setattr(
                ctx.metrics,
                name,
                int(external_counts.get(name, 0))
                - int(ctx.external_counts_before.get(name, 0)),
            )
        ctx.metrics.mcp_mutation_seconds = max(
            0.0,
            float(external_counts.get("mcp_mutation_seconds", 0.0))
            - float(ctx.external_counts_before.get("mcp_mutation_seconds", 0.0)),
        )
        ctx.metrics.mcp_call_count = (
            ctx.metrics.mcp_list_tools_count
            + ctx.metrics.mcp_read_query_count
            + ctx.metrics.mcp_mutation_count
        )
        if not external_counts:
            ctx.metrics.mcp_call_count = self.game.call_count - ctx.call_count_before
        ctx.metrics.mutation_count = ctx.budget.used
        ctx.metrics.total_seconds = self._monotonic() - ctx.started_monotonic

    def _external_call_metrics(self) -> dict[str, float | int]:
        metrics = getattr(self.game, "call_metrics", None)
        if not isinstance(metrics, dict):
            return {}
        return dict(metrics)

    def _uncertain_tasks(self, game_id: str) -> list[TurnActionExecution]:
        return self.store.list_tasks(game_id, statuses=[TaskStatus.UNCERTAIN])

    def _may_end_turn(
        self,
        snapshot: RuntimeSnapshot,
        result: TickResult,
        *,
        pending_repair_scopes: frozenset[str] = frozenset(),
    ) -> bool:
        if self.config.execution_mode is ExecutionMode.READONLY:
            return False
        if not self.config.auto_end_turn or result.paused or result.agent_invoked:
            return False
        if self.store.unresolved_action_attempt(snapshot.game_id) is not None:
            return False
        if self.store.get_active_strategic_contract(snapshot.game_id) is None:
            return False
        if pending_repair_scopes:
            return False
        if self.store.active_planner_request(snapshot.game_id) is not None:
            return False
        if (
            not snapshot.blockers_loaded
            or snapshot.blockers
            or any(event.blocking for event in result.events)
        ):
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

        authorization = attempt.authorization_evidence
        rejected_hash = authorization.get("projection_hash")
        projection_version = authorization.get("projection_version")
        if not authorization:
            # Replay compatibility for attempts written before authorization
            # evidence was separated from the transmitted tool arguments.
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
