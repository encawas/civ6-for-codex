from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .actions import (
    ACTION_REGISTRY,
    END_TURN_ACTION_SPEC,
    ActionValidationError,
    resolve_action,
    resolve_action_spec,
)
from .codex_planner import Planner
from .conditions import ConditionEvaluator, extract_known_entities
from .domain import (
    ActionAttempt,
    AttemptRecoveredTick,
    AttemptReconciledTick,
    AttemptStatus,
    AwaitingApprovalTick,
    AwaitingHumanTick,
    AwaitingVerificationTick,
    MutationRejectedTick,
    MutationSentTick,
    MutationUncertainTick,
    NoSafeActionTick,
    PlanRequestedTick,
    RetryClassification,
    RuntimeState,
    TaskCreatedTick,
    TaskInvalidatedTick,
    TurnTransitionConfirmedTick,
    TurnTransitionStartedTick,
    TurnTransitionWaitingTick,
    VerificationStatus,
    validate_workflow_tick,
)
from .events import events_from_snapshot
from .gate import EventGate
from .mcp_port import BoundedGamePort, GamePort, MutationBudget
from .models import (
    AgentRequest,
    EventLevel,
    ExecutionMode,
    GameEvent,
    MutationDeliveryStatus,
    PlanBundle,
    RuntimeSnapshot,
    StoredTask,
    TaskStatus,
    TickMetrics,
    TickResult,
)
from .observation_normalization import (
    NormalizedRuntimeObservation,
    normalize_runtime_snapshot,
)
from .progression import ProgressionRuleCompiler
from .recovery import recover_turn_rewind
from .rules import DeterministicRuleCompiler
from .store import WorkflowStore
from .validation import PlanValidationContext, validate_plan_bundle


@dataclass(slots=True)
class EngineConfig:
    execution_mode: ExecutionMode = ExecutionMode.CONFIRM
    auto_end_turn: bool = False
    max_agent_calls_per_turn: int = 1
    repeated_failure_threshold: int = 2
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


class WorkflowEngine:
    """Canonical bounded runtime; TickResult is only a compatibility envelope."""

    def __init__(
        self,
        *,
        store: WorkflowStore,
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
        self.gate = EventGate(store)
        self.conditions = ConditionEvaluator()
        self.rules = DeterministicRuleCompiler(store)
        self.progression = ProgressionRuleCompiler(store)
        self._available_tools: set[str] | None = None
        self._active_observation_id: str | None = None

    async def tick(self) -> TickResult:
        ctx = _TickContext(
            tick_id=f"tick_{uuid4().hex}",
            started_at=self._now(),
            started_monotonic=self._monotonic(),
            call_count_before=self.game.call_count,
            metrics=TickMetrics(),
            budget=MutationBudget(),
        )
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
            return self._reconcile_attempt(ctx, observation, unresolved)

        existing_due = self.store.due_tasks(snapshot.game_id, snapshot.turn)
        need_units = (
            observation.canonical.unit_summary.detail_required
            or self.rules.needs_units(snapshot.game_id)
            or any(task.entity_type in {"unit", "builder"} for task in existing_due)
        )
        if need_units and snapshot.units is None:
            raw = await self._read_snapshot(ctx.metrics, include_units=True)
            observation = self._normalize_snapshot(raw, ctx.metrics)
            snapshot = observation.snapshot
            observation_id = self._observation_id(observation)
            self._active_observation_id = observation_id
            ctx.observation_ids.append(observation_id)

        before = self.store.task_ids(snapshot.game_id)
        materialization_started = self._monotonic()
        rule_compilation = self.rules.compile(observation)
        progression_compilation = self.progression.compile(observation)
        for compilation in (rule_compilation, progression_compilation):
            if compilation.bundle is not None:
                self.store.save_plan_bundle(
                    snapshot.game_id,
                    snapshot.turn,
                    compilation.bundle,
                    mode=self.config.execution_mode,
                    auto_action_types=self.config.auto_action_types,
                    observation_id=observation_id,
                )
        ctx.metrics.task_materialization_seconds += (
            self._monotonic() - materialization_started
        )
        created = sorted(self.store.task_ids(snapshot.game_id) - before)
        if created:
            return self._finish(ctx, snapshot, TaskCreatedTick, task_id=created[0])

        awaiting = self.store.list_tasks(
            snapshot.game_id, statuses=[TaskStatus.AWAITING_CONFIRMATION]
        )
        if awaiting and awaiting[0].due_turn <= snapshot.turn:
            return self._finish(
                ctx,
                snapshot,
                AwaitingApprovalTick,
                proposal_id=awaiting[0].task_id,
                blocking_reason="task approval is required",
            )

        due_tasks = self.store.due_tasks(snapshot.game_id, snapshot.turn)
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

        if self.config.execution_mode is not ExecutionMode.READONLY and due_tasks:
            task = due_tasks[0]
            if task.status is TaskStatus.AWAITING_CONFIRMATION:
                return self._finish(
                    ctx,
                    snapshot,
                    AwaitingApprovalTick,
                    proposal_id=task.task_id,
                    blocking_reason="task approval is required",
                )
            invalid = self._task_invalidation(task, observation)
            if invalid is not None:
                self.store.set_task_status(
                    snapshot.game_id, task.task_id, TaskStatus.CANCELLED, error=invalid
                )
                return self._finish(
                    ctx,
                    snapshot,
                    TaskInvalidatedTick,
                    task_id=task.task_id,
                    blocking_reason=invalid,
                )
            return await self._send_task(ctx, observation, task)

        rewind_event = recover_turn_rewind(self.store, snapshot)
        events = [] if rewind_event is None else [rewind_event]
        events.extend(rule_compilation.events)
        events.extend(progression_compilation.events)
        events.extend(events_from_snapshot(snapshot))
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
        already_called = self.store.agent_called_for_turn(
            snapshot.game_id, snapshot.turn
        )
        if (
            agent_events
            and not already_called
            and self.config.max_agent_calls_per_turn > 0
        ):
            tasks_before_planner = self.store.task_ids(snapshot.game_id)
            await self._invoke_planner(snapshot, agent_events, compat, ctx.metrics)
            planner_created = sorted(
                self.store.task_ids(snapshot.game_id) - tasks_before_planner
            )
            if planner_created:
                return self._finish(
                    ctx,
                    snapshot,
                    TaskCreatedTick,
                    compatibility=compat,
                    task_id=planner_created[0],
                )
            if compat.planner_request_id is not None:
                return self._finish(
                    ctx,
                    snapshot,
                    PlanRequestedTick,
                    compatibility=compat,
                    planner_request_id=compat.planner_request_id,
                )
        if compat.paused:
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                compatibility=compat,
                blocking_reason=compat.pause_reason or "human review is required",
            )
        if self._may_end_turn(snapshot, compat):
            return await self._send_end_turn(ctx, observation)
        return self._finish(
            ctx,
            snapshot,
            NoSafeActionTick,
            compatibility=compat,
            blocking_reason="no safe action is available",
        )

    async def _send_task(
        self,
        ctx: _TickContext,
        observation: NormalizedRuntimeObservation,
        task: StoredTask,
    ) -> TickResult:
        snapshot = observation.snapshot
        try:
            spec = resolve_action_spec(task.action_type)
            _, normalized_arguments = resolve_action(
                task, self._available_tools or set()
            )
        except ActionValidationError as exc:
            self.store.set_task_status(
                snapshot.game_id, task.task_id, TaskStatus.CANCELLED, error=str(exc)
            )
            return self._finish(
                ctx,
                snapshot,
                TaskInvalidatedTick,
                task_id=task.task_id,
                blocking_reason=str(exc),
            )

        parent = self.store.latest_attempt_for_task(snapshot.game_id, task.task_id)
        now = self._now()
        attempt = ActionAttempt(
            action_attempt_id=f"attempt_{uuid4().hex}",
            game_session_id=snapshot.game_id,
            task_id=task.task_id,
            action_type=task.action_type,
            attempt_number=self.store.next_attempt_number(
                snapshot.game_id, task.task_id
            ),
            request_id=f"request_{uuid4().hex}",
            idempotency_key=self._idempotency_key(task, normalized_arguments),
            prepared_from_observation_id=self._active_observation_id or "missing",
            prepared_at=now,
            status=AttemptStatus.PREPARED,
            retry_classification=spec.retry_classification,
            normalized_arguments=normalized_arguments,
            postconditions=tuple(task.postconditions),
            parent_attempt_id=None if parent is None else parent.action_attempt_id,
        )
        persistence_started = self._monotonic()
        self.store.save_action_attempt(attempt)
        self.store.set_task_status(snapshot.game_id, task.task_id, TaskStatus.RUNNING)
        ctx.metrics.persistence_seconds += self._monotonic() - persistence_started
        self._checkpoint("after_attempt_prepared")

        delivery_started = self._replace_attempt(
            attempt,
            status=AttemptStatus.UNCERTAIN,
            sent_at=self._now(),
            transport_result={"phase": "delivery_started"},
        )
        self.store.update_action_attempt(delivery_started)
        self.store.save_runtime_state(
            snapshot.game_id,
            RuntimeState.RECONCILING,
            active_attempt_id=attempt.action_attempt_id,
        )
        self._checkpoint("after_delivery_started")

        bounded = BoundedGamePort(self.game, ctx.budget)
        delivery_started_at = self._monotonic()
        try:
            action_result = await bounded.execute_task(task)
        except Exception as exc:
            action_result = None
            delivery_error = exc
        else:
            delivery_error = None
        ctx.metrics.mutation_delivery_seconds += self._monotonic() - delivery_started_at
        ctx.metrics.mutation_count = ctx.budget.used
        self._checkpoint("after_port_call")

        if action_result is None:
            uncertain = self._replace_attempt(
                delivery_started,
                status=AttemptStatus.UNCERTAIN,
                transport_result={
                    "phase": "delivery_unknown",
                    "error_type": type(delivery_error).__name__,
                },
            )
            self.store.update_action_attempt(uncertain)
            self.store.set_task_status(
                snapshot.game_id,
                task.task_id,
                TaskStatus.UNCERTAIN,
                error="mutation delivery outcome is unknown",
            )
            return self._finish(
                ctx,
                snapshot,
                MutationUncertainTick,
                action_attempt_id=attempt.action_attempt_id,
                task_id=task.task_id,
                selected_operation=task.action_type,
                blocking_reason="mutation delivery outcome is unknown",
            )

        status = action_result.effective_delivery_status
        response_at = self._now()
        if status is MutationDeliveryStatus.ACKNOWLEDGED:
            verifying = self._replace_attempt(
                delivery_started,
                status=AttemptStatus.VERIFYING,
                response_received_at=response_at,
                transport_result={"delivery_status": status.value},
                tool_result=action_result.model_dump(mode="json"),
                verification_status=VerificationStatus.PENDING,
            )
            self.store.update_action_attempt(verifying)
            self.store.set_task_status(
                snapshot.game_id, task.task_id, TaskStatus.VERIFYING
            )
            return self._finish(
                ctx,
                snapshot,
                MutationSentTick,
                action_attempt_id=attempt.action_attempt_id,
                task_id=task.task_id,
                selected_operation=task.action_type,
            )

        if status is MutationDeliveryStatus.UNKNOWN:
            uncertain = self._replace_attempt(
                delivery_started,
                status=AttemptStatus.UNCERTAIN,
                response_received_at=response_at,
                transport_result={"delivery_status": status.value},
                tool_result=action_result.model_dump(mode="json"),
            )
            self.store.update_action_attempt(uncertain)
            self.store.set_task_status(
                snapshot.game_id,
                task.task_id,
                TaskStatus.UNCERTAIN,
                error=action_result.message or "mutation outcome is unknown",
            )
            return self._finish(
                ctx,
                snapshot,
                MutationUncertainTick,
                action_attempt_id=attempt.action_attempt_id,
                task_id=task.task_id,
                selected_operation=task.action_type,
                blocking_reason=action_result.message or "mutation outcome is unknown",
            )

        failed = self._replace_attempt(
            delivery_started,
            status=AttemptStatus.FAILED,
            response_received_at=response_at,
            transport_result={"delivery_status": status.value},
            tool_result=action_result.model_dump(mode="json"),
            verification_status=VerificationStatus.FAILED,
        )
        self.store.update_action_attempt(failed)
        self._apply_failed_attempt_retry(snapshot.game_id, task, failed)
        return self._finish(
            ctx,
            snapshot,
            MutationRejectedTick,
            action_attempt_id=attempt.action_attempt_id,
            task_id=task.task_id,
            selected_operation=task.action_type,
            blocking_reason=action_result.message or "game rejected mutation",
            failed_task_ids=[task.task_id],
        )

    def _reconcile_attempt(
        self,
        ctx: _TickContext,
        observation: NormalizedRuntimeObservation,
        attempt: ActionAttempt,
    ) -> TickResult:
        snapshot = observation.snapshot
        if attempt.status is AttemptStatus.PREPARED:
            task = self.store.get_task(snapshot.game_id, attempt.task_id)
            rejected = self._replace_attempt(
                attempt,
                status=AttemptStatus.REJECTED_BEFORE_SEND,
                transport_result={"recovery": "prepared commit proves no send began"},
            )
            self.store.update_action_attempt(rejected)
            if task is not None:
                self.store.set_task_status(
                    snapshot.game_id,
                    task.task_id,
                    TaskStatus.READY,
                    error="recovered a prepared attempt before delivery began",
                )
            return self._finish(
                ctx,
                snapshot,
                AttemptRecoveredTick,
                action_attempt_id=attempt.action_attempt_id,
                task_id=attempt.task_id,
            )

        if attempt.action_type == "end_turn":
            return self._reconcile_end_turn(ctx, observation, attempt)

        task = self.store.get_task(snapshot.game_id, attempt.task_id)
        if task is None:
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                action_attempt_id=attempt.action_attempt_id,
                blocking_reason="attempt task is missing",
            )

        verification_started = self._monotonic()
        result = self.conditions.evaluate_all(list(attempt.postconditions), observation)
        ctx.metrics.verification_seconds += self._monotonic() - verification_started
        observation_id = self._active_observation_id or "missing"
        if result.valid:
            succeeded = self._replace_attempt(
                attempt,
                status=AttemptStatus.SUCCEEDED,
                verification_status=VerificationStatus.PASSED,
                last_verification_observation_id=observation_id,
                verification_count=attempt.verification_count + 1,
            )
            self.store.update_action_attempt(succeeded)
            self.store.set_task_status(snapshot.game_id, task.task_id, TaskStatus.DONE)
            return self._finish(
                ctx,
                snapshot,
                AttemptReconciledTick,
                action_attempt_id=attempt.action_attempt_id,
                task_id=task.task_id,
                attempt_status=AttemptStatus.SUCCEEDED,
                executed_task_ids=[task.task_id],
            )

        preconditions = self.conditions.evaluate_all(task.preconditions, observation)
        invalidation = self._first_active_invalidator(task.invalidators, observation)
        if not preconditions.valid or invalidation is not None:
            failed = self._replace_attempt(
                attempt,
                status=AttemptStatus.FAILED,
                transport_result={
                    **dict(attempt.transport_result or {}),
                    "proven_not_committed": True,
                },
                verification_status=VerificationStatus.FAILED,
                last_verification_observation_id=observation_id,
                verification_count=attempt.verification_count + 1,
            )
            self.store.update_action_attempt(failed)
            self._apply_failed_attempt_retry(snapshot.game_id, task, failed)
            return self._finish(
                ctx,
                snapshot,
                AttemptReconciledTick,
                action_attempt_id=attempt.action_attempt_id,
                task_id=task.task_id,
                attempt_status=AttemptStatus.FAILED,
            )

        count = attempt.verification_count + 1
        if count < max(1, self.config.verification_attempts):
            verifying = self._replace_attempt(
                attempt,
                status=AttemptStatus.VERIFYING,
                verification_status=VerificationStatus.INCONCLUSIVE,
                last_verification_observation_id=observation_id,
                verification_count=count,
            )
            self.store.update_action_attempt(verifying)
            self.store.set_task_status(
                snapshot.game_id,
                task.task_id,
                TaskStatus.VERIFYING,
                error=result.reason,
            )
            return self._finish(
                ctx,
                snapshot,
                AwaitingVerificationTick,
                action_attempt_id=attempt.action_attempt_id,
                task_id=task.task_id,
            )

        uncertain = self._replace_attempt(
            attempt,
            status=AttemptStatus.UNCERTAIN,
            verification_status=VerificationStatus.INCONCLUSIVE,
            last_verification_observation_id=observation_id,
            verification_count=count,
        )
        self.store.update_action_attempt(uncertain)
        self.store.set_task_status(
            snapshot.game_id,
            task.task_id,
            TaskStatus.UNCERTAIN,
            error=result.reason,
        )
        return self._finish(
            ctx,
            snapshot,
            AwaitingHumanTick,
            action_attempt_id=attempt.action_attempt_id,
            blocking_reason=result.reason or "verification remained inconclusive",
        )

    def _apply_failed_attempt_retry(
        self, game_id: str, task: StoredTask, attempt: ActionAttempt
    ) -> None:
        if (
            attempt.retry_classification is RetryClassification.SAFE_IF_PROVEN_NOT_SENT
            and (
                (
                    attempt.tool_result is not None
                    and attempt.tool_result.get("delivery_status")
                    == MutationDeliveryStatus.PROVEN_NOT_SENT.value
                )
                or (
                    attempt.transport_result is not None
                    and attempt.transport_result.get("proven_not_committed") is True
                )
            )
        ):
            self.store.set_task_status(game_id, task.task_id, TaskStatus.READY)
            return
        self.store.set_task_status(
            game_id,
            task.task_id,
            TaskStatus.FAILED,
            error="mutation was rejected or cannot be safely retried",
        )

    async def _send_end_turn(
        self, ctx: _TickContext, observation: NormalizedRuntimeObservation
    ) -> TickResult:
        snapshot = observation.snapshot
        task_id = f"end_turn:{snapshot.turn}"
        parent = self.store.latest_attempt_for_task(snapshot.game_id, task_id)
        attempt = ActionAttempt(
            action_attempt_id=f"attempt_{uuid4().hex}",
            game_session_id=snapshot.game_id,
            task_id=task_id,
            action_type="end_turn",
            attempt_number=self.store.next_attempt_number(snapshot.game_id, task_id),
            request_id=f"request_{uuid4().hex}",
            idempotency_key=f"{snapshot.game_id}:end_turn:{snapshot.turn}",
            prepared_from_observation_id=self._active_observation_id or "missing",
            prepared_at=self._now(),
            status=AttemptStatus.PREPARED,
            retry_classification=END_TURN_ACTION_SPEC.retry_classification,
            normalized_arguments={},
            postconditions=(),
            parent_attempt_id=(None if parent is None else parent.action_attempt_id),
            pre_send_turn=snapshot.turn,
        )
        self.store.save_action_attempt(attempt)
        self._checkpoint("after_attempt_prepared")
        delivery_started = self._replace_attempt(
            attempt,
            status=AttemptStatus.UNCERTAIN,
            sent_at=self._now(),
            transport_result={"phase": "delivery_started"},
        )
        self.store.update_action_attempt(delivery_started)
        self.store.save_runtime_state(
            snapshot.game_id,
            RuntimeState.TURN_TRANSITIONING,
            active_attempt_id=attempt.action_attempt_id,
        )
        self._checkpoint("after_delivery_started")
        bounded = BoundedGamePort(self.game, ctx.budget)
        started = self._monotonic()
        try:
            action_result = await bounded.end_turn()
        except Exception as exc:
            action_result = None
            error = exc
        else:
            error = None
        ctx.metrics.mutation_delivery_seconds += self._monotonic() - started
        ctx.metrics.mutation_count = ctx.budget.used
        self._checkpoint("after_port_call")

        if action_result is not None and (
            action_result.effective_delivery_status
            is MutationDeliveryStatus.ACKNOWLEDGED
        ):
            verifying = self._replace_attempt(
                delivery_started,
                status=AttemptStatus.VERIFYING,
                response_received_at=self._now(),
                transport_result={"delivery_status": "acknowledged"},
                tool_result=action_result.model_dump(mode="json"),
                verification_status=VerificationStatus.PENDING,
            )
            self.store.update_action_attempt(verifying)
            return self._finish(
                ctx,
                snapshot,
                TurnTransitionStartedTick,
                action_attempt_id=attempt.action_attempt_id,
            )

        if action_result is None or (
            action_result.effective_delivery_status is MutationDeliveryStatus.UNKNOWN
        ):
            uncertain = self._replace_attempt(
                delivery_started,
                status=AttemptStatus.UNCERTAIN,
                response_received_at=(None if action_result is None else self._now()),
                transport_result={
                    "delivery_status": "unknown",
                    "error_type": None if error is None else type(error).__name__,
                },
                tool_result=(
                    None
                    if action_result is None
                    else action_result.model_dump(mode="json")
                ),
            )
            self.store.update_action_attempt(uncertain)
            return self._finish(
                ctx,
                snapshot,
                MutationUncertainTick,
                action_attempt_id=attempt.action_attempt_id,
                task_id=task_id,
                selected_operation="end_turn",
                blocking_reason="end-turn delivery outcome is unknown",
            )

        failed = self._replace_attempt(
            delivery_started,
            status=AttemptStatus.FAILED,
            response_received_at=self._now(),
            transport_result={
                "delivery_status": action_result.effective_delivery_status.value
            },
            tool_result=action_result.model_dump(mode="json"),
            verification_status=VerificationStatus.FAILED,
        )
        self.store.update_action_attempt(failed)
        return self._finish(
            ctx,
            snapshot,
            MutationRejectedTick,
            action_attempt_id=attempt.action_attempt_id,
            task_id=task_id,
            selected_operation="end_turn",
            blocking_reason=action_result.message or "end turn was rejected",
        )

    def _reconcile_end_turn(
        self,
        ctx: _TickContext,
        observation: NormalizedRuntimeObservation,
        attempt: ActionAttempt,
    ) -> TickResult:
        snapshot = observation.snapshot
        if snapshot.turn > int(attempt.pre_send_turn or 0):
            succeeded = self._replace_attempt(
                attempt,
                status=AttemptStatus.SUCCEEDED,
                verification_status=VerificationStatus.PASSED,
                last_verification_observation_id=self._active_observation_id,
                verification_count=attempt.verification_count + 1,
            )
            self.store.update_action_attempt(succeeded)
            return self._finish(
                ctx,
                snapshot,
                TurnTransitionConfirmedTick,
                action_attempt_id=attempt.action_attempt_id,
                turn_ended=True,
            )

        count = attempt.verification_count + 1
        if count < max(1, self.config.verification_attempts):
            waiting = self._replace_attempt(
                attempt,
                status=AttemptStatus.VERIFYING,
                verification_status=VerificationStatus.INCONCLUSIVE,
                last_verification_observation_id=self._active_observation_id,
                verification_count=count,
            )
            self.store.update_action_attempt(waiting)
            return self._finish(
                ctx,
                snapshot,
                TurnTransitionWaitingTick,
                action_attempt_id=attempt.action_attempt_id,
            )

        uncertain = self._replace_attempt(
            attempt,
            status=AttemptStatus.UNCERTAIN,
            verification_status=VerificationStatus.INCONCLUSIVE,
            last_verification_observation_id=self._active_observation_id,
            verification_count=count,
        )
        self.store.update_action_attempt(uncertain)
        return self._finish(
            ctx,
            snapshot,
            AwaitingHumanTick,
            action_attempt_id=attempt.action_attempt_id,
            blocking_reason="turn number did not increase within verification policy",
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
        self.store.save_runtime_state(
            snapshot.game_id,
            tick.ending_runtime_state,
            active_attempt_id=getattr(tick, "action_attempt_id", None),
        )
        self.store.save_workflow_tick(tick)
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
        if isinstance(tick, AwaitingHumanTick):
            result.paused = True
            result.pause_reason = tick.blocking_reason
        return result

    @staticmethod
    def _replace_attempt(attempt: ActionAttempt, **updates: Any) -> ActionAttempt:
        payload = attempt.model_dump(mode="python")
        payload.update(updates)
        return ActionAttempt.model_validate(payload)

    @staticmethod
    def _idempotency_key(task: StoredTask, normalized_arguments: dict[str, Any]) -> str:
        semantic = {
            "task_id": task.task_id,
            "action_type": task.action_type,
            "entity_type": task.entity_type,
            "entity_id": task.entity_id,
            "arguments": normalized_arguments,
            "preconditions": task.preconditions,
            "postconditions": task.postconditions,
        }
        digest = hashlib.sha256(
            json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return f"task:{task.task_id}:{digest}"

    @staticmethod
    def _observation_id(observation: NormalizedRuntimeObservation) -> str:
        payload = observation.canonical.model_dump_json()
        digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
        return f"obs_{digest}_{uuid4().hex[:12]}"

    def _task_invalidation(
        self, task: StoredTask, observation: NormalizedRuntimeObservation
    ) -> str | None:
        preconditions = self.conditions.evaluate_all(task.preconditions, observation)
        if not preconditions.valid:
            return preconditions.reason
        return self._first_active_invalidator(task.invalidators, observation)

    def _checkpoint(self, name: str) -> None:
        if self.crash_injector is not None:
            self.crash_injector.checkpoint(name)

    def _now(self) -> datetime:
        if self.clock is not None:
            return self.clock.now()
        return datetime.now(UTC)

    def _monotonic(self) -> float:
        if self.clock is not None:
            return float(self.clock.monotonic())
        return time.perf_counter()

    async def _invoke_planner(
        self,
        snapshot: RuntimeSnapshot,
        agent_events: list[GameEvent],
        result: TickResult,
        metrics: TickMetrics,
    ) -> None:
        request = self._build_agent_request(snapshot, agent_events)
        result.planner_request_id = request.request_id
        started = self._monotonic()
        bundle: PlanBundle | None = None
        try:
            bundle = await self.planner.plan(request)
            validate_plan_bundle(
                bundle,
                PlanValidationContext(
                    current_turn=snapshot.turn,
                    allowed_action_types=self.config.allowed_action_types,
                    known_entities=extract_known_entities(snapshot),
                ),
            )
            self.store.save_plan_bundle(
                snapshot.game_id,
                snapshot.turn,
                bundle,
                mode=self.config.execution_mode,
                auto_action_types=self.config.auto_action_types,
                observation_id=self._active_observation_id,
            )
            self.store.mark_events_sent_to_agent(
                snapshot.game_id,
                [event.dedupe_key for event in agent_events],
                snapshot.turn,
            )
            metrics.agent_call_count = 1
            result.agent_invoked = True
            result.plan_id = bundle.plan_id
            if bundle.requires_human_review:
                result.paused = True
                result.pause_reason = "Planner requested human review"
            self.store.record_agent_run(
                snapshot.game_id,
                request,
                response=bundle,
                success=True,
                error=None,
                duration_seconds=self._monotonic() - started,
            )
        except Exception as exc:
            result.paused = True
            result.pause_reason = f"Agent planning failed: {exc}"
            self.store.record_agent_run(
                snapshot.game_id,
                request,
                response=bundle,
                success=False,
                error=str(exc),
                duration_seconds=self._monotonic() - started,
            )
        metrics.agent_seconds = self._monotonic() - started

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

    def _first_active_invalidator(
        self,
        invalidators: list[dict[str, Any]],
        observation: NormalizedRuntimeObservation,
    ) -> str | None:
        for invalidator in invalidators:
            evaluation = self.conditions.evaluate(invalidator, observation)
            if evaluation.valid:
                return str(invalidator)
            if evaluation.reason.startswith("unsupported condition type"):
                return evaluation.reason
        return None

    @staticmethod
    def _suppress_recoverable_blockers(
        events: list[GameEvent], retrying_tasks: list[StoredTask]
    ) -> list[GameEvent]:
        if not retrying_tasks:
            return events
        production_city_ids = {
            str(task.entity_id)
            for task in retrying_tasks
            if task.action_type == "city_set_production"
        }
        has_research = any(t.action_type == "set_research" for t in retrying_tasks)
        has_civic = any(t.action_type == "set_civic" for t in retrying_tasks)
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
        return retained

    def _build_agent_request(
        self, snapshot: RuntimeSnapshot, events: list[GameEvent]
    ) -> AgentRequest:
        context = self.store.current_context(snapshot.game_id)
        return AgentRequest(
            turn=snapshot.turn,
            execution_mode=self.config.execution_mode,
            trigger_events=events,
            current_strategy=context["strategy"],
            current_plans=context,
            relevant_state=snapshot.model_dump(mode="json"),
            constraints={
                "allowed_action_types": sorted(self.config.allowed_action_types),
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
                    "unit_has_build_charge",
                    "unit_build_charges_equals",
                    "unit_can_improve",
                ],
                "strategy_queue_fields": {
                    "research_queue": "ordered TECH_* type names",
                    "civic_queue": "ordered CIVIC_* type names",
                },
                "task_postconditions_required": True,
                "max_tasks": 100,
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
            },
        )

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
