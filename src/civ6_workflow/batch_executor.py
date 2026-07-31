"""Deterministic one-mutation execution for current workflow work."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from .actions import (
    END_TURN_ACTION_SPEC,
    ActionValidationError,
    resolve_action,
    resolve_action_spec,
)
from .conditions import ConditionEvaluator
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
    RuntimeState,
    TaskInvalidatedTick,
    TurnTransitionConfirmedTick,
    TurnTransitionStartedTick,
    TurnTransitionWaitingTick,
    VerificationStatus,
)
from .models import (
    ExecutionMode,
    MutationDeliveryStatus,
    StoredTask,
    TaskStatus,
    TickMetrics,
)
from .observation_normalization import NormalizedRuntimeObservation
from .ports import BoundedGamePort, GamePort, MutationBudget, WorkflowStorePort
from .verification import VerificationEvidence, evaluate_action_verification


class BarrierKind(StrEnum):
    DEPENDENCY = "dependency"
    VERIFICATION = "verification"
    APPROVAL = "approval"
    TURN = "turn"


@dataclass(frozen=True, slots=True)
class BarrierState:
    kind: BarrierKind
    node_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExecutionWave:
    graph_id: str | None
    eligible: tuple[StoredTask, ...] = ()
    barriers: tuple[BarrierState, ...] = ()

    @property
    def selected(self) -> StoredTask | None:
        return self.eligible[0] if self.eligible else None


@dataclass(slots=True)
class ExecutionTransition:
    tick_type: type[Any]
    fields: dict[str, Any] = field(default_factory=dict)
    attempt_update: ActionAttempt | None = None
    task_status: TaskStatus | None = None
    task_error: str | None = None
    executed_task_ids: tuple[str, ...] = ()
    failed_task_ids: tuple[str, ...] = ()
    blocked_task_ids: tuple[str, ...] = ()
    turn_ended: bool = False


class BatchExecutor:
    """Select one deterministic Wave member and advance its durable execution."""

    def __init__(
        self,
        *,
        store: WorkflowStorePort,
        game: GamePort,
        conditions: ConditionEvaluator,
        verification_attempts: int,
        now: Callable[[], datetime],
        monotonic: Callable[[], float],
        checkpoint: Callable[[str], None],
    ):
        self.store = store
        self.game = game
        self.conditions = conditions
        self.verification_attempts = max(1, verification_attempts)
        self._now = now
        self._monotonic = monotonic
        self._checkpoint = checkpoint

    def select_wave(
        self,
        observation: NormalizedRuntimeObservation,
        *,
        source_observation_id: str,
    ) -> ExecutionWave:
        snapshot = observation.snapshot
        graph_state = self.store.active_turn_action_graph(snapshot.game_id)
        if graph_state is None:
            return ExecutionWave(graph_id=None)
        graph, graph_tasks = graph_state
        active_tasks = tuple(
            sorted(
                (
                    task
                    for task in graph_tasks
                    if task.status
                    not in {
                        TaskStatus.DONE,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED,
                        TaskStatus.EXPIRED,
                        TaskStatus.ESCALATED,
                    }
                ),
                key=lambda task: (task.due_turn, task.task_id),
            )
        )
        if (
            graph.turn_number != snapshot.turn
            or graph.source_observation_projection_hash
            != observation.canonical.projection_hash
        ):
            return ExecutionWave(
                graph_id=graph.graph_id,
                barriers=(
                    BarrierState(
                        kind=BarrierKind.TURN,
                        node_ids=tuple(task.task_id for task in active_tasks),
                    ),
                )
                if active_tasks
                else (),
            )

        eligible = tuple(
            self.store.due_turn_action_nodes(
                snapshot.game_id,
                snapshot.turn,
                source_observation_id=source_observation_id,
            )
        )
        eligible_ids = {task.task_id for task in eligible}
        blocked: dict[BarrierKind, list[str]] = {kind: [] for kind in BarrierKind}
        for task in active_tasks:
            if task.status in {
                TaskStatus.RUNNING,
                TaskStatus.VERIFYING,
                TaskStatus.UNCERTAIN,
            }:
                blocked[BarrierKind.VERIFICATION].append(task.task_id)
            elif task.due_turn > snapshot.turn:
                blocked[BarrierKind.TURN].append(task.task_id)
            elif task.status is TaskStatus.AWAITING_CONFIRMATION:
                blocked[BarrierKind.APPROVAL].append(task.task_id)
            elif task.status is TaskStatus.READY and task.task_id not in eligible_ids:
                blocked[BarrierKind.DEPENDENCY].append(task.task_id)

        barriers = tuple(
            BarrierState(kind=kind, node_ids=tuple(sorted(blocked[kind])))
            for kind in BarrierKind
            if blocked[kind]
        )
        return ExecutionWave(
            graph_id=graph.graph_id,
            eligible=tuple(
                sorted(eligible, key=lambda task: (task.due_turn, task.task_id))
            ),
            barriers=barriers,
        )

    async def advance(
        self,
        observation: NormalizedRuntimeObservation,
        *,
        source_observation_id: str,
        mode: ExecutionMode,
        available_tools: set[str],
        metrics: TickMetrics,
        budget: MutationBudget,
    ) -> ExecutionTransition | None:
        snapshot = observation.snapshot
        wave = self.select_wave(
            observation,
            source_observation_id=source_observation_id,
        )
        graph_approval_ids = {
            node_id
            for barrier in wave.barriers
            if barrier.kind is BarrierKind.APPROVAL
            for node_id in barrier.node_ids
        }
        awaiting_ids = sorted(graph_approval_ids)
        if awaiting_ids:
            return ExecutionTransition(
                tick_type=AwaitingApprovalTick,
                fields={
                    "proposal_id": awaiting_ids[0],
                    "blocking_reason": "task approval is required",
                },
            )
        if mode is ExecutionMode.READONLY:
            return None

        candidates = list(wave.eligible)
        candidates.sort(key=lambda task: (task.due_turn, task.task_id))
        if not candidates:
            return None
        task = candidates[0]
        invalid = self._task_invalidation(task, observation)
        if invalid is not None:
            self.store.set_task_status(
                snapshot.game_id,
                task.task_id,
                TaskStatus.CANCELLED,
                error=invalid,
            )
            return ExecutionTransition(
                tick_type=TaskInvalidatedTick,
                fields={
                    "task_id": task.task_id,
                    "blocking_reason": invalid,
                },
            )
        return await self._send_task(
            observation,
            task,
            source_observation_id=source_observation_id,
            available_tools=available_tools,
            metrics=metrics,
            budget=budget,
        )

    async def _send_task(
        self,
        observation: NormalizedRuntimeObservation,
        task: StoredTask,
        *,
        source_observation_id: str,
        available_tools: set[str],
        metrics: TickMetrics,
        budget: MutationBudget,
    ) -> ExecutionTransition:
        snapshot = observation.snapshot
        try:
            spec = resolve_action_spec(task.action_type)
            _, normalized_arguments = resolve_action(task, available_tools)
        except ActionValidationError as exc:
            self.store.set_task_status(
                snapshot.game_id,
                task.task_id,
                TaskStatus.CANCELLED,
                error=str(exc),
            )
            return ExecutionTransition(
                tick_type=TaskInvalidatedTick,
                fields={
                    "task_id": task.task_id,
                    "blocking_reason": str(exc),
                },
            )

        parent = self.store.latest_attempt_for_task(snapshot.game_id, task.task_id)
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
            prepared_from_observation_id=source_observation_id,
            prepared_at=self._now(),
            status=AttemptStatus.PREPARED,
            retry_classification=spec.retry_classification,
            normalized_arguments=normalized_arguments,
            postconditions=tuple(task.postconditions),
            parent_attempt_id=None if parent is None else parent.action_attempt_id,
        )
        persistence_started = self._monotonic()
        self.store.save_action_attempt(attempt)
        self.store.set_task_status(
            snapshot.game_id,
            task.task_id,
            TaskStatus.RUNNING,
        )
        metrics.persistence_seconds += self._monotonic() - persistence_started
        self._checkpoint("after_attempt_prepared")

        delivery_started = self.replace_attempt(
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

        bounded = BoundedGamePort(self.game, budget)
        delivery_started_at = self._monotonic()
        try:
            action_result = await bounded.execute_task(task)
        except Exception as exc:
            action_result = None
            delivery_error = exc
        else:
            delivery_error = None
        metrics.mutation_delivery_seconds += self._monotonic() - delivery_started_at
        metrics.mutation_count = budget.used
        self._checkpoint("after_port_call")

        if action_result is None:
            uncertain = self.replace_attempt(
                delivery_started,
                status=AttemptStatus.UNCERTAIN,
                transport_result={
                    "phase": "delivery_unknown",
                    "error_type": type(delivery_error).__name__,
                },
            )
            return ExecutionTransition(
                tick_type=MutationUncertainTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "task_id": task.task_id,
                    "selected_operation": task.action_type,
                    "blocking_reason": "mutation delivery outcome is unknown",
                },
                attempt_update=uncertain,
                task_status=TaskStatus.UNCERTAIN,
                task_error="mutation delivery outcome is unknown",
            )

        status = action_result.effective_delivery_status
        response_at = self._now()
        if status is MutationDeliveryStatus.ACKNOWLEDGED:
            verifying = self.replace_attempt(
                delivery_started,
                status=AttemptStatus.VERIFYING,
                response_received_at=response_at,
                transport_result={"delivery_status": status.value},
                tool_result=action_result.model_dump(mode="json"),
                verification_status=VerificationStatus.PENDING,
            )
            return ExecutionTransition(
                tick_type=MutationSentTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "task_id": task.task_id,
                    "selected_operation": task.action_type,
                },
                attempt_update=verifying,
                task_status=TaskStatus.VERIFYING,
            )
        if status is MutationDeliveryStatus.UNKNOWN:
            reason = action_result.message or "mutation outcome is unknown"
            uncertain = self.replace_attempt(
                delivery_started,
                status=AttemptStatus.UNCERTAIN,
                response_received_at=response_at,
                transport_result={"delivery_status": status.value},
                tool_result=action_result.model_dump(mode="json"),
            )
            return ExecutionTransition(
                tick_type=MutationUncertainTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "task_id": task.task_id,
                    "selected_operation": task.action_type,
                    "blocking_reason": reason,
                },
                attempt_update=uncertain,
                task_status=TaskStatus.UNCERTAIN,
                task_error=reason,
            )

        reason = action_result.message or "game rejected mutation"
        failed = self.replace_attempt(
            delivery_started,
            status=AttemptStatus.FAILED,
            response_received_at=response_at,
            transport_result={"delivery_status": status.value},
            tool_result=action_result.model_dump(mode="json"),
            verification_status=VerificationStatus.FAILED,
        )
        return ExecutionTransition(
            tick_type=MutationRejectedTick,
            fields={
                "action_attempt_id": attempt.action_attempt_id,
                "task_id": task.task_id,
                "selected_operation": task.action_type,
                "blocking_reason": reason,
            },
            failed_task_ids=(task.task_id,),
            attempt_update=failed,
            task_error=reason,
        )

    async def send_end_turn(
        self,
        observation: NormalizedRuntimeObservation,
        *,
        source_observation_id: str,
        authorization_projection_version: str,
        authorization_projection_hash: str,
        reflections: Mapping[str, str],
        metrics: TickMetrics,
        budget: MutationBudget,
    ) -> ExecutionTransition:
        snapshot = observation.snapshot
        task_id = f"end_turn:{snapshot.turn}"
        parent = self.store.latest_attempt_for_task(snapshot.game_id, task_id)
        attempt = ActionAttempt(
            action_attempt_id=f"attempt_{uuid4().hex}",
            game_session_id=snapshot.game_id,
            task_id=task_id,
            action_type="end_turn",
            attempt_number=self.store.next_attempt_number(
                snapshot.game_id,
                task_id,
            ),
            request_id=f"request_{uuid4().hex}",
            idempotency_key=f"{snapshot.game_id}:end_turn:{snapshot.turn}",
            prepared_from_observation_id=source_observation_id,
            prepared_at=self._now(),
            status=AttemptStatus.PREPARED,
            retry_classification=END_TURN_ACTION_SPEC.retry_classification,
            normalized_arguments={
                "authorization_projection_version": authorization_projection_version,
                "authorization_projection_hash": authorization_projection_hash,
                **dict(reflections),
            },
            postconditions=(),
            parent_attempt_id=(None if parent is None else parent.action_attempt_id),
            pre_send_turn=snapshot.turn,
        )
        self.store.save_action_attempt(attempt)
        self._checkpoint("after_attempt_prepared")
        delivery_started = self.replace_attempt(
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

        bounded = BoundedGamePort(self.game, budget)
        started = self._monotonic()
        try:
            action_result = await bounded.end_turn(dict(reflections))
        except Exception as exc:
            action_result = None
            error = exc
        else:
            error = None
        metrics.mutation_delivery_seconds += self._monotonic() - started
        metrics.mutation_count = budget.used
        self._checkpoint("after_port_call")

        if action_result is not None and (
            action_result.effective_delivery_status
            is MutationDeliveryStatus.ACKNOWLEDGED
        ):
            verifying = self.replace_attempt(
                delivery_started,
                status=AttemptStatus.VERIFYING,
                response_received_at=self._now(),
                transport_result={"delivery_status": "acknowledged"},
                tool_result=action_result.model_dump(mode="json"),
                verification_status=VerificationStatus.PENDING,
            )
            return ExecutionTransition(
                tick_type=TurnTransitionStartedTick,
                fields={"action_attempt_id": attempt.action_attempt_id},
                attempt_update=verifying,
            )

        if action_result is None or (
            action_result.effective_delivery_status is MutationDeliveryStatus.UNKNOWN
        ):
            uncertain = self.replace_attempt(
                delivery_started,
                status=AttemptStatus.UNCERTAIN,
                response_received_at=(None if action_result is None else self._now()),
                transport_result={
                    "delivery_status": "unknown",
                    "error_type": (None if error is None else type(error).__name__),
                },
                tool_result=(
                    None
                    if action_result is None
                    else action_result.model_dump(mode="json")
                ),
            )
            return ExecutionTransition(
                tick_type=MutationUncertainTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "task_id": task_id,
                    "selected_operation": "end_turn",
                    "blocking_reason": "end-turn delivery outcome is unknown",
                },
                attempt_update=uncertain,
            )

        failed = self.replace_attempt(
            delivery_started,
            status=AttemptStatus.FAILED,
            response_received_at=self._now(),
            transport_result={
                "delivery_status": action_result.effective_delivery_status.value
            },
            tool_result=action_result.model_dump(mode="json"),
            verification_status=VerificationStatus.FAILED,
        )
        return ExecutionTransition(
            tick_type=MutationRejectedTick,
            fields={
                "action_attempt_id": attempt.action_attempt_id,
                "task_id": task_id,
                "selected_operation": "end_turn",
                "blocking_reason": (action_result.message or "end turn was rejected"),
            },
            attempt_update=failed,
        )

    def reconcile(
        self,
        observation: NormalizedRuntimeObservation,
        attempt: ActionAttempt,
        *,
        source_observation_id: str,
        metrics: TickMetrics,
    ) -> ExecutionTransition:
        snapshot = observation.snapshot
        if attempt.status is AttemptStatus.PREPARED:
            rejected = self.replace_attempt(
                attempt,
                status=AttemptStatus.REJECTED_BEFORE_SEND,
                transport_result={"recovery": "prepared commit proves no send began"},
            )
            return ExecutionTransition(
                tick_type=AttemptRecoveredTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "task_id": attempt.task_id,
                },
                attempt_update=rejected,
            )
        if attempt.action_type == "end_turn":
            return self._reconcile_end_turn(
                observation,
                attempt,
                source_observation_id=source_observation_id,
            )

        task = self.store.get_task(snapshot.game_id, attempt.task_id)
        if task is None:
            return ExecutionTransition(
                tick_type=AwaitingHumanTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "blocking_reason": "attempt task is missing",
                },
                attempt_update=attempt,
            )
        verification_started = self._monotonic()
        decision = evaluate_action_verification(
            attempt,
            task,
            observation,
            self.conditions,
        )
        metrics.verification_seconds += self._monotonic() - verification_started
        if decision.evidence is VerificationEvidence.POSITIVE_COMMIT_EVIDENCE:
            succeeded = self.replace_attempt(
                attempt,
                status=AttemptStatus.SUCCEEDED,
                verification_status=VerificationStatus.PASSED,
                last_verification_observation_id=source_observation_id,
                verification_count=attempt.verification_count + 1,
                transport_result={
                    **dict(attempt.transport_result or {}),
                    "verification_evidence": decision.evidence.value,
                },
            )
            return ExecutionTransition(
                tick_type=AttemptReconciledTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "task_id": task.task_id,
                    "attempt_status": AttemptStatus.SUCCEEDED,
                },
                executed_task_ids=(task.task_id,),
                attempt_update=succeeded,
            )
        if decision.evidence in {
            VerificationEvidence.EXPLICIT_NON_COMMIT_EVIDENCE,
            VerificationEvidence.CONFLICTING_STATE,
            VerificationEvidence.IMPOSSIBLE_POSTCONDITION,
        }:
            failed = self.replace_attempt(
                attempt,
                status=AttemptStatus.FAILED,
                transport_result={
                    **dict(attempt.transport_result or {}),
                    "verification_evidence": decision.evidence.value,
                },
                verification_status=VerificationStatus.FAILED,
                last_verification_observation_id=source_observation_id,
                verification_count=attempt.verification_count + 1,
            )
            return ExecutionTransition(
                tick_type=AttemptReconciledTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "task_id": task.task_id,
                    "attempt_status": AttemptStatus.FAILED,
                },
                failed_task_ids=(task.task_id,),
                attempt_update=failed,
                task_error=decision.reason,
            )

        count = attempt.verification_count + 1
        if count < self.verification_attempts:
            verifying = self.replace_attempt(
                attempt,
                status=AttemptStatus.VERIFYING,
                verification_status=VerificationStatus.INCONCLUSIVE,
                last_verification_observation_id=source_observation_id,
                verification_count=count,
                transport_result={
                    **dict(attempt.transport_result or {}),
                    "verification_evidence": decision.evidence.value,
                },
            )
            return ExecutionTransition(
                tick_type=AwaitingVerificationTick,
                fields={
                    "action_attempt_id": attempt.action_attempt_id,
                    "task_id": task.task_id,
                },
                attempt_update=verifying,
                task_status=TaskStatus.VERIFYING,
                task_error=decision.reason,
            )

        uncertain = self.replace_attempt(
            attempt,
            status=AttemptStatus.UNCERTAIN,
            verification_status=VerificationStatus.INCONCLUSIVE,
            last_verification_observation_id=source_observation_id,
            verification_count=count,
            transport_result={
                **dict(attempt.transport_result or {}),
                "verification_evidence": decision.evidence.value,
            },
        )
        return ExecutionTransition(
            tick_type=AwaitingHumanTick,
            fields={
                "action_attempt_id": attempt.action_attempt_id,
                "blocking_reason": (
                    decision.reason or "verification remained inconclusive"
                ),
            },
            attempt_update=uncertain,
            task_status=TaskStatus.UNCERTAIN,
            task_error=decision.reason,
        )

    def _reconcile_end_turn(
        self,
        observation: NormalizedRuntimeObservation,
        attempt: ActionAttempt,
        *,
        source_observation_id: str,
    ) -> ExecutionTransition:
        snapshot = observation.snapshot
        if snapshot.turn > int(attempt.pre_send_turn or 0):
            succeeded = self.replace_attempt(
                attempt,
                status=AttemptStatus.SUCCEEDED,
                verification_status=VerificationStatus.PASSED,
                last_verification_observation_id=source_observation_id,
                verification_count=attempt.verification_count + 1,
            )
            return ExecutionTransition(
                tick_type=TurnTransitionConfirmedTick,
                fields={"action_attempt_id": attempt.action_attempt_id},
                attempt_update=succeeded,
                turn_ended=True,
            )

        count = attempt.verification_count + 1
        if count < self.verification_attempts:
            waiting = self.replace_attempt(
                attempt,
                status=AttemptStatus.VERIFYING,
                verification_status=VerificationStatus.INCONCLUSIVE,
                last_verification_observation_id=source_observation_id,
                verification_count=count,
            )
            return ExecutionTransition(
                tick_type=TurnTransitionWaitingTick,
                fields={"action_attempt_id": attempt.action_attempt_id},
                attempt_update=waiting,
            )

        uncertain = self.replace_attempt(
            attempt,
            status=AttemptStatus.UNCERTAIN,
            verification_status=VerificationStatus.INCONCLUSIVE,
            last_verification_observation_id=source_observation_id,
            verification_count=count,
        )
        return ExecutionTransition(
            tick_type=AwaitingHumanTick,
            fields={
                "action_attempt_id": attempt.action_attempt_id,
                "blocking_reason": (
                    "turn number did not increase within verification policy"
                ),
            },
            attempt_update=uncertain,
        )

    @staticmethod
    def replace_attempt(attempt: ActionAttempt, **updates: Any) -> ActionAttempt:
        payload = attempt.model_dump(mode="python")
        payload.update(updates)
        return ActionAttempt.model_validate(payload)

    @staticmethod
    def _idempotency_key(
        task: StoredTask,
        normalized_arguments: Mapping[str, Any],
    ) -> str:
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

    def _task_invalidation(
        self,
        task: StoredTask,
        observation: NormalizedRuntimeObservation,
    ) -> str | None:
        preconditions = self.conditions.evaluate_all(task.preconditions, observation)
        if not preconditions.valid:
            return preconditions.reason
        return self._first_active_invalidator(task.invalidators, observation)

    def _first_active_invalidator(
        self,
        invalidators: Sequence[Mapping[str, Any]],
        observation: NormalizedRuntimeObservation,
    ) -> str | None:
        for invalidator in invalidators:
            result = self.conditions.evaluate(dict(invalidator), observation)
            if result.valid:
                return str(dict(invalidator))
            if result.reason.startswith("unsupported condition type"):
                return result.reason
        return None
