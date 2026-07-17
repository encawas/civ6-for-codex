"""Closed discriminated workflow Tick outcomes."""

from __future__ import annotations


from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import Field, TypeAdapter
from pydantic_core import to_json

from .attempts import AttemptStatus
from .base import DomainModel, ImmutableJsonObject


class RuntimeState(StrEnum):
    OBSERVING = "OBSERVING"
    RECONCILING = "RECONCILING"
    ROUTING = "ROUTING"
    GATHERING_CONTEXT = "GATHERING_CONTEXT"
    REQUESTING_PLAN = "REQUESTING_PLAN"
    READY_TO_ACT = "READY_TO_ACT"
    ACTION_SENT = "ACTION_SENT"
    VERIFYING = "VERIFYING"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    AWAITING_HUMAN = "AWAITING_HUMAN"
    PLANNER_BACKOFF = "PLANNER_BACKOFF"
    READY_TO_END_TURN = "READY_TO_END_TURN"
    TURN_TRANSITIONING = "TURN_TRANSITIONING"
    PAUSED = "PAUSED"
    SYSTEM_ERROR = "SYSTEM_ERROR"


class TickOutcomeKind(StrEnum):
    OBSERVED_ONLY = "OBSERVED_ONLY"
    DECISION_GAP_CREATED = "DECISION_GAP_CREATED"
    DECISION_GAP_UPDATED = "DECISION_GAP_UPDATED"
    PLAN_LEASE_UPDATED = "PLAN_LEASE_UPDATED"
    LOGICAL_PLANNER_REQUEST_CREATED = "LOGICAL_PLANNER_REQUEST_CREATED"
    PLANNER_ATTEMPT_COMPLETED = "PLANNER_ATTEMPT_COMPLETED"
    INFORMATION_REQUESTED = "INFORMATION_REQUESTED"
    INFORMATION_COLLECTED = "INFORMATION_COLLECTED"
    CONTEXT_GATHERED = "CONTEXT_GATHERED"
    PLAN_REQUESTED = "PLAN_REQUESTED"
    TASK_CREATED = "TASK_CREATED"
    TASK_INVALIDATED = "TASK_INVALIDATED"
    ATTEMPT_RECOVERED = "ATTEMPT_RECOVERED"
    ATTEMPT_RECONCILED = "ATTEMPT_RECONCILED"
    MUTATION_SENT = "MUTATION_SENT"
    MUTATION_REJECTED = "MUTATION_REJECTED"
    MUTATION_UNCERTAIN = "MUTATION_UNCERTAIN"
    AWAITING_VERIFICATION = "AWAITING_VERIFICATION"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    AWAITING_HUMAN = "AWAITING_HUMAN"
    PLANNER_BACKOFF = "PLANNER_BACKOFF"
    TURN_TRANSITION_STARTED = "TURN_TRANSITION_STARTED"
    TURN_TRANSITION_WAITING = "TURN_TRANSITION_WAITING"
    TURN_TRANSITION_CONFIRMED = "TURN_TRANSITION_CONFIRMED"
    PAUSED = "PAUSED"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    NO_SAFE_ACTION = "NO_SAFE_ACTION"


class TickRecord(DomainModel):
    tick_id: str
    game_session_id: str
    turn_number: int = Field(default=0, ge=0)
    starting_runtime_state: RuntimeState
    observation_ids: tuple[str, ...] = Field(min_length=1)
    started_at: datetime
    completed_at: datetime
    metrics: ImmutableJsonObject = {}

    def model_post_init(self, __context: object) -> None:
        try:
            if self.completed_at < self.started_at:
                raise ValueError("completed_at must not precede started_at")
        except TypeError as exc:
            raise ValueError("Tick timestamps must use compatible timezones") from exc


class ObservedOnlyTick(TickRecord):
    outcome: Literal[TickOutcomeKind.OBSERVED_ONLY] = TickOutcomeKind.OBSERVED_ONLY
    ending_runtime_state: Literal[RuntimeState.OBSERVING] = RuntimeState.OBSERVING
    mutation_budget_used: Literal[0] = 0


class DecisionGapCreatedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.DECISION_GAP_CREATED] = (
        TickOutcomeKind.DECISION_GAP_CREATED
    )
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    decision_gap_id: str


class DecisionGapUpdatedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.DECISION_GAP_UPDATED] = (
        TickOutcomeKind.DECISION_GAP_UPDATED
    )
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    decision_gap_id: str
    update_reason: str = Field(min_length=1)


class PlanLeaseUpdatedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.PLAN_LEASE_UPDATED] = (
        TickOutcomeKind.PLAN_LEASE_UPDATED
    )
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    plan_lease_id: str
    validation_result: str = Field(min_length=1)


class LogicalPlannerRequestCreatedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED] = (
        TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
    )
    ending_runtime_state: Literal[RuntimeState.REQUESTING_PLAN] = (
        RuntimeState.REQUESTING_PLAN
    )
    mutation_budget_used: Literal[0] = 0
    planner_request_id: str
    decision_gap_ids: tuple[str, ...] = Field(min_length=1)


class PlannerAttemptCompletedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED] = (
        TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED
    )
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    planner_request_id: str
    provider_attempt_id: str
    provider_attempt_count: int = Field(ge=0)


class InformationRequestedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.INFORMATION_REQUESTED] = (
        TickOutcomeKind.INFORMATION_REQUESTED
    )
    ending_runtime_state: Literal[RuntimeState.GATHERING_CONTEXT] = (
        RuntimeState.GATHERING_CONTEXT
    )
    mutation_budget_used: Literal[0] = 0
    planner_request_id: str
    information_round_id: str


class InformationCollectedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.INFORMATION_COLLECTED] = (
        TickOutcomeKind.INFORMATION_COLLECTED
    )
    ending_runtime_state: Literal[RuntimeState.REQUESTING_PLAN] = (
        RuntimeState.REQUESTING_PLAN
    )
    mutation_budget_used: Literal[0] = 0
    planner_request_id: str
    information_round_id: str

class ContextGatheredTick(TickRecord):
    outcome: Literal[TickOutcomeKind.CONTEXT_GATHERED] = (
        TickOutcomeKind.CONTEXT_GATHERED
    )
    ending_runtime_state: Literal[RuntimeState.GATHERING_CONTEXT] = (
        RuntimeState.GATHERING_CONTEXT
    )
    mutation_budget_used: Literal[0] = 0


class PlanRequestedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.PLAN_REQUESTED] = TickOutcomeKind.PLAN_REQUESTED
    ending_runtime_state: Literal[RuntimeState.REQUESTING_PLAN] = (
        RuntimeState.REQUESTING_PLAN
    )
    mutation_budget_used: Literal[0] = 0
    planner_request_id: str


class TaskCreatedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.TASK_CREATED] = TickOutcomeKind.TASK_CREATED
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    task_id: str


class TaskInvalidatedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.TASK_INVALIDATED] = (
        TickOutcomeKind.TASK_INVALIDATED
    )
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    task_id: str
    blocking_reason: str = Field(min_length=1)


class AttemptRecoveredTick(TickRecord):
    outcome: Literal[TickOutcomeKind.ATTEMPT_RECOVERED] = (
        TickOutcomeKind.ATTEMPT_RECOVERED
    )
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    action_attempt_id: str
    task_id: str


class AttemptReconciledTick(TickRecord):
    outcome: Literal[TickOutcomeKind.ATTEMPT_RECONCILED] = (
        TickOutcomeKind.ATTEMPT_RECONCILED
    )
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    action_attempt_id: str
    task_id: str
    attempt_status: Literal[AttemptStatus.SUCCEEDED, AttemptStatus.FAILED]


class MutationSentTick(TickRecord):
    outcome: Literal[TickOutcomeKind.MUTATION_SENT] = TickOutcomeKind.MUTATION_SENT
    ending_runtime_state: Literal[RuntimeState.ACTION_SENT] = RuntimeState.ACTION_SENT
    mutation_budget_used: Literal[1] = 1
    action_attempt_id: str
    task_id: str
    selected_operation: str = Field(min_length=1)


class MutationRejectedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.MUTATION_REJECTED] = (
        TickOutcomeKind.MUTATION_REJECTED
    )
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[1] = 1
    action_attempt_id: str
    task_id: str
    selected_operation: str = Field(min_length=1)
    blocking_reason: str = Field(min_length=1)


class MutationUncertainTick(TickRecord):
    outcome: Literal[TickOutcomeKind.MUTATION_UNCERTAIN] = (
        TickOutcomeKind.MUTATION_UNCERTAIN
    )
    ending_runtime_state: Literal[RuntimeState.AWAITING_HUMAN] = (
        RuntimeState.AWAITING_HUMAN
    )
    mutation_budget_used: Literal[1] = 1
    action_attempt_id: str
    task_id: str
    selected_operation: str = Field(min_length=1)
    blocking_reason: str = Field(min_length=1)


class AwaitingVerificationTick(TickRecord):
    outcome: Literal[TickOutcomeKind.AWAITING_VERIFICATION] = (
        TickOutcomeKind.AWAITING_VERIFICATION
    )
    ending_runtime_state: Literal[RuntimeState.VERIFYING] = RuntimeState.VERIFYING
    mutation_budget_used: Literal[0] = 0
    action_attempt_id: str
    task_id: str


class AwaitingApprovalTick(TickRecord):
    outcome: Literal[TickOutcomeKind.AWAITING_APPROVAL] = (
        TickOutcomeKind.AWAITING_APPROVAL
    )
    ending_runtime_state: Literal[RuntimeState.AWAITING_APPROVAL] = (
        RuntimeState.AWAITING_APPROVAL
    )
    mutation_budget_used: Literal[0] = 0
    proposal_id: str
    blocking_reason: str = Field(min_length=1)


class AwaitingHumanTick(TickRecord):
    outcome: Literal[TickOutcomeKind.AWAITING_HUMAN] = TickOutcomeKind.AWAITING_HUMAN
    ending_runtime_state: Literal[RuntimeState.AWAITING_HUMAN] = (
        RuntimeState.AWAITING_HUMAN
    )
    mutation_budget_used: Literal[0] = 0
    blocking_reason: str = Field(min_length=1)
    action_attempt_id: str | None = None


class PlannerBackoffTick(TickRecord):
    outcome: Literal[TickOutcomeKind.PLANNER_BACKOFF] = TickOutcomeKind.PLANNER_BACKOFF
    ending_runtime_state: Literal[RuntimeState.PLANNER_BACKOFF] = (
        RuntimeState.PLANNER_BACKOFF
    )
    mutation_budget_used: Literal[0] = 0
    planner_request_id: str
    blocking_reason: str = Field(min_length=1)


class TurnTransitionStartedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.TURN_TRANSITION_STARTED] = (
        TickOutcomeKind.TURN_TRANSITION_STARTED
    )
    ending_runtime_state: Literal[RuntimeState.TURN_TRANSITIONING] = (
        RuntimeState.TURN_TRANSITIONING
    )
    mutation_budget_used: Literal[1] = 1
    action_attempt_id: str
    selected_operation: Literal["end_turn"] = "end_turn"


class TurnTransitionWaitingTick(TickRecord):
    outcome: Literal[TickOutcomeKind.TURN_TRANSITION_WAITING] = (
        TickOutcomeKind.TURN_TRANSITION_WAITING
    )
    ending_runtime_state: Literal[RuntimeState.TURN_TRANSITIONING] = (
        RuntimeState.TURN_TRANSITIONING
    )
    mutation_budget_used: Literal[0] = 0
    action_attempt_id: str


class TurnTransitionConfirmedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.TURN_TRANSITION_CONFIRMED] = (
        TickOutcomeKind.TURN_TRANSITION_CONFIRMED
    )
    ending_runtime_state: Literal[RuntimeState.OBSERVING] = RuntimeState.OBSERVING
    mutation_budget_used: Literal[0] = 0
    action_attempt_id: str


class PausedTick(TickRecord):
    outcome: Literal[TickOutcomeKind.PAUSED] = TickOutcomeKind.PAUSED
    ending_runtime_state: Literal[RuntimeState.PAUSED] = RuntimeState.PAUSED
    mutation_budget_used: Literal[0] = 0
    blocking_reason: str = Field(min_length=1)


class SystemErrorTick(TickRecord):
    outcome: Literal[TickOutcomeKind.SYSTEM_ERROR] = TickOutcomeKind.SYSTEM_ERROR
    ending_runtime_state: Literal[RuntimeState.SYSTEM_ERROR] = RuntimeState.SYSTEM_ERROR
    mutation_budget_used: Literal[0] = 0
    blocking_reason: str = Field(min_length=1)
    error_category: str = Field(min_length=1)
    diagnostic_summary: str = Field(min_length=1, max_length=500)
    action_attempt_id: str | None = None


class NoSafeActionTick(TickRecord):
    outcome: Literal[TickOutcomeKind.NO_SAFE_ACTION] = TickOutcomeKind.NO_SAFE_ACTION
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    blocking_reason: str = Field(min_length=1)


WorkflowTick: TypeAlias = Annotated[
    ObservedOnlyTick
    | DecisionGapCreatedTick
    | DecisionGapUpdatedTick
    | PlanLeaseUpdatedTick
    | LogicalPlannerRequestCreatedTick
    | PlannerAttemptCompletedTick
    | InformationRequestedTick
    | InformationCollectedTick
    | ContextGatheredTick
    | PlanRequestedTick
    | TaskCreatedTick
    | TaskInvalidatedTick
    | AttemptRecoveredTick
    | AttemptReconciledTick
    | MutationSentTick
    | MutationRejectedTick
    | MutationUncertainTick
    | AwaitingVerificationTick
    | AwaitingApprovalTick
    | AwaitingHumanTick
    | PlannerBackoffTick
    | TurnTransitionStartedTick
    | TurnTransitionWaitingTick
    | TurnTransitionConfirmedTick
    | PausedTick
    | SystemErrorTick
    | NoSafeActionTick,
    Field(discriminator="outcome"),
]

WORKFLOW_TICK_ADAPTER = TypeAdapter(WorkflowTick)


def _unwrap_compatibility_result(value: Any) -> Any:
    workflow_tick = getattr(value, "workflow_tick", None)
    return workflow_tick if workflow_tick is not None else value


def validate_workflow_tick(value: Any) -> WorkflowTick:
    unwrapped = _unwrap_compatibility_result(value)
    if isinstance(unwrapped, dict):
        return WORKFLOW_TICK_ADAPTER.validate_json(to_json(unwrapped))
    return WORKFLOW_TICK_ADAPTER.validate_python(unwrapped)


def validate_workflow_tick_json(value: str | bytes) -> WorkflowTick:
    return WORKFLOW_TICK_ADAPTER.validate_json(value)
