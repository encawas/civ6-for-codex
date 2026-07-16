"""Closed discriminated workflow Tick outcomes."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import Field, TypeAdapter

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
    CONTEXT_GATHERED = "CONTEXT_GATHERED"
    PLAN_REQUESTED = "PLAN_REQUESTED"
    TASK_CREATED = "TASK_CREATED"
    MUTATION_SENT = "MUTATION_SENT"
    AWAITING_VERIFICATION = "AWAITING_VERIFICATION"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    AWAITING_HUMAN = "AWAITING_HUMAN"
    PLANNER_BACKOFF = "PLANNER_BACKOFF"
    TURN_TRANSITION_STARTED = "TURN_TRANSITION_STARTED"
    PAUSED = "PAUSED"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    NO_SAFE_ACTION = "NO_SAFE_ACTION"


class TickRecord(DomainModel):
    tick_id: str
    game_session_id: str
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


class MutationSentTick(TickRecord):
    outcome: Literal[TickOutcomeKind.MUTATION_SENT] = TickOutcomeKind.MUTATION_SENT
    ending_runtime_state: Literal[RuntimeState.ACTION_SENT] = RuntimeState.ACTION_SENT
    mutation_budget_used: Literal[1] = 1
    action_attempt_id: str
    selected_operation: str = Field(min_length=1)


class AwaitingVerificationTick(TickRecord):
    outcome: Literal[TickOutcomeKind.AWAITING_VERIFICATION] = (
        TickOutcomeKind.AWAITING_VERIFICATION
    )
    ending_runtime_state: Literal[RuntimeState.VERIFYING] = RuntimeState.VERIFYING
    mutation_budget_used: Literal[0] = 0
    action_attempt_id: str


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


class NoSafeActionTick(TickRecord):
    outcome: Literal[TickOutcomeKind.NO_SAFE_ACTION] = TickOutcomeKind.NO_SAFE_ACTION
    ending_runtime_state: Literal[RuntimeState.ROUTING] = RuntimeState.ROUTING
    mutation_budget_used: Literal[0] = 0
    blocking_reason: str = Field(min_length=1)


WorkflowTick: TypeAlias = Annotated[
    ObservedOnlyTick
    | ContextGatheredTick
    | PlanRequestedTick
    | TaskCreatedTick
    | MutationSentTick
    | AwaitingVerificationTick
    | AwaitingApprovalTick
    | AwaitingHumanTick
    | PlannerBackoffTick
    | TurnTransitionStartedTick
    | PausedTick
    | SystemErrorTick
    | NoSafeActionTick,
    Field(discriminator="outcome"),
]

WORKFLOW_TICK_ADAPTER = TypeAdapter(WorkflowTick)


def validate_workflow_tick(value: Any) -> WorkflowTick:
    return WORKFLOW_TICK_ADAPTER.validate_python(value)


def validate_workflow_tick_json(value: str | bytes) -> WorkflowTick:
    return WORKFLOW_TICK_ADAPTER.validate_json(value)
