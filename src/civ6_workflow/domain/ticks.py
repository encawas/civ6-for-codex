"""Closed workflow Tick states and outcomes."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import Field

from .base import DomainModel, JsonValue


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


class WorkflowTick(DomainModel):
    tick_id: str
    game_session_id: str
    starting_runtime_state: RuntimeState
    ending_runtime_state: RuntimeState
    outcome: TickOutcomeKind
    observation_ids: tuple[str, ...]
    selected_operation: str | None = None
    mutation_budget_used: int = Field(ge=0, le=1)
    planner_request_id: str | None = None
    action_attempt_id: str | None = None
    blocking_reason: str | None = None
    started_at: datetime
    completed_at: datetime
    metrics: dict[str, JsonValue] = {}

    def model_post_init(self, __context: object) -> None:
        mutation_outcomes = {
            TickOutcomeKind.MUTATION_SENT,
            TickOutcomeKind.TURN_TRANSITION_STARTED,
        }
        if (self.outcome in mutation_outcomes) != (self.mutation_budget_used == 1):
            raise ValueError("mutation outcomes must consume exactly one mutation budget")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
