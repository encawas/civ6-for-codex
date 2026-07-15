"""Revisioned durable intent."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from .base import ApprovalStatus, Condition, DomainModel, JsonValue, SubjectRef


class PlanStatus(StrEnum):
    PROPOSED = "PROPOSED"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    SUPERSEDED = "SUPERSEDED"


class PlanSource(StrEnum):
    PLANNER = "PLANNER"
    RULE = "RULE"
    HUMAN = "HUMAN"
    MIGRATION = "MIGRATION"


class Plan(DomainModel):
    plan_id: str
    game_session_id: str
    scope: str
    subjects: tuple[SubjectRef, ...] = ()
    revision: int = Field(ge=1)
    status: PlanStatus
    source: PlanSource
    approval_status: ApprovalStatus
    created_from_observation_id: str
    valid_from_turn: int = Field(ge=0)
    valid_until_turn: int | None = Field(default=None, ge=0)
    completion_condition: Condition | None = None
    invalidation_conditions: tuple[Condition, ...] = ()
    objective: str
    steps: tuple[dict[str, JsonValue], ...] = ()
    policy_snapshot: dict[str, JsonValue] = {}
    supersedes_plan_id: str | None = None

    def model_post_init(self, __context: object) -> None:
        if self.valid_until_turn is None and self.completion_condition is None:
            raise ValueError("a plan requires a validity horizon or completion condition")
        if (
            self.valid_until_turn is not None
            and self.valid_until_turn < self.valid_from_turn
        ):
            raise ValueError("valid_until_turn must not precede valid_from_turn")
