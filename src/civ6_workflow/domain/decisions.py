"""Durable strategic decision gaps with stable semantic identity."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, model_validator

from .base import DomainModel, ImmutableJsonObject, SubjectRef


DECISION_INPUT_PROJECTION_VERSION = "decision-input/v1"


class DecisionGapStatus(StrEnum):
    OPEN = "OPEN"
    RULE_RESOLVED = "RULE_RESOLVED"
    PLAN_COVERED = "PLAN_COVERED"
    PLANNER_ELIGIBLE = "PLANNER_ELIGIBLE"
    REQUESTED = "REQUESTED"
    AWAITING_INFORMATION = "AWAITING_INFORMATION"
    AWAITING_HUMAN = "AWAITING_HUMAN"
    RESOLVED = "RESOLVED"
    INVALIDATED = "INVALIDATED"
    CONTEXT_REQUIRED = "CONTEXT_REQUIRED"
    PLANNER_REQUESTED = "PLANNER_REQUESTED"
    PROPOSED = "PROPOSED"
    DEFERRED_TO_HUMAN = "DEFERRED_TO_HUMAN"
    CANCELLED = "CANCELLED"
    SUPERSEDED = "SUPERSEDED"


class DecisionRoute(StrEnum):
    RULE = "RULE"
    EXISTING_PLAN = "EXISTING_PLAN"
    PLANNER = "PLANNER"
    HUMAN = "HUMAN"
    INFORMATION = "INFORMATION"


TERMINAL_DECISION_GAP_STATUSES = frozenset(
    {
        DecisionGapStatus.RULE_RESOLVED,
        DecisionGapStatus.PLAN_COVERED,
        DecisionGapStatus.RESOLVED,
        DecisionGapStatus.INVALIDATED,
        DecisionGapStatus.CANCELLED,
        DecisionGapStatus.SUPERSEDED,
    }
)


class DecisionGap(DomainModel):
    decision_gap_id: str
    game_session_id: str = "legacy"
    stable_identity: str = "legacy"
    source_event_ids: tuple[str, ...] = Field(min_length=1)
    gap_type: str
    scope: str
    subjects: tuple[SubjectRef, ...]
    observation_id: str
    first_observation_id: str | None = None
    relevant_input_hash: str = "legacy"
    input_projection_version: str = DECISION_INPUT_PROJECTION_VERSION
    input_projection: ImmutableJsonObject = {}
    strategy_revision: str = "legacy"
    relevant_plan_revisions: tuple[str, ...] = ()
    required_context: tuple[str, ...] = ()
    route: DecisionRoute
    status: DecisionGapStatus
    cooldown_key: str
    logical_request_id: str | None = None
    resolution_reason: str | None = None
    invalidation_reason: str | None = None
    reopen_reason: str | None = None
    turn_specific: bool = False
    identity_turn_number: int | None = Field(default=None, ge=0)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @model_validator(mode="after")
    def validate_identity_and_lifecycle(self) -> Self:
        if self.turn_specific != (self.identity_turn_number is not None):
            raise ValueError(
                "identity_turn_number is allowed exactly for turn-specific gaps"
            )
        if self.status in {
            DecisionGapStatus.REQUESTED,
            DecisionGapStatus.AWAITING_INFORMATION,
            DecisionGapStatus.PLANNER_REQUESTED,
        } and self.logical_request_id is None:
            raise ValueError("requested decision gaps require logical_request_id")
        if self.status in TERMINAL_DECISION_GAP_STATUSES and not (
            self.resolution_reason or self.invalidation_reason
        ):
            raise ValueError("terminal decision gaps require a disposition reason")
        if (
            self.created_at is not None
            and self.updated_at is not None
            and self.updated_at < self.created_at
        ):
            raise ValueError("updated_at must not precede created_at")
        return self


class DecisionGroup(DomainModel):
    decision_group_id: str
    game_session_id: str
    observation_id: str
    decision_gap_ids: tuple[str, ...] = Field(min_length=1)
    input_projection_hash: str
    input_projection_version: str = DECISION_INPUT_PROJECTION_VERSION
    created_at: datetime