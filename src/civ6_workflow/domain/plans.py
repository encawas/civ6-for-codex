"""Revisioned durable intent."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from .base import (
    ApprovalStatus,
    Condition,
    DomainModel,
    ImmutableJsonObject,
    SubjectRef,
)


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


class PlanLeaseStatus(StrEnum):
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"
    AWAITING_INFORMATION = "AWAITING_INFORMATION"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"


class LeaseValidationResult(StrEnum):
    VALID = "VALID"
    PARTIALLY_VALID = "PARTIALLY_VALID"
    EXPIRED = "EXPIRED"
    INVALIDATED = "INVALIDATED"
    UNKNOWN = "UNKNOWN"


class ContinuationPolicy(StrEnum):
    CONTINUE_WHILE_VALID = "CONTINUE_WHILE_VALID"
    EXTEND_WHEN_INPUT_UNCHANGED = "EXTEND_WHEN_INPUT_UNCHANGED"
    REQUIRE_REVIEW = "REQUIRE_REVIEW"


class Plan(DomainModel):
    plan_id: str
    game_session_id: str
    scope: str
    subjects: tuple[SubjectRef, ...] = ()
    covered_slots: tuple[str, ...] = ()
    revision: int = Field(ge=1)
    status: PlanStatus
    source: PlanSource
    approval_status: ApprovalStatus
    created_from_observation_id: str
    valid_from_turn: int = Field(ge=0)
    valid_until_turn: int | None = Field(default=None, ge=0)
    preconditions: tuple[Condition, ...] = ()
    completion_condition: Condition | None = None
    invalidation_conditions: tuple[Condition, ...] = ()
    objective: str = Field(min_length=1)
    steps: tuple[ImmutableJsonObject, ...] = ()
    policy_snapshot: ImmutableJsonObject = {}
    supersedes_plan_id: str | None = None

    def model_post_init(self, __context: object) -> None:
        if self.valid_until_turn is None and self.completion_condition is None:
            raise ValueError(
                "a plan requires a validity horizon or completion condition"
            )
        if (
            self.valid_until_turn is not None
            and self.valid_until_turn < self.valid_from_turn
        ):
            raise ValueError("valid_until_turn must not precede valid_from_turn")
        if self.status in {PlanStatus.ACTIVE, PlanStatus.COMPLETED}:
            if self.approval_status not in {
                ApprovalStatus.NOT_REQUIRED,
                ApprovalStatus.APPROVED,
            }:
                raise ValueError(f"a {self.status} plan must satisfy approval")
        if (self.status is PlanStatus.REJECTED) != (
            self.approval_status is ApprovalStatus.REJECTED
        ):
            raise ValueError("rejected plan and approval statuses must agree")
        if self.supersedes_plan_id == self.plan_id:
            raise ValueError("a plan cannot supersede itself")


class PlanLease(DomainModel):
    plan_lease_id: str
    plan_id: str
    game_session_id: str
    decision_gap_ids: tuple[str, ...] = Field(min_length=1)
    scope: str
    subjects: tuple[SubjectRef, ...] = ()
    covered_slots: tuple[str, ...] = ()
    plan_revision: int = Field(ge=1)
    source_planner_request_id: str | None = None
    task_ids: tuple[str, ...] = ()
    created_from_observation_id: str = "legacy"
    status: PlanLeaseStatus
    approval_status: ApprovalStatus
    valid_from_turn: int = Field(ge=0)
    valid_until_turn: int | None = Field(default=None, ge=0)
    preconditions: tuple[Condition, ...] = ()
    continuation_conditions: tuple[Condition, ...] = ()
    completion_condition: Condition | None = None
    invalidation_conditions: tuple[Condition, ...] = ()
    review_conditions: tuple[Condition, ...] = ()
    continuation_policy: ContinuationPolicy
    relevant_input_hash: str
    input_projection_version: str = "decision-input/v1"
    contract_baseline: ImmutableJsonObject = {}
    last_validated_observation_id: str
    last_validation_result: LeaseValidationResult
    invalidation_reason: str | None = None
    completion_reason: str | None = None

    def model_post_init(self, __context: object) -> None:
        if self.valid_until_turn is None and self.completion_condition is None:
            raise ValueError(
                "a plan lease requires a validity horizon or completion condition"
            )
        if (
            self.valid_until_turn is not None
            and self.valid_until_turn < self.valid_from_turn
        ):
            raise ValueError("valid_until_turn must not precede valid_from_turn")
        if self.status is PlanLeaseStatus.ACTIVE and self.approval_status not in {
            ApprovalStatus.NOT_REQUIRED,
            ApprovalStatus.APPROVED,
        }:
            raise ValueError("an active plan lease must satisfy approval")
        if self.status is PlanLeaseStatus.ACTIVE:
            if not self.preconditions:
                raise ValueError("an active plan lease requires preconditions")
            if not self.invalidation_conditions:
                raise ValueError(
                    "an active plan lease requires invalidation conditions"
                )
            if not self.review_conditions:
                raise ValueError("an active plan lease requires review conditions")
            if not self.continuation_conditions:
                raise ValueError(
                    "an active plan lease requires continuation conditions"
                )
        if (
            self.status is PlanLeaseStatus.AWAITING_APPROVAL
            and self.approval_status is not ApprovalStatus.REQUIRED
        ):
            raise ValueError("a lease awaiting approval must require approval")

        if self.status is PlanLeaseStatus.INVALIDATED and not self.invalidation_reason:
            raise ValueError("an invalidated plan lease requires a reason")
