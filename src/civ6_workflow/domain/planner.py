"""Logical planner requests, provider attempts, and information rounds."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, model_validator

from .base import DomainModel, ImmutableJsonObject


class PlannerRequestStatus(StrEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    AWAITING_INFORMATION = "AWAITING_INFORMATION"
    READY_TO_CONTINUE = "READY_TO_CONTINUE"
    COMPLETED = "COMPLETED"
    PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"
    FAILED = "FAILED"
    BACKOFF = "BACKOFF"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    SUPERSEDED = "SUPERSEDED"


TERMINAL_PLANNER_STATUSES = frozenset(
    {
        PlannerRequestStatus.COMPLETED,
        PlannerRequestStatus.FAILED,
        PlannerRequestStatus.PARTIALLY_COMPLETED,
        PlannerRequestStatus.REJECTED,
        PlannerRequestStatus.CANCELLED,
        PlannerRequestStatus.SUPERSEDED,
    }
)


class ProviderAttemptStatus(StrEnum):
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABANDONED = "ABANDONED"


class InformationRoundStatus(StrEnum):
    REQUESTED = "REQUESTED"
    COLLECTED = "COLLECTED"
    FAILED = "FAILED"


class PlannerRequest(DomainModel):
    planner_request_id: str
    game_session_id: str
    turn_number: int = Field(ge=0)
    observation_id: str
    decision_gap_ids: tuple[str, ...] = Field(min_length=1)
    decision_group_id: str | None = None
    input_projection_hash: str
    input_projection_version: str = "decision-input/v1"
    input_projection: ImmutableJsonObject = {}
    request_payload: ImmutableJsonObject = {}
    plan_revision_refs: tuple[str, ...] = ()
    policy_revision: str
    approval_contract_hash: str = "legacy"
    allowed_actions_hash: str = "legacy"
    model_settings: ImmutableJsonObject
    status: PlannerRequestStatus
    created_at: datetime
    completed_at: datetime | None = None
    response_hash: str | None = None
    validation_result: ImmutableJsonObject | None = None
    pending_information_requests: tuple[ImmutableJsonObject, ...] = ()
    information_results: ImmutableJsonObject = {}
    information_round_count: int = Field(default=0, ge=0)
    provider_attempt_count: int = Field(default=0, ge=0)
    context_bytes: int = Field(default=0, ge=0)
    failure_category: str | None = None
    next_retry_at: datetime | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.status in TERMINAL_PLANNER_STATUSES:
            if self.completed_at is None:
                raise ValueError("terminal planner requests require completed_at")
        elif any(
            value is not None
            for value in (self.completed_at, self.response_hash, self.validation_result)
        ):
            raise ValueError(
                "non-terminal planner requests cannot contain result evidence"
            )

        if self.status in {
            PlannerRequestStatus.COMPLETED,
            PlannerRequestStatus.PARTIALLY_COMPLETED,
        }:
            if self.response_hash is None or self.validation_result is None:
                raise ValueError(
                    "completed planner requests require validated response evidence"
                )
        if (
            self.status is PlannerRequestStatus.AWAITING_INFORMATION
            and not self.pending_information_requests
        ):
            raise ValueError(
                "awaiting-information requests require pending information queries"
            )
        if (
            self.status is not PlannerRequestStatus.AWAITING_INFORMATION
            and self.pending_information_requests
        ):
            raise ValueError(
                "pending information queries require awaiting-information status"
            )

        try:
            if self.completed_at is not None and self.completed_at < self.created_at:
                raise ValueError("completed_at must not precede created_at")
        except TypeError as exc:
            raise ValueError(
                "planner timestamps must use compatible timezones"
            ) from exc
        return self


class ProviderAttempt(DomainModel):
    provider_attempt_id: str
    planner_request_id: str
    attempt_number: int = Field(ge=1)
    provider_request_id: str
    status: ProviderAttemptStatus
    started_at: datetime
    completed_at: datetime | None = None
    latency_seconds: float | None = Field(default=None, ge=0)
    diagnostics: ImmutableJsonObject = {}
    failure_category: str | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        terminal = self.status is not ProviderAttemptStatus.STARTED
        if terminal != (self.completed_at is not None):
            raise ValueError("terminal provider attempts require completed_at")
        return self


class InformationRound(DomainModel):
    information_round_id: str
    planner_request_id: str
    round_number: int = Field(ge=1)
    status: InformationRoundStatus
    requests: tuple[ImmutableJsonObject, ...] = Field(min_length=1)
    results: ImmutableJsonObject = {}
    requested_at: datetime
    completed_at: datetime | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        terminal = self.status is not InformationRoundStatus.REQUESTED
        if terminal != (self.completed_at is not None):
            raise ValueError("terminal information rounds require completed_at")
        if self.status is InformationRoundStatus.COLLECTED and not self.results:
            raise ValueError("collected information rounds require results")
        return self
