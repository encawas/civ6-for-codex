"""Mutation delivery attempts and verification state."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, model_validator

from .base import DomainModel, ImmutableJsonObject, RetryClassification


class AttemptStatus(StrEnum):
    PREPARED = "PREPARED"
    REJECTED_BEFORE_SEND = "REJECTED_BEFORE_SEND"
    FAILED = "FAILED"
    VERIFYING = "VERIFYING"
    UNCERTAIN = "UNCERTAIN"
    SUCCEEDED = "SUCCEEDED"


UNRESOLVED_ATTEMPT_STATUSES = frozenset(
    {
        AttemptStatus.PREPARED,
        AttemptStatus.VERIFYING,
        AttemptStatus.UNCERTAIN,
    }
)


class VerificationStatus(StrEnum):
    PENDING = "PENDING"
    INCONCLUSIVE = "INCONCLUSIVE"
    FAILED = "FAILED"
    PASSED = "PASSED"


class ActionAttempt(DomainModel):
    action_attempt_id: str
    task_id: str
    attempt_number: int = Field(ge=1)
    request_id: str
    idempotency_key: str
    prepared_from_observation_id: str
    prepared_at: datetime
    sent_at: datetime | None = None
    response_received_at: datetime | None = None
    status: AttemptStatus
    retry_classification: RetryClassification
    normalized_arguments: ImmutableJsonObject
    transport_result: ImmutableJsonObject | None = None
    tool_result: ImmutableJsonObject | None = None
    verification_status: VerificationStatus | None = None
    last_verification_observation_id: str | None = None
    parent_attempt_id: str | None = None
    game_session_id: str | None = None
    action_type: str | None = None
    postconditions: tuple[ImmutableJsonObject, ...] = ()
    postcondition_version: int = Field(default=1, ge=1)
    verification_count: int = Field(default=0, ge=0)
    pre_send_turn: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.status is AttemptStatus.PREPARED:
            evidence = (
                self.sent_at,
                self.response_received_at,
                self.transport_result,
                self.tool_result,
                self.verification_status,
                self.last_verification_observation_id,
            )
            if any(value is not None for value in evidence):
                raise ValueError("a prepared attempt cannot contain delivery evidence")

        if self.status is AttemptStatus.REJECTED_BEFORE_SEND:
            if self.sent_at is not None:
                raise ValueError("a rejected-before-send attempt cannot have sent_at")
            if any(
                value is not None
                for value in (
                    self.response_received_at,
                    self.tool_result,
                    self.verification_status,
                    self.last_verification_observation_id,
                )
            ):
                raise ValueError(
                    "a rejected-before-send attempt cannot contain response or verification evidence"
                )

        if (
            self.status
            in {
                AttemptStatus.VERIFYING,
                AttemptStatus.UNCERTAIN,
                AttemptStatus.SUCCEEDED,
            }
            and self.sent_at is None
        ):
            raise ValueError(f"{self.status} requires sent_at")

        if self.status is AttemptStatus.SUCCEEDED:
            if self.last_verification_observation_id is None:
                raise ValueError(
                    "a succeeded attempt requires a verification observation"
                )
            if self.verification_status is not VerificationStatus.PASSED:
                raise ValueError("a succeeded attempt requires passed verification")
        elif self.verification_status is VerificationStatus.PASSED:
            raise ValueError("passed verification requires succeeded attempt status")

        if self.response_received_at is not None and self.sent_at is None:
            raise ValueError("response_received_at requires sent_at")
        if self.last_verification_observation_id is not None and self.sent_at is None:
            raise ValueError("verification evidence requires sent_at")
        if self.parent_attempt_id == self.action_attempt_id:
            raise ValueError("an attempt cannot be its own parent")
        if self.action_type == "end_turn" and self.pre_send_turn is None:
            raise ValueError("an end-turn attempt requires pre_send_turn")

        try:
            if self.sent_at is not None and self.sent_at < self.prepared_at:
                raise ValueError("sent_at must not precede prepared_at")
            if (
                self.response_received_at is not None
                and self.sent_at is not None
                and self.response_received_at < self.sent_at
            ):
                raise ValueError("response_received_at must not precede sent_at")
        except TypeError as exc:
            raise ValueError(
                "attempt timestamps must use compatible timezones"
            ) from exc
        return self
