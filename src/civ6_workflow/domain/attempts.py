"""Mutation delivery attempts and verification state."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import Field

from .base import DomainModel, JsonValue, RetryClassification


class AttemptStatus(StrEnum):
    PREPARED = "PREPARED"
    REJECTED_BEFORE_SEND = "REJECTED_BEFORE_SEND"
    FAILED = "FAILED"
    VERIFYING = "VERIFYING"
    UNCERTAIN = "UNCERTAIN"
    SUCCEEDED = "SUCCEEDED"


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
    normalized_arguments: dict[str, JsonValue]
    transport_result: dict[str, JsonValue] | None = None
    tool_result: dict[str, JsonValue] | None = None
    verification_status: str | None = None
    last_verification_observation_id: str | None = None
    parent_attempt_id: str | None = None

    def model_post_init(self, __context: object) -> None:
        if self.status is AttemptStatus.PREPARED and self.sent_at is not None:
            raise ValueError("a prepared attempt has not been sent")
        if self.status in {
            AttemptStatus.VERIFYING,
            AttemptStatus.UNCERTAIN,
            AttemptStatus.SUCCEEDED,
        } and self.sent_at is None:
            raise ValueError(f"{self.status} requires sent_at")
        if self.last_verification_observation_id and self.sent_at is None:
            raise ValueError("verification evidence requires a sent attempt")
