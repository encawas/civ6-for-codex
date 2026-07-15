"""Logical planner request records, separate from provider attempts."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, model_validator

from .base import DomainModel, ImmutableJsonObject


class PlannerRequestStatus(StrEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    BACKOFF = "BACKOFF"
    CANCELLED = "CANCELLED"


TERMINAL_PLANNER_STATUSES = frozenset(
    {
        PlannerRequestStatus.COMPLETED,
        PlannerRequestStatus.FAILED,
        PlannerRequestStatus.CANCELLED,
    }
)


class PlannerRequest(DomainModel):
    planner_request_id: str
    game_session_id: str
    turn_number: int = Field(ge=0)
    observation_id: str
    decision_gap_ids: tuple[str, ...] = Field(min_length=1)
    input_projection_hash: str
    plan_revision_refs: tuple[str, ...] = ()
    policy_revision: str
    model_settings: ImmutableJsonObject
    status: PlannerRequestStatus
    created_at: datetime
    completed_at: datetime | None = None
    response_hash: str | None = None
    validation_result: ImmutableJsonObject | None = None

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

        if self.status is PlannerRequestStatus.COMPLETED:
            if self.response_hash is None or self.validation_result is None:
                raise ValueError(
                    "completed planner requests require validated response evidence"
                )

        try:
            if self.completed_at is not None and self.completed_at < self.created_at:
                raise ValueError("completed_at must not precede created_at")
        except TypeError as exc:
            raise ValueError(
                "planner timestamps must use compatible timezones"
            ) from exc
        return self
