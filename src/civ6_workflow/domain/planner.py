"""Logical planner request records, separate from provider attempts."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import Field

from .base import DomainModel, JsonValue


class PlannerRequestStatus(StrEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    BACKOFF = "BACKOFF"
    CANCELLED = "CANCELLED"


class PlannerRequest(DomainModel):
    planner_request_id: str
    game_session_id: str
    turn_number: int = Field(ge=0)
    observation_id: str
    decision_gap_ids: tuple[str, ...]
    input_projection_hash: str
    plan_revision_refs: tuple[str, ...] = ()
    policy_revision: str
    model_settings: dict[str, JsonValue]
    status: PlannerRequestStatus
    created_at: datetime
    completed_at: datetime | None = None
    response_hash: str | None = None
    validation_result: dict[str, JsonValue] | None = None

    def model_post_init(self, __context: object) -> None:
        if self.status is PlannerRequestStatus.COMPLETED:
            if self.completed_at is None or self.response_hash is None:
                raise ValueError("completed planner requests require completion evidence")
