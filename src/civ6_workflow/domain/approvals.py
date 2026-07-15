"""Immutable, revision-aware approval records."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, model_validator

from .base import DomainModel, ImmutableJsonObject


class ApprovalDecision(StrEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    REQUESTED_REPLAN = "REQUESTED_REPLAN"
    EDITED_AND_APPROVED = "EDITED_AND_APPROVED"


class ApprovalRecord(DomainModel):
    approval_id: str
    proposal_type: str
    proposal_id: str
    proposal_revision: int = Field(ge=1)
    decision: ApprovalDecision
    actor: str
    created_at: datetime
    reason: str | None = None
    edited_payload: ImmutableJsonObject | None = None
    replacement_revision: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_revision_decision(self) -> Self:
        if self.decision is ApprovalDecision.EDITED_AND_APPROVED:
            if self.edited_payload is None or self.replacement_revision is None:
                raise ValueError(
                    "edited-and-approved requires payload and replacement revision"
                )
            if self.replacement_revision <= self.proposal_revision:
                raise ValueError("replacement revision must advance proposal revision")
        elif self.edited_payload is not None or self.replacement_revision is not None:
            raise ValueError(
                "only edited-and-approved may contain replacement revision data"
            )
        return self
