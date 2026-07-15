"""Immutable, revision-aware approval records."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import Field

from .base import DomainModel, JsonValue


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
    edited_payload: dict[str, JsonValue] | None = None
    replacement_revision: int | None = Field(default=None, ge=1)
