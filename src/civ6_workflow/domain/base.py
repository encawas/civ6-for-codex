"""Shared primitives for the canonical workflow domain."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, JsonValue


class DomainModel(BaseModel):
    """Strict, immutable-at-the-boundary base for canonical domain values."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ApprovalStatus(StrEnum):
    NOT_REQUIRED = "NOT_REQUIRED"
    REQUIRED = "REQUIRED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class SubjectRef(DomainModel):
    subject_type: str
    subject_id: str


class Condition(DomainModel):
    condition_type: str
    schema_version: int = 1
    subject: SubjectRef | None = None
    parameters: dict[str, JsonValue] = {}
    expected: JsonValue = True


class SourceVersions(DomainModel):
    game_api: str
    normalization: str
    runtime: str


class RetryClassification(StrEnum):
    IDEMPOTENT_OR_DEDUPED = "IDEMPOTENT_OR_DEDUPED"
    SAFE_IF_PROVEN_NOT_SENT = "SAFE_IF_PROVEN_NOT_SENT"
    NEVER_BLIND_RETRY = "NEVER_BLIND_RETRY"
