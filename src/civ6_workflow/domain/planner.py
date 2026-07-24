"""Logical planner requests, provider attempts, and information rounds."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import Field, field_validator, model_validator

from .base import DomainModel, ImmutableJsonObject


def _canonical_json_value(value: Any) -> Any:
    if isinstance(value, DomainModel):
        return _canonical_json_value(value.model_dump(mode="json"))
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_json_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_json_value(item) for item in value]
    return value


def canonical_json(value: Any) -> str:
    """Serialize JSON data once for stable identities and response hashes."""

    return json.dumps(
        _canonical_json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def canonical_json_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class PlannerRequestTargetKind(StrEnum):
    LEGACY_DECISION_GROUP = "LEGACY_DECISION_GROUP"
    STRATEGIC_CONTRACT_CREATION = "STRATEGIC_CONTRACT_CREATION"
    MISSION_GRAPH_REPAIR = "MISSION_GRAPH_REPAIR"


class PlannerRequestTarget(DomainModel):
    kind: PlannerRequestTargetKind

    decision_group_id: str | None = None
    decision_gap_ids: tuple[str, ...] = ()

    strategic_contract_id: str | None = None
    base_contract_revision: int | None = None
    strategic_scope: str | None = None
    affected_mission_ids: tuple[str, ...] = ()

    @field_validator("decision_gap_ids", "affected_mission_ids", mode="before")
    @classmethod
    def normalize_identifier_set(cls, value: Any) -> Any:
        if value is None:
            return ()
        if not isinstance(value, (list, tuple)):
            return value
        normalized: list[str] = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("target identifier sets require non-empty strings")
            normalized.append(item.strip())
        if len(set(normalized)) != len(normalized):
            raise ValueError("target identifier sets cannot contain duplicates")
        return tuple(sorted(normalized))

    @field_validator(
        "decision_group_id",
        "strategic_contract_id",
        "strategic_scope",
        mode="before",
    )
    @classmethod
    def normalize_optional_identifier(cls, value: Any) -> Any:
        if value is None or not isinstance(value, str):
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError("target identifiers cannot be blank")
        return normalized

    @model_validator(mode="after")
    def validate_target(self) -> Self:
        legacy_fields = (
            self.strategic_contract_id,
            self.base_contract_revision,
            self.strategic_scope,
        )
        if self.kind is PlannerRequestTargetKind.LEGACY_DECISION_GROUP:
            if not self.decision_gap_ids:
                raise ValueError(
                    "LEGACY_DECISION_GROUP requires decision_gap_ids "
                    "with at least 1 item"
                )
            if any(value is not None for value in legacy_fields):
                raise ValueError(
                    "LEGACY_DECISION_GROUP cannot contain StrategicContract fields"
                )
            if self.affected_mission_ids:
                raise ValueError(
                    "LEGACY_DECISION_GROUP cannot contain affected Mission IDs"
                )
        elif self.kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION:
            if self.decision_group_id is not None or self.decision_gap_ids:
                raise ValueError(
                    "STRATEGIC_CONTRACT_CREATION cannot contain legacy fields"
                )
            if self.strategic_scope is None:
                raise ValueError(
                    "STRATEGIC_CONTRACT_CREATION requires strategic_scope"
                )
            if self.base_contract_revision is not None:
                raise ValueError(
                    "STRATEGIC_CONTRACT_CREATION cannot contain "
                    "base_contract_revision"
                )
            if self.affected_mission_ids:
                raise ValueError(
                    "STRATEGIC_CONTRACT_CREATION cannot contain affected Mission IDs"
                )
        else:
            if self.decision_group_id is not None or self.decision_gap_ids:
                raise ValueError("MISSION_GRAPH_REPAIR cannot contain legacy fields")
            if self.strategic_contract_id is None:
                raise ValueError(
                    "MISSION_GRAPH_REPAIR requires strategic_contract_id"
                )
            if self.base_contract_revision is None or self.base_contract_revision < 1:
                raise ValueError(
                    "MISSION_GRAPH_REPAIR requires base_contract_revision >= 1"
                )
            if self.strategic_scope is None:
                raise ValueError("MISSION_GRAPH_REPAIR requires strategic_scope")
        return self

    @property
    def target_key(self) -> str:
        if self.kind is PlannerRequestTargetKind.LEGACY_DECISION_GROUP:
            identity = {
                "kind": self.kind.value,
                "decision_gap_ids": list(self.decision_gap_ids),
            }
        elif self.kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION:
            identity = {
                "kind": self.kind.value,
                "strategic_scope": self.strategic_scope,
            }
            if self.strategic_contract_id is not None:
                identity["strategic_contract_id"] = self.strategic_contract_id
        else:
            identity = {
                "kind": self.kind.value,
                "strategic_contract_id": self.strategic_contract_id,
                "base_contract_revision": self.base_contract_revision,
                "strategic_scope": self.strategic_scope,
                "affected_mission_ids": list(self.affected_mission_ids),
            }
        return f"planner-target:{canonical_json_hash(identity)}"


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


class PlannerResponseEvidenceCompatibility(StrEnum):
    LEGACY_V7_MISSING_PAYLOAD = "LEGACY_V7_MISSING_PAYLOAD"


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
    target: PlannerRequestTarget
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
    response_payload: ImmutableJsonObject | None = None
    response_hash: str | None = None
    validation_result: ImmutableJsonObject | None = None
    response_evidence_compatibility: (
        PlannerResponseEvidenceCompatibility | None
    ) = None
    pending_information_requests: tuple[ImmutableJsonObject, ...] = ()
    information_results: ImmutableJsonObject = {}
    information_round_count: int = Field(default=0, ge=0)
    provider_attempt_count: int = Field(default=0, ge=0)
    context_bytes: int = Field(default=0, ge=0)
    failure_category: str | None = None
    next_retry_at: datetime | None = None

    @field_validator("plan_revision_refs", "pending_information_requests", mode="before")
    @classmethod
    def restore_json_tuples(cls, value: Any) -> Any:
        return tuple(value) if isinstance(value, list) else value

    @field_validator("created_at", "completed_at", "next_retry_at", mode="before")
    @classmethod
    def restore_json_datetimes(cls, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @model_validator(mode="before")
    @classmethod
    def read_legacy_target(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        payload = dict(value)
        legacy_keys = {
            key for key in ("decision_gap_ids", "decision_group_id") if key in payload
        }
        if "target" in payload and legacy_keys:
            raise ValueError("target cannot be combined with legacy target fields")
        if "target" not in payload and legacy_keys:
            payload["target"] = {
                "kind": PlannerRequestTargetKind.LEGACY_DECISION_GROUP,
                "decision_group_id": payload.pop("decision_group_id", None),
                "decision_gap_ids": payload.pop("decision_gap_ids", ()),
            }
        return payload

    @property
    def decision_gap_ids(self) -> tuple[str, ...]:
        return self.target.decision_gap_ids

    @property
    def decision_group_id(self) -> str | None:
        return self.target.decision_group_id

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Self:
        if self.status in TERMINAL_PLANNER_STATUSES:
            if self.completed_at is None:
                raise ValueError("terminal planner requests require completed_at")
        elif any(
            value is not None
            for value in (
                self.completed_at,
                self.response_payload,
                self.response_hash,
                self.validation_result,
            )
        ):
            raise ValueError(
                "non-terminal planner requests cannot contain result evidence"
            )

        response_statuses = {
            PlannerRequestStatus.COMPLETED,
            PlannerRequestStatus.PARTIALLY_COMPLETED,
            PlannerRequestStatus.REJECTED,
        }
        if self.response_evidence_compatibility is not None:
            if (
                self.target.kind
                is not PlannerRequestTargetKind.LEGACY_DECISION_GROUP
            ):
                raise ValueError(
                    "legacy response compatibility requires a legacy target"
                )
            if self.status not in response_statuses:
                raise ValueError(
                    "legacy response compatibility requires a response-terminal "
                    "status"
                )
            if self.response_payload is not None:
                raise ValueError(
                    "legacy response compatibility cannot include response_payload"
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
                self.target.kind
                is not PlannerRequestTargetKind.LEGACY_DECISION_GROUP
                and self.response_payload is None
            ):
                raise ValueError(
                    "completed non-legacy planner requests require response_payload"
                )
        elif self.status not in response_statuses and self.response_payload is not None:
            raise ValueError(
                "response_payload is only valid for planner response statuses"
            )

        if self.response_payload is not None:
            if self.response_hash is None:
                raise ValueError("response_payload requires response_hash")
            if self.validation_result is None:
                raise ValueError("response_payload requires validation_result")
            if canonical_json_hash(self.response_payload) != self.response_hash:
                raise ValueError("response_hash does not match response_payload")
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
