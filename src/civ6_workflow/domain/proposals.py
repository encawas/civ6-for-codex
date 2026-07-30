"""Validated, immutable strategic research proposals."""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Literal, Self

from pydantic import Field, model_validator

from .base import DomainModel
from .contracts import Mission, research_mission_action
from .planner import PlannerRequestTargetKind, canonical_json_hash


STRATEGIC_RESEARCH_PROPOSAL_SCHEMA_VERSION = "strategic-research-proposal/v1"
STRATEGIC_PROPOSAL_WAIT_RESUME_REQUEST_SCHEMA_VERSION = (
    "strategic-proposal-wait-resume-request/v1"
)
STRATEGIC_PROPOSAL_TARGET_KINDS = frozenset(
    {
        PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
        PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
    }
)


def build_strategic_research_proposal_id(source_planner_request_id: str) -> str:
    digest = hashlib.sha256(source_planner_request_id.encode("utf-8")).hexdigest()[:24]
    return f"strategic_proposal_{digest}"


def build_strategic_proposal_wait_resume_request_id(proposal_id: str) -> str:
    digest = hashlib.sha256(proposal_id.encode("utf-8")).hexdigest()[:24]
    return f"strategic_resume_request_{digest}"


class StrategicProposalWaitResumeRequest(DomainModel):
    """Immutable proof that a user explicitly requested Proposal wait release."""

    schema_version: Literal["strategic-proposal-wait-resume-request/v1"] = (
        STRATEGIC_PROPOSAL_WAIT_RESUME_REQUEST_SCHEMA_VERSION
    )
    resume_request_id: str = Field(min_length=1)
    game_session_id: str = Field(min_length=1)
    proposal_id: str = Field(min_length=1)
    planner_request_id: str = Field(min_length=1)
    proposal_ready_tick_id: str = Field(min_length=1)
    target_kind: Literal[
        PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
        PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
    ]
    expected_base_revision: int = Field(ge=0)
    requested_at: datetime

    @model_validator(mode="after")
    def validate_resume_request(self) -> Self:
        if self.resume_request_id != build_strategic_proposal_wait_resume_request_id(
            self.proposal_id
        ):
            raise ValueError("Resume Request ID must be derived from the Proposal")
        if (
            self.target_kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION
            and self.expected_base_revision != 0
        ):
            raise ValueError(
                "Contract creation Resume Request requires base revision 0"
            )
        if (
            self.target_kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR
            and self.expected_base_revision < 1
        ):
            raise ValueError("Mission repair Resume Request requires a positive base")
        if self.requested_at.tzinfo is None or self.requested_at.utcoffset() is None:
            raise ValueError("Resume Request requested_at must include a timezone")
        return self


class StrategicResearchProposal(DomainModel):
    """A system-wrapped candidate awaiting explicit human handling."""

    schema_version: Literal["strategic-research-proposal/v1"] = (
        STRATEGIC_RESEARCH_PROPOSAL_SCHEMA_VERSION
    )
    proposal_id: str = Field(min_length=1)
    proposal_hash: str = Field(min_length=64, max_length=64)
    game_session_id: str = Field(min_length=1)
    source_planner_request_id: str = Field(min_length=1)
    source_provider_attempt_id: str = Field(min_length=1)
    source_provider_attempt_number: int = Field(ge=1)
    target_kind: PlannerRequestTargetKind
    target_contract_id: str = Field(min_length=1)
    expected_base_revision: int = Field(ge=0)
    strategic_objectives: tuple[str, ...]
    global_constraints: tuple[str, ...]
    proposed_research_mission: Mission
    created_from_observation_id: str = Field(min_length=1)
    created_at: datetime

    @model_validator(mode="after")
    def validate_proposal(self) -> Self:
        if self.target_kind not in STRATEGIC_PROPOSAL_TARGET_KINDS:
            raise ValueError("StrategicResearchProposal target kind is not supported")
        if (
            self.target_kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION
            and self.expected_base_revision != 0
        ):
            raise ValueError("Contract creation Proposal requires base revision 0")
        if (
            self.target_kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR
            and self.expected_base_revision < 1
        ):
            raise ValueError(
                "Mission repair Proposal requires a positive base revision"
            )
        if self.proposal_id != build_strategic_research_proposal_id(
            self.source_planner_request_id
        ):
            raise ValueError("Proposal ID must be derived from the PlannerRequest")
        for field_name, values in (
            ("strategic_objectives", self.strategic_objectives),
            ("global_constraints", self.global_constraints),
        ):
            if any(not value.strip() or value != value.strip() for value in values):
                raise ValueError(f"{field_name} require non-empty canonical strings")
            if values != tuple(sorted(set(values))):
                raise ValueError(f"{field_name} must be unique and sorted")
        mission = self.proposed_research_mission
        if mission.scope != "research":
            raise ValueError("Proposal Mission scope must be research")
        if mission.subject.subject_type != "player":
            raise ValueError("Proposal Mission subject must be a player")
        if mission.slot != "player:research":
            raise ValueError("Proposal Mission slot must be player:research")
        if mission.game_session_id != self.game_session_id:
            raise ValueError("Proposal Mission belongs to another game")
        if mission.contract_id != self.target_contract_id:
            raise ValueError("Proposal Mission belongs to another Contract")
        if mission.mission_revision != 1:
            raise ValueError("Proposal Mission revision must be 1")
        research_mission_action(mission)
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("Proposal created_at must include a timezone")
        if self.proposal_hash != strategic_research_proposal_hash(self):
            raise ValueError("Proposal hash does not match canonical durable content")
        return self


def strategic_research_proposal_hash(
    value: StrategicResearchProposal | dict[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json", exclude={"proposal_hash"})
        if isinstance(value, StrategicResearchProposal)
        else {key: item for key, item in value.items() if key != "proposal_hash"}
    )
    created_at = payload.get("created_at")
    if isinstance(created_at, datetime):
        payload["created_at"] = created_at.isoformat().replace("+00:00", "Z")
    return canonical_json_hash(payload)


def build_strategic_research_proposal(**fields: Any) -> StrategicResearchProposal:
    payload = {
        "schema_version": STRATEGIC_RESEARCH_PROPOSAL_SCHEMA_VERSION,
        **fields,
    }
    payload["proposal_hash"] = strategic_research_proposal_hash(payload)
    return StrategicResearchProposal.model_validate(payload)


def build_strategic_proposal_wait_resume_request(
    *,
    game_session_id: str,
    proposal_id: str,
    planner_request_id: str,
    proposal_ready_tick_id: str,
    target_kind: PlannerRequestTargetKind,
    expected_base_revision: int,
    requested_at: datetime,
) -> StrategicProposalWaitResumeRequest:
    return StrategicProposalWaitResumeRequest(
        resume_request_id=build_strategic_proposal_wait_resume_request_id(proposal_id),
        game_session_id=game_session_id,
        proposal_id=proposal_id,
        planner_request_id=planner_request_id,
        proposal_ready_tick_id=proposal_ready_tick_id,
        target_kind=target_kind,
        expected_base_revision=expected_base_revision,
        requested_at=requested_at,
    )
