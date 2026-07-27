"""Revisioned strategic contract aggregate."""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import StrEnum

from pydantic import Field

from .base import (
    ApprovalStatus,
    DomainModel,
    ImmutableJsonObject,
    SubjectRef,
)


class MissionStatus(StrEnum):
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    COMPLETED = "COMPLETED"
    INVALIDATED = "INVALIDATED"


class AuthorityScopeSet(DomainModel):
    """Scopes whose strategic write authority belongs to MissionGraph."""

    mission_graph_scopes: tuple[str, ...] = ()

    def model_post_init(self, __context: object) -> None:
        if any(not scope for scope in self.mission_graph_scopes):
            raise ValueError("authority scopes must be non-empty")
        if self.mission_graph_scopes != tuple(sorted(set(self.mission_graph_scopes))):
            raise ValueError("authority scopes must be unique and sorted")


class Mission(DomainModel):
    """Minimum durable mission shape needed by the research vertical slice."""

    mission_id: str = Field(min_length=1)
    game_session_id: str = Field(min_length=1)
    contract_id: str = Field(min_length=1)
    mission_revision: int = Field(ge=1)
    scope: str = Field(min_length=1)
    subject: SubjectRef
    slot: str = Field(min_length=1)
    objective: str = Field(min_length=1)
    desired_outcome: ImmutableJsonObject
    status: MissionStatus = MissionStatus.ACTIVE
    dependency_mission_ids: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()

    def model_post_init(self, __context: object) -> None:
        for field_name, values in (
            ("dependency_mission_ids", self.dependency_mission_ids),
            ("evidence_refs", self.evidence_refs),
        ):
            if values != tuple(sorted(set(values))):
                raise ValueError(f"{field_name} must be unique and sorted")
        if self.mission_id in self.dependency_mission_ids:
            raise ValueError("a Mission cannot depend on itself")


class MissionGraph(DomainModel):
    missions: tuple[Mission, ...] = ()

    def model_post_init(self, __context: object) -> None:
        mission_ids = tuple(mission.mission_id for mission in self.missions)
        if mission_ids != tuple(sorted(set(mission_ids))):
            raise ValueError(
                "MissionGraph missions must have unique, sorted identities"
            )
        known = set(mission_ids)
        for mission in self.missions:
            unknown = set(mission.dependency_mission_ids) - known
            if unknown:
                raise ValueError(
                    f"Mission {mission.mission_id} has unknown dependencies: "
                    f"{sorted(unknown)}"
                )

        dependencies = {
            mission.mission_id: mission.dependency_mission_ids
            for mission in self.missions
        }
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(mission_id: str) -> None:
            if mission_id in visiting:
                raise ValueError("MissionGraph dependencies must be acyclic")
            if mission_id in visited:
                return
            visiting.add(mission_id)
            for dependency_id in dependencies[mission_id]:
                visit(dependency_id)
            visiting.remove(mission_id)
            visited.add(mission_id)

        for mission_id in mission_ids:
            visit(mission_id)


class StrategicContract(DomainModel):
    contract_id: str = Field(min_length=1)
    game_session_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    authority_scope_set: AuthorityScopeSet = AuthorityScopeSet()
    mission_graph: MissionGraph = MissionGraph()
    strategic_objectives: tuple[str, ...] = ()
    global_constraints: tuple[str, ...] = ()
    created_from_observation_id: str | None = None
    approval_status: ApprovalStatus = ApprovalStatus.NOT_REQUIRED
    policy_snapshot: ImmutableJsonObject = {}

    def model_post_init(self, __context: object) -> None:
        if self.strategic_objectives != tuple(sorted(set(self.strategic_objectives))):
            raise ValueError("strategic objectives must be unique and sorted")
        if self.global_constraints != tuple(sorted(set(self.global_constraints))):
            raise ValueError("global constraints must be unique and sorted")
        owned_scopes = set(self.authority_scope_set.mission_graph_scopes)
        for mission in self.mission_graph.missions:
            if mission.game_session_id != self.game_session_id:
                raise ValueError("Mission belongs to another game session")
            if mission.contract_id != self.contract_id:
                raise ValueError("Mission belongs to another StrategicContract")
            if mission.scope not in owned_scopes:
                raise ValueError(
                    "Mission scope must be present in the Authority Scope Set"
                )


class StrategicContractCommit(DomainModel):
    """Idempotent command and durable audit for one aggregate append."""

    commit_id: str = Field(min_length=1)
    game_session_id: str = Field(min_length=1)
    contract_id: str = Field(min_length=1)
    expected_base_revision: int = Field(ge=0)
    contract: StrategicContract
    committed_at: datetime
    reason: str = Field(min_length=1)

    def model_post_init(self, __context: object) -> None:
        if self.committed_at.tzinfo is None or self.committed_at.utcoffset() is None:
            raise ValueError("committed_at must include a timezone")
        if self.contract.game_session_id != self.game_session_id:
            raise ValueError("commit and Contract game sessions must agree")
        if self.contract.contract_id != self.contract_id:
            raise ValueError("commit and Contract identities must agree")
        if self.contract.revision != self.expected_base_revision + 1:
            raise ValueError("Contract revision must immediately follow its base")


def build_strategic_contract_id(game_session_id: str) -> str:
    digest = hashlib.sha256(game_session_id.encode("utf-8")).hexdigest()[:24]
    return f"contract_{digest}"


def build_mission_id(game_session_id: str, scope: str, semantic_key: str) -> str:
    identity = f"{game_session_id}\0{scope}\0{semantic_key}".encode("utf-8")
    digest = hashlib.sha256(identity).hexdigest()[:24]
    return f"mission_{digest}"
