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
    thaw_json,
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
    source_proposal_id: str | None = Field(default=None, min_length=1)
    source_proposal_hash: str | None = Field(default=None, min_length=64, max_length=64)
    source_approval_id: str | None = Field(default=None, min_length=1)
    source_planner_request_id: str | None = Field(default=None, min_length=1)
    source_mission_id: str | None = Field(default=None, min_length=1)
    source_mission_revision: int | None = Field(default=None, ge=1)
    source_patch_id: str | None = Field(default=None, min_length=1)
    source_state_delta_id: str | None = Field(default=None, min_length=1)
    source_patch_planner_request_id: str | None = Field(default=None, min_length=1)
    source_patch_provider_attempt_id: str | None = Field(default=None, min_length=1)
    source_patch_mission_id: str | None = Field(default=None, min_length=1)
    source_patch_mission_revision: int | None = Field(default=None, ge=1)
    source_action_attempt_id: str | None = Field(default=None, min_length=1)
    source_turn_action_graph_id: str | None = Field(default=None, min_length=1)
    source_turn_action_node_id: str | None = Field(default=None, min_length=1)
    source_execution_mission_id: str | None = Field(default=None, min_length=1)
    source_execution_mission_revision: int | None = Field(default=None, ge=1)
    source_scope_activation_id: str | None = Field(default=None, min_length=1)
    source_scope: str | None = Field(default=None, min_length=1)
    source_scope_mission_ids: tuple[str, ...] = ()

    def model_post_init(self, __context: object) -> None:
        if self.committed_at.tzinfo is None or self.committed_at.utcoffset() is None:
            raise ValueError("committed_at must include a timezone")
        if self.contract.game_session_id != self.game_session_id:
            raise ValueError("commit and Contract game sessions must agree")
        if self.contract.contract_id != self.contract_id:
            raise ValueError("commit and Contract identities must agree")
        if self.contract.revision != self.expected_base_revision + 1:
            raise ValueError("Contract revision must immediately follow its base")
        proposal_provenance = (
            self.source_proposal_id,
            self.source_proposal_hash,
            self.source_approval_id,
            self.source_planner_request_id,
            self.source_mission_id,
            self.source_mission_revision,
        )
        if any(value is not None for value in proposal_provenance) and not all(
            value is not None for value in proposal_provenance
        ):
            raise ValueError(
                "Proposal-derived Contract provenance must be all present or all null"
            )
        patch_provenance = (
            self.source_patch_id,
            self.source_state_delta_id,
            self.source_patch_planner_request_id,
            self.source_patch_provider_attempt_id,
            self.source_patch_mission_id,
            self.source_patch_mission_revision,
        )
        if any(value is not None for value in patch_provenance) and not all(
            value is not None for value in patch_provenance
        ):
            raise ValueError(
                "Patch-derived Contract provenance must be all present or all null"
            )
        action_provenance = (
            self.source_action_attempt_id,
            self.source_turn_action_graph_id,
            self.source_turn_action_node_id,
            self.source_execution_mission_id,
            self.source_execution_mission_revision,
        )
        if any(value is not None for value in action_provenance) and not all(
            value is not None for value in action_provenance
        ):
            raise ValueError(
                "Action-derived Contract provenance must be all present or all null"
            )
        scope_activation_provenance = (
            self.source_scope_activation_id,
            self.source_scope,
        )
        if any(value is not None for value in scope_activation_provenance) and not all(
            value is not None for value in scope_activation_provenance
        ):
            raise ValueError(
                "Scope activation Contract provenance must be all present or all null"
            )
        if self.source_scope_mission_ids != tuple(
            sorted(set(self.source_scope_mission_ids))
        ):
            raise ValueError("Scope activation Mission IDs must be unique and sorted")
        if (self.source_scope_activation_id is None) != (
            not self.source_scope_mission_ids
        ):
            raise ValueError("Scope activation provenance requires Mission identities")
        provenance_families = sum(
            value is not None
            for value in (
                self.source_proposal_id,
                self.source_patch_id,
                self.source_action_attempt_id,
                self.source_scope_activation_id,
            )
        )
        if provenance_families > 1:
            raise ValueError(
                "Contract commit cannot combine derived provenance families"
            )
        if self.source_proposal_id is not None:
            if self.contract.approval_status is not ApprovalStatus.APPROVED:
                raise ValueError(
                    "Proposal-derived Contract requires APPROVED approval status"
                )
            matching = tuple(
                mission
                for mission in self.contract.mission_graph.missions
                if mission.mission_id == self.source_mission_id
                and mission.mission_revision == self.source_mission_revision
            )
            if len(matching) != 1:
                raise ValueError(
                    "Proposal-derived Contract provenance must identify one Mission"
                )
            research_mission_action(matching[0])
        if self.source_patch_id is not None:
            matching = tuple(
                mission
                for mission in self.contract.mission_graph.missions
                if mission.mission_id == self.source_patch_mission_id
                and mission.mission_revision == self.source_patch_mission_revision
            )
            if len(matching) != 1:
                raise ValueError(
                    "Patch-derived Contract provenance must identify one Mission"
                )
            strategic_mission_action(matching[0])
        if self.source_action_attempt_id is not None:
            matching = tuple(
                mission
                for mission in self.contract.mission_graph.missions
                if mission.mission_id == self.source_execution_mission_id
                and mission.mission_revision
                == int(self.source_execution_mission_revision) + 1
            )
            if len(matching) != 1:
                raise ValueError(
                    "Action-derived Contract must advance one source Mission"
                )
            mission = matching[0]
            if (
                mission.status is not MissionStatus.COMPLETED
                or self.source_action_attempt_id not in mission.evidence_refs
            ):
                raise ValueError(
                    "Action-derived Contract requires completed Mission evidence"
                )
            strategic_mission_action(
                mission.model_copy(update={"status": MissionStatus.ACTIVE})
            )
        if self.source_scope_activation_id is not None:
            scope = str(self.source_scope)
            if scope not in self.contract.authority_scope_set.mission_graph_scopes:
                raise ValueError("activated scope is absent from authority scope set")
            matching = tuple(
                mission
                for mission in self.contract.mission_graph.missions
                if mission.mission_id in self.source_scope_mission_ids
            )
            if len(matching) != len(self.source_scope_mission_ids) or any(
                mission.scope != scope or mission.status is not MissionStatus.ACTIVE
                for mission in matching
            ):
                raise ValueError(
                    "Scope activation provenance must identify ACTIVE scoped Missions"
                )
            for mission in matching:
                validate_scope_activation_mission(mission, scope)


def research_mission_action(mission: Mission) -> str:
    """Resolve the closed research execution mapping without reading free-form JSON."""

    if mission.scope != "research":
        raise ValueError("Proposal-derived Mission scope must be research")
    if mission.status is not MissionStatus.ACTIVE:
        raise ValueError("Proposal-derived research Mission must be ACTIVE")
    desired_outcome = thaw_json(mission.desired_outcome)
    forbidden_action_keys = {
        "action",
        "action_type",
        "tool",
        "tool_name",
        "operation",
    }
    if forbidden_action_keys.intersection(desired_outcome):
        raise ValueError(
            "research Mission desired_outcome cannot select an execution action"
        )
    technology = desired_outcome.get("technology")
    if not isinstance(technology, str) or not technology.strip():
        raise ValueError(
            "research Mission desired_outcome requires a technology identity"
        )
    return "set_research"


def civic_mission_action(mission: Mission) -> str:
    """Resolve the closed civic execution mapping without free-form tool choice."""

    if mission.scope != "civic":
        raise ValueError("civic Mission scope must be civic")
    if mission.status is not MissionStatus.ACTIVE:
        raise ValueError("civic Mission must be ACTIVE")
    desired_outcome = thaw_json(mission.desired_outcome)
    forbidden_action_keys = {
        "action",
        "action_type",
        "tool",
        "tool_name",
        "operation",
    }
    if forbidden_action_keys.intersection(desired_outcome):
        raise ValueError("civic Mission desired_outcome cannot select an action")
    civic = desired_outcome.get("civic")
    if not isinstance(civic, str) or not civic.strip():
        raise ValueError("civic Mission desired_outcome requires a civic identity")
    return "set_civic"


def opening_strategy_mission_policy(mission: Mission) -> dict[str, object]:
    """Validate the non-executable opening policy held by one Mission."""

    if mission.scope != "opening_strategy":
        raise ValueError("opening strategy Mission scope must be opening_strategy")
    if mission.status is not MissionStatus.ACTIVE:
        raise ValueError("opening strategy Mission must be ACTIVE")
    desired_outcome = thaw_json(mission.desired_outcome)
    if set(desired_outcome) != {"opening_strategy"}:
        raise ValueError(
            "opening strategy Mission desired_outcome must contain only "
            "opening_strategy"
        )
    policy = desired_outcome["opening_strategy"]
    if not isinstance(policy, dict) or not policy:
        raise ValueError("opening strategy Mission requires a non-empty policy")

    forbidden = {"action", "action_type", "tool", "tool_name", "operation"}

    def contains_action_selector(value: object) -> bool:
        if isinstance(value, dict):
            return bool(forbidden.intersection(value)) or any(
                contains_action_selector(item) for item in value.values()
            )
        if isinstance(value, list):
            return any(contains_action_selector(item) for item in value)
        return False

    if contains_action_selector(policy):
        raise ValueError("opening strategy policy cannot select an execution action")
    return policy


def validate_scope_activation_mission(mission: Mission, scope: str) -> None:
    """Validate one Mission introduced by a reviewed authority switch."""

    if mission.scope != scope:
        raise ValueError("scope activation Mission belongs to another scope")
    if scope == "opening_strategy":
        opening_strategy_mission_policy(mission)
        return
    strategic_mission_action(mission)


def strategic_mission_action(mission: Mission) -> str:
    """Resolve one supported strategic execution action by authoritative scope."""

    if mission.scope == "research":
        return research_mission_action(mission)
    if mission.scope == "civic":
        return civic_mission_action(mission)
    raise ValueError(f"scope has no Phase 5 execution contract: {mission.scope}")


def build_strategic_contract_id(game_session_id: str) -> str:
    digest = hashlib.sha256(game_session_id.encode("utf-8")).hexdigest()[:24]
    return f"contract_{digest}"


def build_mission_id(game_session_id: str, scope: str, semantic_key: str) -> str:
    identity = f"{game_session_id}\0{scope}\0{semantic_key}".encode("utf-8")
    digest = hashlib.sha256(identity).hexdigest()[:24]
    return f"mission_{digest}"
