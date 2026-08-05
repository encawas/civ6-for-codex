from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from .actions import PreparedAction
from .domain import (
    Mission,
    MissionGraphPatch,
    MissionGraphPatchedTick,
    ScopeAuthorityActivatedTick,
    NormalizedObservation,
    ObservationComparisonResult,
    PlannerRequest,
    ProviderAttempt,
    StateDelta,
    StrategicContract,
    StrategicContractCommit,
    StrategicProposalAppliedTick,
    StrategicProposalApprovalRecord,
    StrategicProposalInvalidatedTick,
    StrategicProposalRejectedTick,
    StrategicProposalWaitResumeRequest,
    StrategicResearchProposal,
    TurnActionGraph,
    TurnActionNode,
)
from .models import ActionResult, RuntimeSnapshot, TurnActionExecution


class StaleStrategicContractBaseError(ValueError):
    """Raised when Proposal persistence loses its frozen Contract base."""


class WorkflowStorePort(Protocol):
    """Application-facing persistence boundary implemented by WorkflowStore."""

    path: Path

    @property
    def phase1c_decisions_enabled(self) -> bool: ...

    def commit_strategic_contract_revision(
        self, commit: StrategicContractCommit
    ) -> StrategicContract: ...

    def get_active_strategic_contract(
        self, game_session_id: str
    ) -> StrategicContract | None: ...

    def get_strategic_contract_revision(
        self, game_session_id: str, revision: int
    ) -> StrategicContract | None: ...

    def list_strategic_contract_revisions(
        self, game_session_id: str
    ) -> list[StrategicContract]: ...

    def list_strategic_contract_commits(
        self, game_session_id: str
    ) -> list[StrategicContractCommit]: ...

    def activate_civic_authority(
        self,
        *,
        game_session_id: str,
        expected_base_revision: int,
        mission: Mission,
        activation_id: str,
        observation_id: str,
        turn_number: int,
        activated_at: datetime,
    ) -> tuple[StrategicContract, ScopeAuthorityActivatedTick]: ...

    def activate_opening_strategy_authority(
        self,
        *,
        game_session_id: str,
        expected_base_revision: int,
        mission: Mission,
        activation_id: str,
        observation_id: str,
        turn_number: int,
        activated_at: datetime,
    ) -> tuple[StrategicContract, ScopeAuthorityActivatedTick]: ...

    def activate_settler_authority(
        self,
        *,
        game_session_id: str,
        expected_base_revision: int,
        mission: Mission,
        activation_id: str,
        observation_id: str,
        turn_number: int,
        activated_at: datetime,
    ) -> tuple[StrategicContract, ScopeAuthorityActivatedTick]: ...

    def activate_city_roles_authority(
        self,
        *,
        game_session_id: str,
        expected_base_revision: int,
        mission: Mission,
        activation_id: str,
        observation_id: str,
        turn_number: int,
        activated_at: datetime,
    ) -> tuple[StrategicContract, ScopeAuthorityActivatedTick]: ...

    def activate_diplomacy_trade_authority(
        self,
        *,
        game_session_id: str,
        expected_base_revision: int,
        mission: Mission,
        activation_id: str,
        observation_id: str,
        turn_number: int,
        activated_at: datetime,
    ) -> tuple[StrategicContract, ScopeAuthorityActivatedTick]: ...

    def activate_tactical_emergency_authority(
        self,
        *,
        game_session_id: str,
        expected_base_revision: int,
        mission: Mission,
        activation_id: str,
        observation_id: str,
        turn_number: int,
        activated_at: datetime,
    ) -> tuple[StrategicContract, ScopeAuthorityActivatedTick]: ...

    def save_strategic_research_proposal(
        self, proposal: StrategicResearchProposal
    ) -> StrategicResearchProposal: ...

    def get_strategic_research_proposal(
        self, proposal_id: str
    ) -> StrategicResearchProposal | None: ...

    def strategic_research_proposal_for_request(
        self, planner_request_id: str
    ) -> StrategicResearchProposal | None: ...

    def list_strategic_research_proposals(
        self, game_session_id: str
    ) -> list[StrategicResearchProposal]: ...

    def get_strategic_proposal_wait_resume_request(
        self, resume_request_id: str
    ) -> StrategicProposalWaitResumeRequest | None: ...

    def strategic_proposal_wait_resume_request_for_proposal(
        self, proposal_id: str
    ) -> StrategicProposalWaitResumeRequest | None: ...

    def list_strategic_proposal_wait_resume_requests(
        self, game_session_id: str
    ) -> list[StrategicProposalWaitResumeRequest]: ...

    def strategic_proposal_approval_record(
        self, game_session_id: str, proposal_id: str
    ) -> StrategicProposalApprovalRecord | None: ...

    def approve_strategic_research_proposal(
        self,
        game_session_id: str,
        approval: StrategicProposalApprovalRecord,
        **kwargs: Any,
    ) -> StrategicProposalAppliedTick | StrategicProposalInvalidatedTick: ...

    def reject_strategic_research_proposal(
        self,
        game_session_id: str,
        approval: StrategicProposalApprovalRecord,
        **kwargs: Any,
    ) -> StrategicProposalRejectedTick | StrategicProposalInvalidatedTick: ...

    def invalidate_stale_strategic_research_proposal(
        self,
        game_session_id: str,
        proposal_id: str,
        **kwargs: Any,
    ) -> StrategicProposalInvalidatedTick: ...

    def active_research_mission(
        self, game_id: str
    ) -> tuple[StrategicContract, Mission] | None: ...

    def active_execution_missions(
        self, game_id: str
    ) -> tuple[StrategicContract, tuple[Mission, ...]] | None: ...

    def activate_turn_action_graph(
        self,
        graph: TurnActionGraph,
        nodes: tuple[TurnActionNode, ...],
        *,
        activated_at: datetime,
    ) -> tuple[TurnActionGraph, tuple[TurnActionExecution, ...]]: ...

    def active_turn_action_graph(
        self, game_id: str
    ) -> tuple[TurnActionGraph, tuple[TurnActionExecution, ...]] | None: ...

    def due_turn_action_nodes(
        self,
        game_id: str,
        turn: int,
        *,
        source_observation_id: str,
    ) -> list[TurnActionExecution]: ...

    def invalidate_active_turn_action_graph(
        self,
        game_id: str,
        *,
        expected_contract_revision: int,
        invalidated_at: datetime,
    ) -> None: ...

    def recover_turn_rewind(
        self, game_id: str, loaded_turn: int, *, recovered_at: datetime
    ) -> None: ...

    def save_normalized_observation(
        self, observation: NormalizedObservation
    ) -> NormalizedObservation: ...

    def get_accepted_observation_baseline(
        self, game_id: str
    ) -> NormalizedObservation | None: ...

    def accept_observation_baseline(
        self,
        observation_id: str,
        *,
        expected_previous_observation_id: str | None,
        reason: str,
        accepted_at: datetime,
    ) -> NormalizedObservation: ...

    def record_observation_comparison(
        self,
        observation: NormalizedObservation,
        *,
        detected_at: datetime,
    ) -> ObservationComparisonResult: ...

    def list_state_deltas(self, game_id: str) -> list[StateDelta]: ...

    def get_mission_graph_patch(self, patch_id: str) -> MissionGraphPatch | None: ...

    def list_mission_graph_patches(self, game_id: str) -> list[MissionGraphPatch]: ...

    def apply_mission_graph_patch(
        self,
        *,
        tick: MissionGraphPatchedTick,
        planner_request: PlannerRequest,
        provider_attempt: ProviderAttempt,
        patch: MissionGraphPatch,
        commit: StrategicContractCommit,
        **kwargs: Any,
    ) -> StrategicContract: ...

    def __getattr__(self, name: str) -> Any: ...


class GamePort(Protocol):
    call_count: int

    async def read_snapshot(
        self, *, include_units: bool = False
    ) -> RuntimeSnapshot: ...

    async def execute_task(self, task: TurnActionExecution) -> ActionResult: ...

    async def execute_prepared_action(
        self, prepared: PreparedAction, task: TurnActionExecution
    ) -> ActionResult: ...

    async def end_turn(self, reflections: dict[str, str]) -> ActionResult: ...

    def preflight_mutation(self, tool_name: str) -> None: ...

    async def recover_mutation_session(self) -> bool: ...

    async def list_tools(self) -> set[str]: ...

    async def query_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any: ...


class ReadOnlyGameQueryPort(Protocol):
    call_count: int

    @property
    def call_metrics(self) -> dict[str, float | int]: ...

    async def read_snapshot(
        self, *, include_units: bool = False
    ) -> RuntimeSnapshot: ...

    async def list_tools(self) -> set[str]: ...

    async def query_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any: ...


class ReadOnlyGameQueryPortView:
    """Capability view that does not expose game mutation methods."""

    def __init__(self, delegate: GamePort):
        self._delegate = delegate

    @property
    def call_count(self) -> int:
        return self._delegate.call_count

    @property
    def call_metrics(self) -> dict[str, float | int]:
        value = getattr(self._delegate, "call_metrics", None)
        return dict(value) if isinstance(value, dict) else {}

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        return await self._delegate.read_snapshot(include_units=include_units)

    async def list_tools(self) -> set[str]:
        return await self._delegate.list_tools()

    async def query_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any:
        return await self._delegate.query_tool(name, arguments)


class Planner(Protocol):
    async def plan(self, request: Any) -> Any: ...


class MutationBudgetExceeded(RuntimeError):
    pass


@dataclass(slots=True)
class MutationBudget:
    limit: int = 1
    used: int = 0

    def consume(self, operation: str) -> None:
        if self.used >= self.limit:
            raise MutationBudgetExceeded(
                f"mutation budget exhausted before {operation}"
            )
        self.used += 1


class BoundedGamePort:
    """Per-Tick structural guard around every mutating GamePort call."""

    def __init__(
        self,
        delegate: GamePort,
        budget: MutationBudget,
        *,
        reserved_operation: str | None = None,
    ):
        self.delegate = delegate
        self.budget = budget
        self.reserved_operation = reserved_operation
        self._mutation_called = False

    @property
    def call_count(self) -> int:
        return self.delegate.call_count

    def _consume_or_validate_reservation(self, operation: str) -> None:
        if self._mutation_called:
            raise MutationBudgetExceeded(
                "bounded game port already performed its one mutation"
            )
        if self.reserved_operation is None:
            self.budget.consume(operation)
        elif self.reserved_operation != operation:
            raise MutationBudgetExceeded(
                "reserved mutation does not match the requested operation"
            )
        self._mutation_called = True

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        return await self.delegate.read_snapshot(include_units=include_units)

    async def execute_task(self, task: TurnActionExecution) -> ActionResult:
        self._consume_or_validate_reservation(task.action_type)
        return await self.delegate.execute_task(task)

    async def execute_prepared_action(
        self, prepared: PreparedAction, task: TurnActionExecution
    ) -> ActionResult:
        self._consume_or_validate_reservation(prepared.action_type)
        execute = getattr(self.delegate, "execute_prepared_action", None)
        if execute is not None:
            return await execute(prepared, task)
        return await self.delegate.execute_task(task)

    async def end_turn(self, reflections: dict[str, str] | None = None) -> ActionResult:
        self._consume_or_validate_reservation("end_turn")
        return await self.delegate.end_turn(reflections or {})

    async def list_tools(self) -> set[str]:
        return await self.delegate.list_tools()

    async def query_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any:
        return await self.delegate.query_tool(name, arguments)
