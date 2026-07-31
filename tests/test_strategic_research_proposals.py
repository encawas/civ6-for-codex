import asyncio
import copy
import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta

import pytest

from civ6_workflow.bootstrap import build_runtime_services, build_store
from civ6_workflow.config import AppConfig
from civ6_workflow.domain import (
    AuthorityScopeSet,
    ApprovalDecision,
    ApprovalStatus,
    AwaitingHumanTick,
    InformationRequestedTick,
    InformationRound,
    InformationRoundStatus,
    Mission,
    MissionGraph,
    MissionGraphPatchedTick,
    MissionStatus,
    ObservedOnlyTick,
    PlannerAttemptCompletedTick,
    ProviderAttempt,
    ProviderAttemptStatus,
    PlannerRequest,
    PlannerRequestStatus,
    PlannerRequestTarget,
    PlannerRequestTargetKind,
    RuntimeState,
    StrategicContract,
    StrategicContractCommit,
    StrategicProposalApprovalRecord,
    StrategicProposalAppliedTick,
    StrategicProposalInvalidatedTick,
    StrategicProposalInvalidationOrigin,
    StrategicProposalInvalidationReason,
    StrategicProposalRejectedTick,
    StrategicProposalReadyTick,
    StrategicRequestTerminatedTick,
    StrategicRequestWaitErrorTick,
    StrategicRequestWaitResumedTick,
    StrategicProposalWaitErrorTick,
    StrategicProposalWaitResumedTick,
    SubjectRef,
    TickOutcomeKind,
    build_strategic_contract_id,
    build_strategic_proposal_terminal_tick_id,
    build_strategic_proposal_wait_resume_request_id,
    build_strategic_research_proposal,
    build_strategic_research_proposal_id,
    canonical_json_hash,
)
from civ6_workflow.runtime import RuntimeConfig, InjectedCrashBoundary, WorkflowRuntime
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    MutationDeliveryStatus,
    RiskLevel,
    RuntimeSnapshot,
    TurnActionExecution,
    TaskStatus,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore
from civ6_workflow.web_ui import ControlPanelState
from civ6_workflow.workflow_protocol import (
    InformationRequest,
    MissionGraphPatchCandidate,
    MissionGraphPatchResponse,
    StrategicResearchProposalCandidate,
    StrategicResearchProposalResponse,
    WorkflowAgentRequest,
)


NOW = datetime.now(UTC)


class _Game:
    def __init__(self, game_id: str = "game-1"):
        self.snapshot = RuntimeSnapshot(
            turn=1,
            game_id=game_id,
            overview={"turn": 1, "player_id": 1, "num_cities": 1},
            cities=[{"city_id": 1, "currently_building": "UNIT_SCOUT"}],
            units=[],
            blockers=[],
        )
        self.call_count = 0
        self.query_count = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        return self.snapshot.model_copy(deep=True)

    async def list_tools(self):
        return {
            "set_city_production",
            "set_civic",
            "set_research",
            "unit_action",
            "end_turn",
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "get_policies",
        }

    async def query_tool(self, name, arguments=None):
        self.call_count += 1
        self.query_count += 1
        return {"notifications": [], "tool": name}

    async def execute_task(self, task):
        raise AssertionError("Proposal generation cannot execute TurnActionExecution")

    async def end_turn(self, reflections=None):
        raise AssertionError("Proposal generation cannot end the turn")


class _Planner:
    def __init__(self, *responses, on_call=None):
        self.responses = list(responses)
        self.on_call = on_call
        self.calls = 0
        self.requests = []
        self.last_diagnostics = None

    async def plan(self, request):
        self.calls += 1
        self.requests.append(request)
        self.last_diagnostics = {"attempt_count": 1, "backend": "test"}
        if self.on_call is not None:
            self.on_call()
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _contract(game_id: str, revision: int = 1) -> StrategicContract:
    return StrategicContract(
        contract_id=build_strategic_contract_id(game_id),
        game_session_id=game_id,
        revision=revision,
        authority_scope_set=AuthorityScopeSet(),
        mission_graph=MissionGraph(),
        created_from_observation_id=f"obs-contract-{revision}",
        strategic_objectives=(f"objective-{revision}",),
    )


def _commit_contract(store: WorkflowStore, game_id: str, revision: int = 1):
    contract = _contract(game_id, revision)
    commit = StrategicContractCommit(
        commit_id=f"contract-commit-{game_id}-{revision}",
        game_session_id=game_id,
        contract_id=contract.contract_id,
        expected_base_revision=revision - 1,
        contract=contract,
        committed_at=NOW + timedelta(minutes=revision),
        reason="test foundation contract",
    )
    return store.commit_strategic_contract_revision(commit)


def _mission(
    game_id: str,
    contract_id: str,
    *,
    scope: str = "research",
    subject_type: str = "player",
    slot: str = "player:research",
    revision: int = 1,
) -> Mission:
    return Mission(
        mission_id="mission-research-writing",
        game_session_id=game_id,
        contract_id=contract_id,
        mission_revision=revision,
        scope=scope,
        subject=SubjectRef(subject_type=subject_type, subject_id="player-1"),
        slot=slot,
        objective="Research Writing",
        desired_outcome={"technology": "TECH_WRITING"},
    )


def _response(game_id: str, contract_id: str, *, candidates=1, mission=None):
    candidate = StrategicResearchProposalCandidate(
        strategic_objectives=("Unlock campuses", "Unlock campuses"),
        global_constraints=("Do not change research authority",),
        proposed_research_mission=mission or _mission(game_id, contract_id),
        created_from_observation_id="obs-source",
    )
    return StrategicResearchProposalResponse(
        schema_version="strategic-research-proposal-response/v1",
        proposal_candidates=tuple(candidate for _ in range(candidates)),
    )


def _information_response():
    return StrategicResearchProposalResponse(
        schema_version="strategic-research-proposal-response/v1",
        information_requests=(
            InformationRequest(
                event_dedupe_key="strategic-research-context",
                query_type="notifications",
                tool_name="get_policies",
                purpose="Check known research constraints",
            ),
        ),
    )


def _request(
    game_id: str,
    *,
    kind=PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
    contract_id: str | None = None,
    base_revision: int | None = None,
) -> PlannerRequest:
    resolved_contract_id = contract_id or build_strategic_contract_id(game_id)
    expected_base = (
        0
        if kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION
        else base_revision
    )
    target = PlannerRequestTarget(
        kind=kind,
        strategic_contract_id=contract_id,
        base_contract_revision=(
            None
            if kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION
            else base_revision
        ),
        strategic_scope="research",
    )
    projection = {
        "strategic_proposal_context": {
            "target_contract_id": resolved_contract_id,
            "expected_base_revision": expected_base,
            "strategic_scope": "research",
        }
    }
    provider_request = WorkflowAgentRequest(
        turn=1,
        execution_mode=ExecutionMode.AUTO,
        trigger_events=[],
        relevant_state={"research": {"current": None}},
        constraints={
            "planning_phase": "initial",
            "allow_information_requests": True,
        },
    )
    return PlannerRequest(
        planner_request_id=f"request-{kind.value.lower()}-{game_id}",
        game_session_id=game_id,
        turn_number=1,
        observation_id="obs-source",
        target=target,
        input_projection_hash=canonical_json_hash(projection),
        input_projection_version="strategic-proposal-input/v1",
        input_projection=projection,
        request_payload=provider_request.model_dump(mode="json"),
        policy_revision="strategic-proposal-policy/v1",
        approval_contract_hash="proposal-only",
        allowed_actions_hash="no-actions",
        model_settings={"provider": "test"},
        status=PlannerRequestStatus.PENDING,
        created_at=NOW,
        context_bytes=100,
    )


def _engine(store, game, planner):
    return WorkflowRuntime(
        service_factory=build_runtime_services,
        store=store,
        game=game,
        planner=planner,
        config=RuntimeConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=False,
            max_agent_calls_per_turn=0,
        ),
    )


def _assert_proposal_wait(store, request, proposal):
    wait = store.human_wait_context(request.game_session_id)
    assert wait is not None
    assert wait["wait_kind"] == "strategic_contract_proposal_ready"
    assert wait["resume_policy"] == "explicit_only"
    assert wait["reason"] == "strategic_contract_proposal_ready"
    assert wait["planner_request_id"] == request.planner_request_id
    assert wait["proposal_id"] == proposal.proposal_id
    assert wait["target_kind"] == request.target.kind.value
    assert wait["expected_base_revision"] == proposal.expected_base_revision
    assert wait["resume_requested"] is False
    assert "resume_request_id" not in wait
    assert "resume_requested_at" not in wait


def test_creation_uses_isolated_lifecycle_and_persists_proposal(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "creation.sqlite3")
        game = _Game()
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(_response("game-1", contract_id))
        engine = _engine(store, game, planner)
        request = _request("game-1")
        store.save_planner_request(request)

        for name in (
            "_supersede_stale_request",
            "_request_gaps",
            "_partition_bundle",
            "_resolve_gaps",
        ):
            assert not hasattr(engine.strategic_workflow.planner_lifecycle, name)

        result = await engine.tick()

        assert (
            result.workflow_tick["outcome"] == TickOutcomeKind.STRATEGIC_PROPOSAL_READY
        )
        assert result.runtime_state == RuntimeState.AWAITING_HUMAN
        stored_request = store.get_planner_request(request.planner_request_id)
        assert stored_request.status is PlannerRequestStatus.COMPLETED
        proposals = store.list_strategic_research_proposals("game-1")
        assert len(proposals) == 1
        proposal = proposals[0]
        assert proposal.source_provider_attempt_number == 1
        assert proposal.target_contract_id == contract_id
        assert proposal.expected_base_revision == 0
        assert proposal.strategic_objectives == ("Unlock campuses",)
        assert store.get_active_strategic_contract("game-1") is None
        assert store.list_decision_gaps("game-1") == []
        assert store.list_plan_leases("game-1") == []
        assert store.list_tasks("game-1") == []
        wait = store.human_wait_context("game-1")
        assert wait["wait_kind"] == "strategic_contract_proposal_ready"
        assert wait["resume_policy"] == "explicit_only"

    asyncio.run(scenario())


def test_repair_binds_active_contract_revision(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "repair.sqlite3")
        active = _commit_contract(store, "game-1")
        request = _request(
            "game-1",
            kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            contract_id=active.contract_id,
            base_revision=active.revision,
        )
        planner = _Planner(_response("game-1", active.contract_id))
        store.save_planner_request(request)

        result = await _engine(store, _Game(), planner).tick()

        assert result.workflow_tick["expected_base_revision"] == active.revision
        proposal = store.list_strategic_research_proposals("game-1")[0]
        assert proposal.target_kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR
        assert proposal.expected_base_revision == active.revision
        assert store.get_active_strategic_contract("game-1") == active

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["creation", "repair"])
def test_stale_base_before_provider_supersedes_without_attempt(tmp_path, kind):
    async def scenario():
        store = WorkflowStore(tmp_path / f"stale-{kind}.sqlite3")
        if kind == "creation":
            _commit_contract(store, "game-1")
            request = _request("game-1")
        else:
            active = _commit_contract(store, "game-1")
            request = _request(
                "game-1",
                kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
                contract_id=active.contract_id,
                base_revision=active.revision + 1,
            )
        planner = _Planner(_response("game-1", build_strategic_contract_id("game-1")))
        store.save_planner_request(request)

        await _engine(store, _Game(), planner).tick()

        stored = store.get_planner_request(request.planner_request_id)
        assert stored.status is PlannerRequestStatus.SUPERSEDED
        assert stored.failure_category == "stale_strategic_contract_base"
        assert planner.calls == 0
        assert store.list_provider_attempts(request.planner_request_id) == []

    asyncio.run(scenario())


def test_contract_change_during_provider_rejects_with_canonical_evidence(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "stale-during-provider.sqlite3")
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(
            _response("game-1", contract_id),
            on_call=lambda: _commit_contract(store, "game-1"),
        )
        request = _request("game-1")
        store.save_planner_request(request)

        result = await _engine(store, _Game(), planner).tick()

        stored = store.get_planner_request(request.planner_request_id)
        assert result.workflow_tick["outcome"] == (
            TickOutcomeKind.STRATEGIC_REQUEST_TERMINATED
        )
        assert stored.status is PlannerRequestStatus.REJECTED
        assert stored.failure_category == "stale_strategic_contract_base"
        assert stored.response_payload is not None
        assert stored.response_hash == canonical_json_hash(stored.response_payload)
        assert stored.validation_result is not None
        assert store.list_strategic_research_proposals("game-1") == []
        attempts = store.list_provider_attempts(request.planner_request_id)
        assert attempts[-1].status.value == "SUCCEEDED"

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("mission_updates", "category"),
    [
        ({"scope": "civic"}, "invalid_strategic_proposal"),
        (
            {"subject": SubjectRef(subject_type="city", subject_id="1")},
            "invalid_strategic_proposal",
        ),
        ({"slot": "player:civic"}, "invalid_strategic_proposal"),
        ({"mission_revision": 2}, "invalid_strategic_proposal"),
        ({"contract_id": "other-contract"}, "invalid_strategic_proposal"),
    ],
)
def test_schema_valid_invalid_mission_is_auditable_rejection(
    tmp_path, mission_updates, category
):
    async def scenario():
        store = WorkflowStore(
            tmp_path / f"invalid-{next(iter(mission_updates))}.sqlite3"
        )
        contract_id = build_strategic_contract_id("game-1")
        mission = _mission("game-1", contract_id).model_copy(update=mission_updates)
        planner = _Planner(_response("game-1", contract_id, mission=mission))
        request = _request("game-1")
        store.save_planner_request(request)

        await _engine(store, _Game(), planner).tick()

        stored = store.get_planner_request(request.planner_request_id)
        assert stored.status is PlannerRequestStatus.REJECTED
        assert stored.failure_category == category
        assert stored.response_payload is not None
        assert stored.response_hash == canonical_json_hash(stored.response_payload)
        assert stored.validation_result is not None
        assert store.list_strategic_research_proposals("game-1") == []

    asyncio.run(scenario())


@pytest.mark.parametrize("candidate_count", [0, 2])
def test_candidate_count_rejection_preserves_response_evidence(
    tmp_path, candidate_count
):
    async def scenario():
        store = WorkflowStore(tmp_path / f"candidate-count-{candidate_count}.sqlite3")
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(_response("game-1", contract_id, candidates=candidate_count))
        request = _request("game-1")
        store.save_planner_request(request)

        await _engine(store, _Game(), planner).tick()

        stored = store.get_planner_request(request.planner_request_id)
        assert stored.status is PlannerRequestStatus.REJECTED
        assert stored.failure_category == "invalid_proposal_candidate_count"
        assert stored.response_payload is not None
        assert stored.response_hash == canonical_json_hash(stored.response_payload)

    asyncio.run(scenario())


def test_schema_failure_has_no_canonical_response_evidence(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "schema-failure.sqlite3")
        request = _request("game-1")
        store.save_planner_request(request)

        await _engine(store, _Game(), _Planner("not-json")).tick()

        stored = store.get_planner_request(request.planner_request_id)
        assert stored.status is PlannerRequestStatus.REJECTED
        assert stored.failure_category == "planner_contract_failure"
        assert stored.response_payload is None
        assert stored.response_hash is None
        assert stored.validation_result is None
        assert (
            store.list_provider_attempts(request.planner_request_id)[-1].status.value
            == "SUCCEEDED"
        )

    asyncio.run(scenario())


def test_information_round_then_proposal_and_second_round_limit(tmp_path):
    async def successful_round():
        store = WorkflowStore(tmp_path / "information-success.sqlite3")
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(
            _information_response(),
            _response("game-1", contract_id),
        )
        game = _Game()
        request = _request("game-1")
        store.save_planner_request(request)
        engine = _engine(store, game, planner)

        first = await engine.tick()
        awaiting = store.get_planner_request(request.planner_request_id)
        requested_round = store.list_information_rounds(request.planner_request_id)[0]
        assert awaiting is not None
        assert awaiting.status is PlannerRequestStatus.AWAITING_INFORMATION
        assert requested_round.status is InformationRoundStatus.REQUESTED
        store.save_planner_request(awaiting)
        store.save_information_round("game-1", requested_round)

        second = await engine.tick()
        ready = store.get_planner_request(request.planner_request_id)
        collected_round = store.list_information_rounds(request.planner_request_id)[0]
        assert ready is not None
        assert ready.status is PlannerRequestStatus.READY_TO_CONTINUE
        assert ready.information_round_count == 1
        assert ready.information_results == collected_round.results
        assert collected_round.status is InformationRoundStatus.COLLECTED
        store.save_planner_request(ready)
        store.save_information_round("game-1", collected_round)

        third = await engine.tick()

        assert first.workflow_tick["outcome"] == TickOutcomeKind.INFORMATION_REQUESTED
        assert second.workflow_tick["outcome"] == TickOutcomeKind.INFORMATION_COLLECTED
        assert (
            third.workflow_tick["outcome"] == TickOutcomeKind.STRATEGIC_PROPOSAL_READY
        )
        assert store.request_human_resume("game-1") is True
        fourth = await engine.tick()
        assert fourth.workflow_tick["outcome"] == (
            TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
        )
        assert game.query_count == 1
        assert planner.calls == 2
        assert store.human_wait_context("game-1") is None
        assert len(store.list_strategic_proposal_wait_resume_requests("game-1")) == 1
        assert store.get_active_strategic_contract("game-1") is None
        assert store.list_tasks("game-1") == []

    async def limited_round():
        store = WorkflowStore(tmp_path / "information-limit.sqlite3")
        planner = _Planner(_information_response(), _information_response())
        request = _request("game-1")
        store.save_planner_request(request)
        engine = _engine(store, _Game(), planner)

        await engine.tick()
        await engine.tick()
        await engine.tick()

        stored = store.get_planner_request(request.planner_request_id)
        assert stored.status is PlannerRequestStatus.REJECTED
        assert stored.failure_category == "information_round_limit_exceeded"
        assert stored.response_payload is not None

    asyncio.run(successful_round())
    asyncio.run(limited_round())


def test_proposal_wait_survives_repeated_observation_changes(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "observation-changes.sqlite3")
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(_response("game-1", contract_id))
        game = _Game()
        request = _request("game-1")
        store.save_planner_request(request)
        engine = _engine(store, game, planner)

        await engine.tick()
        proposal = store.list_strategic_research_proposals("game-1")[0]
        _assert_proposal_wait(store, request, proposal)

        game.snapshot = game.snapshot.model_copy(
            update={"turn": 2, "notifications": [{"changed": True}]}
        )
        await engine.tick()
        _assert_proposal_wait(store, request, proposal)

        game.snapshot = game.snapshot.model_copy(
            update={"turn": 3, "notifications": [{"changed": "again"}]}
        )
        await engine.tick()
        _assert_proposal_wait(store, request, proposal)

        assert planner.calls == 1
        assert store.load_runtime_state("game-1") is RuntimeState.AWAITING_HUMAN

    asyncio.run(scenario())


def test_proposal_wait_survives_auto_mode_after_intermediate_wait(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "auto-after-wait.sqlite3")
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(_response("game-1", contract_id))
        game = _Game()
        request = _request("game-1")
        store.save_planner_request(request)
        engine = _engine(store, game, planner)
        engine.config.execution_mode = ExecutionMode.CONFIRM

        await engine.tick()
        proposal = store.list_strategic_research_proposals("game-1")[0]
        await engine.tick()
        _assert_proposal_wait(store, request, proposal)

        engine.config.execution_mode = ExecutionMode.AUTO
        result = await engine.tick()

        assert result.runtime_state == RuntimeState.AWAITING_HUMAN
        _assert_proposal_wait(store, request, proposal)
        assert planner.calls == 1
        assert store.get_active_strategic_contract("game-1") is None
        assert store.list_tasks("game-1") == []

    asyncio.run(scenario())


def test_proposal_wait_survives_restart_after_intermediate_wait(tmp_path):
    async def scenario():
        path = tmp_path / "restart.sqlite3"
        store = WorkflowStore(path)
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(_response("game-1", contract_id))
        game = _Game()
        request = _request("game-1")
        store.save_planner_request(request)
        engine = _engine(store, game, planner)

        await engine.tick()
        proposal = store.list_strategic_research_proposals("game-1")[0]
        await engine.tick()
        _assert_proposal_wait(store, request, proposal)

        restarted_store = WorkflowStore(path)
        restarted_game = _Game()
        restarted_game.snapshot = restarted_game.snapshot.model_copy(
            update={"turn": 2, "notifications": [{"after_restart": True}]}
        )
        result = await _engine(restarted_store, restarted_game, planner).tick()

        assert result.runtime_state == RuntimeState.AWAITING_HUMAN
        _assert_proposal_wait(restarted_store, request, proposal)
        assert planner.calls == 1

    asyncio.run(scenario())


def test_proposal_wait_survives_replay_after_intermediate_wait(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "source.sqlite3")
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(_response("game-1", contract_id))
        request = _request("game-1")
        store.save_planner_request(request)
        engine = _engine(store, _Game(), planner)

        await engine.tick()
        proposal = store.list_strategic_research_proposals("game-1")[0]
        await engine.tick()
        _assert_proposal_wait(store, request, proposal)
        exported = store.export_replay_state("game-1")

        restored = WorkflowStore(tmp_path / "restored.sqlite3")
        restored.import_replay_state(exported)
        restored_game = _Game()
        restored_game.snapshot = restored_game.snapshot.model_copy(
            update={"turn": 2, "notifications": [{"after_replay": True}]}
        )
        result = await _engine(restored, restored_game, planner).tick()

        assert result.runtime_state == RuntimeState.AWAITING_HUMAN
        _assert_proposal_wait(restored, request, proposal)
        assert planner.calls == 1

    asyncio.run(scenario())


def test_explicit_resume_after_intermediate_proposal_wait_does_not_apply(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "explicit-only.sqlite3")
        active = _commit_contract(store, "game-1")
        planner = _Planner(_response("game-1", active.contract_id))
        game = _Game()
        request = _request(
            "game-1",
            kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            contract_id=active.contract_id,
            base_revision=active.revision,
        )
        store.save_planner_request(request)
        engine = _engine(store, game, planner)

        await engine.tick()
        proposal = store.list_strategic_research_proposals("game-1")[0]
        await engine.tick()
        _assert_proposal_wait(store, request, proposal)

        assert store.request_human_resume("game-1") is True
        result = await engine.tick()

        assert (
            result.workflow_tick["outcome"]
            == TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
        )
        assert result.workflow_tick["starting_runtime_state"] == "AWAITING_HUMAN"
        assert result.workflow_tick["ending_runtime_state"] == "ROUTING"
        assert result.workflow_tick["mutation_budget_used"] == 0
        assert store.human_wait_context("game-1") is None
        unchanged = store.get_active_strategic_contract("game-1")
        assert unchanged is not None
        assert unchanged.revision == active.revision
        assert unchanged.authority_scope_set.mission_graph_scopes == ()
        assert unchanged.mission_graph.missions == ()
        assert len(store.list_strategic_research_proposals("game-1")) == 1
        assert store.list_tasks("game-1") == []
        assert planner.calls == 1

    asyncio.run(scenario())


async def _completed_proposal_state(
    path,
    *,
    game_id: str = "game-1",
    planner_request_id: str | None = None,
    repair: bool = False,
):
    store = WorkflowStore(path)
    contract_id = build_strategic_contract_id(game_id)
    if repair:
        active = _commit_contract(store, game_id)
        request = _request(
            game_id,
            kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            contract_id=active.contract_id,
            base_revision=active.revision,
        )
    else:
        request = _request(game_id)
    if planner_request_id is not None:
        request = request.model_copy(update={"planner_request_id": planner_request_id})
    store.save_planner_request(request)
    planner = _Planner(_response(game_id, contract_id))
    result = await _engine(store, _Game(game_id), planner).tick()
    assert result.workflow_tick["outcome"] == TickOutcomeKind.STRATEGIC_PROPOSAL_READY
    completed = store.get_planner_request(request.planner_request_id)
    assert completed is not None
    proposal = store.list_strategic_research_proposals(game_id)[0]
    attempt = store.list_provider_attempts(request.planner_request_id)[-1]
    return store, completed, proposal, attempt


def _rebuild_proposal(proposal, **updates):
    fields = {
        "proposal_id": proposal.proposal_id,
        "game_session_id": proposal.game_session_id,
        "source_planner_request_id": proposal.source_planner_request_id,
        "source_provider_attempt_id": proposal.source_provider_attempt_id,
        "source_provider_attempt_number": proposal.source_provider_attempt_number,
        "target_kind": proposal.target_kind,
        "target_contract_id": proposal.target_contract_id,
        "expected_base_revision": proposal.expected_base_revision,
        "strategic_objectives": proposal.strategic_objectives,
        "global_constraints": proposal.global_constraints,
        "proposed_research_mission": proposal.proposed_research_mission,
        "created_from_observation_id": proposal.created_from_observation_id,
        "created_at": proposal.created_at,
    }
    fields.update(updates)
    return build_strategic_research_proposal(**fields)


def _replace_replay_proposal(state, proposal):
    row = state["tables"]["strategic_research_proposals"][0]
    row.update(
        {
            "proposal_id": proposal.proposal_id,
            "game_id": proposal.game_session_id,
            "source_planner_request_id": proposal.source_planner_request_id,
            "source_provider_attempt_id": proposal.source_provider_attempt_id,
            "source_provider_attempt_number": proposal.source_provider_attempt_number,
            "target_kind": proposal.target_kind.value,
            "target_contract_id": proposal.target_contract_id,
            "expected_base_revision": proposal.expected_base_revision,
            "proposal_hash": proposal.proposal_hash,
            "proposal_json": proposal.model_dump_json(),
            "created_at": proposal.created_at.isoformat(),
        }
    )


@pytest.mark.parametrize(
    "status",
    [
        ProviderAttemptStatus.FAILED,
        ProviderAttemptStatus.STARTED,
        ProviderAttemptStatus.ABANDONED,
    ],
)
def test_completed_proposal_rejects_a_higher_nonfinal_attempt(tmp_path, status):
    store, request, _proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / f"higher-{status.value}.sqlite3")
    )
    started_at = (attempt.completed_at or attempt.started_at) + timedelta(seconds=1)
    candidate = ProviderAttempt(
        provider_attempt_id=f"provider-higher-{status.value.lower()}",
        planner_request_id=request.planner_request_id,
        attempt_number=2,
        provider_request_id=f"request-higher-{status.value.lower()}",
        status=status,
        started_at=started_at,
        completed_at=(
            None
            if status is ProviderAttemptStatus.STARTED
            else started_at + timedelta(seconds=1)
        ),
        failure_category=(
            None
            if status is ProviderAttemptStatus.STARTED
            else "injected_higher_attempt"
        ),
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="start_provider_attempt"):
        store.save_provider_attempt("game-1", candidate)

    assert store.export_replay_state("game-1") == before
    assert store.list_provider_attempts(request.planner_request_id) == [attempt]


def test_public_attempt_and_request_saves_cannot_stage_completed_nonlegacy(
    tmp_path,
):
    _, completed, _proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "completed-source.sqlite3")
    )
    store = WorkflowStore(tmp_path / "completed-target.sqlite3")
    pending = completed.model_copy(
        update={
            "status": PlannerRequestStatus.PENDING,
            "completed_at": None,
            "response_payload": None,
            "response_hash": None,
            "validation_result": None,
            "provider_attempt_count": 0,
            "failure_category": None,
        }
    )
    store.save_planner_request(pending)
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="start_provider_attempt"):
        store.save_provider_attempt("game-1", attempt)
    with pytest.raises(ValueError, match="atomic Runtime transaction"):
        store.save_planner_request(completed)

    assert store.export_replay_state("game-1") == before
    assert store.get_planner_request(pending.planner_request_id) == pending


def test_startup_rejects_completed_nonlegacy_without_proposal(tmp_path):
    path = tmp_path / "missing-proposal-startup.sqlite3"
    store, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(path)
    )
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM strategic_research_proposals")

    with pytest.raises(
        ValueError, match="COMPLETED non-legacy PlannerRequest requires Proposal"
    ):
        WorkflowStore(path)

    assert store.path == path


def test_replay_rejects_completed_nonlegacy_without_proposal_before_delete(tmp_path):
    source, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "missing-proposal-source.sqlite3")
    )
    invalid = source.export_replay_state("game-1")
    invalid["tables"]["strategic_research_proposals"] = []
    target, _target_request, _target_proposal, _target_attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "missing-proposal-target.sqlite3")
    )
    before = target.export_replay_state("game-1")

    with pytest.raises(
        ValueError, match="COMPLETED non-legacy PlannerRequest requires Proposal"
    ):
        target.import_replay_state(invalid)

    assert target.export_replay_state("game-1") == before


@pytest.mark.parametrize(
    "status",
    [PlannerRequestStatus.PENDING, PlannerRequestStatus.REJECTED],
)
def test_public_proposal_save_rejects_noncompleted_parent(tmp_path, status):
    path = tmp_path / f"parent-{status.value.lower()}.sqlite3"
    store, completed, proposal, _attempt = asyncio.run(_completed_proposal_state(path))
    if status is PlannerRequestStatus.PENDING:
        parent = completed.model_copy(
            update={
                "status": status,
                "completed_at": None,
                "response_payload": None,
                "response_hash": None,
                "validation_result": None,
                "failure_category": None,
            }
        )
    else:
        parent = completed.model_copy(
            update={"status": status, "failure_category": "test_rejection"}
        )
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM strategic_research_proposals")
        conn.execute(
            "UPDATE logical_planner_requests SET status=?, request_json=?, "
            "completed_at=? WHERE planner_request_id=?",
            (
                parent.status.value,
                WorkflowStore._dump(parent.model_dump(mode="json")),
                None
                if parent.completed_at is None
                else parent.completed_at.isoformat(),
                parent.planner_request_id,
            ),
        )

    with pytest.raises(
        ValueError,
        match="complete Proposal-ready Tick transaction",
    ):
        store.save_strategic_research_proposal(proposal)

    assert store.list_strategic_research_proposals("game-1") == []


def test_proposal_identity_cannot_be_reused_with_new_content(tmp_path):
    store, _request_record, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "proposal-reuse.sqlite3")
    )
    changed = _rebuild_proposal(
        proposal, strategic_objectives=("Choose a different technology",)
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="identity was reused with new content"):
        store.save_strategic_research_proposal(changed)

    assert store.export_replay_state("game-1") == before


def test_database_unique_constraints_reject_second_proposal_for_request(tmp_path):
    path = tmp_path / "second-proposal.sqlite3"
    store, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(path)
    )
    with sqlite3.connect(path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO strategic_research_proposals "
                "SELECT ?, game_id, source_planner_request_id, "
                "source_provider_attempt_id, source_provider_attempt_number, "
                "target_kind, target_contract_id, expected_base_revision, "
                "proposal_hash, proposal_json, created_at "
                "FROM strategic_research_proposals",
                ("second-proposal-id",),
            )

    assert len(store.list_strategic_research_proposals("game-1")) == 1


@pytest.mark.parametrize("mismatch", ["target_kind", "contract_id", "base_revision"])
def test_replay_rejects_proposal_target_mismatch(tmp_path, mismatch):
    repair = mismatch == "base_revision"
    source, _request_record, proposal, _attempt = asyncio.run(
        _completed_proposal_state(
            tmp_path / f"mismatch-source-{mismatch}.sqlite3", repair=repair
        )
    )
    if mismatch == "target_kind":
        replacement = _rebuild_proposal(
            proposal,
            target_kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            expected_base_revision=1,
        )
    elif mismatch == "contract_id":
        other_contract = "contract-other"
        replacement = _rebuild_proposal(
            proposal,
            target_contract_id=other_contract,
            proposed_research_mission=proposal.proposed_research_mission.model_copy(
                update={"contract_id": other_contract}
            ),
        )
    else:
        replacement = _rebuild_proposal(
            proposal, expected_base_revision=proposal.expected_base_revision + 1
        )
    invalid = source.export_replay_state("game-1")
    _replace_replay_proposal(invalid, replacement)
    target = WorkflowStore(tmp_path / f"mismatch-target-{mismatch}.sqlite3")

    with pytest.raises(ValueError, match="Proposal .* disagrees with PlannerRequest"):
        target.import_replay_state(invalid)

    assert (
        target.export_replay_state("game-1")["tables"]["strategic_research_proposals"]
        == []
    )


def test_final_transaction_failure_rolls_back_terminal_proposal_facts(tmp_path):
    async def scenario():
        path = tmp_path / "atomic-final.sqlite3"
        store = WorkflowStore(path)
        request = _request("game-1")
        store.save_planner_request(request)
        with sqlite3.connect(path) as conn:
            conn.execute(
                "CREATE TRIGGER fail_proposal_ready_tick "
                "BEFORE INSERT ON workflow_ticks "
                "WHEN NEW.outcome = 'STRATEGIC_PROPOSAL_READY' "
                "BEGIN SELECT RAISE(ABORT, 'injected final tick failure'); END"
            )
        planner = _Planner(_response("game-1", build_strategic_contract_id("game-1")))

        result = await _engine(store, _Game(), planner).tick()

        persisted = store.get_planner_request(request.planner_request_id)
        attempts = store.list_provider_attempts(request.planner_request_id)
        assert result.workflow_tick["outcome"] == TickOutcomeKind.SYSTEM_ERROR
        assert persisted.status is PlannerRequestStatus.IN_PROGRESS
        assert len(attempts) == 1
        assert attempts[0].status is ProviderAttemptStatus.STARTED
        assert store.list_strategic_research_proposals("game-1") == []
        assert all(
            tick.outcome != TickOutcomeKind.STRATEGIC_PROPOSAL_READY
            for tick in store.list_workflow_ticks("game-1")
        )

    asyncio.run(scenario())


def test_replay_cross_game_request_identity_conflict_preserves_target(tmp_path):
    source, source_request, _source_proposal, _source_attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "cross-game-source.sqlite3")
    )
    target, _target_request, _target_proposal, _target_attempt = asyncio.run(
        _completed_proposal_state(
            tmp_path / "cross-game-target.sqlite3",
            game_id="game-2",
            planner_request_id=source_request.planner_request_id,
        )
    )
    before = target.export_replay_state("game-2")

    with pytest.raises(ValueError, match="belongs to another game"):
        target.import_replay_state(source.export_replay_state("game-1"))

    assert target.export_replay_state("game-2") == before


def test_v9_upgrade_creates_empty_proposal_table_without_changing_state(tmp_path):
    path = tmp_path / "v9-upgrade.sqlite3"
    store = WorkflowStore(path)
    contract = _commit_contract(store, "game-1")
    before_contract = store.get_active_strategic_contract("game-1")
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE strategic_research_proposals")
        conn.execute("PRAGMA user_version=9")

    upgraded = WorkflowStore(path)

    assert (
        upgraded.get_active_strategic_contract("game-1") == before_contract == contract
    )
    assert upgraded.list_strategic_research_proposals("game-1") == []
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 14


@pytest.mark.parametrize("kind", ["creation", "repair"])
def test_stale_base_terminates_requested_information_round_atomically(tmp_path, kind):
    async def scenario():
        path = tmp_path / f"stale-information-{kind}.sqlite3"
        store = WorkflowStore(path)
        if kind == "repair":
            active = _commit_contract(store, "game-1")
            request = _request(
                "game-1",
                kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
                contract_id=active.contract_id,
                base_revision=active.revision,
            )
        else:
            active = None
            request = _request("game-1")
        planner = _Planner(
            _information_response(),
            _response("game-1", build_strategic_contract_id("game-1")),
        )
        game = _Game()
        store.save_planner_request(request)
        engine = _engine(store, game, planner)

        first = await engine.tick()
        assert first.workflow_tick["outcome"] == TickOutcomeKind.INFORMATION_REQUESTED
        awaiting = store.get_planner_request(request.planner_request_id)
        assert awaiting is not None
        assert awaiting.status is PlannerRequestStatus.AWAITING_INFORMATION
        rounds = store.list_information_rounds(request.planner_request_id)
        assert len(rounds) == 1
        assert rounds[0].status is InformationRoundStatus.REQUESTED

        if active is None:
            _commit_contract(store, "game-1")
        else:
            _commit_contract(store, "game-1", revision=active.revision + 1)
        provider_calls = planner.calls
        query_calls = game.query_count

        result = await engine.tick()

        stored = store.get_planner_request(request.planner_request_id)
        assert stored is not None
        assert stored.status is PlannerRequestStatus.SUPERSEDED
        assert stored.failure_category == "stale_strategic_contract_base"
        assert stored.pending_information_requests == ()
        failed_round = store.list_information_rounds(request.planner_request_id)[0]
        assert failed_round.status is InformationRoundStatus.FAILED
        assert failed_round.completed_at is not None
        assert stored.completed_at is not None
        assert failed_round.completed_at >= stored.completed_at
        assert result.runtime_state == RuntimeState.AWAITING_HUMAN
        assert result.workflow_tick["outcome"] != TickOutcomeKind.SYSTEM_ERROR
        assert planner.calls == provider_calls
        assert game.query_count == query_calls
        assert store.list_strategic_research_proposals("game-1") == []

        exported = store.export_replay_state("game-1")
        restored = WorkflowStore(
            tmp_path / f"stale-information-restored-{kind}.sqlite3"
        )
        restored.import_replay_state(copy.deepcopy(exported))
        assert restored.export_replay_state("game-1") == exported

    asyncio.run(scenario())


def _human_wait_replay_row(state):
    rows = [
        row
        for row in state["tables"]["workflow_meta"]
        if row["key"] == "human_wait:game-1"
    ]
    assert len(rows) == 1
    return rows[0]


def _workflow_tick_replay_row(state, outcome):
    rows = [
        row for row in state["tables"]["workflow_ticks"] if row["outcome"] == outcome
    ]
    assert len(rows) == 1
    return rows[0]


def _change_replay_tick(state, outcome, **updates):
    row = _workflow_tick_replay_row(state, outcome)
    payload = WorkflowStore._load(row["tick_json"])
    payload.update(updates)
    row["tick_json"] = WorkflowStore._dump(payload)
    relational = {
        "game_session_id": "game_id",
        "planner_request_id": "planner_request_id",
        "outcome": "outcome",
        "starting_runtime_state": "starting_runtime_state",
        "ending_runtime_state": "ending_runtime_state",
        "mutation_budget_used": "mutation_budget_used",
        "turn_number": "turn",
    }
    for field, column in relational.items():
        if field in updates:
            value = updates[field]
            row[column] = value.value if hasattr(value, "value") else value


def _assert_invalid_replay_preserves_target(tmp_path, invalid, label, *, match=None):
    target, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / f"invalid-target-{label}.sqlite3")
    )
    before = target.export_replay_state("game-1")
    with pytest.raises(ValueError, match=match):
        target.import_replay_state(invalid)
    assert target.export_replay_state("game-1") == before


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_wait",
        "missing_runtime",
        "missing_ready_tick",
        "wrong_context_proposal",
        "wrong_context_request",
        "generic_context",
        "wrong_resume_policy",
        "string_resume_requested",
        "integer_resume_requested",
        "wrong_ready_target",
        "wrong_ready_base",
    ],
)
def test_replay_rejects_incomplete_unresolved_proposal_lifecycle_before_delete(
    tmp_path, corruption
):
    source, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / f"invalid-source-{corruption}.sqlite3")
    )
    invalid = copy.deepcopy(source.export_replay_state("game-1"))
    tables = invalid["tables"]
    if corruption == "missing_wait":
        tables["workflow_meta"] = [
            row for row in tables["workflow_meta"] if row["key"] != "human_wait:game-1"
        ]
    elif corruption == "missing_runtime":
        tables["runtime_state"] = []
    elif corruption == "missing_ready_tick":
        tables["workflow_ticks"] = [
            row
            for row in tables["workflow_ticks"]
            if row["outcome"] != TickOutcomeKind.STRATEGIC_PROPOSAL_READY
        ]
    elif corruption in {
        "wrong_context_proposal",
        "wrong_context_request",
        "generic_context",
        "wrong_resume_policy",
        "string_resume_requested",
        "integer_resume_requested",
    }:
        row = _human_wait_replay_row(invalid)
        context = WorkflowStore._load(row["value_json"])
        if corruption == "wrong_context_proposal":
            context["proposal_id"] = "proposal-other"
        elif corruption == "wrong_context_request":
            context["planner_request_id"] = "request-other"
        elif corruption == "generic_context":
            context = {"version": "human-wait/v1", "resume_requested": False}
        elif corruption == "wrong_resume_policy":
            context["resume_policy"] = "observation_change"
        elif corruption == "string_resume_requested":
            context["resume_requested"] = "true"
        else:
            context["resume_requested"] = 1
        row["value_json"] = WorkflowStore._dump(context)
    elif corruption == "wrong_ready_target":
        _change_replay_tick(
            invalid,
            TickOutcomeKind.STRATEGIC_PROPOSAL_READY,
            target_kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
        )
    else:
        ready = WorkflowStore._load(
            _workflow_tick_replay_row(
                invalid, TickOutcomeKind.STRATEGIC_PROPOSAL_READY
            )["tick_json"]
        )
        _change_replay_tick(
            invalid,
            TickOutcomeKind.STRATEGIC_PROPOSAL_READY,
            expected_base_revision=ready["expected_base_revision"] + 1,
        )

    _assert_invalid_replay_preserves_target(tmp_path, invalid, corruption)


@pytest.mark.parametrize(
    "corruption", ["missing_wait", "missing_runtime", "missing_ready_tick"]
)
def test_startup_rejects_incomplete_unresolved_proposal_lifecycle(tmp_path, corruption):
    path = tmp_path / f"startup-{corruption}.sqlite3"
    asyncio.run(_completed_proposal_state(path))
    with sqlite3.connect(path) as conn:
        if corruption == "missing_wait":
            conn.execute("DELETE FROM workflow_meta WHERE key='human_wait:game-1'")
        elif corruption == "missing_runtime":
            conn.execute("DELETE FROM runtime_state WHERE game_id='game-1'")
        else:
            conn.execute(
                "DELETE FROM workflow_ticks WHERE game_id='game-1' AND outcome=?",
                (TickOutcomeKind.STRATEGIC_PROPOSAL_READY.value,),
            )

    with pytest.raises(ValueError):
        WorkflowStore(path)


async def _resumed_proposal_state(path):
    store = WorkflowStore(path)
    active = _commit_contract(store, "game-1")
    request = _request(
        "game-1",
        kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
        contract_id=active.contract_id,
        base_revision=active.revision,
    )
    planner = _Planner(_response("game-1", active.contract_id))
    game = _Game()
    store.save_planner_request(request)
    engine = _engine(store, game, planner)
    await engine.tick()
    await engine.tick()
    proposal = store.list_strategic_research_proposals("game-1")[0]
    assert store.request_human_resume("game-1") is True
    resumed = await engine.tick()
    assert (
        resumed.workflow_tick["outcome"]
        == TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
    )
    return store, request, proposal, active, planner, engine


def test_resumed_proposal_lifecycle_survives_restart_and_replay(tmp_path):
    path = tmp_path / "resumed-source.sqlite3"
    store, request, proposal, active, planner, _engine_instance = asyncio.run(
        _resumed_proposal_state(path)
    )

    restarted = WorkflowStore(path)
    assert restarted.human_wait_context("game-1") is None
    assert restarted.get_strategic_research_proposal(proposal.proposal_id) == proposal
    assert (
        restarted.get_planner_request(request.planner_request_id).status
        is PlannerRequestStatus.COMPLETED
    )
    unchanged = restarted.get_active_strategic_contract("game-1")
    assert unchanged == active
    assert unchanged.authority_scope_set.mission_graph_scopes == ()
    assert unchanged.mission_graph.missions == ()
    assert restarted.list_tasks("game-1") == []
    assert planner.calls == 1

    exported = restarted.export_replay_state("game-1")
    restored = WorkflowStore(tmp_path / "resumed-restored.sqlite3")
    restored.import_replay_state(copy.deepcopy(exported))
    assert restored.export_replay_state("game-1") == exported


def test_startup_rejects_resumed_proposal_without_resume_tick(tmp_path):
    path = tmp_path / "resumed-startup-missing-tick.sqlite3"
    asyncio.run(_resumed_proposal_state(path))
    with sqlite3.connect(path) as conn:
        conn.execute(
            "DELETE FROM workflow_ticks WHERE game_id='game-1' AND outcome=?",
            (TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED.value,),
        )

    with pytest.raises(ValueError):
        WorkflowStore(path)


@pytest.mark.parametrize(
    "corruption",
    ["missing", "proposal_id", "planner_request_id", "target_kind", "base_revision"],
)
def test_replay_rejects_missing_or_tampered_proposal_resume_tick(tmp_path, corruption):
    source, _request, _proposal, _active, _planner, _engine_instance = asyncio.run(
        _resumed_proposal_state(
            tmp_path / f"resumed-invalid-source-{corruption}.sqlite3"
        )
    )
    invalid = copy.deepcopy(source.export_replay_state("game-1"))
    if corruption == "missing":
        invalid["tables"]["workflow_ticks"] = [
            row
            for row in invalid["tables"]["workflow_ticks"]
            if row["outcome"] != TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
        ]
    elif corruption == "proposal_id":
        _change_replay_tick(
            invalid,
            TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED,
            proposal_id="proposal-other",
        )
    elif corruption == "planner_request_id":
        _change_replay_tick(
            invalid,
            TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED,
            planner_request_id="request-other",
        )
    elif corruption == "target_kind":
        _change_replay_tick(
            invalid,
            TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED,
            target_kind=PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
        )
    else:
        resumed = WorkflowStore._load(
            _workflow_tick_replay_row(
                invalid, TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
            )["tick_json"]
        )
        _change_replay_tick(
            invalid,
            TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED,
            expected_base_revision=resumed["expected_base_revision"] + 1,
        )

    _assert_invalid_replay_preserves_target(tmp_path, invalid, f"resumed-{corruption}")


def test_resumed_historical_proposal_allows_later_generic_human_wait(tmp_path):
    async def scenario():
        path = tmp_path / "resumed-then-generic.sqlite3"
        (
            store,
            _request,
            proposal,
            active,
            planner,
            engine,
        ) = await _resumed_proposal_state(path)

        later = await engine.tick()

        assert (
            later.workflow_tick["outcome"]
            != TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
        )
        context = store.human_wait_context("game-1")
        if context is not None:
            assert context.get("wait_kind") != "strategic_contract_proposal_ready"
        assert store.get_strategic_research_proposal(proposal.proposal_id) == proposal
        assert store.get_active_strategic_contract("game-1") == active
        assert planner.calls == 1
        WorkflowStore(path)

    asyncio.run(scenario())


def _proposal_ready_tick(store, proposal_id):
    matches = [
        tick
        for tick in store.list_workflow_ticks("game-1")
        if tick.outcome is TickOutcomeKind.STRATEGIC_PROPOSAL_READY
        and tick.proposal_id == proposal_id
    ]
    assert len(matches) == 1
    return matches[0]


def _forged_resume_tick(store, proposal):
    ready = _proposal_ready_tick(store, proposal.proposal_id)
    return StrategicProposalWaitResumedTick(
        tick_id="tick-forged-proposal-resume",
        game_session_id="game-1",
        turn_number=ready.turn_number,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=("obs-forged-resume",),
        started_at=ready.completed_at,
        completed_at=ready.completed_at,
        resume_request_id=build_strategic_proposal_wait_resume_request_id(
            proposal.proposal_id
        ),
        proposal_ready_tick_id=ready.tick_id,
        planner_request_id=proposal.source_planner_request_id,
        proposal_id=proposal.proposal_id,
        target_kind=proposal.target_kind,
        expected_base_revision=proposal.expected_base_revision,
    )


@pytest.mark.parametrize("entrypoint", ["phase4", "tick_and_runtime"])
def test_forged_resume_tick_is_rejected_before_any_write(tmp_path, entrypoint):
    path = tmp_path / f"forged-resume-{entrypoint}.sqlite3"
    store, _request_record, proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    tick = _forged_resume_tick(store, proposal)
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError):
        if entrypoint == "phase4":
            store.persist_phase4_tick(tick, human_wait_context=None)
        else:
            store.persist_tick_and_runtime_state(tick, human_wait_context=None)

    assert store.export_replay_state("game-1") == before
    assert store.load_runtime_state("game-1") is RuntimeState.AWAITING_HUMAN
    assert store.list_strategic_proposal_wait_resume_requests("game-1") == []
    WorkflowStore(path)
    restored = WorkflowStore(tmp_path / f"forged-restored-{entrypoint}.sqlite3")
    restored.import_replay_state(copy.deepcopy(before))
    assert restored.export_replay_state("game-1") == before


def test_resume_request_intermediate_state_is_idempotent_restartable_and_replayable(
    tmp_path,
):
    path = tmp_path / "resume-request-intermediate.sqlite3"
    store, request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    active = store.get_active_strategic_contract("game-1")
    assert active is not None
    ready = _proposal_ready_tick(store, proposal.proposal_id)

    assert store.request_human_resume("game-1") is True
    assert store.request_human_resume("game-1") is True
    requests = store.list_strategic_proposal_wait_resume_requests("game-1")
    assert len(requests) == 1
    resume_request = requests[0]
    context = store.human_wait_context("game-1")
    assert context is not None
    assert context["resume_requested"] is True
    assert context["resume_request_id"] == resume_request.resume_request_id
    assert context["proposal_ready_tick_id"] == ready.tick_id
    assert datetime.fromisoformat(context["resume_requested_at"]) == (
        resume_request.requested_at
    )
    assert store.load_runtime_state("game-1") is RuntimeState.AWAITING_HUMAN

    restarted = WorkflowStore(path)
    exported = restarted.export_replay_state("game-1")
    restored = WorkflowStore(tmp_path / "resume-request-intermediate-restored.sqlite3")
    restored.import_replay_state(copy.deepcopy(exported))
    assert restored.export_replay_state("game-1") == exported

    planner = _Planner()
    result = asyncio.run(_engine(restarted, _Game(), planner).tick())
    assert result.workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
    )
    assert result.workflow_tick["resume_request_id"] == resume_request.resume_request_id
    assert result.workflow_tick["proposal_ready_tick_id"] == ready.tick_id
    assert restarted.human_wait_context("game-1") is None
    assert restarted.get_active_strategic_contract("game-1") == active
    assert restarted.get_planner_request(request.planner_request_id) is not None
    assert restarted.list_tasks("game-1") == []
    assert planner.calls == 0


def test_public_runtime_save_cannot_bypass_unresolved_proposal_wait(tmp_path):
    path = tmp_path / "runtime-bypass.sqlite3"
    store, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    before = store.export_replay_state("game-1")
    with pytest.raises(ValueError):
        store.save_runtime_state("game-1", RuntimeState.ROUTING)
    assert store.export_replay_state("game-1") == before
    WorkflowStore(path)
    restored = WorkflowStore(tmp_path / "runtime-bypass-restored.sqlite3")
    restored.import_replay_state(copy.deepcopy(before))
    assert restored.export_replay_state("game-1") == before


@pytest.mark.parametrize(
    "context",
    [{}, {"proposal_id": "other"}, {"resume_requested": True}],
)
def test_public_meta_api_rejects_human_wait_namespace(tmp_path, context):
    store = WorkflowStore(tmp_path / "meta-boundary.sqlite3")
    with pytest.raises(ValueError, match="dedicated Human Wait APIs"):
        store.set_meta("human_wait:game-1", context)
    assert store.human_wait_context("game-1") is None
    store.set_meta("ordinary-meta", context)
    assert store.get_meta("ordinary-meta") == context


@pytest.mark.parametrize(
    "terminal_status",
    [InformationRoundStatus.FAILED, InformationRoundStatus.COLLECTED],
)
def test_public_strategic_information_round_transition_rolls_back(
    tmp_path, terminal_status
):
    async def setup():
        store = WorkflowStore(tmp_path / f"round-{terminal_status.value}.sqlite3")
        request = _request("game-1")
        store.save_planner_request(request)
        result = await _engine(store, _Game(), _Planner(_information_response())).tick()
        assert result.workflow_tick["outcome"] == TickOutcomeKind.INFORMATION_REQUESTED
        return store, request

    store, request = asyncio.run(setup())
    original = store.list_information_rounds(request.planner_request_id)[0]
    updates = {
        "status": terminal_status,
        "completed_at": datetime.now(UTC),
    }
    if terminal_status is InformationRoundStatus.COLLECTED:
        updates["results"] = {"research": "known"}
    candidate = original.model_copy(update=updates)
    before = store.export_replay_state("game-1")
    with pytest.raises(ValueError):
        store.save_information_round("game-1", candidate)
    assert store.list_information_rounds(request.planner_request_id) == [original]
    assert store.export_replay_state("game-1") == before
    WorkflowStore(store.path)


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_audit",
        "context_false",
        "context_wrong_audit",
        "audit_wrong_ready",
        "audit_wrong_request",
    ],
)
def test_replay_rejects_corrupt_resume_request_state_before_delete(
    tmp_path, corruption
):
    source, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(
            tmp_path / f"resume-corrupt-{corruption}.sqlite3", repair=True
        )
    )
    assert source.request_human_resume("game-1") is True
    invalid = copy.deepcopy(source.export_replay_state("game-1"))
    tables = invalid["tables"]
    if corruption == "missing_audit":
        tables["strategic_proposal_wait_resume_requests"] = []
    elif corruption in {"context_false", "context_wrong_audit"}:
        row = _human_wait_replay_row(invalid)
        context = WorkflowStore._load(row["value_json"])
        if corruption == "context_false":
            context["resume_requested"] = False
        else:
            context["resume_request_id"] = "strategic_resume_request_wrong"
        row["value_json"] = WorkflowStore._dump(context)
    else:
        row = tables["strategic_proposal_wait_resume_requests"][0]
        payload = WorkflowStore._load(row["request_json"])
        field = (
            "proposal_ready_tick_id"
            if corruption == "audit_wrong_ready"
            else "planner_request_id"
        )
        value = "tick-wrong" if field == "proposal_ready_tick_id" else "request-wrong"
        payload[field] = value
        row[field] = value
        row["request_json"] = WorkflowStore._dump(payload)
    _assert_invalid_replay_preserves_target(
        tmp_path, invalid, f"resume-request-{corruption}"
    )


def test_startup_rejects_deleted_resume_request_after_resume_tick(tmp_path):
    path = tmp_path / "deleted-resume-audit.sqlite3"
    asyncio.run(_resumed_proposal_state(path))
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM strategic_proposal_wait_resume_requests")
    with pytest.raises(ValueError):
        WorkflowStore(path)


def _terminal_information_round(request, status):
    fields = {
        "information_round_id": f"forged-round-{status.value.lower()}",
        "planner_request_id": request.planner_request_id,
        "round_number": 1,
        "status": status,
        "requests": ({"query": "research"},),
        "requested_at": NOW,
        "completed_at": NOW + timedelta(seconds=1),
    }
    if status is InformationRoundStatus.COLLECTED:
        fields["results"] = {"research": {"current": "TECH_WRITING"}}
    return InformationRound(**fields)


def _assert_replay_round_trip(tmp_path, store, label):
    exported = store.export_replay_state("game-1")
    WorkflowStore(store.path)
    restored = WorkflowStore(tmp_path / f"{label}-restored.sqlite3")
    restored.import_replay_state(copy.deepcopy(exported))
    assert restored.export_replay_state("game-1") == exported


@pytest.mark.parametrize(
    "status", [InformationRoundStatus.FAILED, InformationRoundStatus.COLLECTED]
)
def test_public_round_save_rejects_forged_terminal_round_for_pending_request(
    tmp_path, status
):
    store = WorkflowStore(tmp_path / f"pending-forged-{status.value}.sqlite3")
    request = _request("game-1")
    store.save_planner_request(request)
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="atomic Runtime transaction"):
        store.save_information_round(
            "game-1", _terminal_information_round(request, status)
        )

    assert store.export_replay_state("game-1") == before
    assert store.list_information_rounds(request.planner_request_id) == []
    _assert_replay_round_trip(tmp_path, store, f"pending-forged-{status.value}")


@pytest.mark.parametrize(
    "status", [InformationRoundStatus.FAILED, InformationRoundStatus.COLLECTED]
)
def test_public_round_save_rejects_forged_round_for_completed_proposal(
    tmp_path, status
):
    store, request, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / f"completed-forged-{status.value}.sqlite3")
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="atomic Runtime transaction"):
        store.save_information_round(
            "game-1", _terminal_information_round(request, status)
        )

    assert store.export_replay_state("game-1") == before
    assert store.list_information_rounds(request.planner_request_id) == []
    _assert_replay_round_trip(tmp_path, store, f"completed-forged-{status.value}")


@pytest.mark.parametrize("save_mode", ["new", "update"])
def test_public_request_save_rejects_forged_ready_to_continue(tmp_path, save_mode):
    store = WorkflowStore(tmp_path / f"forged-ready-{save_mode}.sqlite3")
    pending = _request("game-1")
    if save_mode == "update":
        store.save_planner_request(pending)
        store.save_planner_request(pending)
    forged = pending.model_copy(
        update={
            "status": PlannerRequestStatus.READY_TO_CONTINUE,
            "information_round_count": 1,
            "information_results": {"research": {"current": "TECH_WRITING"}},
        }
    )
    planner = _Planner(_response("game-1", build_strategic_contract_id("game-1")))

    with pytest.raises(ValueError):
        store.save_planner_request(forged)

    assert planner.calls == 0
    if save_mode == "new":
        assert store.get_planner_request(pending.planner_request_id) is None
    else:
        assert store.get_planner_request(pending.planner_request_id) == pending
        _assert_replay_round_trip(tmp_path, store, "forged-ready-update")


def test_startup_and_replay_reject_ready_without_collected_round(tmp_path):
    path = tmp_path / "forged-ready-aggregate.sqlite3"
    store = WorkflowStore(path)
    pending = _request("game-1")
    store.save_planner_request(pending)
    forged = pending.model_copy(
        update={
            "status": PlannerRequestStatus.READY_TO_CONTINUE,
            "information_round_count": 1,
            "information_results": {"research": {"current": "TECH_WRITING"}},
        }
    )
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    row = invalid["tables"]["logical_planner_requests"][0]
    row["status"] = forged.status.value
    row["request_json"] = WorkflowStore._dump(forged.model_dump(mode="json"))

    _assert_invalid_replay_preserves_target(tmp_path, invalid, "forged-ready-aggregate")

    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE logical_planner_requests
            SET status=?, request_json=?
            WHERE planner_request_id=?
            """,
            (
                forged.status.value,
                WorkflowStore._dump(forged.model_dump(mode="json")),
                forged.planner_request_id,
            ),
        )
    with pytest.raises(ValueError, match="information_round_count"):
        WorkflowStore(path)


def _start_strategic_attempt(store, request, *, suffix="1"):
    store.save_planner_request(request)
    started = ProviderAttempt(
        provider_attempt_id=f"provider-strategic-boundary-{suffix}",
        planner_request_id=request.planner_request_id,
        attempt_number=1,
        provider_request_id=f"provider-call-{suffix}",
        status=ProviderAttemptStatus.STARTED,
        started_at=NOW,
    )
    in_progress = store.start_provider_attempt("game-1", request, started)
    return in_progress, started


def test_public_strategic_provider_attempt_cannot_commit_success(tmp_path):
    path = tmp_path / "public-strategic-success.sqlite3"
    store = WorkflowStore(path)
    request = _request("game-1")
    in_progress, started = _start_strategic_attempt(store, request)
    succeeded = started.model_copy(
        update={
            "status": ProviderAttemptStatus.SUCCEEDED,
            "completed_at": NOW + timedelta(seconds=1),
            "latency_seconds": 1,
        }
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="atomic Runtime transaction"):
        store.save_provider_attempt("game-1", succeeded)

    assert store.export_replay_state("game-1") == before
    assert store.get_planner_request(request.planner_request_id) == in_progress
    assert store.list_provider_attempts(request.planner_request_id) == [started]
    _assert_replay_round_trip(tmp_path, store, "public-strategic-success")


def test_public_strategic_provider_attempt_cannot_create_terminal_row(tmp_path):
    store = WorkflowStore(tmp_path / "public-strategic-terminal.sqlite3")
    request = _request("game-1")
    store.save_planner_request(request)
    succeeded = ProviderAttempt(
        provider_attempt_id="provider-terminal-without-start",
        planner_request_id=request.planner_request_id,
        attempt_number=1,
        provider_request_id="provider-call-without-start",
        status=ProviderAttemptStatus.SUCCEEDED,
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=1),
        latency_seconds=1,
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="start_provider_attempt"):
        store.save_provider_attempt("game-1", succeeded)

    assert store.export_replay_state("game-1") == before
    assert store.list_provider_attempts(request.planner_request_id) == []


def test_startup_and_replay_reject_orphan_strategic_success(tmp_path):
    path = tmp_path / "orphan-strategic-success.sqlite3"
    store = WorkflowStore(path)
    request = _request("game-1")
    _in_progress, started = _start_strategic_attempt(store, request)
    succeeded = started.model_copy(
        update={
            "status": ProviderAttemptStatus.SUCCEEDED,
            "completed_at": NOW + timedelta(seconds=1),
            "latency_seconds": 1,
        }
    )
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    replay_row = invalid["tables"]["provider_attempts"][0]
    replay_row["status"] = succeeded.status.value
    replay_row["attempt_json"] = succeeded.model_dump_json()
    replay_row["completed_at"] = succeeded.completed_at.isoformat()

    _assert_invalid_replay_preserves_target(
        tmp_path, invalid, "orphan-strategic-success"
    )

    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE provider_attempts
            SET status=?, attempt_json=?, completed_at=?
            WHERE provider_attempt_id=?
            """,
            (
                succeeded.status.value,
                succeeded.model_dump_json(),
                succeeded.completed_at.isoformat(),
                succeeded.provider_attempt_id,
            ),
        )
    with pytest.raises(ValueError, match="SUCCEEDED ProviderAttempt"):
        WorkflowStore(path)


def _forged_information_requested_aggregate(
    request,
    *,
    source_attempt_id,
    source_attempt_number,
):
    pending = tuple(
        item.model_dump(mode="json")
        for item in _information_response().information_requests
    )
    awaiting = request.model_copy(
        update={
            "status": PlannerRequestStatus.AWAITING_INFORMATION,
            "pending_information_requests": pending,
        }
    )
    round_record = InformationRound(
        information_round_id="forged-information-round",
        planner_request_id=request.planner_request_id,
        round_number=1,
        source_provider_attempt_id=source_attempt_id,
        source_provider_attempt_number=source_attempt_number,
        status=InformationRoundStatus.REQUESTED,
        requests=pending,
        requested_at=NOW + timedelta(seconds=3),
    )
    tick = InformationRequestedTick(
        tick_id="tick-forged-information-requested",
        game_session_id="game-1",
        turn_number=1,
        starting_runtime_state=RuntimeState.REQUESTING_PLAN,
        observation_ids=("obs-forged-information",),
        started_at=NOW + timedelta(seconds=2),
        completed_at=NOW + timedelta(seconds=4),
        planner_request_id=request.planner_request_id,
        information_round_id=round_record.information_round_id,
    )
    return awaiting, round_record, tick


@pytest.mark.parametrize(
    "source_state",
    ["missing", "started", "failed", "nonmax"],
)
def test_forged_information_requested_aggregate_is_rejected(tmp_path, source_state):
    store = WorkflowStore(
        tmp_path / f"forged-information-source-{source_state}.sqlite3"
    )
    request = _request("game-1")
    provider_attempts = ()
    if source_state == "missing":
        store.save_planner_request(request)
        current = request
        source_id = "provider-missing"
        source_number = 1
    else:
        current, started = _start_strategic_attempt(store, request, suffix=source_state)
        source_id = started.provider_attempt_id
        source_number = started.attempt_number
        if source_state == "started":
            provider_attempts = (started,)
        else:
            failed = started.model_copy(
                update={
                    "status": ProviderAttemptStatus.FAILED,
                    "completed_at": NOW + timedelta(seconds=1),
                    "latency_seconds": 1,
                    "failure_category": "injected_provider_failure",
                }
            )
            store.save_provider_attempt("game-1", failed)
            provider_attempts = (failed,)
            if source_state == "nonmax":
                second = ProviderAttempt(
                    provider_attempt_id="provider-strategic-boundary-second",
                    planner_request_id=request.planner_request_id,
                    attempt_number=2,
                    provider_request_id="provider-call-second",
                    status=ProviderAttemptStatus.STARTED,
                    started_at=NOW + timedelta(seconds=2),
                )
                current = store.start_provider_attempt("game-1", current, second)
    awaiting, round_record, tick = _forged_information_requested_aggregate(
        current,
        source_attempt_id=source_id,
        source_attempt_number=source_number,
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError):
        store.persist_phase4_tick(
            tick,
            planner_request=awaiting,
            provider_attempts=provider_attempts,
            information_round=round_record,
        )

    assert store.export_replay_state("game-1") == before
    assert store.list_information_rounds(request.planner_request_id) == []
    _assert_replay_round_trip(
        tmp_path, store, f"forged-information-source-{source_state}"
    )


def _move_collected_tick_before_requested(state):
    requested_row = _workflow_tick_replay_row(
        state, TickOutcomeKind.INFORMATION_REQUESTED
    )
    collected_row = _workflow_tick_replay_row(
        state, TickOutcomeKind.INFORMATION_COLLECTED
    )
    requested_tick = WorkflowStore._load(requested_row["tick_json"])
    bad_started_at = datetime.fromisoformat(requested_tick["started_at"]) - timedelta(
        seconds=2
    )
    bad_completed_at = bad_started_at + timedelta(seconds=1)
    collected_tick = WorkflowStore._load(collected_row["tick_json"])
    collected_tick["started_at"] = bad_started_at.isoformat()
    collected_tick["completed_at"] = bad_completed_at.isoformat()
    collected_row["started_at"] = bad_started_at.isoformat()
    collected_row["completed_at"] = bad_completed_at.isoformat()
    collected_row["tick_json"] = WorkflowStore._dump(collected_tick)


def test_startup_and_replay_reject_information_collected_before_requested(
    tmp_path,
):
    async def setup():
        path = tmp_path / "collected-before-requested.sqlite3"
        store = WorkflowStore(path)
        request = _request("game-1")
        store.save_planner_request(request)
        engine = _engine(
            store,
            _Game(),
            _Planner(
                _information_response(),
                _response("game-1", build_strategic_contract_id("game-1")),
            ),
        )
        await engine.tick()
        await engine.tick()
        assert (
            store.get_planner_request(request.planner_request_id).status
            is PlannerRequestStatus.READY_TO_CONTINUE
        )
        return path, store

    path, store = asyncio.run(setup())
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    _move_collected_tick_before_requested(invalid)
    _assert_invalid_replay_preserves_target(
        tmp_path, invalid, "collected-before-requested"
    )

    corrupted_row = _workflow_tick_replay_row(
        invalid, TickOutcomeKind.INFORMATION_COLLECTED
    )
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE workflow_ticks
            SET started_at=?, completed_at=?, tick_json=?
            WHERE tick_id=?
            """,
            (
                corrupted_row["started_at"],
                corrupted_row["completed_at"],
                corrupted_row["tick_json"],
                corrupted_row["tick_id"],
            ),
        )
    with pytest.raises(ValueError, match="collected before"):
        WorkflowStore(path)


def _start_tick_thread(engine):
    result = {}

    def run():
        try:
            result["value"] = asyncio.run(engine.tick())
        except BaseException as exc:
            result["error"] = exc

    thread = threading.Thread(target=run)
    thread.start()
    return thread, result


def test_resume_committed_after_tick_start_before_wait_read_is_consumed(tmp_path):
    path = tmp_path / "resume-before-wait-read.sqlite3"
    store, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    game = _Game()
    planner = _Planner()
    engine = _engine(store, game, planner)
    read_started = threading.Event()
    allow_read = threading.Event()
    original_read = game.read_snapshot

    async def blocked_read(*, include_units=False):
        read_started.set()
        completed = await asyncio.to_thread(allow_read.wait, 5)
        if not completed:
            raise TimeoutError("test did not release snapshot read")
        return await original_read(include_units=include_units)

    game.read_snapshot = blocked_read
    thread, result = _start_tick_thread(engine)
    assert read_started.wait(5)
    assert store.request_human_resume("game-1") is True
    resume_request = store.list_strategic_proposal_wait_resume_requests("game-1")[0]
    allow_read.set()
    thread.join(10)

    assert not thread.is_alive()
    assert "error" not in result
    tick_result = result["value"]
    assert tick_result.workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
    )
    assert (
        datetime.fromisoformat(tick_result.workflow_tick["started_at"])
        <= resume_request.requested_at
        <= datetime.fromisoformat(tick_result.workflow_tick["completed_at"])
    )
    assert store.human_wait_context("game-1") is None
    assert planner.calls == 0


def test_resume_committed_after_wait_read_before_persist_is_deferred(tmp_path):
    path = tmp_path / "resume-before-wait-persist.sqlite3"
    store, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    planner = _Planner()
    engine = _engine(store, _Game(), planner)
    persist_started = threading.Event()
    allow_persist = threading.Event()
    original_persist = store.persist_tick_and_runtime_state

    def blocked_persist(*args, **kwargs):
        persist_started.set()
        if not allow_persist.wait(5):
            raise TimeoutError("test did not release Tick persistence")
        return original_persist(*args, **kwargs)

    store.persist_tick_and_runtime_state = blocked_persist
    thread, result = _start_tick_thread(engine)
    assert persist_started.wait(5)
    assert store.request_human_resume("game-1") is True
    allow_persist.set()
    thread.join(10)
    store.persist_tick_and_runtime_state = original_persist

    assert not thread.is_alive()
    assert "error" not in result
    assert result["value"].workflow_tick["outcome"] == (TickOutcomeKind.AWAITING_HUMAN)
    context = store.human_wait_context("game-1")
    assert context is not None
    assert context["wait_kind"] == "strategic_contract_proposal_ready"
    assert context["resume_policy"] == "explicit_only"
    assert context["resume_requested"] is True
    assert len(store.list_strategic_proposal_wait_resume_requests("game-1")) == 1

    resumed = asyncio.run(engine.tick())
    assert resumed.workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
    )
    assert store.human_wait_context("game-1") is None
    assert planner.calls == 0


@pytest.mark.parametrize(
    ("status", "updates"),
    [
        (
            PlannerRequestStatus.IN_PROGRESS,
            {
                "status": PlannerRequestStatus.IN_PROGRESS,
                "provider_attempt_count": 0,
            },
        ),
        (
            PlannerRequestStatus.BACKOFF,
            {
                "status": PlannerRequestStatus.BACKOFF,
                "failure_category": "transient_provider_failure",
                "next_retry_at": NOW + timedelta(minutes=5),
            },
        ),
        (
            PlannerRequestStatus.FAILED,
            {
                "status": PlannerRequestStatus.FAILED,
                "failure_category": "planner_failure",
                "completed_at": NOW + timedelta(seconds=1),
            },
        ),
    ],
)
def test_phase4_rejects_forged_strategic_request_transition(tmp_path, status, updates):
    path = tmp_path / f"forged-request-transition-{status.value}.sqlite3"
    store = WorkflowStore(path)
    request = _request("game-1")
    store.save_planner_request(request)
    forged = request.model_copy(update=updates)
    tick = PlannerAttemptCompletedTick(
        tick_id=f"tick-forged-{status.value.lower()}",
        game_session_id="game-1",
        turn_number=1,
        starting_runtime_state=RuntimeState.REQUESTING_PLAN,
        observation_ids=("obs-forged-request-transition",),
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=2),
        planner_request_id=request.planner_request_id,
        provider_attempt_id="provider-does-not-exist",
        provider_attempt_count=0,
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError):
        store.persist_phase4_tick(tick, planner_request=forged)

    assert store.export_replay_state("game-1") == before
    assert store.get_planner_request(request.planner_request_id) == request
    assert store.list_provider_attempts(request.planner_request_id) == []
    WorkflowStore(path)
    _assert_replay_round_trip(
        tmp_path, store, f"forged-request-transition-{status.value}"
    )


def test_startup_and_replay_reject_backoff_without_attempt_or_tick(tmp_path):
    path = tmp_path / "forged-backoff-without-evidence.sqlite3"
    store = WorkflowStore(path)
    request = _request("game-1")
    store.save_planner_request(request)
    forged = request.model_copy(
        update={
            "status": PlannerRequestStatus.BACKOFF,
            "failure_category": "transient_provider_failure",
            "next_retry_at": NOW + timedelta(minutes=5),
        }
    )
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    row = invalid["tables"]["logical_planner_requests"][0]
    row["status"] = forged.status.value
    row["request_json"] = WorkflowStore._dump(forged.model_dump(mode="json"))
    _assert_invalid_replay_preserves_target(
        tmp_path, invalid, "backoff-without-evidence"
    )

    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE logical_planner_requests
            SET status=?, request_json=?
            WHERE planner_request_id=?
            """,
            (
                forged.status.value,
                WorkflowStore._dump(forged.model_dump(mode="json")),
                forged.planner_request_id,
            ),
        )
    with pytest.raises(ValueError, match="BACKOFF"):
        WorkflowStore(path)


def test_superseded_requires_a_real_stale_contract_base(tmp_path):
    path = tmp_path / "forged-superseded-with-current-base.sqlite3"
    store = WorkflowStore(path)
    request = _request("game-1")
    store.save_planner_request(request)
    forged = request.model_copy(
        update={
            "status": PlannerRequestStatus.SUPERSEDED,
            "failure_category": "stale_strategic_contract_base",
            "completed_at": NOW + timedelta(seconds=1),
        }
    )
    tick = StrategicRequestTerminatedTick(
        tick_id="tick-forged-superseded",
        game_session_id="game-1",
        turn_number=1,
        starting_runtime_state=RuntimeState.REQUESTING_PLAN,
        observation_ids=("obs-forged-superseded",),
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=2),
        planner_request_id=request.planner_request_id,
        terminal_status=PlannerRequestStatus.SUPERSEDED,
        failure_category="stale_strategic_contract_base",
        blocking_reason="forged stale base",
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="SUPERSEDED"):
        store.persist_phase4_tick(tick, planner_request=forged)

    assert store.export_replay_state("game-1") == before
    invalid = copy.deepcopy(before)
    row = invalid["tables"]["logical_planner_requests"][0]
    row["status"] = forged.status.value
    row["completed_at"] = forged.completed_at.isoformat()
    row["request_json"] = WorkflowStore._dump(forged.model_dump(mode="json"))
    _assert_invalid_replay_preserves_target(
        tmp_path, invalid, "superseded-with-current-base"
    )


class _RetryThenTransientFailurePlanner:
    def __init__(self):
        self.hook = None
        self.calls = 0
        self.last_diagnostics = {"attempt_count": 2, "backend": "test"}

    def set_provider_attempt_hook(self, hook):
        self.hook = hook
        return True

    async def plan(self, request):
        self.calls += 1
        await self.hook("started", {"provider_request_id": f"{request.request_id}:1"})
        await self.hook("failed", {"failure_category": "retry-1"})
        await self.hook("started", {"provider_request_id": f"{request.request_id}:2"})
        raise TimeoutError("transport failed after retry")


def test_strategic_backoff_derives_failure_count_from_attempt_history(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "strategic-backoff-retry.sqlite3")
        request = _request("game-1")
        store.save_planner_request(request)
        planner = _RetryThenTransientFailurePlanner()
        engine = _engine(store, _Game(), planner)

        result = await engine.tick()

        stored = store.get_planner_request(request.planner_request_id)
        attempts = store.list_provider_attempts(request.planner_request_id)
        assert stored.status is PlannerRequestStatus.BACKOFF
        assert stored.provider_attempt_count == 2
        assert [item.status for item in attempts] == [
            ProviderAttemptStatus.FAILED,
            ProviderAttemptStatus.FAILED,
        ]
        assert result.workflow_tick["provider_attempt_id"] == (
            attempts[-1].provider_attempt_id
        )
        assert result.workflow_tick["provider_attempt_count"] == 2
        assert (
            engine.strategic_workflow.planner_lifecycle._active_backoff(stored)[
                "failure_count"
            ]
            == 2
        )
        assert stored.next_retry_at >= attempts[-1].completed_at + timedelta(seconds=9)
        _assert_replay_round_trip(tmp_path, store, "strategic-backoff-retry")

    asyncio.run(scenario())


def test_strategic_backoff_is_atomic_durable_and_replay_stable(tmp_path):
    async def scenario():
        source_path = tmp_path / "strategic-backoff-source.sqlite3"
        store = WorkflowStore(source_path)
        request = _request("game-1")
        store.save_planner_request(request)
        planner = _Planner(TimeoutError("transport failed"))
        result = await _engine(store, _Game(), planner).tick()

        assert result.workflow_tick["outcome"] == (
            TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED
        )
        stored = store.get_planner_request(request.planner_request_id)
        assert stored.status is PlannerRequestStatus.BACKOFF
        assert stored.next_retry_at is not None
        assert stored.next_retry_at.utcoffset() is not None
        attempts = store.list_provider_attempts(request.planner_request_id)
        assert len(attempts) == 1
        assert attempts[0].status is ProviderAttemptStatus.FAILED
        assert result.workflow_tick["provider_attempt_id"] == (
            attempts[0].provider_attempt_id
        )
        assert store.get_meta("planner_provider_backoff") is None
        assert store.get_meta("planner_transient_failure_count") is None
        early_attempt = ProviderAttempt(
            provider_attempt_id="provider-before-retry-deadline",
            planner_request_id=request.planner_request_id,
            attempt_number=2,
            provider_request_id="provider-before-retry-deadline",
            status=ProviderAttemptStatus.STARTED,
            started_at=stored.next_retry_at - timedelta(microseconds=1),
        )
        before_early_retry = store.export_replay_state("game-1")
        with pytest.raises(ValueError, match="before next_retry_at"):
            store.start_provider_attempt("game-1", stored, early_attempt)
        assert store.export_replay_state("game-1") == before_early_retry
        WorkflowStore(source_path)

        exported = store.export_replay_state("game-1")
        restored = WorkflowStore(tmp_path / "strategic-backoff-restored.sqlite3")
        restored.set_meta(
            "planner_provider_backoff",
            {
                "category": "target-residue",
                "until_epoch": 4102444800,
            },
        )
        restored.set_meta("planner_transient_failure_count", 99)
        restored.import_replay_state(copy.deepcopy(exported))
        restored_request = restored.get_planner_request(request.planner_request_id)
        assert restored_request == stored
        assert restored.export_replay_state("game-1") == exported

        waiting_planner = _Planner()
        waiting_engine = _engine(restored, _Game(), waiting_planner)
        waiting_engine._now = lambda: stored.next_retry_at - timedelta(seconds=1)
        backoff = waiting_engine.strategic_workflow.planner_lifecycle._active_backoff(
            restored_request
        )
        assert backoff["until"] == stored.next_retry_at.isoformat()
        waiting = await waiting_engine.tick()
        assert waiting.workflow_tick["outcome"] == TickOutcomeKind.PLANNER_BACKOFF
        assert waiting_planner.calls == 0

    asyncio.run(scenario())


def _install_proposal_wait_preflight_failure(engine, game, monkeypatch, failure):
    if failure == "read_snapshot":

        async def fail_read_snapshot(*, include_units=False):
            raise TimeoutError("snapshot unavailable")

        game.read_snapshot = fail_read_snapshot
    elif failure == "normalization":

        def fail_normalization(*args, **kwargs):
            raise ValueError("normalization failed")

        monkeypatch.setattr(engine, "_normalize_snapshot", fail_normalization)
    else:

        async def fail_list_tools():
            raise ConnectionError("tool surface unavailable")

        game.list_tools = fail_list_tools


@pytest.mark.parametrize("failure", ["read_snapshot", "normalization", "list_tools"])
def test_proposal_wait_preflight_failure_is_durable_and_preserves_wait(
    tmp_path, monkeypatch, failure
):
    path = tmp_path / f"proposal-wait-error-{failure}.sqlite3"
    store, request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    original_wait = store.human_wait_context("game-1")
    game = _Game()
    planner = _Planner()
    engine = _engine(store, game, planner)
    _install_proposal_wait_preflight_failure(engine, game, monkeypatch, failure)

    result = asyncio.run(engine.tick())

    assert result.workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_ERROR
    )
    assert result.runtime_state == RuntimeState.AWAITING_HUMAN
    assert result.paused is True
    updated_wait = store.human_wait_context("game-1")
    assert updated_wait["blocking_reason"] == (
        "workflow Tick failed while Proposal wait remains active"
    )
    assert {
        key: value for key, value in updated_wait.items() if key != "blocking_reason"
    } == {
        key: value for key, value in original_wait.items() if key != "blocking_reason"
    }
    assert store.get_strategic_research_proposal(proposal.proposal_id) == proposal
    assert store.get_planner_request(request.planner_request_id) == request
    assert planner.calls == 0
    WorkflowStore(path)
    _assert_replay_round_trip(tmp_path, store, f"proposal-wait-error-{failure}")


def test_requested_resume_survives_wait_error_restart_and_replay(tmp_path, monkeypatch):
    path = tmp_path / "proposal-wait-requested-error.sqlite3"
    store, request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    assert store.request_human_resume("game-1") is True
    game = _Game()
    planner = _Planner()
    engine = _engine(store, game, planner)
    _install_proposal_wait_preflight_failure(engine, game, monkeypatch, "read_snapshot")

    failed = asyncio.run(engine.tick())

    assert failed.workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_ERROR
    )
    context = store.human_wait_context("game-1")
    assert context["resume_requested"] is True
    assert context["proposal_id"] == proposal.proposal_id
    restarted = WorkflowStore(path)
    assert restarted.human_wait_context("game-1") == context

    exported = restarted.export_replay_state("game-1")
    restored = WorkflowStore(tmp_path / "proposal-wait-requested-restored.sqlite3")
    restored.import_replay_state(copy.deepcopy(exported))
    resume_planner = _Planner()
    resumed = asyncio.run(_engine(restored, _Game(), resume_planner).tick())

    assert resumed.workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
    )
    assert restored.human_wait_context("game-1") is None
    assert restored.get_strategic_research_proposal(proposal.proposal_id) == proposal
    assert restored.get_planner_request(request.planner_request_id) == request
    assert restored.get_active_strategic_contract("game-1") is not None
    assert restored.list_tasks("game-1") == []
    assert resume_planner.calls == 0


@pytest.mark.parametrize("kind", ["creation", "repair"])
def test_stale_contract_base_supersedes_strategic_backoff(tmp_path, kind):
    async def scenario():
        path = tmp_path / f"strategic-backoff-stale-{kind}.sqlite3"
        store = WorkflowStore(path)
        if kind == "creation":
            request = _request("game-1")
        else:
            active = _commit_contract(store, "game-1")
            request = _request(
                "game-1",
                kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
                contract_id=active.contract_id,
                base_revision=active.revision,
            )
        planner = _Planner(TimeoutError("transport failed"))
        store.save_planner_request(request)
        engine = _engine(store, _Game(), planner)

        await engine.tick()

        backoff = store.get_planner_request(request.planner_request_id)
        assert backoff.status is PlannerRequestStatus.BACKOFF
        assert backoff.next_retry_at is not None
        if kind == "creation":
            _commit_contract(store, "game-1")
        else:
            _commit_contract(store, "game-1", revision=2)

        result = await engine.tick()

        stored = store.get_planner_request(request.planner_request_id)
        assert result.workflow_tick["outcome"] == (
            TickOutcomeKind.STRATEGIC_REQUEST_TERMINATED
        )
        assert result.runtime_state == RuntimeState.AWAITING_HUMAN.value
        assert stored.status is PlannerRequestStatus.SUPERSEDED
        assert stored.failure_category == "stale_strategic_contract_base"
        assert stored.next_retry_at is None
        assert planner.calls == 1
        WorkflowStore(path)
        _assert_replay_round_trip(tmp_path, store, f"strategic-backoff-stale-{kind}")

    asyncio.run(scenario())


def test_creation_with_explicit_contract_id_uses_that_identity_for_staleness(
    tmp_path,
):
    custom_contract_id = "contract-custom"
    request = _request("game-1", contract_id=custom_contract_id)
    forged = request.model_copy(
        update={
            "status": PlannerRequestStatus.SUPERSEDED,
            "failure_category": "stale_strategic_contract_base",
            "completed_at": NOW + timedelta(minutes=3),
        }
    )
    tick = StrategicRequestTerminatedTick(
        tick_id="tick-explicit-contract-superseded",
        game_session_id="game-1",
        turn_number=1,
        starting_runtime_state=RuntimeState.REQUESTING_PLAN,
        observation_ids=("obs-explicit-contract-superseded",),
        started_at=NOW + timedelta(minutes=3),
        completed_at=NOW + timedelta(minutes=3, seconds=1),
        planner_request_id=request.planner_request_id,
        terminal_status=PlannerRequestStatus.SUPERSEDED,
        failure_category="stale_strategic_contract_base",
        blocking_reason="explicit Contract base became stale",
    )

    invalid_path = tmp_path / "explicit-contract-not-stale.sqlite3"
    invalid_store = WorkflowStore(invalid_path)
    invalid_store.save_planner_request(request)
    before = invalid_store.export_replay_state("game-1")
    with pytest.raises(ValueError, match="SUPERSEDED"):
        invalid_store.persist_phase4_tick(tick, planner_request=forged)
    assert invalid_store.export_replay_state("game-1") == before

    invalid_replay = copy.deepcopy(before)
    row = invalid_replay["tables"]["logical_planner_requests"][0]
    row["status"] = forged.status.value
    row["completed_at"] = forged.completed_at.isoformat()
    row["request_json"] = WorkflowStore._dump(forged.model_dump(mode="json"))
    _assert_invalid_replay_preserves_target(
        tmp_path, invalid_replay, "explicit-contract-not-stale"
    )

    with sqlite3.connect(invalid_path) as conn:
        conn.execute(
            """
            UPDATE logical_planner_requests
            SET status=?, completed_at=?, request_json=?
            WHERE planner_request_id=?
            """,
            (
                forged.status.value,
                forged.completed_at.isoformat(),
                WorkflowStore._dump(forged.model_dump(mode="json")),
                forged.planner_request_id,
            ),
        )
    with pytest.raises(ValueError, match="SUPERSEDED"):
        WorkflowStore(invalid_path)

    valid_path = tmp_path / "explicit-contract-now-stale.sqlite3"
    valid_store = WorkflowStore(valid_path)
    valid_store.save_planner_request(request)
    _commit_contract(valid_store, "game-1")
    game = _Game()
    context = _engine(valid_store, game, _Planner())._human_wait_context(game.snapshot)
    context.update(
        {
            "wait_kind": "strategic_request_terminated",
            "resume_policy": "explicit_only",
            "planner_request_id": request.planner_request_id,
            "terminal_tick_id": tick.tick_id,
            "terminal_status": PlannerRequestStatus.SUPERSEDED.value,
            "failure_category": "stale_strategic_contract_base",
            "blocking_reason": tick.blocking_reason,
        }
    )
    valid_store.persist_phase4_tick(
        tick, planner_request=forged, human_wait_context=context
    )
    assert (
        valid_store.get_planner_request(request.planner_request_id).status
        is PlannerRequestStatus.SUPERSEDED
    )
    WorkflowStore(valid_path)
    _assert_replay_round_trip(tmp_path, valid_store, "explicit-contract-now-stale")


def test_wait_error_tick_cannot_be_appended_after_proposal_resume(tmp_path):
    async def scenario():
        path = tmp_path / "proposal-wait-error-after-resume.sqlite3"
        store, request, proposal, _attempt = await _completed_proposal_state(
            path, repair=True
        )
        ready = _proposal_ready_tick(store, proposal.proposal_id)
        error_tick = StrategicProposalWaitErrorTick(
            tick_id="tick-proposal-wait-error-before-resume",
            game_session_id="game-1",
            turn_number=ready.turn_number,
            starting_runtime_state=RuntimeState.AWAITING_HUMAN,
            observation_ids=("obs-wait-error-before-resume",),
            started_at=ready.completed_at + timedelta(seconds=1),
            completed_at=ready.completed_at + timedelta(seconds=2),
            blocking_reason="Proposal wait preflight failed",
            error_category="TimeoutError",
            diagnostic_summary="snapshot unavailable",
            proposal_ready_tick_id=ready.tick_id,
            planner_request_id=request.planner_request_id,
            proposal_id=proposal.proposal_id,
            target_kind=proposal.target_kind,
            expected_base_revision=proposal.expected_base_revision,
        )
        context = store.human_wait_context("game-1")
        context["blocking_reason"] = error_tick.blocking_reason
        store.persist_phase4_tick(error_tick, human_wait_context=context)
        assert store.request_human_resume("game-1") is True
        planner = _Planner()
        resume_engine = _engine(store, _Game(), planner)
        resume_engine._now = lambda: error_tick.completed_at + timedelta(seconds=1)
        resumed_result = await resume_engine.tick()
        assert resumed_result.workflow_tick["outcome"] == (
            TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
        )
        resumed = next(
            tick
            for tick in store.list_workflow_ticks("game-1")
            if tick.outcome is TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
        )
        forged = error_tick.model_copy(
            update={
                "tick_id": "tick-proposal-wait-error-after-resume",
                "started_at": resumed.completed_at + timedelta(seconds=1),
                "completed_at": resumed.completed_at + timedelta(seconds=2),
            }
        )
        before = store.export_replay_state("game-1")
        with pytest.raises(ValueError, match="persisted atomically"):
            store.save_workflow_tick(forged)
        assert store.export_replay_state("game-1") == before
        assert planner.calls == 0
        return path, store, resumed

    path, store, resumed = asyncio.run(scenario())
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    error_row = _workflow_tick_replay_row(
        invalid, TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_ERROR
    )
    error_payload = WorkflowStore._load(error_row["tick_json"])
    error_started_at = resumed.completed_at + timedelta(seconds=1)
    error_completed_at = resumed.completed_at + timedelta(seconds=2)
    error_payload["started_at"] = error_started_at.isoformat()
    error_payload["completed_at"] = error_completed_at.isoformat()
    error_row["started_at"] = error_started_at.isoformat()
    error_row["completed_at"] = error_completed_at.isoformat()
    error_row["tick_json"] = WorkflowStore._dump(error_payload)
    _assert_invalid_replay_preserves_target(
        tmp_path, invalid, "wait-error-after-resume"
    )

    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE workflow_ticks
            SET started_at=?, completed_at=?, tick_json=?
            WHERE tick_id=?
            """,
            (
                error_row["started_at"],
                error_row["completed_at"],
                error_row["tick_json"],
                error_row["tick_id"],
            ),
        )
    with pytest.raises(ValueError, match="after Proposal wait resumed"):
        WorkflowStore(path)


async def _terminal_strategic_request_state(path, status):
    store = WorkflowStore(path)
    request = _request("game-1")
    if status is PlannerRequestStatus.SUPERSEDED:
        _commit_contract(store, "game-1")
        planner = _Planner()
    elif status is PlannerRequestStatus.REJECTED:
        planner = _Planner(
            _response(
                "game-1",
                build_strategic_contract_id("game-1"),
                candidates=2,
            )
        )
    else:
        planner = _Planner(ValueError("permanent provider failure"))
    game = _Game()
    store.save_planner_request(request)
    engine = _engine(store, game, planner)

    result = await engine.tick()

    stored = store.get_planner_request(request.planner_request_id)
    assert stored.status is status
    assert result.workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_REQUEST_TERMINATED
    )
    terminal = next(
        tick
        for tick in store.list_workflow_ticks("game-1")
        if isinstance(tick, StrategicRequestTerminatedTick)
    )
    attempts = store.list_provider_attempts(request.planner_request_id)
    assert terminal.planner_request_id == request.planner_request_id
    assert terminal.terminal_status is status
    assert terminal.failure_category == stored.failure_category
    assert terminal.provider_attempt_id == (
        None if not attempts else attempts[-1].provider_attempt_id
    )
    context = store.human_wait_context("game-1")
    assert context["wait_kind"] == "strategic_request_terminated"
    assert context["terminal_tick_id"] == terminal.tick_id
    assert context["planner_request_id"] == request.planner_request_id
    assert context["terminal_status"] == status.value
    assert context["failure_category"] == stored.failure_category
    provider_calls = planner.calls

    waiting = await engine.tick()

    assert waiting.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
    assert planner.calls == provider_calls
    assert store.human_wait_context("game-1") == context
    WorkflowStore(path)
    return store, request, terminal


@pytest.mark.parametrize(
    "status",
    [
        PlannerRequestStatus.FAILED,
        PlannerRequestStatus.REJECTED,
        PlannerRequestStatus.SUPERSEDED,
    ],
)
def test_terminal_strategic_wait_evidence_is_required_by_startup_and_replay(
    tmp_path, status
):
    path = tmp_path / f"terminal-wait-evidence-{status.value}.sqlite3"
    store, _request, terminal = asyncio.run(
        _terminal_strategic_request_state(path, status)
    )
    exported = store.export_replay_state("game-1")
    invalid = copy.deepcopy(exported)
    invalid["tables"]["workflow_ticks"] = [
        row
        for row in invalid["tables"]["workflow_ticks"]
        if row["tick_id"] != terminal.tick_id
    ]
    invalid["tables"]["runtime_state"] = []
    invalid["tables"]["workflow_meta"] = [
        row
        for row in invalid["tables"]["workflow_meta"]
        if row["key"] != "human_wait:game-1"
    ]
    target, _target_request, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(
            tmp_path / f"terminal-wait-target-{status.value}.sqlite3"
        )
    )
    before = target.export_replay_state("game-1")
    with pytest.raises(ValueError, match="termination Tick"):
        target.import_replay_state(copy.deepcopy(invalid))
    assert target.export_replay_state("game-1") == before

    tampered = copy.deepcopy(exported)
    _change_replay_tick(
        tampered,
        TickOutcomeKind.STRATEGIC_REQUEST_TERMINATED,
        failure_category="tampered_failure_category",
    )
    with pytest.raises(ValueError, match="termination Tick"):
        target.import_replay_state(tampered)
    assert target.export_replay_state("game-1") == before

    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM workflow_ticks WHERE tick_id=?", (terminal.tick_id,))
        conn.execute("DELETE FROM runtime_state WHERE game_id='game-1'")
        conn.execute("DELETE FROM workflow_meta WHERE key='human_wait:game-1'")
    with pytest.raises(ValueError, match="termination Tick"):
        WorkflowStore(path)


@pytest.mark.parametrize("entrypoint", ["tick_and_runtime", "phase4"])
def test_atomic_tick_entrypoints_reject_backdated_wait_error_after_resume(
    tmp_path, entrypoint
):
    path = tmp_path / f"backdated-wait-error-{entrypoint}.sqlite3"
    store, request, proposal, _active, planner, _engine_instance = asyncio.run(
        _resumed_proposal_state(path)
    )
    ready = _proposal_ready_tick(store, proposal.proposal_id)
    resumed = next(
        tick
        for tick in store.list_workflow_ticks("game-1")
        if tick.outcome is TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
    )
    forged = StrategicProposalWaitErrorTick(
        tick_id=f"tick-backdated-wait-error-{entrypoint}",
        game_session_id="game-1",
        turn_number=ready.turn_number,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=("obs-backdated-wait-error",),
        started_at=ready.completed_at,
        completed_at=resumed.started_at,
        blocking_reason="forged backdated Proposal wait error",
        error_category="TimeoutError",
        diagnostic_summary="forged after resume with an earlier timestamp",
        proposal_ready_tick_id=ready.tick_id,
        planner_request_id=request.planner_request_id,
        proposal_id=proposal.proposal_id,
        target_kind=proposal.target_kind,
        expected_base_revision=proposal.expected_base_revision,
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="active AWAITING_HUMAN"):
        if entrypoint == "tick_and_runtime":
            store.persist_tick_and_runtime_state(forged, human_wait_context=None)
        else:
            store.persist_phase4_tick(forged, human_wait_context=None)

    assert store.export_replay_state("game-1") == before
    assert store.load_runtime_state("game-1") is RuntimeState.ROUTING
    assert store.human_wait_context("game-1") is None
    assert planner.calls == 1
    WorkflowStore(path)
    _assert_replay_round_trip(tmp_path, store, f"backdated-wait-error-{entrypoint}")


@pytest.mark.parametrize("entrypoint", ["tick_and_runtime", "phase4"])
def test_terminal_wait_rejects_generic_tick_that_leaves_human_wait(
    tmp_path, entrypoint
):
    path = tmp_path / f"terminal-wait-bypass-{entrypoint}.sqlite3"
    store, _request_record, terminal = asyncio.run(
        _terminal_strategic_request_state(path, PlannerRequestStatus.FAILED)
    )
    started_at = max(
        tick.completed_at for tick in store.list_workflow_ticks("game-1")
    ) + timedelta(seconds=1)
    forged = ObservedOnlyTick(
        tick_id=f"tick-forged-terminal-resume-{entrypoint}",
        game_session_id="game-1",
        turn_number=terminal.turn_number,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=("obs-forged-terminal-resume",),
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=1),
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="only a strategic Request wait-resumed Tick"):
        if entrypoint == "tick_and_runtime":
            store.persist_tick_and_runtime_state(forged, human_wait_context=None)
        else:
            store.persist_phase4_tick(forged, human_wait_context=None)

    assert store.export_replay_state("game-1") == before
    assert store.load_runtime_state("game-1") is RuntimeState.AWAITING_HUMAN
    assert store.human_wait_context("game-1") is not None
    WorkflowStore(path)

    if entrypoint == "tick_and_runtime":
        invalid = copy.deepcopy(before)
        template = next(
            row
            for row in invalid["tables"]["workflow_ticks"]
            if row["outcome"] == TickOutcomeKind.AWAITING_HUMAN
        )
        forged_row = copy.deepcopy(template)
        forged_row.update(
            {
                "tick_id": forged.tick_id,
                "outcome": forged.outcome.value,
                "starting_runtime_state": forged.starting_runtime_state.value,
                "ending_runtime_state": forged.ending_runtime_state.value,
                "observation_ids_json": WorkflowStore._dump(
                    list(forged.observation_ids)
                ),
                "mutation_budget_used": forged.mutation_budget_used,
                "planner_request_id": None,
                "started_at": forged.started_at.isoformat(),
                "completed_at": forged.completed_at.isoformat(),
                "metrics_json": WorkflowStore._dump({}),
                "tick_json": forged.model_dump_json(),
            }
        )
        invalid["tables"]["workflow_ticks"].append(forged_row)
        metric_template = next(
            row
            for row in invalid["tables"]["turn_metrics"]
            if row["tick_id"] == template["tick_id"]
        )
        forged_metric = copy.deepcopy(metric_template)
        forged_metric["tick_id"] = forged.tick_id
        forged_metric["metrics_json"] = WorkflowStore._dump({})
        invalid["tables"]["turn_metrics"].append(forged_metric)
        invalid["tables"]["runtime_state"][0]["state"] = RuntimeState.OBSERVING.value
        invalid["tables"]["runtime_state"][0]["active_attempt_id"] = None
        invalid["tables"]["workflow_meta"] = [
            row
            for row in invalid["tables"]["workflow_meta"]
            if row["key"] != "human_wait:game-1"
        ]
        _assert_invalid_replay_preserves_target(
            tmp_path, invalid, "terminal-wait-generic-bypass"
        )


def test_terminal_wait_preflight_failure_preserves_wait_and_writes_diagnostic(
    tmp_path,
):
    async def scenario():
        path = tmp_path / "terminal-wait-system-error.sqlite3"
        store, request, terminal = await _terminal_strategic_request_state(
            path, PlannerRequestStatus.FAILED
        )
        before_context = store.human_wait_context("game-1")
        planner = _Planner()
        game = _Game()

        async def failed_snapshot(*, include_units=False):
            raise TimeoutError("snapshot unavailable")

        game.read_snapshot = failed_snapshot
        result = await _engine(store, game, planner).tick()

        assert result.workflow_tick["outcome"] == (
            TickOutcomeKind.STRATEGIC_REQUEST_WAIT_ERROR
        )
        assert result.runtime_state == RuntimeState.AWAITING_HUMAN.value
        assert planner.calls == 0
        context = store.human_wait_context("game-1")
        for field in (
            "wait_kind",
            "resume_policy",
            "planner_request_id",
            "terminal_tick_id",
            "terminal_status",
            "failure_category",
            "resume_requested",
        ):
            assert context[field] == before_context[field]
        errors = [
            tick
            for tick in store.list_workflow_ticks("game-1")
            if isinstance(tick, StrategicRequestWaitErrorTick)
        ]
        assert len(errors) == 1
        assert errors[0].planner_request_id == request.planner_request_id
        assert errors[0].terminal_tick_id == terminal.tick_id
        WorkflowStore(path)
        _assert_replay_round_trip(tmp_path, store, "terminal-wait-system-error")

    asyncio.run(scenario())


def test_terminal_wait_explicit_resume_is_auditable_restartable_and_replayable(
    tmp_path,
):
    async def scenario():
        path = tmp_path / "terminal-wait-explicit-resume.sqlite3"
        store, request, terminal = await _terminal_strategic_request_state(
            path, PlannerRequestStatus.FAILED
        )
        assert store.request_human_resume("game-1") is True
        requested_context = store.human_wait_context("game-1")
        assert requested_context["resume_requested"] is True
        requested_at = datetime.fromisoformat(requested_context["resume_requested_at"])

        restarted = WorkflowStore(path)
        assert restarted.human_wait_context("game-1") == requested_context
        exported = restarted.export_replay_state("game-1")
        restored = WorkflowStore(tmp_path / "terminal-wait-requested-restored.sqlite3")
        restored.import_replay_state(copy.deepcopy(exported))
        assert restored.human_wait_context("game-1") == requested_context
        assert restored.export_replay_state("game-1") == exported

        planner = _Planner()
        result = await _engine(restarted, _Game(), planner).tick()

        assert result.workflow_tick["outcome"] == (
            TickOutcomeKind.STRATEGIC_REQUEST_WAIT_RESUMED
        )
        resumed = next(
            tick
            for tick in restarted.list_workflow_ticks("game-1")
            if isinstance(tick, StrategicRequestWaitResumedTick)
        )
        assert resumed.planner_request_id == request.planner_request_id
        assert resumed.terminal_tick_id == terminal.tick_id
        assert resumed.terminal_status is PlannerRequestStatus.FAILED
        assert resumed.resume_reason == "explicit_user_resume"
        assert resumed.resumed_at == requested_at
        assert restarted.load_runtime_state("game-1") is RuntimeState.ROUTING
        assert restarted.human_wait_context("game-1") is None
        assert planner.calls == 0
        WorkflowStore(path)
        _assert_replay_round_trip(tmp_path, restarted, "terminal-wait-resumed")

    asyncio.run(scenario())


def test_terminal_resume_committed_before_wait_read_is_consumed(tmp_path):
    path = tmp_path / "terminal-resume-before-wait-read.sqlite3"
    store, _request_record, _terminal = asyncio.run(
        _terminal_strategic_request_state(path, PlannerRequestStatus.FAILED)
    )
    game = _Game()
    planner = _Planner()
    engine = _engine(store, game, planner)
    read_started = threading.Event()
    allow_read = threading.Event()
    original_read = game.read_snapshot

    async def blocked_read(*, include_units=False):
        read_started.set()
        completed = await asyncio.to_thread(allow_read.wait, 5)
        if not completed:
            raise TimeoutError("test did not release snapshot read")
        return await original_read(include_units=include_units)

    game.read_snapshot = blocked_read
    thread, result = _start_tick_thread(engine)
    assert read_started.wait(5)
    assert store.request_human_resume("game-1") is True
    requested_at = datetime.fromisoformat(
        store.human_wait_context("game-1")["resume_requested_at"]
    )
    allow_read.set()
    thread.join(10)

    assert not thread.is_alive()
    assert "error" not in result
    assert result["value"].workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_REQUEST_WAIT_RESUMED
    )
    resumed = next(
        tick
        for tick in store.list_workflow_ticks("game-1")
        if isinstance(tick, StrategicRequestWaitResumedTick)
    )
    assert resumed.resumed_at == requested_at
    assert store.human_wait_context("game-1") is None
    assert planner.calls == 0
    WorkflowStore(path)
    _assert_replay_round_trip(tmp_path, store, "terminal-resume-before-read")


def test_terminal_resume_committed_after_wait_read_before_persist_is_preserved(
    tmp_path,
):
    path = tmp_path / "terminal-resume-before-wait-persist.sqlite3"
    store, _request_record, _terminal = asyncio.run(
        _terminal_strategic_request_state(path, PlannerRequestStatus.FAILED)
    )
    planner = _Planner()
    engine = _engine(store, _Game(), planner)
    persist_started = threading.Event()
    allow_persist = threading.Event()
    original_persist = store.persist_tick_and_runtime_state

    def blocked_persist(*args, **kwargs):
        persist_started.set()
        if not allow_persist.wait(5):
            raise TimeoutError("test did not release Tick persistence")
        return original_persist(*args, **kwargs)

    store.persist_tick_and_runtime_state = blocked_persist
    thread, result = _start_tick_thread(engine)
    assert persist_started.wait(5)
    assert store.request_human_resume("game-1") is True
    requested_context = store.human_wait_context("game-1")
    allow_persist.set()
    thread.join(10)
    store.persist_tick_and_runtime_state = original_persist

    assert not thread.is_alive()
    assert "error" not in result
    assert result["value"].workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
    assert store.human_wait_context("game-1") == requested_context
    restarted = WorkflowStore(path)
    assert restarted.human_wait_context("game-1") == requested_context
    exported = restarted.export_replay_state("game-1")
    restored = WorkflowStore(tmp_path / "terminal-resume-race-restored.sqlite3")
    restored.import_replay_state(copy.deepcopy(exported))
    assert restored.human_wait_context("game-1") == requested_context
    assert restored.export_replay_state("game-1") == exported

    resumed = asyncio.run(_engine(restarted, _Game(), planner).tick())
    assert resumed.workflow_tick["outcome"] == (
        TickOutcomeKind.STRATEGIC_REQUEST_WAIT_RESUMED
    )
    assert restarted.human_wait_context("game-1") is None
    assert planner.calls == 0


def test_terminal_wait_aggregate_binds_latest_diagnostic_per_request():
    request_one = _request("game-1").model_copy(
        update={
            "status": PlannerRequestStatus.FAILED,
            "completed_at": NOW + timedelta(seconds=1),
            "failure_category": "planner_failure",
        }
    )
    request_two = _request("game-2").model_copy(
        update={
            "status": PlannerRequestStatus.SUPERSEDED,
            "completed_at": NOW + timedelta(seconds=1),
            "failure_category": "stale_strategic_contract_base",
        }
    )
    terminal_one = StrategicRequestTerminatedTick(
        tick_id="tick-terminal-game-1",
        game_session_id="game-1",
        turn_number=1,
        starting_runtime_state=RuntimeState.REQUESTING_PLAN,
        observation_ids=("obs-terminal-game-1",),
        started_at=NOW + timedelta(seconds=2),
        completed_at=NOW + timedelta(seconds=3),
        planner_request_id=request_one.planner_request_id,
        terminal_status=request_one.status,
        failure_category=request_one.failure_category,
        blocking_reason="game one terminated",
    )
    error_one = StrategicRequestWaitErrorTick(
        tick_id="tick-terminal-error-game-1",
        game_session_id="game-1",
        turn_number=1,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=("obs-terminal-error-game-1",),
        started_at=NOW + timedelta(seconds=4),
        completed_at=NOW + timedelta(seconds=5),
        planner_request_id=request_one.planner_request_id,
        terminal_tick_id=terminal_one.tick_id,
        terminal_status=request_one.status,
        failure_category=request_one.failure_category,
        blocking_reason="game one diagnostic",
        error_category="TimeoutError",
        diagnostic_summary="snapshot unavailable",
    )
    terminal_two = StrategicRequestTerminatedTick(
        tick_id="tick-terminal-game-2",
        game_session_id="game-2",
        turn_number=1,
        starting_runtime_state=RuntimeState.REQUESTING_PLAN,
        observation_ids=("obs-terminal-game-2",),
        started_at=NOW + timedelta(seconds=2),
        completed_at=NOW + timedelta(seconds=3),
        planner_request_id=request_two.planner_request_id,
        terminal_status=request_two.status,
        failure_category=request_two.failure_category,
        blocking_reason="game two terminated",
    )
    contexts = {
        "game-1": {
            "wait_kind": "strategic_request_terminated",
            "resume_policy": "explicit_only",
            "planner_request_id": request_one.planner_request_id,
            "terminal_tick_id": terminal_one.tick_id,
            "terminal_status": request_one.status.value,
            "failure_category": request_one.failure_category,
            "blocking_reason": error_one.blocking_reason,
            "resume_requested": False,
        },
        "game-2": {
            "wait_kind": "strategic_request_terminated",
            "resume_policy": "explicit_only",
            "planner_request_id": request_two.planner_request_id,
            "terminal_tick_id": terminal_two.tick_id,
            "terminal_status": request_two.status.value,
            "failure_category": request_two.failure_category,
            "blocking_reason": terminal_two.blocking_reason,
            "resume_requested": False,
        },
    }

    WorkflowStore._validate_strategic_terminal_wait_state(
        {
            request_one.planner_request_id: request_one,
            request_two.planner_request_id: request_two,
        },
        [],
        [terminal_one, error_one, terminal_two],
        {
            "game-1": RuntimeState.AWAITING_HUMAN,
            "game-2": RuntimeState.AWAITING_HUMAN,
        },
        contexts,
    )


def test_nonterminal_strategic_request_rejects_orphan_wait_tick():
    request = _request("game-1")
    orphan = StrategicRequestWaitErrorTick(
        tick_id="tick-orphan-terminal-error",
        game_session_id="game-1",
        turn_number=1,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=("obs-orphan-terminal-error",),
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=1),
        planner_request_id=request.planner_request_id,
        terminal_tick_id="tick-missing-termination",
        terminal_status=PlannerRequestStatus.FAILED,
        failure_category="planner_failure",
        blocking_reason="orphan diagnostic",
        error_category="TimeoutError",
        diagnostic_summary="orphan",
    )

    with pytest.raises(ValueError, match="non-terminal strategic Request"):
        WorkflowStore._validate_strategic_terminal_wait_state(
            {request.planner_request_id: request},
            [],
            [orphan],
            {},
            {},
        )


def _append_forged_replay_tick(state, tick):
    template = state["tables"]["workflow_ticks"][0]
    row = copy.deepcopy(template)
    row.update(
        {
            "tick_id": tick.tick_id,
            "game_id": tick.game_session_id,
            "turn": tick.turn_number,
            "outcome": tick.outcome.value,
            "starting_runtime_state": tick.starting_runtime_state.value,
            "ending_runtime_state": tick.ending_runtime_state.value,
            "observation_ids_json": WorkflowStore._dump(list(tick.observation_ids)),
            "mutation_budget_used": tick.mutation_budget_used,
            "selected_task_id": None,
            "action_attempt_id": getattr(tick, "action_attempt_id", None),
            "planner_request_id": getattr(tick, "planner_request_id", None),
            "started_at": tick.started_at.isoformat(),
            "completed_at": tick.completed_at.isoformat(),
            "metrics_json": WorkflowStore._dump({}),
            "tick_json": tick.model_dump_json(),
        }
    )
    state["tables"]["workflow_ticks"].append(row)
    metric_template = state["tables"]["turn_metrics"][0]
    metric = copy.deepcopy(metric_template)
    metric.update(
        {
            "tick_id": tick.tick_id,
            "game_id": tick.game_session_id,
            "turn": tick.turn_number,
            "metrics_json": WorkflowStore._dump({}),
        }
    )
    state["tables"]["turn_metrics"].append(metric)
    return row, metric


def _append_forged_observed_replay_tick(
    state,
    *,
    tick_id,
    started_at,
    completed_at,
):
    template = state["tables"]["workflow_ticks"][0]
    tick = ObservedOnlyTick(
        tick_id=tick_id,
        game_session_id=template["game_id"],
        turn_number=template["turn"],
        starting_runtime_state=RuntimeState.OBSERVING,
        observation_ids=(f"obs-{tick_id}",),
        started_at=started_at,
        completed_at=completed_at,
    )
    return _append_forged_replay_tick(state, tick)


def _insert_forged_replay_tick(path, row, metric):
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            INSERT INTO workflow_ticks(
                tick_id, game_id, turn, outcome,
                starting_runtime_state, ending_runtime_state,
                observation_ids_json, mutation_budget_used,
                selected_task_id, action_attempt_id,
                planner_request_id, started_at, completed_at,
                metrics_json, tick_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["tick_id"],
                row["game_id"],
                row["turn"],
                row["outcome"],
                row["starting_runtime_state"],
                row["ending_runtime_state"],
                row["observation_ids_json"],
                row["mutation_budget_used"],
                row["selected_task_id"],
                row["action_attempt_id"],
                row["planner_request_id"],
                row["started_at"],
                row["completed_at"],
                row["metrics_json"],
                row["tick_json"],
            ),
        )
        conn.execute(
            """
            INSERT INTO turn_metrics(tick_id, game_id, turn, metrics_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                metric["tick_id"],
                metric["game_id"],
                metric["turn"],
                metric["metrics_json"],
            ),
        )


@pytest.mark.parametrize("resolved", [False, True], ids=["unresolved", "resolved"])
def test_terminal_wait_interval_rejects_forged_nonwaiting_tick(tmp_path, resolved):
    path = tmp_path / f"terminal-interval-{resolved}.sqlite3"
    store, _request_record, terminal = asyncio.run(
        _terminal_strategic_request_state(path, PlannerRequestStatus.FAILED)
    )
    resumed = None
    if resolved:
        assert store.request_human_resume("game-1") is True
        result = asyncio.run(_engine(store, _Game(), _Planner()).tick())
        assert result.workflow_tick["outcome"] == (
            TickOutcomeKind.STRATEGIC_REQUEST_WAIT_RESUMED
        )
        resumed = next(
            tick
            for tick in store.list_workflow_ticks("game-1")
            if isinstance(tick, StrategicRequestWaitResumedTick)
        )
        started_at = terminal.completed_at + timedelta(microseconds=1)
        assert started_at < resumed.started_at
    else:
        started_at = max(
            tick.completed_at for tick in store.list_workflow_ticks("game-1")
        ) + timedelta(microseconds=1)
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    row, metric = _append_forged_observed_replay_tick(
        invalid,
        tick_id=f"tick-forged-terminal-interval-{resolved}",
        started_at=started_at,
        completed_at=started_at,
    )

    _assert_invalid_replay_preserves_target(
        tmp_path,
        invalid,
        f"terminal-interval-{resolved}",
        match="explicit-only wait interval",
    )

    _insert_forged_replay_tick(path, row, metric)
    with pytest.raises(ValueError, match="explicit-only wait interval"):
        WorkflowStore(path)


@pytest.mark.parametrize("resolved", [False, True], ids=["unresolved", "resolved"])
def test_proposal_wait_interval_rejects_forged_nonwaiting_tick(tmp_path, resolved):
    path = tmp_path / f"proposal-interval-{resolved}.sqlite3"
    if resolved:
        store, _request_record, proposal, _active, _planner, _engine_instance = (
            asyncio.run(_resumed_proposal_state(path))
        )
        ready = _proposal_ready_tick(store, proposal.proposal_id)
        resumed = next(
            tick
            for tick in store.list_workflow_ticks("game-1")
            if isinstance(tick, StrategicProposalWaitResumedTick)
        )
        started_at = ready.completed_at + timedelta(microseconds=1)
        assert started_at < resumed.started_at
    else:
        store, _request_record, proposal, _attempt = asyncio.run(
            _completed_proposal_state(path)
        )
        ready = _proposal_ready_tick(store, proposal.proposal_id)
        started_at = ready.completed_at + timedelta(microseconds=1)
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    row, metric = _append_forged_observed_replay_tick(
        invalid,
        tick_id=f"tick-forged-proposal-interval-{resolved}",
        started_at=started_at,
        completed_at=started_at,
    )

    _assert_invalid_replay_preserves_target(
        tmp_path,
        invalid,
        f"proposal-interval-{resolved}",
        match="explicit-only wait interval",
    )

    _insert_forged_replay_tick(path, row, metric)
    with pytest.raises(ValueError, match="explicit-only wait interval"):
        WorkflowStore(path)


@pytest.mark.parametrize("entrypoint", ["tick_and_runtime", "phase4"])
def test_proposal_resume_rejects_tick_started_before_ready(tmp_path, entrypoint):
    path = tmp_path / f"proposal-backdated-resume-{entrypoint}.sqlite3"
    store, _request_record, proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    ready = _proposal_ready_tick(store, proposal.proposal_id)
    assert store.request_human_resume("game-1") is True
    resume_request = store.list_strategic_proposal_wait_resume_requests("game-1")[0]
    completed_at = max(ready.completed_at, resume_request.requested_at) + timedelta(
        microseconds=1
    )
    forged = StrategicProposalWaitResumedTick(
        tick_id=f"tick-backdated-proposal-resume-{entrypoint}",
        game_session_id="game-1",
        turn_number=ready.turn_number,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=("obs-backdated-proposal-resume",),
        started_at=ready.completed_at - timedelta(microseconds=1),
        completed_at=completed_at,
        resume_request_id=resume_request.resume_request_id,
        proposal_ready_tick_id=ready.tick_id,
        planner_request_id=proposal.source_planner_request_id,
        proposal_id=proposal.proposal_id,
        target_kind=proposal.target_kind,
        expected_base_revision=proposal.expected_base_revision,
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="precedes the Proposal Ready Tick"):
        if entrypoint == "tick_and_runtime":
            store.persist_tick_and_runtime_state(forged, human_wait_context=None)
        else:
            store.persist_phase4_tick(forged, human_wait_context=None)

    assert store.export_replay_state("game-1") == before
    assert store.load_runtime_state("game-1") is RuntimeState.AWAITING_HUMAN
    context = store.human_wait_context("game-1")
    assert context is not None
    assert context["resume_requested"] is True
    WorkflowStore(path)
    _assert_replay_round_trip(tmp_path, store, f"backdated-resume-{entrypoint}")


def test_startup_and_replay_reject_backdated_proposal_resume_interval(tmp_path):
    path = tmp_path / "proposal-backdated-resume-interval.sqlite3"
    store, _request_record, proposal, _active, _planner, _engine_instance = asyncio.run(
        _resumed_proposal_state(path)
    )
    ready = _proposal_ready_tick(store, proposal.proposal_id)
    resumed = next(
        tick
        for tick in store.list_workflow_ticks("game-1")
        if isinstance(tick, StrategicProposalWaitResumedTick)
    )
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    resumed_row = _workflow_tick_replay_row(
        invalid, TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED
    )
    resumed_payload = WorkflowStore._load(resumed_row["tick_json"])
    backdated_at = ready.completed_at - timedelta(microseconds=1)
    resumed_row["started_at"] = backdated_at.isoformat()
    resumed_payload["started_at"] = backdated_at.isoformat()
    resumed_row["tick_json"] = WorkflowStore._dump(resumed_payload)
    forged_at = ready.completed_at + timedelta(microseconds=1)
    assert forged_at < resumed.completed_at
    row, metric = _append_forged_observed_replay_tick(
        invalid,
        tick_id="tick-forged-after-ready-before-backdated-resume-completion",
        started_at=forged_at,
        completed_at=forged_at,
    )

    _assert_invalid_replay_preserves_target(
        tmp_path,
        invalid,
        "backdated-proposal-resume-interval",
        match="resume Tick precedes the explicit-only wait",
    )

    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE workflow_ticks
            SET started_at=?, tick_json=?
            WHERE tick_id=?
            """,
            (
                resumed_row["started_at"],
                resumed_row["tick_json"],
                resumed_row["tick_id"],
            ),
        )
    _insert_forged_replay_tick(path, row, metric)
    with pytest.raises(ValueError, match="resume Tick precedes the explicit-only wait"):
        WorkflowStore(path)


def _resume_during_snapshot_read(store, resumed_type):
    game = _Game()
    planner = _Planner()
    engine = _engine(store, game, planner)
    read_started = threading.Event()
    allow_read = threading.Event()
    original_read = game.read_snapshot
    completion_floor = {"value": None}

    def controlled_now():
        current = datetime.now(UTC)
        floor = completion_floor["value"]
        return current if floor is None or current >= floor else floor

    engine._now = controlled_now

    async def blocked_read(*, include_units=False):
        read_started.set()
        completed = await asyncio.to_thread(allow_read.wait, 5)
        if not completed:
            raise TimeoutError("test did not release snapshot read")
        return await original_read(include_units=include_units)

    game.read_snapshot = blocked_read
    thread, result = _start_tick_thread(engine)
    assert read_started.wait(5)
    try:
        assert store.request_human_resume("game-1") is True
        if resumed_type is StrategicProposalWaitResumedTick:
            authorized_at = store.list_strategic_proposal_wait_resume_requests(
                "game-1"
            )[0].requested_at
        else:
            authorized_at = datetime.fromisoformat(
                store.human_wait_context("game-1")["resume_requested_at"]
            )
        completion_floor["value"] = authorized_at + timedelta(seconds=1)
    finally:
        allow_read.set()
    thread.join(10)

    assert not thread.is_alive()
    assert "error" not in result
    resumed = next(
        tick
        for tick in store.list_workflow_ticks("game-1")
        if isinstance(tick, resumed_type)
    )
    assert resumed.started_at < authorized_at <= resumed.completed_at
    assert planner.calls == 0
    return resumed, authorized_at


def _proposal_resume_during_tick_state(path):
    store, _request_record, proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    opening = _proposal_ready_tick(store, proposal.proposal_id)
    resumed, authorized_at = _resume_during_snapshot_read(
        store, StrategicProposalWaitResumedTick
    )
    return store, opening, resumed, authorized_at


def _terminal_resume_during_tick_state(path):
    store, _request_record, opening = asyncio.run(
        _terminal_strategic_request_state(path, PlannerRequestStatus.FAILED)
    )
    resumed, authorized_at = _resume_during_snapshot_read(
        store, StrategicRequestWaitResumedTick
    )
    return store, opening, resumed, authorized_at


def _resume_after_existing_authorization(store, resumed_type, authorized_at):
    engine = _engine(store, _Game(), _Planner())
    times = iter(
        (
            authorized_at + timedelta(seconds=1),
            authorized_at + timedelta(seconds=3),
        )
    )
    engine._now = lambda: next(times)
    result = asyncio.run(engine.tick())
    resumed = next(
        tick
        for tick in store.list_workflow_ticks("game-1")
        if isinstance(tick, resumed_type)
    )
    assert resumed.started_at > authorized_at
    assert resumed.completed_at > resumed.started_at
    assert result.workflow_tick["outcome"] == resumed.outcome
    return resumed


def _proposal_resume_before_tick_state(path):
    store, _request_record, proposal, _attempt = asyncio.run(
        _completed_proposal_state(path, repair=True)
    )
    opening = _proposal_ready_tick(store, proposal.proposal_id)
    assert store.request_human_resume("game-1") is True
    authorized_at = store.list_strategic_proposal_wait_resume_requests("game-1")[
        0
    ].requested_at
    resumed = _resume_after_existing_authorization(
        store, StrategicProposalWaitResumedTick, authorized_at
    )
    return store, opening, resumed, authorized_at


def _terminal_resume_before_tick_state(path):
    store, _request_record, opening = asyncio.run(
        _terminal_strategic_request_state(path, PlannerRequestStatus.FAILED)
    )
    assert store.request_human_resume("game-1") is True
    authorized_at = datetime.fromisoformat(
        store.human_wait_context("game-1")["resume_requested_at"]
    )
    resumed = _resume_after_existing_authorization(
        store, StrategicRequestWaitResumedTick, authorized_at
    )
    return store, opening, resumed, authorized_at


def _resume_transition_state(path, wait_kind, transition_case):
    if transition_case == "authorized_during_resume":
        setup = (
            _proposal_resume_during_tick_state
            if wait_kind == "proposal"
            else _terminal_resume_during_tick_state
        )
    else:
        setup = (
            _proposal_resume_before_tick_state
            if wait_kind == "proposal"
            else _terminal_resume_before_tick_state
        )
    return setup(path)


def _forged_resume_transition_tick(
    opening, resumed, authorized_at, wait_kind, transition_case
):
    if transition_case == "authorized_before_resume":
        assert authorized_at < resumed.started_at
        started_at = authorized_at + (resumed.started_at - authorized_at) / 2
        return ObservedOnlyTick(
            tick_id=f"tick-forged-{wait_kind}-after-authorization-before-resume",
            game_session_id="game-1",
            turn_number=opening.turn_number,
            starting_runtime_state=RuntimeState.OBSERVING,
            observation_ids=(f"obs-forged-{wait_kind}-before-resume",),
            started_at=started_at,
            completed_at=started_at,
        )
    if transition_case == "authorized_during_resume":
        assert resumed.started_at < authorized_at < resumed.completed_at
        started_at = authorized_at + (resumed.completed_at - authorized_at) / 2
        return ObservedOnlyTick(
            tick_id=f"tick-forged-{wait_kind}-during-resume",
            game_session_id="game-1",
            turn_number=opening.turn_number,
            starting_runtime_state=RuntimeState.OBSERVING,
            observation_ids=(f"obs-forged-{wait_kind}-during-resume",),
            started_at=started_at,
            completed_at=started_at,
        )
    assert transition_case == "waiting_overlaps_resume"
    started_at = opening.completed_at + (resumed.started_at - opening.completed_at) / 2
    completed_at = resumed.started_at + (resumed.completed_at - resumed.started_at) / 2
    return AwaitingHumanTick(
        tick_id=f"tick-forged-{wait_kind}-waiting-overlap",
        game_session_id="game-1",
        turn_number=opening.turn_number,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=(f"obs-forged-{wait_kind}-waiting-overlap",),
        started_at=started_at,
        completed_at=completed_at,
        blocking_reason="forged waiting Tick overlaps the Resume transition",
    )


@pytest.mark.parametrize("wait_kind", ["proposal", "terminal"])
def test_save_workflow_tick_rejects_tick_before_resume_authorization(
    tmp_path, wait_kind
):
    path = tmp_path / f"{wait_kind}-authorization-public-save.sqlite3"
    setup = (
        _proposal_resume_during_tick_state
        if wait_kind == "proposal"
        else _terminal_resume_during_tick_state
    )
    store, opening, resumed, authorized_at = setup(path)
    forged_at = resumed.started_at + (authorized_at - resumed.started_at) / 2
    forged = ObservedOnlyTick(
        tick_id=f"tick-forged-before-{wait_kind}-resume-authorization",
        game_session_id="game-1",
        turn_number=opening.turn_number,
        starting_runtime_state=RuntimeState.OBSERVING,
        observation_ids=(f"obs-forged-before-{wait_kind}-authorization",),
        started_at=forged_at,
        completed_at=forged_at,
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="explicit-only wait interval"):
        store.save_workflow_tick(forged)

    assert store.export_replay_state("game-1") == before
    WorkflowStore(path)
    _assert_replay_round_trip(tmp_path, store, f"{wait_kind}-authorization-save")


@pytest.mark.parametrize("wait_kind", ["proposal", "terminal"])
def test_startup_and_replay_reject_tick_before_resume_authorization(
    tmp_path, wait_kind
):
    path = tmp_path / f"{wait_kind}-authorization-replay.sqlite3"
    setup = (
        _proposal_resume_during_tick_state
        if wait_kind == "proposal"
        else _terminal_resume_during_tick_state
    )
    store, _opening, resumed, authorized_at = setup(path)
    forged_at = resumed.started_at + (authorized_at - resumed.started_at) / 2
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    row, metric = _append_forged_observed_replay_tick(
        invalid,
        tick_id=f"tick-forged-before-{wait_kind}-authorization-replay",
        started_at=forged_at,
        completed_at=forged_at,
    )

    _assert_invalid_replay_preserves_target(
        tmp_path,
        invalid,
        f"{wait_kind}-authorization-interval",
        match="explicit-only wait interval",
    )

    _insert_forged_replay_tick(path, row, metric)
    with pytest.raises(ValueError, match="explicit-only wait interval"):
        WorkflowStore(path)


@pytest.mark.parametrize("wait_kind", ["proposal", "terminal"])
@pytest.mark.parametrize(
    "transition_case",
    [
        "authorized_before_resume",
        "authorized_during_resume",
        "waiting_overlaps_resume",
    ],
)
def test_save_workflow_tick_rejects_tick_before_resume_transition_completes(
    tmp_path, wait_kind, transition_case
):
    path = tmp_path / f"{wait_kind}-{transition_case}-public-save.sqlite3"
    store, opening, resumed, authorized_at = _resume_transition_state(
        path, wait_kind, transition_case
    )
    forged = _forged_resume_transition_tick(
        opening, resumed, authorized_at, wait_kind, transition_case
    )
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="explicit-only wait interval"):
        store.save_workflow_tick(forged)

    assert store.export_replay_state("game-1") == before
    WorkflowStore(path)
    _assert_replay_round_trip(
        tmp_path, store, f"{wait_kind}-{transition_case}-public-save"
    )


@pytest.mark.parametrize("wait_kind", ["proposal", "terminal"])
@pytest.mark.parametrize(
    "transition_case",
    [
        "authorized_before_resume",
        "authorized_during_resume",
        "waiting_overlaps_resume",
    ],
)
def test_startup_and_replay_reject_tick_before_resume_transition_completes(
    tmp_path, wait_kind, transition_case
):
    path = tmp_path / f"{wait_kind}-{transition_case}-replay.sqlite3"
    store, opening, resumed, authorized_at = _resume_transition_state(
        path, wait_kind, transition_case
    )
    forged = _forged_resume_transition_tick(
        opening, resumed, authorized_at, wait_kind, transition_case
    )
    invalid = copy.deepcopy(store.export_replay_state("game-1"))
    row, metric = _append_forged_replay_tick(invalid, forged)

    _assert_invalid_replay_preserves_target(
        tmp_path,
        invalid,
        f"{wait_kind}-{transition_case}-interval",
        match="explicit-only wait interval",
    )

    _insert_forged_replay_tick(path, row, metric)
    with pytest.raises(ValueError, match="explicit-only wait interval"):
        WorkflowStore(path)


def _canonical_model_json(value):
    return json.dumps(
        value.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _append_terminal_tick_to_replay(state, tick):
    payload = tick.model_dump(mode="json")
    state["tables"]["workflow_ticks"].append(
        {
            "tick_id": tick.tick_id,
            "game_id": tick.game_session_id,
            "turn": tick.turn_number,
            "outcome": tick.outcome.value,
            "starting_runtime_state": tick.starting_runtime_state.value,
            "ending_runtime_state": tick.ending_runtime_state.value,
            "observation_ids_json": json.dumps(list(tick.observation_ids)),
            "mutation_budget_used": tick.mutation_budget_used,
            "selected_task_id": None,
            "action_attempt_id": None,
            "planner_request_id": tick.planner_request_id,
            "started_at": tick.started_at.isoformat(),
            "completed_at": tick.completed_at.isoformat(),
            "metrics_json": json.dumps(payload["metrics"], separators=(",", ":")),
            "tick_json": tick.model_dump_json(),
        }
    )
    state["tables"]["turn_metrics"].append(
        {
            "tick_id": tick.tick_id,
            "game_id": tick.game_session_id,
            "turn": tick.turn_number,
            "metrics_json": json.dumps(payload["metrics"], separators=(",", ":")),
            "created_at": tick.completed_at.isoformat(),
        }
    )


def _rejected_terminal_replay(store, proposal):
    state = copy.deepcopy(store.export_replay_state(proposal.game_session_id))
    ready = next(
        tick
        for tick in store.list_workflow_ticks(proposal.game_session_id)
        if isinstance(tick, StrategicProposalReadyTick)
        and tick.proposal_id == proposal.proposal_id
    )
    decided_at = ready.completed_at + timedelta(seconds=1)
    approval = StrategicProposalApprovalRecord(
        approval_id=f"approval-{proposal.proposal_id}",
        proposal_id=proposal.proposal_id,
        decision=ApprovalDecision.REJECTED,
        actor="test-user",
        created_at=decided_at,
        reason="not this research plan",
    )
    rejected = StrategicProposalRejectedTick(
        tick_id=build_strategic_proposal_terminal_tick_id(
            proposal.proposal_id,
            TickOutcomeKind.STRATEGIC_PROPOSAL_REJECTED,
        ),
        game_session_id=proposal.game_session_id,
        turn_number=ready.turn_number,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=ready.observation_ids,
        started_at=decided_at,
        completed_at=decided_at + timedelta(seconds=1),
        planner_request_id=proposal.source_planner_request_id,
        proposal_id=proposal.proposal_id,
        proposal_hash=proposal.proposal_hash,
        proposal_ready_tick_id=ready.tick_id,
        approval_id=approval.approval_id,
        target_contract_id=proposal.target_contract_id,
        expected_base_revision=proposal.expected_base_revision,
    )
    state["tables"]["approval_records"].append(
        {
            "approval_id": approval.approval_id,
            "game_id": proposal.game_session_id,
            "proposal_type": approval.proposal_type,
            "proposal_id": approval.proposal_id,
            "proposal_revision": approval.proposal_revision,
            "decision": approval.decision.value,
            "record_json": _canonical_model_json(approval),
            "created_at": approval.created_at.isoformat(),
        }
    )
    _append_terminal_tick_to_replay(state, rejected)
    state["tables"]["workflow_meta"] = [
        row
        for row in state["tables"]["workflow_meta"]
        if row["key"] != f"human_wait:{proposal.game_session_id}"
    ]
    runtime = state["tables"]["runtime_state"][0]
    runtime["state"] = RuntimeState.ROUTING.value
    runtime["active_attempt_id"] = None
    runtime["revision"] = int(runtime["revision"]) + 1
    return state, approval, rejected


def _approved_terminal_replay(store, proposal):
    state = copy.deepcopy(store.export_replay_state(proposal.game_session_id))
    ready = next(
        tick
        for tick in store.list_workflow_ticks(proposal.game_session_id)
        if isinstance(tick, StrategicProposalReadyTick)
        and tick.proposal_id == proposal.proposal_id
    )
    decided_at = ready.completed_at + timedelta(seconds=1)
    approval = StrategicProposalApprovalRecord(
        approval_id=f"approval-{proposal.proposal_id}",
        proposal_id=proposal.proposal_id,
        decision=ApprovalDecision.APPROVED,
        actor="test-user",
        created_at=decided_at,
        reason="approve research strategy",
    )
    contract = StrategicContract(
        contract_id=proposal.target_contract_id,
        game_session_id=proposal.game_session_id,
        revision=proposal.expected_base_revision + 1,
        authority_scope_set=AuthorityScopeSet(mission_graph_scopes=("research",)),
        mission_graph=MissionGraph(missions=(proposal.proposed_research_mission,)),
        strategic_objectives=proposal.strategic_objectives,
        global_constraints=proposal.global_constraints,
        created_from_observation_id=proposal.created_from_observation_id,
        approval_status=ApprovalStatus.APPROVED,
    )
    commit = StrategicContractCommit(
        commit_id=f"commit-{proposal.proposal_id}",
        game_session_id=proposal.game_session_id,
        contract_id=proposal.target_contract_id,
        expected_base_revision=proposal.expected_base_revision,
        contract=contract,
        committed_at=decided_at,
        reason="Proposal-derived research activation",
        source_proposal_id=proposal.proposal_id,
        source_proposal_hash=proposal.proposal_hash,
        source_approval_id=approval.approval_id,
        source_planner_request_id=proposal.source_planner_request_id,
        source_mission_id=proposal.proposed_research_mission.mission_id,
        source_mission_revision=proposal.proposed_research_mission.mission_revision,
    )
    applied = StrategicProposalAppliedTick(
        tick_id=build_strategic_proposal_terminal_tick_id(
            proposal.proposal_id,
            TickOutcomeKind.STRATEGIC_PROPOSAL_APPLIED,
        ),
        game_session_id=proposal.game_session_id,
        turn_number=ready.turn_number,
        starting_runtime_state=RuntimeState.AWAITING_HUMAN,
        observation_ids=ready.observation_ids,
        started_at=decided_at,
        completed_at=decided_at + timedelta(seconds=1),
        planner_request_id=proposal.source_planner_request_id,
        proposal_id=proposal.proposal_id,
        proposal_hash=proposal.proposal_hash,
        proposal_ready_tick_id=ready.tick_id,
        approval_id=approval.approval_id,
        target_contract_id=proposal.target_contract_id,
        expected_base_revision=proposal.expected_base_revision,
        contract_commit_id=commit.commit_id,
        activated_contract_revision=contract.revision,
        source_mission_id=proposal.proposed_research_mission.mission_id,
        source_mission_revision=proposal.proposed_research_mission.mission_revision,
    )
    state["tables"]["approval_records"].append(
        {
            "approval_id": approval.approval_id,
            "game_id": proposal.game_session_id,
            "proposal_type": approval.proposal_type,
            "proposal_id": approval.proposal_id,
            "proposal_revision": approval.proposal_revision,
            "decision": approval.decision.value,
            "record_json": _canonical_model_json(approval),
            "created_at": approval.created_at.isoformat(),
        }
    )
    roots = state["tables"]["strategic_contract_roots"]
    if roots:
        assert len(roots) == 1
        roots[0]["active_revision"] = contract.revision
    else:
        roots.append(
            {
                "game_id": proposal.game_session_id,
                "contract_id": contract.contract_id,
                "active_revision": contract.revision,
                "created_at": commit.committed_at.isoformat(),
            }
        )
    state["tables"]["strategic_contract_revisions"].append(
        {
            "game_id": proposal.game_session_id,
            "contract_id": contract.contract_id,
            "revision": contract.revision,
            "contract_json": _canonical_model_json(contract),
            "committed_at": commit.committed_at.isoformat(),
        }
    )
    state["tables"]["strategic_contract_commits"].append(
        {
            "commit_id": commit.commit_id,
            "game_id": proposal.game_session_id,
            "contract_id": contract.contract_id,
            "expected_base_revision": commit.expected_base_revision,
            "committed_revision": contract.revision,
            "commit_json": _canonical_model_json(commit),
            "committed_at": commit.committed_at.isoformat(),
        }
    )
    _append_terminal_tick_to_replay(state, applied)
    state["tables"]["workflow_meta"] = [
        row
        for row in state["tables"]["workflow_meta"]
        if row["key"] != f"human_wait:{proposal.game_session_id}"
    ]
    runtime = state["tables"]["runtime_state"][0]
    runtime["state"] = RuntimeState.ROUTING.value
    runtime["active_attempt_id"] = None
    runtime["revision"] = int(runtime["revision"]) + 1
    return state, approval, commit, applied


def _migration_invalidated_replay(store, request, proposal, attempt):
    ready = next(
        tick
        for tick in store.list_workflow_ticks(proposal.game_session_id)
        if isinstance(tick, StrategicProposalReadyTick)
        and tick.proposal_id == proposal.proposal_id
    )
    resume_request = store.strategic_proposal_wait_resume_request_for_proposal(
        proposal.proposal_id
    )
    resumed = next(
        tick
        for tick in store.list_workflow_ticks(proposal.game_session_id)
        if isinstance(tick, StrategicProposalWaitResumedTick)
        and tick.proposal_id == proposal.proposal_id
    )
    assert resume_request is not None
    assert request.completed_at is not None
    assert attempt.completed_at is not None
    causal_time = max(
        proposal.created_at,
        request.completed_at,
        attempt.completed_at,
        ready.completed_at,
        resume_request.requested_at,
        resumed.completed_at,
    ) + timedelta(microseconds=1)
    root = store.get_active_strategic_contract(proposal.game_session_id)
    if root is None:
        invalidation_reason = (
            StrategicProposalInvalidationReason.PRE_PHASE1C_WAIT_RELEASED
        )
    elif root.contract_id != proposal.target_contract_id:
        invalidation_reason = (
            StrategicProposalInvalidationReason.TARGET_CONTRACT_CHANGED
        )
    elif proposal.target_kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION:
        invalidation_reason = (
            StrategicProposalInvalidationReason.TARGET_CONTRACT_CREATED
        )
    elif root.revision != proposal.expected_base_revision:
        invalidation_reason = StrategicProposalInvalidationReason.BASE_REVISION_CHANGED
    else:
        invalidation_reason = (
            StrategicProposalInvalidationReason.PRE_PHASE1C_WAIT_RELEASED
        )
    invalidated = StrategicProposalInvalidatedTick(
        tick_id=build_strategic_proposal_terminal_tick_id(
            proposal.proposal_id,
            TickOutcomeKind.STRATEGIC_PROPOSAL_INVALIDATED,
        ),
        game_session_id=proposal.game_session_id,
        turn_number=resumed.turn_number,
        starting_runtime_state=RuntimeState.ROUTING,
        observation_ids=ready.observation_ids,
        started_at=causal_time,
        completed_at=causal_time,
        planner_request_id=proposal.source_planner_request_id,
        proposal_id=proposal.proposal_id,
        proposal_hash=proposal.proposal_hash,
        proposal_ready_tick_id=ready.tick_id,
        target_contract_id=proposal.target_contract_id,
        expected_base_revision=proposal.expected_base_revision,
        invalidation_origin=(
            StrategicProposalInvalidationOrigin.PHASE1C_ENABLEMENT_MIGRATION
        ),
        invalidation_reason=invalidation_reason,
        source_resume_request_id=resume_request.resume_request_id,
        source_wait_resumed_tick_id=resumed.tick_id,
    )
    state = copy.deepcopy(store.export_replay_state(proposal.game_session_id))
    _append_terminal_tick_to_replay(state, invalidated)
    return state, invalidated


def _research_task_replay_row(
    proposal,
    *,
    action_type="set_research",
    status=TaskStatus.READY,
    provenance=True,
):
    return {
        "game_id": proposal.game_session_id,
        "task_id": "phase1c-research-task",
        "plan_id": "phase1c-research-plan",
        "action_type": action_type,
        "entity_type": "player",
        "entity_id": "player-1",
        "due_turn": 1,
        "expires_turn": None,
        "arguments_json": json.dumps({"tech_or_civic": "TECH_WRITING"}),
        "preconditions_json": "[]",
        "postconditions_json": "[]",
        "invalidators_json": "[]",
        "risk": RiskLevel.LOW.value,
        "requires_confirmation": 0,
        "reason": "Mission-derived research projection",
        "status": status.value,
        "retry_count": 0,
        "max_retries": 2,
        "last_error": None,
        "approved_by": None,
        "created_turn": 1,
        "created_from_observation_id": proposal.created_from_observation_id,
        "source_contract_id": proposal.target_contract_id if provenance else None,
        "source_contract_revision": (
            proposal.expected_base_revision + 1 if provenance else None
        ),
        "source_mission_id": (
            proposal.proposed_research_mission.mission_id if provenance else None
        ),
        "source_mission_revision": (
            proposal.proposed_research_mission.mission_revision if provenance else None
        ),
        "updated_at": NOW.isoformat(),
    }


@pytest.mark.parametrize(
    "decision",
    [
        ApprovalDecision.CANCELLED,
        ApprovalDecision.REQUESTED_REPLAN,
        ApprovalDecision.EDITED_AND_APPROVED,
    ],
)
def test_strategic_proposal_decision_contract_rejects_shared_decisions(decision):
    fields = {
        "approval_id": "approval-unsupported",
        "proposal_id": "proposal-1",
        "decision": decision,
        "actor": "test-user",
        "created_at": NOW,
    }
    if decision is ApprovalDecision.EDITED_AND_APPROVED:
        fields.update(
            edited_payload={"technology": "TECH_WRITING"}, replacement_revision=2
        )
    with pytest.raises(ValueError):
        StrategicProposalApprovalRecord(**fields)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"status": MissionStatus.COMPLETED}, "must be ACTIVE"),
        ({"scope": "civic", "slot": "player:civic"}, "scope must be research"),
        (
            {
                "desired_outcome": {
                    "technology": "TECH_WRITING",
                    "tool_name": "set_civic",
                }
            },
            "cannot select an execution action",
        ),
    ],
)
def test_proposal_provenance_requires_active_closed_research_mission(updates, message):
    request_id = "request-fixed-for-proposal-id"
    mission = _mission("game-1", build_strategic_contract_id("game-1")).model_copy(
        update=updates
    )
    with pytest.raises(ValueError, match=message):
        build_strategic_research_proposal(
            proposal_id=build_strategic_research_proposal_id(request_id),
            game_session_id="game-1",
            source_planner_request_id=request_id,
            source_provider_attempt_id="attempt-1",
            source_provider_attempt_number=1,
            target_kind=PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
            target_contract_id=build_strategic_contract_id("game-1"),
            expected_base_revision=0,
            strategic_objectives=(),
            global_constraints=(),
            proposed_research_mission=mission,
            created_from_observation_id="obs-1",
            created_at=NOW,
        )


@pytest.mark.parametrize(
    "provenance",
    [
        {"source_contract_id": "contract-1"},
        {"source_contract_revision": 1},
        {"source_mission_id": "mission-1"},
        {"source_mission_revision": 1},
    ],
)
def test_stored_task_rejects_partial_mission_provenance(provenance):
    fields = {
        "task_id": "task-1",
        "plan_id": "plan-1",
        "action_type": "set_research",
        "entity_type": "player",
        "entity_id": "player-1",
        "due_turn": 1,
        "arguments": {"technology": "TECH_WRITING"},
        "preconditions": [],
        "postconditions": [],
        "invalidators": [],
        "risk": RiskLevel.LOW,
        "requires_confirmation": False,
        "reason": "research projection",
        "created_turn": 1,
        "status": TaskStatus.READY,
        **provenance,
    }
    with pytest.raises(ValueError, match="all present or all null"):
        TurnActionExecution(**fields)


def test_stored_task_accepts_complete_mission_provenance():
    task = TurnActionExecution(
        task_id="task-1",
        plan_id="plan-1",
        action_type="set_research",
        entity_type="player",
        entity_id="player-1",
        due_turn=1,
        arguments={"technology": "TECH_WRITING"},
        preconditions=[],
        postconditions=[],
        invalidators=[],
        risk=RiskLevel.LOW,
        requires_confirmation=False,
        reason="research projection",
        created_turn=1,
        status=TaskStatus.READY,
        source_contract_id="contract-1",
        source_contract_revision=1,
        source_mission_id="mission-1",
        source_mission_revision=1,
    )

    assert task.source_contract_id == "contract-1"


def test_rejected_terminal_shape_round_trips_and_has_typed_read(tmp_path):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "rejected-source.sqlite3")
    )
    state, approval, rejected = _rejected_terminal_replay(source, proposal)
    restored_path = tmp_path / "rejected-restored.sqlite3"
    restored = WorkflowStore(restored_path)

    restored.import_replay_state(state)

    assert (
        restored.strategic_proposal_approval_record(
            proposal.game_session_id, proposal.proposal_id
        )
        == approval
    )
    assert rejected in restored.list_workflow_ticks(proposal.game_session_id)
    WorkflowStore(restored_path)
    second = WorkflowStore(tmp_path / "rejected-second.sqlite3")
    second.import_replay_state(restored.export_replay_state(proposal.game_session_id))
    assert second.export_replay_state(
        proposal.game_session_id
    ) == restored.export_replay_state(proposal.game_session_id)


def test_approved_terminal_shape_round_trips_complete_contract_evidence(tmp_path):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "approved-source.sqlite3")
    )
    state, approval, commit, applied = _approved_terminal_replay(source, proposal)
    restored_path = tmp_path / "approved-restored.sqlite3"
    restored = WorkflowStore(restored_path)

    restored.import_replay_state(state)

    active = restored.get_active_strategic_contract(proposal.game_session_id)
    assert active == commit.contract
    assert active is not None
    assert active.authority_scope_set.mission_graph_scopes == ("research",)
    assert active.mission_graph.missions == (proposal.proposed_research_mission,)
    assert (
        restored.strategic_proposal_approval_record(
            proposal.game_session_id, proposal.proposal_id
        )
        == approval
    )
    assert applied in restored.list_workflow_ticks(proposal.game_session_id)
    WorkflowStore(restored_path)
    replayed = WorkflowStore(tmp_path / "approved-replayed.sqlite3")
    replayed.import_replay_state(restored.export_replay_state(proposal.game_session_id))
    assert replayed.export_replay_state(
        proposal.game_session_id
    ) == restored.export_replay_state(proposal.game_session_id)


def test_repair_proposal_approved_shape_appends_existing_contract(tmp_path):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(
            tmp_path / "approved-repair-source.sqlite3",
            repair=True,
        )
    )
    state, _approval, commit, applied = _approved_terminal_replay(source, proposal)
    restored_path = tmp_path / "approved-repair-restored.sqlite3"
    restored = WorkflowStore(restored_path)

    restored.import_replay_state(state)

    revisions = restored.list_strategic_contract_revisions(proposal.game_session_id)
    assert [item.revision for item in revisions] == [1, 2]
    assert revisions[-1] == commit.contract
    assert applied in restored.list_workflow_ticks(proposal.game_session_id)
    WorkflowStore(restored_path)


@pytest.mark.parametrize(
    "missing_table",
    ["approval_records", "strategic_contract_commits", "workflow_ticks"],
)
def test_approved_terminal_missing_evidence_fails_before_replay_delete(
    tmp_path, missing_table
):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(
            tmp_path / f"approved-missing-{missing_table}.sqlite3"
        )
    )
    invalid, approval, commit, applied = _approved_terminal_replay(source, proposal)
    identities = {
        "approval_records": ("approval_id", approval.approval_id),
        "strategic_contract_commits": ("commit_id", commit.commit_id),
        "workflow_ticks": ("tick_id", applied.tick_id),
    }
    key, value = identities[missing_table]
    invalid["tables"][missing_table] = [
        row for row in invalid["tables"][missing_table] if row[key] != value
    ]
    if missing_table == "workflow_ticks":
        invalid["tables"]["turn_metrics"] = [
            row
            for row in invalid["tables"]["turn_metrics"]
            if row["tick_id"] != applied.tick_id
        ]
    target = WorkflowStore(tmp_path / f"approved-target-{missing_table}.sqlite3")
    target.set_meta("last_game_id", proposal.game_session_id)
    before = target.export_replay_state(proposal.game_session_id)

    with pytest.raises((ValueError, sqlite3.IntegrityError)):
        target.import_replay_state(invalid)

    assert target.export_replay_state(proposal.game_session_id) == before


def test_migration_invalidated_tick_uses_one_canonical_causal_time(tmp_path):
    store, request, proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "migration-invalidated-source.sqlite3")
    )
    assert store.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(store, _Game(), _Planner()).tick())
    state, invalidated = _migration_invalidated_replay(
        store, request, proposal, attempt
    )
    restored_path = tmp_path / "migration-invalidated-restored.sqlite3"
    restored = WorkflowStore(restored_path)

    restored.import_replay_state(state)

    assert invalidated in restored.list_workflow_ticks(proposal.game_session_id)
    WorkflowStore(restored_path)
    replayed = WorkflowStore(tmp_path / "migration-invalidated-replayed.sqlite3")
    replayed.import_replay_state(restored.export_replay_state(proposal.game_session_id))
    assert replayed.export_replay_state(
        proposal.game_session_id
    ) == restored.export_replay_state(proposal.game_session_id)


def test_migration_invalidated_tick_uses_stale_reason_precedence(tmp_path):
    store, request, proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "migration-stale-source.sqlite3")
    )
    assert store.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(store, _Game(), _Planner()).tick())
    _commit_contract(store, proposal.game_session_id)
    state, invalidated = _migration_invalidated_replay(
        store, request, proposal, attempt
    )
    restored = WorkflowStore(tmp_path / "migration-stale-restored.sqlite3")

    restored.import_replay_state(state)

    assert (
        invalidated.invalidation_reason
        is StrategicProposalInvalidationReason.TARGET_CONTRACT_CREATED
    )
    WorkflowStore(restored.path)


def test_migration_invalidated_wrong_reason_fails_before_replay_delete(tmp_path):
    store, request, proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "migration-reason-source.sqlite3")
    )
    assert store.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(store, _Game(), _Planner()).tick())
    invalid, invalidated = _migration_invalidated_replay(
        store, request, proposal, attempt
    )
    row = next(
        item
        for item in invalid["tables"]["workflow_ticks"]
        if item["tick_id"] == invalidated.tick_id
    )
    forged = invalidated.model_copy(
        update={
            "invalidation_reason": (
                StrategicProposalInvalidationReason.TARGET_CONTRACT_CREATED
            )
        }
    )
    row["tick_json"] = forged.model_dump_json()
    target = WorkflowStore(tmp_path / "migration-reason-target.sqlite3")
    target.set_meta("last_game_id", proposal.game_session_id)
    before = target.export_replay_state(proposal.game_session_id)

    with pytest.raises(ValueError, match="deterministic reason"):
        target.import_replay_state(invalid)

    assert target.export_replay_state(proposal.game_session_id) == before


def test_migration_invalidated_wrong_time_fails_before_replay_delete(tmp_path):
    store, request, proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "migration-time-source.sqlite3")
    )
    assert store.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(store, _Game(), _Planner()).tick())
    invalid, invalidated = _migration_invalidated_replay(
        store, request, proposal, attempt
    )
    row = next(
        item
        for item in invalid["tables"]["workflow_ticks"]
        if item["tick_id"] == invalidated.tick_id
    )
    forged = invalidated.model_copy(
        update={
            "started_at": invalidated.started_at + timedelta(microseconds=1),
            "completed_at": invalidated.completed_at + timedelta(microseconds=1),
        }
    )
    row["started_at"] = forged.started_at.isoformat()
    row["completed_at"] = forged.completed_at.isoformat()
    row["tick_json"] = forged.model_dump_json()
    target = WorkflowStore(tmp_path / "migration-time-target.sqlite3")
    target.set_meta("last_game_id", proposal.game_session_id)
    before = target.export_replay_state(proposal.game_session_id)

    with pytest.raises(ValueError, match="canonical causal time"):
        target.import_replay_state(invalid)

    assert target.export_replay_state(proposal.game_session_id) == before


def test_foundation_commit_cannot_remove_activated_research_authority(tmp_path):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "foundation-rollback-source.sqlite3")
    )
    state, _approval, commit, _applied = _approved_terminal_replay(source, proposal)
    store = WorkflowStore(tmp_path / "foundation-rollback.sqlite3")
    store.import_replay_state(state)
    before = store.export_replay_state(proposal.game_session_id)
    rollback_contract = StrategicContract(
        contract_id=proposal.target_contract_id,
        game_session_id=proposal.game_session_id,
        revision=commit.contract.revision + 1,
        strategic_objectives=("rollback authority",),
    )
    rollback_commit = StrategicContractCommit(
        commit_id="foundation-rollback-commit",
        game_session_id=proposal.game_session_id,
        contract_id=proposal.target_contract_id,
        expected_base_revision=commit.contract.revision,
        contract=rollback_contract,
        committed_at=commit.committed_at + timedelta(seconds=2),
        reason="forged foundation rollback",
    )

    with pytest.raises(ValueError, match="cannot follow Proposal activation"):
        store.commit_strategic_contract_revision(rollback_commit)

    assert store.export_replay_state(proposal.game_session_id) == before
    WorkflowStore(store.path)


@pytest.mark.parametrize("missing", ["approval", "commit", "applied"])
def test_startup_rejects_incomplete_approved_terminal_aggregate(tmp_path, missing):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(
            tmp_path / f"startup-approved-{missing}-source.sqlite3"
        )
    )
    state, approval, commit, applied = _approved_terminal_replay(source, proposal)
    path = tmp_path / f"startup-approved-{missing}.sqlite3"
    restored = WorkflowStore(path)
    restored.import_replay_state(state)

    with sqlite3.connect(path) as conn:
        if missing == "approval":
            conn.execute(
                "DELETE FROM approval_records WHERE approval_id=?",
                (approval.approval_id,),
            )
        elif missing == "commit":
            conn.execute(
                "DELETE FROM strategic_contract_commits WHERE commit_id=?",
                (commit.commit_id,),
            )
        else:
            conn.execute(
                "DELETE FROM workflow_ticks WHERE tick_id=?", (applied.tick_id,)
            )

    with pytest.raises(ValueError):
        WorkflowStore(path)


def test_startup_rejects_wrong_proposal_hash_in_contract_commit(tmp_path):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "startup-wrong-hash-source.sqlite3")
    )
    state, _approval, commit, _applied = _approved_terminal_replay(source, proposal)
    path = tmp_path / "startup-wrong-hash.sqlite3"
    restored = WorkflowStore(path)
    restored.import_replay_state(state)
    forged = commit.model_copy(update={"source_proposal_hash": "0" * 64})

    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE strategic_contract_commits SET commit_json=?
            WHERE commit_id=?
            """,
            (_canonical_model_json(forged), commit.commit_id),
        )

    with pytest.raises(ValueError, match="APPROVED Proposal"):
        WorkflowStore(path)


def test_replay_rejects_mixed_human_and_system_terminal_facts_before_delete(
    tmp_path,
):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "mixed-terminal-source.sqlite3")
    )
    invalid, _approval, _commit, applied = _approved_terminal_replay(source, proposal)
    invalidated = StrategicProposalInvalidatedTick(
        tick_id=build_strategic_proposal_terminal_tick_id(
            proposal.proposal_id,
            TickOutcomeKind.STRATEGIC_PROPOSAL_INVALIDATED,
        ),
        game_session_id=proposal.game_session_id,
        turn_number=applied.turn_number,
        starting_runtime_state=RuntimeState.ROUTING,
        observation_ids=applied.observation_ids,
        started_at=applied.completed_at + timedelta(seconds=1),
        completed_at=applied.completed_at + timedelta(seconds=1),
        planner_request_id=proposal.source_planner_request_id,
        proposal_id=proposal.proposal_id,
        proposal_hash=proposal.proposal_hash,
        proposal_ready_tick_id=applied.proposal_ready_tick_id,
        target_contract_id=proposal.target_contract_id,
        expected_base_revision=proposal.expected_base_revision,
        invalidation_origin=StrategicProposalInvalidationOrigin.RUNTIME,
        invalidation_reason=(
            StrategicProposalInvalidationReason.TARGET_CONTRACT_CREATED
        ),
    )
    _append_terminal_tick_to_replay(invalid, invalidated)
    target = WorkflowStore(tmp_path / "mixed-terminal-target.sqlite3")
    target.set_meta("last_game_id", proposal.game_session_id)
    before = target.export_replay_state(proposal.game_session_id)

    with pytest.raises(ValueError, match="mixed terminal authorities"):
        target.import_replay_state(invalid)

    assert target.export_replay_state(proposal.game_session_id) == before


def test_replay_rejects_duplicate_invalidation_fact_before_delete(tmp_path):
    store, request, proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "duplicate-invalidation-source.sqlite3")
    )
    assert store.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(store, _Game(), _Planner()).tick())
    invalid, invalidated = _migration_invalidated_replay(
        store, request, proposal, attempt
    )
    tick_row = next(
        row
        for row in invalid["tables"]["workflow_ticks"]
        if row["tick_id"] == invalidated.tick_id
    )
    metric_row = next(
        row
        for row in invalid["tables"]["turn_metrics"]
        if row["tick_id"] == invalidated.tick_id
    )
    invalid["tables"]["workflow_ticks"].append(copy.deepcopy(tick_row))
    invalid["tables"]["turn_metrics"].append(copy.deepcopy(metric_row))
    target = WorkflowStore(tmp_path / "duplicate-invalidation-target.sqlite3")
    target.set_meta("last_game_id", proposal.game_session_id)
    before = target.export_replay_state(proposal.game_session_id)

    with pytest.raises(ValueError, match="duplicate replay primary key"):
        target.import_replay_state(invalid)

    assert target.export_replay_state(proposal.game_session_id) == before


def test_startup_rejects_noncanonical_migration_invalidation_time(tmp_path):
    store, request, proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "startup-migration-source.sqlite3")
    )
    assert store.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(store, _Game(), _Planner()).tick())
    state, invalidated = _migration_invalidated_replay(
        store, request, proposal, attempt
    )
    path = tmp_path / "startup-migration.sqlite3"
    restored = WorkflowStore(path)
    restored.import_replay_state(state)
    forged = invalidated.model_copy(
        update={
            "started_at": invalidated.started_at + timedelta(microseconds=1),
            "completed_at": invalidated.completed_at + timedelta(microseconds=1),
        }
    )

    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE workflow_ticks
            SET started_at=?, completed_at=?, tick_json=?
            WHERE tick_id=?
            """,
            (
                forged.started_at.isoformat(),
                forged.completed_at.isoformat(),
                forged.model_dump_json(),
                forged.tick_id,
            ),
        )

    with pytest.raises(ValueError, match="canonical causal time"):
        WorkflowStore(path)


def test_replay_rejects_incomplete_rejected_terminal_before_delete(tmp_path):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "incomplete-source.sqlite3")
    )
    invalid, _approval, rejected = _rejected_terminal_replay(source, proposal)
    invalid["tables"]["workflow_ticks"] = [
        row
        for row in invalid["tables"]["workflow_ticks"]
        if row["tick_id"] != rejected.tick_id
    ]
    invalid["tables"]["turn_metrics"] = [
        row
        for row in invalid["tables"]["turn_metrics"]
        if row["tick_id"] != rejected.tick_id
    ]
    target = WorkflowStore(tmp_path / "incomplete-target.sqlite3")
    target.set_meta("last_game_id", proposal.game_session_id)
    before = target.export_replay_state(proposal.game_session_id)

    with pytest.raises(ValueError, match="Proposal"):
        target.import_replay_state(invalid)

    assert target.export_replay_state(proposal.game_session_id) == before


@pytest.mark.parametrize(
    "entrypoint",
    [
        "save_workflow_tick",
        "persist_tick_and_runtime_state",
        "persist_phase4_tick",
    ],
)
def test_public_store_rejects_strategic_terminal_facts(tmp_path, entrypoint):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "public-terminal.sqlite3")
    )
    _state, approval, rejected = _rejected_terminal_replay(source, proposal)
    before = source.export_replay_state(proposal.game_session_id)

    with pytest.raises(ValueError, match="aggregate decision transaction"):
        source.save_approval_record(proposal.game_session_id, approval)
    with pytest.raises(ValueError, match="aggregate decision transaction"):
        getattr(source, entrypoint)(rejected)

    assert source.export_replay_state(proposal.game_session_id) == before


@pytest.mark.parametrize(
    "forgery",
    ["proposal_hash", "strategic_objectives", "extra_mission"],
)
def test_replay_rejects_forged_approved_contract_content_before_delete(
    tmp_path, forgery
):
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / f"approved-forgery-{forgery}.sqlite3")
    )
    invalid, _approval, commit, _applied = _approved_terminal_replay(source, proposal)
    revision_row = invalid["tables"]["strategic_contract_revisions"][0]
    commit_row = invalid["tables"]["strategic_contract_commits"][0]
    if forgery == "proposal_hash":
        forged_commit = commit.model_copy(update={"source_proposal_hash": "0" * 64})
    else:
        contract = commit.contract
        if forgery == "strategic_objectives":
            forged_contract = contract.model_copy(
                update={"strategic_objectives": ("forged objective",)}
            )
        else:
            extra = proposal.proposed_research_mission.model_copy(
                update={
                    "mission_id": "mission-z-extra",
                    "objective": "extra research",
                }
            )
            forged_contract = contract.model_copy(
                update={
                    "mission_graph": MissionGraph(
                        missions=(
                            proposal.proposed_research_mission,
                            extra,
                        )
                    )
                }
            )
        forged_commit = commit.model_copy(update={"contract": forged_contract})
        revision_row["contract_json"] = _canonical_model_json(forged_contract)
    commit_row["commit_json"] = _canonical_model_json(forged_commit)
    target = WorkflowStore(tmp_path / f"approved-forgery-{forgery}-target.sqlite3")
    target.set_meta("last_game_id", proposal.game_session_id)
    before = target.export_replay_state(proposal.game_session_id)

    with pytest.raises(ValueError, match="APPROVED Proposal"):
        target.import_replay_state(invalid)

    assert target.export_replay_state(proposal.game_session_id) == before


@pytest.mark.parametrize("missing", ["resume_request", "resumed_tick"])
def test_replay_rejects_migration_invalidation_missing_source_before_delete(
    tmp_path, missing
):
    store, request, proposal, attempt = asyncio.run(
        _completed_proposal_state(tmp_path / f"migration-missing-{missing}.sqlite3")
    )
    assert store.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(store, _Game(), _Planner()).tick())
    invalid, invalidated = _migration_invalidated_replay(
        store, request, proposal, attempt
    )
    if missing == "resume_request":
        invalid["tables"]["strategic_proposal_wait_resume_requests"] = []
    else:
        invalid["tables"]["workflow_ticks"] = [
            row
            for row in invalid["tables"]["workflow_ticks"]
            if row["tick_id"] != invalidated.source_wait_resumed_tick_id
        ]
        invalid["tables"]["turn_metrics"] = [
            row
            for row in invalid["tables"]["turn_metrics"]
            if row["tick_id"] != invalidated.source_wait_resumed_tick_id
        ]
    target = WorkflowStore(tmp_path / f"migration-missing-{missing}-target.sqlite3")
    target.set_meta("last_game_id", proposal.game_session_id)
    before = target.export_replay_state(proposal.game_session_id)

    with pytest.raises(ValueError):
        target.import_replay_state(invalid)

    assert target.export_replay_state(proposal.game_session_id) == before


def _proposal_decision(
    store, proposal, decision, *, suffix="primary", actor="phase1c-test"
):
    ready = _proposal_ready_tick(store, proposal.proposal_id)
    return StrategicProposalApprovalRecord(
        approval_id=f"approval-{proposal.proposal_id}-{suffix}",
        proposal_id=proposal.proposal_id,
        decision=decision,
        actor=actor,
        created_at=ready.completed_at + timedelta(seconds=1),
        reason=f"{decision.value.lower()} research Proposal",
    )


def _dormant_store(store):
    store._phase1c_dormant_activation_enabled = True
    return store


def test_phase1c_atomic_decisions_are_dormant_by_default(tmp_path):
    store, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "dormant-gate.sqlite3")
    )
    approval = _proposal_decision(store, proposal, ApprovalDecision.APPROVED)

    with pytest.raises(ValueError, match="remain dormant"):
        store.approve_strategic_research_proposal(proposal.game_session_id, approval)

    assert (
        store.strategic_proposal_approval_record(
            proposal.game_session_id, proposal.proposal_id
        )
        is None
    )
    assert store.get_active_strategic_contract(proposal.game_session_id) is None


def test_dormant_approval_atomically_activates_research_and_is_idempotent(tmp_path):
    store, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "atomic-approval.sqlite3")
    )
    approval = _proposal_decision(store, proposal, ApprovalDecision.APPROVED)
    store = _dormant_store(store)

    applied = store.approve_strategic_research_proposal(
        proposal.game_session_id, approval
    )
    repeated = store.approve_strategic_research_proposal(
        proposal.game_session_id, approval
    )

    assert repeated == applied
    assert isinstance(applied, StrategicProposalAppliedTick)
    assert (
        store.strategic_proposal_approval_record(
            proposal.game_session_id, proposal.proposal_id
        )
        == approval
    )
    active = store.get_active_strategic_contract(proposal.game_session_id)
    assert active is not None
    assert active.revision == proposal.expected_base_revision + 1
    assert active.authority_scope_set.mission_graph_scopes == ("research",)
    assert active.mission_graph.missions == (proposal.proposed_research_mission,)
    assert store.list_tasks(proposal.game_session_id) == []
    assert store.load_runtime_state(proposal.game_session_id) is RuntimeState.ROUTING
    assert store.human_wait_context(proposal.game_session_id) is None
    assert len(store.list_strategic_contract_revisions(proposal.game_session_id)) == 1
    assert len(store.list_strategic_contract_commits(proposal.game_session_id)) == 1

    WorkflowStore(store.path)
    replay = store.export_replay_state(proposal.game_session_id)
    restored = WorkflowStore(tmp_path / "atomic-approval-restored.sqlite3")
    restored.import_replay_state(replay)
    assert restored.export_replay_state(proposal.game_session_id) == replay


def test_active_research_change_commits_local_mission_patch_and_replays(tmp_path):
    async def scenario():
        path = tmp_path / "phase2-repair.sqlite3"
        store, _request_record, proposal, _attempt = await _completed_proposal_state(
            path
        )
        store = _dormant_store(store)
        store.approve_strategic_research_proposal(
            proposal.game_session_id,
            _proposal_decision(store, proposal, ApprovalDecision.APPROVED),
        )
        active = store.get_active_strategic_contract(proposal.game_session_id)
        assert active is not None

        game = _Game(proposal.game_session_id)
        game.snapshot = game.snapshot.model_copy(
            update={
                "tech_civics": {
                    "current_research_type": "TECH_WRITING",
                    "available_techs": [
                        {"tech_type": "TECH_WRITING"},
                        {"tech_type": "TECH_MINING"},
                    ],
                }
            }
        )
        planner = _Planner()
        engine = _engine(store, game, planner)
        runtime_clock = [
            store.list_strategic_contract_commits(proposal.game_session_id)[
                -1
            ].committed_at
            + timedelta(seconds=10)
        ]

        def next_runtime_time():
            runtime_clock[0] += timedelta(milliseconds=1)
            return runtime_clock[0]

        engine._now = next_runtime_time

        await engine.tick()
        baseline = store.get_accepted_observation_baseline(proposal.game_session_id)
        assert baseline is not None
        assert planner.calls == 0

        engine.config.execution_mode = ExecutionMode.READONLY
        game.snapshot = game.snapshot.model_copy(
            update={
                "turn": 2,
                "overview": {"turn": 2, "player_id": 1, "num_cities": 1},
                "cities": [
                    {
                        "city_id": 1,
                        "currently_building": "BUILDING_MONUMENT",
                    }
                ],
            }
        )
        await engine.tick()
        unrelated_baseline = store.get_accepted_observation_baseline(
            proposal.game_session_id
        )
        assert unrelated_baseline is not None
        assert unrelated_baseline.observation_id != baseline.observation_id
        assert planner.calls == 0

        game.snapshot = game.snapshot.model_copy(
            update={
                "turn": 3,
                "overview": {"turn": 3, "player_id": 1, "num_cities": 1},
                "tech_civics": {
                    "current_research_type": "TECH_MINING",
                    "available_techs": [
                        {"tech_type": "TECH_WRITING"},
                        {"tech_type": "TECH_MINING"},
                    ],
                },
            }
        )
        request_tick = await engine.tick()
        assert (
            request_tick.workflow_tick["outcome"]
            == TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
        )
        repair_request = store.active_planner_request(proposal.game_session_id)
        assert repair_request is not None
        assert (
            repair_request.target.kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR
        )
        assert planner.calls == 0
        assert (
            store.get_accepted_observation_baseline(proposal.game_session_id)
            == unrelated_baseline
        )

        prior_mission = active.mission_graph.missions[0]
        repaired_mission = prior_mission.model_copy(
            update={
                "mission_revision": prior_mission.mission_revision + 1,
                "objective": "Continue Mining",
                "desired_outcome": {"technology": "TECH_MINING"},
                "evidence_refs": (f"observation:{repair_request.observation_id}",),
            }
        )
        planner.responses.append(
            MissionGraphPatchResponse(
                schema_version="mission-graph-patch-response/v1",
                patch_candidates=(
                    MissionGraphPatchCandidate(
                        mission_updates=(repaired_mission,),
                        created_from_observation_id=repair_request.observation_id,
                    ),
                ),
            )
        )
        patch_tick = await engine.tick()

        persisted_tick = store.list_workflow_ticks(proposal.game_session_id)[-1]
        assert isinstance(persisted_tick, MissionGraphPatchedTick), (
            persisted_tick.blocking_reason
        )
        assert (
            patch_tick.workflow_tick["outcome"] == TickOutcomeKind.MISSION_GRAPH_PATCHED
        )
        assert planner.calls == 1
        updated = store.get_active_strategic_contract(proposal.game_session_id)
        assert updated is not None
        assert updated.revision == active.revision + 1
        assert updated.mission_graph.missions == (repaired_mission,)
        assert len(store.list_mission_graph_patches(proposal.game_session_id)) == 1
        assert (
            store.get_accepted_observation_baseline(
                proposal.game_session_id
            ).observation_id
            == repair_request.observation_id
        )
        patch = store.list_mission_graph_patches(proposal.game_session_id)[0]
        completed_request = store.get_planner_request(repair_request.planner_request_id)
        final_attempt = store.list_provider_attempts(repair_request.planner_request_id)[
            -1
        ]
        patch_commit = store.list_strategic_contract_commits(proposal.game_session_id)[
            -1
        ]
        repeated = store.apply_mission_graph_patch(
            tick=persisted_tick,
            planner_request=completed_request,
            provider_attempt=final_attempt,
            patch=patch,
            commit=patch_commit,
        )
        assert repeated == updated
        assert (
            len(store.list_strategic_contract_revisions(proposal.game_session_id)) == 2
        )
        assert planner.calls == 1

        replay = store.export_replay_state(proposal.game_session_id)
        restored = WorkflowStore(tmp_path / "phase2-repair-restored.sqlite3")
        restored.import_replay_state(replay)
        assert restored.export_replay_state(proposal.game_session_id) == replay
        assert (
            restored.get_active_strategic_contract(proposal.game_session_id) == updated
        )

        invalid_replay = copy.deepcopy(replay)
        invalid_replay["tables"]["mission_graph_patches"] = []
        target = WorkflowStore(tmp_path / "phase2-repair-target.sqlite3")
        target.set_meta("last_game_id", proposal.game_session_id)
        before = target.export_replay_state(proposal.game_session_id)
        with pytest.raises(ValueError, match="MissionGraphPatch"):
            target.import_replay_state(invalid_replay)
        assert target.export_replay_state(proposal.game_session_id) == before

        with sqlite3.connect(restored.path) as conn:
            conn.execute("DELETE FROM mission_graph_patches")
        with pytest.raises(ValueError, match="MissionGraphPatch"):
            WorkflowStore(restored.path)

    asyncio.run(scenario())


def test_active_civic_change_commits_local_mission_patch_and_replays(tmp_path):
    async def scenario():
        path = tmp_path / "phase5-civic-repair.sqlite3"
        store = WorkflowStore(path)
        base = _commit_contract(store, "game-1")
        game = _Game()
        game.snapshot = game.snapshot.model_copy(
            update={
                "tech_civics": {
                    "current_research_type": "TECH_WRITING",
                    "available_techs": [{"tech_type": "TECH_WRITING"}],
                    "current_civic_type": "CIVIC_CODE_OF_LAWS",
                    "available_civics": [
                        {"civic_type": "CIVIC_CODE_OF_LAWS"},
                        {"civic_type": "CIVIC_CRAFTSMANSHIP"},
                    ],
                }
            }
        )
        activation_observation = normalize_runtime_snapshot(game.snapshot).canonical
        store.save_normalized_observation(activation_observation)
        civic_mission = Mission(
            mission_id="mission-civic-opening",
            game_session_id="game-1",
            contract_id=base.contract_id,
            mission_revision=1,
            scope="civic",
            subject=SubjectRef(subject_type="player", subject_id="player-1"),
            slot="player:civic",
            objective="Complete Code of Laws",
            desired_outcome={"civic": "CIVIC_CODE_OF_LAWS"},
            status=MissionStatus.ACTIVE,
        )
        activated, _activation_tick = store.activate_civic_authority(
            game_session_id="game-1",
            expected_base_revision=base.revision,
            mission=civic_mission,
            activation_id="activate-civic-repair-test",
            observation_id=activation_observation.observation_id,
            turn_number=game.snapshot.turn,
            activated_at=activation_observation.observed_at + timedelta(seconds=1),
        )
        planner = _Planner()
        engine = _engine(store, game, planner)
        engine.config.execution_mode = ExecutionMode.READONLY
        runtime_clock = [activation_observation.observed_at + timedelta(seconds=10)]

        def next_runtime_time():
            runtime_clock[0] += timedelta(milliseconds=1)
            return runtime_clock[0]

        engine._now = next_runtime_time
        await engine.tick()
        baseline = store.get_accepted_observation_baseline("game-1")
        assert baseline is not None
        assert planner.calls == 0

        game.snapshot = game.snapshot.model_copy(
            update={
                "turn": 2,
                "overview": {"turn": 2, "player_id": 1, "num_cities": 1},
                "tech_civics": {
                    "current_research_type": "TECH_WRITING",
                    "available_techs": [{"tech_type": "TECH_WRITING"}],
                    "current_civic_type": "CIVIC_CRAFTSMANSHIP",
                    "available_civics": [
                        {"civic_type": "CIVIC_CRAFTSMANSHIP"},
                        {"civic_type": "CIVIC_FOREIGN_TRADE"},
                    ],
                },
            }
        )
        request_tick = await engine.tick()
        assert (
            request_tick.workflow_tick["outcome"]
            == TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
        ), request_tick.workflow_tick
        request = store.active_planner_request("game-1")
        assert request is not None
        assert request.target.kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR
        assert request.target.strategic_scope == "civic"
        assert request.target.affected_mission_ids == (civic_mission.mission_id,)
        assert planner.calls == 0
        assert store.get_accepted_observation_baseline("game-1") == baseline

        repaired_mission = civic_mission.model_copy(
            update={
                "mission_revision": 2,
                "objective": "Continue Craftsmanship",
                "desired_outcome": {"civic": "CIVIC_CRAFTSMANSHIP"},
                "evidence_refs": (f"observation:{request.observation_id}",),
            }
        )
        planner.responses.append(
            MissionGraphPatchResponse(
                schema_version="mission-graph-patch-response/v1",
                patch_candidates=(
                    MissionGraphPatchCandidate(
                        mission_updates=(repaired_mission,),
                        created_from_observation_id=request.observation_id,
                    ),
                ),
            )
        )
        patch_tick = await engine.tick()

        assert (
            patch_tick.workflow_tick["outcome"] == TickOutcomeKind.MISSION_GRAPH_PATCHED
        )
        assert planner.calls == 1
        updated = store.get_active_strategic_contract("game-1")
        assert updated is not None
        assert updated.revision == activated.revision + 1
        assert updated.authority_scope_set.mission_graph_scopes == ("civic",)
        assert updated.mission_graph.missions == (repaired_mission,)
        replay = store.export_replay_state("game-1")
        restored = WorkflowStore(tmp_path / "phase5-civic-repair-restored.sqlite3")
        restored.import_replay_state(replay)
        assert restored.export_replay_state("game-1") == replay

    asyncio.run(scenario())


def test_mission_graph_patch_transaction_rolls_back_before_completion_tick(tmp_path):
    async def scenario():
        path = tmp_path / "phase2-repair-crash.sqlite3"
        store, _request_record, proposal, _attempt = await _completed_proposal_state(
            path
        )
        store = _dormant_store(store)
        store.approve_strategic_research_proposal(
            proposal.game_session_id,
            _proposal_decision(store, proposal, ApprovalDecision.APPROVED),
        )
        active = store.get_active_strategic_contract(proposal.game_session_id)
        assert active is not None

        game = _Game(proposal.game_session_id)
        game.snapshot = game.snapshot.model_copy(
            update={
                "tech_civics": {
                    "current_research_type": "TECH_WRITING",
                    "available_techs": [
                        {"tech_type": "TECH_WRITING"},
                        {"tech_type": "TECH_MINING"},
                    ],
                }
            }
        )
        planner = _Planner()
        engine = _engine(store, game, planner)
        runtime_clock = [
            store.list_strategic_contract_commits(proposal.game_session_id)[
                -1
            ].committed_at
            + timedelta(seconds=10)
        ]

        def next_runtime_time():
            runtime_clock[0] += timedelta(milliseconds=1)
            return runtime_clock[0]

        engine._now = next_runtime_time
        engine.config.execution_mode = ExecutionMode.READONLY
        await engine.tick()
        baseline = store.get_accepted_observation_baseline(proposal.game_session_id)
        assert baseline is not None

        game.snapshot = game.snapshot.model_copy(
            update={
                "turn": 2,
                "overview": {"turn": 2, "player_id": 1, "num_cities": 1},
                "tech_civics": {
                    "current_research_type": "TECH_MINING",
                    "available_techs": [
                        {"tech_type": "TECH_WRITING"},
                        {"tech_type": "TECH_MINING"},
                    ],
                },
            }
        )
        await engine.tick()
        request = store.active_planner_request(proposal.game_session_id)
        assert request is not None
        repaired_mission = active.mission_graph.missions[0].model_copy(
            update={
                "mission_revision": 2,
                "objective": "Continue Mining",
                "desired_outcome": {"technology": "TECH_MINING"},
            }
        )
        planner.responses.append(
            MissionGraphPatchResponse(
                schema_version="mission-graph-patch-response/v1",
                patch_candidates=(
                    MissionGraphPatchCandidate(
                        mission_updates=(repaired_mission,),
                        created_from_observation_id=request.observation_id,
                    ),
                ),
            )
        )

        class CrashBeforeTick:
            @staticmethod
            def checkpoint(name):
                if name == "before_mission_patch_tick":
                    raise RuntimeError(name)

        engine.crash_injector = CrashBeforeTick()
        with pytest.raises(InjectedCrashBoundary, match="before_mission_patch_tick"):
            await engine.tick()

        assert store.get_active_strategic_contract(proposal.game_session_id) == active
        assert store.list_mission_graph_patches(proposal.game_session_id) == []
        assert (
            store.get_accepted_observation_baseline(proposal.game_session_id)
            == baseline
        )
        assert not any(
            isinstance(tick, MissionGraphPatchedTick)
            for tick in store.list_workflow_ticks(proposal.game_session_id)
        )
        in_progress = store.get_planner_request(request.planner_request_id)
        assert in_progress is not None
        assert in_progress.status is PlannerRequestStatus.IN_PROGRESS
        assert (
            store.list_provider_attempts(request.planner_request_id)[-1].status
            is ProviderAttemptStatus.STARTED
        )
        WorkflowStore(path)

    asyncio.run(scenario())


def test_dormant_rejection_is_atomic_idempotent_and_conflicts_with_approval(tmp_path):
    store, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "atomic-rejection.sqlite3")
    )
    rejection = _proposal_decision(store, proposal, ApprovalDecision.REJECTED)
    approval = rejection.model_copy(
        update={
            "approval_id": f"approval-{proposal.proposal_id}-conflict",
            "decision": ApprovalDecision.APPROVED,
        }
    )
    store = _dormant_store(store)

    rejected = store.reject_strategic_research_proposal(
        proposal.game_session_id, rejection
    )
    assert (
        store.reject_strategic_research_proposal(proposal.game_session_id, rejection)
        == rejected
    )
    with pytest.raises(ValueError, match="conflicts"):
        store.approve_strategic_research_proposal(proposal.game_session_id, approval)

    assert isinstance(rejected, StrategicProposalRejectedTick)
    assert store.get_active_strategic_contract(proposal.game_session_id) is None
    assert store.load_runtime_state(proposal.game_session_id) is RuntimeState.ROUTING
    assert store.human_wait_context(proposal.game_session_id) is None


def test_stale_approval_becomes_system_invalidation_without_human_decision(tmp_path):
    store, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "stale-approval.sqlite3")
    )
    approval = _proposal_decision(store, proposal, ApprovalDecision.APPROVED)
    _commit_contract(store, proposal.game_session_id)
    store = _dormant_store(store)

    invalidated = store.approve_strategic_research_proposal(
        proposal.game_session_id, approval
    )
    repeated = store.invalidate_stale_strategic_research_proposal(
        proposal.game_session_id, proposal.proposal_id
    )
    repeated_approval = store.approve_strategic_research_proposal(
        proposal.game_session_id, approval
    )

    assert repeated == invalidated
    assert repeated_approval == invalidated
    assert isinstance(invalidated, StrategicProposalInvalidatedTick)
    assert invalidated.invalidation_reason is (
        StrategicProposalInvalidationReason.TARGET_CONTRACT_CREATED
    )
    assert (
        store.strategic_proposal_approval_record(
            proposal.game_session_id, proposal.proposal_id
        )
        is None
    )
    assert store.get_active_strategic_contract(proposal.game_session_id).revision == 1


@pytest.mark.parametrize(
    "checkpoint",
    [
        "after_legacy_research_disposition",
        "after_strategic_approval",
        "after_strategic_contract_activation",
        "before_strategic_applied_tick",
    ],
)
def test_dormant_approval_crash_points_roll_back_complete_aggregate(
    tmp_path, checkpoint
):
    store, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / f"approval-crash-{checkpoint}.sqlite3")
    )
    approval = _proposal_decision(store, proposal, ApprovalDecision.APPROVED)
    store = _dormant_store(store)
    before = store.export_replay_state(proposal.game_session_id)

    def crash(point):
        if point == checkpoint:
            raise RuntimeError(point)

    with pytest.raises(RuntimeError, match=checkpoint):
        store.approve_strategic_research_proposal(
            proposal.game_session_id, approval, checkpoint=crash
        )

    assert store.export_replay_state(proposal.game_session_id) == before
    WorkflowStore(store.path)


def test_two_concurrent_identical_approvals_commit_one_revision(tmp_path):
    store, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "concurrent-approval.sqlite3")
    )
    store = _dormant_store(store)
    approval = _proposal_decision(store, proposal, ApprovalDecision.APPROVED)
    barrier = threading.Barrier(3)
    results = []
    errors = []

    def decide():
        barrier.wait()
        try:
            results.append(
                store.approve_strategic_research_proposal(
                    proposal.game_session_id, approval
                )
            )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=decide) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(results) == 2 and results[0] == results[1]
    assert len(store.list_strategic_contract_revisions(proposal.game_session_id)) == 1
    assert len(store.list_strategic_contract_commits(proposal.game_session_id)) == 1


def test_concurrent_approve_reject_race_persists_one_terminal_authority(tmp_path):
    store, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / "approve-reject-race.sqlite3")
    )
    store = _dormant_store(store)
    approval = _proposal_decision(store, proposal, ApprovalDecision.APPROVED)
    rejection = approval.model_copy(
        update={
            "approval_id": f"approval-{proposal.proposal_id}-reject-race",
            "decision": ApprovalDecision.REJECTED,
        }
    )
    barrier = threading.Barrier(3)
    results = []
    errors = []

    def decide(record):
        barrier.wait()
        try:
            operation = (
                store.approve_strategic_research_proposal
                if record.decision is ApprovalDecision.APPROVED
                else store.reject_strategic_research_proposal
            )
            results.append(operation(proposal.game_session_id, record))
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=decide, args=(approval,)),
        threading.Thread(target=decide, args=(rejection,)),
    ]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    terminal = [
        tick
        for tick in store.list_workflow_ticks(proposal.game_session_id)
        if isinstance(
            tick,
            (
                StrategicProposalAppliedTick,
                StrategicProposalRejectedTick,
                StrategicProposalInvalidatedTick,
            ),
        )
    ]
    assert terminal == results
    assert len(
        store.list_strategic_contract_revisions(proposal.game_session_id)
    ) == int(isinstance(results[0], StrategicProposalAppliedTick))


def test_phase1c_enablement_migrates_released_wait_deterministically(tmp_path):
    path = tmp_path / "phase1c-enable-released-wait.sqlite3"
    store, request, proposal, attempt = asyncio.run(_completed_proposal_state(path))
    assert store.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(store, _Game(), _Planner()).tick())
    resumed = next(
        tick
        for tick in store.list_workflow_ticks(proposal.game_session_id)
        if isinstance(tick, StrategicProposalWaitResumedTick)
    )
    ready = _proposal_ready_tick(store, proposal.proposal_id)
    resume_request = store.strategic_proposal_wait_resume_request_for_proposal(
        proposal.proposal_id
    )
    assert resume_request is not None
    assert request.completed_at is not None
    assert attempt.completed_at is not None
    expected_time = max(
        proposal.created_at,
        request.completed_at,
        attempt.completed_at,
        ready.completed_at,
        resume_request.requested_at,
        resumed.completed_at,
    ) + timedelta(microseconds=1)

    enabled = WorkflowStore(path, enable_phase1c_decisions=True)

    invalidated = next(
        tick
        for tick in enabled.list_workflow_ticks(proposal.game_session_id)
        if isinstance(tick, StrategicProposalInvalidatedTick)
    )
    assert invalidated.invalidation_origin is (
        StrategicProposalInvalidationOrigin.PHASE1C_ENABLEMENT_MIGRATION
    )
    assert invalidated.invalidation_reason is (
        StrategicProposalInvalidationReason.PRE_PHASE1C_WAIT_RELEASED
    )
    assert invalidated.started_at == invalidated.completed_at == expected_time
    assert invalidated.source_resume_request_id == resume_request.resume_request_id
    assert invalidated.source_wait_resumed_tick_id == resumed.tick_id
    assert (
        enabled.strategic_proposal_approval_record(
            proposal.game_session_id, proposal.proposal_id
        )
        is None
    )
    assert enabled.get_active_strategic_contract(proposal.game_session_id) is None
    assert enabled.list_tasks(proposal.game_session_id) == []

    first = enabled.export_replay_state(proposal.game_session_id)
    reopened = WorkflowStore(path, enable_phase1c_decisions=True)
    assert reopened.export_replay_state(proposal.game_session_id) == first
    restored = WorkflowStore(
        tmp_path / "phase1c-enable-released-wait-restored.sqlite3",
        enable_phase1c_decisions=True,
    )
    restored.import_replay_state(first)
    assert restored.export_replay_state(proposal.game_session_id) == first


def test_phase1c_enablement_preserves_unresolved_wait_and_rejects_generic_resume(
    tmp_path,
):
    path = tmp_path / "phase1c-enable-unresolved-wait.sqlite3"
    store, _request, proposal, _attempt = asyncio.run(_completed_proposal_state(path))
    before = store.export_replay_state(proposal.game_session_id)

    enabled = WorkflowStore(path, enable_phase1c_decisions=True)

    assert enabled.export_replay_state(proposal.game_session_id) == before
    assert enabled.load_runtime_state(proposal.game_session_id) is (
        RuntimeState.AWAITING_HUMAN
    )
    context = enabled.human_wait_context(proposal.game_session_id)
    assert context is not None
    assert context["proposal_id"] == proposal.proposal_id
    with pytest.raises(ValueError, match="require APPROVE or REJECT"):
        enabled.request_human_resume(proposal.game_session_id)
    assert enabled.export_replay_state(proposal.game_session_id) == before


def test_phase1c_enabled_replay_migrates_released_wait_before_commit(tmp_path):
    source_path = tmp_path / "phase1c-enable-replay-source.sqlite3"
    source, _request, proposal, _attempt = asyncio.run(
        _completed_proposal_state(source_path)
    )
    assert source.request_human_resume(proposal.game_session_id) is True
    asyncio.run(_engine(source, _Game(), _Planner()).tick())
    pre_enable = source.export_replay_state(proposal.game_session_id)
    target = WorkflowStore(
        tmp_path / "phase1c-enable-replay-target.sqlite3",
        enable_phase1c_decisions=True,
    )

    target.import_replay_state(pre_enable)

    terminal = [
        tick
        for tick in target.list_workflow_ticks(proposal.game_session_id)
        if isinstance(tick, StrategicProposalInvalidatedTick)
    ]
    assert len(terminal) == 1
    WorkflowStore(target.path, enable_phase1c_decisions=True)


def _research_ready_game(game_id="game-1"):
    game = _Game(game_id)
    game.snapshot = game.snapshot.model_copy(
        update={
            "tech_civics": {
                "current_research": "None",
                "current_civic": "None",
                "available_techs": [{"name": "Writing", "tech_type": "TECH_WRITING"}],
                "available_civics": [
                    {
                        "name": "Code of Laws",
                        "civic_type": "CIVIC_CODE_OF_LAWS",
                    }
                ],
            }
        }
    )
    return game


def test_phase1c_enabled_engine_does_not_consume_pending_legacy_resume(tmp_path):
    path = tmp_path / "phase1c-pending-resume.sqlite3"
    store, _request, proposal, _attempt = asyncio.run(_completed_proposal_state(path))
    assert store.request_human_resume(proposal.game_session_id) is True
    enabled = WorkflowStore(path, enable_phase1c_decisions=True)
    planner = _Planner()

    result = asyncio.run(_engine(enabled, _research_ready_game(), planner).tick())

    assert result.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
    assert planner.calls == 0
    assert not any(
        isinstance(tick, StrategicProposalWaitResumedTick)
        for tick in enabled.list_workflow_ticks(proposal.game_session_id)
    )
    wait = enabled.human_wait_context(proposal.game_session_id)
    assert wait is not None
    assert wait["proposal_id"] == proposal.proposal_id
    assert wait["resume_requested"] is True

    applied = enabled.approve_strategic_research_proposal(
        proposal.game_session_id,
        _proposal_decision(enabled, proposal, ApprovalDecision.APPROVED),
    )
    assert isinstance(applied, StrategicProposalAppliedTick)


def test_turn_action_graph_verification_commits_mission_completion_once(tmp_path):
    path = tmp_path / "phase3-turn-action-graph.sqlite3"
    store, _request, proposal, _attempt = asyncio.run(_completed_proposal_state(path))
    enabled = WorkflowStore(path, enable_phase1c_decisions=True)
    ready = _proposal_ready_tick(enabled, proposal.proposal_id)
    approval = _proposal_decision(
        enabled, proposal, ApprovalDecision.APPROVED
    ).model_copy(update={"created_at": max(datetime.now(UTC), ready.completed_at)})
    applied = enabled.approve_strategic_research_proposal(
        proposal.game_session_id,
        approval,
    )

    class ExecutingResearchGame(_Game):
        async def execute_task(self, task):
            assert task.action_type == "set_research"
            assert task.arguments == {"tech_or_civic": "TECH_WRITING"}
            return ActionResult(
                success=True,
                delivery_status=MutationDeliveryStatus.ACKNOWLEDGED,
            )

    game = ExecutingResearchGame(proposal.game_session_id)
    game.snapshot = _research_ready_game(proposal.game_session_id).snapshot
    engine = _engine(enabled, game, _Planner())

    sent = asyncio.run(engine.tick())
    assert sent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
    graph_state = enabled.active_turn_action_graph(proposal.game_session_id)
    assert graph_state is not None
    graph, nodes = graph_state
    assert len(nodes) == 1
    assert nodes[0].action_type == "set_research"
    assert (
        enabled.due_turn_action_nodes(
            proposal.game_session_id,
            1,
            source_observation_id=graph.source_observation_id,
        )
        == []
    )

    game.snapshot = game.snapshot.model_copy(
        update={
            "tech_civics": {
                **game.snapshot.tech_civics,
                "current_research": "TECH_WRITING",
            }
        }
    )
    verified = asyncio.run(engine.tick())
    assert verified.workflow_tick["outcome"] == TickOutcomeKind.ATTEMPT_RECONCILED

    contract = enabled.get_active_strategic_contract(proposal.game_session_id)
    assert contract is not None
    assert contract.revision == applied.activated_contract_revision + 1
    mission = contract.mission_graph.missions[0]
    assert mission.status is MissionStatus.COMPLETED
    assert mission.mission_revision == applied.source_mission_revision + 1
    assert len(mission.evidence_refs) == 1
    assert enabled.active_turn_action_graph(proposal.game_session_id) is None
    assert (
        enabled.due_turn_action_nodes(
            proposal.game_session_id,
            1,
            source_observation_id=graph.source_observation_id,
        )
        == []
    )
    final_attempt = enabled.latest_attempt_for_task(
        proposal.game_session_id, nodes[0].task_id
    )
    assert final_attempt is not None
    final_tick = enabled.list_workflow_ticks(proposal.game_session_id)[-1]
    enabled.finalize_attempt_success(final_attempt, final_tick)
    assert (
        enabled.get_active_strategic_contract(proposal.game_session_id).revision
        == contract.revision
    )

    replay = enabled.export_replay_state(proposal.game_session_id)
    restored = WorkflowStore(
        tmp_path / "phase3-turn-action-graph-restored.sqlite3",
        enable_phase1c_decisions=True,
    )
    restored.import_replay_state(replay)
    assert restored.export_replay_state(proposal.game_session_id) == replay
    assert restored.get_active_strategic_contract(proposal.game_session_id) == contract
    before_tamper = restored.export_replay_state(proposal.game_session_id)
    tampered = copy.deepcopy(replay)
    tampered["tables"]["active_turn_action_graphs"] = [
        {
            "game_id": proposal.game_session_id,
            "graph_id": graph.graph_id,
            "activated_at": graph.compiled_at.isoformat(),
        }
    ]
    with pytest.raises(ValueError, match="sources are stale"):
        restored.import_replay_state(tampered)
    assert restored.export_replay_state(proposal.game_session_id) == before_tamper


def test_civic_turn_action_verification_commits_mission_completion_once(tmp_path):
    path = tmp_path / "phase5-civic-turn-action.sqlite3"
    store = WorkflowStore(path)
    base = _commit_contract(store, "game-1")

    class ExecutingCivicGame(_Game):
        async def execute_task(self, task):
            assert task.action_type == "set_civic"
            assert task.arguments == {"tech_or_civic": "CIVIC_CODE_OF_LAWS"}
            return ActionResult(
                success=True,
                delivery_status=MutationDeliveryStatus.ACKNOWLEDGED,
            )

    game = ExecutingCivicGame()
    game.snapshot = game.snapshot.model_copy(
        update={
            "tech_civics": {
                "current_research_type": "TECH_WRITING",
                "available_techs": [{"tech_type": "TECH_WRITING"}],
                "current_civic": None,
                "available_civics": [{"civic_type": "CIVIC_CODE_OF_LAWS"}],
            }
        }
    )
    observation = normalize_runtime_snapshot(game.snapshot).canonical
    store.save_normalized_observation(observation)
    mission = Mission(
        mission_id="mission-civic-code-of-laws",
        game_session_id="game-1",
        contract_id=base.contract_id,
        mission_revision=1,
        scope="civic",
        subject=SubjectRef(subject_type="player", subject_id="player-1"),
        slot="player:civic",
        objective="Select Code of Laws",
        desired_outcome={"civic": "CIVIC_CODE_OF_LAWS"},
        status=MissionStatus.ACTIVE,
    )
    activated, _activation_tick = store.activate_civic_authority(
        game_session_id="game-1",
        expected_base_revision=base.revision,
        mission=mission,
        activation_id="activate-civic-execution-test",
        observation_id=observation.observation_id,
        turn_number=game.snapshot.turn,
        activated_at=observation.observed_at + timedelta(seconds=1),
    )
    engine = _engine(store, game, _Planner())
    engine._now = lambda: observation.observed_at + timedelta(seconds=10)

    sent = asyncio.run(engine.tick())
    assert sent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT, (
        sent.workflow_tick
    )
    graph_state = store.active_turn_action_graph("game-1")
    assert graph_state is not None
    graph, nodes = graph_state
    assert [node.action_type for node in nodes] == ["set_civic"]

    game.snapshot = game.snapshot.model_copy(
        update={
            "tech_civics": {
                **game.snapshot.tech_civics,
                "current_civic": "CIVIC_CODE_OF_LAWS",
            }
        }
    )
    verified = asyncio.run(engine.tick())
    assert verified.workflow_tick["outcome"] == TickOutcomeKind.ATTEMPT_RECONCILED

    completed = store.get_active_strategic_contract("game-1")
    assert completed is not None
    assert completed.revision == activated.revision + 1
    completed_mission = completed.mission_graph.missions[0]
    assert completed_mission.status is MissionStatus.COMPLETED
    assert completed_mission.mission_revision == mission.mission_revision + 1
    assert store.active_turn_action_graph("game-1") is None
    replay = store.export_replay_state("game-1")
    restored = WorkflowStore(tmp_path / "phase5-civic-turn-action-restored.sqlite3")
    restored.import_replay_state(replay)
    assert restored.export_replay_state("game-1") == replay
    assert restored.get_active_strategic_contract("game-1") == completed


def test_turn_action_graph_approval_survives_equivalent_observation(tmp_path):
    path = tmp_path / "phase3-turn-action-approval.sqlite3"
    store, _request, proposal, _attempt = asyncio.run(_completed_proposal_state(path))
    enabled = WorkflowStore(path, enable_phase1c_decisions=True)
    ready = _proposal_ready_tick(enabled, proposal.proposal_id)
    approval = _proposal_decision(
        enabled, proposal, ApprovalDecision.APPROVED
    ).model_copy(update={"created_at": max(datetime.now(UTC), ready.completed_at)})
    enabled.approve_strategic_research_proposal(
        proposal.game_session_id,
        approval,
    )

    class ConfirmedResearchGame(_Game):
        calls = 0

        async def execute_task(self, task):
            self.calls += 1
            return ActionResult(
                success=True,
                delivery_status=MutationDeliveryStatus.ACKNOWLEDGED,
            )

    game = ConfirmedResearchGame(proposal.game_session_id)
    game.snapshot = _research_ready_game(proposal.game_session_id).snapshot
    engine = WorkflowRuntime(
        service_factory=build_runtime_services,
        store=enabled,
        game=game,
        planner=_Planner(),
        config=RuntimeConfig(
            execution_mode=ExecutionMode.CONFIRM,
            auto_end_turn=False,
            max_agent_calls_per_turn=0,
        ),
    )

    waiting = asyncio.run(engine.tick())
    assert waiting.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_APPROVAL
    graph_state = enabled.active_turn_action_graph(proposal.game_session_id)
    assert graph_state is not None
    graph, nodes = graph_state
    assert len(nodes) == 1
    assert game.calls == 0
    assert not hasattr(enabled, "save_authoritative_research_plan_bundle")
    assert enabled.approve_task(
        proposal.game_session_id, nodes[0].task_id, approved_by="operator"
    )

    sent = asyncio.run(engine.tick())
    assert sent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
    assert game.calls == 1
    current_graph = enabled.active_turn_action_graph(proposal.game_session_id)
    assert current_graph is not None
    assert current_graph[0].graph_id == graph.graph_id


def test_turn_change_expires_old_graph_before_any_node_can_be_claimed(tmp_path):
    path = tmp_path / "phase3-turn-barrier.sqlite3"
    store, _request, proposal, _attempt = asyncio.run(_completed_proposal_state(path))
    enabled = WorkflowStore(path, enable_phase1c_decisions=True)
    ready = _proposal_ready_tick(enabled, proposal.proposal_id)
    approval = _proposal_decision(
        enabled, proposal, ApprovalDecision.APPROVED
    ).model_copy(update={"created_at": max(datetime.now(UTC), ready.completed_at)})
    enabled.approve_strategic_research_proposal(
        proposal.game_session_id,
        approval,
    )
    game = _research_ready_game(proposal.game_session_id)
    engine = WorkflowRuntime(
        service_factory=build_runtime_services,
        store=enabled,
        game=game,
        planner=_Planner(),
        config=RuntimeConfig(
            execution_mode=ExecutionMode.CONFIRM,
            auto_end_turn=False,
            max_agent_calls_per_turn=0,
        ),
    )

    first = asyncio.run(engine.tick())
    assert first.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_APPROVAL
    old_graph, old_nodes = enabled.active_turn_action_graph(proposal.game_session_id)
    game.snapshot = game.snapshot.model_copy(
        update={
            "turn": 2,
            "overview": {**game.snapshot.overview, "turn": 2},
        }
    )
    second = asyncio.run(engine.tick())
    assert second.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_APPROVAL, (
        second.workflow_tick
    )
    new_graph, new_nodes = enabled.active_turn_action_graph(proposal.game_session_id)
    assert new_graph.turn_number == 2
    assert new_graph.graph_id != old_graph.graph_id
    assert old_nodes[0].task_id != new_nodes[0].task_id
    assert (
        enabled.task_status(proposal.game_session_id, old_nodes[0].task_id)
        is TaskStatus.EXPIRED
    )
    with pytest.raises(ValueError, match="not part of the active graph"):
        enabled.approve_task(
            proposal.game_session_id,
            old_nodes[0].task_id,
            approved_by="operator",
        )


@pytest.mark.parametrize("decision", [ApprovalDecision.REJECTED, "stale"])
def test_phase1c_nonapproval_never_switches_research_authority(tmp_path, decision):
    path = tmp_path / f"phase1c-no-activation-{decision}.sqlite3"
    store, _request, proposal, _attempt = asyncio.run(_completed_proposal_state(path))
    if decision == "stale":
        _commit_contract(store, proposal.game_session_id)
    enabled = WorkflowStore(path, enable_phase1c_decisions=True)

    if decision is ApprovalDecision.REJECTED:
        terminal = enabled.reject_strategic_research_proposal(
            proposal.game_session_id,
            _proposal_decision(enabled, proposal, ApprovalDecision.REJECTED),
        )
        assert isinstance(terminal, StrategicProposalRejectedTick)
    else:
        terminal = enabled.approve_strategic_research_proposal(
            proposal.game_session_id,
            _proposal_decision(enabled, proposal, ApprovalDecision.APPROVED),
        )
        assert isinstance(terminal, StrategicProposalInvalidatedTick)

    active = enabled.get_active_strategic_contract(proposal.game_session_id)
    if active is not None:
        assert active.authority_scope_set.mission_graph_scopes == ()
        assert active.mission_graph.missions == ()
    assert enabled.active_research_mission(proposal.game_session_id) is None
    assert enabled.list_tasks(proposal.game_session_id) == []


def test_control_panel_exposes_and_idempotently_decides_research_proposal(tmp_path):
    path = tmp_path / "phase1c-control-panel.sqlite3"
    store, _request, proposal, _attempt = asyncio.run(_completed_proposal_state(path))
    enabled = WorkflowStore(path, enable_phase1c_decisions=True)
    config = AppConfig.model_validate(
        {
            "runtime": {
                "database_path": str(path),
                "execution_mode": "confirm",
                "auto_end_turn": False,
            },
            "codex": {
                "backend": "responses",
                "model": "test-model",
                "api_key_env": "OPENAI_API_KEY",
            },
        }
    )
    panel = ControlPanelState(
        config=config,
        store=enabled,
        run_tick_callback=lambda: {"turn": 1},
        token="phase1c-token",
    )

    snapshot = panel.snapshot()
    exposed = snapshot["human_actions"]["strategic_proposal"]
    assert exposed["proposal_id"] == proposal.proposal_id
    assert exposed["research_mission"]["scope"] == "research"

    first = panel.decide_strategic_proposal(
        proposal.game_session_id, proposal.proposal_id, approved=True
    )
    repeated = panel.decide_strategic_proposal(
        proposal.game_session_id, proposal.proposal_id, approved=True
    )
    conflicting = panel.decide_strategic_proposal(
        proposal.game_session_id, proposal.proposal_id, approved=False
    )

    assert first[0] is True
    assert repeated[0] is True
    assert conflicting == (
        False,
        "strategic Proposal already has a conflicting decision",
    )
    assert len(enabled.list_strategic_contract_revisions(proposal.game_session_id)) == 1
    assert (
        len(
            [
                tick
                for tick in enabled.list_workflow_ticks(proposal.game_session_id)
                if isinstance(tick, StrategicProposalAppliedTick)
            ]
        )
        == 1
    )


def test_production_bootstrap_enables_phase1c_decisions(tmp_path):
    config_path = tmp_path / "config.toml"
    config = AppConfig.model_validate(
        {
            "runtime": {
                "database_path": "phase1c-bootstrap.sqlite3",
                "execution_mode": "confirm",
                "auto_end_turn": False,
            },
            "codex": {
                "backend": "responses",
                "model": "test-model",
                "api_key_env": "OPENAI_API_KEY",
            },
        }
    )

    store = build_store(config, config_path)

    assert store.phase1c_decisions_enabled is True
    assert store.path == tmp_path / "phase1c-bootstrap.sqlite3"


def test_phase1c_enablement_migrates_multiple_released_proposals_once(tmp_path):
    path = tmp_path / "phase1c-multiple-released.sqlite3"
    first, _request_one, proposal_one, _attempt_one = asyncio.run(
        _completed_proposal_state(
            path,
            game_id="game-1",
            planner_request_id="phase1c-request-one",
        )
    )
    second, _request_two, proposal_two, _attempt_two = asyncio.run(
        _completed_proposal_state(
            path,
            game_id="game-2",
            planner_request_id="phase1c-request-two",
        )
    )
    assert first.request_human_resume("game-1") is True
    assert second.request_human_resume("game-2") is True
    asyncio.run(_engine(first, _Game("game-1"), _Planner()).tick())
    asyncio.run(_engine(second, _Game("game-2"), _Planner()).tick())

    enabled = WorkflowStore(path, enable_phase1c_decisions=True)

    for proposal in (proposal_one, proposal_two):
        terminal = [
            tick
            for tick in enabled.list_workflow_ticks(proposal.game_session_id)
            if isinstance(tick, StrategicProposalInvalidatedTick)
            and tick.proposal_id == proposal.proposal_id
        ]
        assert len(terminal) == 1
        assert terminal[0].invalidation_origin is (
            StrategicProposalInvalidationOrigin.PHASE1C_ENABLEMENT_MIGRATION
        )
    first_state = {
        game_id: enabled.export_replay_state(game_id)
        for game_id in ("game-1", "game-2")
    }

    reopened = WorkflowStore(path, enable_phase1c_decisions=True)

    assert {
        game_id: reopened.export_replay_state(game_id)
        for game_id in ("game-1", "game-2")
    } == first_state


def test_phase1c_enablement_preserves_generic_human_resume(tmp_path):
    path = tmp_path / "phase1c-generic-resume.sqlite3"
    store = WorkflowStore(path, enable_phase1c_decisions=True)
    now = datetime.now(UTC)
    store.persist_tick_and_runtime_state(
        AwaitingHumanTick(
            tick_id="phase1c-generic-wait",
            game_session_id="game-generic",
            turn_number=1,
            starting_runtime_state=RuntimeState.OBSERVING,
            observation_ids=("obs-generic",),
            started_at=now,
            completed_at=now,
            blocking_reason="generic review",
        ),
        human_wait_context={
            "version": "human-wait/v1",
            "execution_mode": "confirm",
            "observation_projection_hash": "generic",
            "blocking_reason": "generic review",
            "resume_requested": False,
        },
    )

    assert store.request_human_resume("game-generic") is True
    context = store.human_wait_context("game-generic")
    assert context is not None
    assert context["resume_requested"] is True
