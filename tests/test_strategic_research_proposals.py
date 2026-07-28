import asyncio
import copy
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from civ6_workflow.domain import (
    AuthorityScopeSet,
    InformationRound,
    InformationRoundStatus,
    Mission,
    MissionGraph,
    ProviderAttempt,
    ProviderAttemptStatus,
    PlannerRequest,
    PlannerRequestStatus,
    PlannerRequestTarget,
    PlannerRequestTargetKind,
    RuntimeState,
    StrategicContract,
    StrategicContractCommit,
    StrategicProposalWaitResumedTick,
    SubjectRef,
    TickOutcomeKind,
    build_strategic_contract_id,
    build_strategic_proposal_wait_resume_request_id,
    build_strategic_research_proposal,
    canonical_json_hash,
)
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import ExecutionMode, RuntimeSnapshot
from civ6_workflow.store import WorkflowStore
from civ6_workflow.workflow_protocol import (
    InformationRequest,
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
        raise AssertionError("Proposal generation cannot execute StoredTask")

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
    return WorkflowEngine(
        store=store,
        game=game,
        planner=planner,
        config=EngineConfig(
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


def test_creation_uses_isolated_lifecycle_and_persists_proposal(tmp_path, monkeypatch):
    async def scenario():
        store = WorkflowStore(tmp_path / "creation.sqlite3")
        game = _Game()
        contract_id = build_strategic_contract_id("game-1")
        planner = _Planner(_response("game-1", contract_id))
        engine = _engine(store, game, planner)
        request = _request("game-1")
        store.save_planner_request(request)

        def legacy_path(*args, **kwargs):
            raise AssertionError("strategic target entered legacy lifecycle")

        for name in (
            "_supersede_stale_request",
            "_request_gaps",
            "_partition_bundle",
            "_resolve_gaps",
        ):
            monkeypatch.setattr(engine.planner_lifecycle, name, legacy_path)

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
        assert result.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
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

    with pytest.raises(ValueError, match="maximum Attempt"):
        store.save_provider_attempt("game-1", candidate)

    assert store.export_replay_state("game-1") == before
    assert store.list_provider_attempts(request.planner_request_id) == [attempt]


def test_public_request_save_rejects_completed_nonlegacy_without_proposal(tmp_path):
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
    store.save_provider_attempt("game-1", attempt)
    before = store.export_replay_state("game-1")

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
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 10


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


def _assert_invalid_replay_preserves_target(tmp_path, invalid, label):
    target, _request_record, _proposal, _attempt = asyncio.run(
        _completed_proposal_state(tmp_path / f"invalid-target-{label}.sqlite3")
    )
    before = target.export_replay_state("game-1")
    with pytest.raises(ValueError):
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
