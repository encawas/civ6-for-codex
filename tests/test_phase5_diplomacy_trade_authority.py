from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from civ6_workflow.bootstrap import build_runtime_services
from civ6_workflow.domain import (
    ApprovalStatus,
    AuthorityScopeSet,
    Condition,
    Mission,
    MissionGraph,
    MissionStatus,
    StrategicContract,
    StrategicContractCommit,
    SubjectRef,
    build_strategic_contract_id,
)
from civ6_workflow.domain.legacy_decisions import (
    DecisionGap,
    DecisionGapStatus,
    DecisionRoute,
)
from civ6_workflow.domain.legacy_plans import (
    ContinuationPolicy,
    LeaseValidationResult,
    PlanLease,
    PlanLeaseStatus,
)
from civ6_workflow.runtime import RuntimeConfig, WorkflowRuntime
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    MutationDeliveryStatus,
    RuntimeSnapshot,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore


NOW = datetime(2026, 7, 31, 18, 0, tzinfo=UTC)
GAME_ID = "phase5-diplomacy-trade"


class _Planner:
    def __init__(self):
        self.calls = 0

    async def plan(self, _request):
        self.calls += 1
        raise AssertionError("planner must not own diplomacy/trade after cutover")


class _Game:
    def __init__(self, snapshot: RuntimeSnapshot):
        self.snapshot = snapshot
        self.call_count = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        return self.snapshot.model_copy(deep=True)

    async def execute_task(self, _task):
        raise AssertionError("diplomacy/trade policy cannot execute a game mutation")

    async def end_turn(self, reflections=None):
        raise AssertionError("a pending human-only response must block end turn")

    async def list_tools(self):
        return {
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
        }


def _snapshot(*, blockers=None) -> RuntimeSnapshot:
    return RuntimeSnapshot(
        game_id=GAME_ID,
        turn=15,
        overview={"turn": 15, "player_id": "player-1"},
        cities=[{"city_id": 1, "currently_building": "BUILDING_MONUMENT"}],
        tech_civics={
            "current_research": "TECH_MINING",
            "available_techs": [],
            "current_civic": "CIVIC_CODE_OF_LAWS",
            "available_civics": [],
        },
        diplomacy={"pending": [{"diplomacy_id": "contact-2", "player_id": 2}]},
        trades={"offers": [{"offer_id": "offer-3", "player_id": 3}]},
        blockers=blockers
        if blockers is not None
        else [
            {
                "type": "pending_diplomacy",
                "data": {"diplomacy_id": "contact-2", "player_id": 2},
            },
            {
                "type": "pending_trades",
                "data": {"offer_id": "offer-3", "player_id": 3},
            },
        ],
    )


def _observation(observation_id: str = "obs-diplomacy"):
    observation = normalize_runtime_snapshot(_snapshot()).canonical
    return observation.model_copy(
        update={"observation_id": observation_id, "observed_at": NOW}
    )


def _foundation(store: WorkflowStore) -> StrategicContract:
    contract_id = build_strategic_contract_id(GAME_ID)
    contract = StrategicContract(
        contract_id=contract_id,
        game_session_id=GAME_ID,
        revision=1,
        authority_scope_set=AuthorityScopeSet(),
        mission_graph=MissionGraph(),
        created_from_observation_id="obs-foundation",
    )
    return store.commit_strategic_contract_revision(
        StrategicContractCommit(
            commit_id="diplomacy-trade-foundation",
            game_session_id=GAME_ID,
            contract_id=contract_id,
            expected_base_revision=0,
            contract=contract,
            committed_at=NOW,
            reason="create diplomacy/trade foundation",
        )
    )


def _mission(
    contract_id: str,
    *,
    handling: str = "human_review",
    envoy_player_id: int | None = None,
) -> Mission:
    policy = {
        "owner": "player-1",
        "handling": handling,
    }
    if envoy_player_id is not None:
        policy["envoy_player_id"] = envoy_player_id
    return Mission(
        mission_id="mission-diplomacy-trade",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        mission_revision=1,
        scope="diplomacy_trade",
        subject=SubjectRef(subject_type="player", subject_id="player-1"),
        slot="player:diplomacy_trade:response",
        objective="Keep diplomacy and trade responses under explicit human control",
        desired_outcome={"diplomacy_trade": policy},
        status=MissionStatus.ACTIVE,
    )


def _gap(kind: str) -> DecisionGap:
    is_trade = kind == "pending_trade_offer"
    identity = "offer-3" if is_trade else "contact-2"
    return DecisionGap(
        decision_gap_id=f"legacy-{kind}",
        game_session_id=GAME_ID,
        stable_identity=f"{kind}:{identity}",
        source_event_ids=(f"event-{identity}",),
        gap_type=kind,
        scope="trade_offer:offer-3" if is_trade else "player:2",
        subjects=(
            SubjectRef(
                subject_type="trade_offer" if is_trade else "player",
                subject_id=identity if is_trade else "2",
            ),
        ),
        observation_id="obs-diplomacy",
        first_observation_id="obs-diplomacy",
        relevant_input_hash=f"input-{identity}",
        input_projection={},
        strategy_revision="legacy",
        route=DecisionRoute.HUMAN,
        status=DecisionGapStatus.OPEN,
        cooldown_key=f"{kind}:{identity}",
        created_at=NOW,
        updated_at=NOW,
    )


def _lease(gaps: tuple[DecisionGap, ...]) -> PlanLease:
    always = Condition(condition_type="turn_at_least", parameters={"turn": 0})
    never = Condition(condition_type="turn_equals", parameters={"turn": 999})
    return PlanLease(
        plan_lease_id="legacy-diplomacy-trade-lease",
        plan_id="legacy-diplomacy-trade-plan",
        game_session_id=GAME_ID,
        decision_gap_ids=tuple(gap.decision_gap_id for gap in gaps),
        scope="diplomacy_trade",
        subjects=tuple(subject for gap in gaps for subject in gap.subjects),
        covered_slots=("player:diplomacy:response", "player:trade:response"),
        plan_revision=1,
        task_ids=(),
        created_from_observation_id="obs-diplomacy",
        status=PlanLeaseStatus.ACTIVE,
        approval_status=ApprovalStatus.APPROVED,
        valid_from_turn=15,
        valid_until_turn=15,
        preconditions=(always,),
        continuation_conditions=(always,),
        invalidation_conditions=(never,),
        review_conditions=(never,),
        continuation_policy=ContinuationPolicy.REQUIRE_REVIEW,
        relevant_input_hash="legacy-diplomacy-trade",
        last_validated_observation_id="obs-diplomacy",
        last_validation_result=LeaseValidationResult.VALID,
    )


def _activate(store: WorkflowStore, base: StrategicContract):
    observation = _observation()
    store.save_normalized_observation(observation)
    return store.activate_diplomacy_trade_authority(
        game_session_id=GAME_ID,
        expected_base_revision=base.revision,
        mission=_mission(base.contract_id),
        activation_id="activate-diplomacy-trade",
        observation_id=observation.observation_id,
        turn_number=observation.turn_number,
        activated_at=NOW + timedelta(minutes=1),
    )


def test_runtime_compiles_empty_graph_and_waits_without_legacy_planning(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "workflow.sqlite3")
        base = _foundation(store)
        _activate(store, base)
        planner = _Planner()
        engine = WorkflowRuntime(
            service_factory=build_runtime_services,
            store=store,
            game=_Game(_snapshot()),
            planner=planner,
            config=RuntimeConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=True,
                verification_delay_seconds=0,
            ),
        )

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == "AWAITING_HUMAN"
        assert result.paused is True
        assert planner.calls == 0
        assert store.list_decision_gaps(GAME_ID) == []
        active_graph = store.active_turn_action_graph(GAME_ID)
        assert active_graph is not None
        assert active_graph[0].node_ids == ()
        assert active_graph[1] == ()

        repeated = await engine.tick()
        assert repeated.workflow_tick["outcome"] == "AWAITING_HUMAN"
        assert planner.calls == 0

        replay = store.export_replay_state(GAME_ID)
        restored = WorkflowStore(tmp_path / "restored.sqlite3")
        restored.import_replay_state(replay)
        restored_planner = _Planner()
        restored_engine = WorkflowRuntime(
            service_factory=build_runtime_services,
            store=restored,
            game=_Game(_snapshot()),
            planner=restored_planner,
            config=RuntimeConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=True,
                verification_delay_seconds=0,
            ),
        )
        recovered = await restored_engine.tick()
        assert recovered.workflow_tick["outcome"] == "AWAITING_HUMAN"
        assert restored_planner.calls == 0
        assert restored.list_decision_gaps(GAME_ID) == []

    asyncio.run(scenario())


def test_diplomacy_trade_policy_cannot_select_an_automatic_response(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    observation = _observation()
    store.save_normalized_observation(observation)
    before = store.export_replay_state(GAME_ID)

    with pytest.raises(ValueError, match="must remain human-only"):
        store.activate_diplomacy_trade_authority(
            game_session_id=GAME_ID,
            expected_base_revision=base.revision,
            mission=_mission(base.contract_id, handling="accept"),
            activation_id="unsafe-diplomacy-activation",
            observation_id=observation.observation_id,
            turn_number=observation.turn_number,
            activated_at=NOW + timedelta(minutes=1),
        )

    assert store.export_replay_state(GAME_ID) == before


def test_reviewed_envoy_target_compiles_one_verified_never_blind_retry_action(
    tmp_path,
):
    store = WorkflowStore(tmp_path / "envoy.sqlite3")
    base = _foundation(store)
    observation = normalize_runtime_snapshot(
        _snapshot(
            blockers=[
                {
                    "type": "end_turn_blocker",
                    "blocking_type": "ENDTURN_BLOCKING_GIVE_INFLUENCE_TOKEN",
                    "message": "Send an envoy",
                }
            ]
        )
    ).canonical.model_copy(update={"observation_id": "obs-envoy", "observed_at": NOW})
    store.save_normalized_observation(observation)
    contract, _tick = store.activate_diplomacy_trade_authority(
        game_session_id=GAME_ID,
        expected_base_revision=base.revision,
        mission=_mission(base.contract_id, envoy_player_id=8),
        activation_id="activate-envoy",
        observation_id=observation.observation_id,
        turn_number=observation.turn_number,
        activated_at=NOW + timedelta(minutes=1),
    )

    from civ6_workflow.turn_compiler import TurnCompiler

    compilation = TurnCompiler().compile_missions(
        observation,
        contract,
        contract.mission_graph.missions,
        mode=ExecutionMode.AUTO,
        auto_action_types={"send_envoy"},
        compiled_at=NOW + timedelta(minutes=2),
    )

    assert len(compilation.nodes) == 1
    node = compilation.nodes[0]
    assert node.action_type == "send_envoy"
    assert dict(node.arguments) == {"player_id": 8}
    assert node.requires_confirmation is False
    assert tuple(map(dict, node.preconditions)) == (
        {
            "type": "blocker_kind_present",
            "blocker_kind": "ENDTURN_BLOCKING_GIVE_INFLUENCE_TOKEN",
        },
    )
    assert tuple(map(dict, node.postconditions)) == (
        {
            "type": "no_blocker_kind",
            "blocker_kind": "ENDTURN_BLOCKING_GIVE_INFLUENCE_TOKEN",
        },
    )


def test_runtime_sends_and_verifies_reviewed_envoy_before_completing_mission(
    tmp_path,
):
    async def scenario():
        store = WorkflowStore(tmp_path / "envoy-runtime.sqlite3")
        base = _foundation(store)
        envoy_snapshot = _snapshot(
            blockers=[
                {
                    "type": "end_turn_blocker",
                    "blocking_type": "ENDTURN_BLOCKING_GIVE_INFLUENCE_TOKEN",
                    "message": "Send an envoy",
                }
            ]
        )
        observation = normalize_runtime_snapshot(envoy_snapshot).canonical.model_copy(
            update={"observation_id": "obs-envoy-runtime", "observed_at": NOW}
        )
        store.save_normalized_observation(observation)
        activated, _tick = store.activate_diplomacy_trade_authority(
            game_session_id=GAME_ID,
            expected_base_revision=base.revision,
            mission=_mission(base.contract_id, envoy_player_id=8),
            activation_id="activate-envoy-runtime",
            observation_id=observation.observation_id,
            turn_number=observation.turn_number,
            activated_at=NOW + timedelta(minutes=1),
        )

        class EnvoyGame(_Game):
            def __init__(self):
                super().__init__(envoy_snapshot)
                self.executed = []

            async def execute_task(self, task):
                assert task.action_type == "send_envoy"
                assert task.arguments == {"player_id": 8}
                self.executed.append(task.task_id)
                self.snapshot = self.snapshot.model_copy(update={"blockers": []})
                return ActionResult(
                    success=True,
                    delivery_status=MutationDeliveryStatus.ACKNOWLEDGED,
                )

            async def list_tools(self):
                return {
                    "send_envoy",
                    "end_turn",
                    "get_notifications",
                    "get_pending_diplomacy",
                    "get_pending_trades",
                }

        game = EnvoyGame()
        engine = WorkflowRuntime(
            service_factory=build_runtime_services,
            store=store,
            game=game,
            planner=_Planner(),
            config=RuntimeConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=False,
                auto_action_types={"send_envoy"},
                allowed_action_types={"send_envoy"},
                allowed_tools={"send_envoy", "end_turn"},
                verification_delay_seconds=0,
            ),
        )

        sent = await engine.tick()
        assert sent.workflow_tick["outcome"] == "MUTATION_SENT", sent.workflow_tick
        assert len(game.executed) == 1
        graph, nodes = store.active_turn_action_graph(GAME_ID)
        attempt = store.latest_attempt_for_task(GAME_ID, nodes[0].task_id)
        assert attempt is not None
        assert attempt.retry_classification.value == "NEVER_BLIND_RETRY"

        restarted_store = WorkflowStore(tmp_path / "envoy-runtime.sqlite3")
        engine = WorkflowRuntime(
            service_factory=build_runtime_services,
            store=restarted_store,
            game=game,
            planner=_Planner(),
            config=RuntimeConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=False,
                auto_action_types={"send_envoy"},
                allowed_action_types={"send_envoy"},
                allowed_tools={"send_envoy", "end_turn"},
                verification_delay_seconds=0,
            ),
        )
        verified = await engine.tick()
        assert verified.workflow_tick["outcome"] == "ATTEMPT_RECONCILED"
        assert len(game.executed) == 1
        store = restarted_store
        completed = store.get_active_strategic_contract(GAME_ID)
        assert completed.revision == activated.revision + 1
        mission = next(
            item
            for item in completed.mission_graph.missions
            if item.scope == "diplomacy_trade"
        )
        assert mission.status is MissionStatus.COMPLETED
        assert mission.mission_revision == 2
        assert store.active_turn_action_graph(GAME_ID) is None

        replay = store.export_replay_state(GAME_ID)
        restored = WorkflowStore(tmp_path / "envoy-runtime-restored.sqlite3")
        restored.import_replay_state(replay)
        assert restored.export_replay_state(GAME_ID) == replay

    asyncio.run(scenario())


def test_replay_rejects_missing_diplomacy_trade_activation_audit_before_delete(
    tmp_path,
):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    base = _foundation(source)
    _activate(source, base)
    replay = source.export_replay_state(GAME_ID)
    replay["tables"]["workflow_ticks"] = [
        row
        for row in replay["tables"]["workflow_ticks"]
        if row["outcome"] != "SCOPE_AUTHORITY_ACTIVATED"
    ]

    target = WorkflowStore(tmp_path / "target.sqlite3")
    target_base = _foundation(target)
    before = target.export_replay_state(GAME_ID)
    with pytest.raises(ValueError, match="scope activation"):
        target.import_replay_state(replay)
    assert target.export_replay_state(GAME_ID) == before
    assert target.get_active_strategic_contract(GAME_ID) == target_base
