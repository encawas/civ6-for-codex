from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from civ6_workflow.domain import (
    ApprovalStatus,
    AuthorityScopeSet,
    Condition,
    ContinuationPolicy,
    DecisionGap,
    DecisionGapStatus,
    DecisionRoute,
    LeaseValidationResult,
    Mission,
    MissionGraph,
    MissionStatus,
    PlanLease,
    PlanLeaseStatus,
    ScopeAuthorityActivatedTick,
    StrategicContract,
    StrategicContractCommit,
    SubjectRef,
    build_strategic_contract_id,
)
from civ6_workflow.models import (
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RiskLevel,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore
from civ6_workflow.turn_compiler import TurnCompiler


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)
GAME_ID = "phase5-civic"


def _create_foundation(store: WorkflowStore) -> StrategicContract:
    contract_id = build_strategic_contract_id(GAME_ID)
    contract = StrategicContract(
        contract_id=contract_id,
        game_session_id=GAME_ID,
        revision=1,
        authority_scope_set=AuthorityScopeSet(),
        mission_graph=MissionGraph(),
        created_from_observation_id="obs-foundation",
    )
    commit = StrategicContractCommit(
        commit_id="phase5-foundation",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        expected_base_revision=0,
        contract=contract,
        committed_at=NOW,
        reason="create the pre-migration Contract root",
    )
    assert store.commit_strategic_contract_revision(commit) == contract
    return contract


def _observation(
    *,
    observation_id: str = "obs-civic",
    turn: int = 8,
    current_research: str | None = "TECH_MINING",
):
    return normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id=GAME_ID,
            turn=turn,
            tech_civics={
                "current_research": current_research,
                "available_techs": [{"tech_type": "TECH_WRITING"}],
                "current_civic": None,
                "available_civics": [
                    {"civic_type": "CIVIC_CODE_OF_LAWS"},
                    {"civic_type": "CIVIC_CRAFTSMANSHIP"},
                ],
            },
        )
    ).canonical.model_copy(
        update={"observation_id": observation_id, "observed_at": NOW}
    )


def _civic_mission(contract_id: str) -> Mission:
    return Mission(
        mission_id="mission-civic-code-of-laws",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        mission_revision=1,
        scope="civic",
        subject=SubjectRef(subject_type="player", subject_id="player-1"),
        slot="player:civic",
        objective="Select the opening civic",
        desired_outcome={"civic": "CIVIC_CODE_OF_LAWS"},
        status=MissionStatus.ACTIVE,
    )


def _research_mission(contract_id: str) -> Mission:
    return Mission(
        mission_id="mission-research-writing",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        mission_revision=1,
        scope="research",
        subject=SubjectRef(subject_type="player", subject_id="player-1"),
        slot="player:research",
        objective="Select Writing",
        desired_outcome={"technology": "TECH_WRITING"},
        status=MissionStatus.ACTIVE,
    )


def _civic_gap(*, gap_id: str = "gap-civic", scope: str = "civic") -> DecisionGap:
    return DecisionGap(
        decision_gap_id=gap_id,
        game_session_id=GAME_ID,
        stable_identity=f"civic-direction:{gap_id}",
        source_event_ids=(f"event-{gap_id}",),
        gap_type="civic_direction_required",
        scope=scope,
        subjects=(SubjectRef(subject_type="player", subject_id="player-1"),),
        observation_id="obs-civic",
        first_observation_id="obs-civic",
        relevant_input_hash=f"hash-{gap_id}",
        input_projection={},
        strategy_revision="legacy",
        route=DecisionRoute.PLANNER,
        status=DecisionGapStatus.OPEN,
        cooldown_key=f"cooldown-{gap_id}",
        created_at=NOW,
        updated_at=NOW,
    )


def _civic_lease(gap: DecisionGap, *, scope: str = "civic") -> PlanLease:
    always = Condition(condition_type="turn_at_least", parameters={"turn": 0})
    never = Condition(condition_type="turn_equals", parameters={"turn": 999})
    return PlanLease(
        plan_lease_id="lease-civic",
        plan_id="plan-civic",
        game_session_id=GAME_ID,
        decision_gap_ids=(gap.decision_gap_id,),
        scope=scope,
        subjects=gap.subjects,
        covered_slots=("civic",),
        plan_revision=1,
        task_ids=("legacy-civic-task",),
        created_from_observation_id="obs-civic",
        status=PlanLeaseStatus.ACTIVE,
        approval_status=ApprovalStatus.APPROVED,
        valid_from_turn=1,
        valid_until_turn=20,
        preconditions=(always,),
        continuation_conditions=(always,),
        invalidation_conditions=(never,),
        review_conditions=(never,),
        continuation_policy=ContinuationPolicy.EXTEND_WHEN_INPUT_UNCHANGED,
        relevant_input_hash=gap.relevant_input_hash,
        last_validated_observation_id="obs-civic",
        last_validation_result=LeaseValidationResult.VALID,
    )


def _legacy_civic_bundle(*, task_id: str = "legacy-civic-task") -> PlanBundle:
    return PlanBundle(
        plan_id=f"plan-{task_id}",
        summary="legacy civic projection",
        tasks=[
            ProposedTask(
                task_id=task_id,
                action_type="set_civic",
                entity_type="civic",
                entity_id="CIVIC_CODE_OF_LAWS",
                due_turn=8,
                arguments={"tech_or_civic": "CIVIC_CODE_OF_LAWS"},
                postconditions=[
                    {
                        "type": "civic_equals",
                        "civic_type": "CIVIC_CODE_OF_LAWS",
                    }
                ],
                risk=RiskLevel.LOW,
                reason="legacy civic queue",
            )
        ],
    )


def _activate(
    store: WorkflowStore,
    base: StrategicContract,
    *,
    activation_id: str = "activate-civic-1",
):
    return store.activate_civic_authority(
        game_session_id=GAME_ID,
        expected_base_revision=base.revision,
        mission=_civic_mission(base.contract_id),
        activation_id=activation_id,
        observation_id="obs-civic",
        turn_number=8,
        activated_at=NOW + timedelta(minutes=1),
    )


def test_civic_cutover_is_atomic_idempotent_and_replay_stable(tmp_path):
    store = WorkflowStore(tmp_path / "source.sqlite3")
    base = _create_foundation(store)
    observation = _observation(current_research=None)
    store.save_normalized_observation(observation)
    gap = _civic_gap(scope="empire")
    store.save_decision_gap(gap, turn=8)
    store.save_plan_lease(_civic_lease(gap, scope="empire"))
    store.save_plan_bundle(
        GAME_ID,
        8,
        _legacy_civic_bundle(),
        mode=ExecutionMode.AUTO,
        auto_action_types={"set_civic"},
        observation_id=observation.observation_id,
    )

    contract, tick = _activate(store, base)

    assert isinstance(tick, ScopeAuthorityActivatedTick)
    assert contract.revision == 2
    assert contract.authority_scope_set.mission_graph_scopes == ("civic",)
    assert contract.mission_graph.missions == (_civic_mission(base.contract_id),)
    assert store.get_strategic_contract_revision(GAME_ID, 1) == base
    assert store.get_decision_gap(GAME_ID, gap.decision_gap_id).status is (
        DecisionGapStatus.SUPERSEDED
    )
    assert store.get_task(GAME_ID, "legacy-civic-task").status is TaskStatus.CANCELLED
    with store._connect() as conn:
        lease = PlanLease.model_validate_json(
            conn.execute(
                "SELECT lease_json FROM plan_leases WHERE plan_lease_id='lease-civic'"
            ).fetchone()["lease_json"]
        )
    assert lease.status is PlanLeaseStatus.INVALIDATED
    assert {
        (item.object_kind, item.object_id, item.final_status)
        for item in tick.legacy_dispositions
    } == {
        ("decision_gap", gap.decision_gap_id, "SUPERSEDED"),
        ("plan_lease", "lease-civic", "INVALIDATED"),
        ("stored_task", "legacy-civic-task", "cancelled"),
    }

    assert _activate(store, base) == (contract, tick)
    assert len(store.list_strategic_contract_revisions(GAME_ID)) == 2
    first_export = store.export_replay_state(GAME_ID)
    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(first_export)
    assert restored.export_replay_state(GAME_ID) == first_export
    assert WorkflowStore(restored.path).get_active_strategic_contract(GAME_ID) == (
        contract
    )


def test_civic_cutover_closes_only_civic_legacy_writes(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _create_foundation(store)
    store.save_normalized_observation(_observation())
    _activate(store, base)

    with pytest.raises(ValueError, match="legacy civic DecisionGap"):
        store.save_decision_gap(_civic_gap(gap_id="late-civic-gap"), turn=8)
    with pytest.raises(ValueError, match="legacy civic plan writes"):
        store.save_plan_bundle(
            GAME_ID,
            8,
            _legacy_civic_bundle(task_id="late-civic-task"),
            mode=ExecutionMode.AUTO,
            auto_action_types={"set_civic"},
            observation_id="obs-civic",
        )

    research_gap = _civic_gap(gap_id="research-gap").model_copy(
        update={
            "gap_type": "research_direction_required",
            "scope": "research",
        }
    )
    store.save_decision_gap(research_gap, turn=8)
    assert store.get_decision_gap(GAME_ID, research_gap.decision_gap_id) == research_gap


def test_civic_turn_graph_is_the_only_claimable_execution_authority(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _create_foundation(store)
    observation = _observation()
    store.save_normalized_observation(observation)
    contract, _tick = _activate(store, base)
    mission = contract.mission_graph.missions[0]
    compilation = TurnCompiler().compile_missions(
        observation,
        contract,
        (mission,),
        mode=ExecutionMode.AUTO,
        auto_action_types={"set_civic"},
        compiled_at=NOW + timedelta(minutes=2),
    )

    graph, tasks = store.activate_turn_action_graph(
        compilation.graph,
        compilation.nodes,
        activated_at=NOW + timedelta(minutes=2),
    )

    assert graph == compilation.graph
    assert len(tasks) == 1
    assert tasks[0].action_type == "set_civic"
    assert tasks[0].source_contract_revision == contract.revision
    assert store.due_turn_action_nodes(
        GAME_ID, 8, source_observation_id=observation.observation_id
    ) == list(tasks)
    assert [
        task for task in store.due_tasks(GAME_ID, 8) if task.action_type == "set_civic"
    ] == []


def test_turn_compiler_emits_one_graph_for_research_and_civic():
    observation = _observation(current_research=None)
    contract_id = build_strategic_contract_id(GAME_ID)
    missions = (_civic_mission(contract_id), _research_mission(contract_id))
    contract = StrategicContract(
        contract_id=contract_id,
        game_session_id=GAME_ID,
        revision=3,
        authority_scope_set=AuthorityScopeSet(
            mission_graph_scopes=("civic", "research")
        ),
        mission_graph=MissionGraph(missions=missions),
        created_from_observation_id=observation.observation_id,
    )

    compilation = TurnCompiler().compile_missions(
        observation,
        contract,
        missions,
        mode=ExecutionMode.AUTO,
        auto_action_types={"set_civic", "set_research"},
        compiled_at=NOW,
    )

    assert {node.action_type for node in compilation.nodes} == {
        "set_civic",
        "set_research",
    }
    assert {node.graph_id for node in compilation.nodes} == {compilation.graph.graph_id}
    assert compilation.graph.node_ids == tuple(
        sorted(node.node_id for node in compilation.nodes)
    )


def test_scope_activation_audit_cannot_be_removed_during_replay(tmp_path):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    base = _create_foundation(source)
    source.save_normalized_observation(_observation())
    _activate(source, base)
    state = source.export_replay_state(GAME_ID)
    state["tables"]["workflow_ticks"] = [
        row
        for row in state["tables"]["workflow_ticks"]
        if row["outcome"] != "SCOPE_AUTHORITY_ACTIVATED"
    ]

    target = WorkflowStore(tmp_path / "target.sqlite3")
    target_base = _create_foundation(target)
    before = target.export_replay_state(GAME_ID)
    with pytest.raises(ValueError, match="scope activation"):
        target.import_replay_state(state)
    assert target.export_replay_state(GAME_ID) == before
    assert target.get_active_strategic_contract(GAME_ID) == target_base


@pytest.mark.parametrize("object_kind", ["decision_gap", "plan_lease"])
def test_replay_rejects_nonterminal_legacy_civic_state_after_cutover(
    tmp_path, object_kind
):
    source = WorkflowStore(tmp_path / f"source-{object_kind}.sqlite3")
    base = _create_foundation(source)
    source.save_normalized_observation(_observation(current_research=None))
    gap = _civic_gap(scope="empire")
    source.save_decision_gap(gap, turn=8)
    source.save_plan_lease(_civic_lease(gap, scope="empire"))
    _activate(source, base)
    state = source.export_replay_state(GAME_ID)

    if object_kind == "decision_gap":
        row = state["tables"]["decision_gaps"][0]
        restored_gap = DecisionGap.model_validate_json(row["gap_json"]).model_copy(
            update={"status": DecisionGapStatus.OPEN}
        )
        row["status"] = DecisionGapStatus.OPEN.value
        row["gap_json"] = restored_gap.model_dump_json()
    else:
        row = state["tables"]["plan_leases"][0]
        restored_lease = PlanLease.model_validate_json(row["lease_json"]).model_copy(
            update={"status": PlanLeaseStatus.ACTIVE}
        )
        row["status"] = PlanLeaseStatus.ACTIVE.value
        row["lease_json"] = restored_lease.model_dump_json()
    tick_row = next(
        row
        for row in state["tables"]["workflow_ticks"]
        if row["outcome"] == "SCOPE_AUTHORITY_ACTIVATED"
    )
    activation_tick = ScopeAuthorityActivatedTick.model_validate_json(
        tick_row["tick_json"]
    )
    tick_row["tick_json"] = activation_tick.model_copy(
        update={
            "legacy_dispositions": tuple(
                disposition
                for disposition in activation_tick.legacy_dispositions
                if disposition.object_kind != object_kind
            )
        }
    ).model_dump_json()

    target = WorkflowStore(tmp_path / f"target-{object_kind}.sqlite3")
    target_base = _create_foundation(target)
    before = target.export_replay_state(GAME_ID)
    with pytest.raises(ValueError, match="nonterminal legacy"):
        target.import_replay_state(state)
    assert target.export_replay_state(GAME_ID) == before
    assert target.get_active_strategic_contract(GAME_ID) == target_base


def test_startup_rejects_nonterminal_legacy_civic_gap_after_cutover(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _create_foundation(store)
    store.save_normalized_observation(_observation(current_research=None))
    gap = _civic_gap(scope="empire")
    store.save_decision_gap(gap, turn=8)
    _activate(store, base)

    with store._connect() as conn:
        tick_row = conn.execute(
            "SELECT tick_json FROM workflow_ticks "
            "WHERE game_id=? AND outcome='SCOPE_AUTHORITY_ACTIVATED'",
            (GAME_ID,),
        ).fetchone()
        activation_tick = ScopeAuthorityActivatedTick.model_validate_json(
            tick_row["tick_json"]
        )
        amended_tick = activation_tick.model_copy(
            update={
                "legacy_dispositions": tuple(
                    disposition
                    for disposition in activation_tick.legacy_dispositions
                    if disposition.object_kind != "decision_gap"
                )
            }
        )
        conn.execute(
            "UPDATE workflow_ticks SET tick_json=? WHERE tick_id=?",
            (amended_tick.model_dump_json(), activation_tick.tick_id),
        )
        conn.execute(
            "UPDATE decision_gaps SET status=?, gap_json=? "
            "WHERE game_id=? AND decision_gap_id=?",
            (
                DecisionGapStatus.OPEN.value,
                gap.model_dump_json(),
                GAME_ID,
                gap.decision_gap_id,
            ),
        )

    with pytest.raises(ValueError, match="nonterminal legacy DecisionGap"):
        WorkflowStore(store.path)


def test_stale_civic_activation_rolls_back_every_legacy_object(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _create_foundation(store)
    store.save_normalized_observation(_observation())
    gap = _civic_gap()
    store.save_decision_gap(gap, turn=8)
    before = store.export_replay_state(GAME_ID)

    with pytest.raises(ValueError, match="stale civic authority"):
        store.activate_civic_authority(
            game_session_id=GAME_ID,
            expected_base_revision=base.revision + 1,
            mission=_civic_mission(base.contract_id),
            activation_id="stale-civic",
            observation_id="obs-civic",
            turn_number=8,
            activated_at=NOW + timedelta(minutes=1),
        )

    assert store.export_replay_state(GAME_ID) == before
