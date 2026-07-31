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
