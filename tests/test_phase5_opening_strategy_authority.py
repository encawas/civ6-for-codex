from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

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
from civ6_workflow.models import (
    RuntimeSnapshot,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore


NOW = datetime(2026, 7, 31, 13, 0, tzinfo=UTC)
GAME_ID = "phase5-opening"


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
    commit = StrategicContractCommit(
        commit_id="opening-foundation",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        expected_base_revision=0,
        contract=contract,
        committed_at=NOW,
        reason="create the opening migration Contract root",
    )
    assert store.commit_strategic_contract_revision(commit) == contract
    return contract


def _observation():
    normalized = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id=GAME_ID,
            turn=9,
            cities=[
                {
                    "city_id": 1,
                    "currently_building": "BUILDING_MONUMENT",
                }
            ],
            tech_civics={
                "current_research": "TECH_MINING",
                "available_techs": [{"tech_type": "TECH_POTTERY"}],
                "current_civic": "CIVIC_CODE_OF_LAWS",
                "available_civics": [{"civic_type": "CIVIC_FOREIGN_TRADE"}],
            },
        )
    ).canonical
    return normalized.model_copy(
        update={"observation_id": "obs-opening", "observed_at": NOW}
    )


def _mission(contract_id: str, *, policy=None) -> Mission:
    return Mission(
        mission_id="mission-opening-policy",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        mission_revision=1,
        scope="opening_strategy",
        subject=SubjectRef(subject_type="player", subject_id="player-1"),
        slot="empire:opening_strategy",
        objective="Establish the opening strategic posture",
        desired_outcome={
            "opening_strategy": policy
            or {
                "stage": "opening",
                "victory_focus": "science",
                "production_priorities": ["BUILDING_MONUMENT", "UNIT_SCOUT"],
            }
        },
        status=MissionStatus.ACTIVE,
    )


def _gap() -> DecisionGap:
    return DecisionGap(
        decision_gap_id="gap-opening",
        game_session_id=GAME_ID,
        stable_identity="opening-strategy:empire",
        source_event_ids=("event-opening",),
        gap_type="opening_strategy_required",
        scope="empire",
        subjects=(SubjectRef(subject_type="player", subject_id="player-1"),),
        observation_id="obs-opening",
        first_observation_id="obs-opening",
        relevant_input_hash="opening-input",
        input_projection={},
        strategy_revision="legacy",
        route=DecisionRoute.PLANNER,
        status=DecisionGapStatus.OPEN,
        cooldown_key="opening-strategy",
        created_at=NOW,
        updated_at=NOW,
    )


def _lease(gap: DecisionGap, *, task_ids=()) -> PlanLease:
    always = Condition(condition_type="turn_at_least", parameters={"turn": 0})
    never = Condition(condition_type="turn_equals", parameters={"turn": 999})
    return PlanLease(
        plan_lease_id="lease-opening",
        plan_id="plan-opening",
        game_session_id=GAME_ID,
        decision_gap_ids=(gap.decision_gap_id,),
        scope="empire",
        subjects=gap.subjects,
        covered_slots=("opening_strategy",),
        plan_revision=1,
        task_ids=task_ids,
        created_from_observation_id="obs-opening",
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
        last_validated_observation_id="obs-opening",
        last_validation_result=LeaseValidationResult.VALID,
    )


def _activate(store: WorkflowStore, base: StrategicContract):
    return store.activate_opening_strategy_authority(
        game_session_id=GAME_ID,
        expected_base_revision=base.revision,
        mission=_mission(base.contract_id),
        activation_id="activate-opening-1",
        observation_id="obs-opening",
        turn_number=9,
        activated_at=NOW + timedelta(minutes=1),
    )


def test_opening_activation_audit_is_required_before_replay_delete(tmp_path):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    base = _foundation(source)
    source.save_normalized_observation(_observation())
    _activate(source, base)
    state = source.export_replay_state(GAME_ID)
    state["tables"]["workflow_ticks"] = [
        row
        for row in state["tables"]["workflow_ticks"]
        if row["outcome"] != "SCOPE_AUTHORITY_ACTIVATED"
    ]

    target = WorkflowStore(tmp_path / "target.sqlite3")
    target_base = _foundation(target)
    before = target.export_replay_state(GAME_ID)
    with pytest.raises(ValueError, match="scope activation"):
        target.import_replay_state(state)
    assert target.export_replay_state(GAME_ID) == before
    assert target.get_active_strategic_contract(GAME_ID) == target_base


def test_opening_strategy_mission_cannot_select_an_action():
    contract_id = build_strategic_contract_id(GAME_ID)
    mission = _mission(
        contract_id,
        policy={
            "stage": "opening",
            "nested": {"tool_name": "set_city_production"},
        },
    )
    contract = StrategicContract(
        contract_id=contract_id,
        game_session_id=GAME_ID,
        revision=2,
        authority_scope_set=AuthorityScopeSet(
            mission_graph_scopes=("opening_strategy",)
        ),
        mission_graph=MissionGraph(missions=(mission,)),
    )
    with pytest.raises(ValueError, match="cannot select an execution action"):
        StrategicContractCommit(
            commit_id="invalid-opening-activation",
            game_session_id=GAME_ID,
            contract_id=contract_id,
            expected_base_revision=1,
            contract=contract,
            committed_at=NOW,
            reason="invalid opening activation",
            source_scope_activation_id="invalid-opening",
            source_scope="opening_strategy",
            source_scope_mission_ids=(mission.mission_id,),
        )
