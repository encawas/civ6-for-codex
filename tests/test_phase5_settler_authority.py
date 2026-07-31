from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from civ6_workflow.actions import resolve_action_spec
from civ6_workflow.domain import (
    ActionAttempt,
    ApprovalStatus,
    AttemptReconciledTick,
    AttemptStatus,
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
    ObservationComparisonKind,
    PlanLease,
    PlanLeaseStatus,
    RuntimeState,
    StateDeltaBuilder,
    StateDeltaChangeKind,
    StrategicContract,
    StrategicContractCommit,
    SubjectRef,
    VerificationStatus,
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


NOW = datetime(2026, 7, 31, 14, 0, tzinfo=UTC)
GAME_ID = "phase5-settler"
UNIT_ID = 17
TARGET = (4, 5)


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
            commit_id="settler-foundation",
            game_session_id=GAME_ID,
            contract_id=contract_id,
            expected_base_revision=0,
            contract=contract,
            committed_at=NOW,
            reason="create settler migration foundation",
        )
    )


def _observation(
    *,
    observation_id: str,
    position: tuple[int, int] | None = (1, 2),
    city_count: int = 1,
    units_loaded: bool = True,
):
    units = None
    if units_loaded:
        units = []
        if position is not None:
            units.append(
                {
                    "unit_id": UNIT_ID,
                    "unit_type": "UNIT_SETTLER",
                    "owner": "player-1",
                    "x": position[0],
                    "y": position[1],
                    "moves_remaining": 2,
                    "targets": [
                        {
                            "x": TARGET[0],
                            "y": TARGET[1],
                            "legal": True,
                            "reachable": True,
                        }
                    ],
                }
            )
    cities = [
        {
            "city_id": index + 1,
            "name": f"City {index + 1}",
            "x": index,
            "y": 0,
            "owner": "player-1",
            "production": None,
        }
        for index in range(city_count)
    ]
    if city_count > 1:
        cities[-1].update(
            {
                "x": TARGET[0],
                "y": TARGET[1],
                "owner": "player-1",
            }
        )
    snapshot = RuntimeSnapshot(
        game_id=GAME_ID,
        turn=9,
        cities=cities,
        units=units,
        tech_civics={
            "current_research": "TECH_MINING",
            "available_techs": [],
            "current_civic": "CIVIC_CODE_OF_LAWS",
            "available_civics": [],
        },
    )
    canonical = normalize_runtime_snapshot(snapshot).canonical
    return canonical.model_copy(
        update={"observation_id": observation_id, "observed_at": NOW}
    )


def _mission(contract_id: str) -> Mission:
    return Mission(
        mission_id="mission-settler-17",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        mission_revision=1,
        scope="settler",
        subject=SubjectRef(subject_type="unit", subject_id=str(UNIT_ID)),
        slot="settler:17",
        objective="Found the approved second city",
        desired_outcome={
            "settler": {
                "unit_id": UNIT_ID,
                "target_x": TARGET[0],
                "target_y": TARGET[1],
                "baseline_city_count": 1,
                "owner": "player-1",
            }
        },
        status=MissionStatus.ACTIVE,
    )


def _legacy_gap() -> DecisionGap:
    return DecisionGap(
        decision_gap_id="legacy-settler-gap",
        game_session_id=GAME_ID,
        stable_identity="settler-site:17",
        source_event_ids=("event-settler",),
        gap_type="settler_site_selection_required",
        scope="settler",
        subjects=(SubjectRef(subject_type="unit", subject_id=str(UNIT_ID)),),
        observation_id="obs-activation",
        first_observation_id="obs-activation",
        relevant_input_hash="settler-input",
        input_projection={},
        strategy_revision="legacy",
        route=DecisionRoute.PLANNER,
        status=DecisionGapStatus.OPEN,
        cooldown_key="settler-site:17",
        created_at=NOW,
        updated_at=NOW,
    )


def _legacy_lease(gap: DecisionGap) -> PlanLease:
    always = Condition(condition_type="turn_at_least", parameters={"turn": 0})
    never = Condition(condition_type="turn_equals", parameters={"turn": 999})
    return PlanLease(
        plan_lease_id="legacy-settler-lease",
        plan_id="legacy-settler-plan",
        game_session_id=GAME_ID,
        decision_gap_ids=(gap.decision_gap_id,),
        scope="settler",
        subjects=gap.subjects,
        covered_slots=("settler:17",),
        plan_revision=1,
        task_ids=("legacy-settler-task",),
        created_from_observation_id="obs-activation",
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
        last_validated_observation_id="obs-activation",
        last_validation_result=LeaseValidationResult.VALID,
    )


def _legacy_bundle() -> PlanBundle:
    return PlanBundle(
        plan_id="legacy-settler-plan",
        summary="legacy settler projection",
        tasks=[
            ProposedTask(
                task_id="legacy-settler-task",
                action_type="unit_move",
                entity_type="unit",
                entity_id=UNIT_ID,
                due_turn=9,
                arguments={
                    "unit_id": UNIT_ID,
                    "target_x": TARGET[0],
                    "target_y": TARGET[1],
                },
                postconditions=[
                    {
                        "type": "unit_moved_from",
                        "unit_id": UNIT_ID,
                        "x": 1,
                        "y": 2,
                    }
                ],
                risk=RiskLevel.HIGH,
                reason="legacy settlement route",
            )
        ],
    )


def _activate(
    store: WorkflowStore,
    base: StrategicContract,
    observation,
):
    store.save_normalized_observation(observation)
    return store.activate_settler_authority(
        game_session_id=GAME_ID,
        expected_base_revision=base.revision,
        mission=_mission(base.contract_id),
        activation_id="activate-settler-17",
        observation_id=observation.observation_id,
        turn_number=observation.turn_number,
        activated_at=NOW + timedelta(minutes=1),
    )


def _compile_and_activate(store, observation, contract):
    compilation = TurnCompiler().compile_missions(
        observation,
        contract,
        contract.mission_graph.missions,
        mode=ExecutionMode.AUTO,
        auto_action_types={"unit_move", "unit_found_city"},
        compiled_at=NOW + timedelta(minutes=2),
    )
    graph, tasks = store.activate_turn_action_graph(
        compilation.graph,
        compilation.nodes,
        activated_at=NOW + timedelta(minutes=2),
    )
    assert len(tasks) == 1
    return graph, tasks[0]


def _finalize_success(
    store: WorkflowStore,
    task,
    *,
    verification_observation_id: str,
    minute: int,
):
    spec = resolve_action_spec(task.action_type)
    prepared = ActionAttempt(
        action_attempt_id=f"attempt-{task.task_id}",
        game_session_id=GAME_ID,
        task_id=task.task_id,
        action_type=task.action_type,
        attempt_number=1,
        request_id=f"request-{task.task_id}",
        idempotency_key=f"turn-action:{task.task_id}",
        prepared_from_observation_id=task.created_from_observation_id,
        prepared_at=NOW + timedelta(minutes=minute),
        status=AttemptStatus.PREPARED,
        retry_classification=spec.retry_classification,
        normalized_arguments=spec.build_arguments(task),
        postconditions=tuple(task.postconditions),
    )
    store.save_action_attempt(prepared)
    uncertain = prepared.model_copy(
        update={
            "status": AttemptStatus.UNCERTAIN,
            "sent_at": NOW + timedelta(minutes=minute),
            "transport_result": {"phase": "delivery_started"},
        }
    )
    store.update_action_attempt(uncertain)
    succeeded = uncertain.model_copy(
        update={
            "status": AttemptStatus.SUCCEEDED,
            "response_received_at": NOW + timedelta(minutes=minute),
            "tool_result": {"success": True},
            "verification_status": VerificationStatus.PASSED,
            "last_verification_observation_id": verification_observation_id,
            "verification_count": 1,
        }
    )
    tick = AttemptReconciledTick(
        tick_id=f"tick-{task.task_id}",
        game_session_id=GAME_ID,
        turn_number=9,
        starting_runtime_state=RuntimeState.VERIFYING,
        observation_ids=(verification_observation_id,),
        started_at=NOW + timedelta(minutes=minute + 1),
        completed_at=NOW + timedelta(minutes=minute + 1),
        metrics={},
        action_attempt_id=succeeded.action_attempt_id,
        task_id=succeeded.task_id,
        attempt_status=AttemptStatus.SUCCEEDED,
    )
    store.finalize_attempt_success(succeeded, tick)
    return succeeded, tick


def test_turn_compiler_uses_move_then_found_city_from_fresh_observation(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    moving = _observation(observation_id="obs-moving")
    contract, _tick = _activate(store, base, moving)

    move = (
        TurnCompiler()
        .compile_missions(
            moving,
            contract,
            contract.mission_graph.missions,
            mode=ExecutionMode.AUTO,
            auto_action_types={"unit_move", "unit_found_city"},
            compiled_at=NOW,
        )
        .nodes[0]
    )
    assert move.action_type == "unit_move"
    assert move.arguments == {
        "unit_id": UNIT_ID,
        "target_x": TARGET[0],
        "target_y": TARGET[1],
    }

    at_target = _observation(observation_id="obs-at-target", position=TARGET)
    found = (
        TurnCompiler()
        .compile_missions(
            at_target,
            contract,
            contract.mission_graph.missions,
            mode=ExecutionMode.AUTO,
            auto_action_types={"unit_move", "unit_found_city"},
            compiled_at=NOW,
        )
        .nodes[0]
    )
    assert found.action_type == "unit_found_city"
    assert found.arguments == {"unit_id": UNIT_ID}


def test_verified_move_expires_graph_without_completing_mission(tmp_path):
    store = WorkflowStore(tmp_path / "source.sqlite3")
    base = _foundation(store)
    before = _observation(observation_id="obs-before-move")
    contract, _tick = _activate(store, base, before)
    _graph, task = _compile_and_activate(store, before, contract)
    after = _observation(observation_id="obs-after-move", position=(2, 3))
    store.save_normalized_observation(after)

    attempt, tick = _finalize_success(
        store,
        task,
        verification_observation_id=after.observation_id,
        minute=3,
    )

    unchanged = store.get_active_strategic_contract(GAME_ID)
    assert unchanged == contract
    assert unchanged.mission_graph.missions[0].status is MissionStatus.ACTIVE
    assert store.active_turn_action_graph(GAME_ID) is None
    assert store.get_task(GAME_ID, task.task_id).status.value == "done"
    store.finalize_attempt_success(attempt, tick)
    assert store.get_active_strategic_contract(GAME_ID) == contract

    replay = store.export_replay_state(GAME_ID)
    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(replay)
    assert restored.export_replay_state(GAME_ID) == replay


def test_verified_found_city_completes_mission_once(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    at_target = _observation(observation_id="obs-at-target", position=TARGET)
    contract, _tick = _activate(store, base, at_target)
    _graph, task = _compile_and_activate(store, at_target, contract)
    after = _observation(
        observation_id="obs-city-founded",
        position=None,
        city_count=2,
    )
    store.save_normalized_observation(after)

    attempt, tick = _finalize_success(
        store,
        task,
        verification_observation_id=after.observation_id,
        minute=3,
    )

    completed = store.get_active_strategic_contract(GAME_ID)
    assert completed.revision == contract.revision + 1
    mission = completed.mission_graph.missions[0]
    assert mission.status is MissionStatus.COMPLETED
    assert mission.mission_revision == 2
    assert mission.evidence_refs == (attempt.action_attempt_id,)
    store.finalize_attempt_success(attempt, tick)
    assert store.get_active_strategic_contract(GAME_ID) == completed


def test_unsafe_settler_activation_fails_without_partial_state(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    observation = _observation(observation_id="obs-unsafe")
    unsafe = observation.model_copy(
        update={
            "units": tuple(
                unit.model_copy(
                    update={
                        "values": {
                            **unit.model_dump(mode="json")["values"],
                            "targets": [],
                        }
                    }
                )
                for unit in observation.units or ()
            )
        }
    )
    store.save_normalized_observation(unsafe)
    before = store.export_replay_state(GAME_ID)

    with pytest.raises(ValueError, match="safe unoccupied target"):
        store.activate_settler_authority(
            game_session_id=GAME_ID,
            expected_base_revision=base.revision,
            mission=_mission(base.contract_id),
            activation_id="unsafe-activation",
            observation_id=unsafe.observation_id,
            turn_number=unsafe.turn_number,
            activated_at=NOW + timedelta(minutes=1),
        )

    assert store.export_replay_state(GAME_ID) == before


def test_settler_state_delta_tracks_complete_facts_but_not_unknown_deletions():
    baseline = _observation(observation_id="obs-baseline")
    moved = _observation(observation_id="obs-moved", position=(2, 3))

    result = StateDeltaBuilder().compare(baseline, moved)

    assert result.kind is ObservationComparisonKind.STATE_DELTA
    assert result.state_delta is not None
    assert any(
        change.scope == "unit"
        and change.field_path == f"units.{UNIT_ID}"
        and change.change_kind is StateDeltaChangeKind.FIELD_CHANGED
        for change in result.state_delta.changes
    )

    incomplete = _observation(
        observation_id="obs-incomplete",
        units_loaded=False,
    )
    unknown = StateDeltaBuilder().compare(baseline, incomplete)
    assert unknown.state_delta is None or not any(
        change.scope == "unit"
        and change.change_kind is StateDeltaChangeKind.ENTITY_DELETED
        for change in unknown.state_delta.changes
    )
