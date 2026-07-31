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
    MissionImpactAnalyzer,
    MissionStatus,
    PlanLease,
    PlanLeaseStatus,
    RuntimeState,
    StateDeltaBuilder,
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
    TaskStatus,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore
from civ6_workflow.turn_compiler import TurnCompiler


NOW = datetime(2026, 7, 31, 20, 0, tzinfo=UTC)
GAME_ID = "phase5-tactical-emergency"
UNIT_ID = 7
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
            commit_id="tactical-foundation",
            game_session_id=GAME_ID,
            contract_id=contract_id,
            expected_base_revision=0,
            contract=contract,
            committed_at=NOW,
            reason="create tactical migration foundation",
        )
    )


def _observation(
    observation_id: str,
    *,
    position: tuple[int, int] = (1, 2),
    targets: list[dict[str, object]] | None = None,
    unit_id: int = UNIT_ID,
    moves_remaining: int = 2,
):
    snapshot = RuntimeSnapshot(
        game_id=GAME_ID,
        turn=12,
        cities=[
            {
                "city_id": 1,
                "owner": "player-1",
                "currently_building": "BUILDING_MONUMENT",
            }
        ],
        units=[
            {
                "unit_id": unit_id,
                "unit_type": "UNIT_WARRIOR",
                "owner": "player-1",
                "x": position[0],
                "y": position[1],
                "moves_remaining": moves_remaining,
                "targets": (
                    [
                        {
                            "x": TARGET[0],
                            "y": TARGET[1],
                            "legal": True,
                            "reachable": True,
                        }
                    ]
                    if targets is None
                    else targets
                ),
            }
        ],
        blockers=[
            {
                "type": "end_turn_blocker",
                "blocking_type": "ENDTURN_BLOCKING_UNITS",
            }
        ],
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


def _mission(contract_id: str, *, target_turn: int = 12) -> Mission:
    return Mission(
        mission_id="mission-tactical-unit-7",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        mission_revision=1,
        scope="tactical_emergency",
        subject=SubjectRef(subject_type="unit", subject_id=str(UNIT_ID)),
        slot="unit:7:tactical-response",
        objective="Move the warrior to the reviewed tactical position",
        desired_outcome={
            "tactical_emergency": {
                "unit_id": UNIT_ID,
                "response_kind": "tactical",
                "target_turn": target_turn,
                "order": {
                    "kind": "move",
                    "target_x": TARGET[0],
                    "target_y": TARGET[1],
                },
            }
        },
        status=MissionStatus.ACTIVE,
    )


def _legacy_gap() -> DecisionGap:
    return DecisionGap(
        decision_gap_id="legacy-tactical-gap",
        game_session_id=GAME_ID,
        stable_identity="tactical-attack:unit-7:turn-12",
        source_event_ids=("event-tactical-7",),
        gap_type="tactical_attack_opportunity",
        scope="unit:7",
        subjects=(SubjectRef(subject_type="unit", subject_id=str(UNIT_ID)),),
        observation_id="obs-activation",
        first_observation_id="obs-activation",
        relevant_input_hash="tactical-input",
        input_projection={},
        strategy_revision="legacy",
        route=DecisionRoute.PLANNER,
        status=DecisionGapStatus.OPEN,
        cooldown_key="tactical:unit-7",
        created_at=NOW,
        updated_at=NOW,
    )


def _legacy_lease(gap: DecisionGap) -> PlanLease:
    always = Condition(condition_type="turn_at_least", parameters={"turn": 0})
    never = Condition(condition_type="turn_equals", parameters={"turn": 999})
    return PlanLease(
        plan_lease_id="legacy-tactical-lease",
        plan_id="legacy-tactical-plan",
        game_session_id=GAME_ID,
        decision_gap_ids=(gap.decision_gap_id,),
        scope="tactical",
        subjects=gap.subjects,
        covered_slots=("unit:7:tactical-response",),
        plan_revision=1,
        task_ids=("legacy-unit-skip",),
        created_from_observation_id="obs-activation",
        status=PlanLeaseStatus.ACTIVE,
        approval_status=ApprovalStatus.APPROVED,
        valid_from_turn=12,
        valid_until_turn=12,
        preconditions=(always,),
        continuation_conditions=(always,),
        invalidation_conditions=(never,),
        review_conditions=(never,),
        continuation_policy=ContinuationPolicy.REQUIRE_REVIEW,
        relevant_input_hash=gap.relevant_input_hash,
        last_validated_observation_id="obs-activation",
        last_validation_result=LeaseValidationResult.VALID,
    )


def _legacy_bundle() -> PlanBundle:
    return PlanBundle(
        plan_id="legacy-tactical-plan",
        summary="legacy routine unit projection",
        tasks=[
            ProposedTask(
                task_id="legacy-unit-skip",
                action_type="unit_skip",
                entity_type="unit",
                entity_id=UNIT_ID,
                due_turn=12,
                arguments={"unit_id": UNIT_ID},
                postconditions=[{"type": "unit_no_moves", "unit_id": UNIT_ID}],
                risk=RiskLevel.LOW,
                reason="legacy routine order",
            )
        ],
    )


def _activate(store, base, observation, *, activation_id="activate-tactical"):
    store.save_normalized_observation(observation)
    return store.activate_tactical_emergency_authority(
        game_session_id=GAME_ID,
        expected_base_revision=base.revision,
        mission=_mission(base.contract_id),
        activation_id=activation_id,
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
        auto_action_types=set(),
        compiled_at=NOW + timedelta(minutes=2),
    )
    graph, tasks = store.activate_turn_action_graph(
        compilation.graph,
        compilation.nodes,
        activated_at=NOW + timedelta(minutes=2),
    )
    assert len(tasks) == 1
    return graph, tasks[0]


def test_tactical_graph_is_high_risk_and_suppresses_legacy_unit_skip(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    observation = _observation("obs-activation")
    contract, _tick = _activate(store, base, observation)

    compilation = TurnCompiler().compile_missions(
        observation,
        contract,
        contract.mission_graph.missions,
        mode=ExecutionMode.AUTO,
        auto_action_types={"tactical_unit_move"},
        compiled_at=NOW + timedelta(minutes=2),
    )
    node = compilation.nodes[0]
    assert node.action_type == "tactical_unit_move"
    assert node.arguments == {
        "unit_id": UNIT_ID,
        "target_x": TARGET[0],
        "target_y": TARGET[1],
    }
    assert node.risk == RiskLevel.HIGH.value
    assert node.requires_confirmation is True

    normalized = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id=GAME_ID,
            turn=12,
            units=[
                {
                    "unit_id": UNIT_ID,
                    "unit_type": "UNIT_WARRIOR",
                    "x": 1,
                    "y": 2,
                    "moves_remaining": 2,
                }
            ],
            blockers=[
                {
                    "type": "end_turn_blocker",
                    "blocking_type": "ENDTURN_BLOCKING_UNITS",
                }
            ],
        )
    )
    compiled = DeterministicRuleCompiler(store).compile(normalized)
    assert compiled.bundle is None or all(
        task.entity_id != UNIT_ID for task in compiled.bundle.tasks
    )


def test_verified_tactical_action_completes_mission_once(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    before = _observation("obs-before")
    contract, _tick = _activate(store, base, before)
    _graph, task = _compile_and_activate(store, before, contract)
    assert task.status is TaskStatus.AWAITING_CONFIRMATION
    assert store.approve_task(GAME_ID, task.task_id, approved_by="reviewer")
    task = store.get_task(GAME_ID, task.task_id)

    after = _observation("obs-after", position=TARGET, targets=[])
    store.save_normalized_observation(after)
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
        prepared_at=NOW + timedelta(minutes=3),
        status=AttemptStatus.PREPARED,
        retry_classification=spec.retry_classification,
        normalized_arguments=spec.build_arguments(task),
        postconditions=tuple(task.postconditions),
    )
    store.save_action_attempt(prepared)
    uncertain = prepared.model_copy(
        update={
            "status": AttemptStatus.UNCERTAIN,
            "sent_at": NOW + timedelta(minutes=3),
            "transport_result": {"phase": "delivery_started"},
        }
    )
    store.update_action_attempt(uncertain)
    succeeded = uncertain.model_copy(
        update={
            "status": AttemptStatus.SUCCEEDED,
            "response_received_at": NOW + timedelta(minutes=3),
            "tool_result": {"success": True},
            "verification_status": VerificationStatus.PASSED,
            "last_verification_observation_id": after.observation_id,
            "verification_count": 1,
        }
    )
    tick = AttemptReconciledTick(
        tick_id=f"tick-{task.task_id}",
        game_session_id=GAME_ID,
        turn_number=12,
        starting_runtime_state=RuntimeState.VERIFYING,
        observation_ids=(after.observation_id,),
        started_at=NOW + timedelta(minutes=4),
        completed_at=NOW + timedelta(minutes=4),
        metrics={},
        action_attempt_id=succeeded.action_attempt_id,
        task_id=succeeded.task_id,
        attempt_status=AttemptStatus.SUCCEEDED,
    )

    store.finalize_attempt_success(succeeded, tick)

    completed = store.get_active_strategic_contract(GAME_ID)
    assert completed.revision == contract.revision + 1
    assert completed.mission_graph.missions[0].status is MissionStatus.COMPLETED
    assert completed.mission_graph.missions[0].evidence_refs == (
        succeeded.action_attempt_id,
    )
    store.finalize_attempt_success(succeeded, tick)
    assert store.get_active_strategic_contract(GAME_ID) == completed


def test_tactical_activation_rejects_unsafe_or_conflicting_unit_atomically(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    unsafe = _observation("obs-unsafe", targets=[])
    store.save_normalized_observation(unsafe)
    before = store.export_replay_state(GAME_ID)

    with pytest.raises(ValueError, match="legal reachable target"):
        store.activate_tactical_emergency_authority(
            game_session_id=GAME_ID,
            expected_base_revision=base.revision,
            mission=_mission(base.contract_id),
            activation_id="unsafe-tactical",
            observation_id=unsafe.observation_id,
            turn_number=unsafe.turn_number,
            activated_at=NOW + timedelta(minutes=1),
        )

    assert store.export_replay_state(GAME_ID) == before


def test_current_turn_tactical_mission_with_no_moves_is_unavailable(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    activation = _observation("obs-activation")
    contract, _tick = _activate(store, base, activation)
    exhausted = _observation("obs-exhausted", moves_remaining=0)

    compilation = TurnCompiler().compile_missions(
        exhausted,
        contract,
        contract.mission_graph.missions,
        mode=ExecutionMode.AUTO,
        auto_action_types=set(),
        compiled_at=NOW + timedelta(minutes=2),
    )

    assert compilation.nodes == ()
    assert compilation.unavailable_targets == (
        ("tactical_emergency", f"unit:{UNIT_ID}:no-moves"),
    )


def test_unit_delta_impacts_only_the_matching_unit_mission():
    baseline = _observation("obs-baseline")
    current = _observation("obs-current", position=(2, 3))
    result = StateDeltaBuilder().compare(baseline, current)
    assert result.state_delta is not None
    assert {change.scope for change in result.state_delta.changes} == {"unit"}

    other = _mission("contract").model_copy(
        update={
            "mission_id": "mission-settler-unit-8",
            "scope": "settler",
            "subject": SubjectRef(subject_type="unit", subject_id="8"),
            "slot": "settler:8",
            "desired_outcome": {
                "settler": {
                    "unit_id": 8,
                    "target_x": 8,
                    "target_y": 8,
                    "baseline_city_count": 1,
                    "owner": "player-1",
                }
            },
        }
    )
    graph = MissionGraph(
        missions=tuple(
            sorted((_mission("contract"), other), key=lambda m: m.mission_id)
        )
    )
    affected = MissionImpactAnalyzer().affected_mission_ids(result.state_delta, graph)
    assert affected == ("mission-tactical-unit-7",)


def test_replay_rejects_missing_tactical_activation_tick_before_delete(tmp_path):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    base = _foundation(source)
    observation = _observation("obs-activation")
    _activate(source, base, observation)
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
