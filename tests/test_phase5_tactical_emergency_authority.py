from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from civ6_workflow.actions import (
    build_action_attempt_idempotency_key,
    resolve_action_spec,
)
from civ6_workflow.domain import (
    ActionAttempt,
    ApprovalStatus,
    AttemptReconciledTick,
    AttemptStatus,
    AuthorityScopeSet,
    Condition,
    Mission,
    MissionGraph,
    MissionImpactAnalyzer,
    MissionStatus,
    RetryClassification,
    RuntimeState,
    StateDeltaBuilder,
    StrategicContract,
    StrategicContractCommit,
    SubjectRef,
    VerificationEvidence,
    VerificationStatus,
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
    ExecutionMode,
    RiskLevel,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
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
    normalized_arguments = spec.build_arguments(task)
    prepared = ActionAttempt(
        action_attempt_id=f"attempt-{task.task_id}",
        game_session_id=GAME_ID,
        task_id=task.task_id,
        action_type=task.action_type,
        attempt_number=1,
        request_id=f"request-{task.task_id}",
        idempotency_key=build_action_attempt_idempotency_key(
            task, normalized_arguments
        ),
        prepared_from_observation_id=task.created_from_observation_id,
        prepared_at=NOW,
        status=AttemptStatus.PREPARED,
        retry_classification=spec.retry_classification,
        normalized_arguments=normalized_arguments,
        postconditions=tuple(task.postconditions),
    )
    store.save_action_attempt(prepared)
    uncertain = prepared.model_copy(
        update={
            "status": AttemptStatus.UNCERTAIN,
            "sent_at": NOW,
            "transport_result": {"phase": "delivery_started"},
        }
    )
    store.update_action_attempt(uncertain)
    succeeded = uncertain.model_copy(
        update={
            "status": AttemptStatus.SUCCEEDED,
            "response_received_at": NOW,
            "tool_result": {"success": True},
            "verification_status": VerificationStatus.PASSED,
            "last_verification_observation_id": after.observation_id,
            "last_verification_projection_hash": after.projection_hash,
            "verification_evidence": VerificationEvidence.POSITIVE_COMMIT_EVIDENCE,
            "verification_reason": "test postconditions satisfied",
            "verified_at": after.observed_at,
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
    assert {change.scope for change in result.state_delta.changes} == {
        "tactical_emergency"
    }

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


def test_tactical_move_requires_explicit_legal_and_reachable_facts(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    activation = _observation("obs-policy-activation")
    contract, _tick = _activate(store, base, activation)
    unknown = _observation(
        "obs-policy-unknown",
        targets=[{"x": TARGET[0], "y": TARGET[1]}],
    )

    compilation = TurnCompiler().compile_missions(
        unknown,
        contract,
        contract.mission_graph.missions,
        mode=ExecutionMode.AUTO,
        auto_action_types={"tactical_unit_move"},
        compiled_at=NOW + timedelta(minutes=2),
    )

    assert compilation.nodes == ()
    assert compilation.unavailable_target == f"tile:{TARGET[0]}:{TARGET[1]}"


def _prepared_tactical_attempt(store: WorkflowStore, task) -> ActionAttempt:
    spec = resolve_action_spec(task.action_type)
    arguments = spec.build_arguments(task)
    return ActionAttempt(
        action_attempt_id=f"attempt-contract-{task.task_id}",
        game_session_id=GAME_ID,
        task_id=task.task_id,
        action_type=task.action_type,
        attempt_number=1,
        request_id=f"request-contract-{task.task_id}",
        idempotency_key=build_action_attempt_idempotency_key(task, arguments),
        prepared_from_observation_id=task.created_from_observation_id,
        prepared_at=NOW,
        status=AttemptStatus.PREPARED,
        retry_classification=spec.retry_classification,
        normalized_arguments=arguments,
        postconditions=tuple(task.postconditions),
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("postconditions", ({"type": "turn_at_least", "turn": 0},), "postconditions"),
        ("retry_classification", RetryClassification.NEVER_BLIND_RETRY, "retry policy"),
        ("idempotency_key", "task:forged", "idempotency key"),
        ("postcondition_version", 2, "postcondition version"),
    ],
)
def test_store_binds_attempt_to_turn_action_contract(tmp_path, field, value, message):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    before = _observation("obs-contract-before")
    contract, _tick = _activate(store, base, before)
    _graph, task = _compile_and_activate(store, before, contract)
    assert store.approve_task(GAME_ID, task.task_id, approved_by="reviewer")
    task = store.get_task(GAME_ID, task.task_id)
    candidate = _prepared_tactical_attempt(store, task).model_copy(
        update={field: value}
    )

    with pytest.raises(ValueError, match=message):
        store.save_action_attempt(candidate)

    assert store.list_action_attempts(GAME_ID) == []


@pytest.mark.parametrize(
    "case",
    [
        "missing_observation",
        "other_game",
        "projection_hash_mismatch",
        "observation_before_send",
    ],
)
def test_store_rejects_untrusted_verification_observation(tmp_path, case):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    before = _observation(f"obs-verification-before-{case}")
    contract, _tick = _activate(store, base, before)
    _graph, task = _compile_and_activate(store, before, contract)
    assert store.approve_task(GAME_ID, task.task_id, approved_by="reviewer")
    task = store.get_task(GAME_ID, task.task_id)
    prepared = _prepared_tactical_attempt(store, task)
    store.save_action_attempt(prepared)
    uncertain = prepared.model_copy(
        update={
            "status": AttemptStatus.UNCERTAIN,
            "sent_at": NOW,
            "transport_result": {"phase": "delivery_started"},
        }
    )
    store.update_action_attempt(uncertain)

    evidence = _observation(f"obs-verification-{case}", position=TARGET, targets=[])
    evidence_id = evidence.observation_id
    evidence_hash = evidence.projection_hash
    if case == "missing_observation":
        evidence_id = "obs-verification-missing"
    elif case == "other_game":
        evidence = evidence.model_copy(
            update={
                "observation_id": "obs-verification-other-game",
                "game_session_id": "other-game",
            }
        )
        evidence_id = evidence.observation_id
        evidence_hash = evidence.projection_hash
        store.save_normalized_observation(evidence)
    elif case == "projection_hash_mismatch":
        store.save_normalized_observation(evidence)
        evidence_hash = "0" * 64
    elif case == "observation_before_send":
        evidence = evidence.model_copy(
            update={"observed_at": NOW - timedelta(minutes=1)}
        )
        store.save_normalized_observation(evidence)
    else:
        raise AssertionError(case)

    forged = uncertain.model_copy(
        update={
            "status": AttemptStatus.SUCCEEDED,
            "response_received_at": NOW,
            "tool_result": {"success": True},
            "verification_status": VerificationStatus.PASSED,
            "last_verification_observation_id": evidence_id,
            "last_verification_projection_hash": evidence_hash,
            "verification_evidence": VerificationEvidence.POSITIVE_COMMIT_EVIDENCE,
            "verification_reason": "forged verification evidence",
            "verified_at": NOW,
            "verification_count": 1,
        }
    )

    with pytest.raises(ValueError):
        store.update_action_attempt(forged)

    assert store.get_action_attempt(forged.action_attempt_id) == uncertain


def test_terminal_action_failure_blocks_mission_and_expires_graph(tmp_path):

    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    before = _observation("obs-terminal-before")
    contract, _tick = _activate(store, base, before)
    graph, task = _compile_and_activate(store, before, contract)
    assert store.approve_task(GAME_ID, task.task_id, approved_by="reviewer")
    task = store.get_task(GAME_ID, task.task_id)
    prepared = _prepared_tactical_attempt(store, task)
    store.save_action_attempt(prepared)
    uncertain = prepared.model_copy(
        update={
            "status": AttemptStatus.UNCERTAIN,
            "sent_at": NOW,
            "transport_result": {"phase": "delivery_started"},
        }
    )
    store.update_action_attempt(uncertain)
    conflicting = _observation("obs-terminal-conflict", position=(4, 4), targets=[])
    store.save_normalized_observation(conflicting)
    failed = uncertain.model_copy(
        update={
            "status": AttemptStatus.FAILED,
            "response_received_at": NOW,
            "verification_status": VerificationStatus.FAILED,
            "last_verification_observation_id": conflicting.observation_id,
            "last_verification_projection_hash": conflicting.projection_hash,
            "verification_evidence": VerificationEvidence.CONFLICTING_STATE,
            "verification_reason": "unit is not at the approved target",
            "verified_at": conflicting.observed_at,
            "verification_count": 1,
        }
    )
    tick = AttemptReconciledTick(
        tick_id="tick-terminal-failure",
        game_session_id=GAME_ID,
        turn_number=12,
        starting_runtime_state=RuntimeState.RECONCILING,
        observation_ids=(conflicting.observation_id,),
        started_at=NOW + timedelta(minutes=4),
        completed_at=NOW + timedelta(minutes=4),
        metrics={},
        action_attempt_id=failed.action_attempt_id,
        task_id=failed.task_id,
        attempt_status=AttemptStatus.FAILED,
    )

    resolution = store.finalize_attempt_failure(
        failed, tick, task_error="unit is not at the approved target"
    )

    assert resolution.task_status is TaskStatus.FAILED
    active = store.get_active_strategic_contract(GAME_ID)
    assert active.revision == contract.revision + 1
    assert active.mission_graph.missions[0].status is MissionStatus.BLOCKED
    assert failed.action_attempt_id in active.mission_graph.missions[0].evidence_refs
    assert store.active_turn_action_graph(GAME_ID) is None
    assert graph.graph_id
    assert store.get_task(GAME_ID, task.task_id).status is TaskStatus.FAILED

    reopened = WorkflowStore(tmp_path / "workflow.sqlite3")
    assert reopened.get_active_strategic_contract(GAME_ID) == active
    replay = reopened.export_replay_state(GAME_ID)
    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(replay)
    assert restored.export_replay_state(GAME_ID) == replay
    assert (
        restored.get_active_strategic_contract(GAME_ID).mission_graph.missions[0].status
        is MissionStatus.BLOCKED
    )
