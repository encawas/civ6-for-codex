from __future__ import annotations

import copy
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
    thaw_json,
)
from civ6_workflow.models import (
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RiskLevel,
    RuntimeSnapshot,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore
from civ6_workflow.turn_compiler import TurnCompiler


NOW = datetime(2026, 7, 31, 16, 0, tzinfo=UTC)
GAME_ID = "phase5-city-roles"
CITY_ID = 3


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
            commit_id="city-roles-foundation",
            game_session_id=GAME_ID,
            contract_id=contract_id,
            expected_base_revision=0,
            contract=contract,
            committed_at=NOW,
            reason="create city roles foundation",
        )
    )


def _observation(
    observation_id: str,
    *,
    production: str | None = None,
    owner: str = "player-1",
):
    canonical = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id=GAME_ID,
            turn=12,
            cities=[
                {
                    "city_id": CITY_ID,
                    "name": "Capital",
                    "owner": owner,
                    "x": 1,
                    "y": 1,
                    "production": production,
                }
            ],
            tech_civics={
                "current_research": "TECH_MINING",
                "available_techs": [],
                "current_civic": "CIVIC_CODE_OF_LAWS",
                "available_civics": [],
            },
        )
    ).canonical
    return canonical.model_copy(
        update={"observation_id": observation_id, "observed_at": NOW}
    )


def _mission(contract_id: str) -> Mission:
    return Mission(
        mission_id="mission-city-roles",
        game_session_id=GAME_ID,
        contract_id=contract_id,
        mission_revision=1,
        scope="city_roles",
        subject=SubjectRef(subject_type="player", subject_id="player-1"),
        slot="player:city_roles",
        objective="Develop the capital as the opening production center",
        desired_outcome={
            "city_roles": {
                "owner": "player-1",
                "cities": [
                    {
                        "city_id": CITY_ID,
                        "role": "production",
                        "production_queue": [
                            {
                                "item_type": "BUILDING",
                                "item_name": "BUILDING_MONUMENT",
                            },
                            {
                                "item_type": "UNIT",
                                "item_name": "UNIT_WARRIOR",
                            },
                        ],
                    }
                ],
            }
        },
        status=MissionStatus.ACTIVE,
    )


def _activate(store, base, observation):
    store.save_normalized_observation(observation)
    return store.activate_city_roles_authority(
        game_session_id=GAME_ID,
        expected_base_revision=base.revision,
        mission=_mission(base.contract_id),
        activation_id="activate-city-roles",
        observation_id=observation.observation_id,
        turn_number=observation.turn_number,
        activated_at=NOW + timedelta(minutes=1),
    )


def _legacy_gap() -> DecisionGap:
    return DecisionGap(
        decision_gap_id="legacy-city-gap",
        game_session_id=GAME_ID,
        stable_identity=f"city-role:city-{CITY_ID}",
        source_event_ids=("event-city-role",),
        gap_type="city_role_required",
        scope="city",
        subjects=(SubjectRef(subject_type="city", subject_id=str(CITY_ID)),),
        observation_id="obs-activation",
        first_observation_id="obs-activation",
        relevant_input_hash="city-role-input",
        input_projection={},
        strategy_revision="legacy",
        route=DecisionRoute.PLANNER,
        status=DecisionGapStatus.OPEN,
        cooldown_key=f"city-role:{CITY_ID}",
        created_at=NOW,
        updated_at=NOW,
    )


def _legacy_lease(gap: DecisionGap) -> PlanLease:
    always = Condition(condition_type="turn_at_least", parameters={"turn": 0})
    never = Condition(condition_type="turn_equals", parameters={"turn": 999})
    return PlanLease(
        plan_lease_id="legacy-city-lease",
        plan_id="legacy-city-plan",
        game_session_id=GAME_ID,
        decision_gap_ids=(gap.decision_gap_id,),
        scope="city",
        subjects=gap.subjects,
        covered_slots=(f"city:{CITY_ID}:production",),
        plan_revision=1,
        task_ids=("legacy-city-task",),
        created_from_observation_id="obs-activation",
        status=PlanLeaseStatus.ACTIVE,
        approval_status=ApprovalStatus.APPROVED,
        valid_from_turn=1,
        valid_until_turn=30,
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
        plan_id="legacy-city-plan",
        summary="legacy city role projection",
        city_plan_updates=[
            {
                "city_id": CITY_ID,
                "role": "production",
                "followup_queue": ["BUILDING_MONUMENT"],
            }
        ],
        tasks=[
            ProposedTask(
                task_id="legacy-city-task",
                action_type="city_set_production",
                entity_type="city",
                entity_id=CITY_ID,
                due_turn=12,
                arguments={
                    "city_id": CITY_ID,
                    "item_type": "BUILDING",
                    "item_name": "BUILDING_MONUMENT",
                },
                postconditions=[
                    {
                        "type": "city_production_equals",
                        "city_id": CITY_ID,
                        "item_name": "BUILDING_MONUMENT",
                    }
                ],
                risk=RiskLevel.LOW,
                reason="legacy city production",
            )
        ],
    )


def _compile_activate(store, observation, contract):
    compilation = TurnCompiler().compile_missions(
        observation,
        contract,
        contract.mission_graph.missions,
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
        compiled_at=NOW + timedelta(minutes=2),
    )
    graph, tasks = store.activate_turn_action_graph(
        compilation.graph,
        compilation.nodes,
        activated_at=NOW + timedelta(minutes=2),
    )
    assert len(tasks) == 1
    return graph, tasks[0]


def _finalize(store, task, observation_id: str, suffix: str):
    spec = resolve_action_spec(task.action_type)
    prepared = ActionAttempt(
        action_attempt_id=f"attempt-city-{suffix}",
        game_session_id=GAME_ID,
        task_id=task.task_id,
        action_type=task.action_type,
        attempt_number=1,
        request_id=f"request-city-{suffix}",
        idempotency_key=f"city-role:{suffix}",
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
            "last_verification_observation_id": observation_id,
            "verification_count": 1,
        }
    )
    tick = AttemptReconciledTick(
        tick_id=f"tick-city-{suffix}",
        game_session_id=GAME_ID,
        turn_number=12,
        starting_runtime_state=RuntimeState.VERIFYING,
        observation_ids=(observation_id,),
        started_at=NOW + timedelta(minutes=4),
        completed_at=NOW + timedelta(minutes=4),
        metrics={},
        action_attempt_id=succeeded.action_attempt_id,
        task_id=succeeded.task_id,
        attempt_status=AttemptStatus.SUCCEEDED,
    )
    store.finalize_attempt_success(succeeded, tick)
    return succeeded, tick


def test_city_roles_cutover_disposes_legacy_state_and_is_replay_stable(tmp_path):
    store = WorkflowStore(tmp_path / "source.sqlite3")
    base = _foundation(store)
    observation = _observation("obs-activation")
    store.save_normalized_observation(observation)
    gap = _legacy_gap()
    store.save_decision_gap(gap, turn=12)
    store.save_plan_lease(_legacy_lease(gap))
    store.save_plan_bundle(
        GAME_ID,
        12,
        _legacy_bundle(),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
        observation_id=observation.observation_id,
    )

    contract, tick = store.activate_city_roles_authority(
        game_session_id=GAME_ID,
        expected_base_revision=base.revision,
        mission=_mission(base.contract_id),
        activation_id="activate-with-legacy-city-state",
        observation_id=observation.observation_id,
        turn_number=observation.turn_number,
        activated_at=NOW + timedelta(minutes=1),
    )

    assert contract.authority_scope_set.mission_graph_scopes == ("city_roles",)
    assert store.get_decision_gap(GAME_ID, gap.decision_gap_id).status is (
        DecisionGapStatus.SUPERSEDED
    )
    assert store.get_task(GAME_ID, "legacy-city-task").status.value == "cancelled"
    assert {
        (item.object_kind, item.object_id, item.final_status)
        for item in tick.legacy_dispositions
    } == {
        ("decision_gap", gap.decision_gap_id, "SUPERSEDED"),
        ("plan_lease", "legacy-city-lease", "INVALIDATED"),
        ("stored_task", "legacy-city-task", "cancelled"),
    }
    replay = store.export_replay_state(GAME_ID)
    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(replay)
    assert restored.export_replay_state(GAME_ID) == replay


def test_city_roles_closes_legacy_writes_but_not_settler(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    observation = _observation("obs-activation")
    _activate(store, base, observation)

    with pytest.raises(ValueError, match="legacy city roles plan writes"):
        store.save_plan_bundle(
            GAME_ID,
            12,
            _legacy_bundle(),
            mode=ExecutionMode.AUTO,
            auto_action_types={"city_set_production"},
            observation_id=observation.observation_id,
        )
    store.save_plan_bundle(
        GAME_ID,
        12,
        PlanBundle(
            plan_id="unrelated-unit-plan",
            summary="settler remains independently owned",
            unit_plan_updates=[
                {
                    "unit_id": 99,
                    "goal": "found_city",
                    "target": {"x": 5, "y": 5},
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"unit_move"},
        observation_id=observation.observation_id,
    )


def test_unresolved_legacy_city_attempt_blocks_cutover(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    observation = _observation("obs-activation")
    store.save_normalized_observation(observation)
    store.save_plan_bundle(
        GAME_ID,
        12,
        _legacy_bundle(),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
        observation_id=observation.observation_id,
    )
    task = store.get_task(GAME_ID, "legacy-city-task")
    spec = resolve_action_spec(task.action_type)
    store.save_action_attempt(
        ActionAttempt(
            action_attempt_id="legacy-city-attempt",
            game_session_id=GAME_ID,
            task_id=task.task_id,
            action_type=task.action_type,
            attempt_number=1,
            request_id="legacy-city-request",
            idempotency_key="legacy-city-attempt",
            prepared_from_observation_id=observation.observation_id,
            prepared_at=NOW,
            status=AttemptStatus.PREPARED,
            retry_classification=spec.retry_classification,
            normalized_arguments=spec.build_arguments(task),
            postconditions=tuple(task.postconditions),
        )
    )
    before = store.export_replay_state(GAME_ID)

    with pytest.raises(ValueError, match="unresolved legacy execution"):
        store.activate_city_roles_authority(
            game_session_id=GAME_ID,
            expected_base_revision=base.revision,
            mission=_mission(base.contract_id),
            activation_id="blocked-city-activation",
            observation_id=observation.observation_id,
            turn_number=observation.turn_number,
            activated_at=NOW + timedelta(minutes=1),
        )

    assert store.export_replay_state(GAME_ID) == before


def test_city_role_actions_consume_queue_via_contract_revisions(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    empty = _observation("obs-empty")
    contract, _tick = _activate(store, base, empty)
    _graph, first = _compile_activate(store, empty, contract)
    assert first.arguments["item_name"] == "BUILDING_MONUMENT"
    first_verified = _observation(
        "obs-monument-selected", production="BUILDING_MONUMENT"
    )
    store.save_normalized_observation(first_verified)

    first_attempt, first_tick = _finalize(
        store, first, first_verified.observation_id, "monument"
    )

    revised = store.get_active_strategic_contract(GAME_ID)
    assert revised.revision == contract.revision + 1
    mission = revised.mission_graph.missions[0]
    assert mission.status is MissionStatus.ACTIVE
    assert mission.mission_revision == 2
    policy = thaw_json(mission.desired_outcome)["city_roles"]
    assert [item["item_name"] for item in policy["cities"][0]["production_queue"]] == [
        "UNIT_WARRIOR"
    ]
    assert mission.evidence_refs == (first_attempt.action_attempt_id,)
    store.finalize_attempt_success(first_attempt, first_tick)
    assert store.get_active_strategic_contract(GAME_ID) == revised

    next_empty = _observation("obs-next-empty")
    store.save_normalized_observation(next_empty)
    _graph, second = _compile_activate(store, next_empty, revised)
    assert second.arguments["item_name"] == "UNIT_WARRIOR"
    second_verified = _observation("obs-warrior-selected", production="UNIT_WARRIOR")
    store.save_normalized_observation(second_verified)
    second_attempt, _tick = _finalize(
        store, second, second_verified.observation_id, "warrior"
    )

    completed = store.get_active_strategic_contract(GAME_ID)
    assert completed.revision == revised.revision + 1
    final_mission = completed.mission_graph.missions[0]
    assert final_mission.status is MissionStatus.COMPLETED
    assert final_mission.mission_revision == 3
    assert set(final_mission.evidence_refs) == {
        first_attempt.action_attempt_id,
        second_attempt.action_attempt_id,
    }
    replay = store.export_replay_state(GAME_ID)
    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(replay)
    assert restored.export_replay_state(GAME_ID) == replay

    tampered = copy.deepcopy(replay)
    revision_row = next(
        row
        for row in tampered["tables"]["strategic_contract_revisions"]
        if row["revision"] == revised.revision
    )
    forged_contract = StrategicContract.model_validate_json(
        revision_row["contract_json"]
    )
    forged_mission = forged_contract.mission_graph.missions[0]
    forged_policy = thaw_json(forged_mission.desired_outcome)["city_roles"]
    forged_policy["cities"][0]["role"] = "forged-role"
    forged_mission = forged_mission.model_copy(
        update={"desired_outcome": {"city_roles": forged_policy}}
    )
    forged_contract = forged_contract.model_copy(
        update={"mission_graph": MissionGraph(missions=(forged_mission,))}
    )
    revision_row["contract_json"] = forged_contract.model_dump_json()
    commit_row = next(
        row
        for row in tampered["tables"]["strategic_contract_commits"]
        if row["committed_revision"] == revised.revision
    )
    forged_commit = StrategicContractCommit.model_validate_json(
        commit_row["commit_json"]
    ).model_copy(update={"contract": forged_contract})
    commit_row["commit_json"] = forged_commit.model_dump_json()
    target = WorkflowStore(tmp_path / "tamper-target.sqlite3")
    before_import = target.export_replay_state(GAME_ID)
    with pytest.raises(ValueError, match="unrelated strategy facts"):
        target.import_replay_state(tampered)
    assert target.export_replay_state(GAME_ID) == before_import


def test_legacy_rule_compiler_emits_no_city_work_after_cutover(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    observation = _observation("obs-activation")
    store.save_normalized_observation(observation)
    store.save_plan_bundle(
        GAME_ID,
        12,
        PlanBundle(
            plan_id="historical-city-plan",
            summary="historical city context",
            city_plan_updates=[
                {
                    "city_id": CITY_ID,
                    "role": "production",
                    "followup_queue": ["BUILDING_MONUMENT"],
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
        observation_id=observation.observation_id,
    )
    _activate(store, base, observation)
    normalized = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id=GAME_ID,
            turn=12,
            cities=[
                {
                    "city_id": CITY_ID,
                    "owner": "player-1",
                    "production": None,
                }
            ],
        )
    )

    compiled = DeterministicRuleCompiler(store).compile(normalized)

    assert compiled.bundle is None
    assert not any("city" in event.event_type for event in compiled.events)


def test_city_roles_activation_requires_same_owner_and_rolls_back(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    base = _foundation(store)
    observation = _observation("obs-foreign-city", owner="player-2")
    store.save_normalized_observation(observation)
    before = store.export_replay_state(GAME_ID)

    with pytest.raises(ValueError, match="ownership evidence"):
        store.activate_city_roles_authority(
            game_session_id=GAME_ID,
            expected_base_revision=base.revision,
            mission=_mission(base.contract_id),
            activation_id="foreign-city",
            observation_id=observation.observation_id,
            turn_number=observation.turn_number,
            activated_at=NOW + timedelta(minutes=1),
        )

    assert store.export_replay_state(GAME_ID) == before


def test_city_state_delta_is_scoped_to_city_roles():
    baseline = _observation("obs-baseline")
    current = _observation("obs-current", production="BUILDING_MONUMENT")

    result = StateDeltaBuilder().compare(baseline, current)

    assert result.kind is ObservationComparisonKind.STATE_DELTA
    assert result.state_delta is not None
    city_changes = [
        change
        for change in result.state_delta.changes
        if change.field_path == f"cities.{CITY_ID}"
    ]
    assert len(city_changes) == 1
    assert city_changes[0].scope == "city_roles"
    assert city_changes[0].change_kind is StateDeltaChangeKind.FIELD_CHANGED
