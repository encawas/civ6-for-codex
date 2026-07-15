from datetime import UTC, datetime, timedelta

import pytest

from civ6_workflow.domain import (
    ActionAttempt,
    ApprovalDecision,
    ApprovalRecord,
    ApprovalStatus,
    AttemptStatus,
    Event,
    EventRoute,
    EventStatus,
    Observation,
    Plan,
    PlanRequestedTick,
    PlanSource,
    PlanStatus,
    PlannerRequest,
    PlannerRequestStatus,
    RetryClassification,
    RuntimeState,
    SourceVersions,
    SubjectRef,
    Task,
    TaskStatus,
    VerificationStatus,
    build_task_idempotency_key,
)


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)


def _nested_json() -> dict:
    return {"nested": {"items": [{"value": 1}]}}


def test_all_canonical_identity_semantic_and_audit_objects_are_deeply_immutable():
    """OBS-006/TASK-002/VER-006: every JSON-bearing domain field is frozen."""

    subject = SubjectRef(subject_type="research", subject_id="player-1")
    desired_outcome = _nested_json()
    key = build_task_idempotency_key(
        game_session_id="game-1",
        subject=subject,
        slot="player:research",
        desired_outcome=desired_outcome,
        source_plan_revision=1,
        earliest_turn=12,
        latest_turn=12,
    )
    observation = Observation(
        observation_id="obs-1",
        game_session_id="game-1",
        turn_number=12,
        sequence=1,
        observed_at=NOW,
        source_versions=SourceVersions(
            game_api="civ6-mcp/v1", normalization="1", runtime="1"
        ),
        base_state=_nested_json(),
    )
    task = Task(
        task_id="task-1",
        game_session_id="game-1",
        idempotency_key=key,
        task_type="set_research",
        subject=subject,
        slot="player:research",
        arguments=_nested_json(),
        desired_outcome=desired_outcome,
        source_plan_id="plan-1",
        source_plan_revision=1,
        created_from_observation_id="obs-1",
        status=TaskStatus.READY,
        earliest_turn=12,
        latest_turn=12,
        must_complete_before_end_turn=True,
        approval_status=ApprovalStatus.APPROVED,
        preconditions=(),
        postconditions=(),
    )
    event = Event(
        event_id="event-1",
        game_session_id="game-1",
        dedupe_key="research:player-1",
        event_type="research_required",
        opened_from_observation_id="obs-1",
        last_seen_observation_id="obs-1",
        status=EventStatus.OPEN,
        severity=1,
        route=EventRoute.RULES,
        payload=_nested_json(),
    )
    plan = Plan(
        plan_id="plan-1",
        game_session_id="game-1",
        scope="research",
        revision=1,
        status=PlanStatus.ACTIVE,
        source=PlanSource.PLANNER,
        approval_status=ApprovalStatus.APPROVED,
        created_from_observation_id="obs-1",
        valid_from_turn=12,
        valid_until_turn=20,
        objective="Research writing",
        steps=(_nested_json(),),
        policy_snapshot=_nested_json(),
    )
    approval = ApprovalRecord(
        approval_id="approval-1",
        proposal_type="plan",
        proposal_id="plan-1",
        proposal_revision=1,
        decision=ApprovalDecision.EDITED_AND_APPROVED,
        actor="user",
        created_at=NOW,
        edited_payload=_nested_json(),
        replacement_revision=2,
    )
    planner = PlannerRequest(
        planner_request_id="planner-1",
        game_session_id="game-1",
        turn_number=12,
        observation_id="obs-1",
        decision_gap_ids=("gap-1",),
        input_projection_hash="projection",
        policy_revision="1",
        model_settings=_nested_json(),
        status=PlannerRequestStatus.COMPLETED,
        created_at=NOW,
        completed_at=NOW + timedelta(seconds=1),
        response_hash="response",
        validation_result=_nested_json(),
    )
    attempt = ActionAttempt(
        action_attempt_id="attempt-1",
        task_id="task-1",
        attempt_number=1,
        request_id="request-1",
        idempotency_key=key,
        prepared_from_observation_id="obs-1",
        prepared_at=NOW,
        sent_at=NOW + timedelta(seconds=1),
        response_received_at=NOW + timedelta(seconds=2),
        status=AttemptStatus.SUCCEEDED,
        retry_classification=RetryClassification.NEVER_BLIND_RETRY,
        normalized_arguments=_nested_json(),
        transport_result=_nested_json(),
        tool_result=_nested_json(),
        verification_status=VerificationStatus.PASSED,
        last_verification_observation_id="obs-2",
    )
    tick = PlanRequestedTick(
        tick_id="tick-1",
        game_session_id="game-1",
        starting_runtime_state=RuntimeState.ROUTING,
        observation_ids=("obs-1",),
        started_at=NOW,
        completed_at=NOW,
        planner_request_id="planner-1",
        metrics=_nested_json(),
    )

    containers = (
        observation.base_state,
        task.arguments,
        task.desired_outcome,
        event.payload,
        plan.steps[0],
        plan.policy_snapshot,
        approval.edited_payload,
        planner.model_settings,
        planner.validation_result,
        attempt.normalized_arguments,
        attempt.transport_result,
        attempt.tool_result,
        tick.metrics,
    )
    for container in containers:
        assert container is not None
        with pytest.raises(TypeError):
            container["added"] = True
        nested_items = container["nested"]["items"]
        assert isinstance(nested_items, tuple)
        with pytest.raises(TypeError):
            nested_items[0]["value"] = 2
