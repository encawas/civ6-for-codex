from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from civ6_workflow.domain import (
    ActionAttempt,
    ApprovalDecision,
    ApprovalRecord,
    ApprovalStatus,
    AttemptStatus,
    Event,
    EventRoute,
    EventStatus,
    MutationSentTick,
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
    TickOutcomeKind,
    VerificationStatus,
    build_task_idempotency_key,
    validate_workflow_tick,
    validate_workflow_tick_json,
)


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
VERSIONS = SourceVersions(game_api="civ6-mcp/v1", normalization="1", runtime="1")


def _task_payload(
    *,
    status: TaskStatus = TaskStatus.READY,
    approval_status: ApprovalStatus = ApprovalStatus.APPROVED,
) -> dict:
    subject = SubjectRef(subject_type="research", subject_id="player-1")
    desired_outcome = {"research": "TECH_MINING", "path": ["A", {"step": 2}]}
    key = build_task_idempotency_key(
        game_session_id="game-1",
        subject=subject,
        slot="player:research",
        desired_outcome=desired_outcome,
        source_plan_revision=3,
        earliest_turn=12,
        latest_turn=12,
    )
    return {
        "task_id": "task-1",
        "game_session_id": "game-1",
        "idempotency_key": key,
        "task_type": "set_research",
        "subject": subject,
        "slot": "player:research",
        "arguments": {"tech_or_civic": "TECH_MINING", "options": [1, 2]},
        "desired_outcome": desired_outcome,
        "source_plan_id": "plan-1",
        "source_plan_revision": 3,
        "created_from_observation_id": "obs-1",
        "status": status,
        "earliest_turn": 12,
        "latest_turn": 12,
        "must_complete_before_end_turn": True,
        "approval_status": approval_status,
        "preconditions": (),
        "postconditions": (),
    }


def _attempt_payload(status: AttemptStatus) -> dict:
    return {
        "action_attempt_id": "attempt-1",
        "task_id": "task-1",
        "attempt_number": 1,
        "request_id": "request-1",
        "idempotency_key": "task:key",
        "prepared_from_observation_id": "obs-1",
        "prepared_at": NOW,
        "status": status,
        "retry_classification": RetryClassification.NEVER_BLIND_RETRY,
        "normalized_arguments": {"unit_id": 7, "path": [{"x": 1, "y": 2}]},
    }


def _tick_payload(outcome: TickOutcomeKind) -> dict:
    return {
        "tick_id": "tick-1",
        "game_session_id": "game-1",
        "starting_runtime_state": RuntimeState.READY_TO_ACT,
        "observation_ids": ("obs-1",),
        "started_at": NOW,
        "completed_at": NOW,
        "outcome": outcome,
    }


def test_domain_json_objects_are_recursively_immutable():
    """OBS-006/TASK-002: identity and audit projections cannot mutate in place."""

    observation = Observation(
        observation_id="obs-1",
        game_session_id="game-1",
        turn_number=12,
        sequence=1,
        observed_at=NOW,
        source_versions=VERSIONS,
        base_state={"nested": {"items": [1, {"value": "original"}]}},
    )
    task = Task(**_task_payload())

    with pytest.raises(TypeError):
        observation.base_state["nested"] = "changed"
    with pytest.raises(TypeError):
        observation.base_state["nested"]["items"][1]["value"] = "changed"
    with pytest.raises(AttributeError):
        observation.base_state["nested"]["items"].append(3)
    with pytest.raises(TypeError):
        task.arguments["tech_or_civic"] = "TECH_WRITING"


def test_recursive_immutability_preserves_stable_json_round_trip():
    observation = Observation(
        observation_id="obs-1",
        game_session_id="game-1",
        turn_number=12,
        sequence=1,
        observed_at=NOW,
        source_versions=VERSIONS,
        base_state={"nested": {"items": [1, {"value": "original"}]}},
    )

    encoded = observation.model_dump_json()
    restored = Observation.model_validate_json(encoded)

    assert restored == observation
    assert restored.model_dump_json() == encoded


def test_task_semantic_copy_cannot_silently_stale_idempotency_key():
    """TASK-002: validated copy rejects semantic changes with an old identity."""

    task = Task(**_task_payload())

    with pytest.raises(ValidationError, match="idempotency_key"):
        task.model_copy(update={"desired_outcome": {"research": "TECH_WRITING"}})


def test_executing_task_requires_satisfied_approval():
    with pytest.raises(ValidationError, match="must satisfy approval"):
        Task(
            **_task_payload(
                status=TaskStatus.EXECUTING,
                approval_status=ApprovalStatus.REQUIRED,
            )
        )


def test_awaiting_approval_task_requires_required_approval_status():
    with pytest.raises(ValidationError, match="requires approval"):
        Task(
            **_task_payload(
                status=TaskStatus.AWAITING_APPROVAL,
                approval_status=ApprovalStatus.NOT_REQUIRED,
            )
        )


@pytest.mark.parametrize(
    "extra",
    [
        {"sent_at": NOW},
        {"response_received_at": NOW},
        {"tool_result": {"success": True}},
        {"verification_status": VerificationStatus.PENDING},
        {"last_verification_observation_id": "obs-2"},
    ],
)
def test_prepared_attempt_rejects_delivery_or_verification_evidence(extra):
    """ACT-003: PREPARED is strictly before external delivery."""

    with pytest.raises(ValidationError, match="delivery evidence"):
        ActionAttempt(**(_attempt_payload(AttemptStatus.PREPARED) | extra))


def test_rejected_before_send_attempt_cannot_have_sent_at():
    with pytest.raises(ValidationError, match="cannot have sent_at"):
        ActionAttempt(
            **(_attempt_payload(AttemptStatus.REJECTED_BEFORE_SEND) | {"sent_at": NOW})
        )


@pytest.mark.parametrize(
    "extra, message",
    [
        ({"sent_at": NOW - timedelta(seconds=1)}, "sent_at must not precede"),
        (
            {
                "sent_at": NOW + timedelta(seconds=2),
                "response_received_at": NOW + timedelta(seconds=1),
            },
            "response_received_at must not precede",
        ),
    ],
)
def test_attempt_timestamps_are_monotonic(extra, message):
    with pytest.raises(ValidationError, match=message):
        ActionAttempt(**(_attempt_payload(AttemptStatus.FAILED) | extra))


def test_succeeded_attempt_requires_later_observation_and_passed_verification():
    """VER-001/VER-002: delivery acknowledgement cannot construct success."""

    with pytest.raises(ValidationError, match="verification observation"):
        ActionAttempt(
            **(
                _attempt_payload(AttemptStatus.SUCCEEDED)
                | {
                    "sent_at": NOW + timedelta(seconds=1),
                    "verification_status": VerificationStatus.PASSED,
                }
            )
        )

    with pytest.raises(ValidationError, match="passed verification"):
        ActionAttempt(
            **(
                _attempt_payload(AttemptStatus.SUCCEEDED)
                | {
                    "sent_at": NOW + timedelta(seconds=1),
                    "last_verification_observation_id": "obs-2",
                    "verification_status": VerificationStatus.INCONCLUSIVE,
                }
            )
        )


def test_verified_succeeded_attempt_round_trips_with_immutable_evidence():
    attempt = ActionAttempt(
        **(
            _attempt_payload(AttemptStatus.SUCCEEDED)
            | {
                "sent_at": NOW + timedelta(seconds=1),
                "response_received_at": NOW + timedelta(seconds=2),
                "tool_result": {"accepted": True},
                "last_verification_observation_id": "obs-2",
                "verification_status": VerificationStatus.PASSED,
            }
        )
    )

    restored = ActionAttempt.model_validate_json(attempt.model_dump_json())

    assert restored == attempt
    with pytest.raises(TypeError):
        restored.tool_result["accepted"] = False


def test_mutation_sent_requires_attempt_id_and_matching_state():
    """ACT-001/MET-005: mutation outcome shape is closed by discriminator."""

    with pytest.raises(ValidationError, match="action_attempt_id"):
        validate_workflow_tick(
            _tick_payload(TickOutcomeKind.MUTATION_SENT)
            | {"selected_operation": "set_research"}
        )

    with pytest.raises(ValidationError, match="ACTION_SENT"):
        validate_workflow_tick(
            _tick_payload(TickOutcomeKind.MUTATION_SENT)
            | {
                "ending_runtime_state": RuntimeState.ROUTING,
                "action_attempt_id": "attempt-1",
                "selected_operation": "set_research",
            }
        )


def test_plan_requested_requires_planner_id_and_rejects_action_id():
    with pytest.raises(ValidationError, match="planner_request_id"):
        validate_workflow_tick(_tick_payload(TickOutcomeKind.PLAN_REQUESTED))

    with pytest.raises(ValidationError, match="action_attempt_id"):
        validate_workflow_tick(
            _tick_payload(TickOutcomeKind.PLAN_REQUESTED)
            | {
                "planner_request_id": "planner-1",
                "action_attempt_id": "attempt-1",
            }
        )


def test_non_mutation_tick_cannot_consume_mutation_budget():
    with pytest.raises(ValidationError, match="Input should be 0"):
        validate_workflow_tick(
            _tick_payload(TickOutcomeKind.PLAN_REQUESTED)
            | {"planner_request_id": "planner-1", "mutation_budget_used": 1}
        )


def test_tick_observation_ids_cannot_be_empty():
    with pytest.raises(ValidationError, match="at least 1 item"):
        MutationSentTick(
            tick_id="tick-1",
            game_session_id="game-1",
            starting_runtime_state=RuntimeState.READY_TO_ACT,
            observation_ids=(),
            started_at=NOW,
            completed_at=NOW,
            action_attempt_id="attempt-1",
            selected_operation="set_research",
        )


def test_discriminated_tick_json_round_trip_preserves_concrete_outcome():
    tick = PlanRequestedTick(
        tick_id="tick-1",
        game_session_id="game-1",
        starting_runtime_state=RuntimeState.ROUTING,
        observation_ids=("obs-1",),
        started_at=NOW,
        completed_at=NOW,
        planner_request_id="planner-1",
        metrics={"latency": {"planner": 0.5}},
    )

    restored = validate_workflow_tick_json(tick.model_dump_json())

    assert isinstance(restored, PlanRequestedTick)
    assert restored == tick
    with pytest.raises(TypeError):
        restored.metrics["latency"]["planner"] = 1.0


def test_edited_and_approved_requires_replacement_revision():
    """APR-002: edits must authorize an exact replacement revision."""

    with pytest.raises(ValidationError, match="replacement revision"):
        ApprovalRecord(
            approval_id="approval-1",
            proposal_type="plan",
            proposal_id="plan-1",
            proposal_revision=2,
            decision=ApprovalDecision.EDITED_AND_APPROVED,
            actor="user",
            created_at=NOW,
            edited_payload={"objective": "updated"},
        )


def test_non_edit_approval_rejects_replacement_payload():
    with pytest.raises(ValidationError, match="only edited-and-approved"):
        ApprovalRecord(
            approval_id="approval-1",
            proposal_type="plan",
            proposal_id="plan-1",
            proposal_revision=2,
            decision=ApprovalDecision.APPROVED,
            actor="user",
            created_at=NOW,
            edited_payload={"objective": "updated"},
            replacement_revision=3,
        )


def test_active_plan_requires_satisfied_approval():
    with pytest.raises(ValidationError, match="must satisfy approval"):
        Plan(
            plan_id="plan-1",
            game_session_id="game-1",
            scope="research",
            revision=1,
            status=PlanStatus.ACTIVE,
            source=PlanSource.PLANNER,
            approval_status=ApprovalStatus.REQUIRED,
            created_from_observation_id="obs-1",
            valid_from_turn=12,
            valid_until_turn=20,
            objective="Research writing",
        )


def test_rejected_plan_and_approval_statuses_must_agree():
    with pytest.raises(ValidationError, match="must agree"):
        Plan(
            plan_id="plan-1",
            game_session_id="game-1",
            scope="research",
            revision=1,
            status=PlanStatus.REJECTED,
            source=PlanSource.PLANNER,
            approval_status=ApprovalStatus.APPROVED,
            created_from_observation_id="obs-1",
            valid_from_turn=12,
            valid_until_turn=20,
            objective="Research writing",
        )


def test_non_terminal_planner_request_rejects_result_evidence():
    with pytest.raises(ValidationError, match="non-terminal"):
        PlannerRequest(
            planner_request_id="planner-1",
            game_session_id="game-1",
            turn_number=12,
            observation_id="obs-1",
            decision_gap_ids=("gap-1",),
            input_projection_hash="hash",
            policy_revision="1",
            model_settings={"model": "test"},
            status=PlannerRequestStatus.IN_PROGRESS,
            created_at=NOW,
            response_hash="response",
        )


def test_completed_planner_request_requires_gaps_and_validation_evidence():
    common = {
        "planner_request_id": "planner-1",
        "game_session_id": "game-1",
        "turn_number": 12,
        "observation_id": "obs-1",
        "input_projection_hash": "hash",
        "policy_revision": "1",
        "model_settings": {"model": "test"},
        "status": PlannerRequestStatus.COMPLETED,
        "created_at": NOW,
        "completed_at": NOW + timedelta(seconds=1),
    }
    with pytest.raises(ValidationError, match="at least 1 item"):
        PlannerRequest(
            decision_gap_ids=(),
            response_hash="response",
            validation_result={},
            **common,
        )
    with pytest.raises(ValidationError, match="validated response evidence"):
        PlannerRequest(decision_gap_ids=("gap-1",), **common)


def test_event_resolution_evidence_matches_status():
    common = {
        "event_id": "event-1",
        "game_session_id": "game-1",
        "dedupe_key": "production:city:1",
        "event_type": "production_empty",
        "opened_from_observation_id": "obs-1",
        "last_seen_observation_id": "obs-2",
        "severity": 1,
        "route": EventRoute.RULES,
    }
    with pytest.raises(ValidationError, match="observation and reason"):
        Event(
            status=EventStatus.RESOLVED,
            resolved_by_observation_id="obs-2",
            **common,
        )
    with pytest.raises(ValidationError, match="only resolved"):
        Event(
            status=EventStatus.OPEN,
            resolved_by_observation_id="obs-2",
            resolution_reason="slot occupied",
            **common,
        )
