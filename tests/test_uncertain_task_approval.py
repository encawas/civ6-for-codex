import pytest
from pydantic import ValidationError

from civ6_workflow.domain import (
    ApprovalStatus,
    SubjectRef,
    Task,
    TaskStatus,
    build_task_idempotency_key,
)


def _uncertain_task(approval_status: ApprovalStatus) -> Task:
    subject = SubjectRef(subject_type="unit", subject_id="7")
    desired_outcome = {"position": {"x": 3, "y": 4}}
    idempotency_key = build_task_idempotency_key(
        game_session_id="game-1",
        subject=subject,
        slot="unit:7:order",
        desired_outcome=desired_outcome,
        source_plan_revision=2,
        earliest_turn=12,
        latest_turn=12,
    )
    return Task(
        task_id="task-uncertain",
        game_session_id="game-1",
        idempotency_key=idempotency_key,
        task_type="unit_move",
        subject=subject,
        slot="unit:7:order",
        arguments={"unit_id": 7, "target_x": 3, "target_y": 4},
        desired_outcome=desired_outcome,
        source_plan_id="plan-1",
        source_plan_revision=2,
        created_from_observation_id="obs-1",
        status=TaskStatus.UNCERTAIN,
        earliest_turn=12,
        latest_turn=12,
        must_complete_before_end_turn=True,
        approval_status=approval_status,
        preconditions=(),
        postconditions=(),
    )


def test_uncertain_task_rejects_unsatisfied_approval():
    with pytest.raises(ValidationError, match="must satisfy approval"):
        _uncertain_task(ApprovalStatus.REQUIRED)


@pytest.mark.parametrize(
    "approval_status",
    [ApprovalStatus.APPROVED, ApprovalStatus.NOT_REQUIRED],
)
def test_uncertain_task_accepts_satisfied_approval(approval_status):
    task = _uncertain_task(approval_status)

    assert task.status is TaskStatus.UNCERTAIN
    assert task.approval_status is approval_status
