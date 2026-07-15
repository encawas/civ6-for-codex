"""Concrete work items, stable identity, and slot ownership."""

from __future__ import annotations

from enum import StrEnum
from hashlib import sha256
import json

from pydantic import Field

from .base import ApprovalStatus, Condition, DomainModel, JsonValue, SubjectRef


class TaskStatus(StrEnum):
    PROPOSED = "PROPOSED"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    READY = "READY"
    EXECUTING = "EXECUTING"
    VERIFYING = "VERIFYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNCERTAIN = "UNCERTAIN"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    SUPERSEDED = "SUPERSEDED"
    ESCALATED = "ESCALATED"


ACTIVE_TASK_STATUSES = frozenset(
    {
        TaskStatus.PROPOSED,
        TaskStatus.AWAITING_APPROVAL,
        TaskStatus.READY,
        TaskStatus.EXECUTING,
        TaskStatus.VERIFYING,
        TaskStatus.UNCERTAIN,
    }
)


def build_task_idempotency_key(
    *,
    game_session_id: str,
    subject: SubjectRef,
    slot: str,
    desired_outcome: dict[str, JsonValue],
    source_plan_revision: int | None,
    earliest_turn: int,
    latest_turn: int | None,
) -> str:
    identity = {
        "game_session_id": game_session_id,
        "subject": subject.model_dump(mode="json"),
        "slot": slot,
        "desired_outcome": desired_outcome,
        "source_plan_revision": source_plan_revision,
        "execution_window": [earliest_turn, latest_turn],
    }
    encoded = json.dumps(
        identity,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"task:{sha256(encoded).hexdigest()}"


class Task(DomainModel):
    task_id: str
    game_session_id: str
    idempotency_key: str
    task_type: str
    subject: SubjectRef
    slot: str
    arguments: dict[str, JsonValue]
    desired_outcome: dict[str, JsonValue]
    source_plan_id: str | None = None
    source_plan_revision: int | None = Field(default=None, ge=1)
    source_event_ids: tuple[str, ...] = ()
    created_from_observation_id: str
    status: TaskStatus
    priority: int = 0
    earliest_turn: int = Field(ge=0)
    latest_turn: int | None = Field(default=None, ge=0)
    must_complete_before_end_turn: bool
    approval_status: ApprovalStatus
    preconditions: tuple[Condition, ...]
    postconditions: tuple[Condition, ...]

    def model_post_init(self, __context: object) -> None:
        if (self.source_plan_id is None) != (self.source_plan_revision is None):
            raise ValueError("plan ID and revision must be supplied together")
        if self.latest_turn is not None and self.latest_turn < self.earliest_turn:
            raise ValueError("latest_turn must not precede earliest_turn")
        if self.status is TaskStatus.READY and self.approval_status not in {
            ApprovalStatus.NOT_REQUIRED,
            ApprovalStatus.APPROVED,
        }:
            raise ValueError("a ready task must satisfy its approval requirement")
        expected_key = build_task_idempotency_key(
            game_session_id=self.game_session_id,
            subject=self.subject,
            slot=self.slot,
            desired_outcome=self.desired_outcome,
            source_plan_revision=self.source_plan_revision,
            earliest_turn=self.earliest_turn,
            latest_turn=self.latest_turn,
        )
        if self.idempotency_key != expected_key:
            raise ValueError("idempotency_key does not match task semantics")

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE_TASK_STATUSES


def tasks_conflict(left: Task, right: Task) -> bool:
    if not left.is_active or not right.is_active:
        return False
    if left.game_session_id != right.game_session_id:
        return False
    if left.idempotency_key == right.idempotency_key:
        return True
    if left.slot != right.slot:
        return False
    left_end = left.latest_turn if left.latest_turn is not None else float("inf")
    right_end = right.latest_turn if right.latest_turn is not None else float("inf")
    return left.earliest_turn <= right_end and right.earliest_turn <= left_end
