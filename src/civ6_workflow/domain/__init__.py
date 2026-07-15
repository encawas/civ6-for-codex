"""Canonical workflow domain contracts.

Importing this package has no domain-level runtime composition side effects.
"""

from .approvals import ApprovalDecision, ApprovalRecord
from .attempts import ActionAttempt, AttemptStatus
from .base import (
    ApprovalStatus,
    Condition,
    DomainModel,
    RetryClassification,
    SourceVersions,
    SubjectRef,
)
from .decisions import DecisionGap, DecisionGapStatus, DecisionRoute
from .events import Event, EventRoute, EventStatus
from .observations import Observation, SlotState, SlotValue, normalize_slot
from .planner import PlannerRequest, PlannerRequestStatus
from .plans import Plan, PlanSource, PlanStatus
from .tasks import (
    ACTIVE_TASK_STATUSES,
    Task,
    TaskStatus,
    build_task_idempotency_key,
    tasks_conflict,
)
from .ticks import RuntimeState, TickOutcomeKind, WorkflowTick

__all__ = [
    "ACTIVE_TASK_STATUSES",
    "ActionAttempt",
    "ApprovalDecision",
    "ApprovalRecord",
    "ApprovalStatus",
    "AttemptStatus",
    "Condition",
    "DecisionGap",
    "DecisionGapStatus",
    "DecisionRoute",
    "DomainModel",
    "Event",
    "EventRoute",
    "EventStatus",
    "Observation",
    "Plan",
    "PlanSource",
    "PlanStatus",
    "PlannerRequest",
    "PlannerRequestStatus",
    "RetryClassification",
    "RuntimeState",
    "SlotState",
    "SlotValue",
    "SourceVersions",
    "SubjectRef",
    "Task",
    "TaskStatus",
    "TickOutcomeKind",
    "WorkflowTick",
    "build_task_idempotency_key",
    "normalize_slot",
    "tasks_conflict",
]
