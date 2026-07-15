"""Bounded strategic decision gaps."""

from __future__ import annotations

from enum import StrEnum

from .base import DomainModel, SubjectRef


class DecisionGapStatus(StrEnum):
    OPEN = "OPEN"
    CONTEXT_REQUIRED = "CONTEXT_REQUIRED"
    PLANNER_REQUESTED = "PLANNER_REQUESTED"
    PROPOSED = "PROPOSED"
    RESOLVED = "RESOLVED"
    DEFERRED_TO_HUMAN = "DEFERRED_TO_HUMAN"
    CANCELLED = "CANCELLED"
    SUPERSEDED = "SUPERSEDED"


class DecisionRoute(StrEnum):
    PLANNER = "PLANNER"
    HUMAN = "HUMAN"


class DecisionGap(DomainModel):
    decision_gap_id: str
    source_event_ids: tuple[str, ...]
    gap_type: str
    scope: str
    subjects: tuple[SubjectRef, ...]
    observation_id: str
    relevant_plan_revisions: tuple[str, ...] = ()
    required_context: tuple[str, ...] = ()
    route: DecisionRoute
    status: DecisionGapStatus
    cooldown_key: str
