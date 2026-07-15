"""Canonical event lifecycle."""

from __future__ import annotations

from enum import StrEnum

from .base import DomainModel, JsonValue, SubjectRef


class EventStatus(StrEnum):
    OPEN = "OPEN"
    RESOLVED = "RESOLVED"
    SUPPRESSED = "SUPPRESSED"
    SUPERSEDED = "SUPERSEDED"


class EventRoute(StrEnum):
    RULES = "RULES"
    PLANNER = "PLANNER"
    HUMAN = "HUMAN"
    SYSTEM = "SYSTEM"


class Event(DomainModel):
    event_id: str
    game_session_id: str
    dedupe_key: str
    event_type: str
    subject: SubjectRef | None = None
    opened_from_observation_id: str
    last_seen_observation_id: str
    status: EventStatus
    severity: int
    route: EventRoute
    payload: dict[str, JsonValue] = {}
    resolved_by_observation_id: str | None = None
    resolution_reason: str | None = None

    def model_post_init(self, __context: object) -> None:
        is_resolved = self.status is EventStatus.RESOLVED
        if is_resolved != bool(self.resolved_by_observation_id):
            raise ValueError("resolved events require a resolving observation")
        if not is_resolved and self.resolution_reason is not None:
            raise ValueError("only resolved events may have a resolution reason")
