"""Canonical event lifecycle."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field

from .base import DomainModel, ImmutableJsonObject, SubjectRef


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
    dedupe_key: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    subject: SubjectRef | None = None
    opened_from_observation_id: str
    last_seen_observation_id: str
    status: EventStatus
    severity: int = Field(ge=0)
    route: EventRoute
    payload: ImmutableJsonObject = {}
    resolved_by_observation_id: str | None = None
    resolution_reason: str | None = None

    def model_post_init(self, __context: object) -> None:
        is_resolved = self.status is EventStatus.RESOLVED
        has_observation = bool(self.resolved_by_observation_id)
        has_reason = bool(self.resolution_reason and self.resolution_reason.strip())
        if is_resolved and not (has_observation and has_reason):
            raise ValueError("resolved events require observation and reason")
        if not is_resolved and (has_observation or self.resolution_reason is not None):
            raise ValueError("only resolved events may contain resolution evidence")
