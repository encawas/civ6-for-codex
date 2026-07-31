from __future__ import annotations

from typing import Any
from uuid import uuid4

from pydantic import Field

from .models import RiskLevel, StrictModel


class ProposedTask(StrictModel):
    task_id: str
    action_type: str
    entity_type: str
    entity_id: str | int
    due_turn: int = Field(ge=0)
    arguments: dict[str, Any] = Field(default_factory=dict)
    preconditions: list[dict[str, Any]] = Field(default_factory=list)
    postconditions: list[dict[str, Any]] = Field(default_factory=list)
    invalidators: list[dict[str, Any]] = Field(default_factory=list)
    risk: RiskLevel = RiskLevel.LOW
    requires_confirmation: bool = False
    expires_turn: int | None = Field(default=None, ge=0)
    reason: str = Field(min_length=1, max_length=500)


class PlanBundle(StrictModel):
    plan_id: str = Field(default_factory=lambda: f"plan_{uuid4().hex}")
    summary: str = Field(min_length=1, max_length=2000)
    strategy_updates: dict[str, Any] = Field(default_factory=dict)
    city_plan_updates: list[dict[str, Any]] = Field(default_factory=list)
    unit_plan_updates: list[dict[str, Any]] = Field(default_factory=list)
    builder_plan_updates: list[dict[str, Any]] = Field(default_factory=list)
    tasks: list[ProposedTask] = Field(default_factory=list, max_length=100)
    cancel_task_ids: list[str] = Field(default_factory=list)
    next_review_turn: int | None = Field(default=None, ge=0)
    requires_human_review: bool = False
    information_requests: list[Any] = Field(default_factory=list, max_length=8)
    event_resolutions: list[Any] = Field(default_factory=list, max_length=100)
