from __future__ import annotations

from enum import Enum, IntEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EventLevel(IntEnum):
    L0 = 0
    L1 = 1
    L2 = 2
    L3 = 3


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TaskStatus(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    VERIFYING = "verifying"
    AWAITING_CONFIRMATION = "awaiting_confirmation"
    DONE = "done"
    BLOCKED = "blocked"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    ESCALATED = "escalated"
    UNCERTAIN = "uncertain"


class ExecutionMode(str, Enum):
    READONLY = "readonly"
    CONFIRM = "confirm"
    AUTO = "auto"


class MutationDeliveryStatus(str, Enum):
    PROVEN_NOT_SENT = "proven_not_sent"
    EXPLICITLY_REJECTED = "explicitly_rejected"
    ACKNOWLEDGED = "acknowledged"
    UNKNOWN = "unknown"


class GameEvent(StrictModel):
    event_id: str = Field(default_factory=lambda: f"evt_{uuid4().hex}")
    event_type: str
    turn: int = Field(ge=0)
    entity_type: str | None = None
    entity_id: str | int | None = None
    level: EventLevel = EventLevel.L1
    risk: RiskLevel = RiskLevel.LOW
    blocking: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)
    dedupe_key: str
    first_seen_turn: int | None = None
    last_seen_turn: int | None = None

    @field_validator("dedupe_key")
    @classmethod
    def validate_dedupe_key(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("dedupe_key must not be empty")
        return value


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


class StoredTask(ProposedTask):
    plan_id: str
    created_turn: int = Field(ge=0)
    created_from_observation_id: str | None = None
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = Field(default=0, ge=0)
    max_retries: int = Field(default=2, ge=0, le=10)
    last_error: str | None = None
    approved_by: str | None = None


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


class AgentRequest(StrictModel):
    request_id: str = Field(default_factory=lambda: f"req_{uuid4().hex}")
    turn: int = Field(ge=0)
    execution_mode: ExecutionMode
    trigger_events: list[GameEvent]
    current_strategy: dict[str, Any] = Field(default_factory=dict)
    relevant_state: dict[str, Any] = Field(default_factory=dict)
    current_plans: dict[str, Any] = Field(default_factory=dict)
    constraints: dict[str, Any] = Field(default_factory=dict)
    information_results: dict[str, Any] = Field(default_factory=dict)


class ActionResult(StrictModel):
    success: bool
    blocked: bool = False
    message: str = ""
    details: dict[str, Any] = Field(default_factory=dict)
    delivery_status: MutationDeliveryStatus | None = None

    @property
    def effective_delivery_status(self) -> MutationDeliveryStatus:
        if self.delivery_status is not None:
            return self.delivery_status
        if self.success:
            return MutationDeliveryStatus.ACKNOWLEDGED
        if self.blocked:
            return MutationDeliveryStatus.EXPLICITLY_REJECTED
        return MutationDeliveryStatus.UNKNOWN


class TickMetrics(StrictModel):
    state_query_seconds: float = 0.0
    normalization_seconds: float = 0.0
    task_execution_seconds: float = 0.0
    agent_seconds: float = 0.0
    verification_seconds: float = 0.0
    task_materialization_seconds: float = 0.0
    mutation_delivery_seconds: float = 0.0
    persistence_seconds: float = 0.0
    total_seconds: float = 0.0
    mcp_call_count: int = 0
    mutation_count: int = 0
    agent_call_count: int = 0
    agent_attempt_count: int = Field(default=0, ge=0)
    agent_success_count: int = Field(default=0, ge=0)
    information_query_count: int = Field(default=0, ge=0)
    logical_planner_request_count: int = Field(default=0, ge=0)
    provider_attempt_count: int = Field(default=0, ge=0)
    information_round_count: int = Field(default=0, ge=0)
    duplicate_request_suppression_count: int = Field(default=0, ge=0)
    planner_context_bytes: int = Field(default=0, ge=0)


class TickResult(StrictModel):
    turn: int
    executed_task_ids: list[str] = Field(default_factory=list)
    blocked_task_ids: list[str] = Field(default_factory=list)
    failed_task_ids: list[str] = Field(default_factory=list)
    events: list[GameEvent] = Field(default_factory=list)
    agent_invoked: bool = False
    plan_id: str | None = None
    planner_request_id: str | None = None
    turn_ended: bool = False
    paused: bool = False
    pause_reason: str | None = None
    metrics: TickMetrics = Field(default_factory=TickMetrics)
    tick_id: str | None = None
    runtime_state: str | None = None
    workflow_tick: dict[str, Any] | None = None


class RuntimeSnapshot(StrictModel):
    turn: int = Field(ge=0)
    game_id: str
    overview: dict[str, Any] = Field(default_factory=dict)
    tech_civics: dict[str, Any] | list[Any] = Field(default_factory=dict)
    notifications: dict[str, Any] | list[Any] = Field(default_factory=dict)
    diplomacy: dict[str, Any] | list[Any] = Field(default_factory=dict)
    trades: dict[str, Any] | list[Any] = Field(default_factory=dict)
    cities: dict[str, Any] | list[Any] = Field(default_factory=dict)
    units: dict[str, Any] | list[Any] | None = None
    blockers: list[dict[str, Any]] = Field(default_factory=list)


class RuntimeConfig(StrictModel):
    database_path: str = "state/civ6-workflow.sqlite3"
    execution_mode: ExecutionMode = ExecutionMode.CONFIRM
    auto_end_turn: bool = False
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    max_agent_calls_per_turn: int = Field(default=1, ge=0, le=2)
    max_turn_seconds: int = Field(default=300, ge=10)


ActionType = Literal[
    "city_set_production",
    "set_research",
    "set_civic",
    "unit_move",
    "unit_found_city",
    "builder_improve",
    "unit_heal",
    "unit_fortify",
    "unit_skip",
]
