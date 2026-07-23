"""Event-driven Civilization VI workflow runtime.

Importing this package is intentionally passive. Concrete runtime composition
belongs to :mod:`civ6_workflow.bootstrap`.
"""

from .engine import WorkflowEngine
from .models import (
    EventLevel,
    GameEvent,
    ProposedTask,
    TaskStatus,
    TickResult,
)
from .workflow_protocol import (
    EventResolution,
    InformationRequest,
    ResolutionDisposition,
    WorkflowAgentRequest as AgentRequest,
    WorkflowPlanBundle as PlanBundle,
)

__all__ = [
    "AgentRequest",
    "EventLevel",
    "EventResolution",
    "GameEvent",
    "InformationRequest",
    "PlanBundle",
    "ProposedTask",
    "ResolutionDisposition",
    "TaskStatus",
    "TickResult",
    "WorkflowEngine",
]
