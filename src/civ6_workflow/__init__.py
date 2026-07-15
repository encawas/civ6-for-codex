"""Event-driven Civilization VI workflow runtime."""

# Extend the public protocol before runtime modules import model classes. This
# preserves backwards-compatible constructors while adding event-resolution and
# focused-information phases to the generated planner schema.
from . import models as _models_module
from .workflow_protocol import (
    EventResolution,
    InformationRequest,
    ResolutionDisposition,
    WorkflowAgentRequest,
    WorkflowPlanBundle,
    WorkflowTickMetrics,
)

_models_module.PlanBundle = WorkflowPlanBundle
_models_module.AgentRequest = WorkflowAgentRequest
_models_module.TickMetrics = WorkflowTickMetrics

# Install the settler action before validation and engine defaults snapshot the
# registry.
from . import actions as _actions_module
from .actions import ActionSpec

_actions_module.ACTION_REGISTRY["unit_found_city"] = ActionSpec(
    tool_name="unit_action",
    required_arguments=frozenset({"unit_id"}),
    fixed_arguments={"action": "found_city"},
    retry_safe_after_unknown=False,
)

# Extend the auditable validation/condition language used by irreversible
# settler operations.
from . import validation as _validation_module

_validation_module.DEFAULT_CONDITION_TYPES.update(
    {
        "unit_absent",
        "unit_moved_from",
        "unit_type_contains",
        "city_count_at_least",
    }
)
_validation_module.ACTION_ENTITY_TYPES["unit_found_city"] = {"unit"}

from . import conditions as _conditions_module
from .workflow_conditions import WorkflowConditionEvaluator

_conditions_module.ConditionEvaluator = WorkflowConditionEvaluator

# Install hardened persistence semantics.
from . import store as _store_module
from .safe_store import SafeWorkflowStore

_store_module.WorkflowStore = SafeWorkflowStore

# Install deterministic city/builder/unit logic plus the settler state machine.
from . import rules as _rules_module
from .safe_rules import SafeDeterministicRuleCompiler
from .settler_rules import SettlerDeterministicRuleCompiler

_rules_module.DeterministicRuleCompiler = SettlerDeterministicRuleCompiler

# Install the structured-read/MCP action port with focused read-only queries.
from . import mcp_port as _mcp_port_module
from .safe_mcp_port import SafeCiv6GamePort

_mcp_port_module.Civ6GamePort = SafeCiv6GamePort

# The planner remains disconnected from tools; the prompt describes the workflow
# protocol and asks for explicit event coverage or information requests.
from . import codex_planner as _codex_planner_module
from .workflow_prompt import EXTENDED_SYSTEM_INSTRUCTIONS

_codex_planner_module.SYSTEM_INSTRUCTIONS = EXTENDED_SYSTEM_INSTRUCTIONS

# Install serialized ticks, execution-mode safety, event coverage, focused query
# rounds, provider error classification, cross-tick backoff, and unknown-commit
# protection for irreversible actions.
from . import engine as _engine_module
from .safe_engine import SafeEngineConfig
from .runtime_safety import CommitSafeWorkflowEngine

_engine_module.EngineConfig = SafeEngineConfig
_engine_module.WorkflowEngine = CommitSafeWorkflowEngine

# Install strict recording/replay behavior.
from . import replay as _replay_module
from .safe_replay import SafeRecordingGamePort, SafeReplayGamePort

_replay_module.RecordingGamePort = SafeRecordingGamePort
_replay_module.ReplayGamePort = SafeReplayGamePort

# Install the enhanced localhost control panel.
from . import web_ui as _web_ui_module
from .safe_web_ui import (
    ENHANCED_CONTROL_PANEL_HTML,
    SafeControlPanelHandler,
    SafeControlPanelHTTPServer,
    SafeControlPanelState,
)

_web_ui_module.ControlPanelState = SafeControlPanelState
_web_ui_module.ControlPanelHandler = SafeControlPanelHandler
_web_ui_module.ControlPanelHTTPServer = SafeControlPanelHTTPServer
_web_ui_module.CONTROL_PANEL_HTML = ENHANCED_CONTROL_PANEL_HTML

from .engine import WorkflowEngine
from .models import (
    AgentRequest,
    EventLevel,
    GameEvent,
    PlanBundle,
    ProposedTask,
    TaskStatus,
    TickResult,
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
