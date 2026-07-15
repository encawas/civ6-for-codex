"""Event-driven Civilization VI workflow runtime."""

# Install the hardened implementations before public modules import the runtime
# classes. Keeping the original modules as storage/algorithm bases avoids a
# destructive schema rewrite while making every normal package import use the
# fail-closed behavior.
from . import store as _store_module
from .safe_store import SafeWorkflowStore

_store_module.WorkflowStore = SafeWorkflowStore

from . import rules as _rules_module
from .safe_rules import SafeDeterministicRuleCompiler

_rules_module.DeterministicRuleCompiler = SafeDeterministicRuleCompiler

from . import mcp_port as _mcp_port_module
from .safe_mcp_port import SafeCiv6GamePort

_mcp_port_module.Civ6GamePort = SafeCiv6GamePort

from . import engine as _engine_module
from .safe_engine import SafeEngineConfig, SafeWorkflowEngine

_engine_module.EngineConfig = SafeEngineConfig
_engine_module.WorkflowEngine = SafeWorkflowEngine

from . import replay as _replay_module
from .safe_replay import SafeRecordingGamePort, SafeReplayGamePort

_replay_module.RecordingGamePort = SafeRecordingGamePort
_replay_module.ReplayGamePort = SafeReplayGamePort

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
    "GameEvent",
    "PlanBundle",
    "ProposedTask",
    "TaskStatus",
    "TickResult",
    "WorkflowEngine",
]
