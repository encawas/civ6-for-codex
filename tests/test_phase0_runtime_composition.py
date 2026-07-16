import json
from pathlib import Path

import civ6_workflow
from civ6_workflow import (
    actions,
    codex_planner,
    conditions,
    engine,
    mcp_port,
    models,
    replay,
    rules,
    safe_web_ui,
    store,
    validation,
    web_ui,
    workflow_prompt,
)
from civ6_workflow.runtime_safety import CommitSafeWorkflowEngine
from civ6_workflow.safe_rules import SafeDeterministicRuleCompiler
from civ6_workflow.settler_rules import SettlerDeterministicRuleCompiler


BASE_CONDITION_TYPES = {
    "turn_at_least",
    "turn_equals",
    "no_blocker_type",
    "field_equals",
    "field_in",
    "entity_exists",
    "city_production_equals",
    "city_has_no_production",
    "research_unselected",
    "research_available",
    "research_equals",
    "civic_unselected",
    "civic_available",
    "civic_equals",
    "unit_at",
    "unit_has_moves",
    "unit_no_moves",
    "unit_has_build_charge",
    "unit_build_charges_equals",
    "unit_can_improve",
}


def _identity(value) -> str:
    return f"{value.__module__}.{value.__name__}"


def test_imp_001_effective_public_engine_is_commit_safe():
    """IMP-001 (MIGRATE): package import exposes the commit-safe engine."""

    assert civ6_workflow.WorkflowEngine is CommitSafeWorkflowEngine
    assert engine.WorkflowEngine is CommitSafeWorkflowEngine


def test_imp_002_source_defined_base_engine_remains_distinct():
    """IMP-002 (REPLACE): hidden composition still wraps a distinct base engine."""

    source_base = next(
        candidate
        for candidate in CommitSafeWorkflowEngine.__mro__
        if candidate.__module__ == "civ6_workflow.engine"
        and candidate.__name__ == "WorkflowEngine"
    )

    assert source_base is not engine.WorkflowEngine
    assert source_base in CommitSafeWorkflowEngine.__mro__


def test_imp_003_compiler_inheritance_chain_is_frozen():
    """IMP-003: compiler overlays retain the exact current inheritance chain."""

    source_compiler = SafeDeterministicRuleCompiler.__base__

    assert SettlerDeterministicRuleCompiler.__base__ is SafeDeterministicRuleCompiler
    assert source_compiler.__module__ == "civ6_workflow.rules"
    assert source_compiler.__name__ == "DeterministicRuleCompiler"
    assert SettlerDeterministicRuleCompiler.__mro__[:3] == (
        SettlerDeterministicRuleCompiler,
        SafeDeterministicRuleCompiler,
        source_compiler,
    )


def test_imp_003_import_time_replacement_inventory_matches_fixture():
    """IMP-003 (REPLACE): every current replacement is machine-checkable."""

    expected = json.loads(
        (Path(__file__).parent / "fixtures" / "runtime_composition_v1.json").read_text(
            encoding="utf-8"
        )
    )
    actual = {
        "engine.WorkflowEngine": _identity(engine.WorkflowEngine),
        "engine.EngineConfig": _identity(engine.EngineConfig),
        "rules.DeterministicRuleCompiler": _identity(rules.DeterministicRuleCompiler),
        "rules.compiler_inheritance": [
            _identity(candidate)
            for candidate in SettlerDeterministicRuleCompiler.__mro__[:3]
        ],
        "store.WorkflowStore": _identity(store.WorkflowStore),
        "mcp_port.Civ6GamePort": _identity(mcp_port.Civ6GamePort),
        "conditions.ConditionEvaluator": _identity(conditions.ConditionEvaluator),
        "replay.RecordingGamePort": _identity(replay.RecordingGamePort),
        "replay.ReplayGamePort": _identity(replay.ReplayGamePort),
        "web_ui.ControlPanelState": _identity(web_ui.ControlPanelState),
        "web_ui.ControlPanelHandler": _identity(web_ui.ControlPanelHandler),
        "web_ui.ControlPanelHTTPServer": _identity(web_ui.ControlPanelHTTPServer),
        "models.PlanBundle": _identity(models.PlanBundle),
        "models.AgentRequest": _identity(models.AgentRequest),
        "models.TickMetrics": _identity(models.TickMetrics),
        "unit_found_city_spec": {
            "tool_name": actions.ACTION_REGISTRY["unit_found_city"].tool_name,
            "required_arguments": sorted(
                actions.ACTION_REGISTRY["unit_found_city"].required_arguments
            ),
            "fixed_arguments": actions.ACTION_REGISTRY[
                "unit_found_city"
            ].fixed_arguments,
            "retry_safe_after_unknown": actions.ACTION_REGISTRY[
                "unit_found_city"
            ].retry_safe_after_unknown,
        },
        "unit_found_city_entity_types": sorted(
            validation.ACTION_ENTITY_TYPES["unit_found_city"]
        ),
        "codex_system_instructions_extended": (
            codex_planner.SYSTEM_INSTRUCTIONS
            == workflow_prompt.EXTENDED_SYSTEM_INSTRUCTIONS
        ),
        "control_panel_html_enhanced": (
            web_ui.CONTROL_PANEL_HTML == safe_web_ui.ENHANCED_CONTROL_PANEL_HTML
        ),
        "condition_types": sorted(
            set(validation.DEFAULT_CONDITION_TYPES) - BASE_CONDITION_TYPES
        ),
    }

    assert actual == expected
