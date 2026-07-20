import json
import os
import subprocess
import sys
from pathlib import Path
from types import MappingProxyType

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
    store,
    validation,
    web_ui,
    workflow_prompt,
)
from civ6_workflow.bootstrap import compose_runtime
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.store import WorkflowStore


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _Game:
    call_count = 0


class _Planner:
    async def plan(self, request):
        raise AssertionError("composition test does not invoke the planner")


def _identity(value) -> str:
    return f"{value.__module__}.{value.__name__}"


def _composition_snapshot() -> dict:
    return {
        "package.WorkflowEngine": _identity(civ6_workflow.WorkflowEngine),
        "engine.WorkflowEngine": _identity(engine.WorkflowEngine),
        "engine.EngineConfig": _identity(engine.EngineConfig),
        "rules.DeterministicRuleCompiler": _identity(rules.DeterministicRuleCompiler),
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
            "fixed_arguments": dict(
                actions.ACTION_REGISTRY["unit_found_city"].fixed_arguments
            ),
            "retry_classification": actions.ACTION_REGISTRY[
                "unit_found_city"
            ].retry_classification.value,
        },
        "unit_found_city_entity_types": sorted(
            validation.ACTION_ENTITY_TYPES["unit_found_city"]
        ),
        "codex_system_instructions_extended": (
            codex_planner.SYSTEM_INSTRUCTIONS
            == workflow_prompt.EXTENDED_SYSTEM_INSTRUCTIONS
        ),
        "control_panel_html_enhanced": "plannerBtn" in web_ui.CONTROL_PANEL_HTML,
        "condition_types": sorted(validation.DEFAULT_CONDITION_TYPES),
    }


def test_imp_001_public_engine_is_the_canonical_engine():
    """IMP-001 (MIGRATED): public and source Engine identities are identical."""

    assert civ6_workflow.WorkflowEngine is WorkflowEngine
    assert engine.WorkflowEngine is WorkflowEngine
    assert WorkflowEngine.__module__ == "civ6_workflow.engine"
    assert WorkflowEngine.__mro__ == (WorkflowEngine, object)


def test_imp_002_bootstrap_constructs_the_explicit_runtime_graph(tmp_path: Path):
    """IMP-002 (REPLACED): runtime dependencies are wired by bootstrap."""

    workflow_store = WorkflowStore(tmp_path / "composition.sqlite3")
    game = _Game()
    planner = _Planner()

    composition = compose_runtime(
        store=workflow_store,
        game=game,
        planner=planner,
        engine_config=EngineConfig(auto_end_turn=False),
    )

    assert composition.store is workflow_store
    assert composition.game is game
    assert composition.planner is planner
    assert composition.engine.store is workflow_store
    assert composition.engine.game is game
    assert composition.engine.planner is planner
    assert type(composition.engine) is WorkflowEngine


def test_imp_003_canonical_composition_matches_fixture():
    """IMP-003 (REPLACED): the explicit canonical graph is machine-checkable."""

    expected = json.loads(
        (Path(__file__).parent / "fixtures" / "runtime_composition_v2.json").read_text(
            encoding="utf-8"
        )
    )

    assert _composition_snapshot() == expected
    assert isinstance(actions.ACTION_REGISTRY, MappingProxyType)


def test_imp_004_import_order_does_not_mutate_runtime_identity():
    """IMP-004: package import order cannot replace classes or registries."""

    program = """
import json
{imports}
from civ6_workflow import actions, conditions, engine, mcp_port, replay, rules, store, web_ui
before = {{
    "engine": id(engine.WorkflowEngine),
    "rules": id(rules.DeterministicRuleCompiler),
    "store": id(store.WorkflowStore),
    "mcp": id(mcp_port.Civ6GamePort),
    "conditions": id(conditions.ConditionEvaluator),
    "record": id(replay.RecordingGamePort),
    "replay": id(replay.ReplayGamePort),
    "state": id(web_ui.ControlPanelState),
    "actions": tuple(sorted(actions.ACTION_REGISTRY)),
}}
import civ6_workflow
after = {{
    "engine": id(engine.WorkflowEngine),
    "rules": id(rules.DeterministicRuleCompiler),
    "store": id(store.WorkflowStore),
    "mcp": id(mcp_port.Civ6GamePort),
    "conditions": id(conditions.ConditionEvaluator),
    "record": id(replay.RecordingGamePort),
    "replay": id(replay.ReplayGamePort),
    "state": id(web_ui.ControlPanelState),
    "actions": tuple(sorted(actions.ACTION_REGISTRY)),
}}
print(json.dumps({{"stable": before == after, "modules": [
    engine.WorkflowEngine.__module__,
    rules.DeterministicRuleCompiler.__module__,
    store.WorkflowStore.__module__,
    mcp_port.Civ6GamePort.__module__,
]}}))
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")
    outputs = []
    for imports in (
        "import civ6_workflow.engine\nimport civ6_workflow.rules",
        "import civ6_workflow\nimport civ6_workflow.bootstrap",
    ):
        process = subprocess.run(
            [sys.executable, "-c", program.format(imports=imports)],
            cwd=PROJECT_ROOT,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        outputs.append(json.loads(process.stdout))

    assert outputs == [
        {
            "stable": True,
            "modules": [
                "civ6_workflow.engine",
                "civ6_workflow.rules",
                "civ6_workflow.store",
                "civ6_workflow.mcp_port",
            ],
        },
        {
            "stable": True,
            "modules": [
                "civ6_workflow.engine",
                "civ6_workflow.rules",
                "civ6_workflow.store",
                "civ6_workflow.mcp_port",
            ],
        },
    ]


def test_imp_004_legacy_patch_modules_are_removed():
    production_modules = {
        path.name for path in (PROJECT_ROOT / "src" / "civ6_workflow").glob("*.py")
    }

    assert not {name for name in production_modules if name.startswith("safe_")}
    assert {
        "workflow_engine.py",
        "runtime_safety.py",
        "settler_rules.py",
        "workflow_conditions.py",
    }.isdisjoint(production_modules)
