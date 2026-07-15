from pathlib import Path

from civ6_workflow.models import ExecutionMode, PlanBundle, RuntimeSnapshot
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore


def test_city_followup_queue_becomes_verified_task(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(
            plan_id="agent-plan-1",
            summary="plan city queue",
            city_plan_updates=[
                {
                    "city_id": 1,
                    "role": "core",
                    "followup_queue": [
                        {"item_type": "UNIT", "item_name": "UNIT_BUILDER"},
                        {"item_type": "UNIT", "item_name": "UNIT_SETTLER"},
                    ],
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )
    snapshot = RuntimeSnapshot(
        turn=12,
        game_id="game-1",
        overview={"turn": 12},
        cities=[{"city_id": 1, "currently_building": "NONE"}],
    )

    compiled = DeterministicRuleCompiler(store).compile(snapshot)
    assert compiled.bundle is not None
    task = compiled.bundle.tasks[0]
    assert task.action_type == "city_set_production"
    assert task.arguments["item_name"] == "UNIT_BUILDER"
    assert task.preconditions[1]["type"] == "city_has_no_production"
    assert task.postconditions[0]["type"] == "city_production_equals"


def test_builder_path_emits_one_step_only(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        30,
        PlanBundle(
            plan_id="agent-plan-2",
            summary="plan builder route",
            builder_plan_updates=[
                {
                    "builder_key": "beijing-builder-1",
                    "assigned_unit_id": 9,
                    "path": [[3, 4], [4, 4], [5, 4]],
                    "target": {
                        "x": 5,
                        "y": 4,
                        "improvement_type": "IMPROVEMENT_MINE",
                    },
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"unit_move", "builder_improve"},
    )
    snapshot = RuntimeSnapshot(
        turn=30,
        game_id="game-1",
        overview={"turn": 30},
        cities=[],
        units=[
            {
                "unit_id": 9,
                "unit_type": "UNIT_BUILDER",
                "x": 3,
                "y": 4,
                "build_charges": 3,
                "valid_improvements": [],
            }
        ],
    )

    compiled = DeterministicRuleCompiler(store).compile(snapshot)
    assert compiled.bundle is not None
    assert len(compiled.bundle.tasks) == 1
    task = compiled.bundle.tasks[0]
    assert task.action_type == "unit_move"
    assert task.arguments == {"unit_id": 9, "target_x": 4, "target_y": 4}
    assert task.postconditions == [{"type": "unit_at", "unit_id": 9, "x": 4, "y": 4}]


def test_builder_at_target_emits_improvement_with_charge_check(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        31,
        PlanBundle(
            plan_id="agent-plan-3",
            summary="improve target",
            builder_plan_updates=[
                {
                    "builder_key": "builder-9",
                    "assigned_unit_id": 9,
                    "path": [[5, 4]],
                    "target": {
                        "x": 5,
                        "y": 4,
                        "improvement_type": "IMPROVEMENT_MINE",
                    },
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"builder_improve"},
    )
    snapshot = RuntimeSnapshot(
        turn=31,
        game_id="game-1",
        overview={"turn": 31},
        cities=[],
        units=[
            {
                "unit_id": 9,
                "unit_type": "UNIT_BUILDER",
                "x": 5,
                "y": 4,
                "build_charges": 2,
                "valid_improvements": ["IMPROVEMENT_MINE"],
            }
        ],
    )

    compiled = DeterministicRuleCompiler(store).compile(snapshot)
    task = compiled.bundle.tasks[0]
    assert task.action_type == "builder_improve"
    assert task.postconditions[0] == {
        "type": "unit_build_charges_equals",
        "unit_id": 9,
        "charges": 1,
    }
