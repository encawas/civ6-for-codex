from pathlib import Path

from civ6_workflow.conditions import ConditionEvaluator
from civ6_workflow.models import EventLevel, RuntimeSnapshot
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore


def _snapshot(unit: dict) -> RuntimeSnapshot:
    return RuntimeSnapshot(
        turn=12,
        game_id="game-1",
        overview={"turn": 12},
        cities=[],
        units=[unit],
        blockers=[
            {
                "type": "end_turn_blocker",
                "blocking_type": "ENDTURN_BLOCKING_UNITS",
                "message": "A unit needs orders",
            }
        ],
    )


def test_ordinary_unplanned_unit_becomes_verified_skip(tmp_path: Path):
    compiler = DeterministicRuleCompiler(WorkflowStore(tmp_path / "workflow.sqlite3"))
    compiled = compiler.compile(
        _snapshot(
            {
                "unit_id": 41,
                "unit_type": "UNIT_WARRIOR",
                "x": 4,
                "y": 5,
                "moves_remaining": 2,
                "targets": [],
                "needs_promotion": False,
            }
        )
    )

    assert compiled.bundle is not None
    assert len(compiled.bundle.tasks) == 1
    task = compiled.bundle.tasks[0]
    assert task.action_type == "unit_skip"
    assert task.arguments == {"unit_id": 41}
    assert {"type": "unit_has_moves", "unit_id": 41} in task.preconditions
    assert task.postconditions == [{"type": "unit_no_moves", "unit_id": 41}]
    assert not any(event.level >= EventLevel.L3 for event in compiled.events)


def test_settler_and_promotion_are_not_auto_skipped(tmp_path: Path):
    compiler = DeterministicRuleCompiler(WorkflowStore(tmp_path / "workflow.sqlite3"))

    settler = compiler.compile(
        _snapshot(
            {
                "unit_id": 42,
                "unit_type": "UNIT_SETTLER",
                "moves_remaining": 2,
                "targets": [],
            }
        )
    )
    assert settler.bundle is None
    assert [event.event_type for event in settler.events] == [
        "special_unit_orders_required"
    ]
    assert settler.events[0].blocking is True

    promoted = compiler.compile(
        _snapshot(
            {
                "unit_id": 43,
                "unit_type": "UNIT_SLINGER",
                "moves_remaining": 2,
                "needs_promotion": True,
                "targets": [],
            }
        )
    )
    assert promoted.bundle is None
    assert [event.event_type for event in promoted.events] == [
        "unit_promotion_required"
    ]


def test_unit_move_conditions_verify_remaining_orders():
    evaluator = ConditionEvaluator()
    active = _snapshot(
        {
            "unit_id": 9,
            "unit_type": "UNIT_SCOUT",
            "moves_remaining": 1.5,
        }
    )
    spent = active.model_copy(
        update={
            "units": [
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_SCOUT",
                    "moves_remaining": 0,
                }
            ]
        }
    )

    assert evaluator.evaluate({"type": "unit_has_moves", "unit_id": 9}, active).valid
    assert not evaluator.evaluate({"type": "unit_no_moves", "unit_id": 9}, active).valid
    assert evaluator.evaluate({"type": "unit_no_moves", "unit_id": 9}, spent).valid
