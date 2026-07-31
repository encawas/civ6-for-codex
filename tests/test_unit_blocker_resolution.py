from civ6_workflow.conditions import ConditionEvaluator
from civ6_workflow.models import RuntimeSnapshot


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


def test_canonical_evaluator_handles_settler_movement_evidence():
    evaluator = ConditionEvaluator()
    original = _snapshot(
        {
            "unit_id": 9,
            "unit_type": "UNIT_SETTLER",
            "x": 4,
            "y": 5,
            "moves_remaining": 2,
        }
    )
    moved = original.model_copy(
        update={
            "units": [
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_SETTLER",
                    "x": 5,
                    "y": 5,
                    "moves_remaining": 1,
                }
            ]
        }
    )
    condition = {"type": "unit_moved_from", "unit_id": 9, "x": 4, "y": 5}

    assert not evaluator.evaluate(condition, original).valid
    assert evaluator.evaluate(condition, moved).valid
