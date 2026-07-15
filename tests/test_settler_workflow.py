from civ6_workflow.models import ExecutionMode, PlanBundle, RiskLevel, RuntimeSnapshot
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore


def _snapshot(*, x=2, y=3, cities=None):
    return RuntimeSnapshot(
        turn=10,
        game_id="settler-game",
        overview={"turn": 10},
        cities=cities or [{"city_id": 1, "currently_building": "UNIT_WARRIOR"}],
        units=[
            {
                "unit_id": 7,
                "unit_type": "UNIT_SETTLER",
                "x": x,
                "y": y,
                "moves_remaining": 2,
            }
        ],
        blockers=[
            {
                "type": "end_turn_blocker",
                "blocking_type": "ENDTURN_BLOCKING_UNITS",
                "message": "unit needs orders",
            }
        ],
    )


def _save_plan(store, target):
    store.save_plan_bundle(
        "settler-game",
        10,
        PlanBundle(
            plan_id="settler-plan",
            summary="approved settlement site",
            unit_plan_updates=[
                {"unit_id": 7, "goal": "found_city", "target": target}
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"unit_move"},
    )


def test_unplanned_settler_becomes_site_selection_event(tmp_path):
    compiler = DeterministicRuleCompiler(WorkflowStore(tmp_path / "state.sqlite3"))
    result = compiler.compile(_snapshot())
    assert any(
        event.event_type == "settler_site_selection_required"
        for event in result.events
    )
    assert not any(
        event.event_type == "special_unit_orders_required"
        for event in result.events
    )


def test_zero_city_start_detects_settler_without_reported_blocker(tmp_path):
    snapshot = _snapshot().model_copy(
        update={
            "overview": {"turn": 10, "num_cities": 0},
            "cities": [],
            "blockers": [],
        }
    )
    compiler = DeterministicRuleCompiler(WorkflowStore(tmp_path / "state.sqlite3"))

    result = compiler.compile(snapshot)

    assert [event.event_type for event in result.events] == [
        "settler_site_selection_required"
    ]


def test_approved_settler_target_compiles_safe_travel(tmp_path):
    store = WorkflowStore(tmp_path / "state.sqlite3")
    _save_plan(store, {"x": 8, "y": 9})
    result = DeterministicRuleCompiler(store).compile(_snapshot())
    task = next(task for task in result.bundle.tasks if task.entity_id == 7)
    assert task.action_type == "unit_move"
    assert task.arguments["target_x"] == 8
    assert task.postconditions == [
        {"type": "unit_moved_from", "unit_id": 7, "x": 2, "y": 3}
    ]
    assert task.risk is RiskLevel.HIGH
    assert task.requires_confirmation is True


def test_settler_at_target_compiles_found_city(tmp_path):
    store = WorkflowStore(tmp_path / "state.sqlite3")
    _save_plan(store, {"x": 8, "y": 9})
    result = DeterministicRuleCompiler(store).compile(_snapshot(x=8, y=9))
    task = next(task for task in result.bundle.tasks if task.entity_id == 7)
    assert task.action_type == "unit_found_city"
    assert {"type": "unit_absent", "unit_id": 7} in task.postconditions
    assert {"type": "city_count_at_least", "count": 2} in task.postconditions
    assert task.requires_confirmation is True
