from civ6_workflow.models import EventLevel, ExecutionMode, PlanBundle, RuntimeSnapshot
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore


def _compile(compiler, snapshot):
    return getattr(compiler, "compile")(normalize_runtime_snapshot(snapshot))


def _snapshot(turn: int, units):
    return RuntimeSnapshot(
        turn=turn,
        game_id="game-1",
        overview={"turn": turn},
        units=units,
    )


def _save_builder_plans(store: WorkflowStore, plans):
    store.save_plan_bundle(
        "game-1",
        30,
        PlanBundle(
            plan_id="builder-reservations",
            summary="reserve upcoming builders",
            builder_plan_updates=plans,
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"unit_move", "builder_improve"},
    )


def test_shared_builder_candidate_is_blocking_l3(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    _save_builder_plans(
        store,
        [
            {"builder_key": "builder-a"},
            {"builder_key": "builder-b"},
        ],
    )
    compiler = DeterministicRuleCompiler(store)
    _compile(compiler, _snapshot(30, []))

    result = _compile(
        compiler,
        _snapshot(
            31,
            [
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_BUILDER",
                    "x": 3,
                    "y": 4,
                    "build_charges": 3,
                }
            ],
        ),
    )

    event = next(
        event
        for event in result.events
        if event.event_type == "builder_binding_ambiguous"
    )
    assert event.level is EventLevel.L3
    assert event.blocking is True
    builders = store.current_context("game-1")["builders"]
    assert builders["builder-a"].get("assigned_unit_id") is None
    assert builders["builder-b"].get("assigned_unit_id") is None


def test_missing_origin_metadata_is_blocking_l3(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    _save_builder_plans(
        store,
        [
            {
                "builder_key": "city-one-builder",
                "origin_city_id": 1,
            }
        ],
    )
    compiler = DeterministicRuleCompiler(store)
    _compile(compiler, _snapshot(30, []))

    result = _compile(
        compiler,
        _snapshot(
            31,
            [
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_BUILDER",
                    "x": 3,
                    "y": 4,
                    "build_charges": 3,
                }
            ],
        ),
    )

    event = next(
        event
        for event in result.events
        if event.event_type == "builder_binding_unmatched"
    )
    assert event.level is EventLevel.L3
    assert event.blocking is True
    assert event.payload["plans"]["city-one-builder"]["candidate_unit_ids"] == ["9"]
    assert (
        store.current_context("game-1")["builders"]["city-one-builder"].get(
            "assigned_unit_id"
        )
        is None
    )


def test_builder_bound_to_one_plan_is_not_unmatched_for_another(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    _save_builder_plans(
        store,
        [
            {
                "builder_key": "city-one-builder",
                "origin_city_id": 1,
                "path": [[3, 4], [4, 4]],
                "target": {
                    "x": 4,
                    "y": 4,
                    "improvement_type": "IMPROVEMENT_MINE",
                },
            },
            {
                "builder_key": "city-two-builder",
                "origin_city_id": 2,
            },
        ],
    )
    compiler = DeterministicRuleCompiler(store)
    _compile(compiler, _snapshot(30, []))

    result = _compile(
        compiler,
        _snapshot(
            31,
            [
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_BUILDER",
                    "origin_city_id": 1,
                    "x": 3,
                    "y": 4,
                    "build_charges": 3,
                }
            ],
        ),
    )

    assert not any(
        event.event_type == "builder_binding_unmatched" for event in result.events
    )
    builders = store.current_context("game-1")["builders"]
    assert builders["city-one-builder"]["assigned_unit_id"] == 9
    assert builders["city-two-builder"].get("assigned_unit_id") is None
