import asyncio
import json
from pathlib import Path


from civ6_workflow.characterization import (
    RecordingGamePort,
    ScriptedPlanner,
    ScriptedSnapshot,
    ScriptedSnapshotSource,
)
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RuntimeSnapshot,
    TickMetrics,
)
from civ6_workflow.store import WorkflowStore


def _production_task(task_id: str, city_id: int, item_name: str) -> ProposedTask:
    return ProposedTask(
        task_id=task_id,
        action_type="city_set_production",
        entity_type="city",
        entity_id=city_id,
        due_turn=10,
        arguments={
            "city_id": city_id,
            "item_type": "UNIT" if item_name.startswith("UNIT_") else "BUILDING",
            "item_name": item_name,
        },
        preconditions=[{"type": "city_has_no_production", "city_id": city_id}],
        postconditions=[
            {
                "type": "city_production_equals",
                "city_id": city_id,
                "item_name": item_name,
            }
        ],
        reason=f"characterize production for city {city_id}",
    )


def _snapshot(city_1_production: str, city_2_production: str) -> RuntimeSnapshot:
    return RuntimeSnapshot(
        turn=10,
        game_id="game-1",
        overview={"turn": 10},
        cities={
            "cities": [
                {"city_id": 1, "currently_building": city_1_production},
                {"city_id": 2, "currently_building": city_2_production},
            ]
        },
    )


def _run_legacy_multi_mutation_tick(database_path: Path):
    async def scenario():
        store = WorkflowStore(database_path)
        store.save_plan_bundle(
            "game-1",
            10,
            PlanBundle(
                summary="characterize two due tasks",
                tasks=[
                    _production_task("city-1-production", 1, "UNIT_BUILDER"),
                    _production_task("city-2-production", 2, "BUILDING_MONUMENT"),
                ],
            ),
            mode=ExecutionMode.AUTO,
            auto_action_types={"city_set_production"},
        )
        delegate = ScriptedSnapshotSource(
            [
                ScriptedSnapshot(_snapshot("NONE", "NONE")),
                ScriptedSnapshot(_snapshot("UNIT_BUILDER", "NONE")),
                ScriptedSnapshot(_snapshot("UNIT_BUILDER", "BUILDING_MONUMENT")),
            ],
            action_results=[
                ActionResult(success=True, message="city 1 accepted"),
                ActionResult(success=True, message="city 2 accepted"),
            ],
            tools={
                "set_city_production",
                "get_notifications",
                "get_pending_diplomacy",
                "get_pending_trades",
            },
        )
        game = RecordingGamePort(delegate)
        game.begin_tick("legacy-tick-1")
        engine = WorkflowEngine(
            store=store,
            game=game,
            planner=ScriptedPlanner([]),
            config=EngineConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=False,
                max_agent_calls_per_turn=0,
                auto_action_types={"city_set_production"},
                allowed_action_types={"city_set_production"},
                allowed_tools={"set_city_production"},
                verification_delay_seconds=0,
            ),
        )

        result = await engine.tick()
        return result, game

    return asyncio.run(scenario())


def test_act_001_legacy_tick_now_records_one_mutation(tmp_path: Path):
    """ACT-001 (REPLACED): the former multi-mutation path is bounded."""

    result, game = _run_legacy_multi_mutation_tick(tmp_path / "legacy.sqlite3")

    assert result.executed_task_ids == []
    assert result.workflow_tick["outcome"] == "MUTATION_SENT"
    assert game.summary().mutations == 1
    assert game.summary().end_turn_mutations == 0
    game.assert_at_most_one_mutation()


def test_act_001_target_tick_has_at_most_one_mutation(tmp_path: Path):
    """ACT-001 target: bounded Tick structurally permits one mutation."""

    _, game = _run_legacy_multi_mutation_tick(tmp_path / "target.sqlite3")

    game.assert_at_most_one_mutation()


def _record_two_metrics_for_one_turn(store: WorkflowStore) -> list[dict]:
    store.record_metrics("game-1", 10, TickMetrics(mcp_call_count=1))
    store.record_metrics("game-1", 10, TickMetrics(mcp_call_count=2))
    rows = store.export_replay_state("game-1")["tables"]["turn_metrics"]
    return rows


def test_met_001_legacy_store_now_retains_each_tick_metric(tmp_path: Path):
    """MET-001 (REPLACED): migration retains both same-turn Tick metrics."""

    rows = _record_two_metrics_for_one_turn(
        WorkflowStore(tmp_path / "legacy-metrics.sqlite3")
    )

    assert len(rows) == 2
    assert [json.loads(row["metrics_json"])["mcp_call_count"] for row in rows] == [1, 2]


def test_met_001_target_store_retains_one_metrics_record_per_tick(tmp_path: Path):
    """MET-001 target: one durable metrics record exists per Tick."""

    rows = _record_two_metrics_for_one_turn(
        WorkflowStore(tmp_path / "target-metrics.sqlite3")
    )

    assert len(rows) == 2
