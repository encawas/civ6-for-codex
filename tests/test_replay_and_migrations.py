import asyncio
import sqlite3
from pathlib import Path

import pytest

from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.replay import (
    RecordedAction,
    ReplayDataError,
    ReplayFrame,
    ReplayGamePort,
    ReplayPlanner,
    ReplayPlanSeed,
    SnapshotRecording,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore


def _compile(compiler, snapshot):
    return getattr(compiler, "compile")(normalize_runtime_snapshot(snapshot))


def _production_task() -> ProposedTask:
    return ProposedTask(
        task_id="set-production",
        action_type="city_set_production",
        entity_type="city",
        entity_id=1,
        due_turn=10,
        arguments={
            "city_id": 1,
            "item_type": "UNIT",
            "item_name": "UNIT_BUILDER",
        },
        preconditions=[{"type": "city_has_no_production", "city_id": 1}],
        postconditions=[
            {
                "type": "city_production_equals",
                "city_id": 1,
                "item_name": "UNIT_BUILDER",
            }
        ],
        reason="replay production",
    )


def test_legacy_database_migrates_retry_state(tmp_path: Path):
    database = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """
            CREATE TABLE workflow_tasks (
                game_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                plan_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                due_turn INTEGER NOT NULL,
                expires_turn INTEGER,
                arguments_json TEXT NOT NULL,
                preconditions_json TEXT NOT NULL,
                invalidators_json TEXT NOT NULL,
                risk TEXT NOT NULL,
                requires_confirmation INTEGER NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL,
                created_turn INTEGER NOT NULL,
                PRIMARY KEY (game_id, task_id)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO workflow_tasks VALUES (
                'game-1', 'legacy-task', 'legacy-plan', 'unit_skip', 'unit', '9',
                5, NULL, '{"unit_id":9}', '[]', '[]', 'low', 0,
                'resume after upgrade', 'running', 5
            )
            """
        )

    store = WorkflowStore(database)
    task = store.list_tasks("game-1")[0]
    assert task.status is TaskStatus.READY
    assert task.postconditions == []
    assert task.retry_count == 0
    assert task.max_retries == 2
    with sqlite3.connect(database) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 6


def test_replay_store_state_restores_exact_task_status(tmp_path: Path):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    source.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(
            summary="state export",
            city_plan_updates=[{"city_id": 1, "followup_queue": ["UNIT_BUILDER"]}],
            tasks=[_production_task()],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )
    exported = source.export_replay_state("game-1")
    target = WorkflowStore(tmp_path / "target.sqlite3")
    target.import_replay_state(exported)
    target.set_task_status("game-1", "set-production", TaskStatus.DONE)
    target.import_replay_state(exported)

    assert target.task_status("game-1", "set-production") is TaskStatus.READY
    assert target.current_context("game-1")["cities"]["1"]["followup_queue"] == [
        "UNIT_BUILDER"
    ]


def test_recording_rejects_unknown_schema_version(tmp_path: Path):
    path = tmp_path / "future.json"
    path.write_text(
        '{"schema_version":2,"tools":[],"frames":[]}',
        encoding="utf-8",
    )
    with pytest.raises(ReplayDataError, match="schema_version"):
        SnapshotRecording.load(path)


def test_new_builder_is_bound_but_baseline_builder_is_not(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        30,
        PlanBundle(
            plan_id="builder-plan",
            summary="reserve builder from city one",
            builder_plan_updates=[
                {
                    "builder_key": "city-1-builder",
                    "origin_city_id": 1,
                    "path": [[3, 4], [4, 4]],
                    "target": {
                        "x": 4,
                        "y": 4,
                        "improvement_type": "IMPROVEMENT_MINE",
                    },
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"unit_move", "builder_improve"},
    )
    compiler = DeterministicRuleCompiler(store)
    baseline = RuntimeSnapshot(
        turn=30,
        game_id="game-1",
        overview={"turn": 30},
        units=[
            {
                "unit_id": 8,
                "unit_type": "UNIT_BUILDER",
                "origin_city_id": 1,
                "x": 2,
                "y": 4,
            }
        ],
    )
    assert _compile(compiler, baseline).bundle is None
    assert (
        store.current_context("game-1")["builders"]["city-1-builder"].get(
            "assigned_unit_id"
        )
        is None
    )

    produced = baseline.model_copy(
        update={
            "turn": 31,
            "overview": {"turn": 31},
            "units": [
                *baseline.units,
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_BUILDER",
                    "origin_city_id": 1,
                    "x": 3,
                    "y": 4,
                    "build_charges": 3,
                },
            ],
        }
    )
    compiled = _compile(compiler, produced)
    assert (
        store.current_context("game-1")["builders"]["city-1-builder"][
            "assigned_unit_id"
        ]
        == 9
    )
    assert any(event.event_type == "builder_auto_bound" for event in compiled.events)
    assert compiled.bundle.tasks[0].arguments["unit_id"] == 9
    assert compiled.bundle.tasks[0].arguments["target_x"] == 4


def test_json_recording_round_trip_replays_verified_task(tmp_path: Path):
    initial = RuntimeSnapshot(
        turn=10,
        game_id="game-1",
        overview={"turn": 10},
        cities={"cities": [{"city_id": 1, "currently_building": "NONE"}]},
        units=[],
        blockers=[{"type": "city_no_production", "city_ids": ["1"]}],
    )
    verified = initial.model_copy(
        update={
            "cities": {
                "cities": [{"city_id": 1, "currently_building": "UNIT_BUILDER"}]
            },
            "blockers": [],
        }
    )
    seed = ReplayPlanSeed(
        turn=10,
        bundle=PlanBundle(summary="seed replay task", tasks=[_production_task()]),
        auto_action_types=["city_set_production"],
    )
    tape = SnapshotRecording(
        tools=[
            "end_turn",
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "unit_action",
        ],
        frames=[
            ReplayFrame(
                snapshot=initial,
                actions=[
                    RecordedAction(
                        task_id="set-production",
                        action_type="city_set_production",
                        result=ActionResult(success=True, message="recorded success"),
                    )
                ],
            ),
            ReplayFrame(snapshot=verified),
        ],
        seed_plans=[seed],
    )
    path = tmp_path / "recording.json"
    tape.save(path)
    loaded = SnapshotRecording.load(path)
    store = WorkflowStore(tmp_path / "replay.sqlite3")
    store.save_plan_bundle(
        "game-1",
        seed.turn,
        seed.bundle,
        mode=seed.mode,
        auto_action_types=set(seed.auto_action_types),
    )
    game = ReplayGamePort(loaded)
    engine = WorkflowEngine(
        store=store,
        game=game,
        planner=ReplayPlanner(loaded),
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=False,
            auto_action_types={"city_set_production"},
            allowed_action_types={"city_set_production"},
            allowed_tools=set(loaded.tools),
            verification_delay_seconds=0,
        ),
    )

    sent = asyncio.run(engine.tick())
    result = asyncio.run(engine.tick())
    assert sent.workflow_tick["outcome"] == "MUTATION_SENT"
    assert sent.executed_task_ids == []
    assert result.executed_task_ids == ["set-production"]
    assert result.agent_invoked is False
    assert game.remaining_frames == 0


def test_recorded_real_blocker_prevents_automatic_end_turn(tmp_path: Path):
    blocked = RuntimeSnapshot(
        turn=40,
        game_id="game-1",
        overview={"turn": 40},
        cities=[{"city_id": 1, "currently_building": "UNIT_BUILDER"}],
        blockers=[
            {
                "type": "end_turn_blocker",
                "blocking_type": "ENDTURN_BLOCKING_FILL_CIVIC_SLOT",
                "message": "Policies must be assigned",
            }
        ],
    )
    tape = SnapshotRecording(
        tools=[
            "end_turn",
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "unit_action",
        ],
        frames=[ReplayFrame(snapshot=blocked)],
    )
    game = ReplayGamePort(tape)
    engine = WorkflowEngine(
        store=WorkflowStore(tmp_path / "blocked.sqlite3"),
        game=game,
        planner=ReplayPlanner(tape),
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=True,
            max_agent_calls_per_turn=0,
            allowed_tools=set(tape.tools),
        ),
    )

    result = asyncio.run(engine.tick())
    assert result.turn_ended is False
    assert any(event.blocking for event in result.events)
    assert game.call_count == 1


def test_recorded_explicit_rejection_fails_without_retry(tmp_path: Path):
    snapshot = RuntimeSnapshot(
        turn=10,
        game_id="game-1",
        overview={"turn": 10},
        cities={"cities": [{"city_id": 1, "currently_building": "NONE"}]},
        units=[],
        blockers=[{"type": "city_no_production", "city_ids": ["1"]}],
    )
    failed_action = RecordedAction(
        task_id="set-production",
        action_type="city_set_production",
        result=ActionResult(
            success=False,
            blocked=True,
            message="recorded failure",
            delivery_status="explicitly_rejected",
        ),
    )
    tape = SnapshotRecording(
        tools=[
            "end_turn",
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "unit_action",
        ],
        frames=[
            ReplayFrame(snapshot=snapshot, actions=[failed_action]),
            ReplayFrame(snapshot=snapshot, actions=[failed_action]),
        ],
        planner_responses=[
            PlanBundle(
                summary="repeated failure needs review",
                requires_human_review=True,
            )
        ],
    )
    store = WorkflowStore(tmp_path / "failure.sqlite3")
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="seed failing task", tasks=[_production_task()]),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )
    engine = WorkflowEngine(
        store=store,
        game=ReplayGamePort(tape),
        planner=ReplayPlanner(tape),
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            repeated_failure_threshold=2,
            auto_action_types={"city_set_production"},
            allowed_action_types={"city_set_production"},
            allowed_tools=set(tape.tools),
            verification_delay_seconds=0,
        ),
    )

    first = asyncio.run(engine.tick())
    assert first.failed_task_ids == ["set-production"]
    assert first.agent_invoked is False
    assert first.workflow_tick["outcome"] == "MUTATION_REJECTED"
    assert store.task_status("game-1", "set-production") is TaskStatus.FAILED

    second = asyncio.run(engine.tick())
    assert second.agent_invoked is True
    assert store.task_status("game-1", "set-production") is TaskStatus.FAILED
