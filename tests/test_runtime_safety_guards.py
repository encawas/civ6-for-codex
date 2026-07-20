import asyncio
from pathlib import Path

import pytest

from civ6_workflow.config import AppConfig
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.replay import (
    RecordingGamePort,
    ReplayDataError,
    ReplayGamePort,
    SnapshotRecording,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.engine import _TickFileLock
from civ6_workflow.store import TaskIdentityConflictError, WorkflowStore


def _compile(compiler, snapshot):
    return getattr(compiler, "compile")(normalize_runtime_snapshot(snapshot))


def _production_task(item_name: str, *, task_id: str = "production") -> ProposedTask:
    return ProposedTask(
        task_id=task_id,
        action_type="city_set_production",
        entity_type="city",
        entity_id=1,
        due_turn=10,
        arguments={
            "city_id": 1,
            "item_type": "UNIT",
            "item_name": item_name,
        },
        preconditions=[{"type": "city_has_no_production", "city_id": 1}],
        postconditions=[
            {
                "type": "city_production_equals",
                "city_id": 1,
                "item_name": item_name,
            }
        ],
        reason=f"produce {item_name}",
    )


def test_task_id_cannot_be_reused_for_different_semantics(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="first", tasks=[_production_task("UNIT_BUILDER")]),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )

    with pytest.raises(TaskIdentityConflictError, match="different action semantics"):
        store.save_plan_bundle(
            "game-1",
            10,
            PlanBundle(summary="reuse", tasks=[_production_task("UNIT_SETTLER")]),
            mode=ExecutionMode.AUTO,
            auto_action_types={"city_set_production"},
        )


def test_city_queue_progress_survives_new_plan_id(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    first_plan = PlanBundle(
        plan_id="plan-one",
        summary="initial queue",
        city_plan_updates=[
            {
                "city_id": 1,
                "role": "growth",
                "followup_queue": ["UNIT_BUILDER", "UNIT_SETTLER"],
            }
        ],
    )
    store.save_plan_bundle(
        "game-1",
        10,
        first_plan,
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )
    snapshot = RuntimeSnapshot(
        turn=10,
        game_id="game-1",
        overview={"turn": 10},
        cities=[{"city_id": 1, "currently_building": "NONE"}],
    )
    compiler = DeterministicRuleCompiler(store)
    first_task = _compile(compiler, snapshot).bundle.tasks[0]
    assert first_task.arguments["item_name"] == "UNIT_BUILDER"
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="compiled", tasks=[first_task]),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )
    store.set_task_status("game-1", first_task.task_id, TaskStatus.DONE)

    store.save_plan_bundle(
        "game-1",
        11,
        PlanBundle(
            plan_id="plan-two",
            summary="same queue, updated role",
            city_plan_updates=[
                {
                    "city_id": 1,
                    "role": "production",
                    "followup_queue": ["UNIT_BUILDER", "UNIT_SETTLER"],
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )
    next_snapshot = snapshot.model_copy(update={"turn": 11, "overview": {"turn": 11}})
    second_task = _compile(compiler, next_snapshot).bundle.tasks[0]
    assert second_task.arguments["item_name"] == "UNIT_SETTLER"
    assert second_task.task_id != first_task.task_id


def test_engine_config_separates_allowed_and_auto_actions():
    config = AppConfig.model_validate(
        {
            "gate": {"default_cooldown_turns": 7},
            "safety": {
                "allowed_action_types": ["city_set_production", "unit_move"],
                "auto_action_types": ["city_set_production"],
                "allowed_tools": ["set_city_production", "unit_action", "end_turn"],
            },
        }
    )
    engine = config.engine_config()
    assert engine.default_cooldown_turns == 7
    assert engine.allowed_action_types == {"city_set_production", "unit_move"}
    assert engine.auto_action_types == {"city_set_production"}


def test_second_tick_lock_holder_is_rejected(tmp_path: Path):
    lock_path = tmp_path / "runtime.tick.lock"
    with _TickFileLock(lock_path):
        with pytest.raises(RuntimeError, match="already executing a game tick"):
            with _TickFileLock(lock_path):
                pass


class _RecordingDelegate:
    def __init__(self, snapshot: RuntimeSnapshot):
        self.snapshot = snapshot
        self.call_count = 0

    async def read_snapshot(self, *, include_units: bool = False):
        self.call_count += 1
        return self.snapshot

    async def execute_task(self, task):
        self.call_count += 1
        return ActionResult(success=True, message="ok")

    async def end_turn(self, reflections=None):
        self.call_count += 1
        return ActionResult(success=True, message="ok")

    async def list_tools(self):
        return {"set_city_production"}


def test_replay_rejects_changed_action_arguments(tmp_path: Path):
    store = WorkflowStore(tmp_path / "record.sqlite3")
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="seed", tasks=[_production_task("UNIT_BUILDER")]),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )
    recorded_task = store.list_tasks("game-1")[0]
    snapshot = RuntimeSnapshot(
        turn=10,
        game_id="game-1",
        overview={"turn": 10},
        cities=[{"city_id": 1, "currently_building": "NONE"}],
    )
    tape = SnapshotRecording()
    recorder = RecordingGamePort(_RecordingDelegate(snapshot), tape)
    asyncio.run(recorder.read_snapshot())
    asyncio.run(recorder.execute_task(recorded_task))

    replay = ReplayGamePort(tape)
    asyncio.run(replay.read_snapshot())
    changed = recorded_task.model_copy(
        update={
            "arguments": {
                "city_id": 1,
                "item_type": "UNIT",
                "item_name": "UNIT_SETTLER",
            }
        }
    )
    with pytest.raises(ReplayDataError, match="action semantics"):
        asyncio.run(replay.execute_task(changed))
