import asyncio
from pathlib import Path

from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.store import WorkflowStore


class _NoopPlanner:
    async def plan(self, request):
        return PlanBundle(summary="no changes")


class _Game:
    def __init__(self):
        self.call_count = 0
        self.executed: list[str] = []
        self.snapshot = RuntimeSnapshot(
            turn=10,
            game_id="game-1",
            overview={"turn": 10},
            cities={"cities": [{"city_id": 1, "currently_building": "NONE"}]},
            blockers=[{"type": "city_no_production", "city_ids": ["1"]}],
        )

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        return self.snapshot.model_copy(deep=True)

    async def execute_task(self, task):
        self.call_count += 1
        self.executed.append(task.task_id)
        return ActionResult(success=True, message="unexpected execution")

    async def end_turn(self):
        self.call_count += 1
        return ActionResult(success=True, message="unexpected end turn")

    async def list_tools(self):
        return {
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
        }


def _task() -> ProposedTask:
    return ProposedTask(
        task_id="confirm-production",
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
        reason="confirm-mode safety test",
    )


def _confirm_engine(store: WorkflowStore, game: _Game) -> WorkflowEngine:
    return WorkflowEngine(
        store=store,
        game=game,
        planner=_NoopPlanner(),
        config=EngineConfig(
            execution_mode=ExecutionMode.CONFIRM,
            auto_end_turn=False,
            max_agent_calls_per_turn=0,
            auto_action_types={"city_set_production"},
            allowed_action_types={"city_set_production"},
            allowed_tools={
                "set_city_production",
                "set_research",
                "unit_action",
                "end_turn",
            },
            verification_delay_seconds=0,
        ),
    )


def test_confirm_tick_does_not_execute_before_approval(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    task = _task()
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="confirm", tasks=[task]),
        mode=ExecutionMode.CONFIRM,
        auto_action_types={"city_set_production"},
    )
    game = _Game()

    result = asyncio.run(_confirm_engine(store, game).tick())

    assert game.executed == []
    assert result.executed_task_ids == []
    assert store.task_status("game-1", task.task_id) is TaskStatus.AWAITING_CONFIRMATION


def test_switching_from_auto_to_confirm_demotes_unapproved_ready_task(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    task = _task()
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="created in auto", tasks=[task]),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )
    assert store.task_status("game-1", task.task_id) is TaskStatus.READY
    game = _Game()

    result = asyncio.run(_confirm_engine(store, game).tick())

    assert game.executed == []
    assert result.executed_task_ids == []
    assert store.task_status("game-1", task.task_id) is TaskStatus.AWAITING_CONFIRMATION
