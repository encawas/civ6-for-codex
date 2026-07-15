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


class FakePlanner:
    def __init__(self, bundle: PlanBundle | None = None):
        self.calls = 0
        self.bundle = bundle or PlanBundle(summary="no plan changes required")

    async def plan(self, _request):
        self.calls += 1
        return self.bundle


class FakeGame:
    def __init__(self, snapshot: RuntimeSnapshot):
        self.snapshot = snapshot
        self.call_count = 0
        self.executed = []
        self.end_turn_calls = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        return self.snapshot.model_copy(deep=True)

    async def execute_task(self, task):
        self.call_count += 1
        self.executed.append(task.task_id)
        if task.action_type == "city_set_production":
            for city in self.snapshot.cities["cities"]:
                if str(city["city_id"]) == str(task.entity_id):
                    city["currently_building"] = task.arguments["item_name"]
            self.snapshot.blockers = [
                blocker
                for blocker in self.snapshot.blockers
                if blocker.get("type") != "city_no_production"
            ]
        return ActionResult(success=True, message="ok")

    async def end_turn(self):
        self.call_count += 1
        self.end_turn_calls += 1
        return ActionResult(success=True, message="advanced")

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


class AlwaysBlockedGame(FakeGame):
    async def execute_task(self, task):
        self.call_count += 1
        self.executed.append(task.task_id)
        return ActionResult(success=False, blocked=True, message="target occupied")


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
        reason="approved follow-up production",
    )


def _empty_city_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot(
        turn=10,
        game_id="game-1",
        overview={"turn": 10},
        cities={"cities": [{"city_id": 1, "currently_building": "NONE"}]},
        blockers=[{"type": "city_no_production", "city_ids": ["1"]}],
    )


def test_ordinary_turn_executes_task_without_agent(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "workflow.sqlite3")
        store.save_plan_bundle(
            "game-1",
            10,
            PlanBundle(summary="continue the approved queue", tasks=[_production_task()]),
            mode=ExecutionMode.AUTO,
            auto_action_types={"city_set_production"},
        )
        game = FakeGame(_empty_city_snapshot())
        planner = FakePlanner()
        engine = WorkflowEngine(
            store=store,
            game=game,
            planner=planner,
            config=EngineConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=True,
                auto_action_types={"city_set_production"},
                allowed_action_types={"city_set_production"},
                verification_delay_seconds=0,
            ),
        )

        result = await engine.tick()
        assert result.executed_task_ids == ["set-production"]
        assert planner.calls == 0
        assert result.agent_invoked is False
        assert result.turn_ended is True
        assert game.end_turn_calls == 1

    asyncio.run(scenario())


def test_blocked_task_retries_once_then_escalates(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "workflow.sqlite3")
        store.save_plan_bundle(
            "game-1",
            10,
            PlanBundle(summary="retry bounded task", tasks=[_production_task()]),
            mode=ExecutionMode.AUTO,
            auto_action_types={"city_set_production"},
        )
        game = AlwaysBlockedGame(_empty_city_snapshot())
        planner = FakePlanner(
            PlanBundle(summary="blocked task needs human review", requires_human_review=True)
        )
        engine = WorkflowEngine(
            store=store,
            game=game,
            planner=planner,
            config=EngineConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=True,
                repeated_failure_threshold=2,
                auto_action_types={"city_set_production"},
                allowed_action_types={"city_set_production"},
                verification_delay_seconds=0,
            ),
        )

        first = await engine.tick()
        assert planner.calls == 0
        assert first.agent_invoked is False
        assert store.task_status("game-1", "set-production") is TaskStatus.READY
        assert first.turn_ended is False

        second = await engine.tick()
        assert planner.calls == 1
        assert second.agent_invoked is True
        assert store.task_status("game-1", "set-production") is TaskStatus.ESCALATED
        assert second.turn_ended is False

    asyncio.run(scenario())


def test_l3_events_are_batched_into_one_codex_call(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "workflow.sqlite3")
        snapshot = RuntimeSnapshot(
            turn=20,
            game_id="game-1",
            overview={"turn": 20},
            diplomacy={"pending": [{"player_id": 2}]},
            trades={"offers": [{"player_id": 3}]},
            cities={"cities": [{"city_id": 1, "currently_building": "UNIT_BUILDER"}]},
            blockers=[
                {"type": "pending_diplomacy", "data": {"player_id": 2}},
                {"type": "pending_trades", "data": {"player_id": 3}},
            ],
        )
        game = FakeGame(snapshot)
        planner = FakePlanner(
            PlanBundle(
                summary="human diplomacy review required",
                requires_human_review=True,
            )
        )
        engine = WorkflowEngine(
            store=store,
            game=game,
            planner=planner,
            config=EngineConfig(
                execution_mode=ExecutionMode.CONFIRM,
                auto_end_turn=True,
                verification_delay_seconds=0,
            ),
        )

        result = await engine.tick()
        assert planner.calls == 1
        assert result.agent_invoked is True
        assert result.turn_ended is False
        assert result.paused is True
        assert len(result.events) == 2

        second = await engine.tick()
        assert planner.calls == 1
        assert second.paused is True
        assert "already been called" in second.pause_reason

    asyncio.run(scenario())
