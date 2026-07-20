import asyncio
from pathlib import Path

import pytest

from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    PlanBundle,
    RuntimeSnapshot,
)
from civ6_workflow.store import WorkflowStore


class _Planner:
    def __init__(self):
        self.calls = 0

    async def plan(self, request):
        self.calls += 1
        return PlanBundle(summary="planner must not run while wait is unchanged")


class _Game:
    def __init__(self):
        self.call_count = 0
        self.mutations = 0
        self.snapshot = RuntimeSnapshot(
            turn=12,
            game_id="human-wait",
            overview={"turn": 12},
            cities=[],
            units=[
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_BUILDER",
                    "x": 3,
                    "y": 4,
                    "build_charges": 2,
                    "valid_improvements": ["IMPROVEMENT_MINE"],
                }
            ],
            blockers=[],
        )

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        snapshot = self.snapshot.model_copy(deep=True)
        if not include_units:
            snapshot.units = None
        return snapshot

    async def execute_task(self, task):
        self.call_count += 1
        self.mutations += 1
        return ActionResult(success=True, message="unexpected mutation")

    async def end_turn(self, reflections=None):
        self.call_count += 1
        self.mutations += 1
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


def _engine(tmp_path: Path, *, mode: ExecutionMode = ExecutionMode.AUTO):
    path = tmp_path / "workflow.sqlite3"
    store = WorkflowStore(path)
    store.save_plan_bundle(
        "human-wait",
        12,
        PlanBundle(
            summary="invalid builder plan",
            builder_plan_updates=[
                {
                    "builder_key": "builder-9",
                    "assigned_unit_id": 9,
                    "path": [[3, 4]],
                }
            ],
        ),
        mode=mode,
        auto_action_types={"unit_move", "builder_improve"},
    )
    game = _Game()
    planner = _Planner()
    engine = WorkflowEngine(
        store=store,
        game=game,
        planner=planner,
        config=EngineConfig(
            execution_mode=mode,
            auto_end_turn=False,
            auto_action_types={"unit_move", "builder_improve"},
            allowed_action_types={"unit_move", "builder_improve"},
            allowed_tools={
                "set_city_production",
                "set_research",
                "unit_action",
                "end_turn",
            },
            verification_delay_seconds=0,
        ),
    )
    return engine, game, planner, path


def _compile_counter(engine):
    calls = {"value": 0}
    original = engine.rules.compile

    def compile(observation):
        calls["value"] += 1
        return original(observation)

    engine.rules.compile = compile
    return calls


def test_unchanged_human_wait_does_not_replan_or_mutate(tmp_path: Path):
    async def scenario():
        engine, game, planner, _ = _engine(tmp_path)
        first = await engine.tick()
        assert first.workflow_tick["outcome"] == "AWAITING_HUMAN"
        baseline = engine.store.human_wait_context("human-wait")
        assert baseline is not None
        assert baseline["resume_requested"] is False

        second = await engine.tick()
        assert second.workflow_tick["outcome"] == "AWAITING_HUMAN"
        assert planner.calls == 0
        assert game.mutations == 0
        assert engine.store.load_runtime_state("human-wait").value == "AWAITING_HUMAN"

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", [ExecutionMode.READONLY, ExecutionMode.CONFIRM])
def test_execution_mode_auto_rechecks_a_human_wait_once(
    tmp_path: Path, mode: ExecutionMode
):
    async def scenario():
        engine, game, planner, _ = _engine(tmp_path, mode=mode)
        compiled = _compile_counter(engine)
        await engine.tick()
        assert compiled["value"] == 1

        engine.config.execution_mode = ExecutionMode.AUTO
        resumed = await engine.tick()
        assert compiled["value"] == 2
        assert resumed.workflow_tick["starting_runtime_state"] == "AWAITING_HUMAN"
        assert resumed.workflow_tick["outcome"] == "AWAITING_HUMAN"
        assert engine.store.human_wait_context("human-wait")["execution_mode"] == "auto"
        assert planner.calls == 0
        assert game.mutations == 0

        await engine.tick()
        assert compiled["value"] == 2
        assert planner.calls == 0
        assert game.mutations == 0

    asyncio.run(scenario())


class _OpeningWaitGame:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.call_count = 0
        self.mutations = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        return self.snapshot.model_copy(deep=True)

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

    async def execute_task(self, task):
        self.mutations += 1
        raise AssertionError("opening human-wait test must not mutate")

    async def end_turn(self, reflections=None):
        self.mutations += 1
        raise AssertionError("opening human-wait test must not end turn")


class _HumanReviewPlanner:
    def __init__(self):
        self.calls = 0
        self.last_diagnostics = {"attempt_count": 1, "backend": "test"}

    async def plan(self, request):
        self.calls += 1
        return PlanBundle(
            summary="opening choice requires review", requires_human_review=True
        )


def _opening_snapshot(production):
    return RuntimeSnapshot(
        turn=1,
        game_id="opening-human-wait",
        overview={"turn": 1, "num_cities": 1},
        cities=[{"city_id": 1, "currently_building": production}],
        tech_civics={
            "current_research": "TECH_MINING",
            "available_techs": [{"tech_type": "TECH_MINING", "name": "Mining"}],
        },
        blockers=[],
    )


def _opening_human_wait_engine(tmp_path: Path):
    path = tmp_path / "opening-human-wait.sqlite3"
    game = _OpeningWaitGame(_opening_snapshot(None))
    planner = _HumanReviewPlanner()
    engine = WorkflowEngine(
        store=WorkflowStore(path),
        game=game,
        planner=planner,
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=False,
            auto_action_types={"city_set_production", "set_research"},
        ),
    )
    return engine, game, planner, path


async def _enter_opening_human_wait(engine):
    await engine.tick()
    await engine.tick()
    result = await engine.tick()
    assert result.workflow_tick["outcome"] == "AWAITING_HUMAN"


def test_manual_game_resolution_releases_human_wait(tmp_path: Path):
    async def scenario():
        engine, game, planner, _ = _opening_human_wait_engine(tmp_path)
        await _enter_opening_human_wait(engine)
        game.snapshot = _opening_snapshot("BUILDING_MONUMENT")

        resumed = await engine.tick()
        assert resumed.workflow_tick["starting_runtime_state"] == "AWAITING_HUMAN"
        assert resumed.workflow_tick["outcome"] == "DECISION_GAP_UPDATED"
        assert (
            engine.store.load_runtime_state("opening-human-wait").value
            != "AWAITING_HUMAN"
        )
        assert engine.store.human_wait_context("opening-human-wait") is None
        assert planner.calls == 1
        assert game.mutations == 0

    asyncio.run(scenario())


def test_restart_rechecks_manual_resolution_of_human_wait(tmp_path: Path):
    async def scenario():
        engine, game, planner, path = _opening_human_wait_engine(tmp_path)
        await _enter_opening_human_wait(engine)
        game.snapshot = _opening_snapshot("BUILDING_MONUMENT")
        restarted = WorkflowEngine(
            store=WorkflowStore(path),
            game=game,
            planner=planner,
            config=engine.config,
        )

        resumed = await restarted.tick()
        assert resumed.workflow_tick["starting_runtime_state"] == "AWAITING_HUMAN"
        assert resumed.workflow_tick["outcome"] == "DECISION_GAP_UPDATED"
        assert restarted.store.human_wait_context("opening-human-wait") is None
        assert planner.calls == 1
        assert game.mutations == 0

    asyncio.run(scenario())


def test_explicit_resume_rechecks_once_without_replanning_or_mutating(tmp_path: Path):
    async def scenario():
        engine, game, planner, _ = _engine(tmp_path)
        compiled = _compile_counter(engine)
        await engine.tick()
        assert compiled["value"] == 1
        assert engine.store.request_human_resume("human-wait") is True

        resumed = await engine.tick()
        assert compiled["value"] == 2
        assert resumed.workflow_tick["outcome"] == "AWAITING_HUMAN"
        assert (
            engine.store.human_wait_context("human-wait")["resume_requested"] is False
        )
        assert planner.calls == 0
        assert game.mutations == 0

        await engine.tick()
        assert compiled["value"] == 2
        assert planner.calls == 0
        assert game.mutations == 0

    asyncio.run(scenario())


def test_explicit_resume_survives_restart(tmp_path: Path):
    async def scenario():
        engine, game, planner, path = _engine(tmp_path)
        await engine.tick()
        assert engine.store.request_human_resume("human-wait") is True
        restarted = WorkflowEngine(
            store=WorkflowStore(path),
            game=game,
            planner=planner,
            config=engine.config,
        )
        compiled = _compile_counter(restarted)

        resumed = await restarted.tick()
        assert compiled["value"] == 1
        assert resumed.workflow_tick["outcome"] == "AWAITING_HUMAN"
        assert (
            restarted.store.human_wait_context("human-wait")["resume_requested"]
            is False
        )
        assert planner.calls == 0
        assert game.mutations == 0

    asyncio.run(scenario())
