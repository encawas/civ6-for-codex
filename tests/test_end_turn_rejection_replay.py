import asyncio
from pathlib import Path

from civ6_workflow.domain import AttemptStatus, TickOutcomeKind
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    MutationDeliveryStatus,
    PlanBundle,
    RuntimeSnapshot,
)
from civ6_workflow.store import WorkflowStore


class Planner:
    def __init__(self) -> None:
        self.calls = 0

    async def plan(self, request):
        self.calls += 1
        return PlanBundle(summary="no strategic change")


class Game:
    def __init__(self, snapshots, result: ActionResult) -> None:
        self.snapshots = list(snapshots)
        self.result = result
        self.call_count = 0
        self.reads = 0
        self.end_turn_calls = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        index = min(self.reads, len(self.snapshots) - 1)
        self.reads += 1
        return self.snapshots[index].model_copy(deep=True)

    async def execute_task(self, task):
        raise AssertionError("this regression must not execute a task")

    async def end_turn(self):
        self.call_count += 1
        self.end_turn_calls += 1
        return self.result

    async def list_tools(self):
        return {
            "end_turn",
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "set_research",
            "unit_action",
        }


def snapshot(*, production="UNIT_BUILDER", turn=10, blockers=None):
    return RuntimeSnapshot(
        turn=turn,
        game_id="game-1",
        overview={"turn": turn},
        cities={"cities": [{"city_id": 1, "currently_building": production}]},
        tech_civics={
            "current_research": "TECH_POTTERY",
            "current_civic": "CIVIC_CODE_OF_LAWS",
        },
        blockers=list(blockers or []),
    )


def engine(store, game, planner=None, **config):
    return WorkflowEngine(
        store=store,
        game=game,
        planner=planner or Planner(),
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=True,
            max_agent_calls_per_turn=0,
            verification_delay_seconds=0,
            **config,
        ),
    )


def explicit_rejection():
    return ActionResult(
        success=False,
        blocked=True,
        message="the game rejected end turn",
        delivery_status=MutationDeliveryStatus.EXPLICITLY_REJECTED,
    )


def test_explicit_end_turn_rejection_is_not_replayed_on_next_tick(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "cross-tick.sqlite3")
        game = Game([snapshot(), snapshot()], explicit_rejection())
        runtime = engine(store, game)

        rejected = await runtime.tick()
        suppressed = await runtime.tick()

        assert rejected.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_REJECTED
        assert suppressed.workflow_tick["outcome"] == TickOutcomeKind.NO_SAFE_ACTION
        assert "explicitly rejected" in suppressed.workflow_tick["blocking_reason"]
        assert suppressed.metrics.mutation_count == 0
        assert game.end_turn_calls == 1
        attempts = store.list_action_attempts("game-1")
        assert len(attempts) == 1
        assert attempts[0].status is AttemptStatus.FAILED
        assert (
            attempts[0].transport_result["delivery_status"]
            == MutationDeliveryStatus.EXPLICITLY_REJECTED.value
        )

    asyncio.run(scenario())


def test_explicit_end_turn_rejection_suppression_survives_restart(tmp_path: Path):
    async def scenario():
        database = tmp_path / "restart.sqlite3"
        first_game = Game([snapshot()], explicit_rejection())
        await engine(WorkflowStore(database), first_game).tick()

        second_game = Game([snapshot()], explicit_rejection())
        restarted = WorkflowStore(database)
        suppressed = await engine(restarted, second_game).tick()

        assert suppressed.workflow_tick["outcome"] == TickOutcomeKind.NO_SAFE_ACTION
        assert second_game.end_turn_calls == 0
        assert len(restarted.list_action_attempts("game-1")) == 1

    asyncio.run(scenario())


def test_material_end_turn_state_change_allows_safe_reevaluation(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "state-change.sqlite3")
        game = Game(
            [snapshot(production="UNIT_BUILDER"), snapshot(production="UNIT_SETTLER")],
            explicit_rejection(),
        )
        runtime = engine(store, game)

        await runtime.tick()
        retried = await runtime.tick()

        assert retried.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_REJECTED
        assert retried.metrics.mutation_count == 1
        assert game.end_turn_calls == 2
        attempts = store.list_action_attempts("game-1")
        assert [attempt.attempt_number for attempt in attempts] == [1, 2]
        assert attempts[1].parent_attempt_id == attempts[0].action_attempt_id

    asyncio.run(scenario())


def test_end_turn_delivery_classifications_keep_distinct_recovery(tmp_path: Path):
    async def scenario():
        uncertain_store = WorkflowStore(tmp_path / "uncertain.sqlite3")
        uncertain_game = Game(
            [snapshot(), snapshot()],
            ActionResult(
                success=False,
                message="transport timeout",
                delivery_status=MutationDeliveryStatus.UNKNOWN,
            ),
        )
        uncertain_runtime = engine(uncertain_store, uncertain_game)
        uncertain = await uncertain_runtime.tick()
        reconciled = await uncertain_runtime.tick()

        assert uncertain.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_UNCERTAIN
        assert reconciled.workflow_tick["outcome"] != TickOutcomeKind.NO_SAFE_ACTION
        assert uncertain_game.end_turn_calls == 1
        assert uncertain_store.unresolved_action_attempt("game-1") is not None

        success_store = WorkflowStore(tmp_path / "success.sqlite3")
        success_game = Game(
            [snapshot(turn=10), snapshot(turn=11)],
            ActionResult(success=True),
        )
        success_runtime = engine(success_store, success_game)
        started = await success_runtime.tick()
        confirmed = await success_runtime.tick()

        assert (
            started.workflow_tick["outcome"] == TickOutcomeKind.TURN_TRANSITION_STARTED
        )
        assert (
            confirmed.workflow_tick["outcome"]
            == TickOutcomeKind.TURN_TRANSITION_CONFIRMED
        )
        assert success_game.end_turn_calls == 1

    asyncio.run(scenario())


def test_explicit_user_retry_authorizes_only_latest_rejection(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "user-retry.sqlite3")
        game = Game([snapshot(), snapshot(), snapshot()], explicit_rejection())
        runtime = engine(store, game)

        await runtime.tick()
        rejected_attempt = store.list_action_attempts("game-1")[0]
        runtime.request_end_turn_retry("game-1", 10)
        await runtime.tick()
        suppressed = await runtime.tick()

        assert game.end_turn_calls == 2
        assert suppressed.workflow_tick["outcome"] == TickOutcomeKind.NO_SAFE_ACTION
        assert len(store.list_action_attempts("game-1")) == 2
        assert store.get_meta(
            f"end_turn_explicit_retry:{rejected_attempt.action_attempt_id}"
        )

    asyncio.run(scenario())
