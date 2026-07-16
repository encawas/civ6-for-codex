import asyncio
from datetime import UTC, datetime

import pytest

from civ6_workflow.characterization import (
    CrashInjector,
    DeterministicClock,
    GameCallKind,
    InjectedCrash,
    RecordingGamePort,
    RecordingPlanner,
    ScriptedPlanner,
    ScriptedSnapshot,
    ScriptedSnapshotSource,
    SecondMutationError,
)
from civ6_workflow.models import (
    ActionResult,
    AgentRequest,
    ExecutionMode,
    PlanBundle,
    RuntimeSnapshot,
    StoredTask,
    TaskStatus,
)


def _task() -> StoredTask:
    return StoredTask(
        task_id="task-1",
        action_type="city_set_production",
        entity_type="city",
        entity_id=1,
        due_turn=10,
        arguments={
            "city_id": 1,
            "item_type": "UNIT",
            "item_name": "UNIT_BUILDER",
        },
        postconditions=[
            {
                "type": "city_production_equals",
                "city_id": 1,
                "item_name": "UNIT_BUILDER",
            }
        ],
        reason="record one mutation",
        plan_id="plan-1",
        created_turn=10,
        status=TaskStatus.READY,
    )


def _snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot(turn=10, game_id="game-1", overview={"turn": 10})


def test_met_005_game_calls_are_classified_separately():
    """MET-005: reads, mutations, and end-turn mutations remain distinct."""

    async def scenario():
        delegate = ScriptedSnapshotSource(
            [ScriptedSnapshot(_snapshot())],
            action_results=[ActionResult(success=True)],
            end_turn_results=[ActionResult(success=True)],
            tools={"set_city_production", "end_turn"},
        )
        game = RecordingGamePort(delegate)
        game.begin_tick("tick-1")

        await game.list_tools()
        await game.read_snapshot()
        await game.execute_task(_task())
        await game.end_turn()

        assert [call.kind for call in game.calls_for_tick()] == [
            GameCallKind.READ,
            GameCallKind.READ,
            GameCallKind.MUTATION,
            GameCallKind.END_TURN_MUTATION,
        ]
        assert game.summary().reads == 2
        assert game.summary().mutations == 1
        assert game.summary().end_turn_mutations == 1
        assert game.summary().total_mutations == 2

    asyncio.run(scenario())


def test_act_001_recording_port_fails_before_second_mutation():
    """ACT-001 target harness: strict mode rejects a second mutation immediately."""

    async def scenario():
        delegate = ScriptedSnapshotSource(
            [],
            action_results=[ActionResult(success=True)],
            end_turn_results=[ActionResult(success=True)],
        )
        game = RecordingGamePort(delegate, fail_on_second_mutation=True)
        game.begin_tick("tick-1")

        await game.execute_task(_task())
        with pytest.raises(SecondMutationError, match="second mutation"):
            await game.end_turn()

        assert game.summary().total_mutations == 1

    asyncio.run(scenario())


def test_met_003_planner_counts_logical_requests_and_provider_attempts():
    """MET-003: stable request identity is counted separately from retries."""

    async def scenario():
        request = AgentRequest(
            request_id="logical-1",
            turn=10,
            execution_mode=ExecutionMode.CONFIRM,
            trigger_events=[],
        )
        delegate = ScriptedPlanner(
            [PlanBundle(summary="first"), PlanBundle(summary="final")],
            provider_attempts=[2, 3],
        )
        planner = RecordingPlanner(delegate)

        await planner.plan(request)
        await planner.plan(request.model_copy(update={"information_results": {}}))

        assert planner.summary.logical_requests == 1
        assert planner.summary.provider_attempts == 5
        assert len(planner.requests) == 2
        assert len(planner.responses) == 2

    asyncio.run(scenario())


def test_deterministic_clock_and_crash_injector_are_repeatable():
    async def scenario():
        clock = DeterministicClock(datetime(2026, 1, 1, tzinfo=UTC))
        await clock.sleep(2.5)
        assert clock.monotonic() == 2.5
        assert clock.now() == datetime(2026, 1, 1, 0, 0, 2, 500000, tzinfo=UTC)

        injector = CrashInjector({"after_send": 2})
        injector.checkpoint("after_send")
        with pytest.raises(InjectedCrash, match="occurrence 2"):
            injector.checkpoint("after_send")

    asyncio.run(scenario())
