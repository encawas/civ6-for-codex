import asyncio

import pytest

from civ6_workflow.models import (
    ActionResult,
    AgentRequest,
    ExecutionMode,
    GameEvent,
    PlanBundle,
    RuntimeSnapshot,
    StoredTask,
)
from civ6_workflow.replay import (
    RecordedAction,
    RecordedPlannerCall,
    ReplayDataError,
    ReplayFrame,
    ReplayGamePort,
    ReplayPlanner,
    SnapshotRecording,
)


def _task(
    *, action_type: str, task_id: str = "same-id", unit_id: int = 7
) -> StoredTask:
    return StoredTask(
        task_id=task_id,
        plan_id="plan-1",
        action_type=action_type,
        entity_type="unit",
        entity_id=unit_id,
        due_turn=12,
        arguments={"unit_id": unit_id},
        reason="strict replay test",
        created_turn=12,
    )


def _request(*, turn: int) -> AgentRequest:
    return AgentRequest(
        turn=turn,
        execution_mode=ExecutionMode.AUTO,
        trigger_events=[
            GameEvent(
                event_type="test_event",
                turn=turn,
                dedupe_key=f"test_event:{turn}",
            )
        ],
        relevant_state={"turn": turn},
    )


def _frame(*, game_id: str = "game-1", turn: int = 12) -> ReplayFrame:
    return ReplayFrame(
        snapshot=RuntimeSnapshot(
            turn=turn,
            game_id=game_id,
            overview={"turn": turn},
        )
    )


def test_replay_matches_both_task_id_and_action_type():
    frame = _frame()
    frame.actions.append(
        RecordedAction(
            task_id="same-id",
            action_type="city_set_production",
            result=ActionResult(success=True),
        )
    )
    game = ReplayGamePort(SnapshotRecording(frames=[frame]))
    asyncio.run(game.read_snapshot())

    with pytest.raises(ReplayDataError, match="expected same-id:city_set_production"):
        asyncio.run(game.execute_task(_task(action_type="unit_skip")))


def test_replay_rejects_unconsumed_recorded_actions():
    frame = _frame()
    frame.actions.append(
        RecordedAction(
            task_id="same-id",
            action_type="unit_skip",
            result=ActionResult(success=True),
        )
    )
    game = ReplayGamePort(SnapshotRecording(frames=[frame]))
    asyncio.run(game.read_snapshot())

    with pytest.raises(ReplayDataError, match="did not execute"):
        _ = game.remaining_frames


def test_replay_requires_recorded_action_order():
    frame = _frame()
    frame.actions.extend(
        [
            RecordedAction(
                task_id="first",
                action_type="unit_skip",
                result=ActionResult(success=True),
            ),
            RecordedAction(
                task_id="second",
                action_type="unit_skip",
                result=ActionResult(success=True),
            ),
        ]
    )
    game = ReplayGamePort(SnapshotRecording(frames=[frame]))
    asyncio.run(game.read_snapshot())

    with pytest.raises(ReplayDataError, match="expected first:unit_skip"):
        asyncio.run(
            game.execute_task(
                _task(action_type="unit_skip", task_id="second", unit_id=8)
            )
        )


def test_replay_rejects_end_turn_before_recorded_actions():
    frame = _frame()
    frame.actions.append(
        RecordedAction(
            task_id="same-id",
            action_type="unit_skip",
            result=ActionResult(success=True),
        )
    )
    frame.end_turn_result = ActionResult(success=True)
    game = ReplayGamePort(SnapshotRecording(frames=[frame]))
    asyncio.run(game.read_snapshot())

    with pytest.raises(ReplayDataError, match="before recorded actions"):
        asyncio.run(game.end_turn())


def test_replay_assert_finished_rejects_remaining_frames():
    game = ReplayGamePort(
        SnapshotRecording(frames=[_frame(turn=12), _frame(turn=13)])
    )
    asyncio.run(game.read_snapshot())

    with pytest.raises(ReplayDataError, match="1 unconsumed snapshot"):
        game.assert_finished()


def test_replay_validates_include_units_request_shape():
    tape = SnapshotRecording(
        frames=[
            ReplayFrame(
                snapshot=RuntimeSnapshot(
                    turn=12,
                    game_id="game-1",
                    overview={"turn": 12},
                    units=[],
                ),
                include_units=True,
            )
        ]
    )
    game = ReplayGamePort(tape)

    with pytest.raises(ReplayDataError, match="include_units"):
        asyncio.run(game.read_snapshot(include_units=False))


def test_replay_rejects_planner_request_drift():
    expected = _request(turn=12)
    tape = SnapshotRecording(
        planner_calls=[
            RecordedPlannerCall(
                request=expected,
                response=PlanBundle(summary="recorded response"),
            )
        ]
    )
    planner = ReplayPlanner(tape)

    with pytest.raises(ReplayDataError, match="planner request does not match"):
        asyncio.run(planner.plan(_request(turn=13)))


def test_replay_rejects_unconsumed_planner_calls():
    tape = SnapshotRecording(
        planner_calls=[
            RecordedPlannerCall(
                request=_request(turn=12),
                response=PlanBundle(summary="recorded response"),
            )
        ]
    )
    planner = ReplayPlanner(tape)

    with pytest.raises(ReplayDataError, match="1 unconsumed planner call"):
        planner.assert_consumed()


def test_recording_rejects_cross_game_store_rows():
    with pytest.raises(ValueError, match="another game_id"):
        SnapshotRecording(
            frames=[_frame()],
            store_state={
                "game_id": "game-1",
                "tables": {
                    "agent_runs": [
                        {
                            "run_id": 9,
                            "game_id": "game-2",
                        }
                    ]
                },
            },
        )


def test_recording_strips_database_internal_agent_run_id():
    tape = SnapshotRecording(
        frames=[_frame()],
        store_state={
            "game_id": "game-1",
            "tables": {
                "agent_runs": [
                    {
                        "run_id": 9,
                        "game_id": "game-1",
                    }
                ],
                "workflow_meta": [
                    {
                        "key": "last_game_id",
                        "value_json": '"game-1"',
                    }
                ],
            },
        },
    )

    assert "run_id" not in tape.store_state["tables"]["agent_runs"][0]


def test_save_revalidates_recording_after_mutation(tmp_path):
    tape = SnapshotRecording(frames=[_frame()])
    tape.frames.append(_frame(game_id="game-2", turn=13))

    with pytest.raises(ReplayDataError, match="cannot save inconsistent recording"):
        tape.save(tmp_path / "invalid.json")
