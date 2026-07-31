import asyncio
from pathlib import Path

import pytest

from civ6_workflow.bootstrap import build_runtime_services
from civ6_workflow.runtime import RuntimeConfig, WorkflowRuntime
from civ6_workflow.models import ExecutionMode, RuntimeSnapshot
from civ6_workflow.replay import (
    ReplayDataError,
    ReplayFrame,
    ReplayGamePort,
    ReplayPlanner,
    SnapshotRecording,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore


def _compile(compiler, snapshot):
    return getattr(compiler, "compile")(normalize_runtime_snapshot(snapshot))


def test_recording_rejects_unknown_schema_version(tmp_path: Path):
    path = tmp_path / "future.json"
    path.write_text(
        '{"schema_version":2,"tools":[],"frames":[]}',
        encoding="utf-8",
    )
    with pytest.raises(ReplayDataError, match="schema_version"):
        SnapshotRecording.load(path)


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
    engine = WorkflowRuntime(
        service_factory=build_runtime_services,
        store=WorkflowStore(tmp_path / "blocked.sqlite3"),
        game=game,
        planner=ReplayPlanner(tape),
        config=RuntimeConfig(
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
