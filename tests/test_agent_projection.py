from civ6_workflow.agent_projection import project_agent_context
from civ6_workflow.models import EventLevel, GameEvent, RuntimeSnapshot


def test_unit_event_projection_excludes_unrelated_domains_and_caps_units():
    snapshot = RuntimeSnapshot(
        turn=25,
        game_id="game-1",
        overview={
            "turn": 25,
            "civ_name": "China",
            "leader_name": "Yongle",
            "gold": 100,
            "rankings": ["large", "unneeded"],
        },
        cities=[{"city_id": index, "name": f"City {index}"} for index in range(30)],
        units=[
            {
                "unit_id": index,
                "unit_type": "UNIT_WARRIOR",
                "moves_remaining": 2,
                "x": index,
                "y": 0,
            }
            for index in range(30)
        ],
        diplomacy={"pending": [{"player_id": 2}]},
        trades={"offers": [{"player_id": 3}]},
        notifications=[{"message": "unrelated"}],
        blockers=[
            {
                "type": "end_turn_blocker",
                "blocking_type": "ENDTURN_BLOCKING_UNITS",
            }
        ],
    )
    event = GameEvent(
        event_type="unit_combat_decision_required",
        turn=25,
        entity_type="unit",
        entity_id=3,
        level=EventLevel.L3,
        blocking=True,
        payload={"blocking_type": "ENDTURN_BLOCKING_UNITS"},
        dedupe_key="unit:3",
    )
    context = {
        "strategy": {"victory": "science"},
        "cities": {str(index): {"role": "core"} for index in range(30)},
        "units": {"3": {"path": [[3, 0], [4, 0]]}, "20": {"path": []}},
        "builders": {},
    }

    state, plans, max_tasks = project_agent_context(snapshot, [event], context)

    assert "diplomacy" not in state
    assert "trades" not in state
    assert "notifications" not in state
    assert "cities" not in state
    assert len(state["units"]) <= 16
    assert any(row["unit_id"] == 3 for row in state["units"])
    assert plans["units"] == {"3": {"path": [[3, 0], [4, 0]]}}
    assert max_tasks == 2
    assert "rankings" not in state["overview"]


def test_event_batch_task_limit_is_small_and_bounded():
    snapshot = RuntimeSnapshot(turn=1, game_id="g", overview={"turn": 1})
    events = [
        GameEvent(
            event_type=f"event_{index}",
            turn=1,
            level=EventLevel.L3,
            blocking=True,
            dedupe_key=f"event:{index}",
        )
        for index in range(10)
    ]

    _, _, max_tasks = project_agent_context(snapshot, events, {"strategy": {}})

    assert max_tasks == 8
