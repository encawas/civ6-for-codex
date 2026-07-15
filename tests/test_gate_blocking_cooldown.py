from civ6_workflow.gate import EventGate, GateConfig
from civ6_workflow.models import EventLevel, GameEvent
from civ6_workflow.store import WorkflowStore


def test_blocking_event_remains_active_inside_cooldown(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    gate = EventGate(store, GateConfig(default_cooldown_turns=3))
    first = GameEvent(
        event_type="builder_binding_ambiguous",
        turn=20,
        level=EventLevel.L3,
        blocking=True,
        dedupe_key="builder-binding:one",
    )
    initial = gate.ingest("game-1", [first])
    assert initial.emitted == [first]
    assert initial.agent_events == [first]
    store.mark_events_sent_to_agent("game-1", [first.dedupe_key], first.turn)

    duplicate = first.model_copy(update={"turn": 21})
    cooled = gate.ingest("game-1", [duplicate])

    assert cooled.emitted == [duplicate]
    assert cooled.suppressed == [duplicate]
    assert cooled.agent_events == []


def test_unsent_blocking_event_retries_inside_cooldown(tmp_path):
    gate = EventGate(
        WorkflowStore(tmp_path / "workflow.sqlite3"),
        GateConfig(default_cooldown_turns=3),
    )
    first = GameEvent(
        event_type="builder_binding_ambiguous",
        turn=20,
        level=EventLevel.L3,
        blocking=True,
        dedupe_key="builder-binding:retry",
    )

    assert gate.ingest("game-1", [first]).agent_events == [first]

    retry = first.model_copy(update={"turn": 21})
    result = gate.ingest("game-1", [retry])

    assert result.emitted == [retry]
    assert result.suppressed == []
    assert result.agent_events == [retry]
