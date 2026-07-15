from civ6_workflow.gate import EventGate
from civ6_workflow.models import EventLevel, GameEvent, RiskLevel
from civ6_workflow.store import WorkflowStore


def _event(event_type: str, key: str, turn: int = 5):
    return GameEvent(
        event_type=event_type,
        turn=turn,
        level=EventLevel.L3,
        risk=RiskLevel.HIGH,
        blocking=True,
        dedupe_key=key,
    )


def _event_row(store, game_id, key):
    with store._connect() as conn:
        return conn.execute(
            "SELECT * FROM event_log WHERE game_id=? AND dedupe_key=?",
            (game_id, key),
        ).fetchone()


def test_disappeared_snapshot_event_is_resolved(tmp_path):
    store = WorkflowStore(tmp_path / "state.sqlite3")
    store.set_meta("last_observed_turn", 5)
    gate = EventGate(store)
    gate.ingest("game", [_event("pending_diplomacy", "diplomacy:1")])
    assert _event_row(store, "game", "diplomacy:1")["status"] == "open"

    store.set_meta("last_observed_turn", 6)
    gate.ingest("game", [])
    row = _event_row(store, "game", "diplomacy:1")
    assert row["status"] == "resolved"
    assert row["resolved_turn"] == 6
    assert row["resolved_by"] == "snapshot_reconciliation"


def test_uncertain_commit_event_is_sticky(tmp_path):
    store = WorkflowStore(tmp_path / "state.sqlite3")
    store.set_meta("last_observed_turn", 5)
    gate = EventGate(store)
    gate.ingest("game", [_event("action_commit_uncertain", "uncertain:task")])

    store.set_meta("last_observed_turn", 6)
    gate.ingest("game", [])
    row = _event_row(store, "game", "uncertain:task")
    assert row["status"] == "open"
    assert row["resolved_turn"] is None
