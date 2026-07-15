from __future__ import annotations

from .models import EventLevel, GameEvent, RiskLevel, RuntimeSnapshot
from .store import WorkflowStore


def recover_turn_rewind(
    store: WorkflowStore, snapshot: RuntimeSnapshot
) -> GameEvent | None:
    """Clear future-derived workflow state when a loaded save moves backwards.

    The current schema stores only the latest plan version, so an older plan cannot
    be reconstructed safely after a reload. Clearing plans and executable state is
    safer than applying decisions derived from a future timeline. Historical agent
    and metric rows before the loaded turn remain available for inspection.
    """

    previous_game_id = store.get_meta("last_game_id")
    previous_turn = store.get_meta("last_observed_turn")
    if previous_game_id != snapshot.game_id or not isinstance(previous_turn, int):
        return None
    if snapshot.turn >= previous_turn:
        return None

    with store._connect() as conn:  # package-internal recovery transaction
        for table in (
            "strategy_state",
            "city_plans",
            "unit_plans",
            "builder_plans",
            "workflow_tasks",
            "event_log",
            "unit_observations",
        ):
            conn.execute(f"DELETE FROM {table} WHERE game_id=?", (snapshot.game_id,))
        conn.execute(
            "DELETE FROM agent_runs WHERE game_id=? AND turn>=?",
            (snapshot.game_id, snapshot.turn),
        )
        conn.execute(
            "DELETE FROM turn_metrics WHERE game_id=? AND turn>=?",
            (snapshot.game_id, snapshot.turn),
        )
        conn.execute(
            "DELETE FROM workflow_meta WHERE key=?",
            (f"unit_observations_initialized:{snapshot.game_id}",),
        )

    return GameEvent(
        event_type="turn_rewind_detected",
        turn=snapshot.turn,
        entity_type="game",
        entity_id=snapshot.game_id,
        level=EventLevel.L3,
        risk=RiskLevel.HIGH,
        blocking=True,
        payload={"previous_turn": previous_turn, "loaded_turn": snapshot.turn},
        dedupe_key=f"turn_rewind:{previous_turn}:{snapshot.turn}",
    )
