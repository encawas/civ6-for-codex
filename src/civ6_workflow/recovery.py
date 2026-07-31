from __future__ import annotations

from datetime import datetime

from .models import EventLevel, GameEvent, RiskLevel, RuntimeSnapshot
from .ports import WorkflowStorePort


def recover_turn_rewind(
    store: WorkflowStorePort,
    snapshot: RuntimeSnapshot,
    *,
    previous_game_id: object | None = None,
    previous_turn: object | None = None,
    recovered_at: datetime,
) -> GameEvent | None:
    """Clear future-derived workflow state when a loaded save moves backwards.

    The current schema stores only the latest plan version, so an older plan cannot
    be reconstructed safely after a reload. Clearing plans and executable state is
    safer than applying decisions derived from a future timeline. Historical agent
    and metric rows before the loaded turn remain available for inspection.
    """

    if previous_game_id is None:
        previous_game_id = store.get_meta("last_game_id")
    if previous_turn is None:
        previous_turn = store.get_meta("last_observed_turn")
    if previous_game_id != snapshot.game_id or not isinstance(previous_turn, int):
        return None
    if snapshot.turn >= previous_turn:
        return None

    store.recover_turn_rewind(
        snapshot.game_id, snapshot.turn, recovered_at=recovered_at
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
