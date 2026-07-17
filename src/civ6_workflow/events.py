from __future__ import annotations

import hashlib
import json
from typing import Any

from .models import EventLevel, GameEvent, RiskLevel, RuntimeSnapshot, StoredTask


def _stable_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def events_from_snapshot(snapshot: RuntimeSnapshot) -> list[GameEvent]:
    events: list[GameEvent] = []
    for blocker in snapshot.blockers:
        blocker_type = str(blocker.get("type", "unknown_blocker"))
        if blocker_type == "pending_diplomacy":
            data = blocker.get("data")
            rows = data if isinstance(data, list) else [data or blocker]
            base = {key: value for key, value in blocker.items() if key != "data"}
            for row in rows:
                payload = {**base, **(row if isinstance(row, dict) else {})}
                instance_id = next(
                    (
                        payload.get(key)
                        for key in (
                            "diplomacy_id",
                            "request_id",
                            "other_player_id",
                            "player_id",
                        )
                        if payload.get(key) is not None
                    ),
                    None,
                )
                if instance_id is None:
                    instance_id = f"content-{_stable_hash(payload)}"
                    payload["diplomacy_id"] = instance_id
                player_id = payload.get(
                    "other_player_id", payload.get("player_id", instance_id)
                )
                events.append(
                    GameEvent(
                        event_type="pending_diplomacy",
                        turn=snapshot.turn,
                        entity_type="player",
                        entity_id=player_id,
                        level=EventLevel.L3,
                        risk=RiskLevel.HIGH,
                        blocking=True,
                        payload=payload,
                        dedupe_key=f"pending_diplomacy:{instance_id}",
                    )
                )
        elif blocker_type == "pending_trades":
            data = blocker.get("data")
            rows = data if isinstance(data, list) else [data or blocker]
            base = {key: value for key, value in blocker.items() if key != "data"}
            for row in rows:
                payload = {**base, **(row if isinstance(row, dict) else {})}
                offer_id = payload.get("offer_id")
                if offer_id is None:
                    offer_id = f"content-{_stable_hash(payload)}"
                payload["offer_id"] = str(offer_id)
                events.append(
                    GameEvent(
                        event_type="pending_trade_offer",
                        turn=snapshot.turn,
                        entity_type="trade_offer",
                        entity_id=str(offer_id),
                        level=EventLevel.L3,
                        risk=RiskLevel.HIGH,
                        blocking=True,
                        payload=payload,
                        dedupe_key=f"pending_trade_offer:{offer_id}",
                    )
                )
        elif blocker_type == "city_no_production":
            for city_id in blocker.get("city_ids", []):
                events.append(
                    GameEvent(
                        event_type="city_no_production",
                        turn=snapshot.turn,
                        entity_type="city",
                        entity_id=city_id,
                        level=EventLevel.L3,
                        risk=RiskLevel.MEDIUM,
                        blocking=True,
                        payload={"city_id": city_id},
                        dedupe_key=f"city_no_production:{city_id}",
                    )
                )
        elif blocker_type == "notifications":
            events.append(
                GameEvent(
                    event_type="action_required_notification",
                    turn=snapshot.turn,
                    level=EventLevel.L2,
                    risk=RiskLevel.MEDIUM,
                    blocking=True,
                    payload=blocker,
                    dedupe_key=f"action_required_notification:{_stable_hash(blocker)}",
                )
            )
        else:
            events.append(
                GameEvent(
                    event_type=blocker_type,
                    turn=snapshot.turn,
                    level=EventLevel.L2,
                    risk=RiskLevel.MEDIUM,
                    blocking=True,
                    payload=blocker,
                    dedupe_key=f"{blocker_type}:{_stable_hash(blocker)}",
                )
            )
    return events


def task_failure_event(
    task: StoredTask,
    *,
    turn: int,
    message: str,
    blocked: bool,
    repeated_failure_threshold: int,
) -> GameEvent:
    next_retry_count = task.retry_count + 1
    escalate = next_retry_count >= repeated_failure_threshold
    event_type = "planned_task_blocked" if blocked else "planned_task_failed"
    return GameEvent(
        event_type=event_type,
        turn=turn,
        entity_type=task.entity_type,
        entity_id=task.entity_id,
        level=EventLevel.L3 if escalate else EventLevel.L2,
        risk=RiskLevel.HIGH if escalate else RiskLevel.MEDIUM,
        blocking=escalate,
        payload={
            "task_id": task.task_id,
            "action_type": task.action_type,
            "message": message,
            "retry_count": next_retry_count,
            "max_retries": task.max_retries,
        },
        dedupe_key=f"{event_type}:{task.task_id}:{message}",
    )
