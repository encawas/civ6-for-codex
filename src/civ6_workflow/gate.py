from __future__ import annotations

from dataclasses import dataclass, field

from .models import EventLevel, GameEvent
from .store import WorkflowStore


@dataclass(slots=True)
class GateConfig:
    default_cooldown_turns: int = 2
    cooldowns: dict[str, int] = field(
        default_factory=lambda: {
            "pending_diplomacy": 0,
            "pending_trade_offer": 0,
            "city_no_production": 1,
            "action_required_notification": 1,
            "planned_task_blocked": 1,
            "planned_task_failed": 1,
            "war_declared": 0,
            "world_congress_window": 0,
        }
    )


@dataclass(slots=True)
class GateResult:
    emitted: list[GameEvent] = field(default_factory=list)
    suppressed: list[GameEvent] = field(default_factory=list)
    by_level: dict[EventLevel, list[GameEvent]] = field(
        default_factory=lambda: {level: [] for level in EventLevel}
    )

    @property
    def agent_events(self) -> list[GameEvent]:
        return self.by_level[EventLevel.L3]


class EventGate:
    def __init__(self, store: WorkflowStore, config: GateConfig | None = None):
        self.store = store
        self.config = config or GateConfig()

    def ingest(self, game_id: str, events: list[GameEvent]) -> GateResult:
        result = GateResult()
        for event in events:
            cooldown = self.config.cooldowns.get(
                event.event_type, self.config.default_cooldown_turns
            )
            normalized, should_emit = self.store.upsert_event(
                game_id, event, cooldown_turns=cooldown
            )
            if should_emit:
                result.emitted.append(normalized)
                result.by_level[normalized.level].append(normalized)
                continue

            result.suppressed.append(normalized)
            if normalized.blocking:
                # Cooldown prevents repeated notifications and repeated Agent
                # calls, but a still-active blocker must remain visible to the
                # engine's end-turn safety check on every tick.
                result.emitted.append(normalized)
        return result
