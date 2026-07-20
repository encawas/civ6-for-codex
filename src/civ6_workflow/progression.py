from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from .domain.observations import SlotState
from .models import (
    EventLevel,
    GameEvent,
    PlanBundle,
    ProposedTask,
    RiskLevel,
    RuntimeSnapshot,
)
from .observation_normalization import NormalizedRuntimeObservation
from .ports import WorkflowStorePort


@dataclass(slots=True)
class ProgressionCompilation:
    bundle: PlanBundle | None = None
    events: list[GameEvent] = field(default_factory=list)


class ProgressionRuleCompiler:
    """Compile approved research and civic queues into one verifiable choice."""

    def __init__(self, store: WorkflowStorePort):
        self.store = store

    def compile(
        self,
        observation: NormalizedRuntimeObservation,
    ) -> ProgressionCompilation:
        snapshot = observation.snapshot
        strategy = self.store.current_context(snapshot.game_id).get("strategy", {})
        if not isinstance(strategy, dict):
            strategy = {}
        tasks: list[ProposedTask] = []
        events: list[GameEvent] = []
        research = self._compile_category(
            observation,
            strategy,
            category="research",
            queue_keys=("research_queue", "tech_queue"),
            action_type="set_research",
            target_keys=("tech_type", "item_name", "name"),
            events=events,
        )
        if research is not None:
            tasks.append(research)
        civic = self._compile_category(
            observation,
            strategy,
            category="civic",
            queue_keys=("civic_queue",),
            action_type="set_civic",
            target_keys=("civic_type", "item_name", "name"),
            events=events,
        )
        if civic is not None:
            tasks.append(civic)
        if not tasks:
            return ProgressionCompilation(events=events)
        return ProgressionCompilation(
            bundle=PlanBundle(
                plan_id=f"progression_turn_{snapshot.turn}",
                summary="Continue approved technology and civic queues.",
                tasks=tasks,
            ),
            events=events,
        )

    def _compile_category(
        self,
        observation: NormalizedRuntimeObservation,
        strategy: dict[str, Any],
        *,
        category: str,
        queue_keys: tuple[str, ...],
        action_type: str,
        target_keys: tuple[str, ...],
        events: list[GameEvent],
    ) -> ProposedTask | None:
        snapshot = observation.snapshot
        queue = self._first_queue(strategy, queue_keys)
        if not queue:
            return None
        progression = observation.canonical.progression
        slot = (
            progression.current_research
            if category == "research"
            else progression.current_civic
        )
        if slot.state is not SlotState.EMPTY:
            return None
        available = {
            entity.value
            for entity in (
                progression.available_research_ids
                if category == "research"
                else progression.available_civic_ids
            )
        }
        for index, raw in enumerate(queue):
            target = self._queue_target(raw, target_keys)
            if target is None:
                events.append(
                    self._invalid_queue_event(
                        snapshot,
                        category,
                        index,
                        raw,
                        "queue item does not contain a valid target type",
                    )
                )
                return None
            task_id = self._task_id(category, index, target)
            status = self.store.task_status(snapshot.game_id, task_id)
            if status is not None:
                if status.value in {"done", "cancelled", "expired"}:
                    continue
                return None

            if target not in available:
                events.append(
                    GameEvent(
                        event_type=f"{category}_plan_target_unavailable",
                        turn=snapshot.turn,
                        entity_type=category,
                        entity_id=target,
                        level=EventLevel.L3,
                        risk=RiskLevel.MEDIUM,
                        blocking=True,
                        payload={
                            "queue_index": index,
                            "target": target,
                            "available": sorted(available),
                        },
                        dedupe_key=f"{category}_plan_target_unavailable:{index}:{target}",
                    )
                )
                return None
            available_condition = (
                {"type": "research_available", "tech_type": target}
                if category == "research"
                else {"type": "civic_available", "civic_type": target}
            )
            unselected_condition = {
                "type": (
                    "research_unselected"
                    if category == "research"
                    else "civic_unselected"
                )
            }
            equals_condition = (
                {"type": "research_equals", "tech_type": target}
                if category == "research"
                else {"type": "civic_equals", "civic_type": target}
            )
            return ProposedTask(
                task_id=task_id,
                action_type=action_type,
                entity_type=category,
                entity_id=target,
                due_turn=snapshot.turn,
                expires_turn=snapshot.turn,
                arguments={"tech_or_civic": target},
                preconditions=[unselected_condition, available_condition],
                postconditions=[equals_condition],
                invalidators=[],
                reason=f"Continue approved {category} queue with {target}.",
            )
        return None

    @staticmethod
    def _first_queue(strategy: dict[str, Any], keys: tuple[str, ...]) -> list[Any]:
        for key in keys:
            value = strategy.get(key)
            if isinstance(value, list):
                return value
        return []

    @staticmethod
    def _queue_target(raw: Any, keys: tuple[str, ...]) -> str | None:
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        if not isinstance(raw, dict):
            return None
        for key in keys:
            value = raw.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _task_id(category: str, index: int, target: str) -> str:
        digest = hashlib.sha1(f"{category}:{index}:{target}".encode()).hexdigest()[:12]
        return f"{category}-queue:{index}:{digest}"

    @staticmethod
    def _invalid_queue_event(
        snapshot: RuntimeSnapshot,
        category: str,
        index: int,
        raw: Any,
        reason: str,
    ) -> GameEvent:
        return GameEvent(
            event_type=f"invalid_{category}_queue_item",
            turn=snapshot.turn,
            entity_type=category,
            level=EventLevel.L3,
            risk=RiskLevel.MEDIUM,
            blocking=True,
            payload={"queue_index": index, "item": raw, "reason": reason},
            dedupe_key=f"invalid_{category}_queue_item:{index}:{raw!r}",
        )
