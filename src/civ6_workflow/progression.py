from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from .models import EventLevel, GameEvent, PlanBundle, ProposedTask, RiskLevel, RuntimeSnapshot
from .store import WorkflowStore


@dataclass(slots=True)
class ProgressionCompilation:
    bundle: PlanBundle | None = None
    events: list[GameEvent] = field(default_factory=list)


class ProgressionRuleCompiler:
    """Compile approved research and civic queues into one verifiable choice."""

    def __init__(self, store: WorkflowStore):
        self.store = store

    def compile(self, snapshot: RuntimeSnapshot) -> ProgressionCompilation:
        strategy = self.store.current_context(snapshot.game_id).get("strategy", {})
        if not isinstance(strategy, dict):
            strategy = {}
        tasks: list[ProposedTask] = []
        events: list[GameEvent] = []
        research = self._compile_category(
            snapshot,
            strategy,
            category="research",
            queue_keys=("research_queue", "tech_queue"),
            action_type="set_research",
            target_keys=("tech_type", "item_name", "name"),
            available_key="available_techs",
            available_type_key="tech_type",
            events=events,
        )
        if research is not None:
            tasks.append(research)
        civic = self._compile_category(
            snapshot,
            strategy,
            category="civic",
            queue_keys=("civic_queue",),
            action_type="set_civic",
            target_keys=("civic_type", "item_name", "name"),
            available_key="available_civics",
            available_type_key="civic_type",
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
        snapshot: RuntimeSnapshot,
        strategy: dict[str, Any],
        *,
        category: str,
        queue_keys: tuple[str, ...],
        action_type: str,
        target_keys: tuple[str, ...],
        available_key: str,
        available_type_key: str,
        events: list[GameEvent],
    ) -> ProposedTask | None:
        queue = self._first_queue(strategy, queue_keys)
        if not queue:
            return None
        progress = self._progress(snapshot)
        current_type = self.current_type(progress, category)
        current_name = self.current_name(progress, category)
        if not self._is_unselected(current_name):
            return None
        available = {
            str(item[available_type_key])
            for item in self._rows(progress.get(available_key))
            if item.get(available_type_key)
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
            if current_type == target:
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
    def _progress(snapshot: RuntimeSnapshot) -> dict[str, Any]:
        return snapshot.tech_civics if isinstance(snapshot.tech_civics, dict) else {}

    @classmethod
    def current_name(cls, progress: dict[str, Any], category: str) -> Any:
        key = "current_research" if category == "research" else "current_civic"
        return progress.get(key)

    @classmethod
    def current_type(cls, progress: dict[str, Any], category: str) -> str | None:
        explicit_key = (
            "current_research_type"
            if category == "research"
            else "current_civic_type"
        )
        explicit = progress.get(explicit_key)
        if isinstance(explicit, str) and explicit:
            return explicit
        current = cls.current_name(progress, category)
        if cls._is_unselected(current):
            return None
        current_text = str(current)
        if current_text.startswith(("TECH_", "CIVIC_")):
            return current_text
        options_key = "available_techs" if category == "research" else "available_civics"
        type_key = "tech_type" if category == "research" else "civic_type"
        for item in cls._rows(progress.get(options_key)):
            if str(item.get("name", "")) == current_text and item.get(type_key):
                return str(item[type_key])
        return None

    @staticmethod
    def _is_unselected(value: Any) -> bool:
        return value in (None, "", "None", "NONE", "none", {}, [])

    @staticmethod
    def _rows(value: Any) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        return [item for item in value if isinstance(item, dict)]

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
