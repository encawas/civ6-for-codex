from __future__ import annotations

import hashlib
import json
from typing import Any

from .conditions import find_entity
from .models import (
    EventLevel,
    GameEvent,
    PlanBundle,
    ProposedTask,
    RiskLevel,
    RuntimeSnapshot,
    TaskStatus,
)
from .rules import DeterministicRuleCompiler as BaseDeterministicRuleCompiler


_TERMINAL_TASKS = {
    TaskStatus.DONE,
    TaskStatus.CANCELLED,
    TaskStatus.EXPIRED,
    TaskStatus.ESCALATED,
}
_SPECIAL_CIVILIAN_MARKERS = (
    "SETTLER",
    "GREAT_",
    "SPY",
    "TRADER",
    "MISSIONARY",
    "APOSTLE",
    "GURU",
    "INQUISITOR",
    "ARCHAEOLOGIST",
    "NATURALIST",
    "ROCK_BAND",
)


class SafeDeterministicRuleCompiler(BaseDeterministicRuleCompiler):
    """Hardened deterministic compiler, including ordinary unit blockers."""

    def compile(self, snapshot: RuntimeSnapshot):
        compiled = super().compile(snapshot)
        zero_city_start = (
            isinstance(snapshot.overview, dict)
            and snapshot.overview.get("num_cities") == 0
        )
        needs_unit_orders = self._has_unit_blocker(snapshot) or zero_city_start
        if snapshot.units is None or not needs_unit_orders:
            return compiled

        context = self.store.current_context(snapshot.game_id)
        unit_tasks, unit_events = self._compile_unit_blocker(snapshot, context)
        compiled.events.extend(unit_events)
        if not unit_tasks:
            return compiled

        if compiled.bundle is None:
            compiled.bundle = PlanBundle(
                plan_id=f"rules_units_turn_{snapshot.turn}",
                summary="Deterministic unit orders compiled for the end-turn blocker.",
                tasks=unit_tasks,
            )
        else:
            compiled.bundle = compiled.bundle.model_copy(
                update={
                    "summary": (
                        compiled.bundle.summary
                        + " Deterministic unit orders resolve ordinary end-turn blockers."
                    ),
                    "tasks": [*compiled.bundle.tasks, *unit_tasks],
                }
            )
        return compiled

    def _compile_unit_blocker(
        self, snapshot: RuntimeSnapshot, context: dict[str, Any]
    ) -> tuple[list[ProposedTask], list[GameEvent]]:
        active_units = {
            str(task.entity_id)
            for task in self.store.list_tasks(snapshot.game_id)
            if task.entity_type in {"unit", "builder"}
            and task.status not in _TERMINAL_TASKS
        }
        builder_units = {
            str(plan["assigned_unit_id"])
            for plan in context.get("builders", {}).values()
            if isinstance(plan, dict) and plan.get("assigned_unit_id") is not None
        }
        unit_plans = context.get("units", {})
        tasks: list[ProposedTask] = []
        events: list[GameEvent] = []

        for unit in self._unit_rows(snapshot.units):
            raw_id = unit.get("unit_id", unit.get("id"))
            if raw_id is None:
                continue
            unit_id = str(raw_id)
            if unit_id in active_units or unit_id in builder_units:
                continue
            moves = self._moves_remaining(unit)
            if moves <= 0:
                continue

            unit_type = str(
                unit.get("unit_type", unit.get("type", unit.get("name", "")))
            ).upper()
            if bool(unit.get("needs_promotion")):
                events.append(
                    self._unit_review_event(
                        snapshot,
                        unit,
                        "unit_promotion_required",
                        "Unit has an unselected promotion.",
                    )
                )
                continue
            if any(marker in unit_type for marker in _SPECIAL_CIVILIAN_MARKERS):
                events.append(
                    self._unit_review_event(
                        snapshot,
                        unit,
                        "special_unit_orders_required",
                        "Special civilian unit requires an explicit strategic decision.",
                        risk=RiskLevel.HIGH,
                    )
                )
                continue
            if unit.get("targets"):
                events.append(
                    self._unit_review_event(
                        snapshot,
                        unit,
                        "unit_combat_decision_required",
                        "Unit has available combat targets; deterministic skip is unsafe.",
                        risk=RiskLevel.HIGH,
                    )
                )
                continue

            plan = unit_plans.get(unit_id) if isinstance(unit_plans, dict) else None
            if isinstance(plan, dict):
                planned_task = self._compile_unit_plan(snapshot, unit, plan, events)
                if planned_task is not None:
                    tasks.append(planned_task)
                continue

            task_id = f"unit-skip:{unit_id}:{snapshot.turn}"
            if self.store.task_status(snapshot.game_id, task_id) is not None:
                continue
            tasks.append(
                ProposedTask(
                    task_id=task_id,
                    action_type="unit_skip",
                    entity_type="unit",
                    entity_id=raw_id,
                    due_turn=snapshot.turn,
                    expires_turn=snapshot.turn,
                    arguments={"unit_id": raw_id},
                    preconditions=[
                        {
                            "type": "entity_exists",
                            "entity_type": "unit",
                            "entity_id": raw_id,
                        },
                        {"type": "unit_has_moves", "unit_id": raw_id},
                    ],
                    postconditions=[
                        {"type": "unit_no_moves", "unit_id": raw_id}
                    ],
                    reason=(
                        "No approved plan or high-risk decision applies; consume the "
                        "ordinary unit's remaining orders deterministically."
                    ),
                )
            )
        return tasks, events

    def _compile_unit_plan(
        self,
        snapshot: RuntimeSnapshot,
        unit: dict[str, Any],
        plan: dict[str, Any],
        events: list[GameEvent],
    ) -> ProposedTask | None:
        raw_id = unit.get("unit_id", unit.get("id"))
        unit_id = str(raw_id)
        path = [point for point in (self._point(item) for item in plan.get("path", [])) if point]
        current = (int(unit.get("x", -1)), int(unit.get("y", -1)))
        current_index = self._last_index(path, current)
        if not path or current_index is None or current_index + 1 >= len(path):
            events.append(
                self._unit_review_event(
                    snapshot,
                    unit,
                    "unit_plan_requires_review",
                    "Existing unit plan has no deterministic next path step.",
                )
            )
            return None
        target = path[current_index + 1]
        plan_id = str(plan.get("_plan_id", "unknown"))
        task_id = f"unit-move:{unit_id}:{plan_id}:{current_index + 1}"
        if self.store.task_status(snapshot.game_id, task_id) is not None:
            return None
        return ProposedTask(
            task_id=task_id,
            action_type="unit_move",
            entity_type="unit",
            entity_id=raw_id,
            due_turn=snapshot.turn,
            expires_turn=snapshot.turn,
            arguments={
                "unit_id": raw_id,
                "target_x": target[0],
                "target_y": target[1],
            },
            preconditions=[
                {"type": "entity_exists", "entity_type": "unit", "entity_id": raw_id},
                {"type": "unit_has_moves", "unit_id": raw_id},
                {"type": "unit_at", "unit_id": raw_id, "x": current[0], "y": current[1]},
            ],
            postconditions=[
                {"type": "unit_at", "unit_id": raw_id, "x": target[0], "y": target[1]}
            ],
            reason=f"Advance the approved unit path to {target}.",
        )

    @staticmethod
    def _has_unit_blocker(snapshot: RuntimeSnapshot) -> bool:
        return any(
            str(blocker.get("blocking_type", "")) == "ENDTURN_BLOCKING_UNITS"
            for blocker in snapshot.blockers
            if isinstance(blocker, dict)
        )

    @staticmethod
    def _moves_remaining(unit: dict[str, Any]) -> float:
        try:
            return float(unit.get("moves_remaining", unit.get("moves", 0)) or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _unit_review_event(
        snapshot: RuntimeSnapshot,
        unit: dict[str, Any],
        event_type: str,
        reason: str,
        *,
        risk: RiskLevel = RiskLevel.MEDIUM,
    ) -> GameEvent:
        raw_id = unit.get("unit_id", unit.get("id", "unknown"))
        return GameEvent(
            event_type=event_type,
            turn=snapshot.turn,
            entity_type="unit",
            entity_id=raw_id,
            level=EventLevel.L3,
            risk=risk,
            blocking=True,
            payload={
                "reason": reason,
                "unit": {
                    key: unit.get(key)
                    for key in (
                        "unit_id",
                        "unit_type",
                        "name",
                        "x",
                        "y",
                        "moves_remaining",
                        "health",
                        "max_health",
                        "needs_promotion",
                        "targets",
                    )
                    if key in unit
                },
            },
            dedupe_key=f"{event_type}:{raw_id}:{snapshot.turn}",
        )

    def _compile_city_production(
        self,
        snapshot: RuntimeSnapshot,
        plans: dict[str, dict[str, Any]],
        events: list[GameEvent],
    ) -> list[ProposedTask]:
        tasks: list[ProposedTask] = []
        for city_id, plan in plans.items():
            city = find_entity(snapshot.cities, ("city_id", "id"), str(city_id))
            if city is None:
                continue
            current = city.get("currently_building", city.get("producing"))
            if current not in (None, "", "NONE", "none", {}, []):
                continue
            queue = plan.get("followup_queue", [])
            if not isinstance(queue, list) or not queue:
                continue

            occurrences: dict[str, int] = {}
            selected: tuple[str, dict[str, Any]] | None = None
            for index, raw_item in enumerate(queue):
                item = self._production_item(raw_item)
                if item is None:
                    events.append(
                        GameEvent(
                            event_type="invalid_city_plan_item",
                            turn=snapshot.turn,
                            entity_type="city",
                            entity_id=city_id,
                            level=EventLevel.L3,
                            risk=RiskLevel.MEDIUM,
                            blocking=True,
                            payload={"queue_index": index, "item": raw_item},
                            dedupe_key=f"invalid_city_plan_item:{city_id}:{index}:{raw_item!r}",
                        )
                    )
                    break

                semantic_key = self._production_semantic_key(item)
                occurrence = occurrences.get(semantic_key, 0)
                occurrences[semantic_key] = occurrence + 1
                task_id = f"city-production:{city_id}:{semantic_key}:{occurrence}"
                status = self.store.task_status(snapshot.game_id, task_id)
                if status is not None:
                    if status.value in {"done", "cancelled", "expired"}:
                        continue
                    selected = None
                    break
                selected = (task_id, item)
                break

            if selected is None:
                continue
            task_id, item = selected
            arguments = {
                "city_id": int(city_id) if str(city_id).isdigit() else city_id,
                "item_type": item["item_type"],
                "item_name": item["item_name"],
            }
            for key in ("target_x", "target_y"):
                if item.get(key) is not None:
                    arguments[key] = item[key]
            tasks.append(
                ProposedTask(
                    task_id=task_id,
                    action_type="city_set_production",
                    entity_type="city",
                    entity_id=city_id,
                    due_turn=snapshot.turn,
                    expires_turn=snapshot.turn,
                    arguments=arguments,
                    preconditions=[
                        {"type": "entity_exists", "entity_type": "city", "entity_id": city_id},
                        {"type": "city_has_no_production", "city_id": city_id},
                    ],
                    postconditions=[
                        {"type": "city_production_equals", "city_id": city_id, "item_name": item["item_name"]}
                    ],
                    invalidators=[],
                    reason=f"Continue approved production queue with {item['item_name']}.",
                )
            )
        return tasks

    @staticmethod
    def _production_semantic_key(item: dict[str, Any]) -> str:
        canonical = json.dumps(
            {
                "item_type": item.get("item_type"),
                "item_name": item.get("item_name"),
                "target_x": item.get("target_x"),
                "target_y": item.get("target_y"),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
