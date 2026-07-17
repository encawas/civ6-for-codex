from __future__ import annotations

from typing import Any

from .models import EventLevel, GameEvent, ProposedTask, RiskLevel, RuntimeSnapshot
from .safe_rules import SafeDeterministicRuleCompiler


class SettlerDeterministicRuleCompiler(SafeDeterministicRuleCompiler):
    """Adds a safe settler plan: site selection -> travel -> found city."""

    def _compile_unit_blocker(
        self,
        snapshot: RuntimeSnapshot,
        context: dict[str, Any],
        *,
        unit_blocker_present: bool,
    ) -> tuple[list[ProposedTask], list[GameEvent]]:
        tasks, events = super()._compile_unit_blocker(
            snapshot,
            context,
            unit_blocker_present=unit_blocker_present,
        )
        unit_plans = context.get("units", {})
        if not isinstance(unit_plans, dict):
            unit_plans = {}

        retained: list[GameEvent] = []
        for event in events:
            if event.event_type != "special_unit_orders_required":
                retained.append(event)
                continue
            unit = (
                event.payload.get("unit") if isinstance(event.payload, dict) else None
            )
            if not isinstance(unit, dict):
                retained.append(event)
                continue
            unit_type = str(
                unit.get("unit_type", unit.get("type", unit.get("name", "")))
            ).upper()
            if "SETTLER" not in unit_type:
                retained.append(event)
                continue

            raw_id = unit.get("unit_id", unit.get("id", event.entity_id))
            unit_id = str(raw_id)
            plan = unit_plans.get(unit_id)
            if (
                not isinstance(plan, dict)
                or str(plan.get("goal", "")).lower() != "found_city"
            ):
                retained.append(self._settler_selection_event(snapshot, unit, raw_id))
                continue

            task, review_event = self._compile_settler_plan(snapshot, unit, plan)
            if task is not None:
                tasks.append(task)
            if review_event is not None:
                retained.append(review_event)

        return tasks, retained

    def _compile_settler_plan(
        self,
        snapshot: RuntimeSnapshot,
        unit: dict[str, Any],
        plan: dict[str, Any],
    ) -> tuple[ProposedTask | None, GameEvent | None]:
        raw_id = unit.get("unit_id", unit.get("id"))
        unit_id = str(raw_id)
        target = self._point(plan.get("target"))
        if target is None:
            return None, self._settler_review_event(
                snapshot,
                unit,
                "settler_plan_requires_review",
                "Settler plan has no valid target coordinates.",
                plan_revision=plan.get(
                    "revision", plan.get("_plan_id", plan.get("plan_id", "unknown"))
                ),
            )

        current = (int(unit.get("x", -1)), int(unit.get("y", -1)))
        plan_id = str(plan.get("_plan_id", "unknown"))
        if current != target:
            task_id = (
                f"settler-move:{unit_id}:{target[0]}:{target[1]}:"
                f"{current[0]}:{current[1]}"
            )
            if self.store.task_status(snapshot.game_id, task_id) is not None:
                return None, None
            return (
                ProposedTask(
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
                        {
                            "type": "entity_exists",
                            "entity_type": "unit",
                            "entity_id": raw_id,
                        },
                        {"type": "unit_has_moves", "unit_id": raw_id},
                        {
                            "type": "unit_type_contains",
                            "unit_id": raw_id,
                            "marker": "SETTLER",
                        },
                        {
                            "type": "unit_at",
                            "unit_id": raw_id,
                            "x": current[0],
                            "y": current[1],
                        },
                    ],
                    postconditions=[
                        {
                            "type": "unit_moved_from",
                            "unit_id": raw_id,
                            "x": current[0],
                            "y": current[1],
                        }
                    ],
                    invalidators=[],
                    risk=RiskLevel.HIGH,
                    requires_confirmation=True,
                    reason=(
                        f"Advance the approved settler plan {plan_id} toward "
                        f"the selected site {target}."
                    ),
                ),
                None,
            )

        city_count = len(self._city_rows(snapshot.cities))
        task_id = f"settler-found-city:{unit_id}:{target[0]}:{target[1]}"
        if self.store.task_status(snapshot.game_id, task_id) is not None:
            return None, None
        return (
            ProposedTask(
                task_id=task_id,
                action_type="unit_found_city",
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
                    {
                        "type": "unit_type_contains",
                        "unit_id": raw_id,
                        "marker": "SETTLER",
                    },
                    {
                        "type": "unit_at",
                        "unit_id": raw_id,
                        "x": target[0],
                        "y": target[1],
                    },
                ],
                postconditions=[
                    {"type": "unit_absent", "unit_id": raw_id},
                    {"type": "city_count_at_least", "count": city_count + 1},
                ],
                invalidators=[],
                risk=RiskLevel.HIGH,
                requires_confirmation=True,
                reason=f"Found a city at the approved settlement site {target}.",
            ),
            None,
        )

    @staticmethod
    def _settler_selection_event(
        snapshot: RuntimeSnapshot, unit: dict[str, Any], raw_id: Any
    ) -> GameEvent:
        return GameEvent(
            event_type="settler_site_selection_required",
            turn=snapshot.turn,
            entity_type="unit",
            entity_id=raw_id,
            level=EventLevel.L3,
            risk=RiskLevel.HIGH,
            blocking=True,
            payload={
                "reason": "Settler needs an approved city site before it can move.",
                "unit": unit,
            },
            dedupe_key=f"settler_site_selection_required:{raw_id}",
        )

    @staticmethod
    def _settler_review_event(
        snapshot: RuntimeSnapshot,
        unit: dict[str, Any],
        event_type: str,
        reason: str,
        plan_revision: Any,
    ) -> GameEvent:
        raw_id = unit.get("unit_id", unit.get("id", "unknown"))
        return GameEvent(
            event_type=event_type,
            turn=snapshot.turn,
            entity_type="unit",
            entity_id=raw_id,
            level=EventLevel.L3,
            risk=RiskLevel.HIGH,
            blocking=True,
            payload={
                "reason": reason,
                "unit": unit,
                "plan_revision": plan_revision,
            },
            dedupe_key=f"{event_type}:{raw_id}:{plan_revision}",
        )

    @staticmethod
    def _route_unit_without_blocker(unit: dict[str, Any]) -> bool:
        unit_type = str(
            unit.get("unit_type", unit.get("type", unit.get("name", "")))
        ).upper()
        return "SETTLER" in unit_type

    @staticmethod
    def _city_rows(value: Any) -> list[dict[str, Any]]:
        if isinstance(value, dict):
            value = value.get("items", value.get("cities", []))
        if not isinstance(value, list):
            return []
        return [row for row in value if isinstance(row, dict)]
