from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .conditions import find_entity
from .models import (
    EventLevel,
    GameEvent,
    PlanBundle,
    ProposedTask,
    RiskLevel,
    RuntimeSnapshot,
)
from .store import WorkflowStore


@dataclass(slots=True)
class RuleCompilation:
    bundle: PlanBundle | None = None
    events: list[GameEvent] = field(default_factory=list)


class DeterministicRuleCompiler:
    """Turn approved structured plans into small, verifiable current-turn tasks."""

    def __init__(self, store: WorkflowStore):
        self.store = store

    def compile(self, snapshot: RuntimeSnapshot) -> RuleCompilation:
        context = self.store.current_context(snapshot.game_id)
        tasks: list[ProposedTask] = []
        events: list[GameEvent] = []
        bindable_units = self.store.observe_units(
            snapshot.game_id, snapshot.turn, snapshot.units
        )
        self._auto_bind_builders(
            snapshot,
            context.get("builders", {}),
            bindable_units,
            events,
        )
        tasks.extend(
            self._compile_city_production(
                snapshot, context.get("cities", {}), events
            )
        )
        tasks.extend(
            self._compile_builders(snapshot, context.get("builders", {}), events)
        )
        if not tasks:
            return RuleCompilation(events=events)
        return RuleCompilation(
            bundle=PlanBundle(
                plan_id=f"rules_turn_{snapshot.turn}",
                summary="Deterministic tasks compiled from approved plans.",
                tasks=tasks,
            ),
            events=events,
        )

    def needs_units(self, game_id: str) -> bool:
        context = self.store.current_context(game_id)
        return bool(context.get("builders") or context.get("units"))

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
            source_plan_id = str(plan.get("_plan_id", "unknown"))
            selected: tuple[int, dict[str, Any]] | None = None
            for index, raw_item in enumerate(queue):
                task_id = f"city-production:{city_id}:{source_plan_id}:{index}"
                status = self.store.task_status(snapshot.game_id, task_id)
                if status is not None:
                    if status.value in {"done", "cancelled", "expired"}:
                        continue
                    selected = None
                    break
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
                            dedupe_key=(
                                f"invalid_city_plan_item:{city_id}:"
                                f"{source_plan_id}:{index}"
                            ),
                        )
                    )
                    break
                selected = (index, item)
                break
            if selected is None:
                continue
            index, item = selected
            task_id = f"city-production:{city_id}:{source_plan_id}:{index}"
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
                        {
                            "type": "entity_exists",
                            "entity_type": "city",
                            "entity_id": city_id,
                        },
                        {"type": "city_has_no_production", "city_id": city_id},
                    ],
                    postconditions=[
                        {
                            "type": "city_production_equals",
                            "city_id": city_id,
                            "item_name": item["item_name"],
                        }
                    ],
                    invalidators=[],
                    reason=(
                        "Continue approved production queue with "
                        f"{item['item_name']}."
                    ),
                )
            )
        return tasks

    def _compile_builders(
        self,
        snapshot: RuntimeSnapshot,
        plans: dict[str, dict[str, Any]],
        events: list[GameEvent],
    ) -> list[ProposedTask]:
        if snapshot.units is None:
            return []
        tasks: list[ProposedTask] = []
        for builder_key, plan in plans.items():
            unit_id = plan.get("assigned_unit_id")
            if unit_id is None:
                continue
            unit = find_entity(snapshot.units, ("unit_id", "id"), str(unit_id))
            if unit is None:
                events.append(
                    GameEvent(
                        event_type="builder_unit_missing",
                        turn=snapshot.turn,
                        entity_type="builder",
                        entity_id=unit_id,
                        level=EventLevel.L3,
                        risk=RiskLevel.HIGH,
                        blocking=True,
                        payload={"builder_key": builder_key},
                        dedupe_key=f"builder_unit_missing:{builder_key}:{unit_id}",
                    )
                )
                continue

            source_plan_id = str(plan.get("_plan_id", "unknown"))
            current_xy = (int(unit.get("x", -1)), int(unit.get("y", -1)))
            path = [
                point
                for point in (
                    self._point(item) for item in plan.get("path", [])
                )
                if point
            ]
            target = self._target(plan)
            if target is None:
                events.append(
                    GameEvent(
                        event_type="invalid_builder_plan",
                        turn=snapshot.turn,
                        entity_type="builder",
                        entity_id=unit_id,
                        level=EventLevel.L3,
                        risk=RiskLevel.MEDIUM,
                        blocking=True,
                        payload={"builder_key": builder_key, "reason": "missing target"},
                        dedupe_key=(
                            f"invalid_builder_plan:{builder_key}:"
                            f"{source_plan_id}:missing_target"
                        ),
                    )
                )
                continue

            if current_xy == (target["x"], target["y"]):
                improvement = target.get("improvement_type")
                charges = int(unit.get("build_charges", 0) or 0)
                if not improvement:
                    events.append(
                        GameEvent(
                            event_type="invalid_builder_plan",
                            turn=snapshot.turn,
                            entity_type="builder",
                            entity_id=unit_id,
                            level=EventLevel.L3,
                            risk=RiskLevel.MEDIUM,
                            blocking=True,
                            payload={
                                "builder_key": builder_key,
                                "reason": "target has no improvement_type",
                            },
                            dedupe_key=(
                                f"invalid_builder_plan:{builder_key}:"
                                f"{source_plan_id}:missing_improvement"
                            ),
                        )
                    )
                    continue
                if charges <= 0:
                    events.append(
                        GameEvent(
                            event_type="builder_no_charges",
                            turn=snapshot.turn,
                            entity_type="builder",
                            entity_id=unit_id,
                            level=EventLevel.L3,
                            risk=RiskLevel.MEDIUM,
                            blocking=True,
                            payload={"builder_key": builder_key},
                            dedupe_key=(
                                f"builder_no_charges:{builder_key}:{unit_id}"
                            ),
                        )
                    )
                    continue
                task_id = (
                    f"builder-improve:{builder_key}:"
                    f"{source_plan_id}:{improvement}"
                )
                if self.store.task_status(snapshot.game_id, task_id) is not None:
                    continue
                tasks.append(
                    ProposedTask(
                        task_id=task_id,
                        action_type="builder_improve",
                        entity_type="builder",
                        entity_id=unit_id,
                        due_turn=snapshot.turn,
                        expires_turn=snapshot.turn,
                        arguments={
                            "unit_id": (
                                int(unit_id)
                                if str(unit_id).isdigit()
                                else unit_id
                            ),
                            "improvement_type": improvement,
                        },
                        preconditions=[
                            {
                                "type": "entity_exists",
                                "entity_type": "builder",
                                "entity_id": unit_id,
                            },
                            {
                                "type": "unit_at",
                                "unit_id": unit_id,
                                "x": target["x"],
                                "y": target["y"],
                            },
                            {
                                "type": "unit_has_build_charge",
                                "unit_id": unit_id,
                            },
                            {
                                "type": "unit_can_improve",
                                "unit_id": unit_id,
                                "improvement_type": improvement,
                            },
                        ],
                        postconditions=[
                            {
                                "type": "unit_build_charges_equals",
                                "unit_id": unit_id,
                                "charges": charges - 1,
                            }
                        ],
                        invalidators=[],
                        reason=(
                            "Execute approved builder improvement "
                            f"{improvement}."
                        ),
                    )
                )
                continue

            current_index = self._last_index(path, current_xy)
            if current_index is None:
                events.append(
                    GameEvent(
                        event_type="builder_path_mismatch",
                        turn=snapshot.turn,
                        entity_type="builder",
                        entity_id=unit_id,
                        level=EventLevel.L3,
                        risk=RiskLevel.MEDIUM,
                        blocking=True,
                        payload={
                            "builder_key": builder_key,
                            "current": current_xy,
                            "path": path,
                            "target": target,
                        },
                        dedupe_key=(
                            f"builder_path_mismatch:{builder_key}:{current_xy}"
                        ),
                    )
                )
                continue
            next_index = current_index + 1
            next_point = (
                (target["x"], target["y"])
                if next_index >= len(path)
                else path[next_index]
            )
            task_id = (
                f"builder-move:{builder_key}:{source_plan_id}:{next_index}"
            )
            if self.store.task_status(snapshot.game_id, task_id) is not None:
                continue
            tasks.append(
                ProposedTask(
                    task_id=task_id,
                    action_type="unit_move",
                    entity_type="builder",
                    entity_id=unit_id,
                    due_turn=snapshot.turn,
                    expires_turn=snapshot.turn,
                    arguments={
                        "unit_id": (
                            int(unit_id) if str(unit_id).isdigit() else unit_id
                        ),
                        "target_x": next_point[0],
                        "target_y": next_point[1],
                    },
                    preconditions=[
                        {
                            "type": "entity_exists",
                            "entity_type": "builder",
                            "entity_id": unit_id,
                        },
                        {
                            "type": "unit_at",
                            "unit_id": unit_id,
                            "x": current_xy[0],
                            "y": current_xy[1],
                        },
                    ],
                    postconditions=[
                        {
                            "type": "unit_at",
                            "unit_id": unit_id,
                            "x": next_point[0],
                            "y": next_point[1],
                        }
                    ],
                    invalidators=[],
                    reason=f"Advance approved builder path to {next_point}.",
                )
            )
        return tasks

    def _auto_bind_builders(
        self,
        snapshot: RuntimeSnapshot,
        plans: dict[str, dict[str, Any]],
        bindable_units: dict[str, int],
        events: list[GameEvent],
    ) -> None:
        if snapshot.units is None or not bindable_units:
            return
        assigned = {
            str(plan["assigned_unit_id"])
            for plan in plans.values()
            if plan.get("assigned_unit_id") is not None
        }
        candidates: dict[str, dict[str, Any]] = {}
        for row in self._unit_rows(snapshot.units):
            raw_id = row.get("unit_id", row.get("id"))
            if raw_id is None:
                continue
            unit_id = str(raw_id)
            unit_type = str(
                row.get("unit_type", row.get("type", row.get("name", "")))
            ).upper()
            if (
                unit_id not in bindable_units
                or unit_id in assigned
                or "BUILDER" not in unit_type
            ):
                continue
            candidates[unit_id] = {
                **row,
                "_workflow_first_seen_turn": bindable_units[unit_id],
            }

        unbound = {
            key: plan
            for key, plan in plans.items()
            if plan.get("assigned_unit_id") is None
        }
        eligible_after_plan = {
            key: sorted(
                unit_id
                for unit_id, unit in candidates.items()
                if int(unit["_workflow_first_seen_turn"])
                > self._bind_after_turn(plan)
            )
            for key, plan in unbound.items()
        }
        matches = {
            key: sorted(
                unit_id
                for unit_id, unit in candidates.items()
                if self._builder_matches_plan(plan, unit_id, unit)
            )
            for key, plan in unbound.items()
        }
        owners: dict[str, list[str]] = {}
        for key, unit_ids in matches.items():
            for unit_id in unit_ids:
                owners.setdefault(unit_id, []).append(key)
        pairs = [
            (key, unit_ids[0])
            for key, unit_ids in matches.items()
            if len(unit_ids) == 1 and len(owners.get(unit_ids[0], [])) == 1
        ]

        bound_keys: set[str] = set()
        bound_unit_ids: set[str] = set()
        for builder_key, unit_id in pairs:
            if not self.store.bind_builder_plan(
                snapshot.game_id, builder_key, unit_id, snapshot.turn
            ):
                continue
            bound_keys.add(builder_key)
            bound_unit_ids.add(unit_id)
            plan = plans[builder_key]
            plan["assigned_unit_id"] = (
                int(unit_id) if unit_id.isdigit() else unit_id
            )
            plan["auto_bound_turn"] = snapshot.turn
            events.append(
                GameEvent(
                    event_type="builder_auto_bound",
                    turn=snapshot.turn,
                    entity_type="builder",
                    entity_id=unit_id,
                    level=EventLevel.L1,
                    payload={"builder_key": builder_key, "unit_id": unit_id},
                    dedupe_key=f"builder_auto_bound:{builder_key}:{unit_id}",
                )
            )

        ambiguous = {
            key: unit_ids
            for key, unit_ids in matches.items()
            if unit_ids and key not in bound_keys
        }
        if ambiguous:
            events.append(
                GameEvent(
                    event_type="builder_binding_ambiguous",
                    turn=snapshot.turn,
                    entity_type="builder",
                    level=EventLevel.L3,
                    risk=RiskLevel.MEDIUM,
                    blocking=True,
                    payload={"candidates": ambiguous},
                    dedupe_key=(
                        "builder_binding_ambiguous:"
                        + ";".join(
                            f"{key}={','.join(unit_ids)}"
                            for key, unit_ids in sorted(ambiguous.items())
                        )
                    ),
                )
            )

        remaining_eligible = {
            key: [
                unit_id
                for unit_id in unit_ids
                if unit_id not in bound_unit_ids
            ]
            for key, unit_ids in eligible_after_plan.items()
        }
        unmatched = {
            key: {
                "candidate_unit_ids": remaining_eligible[key],
                "required": {
                    field: plan[field]
                    for field in (
                        "expected_unit_id",
                        "origin_city_id",
                        "bind_after_turn",
                    )
                    if plan.get(field) is not None
                },
                "observed_origin_fields": {
                    unit_id: self._origin_fields(candidates[unit_id])
                    for unit_id in remaining_eligible[key]
                },
            }
            for key, plan in unbound.items()
            if remaining_eligible[key]
            and not matches[key]
            and key not in bound_keys
        }
        if unmatched:
            events.append(
                GameEvent(
                    event_type="builder_binding_unmatched",
                    turn=snapshot.turn,
                    entity_type="builder",
                    level=EventLevel.L3,
                    risk=RiskLevel.MEDIUM,
                    blocking=True,
                    payload={"plans": unmatched},
                    dedupe_key=(
                        "builder_binding_unmatched:"
                        + ";".join(
                            f"{key}={','.join(value['candidate_unit_ids'])}"
                            for key, value in sorted(unmatched.items())
                        )
                    ),
                )
            )

    @classmethod
    def _builder_matches_plan(
        cls, plan: dict[str, Any], unit_id: str, unit: dict[str, Any]
    ) -> bool:
        first_seen_turn = int(unit["_workflow_first_seen_turn"])
        if first_seen_turn <= cls._bind_after_turn(plan):
            return False
        expected_id = plan.get("expected_unit_id")
        if expected_id is not None and str(expected_id) != unit_id:
            return False
        origin_city_id = plan.get("origin_city_id")
        if origin_city_id is not None:
            actual_origin = next(
                (
                    unit.get(key)
                    for key in (
                        "origin_city_id",
                        "home_city_id",
                        "produced_by_city_id",
                        "city_id",
                    )
                    if unit.get(key) is not None
                ),
                None,
            )
            if actual_origin is None or str(actual_origin) != str(origin_city_id):
                return False
        return True

    @staticmethod
    def _bind_after_turn(plan: dict[str, Any]) -> int:
        return int(plan.get("bind_after_turn", plan.get("_updated_turn", -1)))

    @staticmethod
    def _origin_fields(unit: dict[str, Any]) -> dict[str, Any]:
        return {
            key: unit[key]
            for key in (
                "origin_city_id",
                "home_city_id",
                "produced_by_city_id",
                "city_id",
            )
            if unit.get(key) is not None
        }

    @staticmethod
    def _unit_rows(value: Any) -> list[dict[str, Any]]:
        if isinstance(value, dict):
            value = value.get("units", value.get("items", []))
        if not isinstance(value, list):
            return []
        return [row for row in value if isinstance(row, dict)]

    @staticmethod
    def _production_item(raw: Any) -> dict[str, Any] | None:
        if isinstance(raw, str):
            prefix = raw.split("_", 1)[0]
            if prefix not in {"UNIT", "BUILDING", "DISTRICT", "PROJECT"}:
                return None
            return {"item_type": prefix, "item_name": raw}
        if not isinstance(raw, dict):
            return None
        item_name = raw.get("item_name") or raw.get("name")
        item_type = raw.get("item_type") or raw.get("category")
        if not item_name or not item_type:
            return None
        return {
            "item_type": str(item_type),
            "item_name": str(item_name),
            "target_x": raw.get("target_x"),
            "target_y": raw.get("target_y"),
        }

    @staticmethod
    def _point(raw: Any) -> tuple[int, int] | None:
        if (
            isinstance(raw, dict)
            and raw.get("x") is not None
            and raw.get("y") is not None
        ):
            return int(raw["x"]), int(raw["y"])
        if isinstance(raw, (list, tuple)) and len(raw) == 2:
            return int(raw[0]), int(raw[1])
        return None

    @classmethod
    def _target(cls, plan: dict[str, Any]) -> dict[str, Any] | None:
        raw = plan.get("target") or plan.get("primary_target")
        if (
            not isinstance(raw, dict)
            or raw.get("x") is None
            or raw.get("y") is None
        ):
            return None
        return {
            "x": int(raw["x"]),
            "y": int(raw["y"]),
            "improvement_type": raw.get("improvement_type")
            or raw.get("improvement"),
        }

    @staticmethod
    def _last_index(
        path: list[tuple[int, int]], point: tuple[int, int]
    ) -> int | None:
        matches = [
            index for index, candidate in enumerate(path) if candidate == point
        ]
        return matches[-1] if matches else None
