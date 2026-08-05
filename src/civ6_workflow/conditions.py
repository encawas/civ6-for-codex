from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from .domain.observations import SlotState
from .models import RuntimeSnapshot
from .observation_normalization import (
    NormalizedRuntimeObservation,
    normalize_runtime_snapshot,
)


ObservationInput = RuntimeSnapshot | NormalizedRuntimeObservation


CONDITION_REQUIRED_FACTS: dict[str, tuple[str, ...]] = {
    "no_blocker_type": ("blockers",),
    "blocker_kind_present": ("blockers",),
    "no_blocker_kind": ("blockers",),
    "city_production_equals": ("cities",),
    "city_has_no_production": ("cities",),
    "city_count_at_least": ("cities",),
    "city_at_target": ("cities",),
    "tile_unoccupied": ("cities",),
    "research_unselected": ("current_research",),
    "research_available": ("available_research",),
    "research_equals": ("current_research",),
    "civic_unselected": ("current_civic",),
    "civic_available": ("available_civics",),
    "civic_equals": ("current_civic",),
    "unit_at": ("units",),
    "unit_has_moves": ("units",),
    "unit_no_moves": ("units",),
    "unit_has_build_charge": ("units",),
    "unit_build_charges_equals": ("units",),
    "unit_can_improve": ("units",),
    "unit_type_contains": ("units",),
    "unit_moved_from": ("units",),
    "unit_absent": ("units",),
}


@dataclass(slots=True)
class ConditionResult:
    valid: bool
    reason: str = ""
    known: bool = True


class ConditionEvaluator:
    """Evaluates a deliberately small, auditable condition language."""

    def evaluate_all(
        self,
        conditions: list[dict[str, Any]],
        snapshot: ObservationInput,
        *,
        decision_projection: Mapping[str, Any] | None = None,
    ) -> ConditionResult:
        observation = self._normalize(snapshot)
        compatibility_projection = (
            observation.snapshot.model_dump(mode="json")
            if any(_uses_field_projection(condition) for condition in conditions)
            else None
        )
        for condition in conditions:
            result = self._evaluate_normalized(
                condition,
                observation,
                decision_projection=decision_projection,
                compatibility_projection=compatibility_projection,
            )
            if not result.valid:
                return result
        return ConditionResult(True)

    def evaluate(
        self,
        condition: dict[str, Any],
        snapshot: ObservationInput,
        *,
        decision_projection: Mapping[str, Any] | None = None,
    ) -> ConditionResult:
        observation = self._normalize(snapshot)
        compatibility_projection = (
            observation.snapshot.model_dump(mode="json")
            if _uses_field_projection(condition)
            else None
        )
        return self._evaluate_normalized(
            condition,
            observation,
            decision_projection=decision_projection,
            compatibility_projection=compatibility_projection,
        )

    @staticmethod
    def _normalize(snapshot: ObservationInput) -> NormalizedRuntimeObservation:
        if isinstance(snapshot, NormalizedRuntimeObservation):
            return snapshot
        return normalize_runtime_snapshot(snapshot)

    def _evaluate_normalized(
        self,
        condition: dict[str, Any],
        observation: NormalizedRuntimeObservation,
        *,
        decision_projection: Mapping[str, Any] | None = None,
        compatibility_projection: Mapping[str, Any] | None = None,
    ) -> ConditionResult:
        normalized_snapshot = observation.snapshot
        kind = condition.get("type")
        if kind != "all_of":
            missing = [
                fact
                for fact in _required_facts(condition)
                if not bool(getattr(observation.canonical.completeness, fact))
            ]
            if missing:
                return ConditionResult(
                    False,
                    "condition evidence unavailable: " + ", ".join(sorted(missing)),
                    known=False,
                )
        if kind == "all_of":
            nested = condition.get("conditions")
            if not isinstance(nested, (list, tuple)) or not nested:
                return ConditionResult(False, "all_of requires nested conditions")
            for item in nested:
                if not isinstance(item, Mapping):
                    return ConditionResult(
                        False, "all_of contains an invalid condition"
                    )
                result = self._evaluate_normalized(
                    dict(item),
                    observation,
                    decision_projection=decision_projection,
                    compatibility_projection=compatibility_projection,
                )
                if not result.valid:
                    return result
            return ConditionResult(True)

        if kind == "turn_at_least":
            expected = int(condition["turn"])
            return ConditionResult(
                normalized_snapshot.turn >= expected,
                f"turn {normalized_snapshot.turn} is below required turn {expected}",
            )
        if kind == "turn_equals":
            expected = int(condition["turn"])
            return ConditionResult(
                normalized_snapshot.turn == expected,
                f"turn {normalized_snapshot.turn} does not equal {expected}",
            )
        if kind == "no_blocker_type":
            blocker_type = str(condition["blocker_type"]).strip().casefold()
            present = any(
                blocker.source_type == blocker_type
                for blocker in observation.canonical.blockers
            )
            return ConditionResult(
                not present, f"blocker {blocker_type} is currently present"
            )
        if kind in {"blocker_kind_present", "no_blocker_kind"}:
            blocker_kind = str(condition["blocker_kind"]).strip().upper()
            present = any(
                blocker.blocker_type == blocker_kind
                for blocker in observation.canonical.blockers
            )
            expected = kind == "blocker_kind_present"
            return ConditionResult(
                present is expected,
                (
                    f"blocker kind {blocker_kind} is not present"
                    if expected
                    else f"blocker kind {blocker_kind} is still present"
                ),
            )
        if kind == "field_equals":
            path = str(condition["path"])
            expected = condition.get("value")
            actual = self._get_path(compatibility_projection or {}, path)
            return ConditionResult(
                actual == expected,
                f"field {path} expected {expected!r}, got {actual!r}",
            )
        if kind == "field_in":
            path = str(condition["path"])
            allowed = condition.get("values", [])
            actual = self._get_path(compatibility_projection or {}, path)
            return ConditionResult(
                actual in allowed,
                f"field {path} value {actual!r} is not in {allowed!r}",
            )
        if kind == "entity_exists":
            entity_type = str(condition["entity_type"])
            entity_id = str(condition["entity_id"])
            exists = entity_id in extract_known_entities(observation).get(
                entity_type, set()
            )
            return ConditionResult(
                exists,
                f"{entity_type} {entity_id} does not exist in the current snapshot",
            )
        if kind == "city_production_equals":
            city_id = str(condition["city_id"])
            expected = str(condition["item_name"])
            city = observation.canonical.city(city_id)
            if city is None:
                return ConditionResult(False, f"city {city_id} does not exist")
            if city.production.state is SlotState.NOT_LOADED:
                return ConditionResult(
                    False,
                    "condition evidence unavailable: city production",
                    known=False,
                )
            actual = city.production.value
            return ConditionResult(
                actual == expected,
                f"city {city_id} production expected {expected!r}, got {actual!r}",
            )
        if kind == "city_has_no_production":
            city_id = str(condition["city_id"])
            city = observation.canonical.city(city_id)
            if city is None:
                return ConditionResult(False, f"city {city_id} does not exist")
            if city.production.state is SlotState.NOT_LOADED:
                return ConditionResult(
                    False,
                    "condition evidence unavailable: city production",
                    known=False,
                )
            empty = city.production.state is SlotState.EMPTY
            return ConditionResult(
                empty,
                f"city {city_id} is already producing {city.production.value!r}",
            )
        if kind == "research_unselected":
            slot = observation.canonical.progression.current_research
            if slot.state is SlotState.NOT_LOADED:
                return ConditionResult(
                    False,
                    "condition evidence unavailable: current research",
                    known=False,
                )
            return ConditionResult(
                slot.state is SlotState.EMPTY,
                f"research is already selected: {slot.value!r}",
            )
        if kind == "civic_unselected":
            slot = observation.canonical.progression.current_civic
            if slot.state is SlotState.NOT_LOADED:
                return ConditionResult(
                    False, "condition evidence unavailable: current civic", known=False
                )
            return ConditionResult(
                slot.state is SlotState.EMPTY,
                f"civic is already selected: {slot.value!r}",
            )
        if kind == "research_available":
            expected = str(condition["tech_type"])
            available = {
                entity.value
                for entity in (observation.canonical.progression.available_research_ids)
            }
            return ConditionResult(
                expected in available,
                f"technology {expected} is not available; available={sorted(available)}",
            )
        if kind == "civic_available":
            expected = str(condition["civic_type"])
            available = {
                entity.value
                for entity in (observation.canonical.progression.available_civic_ids)
            }
            return ConditionResult(
                expected in available,
                f"civic {expected} is not available; available={sorted(available)}",
            )
        if kind == "research_equals":
            expected = str(condition["tech_type"])
            actual = observation.canonical.progression.current_research.value
            return ConditionResult(
                actual == expected,
                f"research expected {expected!r}, got {actual!r}",
            )
        if kind == "civic_equals":
            expected = str(condition["civic_type"])
            actual = observation.canonical.progression.current_civic.value
            return ConditionResult(
                actual == expected,
                f"civic expected {expected!r}, got {actual!r}",
            )
        if kind == "unit_at":
            unit_id = str(condition["unit_id"])
            unit = observation.canonical.unit(unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            if unit.x is None or unit.y is None:
                return _unknown("unit position")
            expected = (int(condition["x"]), int(condition["y"]))
            actual = (unit.x, unit.y)
            return ConditionResult(
                actual == expected,
                f"unit {unit_id} expected at {expected}, got {actual}",
            )
        if kind in {"unit_has_moves", "unit_no_moves"}:
            unit_id = str(condition["unit_id"])
            unit = observation.canonical.unit(unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            if unit.moves_remaining is None:
                return _unknown("unit moves")
            if kind == "unit_has_moves":
                return ConditionResult(
                    unit.moves_remaining > 0,
                    f"unit {unit_id} has no moves remaining ({unit.moves_remaining})",
                )
            return ConditionResult(
                unit.moves_remaining <= 0,
                f"unit {unit_id} still has {unit.moves_remaining} moves remaining",
            )
        if kind in {"unit_has_build_charge", "unit_build_charges_equals"}:
            unit_id = str(condition["unit_id"])
            unit = observation.canonical.unit(unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            if unit.build_charges is None:
                return _unknown("unit build charges")
            if kind == "unit_has_build_charge":
                return ConditionResult(
                    unit.build_charges > 0,
                    f"unit {unit_id} has no build charges",
                )
            expected = int(condition["charges"])
            return ConditionResult(
                unit.build_charges == expected,
                (
                    f"unit {unit_id} charges expected {expected}, "
                    f"got {unit.build_charges}"
                ),
            )
        if kind == "unit_can_improve":
            unit_id = str(condition["unit_id"])
            improvement = str(condition["improvement_type"]).strip().upper()
            unit = observation.canonical.unit(unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            return ConditionResult(
                improvement in unit.valid_improvements,
                (
                    f"unit {unit_id} cannot build {improvement}; "
                    f"valid={list(unit.valid_improvements)}"
                ),
            )
        if kind == "unit_type_contains":
            unit_id = str(condition["unit_id"])
            marker = str(condition["marker"]).upper()
            unit = observation.canonical.unit(unit_id)
            actual = "" if unit is None else unit.unit_type
            return ConditionResult(
                marker in actual,
                f"unit {unit_id} type {actual!r} does not contain {marker!r}",
            )
        if kind == "unit_moved_from":
            unit_id = str(condition["unit_id"])
            unit = observation.canonical.unit(unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            if unit.x is None or unit.y is None:
                return _unknown("unit position")
            original = (int(condition["x"]), int(condition["y"]))
            current = (unit.x, unit.y)
            return ConditionResult(
                current != original,
                f"unit {unit_id} did not move from {original}; current={current}",
            )
        if kind == "unit_absent":
            unit_id = str(condition["unit_id"])
            absent = observation.canonical.unit(unit_id) is None
            return ConditionResult(absent, f"unit {unit_id} still exists")
        if kind == "city_count_at_least":
            expected = int(condition["count"])
            actual = len(observation.canonical.cities)
            return ConditionResult(
                actual >= expected,
                f"city count {actual} is below required {expected}",
            )
        if kind == "tile_unoccupied":
            expected = (int(condition["x"]), int(condition["y"]))
            occupied = any(
                (row.get("x"), row.get("y")) == expected
                for row in _rows(normalized_snapshot.cities)
            )
            return ConditionResult(
                not occupied,
                f"settlement target {expected} is already occupied",
            )
        if kind in {"settler_target_legal", "settler_path_reachable"}:
            target = self._settler_target_fact(condition, decision_projection)
            fact = "legal" if kind == "settler_target_legal" else "reachable"
            if target is None or target.get(fact) is None:
                return ConditionResult(
                    False,
                    f"condition evidence unavailable: settler target {fact}",
                    known=False,
                )
            value = bool(target[fact])
            return ConditionResult(
                value,
                f"settlement target {(condition['x'], condition['y'])} is not {fact}",
            )
        if kind == "approved_target_equals":
            expected = (int(condition["x"]), int(condition["y"]))
            actual = self._projection_target(decision_projection)
            if actual is None:
                return ConditionResult(
                    False,
                    "condition evidence unavailable: approved target",
                    known=False,
                )
            return ConditionResult(
                actual == expected,
                f"approved target expected {expected}, got {actual}",
            )
        if kind == "severe_threat_absent":
            if decision_projection is None or "major_threat" not in decision_projection:
                return ConditionResult(
                    False,
                    "condition evidence unavailable: major threat",
                    known=False,
                )
            present = bool(decision_projection["major_threat"])
            return ConditionResult(not present, "a severe settlement threat is present")
        if kind == "city_at_target":
            expected = (int(condition["x"]), int(condition["y"]))
            owner = condition.get("owner")
            matches = [
                city
                for city in observation.canonical.cities
                if (city.values.get("x"), city.values.get("y")) == expected
            ]
            if owner is not None:
                matches = [
                    city
                    for city in matches
                    if str(city.values.get("owner")) == str(owner)
                ]
            return ConditionResult(
                bool(matches),
                f"no matching city exists at approved target {expected}",
            )
        return ConditionResult(
            False, f"unsupported condition type: {kind!r}", known=False
        )

    @classmethod
    def _settler_target_fact(
        cls,
        condition: Mapping[str, Any],
        projection: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        if projection is None:
            return None
        expected = (int(condition["x"]), int(condition["y"]))
        fact = (
            "legal" if condition.get("type") == "settler_target_legal" else "reachable"
        )
        matching: Mapping[str, Any] | None = None
        for row in projection.get("candidate_targets", ()):
            if not isinstance(row, Mapping):
                continue
            if (row.get("x"), row.get("y")) != expected:
                continue
            matching = row
            value = row.get(
                fact,
                row.get("path_reachable" if fact == "reachable" else "target_legal"),
            )
            if (normalized := cls._boolean_fact_value(value)) is not None:
                return {fact: normalized}
            if fact == "reachable":
                status = cls._path_status_value(row.get("path_status"))
                if status is not None:
                    return {fact: status}

        path = projection.get("path")
        if not isinstance(path, Mapping):
            return matching
        path_x = path.get("x", path.get("target_x"))
        path_y = path.get("y", path.get("target_y"))
        if (path_x is None) != (path_y is None):
            return matching
        if path_x is not None and (int(path_x), int(path_y)) != expected:
            return matching
        if fact == "reachable":
            value = path.get("reachable", path.get("path_reachable"))
            if (normalized := cls._boolean_fact_value(value)) is not None:
                return {fact: normalized}
            status = cls._path_status_value(path.get("path_status"))
            if status is not None:
                return {fact: status}
        else:
            value = path.get("target_legal", path.get("route_legal"))
            if (normalized := cls._boolean_fact_value(value)) is not None:
                return {fact: normalized}
        return matching

    @staticmethod
    def _boolean_fact_value(value: Any) -> bool | None:
        if type(value) is bool:
            return value
        if type(value) is int and value in {0, 1}:
            return bool(value)
        if not isinstance(value, str):
            return None
        normalized = value.strip().upper()
        if normalized in {"TRUE", "YES", "VALID", "REACHABLE", "LEGAL"}:
            return True
        if normalized in {
            "FALSE",
            "NO",
            "INVALID",
            "UNREACHABLE",
            "ILLEGAL",
        }:
            return False
        return None

    @staticmethod
    def _path_status_value(value: Any) -> bool | None:
        if not isinstance(value, str):
            return None
        normalized = value.strip().upper().replace("-", "_").replace(" ", "_")
        if normalized in {"REACHABLE", "VALID", "OK", "SUCCESS", "PATH_FOUND"}:
            return True
        if normalized in {
            "UNREACHABLE",
            "BLOCKED",
            "INVALID",
            "NO_PATH",
            "PATH_NOT_FOUND",
        }:
            return False
        return None

    @staticmethod
    def _projection_target(
        projection: Mapping[str, Any] | None,
    ) -> tuple[int, int] | None:
        if projection is None:
            return None
        value = projection.get("plan_target")
        if isinstance(value, Mapping) and isinstance(value.get("target"), Mapping):
            value = value["target"]
        if not isinstance(value, Mapping):
            return None
        x = value.get("x", value.get("target_x"))
        y = value.get("y", value.get("target_y"))
        if x is None or y is None:
            return None
        return int(x), int(y)

    @staticmethod
    def _get_path(value: Any, path: str) -> Any:
        current = value
        for segment in path.split("."):
            if isinstance(current, dict):
                if segment not in current:
                    return None
                current = current[segment]
            elif isinstance(current, list) and segment.isdigit():
                index = int(segment)
                if index >= len(current):
                    return None
                current = current[index]
            else:
                return None
        return current


def _required_facts(condition: Mapping[str, Any]) -> tuple[str, ...]:
    kind = str(condition.get("type"))
    if kind == "entity_exists":
        return {
            "city": ("cities",),
            "unit": ("units",),
            "builder": ("units",),
            "research": ("available_research",),
            "civic": ("available_civics",),
        }.get(str(condition.get("entity_type")), ())
    if kind in {"field_equals", "field_in"}:
        root = str(condition.get("path", "")).split(".", 1)[0]
        return {
            "cities": ("cities",),
            "units": ("units",),
            "blockers": ("blockers",),
        }.get(root, ())
    return CONDITION_REQUIRED_FACTS.get(kind, ())


def _uses_field_projection(condition: Mapping[str, Any]) -> bool:
    if condition.get("type") in {"field_equals", "field_in"}:
        return True
    nested = condition.get("conditions")
    return isinstance(nested, (list, tuple)) and any(
        isinstance(item, Mapping) and _uses_field_projection(item) for item in nested
    )


def _unknown(fact: str) -> ConditionResult:
    return ConditionResult(
        valid=False,
        known=False,
        reason=f"condition evidence unavailable: {fact}",
    )


def extract_known_entities(
    observation: NormalizedRuntimeObservation | RuntimeSnapshot,
) -> dict[str, set[str]]:
    """Extract IDs from an already-normalized observation or projection."""

    snapshot = (
        observation.snapshot
        if isinstance(observation, NormalizedRuntimeObservation)
        else observation
    )
    return {
        "city": _entity_ids(snapshot.cities, ("city_id", "id")),
        "research": _available_progress_types(snapshot, "available_techs", "tech_type"),
        "civic": _available_progress_types(snapshot, "available_civics", "civic_type"),
        "unit": _entity_ids(snapshot.units, ("unit_id", "id")),
        "builder": _entity_ids(
            snapshot.units,
            ("unit_id", "id"),
            predicate=lambda row: (
                "BUILDER"
                in str(
                    row.get("unit_type", row.get("type", row.get("name", "")))
                ).upper()
            ),
        ),
    }


def find_entity(
    value: Any, keys: tuple[str, ...], entity_id: str
) -> dict[str, Any] | None:
    rows = _rows(value)
    for row in rows:
        for key in keys:
            if row.get(key) is not None and str(row[key]) == entity_id:
                return row
    return None


def _progress_dict(snapshot: RuntimeSnapshot) -> dict[str, Any]:
    return snapshot.tech_civics if isinstance(snapshot.tech_civics, dict) else {}


def _available_progress_types(
    snapshot: RuntimeSnapshot, list_key: str, type_key: str
) -> set[str]:
    value = _progress_dict(snapshot).get(list_key)
    if not isinstance(value, list):
        return set()
    return {
        str(row[type_key])
        for row in value
        if isinstance(row, dict) and row.get(type_key) is not None
    }


def _unit_moves(unit: dict[str, Any]) -> float:
    value = unit.get("moves_remaining", unit.get("moves", 0))
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _entity_ids(
    value: Any,
    keys: tuple[str, ...],
    predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> set[str]:
    result: set[str] = set()
    for row in _rows(value):
        if predicate is not None and not predicate(row):
            continue
        for key in keys:
            if row.get(key) is not None:
                result.add(str(row[key]))
                break
    return result


def _rows(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, dict):
        rows = value.get("items", value.get("cities", value.get("units", [])))
    else:
        rows = value
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]
