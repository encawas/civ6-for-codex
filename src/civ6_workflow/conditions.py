from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .domain.observations import SlotState
from .models import RuntimeSnapshot
from .observation_normalization import (
    NormalizedRuntimeObservation,
    normalize_runtime_snapshot,
)


ObservationInput = RuntimeSnapshot | NormalizedRuntimeObservation


@dataclass(slots=True)
class ConditionResult:
    valid: bool
    reason: str = ""


class ConditionEvaluator:
    """Evaluates a deliberately small, auditable condition language."""

    def evaluate_all(
        self, conditions: list[dict[str, Any]], snapshot: ObservationInput
    ) -> ConditionResult:
        observation = self._normalize(snapshot)
        for condition in conditions:
            result = self._evaluate_normalized(condition, observation)
            if not result.valid:
                return result
        return ConditionResult(True)

    def evaluate(
        self, condition: dict[str, Any], snapshot: ObservationInput
    ) -> ConditionResult:
        return self._evaluate_normalized(condition, self._normalize(snapshot))

    @staticmethod
    def _normalize(snapshot: ObservationInput) -> NormalizedRuntimeObservation:
        if isinstance(snapshot, NormalizedRuntimeObservation):
            return snapshot
        return normalize_runtime_snapshot(snapshot)

    def _evaluate_normalized(
        self,
        condition: dict[str, Any],
        observation: NormalizedRuntimeObservation,
    ) -> ConditionResult:
        normalized_snapshot = observation.snapshot
        kind = condition.get("type")
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
        if kind == "field_equals":
            path = str(condition["path"])
            expected = condition.get("value")
            actual = self._get_path(normalized_snapshot.model_dump(mode="json"), path)
            return ConditionResult(
                actual == expected,
                f"field {path} expected {expected!r}, got {actual!r}",
            )
        if kind == "field_in":
            path = str(condition["path"])
            allowed = condition.get("values", [])
            actual = self._get_path(normalized_snapshot.model_dump(mode="json"), path)
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
            empty = city.production.state is SlotState.EMPTY
            return ConditionResult(
                empty,
                f"city {city_id} is already producing {city.production.value!r}",
            )
        if kind == "research_unselected":
            slot = observation.canonical.progression.current_research
            return ConditionResult(
                slot.state is SlotState.EMPTY,
                f"research is already selected: {slot.value!r}",
            )
        if kind == "civic_unselected":
            slot = observation.canonical.progression.current_civic
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
            unit = find_entity(normalized_snapshot.units, ("unit_id", "id"), unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            expected_x = int(condition["x"])
            expected_y = int(condition["y"])
            actual = (unit.get("x"), unit.get("y"))
            return ConditionResult(
                actual == (expected_x, expected_y),
                f"unit {unit_id} expected at {(expected_x, expected_y)}, got {actual}",
            )
        if kind in {"unit_has_moves", "unit_no_moves"}:
            unit_id = str(condition["unit_id"])
            unit = find_entity(normalized_snapshot.units, ("unit_id", "id"), unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            moves = _unit_moves(unit)
            if kind == "unit_has_moves":
                return ConditionResult(
                    moves > 0,
                    f"unit {unit_id} has no moves remaining ({moves})",
                )
            return ConditionResult(
                moves <= 0,
                f"unit {unit_id} still has {moves} moves remaining",
            )
        if kind == "unit_has_build_charge":
            unit_id = str(condition["unit_id"])
            unit = find_entity(normalized_snapshot.units, ("unit_id", "id"), unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            charges = int(unit.get("build_charges", 0) or 0)
            return ConditionResult(charges > 0, f"unit {unit_id} has no build charges")
        if kind == "unit_build_charges_equals":
            unit_id = str(condition["unit_id"])
            expected = int(condition["charges"])
            unit = find_entity(normalized_snapshot.units, ("unit_id", "id"), unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            actual = int(unit.get("build_charges", 0) or 0)
            return ConditionResult(
                actual == expected,
                f"unit {unit_id} charges expected {expected}, got {actual}",
            )
        if kind == "unit_can_improve":
            unit_id = str(condition["unit_id"])
            improvement = str(condition["improvement_type"])
            unit = find_entity(normalized_snapshot.units, ("unit_id", "id"), unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            valid = unit.get("valid_improvements", []) or []
            return ConditionResult(
                improvement in valid,
                f"unit {unit_id} cannot build {improvement}; valid={valid}",
            )
        return ConditionResult(False, f"unsupported condition type: {kind!r}")

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
