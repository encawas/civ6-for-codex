from __future__ import annotations

from typing import Any

from .conditions import (
    ConditionEvaluator as BaseConditionEvaluator,
    ConditionResult,
    find_entity,
)
from .observation_normalization import NormalizedRuntimeObservation


class WorkflowConditionEvaluator(BaseConditionEvaluator):
    """Condition extensions required by irreversible unit operations."""

    def _evaluate_normalized(
        self,
        condition: dict[str, Any],
        observation: NormalizedRuntimeObservation,
        *,
        decision_projection: dict[str, Any] | None = None,
    ) -> ConditionResult:
        snapshot = observation.snapshot
        kind = condition.get("type")
        if kind == "unit_absent":
            unit_id = str(condition["unit_id"])
            unit = find_entity(snapshot.units, ("unit_id", "id"), unit_id)
            return ConditionResult(
                unit is None,
                f"unit {unit_id} still exists after an operation that should consume it",
            )
        if kind == "unit_moved_from":
            unit_id = str(condition["unit_id"])
            unit = find_entity(snapshot.units, ("unit_id", "id"), unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            original = (int(condition["x"]), int(condition["y"]))
            current = (unit.get("x"), unit.get("y"))
            return ConditionResult(
                current != original,
                f"unit {unit_id} did not move from {original}; current={current}",
            )
        if kind == "unit_type_contains":
            unit_id = str(condition["unit_id"])
            marker = str(condition["marker"]).upper()
            unit = find_entity(snapshot.units, ("unit_id", "id"), unit_id)
            if unit is None:
                return ConditionResult(False, f"unit {unit_id} does not exist")
            unit_type = str(
                unit.get("unit_type", unit.get("type", unit.get("name", "")))
            ).upper()
            return ConditionResult(
                marker in unit_type,
                f"unit {unit_id} type {unit_type!r} does not contain {marker!r}",
            )
        if kind == "city_count_at_least":
            expected = int(condition["count"])
            actual = len(_rows(snapshot.cities))
            return ConditionResult(
                actual >= expected,
                f"city count expected at least {expected}, got {actual}",
            )
        return super()._evaluate_normalized(
            condition,
            observation,
            decision_projection=decision_projection,
        )


def _rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        value = value.get("items", value.get("cities", []))
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
