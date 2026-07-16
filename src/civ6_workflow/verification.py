from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .conditions import ConditionEvaluator
from .domain import ActionAttempt
from .domain.observations import SlotState
from .models import StoredTask
from .observation_normalization import NormalizedRuntimeObservation


class VerificationEvidence(StrEnum):
    POSITIVE_COMMIT_EVIDENCE = "POSITIVE_COMMIT_EVIDENCE"
    EXPLICIT_NON_COMMIT_EVIDENCE = "EXPLICIT_NON_COMMIT_EVIDENCE"
    INCONCLUSIVE = "INCONCLUSIVE"
    CONFLICTING_STATE = "CONFLICTING_STATE"
    IMPOSSIBLE_POSTCONDITION = "IMPOSSIBLE_POSTCONDITION"


@dataclass(frozen=True, slots=True)
class ActionVerificationDecision:
    evidence: VerificationEvidence
    reason: str


def evaluate_action_verification(
    attempt: ActionAttempt,
    task: StoredTask,
    observation: NormalizedRuntimeObservation,
    conditions: ConditionEvaluator,
) -> ActionVerificationDecision:
    """Evaluate typed, action-specific commit evidence from one observation."""

    if not attempt.postconditions:
        return ActionVerificationDecision(
            VerificationEvidence.IMPOSSIBLE_POSTCONDITION,
            "action attempt has no versioned postcondition",
        )

    postconditions = conditions.evaluate_all(list(attempt.postconditions), observation)
    if postconditions.valid:
        return ActionVerificationDecision(
            VerificationEvidence.POSITIVE_COMMIT_EVIDENCE,
            "all versioned postconditions are satisfied",
        )
    if postconditions.reason.startswith("unsupported condition type"):
        return ActionVerificationDecision(
            VerificationEvidence.IMPOSSIBLE_POSTCONDITION,
            postconditions.reason,
        )

    action_type = attempt.action_type or task.action_type
    if action_type == "city_set_production":
        city_id = attempt.normalized_arguments.get("city_id", task.entity_id)
        city = observation.canonical.city(str(city_id))
        if city is None:
            return ActionVerificationDecision(
                VerificationEvidence.CONFLICTING_STATE,
                f"city {city_id} is absent while verifying production",
            )
        if city.production.state is SlotState.EMPTY:
            return ActionVerificationDecision(
                VerificationEvidence.EXPLICIT_NON_COMMIT_EVIDENCE,
                f"city {city_id} production slot remains empty",
            )
        return ActionVerificationDecision(
            VerificationEvidence.CONFLICTING_STATE,
            f"city {city_id} is producing a different item: {city.production.value}",
        )

    if action_type in {"set_research", "set_civic"}:
        slot = (
            observation.canonical.progression.current_research
            if action_type == "set_research"
            else observation.canonical.progression.current_civic
        )
        if slot.state is SlotState.EMPTY:
            return ActionVerificationDecision(
                VerificationEvidence.EXPLICIT_NON_COMMIT_EVIDENCE,
                f"{action_type} slot remains empty",
            )
        return ActionVerificationDecision(
            VerificationEvidence.CONFLICTING_STATE,
            f"{action_type} slot contains a different selection: {slot.value}",
        )

    if action_type == "unit_move":
        unit_id = attempt.normalized_arguments.get("unit_id", task.entity_id)
        if observation.canonical.units is None:
            return ActionVerificationDecision(
                VerificationEvidence.INCONCLUSIVE,
                f"unit {unit_id} details were not loaded",
            )
        unit = observation.canonical.unit(str(unit_id))
        if unit is None:
            return ActionVerificationDecision(
                VerificationEvidence.CONFLICTING_STATE,
                f"unit {unit_id} is absent while verifying movement",
            )
        x = unit.values.get("x")
        y = unit.values.get("y")
        return ActionVerificationDecision(
            VerificationEvidence.CONFLICTING_STATE,
            f"unit {unit_id} is at {(x, y)}, not the requested destination",
        )

    return ActionVerificationDecision(
        VerificationEvidence.INCONCLUSIVE,
        postconditions.reason or f"{action_type} has no conclusive evidence",
    )
