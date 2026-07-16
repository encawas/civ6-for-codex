"""Deterministic decision-gap identity, projection, and lease policy."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .conditions import ConditionEvaluator, find_entity
from .domain import (
    DECISION_INPUT_PROJECTION_VERSION,
    ApprovalStatus,
    ContinuationPolicy,
    DecisionGap,
    DecisionGapStatus,
    DecisionGroup,
    DecisionRoute,
    LeaseValidationResult,
    PlanLease,
    PlanLeaseStatus,
    SubjectRef,
)
from .models import GameEvent, RuntimeSnapshot
from .observation_normalization import NormalizedRuntimeObservation


DECISION_POLICY_REVISION = "planner-call-policy/v1"

STRATEGIC_GAP_TYPES = frozenset(
    {
        "opening_strategy_required",
        "settler_site_selection_required",
        "settler_plan_requires_review",
        "research_direction_required",
        "civic_direction_required",
        "research_unavailable",
        "civic_unavailable",
        "invalid_city_plan_item",
        "city_role_required",
        "district_placement_required",
        "war_posture_required",
        "emergency_defense_required",
        "pending_diplomacy",
        "pending_trade_offer",
        "world_congress_vote_required",
        "tactical_attack_opportunity",
        "emergency_response_window",
    }
)

TURN_SPECIFIC_GAP_TYPES = frozenset(
    {
        "tactical_attack_opportunity",
        "world_congress_vote_required",
        "emergency_response_window",
    }
)

ROUTINE_EVENT_TYPES = frozenset(
    {
        "city_no_production",
        "research_selection_required",
        "civic_selection_required",
        "unit_orders_required",
        "special_unit_orders_required",
        "end_turn_blocker",
        "action_required_notification",
        "planned_task_blocked",
        "planned_task_failed",
    }
)

_OVERVIEW_KEYS = (
    "player_id",
    "civ_name",
    "leader_name",
    "gold",
    "gold_per_turn",
    "science_yield",
    "culture_yield",
    "faith",
    "current_research",
    "current_civic",
    "num_cities",
    "num_units",
    "score",
    "era_name",
    "era_score",
    "game_speed",
    "at_war",
    "military_strength",
    "threat_level",
)
_CITY_KEYS = (
    "city_id",
    "id",
    "name",
    "owner",
    "population",
    "x",
    "y",
    "currently_building",
    "available_production",
    "districts",
)
_UNIT_KEYS = (
    "unit_id",
    "id",
    "unit_type",
    "type",
    "x",
    "y",
    "moves_remaining",
    "build_charges",
    "needs_promotion",
    "valid_improvements",
    "targets",
)
_PAYLOAD_KEYS = (
    "blocking_type",
    "offer_id",
    "expires_turn",
    "vote_id",
    "emergency_id",
    "plan_id",
    "plan_revision",
    "strategy_revision",
    "target",
    "targets",
    "available_targets",
    "threat_level",
    "other_player_id",
)


@dataclass(frozen=True, slots=True)
class LeaseEvaluation:
    result: LeaseValidationResult
    lease: PlanLease
    reason: str


@dataclass(frozen=True, slots=True)
class PlannerEligibility:
    eligible: bool
    reason: str
    gaps: tuple[DecisionGap, ...] = ()


def stable_decision_identity(event: GameEvent) -> tuple[str, bool]:
    """Return semantic identity and whether the event explicitly expires by turn."""

    if event.event_type in ROUTINE_EVENT_TYPES:
        raise ValueError(f"routine event {event.event_type!r} is not a decision gap")
    if event.event_type not in STRATEGIC_GAP_TYPES:
        raise ValueError(f"event {event.event_type!r} has no strategic gap policy")

    entity = (
        "empire"
        if event.entity_id is None
        else f"{event.entity_type or 'entity'}-{event.entity_id}"
    )
    parts = [event.event_type.replace("_", "-"), entity]

    if event.event_type in {
        "settler_plan_requires_review",
        "invalid_city_plan_item",
    }:
        revision = event.payload.get("plan_revision", event.payload.get("plan_id"))
        if revision is not None:
            parts.append(f"plan-{revision}")
    elif event.event_type == "pending_trade_offer":
        offer_id = event.payload.get("offer_id")
        if offer_id is None:
            raise ValueError("trade-offer decisions require a stable upstream offer_id")
        parts.append(f"offer-{offer_id}")
    elif event.event_type == "world_congress_vote_required":
        vote_id = event.payload.get("vote_id")
        if vote_id is None:
            raise ValueError("World Congress decisions require a stable vote_id")
        parts.append(f"vote-{vote_id}")

    turn_specific = event.event_type in TURN_SPECIFIC_GAP_TYPES
    if turn_specific:
        parts.append(f"turn-{event.turn}")
    return ":".join(str(part) for part in parts), turn_specific


def build_decision_input_projection(
    snapshot: RuntimeSnapshot,
    event: GameEvent,
    context: dict[str, Any],
    *,
    policy_revision: str = DECISION_POLICY_REVISION,
) -> dict[str, Any]:
    """Project only material, explicitly versioned facts for one strategic gap."""

    identity, turn_specific = stable_decision_identity(event)
    projection: dict[str, Any] = {
        "projection_version": DECISION_INPUT_PROJECTION_VERSION,
        "policy_revision": policy_revision,
        "stable_identity": identity,
        "gap_type": event.event_type,
        "scope": _scope_for_event(event),
        "subject": {
            "type": event.entity_type,
            "id": None if event.entity_id is None else str(event.entity_id),
        },
        "overview": _pick(snapshot.overview, _OVERVIEW_KEYS),
        "event_facts": _pick(event.payload, _PAYLOAD_KEYS),
        "strategy": _compact_strategy(context.get("strategy")),
        "plan_revisions": _relevant_plan_revisions(event, context),
        "approval": {
            "execution_mode": context.get("execution_mode"),
            "required": bool(event.risk.value in {"high", "critical"}),
        },
    }
    if turn_specific:
        projection["identity_turn_number"] = event.turn

    if event.entity_type == "city" and event.entity_id is not None:
        row = find_entity(snapshot.cities, ("city_id", "id"), str(event.entity_id))
        projection["city"] = _pick(row, _CITY_KEYS)
    elif event.entity_type in {"unit", "builder"} and event.entity_id is not None:
        row = find_entity(snapshot.units, ("unit_id", "id"), str(event.entity_id))
        projection["unit"] = _pick(row, _UNIT_KEYS)

    if event.event_type in {
        "research_direction_required",
        "research_unavailable",
        "civic_direction_required",
        "civic_unavailable",
    }:
        projection["progression"] = _compact_progression(snapshot.tech_civics)
    if event.event_type in {"pending_diplomacy", "war_posture_required"}:
        projection["diplomacy"] = _bounded_rows(snapshot.diplomacy, 12)
    if event.event_type == "pending_trade_offer":
        projection["trades"] = _matching_offer(
            snapshot.trades, event.payload.get("offer_id")
        )
    return projection


def hash_decision_input(projection: dict[str, Any]) -> str:
    encoded = json.dumps(
        projection,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_decision_gap(
    game_id: str,
    observation_id: str,
    snapshot: RuntimeSnapshot,
    event: GameEvent,
    context: dict[str, Any],
    *,
    existing: DecisionGap | None = None,
    now: datetime | None = None,
) -> DecisionGap:
    identity, turn_specific = stable_decision_identity(event)
    projection = build_decision_input_projection(snapshot, event, context)
    relevant_hash = hash_decision_input(projection)
    timestamp = now or datetime.now(UTC)
    gap_id = f"gap_{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:24]}"
    changed = existing is not None and existing.relevant_input_hash != relevant_hash
    return DecisionGap(
        decision_gap_id=gap_id,
        game_session_id=game_id,
        stable_identity=identity,
        source_event_ids=tuple(
            dict.fromkeys(
                (
                    *(existing.source_event_ids if existing else ()),
                    event.event_id,
                )
            )
        ),
        gap_type=event.event_type,
        scope=_scope_for_event(event),
        subjects=(
            ()
            if event.entity_id is None
            else (
                SubjectRef(
                    subject_type=event.entity_type or "entity",
                    subject_id=str(event.entity_id),
                ),
            )
        ),
        observation_id=observation_id,
        first_observation_id=(
            observation_id
            if existing is None
            else existing.first_observation_id or existing.observation_id
        ),
        relevant_input_hash=relevant_hash,
        input_projection=projection,
        strategy_revision=str(projection.get("strategy", {}).get("revision", "none")),
        relevant_plan_revisions=tuple(
            str(value) for value in projection.get("plan_revisions", {}).values()
        ),
        required_context=(),
        route=DecisionRoute.PLANNER,
        status=DecisionGapStatus.OPEN,
        cooldown_key=identity,
        logical_request_id=None,
        reopen_reason="material input changed" if changed else None,
        turn_specific=turn_specific,
        identity_turn_number=event.turn if turn_specific else None,
        created_at=existing.created_at if existing else timestamp,
        updated_at=timestamp,
    )


def batch_compatible_gaps(
    game_id: str,
    observation_id: str,
    gaps: list[DecisionGap],
    *,
    now: datetime | None = None,
) -> DecisionGroup:
    if not gaps:
        raise ValueError("a decision group requires at least one gap")
    if any(gap.game_session_id != game_id for gap in gaps):
        raise ValueError("decision group gaps must belong to one game")
    if any(gap.observation_id != observation_id for gap in gaps):
        raise ValueError("decision group gaps must share one observation")
    scopes = {gap.scope.split(":", 1)[0] for gap in gaps}
    incompatible = {"system", "verification", "approval"} & scopes
    if incompatible:
        raise ValueError(f"non-strategic scopes cannot be batched: {incompatible}")
    ordered = tuple(sorted(gap.decision_gap_id for gap in gaps))
    combined = {
        "projection_version": DECISION_INPUT_PROJECTION_VERSION,
        "gaps": [
            {
                "decision_gap_id": gap.decision_gap_id,
                "stable_identity": gap.stable_identity,
                "input_hash": gap.relevant_input_hash,
            }
            for gap in sorted(gaps, key=lambda item: item.decision_gap_id)
        ],
    }
    group_hash = hash_decision_input(combined)
    identity = "|".join(ordered)
    return DecisionGroup(
        decision_group_id=(
            f"group_{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:24]}"
        ),
        game_session_id=game_id,
        observation_id=observation_id,
        decision_gap_ids=ordered,
        input_projection_hash=group_hash,
        created_at=now or datetime.now(UTC),
    )


def evaluate_plan_lease(
    lease: PlanLease,
    observation: NormalizedRuntimeObservation,
    *,
    relevant_input_hash: str,
    evaluator: ConditionEvaluator | None = None,
) -> LeaseEvaluation:
    evaluator = evaluator or ConditionEvaluator()
    turn = observation.snapshot.turn

    if lease.status is not PlanLeaseStatus.ACTIVE:
        result = {
            PlanLeaseStatus.COMPLETED: LeaseValidationResult.VALID,
            PlanLeaseStatus.EXPIRED: LeaseValidationResult.EXPIRED,
            PlanLeaseStatus.INVALIDATED: LeaseValidationResult.INVALIDATED,
            PlanLeaseStatus.AWAITING_INFORMATION: LeaseValidationResult.UNKNOWN,
        }[lease.status]
        return LeaseEvaluation(result, lease, f"lease is already {lease.status.value}")

    if lease.completion_condition is not None:
        condition = _condition_payload(lease.completion_condition)
        outcome = evaluator.evaluate(condition, observation)
        if outcome.valid:
            updated = lease.model_copy(
                update={
                    "status": PlanLeaseStatus.COMPLETED,
                    "last_validated_observation_id": _observation_marker(observation),
                    "last_validation_result": LeaseValidationResult.VALID,
                }
            )
            return LeaseEvaluation(
                LeaseValidationResult.VALID,
                updated,
                "completion condition is satisfied",
            )

    for condition in lease.invalidation_conditions:
        outcome = evaluator.evaluate(_condition_payload(condition), observation)
        if outcome.valid:
            reason = f"invalidation condition matched: {condition.condition_type}"
            updated = lease.model_copy(
                update={
                    "status": PlanLeaseStatus.INVALIDATED,
                    "last_validated_observation_id": _observation_marker(observation),
                    "last_validation_result": LeaseValidationResult.INVALIDATED,
                    "invalidation_reason": reason,
                }
            )
            return LeaseEvaluation(LeaseValidationResult.INVALIDATED, updated, reason)
        if outcome.reason.startswith("unsupported condition type"):
            updated = lease.model_copy(
                update={
                    "status": PlanLeaseStatus.AWAITING_INFORMATION,
                    "last_validated_observation_id": _observation_marker(observation),
                    "last_validation_result": LeaseValidationResult.UNKNOWN,
                }
            )
            return LeaseEvaluation(
                LeaseValidationResult.UNKNOWN,
                updated,
                outcome.reason,
            )

    if lease.valid_until_turn is not None and turn > lease.valid_until_turn:
        unchanged = lease.relevant_input_hash == relevant_input_hash
        if (
            unchanged
            and lease.continuation_policy
            is ContinuationPolicy.EXTEND_WHEN_INPUT_UNCHANGED
        ):
            updated = lease.model_copy(
                update={
                    "valid_until_turn": turn + 1,
                    "last_validated_observation_id": _observation_marker(observation),
                    "last_validation_result": LeaseValidationResult.VALID,
                }
            )
            return LeaseEvaluation(
                LeaseValidationResult.VALID,
                updated,
                "review boundary reached with unchanged relevant input",
            )
        updated = lease.model_copy(
            update={
                "status": PlanLeaseStatus.EXPIRED,
                "last_validated_observation_id": _observation_marker(observation),
                "last_validation_result": LeaseValidationResult.EXPIRED,
            }
        )
        return LeaseEvaluation(
            LeaseValidationResult.EXPIRED, updated, "lease horizon expired"
        )

    updated = lease.model_copy(
        update={
            "last_validated_observation_id": _observation_marker(observation),
            "last_validation_result": LeaseValidationResult.VALID,
        }
    )
    return LeaseEvaluation(LeaseValidationResult.VALID, updated, "lease remains valid")


def evaluate_planner_eligibility(
    gaps: list[DecisionGap],
    leases: list[PlanLease],
    *,
    runtime_state: str,
    has_ready_deterministic_task: bool,
    active_attempt: bool,
    logical_requests_this_turn: int,
    active_logical_request: bool,
    max_logical_requests_per_turn: int = 1,
) -> PlannerEligibility:
    forbidden_states = {
        "VERIFYING",
        "AWAITING_APPROVAL",
        "AWAITING_HUMAN",
        "TURN_TRANSITIONING",
        "SYSTEM_ERROR",
        "PLANNER_BACKOFF",
    }
    if runtime_state in forbidden_states or active_attempt:
        return PlannerEligibility(False, "runtime state forbids planner calls")
    if has_ready_deterministic_task:
        return PlannerEligibility(False, "deterministic work has priority")
    if active_logical_request:
        return PlannerEligibility(False, "a logical planner request is already active")
    if logical_requests_this_turn >= max_logical_requests_per_turn:
        return PlannerEligibility(False, "logical planner request budget exhausted")

    covered = {
        gap_id
        for lease in leases
        if lease.status is PlanLeaseStatus.ACTIVE
        and lease.approval_status
        in {ApprovalStatus.NOT_REQUIRED, ApprovalStatus.APPROVED}
        for gap_id in lease.decision_gap_ids
    }
    eligible = tuple(
        gap
        for gap in gaps
        if gap.status
        in {
            DecisionGapStatus.OPEN,
            DecisionGapStatus.PLANNER_ELIGIBLE,
        }
        and gap.route is DecisionRoute.PLANNER
        and gap.decision_gap_id not in covered
    )
    if not eligible:
        return PlannerEligibility(False, "no uncovered planner-routed decision gap")
    return PlannerEligibility(
        True, "strategic decision gaps require planning", eligible
    )


def _condition_payload(condition: Any) -> dict[str, Any]:
    payload = {"type": condition.condition_type, **dict(condition.parameters)}
    if condition.subject is not None:
        payload.setdefault("entity_type", condition.subject.subject_type)
        payload.setdefault("entity_id", condition.subject.subject_id)
    if condition.expected is not True:
        payload.setdefault("value", condition.expected)
    return payload


def _scope_for_event(event: GameEvent) -> str:
    if event.entity_type is None or event.entity_id is None:
        return "empire"
    return f"{event.entity_type}:{event.entity_id}"


def _pick(value: Any, keys: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {key: value[key] for key in keys if key in value}


def _compact_strategy(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"revision": "none"}
    revision = value.get(
        "revision",
        value.get("_revision", value.get("_plan_id", value.get("plan_id", "none"))),
    )
    result = {"revision": str(revision)}
    for key in (
        "victory_focus",
        "opening",
        "stage",
        "research_queue",
        "civic_queue",
        "expansion_target",
        "military_posture",
    ):
        if key in value:
            result[key] = value[key]
    return result


def _relevant_plan_revisions(
    event: GameEvent, context: dict[str, Any]
) -> dict[str, str]:
    result: dict[str, str] = {}
    if event.entity_type == "city":
        plans = context.get("cities", {})
    elif event.entity_type in {"unit", "builder"}:
        plans = context.get("units", {})
    else:
        plans = {}
    if isinstance(plans, dict) and event.entity_id is not None:
        plan = plans.get(str(event.entity_id), plans.get(event.entity_id))
        if isinstance(plan, dict):
            revision = plan.get(
                "revision", plan.get("_revision", plan.get("_plan_id", "none"))
            )
            result[_scope_for_event(event)] = str(revision)
    return result


def _compact_progression(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return _pick(
        value,
        (
            "current_research",
            "current_tech",
            "research",
            "available_techs",
            "current_civic",
            "civic",
            "available_civics",
        ),
    )


def _bounded_rows(value: Any, limit: int) -> Any:
    if isinstance(value, dict):
        rows = value.get("items", value)
        if isinstance(rows, list):
            return rows[:limit]
        return _pick(value, tuple(sorted(value)[:limit]))
    if isinstance(value, list):
        return value[:limit]
    return {}


def _matching_offer(value: Any, offer_id: Any) -> Any:
    rows = value
    if isinstance(value, dict):
        rows = value.get("items", value.get("offers", []))
    if not isinstance(rows, list):
        return {}
    for row in rows:
        if isinstance(row, dict) and str(row.get("offer_id")) == str(offer_id):
            return row
    return {}


def _observation_marker(observation: NormalizedRuntimeObservation) -> str:
    payload = {
        "turn": observation.snapshot.turn,
        "game_id": observation.snapshot.game_id,
        "normalization_version": observation.canonical.normalization_version,
    }
    return f"obs_{hash_decision_input(payload)[:24]}"
