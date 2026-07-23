from __future__ import annotations

import math

from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import Field, field_validator, model_validator

from .decisioning import SETTLER_GAP_TYPES, STRATEGIC_GAP_TYPES
from .domain import ContinuationPolicy

from .models import (
    ExecutionMode,
    RiskLevel,
    AgentRequest as BaseAgentRequest,
    PlanBundle as BasePlanBundle,
    StrictModel,
    TickMetrics as BaseTickMetrics,
)

_LEASE_CONDITION_FIELDS: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "all_of": (frozenset({"conditions"}), frozenset()),
    "approved_target_equals": (frozenset({"x", "y"}), frozenset()),
    "city_at_target": (frozenset({"x", "y"}), frozenset({"owner"})),
    "city_count_at_least": (frozenset({"count"}), frozenset()),
    "entity_exists": (
        frozenset({"entity_type", "entity_id"}),
        frozenset(),
    ),
    "field_in": (frozenset({"path", "values"}), frozenset()),
    "settler_path_reachable": (frozenset({"x", "y"}), frozenset()),
    "settler_target_legal": (frozenset({"x", "y"}), frozenset()),
    "severe_threat_absent": (frozenset(), frozenset()),
    "tile_unoccupied": (frozenset({"x", "y"}), frozenset()),
    "turn_at_least": (frozenset({"turn"}), frozenset()),
    "turn_equals": (frozenset({"turn"}), frozenset()),
    "unit_absent": (frozenset({"unit_id"}), frozenset()),
    "unit_type_contains": (
        frozenset({"unit_id", "marker"}),
        frozenset(),
    ),
}
LEASE_CONDITION_TYPES = frozenset(_LEASE_CONDITION_FIELDS)
_COORDINATE_CONDITION_TYPES = frozenset(
    {
        "approved_target_equals",
        "city_at_target",
        "settler_path_reachable",
        "settler_target_legal",
        "tile_unoccupied",
    }
)


def _strict_non_negative_int(
    condition: dict[str, Any],
    field_name: str,
    *,
    maximum: int,
) -> int:
    value = condition[field_name]
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError(
            f"lease condition {field_name} must be an integer from 0 to {maximum}"
        )
    return value


def _strict_identifier(condition: dict[str, Any], field_name: str) -> None:
    value = condition[field_name]
    if type(value) is int:
        if value < 0:
            raise ValueError(f"lease condition {field_name} must not be negative")
        return
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"lease condition {field_name} must be a non-empty string or integer"
        )


def _strict_string(condition: dict[str, Any], field_name: str) -> None:
    value = condition[field_name]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"lease condition {field_name} must be a non-empty string")


def _validate_lease_condition(condition: dict[str, Any]) -> None:
    if not isinstance(condition, dict):
        raise ValueError("lease conditions must be objects")
    condition_type = condition.get("type")
    if (
        not isinstance(condition_type, str)
        or condition_type not in LEASE_CONDITION_TYPES
    ):
        raise ValueError(f"unsupported lease condition type: {condition_type}")

    required, optional = _LEASE_CONDITION_FIELDS[condition_type]
    supplied = set(condition) - {"type"}
    missing = required - supplied
    if missing:
        raise ValueError(
            f"lease condition {condition_type} missing fields: {sorted(missing)}"
        )
    unknown = supplied - required - optional
    if unknown:
        raise ValueError(
            f"lease condition {condition_type} has unknown fields: {sorted(unknown)}"
        )

    if condition_type == "all_of":
        nested = condition["conditions"]
        if not isinstance(nested, list) or not nested:
            raise ValueError(
                "all_of lease condition requires a non-empty conditions list"
            )
        for item in nested:
            _validate_lease_condition(item)
        return

    if condition_type in _COORDINATE_CONDITION_TYPES:
        _strict_non_negative_int(condition, "x", maximum=9999)
        _strict_non_negative_int(condition, "y", maximum=9999)
    if condition_type in {"turn_at_least", "turn_equals"}:
        _strict_non_negative_int(condition, "turn", maximum=2_147_483_647)
    if condition_type == "city_count_at_least":
        _strict_non_negative_int(condition, "count", maximum=1_000_000)
    if condition_type == "entity_exists":
        _strict_string(condition, "entity_type")
        _strict_identifier(condition, "entity_id")
    if condition_type in {"unit_absent", "unit_type_contains"}:
        _strict_identifier(condition, "unit_id")
    if condition_type == "unit_type_contains":
        _strict_string(condition, "marker")
    if condition_type == "field_in":
        _strict_string(condition, "path")
        values = condition["values"]
        if not isinstance(values, list) or not values:
            raise ValueError("lease condition values must be a non-empty list")
        if any(
            type(value) not in {str, int, float, bool}
            or (type(value) is float and not math.isfinite(value))
            for value in values
        ):
            raise ValueError("lease condition values must contain JSON scalar values")
    if condition_type == "city_at_target" and "owner" in condition:
        _strict_identifier(condition, "owner")


class ResolutionDisposition(str, Enum):
    TASK = "task"
    PLAN_UPDATE = "plan_update"
    HUMAN_REVIEW = "human_review"
    INFORMATION_REQUIRED = "information_required"
    DEFERRED = "deferred"


class InformationRequest(StrictModel):
    request_id: str = Field(default_factory=lambda: f"info_{uuid4().hex}")
    event_dedupe_key: str
    query_type: str
    tool_name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    purpose: str = Field(min_length=1, max_length=500)

    @field_validator("event_dedupe_key", "query_type", "tool_name")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("value must not be blank")
        return value


class LeaseContract(StrictModel):
    """Planner-proposed durability contract; no implicit lease defaults."""

    valid_until_turn: int | None = Field(default=None, ge=0)
    preconditions: list[dict[str, Any]] = Field(min_length=1)
    completion_condition: dict[str, Any]
    invalidation_conditions: list[dict[str, Any]] = Field(min_length=1)
    continuation_conditions: list[dict[str, Any]] = Field(min_length=1)
    review_conditions: list[dict[str, Any]] = Field(min_length=1)
    continuation_policy: ContinuationPolicy
    approval_required: bool = False
    recommended_risk: RiskLevel = RiskLevel.LOW
    proposed_execution_mode: ExecutionMode | None = None
    covered_slots: list[str] = Field(default_factory=list)
    subjects: list[dict[str, str]] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_horizon(self):
        _validate_lease_condition(self.completion_condition)
        for name in (
            "preconditions",
            "invalidation_conditions",
            "continuation_conditions",
            "review_conditions",
        ):
            for item in getattr(self, name):
                _validate_lease_condition(item)
        for subject in self.subjects:
            if set(subject) != {"subject_type", "subject_id"}:
                raise ValueError("lease subjects require subject_type and subject_id")
        return self


class EventResolution(StrictModel):
    event_dedupe_key: str
    disposition: ResolutionDisposition
    task_ids: list[str] = Field(default_factory=list)
    plan_refs: list[str] = Field(default_factory=list)
    information_request_ids: list[str] = Field(default_factory=list)
    lease_contract: LeaseContract | None = None
    reason: str = Field(min_length=1, max_length=500)
    decision_gap_ids: list[str] = Field(default_factory=list, max_length=100)
    atomic: bool = False

    @field_validator("event_dedupe_key")
    @classmethod
    def _dedupe_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("event_dedupe_key must not be blank")
        return value


class WorkflowPlanBundle(BasePlanBundle):
    information_requests: list[InformationRequest] = Field(
        default_factory=list, max_length=8
    )
    event_resolutions: list[EventResolution] = Field(
        default_factory=list, max_length=100
    )


class WorkflowAgentRequest(BaseAgentRequest):
    information_results: dict[str, Any] = Field(default_factory=dict)


class WorkflowTickMetrics(BaseTickMetrics):
    agent_attempt_count: int = Field(default=0, ge=0)
    agent_success_count: int = Field(default=0, ge=0)
    information_query_count: int = Field(default=0, ge=0)
    logical_planner_request_count: int = Field(default=0, ge=0)
    provider_attempt_count: int = Field(default=0, ge=0)
    information_round_count: int = Field(default=0, ge=0)
    duplicate_request_suppression_count: int = Field(default=0, ge=0)
    planner_context_bytes: int = Field(default=0, ge=0)


@dataclass(frozen=True, slots=True)
class QuerySpec:
    required_arguments: frozenset[str] = field(default_factory=frozenset)
    optional_arguments: frozenset[str] = field(default_factory=frozenset)


READ_ONLY_QUERY_SPECS: dict[str, QuerySpec] = {
    "get_settle_advisor": QuerySpec(frozenset({"unit_id"})),
    "get_global_settle_advisor": QuerySpec(),
    "get_pathing_estimate": QuerySpec(frozenset({"unit_id", "target_x", "target_y"})),
    "get_unit_promotions": QuerySpec(frozenset({"unit_id"})),
    "get_district_advisor": QuerySpec(frozenset({"city_id", "district_type"})),
    "get_city_production": QuerySpec(frozenset({"city_id"})),
    "get_map_area": QuerySpec(
        frozenset({"center_x", "center_y"}), frozenset({"radius"})
    ),
    "get_policies": QuerySpec(),
    "get_trade_options": QuerySpec(frozenset({"other_player_id"})),
    "get_pantheon_beliefs": QuerySpec(),
    "get_religion_beliefs": QuerySpec(),
    "get_dedications": QuerySpec(),
    "get_city_states": QuerySpec(),
    "get_builder_tasks": QuerySpec(),
}


def information_tool_argument_contracts() -> dict[str, dict[str, list[str]]]:
    return {
        name: {
            "required": sorted(spec.required_arguments),
            "optional": sorted(spec.optional_arguments),
        }
        for name, spec in sorted(READ_ONLY_QUERY_SPECS.items())
    }


class WorkflowProtocolError(ValueError):
    pass


def validate_information_request(request: InformationRequest) -> None:
    spec = READ_ONLY_QUERY_SPECS.get(request.tool_name)
    if spec is None:
        raise WorkflowProtocolError(
            f"information request uses non-whitelisted tool: {request.tool_name}"
        )
    supplied = set(request.arguments)
    missing = set(spec.required_arguments) - supplied
    unknown = supplied - set(spec.required_arguments) - set(spec.optional_arguments)
    if missing:
        raise WorkflowProtocolError(
            f"information request {request.request_id} missing arguments: {sorted(missing)}"
        )
    if unknown:
        raise WorkflowProtocolError(
            f"information request {request.request_id} has unknown arguments: {sorted(unknown)}"
        )


def validate_global_resolution_structure(
    bundle: WorkflowPlanBundle,
    _trigger_events: list[Any],
    *,
    required_gap_ids: set[str] | None = None,
) -> None:
    """Validate ownership and atomic relationships before item partitioning."""

    errors: list[str] = []
    event_owners: dict[str, int] = {}
    gap_owners: dict[str, int] = {}
    task_owners: dict[str, int] = {}
    plan_owners: dict[str, int] = {}
    for index, resolution in enumerate(bundle.event_resolutions):
        key = resolution.event_dedupe_key
        if key in event_owners:
            errors.append(f"duplicate event resolution for {key}")
        event_owners[key] = index
        if len(resolution.decision_gap_ids) > 1 and not resolution.atomic:
            errors.append(
                f"multi-gap resolution for {key} must explicitly declare atomic=true"
            )
        for gap_id in resolution.decision_gap_ids:
            if gap_id in gap_owners:
                errors.append(f"decision gap {gap_id} has multiple resolutions")
            gap_owners[gap_id] = index
        for task_id in resolution.task_ids:
            if task_id in task_owners:
                errors.append(f"task {task_id} has conflicting resolution owners")
            task_owners[task_id] = index
        for plan_ref in resolution.plan_refs:
            if plan_ref in plan_owners:
                errors.append(
                    f"plan update {plan_ref} has conflicting resolution owners"
                )
            plan_owners[plan_ref] = index

    if required_gap_ids is not None:
        missing = set(required_gap_ids) - set(gap_owners)
        if missing:
            errors.append(f"blocking gaps have no resolution: {sorted(missing)}")
    if errors:
        raise WorkflowProtocolError("; ".join(errors))


def validate_event_resolution_contract(
    bundle: WorkflowPlanBundle,
    trigger_events: list[Any],
    *,
    known_task_ids: set[str],
    allow_information_requests: bool,
) -> None:
    errors: list[str] = []
    trigger_keys = {str(event.dedupe_key) for event in trigger_events}
    trigger_by_key = {str(event.dedupe_key): event for event in trigger_events}
    blocking_keys = {
        str(event.dedupe_key) for event in trigger_events if bool(event.blocking)
    }

    resolutions: dict[str, EventResolution] = {}
    for resolution in bundle.event_resolutions:
        key = resolution.event_dedupe_key
        if key in resolutions:
            errors.append(f"duplicate event resolution for {key}")
        resolutions[key] = resolution
        if len(resolution.decision_gap_ids) > 1 and not resolution.atomic:
            errors.append(
                f"multi-gap resolution for {key} must explicitly declare atomic=true"
            )
        if resolution.lease_contract is not None:
            _validate_lease_contract_for_event(
                resolution, trigger_by_key.get(key), errors
            )
        if key not in trigger_keys:
            errors.append(f"event resolution references unknown trigger event: {key}")

    missing = blocking_keys - set(resolutions)
    if missing:
        errors.append(f"blocking events have no resolution: {sorted(missing)}")

    request_by_id = {item.request_id: item for item in bundle.information_requests}
    if len(request_by_id) != len(bundle.information_requests):
        errors.append("duplicate information request_id")
    for request in bundle.information_requests:
        try:
            validate_information_request(request)
        except WorkflowProtocolError as exc:
            errors.append(str(exc))
        if request.event_dedupe_key not in trigger_keys:
            errors.append(
                f"information request {request.request_id} references unknown event "
                f"{request.event_dedupe_key}"
            )

    task_ids = {task.task_id for task in bundle.tasks} | set(known_task_ids)
    plan_refs = _plan_refs(bundle)
    referenced_information: set[str] = set()

    for key, resolution in resolutions.items():
        disposition = resolution.disposition
        if key in blocking_keys and disposition is ResolutionDisposition.DEFERRED:
            errors.append(f"blocking event {key} cannot be deferred")

        if disposition is ResolutionDisposition.TASK:
            if not resolution.task_ids:
                errors.append(f"task resolution for {key} has no task_ids")
            unknown_tasks = set(resolution.task_ids) - task_ids
            if unknown_tasks:
                errors.append(
                    f"task resolution for {key} references unknown tasks: "
                    f"{sorted(unknown_tasks)}"
                )
            requires_lease = bool(resolution.decision_gap_ids) or (
                trigger_by_key[key].event_type in STRATEGIC_GAP_TYPES
            )
            if requires_lease and resolution.lease_contract is None:
                errors.append(f"task resolution for {key} has no lease contract")
        elif disposition is ResolutionDisposition.PLAN_UPDATE:
            if not resolution.plan_refs:
                errors.append(f"plan_update resolution for {key} has no plan_refs")
            unknown_refs = set(resolution.plan_refs) - plan_refs
            if unknown_refs:
                errors.append(
                    f"plan_update resolution for {key} references missing plans: "
                    f"{sorted(unknown_refs)}"
                )
            requires_lease = bool(resolution.decision_gap_ids) or (
                trigger_by_key[key].event_type in STRATEGIC_GAP_TYPES
            )
            if requires_lease and resolution.lease_contract is None:
                errors.append(f"plan_update resolution for {key} has no lease contract")
        elif disposition is ResolutionDisposition.HUMAN_REVIEW:
            if not bundle.requires_human_review:
                errors.append(
                    f"human_review resolution for {key} requires "
                    "bundle.requires_human_review=true"
                )
        elif disposition is ResolutionDisposition.INFORMATION_REQUIRED:
            if not allow_information_requests:
                errors.append(
                    f"final planning phase cannot request more information: {key}"
                )
            if not resolution.information_request_ids:
                errors.append(
                    f"information_required resolution for {key} has no request IDs"
                )
            unknown_requests = set(resolution.information_request_ids) - set(
                request_by_id
            )
            if unknown_requests:
                errors.append(
                    f"information resolution for {key} references unknown requests: "
                    f"{sorted(unknown_requests)}"
                )
            referenced_information.update(resolution.information_request_ids)

    unreferenced = set(request_by_id) - referenced_information
    if unreferenced:
        errors.append(
            f"information requests are not attached to an event resolution: "
            f"{sorted(unreferenced)}"
        )

    for task in bundle.tasks:
        if task.action_type != "unit_found_city":
            continue
        unit_id = task.arguments.get("unit_id")
        required_preconditions = [
            {
                "type": "entity_exists",
                "entity_type": "unit",
                "entity_id": task.entity_id,
            },
            {
                "type": "unit_type_contains",
                "unit_id": unit_id,
                "marker": "SETTLER",
            },
        ]
        for required in required_preconditions:
            if required not in task.preconditions:
                errors.append(
                    f"unit_found_city task {task.task_id} missing precondition {required}"
                )
        if not any(
            condition.get("type") == "unit_at"
            and str(condition.get("unit_id")) == str(unit_id)
            for condition in task.preconditions
        ):
            errors.append(
                f"unit_found_city task {task.task_id} must pin the settler position"
            )
        if {"type": "unit_absent", "unit_id": unit_id} not in task.postconditions:
            errors.append(
                f"unit_found_city task {task.task_id} must verify the settler was consumed"
            )
        if not any(
            condition.get("type") == "city_count_at_least"
            for condition in task.postconditions
        ):
            errors.append(
                f"unit_found_city task {task.task_id} must verify a new city exists"
            )

    if bundle.information_requests:
        has_mutations = bool(
            bundle.tasks
            or bundle.cancel_task_ids
            or bundle.strategy_updates
            or bundle.city_plan_updates
            or bundle.unit_plan_updates
            or bundle.builder_plan_updates
        )
        if has_mutations:
            errors.append(
                "an information-request phase cannot also mutate plans or create tasks"
            )

    if errors:
        raise WorkflowProtocolError("; ".join(errors))


def _validate_lease_contract_for_event(resolution, event, errors):
    if event is None or event.event_type not in SETTLER_GAP_TYPES:
        return
    contract = resolution.lease_contract
    precondition_types = {item.get("type") for item in contract.preconditions}
    invalidation_types = {item.get("type") for item in contract.invalidation_conditions}
    if not {"entity_exists", "unit_type_contains"}.issubset(precondition_types):
        errors.append("settler lease requires existence and settler-type preconditions")
    required_start = {
        "tile_unoccupied",
        "settler_target_legal",
        "settler_path_reachable",
    }
    if not required_start.issubset(precondition_types):
        errors.append(
            "settler lease requires legal, unoccupied, reachable target evidence"
        )
    continuation_types = {item.get("type") for item in contract.continuation_conditions}
    required_continuation = {
        *required_start,
        "approved_target_equals",
        "severe_threat_absent",
    }
    if not required_continuation.issubset(continuation_types):
        errors.append("settler lease lacks material continuation conditions")
    completion = contract.completion_condition
    completion_items = (
        completion.get("conditions", [])
        if completion.get("type") == "all_of"
        else [completion]
    )
    completion_types = {item.get("type") for item in completion_items}
    if not {"unit_absent", "city_count_at_least"}.issubset(completion_types):
        errors.append(
            "settler lease completion requires consumed unit and new city evidence"
        )
    if "unit_absent" not in invalidation_types:
        errors.append("settler lease must invalidate when the settler disappears")
    severe_threat = any(
        item.get("type") == "field_in" and item.get("path") == "overview.threat_level"
        for item in contract.invalidation_conditions
    )
    if not severe_threat:
        errors.append("settler lease must handle severe threat invalidation")
    if (
        contract.continuation_policy
        is not ContinuationPolicy.EXTEND_WHEN_INPUT_UNCHANGED
    ):
        errors.append(
            "settler lease may continue only while relevant input is unchanged"
        )


def _plan_refs(bundle: WorkflowPlanBundle) -> set[str]:
    refs: set[str] = set()
    if bundle.strategy_updates:
        refs.add("strategy")
    for row in bundle.city_plan_updates:
        if isinstance(row, dict) and row.get("city_id") is not None:
            refs.add(f"city:{row['city_id']}")
    for row in bundle.unit_plan_updates:
        if isinstance(row, dict) and row.get("unit_id") is not None:
            refs.add(f"unit:{row['unit_id']}")
    for row in bundle.builder_plan_updates:
        if isinstance(row, dict) and row.get("builder_key") is not None:
            refs.add(f"builder:{row['builder_key']}")
    return refs
