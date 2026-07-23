from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

from .actions import ACTION_REGISTRY
from .models import PlanBundle, ProposedTask, RiskLevel


class PlanValidationError(ValueError):
    pass


DEFAULT_CONDITION_TYPES = {
    "all_of",
    "turn_at_least",
    "turn_equals",
    "no_blocker_type",
    "field_equals",
    "field_in",
    "entity_exists",
    "city_production_equals",
    "city_has_no_production",
    "research_unselected",
    "research_available",
    "research_equals",
    "civic_unselected",
    "civic_available",
    "civic_equals",
    "unit_at",
    "unit_has_moves",
    "unit_no_moves",
    "unit_has_build_charge",
    "unit_build_charges_equals",
    "unit_can_improve",
    "unit_absent",
    "unit_moved_from",
    "unit_type_contains",
    "city_count_at_least",
}

ACTION_ENTITY_TYPES = {
    "city_set_production": {"city"},
    "set_research": {"research"},
    "set_civic": {"civic"},
    "unit_move": {"unit", "builder"},
    "builder_improve": {"builder"},
    "unit_heal": {"unit"},
    "unit_fortify": {"unit"},
    "unit_skip": {"unit"},
    "unit_found_city": {"unit"},
}

ENTITY_ID_ARGUMENTS: Mapping[str, str] = MappingProxyType(
    {
        "builder": "unit_id",
        "city": "city_id",
        "civic": "tech_or_civic",
        "research": "tech_or_civic",
        "unit": "unit_id",
    }
)


def _freeze_contract(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_contract(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_contract(item) for item in value)
    return value


ACTION_CONDITION_CONTRACTS: Mapping[
    str, Mapping[str, tuple[Mapping[str, Any], ...]]
] = _freeze_contract(
    {
        "set_civic": {
            "required_preconditions": (
                {"type": "civic_unselected"},
                {
                    "type": "civic_available",
                    "civic_type": "$tech_or_civic",
                },
            ),
            "required_postconditions": (
                {
                    "type": "civic_equals",
                    "civic_type": "$tech_or_civic",
                },
            ),
        },
        "set_research": {
            "required_preconditions": (
                {"type": "research_unselected"},
                {
                    "type": "research_available",
                    "tech_type": "$tech_or_civic",
                },
            ),
            "required_postconditions": (
                {
                    "type": "research_equals",
                    "tech_type": "$tech_or_civic",
                },
            ),
        },
    }
)


def action_entity_type_contracts(
    action_types: set[str] | None = None,
) -> dict[str, list[str]]:
    selected = set(ACTION_REGISTRY) if action_types is None else set(action_types)
    unknown = selected - set(ACTION_REGISTRY)
    if unknown:
        raise PlanValidationError(
            f"unsupported action types in entity contract projection: {sorted(unknown)}"
        )
    missing = selected - set(ACTION_ENTITY_TYPES)
    if missing:
        raise PlanValidationError(
            f"actions missing entity type contracts: {sorted(missing)}"
        )
    return {
        action_type: sorted(ACTION_ENTITY_TYPES[action_type])
        for action_type in sorted(selected)
    }


def entity_id_argument_contracts(
    selected_action_entity_types: Mapping[str, list[str]],
) -> dict[str, str]:
    selected: dict[str, str] = {}
    for action_type in sorted(selected_action_entity_types):
        spec = ACTION_REGISTRY.get(action_type)
        if spec is None:
            raise PlanValidationError(
                f"entity contract references unsupported action type: {action_type}"
            )
        for entity_type in selected_action_entity_types[action_type]:
            argument_name = ENTITY_ID_ARGUMENTS.get(entity_type)
            if argument_name is None:
                raise PlanValidationError(
                    f"entity type has no ID argument contract: {entity_type}"
                )
            if argument_name not in spec.required_arguments:
                raise PlanValidationError(
                    f"action {action_type} entity type {entity_type} requires "
                    f"ID argument {argument_name} to be a required action argument"
                )
            selected[entity_type] = argument_name
    return {name: selected[name] for name in sorted(selected)}


def condition_contracts(
    action_types: set[str] | None = None,
) -> dict[str, dict[str, object]]:
    selected = set(ACTION_REGISTRY) if action_types is None else set(action_types)
    unknown = selected - set(ACTION_REGISTRY)
    if unknown:
        raise PlanValidationError(
            f"unsupported action types in condition contract projection: {sorted(unknown)}"
        )
    return {
        action_type: _stable_contract_copy(ACTION_CONDITION_CONTRACTS[action_type])
        for action_type in sorted(selected & set(ACTION_CONDITION_CONTRACTS))
    }


def _stable_contract_copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _stable_contract_copy(value[key])
            for key in sorted(value)
        }
    if isinstance(value, (list, tuple)):
        return [_stable_contract_copy(item) for item in value]
    return deepcopy(value)


@dataclass(slots=True)
class PlanValidationContext:
    current_turn: int
    allowed_action_types: set[str]
    known_entities: dict[str, set[str]]
    max_tasks: int = 100
    supported_condition_types: set[str] = field(
        default_factory=lambda: set(DEFAULT_CONDITION_TYPES)
    )


def validate_plan_bundle(bundle: PlanBundle, context: PlanValidationContext) -> None:
    errors: list[str] = []
    if len(bundle.tasks) > context.max_tasks:
        errors.append(f"too many tasks: {len(bundle.tasks)} > {context.max_tasks}")

    seen_task_ids: set[str] = set()
    entity_turn_claims: set[tuple[str, str, int]] = set()
    cancelled = set(bundle.cancel_task_ids)

    for task in bundle.tasks:
        if task.task_id in seen_task_ids:
            errors.append(f"duplicate task_id: {task.task_id}")
        seen_task_ids.add(task.task_id)
        if task.task_id in cancelled:
            errors.append(f"task is both created and cancelled: {task.task_id}")
        if task.action_type not in context.allowed_action_types:
            errors.append(f"action_type is not allowed: {task.action_type}")

        allowed_entities = ACTION_ENTITY_TYPES.get(task.action_type)
        if allowed_entities is not None and task.entity_type not in allowed_entities:
            errors.append(
                f"task {task.task_id} action {task.action_type} cannot target "
                f"entity_type {task.entity_type}; allowed={sorted(allowed_entities)}"
            )

        spec = ACTION_REGISTRY.get(task.action_type)
        if spec is None:
            errors.append(
                f"action_type has no deterministic registry entry: {task.action_type}"
            )
        else:
            supplied = set(task.arguments)
            missing = set(spec.required_arguments) - supplied
            unknown = (
                supplied - set(spec.required_arguments) - set(spec.optional_arguments)
            )
            fixed_collision = supplied & set(spec.fixed_arguments)
            if missing:
                errors.append(
                    f"task {task.task_id} missing arguments: {sorted(missing)}"
                )
            if unknown:
                errors.append(
                    f"task {task.task_id} has unknown arguments: {sorted(unknown)}"
                )
            if fixed_collision:
                errors.append(
                    f"task {task.task_id} attempts to override fixed arguments: "
                    f"{sorted(fixed_collision)}"
                )

        if task.action_type == "city_set_production":
            item_type = str(task.arguments.get("item_type", "")).upper()
            has_x = task.arguments.get("target_x") is not None
            has_y = task.arguments.get("target_y") is not None
            if item_type == "DISTRICT" and not (has_x and has_y):
                errors.append(
                    f"district production task {task.task_id} requires target_x and target_y"
                )
            if has_x != has_y:
                errors.append(
                    f"task {task.task_id} must provide target_x and target_y together"
                )

        _validate_action_contract(task, errors)

        if task.due_turn < context.current_turn:
            errors.append(
                f"task {task.task_id} due_turn {task.due_turn} is before current turn"
            )
        if task.expires_turn is not None and task.expires_turn < task.due_turn:
            errors.append(f"task {task.task_id} expires before it is due")
        if (
            task.risk in {RiskLevel.HIGH, RiskLevel.CRITICAL}
            and not task.requires_confirmation
        ):
            errors.append(
                f"high-risk task {task.task_id} must require user confirmation"
            )
        if not task.postconditions:
            errors.append(f"task {task.task_id} has no verifiable postconditions")

        for phase, conditions in (
            ("precondition", task.preconditions),
            ("postcondition", task.postconditions),
            ("invalidator", task.invalidators),
        ):
            for condition in conditions:
                condition_type = condition.get("type")
                if condition_type not in context.supported_condition_types:
                    errors.append(
                        f"task {task.task_id} has unsupported {phase} type: "
                        f"{condition_type!r}"
                    )

        entity_id = str(task.entity_id)
        known = context.known_entities.get(task.entity_type)
        if known is not None and entity_id not in known:
            errors.append(
                f"task {task.task_id} references unknown {task.entity_type} {entity_id}"
            )

        claim = (task.entity_type, entity_id, task.due_turn)
        if claim in entity_turn_claims:
            errors.append(
                f"multiple tasks claim {task.entity_type} {entity_id} on turn {task.due_turn}"
            )
        entity_turn_claims.add(claim)

        expected_argument = ENTITY_ID_ARGUMENTS.get(task.entity_type)
        if expected_argument and expected_argument in task.arguments:
            if str(task.arguments[expected_argument]) != entity_id:
                errors.append(
                    f"task {task.task_id} entity_id does not match {expected_argument}"
                )

    if errors:
        raise PlanValidationError("; ".join(errors))


def _validate_action_contract(task: ProposedTask, errors: list[str]) -> None:
    placeholders = sorted(
        {
            placeholder
            for conditions in (
                task.preconditions,
                task.postconditions,
                task.invalidators,
            )
            for condition in conditions
            for placeholder in _contract_placeholders(condition)
        }
    )
    if placeholders:
        errors.append(
            f"task {task.task_id} contains unresolved contract placeholder(s): "
            f"{placeholders}"
        )

    contract = ACTION_CONDITION_CONTRACTS.get(task.action_type)
    if contract is None:
        return
    try:
        required_preconditions = [
            _render_contract_value(condition, task.arguments)
            for condition in contract["required_preconditions"]
        ]
        required_postconditions = [
            _render_contract_value(condition, task.arguments)
            for condition in contract["required_postconditions"]
        ]
    except KeyError as exc:
        errors.append(
            f"task {task.task_id} condition contract references missing task "
            f"argument: {exc.args[0]}"
        )
        return
    _require_conditions(
        task,
        required_preconditions,
        required_postconditions,
        errors,
    )


def _render_contract_value(value: Any, arguments: Mapping[str, Any]) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        argument_name = value[1:]
        if argument_name not in arguments:
            raise KeyError(argument_name)
        return deepcopy(arguments[argument_name])
    if isinstance(value, Mapping):
        return {
            key: _render_contract_value(item, arguments)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_render_contract_value(item, arguments) for item in value]
    return deepcopy(value)


def _contract_placeholders(value: Any) -> set[str]:
    if isinstance(value, str) and value.startswith("$"):
        return {value}
    if isinstance(value, Mapping):
        return {
            placeholder
            for item in value.values()
            for placeholder in _contract_placeholders(item)
        }
    if isinstance(value, (list, tuple)):
        return {
            placeholder
            for item in value
            for placeholder in _contract_placeholders(item)
        }
    return set()


def _require_conditions(
    task: ProposedTask,
    required_preconditions: list[dict[str, Any]],
    required_postconditions: list[dict[str, Any]],
    errors: list[str],
) -> None:
    for condition in required_preconditions:
        if condition not in task.preconditions:
            errors.append(
                f"task {task.task_id} missing required precondition {condition}"
            )
    for condition in required_postconditions:
        if condition not in task.postconditions:
            errors.append(
                f"task {task.task_id} missing required postcondition {condition}"
            )
