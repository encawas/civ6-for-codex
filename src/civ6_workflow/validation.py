from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .actions import ACTION_REGISTRY
from .models import PlanBundle, ProposedTask, RiskLevel


class PlanValidationError(ValueError):
    pass


DEFAULT_CONDITION_TYPES = {
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
}


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
            unknown = supplied - set(spec.required_arguments) - set(
                spec.optional_arguments
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

        expected_argument = {
            "city": "city_id",
            "research": "tech_or_civic",
            "civic": "tech_or_civic",
            "unit": "unit_id",
            "builder": "unit_id",
        }.get(task.entity_type)
        if expected_argument and expected_argument in task.arguments:
            if str(task.arguments[expected_argument]) != entity_id:
                errors.append(
                    f"task {task.task_id} entity_id does not match {expected_argument}"
                )

    if errors:
        raise PlanValidationError("; ".join(errors))


def _validate_action_contract(task: ProposedTask, errors: list[str]) -> None:
    if task.action_type == "set_research":
        target = str(task.arguments.get("tech_or_civic", ""))
        _require_conditions(
            task,
            [
                {"type": "research_unselected"},
                {"type": "research_available", "tech_type": target},
            ],
            [{"type": "research_equals", "tech_type": target}],
            errors,
        )
    elif task.action_type == "set_civic":
        target = str(task.arguments.get("tech_or_civic", ""))
        _require_conditions(
            task,
            [
                {"type": "civic_unselected"},
                {"type": "civic_available", "civic_type": target},
            ],
            [{"type": "civic_equals", "civic_type": target}],
            errors,
        )


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
