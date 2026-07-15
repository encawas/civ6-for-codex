from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import StoredTask


class ActionValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ActionSpec:
    tool_name: str
    required_arguments: frozenset[str]
    optional_arguments: frozenset[str] = field(default_factory=frozenset)
    fixed_arguments: dict[str, Any] = field(default_factory=dict)
    argument_aliases: dict[str, str] = field(default_factory=dict)
    # False means that, after a transport error or an unverified success, the
    # workflow must reconcile state instead of sending the action again.
    retry_safe_after_unknown: bool = True

    def build_arguments(self, task: StoredTask) -> dict[str, Any]:
        supplied = dict(task.arguments)
        allowed = self.required_arguments | self.optional_arguments
        unknown = set(supplied) - allowed
        missing = self.required_arguments - set(supplied)
        if unknown:
            raise ActionValidationError(
                f"unknown arguments for {task.action_type}: {sorted(unknown)}"
            )
        if missing:
            raise ActionValidationError(
                f"missing arguments for {task.action_type}: {sorted(missing)}"
            )
        translated = {
            self.argument_aliases.get(name, name): value
            for name, value in supplied.items()
        }
        collision = set(translated) & set(self.fixed_arguments)
        if collision:
            raise ActionValidationError(
                f"fixed arguments cannot be overridden: {sorted(collision)}"
            )
        if len(translated) != len(supplied):
            raise ActionValidationError(
                f"argument aliases collide for {task.action_type}"
            )
        return {**translated, **self.fixed_arguments}


ACTION_REGISTRY: dict[str, ActionSpec] = {
    "city_set_production": ActionSpec(
        tool_name="set_city_production",
        required_arguments=frozenset({"city_id", "item_type", "item_name"}),
        optional_arguments=frozenset({"target_x", "target_y"}),
    ),
    "set_research": ActionSpec(
        tool_name="set_research",
        required_arguments=frozenset({"tech_or_civic"}),
        fixed_arguments={"category": "tech"},
    ),
    "set_civic": ActionSpec(
        tool_name="set_research",
        required_arguments=frozenset({"tech_or_civic"}),
        fixed_arguments={"category": "civic"},
    ),
    "unit_move": ActionSpec(
        tool_name="unit_action",
        required_arguments=frozenset({"unit_id", "target_x", "target_y"}),
        fixed_arguments={"action": "move"},
    ),
    "builder_improve": ActionSpec(
        tool_name="unit_action",
        required_arguments=frozenset({"unit_id", "improvement_type"}),
        fixed_arguments={"action": "improve"},
        argument_aliases={"improvement_type": "improvement"},
        retry_safe_after_unknown=False,
    ),
    "unit_heal": ActionSpec(
        tool_name="unit_action",
        required_arguments=frozenset({"unit_id"}),
        fixed_arguments={"action": "heal"},
    ),
    "unit_fortify": ActionSpec(
        tool_name="unit_action",
        required_arguments=frozenset({"unit_id"}),
        fixed_arguments={"action": "fortify"},
    ),
    "unit_skip": ActionSpec(
        tool_name="unit_action",
        required_arguments=frozenset({"unit_id"}),
        fixed_arguments={"action": "skip"},
    ),
}


def action_argument_contracts() -> dict[str, dict[str, Any]]:
    return {
        action_type: {
            "required": sorted(spec.required_arguments),
            "optional": sorted(spec.optional_arguments),
            "injected_by_runtime": dict(spec.fixed_arguments),
        }
        for action_type, spec in sorted(ACTION_REGISTRY.items())
    }


def resolve_action(task: StoredTask, allowed_tools: set[str]) -> tuple[str, dict[str, Any]]:
    spec = ACTION_REGISTRY.get(task.action_type)
    if spec is None:
        raise ActionValidationError(f"unsupported action_type: {task.action_type}")
    if spec.tool_name not in allowed_tools:
        raise ActionValidationError(f"tool is not allowed: {spec.tool_name}")
    return spec.tool_name, spec.build_arguments(task)
