from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from dataclasses import InitVar, dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

from .domain import RetryClassification
from .models import StoredTask


class ActionValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ActionSpec:
    tool_name: str
    required_arguments: frozenset[str]
    optional_arguments: frozenset[str] = field(default_factory=frozenset)
    fixed_arguments: Mapping[str, Any] = field(default_factory=dict)
    argument_aliases: Mapping[str, str] = field(default_factory=dict)
    retry_classification: RetryClassification = (
        RetryClassification.SAFE_IF_PROVEN_NOT_SENT
    )
    retry_safe_after_unknown: InitVar[bool | None] = None

    def __post_init__(self, retry_safe_after_unknown: bool | None) -> None:
        if retry_safe_after_unknown is not None:
            classification = (
                RetryClassification.SAFE_IF_PROVEN_NOT_SENT
                if retry_safe_after_unknown
                else RetryClassification.NEVER_BLIND_RETRY
            )
            object.__setattr__(self, "retry_classification", classification)
        object.__setattr__(
            self,
            "fixed_arguments",
            MappingProxyType(dict(self.fixed_arguments)),
        )
        object.__setattr__(
            self,
            "argument_aliases",
            MappingProxyType(dict(self.argument_aliases)),
        )

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


_ACTION_REGISTRY = {
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
        retry_classification=RetryClassification.NEVER_BLIND_RETRY,
    ),
    "unit_found_city": ActionSpec(
        tool_name="unit_action",
        required_arguments=frozenset({"unit_id"}),
        fixed_arguments={"action": "found_city"},
        retry_classification=RetryClassification.NEVER_BLIND_RETRY,
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


class BootstrapActionRegistry(MutableMapping[str, ActionSpec]):
    """One canonical registry that freezes after the legacy bootstrap write."""

    def __init__(self, initial: Mapping[str, ActionSpec]):
        self._items = dict(initial)
        self._frozen = False

    def __getitem__(self, key: str) -> ActionSpec:
        return self._items[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __setitem__(self, key: str, value: ActionSpec) -> None:
        if self._frozen or key != "unit_found_city":
            raise TypeError("action registry is frozen after bootstrap")
        self._items[key] = value
        self._frozen = True

    def __delitem__(self, key: str) -> None:
        raise TypeError("action registry is frozen after bootstrap")


ACTION_REGISTRY: MutableMapping[str, ActionSpec] = BootstrapActionRegistry(
    _ACTION_REGISTRY
)

END_TURN_ACTION_SPEC = ActionSpec(
    tool_name="end_turn",
    required_arguments=frozenset(),
    retry_classification=RetryClassification.NEVER_BLIND_RETRY,
)


def resolve_action_spec(action_type: str) -> ActionSpec:
    spec = ACTION_REGISTRY.get(action_type)
    if spec is None:
        raise ActionValidationError(f"unsupported action_type: {action_type}")
    return spec


def resolve_action(
    task: StoredTask,
    allowed_tools: set[str],
) -> tuple[str, dict[str, Any]]:
    spec = resolve_action_spec(task.action_type)
    if spec.tool_name not in allowed_tools:
        raise ActionValidationError(f"tool is not allowed: {spec.tool_name}")
    return spec.tool_name, spec.build_arguments(task)
