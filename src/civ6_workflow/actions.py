from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

from .domain import RetryClassification
from .models import RiskLevel, TurnActionExecution


class ActionValidationError(ValueError):
    pass


_RISK_ORDER = {
    RiskLevel.LOW: 0,
    RiskLevel.MEDIUM: 1,
    RiskLevel.HIGH: 2,
    RiskLevel.CRITICAL: 3,
}
_COORDINATE_ARGUMENTS = frozenset({"target_x", "target_y"})
_STABLE_IDENTIFIER_ARGUMENTS = frozenset({"unit_id", "city_id"})
_NONEMPTY_STRING_ARGUMENTS = frozenset(
    {
        "tech_or_civic",
        "item_type",
        "item_name",
        "improvement_type",
    }
)
_MAX_COORDINATE = 9999


def build_action_attempt_idempotency_key(
    task: TurnActionExecution,
    normalized_arguments: Mapping[str, Any],
) -> str:
    semantic = {
        "task_id": task.task_id,
        "action_type": task.action_type,
        "entity_type": task.entity_type,
        "entity_id": task.entity_id,
        "arguments": dict(normalized_arguments),
        "preconditions": task.preconditions,
        "postconditions": task.postconditions,
    }
    digest = hashlib.sha256(
        json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return f"task:{task.task_id}:{digest}"


@dataclass(frozen=True, slots=True)
class ActionSpec:
    tool_name: str
    entity_type: str
    scopes: frozenset[str]
    required_arguments: frozenset[str]
    optional_arguments: frozenset[str] = field(default_factory=frozenset)
    fixed_arguments: Mapping[str, Any] = field(default_factory=dict)
    argument_aliases: Mapping[str, str] = field(default_factory=dict)
    retry_classification: RetryClassification = (
        RetryClassification.SAFE_IF_PROVEN_NOT_SENT
    )
    minimum_risk: RiskLevel = RiskLevel.LOW
    always_requires_confirmation: bool = False
    subject_argument: str | None = None
    coordinate_pair: tuple[str, str] | None = None
    canonical_turn_action: bool = True

    def __post_init__(self) -> None:
        if not self.tool_name.strip():
            raise ValueError("ActionSpec tool_name must not be empty")
        if not self.entity_type.strip():
            raise ValueError("ActionSpec entity_type must not be empty")
        overlap = self.required_arguments & self.optional_arguments
        if overlap:
            raise ValueError(
                f"ActionSpec required and optional arguments overlap: {sorted(overlap)}"
            )
        planner_arguments = self.required_arguments | self.optional_arguments
        fixed_overlap = set(self.fixed_arguments) & planner_arguments
        if fixed_overlap:
            raise ValueError(
                "ActionSpec fixed arguments overlap planner arguments: "
                f"{sorted(fixed_overlap)}"
            )
        unknown_aliases = set(self.argument_aliases) - planner_arguments
        if unknown_aliases:
            raise ValueError(
                f"ActionSpec aliases have unknown sources: {sorted(unknown_aliases)}"
            )
        alias_targets = tuple(self.argument_aliases.values())
        if len(alias_targets) != len(set(alias_targets)):
            raise ValueError("ActionSpec alias targets must be unique")
        fixed_alias_targets = set(alias_targets) & set(self.fixed_arguments)
        if fixed_alias_targets:
            raise ValueError(
                "ActionSpec alias targets overlap fixed arguments: "
                f"{sorted(fixed_alias_targets)}"
            )
        if (
            self.subject_argument is not None
            and self.subject_argument not in planner_arguments
        ):
            raise ValueError("ActionSpec subject argument must be a planner argument")
        if self.coordinate_pair is not None:
            first, second = self.coordinate_pair
            if first == second or not {first, second} <= planner_arguments:
                raise ValueError(
                    "ActionSpec coordinate pair must name two planner arguments"
                )
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

    def build_arguments(self, task: TurnActionExecution) -> dict[str, Any]:
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
        self._validate_argument_values(task, supplied)
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

    def validate_node_policy(
        self,
        *,
        entity_type: str,
        risk: str,
        requires_confirmation: bool,
    ) -> None:
        if not self.canonical_turn_action:
            raise ActionValidationError(
                "action is not a canonical TurnActionGraph action"
            )
        self.validate_execution_policy(
            entity_type=entity_type,
            risk=risk,
            requires_confirmation=requires_confirmation,
        )

    def validate_execution_policy(
        self,
        *,
        entity_type: str,
        risk: str | RiskLevel,
        requires_confirmation: bool,
    ) -> None:
        if entity_type != self.entity_type:
            raise ActionValidationError(
                f"action requires entity_type={self.entity_type}"
            )
        try:
            actual_risk = RiskLevel(risk)
        except ValueError as exc:
            raise ActionValidationError(f"unsupported action risk: {risk}") from exc
        if _RISK_ORDER[actual_risk] < _RISK_ORDER[self.minimum_risk]:
            raise ActionValidationError(
                f"action risk must be at least {self.minimum_risk.value}"
            )
        if self.always_requires_confirmation and not requires_confirmation:
            raise ActionValidationError("action always requires confirmation")

    def _validate_argument_values(
        self,
        task: TurnActionExecution,
        supplied: dict[str, Any],
    ) -> None:
        for name in _STABLE_IDENTIFIER_ARGUMENTS & set(supplied):
            value = supplied[name]
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise ActionValidationError(f"{name} must be a stable identifier")
            if isinstance(value, int) and value < 0:
                raise ActionValidationError(f"{name} must be non-negative")
            if isinstance(value, str) and not value.strip():
                raise ActionValidationError(f"{name} must not be blank")
        if "player_id" in supplied:
            player_id = supplied["player_id"]
            if (
                isinstance(player_id, bool)
                or not isinstance(player_id, int)
                or player_id < 0
            ):
                raise ActionValidationError("player_id must be a non-negative integer")
        for name in _NONEMPTY_STRING_ARGUMENTS & set(supplied):
            value = supplied[name]
            if not isinstance(value, str) or not value.strip():
                raise ActionValidationError(f"{name} must be a non-blank string")
        for name in _COORDINATE_ARGUMENTS & set(supplied):
            value = supplied[name]
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or abs(value) > _MAX_COORDINATE
            ):
                raise ActionValidationError(
                    f"{name} must be a bounded integer coordinate"
                )
        if self.coordinate_pair is not None:
            first, second = self.coordinate_pair
            if (first in supplied) != (second in supplied):
                raise ActionValidationError(
                    f"{first} and {second} must be provided together"
                )
        if self.subject_argument is not None:
            subject = supplied[self.subject_argument]
            if str(task.entity_id) != str(subject):
                raise ActionValidationError(
                    f"task entity_id must match {self.subject_argument}"
                )


@dataclass(frozen=True, slots=True)
class PreparedAction:
    action_type: str
    tool_name: str
    normalized_arguments: Mapping[str, Any]
    retry_classification: RetryClassification

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "normalized_arguments",
            MappingProxyType(deepcopy(dict(self.normalized_arguments))),
        )


ACTION_REGISTRY: Mapping[str, ActionSpec] = MappingProxyType(
    {
        "city_set_production": ActionSpec(
            tool_name="set_city_production",
            entity_type="city",
            scopes=frozenset({"city_roles"}),
            required_arguments=frozenset({"city_id", "item_type", "item_name"}),
            optional_arguments=frozenset({"target_x", "target_y"}),
            subject_argument="city_id",
            coordinate_pair=("target_x", "target_y"),
        ),
        "set_research": ActionSpec(
            tool_name="set_research",
            entity_type="research",
            scopes=frozenset({"research"}),
            required_arguments=frozenset({"tech_or_civic"}),
            fixed_arguments={"category": "tech"},
            subject_argument="tech_or_civic",
        ),
        "set_civic": ActionSpec(
            tool_name="set_research",
            entity_type="civic",
            scopes=frozenset({"civic"}),
            required_arguments=frozenset({"tech_or_civic"}),
            fixed_arguments={"category": "civic"},
            subject_argument="tech_or_civic",
        ),
        "send_envoy": ActionSpec(
            tool_name="send_envoy",
            entity_type="city_state",
            scopes=frozenset({"diplomacy_trade"}),
            required_arguments=frozenset({"player_id"}),
            retry_classification=RetryClassification.NEVER_BLIND_RETRY,
            minimum_risk=RiskLevel.HIGH,
            subject_argument="player_id",
        ),
        "unit_move": ActionSpec(
            tool_name="unit_action",
            entity_type="unit",
            scopes=frozenset({"settler"}),
            required_arguments=frozenset({"unit_id", "target_x", "target_y"}),
            fixed_arguments={"action": "move"},
            minimum_risk=RiskLevel.HIGH,
            subject_argument="unit_id",
            coordinate_pair=("target_x", "target_y"),
        ),
        "unit_found_city": ActionSpec(
            tool_name="unit_action",
            entity_type="unit",
            scopes=frozenset({"settler"}),
            required_arguments=frozenset({"unit_id"}),
            fixed_arguments={"action": "found_city"},
            retry_classification=RetryClassification.NEVER_BLIND_RETRY,
            minimum_risk=RiskLevel.HIGH,
            always_requires_confirmation=True,
            subject_argument="unit_id",
        ),
        "tactical_unit_move": ActionSpec(
            tool_name="unit_action",
            entity_type="unit",
            scopes=frozenset({"tactical_emergency"}),
            required_arguments=frozenset({"unit_id", "target_x", "target_y"}),
            fixed_arguments={"action": "move"},
            minimum_risk=RiskLevel.HIGH,
            always_requires_confirmation=True,
            subject_argument="unit_id",
            coordinate_pair=("target_x", "target_y"),
        ),
        "tactical_unit_fortify": ActionSpec(
            tool_name="unit_action",
            entity_type="unit",
            scopes=frozenset({"tactical_emergency"}),
            required_arguments=frozenset({"unit_id"}),
            fixed_arguments={"action": "fortify"},
            minimum_risk=RiskLevel.HIGH,
            always_requires_confirmation=True,
            subject_argument="unit_id",
        ),
        "tactical_unit_skip": ActionSpec(
            tool_name="unit_action",
            entity_type="unit",
            scopes=frozenset({"tactical_emergency"}),
            required_arguments=frozenset({"unit_id"}),
            fixed_arguments={"action": "skip"},
            minimum_risk=RiskLevel.HIGH,
            always_requires_confirmation=True,
            subject_argument="unit_id",
        ),
        # Historical replay-only actions. The canonical TurnActionGraph cannot emit them.
        "builder_improve": ActionSpec(
            tool_name="unit_action",
            entity_type="builder",
            scopes=frozenset(),
            required_arguments=frozenset({"unit_id", "improvement_type"}),
            fixed_arguments={"action": "improve"},
            argument_aliases={"improvement_type": "improvement"},
            retry_classification=RetryClassification.NEVER_BLIND_RETRY,
            subject_argument="unit_id",
            canonical_turn_action=False,
        ),
        "unit_heal": ActionSpec(
            tool_name="unit_action",
            entity_type="unit",
            scopes=frozenset(),
            required_arguments=frozenset({"unit_id"}),
            fixed_arguments={"action": "heal"},
            subject_argument="unit_id",
            canonical_turn_action=False,
        ),
        "unit_fortify": ActionSpec(
            tool_name="unit_action",
            entity_type="unit",
            scopes=frozenset(),
            required_arguments=frozenset({"unit_id"}),
            fixed_arguments={"action": "fortify"},
            subject_argument="unit_id",
            canonical_turn_action=False,
        ),
        "unit_skip": ActionSpec(
            tool_name="unit_action",
            entity_type="unit",
            scopes=frozenset(),
            required_arguments=frozenset({"unit_id"}),
            fixed_arguments={"action": "skip"},
            subject_argument="unit_id",
            canonical_turn_action=False,
        ),
    }
)

END_TURN_ACTION_SPEC = ActionSpec(
    tool_name="end_turn",
    entity_type="game",
    scopes=frozenset(),
    required_arguments=frozenset(),
    retry_classification=RetryClassification.NEVER_BLIND_RETRY,
    minimum_risk=RiskLevel.HIGH,
    always_requires_confirmation=True,
    canonical_turn_action=False,
)


def canonical_action_types() -> frozenset[str]:
    return frozenset(
        action_type
        for action_type, spec in ACTION_REGISTRY.items()
        if spec.canonical_turn_action
    )


def action_types_for_scope(scope: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            action_type
            for action_type, spec in ACTION_REGISTRY.items()
            if spec.canonical_turn_action and scope in spec.scopes
        )
    )


def action_argument_contracts(
    action_types: set[str] | None = None,
) -> dict[str, dict[str, object]]:
    selected = (
        set(canonical_action_types()) if action_types is None else set(action_types)
    )
    unknown = selected - set(ACTION_REGISTRY)
    if unknown:
        raise ActionValidationError(
            f"unsupported action types in contract projection: {sorted(unknown)}"
        )

    contracts: dict[str, dict[str, object]] = {}
    for action_type in sorted(selected):
        spec = ACTION_REGISTRY[action_type]
        contracts[action_type] = {
            "required": sorted(spec.required_arguments),
            "optional": sorted(spec.optional_arguments),
            "injected_by_runtime": {
                name: deepcopy(spec.fixed_arguments[name])
                for name in sorted(spec.fixed_arguments)
            },
        }
    return contracts


def resolve_action_spec(action_type: str) -> ActionSpec:
    spec = ACTION_REGISTRY.get(action_type)
    if spec is None:
        raise ActionValidationError(f"unsupported action_type: {action_type}")
    return spec


def prepare_action(
    task: TurnActionExecution,
    allowed_tools: set[str],
) -> PreparedAction:
    spec = resolve_action_spec(task.action_type)
    spec.validate_execution_policy(
        entity_type=task.entity_type,
        risk=task.risk,
        requires_confirmation=task.requires_confirmation,
    )
    if spec.tool_name not in allowed_tools:
        raise ActionValidationError(f"tool is not allowed: {spec.tool_name}")
    return PreparedAction(
        action_type=task.action_type,
        tool_name=spec.tool_name,
        normalized_arguments=spec.build_arguments(task),
        retry_classification=spec.retry_classification,
    )


def resolve_action(
    task: TurnActionExecution,
    allowed_tools: set[str],
) -> tuple[str, dict[str, Any]]:
    prepared = prepare_action(task, allowed_tools)
    return prepared.tool_name, dict(prepared.normalized_arguments)
