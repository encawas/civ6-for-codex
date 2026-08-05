import json
from pathlib import Path

import pytest

from civ6_workflow.actions import (
    ACTION_REGISTRY,
    ActionSpec,
    ActionValidationError,
    action_argument_contracts,
    action_types_for_scope,
    canonical_action_types,
    resolve_action,
)
from civ6_workflow.config import load_config
from civ6_workflow.domain import (
    TurnActionNode,
    build_turn_action_graph_id,
    build_turn_action_node_id,
)
from civ6_workflow.mcp_port import Civ6GamePort
from civ6_workflow.models import MutationDeliveryStatus, RiskLevel, TurnActionExecution
from civ6_workflow.runtime import RuntimeConfig


def _task(action_type, arguments):
    subject_argument = {
        "city_set_production": "city_id",
        "set_research": "tech_or_civic",
        "set_civic": "tech_or_civic",
        "send_envoy": "player_id",
    }.get(action_type, "unit_id")
    entity_type = {
        "city_set_production": "city",
        "set_research": "research",
        "set_civic": "civic",
        "send_envoy": "city_state",
        "builder_improve": "builder",
    }.get(action_type, "unit")
    high_risk = {
        "send_envoy",
        "unit_move",
        "unit_found_city",
        "tactical_unit_move",
        "tactical_unit_fortify",
        "tactical_unit_skip",
    }
    always_confirm = {
        "unit_found_city",
        "tactical_unit_move",
        "tactical_unit_fortify",
        "tactical_unit_skip",
    }
    return TurnActionExecution(
        task_id=f"test-{action_type}",
        plan_id="plan-1",
        action_type=action_type,
        entity_type=entity_type,
        entity_id=arguments[subject_argument],
        due_turn=10,
        arguments=arguments,
        risk=RiskLevel.HIGH if action_type in high_risk else RiskLevel.LOW,
        requires_confirmation=action_type in always_confirm,
        reason="verify upstream argument contract",
        created_turn=10,
    )


def test_builder_improvement_uses_upstream_improvement_parameter():
    tool, arguments = resolve_action(
        _task(
            "builder_improve",
            {"unit_id": 9, "improvement_type": "IMPROVEMENT_MINE"},
        ),
        {"unit_action"},
    )
    assert tool == "unit_action"
    assert arguments == {
        "unit_id": 9,
        "improvement": "IMPROVEMENT_MINE",
        "action": "improve",
    }
    assert "improvement_type" not in arguments


def test_research_and_civic_share_upstream_tool_with_fixed_category():
    research_tool, research_arguments = resolve_action(
        _task("set_research", {"tech_or_civic": "TECH_MINING"}),
        {"set_research"},
    )
    civic_tool, civic_arguments = resolve_action(
        _task("set_civic", {"tech_or_civic": "CIVIC_CRAFTSMANSHIP"}),
        {"set_research"},
    )
    assert research_tool == civic_tool == "set_research"
    assert research_arguments == {
        "tech_or_civic": "TECH_MINING",
        "category": "tech",
    }
    assert civic_arguments == {
        "tech_or_civic": "CIVIC_CRAFTSMANSHIP",
        "category": "civic",
    }


def test_envoy_uses_closed_player_id_contract_and_is_never_blind_retry():
    tool, arguments = resolve_action(
        _task("send_envoy", {"player_id": 8}),
        {"send_envoy"},
    )
    assert tool == "send_envoy"
    assert arguments == {"player_id": 8}
    assert (
        ACTION_REGISTRY["send_envoy"].retry_classification.value == "NEVER_BLIND_RETRY"
    )


def test_mcp_connection_failure_result_is_proven_not_sent():
    result = Civ6GamePort._normalize_action_result(
        {
            "result": (
                "Cannot connect to Civ 6 at 127.0.0.1:4318. "
                "Is the game running with EnableTuner=1?"
            )
        }
    )
    assert result.success is False
    assert result.blocked is False
    assert result.delivery_status is MutationDeliveryStatus.PROVEN_NOT_SENT


def test_action_argument_contracts_are_stable_and_match_registry():
    contracts = action_argument_contracts()
    assert list(contracts) == sorted(contracts)
    assert contracts["set_research"] == {
        "required": ["tech_or_civic"],
        "optional": [],
        "injected_by_runtime": {"category": "tech"},
    }
    for action_type, contract in contracts.items():
        spec = ACTION_REGISTRY[action_type]
        assert contract["required"] == sorted(spec.required_arguments)
        assert contract["optional"] == sorted(spec.optional_arguments)
        assert list(contract["injected_by_runtime"]) == sorted(
            contract["injected_by_runtime"]
        )
        assert not (
            set(contract["injected_by_runtime"])
            & (set(contract["required"]) | set(contract["optional"]))
        )
    first = json.dumps(contracts, separators=(",", ":"))
    second = json.dumps(action_argument_contracts(), separators=(",", ":"))
    assert first == second


def test_action_argument_contract_projection_is_filtered_and_defensive():
    first = action_argument_contracts({"set_research"})
    first["set_research"]["required"].append("pollution")
    first["set_research"]["injected_by_runtime"]["category"] = "pollution"
    second = action_argument_contracts({"set_research"})
    assert list(second) == ["set_research"]
    assert second["set_research"]["required"] == ["tech_or_civic"]
    assert second["set_research"]["injected_by_runtime"] == {"category": "tech"}
    assert ACTION_REGISTRY["set_research"].fixed_arguments["category"] == "tech"


def test_action_argument_contract_projection_rejects_unknown_actions():
    with pytest.raises(ActionValidationError, match="unsupported action types"):
        action_argument_contracts({"unknown_action"})


@pytest.mark.parametrize(
    ("action_type", "arguments", "message"),
    [
        (
            "unit_move",
            {"unit_id": 9, "target_x": "east", "target_y": 4},
            "target_x must be a bounded integer coordinate",
        ),
        (
            "city_set_production",
            {
                "city_id": 9,
                "item_type": "UNIT",
                "item_name": "SCOUT",
                "target_x": 1,
            },
            "target_x and target_y must be provided together",
        ),
        (
            "send_envoy",
            {"player_id": "8"},
            "player_id must be a non-negative integer",
        ),
    ],
)
def test_action_argument_types_and_cross_field_contracts_fail_closed(
    action_type, arguments, message
):
    with pytest.raises(ActionValidationError, match=message):
        resolve_action(
            _task(action_type, arguments),
            {ACTION_REGISTRY[action_type].tool_name},
        )


@pytest.mark.parametrize(
    ("action_type", "arguments", "different_entity_id", "subject_argument"),
    [
        (
            "unit_move",
            {"unit_id": 9, "target_x": 1, "target_y": 2},
            10,
            "unit_id",
        ),
        (
            "city_set_production",
            {"city_id": 9, "item_type": "UNIT", "item_name": "SCOUT"},
            10,
            "city_id",
        ),
        ("send_envoy", {"player_id": 8}, 9, "player_id"),
    ],
)
def test_action_subject_must_match_task_entity(
    action_type, arguments, different_entity_id, subject_argument
):
    task = _task(action_type, arguments).model_copy(
        update={"entity_id": different_entity_id}
    )
    with pytest.raises(
        ActionValidationError,
        match=f"entity_id must match {subject_argument}",
    ):
        resolve_action(task, {ACTION_REGISTRY[action_type].tool_name})


@pytest.mark.parametrize(
    "updates",
    [
        {
            "required_arguments": frozenset({"value"}),
            "optional_arguments": frozenset({"value"}),
        },
        {
            "required_arguments": frozenset({"value"}),
            "fixed_arguments": {"value": 1},
        },
        {
            "required_arguments": frozenset({"value"}),
            "argument_aliases": {"other": "value"},
        },
        {
            "required_arguments": frozenset({"first", "second"}),
            "argument_aliases": {"first": "same", "second": "same"},
        },
    ],
)
def test_action_spec_rejects_registry_conflicts_at_construction(updates):
    fields = {
        "tool_name": "tool",
        "entity_type": "unit",
        "scopes": frozenset({"test"}),
        "required_arguments": frozenset(),
    }
    fields.update(updates)
    with pytest.raises(ValueError):
        ActionSpec(**fields)


def _node_identity(**updates):
    values = {
        "game_session_id": "game",
        "turn_number": 4,
        "source_observation_id": "obs",
        "source_observation_projection_hash": "a" * 64,
        "source_contract_id": "contract",
        "source_contract_revision": 2,
        "source_mission_id": "mission",
        "source_mission_revision": 3,
        "action_type": "set_research",
        "entity_type": "research",
        "entity_id": "TECH_MINING",
        "arguments": {"tech_or_civic": "TECH_MINING"},
        "preconditions": ({"type": "research_unselected"},),
        "postconditions": ({"type": "research_equals", "tech_type": "TECH_MINING"},),
        "invalidators": (),
        "risk": RiskLevel.LOW.value,
        "requires_confirmation": False,
        "target_turn": 4,
        "dependency_node_ids": (),
        "reason": "advance research",
    }
    values.update(updates)
    return values


@pytest.mark.parametrize(
    "updates",
    [
        {"preconditions": ()},
        {"postconditions": ()},
        {"invalidators": ({"type": "turn_changed"},)},
        {"risk": RiskLevel.MEDIUM.value},
        {"requires_confirmation": True},
        {"dependency_node_ids": ("node-prior",)},
    ],
)
def test_turn_action_node_identity_binds_safety_semantics(updates):
    original = _node_identity()
    changed = _node_identity(**updates)
    assert build_turn_action_node_id(**original) != build_turn_action_node_id(**changed)


def test_graph_identity_changes_with_node_safety_semantics():
    original_node = build_turn_action_node_id(**_node_identity())
    changed_node = build_turn_action_node_id(
        **_node_identity(requires_confirmation=True)
    )
    graph = {
        "game_session_id": "game",
        "turn_number": 4,
        "source_observation_id": "obs",
        "source_observation_projection_hash": "a" * 64,
        "source_contract_id": "contract",
        "source_contract_revision": 2,
    }
    assert build_turn_action_graph_id(
        **graph, node_ids=(original_node,)
    ) != build_turn_action_graph_id(**graph, node_ids=(changed_node,))


def test_turn_action_node_rejects_policy_below_catalog_minimum():
    identity = _node_identity(
        action_type="unit_found_city",
        entity_type="unit",
        entity_id="9",
        arguments={"unit_id": 9},
        preconditions=(),
        postconditions=(),
        risk=RiskLevel.LOW.value,
        requires_confirmation=False,
    )
    node_id = build_turn_action_node_id(**identity)
    with pytest.raises(ValueError, match="risk must be at least high"):
        TurnActionNode(
            node_id=node_id,
            graph_id="pending",
            idempotency_key=f"turn-action:{node_id}",
            **identity,
        )


def test_canonical_action_catalog_has_one_scope_projection():
    projected = {
        action_type
        for scope in {
            "research",
            "civic",
            "settler",
            "city_roles",
            "diplomacy_trade",
            "tactical_emergency",
        }
        for action_type in action_types_for_scope(scope)
    }
    assert projected == set(canonical_action_types())
    assert {
        "builder_improve",
        "unit_heal",
        "unit_fortify",
        "unit_skip",
    }.isdisjoint(projected)


def test_runtime_config_enforces_action_safety_for_direct_callers():
    with pytest.raises(ValueError, match="subset"):
        RuntimeConfig(
            allowed_action_types=set(),
            auto_action_types={"set_research"},
        )
    deny_all = RuntimeConfig(
        allowed_action_types=set(),
        auto_action_types=set(),
        allowed_tools=set(),
    )
    assert deny_all.allowed_action_types == set()


def test_example_config_loads_with_explicit_action_subset():
    config = load_config(Path(__file__).parents[1] / "config.example.toml")
    assert set(config.safety.auto_action_types) <= set(
        config.safety.allowed_action_types
    )
    assert config.runtime_config().allowed_action_types == set(
        config.safety.allowed_action_types
    )
