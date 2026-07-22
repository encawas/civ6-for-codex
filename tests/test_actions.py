import json

import pytest

from civ6_workflow.actions import (
    ACTION_REGISTRY,
    ActionValidationError,
    action_argument_contracts,
    resolve_action,
)
from civ6_workflow.models import StoredTask


def _task(action_type, arguments):
    return StoredTask(
        task_id=f"test-{action_type}",
        plan_id="plan-1",
        action_type=action_type,
        entity_type="builder" if action_type == "builder_improve" else "unit",
        entity_id=9,
        due_turn=10,
        arguments=arguments,
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
