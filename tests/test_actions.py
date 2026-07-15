from civ6_workflow.actions import resolve_action
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
