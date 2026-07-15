import pytest

from civ6_workflow.models import PlanBundle, ProposedTask
from civ6_workflow.validation import (
    PlanValidationContext,
    PlanValidationError,
    validate_plan_bundle,
)


def _context():
    return PlanValidationContext(
        current_turn=10,
        allowed_action_types={
            "city_set_production",
            "builder_improve",
            "set_research",
            "set_civic",
        },
        known_entities={
            "city": {"1"},
            "builder": {"9"},
            "unit": {"9"},
            "research": {"TECH_MINING"},
            "civic": {"CIVIC_CODE_OF_LAWS"},
        },
    )


def test_rejects_wrong_entity_type_for_builder_action():
    bundle = PlanBundle(
        summary="invalid entity contract",
        tasks=[
            ProposedTask(
                task_id="bad-builder",
                action_type="builder_improve",
                entity_type="unit",
                entity_id=9,
                due_turn=10,
                arguments={"unit_id": 9, "improvement_type": "IMPROVEMENT_MINE"},
                postconditions=[
                    {"type": "unit_build_charges_equals", "unit_id": 9, "charges": 1}
                ],
                reason="invalid test",
            )
        ],
    )

    with pytest.raises(PlanValidationError, match="cannot target entity_type"):
        validate_plan_bundle(bundle, _context())


def test_rejects_district_without_target_coordinates():
    bundle = PlanBundle(
        summary="missing district placement",
        tasks=[
            ProposedTask(
                task_id="bad-district",
                action_type="city_set_production",
                entity_type="city",
                entity_id=1,
                due_turn=10,
                arguments={
                    "city_id": 1,
                    "item_type": "DISTRICT",
                    "item_name": "DISTRICT_CAMPUS",
                },
                postconditions=[
                    {
                        "type": "city_production_equals",
                        "city_id": 1,
                        "item_name": "DISTRICT_CAMPUS",
                    }
                ],
                reason="invalid test",
            )
        ],
    )

    with pytest.raises(PlanValidationError, match="requires target_x and target_y"):
        validate_plan_bundle(bundle, _context())


def test_rejects_research_task_that_can_override_active_choice():
    bundle = PlanBundle(
        summary="unsafe research selection",
        tasks=[
            ProposedTask(
                task_id="unsafe-research",
                action_type="set_research",
                entity_type="research",
                entity_id="TECH_MINING",
                due_turn=10,
                arguments={"tech_or_civic": "TECH_MINING"},
                preconditions=[
                    {"type": "research_available", "tech_type": "TECH_MINING"}
                ],
                postconditions=[
                    {"type": "research_equals", "tech_type": "TECH_MINING"}
                ],
                reason="must not replace active research",
            )
        ],
    )

    with pytest.raises(PlanValidationError, match="research_unselected"):
        validate_plan_bundle(bundle, _context())


def test_accepts_complete_research_contract():
    bundle = PlanBundle(
        summary="safe research selection",
        tasks=[
            ProposedTask(
                task_id="safe-research",
                action_type="set_research",
                entity_type="research",
                entity_id="TECH_MINING",
                due_turn=10,
                arguments={"tech_or_civic": "TECH_MINING"},
                preconditions=[
                    {"type": "research_unselected"},
                    {"type": "research_available", "tech_type": "TECH_MINING"},
                ],
                postconditions=[
                    {"type": "research_equals", "tech_type": "TECH_MINING"}
                ],
                reason="select approved available research",
            )
        ],
    )

    validate_plan_bundle(bundle, _context())


def test_rejects_civic_postcondition_for_different_target():
    bundle = PlanBundle(
        summary="unsafe civic verification",
        tasks=[
            ProposedTask(
                task_id="unsafe-civic",
                action_type="set_civic",
                entity_type="civic",
                entity_id="CIVIC_CODE_OF_LAWS",
                due_turn=10,
                arguments={"tech_or_civic": "CIVIC_CODE_OF_LAWS"},
                preconditions=[
                    {"type": "civic_unselected"},
                    {
                        "type": "civic_available",
                        "civic_type": "CIVIC_CODE_OF_LAWS",
                    },
                ],
                postconditions=[
                    {
                        "type": "civic_equals",
                        "civic_type": "CIVIC_CRAFTSMANSHIP",
                    }
                ],
                reason="mismatched verification target",
            )
        ],
    )

    with pytest.raises(PlanValidationError, match="CIVIC_CODE_OF_LAWS"):
        validate_plan_bundle(bundle, _context())
