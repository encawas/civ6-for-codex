import json

import pytest

from civ6_workflow.actions import ACTION_REGISTRY
from civ6_workflow.models import PlanBundle, ProposedTask
from civ6_workflow.validation import (
    ACTION_CONDITION_CONTRACTS,
    ACTION_ENTITY_TYPES,
    ENTITY_ID_ARGUMENTS,
    PlanValidationContext,
    PlanValidationError,
    action_entity_type_contracts,
    condition_contracts,
    entity_id_argument_contracts,
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


def test_entity_id_contracts_match_action_registry_requirements():
    entity_types = action_entity_type_contracts()
    entity_ids = entity_id_argument_contracts(entity_types)

    assert list(entity_types) == sorted(entity_types)
    assert list(entity_ids) == sorted(entity_ids)
    assert entity_ids == dict(ENTITY_ID_ARGUMENTS)
    for action_type, allowed_entities in entity_types.items():
        assert allowed_entities == sorted(allowed_entities)
        spec = ACTION_REGISTRY[action_type]
        for entity_type in allowed_entities:
            assert entity_ids[entity_type] in spec.required_arguments
    assert json.dumps(entity_types, separators=(",", ":")) == json.dumps(
        action_entity_type_contracts(), separators=(",", ":")
    )


def test_action_entity_contract_projection_rejects_missing_definition(monkeypatch):
    monkeypatch.delitem(ACTION_ENTITY_TYPES, "set_research")

    with pytest.raises(PlanValidationError, match="missing entity type contracts"):
        action_entity_type_contracts({"set_research"})


def test_entity_id_contract_projection_rejects_missing_mapping():
    with pytest.raises(PlanValidationError, match="no ID argument contract"):
        entity_id_argument_contracts({"set_research": ["unknown_entity"]})


def test_condition_contract_projection_is_stable_filtered_and_defensive():
    contracts = condition_contracts({"set_research", "city_set_production"})

    assert list(contracts) == ["set_research"]
    assert list(contracts["set_research"]) == sorted(contracts["set_research"])
    for conditions in contracts["set_research"].values():
        for condition in conditions:
            assert list(condition) == sorted(condition)
    first_json = json.dumps(contracts, separators=(",", ":"))
    second_json = json.dumps(
        condition_contracts({"set_research", "city_set_production"}),
        separators=(",", ":"),
    )
    assert first_json == second_json
    assert "city_set_production" not in contracts
    first = contracts["set_research"]["required_preconditions"]
    first[0]["type"] = "pollution"

    second = condition_contracts({"set_research"})
    assert second["set_research"]["required_preconditions"][0] == {
        "type": "research_unselected"
    }
    assert ACTION_CONDITION_CONTRACTS["set_research"][
        "required_preconditions"
    ][0] == {"type": "research_unselected"}


def test_action_condition_contracts_are_recursively_immutable():
    with pytest.raises(TypeError):
        ACTION_CONDITION_CONTRACTS["set_research"][
            "required_preconditions"
        ][0]["type"] = "anything"

    validate_plan_bundle(
        PlanBundle(
            summary="unchanged research contract",
            tasks=[
                ProposedTask(
                    task_id="immutable-safe-research",
                    action_type="set_research",
                    entity_type="research",
                    entity_id="TECH_MINING",
                    due_turn=10,
                    arguments={"tech_or_civic": "TECH_MINING"},
                    preconditions=[
                        {"type": "research_unselected"},
                        {
                            "type": "research_available",
                            "tech_type": "TECH_MINING",
                        },
                    ],
                    postconditions=[
                        {
                            "type": "research_equals",
                            "tech_type": "TECH_MINING",
                        }
                    ],
                    reason="verify immutable canonical contract",
                )
            ],
        ),
        _context(),
    )


def test_accepts_complete_civic_contract_rendered_from_shared_template():
    bundle = PlanBundle(
        summary="safe civic selection",
        tasks=[
            ProposedTask(
                task_id="safe-civic",
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
                        "civic_type": "CIVIC_CODE_OF_LAWS",
                    }
                ],
                reason="select approved available civic",
            )
        ],
    )

    validate_plan_bundle(bundle, _context())


def test_rejects_missing_required_research_postcondition():
    task = ProposedTask(
        task_id="missing-research-postcondition",
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
            {"type": "field_equals", "path": "overview.turn", "value": 10}
        ],
        reason="missing contract postcondition",
    )

    with pytest.raises(PlanValidationError, match="missing required postcondition"):
        validate_plan_bundle(PlanBundle(summary="invalid", tasks=[task]), _context())


def test_rejects_condition_type_alias_instead_of_type():
    task = ProposedTask(
        task_id="bad-condition-discriminator",
        action_type="set_research",
        entity_type="research",
        entity_id="TECH_MINING",
        due_turn=10,
        arguments={"tech_or_civic": "TECH_MINING"},
        preconditions=[
            {"condition_type": "research_unselected"},
            {"type": "research_available", "tech_type": "TECH_MINING"},
        ],
        postconditions=[{"type": "research_equals", "tech_type": "TECH_MINING"}],
        reason="invalid discriminator",
    )

    with pytest.raises(PlanValidationError, match="unsupported precondition type"):
        validate_plan_bundle(PlanBundle(summary="invalid", tasks=[task]), _context())


def test_rejects_unresolved_condition_contract_placeholder():
    task = ProposedTask(
        task_id="unresolved-placeholder",
        action_type="set_research",
        entity_type="research",
        entity_id="TECH_MINING",
        due_turn=10,
        arguments={"tech_or_civic": "TECH_MINING"},
        preconditions=[
            {"type": "research_unselected"},
            {"type": "research_available", "tech_type": "$tech_or_civic"},
        ],
        postconditions=[
            {"type": "research_equals", "tech_type": "$tech_or_civic"}
        ],
        reason="unresolved contract placeholder",
    )

    with pytest.raises(PlanValidationError, match="unresolved contract placeholder"):
        validate_plan_bundle(PlanBundle(summary="invalid", tasks=[task]), _context())


def test_missing_template_argument_has_clear_validation_error():
    task = ProposedTask(
        task_id="missing-template-argument",
        action_type="set_research",
        entity_type="research",
        entity_id="TECH_MINING",
        due_turn=10,
        arguments={},
        preconditions=[{"type": "research_unselected"}],
        postconditions=[{"type": "research_equals", "tech_type": "TECH_MINING"}],
        reason="missing contract argument",
    )

    with pytest.raises(
        PlanValidationError, match="condition contract references missing task argument"
    ):
        validate_plan_bundle(PlanBundle(summary="invalid", tasks=[task]), _context())
