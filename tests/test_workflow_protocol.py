import pytest

from civ6_workflow.models import (
    EventLevel,
    GameEvent,
    PlanBundle,
    ProposedTask,
    RiskLevel,
)
from civ6_workflow.workflow_protocol import (
    EventResolution,
    InformationRequest,
    ResolutionDisposition,
    WorkflowProtocolError,
    validate_event_resolution_contract,
)


def _blocking_event():
    return GameEvent(
        event_type="settler_site_selection_required",
        turn=10,
        entity_type="unit",
        entity_id=7,
        level=EventLevel.L3,
        risk=RiskLevel.HIGH,
        blocking=True,
        dedupe_key="settler:7:10",
    )


def test_blocking_event_requires_explicit_resolution():
    bundle = PlanBundle(summary="empty but syntactically valid")
    with pytest.raises(WorkflowProtocolError, match="no resolution"):
        validate_event_resolution_contract(
            bundle,
            [_blocking_event()],
            known_task_ids=set(),
            allow_information_requests=False,
        )


def test_information_phase_cannot_also_create_tasks():
    event = _blocking_event()
    info = InformationRequest(
        request_id="info-1",
        event_dedupe_key=event.dedupe_key,
        query_type="settler_select_site",
        tool_name="get_settle_advisor",
        arguments={"unit_id": 7},
        purpose="rank sites",
    )
    bundle = PlanBundle(
        summary="mixed phase",
        information_requests=[info],
        event_resolutions=[
            EventResolution(
                event_dedupe_key=event.dedupe_key,
                disposition=ResolutionDisposition.INFORMATION_REQUIRED,
                information_request_ids=[info.request_id],
                reason="need advisor",
            )
        ],
        tasks=[
            ProposedTask(
                task_id="skip",
                action_type="unit_skip",
                entity_type="unit",
                entity_id=7,
                due_turn=10,
                arguments={"unit_id": 7},
                postconditions=[{"type": "unit_no_moves", "unit_id": 7}],
                reason="invalid mixed phase",
            )
        ],
    )
    with pytest.raises(WorkflowProtocolError, match="cannot also mutate"):
        validate_event_resolution_contract(
            bundle,
            [event],
            known_task_ids=set(),
            allow_information_requests=True,
        )


def test_settler_plan_update_closes_event_contract():
    event = _blocking_event()
    bundle = PlanBundle(
        summary="select site",
        unit_plan_updates=[
            {"unit_id": 7, "goal": "found_city", "target": {"x": 5, "y": 6}}
        ],
        event_resolutions=[
            EventResolution(
                event_dedupe_key=event.dedupe_key,
                disposition=ResolutionDisposition.PLAN_UPDATE,
                plan_refs=["unit:7"],
                reason="advisor candidate selected",
            )
        ],
    )
    validate_event_resolution_contract(
        bundle,
        [event],
        known_task_ids=set(),
        allow_information_requests=False,
    )


def test_found_city_requires_consumed_unit_and_new_city_proof():
    event = _blocking_event()
    task = ProposedTask(
        task_id="found-7",
        action_type="unit_found_city",
        entity_type="unit",
        entity_id=7,
        due_turn=10,
        arguments={"unit_id": 7},
        preconditions=[
            {"type": "entity_exists", "entity_type": "unit", "entity_id": 7},
            {"type": "unit_type_contains", "unit_id": 7, "marker": "SETTLER"},
            {"type": "unit_at", "unit_id": 7, "x": 5, "y": 6},
        ],
        postconditions=[
            {"type": "unit_absent", "unit_id": 7},
            {"type": "city_count_at_least", "count": 2},
        ],
        risk=RiskLevel.HIGH,
        requires_confirmation=True,
        reason="found approved city",
    )
    bundle = PlanBundle(
        summary="found city",
        tasks=[task],
        event_resolutions=[
            EventResolution(
                event_dedupe_key=event.dedupe_key,
                disposition=ResolutionDisposition.TASK,
                task_ids=[task.task_id],
                reason="execute approved site",
            )
        ],
    )
    validate_event_resolution_contract(
        bundle,
        [event],
        known_task_ids=set(),
        allow_information_requests=False,
    )
