import asyncio
import json

import pytest

from civ6_workflow.models import EventLevel, GameEvent, RiskLevel
from civ6_workflow.workflow_protocol import (
    READ_ONLY_QUERY_SPECS,
    InformationRequest,
    WorkflowProtocolError,
    information_tool_argument_contracts,
)
from civ6_workflow.workflow_queries import InformationQueryRouter


class FakeGame:
    def __init__(self):
        self.calls = []

    async def query_tool(self, name, arguments):
        self.calls.append((name, arguments))
        return {"text": "#1 (5,6): Score 80"}


def test_settler_event_prefetches_focused_advisor():
    event = GameEvent(
        event_type="settler_site_selection_required",
        turn=10,
        entity_type="unit",
        entity_id=7,
        level=EventLevel.L3,
        risk=RiskLevel.HIGH,
        blocking=True,
        dedupe_key="settler:7:10",
    )
    game = FakeGame()
    router = InformationQueryRouter(game)
    requests = router.prefetch([event])
    assert len(requests) == 1
    assert requests[0].query_type == "settler_select_site"
    results = asyncio.run(router.execute(requests))
    assert game.calls == [("get_settle_advisor", {"unit_id": 7})]
    assert results[requests[0].request_id]["result"]["text"].startswith("#1")


def test_non_read_only_query_is_rejected():
    router = InformationQueryRouter(FakeGame())
    request = InformationRequest(
        event_dedupe_key="event",
        query_type="bad",
        tool_name="unit_action",
        arguments={"unit_id": 7, "action": "found_city"},
        purpose="must not execute",
    )
    with pytest.raises(WorkflowProtocolError, match="non-whitelisted"):
        asyncio.run(router.execute([request]))


def test_information_tool_argument_contracts_are_stable_and_complete():
    contracts = information_tool_argument_contracts()

    assert list(contracts) == sorted(contracts)
    assert set(contracts) == set(READ_ONLY_QUERY_SPECS)
    assert contracts["get_map_area"] == {
        "required": ["center_x", "center_y"],
        "optional": ["radius"],
    }
    for contract in contracts.values():
        assert contract["required"] == sorted(contract["required"])
        assert contract["optional"] == sorted(contract["optional"])

    first = json.dumps(contracts, separators=(",", ":"))
    second = json.dumps(
        information_tool_argument_contracts(), separators=(",", ":")
    )
    assert first == second


def test_information_tool_argument_contracts_are_defensive_copies():
    first = information_tool_argument_contracts()
    first["get_map_area"]["required"].append("pollution")

    second = information_tool_argument_contracts()
    assert second["get_map_area"]["required"] == ["center_x", "center_y"]
    assert READ_ONLY_QUERY_SPECS["get_map_area"].required_arguments == frozenset(
        {"center_x", "center_y"}
    )
