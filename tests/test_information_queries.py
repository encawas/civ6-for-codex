import asyncio
import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from civ6_workflow.domain import (
    InformationRound,
    InformationRoundStatus,
)
from civ6_workflow.workflow_protocol import (
    MAX_INFORMATION_QUERIES_PER_ROUND,
    READ_ONLY_QUERY_SPECS,
    InformationRequest,
    StrategicResearchProposalResponse,
    WorkflowProtocolError,
    information_tool_argument_contracts,
    materialize_information_requests,
    validate_information_request,
)
from civ6_workflow.workflow_queries import InformationQueryRouter


def _request(tool_name="get_policies", *, request_id="info-test", arguments=None):
    return InformationRequest(
        request_id=request_id,
        event_dedupe_key="strategic-research-context",
        query_type=tool_name,
        tool_name=tool_name,
        arguments=arguments or {},
        purpose="Collect focused strategic evidence.",
    )


class FakeGame:
    def __init__(self, results=None):
        self.calls = []
        self.list_calls = 0
        self.results = results or {}

    async def list_tools(self):
        self.list_calls += 1
        return {"get_policies", "get_city_states", "get_map_area"}

    async def query_tool(self, name, arguments):
        self.calls.append((name, arguments))
        result = self.results.get(name, {"text": "available"})
        if isinstance(result, Exception):
            raise result
        return result


def test_batch_lists_tools_once_and_preserves_structured_results():
    game = FakeGame(
        {
            "get_policies": {"policies": [{"id": "b"}, {"id": "a"}]},
            "get_city_states": RuntimeError("sidecar rejected query"),
        }
    )
    router = InformationQueryRouter(game)
    requests = [
        _request("get_policies", request_id="info-a"),
        _request("get_city_states", request_id="info-b"),
    ]

    results = asyncio.run(router.execute(requests))

    assert game.list_calls == 1
    assert game.calls == [
        ("get_policies", {}),
        ("get_city_states", {}),
    ]
    assert results["info-a"]["status"] == "SUCCEEDED"
    assert results["info-a"]["result"]["policies"] == [{"id": "a"}, {"id": "b"}]
    assert results["info-b"]["status"] == "FAILED"
    assert results["info-b"]["failure_category"] == "query_rejected"


def test_transient_failure_is_retried_only_to_the_bound():
    class TransientGame(FakeGame):
        async def query_tool(self, name, arguments):
            self.calls.append((name, arguments))
            if len(self.calls) == 1:
                raise ConnectionError("sidecar reconnecting")
            return {"ok": True}

    game = TransientGame()
    result = asyncio.run(
        InformationQueryRouter(game, transient_attempts=2).execute([_request()])
    )["info-test"]

    assert result["status"] == "SUCCEEDED"
    assert result["attempt_count"] == 2
    assert len(game.calls) == 2


def test_unavailable_tool_is_recorded_without_calling_it():
    game = FakeGame()
    result = asyncio.run(
        InformationQueryRouter(game).execute([_request("get_religion_beliefs")])
    )["info-test"]

    assert result["status"] == "FAILED"
    assert result["failure_category"] == "tool_unavailable"
    assert result["attempt_count"] == 0
    assert game.calls == []


def test_none_and_oversized_results_are_explicit_failures():
    none_game = FakeGame({"get_policies": None})
    none_result = asyncio.run(InformationQueryRouter(none_game).execute([_request()]))[
        "info-test"
    ]
    assert none_result["status"] == "FAILED"
    assert none_result["message"] == "information query returned no result"

    large_game = FakeGame({"get_policies": {"text": "x" * (70 * 1024)}})
    large_result = asyncio.run(
        InformationQueryRouter(large_game).execute([_request()])
    )["info-test"]
    assert large_result["status"] == "FAILED"
    assert large_result["failure_category"] == "result_too_large"
    assert "result" not in large_result


def test_materialized_request_identity_is_deterministic_and_order_stable():
    source = [
        _request("get_city_states", request_id=None),
        _request("get_policies", request_id=None),
    ]
    first = materialize_information_requests(
        source, planner_request_id="planner-1", round_number=1
    )
    second = materialize_information_requests(
        list(reversed(source)), planner_request_id="planner-1", round_number=1
    )

    assert first == second
    assert all(item.request_id.startswith("info_") for item in first)
    assert len({item.request_id for item in first}) == 2


@pytest.mark.parametrize("duplicate_kind", ["provider_id", "semantic_query"])
def test_duplicate_information_queries_are_rejected(duplicate_kind):
    if duplicate_kind == "provider_id":
        rows = [
            _request("get_policies", request_id="same"),
            _request("get_city_states", request_id="same"),
        ]
        match = "request_id"
    else:
        rows = [
            _request("get_policies", request_id="one"),
            _request("get_policies", request_id="two"),
        ]
        match = "semantic"

    with pytest.raises(WorkflowProtocolError, match=match):
        materialize_information_requests(
            rows, planner_request_id="planner-1", round_number=1
        )


@pytest.mark.parametrize(
    ("tool_name", "arguments", "message"),
    [
        ("get_map_area", {"center_x": "bad", "center_y": 1}, "integer"),
        ("get_map_area", {"center_x": 1, "center_y": 2, "radius": 0}, "radius"),
        ("get_unit_promotions", {"unit_id": []}, "stable identifier"),
        ("get_district_advisor", {"city_id": 1, "district_type": " "}, "district_type"),
    ],
)
def test_typed_argument_contract_rejects_invalid_values(tool_name, arguments, message):
    with pytest.raises(WorkflowProtocolError, match=message):
        validate_information_request(_request(tool_name, arguments=arguments))


def test_query_type_must_match_tool_name():
    request = _request().model_copy(update={"query_type": "notifications"})
    with pytest.raises(WorkflowProtocolError, match="query_type"):
        validate_information_request(request)


def test_non_read_only_query_is_rejected():
    with pytest.raises(WorkflowProtocolError, match="non-whitelisted"):
        validate_information_request(_request("unit_action"))


def test_information_tool_contracts_filter_actual_tools_and_scope():
    contracts = information_tool_argument_contracts(
        available_tools={"get_policies", "get_map_area", "unit_action"},
        strategic_scope="research",
    )

    assert set(contracts) == {"get_policies"}
    assert contracts["get_policies"]["required"] == []
    assert contracts["get_policies"]["arguments"] == {}


def test_information_tool_argument_contracts_are_stable_and_defensive():
    contracts = information_tool_argument_contracts()
    assert list(contracts) == sorted(contracts)
    assert set(contracts) == set(READ_ONLY_QUERY_SPECS)
    map_contract = contracts["get_map_area"]
    assert map_contract["required"] == ["center_x", "center_y"]
    assert map_contract["optional"] == ["radius"]
    assert map_contract["arguments"]["radius"]["minimum"] == 1

    first_json = json.dumps(contracts, separators=(",", ":"))
    assert first_json == json.dumps(
        information_tool_argument_contracts(), separators=(",", ":")
    )
    contracts["get_map_area"]["required"].append("pollution")
    assert information_tool_argument_contracts()["get_map_area"]["required"] == [
        "center_x",
        "center_y",
    ]


def test_strategic_response_enforces_the_shared_query_limit():
    payload = {
        "schema_version": "strategic-research-proposal-response/v1",
        "information_requests": [
            _request(request_id=None).model_dump(mode="json")
            for _ in range(MAX_INFORMATION_QUERIES_PER_ROUND + 1)
        ],
        "proposal_candidates": [],
    }
    with pytest.raises(ValidationError):
        StrategicResearchProposalResponse.model_validate(payload)


def test_collected_round_result_keys_must_exactly_match_requests():
    request = _request()
    with pytest.raises(ValueError, match="results must match request IDs"):
        InformationRound(
            information_round_id="round-1",
            planner_request_id="planner-1",
            round_number=1,
            status=InformationRoundStatus.COLLECTED,
            requests=(request.model_dump(mode="json"),),
            results={
                "wrong-id": {
                    "information_request_id": "wrong-id",
                    "status": "SUCCEEDED",
                    "result": {},
                }
            },
            requested_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )
