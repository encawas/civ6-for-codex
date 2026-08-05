import asyncio

from civ6_workflow.domain.observations import SlotState, UnitDetailReason
from civ6_workflow.events import events_from_snapshot
from civ6_workflow.mcp_port import Civ6GamePort
from civ6_workflow.observation_normalization import normalize_runtime_snapshot


class FakeMcpClient:
    def __init__(self):
        self.call_count = 0
        self.calls = []

    async def list_tools(self):
        return {"set_city_production", "unit_action", "end_turn"}

    async def call_tool(self, name, arguments=None):
        self.call_count += 1
        self.calls.append((name, arguments or {}))
        results = {
            "set_city_production": "PRODUCING: city",
            "unit_action": "SKIPPED: unit",
            "end_turn": "Turn 10 -> 11",
        }
        return {"result": results[name]}


class FakeStateApi:
    def __init__(self, bundled):
        self.bundled = bundled
        self.call_count = 0
        self.paths = []

    async def get_optional(self, path):
        self.call_count += 1
        self.paths.append(path)
        if path.startswith("/api/workflow/snapshot"):
            return self.bundled
        return None

    async def get(self, path):
        raise AssertionError(f"fallback endpoint should not be used: {path}")


def _port(bundled):
    mcp = FakeMcpClient()
    state = FakeStateApi(bundled)
    return (
        Civ6GamePort(
            mcp,
            state,
            allowed_tools={"set_city_production", "unit_action", "end_turn"},
        ),
        mcp,
        state,
    )


def test_one_bundled_http_read_builds_runtime_snapshot():
    async def scenario():
        port, mcp, state = _port(
            {
                "identity": {"civ": "CIVILIZATION_CHINA", "seed": 1234},
                "overview": {
                    "turn": 25,
                    "player_id": 0,
                    "civ_name": "China",
                    "leader_name": "Yongle",
                },
                "cities": [
                    {"city_id": 1, "currently_building": "UNIT_BUILDER"},
                    {"city_id": 2, "currently_building": "NONE"},
                ],
                "units": None,
                "notifications": [],
                "end_turn_blockers": [],
                "pending_diplomacy": [],
                "pending_trades": [],
            }
        )
        snapshot = await port.read_snapshot(include_units=False)

        assert snapshot.turn == 25
        assert snapshot.game_id == "CIVILIZATION_CHINA:1234"
        assert snapshot.blockers == [{"type": "city_no_production", "city_ids": ["2"]}]
        assert state.paths == ["/api/workflow/snapshot?include_units=false"]
        assert mcp.calls == []

    asyncio.run(scenario())


def test_exact_end_turn_blocker_is_preserved():
    async def scenario():
        port, _, _ = _port(
            {
                "identity": {"civ": "china", "seed": 1234},
                "overview": {"turn": 26},
                "cities": [{"city_id": 1, "currently_building": "UNIT_BUILDER"}],
                "units": None,
                "notifications": [],
                "end_turn_blockers": [
                    {
                        "blocking_type": "ENDTURN_BLOCKING_FILL_CIVIC_SLOT",
                        "message": "Policies must be assigned",
                    }
                ],
                "pending_diplomacy": [],
                "pending_trades": [],
            }
        )
        snapshot = await port.read_snapshot(include_units=False)

        assert snapshot.blockers == [
            {
                "type": "end_turn_blocker",
                "blocking_type": "ENDTURN_BLOCKING_FILL_CIVIC_SLOT",
                "message": "Policies must be assigned",
            }
        ]

    asyncio.run(scenario())


def test_end_turn_forwards_auditable_reflections():
    async def scenario():
        port, mcp, _ = _port({})
        reflections = {
            "tactical": "Turn 10: unit orders complete.",
            "strategic": "One city is producing a builder.",
            "tooling": "No tool errors observed.",
            "planning": "Reobserve after the turn transition.",
            "hypothesis": "No new mandatory blocker will appear.",
        }

        result = await port.end_turn(reflections)

        assert result.success is True
        assert mcp.calls == [("end_turn", reflections)]

    asyncio.run(scenario())


class LegacyStateApi:
    def __init__(self, responses):
        self.responses = responses
        self.call_count = 0
        self.paths = []
        self.availability_epoch = 0

    def _next(self, path):
        value = self.responses[path]
        if path in {"/api/overview", "/api/identity"} and isinstance(value, list):
            if len(value) > 1:
                return value.pop(0)
            return value[0]
        return value

    async def get_optional(self, path):
        self.call_count += 1
        self.paths.append(path)
        if path.startswith("/api/workflow/snapshot"):
            return self.responses.get("__bundled__")
        return self._next(path)

    async def get(self, path):
        self.call_count += 1
        self.paths.append(path)
        return self._next(path)


def _legacy_port(responses):
    mcp = FakeMcpClient()
    state = LegacyStateApi(responses)
    return (
        Civ6GamePort(
            mcp,
            state,
            allowed_tools={"set_city_production", "unit_action", "end_turn"},
        ),
        mcp,
        state,
    )


def _legacy_responses(*, overview, end_turn_blockers=None):
    return {
        "/api/overview": overview,
        "/api/identity": {"civ": "china", "seed": 1234},
        "/api/tech-civics": {},
        "/api/cities": [{"city_id": 1, "currently_building": "UNIT_SCOUT"}],
        "/api/units": [{"unit_id": 7, "unit_type": "UNIT_WARRIOR"}],
        "/api/notifications": [],
        "/api/end-turn-blockers": end_turn_blockers or [],
        "/api/pending-diplomacy": [],
        "/api/pending-trades": [],
    }


def test_bundled_missing_collections_remain_incomplete_not_empty_facts():
    async def scenario():
        port, _, _ = _port(
            {
                "identity": {"civ": "china", "seed": 1234},
                "overview": {"turn": 25},
                "units": [{"unit_id": 9, "unit_type": "UNIT_SETTLER"}],
            }
        )
        snapshot = await port.read_snapshot(include_units=True)
        normalized = normalize_runtime_snapshot(snapshot)

        assert snapshot.cities == []
        assert snapshot.tech_civics_loaded is False
        assert snapshot.cities_loaded is False
        assert snapshot.blockers_loaded is False
        assert normalized.canonical.completeness.cities is False
        assert normalized.canonical.completeness.blockers is False
        assert (
            UnitDetailReason.ZERO_CITIES
            not in normalized.canonical.unit_summary.detail_reasons
        )
        assert not any(
            event.event_type == "settler_site_selection_required"
            for event in events_from_snapshot(snapshot)
        )

    asyncio.run(scenario())


def test_bundled_city_without_production_field_is_not_empty_production():
    async def scenario():
        port, _, _ = _port(
            {
                "identity": {"civ": "china", "seed": 1234},
                "overview": {"turn": 25},
                "tech_civics": {},
                "cities": [{"city_id": 1, "name": "Capital"}],
                "notifications": [],
                "end_turn_blockers": [],
                "pending_diplomacy": [],
                "pending_trades": [],
            }
        )
        snapshot = await port.read_snapshot()
        normalized = normalize_runtime_snapshot(snapshot)

        assert snapshot.blockers == []
        assert snapshot.blockers_loaded is False
        assert normalized.canonical.cities[0].production.state is SlotState.NOT_LOADED

    asyncio.run(scenario())


def test_game_session_id_uses_stable_overview_seed_when_identity_is_missing():
    async def scenario():
        first, _, _ = _port(
            {
                "overview": {"turn": 25, "civ_name": "China", "map_seed": 11},
            }
        )
        second, _, _ = _port(
            {
                "overview": {"turn": 25, "civ_name": "China", "map_seed": 22},
            }
        )

        first_snapshot = await first.read_snapshot()
        second_snapshot = await second.read_snapshot()

        assert first_snapshot.game_id != second_snapshot.game_id
        assert first_snapshot.game_id.startswith("session:map_seed:")

    asyncio.run(scenario())


def test_game_session_id_rejects_weak_identity_without_a_session_discriminator():
    async def scenario():
        port, _, _ = _port(
            {
                "overview": {
                    "turn": 25,
                    "civ_name": "China",
                    "leader_name": "Qin Shi Huang",
                    "player_id": 0,
                }
            }
        )

        try:
            await port.read_snapshot()
        except RuntimeError as exc:
            assert "stable game session identity" in str(exc)
        else:
            raise AssertionError("weak game identity was accepted")

    asyncio.run(scenario())


def test_turn_extraction_does_not_search_nested_history_payloads():
    async def scenario():
        port, _, _ = _port(
            {
                "identity": {"civ": "china", "seed": 1234},
                "overview": {"history": [{"turn": 42}]},
            }
        )

        try:
            await port.read_snapshot()
        except RuntimeError as exc:
            assert "turn number" in str(exc)
        else:
            raise AssertionError("nested historical turn was accepted")

    asyncio.run(scenario())


def test_actionable_list_evaluates_each_item_across_known_flag_spellings():
    assert Civ6GamePort._has_actionable(
        [{"is_action_required": False}, {"blocking": True}]
    )
    assert Civ6GamePort._has_actionable(
        [{"action_required": False}, {"actionRequired": True}]
    )
    assert not Civ6GamePort._has_actionable(
        [{"is_action_required": False}, {"blocking": False}]
    )


def test_legacy_read_retries_the_whole_batch_when_turn_changes_mid_read():
    async def scenario():
        responses = _legacy_responses(
            overview=[
                {"turn": 10},
                {"turn": 11},
                {"turn": 12},
                {"turn": 12},
            ]
        )
        port, mcp, state = _legacy_port(responses)

        snapshot = await port.read_snapshot()

        assert snapshot.turn == 12
        assert state.paths.count("/api/overview") == 4
        assert state.paths.count("/api/workflow/snapshot?include_units=false") == 1
        assert mcp.calls == []

    asyncio.run(scenario())


def test_legacy_snapshot_capability_is_cached_until_state_api_availability_changes():
    async def scenario():
        port, _, state = _legacy_port(_legacy_responses(overview={"turn": 12}))

        await port.read_snapshot()
        await port.read_snapshot()

        assert state.paths.count("/api/workflow/snapshot?include_units=false") == 1

    asyncio.run(scenario())


def test_legacy_unit_upgrade_reads_units_and_rechecks_identity_without_full_reload():
    async def scenario():
        port, _, state = _legacy_port(
            _legacy_responses(
                overview={"turn": 12},
                end_turn_blockers=[
                    {"blocking_type": "ENDTURN_BLOCKING_UNITS", "message": "move"}
                ],
            )
        )

        snapshot = await port.read_snapshot()

        assert snapshot.units == [{"unit_id": 7, "unit_type": "UNIT_WARRIOR"}]
        assert state.paths.count("/api/units") == 1
        assert state.paths.count("/api/overview") == 3

    asyncio.run(scenario())


def test_legacy_read_retries_when_session_identity_changes_mid_read():
    async def scenario():
        responses = _legacy_responses(overview={"turn": 12})
        responses["/api/identity"] = [
            {"civ": "china", "seed": 1},
            {"civ": "china", "seed": 2},
            {"civ": "china", "seed": 3},
            {"civ": "china", "seed": 3},
        ]
        port, _, state = _legacy_port(responses)

        snapshot = await port.read_snapshot()

        assert snapshot.game_id == "china:3"
        assert state.paths.count("/api/overview") == 4

    asyncio.run(scenario())


def test_snapshot_capability_is_reprobed_after_state_api_availability_epoch_changes():
    async def scenario():
        responses = _legacy_responses(overview={"turn": 12})
        port, _, state = _legacy_port(responses)

        await port.read_snapshot()
        state.availability_epoch += 1
        state.responses["__bundled__"] = {
            "identity": {"civ": "china", "seed": 1234},
            "overview": {"turn": 12},
            "tech_civics": {},
            "cities": [{"city_id": 1, "currently_building": "UNIT_SCOUT"}],
            "notifications": [],
            "end_turn_blockers": [],
            "pending_diplomacy": [],
            "pending_trades": [],
        }

        await port.read_snapshot()

        assert state.paths.count("/api/workflow/snapshot?include_units=false") == 2

    asyncio.run(scenario())


def test_bundled_and_legacy_paths_preserve_equivalent_complete_snapshot_facts():
    async def scenario():
        bundled_payload = {
            "identity": {"civ": "china", "seed": 1234},
            "overview": {"turn": 12},
            "tech_civics": {"current_research": "TECH_MINING"},
            "cities": [{"city_id": 1, "currently_building": "UNIT_SCOUT"}],
            "notifications": [],
            "end_turn_blockers": [],
            "pending_diplomacy": [],
            "pending_trades": [],
        }
        bundled_port, _, _ = _port(bundled_payload)
        legacy_payload = _legacy_responses(overview={"turn": 12})
        legacy_payload["/api/tech-civics"] = {"current_research": "TECH_MINING"}
        legacy_port, _, _ = _legacy_port(legacy_payload)

        bundled_snapshot = await bundled_port.read_snapshot()
        legacy_snapshot = await legacy_port.read_snapshot()

        assert bundled_snapshot.model_dump() == legacy_snapshot.model_dump()

    asyncio.run(scenario())


def test_malformed_bundled_collections_and_identity_fail_closed():
    async def scenario():
        cases = [
            {"overview": {"turn": 12}, "identity": {"seed": 1}, "cities": "bad"},
            {"overview": {"turn": 12}, "identity": "bad"},
            {
                "overview": {"turn": 12},
                "identity": {"seed": 1},
                "cities": [],
                "units": "bad",
            },
        ]
        for payload in cases:
            port, _, _ = _port(payload)
            try:
                await port.read_snapshot(include_units=True)
            except RuntimeError:
                continue
            raise AssertionError("malformed bundled payload was accepted")

    asyncio.run(scenario())


def test_wrapped_empty_cities_still_triggers_settler_discovery():
    async def scenario():
        port, _, _ = _port(
            {
                "identity": {"civ": "china", "seed": 1234},
                "overview": {"turn": 25},
                "tech_civics": {},
                "cities": {"cities": []},
                "units": [{"unit_id": 9, "unit_type": "UNIT_SETTLER"}],
                "notifications": [],
                "end_turn_blockers": [],
                "pending_diplomacy": [],
                "pending_trades": [],
            }
        )

        snapshot = await port.read_snapshot(include_units=True)

        assert any(
            event.event_type == "settler_site_selection_required"
            for event in events_from_snapshot(snapshot)
        )

    asyncio.run(scenario())
