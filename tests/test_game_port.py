import asyncio

from civ6_workflow.mcp_port import Civ6GamePort


class FakeMcpClient:
    def __init__(self):
        self.call_count = 0
        self.calls = []

    async def list_tools(self):
        return {"set_city_production", "unit_action", "end_turn"}

    async def call_tool(self, name, arguments=None):
        self.call_count += 1
        self.calls.append((name, arguments or {}))
        return {"text": "ok"}


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
    return Civ6GamePort(
        mcp,
        state,
        allowed_tools={"set_city_production", "unit_action", "end_turn"},
    ), mcp, state


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
        assert snapshot.blockers == [
            {"type": "city_no_production", "city_ids": ["2"]}
        ]
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
