import asyncio

from civ6_workflow.mcp_port import Civ6GamePort


class _Client:
    call_count = 0


class _StateApi:
    def __init__(self, blocking_type: str):
        self.call_count = 0
        self.blocking_type = blocking_type
        self.paths: list[str] = []

    async def get_optional(self, path: str):
        self.call_count += 1
        self.paths.append(path)
        return {
            "identity": {"civ": "China", "seed": 123},
            "overview": {"turn": 8},
            "tech_civics": {},
            "cities": [],
            "units": None,
            "notifications": [],
            "end_turn_blockers": [
                {"blocking_type": self.blocking_type, "message": "blocked"}
            ],
            "pending_diplomacy": [],
            "pending_trades": [],
        }

    async def get(self, path: str):
        self.call_count += 1
        self.paths.append(path)
        assert path == "/api/units"
        return [{"unit_id": 7, "unit_type": "UNIT_SCOUT", "moves_remaining": 2}]


class _ZeroCityStateApi(_StateApi):
    async def get_optional(self, path: str):
        payload = await super().get_optional(path)
        payload["overview"]["num_cities"] = 0
        payload["end_turn_blockers"] = []
        return payload


def test_zero_city_start_expands_units_without_reported_blocker():
    state = _ZeroCityStateApi("")
    port = Civ6GamePort(_Client(), state, allowed_tools=set())

    snapshot = asyncio.run(port.read_snapshot(include_units=False))

    assert snapshot.units == [
        {"unit_id": 7, "unit_type": "UNIT_SCOUT", "moves_remaining": 2}
    ]
    assert state.paths == [
        "/api/workflow/snapshot?include_units=false",
        "/api/units",
    ]


def test_unit_blocker_expands_snapshot_units():
    state = _StateApi("ENDTURN_BLOCKING_UNITS")
    port = Civ6GamePort(_Client(), state, allowed_tools=set())

    snapshot = asyncio.run(port.read_snapshot(include_units=False))

    assert snapshot.units == [
        {"unit_id": 7, "unit_type": "UNIT_SCOUT", "moves_remaining": 2}
    ]
    assert state.paths == [
        "/api/workflow/snapshot?include_units=false",
        "/api/units",
    ]


def test_non_unit_blocker_keeps_compact_snapshot():
    state = _StateApi("ENDTURN_BLOCKING_RESEARCH")
    port = Civ6GamePort(_Client(), state, allowed_tools=set())

    snapshot = asyncio.run(port.read_snapshot(include_units=False))

    assert snapshot.units is None
    assert state.paths == ["/api/workflow/snapshot?include_units=false"]
