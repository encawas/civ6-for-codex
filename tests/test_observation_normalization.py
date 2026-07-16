import asyncio
from pathlib import Path

import pytest

from civ6_workflow.domain import (
    NORMALIZATION_VERSION,
    SlotState,
    UnitActionState,
    UnitDetailReason,
    thaw_json,
)
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ExecutionMode,
    PlanBundle,
    RuntimeSnapshot,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.progression import ProgressionRuleCompiler
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore


EMPTY_PRODUCTION_VALUES = [
    "nothing",
    "NOTHING",
    "none",
    "NONE",
    "",
    "   ",
    None,
    {},
    [],
]


def _city_snapshot(production, *, turn: int = 10) -> RuntimeSnapshot:
    return RuntimeSnapshot(
        turn=turn,
        game_id="game-1",
        overview={"turn": turn, "num_cities": 1},
        cities=[{"city_id": 1, "currently_building": production}],
        tech_civics={
            "current_research": None,
            "current_civic": None,
            "available_techs": [
                {"name": "Mining", "tech_type": "TECH_MINING"},
                {"name": "Pottery", "tech_type": "TECH_POTTERY"},
            ],
            "available_civics": [
                {
                    "name": "Code of Laws",
                    "civic_type": "CIVIC_CODE_OF_LAWS",
                }
            ],
        },
    )


def _save_city_plan(store: WorkflowStore) -> None:
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(
            plan_id="city-plan",
            summary="continue city production",
            city_plan_updates=[
                {
                    "city_id": 1,
                    "followup_queue": [
                        {
                            "item_type": "BUILDING",
                            "item_name": "BUILDING_MONUMENT",
                        }
                    ],
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"city_set_production"},
    )


def _save_research_plan(store: WorkflowStore) -> None:
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(
            plan_id="research-plan",
            summary="continue research queue",
            strategy_updates={"research_queue": ["TECH_POTTERY"]},
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"set_research"},
    )


@pytest.mark.parametrize("raw_production", EMPTY_PRODUCTION_VALUES)
def test_obs_001_empty_production_variants_use_one_vertical_boundary(
    tmp_path: Path,
    raw_production,
):
    """OBS-001: every upstream empty spelling materializes the same task."""

    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    _save_city_plan(store)
    raw_snapshot = _city_snapshot(raw_production)
    observation = normalize_runtime_snapshot(raw_snapshot)

    raw_audit = thaw_json(observation.canonical.raw_observation)
    assert raw_audit["cities"][0]["currently_building"] == raw_production
    assert observation.canonical.cities[0].production.state is SlotState.EMPTY
    assert observation.snapshot.cities[0]["currently_building"] is None

    compiled = DeterministicRuleCompiler(store).compile(observation)

    assert compiled.bundle is not None
    assert [task.action_type for task in compiled.bundle.tasks] == [
        "city_set_production"
    ]


@pytest.mark.parametrize(
    "production",
    [
        "UNIT_BUILDER",
        "BUILDING_MONUMENT",
        "DISTRICT_CAMPUS",
        "PROJECT_CAMPUS_RESEARCH_GRANTS",
        "BUILDING_PYRAMIDS",
        "MODDED_VALID_PROJECT",
    ],
)
def test_obs_002_occupied_production_is_never_classified_as_empty(
    tmp_path: Path,
    production: str,
):
    """OBS-002: any non-empty production identifier remains occupied."""

    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    _save_city_plan(store)
    observation = normalize_runtime_snapshot(_city_snapshot(production))

    assert observation.canonical.cities[0].production.state is SlotState.OCCUPIED
    assert observation.canonical.cities[0].production.value == production
    assert DeterministicRuleCompiler(store).compile(observation).bundle is None


def test_obs_003_raw_payload_is_audit_only_and_rules_have_no_empty_spelling_list():
    """OBS-003: representation quirks are owned only by the boundary."""

    repository = Path(__file__).parents[1]
    rule_files = [
        repository / "src" / "civ6_workflow" / "rules.py",
        repository / "src" / "civ6_workflow" / "safe_rules.py",
        repository / "src" / "civ6_workflow" / "progression.py",
    ]

    for path in rule_files:
        source = path.read_text(encoding="utf-8").casefold()
        assert "nothing" not in source
        assert "empty_slot_strings" not in source

    boundary = (
        repository / "src" / "civ6_workflow" / "domain" / "observations.py"
    ).read_text(encoding="utf-8")
    assert boundary.count("EMPTY_SLOT_STRINGS") == 2


def test_obs_003_rule_compilers_reject_raw_runtime_snapshots(tmp_path: Path):
    """OBS-003: rules accept the normalized boundary type, not raw snapshots."""

    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    raw_snapshot = _city_snapshot("nothing")

    with pytest.raises(AttributeError):
        DeterministicRuleCompiler(store).compile(raw_snapshot)  # type: ignore[arg-type]
    with pytest.raises(AttributeError):
        ProgressionRuleCompiler(store).compile(raw_snapshot)  # type: ignore[arg-type]


def test_obs_006_normalization_identity_is_unique_and_projection_is_stable():
    """OBS-006: equal facts get unique observation IDs and equal hashes."""

    first = normalize_runtime_snapshot(_city_snapshot("NONE")).canonical
    second = normalize_runtime_snapshot(_city_snapshot("nothing")).canonical

    assert first.normalization_version == NORMALIZATION_VERSION
    assert first.normalization_version == "civ6-observation/v1"
    assert first.observation_id != second.observation_id
    assert first.projection_hash == second.projection_hash


def test_normalized_values_cover_progression_units_blockers_and_identifiers():
    raw = RuntimeSnapshot(
        turn=3,
        game_id="game-typed",
        overview={"turn": 3, "num_units": 1},
        cities=[{"city_id": " 11 ", "currently_building": "UNIT_SCOUT"}],
        tech_civics={
            "current_research": "tech_mining",
            "current_civic": "Code of Laws",
            "available_techs": [{"name": "Mining", "tech_type": "TECH_MINING"}],
            "available_civics": [
                {
                    "name": "Code of Laws",
                    "civic_type": "CIVIC_CODE_OF_LAWS",
                }
            ],
        },
        units=[
            {
                "unit_id": " 7 ",
                "unit_type": "unit_settler",
                "moves_remaining": "2",
                "x": "4",
                "y": 5,
            }
        ],
        blockers=[
            {
                "type": "END_TURN_BLOCKER",
                "blocking_type": "endturn_blocking_units",
            }
        ],
    )

    observation = normalize_runtime_snapshot(raw).canonical

    assert observation.cities[0].entity_id.value == "11"
    assert observation.progression.current_research.value == "TECH_MINING"
    assert observation.progression.current_civic.value == "CIVIC_CODE_OF_LAWS"
    assert observation.units is not None
    assert observation.units[0].entity_id.value == "7"
    assert observation.units[0].unit_type == "UNIT_SETTLER"
    assert observation.units[0].action_state is UnitActionState.ACTIONABLE
    assert observation.blockers[0].source_type == "end_turn_blocker"
    assert observation.blockers[0].blocker_type == "ENDTURN_BLOCKING_UNITS"


@pytest.mark.parametrize(
    "empty_research", [None, "", "  ", "none", "NONE", "NoThInG", {}, []]
)
def test_plan_002_empty_research_allows_queue_materialization(
    tmp_path: Path,
    empty_research,
):
    """PLAN-002: an empty normalized research slot permits queue continuation."""

    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    _save_research_plan(store)
    snapshot = _city_snapshot("UNIT_SCOUT")
    snapshot.tech_civics["current_research"] = empty_research

    compiled = ProgressionRuleCompiler(store).compile(
        normalize_runtime_snapshot(snapshot)
    )

    assert compiled.bundle is not None
    assert [task.action_type for task in compiled.bundle.tasks] == ["set_research"]


@pytest.mark.parametrize(
    "current_research",
    ["TECH_MINING", "tech_mining", "Mining", " mining "],
)
def test_plan_002_occupied_research_suppresses_queue_materialization(
    tmp_path: Path,
    current_research: str,
):
    """PLAN-002: an occupied research slot cannot create a replacement task."""

    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    _save_research_plan(store)
    snapshot = _city_snapshot("UNIT_SCOUT")
    snapshot.tech_civics["current_research"] = current_research

    compiled = ProgressionRuleCompiler(store).compile(
        normalize_runtime_snapshot(snapshot)
    )

    assert compiled.bundle is None
    assert compiled.events == []


def test_city_production_tick_uses_normalized_observation_boundary(
    tmp_path: Path,
):
    store = WorkflowStore(tmp_path / "vertical.sqlite3")
    _save_city_plan(store)
    game = _ReadPolicyGame(_city_snapshot("nothing"))
    engine = WorkflowEngine(
        store=store,
        game=game,
        planner=_NoPlanner(),
        config=EngineConfig(
            execution_mode=ExecutionMode.CONFIRM,
            auto_end_turn=False,
            max_agent_calls_per_turn=0,
            auto_action_types={"city_set_production"},
            allowed_action_types={"city_set_production"},
        ),
    )

    result = asyncio.run(engine.tick())

    tasks = store.list_tasks("game-1")
    assert [task.action_type for task in tasks] == ["city_set_production"]
    assert result.metrics.normalization_seconds > 0
    assert game.read_requests == [False]


class _NoPlanner:
    calls = 0

    async def plan(self, request):
        self.calls += 1
        return PlanBundle(summary="unexpected planner call")


class _ReadPolicyGame:
    def __init__(self, snapshot: RuntimeSnapshot):
        self.snapshot = snapshot
        self.read_requests: list[bool] = []
        self.call_count = 0

    async def read_snapshot(self, *, include_units: bool = False):
        self.call_count += 1
        self.read_requests.append(include_units)
        snapshot = self.snapshot.model_copy(deep=True)
        if not include_units:
            snapshot.units = None
        return snapshot

    async def execute_task(self, task):
        raise AssertionError("read-only policy test must not mutate the game")

    async def end_turn(self):
        raise AssertionError("read-only policy test must not end the turn")

    async def list_tools(self):
        return {
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
        }


def _policy_engine(tmp_path: Path, snapshot: RuntimeSnapshot):
    game = _ReadPolicyGame(snapshot)
    engine = WorkflowEngine(
        store=WorkflowStore(tmp_path / "workflow.sqlite3"),
        game=game,
        planner=_NoPlanner(),
        config=EngineConfig(
            execution_mode=ExecutionMode.READONLY,
            auto_end_turn=False,
            max_agent_calls_per_turn=0,
        ),
    )
    return engine, game


def test_obs_004_unit_blocker_requests_detail_before_unit_routing(tmp_path: Path):
    """OBS-004: a light unit blocker observation upgrades to unit detail."""

    snapshot = _city_snapshot("UNIT_SCOUT")
    snapshot.units = [
        {
            "unit_id": 9,
            "unit_type": "UNIT_WARRIOR",
            "moves_remaining": 2,
        }
    ]
    snapshot.blockers = [
        {
            "type": "end_turn_blocker",
            "blocking_type": "ENDTURN_BLOCKING_UNITS",
        }
    ]
    light = normalize_runtime_snapshot(snapshot.model_copy(update={"units": None}))
    assert light.canonical.unit_summary.detail_reasons == (
        UnitDetailReason.UNIT_BLOCKER,
    )
    assert light.canonical.unit_summary.detail_required is True
    engine, game = _policy_engine(tmp_path, snapshot)

    asyncio.run(engine.tick())

    assert game.read_requests == [False, True]


def test_obs_005_zero_city_without_blocker_still_discovers_settler(tmp_path: Path):
    """OBS-005: zero cities trigger enough detail to discover a settler."""

    snapshot = RuntimeSnapshot(
        turn=1,
        game_id="opening",
        overview={"turn": 1, "num_cities": 0, "num_units": 1},
        cities=[],
        units=[
            {
                "unit_id": 7,
                "unit_type": "UNIT_SETTLER",
                "moves_remaining": 2,
                "x": 4,
                "y": 5,
            }
        ],
        blockers=[],
    )
    light = normalize_runtime_snapshot(snapshot.model_copy(update={"units": None}))
    assert UnitDetailReason.ZERO_CITIES in (light.canonical.unit_summary.detail_reasons)
    assert light.canonical.unit_summary.detail_required is True
    engine, game = _policy_engine(tmp_path, snapshot)

    result = asyncio.run(engine.tick())

    assert game.read_requests == [False, True]
    assert any(
        event.event_type == "settler_site_selection_required" for event in result.events
    )


def test_ordinary_tick_does_not_unconditionally_read_unit_detail(tmp_path: Path):
    snapshot = _city_snapshot("UNIT_SCOUT")
    snapshot.units = [
        {
            "unit_id": 9,
            "unit_type": "UNIT_WARRIOR",
            "moves_remaining": 2,
        }
    ]
    engine, game = _policy_engine(tmp_path, snapshot)

    asyncio.run(engine.tick())

    assert game.read_requests == [False]
    observation = normalize_runtime_snapshot(
        snapshot.model_copy(update={"units": None})
    )
    assert observation.canonical.unit_summary.detail_reasons == ()
    assert UnitDetailReason.ZERO_CITIES not in (
        observation.canonical.unit_summary.detail_reasons
    )
