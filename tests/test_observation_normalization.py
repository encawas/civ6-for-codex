import asyncio
from pathlib import Path

import pytest

import civ6_workflow.conditions as conditions_module
from civ6_workflow.bootstrap import build_runtime_services
from civ6_workflow.domain import (
    NORMALIZATION_VERSION,
    SlotState,
    UnitActionState,
    UnitDetailReason,
)
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    PlanBundle,
    RuntimeSnapshot,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.progression import ProgressionRuleCompiler
from civ6_workflow.rules import DeterministicRuleCompiler
from civ6_workflow.store import WorkflowStore
from civ6_workflow.conditions import ConditionEvaluator


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


def test_obs_003_raw_payload_is_audit_only_and_rules_have_no_empty_spelling_list():
    """OBS-003: representation quirks are owned only by the boundary."""

    repository = Path(__file__).parents[1]
    rule_files = [
        repository / "src" / "civ6_workflow" / "rules.py",
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

    async def end_turn(self, reflections=None):
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
        service_factory=build_runtime_services,
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


def _count_normalizations(monkeypatch):
    calls = 0
    original = conditions_module.normalize_runtime_snapshot

    def counting_normalize(snapshot):
        nonlocal calls
        calls += 1
        return original(snapshot)

    monkeypatch.setattr(
        conditions_module,
        "normalize_runtime_snapshot",
        counting_normalize,
    )
    return lambda: calls


def test_evaluate_all_normalizes_once_for_multiple_conditions(monkeypatch):
    call_count = _count_normalizations(monkeypatch)
    snapshot = _city_snapshot("nothing")

    result = ConditionEvaluator().evaluate_all(
        [
            {"type": "turn_equals", "turn": 10},
            {
                "type": "field_equals",
                "path": "cities.0.currently_building",
                "value": None,
            },
            {"type": "city_has_no_production", "city_id": 1},
        ],
        snapshot,
    )

    assert result.valid
    assert call_count() == 1


def test_workflow_parent_condition_normalizes_once(monkeypatch):
    call_count = _count_normalizations(monkeypatch)

    result = ConditionEvaluator().evaluate(
        {"type": "turn_equals", "turn": 10},
        _city_snapshot("nothing"),
    )

    assert result.valid
    assert call_count() == 1


def test_entity_exists_does_not_normalize_twice(monkeypatch):
    call_count = _count_normalizations(monkeypatch)

    result = ConditionEvaluator().evaluate(
        {"type": "entity_exists", "entity_type": "city", "entity_id": 1},
        _city_snapshot("nothing"),
    )

    assert result.valid
    assert call_count() == 1


def test_compatibility_projection_preserves_unknown_fields_and_typed_facts():
    raw = RuntimeSnapshot(
        turn=3,
        game_id="game-preserve",
        overview={"turn": 3},
        cities=[
            {
                "city_id": " 11 ",
                "currently_building": "nothing",
                "name": "Capital",
                "population": 4,
                "x": 8,
                "y": 9,
                "owner": "PLAYER_0",
                "extension": {"mod": "city-value"},
            }
        ],
        tech_civics={
            "current_research": None,
            "current_civic": None,
            "available_techs": [
                {
                    "name": " Mining ",
                    "tech_type": "tech_mining",
                    "cost": 25,
                    "extension": {"mod": "tech-value"},
                }
            ],
            "available_civics": [],
        },
        units=[
            {
                "unit_id": " 7 ",
                "unit_type": "unit_settler",
                "x": "4",
                "y": "5",
                "moves_remaining": "2",
                "needs_promotion": 1,
                "targets": [{"x": 6, "y": 7, "score": 3}],
                "build_charges": "1",
                "owner": "PLAYER_0",
                "promotion_options": ["PROMOTION_1"],
                "extension": {"mod": "unit-value"},
            }
        ],
    )

    observation = normalize_runtime_snapshot(raw)
    city = observation.snapshot.cities[0]
    unit = observation.snapshot.units[0]
    tech = observation.snapshot.tech_civics["available_techs"][0]

    assert city["city_id"] == "11"
    assert city["currently_building"] is None
    assert city["name"] == "Capital"
    assert city["population"] == 4
    assert city["x"] == 8
    assert city["y"] == 9
    assert city["owner"] == "PLAYER_0"
    assert city["extension"] == {"mod": "city-value"}

    assert unit["unit_id"] == "7"
    assert unit["unit_type"] == "UNIT_SETTLER"
    assert unit["x"] == 4
    assert unit["y"] == 5
    assert unit["moves_remaining"] == 2.0
    assert unit["needs_promotion"] is True
    assert unit["targets"] == [{"x": 6, "y": 7, "score": 3}]
    assert unit["build_charges"] == 1
    assert unit["owner"] == "PLAYER_0"
    assert unit["promotion_options"] == ["PROMOTION_1"]
    assert unit["extension"] == {"mod": "unit-value"}

    assert tech["tech_type"] == "TECH_MINING"
    assert tech["name"] == "Mining"
    assert tech["cost"] == 25
    assert tech["extension"] == {"mod": "tech-value"}

    canonical = observation.canonical
    assert canonical.city("11").production.state is SlotState.EMPTY
    assert canonical.unit("7").unit_type == "UNIT_SETTLER"
    assert canonical.progression.available_research_ids[0].value == "TECH_MINING"

    raw_audit = canonical.raw_observation
    assert raw_audit["cities"][0]["city_id"] == " 11 "
    assert raw_audit["cities"][0]["currently_building"] == "nothing"
    assert raw_audit["units"][0]["x"] == "4"
    assert raw_audit["tech_civics"]["available_techs"][0]["tech_type"] == "tech_mining"
    with pytest.raises(TypeError):
        raw_audit["cities"][0]["extension"]["mod"] = "changed"


def test_field_conditions_read_normalized_projection_not_raw_audit():
    raw = _city_snapshot("nothing")
    observation = normalize_runtime_snapshot(raw)
    evaluator = ConditionEvaluator()

    assert (
        observation.canonical.raw_observation["cities"][0]["currently_building"]
        == "nothing"
    )
    assert observation.snapshot.cities[0]["currently_building"] is None
    assert evaluator.evaluate(
        {
            "type": "field_equals",
            "path": "cities.0.currently_building",
            "value": None,
        },
        observation,
    ).valid
    assert evaluator.evaluate(
        {
            "type": "field_in",
            "path": "cities.0.currently_building",
            "values": [None, "UNIT_SCOUT"],
        },
        observation,
    ).valid
    assert not evaluator.evaluate(
        {
            "type": "field_equals",
            "path": "cities.0.currently_building",
            "value": "nothing",
        },
        observation,
    ).valid


class _ExecutingGame(_ReadPolicyGame):
    async def execute_task(self, task):
        self.call_count += 1
        self.snapshot.cities[0]["currently_building"] = task.arguments["item_name"]
        return ActionResult(success=True, message="production selected")
