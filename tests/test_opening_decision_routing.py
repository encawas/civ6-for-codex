import asyncio

from civ6_workflow.decisioning import opening_decision_events
from civ6_workflow.domain import SlotState, TickOutcomeKind
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import ExecutionMode, PlanBundle, RuntimeSnapshot
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore


class _OpeningGame:
    def __init__(self, snapshot: RuntimeSnapshot):
        self.snapshot = snapshot
        self.call_count = 0
        self.mutation_count = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        return self.snapshot.model_copy(deep=True)

    async def list_tools(self):
        return {
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
        }

    async def execute_task(self, task):
        self.mutation_count += 1
        raise AssertionError("opening routing must not mutate in this test")

    async def end_turn(self, reflections=None):
        self.mutation_count += 1
        raise AssertionError("opening routing must not end the turn in this test")


class _PlannerMustNotRun:
    def __init__(self):
        self.calls = 0
        self.last_diagnostics = {"attempt_count": 0, "backend": "test"}

    async def plan(self, request):
        self.calls += 1
        raise AssertionError("creating a logical request must precede provider use")


def _snapshot(*, research=None, production=None):
    return RuntimeSnapshot(
        turn=1,
        game_id="opening-routing",
        overview={"turn": 1, "num_cities": 1},
        cities=[{"city_id": 1, "currently_building": production}],
        tech_civics={
            "current_research": research,
            "available_techs": [{"tech_type": "TECH_MINING", "name": "Mining"}],
        },
        blockers=[],
    )


def _engine(tmp_path, snapshot):
    game = _OpeningGame(snapshot)
    planner = _PlannerMustNotRun()
    return (
        WorkflowEngine(
            store=WorkflowStore(tmp_path / "opening-routing.sqlite3"),
            game=game,
            planner=planner,
            config=EngineConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=False,
                auto_action_types={"set_research", "city_set_production"},
            ),
        ),
        game,
        planner,
    )


def test_opening_empty_slots_create_durable_gaps_then_a_logical_request(tmp_path):
    """AI-003 / AI-005: empty opening slots route through gaps before human wait."""

    async def scenario():
        engine, game, planner = _engine(tmp_path, _snapshot())

        first = await engine.tick()

        assert first.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_CREATED
        assert first.workflow_tick["outcome"] != TickOutcomeKind.AWAITING_HUMAN
        assert {
            gap.gap_type for gap in engine.store.list_decision_gaps("opening-routing")
        } == {
            "research_direction_required",
            "city_role_required",
        }
        assert game.mutation_count == 0
        assert planner.calls == 0

        second = await engine.tick()

        assert (
            second.workflow_tick["outcome"]
            == TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
        )
        assert second.workflow_tick["outcome"] != TickOutcomeKind.AWAITING_HUMAN
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 1
        assert game.mutation_count == 0
        assert planner.calls == 0

    asyncio.run(scenario())


def test_opening_research_queue_materializes_before_decision_gap(tmp_path):
    """PLAN-002 / TASK-004: an approved research queue remains deterministic."""

    async def scenario():
        engine, game, planner = _engine(
            tmp_path,
            _snapshot(research=None, production="BUILDING_MONUMENT"),
        )
        engine.store.save_plan_bundle(
            "opening-routing",
            1,
            PlanBundle(
                summary="continue approved research",
                strategy_updates={"research_queue": ["TECH_MINING"]},
            ),
            mode=ExecutionMode.AUTO,
            auto_action_types={"set_research"},
        )

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == TickOutcomeKind.TASK_CREATED
        task = engine.store.get_task("opening-routing", result.workflow_tick["task_id"])
        assert task.action_type == "set_research"
        assert engine.store.list_decision_gaps("opening-routing") == []
        assert game.mutation_count == 0
        assert planner.calls == 0

    asyncio.run(scenario())


def test_opening_city_queue_materializes_before_decision_gap(tmp_path):
    """TASK-004: an approved city queue remains deterministic."""

    async def scenario():
        engine, game, planner = _engine(
            tmp_path,
            _snapshot(research="TECH_MINING", production=None),
        )
        engine.store.save_plan_bundle(
            "opening-routing",
            1,
            PlanBundle(
                summary="continue approved city production",
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

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == TickOutcomeKind.TASK_CREATED
        task = engine.store.get_task("opening-routing", result.workflow_tick["task_id"])
        assert task.action_type == "city_set_production"
        assert engine.store.list_decision_gaps("opening-routing") == []
        assert game.mutation_count == 0
        assert planner.calls == 0

    asyncio.run(scenario())


def test_unloaded_research_slot_does_not_create_a_decision_gap():
    """Missing progression data is not evidence of an empty slot."""

    observation = normalize_runtime_snapshot(
        RuntimeSnapshot(
            turn=1,
            game_id="opening-routing",
            overview={"turn": 1},
            cities=[{"city_id": 1, "currently_building": "UNIT_BUILDER"}],
        )
    )

    assert (
        observation.canonical.progression.current_research.state is SlotState.NOT_LOADED
    )
    assert opening_decision_events(observation) == []
