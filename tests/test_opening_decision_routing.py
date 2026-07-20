import asyncio
from datetime import UTC, datetime

import pytest

from civ6_workflow.decisioning import opening_decision_events
from civ6_workflow.domain import (
    DecisionGapStatus,
    PlannerRequestStatus,
    ProviderAttempt,
    ProviderAttemptStatus,
    SlotState,
    TickOutcomeKind,
)
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


def _engine(tmp_path, snapshot, *, planner=None):
    game = _OpeningGame(snapshot)
    planner = planner or _PlannerMustNotRun()
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


class _HumanReviewPlanner:
    def __init__(self):
        self.calls = 0
        self.last_diagnostics = {"attempt_count": 1, "backend": "test"}

    async def plan(self, request):
        self.calls += 1
        return PlanBundle(
            summary="opening choice requires review",
            requires_human_review=True,
        )


async def _create_opening_request(engine):
    created = await engine.tick()
    assert created.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_CREATED
    requested = await engine.tick()
    assert (
        requested.workflow_tick["outcome"]
        == TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
    )
    return requested.workflow_tick["planner_request_id"]


def test_city_gap_request_closes_when_player_starts_production(tmp_path):
    """Opening city requests stop before provider use once production is filled."""

    async def scenario():
        engine, game, planner = _engine(
            tmp_path,
            _snapshot(research="TECH_MINING", production=None),
        )
        request_id = await _create_opening_request(engine)
        game.snapshot = _snapshot(
            research="TECH_MINING",
            production="BUILDING_MONUMENT",
        )

        closed = await engine.tick()

        request = engine.store.get_planner_request(request_id)
        gap = engine.store.list_decision_gaps("opening-routing")[0]
        assert closed.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        assert request.status is PlannerRequestStatus.SUPERSEDED
        assert gap.status is DecisionGapStatus.RESOLVED
        assert (
            gap.resolution_reason
            == "city production slot was filled outside the workflow"
        )
        assert engine.store.list_provider_attempts(request_id) == []
        assert planner.calls == 0
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 1

        await engine.tick()
        assert planner.calls == 0
        assert engine.store.active_planner_request("opening-routing") is None
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 1

    asyncio.run(scenario())


def test_research_gap_request_closes_when_player_selects_research(tmp_path):
    """Opening research requests stop before provider use once research is filled."""

    async def scenario():
        engine, game, planner = _engine(
            tmp_path,
            _snapshot(research=None, production="BUILDING_MONUMENT"),
        )
        request_id = await _create_opening_request(engine)
        game.snapshot = _snapshot(
            research="TECH_MINING",
            production="BUILDING_MONUMENT",
        )

        closed = await engine.tick()

        request = engine.store.get_planner_request(request_id)
        gap = engine.store.list_decision_gaps("opening-routing")[0]
        assert closed.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        assert request.status is PlannerRequestStatus.SUPERSEDED
        assert gap.status is DecisionGapStatus.RESOLVED
        assert gap.resolution_reason == "research slot was filled outside the workflow"
        assert engine.store.list_provider_attempts(request_id) == []
        assert planner.calls == 0
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 1

        await engine.tick()
        assert planner.calls == 0
        assert engine.store.active_planner_request("opening-routing") is None
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 1

    asyncio.run(scenario())


def test_unresolved_opening_request_reaches_the_provider_once(tmp_path):
    """An empty slot keeps its active request eligible for the normal provider call."""

    async def scenario():
        planner = _HumanReviewPlanner()
        engine, _, _ = _engine(
            tmp_path,
            _snapshot(research=None, production="BUILDING_MONUMENT"),
            planner=planner,
        )
        request_id = await _create_opening_request(engine)

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
        assert planner.calls == 1
        assert (
            engine.store.get_planner_request(request_id).status
            is not PlannerRequestStatus.SUPERSEDED
        )

    asyncio.run(scenario())


def test_restarted_opening_request_closes_when_player_fills_city_slot(tmp_path):
    """Restart reconciliation closes a manually resolved city decision before planning."""

    async def scenario():
        engine, game, planner = _engine(
            tmp_path,
            _snapshot(research="TECH_MINING", production=None),
        )
        request_id = await _create_opening_request(engine)
        game.snapshot = _snapshot(
            research="TECH_MINING",
            production="UNIT_BUILDER",
        )
        restarted = WorkflowEngine(
            store=WorkflowStore(tmp_path / "opening-routing.sqlite3"),
            game=game,
            planner=planner,
            config=engine.config,
        )

        closed = await restarted.tick()

        request = restarted.store.get_planner_request(request_id)
        gap = restarted.store.list_decision_gaps("opening-routing")[0]
        assert closed.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        assert request.status is PlannerRequestStatus.SUPERSEDED
        assert gap.status is DecisionGapStatus.RESOLVED
        assert planner.calls == 0
        assert restarted.store.list_provider_attempts(request_id) == []

    asyncio.run(scenario())


async def _create_successor_after_partial_resolution(
    engine,
    game,
    *,
    research,
    production,
):
    original_request_id = await _create_opening_request(engine)
    game.snapshot = _snapshot(research=research, production=production)
    closed = await engine.tick()
    successor = await engine.tick()
    return original_request_id, closed, successor


def test_partial_research_resolution_creates_city_successor_request(tmp_path):
    """A pre-provider partial resolution releases budget for the remaining city gap."""

    async def scenario():
        planner = _HumanReviewPlanner()
        engine, game, _ = _engine(tmp_path, _snapshot(), planner=planner)
        (
            original_id,
            closed,
            successor,
        ) = await _create_successor_after_partial_resolution(
            engine,
            game,
            research="TECH_MINING",
            production=None,
        )

        gaps = {
            gap.gap_type: gap
            for gap in engine.store.list_decision_gaps("opening-routing")
        }
        successor_id = successor.workflow_tick["planner_request_id"]
        assert closed.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        assert (
            engine.store.get_planner_request(original_id).status
            is PlannerRequestStatus.SUPERSEDED
        )
        assert gaps["research_direction_required"].status is DecisionGapStatus.RESOLVED
        assert gaps["city_role_required"].status is DecisionGapStatus.REQUESTED
        assert (
            successor.workflow_tick["outcome"]
            == TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
        )
        assert successor_id != original_id
        assert engine.store.get_planner_request(successor_id).decision_gap_ids == (
            gaps["city_role_required"].decision_gap_id,
        )
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 2
        assert (
            engine.store.provider_budget_request_count_for_turn("opening-routing", 1)
            == 1
        )
        assert planner.calls == 0

        planned = await engine.tick()
        assert planned.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
        assert planner.calls == 1
        assert len(engine.store.list_provider_attempts(original_id)) == 0

        await engine.tick()
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 2
        assert planner.calls == 1

    asyncio.run(scenario())


def test_partial_city_resolution_creates_research_successor_request(tmp_path):
    """A pre-provider partial resolution releases budget for the remaining research gap."""

    async def scenario():
        planner = _HumanReviewPlanner()
        engine, game, _ = _engine(tmp_path, _snapshot(), planner=planner)
        (
            original_id,
            closed,
            successor,
        ) = await _create_successor_after_partial_resolution(
            engine,
            game,
            research=None,
            production="BUILDING_MONUMENT",
        )

        gaps = {
            gap.gap_type: gap
            for gap in engine.store.list_decision_gaps("opening-routing")
        }
        successor_id = successor.workflow_tick["planner_request_id"]
        assert closed.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        assert (
            engine.store.get_planner_request(original_id).status
            is PlannerRequestStatus.SUPERSEDED
        )
        assert gaps["city_role_required"].status is DecisionGapStatus.RESOLVED
        assert gaps["research_direction_required"].status is DecisionGapStatus.REQUESTED
        assert (
            successor.workflow_tick["outcome"]
            == TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
        )
        assert successor_id != original_id
        assert engine.store.get_planner_request(successor_id).decision_gap_ids == (
            gaps["research_direction_required"].decision_gap_id,
        )
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 2
        assert (
            engine.store.provider_budget_request_count_for_turn("opening-routing", 1)
            == 1
        )
        assert planner.calls == 0

        planned = await engine.tick()
        assert planned.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
        assert planner.calls == 1
        assert len(engine.store.list_provider_attempts(original_id)) == 0

        await engine.tick()
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 2
        assert planner.calls == 1

    asyncio.run(scenario())


def test_partial_resolution_after_restart_keeps_successor_budget(tmp_path):
    """Restart keeps the pre-provider successor allowance and request audit chain."""

    async def scenario():
        planner = _HumanReviewPlanner()
        engine, game, _ = _engine(tmp_path, _snapshot(), planner=planner)
        original_id = await _create_opening_request(engine)
        game.snapshot = _snapshot(research="TECH_MINING", production=None)
        restarted = WorkflowEngine(
            store=WorkflowStore(tmp_path / "opening-routing.sqlite3"),
            game=game,
            planner=planner,
            config=engine.config,
        )

        closed = await restarted.tick()
        successor = await restarted.tick()
        planned = await restarted.tick()

        assert closed.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        assert (
            restarted.store.get_planner_request(original_id).status
            is PlannerRequestStatus.SUPERSEDED
        )
        assert (
            successor.workflow_tick["outcome"]
            == TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
        )
        assert planned.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
        assert restarted.store.logical_request_count_for_turn("opening-routing", 1) == 2
        assert planner.calls == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "provider_status",
    [
        ProviderAttemptStatus.STARTED,
        ProviderAttemptStatus.SUCCEEDED,
        ProviderAttemptStatus.FAILED,
    ],
)
def test_provider_attempt_history_keeps_partial_resolution_budget(
    tmp_path,
    provider_status,
):
    """Any provider-attempt history keeps the original turn budget consumed."""

    async def scenario():
        planner = _HumanReviewPlanner()
        engine, game, _ = _engine(tmp_path, _snapshot(), planner=planner)
        request_id = await _create_opening_request(engine)
        request = engine.store.get_planner_request(request_id)
        now = datetime.now(UTC)
        attempt = ProviderAttempt(
            provider_attempt_id=f"provider-budget-{provider_status.value}",
            planner_request_id=request_id,
            attempt_number=1,
            provider_request_id="provider-wire-1",
            status=provider_status,
            started_at=now,
            completed_at=(
                None if provider_status is ProviderAttemptStatus.STARTED else now
            ),
            latency_seconds=(
                None if provider_status is ProviderAttemptStatus.STARTED else 0.0
            ),
            failure_category=(
                None if provider_status is not ProviderAttemptStatus.FAILED else "test"
            ),
        )
        if provider_status is ProviderAttemptStatus.STARTED:
            engine.store.start_provider_attempt("opening-routing", request, attempt)
        else:
            engine.store.save_provider_attempt("opening-routing", attempt)

        game.snapshot = _snapshot(research="TECH_MINING", production=None)
        closed = await engine.tick()
        await engine.tick()

        assert closed.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        assert (
            engine.store.get_planner_request(request_id).status
            is PlannerRequestStatus.SUPERSEDED
        )
        assert (
            engine.store.provider_budget_request_count_for_turn("opening-routing", 1)
            == 1
        )
        assert engine.store.logical_request_count_for_turn("opening-routing", 1) == 1
        assert planner.calls == 0

    asyncio.run(scenario())
