from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from civ6_workflow.domain import (
    AttemptStatus,
    Mission,
    MissionGraph,
    MissionImpactAnalyzer,
    MissionStatus,
    StateDeltaBuilder,
    SubjectRef,
)
from civ6_workflow.models import RuntimeSnapshot
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.planner_lifecycle import (
    PlannerLifecycleCoordinator,
    PlannerLifecycleRuntime,
)
from civ6_workflow.runtime import WorkflowRuntime


NOW = datetime(2026, 8, 5, 3, 0, tzinfo=UTC)


def _observation(*, x: int):
    return normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id="game-1",
            turn=10,
            units=[
                {
                    "unit_id": 7,
                    "unit_type": "UNIT_SETTLER",
                    "x": x,
                    "y": 2,
                    "moves_remaining": 1,
                }
            ],
        ),
        observed_at=NOW + timedelta(seconds=x),
    ).canonical


def _mission(status: MissionStatus) -> Mission:
    return Mission(
        mission_id=f"mission-{status.value.lower()}",
        game_session_id="game-1",
        contract_id="contract-1",
        mission_revision=1,
        scope="settler",
        subject=SubjectRef(subject_type="unit", subject_id="7"),
        slot="unit:7:settlement",
        objective="found city",
        desired_outcome={
            "settler": {
                "unit_id": 7,
                "target_x": 3,
                "target_y": 2,
                "baseline_city_count": 1,
                "owner": "PLAYER_0",
            }
        },
        status=status,
    )


def test_settler_motion_does_not_also_create_tactical_delta():
    result = StateDeltaBuilder().compare(_observation(x=1), _observation(x=2))

    assert result.state_delta is not None
    assert {change.scope for change in result.state_delta.changes} == {"settler"}


def test_completed_mission_is_not_reopened_by_state_delta():
    result = StateDeltaBuilder().compare(_observation(x=1), _observation(x=2))
    assert result.state_delta is not None
    completed_graph = MissionGraph(
        missions=(_mission(MissionStatus.COMPLETED),)
    )
    active_graph = MissionGraph(
        missions=(_mission(MissionStatus.ACTIVE),)
    )

    assert (
        MissionImpactAnalyzer().affected_mission_ids(
            result.state_delta, completed_graph
        )
        == ()
    )
    assert MissionImpactAnalyzer().affected_mission_ids(
        result.state_delta, active_graph
    ) == ("mission-active",)


def test_verified_action_projection_advances_observation_baseline():
    baseline = _observation(x=1)
    current = _observation(x=2)

    class Store:
        def __init__(self):
            self.accepted = []

        def list_action_attempts(self, game_id):
            assert game_id == "game-1"
            return [
                SimpleNamespace(
                    status=AttemptStatus.SUCCEEDED,
                    action_type="unit_move",
                    prepared_at=NOW + timedelta(seconds=1),
                    verified_at=NOW + timedelta(seconds=3),
                    last_verification_projection_hash=current.projection_hash,
                    normalized_arguments={
                        "unit_id": 7,
                        "target_x": 2,
                        "target_y": 2,
                    },
                    action_attempt_id="attempt-1",
                )
            ]

        def accept_observation_baseline(self, observation_id, **kwargs):
            self.accepted.append((observation_id, kwargs))

    store = Store()
    coordinator = PlannerLifecycleCoordinator(
        PlannerLifecycleRuntime(
            store=store,
            game=SimpleNamespace(),
            planner=SimpleNamespace(),
            config=SimpleNamespace(),
            conditions=SimpleNamespace(),
            information_queries=SimpleNamespace(),
            now=lambda: NOW + timedelta(seconds=4),
            monotonic=lambda: 0.0,
            checkpoint=lambda _name: None,
            observation_id=lambda: current.observation_id,
            human_wait_context=lambda _snapshot: {},
            available_tools=lambda: set(),
        )
    )
    delta = StateDeltaBuilder().compare(baseline, current).state_delta
    assert delta is not None

    accepted = coordinator._accept_verified_action_baseline(
        current, baseline, delta
    )

    assert accepted is True
    assert store.accepted[0][0] == current.observation_id
    assert (
        store.accepted[0][1]["expected_previous_observation_id"]
        == baseline.observation_id
    )


def test_verified_action_does_not_swallow_unrelated_scope_change():
    baseline = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id="game-1",
            turn=10,
            tech_civics={"current_research_type": "TECH_WRITING"},
            units=[
                {
                    "unit_id": 7,
                    "unit_type": "UNIT_SETTLER",
                    "x": 1,
                    "y": 2,
                    "moves_remaining": 1,
                }
            ],
        ),
        observed_at=NOW + timedelta(seconds=1),
    ).canonical
    current = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id="game-1",
            turn=10,
            tech_civics={"current_research_type": "TECH_MINING"},
            units=[
                {
                    "unit_id": 7,
                    "unit_type": "UNIT_SETTLER",
                    "x": 2,
                    "y": 2,
                    "moves_remaining": 1,
                }
            ],
        ),
        observed_at=NOW + timedelta(seconds=2),
    ).canonical
    delta = StateDeltaBuilder().compare(baseline, current).state_delta
    assert delta is not None

    class Store:
        def list_action_attempts(self, _game_id):
            return [
                SimpleNamespace(
                    status=AttemptStatus.SUCCEEDED,
                    action_type="unit_move",
                    prepared_at=NOW + timedelta(seconds=1),
                    verified_at=NOW + timedelta(seconds=3),
                    last_verification_projection_hash=current.projection_hash,
                    normalized_arguments={
                        "unit_id": 7,
                        "target_x": 2,
                        "target_y": 2,
                    },
                    action_attempt_id="attempt-1",
                )
            ]

        def accept_observation_baseline(self, *_args, **_kwargs):
            raise AssertionError("unrelated changes must not advance the baseline")

    coordinator = PlannerLifecycleCoordinator(
        PlannerLifecycleRuntime(
            store=Store(),
            game=SimpleNamespace(),
            planner=SimpleNamespace(),
            config=SimpleNamespace(),
            conditions=SimpleNamespace(),
            information_queries=SimpleNamespace(),
            now=lambda: NOW + timedelta(seconds=4),
            monotonic=lambda: 0.0,
            checkpoint=lambda _name: None,
            observation_id=lambda: current.observation_id,
            human_wait_context=lambda _snapshot: {},
            available_tools=lambda: set(),
        )
    )

    assert (
        coordinator._accept_verified_action_baseline(current, baseline, delta)
        is False
    )


def test_rewind_recovery_precedes_projection_and_mutation():
    source = inspect.getsource(WorkflowRuntime._run_tick)
    rewind = source.index("if rewind_pending:", source.index("unresolved ="))
    projection = source.index("projection = await self.strategic_workflow")
    execution = source.index("execution = await self.batch_executor.advance")

    assert rewind < projection < execution
    assert "turn_rewind_requires_strategic_reset" in source


def test_repair_budget_exhaustion_is_an_explicit_human_wait():
    source = inspect.getsource(PlannerLifecycleCoordinator.advance_mission_repair)

    assert "Mission repair closure exhausted" in source
    assert "AwaitingHumanTick" in source
