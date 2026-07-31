from __future__ import annotations

import inspect
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import civ6_workflow.batch_executor as batch_executor_module
from civ6_workflow.batch_executor import BatchExecutor, BarrierKind
from civ6_workflow.conditions import ConditionEvaluator
from civ6_workflow.domain import AttemptStatus, MutationSentTick, RuntimeState
from civ6_workflow.engine import WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    MutationDeliveryStatus,
    RiskLevel,
    RuntimeSnapshot,
    StoredTask,
    TaskStatus,
    TickMetrics,
)
from civ6_workflow.ports import MutationBudget
from civ6_workflow.observation_normalization import normalize_runtime_snapshot


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)


def _task(
    task_id: str,
    *,
    status: TaskStatus = TaskStatus.READY,
    due_turn: int = 7,
) -> StoredTask:
    return StoredTask(
        task_id=task_id,
        plan_id="turn_action_graph_test",
        action_type="set_research",
        entity_type="research",
        entity_id="TECH_POTTERY",
        due_turn=due_turn,
        arguments={"tech_or_civic": "TECH_POTTERY"},
        preconditions=[],
        postconditions=[{"type": "research_equals", "tech_type": "TECH_POTTERY"}],
        risk=RiskLevel.LOW,
        reason="test",
        created_turn=7,
        created_from_observation_id="obs_7",
        status=status,
    )


def _observation(*, turn: int = 7):
    return normalize_runtime_snapshot(
        RuntimeSnapshot(
            turn=turn,
            game_id="game",
            overview={"turn": turn},
        )
    )


class _WaveStore:
    def __init__(
        self,
        tasks: list[StoredTask],
        eligible: list[StoredTask],
        *,
        projection_hash: str,
    ):
        self.tasks = tasks
        self.eligible = eligible
        self.projection_hash = projection_hash

    def active_turn_action_graph(self, game_id: str):
        assert game_id == "game"
        return (
            SimpleNamespace(
                graph_id="graph",
                turn_number=7,
                source_observation_projection_hash=self.projection_hash,
            ),
            tuple(self.tasks),
        )

    def due_turn_action_nodes(
        self,
        game_id: str,
        turn: int,
        *,
        source_observation_id: str,
    ):
        assert (game_id, turn, source_observation_id) == ("game", 7, "obs_7")
        return list(reversed(self.eligible))


def _executor(store, game=None) -> BatchExecutor:
    return BatchExecutor(
        store=store,
        game=game or SimpleNamespace(),
        conditions=ConditionEvaluator(),
        verification_attempts=3,
        now=lambda: NOW,
        monotonic=lambda: 0.0,
        checkpoint=lambda _name: None,
    )


def test_wave_selection_is_deterministic_and_reports_all_four_barriers():
    eligible_a = _task("node_a")
    eligible_b = _task("node_b")
    dependency = _task("node_dependency")
    approval = _task("node_approval", status=TaskStatus.AWAITING_CONFIRMATION)
    verification = _task("node_verification", status=TaskStatus.VERIFYING)
    future = _task("node_future", due_turn=8)
    observation = _observation()
    store = _WaveStore(
        [
            future,
            dependency,
            eligible_b,
            verification,
            approval,
            eligible_a,
        ],
        [eligible_b, eligible_a],
        projection_hash=observation.canonical.projection_hash,
    )

    wave = _executor(store).select_wave(
        observation,
        source_observation_id="obs_7",
    )

    assert [task.task_id for task in wave.eligible] == ["node_a", "node_b"]
    assert {barrier.kind: barrier.node_ids for barrier in wave.barriers} == {
        BarrierKind.DEPENDENCY: ("node_dependency",),
        BarrierKind.VERIFICATION: ("node_verification",),
        BarrierKind.APPROVAL: ("node_approval",),
        BarrierKind.TURN: ("node_future",),
    }


def test_wave_barriers_are_reconstructed_from_persisted_state():
    tasks = [
        _task("node_dependency"),
        _task("node_approval", status=TaskStatus.AWAITING_CONFIRMATION),
        _task("node_verification", status=TaskStatus.UNCERTAIN),
        _task("node_future", due_turn=8),
    ]
    observation = _observation()
    first = _executor(
        _WaveStore(
            tasks,
            [],
            projection_hash=observation.canonical.projection_hash,
        )
    ).select_wave(
        observation,
        source_observation_id="obs_7",
    )
    restarted = _executor(
        _WaveStore(
            tasks,
            [],
            projection_hash=observation.canonical.projection_hash,
        )
    ).select_wave(
        observation,
        source_observation_id="obs_7",
    )

    assert restarted == first


class _ExecutionStore:
    def __init__(self):
        self.tasks = [_task("node_b"), _task("node_a")]
        self.saved_attempts = []
        self.updated_attempts = []
        self.status_updates = []
        self.runtime_updates = []

    def active_turn_action_graph(self, _game_id: str):
        return (
            SimpleNamespace(
                graph_id="turn_action_graph_test",
                turn_number=7,
                source_observation_projection_hash=(
                    _observation().canonical.projection_hash
                ),
            ),
            tuple(self.tasks),
        )

    def list_tasks(self, _game_id: str, statuses=None):
        return []

    def due_turn_action_nodes(
        self,
        _game_id: str,
        _turn: int,
        *,
        source_observation_id: str,
    ):
        assert source_observation_id == "obs_7"
        return list(self.tasks)

    def latest_attempt_for_task(self, _game_id: str, _task_id: str):
        return None

    def next_attempt_number(self, _game_id: str, _task_id: str):
        return 1

    def save_action_attempt(self, attempt):
        assert attempt.status is AttemptStatus.PREPARED
        self.saved_attempts.append(attempt)

    def set_task_status(self, game_id, task_id, status, **_kwargs):
        self.status_updates.append((game_id, task_id, status))

    def update_action_attempt(self, attempt):
        self.updated_attempts.append(attempt)

    def save_runtime_state(self, game_id, state, *, active_attempt_id):
        self.runtime_updates.append((game_id, state, active_attempt_id))


class _ExecutionGame:
    call_count = 0

    def __init__(self, store: _ExecutionStore):
        self.store = store
        self.executed = []

    async def execute_task(self, task):
        assert self.store.saved_attempts
        assert self.store.saved_attempts[0].status is AttemptStatus.PREPARED
        self.call_count += 1
        self.executed.append(task.task_id)
        return ActionResult(
            success=True,
            delivery_status=MutationDeliveryStatus.ACKNOWLEDGED,
        )


@pytest.mark.asyncio
async def test_executor_persists_attempt_before_one_deterministic_mutation():
    store = _ExecutionStore()
    game = _ExecutionGame(store)
    budget = MutationBudget()

    transition = await _executor(store, game).advance(
        _observation(),
        source_observation_id="obs_7",
        mode=ExecutionMode.AUTO,
        available_tools={"set_research"},
        metrics=TickMetrics(),
        budget=budget,
    )

    assert transition is not None
    assert transition.tick_type is MutationSentTick
    assert game.executed == ["node_a"]
    assert budget.used == 1
    assert store.status_updates == [("game", "node_a", TaskStatus.RUNNING)]
    assert store.runtime_updates[0][1] is RuntimeState.RECONCILING


def test_batch_executor_has_no_planner_dependency():
    source = inspect.getsource(batch_executor_module)
    assert "PlannerLifecycleCoordinator" not in source
    assert "PlannerRequest" not in source
    assert "planner:" not in str(inspect.signature(BatchExecutor))


def test_runtime_routes_task_execution_only_through_batch_executor():
    source = inspect.getsource(WorkflowEngine._run_tick)
    assert "batch_executor.advance" in source
    assert "batch_executor.reconcile" in source
    assert "batch_executor.send_end_turn" in source
    assert not hasattr(WorkflowEngine, "_send_task")
    assert not hasattr(WorkflowEngine, "_reconcile_attempt")
    assert not hasattr(WorkflowEngine, "_send_end_turn")
    assert not hasattr(WorkflowEngine, "_reconcile_end_turn")
