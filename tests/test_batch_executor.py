from __future__ import annotations

import asyncio
import inspect
from datetime import UTC, datetime
from types import SimpleNamespace

import civ6_workflow.batch_executor as batch_executor_module
from civ6_workflow.batch_executor import BatchExecutor, BarrierKind
from civ6_workflow.conditions import ConditionEvaluator
from civ6_workflow.domain import (
    AttemptRecoveredTick,
    AttemptStatus,
    MutationSentTick,
    RuntimeState,
    TaskInvalidatedTick,
)
from civ6_workflow.runtime import WorkflowRuntime
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    MutationDeliveryStatus,
    RiskLevel,
    RuntimeSnapshot,
    TurnActionExecution,
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
) -> TurnActionExecution:
    return TurnActionExecution(
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
        tasks: list[TurnActionExecution],
        eligible: list[TurnActionExecution],
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


def _executor(
    store,
    game=None,
    *,
    allowed_action_types=None,
    allowed_tools=None,
) -> BatchExecutor:
    return BatchExecutor(
        store=store,
        game=game or SimpleNamespace(),
        conditions=ConditionEvaluator(),
        allowed_action_types=(
            {
                "city_set_production",
                "set_research",
                "set_civic",
                "send_envoy",
                "unit_move",
                "unit_found_city",
                "tactical_unit_move",
                "tactical_unit_fortify",
                "tactical_unit_skip",
            }
            if allowed_action_types is None
            else allowed_action_types
        ),
        allowed_tools=(
            {
                "set_city_production",
                "set_research",
                "send_envoy",
                "unit_action",
            }
            if allowed_tools is None
            else allowed_tools
        ),
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
        raise AssertionError("canonical execution must use PreparedAction")

    async def execute_prepared_action(self, prepared, task):
        assert self.store.saved_attempts
        attempt = self.store.saved_attempts[0]
        assert attempt.status is AttemptStatus.PREPARED
        assert dict(attempt.normalized_arguments) == dict(prepared.normalized_arguments)
        self.call_count += 1
        self.executed.append(task.task_id)
        return ActionResult(
            success=True,
            delivery_status=MutationDeliveryStatus.ACKNOWLEDGED,
        )


def test_executor_persists_attempt_before_one_deterministic_mutation():
    store = _ExecutionStore()
    game = _ExecutionGame(store)
    budget = MutationBudget()

    transition = asyncio.run(
        _executor(store, game).advance(
            _observation(),
            source_observation_id="obs_7",
            mode=ExecutionMode.AUTO,
            available_tools={"set_research"},
            metrics=TickMetrics(),
            budget=budget,
        )
    )

    assert transition is not None
    assert transition.tick_type is MutationSentTick
    assert game.executed == ["node_a"]
    assert budget.used == 1
    assert store.status_updates == [("game", "node_a", TaskStatus.RUNNING)]
    assert store.runtime_updates[0][1] is RuntimeState.RECONCILING
    assert transition.attempt_update is not None
    assert "delivery_status" in transition.attempt_update.transport_result
    assert "delivery_status" not in transition.attempt_update.tool_result


def test_batch_executor_has_no_planner_dependency():
    source = inspect.getsource(batch_executor_module)
    assert "PlannerLifecycleCoordinator" not in source
    assert "PlannerRequest" not in source
    assert "planner:" not in str(inspect.signature(BatchExecutor))


def test_runtime_routes_task_execution_only_through_batch_executor():
    source = inspect.getsource(WorkflowRuntime._run_tick)
    assert "batch_executor.advance" in source
    assert "batch_executor.reconcile" in source
    assert "batch_executor.send_end_turn" in source
    assert not hasattr(WorkflowRuntime, "_send_task")
    assert not hasattr(WorkflowRuntime, "_reconcile_attempt")
    assert not hasattr(WorkflowRuntime, "_send_end_turn")
    assert not hasattr(WorkflowRuntime, "_reconcile_end_turn")


def test_configured_tool_policy_rejects_before_action_attempt():
    store = _ExecutionStore()
    game = _ExecutionGame(store)
    budget = MutationBudget()

    transition = asyncio.run(
        _executor(store, game, allowed_tools=set()).advance(
            _observation(),
            source_observation_id="obs_7",
            mode=ExecutionMode.AUTO,
            available_tools={"set_research"},
            metrics=TickMetrics(),
            budget=budget,
        )
    )

    assert transition is not None
    assert transition.tick_type is TaskInvalidatedTick
    assert store.saved_attempts == []
    assert store.updated_attempts == []
    assert game.executed == []
    assert budget.used == 0


def test_disallowed_action_policy_rejects_before_action_attempt():
    store = _ExecutionStore()
    game = _ExecutionGame(store)
    budget = MutationBudget()

    transition = asyncio.run(
        _executor(store, game, allowed_action_types=set()).advance(
            _observation(),
            source_observation_id="obs_7",
            mode=ExecutionMode.AUTO,
            available_tools={"set_research"},
            metrics=TickMetrics(),
            budget=budget,
        )
    )

    assert transition is not None
    assert transition.tick_type is TaskInvalidatedTick
    assert store.saved_attempts == []
    assert game.executed == []
    assert budget.used == 0


def test_exhausted_budget_is_rejected_before_send_without_uncertain_state():
    store = _ExecutionStore()
    game = _ExecutionGame(store)
    budget = MutationBudget(limit=1, used=1)

    transition = asyncio.run(
        _executor(store, game).advance(
            _observation(),
            source_observation_id="obs_7",
            mode=ExecutionMode.AUTO,
            available_tools={"set_research"},
            metrics=TickMetrics(),
            budget=budget,
        )
    )

    assert transition is not None
    assert transition.tick_type is AttemptRecoveredTick
    assert transition.attempt_update is not None
    assert transition.attempt_update.status is AttemptStatus.REJECTED_BEFORE_SEND
    assert transition.attempt_update.transport_result["phase"] == "pre_send"
    assert game.executed == []
    assert store.runtime_updates == []
    assert budget.used == 1


def test_preflight_failure_is_rejected_before_send_without_consuming_budget():
    class _DisconnectedGame(_ExecutionGame):
        def preflight_mutation(self, _tool_name):
            raise RuntimeError("MCP client is not connected")

    store = _ExecutionStore()
    game = _DisconnectedGame(store)
    budget = MutationBudget()

    transition = asyncio.run(
        _executor(store, game).advance(
            _observation(),
            source_observation_id="obs_7",
            mode=ExecutionMode.AUTO,
            available_tools={"set_research"},
            metrics=TickMetrics(),
            budget=budget,
        )
    )

    assert transition is not None
    assert transition.tick_type is AttemptRecoveredTick
    assert transition.attempt_update is not None
    assert transition.attempt_update.status is AttemptStatus.REJECTED_BEFORE_SEND
    assert game.executed == []
    assert store.runtime_updates == []
    assert budget.used == 0
