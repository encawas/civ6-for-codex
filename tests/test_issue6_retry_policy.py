import asyncio
from datetime import UTC, datetime

import pytest

from civ6_workflow.actions import ACTION_REGISTRY
from civ6_workflow.domain import (
    ActionAttempt,
    AttemptStatus,
    RetryClassification,
    RuntimeState,
    VerificationStatus,
    validate_workflow_tick,
)
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    MutationDeliveryStatus,
    PlanBundle,
    ProposedTask,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.store import WorkflowStore


class Planner:
    def __init__(self):
        self.calls = 0

    async def plan(self, request):
        self.calls += 1
        return PlanBundle(summary="no planner work")


class Game:
    def __init__(self, snapshots, *, result=None):
        self.snapshots = list(snapshots)
        self.result = result or ActionResult(success=True)
        self.call_count = 0
        self.reads = 0
        self.mutations = 0
        self.end_turn_calls = 0
        self.fail_read = False
        self.fail_tools = False

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        if self.fail_read:
            raise ConnectionError("snapshot unavailable")
        index = min(self.reads, len(self.snapshots) - 1)
        self.reads += 1
        snapshot = self.snapshots[index].model_copy(deep=True)
        if not include_units:
            snapshot.units = None
        return snapshot

    async def execute_task(self, task):
        self.call_count += 1
        self.mutations += 1
        return self.result

    async def end_turn(self, reflections=None):
        self.call_count += 1
        self.end_turn_calls += 1
        return self.result

    async def list_tools(self):
        if self.fail_tools:
            raise RuntimeError("tool discovery failed")
        return {
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
        }


class Crash:
    def __init__(self, checkpoint):
        self.target_checkpoint = checkpoint

    def checkpoint(self, name):
        if name == self.target_checkpoint:
            raise RuntimeError(f"crash at {name}")


def snapshot(production="NONE", *, turn=10):
    blockers = (
        [{"type": "city_no_production", "city_ids": ["1"]}]
        if production == "NONE"
        else []
    )
    return RuntimeSnapshot(
        turn=turn,
        game_id="game-1",
        overview={"turn": turn},
        cities={"cities": [{"city_id": 1, "currently_building": production}]},
        blockers=blockers,
    )


def task():
    return ProposedTask(
        task_id="set-production",
        action_type="city_set_production",
        entity_type="city",
        entity_id=1,
        due_turn=10,
        arguments={
            "city_id": 1,
            "item_type": "UNIT",
            "item_name": "UNIT_BUILDER",
        },
        preconditions=[{"type": "city_has_no_production", "city_id": 1}],
        postconditions=[
            {
                "type": "city_production_equals",
                "city_id": 1,
                "item_name": "UNIT_BUILDER",
            }
        ],
        reason="set approved city production",
    )


def seed(store):
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="seed action", tasks=[task()]),
        mode=ExecutionMode.AUTO,
        auto_action_types=set(ACTION_REGISTRY),
        observation_id="obs-seed",
    )


def runtime(store, game, *, planner=None, crash=None, auto_end_turn=False):
    planner = planner or Planner()
    return WorkflowEngine(
        store=store,
        game=game,
        planner=planner,
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=auto_end_turn,
            max_agent_calls_per_turn=0,
            verification_attempts=3,
            verification_delay_seconds=0,
            auto_action_types=set(ACTION_REGISTRY),
            allowed_action_types=set(ACTION_REGISTRY),
        ),
        crash_injector=crash,
    )


def set_retry_state(store, *, retry_count=0, max_retries=2):
    with store._connect() as conn:
        conn.execute(
            """
            UPDATE workflow_tasks SET retry_count=?, max_retries=?
            WHERE game_id='game-1' AND task_id='set-production'
            """,
            (retry_count, max_retries),
        )


def active_attempt_id(store):
    with store._connect() as conn:
        row = conn.execute(
            "SELECT active_attempt_id FROM runtime_state WHERE game_id='game-1'"
        ).fetchone()
    return None if row is None else row["active_attempt_id"]


def failed_attempt(
    *,
    retry_classification=RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
    delivery_status=None,
    verification_evidence=None,
):
    now = datetime.now(UTC)
    return ActionAttempt(
        action_attempt_id="attempt-failed",
        game_session_id="game-1",
        task_id="set-production",
        action_type="city_set_production",
        attempt_number=1,
        request_id="request-failed",
        idempotency_key="idempotency-failed",
        prepared_from_observation_id="obs-before",
        prepared_at=now,
        sent_at=now,
        response_received_at=now,
        status=AttemptStatus.FAILED,
        retry_classification=retry_classification,
        normalized_arguments=task().arguments,
        transport_result=(
            {
                **(
                    {}
                    if delivery_status is None
                    else {"delivery_status": delivery_status}
                ),
                **(
                    {}
                    if verification_evidence is None
                    else {"verification_evidence": verification_evidence}
                ),
            }
            or None
        ),
        tool_result=None,
        verification_status=VerificationStatus.FAILED,
        postconditions=tuple(task().postconditions),
    )


@pytest.mark.parametrize(
    (
        "case",
        "retry_classification",
        "delivery_status",
        "verification_evidence",
        "expected_status",
        "expected_retry_count",
    ),
    [
        (
            "proven-not-sent",
            RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
            MutationDeliveryStatus.PROVEN_NOT_SENT.value,
            None,
            TaskStatus.READY,
            1,
        ),
        (
            "explicit-non-commit",
            RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
            None,
            "EXPLICIT_NON_COMMIT_EVIDENCE",
            TaskStatus.READY,
            1,
        ),
        (
            "conflict",
            RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
            None,
            "CONFLICTING_STATE",
            TaskStatus.FAILED,
            0,
        ),
        (
            "explicit-rejection",
            RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
            MutationDeliveryStatus.EXPLICITLY_REJECTED.value,
            None,
            TaskStatus.FAILED,
            0,
        ),
        (
            "impossible-postcondition",
            RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
            None,
            "IMPOSSIBLE_POSTCONDITION",
            TaskStatus.FAILED,
            0,
        ),
        (
            "never-blind-retry",
            RetryClassification.NEVER_BLIND_RETRY,
            MutationDeliveryStatus.PROVEN_NOT_SENT.value,
            None,
            TaskStatus.FAILED,
            0,
        ),
    ],
)
def test_startup_failed_attempt_uses_canonical_retry_resolution(
    tmp_path,
    case,
    retry_classification,
    delivery_status,
    verification_evidence,
    expected_status,
    expected_retry_count,
):
    database = tmp_path / f"startup-{case}.sqlite3"
    store = WorkflowStore(database)
    seed(store)
    attempt = failed_attempt(
        retry_classification=retry_classification,
        delivery_status=delivery_status,
        verification_evidence=verification_evidence,
    )
    store.save_action_attempt(attempt)
    store.set_task_status("game-1", task().task_id, TaskStatus.VERIFYING)
    store.save_runtime_state(
        "game-1",
        RuntimeState.VERIFYING,
        active_attempt_id=attempt.action_attempt_id,
    )

    restarted = WorkflowStore(database)
    repaired = restarted.get_task("game-1", task().task_id)
    ticks = restarted.list_workflow_ticks("game-1")

    assert repaired.status is expected_status
    assert repaired.retry_count == expected_retry_count
    assert active_attempt_id(restarted) is None
    assert len(ticks) == 1
    assert ticks[0].outcome == "ATTEMPT_RECONCILED"
    assert WorkflowStore(database).list_workflow_ticks("game-1") == ticks
    assert (
        WorkflowStore(database).get_task("game-1", task().task_id).retry_count
        == expected_retry_count
    )


def test_startup_safe_failure_respects_retry_limit(tmp_path):
    database = tmp_path / "startup-limit.sqlite3"
    store = WorkflowStore(database)
    seed(store)
    set_retry_state(store, retry_count=1, max_retries=2)
    attempt = failed_attempt(
        delivery_status=MutationDeliveryStatus.PROVEN_NOT_SENT.value
    )
    store.save_action_attempt(attempt)
    store.set_task_status("game-1", task().task_id, TaskStatus.VERIFYING)

    restarted = WorkflowStore(database)
    repaired = restarted.get_task("game-1", task().task_id)

    assert repaired.status is TaskStatus.ESCALATED
    assert repaired.retry_count == 2
    assert restarted.due_tasks("game-1", 10) == []
    assert WorkflowStore(database).get_task("game-1", task().task_id).retry_count == 2


def test_proven_not_sent_retries_stop_at_task_limit(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "proven-limit.sqlite3")
        seed(store)
        set_retry_state(store, max_retries=2)
        game = Game(
            [snapshot()],
            result=ActionResult(
                success=False,
                message="failed before delivery",
                delivery_status=MutationDeliveryStatus.PROVEN_NOT_SENT,
            ),
        )

        await runtime(store, game).tick()
        first = store.get_task("game-1", task().task_id)
        assert first.status is TaskStatus.READY
        assert first.retry_count == 1

        await runtime(store, game).tick()
        limited = store.get_task("game-1", task().task_id)
        assert limited.status is TaskStatus.ESCALATED
        assert limited.retry_count == 2

        await runtime(store, game).tick()
        attempts = store.list_action_attempts("game-1")
        assert [attempt.attempt_number for attempt in attempts] == [1, 2]
        assert game.mutations == 2
        assert store.due_tasks("game-1", 10) == []

    asyncio.run(scenario())


def test_explicit_non_commit_retries_stop_at_task_limit(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "noncommit-limit.sqlite3")
        seed(store)
        set_retry_state(store, max_retries=2)
        game = Game([snapshot()], result=ActionResult(success=True))

        await runtime(store, game).tick()
        await runtime(store, game).tick()
        assert store.get_task("game-1", task().task_id).retry_count == 1

        await runtime(store, game).tick()
        await runtime(store, game).tick()
        limited = store.get_task("game-1", task().task_id)
        assert limited.status is TaskStatus.ESCALATED
        assert limited.retry_count == 2

        await runtime(store, game).tick()
        assert game.mutations == 2
        assert len(store.list_action_attempts("game-1")) == 2
        assert store.due_tasks("game-1", 10) == []

    asyncio.run(scenario())


def test_retry_count_rolls_back_with_failed_attempt_transaction(tmp_path):
    async def scenario():
        database = tmp_path / "retry-crash.sqlite3"
        store = WorkflowStore(database)
        seed(store)
        set_retry_state(store, max_retries=2)
        game = Game(
            [snapshot()],
            result=ActionResult(
                success=False,
                delivery_status=MutationDeliveryStatus.PROVEN_NOT_SENT,
            ),
        )

        with pytest.raises(RuntimeError, match="after_attempt_failed_update"):
            await runtime(
                store,
                game,
                crash=Crash("after_attempt_failed_update"),
            ).tick()

        rolled_back = store.get_task("game-1", task().task_id)
        assert rolled_back.status is TaskStatus.RUNNING
        assert rolled_back.retry_count == 0

        await runtime(WorkflowStore(database), game).tick()
        recovered = WorkflowStore(database).get_task("game-1", task().task_id)
        assert recovered.status is TaskStatus.READY
        assert recovered.retry_count == 1
        assert (
            WorkflowStore(database).get_task("game-1", task().task_id).retry_count == 1
        )
        assert game.mutations == 1

    asyncio.run(scenario())


def assert_system_error_preserves_attempt(store, result, attempt_id):
    tick = validate_workflow_tick(result)
    assert tick.outcome == "SYSTEM_ERROR"
    assert tick.action_attempt_id == attempt_id
    assert tick.mutation_budget_used == 0
    assert store.load_runtime_state("game-1") is RuntimeState.SYSTEM_ERROR
    assert active_attempt_id(store) == attempt_id
    assert store.list_workflow_ticks("game-1")[-1] == tick


def test_system_error_preserves_prepared_attempt_until_recovery(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "prepared-error.sqlite3")
        seed(store)
        game = Game([snapshot()])
        planner = Planner()
        with pytest.raises(RuntimeError, match="after_attempt_prepared"):
            await runtime(
                store,
                game,
                planner=planner,
                crash=Crash("after_attempt_prepared"),
            ).tick()
        attempt = store.unresolved_action_attempt("game-1")
        assert attempt.status is AttemptStatus.PREPARED

        game.fail_read = True
        error = await runtime(store, game, planner=planner).tick()
        assert_system_error_preserves_attempt(store, error, attempt.action_attempt_id)

        game.fail_read = False
        restarted = WorkflowStore(store.path)
        recovered = await runtime(restarted, game, planner=planner).tick()
        assert recovered.workflow_tick["outcome"] == "ATTEMPT_RECOVERED"
        assert (
            restarted.get_action_attempt(attempt.action_attempt_id).status
            is AttemptStatus.REJECTED_BEFORE_SEND
        )
        assert game.mutations == 0
        assert planner.calls == 0

    asyncio.run(scenario())


def test_system_error_preserves_verifying_attempt_until_reconciliation(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "verifying-error.sqlite3")
        seed(store)
        game = Game([snapshot(), snapshot("UNIT_BUILDER")])
        planner = Planner()
        await runtime(store, game, planner=planner).tick()
        attempt_id = active_attempt_id(store)

        game.fail_read = True
        error = await runtime(store, game, planner=planner).tick()
        assert_system_error_preserves_attempt(store, error, attempt_id)

        game.fail_read = False
        restarted = WorkflowStore(store.path)
        assert active_attempt_id(restarted) == attempt_id
        recovered = await runtime(restarted, game, planner=planner).tick()
        assert recovered.workflow_tick["outcome"] == "ATTEMPT_RECONCILED"
        assert (
            restarted.get_action_attempt(attempt_id).status is AttemptStatus.SUCCEEDED
        )
        assert game.mutations == 1
        assert planner.calls == 0

    asyncio.run(scenario())


def test_system_error_preserves_uncertain_attempt_until_reconciliation(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "uncertain-error.sqlite3")
        seed(store)
        game = Game(
            [snapshot()],
            result=ActionResult(
                success=False,
                delivery_status=MutationDeliveryStatus.UNKNOWN,
            ),
        )
        planner = Planner()
        await runtime(store, game, planner=planner).tick()
        attempt_id = active_attempt_id(store)

        game.fail_tools = True
        error = await runtime(store, game, planner=planner).tick()
        assert_system_error_preserves_attempt(store, error, attempt_id)

        game.fail_tools = False
        restarted = WorkflowStore(store.path)
        assert active_attempt_id(restarted) == attempt_id
        recovered = await runtime(restarted, game, planner=planner).tick()
        assert recovered.workflow_tick["outcome"] == "ATTEMPT_RECONCILED"
        assert restarted.get_action_attempt(attempt_id).status is AttemptStatus.FAILED
        assert game.mutations == 1
        assert planner.calls == 0

    asyncio.run(scenario())


def test_system_error_preserves_turn_transition_until_confirmation(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "turn-error.sqlite3")
        game = Game(
            [snapshot("UNIT_BUILDER", turn=10), snapshot("UNIT_BUILDER", turn=11)]
        )
        planner = Planner()
        await runtime(store, game, planner=planner, auto_end_turn=True).tick()
        attempt_id = active_attempt_id(store)

        game.fail_read = True
        error = await runtime(store, game, planner=planner, auto_end_turn=True).tick()
        assert_system_error_preserves_attempt(store, error, attempt_id)

        game.fail_read = False
        restarted = WorkflowStore(store.path)
        assert active_attempt_id(restarted) == attempt_id
        recovered = await runtime(
            restarted, game, planner=planner, auto_end_turn=True
        ).tick()
        assert recovered.workflow_tick["outcome"] == "TURN_TRANSITION_CONFIRMED"
        assert (
            restarted.get_action_attempt(attempt_id).status is AttemptStatus.SUCCEEDED
        )
        assert game.end_turn_calls == 1
        assert game.mutations == 0
        assert planner.calls == 0

    asyncio.run(scenario())
