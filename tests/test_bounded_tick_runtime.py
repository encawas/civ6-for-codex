import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

from civ6_workflow.actions import ACTION_REGISTRY
from civ6_workflow.domain import (
    ActionAttempt,
    AttemptStatus,
    RetryClassification,
    TickOutcomeKind,
    VerificationStatus,
    validate_workflow_tick,
)
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.mcp_port import (
    BoundedGamePort,
    MutationBudget,
    MutationBudgetExceeded,
)
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
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
        return PlanBundle(summary="no strategic change")


class Game:
    def __init__(self, snapshots, *, result=None, inspect_send=None):
        self.snapshots = list(snapshots)
        self.result = result or ActionResult(success=True)
        self.inspect_send = inspect_send
        self.call_count = 0
        self.reads = 0
        self.mutations = 0
        self.end_turn_calls = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        self.reads += 1
        index = min(self.reads - 1, len(self.snapshots) - 1)
        return self.snapshots[index].model_copy(deep=True)

    async def execute_task(self, task):
        self.call_count += 1
        self.mutations += 1
        if self.inspect_send is not None:
            self.inspect_send(task)
        return self.result

    async def end_turn(self):
        self.call_count += 1
        self.end_turn_calls += 1
        if self.inspect_send is not None:
            self.inspect_send(None)
        return self.result

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


def city_snapshot(production="NONE", *, turn=10):
    blockers = (
        [{"type": "city_no_production", "city_ids": ["1"]}]
        if production in {"NONE", "nothing", ""}
        else []
    )
    return RuntimeSnapshot(
        turn=turn,
        game_id="game-1",
        overview={"turn": turn},
        cities={"cities": [{"city_id": 1, "currently_building": production}]},
        blockers=blockers,
    )


def production_task(task_id="set-production"):
    return ProposedTask(
        task_id=task_id,
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
        reason="continue the approved city queue",
    )


def engine(store, game, planner=None, **config):
    return WorkflowEngine(
        store=store,
        game=game,
        planner=planner or Planner(),
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_action_types=set(ACTION_REGISTRY),
            allowed_action_types=set(ACTION_REGISTRY),
            verification_delay_seconds=0,
            **config,
        ),
    )


def seed_task(store, task=None):
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="seed task", tasks=[task or production_task()]),
        mode=ExecutionMode.AUTO,
        auto_action_types=set(ACTION_REGISTRY),
        observation_id="obs-seed",
    )


def seed_city_queue(store):
    store.save_plan_bundle(
        "game-1",
        9,
        PlanBundle(
            summary="seed city queue",
            city_plan_updates=[
                {
                    "city_id": 1,
                    "followup_queue": [
                        {"item_type": "UNIT", "item_name": "UNIT_BUILDER"}
                    ],
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types=set(ACTION_REGISTRY),
        observation_id="obs-plan",
    )


def test_act_001_second_mutation_is_structurally_rejected():
    async def scenario():
        game = Game([city_snapshot()])
        bounded = BoundedGamePort(game, MutationBudget())
        stored = production_task().model_dump()
        stored.update(plan_id="plan", created_turn=10, status=TaskStatus.READY)
        from civ6_workflow.models import StoredTask

        task = StoredTask.model_validate(stored)
        await bounded.execute_task(task)
        with pytest.raises(MutationBudgetExceeded):
            await bounded.end_turn()
        assert game.mutations == 1
        assert game.end_turn_calls == 0

    asyncio.run(scenario())


def test_task_004_005_city_task_creation_send_and_fresh_verification(tmp_path: Path):
    """TASK-004/005, ACT-001/002/003/004, VER-001/002 vertical slice."""

    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        seed_city_queue(store)
        game = Game(
            [
                city_snapshot("nothing"),
                city_snapshot("NONE"),
                city_snapshot("UNIT_BUILDER"),
            ]
        )
        planner = Planner()
        runtime = engine(store, game, planner)

        created = await runtime.tick()
        assert created.workflow_tick["outcome"] == TickOutcomeKind.TASK_CREATED
        assert game.mutations == 0

        sent = await runtime.tick()
        assert sent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
        assert game.mutations == 1
        assert (
            store.task_status("game-1", sent.workflow_tick["task_id"])
            is TaskStatus.VERIFYING
        )
        assert planner.calls == 0
        assert game.end_turn_calls == 0

        reconciled = await runtime.tick()
        assert reconciled.workflow_tick["outcome"] == TickOutcomeKind.ATTEMPT_RECONCILED
        assert reconciled.executed_task_ids == [sent.workflow_tick["task_id"]]
        attempt = store.get_action_attempt(sent.workflow_tick["action_attempt_id"])
        assert attempt.status is AttemptStatus.SUCCEEDED
        assert attempt.verification_status is VerificationStatus.PASSED
        assert attempt.last_verification_observation_id
        assert attempt.postcondition_version == 1
        assert (
            attempt.last_verification_observation_id
            != attempt.prepared_from_observation_id
        )
        assert validate_workflow_tick(reconciled).mutation_budget_used == 0

    asyncio.run(scenario())


def test_task_created_then_stale_precondition_cancels_without_send(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        seed_city_queue(store)
        game = Game([city_snapshot(), city_snapshot("BUILDING_MONUMENT")])
        runtime = engine(store, game)

        created = await runtime.tick()
        invalidated = await runtime.tick()

        assert created.workflow_tick["outcome"] == TickOutcomeKind.TASK_CREATED
        assert invalidated.workflow_tick["outcome"] == TickOutcomeKind.TASK_INVALIDATED
        assert game.mutations == 0
        assert (
            store.task_status("game-1", created.workflow_tick["task_id"])
            is TaskStatus.CANCELLED
        )

    asyncio.run(scenario())


def test_act_002_attempt_is_durable_before_game_port_call(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        seed_task(store)
        observed = {}

        def inspect(_task):
            attempt = store.unresolved_action_attempt("game-1")
            observed["attempt"] = attempt
            observed["task_status"] = store.task_status("game-1", "set-production")

        game = Game([city_snapshot()], inspect_send=inspect)
        sent = await engine(store, game).tick()

        assert sent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
        assert observed["attempt"].status is AttemptStatus.UNCERTAIN
        assert observed["attempt"].sent_at is not None
        assert observed["task_status"] is TaskStatus.RUNNING

    asyncio.run(scenario())


def test_act_003_ack_ends_tick_without_read_planner_or_end_turn(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        seed_task(store)
        game = Game([city_snapshot(), city_snapshot("UNIT_BUILDER")])
        planner = Planner()
        sent = await engine(store, game, planner, auto_end_turn=True).tick()

        assert sent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
        assert game.reads == 1
        assert game.mutations == 1
        assert game.end_turn_calls == 0
        assert planner.calls == 0
        assert store.task_status("game-1", "set-production") is TaskStatus.VERIFYING

    asyncio.run(scenario())


def test_ver_003_inconclusive_verification_is_read_only(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        seed_task(store)
        game = Game([city_snapshot(), city_snapshot(), city_snapshot()])
        runtime = engine(store, game, verification_attempts=3)

        await runtime.tick()
        waiting = await runtime.tick()

        assert waiting.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_VERIFICATION
        assert game.mutations == 1
        attempt = store.unresolved_action_attempt("game-1")
        assert attempt.status is AttemptStatus.VERIFYING
        assert attempt.verification_status is VerificationStatus.INCONCLUSIVE

    asyncio.run(scenario())


def test_act_005_builder_unknown_is_never_blindly_retried(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        task = ProposedTask(
            task_id="improve-9",
            action_type="builder_improve",
            entity_type="builder",
            entity_id=9,
            due_turn=10,
            arguments={"unit_id": 9, "improvement_type": "IMPROVEMENT_MINE"},
            preconditions=[],
            postconditions=[
                {
                    "type": "unit_build_charges_equals",
                    "unit_id": 9,
                    "charges": 1,
                }
            ],
            reason="perform one irreversible improvement",
        )
        seed_task(store, task)
        snapshot = RuntimeSnapshot(
            turn=10,
            game_id="game-1",
            overview={"turn": 10},
            cities=[],
            units=[
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_BUILDER",
                    "x": 1,
                    "y": 1,
                    "build_charges": 2,
                }
            ],
        )
        game = Game(
            [snapshot, snapshot],
            result=ActionResult(
                success=False,
                message="connection lost",
                delivery_status="unknown",
            ),
        )
        runtime = engine(store, game, verification_attempts=2)

        uncertain = await runtime.tick()
        after_restart = await engine(store, game, verification_attempts=2).tick()

        assert uncertain.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_UNCERTAIN
        assert after_restart.workflow_tick["outcome"] in {
            TickOutcomeKind.AWAITING_VERIFICATION,
            TickOutcomeKind.AWAITING_HUMAN,
        }
        assert game.mutations == 1
        attempt = store.list_action_attempts("game-1")[0]
        assert attempt.retry_classification is RetryClassification.NEVER_BLIND_RETRY
        assert dict(attempt.normalized_arguments) == {
            "unit_id": 9,
            "improvement": "IMPROVEMENT_MINE",
            "action": "improve",
        }

    asyncio.run(scenario())


class Crash:
    def __init__(self, checkpoint):
        self.checkpoint_name = checkpoint

    def checkpoint(self, name):
        if name == self.checkpoint_name:
            raise RuntimeError(f"crash at {name}")


def test_rec_002_prepared_restart_proves_not_sent_and_links_retry(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        seed_task(store)
        game = Game([city_snapshot(), city_snapshot()])
        crashing = engine(store, game)
        crashing.crash_injector = Crash("after_attempt_prepared")

        with pytest.raises(RuntimeError, match="after_attempt_prepared"):
            await crashing.tick()
        assert game.mutations == 0
        prepared = store.unresolved_action_attempt("game-1")
        assert prepared.status is AttemptStatus.PREPARED

        recovered = await engine(store, game).tick()
        assert recovered.workflow_tick["outcome"] == TickOutcomeKind.ATTEMPT_RECOVERED
        assert store.task_status("game-1", "set-production") is TaskStatus.READY
        assert game.mutations == 0

        sent = await engine(store, game).tick()
        attempts = store.list_action_attempts("game-1")
        assert sent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
        assert len(attempts) == 2
        assert attempts[1].parent_attempt_id == attempts[0].action_attempt_id
        assert attempts[0].status is AttemptStatus.REJECTED_BEFORE_SEND

    asyncio.run(scenario())


def test_rec_003_delivery_started_restart_does_not_resend(tmp_path: Path):
    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        seed_task(store)
        game = Game([city_snapshot(), city_snapshot()])
        crashing = engine(store, game)
        crashing.crash_injector = Crash("after_delivery_started")

        with pytest.raises(RuntimeError, match="after_delivery_started"):
            await crashing.tick()
        assert game.mutations == 0
        assert (
            store.unresolved_action_attempt("game-1").status is AttemptStatus.UNCERTAIN
        )

        result = await engine(store, game, verification_attempts=2).tick()
        assert result.workflow_tick["outcome"] in {
            TickOutcomeKind.AWAITING_VERIFICATION,
            TickOutcomeKind.AWAITING_HUMAN,
        }
        assert game.mutations == 0

    asyncio.run(scenario())


def test_turn_001_007_end_turn_requires_later_increased_turn(tmp_path: Path):
    """TURN-001..007: end turn is one protected, observed transition."""

    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        game = Game(
            [
                city_snapshot("UNIT_BUILDER", turn=10),
                city_snapshot("UNIT_BUILDER", turn=10),
                city_snapshot("UNIT_BUILDER", turn=11),
            ]
        )
        runtime = engine(
            store,
            game,
            auto_end_turn=True,
            max_agent_calls_per_turn=0,
            verification_attempts=3,
        )

        started = await runtime.tick()
        waiting = await runtime.tick()
        confirmed = await runtime.tick()

        assert (
            started.workflow_tick["outcome"] == TickOutcomeKind.TURN_TRANSITION_STARTED
        )
        assert started.turn_ended is False
        assert (
            waiting.workflow_tick["outcome"] == TickOutcomeKind.TURN_TRANSITION_WAITING
        )
        assert waiting.turn_ended is False
        assert (
            confirmed.workflow_tick["outcome"]
            == TickOutcomeKind.TURN_TRANSITION_CONFIRMED
        )
        assert confirmed.turn_ended is True
        assert game.end_turn_calls == 1
        attempt = store.list_action_attempts("game-1")[0]
        assert attempt.status is AttemptStatus.SUCCEEDED
        assert attempt.pre_send_turn == 10

    asyncio.run(scenario())


def test_act_006_007_retry_registry_is_explicit_and_not_request_deduped():
    assert {spec.retry_classification for spec in ACTION_REGISTRY.values()} <= {
        RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
        RetryClassification.NEVER_BLIND_RETRY,
    }
    assert (
        ACTION_REGISTRY["builder_improve"].retry_classification
        is RetryClassification.NEVER_BLIND_RETRY
    )
    assert (
        ACTION_REGISTRY["unit_found_city"].retry_classification
        is RetryClassification.NEVER_BLIND_RETRY
    )
    assert all(
        spec.retry_classification is not RetryClassification.IDEMPOTENT_OR_DEDUPED
        for spec in ACTION_REGISTRY.values()
    )
    with pytest.raises(TypeError, match="frozen"):
        ACTION_REGISTRY["late_action"] = ACTION_REGISTRY["unit_skip"]


def test_rec_004_006_migration_is_idempotent_and_preserves_attempt_history(
    tmp_path: Path,
):
    """REC-004/005/006: repeated startup is attempt-aware and history preserving."""

    database = tmp_path / "runtime.sqlite3"
    store = WorkflowStore(database)
    seed_task(store)
    now = datetime.now(UTC)
    attempt = ActionAttempt(
        action_attempt_id="attempt-verifying",
        game_session_id="game-1",
        task_id="set-production",
        action_type="city_set_production",
        attempt_number=1,
        request_id="request-verifying",
        idempotency_key="idempotency-verifying",
        prepared_from_observation_id="obs-before-send",
        prepared_at=now,
        sent_at=now,
        response_received_at=now,
        status=AttemptStatus.VERIFYING,
        retry_classification=RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
        normalized_arguments={
            "city_id": 1,
            "item_type": "UNIT",
            "item_name": "UNIT_BUILDER",
        },
        tool_result={"success": True},
        verification_status=VerificationStatus.PENDING,
        postconditions=tuple(production_task().postconditions),
    )
    store.save_action_attempt(attempt)
    store.set_task_status("game-1", "set-production", TaskStatus.READY)

    first_restart = WorkflowStore(database)
    second_restart = WorkflowStore(database)

    assert first_restart.task_status("game-1", "set-production") is TaskStatus.VERIFYING
    assert (
        second_restart.task_status("game-1", "set-production") is TaskStatus.VERIFYING
    )
    assert second_restart.list_action_attempts("game-1") == [attempt]


def test_ver_004_006_explicit_negative_evidence_retries_on_later_tick(
    tmp_path: Path,
):
    """VER-004/005/006: proven non-commit fails history; retry is a later Tick."""

    async def scenario():
        store = WorkflowStore(tmp_path / "runtime.sqlite3")
        seed_task(store)
        game = Game(
            [
                city_snapshot("NONE"),
                city_snapshot("BUILDING_MONUMENT"),
                city_snapshot("NONE"),
            ]
        )
        runtime = engine(store, game)

        sent = await runtime.tick()
        reconciled = await runtime.tick()

        assert sent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
        assert reconciled.workflow_tick["outcome"] == TickOutcomeKind.ATTEMPT_RECONCILED
        assert reconciled.workflow_tick["attempt_status"] == AttemptStatus.FAILED
        assert game.mutations == 1
        assert store.task_status("game-1", "set-production") is TaskStatus.READY

        retried = await runtime.tick()
        attempts = store.list_action_attempts("game-1")
        assert retried.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
        assert game.mutations == 2
        assert len(attempts) == 2
        assert attempts[1].parent_attempt_id == attempts[0].action_attempt_id

    asyncio.run(scenario())


def test_act_006_structured_delivery_classification(tmp_path: Path):
    """ACT-006: local reject, proven-not-sent, unknown, reject, and ack differ."""

    async def scenario():
        invalid_store = WorkflowStore(tmp_path / "invalid.sqlite3")
        invalid = production_task("invalid").model_copy(
            update={"arguments": {"city_id": 1}}
        )
        seed_task(invalid_store, invalid)
        invalid_game = Game([city_snapshot()])
        local = await engine(invalid_store, invalid_game).tick()
        assert local.workflow_tick["outcome"] == TickOutcomeKind.TASK_INVALIDATED
        assert invalid_game.mutations == 0
        assert invalid_store.list_action_attempts("game-1") == []

        unsent_store = WorkflowStore(tmp_path / "unsent.sqlite3")
        seed_task(unsent_store)
        unsent_game = Game(
            [city_snapshot()],
            result=ActionResult(
                success=False,
                message="connect failed before delivery",
                delivery_status="proven_not_sent",
            ),
        )
        unsent = await engine(unsent_store, unsent_game).tick()
        unsent_attempt = unsent_store.list_action_attempts("game-1")[0]
        assert unsent.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_REJECTED
        assert unsent_attempt.status is AttemptStatus.FAILED
        assert unsent_store.task_status("game-1", "set-production") is TaskStatus.READY

        rejected_store = WorkflowStore(tmp_path / "rejected.sqlite3")
        seed_task(rejected_store)
        rejected_game = Game(
            [city_snapshot()],
            result=ActionResult(
                success=False,
                blocked=True,
                message="game rejected request",
                delivery_status="explicitly_rejected",
            ),
        )
        rejected = await engine(rejected_store, rejected_game).tick()
        assert rejected.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_REJECTED
        assert (
            rejected_store.task_status("game-1", "set-production") is TaskStatus.FAILED
        )

    asyncio.run(scenario())


def test_turn_003_007_approval_restart_and_future_task_boundaries(tmp_path: Path):
    """TURN-003..007 and REC-006: terminal states block; future work does not."""

    async def scenario():
        approval_store = WorkflowStore(tmp_path / "approval.sqlite3")
        approval_store.save_plan_bundle(
            "game-1",
            10,
            PlanBundle(summary="approval task", tasks=[production_task()]),
            mode=ExecutionMode.CONFIRM,
            auto_action_types=set(ACTION_REGISTRY),
            observation_id="obs-approval",
        )
        approval_game = Game([city_snapshot("UNIT_BUILDER")])
        approval_config = EngineConfig(
            execution_mode=ExecutionMode.CONFIRM,
            auto_end_turn=True,
            max_agent_calls_per_turn=0,
            auto_action_types=set(ACTION_REGISTRY),
            allowed_action_types=set(ACTION_REGISTRY),
            verification_delay_seconds=0,
        )
        first = await WorkflowEngine(
            store=approval_store,
            game=approval_game,
            planner=Planner(),
            config=approval_config,
        ).tick()
        restarted = await WorkflowEngine(
            store=WorkflowStore(tmp_path / "approval.sqlite3"),
            game=approval_game,
            planner=Planner(),
            config=approval_config,
        ).tick()
        assert first.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_APPROVAL
        assert restarted.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_APPROVAL
        assert approval_game.end_turn_calls == 0

        future_store = WorkflowStore(tmp_path / "future.sqlite3")
        future_task = production_task("future").model_copy(update={"due_turn": 11})
        seed_task(future_store, future_task)
        future_game = Game([city_snapshot("UNIT_BUILDER")])
        transition = await engine(
            future_store,
            future_game,
            auto_end_turn=True,
            max_agent_calls_per_turn=0,
        ).tick()
        assert transition.workflow_tick["outcome"] == (
            TickOutcomeKind.TURN_TRANSITION_STARTED
        )
        assert future_game.end_turn_calls == 1

    asyncio.run(scenario())
