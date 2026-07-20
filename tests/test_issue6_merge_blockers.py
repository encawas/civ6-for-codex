import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import civ6_workflow.engine as engine_module
from civ6_workflow.actions import ACTION_REGISTRY
from civ6_workflow.domain import (
    ActionAttempt,
    AttemptReconciledTick,
    AttemptStatus,
    RetryClassification,
    RuntimeState,
    VerificationStatus,
    validate_workflow_tick,
)
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.mcp_port import Civ6McpClient, McpServerConfig
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.safe_mcp_port import SafeCiv6GamePort
from civ6_workflow.store import WorkflowStore


class Planner:
    def __init__(self):
        self.calls = 0

    async def plan(self, request):
        self.calls += 1
        return PlanBundle(summary="no planner work")


class StatefulGame:
    def __init__(self, snapshots, *, result=None):
        self.snapshots = list(snapshots)
        self.result = result or ActionResult(success=True)
        self.call_count = 0
        self.reads = 0
        self.mutations = 0
        self.end_turn_calls = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        self.reads += 1
        index = min(self.reads - 1, len(self.snapshots) - 1)
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
        return _tools()


class Crash:
    def __init__(self, checkpoint):
        self.target_checkpoint = checkpoint

    def checkpoint(self, name):
        if name == self.target_checkpoint:
            raise RuntimeError(f"crash at {name}")


def _tools():
    return {
        "get_notifications",
        "get_pending_diplomacy",
        "get_pending_trades",
        "set_city_production",
        "set_research",
        "unit_action",
        "end_turn",
    }


def _snapshot(production="NONE", *, turn=10, units=None):
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
        units=units,
        blockers=blockers,
    )


def _task(task_id="set-production", **updates):
    task = ProposedTask(
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
        reason="set approved city production",
    )
    return task.model_copy(update=updates)


def _seed(store, task=None, *, mode=ExecutionMode.AUTO):
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="seed action", tasks=[task or _task()]),
        mode=mode,
        auto_action_types=set(ACTION_REGISTRY),
        observation_id="obs-seed",
    )


def _active_attempt_id(store, game_id="game-1"):
    with store._connect() as conn:
        row = conn.execute(
            "SELECT active_attempt_id FROM runtime_state WHERE game_id=?",
            (game_id,),
        ).fetchone()
    return None if row is None else row["active_attempt_id"]


def _engine(store, game, *, crash=None, auto_end_turn=False, attempts=3):
    return WorkflowEngine(
        store=store,
        game=game,
        planner=Planner(),
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=auto_end_turn,
            max_agent_calls_per_turn=0,
            verification_attempts=attempts,
            verification_delay_seconds=0,
            auto_action_types=set(ACTION_REGISTRY),
            allowed_action_types=set(ACTION_REGISTRY),
        ),
        crash_injector=crash,
    )


def test_atomic_success_finalization_rolls_back_internal_crash(tmp_path: Path):
    async def scenario():
        database = tmp_path / "success.sqlite3"
        store = WorkflowStore(database)
        _seed(store)
        game = StatefulGame(
            [_snapshot(), _snapshot("UNIT_BUILDER"), _snapshot("UNIT_BUILDER")]
        )
        await _engine(store, game).tick()

        with pytest.raises(RuntimeError, match="after_attempt_succeeded_update"):
            await _engine(
                store,
                game,
                crash=Crash("after_attempt_succeeded_update"),
            ).tick()

        attempt = store.unresolved_action_attempt("game-1")
        assert attempt.status is AttemptStatus.VERIFYING
        assert store.task_status("game-1", "set-production") is TaskStatus.VERIFYING
        assert store.load_runtime_state("game-1") is RuntimeState.ACTION_SENT
        assert len(store.list_workflow_ticks("game-1")) == 1

        recovered = await _engine(WorkflowStore(database), game).tick()
        assert recovered.workflow_tick["attempt_status"] == AttemptStatus.SUCCEEDED
        assert (
            store.get_action_attempt(attempt.action_attempt_id).status
            is AttemptStatus.SUCCEEDED
        )
        assert store.task_status("game-1", "set-production") is TaskStatus.DONE
        assert _active_attempt_id(store) is None
        assert game.mutations == 1

    asyncio.run(scenario())


def test_atomic_failure_finalization_rolls_back_internal_crash(tmp_path: Path):
    async def scenario():
        database = tmp_path / "failure.sqlite3"
        store = WorkflowStore(database)
        _seed(store)
        game = StatefulGame(
            [
                _snapshot(),
                _snapshot("BUILDING_MONUMENT"),
                _snapshot("BUILDING_MONUMENT"),
            ]
        )
        await _engine(store, game).tick()

        with pytest.raises(RuntimeError, match="after_attempt_failed_update"):
            await _engine(
                store,
                game,
                crash=Crash("after_attempt_failed_update"),
            ).tick()

        attempt = store.unresolved_action_attempt("game-1")
        assert attempt.status is AttemptStatus.VERIFYING
        assert store.task_status("game-1", "set-production") is TaskStatus.VERIFYING

        recovered = await _engine(WorkflowStore(database), game).tick()
        assert recovered.workflow_tick["attempt_status"] == AttemptStatus.FAILED
        assert store.task_status("game-1", "set-production") is TaskStatus.FAILED
        assert _active_attempt_id(store) is None
        assert game.mutations == 1

    asyncio.run(scenario())


def test_atomic_prepared_recovery_rolls_back_internal_crash(tmp_path: Path):
    async def scenario():
        database = tmp_path / "prepared.sqlite3"
        store = WorkflowStore(database)
        _seed(store)
        game = StatefulGame([_snapshot(), _snapshot(), _snapshot()])

        with pytest.raises(RuntimeError, match="after_attempt_prepared"):
            await _engine(store, game, crash=Crash("after_attempt_prepared")).tick()

        with pytest.raises(
            RuntimeError, match="after_prepared_attempt_rejected_update"
        ):
            await _engine(
                WorkflowStore(database),
                game,
                crash=Crash("after_prepared_attempt_rejected_update"),
            ).tick()

        attempt = store.unresolved_action_attempt("game-1")
        assert attempt.status is AttemptStatus.PREPARED
        assert store.task_status("game-1", "set-production") is TaskStatus.RUNNING

        recovered = await _engine(WorkflowStore(database), game).tick()
        assert recovered.workflow_tick["outcome"] == "ATTEMPT_RECOVERED"
        final = store.get_action_attempt(attempt.action_attempt_id)
        assert final.status is AttemptStatus.REJECTED_BEFORE_SEND
        assert store.task_status("game-1", "set-production") is TaskStatus.READY
        assert _active_attempt_id(store) is None
        assert game.mutations == 0

    asyncio.run(scenario())


def test_atomic_turn_confirmation_rolls_back_internal_crash(tmp_path: Path):
    async def scenario():
        database = tmp_path / "turn.sqlite3"
        store = WorkflowStore(database)
        game = StatefulGame(
            [
                _snapshot("UNIT_BUILDER", turn=10),
                _snapshot("UNIT_BUILDER", turn=11),
                _snapshot("UNIT_BUILDER", turn=11),
            ]
        )
        started = await _engine(store, game, auto_end_turn=True).tick()
        attempt_id = started.workflow_tick["action_attempt_id"]

        with pytest.raises(RuntimeError, match="after_end_turn_succeeded_update"):
            await _engine(
                WorkflowStore(database),
                game,
                crash=Crash("after_end_turn_succeeded_update"),
                auto_end_turn=True,
            ).tick()

        attempt = store.get_action_attempt(attempt_id)
        assert attempt.status is AttemptStatus.VERIFYING
        assert store.load_runtime_state("game-1") is RuntimeState.TURN_TRANSITIONING

        confirmed = await _engine(
            WorkflowStore(database), game, auto_end_turn=True
        ).tick()
        assert confirmed.workflow_tick["outcome"] == "TURN_TRANSITION_CONFIRMED"
        assert store.get_action_attempt(attempt_id).status is AttemptStatus.SUCCEEDED
        assert store.load_runtime_state("game-1") is RuntimeState.OBSERVING
        assert _active_attempt_id(store) is None
        assert game.end_turn_calls == 1

    asyncio.run(scenario())


def test_tick_and_runtime_state_roll_back_together(tmp_path: Path):
    async def scenario():
        database = tmp_path / "tick.sqlite3"
        store = WorkflowStore(database)
        game = StatefulGame([_snapshot("UNIT_BUILDER")])

        with pytest.raises(RuntimeError, match="after_runtime_state_update"):
            await _engine(
                store,
                game,
                crash=Crash("after_runtime_state_update"),
            ).tick()

        assert store.load_runtime_state("game-1") is RuntimeState.OBSERVING
        assert store.list_workflow_ticks("game-1") == []

        recovered = await _engine(WorkflowStore(database), game).tick()
        assert recovered.workflow_tick["outcome"] == "NO_SAFE_ACTION"
        assert len(store.list_workflow_ticks("game-1")) == 1
        assert game.mutations == 0
        assert game.end_turn_calls == 0

    asyncio.run(scenario())


def test_startup_repairs_legacy_terminal_attempt_inconsistency(tmp_path: Path):
    database = tmp_path / "legacy-terminal.sqlite3"
    store = WorkflowStore(database)
    _seed(store)
    now = datetime.now(UTC)
    attempt = ActionAttempt(
        action_attempt_id="attempt-terminal",
        game_session_id="game-1",
        task_id="set-production",
        action_type="city_set_production",
        attempt_number=1,
        request_id="request-terminal",
        idempotency_key="idempotency-terminal",
        prepared_from_observation_id="obs-before",
        prepared_at=now,
        sent_at=now,
        response_received_at=now,
        status=AttemptStatus.VERIFYING,
        retry_classification=RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
        normalized_arguments=_task().arguments,
        tool_result={"success": True},
        verification_status=VerificationStatus.PENDING,
        postconditions=tuple(_task().postconditions),
    )
    store.save_action_attempt(attempt)
    succeeded = attempt.model_copy(
        update={
            "status": AttemptStatus.SUCCEEDED,
            "verification_status": VerificationStatus.PASSED,
            "last_verification_observation_id": "obs-after",
        }
    )
    store.update_action_attempt(succeeded)
    store.set_task_status("game-1", "set-production", TaskStatus.VERIFYING)
    store.save_runtime_state(
        "game-1",
        RuntimeState.VERIFYING,
        active_attempt_id=attempt.action_attempt_id,
    )

    restarted = WorkflowStore(database)

    assert restarted.task_status("game-1", "set-production") is TaskStatus.DONE
    assert restarted.load_runtime_state("game-1") is RuntimeState.ROUTING
    assert _active_attempt_id(restarted) is None
    ticks = restarted.list_workflow_ticks("game-1")
    assert len(ticks) == 1
    assert ticks[0].outcome == "ATTEMPT_RECONCILED"
    assert ticks[0].action_attempt_id == attempt.action_attempt_id

    restarted_again = WorkflowStore(database)
    assert restarted_again.list_workflow_ticks("game-1") == ticks


def test_startup_recovers_missing_terminal_end_turn_audit(tmp_path: Path):
    database = tmp_path / "legacy-terminal-turn.sqlite3"
    store = WorkflowStore(database)
    now = datetime.now(UTC)
    verifying = ActionAttempt(
        action_attempt_id="attempt-terminal-turn",
        game_session_id="game-1",
        task_id="end_turn:10",
        action_type="end_turn",
        attempt_number=1,
        request_id="request-terminal-turn",
        idempotency_key="game-1:end_turn:10",
        prepared_from_observation_id="obs-turn-10",
        prepared_at=now,
        sent_at=now,
        response_received_at=now,
        status=AttemptStatus.VERIFYING,
        retry_classification=RetryClassification.NEVER_BLIND_RETRY,
        normalized_arguments={},
        tool_result={"success": True},
        verification_status=VerificationStatus.PENDING,
        pre_send_turn=10,
    )
    store.save_action_attempt(verifying)
    succeeded = verifying.model_copy(
        update={
            "status": AttemptStatus.SUCCEEDED,
            "verification_status": VerificationStatus.PASSED,
            "last_verification_observation_id": "obs-turn-11",
            "verification_count": 1,
        }
    )
    store.update_action_attempt(succeeded)
    store.save_runtime_state(
        "game-1",
        RuntimeState.TURN_TRANSITIONING,
        active_attempt_id=succeeded.action_attempt_id,
    )

    restarted = WorkflowStore(database)

    assert restarted.load_runtime_state("game-1") is RuntimeState.OBSERVING
    assert _active_attempt_id(restarted) is None
    ticks = restarted.list_workflow_ticks("game-1")
    assert len(ticks) == 1
    assert ticks[0].outcome == "TURN_TRANSITION_CONFIRMED"
    assert ticks[0].action_attempt_id == succeeded.action_attempt_id
    assert WorkflowStore(database).list_workflow_ticks("game-1") == ticks


class FakeStateApi:
    def __init__(self, *, production="NONE"):
        self.call_count = 0
        self.production = production

    async def get_optional(self, path):
        self.call_count += 1
        return {
            "overview": {"turn": 10},
            "identity": {"civ": "game", "seed": 1},
            "tech_civics": {},
            "cities": [{"city_id": 1, "currently_building": self.production}],
            "units": [],
            "notifications": [],
            "end_turn_blockers": [],
            "pending_diplomacy": [],
            "pending_trades": [],
        }


class FakeMcpSession:
    def __init__(self, behavior):
        self.behavior = behavior
        self.action_calls = 0

    async def list_tools(self):
        return SimpleNamespace(
            tools=[SimpleNamespace(name=name) for name in sorted(_tools())]
        )

    async def call_tool(self, name, arguments):
        self.action_calls += 1
        if self.behavior == "is_error":
            return SimpleNamespace(
                isError=True,
                content=[SimpleNamespace(text="server rejected mutation")],
                structuredContent=None,
            )
        if self.behavior == "empty_reflections":
            return SimpleNamespace(
                isError=False,
                content=[
                    SimpleNamespace(
                        text=(
                            "Empty reflections: tactical, strategic, tooling, planning, hypothesis. "
                            "Provide non-empty entries for all 5 fields: tactical, strategic, "
                            "tooling, planning, hypothesis."
                        )
                    )
                ],
                structuredContent=None,
            )
        if self.behavior == "timeout":
            raise TimeoutError("transport timed out")
        if self.behavior == "connection_reset":
            raise ConnectionResetError("connection reset")
        return SimpleNamespace(
            isError=False,
            content=[],
            structuredContent={"success": True, "message": "acknowledged"},
        )


def _real_mcp_engine(tmp_path, behavior, *, task=None):
    store = WorkflowStore(tmp_path / f"{behavior}.sqlite3")
    store.save_plan_bundle(
        "game:1",
        10,
        PlanBundle(summary="seed MCP action", tasks=[task or _task()]),
        mode=ExecutionMode.AUTO,
        auto_action_types=set(ACTION_REGISTRY),
        observation_id="obs-seed",
    )
    client = Civ6McpClient(McpServerConfig())
    session = FakeMcpSession(behavior)
    client.session = session
    game = SafeCiv6GamePort(
        client,
        FakeStateApi(),
        allowed_tools=_tools(),
    )
    return store, session, _engine(store, game)


@pytest.mark.parametrize(
    ("behavior", "outcome", "attempt_status", "task_status"),
    [
        (
            "is_error",
            "MUTATION_REJECTED",
            AttemptStatus.FAILED,
            TaskStatus.FAILED,
        ),
        (
            "timeout",
            "MUTATION_UNCERTAIN",
            AttemptStatus.UNCERTAIN,
            TaskStatus.UNCERTAIN,
        ),
        (
            "connection_reset",
            "MUTATION_UNCERTAIN",
            AttemptStatus.UNCERTAIN,
            TaskStatus.UNCERTAIN,
        ),
        (
            "ack",
            "MUTATION_SENT",
            AttemptStatus.VERIFYING,
            TaskStatus.VERIFYING,
        ),
    ],
)
def test_real_mcp_client_port_engine_delivery_classification(
    tmp_path,
    behavior,
    outcome,
    attempt_status,
    task_status,
):
    async def scenario():
        store, session, runtime = _real_mcp_engine(tmp_path, behavior)
        result = await runtime.tick()
        attempt = store.list_action_attempts("game:1")[0]

        assert result.workflow_tick["outcome"] == outcome
        assert attempt.status is attempt_status
        assert store.task_status("game:1", "set-production") is task_status
        assert session.action_calls == 1

    asyncio.run(scenario())


def test_real_mcp_chain_local_argument_error_is_proven_not_sent(tmp_path):
    async def scenario():
        invalid = _task("invalid").model_copy(update={"arguments": {"city_id": 1}})
        store, session, runtime = _real_mcp_engine(
            tmp_path, "local_error", task=invalid
        )
        result = await runtime.tick()

        assert result.workflow_tick["outcome"] == "TASK_INVALIDATED"
        assert store.list_action_attempts("game:1") == []
        assert session.action_calls == 0

    asyncio.run(scenario())


class FaultGame(StatefulGame):
    def __init__(self, phase):
        super().__init__([_snapshot("UNIT_BUILDER")])
        self.phase = phase

    async def read_snapshot(self, *, include_units=False):
        if self.phase == "read":
            self.call_count += 1
            raise ConnectionError("snapshot unavailable")
        return await super().read_snapshot(include_units=include_units)

    async def list_tools(self):
        if self.phase == "tools":
            raise RuntimeError("tool discovery failed")
        return await super().list_tools()


def _assert_system_error(store, game, result):
    tick = validate_workflow_tick(result)
    assert tick.outcome == "SYSTEM_ERROR"
    assert tick.mutation_budget_used == 0
    assert tick.error_category
    assert tick.diagnostic_summary
    assert store.load_runtime_state(tick.game_session_id) is RuntimeState.SYSTEM_ERROR
    assert len(store.list_workflow_ticks(tick.game_session_id)) == 1
    assert game.mutations == 0
    assert game.end_turn_calls == 0


def test_read_snapshot_failure_persists_system_error(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "read-error.sqlite3")
        store.set_meta("last_game_id", "game-1")
        store.set_meta("last_observed_turn", 10)
        game = FaultGame("read")
        planner = Planner()
        runtime = _engine(store, game)
        runtime.planner = planner

        result = await runtime.tick()

        _assert_system_error(store, game, result)
        assert result.workflow_tick["error_category"] == "ConnectionError"
        assert planner.calls == 0

    asyncio.run(scenario())


def test_normalization_failure_persists_system_error(tmp_path, monkeypatch):
    async def scenario():
        store = WorkflowStore(tmp_path / "normalize-error.sqlite3")
        store.set_meta("last_game_id", "game-1")
        store.set_meta("last_observed_turn", 10)
        game = FaultGame("normalization")
        planner = Planner()
        runtime = _engine(store, game)
        runtime.planner = planner

        def fail_normalization(snapshot):
            raise ValueError("normalization contract failed")

        with monkeypatch.context() as patch:
            patch.setattr(
                engine_module, "normalize_runtime_snapshot", fail_normalization
            )
            result = await runtime.tick()

        _assert_system_error(store, game, result)
        assert result.workflow_tick["error_category"] == "ValueError"
        assert planner.calls == 0

    asyncio.run(scenario())


def test_list_tools_failure_persists_system_error(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "tools-error.sqlite3")
        game = FaultGame("tools")
        planner = Planner()
        runtime = _engine(store, game)
        runtime.planner = planner

        result = await runtime.tick()

        _assert_system_error(store, game, result)
        assert result.workflow_tick["error_category"] == "RuntimeError"
        assert planner.calls == 0

    asyncio.run(scenario())


def test_rule_compiler_failure_persists_system_error(tmp_path, monkeypatch):
    async def scenario():
        store = WorkflowStore(tmp_path / "rules-error.sqlite3")
        game = FaultGame("rules")
        planner = Planner()
        runtime = _engine(store, game)
        runtime.planner = planner

        def fail_rules(observation):
            raise RuntimeError("rule compiler failed")

        monkeypatch.setattr(runtime.rules, "compile", fail_rules)
        result = await runtime.tick()

        _assert_system_error(store, game, result)
        assert result.workflow_tick["error_category"] == "RuntimeError"
        assert planner.calls == 0

    asyncio.run(scenario())


def test_replay_round_trip_preserves_attempt_transition_history(tmp_path):
    source = WorkflowStore(tmp_path / "source-replay.sqlite3")
    _seed(source)
    now = datetime.now(UTC)
    prepared = ActionAttempt(
        action_attempt_id="attempt-replay",
        game_session_id="game-1",
        task_id="set-production",
        action_type="city_set_production",
        attempt_number=1,
        request_id="request-replay",
        idempotency_key="idempotency-replay",
        prepared_from_observation_id="obs-before",
        prepared_at=now,
        status=AttemptStatus.PREPARED,
        retry_classification=RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
        normalized_arguments=_task().arguments,
        postconditions=tuple(_task().postconditions),
    )
    source.save_action_attempt(prepared)
    uncertain = prepared.model_copy(
        update={
            "status": AttemptStatus.UNCERTAIN,
            "sent_at": now,
            "transport_result": {"phase": "delivery_started"},
        }
    )
    source.update_action_attempt(uncertain)
    verifying = uncertain.model_copy(
        update={
            "status": AttemptStatus.VERIFYING,
            "response_received_at": now,
            "tool_result": {"success": True},
            "verification_status": VerificationStatus.PENDING,
        }
    )
    source.update_action_attempt(verifying)
    succeeded = verifying.model_copy(
        update={
            "status": AttemptStatus.SUCCEEDED,
            "verification_status": VerificationStatus.PASSED,
            "last_verification_observation_id": "obs-after",
            "verification_count": 1,
        }
    )
    source.update_action_attempt(succeeded)
    source.set_task_status("game-1", "set-production", TaskStatus.DONE)
    source.save_runtime_state("game-1", RuntimeState.OBSERVING)
    tick = AttemptReconciledTick(
        tick_id="tick-replay",
        game_session_id="game-1",
        turn_number=10,
        starting_runtime_state=RuntimeState.VERIFYING,
        observation_ids=("obs-after",),
        started_at=now,
        completed_at=now,
        metrics={},
        action_attempt_id=succeeded.action_attempt_id,
        task_id=succeeded.task_id,
        attempt_status=AttemptStatus.SUCCEEDED,
    )
    source.save_workflow_tick(tick)
    exported = source.export_replay_state("game-1")

    target = WorkflowStore(tmp_path / "target-replay.sqlite3")
    target.import_replay_state(exported)
    target.import_replay_state(exported)
    restored = target.export_replay_state("game-1")

    assert target.list_action_attempts("game-1") == [succeeded]
    assert target.load_runtime_state("game-1") is RuntimeState.OBSERVING
    assert target.list_workflow_ticks("game-1") == [tick]
    assert len(restored["tables"]["action_attempt_transitions"]) == 4
    for table in (
        "action_attempts",
        "action_attempt_transitions",
        "runtime_state",
        "workflow_ticks",
        "turn_metrics",
    ):
        assert restored["tables"][table] == exported["tables"][table]


def test_unit_moved_to_third_location_is_conflict_not_noncommit(tmp_path):
    async def scenario():
        store = WorkflowStore(tmp_path / "unit-conflict.sqlite3")
        task = ProposedTask(
            task_id="move-9",
            action_type="unit_move",
            entity_type="unit",
            entity_id=9,
            due_turn=10,
            arguments={"unit_id": 9, "target_x": 3, "target_y": 1},
            preconditions=[{"type": "unit_at", "unit_id": 9, "x": 1, "y": 1}],
            postconditions=[{"type": "unit_at", "unit_id": 9, "x": 3, "y": 1}],
            reason="move unit to approved target",
        )
        _seed(store, task)
        initial = _snapshot(
            "UNIT_BUILDER",
            units=[
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_WARRIOR",
                    "x": 1,
                    "y": 1,
                    "moves_remaining": 2,
                }
            ],
        )
        third = _snapshot(
            "UNIT_BUILDER",
            units=[
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_WARRIOR",
                    "x": 2,
                    "y": 1,
                    "moves_remaining": 1,
                }
            ],
        )
        game = StatefulGame([initial, initial, third, third, third])
        runtime = _engine(store, game)

        await runtime.tick()
        reconciled = await runtime.tick()
        await runtime.tick()

        attempt = store.list_action_attempts("game-1")[0]
        assert reconciled.workflow_tick["attempt_status"] == AttemptStatus.FAILED
        assert attempt.transport_result["verification_evidence"] == (
            "CONFLICTING_STATE"
        )
        assert store.task_status("game-1", "move-9") is TaskStatus.FAILED
        assert game.mutations == 1
        assert len(store.list_action_attempts("game-1")) == 1

    asyncio.run(scenario())


def test_real_mcp_empty_reflections_is_an_explicit_end_turn_rejection(tmp_path):
    async def scenario():
        store, session, runtime = _real_mcp_end_turn_engine(
            tmp_path, "empty_reflections"
        )

        result = await runtime.tick()
        attempt = store.list_action_attempts("game:1")[0]

        assert result.workflow_tick["outcome"] == "MUTATION_REJECTED"
        assert attempt.status is AttemptStatus.FAILED
        assert attempt.transport_result["delivery_status"] == "explicitly_rejected"
        assert attempt.tool_result["success"] is False
        assert attempt.tool_result["details"]["rejection_code"] == (
            "end_turn_reflections_required"
        )
        assert session.action_calls == 1

    asyncio.run(scenario())


def test_real_mcp_end_turn_acknowledgement_remains_verifying(tmp_path):
    async def scenario():
        store, session, runtime = _real_mcp_end_turn_engine(tmp_path, "ack")

        result = await runtime.tick()
        attempt = store.list_action_attempts("game:1")[0]

        assert result.workflow_tick["outcome"] == "TURN_TRANSITION_STARTED"
        assert attempt.status is AttemptStatus.VERIFYING
        assert attempt.transport_result["delivery_status"] == "acknowledged"
        assert session.action_calls == 1

    asyncio.run(scenario())


def _real_mcp_end_turn_engine(tmp_path, behavior):
    store = WorkflowStore(tmp_path / f"end-turn-{behavior}.sqlite3")
    client = Civ6McpClient(McpServerConfig())
    session = FakeMcpSession(behavior)
    client.session = session
    game = SafeCiv6GamePort(
        client,
        FakeStateApi(production="UNIT_BUILDER"),
        allowed_tools=_tools(),
    )
    return store, session, _engine(store, game, auto_end_turn=True)
