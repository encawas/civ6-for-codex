from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import civ6_workflow.runtime as runtime_module
from civ6_workflow.mcp_port import (
    Civ6GamePort,
    Civ6McpClient,
    McpMutationTimeoutError,
    McpMutationTransportError,
    McpClientState,
    McpServerConfig,
)
from civ6_workflow.models import (
    ActionResult,
    MutationDeliveryStatus,
    RuntimeSnapshot,
    TickMetrics,
)
from civ6_workflow.mutation_protocol import McpToolEnvelope, MutationToolAdapter
from civ6_workflow.ports import MutationBudget
from civ6_workflow.runtime import RuntimeConfig, WorkflowRuntime


def _result(tool: str, raw, *, text: str = "", is_error: bool = False):
    envelope = McpToolEnvelope._build(tool, raw, text, is_error)
    return MutationToolAdapter().interpret(envelope)


@pytest.mark.parametrize(
    ("tool", "value"),
    [
        ("set_research", "RESEARCHING: TECH_WRITING"),
        ("set_city_production", "PRODUCING: UNIT_BUILDER"),
        ("send_envoy", "ENVOY_SENT: 12"),
        ("unit_action", "MOVING_TO: 4,5"),
        ("end_turn", "Turn 10 -> 11"),
    ],
)
def test_known_tool_success_grammars_are_acknowledged(tool, value):
    result = _result(tool, {"result": value})
    assert result.delivery_status is MutationDeliveryStatus.ACKNOWLEDGED
    assert result.success is True


@pytest.mark.parametrize(
    ("tool", "value"),
    [
        ("set_research", "Error: TECH_NOT_FOUND|missing"),
        ("set_city_production", "Error: CANNOT_PRODUCE|blocked"),
        ("send_envoy", "Error: NO_ENVOYS|none available"),
        ("unit_action", "Error: NO_MOVES|unit exhausted"),
        ("end_turn", "  End turn blocked: pending choice"),
    ],
)
def test_only_stable_business_rejections_are_explicit(tool, value):
    result = _result(tool, {"result": value})
    assert result.delivery_status is MutationDeliveryStatus.EXPLICITLY_REJECTED
    assert result.blocked is True


@pytest.mark.parametrize(
    "raw",
    [
        {},
        {"status": "error"},
        {"result": "FAILED"},
        {"success": False, "error": "timeout waiting for game response"},
        None,
        [],
        False,
        "arbitrary text",
    ],
)
def test_unknown_or_partial_results_never_default_to_ack(raw):
    result = MutationToolAdapter().interpret(
        McpToolEnvelope.from_legacy_payload("unit_action", raw)
    )
    assert result.delivery_status is MutationDeliveryStatus.UNKNOWN
    assert result.success is False


@pytest.mark.parametrize("field", ["result", "error", "message"])
def test_connection_failure_evidence_is_proven_not_sent(field):
    result = _result(
        "unit_action",
        {field: "Cannot connect to Civ 6 at 127.0.0.1:4318"},
    )
    assert result.delivery_status is MutationDeliveryStatus.PROVEN_NOT_SENT
    assert result.blocked is False


def test_is_error_internal_failure_is_unknown_but_stable_rejection_is_explicit():
    internal = _result(
        "unit_action",
        {"result": "response serialization failed"},
        is_error=True,
    )
    rejected = _result(
        "unit_action",
        {"result": "Error: UNIT_NOT_FOUND|gone"},
        is_error=True,
    )
    assert internal.delivery_status is MutationDeliveryStatus.UNKNOWN
    assert rejected.delivery_status is MutationDeliveryStatus.EXPLICITLY_REJECTED


def test_structured_and_text_evidence_must_agree():
    agreed = _result(
        "set_research",
        {"result": "RESEARCHING: TECH_WRITING"},
        text='{"result":"RESEARCHING: TECH_WRITING"}',
    )
    conflict = _result(
        "set_research",
        {"result": "RESEARCHING: TECH_WRITING"},
        text='{"result":"Error: TECH_NOT_FOUND|missing"}',
    )
    assert agreed.delivery_status is MutationDeliveryStatus.ACKNOWLEDGED
    assert conflict.delivery_status is MutationDeliveryStatus.UNKNOWN


def test_large_and_unserializable_results_have_bounded_diagnostics():
    circular = {}
    circular["self"] = circular
    envelope = McpToolEnvelope.from_legacy_payload("unit_action", circular)
    large = _result("unit_action", {"result": "x" * 10_000})
    assert len(envelope.diagnostic_excerpt) <= 1024
    assert len(envelope.raw_result_hash) == 64
    assert len(large.message) <= 1024
    assert len(large.details["diagnostic_excerpt"]) <= 1024


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "success": True,
            "delivery_status": MutationDeliveryStatus.EXPLICITLY_REJECTED,
        },
        {
            "success": False,
            "delivery_status": MutationDeliveryStatus.ACKNOWLEDGED,
        },
        {
            "success": True,
            "delivery_status": MutationDeliveryStatus.UNKNOWN,
        },
        {
            "success": False,
            "blocked": True,
            "delivery_status": MutationDeliveryStatus.PROVEN_NOT_SENT,
        },
    ],
)
def test_action_result_rejects_contradictory_delivery_fields(kwargs):
    with pytest.raises(ValidationError):
        ActionResult(message="bad", **kwargs)


class _HangingSession:
    async def call_tool(self, _name, arguments=None):
        await asyncio.sleep(60)


class _BrokenSession:
    async def call_tool(self, _name, arguments=None):
        raise BrokenPipeError("sidecar pipe closed")


def test_mcp_mutation_timeout_is_bounded_and_marks_session_broken():
    async def scenario():
        client = Civ6McpClient(
            McpServerConfig(command="unused", mutation_timeout_seconds=0.01)
        )
        client.session = _HangingSession()
        client.state = McpClientState.READY
        with pytest.raises(McpMutationTimeoutError):
            await client.call_mutation_tool("unit_action", {})
        assert client.session_broken is True
        assert client.mutation_count == 1
        assert client.mutation_timeout_count == 1
        assert client.mutation_seconds >= 0.01

    asyncio.run(scenario())


def test_mcp_transport_failure_marks_session_broken():
    async def scenario():
        client = Civ6McpClient(McpServerConfig(command="unused"))
        client.session = _BrokenSession()
        client.state = McpClientState.READY
        with pytest.raises(McpMutationTransportError):
            await client.call_mutation_tool("unit_action", {})
        assert client.session_broken is True
        assert client.mutation_count == 1

    asyncio.run(scenario())


def test_runtime_owns_the_complete_tick_deadline(monkeypatch):
    class _Store:
        def prepare_execution_mode(self, _mode):
            return None

    async def scenario():
        runtime = WorkflowRuntime.__new__(WorkflowRuntime)
        runtime.store = _Store()
        runtime.clock = None
        runtime.config = RuntimeConfig(
            max_turn_seconds=0.01,
            mcp_mutation_timeout_seconds=0.001,
        )

        async def hanging_tick():
            await asyncio.sleep(60)

        runtime._tick_once = hanging_tick
        with pytest.raises(TimeoutError):
            await runtime.tick()

    monkeypatch.setattr(runtime_module, "_TickFileLock", lambda: nullcontext())
    asyncio.run(scenario())


def test_disconnected_client_is_proven_not_sent_before_mcp_counting():
    async def scenario():
        client = Civ6McpClient(McpServerConfig(command="unused"))
        port = Civ6GamePort(
            client,
            SimpleNamespace(call_count=0),
            allowed_tools={"end_turn"},
        )
        result = await port.end_turn(
            {
                "tactical": "done",
                "strategic": "done",
                "tooling": "done",
                "planning": "done",
                "hypothesis": "done",
            }
        )
        assert result.delivery_status is MutationDeliveryStatus.PROVEN_NOT_SENT
        assert result.blocked is False
        assert client.mutation_count == 0

    asyncio.run(scenario())


class _CancellableSession:
    async def call_tool(self, _name, arguments=None):
        await asyncio.sleep(60)


def test_cancelled_mcp_mutation_marks_session_broken_for_later_recovery():
    async def scenario():
        client = Civ6McpClient(McpServerConfig(command="unused"))
        client.session = _CancellableSession()
        client.state = McpClientState.READY
        task = asyncio.create_task(client.call_mutation_tool("unit_action", {}))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.session_broken is True
        assert client.mutation_count == 1

    asyncio.run(scenario())


def test_game_port_reports_state_and_mcp_metrics_separately():
    client = SimpleNamespace(
        session=object(),
        call_count=9,
        list_tools_count=1,
        read_query_count=2,
        mutation_count=3,
        mutation_seconds=4.5,
        mutation_timeout_count=1,
        reconnect_count=2,
    )
    port = Civ6GamePort(
        client,
        SimpleNamespace(call_count=7, availability_epoch=0),
        allowed_tools=set(),
    )
    assert port.call_metrics == {
        "state_api_call_count": 7,
        "mcp_list_tools_count": 1,
        "mcp_read_query_count": 2,
        "mcp_mutation_count": 3,
        "mcp_mutation_seconds": 4.5,
        "mcp_timeout_count": 1,
        "mcp_reconnect_count": 2,
    }


def test_unresolved_attempt_is_reconciled_before_session_recovery_or_tool_probe():
    class _Game:
        call_count = 0
        call_metrics = {}

        def __init__(self):
            self.recoveries = 0
            self.tool_probes = 0

        async def read_snapshot(self, *, include_units=False):
            return RuntimeSnapshot(turn=4, game_id="game", overview={"turn": 4})

        async def recover_mutation_session(self):
            self.recoveries += 1

        async def list_tools(self):
            self.tool_probes += 1
            return set()

    unresolved = SimpleNamespace(task_id="task")

    class _Store:
        def load_runtime_state(self, _game_id):
            return runtime_module.RuntimeState.OBSERVING

        def get_meta(self, _key, default=None):
            return default

        def set_meta(self, _key, _value):
            return None

        def save_normalized_observation(self, _observation):
            return None

        def unresolved_action_attempt(self, _game_id):
            return unresolved

        def get_task(self, _game_id, _task_id):
            return None

    class _Batch:
        def __init__(self):
            self.calls = 0

        def reconcile(self, *_args, **_kwargs):
            self.calls += 1
            return object()

    async def scenario():
        runtime = WorkflowRuntime.__new__(WorkflowRuntime)
        runtime.store = _Store()
        runtime.clock = None
        runtime.game = _Game()
        runtime.batch_executor = _Batch()
        runtime._active_observation_id = None
        runtime._finish_execution_transition = lambda *_args: "reconciled"
        ctx = runtime_module._TickContext(
            tick_id="tick",
            started_at=datetime.now(UTC),
            started_monotonic=time.perf_counter(),
            call_count_before=0,
            external_counts_before={},
            metrics=TickMetrics(),
            budget=MutationBudget(),
        )

        result = await runtime._run_tick(ctx)

        assert result == "reconciled"
        assert runtime.batch_executor.calls == 1
        assert runtime.game.recoveries == 0
        assert runtime.game.tool_probes == 0

    asyncio.run(scenario())


def test_turn_rewind_with_unresolved_attempt_waits_without_reconciliation():
    started = datetime.now(UTC)

    class _Game:
        call_count = 0
        call_metrics = {}

        async def read_snapshot(self, *, include_units=False):
            return RuntimeSnapshot(turn=4, game_id="game", overview={"turn": 4})

    unresolved = SimpleNamespace(
        action_attempt_id="attempt-old-timeline",
        task_id="task",
    )

    class _Store:
        def load_runtime_state(self, _game_id):
            return runtime_module.RuntimeState.OBSERVING

        def get_meta(self, key, default=None):
            return {"last_game_id": "game", "last_observed_turn": 5}.get(key, default)

        def set_meta(self, _key, _value):
            raise AssertionError("rewind metadata must not advance")

        def unresolved_action_attempt(self, _game_id):
            return unresolved

        def human_wait_context(self, _game_id):
            return None

    class _Batch:
        def reconcile(self, *_args, **_kwargs):
            raise AssertionError("old-timeline Attempt must not be reconciled")

    async def scenario():
        runtime = WorkflowRuntime.__new__(WorkflowRuntime)
        runtime.store = _Store()
        runtime.clock = None
        runtime.game = _Game()
        runtime.batch_executor = _Batch()
        runtime._active_observation_id = None
        captured = {}

        def finish(_ctx, _snapshot, tick_type, **kwargs):
            captured.update(kwargs)
            captured["tick_type"] = tick_type
            return "waiting"

        runtime._finish = finish
        ctx = runtime_module._TickContext(
            tick_id="tick-rewind",
            started_at=started,
            started_monotonic=time.perf_counter(),
            call_count_before=0,
            external_counts_before={},
            metrics=TickMetrics(),
            budget=MutationBudget(),
        )

        assert await runtime._run_tick(ctx) == "waiting"
        assert captured["human_wait_context_override"] == {
            "version": "human-wait/v1",
            "wait_kind": "turn_rewind_with_unresolved_attempt",
            "resume_policy": "explicit_only",
            "action_attempt_id": unresolved.action_attempt_id,
            "resume_requested": False,
        }

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["exhausted", "minimum_interval"])
def test_unresolved_verification_does_not_repeat_without_new_evidence(mode):
    started = datetime.now(UTC)
    snapshot = RuntimeSnapshot(turn=4, game_id="game", overview={"turn": 4})
    projection_hash = runtime_module.normalize_runtime_snapshot(
        snapshot, observed_at=started
    ).canonical.projection_hash
    status = (
        runtime_module.AttemptStatus.UNCERTAIN
        if mode == "exhausted"
        else runtime_module.AttemptStatus.VERIFYING
    )
    unresolved = SimpleNamespace(
        action_attempt_id="attempt",
        task_id="task",
        status=status,
        verification_count=3 if mode == "exhausted" else 1,
        last_verification_projection_hash=projection_hash,
        verified_at=started,
    )

    class _Game:
        call_count = 0
        call_metrics = {}

        async def read_snapshot(self, *, include_units=False):
            return snapshot

    class _Store:
        def __init__(self):
            self.saved = 0

        def load_runtime_state(self, _game_id):
            if mode == "exhausted":
                return runtime_module.RuntimeState.AWAITING_HUMAN
            return runtime_module.RuntimeState.VERIFYING

        def get_meta(self, _key, default=None):
            return default

        def set_meta(self, _key, _value):
            return None

        def unresolved_action_attempt(self, _game_id):
            return unresolved

        def get_task(self, _game_id, _task_id):
            return None

        def save_normalized_observation(self, _observation):
            self.saved += 1

    class _Batch:
        def __init__(self):
            self.calls = 0

        def reconcile(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("unchanged evidence must not be reconciled")

    class _Clock:
        def now(self):
            return started

        def monotonic(self):
            return time.perf_counter()

    async def scenario():
        runtime = WorkflowRuntime.__new__(WorkflowRuntime)
        runtime.store = _Store()
        runtime.clock = _Clock()
        runtime.game = _Game()
        runtime.batch_executor = _Batch()
        runtime.config = RuntimeConfig(
            verification_attempts=3,
            verification_delay_seconds=60,
        )
        runtime._active_observation_id = None
        runtime._held_result = lambda *_args, **_kwargs: "held"
        ctx = runtime_module._TickContext(
            tick_id=f"tick-{mode}",
            started_at=started,
            started_monotonic=time.perf_counter(),
            call_count_before=0,
            external_counts_before={},
            metrics=TickMetrics(),
            budget=MutationBudget(),
        )

        result = await runtime._run_tick(ctx)

        if mode == "exhausted":
            assert result == "held"
        else:
            assert result.runtime_state == runtime_module.RuntimeState.VERIFYING.value
        assert runtime.batch_executor.calls == 0
        assert runtime.store.saved == 0
        assert unresolved.verification_count == (3 if mode == "exhausted" else 1)

    asyncio.run(scenario())
