"""Reusable Phase 0 test doubles for freezing effective runtime behavior."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Iterable, Iterator

from .models import ActionResult, AgentRequest, PlanBundle, RuntimeSnapshot, StoredTask


class GameCallKind(StrEnum):
    READ = "READ"
    MUTATION = "MUTATION"
    END_TURN_MUTATION = "END_TURN_MUTATION"


@dataclass(frozen=True, slots=True)
class RecordedGameCall:
    sequence: int
    tick_id: str
    kind: GameCallKind
    operation: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GameCallSummary:
    reads: int
    mutations: int
    end_turn_mutations: int

    @property
    def total(self) -> int:
        return self.reads + self.mutations + self.end_turn_mutations

    @property
    def total_mutations(self) -> int:
        return self.mutations + self.end_turn_mutations


class SecondMutationError(RuntimeError):
    pass


class RecordingGamePort:
    """Classify calls made to a delegate without changing its behavior."""

    def __init__(self, delegate: Any, *, fail_on_second_mutation: bool = False):
        self.delegate = delegate
        self.fail_on_second_mutation = fail_on_second_mutation
        self.calls: list[RecordedGameCall] = []
        self._tick_sequence = 0
        self._tick_id = "tick-0"
        self._mutations_in_tick = 0

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def begin_tick(self, tick_id: str | None = None) -> str:
        self._tick_sequence += 1
        self._tick_id = tick_id or f"tick-{self._tick_sequence}"
        self._mutations_in_tick = 0
        return self._tick_id

    def calls_for_tick(
        self, tick_id: str | None = None
    ) -> tuple[RecordedGameCall, ...]:
        selected = self._tick_id if tick_id is None else tick_id
        return tuple(call for call in self.calls if call.tick_id == selected)

    def summary(self, tick_id: str | None = None) -> GameCallSummary:
        calls = self.calls_for_tick(tick_id)
        return GameCallSummary(
            reads=sum(call.kind is GameCallKind.READ for call in calls),
            mutations=sum(call.kind is GameCallKind.MUTATION for call in calls),
            end_turn_mutations=sum(
                call.kind is GameCallKind.END_TURN_MUTATION for call in calls
            ),
        )

    def assert_at_most_one_mutation(self, tick_id: str | None = None) -> None:
        summary = self.summary(tick_id)
        if summary.total_mutations > 1:
            raise AssertionError(
                f"Tick recorded {summary.total_mutations} mutations; expected at most one"
            )

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        self._record(
            GameCallKind.READ,
            "read_snapshot",
            {"include_units": include_units},
        )
        return await self.delegate.read_snapshot(include_units=include_units)

    async def execute_task(self, task: StoredTask) -> ActionResult:
        self._record(
            GameCallKind.MUTATION,
            "execute_task",
            {"task_id": task.task_id, "action_type": task.action_type},
        )
        return await self.delegate.execute_task(task)

    async def end_turn(self, reflections: dict[str, str] | None = None) -> ActionResult:
        self._record(GameCallKind.END_TURN_MUTATION, "end_turn")
        return await self.delegate.end_turn(reflections)

    async def list_tools(self) -> set[str]:
        self._record(GameCallKind.READ, "list_tools")
        return await self.delegate.list_tools()

    def _record(
        self,
        kind: GameCallKind,
        operation: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        if kind in {GameCallKind.MUTATION, GameCallKind.END_TURN_MUTATION}:
            if self.fail_on_second_mutation and self._mutations_in_tick >= 1:
                raise SecondMutationError(
                    f"second mutation in {self._tick_id}: {operation}"
                )
            self._mutations_in_tick += 1
        self.calls.append(
            RecordedGameCall(
                sequence=len(self.calls) + 1,
                tick_id=self._tick_id,
                kind=kind,
                operation=operation,
                details={} if details is None else dict(details),
            )
        )


@dataclass(frozen=True, slots=True)
class ScriptedSnapshot:
    snapshot: RuntimeSnapshot
    include_units: bool = False


class ScriptedSnapshotSource:
    """Deterministic game delegate with ordered snapshot and action results."""

    def __init__(
        self,
        snapshots: Iterable[ScriptedSnapshot],
        *,
        action_results: Iterable[ActionResult] = (),
        end_turn_results: Iterable[ActionResult] = (),
        tools: Iterable[str] = (),
    ):
        self._snapshots = list(snapshots)
        self._action_results = list(action_results)
        self._end_turn_results = list(end_turn_results)
        self._tools = set(tools)
        self.call_count = 0

    @property
    def remaining_snapshots(self) -> int:
        return len(self._snapshots)

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        self.call_count += 1
        if not self._snapshots:
            raise AssertionError("scripted snapshot source is exhausted")
        frame = self._snapshots.pop(0)
        if frame.include_units is not include_units:
            raise AssertionError(
                "snapshot include_units mismatch: "
                f"expected {frame.include_units}, got {include_units}"
            )
        return frame.snapshot.model_copy(deep=True)

    async def execute_task(self, task: StoredTask) -> ActionResult:
        self.call_count += 1
        if self._action_results:
            return self._action_results.pop(0).model_copy(deep=True)
        return ActionResult(success=True, message=f"scripted {task.action_type}")

    async def end_turn(self, reflections: dict[str, str] | None = None) -> ActionResult:
        self.call_count += 1
        if self._end_turn_results:
            return self._end_turn_results.pop(0).model_copy(deep=True)
        return ActionResult(success=True, message="scripted end_turn")

    async def list_tools(self) -> set[str]:
        self.call_count += 1
        return set(self._tools)


@dataclass(frozen=True, slots=True)
class PlannerCallSummary:
    logical_requests: int
    provider_attempts: int


@dataclass(frozen=True, slots=True)
class RecordedPlannerCall:
    sequence: int
    logical_transaction_id: str
    provider_request_id: str
    provider_attempts: int


class RecordingPlanner:
    """Count explicit logical transactions separately from provider attempts."""

    def __init__(self, delegate: Any):
        self.delegate = delegate
        self.logical_request_count = 0
        self.provider_attempt_count = 0
        self._logical_transaction_ids: set[str] = set()
        self._active_logical_transaction_id: str | None = None
        self._implicit_transaction_sequence = 0
        self.calls: list[RecordedPlannerCall] = []
        self.requests: list[AgentRequest] = []
        self.responses: list[PlanBundle] = []

    def set_provider_attempt_hook(self, hook: Any | None) -> bool:
        setter = getattr(self.delegate, "set_provider_attempt_hook", None)
        if callable(setter):
            return bool(setter(hook))
        return False

    @property
    def summary(self) -> PlannerCallSummary:
        return PlannerCallSummary(
            logical_requests=self.logical_request_count,
            provider_attempts=self.provider_attempt_count,
        )

    @property
    def last_diagnostics(self) -> dict[str, Any] | None:
        value = getattr(self.delegate, "last_diagnostics", None)
        return value if isinstance(value, dict) else None

    @contextmanager
    def logical_request_scope(self, transaction_id: str) -> Iterator[str]:
        transaction_id = transaction_id.strip()
        if not transaction_id:
            raise ValueError("logical transaction identity must not be empty")
        if self._active_logical_transaction_id is not None:
            raise RuntimeError("logical request scopes cannot be nested")
        self._register_logical_transaction(transaction_id)
        self._active_logical_transaction_id = transaction_id
        try:
            yield transaction_id
        finally:
            self._active_logical_transaction_id = None

    async def plan(self, request: AgentRequest) -> PlanBundle:
        transaction_id = self._active_logical_transaction_id
        if transaction_id is None:
            self._implicit_transaction_sequence += 1
            transaction_id = f"implicit-{self._implicit_transaction_sequence}"
            self._register_logical_transaction(transaction_id)
        self.requests.append(request.model_copy(deep=True))
        previous_diagnostics = getattr(self.delegate, "last_diagnostics", None)
        try:
            setattr(self.delegate, "last_diagnostics", None)
        except (AttributeError, TypeError):
            pass
        attempts = 0
        try:
            response = await self.delegate.plan(request)
        finally:
            diagnostics = getattr(self.delegate, "last_diagnostics", None)
            if (
                isinstance(diagnostics, dict)
                and diagnostics is not previous_diagnostics
            ):
                attempts = max(0, int(diagnostics.get("attempt_count", 0)))
            self.provider_attempt_count += attempts
            self.calls.append(
                RecordedPlannerCall(
                    sequence=len(self.calls) + 1,
                    logical_transaction_id=transaction_id,
                    provider_request_id=request.request_id,
                    provider_attempts=attempts,
                )
            )
        self.responses.append(response.model_copy(deep=True))
        return response

    def _register_logical_transaction(self, transaction_id: str) -> None:
        if transaction_id in self._logical_transaction_ids:
            return
        self._logical_transaction_ids.add(transaction_id)
        self.logical_request_count += 1


class ScriptedPlanner:
    def __init__(
        self,
        responses: Iterable[PlanBundle],
        *,
        provider_attempts: Iterable[int] = (),
    ):
        self._responses = list(responses)
        self._provider_attempts = list(provider_attempts)
        self.last_diagnostics: dict[str, Any] | None = None
        self.provider_attempt_hook: Any | None = None

    def set_provider_attempt_hook(self, hook: Any | None) -> bool:
        self.provider_attempt_hook = hook
        return True

    async def plan(self, request: AgentRequest) -> PlanBundle:
        self.last_diagnostics = None
        if not self._responses:
            raise AssertionError(
                f"no scripted planner response for {request.request_id}"
            )
        attempts = self._provider_attempts.pop(0) if self._provider_attempts else 1
        for attempt in range(1, attempts + 1):
            if self.provider_attempt_hook is not None:
                await self.provider_attempt_hook(
                    "started",
                    {
                        "provider_request_id": request.request_id,
                        "attempt_number": attempt,
                    },
                )
                if attempt < attempts:
                    await self.provider_attempt_hook(
                        "failed",
                        {"failure_category": "scripted_retry"},
                    )
        self.last_diagnostics = {"attempt_count": attempts}
        return self._responses.pop(0).model_copy(deep=True)


class DeterministicClock:
    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2000, 1, 1, tzinfo=UTC)
        self._monotonic = 0.0

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._monotonic

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("clock cannot move backwards")
        self._now += timedelta(seconds=seconds)
        self._monotonic += seconds

    async def sleep(self, seconds: float) -> None:
        self.advance(seconds)


class InjectedCrash(RuntimeError):
    pass


class CrashInjector:
    def __init__(self, crash_on: dict[str, int] | None = None):
        self.crash_on = {} if crash_on is None else dict(crash_on)
        self.hits: dict[str, int] = {}

    def checkpoint(self, name: str) -> None:
        occurrence = self.hits.get(name, 0) + 1
        self.hits[name] = occurrence
        if self.crash_on.get(name) == occurrence:
            raise InjectedCrash(f"injected crash at {name} occurrence {occurrence}")
