from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Literal, Protocol

from pydantic import Field, model_validator

from .models import (
    ActionResult,
    AgentRequest,
    ExecutionMode,
    PlanBundle,
    RuntimeSnapshot,
    StoredTask,
    StrictModel,
)


class ReplayDataError(RuntimeError):
    pass


class RecordedAction(StrictModel):
    task_id: str
    action_type: str
    result: ActionResult


class ReplayFrame(StrictModel):
    snapshot: RuntimeSnapshot
    include_units: bool = False
    actions: list[RecordedAction] = Field(default_factory=list)
    end_turn_result: ActionResult | None = None


class ReplayPlanSeed(StrictModel):
    turn: int = Field(ge=0)
    bundle: PlanBundle
    mode: ExecutionMode = ExecutionMode.AUTO
    auto_action_types: list[str] = Field(default_factory=list)


class ReplayEngineSettings(StrictModel):
    execution_mode: ExecutionMode
    max_agent_calls_per_turn: int = Field(ge=0, le=2)
    repeated_failure_threshold: int = Field(ge=1, le=10)
    verification_attempts: int = Field(ge=1, le=20)
    auto_action_types: list[str]
    allowed_action_types: list[str]
    allowed_tools: list[str]


class RecordedPlannerCall(StrictModel):
    request: AgentRequest
    response: PlanBundle


class SnapshotRecording(StrictModel):
    schema_version: Literal[1] = 1
    tools: list[str] = Field(default_factory=list)
    frames: list[ReplayFrame] = Field(default_factory=list)
    planner_calls: list[RecordedPlannerCall] = Field(default_factory=list)
    # Compatibility for early hand-authored fixtures. New live recordings use
    # planner_calls so request drift can be detected.
    planner_responses: list[PlanBundle] = Field(default_factory=list)
    seed_plans: list[ReplayPlanSeed] = Field(default_factory=list)
    store_state: dict[str, Any] | None = None
    engine_settings: ReplayEngineSettings | None = None

    @model_validator(mode="after")
    def validate_consistency(self) -> "SnapshotRecording":
        if self.planner_calls and self.planner_responses:
            raise ValueError(
                "recording cannot mix planner_calls with legacy planner_responses"
            )

        frame_game_id: str | None = None
        if self.frames:
            game_ids = {frame.snapshot.game_id for frame in self.frames}
            if len(game_ids) != 1:
                raise ValueError(
                    f"recording frames must belong to one game_id, got {sorted(game_ids)}"
                )
            frame_game_id = self.frames[0].snapshot.game_id
            turns = [frame.snapshot.turn for frame in self.frames]
            if turns != sorted(turns):
                raise ValueError("recording frame turns must be non-decreasing")

        if self.store_state is not None:
            state_game_id = self.store_state.get("game_id")
            if not isinstance(state_game_id, str) or not state_game_id:
                raise ValueError("recording store_state must contain a game_id")
            if frame_game_id is not None and state_game_id != frame_game_id:
                raise ValueError(
                    "recording store_state game_id does not match its snapshot frames"
                )
            tables = self.store_state.get("tables")
            if not isinstance(tables, dict):
                raise ValueError("recording store_state must contain a tables object")
            allowed_meta_keys = {
                "last_game_id",
                "last_observed_turn",
                f"unit_observations_initialized:{state_game_id}",
            }
            for table, rows in tables.items():
                if not isinstance(rows, list):
                    raise ValueError(f"recording table {table!r} must contain a row list")
                for row in rows:
                    if not isinstance(row, dict) or not row:
                        raise ValueError(
                            f"recording table {table!r} contains an invalid row"
                        )
                    if table == "workflow_meta":
                        if row.get("key") not in allowed_meta_keys:
                            raise ValueError(
                                "recording workflow_meta contains a non-replay key"
                            )
                        continue
                    if row.get("game_id") != state_game_id:
                        raise ValueError(
                            f"recording table {table!r} contains a row for another game_id"
                        )
                    if table == "agent_runs":
                        # run_id is a database-internal global primary key. Let the
                        # destination allocate a fresh value to avoid cross-game replace.
                        row.pop("run_id", None)
        return self

    @classmethod
    def load(cls, path: str | Path) -> "SnapshotRecording":
        try:
            return cls.model_validate_json(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:
            raise ReplayDataError(f"invalid snapshot recording {path}: {exc}") from exc

    def save(self, path: str | Path) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            validated = type(self).model_validate(self.model_dump(mode="python"))
        except Exception as exc:
            raise ReplayDataError(f"cannot save inconsistent recording: {exc}") from exc
        target.write_text(validated.model_dump_json(indent=2), encoding="utf-8")


class RecordableGamePort(Protocol):
    @property
    def call_count(self) -> int: ...

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot: ...

    async def execute_task(self, task: StoredTask) -> ActionResult: ...

    async def end_turn(self) -> ActionResult: ...

    async def list_tools(self) -> set[str]: ...


class RecordingGamePort:
    def __init__(
        self,
        delegate: RecordableGamePort,
        recording: SnapshotRecording,
        *,
        on_first_snapshot: Callable[[RuntimeSnapshot], None] | None = None,
    ):
        self.delegate = delegate
        self.recording = recording
        self.on_first_snapshot = on_first_snapshot
        self._current: ReplayFrame | None = None

    @property
    def call_count(self) -> int:
        return self.delegate.call_count

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        snapshot = await self.delegate.read_snapshot(include_units=include_units)
        if not self.recording.frames and self.on_first_snapshot is not None:
            self.on_first_snapshot(snapshot)
        self._current = ReplayFrame(
            snapshot=snapshot.model_copy(deep=True),
            include_units=include_units,
        )
        self.recording.frames.append(self._current)
        return snapshot

    async def execute_task(self, task: StoredTask) -> ActionResult:
        if self._current is None:
            raise ReplayDataError("cannot record an action before a snapshot")
        result = await self.delegate.execute_task(task)
        self._current.actions.append(
            RecordedAction(
                task_id=task.task_id,
                action_type=task.action_type,
                result=result.model_copy(deep=True),
            )
        )
        return result

    async def end_turn(self) -> ActionResult:
        if self._current is None:
            raise ReplayDataError("cannot record end_turn before a snapshot")
        result = await self.delegate.end_turn()
        self._current.end_turn_result = result.model_copy(deep=True)
        return result

    async def list_tools(self) -> set[str]:
        tools = await self.delegate.list_tools()
        self.recording.tools = sorted(tools)
        return tools


class ReplayGamePort:
    def __init__(self, recording: SnapshotRecording):
        self.recording = recording
        self.call_count = 0
        self._next_frame = 0
        self._current: ReplayFrame | None = None
        self._next_action_index = 0
        self._end_turn_used = False

    @property
    def remaining_frames(self) -> int:
        self.assert_current_frame_consumed()
        return len(self.recording.frames) - self._next_frame

    def assert_current_frame_consumed(self) -> None:
        if self._current is None:
            return
        if self._next_action_index < len(self._current.actions):
            missing = [
                f"{action.task_id}:{action.action_type}"
                for action in self._current.actions[self._next_action_index :]
            ]
            raise ReplayDataError(
                "recorded frame contains actions the workflow did not execute: "
                + ", ".join(missing)
            )
        if self._current.end_turn_result is not None and not self._end_turn_used:
            raise ReplayDataError(
                "recorded frame contains an end_turn result the workflow did not consume"
            )

    def assert_finished(self) -> None:
        self.assert_current_frame_consumed()
        remaining = len(self.recording.frames) - self._next_frame
        if remaining:
            raise ReplayDataError(
                f"replay ended with {remaining} unconsumed snapshot frame(s)"
            )

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        self.assert_current_frame_consumed()
        if self._next_frame >= len(self.recording.frames):
            raise ReplayDataError("snapshot recording is exhausted")
        frame = self.recording.frames[self._next_frame]
        frame_index = self._next_frame
        self._next_frame += 1
        if frame.include_units != include_units:
            raise ReplayDataError(
                f"frame {frame_index} was recorded with include_units="
                f"{frame.include_units} but replay requested include_units={include_units}"
            )
        self._current = frame
        self._next_action_index = 0
        self._end_turn_used = False
        self.call_count += 1
        snapshot = frame.snapshot.model_copy(deep=True)
        if include_units and snapshot.units is None:
            raise ReplayDataError(
                f"frame {frame_index} has no units but the workflow requested them"
            )
        return snapshot

    async def execute_task(self, task: StoredTask) -> ActionResult:
        if self._current is None:
            raise ReplayDataError("cannot replay an action before a snapshot")
        if self._end_turn_used:
            raise ReplayDataError("cannot execute a task after end_turn in the same frame")
        if self._next_action_index >= len(self._current.actions):
            raise ReplayDataError(
                f"frame has no recorded result for task {task.task_id}:{task.action_type}"
            )
        expected = self._current.actions[self._next_action_index]
        actual_key = f"{task.task_id}:{task.action_type}"
        expected_key = f"{expected.task_id}:{expected.action_type}"
        if expected.task_id != task.task_id or expected.action_type != task.action_type:
            raise ReplayDataError(
                "recorded action order does not match workflow execution: "
                f"expected {expected_key}, got {actual_key}"
            )
        self._next_action_index += 1
        self.call_count += 1
        return expected.result.model_copy(deep=True)

    async def end_turn(self) -> ActionResult:
        if self._current is None or self._current.end_turn_result is None:
            raise ReplayDataError("frame has no recorded end_turn result")
        if self._next_action_index < len(self._current.actions):
            raise ReplayDataError("cannot consume end_turn before recorded actions")
        if self._end_turn_used:
            raise ReplayDataError("frame end_turn result was already consumed")
        self._end_turn_used = True
        self.call_count += 1
        return self._current.end_turn_result.model_copy(deep=True)

    async def list_tools(self) -> set[str]:
        return set(self.recording.tools)


class RecordingPlanner:
    def __init__(self, delegate, recording: SnapshotRecording):
        self.delegate = delegate
        self.recording = recording

    async def plan(self, request: AgentRequest) -> PlanBundle:
        response = await self.delegate.plan(request)
        self.recording.planner_calls.append(
            RecordedPlannerCall(
                request=request.model_copy(deep=True),
                response=response.model_copy(deep=True),
            )
        )
        return response


class ReplayPlanner:
    def __init__(self, recording: SnapshotRecording):
        self.recording = recording
        self._next_response = 0

    @property
    def remaining_calls(self) -> int:
        total = (
            len(self.recording.planner_calls)
            if self.recording.planner_calls
            else len(self.recording.planner_responses)
        )
        return total - self._next_response

    def assert_consumed(self) -> None:
        if self.remaining_calls:
            raise ReplayDataError(
                f"replay ended with {self.remaining_calls} unconsumed planner call(s)"
            )

    async def plan(self, request: AgentRequest) -> PlanBundle:
        if self.recording.planner_calls:
            if self._next_response >= len(self.recording.planner_calls):
                raise ReplayDataError(
                    f"no recorded planner call for turn {request.turn}"
                )
            recorded = self.recording.planner_calls[self._next_response]
            expected = self._request_signature(recorded.request)
            actual = self._request_signature(request)
            if expected != actual:
                raise ReplayDataError(
                    "planner request does not match the recorded call: "
                    f"expected turn={recorded.request.turn}, "
                    f"events={self._event_keys(recorded.request)}; "
                    f"actual turn={request.turn}, events={self._event_keys(request)}"
                )
            self._next_response += 1
            return recorded.response.model_copy(deep=True)

        if self._next_response >= len(self.recording.planner_responses):
            raise ReplayDataError(
                f"no recorded planner response for turn {request.turn}"
            )
        response = self.recording.planner_responses[self._next_response]
        self._next_response += 1
        return response.model_copy(deep=True)

    @staticmethod
    def _request_signature(request: AgentRequest) -> dict[str, Any]:
        payload = request.model_dump(mode="json")
        payload.pop("request_id", None)
        for event in payload.get("trigger_events", []):
            if isinstance(event, dict):
                event.pop("event_id", None)
        return payload

    @staticmethod
    def _event_keys(request: AgentRequest) -> list[str]:
        return [event.dedupe_key for event in request.trigger_events]
