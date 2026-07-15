from __future__ import annotations

from typing import Any

from .actions import ACTION_REGISTRY
from .models import StoredTask
from .replay import (
    RecordedAction,
    RecordingGamePort as BaseRecordingGamePort,
    ReplayDataError,
    ReplayGamePort as BaseReplayGamePort,
)

_SIGNATURE_KEY = "_workflow_task_signature_v1"


def _task_signature(task: StoredTask) -> dict[str, Any]:
    spec = ACTION_REGISTRY.get(task.action_type)
    if spec is None:
        tool_name = None
        tool_arguments = dict(task.arguments)
    else:
        tool_name = spec.tool_name
        tool_arguments = spec.build_arguments(task)
    return {
        "task_id": task.task_id,
        "action_type": task.action_type,
        "entity_type": task.entity_type,
        "entity_id": str(task.entity_id),
        "tool_name": tool_name,
        "tool_arguments": tool_arguments,
        "preconditions": task.preconditions,
        "postconditions": task.postconditions,
        "invalidators": task.invalidators,
    }


class SafeRecordingGamePort(BaseRecordingGamePort):
    async def execute_task(self, task: StoredTask):
        if self._current is None:
            raise ReplayDataError("cannot record an action before a snapshot")
        result = await self.delegate.execute_task(task)
        recorded_result = result.model_copy(deep=True)
        recorded_result.details = {
            **recorded_result.details,
            _SIGNATURE_KEY: _task_signature(task),
        }
        self._current.actions.append(
            RecordedAction(
                task_id=task.task_id,
                action_type=task.action_type,
                result=recorded_result,
            )
        )
        return result


class SafeReplayGamePort(BaseReplayGamePort):
    async def execute_task(self, task: StoredTask):
        if self._current is None:
            raise ReplayDataError("cannot replay an action before a snapshot")
        if self._next_action_index >= len(self._current.actions):
            raise ReplayDataError(
                f"frame has no recorded result for task {task.task_id}:{task.action_type}"
            )
        expected = self._current.actions[self._next_action_index]
        recorded_signature = expected.result.details.get(_SIGNATURE_KEY)
        if recorded_signature is not None:
            actual_signature = _task_signature(task)
            if recorded_signature != actual_signature:
                raise ReplayDataError(
                    "recorded action semantics do not match workflow execution: "
                    f"expected={recorded_signature!r}, actual={actual_signature!r}"
                )
        result = await super().execute_task(task)
        if _SIGNATURE_KEY not in result.details:
            return result
        details = dict(result.details)
        details.pop(_SIGNATURE_KEY, None)
        return result.model_copy(update={"details": details})
