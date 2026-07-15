from __future__ import annotations

from .actions import ACTION_REGISTRY
from .models import EventLevel, GameEvent, RiskLevel, TaskStatus
from .workflow_engine import WorkflowAwareEngine


_UNKNOWN_COMMIT_PREFIXES = (
    "commit outcome unknown after MCP transport failure:",
    "action returned success but postcondition failed:",
)


class CommitSafeWorkflowEngine(WorkflowAwareEngine):
    """Stops irreversible actions in an explicit uncertain-commit state."""

    async def tick(self):
        result = await super().tick()
        game_id = self.store.get_meta("last_game_id")
        if isinstance(game_id, str) and self._uncertain_tasks(game_id):
            result.paused = True
            if not result.pause_reason:
                result.pause_reason = (
                    "An irreversible action has an uncertain commit outcome; "
                    "reconcile the live game state before retrying."
                )
        return result

    async def _invoke_planner(self, snapshot, agent_events, result, metrics) -> None:
        if result.paused and self._uncertain_tasks(snapshot.game_id):
            return
        await super()._invoke_planner(
            snapshot, agent_events, result, metrics
        )

    def _retry_or_escalate_task(
        self,
        game_id,
        task,
        turn,
        message,
        result,
        events,
        *,
        blocked,
    ) -> None:
        spec = ACTION_REGISTRY.get(task.action_type)
        unknown_commit = (
            spec is not None
            and not spec.retry_safe_after_unknown
            and str(message).startswith(_UNKNOWN_COMMIT_PREFIXES)
        )
        if not unknown_commit:
            super()._retry_or_escalate_task(
                game_id,
                task,
                turn,
                message,
                result,
                events,
                blocked=blocked,
            )
            return

        self.store.set_task_status(
            game_id,
            task.task_id,
            TaskStatus.UNCERTAIN,
            error=message,
            increment_retry=True,
        )
        if task.task_id not in result.blocked_task_ids:
            result.blocked_task_ids.append(task.task_id)
        result.paused = True
        result.pause_reason = (
            f"Task {task.task_id} may already have committed; automatic retry is disabled."
        )
        events.append(
            GameEvent(
                event_type="action_commit_uncertain",
                turn=turn,
                entity_type=task.entity_type,
                entity_id=task.entity_id,
                level=EventLevel.L3,
                risk=RiskLevel.HIGH,
                blocking=True,
                payload={
                    "task_id": task.task_id,
                    "action_type": task.action_type,
                    "message": message,
                    "required_action": "reconcile_live_state",
                },
                dedupe_key=f"action_commit_uncertain:{task.task_id}",
            )
        )

    def _may_end_turn(self, snapshot, result) -> bool:
        if self._uncertain_tasks(snapshot.game_id):
            return False
        return super()._may_end_turn(snapshot, result)

    def _uncertain_tasks(self, game_id: str):
        return self.store.list_tasks(game_id, statuses=[TaskStatus.UNCERTAIN])
