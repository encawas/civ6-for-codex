from __future__ import annotations

from .models import TaskStatus
from .workflow_engine import WorkflowAwareEngine


class CommitSafeWorkflowEngine(WorkflowAwareEngine):
    """Compatibility shell around the canonical attempt-aware bounded engine."""

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
        if self.store.unresolved_action_attempt(snapshot.game_id) is not None:
            return
        await super()._invoke_planner(snapshot, agent_events, result, metrics)

    def _may_end_turn(self, snapshot, result) -> bool:
        if self.store.unresolved_action_attempt(snapshot.game_id) is not None:
            return False
        return super()._may_end_turn(snapshot, result)

    def _uncertain_tasks(self, game_id: str):
        return self.store.list_tasks(game_id, statuses=[TaskStatus.UNCERTAIN])
