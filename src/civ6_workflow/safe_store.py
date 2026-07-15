from __future__ import annotations

from typing import Any

from .models import ExecutionMode, PlanBundle, TaskStatus
from .store import WorkflowStore as BaseWorkflowStore


class TaskIdentityConflictError(ValueError):
    """Raised when an existing task ID is reused for different semantics."""


class SafeWorkflowStore(BaseWorkflowStore):
    """Workflow store with fail-closed task persistence semantics.

    The original schema remains the storage implementation. This class tightens
    three behavioral contracts at the persistence boundary:

    * ``confirm`` means every unapproved task waits for explicit approval;
    * switching from ``auto`` to ``confirm`` cannot execute stale READY tasks;
    * a task ID is immutable and cannot later refer to a different action.
    """

    def prepare_execution_mode(self, mode: ExecutionMode) -> None:
        if mode is ExecutionMode.AUTO:
            return
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE workflow_tasks
                SET status=?, updated_at=CURRENT_TIMESTAMP
                WHERE status=? AND approved_by IS NULL
                """,
                (
                    TaskStatus.AWAITING_CONFIRMATION.value,
                    TaskStatus.READY.value,
                ),
            )

    def save_plan_bundle(
        self,
        game_id: str,
        turn: int,
        bundle: PlanBundle,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
    ) -> None:
        self._reject_task_id_reuse(game_id, bundle)

        # READONLY and CONFIRM both persist tasks for inspection, but neither may
        # make a new task executable without an explicit approval transition.
        effective_auto_actions = (
            set(auto_action_types) if mode is ExecutionMode.AUTO else set()
        )
        super().save_plan_bundle(
            game_id,
            turn,
            bundle,
            mode=mode,
            auto_action_types=effective_auto_actions,
        )

    def _reject_task_id_reuse(self, game_id: str, bundle: PlanBundle) -> None:
        if not bundle.tasks:
            return
        with self._connect() as conn:
            for proposed in bundle.tasks:
                row = conn.execute(
                    "SELECT * FROM workflow_tasks WHERE game_id=? AND task_id=?",
                    (game_id, proposed.task_id),
                ).fetchone()
                if row is None:
                    continue
                existing = {
                    "action_type": row["action_type"],
                    "entity_type": row["entity_type"],
                    "entity_id": str(row["entity_id"]),
                    "due_turn": int(row["due_turn"]),
                    "expires_turn": row["expires_turn"],
                    "arguments": self._load(row["arguments_json"]),
                    "preconditions": self._load(row["preconditions_json"]),
                    "postconditions": self._load(row["postconditions_json"]),
                    "invalidators": self._load(row["invalidators_json"]),
                    "risk": row["risk"],
                    "requires_confirmation": bool(row["requires_confirmation"]),
                }
                incoming: dict[str, Any] = {
                    "action_type": proposed.action_type,
                    "entity_type": proposed.entity_type,
                    "entity_id": str(proposed.entity_id),
                    "due_turn": proposed.due_turn,
                    "expires_turn": proposed.expires_turn,
                    "arguments": proposed.arguments,
                    "preconditions": proposed.preconditions,
                    "postconditions": proposed.postconditions,
                    "invalidators": proposed.invalidators,
                    "risk": proposed.risk.value,
                    "requires_confirmation": proposed.requires_confirmation,
                }
                if existing != incoming:
                    raise TaskIdentityConflictError(
                        f"task_id {proposed.task_id!r} already exists with different "
                        "action semantics; create a new stable task_id instead"
                    )
