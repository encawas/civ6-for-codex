from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .models import ExecutionMode, PlanBundle, TaskStatus
from .store import WorkflowStore as BaseWorkflowStore


_STICKY_EVENT_TYPES = {
    "planned_task_blocked",
    "planned_task_failed",
    "action_commit_uncertain",
    "turn_rewind_detected",
}


class TaskIdentityConflictError(ValueError):
    """Raised when an existing task ID is reused for different semantics."""


class SafeWorkflowStore(BaseWorkflowStore):
    """Workflow store with fail-closed task and event persistence semantics."""

    def __init__(self, path):
        super().__init__(path)
        with self._connect() as conn:
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(event_log)").fetchall()
            }
            additions = {
                "resolved_turn": "INTEGER",
                "resolved_by": "TEXT",
                "resolution_task_id": "TEXT",
            }
            for name, declaration in additions.items():
                if name not in columns:
                    conn.execute(
                        f"ALTER TABLE event_log ADD COLUMN {name} {declaration}"
                    )

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

    def reconcile_open_events(
        self,
        game_id: str,
        active_dedupe_keys: Iterable[str],
        turn: int,
    ) -> list[str]:
        """Resolve snapshot/rule events that disappeared from the current tick.

        Historical failures and uncertain commits remain open until an explicit
        operator or task reconciliation resolves them.
        """

        active = {str(key) for key in active_dedupe_keys}
        resolved: list[str] = []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT dedupe_key, event_type
                FROM event_log
                WHERE game_id=? AND status='open'
                """,
                (game_id,),
            ).fetchall()
            for row in rows:
                key = str(row["dedupe_key"])
                event_type = str(row["event_type"])
                if key in active or event_type in _STICKY_EVENT_TYPES:
                    continue
                conn.execute(
                    """
                    UPDATE event_log
                    SET status='resolved', resolved_turn=?,
                        resolved_by='snapshot_reconciliation',
                        resolution_task_id=NULL
                    WHERE game_id=? AND dedupe_key=? AND status='open'
                    """,
                    (turn, game_id, key),
                )
                resolved.append(key)
        return resolved

    def resolve_event(
        self,
        game_id: str,
        dedupe_key: str,
        *,
        turn: int | None = None,
        resolved_by: str = "workflow",
        resolution_task_id: str | None = None,
    ) -> None:
        resolved_turn = (
            int(turn)
            if turn is not None
            else int(self.get_meta("last_observed_turn", 0) or 0)
        )
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE event_log
                SET status='resolved', resolved_turn=?, resolved_by=?,
                    resolution_task_id=?
                WHERE game_id=? AND dedupe_key=?
                """,
                (
                    resolved_turn,
                    resolved_by,
                    resolution_task_id,
                    game_id,
                    dedupe_key,
                ),
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
