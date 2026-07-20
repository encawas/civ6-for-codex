from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .action_retry import resolve_failed_attempt
from .domain import (
    ApprovalDecision,
    ApprovalRecord,
    ApprovalStatus,
    ActionAttempt,
    AttemptStatus,
    DecisionGap,
    DecisionGapStatus,
    PlanLeaseStatus,
)
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

    def reject_task_confirmation(
        self,
        game_id: str,
        task_id: str,
        *,
        rejected_by: str = "control-panel-user",
    ) -> bool:
        """Cancel one concrete confirmation instead of changing global execution state."""

        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE workflow_tasks SET
                    status=?, approved_by=?, last_error=?, updated_at=CURRENT_TIMESTAMP
                WHERE game_id=? AND task_id=? AND status=?
                """,
                (
                    TaskStatus.CANCELLED.value,
                    rejected_by,
                    "confirmation rejected by user",
                    game_id,
                    task_id,
                    TaskStatus.AWAITING_CONFIRMATION.value,
                ),
            )
            return cursor.rowcount == 1

    def record_lease_approval(
        self,
        game_id: str,
        plan_lease_id: str,
        *,
        approved: bool,
        actor: str = "control-panel-user",
    ) -> tuple[bool, str]:
        """Persist one lease approval decision without activating it in the UI path.

        Activation remains a fresh-observation responsibility of the next workflow
        tick. A rejection invalidates only the concrete lease and its explicitly
        associated tasks.
        """

        turn = int(self.get_meta("last_observed_turn", 0) or 0)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT lease_json FROM plan_leases WHERE game_id=? AND plan_lease_id=?",
                (game_id, plan_lease_id),
            ).fetchone()
            if row is None:
                return False, "plan lease was not found for this game"
            lease = self._plan_lease_from_json(row["lease_json"])
            if lease.status is not PlanLeaseStatus.AWAITING_APPROVAL:
                return False, "plan lease is not awaiting approval"

            decision = (
                ApprovalDecision.APPROVED if approved else ApprovalDecision.REJECTED
            )
            record = ApprovalRecord(
                approval_id=f"approval_{uuid4().hex}",
                proposal_type="decision_gap",
                proposal_id=lease.decision_gap_ids[0],
                proposal_revision=lease.plan_revision,
                decision=decision,
                actor=actor,
                created_at=datetime.now(UTC),
                reason=(
                    "approved from local control panel"
                    if approved
                    else "rejected from local control panel"
                ),
            )
            conn.execute(
                """
                INSERT INTO approval_records(
                    approval_id, game_id, proposal_type, proposal_id,
                    proposal_revision, decision, record_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.approval_id,
                    game_id,
                    record.proposal_type,
                    record.proposal_id,
                    record.proposal_revision,
                    record.decision.value,
                    record.model_dump_json(),
                    record.created_at.isoformat(),
                ),
            )
            if approved:
                return True, "approval recorded; the next tick will revalidate it"

            rejected = lease.model_copy(
                update={
                    "status": PlanLeaseStatus.INVALIDATED,
                    "approval_status": ApprovalStatus.REJECTED,
                    "invalidation_reason": "plan lease rejected by user",
                }
            )
            self._save_plan_lease_in_connection(conn, rejected)
            self._invalidate_plan_projection_in_connection(conn, rejected)
            for task_id in rejected.task_ids:
                conn.execute(
                    """
                    UPDATE workflow_tasks SET
                        status=?, last_error=?, updated_at=CURRENT_TIMESTAMP
                    WHERE game_id=? AND task_id=?
                      AND status IN (?, ?, ?)
                    """,
                    (
                        TaskStatus.CANCELLED.value,
                        "dependent plan lease rejected by user",
                        game_id,
                        task_id,
                        TaskStatus.PENDING.value,
                        TaskStatus.READY.value,
                        TaskStatus.AWAITING_CONFIRMATION.value,
                    ),
                )
            for gap_id in rejected.decision_gap_ids:
                gap_row = conn.execute(
                    "SELECT gap_json FROM decision_gaps WHERE game_id=? AND decision_gap_id=?",
                    (game_id, gap_id),
                ).fetchone()
                if gap_row is None:
                    continue
                gap = DecisionGap.model_validate_json(gap_row["gap_json"])
                invalidated_gap = gap.model_copy(
                    update={
                        "status": DecisionGapStatus.INVALIDATED,
                        "resolution_reason": "plan lease rejected by user",
                        "invalidation_reason": "plan lease rejected by user",
                    }
                )
                self._save_decision_gap_in_connection(conn, invalidated_gap, turn)
            return True, "rejection recorded; dependent tasks were cancelled"

    def retry_failed_attempt_if_safe(
        self,
        game_id: str,
        action_attempt_id: str,
    ) -> tuple[bool, str]:
        """Requeue one failed action only when durable evidence proves it safe."""

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT attempts.attempt_json, tasks.retry_count, tasks.max_retries,
                       tasks.status
                FROM action_attempts AS attempts
                JOIN workflow_tasks AS tasks
                  ON tasks.game_id=attempts.game_id AND tasks.task_id=attempts.task_id
                WHERE attempts.game_id=? AND attempts.action_attempt_id=?
                  AND NOT EXISTS (
                      SELECT 1 FROM action_attempts AS newer
                      WHERE newer.game_id=attempts.game_id
                        AND newer.task_id=attempts.task_id
                        AND newer.attempt_number>attempts.attempt_number
                  )
                """,
                (game_id, action_attempt_id),
            ).fetchone()
            if row is None:
                return False, "action attempt is not the latest attempt for this game"
            attempt = ActionAttempt.model_validate_json(row["attempt_json"])
            if attempt.status is not AttemptStatus.FAILED:
                return False, "action attempt is not a failed retry candidate"
            resolution = resolve_failed_attempt(
                attempt,
                retry_count=int(row["retry_count"]),
                max_retries=int(row["max_retries"]),
            )
            if resolution.task_status is not TaskStatus.READY:
                return False, resolution.reason
            if str(row["status"]) not in {
                TaskStatus.FAILED.value,
                TaskStatus.ESCALATED.value,
            }:
                return False, "task is not waiting for a manual retry"
            conn.execute(
                """
                UPDATE workflow_tasks SET
                    status=?, retry_count=?, last_error=?, updated_at=CURRENT_TIMESTAMP
                WHERE game_id=? AND task_id=?
                """,
                (
                    TaskStatus.READY.value,
                    resolution.retry_count,
                    "manual retry authorized from proven non-commit evidence",
                    game_id,
                    attempt.task_id,
                ),
            )
            return True, "safe retry queued for a later tick"

    def retryable_failed_attempts(self, game_id: str) -> list[dict[str, str]]:
        """Return only concrete failures that the existing retry contract permits."""

        candidates: list[dict[str, str]] = []
        for attempt in self.list_action_attempts(game_id):
            if attempt.status is not AttemptStatus.FAILED:
                continue
            task = self.get_task(game_id, attempt.task_id)
            if task is None or task.status not in {
                TaskStatus.FAILED,
                TaskStatus.ESCALATED,
            }:
                continue
            if self.latest_attempt_for_task(game_id, task.task_id) != attempt:
                continue
            resolution = resolve_failed_attempt(
                attempt,
                retry_count=task.retry_count,
                max_retries=task.max_retries,
            )
            if resolution.task_status is TaskStatus.READY:
                candidates.append(
                    {
                        "action_attempt_id": attempt.action_attempt_id,
                        "task_id": attempt.task_id,
                        "action_type": attempt.action_type or "unknown",
                        "reason": resolution.reason,
                    }
                )
        return candidates

    @staticmethod
    def _plan_lease_from_json(payload: str):
        from .domain import PlanLease

        return PlanLease.model_validate_json(payload)

    def save_plan_bundle(
        self,
        game_id: str,
        turn: int,
        bundle: PlanBundle,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
        observation_id: str | None = None,
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
            observation_id=observation_id,
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
