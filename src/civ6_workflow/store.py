from __future__ import annotations

import json
import hashlib
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence
from uuid import uuid4

from .action_retry import FailedAttemptResolution, resolve_failed_attempt
from .domain import (
    ActionAttempt,
    ApprovalRecord,
    AttemptReconciledTick,
    AttemptRecoveredTick,
    AttemptStatus,
    DecisionGap,
    DecisionGapStatus,
    DecisionGroup,
    InformationRound,
    PlanLease,
    PlanLeaseStatus,
    PlannerRequest,
    PlannerRequestStatus,
    ProviderAttempt,
    RuntimeState,
    TurnTransitionConfirmedTick,
    WorkflowTick,
    validate_workflow_tick,
    ProviderAttemptStatus,
)
from .models import (
    AgentRequest,
    EventLevel,
    ExecutionMode,
    GameEvent,
    PlanBundle,
    StoredTask,
    TaskStatus,
    TickMetrics,
)


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS workflow_meta (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS strategy_state (
    game_id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    plan_id TEXT,
    updated_turn INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS city_plans (
    game_id TEXT NOT NULL,
    city_id TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    updated_turn INTEGER NOT NULL,
    PRIMARY KEY (game_id, city_id)
);

CREATE TABLE IF NOT EXISTS unit_plans (
    game_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    updated_turn INTEGER NOT NULL,
    PRIMARY KEY (game_id, unit_id)
);

CREATE TABLE IF NOT EXISTS builder_plans (
    game_id TEXT NOT NULL,
    builder_key TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    updated_turn INTEGER NOT NULL,
    PRIMARY KEY (game_id, builder_key)
);

CREATE TABLE IF NOT EXISTS workflow_tasks (
    game_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    action_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    due_turn INTEGER NOT NULL,
    expires_turn INTEGER,
    arguments_json TEXT NOT NULL,
    preconditions_json TEXT NOT NULL,
    postconditions_json TEXT NOT NULL DEFAULT '[]',
    invalidators_json TEXT NOT NULL,
    risk TEXT NOT NULL,
    requires_confirmation INTEGER NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 2,
    last_error TEXT,
    approved_by TEXT,
    created_turn INTEGER NOT NULL,
    created_from_observation_id TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_due
ON workflow_tasks (game_id, status, due_turn);

CREATE TABLE IF NOT EXISTS event_log (
    game_id TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    event_json TEXT NOT NULL,
    event_type TEXT NOT NULL,
    level INTEGER NOT NULL,
    first_seen_turn INTEGER NOT NULL,
    last_seen_turn INTEGER NOT NULL,
    seen_count INTEGER NOT NULL DEFAULT 1,
    cooldown_until_turn INTEGER NOT NULL DEFAULT 0,
    last_agent_turn INTEGER,
    status TEXT NOT NULL DEFAULT 'open',
    PRIMARY KEY (game_id, dedupe_key)
);

CREATE TABLE IF NOT EXISTS decision_gaps (
    decision_gap_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    stable_identity TEXT NOT NULL,
    gap_type TEXT NOT NULL,
    scope TEXT NOT NULL,
    status TEXT NOT NULL,
    route TEXT NOT NULL,
    relevant_input_hash TEXT NOT NULL,
    input_projection_version TEXT NOT NULL,
    logical_request_id TEXT,
    first_seen_turn INTEGER NOT NULL,
    last_seen_turn INTEGER NOT NULL,
    gap_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (game_id, stable_identity)
);

CREATE INDEX IF NOT EXISTS idx_decision_gaps_active
ON decision_gaps (game_id, status, route);

CREATE TABLE IF NOT EXISTS decision_groups (
    decision_group_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    decision_gap_ids_json TEXT NOT NULL,
    input_projection_hash TEXT NOT NULL,
    input_projection_version TEXT NOT NULL,
    group_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS logical_planner_requests (
    planner_request_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    decision_group_id TEXT,
    turn INTEGER NOT NULL,
    status TEXT NOT NULL,
    input_projection_hash TEXT NOT NULL,
    input_projection_version TEXT NOT NULL,
    decision_gap_ids_json TEXT NOT NULL,
    request_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (game_id, decision_group_id, input_projection_hash)
);

CREATE INDEX IF NOT EXISTS idx_logical_requests_game_status
ON logical_planner_requests (game_id, status, turn);

CREATE TABLE IF NOT EXISTS provider_attempts (
    provider_attempt_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    planner_request_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    provider_request_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (planner_request_id, attempt_number),
    FOREIGN KEY (planner_request_id)
        REFERENCES logical_planner_requests(planner_request_id)
);

CREATE TABLE IF NOT EXISTS information_rounds (
    information_round_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    planner_request_id TEXT NOT NULL,
    round_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    round_json TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (planner_request_id, round_number),
    FOREIGN KEY (planner_request_id)
        REFERENCES logical_planner_requests(planner_request_id)
);

CREATE TABLE IF NOT EXISTS plan_leases (
    plan_lease_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    status TEXT NOT NULL,
    plan_revision INTEGER NOT NULL,
    relevant_input_hash TEXT NOT NULL,
    source_planner_request_id TEXT,
    lease_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_planner_request_id)
        REFERENCES logical_planner_requests(planner_request_id)
);

CREATE INDEX IF NOT EXISTS idx_plan_leases_active
ON plan_leases (game_id, status, scope);
CREATE TABLE IF NOT EXISTS approval_records (
    approval_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    proposal_type TEXT NOT NULL,
    proposal_id TEXT NOT NULL,
    proposal_revision INTEGER NOT NULL,
    decision TEXT NOT NULL,
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_approval_records_proposal
ON approval_records (
    game_id, proposal_type, proposal_id, proposal_revision, created_at
);

CREATE TABLE IF NOT EXISTS planner_suppressions (
    suppression_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    turn INTEGER NOT NULL,
    decision_gap_id TEXT,
    reason TEXT NOT NULL,
    relevant_input_hash TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS agent_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    turn INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT,
    success INTEGER NOT NULL,
    error TEXT,
    duration_seconds REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_turn
ON agent_runs (game_id, turn);

CREATE TABLE IF NOT EXISTS turn_metrics (
    tick_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    turn INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_turn_metrics_game_turn
ON turn_metrics (game_id, turn);

CREATE TABLE IF NOT EXISTS unit_observations (
    game_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    unit_type TEXT NOT NULL,
    first_seen_turn INTEGER NOT NULL,
    last_seen_turn INTEGER NOT NULL,
    eligible_for_binding INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (game_id, unit_id)
);

CREATE TABLE IF NOT EXISTS action_attempts (
    action_attempt_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    action_type TEXT,
    attempt_number INTEGER NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL,
    prepared_from_observation_id TEXT NOT NULL,
    prepared_at TEXT NOT NULL,
    sent_at TEXT,
    response_received_at TEXT,
    status TEXT NOT NULL,
    retry_classification TEXT NOT NULL,
    normalized_arguments_json TEXT NOT NULL,
    transport_result_json TEXT,
    tool_result_json TEXT,
    verification_status TEXT,
    last_verification_observation_id TEXT,
    parent_attempt_id TEXT,
    pre_send_turn INTEGER,
    postconditions_json TEXT NOT NULL DEFAULT '[]',
    postcondition_version INTEGER NOT NULL DEFAULT 1,
    verification_count INTEGER NOT NULL DEFAULT 0,
    attempt_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (game_id, task_id, attempt_number),
    FOREIGN KEY (parent_attempt_id) REFERENCES action_attempts(action_attempt_id)
);

CREATE INDEX IF NOT EXISTS idx_action_attempts_unresolved
ON action_attempts (game_id, status, attempt_number);

CREATE TABLE IF NOT EXISTS action_attempt_transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    action_attempt_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (action_attempt_id) REFERENCES action_attempts(action_attempt_id)
);

CREATE TABLE IF NOT EXISTS runtime_state (
    game_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    active_attempt_id TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_ticks (
    tick_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    turn INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    starting_runtime_state TEXT NOT NULL,
    ending_runtime_state TEXT NOT NULL,
    observation_ids_json TEXT NOT NULL,
    mutation_budget_used INTEGER NOT NULL CHECK (mutation_budget_used IN (0, 1)),
    selected_task_id TEXT,
    action_attempt_id TEXT,
    planner_request_id TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    tick_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_ticks_game_turn
ON workflow_ticks (game_id, turn);
"""

REPLAY_STATE_TABLES = (
    "strategy_state",
    "city_plans",
    "unit_plans",
    "builder_plans",
    "workflow_tasks",
    "event_log",
    "decision_gaps",
    "decision_groups",
    "approval_records",
    "logical_planner_requests",
    "provider_attempts",
    "information_rounds",
    "plan_leases",
    "planner_suppressions",
    "agent_runs",
    "turn_metrics",
    "unit_observations",
    "action_attempts",
    "action_attempt_transitions",
    "runtime_state",
    "workflow_ticks",
)


class WorkflowStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            self._migrate(conn)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(workflow_tasks)").fetchall()
        }
        additions = {
            "postconditions_json": "TEXT NOT NULL DEFAULT '[]'",
            "retry_count": "INTEGER NOT NULL DEFAULT 0",
            "max_retries": "INTEGER NOT NULL DEFAULT 2",
            "last_error": "TEXT",
            "approved_by": "TEXT",
            "created_from_observation_id": "TEXT",
            "updated_at": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE workflow_tasks ADD COLUMN {name} {declaration}"
                )

        conn.execute(
            """
            UPDATE workflow_tasks
            SET created_from_observation_id =
                'legacy:' || game_id || ':' || created_turn || ':' || task_id
            WHERE created_from_observation_id IS NULL
            """
        )
        WorkflowStore._migrate_turn_metrics(conn)

        unresolved = (
            AttemptStatus.PREPARED.value,
            AttemptStatus.VERIFYING.value,
            AttemptStatus.UNCERTAIN.value,
        )
        conn.execute(
            """
            UPDATE workflow_tasks
            SET status=CASE (
                SELECT status FROM action_attempts
                WHERE action_attempts.game_id=workflow_tasks.game_id
                  AND action_attempts.task_id=workflow_tasks.task_id
                  AND action_attempts.status IN (?, ?, ?)
                ORDER BY attempt_number DESC
                LIMIT 1
            )
                WHEN ? THEN ?
                WHEN ? THEN ?
                WHEN ? THEN ?
                ELSE status
            END,
            updated_at=CURRENT_TIMESTAMP
            WHERE EXISTS (
                SELECT 1 FROM action_attempts
                WHERE action_attempts.game_id=workflow_tasks.game_id
                  AND action_attempts.task_id=workflow_tasks.task_id
                  AND action_attempts.status IN (?, ?, ?)
            )
            """,
            (
                *unresolved,
                AttemptStatus.PREPARED.value,
                TaskStatus.RUNNING.value,
                AttemptStatus.VERIFYING.value,
                TaskStatus.VERIFYING.value,
                AttemptStatus.UNCERTAIN.value,
                TaskStatus.UNCERTAIN.value,
                *unresolved,
            ),
        )

        # A legacy RUNNING row with no attempt is the only transient state that
        # can be proven not to have crossed the new delivery boundary.
        conn.execute(
            """
            UPDATE workflow_tasks SET status=?, updated_at=CURRENT_TIMESTAMP
            WHERE status=?
              AND NOT EXISTS (
                SELECT 1 FROM action_attempts
                WHERE action_attempts.game_id=workflow_tasks.game_id
                  AND action_attempts.task_id=workflow_tasks.task_id
              )
            """,
            (
                TaskStatus.READY.value,
                TaskStatus.RUNNING.value,
            ),
        )
        conn.execute(
            """
            UPDATE workflow_tasks SET status=CASE
                WHEN retry_count >= max_retries THEN ?
                ELSE ?
            END,
            updated_at=CURRENT_TIMESTAMP
            WHERE status IN (?, ?)
              AND NOT EXISTS (
                SELECT 1 FROM action_attempts
                WHERE action_attempts.game_id=workflow_tasks.game_id
                  AND action_attempts.task_id=workflow_tasks.task_id
              )
            """,
            (
                TaskStatus.ESCALATED.value,
                TaskStatus.READY.value,
                TaskStatus.BLOCKED.value,
                TaskStatus.FAILED.value,
            ),
        )
        # Repair databases left by the pre-v6 multi-transaction finalization.
        # The latest terminal attempt is authoritative for task/runtime recovery.
        conn.execute(
            """
            UPDATE workflow_tasks
            SET status=?, last_error=NULL, updated_at=CURRENT_TIMESTAMP
            WHERE (
                SELECT status FROM action_attempts
                WHERE action_attempts.game_id=workflow_tasks.game_id
                  AND action_attempts.task_id=workflow_tasks.task_id
                ORDER BY attempt_number DESC LIMIT 1
            )=?
            """,
            (TaskStatus.DONE.value, AttemptStatus.SUCCEEDED.value),
        )
        conn.execute(
            """
            UPDATE workflow_tasks
            SET status=?, updated_at=CURRENT_TIMESTAMP
            WHERE (
                SELECT status FROM action_attempts
                WHERE action_attempts.game_id=workflow_tasks.game_id
                  AND action_attempts.task_id=workflow_tasks.task_id
                ORDER BY attempt_number DESC LIMIT 1
            )=?
            """,
            (
                TaskStatus.READY.value,
                AttemptStatus.REJECTED_BEFORE_SEND.value,
            ),
        )

        WorkflowStore._repair_failed_attempt_tasks(conn)

        conn.execute(
            """
            UPDATE runtime_state
            SET active_attempt_id=NULL,
                state=CASE
                    WHEN (
                        SELECT action_type FROM action_attempts
                        WHERE action_attempt_id=runtime_state.active_attempt_id
                    )='end_turn'
                    AND (
                        SELECT status FROM action_attempts
                        WHERE action_attempt_id=runtime_state.active_attempt_id
                    )=?
                    THEN ?
                    ELSE ?
                END,
                revision=revision+1,
                updated_at=CURRENT_TIMESTAMP
            WHERE active_attempt_id IN (
                SELECT action_attempt_id FROM action_attempts
                WHERE status IN (?, ?, ?)
            )
            """,
            (
                AttemptStatus.SUCCEEDED.value,
                RuntimeState.OBSERVING.value,
                RuntimeState.ROUTING.value,
                AttemptStatus.SUCCEEDED.value,
                AttemptStatus.FAILED.value,
                AttemptStatus.REJECTED_BEFORE_SEND.value,
            ),
        )
        conn.execute(
            "UPDATE workflow_tasks SET updated_at=CURRENT_TIMESTAMP "
            "WHERE updated_at IS NULL"
        )
        WorkflowStore._repair_terminal_attempt_audits(conn)
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version < 7:
            WorkflowStore._migrate_phase4_v7(conn)
        conn.execute("PRAGMA user_version=7")

    @staticmethod
    def _migrate_phase4_v7(conn: sqlite3.Connection) -> None:
        """Scope Phase 4 identities to a game and invalidate implicit v6 leases."""

        gap_ids: dict[str, str] = {}
        gap_payloads: dict[str, dict[str, Any]] = {}
        rows = conn.execute(
            "SELECT decision_gap_id, game_id, stable_identity, gap_json "
            "FROM decision_gaps"
        ).fetchall()
        for row in rows:
            old_id = str(row["decision_gap_id"])
            game_id = str(row["game_id"])
            identity = str(row["stable_identity"])
            scoped = f"{game_id}\0{identity}".encode("utf-8")
            new_id = f"gap_{hashlib.sha256(scoped).hexdigest()[:24]}"
            payload = json.loads(row["gap_json"])
            payload["decision_gap_id"] = new_id
            payload["game_session_id"] = game_id
            conn.execute(
                "UPDATE decision_gaps SET decision_gap_id=?, gap_json=? "
                "WHERE decision_gap_id=?",
                (new_id, WorkflowStore._dump(payload), old_id),
            )
            gap_ids[old_id] = new_id
            gap_payloads[new_id] = payload

        group_ids: dict[str, tuple[str, str]] = {}
        rows = conn.execute(
            "SELECT decision_group_id, game_id, group_json FROM decision_groups"
        ).fetchall()
        for row in rows:
            old_id = str(row["decision_group_id"])
            game_id = str(row["game_id"])
            payload = json.loads(row["group_json"])
            mapped = sorted(
                gap_ids.get(str(gap_id), str(gap_id))
                for gap_id in payload.get("decision_gap_ids", [])
            )
            identity = f"{game_id}\0{'|'.join(mapped)}".encode("utf-8")
            new_id = f"group_{hashlib.sha256(identity).hexdigest()[:24]}"
            combined = {
                "projection_version": payload.get(
                    "input_projection_version", "decision-input/v1"
                ),
                "gaps": [
                    {
                        "decision_gap_id": gap_id,
                        "stable_identity": gap_payloads.get(gap_id, {}).get(
                            "stable_identity", "legacy"
                        ),
                        "input_hash": gap_payloads.get(gap_id, {}).get(
                            "relevant_input_hash", "legacy"
                        ),
                    }
                    for gap_id in mapped
                ],
            }
            group_hash = hashlib.sha256(
                json.dumps(
                    combined,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode("utf-8")
            ).hexdigest()
            payload.update(
                {
                    "decision_group_id": new_id,
                    "game_session_id": game_id,
                    "decision_gap_ids": mapped,
                    "input_projection_hash": group_hash,
                }
            )
            conn.execute(
                """
                UPDATE decision_groups
                SET decision_group_id=?, decision_gap_ids_json=?,
                    input_projection_hash=?, group_json=?
                WHERE decision_group_id=?
                """,
                (
                    new_id,
                    WorkflowStore._dump(mapped),
                    group_hash,
                    WorkflowStore._dump(payload),
                    old_id,
                ),
            )
            group_ids[old_id] = (new_id, group_hash)

        rows = conn.execute(
            "SELECT planner_request_id, decision_group_id, request_json "
            "FROM logical_planner_requests"
        ).fetchall()
        for row in rows:
            payload = json.loads(row["request_json"])
            mapped = [
                gap_ids.get(str(gap_id), str(gap_id))
                for gap_id in payload.get("decision_gap_ids", [])
            ]
            old_group = row["decision_group_id"]
            group = (
                (None, payload.get("input_projection_hash", "legacy"))
                if old_group is None
                else group_ids.get(
                    str(old_group),
                    (str(old_group), payload.get("input_projection_hash", "legacy")),
                )
            )
            payload["decision_gap_ids"] = mapped
            payload["decision_group_id"] = group[0]
            payload["input_projection_hash"] = group[1]
            projection = payload.get("input_projection")
            if isinstance(projection, dict):
                projection["decision_group_id"] = group[0]
            conn.execute(
                """
                UPDATE logical_planner_requests
                SET decision_group_id=?, decision_gap_ids_json=?,
                    input_projection_hash=?, request_json=?
                WHERE planner_request_id=?
                """,
                (
                    group[0],
                    WorkflowStore._dump(mapped),
                    group[1],
                    WorkflowStore._dump(payload),
                    row["planner_request_id"],
                ),
            )

        rows = conn.execute(
            "SELECT plan_lease_id, status, lease_json FROM plan_leases"
        ).fetchall()
        for row in rows:
            payload = json.loads(row["lease_json"])
            payload["decision_gap_ids"] = [
                gap_ids.get(str(gap_id), str(gap_id))
                for gap_id in payload.get("decision_gap_ids", [])
            ]
            status = str(row["status"])
            if status == "ACTIVE" and not (
                payload.get("preconditions")
                and payload.get("continuation_conditions")
                and payload.get("completion_condition")
                and payload.get("invalidation_conditions")
                and payload.get("review_conditions")
            ):
                status = "AWAITING_INFORMATION"
                payload["status"] = status
                payload["last_validation_result"] = "UNKNOWN"
                payload["invalidation_reason"] = (
                    "v6 lease lacked an explicit durability contract"
                )
            conn.execute(
                "UPDATE plan_leases SET status=?, lease_json=? WHERE plan_lease_id=?",
                (status, WorkflowStore._dump(payload), row["plan_lease_id"]),
            )

        for old_id, new_id in gap_ids.items():
            conn.execute(
                "UPDATE planner_suppressions SET decision_gap_id=? "
                "WHERE decision_gap_id=?",
                (new_id, old_id),
            )

        rows = conn.execute("SELECT tick_id, tick_json FROM workflow_ticks").fetchall()
        for row in rows:
            payload = json.loads(row["tick_json"])
            changed = False
            if payload.get("decision_gap_id") in gap_ids:
                payload["decision_gap_id"] = gap_ids[payload["decision_gap_id"]]
                changed = True
            if isinstance(payload.get("decision_gap_ids"), list):
                payload["decision_gap_ids"] = [
                    gap_ids.get(str(gap_id), str(gap_id))
                    for gap_id in payload["decision_gap_ids"]
                ]
                changed = True
            if changed:
                conn.execute(
                    "UPDATE workflow_ticks SET tick_json=? WHERE tick_id=?",
                    (WorkflowStore._dump(payload), row["tick_id"]),
                )

    @staticmethod
    def _repair_failed_attempt_tasks(conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            """
            SELECT attempts.attempt_json, tasks.game_id, tasks.task_id,
                   tasks.retry_count, tasks.max_retries
            FROM workflow_tasks AS tasks
            JOIN action_attempts AS attempts
              ON attempts.game_id=tasks.game_id
             AND attempts.task_id=tasks.task_id
            WHERE tasks.status IN (?, ?, ?)
              AND attempts.status=?
              AND NOT EXISTS (
                  SELECT 1 FROM action_attempts AS newer
                  WHERE newer.game_id=attempts.game_id
                    AND newer.task_id=attempts.task_id
                    AND newer.attempt_number>attempts.attempt_number
              )
            ORDER BY tasks.game_id, tasks.task_id
            """,
            (
                TaskStatus.RUNNING.value,
                TaskStatus.VERIFYING.value,
                TaskStatus.UNCERTAIN.value,
                AttemptStatus.FAILED.value,
            ),
        ).fetchall()
        for row in rows:
            attempt = ActionAttempt.model_validate_json(row["attempt_json"])
            resolution = resolve_failed_attempt(
                attempt,
                retry_count=int(row["retry_count"]),
                max_retries=int(row["max_retries"]),
            )
            conn.execute(
                """
                UPDATE workflow_tasks SET
                    status=?, last_error=?, retry_count=?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE game_id=? AND task_id=?
                """,
                (
                    resolution.task_status.value,
                    resolution.reason,
                    resolution.retry_count,
                    row["game_id"],
                    row["task_id"],
                ),
            )

    @staticmethod
    def _repair_terminal_attempt_audits(conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            """
            SELECT attempts.attempt_json,
                   COALESCE(tasks.due_turn, attempts.pre_send_turn, 0) AS turn
            FROM action_attempts AS attempts
            LEFT JOIN workflow_tasks AS tasks
              ON tasks.game_id=attempts.game_id
             AND tasks.task_id=attempts.task_id
            WHERE attempts.status IN (?, ?, ?)
            ORDER BY attempts.game_id, attempts.attempt_number,
                     attempts.action_attempt_id
            """,
            (
                AttemptStatus.SUCCEEDED.value,
                AttemptStatus.FAILED.value,
                AttemptStatus.REJECTED_BEFORE_SEND.value,
            ),
        ).fetchall()
        for row in rows:
            attempt = ActionAttempt.model_validate_json(row["attempt_json"])
            outcomes = {
                existing["outcome"]
                for existing in conn.execute(
                    "SELECT outcome FROM workflow_ticks WHERE action_attempt_id=?",
                    (attempt.action_attempt_id,),
                ).fetchall()
            }
            if attempt.status is AttemptStatus.REJECTED_BEFORE_SEND:
                expected_outcomes = {"ATTEMPT_RECOVERED"}
            elif (
                attempt.status is AttemptStatus.SUCCEEDED
                and attempt.action_type == "end_turn"
            ):
                expected_outcomes = {"TURN_TRANSITION_CONFIRMED"}
            elif attempt.status is AttemptStatus.FAILED:
                expected_outcomes = {"ATTEMPT_RECONCILED", "MUTATION_REJECTED"}
            else:
                expected_outcomes = {"ATTEMPT_RECONCILED"}
            if outcomes & expected_outcomes:
                continue

            recovered_at = (
                attempt.response_received_at
                or attempt.sent_at
                or attempt.prepared_at
                or datetime.now(UTC)
            )
            common = {
                "tick_id": (
                    f"recovery_{attempt.action_attempt_id}_{attempt.status.value.lower()}"
                ),
                "game_session_id": attempt.game_session_id,
                "turn_number": max(0, int(row["turn"])),
                "observation_ids": (
                    attempt.last_verification_observation_id
                    or attempt.prepared_from_observation_id,
                ),
                "started_at": recovered_at,
                "completed_at": recovered_at,
                "metrics": {},
            }
            if attempt.status is AttemptStatus.REJECTED_BEFORE_SEND:
                tick = AttemptRecoveredTick(
                    **common,
                    starting_runtime_state=RuntimeState.RECONCILING,
                    action_attempt_id=attempt.action_attempt_id,
                    task_id=attempt.task_id,
                )
            elif (
                attempt.status is AttemptStatus.SUCCEEDED
                and attempt.action_type == "end_turn"
            ):
                tick = TurnTransitionConfirmedTick(
                    **common,
                    starting_runtime_state=RuntimeState.TURN_TRANSITIONING,
                    action_attempt_id=attempt.action_attempt_id,
                )
            else:
                tick = AttemptReconciledTick(
                    **common,
                    starting_runtime_state=(
                        RuntimeState.VERIFYING
                        if attempt.status is AttemptStatus.SUCCEEDED
                        else RuntimeState.RECONCILING
                    ),
                    action_attempt_id=attempt.action_attempt_id,
                    task_id=attempt.task_id,
                    attempt_status=attempt.status,
                )
            WorkflowStore._insert_workflow_tick_in_connection(conn, tick)

    @staticmethod
    def _migrate_turn_metrics(conn: sqlite3.Connection) -> None:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(turn_metrics)").fetchall()
        }
        if "tick_id" in columns:
            return
        conn.execute("DROP INDEX IF EXISTS idx_turn_metrics_game_turn")
        conn.execute("ALTER TABLE turn_metrics RENAME TO turn_metrics_legacy_v3")
        conn.execute(
            """
            CREATE TABLE turn_metrics (
                tick_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                turn INTEGER NOT NULL,
                metrics_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO turn_metrics(
                tick_id, game_id, turn, metrics_json, created_at
            )
            SELECT
                'legacy:' || game_id || ':' || turn,
                game_id,
                turn,
                metrics_json,
                created_at
            FROM turn_metrics_legacy_v3
            """
        )
        conn.execute("DROP TABLE turn_metrics_legacy_v3")
        conn.execute(
            "CREATE INDEX idx_turn_metrics_game_turn ON turn_metrics (game_id, turn)"
        )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _dump(value: Any) -> str:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    @staticmethod
    def _load(value: str) -> Any:
        return json.loads(value)

    @classmethod
    def _set_meta_in_connection(
        cls, conn: sqlite3.Connection, key: str, value: Any
    ) -> None:
        conn.execute(
            """
            INSERT INTO workflow_meta(key, value_json) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=CURRENT_TIMESTAMP
            """,
            (key, cls._dump(value)),
        )

    @staticmethod
    def _human_wait_meta_key(game_id: str) -> str:
        return f"human_wait:{game_id}"

    def set_meta(self, key: str, value: Any) -> None:
        with self._connect() as conn:
            self._set_meta_in_connection(conn, key, value)

    def get_meta(self, key: str, default: Any = None) -> Any:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM workflow_meta WHERE key=?", (key,)
            ).fetchone()
        return default if row is None else self._load(row["value_json"])

    def human_wait_context(self, game_id: str) -> dict[str, Any] | None:
        context = self.get_meta(self._human_wait_meta_key(game_id))
        return dict(context) if isinstance(context, dict) else None

    def request_human_resume(self, game_id: str) -> bool:
        """Durably request one safe re-evaluation of an active human wait."""

        with self._connect() as conn:
            state = conn.execute(
                "SELECT state FROM runtime_state WHERE game_id=?", (game_id,)
            ).fetchone()
            if state is None or state["state"] != RuntimeState.AWAITING_HUMAN.value:
                return False
            key = self._human_wait_meta_key(game_id)
            row = conn.execute(
                "SELECT value_json FROM workflow_meta WHERE key=?", (key,)
            ).fetchone()
            context = {} if row is None else self._load(row["value_json"])
            if not isinstance(context, dict):
                context = {}
            context.update(
                {
                    "version": "human-wait/v1",
                    "resume_requested": True,
                    "resume_requested_at": datetime.now(UTC).isoformat(),
                }
            )
            self._set_meta_in_connection(conn, key, context)
        return True

    def _persist_human_wait_context_in_connection(
        self,
        conn: sqlite3.Connection,
        game_id: str,
        state: RuntimeState,
        context: dict[str, Any] | None,
    ) -> None:
        key = self._human_wait_meta_key(game_id)
        if state is not RuntimeState.AWAITING_HUMAN:
            conn.execute("DELETE FROM workflow_meta WHERE key=?", (key,))
            return
        if context is not None:
            self._set_meta_in_connection(conn, key, context)

    def upsert_event(
        self,
        game_id: str,
        event: GameEvent,
        *,
        cooldown_turns: int,
    ) -> tuple[GameEvent, bool]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM event_log WHERE game_id=? AND dedupe_key=?",
                (game_id, event.dedupe_key),
            ).fetchone()
            if row is None:
                event.first_seen_turn = event.turn
                event.last_seen_turn = event.turn
                conn.execute(
                    """
                    INSERT INTO event_log(
                        game_id, dedupe_key, event_json, event_type, level,
                        first_seen_turn, last_seen_turn, cooldown_until_turn
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        game_id,
                        event.dedupe_key,
                        event.model_dump_json(),
                        event.event_type,
                        int(event.level),
                        event.turn,
                        event.turn,
                        event.turn + max(0, cooldown_turns),
                    ),
                )
                return event, True

            event.first_seen_turn = int(row["first_seen_turn"])
            event.last_seen_turn = event.turn
            materially_changed = int(event.level) > int(row["level"])
            outside_cooldown = event.turn >= int(row["cooldown_until_turn"])
            needs_agent = event.blocking or event.level >= EventLevel.L3
            unsent_to_agent = needs_agent and row["last_agent_turn"] is None
            should_emit = (
                materially_changed
                or outside_cooldown
                or row["status"] == "resolved"
                or unsent_to_agent
            )
            cooldown_until = (
                event.turn + max(0, cooldown_turns)
                if should_emit
                else int(row["cooldown_until_turn"])
            )
            conn.execute(
                """
                UPDATE event_log SET
                    event_json=?, event_type=?, level=?, last_seen_turn=?,
                    seen_count=seen_count+1,
                    cooldown_until_turn=?, status='open'
                WHERE game_id=? AND dedupe_key=?
                """,
                (
                    event.model_dump_json(),
                    event.event_type,
                    int(event.level),
                    event.turn,
                    cooldown_until,
                    game_id,
                    event.dedupe_key,
                ),
            )
            return event, should_emit

    def mark_events_sent_to_agent(
        self, game_id: str, dedupe_keys: Sequence[str], turn: int
    ) -> None:
        if not dedupe_keys:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                UPDATE event_log SET last_agent_turn=?
                WHERE game_id=? AND dedupe_key=?
                """,
                [(turn, game_id, key) for key in dedupe_keys],
            )

    def resolve_event(self, game_id: str, dedupe_key: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE event_log SET status='resolved' WHERE game_id=? AND dedupe_key=?",
                (game_id, dedupe_key),
            )

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
        with self._connect() as conn:
            self._save_plan_bundle_in_connection(
                conn,
                game_id,
                turn,
                bundle,
                mode=mode,
                auto_action_types=auto_action_types,
                observation_id=observation_id,
            )

    def _save_plan_bundle_in_connection(
        self,
        conn: sqlite3.Connection,
        game_id: str,
        turn: int,
        bundle: PlanBundle,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
        observation_id: str | None = None,
    ) -> None:
        created_from_observation_id = (
            observation_id or f"legacy:{game_id}:{turn}:{bundle.plan_id}"
        )
        if bundle.strategy_updates:
            current = conn.execute(
                "SELECT state_json FROM strategy_state WHERE game_id=?", (game_id,)
            ).fetchone()
            merged = {} if current is None else self._load(current["state_json"])
            merged.update(bundle.strategy_updates)
            conn.execute(
                """
                INSERT INTO strategy_state(game_id, state_json, plan_id, updated_turn)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(game_id) DO UPDATE SET
                    state_json=excluded.state_json,
                    plan_id=excluded.plan_id,
                    updated_turn=excluded.updated_turn,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (game_id, self._dump(merged), bundle.plan_id, turn),
            )

        self._upsert_entity_plans(
            conn,
            "city_plans",
            "city_id",
            game_id,
            bundle.plan_id,
            turn,
            bundle.city_plan_updates,
        )
        self._upsert_entity_plans(
            conn,
            "unit_plans",
            "unit_id",
            game_id,
            bundle.plan_id,
            turn,
            bundle.unit_plan_updates,
        )
        self._upsert_entity_plans(
            conn,
            "builder_plans",
            "builder_key",
            game_id,
            bundle.plan_id,
            turn,
            bundle.builder_plan_updates,
        )

        for task_id in bundle.cancel_task_ids:
            conn.execute(
                """
                UPDATE workflow_tasks SET status=?, updated_at=CURRENT_TIMESTAMP
                WHERE game_id=? AND task_id=? AND status NOT IN (?, ?)
                """,
                (
                    TaskStatus.CANCELLED.value,
                    game_id,
                    task_id,
                    TaskStatus.DONE.value,
                    TaskStatus.CANCELLED.value,
                ),
            )

        for proposed in bundle.tasks:
            if mode is ExecutionMode.READONLY:
                status = TaskStatus.AWAITING_CONFIRMATION
            elif (
                proposed.requires_confirmation
                or proposed.action_type not in auto_action_types
            ):
                status = TaskStatus.AWAITING_CONFIRMATION
            elif proposed.due_turn <= turn:
                status = TaskStatus.READY
            else:
                status = TaskStatus.PENDING

            conn.execute(
                """
                INSERT INTO workflow_tasks(
                    game_id, task_id, plan_id, action_type, entity_type,
                    entity_id, due_turn, expires_turn, arguments_json,
                    preconditions_json, postconditions_json, invalidators_json,
                    risk, requires_confirmation, reason, status, created_turn,
                    created_from_observation_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_id, task_id) DO UPDATE SET
                    plan_id=excluded.plan_id,
                    action_type=excluded.action_type,
                    entity_type=excluded.entity_type,
                    entity_id=excluded.entity_id,
                    due_turn=excluded.due_turn,
                    expires_turn=excluded.expires_turn,
                    arguments_json=excluded.arguments_json,
                    preconditions_json=excluded.preconditions_json,
                    postconditions_json=excluded.postconditions_json,
                    invalidators_json=excluded.invalidators_json,
                    risk=excluded.risk,
                    requires_confirmation=excluded.requires_confirmation,
                    reason=excluded.reason,
                    status=CASE
                        WHEN workflow_tasks.status IN (
                            'done', 'failed', 'escalated'
                        ) THEN workflow_tasks.status
                        ELSE excluded.status
                    END,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    game_id,
                    proposed.task_id,
                    bundle.plan_id,
                    proposed.action_type,
                    proposed.entity_type,
                    str(proposed.entity_id),
                    proposed.due_turn,
                    proposed.expires_turn,
                    self._dump(proposed.arguments),
                    self._dump(proposed.preconditions),
                    self._dump(proposed.postconditions),
                    self._dump(proposed.invalidators),
                    proposed.risk.value,
                    int(proposed.requires_confirmation),
                    proposed.reason,
                    status.value,
                    turn,
                    created_from_observation_id,
                ),
            )

    def _upsert_entity_plans(
        self,
        conn: sqlite3.Connection,
        table: str,
        id_column: str,
        game_id: str,
        plan_id: str,
        turn: int,
        plans: Sequence[dict[str, Any]],
    ) -> None:
        allowed = {
            ("city_plans", "city_id"),
            ("unit_plans", "unit_id"),
            ("builder_plans", "builder_key"),
        }
        if (table, id_column) not in allowed:
            raise ValueError("invalid entity plan table")
        for plan in plans:
            entity_id = plan.get(id_column)
            if entity_id is None:
                raise ValueError(f"{id_column} is required in {table} update")
            conn.execute(
                f"""
                INSERT INTO {table}(game_id, {id_column}, plan_json, plan_id, updated_turn)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(game_id, {id_column}) DO UPDATE SET
                    plan_json=excluded.plan_json,
                    plan_id=excluded.plan_id,
                    updated_turn=excluded.updated_turn
                """,
                (game_id, str(entity_id), self._dump(plan), plan_id, turn),
            )

    def refresh_due_statuses(self, game_id: str, turn: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE workflow_tasks SET status=?, updated_at=CURRENT_TIMESTAMP
                WHERE game_id=? AND status=? AND due_turn<=?
                """,
                (TaskStatus.READY.value, game_id, TaskStatus.PENDING.value, turn),
            )
            conn.execute(
                """
                UPDATE workflow_tasks SET status=?, updated_at=CURRENT_TIMESTAMP
                WHERE game_id=? AND expires_turn IS NOT NULL AND expires_turn<?
                  AND status IN (?, ?, ?)
                """,
                (
                    TaskStatus.EXPIRED.value,
                    game_id,
                    turn,
                    TaskStatus.PENDING.value,
                    TaskStatus.READY.value,
                    TaskStatus.BLOCKED.value,
                ),
            )

    def observe_units(self, game_id: str, turn: int, units: Any) -> dict[str, int]:
        """Persist first-seen state and return bindable units in this snapshot.

        The first unit-bearing snapshot for a game is a baseline. Its units are not
        eligible for automatic binding, which prevents a migrated database from
        treating every existing builder as newly produced.
        """

        if units is None:
            return {}
        if isinstance(units, dict):
            rows = units.get("units", units.get("items", []))
        else:
            rows = units
        if not isinstance(rows, list):
            rows = []
        unit_rows = [row for row in rows if isinstance(row, dict)]
        baseline_key = f"unit_observations_initialized:{game_id}"
        current_ids: list[str] = []
        with self._connect() as conn:
            initialized = conn.execute(
                "SELECT 1 FROM workflow_meta WHERE key=?", (baseline_key,)
            ).fetchone()
            eligible = initialized is not None
            if initialized is None:
                conn.execute(
                    "INSERT INTO workflow_meta(key, value_json) VALUES (?, ?)",
                    (baseline_key, self._dump({"turn": turn})),
                )
            for row in unit_rows:
                raw_id = row.get("unit_id", row.get("id"))
                if raw_id is None:
                    continue
                unit_id = str(raw_id)
                current_ids.append(unit_id)
                unit_type = str(
                    row.get("unit_type", row.get("type", row.get("name", "")))
                )
                conn.execute(
                    """
                    INSERT INTO unit_observations(
                        game_id, unit_id, unit_type, first_seen_turn,
                        last_seen_turn, eligible_for_binding
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(game_id, unit_id) DO UPDATE SET
                        unit_type=excluded.unit_type,
                        last_seen_turn=excluded.last_seen_turn
                    """,
                    (game_id, unit_id, unit_type, turn, turn, int(eligible)),
                )
            if not current_ids:
                return {}
            placeholders = ",".join("?" for _ in current_ids)
            observed = conn.execute(
                f"""
                SELECT unit_id, first_seen_turn FROM unit_observations
                WHERE game_id=? AND eligible_for_binding=1
                  AND unit_id IN ({placeholders})
                """,
                (game_id, *current_ids),
            ).fetchall()
        return {str(row["unit_id"]): int(row["first_seen_turn"]) for row in observed}

    def bind_builder_plan(
        self, game_id: str, builder_key: str, unit_id: str, turn: int
    ) -> bool:
        """Atomically bind one unassigned plan without double-assigning the unit."""

        with self._connect() as conn:
            rows = conn.execute(
                "SELECT builder_key, plan_json FROM builder_plans WHERE game_id=?",
                (game_id,),
            ).fetchall()
            target: dict[str, Any] | None = None
            for row in rows:
                plan = self._load(row["plan_json"])
                assigned = plan.get("assigned_unit_id")
                if row["builder_key"] == builder_key:
                    target = plan
                    if assigned is not None:
                        return str(assigned) == str(unit_id)
                elif assigned is not None and str(assigned) == str(unit_id):
                    return False
            if target is None:
                return False
            target["assigned_unit_id"] = int(unit_id) if unit_id.isdigit() else unit_id
            target["auto_bound_turn"] = turn
            cursor = conn.execute(
                """
                UPDATE builder_plans SET plan_json=?, updated_turn=?
                WHERE game_id=? AND builder_key=?
                """,
                (self._dump(target), turn, game_id, builder_key),
            )
            return cursor.rowcount == 1

    def due_tasks(self, game_id: str, turn: int) -> list[StoredTask]:
        self.refresh_due_statuses(game_id, turn)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workflow_tasks
                WHERE game_id=? AND status=? AND due_turn<=?
                ORDER BY due_turn, task_id
                """,
                (game_id, TaskStatus.READY.value, turn),
            ).fetchall()
        return [self._row_to_task(row) for row in rows]

    def _row_to_task(self, row: sqlite3.Row) -> StoredTask:
        return StoredTask(
            task_id=row["task_id"],
            plan_id=row["plan_id"],
            action_type=row["action_type"],
            entity_type=row["entity_type"],
            entity_id=row["entity_id"],
            due_turn=int(row["due_turn"]),
            expires_turn=row["expires_turn"],
            arguments=self._load(row["arguments_json"]),
            preconditions=self._load(row["preconditions_json"]),
            postconditions=self._load(row["postconditions_json"]),
            invalidators=self._load(row["invalidators_json"]),
            risk=row["risk"],
            requires_confirmation=bool(row["requires_confirmation"]),
            reason=row["reason"],
            created_turn=int(row["created_turn"]),
            status=TaskStatus(row["status"]),
            retry_count=int(row["retry_count"]),
            max_retries=int(row["max_retries"]),
            last_error=row["last_error"],
            approved_by=row["approved_by"],
            created_from_observation_id=row["created_from_observation_id"],
        )

    def set_task_status(
        self,
        game_id: str,
        task_id: str,
        status: TaskStatus,
        *,
        error: str | None = None,
        increment_retry: bool = False,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE workflow_tasks SET
                    status=?, last_error=?, retry_count=retry_count+?,
                    updated_at=CURRENT_TIMESTAMP
                WHERE game_id=? AND task_id=?
                """,
                (status.value, error, int(increment_retry), game_id, task_id),
            )

    def approve_task(
        self, game_id: str, task_id: str, approved_by: str = "user"
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE workflow_tasks SET
                    status=?, approved_by=?, updated_at=CURRENT_TIMESTAMP
                WHERE game_id=? AND task_id=? AND status=?
                """,
                (
                    TaskStatus.READY.value,
                    approved_by,
                    game_id,
                    task_id,
                    TaskStatus.AWAITING_CONFIRMATION.value,
                ),
            )
            return cursor.rowcount == 1

    def list_tasks(
        self, game_id: str, statuses: Sequence[TaskStatus] | None = None
    ) -> list[StoredTask]:
        with self._connect() as conn:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                rows = conn.execute(
                    f"""
                    SELECT * FROM workflow_tasks
                    WHERE game_id=? AND status IN ({placeholders})
                    ORDER BY due_turn, task_id
                    """,
                    (game_id, *(status.value for status in statuses)),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM workflow_tasks WHERE game_id=? ORDER BY due_turn, task_id",
                    (game_id,),
                ).fetchall()
        return [self._row_to_task(row) for row in rows]

    def task_status(self, game_id: str, task_id: str) -> TaskStatus | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status FROM workflow_tasks WHERE game_id=? AND task_id=?",
                (game_id, task_id),
            ).fetchone()
        return None if row is None else TaskStatus(row["status"])

    def current_context(self, game_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            strategy = conn.execute(
                "SELECT state_json FROM strategy_state WHERE game_id=?", (game_id,)
            ).fetchone()
            cities = conn.execute(
                "SELECT city_id, plan_json, plan_id FROM city_plans WHERE game_id=?",
                (game_id,),
            ).fetchall()
            units = conn.execute(
                "SELECT unit_id, plan_json, plan_id FROM unit_plans WHERE game_id=?",
                (game_id,),
            ).fetchall()
            builders = conn.execute(
                """
                SELECT builder_key, plan_json, plan_id, updated_turn
                FROM builder_plans WHERE game_id=?
                """,
                (game_id,),
            ).fetchall()
        return {
            "strategy": {} if strategy is None else self._load(strategy["state_json"]),
            "cities": {
                row["city_id"]: {
                    **self._load(row["plan_json"]),
                    "_plan_id": row["plan_id"],
                }
                for row in cities
            },
            "units": {
                row["unit_id"]: {
                    **self._load(row["plan_json"]),
                    "_plan_id": row["plan_id"],
                }
                for row in units
            },
            "builders": {
                row["builder_key"]: {
                    **self._load(row["plan_json"]),
                    "_plan_id": row["plan_id"],
                    "_updated_turn": int(row["updated_turn"]),
                }
                for row in builders
            },
        }

    def task_ids(self, game_id: str) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT task_id FROM workflow_tasks WHERE game_id=?",
                (game_id,),
            ).fetchall()
        return {str(row["task_id"]) for row in rows}

    def get_task(self, game_id: str, task_id: str) -> StoredTask | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM workflow_tasks WHERE game_id=? AND task_id=?",
                (game_id, task_id),
            ).fetchone()
        return None if row is None else self._row_to_task(row)

    @staticmethod
    def _attempt_row_values(attempt: ActionAttempt) -> tuple[Any, ...]:
        payload = attempt.model_dump(mode="json")
        dump = WorkflowStore._dump
        return (
            attempt.action_attempt_id,
            attempt.game_session_id,
            attempt.task_id,
            attempt.action_type,
            attempt.attempt_number,
            attempt.request_id,
            attempt.idempotency_key,
            attempt.prepared_from_observation_id,
            payload["prepared_at"],
            payload["sent_at"],
            payload["response_received_at"],
            attempt.status.value,
            attempt.retry_classification.value,
            dump(payload["normalized_arguments"]),
            (
                None
                if payload["transport_result"] is None
                else dump(payload["transport_result"])
            ),
            None if payload["tool_result"] is None else dump(payload["tool_result"]),
            (
                None
                if attempt.verification_status is None
                else attempt.verification_status.value
            ),
            attempt.last_verification_observation_id,
            attempt.parent_attempt_id,
            attempt.pre_send_turn,
            dump(payload["postconditions"]),
            attempt.postcondition_version,
            attempt.verification_count,
            attempt.model_dump_json(),
        )

    def save_action_attempt(self, attempt: ActionAttempt) -> None:
        if attempt.game_session_id is None:
            raise ValueError("persisted attempts require game_session_id")
        if attempt.action_type is None:
            raise ValueError("persisted attempts require action_type")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO action_attempts(
                    action_attempt_id, game_id, task_id, action_type,
                    attempt_number, request_id, idempotency_key,
                    prepared_from_observation_id, prepared_at, sent_at,
                    response_received_at, status, retry_classification,
                    normalized_arguments_json, transport_result_json,
                    tool_result_json, verification_status,
                    last_verification_observation_id, parent_attempt_id,
                    pre_send_turn, postconditions_json, postcondition_version,
                    verification_count, attempt_json
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?
                )
                """,
                self._attempt_row_values(attempt),
            )
            self._append_attempt_transition(conn, attempt)

    def update_action_attempt(self, attempt: ActionAttempt) -> None:
        with self._connect() as conn:
            self._update_action_attempt_in_connection(conn, attempt)

    def _update_action_attempt_in_connection(
        self,
        conn: sqlite3.Connection,
        attempt: ActionAttempt,
    ) -> None:
        row = conn.execute(
            "SELECT attempt_json FROM action_attempts WHERE action_attempt_id=?",
            (attempt.action_attempt_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown action attempt: {attempt.action_attempt_id}")
        current = ActionAttempt.model_validate_json(row["attempt_json"])
        immutable_fields = (
            "action_attempt_id",
            "game_session_id",
            "task_id",
            "action_type",
            "attempt_number",
            "request_id",
            "idempotency_key",
            "prepared_from_observation_id",
            "prepared_at",
            "retry_classification",
            "normalized_arguments",
            "parent_attempt_id",
            "pre_send_turn",
            "postconditions",
            "postcondition_version",
        )
        if any(
            getattr(current, field) != getattr(attempt, field)
            for field in immutable_fields
        ):
            raise ValueError("attempt identity and delivery contract are immutable")
        allowed = {
            AttemptStatus.PREPARED: {
                AttemptStatus.REJECTED_BEFORE_SEND,
                AttemptStatus.UNCERTAIN,
            },
            AttemptStatus.UNCERTAIN: {
                AttemptStatus.UNCERTAIN,
                AttemptStatus.VERIFYING,
                AttemptStatus.FAILED,
                AttemptStatus.SUCCEEDED,
            },
            AttemptStatus.VERIFYING: {
                AttemptStatus.VERIFYING,
                AttemptStatus.UNCERTAIN,
                AttemptStatus.FAILED,
                AttemptStatus.SUCCEEDED,
            },
            AttemptStatus.REJECTED_BEFORE_SEND: set(),
            AttemptStatus.FAILED: set(),
            AttemptStatus.SUCCEEDED: set(),
        }
        if attempt.status not in allowed[current.status]:
            raise ValueError(
                f"invalid attempt transition {current.status} -> {attempt.status}"
            )
        values = self._attempt_row_values(attempt)
        conn.execute(
            """
            UPDATE action_attempts SET
                game_id=?, task_id=?, action_type=?, attempt_number=?,
                request_id=?, idempotency_key=?,
                prepared_from_observation_id=?, prepared_at=?, sent_at=?,
                response_received_at=?, status=?, retry_classification=?,
                normalized_arguments_json=?, transport_result_json=?,
                tool_result_json=?, verification_status=?,
                last_verification_observation_id=?, parent_attempt_id=?,
                pre_send_turn=?, postconditions_json=?,
                postcondition_version=?, verification_count=?,
                attempt_json=?, updated_at=CURRENT_TIMESTAMP
            WHERE action_attempt_id=?
            """,
            (*values[1:], values[0]),
        )
        self._append_attempt_transition(conn, attempt)

    @staticmethod
    def _append_attempt_transition(
        conn: sqlite3.Connection,
        attempt: ActionAttempt,
    ) -> None:
        conn.execute(
            """
            INSERT INTO action_attempt_transitions(
                game_id, action_attempt_id, status, attempt_json
            ) VALUES (?, ?, ?, ?)
            """,
            (
                attempt.game_session_id,
                attempt.action_attempt_id,
                attempt.status.value,
                attempt.model_dump_json(),
            ),
        )

    def get_action_attempt(self, action_attempt_id: str) -> ActionAttempt | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT attempt_json FROM action_attempts WHERE action_attempt_id=?",
                (action_attempt_id,),
            ).fetchone()
        return (
            None
            if row is None
            else ActionAttempt.model_validate_json(row["attempt_json"])
        )

    def list_action_attempts(
        self,
        game_id: str,
        *,
        statuses: Sequence[AttemptStatus] | None = None,
    ) -> list[ActionAttempt]:
        with self._connect() as conn:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                rows = conn.execute(
                    f"""
                    SELECT attempt_json FROM action_attempts
                    WHERE game_id=? AND status IN ({placeholders})
                    ORDER BY prepared_at, attempt_number
                    """,
                    (game_id, *(status.value for status in statuses)),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT attempt_json FROM action_attempts
                    WHERE game_id=?
                    ORDER BY prepared_at, attempt_number
                    """,
                    (game_id,),
                ).fetchall()
        return [ActionAttempt.model_validate_json(row["attempt_json"]) for row in rows]

    def unresolved_action_attempt(self, game_id: str) -> ActionAttempt | None:
        attempts = self.list_action_attempts(
            game_id,
            statuses=[
                AttemptStatus.PREPARED,
                AttemptStatus.VERIFYING,
                AttemptStatus.UNCERTAIN,
            ],
        )
        if len(attempts) > 1:
            raise RuntimeError(
                f"game {game_id} has multiple unresolved action attempts"
            )
        return attempts[0] if attempts else None

    def latest_attempt_for_task(
        self,
        game_id: str,
        task_id: str,
    ) -> ActionAttempt | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT attempt_json FROM action_attempts
                WHERE game_id=? AND task_id=?
                ORDER BY attempt_number DESC
                LIMIT 1
                """,
                (game_id, task_id),
            ).fetchone()
        return (
            None
            if row is None
            else ActionAttempt.model_validate_json(row["attempt_json"])
        )

    def next_attempt_number(self, game_id: str, task_id: str) -> int:
        latest = self.latest_attempt_for_task(game_id, task_id)
        return 1 if latest is None else latest.attempt_number + 1

    def load_runtime_state(self, game_id: str) -> RuntimeState:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT state FROM runtime_state WHERE game_id=?",
                (game_id,),
            ).fetchone()
        return RuntimeState.OBSERVING if row is None else RuntimeState(row["state"])

    @staticmethod
    def _save_runtime_state_in_connection(
        conn: sqlite3.Connection,
        game_id: str,
        state: RuntimeState,
        active_attempt_id: str | None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO runtime_state(game_id, state, active_attempt_id)
            VALUES (?, ?, ?)
            ON CONFLICT(game_id) DO UPDATE SET
                state=excluded.state,
                active_attempt_id=excluded.active_attempt_id,
                revision=runtime_state.revision+1,
                updated_at=CURRENT_TIMESTAMP
            """,
            (game_id, state.value, active_attempt_id),
        )

    def save_runtime_state(
        self,
        game_id: str,
        state: RuntimeState,
        *,
        active_attempt_id: str | None = None,
    ) -> None:
        with self._connect() as conn:
            self._save_runtime_state_in_connection(
                conn, game_id, state, active_attempt_id
            )

    @classmethod
    def _insert_workflow_tick_in_connection(
        cls,
        conn: sqlite3.Connection,
        tick: WorkflowTick,
    ) -> None:
        conn.execute(
            """
            INSERT INTO workflow_ticks(
                tick_id, game_id, turn, outcome,
                starting_runtime_state, ending_runtime_state,
                observation_ids_json, mutation_budget_used,
                selected_task_id, action_attempt_id,
                planner_request_id, started_at, completed_at,
                metrics_json, tick_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tick.tick_id,
                tick.game_session_id,
                tick.turn_number,
                tick.outcome.value,
                tick.starting_runtime_state.value,
                tick.ending_runtime_state.value,
                cls._dump(list(tick.observation_ids)),
                tick.mutation_budget_used,
                getattr(tick, "task_id", None),
                getattr(tick, "action_attempt_id", None),
                getattr(tick, "planner_request_id", None),
                tick.started_at.isoformat(),
                tick.completed_at.isoformat(),
                cls._dump(tick.model_dump(mode="json")["metrics"]),
                tick.model_dump_json(),
            ),
        )
        conn.execute(
            """
            INSERT INTO turn_metrics(tick_id, game_id, turn, metrics_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                tick.tick_id,
                tick.game_session_id,
                tick.turn_number,
                cls._dump(tick.model_dump(mode="json")["metrics"]),
            ),
        )

    def save_workflow_tick(self, tick: WorkflowTick) -> None:
        tick = validate_workflow_tick(tick)
        with self._connect() as conn:
            self._insert_workflow_tick_in_connection(conn, tick)

    def persist_tick_and_runtime_state(
        self,
        tick: WorkflowTick,
        *,
        active_attempt_id: str | None = None,
        attempt: ActionAttempt | None = None,
        task_status: TaskStatus | None = None,
        task_error: str | None = None,
        resolve_failed_task: bool = False,
        checkpoint: Callable[[str], None] | None = None,
        attempt_checkpoint: str | None = None,
        human_wait_context: dict[str, Any] | None = None,
    ) -> FailedAttemptResolution | None:
        tick = validate_workflow_tick(tick)
        if attempt is not None and attempt.game_session_id != tick.game_session_id:
            raise ValueError("attempt and Tick must belong to the same game")
        if task_status is not None and attempt is None:
            raise ValueError("task status update requires an attempt")
        if resolve_failed_task and (
            attempt is None or attempt.status is not AttemptStatus.FAILED
        ):
            raise ValueError("failed task resolution requires a FAILED attempt")
        if resolve_failed_task and task_status is not None:
            raise ValueError("failed task resolution determines the task status")

        failure_resolution: FailedAttemptResolution | None = None
        task_retry_count: int | None = None
        with self._connect() as conn:
            if attempt is not None:
                self._update_action_attempt_in_connection(conn, attempt)
                if checkpoint is not None and attempt_checkpoint is not None:
                    checkpoint(attempt_checkpoint)
            if resolve_failed_task and attempt is not None:
                task_row = conn.execute(
                    """
                    SELECT retry_count, max_retries FROM workflow_tasks
                    WHERE game_id=? AND task_id=?
                    """,
                    (tick.game_session_id, attempt.task_id),
                ).fetchone()
                if task_row is None:
                    raise KeyError(f"unknown attempt task: {attempt.task_id}")
                failure_resolution = resolve_failed_attempt(
                    attempt,
                    retry_count=int(task_row["retry_count"]),
                    max_retries=int(task_row["max_retries"]),
                    failure_reason=task_error,
                )
                task_status = failure_resolution.task_status
                task_error = failure_resolution.reason
                task_retry_count = failure_resolution.retry_count
            if task_status is not None and attempt is not None:
                cursor = conn.execute(
                    """
                    UPDATE workflow_tasks SET
                        status=?, last_error=?,
                        retry_count=COALESCE(?, retry_count),
                        updated_at=CURRENT_TIMESTAMP
                    WHERE game_id=? AND task_id=?
                    """,
                    (
                        task_status.value,
                        task_error,
                        task_retry_count,
                        tick.game_session_id,
                        attempt.task_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise KeyError(f"unknown attempt task: {attempt.task_id}")
            self._save_runtime_state_in_connection(
                conn,
                tick.game_session_id,
                tick.ending_runtime_state,
                active_attempt_id,
            )
            self._persist_human_wait_context_in_connection(
                conn,
                tick.game_session_id,
                tick.ending_runtime_state,
                human_wait_context,
            )
            if checkpoint is not None:
                checkpoint("after_runtime_state_update")
            self._insert_workflow_tick_in_connection(conn, tick)
        return failure_resolution

    def finalize_attempt_success(
        self,
        attempt: ActionAttempt,
        tick: WorkflowTick,
        *,
        checkpoint: Callable[[str], None] | None = None,
    ) -> None:
        if attempt.status is not AttemptStatus.SUCCEEDED:
            raise ValueError("success finalization requires SUCCEEDED attempt")
        self.persist_tick_and_runtime_state(
            tick,
            attempt=attempt,
            task_status=TaskStatus.DONE,
            active_attempt_id=None,
            checkpoint=checkpoint,
            attempt_checkpoint="after_attempt_succeeded_update",
        )

    def finalize_attempt_failure(
        self,
        attempt: ActionAttempt,
        tick: WorkflowTick,
        *,
        task_error: str | None = None,
        checkpoint: Callable[[str], None] | None = None,
    ) -> FailedAttemptResolution:
        if attempt.status is not AttemptStatus.FAILED:
            raise ValueError("failure finalization requires FAILED attempt")
        resolution = self.persist_tick_and_runtime_state(
            tick,
            attempt=attempt,
            task_error=task_error,
            resolve_failed_task=True,
            active_attempt_id=None,
            checkpoint=checkpoint,
            attempt_checkpoint="after_attempt_failed_update",
        )
        if resolution is None:
            raise AssertionError("failed attempt finalization did not resolve the task")
        return resolution

    def recover_prepared_attempt(
        self,
        attempt: ActionAttempt,
        tick: WorkflowTick,
        *,
        checkpoint: Callable[[str], None] | None = None,
    ) -> None:
        if attempt.status is not AttemptStatus.REJECTED_BEFORE_SEND:
            raise ValueError("prepared recovery requires REJECTED_BEFORE_SEND attempt")
        self.persist_tick_and_runtime_state(
            tick,
            attempt=attempt,
            task_status=(
                None if attempt.action_type == "end_turn" else TaskStatus.READY
            ),
            task_error=(
                None
                if attempt.action_type == "end_turn"
                else "recovered a prepared attempt before delivery began"
            ),
            active_attempt_id=None,
            checkpoint=checkpoint,
            attempt_checkpoint="after_prepared_attempt_rejected_update",
        )

    def finalize_turn_transition(
        self,
        attempt: ActionAttempt,
        tick: WorkflowTick,
        *,
        checkpoint: Callable[[str], None] | None = None,
    ) -> None:
        if attempt.action_type != "end_turn":
            raise ValueError("turn finalization requires end_turn attempt")
        if attempt.status is not AttemptStatus.SUCCEEDED:
            raise ValueError("turn finalization requires SUCCEEDED attempt")
        self.persist_tick_and_runtime_state(
            tick,
            attempt=attempt,
            active_attempt_id=None,
            checkpoint=checkpoint,
            attempt_checkpoint="after_end_turn_succeeded_update",
        )

    def list_workflow_ticks(self, game_id: str) -> list[WorkflowTick]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT tick_json FROM workflow_ticks
                WHERE game_id=?
                ORDER BY started_at, tick_id
                """,
                (game_id,),
            ).fetchall()
        return [validate_workflow_tick(self._load(row["tick_json"])) for row in rows]

    @staticmethod
    def _save_decision_gap_in_connection(
        conn: sqlite3.Connection,
        gap: DecisionGap,
        turn: int,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        created_at = gap.created_at.isoformat() if gap.created_at else now
        updated_at = gap.updated_at.isoformat() if gap.updated_at else now
        conn.execute(
            """
            INSERT INTO decision_gaps(
                decision_gap_id, game_id, stable_identity, gap_type, scope,
                status, route, relevant_input_hash, input_projection_version,
                logical_request_id, first_seen_turn, last_seen_turn,
                gap_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(game_id, stable_identity) DO UPDATE SET
                gap_type=excluded.gap_type,
                scope=excluded.scope,
                status=excluded.status,
                route=excluded.route,
                relevant_input_hash=excluded.relevant_input_hash,
                input_projection_version=excluded.input_projection_version,
                logical_request_id=excluded.logical_request_id,
                last_seen_turn=excluded.last_seen_turn,
                gap_json=excluded.gap_json,
                updated_at=excluded.updated_at
            """,
            (
                gap.decision_gap_id,
                gap.game_session_id,
                gap.stable_identity,
                gap.gap_type,
                gap.scope,
                gap.status.value,
                gap.route.value,
                gap.relevant_input_hash,
                gap.input_projection_version,
                gap.logical_request_id,
                turn,
                turn,
                gap.model_dump_json(),
                created_at,
                updated_at,
            ),
        )

    def save_decision_gap(self, gap: DecisionGap, *, turn: int) -> None:
        with self._connect() as conn:
            self._save_decision_gap_in_connection(conn, gap, turn)

    def decision_gap_by_identity(
        self, game_id: str, stable_identity: str
    ) -> DecisionGap | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT gap_json FROM decision_gaps
                WHERE game_id=? AND stable_identity=?
                """,
                (game_id, stable_identity),
            ).fetchone()
        return None if row is None else DecisionGap.model_validate_json(row["gap_json"])

    def get_decision_gap(
        self, game_id: str, decision_gap_id: str
    ) -> DecisionGap | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT gap_json FROM decision_gaps WHERE game_id=? AND decision_gap_id=?",
                (game_id, decision_gap_id),
            ).fetchone()
        return None if row is None else DecisionGap.model_validate_json(row["gap_json"])

    def list_decision_gaps(
        self,
        game_id: str,
        *,
        statuses: Sequence[DecisionGapStatus] | None = None,
    ) -> list[DecisionGap]:
        query = "SELECT gap_json FROM decision_gaps WHERE game_id=?"
        values: list[Any] = [game_id]
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            query += f" AND status IN ({placeholders})"
            values.extend(status.value for status in statuses)
        query += " ORDER BY created_at, decision_gap_id"
        with self._connect() as conn:
            rows = conn.execute(query, values).fetchall()
        return [DecisionGap.model_validate_json(row["gap_json"]) for row in rows]

    @staticmethod
    def _invalidate_plan_projection_in_connection(
        conn: sqlite3.Connection, lease: PlanLease
    ) -> None:
        for subject in lease.subjects:
            table_and_column = {
                "city": ("city_plans", "city_id"),
                "unit": ("unit_plans", "unit_id"),
                "builder": ("builder_plans", "builder_key"),
            }.get(subject.subject_type)
            if table_and_column is None:
                continue
            table, column = table_and_column
            conn.execute(
                f"DELETE FROM {table} WHERE game_id=? AND {column}=? AND plan_id=?",
                (
                    lease.game_session_id,
                    subject.subject_id,
                    lease.plan_id,
                ),
            )
        if lease.scope == "empire":
            conn.execute(
                "DELETE FROM strategy_state WHERE game_id=? AND plan_id=?",
                (lease.game_session_id, lease.plan_id),
            )

    @staticmethod
    def _save_plan_lease_in_connection(
        conn: sqlite3.Connection, lease: PlanLease
    ) -> None:
        conn.execute(
            """
            INSERT INTO plan_leases(
                plan_lease_id, game_id, scope, status, plan_revision,
                relevant_input_hash, source_planner_request_id, lease_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(plan_lease_id) DO UPDATE SET
                scope=excluded.scope,
                status=excluded.status,
                plan_revision=excluded.plan_revision,
                relevant_input_hash=excluded.relevant_input_hash,
                source_planner_request_id=excluded.source_planner_request_id,
                lease_json=excluded.lease_json,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                lease.plan_lease_id,
                lease.game_session_id,
                lease.scope,
                lease.status.value,
                lease.plan_revision,
                lease.relevant_input_hash,
                lease.source_planner_request_id,
                lease.model_dump_json(),
            ),
        )

    def save_plan_lease(self, lease: PlanLease) -> None:
        with self._connect() as conn:
            self._save_plan_lease_in_connection(conn, lease)

    def save_approval_record(self, game_id: str, record: ApprovalRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO approval_records(
                    approval_id, game_id, proposal_type, proposal_id,
                    proposal_revision, decision, record_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(approval_id) DO NOTHING
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

    def latest_approval_record(
        self,
        game_id: str,
        *,
        proposal_type: str,
        proposal_id: str,
        proposal_revision: int,
    ) -> ApprovalRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT record_json FROM approval_records
                WHERE game_id=? AND proposal_type=? AND proposal_id=?
                  AND proposal_revision=?
                ORDER BY created_at DESC, approval_id DESC
                LIMIT 1
                """,
                (game_id, proposal_type, proposal_id, proposal_revision),
            ).fetchone()
        return (
            None
            if row is None
            else ApprovalRecord.model_validate_json(row["record_json"])
        )

    def list_plan_leases(self, game_id: str) -> list[PlanLease]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT lease_json FROM plan_leases
                WHERE game_id=? ORDER BY scope, plan_lease_id
                """,
                (game_id,),
            ).fetchall()
        return [PlanLease.model_validate_json(row["lease_json"]) for row in rows]

    @staticmethod
    def _save_planner_request_in_connection(
        conn: sqlite3.Connection, request: PlannerRequest
    ) -> None:
        conn.execute(
            """
            INSERT INTO logical_planner_requests(
                planner_request_id, game_id, decision_group_id, turn, status,
                input_projection_hash, input_projection_version,
                decision_gap_ids_json, request_json, created_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(planner_request_id) DO UPDATE SET
                status=excluded.status,
                request_json=excluded.request_json,
                completed_at=excluded.completed_at
            """,
            (
                request.planner_request_id,
                request.game_session_id,
                request.decision_group_id,
                request.turn_number,
                request.status.value,
                request.input_projection_hash,
                request.input_projection_version,
                WorkflowStore._dump(list(request.decision_gap_ids)),
                request.model_dump_json(),
                request.created_at.isoformat(),
                (
                    None
                    if request.completed_at is None
                    else request.completed_at.isoformat()
                ),
            ),
        )
        WorkflowStore._abandon_started_provider_attempts_for_terminal_request(
            conn, request
        )

    @staticmethod
    def _abandon_started_provider_attempts_for_terminal_request(
        conn: sqlite3.Connection, request: PlannerRequest
    ) -> None:
        if request.status not in {
            PlannerRequestStatus.COMPLETED,
            PlannerRequestStatus.PARTIALLY_COMPLETED,
            PlannerRequestStatus.SUPERSEDED,
            PlannerRequestStatus.CANCELLED,
            PlannerRequestStatus.FAILED,
            PlannerRequestStatus.REJECTED,
        }:
            return
        completed_at = request.completed_at or datetime.now(UTC)
        rows = conn.execute(
            """
            SELECT attempt_json FROM provider_attempts
            WHERE planner_request_id=? AND status=?
            ORDER BY attempt_number
            """,
            (
                request.planner_request_id,
                ProviderAttemptStatus.STARTED.value,
            ),
        ).fetchall()
        for row in rows:
            started = ProviderAttempt.model_validate_json(row["attempt_json"])
            diagnostics = dict(started.diagnostics)
            diagnostics.update(
                {
                    "abandoned_by_request_status": request.status.value,
                    "termination_reason": request.failure_category
                    or f"request_{request.status.value.lower()}",
                }
            )
            abandoned = started.model_copy(
                update={
                    "status": ProviderAttemptStatus.ABANDONED,
                    "completed_at": completed_at,
                    "latency_seconds": max(
                        0.0, (completed_at - started.started_at).total_seconds()
                    ),
                    "failure_category": (
                        request.failure_category
                        or f"request_{request.status.value.lower()}"
                    ),
                    "diagnostics": diagnostics,
                }
            )
            WorkflowStore._save_provider_attempt_in_connection(
                conn, request.game_session_id, abandoned
            )

    def save_planner_request(self, request: PlannerRequest) -> None:
        with self._connect() as conn:
            self._save_planner_request_in_connection(conn, request)

    def get_planner_request(self, planner_request_id: str) -> PlannerRequest | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_json FROM logical_planner_requests
                WHERE planner_request_id=?
                """,
                (planner_request_id,),
            ).fetchone()
        return (
            None
            if row is None
            else PlannerRequest.model_validate_json(row["request_json"])
        )

    def active_planner_request(self, game_id: str) -> PlannerRequest | None:
        terminal = tuple(
            status.value
            for status in (
                PlannerRequestStatus.COMPLETED,
                PlannerRequestStatus.PARTIALLY_COMPLETED,
                PlannerRequestStatus.FAILED,
                PlannerRequestStatus.REJECTED,
                PlannerRequestStatus.CANCELLED,
                PlannerRequestStatus.SUPERSEDED,
            )
        )
        placeholders = ",".join("?" for _ in terminal)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT request_json FROM logical_planner_requests
                WHERE game_id=? AND status NOT IN ({placeholders})
                ORDER BY created_at LIMIT 1
                """,
                (game_id, *terminal),
            ).fetchone()
        return (
            None
            if row is None
            else PlannerRequest.model_validate_json(row["request_json"])
        )

    def planner_request_for_input(
        self,
        game_id: str,
        decision_group_id: str,
        input_projection_hash: str,
    ) -> PlannerRequest | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_json FROM logical_planner_requests
                WHERE game_id=? AND decision_group_id=?
                  AND input_projection_hash=?
                """,
                (game_id, decision_group_id, input_projection_hash),
            ).fetchone()
        return (
            None
            if row is None
            else PlannerRequest.model_validate_json(row["request_json"])
        )

    def logical_request_count_for_turn(self, game_id: str, turn: int) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS value FROM logical_planner_requests
                WHERE game_id=? AND turn=?
                """,
                (game_id, turn),
            ).fetchone()
        return int(row["value"])

    def provider_budget_request_count_for_turn(self, game_id: str, turn: int) -> int:
        """Count requests that consumed this turn's provider-call budget.

        A superseded request with no persisted ProviderAttempt never reached the
        provider boundary, so a successor may reuse the same turn's one-call
        budget while the original request remains available for audit.
        """

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS value
                FROM logical_planner_requests AS request
                WHERE request.game_id=? AND request.turn=?
                  AND (
                    request.status != ?
                    OR EXISTS(
                        SELECT 1 FROM provider_attempts AS attempt
                        WHERE attempt.planner_request_id=request.planner_request_id
                    )
                  )
                """,
                (game_id, turn, PlannerRequestStatus.SUPERSEDED.value),
            ).fetchone()
        return int(row["value"])

    @staticmethod
    def _save_provider_attempt_in_connection(
        conn: sqlite3.Connection,
        game_id: str,
        attempt: ProviderAttempt,
    ) -> None:
        conn.execute(
            """
            INSERT INTO provider_attempts(
                provider_attempt_id, game_id, planner_request_id,
                attempt_number, provider_request_id, status, attempt_json,
                started_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_attempt_id) DO UPDATE SET
                status=excluded.status,
                attempt_json=excluded.attempt_json,
                completed_at=excluded.completed_at
            """,
            (
                attempt.provider_attempt_id,
                game_id,
                attempt.planner_request_id,
                attempt.attempt_number,
                attempt.provider_request_id,
                attempt.status.value,
                attempt.model_dump_json(),
                attempt.started_at.isoformat(),
                (
                    None
                    if attempt.completed_at is None
                    else attempt.completed_at.isoformat()
                ),
            ),
        )

    def save_provider_attempt(self, game_id: str, attempt: ProviderAttempt) -> None:
        with self._connect() as conn:
            self._save_provider_attempt_in_connection(conn, game_id, attempt)

    def start_provider_attempt(
        self,
        game_id: str,
        request: PlannerRequest,
        attempt: ProviderAttempt,
    ) -> PlannerRequest:
        """Persist STARTED before the provider call and abandon crash leftovers."""

        if attempt.status is not ProviderAttemptStatus.STARTED:
            raise ValueError("provider attempt must start in STARTED")
        if request.game_session_id != game_id:
            raise ValueError("planner request belongs to another game")
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT attempt_json FROM provider_attempts
                WHERE game_id=? AND planner_request_id=? AND status=?
                ORDER BY attempt_number
                """,
                (
                    game_id,
                    request.planner_request_id,
                    ProviderAttemptStatus.STARTED.value,
                ),
            ).fetchall()
            for row in rows:
                interrupted = ProviderAttempt.model_validate_json(
                    row["attempt_json"]
                ).model_copy(
                    update={
                        "status": ProviderAttemptStatus.ABANDONED,
                        "completed_at": attempt.started_at,
                        "latency_seconds": 0.0,
                        "failure_category": "provider_process_interrupted",
                        "diagnostics": {
                            "recovered_on_restart": True,
                            "delivery": "unknown",
                        },
                    }
                )
                self._save_provider_attempt_in_connection(conn, game_id, interrupted)
            expected = int(
                conn.execute(
                    """
                    SELECT COALESCE(MAX(attempt_number), 0) + 1 AS value
                    FROM provider_attempts
                    WHERE game_id=? AND planner_request_id=?
                    """,
                    (game_id, request.planner_request_id),
                ).fetchone()["value"]
            )
            if attempt.attempt_number != expected:
                raise ValueError(
                    f"provider attempt_number must be {expected}, "
                    f"got {attempt.attempt_number}"
                )
            in_progress = request.model_copy(
                update={
                    "status": PlannerRequestStatus.IN_PROGRESS,
                    "provider_attempt_count": attempt.attempt_number,
                }
            )
            self._save_planner_request_in_connection(conn, in_progress)
            self._save_provider_attempt_in_connection(conn, game_id, attempt)
        return in_progress

    def list_provider_attempts(self, planner_request_id: str) -> list[ProviderAttempt]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT attempt_json FROM provider_attempts
                WHERE planner_request_id=? ORDER BY attempt_number
                """,
                (planner_request_id,),
            ).fetchall()
        return [
            ProviderAttempt.model_validate_json(row["attempt_json"]) for row in rows
        ]

    @staticmethod
    def _save_information_round_in_connection(
        conn: sqlite3.Connection,
        game_id: str,
        round_record: InformationRound,
    ) -> None:
        conn.execute(
            """
            INSERT INTO information_rounds(
                information_round_id, game_id, planner_request_id,
                round_number, status, round_json, requested_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(information_round_id) DO UPDATE SET
                status=excluded.status,
                round_json=excluded.round_json,
                completed_at=excluded.completed_at
            """,
            (
                round_record.information_round_id,
                game_id,
                round_record.planner_request_id,
                round_record.round_number,
                round_record.status.value,
                round_record.model_dump_json(),
                round_record.requested_at.isoformat(),
                (
                    None
                    if round_record.completed_at is None
                    else round_record.completed_at.isoformat()
                ),
            ),
        )

    def save_information_round(
        self, game_id: str, round_record: InformationRound
    ) -> None:
        with self._connect() as conn:
            self._save_information_round_in_connection(conn, game_id, round_record)

    def list_information_rounds(
        self, planner_request_id: str
    ) -> list[InformationRound]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT round_json FROM information_rounds
                WHERE planner_request_id=? ORDER BY round_number
                """,
                (planner_request_id,),
            ).fetchall()
        return [InformationRound.model_validate_json(row["round_json"]) for row in rows]

    def record_planner_suppression(
        self,
        game_id: str,
        turn: int,
        *,
        reason: str,
        decision_gap_id: str | None = None,
        relevant_input_hash: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO planner_suppressions(
                    game_id, turn, decision_gap_id, reason, relevant_input_hash
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    game_id,
                    turn,
                    decision_gap_id,
                    reason,
                    relevant_input_hash,
                ),
            )

    def persist_phase4_tick(
        self,
        tick: WorkflowTick,
        *,
        decision_gaps: Sequence[DecisionGap] = (),
        decision_group: DecisionGroup | None = None,
        plan_leases: Sequence[PlanLease] = (),
        planner_request: PlannerRequest | None = None,
        provider_attempts: Sequence[ProviderAttempt] = (),
        information_round: InformationRound | None = None,
        plan_bundle: PlanBundle | None = None,
        plan_bundle_mode: ExecutionMode | None = None,
        plan_bundle_auto_action_types: Sequence[str] = (),
        plan_bundle_observation_id: str | None = None,
        active_attempt_id: str | None = None,
        cancel_task_ids: Sequence[str] = (),
        human_wait_context: dict[str, Any] | None = None,
    ) -> None:
        tick = validate_workflow_tick(tick)
        with self._connect() as conn:
            if plan_bundle is not None:
                if plan_bundle_mode is None:
                    raise ValueError("plan bundle persistence requires execution mode")
                self._save_plan_bundle_in_connection(
                    conn,
                    tick.game_session_id,
                    tick.turn_number,
                    plan_bundle,
                    mode=plan_bundle_mode,
                    auto_action_types=set(plan_bundle_auto_action_types),
                    observation_id=plan_bundle_observation_id,
                )
            for gap in decision_gaps:
                if gap.game_session_id != tick.game_session_id:
                    raise ValueError("decision gap and Tick must belong to one game")
                self._save_decision_gap_in_connection(conn, gap, tick.turn_number)
            if decision_group is not None:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO decision_groups(
                        decision_group_id, game_id, observation_id,
                        decision_gap_ids_json, input_projection_hash,
                        input_projection_version, group_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        decision_group.decision_group_id,
                        decision_group.game_session_id,
                        decision_group.observation_id,
                        self._dump(list(decision_group.decision_gap_ids)),
                        decision_group.input_projection_hash,
                        decision_group.input_projection_version,
                        decision_group.model_dump_json(),
                        decision_group.created_at.isoformat(),
                    ),
                )
            if planner_request is not None:
                if planner_request.game_session_id != tick.game_session_id:
                    raise ValueError("planner request and Tick must belong to one game")
                self._save_planner_request_in_connection(conn, planner_request)
            for provider_attempt in provider_attempts:
                self._save_provider_attempt_in_connection(
                    conn, tick.game_session_id, provider_attempt
                )
            if information_round is not None:
                self._save_information_round_in_connection(
                    conn, tick.game_session_id, information_round
                )
            for lease in plan_leases:
                if lease.game_session_id != tick.game_session_id:
                    raise ValueError("plan lease and Tick must belong to one game")
                self._save_plan_lease_in_connection(conn, lease)
                if lease.status in {
                    PlanLeaseStatus.COMPLETED,
                    PlanLeaseStatus.EXPIRED,
                    PlanLeaseStatus.INVALIDATED,
                }:
                    self._invalidate_plan_projection_in_connection(conn, lease)
            for task_id in cancel_task_ids:
                conn.execute(
                    """
                    UPDATE workflow_tasks
                    SET status=?, last_error=?, updated_at=CURRENT_TIMESTAMP
                    WHERE game_id=? AND task_id=?
                      AND status IN (?, ?, ?)
                    """,
                    (
                        TaskStatus.CANCELLED.value,
                        "dependent plan lease is no longer executable",
                        tick.game_session_id,
                        task_id,
                        TaskStatus.PENDING.value,
                        TaskStatus.READY.value,
                        TaskStatus.AWAITING_CONFIRMATION.value,
                    ),
                )
            self._save_runtime_state_in_connection(
                conn,
                tick.game_session_id,
                tick.ending_runtime_state,
                active_attempt_id,
            )
            self._persist_human_wait_context_in_connection(
                conn,
                tick.game_session_id,
                tick.ending_runtime_state,
                human_wait_context,
            )
            self._insert_workflow_tick_in_connection(conn, tick)

    def planner_metrics(self, game_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            logical = int(
                conn.execute(
                    """
                SELECT COUNT(*) AS value FROM logical_planner_requests
                WHERE game_id=?
                """,
                    (game_id,),
                ).fetchone()["value"]
            )
            provider = int(
                conn.execute(
                    """
                SELECT COUNT(*) AS value FROM provider_attempts
                WHERE game_id=?
                """,
                    (game_id,),
                ).fetchone()["value"]
            )
            information = int(
                conn.execute(
                    """
                SELECT COUNT(*) AS value FROM information_rounds
                WHERE game_id=?
                """,
                    (game_id,),
                ).fetchone()["value"]
            )
            suppressed = int(
                conn.execute(
                    """
                SELECT COUNT(*) AS value FROM planner_suppressions
                WHERE game_id=?
                """,
                    (game_id,),
                ).fetchone()["value"]
            )
            turn_rows = conn.execute(
                """
                SELECT turn,
                       SUM(CASE WHEN outcome IN (
                           'LOGICAL_PLANNER_REQUEST_CREATED',
                           'PLANNER_ATTEMPT_COMPLETED',
                           'INFORMATION_REQUESTED',
                           'INFORMATION_COLLECTED'
                       ) THEN 1 ELSE 0 END) AS planner_ticks
                FROM workflow_ticks WHERE game_id=? GROUP BY turn
                """,
                (game_id,),
            ).fetchall()
        total_turns = len(turn_rows)
        zero_turns = sum(int(row["planner_ticks"]) == 0 for row in turn_rows)
        return {
            "logical_requests": logical,
            "provider_attempts": provider,
            "information_rounds": information,
            "duplicate_request_suppressions": suppressed,
            "zero_planner_turn_ratio": (
                1.0 if total_turns == 0 else zero_turns / total_turns
            ),
        }

    def agent_called_for_turn(self, game_id: str, turn: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1 FROM agent_runs
                WHERE game_id=? AND turn=? AND success=1
                LIMIT 1
                """,
                (game_id, turn),
            ).fetchone()
        return row is not None

    def record_agent_run(
        self,
        game_id: str,
        request: AgentRequest,
        *,
        response: PlanBundle | None,
        success: bool,
        error: str | None,
        duration_seconds: float,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO agent_runs(
                    game_id, turn, request_id, request_json, response_json,
                    success, error, duration_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    game_id,
                    request.turn,
                    request.request_id,
                    request.model_dump_json(),
                    None if response is None else response.model_dump_json(),
                    int(success),
                    error,
                    duration_seconds,
                ),
            )

    def record_metrics(
        self,
        game_id: str,
        turn: int,
        metrics: TickMetrics,
        *,
        tick_id: str | None = None,
    ) -> str:
        metric_tick_id = tick_id or f"metric_{uuid4().hex}"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO turn_metrics(tick_id, game_id, turn, metrics_json)
                VALUES (?, ?, ?, ?)
                """,
                (metric_tick_id, game_id, turn, metrics.model_dump_json()),
            )
        return metric_tick_id

    def export_replay_state(self, game_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            tables = {
                table: [
                    dict(row)
                    for row in conn.execute(
                        f"SELECT * FROM {table} WHERE game_id=?", (game_id,)
                    ).fetchall()
                ]
                for table in REPLAY_STATE_TABLES
            }
            meta_keys = (
                "last_game_id",
                "last_observed_turn",
                f"unit_observations_initialized:{game_id}",
            )
            placeholders = ",".join("?" for _ in meta_keys)
            tables["workflow_meta"] = [
                dict(row)
                for row in conn.execute(
                    f"SELECT * FROM workflow_meta WHERE key IN ({placeholders})",
                    meta_keys,
                ).fetchall()
            ]
        return {"game_id": game_id, "tables": tables}

    def import_replay_state(self, state: dict[str, Any]) -> None:
        tables = state.get("tables")
        if not isinstance(tables, dict):
            raise ValueError("replay store state must contain a tables object")
        game_id = state.get("game_id")
        if not isinstance(game_id, str) or not game_id:
            raise ValueError("replay store state must contain a game_id")
        allowed_tables = {*REPLAY_STATE_TABLES, "workflow_meta"}
        unknown_tables = set(tables) - allowed_tables
        if unknown_tables:
            raise ValueError(f"invalid replay state tables: {sorted(unknown_tables)}")
        with self._connect() as conn:
            for table in reversed(REPLAY_STATE_TABLES):
                conn.execute(f"DELETE FROM {table} WHERE game_id=?", (game_id,))
            conn.execute(
                """
                DELETE FROM workflow_meta
                WHERE key IN (?, ?, ?)
                """,
                (
                    "last_game_id",
                    "last_observed_turn",
                    f"unit_observations_initialized:{game_id}",
                ),
            )
            for table in (*REPLAY_STATE_TABLES, "workflow_meta"):
                rows = tables.get(table, [])
                if not isinstance(rows, list):
                    raise ValueError(f"invalid replay state table: {table!r}")
                known_columns = {
                    str(row["name"])
                    for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
                }
                for row in rows:
                    if not isinstance(row, dict) or not row:
                        raise ValueError(f"invalid replay row for {table}")
                    unknown = set(row) - known_columns
                    if unknown:
                        raise ValueError(
                            f"unknown replay columns for {table}: {sorted(unknown)}"
                        )
                    columns = sorted(row)
                    placeholders = ",".join("?" for _ in columns)
                    column_sql = ",".join(columns)
                    conn.execute(
                        f"INSERT OR REPLACE INTO {table} ({column_sql}) "
                        f"VALUES ({placeholders})",
                        tuple(row[column] for column in columns),
                    )
