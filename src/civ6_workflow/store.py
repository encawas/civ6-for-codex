from __future__ import annotations

import json
import hashlib
import sqlite3
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence
from uuid import uuid4

from .action_retry import FailedAttemptResolution, resolve_failed_attempt
from .domain import (
    ActionAttempt,
    ApprovalDecision,
    ApprovalRecord,
    ApprovalStatus,
    AwaitingHumanTick,
    AttemptReconciledTick,
    AttemptRecoveredTick,
    AttemptStatus,
    DecisionGap,
    DecisionGapStatus,
    DecisionGroup,
    InformationCollectedTick,
    InformationRound,
    InformationRoundStatus,
    InformationRequestedTick,
    LogicalPlannerRequestCreatedTick,
    PlanLease,
    PlanLeaseStatus,
    PlannerAttemptCompletedTick,
    PlannerBackoffTick,
    PlannerRequest,
    PlannerResponseEvidenceCompatibility,
    PlannerRequestStatus,
    PlannerRequestTarget,
    PlannerRequestTargetKind,
    ProviderAttempt,
    StrategicContract,
    STRATEGIC_PROPOSAL_TARGET_KINDS,
    StrategicProposalWaitResumeRequest,
    StrategicResearchProposal,
    StrategicProposalReadyTick,
    StrategicRequestTerminatedTick,
    StrategicRequestWaitErrorTick,
    StrategicRequestWaitResumedTick,
    StrategicProposalWaitErrorTick,
    StrategicProposalWaitResumedTick,
    build_strategic_proposal_wait_resume_request,
    build_strategic_contract_id,
    StrategicContractCommit,
    RuntimeState,
    TickOutcomeKind,
    TurnTransitionConfirmedTick,
    WorkflowTick,
    validate_workflow_tick,
    validate_workflow_tick_json,
    ProviderAttemptStatus,
    canonical_json,
    canonical_json_hash,
)
from .domain.planner import TERMINAL_PLANNER_STATUSES
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
from .workflow_protocol import (
    canonical_strategic_research_proposal_response_payload,
    canonical_workflow_plan_bundle_payload,
)


_STICKY_EVENT_TYPES = {
    "planned_task_blocked",
    "planned_task_failed",
    "action_commit_uncertain",
    "turn_rewind_detected",
}


class TaskIdentityConflictError(ValueError):
    """Raised when an existing task ID is reused for different semantics."""


class StaleStrategicContractBaseError(ValueError):
    """Raised when Proposal persistence loses its frozen Contract base."""


class _PlannerResponseFacts(StrEnum):
    CANONICAL_RESPONSE = "CANONICAL_RESPONSE"
    CONTRACT_SCHEMA_FAILURE = "CONTRACT_SCHEMA_FAILURE"
    LEGACY_V7_MISSING_PAYLOAD = "LEGACY_V7_MISSING_PAYLOAD"
    NO_RESPONSE = "NO_RESPONSE"


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS workflow_meta (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS strategic_contract_roots (
    game_id TEXT PRIMARY KEY,
    contract_id TEXT NOT NULL UNIQUE,
    active_revision INTEGER NOT NULL CHECK (active_revision >= 1),
    created_at TEXT NOT NULL,
    UNIQUE (game_id, contract_id)
);

CREATE TABLE IF NOT EXISTS strategic_contract_revisions (
    game_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    contract_json TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (game_id, revision),
    UNIQUE (contract_id, revision),
    FOREIGN KEY (game_id, contract_id)
        REFERENCES strategic_contract_roots(game_id, contract_id)
);

CREATE TABLE IF NOT EXISTS strategic_contract_commits (
    commit_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    expected_base_revision INTEGER NOT NULL CHECK (expected_base_revision >= 0),
    committed_revision INTEGER NOT NULL CHECK (committed_revision >= 1),
    commit_json TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    UNIQUE (game_id, committed_revision),
    FOREIGN KEY (game_id, contract_id)
        REFERENCES strategic_contract_roots(game_id, contract_id),
    FOREIGN KEY (game_id, committed_revision)
        REFERENCES strategic_contract_revisions(game_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_contract_revisions_identity
ON strategic_contract_revisions (contract_id, revision);

CREATE INDEX IF NOT EXISTS idx_contract_commits_game_revision
ON strategic_contract_commits (game_id, committed_revision);

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
    resolved_turn INTEGER,
    resolved_by TEXT,
    resolution_task_id TEXT,
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
    request_target_kind TEXT NOT NULL,
    request_target_key TEXT NOT NULL,
    decision_group_id TEXT,
    turn INTEGER NOT NULL,
    status TEXT NOT NULL,
    input_projection_hash TEXT NOT NULL,
    input_projection_version TEXT NOT NULL,
    decision_gap_ids_json TEXT NOT NULL,
    request_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
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

CREATE TABLE IF NOT EXISTS strategic_research_proposals (
    proposal_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    source_planner_request_id TEXT NOT NULL UNIQUE,
    source_provider_attempt_id TEXT NOT NULL UNIQUE,
    source_provider_attempt_number INTEGER NOT NULL
        CHECK (source_provider_attempt_number >= 1),
    target_kind TEXT NOT NULL,
    target_contract_id TEXT NOT NULL,
    expected_base_revision INTEGER NOT NULL CHECK (expected_base_revision >= 0),
    proposal_hash TEXT NOT NULL,
    proposal_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (source_planner_request_id)
        REFERENCES logical_planner_requests(planner_request_id),
    FOREIGN KEY (source_provider_attempt_id)
        REFERENCES provider_attempts(provider_attempt_id)
);

CREATE INDEX IF NOT EXISTS idx_strategic_research_proposals_game
ON strategic_research_proposals (game_id, target_kind, target_contract_id);

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

CREATE TABLE IF NOT EXISTS strategic_proposal_wait_resume_requests (
    resume_request_id TEXT PRIMARY KEY,
    game_id TEXT NOT NULL,
    proposal_id TEXT NOT NULL UNIQUE,
    planner_request_id TEXT NOT NULL,
    proposal_ready_tick_id TEXT NOT NULL UNIQUE,
    target_kind TEXT NOT NULL,
    expected_base_revision INTEGER NOT NULL CHECK (expected_base_revision >= 0),
    request_json TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    FOREIGN KEY (proposal_id)
        REFERENCES strategic_research_proposals(proposal_id),
    FOREIGN KEY (planner_request_id)
        REFERENCES logical_planner_requests(planner_request_id),
    FOREIGN KEY (proposal_ready_tick_id)
        REFERENCES workflow_ticks(tick_id)
);
"""

REPLAY_STATE_TABLES = (
    "strategic_contract_roots",
    "strategic_contract_revisions",
    "strategic_contract_commits",
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
    "strategic_research_proposals",
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
    "strategic_proposal_wait_resume_requests",
)


class WorkflowStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            if version > 10:
                raise ValueError(
                    f"unsupported workflow database version {version}; "
                    "maximum supported version is 10"
                )
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

        event_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(event_log)").fetchall()
        }
        event_additions = {
            "resolved_turn": "INTEGER",
            "resolved_by": "TEXT",
            "resolution_task_id": "TEXT",
        }
        for name, declaration in event_additions.items():
            if name not in event_columns:
                conn.execute(f"ALTER TABLE event_log ADD COLUMN {name} {declaration}")

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
        if version > 10:
            raise ValueError(
                f"unsupported workflow database version {version}; "
                "maximum supported version is 10"
            )
        upgraded_from_pre_v7 = version < 7
        if upgraded_from_pre_v7:
            WorkflowStore._migrate_phase4_v7(conn)
            conn.execute("PRAGMA user_version=7")
            version = 7
        if version < 8:
            conn.commit()
            conn.execute("PRAGMA foreign_keys=OFF")
            try:
                conn.execute("BEGIN IMMEDIATE")
                WorkflowStore._migrate_phase1a_v8(
                    conn,
                    validate_existing_target_columns=not upgraded_from_pre_v7,
                )
                conn.execute("PRAGMA user_version=8")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute("PRAGMA foreign_keys=ON")
        else:
            WorkflowStore._validate_phase1a_v8(conn)

        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version < 9:
            WorkflowStore._migrate_phase1b_v9(conn)
            conn.execute("PRAGMA user_version=9")
        else:
            WorkflowStore._validate_phase1b_v9(conn)

        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version < 10:
            WorkflowStore._migrate_phase1b_v10(conn)
            conn.execute("PRAGMA user_version=10")
        else:
            WorkflowStore._validate_phase1b_proposals_v10(conn)

    @classmethod
    def _migrate_phase1b_v9(cls, conn: sqlite3.Connection) -> None:
        """Add the empty Contract aggregate tables without changing legacy authority."""

        cls._validate_phase1b_v9(conn)

    @classmethod
    def _validate_phase1b_v9(cls, conn: sqlite3.Connection) -> None:
        roots = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM strategic_contract_roots ORDER BY game_id"
            ).fetchall()
        ]
        revisions = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM strategic_contract_revisions ORDER BY game_id, revision"
            ).fetchall()
        ]
        commits = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM strategic_contract_commits "
                "ORDER BY game_id, committed_revision"
            ).fetchall()
        ]
        cls._validate_strategic_contract_state(
            roots,
            revisions,
            commits,
            require_canonical=True,
        )

    @classmethod
    def _migrate_phase1b_v10(cls, conn: sqlite3.Connection) -> None:
        """Add candidate Proposal persistence without changing Contract authority."""

        cls._validate_phase1b_proposals_v10(conn)

    @classmethod
    def _normalize_contract_root_row(cls, row: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        required = {"game_id", "contract_id", "active_revision", "created_at"}
        missing = required - normalized.keys()
        if missing:
            raise ValueError(
                f"StrategicContract root row is missing columns: {sorted(missing)}"
            )
        if not str(normalized["game_id"]) or not str(normalized["contract_id"]):
            raise ValueError("StrategicContract root identities must be non-empty")
        active_revision = int(normalized["active_revision"])
        if active_revision < 1:
            raise ValueError("StrategicContract active revision must be positive")
        created_at = cls._parse_audit_datetime(
            normalized["created_at"], "StrategicContract root created_at"
        )
        if created_at is None or created_at.tzinfo is None:
            raise ValueError(
                "StrategicContract root created_at must include a timezone"
            )
        normalized["active_revision"] = active_revision
        normalized["created_at"] = created_at.isoformat()
        return normalized

    @classmethod
    def _normalize_contract_revision_row(cls, row: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        required = {
            "game_id",
            "contract_id",
            "revision",
            "contract_json",
            "committed_at",
        }
        missing = required - normalized.keys()
        if missing:
            raise ValueError(
                f"StrategicContract revision row is missing columns: {sorted(missing)}"
            )
        try:
            contract = StrategicContract.model_validate_json(
                str(normalized["contract_json"])
            )
        except Exception as exc:
            raise ValueError("invalid StrategicContract revision JSON") from exc
        revision = int(normalized["revision"])
        if (
            contract.game_session_id != str(normalized["game_id"])
            or contract.contract_id != str(normalized["contract_id"])
            or contract.revision != revision
        ):
            raise ValueError(
                "StrategicContract revision relational columns disagree with JSON"
            )
        committed_at = cls._parse_audit_datetime(
            normalized["committed_at"], "StrategicContract revision committed_at"
        )
        if committed_at is None or committed_at.tzinfo is None:
            raise ValueError(
                "StrategicContract revision committed_at must include a timezone"
            )
        normalized["revision"] = revision
        normalized["contract_json"] = cls._dump(contract.model_dump(mode="json"))
        normalized["committed_at"] = committed_at.isoformat()
        return normalized

    @classmethod
    def _normalize_contract_commit_row(cls, row: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        required = {
            "commit_id",
            "game_id",
            "contract_id",
            "expected_base_revision",
            "committed_revision",
            "commit_json",
            "committed_at",
        }
        missing = required - normalized.keys()
        if missing:
            raise ValueError(
                f"StrategicContract commit row is missing columns: {sorted(missing)}"
            )
        try:
            commit = StrategicContractCommit.model_validate_json(
                str(normalized["commit_json"])
            )
        except Exception as exc:
            raise ValueError("invalid StrategicContract commit JSON") from exc
        expected_base = int(normalized["expected_base_revision"])
        committed_revision = int(normalized["committed_revision"])
        committed_at = cls._parse_audit_datetime(
            normalized["committed_at"], "StrategicContract commit committed_at"
        )
        if committed_at is None or committed_at.tzinfo is None:
            raise ValueError(
                "StrategicContract commit committed_at must include a timezone"
            )
        if (
            commit.commit_id != str(normalized["commit_id"])
            or commit.game_session_id != str(normalized["game_id"])
            or commit.contract_id != str(normalized["contract_id"])
            or commit.expected_base_revision != expected_base
            or commit.contract.revision != committed_revision
            or commit.committed_at != committed_at
        ):
            raise ValueError(
                "StrategicContract commit relational columns disagree with JSON"
            )
        normalized["expected_base_revision"] = expected_base
        normalized["committed_revision"] = committed_revision
        normalized["commit_json"] = cls._dump(commit.model_dump(mode="json"))
        normalized["committed_at"] = committed_at.isoformat()
        return normalized

    @classmethod
    def _validate_strategic_contract_state(
        cls,
        roots: Sequence[Mapping[str, Any]],
        revisions: Sequence[Mapping[str, Any]],
        commits: Sequence[Mapping[str, Any]],
        *,
        require_canonical: bool,
    ) -> None:
        normalized_roots = [cls._normalize_contract_root_row(row) for row in roots]
        normalized_revisions = [
            cls._normalize_contract_revision_row(row) for row in revisions
        ]
        normalized_commits = [
            cls._normalize_contract_commit_row(row) for row in commits
        ]
        if require_canonical:
            for source, normalized, record_type in (
                *(
                    (dict(row), normalized_roots[index], "root")
                    for index, row in enumerate(roots)
                ),
                *(
                    (dict(row), normalized_revisions[index], "revision")
                    for index, row in enumerate(revisions)
                ),
                *(
                    (dict(row), normalized_commits[index], "commit")
                    for index, row in enumerate(commits)
                ),
            ):
                if source != normalized:
                    raise ValueError(
                        f"StrategicContract {record_type} row is not canonical"
                    )

        roots_by_game: dict[str, dict[str, Any]] = {}
        contract_games: dict[str, str] = {}
        for root in normalized_roots:
            game_id = str(root["game_id"])
            contract_id = str(root["contract_id"])
            if game_id in roots_by_game:
                raise ValueError("a game session cannot have two Contract roots")
            prior_game = contract_games.setdefault(contract_id, game_id)
            if prior_game != game_id:
                raise ValueError("StrategicContract identity belongs to another game")
            roots_by_game[game_id] = root

        revisions_by_game: dict[str, dict[int, StrategicContract]] = {
            game_id: {} for game_id in roots_by_game
        }
        revision_rows: dict[tuple[str, int], dict[str, Any]] = {}
        for row in normalized_revisions:
            game_id = str(row["game_id"])
            revision = int(row["revision"])
            root = roots_by_game.get(game_id)
            if root is None:
                raise ValueError("StrategicContract revision has no Contract root")
            if str(row["contract_id"]) != str(root["contract_id"]):
                raise ValueError(
                    "StrategicContract revision uses another root identity"
                )
            key = (game_id, revision)
            if key in revision_rows:
                raise ValueError("duplicate StrategicContract revision")
            revision_rows[key] = row
            contract = StrategicContract.model_validate_json(str(row["contract_json"]))
            cls._validate_phase1b_contract_foundation(contract)
            revisions_by_game[game_id][revision] = contract

        commits_by_revision: dict[tuple[str, int], StrategicContractCommit] = {}
        commit_ids: dict[str, str] = {}
        for row in normalized_commits:
            game_id = str(row["game_id"])
            revision = int(row["committed_revision"])
            root = roots_by_game.get(game_id)
            if root is None:
                raise ValueError("StrategicContract commit has no Contract root")
            if str(row["contract_id"]) != str(root["contract_id"]):
                raise ValueError("StrategicContract commit uses another root identity")
            commit_id = str(row["commit_id"])
            prior_game = commit_ids.setdefault(commit_id, game_id)
            if prior_game != game_id:
                raise ValueError(
                    "StrategicContract commit identity belongs to another game"
                )
            key = (game_id, revision)
            if key in commits_by_revision:
                raise ValueError("duplicate StrategicContract commit revision")
            commits_by_revision[key] = StrategicContractCommit.model_validate_json(
                str(row["commit_json"])
            )

        for game_id, root in roots_by_game.items():
            active_revision = int(root["active_revision"])
            history = revisions_by_game[game_id]
            expected_revisions = list(range(1, active_revision + 1))
            if sorted(history) != expected_revisions:
                raise ValueError(
                    "StrategicContract revisions must be contiguous through active revision"
                )
            first_commit = commits_by_revision.get((game_id, 1))
            if first_commit is None:
                raise ValueError("StrategicContract revision has no commit audit")
            root_created_at = cls._parse_audit_datetime(
                root["created_at"], "StrategicContract root created_at"
            )
            if root_created_at != first_commit.committed_at:
                raise ValueError(
                    "StrategicContract root creation time disagrees with revision 1"
                )

            for revision in expected_revisions:
                contract = history[revision]
                commit = commits_by_revision.get((game_id, revision))
                if commit is None:
                    raise ValueError("StrategicContract revision has no commit audit")
                if commit.expected_base_revision != revision - 1:
                    raise ValueError(
                        "StrategicContract commit base revision is not contiguous"
                    )
                if commit.contract != contract:
                    raise ValueError(
                        "StrategicContract commit audit disagrees with revision snapshot"
                    )
                committed_at = cls._parse_audit_datetime(
                    revision_rows[(game_id, revision)]["committed_at"],
                    "StrategicContract revision committed_at",
                )
                if committed_at != commit.committed_at:
                    raise ValueError(
                        "StrategicContract revision and commit timestamps disagree"
                    )

        if len(commits_by_revision) != len(revision_rows):
            raise ValueError("StrategicContract commit audit has no matching revision")

    @staticmethod
    def _validate_phase1b_contract_foundation(contract: StrategicContract) -> None:
        if (
            contract.authority_scope_set.mission_graph_scopes
            or contract.mission_graph.missions
        ):
            raise ValueError(
                "v9 StrategicContract foundation requires empty authority scope and "
                "MissionGraph"
            )

    @classmethod
    def _normalize_strategic_research_proposal_row(
        cls, row: Mapping[str, Any]
    ) -> dict[str, Any]:
        normalized = dict(row)
        required = {
            "proposal_id",
            "game_id",
            "source_planner_request_id",
            "source_provider_attempt_id",
            "source_provider_attempt_number",
            "target_kind",
            "target_contract_id",
            "expected_base_revision",
            "proposal_hash",
            "proposal_json",
            "created_at",
        }
        missing = required - normalized.keys()
        if missing:
            raise ValueError(
                f"StrategicResearchProposal row is missing columns: {sorted(missing)}"
            )
        try:
            proposal = StrategicResearchProposal.model_validate_json(
                str(normalized["proposal_json"])
            )
        except Exception as exc:
            raise ValueError("invalid StrategicResearchProposal JSON") from exc
        created_at = cls._parse_audit_datetime(
            normalized["created_at"], "StrategicResearchProposal created_at"
        )
        if created_at is None or created_at.tzinfo is None:
            raise ValueError("StrategicResearchProposal created_at requires timezone")
        relational = {
            "proposal_id": proposal.proposal_id,
            "game_id": proposal.game_session_id,
            "source_planner_request_id": proposal.source_planner_request_id,
            "source_provider_attempt_id": proposal.source_provider_attempt_id,
            "source_provider_attempt_number": proposal.source_provider_attempt_number,
            "target_kind": proposal.target_kind.value,
            "target_contract_id": proposal.target_contract_id,
            "expected_base_revision": proposal.expected_base_revision,
            "proposal_hash": proposal.proposal_hash,
            "created_at": proposal.created_at,
        }
        for column, expected in relational.items():
            actual = created_at if column == "created_at" else normalized[column]
            if column in {
                "source_provider_attempt_number",
                "expected_base_revision",
            }:
                try:
                    actual = int(actual)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"StrategicResearchProposal {column} must be an integer"
                    ) from exc
            if actual != expected:
                raise ValueError(
                    "StrategicResearchProposal relational columns disagree with JSON"
                )
        normalized.update(
            {
                "source_provider_attempt_number": proposal.source_provider_attempt_number,
                "expected_base_revision": proposal.expected_base_revision,
                "proposal_json": cls._dump(proposal.model_dump(mode="json")),
                "created_at": proposal.created_at.isoformat(),
            }
        )
        return normalized

    @classmethod
    def _normalize_strategic_proposal_wait_resume_request_row(
        cls, row: Mapping[str, Any]
    ) -> dict[str, Any]:
        normalized = dict(row)
        required = {
            "resume_request_id",
            "game_id",
            "proposal_id",
            "planner_request_id",
            "proposal_ready_tick_id",
            "target_kind",
            "expected_base_revision",
            "request_json",
            "requested_at",
        }
        missing = required - normalized.keys()
        if missing:
            raise ValueError(
                "Strategic Proposal Resume Request row is missing columns: "
                f"{sorted(missing)}"
            )
        try:
            request = StrategicProposalWaitResumeRequest.model_validate_json(
                str(normalized["request_json"])
            )
        except Exception as exc:
            raise ValueError("invalid Strategic Proposal Resume Request JSON") from exc
        requested_at = cls._parse_audit_datetime(
            normalized["requested_at"],
            "Strategic Proposal Resume Request requested_at",
        )
        if requested_at is None or requested_at.tzinfo is None:
            raise ValueError("Strategic Proposal Resume Request requires timezone")
        relational = {
            "resume_request_id": request.resume_request_id,
            "game_id": request.game_session_id,
            "proposal_id": request.proposal_id,
            "planner_request_id": request.planner_request_id,
            "proposal_ready_tick_id": request.proposal_ready_tick_id,
            "target_kind": request.target_kind.value,
            "expected_base_revision": request.expected_base_revision,
            "requested_at": request.requested_at,
        }
        for column, expected in relational.items():
            actual = requested_at if column == "requested_at" else normalized[column]
            if column == "expected_base_revision":
                try:
                    actual = int(actual)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "Strategic Proposal Resume Request base must be an integer"
                    ) from exc
            if actual != expected:
                raise ValueError(
                    "Strategic Proposal Resume Request columns disagree with JSON"
                )
        normalized.update(
            {
                "expected_base_revision": request.expected_base_revision,
                "request_json": cls._dump(request.model_dump(mode="json")),
                "requested_at": request.requested_at.isoformat(),
            }
        )
        return normalized

    @classmethod
    def _validate_strategic_proposal_attempt(
        cls,
        proposal: StrategicResearchProposal,
        request: PlannerRequest,
        attempt_rows: Sequence[Mapping[str, Any]],
    ) -> None:
        normalized_attempts = [
            cls._normalize_provider_attempt_row(row) for row in attempt_rows
        ]
        matching = [
            row
            for row in normalized_attempts
            if str(row["planner_request_id"]) == request.planner_request_id
        ]
        if not matching:
            raise ValueError("Proposal requires a matching final ProviderAttempt")
        maximum = max(int(row["attempt_number"]) for row in matching)
        if request.provider_attempt_count != maximum:
            raise ValueError(
                "Proposal requires provider_attempt_count to match the maximum Attempt"
            )
        if proposal.source_provider_attempt_number != maximum:
            raise ValueError("Proposal must bind the maximum ProviderAttempt number")
        final_row = next(
            row for row in matching if int(row["attempt_number"]) == maximum
        )
        if final_row["game_id"] != proposal.game_session_id:
            raise ValueError("Proposal final ProviderAttempt belongs to another game")
        final_attempt = ProviderAttempt.model_validate_json(
            str(final_row["attempt_json"])
        )
        if final_attempt.provider_attempt_id != proposal.source_provider_attempt_id:
            raise ValueError("Proposal must bind the final ProviderAttempt identity")
        if (
            final_attempt.status is not ProviderAttemptStatus.SUCCEEDED
            or final_attempt.completed_at is None
        ):
            raise ValueError("Proposal requires a completed SUCCEEDED final Attempt")

    @staticmethod
    def _validate_strategic_proposal_target(
        proposal: StrategicResearchProposal, request: PlannerRequest
    ) -> None:
        target = request.target
        if target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS:
            raise ValueError("Proposal parent Request target is not supported")
        if proposal.target_kind is not target.kind:
            raise ValueError("Proposal target kind disagrees with PlannerRequest")
        if target.strategic_scope != "research":
            raise ValueError("Strategic research Proposal requires research scope")
        if target.kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION:
            contract_id = target.strategic_contract_id or build_strategic_contract_id(
                request.game_session_id
            )
            base_revision = 0
        else:
            assert target.strategic_contract_id is not None
            assert target.base_contract_revision is not None
            contract_id = target.strategic_contract_id
            base_revision = target.base_contract_revision
        if proposal.target_contract_id != contract_id:
            raise ValueError("Proposal Contract ID disagrees with PlannerRequest")
        if proposal.expected_base_revision != base_revision:
            raise ValueError("Proposal base revision disagrees with PlannerRequest")

    @classmethod
    def _validate_strategic_proposal_state(
        cls,
        proposal_rows: Sequence[Mapping[str, Any]],
        request_rows: Sequence[Mapping[str, Any]],
        attempt_rows: Sequence[Mapping[str, Any]],
        *,
        require_canonical: bool,
    ) -> None:
        normalized_proposals = [
            cls._normalize_strategic_research_proposal_row(row) for row in proposal_rows
        ]
        normalized_requests = [
            cls._normalize_planner_request_row(row, validate_response_contract=True)
            for row in request_rows
        ]
        normalized_attempts = [
            cls._normalize_provider_attempt_row(row) for row in attempt_rows
        ]
        if require_canonical:
            for index, row in enumerate(proposal_rows):
                source = dict(row)
                normalized = normalized_proposals[index]
                if source != normalized:
                    raise ValueError("StrategicResearchProposal row is not canonical")

        requests: dict[str, PlannerRequest] = {}
        for row in normalized_requests:
            request = PlannerRequest.model_validate_json(str(row["request_json"]))
            if request.planner_request_id in requests:
                raise ValueError("duplicate PlannerRequest identity")
            requests[request.planner_request_id] = request

        proposals_by_request: dict[str, StrategicResearchProposal] = {}
        proposal_ids: set[str] = set()
        attempt_ids: set[str] = set()
        for row in normalized_proposals:
            proposal = StrategicResearchProposal.model_validate_json(
                str(row["proposal_json"])
            )
            if proposal.proposal_id in proposal_ids:
                raise ValueError("duplicate StrategicResearchProposal identity")
            proposal_ids.add(proposal.proposal_id)
            if proposal.source_planner_request_id in proposals_by_request:
                raise ValueError("a PlannerRequest cannot have two Proposals")
            if proposal.source_provider_attempt_id in attempt_ids:
                raise ValueError("a ProviderAttempt cannot support two Proposals")
            attempt_ids.add(proposal.source_provider_attempt_id)
            proposals_by_request[proposal.source_planner_request_id] = proposal

            request = requests.get(proposal.source_planner_request_id)
            if request is None:
                raise ValueError("Proposal parent PlannerRequest does not exist")
            if request.game_session_id != proposal.game_session_id:
                raise ValueError(
                    "Proposal and PlannerRequest belong to different games"
                )
            if request.status is not PlannerRequestStatus.COMPLETED:
                raise ValueError("Proposal parent PlannerRequest must be COMPLETED")
            if proposal.created_from_observation_id != request.observation_id:
                raise ValueError(
                    "Proposal observation identity disagrees with PlannerRequest"
                )
            validation = request.validation_result
            if (
                validation is None
                or validation.get("proposal_id") != proposal.proposal_id
                or validation.get("proposal_hash") != proposal.proposal_hash
            ):
                raise ValueError(
                    "Proposal identity disagrees with PlannerRequest validation result"
                )
            projection_context = request.input_projection.get(
                "strategic_proposal_context", request.input_projection
            )
            if not isinstance(projection_context, Mapping):
                raise ValueError("Proposal PlannerRequest input projection is missing")
            expected_projection = {
                "target_contract_id": proposal.target_contract_id,
                "expected_base_revision": proposal.expected_base_revision,
                "strategic_scope": "research",
            }
            if any(
                projection_context.get(key) != value
                for key, value in expected_projection.items()
            ):
                raise ValueError(
                    "Proposal identity disagrees with frozen input projection"
                )
            cls._validate_strategic_proposal_target(proposal, request)
            cls._validate_strategic_proposal_attempt(
                proposal, request, normalized_attempts
            )

        for request in requests.values():
            if request.target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS:
                continue
            proposal = proposals_by_request.get(request.planner_request_id)
            if request.status is PlannerRequestStatus.COMPLETED and proposal is None:
                raise ValueError(
                    "COMPLETED non-legacy PlannerRequest requires Proposal"
                )
            if (
                request.status is not PlannerRequestStatus.COMPLETED
                and proposal is not None
            ):
                raise ValueError("non-COMPLETED PlannerRequest cannot own Proposal")

    @classmethod
    def _workflow_tick_from_row(cls, row: Mapping[str, Any]) -> WorkflowTick:
        required = {
            "tick_id",
            "game_id",
            "turn",
            "outcome",
            "starting_runtime_state",
            "ending_runtime_state",
            "observation_ids_json",
            "mutation_budget_used",
            "planner_request_id",
            "started_at",
            "completed_at",
            "tick_json",
        }
        missing = required - row.keys()
        if missing:
            raise ValueError(f"WorkflowTick row is missing columns: {sorted(missing)}")
        try:
            tick = validate_workflow_tick_json(str(row["tick_json"]))
        except Exception as exc:
            raise ValueError("invalid WorkflowTick JSON") from exc
        checks = {
            "tick_id": tick.tick_id,
            "game_id": tick.game_session_id,
            "turn": tick.turn_number,
            "outcome": tick.outcome.value,
            "starting_runtime_state": tick.starting_runtime_state.value,
            "ending_runtime_state": tick.ending_runtime_state.value,
            "mutation_budget_used": tick.mutation_budget_used,
            "planner_request_id": getattr(tick, "planner_request_id", None),
        }
        for column, expected in checks.items():
            actual = row[column]
            if column in {"turn", "mutation_budget_used"}:
                try:
                    actual = int(actual)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"WorkflowTick {column} must be an integer"
                    ) from exc
            elif actual is not None:
                actual = str(actual)
            if actual != expected:
                raise ValueError(f"WorkflowTick {column} conflicts with tick_json")
        try:
            observation_ids = tuple(cls._load(str(row["observation_ids_json"])))
        except Exception as exc:
            raise ValueError("invalid WorkflowTick observation IDs") from exc
        if observation_ids != tick.observation_ids:
            raise ValueError("WorkflowTick observation IDs conflict with tick_json")
        for column, expected in (
            ("started_at", tick.started_at),
            ("completed_at", tick.completed_at),
        ):
            if cls._parse_audit_datetime(row[column], column) != expected:
                raise ValueError(f"WorkflowTick {column} conflicts with tick_json")
        return tick

    @classmethod
    def _validate_strategic_information_round_state(
        cls,
        requests: Mapping[str, PlannerRequest],
        attempt_rows: Sequence[Mapping[str, Any]],
        round_rows: Sequence[Mapping[str, Any]],
        tick_rows: Sequence[Mapping[str, Any]],
        *,
        require_canonical: bool,
    ) -> None:
        attempts_by_request: dict[str, list[tuple[str, ProviderAttempt]]] = {}
        for source_row in attempt_rows:
            normalized = cls._normalize_provider_attempt_row(source_row)
            parent = requests.get(str(normalized["planner_request_id"]))
            if (
                parent is None
                or parent.target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS
            ):
                continue
            game_id = str(normalized["game_id"])
            if game_id != parent.game_session_id:
                raise ValueError(
                    "strategic ProviderAttempt game disagrees with PlannerRequest"
                )
            attempt = ProviderAttempt.model_validate_json(
                str(normalized["attempt_json"])
            )
            attempts_by_request.setdefault(parent.planner_request_id, []).append(
                (game_id, attempt)
            )

        rounds_by_request: dict[str, list[InformationRound]] = {}
        seen_round_ids: set[str] = set()
        seen_numbers: set[tuple[str, int]] = set()
        for source_row in round_rows:
            normalized = cls._normalize_information_round_row(source_row)
            round_record = InformationRound.model_validate_json(
                str(normalized["round_json"])
            )
            parent = requests.get(round_record.planner_request_id)
            if parent is None:
                raise ValueError(
                    "InformationRound parent PlannerRequest does not exist"
                )
            if parent.game_session_id != str(normalized["game_id"]):
                raise ValueError("InformationRound game disagrees with PlannerRequest")
            if parent.target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS:
                continue
            if require_canonical and dict(source_row) != normalized:
                raise ValueError("strategic InformationRound row is not canonical")
            if round_record.information_round_id in seen_round_ids:
                raise ValueError("duplicate strategic InformationRound identity")
            seen_round_ids.add(round_record.information_round_id)
            number_key = (
                round_record.planner_request_id,
                round_record.round_number,
            )
            if number_key in seen_numbers:
                raise ValueError("duplicate strategic InformationRound number")
            seen_numbers.add(number_key)
            rounds_by_request.setdefault(round_record.planner_request_id, []).append(
                round_record
            )

        strategic_request_ids = {
            request.planner_request_id
            for request in requests.values()
            if request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
        }
        requested_ticks: dict[tuple[str, str], list[InformationRequestedTick]] = {}
        collected_ticks: dict[tuple[str, str], list[InformationCollectedTick]] = {}
        attempt_completed_ticks: dict[str, list[PlannerAttemptCompletedTick]] = {}
        for source_row in tick_rows:
            tick = cls._workflow_tick_from_row(source_row)
            if isinstance(tick, InformationRequestedTick):
                if tick.planner_request_id in strategic_request_ids:
                    requested_ticks.setdefault(
                        (tick.planner_request_id, tick.information_round_id), []
                    ).append(tick)
            elif isinstance(tick, InformationCollectedTick):
                if tick.planner_request_id in strategic_request_ids:
                    collected_ticks.setdefault(
                        (tick.planner_request_id, tick.information_round_id), []
                    ).append(tick)
            elif isinstance(tick, PlannerAttemptCompletedTick):
                if tick.planner_request_id in strategic_request_ids:
                    attempt_completed_ticks.setdefault(
                        tick.planner_request_id, []
                    ).append(tick)

        for request in requests.values():
            if request.target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS:
                continue
            attempts = sorted(
                attempts_by_request.get(request.planner_request_id, []),
                key=lambda item: item[1].attempt_number,
            )
            attempt_numbers = [item[1].attempt_number for item in attempts]
            maximum_attempt_number = attempt_numbers[-1] if attempt_numbers else 0
            if request.provider_attempt_count != maximum_attempt_number:
                raise ValueError(
                    "strategic provider_attempt_count disagrees with Attempt history"
                )
            if attempt_numbers != list(range(1, maximum_attempt_number + 1)):
                raise ValueError("strategic ProviderAttempt numbers must be contiguous")
            attempts_by_id = {
                attempt.provider_attempt_id: attempt for _game_id, attempt in attempts
            }
            completion_ticks = attempt_completed_ticks.get(
                request.planner_request_id, []
            )
            for completion_tick in completion_ticks:
                attempt = attempts_by_id.get(completion_tick.provider_attempt_id)
                if (
                    attempt is None
                    or completion_tick.game_session_id != request.game_session_id
                    or completion_tick.provider_attempt_count < 1
                    or attempt.status is ProviderAttemptStatus.STARTED
                    or attempt.completed_at is None
                    or completion_tick.completed_at < attempt.completed_at
                ):
                    raise ValueError(
                        "strategic PlannerAttemptCompletedTick has no matching "
                        "completed ProviderAttempt"
                    )
            latest_attempt = attempts[-1][1] if attempts else None
            if request.status is PlannerRequestStatus.PENDING:
                if attempts:
                    raise ValueError(
                        "PENDING strategic PlannerRequest cannot have ProviderAttempts"
                    )
            elif request.status is PlannerRequestStatus.IN_PROGRESS:
                if (
                    latest_attempt is not None
                    and latest_attempt.status is ProviderAttemptStatus.SUCCEEDED
                ):
                    raise ValueError(
                        "IN_PROGRESS strategic PlannerRequest cannot end in a "
                        "SUCCEEDED ProviderAttempt"
                    )
                if latest_attempt is None or latest_attempt.status not in {
                    ProviderAttemptStatus.STARTED,
                    ProviderAttemptStatus.FAILED,
                }:
                    raise ValueError(
                        "IN_PROGRESS strategic PlannerRequest requires a current "
                        "ProviderAttempt"
                    )
            elif request.status is PlannerRequestStatus.BACKOFF:
                retry_at = request.next_retry_at
                if (
                    latest_attempt is None
                    or latest_attempt.status is not ProviderAttemptStatus.FAILED
                    or latest_attempt.completed_at is None
                    or retry_at is None
                    or retry_at.utcoffset() is None
                ):
                    raise ValueError(
                        "strategic BACKOFF requires a final FAILED ProviderAttempt "
                        "and timezone-aware next_retry_at"
                    )
                matching_ticks = [
                    item
                    for item in completion_ticks
                    if item.provider_attempt_id == latest_attempt.provider_attempt_id
                ]
                if len(matching_ticks) != 1:
                    raise ValueError(
                        "strategic BACKOFF requires one matching "
                        "PlannerAttemptCompletedTick"
                    )
            if (
                request.status is not PlannerRequestStatus.BACKOFF
                and request.next_retry_at is not None
            ):
                raise ValueError("only strategic BACKOFF may retain next_retry_at")
            if request.status in {
                PlannerRequestStatus.PARTIALLY_COMPLETED,
                PlannerRequestStatus.CANCELLED,
            }:
                raise ValueError(
                    "strategic PlannerRequest uses an unsupported lifecycle status"
                )
            if (
                request.status is PlannerRequestStatus.SUPERSEDED
                and request.failure_category != "stale_strategic_contract_base"
            ):
                raise ValueError("strategic SUPERSEDED requires a stale Contract base")
            if request.status is PlannerRequestStatus.FAILED and (
                latest_attempt is None
                or latest_attempt.status is not ProviderAttemptStatus.FAILED
                or latest_attempt.completed_at is None
            ):
                raise ValueError(
                    "FAILED strategic PlannerRequest requires a final FAILED "
                    "ProviderAttempt"
                )
            if request.status in {
                PlannerRequestStatus.COMPLETED,
                PlannerRequestStatus.REJECTED,
            } and (
                latest_attempt is None
                or latest_attempt.status is not ProviderAttemptStatus.SUCCEEDED
                or latest_attempt.completed_at is None
            ):
                raise ValueError(
                    "response-terminal strategic PlannerRequest requires a final "
                    "SUCCEEDED ProviderAttempt"
                )
            rounds = sorted(
                rounds_by_request.get(request.planner_request_id, []),
                key=lambda item: item.round_number,
            )
            if len(rounds) > 1 or any(
                round_record.round_number != 1 for round_record in rounds
            ):
                raise ValueError(
                    "strategic PlannerRequest supports exactly one information round"
                )
            round_keys = {
                (request.planner_request_id, item.information_round_id)
                for item in rounds
            }
            tick_keys = {
                key
                for key in (*requested_ticks.keys(), *collected_ticks.keys())
                if key[0] == request.planner_request_id
            }
            if tick_keys - round_keys:
                raise ValueError(
                    "strategic information Tick references no InformationRound"
                )
            for round_record in rounds:
                key = (request.planner_request_id, round_record.information_round_id)
                request_ticks = requested_ticks.get(key, [])
                collect_ticks = collected_ticks.get(key, [])
                if (
                    len(request_ticks) != 1
                    or request_ticks[0].game_session_id != request.game_session_id
                ):
                    raise ValueError(
                        "strategic InformationRound requires one requesting Tick"
                    )
                expected_collected_ticks = (
                    1 if round_record.status is InformationRoundStatus.COLLECTED else 0
                )
                if len(collect_ticks) != expected_collected_ticks or any(
                    tick.game_session_id != request.game_session_id
                    for tick in collect_ticks
                ):
                    raise ValueError(
                        "strategic InformationRound collection Tick disagrees"
                    )
                source_attempts = [
                    attempt
                    for game_id, attempt in attempts
                    if game_id == request.game_session_id
                    and attempt.provider_attempt_id
                    == round_record.source_provider_attempt_id
                    and attempt.attempt_number
                    == round_record.source_provider_attempt_number
                ]
                if len(source_attempts) != 1:
                    raise ValueError(
                        "strategic InformationRound requires one source ProviderAttempt"
                    )
                source_attempt = source_attempts[0]
                if (
                    source_attempt.status is not ProviderAttemptStatus.SUCCEEDED
                    or source_attempt.completed_at is None
                ):
                    raise ValueError(
                        "strategic InformationRound source ProviderAttempt must "
                        "be completed and SUCCEEDED"
                    )
                requesting_tick = request_ticks[0]
                if (
                    source_attempt.completed_at > round_record.requested_at
                    or round_record.requested_at > requesting_tick.completed_at
                ):
                    raise ValueError(
                        "strategic InformationRound request timing is not causal"
                    )
                if any(
                    attempt.attempt_number > source_attempt.attempt_number
                    and attempt.started_at < requesting_tick.completed_at
                    for _game_id, attempt in attempts
                ):
                    raise ValueError(
                        "strategic InformationRound source was not the latest "
                        "ProviderAttempt when requested"
                    )
                if collect_ticks:
                    collected_tick = collect_ticks[0]
                    if requesting_tick.completed_at > collected_tick.started_at:
                        raise ValueError(
                            "strategic information was collected before it was "
                            "requested"
                        )
                    if (
                        round_record.completed_at is None
                        or round_record.completed_at < collected_tick.started_at
                        or round_record.completed_at > collected_tick.completed_at
                    ):
                        raise ValueError(
                            "strategic InformationRound completion timing disagrees "
                            "with its collection Tick"
                        )
            requested = [
                item
                for item in rounds
                if item.status is InformationRoundStatus.REQUESTED
            ]
            collected = [
                item
                for item in rounds
                if item.status is InformationRoundStatus.COLLECTED
            ]
            failed = [
                item for item in rounds if item.status is InformationRoundStatus.FAILED
            ]
            if request.information_round_count != len(collected):
                raise ValueError(
                    "strategic information_round_count disagrees with collected history"
                )
            if bool(request.information_results) != bool(collected):
                raise ValueError(
                    "strategic information_results disagree with collected history"
                )
            if collected and canonical_json(
                request.information_results
            ) != canonical_json(collected[0].results):
                raise ValueError(
                    "strategic information_results disagree with collected Round"
                )
            if failed and request.status not in TERMINAL_PLANNER_STATUSES:
                raise ValueError(
                    "strategic failed InformationRound requires a terminal Request"
                )
            for round_record in rounds:
                current_round_allowance = (
                    0 if round_record.status is InformationRoundStatus.COLLECTED else 1
                )
                if round_record.round_number > (
                    request.information_round_count + current_round_allowance
                ):
                    raise ValueError(
                        "strategic InformationRound exceeds proven lifecycle count"
                    )
            if request.status is PlannerRequestStatus.AWAITING_INFORMATION:
                if not request.pending_information_requests:
                    raise ValueError(
                        "strategic AWAITING_INFORMATION Request requires pending inputs"
                    )
                if (
                    len(requested) != 1
                    or not rounds
                    or requested[0].round_number != rounds[-1].round_number
                ):
                    raise ValueError(
                        "strategic AWAITING_INFORMATION Request requires one latest "
                        "REQUESTED InformationRound"
                    )
                if canonical_json(requested[0].requests) != canonical_json(
                    request.pending_information_requests
                ):
                    raise ValueError(
                        "strategic InformationRound requests disagree with "
                        "PlannerRequest pending inputs"
                    )
            elif request.status in TERMINAL_PLANNER_STATUSES:
                if request.pending_information_requests:
                    raise ValueError(
                        "terminal strategic PlannerRequest cannot retain pending inputs"
                    )
                if requested:
                    raise ValueError(
                        "terminal strategic PlannerRequest cannot retain a REQUESTED "
                        "InformationRound"
                    )
            elif requested:
                raise ValueError(
                    "strategic REQUESTED InformationRound requires an "
                    "AWAITING_INFORMATION PlannerRequest"
                )

            source_attempt_keys = {
                (
                    round_record.source_provider_attempt_id,
                    round_record.source_provider_attempt_number,
                )
                for round_record in rounds
            }
            response_facts = cls._classify_planner_request_response(request)
            final_response_uses_success = (
                request.status in TERMINAL_PLANNER_STATUSES
                and response_facts
                in {
                    _PlannerResponseFacts.CANONICAL_RESPONSE,
                    _PlannerResponseFacts.CONTRACT_SCHEMA_FAILURE,
                }
            )
            for _game_id, attempt in attempts:
                if attempt.status is not ProviderAttemptStatus.SUCCEEDED:
                    continue
                if (
                    attempt.provider_attempt_id,
                    attempt.attempt_number,
                ) in source_attempt_keys:
                    continue
                if (
                    final_response_uses_success
                    and attempt.attempt_number == maximum_attempt_number
                ):
                    continue
                raise ValueError(
                    "strategic SUCCEEDED ProviderAttempt has no response aggregate"
                )
            if (
                request.status is PlannerRequestStatus.IN_PROGRESS
                and attempts
                and attempts[-1][1].status is ProviderAttemptStatus.SUCCEEDED
            ):
                raise ValueError(
                    "IN_PROGRESS strategic PlannerRequest cannot end in a "
                    "SUCCEEDED ProviderAttempt"
                )
            if request.status in {
                PlannerRequestStatus.AWAITING_INFORMATION,
                PlannerRequestStatus.READY_TO_CONTINUE,
            } and (
                not attempts
                or attempts[-1][1].status is not ProviderAttemptStatus.SUCCEEDED
            ):
                raise ValueError(
                    "strategic information lifecycle requires a final SUCCEEDED "
                    "ProviderAttempt"
                )

    @staticmethod
    def _strategic_request_base_is_stale(
        request: PlannerRequest,
        contract_root: Mapping[str, Any] | None,
    ) -> bool:
        target = request.target
        if target.kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION:
            target_contract_id = (
                target.strategic_contract_id
                or build_strategic_contract_id(request.game_session_id)
            )
            expected_base_revision = 0
            base_is_stale = contract_root is not None
        elif target.kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR:
            target_contract_id = target.strategic_contract_id
            expected_base_revision = target.base_contract_revision
            base_is_stale = (
                contract_root is None
                or str(contract_root["contract_id"]) != target_contract_id
                or int(contract_root["active_revision"]) != expected_base_revision
            )
        else:
            return False
        projection_context = request.input_projection.get(
            "strategic_proposal_context", request.input_projection
        )
        if not isinstance(projection_context, Mapping):
            return True
        projection_is_stale = (
            target.strategic_scope != "research"
            or projection_context.get("target_contract_id") != target_contract_id
            or projection_context.get("expected_base_revision")
            != expected_base_revision
            or projection_context.get("strategic_scope") != "research"
        )
        return base_is_stale or projection_is_stale

    @classmethod
    def _validate_explicit_wait_tick_interval(
        cls,
        ticks: Sequence[WorkflowTick],
        opening_tick: WorkflowTick,
        resumed_tick: WorkflowTick | None,
        resume_authorized_at: datetime | None,
        allowed_tick_types: tuple[type[Any], ...],
        *,
        label: str,
    ) -> None:
        if (resumed_tick is None) != (resume_authorized_at is None):
            raise ValueError(f"{label} resume Tick and authorization must agree")
        if (
            resumed_tick is not None
            and resumed_tick.started_at < opening_tick.completed_at
        ):
            raise ValueError(f"{label} resume Tick precedes the explicit-only wait")
        if (
            resume_authorized_at is not None
            and resume_authorized_at < opening_tick.completed_at
        ):
            raise ValueError(f"{label} resume authorization precedes the wait")
        if (
            resumed_tick is not None
            and resume_authorized_at is not None
            and resume_authorized_at > resumed_tick.completed_at
        ):
            raise ValueError(f"{label} resume Tick completes before authorization")
        for tick in ticks:
            if (
                tick.game_session_id != opening_tick.game_session_id
                or tick.tick_id == opening_tick.tick_id
                or (resumed_tick is not None and tick.tick_id == resumed_tick.tick_id)
                or tick.completed_at <= opening_tick.completed_at
                or (
                    resumed_tick is not None
                    and tick.started_at >= resumed_tick.completed_at
                )
            ):
                continue
            if (
                not isinstance(tick, allowed_tick_types)
                or tick.starting_runtime_state is not RuntimeState.AWAITING_HUMAN
                or tick.ending_runtime_state is not RuntimeState.AWAITING_HUMAN
                or tick.started_at < opening_tick.completed_at
                or (
                    resumed_tick is not None
                    and tick.completed_at > resumed_tick.started_at
                )
            ):
                raise ValueError(
                    f"{label} explicit-only wait interval contains an invalid Tick"
                )

    @classmethod
    def _validate_strategic_terminal_wait_state(
        cls,
        requests: Mapping[str, PlannerRequest],
        attempt_rows: Sequence[Mapping[str, Any]],
        ticks: Sequence[WorkflowTick],
        runtime_by_game: Mapping[str, RuntimeState],
        wait_by_game: Mapping[str, Mapping[str, Any]],
    ) -> None:
        attempts_by_request: dict[str, list[ProviderAttempt]] = {}
        for row in attempt_rows:
            normalized = cls._normalize_provider_attempt_row(row)
            attempt = ProviderAttempt.model_validate_json(
                str(normalized["attempt_json"])
            )
            attempts_by_request.setdefault(attempt.planner_request_id, []).append(
                attempt
            )
        terminated_by_request: dict[str, list[StrategicRequestTerminatedTick]] = {}
        resumed_by_request: dict[str, list[StrategicRequestWaitResumedTick]] = {}
        errors_by_request: dict[str, list[StrategicRequestWaitErrorTick]] = {}
        for tick in ticks:
            if isinstance(tick, StrategicRequestTerminatedTick):
                terminated_by_request.setdefault(tick.planner_request_id, []).append(
                    tick
                )
            elif isinstance(tick, StrategicRequestWaitResumedTick):
                resumed_by_request.setdefault(tick.planner_request_id, []).append(tick)
            elif isinstance(tick, StrategicRequestWaitErrorTick):
                errors_by_request.setdefault(tick.planner_request_id, []).append(tick)

        unresolved_by_game: dict[
            str, tuple[PlannerRequest, StrategicRequestTerminatedTick]
        ] = {}
        terminal_statuses = {
            PlannerRequestStatus.FAILED,
            PlannerRequestStatus.REJECTED,
            PlannerRequestStatus.SUPERSEDED,
        }
        strategic_request_ids = {
            request.planner_request_id
            for request in requests.values()
            if request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
        }
        referenced_request_ids = (
            set(terminated_by_request)
            | set(resumed_by_request)
            | set(errors_by_request)
        )
        if referenced_request_ids - strategic_request_ids:
            raise ValueError(
                "Strategic Request wait Tick references an unknown Request"
            )
        for request in requests.values():
            if request.target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS:
                continue
            termination_ticks = terminated_by_request.get(
                request.planner_request_id, []
            )
            if request.status not in terminal_statuses:
                if (
                    termination_ticks
                    or resumed_by_request.get(request.planner_request_id)
                    or errors_by_request.get(request.planner_request_id)
                ):
                    raise ValueError(
                        "non-terminal strategic Request has a terminal-wait Tick"
                    )
                continue
            if len(termination_ticks) != 1:
                raise ValueError(
                    "terminal strategic Request requires exactly one termination Tick"
                )
            termination = termination_ticks[0]
            attempts = sorted(
                attempts_by_request.get(request.planner_request_id, []),
                key=lambda item: item.attempt_number,
            )
            latest_attempt = attempts[-1] if attempts else None
            expected_attempt_id = (
                None if latest_attempt is None else latest_attempt.provider_attempt_id
            )
            if (
                termination.game_session_id != request.game_session_id
                or termination.terminal_status is not request.status
                or termination.failure_category != request.failure_category
                or termination.provider_attempt_id != expected_attempt_id
                or request.completed_at is None
                or termination.completed_at < request.completed_at
                or (
                    latest_attempt is not None
                    and (
                        latest_attempt.completed_at is None
                        or termination.completed_at < latest_attempt.completed_at
                    )
                )
            ):
                raise ValueError(
                    "strategic Request termination Tick disagrees with Request or Attempt"
                )
            resume_ticks = resumed_by_request.get(request.planner_request_id, [])
            if len(resume_ticks) > 1:
                raise ValueError("strategic Request wait cannot have two resumed Ticks")
            error_ticks = errors_by_request.get(request.planner_request_id, [])
            for error_tick in error_ticks:
                if (
                    error_tick.game_session_id != request.game_session_id
                    or error_tick.terminal_tick_id != termination.tick_id
                    or error_tick.terminal_status is not request.status
                    or error_tick.failure_category != request.failure_category
                    or error_tick.started_at < termination.completed_at
                ):
                    raise ValueError(
                        "strategic Request wait-error Tick disagrees with termination"
                    )
            if resume_ticks:
                resumed = resume_ticks[0]
                if (
                    resumed.game_session_id != request.game_session_id
                    or resumed.terminal_tick_id != termination.tick_id
                    or resumed.terminal_status is not request.status
                    or resumed.started_at < termination.completed_at
                    or resumed.resumed_at < termination.completed_at
                ):
                    raise ValueError(
                        "strategic Request wait-resumed Tick disagrees with termination"
                    )
                if any(
                    error_tick.completed_at > resumed.started_at
                    for error_tick in error_ticks
                ):
                    raise ValueError(
                        "strategic Request wait-error Tick occurred after wait resumed"
                    )
                cls._validate_explicit_wait_tick_interval(
                    ticks,
                    termination,
                    resumed,
                    resumed.resumed_at,
                    (AwaitingHumanTick, StrategicRequestWaitErrorTick),
                    label="strategic Request terminal",
                )
                continue
            cls._validate_explicit_wait_tick_interval(
                ticks,
                termination,
                None,
                None,
                (AwaitingHumanTick, StrategicRequestWaitErrorTick),
                label="strategic Request terminal",
            )
            if request.game_session_id in unresolved_by_game:
                raise ValueError(
                    "a game cannot have two unresolved strategic Request waits"
                )
            unresolved_by_game[request.game_session_id] = (request, termination)

        for game_id, (request, termination) in unresolved_by_game.items():
            if runtime_by_game.get(game_id) is not RuntimeState.AWAITING_HUMAN:
                raise ValueError(
                    "unresolved strategic Request termination requires AWAITING_HUMAN"
                )
            context = wait_by_game.get(game_id)
            expected = {
                "wait_kind": "strategic_request_terminated",
                "resume_policy": "explicit_only",
                "planner_request_id": request.planner_request_id,
                "terminal_tick_id": termination.tick_id,
                "terminal_status": request.status.value,
                "failure_category": request.failure_category,
            }
            if context is None or any(
                context.get(key) != value for key, value in expected.items()
            ):
                raise ValueError(
                    "strategic Request termination Human Wait context disagrees"
                )
            request_error_ticks = errors_by_request.get(request.planner_request_id, [])
            expected_blocking_reason = (
                max(
                    request_error_ticks, key=lambda item: item.completed_at
                ).blocking_reason
                if request_error_ticks
                else termination.blocking_reason
            )
            if context.get("blocking_reason") != expected_blocking_reason:
                raise ValueError(
                    "strategic Request termination blocking reason disagrees"
                )
            if type(context.get("resume_requested")) is not bool:
                raise ValueError(
                    "strategic Request termination resume_requested must be a bool"
                )
            if context["resume_requested"] is True:
                requested_at = cls._parse_audit_datetime(
                    context.get("resume_requested_at"),
                    "strategic Request wait resume_requested_at",
                )
                if requested_at < termination.completed_at:
                    raise ValueError(
                        "strategic Request wait resume precedes termination"
                    )
            elif "resume_requested_at" in context:
                raise ValueError(
                    "unrequested strategic Request wait contains resume timestamp"
                )
        for game_id, context in wait_by_game.items():
            if context.get("wait_kind") != "strategic_request_terminated":
                continue
            if game_id not in unresolved_by_game:
                raise ValueError(
                    "strategic Request termination context is resolved or unknown"
                )

    @classmethod
    def _validate_strategic_proposal_lifecycle_v10(
        cls,
        proposal_rows: Sequence[Mapping[str, Any]],
        resume_request_rows: Sequence[Mapping[str, Any]],
        request_rows: Sequence[Mapping[str, Any]],
        attempt_rows: Sequence[Mapping[str, Any]],
        information_round_rows: Sequence[Mapping[str, Any]],
        tick_rows: Sequence[Mapping[str, Any]],
        runtime_rows: Sequence[Mapping[str, Any]],
        workflow_meta_rows: Sequence[Mapping[str, Any]],
        contract_root_rows: Sequence[Mapping[str, Any]],
        *,
        require_canonical: bool,
    ) -> None:
        cls._validate_strategic_proposal_state(
            proposal_rows,
            request_rows,
            attempt_rows,
            require_canonical=require_canonical,
        )
        roots_by_game: dict[str, dict[str, Any]] = {}
        for row in contract_root_rows:
            normalized = cls._normalize_contract_root_row(row)
            if require_canonical and dict(row) != normalized:
                raise ValueError("StrategicContract root row is not canonical")
            game_id = str(normalized["game_id"])
            if game_id in roots_by_game:
                raise ValueError("duplicate StrategicContract root game identity")
            roots_by_game[game_id] = normalized

        requests: dict[str, PlannerRequest] = {}
        for row in request_rows:
            normalized = cls._normalize_planner_request_row(
                row, validate_response_contract=True
            )
            request = PlannerRequest.model_validate_json(
                str(normalized["request_json"])
            )
            if request.planner_request_id in requests:
                raise ValueError("duplicate PlannerRequest identity")
            if (
                request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
                and request.status is PlannerRequestStatus.SUPERSEDED
                and not cls._strategic_request_base_is_stale(
                    request, roots_by_game.get(request.game_session_id)
                )
            ):
                raise ValueError(
                    "strategic SUPERSEDED Request has no stale Contract base"
                )
            requests[request.planner_request_id] = request
        cls._validate_strategic_information_round_state(
            requests,
            attempt_rows,
            information_round_rows,
            tick_rows,
            require_canonical=require_canonical,
        )

        proposals: dict[str, StrategicResearchProposal] = {}
        for row in proposal_rows:
            normalized = cls._normalize_strategic_research_proposal_row(row)
            proposal = StrategicResearchProposal.model_validate_json(
                str(normalized["proposal_json"])
            )
            proposals[proposal.proposal_id] = proposal

        resume_requests_by_proposal: dict[
            str, list[StrategicProposalWaitResumeRequest]
        ] = {}
        resume_request_ids: set[str] = set()
        for row in resume_request_rows:
            normalized = cls._normalize_strategic_proposal_wait_resume_request_row(row)
            if require_canonical and dict(row) != normalized:
                raise ValueError(
                    "Strategic Proposal Resume Request row is not canonical"
                )
            resume_request = StrategicProposalWaitResumeRequest.model_validate_json(
                str(normalized["request_json"])
            )
            if resume_request.resume_request_id in resume_request_ids:
                raise ValueError("duplicate Strategic Proposal Resume Request identity")
            resume_request_ids.add(resume_request.resume_request_id)
            resume_requests_by_proposal.setdefault(
                resume_request.proposal_id, []
            ).append(resume_request)

        ticks = [cls._workflow_tick_from_row(row) for row in tick_rows]
        ready_by_proposal: dict[str, list[StrategicProposalReadyTick]] = {}
        resumed_by_proposal: dict[str, list[StrategicProposalWaitResumedTick]] = {}
        errors_by_proposal: dict[str, list[StrategicProposalWaitErrorTick]] = {}
        for tick in ticks:
            if isinstance(tick, StrategicProposalReadyTick):
                ready_by_proposal.setdefault(tick.proposal_id, []).append(tick)
            elif isinstance(tick, StrategicProposalWaitResumedTick):
                resumed_by_proposal.setdefault(tick.proposal_id, []).append(tick)
            elif isinstance(tick, StrategicProposalWaitErrorTick):
                errors_by_proposal.setdefault(tick.proposal_id, []).append(tick)

        runtime_by_game: dict[str, RuntimeState] = {}
        for row in runtime_rows:
            game_id = str(row["game_id"])
            if game_id in runtime_by_game:
                raise ValueError("duplicate RuntimeState game identity")
            runtime_by_game[game_id] = RuntimeState(str(row["state"]))

        wait_by_game: dict[str, dict[str, Any]] = {}
        for row in workflow_meta_rows:
            key = str(row["key"])
            if not key.startswith("human_wait:"):
                continue
            game_id = key.split(":", 1)[1]
            if game_id in wait_by_game:
                raise ValueError("duplicate Human Wait context")
            context = cls._load(str(row["value_json"]))
            if not isinstance(context, dict):
                raise ValueError("Human Wait context must be an object")
            wait_by_game[game_id] = context

        cls._validate_strategic_terminal_wait_state(
            requests,
            attempt_rows,
            ticks,
            runtime_by_game,
            wait_by_game,
        )

        unresolved_by_game: dict[
            str,
            tuple[StrategicResearchProposal, StrategicProposalWaitResumeRequest | None],
        ] = {}
        for proposal in proposals.values():
            ready_ticks = ready_by_proposal.get(proposal.proposal_id, [])
            if len(ready_ticks) != 1:
                raise ValueError(
                    "Proposal requires exactly one matching Proposal-ready Tick"
                )
            ready = ready_ticks[0]
            if (
                ready.game_session_id != proposal.game_session_id
                or ready.planner_request_id != proposal.source_planner_request_id
                or ready.target_kind is not proposal.target_kind
                or ready.expected_base_revision != proposal.expected_base_revision
            ):
                raise ValueError("Proposal-ready Tick identity disagrees with Proposal")
            resume_requests = resume_requests_by_proposal.get(proposal.proposal_id, [])
            if len(resume_requests) > 1:
                raise ValueError("Proposal cannot have two Resume Requests")
            resume_request = resume_requests[0] if resume_requests else None
            if resume_request is not None:
                if (
                    resume_request.game_session_id != proposal.game_session_id
                    or resume_request.planner_request_id
                    != proposal.source_planner_request_id
                    or resume_request.proposal_ready_tick_id != ready.tick_id
                    or resume_request.target_kind is not proposal.target_kind
                    or resume_request.expected_base_revision
                    != proposal.expected_base_revision
                ):
                    raise ValueError(
                        "Strategic Proposal Resume Request identity disagrees with "
                        "Proposal or Ready Tick"
                    )
                if resume_request.requested_at < ready.completed_at:
                    raise ValueError(
                        "Strategic Proposal Resume Request precedes Proposal-ready Tick"
                    )
            for error_tick in errors_by_proposal.get(proposal.proposal_id, []):
                if (
                    error_tick.game_session_id != proposal.game_session_id
                    or error_tick.planner_request_id
                    != proposal.source_planner_request_id
                    or error_tick.proposal_ready_tick_id != ready.tick_id
                    or error_tick.target_kind is not proposal.target_kind
                    or error_tick.expected_base_revision
                    != proposal.expected_base_revision
                    or error_tick.started_at < ready.completed_at
                ):
                    raise ValueError(
                        "Proposal wait-error Tick identity disagrees with Proposal"
                    )
            resumed_ticks = resumed_by_proposal.get(proposal.proposal_id, [])
            if len(resumed_ticks) > 1:
                raise ValueError("Proposal cannot have two wait-resumed Ticks")
            if resumed_ticks:
                if resume_request is None:
                    raise ValueError(
                        "Proposal wait-resumed Tick requires an immutable Resume Request"
                    )
                resumed = resumed_ticks[0]
                if (
                    resumed.game_session_id != proposal.game_session_id
                    or resumed.planner_request_id != proposal.source_planner_request_id
                    or resumed.target_kind is not proposal.target_kind
                    or resumed.expected_base_revision != proposal.expected_base_revision
                    or resumed.resume_request_id != resume_request.resume_request_id
                    or resumed.proposal_ready_tick_id != ready.tick_id
                ):
                    raise ValueError(
                        "Proposal wait-resumed Tick identity disagrees with Proposal"
                    )
                if resumed.completed_at < resume_request.requested_at:
                    raise ValueError(
                        "Proposal wait-resumed Tick precedes its Resume Request"
                    )
                if any(
                    error_tick.completed_at > resumed.started_at
                    for error_tick in errors_by_proposal.get(proposal.proposal_id, [])
                ):
                    raise ValueError(
                        "Proposal wait-error Tick occurred after Proposal wait resumed"
                    )
                cls._validate_explicit_wait_tick_interval(
                    ticks,
                    ready,
                    resumed,
                    resume_request.requested_at,
                    (AwaitingHumanTick, StrategicProposalWaitErrorTick),
                    label="strategic Proposal",
                )
                continue
            cls._validate_explicit_wait_tick_interval(
                ticks,
                ready,
                None,
                None,
                (AwaitingHumanTick, StrategicProposalWaitErrorTick),
                label="strategic Proposal",
            )
            if proposal.game_session_id in unresolved_by_game:
                raise ValueError("a game cannot have two unresolved Proposal waits")
            unresolved_by_game[proposal.game_session_id] = (proposal, resume_request)

        for proposal_id in ready_by_proposal:
            if proposal_id not in proposals:
                raise ValueError("Proposal-ready Tick references an unknown Proposal")
        for proposal_id in resumed_by_proposal:
            if proposal_id not in proposals:
                raise ValueError(
                    "Proposal wait-resumed Tick references an unknown Proposal"
                )
        for proposal_id in errors_by_proposal:
            if proposal_id not in proposals:
                raise ValueError(
                    "Proposal wait-error Tick references an unknown Proposal"
                )
        for proposal_id in resume_requests_by_proposal:
            if proposal_id not in proposals:
                raise ValueError("Resume Request references an unknown Proposal")

        for game_id, unresolved in unresolved_by_game.items():
            proposal, resume_request = unresolved
            if runtime_by_game.get(game_id) is not RuntimeState.AWAITING_HUMAN:
                raise ValueError(
                    "unresolved Proposal wait requires AWAITING_HUMAN RuntimeState"
                )
            context = wait_by_game.get(game_id)
            if context is None:
                raise ValueError("unresolved Proposal wait requires Human Wait context")
            expected = {
                "wait_kind": "strategic_contract_proposal_ready",
                "resume_policy": "explicit_only",
                "reason": "strategic_contract_proposal_ready",
                "planner_request_id": proposal.source_planner_request_id,
                "proposal_id": proposal.proposal_id,
                "target_kind": proposal.target_kind.value,
                "expected_base_revision": proposal.expected_base_revision,
            }
            if any(context.get(key) != value for key, value in expected.items()):
                raise ValueError(
                    "Proposal Human Wait context disagrees with unresolved Proposal"
                )
            if type(context.get("resume_requested")) is not bool:
                raise ValueError("Proposal Human Wait resume_requested must be a bool")
            if resume_request is None:
                if context["resume_requested"] is not False:
                    raise ValueError(
                        "Proposal wait cannot be requested without a Resume Request"
                    )
                if "resume_request_id" in context or "resume_requested_at" in context:
                    raise ValueError(
                        "unrequested Proposal wait cannot contain Resume Request facts"
                    )
            else:
                if context["resume_requested"] is not True:
                    raise ValueError(
                        "Proposal Resume Request requires resume_requested=True"
                    )
                if context.get("resume_request_id") != resume_request.resume_request_id:
                    raise ValueError("Proposal Human Wait Resume Request ID disagrees")
                requested_at = cls._parse_audit_datetime(
                    context.get("resume_requested_at"),
                    "Proposal Human Wait resume_requested_at",
                )
                if requested_at != resume_request.requested_at:
                    raise ValueError("Proposal Human Wait requested_at disagrees")
                if (
                    context.get("proposal_ready_tick_id")
                    != resume_request.proposal_ready_tick_id
                ):
                    raise ValueError("Proposal Human Wait Ready Tick ID disagrees")

        for game_id, context in wait_by_game.items():
            if context.get("wait_kind") != "strategic_contract_proposal_ready":
                continue
            if game_id not in unresolved_by_game:
                raise ValueError(
                    "special Proposal Human Wait context references a resolved "
                    "or unknown Proposal"
                )

    @classmethod
    def _validate_phase1b_proposals_v10(cls, conn: sqlite3.Connection) -> None:
        cls._validate_strategic_proposal_lifecycle_v10(
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM strategic_research_proposals ORDER BY proposal_id"
                ).fetchall()
            ],
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM strategic_proposal_wait_resume_requests "
                    "ORDER BY requested_at, resume_request_id"
                ).fetchall()
            ],
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM logical_planner_requests ORDER BY planner_request_id"
                ).fetchall()
            ],
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM provider_attempts ORDER BY planner_request_id, attempt_number"
                ).fetchall()
            ],
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM information_rounds "
                    "ORDER BY planner_request_id, round_number"
                ).fetchall()
            ],
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM workflow_ticks ORDER BY started_at, tick_id"
                ).fetchall()
            ],
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM runtime_state ORDER BY game_id"
                ).fetchall()
            ],
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM workflow_meta WHERE key LIKE 'human_wait:%' "
                    "ORDER BY key"
                ).fetchall()
            ],
            [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM strategic_contract_roots ORDER BY game_id"
                ).fetchall()
            ],
            require_canonical=True,
        )

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

    @classmethod
    def _planner_request_from_row(cls, row: Mapping[str, Any]) -> PlannerRequest:
        stored = dict(row)
        normalized = cls._normalize_planner_request_row(
            stored,
            validate_response_contract=True,
        )
        for column in (
            "request_target_kind",
            "request_target_key",
            "decision_group_id",
            "decision_gap_ids_json",
            "request_json",
        ):
            if stored.get(column) != normalized.get(column):
                request_id = stored.get("planner_request_id", "<unknown>")
                raise ValueError(
                    f"logical PlannerRequest {request_id} {column} is not canonical"
                )
        return PlannerRequest.model_validate_json(str(stored["request_json"]))

    @classmethod
    def _normalize_planner_request_row(
        cls,
        row: Mapping[str, Any],
        *,
        validate_existing_target_columns: bool = True,
        validate_response_contract: bool = False,
    ) -> dict[str, Any]:
        """Canonicalize one v7/v8 request row for migration and replay import."""

        normalized = dict(row)
        try:
            raw_request = json.loads(str(normalized["request_json"]))
            if not isinstance(raw_request, dict):
                raise ValueError("PlannerRequest JSON must be an object")
        except Exception as exc:
            raise ValueError("invalid logical PlannerRequest JSON") from exc
        has_v8_relational_target = all(
            column in normalized
            for column in ("request_target_kind", "request_target_key")
        )
        legacy_v7_shape = (
            not has_v8_relational_target
            and "target" not in raw_request
            and (
                "decision_gap_ids" in raw_request or "decision_group_id" in raw_request
            )
        )
        if (
            legacy_v7_shape
            and raw_request.get("status")
            in {
                PlannerRequestStatus.COMPLETED.value,
                PlannerRequestStatus.PARTIALLY_COMPLETED.value,
                PlannerRequestStatus.REJECTED.value,
            }
            and raw_request.get("response_payload") is None
            and "response_evidence_compatibility" not in raw_request
        ):
            raw_request["response_evidence_compatibility"] = (
                PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD.value
            )
        try:
            request = PlannerRequest.model_validate_json(canonical_json(raw_request))
        except Exception as exc:
            raise ValueError("invalid logical PlannerRequest JSON") from exc

        required_columns = {
            "planner_request_id",
            "game_id",
            "turn",
            "status",
            "input_projection_hash",
            "input_projection_version",
            "decision_group_id",
            "decision_gap_ids_json",
            "created_at",
            "completed_at",
        }
        missing_columns = required_columns - normalized.keys()
        if missing_columns:
            raise ValueError(
                "logical PlannerRequest row is missing columns: "
                f"{sorted(missing_columns)}"
            )
        relational_checks = {
            "planner_request_id": request.planner_request_id,
            "game_id": request.game_session_id,
            "turn": request.turn_number,
            "input_projection_hash": request.input_projection_hash,
            "input_projection_version": request.input_projection_version,
            "status": request.status.value,
        }
        for column, expected in relational_checks.items():
            actual = normalized[column]
            if column == "turn":
                try:
                    actual = int(actual)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "logical PlannerRequest turn is not an integer"
                    ) from exc
            elif not isinstance(actual, str):
                actual = str(actual)
            if actual != expected:
                raise ValueError(
                    f"logical PlannerRequest {request.planner_request_id} "
                    f"{column} conflicts with request_json"
                )

        for column, expected in (
            ("created_at", request.created_at),
            ("completed_at", request.completed_at),
        ):
            actual_value = normalized[column]
            if actual_value is None:
                actual = None
            else:
                try:
                    actual = datetime.fromisoformat(
                        str(actual_value).replace("Z", "+00:00")
                    )
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"logical PlannerRequest {column} is not a datetime"
                    ) from exc
            if actual != expected:
                raise ValueError(
                    f"logical PlannerRequest {request.planner_request_id} "
                    f"{column} conflicts with request_json"
                )

        try:
            relation_gap_ids = json.loads(
                str(normalized.get("decision_gap_ids_json", "[]"))
            )
        except Exception as exc:
            raise ValueError("invalid decision_gap_ids_json") from exc
        if not isinstance(relation_gap_ids, list):
            raise ValueError("decision_gap_ids_json must contain a JSON array")
        relation_group_id = normalized.get("decision_group_id")
        if request.target.kind is PlannerRequestTargetKind.LEGACY_DECISION_GROUP:
            relation_target = PlannerRequestTarget(
                kind=PlannerRequestTargetKind.LEGACY_DECISION_GROUP,
                decision_group_id=(
                    None if relation_group_id is None else str(relation_group_id)
                ),
                decision_gap_ids=tuple(relation_gap_ids),
            )
            if (
                relation_target.decision_group_id != request.decision_group_id
                or relation_target.decision_gap_ids != request.decision_gap_ids
            ):
                raise ValueError(
                    f"logical PlannerRequest {request.planner_request_id} "
                    "legacy target columns conflict with request_json"
                )
        elif relation_group_id is not None or relation_gap_ids:
            raise ValueError(
                f"logical PlannerRequest {request.planner_request_id} "
                "non-legacy target has legacy relational identity"
            )

        expected_kind = request.target.kind.value
        expected_key = request.target.target_key
        for column, expected in (
            ("request_target_kind", expected_kind),
            ("request_target_key", expected_key),
        ):
            existing = normalized.get(column)
            if (
                validate_existing_target_columns
                and existing is not None
                and str(existing) != expected
            ):
                raise ValueError(
                    f"logical PlannerRequest {request.planner_request_id} "
                    f"{column} conflicts with canonical target"
                )

        if validate_response_contract:
            cls._classify_planner_request_response(request)

        normalized.update(
            {
                "planner_request_id": request.planner_request_id,
                "game_id": request.game_session_id,
                "turn": request.turn_number,
                "status": request.status.value,
                "input_projection_hash": request.input_projection_hash,
                "input_projection_version": request.input_projection_version,
                "request_target_kind": expected_kind,
                "request_target_key": expected_key,
                "decision_group_id": request.decision_group_id,
                "decision_gap_ids_json": canonical_json(list(request.decision_gap_ids)),
                "request_json": canonical_json(request.model_dump(mode="json")),
                "created_at": request.created_at.isoformat(),
                "completed_at": (
                    None
                    if request.completed_at is None
                    else request.completed_at.isoformat()
                ),
            }
        )
        return normalized

    @classmethod
    def _migrate_phase1a_v8(
        cls,
        conn: sqlite3.Connection,
        *,
        validate_existing_target_columns: bool = True,
    ) -> None:
        normalized_rows = [
            cls._normalize_planner_request_row(
                row,
                validate_existing_target_columns=validate_existing_target_columns,
                validate_response_contract=True,
            )
            for row in conn.execute(
                "SELECT * FROM logical_planner_requests ORDER BY planner_request_id"
            ).fetchall()
        ]
        identities: set[tuple[str, str, str]] = set()
        for row in normalized_rows:
            identity = (
                str(row["game_id"]),
                str(row["request_target_key"]),
                str(row["input_projection_hash"]),
            )
            if identity in identities:
                raise ValueError(
                    "v8 migration found duplicate canonical PlannerRequest identity"
                )
            identities.add(identity)

        conn.execute("DROP TABLE IF EXISTS logical_planner_requests_v8")
        conn.execute(
            """
            CREATE TABLE logical_planner_requests_v8 (
                planner_request_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                request_target_kind TEXT NOT NULL,
                request_target_key TEXT NOT NULL,
                decision_group_id TEXT,
                turn INTEGER NOT NULL,
                status TEXT NOT NULL,
                input_projection_hash TEXT NOT NULL,
                input_projection_version TEXT NOT NULL,
                decision_gap_ids_json TEXT NOT NULL,
                request_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT
            )
            """
        )
        for row in normalized_rows:
            conn.execute(
                """
                INSERT INTO logical_planner_requests_v8(
                    planner_request_id, game_id, request_target_kind,
                    request_target_key, decision_group_id, turn, status,
                    input_projection_hash, input_projection_version,
                    decision_gap_ids_json, request_json, created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["planner_request_id"],
                    row["game_id"],
                    row["request_target_kind"],
                    row["request_target_key"],
                    row["decision_group_id"],
                    row["turn"],
                    row["status"],
                    row["input_projection_hash"],
                    row["input_projection_version"],
                    row["decision_gap_ids_json"],
                    row["request_json"],
                    row["created_at"],
                    row["completed_at"],
                ),
            )

        conn.execute("DROP TABLE logical_planner_requests")
        conn.execute(
            "ALTER TABLE logical_planner_requests_v8 RENAME TO logical_planner_requests"
        )
        conn.execute(
            """
            CREATE INDEX idx_logical_requests_game_status
            ON logical_planner_requests (game_id, status, turn)
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_logical_requests_target_input
            ON logical_planner_requests(
                game_id, request_target_key, input_projection_hash
            )
            """
        )
        if conn.execute("PRAGMA foreign_key_check").fetchall():
            raise ValueError("v8 migration would violate PlannerRequest foreign keys")
        cls._validate_phase1a_v8(conn)

    @classmethod
    def _validate_phase1a_v8(cls, conn: sqlite3.Connection) -> None:
        columns = {
            str(row["name"])
            for row in conn.execute(
                "PRAGMA table_info(logical_planner_requests)"
            ).fetchall()
        }
        required = {"request_target_kind", "request_target_key"}
        if not required.issubset(columns):
            raise ValueError("workflow database v8 is missing PlannerRequest columns")
        null_count = int(
            conn.execute(
                """
                SELECT COUNT(*) AS value FROM logical_planner_requests
                WHERE request_target_kind IS NULL OR request_target_key IS NULL
                   OR request_target_kind='' OR request_target_key=''
                """
            ).fetchone()["value"]
        )
        if null_count:
            raise ValueError("workflow database v8 has incomplete PlannerRequest keys")
        for row in conn.execute(
            "SELECT * FROM logical_planner_requests ORDER BY planner_request_id"
        ).fetchall():
            request = cls._planner_request_from_row(row)
            cls._validate_contract_schema_failure_attempt(
                request,
                conn.execute(
                    """
                    SELECT * FROM provider_attempts
                    WHERE planner_request_id=?
                    ORDER BY attempt_number
                    """,
                    (request.planner_request_id,),
                ).fetchall(),
            )

        indexes = {
            str(row["name"]): int(row["unique"])
            for row in conn.execute(
                "PRAGMA index_list(logical_planner_requests)"
            ).fetchall()
        }
        index_name = "idx_logical_requests_target_input"
        if indexes.get(index_name) != 1:
            raise ValueError("workflow database v8 is missing target identity index")
        index_columns = [
            str(row["name"])
            for row in conn.execute(f"PRAGMA index_info({index_name})").fetchall()
        ]
        if index_columns != [
            "game_id",
            "request_target_key",
            "input_projection_hash",
        ]:
            raise ValueError("workflow database v8 target identity index is invalid")

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
    def _contract_from_revision_row(cls, row: Mapping[str, Any]) -> StrategicContract:
        normalized = cls._normalize_contract_revision_row(row)
        if dict(row) != normalized:
            raise ValueError("StrategicContract revision row is not canonical")
        return StrategicContract.model_validate_json(str(row["contract_json"]))

    @classmethod
    def _contract_commit_from_row(
        cls, row: Mapping[str, Any]
    ) -> StrategicContractCommit:
        normalized = cls._normalize_contract_commit_row(row)
        if dict(row) != normalized:
            raise ValueError("StrategicContract commit row is not canonical")
        return StrategicContractCommit.model_validate_json(str(row["commit_json"]))

    def get_active_strategic_contract(
        self, game_session_id: str
    ) -> StrategicContract | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT revision.*
                FROM strategic_contract_roots AS root
                JOIN strategic_contract_revisions AS revision
                  ON revision.game_id=root.game_id
                 AND revision.revision=root.active_revision
                WHERE root.game_id=?
                """,
                (game_session_id,),
            ).fetchone()
        return None if row is None else self._contract_from_revision_row(row)

    def get_strategic_contract_revision(
        self, game_session_id: str, revision: int
    ) -> StrategicContract | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM strategic_contract_revisions
                WHERE game_id=? AND revision=?
                """,
                (game_session_id, revision),
            ).fetchone()
        return None if row is None else self._contract_from_revision_row(row)

    def list_strategic_contract_revisions(
        self, game_session_id: str
    ) -> list[StrategicContract]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM strategic_contract_revisions
                WHERE game_id=? ORDER BY revision
                """,
                (game_session_id,),
            ).fetchall()
        return [self._contract_from_revision_row(row) for row in rows]

    def list_strategic_contract_commits(
        self, game_session_id: str
    ) -> list[StrategicContractCommit]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM strategic_contract_commits
                WHERE game_id=? ORDER BY committed_revision
                """,
                (game_session_id,),
            ).fetchall()
        return [self._contract_commit_from_row(row) for row in rows]

    def commit_strategic_contract_revision(
        self, commit: StrategicContractCommit
    ) -> StrategicContract:
        with self._connect() as conn:
            self._validate_phase1b_contract_foundation(commit.contract)
            existing_row = conn.execute(
                "SELECT * FROM strategic_contract_commits WHERE commit_id=?",
                (commit.commit_id,),
            ).fetchone()
            if existing_row is not None:
                existing = self._contract_commit_from_row(existing_row)
                if existing != commit:
                    raise ValueError(
                        "StrategicContract commit identity was reused with new content"
                    )
                revision_row = conn.execute(
                    """
                    SELECT * FROM strategic_contract_revisions
                    WHERE game_id=? AND revision=?
                    """,
                    (existing.game_session_id, existing.contract.revision),
                ).fetchone()
                if revision_row is None:
                    raise ValueError(
                        "StrategicContract idempotency audit has no revision"
                    )
                return self._contract_from_revision_row(revision_row)

            root = conn.execute(
                "SELECT * FROM strategic_contract_roots WHERE game_id=?",
                (commit.game_session_id,),
            ).fetchone()
            if root is None:
                identity_owner = conn.execute(
                    """
                    SELECT game_id FROM strategic_contract_roots
                    WHERE contract_id=?
                    """,
                    (commit.contract_id,),
                ).fetchone()
                if identity_owner is not None:
                    raise ValueError(
                        "StrategicContract identity belongs to another game"
                    )
                if commit.expected_base_revision != 0:
                    raise ValueError(
                        "stale StrategicContract base revision: no root exists"
                    )
                conn.execute(
                    """
                    INSERT INTO strategic_contract_roots(
                        game_id, contract_id, active_revision, created_at
                    ) VALUES (?, ?, 1, ?)
                    """,
                    (
                        commit.game_session_id,
                        commit.contract_id,
                        commit.committed_at.isoformat(),
                    ),
                )
            else:
                if str(root["contract_id"]) != commit.contract_id:
                    raise ValueError(
                        "game session already has another StrategicContract root"
                    )
                active_revision = int(root["active_revision"])
                if active_revision != commit.expected_base_revision:
                    raise ValueError(
                        "stale StrategicContract base revision: "
                        f"expected {commit.expected_base_revision}, "
                        f"active {active_revision}"
                    )

            contract_json = self._dump(commit.contract.model_dump(mode="json"))
            commit_json = self._dump(commit.model_dump(mode="json"))
            conn.execute(
                """
                INSERT INTO strategic_contract_revisions(
                    game_id, contract_id, revision, contract_json, committed_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    commit.game_session_id,
                    commit.contract_id,
                    commit.contract.revision,
                    contract_json,
                    commit.committed_at.isoformat(),
                ),
            )
            conn.execute(
                """
                INSERT INTO strategic_contract_commits(
                    commit_id, game_id, contract_id, expected_base_revision,
                    committed_revision, commit_json, committed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    commit.commit_id,
                    commit.game_session_id,
                    commit.contract_id,
                    commit.expected_base_revision,
                    commit.contract.revision,
                    commit_json,
                    commit.committed_at.isoformat(),
                ),
            )
            if commit.expected_base_revision > 0:
                updated = conn.execute(
                    """
                    UPDATE strategic_contract_roots
                    SET active_revision=?
                    WHERE game_id=? AND contract_id=? AND active_revision=?
                    """,
                    (
                        commit.contract.revision,
                        commit.game_session_id,
                        commit.contract_id,
                        commit.expected_base_revision,
                    ),
                )
                if updated.rowcount != 1:
                    raise ValueError("stale StrategicContract base revision")

            self._validate_phase1b_v9(conn)
            return commit.contract

    @classmethod
    def _strategic_research_proposal_from_row(
        cls, row: Mapping[str, Any]
    ) -> StrategicResearchProposal:
        normalized = cls._normalize_strategic_research_proposal_row(row)
        if dict(row) != normalized:
            raise ValueError("StrategicResearchProposal row is not canonical")
        return StrategicResearchProposal.model_validate_json(
            str(normalized["proposal_json"])
        )

    @staticmethod
    def _validate_proposal_contract_base_in_connection(
        conn: sqlite3.Connection, proposal: StrategicResearchProposal
    ) -> None:
        root = conn.execute(
            "SELECT contract_id, active_revision FROM strategic_contract_roots "
            "WHERE game_id=?",
            (proposal.game_session_id,),
        ).fetchone()
        if proposal.target_kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION:
            if root is not None:
                raise StaleStrategicContractBaseError(
                    "Contract creation Proposal became stale"
                )
            return
        if (
            root is None
            or str(root["contract_id"]) != proposal.target_contract_id
            or int(root["active_revision"]) != proposal.expected_base_revision
        ):
            raise StaleStrategicContractBaseError(
                "Mission repair Proposal Contract base became stale"
            )

    @classmethod
    def _save_strategic_research_proposal_in_connection(
        cls,
        conn: sqlite3.Connection,
        proposal: StrategicResearchProposal,
    ) -> StrategicResearchProposal:
        existing_row = conn.execute(
            "SELECT * FROM strategic_research_proposals WHERE proposal_id=?",
            (proposal.proposal_id,),
        ).fetchone()
        if existing_row is not None:
            existing = cls._strategic_research_proposal_from_row(existing_row)
            if existing != proposal:
                raise ValueError("Proposal identity was reused with new content")
            return existing
        request_row = conn.execute(
            "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
            (proposal.source_planner_request_id,),
        ).fetchone()
        if request_row is None:
            raise ValueError("Proposal parent PlannerRequest does not exist")
        request = cls._planner_request_from_row(request_row)
        if request.status is not PlannerRequestStatus.COMPLETED:
            raise ValueError("Proposal parent PlannerRequest must be COMPLETED")
        cls._validate_strategic_proposal_target(proposal, request)
        attempt_rows = conn.execute(
            "SELECT * FROM provider_attempts WHERE planner_request_id=? "
            "ORDER BY attempt_number",
            (request.planner_request_id,),
        ).fetchall()
        cls._validate_strategic_proposal_attempt(proposal, request, attempt_rows)
        cls._validate_proposal_contract_base_in_connection(conn, proposal)
        conflicting = conn.execute(
            "SELECT proposal_id FROM strategic_research_proposals "
            "WHERE source_planner_request_id=? OR source_provider_attempt_id=?",
            (
                proposal.source_planner_request_id,
                proposal.source_provider_attempt_id,
            ),
        ).fetchone()
        if conflicting is not None:
            raise ValueError(
                "PlannerRequest or ProviderAttempt already owns a Proposal"
            )
        conn.execute(
            """
            INSERT INTO strategic_research_proposals(
                proposal_id, game_id, source_planner_request_id,
                source_provider_attempt_id, source_provider_attempt_number,
                target_kind, target_contract_id, expected_base_revision,
                proposal_hash, proposal_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                proposal.proposal_id,
                proposal.game_session_id,
                proposal.source_planner_request_id,
                proposal.source_provider_attempt_id,
                proposal.source_provider_attempt_number,
                proposal.target_kind.value,
                proposal.target_contract_id,
                proposal.expected_base_revision,
                proposal.proposal_hash,
                cls._dump(proposal.model_dump(mode="json")),
                proposal.created_at.isoformat(),
            ),
        )
        return proposal

    def save_strategic_research_proposal(
        self, proposal: StrategicResearchProposal
    ) -> StrategicResearchProposal:
        with self._connect() as conn:
            existing_row = conn.execute(
                "SELECT * FROM strategic_research_proposals WHERE proposal_id=?",
                (proposal.proposal_id,),
            ).fetchone()
            if existing_row is None:
                raise ValueError(
                    "new Proposal requires the complete Proposal-ready Tick transaction"
                )
            saved = self._strategic_research_proposal_from_row(existing_row)
            if saved != proposal:
                raise ValueError("Proposal identity was reused with new content")
            self._validate_phase1b_proposals_v10(conn)
            return saved

    def get_strategic_research_proposal(
        self, proposal_id: str
    ) -> StrategicResearchProposal | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM strategic_research_proposals WHERE proposal_id=?",
                (proposal_id,),
            ).fetchone()
        return None if row is None else self._strategic_research_proposal_from_row(row)

    def strategic_research_proposal_for_request(
        self, planner_request_id: str
    ) -> StrategicResearchProposal | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM strategic_research_proposals "
                "WHERE source_planner_request_id=?",
                (planner_request_id,),
            ).fetchone()
        return None if row is None else self._strategic_research_proposal_from_row(row)

    def list_strategic_research_proposals(
        self, game_session_id: str
    ) -> list[StrategicResearchProposal]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM strategic_research_proposals "
                "WHERE game_id=? ORDER BY proposal_id",
                (game_session_id,),
            ).fetchall()
        return [self._strategic_research_proposal_from_row(row) for row in rows]

    @classmethod
    def _strategic_proposal_wait_resume_request_from_row(
        cls, row: Mapping[str, Any]
    ) -> StrategicProposalWaitResumeRequest:
        normalized = cls._normalize_strategic_proposal_wait_resume_request_row(row)
        if dict(row) != normalized:
            raise ValueError("Strategic Proposal Resume Request row is not canonical")
        return StrategicProposalWaitResumeRequest.model_validate_json(
            str(normalized["request_json"])
        )

    @classmethod
    def _insert_strategic_proposal_wait_resume_request_in_connection(
        cls,
        conn: sqlite3.Connection,
        request: StrategicProposalWaitResumeRequest,
    ) -> None:
        row = cls._normalize_strategic_proposal_wait_resume_request_row(
            {
                "resume_request_id": request.resume_request_id,
                "game_id": request.game_session_id,
                "proposal_id": request.proposal_id,
                "planner_request_id": request.planner_request_id,
                "proposal_ready_tick_id": request.proposal_ready_tick_id,
                "target_kind": request.target_kind.value,
                "expected_base_revision": request.expected_base_revision,
                "request_json": request.model_dump_json(),
                "requested_at": request.requested_at.isoformat(),
            }
        )
        columns = (
            "resume_request_id",
            "game_id",
            "proposal_id",
            "planner_request_id",
            "proposal_ready_tick_id",
            "target_kind",
            "expected_base_revision",
            "request_json",
            "requested_at",
        )
        conn.execute(
            """
            INSERT INTO strategic_proposal_wait_resume_requests(
                resume_request_id, game_id, proposal_id, planner_request_id,
                proposal_ready_tick_id, target_kind, expected_base_revision,
                request_json, requested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(row[column] for column in columns),
        )

    def get_strategic_proposal_wait_resume_request(
        self, resume_request_id: str
    ) -> StrategicProposalWaitResumeRequest | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM strategic_proposal_wait_resume_requests "
                "WHERE resume_request_id=?",
                (resume_request_id,),
            ).fetchone()
        return (
            None
            if row is None
            else self._strategic_proposal_wait_resume_request_from_row(row)
        )

    def strategic_proposal_wait_resume_request_for_proposal(
        self, proposal_id: str
    ) -> StrategicProposalWaitResumeRequest | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM strategic_proposal_wait_resume_requests "
                "WHERE proposal_id=?",
                (proposal_id,),
            ).fetchone()
        return (
            None
            if row is None
            else self._strategic_proposal_wait_resume_request_from_row(row)
        )

    def list_strategic_proposal_wait_resume_requests(
        self, game_session_id: str
    ) -> list[StrategicProposalWaitResumeRequest]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM strategic_proposal_wait_resume_requests "
                "WHERE game_id=? ORDER BY requested_at, resume_request_id",
                (game_session_id,),
            ).fetchall()
        return [
            self._strategic_proposal_wait_resume_request_from_row(row) for row in rows
        ]

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
        if key.startswith("human_wait:"):
            raise ValueError(
                "human_wait metadata must use the dedicated Human Wait APIs"
            )
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
            conn.execute("BEGIN IMMEDIATE")
            self._validate_phase1b_proposals_v10(conn)
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
            if context.get("wait_kind") == "strategic_request_terminated":
                if type(context.get("resume_requested")) is not bool:
                    raise ValueError(
                        "strategic Request wait resume_requested must be a bool"
                    )
                terminal_row = conn.execute(
                    "SELECT * FROM workflow_ticks WHERE tick_id=? AND game_id=?",
                    (context.get("terminal_tick_id"), game_id),
                ).fetchone()
                if terminal_row is None:
                    raise ValueError(
                        "strategic Request wait references no termination Tick"
                    )
                terminal = self._workflow_tick_from_row(dict(terminal_row))
                expected = {
                    "resume_policy": "explicit_only",
                    "planner_request_id": getattr(terminal, "planner_request_id", None),
                    "terminal_tick_id": getattr(terminal, "tick_id", None),
                    "terminal_status": getattr(
                        getattr(terminal, "terminal_status", None), "value", None
                    ),
                    "failure_category": getattr(terminal, "failure_category", None),
                }
                if not isinstance(terminal, StrategicRequestTerminatedTick) or any(
                    context.get(field) != value for field, value in expected.items()
                ):
                    raise ValueError(
                        "strategic Request wait disagrees with termination Tick"
                    )
                if context["resume_requested"] is True:
                    self._parse_audit_datetime(
                        context.get("resume_requested_at"),
                        "strategic Request wait resume_requested_at",
                    )
                    return True
                requested_at = max(datetime.now(UTC), terminal.completed_at)
                context.update(
                    {
                        "version": "human-wait/v1",
                        "resume_requested": True,
                        "resume_requested_at": requested_at.isoformat(),
                    }
                )
                self._set_meta_in_connection(conn, key, context)
                self._validate_phase1b_proposals_v10(conn)
                return True
            if (
                context.get("wait_kind") == "strategic_contract_proposal_ready"
                and context.get("resume_policy") == "explicit_only"
            ):
                if type(context.get("resume_requested")) is not bool:
                    raise ValueError(
                        "Proposal Human Wait resume_requested must be a bool"
                    )
                proposal_row = conn.execute(
                    "SELECT * FROM strategic_research_proposals "
                    "WHERE proposal_id=? AND game_id=?",
                    (context.get("proposal_id"), game_id),
                ).fetchone()
                if proposal_row is None:
                    raise ValueError("Proposal Human Wait references no Proposal")
                proposal = self._strategic_research_proposal_from_row(proposal_row)
                ready_ticks = []
                for tick_row in conn.execute(
                    "SELECT * FROM workflow_ticks WHERE game_id=?",
                    (game_id,),
                ).fetchall():
                    tick = self._workflow_tick_from_row(dict(tick_row))
                    if (
                        isinstance(tick, StrategicProposalReadyTick)
                        and tick.proposal_id == proposal.proposal_id
                    ):
                        ready_ticks.append(tick)
                if len(ready_ticks) != 1:
                    raise ValueError(
                        "Proposal Human Wait requires one Proposal-ready Tick"
                    )
                ready = ready_ticks[0]
                existing_row = conn.execute(
                    "SELECT * FROM strategic_proposal_wait_resume_requests "
                    "WHERE proposal_id=?",
                    (proposal.proposal_id,),
                ).fetchone()
                if context["resume_requested"] is True:
                    if existing_row is None:
                        raise ValueError(
                            "Proposal context claims resume without an audit record"
                        )
                    existing = self._strategic_proposal_wait_resume_request_from_row(
                        existing_row
                    )
                    if (
                        context.get("resume_request_id") != existing.resume_request_id
                        or context.get("proposal_ready_tick_id") != ready.tick_id
                    ):
                        raise ValueError(
                            "Proposal Resume Request context disagrees with audit"
                        )
                    return True
                if existing_row is not None:
                    raise ValueError(
                        "Proposal context omits an existing Resume Request"
                    )
                requested_at = max(datetime.now(UTC), ready.completed_at)
                resume_request = build_strategic_proposal_wait_resume_request(
                    game_session_id=game_id,
                    proposal_id=proposal.proposal_id,
                    planner_request_id=proposal.source_planner_request_id,
                    proposal_ready_tick_id=ready.tick_id,
                    target_kind=proposal.target_kind,
                    expected_base_revision=proposal.expected_base_revision,
                    requested_at=requested_at,
                )
                self._insert_strategic_proposal_wait_resume_request_in_connection(
                    conn, resume_request
                )
                context.update(
                    {
                        "version": "human-wait/v1",
                        "resume_requested": True,
                        "resume_requested_at": requested_at.isoformat(),
                        "resume_request_id": resume_request.resume_request_id,
                        "proposal_ready_tick_id": ready.tick_id,
                    }
                )
                self._set_meta_in_connection(conn, key, context)
                self._validate_phase1b_proposals_v10(conn)
                return True
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
        """Persist one lease approval decision without activating it in the UI path."""

        turn = int(self.get_meta("last_observed_turn", 0) or 0)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT lease_json FROM plan_leases WHERE game_id=? AND plan_lease_id=?",
                (game_id, plan_lease_id),
            ).fetchone()
            if row is None:
                return False, "plan lease was not found for this game"
            lease = PlanLease.model_validate_json(row["lease_json"])
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

    def reconcile_open_events(
        self,
        game_id: str,
        active_dedupe_keys: Iterable[str],
        turn: int,
    ) -> list[str]:
        """Resolve snapshot/rule events that disappeared from the current tick."""

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
        effective_auto_actions = (
            set(auto_action_types) if mode is ExecutionMode.AUTO else set()
        )
        with self._connect() as conn:
            self._save_plan_bundle_in_connection(
                conn,
                game_id,
                turn,
                bundle,
                mode=mode,
                auto_action_types=effective_auto_actions,
                observation_id=observation_id,
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
            self._validate_phase1b_proposals_v10(conn)

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
        if isinstance(
            tick,
            (
                StrategicProposalWaitErrorTick,
                StrategicRequestWaitErrorTick,
                StrategicRequestWaitResumedTick,
            ),
        ):
            raise ValueError(
                "Strategic wait transition Tick must be persisted atomically"
            )
        with self._connect() as conn:
            self._insert_workflow_tick_in_connection(conn, tick)
            self._validate_phase1b_proposals_v10(conn)

    @classmethod
    def _validate_strategic_resume_transition_in_connection(
        cls,
        conn: sqlite3.Connection,
        tick: WorkflowTick,
        human_wait_context: dict[str, Any] | None,
    ) -> None:
        if not isinstance(tick, StrategicProposalWaitResumedTick):
            return
        if human_wait_context is not None:
            raise ValueError("Proposal wait resume must clear Human Wait context")
        cls._validate_phase1b_proposals_v10(conn)
        state_row = conn.execute(
            "SELECT state FROM runtime_state WHERE game_id=?",
            (tick.game_session_id,),
        ).fetchone()
        if state_row is None or state_row["state"] != RuntimeState.AWAITING_HUMAN.value:
            raise ValueError("Proposal wait resume requires AWAITING_HUMAN")
        wait_row = conn.execute(
            "SELECT value_json FROM workflow_meta WHERE key=?",
            (cls._human_wait_meta_key(tick.game_session_id),),
        ).fetchone()
        if wait_row is None:
            raise ValueError("Proposal wait resume requires Human Wait context")
        context = cls._load(wait_row["value_json"])
        if not isinstance(context, dict):
            raise ValueError("Proposal Human Wait context must be an object")
        expected = {
            "wait_kind": "strategic_contract_proposal_ready",
            "resume_policy": "explicit_only",
            "resume_requested": True,
            "resume_request_id": tick.resume_request_id,
            "proposal_ready_tick_id": tick.proposal_ready_tick_id,
            "planner_request_id": tick.planner_request_id,
            "proposal_id": tick.proposal_id,
            "target_kind": tick.target_kind.value,
            "expected_base_revision": tick.expected_base_revision,
        }
        if any(context.get(key) != value for key, value in expected.items()):
            raise ValueError("Proposal Resume Tick disagrees with Human Wait context")
        resume_row = conn.execute(
            "SELECT * FROM strategic_proposal_wait_resume_requests "
            "WHERE resume_request_id=?",
            (tick.resume_request_id,),
        ).fetchone()
        if resume_row is None:
            raise ValueError(
                "Proposal Resume Tick requires an immutable Resume Request"
            )
        resume_request = cls._strategic_proposal_wait_resume_request_from_row(
            resume_row
        )
        if (
            resume_request.game_session_id != tick.game_session_id
            or resume_request.proposal_id != tick.proposal_id
            or resume_request.planner_request_id != tick.planner_request_id
            or resume_request.proposal_ready_tick_id != tick.proposal_ready_tick_id
            or resume_request.target_kind is not tick.target_kind
            or resume_request.expected_base_revision != tick.expected_base_revision
            or tick.completed_at < resume_request.requested_at
        ):
            raise ValueError("Proposal Resume Tick disagrees with Resume Request")
        ready_row = conn.execute(
            "SELECT * FROM workflow_ticks WHERE tick_id=?",
            (tick.proposal_ready_tick_id,),
        ).fetchone()
        if ready_row is None:
            raise ValueError("Proposal Resume Tick references no Ready Tick")
        ready = cls._workflow_tick_from_row(dict(ready_row))
        if (
            not isinstance(ready, StrategicProposalReadyTick)
            or ready.game_session_id != tick.game_session_id
            or ready.proposal_id != tick.proposal_id
        ):
            raise ValueError("Proposal Resume Tick references the wrong Ready Tick")
        if tick.started_at < ready.completed_at:
            raise ValueError("Proposal Resume Tick precedes the Proposal Ready Tick")

    @classmethod
    def _preserve_concurrent_strategic_resume_context_in_connection(
        cls,
        conn: sqlite3.Connection,
        tick: WorkflowTick,
        candidate: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not isinstance(
            tick,
            (
                AwaitingHumanTick,
                StrategicProposalWaitErrorTick,
                StrategicRequestWaitErrorTick,
            ),
        ):
            return candidate
        row = conn.execute(
            "SELECT value_json FROM workflow_meta WHERE key=?",
            (cls._human_wait_meta_key(tick.game_session_id),),
        ).fetchone()
        if row is None:
            return candidate
        current = cls._load(str(row["value_json"]))
        if not isinstance(current, dict):
            return candidate
        is_proposal_resume = (
            current.get("wait_kind") == "strategic_contract_proposal_ready"
            and current.get("resume_policy") == "explicit_only"
            and current.get("resume_requested") is True
        )
        is_terminal_resume = (
            current.get("wait_kind") == "strategic_request_terminated"
            and current.get("resume_policy") == "explicit_only"
            and current.get("resume_requested") is True
        )
        if not (is_proposal_resume or is_terminal_resume):
            return candidate
        preserved = dict(current)
        if candidate is not None and isinstance(candidate.get("blocking_reason"), str):
            preserved["blocking_reason"] = candidate["blocking_reason"]
        return preserved

    @classmethod
    def _validate_strategic_request_wait_transition_in_connection(
        cls,
        conn: sqlite3.Connection,
        tick: WorkflowTick,
        human_wait_context: dict[str, Any] | None,
    ) -> None:
        state_row = conn.execute(
            "SELECT state FROM runtime_state WHERE game_id=?",
            (tick.game_session_id,),
        ).fetchone()
        wait_row = conn.execute(
            "SELECT value_json FROM workflow_meta WHERE key=?",
            (cls._human_wait_meta_key(tick.game_session_id),),
        ).fetchone()
        current = None if wait_row is None else cls._load(str(wait_row["value_json"]))
        active = (
            state_row is not None
            and state_row["state"] == RuntimeState.AWAITING_HUMAN.value
            and isinstance(current, dict)
            and current.get("wait_kind") == "strategic_request_terminated"
            and current.get("resume_policy") == "explicit_only"
        )
        is_special_tick = isinstance(
            tick, (StrategicRequestWaitResumedTick, StrategicRequestWaitErrorTick)
        )
        if not active:
            if is_special_tick:
                raise ValueError(
                    "strategic Request wait transition requires an active wait"
                )
            return
        if tick.starting_runtime_state is not RuntimeState.AWAITING_HUMAN:
            raise ValueError(
                "strategic Request wait transition must start AWAITING_HUMAN"
            )
        expected = {
            "planner_request_id": getattr(tick, "planner_request_id", None),
            "terminal_tick_id": getattr(tick, "terminal_tick_id", None),
            "terminal_status": getattr(
                getattr(tick, "terminal_status", None), "value", None
            ),
        }
        if isinstance(tick, StrategicRequestWaitResumedTick):
            if human_wait_context is not None:
                raise ValueError(
                    "strategic Request wait resume must clear Human Wait context"
                )
            expected["resume_requested"] = True
            if any(current.get(field) != value for field, value in expected.items()):
                raise ValueError(
                    "strategic Request wait-resumed Tick disagrees with context"
                )
            requested_at = cls._parse_audit_datetime(
                current.get("resume_requested_at"),
                "strategic Request wait resume_requested_at",
            )
            if tick.resumed_at != requested_at:
                raise ValueError(
                    "strategic Request wait-resumed Tick has the wrong resume time"
                )
            existing_rows = conn.execute(
                "SELECT * FROM workflow_ticks WHERE game_id=? AND outcome=?",
                (
                    tick.game_session_id,
                    TickOutcomeKind.STRATEGIC_REQUEST_WAIT_RESUMED.value,
                ),
            ).fetchall()
            if any(
                isinstance(
                    existing := cls._workflow_tick_from_row(dict(row)),
                    StrategicRequestWaitResumedTick,
                )
                and existing.planner_request_id == tick.planner_request_id
                for row in existing_rows
            ):
                raise ValueError("strategic Request wait already has a resumed Tick")
            return
        if tick.ending_runtime_state is not RuntimeState.AWAITING_HUMAN:
            raise ValueError(
                "only a strategic Request wait-resumed Tick may leave the wait"
            )
        if not isinstance(tick, (AwaitingHumanTick, StrategicRequestWaitErrorTick)):
            raise ValueError(
                "strategic Request wait only accepts waiting or diagnostic Ticks"
            )
        stable_identity = {
            "wait_kind": "strategic_request_terminated",
            "resume_policy": "explicit_only",
            "planner_request_id": current.get("planner_request_id"),
            "terminal_tick_id": current.get("terminal_tick_id"),
            "terminal_status": current.get("terminal_status"),
            "failure_category": current.get("failure_category"),
        }
        if not isinstance(human_wait_context, dict) or any(
            human_wait_context.get(field) != value
            for field, value in stable_identity.items()
        ):
            raise ValueError(
                "strategic Request wait Tick requires matching persistence context"
            )
        if isinstance(tick, StrategicRequestWaitErrorTick) and (
            tick.planner_request_id != current.get("planner_request_id")
            or tick.terminal_tick_id != current.get("terminal_tick_id")
            or tick.terminal_status.value != current.get("terminal_status")
            or tick.failure_category != current.get("failure_category")
        ):
            raise ValueError("strategic Request wait-error Tick disagrees with context")

    @classmethod
    def _validate_strategic_wait_error_transition_in_connection(
        cls,
        conn: sqlite3.Connection,
        tick: WorkflowTick,
        human_wait_context: dict[str, Any] | None,
    ) -> None:
        if not isinstance(tick, StrategicProposalWaitErrorTick):
            return
        state_row = conn.execute(
            "SELECT state FROM runtime_state WHERE game_id=?",
            (tick.game_session_id,),
        ).fetchone()
        if state_row is None or state_row["state"] != RuntimeState.AWAITING_HUMAN.value:
            raise ValueError(
                "Proposal wait-error Tick requires an active AWAITING_HUMAN wait"
            )
        wait_row = conn.execute(
            "SELECT value_json FROM workflow_meta WHERE key=?",
            (cls._human_wait_meta_key(tick.game_session_id),),
        ).fetchone()
        if wait_row is None:
            raise ValueError(
                "Proposal wait-error Tick requires active Human Wait context"
            )
        current = cls._load(str(wait_row["value_json"]))
        expected = {
            "wait_kind": "strategic_contract_proposal_ready",
            "resume_policy": "explicit_only",
            "proposal_ready_tick_id": tick.proposal_ready_tick_id,
            "planner_request_id": tick.planner_request_id,
            "proposal_id": tick.proposal_id,
            "target_kind": tick.target_kind.value,
            "expected_base_revision": tick.expected_base_revision,
        }
        if not isinstance(current, dict) or any(
            current.get(key) != value for key, value in expected.items()
        ):
            raise ValueError(
                "Proposal wait-error Tick disagrees with active Human Wait context"
            )
        if not isinstance(human_wait_context, dict) or any(
            human_wait_context.get(key) != value for key, value in expected.items()
        ):
            raise ValueError(
                "Proposal wait-error Tick requires matching persistence context"
            )
        resumed_rows = conn.execute(
            "SELECT * FROM workflow_ticks WHERE game_id=? AND outcome=?",
            (
                tick.game_session_id,
                TickOutcomeKind.STRATEGIC_PROPOSAL_WAIT_RESUMED.value,
            ),
        ).fetchall()
        if any(
            isinstance(
                resumed := cls._workflow_tick_from_row(dict(row)),
                StrategicProposalWaitResumedTick,
            )
            and resumed.proposal_id == tick.proposal_id
            for row in resumed_rows
        ):
            raise ValueError("Proposal wait-error Tick cannot follow wait resume")

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
            conn.execute("BEGIN IMMEDIATE")
            self._validate_strategic_resume_transition_in_connection(
                conn, tick, human_wait_context
            )
            self._validate_strategic_wait_error_transition_in_connection(
                conn, tick, human_wait_context
            )
            self._validate_strategic_request_wait_transition_in_connection(
                conn, tick, human_wait_context
            )
            human_wait_context = (
                self._preserve_concurrent_strategic_resume_context_in_connection(
                    conn, tick, human_wait_context
                )
            )
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
            self._validate_phase1b_proposals_v10(conn)
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
    def _planner_request_creation_definition(
        request: PlannerRequest,
    ) -> tuple[Any, ...]:
        return (
            request.planner_request_id,
            request.game_session_id,
            request.turn_number,
            request.observation_id,
            request.target,
            request.input_projection_hash,
            request.input_projection_version,
            canonical_json(request.input_projection),
            canonical_json(request.request_payload),
            request.plan_revision_refs,
            request.policy_revision,
            request.approval_contract_hash,
            request.allowed_actions_hash,
            canonical_json(request.model_settings),
            request.created_at,
            request.context_bytes,
        )

    @staticmethod
    def _classify_planner_request_response(
        request: PlannerRequest,
    ) -> _PlannerResponseFacts:
        response_statuses = {
            PlannerRequestStatus.COMPLETED,
            PlannerRequestStatus.PARTIALLY_COMPLETED,
            PlannerRequestStatus.REJECTED,
        }
        if (
            request.response_evidence_compatibility
            is PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD
        ):
            return _PlannerResponseFacts.LEGACY_V7_MISSING_PAYLOAD
        if request.status not in response_statuses:
            return _PlannerResponseFacts.NO_RESPONSE
        response_evidence = {
            "response_payload": request.response_payload,
            "response_hash": request.response_hash,
            "validation_result": request.validation_result,
        }
        if not any(value is not None for value in response_evidence.values()) and (
            request.status is PlannerRequestStatus.REJECTED
            and request.failure_category == "planner_contract_failure"
        ):
            return _PlannerResponseFacts.CONTRACT_SCHEMA_FAILURE

        required_evidence = {
            **response_evidence,
            "completed_at": request.completed_at,
        }
        missing = [field for field, value in required_evidence.items() if value is None]
        if missing:
            raise ValueError(
                f"PlannerRequest completion requires: {', '.join(sorted(missing))}"
            )
        PlannerRequest.model_validate_json(request.model_dump_json())
        match request.target.kind:
            case PlannerRequestTargetKind.LEGACY_DECISION_GROUP:
                canonical_payload = canonical_workflow_plan_bundle_payload(
                    request.response_payload
                )
                contract_name = "WorkflowPlanBundle"
            case (
                PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION
                | PlannerRequestTargetKind.MISSION_GRAPH_REPAIR
            ):
                canonical_payload = (
                    canonical_strategic_research_proposal_response_payload(
                        request.response_payload
                    )
                )
                contract_name = "StrategicResearchProposalResponse"
            case _:
                raise ValueError("unsupported PlannerRequest response target")
        if canonical_json(request.response_payload) != canonical_json(
            canonical_payload
        ):
            raise ValueError(
                f"planner response_payload must be a canonical {contract_name}"
            )
        if request.response_hash != canonical_json_hash(canonical_payload):
            raise ValueError(
                f"planner response_hash must use the canonical {contract_name} payload"
            )
        return _PlannerResponseFacts.CANONICAL_RESPONSE

    @classmethod
    def _validate_contract_schema_failure_attempt(
        cls,
        request: PlannerRequest,
        attempt_rows: Iterable[Mapping[str, Any]],
    ) -> None:
        if (
            cls._classify_planner_request_response(request)
            is not _PlannerResponseFacts.CONTRACT_SCHEMA_FAILURE
        ):
            return
        if request.provider_attempt_count < 1:
            raise ValueError(
                "planner contract schema failure requires provider_attempt_count >= 1"
            )

        request_attempts: list[dict[str, Any]] = []
        for row in attempt_rows:
            normalized = cls._normalize_provider_attempt_row(row)
            if normalized["planner_request_id"] == request.planner_request_id:
                request_attempts.append(normalized)
        if not request_attempts:
            raise ValueError(
                "planner contract schema failure requires a matching final "
                "ProviderAttempt"
            )
        maximum_attempt_number = max(
            int(row["attempt_number"]) for row in request_attempts
        )
        if request.provider_attempt_count != maximum_attempt_number:
            raise ValueError(
                "planner contract schema failure requires provider_attempt_count "
                "to match the maximum ProviderAttempt attempt_number"
            )
        final_row = next(
            row
            for row in request_attempts
            if int(row["attempt_number"]) == maximum_attempt_number
        )
        if final_row["game_id"] != request.game_session_id:
            raise ValueError(
                "planner contract schema failure final ProviderAttempt belongs "
                "to another game"
            )
        final_attempt = ProviderAttempt.model_validate_json(
            str(final_row["attempt_json"])
        )
        if (
            final_attempt.status is not ProviderAttemptStatus.SUCCEEDED
            or final_attempt.completed_at is None
        ):
            raise ValueError(
                "planner contract schema failure requires a SUCCEEDED final "
                "ProviderAttempt"
            )

    @staticmethod
    def _save_planner_request_in_connection(
        conn: sqlite3.Connection, request: PlannerRequest
    ) -> None:
        existing_row = conn.execute(
            """
            SELECT * FROM logical_planner_requests
            WHERE planner_request_id=?
            """,
            (request.planner_request_id,),
        ).fetchone()
        if existing_row is None:
            response_facts = WorkflowStore._classify_planner_request_response(request)
            if response_facts is _PlannerResponseFacts.CONTRACT_SCHEMA_FAILURE:
                raise ValueError(
                    "planner contract schema failure can only be committed by "
                    "a lifecycle transition or replay recovery"
                )
            if response_facts is _PlannerResponseFacts.LEGACY_V7_MISSING_PAYLOAD:
                raise ValueError(
                    "legacy response compatibility can only be introduced by "
                    "migration or replay normalization"
                )
            if (
                request.target.kind is PlannerRequestTargetKind.LEGACY_DECISION_GROUP
                and request.decision_group_id is None
            ):
                raise ValueError("new legacy PlannerRequest requires a DecisionGroup")
        else:
            existing = WorkflowStore._planner_request_from_row(existing_row)
            if existing.status in TERMINAL_PLANNER_STATUSES:
                if request != existing:
                    raise ValueError("terminal PlannerRequest row is immutable")
            elif WorkflowStore._planner_request_creation_definition(
                request
            ) != WorkflowStore._planner_request_creation_definition(existing):
                raise ValueError("PlannerRequest creation definition is immutable")
            else:
                response_facts = WorkflowStore._classify_planner_request_response(
                    request
                )
                if (
                    response_facts is _PlannerResponseFacts.LEGACY_V7_MISSING_PAYLOAD
                    and existing.response_evidence_compatibility is None
                ):
                    raise ValueError(
                        "legacy response compatibility can only be introduced by "
                        "migration or replay normalization"
                    )

            WorkflowStore._validate_contract_schema_failure_attempt(
                request,
                conn.execute(
                    """
                    SELECT * FROM provider_attempts
                    WHERE planner_request_id=?
                    ORDER BY attempt_number
                    """,
                    (request.planner_request_id,),
                ).fetchall(),
            )

        conn.execute(
            """
            INSERT INTO logical_planner_requests(
                planner_request_id, game_id, request_target_kind,
                request_target_key, decision_group_id, turn, status,
                input_projection_hash, input_projection_version,
                decision_gap_ids_json, request_json, created_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(planner_request_id) DO UPDATE SET
                status=excluded.status,
                request_json=excluded.request_json,
                completed_at=excluded.completed_at
            """,
            (
                request.planner_request_id,
                request.game_session_id,
                request.target.kind.value,
                request.target.target_key,
                request.decision_group_id,
                request.turn_number,
                request.status.value,
                request.input_projection_hash,
                request.input_projection_version,
                canonical_json(list(request.decision_gap_ids)),
                canonical_json(request.model_dump(mode="json")),
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
            SELECT * FROM provider_attempts
            WHERE planner_request_id=? AND status=?
            ORDER BY attempt_number
            """,
            (
                request.planner_request_id,
                ProviderAttemptStatus.STARTED.value,
            ),
        ).fetchall()
        for row in rows:
            started = WorkflowStore._provider_attempt_from_row(conn, row)
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

    @staticmethod
    def _is_clean_initial_strategic_request(request: PlannerRequest) -> bool:
        return (
            request.status is PlannerRequestStatus.PENDING
            and request.provider_attempt_count == 0
            and request.information_round_count == 0
            and request.pending_information_requests == ()
            and not request.information_results
            and request.completed_at is None
            and request.response_payload is None
            and request.response_hash is None
            and request.validation_result is None
            and request.response_evidence_compatibility is None
            and request.failure_category is None
            and request.next_retry_at is None
        )

    def save_planner_request(self, request: PlannerRequest) -> None:
        with self._connect() as conn:
            existing_row = conn.execute(
                "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
                (request.planner_request_id,),
            ).fetchone()
            existing = (
                None
                if existing_row is None
                else self._planner_request_from_row(existing_row)
            )
            strategic_candidate = request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
            strategic_existing = (
                existing is not None
                and existing.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
            )
            if strategic_candidate or strategic_existing:
                if existing is None:
                    if not strategic_candidate or not (
                        self._is_clean_initial_strategic_request(request)
                    ):
                        raise ValueError(
                            "new strategic PlannerRequest must be a clean PENDING request"
                        )
                    self._save_planner_request_in_connection(conn, request)
                    self._validate_phase1b_proposals_v10(conn)
                    return
                if request != existing:
                    raise ValueError(
                        "strategic PlannerRequest lifecycle requires an atomic "
                        "Runtime transaction"
                    )
                self._validate_phase1b_proposals_v10(conn)
                return
            self._save_planner_request_in_connection(conn, request)
            self._validate_phase1b_proposals_v10(conn)

    def get_planner_request(self, planner_request_id: str) -> PlannerRequest | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM logical_planner_requests
                WHERE planner_request_id=?
                """,
                (planner_request_id,),
            ).fetchone()
        return None if row is None else self._planner_request_from_row(row)

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
                SELECT * FROM logical_planner_requests
                WHERE game_id=? AND status NOT IN ({placeholders})
                ORDER BY created_at LIMIT 1
                """,
                (game_id, *terminal),
            ).fetchone()
        return None if row is None else self._planner_request_from_row(row)

    def planner_request_for_input(
        self,
        game_id: str,
        request_target_key: str,
        input_projection_hash: str,
    ) -> PlannerRequest | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM logical_planner_requests
                WHERE game_id=? AND request_target_key=?
                  AND input_projection_hash=?
                """,
                (game_id, request_target_key, input_projection_hash),
            ).fetchone()
        return None if row is None else self._planner_request_from_row(row)

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
        budget while the original request remains available for audit. Contract
        migrations also release the budget after every attempt is durably
        abandoned; all other provider-attempt history remains budget-consuming.
        """

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT request.*,
                       COUNT(attempt.provider_attempt_id) AS attempt_count,
                       SUM(
                           CASE
                               WHEN attempt.provider_attempt_id IS NOT NULL
                                AND attempt.status != ?
                               THEN 1 ELSE 0
                           END
                       ) AS non_abandoned_attempt_count
                FROM logical_planner_requests AS request
                LEFT JOIN provider_attempts AS attempt
                  ON attempt.planner_request_id=request.planner_request_id
                WHERE request.game_id=? AND request.turn=?
                GROUP BY request.planner_request_id
                """,
                (ProviderAttemptStatus.ABANDONED.value, game_id, turn),
            ).fetchall()

        consumed = 0
        for row in rows:
            request = self._planner_request_from_row(row)
            attempt_count = int(row["attempt_count"])
            if request.status is PlannerRequestStatus.SUPERSEDED:
                if attempt_count == 0:
                    continue
                if (
                    request.failure_category == "planner_contract_revision_migration"
                    and int(row["non_abandoned_attempt_count"]) == 0
                ):
                    continue
            consumed += 1
        return consumed

    @staticmethod
    def _parse_audit_datetime(value: Any, field_name: str) -> datetime | None:
        if value is None:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} is not a datetime") from exc

    @classmethod
    def _normalize_provider_attempt_row(
        cls,
        row: Mapping[str, Any],
        *,
        expected_game_id: str | None = None,
    ) -> dict[str, Any]:
        normalized = dict(row)
        required = {
            "provider_attempt_id",
            "game_id",
            "planner_request_id",
            "attempt_number",
            "provider_request_id",
            "status",
            "attempt_json",
            "started_at",
            "completed_at",
        }
        missing = required - normalized.keys()
        if missing:
            raise ValueError(
                f"ProviderAttempt row is missing columns: {sorted(missing)}"
            )
        try:
            attempt = ProviderAttempt.model_validate_json(
                str(normalized["attempt_json"])
            )
        except Exception as exc:
            raise ValueError("invalid ProviderAttempt JSON") from exc
        checks = {
            "provider_attempt_id": attempt.provider_attempt_id,
            "planner_request_id": attempt.planner_request_id,
            "attempt_number": attempt.attempt_number,
            "provider_request_id": attempt.provider_request_id,
            "status": attempt.status.value,
        }
        for column, expected in checks.items():
            actual = normalized[column]
            if column == "attempt_number":
                try:
                    actual = int(actual)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "ProviderAttempt attempt_number is not an integer"
                    ) from exc
            elif not isinstance(actual, str):
                actual = str(actual)
            if actual != expected:
                raise ValueError(
                    f"ProviderAttempt {attempt.provider_attempt_id} {column} "
                    "conflicts with attempt_json"
                )
        for column, expected in (
            ("started_at", attempt.started_at),
            ("completed_at", attempt.completed_at),
        ):
            if cls._parse_audit_datetime(normalized[column], column) != expected:
                raise ValueError(
                    f"ProviderAttempt {attempt.provider_attempt_id} {column} "
                    "conflicts with attempt_json"
                )
        game_id = str(normalized["game_id"])
        if expected_game_id is not None and game_id != expected_game_id:
            raise ValueError("ProviderAttempt belongs to another game")
        normalized.update(
            {
                "provider_attempt_id": attempt.provider_attempt_id,
                "game_id": game_id,
                "planner_request_id": attempt.planner_request_id,
                "attempt_number": attempt.attempt_number,
                "provider_request_id": attempt.provider_request_id,
                "status": attempt.status.value,
                "attempt_json": canonical_json(attempt.model_dump(mode="json")),
                "started_at": attempt.started_at.isoformat(),
                "completed_at": (
                    None
                    if attempt.completed_at is None
                    else attempt.completed_at.isoformat()
                ),
            }
        )
        return normalized

    @classmethod
    def _provider_attempt_from_row(
        cls, conn: sqlite3.Connection, row: Mapping[str, Any]
    ) -> ProviderAttempt:
        normalized = cls._normalize_provider_attempt_row(row)
        parent = conn.execute(
            """
            SELECT game_id FROM logical_planner_requests
            WHERE planner_request_id=?
            """,
            (normalized["planner_request_id"],),
        ).fetchone()
        if parent is None:
            raise ValueError("ProviderAttempt parent PlannerRequest does not exist")
        if str(parent["game_id"]) != normalized["game_id"]:
            raise ValueError(
                "ProviderAttempt game_id conflicts with parent PlannerRequest"
            )
        return ProviderAttempt.model_validate_json(normalized["attempt_json"])

    @staticmethod
    def _provider_attempt_creation_definition(
        game_id: str, attempt: ProviderAttempt
    ) -> tuple[Any, ...]:
        return (
            attempt.provider_attempt_id,
            game_id,
            attempt.planner_request_id,
            attempt.attempt_number,
            attempt.provider_request_id,
            attempt.started_at,
        )

    @classmethod
    def _save_provider_attempt_in_connection(
        cls,
        conn: sqlite3.Connection,
        game_id: str,
        attempt: ProviderAttempt,
        *,
        validate_aggregate: bool = True,
    ) -> None:
        parent = conn.execute(
            """
            SELECT * FROM logical_planner_requests
            WHERE planner_request_id=?
            """,
            (attempt.planner_request_id,),
        ).fetchone()
        if parent is None:
            raise ValueError("ProviderAttempt parent PlannerRequest does not exist")
        if str(parent["game_id"]) != game_id:
            raise ValueError(
                "ProviderAttempt game_id conflicts with parent PlannerRequest"
            )
        parent_request = cls._planner_request_from_row(parent)
        existing_row = conn.execute(
            """
            SELECT * FROM provider_attempts WHERE provider_attempt_id=?
            """,
            (attempt.provider_attempt_id,),
        ).fetchone()
        if (
            parent_request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
            and existing_row is None
            and attempt.status is not ProviderAttemptStatus.STARTED
        ):
            raise ValueError(
                "new strategic ProviderAttempt must be persisted as STARTED"
            )
        if existing_row is not None:
            existing = cls._provider_attempt_from_row(conn, existing_row)
            existing_game_id = str(existing_row["game_id"])
            if cls._provider_attempt_creation_definition(
                game_id, attempt
            ) != cls._provider_attempt_creation_definition(existing_game_id, existing):
                raise ValueError("ProviderAttempt creation identity is immutable")
            if existing.status is not ProviderAttemptStatus.STARTED:
                if attempt != existing:
                    raise ValueError("terminal ProviderAttempt is immutable")
            elif (
                attempt.status is ProviderAttemptStatus.STARTED and attempt != existing
            ):
                raise ValueError(
                    "STARTED ProviderAttempt only allows a terminal transition"
                )
        row = cls._normalize_provider_attempt_row(
            {
                "provider_attempt_id": attempt.provider_attempt_id,
                "game_id": game_id,
                "planner_request_id": attempt.planner_request_id,
                "attempt_number": attempt.attempt_number,
                "provider_request_id": attempt.provider_request_id,
                "status": attempt.status.value,
                "attempt_json": attempt.model_dump_json(),
                "started_at": attempt.started_at.isoformat(),
                "completed_at": (
                    None
                    if attempt.completed_at is None
                    else attempt.completed_at.isoformat()
                ),
            },
            expected_game_id=game_id,
        )
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
                row["provider_attempt_id"],
                row["game_id"],
                row["planner_request_id"],
                row["attempt_number"],
                row["provider_request_id"],
                row["status"],
                row["attempt_json"],
                row["started_at"],
                row["completed_at"],
            ),
        )
        parent_request = cls._planner_request_from_row(
            conn.execute(
                "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
                (attempt.planner_request_id,),
            ).fetchone()
        )
        cls._validate_contract_schema_failure_attempt(
            parent_request,
            conn.execute(
                """
                SELECT * FROM provider_attempts
                WHERE planner_request_id=?
                ORDER BY attempt_number
                """,
                (attempt.planner_request_id,),
            ).fetchall(),
        )
        if validate_aggregate:
            cls._validate_phase1b_proposals_v10(conn)

    def save_provider_attempt(self, game_id: str, attempt: ProviderAttempt) -> None:
        with self._connect() as conn:
            parent_row = conn.execute(
                "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
                (attempt.planner_request_id,),
            ).fetchone()
            if parent_row is None:
                raise ValueError("ProviderAttempt parent PlannerRequest does not exist")
            parent = self._planner_request_from_row(parent_row)
            if parent.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS:
                existing_row = conn.execute(
                    "SELECT * FROM provider_attempts WHERE provider_attempt_id=?",
                    (attempt.provider_attempt_id,),
                ).fetchone()
                if existing_row is None:
                    raise ValueError(
                        "new strategic ProviderAttempt requires "
                        "start_provider_attempt()"
                    )
                existing = self._provider_attempt_from_row(conn, existing_row)
                if attempt == existing:
                    self._validate_phase1b_proposals_v10(conn)
                    return
                if not (
                    existing.status is ProviderAttemptStatus.STARTED
                    and attempt.status is ProviderAttemptStatus.FAILED
                ):
                    raise ValueError(
                        "strategic ProviderAttempt success requires an atomic "
                        "Runtime transaction"
                    )
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
            request_row = conn.execute(
                "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
                (request.planner_request_id,),
            ).fetchone()
            if request_row is None:
                raise ValueError("PlannerRequest does not exist")
            stored_request = self._planner_request_from_row(request_row)
            if (
                request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
                or stored_request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
            ):
                if request != stored_request:
                    raise ValueError(
                        "strategic ProviderAttempt must start from the current Request"
                    )
                if request.status not in {
                    PlannerRequestStatus.PENDING,
                    PlannerRequestStatus.IN_PROGRESS,
                    PlannerRequestStatus.BACKOFF,
                    PlannerRequestStatus.READY_TO_CONTINUE,
                }:
                    raise ValueError(
                        "strategic ProviderAttempt cannot start from this Request status"
                    )
                if request.status is PlannerRequestStatus.BACKOFF and (
                    request.next_retry_at is None
                    or attempt.started_at < request.next_retry_at
                ):
                    raise ValueError(
                        "strategic ProviderAttempt cannot start before next_retry_at"
                    )
            rows = conn.execute(
                """
                SELECT * FROM provider_attempts
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
                interrupted = self._provider_attempt_from_row(conn, row).model_copy(
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
                self._save_provider_attempt_in_connection(
                    conn, game_id, interrupted, validate_aggregate=False
                )
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
            updates = {
                "status": PlannerRequestStatus.IN_PROGRESS,
                "provider_attempt_count": attempt.attempt_number,
            }
            if request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS:
                updates.update(
                    {
                        "failure_category": None,
                        "next_retry_at": None,
                    }
                )
            in_progress = request.model_copy(update=updates)
            self._save_planner_request_in_connection(conn, in_progress)
            self._save_provider_attempt_in_connection(conn, game_id, attempt)
        return in_progress

    def list_provider_attempts(self, planner_request_id: str) -> list[ProviderAttempt]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM provider_attempts
                WHERE planner_request_id=? ORDER BY attempt_number
                """,
                (planner_request_id,),
            ).fetchall()
            return [self._provider_attempt_from_row(conn, row) for row in rows]

    @classmethod
    def _normalize_information_round_row(
        cls,
        row: Mapping[str, Any],
        *,
        expected_game_id: str | None = None,
    ) -> dict[str, Any]:
        normalized = dict(row)
        required = {
            "information_round_id",
            "game_id",
            "planner_request_id",
            "round_number",
            "status",
            "round_json",
            "requested_at",
            "completed_at",
        }
        missing = required - normalized.keys()
        if missing:
            raise ValueError(
                f"InformationRound row is missing columns: {sorted(missing)}"
            )
        try:
            round_record = InformationRound.model_validate_json(
                str(normalized["round_json"])
            )
        except Exception as exc:
            raise ValueError("invalid InformationRound JSON") from exc
        checks = {
            "information_round_id": round_record.information_round_id,
            "planner_request_id": round_record.planner_request_id,
            "round_number": round_record.round_number,
            "status": round_record.status.value,
        }
        for column, expected in checks.items():
            actual = normalized[column]
            if column == "round_number":
                try:
                    actual = int(actual)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "InformationRound round_number is not an integer"
                    ) from exc
            elif not isinstance(actual, str):
                actual = str(actual)
            if actual != expected:
                raise ValueError(
                    f"InformationRound {round_record.information_round_id} "
                    f"{column} conflicts with round_json"
                )
        for column, expected in (
            ("requested_at", round_record.requested_at),
            ("completed_at", round_record.completed_at),
        ):
            if cls._parse_audit_datetime(normalized[column], column) != expected:
                raise ValueError(
                    f"InformationRound {round_record.information_round_id} "
                    f"{column} conflicts with round_json"
                )
        game_id = str(normalized["game_id"])
        if expected_game_id is not None and game_id != expected_game_id:
            raise ValueError("InformationRound belongs to another game")
        normalized.update(
            {
                "information_round_id": round_record.information_round_id,
                "game_id": game_id,
                "planner_request_id": round_record.planner_request_id,
                "round_number": round_record.round_number,
                "status": round_record.status.value,
                "round_json": canonical_json(round_record.model_dump(mode="json")),
                "requested_at": round_record.requested_at.isoformat(),
                "completed_at": (
                    None
                    if round_record.completed_at is None
                    else round_record.completed_at.isoformat()
                ),
            }
        )
        return normalized

    @classmethod
    def _information_round_from_row(
        cls, conn: sqlite3.Connection, row: Mapping[str, Any]
    ) -> InformationRound:
        normalized = cls._normalize_information_round_row(row)
        parent = conn.execute(
            """
            SELECT game_id FROM logical_planner_requests
            WHERE planner_request_id=?
            """,
            (normalized["planner_request_id"],),
        ).fetchone()
        if parent is None:
            raise ValueError("InformationRound parent PlannerRequest does not exist")
        if str(parent["game_id"]) != normalized["game_id"]:
            raise ValueError(
                "InformationRound game_id conflicts with parent PlannerRequest"
            )
        return InformationRound.model_validate_json(normalized["round_json"])

    @staticmethod
    def _information_round_creation_definition(
        game_id: str, round_record: InformationRound
    ) -> tuple[Any, ...]:
        return (
            round_record.information_round_id,
            game_id,
            round_record.planner_request_id,
            round_record.round_number,
            round_record.source_provider_attempt_id,
            round_record.source_provider_attempt_number,
            canonical_json(round_record.requests),
            round_record.requested_at,
        )

    @classmethod
    def _save_information_round_in_connection(
        cls,
        conn: sqlite3.Connection,
        game_id: str,
        round_record: InformationRound,
    ) -> None:
        parent = conn.execute(
            """
            SELECT game_id FROM logical_planner_requests
            WHERE planner_request_id=?
            """,
            (round_record.planner_request_id,),
        ).fetchone()
        if parent is None:
            raise ValueError("InformationRound parent PlannerRequest does not exist")
        if str(parent["game_id"]) != game_id:
            raise ValueError(
                "InformationRound game_id conflicts with parent PlannerRequest"
            )
        existing_row = conn.execute(
            """
            SELECT * FROM information_rounds WHERE information_round_id=?
            """,
            (round_record.information_round_id,),
        ).fetchone()
        if existing_row is not None:
            existing = cls._information_round_from_row(conn, existing_row)
            existing_game_id = str(existing_row["game_id"])
            if cls._information_round_creation_definition(
                game_id, round_record
            ) != cls._information_round_creation_definition(existing_game_id, existing):
                raise ValueError("InformationRound creation identity is immutable")
            if existing.status is not InformationRoundStatus.REQUESTED:
                if round_record != existing:
                    raise ValueError("terminal InformationRound is immutable")
            elif (
                round_record.status is InformationRoundStatus.REQUESTED
                and round_record != existing
            ):
                raise ValueError(
                    "REQUESTED InformationRound only allows a terminal transition"
                )
        row = cls._normalize_information_round_row(
            {
                "information_round_id": round_record.information_round_id,
                "game_id": game_id,
                "planner_request_id": round_record.planner_request_id,
                "round_number": round_record.round_number,
                "status": round_record.status.value,
                "round_json": round_record.model_dump_json(),
                "requested_at": round_record.requested_at.isoformat(),
                "completed_at": (
                    None
                    if round_record.completed_at is None
                    else round_record.completed_at.isoformat()
                ),
            },
            expected_game_id=game_id,
        )
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
                row["information_round_id"],
                row["game_id"],
                row["planner_request_id"],
                row["round_number"],
                row["status"],
                row["round_json"],
                row["requested_at"],
                row["completed_at"],
            ),
        )

    def save_information_round(
        self, game_id: str, round_record: InformationRound
    ) -> None:
        with self._connect() as conn:
            parent_row = conn.execute(
                "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
                (round_record.planner_request_id,),
            ).fetchone()
            if parent_row is None:
                raise ValueError(
                    "InformationRound parent PlannerRequest does not exist"
                )
            parent = self._planner_request_from_row(parent_row)
            if parent.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS:
                existing_row = conn.execute(
                    "SELECT * FROM information_rounds WHERE information_round_id=?",
                    (round_record.information_round_id,),
                ).fetchone()
                if existing_row is None:
                    raise ValueError(
                        "strategic InformationRound creation requires an atomic "
                        "Runtime transaction"
                    )
                existing = self._information_round_from_row(conn, existing_row)
                if str(existing_row["game_id"]) != game_id or round_record != existing:
                    raise ValueError(
                        "strategic InformationRound lifecycle requires an atomic "
                        "Runtime transaction"
                    )
                self._validate_phase1b_proposals_v10(conn)
                return
            self._save_information_round_in_connection(conn, game_id, round_record)
            self._validate_phase1b_proposals_v10(conn)

    def list_information_rounds(
        self, planner_request_id: str
    ) -> list[InformationRound]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM information_rounds
                WHERE planner_request_id=? ORDER BY round_number
                """,
                (planner_request_id,),
            ).fetchall()
            return [self._information_round_from_row(conn, row) for row in rows]

    @classmethod
    def _validate_strategic_request_transition_in_connection(
        cls,
        conn: sqlite3.Connection,
        tick: WorkflowTick,
        planner_request: PlannerRequest | None,
        provider_attempts: Sequence[ProviderAttempt],
        information_round: InformationRound | None,
        strategic_research_proposal: StrategicResearchProposal | None,
    ) -> None:
        referenced_request_id = getattr(tick, "planner_request_id", None)
        referenced_row = (
            None
            if not isinstance(referenced_request_id, str)
            else conn.execute(
                "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
                (referenced_request_id,),
            ).fetchone()
        )
        referenced_request = (
            None
            if referenced_row is None
            else cls._planner_request_from_row(referenced_row)
        )
        if planner_request is None:
            if (
                referenced_request is not None
                and referenced_request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
                and isinstance(
                    tick,
                    (
                        PlannerAttemptCompletedTick,
                        PlannerBackoffTick,
                        InformationRequestedTick,
                        InformationCollectedTick,
                        StrategicProposalReadyTick,
                        StrategicRequestTerminatedTick,
                    ),
                )
            ):
                raise ValueError(
                    "strategic PlannerRequest Tick requires its Request aggregate"
                )
            return

        existing_row = conn.execute(
            "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
            (planner_request.planner_request_id,),
        ).fetchone()
        existing = (
            None
            if existing_row is None
            else cls._planner_request_from_row(existing_row)
        )
        strategic = planner_request.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS or (
            existing is not None
            and existing.target.kind in STRATEGIC_PROPOSAL_TARGET_KINDS
        )
        if not strategic:
            return
        if (
            planner_request.target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS
            or planner_request.game_session_id != tick.game_session_id
        ):
            raise ValueError("strategic PlannerRequest aggregate identity disagrees")
        if existing is None:
            if (
                not isinstance(tick, LogicalPlannerRequestCreatedTick)
                or tick.planner_request_id != planner_request.planner_request_id
                or tick.request_target_kind is not planner_request.target.kind
                or not cls._is_clean_initial_strategic_request(planner_request)
                or provider_attempts
                or information_round is not None
                or strategic_research_proposal is not None
            ):
                raise ValueError(
                    "strategic PlannerRequest creation aggregate is invalid"
                )
            return
        if existing.target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS:
            raise ValueError("PlannerRequest target kind cannot change")
        if planner_request == existing:
            if (
                isinstance(tick, PlannerBackoffTick)
                and existing.status is PlannerRequestStatus.BACKOFF
                and tick.planner_request_id == existing.planner_request_id
                and not provider_attempts
                and information_round is None
                and strategic_research_proposal is None
            ):
                return
            raise ValueError(
                "strategic PlannerRequest aggregate has no valid lifecycle transition"
            )

        transition = (existing.status, planner_request.status)
        information_transitions = {
            (
                PlannerRequestStatus.IN_PROGRESS,
                PlannerRequestStatus.AWAITING_INFORMATION,
            ),
            (
                PlannerRequestStatus.AWAITING_INFORMATION,
                PlannerRequestStatus.READY_TO_CONTINUE,
            ),
            (
                PlannerRequestStatus.AWAITING_INFORMATION,
                PlannerRequestStatus.FAILED,
            ),
        }
        if transition in information_transitions:
            return
        if planner_request.status is PlannerRequestStatus.SUPERSEDED:
            contract_root = conn.execute(
                "SELECT * FROM strategic_contract_roots WHERE game_id=?",
                (existing.game_session_id,),
            ).fetchone()
            latest_attempt_row = conn.execute(
                "SELECT * FROM provider_attempts WHERE planner_request_id=? "
                "ORDER BY attempt_number DESC LIMIT 1",
                (existing.planner_request_id,),
            ).fetchone()
            latest_attempt_id = (
                None
                if latest_attempt_row is None
                else str(latest_attempt_row["provider_attempt_id"])
            )
            if (
                isinstance(tick, StrategicRequestTerminatedTick)
                and tick.planner_request_id == existing.planner_request_id
                and tick.terminal_status is PlannerRequestStatus.SUPERSEDED
                and tick.failure_category == planner_request.failure_category
                and tick.provider_attempt_id == latest_attempt_id
                and planner_request.failure_category == "stale_strategic_contract_base"
                and planner_request.completed_at is not None
                and planner_request.next_retry_at is None
                and not provider_attempts
                and strategic_research_proposal is None
                and cls._strategic_request_base_is_stale(
                    existing,
                    None if contract_root is None else dict(contract_root),
                )
            ):
                return
            raise ValueError("strategic SUPERSEDED transition is invalid")
        if existing.status is not PlannerRequestStatus.IN_PROGRESS:
            raise ValueError(
                "strategic PlannerRequest status transition is not allowed"
            )
        if len(provider_attempts) != 1:
            raise ValueError(
                "strategic Provider completion requires one ProviderAttempt "
                "in the same Runtime transaction"
            )
        final_attempt = provider_attempts[0]
        stored_attempt_row = conn.execute(
            "SELECT * FROM provider_attempts WHERE provider_attempt_id=?",
            (final_attempt.provider_attempt_id,),
        ).fetchone()
        stored_attempt = (
            None
            if stored_attempt_row is None
            else cls._provider_attempt_from_row(conn, stored_attempt_row)
        )
        if (
            stored_attempt is None
            or stored_attempt.status is not ProviderAttemptStatus.STARTED
            or final_attempt.planner_request_id != existing.planner_request_id
            or final_attempt.attempt_number != existing.provider_attempt_count
            or planner_request.provider_attempt_count != existing.provider_attempt_count
            or final_attempt.completed_at is None
        ):
            raise ValueError(
                "strategic Request transition requires its current completed "
                "ProviderAttempt"
            )

        if planner_request.status is PlannerRequestStatus.BACKOFF:
            if (
                not isinstance(tick, PlannerAttemptCompletedTick)
                or tick.planner_request_id != existing.planner_request_id
                or tick.provider_attempt_id != final_attempt.provider_attempt_id
                or tick.provider_attempt_count < 1
                or final_attempt.status is not ProviderAttemptStatus.FAILED
                or planner_request.next_retry_at is None
                or planner_request.next_retry_at.utcoffset() is None
                or planner_request.failure_category != "transient_provider_failure"
                or planner_request.completed_at is not None
                or information_round is not None
                or strategic_research_proposal is not None
            ):
                raise ValueError("strategic BACKOFF transition is invalid")
            return
        if planner_request.status is PlannerRequestStatus.FAILED:
            if (
                not isinstance(tick, StrategicRequestTerminatedTick)
                or tick.planner_request_id != existing.planner_request_id
                or tick.terminal_status is not PlannerRequestStatus.FAILED
                or tick.failure_category != planner_request.failure_category
                or tick.provider_attempt_id != final_attempt.provider_attempt_id
                or tick.completed_at < final_attempt.completed_at
                or final_attempt.status is not ProviderAttemptStatus.FAILED
                or planner_request.completed_at is None
                or planner_request.next_retry_at is not None
                or information_round is not None
                or strategic_research_proposal is not None
            ):
                raise ValueError("strategic FAILED transition is invalid")
            return
        if planner_request.status is PlannerRequestStatus.REJECTED:
            if (
                not isinstance(tick, StrategicRequestTerminatedTick)
                or tick.planner_request_id != existing.planner_request_id
                or tick.terminal_status is not PlannerRequestStatus.REJECTED
                or tick.failure_category != planner_request.failure_category
                or tick.provider_attempt_id != final_attempt.provider_attempt_id
                or tick.completed_at < final_attempt.completed_at
                or final_attempt.status is not ProviderAttemptStatus.SUCCEEDED
                or planner_request.completed_at is None
                or planner_request.next_retry_at is not None
                or information_round is not None
                or strategic_research_proposal is not None
                or cls._classify_planner_request_response(planner_request)
                not in {
                    _PlannerResponseFacts.CANONICAL_RESPONSE,
                    _PlannerResponseFacts.CONTRACT_SCHEMA_FAILURE,
                }
            ):
                raise ValueError("strategic REJECTED transition is invalid")
            return
        if planner_request.status is PlannerRequestStatus.COMPLETED:
            if (
                not isinstance(tick, StrategicProposalReadyTick)
                or tick.planner_request_id != existing.planner_request_id
                or final_attempt.status is not ProviderAttemptStatus.SUCCEEDED
                or planner_request.completed_at is None
                or planner_request.next_retry_at is not None
                or information_round is not None
                or strategic_research_proposal is None
                or strategic_research_proposal.source_planner_request_id
                != existing.planner_request_id
                or strategic_research_proposal.source_provider_attempt_id
                != final_attempt.provider_attempt_id
                or strategic_research_proposal.proposal_id != tick.proposal_id
            ):
                raise ValueError("strategic COMPLETED transition is invalid")
            return
        raise ValueError("strategic PlannerRequest transition is invalid")

    @classmethod
    def _validate_strategic_information_transition_in_connection(
        cls,
        conn: sqlite3.Connection,
        tick: WorkflowTick,
        planner_request: PlannerRequest | None,
        provider_attempts: Sequence[ProviderAttempt],
        information_round: InformationRound | None,
    ) -> None:
        if information_round is None:
            return
        parent_row = conn.execute(
            "SELECT * FROM logical_planner_requests WHERE planner_request_id=?",
            (information_round.planner_request_id,),
        ).fetchone()
        if parent_row is None:
            raise ValueError("InformationRound parent PlannerRequest does not exist")
        current_request = cls._planner_request_from_row(parent_row)
        if current_request.target.kind not in STRATEGIC_PROPOSAL_TARGET_KINDS:
            return
        if (
            planner_request is None
            or planner_request.planner_request_id != current_request.planner_request_id
            or planner_request.game_session_id != tick.game_session_id
            or information_round.planner_request_id
            != planner_request.planner_request_id
        ):
            raise ValueError(
                "strategic InformationRound requires its PlannerRequest in the "
                "same Runtime transaction"
            )
        existing_row = conn.execute(
            "SELECT * FROM information_rounds WHERE information_round_id=?",
            (information_round.information_round_id,),
        ).fetchone()
        if existing_row is None:
            if (
                not isinstance(tick, InformationRequestedTick)
                or tick.planner_request_id != planner_request.planner_request_id
                or tick.information_round_id != information_round.information_round_id
                or current_request.status is not PlannerRequestStatus.IN_PROGRESS
                or planner_request.status
                is not PlannerRequestStatus.AWAITING_INFORMATION
                or information_round.status is not InformationRoundStatus.REQUESTED
            ):
                raise ValueError(
                    "strategic InformationRound request transition is invalid"
                )
            matching = [
                attempt
                for attempt in provider_attempts
                if attempt.provider_attempt_id
                == information_round.source_provider_attempt_id
                and attempt.attempt_number
                == information_round.source_provider_attempt_number
                and attempt.planner_request_id == planner_request.planner_request_id
            ]
            if len(matching) != 1:
                raise ValueError(
                    "strategic InformationRound requires the successful "
                    "ProviderAttempt in the same Runtime transaction"
                )
            source = matching[0]
            source_row = conn.execute(
                "SELECT * FROM provider_attempts WHERE provider_attempt_id=?",
                (source.provider_attempt_id,),
            ).fetchone()
            if source_row is None:
                raise ValueError(
                    "strategic InformationRound source ProviderAttempt was not started"
                )
            started = cls._provider_attempt_from_row(conn, source_row)
            if (
                started.status is not ProviderAttemptStatus.STARTED
                or source.status is not ProviderAttemptStatus.SUCCEEDED
                or source.completed_at is None
                or source.attempt_number != current_request.provider_attempt_count
                or planner_request.provider_attempt_count
                != current_request.provider_attempt_count
                or source.completed_at > information_round.requested_at
            ):
                raise ValueError(
                    "strategic InformationRound source ProviderAttempt is not "
                    "the completed current Attempt"
                )
            return

        existing = cls._information_round_from_row(conn, existing_row)
        if existing.status is not InformationRoundStatus.REQUESTED:
            raise ValueError(
                "terminal strategic InformationRound cannot transition again"
            )
        if cls._information_round_creation_definition(
            tick.game_session_id, information_round
        ) != cls._information_round_creation_definition(
            str(existing_row["game_id"]), existing
        ):
            raise ValueError("InformationRound creation identity is immutable")
        if provider_attempts:
            raise ValueError(
                "strategic InformationRound completion cannot add ProviderAttempts"
            )
        if information_round.status is InformationRoundStatus.COLLECTED:
            if (
                not isinstance(tick, InformationCollectedTick)
                or tick.planner_request_id != planner_request.planner_request_id
                or tick.information_round_id != information_round.information_round_id
                or current_request.status
                is not PlannerRequestStatus.AWAITING_INFORMATION
                or planner_request.status is not PlannerRequestStatus.READY_TO_CONTINUE
            ):
                raise ValueError(
                    "strategic InformationRound collection transition is invalid"
                )
            return
        if information_round.status is InformationRoundStatus.FAILED:
            if (
                not isinstance(tick, StrategicRequestTerminatedTick)
                or tick.planner_request_id != planner_request.planner_request_id
                or tick.terminal_status is not planner_request.status
                or tick.failure_category != planner_request.failure_category
                or current_request.status
                is not PlannerRequestStatus.AWAITING_INFORMATION
                or planner_request.status not in TERMINAL_PLANNER_STATUSES
            ):
                raise ValueError(
                    "strategic InformationRound failure transition is invalid"
                )
            return
        raise ValueError("strategic InformationRound requires a lifecycle transition")

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
        strategic_research_proposal: StrategicResearchProposal | None = None,
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
            conn.execute("BEGIN IMMEDIATE")
            self._validate_strategic_resume_transition_in_connection(
                conn, tick, human_wait_context
            )
            self._validate_strategic_wait_error_transition_in_connection(
                conn, tick, human_wait_context
            )
            self._validate_strategic_request_wait_transition_in_connection(
                conn, tick, human_wait_context
            )
            self._validate_strategic_request_transition_in_connection(
                conn,
                tick,
                planner_request,
                provider_attempts,
                information_round,
                strategic_research_proposal,
            )
            self._validate_strategic_information_transition_in_connection(
                conn,
                tick,
                planner_request,
                provider_attempts,
                information_round,
            )
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
            for provider_attempt in provider_attempts:
                self._save_provider_attempt_in_connection(
                    conn,
                    tick.game_session_id,
                    provider_attempt,
                    validate_aggregate=False,
                )
            if planner_request is not None:
                if planner_request.game_session_id != tick.game_session_id:
                    raise ValueError("planner request and Tick must belong to one game")
                self._save_planner_request_in_connection(conn, planner_request)
            if strategic_research_proposal is not None:
                if strategic_research_proposal.game_session_id != tick.game_session_id:
                    raise ValueError("proposal and Tick must belong to one game")
                self._save_strategic_research_proposal_in_connection(
                    conn, strategic_research_proposal
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
            human_wait_context = (
                self._preserve_concurrent_strategic_resume_context_in_connection(
                    conn, tick, human_wait_context
                )
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
            self._validate_phase1b_proposals_v10(conn)

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
                self._human_wait_meta_key(game_id),
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

    @classmethod
    def _prepare_replay_state(
        cls,
        conn: sqlite3.Connection,
        game_id: str,
        tables: Mapping[str, Any],
    ) -> dict[str, list[dict[str, Any]]]:
        prepared: dict[str, list[dict[str, Any]]] = {}
        meta_keys = {
            "last_game_id",
            "last_observed_turn",
            f"unit_observations_initialized:{game_id}",
            cls._human_wait_meta_key(game_id),
        }
        for table in (*REPLAY_STATE_TABLES, "workflow_meta"):
            rows = tables.get(table, [])
            if not isinstance(rows, list):
                raise ValueError(f"invalid replay state table: {table!r}")
            column_rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            known_columns = {str(row["name"]) for row in column_rows}
            primary_columns = [
                str(row["name"])
                for row in sorted(column_rows, key=lambda item: int(item["pk"]))
                if int(row["pk"]) > 0
            ]
            unique_specs: list[tuple[str, ...]] = []
            for index in conn.execute(f"PRAGMA index_list({table})").fetchall():
                if not int(index["unique"]):
                    continue
                columns = tuple(
                    str(row["name"])
                    for row in conn.execute(
                        f"PRAGMA index_info({index['name']})"
                    ).fetchall()
                )
                if columns:
                    unique_specs.append(columns)
            seen_primary: set[tuple[Any, ...]] = set()
            seen_unique: dict[tuple[str, ...], set[tuple[Any, ...]]] = {
                spec: set() for spec in unique_specs
            }
            prepared_rows: list[dict[str, Any]] = []
            for source_row in rows:
                if not isinstance(source_row, dict) or not source_row:
                    raise ValueError(f"invalid replay row for {table}")
                unknown = set(source_row) - known_columns
                if unknown:
                    raise ValueError(
                        f"unknown replay columns for {table}: {sorted(unknown)}"
                    )
                row = dict(source_row)
                if table == "workflow_meta":
                    if row.get("key") not in meta_keys:
                        raise ValueError(
                            "replay workflow_meta key is outside game scope"
                        )
                elif row.get("game_id") != game_id:
                    raise ValueError(f"replay row for {table} belongs to another game")
                for column, value in row.items():
                    if column.endswith("_json") and value is not None:
                        try:
                            json.loads(str(value))
                        except Exception as exc:
                            raise ValueError(
                                f"invalid replay JSON in {table}.{column}"
                            ) from exc
                if table == "strategic_contract_roots":
                    row = cls._normalize_contract_root_row(row)
                elif table == "strategic_contract_revisions":
                    row = cls._normalize_contract_revision_row(row)
                elif table == "strategic_contract_commits":
                    row = cls._normalize_contract_commit_row(row)
                elif table == "strategic_research_proposals":
                    row = cls._normalize_strategic_research_proposal_row(row)
                elif table == "strategic_proposal_wait_resume_requests":
                    row = cls._normalize_strategic_proposal_wait_resume_request_row(row)
                elif table == "logical_planner_requests":
                    row = cls._normalize_planner_request_row(
                        row,
                        validate_response_contract=True,
                    )
                elif table == "provider_attempts":
                    row = cls._normalize_provider_attempt_row(
                        row, expected_game_id=game_id
                    )
                elif table == "information_rounds":
                    row = cls._normalize_information_round_row(
                        row, expected_game_id=game_id
                    )

                if primary_columns:
                    if any(column not in row for column in primary_columns):
                        raise ValueError(
                            f"replay row for {table} is missing its primary key"
                        )
                    primary = tuple(row[column] for column in primary_columns)
                    if primary in seen_primary:
                        raise ValueError(
                            f"duplicate replay primary key for {table}: {primary}"
                        )
                    seen_primary.add(primary)
                    where = " AND ".join(f"{column}=?" for column in primary_columns)
                    existing = conn.execute(
                        f"SELECT * FROM {table} WHERE {where}",
                        primary,
                    ).fetchone()
                    if (
                        existing is not None
                        and table != "workflow_meta"
                        and str(existing["game_id"]) != game_id
                    ):
                        raise ValueError(
                            f"replay primary key for {table} belongs to another game"
                        )
                for spec, seen in seen_unique.items():
                    if any(column not in row for column in spec):
                        continue
                    identity = tuple(row[column] for column in spec)
                    if any(value is None for value in identity):
                        continue
                    if identity in seen:
                        raise sqlite3.IntegrityError(
                            f"duplicate replay identity for {table}: {spec}"
                        )
                    seen.add(identity)
                prepared_rows.append(row)
            prepared[table] = prepared_rows

        for row in prepared["logical_planner_requests"]:
            cls._validate_contract_schema_failure_attempt(
                PlannerRequest.model_validate_json(str(row["request_json"])),
                prepared["provider_attempts"],
            )
        external_proposals = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM strategic_research_proposals WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        external_resume_requests = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM strategic_proposal_wait_resume_requests "
                "WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        external_requests = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM logical_planner_requests WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        external_attempts = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM provider_attempts WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        external_information_rounds = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM information_rounds WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        external_ticks = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM workflow_ticks WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        external_runtime = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM runtime_state WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        external_wait_meta = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM workflow_meta WHERE key LIKE 'human_wait:%' AND key<>?",
                (cls._human_wait_meta_key(game_id),),
            ).fetchall()
        ]
        external_roots = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM strategic_contract_roots WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        cls._validate_strategic_proposal_lifecycle_v10(
            [
                *external_proposals,
                *prepared["strategic_research_proposals"],
            ],
            [
                *external_resume_requests,
                *prepared["strategic_proposal_wait_resume_requests"],
            ],
            [
                *external_requests,
                *prepared["logical_planner_requests"],
            ],
            [
                *external_attempts,
                *prepared["provider_attempts"],
            ],
            [
                *external_information_rounds,
                *prepared["information_rounds"],
            ],
            [*external_ticks, *prepared["workflow_ticks"]],
            [*external_runtime, *prepared["runtime_state"]],
            [*external_wait_meta, *prepared["workflow_meta"]],
            [*external_roots, *prepared["strategic_contract_roots"]],
            require_canonical=True,
        )
        external_revisions = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM strategic_contract_revisions WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        external_commits = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM strategic_contract_commits WHERE game_id<>?",
                (game_id,),
            ).fetchall()
        ]
        cls._validate_strategic_contract_state(
            [*external_roots, *prepared["strategic_contract_roots"]],
            [*external_revisions, *prepared["strategic_contract_revisions"]],
            [*external_commits, *prepared["strategic_contract_commits"]],
            require_canonical=True,
        )

        for table in REPLAY_STATE_TABLES:
            for foreign_key in conn.execute(
                f"PRAGMA foreign_key_list({table})"
            ).fetchall():
                parent_table = str(foreign_key["table"])
                child_column = str(foreign_key["from"])
                parent_column = str(foreign_key["to"])
                parent_values = {
                    row[parent_column]
                    for row in prepared.get(parent_table, [])
                    if parent_column in row
                }
                for row in prepared[table]:
                    child_value = row.get(child_column)
                    if child_value is not None and child_value not in parent_values:
                        raise ValueError(
                            f"replay {table}.{child_column} references a missing "
                            f"{parent_table}.{parent_column}"
                        )
        return prepared

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
            prepared = self._prepare_replay_state(conn, game_id, tables)
            for table in reversed(REPLAY_STATE_TABLES):
                conn.execute(f"DELETE FROM {table} WHERE game_id=?", (game_id,))
            conn.execute(
                """
                DELETE FROM workflow_meta
                WHERE key IN (?, ?, ?, ?)
                """,
                (
                    "last_game_id",
                    "last_observed_turn",
                    f"unit_observations_initialized:{game_id}",
                    self._human_wait_meta_key(game_id),
                ),
            )
            for table in (*REPLAY_STATE_TABLES, "workflow_meta"):
                for row in prepared[table]:
                    columns = sorted(row)
                    placeholders = ",".join("?" for _ in columns)
                    column_sql = ",".join(columns)
                    conn.execute(
                        f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
                        tuple(row[column] for column in columns),
                    )
