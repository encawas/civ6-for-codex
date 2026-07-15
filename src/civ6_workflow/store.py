from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

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
    game_id TEXT NOT NULL,
    turn INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, turn)
);

CREATE TABLE IF NOT EXISTS unit_observations (
    game_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    unit_type TEXT NOT NULL,
    first_seen_turn INTEGER NOT NULL,
    last_seen_turn INTEGER NOT NULL,
    eligible_for_binding INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (game_id, unit_id)
);
"""

REPLAY_STATE_TABLES = (
    "strategy_state",
    "city_plans",
    "unit_plans",
    "builder_plans",
    "workflow_tasks",
    "event_log",
    "agent_runs",
    "turn_metrics",
    "unit_observations",
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
            "updated_at": "TEXT",
        }
        for name, declaration in additions.items():
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE workflow_tasks ADD COLUMN {name} {declaration}"
                )

        # Recover tasks left in transient or legacy retry states after a restart.
        conn.execute(
            "UPDATE workflow_tasks SET status=? WHERE status=?",
            (TaskStatus.READY.value, TaskStatus.RUNNING.value),
        )
        conn.execute(
            """
            UPDATE workflow_tasks SET status=CASE
                WHEN retry_count >= max_retries THEN ?
                ELSE ?
            END
            WHERE status IN (?, ?)
            """,
            (
                TaskStatus.ESCALATED.value,
                TaskStatus.READY.value,
                TaskStatus.BLOCKED.value,
                TaskStatus.FAILED.value,
            ),
        )
        conn.execute(
            "UPDATE workflow_tasks SET updated_at=CURRENT_TIMESTAMP "
            "WHERE updated_at IS NULL"
        )
        conn.execute("PRAGMA user_version=3")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
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
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _load(value: str) -> Any:
        return json.loads(value)

    def set_meta(self, key: str, value: Any) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO workflow_meta(key, value_json) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json=excluded.value_json,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (key, self._dump(value)),
            )

    def get_meta(self, key: str, default: Any = None) -> Any:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value_json FROM workflow_meta WHERE key=?", (key,)
            ).fetchone()
        return default if row is None else self._load(row["value_json"])

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
    ) -> None:
        with self._connect() as conn:
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
                conn, "city_plans", "city_id", game_id, bundle.plan_id, turn,
                bundle.city_plan_updates,
            )
            self._upsert_entity_plans(
                conn, "unit_plans", "unit_id", game_id, bundle.plan_id, turn,
                bundle.unit_plan_updates,
            )
            self._upsert_entity_plans(
                conn, "builder_plans", "builder_key", game_id, bundle.plan_id, turn,
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
                elif proposed.requires_confirmation or proposed.action_type not in auto_action_types:
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
                        risk, requires_confirmation, reason, status, created_turn
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                            WHEN workflow_tasks.status='done' THEN workflow_tasks.status
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
        return {
            str(row["unit_id"]): int(row["first_seen_turn"]) for row in observed
        }

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

    def approve_task(self, game_id: str, task_id: str, approved_by: str = "user") -> bool:
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
                row["city_id"]: {**self._load(row["plan_json"]), "_plan_id": row["plan_id"]}
                for row in cities
            },
            "units": {
                row["unit_id"]: {**self._load(row["plan_json"]), "_plan_id": row["plan_id"]}
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

    def record_metrics(self, game_id: str, turn: int, metrics: TickMetrics) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO turn_metrics(game_id, turn, metrics_json)
                VALUES (?, ?, ?)
                ON CONFLICT(game_id, turn) DO UPDATE SET
                    metrics_json=excluded.metrics_json,
                    created_at=CURRENT_TIMESTAMP
                """,
                (game_id, turn, metrics.model_dump_json()),
            )

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
        with self._connect() as conn:
            for table in REPLAY_STATE_TABLES:
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
            for table, rows in tables.items():
                if table not in allowed_tables or not isinstance(rows, list):
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
