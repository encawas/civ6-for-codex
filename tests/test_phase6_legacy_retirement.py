from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from datetime import UTC, datetime

import pytest

from civ6_workflow.domain import (
    PlannerRequest,
    PlannerRequestStatus,
    PlannerRequestTarget,
    PlannerRequestTargetKind,
)
from civ6_workflow.models import TaskStatus
from civ6_workflow.store import PHASE5_REPLAY_STATE_TABLES, SCHEMA, WorkflowStore


LEGACY_TABLES = {
    "strategy_state",
    "city_plans",
    "unit_plans",
    "builder_plans",
    "workflow_tasks",
    "decision_gaps",
    "decision_groups",
    "plan_leases",
    "planner_suppressions",
}


def _legacy_request() -> PlannerRequest:
    return PlannerRequest(
        planner_request_id="legacy-request",
        game_session_id="game-1",
        turn_number=4,
        observation_id="legacy-observation",
        target=PlannerRequestTarget(
            kind=PlannerRequestTargetKind.LEGACY_DECISION_GROUP,
            decision_group_id="legacy-group",
            decision_gap_ids=("legacy-gap",),
        ),
        input_projection_hash="legacy-input",
        input_projection_version="decision-input/v2",
        input_projection={"turn": 4},
        request_payload={"events": []},
        policy_revision="legacy-policy",
        model_settings={"model": "legacy"},
        status=PlannerRequestStatus.PENDING,
        created_at=datetime(2026, 7, 30, tzinfo=UTC),
    )


def _seed_v13_task(path, status: TaskStatus) -> None:
    WorkflowStore(path)
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            """
            INSERT INTO workflow_tasks(
                game_id, task_id, plan_id, action_type, entity_type, entity_id,
                due_turn, expires_turn, arguments_json, preconditions_json,
                postconditions_json, invalidators_json, risk,
                requires_confirmation, reason, status, retry_count, max_retries,
                last_error, approved_by, created_turn,
                created_from_observation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "game-1",
                "legacy-task",
                "legacy-plan",
                "set_research",
                "player",
                "player-1",
                4,
                None,
                '{"tech_or_civic":"TECH_MINING"}',
                "[]",
                "[]",
                "[]",
                "low",
                0,
                "historical task",
                status.value,
                0,
                2,
                None,
                None,
                4,
                "legacy-observation",
            ),
        )
        conn.execute("PRAGMA user_version=13")


def _table_names(path) -> set[str]:
    with sqlite3.connect(path) as conn:
        return {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }


def _raw_v13_replay(path) -> dict:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        tables = {}
        existing = _table_names(path)
        for table in PHASE5_REPLAY_STATE_TABLES:
            tables[table] = (
                []
                if table not in existing
                else [
                    dict(row)
                    for row in conn.execute(
                        f"SELECT * FROM {table} WHERE game_id='game-1'"
                    )
                ]
            )
        tables["workflow_meta"] = []
    return {"game_id": "game-1", "tables": tables}


def test_public_legacy_authority_writes_fail_closed(tmp_path):
    store = WorkflowStore(tmp_path / "public-writes.sqlite3")

    with pytest.raises(ValueError, match="PlannerRequest writes are retired"):
        store.save_planner_request(_legacy_request())

    assert not hasattr(store, "save_plan_bundle")

    assert store.list_tasks("game-1") == []
    assert store.list_decision_gaps("game-1") == []
    assert store.list_plan_leases("game-1") == []


def test_v13_terminal_legacy_state_is_hash_archived_and_tables_are_dropped(tmp_path):
    path = tmp_path / "terminal-v13.sqlite3"
    _seed_v13_task(path, TaskStatus.DONE)

    store = WorkflowStore(path)
    exported = store.export_replay_state("game-1")
    archive = exported["tables"]["legacy_workflow_archive"]

    assert LEGACY_TABLES.isdisjoint(_table_names(path))
    assert len(archive) == 1
    assert archive[0]["source_table"] == "workflow_tasks"
    assert json.loads(archive[0]["record_json"])["task_id"] == "legacy-task"
    assert (
        archive[0]["record_hash"]
        == hashlib.sha256(archive[0]["record_json"].encode("utf-8")).hexdigest()
    )

    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(copy.deepcopy(exported))
    assert restored.export_replay_state("game-1") == exported


@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.PENDING,
        TaskStatus.READY,
        TaskStatus.FAILED,
        TaskStatus.ESCALATED,
        TaskStatus.UNCERTAIN,
    ],
)
def test_v13_active_or_recoverable_legacy_task_blocks_migration(tmp_path, status):
    path = tmp_path / f"blocked-{status.value}.sqlite3"
    _seed_v13_task(path, status)

    with pytest.raises(ValueError, match="legacy StoredTask to be terminal"):
        WorkflowStore(path)

    assert "workflow_tasks" in _table_names(path)
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            "SELECT status FROM workflow_tasks WHERE task_id='legacy-task'"
        ).fetchone()
        assert row == (status.value,)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 13


def test_tampered_legacy_archive_fails_startup(tmp_path):
    path = tmp_path / "tampered-archive.sqlite3"
    _seed_v13_task(path, TaskStatus.DONE)
    WorkflowStore(path)
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE legacy_workflow_archive SET record_hash='tampered'")

    with pytest.raises(ValueError, match="record hash does not match"):
        WorkflowStore(path)


def test_v13_replay_is_migrated_before_canonical_import(tmp_path):
    legacy_path = tmp_path / "replay-v13.sqlite3"
    _seed_v13_task(legacy_path, TaskStatus.DONE)
    state = _raw_v13_replay(legacy_path)

    target = WorkflowStore(tmp_path / "target.sqlite3")
    target.import_replay_state(copy.deepcopy(state))
    archive = target.export_replay_state("game-1")["tables"]["legacy_workflow_archive"]

    assert len(archive) == 1
    assert archive[0]["source_table"] == "workflow_tasks"
    canonical = target.export_replay_state("game-1")
    restored = WorkflowStore(tmp_path / "restored-v14.sqlite3")
    restored.import_replay_state(copy.deepcopy(canonical))
    assert restored.export_replay_state("game-1") == canonical


def test_v13_replay_preflight_failure_preserves_target_state(tmp_path):
    legacy_path = tmp_path / "blocked-replay-v13.sqlite3"
    _seed_v13_task(legacy_path, TaskStatus.READY)
    state = _raw_v13_replay(legacy_path)
    target = WorkflowStore(tmp_path / "existing-target.sqlite3")
    target.set_meta("last_game_id", "game-1")
    target.set_meta("last_observed_turn", 99)

    with pytest.raises(ValueError, match="legacy StoredTask to be terminal"):
        target.import_replay_state(state)

    assert target.get_meta("last_game_id") == "game-1"
    assert target.get_meta("last_observed_turn") == 99
    assert (
        target.export_replay_state("game-1")["tables"]["legacy_workflow_archive"] == []
    )
