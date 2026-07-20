from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from civ6_workflow.config import AppConfig
from civ6_workflow.domain import (
    ActionAttempt,
    ApprovalStatus,
    AttemptStatus,
    ContinuationPolicy,
    LeaseValidationResult,
    PlanLease,
    PlanLeaseStatus,
    RetryClassification,
    RuntimeState,
)
from civ6_workflow.models import (
    ExecutionMode,
    MutationDeliveryStatus,
    PlanBundle,
    ProposedTask,
    TaskStatus,
)
from civ6_workflow.store import WorkflowStore
from civ6_workflow.web_ui import ControlPanelHTTPServer, ControlPanelState


def _task() -> ProposedTask:
    return ProposedTask(
        task_id="ui-production",
        action_type="city_set_production",
        entity_type="city",
        entity_id=1,
        due_turn=10,
        arguments={
            "city_id": 1,
            "item_type": "UNIT",
            "item_name": "UNIT_BUILDER",
        },
        preconditions=[{"type": "city_has_no_production", "city_id": 1}],
        postconditions=[
            {
                "type": "city_production_equals",
                "city_id": 1,
                "item_name": "UNIT_BUILDER",
            }
        ],
        reason="approve from the local control panel",
    )


def _panel(tmp_path: Path, *, tick_result=None) -> ControlPanelState:
    config = AppConfig.model_validate(
        {
            "runtime": {
                "database_path": str(tmp_path / "workflow.sqlite3"),
                "execution_mode": "confirm",
                "auto_end_turn": False,
            },
            "codex": {
                "backend": "responses",
                "model": "test-model",
                "api_key_env": "OPENAI_API_KEY",
            },
        }
    )
    store = WorkflowStore(config.runtime.database_path)
    store.set_meta("last_game_id", "game-1")
    store.set_meta("last_observed_turn", 10)
    store.save_plan_bundle(
        "game-1",
        10,
        PlanBundle(summary="dashboard task", tasks=[_task()]),
        mode=ExecutionMode.CONFIRM,
        auto_action_types={"city_set_production"},
    )
    return ControlPanelState(
        config=config,
        store=store,
        run_tick_callback=lambda: tick_result or {"turn": 10, "paused": False},
        token="test-token",
    )


def test_dashboard_snapshot_and_approval(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    panel = _panel(tmp_path)

    state = panel.snapshot()
    assert state["game"] == {"game_id": "game-1", "turn": 10, "observed": True}
    assert state["config"]["execution_mode"] == "confirm"
    assert state["task_counts"]["awaiting_confirmation"] == 1
    assert state["waiting_tasks"][0]["task_id"] == "ui-production"
    assert state["planner_connection"]["configured"] is True
    assert state["planner_connection"]["connection_owner"] == (
        "frontend_via_local_backend"
    )
    assert state["planner_connection"]["secret_exposed_to_browser"] is False

    assert panel.approve("ui-production") is True
    assert panel.store.task_status("game-1", "ui-production") is TaskStatus.READY
    assert panel.approve("ui-production") is False


def test_dashboard_records_tick_result_and_error(tmp_path: Path):
    panel = _panel(tmp_path, tick_result={"turn": 10, "executed_task_ids": []})
    assert panel.run_tick()["turn"] == 10
    assert panel.snapshot()["last_tick"]["turn"] == 10

    panel.run_tick_callback = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        panel.run_tick()
    assert "RuntimeError: boom" in panel.snapshot()["server"]["last_error"]


def test_http_api_requires_token_and_exposes_state(tmp_path: Path, monkeypatch):
    async def fake_probe(config):
        return {
            "ok": True,
            "backend": config.backend,
            "model": config.model,
            "duration_seconds": 0.01,
            "http_status": 200,
            "request_id": "req_probe_1",
            "error": None,
            "connection_owner": "frontend_via_local_backend",
            "secret_exposed_to_browser": False,
        }

    monkeypatch.setattr(
        "civ6_workflow.safe_web_ui.probe_planner_connection", fake_probe
    )
    panel = _panel(tmp_path)
    server = ControlPanelHTTPServer(("127.0.0.1", 0), panel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with pytest.raises(HTTPError) as exc_info:
            urlopen(f"{base}/api/state", timeout=3)
        assert exc_info.value.code == 401

        request = Request(
            f"{base}/api/state",
            headers={"X-Civ6-Token": "test-token"},
        )
        with urlopen(request, timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert payload["game"]["game_id"] == "game-1"
        assert payload["waiting_tasks"][0]["task_id"] == "ui-production"
        assert payload["planner_connection"]["model"] == "test-model"

        status_request = Request(
            f"{base}/api/planner/status",
            headers={"X-Civ6-Token": "test-token"},
        )
        with urlopen(status_request, timeout=3) as response:
            status = json.loads(response.read().decode("utf-8"))
        assert status["status"]["connection_owner"] == ("frontend_via_local_backend")

        probe = Request(
            f"{base}/api/planner/probe",
            method="POST",
            data=b"{}",
            headers={
                "X-Civ6-Token": "test-token",
                "Content-Type": "application/json",
            },
        )
        with urlopen(probe, timeout=3) as response:
            probe_payload = json.loads(response.read().decode("utf-8"))
        assert probe_payload["ok"] is True
        assert probe_payload["result"]["request_id"] == "req_probe_1"
        assert panel.store.get_meta("last_planner_probe")["ok"] is True

        approve = Request(
            f"{base}/api/tasks/ui-production/approve",
            method="POST",
            data=b"{}",
            headers={
                "X-Civ6-Token": "test-token",
                "Content-Type": "application/json",
            },
        )
        with urlopen(approve, timeout=3) as response:
            assert json.loads(response.read().decode("utf-8"))["ok"] is True
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_http_resume_marks_a_durable_human_wait(tmp_path: Path):
    panel = _panel(tmp_path)
    panel.store.save_runtime_state("game-1", RuntimeState.AWAITING_HUMAN)
    panel.store.set_meta(
        "human_wait:game-1",
        {
            "version": "human-wait/v1",
            "execution_mode": "confirm",
            "observation_projection_hash": "test",
            "resume_requested": False,
        },
    )
    server = ControlPanelHTTPServer(("127.0.0.1", 0), panel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        resume = Request(
            f"{base}/api/workflow/resume",
            method="POST",
            data=b"{}",
            headers={
                "X-Civ6-Token": "test-token",
                "Content-Type": "application/json",
            },
        )
        with urlopen(resume, timeout=3) as response:
            assert json.loads(response.read().decode("utf-8")) == {
                "ok": True,
                "resume_requested": True,
            }
        assert panel.store.human_wait_context("game-1")["resume_requested"] is True
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def _post(base: str, path: str) -> dict:
    request = Request(
        f"{base}{path}",
        method="POST",
        data=b"{}",
        headers={
            "X-Civ6-Token": "test-token",
            "Content-Type": "application/json",
        },
    )
    with urlopen(request, timeout=3) as response:
        return json.loads(response.read().decode("utf-8"))


def _awaiting_lease(*, task_ids: tuple[str, ...] = ()) -> PlanLease:
    return PlanLease(
        plan_lease_id="lease-ui-approval",
        plan_id="plan-ui-approval",
        game_session_id="game-1",
        decision_gap_ids=("gap-ui-approval",),
        scope="city:1",
        plan_revision=1,
        task_ids=task_ids,
        created_from_observation_id="obs-ui",
        status=PlanLeaseStatus.AWAITING_APPROVAL,
        approval_status=ApprovalStatus.REQUIRED,
        valid_from_turn=10,
        valid_until_turn=11,
        continuation_policy=ContinuationPolicy.REQUIRE_REVIEW,
        relevant_input_hash="ui-input",
        last_validated_observation_id="obs-ui",
        last_validation_result=LeaseValidationResult.VALID,
    )


def test_game_bound_task_confirmation_and_rejection(tmp_path: Path):
    panel = _panel(tmp_path)
    server = ControlPanelHTTPServer(("127.0.0.1", 0), panel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        confirmed = _post(base, "/api/games/game-1/tasks/ui-production/confirm")
        assert confirmed["ok"] is True
        assert confirmed["game_id"] == "game-1"
        assert panel.store.task_status("game-1", "ui-production") is TaskStatus.READY

        panel.store.set_task_status(
            "game-1", "ui-production", TaskStatus.AWAITING_CONFIRMATION
        )
        rejected = _post(base, "/api/games/game-1/tasks/ui-production/reject")
        assert rejected["ok"] is True
        assert panel.store.task_status("game-1", "ui-production") is TaskStatus.CANCELLED

        with pytest.raises(HTTPError) as exc_info:
            _post(base, "/api/games/other-game/tasks/ui-production/confirm")
        assert exc_info.value.code == 409
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_game_bound_lease_approval_and_rejection_are_durable(tmp_path: Path):
    panel = _panel(tmp_path)
    lease = _awaiting_lease(task_ids=("ui-production",))
    panel.store.save_plan_lease(lease)
    server = ControlPanelHTTPServer(("127.0.0.1", 0), panel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        approved = _post(base, "/api/games/game-1/leases/lease-ui-approval/approve")
        assert approved["ok"] is True
        record = panel.store.latest_approval_record(
            "game-1",
            proposal_type="decision_gap",
            proposal_id="gap-ui-approval",
            proposal_revision=1,
        )
        assert record is not None
        assert record.decision.value == "APPROVED"

        second_lease = _awaiting_lease(task_ids=("ui-production",)).model_copy(
            update={"plan_lease_id": "lease-ui-reject", "decision_gap_ids": ("gap-ui-reject",)}
        )
        panel.store.save_plan_lease(second_lease)
        rejected = _post(base, "/api/games/game-1/leases/lease-ui-reject/reject")
        assert rejected["ok"] is True
        assert panel.store.list_plan_leases("game-1")[1].status is PlanLeaseStatus.INVALIDATED
        assert panel.store.task_status("game-1", "ui-production") is TaskStatus.CANCELLED
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_retry_endpoint_only_requeues_proven_not_sent_attempts(tmp_path: Path):
    panel = _panel(tmp_path)
    panel.store.set_task_status("game-1", "ui-production", TaskStatus.FAILED)
    attempt = ActionAttempt(
        action_attempt_id="attempt-ui-retry",
        task_id="ui-production",
        attempt_number=1,
        request_id="request-ui-retry",
        idempotency_key="task-ui-retry",
        prepared_from_observation_id="obs-ui",
        prepared_at=datetime.now(UTC),
        status=AttemptStatus.FAILED,
        retry_classification=RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
        normalized_arguments={"city_id": 1},
        transport_result={
            "delivery_status": MutationDeliveryStatus.PROVEN_NOT_SENT.value
        },
        game_session_id="game-1",
        action_type="city_set_production",
    )
    panel.store.save_action_attempt(attempt)
    assert panel.snapshot()["human_actions"]["retryable_attempts"] == [
        {
            "action_attempt_id": "attempt-ui-retry",
            "task_id": "ui-production",
            "action_type": "city_set_production",
            "reason": "attempt is proven not committed",
        }
    ]

    server = ControlPanelHTTPServer(("127.0.0.1", 0), panel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        retried = _post(base, "/api/games/game-1/attempts/attempt-ui-retry/retry")
        assert retried["ok"] is True
        task = panel.store.get_task("game-1", "ui-production")
        assert task is not None
        assert task.status is TaskStatus.READY
        assert task.retry_count == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)

def test_game_bound_resume_exposes_human_wait_state(tmp_path: Path):
    panel = _panel(tmp_path)
    panel.store.save_runtime_state("game-1", RuntimeState.AWAITING_HUMAN)
    panel.store.set_meta(
        "human_wait:game-1",
        {
            "version": "human-wait/v1",
            "blocking_reason": "input requires review",
            "resume_requested": False,
        },
    )
    state = panel.snapshot()
    assert state["human_actions"]["runtime_state"] == "AWAITING_HUMAN"
    assert state["human_actions"]["human_wait"]["blocking_reason"] == (
        "input requires review"
    )

    server = ControlPanelHTTPServer(("127.0.0.1", 0), panel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        resumed = _post(base, "/api/games/game-1/workflow/resume")
        assert resumed == {
            "ok": True,
            "reason": "resume recorded; the next tick will re-evaluate the wait",
            "game_id": "game-1",
        }
        assert panel.store.human_wait_context("game-1")["resume_requested"] is True
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)