from __future__ import annotations

import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from civ6_workflow.config import AppConfig
from civ6_workflow.models import ExecutionMode, PlanBundle, ProposedTask, TaskStatus
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
        assert status["status"]["connection_owner"] == (
            "frontend_via_local_backend"
        )

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
