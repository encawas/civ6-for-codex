from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from types import SimpleNamespace
from urllib.request import Request, urlopen

import pytest

from civ6_workflow.actions import (
    build_action_attempt_idempotency_key,
    resolve_action_spec,
)
from civ6_workflow.config import AppConfig
from civ6_workflow.domain import (
    ActionAttempt,
    AuthorityScopeSet,
    AwaitingHumanTick,
    AttemptStatus,
    Mission,
    MissionGraph,
    MissionStatus,
    RetryClassification,
    RuntimeState,
    StrategicContract,
    StrategicContractCommit,
    SubjectRef,
    build_strategic_contract_id,
)
from civ6_workflow.models import (
    ExecutionMode,
    MutationDeliveryStatus,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.web_cli import _RuntimeWorker
from civ6_workflow.store import WorkflowStore
from civ6_workflow.turn_compiler import TurnCompiler
from civ6_workflow.web_ui import ControlPanelHTTPServer, ControlPanelState


def _task_id(panel: ControlPanelState) -> str:
    value = panel.store.get_meta("test_turn_action_node_id")
    assert isinstance(value, str)
    return value


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
    now = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)
    observation = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id="game-1",
            turn=10,
            overview={"turn": 10},
            cities=[
                {
                    "city_id": 1,
                    "owner": "player-1",
                    "currently_building": None,
                }
            ],
        )
    ).canonical.model_copy(update={"observed_at": now})
    store.save_normalized_observation(observation)
    contract_id = build_strategic_contract_id("game-1")
    foundation = store.commit_strategic_contract_revision(
        StrategicContractCommit(
            commit_id="ui-contract-foundation",
            game_session_id="game-1",
            contract_id=contract_id,
            expected_base_revision=0,
            contract=StrategicContract(
                contract_id=contract_id,
                game_session_id="game-1",
                revision=1,
                authority_scope_set=AuthorityScopeSet(),
                mission_graph=MissionGraph(),
                created_from_observation_id=observation.observation_id,
            ),
            committed_at=now,
            reason="create control panel fixture Contract",
        )
    )
    contract, _ = store.activate_city_roles_authority(
        game_session_id="game-1",
        expected_base_revision=foundation.revision,
        mission=Mission(
            mission_id="mission-ui-city-role",
            game_session_id="game-1",
            contract_id=contract_id,
            mission_revision=1,
            scope="city_roles",
            subject=SubjectRef(subject_type="player", subject_id="player-1"),
            slot="player:city_roles",
            objective="Develop the capital",
            desired_outcome={
                "city_roles": {
                    "owner": "player-1",
                    "cities": [
                        {
                            "city_id": 1,
                            "role": "production",
                            "production_queue": [
                                {
                                    "item_type": "UNIT",
                                    "item_name": "UNIT_BUILDER",
                                }
                            ],
                        }
                    ],
                }
            },
            status=MissionStatus.ACTIVE,
        ),
        activation_id="ui-city-role-activation",
        observation_id=observation.observation_id,
        turn_number=10,
        activated_at=now + timedelta(seconds=1),
    )
    compilation = TurnCompiler().compile_missions(
        observation,
        contract,
        contract.mission_graph.missions,
        mode=ExecutionMode.CONFIRM,
        auto_action_types={"city_set_production"},
        compiled_at=now + timedelta(seconds=2),
    )
    _, tasks = store.activate_turn_action_graph(
        compilation.graph,
        compilation.nodes,
        activated_at=now + timedelta(seconds=2),
    )
    assert len(tasks) == 1
    store.set_meta("test_turn_action_node_id", tasks[0].task_id)
    return ControlPanelState(
        config=config,
        store=store,
        run_tick_callback=lambda: tick_result or {"turn": 10, "paused": False},
        token="test-token",
    )


def _persist_generic_human_wait(
    panel: ControlPanelState, *, blocking_reason: str
) -> None:
    now = datetime.now(UTC)
    panel.store.persist_tick_and_runtime_state(
        AwaitingHumanTick(
            tick_id="tick-web-human-wait",
            game_session_id="game-1",
            turn_number=10,
            starting_runtime_state=RuntimeState.OBSERVING,
            observation_ids=("obs-web-human-wait",),
            started_at=now,
            completed_at=now,
            blocking_reason=blocking_reason,
        ),
        human_wait_context={
            "version": "human-wait/v1",
            "execution_mode": "confirm",
            "observation_projection_hash": "test",
            "blocking_reason": blocking_reason,
            "resume_requested": False,
        },
    )


def test_dashboard_snapshot_and_approval(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    panel = _panel(tmp_path)

    state = panel.snapshot()
    assert state["game"] == {"game_id": "game-1", "turn": 10, "observed": True}
    assert state["config"]["execution_mode"] == "confirm"
    assert state["task_counts"]["awaiting_confirmation"] == 1
    task_id = _task_id(panel)
    assert state["waiting_tasks"][0]["task_id"] == task_id
    assert state["planner_connection"]["configured"] is True
    assert state["planner_connection"]["connection_owner"] == (
        "frontend_via_local_backend"
    )
    assert state["planner_connection"]["secret_exposed_to_browser"] is False

    assert panel.approve(task_id) is True
    assert panel.store.task_status("game-1", task_id) is TaskStatus.READY
    assert panel.approve(task_id) is False


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

    monkeypatch.setattr("civ6_workflow.web_ui.probe_planner_connection", fake_probe)
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
        task_id = _task_id(panel)
        assert payload["waiting_tasks"][0]["task_id"] == task_id
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
            f"{base}/api/tasks/{task_id}/approve",
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
    _persist_generic_human_wait(panel, blocking_reason="input requires review")
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


def test_game_bound_task_confirmation_and_rejection(tmp_path: Path):
    panel = _panel(tmp_path)
    task_id = _task_id(panel)
    server = ControlPanelHTTPServer(("127.0.0.1", 0), panel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        confirmed = _post(base, f"/api/games/game-1/tasks/{task_id}/confirm")
        assert confirmed["ok"] is True
        assert confirmed["game_id"] == "game-1"
        assert panel.store.task_status("game-1", task_id) is TaskStatus.READY

        panel.store.set_task_status("game-1", task_id, TaskStatus.AWAITING_CONFIRMATION)
        rejected = _post(base, f"/api/games/game-1/tasks/{task_id}/reject")
        assert rejected["ok"] is True
        assert panel.store.task_status("game-1", task_id) is TaskStatus.CANCELLED

        with pytest.raises(HTTPError) as exc_info:
            _post(base, f"/api/games/other-game/tasks/{task_id}/confirm")
        assert exc_info.value.code == 409
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_retry_endpoint_only_requeues_proven_not_sent_attempts(tmp_path: Path):
    panel = _panel(tmp_path)
    task_id = _task_id(panel)
    assert panel.store.approve_task("game-1", task_id)
    task = panel.store.get_task("game-1", task_id)
    assert task is not None
    normalized_arguments = resolve_action_spec(task.action_type).build_arguments(task)
    prepared = ActionAttempt(
        action_attempt_id="attempt-ui-retry",
        task_id=task_id,
        attempt_number=1,
        request_id="request-ui-retry",
        idempotency_key=build_action_attempt_idempotency_key(
            task, normalized_arguments
        ),
        prepared_from_observation_id=task.created_from_observation_id,
        prepared_at=datetime.now(UTC),
        status=AttemptStatus.PREPARED,
        retry_classification=RetryClassification.SAFE_IF_PROVEN_NOT_SENT,
        normalized_arguments=normalized_arguments,
        postconditions=tuple(task.postconditions),
        game_session_id="game-1",
        action_type="city_set_production",
    )
    panel.store.save_action_attempt(prepared)
    uncertain = prepared.model_copy(
        update={
            "status": AttemptStatus.UNCERTAIN,
            "sent_at": datetime.now(UTC),
            "transport_result": {"delivery_status": "delivery_started"},
        }
    )
    panel.store.update_action_attempt(uncertain)
    failed = uncertain.model_copy(
        update={
            "status": AttemptStatus.FAILED,
            "transport_result": {
                "delivery_status": MutationDeliveryStatus.PROVEN_NOT_SENT.value
            },
        }
    )
    panel.store.update_action_attempt(failed)
    panel.store.set_task_status("game-1", task_id, TaskStatus.FAILED)
    assert panel.snapshot()["human_actions"]["retryable_attempts"] == [
        {
            "action_attempt_id": "attempt-ui-retry",
            "task_id": task_id,
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
        task = panel.store.get_task("game-1", task_id)
        assert task is not None
        assert task.status is TaskStatus.READY
        assert task.retry_count == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_game_bound_resume_exposes_human_wait_state(tmp_path: Path):
    panel = _panel(tmp_path)
    _persist_generic_human_wait(panel, blocking_reason="input requires review")
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


@pytest.mark.parametrize(
    ("path", "approved"),
    [
        ("/api/games/game-1/proposals/proposal-1/approve", True),
        ("/api/games/game-1/proposals/proposal-1/reject", False),
    ],
)
def test_http_routes_strategic_proposal_decisions(
    tmp_path: Path, monkeypatch, path: str, approved: bool
):
    panel = _panel(tmp_path)
    calls = []

    def decide(self, game_id, proposal_id, *, approved):
        calls.append((game_id, proposal_id, approved))
        return True, "strategic Proposal decision recorded"

    monkeypatch.setattr(ControlPanelState, "decide_strategic_proposal", decide)
    server = ControlPanelHTTPServer(("127.0.0.1", 0), panel)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        result = _post(base, path)

        assert result == {
            "ok": True,
            "reason": "strategic Proposal decision recorded",
            "game_id": "game-1",
            "proposal_id": "proposal-1",
        }
        assert calls == [("game-1", "proposal-1", approved)]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)


def test_control_panel_runtime_worker_reuses_one_live_runtime():
    lifecycle = {"opens": 0, "closes": 0, "ticks": 0}

    class _Runtime:
        async def tick(self):
            lifecycle["ticks"] += 1
            return {"turn": lifecycle["ticks"]}

    class _Context:
        async def __aenter__(self):
            lifecycle["opens"] += 1
            return SimpleNamespace(runtime=_Runtime())

        async def __aexit__(self, exc_type, exc, tb):
            lifecycle["closes"] += 1

    worker = _RuntimeWorker(
        _Context,
        startup_timeout_seconds=1,
        tick_timeout_seconds=1,
    )
    try:
        assert worker.run_tick() == {"turn": 1}
        assert worker.run_tick() == {"turn": 2}
        assert lifecycle == {"opens": 1, "closes": 0, "ticks": 2}
    finally:
        worker.close()

    assert lifecycle == {"opens": 1, "closes": 1, "ticks": 2}
