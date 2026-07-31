import json

import pytest

import civ6_workflow.mcp_port as mcp_port


def test_sidecar_guard_reclaims_only_recorded_stale_processes(tmp_path, monkeypatch):
    running = {101, 102}
    terminated = []
    monkeypatch.setattr(
        mcp_port,
        "_windows_civ_mcp_process_ids",
        lambda: set(running),
    )

    def terminate(process_ids):
        terminated.append(set(process_ids))
        running.difference_update(process_ids)

    monkeypatch.setattr(
        mcp_port,
        "_terminate_windows_civ_mcp_processes",
        terminate,
    )
    guard = mcp_port._McpSidecarGuard("owner", state_directory=tmp_path, windows=True)
    guard.owner_path.write_text(
        json.dumps({"process_ids": [101, 102]}), encoding="utf-8"
    )

    with guard:
        assert terminated == [{101, 102}]
        assert not guard.owner_path.exists()


def test_sidecar_guard_rejects_unowned_existing_process(tmp_path, monkeypatch):
    monkeypatch.setattr(
        mcp_port,
        "_windows_civ_mcp_process_ids",
        lambda: {201},
    )
    guard = mcp_port._McpSidecarGuard("unowned", state_directory=tmp_path, windows=True)

    with pytest.raises(RuntimeError, match="unowned civ6-mcp sidecar"):
        guard.__enter__()


def test_sidecar_guard_records_and_cleans_owned_processes(tmp_path, monkeypatch):
    running = set()
    terminated = []
    monkeypatch.setattr(
        mcp_port,
        "_windows_civ_mcp_process_ids",
        lambda: set(running),
    )

    def terminate(process_ids):
        terminated.append(set(process_ids))
        running.difference_update(process_ids)

    monkeypatch.setattr(
        mcp_port,
        "_terminate_windows_civ_mcp_processes",
        terminate,
    )
    guard = mcp_port._McpSidecarGuard("owned", state_directory=tmp_path, windows=True)

    with guard:
        running.update({301, 302})
        guard.record_started(running)
        payload = json.loads(guard.owner_path.read_text(encoding="utf-8"))
        assert payload["process_ids"] == [301, 302]

    assert terminated == [{301, 302}]
    assert not guard.owner_path.exists()


def test_sidecar_guard_is_exclusive(tmp_path):
    first = mcp_port._McpSidecarGuard(
        "exclusive", state_directory=tmp_path, windows=False
    )
    second = mcp_port._McpSidecarGuard(
        "exclusive", state_directory=tmp_path, windows=False
    )

    with first:
        with pytest.raises(RuntimeError, match="another Civ6 MCP sidecar owner"):
            second.__enter__()
