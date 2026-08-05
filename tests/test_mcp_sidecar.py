import json
import asyncio

import pytest

import civ6_workflow.mcp_port as mcp_port


def _process(process_id: int, *, parent_process_id: int | None = None):
    return mcp_port._WindowsProcessIdentity(
        process_id=process_id,
        parent_process_id=(
            mcp_port.os.getpid() if parent_process_id is None else parent_process_id
        ),
        creation_time=f"created-{process_id}",
        executable_path="c:/python/python.exe",
        command_line_hash=f"command-{process_id}",
    )


def test_sidecar_guard_reclaims_only_recorded_stale_processes(tmp_path, monkeypatch):
    running = {101: _process(101), 102: _process(102)}
    terminated = []
    monkeypatch.setattr(
        mcp_port,
        "_windows_civ_mcp_processes",
        lambda: dict(running),
    )

    def terminate(processes):
        terminated.append(set(processes))
        for process_id in processes:
            running.pop(process_id, None)

    monkeypatch.setattr(
        mcp_port,
        "_terminate_windows_sidecar_processes",
        terminate,
    )
    guard = mcp_port._McpSidecarGuard("owner", state_directory=tmp_path, windows=True)
    guard.owner_path.write_text(
        json.dumps(
            {
                "schema_version": "civ6-sidecar-owner/v2",
                "processes": [_process(101).to_json(), _process(102).to_json()],
            }
        ),
        encoding="utf-8",
    )

    with guard:
        assert terminated == [{101, 102}]
        assert not guard.owner_path.exists()


def test_sidecar_guard_rejects_unowned_existing_process(tmp_path, monkeypatch):
    monkeypatch.setattr(
        mcp_port,
        "_windows_civ_mcp_processes",
        lambda: {201: _process(201)},
    )
    guard = mcp_port._McpSidecarGuard("unowned", state_directory=tmp_path, windows=True)

    with pytest.raises(RuntimeError, match="unowned civ6-mcp sidecar"):
        guard.__enter__()


def test_sidecar_guard_records_and_cleans_owned_processes(tmp_path, monkeypatch):
    running = {}
    terminated = []
    monkeypatch.setattr(
        mcp_port,
        "_windows_civ_mcp_processes",
        lambda: dict(running),
    )

    def terminate(processes):
        terminated.append(set(processes))
        running.clear()

    monkeypatch.setattr(
        mcp_port,
        "_terminate_windows_sidecar_processes",
        terminate,
    )
    guard = mcp_port._McpSidecarGuard("owned", state_directory=tmp_path, windows=True)

    with guard:
        running.update({301: _process(301), 302: _process(302)})
        guard.record_started(dict(running))
        payload = json.loads(guard.owner_path.read_text(encoding="utf-8"))
        assert payload["schema_version"] == "civ6-sidecar-owner/v2"
        assert [item["process_id"] for item in payload["processes"]] == [301, 302]

    assert terminated == [{301, 302}]
    assert not guard.owner_path.exists()


def test_sidecar_guard_is_exclusive(tmp_path):
    first = mcp_port._McpSidecarGuard(
        "exclusive", state_directory=tmp_path, windows=False
    )
    second = mcp_port._McpSidecarGuard(
        "different-command", state_directory=tmp_path, windows=False
    )

    with first:
        with pytest.raises(RuntimeError, match="another Civ6 MCP sidecar owner"):
            second.__enter__()


def test_pid_reuse_never_authorizes_termination(monkeypatch):
    expected = {401: _process(401)}
    reused = {401: _process(401, parent_process_id=999)}
    taskkill_calls = []
    monkeypatch.setattr(
        mcp_port,
        "_windows_civ_mcp_processes",
        lambda: dict(reused),
    )
    monkeypatch.setattr(
        mcp_port.subprocess,
        "run",
        lambda *_args, **_kwargs: taskkill_calls.append(True),
    )

    with pytest.raises(RuntimeError, match="identity changed"):
        mcp_port._terminate_windows_sidecar_processes(expected)

    assert taskkill_calls == []


def test_process_matching_uses_executable_or_exact_module_tokens():
    assert mcp_port._is_civ_mcp_process(
        name="python.exe",
        executable_path="C:/Python/python.exe",
        command_line="python.exe -m civ_mcp --transport stdio",
    )
    assert not mcp_port._is_civ_mcp_process(
        name="python.exe",
        executable_path="C:/Python/python.exe",
        command_line="python.exe C:/projects/civ-mcp/scripts/worker.py",
    )


def test_sidecar_guard_refuses_process_from_another_parent(tmp_path):
    guard = mcp_port._McpSidecarGuard(
        "owner",
        state_directory=tmp_path,
        windows=True,
    )

    with pytest.raises(RuntimeError, match="not attributable"):
        guard.record_started({501: _process(501, parent_process_id=999)})

    assert not guard.owner_path.exists()


def test_broken_session_rejects_calls_and_shutdown_is_bounded():
    class _HangingStack:
        async def aclose(self):
            await asyncio.sleep(60)

    class _Guard:
        def __init__(self):
            self.closed = False

        async def aclose(self):
            self.closed = True

    async def scenario():
        client = mcp_port.Civ6McpClient(
            mcp_port.McpServerConfig(shutdown_timeout_seconds=0.01)
        )
        client.state = mcp_port.McpClientState.READY
        client.session = object()
        client._mark_broken()
        with pytest.raises(mcp_port.McpClientNotConnectedError):
            client._require_session()

        guard = _Guard()
        client._stack = _HangingStack()
        client._sidecar_guard = guard
        await client.__aexit__(None, None, None)

        assert guard.closed is True
        assert client.state is mcp_port.McpClientState.CLOSED
        assert client.session is None

    asyncio.run(scenario())


def test_duplicate_client_enter_is_rejected_before_spawning():
    async def scenario():
        client = mcp_port.Civ6McpClient(mcp_port.McpServerConfig())
        client.state = mcp_port.McpClientState.READY
        with pytest.raises(RuntimeError, match="cannot start"):
            await client.__aenter__()

    asyncio.run(scenario())


def test_fail_closed_preserves_mismatched_owner_record(tmp_path, monkeypatch):
    recorded = _process(601)
    reused = _process(601, parent_process_id=999)
    monkeypatch.setattr(
        mcp_port,
        "_windows_civ_mcp_processes",
        lambda: {601: reused},
    )
    guard = mcp_port._McpSidecarGuard(
        "owner",
        state_directory=tmp_path,
        windows=True,
    )
    guard.owner_path.write_text(
        json.dumps(
            {
                "schema_version": "civ6-sidecar-owner/v2",
                "processes": [recorded.to_json()],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="unowned"):
        guard.__enter__()

    assert guard.owner_path.exists()


def test_posix_orphaned_state_api_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(mcp_port.sys, "platform", "linux")
    monkeypatch.setattr(mcp_port, "_state_api_port_is_open", lambda: True)
    guard = mcp_port._McpSidecarGuard(
        "owner",
        state_directory=tmp_path,
        windows=False,
    )

    with pytest.raises(RuntimeError, match="port 8000"):
        guard.__enter__()


def test_list_tools_timeout_marks_session_broken():
    class _Session:
        async def list_tools(self):
            await asyncio.sleep(60)

    async def scenario():
        client = mcp_port.Civ6McpClient(
            mcp_port.McpServerConfig(list_tools_timeout_seconds=0.01)
        )
        client.state = mcp_port.McpClientState.READY
        client.session = _Session()

        with pytest.raises(TimeoutError):
            await client.list_tools()

        assert client.state is mcp_port.McpClientState.BROKEN
        assert client.session_broken is True

    asyncio.run(scenario())


def test_config_maps_sidecar_timeouts():
    config = mcp_port.McpServerConfig(
        startup_timeout_seconds=11,
        shutdown_timeout_seconds=12,
        list_tools_timeout_seconds=13,
        read_query_timeout_seconds=14,
    )

    assert config.startup_timeout_seconds == 11
    assert config.shutdown_timeout_seconds == 12
    assert config.list_tools_timeout_seconds == 13
    assert config.read_query_timeout_seconds == 14


def test_client_startup_timeout_is_bounded():
    async def scenario():
        client = mcp_port.Civ6McpClient(
            mcp_port.McpServerConfig(startup_timeout_seconds=0.01)
        )

        async def hang():
            await asyncio.sleep(60)

        client._start = hang
        with pytest.raises(TimeoutError, match="startup exceeded"):
            await client.__aenter__()
        assert client.state is mcp_port.McpClientState.CLOSED

    asyncio.run(scenario())
