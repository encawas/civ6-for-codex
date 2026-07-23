import asyncio
from pathlib import Path

import pytest

from civ6_workflow.characterization import RecordingPlanner
from civ6_workflow.codex_planner import (
    CodexPlanner,
    CodexPlannerConfig,
    PlannerError,
)
from civ6_workflow.models import AgentRequest, ExecutionMode, PlanBundle
from civ6_workflow.workflow_prompt import EXTENDED_SYSTEM_INSTRUCTIONS


def test_planner_prompt_names_versioned_input_contracts():
    for contract_key in (
        "constraints.action_argument_contracts",
        "constraints.action_entity_types",
        "constraints.entity_id_arguments",
        "constraints.condition_contracts",
    ):
        assert contract_key in EXTENDED_SYSTEM_INSTRUCTIONS
    assert "constraints.information_tool_arguments" in EXTENDED_SYSTEM_INSTRUCTIONS
    assert "never emit arguments listed in injected_by_runtime" in (
        EXTENDED_SYSTEM_INSTRUCTIONS
    )
    assert "$..." in EXTENDED_SYSTEM_INSTRUCTIONS
    assert "never emit an unresolved `$...` placeholder" in (
        EXTENDED_SYSTEM_INSTRUCTIONS
    )


def test_codex_command_is_noninteractive_and_read_only(tmp_path: Path):
    planner = CodexPlanner(
        CodexPlannerConfig(
            command="codex",
            model="gpt-test",
            reasoning_effort="medium",
            sandbox="read-only",
            ignore_user_config=True,
            disabled_mcp_servers=("civ6", "openaiDeveloperDocs", "node_repl"),
            config_overrides=('model_provider="OpenAI"',),
        )
    )
    command = planner.build_command(
        schema_path=tmp_path / "schema.json",
        output_path=tmp_path / "result.json",
        working_directory=tmp_path,
    )

    assert command[:2] == ["codex", "exec"]
    assert "--ephemeral" in command
    assert "--ignore-user-config" in command
    assert "--skip-git-repo-check" in command
    assert command[command.index("--sandbox") + 1] == "read-only"
    assert command[command.index("--model") + 1] == "gpt-test"
    config_overrides = [
        command[index + 1]
        for index, value in enumerate(command[:-1])
        if value == "--config"
    ]
    assert 'approval_policy="never"' in config_overrides
    assert 'model_reasoning_effort="medium"' in config_overrides
    assert "features.shell_tool=false" in config_overrides
    assert 'web_search="disabled"' in config_overrides
    assert any(value.startswith('sqlite_home="') for value in config_overrides)
    assert any(value.startswith('log_dir="') for value in config_overrides)
    assert 'history.persistence="none"' in config_overrides
    assert not any(value.startswith("mcp_servers.") for value in config_overrides)
    assert 'model_provider="OpenAI"' in config_overrides
    assert "--output-schema" in command
    assert "--output-last-message" in command
    assert command[-1] == "-"
    assert "--dangerously-bypass-approvals-and-sandbox" not in command
    assert "--yolo" not in command


def test_codex_command_disables_configured_mcp_servers(tmp_path: Path):
    planner = CodexPlanner(
        CodexPlannerConfig(
            ignore_user_config=False,
            disabled_mcp_servers=("civ6", "openaiDeveloperDocs"),
        )
    )

    command = planner.build_command(
        schema_path=tmp_path / "schema.json",
        output_path=tmp_path / "result.json",
        working_directory=tmp_path,
    )
    config_overrides = [
        command[index + 1]
        for index, value in enumerate(command[:-1])
        if value == "--config"
    ]

    assert "mcp_servers.civ6.enabled=false" in config_overrides
    assert "mcp_servers.openaiDeveloperDocs.enabled=false" in config_overrides


def test_codex_command_can_skip_remote_output_schema(tmp_path: Path):
    planner = CodexPlanner(CodexPlannerConfig(use_output_schema=False))

    command = planner.build_command(
        schema_path=tmp_path / "schema.json",
        output_path=tmp_path / "result.json",
        working_directory=tmp_path,
    )

    assert "--output-schema" not in command
    assert "--output-last-message" in command


def test_cli_planner_keeps_request_artifacts_in_project_state(
    tmp_path: Path, monkeypatch
):
    state_directory = tmp_path / "state" / "codex-planner"
    planner = CodexPlanner(
        CodexPlannerConfig(
            backend="codex_cli",
            command=str(Path(__file__).resolve()),
            state_directory=state_directory,
            use_output_schema=False,
        )
    )
    recording = RecordingPlanner(planner)
    request = AgentRequest(
        request_id="req_persistent_test",
        turn=1,
        execution_mode=ExecutionMode.READONLY,
        trigger_events=[],
        constraints={"max_tasks": 1},
    )

    class FakeProcess:
        returncode = 0

        def __init__(self, output_path: Path):
            self.output_path = output_path

        async def communicate(self, payload: bytes):
            assert payload
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.output_path.write_text(
                PlanBundle(summary="persistent test").model_dump_json(),
                encoding="utf-8",
            )
            return b"", b""

        def kill(self):
            self.returncode = -9

        async def wait(self):
            return self.returncode

    async def fake_create_subprocess_exec(*command, **kwargs):
        output_index = command.index("--output-last-message") + 1
        return FakeProcess(Path(command[output_index]))

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    async def scenario():
        with recording.logical_request_scope("cli-success"):
            return await recording.plan(request)

    bundle = asyncio.run(scenario())

    request_directory = state_directory / "requests" / request.request_id
    assert bundle.summary == "persistent test"
    assert (request_directory / "request.txt").exists()
    assert (request_directory / "plan.schema.json").exists()
    assert (request_directory / "plan.json").exists()
    assert planner.last_diagnostics is not None
    assert planner.last_diagnostics["state_directory"] == str(state_directory.resolve())
    assert planner.last_diagnostics["attempt_count"] == 1
    assert recording.summary.logical_requests == 1
    assert recording.summary.provider_attempts == 1


def test_recording_planner_counts_started_cli_with_invalid_output(
    tmp_path: Path, monkeypatch
):
    planner = CodexPlanner(
        CodexPlannerConfig(
            backend="codex_cli",
            command=str(Path(__file__).resolve()),
            state_directory=tmp_path / "invalid-output",
            use_output_schema=False,
        )
    )
    recording = RecordingPlanner(planner)
    request = AgentRequest(
        request_id="req_invalid_output",
        turn=1,
        execution_mode=ExecutionMode.READONLY,
        trigger_events=[],
    )

    class InvalidOutputProcess:
        returncode = 0

        async def communicate(self, payload: bytes):
            output_path = (
                Path(planner.config.state_directory)
                / "requests"
                / request.request_id
                / "plan.json"
            )
            output_path.write_text("{not valid json", encoding="utf-8")
            return b"", b""

        def kill(self):
            self.returncode = -9

        async def wait(self):
            return self.returncode

    async def fake_create_subprocess_exec(*command, **kwargs):
        return InvalidOutputProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    async def scenario():
        with recording.logical_request_scope("cli-invalid-output"):
            await recording.plan(request)

    with pytest.raises(PlannerError, match="invalid plan"):
        asyncio.run(scenario())

    assert planner.last_diagnostics is not None
    assert planner.last_diagnostics["attempt_count"] == 1
    assert recording.summary.logical_requests == 1
    assert recording.summary.provider_attempts == 1


def test_recording_planner_does_not_count_missing_cli_executable(
    tmp_path: Path, monkeypatch
):
    planner = CodexPlanner(
        CodexPlannerConfig(
            backend="codex_cli",
            command="missing-codex",
            state_directory=tmp_path / "missing-cli",
            use_output_schema=False,
        )
    )
    recording = RecordingPlanner(planner)
    request = AgentRequest(
        request_id="req_missing_cli",
        turn=1,
        execution_mode=ExecutionMode.READONLY,
        trigger_events=[],
    )

    async def fake_create_subprocess_exec(*command, **kwargs):
        raise FileNotFoundError("missing-codex")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    async def scenario():
        with recording.logical_request_scope("cli-missing"):
            await recording.plan(request)

    with pytest.raises(PlannerError, match="executable was not found"):
        asyncio.run(scenario())

    assert planner.last_diagnostics is not None
    assert planner.last_diagnostics["attempt_count"] == 0
    assert recording.summary.logical_requests == 1
    assert recording.summary.provider_attempts == 0


@pytest.mark.parametrize(
    ("mode", "error_match"),
    [
        ("nonzero", "exit code 7"),
        ("missing_output", "without writing"),
        ("empty_output", "empty plan"),
        ("too_many_tasks", "max_tasks=1"),
    ],
)
def test_started_cli_failure_paths_record_current_diagnostics(
    tmp_path: Path, monkeypatch, mode: str, error_match: str
):
    planner = CodexPlanner(
        CodexPlannerConfig(
            backend="codex_cli",
            command=str(Path(__file__).resolve()),
            state_directory=tmp_path / mode,
            use_output_schema=False,
        )
    )
    request = AgentRequest(
        request_id=f"req_{mode}",
        turn=1,
        execution_mode=ExecutionMode.READONLY,
        trigger_events=[],
        constraints={"max_tasks": 1},
    )

    class FailureProcess:
        returncode = 7 if mode == "nonzero" else 0

        async def communicate(self, payload: bytes):
            output_path = (
                Path(planner.config.state_directory)
                / "requests"
                / request.request_id
                / "plan.json"
            )
            if mode == "empty_output":
                output_path.write_text("   ", encoding="utf-8")
            elif mode == "too_many_tasks":
                bundle = PlanBundle(
                    summary="too many",
                    tasks=[
                        {
                            "task_id": f"task-{index}",
                            "action_type": "unit_skip",
                            "entity_type": "unit",
                            "entity_id": index,
                            "due_turn": 1,
                            "reason": "diagnostics coverage",
                        }
                        for index in range(2)
                    ],
                )
                output_path.write_text(bundle.model_dump_json(), encoding="utf-8")
            return b"", b"cli failure"

        def kill(self):
            self.returncode = -9

        async def wait(self):
            return self.returncode

    async def fake_create_subprocess_exec(*command, **kwargs):
        return FailureProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(PlannerError, match=error_match):
        asyncio.run(planner.plan(request))

    assert planner.last_diagnostics is not None
    assert planner.last_diagnostics["attempt_count"] == 1
    assert planner.last_diagnostics["error_body"]


def test_timed_out_cli_records_started_provider_attempt(tmp_path: Path, monkeypatch):
    planner = CodexPlanner(
        CodexPlannerConfig(
            backend="codex_cli",
            command=str(Path(__file__).resolve()),
            state_directory=tmp_path / "timeout",
            timeout_seconds=0,
            use_output_schema=False,
        )
    )
    request = AgentRequest(
        request_id="req_timeout",
        turn=1,
        execution_mode=ExecutionMode.READONLY,
        trigger_events=[],
    )

    class TimeoutProcess:
        returncode = None

        def __init__(self):
            self.released = asyncio.Event()

        async def communicate(self, payload: bytes):
            await self.released.wait()
            return b"", b"timed out"

        def kill(self):
            self.returncode = -9
            self.released.set()

        async def wait(self):
            return self.returncode

    async def fake_create_subprocess_exec(*command, **kwargs):
        return TimeoutProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(PlannerError, match="timed out"):
        asyncio.run(planner.plan(request))

    assert planner.last_diagnostics is not None
    assert planner.last_diagnostics["attempt_count"] == 1
    assert planner.last_diagnostics["error_body"]


def test_cli_started_hook_precedes_subprocess_creation(tmp_path: Path, monkeypatch):
    planner = CodexPlanner(
        CodexPlannerConfig(
            backend="codex_cli",
            command=str(Path(__file__).resolve()),
            state_directory=tmp_path / "prelaunch",
            use_output_schema=False,
        )
    )
    request = AgentRequest(
        request_id="req-prelaunch-crash",
        turn=1,
        execution_mode=ExecutionMode.READONLY,
        trigger_events=[],
    )
    phases = []
    spawned = False

    class CrashBeforeSpawn(RuntimeError):
        pass

    async def hook(phase, details):
        phases.append((phase, details["attempt_number"]))
        raise CrashBeforeSpawn("persisted STARTED boundary")

    async def fake_create_subprocess_exec(*command, **kwargs):
        nonlocal spawned
        spawned = True
        raise AssertionError("subprocess must not start before STARTED is persisted")

    planner.set_provider_attempt_hook(hook)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(CrashBeforeSpawn, match="STARTED"):
        asyncio.run(planner.plan(request))

    assert phases == [("started", 1)]
    assert spawned is False
