import asyncio
from pathlib import Path

from civ6_workflow.codex_planner import CodexPlanner, CodexPlannerConfig
from civ6_workflow.models import AgentRequest, ExecutionMode, PlanBundle


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


def test_cli_planner_keeps_request_artifacts_in_project_state(tmp_path: Path, monkeypatch):
    state_directory = tmp_path / "state" / "codex-planner"
    planner = CodexPlanner(
        CodexPlannerConfig(
            backend="codex_cli",
            state_directory=state_directory,
            use_output_schema=False,
        )
    )
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

    bundle = asyncio.run(planner.plan(request))

    request_directory = state_directory / "requests" / request.request_id
    assert bundle.summary == "persistent test"
    assert (request_directory / "request.txt").exists()
    assert (request_directory / "plan.schema.json").exists()
    assert (request_directory / "plan.json").exists()
    assert planner.last_diagnostics is not None
    assert planner.last_diagnostics["state_directory"] == str(state_directory.resolve())
