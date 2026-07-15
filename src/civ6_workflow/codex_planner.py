from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .models import AgentRequest, PlanBundle


class PlannerError(RuntimeError):
    pass


class Planner(Protocol):
    async def plan(self, request: AgentRequest) -> PlanBundle: ...


@dataclass(slots=True)
class CodexPlannerConfig:
    # The direct Responses API is the runtime path. ``codex_cli`` remains an
    # explicit diagnostic fallback, not the default turn-control mechanism.
    backend: str = "responses"
    command: str = "codex"
    model: str | None = None
    reasoning_effort: str | None = "low"
    timeout_seconds: int = 120
    api_base_url: str = "https://api.openai.com/v1"
    api_key_env: str = "OPENAI_API_KEY"
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 90.0
    write_timeout_seconds: float = 30.0
    pool_timeout_seconds: float = 10.0
    # Persistent project-local directory used only by the CLI diagnostic
    # fallback. Responses API planning does not create Codex process state.
    state_directory: str | Path = "state/codex-planner"
    sandbox: str = "read-only"
    ephemeral: bool = True
    ignore_user_config: bool = False
    ignore_project_rules: bool = True
    disable_external_tools: bool = True
    disabled_mcp_servers: tuple[str, ...] = ()
    config_overrides: tuple[str, ...] = ()
    use_output_schema: bool = True
    env: dict[str, str] | None = None


SYSTEM_INSTRUCTIONS = """You are the strategic planning worker for a Civilization VI workflow runtime.

You are not connected to the game. You must not attempt to edit files, run shell commands, call MCP tools, or perform actions directly. The only authoritative state is the JSON request below.

Your job is to return one structured plan bundle for the triggering events. Prefer updating plans for several future turns over creating one immediate action. Use only the action types listed in request.constraints.allowed_action_types. Never invent entity IDs. High-impact or irreversible actions must set requires_confirmation=true. If the supplied state is insufficient, return requires_human_review=true and explain what focused state must be queried; do not guess.

Rules:
1. Existing approved plans should be preserved unless an event invalidates them.
2. Ordinary continuation work should become scheduled tasks.
3. Each entity may have at most one task due on the same turn.
4. Do not create diplomacy, war, peace, policy, city placement, purchase, city capture, or World Congress actions unless an allowed action type explicitly exists.
5. Every task needs a concise reason, preconditions, postconditions, invalidators, due_turn, and risk.
6. Postconditions must prove the game state changed as intended, using only request.constraints.supported_condition_types.
7. City plans should use a followup_queue of production items. Builder plans should use assigned_unit_id, a stepwise path, and a target with improvement_type.
8. Return only the JSON object required by the output schema.
"""


class CodexPlanner:
    """Runtime planner with a direct HTTP backend and isolated CLI fallback."""

    def __init__(self, config: CodexPlannerConfig):
        self.config = config
        self.last_diagnostics: dict[str, Any] | None = None

    def build_command(
        self, *, schema_path: Path, output_path: Path, working_directory: Path
    ) -> list[str]:
        command = [self.config.command, "exec"]
        if self.config.ephemeral:
            command.append("--ephemeral")
        if self.config.ignore_user_config:
            command.append("--ignore-user-config")
        command.extend(["--skip-git-repo-check", "--sandbox", self.config.sandbox])
        if self.config.ignore_project_rules:
            command.append("--ignore-rules")
        command.extend(["--config", 'approval_policy="never"'])
        if self.config.reasoning_effort:
            command.extend(
                [
                    "--config",
                    f'model_reasoning_effort="{self.config.reasoning_effort}"',
                ]
            )
        if self.config.disable_external_tools:
            for override in (
                "features.apps=false",
                "features.code_mode.enabled=false",
                "features.goals=false",
                "features.hooks=false",
                "features.memories=false",
                "features.multi_agent=false",
                "features.remote_plugin=false",
                "features.shell_tool=false",
                "features.skill_mcp_dependency_install=false",
                'web_search="disabled"',
            ):
                command.extend(["--config", override])
        isolated_root = working_directory.as_posix()
        for override in (
            f'sqlite_home="{isolated_root}/state"',
            f'log_dir="{isolated_root}/log"',
            'history.persistence="none"',
        ):
            command.extend(["--config", override])
        if not self.config.ignore_user_config:
            for server_name in self.config.disabled_mcp_servers:
                command.extend(
                    ["--config", f"mcp_servers.{server_name}.enabled=false"]
                )
        for override in self.config.config_overrides:
            command.extend(["--config", override])
        if self.config.model:
            command.extend(["--model", self.config.model])
        command.extend(["--cd", str(working_directory)])
        if self.config.use_output_schema:
            command.extend(["--output-schema", str(schema_path)])
        command.extend(["--output-last-message", str(output_path), "-"])
        return command

    async def plan(self, request: AgentRequest) -> PlanBundle:
        backend = self.config.backend.strip().lower()
        if backend == "responses":
            from .responses_planner import ResponsesPlanner

            delegate = ResponsesPlanner(self.config, SYSTEM_INSTRUCTIONS, PlannerError)
            try:
                return await delegate.plan(request)
            finally:
                self.last_diagnostics = delegate.last_diagnostics
        if backend != "codex_cli":
            raise PlannerError(
                f"unsupported planner backend {self.config.backend!r}; "
                "expected 'responses' or 'codex_cli'"
            )
        return await self._plan_with_cli(request)

    async def _plan_with_cli(self, request: AgentRequest) -> PlanBundle:
        started = asyncio.get_running_loop().time()
        prompt = self._build_prompt(request)
        root = Path(self.config.state_directory).expanduser().resolve()
        request_dir = root / "requests" / request.request_id
        request_dir.mkdir(parents=True, exist_ok=True)
        schema_path = request_dir / "plan.schema.json"
        output_path = request_dir / "plan.json"
        prompt_path = request_dir / "request.txt"
        schema_path.write_text(
            json.dumps(PlanBundle.model_json_schema(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        prompt_path.write_text(prompt, encoding="utf-8")
        command = self.build_command(
            schema_path=schema_path,
            output_path=output_path,
            working_directory=root,
        )
        env = {**os.environ, **(self.config.env or {})}
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
        except FileNotFoundError as exc:
            raise PlannerError(
                f"Codex executable was not found: {self.config.command}"
            ) from exc

        communication = asyncio.create_task(
            process.communicate(prompt.encode("utf-8"))
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                asyncio.shield(communication),
                timeout=self.config.timeout_seconds,
            )
        except TimeoutError as exc:
            process.kill()
            try:
                stdout, stderr = await asyncio.wait_for(communication, timeout=5)
            except TimeoutError:
                communication.cancel()
                await process.wait()
                stdout, stderr = b"", b""
            self.last_diagnostics = {
                "backend": "codex_cli",
                "completion_seconds": asyncio.get_running_loop().time() - started,
                "request_bytes": len(prompt.encode("utf-8")),
                "state_directory": str(root),
                "request_directory": str(request_dir),
                "error_body": self._diagnostic_output(stdout, stderr),
            }
            raise PlannerError(
                f"Codex planning timed out after {self.config.timeout_seconds}s. "
                f"{self._diagnostic_output(stdout, stderr)}"
            ) from exc

        if process.returncode != 0:
            self.last_diagnostics = {
                "backend": "codex_cli",
                "completion_seconds": asyncio.get_running_loop().time() - started,
                "request_bytes": len(prompt.encode("utf-8")),
                "state_directory": str(root),
                "request_directory": str(request_dir),
                "error_body": self._diagnostic_output(stdout, stderr),
            }
            raise PlannerError(
                "Codex planning failed "
                f"with exit code {process.returncode}. "
                f"{self._diagnostic_output(stdout, stderr)}"
            )
        if not output_path.exists():
            raise PlannerError("Codex completed without writing the final message file")
        raw = output_path.read_text(encoding="utf-8").strip()
        if not raw:
            raise PlannerError("Codex returned an empty plan")
        try:
            bundle = PlanBundle.model_validate_json(raw)
        except Exception as exc:
            raise PlannerError(f"Codex returned an invalid plan: {exc}") from exc
        max_tasks = int(request.constraints.get("max_tasks", 8))
        if len(bundle.tasks) > max_tasks:
            raise PlannerError(
                f"Codex returned {len(bundle.tasks)} tasks; max_tasks={max_tasks}"
            )
        self.last_diagnostics = {
            "backend": "codex_cli",
            "completion_seconds": asyncio.get_running_loop().time() - started,
            "request_bytes": len(prompt.encode("utf-8")),
            "response_bytes": len(raw.encode("utf-8")),
            "state_directory": str(root),
            "request_directory": str(request_dir),
        }
        return bundle

    @staticmethod
    def _diagnostic_output(stdout: bytes, stderr: bytes) -> str:
        stderr_text = stderr.decode("utf-8", errors="replace")[-4000:]
        stdout_text = stdout.decode("utf-8", errors="replace")[-2000:]
        return f"stderr={stderr_text!r} stdout={stdout_text!r}"

    @staticmethod
    def _build_prompt(request: AgentRequest) -> str:
        request_json = request.model_dump_json(indent=2)
        return f"{SYSTEM_INSTRUCTIONS}\n\n<agent_request>\n{request_json}\n</agent_request>\n"
