from __future__ import annotations

import os
import tomllib
from pathlib import Path

from pydantic import Field, field_validator

from .actions import ACTION_REGISTRY
from .codex_planner import CodexPlannerConfig
from .engine import EngineConfig
from .mcp_port import McpServerConfig
from .models import ExecutionMode, StrictModel
from .state_api import StateApiConfig


class RuntimeSection(StrictModel):
    database_path: str = "state/civ6-workflow.sqlite3"
    execution_mode: ExecutionMode = ExecutionMode.CONFIRM
    auto_end_turn: bool = False
    poll_interval_seconds: float = Field(default=1.0, gt=0)
    max_agent_calls_per_turn: int = Field(default=1, ge=0, le=2)
    max_turn_seconds: int = Field(default=300, ge=10)


class Civ6McpSection(StrictModel):
    command: str = "civ-mcp"
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)


class StateApiSection(StrictModel):
    base_url: str = "http://127.0.0.1:8000"
    timeout_seconds: float = Field(default=10.0, gt=0)
    startup_retry_seconds: float = Field(default=5.0, ge=0)


class CodexSection(StrictModel):
    backend: str = "responses"
    command: str = "codex"
    model: str | None = None
    reasoning_effort: str | None = "low"
    timeout_seconds: int = Field(default=120, ge=10)
    api_base_url: str = "https://api.openai.com/v1"
    api_key_env: str = "OPENAI_API_KEY"
    api_key_file: str | None = None
    connect_timeout_seconds: float = Field(default=10.0, gt=0)
    read_timeout_seconds: float = Field(default=90.0, gt=0)
    write_timeout_seconds: float = Field(default=30.0, gt=0)
    pool_timeout_seconds: float = Field(default=10.0, gt=0)
    max_http_attempts: int = Field(default=3, ge=1, le=6)
    retry_base_seconds: float = Field(default=0.5, ge=0, le=10)
    state_directory: str = "state/codex-planner"
    sandbox: str = "read-only"
    ephemeral: bool = True
    ignore_user_config: bool = False
    ignore_project_rules: bool = True
    disable_external_tools: bool = True
    disabled_mcp_servers: list[str] = Field(default_factory=list)
    config_overrides: list[str] = Field(default_factory=list)
    use_output_schema: bool = True
    env: dict[str, str] = Field(default_factory=dict)

    @field_validator("model", mode="before")
    @classmethod
    def empty_model_is_none(cls, value):
        return None if value == "" else value

    @field_validator("backend")
    @classmethod
    def valid_backend(cls, value: str) -> str:
        value = value.strip().lower()
        if value not in {"responses", "codex_cli"}:
            raise ValueError("codex.backend must be 'responses' or 'codex_cli'")
        return value


class GateSection(StrictModel):
    repeated_failure_threshold: int = Field(default=2, ge=1, le=10)
    default_cooldown_turns: int = Field(default=2, ge=0, le=20)


class SafetySection(StrictModel):
    # Actions the planner may propose at all.
    allowed_action_types: list[str] = Field(default_factory=list)
    # Subset that may execute without approval, and only in AUTO mode.
    auto_action_types: list[str] = Field(default_factory=list)
    allowed_tools: list[str] = Field(default_factory=list)


class AppConfig(StrictModel):
    runtime: RuntimeSection = Field(default_factory=RuntimeSection)
    civ6_mcp: Civ6McpSection = Field(default_factory=Civ6McpSection)
    state_api: StateApiSection = Field(default_factory=StateApiSection)
    codex: CodexSection = Field(default_factory=CodexSection)
    gate: GateSection = Field(default_factory=GateSection)
    safety: SafetySection = Field(default_factory=SafetySection)

    def engine_config(self) -> EngineConfig:
        auto_actions = set(self.safety.auto_action_types)
        allowed_actions = set(self.safety.allowed_action_types) or set(ACTION_REGISTRY)
        if not auto_actions <= allowed_actions:
            extra = sorted(auto_actions - allowed_actions)
            raise ValueError(
                "safety.auto_action_types must be a subset of "
                f"safety.allowed_action_types; extra={extra}"
            )
        allowed_tools = set(self.safety.allowed_tools)
        return EngineConfig(
            execution_mode=self.runtime.execution_mode,
            auto_end_turn=self.runtime.auto_end_turn,
            max_agent_calls_per_turn=self.runtime.max_agent_calls_per_turn,
            repeated_failure_threshold=self.gate.repeated_failure_threshold,
            default_cooldown_turns=self.gate.default_cooldown_turns,
            auto_action_types=auto_actions,
            allowed_action_types=allowed_actions,
            allowed_tools=allowed_tools,
        )

    def mcp_config(self) -> McpServerConfig:
        return McpServerConfig(
            command=self.civ6_mcp.command,
            args=self.civ6_mcp.args,
            env=self.civ6_mcp.env,
        )

    def state_api_config(self) -> StateApiConfig:
        return StateApiConfig(
            base_url=self.state_api.base_url,
            timeout_seconds=self.state_api.timeout_seconds,
            startup_retry_seconds=self.state_api.startup_retry_seconds,
        )

    def codex_config(self, base_directory: str | Path | None = None) -> CodexPlannerConfig:
        state_directory = Path(self.codex.state_directory).expanduser()
        if not state_directory.is_absolute() and base_directory is not None:
            state_directory = Path(base_directory) / state_directory
        api_key_file = None
        if self.codex.api_key_file:
            api_key_file = Path(
                os.path.expandvars(self.codex.api_key_file)
            ).expanduser()
            if not api_key_file.is_absolute() and base_directory is not None:
                api_key_file = Path(base_directory) / api_key_file
        return CodexPlannerConfig(
            backend=self.codex.backend,
            command=self.codex.command,
            model=self.codex.model,
            reasoning_effort=self.codex.reasoning_effort,
            timeout_seconds=self.codex.timeout_seconds,
            api_base_url=self.codex.api_base_url,
            api_key_env=self.codex.api_key_env,
            api_key_file=api_key_file,
            connect_timeout_seconds=self.codex.connect_timeout_seconds,
            read_timeout_seconds=self.codex.read_timeout_seconds,
            write_timeout_seconds=self.codex.write_timeout_seconds,
            pool_timeout_seconds=self.codex.pool_timeout_seconds,
            max_http_attempts=self.codex.max_http_attempts,
            retry_base_seconds=self.codex.retry_base_seconds,
            state_directory=state_directory,
            sandbox=self.codex.sandbox,
            ephemeral=self.codex.ephemeral,
            ignore_user_config=self.codex.ignore_user_config,
            ignore_project_rules=self.codex.ignore_project_rules,
            disable_external_tools=self.codex.disable_external_tools,
            disabled_mcp_servers=tuple(self.codex.disabled_mcp_servers),
            config_overrides=tuple(self.codex.config_overrides),
            use_output_schema=self.codex.use_output_schema,
            env=self.codex.env,
        )


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)
    config = AppConfig.model_validate(raw)
    if not config.safety.auto_action_types:
        raise ValueError("safety.auto_action_types must not be empty")
    if not config.safety.allowed_tools:
        raise ValueError("safety.allowed_tools must not be empty")
    config.engine_config()  # validate cross-field safety invariants eagerly
    return config
