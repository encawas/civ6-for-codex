from __future__ import annotations

import asyncio
import shutil
import tempfile
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .bootstrap import (
    build_store,
    compose_live_runtime,
    compose_recording_runtime,
    compose_replay_runtime,
)
from .config import AppConfig, load_config
from .mcp_port import Civ6McpClient
from .models import RuntimeSnapshot, TaskStatus
from .replay import ReplayEngineSettings, SnapshotRecording
from .state_api import Civ6StateApi
from .store import WorkflowStore


app = typer.Typer(no_args_is_help=True, help="Civilization VI event workflow runtime")
console = Console()


def _store(config: AppConfig, config_path: Path) -> WorkflowStore:
    return build_store(config, config_path)


def _engine(
    config: AppConfig,
    config_path: Path,
    client: Civ6McpClient,
    state_api: Civ6StateApi,
):
    return compose_live_runtime(config, config_path, client, state_api).engine


async def _run_tick(config: AppConfig, config_path: Path):
    async with Civ6McpClient(config.mcp_config()) as client:
        async with Civ6StateApi(config.state_api_config()) as state_api:
            engine = _engine(config, config_path, client, state_api)
            return await asyncio.wait_for(
                engine.tick(), timeout=config.runtime.max_turn_seconds
            )


@app.command()
def doctor(config: Path = typer.Option(Path("config.toml"), exists=True)) -> None:
    """Check executables, database, structured state API and MCP tools."""

    loaded = load_config(config)
    rows: list[tuple[str, bool, str]] = []
    rows.append(("config", True, str(config.resolve())))
    database = _store(loaded, config)
    rows.append(("database", database.path.exists(), str(database.path)))

    civ_path = shutil.which(loaded.civ6_mcp.command)
    codex_path = shutil.which(loaded.codex.command)
    rows.append(("civ-mcp executable", civ_path is not None, civ_path or "not found"))
    rows.append(("codex executable", codex_path is not None, codex_path or "not found"))

    async def inspect_runtime() -> tuple[set[str], str, dict[str, bool]]:
        version = "not checked"
        if codex_path:
            process = await asyncio.create_subprocess_exec(
                loaded.codex.command,
                "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            version = (stdout or stderr).decode("utf-8", errors="replace").strip()

        tools: set[str] = set()
        endpoints: dict[str, bool] = {}
        if civ_path:
            async with Civ6McpClient(loaded.mcp_config()) as client:
                tools = await client.list_tools()
                async with Civ6StateApi(loaded.state_api_config()) as state_api:
                    for path in (
                        "/api/overview",
                        "/api/cities",
                        "/api/units",
                        "/api/identity",
                        "/api/notifications",
                        "/api/end-turn-blockers",
                        "/api/pending-diplomacy",
                        "/api/pending-trades",
                        "/api/tech-civics",
                        "/api/workflow/snapshot?include_units=false",
                    ):
                        try:
                            value = await state_api.get_optional(path)
                            endpoints[path] = value is not None
                        except Exception:
                            endpoints[path] = False
        return tools, version, endpoints

    try:
        tools, codex_version, endpoints = asyncio.run(inspect_runtime())
        rows.append(("codex version", bool(codex_version), codex_version))
        missing_tools = set(loaded.safety.allowed_tools) - tools if tools else set()
        rows.append(
            (
                "configured MCP tools",
                not missing_tools and bool(tools),
                "ok"
                if not missing_tools and tools
                else f"missing: {sorted(missing_tools)}",
            )
        )
        core_paths = {"/api/overview", "/api/cities", "/api/units"}
        missing_core = sorted(path for path in core_paths if not endpoints.get(path))
        rows.append(
            (
                "structured state API",
                not missing_core,
                "ok" if not missing_core else f"missing: {missing_core}",
            )
        )
        overlay_paths = {
            "/api/identity",
            "/api/notifications",
            "/api/end-turn-blockers",
            "/api/pending-diplomacy",
            "/api/pending-trades",
            "/api/tech-civics",
            "/api/workflow/snapshot?include_units=false",
        }
        missing_overlay = sorted(
            path for path in overlay_paths if not endpoints.get(path)
        )
        rows.append(
            (
                "workflow overlay endpoints",
                not missing_overlay,
                "ok" if not missing_overlay else f"missing: {missing_overlay}",
            )
        )
    except Exception as exc:
        rows.append(("runtime connection", False, str(exc)))

    table = Table(title="Civ6 workflow doctor")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Detail")
    for name, ok, detail in rows:
        table.add_row(name, "OK" if ok else "FAIL", detail)
    console.print(table)
    if any(not ok for _, ok, _ in rows):
        raise typer.Exit(code=1)


@app.command()
def tick(config: Path = typer.Option(Path("config.toml"), exists=True)) -> None:
    """Run one workflow cycle with one-off MCP and HTTP sessions."""

    loaded = load_config(config)
    result = asyncio.run(_run_tick(loaded, config))
    console.print_json(result.model_dump_json(indent=2))
    if result.paused:
        raise typer.Exit(code=2)


@app.command()
def run(
    config: Path = typer.Option(Path("config.toml"), exists=True),
    max_ticks: int | None = typer.Option(None, min=1),
) -> None:
    """Run workflow cycles over persistent MCP and structured-state sessions."""

    loaded = load_config(config)

    async def loop() -> None:
        count = 0
        async with Civ6McpClient(loaded.mcp_config()) as client:
            async with Civ6StateApi(loaded.state_api_config()) as state_api:
                engine = _engine(loaded, config, client, state_api)
                while max_ticks is None or count < max_ticks:
                    result = await asyncio.wait_for(
                        engine.tick(), timeout=loaded.runtime.max_turn_seconds
                    )
                    count += 1
                    console.print(
                        f"turn={result.turn} ended={result.turn_ended} "
                        f"agent={result.agent_invoked} tasks={len(result.executed_task_ids)} "
                        f"io_calls={result.metrics.mcp_call_count} "
                        f"seconds={result.metrics.total_seconds:.2f}"
                    )
                    if result.paused:
                        console.print(f"[yellow]paused:[/yellow] {result.pause_reason}")
                        return
                    await asyncio.sleep(loaded.runtime.poll_interval_seconds)

    asyncio.run(loop())


@app.command()
def record(
    output: Path = typer.Argument(..., dir_okay=False),
    config: Path = typer.Option(Path("config.toml"), exists=True),
    max_ticks: int = typer.Option(1, min=1),
) -> None:
    """Record live workflow snapshots and results into a replayable JSON file."""

    loaded = load_config(config)
    recorded_config = loaded.engine_config()
    tape = SnapshotRecording(
        engine_settings=ReplayEngineSettings(
            execution_mode=recorded_config.execution_mode,
            max_agent_calls_per_turn=recorded_config.max_agent_calls_per_turn,
            repeated_failure_threshold=recorded_config.repeated_failure_threshold,
            verification_attempts=recorded_config.verification_attempts,
            auto_action_types=sorted(recorded_config.auto_action_types),
            allowed_action_types=sorted(recorded_config.allowed_action_types),
            allowed_tools=sorted(recorded_config.allowed_tools),
        )
    )
    store = _store(loaded, config)

    def capture_store_state(snapshot: RuntimeSnapshot) -> None:
        tape.store_state = store.export_replay_state(snapshot.game_id)

    async def loop() -> list[dict]:
        results: list[dict] = []
        async with Civ6McpClient(loaded.mcp_config()) as client:
            async with Civ6StateApi(loaded.state_api_config()) as state_api:
                engine = compose_recording_runtime(
                    loaded,
                    config,
                    client,
                    state_api,
                    tape,
                    store=store,
                    on_first_snapshot=capture_store_state,
                ).engine
                for _ in range(max_ticks):
                    result = await asyncio.wait_for(
                        engine.tick(), timeout=loaded.runtime.max_turn_seconds
                    )
                    results.append(result.model_dump(mode="json"))
                    if result.paused:
                        break
                    await asyncio.sleep(loaded.runtime.poll_interval_seconds)
        return results

    try:
        results = asyncio.run(loop())
    finally:
        tape.save(output)
    console.print_json(
        data={
            "output": str(output.resolve()),
            "frames": len(tape.frames),
            "ticks": results,
        }
    )


@app.command()
def replay(
    recording: Path = typer.Argument(..., exists=True, dir_okay=False),
    database: Path | None = typer.Option(None),
    max_ticks: int | None = typer.Option(None, min=1),
    auto_end_turn: bool = typer.Option(False),
) -> None:
    """Replay a complete recording, or a deliberate prefix with --max-ticks."""

    tape = SnapshotRecording.load(recording)
    if not tape.frames:
        raise typer.BadParameter("recording contains no snapshot frames")
    temporary = None
    if database is None:
        temporary = tempfile.TemporaryDirectory(prefix="civ6-replay-")
        database = Path(temporary.name) / "workflow.sqlite3"
    composition = compose_replay_runtime(
        tape,
        database,
        auto_end_turn=auto_end_turn,
    )
    game = composition.game
    planner = composition.planner
    engine = composition.engine

    async def loop() -> list[dict]:
        results: list[dict] = []
        while game.remaining_frames and (max_ticks is None or len(results) < max_ticks):
            result = await engine.tick()
            results.append(result.model_dump(mode="json"))
            if result.paused:
                break
        return results

    try:
        results = asyncio.run(loop())
        remaining_frames = game.remaining_frames
        partial = max_ticks is not None and remaining_frames > 0
        if not partial:
            game.assert_finished()
            planner.assert_consumed()
        console.print_json(
            data={
                "ticks": results,
                "remaining_frames": remaining_frames,
                "remaining_planner_calls": planner.remaining_calls,
                "partial": partial,
            }
        )
    finally:
        if temporary is not None:
            temporary.cleanup()


@app.command()
def tasks(
    config: Path = typer.Option(Path("config.toml"), exists=True),
    status: list[TaskStatus] | None = typer.Option(None),
) -> None:
    """List tasks for the most recently observed game."""

    loaded = load_config(config)
    store = _store(loaded, config)
    game_id = store.get_meta("last_game_id")
    if not game_id:
        console.print("No game has been observed yet.")
        raise typer.Exit(code=1)
    records = store.list_tasks(game_id, status)
    table = Table(title=f"Workflow tasks: {game_id}")
    for column in ("task", "status", "due", "entity", "action", "reason"):
        table.add_column(column)
    for record in records:
        table.add_row(
            record.task_id,
            record.status.value,
            str(record.due_turn),
            f"{record.entity_type}:{record.entity_id}",
            record.action_type,
            record.reason,
        )
    console.print(table)


@app.command()
def approve(
    task_id: str,
    config: Path = typer.Option(Path("config.toml"), exists=True),
) -> None:
    """Approve one task that is waiting for confirmation."""

    loaded = load_config(config)
    store = _store(loaded, config)
    game_id = store.get_meta("last_game_id")
    if not game_id:
        console.print("No game has been observed yet.")
        raise typer.Exit(code=1)
    if not store.approve_task(game_id, task_id):
        console.print(f"Task {task_id} was not awaiting confirmation.")
        raise typer.Exit(code=1)
    console.print(f"Approved {task_id}.")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
