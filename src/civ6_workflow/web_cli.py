from __future__ import annotations

import asyncio
import webbrowser
from pathlib import Path
from urllib.parse import quote

import typer

from .codex_planner import CodexPlanner
from .config import AppConfig, load_config
from .engine import WorkflowEngine
from .mcp_port import Civ6GamePort, Civ6McpClient
from .state_api import Civ6StateApi
from .store import WorkflowStore
from .web_ui import ControlPanelHTTPServer, ControlPanelState


def _store(config: AppConfig, config_path: Path) -> WorkflowStore:
    database_path = Path(config.runtime.database_path)
    if not database_path.is_absolute():
        database_path = config_path.parent / database_path
    return WorkflowStore(database_path)


async def _run_tick(config: AppConfig, config_path: Path):
    async with Civ6McpClient(config.mcp_config()) as client:
        async with Civ6StateApi(config.state_api_config()) as state_api:
            engine = WorkflowEngine(
                store=_store(config, config_path),
                game=Civ6GamePort(
                    client,
                    state_api,
                    allowed_tools=set(config.safety.allowed_tools),
                ),
                planner=CodexPlanner(config.codex_config(config_path.parent)),
                config=config.engine_config(),
            )
            return await asyncio.wait_for(
                engine.tick(), timeout=config.runtime.max_turn_seconds
            )


def serve(
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
    port: int = typer.Option(8765, min=1024, max=65535),
    open_browser: bool = typer.Option(False, "--open-browser/--no-open-browser"),
) -> None:
    """Start the localhost backend used by the browser control panel.

    The browser is the control entrypoint. Starting the service does not open a
    page or initiate a planner connection unless --open-browser is explicitly
    requested; the user connects the planner from the page.
    """

    config = config.resolve()
    loaded = load_config(config)
    store = _store(loaded, config)

    def run_tick():
        return asyncio.run(_run_tick(loaded, config))

    control = ControlPanelState(
        config=loaded,
        store=store,
        run_tick_callback=run_tick,
    )
    server = ControlPanelHTTPServer(("127.0.0.1", port), control)
    url = f"http://127.0.0.1:{port}/?token={quote(control.token)}"
    typer.echo("Civ6 workflow local backend")
    typer.echo(f"  Open this frontend URL: {url}")
    typer.echo("  The page initiates planner checks and workflow actions.")
    typer.echo("  Listening on localhost only. Press Ctrl+C to stop.")
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        typer.echo("\nStopping local backend.")
    finally:
        server.server_close()


def main() -> None:
    typer.run(serve)


if __name__ == "__main__":
    main()
