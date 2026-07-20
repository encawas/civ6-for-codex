from __future__ import annotations

import asyncio
import webbrowser
from pathlib import Path
from urllib.parse import quote

import typer

from .bootstrap import build_store, compose_control_panel, open_live_runtime
from .config import AppConfig, load_config
from .store import WorkflowStore


def _store(config: AppConfig, config_path: Path) -> WorkflowStore:
    return build_store(config, config_path)


async def _run_tick(config: AppConfig, config_path: Path):
    async with open_live_runtime(
        config,
        config_path,
        planner_base_directory=config_path.parent,
    ) as runtime:
        return await asyncio.wait_for(
            runtime.engine.tick(), timeout=config.runtime.max_turn_seconds
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

    def run_tick():
        return asyncio.run(_run_tick(loaded, config))

    composition = compose_control_panel(
        loaded,
        config,
        address=("127.0.0.1", port),
        run_tick_callback=run_tick,
    )
    control = composition.control
    server = composition.server
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
