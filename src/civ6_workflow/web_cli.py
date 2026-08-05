from __future__ import annotations

import asyncio
import webbrowser
import threading
from pathlib import Path
from collections.abc import Callable
from urllib.parse import quote

from typing import Any
import typer

from .bootstrap import build_store, compose_control_panel, open_live_runtime
from .config import AppConfig, load_config
from .store import WorkflowStore


def _store(config: AppConfig, config_path: Path) -> WorkflowStore:
    return build_store(config, config_path)


class _RuntimeWorker:
    def __init__(
        self,
        context_factory: Callable[[], Any],
        *,
        startup_timeout_seconds: float,
        tick_timeout_seconds: float,
    ):
        self._context_factory = context_factory
        self._startup_timeout_seconds = startup_timeout_seconds
        self._tick_timeout_seconds = tick_timeout_seconds
        self._start_lock = threading.Lock()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._runtime: Any | None = None
        self._stop_event: asyncio.Event | None = None
        self._startup_error: BaseException | None = None

    def _thread_main(self) -> None:
        async def supervise() -> None:
            try:
                async with self._context_factory() as composition:
                    self._loop = asyncio.get_running_loop()
                    self._runtime = composition.runtime
                    self._stop_event = asyncio.Event()
                    self._ready.set()
                    await self._stop_event.wait()
            except BaseException as exc:
                self._startup_error = exc
                self._ready.set()
            finally:
                self._runtime = None
                self._loop = None
                self._stop_event = None

        asyncio.run(supervise())

    def _ensure_started(self) -> None:
        with self._start_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._ready.clear()
            self._startup_error = None
            self._thread = threading.Thread(
                target=self._thread_main,
                name="civ6-runtime-worker",
                daemon=False,
            )
            self._thread.start()
        if not self._ready.wait(self._startup_timeout_seconds):
            self.close()
            raise TimeoutError("persistent Civ6 Runtime worker startup timed out")
        if self._startup_error is not None:
            error = self._startup_error
            self.close()
            raise RuntimeError(
                "persistent Civ6 Runtime worker failed to start"
            ) from error

    def run_tick(self):
        self._ensure_started()
        loop = self._loop
        runtime = self._runtime
        if loop is None or runtime is None:
            raise RuntimeError("persistent Civ6 Runtime worker is unavailable")
        future = asyncio.run_coroutine_threadsafe(runtime.tick(), loop)
        try:
            return future.result(timeout=self._tick_timeout_seconds)
        except TimeoutError:
            future.cancel()
            raise

    def close(self) -> None:
        thread = self._thread
        loop = self._loop
        stop_event = self._stop_event
        if loop is not None and stop_event is not None:
            loop.call_soon_threadsafe(stop_event.set)
        if thread is not None and thread is not threading.current_thread():
            thread.join(
                timeout=(self._startup_timeout_seconds + self._tick_timeout_seconds)
            )
            if thread.is_alive():
                raise RuntimeError("persistent Civ6 Runtime worker did not stop")
        self._thread = None


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

    worker = _RuntimeWorker(
        lambda: open_live_runtime(
            loaded,
            config,
            planner_base_directory=config.parent,
        ),
        startup_timeout_seconds=loaded.runtime.sidecar_startup_timeout_seconds,
        tick_timeout_seconds=(
            loaded.runtime.max_turn_seconds
            + loaded.runtime.sidecar_shutdown_timeout_seconds
        ),
    )

    def run_tick():
        return worker.run_tick()

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
        try:
            worker.close()
        finally:
            server.server_close()


def main() -> None:
    typer.run(serve)


if __name__ == "__main__":
    main()
