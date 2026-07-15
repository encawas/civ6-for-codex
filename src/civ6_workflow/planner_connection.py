from __future__ import annotations

import asyncio
import os
import time
from typing import Any
from urllib.parse import quote

import httpx


def planner_connection_status(config: Any) -> dict[str, Any]:
    backend = str(config.backend).strip().lower()
    if backend == "responses":
        credential_present = bool(os.environ.get(config.api_key_env))
        model_present = bool(config.model)
        return {
            "backend": backend,
            "model": config.model,
            "credential_env": config.api_key_env,
            "credential_present": credential_present,
            "configured": credential_present and model_present,
            "connection_owner": "frontend_via_local_backend",
            "secret_exposed_to_browser": False,
        }
    return {
        "backend": backend,
        "model": config.model,
        "command": config.command,
        "configured": bool(config.command),
        "connection_owner": "frontend_via_local_backend",
        "secret_exposed_to_browser": False,
    }


async def probe_planner_connection(config: Any) -> dict[str, Any]:
    """Verify planner availability without running a gameplay planning request."""

    backend = str(config.backend).strip().lower()
    started = time.perf_counter()
    if backend == "responses":
        api_key = os.environ.get(config.api_key_env)
        if not api_key:
            return _result(
                backend,
                started,
                ok=False,
                model=config.model,
                error=f"missing environment variable {config.api_key_env}",
            )
        if not config.model:
            return _result(
                backend,
                started,
                ok=False,
                model=None,
                error="codex.model is not configured",
            )

        timeout = httpx.Timeout(
            connect=config.connect_timeout_seconds,
            read=min(config.read_timeout_seconds, 30.0),
            write=config.write_timeout_seconds,
            pool=config.pool_timeout_seconds,
        )
        url = (
            f"{config.api_base_url.rstrip('/')}/models/"
            f"{quote(str(config.model), safe='')}"
        )
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await asyncio.wait_for(
                    client.get(
                        url,
                        headers={"Authorization": f"Bearer {api_key}"},
                    ),
                    timeout=min(float(config.timeout_seconds), 45.0),
                )
        except TimeoutError:
            return _result(
                backend,
                started,
                ok=False,
                model=config.model,
                error="planner connection probe timed out",
            )
        except httpx.HTTPError as exc:
            return _result(
                backend,
                started,
                ok=False,
                model=config.model,
                error=f"planner connection transport failed: {exc}",
            )

        request_id = response.headers.get("x-request-id")
        if response.status_code >= 400:
            body = response.text[-2000:]
            return _result(
                backend,
                started,
                ok=False,
                model=config.model,
                http_status=response.status_code,
                request_id=request_id,
                error=body or f"HTTP {response.status_code}",
            )
        try:
            data = response.json()
        except ValueError:
            data = {}
        return _result(
            backend,
            started,
            ok=True,
            model=data.get("id", config.model) if isinstance(data, dict) else config.model,
            http_status=response.status_code,
            request_id=request_id,
        )

    if backend == "codex_cli":
        try:
            process = await asyncio.create_subprocess_exec(
                config.command,
                "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
        except FileNotFoundError:
            return _result(
                backend,
                started,
                ok=False,
                model=config.model,
                error=f"executable not found: {config.command}",
            )
        except TimeoutError:
            return _result(
                backend,
                started,
                ok=False,
                model=config.model,
                error="codex --version timed out",
            )
        if process.returncode != 0:
            error = stderr.decode("utf-8", errors="replace")[-2000:]
            return _result(
                backend,
                started,
                ok=False,
                model=config.model,
                error=error or f"exit code {process.returncode}",
            )
        return _result(
            backend,
            started,
            ok=True,
            model=config.model,
            version=stdout.decode("utf-8", errors="replace").strip(),
        )

    return _result(
        backend,
        started,
        ok=False,
        model=config.model,
        error=f"unsupported backend: {backend}",
    )


def _result(
    backend: str,
    started: float,
    *,
    ok: bool,
    model: str | None,
    error: str | None = None,
    http_status: int | None = None,
    request_id: str | None = None,
    version: str | None = None,
) -> dict[str, Any]:
    return {
        "ok": ok,
        "backend": backend,
        "model": model,
        "duration_seconds": round(time.perf_counter() - started, 4),
        "http_status": http_status,
        "request_id": request_id,
        "version": version,
        "error": error,
        "connection_owner": "frontend_via_local_backend",
        "secret_exposed_to_browser": False,
    }
