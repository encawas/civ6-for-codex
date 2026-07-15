from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

from .credentials import CredentialError, resolve_api_credential


def planner_connection_status(config: Any) -> dict[str, Any]:
    backend = str(config.backend).strip().lower()
    if backend == "responses":
        credential_error = None
        try:
            credential = resolve_api_credential(
                config.api_key_env, config.api_key_file
            )
        except CredentialError as exc:
            credential = None
            credential_error = str(exc)
        credential_present = credential is not None
        model_present = bool(config.model)
        return {
            "backend": backend,
            "model": config.model,
            "credential_env": config.api_key_env,
            "credential_present": credential_present,
            "configured": credential_present and model_present,
            "credential_source": None if credential is None else credential.source,
            "credential_error": credential_error,
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
        return await _probe_responses_backend(config, backend, started)

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


async def _probe_responses_backend(
    config: Any, backend: str, started: float
) -> dict[str, Any]:
    try:
        credential = resolve_api_credential(config.api_key_env, config.api_key_file)
    except CredentialError as exc:
        return _result(
            backend, started, ok=False, model=config.model, error=str(exc)
        )
    if credential is None:
        return _result(
            backend,
            started,
            ok=False,
            model=config.model,
            error=(
                f"missing API credential from {config.api_key_env} "
                "or codex.api_key_file"
            ),
        )
    if not config.model:
        return _result(
            backend,
            started,
            ok=False,
            model=None,
            error="codex.model is not configured",
        )

    payload = {
        "model": config.model,
        "instructions": "Return a JSON object with ok set to true.",
        "input": "Verify this Responses API connection.",
        "text": {
            "format": {
                "type": "json_schema",
                "name": "connection_probe",
                "schema": {
                    "type": "object",
                    "properties": {"ok": {"type": "boolean"}},
                    "required": ["ok"],
                    "additionalProperties": False,
                },
                "strict": True,
            }
        },
        "store": False,
        "reasoning": {"effort": "low"},
    }
    timeout = httpx.Timeout(
        connect=config.connect_timeout_seconds,
        read=min(config.read_timeout_seconds, 45.0),
        write=config.write_timeout_seconds,
        pool=config.pool_timeout_seconds,
    )
    url = f"{config.api_base_url.rstrip('/')}/responses"
    headers = {
        "Authorization": f"Bearer {credential.value}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await asyncio.wait_for(
                client.post(
                    url,
                    headers=headers,
                    content=json.dumps(payload, separators=(",", ":")).encode(),
                ),
                timeout=min(float(config.timeout_seconds), 60.0),
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
        return _result(
            backend,
            started,
            ok=False,
            model=config.model,
            http_status=response.status_code,
            request_id=request_id,
            error=response.text[-2000:] or f"HTTP {response.status_code}",
        )
    content_type = response.headers.get("content-type", "").lower()
    if "application/json" not in content_type:
        return _result(
            backend,
            started,
            ok=False,
            model=config.model,
            http_status=response.status_code,
            request_id=request_id,
            error=f"non-JSON response content type: {content_type or 'missing'}",
        )
    try:
        data = response.json()
        output = json.loads(_extract_output_text(data))
    except (ValueError, TypeError):
        return _result(
            backend,
            started,
            ok=False,
            model=config.model,
            http_status=response.status_code,
            request_id=request_id,
            error="probe response did not contain valid structured output",
        )
    return _result(
        backend,
        started,
        ok=output.get("ok") is True,
        model=data.get("model", config.model),
        http_status=response.status_code,
        request_id=request_id,
        error=None if output.get("ok") is True else "probe returned ok=false",
    )


def _extract_output_text(response: dict[str, Any]) -> str:
    for item in response.get("output", []) or []:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            if isinstance(content, dict) and content.get("type") == "output_text":
                return str(content.get("text", "")).strip()
    return ""


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
