from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(slots=True)
class StateApiConfig:
    base_url: str = "http://127.0.0.1:8000"
    timeout_seconds: float = 10.0
    runtime_timeout_seconds: float = 2.0
    startup_retry_seconds: float = 5.0
    runtime_retry_seconds: float = 0.5
    retry_interval_seconds: float = 0.25


class Civ6StateApi:
    """Persistent client for the structured read-only API embedded in civ6-mcp."""

    def __init__(self, config: StateApiConfig):
        self.config = config
        self.client: httpx.AsyncClient | None = None
        self.call_count = 0
        self.availability_epoch = 0
        self._startup_pending = True

    async def __aenter__(self) -> "Civ6StateApi":
        self.client = httpx.AsyncClient(
            base_url=self.config.base_url.rstrip("/"),
            timeout=self.config.timeout_seconds,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.client is not None:
            await self.client.aclose()
        self.client = None

    def _require_client(self) -> httpx.AsyncClient:
        if self.client is None:
            raise RuntimeError("Civ6 state API client is not connected")
        return self.client

    async def get(self, path: str) -> Any:
        response = await self._request(path)
        response.raise_for_status()
        return response.json()

    async def get_optional(self, path: str) -> Any | None:
        response = await self._request(path)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    async def _request(self, path: str) -> httpx.Response:
        startup_request = self._startup_pending
        retry_seconds = (
            self.config.startup_retry_seconds
            if startup_request
            else self.config.runtime_retry_seconds
        )
        timeout_seconds = (
            self.config.timeout_seconds
            if startup_request
            else min(self.config.timeout_seconds, self.config.runtime_timeout_seconds)
        )
        deadline = time.monotonic() + retry_seconds
        while True:
            self.call_count += 1
            try:
                response = await self._require_client().get(
                    path,
                    timeout=timeout_seconds,
                )
            except (httpx.ConnectError, httpx.TimeoutException):
                if time.monotonic() >= deadline:
                    self.availability_epoch += 1
                    raise
            else:
                if response.status_code != 503:
                    self._startup_pending = False
                    return response
                if time.monotonic() >= deadline:
                    self.availability_epoch += 1
                    return response
            await asyncio.sleep(self.config.retry_interval_seconds)
