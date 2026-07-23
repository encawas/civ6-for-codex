from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .models import ActionResult, RuntimeSnapshot, StoredTask


class WorkflowStorePort(Protocol):
    """Application-facing persistence boundary implemented by WorkflowStore."""

    path: Path

    def __getattr__(self, name: str) -> Any: ...


class GamePort(Protocol):
    call_count: int

    async def read_snapshot(
        self, *, include_units: bool = False
    ) -> RuntimeSnapshot: ...

    async def execute_task(self, task: StoredTask) -> ActionResult: ...

    async def end_turn(self, reflections: dict[str, str]) -> ActionResult: ...

    async def list_tools(self) -> set[str]: ...

    async def query_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any: ...


class Planner(Protocol):
    async def plan(self, request: Any) -> Any: ...


class MutationBudgetExceeded(RuntimeError):
    pass


@dataclass(slots=True)
class MutationBudget:
    limit: int = 1
    used: int = 0

    def consume(self, operation: str) -> None:
        if self.used >= self.limit:
            raise MutationBudgetExceeded(
                f"mutation budget exhausted before {operation}"
            )
        self.used += 1


class BoundedGamePort:
    """Per-Tick structural guard around every mutating GamePort call."""

    def __init__(self, delegate: GamePort, budget: MutationBudget):
        self.delegate = delegate
        self.budget = budget

    @property
    def call_count(self) -> int:
        return self.delegate.call_count

    async def read_snapshot(self, *, include_units: bool = False) -> RuntimeSnapshot:
        return await self.delegate.read_snapshot(include_units=include_units)

    async def execute_task(self, task: StoredTask) -> ActionResult:
        self.budget.consume(task.action_type)
        return await self.delegate.execute_task(task)

    async def end_turn(self, reflections: dict[str, str] | None = None) -> ActionResult:
        self.budget.consume("end_turn")
        return await self.delegate.end_turn(reflections or {})

    async def list_tools(self) -> set[str]:
        return await self.delegate.list_tools()

    async def query_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> Any:
        return await self.delegate.query_tool(name, arguments)
