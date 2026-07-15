from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

from .agent_projection import project_agent_context
from .actions import action_argument_contracts
from .engine import EngineConfig as BaseEngineConfig
from .engine import WorkflowEngine as BaseWorkflowEngine
from .gate import EventGate, GateConfig
from .models import AgentRequest, ExecutionMode, GameEvent, StoredTask, TaskStatus
from .validation import ACTION_ENTITY_TYPES


@dataclass(slots=True)
class SafeEngineConfig(BaseEngineConfig):
    default_cooldown_turns: int = 2


class _TickFileLock:
    """User-global non-blocking lock held for one complete workflow tick.

    Civilization VI exposes one local FireTuner-controlled game instance. A
    database-scoped lock would still allow two configurations with different
    SQLite paths to mutate that same game concurrently, so the lock is global
    for the current OS user.
    """

    def __init__(self, lock_path: Path | None = None):
        self.path = lock_path or (
            Path.home() / ".civ6-workflow" / "runtime.tick.lock"
        )
        self.handle: BinaryIO | None = None

    def __enter__(self) -> "_TickFileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            raise RuntimeError(
                "another civ6-workflow process is already executing a game tick "
                f"under this user account ({self.path})"
            ) from exc
        self.handle = handle
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        handle = self.handle
        self.handle = None
        if handle is None:
            return
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


class SafeWorkflowEngine(BaseWorkflowEngine):
    """Workflow engine with serialized ticks and fail-closed planning."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cooldown = int(getattr(self.config, "default_cooldown_turns", 2))
        self.gate = EventGate(
            self.store,
            GateConfig(default_cooldown_turns=max(0, cooldown)),
        )

    async def tick(self):
        with _TickFileLock():
            prepare_mode = getattr(self.store, "prepare_execution_mode", None)
            if prepare_mode is not None:
                prepare_mode(self.config.execution_mode)
            result = await super().tick()
            game_id = self.store.get_meta("last_game_id")
            if (
                isinstance(game_id, str)
                and not result.paused
                and not result.turn_ended
                and not result.agent_invoked
                and any(event.blocking for event in result.events)
                and self.store.agent_called_for_turn(game_id, result.turn)
                and not self.store.due_tasks(game_id, result.turn)
            ):
                result.paused = True
                result.pause_reason = (
                    "A blocking workflow event remains after this turn's planning "
                    "call and no executable recovery task exists; human review is "
                    "required."
                )
            return result

    def _build_agent_request(
        self, snapshot, events: list[GameEvent]
    ) -> AgentRequest:
        context = self.store.current_context(snapshot.game_id)
        relevant_state, relevant_plans, max_tasks = project_agent_context(
            snapshot, events, context
        )
        return AgentRequest(
            turn=snapshot.turn,
            execution_mode=self.config.execution_mode,
            trigger_events=events,
            current_strategy=context.get("strategy", {}),
            current_plans=relevant_plans,
            relevant_state=relevant_state,
            constraints={
                "allowed_action_types": sorted(self.config.allowed_action_types),
                "action_argument_contracts": action_argument_contracts(),
                "action_entity_types": {
                    action_type: sorted(entity_types)
                    for action_type, entity_types in sorted(
                        ACTION_ENTITY_TYPES.items()
                    )
                },
                "entity_id_arguments": {
                    "city": "city_id",
                    "research": "tech_or_civic",
                    "civic": "tech_or_civic",
                    "unit": "unit_id",
                    "builder": "unit_id",
                },
                "condition_contracts": {
                    "discriminator_key": "type",
                    "set_research": {
                        "required_preconditions": [
                            {"type": "research_unselected"},
                            {
                                "type": "research_available",
                                "tech_type": "$tech_or_civic",
                            },
                        ],
                        "required_postconditions": [
                            {
                                "type": "research_equals",
                                "tech_type": "$tech_or_civic",
                            },
                        ],
                    },
                    "city_set_production": {
                        "required_preconditions": [
                            {
                                "type": "city_has_no_production",
                                "city_id": "$city_id",
                            },
                        ],
                        "required_postconditions": [
                            {
                                "type": "city_production_equals",
                                "city_id": "$city_id",
                                "item_name": "$item_name",
                            },
                        ],
                    },
                },
                "supported_condition_types": [
                    "turn_at_least",
                    "turn_equals",
                    "no_blocker_type",
                    "field_equals",
                    "field_in",
                    "entity_exists",
                    "city_production_equals",
                    "city_has_no_production",
                    "research_unselected",
                    "research_available",
                    "research_equals",
                    "civic_unselected",
                    "civic_available",
                    "civic_equals",
                    "unit_at",
                    "unit_has_moves",
                    "unit_no_moves",
                    "unit_has_build_charge",
                    "unit_build_charges_equals",
                    "unit_can_improve",
                ],
                "strategy_queue_fields": {
                    "research_queue": "ordered TECH_* type names",
                    "civic_queue": "ordered CIVIC_* type names",
                },
                "task_postconditions_required": True,
                "max_tasks": max_tasks,
                "max_agent_calls_this_turn": self.config.max_agent_calls_per_turn,
                "forbidden_domains": [
                    "declare_war",
                    "peace",
                    "diplomacy_accept",
                    "world_congress",
                    "policy_rebuild",
                    "city_capture",
                    "city_placement",
                    "purchase",
                ],
            },
        )

    async def _invoke_planner(self, snapshot, agent_events, result, metrics) -> None:
        try:
            await super()._invoke_planner(snapshot, agent_events, result, metrics)
        finally:
            diagnostics = getattr(self.planner, "last_diagnostics", None)
            if isinstance(diagnostics, dict):
                self.store.set_meta("last_planner_diagnostics", diagnostics)

    def _suppress_recoverable_blockers(
        self, events: list[GameEvent], retrying_tasks: list[StoredTask]
    ) -> list[GameEvent]:
        retained = super()._suppress_recoverable_blockers(events, retrying_tasks)
        game_id = self.store.get_meta("last_game_id")
        if not isinstance(game_id, str):
            return retained
        unit_actions = {
            "unit_move",
            "builder_improve",
            "unit_heal",
            "unit_fortify",
            "unit_skip",
        }
        active_statuses = {
            TaskStatus.PENDING,
            TaskStatus.READY,
            TaskStatus.RUNNING,
            TaskStatus.AWAITING_CONFIRMATION,
        }
        has_unit_recovery = any(
            task.action_type in unit_actions and task.status in active_statuses
            for task in self.store.list_tasks(game_id)
        )
        if not has_unit_recovery:
            return retained
        return [
            event
            for event in retained
            if not (
                event.event_type == "end_turn_blocker"
                and str(event.payload.get("blocking_type", ""))
                == "ENDTURN_BLOCKING_UNITS"
            )
        ]
