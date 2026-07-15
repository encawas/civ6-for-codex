from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from .actions import ACTION_REGISTRY
from .codex_planner import Planner
from .conditions import ConditionEvaluator, extract_known_entities
from .events import events_from_snapshot, task_failure_event
from .gate import EventGate
from .mcp_port import GamePort
from .models import (
    AgentRequest,
    EventLevel,
    ExecutionMode,
    GameEvent,
    PlanBundle,
    RuntimeSnapshot,
    StoredTask,
    TaskStatus,
    TickMetrics,
    TickResult,
)
from .progression import ProgressionRuleCompiler
from .recovery import recover_turn_rewind
from .rules import DeterministicRuleCompiler
from .store import WorkflowStore
from .validation import PlanValidationContext, validate_plan_bundle


@dataclass(slots=True)
class EngineConfig:
    execution_mode: ExecutionMode = ExecutionMode.CONFIRM
    auto_end_turn: bool = False
    max_agent_calls_per_turn: int = 1
    repeated_failure_threshold: int = 2
    verification_attempts: int = 3
    verification_delay_seconds: float = 0.25
    auto_action_types: set[str] = field(default_factory=lambda: set(ACTION_REGISTRY))
    allowed_action_types: set[str] = field(default_factory=lambda: set(ACTION_REGISTRY))
    allowed_tools: set[str] = field(
        default_factory=lambda: {
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
        }
    )


class WorkflowEngine:
    def __init__(
        self,
        *,
        store: WorkflowStore,
        game: GamePort,
        planner: Planner,
        config: EngineConfig | None = None,
    ):
        self.store = store
        self.game = game
        self.planner = planner
        self.config = config or EngineConfig()
        self.gate = EventGate(store)
        self.conditions = ConditionEvaluator()
        self.rules = DeterministicRuleCompiler(store)
        self.progression = ProgressionRuleCompiler(store)
        self._available_tools: set[str] | None = None

    async def tick(self) -> TickResult:
        started = time.perf_counter()
        metrics = TickMetrics()
        call_count_before = self.game.call_count

        snapshot = await self._read_snapshot(metrics, include_units=False)
        rewind_event = recover_turn_rewind(self.store, snapshot)
        result = TickResult(turn=snapshot.turn, metrics=metrics)
        self.store.set_meta("last_game_id", snapshot.game_id)
        self.store.set_meta("last_observed_turn", snapshot.turn)
        await self._verify_tool_surface()

        existing_due = self.store.due_tasks(snapshot.game_id, snapshot.turn)
        need_units = self.rules.needs_units(snapshot.game_id) or any(
            task.entity_type in {"unit", "builder"} for task in existing_due
        )
        if need_units:
            snapshot = await self._read_snapshot(metrics, include_units=True)

        rule_compilation = self.rules.compile(snapshot)
        progression_compilation = self.progression.compile(snapshot)
        for compilation in (rule_compilation, progression_compilation):
            if compilation.bundle is not None:
                self.store.save_plan_bundle(
                    snapshot.game_id,
                    snapshot.turn,
                    compilation.bundle,
                    mode=self.config.execution_mode,
                    auto_action_types=self.config.auto_action_types,
                )

        due_tasks = self.store.due_tasks(snapshot.game_id, snapshot.turn)
        if (
            any(task.entity_type in {"unit", "builder"} for task in due_tasks)
            and snapshot.units is None
        ):
            snapshot = await self._read_snapshot(metrics, include_units=True)

        events: list[GameEvent] = []
        if rewind_event is not None:
            events.append(rewind_event)
        events.extend(rule_compilation.events)
        events.extend(progression_compilation.events)
        execution_started = time.perf_counter()
        if self.config.execution_mode is not ExecutionMode.READONLY:
            for task in due_tasks:
                snapshot, task_events = await self._execute_one_task(
                    task, snapshot, result, metrics
                )
                events.extend(task_events)
        metrics.task_execution_seconds = time.perf_counter() - execution_started

        # Only final state creates blocker events. A blocker with a deterministic
        # task still inside its retry budget is suppressed until that budget is
        # exhausted; the task itself prevents end_turn in the meantime.
        retrying_tasks = self.store.due_tasks(snapshot.game_id, snapshot.turn)
        snapshot_events = events_from_snapshot(snapshot)
        events.extend(
            self._suppress_recoverable_blockers(snapshot_events, retrying_tasks)
        )
        gate_result = self.gate.ingest(snapshot.game_id, events)
        result.events = gate_result.emitted
        agent_events = list(gate_result.agent_events)
        agent_events.extend(
            event
            for event in gate_result.by_level[EventLevel.L2]
            if event.blocking and event not in agent_events
        )

        already_called = self.store.agent_called_for_turn(snapshot.game_id, snapshot.turn)
        if (
            agent_events
            and self.config.max_agent_calls_per_turn > 0
            and not already_called
        ):
            if snapshot.units is None:
                snapshot = await self._read_snapshot(metrics, include_units=True)
            await self._invoke_planner(snapshot, agent_events, result, metrics)
        elif agent_events and already_called:
            result.paused = True
            result.pause_reason = (
                "Codex has already been called for this turn; unresolved blocking "
                "events require plan execution or human review"
            )
        elif snapshot.blockers and already_called and not retrying_tasks:
            result.paused = True
            result.pause_reason = (
                "The turn remains blocked after its Codex planning call; "
                "human review is required"
            )

        if self._may_end_turn(snapshot, result):
            end_result = await self.game.end_turn()
            if end_result.success:
                result.turn_ended = True
            else:
                result.paused = True
                result.pause_reason = f"end_turn failed: {end_result.message}"

        metrics.mcp_call_count = self.game.call_count - call_count_before
        metrics.total_seconds = time.perf_counter() - started
        self.store.record_metrics(snapshot.game_id, snapshot.turn, metrics)
        return result

    async def _execute_one_task(
        self,
        task: StoredTask,
        snapshot: RuntimeSnapshot,
        result: TickResult,
        metrics: TickMetrics,
    ) -> tuple[RuntimeSnapshot, list[GameEvent]]:
        events: list[GameEvent] = []
        self.store.set_task_status(snapshot.game_id, task.task_id, TaskStatus.RUNNING)
        preconditions = self.conditions.evaluate_all(task.preconditions, snapshot)
        invalidation = self._first_active_invalidator(task.invalidators, snapshot)
        if not preconditions.valid or invalidation is not None:
            message = (
                preconditions.reason
                if not preconditions.valid
                else f"task invalidated: {invalidation}"
            )
            self._retry_or_escalate_task(
                snapshot.game_id,
                task,
                snapshot.turn,
                message,
                result,
                events,
                blocked=True,
            )
            return snapshot, events

        action_result = await self.game.execute_task(task)
        if not action_result.success:
            self._retry_or_escalate_task(
                snapshot.game_id,
                task,
                snapshot.turn,
                action_result.message or "task execution failed",
                result,
                events,
                blocked=action_result.blocked,
            )
            return snapshot, events

        verification_started = time.perf_counter()
        verification_snapshot = snapshot
        verified = not task.postconditions
        verification_reason = "task has no postconditions"
        for attempt in range(max(1, self.config.verification_attempts)):
            if attempt > 0:
                await asyncio.sleep(self.config.verification_delay_seconds)
            verification_snapshot = await self._read_snapshot(
                metrics,
                include_units=task.entity_type in {"unit", "builder"},
            )
            postconditions = self.conditions.evaluate_all(
                task.postconditions, verification_snapshot
            )
            if postconditions.valid:
                verified = True
                verification_reason = ""
                break
            verification_reason = postconditions.reason
        metrics.verification_seconds += time.perf_counter() - verification_started

        if verified:
            self.store.set_task_status(
                verification_snapshot.game_id, task.task_id, TaskStatus.DONE
            )
            result.executed_task_ids.append(task.task_id)
            return verification_snapshot, events

        self._retry_or_escalate_task(
            verification_snapshot.game_id,
            task,
            verification_snapshot.turn,
            f"action returned success but postcondition failed: {verification_reason}",
            result,
            events,
            blocked=True,
        )
        return verification_snapshot, events

    def _retry_or_escalate_task(
        self,
        game_id: str,
        task: StoredTask,
        turn: int,
        message: str,
        result: TickResult,
        events: list[GameEvent],
        *,
        blocked: bool,
    ) -> None:
        retry_limit = min(task.max_retries, self.config.repeated_failure_threshold)
        next_retry = task.retry_count + 1
        terminal = next_retry >= retry_limit
        status = TaskStatus.ESCALATED if terminal else TaskStatus.READY
        self.store.set_task_status(
            game_id,
            task.task_id,
            status,
            error=message,
            increment_retry=True,
        )
        if blocked:
            result.blocked_task_ids.append(task.task_id)
        else:
            result.failed_task_ids.append(task.task_id)
        events.append(
            task_failure_event(
                task,
                turn=turn,
                message=message,
                blocked=blocked,
                repeated_failure_threshold=retry_limit,
            )
        )

    @staticmethod
    def _suppress_recoverable_blockers(
        events: list[GameEvent], retrying_tasks: list[StoredTask]
    ) -> list[GameEvent]:
        if not retrying_tasks:
            return events
        production_city_ids = {
            str(task.entity_id)
            for task in retrying_tasks
            if task.action_type == "city_set_production"
        }
        has_production_retry = bool(production_city_ids)
        has_research_retry = any(
            task.action_type == "set_research" for task in retrying_tasks
        )
        has_civic_retry = any(task.action_type == "set_civic" for task in retrying_tasks)
        retained: list[GameEvent] = []
        for event in events:
            if (
                event.event_type == "city_no_production"
                and str(event.entity_id) in production_city_ids
            ):
                continue
            if event.event_type == "end_turn_blocker":
                blocking_type = str(event.payload.get("blocking_type", ""))
                if has_production_retry and blocking_type == "ENDTURN_BLOCKING_PRODUCTION":
                    continue
                if has_research_retry and blocking_type == "ENDTURN_BLOCKING_RESEARCH":
                    continue
                if has_civic_retry and blocking_type == "ENDTURN_BLOCKING_CIVIC":
                    continue
            retained.append(event)
        return retained

    async def _invoke_planner(
        self,
        snapshot: RuntimeSnapshot,
        agent_events: list[GameEvent],
        result: TickResult,
        metrics: TickMetrics,
    ) -> None:
        request = self._build_agent_request(snapshot, agent_events)
        agent_started = time.perf_counter()
        bundle: PlanBundle | None = None
        try:
            bundle = await self.planner.plan(request)
            validate_plan_bundle(
                bundle,
                PlanValidationContext(
                    current_turn=snapshot.turn,
                    allowed_action_types=self.config.allowed_action_types,
                    known_entities=extract_known_entities(snapshot),
                ),
            )
            self.store.save_plan_bundle(
                snapshot.game_id,
                snapshot.turn,
                bundle,
                mode=self.config.execution_mode,
                auto_action_types=self.config.auto_action_types,
            )
            self.store.mark_events_sent_to_agent(
                snapshot.game_id,
                [event.dedupe_key for event in agent_events],
                snapshot.turn,
            )
            metrics.agent_call_count = 1
            result.agent_invoked = True
            result.plan_id = bundle.plan_id
            if bundle.requires_human_review:
                result.paused = True
                result.pause_reason = "Codex requested human review"
            self.store.record_agent_run(
                snapshot.game_id,
                request,
                response=bundle,
                success=True,
                error=None,
                duration_seconds=time.perf_counter() - agent_started,
            )
        except Exception as exc:
            result.paused = True
            result.pause_reason = f"Agent planning failed: {exc}"
            self.store.record_agent_run(
                snapshot.game_id,
                request,
                response=bundle,
                success=False,
                error=str(exc),
                duration_seconds=time.perf_counter() - agent_started,
            )
        metrics.agent_seconds = time.perf_counter() - agent_started

    async def _read_snapshot(
        self, metrics: TickMetrics, *, include_units: bool
    ) -> RuntimeSnapshot:
        started = time.perf_counter()
        snapshot = await self.game.read_snapshot(include_units=include_units)
        metrics.state_query_seconds += time.perf_counter() - started
        return snapshot

    async def _verify_tool_surface(self) -> None:
        if self._available_tools is not None:
            return
        self._available_tools = await self.game.list_tools()
        fallback_queries = {
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
        }
        required = self.config.allowed_tools | fallback_queries
        missing = required - self._available_tools
        if missing:
            raise RuntimeError(f"civ6-mcp is missing required tools: {sorted(missing)}")

    def _first_active_invalidator(
        self, invalidators: list[dict[str, Any]], snapshot: RuntimeSnapshot
    ) -> str | None:
        for invalidator in invalidators:
            evaluation = self.conditions.evaluate(invalidator, snapshot)
            if evaluation.valid:
                return str(invalidator)
            if evaluation.reason.startswith("unsupported condition type"):
                return evaluation.reason
        return None

    def _build_agent_request(
        self, snapshot: RuntimeSnapshot, events: list[GameEvent]
    ) -> AgentRequest:
        context = self.store.current_context(snapshot.game_id)
        return AgentRequest(
            turn=snapshot.turn,
            execution_mode=self.config.execution_mode,
            trigger_events=events,
            current_strategy=context["strategy"],
            current_plans=context,
            relevant_state=snapshot.model_dump(mode="json"),
            constraints={
                "allowed_action_types": sorted(self.config.allowed_action_types),
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
                    "unit_has_build_charge",
                    "unit_build_charges_equals",
                    "unit_can_improve",
                ],
                "strategy_queue_fields": {
                    "research_queue": "ordered TECH_* type names",
                    "civic_queue": "ordered CIVIC_* type names",
                },
                "task_postconditions_required": True,
                "max_tasks": 100,
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

    def _may_end_turn(self, snapshot: RuntimeSnapshot, result: TickResult) -> bool:
        if self.config.execution_mode is ExecutionMode.READONLY:
            return False
        if not self.config.auto_end_turn or result.paused or result.agent_invoked:
            return False
        if snapshot.blockers:
            return False
        if any(event.blocking for event in result.events):
            return False
        if self.store.due_tasks(snapshot.game_id, snapshot.turn):
            return False
        return True
