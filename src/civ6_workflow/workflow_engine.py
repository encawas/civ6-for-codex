from __future__ import annotations

import json
import time
from typing import Any
from uuid import uuid4

from .conditions import extract_known_entities
from .models import AgentRequest, PlanBundle, TickMetrics, TickResult
from .safe_engine import SafeWorkflowEngine
from .validation import PlanValidationContext, validate_plan_bundle
from .workflow_protocol import (
    WorkflowPlanBundle,
    validate_event_resolution_contract,
)
from .workflow_queries import InformationQueryRouter


_TRANSIENT_HTTP = {429, 500, 502, 503, 504}


class WorkflowAwareEngine(SafeWorkflowEngine):
    """Planner loop with event coverage, focused queries, and provider backoff."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.information_queries = InformationQueryRouter(self.game)

    def _build_agent_request(self, snapshot, events):
        request = super()._build_agent_request(snapshot, events)
        constraints = dict(request.constraints)
        constraints.update(
            {
                "event_resolution_required": True,
                "planning_phase": "initial",
                "allow_information_requests": True,
                "allowed_information_tools": [
                    "get_settle_advisor",
                    "get_global_settle_advisor",
                    "get_pathing_estimate",
                    "get_unit_promotions",
                    "get_district_advisor",
                    "get_city_production",
                    "get_map_area",
                    "get_policies",
                    "get_trade_options",
                    "get_pantheon_beliefs",
                    "get_religion_beliefs",
                    "get_dedications",
                    "get_city_states",
                    "get_builder_tasks",
                ],
            }
        )
        return request.model_copy(update={"constraints": constraints})

    def _suppress_recoverable_blockers(self, events, retrying_tasks):
        retained = super()._suppress_recoverable_blockers(events, retrying_tasks)
        has_settler_recovery = any(
            task.action_type == "unit_found_city"
            and str(task.status.value)
            in {"pending", "ready", "running", "awaiting_confirmation"}
            for task in self.store.list_tasks(
                self.store.get_meta("last_game_id", "")
            )
        )
        if not has_settler_recovery:
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

    async def _invoke_planner(
        self,
        snapshot,
        agent_events,
        result: TickResult,
        metrics: TickMetrics,
    ) -> None:
        backoff = self._active_backoff()
        if backoff is not None:
            result.paused = True
            result.pause_reason = (
                "Planner provider is in transient backoff for "
                f"{backoff['remaining_seconds']:.1f}s: {backoff.get('category')}"
            )
            return

        request = self._build_agent_request(snapshot, agent_events)
        planner_started = time.perf_counter()
        current_request = request
        bundle: PlanBundle | None = None
        result.agent_invoked = True

        try:
            prefetched = self.information_queries.prefetch(agent_events)
            if prefetched:
                prefetched_results = await self.information_queries.execute(prefetched)
                metrics.information_query_count += len(prefetched_results)
                request = request.model_copy(
                    update={"information_results": prefetched_results}
                )
                current_request = request
                self.store.set_meta(
                    "last_information_results",
                    {
                        "turn": snapshot.turn,
                        "phase": "prefetch",
                        "results": prefetched_results,
                    },
                )

            bundle = await self._plan_once(request, metrics)
            self._validate_planner_bundle(
                bundle,
                request,
                snapshot,
                agent_events,
                allow_information_requests=True,
            )

            if getattr(bundle, "information_requests", []):
                self.store.set_meta(
                    "last_information_phase_bundle",
                    bundle.model_dump(mode="json"),
                )
                focused_results = await self.information_queries.execute(
                    bundle.information_requests
                )
                metrics.information_query_count += len(focused_results)
                combined = dict(getattr(request, "information_results", {}))
                combined.update(focused_results)
                payload = request.model_dump(mode="python")
                payload.update(
                    {
                        "request_id": f"req_{uuid4().hex}",
                        "information_results": combined,
                        "constraints": {
                            **request.constraints,
                            "planning_phase": "final",
                            "allow_information_requests": False,
                        },
                    }
                )
                current_request = AgentRequest.model_validate(payload)
                self.store.set_meta(
                    "last_information_results",
                    {
                        "turn": snapshot.turn,
                        "phase": "planner_requested",
                        "results": combined,
                    },
                )
                bundle = await self._plan_once(current_request, metrics)

            self._validate_planner_bundle(
                bundle,
                current_request,
                snapshot,
                agent_events,
                allow_information_requests=False,
            )

            self.store.save_plan_bundle(
                snapshot.game_id,
                snapshot.turn,
                bundle,
                mode=self.config.execution_mode,
                auto_action_types=self.config.auto_action_types,
            )
            self.store.set_meta(
                "last_event_resolutions",
                {
                    "turn": snapshot.turn,
                    "plan_id": bundle.plan_id,
                    "resolutions": [
                        item.model_dump(mode="json")
                        for item in getattr(bundle, "event_resolutions", [])
                    ],
                },
            )
            self.store.mark_events_sent_to_agent(
                snapshot.game_id,
                [event.dedupe_key for event in agent_events],
                snapshot.turn,
            )
            result.plan_id = bundle.plan_id
            if bundle.requires_human_review:
                result.paused = True
                result.pause_reason = "Planner requested human review"

            self.store.record_agent_run(
                snapshot.game_id,
                current_request,
                response=bundle,
                success=True,
                error=None,
                duration_seconds=time.perf_counter() - planner_started,
            )
            self._clear_backoff()
        except Exception as exc:
            failure = self._classify_planner_failure(exc)
            result.paused = True
            result.pause_reason = (
                f"Agent planning failed [{failure['category']}]: "
                f"{failure['final_error']}"
            )
            self.store.record_agent_run(
                snapshot.game_id,
                current_request,
                response=bundle,
                success=False,
                error=json.dumps(failure, ensure_ascii=False, separators=(",", ":")),
                duration_seconds=time.perf_counter() - planner_started,
            )
            if failure["transient"]:
                self._set_backoff(failure)
        finally:
            diagnostics = getattr(self.planner, "last_diagnostics", None)
            if isinstance(diagnostics, dict):
                self.store.set_meta("last_planner_diagnostics", diagnostics)
            metrics.agent_seconds = time.perf_counter() - planner_started

    async def _plan_once(self, request: AgentRequest, metrics: TickMetrics) -> PlanBundle:
        metrics.agent_attempt_count += 1
        # Backward-compatible metric now means attempted planner calls, not only
        # successful calls.
        metrics.agent_call_count = metrics.agent_attempt_count
        bundle = await self.planner.plan(request)
        metrics.agent_success_count += 1
        return bundle

    def _validate_planner_bundle(
        self,
        bundle: PlanBundle,
        request: AgentRequest,
        snapshot,
        agent_events,
        *,
        allow_information_requests: bool,
    ) -> None:
        max_tasks = int(request.constraints.get("max_tasks", 8))
        validate_plan_bundle(
            bundle,
            PlanValidationContext(
                current_turn=snapshot.turn,
                allowed_action_types=self.config.allowed_action_types,
                known_entities=extract_known_entities(snapshot),
                max_tasks=max_tasks,
            ),
        )
        known_task_ids = {
            task.task_id for task in self.store.list_tasks(snapshot.game_id)
        }
        validate_event_resolution_contract(
            WorkflowPlanBundle.model_validate(bundle.model_dump(mode="python")),
            agent_events,
            known_task_ids=known_task_ids,
            allow_information_requests=allow_information_requests,
        )

    def _active_backoff(self) -> dict[str, Any] | None:
        value = self.store.get_meta("planner_provider_backoff")
        if not isinstance(value, dict):
            return None
        until = float(value.get("until_epoch", 0) or 0)
        remaining = until - time.time()
        if remaining <= 0:
            return None
        return {**value, "remaining_seconds": remaining}

    def _classify_planner_failure(self, exc: Exception) -> dict[str, Any]:
        diagnostics = getattr(self.planner, "last_diagnostics", None)
        if not isinstance(diagnostics, dict):
            diagnostics = {}
        status = diagnostics.get("http_status")
        try:
            status = None if status is None else int(status)
        except (TypeError, ValueError):
            status = None
        text = str(exc)
        lowered = text.lower()
        transient = status in _TRANSIENT_HTTP or any(
            marker in lowered
            for marker in (
                "timeout",
                "timed out",
                "transport failed",
                "connection reset",
                "temporarily unavailable",
            )
        )
        if transient:
            category = "transient_provider_failure"
        elif status in {401, 403}:
            category = "authentication_failure"
        elif status == 404:
            category = "model_or_endpoint_not_found"
        elif "planbundle" in lowered or "event resolution" in lowered:
            category = "planner_contract_failure"
        else:
            category = "planner_failure"
        return {
            "category": category,
            "transient": transient,
            "provider": diagnostics.get("backend", "unknown"),
            "http_status": status,
            "request_id": diagnostics.get("request_id"),
            "retry_count": diagnostics.get("attempt_count", 0),
            "final_error": text[-1000:],
        }

    def _set_backoff(self, failure: dict[str, Any]) -> None:
        count = int(self.store.get_meta("planner_transient_failure_count", 0) or 0) + 1
        delay = min(120.0, 5.0 * (2 ** min(count - 1, 5)))
        self.store.set_meta("planner_transient_failure_count", count)
        self.store.set_meta(
            "planner_provider_backoff",
            {
                **failure,
                "failure_count": count,
                "delay_seconds": delay,
                "until_epoch": time.time() + delay,
            },
        )

    def _clear_backoff(self) -> None:
        self.store.set_meta("planner_transient_failure_count", 0)
        self.store.set_meta("planner_provider_backoff", {})
