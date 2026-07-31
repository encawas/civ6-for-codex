"""Persistent planner lifecycle used by the canonical bounded workflow runtime."""

from __future__ import annotations

import json
import time
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable
from uuid import uuid4

from .domain.base import thaw_json
from .domain import (
    AwaitingHumanTick,
    InformationCollectedTick,
    InformationRequestedTick,
    InformationRound,
    InformationRoundStatus,
    LogicalPlannerRequestCreatedTick,
    MissionGraph,
    MissionGraphPatchedTick,
    MissionImpactAnalyzer,
    ObservationComparisonKind,
    PlannerAttemptCompletedTick,
    PlannerBackoffTick,
    PlannerRequest,
    PlannerRequestStatus,
    PlannerRequestTarget,
    PlannerRequestTargetKind,
    ProviderAttempt,
    ProviderAttemptStatus,
    RuntimeState,
    StrategicContract,
    StrategicContractCommit,
    StrategicProposalReadyTick,
    StrategicRequestTerminatedTick,
    build_strategic_contract_id,
    build_mission_graph_patch,
    build_mission_graph_patch_id,
    build_strategic_research_proposal,
    build_strategic_research_proposal_id,
    validate_workflow_tick,
    canonical_json,
    canonical_json_hash,
)
from .models import (
    TickResult,
)
from .workflow_protocol import (
    InformationRequest,
    WorkflowAgentRequest as AgentRequest,
    MissionGraphPatchResponse,
    StrategicResearchProposalResponse,
    canonical_mission_graph_patch_response_payload,
    canonical_strategic_research_proposal_response_payload,
    validate_information_request,
)
from .ports import GamePort, Planner, StaleStrategicContractBaseError, WorkflowStorePort
from .runtime_errors import InjectedCrashBoundary


PLANNER_CALL_POLICY_REVISION = "planner-call-policy/v1"
PLANNER_INPUT_CONTRACT_REVISION = "planner-input-contract/v2"
PLANNER_REQUEST_POLICY_REVISION = (
    f"{PLANNER_CALL_POLICY_REVISION}+{PLANNER_INPUT_CONTRACT_REVISION}"
)
_TRANSIENT_HTTP = {429, 500, 502, 503, 504}


@dataclass(slots=True)
class PlannerLifecycleRuntime:
    """Narrow runtime services used by the planner application service."""

    store: WorkflowStorePort
    game: GamePort
    planner: Planner
    config: Any
    conditions: Any
    information_queries: Any
    now: Callable[[], Any]
    monotonic: Callable[[], float]
    checkpoint: Callable[[str], None]
    observation_id: Callable[[], str | None]
    human_wait_context: Callable[[Any], dict[str, Any]]

    @property
    def _active_observation_id(self) -> str | None:
        return self.observation_id()

    def _now(self):
        return self.now()

    def _monotonic(self) -> float:
        return self.monotonic()

    def _checkpoint(self, name: str) -> None:
        self.checkpoint(name)

    def _human_wait_context(self, snapshot) -> dict[str, Any]:
        return self.human_wait_context(snapshot)


class PlannerLifecycleCoordinator:
    """Advance durable planning state without owning the workflow Tick loop."""

    def __init__(self, runtime: PlannerLifecycleRuntime):
        self.runtime = runtime

    async def advance_mission_repair(self, ctx, observation):
        """Advance active Mission repair before current-turn graph projection."""

        runtime = self.runtime
        snapshot = observation.snapshot
        active_request = runtime.store.active_planner_request(snapshot.game_id)
        if (
            active_request is not None
            and active_request.target.kind
            is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR
        ):
            compatibility = TickResult(
                turn=snapshot.turn,
                metrics=ctx.metrics,
                events=[],
            )
            return await self._advance_active_strategic_request(
                ctx,
                observation,
                active_request,
                compatibility,
            )
        if active_request is not None:
            return None

        active_contract = runtime.store.get_active_strategic_contract(snapshot.game_id)
        if active_contract is None:
            return None
        owned_scopes = set(
            active_contract.authority_scope_set.mission_graph_scopes
        ).intersection(
            {
                "research",
                "civic",
                "settler",
                "city_roles",
                "diplomacy_trade",
                "tactical_emergency",
            }
        )
        if not owned_scopes:
            return None

        current = observation.canonical
        comparison = runtime.store.record_observation_comparison(
            current,
            detected_at=runtime._now(),
        )
        baseline = runtime.store.get_accepted_observation_baseline(snapshot.game_id)
        if comparison.kind is ObservationComparisonKind.STATE_DELTA:
            assert comparison.state_delta is not None
            affected_mission_ids = MissionImpactAnalyzer().affected_mission_ids(
                comparison.state_delta,
                active_contract.mission_graph,
            )
            affected = tuple(
                mission
                for mission in active_contract.mission_graph.missions
                if mission.mission_id in affected_mission_ids
            )
            affected_scopes = tuple(sorted({mission.scope for mission in affected}))
            if len(affected_scopes) > 1:
                raise RuntimeError(
                    "MissionGraph repair cannot combine independently owned scopes"
                )
            repair_scope = None if not affected_scopes else affected_scopes[0]
            if (
                affected_mission_ids
                and repair_scope is not None
                and repair_scope in owned_scopes
                and current.completeness.supports_scope(repair_scope)
            ):
                base_context = {
                    "target_contract_id": active_contract.contract_id,
                    "expected_base_revision": active_contract.revision,
                    "strategic_scope": repair_scope,
                }
                repair_context = {
                    "source_state_delta_id": comparison.state_delta.state_delta_id,
                    "baseline_observation_id": (
                        comparison.state_delta.baseline_observation_id
                    ),
                    "current_observation_id": (
                        comparison.state_delta.current_observation_id
                    ),
                    "current_observation_projection_hash": current.projection_hash,
                    "affected_mission_ids": list(affected_mission_ids),
                    "affected_missions": [
                        mission.model_dump(mode="json") for mission in affected
                    ],
                }
                projection = {
                    "strategic_proposal_context": base_context,
                    "mission_repair_context": repair_context,
                    "planner_input_contract_revision": (
                        PLANNER_INPUT_CONTRACT_REVISION
                    ),
                }
                request_payload = AgentRequest(
                    turn=snapshot.turn,
                    execution_mode=runtime.config.execution_mode,
                    trigger_events=[],
                    relevant_state={
                        "state_delta": comparison.state_delta.model_dump(mode="json"),
                        "affected_missions": repair_context["affected_missions"],
                    },
                    constraints={
                        "planning_phase": "initial",
                        "allow_information_requests": True,
                        "planner_request_target_kind": (
                            PlannerRequestTargetKind.MISSION_GRAPH_REPAIR.value
                        ),
                        "response_schema_version": ("mission-graph-patch-response/v1"),
                    },
                )
                request_id = (
                    "mission_repair_request_"
                    + canonical_json_hash(
                        {
                            "contract_id": active_contract.contract_id,
                            "base_revision": active_contract.revision,
                            "state_delta_id": comparison.state_delta.state_delta_id,
                            "affected_mission_ids": affected_mission_ids,
                        }
                    )[:24]
                )
                request = PlannerRequest(
                    planner_request_id=request_id,
                    game_session_id=snapshot.game_id,
                    turn_number=snapshot.turn,
                    observation_id=current.observation_id,
                    target=PlannerRequestTarget(
                        kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
                        strategic_contract_id=active_contract.contract_id,
                        base_contract_revision=active_contract.revision,
                        strategic_scope=repair_scope,
                        affected_mission_ids=affected_mission_ids,
                    ),
                    input_projection_hash=canonical_json_hash(projection),
                    input_projection_version="mission-repair-input/v1",
                    input_projection=projection,
                    request_payload=request_payload.model_dump(mode="json"),
                    policy_revision="mission-graph-repair-policy/v1",
                    approval_contract_hash=canonical_json_hash(
                        {"approval": "not-required"}
                    ),
                    allowed_actions_hash=canonical_json_hash(
                        {
                            "research": ["set_research"],
                            "civic": ["set_civic"],
                            "settler": ["unit_found_city", "unit_move"],
                            "city_roles": ["city_set_production"],
                            "diplomacy_trade": [],
                            "tactical_emergency": [
                                "tactical_unit_fortify",
                                "tactical_unit_move",
                                "tactical_unit_skip",
                            ],
                        }[repair_scope]
                    ),
                    model_settings={"provider": type(runtime.planner).__name__},
                    status=PlannerRequestStatus.PENDING,
                    created_at=runtime._now(),
                    context_bytes=len(canonical_json(projection).encode("utf-8")),
                )
                ctx.metrics.logical_planner_request_count += 1
                ctx.metrics.planner_context_bytes += request.context_bytes
                return self._finish(
                    ctx,
                    snapshot,
                    LogicalPlannerRequestCreatedTick,
                    planner_request=request,
                    planner_request_id=request.planner_request_id,
                    request_target_kind=request.target.kind,
                    decision_gap_ids=(),
                )

        if all(current.completeness.supports_scope(scope) for scope in owned_scopes):
            expected_previous = None if baseline is None else baseline.observation_id
            runtime.store.accept_observation_baseline(
                current.observation_id,
                expected_previous_observation_id=expected_previous,
                reason=f"accepted after {comparison.kind.value}",
                accepted_at=runtime._now(),
            )
        return None

    async def _advance_active_strategic_request(
        self,
        ctx,
        observation,
        active: PlannerRequest,
        compatibility: TickResult,
    ):
        runtime = self.runtime
        snapshot = observation.snapshot
        try:
            proposal_context = self._strategic_proposal_context(
                active, observation=observation
            )
        except ValueError as exc:
            return self._supersede_strategic_request(
                ctx, snapshot, active, compatibility, str(exc)
            )
        if active.status is PlannerRequestStatus.AWAITING_INFORMATION:
            return await self._collect_information(
                ctx, observation, active, compatibility
            )
        backoff = self._active_backoff(active)
        if active.status is PlannerRequestStatus.BACKOFF and backoff:
            runtime.store.record_planner_suppression(
                snapshot.game_id,
                snapshot.turn,
                reason="provider_backoff",
                relevant_input_hash=active.input_projection_hash,
            )
            return self._finish(
                ctx,
                snapshot,
                PlannerBackoffTick,
                compatibility=compatibility,
                planner_request=active,
                planner_request_id=active.planner_request_id,
                blocking_reason=(
                    "planner provider backoff remains active for "
                    f"{backoff['remaining_seconds']:.1f}s"
                ),
            )
        return await self._continue_strategic_proposal_request(
            ctx,
            observation,
            active,
            compatibility,
            proposal_context,
        )

    async def advance(
        self, ctx, observation, agent_events, compatibility, *, current_events=None
    ):
        runtime = self.runtime
        snapshot = observation.snapshot
        game_id = snapshot.game_id
        if (
            ctx.starting_state
            in {
                RuntimeState.SYSTEM_ERROR,
                RuntimeState.AWAITING_APPROVAL,
                RuntimeState.AWAITING_HUMAN,
                RuntimeState.TURN_TRANSITIONING,
                RuntimeState.VERIFYING,
            }
            and not ctx.resuming_human_wait
        ):
            return [], None

        active = runtime.store.active_planner_request(game_id)
        if active is not None:
            if active.target.kind in {
                PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
                PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            }:
                return [], await self._advance_active_strategic_request(
                    ctx, observation, active, compatibility
                )
            raise RuntimeError(
                "legacy PlannerRequest authority must be migrated before Runtime starts"
            )

        return list(agent_events), None

    async def _collect_information(
        self,
        ctx,
        observation,
        logical_request: PlannerRequest,
        compatibility: TickResult,
    ) -> TickResult:
        runtime = self.runtime
        rounds = runtime.store.list_information_rounds(
            logical_request.planner_request_id
        )
        if not rounds or rounds[-1].status is not InformationRoundStatus.REQUESTED:
            raise RuntimeError("awaiting-information request has no pending round")
        pending_round = rounds[-1]
        requests = [
            InformationRequest.model_validate(payload)
            for payload in pending_round.requests
        ]
        results = await runtime.information_queries.execute(requests)
        observation_id = runtime._active_observation_id or ctx.observation_ids[-1]
        results = {
            request_id: {
                **payload,
                "information_request_id": request_id,
                "planner_request_id": logical_request.planner_request_id,
                "information_round_id": pending_round.information_round_id,
                "collected_from_observation_id": observation_id,
            }
            for request_id, payload in results.items()
        }
        ctx.metrics.information_query_count += len(results)
        ctx.metrics.information_round_count += 1
        now = runtime._now()
        collected = pending_round.model_copy(
            update={
                "status": InformationRoundStatus.COLLECTED,
                "results": results,
                "completed_at": now,
            }
        )
        combined = dict(logical_request.information_results)
        combined.update(results)
        updated_request = logical_request.model_copy(
            update={
                "status": PlannerRequestStatus.READY_TO_CONTINUE,
                "pending_information_requests": (),
                "information_results": combined,
                "information_round_count": (
                    logical_request.information_round_count + 1
                ),
            }
        )
        compatibility.planner_request_id = logical_request.planner_request_id
        return self._finish(
            ctx,
            observation.snapshot,
            InformationCollectedTick,
            compatibility=compatibility,
            planner_request=updated_request,
            information_round=collected,
            planner_request_id=logical_request.planner_request_id,
            information_round_id=collected.information_round_id,
        )

    def _strategic_proposal_context(
        self, logical_request: PlannerRequest, *, observation=None
    ) -> tuple[str, int]:
        target = logical_request.target
        if target.strategic_scope not in {
            "research",
            "civic",
            "settler",
            "city_roles",
            "diplomacy_trade",
            "tactical_emergency",
        }:
            raise ValueError("strategic request scope is unsupported")
        active = self.runtime.store.get_active_strategic_contract(
            logical_request.game_session_id
        )
        if target.kind is PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION:
            if target.strategic_scope != "research":
                raise ValueError(
                    "StrategicContract creation request scope must be research"
                )
            if active is not None:
                raise ValueError("StrategicContract creation base is stale")
            contract_id = target.strategic_contract_id or build_strategic_contract_id(
                logical_request.game_session_id
            )
            expected_base_revision = 0
        elif target.kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR:
            if active is None:
                raise ValueError("MissionGraph repair requires an active Contract")
            if target.strategic_contract_id != active.contract_id:
                raise ValueError("MissionGraph repair Contract identity is stale")
            if target.base_contract_revision != active.revision:
                raise ValueError("MissionGraph repair Contract revision is stale")
            repair_projection = thaw_json(logical_request.input_projection).get(
                "mission_repair_context"
            )
            if (
                isinstance(repair_projection, dict)
                and target.strategic_scope
                not in active.authority_scope_set.mission_graph_scopes
            ):
                raise ValueError("MissionGraph repair scope is not authoritative")
            contract_id = active.contract_id
            expected_base_revision = active.revision
        else:
            raise ValueError("PlannerRequest target is not a strategic Proposal target")

        projection = thaw_json(logical_request.input_projection)
        projection_context = projection.get("strategic_proposal_context", projection)
        if not isinstance(projection_context, dict):
            raise ValueError("strategic Proposal input projection context is missing")
        expected_projection = {
            "target_contract_id": contract_id,
            "expected_base_revision": expected_base_revision,
            "strategic_scope": target.strategic_scope,
        }
        if any(
            projection_context.get(key) != value
            for key, value in expected_projection.items()
        ):
            raise ValueError("strategic Proposal input projection is stale")
        if target.kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR:
            repair_context = projection.get("mission_repair_context")
            if not isinstance(repair_context, dict):
                # Phase 1B repair-target Proposal requests remain readable.
                return contract_id, expected_base_revision
            if (
                repair_context.get("affected_mission_ids")
                != list(target.affected_mission_ids)
                or repair_context.get("current_observation_id")
                != logical_request.observation_id
            ):
                raise ValueError("MissionGraph repair input identity is stale")
            if (
                observation is not None
                and repair_context.get("current_observation_projection_hash")
                != observation.canonical.projection_hash
            ):
                raise ValueError("MissionGraph repair Observation is stale")
        return contract_id, expected_base_revision

    def _supersede_strategic_request(
        self,
        ctx,
        snapshot,
        logical_request: PlannerRequest,
        compatibility: TickResult,
        reason: str,
    ) -> TickResult:
        now = self.runtime._now()
        failed_round = None
        if logical_request.status is PlannerRequestStatus.AWAITING_INFORMATION:
            rounds = self.runtime.store.list_information_rounds(
                logical_request.planner_request_id
            )
            requested = [
                round_record
                for round_record in rounds
                if round_record.status is InformationRoundStatus.REQUESTED
            ]
            if (
                len(requested) != 1
                or not rounds
                or rounds[-1].information_round_id != requested[0].information_round_id
            ):
                raise RuntimeError(
                    "awaiting-information strategic request requires one latest "
                    "REQUESTED InformationRound"
                )
            failed_round = requested[0].model_copy(
                update={
                    "status": InformationRoundStatus.FAILED,
                    "completed_at": now,
                }
            )
        updated_request = logical_request.model_copy(
            update={
                "status": PlannerRequestStatus.SUPERSEDED,
                "completed_at": now,
                "failure_category": "stale_strategic_contract_base",
                "pending_information_requests": (),
                "next_retry_at": None,
            }
        )
        compatibility.paused = True
        compatibility.pause_reason = reason
        compatibility.planner_request_id = logical_request.planner_request_id
        existing_attempts = self.runtime.store.list_provider_attempts(
            logical_request.planner_request_id
        )
        return self._finish(
            ctx,
            snapshot,
            StrategicRequestTerminatedTick,
            compatibility=compatibility,
            planner_request=updated_request,
            information_round=failed_round,
            planner_request_id=logical_request.planner_request_id,
            terminal_status=PlannerRequestStatus.SUPERSEDED,
            failure_category="stale_strategic_contract_base",
            provider_attempt_id=(
                None
                if not existing_attempts
                else existing_attempts[-1].provider_attempt_id
            ),
            blocking_reason=reason,
        )

    async def _continue_strategic_proposal_request(
        self,
        ctx,
        observation,
        logical_request: PlannerRequest,
        compatibility: TickResult,
        proposal_context: tuple[str, int],
    ) -> TickResult:
        runtime = self.runtime
        snapshot = observation.snapshot
        target_contract_id, expected_base_revision = proposal_context
        mission_repair = (
            logical_request.target.kind is PlannerRequestTargetKind.MISSION_GRAPH_REPAIR
            and isinstance(
                thaw_json(logical_request.input_projection).get(
                    "mission_repair_context"
                ),
                dict,
            )
        )
        payload = thaw_json(logical_request.request_payload)
        payload["request_id"] = f"req_{uuid4().hex}"
        constraints = thaw_json(payload.get("constraints", {}))
        constraints.update(
            {
                "planner_request_target_kind": logical_request.target.kind.value,
                "target_contract_id": target_contract_id,
                "expected_base_revision": expected_base_revision,
                "strategic_scope": logical_request.target.strategic_scope,
                "response_schema_version": (
                    "mission-graph-patch-response/v1"
                    if mission_repair
                    else "strategic-research-proposal-response/v1"
                ),
            }
        )
        if logical_request.information_results:
            payload["information_results"] = thaw_json(
                logical_request.information_results
            )
            constraints.update(
                {
                    "planning_phase": "final",
                    "allow_information_requests": False,
                }
            )
        payload["constraints"] = constraints
        provider_request = AgentRequest.model_validate(payload)
        provider_attempts: list[ProviderAttempt] = []
        active_provider_attempt: ProviderAttempt | None = None
        pending_failed_attempt: ProviderAttempt | None = None
        provider_count = 0

        async def provider_attempt_hook(phase, details):
            nonlocal logical_request
            nonlocal active_provider_attempt
            nonlocal pending_failed_attempt
            nonlocal provider_count
            now = runtime._now()
            if phase == "started":
                if pending_failed_attempt is not None:
                    runtime.store.save_provider_attempt(
                        snapshot.game_id, pending_failed_attempt
                    )
                    pending_failed_attempt = None
                provider_request_id = str(
                    details.get("provider_request_id", provider_request.request_id)
                )
                attempt_number = (
                    len(
                        runtime.store.list_provider_attempts(
                            logical_request.planner_request_id
                        )
                    )
                    + 1
                )
                started_record = ProviderAttempt(
                    provider_attempt_id=f"provider_{uuid4().hex}",
                    planner_request_id=logical_request.planner_request_id,
                    attempt_number=attempt_number,
                    provider_request_id=provider_request_id,
                    status=ProviderAttemptStatus.STARTED,
                    started_at=now,
                    diagnostics=details.get("diagnostics", {}),
                )
                logical_request = runtime.store.start_provider_attempt(
                    snapshot.game_id, logical_request, started_record
                )
                active_provider_attempt = started_record
                provider_count += 1
                runtime._checkpoint("after_provider_attempt_started")
                return
            if phase == "failed" and active_provider_attempt is not None:
                failed = active_provider_attempt.model_copy(
                    update={
                        "status": ProviderAttemptStatus.FAILED,
                        "completed_at": now,
                        "latency_seconds": max(
                            0.0,
                            (now - active_provider_attempt.started_at).total_seconds(),
                        ),
                        "diagnostics": details.get("diagnostics", details),
                        "failure_category": str(
                            details.get("failure_category", "provider_retry_failed")
                        ),
                    }
                )
                pending_failed_attempt = failed
                active_provider_attempt = None

        setter = getattr(runtime.planner, "set_provider_attempt_hook", None)
        hook_supported = (
            bool(setter(provider_attempt_hook)) if callable(setter) else False
        )
        if not hook_supported:
            await provider_attempt_hook(
                "started", {"provider_request_id": provider_request.request_id}
            )

        started_monotonic = time.perf_counter()
        response: (
            StrategicResearchProposalResponse | MissionGraphPatchResponse | None
        ) = None
        canonical_response_payload: dict[str, Any] | None = None
        error: Exception | None = None
        contract_error: Exception | None = None
        planner_scope = getattr(runtime.planner, "logical_request_scope", None)
        scope = (
            planner_scope(logical_request.planner_request_id)
            if callable(planner_scope)
            else nullcontext()
        )
        try:
            with scope:
                raw_response = await self._plan_once(provider_request, ctx.metrics)
        except Exception as exc:
            if isinstance(exc, InjectedCrashBoundary):
                raise
            error = exc
        else:
            try:
                if mission_repair:
                    canonical_response_payload = (
                        canonical_mission_graph_patch_response_payload(raw_response)
                    )
                    response = MissionGraphPatchResponse.model_validate_json(
                        json.dumps(canonical_response_payload)
                    )
                else:
                    canonical_response_payload = (
                        canonical_strategic_research_proposal_response_payload(
                            raw_response
                        )
                    )
                    response = StrategicResearchProposalResponse.model_validate_json(
                        json.dumps(canonical_response_payload)
                    )
            except Exception as exc:
                contract_error = exc
        finally:
            if hook_supported:
                setter(None)
        completed = runtime._now()
        duration = max(0.0, time.perf_counter() - started_monotonic)
        diagnostics = self._json_diagnostics(
            getattr(runtime.planner, "last_diagnostics", None)
        )
        if active_provider_attempt is not None:
            completed_attempt = active_provider_attempt.model_copy(
                update={
                    "status": (
                        ProviderAttemptStatus.SUCCEEDED
                        if error is None
                        else ProviderAttemptStatus.FAILED
                    ),
                    "completed_at": completed,
                    "latency_seconds": max(
                        0.0,
                        (
                            completed - active_provider_attempt.started_at
                        ).total_seconds(),
                    ),
                    "diagnostics": diagnostics,
                    "failure_category": (
                        None if error is None else type(error).__name__
                    ),
                }
            )
            provider_attempts = [completed_attempt]
        elif pending_failed_attempt is not None:
            provider_attempts = [pending_failed_attempt]
        ctx.metrics.provider_attempt_count += provider_count
        compatibility.agent_invoked = True
        compatibility.planner_request_id = logical_request.planner_request_id
        runtime._checkpoint("after_provider_call")

        if error is not None:
            return self._strategic_provider_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                provider_count,
                error,
            )
        if contract_error is not None:
            return self._strategic_contract_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                provider_count,
                str(contract_error),
            )

        assert response is not None
        assert canonical_response_payload is not None
        if response.information_requests:
            if logical_request.information_round_count >= 1:
                return self._strategic_contract_failure(
                    ctx,
                    snapshot,
                    logical_request,
                    compatibility,
                    provider_attempts,
                    provider_count,
                    "information round limit exceeded",
                    response_payload=canonical_response_payload,
                    failure_category="information_round_limit_exceeded",
                )
            try:
                for information_request in response.information_requests:
                    validate_information_request(information_request)
            except Exception as exc:
                return self._strategic_contract_failure(
                    ctx,
                    snapshot,
                    logical_request,
                    compatibility,
                    provider_attempts,
                    provider_count,
                    str(exc),
                    response_payload=canonical_response_payload,
                    failure_category="invalid_information_request",
                )
            if (
                not provider_attempts
                or provider_attempts[-1].status is not ProviderAttemptStatus.SUCCEEDED
            ):
                raise RuntimeError(
                    "strategic information response has no successful ProviderAttempt"
                )
            source_attempt = provider_attempts[-1]
            round_id = f"info_round_{uuid4().hex}"
            pending = tuple(
                request.model_dump(mode="json")
                for request in response.information_requests
            )
            round_record = InformationRound(
                information_round_id=round_id,
                planner_request_id=logical_request.planner_request_id,
                round_number=logical_request.information_round_count + 1,
                source_provider_attempt_id=source_attempt.provider_attempt_id,
                source_provider_attempt_number=source_attempt.attempt_number,
                status=InformationRoundStatus.REQUESTED,
                requests=pending,
                requested_at=completed,
            )
            updated_request = logical_request.model_copy(
                update={
                    "status": PlannerRequestStatus.AWAITING_INFORMATION,
                    "pending_information_requests": pending,
                }
            )
            ctx.metrics.information_round_count += 1
            return self._finish(
                ctx,
                snapshot,
                InformationRequestedTick,
                compatibility=compatibility,
                planner_request=updated_request,
                provider_attempts=provider_attempts,
                information_round=round_record,
                planner_request_id=logical_request.planner_request_id,
                information_round_id=round_id,
            )

        if isinstance(response, MissionGraphPatchResponse):
            return self._complete_mission_graph_patch(
                ctx,
                observation,
                logical_request,
                compatibility,
                response,
                canonical_response_payload,
                provider_request,
                provider_attempts,
                duration,
                completed,
            )

        if len(response.proposal_candidates) != 1:
            return self._strategic_contract_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                provider_count,
                "final response requires exactly one Proposal candidate",
                response_payload=canonical_response_payload,
                failure_category="invalid_proposal_candidate_count",
            )
        if not provider_attempts:
            raise RuntimeError("strategic Proposal has no final ProviderAttempt")

        candidate = response.proposal_candidates[0]
        try:
            if candidate.created_from_observation_id != logical_request.observation_id:
                raise ValueError("Proposal observation identity does not match Request")
            for field_name, values in (
                ("strategic_objectives", candidate.strategic_objectives),
                ("global_constraints", candidate.global_constraints),
            ):
                if any(not value.strip() for value in values):
                    raise ValueError(f"{field_name} contain a blank value")
            proposal = build_strategic_research_proposal(
                proposal_id=build_strategic_research_proposal_id(
                    logical_request.planner_request_id
                ),
                game_session_id=snapshot.game_id,
                source_planner_request_id=logical_request.planner_request_id,
                source_provider_attempt_id=provider_attempts[-1].provider_attempt_id,
                source_provider_attempt_number=provider_attempts[-1].attempt_number,
                target_kind=logical_request.target.kind,
                target_contract_id=target_contract_id,
                expected_base_revision=expected_base_revision,
                strategic_objectives=tuple(sorted(set(candidate.strategic_objectives))),
                global_constraints=tuple(sorted(set(candidate.global_constraints))),
                proposed_research_mission=candidate.proposed_research_mission,
                created_from_observation_id=candidate.created_from_observation_id,
                created_at=completed,
            )
        except Exception as exc:
            return self._strategic_contract_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                provider_count,
                str(exc),
                response_payload=canonical_response_payload,
                failure_category="invalid_strategic_proposal",
            )

        updated_request = logical_request.model_copy(
            update={
                "status": PlannerRequestStatus.COMPLETED,
                "completed_at": completed,
                "response_payload": canonical_response_payload,
                "response_hash": canonical_json_hash(canonical_response_payload),
                "validation_result": {
                    "result": "completed",
                    "proposal_id": proposal.proposal_id,
                    "proposal_hash": proposal.proposal_hash,
                },
                "failure_category": None,
            }
        )
        compatibility.paused = True
        compatibility.pause_reason = "strategic_contract_proposal_ready"
        runtime.store.record_agent_run(
            snapshot.game_id,
            provider_request,
            response=response,
            success=True,
            error=None,
            duration_seconds=duration,
        )
        try:
            result = self._finish(
                ctx,
                snapshot,
                StrategicProposalReadyTick,
                compatibility=compatibility,
                planner_request=updated_request,
                provider_attempts=provider_attempts,
                strategic_research_proposal=proposal,
                planner_request_id=logical_request.planner_request_id,
                proposal_id=proposal.proposal_id,
                target_kind=logical_request.target.kind,
                expected_base_revision=expected_base_revision,
                blocking_reason="strategic_contract_proposal_ready",
            )
        except StaleStrategicContractBaseError as exc:
            return self._strategic_contract_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                provider_count,
                str(exc),
                response_payload=canonical_response_payload,
                failure_category="stale_strategic_contract_base",
            )
        runtime._checkpoint("after_provider_attempt_finalized")
        return result

    def _complete_mission_graph_patch(
        self,
        ctx,
        observation,
        logical_request: PlannerRequest,
        compatibility: TickResult,
        response: MissionGraphPatchResponse,
        canonical_response_payload: dict[str, Any],
        provider_request: AgentRequest,
        provider_attempts: list[ProviderAttempt],
        duration: float,
        completed,
    ) -> TickResult:
        runtime = self.runtime
        snapshot = observation.snapshot
        if len(response.patch_candidates) != 1:
            return self._strategic_contract_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                len(provider_attempts),
                "final response requires exactly one MissionGraphPatch candidate",
                response_payload=canonical_response_payload,
                failure_category="invalid_mission_graph_patch",
            )
        if (
            not provider_attempts
            or provider_attempts[-1].status is not ProviderAttemptStatus.SUCCEEDED
        ):
            raise RuntimeError("MissionGraphPatch has no final ProviderAttempt")

        target = logical_request.target
        active = runtime.store.get_active_strategic_contract(snapshot.game_id)
        projection = thaw_json(logical_request.input_projection)
        repair_context = projection.get("mission_repair_context")
        candidate = response.patch_candidates[0]
        try:
            if (
                active is None
                or target.strategic_contract_id != active.contract_id
                or target.base_contract_revision != active.revision
            ):
                raise ValueError("MissionGraphPatch Contract base is stale")
            if not isinstance(repair_context, dict):
                raise ValueError("MissionGraphPatch input context is missing")
            if candidate.created_from_observation_id != logical_request.observation_id:
                raise ValueError(
                    "MissionGraphPatch observation identity does not match Request"
                )
            source_attempt = provider_attempts[-1]
            patch = build_mission_graph_patch(
                patch_id=build_mission_graph_patch_id(
                    logical_request.planner_request_id
                ),
                game_session_id=snapshot.game_id,
                contract_id=active.contract_id,
                expected_base_revision=active.revision,
                source_state_delta_id=str(repair_context["source_state_delta_id"]),
                source_planner_request_id=logical_request.planner_request_id,
                source_provider_attempt_id=source_attempt.provider_attempt_id,
                source_provider_attempt_number=source_attempt.attempt_number,
                affected_mission_ids=target.affected_mission_ids,
                mission_updates=tuple(
                    sorted(
                        candidate.mission_updates,
                        key=lambda mission: mission.mission_id,
                    )
                ),
                created_from_observation_id=candidate.created_from_observation_id,
                created_at=completed,
            )
            by_id = {
                mission.mission_id: mission for mission in active.mission_graph.missions
            }
            by_id.update(
                {mission.mission_id: mission for mission in patch.mission_updates}
            )
            updated_graph = MissionGraph(
                missions=tuple(sorted(by_id.values(), key=lambda item: item.mission_id))
            )
            next_contract = StrategicContract(
                contract_id=active.contract_id,
                game_session_id=active.game_session_id,
                revision=active.revision + 1,
                authority_scope_set=active.authority_scope_set,
                mission_graph=updated_graph,
                strategic_objectives=active.strategic_objectives,
                global_constraints=active.global_constraints,
                created_from_observation_id=patch.created_from_observation_id,
                approval_status=active.approval_status,
                policy_snapshot=thaw_json(active.policy_snapshot),
            )
            source_mission = patch.mission_updates[0]
            commit = StrategicContractCommit(
                commit_id=f"mission_graph_patch_commit_{patch.patch_id}",
                game_session_id=snapshot.game_id,
                contract_id=active.contract_id,
                expected_base_revision=active.revision,
                contract=next_contract,
                committed_at=completed,
                reason="deterministic local MissionGraph repair",
                source_patch_id=patch.patch_id,
                source_state_delta_id=patch.source_state_delta_id,
                source_patch_planner_request_id=logical_request.planner_request_id,
                source_patch_provider_attempt_id=source_attempt.provider_attempt_id,
                source_patch_mission_id=source_mission.mission_id,
                source_patch_mission_revision=source_mission.mission_revision,
            )
        except Exception as exc:
            return self._strategic_contract_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                len(provider_attempts),
                str(exc),
                response_payload=canonical_response_payload,
                failure_category="invalid_mission_graph_patch",
            )

        updated_request = logical_request.model_copy(
            update={
                "status": PlannerRequestStatus.COMPLETED,
                "completed_at": completed,
                "response_payload": canonical_response_payload,
                "response_hash": canonical_json_hash(canonical_response_payload),
                "validation_result": {
                    "result": "completed",
                    "patch_id": patch.patch_id,
                    "patch_hash": patch.patch_hash,
                },
                "failure_category": None,
            }
        )
        ctx.metrics.mcp_call_count = runtime.game.call_count - ctx.call_count_before
        ctx.metrics.mutation_count = ctx.budget.used
        ctx.metrics.total_seconds = runtime._monotonic() - ctx.started_monotonic
        tick_completed = runtime._now()
        tick = validate_workflow_tick(
            MissionGraphPatchedTick(
                tick_id=ctx.tick_id,
                game_session_id=snapshot.game_id,
                turn_number=snapshot.turn,
                starting_runtime_state=ctx.starting_state,
                observation_ids=tuple(ctx.observation_ids),
                started_at=ctx.started_at,
                completed_at=tick_completed,
                metrics=ctx.metrics.model_dump(mode="json"),
                planner_request_id=logical_request.planner_request_id,
                provider_attempt_id=source_attempt.provider_attempt_id,
                patch_id=patch.patch_id,
                state_delta_id=patch.source_state_delta_id,
                contract_id=patch.contract_id,
                committed_revision=next_contract.revision,
                previous_baseline_observation_id=str(
                    repair_context["baseline_observation_id"]
                ),
                accepted_observation_id=patch.created_from_observation_id,
            )
        )
        runtime.store.record_agent_run(
            snapshot.game_id,
            provider_request,
            response=response,
            success=True,
            error=None,
            duration_seconds=duration,
        )
        try:
            runtime.store.apply_mission_graph_patch(
                tick=tick,
                planner_request=updated_request,
                provider_attempt=source_attempt,
                patch=patch,
                commit=commit,
                checkpoint=runtime._checkpoint,
            )
        except ValueError as exc:
            return self._strategic_contract_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                len(provider_attempts),
                str(exc),
                response_payload=canonical_response_payload,
                failure_category="stale_mission_graph_patch",
            )
        runtime._checkpoint("after_provider_attempt_finalized")
        compatibility.metrics = ctx.metrics
        compatibility.tick_id = tick.tick_id
        compatibility.runtime_state = tick.ending_runtime_state.value
        compatibility.workflow_tick = tick.model_dump(mode="json")
        compatibility.planner_request_id = logical_request.planner_request_id
        return compatibility

    def _strategic_provider_failure(
        self,
        ctx,
        snapshot,
        logical_request,
        compatibility,
        provider_attempts,
        provider_count,
        error,
    ):
        runtime = self.runtime
        failure = self._classify_planner_failure(error)
        transient = bool(failure["transient"])
        retry_at = None
        if transient:
            existing_attempts = runtime.store.list_provider_attempts(
                logical_request.planner_request_id
            )
            failed_ids = {
                attempt.provider_attempt_id
                for attempt in existing_attempts
                if attempt.status is ProviderAttemptStatus.FAILED
            }
            failed_ids.update(
                attempt.provider_attempt_id
                for attempt in provider_attempts
                if attempt.status is ProviderAttemptStatus.FAILED
            )
            failure_count = max(1, len(failed_ids))
            delay = min(120.0, 5.0 * (2 ** min(failure_count - 1, 5)))
            retry_at = runtime._now() + timedelta(seconds=delay)
        updated_request = logical_request.model_copy(
            update={
                "status": (
                    PlannerRequestStatus.BACKOFF
                    if transient
                    else PlannerRequestStatus.FAILED
                ),
                "failure_category": str(failure["category"]),
                "completed_at": None if transient else runtime._now(),
                "next_retry_at": retry_at,
            }
        )
        if transient:
            return self._finish(
                ctx,
                snapshot,
                PlannerAttemptCompletedTick,
                compatibility=compatibility,
                planner_request=updated_request,
                provider_attempts=provider_attempts,
                planner_request_id=logical_request.planner_request_id,
                provider_attempt_id=self._provider_tick_id(
                    logical_request, provider_attempts
                ),
                provider_attempt_count=provider_count,
            )
        compatibility.paused = True
        compatibility.pause_reason = f"planner failed: {failure['category']}"
        return self._finish(
            ctx,
            snapshot,
            StrategicRequestTerminatedTick,
            compatibility=compatibility,
            planner_request=updated_request,
            provider_attempts=provider_attempts,
            planner_request_id=logical_request.planner_request_id,
            terminal_status=PlannerRequestStatus.FAILED,
            failure_category=str(failure["category"]),
            provider_attempt_id=provider_attempts[-1].provider_attempt_id,
            blocking_reason=compatibility.pause_reason,
        )

    def _strategic_contract_failure(
        self,
        ctx,
        snapshot,
        logical_request,
        compatibility,
        provider_attempts,
        provider_count,
        reason,
        *,
        response_payload=None,
        failure_category="planner_contract_failure",
    ):
        response_evidence = (
            {}
            if response_payload is None
            else {
                "response_payload": response_payload,
                "response_hash": canonical_json_hash(response_payload),
                "validation_result": {
                    "result": "rejected",
                    "reason": reason[:300],
                },
            }
        )
        updated_request = logical_request.model_copy(
            update={
                **response_evidence,
                "status": PlannerRequestStatus.REJECTED,
                "completed_at": self.runtime._now(),
                "failure_category": failure_category,
            }
        )
        compatibility.paused = True
        compatibility.pause_reason = f"strategic Proposal rejected: {reason[:300]}"
        return self._finish(
            ctx,
            snapshot,
            StrategicRequestTerminatedTick,
            compatibility=compatibility,
            planner_request=updated_request,
            provider_attempts=provider_attempts,
            planner_request_id=logical_request.planner_request_id,
            terminal_status=PlannerRequestStatus.REJECTED,
            failure_category=failure_category,
            provider_attempt_id=provider_attempts[-1].provider_attempt_id,
            blocking_reason=compatibility.pause_reason,
        )

    @staticmethod
    def _json_diagnostics(value):
        if not isinstance(value, dict):
            return {}
        return json.loads(json.dumps(value, default=str))

    async def _plan_once(self, request: AgentRequest, metrics) -> Any:
        metrics.agent_attempt_count += 1
        metrics.agent_call_count = metrics.agent_attempt_count
        response = await self.runtime.planner.plan(request)
        metrics.agent_success_count += 1
        return response

    def _active_backoff(self, request: PlannerRequest) -> dict[str, Any] | None:
        if request.status is not PlannerRequestStatus.BACKOFF:
            return None
        if request.next_retry_at is None:
            raise ValueError("strategic BACKOFF request requires next_retry_at")
        remaining = (request.next_retry_at - self.runtime._now()).total_seconds()
        if remaining <= 0:
            return None
        attempts = self.runtime.store.list_provider_attempts(request.planner_request_id)
        return {
            "category": request.failure_category,
            "failure_count": sum(
                attempt.status is ProviderAttemptStatus.FAILED for attempt in attempts
            ),
            "until": request.next_retry_at.isoformat(),
            "remaining_seconds": remaining,
        }

    def _classify_planner_failure(self, exc: Exception) -> dict[str, Any]:
        diagnostics = getattr(self.runtime.planner, "last_diagnostics", None)
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

    @staticmethod
    def _provider_tick_id(logical_request, provider_attempts):
        if provider_attempts:
            return provider_attempts[-1].provider_attempt_id
        return f"provider_none_{logical_request.planner_request_id}"

    def _finish(
        self,
        ctx,
        snapshot,
        tick_type,
        *,
        compatibility=None,
        planner_request=None,
        provider_attempts=(),
        information_round=None,
        strategic_research_proposal=None,
        **fields,
    ):
        runtime = self.runtime
        completed = runtime._now()
        ctx.metrics.mcp_call_count = runtime.game.call_count - ctx.call_count_before
        ctx.metrics.mutation_count = ctx.budget.used
        ctx.metrics.total_seconds = runtime._monotonic() - ctx.started_monotonic
        common = {
            "tick_id": ctx.tick_id,
            "game_session_id": snapshot.game_id,
            "turn_number": snapshot.turn,
            "starting_runtime_state": ctx.starting_state,
            "observation_ids": tuple(ctx.observation_ids),
            "started_at": ctx.started_at,
            "completed_at": completed,
            "metrics": ctx.metrics.model_dump(mode="json"),
        }
        tick = validate_workflow_tick(tick_type(**common, **fields))
        human_wait_context = None
        if isinstance(tick, StrategicProposalReadyTick):
            human_wait_context = runtime._human_wait_context(snapshot)
            human_wait_context.update(
                {
                    "wait_kind": "strategic_contract_proposal_ready",
                    "resume_policy": "explicit_only",
                    "reason": "strategic_contract_proposal_ready",
                    "blocking_reason": tick.blocking_reason,
                    "proposal_ready_tick_id": tick.tick_id,
                    "planner_request_id": tick.planner_request_id,
                    "proposal_id": tick.proposal_id,
                    "target_kind": tick.target_kind.value,
                    "expected_base_revision": tick.expected_base_revision,
                }
            )
        elif isinstance(tick, (AwaitingHumanTick, StrategicRequestTerminatedTick)):
            human_wait_context = runtime._human_wait_context(snapshot)
            human_wait_context["blocking_reason"] = tick.blocking_reason
            if isinstance(tick, StrategicRequestTerminatedTick):
                human_wait_context.update(
                    {
                        "wait_kind": "strategic_request_terminated",
                        "resume_policy": "explicit_only",
                        "planner_request_id": tick.planner_request_id,
                        "terminal_tick_id": tick.tick_id,
                        "terminal_status": tick.terminal_status.value,
                        "failure_category": tick.failure_category,
                    }
                )
        runtime.store.persist_phase4_tick(
            tick,
            planner_request=planner_request,
            strategic_research_proposal=strategic_research_proposal,
            provider_attempts=provider_attempts,
            information_round=information_round,
            human_wait_context=human_wait_context,
        )
        result = compatibility or TickResult(turn=snapshot.turn, metrics=ctx.metrics)
        result.metrics = ctx.metrics
        result.tick_id = tick.tick_id
        result.runtime_state = tick.ending_runtime_state.value
        result.workflow_tick = tick.model_dump(mode="json")
        if planner_request is not None:
            result.planner_request_id = planner_request.planner_request_id
        return result
