"""Persistent planner lifecycle used by the canonical bounded workflow engine."""

from __future__ import annotations

import hashlib
import json
import time
from contextlib import nullcontext
from typing import Any
from uuid import uuid4

from .conditions import extract_known_entities
from .domain.base import thaw_json
from .decisioning import (
    STRATEGIC_GAP_TYPES,
    batch_compatible_gaps,
    build_decision_gap,
    evaluate_plan_lease,
    evaluate_planner_eligibility,
    stable_decision_identity,
)
from .domain import (
    ApprovalStatus,
    AwaitingHumanTick,
    Condition,
    DecisionGap,
    DecisionGapCreatedTick,
    DecisionGapStatus,
    DecisionGapUpdatedTick,
    InformationCollectedTick,
    InformationRequestedTick,
    InformationRound,
    InformationRoundStatus,
    LeaseValidationResult,
    LogicalPlannerRequestCreatedTick,
    PlanLease,
    PlanLeaseStatus,
    PlanLeaseUpdatedTick,
    PlannerAttemptCompletedTick,
    PlannerBackoffTick,
    PlannerRequest,
    PlannerRequestStatus,
    ProviderAttempt,
    ProviderAttemptStatus,
    RuntimeState,
    SubjectRef,
    validate_workflow_tick,
)
from .models import (
    AgentRequest,
    EventLevel,
    GameEvent,
    RiskLevel,
    TickResult,
)
from .validation import PlanValidationContext, validate_plan_bundle
from .workflow_protocol import (
    InformationRequest,
    ResolutionDisposition,
    WorkflowPlanBundle,
)


class PlannerLifecycleCoordinator:
    """Advance durable planning state without owning the workflow Tick loop."""

    def __init__(self, engine: Any):
        self.engine = engine

    def validate_before_routing(self, ctx, observation, current_events, compatibility):
        if ctx.starting_state in {
            RuntimeState.AWAITING_APPROVAL,
            RuntimeState.AWAITING_HUMAN,
            RuntimeState.TURN_TRANSITIONING,
            RuntimeState.VERIFYING,
        }:
            return None
        return self._validate_leases(
            ctx, observation, compatibility, current_events=current_events
        )

    async def advance(
        self, ctx, observation, agent_events, compatibility, *, current_events=None
    ):
        engine = self.engine
        snapshot = observation.snapshot
        game_id = snapshot.game_id
        if ctx.starting_state in {
            RuntimeState.SYSTEM_ERROR,
            RuntimeState.AWAITING_APPROVAL,
            RuntimeState.AWAITING_HUMAN,
            RuntimeState.TURN_TRANSITIONING,
            RuntimeState.VERIFYING,
        }:
            return [], None

        active = engine.store.active_planner_request(game_id)
        if active is not None:
            stale_tick = self._supersede_stale_request(
                ctx,
                observation,
                active,
                compatibility,
                current_events=current_events or agent_events,
            )
            if stale_tick is not None:
                return [], stale_tick
            if active.status is PlannerRequestStatus.AWAITING_INFORMATION:
                return [], await self._collect_information(
                    ctx, observation, active, compatibility
                )
            backoff = engine._active_backoff()
            if active.status is PlannerRequestStatus.BACKOFF and backoff:
                engine.store.record_planner_suppression(
                    game_id,
                    snapshot.turn,
                    reason="provider_backoff",
                    relevant_input_hash=active.input_projection_hash,
                )
                return [], self._finish(
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
            return [], await self._continue_request(
                ctx, observation, active, compatibility
            )

        decision_events = current_events if current_events is not None else agent_events
        strategic = [
            event
            for event in decision_events
            if event.event_type in STRATEGIC_GAP_TYPES
        ]
        if not strategic:
            return [], None
        context = engine.store.current_context(game_id)
        context["execution_mode"] = engine.config.execution_mode.value
        observation_id = engine._active_observation_id or ctx.observation_ids[-1]
        gaps: list[DecisionGap] = []
        changed: list[DecisionGap] = []
        update_reason = "decision gap discovered"
        for event in strategic:
            prototype = build_decision_gap(
                game_id,
                observation_id,
                snapshot,
                event,
                context,
                now=engine._now(),
            )
            existing = engine.store.decision_gap_by_identity(
                game_id, prototype.stable_identity
            )
            gap = (
                prototype
                if existing is None
                else build_decision_gap(
                    game_id,
                    observation_id,
                    snapshot,
                    event,
                    context,
                    existing=existing,
                    now=engine._now(),
                )
            )
            if existing is None:
                changed.append(gap)
            elif existing.relevant_input_hash != gap.relevant_input_hash:
                update_reason = "material decision input changed"
                changed.append(gap)
            else:
                gap = gap.model_copy(
                    update={
                        "status": existing.status,
                        "route": existing.route,
                        "logical_request_id": existing.logical_request_id,
                        "resolution_reason": existing.resolution_reason,
                        "invalidation_reason": existing.invalidation_reason,
                        "reopen_reason": existing.reopen_reason,
                    }
                )
                engine.store.save_decision_gap(gap, turn=snapshot.turn)
            gaps.append(gap)

        if changed:
            tick_type = (
                DecisionGapCreatedTick
                if update_reason == "decision gap discovered"
                else DecisionGapUpdatedTick
            )
            fields = {"decision_gap_id": changed[0].decision_gap_id}
            if tick_type is DecisionGapUpdatedTick:
                fields["update_reason"] = update_reason
            return [], self._finish(
                ctx,
                snapshot,
                tick_type,
                compatibility=compatibility,
                decision_gaps=changed,
                **fields,
            )
        return [], self._create_request_if_eligible(
            ctx,
            snapshot,
            strategic,
            gaps,
            compatibility,
            observation_id,
        )

    def _validate_leases(self, ctx, observation, compatibility, *, current_events):
        engine = self.engine
        snapshot = observation.snapshot
        observation_id = engine._active_observation_id or ctx.observation_ids[-1]
        for lease in engine.store.list_plan_leases(snapshot.game_id):
            if lease.status is not PlanLeaseStatus.ACTIVE:
                continue
            gap = (
                engine.store.get_decision_gap(
                    snapshot.game_id, lease.decision_gap_ids[0]
                )
                if lease.decision_gap_ids
                else None
            )
            current_gap = (
                None
                if gap is None
                else self._rebuild_current_gap(gap, observation, current_events)
            )
            if current_gap is None:
                reason = "the source strategic decision is no longer present"
                updated_lease = lease.model_copy(
                    update={
                        "status": PlanLeaseStatus.INVALIDATED,
                        "last_validated_observation_id": observation_id,
                        "last_validation_result": LeaseValidationResult.INVALIDATED,
                        "invalidation_reason": reason,
                    }
                )
                invalidated_gap = (
                    []
                    if gap is None
                    else [
                        gap.model_copy(
                            update={
                                "status": DecisionGapStatus.INVALIDATED,
                                "observation_id": observation_id,
                                "logical_request_id": None,
                                "invalidation_reason": reason,
                                "resolution_reason": None,
                            }
                        )
                    ]
                )
                return self._finish(
                    ctx,
                    snapshot,
                    PlanLeaseUpdatedTick,
                    compatibility=compatibility,
                    decision_gaps=invalidated_gap,
                    plan_leases=[updated_lease],
                    cancel_task_ids=self._dependent_task_ids(lease),
                    plan_lease_id=lease.plan_lease_id,
                    validation_result=LeaseValidationResult.INVALIDATED.value,
                )

            evaluation = evaluate_plan_lease(
                lease,
                observation,
                relevant_input_hash=current_gap.relevant_input_hash,
                evaluator=engine.conditions,
            )
            input_changed = current_gap.relevant_input_hash != gap.relevant_input_hash
            material = (
                input_changed
                or evaluation.lease.status is not lease.status
                or evaluation.lease.valid_until_turn != lease.valid_until_turn
                or evaluation.lease.last_validation_result
                is not lease.last_validation_result
            )
            if not material:
                continue

            gap_update = current_gap.model_copy(
                update={
                    "status": gap.status,
                    "route": gap.route,
                    "logical_request_id": gap.logical_request_id,
                    "resolution_reason": gap.resolution_reason,
                    "invalidation_reason": gap.invalidation_reason,
                    "reopen_reason": gap.reopen_reason,
                }
            )
            cancel_task_ids: tuple[str, ...] = ()
            if evaluation.result in {
                LeaseValidationResult.EXPIRED,
                LeaseValidationResult.INVALIDATED,
                LeaseValidationResult.UNKNOWN,
            }:
                gap_update = gap_update.model_copy(
                    update={
                        "status": DecisionGapStatus.OPEN,
                        "logical_request_id": None,
                        "reopen_reason": evaluation.reason,
                        "resolution_reason": None,
                        "invalidation_reason": None,
                    }
                )
                cancel_task_ids = self._dependent_task_ids(lease)

            if evaluation.result is LeaseValidationResult.UNKNOWN:
                compatibility.paused = True
                compatibility.pause_reason = evaluation.reason
                return self._finish(
                    ctx,
                    snapshot,
                    AwaitingHumanTick,
                    compatibility=compatibility,
                    decision_gaps=[gap_update],
                    plan_leases=[evaluation.lease],
                    cancel_task_ids=cancel_task_ids,
                    blocking_reason=evaluation.reason,
                )
            return self._finish(
                ctx,
                snapshot,
                PlanLeaseUpdatedTick,
                compatibility=compatibility,
                decision_gaps=[gap_update],
                plan_leases=[evaluation.lease],
                cancel_task_ids=cancel_task_ids,
                plan_lease_id=lease.plan_lease_id,
                validation_result=evaluation.result.value,
            )
        return None

    def _dependent_task_ids(self, lease):
        subjects = {
            (subject.subject_type, subject.subject_id) for subject in lease.subjects
        }
        result = set(lease.task_ids)
        for task in self.engine.store.list_tasks(lease.game_session_id):
            if task.plan_id == lease.plan_id or (
                (task.entity_type, str(task.entity_id)) in subjects
            ):
                result.add(task.task_id)
        return tuple(sorted(result))

    def _rebuild_current_gap(self, gap, observation, current_events):
        engine = self.engine
        snapshot = observation.snapshot
        context = engine.store.current_context(snapshot.game_id)
        context["execution_mode"] = engine.config.execution_mode.value
        matching = None
        for event in current_events:
            if event.event_type not in STRATEGIC_GAP_TYPES:
                continue
            try:
                identity, _ = stable_decision_identity(event)
            except ValueError:
                continue
            if identity == gap.stable_identity:
                matching = event
                break

        if matching is None and gap.gap_type in {
            "pending_diplomacy",
            "pending_trade_offer",
            "world_congress_vote_required",
            "emergency_response_window",
        }:
            return None
        if matching is None:
            projection = thaw_json(gap.input_projection)
            subject = projection.get("subject", {})
            matching = GameEvent(
                event_type=gap.gap_type,
                turn=snapshot.turn,
                entity_type=subject.get("type"),
                entity_id=subject.get("id"),
                level=EventLevel.L3,
                risk=RiskLevel.HIGH,
                blocking=True,
                payload=projection.get("event_facts", {}),
                dedupe_key=gap.cooldown_key,
            )
        return build_decision_gap(
            snapshot.game_id,
            engine._active_observation_id,
            snapshot,
            matching,
            context,
            existing=gap,
            now=engine._now(),
        )

    def _supersede_stale_request(
        self,
        ctx,
        observation,
        request,
        compatibility,
        *,
        current_events,
    ):
        engine = self.engine
        snapshot = observation.snapshot
        gaps = self._request_gaps(request)
        reason = None
        refreshed = []
        if len(gaps) != len(request.decision_gap_ids):
            reason = "source decision gap no longer exists"
        else:
            for gap in gaps:
                current = self._rebuild_current_gap(gap, observation, current_events)
                if current is None:
                    reason = "source strategic event is no longer active"
                    break
                refreshed.append(current)

        if reason is None:
            group = batch_compatible_gaps(
                snapshot.game_id,
                engine._active_observation_id or request.observation_id,
                refreshed,
                now=engine._now(),
            )
            if group.decision_group_id != request.decision_group_id:
                reason = "stable decision identity was replaced"
            elif group.input_projection_hash != request.input_projection_hash:
                reason = "relevant decision input changed"
            elif (
                tuple(
                    revision
                    for gap in refreshed
                    for revision in gap.relevant_plan_revisions
                )
                != request.plan_revision_refs
            ):
                reason = "relevant plan revision changed"
            elif request.policy_revision != "planner-call-policy/v1":
                reason = "planner policy revision changed"
            elif request.approval_contract_hash != self._contract_hash(
                [
                    thaw_json(gap.input_projection).get("approval", {})
                    for gap in refreshed
                ]
            ):
                reason = "approval contract changed"
            elif request.allowed_actions_hash != self._contract_hash(
                sorted(engine.config.allowed_action_types)
            ):
                reason = "allowed action contract changed"

        if reason is None:
            return None

        updated_request = request.model_copy(
            update={
                "status": PlannerRequestStatus.SUPERSEDED,
                "completed_at": engine._now(),
                "failure_category": "stale_planning_input",
                "pending_information_requests": (),
            }
        )
        information_round = None
        rounds = engine.store.list_information_rounds(request.planner_request_id)
        if rounds and rounds[-1].status is InformationRoundStatus.REQUESTED:
            information_round = rounds[-1].model_copy(
                update={
                    "status": InformationRoundStatus.FAILED,
                    "completed_at": engine._now(),
                }
            )
        reopened = []
        source = refreshed or gaps
        for gap in source:
            reopened.append(
                gap.model_copy(
                    update={
                        "status": DecisionGapStatus.OPEN,
                        "logical_request_id": None,
                        "reopen_reason": reason,
                        "resolution_reason": None,
                        "invalidation_reason": None,
                    }
                )
            )
        if not reopened:
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                compatibility=compatibility,
                planner_request=updated_request,
                information_round=information_round,
                blocking_reason=reason,
            )
        return self._finish(
            ctx,
            snapshot,
            DecisionGapUpdatedTick,
            compatibility=compatibility,
            decision_gaps=reopened,
            planner_request=updated_request,
            information_round=information_round,
            decision_gap_id=reopened[0].decision_gap_id,
            update_reason=reason,
        )

    @staticmethod
    def _contract_hash(value):
        return hashlib.sha256(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                default=str,
            ).encode("utf-8")
        ).hexdigest()

    def _create_request_if_eligible(
        self,
        ctx,
        snapshot,
        strategic,
        gaps,
        compatibility,
        observation_id,
    ):
        engine = self.engine
        open_gaps = [
            gap
            for gap in gaps
            if gap.status
            in {DecisionGapStatus.OPEN, DecisionGapStatus.PLANNER_ELIGIBLE}
        ]
        eligibility = evaluate_planner_eligibility(
            open_gaps,
            engine.store.list_plan_leases(snapshot.game_id),
            runtime_state=ctx.starting_state.value,
            has_ready_deterministic_task=bool(
                engine.store.due_tasks(snapshot.game_id, snapshot.turn)
            ),
            active_attempt=(
                engine.store.unresolved_action_attempt(snapshot.game_id) is not None
            ),
            logical_requests_this_turn=engine.store.logical_request_count_for_turn(
                snapshot.game_id, snapshot.turn
            ),
            active_logical_request=False,
            max_logical_requests_per_turn=min(
                1, engine.config.max_agent_calls_per_turn
            ),
        )
        if not eligibility.eligible:
            for gap in open_gaps:
                engine.store.record_planner_suppression(
                    snapshot.game_id,
                    snapshot.turn,
                    reason=eligibility.reason,
                    decision_gap_id=gap.decision_gap_id,
                    relevant_input_hash=gap.relevant_input_hash,
                )
                ctx.metrics.duplicate_request_suppression_count += 1
            return None

        group = batch_compatible_gaps(
            snapshot.game_id,
            observation_id,
            list(eligibility.gaps),
            now=engine._now(),
        )
        duplicate = engine.store.planner_request_for_input(
            snapshot.game_id,
            group.decision_group_id,
            group.input_projection_hash,
        )
        if duplicate is not None:
            for gap in eligibility.gaps:
                engine.store.record_planner_suppression(
                    snapshot.game_id,
                    snapshot.turn,
                    reason="stable_identity_and_input_hash_already_requested",
                    decision_gap_id=gap.decision_gap_id,
                    relevant_input_hash=gap.relevant_input_hash,
                )
                ctx.metrics.duplicate_request_suppression_count += 1
            return None

        selected_ids = set(group.decision_gap_ids)
        selected_events = [
            event
            for event, gap in zip(strategic, gaps, strict=True)
            if gap.decision_gap_id in selected_ids
        ]
        provider_request = engine._build_agent_request(snapshot, selected_events)
        constraints = dict(provider_request.constraints)
        constraints.update(
            {
                "decision_gap_ids": list(group.decision_gap_ids),
                "decision_input_projection_version": group.input_projection_version,
                "logical_request_scope": "stable_decision_group",
            }
        )
        provider_request = provider_request.model_copy(
            update={"constraints": constraints}
        )
        logical_id = f"logical_{uuid4().hex}"
        request_projection = {
            "projection_version": group.input_projection_version,
            "decision_group_id": group.decision_group_id,
            "gaps": [thaw_json(gap.input_projection) for gap in eligibility.gaps],
        }
        request_payload = provider_request.model_dump(mode="json")
        context_bytes = len(
            json.dumps(
                request_payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        )
        logical_request = PlannerRequest(
            planner_request_id=logical_id,
            game_session_id=snapshot.game_id,
            turn_number=snapshot.turn,
            observation_id=observation_id,
            decision_gap_ids=group.decision_gap_ids,
            decision_group_id=group.decision_group_id,
            input_projection_hash=group.input_projection_hash,
            input_projection=request_projection,
            request_payload=request_payload,
            plan_revision_refs=tuple(
                revision
                for gap in eligibility.gaps
                for revision in gap.relevant_plan_revisions
            ),
            policy_revision="planner-call-policy/v1",
            approval_contract_hash=self._contract_hash(
                [
                    thaw_json(gap.input_projection).get("approval", {})
                    for gap in eligibility.gaps
                ]
            ),
            allowed_actions_hash=self._contract_hash(
                sorted(engine.config.allowed_action_types)
            ),
            model_settings={"provider": type(engine.planner).__name__},
            status=PlannerRequestStatus.PENDING,
            created_at=engine._now(),
            context_bytes=context_bytes,
        )
        requested_gaps = [
            gap.model_copy(
                update={
                    "status": DecisionGapStatus.REQUESTED,
                    "logical_request_id": logical_id,
                }
            )
            for gap in eligibility.gaps
        ]
        ctx.metrics.logical_planner_request_count += 1
        ctx.metrics.planner_context_bytes += context_bytes
        compatibility.planner_request_id = logical_id
        return self._finish(
            ctx,
            snapshot,
            LogicalPlannerRequestCreatedTick,
            compatibility=compatibility,
            decision_gaps=requested_gaps,
            decision_group=group,
            planner_request=logical_request,
            planner_request_id=logical_id,
            decision_gap_ids=group.decision_gap_ids,
        )

    async def _collect_information(
        self,
        ctx,
        observation,
        logical_request: PlannerRequest,
        compatibility: TickResult,
    ) -> TickResult:
        engine = self.engine
        rounds = engine.store.list_information_rounds(
            logical_request.planner_request_id
        )
        if not rounds or rounds[-1].status is not InformationRoundStatus.REQUESTED:
            raise RuntimeError("awaiting-information request has no pending round")
        pending_round = rounds[-1]
        requests = [
            InformationRequest.model_validate(payload)
            for payload in pending_round.requests
        ]
        results = await engine.information_queries.execute(requests)
        ctx.metrics.information_query_count += len(results)
        ctx.metrics.information_round_count += 1
        now = engine._now()
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

    async def _continue_request(
        self,
        ctx,
        observation,
        logical_request: PlannerRequest,
        compatibility: TickResult,
    ) -> TickResult:
        engine = self.engine
        snapshot = observation.snapshot
        payload = thaw_json(logical_request.request_payload)
        payload["request_id"] = f"req_{uuid4().hex}"
        if logical_request.information_results:
            payload["information_results"] = thaw_json(
                logical_request.information_results
            )
            constraints = thaw_json(payload.get("constraints", {}))
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
        provider_count = 0

        async def provider_attempt_hook(phase, details):
            nonlocal logical_request, active_provider_attempt, provider_count
            now = engine._now()
            if phase == "started":
                provider_request_id = str(
                    details.get("provider_request_id", provider_request.request_id)
                )
                attempt_number = (
                    len(
                        engine.store.list_provider_attempts(
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
                logical_request = engine.store.start_provider_attempt(
                    snapshot.game_id, logical_request, started_record
                )
                active_provider_attempt = started_record
                provider_count += 1
                engine._checkpoint("after_provider_attempt_started")
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
                engine.store.save_provider_attempt(snapshot.game_id, failed)
                active_provider_attempt = None

        setter = getattr(engine.planner, "set_provider_attempt_hook", None)
        hook_supported = (
            bool(setter(provider_attempt_hook)) if callable(setter) else False
        )
        if not hook_supported:
            await provider_attempt_hook(
                "started", {"provider_request_id": provider_request.request_id}
            )

        started_monotonic = time.perf_counter()
        bundle: WorkflowPlanBundle | None = None
        error: Exception | None = None
        planner_scope = getattr(engine.planner, "logical_request_scope", None)
        scope = (
            planner_scope(logical_request.planner_request_id)
            if callable(planner_scope)
            else nullcontext()
        )
        try:
            with scope:
                raw_bundle = await engine._plan_once(provider_request, ctx.metrics)
            bundle = WorkflowPlanBundle.model_validate(
                raw_bundle.model_dump(mode="python")
            )
        except Exception as exc:
            from .engine import InjectedCrashBoundary

            if isinstance(exc, InjectedCrashBoundary):
                raise
            error = exc
        finally:
            if hook_supported:
                setter(None)
        completed = engine._now()
        duration = max(0.0, time.perf_counter() - started_monotonic)
        diagnostics = self._json_diagnostics(
            getattr(engine.planner, "last_diagnostics", None)
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
        ctx.metrics.provider_attempt_count += provider_count
        compatibility.agent_invoked = True
        compatibility.planner_request_id = logical_request.planner_request_id
        engine._checkpoint("after_provider_call")
        if error is not None:
            return self._provider_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                provider_count,
                error,
            )

        assert bundle is not None
        trigger_events = [
            event
            for event in provider_request.trigger_events
            if event.event_type in STRATEGIC_GAP_TYPES
        ]
        if bundle.information_requests:
            if logical_request.information_round_count >= 1:
                return self._contract_failure(
                    ctx,
                    snapshot,
                    logical_request,
                    compatibility,
                    provider_attempts,
                    provider_count,
                    "information round limit exceeded",
                )
            try:
                engine._validate_planner_bundle(
                    bundle,
                    provider_request,
                    snapshot,
                    trigger_events,
                    allow_information_requests=True,
                )
            except Exception as exc:
                return self._contract_failure(
                    ctx,
                    snapshot,
                    logical_request,
                    compatibility,
                    provider_attempts,
                    provider_count,
                    str(exc),
                )
            round_id = f"info_round_{uuid4().hex}"
            pending = tuple(
                request.model_dump(mode="json")
                for request in bundle.information_requests
            )
            round_record = InformationRound(
                information_round_id=round_id,
                planner_request_id=logical_request.planner_request_id,
                round_number=logical_request.information_round_count + 1,
                status=InformationRoundStatus.REQUESTED,
                requests=pending,
                requested_at=completed,
            )
            updated_request = logical_request.model_copy(
                update={
                    "status": PlannerRequestStatus.AWAITING_INFORMATION,
                    "pending_information_requests": pending,
                    "provider_attempt_count": logical_request.provider_attempt_count,
                }
            )
            gaps = [
                gap.model_copy(
                    update={
                        "status": DecisionGapStatus.AWAITING_INFORMATION,
                        "logical_request_id": logical_request.planner_request_id,
                    }
                )
                for gap in self._request_gaps(logical_request)
            ]
            ctx.metrics.information_round_count += 1
            return self._finish(
                ctx,
                snapshot,
                InformationRequestedTick,
                compatibility=compatibility,
                decision_gaps=gaps,
                planner_request=updated_request,
                provider_attempts=provider_attempts,
                information_round=round_record,
                planner_request_id=logical_request.planner_request_id,
                information_round_id=round_id,
            )

        valid_bundle, validation = self._partition_bundle(
            bundle,
            provider_request,
            snapshot,
        )
        updated_request = logical_request.model_copy(
            update={
                "status": PlannerRequestStatus.COMPLETED,
                "completed_at": completed,
                "response_hash": hashlib.sha256(
                    json.dumps(
                        bundle.model_dump(mode="json"),
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                        default=str,
                    ).encode("utf-8")
                ).hexdigest(),
                "validation_result": validation,
                "provider_attempt_count": logical_request.provider_attempt_count,
            }
        )
        resolved_gaps, leases = self._resolve_gaps(
            logical_request,
            valid_bundle,
            validation,
            snapshot.turn,
        )
        needs_human = any(
            gap.status is DecisionGapStatus.AWAITING_HUMAN for gap in resolved_gaps
        )
        if needs_human:
            updated_request = updated_request.model_copy(
                update={
                    "status": PlannerRequestStatus.REJECTED,
                    "failure_category": "invalid_planner_output_item",
                }
            )
        if not needs_human and self._bundle_has_updates(valid_bundle):
            engine.store.save_plan_bundle(
                snapshot.game_id,
                snapshot.turn,
                valid_bundle,
                mode=engine.config.execution_mode,
                auto_action_types=engine.config.auto_action_types,
                observation_id=engine._active_observation_id,
            )
            compatibility.plan_id = valid_bundle.plan_id
        engine.store.mark_events_sent_to_agent(
            snapshot.game_id,
            [event.dedupe_key for event in trigger_events],
            snapshot.turn,
        )
        engine.store.record_agent_run(
            snapshot.game_id,
            provider_request,
            response=valid_bundle,
            success=True,
            error=None,
            duration_seconds=duration,
        )
        engine._clear_backoff()
        if needs_human:
            compatibility.paused = True
            compatibility.pause_reason = (
                "planner output did not establish a valid executable lease"
            )
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                compatibility=compatibility,
                decision_gaps=resolved_gaps,
                planner_request=updated_request,
                provider_attempts=provider_attempts,
                blocking_reason=compatibility.pause_reason,
            )
        result = self._finish(
            ctx,
            snapshot,
            PlannerAttemptCompletedTick,
            compatibility=compatibility,
            decision_gaps=resolved_gaps,
            plan_leases=leases,
            planner_request=updated_request,
            provider_attempts=provider_attempts,
            planner_request_id=logical_request.planner_request_id,
            provider_attempt_id=self._provider_tick_id(
                logical_request, provider_attempts
            ),
            provider_attempt_count=provider_count,
        )

        engine._checkpoint("after_provider_attempt_finalized")
        return result

    def _provider_failure(
        self,
        ctx,
        snapshot,
        logical_request,
        compatibility,
        provider_attempts,
        provider_count,
        error,
    ):
        engine = self.engine
        failure = engine._classify_planner_failure(error)
        transient = bool(failure["transient"])
        status = (
            PlannerRequestStatus.BACKOFF if transient else PlannerRequestStatus.FAILED
        )
        updated_request = logical_request.model_copy(
            update={
                "status": status,
                "provider_attempt_count": logical_request.provider_attempt_count,
                "failure_category": str(failure["category"]),
                "completed_at": None if transient else engine._now(),
            }
        )
        if transient:
            engine._set_backoff(failure)
            gaps = self._request_gaps(logical_request)
        else:
            gaps = [
                gap.model_copy(
                    update={
                        "status": DecisionGapStatus.AWAITING_HUMAN,
                        "logical_request_id": logical_request.planner_request_id,
                        "resolution_reason": (f"planner failed: {failure['category']}"),
                    }
                )
                for gap in self._request_gaps(logical_request)
            ]
        if not transient:
            compatibility.paused = True
            compatibility.pause_reason = f"planner failed: {failure['category']}"
            return self._finish(
                ctx,
                snapshot,
                AwaitingHumanTick,
                compatibility=compatibility,
                decision_gaps=gaps,
                planner_request=updated_request,
                provider_attempts=provider_attempts,
                blocking_reason=compatibility.pause_reason,
            )
        return self._finish(
            ctx,
            snapshot,
            PlannerAttemptCompletedTick,
            compatibility=compatibility,
            decision_gaps=gaps,
            planner_request=updated_request,
            provider_attempts=provider_attempts,
            planner_request_id=logical_request.planner_request_id,
            provider_attempt_id=self._provider_tick_id(
                logical_request, provider_attempts
            ),
            provider_attempt_count=provider_count,
        )

    def _contract_failure(
        self,
        ctx,
        snapshot,
        logical_request,
        compatibility,
        provider_attempts,
        provider_count,
        reason,
    ):
        updated_request = logical_request.model_copy(
            update={
                "status": PlannerRequestStatus.REJECTED,
                "completed_at": self.engine._now(),
                "failure_category": "planner_contract_failure",
                "provider_attempt_count": logical_request.provider_attempt_count,
            }
        )
        gaps = [
            gap.model_copy(
                update={
                    "status": DecisionGapStatus.AWAITING_HUMAN,
                    "logical_request_id": logical_request.planner_request_id,
                    "resolution_reason": (f"planner contract rejected: {reason[:300]}"),
                }
            )
            for gap in self._request_gaps(logical_request)
        ]
        compatibility.paused = True
        compatibility.pause_reason = f"planner contract rejected: {reason[:300]}"
        return self._finish(
            ctx,
            snapshot,
            AwaitingHumanTick,
            compatibility=compatibility,
            decision_gaps=gaps,
            planner_request=updated_request,
            provider_attempts=provider_attempts,
            blocking_reason=compatibility.pause_reason,
        )

    def _partition_bundle(self, bundle, request, snapshot):
        engine = self.engine
        valid_tasks = []
        invalid_tasks: dict[str, str] = {}
        context = PlanValidationContext(
            current_turn=snapshot.turn,
            allowed_action_types=engine.config.allowed_action_types,
            known_entities=extract_known_entities(snapshot),
            max_tasks=int(request.constraints.get("max_tasks", 8)),
        )
        for task in bundle.tasks:
            candidate = bundle.model_copy(
                update={
                    "tasks": [task],
                    "cancel_task_ids": [
                        task_id
                        for task_id in bundle.cancel_task_ids
                        if task_id != task.task_id
                    ],
                    "information_requests": [],
                    "event_resolutions": [],
                }
            )
            try:
                validate_plan_bundle(candidate, context)
            except Exception as exc:
                invalid_tasks[task.task_id] = str(exc)
            else:
                valid_tasks.append(task)
        valid_ids = {task.task_id for task in valid_tasks}
        valid_resolutions = []
        invalid_resolution_gaps: set[str] = set()
        for resolution in bundle.event_resolutions:
            if resolution.disposition is ResolutionDisposition.TASK and (
                set(resolution.task_ids) - valid_ids
            ):
                invalid_resolution_gaps.update(resolution.decision_gap_ids)
                continue
            valid_resolutions.append(resolution)
        valid_bundle = bundle.model_copy(
            update={
                "tasks": valid_tasks,
                "information_requests": [],
                "event_resolutions": valid_resolutions,
            }
        )
        return valid_bundle, {
            "valid_task_ids": sorted(valid_ids),
            "invalid_tasks": invalid_tasks,
            "invalid_resolution_gap_ids": sorted(invalid_resolution_gaps),
            "independent_validation": True,
        }

    def _resolve_gaps(self, logical_request, bundle, validation, turn):
        resolutions_by_gap = {}
        for resolution in bundle.event_resolutions:
            for gap_id in resolution.decision_gap_ids:
                if gap_id in logical_request.decision_gap_ids:
                    resolutions_by_gap[gap_id] = resolution
        invalid_gap_ids = set(validation["invalid_resolution_gap_ids"])
        resolved: list[DecisionGap] = []
        leases: list[PlanLease] = []
        for gap in self._request_gaps(logical_request):
            resolution = resolutions_by_gap.get(gap.decision_gap_id)
            if gap.decision_gap_id in invalid_gap_ids or resolution is None:
                resolved.append(
                    gap.model_copy(
                        update={
                            "status": DecisionGapStatus.AWAITING_HUMAN,
                            "logical_request_id": logical_request.planner_request_id,
                            "resolution_reason": (
                                "planner output item was missing or invalid"
                            ),
                        }
                    )
                )
                continue
            if resolution.disposition is ResolutionDisposition.HUMAN_REVIEW:
                resolved.append(
                    gap.model_copy(
                        update={
                            "status": DecisionGapStatus.AWAITING_HUMAN,
                            "logical_request_id": logical_request.planner_request_id,
                            "resolution_reason": resolution.reason,
                        }
                    )
                )
                continue
            if (
                resolution.disposition
                not in {
                    ResolutionDisposition.TASK,
                    ResolutionDisposition.PLAN_UPDATE,
                }
                or resolution.lease_contract is None
            ):
                resolved.append(
                    gap.model_copy(
                        update={
                            "status": DecisionGapStatus.AWAITING_HUMAN,
                            "logical_request_id": logical_request.planner_request_id,
                            "resolution_reason": (
                                "planner output lacks a valid lease contract"
                            ),
                        }
                    )
                )
                continue
            completed_gap = gap.model_copy(
                update={
                    "status": DecisionGapStatus.RESOLVED,
                    "logical_request_id": logical_request.planner_request_id,
                    "resolution_reason": resolution.reason,
                }
            )
            try:
                lease = self._lease_for_resolution(
                    completed_gap,
                    bundle,
                    logical_request,
                    turn,
                    resolution,
                )
            except (KeyError, TypeError, ValueError) as exc:
                resolved.append(
                    gap.model_copy(
                        update={
                            "status": DecisionGapStatus.AWAITING_HUMAN,
                            "logical_request_id": logical_request.planner_request_id,
                            "resolution_reason": (
                                f"planner lease contract rejected: {str(exc)[:240]}"
                            ),
                        }
                    )
                )
                continue
            resolved.append(completed_gap)
            leases.append(lease)
        return resolved, leases

    def _lease_for_resolution(self, gap, bundle, logical_request, turn, resolution):
        contract = resolution.lease_contract
        if contract is None:
            raise ValueError("resolution has no lease contract")
        if contract.approval_status not in {
            ApprovalStatus.NOT_REQUIRED,
            ApprovalStatus.APPROVED,
        }:
            raise ValueError("lease contract approval is not satisfied")
        engine = self.engine
        existing = [
            lease
            for lease in engine.store.list_plan_leases(gap.game_session_id)
            if lease.scope == gap.scope
        ]
        revision = 1 + max(
            (lease.plan_revision for lease in existing),
            default=0,
        )
        contract_subjects = tuple(
            SubjectRef(
                subject_type=str(item["subject_type"]),
                subject_id=str(item["subject_id"]),
            )
            for item in contract.subjects
        )
        return PlanLease(
            plan_lease_id=f"lease_{uuid4().hex}",
            plan_id=bundle.plan_id,
            game_session_id=gap.game_session_id,
            decision_gap_ids=(gap.decision_gap_id,),
            scope=gap.scope,
            subjects=contract_subjects or gap.subjects,
            covered_slots=tuple(sorted(set(contract.covered_slots))),
            task_ids=tuple(sorted(set(resolution.task_ids))),
            plan_revision=revision,
            source_planner_request_id=logical_request.planner_request_id,
            created_from_observation_id=logical_request.observation_id,
            status=PlanLeaseStatus.ACTIVE,
            approval_status=contract.approval_status,
            valid_from_turn=turn,
            valid_until_turn=contract.valid_until_turn,
            preconditions=tuple(
                self._contract_condition(item) for item in contract.preconditions
            ),
            completion_condition=self._contract_condition(
                contract.completion_condition
            ),
            invalidation_conditions=tuple(
                self._contract_condition(item)
                for item in contract.invalidation_conditions
            ),
            review_conditions=tuple(
                self._contract_condition(item) for item in contract.review_conditions
            ),
            continuation_policy=contract.continuation_policy,
            relevant_input_hash=gap.relevant_input_hash,
            last_validated_observation_id=logical_request.observation_id,
            last_validation_result=LeaseValidationResult.VALID,
        )

    @staticmethod
    def _contract_condition(payload):
        values = dict(payload)
        condition_type = str(values.pop("type"))
        expected = values.pop("expected", values.pop("value", True))
        entity_type = values.pop("entity_type", None)
        entity_id = values.pop("entity_id", None)
        subject = (
            None
            if entity_type is None or entity_id is None
            else SubjectRef(
                subject_type=str(entity_type),
                subject_id=str(entity_id),
            )
        )
        return Condition(
            condition_type=condition_type,
            subject=subject,
            parameters=values,
            expected=expected,
        )

    def _request_gaps(self, logical_request):
        return [
            gap
            for gap_id in logical_request.decision_gap_ids
            if (
                gap := self.engine.store.get_decision_gap(
                    logical_request.game_session_id, gap_id
                )
            )
            is not None
        ]

    @staticmethod
    def _bundle_has_updates(bundle) -> bool:
        return bool(
            bundle.tasks
            or bundle.cancel_task_ids
            or bundle.strategy_updates
            or bundle.city_plan_updates
            or bundle.unit_plan_updates
            or bundle.builder_plan_updates
        )

    @staticmethod
    def _json_diagnostics(value):
        if not isinstance(value, dict):
            return {}
        return json.loads(json.dumps(value, default=str))

    @staticmethod
    def _provider_attempt_count(diagnostics):
        if "attempt_count" in diagnostics:
            return max(0, int(diagnostics["attempt_count"]))
        return 1

    @staticmethod
    def _provider_attempt_records(
        logical_request,
        provider_request_id,
        started,
        completed,
        duration,
        diagnostics,
        count,
        previous_count,
        *,
        success,
    ):
        records = []
        for offset in range(count):
            final = offset == count - 1
            status = (
                ProviderAttemptStatus.SUCCEEDED
                if success and final
                else ProviderAttemptStatus.FAILED
            )
            records.append(
                ProviderAttempt(
                    provider_attempt_id=f"provider_{uuid4().hex}",
                    planner_request_id=logical_request.planner_request_id,
                    attempt_number=previous_count + offset + 1,
                    provider_request_id=(
                        provider_request_id
                        if count == 1
                        else f"{provider_request_id}:{offset + 1}"
                    ),
                    status=status,
                    started_at=started,
                    completed_at=completed,
                    latency_seconds=duration,
                    diagnostics=diagnostics,
                    failure_category=(
                        None
                        if status is ProviderAttemptStatus.SUCCEEDED
                        else "provider_retry_or_failure"
                    ),
                )
            )
        return records

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
        decision_gaps=(),
        decision_group=None,
        plan_leases=(),
        planner_request=None,
        provider_attempts=(),
        information_round=None,
        cancel_task_ids=(),
        **fields,
    ):
        engine = self.engine
        completed = engine._now()
        ctx.metrics.mcp_call_count = engine.game.call_count - ctx.call_count_before
        ctx.metrics.mutation_count = ctx.budget.used
        ctx.metrics.total_seconds = engine._monotonic() - ctx.started_monotonic
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
        engine.store.persist_phase4_tick(
            tick,
            decision_gaps=decision_gaps,
            decision_group=decision_group,
            plan_leases=plan_leases,
            planner_request=planner_request,
            provider_attempts=provider_attempts,
            information_round=information_round,
            cancel_task_ids=cancel_task_ids,
        )
        result = compatibility or TickResult(turn=snapshot.turn, metrics=ctx.metrics)
        result.metrics = ctx.metrics
        result.tick_id = tick.tick_id
        result.runtime_state = tick.ending_runtime_state.value
        result.workflow_tick = tick.model_dump(mode="json")
        if planner_request is not None:
            result.planner_request_id = planner_request.planner_request_id
        return result
