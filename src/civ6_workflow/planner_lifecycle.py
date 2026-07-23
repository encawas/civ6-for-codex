"""Persistent planner lifecycle used by the canonical bounded workflow engine."""

from __future__ import annotations

import json
import time
from contextlib import nullcontext
from typing import Any
from uuid import uuid4

from .conditions import extract_known_entities, find_entity
from .domain.observations import SlotState
from .domain.base import thaw_json
from .decisioning import (
    TURN_SPECIFIC_GAP_TYPES,
    SETTLER_GAP_TYPES,
    STRATEGIC_GAP_TYPES,
    batch_compatible_gaps,
    build_decision_gap,
    hash_decision_input,
    evaluate_plan_lease,
    evaluate_planner_eligibility,
    opening_decision_events,
    stable_decision_identity,
)
from .domain import (
    ApprovalStatus,
    ApprovalDecision,
    AwaitingApprovalTick,
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
    PlannerRequestTarget,
    PlannerRequestTargetKind,
    ProviderAttempt,
    ProviderAttemptStatus,
    RuntimeState,
    SubjectRef,
    validate_workflow_tick,
    canonical_json,
    canonical_json_hash,
)
from .models import (
    EventLevel,
    ExecutionMode,
    GameEvent,
    RiskLevel,
    TickResult,
)
from .validation import PlanValidationContext, validate_plan_bundle
from .workflow_protocol import (
    InformationRequest,
    WorkflowAgentRequest as AgentRequest,
    ResolutionDisposition,
    WorkflowPlanBundle,
    validate_event_resolution_contract,
    validate_global_resolution_structure,
)


PLANNER_CALL_POLICY_REVISION = "planner-call-policy/v1"
PLANNER_INPUT_CONTRACT_REVISION = "planner-input-contract/v2"
PLANNER_REQUEST_POLICY_REVISION = (
    f"{PLANNER_CALL_POLICY_REVISION}+{PLANNER_INPUT_CONTRACT_REVISION}"
)


class PlannerLifecycleCoordinator:
    """Advance durable planning state without owning the workflow Tick loop."""

    def __init__(self, engine: Any):
        self.engine = engine

    def validate_before_routing(self, ctx, observation, current_events, compatibility):
        if (
            ctx.starting_state
            in {
                RuntimeState.AWAITING_HUMAN,
                RuntimeState.TURN_TRANSITIONING,
                RuntimeState.VERIFYING,
            }
            and not ctx.resuming_human_wait
        ):
            return None
        gap_tick = self._reconcile_persisted_gaps(
            ctx, observation, compatibility, current_events=current_events
        )
        if gap_tick is not None:
            return gap_tick
        return self._validate_leases(
            ctx, observation, compatibility, current_events=current_events
        )

    async def advance(
        self, ctx, observation, agent_events, compatibility, *, current_events=None
    ):
        engine = self.engine
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

        active = engine.store.active_planner_request(game_id)
        if active is not None:
            if (
                active.target.kind
                is not PlannerRequestTargetKind.LEGACY_DECISION_GROUP
            ):
                reason = (
                    "non-legacy planner request routing is not enabled "
                    "before Phase 1B"
                )
                compatibility.paused = True
                compatibility.pause_reason = reason
                compatibility.planner_request_id = active.planner_request_id
                return [], self._finish(
                    ctx,
                    snapshot,
                    AwaitingHumanTick,
                    compatibility=compatibility,
                    blocking_reason=reason,
                )
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

        decision_events = list(
            current_events if current_events is not None else agent_events
        )
        if engine.config.max_agent_calls_per_turn > 0 and not any(
            event.event_type in STRATEGIC_GAP_TYPES for event in decision_events
        ):
            decision_events.extend(
                opening_decision_events(
                    observation,
                    existing_events=decision_events,
                )
            )
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

    def _reconcile_persisted_gaps(
        self,
        ctx,
        observation,
        compatibility,
        *,
        current_events,
    ):
        engine = self.engine
        snapshot = observation.snapshot
        active_request = engine.store.active_planner_request(snapshot.game_id)
        protected_gap_ids = (
            set(active_request.decision_gap_ids)
            if active_request is not None
            else set()
        )
        for lease in engine.store.list_plan_leases(snapshot.game_id):
            if lease.status in {
                PlanLeaseStatus.ACTIVE,
                PlanLeaseStatus.AWAITING_APPROVAL,
                PlanLeaseStatus.AWAITING_INFORMATION,
            }:
                protected_gap_ids.update(lease.decision_gap_ids)

        current_identities = set()
        for event in current_events:
            if event.event_type not in STRATEGIC_GAP_TYPES:
                continue
            try:
                identity, _ = stable_decision_identity(event)
            except ValueError:
                continue
            current_identities.add(identity)

        candidates = {
            DecisionGapStatus.OPEN,
            DecisionGapStatus.PLANNER_ELIGIBLE,
            DecisionGapStatus.REQUESTED,
            DecisionGapStatus.AWAITING_INFORMATION,
            DecisionGapStatus.PROPOSED,
        }
        if ctx.resuming_human_wait:
            candidates.add(DecisionGapStatus.AWAITING_HUMAN)

        updates = []
        cancel_task_ids: set[str] = set()
        for gap in engine.store.list_decision_gaps(snapshot.game_id):
            if gap.decision_gap_id in protected_gap_ids or gap.status not in candidates:
                continue
            projection = thaw_json(gap.input_projection)
            opening_resolution = self._opening_gap_resolution_reason(gap, observation)
            if opening_resolution is not None:
                status = DecisionGapStatus.RESOLVED
                resolution_reason = opening_resolution
                invalidation_reason = None
            elif gap.turn_specific and gap.identity_turn_number != snapshot.turn:
                status = DecisionGapStatus.INVALIDATED
                resolution_reason = None
                invalidation_reason = "turn-specific decision expired before routing"
            elif (
                projection.get("requires_current_event")
                and gap.stable_identity not in current_identities
            ):
                status = DecisionGapStatus.INVALIDATED
                resolution_reason = None
                invalidation_reason = (
                    "required strategic event disappeared before routing"
                )
            else:
                continue
            updates.append(
                gap.model_copy(
                    update={
                        "status": status,
                        "observation_id": (
                            engine._active_observation_id or gap.observation_id
                        ),
                        "logical_request_id": None,
                        "resolution_reason": resolution_reason,
                        "invalidation_reason": invalidation_reason,
                        "reopen_reason": None,
                        "updated_at": engine._now(),
                    }
                )
            )
            cancel_task_ids.update(self._dependent_task_ids_for_gap(gap))

        if not updates:
            return None
        first = updates[0]
        return self._finish(
            ctx,
            snapshot,
            DecisionGapUpdatedTick,
            compatibility=compatibility,
            decision_gaps=updates,
            cancel_task_ids=tuple(sorted(cancel_task_ids)),
            decision_gap_id=first.decision_gap_id,
            update_reason=(
                first.resolution_reason
                or first.invalidation_reason
                or "persisted decision gap was reconciled"
            ),
        )

    def _dependent_task_ids_for_gap(self, gap):
        subjects = {
            (subject.subject_type, subject.subject_id) for subject in gap.subjects
        }
        action_slots = {
            "city_set_production": "city_production",
            "set_research": "research",
            "set_civic": "civic",
            "unit_move": "unit_route",
            "unit_skip": "unit_route",
            "unit_found_city": "unit_route",
            "builder_improve": "builder_route",
        }
        scope_slot = {
            "research": "research",
            "civic": "civic",
        }.get(gap.scope.split(":", 1)[0])
        result = set()
        for task in self.engine.store.list_tasks(gap.game_session_id):
            subject_matches = (task.entity_type, str(task.entity_id)) in subjects
            slot_matches = (
                scope_slot is not None
                and action_slots.get(task.action_type) == scope_slot
            )
            if subject_matches or slot_matches:
                result.add(task.task_id)
        return tuple(sorted(result))

    def _validate_leases(self, ctx, observation, compatibility, *, current_events):
        engine = self.engine
        snapshot = observation.snapshot
        observation_id = engine._active_observation_id or ctx.observation_ids[-1]
        for lease in engine.store.list_plan_leases(snapshot.game_id):
            if lease.status is PlanLeaseStatus.AWAITING_APPROVAL:
                approval = engine.store.latest_approval_record(
                    snapshot.game_id,
                    proposal_type="decision_gap",
                    proposal_id=lease.decision_gap_ids[0],
                    proposal_revision=lease.plan_revision,
                )
                if approval is None or approval.decision not in {
                    ApprovalDecision.APPROVED,
                    ApprovalDecision.EDITED_AND_APPROVED,
                }:
                    compatibility.paused = True
                    compatibility.pause_reason = "plan lease approval is required"
                    return self._finish(
                        ctx,
                        snapshot,
                        AwaitingApprovalTick,
                        compatibility=compatibility,
                        plan_leases=[lease],
                        proposal_id=lease.decision_gap_ids[0],
                        blocking_reason=compatibility.pause_reason,
                    )
                activated = lease.model_copy(
                    update={
                        "status": PlanLeaseStatus.ACTIVE,
                        "approval_status": ApprovalStatus.APPROVED,
                        "last_validated_observation_id": observation_id,
                    }
                )
                gap_updates = []
                gap = engine.store.get_decision_gap(
                    snapshot.game_id, lease.decision_gap_ids[0]
                )
                if gap is not None:
                    gap_updates = [
                        gap.model_copy(
                            update={
                                "status": DecisionGapStatus.RESOLVED,
                                "resolution_reason": "durable approval activated lease",
                            }
                        )
                    ]
                return self._finish(
                    ctx,
                    snapshot,
                    PlanLeaseUpdatedTick,
                    compatibility=compatibility,
                    decision_gaps=gap_updates,
                    plan_leases=[activated],
                    plan_lease_id=lease.plan_lease_id,
                    validation_result=LeaseValidationResult.VALID.value,
                )
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
            evaluation = evaluate_plan_lease(
                lease,
                observation,
                relevant_input_hash=(
                    lease.relevant_input_hash
                    if current_gap is None
                    else current_gap.relevant_input_hash
                ),
                evaluator=engine.conditions,
                relevant_input_projection=(
                    None
                    if current_gap is None
                    else thaw_json(current_gap.input_projection)
                ),
            )
            evaluated_lease = evaluation.lease
            result = evaluation.result
            reason = evaluation.reason

            if (
                current_gap is None
                and evaluated_lease.status is not PlanLeaseStatus.COMPLETED
            ):
                if gap is not None and gap.gap_type in TURN_SPECIFIC_GAP_TYPES:
                    reason = "turn-specific decision is no longer current"
                    evaluated_lease = lease.model_copy(
                        update={
                            "status": PlanLeaseStatus.EXPIRED,
                            "last_validated_observation_id": observation_id,
                            "last_validation_result": LeaseValidationResult.EXPIRED,
                        }
                    )
                    result = LeaseValidationResult.EXPIRED
                else:
                    reason = "one-shot event disappeared without completion evidence"
                    evaluated_lease = lease.model_copy(
                        update={
                            "status": PlanLeaseStatus.AWAITING_INFORMATION,
                            "last_validated_observation_id": observation_id,
                            "last_validation_result": LeaseValidationResult.UNKNOWN,
                        }
                    )
                    result = LeaseValidationResult.UNKNOWN

            input_changed = (
                current_gap is not None
                and gap is not None
                and current_gap.relevant_input_hash != gap.relevant_input_hash
            )
            material = (
                current_gap is None
                or input_changed
                or evaluated_lease.status is not lease.status
                or evaluated_lease.valid_until_turn != lease.valid_until_turn
                or evaluated_lease.last_validation_result
                is not lease.last_validation_result
            )
            if not material:
                continue

            gap_update = current_gap or gap
            gap_updates = []
            cancel_task_ids: tuple[str, ...] = ()
            if gap_update is not None:
                if (
                    gap is not None
                    and gap.gap_type in TURN_SPECIFIC_GAP_TYPES
                    and current_gap is None
                ):
                    gap_update = gap.model_copy(
                        update={
                            "status": DecisionGapStatus.INVALIDATED,
                            "logical_request_id": None,
                            "resolution_reason": None,
                            "invalidation_reason": reason,
                            "reopen_reason": None,
                        }
                    )
                    cancel_task_ids = self._dependent_task_ids(lease)
                elif evaluated_lease.status is PlanLeaseStatus.COMPLETED:
                    gap_update = gap_update.model_copy(
                        update={
                            "status": DecisionGapStatus.RESOLVED,
                            "observation_id": observation_id,
                            "resolution_reason": reason,
                            "invalidation_reason": None,
                            "reopen_reason": None,
                        }
                    )
                    cancel_task_ids = self._dependent_task_ids(lease)
                elif result in {
                    LeaseValidationResult.EXPIRED,
                    LeaseValidationResult.INVALIDATED,
                    LeaseValidationResult.UNKNOWN,
                }:
                    gap_update = gap_update.model_copy(
                        update={
                            "status": DecisionGapStatus.OPEN,
                            "logical_request_id": None,
                            "reopen_reason": reason,
                            "resolution_reason": None,
                            "invalidation_reason": None,
                        }
                    )
                    cancel_task_ids = self._dependent_task_ids(lease)
                else:
                    gap_update = gap_update.model_copy(
                        update={
                            "status": gap.status,
                            "route": gap.route,
                            "logical_request_id": gap.logical_request_id,
                            "resolution_reason": gap.resolution_reason,
                            "invalidation_reason": gap.invalidation_reason,
                            "reopen_reason": gap.reopen_reason,
                        }
                    )
                gap_updates = [gap_update]
            elif evaluated_lease.status is not PlanLeaseStatus.ACTIVE:
                cancel_task_ids = self._dependent_task_ids(lease)

            if result is LeaseValidationResult.UNKNOWN:
                compatibility.paused = True
                compatibility.pause_reason = reason
                return self._finish(
                    ctx,
                    snapshot,
                    AwaitingHumanTick,
                    compatibility=compatibility,
                    decision_gaps=gap_updates,
                    plan_leases=[evaluated_lease],
                    cancel_task_ids=cancel_task_ids,
                    blocking_reason=reason,
                )
            return self._finish(
                ctx,
                snapshot,
                PlanLeaseUpdatedTick,
                compatibility=compatibility,
                decision_gaps=gap_updates,
                plan_leases=[evaluated_lease],
                cancel_task_ids=cancel_task_ids,
                plan_lease_id=lease.plan_lease_id,
                validation_result=result.value,
            )
        return None

    def _dependent_task_ids(self, lease):
        subjects = {
            (subject.subject_type, subject.subject_id) for subject in lease.subjects
        }
        action_slots = {
            "city_set_production": "city_production",
            "set_research": "research",
            "set_civic": "civic",
            "unit_move": "unit_route",
            "unit_skip": "unit_route",
            "unit_found_city": "unit_route",
            "builder_improve": "builder_route",
        }
        result = set(lease.task_ids)
        for task in self.engine.store.list_tasks(lease.game_session_id):
            subject_matches = (task.entity_type, str(task.entity_id)) in subjects
            slot = action_slots.get(task.action_type)
            slot_matches = not lease.covered_slots or slot in lease.covered_slots
            if subject_matches and slot_matches:
                result.add(task.task_id)
            elif not subjects and slot is not None and slot in lease.covered_slots:
                result.add(task.task_id)
        return tuple(sorted(result))

    @staticmethod
    def _opening_gap_resolution_reason(gap, observation):
        if gap.gap_type == "research_direction_required":
            if (
                observation.canonical.progression.current_research.state
                is SlotState.OCCUPIED
            ):
                return "research slot was filled outside the workflow"
            return None

        if gap.gap_type == "city_role_required":
            subject_id = next(
                (
                    subject.subject_id
                    for subject in gap.subjects
                    if subject.subject_type == "city"
                ),
                None,
            )
            city = (
                None if subject_id is None else observation.canonical.city(subject_id)
            )
            if city is not None and city.production.state is SlotState.OCCUPIED:
                return "city production slot was filled outside the workflow"
        return None

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

        resolution_reason = self._opening_gap_resolution_reason(gap, observation)
        if resolution_reason is not None:
            return None

        projection = thaw_json(gap.input_projection)
        if gap.gap_type in TURN_SPECIFIC_GAP_TYPES and (
            matching is None or snapshot.turn != gap.identity_turn_number
        ):
            return None
        if matching is None and projection.get("requires_current_event"):
            return None
        if matching is None:
            subject = projection.get("subject", {})
            payload = dict(projection.get("event_facts", {}))
            if gap.gap_type in {
                "settler_site_selection_required",
                "settler_plan_requires_review",
            }:
                unit = find_entity(
                    snapshot.units, ("unit_id", "id"), str(subject.get("id"))
                )
                current_targets = (
                    unit.get("targets", []) if isinstance(unit, dict) else []
                )
                payload["available_targets"] = current_targets
                if isinstance(unit, dict):
                    payload.update(
                        {
                            key: unit[key]
                            for key in (
                                "path_reachable",
                                "path_status",
                                "route_legal",
                                "target_legal",
                            )
                            if key in unit
                        }
                    )
            matching = GameEvent(
                event_type=gap.gap_type,
                turn=snapshot.turn,
                entity_type=subject.get("type"),
                entity_id=subject.get("id"),
                level=EventLevel.L3,
                risk=(
                    RiskLevel.HIGH
                    if projection.get("approval", {}).get("required")
                    else RiskLevel.LOW
                ),
                blocking=True,
                payload=payload,
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
        projection = thaw_json(request.input_projection)
        reason = None
        contract_revision_migration = False
        refreshed = []
        if len(gaps) != len(request.decision_gap_ids):
            reason = "source decision gap no longer exists"
        else:
            for gap in gaps:
                current = self._rebuild_current_gap(gap, observation, current_events)
                if current is None:
                    reason = (
                        self._opening_gap_resolution_reason(gap, observation)
                        or "source strategic event is no longer active"
                    )
                    break
                refreshed.append(current)

        if reason is None:
            group = batch_compatible_gaps(
                snapshot.game_id,
                engine._active_observation_id or request.observation_id,
                refreshed,
                now=engine._now(),
            )
            if request.policy_revision != PLANNER_REQUEST_POLICY_REVISION:
                reason = "planner request policy revision changed"
                contract_revision_migration = (
                    projection.get("planner_input_contract_revision")
                    != PLANNER_INPUT_CONTRACT_REVISION
                )
            elif group.decision_group_id != request.decision_group_id:
                reason = "stable decision identity was replaced"
            elif self._planner_input_hash(
                group.input_projection_hash
            ) != request.input_projection_hash:
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
                "failure_category": (
                    "planner_contract_revision_migration"
                    if contract_revision_migration
                    else "stale_planning_input"
                ),
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
        opening_resolution_reasons = {
            gap.decision_gap_id: resolution_reason
            for gap in gaps
            if (
                resolution_reason := self._opening_gap_resolution_reason(
                    gap, observation
                )
            )
            is not None
        }
        reopened = []
        if opening_resolution_reasons:
            for gap in gaps:
                resolution_reason = opening_resolution_reasons.get(gap.decision_gap_id)
                if resolution_reason is not None:
                    update = {
                        "status": DecisionGapStatus.RESOLVED,
                        "logical_request_id": None,
                        "reopen_reason": None,
                        "resolution_reason": resolution_reason,
                        "invalidation_reason": None,
                    }
                else:
                    update = {
                        "status": DecisionGapStatus.OPEN,
                        "logical_request_id": None,
                        "reopen_reason": reason,
                        "resolution_reason": None,
                        "invalidation_reason": None,
                    }
                reopened.append(gap.model_copy(update=update))
        else:
            source = refreshed or gaps
            for gap in source:
                turn_specific_expired = (
                    gap.gap_type in TURN_SPECIFIC_GAP_TYPES
                    and snapshot.turn != gap.identity_turn_number
                )
                reopened.append(
                    gap.model_copy(
                        update={
                            "status": (
                                DecisionGapStatus.INVALIDATED
                                if turn_specific_expired
                                else DecisionGapStatus.OPEN
                            ),
                            "logical_request_id": None,
                            "reopen_reason": (
                                None if turn_specific_expired else reason
                            ),
                            "resolution_reason": None,
                            "invalidation_reason": (
                                reason if turn_specific_expired else None
                            ),
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
        return canonical_json_hash(value)

    @classmethod
    def _planner_input_hash(
        cls,
        decision_input_hash: str,
        planner_request_policy_revision: str = PLANNER_REQUEST_POLICY_REVISION,
    ) -> str:
        return cls._contract_hash(
            {
                "decision_input_hash": decision_input_hash,
                "planner_request_policy_revision": planner_request_policy_revision,
            }
        )

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
            runtime_state=(
                RuntimeState.ROUTING.value
                if ctx.resuming_human_wait
                else ctx.starting_state.value
            ),
            has_ready_deterministic_task=bool(
                engine.store.due_tasks(snapshot.game_id, snapshot.turn)
            ),
            active_attempt=(
                engine.store.unresolved_action_attempt(snapshot.game_id) is not None
            ),
            logical_requests_this_turn=engine.store.provider_budget_request_count_for_turn(
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
        target = PlannerRequestTarget(
            kind=PlannerRequestTargetKind.LEGACY_DECISION_GROUP,
            decision_group_id=group.decision_group_id,
            decision_gap_ids=group.decision_gap_ids,
        )
        planner_input_hash = self._planner_input_hash(group.input_projection_hash)
        duplicate = engine.store.planner_request_for_input(
            snapshot.game_id,
            target.target_key,
            planner_input_hash,
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
            "planner_call_policy_revision": PLANNER_CALL_POLICY_REVISION,
            "planner_input_contract_revision": PLANNER_INPUT_CONTRACT_REVISION,
            "planner_request_policy_revision": PLANNER_REQUEST_POLICY_REVISION,
            "decision_group_id": group.decision_group_id,
            "gaps": [thaw_json(gap.input_projection) for gap in eligibility.gaps],
        }
        request_payload = provider_request.model_dump(mode="json")
        context_bytes = len(canonical_json(request_payload).encode("utf-8"))
        logical_request = PlannerRequest(
            planner_request_id=logical_id,
            game_session_id=snapshot.game_id,
            turn_number=snapshot.turn,
            observation_id=observation_id,
            target=target,
            input_projection_hash=planner_input_hash,
            input_projection=request_projection,
            request_payload=request_payload,
            plan_revision_refs=tuple(
                revision
                for gap in eligibility.gaps
                for revision in gap.relevant_plan_revisions
            ),
            policy_revision=PLANNER_REQUEST_POLICY_REVISION,
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
            request_target_kind=target.kind,
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
        observation_id = engine._active_observation_id or ctx.observation_ids[-1]
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
        contract_error: Exception | None = None
        planner_scope = getattr(engine.planner, "logical_request_scope", None)
        scope = (
            planner_scope(logical_request.planner_request_id)
            if callable(planner_scope)
            else nullcontext()
        )
        try:
            with scope:
                raw_bundle = await engine._plan_once(provider_request, ctx.metrics)
        except Exception as exc:
            from .engine import InjectedCrashBoundary

            if isinstance(exc, InjectedCrashBoundary):
                raise
            error = exc
        else:
            try:
                bundle = WorkflowPlanBundle.model_validate(
                    raw_bundle.model_dump(mode="python")
                )
            except Exception as exc:
                contract_error = exc
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
        if contract_error is not None:
            return self._contract_failure(
                ctx,
                snapshot,
                logical_request,
                compatibility,
                provider_attempts,
                provider_count,
                str(contract_error),
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
        resolved_gaps, leases = self._resolve_gaps(
            logical_request,
            valid_bundle,
            validation,
            snapshot.turn,
            observation,
        )
        successful_gap_ids = {
            gap_id for lease in leases for gap_id in lease.decision_gap_ids
        }
        valid_subset = self._subset_for_resolved_gaps(valid_bundle, successful_gap_ids)
        resolved_gaps, leases = self._refresh_effective_lease_baselines(
            snapshot,
            provider_request,
            resolved_gaps,
            leases,
            valid_subset,
        )
        human_gaps = [
            gap
            for gap in resolved_gaps
            if gap.status is DecisionGapStatus.AWAITING_HUMAN
        ]
        active_leases = [
            lease for lease in leases if lease.status is PlanLeaseStatus.ACTIVE
        ]
        pending_approval = [
            lease
            for lease in leases
            if lease.status is PlanLeaseStatus.AWAITING_APPROVAL
        ]
        if human_gaps and leases:
            request_status = PlannerRequestStatus.PARTIALLY_COMPLETED
            validation = {
                **validation,
                "result": "partially_completed",
                "successful_gap_ids": sorted(successful_gap_ids),
                "human_gap_ids": sorted(gap.decision_gap_id for gap in human_gaps),
            }
        elif human_gaps:
            request_status = PlannerRequestStatus.REJECTED
            validation = {**validation, "result": "rejected"}
        else:
            request_status = PlannerRequestStatus.COMPLETED
            validation = {
                **validation,
                "result": "completed",
                "successful_gap_ids": sorted(successful_gap_ids),
            }
        response_payload = (
            bundle.model_dump(mode="json")
            if request_status
            in {
                PlannerRequestStatus.COMPLETED,
                PlannerRequestStatus.PARTIALLY_COMPLETED,
            }
            else None
        )
        updated_request = logical_request.model_copy(
            update={
                "status": request_status,
                "completed_at": completed,
                "response_payload": response_payload,
                "response_hash": (
                    None
                    if response_payload is None
                    else canonical_json_hash(response_payload)
                ),
                "validation_result": validation,
                "provider_attempt_count": logical_request.provider_attempt_count,
                "failure_category": (
                    "invalid_planner_output_item" if human_gaps else None
                ),
            }
        )
        if self._bundle_has_updates(valid_subset):
            compatibility.plan_id = valid_subset.plan_id
        engine.store.mark_events_sent_to_agent(
            snapshot.game_id,
            [event.dedupe_key for event in trigger_events],
            snapshot.turn,
        )
        engine.store.record_agent_run(
            snapshot.game_id,
            provider_request,
            response=valid_subset,
            success=True,
            error=None,
            duration_seconds=duration,
        )
        engine._clear_backoff()
        if human_gaps and not leases:
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
        if pending_approval and not active_leases and not human_gaps:
            compatibility.paused = True
            compatibility.pause_reason = "plan lease approval is required"
            return self._finish(
                ctx,
                snapshot,
                AwaitingApprovalTick,
                compatibility=compatibility,
                decision_gaps=resolved_gaps,
                plan_leases=leases,
                planner_request=updated_request,
                provider_attempts=provider_attempts,
                plan_bundle=valid_subset,
                proposal_id=pending_approval[0].decision_gap_ids[0],
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
            plan_bundle=valid_subset,
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
        request_gap_ids = {
            str(gap_id) for gap_id in request.constraints.get("decision_gap_ids", ())
        } or {
            str(gap_id)
            for resolution in bundle.event_resolutions
            for gap_id in resolution.decision_gap_ids
        }
        try:
            validate_global_resolution_structure(
                bundle,
                request.trigger_events,
                required_gap_ids=request_gap_ids,
            )
        except Exception as exc:
            empty_bundle = bundle.model_copy(
                update={
                    "strategy_updates": {},
                    "city_plan_updates": [],
                    "unit_plan_updates": [],
                    "builder_plan_updates": [],
                    "tasks": [],
                    "cancel_task_ids": [],
                    "information_requests": [],
                    "event_resolutions": [],
                }
            )
            reason = str(exc)
            return empty_bundle, {
                "valid_task_ids": [],
                "invalid_tasks": {},
                "invalid_resolution_gap_ids": sorted(request_gap_ids),
                "resolution_errors": {gap_id: reason for gap_id in request_gap_ids},
                "independent_validation": False,
                "global_structure_error": reason,
            }
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
        resolution_errors: dict[str, str] = {}
        trigger_by_key = {
            str(event.dedupe_key): event for event in request.trigger_events
        }
        for resolution in bundle.event_resolutions:
            if resolution.disposition is ResolutionDisposition.TASK and (
                set(resolution.task_ids) - valid_ids
            ):
                invalid_resolution_gaps.update(resolution.decision_gap_ids)
                for gap_id in resolution.decision_gap_ids:
                    resolution_errors[gap_id] = "resolution references an invalid task"
                continue
            event = trigger_by_key.get(resolution.event_dedupe_key)
            candidate = bundle.model_copy(
                update={
                    "tasks": [
                        task
                        for task in valid_tasks
                        if task.task_id in set(resolution.task_ids)
                    ],
                    "cancel_task_ids": [],
                    "information_requests": [],
                    "event_resolutions": [resolution],
                    "requires_human_review": (
                        resolution.disposition is ResolutionDisposition.HUMAN_REVIEW
                    ),
                }
            )
            try:
                validate_event_resolution_contract(
                    candidate,
                    [] if event is None else [event],
                    known_task_ids=set(),
                    allow_information_requests=False,
                )
            except Exception as exc:
                invalid_resolution_gaps.update(resolution.decision_gap_ids)
                for gap_id in resolution.decision_gap_ids:
                    resolution_errors[gap_id] = str(exc)
            else:
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
            "resolution_errors": resolution_errors,
            "independent_validation": True,
        }

    @staticmethod
    def _subset_for_resolved_gaps(bundle, successful_gap_ids):
        resolutions = [
            resolution
            for resolution in bundle.event_resolutions
            if set(resolution.decision_gap_ids) & set(successful_gap_ids)
        ]
        task_ids = {
            task_id for resolution in resolutions for task_id in resolution.task_ids
        }
        plan_refs = {
            plan_ref for resolution in resolutions for plan_ref in resolution.plan_refs
        }
        return bundle.model_copy(
            update={
                "strategy_updates": (
                    bundle.strategy_updates if "strategy" in plan_refs else {}
                ),
                "city_plan_updates": [
                    row
                    for row in bundle.city_plan_updates
                    if f"city:{row.get('city_id')}" in plan_refs
                ],
                "unit_plan_updates": [
                    row
                    for row in bundle.unit_plan_updates
                    if f"unit:{row.get('unit_id')}" in plan_refs
                ],
                "builder_plan_updates": [
                    row
                    for row in bundle.builder_plan_updates
                    if f"builder:{row.get('builder_key')}" in plan_refs
                ],
                "tasks": [task for task in bundle.tasks if task.task_id in task_ids],
                "cancel_task_ids": [],
                "event_resolutions": resolutions,
                "requires_human_review": False,
            }
        )

    def _effective_context_for_bundle(self, game_id, bundle):
        current = self.engine.store.current_context(game_id)
        context = {
            "strategy": dict(current.get("strategy", {})),
            "cities": {
                str(key): dict(value)
                for key, value in current.get("cities", {}).items()
            },
            "units": {
                str(key): dict(value) for key, value in current.get("units", {}).items()
            },
            "builders": {
                str(key): dict(value)
                for key, value in current.get("builders", {}).items()
            },
        }
        if bundle.strategy_updates:
            context["strategy"].update(bundle.strategy_updates)
            context["strategy"]["_plan_id"] = bundle.plan_id
        for collection, id_key, rows in (
            ("cities", "city_id", bundle.city_plan_updates),
            ("units", "unit_id", bundle.unit_plan_updates),
            ("builders", "builder_key", bundle.builder_plan_updates),
        ):
            for row in rows:
                entity_id = row.get(id_key)
                if entity_id is None:
                    continue
                context[collection][str(entity_id)] = {
                    **dict(row),
                    "_plan_id": bundle.plan_id,
                }
        context["execution_mode"] = self.engine.config.execution_mode.value
        return context

    def _refresh_effective_lease_baselines(
        self, snapshot, provider_request, resolved_gaps, leases, bundle
    ):
        if not leases:
            return resolved_gaps, leases
        context = self._effective_context_for_bundle(snapshot.game_id, bundle)
        events_by_identity = {}
        for event in provider_request.trigger_events:
            try:
                identity, _ = stable_decision_identity(event)
            except ValueError:
                continue
            events_by_identity[identity] = event
        gap_by_id = {gap.decision_gap_id: gap for gap in resolved_gaps}
        refreshed_leases = []
        for lease in leases:
            gap_id = lease.decision_gap_ids[0]
            gap = gap_by_id[gap_id]
            event = events_by_identity.get(gap.stable_identity)
            if event is None:
                refreshed_leases.append(lease)
                continue
            refreshed = build_decision_gap(
                snapshot.game_id,
                self.engine._active_observation_id or gap.observation_id,
                snapshot,
                event,
                context,
                existing=gap,
                now=self.engine._now(),
            ).model_copy(
                update={
                    "status": gap.status,
                    "route": gap.route,
                    "logical_request_id": gap.logical_request_id,
                    "resolution_reason": gap.resolution_reason,
                    "invalidation_reason": gap.invalidation_reason,
                    "reopen_reason": gap.reopen_reason,
                }
            )
            lease_baseline = thaw_json(lease.contract_baseline)
            if lease_baseline.get("information_evidence"):
                effective_projection = self._carry_information_baseline(
                    thaw_json(refreshed.input_projection),
                    lease_baseline,
                )
                refreshed = refreshed.model_copy(
                    update={
                        "input_projection": effective_projection,
                        "relevant_input_hash": hash_decision_input(
                            effective_projection
                        ),
                    }
                )
            gap_by_id[gap_id] = refreshed
            refreshed_leases.append(
                lease.model_copy(
                    update={
                        "relevant_input_hash": refreshed.relevant_input_hash,
                        "input_projection_version": refreshed.input_projection_version,
                        "contract_baseline": {
                            **thaw_json(lease.contract_baseline),
                            "relevant_input_projection": thaw_json(
                                refreshed.input_projection
                            ),
                        },
                        "last_validated_observation_id": refreshed.observation_id,
                    }
                )
            )
        return (
            [gap_by_id[gap.decision_gap_id] for gap in resolved_gaps],
            refreshed_leases,
        )

    def _resolve_gaps(self, logical_request, bundle, validation, turn, observation):
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
                    observation,
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
            if lease.status is PlanLeaseStatus.AWAITING_APPROVAL:
                completed_gap = completed_gap.model_copy(
                    update={
                        "status": DecisionGapStatus.PROPOSED,
                        "resolution_reason": "validated plan lease awaits runtime approval",
                    }
                )
            resolved.append(completed_gap)
            leases.append(lease)
        successful_ids = {
            gap_id for lease in leases for gap_id in lease.decision_gap_ids
        }
        failed_atomic_ids: set[str] = set()
        for resolution in bundle.event_resolutions:
            atomic_ids = set(resolution.decision_gap_ids) & set(
                logical_request.decision_gap_ids
            )
            if resolution.atomic and atomic_ids - successful_ids:
                failed_atomic_ids.update(atomic_ids)
        if failed_atomic_ids:
            leases = [
                lease
                for lease in leases
                if not (set(lease.decision_gap_ids) & failed_atomic_ids)
            ]
            resolved = [
                gap.model_copy(
                    update={
                        "status": DecisionGapStatus.AWAITING_HUMAN,
                        "logical_request_id": logical_request.planner_request_id,
                        "resolution_reason": (
                            "atomic planner resolution rejected as a whole"
                        ),
                    }
                )
                if gap.decision_gap_id in failed_atomic_ids
                else gap
                for gap in resolved
            ]
        return resolved, leases

    def _lease_for_resolution(
        self,
        gap,
        bundle,
        logical_request,
        turn,
        resolution,
        observation,
    ):
        contract = resolution.lease_contract
        if contract is None:
            raise ValueError("resolution has no lease contract")
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
        bound = self._bind_runtime_lease_contract(
            gap, bundle, logical_request, resolution, observation, contract
        )
        projection = bound["projection"]
        preconditions = tuple(
            self._contract_condition(item) for item in bound["preconditions"]
        )
        for condition in preconditions:
            outcome = engine.conditions.evaluate(
                self._condition_for_evaluation(condition),
                observation,
                decision_projection=projection,
            )
            if not outcome.known:
                raise ValueError(outcome.reason)
            if not outcome.valid:
                raise ValueError(
                    f"lease start precondition failed: {condition.condition_type}"
                )
        resolution_tasks = [
            task for task in bundle.tasks if task.task_id in set(resolution.task_ids)
        ]
        runtime_requires_approval = (
            contract.approval_required
            or contract.recommended_risk in {RiskLevel.HIGH, RiskLevel.CRITICAL}
            or any(
                task.requires_confirmation
                or task.risk in {RiskLevel.HIGH, RiskLevel.CRITICAL}
                for task in resolution_tasks
            )
        )
        runtime_policy_allows_auto = all(
            task.action_type in engine.config.auto_action_types
            for task in resolution_tasks
        )

        approval_record = engine.store.latest_approval_record(
            gap.game_session_id,
            proposal_type="decision_gap",
            proposal_id=gap.decision_gap_id,
            proposal_revision=revision,
        )
        if approval_record is not None and approval_record.decision in {
            ApprovalDecision.APPROVED,
            ApprovalDecision.EDITED_AND_APPROVED,
        }:
            approval_status = ApprovalStatus.APPROVED
        elif (
            engine.config.execution_mode is ExecutionMode.AUTO
            and not runtime_requires_approval
            and runtime_policy_allows_auto
            and contract.proposed_execution_mode in {None, ExecutionMode.AUTO}
        ):
            approval_status = ApprovalStatus.NOT_REQUIRED
        else:
            approval_status = ApprovalStatus.REQUIRED
        lease_status = (
            PlanLeaseStatus.ACTIVE
            if approval_status in {ApprovalStatus.APPROVED, ApprovalStatus.NOT_REQUIRED}
            else PlanLeaseStatus.AWAITING_APPROVAL
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
            status=lease_status,
            approval_status=approval_status,
            valid_from_turn=turn,
            valid_until_turn=contract.valid_until_turn,
            preconditions=preconditions,
            continuation_conditions=tuple(
                self._contract_condition(item) for item in bound["continuation"]
            ),
            completion_condition=self._contract_condition(bound["completion"]),
            invalidation_conditions=tuple(
                self._contract_condition(item)
                for item in contract.invalidation_conditions
            ),
            review_conditions=tuple(
                self._contract_condition(item) for item in contract.review_conditions
            ),
            contract_baseline=bound["baseline"],
            continuation_policy=contract.continuation_policy,
            relevant_input_hash=bound["relevant_input_hash"],
            last_validated_observation_id=logical_request.observation_id,
            last_validation_result=LeaseValidationResult.VALID,
        )

    @classmethod
    def _bind_runtime_lease_contract(
        cls, gap, bundle, logical_request, resolution, observation, contract
    ):
        projection = thaw_json(gap.input_projection)
        baseline = {
            "gap_type": gap.gap_type,
            "relevant_input_projection": projection,
        }
        if gap.gap_type not in SETTLER_GAP_TYPES:
            return {
                "projection": projection,
                "relevant_input_hash": gap.relevant_input_hash,
                "preconditions": [dict(item) for item in contract.preconditions],
                "continuation": [
                    dict(item) for item in contract.continuation_conditions
                ],
                "completion": dict(contract.completion_condition),
                "baseline": baseline,
            }

        unit_id = next(
            (
                subject.subject_id
                for subject in gap.subjects
                if subject.subject_type == "unit"
            ),
            None,
        )
        if unit_id is None:
            raise ValueError("settler lease requires a bound unit subject")
        target = cls._approved_settler_target(bundle, resolution, unit_id, contract)
        if target is None:
            raise ValueError("settler lease requires an approved target")
        target_x, target_y = target
        snapshot = observation.snapshot
        owner = next(
            (
                row.get("owner")
                for row in snapshot.cities or []
                if isinstance(row, dict) and row.get("owner") is not None
            ),
            snapshot.overview.get("player_id"),
        )
        if owner is None:
            raise ValueError("settler lease requires current-player ownership evidence")
        baseline_city_count = len(snapshot.cities or [])
        projection = {
            **projection,
            "plan_target": {"target": {"x": target_x, "y": target_y}},
        }
        projection, information_evidence = cls._merge_information_evidence(
            projection,
            logical_request,
            resolution,
            unit_id,
            (target_x, target_y),
        )
        start_conditions = [
            {"type": "entity_exists", "entity_type": "unit", "entity_id": unit_id},
            {"type": "unit_type_contains", "unit_id": unit_id, "marker": "SETTLER"},
            {"type": "tile_unoccupied", "x": target_x, "y": target_y},
            {"type": "settler_target_legal", "x": target_x, "y": target_y},
            {"type": "settler_path_reachable", "x": target_x, "y": target_y},
        ]
        continuation_conditions = [
            *start_conditions,
            {"type": "approved_target_equals", "x": target_x, "y": target_y},
            {"type": "severe_threat_absent"},
        ]
        completion = {
            "type": "all_of",
            "conditions": [
                {"type": "unit_absent", "unit_id": unit_id},
                {
                    "type": "city_count_at_least",
                    "count": baseline_city_count + 1,
                },
                {
                    "type": "city_at_target",
                    "x": target_x,
                    "y": target_y,
                    "owner": owner,
                },
            ],
        }
        baseline = {
            "gap_type": gap.gap_type,
            "baseline_city_count": baseline_city_count,
            "approved_target": {"x": target_x, "y": target_y},
            "owner": owner,
            "settler_unit_id": unit_id,
            "information_evidence": information_evidence,
            "relevant_input_projection": projection,
        }
        return {
            "projection": projection,
            "relevant_input_hash": hash_decision_input(projection),
            "preconditions": cls._replace_conditions(
                contract.preconditions, start_conditions
            ),
            "continuation": cls._replace_conditions(
                contract.continuation_conditions, continuation_conditions
            ),
            "completion": completion,
            "baseline": baseline,
        }

    @staticmethod
    def _carry_information_baseline(projection, baseline):
        sources = baseline.get("information_evidence", [])
        target = baseline.get("approved_target", {})
        target_x = target.get("x") if isinstance(target, dict) else None
        target_y = target.get("y") if isinstance(target, dict) else None
        if target_x is None or target_y is None:
            return projection
        facts = {}
        for source in sources:
            if isinstance(source, dict) and isinstance(source.get("facts"), dict):
                facts.update(source["facts"])
        if not facts:
            return projection
        targets = [
            dict(row)
            for row in projection.get("candidate_targets", [])
            if isinstance(row, dict)
        ]
        for index, row in enumerate(targets):
            if (row.get("x"), row.get("y")) == (target_x, target_y):
                targets[index] = {**row, **facts}
                break
        else:
            targets.append({"x": target_x, "y": target_y, **facts})
        targets.sort(key=lambda row: (str(row.get("x")), str(row.get("y"))))
        updated = {
            **projection,
            "candidate_targets": targets,
            "information_evidence": sources,
            "plan_target": {"target": {"x": target_x, "y": target_y}},
        }
        if "reachable" in facts:
            updated["path"] = {
                **(
                    projection.get("path", {})
                    if isinstance(projection.get("path"), dict)
                    else {}
                ),
                "target_x": target_x,
                "target_y": target_y,
                "path_reachable": facts["reachable"],
            }
        return updated

    @classmethod
    def _merge_information_evidence(
        cls,
        projection,
        logical_request,
        resolution,
        unit_id,
        target,
    ):
        results = thaw_json(logical_request.information_results)
        if not isinstance(results, dict):
            return projection, []
        target_x, target_y = target
        merged_facts: dict[str, bool] = {}
        sources = []
        allowed_tools = {
            "get_pathing_estimate",
            "get_settle_advisor",
            "get_global_settle_advisor",
        }
        for request_id, payload in results.items():
            if not isinstance(payload, dict):
                continue
            if payload.get("information_request_id") != request_id:
                continue
            if payload.get("planner_request_id") != logical_request.planner_request_id:
                continue
            if payload.get("event_dedupe_key") != resolution.event_dedupe_key:
                continue
            if not payload.get("information_round_id") or not payload.get(
                "collected_from_observation_id"
            ):
                continue
            tool_name = payload.get("tool_name")
            if tool_name not in allowed_tools:
                continue
            arguments = payload.get("arguments")
            if not isinstance(arguments, dict):
                continue
            argument_unit = arguments.get("unit_id")
            if argument_unit is not None and str(argument_unit) != str(unit_id):
                continue
            facts = cls._settler_query_facts(
                tool_name,
                arguments,
                payload.get("result"),
                (target_x, target_y),
            )
            if not facts:
                continue
            for fact_name, fact_value in facts.items():
                if (
                    fact_name in merged_facts
                    and merged_facts[fact_name] is not fact_value
                ):
                    raise ValueError("conflicting information evidence")
                merged_facts[fact_name] = fact_value
            sources.append(
                {
                    "information_request_id": request_id,
                    "planner_request_id": logical_request.planner_request_id,
                    "information_round_id": payload["information_round_id"],
                    "collected_from_observation_id": payload[
                        "collected_from_observation_id"
                    ],
                    "event_dedupe_key": resolution.event_dedupe_key,
                    "query_type": payload.get("query_type"),
                    "tool_name": tool_name,
                    "arguments": arguments,
                    "target": {"x": target_x, "y": target_y},
                    "facts": facts,
                }
            )

        if not merged_facts:
            return projection, []
        targets = [
            dict(row)
            for row in projection.get("candidate_targets", [])
            if isinstance(row, dict)
        ]
        matched = False
        for index, row in enumerate(targets):
            if (row.get("x"), row.get("y")) != (target_x, target_y):
                continue
            targets[index] = {**row, **merged_facts}
            matched = True
            break
        if not matched:
            targets.append({"x": target_x, "y": target_y, **merged_facts})
        targets.sort(key=lambda row: (str(row.get("x")), str(row.get("y"))))
        updated = {
            **projection,
            "candidate_targets": targets,
            "information_evidence": sources,
        }
        if "reachable" in merged_facts:
            updated["path"] = {
                **(
                    projection.get("path", {})
                    if isinstance(projection.get("path"), dict)
                    else {}
                ),
                "target_x": target_x,
                "target_y": target_y,
                "path_reachable": merged_facts["reachable"],
            }
        return updated, sources

    @classmethod
    def _settler_query_facts(cls, tool_name, arguments, raw_result, target):
        facts: dict[str, bool] = {}
        for row in cls._query_candidate_rows(raw_result):
            x = row.get("x", row.get("target_x"))
            y = row.get("y", row.get("target_y"))
            if x is None or y is None or (int(x), int(y)) != target:
                continue
            legal = cls._strict_bool_fact(
                row,
                ("legal", "is_legal", "valid", "is_valid", "target_legal"),
            )
            reachable = cls._strict_bool_fact(
                row,
                ("reachable", "path_reachable"),
            )
            path = row.get("path")
            if reachable is None and isinstance(path, dict):
                reachable = cls._strict_bool_fact(
                    path,
                    ("reachable", "path_reachable"),
                )
                if reachable is None:
                    reachable = cls._path_status_fact(path.get("path_status"))
            if reachable is None:
                reachable = cls._path_status_fact(row.get("path_status"))
            if legal is not None:
                facts["legal"] = legal
            if reachable is not None:
                facts["reachable"] = reachable

        argument_target = (arguments.get("target_x"), arguments.get("target_y"))
        if tool_name == "get_pathing_estimate" and None not in argument_target:
            if (int(argument_target[0]), int(argument_target[1])) == target:
                rows = cls._query_mapping_rows(raw_result)
                reachable = next(
                    (
                        value
                        for row in rows
                        if (
                            value := cls._strict_bool_fact(
                                row, ("reachable", "path_reachable")
                            )
                        )
                        is not None
                    ),
                    None,
                )
                if reachable is None:
                    reachable = next(
                        (
                            value
                            for row in rows
                            if (value := cls._path_status_fact(row.get("path_status")))
                            is not None
                        ),
                        None,
                    )
                if reachable is not None:
                    facts["reachable"] = reachable
        return facts

    @classmethod
    def _query_candidate_rows(cls, value):
        rows = []
        if isinstance(value, list):
            for item in value:
                rows.extend(cls._query_candidate_rows(item))
            return rows
        if not isinstance(value, dict):
            return rows
        x = value.get("x", value.get("target_x"))
        y = value.get("y", value.get("target_y"))
        if x is not None and y is not None:
            rows.append(value)
        for key in (
            "sites",
            "candidates",
            "targets",
            "recommended_sites",
            "settle_sites",
            "items",
        ):
            nested = value.get(key)
            if isinstance(nested, (dict, list)):
                rows.extend(cls._query_candidate_rows(nested))
        return rows

    @staticmethod
    def _query_mapping_rows(value):
        if not isinstance(value, dict):
            return []
        rows = [value]
        for key in ("path", "estimate", "data", "result"):
            nested = value.get(key)
            if isinstance(nested, dict):
                rows.append(nested)
        return rows

    @staticmethod
    def _strict_bool_fact(row, keys):
        for key in keys:
            value = row.get(key)
            if type(value) is bool:
                return value
            if type(value) is int and value in {0, 1}:
                return bool(value)
            if isinstance(value, str):
                normalized = value.strip().upper()
                if normalized in {"TRUE", "YES", "VALID", "REACHABLE"}:
                    return True
                if normalized in {"FALSE", "NO", "INVALID", "UNREACHABLE"}:
                    return False
        return None

    @staticmethod
    def _path_status_fact(value):
        if not isinstance(value, str):
            return None
        normalized = value.strip().upper().replace("-", "_").replace(" ", "_")
        if normalized in {"REACHABLE", "VALID", "OK", "SUCCESS", "PATH_FOUND"}:
            return True
        if normalized in {
            "UNREACHABLE",
            "BLOCKED",
            "INVALID",
            "NO_PATH",
            "PATH_NOT_FOUND",
        }:
            return False
        return None

    @staticmethod
    def _replace_conditions(existing, replacements):
        replacement_types = {item["type"] for item in replacements}
        return [
            *(
                dict(item)
                for item in existing
                if item.get("type") not in replacement_types
            ),
            *(dict(item) for item in replacements),
        ]

    @staticmethod
    def _approved_settler_target(bundle, resolution, unit_id, contract):
        for row in bundle.unit_plan_updates:
            if str(row.get("unit_id")) != str(unit_id):
                continue
            target = row.get("target")
            if (
                isinstance(target, dict)
                and target.get("x") is not None
                and target.get("y") is not None
            ):
                return int(target["x"]), int(target["y"])
        for task in bundle.tasks:
            if task.task_id not in set(resolution.task_ids):
                continue
            x = task.arguments.get("target_x")
            y = task.arguments.get("target_y")
            if x is not None and y is not None:
                return int(x), int(y)
        for condition in contract.preconditions:
            if condition.get("type") not in {"tile_unoccupied", "settler_target_legal"}:
                continue
            if condition.get("x") is not None and condition.get("y") is not None:
                return int(condition["x"]), int(condition["y"])
        return None

    @staticmethod
    def _condition_for_evaluation(condition):
        payload = {"type": condition.condition_type, **dict(condition.parameters)}
        if condition.subject is not None:
            payload.setdefault("entity_type", condition.subject.subject_type)
            payload.setdefault("entity_id", condition.subject.subject_id)
        if condition.expected is not True:
            payload.setdefault("value", condition.expected)
        return payload

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
        plan_bundle=None,
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
        human_wait_context = None
        if isinstance(tick, AwaitingHumanTick):
            human_wait_context = engine._human_wait_context(snapshot)
            human_wait_context["blocking_reason"] = tick.blocking_reason
        engine.store.persist_phase4_tick(
            tick,
            decision_gaps=decision_gaps,
            decision_group=decision_group,
            plan_leases=plan_leases,
            planner_request=planner_request,
            provider_attempts=provider_attempts,
            information_round=information_round,
            plan_bundle=plan_bundle,
            plan_bundle_mode=engine.config.execution_mode,
            plan_bundle_auto_action_types=engine.config.auto_action_types,
            plan_bundle_observation_id=engine._active_observation_id,
            cancel_task_ids=cancel_task_ids,
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
