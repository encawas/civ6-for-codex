import asyncio
import hashlib
import json
import sqlite3
from datetime import UTC, datetime

import pytest

from civ6_workflow.characterization import RecordingPlanner
from civ6_workflow.decisioning import (
    batch_compatible_gaps,
    build_decision_gap,
    build_decision_input_projection,
    evaluate_plan_lease,
    evaluate_planner_eligibility,
    hash_decision_input,
    stable_decision_identity,
)
from civ6_workflow.domain import (
    ApprovalStatus,
    ContinuationPolicy,
    Condition,
    ApprovalDecision,
    ApprovalRecord,
    DecisionGapStatus,
    PlannerRequest,
    PlannerRequestStatus,
    ProviderAttemptStatus,
    LeaseValidationResult,
    ProviderAttempt,
    LogicalPlannerRequestCreatedTick,
    PlanLeaseUpdatedTick,
    PlanLease,
    PlanLeaseStatus,
    RuntimeState,
    TickOutcomeKind,
    SubjectRef,
)
from civ6_workflow.engine import (
    EngineConfig,
    InjectedCrashBoundary,
    WorkflowEngine,
)
from civ6_workflow.models import (
    ActionResult,
    EventLevel,
    ExecutionMode,
    GameEvent,
    PlanBundle,
    ProposedTask,
    RiskLevel,
    RuntimeSnapshot,
)
from civ6_workflow.events import events_from_snapshot
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore
from civ6_workflow.workflow_protocol import (
    EventResolution,
    LeaseContract,
    ResolutionDisposition,
    WorkflowPlanBundle,
)


def _settler_event(turn=1, *, targets=None):
    return GameEvent(
        event_id=f"event-{turn}",
        event_type="settler_site_selection_required",
        turn=turn,
        entity_type="unit",
        entity_id=7,
        level=EventLevel.L3,
        risk=RiskLevel.HIGH,
        blocking=True,
        payload={
            "reason": "choose a site",
            "available_targets": targets or [{"x": 8, "y": 9}],
            "unrelated": f"audit-{turn}",
        },
        dedupe_key=f"settler-audit:{turn}",
    )


def _snapshot(turn=1, *, threat="LOW", targets=None):
    return RuntimeSnapshot(
        turn=turn,
        game_id="opening",
        overview={
            "turn": turn,
            "num_cities": 1,
            "threat_level": threat,
            "unrelated_raw_field": f"raw-{turn}",
            "player_id": 1,
        },
        cities=[{"city_id": 1, "owner": 1, "currently_building": "UNIT_SCOUT"}],
        units=[
            {
                "unit_id": 7,
                "unit_type": "UNIT_SETTLER",
                "moves_remaining": 2,
                "x": 4,
                "y": 5,
                "unrelated_extension": targets,
                "targets": targets
                or [{"x": 8, "y": 9, "legal": True, "reachable": True}],
            }
        ],
        blockers=[
            {
                "type": "end_turn_blocker",
                "blocking_type": "ENDTURN_BLOCKING_UNITS",
            }
        ],
    )


def _gap(turn=1, *, event=None, snapshot=None, context=None, existing=None):
    snapshot = snapshot or _snapshot(turn)
    event = event or _settler_event(turn)
    return build_decision_gap(
        "opening",
        f"obs-{turn}",
        snapshot,
        event,
        context or {"strategy": {"revision": 3}},
        existing=existing,
        now=datetime(2026, 1, turn, tzinfo=UTC),
    )


def test_evt_002_stable_gap_identity_and_hash_ignore_observation_time():
    """EVT-002 / AI-007: persistent strategic questions retain semantic identity."""

    first = _gap(1)
    second = _gap(2, existing=first)

    assert first.decision_gap_id == second.decision_gap_id
    assert first.stable_identity == "settler-site-selection-required:unit-7"
    assert first.relevant_input_hash == second.relevant_input_hash
    assert second.observation_id == "obs-2"
    assert second.first_observation_id == "obs-1"
    assert second.source_event_ids == ("event-1", "event-2")
    assert "turn" not in second.stable_identity


def test_evt_003_turn_specific_identity_requires_declared_policy():
    """EVT-003: only declared ephemeral decisions include the turn."""

    one = GameEvent(
        event_type="tactical_attack_opportunity",
        turn=7,
        entity_type="unit",
        entity_id=4,
        dedupe_key="attack-audit-7",
    )
    two = one.model_copy(update={"turn": 8, "dedupe_key": "attack-audit-8"})

    assert stable_decision_identity(one) == (
        "tactical-attack-opportunity:unit-4:turn-7",
        True,
    )
    assert stable_decision_identity(two)[0].endswith("turn-8")
    with pytest.raises(ValueError, match="routine event"):
        stable_decision_identity(
            GameEvent(
                event_type="unit_orders_required",
                turn=7,
                dedupe_key="routine",
            )
        )


def test_ai_007_projection_hash_is_versioned_and_material_only():
    """AI-007: irrelevant raw changes do not hash; relevant target/threat changes do."""

    first = build_decision_input_projection(
        _snapshot(1), _settler_event(1), {"strategy": {"revision": 3}}
    )
    reordered = {key: first[key] for key in reversed(tuple(first))}
    assert hash_decision_input(first) == hash_decision_input(reordered)
    assert first["projection_version"].startswith("decision-input/v2/")
    assert "overview" not in first
    assert "event_facts" not in first
    natural = _snapshot(2)
    moved_unit = dict(natural.units[0])
    moved_unit.update({"x": 6, "y": 7, "moves_remaining": 0})
    natural = natural.model_copy(
        update={
            "overview": {
                **natural.overview,
                "gold": 500,
                "science_yield": 42,
                "score": 99,
                "era_score": 12,
            },
            "units": [moved_unit],
            "tech_civics": {"current_research": "TECH_MINING"},
        }
    )
    unchanged = build_decision_input_projection(
        natural, _settler_event(2), {"strategy": {"revision": 99}}
    )
    assert hash_decision_input(unchanged) == hash_decision_input(first)

    target_changed = _gap(
        2,
        event=_settler_event(2, targets=[{"x": 10, "y": 11}]),
    )
    threat_changed = _gap(2, snapshot=_snapshot(2, threat="HIGH"))
    plan_changed = _gap(2, context={"strategy": {"expansion_target": {"x": 9, "y": 9}}})
    baseline = _gap(1)
    assert target_changed.relevant_input_hash != baseline.relevant_input_hash
    assert threat_changed.relevant_input_hash != baseline.relevant_input_hash
    assert plan_changed.relevant_input_hash != baseline.relevant_input_hash
    occupied_snapshot = _snapshot(2).model_copy(
        update={
            "cities": [
                *_snapshot(2).cities,
                {"city_id": 2, "x": 8, "y": 9, "currently_building": "UNIT_SCOUT"},
            ]
        }
    )
    occupied_changed = _gap(2, snapshot=occupied_snapshot, event=_settler_event(2))
    legal_changed = _gap(
        2,
        event=_settler_event(2, targets=[{"x": 8, "y": 9, "legal": False}]),
    )
    path_changed = _gap(
        2,
        event=_settler_event(2, targets=[{"x": 8, "y": 9, "reachable": False}]),
    )
    assert occupied_changed.relevant_input_hash != baseline.relevant_input_hash
    assert legal_changed.relevant_input_hash != baseline.relevant_input_hash
    assert path_changed.relevant_input_hash != baseline.relevant_input_hash


def _lease(gap, *, scope="unit:7", until=5, input_hash=None):
    return PlanLease(
        plan_lease_id=f"lease:{scope}",
        plan_id=f"plan:{scope}",
        game_session_id="opening",
        decision_gap_ids=(gap.decision_gap_id,),
        scope=scope,
        subjects=gap.subjects,
        covered_slots=("unit_route",),
        task_ids=(f"task:{scope}",),
        preconditions=(
            Condition(condition_type="turn_at_least", parameters={"turn": 0}),
        ),
        continuation_conditions=(
            Condition(condition_type="turn_at_least", parameters={"turn": 0}),
        ),
        completion_condition=Condition(
            condition_type="city_count_at_least", parameters={"count": 99}
        ),
        invalidation_conditions=(
            Condition(condition_type="turn_equals", parameters={"turn": 999}),
        ),
        review_conditions=(
            Condition(condition_type="turn_equals", parameters={"turn": 999}),
        ),
        plan_revision=1,
        source_planner_request_id=None,
        created_from_observation_id="obs-1",
        status=PlanLeaseStatus.ACTIVE,
        approval_status=ApprovalStatus.APPROVED,
        valid_from_turn=1,
        valid_until_turn=until,
        continuation_policy=ContinuationPolicy.EXTEND_WHEN_INPUT_UNCHANGED,
        relevant_input_hash=input_hash or gap.relevant_input_hash,
        last_validated_observation_id="obs-1",
        last_validation_result=LeaseValidationResult.VALID,
    )


def test_plan_003_006_review_extends_unchanged_lease_without_planner():
    """PLAN-003 / PLAN-006: a review boundary is a deterministic comparison."""

    gap = _gap(1)
    lease = _lease(gap, until=1)
    observation = normalize_runtime_snapshot(_snapshot(2))
    result = evaluate_plan_lease(
        lease, observation, relevant_input_hash=gap.relevant_input_hash
    )

    assert result.result is LeaseValidationResult.VALID
    assert result.lease.status is PlanLeaseStatus.ACTIVE
    assert result.lease.valid_until_turn == 3
    gate = evaluate_planner_eligibility(
        [gap],
        [result.lease],
        runtime_state=RuntimeState.ROUTING.value,
        has_ready_deterministic_task=False,
        active_attempt=False,
        logical_requests_this_turn=0,
        active_logical_request=False,
    )
    assert gate.eligible is False
    assert "uncovered" in gate.reason


def test_plan_004_local_lease_invalidation_preserves_other_scopes():
    """PLAN-004 / PLAN-005: lease state and plan revisions are scope-local."""

    first = _gap(1)
    other = first.model_copy(
        update={
            "decision_gap_id": "gap-other",
            "stable_identity": "city-role:city-2",
            "scope": "city:2",
        }
    )
    leases = [_lease(first, scope="city:1"), _lease(other, scope="city:2")]
    invalidated = leases[0].model_copy(
        update={
            "status": PlanLeaseStatus.INVALIDATED,
            "last_validation_result": LeaseValidationResult.INVALIDATED,
            "invalidation_reason": "production item unavailable",
        }
    )
    leases[0] = invalidated

    assert leases[1].status is PlanLeaseStatus.ACTIVE
    assert leases[1].plan_revision == 1
    gate = evaluate_planner_eligibility(
        [first, other],
        leases,
        runtime_state=RuntimeState.ROUTING.value,
        has_ready_deterministic_task=False,
        active_attempt=False,
        logical_requests_this_turn=0,
        active_logical_request=False,
    )
    assert [gap.decision_gap_id for gap in gate.gaps] == [first.decision_gap_id]


def test_issue7_city_lease_invalidation_preserves_other_plan_projections(tmp_path):
    store = WorkflowStore(tmp_path / "scope-local.sqlite3")
    plan_id = "plan:city:1"
    store.save_plan_bundle(
        "opening",
        1,
        PlanBundle(
            plan_id=plan_id,
            summary="mixed scope plan",
            strategy_updates={
                "revision": "research-v1",
                "research_queue": ["TECH_MINING"],
            },
            city_plan_updates=[
                {
                    "city_id": 1,
                    "role": "production",
                    "followup_queue": ["UNIT_BUILDER"],
                },
                {
                    "city_id": 2,
                    "role": "science",
                    "followup_queue": ["BUILDING_LIBRARY"],
                },
            ],
            unit_plan_updates=[
                {
                    "unit_id": 7,
                    "goal": "defend",
                    "target": {"x": 4, "y": 5},
                    "revision": 1,
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types=set(),
    )
    gap = _gap(1)
    lease = _lease(gap, scope="city:1").model_copy(
        update={
            "plan_id": plan_id,
            "subjects": (SubjectRef(subject_type="city", subject_id="1"),),
            "covered_slots": ("city_production",),
            "status": PlanLeaseStatus.INVALIDATED,
            "last_validation_result": LeaseValidationResult.INVALIDATED,
            "invalidation_reason": "city production target changed",
        }
    )
    now = datetime.now(UTC)
    tick = PlanLeaseUpdatedTick(
        tick_id="tick-city-lease-invalidated",
        game_session_id="opening",
        turn_number=1,
        starting_runtime_state=RuntimeState.ROUTING,
        observation_ids=("obs-scope-local",),
        started_at=now,
        completed_at=now,
        plan_lease_id=lease.plan_lease_id,
        validation_result=LeaseValidationResult.INVALIDATED.value,
    )

    store.persist_phase4_tick(tick, plan_leases=[lease])

    context = store.current_context("opening")
    assert "1" not in context["cities"]
    assert "2" in context["cities"]
    assert "7" in context["units"]
    assert context["strategy"]["research_queue"] == ["TECH_MINING"]


@pytest.mark.parametrize(
    "state",
    [
        RuntimeState.VERIFYING,
        RuntimeState.AWAITING_HUMAN,
        RuntimeState.AWAITING_APPROVAL,
        RuntimeState.TURN_TRANSITIONING,
        RuntimeState.SYSTEM_ERROR,
        RuntimeState.PLANNER_BACKOFF,
    ],
)
def test_ai_001_002_006_planner_gate_rejects_runtime_work(state):
    """AI-001 / AI-002 / AI-006: runtime work is never a strategic request."""

    gap = _gap(1)
    gate = evaluate_planner_eligibility(
        [gap],
        [],
        runtime_state=state.value,
        has_ready_deterministic_task=False,
        active_attempt=False,
        logical_requests_this_turn=0,
        active_logical_request=False,
    )
    assert gate.eligible is False


def test_ai_003_005_group_is_stable_and_requires_one_observation():
    """AI-003 / AI-005: compatible gaps form one stable logical decision group."""

    first = _gap(1)
    second = first.model_copy(
        update={
            "decision_gap_id": "gap-research",
            "stable_identity": "research-direction:empire:strategy-3",
            "gap_type": "research_direction_required",
            "scope": "empire",
            "source_event_ids": ("research-event",),
        }
    )
    group = batch_compatible_gaps("opening", "obs-1", [second, first])
    repeated = batch_compatible_gaps("opening", "obs-1", [first, second])
    assert group.decision_group_id == repeated.decision_group_id
    assert group.input_projection_hash == repeated.input_projection_hash

    with pytest.raises(ValueError, match="share one observation"):
        batch_compatible_gaps(
            "opening",
            "obs-1",
            [first, second.model_copy(update={"observation_id": "obs-2"})],
        )


def test_ai_007_met_003_004_dedup_and_metrics_survive_restart(tmp_path):
    """AI-007 / MET-003 / MET-004: durable records remain authoritative."""

    path = tmp_path / "phase4.sqlite3"
    store = WorkflowStore(path)
    gap = _gap(1)
    store.save_decision_gap(gap, turn=1)

    restarted = WorkflowStore(path)
    loaded = restarted.decision_gap_by_identity("opening", gap.stable_identity)
    assert loaded == gap
    assert restarted.planner_metrics("opening") == {
        "logical_requests": 0,
        "provider_attempts": 0,
        "information_rounds": 0,
        "duplicate_request_suppressions": 0,
        "zero_planner_turn_ratio": 1.0,
    }


class _Game:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.call_count = 0
        self.mutations = 0
        self.query_count = 0

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        return self.snapshot.model_copy(deep=True)

    async def list_tools(self):
        return {
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "get_settle_advisor",
        }

    async def execute_task(self, task):
        self.call_count += 1
        self.mutations += 1
        return ActionResult(success=True)

    async def end_turn(self):
        self.call_count += 1
        self.mutations += 1
        return ActionResult(success=True)

    async def query_tool(self, name, arguments):
        self.call_count += 1
        self.query_count += 1
        return {"sites": [{"x": 8, "y": 9}], "tool": name}


def _settler_lease_contract(turn=1):
    return LeaseContract(
        valid_until_turn=turn + 5,
        preconditions=[
            {"type": "entity_exists", "entity_type": "unit", "entity_id": "7"},
            {"type": "unit_type_contains", "unit_id": "7", "marker": "SETTLER"},
            {"type": "tile_unoccupied", "x": 8, "y": 9},
            {"type": "settler_target_legal", "x": 8, "y": 9},
            {"type": "settler_path_reachable", "x": 8, "y": 9},
        ],
        continuation_conditions=[
            {"type": "entity_exists", "entity_type": "unit", "entity_id": "7"},
            {"type": "unit_type_contains", "unit_id": "7", "marker": "SETTLER"},
            {"type": "tile_unoccupied", "x": 8, "y": 9},
            {"type": "settler_target_legal", "x": 8, "y": 9},
            {"type": "settler_path_reachable", "x": 8, "y": 9},
            {"type": "approved_target_equals", "x": 8, "y": 9},
            {"type": "severe_threat_absent"},
        ],
        completion_condition={
            "type": "all_of",
            "conditions": [
                {"type": "unit_absent", "unit_id": "7"},
                {"type": "city_count_at_least", "count": 1},
            ],
        },
        invalidation_conditions=[
            {"type": "unit_absent", "unit_id": "7"},
            {
                "type": "field_in",
                "path": "overview.threat_level",
                "values": ["HIGH", "SEVERE", "CRITICAL"],
            },
        ],
        review_conditions=[{"type": "turn_at_least", "turn": turn + 5}],
        continuation_policy=ContinuationPolicy.EXTEND_WHEN_INPUT_UNCHANGED,
        approval_required=False,
        covered_slots=["unit_route"],
        subjects=[{"subject_type": "unit", "subject_id": "7"}],
    )


class _ResolvingPlanner:
    def __init__(self):
        self.calls = 0
        self.last_diagnostics = None

    async def plan(self, request):
        self.calls += 1
        self.last_diagnostics = {"attempt_count": 1, "backend": "test"}
        gap_ids = list(request.constraints["decision_gap_ids"])
        event = request.trigger_events[0]
        return WorkflowPlanBundle(
            summary="select settlement site",
            unit_plan_updates=[
                {
                    "unit_id": 7,
                    "goal": "found_city",
                    "target": {"x": 8, "y": 9},
                    "revision": 1,
                }
            ],
            next_review_turn=request.turn + 5,
            event_resolutions=[
                EventResolution(
                    event_dedupe_key=event.dedupe_key,
                    decision_gap_ids=gap_ids,
                    disposition=ResolutionDisposition.PLAN_UPDATE,
                    plan_refs=["unit:7"],
                    reason="approved settlement route",
                    lease_contract=_settler_lease_contract(request.turn),
                )
            ],
        )


class _InformationPlanner(_ResolvingPlanner):
    async def plan(self, request):
        self.calls += 1
        self.last_diagnostics = {"attempt_count": 1, "backend": "test"}
        gap_ids = list(request.constraints["decision_gap_ids"])
        event = request.trigger_events[0]
        if not request.information_results:
            from civ6_workflow.workflow_protocol import InformationRequest

            info = InformationRequest(
                request_id="site-info",
                event_dedupe_key=event.dedupe_key,
                query_type="settler_select_site",
                tool_name="get_settle_advisor",
                arguments={"unit_id": 7},
                purpose="rank legal settlement sites",
            )
            return WorkflowPlanBundle(
                summary="need focused site data",
                information_requests=[info],
                event_resolutions=[
                    EventResolution(
                        event_dedupe_key=event.dedupe_key,
                        decision_gap_ids=gap_ids,
                        disposition=ResolutionDisposition.INFORMATION_REQUIRED,
                        information_request_ids=[info.request_id],
                        reason="site legality is missing",
                    )
                ],
            )
        return await super().plan(request)


def _engine(tmp_path, planner):
    snapshot = RuntimeSnapshot(
        turn=1,
        game_id="opening",
        overview={"turn": 1, "player_id": 1, "num_cities": 0, "num_units": 1},
        cities=[],
        units=[
            {
                "unit_id": 7,
                "unit_type": "UNIT_SETTLER",
                "moves_remaining": 2,
                "x": 4,
                "y": 5,
                "targets": [{"x": 8, "y": 9, "legal": True, "reachable": True}],
            }
        ],
        blockers=[],
    )
    game = _Game(snapshot)
    recording = RecordingPlanner(planner)
    engine = WorkflowEngine(
        store=WorkflowStore(tmp_path / "runtime.sqlite3"),
        game=game,
        planner=recording,
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=False,
            max_agent_calls_per_turn=1,
        ),
    )
    return engine, game, recording


def test_ai_001_003_phase4_vertical_chain_and_zero_mutation(tmp_path):
    """AI-001 / AI-003 / PLAN-003: gap, request, attempt, and lease cross Ticks."""

    async def scenario():
        engine, game, planner = _engine(tmp_path, _ResolvingPlanner())

        first = await engine.tick()
        assert first.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_CREATED
        assert planner.summary.logical_requests == 0

        second = await engine.tick()
        assert (
            second.workflow_tick["outcome"]
            == TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED
        )
        assert planner.summary.logical_requests == 0

        third = await engine.tick()
        assert (
            third.workflow_tick["outcome"] == TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED
        ), third.workflow_tick
        assert planner.summary.logical_requests == 1
        assert planner.summary.provider_attempts == 1
        assert game.mutations == 0

        metrics = engine.store.planner_metrics("opening")
        assert metrics["logical_requests"] == 1
        assert metrics["provider_attempts"] == 1
        assert len(engine.store.list_plan_leases("opening")) == 1
        lease = engine.store.list_plan_leases("opening")[0]
        assert lease.status is PlanLeaseStatus.ACTIVE
        assert lease.approval_status is ApprovalStatus.NOT_REQUIRED
        gap = engine.store.list_decision_gaps("opening")[0]
        assert gap.status is DecisionGapStatus.RESOLVED

    asyncio.run(scenario())


def test_ai_004_information_round_is_one_logical_request(tmp_path):
    """AI-004 / AI-009 / MET-003: information continuation reuses logical identity."""

    async def scenario():
        engine, game, planner = _engine(tmp_path, _InformationPlanner())

        outcomes = []
        for _ in range(5):
            result = await engine.tick()
            outcomes.append(result.workflow_tick["outcome"])

        assert outcomes == [
            TickOutcomeKind.DECISION_GAP_CREATED,
            TickOutcomeKind.LOGICAL_PLANNER_REQUEST_CREATED,
            TickOutcomeKind.INFORMATION_REQUESTED,
            TickOutcomeKind.INFORMATION_COLLECTED,
            TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED,
        ]
        assert planner.summary.logical_requests == 1
        assert planner.summary.provider_attempts == 2
        assert game.query_count == 1
        metrics = engine.store.planner_metrics("opening")
        assert metrics["logical_requests"] == 1
        assert metrics["provider_attempts"] == 2
        assert metrics["information_rounds"] == 1
        requests = planner.requests
        assert requests[0].request_id != requests[1].request_id
        assert planner.calls[0].logical_transaction_id == (
            planner.calls[1].logical_transaction_id
        )

    asyncio.run(scenario())


class _Crash:
    def __init__(self, target):
        self.target = target

    def checkpoint(self, name):
        if name == self.target:
            raise RuntimeError(f"crash at {name}")


class _HookResolvingPlanner(_ResolvingPlanner):
    def __init__(self):
        super().__init__()
        self.hook = None

    def set_provider_attempt_hook(self, hook):
        self.hook = hook
        return True

    async def plan(self, request):
        if self.hook is not None:
            await self.hook(
                "started",
                {
                    "provider_request_id": request.request_id,
                    "attempt_number": self.calls + 1,
                },
            )
        return await super().plan(request)


def test_issue7_game_scoped_identity_and_replay_are_isolated(tmp_path):
    now = datetime(2026, 2, 1, tzinfo=UTC)
    records = []
    source = WorkflowStore(tmp_path / "source.sqlite3")
    for game_id in ("game-a", "game-b"):
        snapshot = _snapshot().model_copy(update={"game_id": game_id})
        gap = build_decision_gap(
            game_id,
            f"obs-{game_id}",
            snapshot,
            _settler_event(),
            {"strategy": {"revision": 3}},
            now=now,
        )
        group = batch_compatible_gaps(game_id, f"obs-{game_id}", [gap], now=now)
        request = PlannerRequest(
            planner_request_id=f"request-{game_id}",
            game_session_id=game_id,
            turn_number=1,
            observation_id=f"obs-{game_id}",
            decision_gap_ids=(gap.decision_gap_id,),
            decision_group_id=group.decision_group_id,
            input_projection_hash=group.input_projection_hash,
            input_projection={"game": game_id},
            policy_revision="planner-call-policy/v1",
            model_settings={"provider": "test"},
            status=PlannerRequestStatus.PENDING,
            created_at=now,
        )
        lease = _lease(gap).model_copy(
            update={
                "plan_lease_id": f"lease-{game_id}",
                "plan_id": f"plan-{game_id}",
                "game_session_id": game_id,
                "source_planner_request_id": request.planner_request_id,
            }
        )
        source.save_decision_gap(gap, turn=1)
        source.save_planner_request(request)
        source.save_plan_lease(lease)
        source.save_approval_record(
            game_id,
            ApprovalRecord(
                approval_id=f"approval-{game_id}",
                proposal_type="decision_gap",
                proposal_id=gap.decision_gap_id,
                proposal_revision=lease.plan_revision,
                decision=ApprovalDecision.APPROVED,
                actor="reviewer",
                created_at=now,
            ),
        )
        records.append((gap, group, request, lease))

    first, second = records
    assert first[0].stable_identity == second[0].stable_identity
    assert first[0].decision_gap_id != second[0].decision_gap_id
    assert first[1].decision_group_id != second[1].decision_group_id
    assert source.get_decision_gap("game-a", second[0].decision_gap_id) is None
    assert (
        source.get_planner_request(first[2].planner_request_id).game_session_id
        == "game-a"
    )
    assert (
        source.get_planner_request(second[2].planner_request_id).game_session_id
        == "game-b"
    )

    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(source.export_replay_state("game-a"))
    restored.import_replay_state(source.export_replay_state("game-b"))
    assert len(restored.list_decision_gaps("game-a")) == 1
    assert len(restored.list_decision_gaps("game-b")) == 1
    assert len(restored.list_plan_leases("game-a")) == 1
    assert len(restored.list_plan_leases("game-b")) == 1
    for index, game_id in enumerate(("game-a", "game-b")):
        gap, _, _, lease = records[index]
        approval = restored.latest_approval_record(
            game_id,
            proposal_type="decision_gap",
            proposal_id=gap.decision_gap_id,
            proposal_revision=lease.plan_revision,
        )
        assert approval.approval_id == f"approval-{game_id}"


def test_issue7_diplomacy_and_trade_identity_preserve_cardinality():
    snapshot = _snapshot().model_copy(
        update={
            "blockers": [
                {
                    "type": "pending_diplomacy",
                    "data": [
                        {"request_id": "dip-a", "player_id": 2},
                        {"request_id": "dip-b", "player_id": 2},
                    ],
                },
                {
                    "type": "pending_trades",
                    "data": [
                        {"player_id": 3, "give": {"gold": 10}},
                        {"player_id": 3, "give": {"gold": 20}},
                    ],
                },
            ]
        }
    )
    events = events_from_snapshot(snapshot)
    diplomacy = [event for event in events if event.event_type == "pending_diplomacy"]
    trades = [event for event in events if event.event_type == "pending_trade_offer"]
    assert len({stable_decision_identity(event)[0] for event in diplomacy}) == 2
    assert len({stable_decision_identity(event)[0] for event in trades}) == 2
    assert len({event.payload["offer_id"] for event in trades}) == 2


def test_issue7_lease_invalidation_precedes_due_and_new_tasks(tmp_path):
    async def scenario():
        planner = _ResolvingPlanner()
        engine, game, recording = _engine(tmp_path, planner)
        game.snapshot = game.snapshot.model_copy(
            update={
                "overview": {
                    **game.snapshot.overview,
                    "threat_level": "CRITICAL",
                },
                "units": [
                    *game.snapshot.units,
                    {
                        "unit_id": 8,
                        "unit_type": "UNIT_WARRIOR",
                        "moves_remaining": 2,
                        "x": 1,
                        "y": 1,
                    },
                ],
            }
        )
        gap = _gap(snapshot=game.snapshot).model_copy(
            update={
                "status": DecisionGapStatus.RESOLVED,
                "resolution_reason": "approved route",
            }
        )
        dependent = ProposedTask(
            task_id="dependent-unit-7",
            action_type="unit_skip",
            entity_type="unit",
            entity_id=7,
            due_turn=1,
            arguments={"unit_id": 7},
            preconditions=[
                {"type": "entity_exists", "entity_type": "unit", "entity_id": 7}
            ],
            postconditions=[{"type": "unit_no_moves", "unit_id": 7}],
            reason="lease-dependent work",
        )
        unrelated = dependent.model_copy(
            update={
                "task_id": "unrelated-unit-8",
                "entity_id": 8,
                "arguments": {"unit_id": 8},
                "preconditions": [
                    {"type": "entity_exists", "entity_type": "unit", "entity_id": 8}
                ],
                "postconditions": [{"type": "unit_no_moves", "unit_id": 8}],
                "reason": "unrelated deterministic work",
            }
        )
        lease = _lease(gap).model_copy(
            update={
                "plan_id": "leased-plan",
                "task_ids": (dependent.task_id,),
                "invalidation_conditions": (
                    Condition(
                        condition_type="field_in",
                        parameters={
                            "path": "overview.threat_level",
                            "values": ["CRITICAL"],
                        },
                    ),
                ),
            }
        )
        engine.store.save_decision_gap(gap, turn=1)
        engine.store.save_plan_lease(lease)
        engine.store.save_plan_bundle(
            "opening",
            1,
            PlanBundle(
                plan_id="leased-plan",
                summary="leased and unrelated tasks",
                unit_plan_updates=[
                    {
                        "unit_id": 7,
                        "goal": "found_city",
                        "target": {"x": 8, "y": 9},
                        "revision": 1,
                    }
                ],
                tasks=[dependent],
            ),
            mode=ExecutionMode.AUTO,
            auto_action_types={"unit_skip"},
        )
        engine.store.save_plan_bundle(
            "opening",
            1,
            PlanBundle(
                plan_id="unrelated-plan",
                summary="unrelated task",
                tasks=[unrelated],
            ),
            mode=ExecutionMode.AUTO,
            auto_action_types={"unit_skip"},
        )

        first = await engine.tick()
        assert first.workflow_tick["outcome"] == TickOutcomeKind.PLAN_LEASE_UPDATED
        assert game.mutations == 0
        assert (
            engine.store.task_status("opening", dependent.task_id).value == "cancelled"
        )
        assert engine.store.task_status("opening", unrelated.task_id).value == "ready"
        assert all(
            task.entity_id != "7" and str(task.entity_id) != "7"
            for task in engine.store.due_tasks("opening", 1)
        )

        second = await engine.tick()
        assert second.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
        assert second.workflow_tick["task_id"] == unrelated.task_id
        assert game.mutations == 1
        assert recording.summary.logical_requests == 0

    asyncio.run(scenario())


def test_issue7_active_request_is_superseded_before_provider_call(tmp_path):
    async def scenario():
        engine, game, planner = _engine(tmp_path, _ResolvingPlanner())
        await engine.tick()
        created = await engine.tick()
        request_id = created.workflow_tick["planner_request_id"]
        game.snapshot = game.snapshot.model_copy(update={"units": []})

        superseded = await engine.tick()
        assert (
            superseded.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        )
        request = engine.store.get_planner_request(request_id)
        assert request.status is PlannerRequestStatus.SUPERSEDED
        assert planner.summary.logical_requests == 0
        assert engine.store.list_provider_attempts(request_id) == []
        assert game.query_count == 0

        restarted = WorkflowStore(tmp_path / "runtime.sqlite3")
        assert (
            restarted.get_planner_request(request_id).status
            is PlannerRequestStatus.SUPERSEDED
        )
        assert restarted.active_planner_request("opening") is None

    asyncio.run(scenario())


def test_issue7_information_request_revalidates_plan_revision(tmp_path):
    async def scenario():
        engine, game, planner = _engine(tmp_path, _InformationPlanner())
        await engine.tick()
        await engine.tick()
        requested = await engine.tick()
        request_id = requested.workflow_tick["planner_request_id"]
        assert (
            requested.workflow_tick["outcome"] == TickOutcomeKind.INFORMATION_REQUESTED
        )
        engine.store.save_plan_bundle(
            "opening",
            1,
            PlanBundle(
                summary="material strategy revision",
                strategy_updates={
                    "expansion_target": {"x": 11, "y": 12},
                    "expansion_target_revision": "changed-after-request",
                },
            ),
            mode=ExecutionMode.AUTO,
            auto_action_types=set(),
        )

        superseded = await engine.tick()
        assert (
            superseded.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        )
        assert (
            engine.store.get_planner_request(request_id).status
            is PlannerRequestStatus.SUPERSEDED
        )
        assert game.query_count == 0
        assert planner.summary.provider_attempts == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "crash_target",
    ["after_provider_attempt_started", "after_provider_call"],
)
def test_issue7_provider_started_history_survives_crash_and_restart(
    tmp_path, crash_target
):
    async def scenario():
        delegate = _HookResolvingPlanner()
        engine, game, planner = _engine(tmp_path, delegate)
        await engine.tick()
        created = await engine.tick()
        request_id = created.workflow_tick["planner_request_id"]
        engine.crash_injector = _Crash(crash_target)
        with pytest.raises(InjectedCrashBoundary):
            await engine.tick()

        attempts = engine.store.list_provider_attempts(request_id)
        assert [item.status for item in attempts] == [ProviderAttemptStatus.STARTED]
        assert (
            engine.store.get_planner_request(request_id).status
            is PlannerRequestStatus.IN_PROGRESS
        )

        restarted = WorkflowEngine(
            store=WorkflowStore(tmp_path / "runtime.sqlite3"),
            game=game,
            planner=planner,
            config=engine.config,
        )
        completed = await restarted.tick()
        assert (
            completed.workflow_tick["outcome"]
            == TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED
        )
        attempts = restarted.store.list_provider_attempts(request_id)
        assert [item.attempt_number for item in attempts] == [1, 2]
        assert [item.status for item in attempts] == [
            ProviderAttemptStatus.ABANDONED,
            ProviderAttemptStatus.SUCCEEDED,
        ]
        assert restarted.store.planner_metrics("opening")["logical_requests"] == 1

    asyncio.run(scenario())


def test_issue7_post_commit_crash_does_not_repeat_provider(tmp_path):
    async def scenario():
        delegate = _HookResolvingPlanner()
        engine, game, planner = _engine(tmp_path, delegate)
        await engine.tick()
        created = await engine.tick()
        request_id = created.workflow_tick["planner_request_id"]
        engine.crash_injector = _Crash("after_provider_attempt_finalized")
        with pytest.raises(InjectedCrashBoundary):
            await engine.tick()

        assert (
            engine.store.get_planner_request(request_id).status
            is PlannerRequestStatus.COMPLETED
        )
        assert (
            engine.store.list_provider_attempts(request_id)[0].status
            is ProviderAttemptStatus.SUCCEEDED
        )
        calls = delegate.calls
        restarted = WorkflowEngine(
            store=WorkflowStore(tmp_path / "runtime.sqlite3"),
            game=game,
            planner=planner,
            config=engine.config,
        )
        await restarted.tick()
        assert delegate.calls == calls

    asyncio.run(scenario())


def test_issue7_effective_lease_baseline_ignores_normal_route_progress(tmp_path):
    async def scenario():
        engine, game, planner = _engine(tmp_path, _ResolvingPlanner())

        await engine.tick()
        await engine.tick()
        completed = await engine.tick()
        assert (
            completed.workflow_tick["outcome"]
            == TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED
        )
        lease = engine.store.list_plan_leases("opening")[0]
        baseline_hash = lease.relevant_input_hash
        engine.store.save_plan_lease(lease.model_copy(update={"valid_until_turn": 1}))

        moved = dict(game.snapshot.units[0])
        moved.update({"x": 6, "y": 7, "moves_remaining": 0})
        game.snapshot = game.snapshot.model_copy(
            update={
                "turn": 2,
                "overview": {
                    **game.snapshot.overview,
                    "turn": 2,
                    "gold": 100,
                    "science_yield": 20,
                    "score": 30,
                },
                "units": [moved],
            }
        )

        reviewed = await engine.tick()
        assert reviewed.workflow_tick["outcome"] == TickOutcomeKind.PLAN_LEASE_UPDATED
        assert (
            reviewed.workflow_tick["validation_result"] == LeaseValidationResult.VALID
        )
        stored_lease = engine.store.list_plan_leases("opening")[0]
        assert stored_lease.status is PlanLeaseStatus.ACTIVE
        assert stored_lease.valid_until_turn == 3
        assert stored_lease.relevant_input_hash == baseline_hash
        stored_gap = engine.store.get_decision_gap(
            "opening", stored_lease.decision_gap_ids[0]
        )
        assert stored_gap.status is DecisionGapStatus.RESOLVED
        assert stored_gap.relevant_input_hash == baseline_hash
        assert planner.summary.logical_requests == 1
        assert planner.summary.provider_attempts == 1

    asyncio.run(scenario())


def test_issue7_three_ordinary_turns_have_zero_planner_activity(tmp_path):
    async def scenario():
        snapshot = RuntimeSnapshot(
            turn=1,
            game_id="stable-game",
            overview={"turn": 1, "num_cities": 1, "num_units": 0},
            cities=[{"city_id": 1, "currently_building": "BUILDING_MONUMENT"}],
            units=[],
            blockers=[],
            tech_civics={
                "current_research": "TECH_MINING",
                "current_civic": "CIVIC_CODE_OF_LAWS",
            },
        )
        game = _Game(snapshot)
        planner = RecordingPlanner(_ResolvingPlanner())
        engine = WorkflowEngine(
            store=WorkflowStore(tmp_path / "ordinary.sqlite3"),
            game=game,
            planner=planner,
            config=EngineConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=False,
                max_agent_calls_per_turn=1,
                auto_action_types={"city_set_production"},
            ),
        )
        engine.store.save_plan_bundle(
            "stable-game",
            1,
            PlanBundle(
                summary="stable progression policy",
                strategy_updates={
                    "revision": "stable-v1",
                    "research_queue": ["TECH_POTTERY"],
                },
                city_plan_updates=[
                    {
                        "city_id": 1,
                        "role": "core",
                        "followup_queue": [
                            {"item_type": "UNIT", "item_name": "UNIT_BUILDER"}
                        ],
                    }
                ],
            ),
            mode=ExecutionMode.AUTO,
            auto_action_types={"city_set_production"},
        )
        for turn in (1, 2, 3):
            game.snapshot = game.snapshot.model_copy(
                update={
                    "turn": turn,
                    "overview": {**game.snapshot.overview, "turn": turn},
                }
            )
            result = await engine.tick()
            assert result.agent_invoked is False
        assert planner.summary.logical_requests == 0
        assert planner.summary.provider_attempts == 0
        assert engine.store.list_decision_gaps("stable-game") == []
        assert engine.store.planner_metrics("stable-game")["logical_requests"] == 0

        game.snapshot = game.snapshot.model_copy(
            update={
                "turn": 4,
                "overview": {**game.snapshot.overview, "turn": 4},
                "cities": [{"city_id": 1, "currently_building": None}],
            }
        )
        created = await engine.tick()
        assert created.workflow_tick["outcome"] == TickOutcomeKind.TASK_CREATED
        task = engine.store.get_task("stable-game", created.workflow_tick["task_id"])
        assert task.action_type == "city_set_production"
        assert task.arguments["item_name"] == "UNIT_BUILDER"
        assert game.mutations == 0
        assert planner.summary.logical_requests == 0
        assert planner.summary.provider_attempts == 0

    asyncio.run(scenario())


def test_issue7_planner_backoff_does_not_block_deterministic_work(tmp_path):
    async def scenario():
        engine, game, planner = _engine(tmp_path, _ResolvingPlanner())
        task = ProposedTask(
            task_id="deterministic-during-backoff",
            action_type="unit_skip",
            entity_type="unit",
            entity_id=7,
            due_turn=1,
            arguments={"unit_id": 7},
            preconditions=[
                {"type": "entity_exists", "entity_type": "unit", "entity_id": 7}
            ],
            postconditions=[{"type": "unit_no_moves", "unit_id": 7}],
            reason="unrelated deterministic work remains runnable",
        )
        engine.store.save_plan_bundle(
            "opening",
            1,
            PlanBundle(summary="deterministic work", tasks=[task]),
            mode=ExecutionMode.AUTO,
            auto_action_types={"unit_skip"},
        )
        engine._set_backoff(
            {"category": "transient_provider_failure", "transient": True}
        )

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == TickOutcomeKind.MUTATION_SENT
        assert result.workflow_tick["task_id"] == task.task_id
        assert game.mutations == 1
        assert planner.summary.logical_requests == 0
        assert planner.summary.provider_attempts == 0
        assert engine._active_backoff() is not None

    asyncio.run(scenario())


class _RetryResolvingPlanner(_HookResolvingPlanner):
    async def plan(self, request):
        for attempt in (1, 2, 3):
            await self.hook(
                "started",
                {
                    "provider_request_id": f"{request.request_id}:{attempt}",
                    "attempt_number": attempt,
                },
            )
            if attempt < 3:
                await self.hook(
                    "failed",
                    {"failure_category": f"retry-{attempt}"},
                )
        return await _ResolvingPlanner.plan(self, request)


class _PreflightFailurePlanner:
    def __init__(self):
        self.hook = None
        self.calls = 0
        self.last_diagnostics = {"attempt_count": 0, "backend": "test"}

    def set_provider_attempt_hook(self, hook):
        self.hook = hook
        return True

    async def plan(self, request):
        self.calls += 1
        raise RuntimeError("provider configuration is invalid before send")


def test_issue7_provider_retries_are_real_durable_attempts(tmp_path):
    async def scenario():
        engine, _, _ = _engine(tmp_path, _RetryResolvingPlanner())
        await engine.tick()
        created = await engine.tick()
        request_id = created.workflow_tick["planner_request_id"]
        completed = await engine.tick()
        assert (
            completed.workflow_tick["outcome"]
            == TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED
        )
        attempts = engine.store.list_provider_attempts(request_id)
        assert [item.attempt_number for item in attempts] == [1, 2, 3]
        assert [item.status for item in attempts] == [
            ProviderAttemptStatus.FAILED,
            ProviderAttemptStatus.FAILED,
            ProviderAttemptStatus.SUCCEEDED,
        ]
        assert len({item.started_at for item in attempts}) == 3
        assert completed.workflow_tick["provider_attempt_count"] == 3

    asyncio.run(scenario())


def test_issue7_provider_preflight_failure_records_zero_attempts(tmp_path):
    async def scenario():
        engine, _, _ = _engine(tmp_path, _PreflightFailurePlanner())
        await engine.tick()
        created = await engine.tick()
        request_id = created.workflow_tick["planner_request_id"]
        failed = await engine.tick()
        assert failed.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
        assert engine.store.list_provider_attempts(request_id) == []
        request = engine.store.get_planner_request(request_id)
        assert request.provider_attempt_count == 0
        assert request.status is PlannerRequestStatus.FAILED

    asyncio.run(scenario())


def test_issue7_v6_phase4_identity_migration_is_idempotent(tmp_path):
    path = tmp_path / "phase4-v6.sqlite3"
    store = WorkflowStore(path)
    now = datetime(2026, 3, 1, tzinfo=UTC)
    gap = _gap()
    group = batch_compatible_gaps("opening", "obs-1", [gap], now=now)
    request = PlannerRequest(
        planner_request_id="request-v6",
        game_session_id="opening",
        turn_number=1,
        observation_id="obs-1",
        decision_gap_ids=(gap.decision_gap_id,),
        decision_group_id=group.decision_group_id,
        input_projection_hash=group.input_projection_hash,
        input_projection={"decision_group_id": group.decision_group_id},
        policy_revision="planner-call-policy/v1",
        model_settings={"provider": "test"},
        status=PlannerRequestStatus.PENDING,
        created_at=now,
    )
    lease = _lease(gap).model_copy(
        update={
            "plan_lease_id": "lease-v6",
            "source_planner_request_id": request.planner_request_id,
        }
    )
    tick = LogicalPlannerRequestCreatedTick(
        tick_id="tick-v6",
        game_session_id="opening",
        turn_number=1,
        starting_runtime_state=RuntimeState.ROUTING,
        observation_ids=("obs-1",),
        started_at=now,
        completed_at=now,
        planner_request_id=request.planner_request_id,
        decision_gap_ids=(gap.decision_gap_id,),
    )
    store.persist_phase4_tick(
        tick,
        decision_gaps=[gap],
        decision_group=group,
        planner_request=request,
        plan_leases=[lease],
    )

    old_gap_id = (
        "gap_" + hashlib.sha256(gap.stable_identity.encode("utf-8")).hexdigest()[:24]
    )
    old_group_id = (
        "group_" + hashlib.sha256(old_gap_id.encode("utf-8")).hexdigest()[:24]
    )
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        gap_json = json.loads(
            conn.execute("SELECT gap_json FROM decision_gaps").fetchone()["gap_json"]
        )
        gap_json["decision_gap_id"] = old_gap_id
        conn.execute(
            "UPDATE decision_gaps SET decision_gap_id=?, gap_json=?",
            (old_gap_id, json.dumps(gap_json)),
        )
        group_json = json.loads(
            conn.execute("SELECT group_json FROM decision_groups").fetchone()[
                "group_json"
            ]
        )
        group_json["decision_group_id"] = old_group_id
        group_json["decision_gap_ids"] = [old_gap_id]
        conn.execute(
            """
            UPDATE decision_groups
            SET decision_group_id=?, decision_gap_ids_json=?, group_json=?
            """,
            (old_group_id, json.dumps([old_gap_id]), json.dumps(group_json)),
        )
        request_json = json.loads(
            conn.execute(
                "SELECT request_json FROM logical_planner_requests"
            ).fetchone()["request_json"]
        )
        request_json["decision_group_id"] = old_group_id
        request_json["decision_gap_ids"] = [old_gap_id]
        conn.execute(
            """
            UPDATE logical_planner_requests
            SET decision_group_id=?, decision_gap_ids_json=?, request_json=?
            """,
            (old_group_id, json.dumps([old_gap_id]), json.dumps(request_json)),
        )
        lease_json = json.loads(
            conn.execute("SELECT lease_json FROM plan_leases").fetchone()["lease_json"]
        )
        lease_json["decision_gap_ids"] = [old_gap_id]
        lease_json.pop("continuation_conditions")
        conn.execute(
            "UPDATE plan_leases SET lease_json=?",
            (json.dumps(lease_json),),
        )
        conn.execute("PRAGMA user_version=6")

    migrated = WorkflowStore(path)
    migrated_gap = migrated.decision_gap_by_identity("opening", gap.stable_identity)
    assert migrated_gap.decision_gap_id == gap.decision_gap_id
    migrated_request = migrated.get_planner_request(request.planner_request_id)
    assert migrated_request.decision_gap_ids == (gap.decision_gap_id,)
    assert migrated_request.decision_group_id == group.decision_group_id
    migrated_lease = migrated.list_plan_leases("opening")[0]
    assert migrated_lease.decision_gap_ids == (gap.decision_gap_id,)
    assert migrated_lease.status is PlanLeaseStatus.AWAITING_INFORMATION
    again = WorkflowStore(path)
    assert again.get_planner_request(request.planner_request_id) == migrated_request
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 7


def _settler_domain_lease_for_completion():
    gap = _gap(1)
    return _lease(gap).model_copy(
        update={
            "continuation_conditions": (
                Condition(
                    condition_type="entity_exists",
                    subject=SubjectRef(subject_type="unit", subject_id="7"),
                ),
                Condition(
                    condition_type="tile_unoccupied",
                    parameters={"x": 8, "y": 9},
                ),
            ),
            "completion_condition": Condition(
                condition_type="all_of",
                parameters={
                    "conditions": [
                        {"type": "unit_absent", "unit_id": "7"},
                        {"type": "city_count_at_least", "count": 2},
                    ]
                },
            ),
            "invalidation_conditions": (
                Condition(condition_type="unit_absent", parameters={"unit_id": "7"}),
                Condition(
                    condition_type="field_in",
                    parameters={
                        "path": "overview.threat_level",
                        "values": ["HIGH", "SEVERE", "CRITICAL"],
                    },
                ),
            ),
        }
    )


def test_issue7_settler_completion_precedes_continuation_and_requires_both_facts():
    lease = _settler_domain_lease_for_completion()

    founded = _snapshot(2).model_copy(
        update={
            "units": [],
            "cities": [
                *_snapshot(2).cities,
                {"city_id": 2, "x": 8, "y": 9, "currently_building": None},
            ],
        }
    )
    completed = evaluate_plan_lease(
        lease,
        normalize_runtime_snapshot(founded),
        relevant_input_hash=lease.relevant_input_hash,
    )
    assert completed.lease.status is PlanLeaseStatus.COMPLETED

    missing_without_city = _snapshot(2).model_copy(update={"units": []})
    invalidated = evaluate_plan_lease(
        lease,
        normalize_runtime_snapshot(missing_without_city),
        relevant_input_hash=lease.relevant_input_hash,
    )
    assert invalidated.lease.status is PlanLeaseStatus.INVALIDATED

    occupied_without_consumption = _snapshot(2).model_copy(
        update={
            "cities": [
                *_snapshot(2).cities,
                {"city_id": 2, "x": 8, "y": 9, "currently_building": None},
            ]
        }
    )
    occupied = evaluate_plan_lease(
        lease,
        normalize_runtime_snapshot(occupied_without_consumption),
        relevant_input_hash=lease.relevant_input_hash,
    )
    assert occupied.lease.status is PlanLeaseStatus.INVALIDATED

    threatened = evaluate_plan_lease(
        lease,
        normalize_runtime_snapshot(_snapshot(2, threat="CRITICAL")),
        relevant_input_hash=lease.relevant_input_hash,
    )
    assert threatened.lease.status is PlanLeaseStatus.INVALIDATED


class _ApprovalPlanner(_ResolvingPlanner):
    async def plan(self, request):
        bundle = await super().plan(request)
        resolution = bundle.event_resolutions[0]
        contract = resolution.lease_contract.model_copy(
            update={"approval_required": True}
        )
        return bundle.model_copy(
            update={
                "event_resolutions": [
                    resolution.model_copy(update={"lease_contract": contract})
                ]
            }
        )


@pytest.mark.parametrize("mode", [ExecutionMode.CONFIRM, ExecutionMode.READONLY])
def test_issue7_model_cannot_activate_lease_in_non_auto_modes(tmp_path, mode):
    async def scenario():
        engine, _, _ = _engine(tmp_path, _ResolvingPlanner())
        engine.config.execution_mode = mode
        await engine.tick()
        await engine.tick()
        result = await engine.tick()
        assert result.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_APPROVAL
        lease = engine.store.list_plan_leases("opening")[0]
        assert lease.status is PlanLeaseStatus.AWAITING_APPROVAL
        assert lease.approval_status is ApprovalStatus.REQUIRED

    asyncio.run(scenario())


def test_issue7_durable_approval_activates_lease_after_restart(tmp_path):
    async def scenario():
        engine, game, planner = _engine(tmp_path, _ApprovalPlanner())
        await engine.tick()
        await engine.tick()
        waiting = await engine.tick()
        assert waiting.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_APPROVAL
        lease = engine.store.list_plan_leases("opening")[0]
        assert lease.status is PlanLeaseStatus.AWAITING_APPROVAL
        assert "7" in engine.store.current_context("opening")["units"]

        approval = ApprovalRecord(
            approval_id="approval-settler-route",
            proposal_type="decision_gap",
            proposal_id=lease.decision_gap_ids[0],
            proposal_revision=lease.plan_revision,
            decision=ApprovalDecision.APPROVED,
            actor="reviewer",
            created_at=datetime.now(UTC),
        )
        engine.store.save_approval_record("opening", approval)

        restarted = WorkflowEngine(
            store=WorkflowStore(tmp_path / "runtime.sqlite3"),
            game=game,
            planner=planner,
            config=engine.config,
        )
        activated = await restarted.tick()
        assert activated.workflow_tick["outcome"] == TickOutcomeKind.PLAN_LEASE_UPDATED
        stored = restarted.store.list_plan_leases("opening")[0]
        assert stored.status is PlanLeaseStatus.ACTIVE
        assert stored.approval_status is ApprovalStatus.APPROVED
        assert (
            restarted.store.latest_approval_record(
                "opening",
                proposal_type="decision_gap",
                proposal_id=lease.decision_gap_ids[0],
                proposal_revision=lease.plan_revision,
            )
            == approval
        )
        assert planner.summary.logical_requests == 1

    asyncio.run(scenario())


def _city_lease_contract(city_id):
    return LeaseContract(
        valid_until_turn=10,
        preconditions=[
            {"type": "entity_exists", "entity_type": "city", "entity_id": city_id}
        ],
        continuation_conditions=[
            {"type": "entity_exists", "entity_type": "city", "entity_id": city_id}
        ],
        completion_condition={"type": "turn_equals", "turn": 999},
        invalidation_conditions=[{"type": "turn_equals", "turn": 999}],
        review_conditions=[{"type": "turn_at_least", "turn": 10}],
        continuation_policy=ContinuationPolicy.EXTEND_WHEN_INPUT_UNCHANGED,
        covered_slots=["city_production"],
        subjects=[{"subject_type": "city", "subject_id": str(city_id)}],
    )


class _PartialCityPlanner:
    def __init__(self):
        self.gap_by_key = {}
        self.calls = 0
        self.last_diagnostics = None

    async def plan(self, request):
        self.calls += 1
        self.last_diagnostics = {"attempt_count": 1, "backend": "test"}
        tasks = []
        resolutions = []
        plans = []
        for event in request.trigger_events:
            city_id = str(event.entity_id)
            task = ProposedTask(
                task_id=f"produce-{city_id}",
                action_type="city_set_production",
                entity_type="city",
                entity_id=city_id,
                due_turn=request.turn,
                arguments={
                    "city_id": city_id,
                    "item_type": "UNIT",
                    "item_name": "UNIT_BUILDER",
                },
                preconditions=[{"type": "city_has_no_production", "city_id": city_id}],
                postconditions=[
                    {
                        "type": "city_production_equals",
                        "city_id": city_id,
                        "item_name": "UNIT_BUILDER",
                    }
                ],
                reason=f"materialize city {city_id} plan",
            )
            tasks.append(task)
            plans.append(
                {
                    "city_id": city_id,
                    "role": "production",
                    "followup_queue": ["UNIT_BUILDER"],
                }
            )
            contract_city_id = city_id if city_id == "A" else "missing-city"
            resolutions.append(
                EventResolution(
                    event_dedupe_key=event.dedupe_key,
                    decision_gap_ids=[self.gap_by_key[event.dedupe_key]],
                    disposition=ResolutionDisposition.TASK,
                    task_ids=[task.task_id],
                    plan_refs=[f"city:{city_id}"],
                    reason=f"city {city_id} production policy",
                    lease_contract=_city_lease_contract(contract_city_id),
                )
            )
        return WorkflowPlanBundle(
            plan_id="partial-city-plan",
            summary="independent city decisions",
            city_plan_updates=plans,
            tasks=tasks,
            event_resolutions=resolutions,
        )


def test_issue7_independent_gap_items_commit_partial_success(tmp_path):
    async def scenario():
        snapshot = RuntimeSnapshot(
            turn=1,
            game_id="opening",
            overview={"turn": 1, "num_cities": 2, "num_units": 0},
            cities=[
                {"city_id": "A", "currently_building": None},
                {"city_id": "B", "currently_building": None},
            ],
            units=[],
            blockers=[],
        )
        game = _Game(snapshot)
        delegate = _PartialCityPlanner()
        planner = RecordingPlanner(delegate)
        engine = WorkflowEngine(
            store=WorkflowStore(tmp_path / "partial.sqlite3"),
            game=game,
            planner=planner,
            config=EngineConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=False,
                max_agent_calls_per_turn=1,
            ),
        )
        events = [
            GameEvent(
                event_type="city_role_required",
                turn=1,
                entity_type="city",
                entity_id=city_id,
                blocking=True,
                dedupe_key=f"city-role:{city_id}",
            )
            for city_id in ("A", "B")
        ]
        context = engine.store.current_context("opening")
        context["execution_mode"] = ExecutionMode.AUTO.value
        gaps = [
            build_decision_gap(
                "opening",
                "obs-partial",
                snapshot,
                event,
                context,
                now=datetime.now(UTC),
            )
            for event in events
        ]
        group = batch_compatible_gaps("opening", "obs-partial", gaps)
        logical_id = "logical-partial"
        delegate.gap_by_key = {
            event.dedupe_key: gap.decision_gap_id
            for event, gap in zip(events, gaps, strict=True)
        }
        provider_request = engine._build_agent_request(snapshot, events)
        provider_request = provider_request.model_copy(
            update={
                "constraints": {
                    **provider_request.constraints,
                    "decision_gap_ids": list(group.decision_gap_ids),
                }
            }
        )
        requested_gaps = [
            gap.model_copy(
                update={
                    "status": DecisionGapStatus.REQUESTED,
                    "logical_request_id": logical_id,
                }
            )
            for gap in gaps
        ]
        approval_contract = [
            gap.model_dump(mode="json")["input_projection"]["approval"]
            for gap in requested_gaps
        ]
        request = PlannerRequest(
            planner_request_id=logical_id,
            game_session_id="opening",
            turn_number=1,
            observation_id="obs-partial",
            decision_gap_ids=group.decision_gap_ids,
            decision_group_id=group.decision_group_id,
            input_projection_hash=group.input_projection_hash,
            input_projection={
                "decision_group_id": group.decision_group_id,
                "gaps": [
                    gap.model_dump(mode="json")["input_projection"]
                    for gap in requested_gaps
                ],
            },
            request_payload=provider_request.model_dump(mode="json"),
            plan_revision_refs=tuple(
                revision
                for gap in requested_gaps
                for revision in gap.relevant_plan_revisions
            ),
            policy_revision="planner-call-policy/v1",
            approval_contract_hash=engine.planner_lifecycle._contract_hash(
                approval_contract
            ),
            allowed_actions_hash=engine.planner_lifecycle._contract_hash(
                sorted(engine.config.allowed_action_types)
            ),
            model_settings={"provider": "test"},
            status=PlannerRequestStatus.PENDING,
            created_at=datetime.now(UTC),
        )
        for gap in requested_gaps:
            engine.store.save_decision_gap(gap, turn=1)
        engine.store.save_planner_request(request)

        result = await engine.tick()
        assert (
            result.workflow_tick["outcome"] == TickOutcomeKind.PLANNER_ATTEMPT_COMPLETED
        )
        stored_request = engine.store.get_planner_request(logical_id)
        assert stored_request.status is PlannerRequestStatus.PARTIALLY_COMPLETED
        stored_gaps = {
            gap.scope: gap for gap in engine.store.list_decision_gaps("opening")
        }
        assert stored_gaps["city:A"].status is DecisionGapStatus.RESOLVED
        assert stored_gaps["city:B"].status is DecisionGapStatus.AWAITING_HUMAN
        assert engine.store.get_task("opening", "produce-A") is not None
        assert engine.store.get_task("opening", "produce-B") is None
        assert len(engine.store.list_plan_leases("opening")) == 1
        context = engine.store.current_context("opening")
        assert "A" in context["cities"]
        assert "B" not in context["cities"]
        assert planner.summary.logical_requests == 1

    asyncio.run(scenario())


class _HighRiskTaskPlanner(_ResolvingPlanner):
    async def plan(self, request):
        bundle = await super().plan(request)
        task = ProposedTask(
            task_id="high-risk-settler-move",
            action_type="unit_move",
            entity_type="unit",
            entity_id=7,
            due_turn=request.turn,
            arguments={"unit_id": 7, "target_x": 8, "target_y": 9},
            preconditions=[
                {"type": "entity_exists", "entity_type": "unit", "entity_id": 7},
                {"type": "unit_has_moves", "unit_id": 7},
            ],
            postconditions=[{"type": "unit_moved_from", "unit_id": 7, "x": 4, "y": 5}],
            risk=RiskLevel.HIGH,
            requires_confirmation=True,
            reason="move settler along proposed route",
        )
        resolution = bundle.event_resolutions[0].model_copy(
            update={
                "disposition": ResolutionDisposition.TASK,
                "task_ids": [task.task_id],
            }
        )
        return bundle.model_copy(
            update={"tasks": [task], "event_resolutions": [resolution]}
        )


def test_issue7_high_risk_task_requires_runtime_approval(tmp_path):
    async def scenario():
        engine, _, _ = _engine(tmp_path, _HighRiskTaskPlanner())
        await engine.tick()
        await engine.tick()
        result = await engine.tick()
        assert result.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_APPROVAL
        lease = engine.store.list_plan_leases("opening")[0]
        assert lease.status is PlanLeaseStatus.AWAITING_APPROVAL
        assert lease.approval_status is ApprovalStatus.REQUIRED
        task = engine.store.get_task("opening", "high-risk-settler-move")
        assert task.status.value == "awaiting_confirmation"

    asyncio.run(scenario())


def test_issue7_same_plan_task_cancellation_is_scope_local(tmp_path):
    store = WorkflowStore(tmp_path / "same-plan-scope.sqlite3")
    snapshot = RuntimeSnapshot(
        turn=1,
        game_id="opening",
        overview={"turn": 1, "num_cities": 2, "num_units": 1},
        cities=[
            {"city_id": 1, "currently_building": None},
            {"city_id": 2, "currently_building": None},
        ],
        units=[{"unit_id": 7, "unit_type": "UNIT_WARRIOR", "moves_remaining": 2}],
        blockers=[],
    )
    engine = WorkflowEngine(
        store=store,
        game=_Game(snapshot),
        planner=RecordingPlanner(_ResolvingPlanner()),
        config=EngineConfig(execution_mode=ExecutionMode.AUTO, auto_end_turn=False),
    )
    tasks = [
        ProposedTask(
            task_id="scope-city-a",
            action_type="city_set_production",
            entity_type="city",
            entity_id=1,
            due_turn=1,
            reason="city A",
        ),
        ProposedTask(
            task_id="scope-city-b",
            action_type="city_set_production",
            entity_type="city",
            entity_id=2,
            due_turn=1,
            reason="city B",
        ),
        ProposedTask(
            task_id="scope-research",
            action_type="set_research",
            entity_type="research",
            entity_id="TECH_MINING",
            due_turn=1,
            reason="research",
        ),
        ProposedTask(
            task_id="scope-unit",
            action_type="unit_skip",
            entity_type="unit",
            entity_id=7,
            due_turn=1,
            reason="unit",
        ),
    ]
    plan_id = "shared-scope-plan"
    store.save_plan_bundle(
        "opening",
        1,
        PlanBundle(
            plan_id=plan_id,
            summary="four independent scopes",
            strategy_updates={"research_queue": ["TECH_MINING"]},
            city_plan_updates=[
                {"city_id": 1, "role": "production"},
                {"city_id": 2, "role": "science"},
            ],
            unit_plan_updates=[{"unit_id": 7, "goal": "defend"}],
            tasks=tasks,
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={
            "city_set_production",
            "set_research",
            "unit_skip",
        },
    )
    gap = _gap(1)
    lease = _lease(gap, scope="city:1").model_copy(
        update={
            "plan_id": plan_id,
            "subjects": (SubjectRef(subject_type="city", subject_id="1"),),
            "covered_slots": ("city_production",),
            "task_ids": ("scope-city-a",),
            "status": PlanLeaseStatus.INVALIDATED,
            "last_validation_result": LeaseValidationResult.INVALIDATED,
            "invalidation_reason": "city A policy changed",
        }
    )
    cancel_ids = engine.planner_lifecycle._dependent_task_ids(lease)
    assert cancel_ids == ("scope-city-a",)
    now = datetime.now(UTC)
    tick = PlanLeaseUpdatedTick(
        tick_id="tick-same-plan-scope",
        game_session_id="opening",
        turn_number=1,
        starting_runtime_state=RuntimeState.ROUTING,
        observation_ids=("obs-same-plan",),
        started_at=now,
        completed_at=now,
        plan_lease_id=lease.plan_lease_id,
        validation_result=LeaseValidationResult.INVALIDATED.value,
    )
    store.persist_phase4_tick(
        tick,
        plan_leases=[lease],
        cancel_task_ids=cancel_ids,
    )

    assert store.task_status("opening", "scope-city-a").value == "cancelled"
    for task_id in ("scope-city-b", "scope-research", "scope-unit"):
        assert store.task_status("opening", task_id).value == "ready"
    context = store.current_context("opening")
    assert "1" not in context["cities"]
    assert "2" in context["cities"]
    assert "7" in context["units"]
    assert context["strategy"]["research_queue"] == ["TECH_MINING"]


def test_issue7_terminal_request_atomically_abandons_started_attempt(tmp_path):
    store = WorkflowStore(tmp_path / "terminal-provider.sqlite3")
    now = datetime.now(UTC)
    gap = _gap(1)
    group = batch_compatible_gaps("opening", gap.observation_id, [gap], now=now)
    request = PlannerRequest(
        planner_request_id="logical-provider-terminal",
        game_session_id="opening",
        turn_number=1,
        observation_id=gap.observation_id,
        decision_gap_ids=(gap.decision_gap_id,),
        decision_group_id=group.decision_group_id,
        input_projection_hash=group.input_projection_hash,
        input_projection={"decision_group_id": group.decision_group_id},
        policy_revision="planner-call-policy/v1",
        model_settings={"provider": "test"},
        status=PlannerRequestStatus.PENDING,
        created_at=now,
    )
    store.save_decision_gap(gap, turn=1)
    store.save_planner_request(request)
    attempt = ProviderAttempt(
        provider_attempt_id="provider-started-terminal",
        planner_request_id=request.planner_request_id,
        attempt_number=1,
        provider_request_id="provider-wire-1",
        status=ProviderAttemptStatus.STARTED,
        started_at=now,
    )
    in_progress = store.start_provider_attempt("opening", request, attempt)
    terminal = in_progress.model_copy(
        update={
            "status": PlannerRequestStatus.SUPERSEDED,
            "completed_at": datetime.now(UTC),
            "failure_category": "decision_input_changed",
        }
    )

    store.save_planner_request(terminal)

    stored_request = store.get_planner_request(request.planner_request_id)
    stored_attempt = store.list_provider_attempts(request.planner_request_id)[0]
    assert stored_request.status is PlannerRequestStatus.SUPERSEDED
    assert stored_attempt.status is ProviderAttemptStatus.ABANDONED
    assert stored_attempt.completed_at is not None
    assert stored_attempt.failure_category == "decision_input_changed"


def test_issue7_material_path_change_invalidates_active_lease_immediately(tmp_path):
    async def scenario():
        engine, game, planner = _engine(tmp_path, _ResolvingPlanner())
        await engine.tick()
        await engine.tick()
        await engine.tick()
        lease = engine.store.list_plan_leases("opening")[0]
        assert lease.valid_until_turn > game.snapshot.turn
        baseline_hash = lease.relevant_input_hash
        dependent = ProposedTask(
            task_id="approved-route-step",
            action_type="unit_skip",
            entity_type="unit",
            entity_id=7,
            due_turn=1,
            arguments={"unit_id": 7},
            preconditions=[
                {"type": "entity_exists", "entity_type": "unit", "entity_id": 7}
            ],
            postconditions=[{"type": "unit_no_moves", "unit_id": 7}],
            reason="continue the approved settler route",
        )
        engine.store.save_plan_bundle(
            "opening",
            1,
            PlanBundle(summary="leased route task", tasks=[dependent]),
            mode=ExecutionMode.AUTO,
            auto_action_types={"unit_skip"},
        )
        changed_unit = dict(game.snapshot.units[0])
        changed_unit["targets"] = [{"x": 8, "y": 9, "legal": True, "reachable": False}]
        game.snapshot = game.snapshot.model_copy(update={"units": [changed_unit]})

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == TickOutcomeKind.PLAN_LEASE_UPDATED
        assert result.metrics.mutation_count == 0
        assert game.mutations == 0
        stored_lease = engine.store.list_plan_leases("opening")[0]
        assert stored_lease.status is PlanLeaseStatus.INVALIDATED
        stored_gap = engine.store.get_decision_gap(
            "opening", stored_lease.decision_gap_ids[0]
        )
        assert stored_gap.status is DecisionGapStatus.OPEN
        assert stored_gap.relevant_input_hash != baseline_hash
        assert (
            engine.store.task_status("opening", dependent.task_id).value == "cancelled"
        )
        assert planner.summary.logical_requests == 1

    asyncio.run(scenario())


def test_issue7_settler_completion_uses_runtime_bound_city_baseline(tmp_path):
    async def scenario():
        engine, game, _ = _engine(tmp_path, _ResolvingPlanner())
        game.snapshot = _snapshot(1)
        await engine.tick()
        await engine.tick()
        await engine.tick()
        lease = engine.store.list_plan_leases("opening")[0]
        baseline = lease.model_dump(mode="json")["contract_baseline"]
        assert baseline == {
            **baseline,
            "baseline_city_count": 1,
            "approved_target": {"x": 8, "y": 9},
            "owner": 1,
            "settler_unit_id": "7",
        }
        completion = lease.completion_condition.model_dump(mode="json")
        nested = completion["parameters"]["conditions"]
        assert {"type": "city_count_at_least", "count": 2} in nested
        assert {
            "type": "city_at_target",
            "x": 8,
            "y": 9,
            "owner": 1,
        } in nested
        projection = baseline["relevant_input_projection"]

        destroyed = _snapshot(2).model_copy(update={"units": []})
        destroyed_result = evaluate_plan_lease(
            lease,
            normalize_runtime_snapshot(destroyed),
            relevant_input_hash=lease.relevant_input_hash,
            relevant_input_projection=projection,
        )
        assert destroyed_result.lease.status is PlanLeaseStatus.INVALIDATED

        founded = destroyed.model_copy(
            update={
                "cities": [
                    *destroyed.cities,
                    {
                        "city_id": 2,
                        "owner": 1,
                        "x": 8,
                        "y": 9,
                        "currently_building": None,
                    },
                ]
            }
        )
        founded_result = evaluate_plan_lease(
            lease,
            normalize_runtime_snapshot(founded),
            relevant_input_hash=lease.relevant_input_hash,
            relevant_input_projection=projection,
        )
        assert founded_result.lease.status is PlanLeaseStatus.COMPLETED

        wrong_site = destroyed.model_copy(
            update={
                "cities": [
                    *destroyed.cities,
                    {
                        "city_id": 2,
                        "owner": 1,
                        "x": 12,
                        "y": 13,
                        "currently_building": None,
                    },
                ]
            }
        )
        wrong_site_result = evaluate_plan_lease(
            lease,
            normalize_runtime_snapshot(wrong_site),
            relevant_input_hash=lease.relevant_input_hash,
            relevant_input_projection=projection,
        )
        assert wrong_site_result.lease.status is PlanLeaseStatus.INVALIDATED

        occupied = _snapshot(2).model_copy(
            update={
                "cities": [
                    *_snapshot(2).cities,
                    {
                        "city_id": 2,
                        "owner": 2,
                        "x": 8,
                        "y": 9,
                        "currently_building": None,
                    },
                ]
            }
        )
        occupied_result = evaluate_plan_lease(
            lease,
            normalize_runtime_snapshot(occupied),
            relevant_input_hash=lease.relevant_input_hash,
            relevant_input_projection=projection,
        )
        assert occupied_result.lease.status is PlanLeaseStatus.INVALIDATED

    asyncio.run(scenario())


def test_issue7_turn_specific_tactical_gap_expires_without_reconstruction(tmp_path):
    async def scenario():
        engine, game, planner = _engine(tmp_path, _ResolvingPlanner())
        tactical = GameEvent(
            event_type="tactical_attack_opportunity",
            turn=1,
            entity_type="unit",
            entity_id=7,
            blocking=True,
            dedupe_key="tactical:7:1",
        )
        gap = build_decision_gap(
            "opening",
            "obs-tactical-1",
            game.snapshot,
            tactical,
            {"strategy": {}},
            now=datetime.now(UTC),
        ).model_copy(
            update={
                "status": DecisionGapStatus.RESOLVED,
                "resolution_reason": "approved one-turn attack",
            }
        )
        lease = _lease(gap, scope="unit:7", until=5).model_copy(
            update={
                "decision_gap_ids": (gap.decision_gap_id,),
                "relevant_input_hash": gap.relevant_input_hash,
            }
        )
        task = ProposedTask(
            task_id="turn-one-tactical-task",
            action_type="unit_skip",
            entity_type="unit",
            entity_id=7,
            due_turn=2,
            arguments={"unit_id": 7},
            preconditions=[
                {"type": "entity_exists", "entity_type": "unit", "entity_id": 7}
            ],
            postconditions=[{"type": "unit_no_moves", "unit_id": 7}],
            reason="old tactical action",
        )
        engine.store.save_decision_gap(gap, turn=1)
        engine.store.save_plan_lease(lease)
        engine.store.save_plan_bundle(
            "opening",
            1,
            PlanBundle(summary="one-turn tactic", tasks=[task]),
            mode=ExecutionMode.AUTO,
            auto_action_types={"unit_skip"},
        )
        game.snapshot = game.snapshot.model_copy(
            update={
                "turn": 2,
                "overview": {**game.snapshot.overview, "turn": 2},
            }
        )

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == TickOutcomeKind.PLAN_LEASE_UPDATED
        assert result.metrics.mutation_count == 0
        assert game.mutations == 0
        assert planner.summary.provider_attempts == 0
        stored = engine.store.get_decision_gap("opening", gap.decision_gap_id)
        assert stored.status is DecisionGapStatus.INVALIDATED
        assert (
            engine.store.list_plan_leases("opening")[0].status
            is PlanLeaseStatus.EXPIRED
        )
        assert engine.store.task_status("opening", task.task_id).value == "cancelled"
        tactical_gaps = [
            item
            for item in engine.store.list_decision_gaps("opening")
            if item.gap_type == "tactical_attack_opportunity"
        ]
        assert [item.decision_gap_id for item in tactical_gaps] == [gap.decision_gap_id]

    asyncio.run(scenario())


class _AtomicCityPlanner:
    def __init__(self):
        self.gap_ids = ()
        self.calls = 0
        self.last_diagnostics = None

    async def plan(self, request):
        self.calls += 1
        self.last_diagnostics = {"attempt_count": 1, "backend": "test"}
        tasks = [
            ProposedTask(
                task_id="atomic-produce-A",
                action_type="city_set_production",
                entity_type="city",
                entity_id="A",
                due_turn=request.turn,
                arguments={
                    "city_id": "A",
                    "item_type": "UNIT",
                    "item_name": "UNIT_BUILDER",
                },
                preconditions=[{"type": "city_has_no_production", "city_id": "A"}],
                postconditions=[
                    {
                        "type": "city_production_equals",
                        "city_id": "A",
                        "item_name": "UNIT_BUILDER",
                    }
                ],
                reason="valid atomic item",
            ),
            ProposedTask(
                task_id="atomic-produce-B",
                action_type="unsupported_atomic_action",
                entity_type="city",
                entity_id="B",
                due_turn=request.turn,
                arguments={"city_id": "B"},
                reason="invalid atomic item",
            ),
        ]
        return WorkflowPlanBundle(
            plan_id="atomic-city-plan",
            summary="atomic city decisions",
            city_plan_updates=[
                {"city_id": "A", "role": "production"},
                {"city_id": "B", "role": "production"},
            ],
            tasks=tasks,
            event_resolutions=[
                EventResolution(
                    event_dedupe_key=request.trigger_events[0].dedupe_key,
                    decision_gap_ids=list(self.gap_ids),
                    disposition=ResolutionDisposition.TASK,
                    task_ids=[task.task_id for task in tasks],
                    plan_refs=["city:A", "city:B"],
                    reason="both city decisions are atomic",
                    lease_contract=_city_lease_contract("A"),
                    atomic=True,
                )
            ],
        )


def test_issue7_atomic_multi_gap_failure_persists_no_partial_outputs(tmp_path):
    async def scenario():
        snapshot = RuntimeSnapshot(
            turn=1,
            game_id="opening",
            overview={"turn": 1, "num_cities": 2, "num_units": 0},
            cities=[
                {"city_id": "A", "currently_building": None},
                {"city_id": "B", "currently_building": None},
            ],
            units=[],
            blockers=[],
        )
        game = _Game(snapshot)
        delegate = _AtomicCityPlanner()
        planner = RecordingPlanner(delegate)
        engine = WorkflowEngine(
            store=WorkflowStore(tmp_path / "atomic.sqlite3"),
            game=game,
            planner=planner,
            config=EngineConfig(
                execution_mode=ExecutionMode.AUTO,
                auto_end_turn=False,
                max_agent_calls_per_turn=1,
            ),
        )
        events = [
            GameEvent(
                event_type="city_role_required",
                turn=1,
                entity_type="city",
                entity_id=city_id,
                blocking=True,
                dedupe_key=f"atomic-city-role:{city_id}",
            )
            for city_id in ("A", "B")
        ]
        context = engine.store.current_context("opening")
        context["execution_mode"] = ExecutionMode.AUTO.value
        gaps = [
            build_decision_gap(
                "opening",
                "obs-atomic",
                snapshot,
                event,
                context,
                now=datetime.now(UTC),
            )
            for event in events
        ]
        group = batch_compatible_gaps("opening", "obs-atomic", gaps)
        delegate.gap_ids = group.decision_gap_ids
        logical_id = "logical-atomic"
        provider_request = engine._build_agent_request(snapshot, events).model_copy(
            update={
                "constraints": {
                    **engine._build_agent_request(snapshot, events).constraints,
                    "decision_gap_ids": list(group.decision_gap_ids),
                }
            }
        )
        requested_gaps = [
            gap.model_copy(
                update={
                    "status": DecisionGapStatus.REQUESTED,
                    "logical_request_id": logical_id,
                }
            )
            for gap in gaps
        ]
        request = PlannerRequest(
            planner_request_id=logical_id,
            game_session_id="opening",
            turn_number=1,
            observation_id="obs-atomic",
            decision_gap_ids=group.decision_gap_ids,
            decision_group_id=group.decision_group_id,
            input_projection_hash=group.input_projection_hash,
            input_projection={
                "decision_group_id": group.decision_group_id,
                "gaps": [
                    gap.model_dump(mode="json")["input_projection"]
                    for gap in requested_gaps
                ],
            },
            request_payload=provider_request.model_dump(mode="json"),
            plan_revision_refs=(),
            policy_revision="planner-call-policy/v1",
            approval_contract_hash=engine.planner_lifecycle._contract_hash(
                [
                    gap.model_dump(mode="json")["input_projection"]["approval"]
                    for gap in requested_gaps
                ]
            ),
            allowed_actions_hash=engine.planner_lifecycle._contract_hash(
                sorted(engine.config.allowed_action_types)
            ),
            model_settings={"provider": "test"},
            status=PlannerRequestStatus.PENDING,
            created_at=datetime.now(UTC),
        )
        for gap in requested_gaps:
            engine.store.save_decision_gap(gap, turn=1)
        engine.store.save_planner_request(request)

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == TickOutcomeKind.AWAITING_HUMAN
        stored_request = engine.store.get_planner_request(logical_id)
        assert stored_request.status is PlannerRequestStatus.REJECTED
        assert all(
            gap.status is DecisionGapStatus.AWAITING_HUMAN
            for gap in engine.store.list_decision_gaps("opening")
        )
        assert engine.store.get_task("opening", "atomic-produce-A") is None
        assert engine.store.get_task("opening", "atomic-produce-B") is None
        assert engine.store.list_plan_leases("opening") == []
        assert engine.store.current_context("opening")["cities"] == {}
        assert planner.summary.logical_requests == 1

    asyncio.run(scenario())


def test_issue7_turn_specific_active_request_expires_before_provider_call(tmp_path):
    async def scenario():
        engine, game, planner = _engine(tmp_path, _ResolvingPlanner())
        event = GameEvent(
            event_type="tactical_attack_opportunity",
            turn=1,
            entity_type="unit",
            entity_id=7,
            blocking=True,
            dedupe_key="tactical-request:7:1",
        )
        gap = build_decision_gap(
            "opening",
            "obs-tactical-request",
            game.snapshot,
            event,
            {"strategy": {}},
            now=datetime.now(UTC),
        )
        group = batch_compatible_gaps("opening", "obs-tactical-request", [gap])
        request_id = "logical-tactical-request"
        requested_gap = gap.model_copy(
            update={
                "status": DecisionGapStatus.REQUESTED,
                "logical_request_id": request_id,
            }
        )
        provider_request = engine._build_agent_request(
            game.snapshot, [event]
        ).model_copy(
            update={
                "constraints": {
                    **engine._build_agent_request(game.snapshot, [event]).constraints,
                    "decision_gap_ids": [gap.decision_gap_id],
                }
            }
        )
        request = PlannerRequest(
            planner_request_id=request_id,
            game_session_id="opening",
            turn_number=1,
            observation_id="obs-tactical-request",
            decision_gap_ids=(gap.decision_gap_id,),
            decision_group_id=group.decision_group_id,
            input_projection_hash=group.input_projection_hash,
            input_projection=gap.model_dump(mode="json")["input_projection"],
            request_payload=provider_request.model_dump(mode="json"),
            plan_revision_refs=gap.relevant_plan_revisions,
            policy_revision="planner-call-policy/v1",
            approval_contract_hash=engine.planner_lifecycle._contract_hash(
                [gap.model_dump(mode="json")["input_projection"]["approval"]]
            ),
            allowed_actions_hash=engine.planner_lifecycle._contract_hash(
                sorted(engine.config.allowed_action_types)
            ),
            model_settings={"provider": "test"},
            status=PlannerRequestStatus.PENDING,
            created_at=datetime.now(UTC),
        )
        engine.store.save_decision_gap(requested_gap, turn=1)
        engine.store.save_planner_request(request)
        game.snapshot = game.snapshot.model_copy(
            update={
                "turn": 2,
                "overview": {**game.snapshot.overview, "turn": 2},
            }
        )

        result = await engine.tick()

        assert result.workflow_tick["outcome"] == TickOutcomeKind.DECISION_GAP_UPDATED
        assert (
            engine.store.get_planner_request(request_id).status
            is PlannerRequestStatus.SUPERSEDED
        )
        assert (
            engine.store.get_decision_gap("opening", gap.decision_gap_id).status
            is DecisionGapStatus.INVALIDATED
        )
        assert planner.summary.logical_requests == 0
        assert engine.store.list_provider_attempts(request_id) == []
        assert game.mutations == 0

    asyncio.run(scenario())
