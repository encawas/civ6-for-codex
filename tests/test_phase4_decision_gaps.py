import asyncio
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
    DecisionGapStatus,
    LeaseValidationResult,
    PlanLease,
    PlanLeaseStatus,
    RuntimeState,
    TickOutcomeKind,
)
from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    EventLevel,
    ExecutionMode,
    GameEvent,
    RiskLevel,
    RuntimeSnapshot,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore
from civ6_workflow.workflow_protocol import (
    EventResolution,
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
        },
        cities=[{"city_id": 1, "currently_building": "UNIT_SCOUT"}],
        units=[
            {
                "unit_id": 7,
                "unit_type": "UNIT_SETTLER",
                "moves_remaining": 2,
                "x": 4,
                "y": 5,
                "unrelated_extension": targets,
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
    assert first["projection_version"] == "decision-input/v1"
    assert "unrelated_raw_field" not in first["overview"]
    assert "unrelated" not in first["event_facts"]

    target_changed = _gap(
        2,
        event=_settler_event(2, targets=[{"x": 10, "y": 11}]),
    )
    threat_changed = _gap(2, snapshot=_snapshot(2, threat="HIGH"))
    plan_changed = _gap(2, context={"strategy": {"revision": 4}})
    baseline = _gap(1)
    assert target_changed.relevant_input_hash != baseline.relevant_input_hash
    assert threat_changed.relevant_input_hash != baseline.relevant_input_hash
    assert plan_changed.relevant_input_hash != baseline.relevant_input_hash


def _lease(gap, *, scope="unit:7", until=5, input_hash=None):
    return PlanLease(
        plan_lease_id=f"lease:{scope}",
        plan_id=f"plan:{scope}",
        game_session_id="opening",
        decision_gap_ids=(gap.decision_gap_id,),
        scope=scope,
        subjects=gap.subjects,
        covered_slots=("unit_route",),
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
        overview={"turn": 1, "num_cities": 0, "num_units": 1},
        cities=[],
        units=[
            {
                "unit_id": 7,
                "unit_type": "UNIT_SETTLER",
                "moves_remaining": 2,
                "x": 4,
                "y": 5,
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
        )
        assert planner.summary.logical_requests == 1
        assert planner.summary.provider_attempts == 1
        assert game.mutations == 0

        metrics = engine.store.planner_metrics("opening")
        assert metrics["logical_requests"] == 1
        assert metrics["provider_attempts"] == 1
        assert len(engine.store.list_plan_leases("opening")) == 1
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
