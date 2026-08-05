from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


runtime_path = "src/civ6_workflow/runtime.py"
replace_once(
    runtime_path,
    """        if ctx.starting_state in {RuntimeState.SYSTEM_ERROR, RuntimeState.PAUSED}:\n""",
    """        if rewind_pending:\n            rewind_event = recover_turn_rewind(\n                self.store,\n                snapshot,\n                previous_game_id=previous_game_id,\n                previous_turn=previous_turn,\n                recovered_at=observation.canonical.observed_at,\n            )\n            reason = (\n                \"the loaded save predates the active strategic timeline; \"\n                \"automatic planning and mutation are disabled until strategic \"\n                \"authority is explicitly reset\"\n            )\n            compatibility = TickResult(\n                turn=snapshot.turn,\n                metrics=ctx.metrics,\n                events=[] if rewind_event is None else [rewind_event],\n                paused=True,\n                pause_reason=reason,\n            )\n            return self._finish(\n                ctx,\n                snapshot,\n                AwaitingHumanTick,\n                compatibility=compatibility,\n                blocking_reason=reason,\n                human_wait_context_override={\n                    \"version\": \"human-wait/v1\",\n                    \"wait_kind\": \"turn_rewind_requires_strategic_reset\",\n                    \"resume_policy\": \"explicit_reset_only\",\n                    \"previous_turn\": previous_turn,\n                    \"loaded_turn\": snapshot.turn,\n                    \"resume_requested\": False,\n                },\n            )\n        if ctx.starting_state in {RuntimeState.SYSTEM_ERROR, RuntimeState.PAUSED}:\n""",
)
replace_once(
    runtime_path,
    """        if ctx.starting_state is RuntimeState.AWAITING_HUMAN:\n            wait = self.store.human_wait_context(snapshot.game_id) or {}\n            if (\n""",
    """        if ctx.starting_state is RuntimeState.AWAITING_HUMAN:\n            wait = self.store.human_wait_context(snapshot.game_id) or {}\n            if wait.get(\"wait_kind\") == \"turn_rewind_requires_strategic_reset\":\n                return self._finish(\n                    ctx,\n                    snapshot,\n                    AwaitingHumanTick,\n                    blocking_reason=(\n                        \"the loaded save invalidated the active strategic timeline; \"\n                        \"start a fresh workflow state or perform an explicit \"\n                        \"strategic reset before automation resumes\"\n                    ),\n                )\n            if (\n""",
)

planner_path = "src/civ6_workflow/planner_lifecycle.py"
replace_once(
    planner_path,
    """    AwaitingHumanTick,\n""",
    """    ActionAttempt,\n    AttemptStatus,\n    AwaitingHumanTick,\n""",
)
replace_once(
    planner_path,
    """class PlannerLifecycleCoordinator:\n    \"\"\"Advance durable planning state without owning the workflow Tick loop.\"\"\"\n\n    def __init__(self, runtime: PlannerLifecycleRuntime):\n        self.runtime = runtime\n\n    async def advance_mission_repair(self, ctx, observation):\n""",
    """class PlannerLifecycleCoordinator:\n    \"\"\"Advance durable planning state without owning the workflow Tick loop.\"\"\"\n\n    def __init__(self, runtime: PlannerLifecycleRuntime):\n        self.runtime = runtime\n\n    def _accept_verified_action_baseline(\n        self,\n        current,\n        baseline,\n        state_delta,\n    ) -> bool:\n        \"\"\"Accept only the closed, verified effect of one settler move.\n\n        Other successful actions complete or revise their Mission and are handled by\n        the ordinary active-Mission impact filter. This narrow exception keeps a\n        multi-turn settler Mission alive without swallowing unrelated game changes.\n        \"\"\"\n\n        if not current.completeness.supports_scope(\"settler\"):\n            return False\n        candidates: list[ActionAttempt] = []\n        for attempt in self.runtime.store.list_action_attempts(\n            current.game_session_id\n        ):\n            if (\n                attempt.status is not AttemptStatus.SUCCEEDED\n                or attempt.action_type != \"unit_move\"\n                or attempt.verified_at is None\n                or attempt.prepared_at < baseline.observed_at\n                or attempt.last_verification_projection_hash\n                != current.projection_hash\n            ):\n                continue\n            arguments = thaw_json(attempt.normalized_arguments)\n            unit_id = str(arguments.get(\"unit_id\", \"\"))\n            target_x = arguments.get(\"target_x\")\n            target_y = arguments.get(\"target_y\")\n            expected_path = f\"units.{unit_id}\"\n            if (\n                not unit_id\n                or type(target_x) is not int\n                or type(target_y) is not int\n                or any(\n                    change.scope != \"settler\"\n                    or change.field_path != expected_path\n                    for change in state_delta.changes\n                )\n            ):\n                continue\n            resulting = thaw_json(state_delta.changes[0].after)\n            if (\n                not isinstance(resulting, dict)\n                or resulting.get(\"x\") != target_x\n                or resulting.get(\"y\") != target_y\n            ):\n                continue\n            candidates.append(attempt)\n        if not candidates:\n            return False\n        latest = max(\n            candidates,\n            key=lambda attempt: (attempt.verified_at, attempt.action_attempt_id),\n        )\n        self.runtime.store.accept_observation_baseline(\n            current.observation_id,\n            expected_previous_observation_id=baseline.observation_id,\n            reason=(\n                \"accepted verified settler movement effect from \"\n                f\"{latest.action_attempt_id}\"\n            ),\n            accepted_at=self.runtime._now(),\n        )\n        return True\n\n    async def advance_mission_repair(self, ctx, observation):\n""",
)
replace_once(
    planner_path,
    """        baseline = runtime.store.get_accepted_observation_baseline(snapshot.game_id)\n        if comparison.kind is ObservationComparisonKind.STATE_DELTA:\n""",
    """        baseline = runtime.store.get_accepted_observation_baseline(snapshot.game_id)\n        if (\n            baseline is not None\n            and comparison.kind is ObservationComparisonKind.STATE_DELTA\n            and self._accept_verified_action_baseline(\n                current, baseline, comparison.state_delta\n            )\n        ):\n            return None\n        if comparison.kind is ObservationComparisonKind.STATE_DELTA:\n""",
)
replace_once(
    planner_path,
    """                if (\n                    runtime.store.provider_budget_request_count_for_turn(\n                        snapshot.game_id, snapshot.turn\n                    )\n                    >= runtime.config.new_planner_request_limit\n                ):\n                    return None\n                base_context = {\n""",
    """                if (\n                    runtime.store.provider_budget_request_count_for_turn(\n                        snapshot.game_id, snapshot.turn\n                    )\n                    >= runtime.config.new_planner_request_limit\n                ):\n                    reason = (\n                        \"Mission repair closure exhausted this turn's \"\n                        \"PlannerRequest budget; remaining scope \"\n                        f\"{repair_scope} requires explicit human review\"\n                    )\n                    compatibility = TickResult(\n                        turn=snapshot.turn,\n                        metrics=ctx.metrics,\n                        events=[],\n                        paused=True,\n                        pause_reason=reason,\n                    )\n                    return self._finish(\n                        ctx,\n                        snapshot,\n                        AwaitingHumanTick,\n                        compatibility=compatibility,\n                        blocking_reason=reason,\n                    )\n                base_context = {\n""",
)

delta_path = "src/civ6_workflow/domain/state_delta.py"
replace_once(
    delta_path,
    """from .contracts import MissionGraph\n""",
    """from .contracts import MissionGraph, MissionStatus\n""",
)
replace_once(
    delta_path,
    """                before={\n                    unit.entity_id.value: _tactical_unit_projection(unit)\n                    for unit in baseline.units\n                },\n""",
    """                before={\n                    unit.entity_id.value: _tactical_unit_projection(unit)\n                    for unit in baseline.units\n                    if \"SETTLER\" not in unit.unit_type\n                },\n""",
)
replace_once(
    delta_path,
    """                after={\n                    unit.entity_id.value: _tactical_unit_projection(unit)\n                    for unit in current.units\n                },\n""",
    """                after={\n                    unit.entity_id.value: _tactical_unit_projection(unit)\n                    for unit in current.units\n                    if \"SETTLER\" not in unit.unit_type\n                },\n""",
)
replace_once(
    delta_path,
    """            self._append_entity_changes(\n                changes,\n                scope=\"tactical_emergency\",\n                collection=\"units\",\n                before={\n                    unit.entity_id.value: _tactical_unit_projection(unit)\n                    for unit in baseline.units\n                    if \"SETTLER\" not in unit.unit_type\n                },\n                after={\n                    unit.entity_id.value: _tactical_unit_projection(unit)\n                    for unit in current.units\n                    if \"SETTLER\" not in unit.unit_type\n                },\n            )\n\n        if baseline.completeness.blockers and current.completeness.blockers:\n""",
    """            self._append_entity_changes(\n                changes,\n                scope=\"tactical_emergency\",\n                collection=\"units\",\n                before={\n                    unit.entity_id.value: _tactical_unit_projection(unit)\n                    for unit in baseline.units\n                    if \"SETTLER\" not in unit.unit_type\n                },\n                after={\n                    unit.entity_id.value: _tactical_unit_projection(unit)\n                    for unit in current.units\n                    if \"SETTLER\" not in unit.unit_type\n                },\n            )\n            settler_unit_paths = {\n                f\"units.{unit.entity_id.value}\"\n                for unit in (*baseline.units, *current.units)\n                if \"SETTLER\" in unit.unit_type\n            }\n            changes[:] = [\n                change\n                for change in changes\n                if not (\n                    change.scope == \"tactical_emergency\"\n                    and change.field_path in settler_unit_paths\n                )\n            ]\n\n        if baseline.completeness.blockers and current.completeness.blockers:\n""",
)
replace_once(
    delta_path,
    """        direct = {\n            mission.mission_id\n            for mission in mission_graph.missions\n            if mission.scope in changed_scopes\n        }\n""",
    """        direct = {\n            mission.mission_id\n            for mission in mission_graph.missions\n            if mission.status is MissionStatus.ACTIVE\n            and mission.scope in changed_scopes\n        }\n""",
)
replace_once(
    delta_path,
    """        direct.update(\n            mission.mission_id\n            for mission in mission_graph.missions\n            if mission.subject.subject_type == \"unit\"\n            and mission.subject.subject_id in changed_unit_ids\n        )\n""",
    """        direct.update(\n            mission.mission_id\n            for mission in mission_graph.missions\n            if mission.status is MissionStatus.ACTIVE\n            and mission.subject.subject_type == \"unit\"\n            and mission.subject.subject_id in changed_unit_ids\n        )\n""",
)
replace_once(
    delta_path,
    """        missions = {mission.mission_id: mission for mission in mission_graph.missions}\n        affected = set(direct)\n        changed = True\n        while changed:\n            changed = False\n            for mission in mission_graph.missions:\n""",
    """        missions = {\n            mission.mission_id: mission\n            for mission in mission_graph.missions\n            if mission.status is MissionStatus.ACTIVE\n        }\n        affected = set(direct)\n        changed = True\n        while changed:\n            changed = False\n            for mission in missions.values():\n""",
)
replace_once(
    delta_path,
    """                for dependency_id in mission.dependency_mission_ids:\n                    if mission.mission_id in affected and dependency_id not in affected:\n                        affected.add(dependency_id)\n                        changed = True\n""",
    """                for dependency_id in mission.dependency_mission_ids:\n                    if (\n                        mission.mission_id in affected\n                        and dependency_id in missions\n                        and dependency_id not in affected\n                    ):\n                        affected.add(dependency_id)\n                        changed = True\n""",
)

Path("tests/test_runtime_loop_closure.py").write_text(
    '''from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from civ6_workflow.domain import (
    AttemptStatus,
    Mission,
    MissionGraph,
    MissionImpactAnalyzer,
    MissionStatus,
    StateDeltaBuilder,
    SubjectRef,
)
from civ6_workflow.models import RuntimeSnapshot
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.planner_lifecycle import (
    PlannerLifecycleCoordinator,
    PlannerLifecycleRuntime,
)
from civ6_workflow.runtime import WorkflowRuntime


NOW = datetime(2026, 8, 5, 3, 0, tzinfo=UTC)


def _observation(*, x: int):
    return normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id="game-1",
            turn=10,
            units=[
                {
                    "unit_id": 7,
                    "unit_type": "UNIT_SETTLER",
                    "x": x,
                    "y": 2,
                    "moves_remaining": 1,
                }
            ],
        ),
        observed_at=NOW + timedelta(seconds=x),
    ).canonical


def _mission(status: MissionStatus) -> Mission:
    return Mission(
        mission_id=f"mission-{status.value.lower()}",
        game_session_id="game-1",
        contract_id="contract-1",
        mission_revision=1,
        scope="settler",
        subject=SubjectRef(subject_type="unit", subject_id="7"),
        slot="unit:7:settlement",
        objective="found city",
        desired_outcome={
            "settler": {
                "unit_id": 7,
                "target_x": 3,
                "target_y": 2,
                "baseline_city_count": 1,
                "owner": "PLAYER_0",
            }
        },
        status=status,
    )


def test_settler_motion_does_not_also_create_tactical_delta():
    result = StateDeltaBuilder().compare(_observation(x=1), _observation(x=2))

    assert result.state_delta is not None
    assert {change.scope for change in result.state_delta.changes} == {"settler"}


def test_completed_mission_is_not_reopened_by_state_delta():
    result = StateDeltaBuilder().compare(_observation(x=1), _observation(x=2))
    assert result.state_delta is not None
    completed_graph = MissionGraph(
        missions=(_mission(MissionStatus.COMPLETED),)
    )
    active_graph = MissionGraph(
        missions=(_mission(MissionStatus.ACTIVE),)
    )

    assert (
        MissionImpactAnalyzer().affected_mission_ids(
            result.state_delta, completed_graph
        )
        == ()
    )
    assert MissionImpactAnalyzer().affected_mission_ids(
        result.state_delta, active_graph
    ) == ("mission-active",)


def test_verified_action_projection_advances_observation_baseline():
    baseline = _observation(x=1)
    current = _observation(x=2)

    class Store:
        def __init__(self):
            self.accepted = []

        def list_action_attempts(self, game_id):
            assert game_id == "game-1"
            return [
                SimpleNamespace(
                    status=AttemptStatus.SUCCEEDED,
                    action_type="unit_move",
                    prepared_at=NOW + timedelta(seconds=1),
                    verified_at=NOW + timedelta(seconds=3),
                    last_verification_projection_hash=current.projection_hash,
                    normalized_arguments={
                        "unit_id": 7,
                        "target_x": 2,
                        "target_y": 2,
                    },
                    action_attempt_id="attempt-1",
                )
            ]

        def accept_observation_baseline(self, observation_id, **kwargs):
            self.accepted.append((observation_id, kwargs))

    store = Store()
    coordinator = PlannerLifecycleCoordinator(
        PlannerLifecycleRuntime(
            store=store,
            game=SimpleNamespace(),
            planner=SimpleNamespace(),
            config=SimpleNamespace(),
            conditions=SimpleNamespace(),
            information_queries=SimpleNamespace(),
            now=lambda: NOW + timedelta(seconds=4),
            monotonic=lambda: 0.0,
            checkpoint=lambda _name: None,
            observation_id=lambda: current.observation_id,
            human_wait_context=lambda _snapshot: {},
            available_tools=lambda: set(),
        )
    )
    delta = StateDeltaBuilder().compare(baseline, current).state_delta
    assert delta is not None

    accepted = coordinator._accept_verified_action_baseline(
        current, baseline, delta
    )

    assert accepted is True
    assert store.accepted[0][0] == current.observation_id
    assert (
        store.accepted[0][1]["expected_previous_observation_id"]
        == baseline.observation_id
    )


def test_verified_action_does_not_swallow_unrelated_scope_change():
    baseline = _observation(x=1)
    current = normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id="game-1",
            turn=10,
            tech_civics={"current_research_type": "TECH_MINING"},
            units=[
                {
                    "unit_id": 7,
                    "unit_type": "UNIT_SETTLER",
                    "x": 2,
                    "y": 2,
                    "moves_remaining": 1,
                }
            ],
        ),
        observed_at=NOW + timedelta(seconds=2),
    ).canonical
    delta = StateDeltaBuilder().compare(baseline, current).state_delta
    assert delta is not None

    class Store:
        def list_action_attempts(self, _game_id):
            return [
                SimpleNamespace(
                    status=AttemptStatus.SUCCEEDED,
                    action_type="unit_move",
                    prepared_at=NOW + timedelta(seconds=1),
                    verified_at=NOW + timedelta(seconds=3),
                    last_verification_projection_hash=current.projection_hash,
                    normalized_arguments={
                        "unit_id": 7,
                        "target_x": 2,
                        "target_y": 2,
                    },
                    action_attempt_id="attempt-1",
                )
            ]

        def accept_observation_baseline(self, *_args, **_kwargs):
            raise AssertionError("unrelated changes must not advance the baseline")

    coordinator = PlannerLifecycleCoordinator(
        PlannerLifecycleRuntime(
            store=Store(),
            game=SimpleNamespace(),
            planner=SimpleNamespace(),
            config=SimpleNamespace(),
            conditions=SimpleNamespace(),
            information_queries=SimpleNamespace(),
            now=lambda: NOW + timedelta(seconds=4),
            monotonic=lambda: 0.0,
            checkpoint=lambda _name: None,
            observation_id=lambda: current.observation_id,
            human_wait_context=lambda _snapshot: {},
            available_tools=lambda: set(),
        )
    )

    assert (
        coordinator._accept_verified_action_baseline(current, baseline, delta)
        is False
    )


def test_rewind_recovery_precedes_projection_and_mutation():
    source = inspect.getsource(WorkflowRuntime._run_tick)
    rewind = source.index("if rewind_pending:", source.index("unresolved ="))
    projection = source.index("projection = await self.strategic_workflow")
    execution = source.index("execution = await self.batch_executor.advance")

    assert rewind < projection < execution
    assert "turn_rewind_requires_strategic_reset" in source


def test_repair_budget_exhaustion_is_an_explicit_human_wait():
    source = inspect.getsource(PlannerLifecycleCoordinator.advance_mission_repair)

    assert "Mission repair closure exhausted" in source
    assert "AwaitingHumanTick" in source
''',
    encoding="utf-8",
)
