from __future__ import annotations

from datetime import UTC, datetime

import pytest

from civ6_workflow.domain import (
    Mission,
    MissionGraph,
    MissionImpactAnalyzer,
    MissionStatus,
    ObservationComparisonKind,
    StateDeltaBuilder,
    SubjectRef,
    build_mission_graph_patch,
    build_mission_graph_patch_id,
)
from civ6_workflow.models import RuntimeSnapshot
from civ6_workflow.observation_normalization import normalize_runtime_snapshot


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)


def _observation(
    *,
    game_id: str = "game-1",
    turn: int = 10,
    research: object = "TECH_WRITING",
    available: object = (
        {"tech_type": "TECH_WRITING", "name": "Writing"},
        {"tech_type": "TECH_MINING", "name": "Mining"},
    ),
    include_research: bool = True,
    include_available: bool = True,
):
    progression = {}
    if include_research:
        progression["current_research_type"] = research
    if include_available:
        progression["available_techs"] = list(available)
    return normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id=game_id,
            turn=turn,
            tech_civics=progression,
        )
    ).canonical.model_copy(update={"observed_at": NOW})


def test_initial_baseline_is_a_control_result():
    current = _observation()

    result = StateDeltaBuilder().compare(None, current)

    assert result.kind is ObservationComparisonKind.INITIAL_BASELINE
    assert result.state_delta is None


@pytest.mark.parametrize(
    ("update", "reason"),
    [
        ({"game_session_id": "game-2"}, "session"),
        ({"normalization_version": "civ6-observation/v0"}, "normalization"),
        ({"source_version": "civ6-runtime-snapshot/v0"}, "source"),
        ({"turn_number": 9}, "turn"),
    ],
)
def test_incompatible_observation_requires_rebaseline(update, reason):
    baseline = _observation()
    current = _observation().model_copy(update=update)

    result = StateDeltaBuilder().compare(baseline, current)

    assert result.kind is ObservationComparisonKind.REBASELINE_REQUIRED
    assert reason.casefold() in result.reason.casefold()
    assert result.state_delta is None


def test_not_loaded_research_is_unknown_not_deleted():
    baseline = _observation()
    current = _observation(include_research=False, include_available=False)

    result = StateDeltaBuilder().compare(baseline, current)

    assert result.kind is ObservationComparisonKind.REBASELINE_REQUIRED
    assert result.state_delta is None


def test_known_research_change_produces_deterministic_delta():
    baseline = _observation()
    current = _observation(research="TECH_MINING").model_copy(
        update={"observation_id": "obs-current"}
    )

    first = StateDeltaBuilder().compare(baseline, current)
    second = StateDeltaBuilder().compare(baseline, current)

    assert first == second
    assert first.kind is ObservationComparisonKind.STATE_DELTA
    assert first.state_delta is not None
    assert tuple(change.field_path for change in first.state_delta.changes) == (
        "progression.current_research",
    )


def test_incomplete_independent_field_can_produce_local_delta():
    baseline = _observation()
    current = _observation(
        research="TECH_MINING",
        include_available=False,
    )

    result = StateDeltaBuilder().compare(baseline, current)

    assert result.kind is ObservationComparisonKind.STATE_DELTA
    assert result.state_delta is not None
    assert tuple(change.field_path for change in result.state_delta.changes) == (
        "progression.current_research",
    )


def _mission(
    mission_id: str,
    *,
    scope: str,
    slot: str,
    dependencies: tuple[str, ...] = (),
) -> Mission:
    return Mission(
        mission_id=mission_id,
        game_session_id="game-1",
        contract_id="contract-1",
        mission_revision=1,
        scope=scope,
        subject=SubjectRef(subject_type="player", subject_id="game-1"),
        slot=slot,
        objective=mission_id,
        desired_outcome={"technology": "TECH_WRITING"},
        status=MissionStatus.ACTIVE,
        dependency_mission_ids=dependencies,
    )


def test_mission_impact_expands_dependency_and_shared_slot_closure():
    baseline = _observation()
    current = _observation(research="TECH_MINING")
    delta = StateDeltaBuilder().compare(baseline, current).state_delta
    assert delta is not None
    graph = MissionGraph(
        missions=(
            _mission(
                "mission-civic",
                scope="civic",
                slot="player:civic",
                dependencies=("mission-research",),
            ),
            _mission("mission-research", scope="research", slot="player:research"),
            _mission("mission-shared", scope="future", slot="player:research"),
        )
    )

    affected = MissionImpactAnalyzer().affected_mission_ids(delta, graph)

    assert affected == ("mission-civic", "mission-research", "mission-shared")


def test_unrelated_delta_has_no_mission_impact():
    baseline = _observation()
    current = _observation(research="TECH_MINING")
    delta = StateDeltaBuilder().compare(baseline, current).state_delta
    assert delta is not None
    graph = MissionGraph(
        missions=(_mission("mission-civic", scope="civic", slot="player:civic"),)
    )

    assert MissionImpactAnalyzer().affected_mission_ids(delta, graph) == ()


def test_mission_graph_patch_is_closed_over_affected_research_missions():
    request_id = "request-repair-1"
    repaired = _mission(
        "mission-research",
        scope="research",
        slot="player:research",
    ).model_copy(update={"mission_revision": 2})

    patch = build_mission_graph_patch(
        patch_id=build_mission_graph_patch_id(request_id),
        game_session_id="game-1",
        contract_id="contract-1",
        expected_base_revision=1,
        source_state_delta_id="delta-1",
        source_planner_request_id=request_id,
        source_provider_attempt_id="attempt-1",
        source_provider_attempt_number=1,
        affected_mission_ids=("mission-research",),
        mission_updates=(repaired,),
        created_from_observation_id="obs-current",
        created_at=NOW,
    )

    assert patch.mission_updates == (repaired,)


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {
                "scope": "production",
                "slot": "city:production",
            },
            "execution contract",
        ),
        (
            {"status": MissionStatus.BLOCKED},
            "ACTIVE",
        ),
    ],
)
def test_mission_graph_patch_rejects_out_of_scope_or_inactive_updates(updates, message):
    request_id = "request-repair-invalid"
    mission = _mission(
        "mission-research",
        scope="research",
        slot="player:research",
    ).model_copy(update={"mission_revision": 2, **updates})

    with pytest.raises(ValueError, match=message):
        build_mission_graph_patch(
            patch_id=build_mission_graph_patch_id(request_id),
            game_session_id="game-1",
            contract_id="contract-1",
            expected_base_revision=1,
            source_state_delta_id="delta-1",
            source_planner_request_id=request_id,
            source_provider_attempt_id="attempt-1",
            source_provider_attempt_number=1,
            affected_mission_ids=("mission-research",),
            mission_updates=(mission,),
            created_from_observation_id="obs-current",
            created_at=NOW,
        )
