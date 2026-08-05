from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import sqlite3

import pytest

from civ6_workflow.domain import ObservationComparisonKind
from civ6_workflow.models import RuntimeSnapshot
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.store import WorkflowStore


NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)


def _observation(
    *,
    game_id: str = "game-1",
    turn: int = 10,
    research: str = "TECH_WRITING",
    include_research: bool = True,
    include_available: bool = True,
    observation_id: str = "obs-1",
):
    progression = {}
    if include_research:
        progression["current_research_type"] = research
    if include_available:
        progression["available_techs"] = [
            {"tech_type": "TECH_WRITING", "name": "Writing"},
            {"tech_type": "TECH_MINING", "name": "Mining"},
        ]
    return normalize_runtime_snapshot(
        RuntimeSnapshot(
            game_id=game_id,
            turn=turn,
            tech_civics=progression,
        )
    ).canonical.model_copy(
        update={
            "observation_id": observation_id,
            "observed_at": NOW + timedelta(minutes=turn),
        }
    )


def _accept(store: WorkflowStore, observation, *, previous=None):
    store.save_normalized_observation(observation)
    return store.accept_observation_baseline(
        observation.observation_id,
        expected_previous_observation_id=previous,
        reason="accepted for research comparison",
        accepted_at=observation.observed_at + timedelta(seconds=1),
    )


def test_new_database_uses_phase2_schema(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")

    with store._connect() as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 14
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert {
        "normalized_observations",
        "observation_baseline_acceptances",
        "state_deltas",
    }.issubset(tables)


def test_incomplete_observation_cannot_replace_accepted_baseline(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    baseline = _observation()
    _accept(store, baseline)
    incomplete = _observation(
        turn=11,
        include_research=False,
        include_available=False,
        observation_id="obs-incomplete",
    )

    comparison = store.record_observation_comparison(
        incomplete,
        detected_at=incomplete.observed_at + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="incomplete"):
        store.accept_observation_baseline(
            incomplete.observation_id,
            expected_previous_observation_id=baseline.observation_id,
            reason="must fail",
            accepted_at=incomplete.observed_at + timedelta(seconds=2),
        )

    assert comparison.kind is ObservationComparisonKind.NO_CHANGE
    assert store.get_accepted_observation_baseline("game-1") == baseline

    complete = _observation(
        turn=12,
        observation_id="obs-complete",
    )
    later = store.record_observation_comparison(
        complete,
        detected_at=complete.observed_at + timedelta(seconds=1),
    )
    assert later.kind is ObservationComparisonKind.NO_CHANGE
    assert store.get_accepted_observation_baseline("game-1") == baseline


def test_state_delta_is_persisted_and_baseline_advance_is_explicit(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    baseline = _observation()
    _accept(store, baseline)
    current = _observation(
        turn=11,
        research="TECH_MINING",
        observation_id="obs-2",
    )

    comparison = store.record_observation_comparison(
        current,
        detected_at=current.observed_at + timedelta(seconds=1),
    )

    assert comparison.kind is ObservationComparisonKind.STATE_DELTA
    assert comparison.state_delta is not None
    assert store.list_state_deltas("game-1") == [comparison.state_delta]
    assert store.get_accepted_observation_baseline("game-1") == baseline

    _accept(store, current, previous=baseline.observation_id)
    assert store.get_accepted_observation_baseline("game-1") == current


def test_stale_baseline_compare_and_swap_fails_closed(tmp_path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    baseline = _observation()
    _accept(store, baseline)
    current = _observation(turn=11, observation_id="obs-2")
    store.save_normalized_observation(current)

    with pytest.raises(ValueError, match="stale accepted Observation baseline"):
        store.accept_observation_baseline(
            current.observation_id,
            expected_previous_observation_id="obs-wrong",
            reason="must fail",
            accepted_at=current.observed_at + timedelta(seconds=1),
        )

    assert store.get_accepted_observation_baseline("game-1") == baseline


def test_observation_delta_replay_round_trip_is_stable(tmp_path):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    baseline = _observation()
    _accept(source, baseline)
    current = _observation(
        turn=11,
        research="TECH_MINING",
        observation_id="obs-2",
    )
    source.record_observation_comparison(
        current,
        detected_at=current.observed_at + timedelta(seconds=1),
    )
    _accept(source, current, previous=baseline.observation_id)
    exported = source.export_replay_state("game-1")

    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(exported)

    assert restored.export_replay_state("game-1") == exported
    assert restored.get_accepted_observation_baseline("game-1") == current
    assert restored.list_state_deltas("game-1") == source.list_state_deltas("game-1")


def test_startup_rejects_forged_state_delta(tmp_path):
    path = tmp_path / "workflow.sqlite3"
    store = WorkflowStore(path)
    baseline = _observation()
    _accept(store, baseline)
    current = _observation(
        turn=11,
        research="TECH_MINING",
        observation_id="obs-2",
    )
    result = store.record_observation_comparison(
        current,
        detected_at=current.observed_at + timedelta(seconds=1),
    )
    assert result.state_delta is not None

    with sqlite3.connect(path) as conn:
        payload = json.loads(
            conn.execute("SELECT delta_json FROM state_deltas").fetchone()[0]
        )
        payload["current_turn"] = 99
        conn.execute(
            "UPDATE state_deltas SET delta_json=?",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")),),
        )

    with pytest.raises(ValueError, match="StateDelta"):
        WorkflowStore(path)


def test_replay_preflight_rejects_bad_baseline_before_delete(tmp_path):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    baseline = _observation()
    _accept(source, baseline)
    replay = source.export_replay_state("game-1")
    replay["tables"]["observation_baseline_acceptances"][0][
        "previous_observation_id"
    ] = "obs-missing"

    target = WorkflowStore(tmp_path / "target.sqlite3")
    sentinel = _observation(observation_id="obs-sentinel")
    _accept(target, sentinel)

    with pytest.raises(ValueError, match="baseline"):
        target.import_replay_state(replay)

    assert target.get_accepted_observation_baseline("game-1") == sentinel
