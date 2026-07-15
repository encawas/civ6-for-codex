from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from civ6_workflow.domain import (
    ActionAttempt,
    ApprovalStatus,
    AttemptStatus,
    MutationSentTick,
    Observation,
    RetryClassification,
    RuntimeState,
    SlotState,
    SourceVersions,
    SubjectRef,
    Task,
    TaskStatus,
    build_task_idempotency_key,
    normalize_slot,
    tasks_conflict,
)


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
VERSIONS = SourceVersions(game_api="civ6-mcp/v1", normalization="1", runtime="1")


@pytest.mark.parametrize(
    "raw",
    [None, "", "NONE", "none", "nothing", "NOTHING", {}, []],
)
def test_obs_001_empty_production_spellings_normalize_identically(raw):
    """OBS-001: upstream empty spellings have one canonical representation."""

    assert normalize_slot(raw).state is SlotState.EMPTY


@pytest.mark.parametrize(
    "raw",
    [
        "UNIT_SCOUT",
        "BUILDING_MONUMENT",
        "DISTRICT_CAMPUS",
        "PROJECT_CAMPUS_RESEARCH_GRANTS",
        "BUILDING_PYRAMIDS",
    ],
)
def test_obs_002_occupied_production_remains_occupied(raw):
    """OBS-002: valid production identifiers are never treated as empty."""

    slot = normalize_slot(raw)

    assert slot.state is SlotState.OCCUPIED
    assert slot.value == raw


def test_obs_006_equal_facts_have_distinct_ids_and_equal_projection_hashes():
    """OBS-006: identity is append-only while equivalent facts hash equally."""

    common = {
        "game_session_id": "game-1",
        "turn_number": 12,
        "source_versions": VERSIONS,
        "base_state": {"research": "TECH_MINING"},
    }
    first = Observation(observation_id="obs-1", sequence=1, observed_at=NOW, **common)
    second = Observation(observation_id="obs-2", sequence=2, observed_at=NOW, **common)

    assert first.observation_id != second.observation_id
    assert first.projection_hash == second.projection_hash
    with pytest.raises(ValidationError):
        first.turn_number = 13


def _task(
    task_id: str,
    *,
    slot: str = "player:research",
    status: TaskStatus = TaskStatus.READY,
    outcome: str = "TECH_MINING",
    earliest_turn: int = 12,
    latest_turn: int | None = 12,
) -> Task:
    subject = SubjectRef(subject_type="research", subject_id="player-1")
    desired_outcome = {"research": outcome}
    key = build_task_idempotency_key(
        game_session_id="game-1",
        subject=subject,
        slot=slot,
        desired_outcome=desired_outcome,
        source_plan_revision=3,
        earliest_turn=earliest_turn,
        latest_turn=latest_turn,
    )
    return Task(
        task_id=task_id,
        game_session_id="game-1",
        idempotency_key=key,
        task_type="set_research",
        subject=subject,
        slot=slot,
        arguments={"tech_or_civic": outcome},
        desired_outcome=desired_outcome,
        source_plan_id="opening-plan",
        source_plan_revision=3,
        created_from_observation_id="obs-1",
        status=status,
        earliest_turn=earliest_turn,
        latest_turn=latest_turn,
        must_complete_before_end_turn=True,
        approval_status=ApprovalStatus.APPROVED,
        preconditions=(),
        postconditions=(),
    )


def test_task_001_repeated_semantics_produce_a_stable_identity():
    """TASK-001: repeated equivalent work has one semantic identity."""

    assert _task("task-a").idempotency_key == _task("task-a").idempotency_key


def test_task_002_different_task_ids_still_conflict_on_semantics():
    """TASK-002: caller-generated IDs cannot bypass semantic deduplication."""

    assert tasks_conflict(_task("task-a"), _task("task-b")) is True


def test_task_003_active_slot_windows_conflict_before_scheduling():
    """TASK-003: overlapping active work cannot own the same slot twice."""

    current = _task("task-a", outcome="TECH_MINING", earliest_turn=12, latest_turn=13)
    replacement = _task(
        "task-b", outcome="TECH_POTTERY", earliest_turn=13, latest_turn=14
    )
    future = _task("task-c", outcome="TECH_WRITING", earliest_turn=14, latest_turn=15)
    completed = _task("task-d", status=TaskStatus.SUCCEEDED, outcome="TECH_POTTERY")

    assert tasks_conflict(current, replacement) is True
    assert tasks_conflict(current, future) is False
    assert tasks_conflict(current, completed) is False


def test_task_rejects_identity_that_does_not_match_semantics():
    payload = _task("task-a").model_dump()
    payload["idempotency_key"] = "task:forged"

    with pytest.raises(ValidationError, match="idempotency_key"):
        Task.model_validate(payload)


def test_attempt_cannot_verify_before_it_is_sent():
    with pytest.raises(ValidationError, match="requires sent_at"):
        ActionAttempt(
            action_attempt_id="attempt-1",
            task_id="task-a",
            attempt_number=1,
            request_id="request-1",
            idempotency_key=_task("task-a").idempotency_key,
            prepared_from_observation_id="obs-1",
            prepared_at=NOW,
            status=AttemptStatus.VERIFYING,
            retry_classification=RetryClassification.NEVER_BLIND_RETRY,
            normalized_arguments={"tech_or_civic": "TECH_MINING"},
        )


def test_tick_rejects_a_second_mutation_structurally():
    with pytest.raises(ValidationError, match="Input should be 1"):
        MutationSentTick(
            tick_id="tick-1",
            game_session_id="game-1",
            starting_runtime_state=RuntimeState.READY_TO_ACT,
            observation_ids=("obs-1",),
            selected_operation="set_research",
            mutation_budget_used=2,
            action_attempt_id="attempt-1",
            started_at=NOW,
            completed_at=NOW,
        )


def test_domain_model_json_round_trip_is_lossless():
    original = _task("task-a")

    restored = Task.model_validate_json(original.model_dump_json())

    assert restored == original
