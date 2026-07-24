import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from civ6_workflow.domain import (
    ApprovalStatus,
    ContinuationPolicy,
    InformationRound,
    InformationRoundStatus,
    LeaseValidationResult,
    LogicalPlannerRequestCreatedTick,
    PlanLease,
    PlanLeaseStatus,
    PlannerRequest,
    PlannerResponseEvidenceCompatibility,
    PlannerRequestStatus,
    PlannerRequestTarget,
    PlannerRequestTargetKind,
    ProviderAttempt,
    ProviderAttemptStatus,
    RuntimeState,
    canonical_json,
    canonical_json_hash,
)
from civ6_workflow.store import WorkflowStore
from civ6_workflow.workflow_protocol import (
    WorkflowPlanBundle,
    canonical_workflow_plan_bundle_payload,
)


NOW = datetime(2026, 7, 23, tzinfo=UTC)


def _legacy_target(
    *,
    group_id: str | None = "group-1",
    gap_ids: tuple[str, ...] = ("gap-1",),
) -> PlannerRequestTarget:
    return PlannerRequestTarget(
        kind=PlannerRequestTargetKind.LEGACY_DECISION_GROUP,
        decision_group_id=group_id,
        decision_gap_ids=gap_ids,
    )


def _repair_target(
    *,
    contract_id: str = "contract-1",
    revision: int = 1,
    scope: str = "research",
    mission_ids: tuple[str, ...] = (),
) -> PlannerRequestTarget:
    return PlannerRequestTarget(
        kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
        strategic_contract_id=contract_id,
        base_contract_revision=revision,
        strategic_scope=scope,
        affected_mission_ids=mission_ids,
    )


def _request(
    request_id: str = "request-1",
    *,
    target: PlannerRequestTarget | None = None,
    input_hash: str = "input-1",
    status: PlannerRequestStatus = PlannerRequestStatus.PENDING,
    created_at: datetime = NOW,
    completed_at: datetime | None = None,
    response_payload: dict | None = None,
    response_hash: str | None = None,
    validation_result: dict | None = None,
    response_evidence_compatibility: (
        PlannerResponseEvidenceCompatibility | None
    ) = None,
) -> PlannerRequest:
    return PlannerRequest(
        planner_request_id=request_id,
        game_session_id="game-1",
        turn_number=4,
        observation_id="obs-1",
        target=target or _legacy_target(),
        input_projection_hash=input_hash,
        input_projection_version="decision-input/v1",
        input_projection={"projection": 1},
        request_payload={"request": request_id},
        policy_revision="policy-1",
        model_settings={"provider": "test"},
        status=status,
        created_at=created_at,
        completed_at=completed_at,
        response_payload=response_payload,
        response_hash=response_hash,
        validation_result=validation_result,
        response_evidence_compatibility=response_evidence_compatibility,
    )


def _completed_request(
    request_id: str,
    *,
    target: PlannerRequestTarget | None = None,
    payload: dict | None = None,
) -> PlannerRequest:
    payload = payload or {"summary": request_id, "tasks": []}
    payload = canonical_workflow_plan_bundle_payload(payload)
    return _request(
        request_id,
        target=target,
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW + timedelta(seconds=1),
        response_payload=payload,
        response_hash=canonical_json_hash(payload),
        validation_result={"result": "completed"},
    )


def _schema_failure_request(
    request_id: str,
    *,
    provider_attempt_count: int = 1,
) -> PlannerRequest:
    return _request(
        request_id,
        status=PlannerRequestStatus.REJECTED,
        completed_at=NOW + timedelta(seconds=1),
    ).model_copy(
        update={
            "failure_category": "planner_contract_failure",
            "provider_attempt_count": provider_attempt_count,
        }
    )


def test_legacy_target_key_uses_only_sorted_gap_ids():
    historical = _legacy_target(group_id=None, gap_ids=(" gap-b ", "gap-a"))
    current = _legacy_target(group_id="group-new", gap_ids=("gap-a", "gap-b"))

    assert historical.decision_gap_ids == ("gap-a", "gap-b")
    assert historical.target_key == current.target_key
    assert historical.target_key.startswith("planner-target:")


@pytest.mark.parametrize("field", ["decision_gap_ids", "affected_mission_ids"])
@pytest.mark.parametrize("values", [("same", "same"), ("valid", "   ")])
def test_target_identifier_sets_reject_duplicates_and_blank_values(field, values):
    payload = {
        "kind": PlannerRequestTargetKind.LEGACY_DECISION_GROUP,
        "decision_gap_ids": ("gap-1",),
    }
    if field == "affected_mission_ids":
        payload = {
            "kind": PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            "strategic_contract_id": "contract-1",
            "base_contract_revision": 1,
            "strategic_scope": "research",
        }
    payload[field] = values
    with pytest.raises(ValidationError):
        PlannerRequestTarget(**payload)


def test_target_kinds_enforce_field_contracts_and_scope_wide_repair():
    creation = PlannerRequestTarget(
        kind=PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
        strategic_scope="research",
    )
    repair = _repair_target(mission_ids=("mission-b", " mission-a "))

    assert creation.strategic_contract_id is None
    assert repair.affected_mission_ids == ("mission-a", "mission-b")
    assert _repair_target().affected_mission_ids == ()

    invalid = [
        {
            "kind": PlannerRequestTargetKind.LEGACY_DECISION_GROUP,
            "decision_gap_ids": (),
        },
        {
            "kind": PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
            "strategic_scope": "research",
            "base_contract_revision": 1,
        },
        {
            "kind": PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            "strategic_contract_id": "contract-1",
            "base_contract_revision": 0,
            "strategic_scope": "research",
        },
        {
            "kind": PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            "strategic_contract_id": "contract-1",
            "base_contract_revision": 1,
            "strategic_scope": "research",
            "decision_gap_ids": ("gap-1",),
        },
    ]
    for payload in invalid:
        with pytest.raises(ValidationError):
            PlannerRequestTarget(**payload)


def test_planner_request_reads_legacy_json_but_serializes_only_target():
    legacy = _request().model_dump(mode="json")
    target = legacy.pop("target")
    legacy["decision_group_id"] = target["decision_group_id"]
    legacy["decision_gap_ids"] = list(reversed(target["decision_gap_ids"]))

    request = PlannerRequest.model_validate_json(json.dumps(legacy))
    serialized = json.loads(canonical_json(request.model_dump(mode="json")))

    assert request.decision_group_id == "group-1"
    assert request.decision_gap_ids == ("gap-1",)
    assert "decision_group_id" not in serialized
    assert "decision_gap_ids" not in serialized
    assert "request_target_key" not in serialized
    assert "target_key" not in serialized["target"]

    mixed = {**serialized, "decision_gap_ids": ["gap-1"]}
    with pytest.raises(ValidationError, match="cannot be combined"):
        PlannerRequest.model_validate_json(json.dumps(mixed))


def test_nonlegacy_target_keys_cover_contract_revision_scope_and_missions():
    creation = PlannerRequestTarget(
        kind=PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
        strategic_scope="research",
    )
    existing_creation = creation.model_copy(
        update={"strategic_contract_id": "contract-1"}
    )
    repair = _repair_target(mission_ids=("mission-1",))
    scope_wide = _repair_target()
    newer = _repair_target(revision=2, mission_ids=("mission-1",))

    assert len(
        {
            creation.target_key,
            existing_creation.target_key,
            repair.target_key,
            scope_wide.target_key,
            newer.target_key,
        }
    ) == 5


def test_created_tick_defaults_old_json_to_legacy_and_validates_gap_summary():
    common = {
        "tick_id": "tick-1",
        "game_session_id": "game-1",
        "turn_number": 4,
        "starting_runtime_state": RuntimeState.ROUTING,
        "observation_ids": ("obs-1",),
        "started_at": NOW,
        "completed_at": NOW,
        "planner_request_id": "request-1",
    }
    legacy = LogicalPlannerRequestCreatedTick(
        **common,
        decision_gap_ids=("gap-1",),
    )
    repair = LogicalPlannerRequestCreatedTick(
        **common,
        request_target_kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
    )

    assert legacy.request_target_kind is PlannerRequestTargetKind.LEGACY_DECISION_GROUP
    assert repair.decision_gap_ids == ()
    with pytest.raises(ValidationError):
        LogicalPlannerRequestCreatedTick(**common)
    with pytest.raises(ValidationError):
        LogicalPlannerRequestCreatedTick(
            **common,
            request_target_kind=PlannerRequestTargetKind.MISSION_GRAPH_REPAIR,
            decision_gap_ids=("gap-1",),
        )


def test_response_payload_lifecycle_and_hash_contract():
    payload = {"summary": "complete", "tasks": []}
    with pytest.raises(ValidationError):
        _request(response_payload=payload)
    with pytest.raises(ValidationError):
        _request(
            status=PlannerRequestStatus.COMPLETED,
            completed_at=NOW,
            response_payload=payload,
            response_hash="wrong",
            validation_result={"result": "completed"},
        )
    with pytest.raises(ValidationError, match="response_payload"):
        _request(
            target=_repair_target(),
            status=PlannerRequestStatus.COMPLETED,
            completed_at=NOW,
            response_hash="historical",
            validation_result={"result": "completed"},
        )

    historical = _request(
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW,
        response_hash="historical",
        validation_result={"result": "completed"},
    )
    completed = _completed_request("request-completed", target=_repair_target())

    assert historical.response_payload is None
    assert completed.response_hash == canonical_json_hash(completed.response_payload)


@pytest.mark.parametrize(
    "status",
    [
        PlannerRequestStatus.PENDING,
        PlannerRequestStatus.FAILED,
        PlannerRequestStatus.CANCELLED,
        PlannerRequestStatus.SUPERSEDED,
    ],
)
def test_legacy_response_compatibility_requires_response_terminal_status(status):
    with pytest.raises(ValidationError, match="response-terminal status"):
        _request(
            status=status,
            completed_at=(NOW if status is not PlannerRequestStatus.PENDING else None),
            response_evidence_compatibility=(
                PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD
            ),
        )


def test_legacy_response_compatibility_rejects_nonlegacy_target_and_payload():
    compatibility = (
        PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD
    )
    with pytest.raises(ValidationError, match="requires a legacy target"):
        _request(
            target=_repair_target(),
            status=PlannerRequestStatus.REJECTED,
            completed_at=NOW,
            response_evidence_compatibility=compatibility,
        )

    completed = _completed_request("marked-with-payload")
    with pytest.raises(ValidationError, match="cannot include response_payload"):
        completed.model_copy(
            update={"response_evidence_compatibility": compatibility}
        )


def test_legacy_response_compatibility_rejects_unknown_value():
    payload = _request(
        status=PlannerRequestStatus.REJECTED,
        completed_at=NOW,
    ).model_dump(mode="python")
    payload["response_evidence_compatibility"] = "UNKNOWN_COMPATIBILITY"

    with pytest.raises(ValidationError, match="response_evidence_compatibility"):
        PlannerRequest.model_validate(payload)


def test_store_rejects_new_or_lifecycle_introduced_response_compatibility(tmp_path):
    store = WorkflowStore(tmp_path / "ordinary-save-compatibility.sqlite3")
    compatibility = (
        PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD
    )
    marked = _request(
        "marked-new-request",
        status=PlannerRequestStatus.REJECTED,
        completed_at=NOW,
        response_evidence_compatibility=compatibility,
    )

    with pytest.raises(ValueError, match="migration or replay normalization"):
        store.save_planner_request(marked)
    assert store.get_planner_request(marked.planner_request_id) is None

    pending = _request("marked-lifecycle-request")
    store.save_planner_request(pending)
    marked_completion = pending.model_copy(
        update={
            "status": PlannerRequestStatus.REJECTED,
            "completed_at": NOW + timedelta(seconds=1),
            "response_evidence_compatibility": compatibility,
        }
    )
    with pytest.raises(ValueError, match="migration or replay normalization"):
        store.save_planner_request(marked_completion)
    assert store.get_planner_request(pending.planner_request_id) == pending


def test_store_rejects_identity_changes_and_allows_lifecycle_updates(tmp_path):
    store = WorkflowStore(tmp_path / "identity.sqlite3")
    request = _request()
    store.save_planner_request(request)

    changed = [
        request.model_copy(update={"game_session_id": "game-2"}),
        request.model_copy(update={"target": _legacy_target(group_id="group-2")}),
        request.model_copy(update={"input_projection_hash": "input-2"}),
        request.model_copy(update={"input_projection_version": "decision-input/v2"}),
        request.model_copy(update={"created_at": NOW + timedelta(seconds=1)}),
    ]
    for candidate in changed:
        with pytest.raises(ValueError, match="immutable"):
            store.save_planner_request(candidate)

    in_progress = request.model_copy(
        update={"status": PlannerRequestStatus.IN_PROGRESS}
    )
    store.save_planner_request(in_progress)
    assert store.get_planner_request(request.planner_request_id) == in_progress


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("turn_number", 5),
        ("observation_id", "obs-2"),
        ("input_projection", {"projection": 2}),
        ("request_payload", {"request": "changed"}),
        ("policy_revision", "policy-2"),
        ("approval_contract_hash", "approval-2"),
        ("allowed_actions_hash", "actions-2"),
        ("model_settings", {"provider": "changed"}),
        ("context_bytes", 99),
    ],
)
def test_store_rejects_creation_definition_changes_without_writing(
    tmp_path, field, value
):
    path = tmp_path / f"immutable-{field}.sqlite3"
    store = WorkflowStore(path)
    request = _request()
    store.save_planner_request(request)

    with pytest.raises(ValueError, match="creation definition is immutable"):
        store.save_planner_request(request.model_copy(update={field: value}))

    assert store.get_planner_request(request.planner_request_id) == request
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            """
            SELECT turn, request_json FROM logical_planner_requests
            WHERE planner_request_id=?
            """,
            (request.planner_request_id,),
        ).fetchone()
    assert row[0] == 4
    assert json.loads(row[1])["turn_number"] == 4


def test_store_rejects_new_legacy_request_without_decision_group(tmp_path):
    store = WorkflowStore(tmp_path / "new-legacy-no-group.sqlite3")
    request = _request(target=_legacy_target(group_id=None))

    with pytest.raises(ValueError, match="requires a DecisionGroup"):
        store.save_planner_request(request)

    assert store.get_planner_request(request.planner_request_id) is None


def test_store_requires_payload_for_new_and_newly_completed_requests(tmp_path):
    store = WorkflowStore(tmp_path / "completion-payload.sqlite3")
    historical_shape = _request(
        "new-completed",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW + timedelta(seconds=1),
        response_hash="legacy-response",
        validation_result={"result": "completed"},
    )
    with pytest.raises(ValueError, match="requires.*response_payload"):
        store.save_planner_request(historical_shape)
    assert store.get_planner_request(historical_shape.planner_request_id) is None

    pending = _request("pending-completion", input_hash="pending-input")
    store.save_planner_request(pending)
    missing_payload = pending.model_copy(
        update={
            "status": PlannerRequestStatus.COMPLETED,
            "completed_at": NOW + timedelta(seconds=1),
            "response_hash": "legacy-response",
            "validation_result": {"result": "completed"},
        }
    )
    with pytest.raises(ValueError, match="requires.*response_payload"):
        store.save_planner_request(missing_payload)
    assert store.get_planner_request(pending.planner_request_id) == pending

    payload = canonical_workflow_plan_bundle_payload(
        {"plan_id": "plan-1", "summary": "accepted", "tasks": []}
    )
    completed = pending.model_copy(
        update={
            "status": PlannerRequestStatus.COMPLETED,
            "completed_at": NOW + timedelta(seconds=1),
            "response_payload": payload,
            "response_hash": canonical_json_hash(payload),
            "validation_result": {"result": "completed"},
        }
    )
    store.save_planner_request(completed)
    assert store.get_planner_request(pending.planner_request_id) == completed


def test_store_schema_failure_requires_an_existing_request(tmp_path):
    store = WorkflowStore(tmp_path / "new-schema-failure.sqlite3")
    request = _schema_failure_request("new-schema-failure")

    with pytest.raises(
        ValueError,
        match="schema failure can only be committed by a lifecycle transition",
    ):
        store.save_planner_request(request)

    assert store.get_planner_request(request.planner_request_id) is None


def test_store_lifecycle_can_commit_and_reopen_schema_failure(tmp_path):
    path = tmp_path / "lifecycle-schema-failure.sqlite3"
    store = WorkflowStore(path)
    pending = _request("lifecycle-schema-failure")
    store.save_planner_request(pending)
    in_progress = pending.model_copy(
        update={
            "status": PlannerRequestStatus.IN_PROGRESS,
            "provider_attempt_count": 1,
        }
    )
    store.save_planner_request(in_progress)
    rejected = in_progress.model_copy(
        update={
            "status": PlannerRequestStatus.REJECTED,
            "completed_at": NOW + timedelta(seconds=1),
            "failure_category": "planner_contract_failure",
        }
    )

    store.save_planner_request(rejected)
    store.save_planner_request(rejected)

    assert store.get_planner_request(rejected.planner_request_id) == rejected
    assert WorkflowStore(path).get_planner_request(
        rejected.planner_request_id
    ) == rejected
    with pytest.raises(ValueError, match="terminal PlannerRequest row is immutable"):
        store.save_planner_request(
            rejected.model_copy(update={"failure_category": "rewritten"})
        )
    assert store.get_planner_request(rejected.planner_request_id) == rejected


def test_v8_read_and_startup_reject_the_same_missing_response_shape(tmp_path):
    path = tmp_path / "invalid-v8-response-shape.sqlite3"
    store = WorkflowStore(path)
    pending = _request("invalid-v8-response-shape")
    store.save_planner_request(pending)
    invalid = pending.model_copy(
        update={
            "status": PlannerRequestStatus.REJECTED,
            "completed_at": NOW + timedelta(seconds=1),
            "failure_category": "ordinary_rejection",
        }
    )
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE logical_planner_requests
            SET status=?, request_json=?, completed_at=?
            WHERE planner_request_id=?
            """,
            (
                invalid.status.value,
                invalid.model_dump_json(),
                invalid.completed_at.isoformat(),
                invalid.planner_request_id,
            ),
        )

    with pytest.raises(ValueError, match="requires.*response_payload"):
        store.get_planner_request(invalid.planner_request_id)
    with pytest.raises(ValueError, match="requires.*response_payload"):
        WorkflowStore(path)


@pytest.mark.parametrize(
    "updates",
    [
        {
            "response_payload": {"plan_id": "replacement", "tasks": []},
            "response_hash": canonical_json_hash(
                {"plan_id": "replacement", "tasks": []}
            ),
        },
        {"status": PlannerRequestStatus.PARTIALLY_COMPLETED},
        {"completed_at": NOW + timedelta(seconds=2)},
        {"validation_result": {"result": "rewritten"}},
        {"failure_category": "rewritten_failure"},
    ],
    ids=[
        "payload-and-matching-hash",
        "completed-to-partially-completed",
        "completed-at",
        "validation-result",
        "failure-category",
    ],
)
def test_completed_response_facts_are_permanently_immutable(tmp_path, updates):
    store = WorkflowStore(tmp_path / "terminal-facts.sqlite3")
    completed = _completed_request("terminal-facts")
    store.save_planner_request(completed)

    with pytest.raises(ValueError, match="terminal PlannerRequest row is immutable"):
        store.save_planner_request(completed.model_copy(update=updates))

    assert store.get_planner_request(completed.planner_request_id) == completed


def test_store_validates_relational_target_columns_on_read(tmp_path):
    path = tmp_path / "read-validation.sqlite3"
    store = WorkflowStore(path)
    request = _request()
    store.save_planner_request(request)

    for column, value in (
        ("request_target_kind", PlannerRequestTargetKind.MISSION_GRAPH_REPAIR.value),
        ("request_target_key", "planner-target:corrupt"),
    ):
        with sqlite3.connect(path) as conn:
            conn.execute(
                f"UPDATE logical_planner_requests SET {column}=?",
                (value,),
            )
        with pytest.raises(ValueError):
            store.get_planner_request(request.planner_request_id)
        with sqlite3.connect(path) as conn:
            conn.execute(
                f"UPDATE logical_planner_requests SET {column}=?",
                (
                    request.target.kind.value
                    if column == "request_target_kind"
                    else request.target.target_key,
                ),
            )


def test_store_uniqueness_uses_target_key_and_input_hash(tmp_path):
    store = WorkflowStore(tmp_path / "unique.sqlite3")
    first = _request("request-1")
    store.save_planner_request(first)

    with pytest.raises(sqlite3.IntegrityError):
        store.save_planner_request(
            _request(
                "request-duplicate",
                target=_legacy_target(group_id="group-2"),
            )
        )

    store.save_planner_request(_request("request-new-input", input_hash="input-2"))
    store.save_planner_request(
        _request("request-new-target", target=_legacy_target(gap_ids=("gap-2",)))
    )
    assert store.planner_request_for_input(
        "game-1", first.target.target_key, first.input_projection_hash
    ) == first


def _legacy_request_json(request: PlannerRequest) -> str:
    payload = request.model_dump(mode="json")
    payload.pop("response_evidence_compatibility", None)
    target = payload.pop("target")
    payload["decision_group_id"] = target["decision_group_id"]
    payload["decision_gap_ids"] = target["decision_gap_ids"]
    return canonical_json(payload)


def _v7_request_row(request: PlannerRequest) -> dict:
    return {
        "planner_request_id": request.planner_request_id,
        "game_id": request.game_session_id,
        "decision_group_id": request.decision_group_id,
        "turn": request.turn_number,
        "status": request.status.value,
        "input_projection_hash": request.input_projection_hash,
        "input_projection_version": request.input_projection_version,
        "decision_gap_ids_json": canonical_json(list(request.decision_gap_ids)),
        "request_json": _legacy_request_json(request),
        "created_at": request.created_at.isoformat(),
        "completed_at": (
            None if request.completed_at is None else request.completed_at.isoformat()
        ),
    }


def _create_v7_database(
    path,
    requests,
    *,
    attempts=(),
    rounds=(),
    partial_column=None,
):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE logical_planner_requests (
                planner_request_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                decision_group_id TEXT,
                turn INTEGER NOT NULL,
                status TEXT NOT NULL,
                input_projection_hash TEXT NOT NULL,
                input_projection_version TEXT NOT NULL,
                decision_gap_ids_json TEXT NOT NULL,
                request_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                UNIQUE (game_id, decision_group_id, input_projection_hash)
            );
            CREATE TABLE provider_attempts (
                provider_attempt_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                planner_request_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                provider_request_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                UNIQUE (planner_request_id, attempt_number)
            );
            CREATE TABLE information_rounds (
                information_round_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                planner_request_id TEXT NOT NULL,
                round_number INTEGER NOT NULL,
                status TEXT NOT NULL,
                round_json TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                completed_at TEXT,
                UNIQUE (planner_request_id, round_number)
            );
            """
        )
        for request in requests:
            row = _v7_request_row(request)
            columns = tuple(row)
            conn.execute(
                f"INSERT INTO logical_planner_requests "
                f"({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                tuple(row[column] for column in columns),
            )
        for attempt in attempts:
            conn.execute(
                """
                INSERT INTO provider_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt.provider_attempt_id,
                    "game-1",
                    attempt.planner_request_id,
                    attempt.attempt_number,
                    attempt.provider_request_id,
                    attempt.status.value,
                    attempt.model_dump_json(),
                    attempt.started_at.isoformat(),
                    (
                        None
                        if attempt.completed_at is None
                        else attempt.completed_at.isoformat()
                    ),
                ),
            )
        for round_record in rounds:
            conn.execute(
                """
                INSERT INTO information_rounds VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_record.information_round_id,
                    "game-1",
                    round_record.planner_request_id,
                    round_record.round_number,
                    round_record.status.value,
                    round_record.model_dump_json(),
                    round_record.requested_at.isoformat(),
                    (
                        None
                        if round_record.completed_at is None
                        else round_record.completed_at.isoformat()
                    ),
                ),
            )
        if partial_column == "request_target_kind":
            conn.execute(
                "ALTER TABLE logical_planner_requests "
                "ADD COLUMN request_target_kind TEXT"
            )
            conn.execute(
                "UPDATE logical_planner_requests SET request_target_kind=?",
                (PlannerRequestTargetKind.LEGACY_DECISION_GROUP.value,),
            )
        elif partial_column == "request_target_key":
            conn.execute(
                "ALTER TABLE logical_planner_requests ADD COLUMN request_target_key TEXT"
            )
            for request in requests:
                conn.execute(
                    """
                    UPDATE logical_planner_requests SET request_target_key=?
                    WHERE planner_request_id=?
                    """,
                    (request.target.target_key, request.planner_request_id),
                )
        conn.execute("PRAGMA user_version=7")


def test_v7_to_v8_migration_preserves_status_attempt_round_and_canonical_json(
    tmp_path,
):
    path = tmp_path / "v7.sqlite3"
    completed = _request(
        "completed",
        input_hash="input-completed",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW,
        response_hash="legacy-response",
        validation_result={"result": "completed"},
    )
    failed = _request(
        "failed",
        target=_legacy_target(group_id=None, gap_ids=("gap-no-group",)),
        input_hash="input-failed",
        status=PlannerRequestStatus.FAILED,
        completed_at=NOW,
    )
    backoff = _request(
        "backoff",
        target=_legacy_target(gap_ids=("gap-backoff",)),
        input_hash="input-backoff",
        status=PlannerRequestStatus.BACKOFF,
    )
    superseded = _request(
        "superseded",
        target=_legacy_target(gap_ids=("gap-superseded",)),
        input_hash="input-superseded",
        status=PlannerRequestStatus.SUPERSEDED,
        completed_at=NOW,
    )
    awaiting = _request(
        "awaiting",
        target=_legacy_target(gap_ids=("gap-awaiting",)),
        input_hash="input-awaiting",
    ).model_copy(
        update={
            "status": PlannerRequestStatus.AWAITING_INFORMATION,
            "pending_information_requests": ({"query": "research"},),
        }
    )
    attempt = ProviderAttempt(
        provider_attempt_id="attempt-1",
        planner_request_id=backoff.planner_request_id,
        attempt_number=1,
        provider_request_id="provider-1",
        status=ProviderAttemptStatus.FAILED,
        started_at=NOW,
        completed_at=NOW,
        latency_seconds=0,
        failure_category="provider_failure",
    )
    round_record = InformationRound(
        information_round_id="round-1",
        planner_request_id=awaiting.planner_request_id,
        round_number=1,
        status=InformationRoundStatus.REQUESTED,
        requests=({"query": "research"},),
        requested_at=NOW,
    )
    requests = (completed, failed, backoff, superseded, awaiting)
    _create_v7_database(path, requests, attempts=(attempt,), rounds=(round_record,))

    store = WorkflowStore(path)

    assert {
        store.get_planner_request(request.planner_request_id).status
        for request in requests
    } == {
        PlannerRequestStatus.COMPLETED,
        PlannerRequestStatus.FAILED,
        PlannerRequestStatus.BACKOFF,
        PlannerRequestStatus.SUPERSEDED,
        PlannerRequestStatus.AWAITING_INFORMATION,
    }
    assert store.get_planner_request("failed").decision_group_id is None
    restored_completed = store.get_planner_request("completed")
    assert restored_completed.response_payload is None
    assert (
        restored_completed.response_evidence_compatibility
        is PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD
    )
    assert store.list_provider_attempts("backoff") == [attempt]
    assert store.list_information_rounds("awaiting") == [round_record]
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 8
        rows = conn.execute(
            "SELECT * FROM logical_planner_requests ORDER BY planner_request_id"
        ).fetchall()
        assert all(row["request_target_kind"] for row in rows)
        assert all(row["request_target_key"] for row in rows)
        assert all("target" in json.loads(row["request_json"]) for row in rows)
        assert all(
            "decision_gap_ids" not in json.loads(row["request_json"]) for row in rows
        )
        indexes = {
            row[1] for row in conn.execute(
                "PRAGMA index_list(logical_planner_requests)"
            )
        }
        assert "idx_logical_requests_target_input" in indexes

    reopened = WorkflowStore(path)
    assert reopened.get_planner_request("completed") == store.get_planner_request(
        "completed"
    )


def test_migrated_legacy_request_without_group_allows_lifecycle_updates(tmp_path):
    path = tmp_path / "historical-no-group.sqlite3"
    historical = _request(
        "historical-no-group",
        target=_legacy_target(group_id=None, gap_ids=("historical-gap",)),
    )
    _create_v7_database(path, (historical,))
    store = WorkflowStore(path)

    in_progress = historical.model_copy(
        update={"status": PlannerRequestStatus.IN_PROGRESS}
    )
    store.save_planner_request(in_progress)

    assert store.get_planner_request(historical.planner_request_id) == in_progress


def test_migrated_completed_request_without_payload_preserves_response_facts(tmp_path):
    path = tmp_path / "historical-completed.sqlite3"
    historical = _request(
        "historical-completed",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW + timedelta(seconds=1),
        response_hash="historical-response",
        validation_result={"result": "completed"},
    )
    _create_v7_database(path, (historical,))
    store = WorkflowStore(path)
    restored = store.get_planner_request(historical.planner_request_id)

    assert restored.response_payload is None
    store.save_planner_request(restored)
    assert store.get_planner_request(historical.planner_request_id) == restored

    changed_count = restored.model_copy(update={"provider_attempt_count": 1})
    with pytest.raises(ValueError, match="terminal PlannerRequest row is immutable"):
        store.save_planner_request(changed_count)
    assert store.get_planner_request(historical.planner_request_id) == restored

    payload = canonical_workflow_plan_bundle_payload(
        {
            "plan_id": "invented-history",
            "summary": "must not be backfilled",
            "tasks": [],
        }
    )
    backfilled_response = restored.model_copy(
        update={
            "response_evidence_compatibility": None,
            "response_payload": payload,
            "response_hash": canonical_json_hash(payload),
            "validation_result": {"result": "backfilled"},
        }
    )
    with pytest.raises(ValueError, match="terminal PlannerRequest row is immutable"):
        store.save_planner_request(backfilled_response)
    assert store.get_planner_request(historical.planner_request_id) == restored

    changed_response = restored.model_copy(
        update={"response_hash": "rewritten-history"}
    )
    with pytest.raises(ValueError, match="terminal PlannerRequest row is immutable"):
        store.save_planner_request(changed_response)
    assert store.get_planner_request(historical.planner_request_id) == restored


@pytest.mark.parametrize(
    "partial_column",
    ["request_target_kind", "request_target_key"],
)
def test_v8_migration_recovers_one_column_interruption(tmp_path, partial_column):
    path = tmp_path / f"partial-{partial_column}.sqlite3"
    request = _request()
    _create_v7_database(path, (request,), partial_column=partial_column)

    store = WorkflowStore(path)

    assert store.get_planner_request(request.planner_request_id).target == request.target
    with sqlite3.connect(path) as conn:
        columns = {
            row[1] for row in conn.execute(
                "PRAGMA table_info(logical_planner_requests)"
            )
        }
        assert {"request_target_kind", "request_target_key"} <= columns
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 8


def test_v8_migration_rolls_back_canonical_duplicates(tmp_path):
    path = tmp_path / "duplicates.sqlite3"
    first = _request("request-1", target=_legacy_target(group_id="group-1"))
    duplicate = _request(
        "request-2",
        target=_legacy_target(group_id="group-2"),
    )
    _create_v7_database(path, (first, duplicate))

    with pytest.raises(ValueError, match="duplicate canonical"):
        WorkflowStore(path)

    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 7
        assert conn.execute(
            "SELECT COUNT(*) FROM logical_planner_requests"
        ).fetchone()[0] == 2


def test_v8_migration_fails_closed_on_relational_json_conflict(tmp_path):
    path = tmp_path / "conflict.sqlite3"
    request = _request()
    _create_v7_database(path, (request,))
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            UPDATE logical_planner_requests SET decision_group_id='other-group'
            """
        )

    with pytest.raises(ValueError, match="conflict"):
        WorkflowStore(path)
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 7


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("planner_request_id", "row-request-id"),
        ("turn", 5),
        ("created_at", (NOW + timedelta(seconds=1)).isoformat()),
        ("completed_at", (NOW + timedelta(seconds=2)).isoformat()),
    ],
)
def test_v8_migration_rolls_back_core_relational_json_conflicts(
    tmp_path, column, value
):
    path = tmp_path / f"conflict-{column}.sqlite3"
    request = _request(
        "json-request-id",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW + timedelta(seconds=1),
        response_hash="historical-response",
        validation_result={"result": "completed"},
    )
    _create_v7_database(path, (request,))
    with sqlite3.connect(path) as conn:
        conn.execute(
            f"UPDATE logical_planner_requests SET {column}=?",
            (value,),
        )

    with pytest.raises(ValueError, match=f"{column} conflicts"):
        WorkflowStore(path)

    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 7
        assert conn.execute(
            "SELECT COUNT(*) FROM logical_planner_requests"
        ).fetchone()[0] == 1


def test_v8_migration_accepts_equivalent_timestamp_formats_and_normalizes(tmp_path):
    path = tmp_path / "timestamp-formats.sqlite3"
    request = _request(
        "timestamp-formats",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW + timedelta(seconds=1),
        response_hash="historical-response",
        validation_result={"result": "completed"},
    )
    _create_v7_database(path, (request,))
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            "SELECT request_json FROM logical_planner_requests"
        ).fetchone()
        payload = json.loads(row[0])
        payload["created_at"] = request.created_at.isoformat()
        payload["completed_at"] = request.completed_at.isoformat()
        conn.execute(
            """
            UPDATE logical_planner_requests
            SET created_at=?, completed_at=?, request_json=?
            """,
            (
                request.created_at.isoformat().replace("+00:00", "Z"),
                request.completed_at.isoformat().replace("+00:00", "Z"),
                canonical_json(payload),
            ),
        )

    store = WorkflowStore(path)

    assert store.get_planner_request(
        request.planner_request_id
    ) == request.model_copy(
        update={
            "response_evidence_compatibility": (
                PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD
            )
        }
    )
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            "SELECT created_at, completed_at FROM logical_planner_requests"
        ).fetchone()
    assert row == (
        request.created_at.isoformat(),
        request.completed_at.isoformat(),
    )


def test_future_database_version_fails_before_content_changes(tmp_path):
    path = tmp_path / "future.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE sentinel(value TEXT NOT NULL)")
        conn.execute("INSERT INTO sentinel VALUES ('unchanged')")
        conn.execute("PRAGMA user_version=9")

    with pytest.raises(ValueError, match="unsupported workflow database version 9"):
        WorkflowStore(path)

    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 9
        assert conn.execute("SELECT value FROM sentinel").fetchone()[0] == "unchanged"
        assert conn.execute(
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type='table' AND name='logical_planner_requests'"
        ).fetchone()[0] == 0


def test_all_target_kinds_reuse_provider_attempts_and_information_rounds(tmp_path):
    store = WorkflowStore(tmp_path / "shared-lifecycle.sqlite3")
    targets = (
        _legacy_target(),
        PlannerRequestTarget(
            kind=PlannerRequestTargetKind.STRATEGIC_CONTRACT_CREATION,
            strategic_scope="research",
        ),
        _repair_target(),
    )
    for index, target in enumerate(targets, start=1):
        request = _request(
            f"request-{index}",
            target=target,
            input_hash=f"input-{index}",
        )
        store.save_planner_request(request)
        started = ProviderAttempt(
            provider_attempt_id=f"attempt-{index}",
            planner_request_id=request.planner_request_id,
            attempt_number=1,
            provider_request_id=f"provider-{index}",
            status=ProviderAttemptStatus.STARTED,
            started_at=NOW,
        )
        in_progress = store.start_provider_attempt("game-1", request, started)
        round_record = InformationRound(
            information_round_id=f"round-{index}",
            planner_request_id=request.planner_request_id,
            round_number=1,
            status=InformationRoundStatus.REQUESTED,
            requests=({"query": "research"},),
            requested_at=NOW,
        )
        store.save_information_round("game-1", round_record)
        failed = in_progress.model_copy(
            update={
                "status": PlannerRequestStatus.FAILED,
                "completed_at": NOW + timedelta(seconds=1),
                "failure_category": "test_failure",
            }
        )
        store.save_planner_request(failed)

        assert store.list_provider_attempts(request.planner_request_id)[0].status is (
            ProviderAttemptStatus.ABANDONED
        )
        assert store.list_information_rounds(request.planner_request_id) == [
            round_record
        ]


def test_response_payload_survives_store_restart(tmp_path):
    path = tmp_path / "response.sqlite3"
    request = _completed_request("request-response")
    WorkflowStore(path).save_planner_request(request)

    assert WorkflowStore(path).get_planner_request(request.planner_request_id) == request


def _v7_replay_state(request, *, attempt=None, round_record=None):
    tables = {"logical_planner_requests": [_v7_request_row(request)]}
    if attempt is not None:
        tables["provider_attempts"] = [_provider_attempt_row(attempt)]
    if round_record is not None:
        tables["information_rounds"] = [
            {
                "information_round_id": round_record.information_round_id,
                "game_id": "game-1",
                "planner_request_id": round_record.planner_request_id,
                "round_number": round_record.round_number,
                "status": round_record.status.value,
                "round_json": round_record.model_dump_json(),
                "requested_at": round_record.requested_at.isoformat(),
                "completed_at": (
                    None
                    if round_record.completed_at is None
                    else round_record.completed_at.isoformat()
                ),
            }
        ]
    return {"game_id": "game-1", "tables": tables}


def _provider_attempt_row(
    attempt: ProviderAttempt,
    *,
    game_id: str = "game-1",
) -> dict:
    return {
        "provider_attempt_id": attempt.provider_attempt_id,
        "game_id": game_id,
        "planner_request_id": attempt.planner_request_id,
        "attempt_number": attempt.attempt_number,
        "provider_request_id": attempt.provider_request_id,
        "status": attempt.status.value,
        "attempt_json": attempt.model_dump_json(),
        "started_at": attempt.started_at.isoformat(),
        "completed_at": (
            None
            if attempt.completed_at is None
            else attempt.completed_at.isoformat()
        ),
    }


def test_v7_replay_import_canonicalizes_request_and_preserves_children(tmp_path):
    path = tmp_path / "replay.sqlite3"
    request = _request(
        "request-replay",
        target=_legacy_target(group_id=None, gap_ids=("gap-replay",)),
    ).model_copy(
        update={
            "status": PlannerRequestStatus.AWAITING_INFORMATION,
            "pending_information_requests": ({"query": "research"},),
        }
    )
    attempt = ProviderAttempt(
        provider_attempt_id="attempt-replay",
        planner_request_id=request.planner_request_id,
        attempt_number=1,
        provider_request_id="provider-replay",
        status=ProviderAttemptStatus.SUCCEEDED,
        started_at=NOW,
        completed_at=NOW,
        latency_seconds=0,
    )
    round_record = InformationRound(
        information_round_id="round-replay",
        planner_request_id=request.planner_request_id,
        round_number=1,
        status=InformationRoundStatus.REQUESTED,
        requests=({"query": "research"},),
        requested_at=NOW,
    )
    store = WorkflowStore(path)

    store.import_replay_state(
        _v7_replay_state(request, attempt=attempt, round_record=round_record)
    )

    restored = store.get_planner_request(request.planner_request_id)
    assert restored.target == request.target
    assert store.list_provider_attempts(request.planner_request_id) == [attempt]
    assert store.list_information_rounds(request.planner_request_id) == [
        round_record
    ]
    exported = store.export_replay_state("game-1")
    row = exported["tables"]["logical_planner_requests"][0]
    payload = json.loads(row["request_json"])
    assert row["request_target_kind"] == request.target.kind.value
    assert row["request_target_key"] == request.target.target_key
    assert "target" in payload
    assert "decision_gap_ids" not in payload


def test_v7_replay_turn_conflict_rolls_back_entire_import(tmp_path):
    store = WorkflowStore(tmp_path / "replay-turn-conflict.sqlite3")
    seed = _request(
        "seed-turn-conflict",
        target=_legacy_target(gap_ids=("seed-turn-gap",)),
        input_hash="seed-turn-input",
    )
    store.save_planner_request(seed)
    replay_request = _request(
        "replay-turn-conflict",
        target=_legacy_target(group_id=None, gap_ids=("replay-turn-gap",)),
        input_hash="replay-turn-input",
    )
    state = _v7_replay_state(replay_request)
    state["tables"]["logical_planner_requests"][0]["turn"] += 1

    with pytest.raises(ValueError, match="turn conflicts"):
        store.import_replay_state(state)

    assert store.get_planner_request(seed.planner_request_id) == seed
    assert store.get_planner_request(replay_request.planner_request_id) is None


def test_replay_canonical_duplicate_rolls_back_entire_import(tmp_path):
    store = WorkflowStore(tmp_path / "replay-duplicate.sqlite3")
    seed = _request(
        "seed",
        target=_legacy_target(gap_ids=("seed-gap",)),
        input_hash="seed-input",
    )
    store.save_planner_request(seed)
    first = _request("request-1", target=_legacy_target(group_id="group-1"))
    duplicate = _request(
        "request-2",
        target=_legacy_target(group_id="group-2"),
    )
    state = {
        "game_id": "game-1",
        "tables": {
            "logical_planner_requests": [
                _v7_request_row(first),
                _v7_request_row(duplicate),
            ]
        },
    }

    with pytest.raises(sqlite3.IntegrityError):
        store.import_replay_state(state)

    assert store.get_planner_request(seed.planner_request_id) == seed
    assert store.get_planner_request(first.planner_request_id) is None


def test_store_rejects_noncanonical_and_arbitrary_legacy_responses(tmp_path):
    store = WorkflowStore(tmp_path / "canonical-response.sqlite3")
    arbitrary = {"answer": 42}
    arbitrary_request = _request(
        "arbitrary-response",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW,
        response_payload=arbitrary,
        response_hash=canonical_json_hash(arbitrary),
        validation_result={"result": "completed"},
    )
    with pytest.raises(ValueError):
        store.save_planner_request(arbitrary_request)

    noncanonical = {"summary": "defaults omitted"}
    noncanonical_request = _request(
        "noncanonical-response",
        input_hash="noncanonical-input",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW,
        response_payload=noncanonical,
        response_hash=canonical_json_hash(noncanonical),
        validation_result={"result": "completed"},
    )
    with pytest.raises(ValueError, match="canonical WorkflowPlanBundle"):
        store.save_planner_request(noncanonical_request)

    canonical = canonical_workflow_plan_bundle_payload(
        WorkflowPlanBundle(summary="canonical response")
    )
    accepted = _request(
        "canonical-response",
        input_hash="canonical-input",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW,
        response_payload=canonical,
        response_hash=canonical_json_hash(canonical),
        validation_result={"result": "completed"},
    )
    store.save_planner_request(accepted)
    assert store.get_planner_request(accepted.planner_request_id) == accepted


def test_store_rejects_nonlegacy_response_before_phase1b(tmp_path):
    store = WorkflowStore(tmp_path / "nonlegacy-response.sqlite3")
    request = _completed_request(
        "nonlegacy-response",
        target=_repair_target(),
    )

    with pytest.raises(
        ValueError,
        match="non-legacy planner response contract is not enabled before Phase 1B",
    ):
        store.save_planner_request(request)
    assert store.get_planner_request(request.planner_request_id) is None


def _terminal_request(
    request_id: str,
    status: PlannerRequestStatus,
) -> PlannerRequest:
    if status in {
        PlannerRequestStatus.COMPLETED,
        PlannerRequestStatus.PARTIALLY_COMPLETED,
    }:
        completed = _completed_request(request_id)
        return completed.model_copy(update={"status": status})
    if status is PlannerRequestStatus.REJECTED:
        payload = canonical_workflow_plan_bundle_payload(
            WorkflowPlanBundle(summary="rejected response")
        )
        return _request(
            request_id,
            status=status,
            completed_at=NOW,
            response_payload=payload,
            response_hash=canonical_json_hash(payload),
            validation_result={"result": "rejected"},
        ).model_copy(
            update={"failure_category": "invalid_planner_output_item"}
        )
    return _request(
        request_id,
        status=status,
        completed_at=NOW,
        validation_result=None,
    )


@pytest.mark.parametrize(
    ("status", "replacement"),
    [
        (PlannerRequestStatus.FAILED, PlannerRequestStatus.PENDING),
        (PlannerRequestStatus.REJECTED, PlannerRequestStatus.IN_PROGRESS),
        (PlannerRequestStatus.SUPERSEDED, PlannerRequestStatus.COMPLETED),
        (PlannerRequestStatus.CANCELLED, PlannerRequestStatus.BACKOFF),
        (
            PlannerRequestStatus.PARTIALLY_COMPLETED,
            PlannerRequestStatus.COMPLETED,
        ),
        (PlannerRequestStatus.COMPLETED, PlannerRequestStatus.COMPLETED),
    ],
)
def test_every_planner_terminal_state_freezes_the_entire_row(
    tmp_path,
    status,
    replacement,
):
    store = WorkflowStore(tmp_path / f"terminal-{status.value}.sqlite3")
    terminal = _terminal_request(f"terminal-{status.value}", status)
    store.save_planner_request(terminal)
    store.save_planner_request(terminal)

    if status is PlannerRequestStatus.COMPLETED:
        changed = terminal.model_copy(
            update={
                "provider_attempt_count": terminal.provider_attempt_count + 1
            }
        )
    elif replacement is PlannerRequestStatus.COMPLETED:
        payload = canonical_workflow_plan_bundle_payload(
            WorkflowPlanBundle(summary="reactivation attempt")
        )
        changed = terminal.model_copy(
            update={
                "status": replacement,
                "response_payload": payload,
                "response_hash": canonical_json_hash(payload),
                "validation_result": {"result": "completed"},
            }
        )
    else:
        changed = terminal.model_copy(
            update={
                "status": replacement,
                "completed_at": None,
                "response_payload": None,
                "response_hash": None,
                "validation_result": None,
            }
        )
    with pytest.raises(ValueError, match="terminal PlannerRequest row is immutable"):
        store.save_planner_request(changed)

    assert store.get_planner_request(terminal.planner_request_id) == terminal


def test_provider_attempt_identity_transition_and_read_consistency(tmp_path):
    path = tmp_path / "provider-attempt-safety.sqlite3"
    store = WorkflowStore(path)
    request_a = _request("provider-parent-a")
    request_b = _request(
        "provider-parent-b",
        target=_legacy_target(group_id="group-b", gap_ids=("gap-b",)),
        input_hash="provider-input-b",
    )
    store.save_planner_request(request_a)
    store.save_planner_request(request_b)
    started = ProviderAttempt(
        provider_attempt_id="provider-attempt",
        planner_request_id=request_a.planner_request_id,
        attempt_number=1,
        provider_request_id="provider-call",
        status=ProviderAttemptStatus.STARTED,
        started_at=NOW,
    )
    store.save_provider_attempt("game-1", started)

    for changed in (
        started.model_copy(update={"attempt_number": 2}),
        started.model_copy(update={"provider_request_id": "other-call"}),
        started.model_copy(
            update={"planner_request_id": request_b.planner_request_id}
        ),
    ):
        with pytest.raises(ValueError, match="creation identity is immutable"):
            store.save_provider_attempt("game-1", changed)

    succeeded = started.model_copy(
        update={
            "status": ProviderAttemptStatus.SUCCEEDED,
            "completed_at": NOW + timedelta(seconds=1),
            "latency_seconds": 1,
        }
    )
    store.save_provider_attempt("game-1", succeeded)
    store.save_provider_attempt("game-1", succeeded)
    with pytest.raises(ValueError, match="terminal ProviderAttempt is immutable"):
        store.save_provider_attempt("game-1", started)
    assert store.list_provider_attempts(request_a.planner_request_id) == [succeeded]

    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE provider_attempts SET planner_request_id=? "
            "WHERE provider_attempt_id=?",
            (request_b.planner_request_id, started.provider_attempt_id),
        )
    with pytest.raises(ValueError, match="conflicts with attempt_json"):
        store.list_provider_attempts(request_b.planner_request_id)


def test_child_save_requires_an_existing_parent_in_the_same_game(tmp_path):
    store = WorkflowStore(tmp_path / "child-parent-safety.sqlite3")
    parent = _request("child-parent").model_copy(
        update={"game_session_id": "game-B"}
    )
    store.save_planner_request(parent)
    attempt = ProviderAttempt(
        provider_attempt_id="child-parent-attempt",
        planner_request_id=parent.planner_request_id,
        attempt_number=1,
        provider_request_id="child-parent-provider",
        status=ProviderAttemptStatus.STARTED,
        started_at=NOW,
    )
    round_record = InformationRound(
        information_round_id="child-parent-round",
        planner_request_id=parent.planner_request_id,
        round_number=1,
        status=InformationRoundStatus.REQUESTED,
        requests=({"query": "research"},),
        requested_at=NOW,
    )

    with pytest.raises(ValueError, match="game_id conflicts"):
        store.save_provider_attempt("game-1", attempt)
    with pytest.raises(ValueError, match="game_id conflicts"):
        store.save_information_round("game-1", round_record)

    missing_attempt = attempt.model_copy(
        update={
            "provider_attempt_id": "missing-parent-attempt",
            "planner_request_id": "missing-parent",
        }
    )
    missing_round = round_record.model_copy(
        update={
            "information_round_id": "missing-parent-round",
            "planner_request_id": "missing-parent",
        }
    )
    with pytest.raises(ValueError, match="does not exist"):
        store.save_provider_attempt("game-1", missing_attempt)
    with pytest.raises(ValueError, match="does not exist"):
        store.save_information_round("game-1", missing_round)


def test_information_round_identity_transition_and_read_consistency(tmp_path):
    path = tmp_path / "information-round-safety.sqlite3"
    store = WorkflowStore(path)
    request_a = _request("round-parent-a")
    request_b = _request(
        "round-parent-b",
        target=_legacy_target(group_id="round-group-b", gap_ids=("round-gap-b",)),
        input_hash="round-input-b",
    )
    store.save_planner_request(request_a)
    store.save_planner_request(request_b)
    requested = InformationRound(
        information_round_id="information-round",
        planner_request_id=request_a.planner_request_id,
        round_number=1,
        status=InformationRoundStatus.REQUESTED,
        requests=({"query": "research"},),
        requested_at=NOW,
    )
    store.save_information_round("game-1", requested)

    for changed in (
        requested.model_copy(update={"round_number": 2}),
        requested.model_copy(update={"requests": ({"query": "civic"},)}),
        requested.model_copy(
            update={"planner_request_id": request_b.planner_request_id}
        ),
    ):
        with pytest.raises(ValueError, match="creation identity is immutable"):
            store.save_information_round("game-1", changed)

    collected = requested.model_copy(
        update={
            "status": InformationRoundStatus.COLLECTED,
            "results": {"research": "POTTERY"},
            "completed_at": NOW + timedelta(seconds=1),
        }
    )
    store.save_information_round("game-1", collected)
    store.save_information_round("game-1", collected)
    with pytest.raises(ValueError, match="terminal InformationRound is immutable"):
        store.save_information_round("game-1", requested)
    assert store.list_information_rounds(request_a.planner_request_id) == [collected]

    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE information_rounds SET planner_request_id=? "
            "WHERE information_round_id=?",
            (request_b.planner_request_id, requested.information_round_id),
        )
    with pytest.raises(ValueError, match="conflicts with round_json"):
        store.list_information_rounds(request_b.planner_request_id)


def _v8_request_row(request: PlannerRequest) -> dict:
    row = _v7_request_row(request)
    row.update(
        {
            "request_target_kind": request.target.kind.value,
            "request_target_key": request.target.target_key,
            "request_json": canonical_json(request.model_dump(mode="json")),
        }
    )
    return row


def _schema_failure_replay_state(
    request: PlannerRequest,
    *,
    attempts: tuple[ProviderAttempt, ...] = (),
) -> dict:
    tables = {"logical_planner_requests": [_v8_request_row(request)]}
    if attempts:
        tables["provider_attempts"] = [
            _provider_attempt_row(attempt) for attempt in attempts
        ]
    return {"game_id": request.game_session_id, "tables": tables}


def _attempt(
    request: PlannerRequest,
    number: int,
    status: ProviderAttemptStatus,
) -> ProviderAttempt:
    terminal = status is not ProviderAttemptStatus.STARTED
    return ProviderAttempt(
        provider_attempt_id=f"{request.planner_request_id}-attempt-{number}",
        planner_request_id=request.planner_request_id,
        attempt_number=number,
        provider_request_id=f"provider-{request.planner_request_id}-{number}",
        status=status,
        started_at=NOW,
        completed_at=(NOW + timedelta(seconds=number) if terminal else None),
        latency_seconds=(number if terminal else None),
    )


@pytest.mark.parametrize(
    ("provider_attempt_count", "message"),
    [
        (0, "provider_attempt_count >= 1"),
        (1, "matching final ProviderAttempt"),
    ],
)
def test_replay_schema_failure_requires_provider_attempt_before_delete(
    tmp_path,
    provider_attempt_count,
    message,
):
    store = WorkflowStore(tmp_path / f"missing-attempt-{provider_attempt_count}.sqlite3")
    seed = _request("schema-failure-seed")
    store.save_planner_request(seed)
    request = _schema_failure_request(
        f"schema-failure-missing-{provider_attempt_count}",
        provider_attempt_count=provider_attempt_count,
    )

    with pytest.raises(ValueError, match=message):
        store.import_replay_state(_schema_failure_replay_state(request))

    assert store.get_planner_request(seed.planner_request_id) == seed
    assert store.get_planner_request(request.planner_request_id) is None


@pytest.mark.parametrize(
    "status",
    [
        ProviderAttemptStatus.STARTED,
        ProviderAttemptStatus.FAILED,
        ProviderAttemptStatus.ABANDONED,
    ],
)
def test_replay_schema_failure_requires_succeeded_final_attempt(tmp_path, status):
    store = WorkflowStore(tmp_path / f"wrong-final-attempt-{status.value}.sqlite3")
    seed = _request("wrong-final-attempt-seed")
    store.save_planner_request(seed)
    request = _schema_failure_request(f"wrong-final-attempt-{status.value}")
    attempt = _attempt(request, 1, status)

    with pytest.raises(ValueError, match="SUCCEEDED final ProviderAttempt"):
        store.import_replay_state(
            _schema_failure_replay_state(request, attempts=(attempt,))
        )

    assert store.get_planner_request(seed.planner_request_id) == seed


def test_replay_schema_failure_requires_declared_attempt_number(tmp_path):
    store = WorkflowStore(tmp_path / "schema-failure-attempt-number.sqlite3")
    seed = _request("attempt-number-seed")
    store.save_planner_request(seed)
    request = _schema_failure_request(
        "schema-failure-attempt-number",
        provider_attempt_count=2,
    )
    attempt = _attempt(request, 1, ProviderAttemptStatus.SUCCEEDED)

    with pytest.raises(ValueError, match="matching final ProviderAttempt"):
        store.import_replay_state(
            _schema_failure_replay_state(request, attempts=(attempt,))
        )

    assert store.get_planner_request(seed.planner_request_id) == seed


def test_replay_schema_failure_attempt_cannot_belong_to_another_request(tmp_path):
    store = WorkflowStore(tmp_path / "schema-failure-other-request.sqlite3")
    seed = _request("other-request-seed")
    store.save_planner_request(seed)
    request = _schema_failure_request("schema-failure-parent")
    other = _request(
        "other-attempt-parent",
        target=_legacy_target(
            group_id="other-attempt-group",
            gap_ids=("other-attempt-gap",),
        ),
        input_hash="other-attempt-input",
    )
    attempt = _attempt(other, 1, ProviderAttemptStatus.SUCCEEDED)
    state = _schema_failure_replay_state(request, attempts=(attempt,))
    state["tables"]["logical_planner_requests"].append(_v8_request_row(other))

    with pytest.raises(ValueError, match="matching final ProviderAttempt"):
        store.import_replay_state(state)

    assert store.get_planner_request(seed.planner_request_id) == seed


def test_replay_schema_failure_attempt_cannot_belong_to_another_game(tmp_path):
    store = WorkflowStore(tmp_path / "schema-failure-other-game.sqlite3")
    seed = _request("other-game-seed")
    store.save_planner_request(seed)
    request = _schema_failure_request("schema-failure-game")
    attempt = _attempt(request, 1, ProviderAttemptStatus.SUCCEEDED)
    state = _schema_failure_replay_state(request, attempts=(attempt,))
    state["tables"]["provider_attempts"][0]["game_id"] = "game-2"

    with pytest.raises(ValueError, match="belongs to another game"):
        store.import_replay_state(state)

    assert store.get_planner_request(seed.planner_request_id) == seed


def test_replay_schema_failure_allows_earlier_failed_attempts(tmp_path):
    request = _schema_failure_request(
        "schema-failure-recovered",
        provider_attempt_count=2,
    )
    attempts = (
        _attempt(request, 1, ProviderAttemptStatus.FAILED),
        _attempt(request, 2, ProviderAttemptStatus.SUCCEEDED),
    )
    store = WorkflowStore(tmp_path / "schema-failure-recovered.sqlite3")

    store.import_replay_state(
        _schema_failure_replay_state(request, attempts=attempts)
    )

    assert store.get_planner_request(request.planner_request_id) == request
    assert store.list_provider_attempts(request.planner_request_id) == list(attempts)


def test_planner_contract_failure_cannot_carry_response_evidence(tmp_path):
    payload = canonical_workflow_plan_bundle_payload(
        WorkflowPlanBundle(summary="not a schema failure")
    )
    request = _request(
        "schema-failure-with-payload",
        status=PlannerRequestStatus.REJECTED,
        completed_at=NOW,
        response_payload=payload,
        response_hash=canonical_json_hash(payload),
        validation_result={"result": "rejected"},
    ).model_copy(update={"failure_category": "planner_contract_failure"})
    store = WorkflowStore(tmp_path / "schema-failure-with-payload.sqlite3")

    with pytest.raises(ValueError, match="invalid response facts"):
        store.save_planner_request(request)

    assert store.get_planner_request(request.planner_request_id) is None


@pytest.mark.parametrize(
    "status",
    [
        PlannerRequestStatus.COMPLETED,
        PlannerRequestStatus.PARTIALLY_COMPLETED,
        PlannerRequestStatus.REJECTED,
    ],
)
def test_v8_replay_terminal_legacy_request_requires_response_evidence(
    tmp_path,
    status,
):
    store = WorkflowStore(tmp_path / f"v8-missing-response-{status.value}.sqlite3")
    seed = _request("v8-replay-seed")
    store.save_planner_request(seed)
    missing = _request(
        f"v8-missing-{status.value}",
        target=_legacy_target(
            group_id=f"group-{status.value}",
            gap_ids=(f"gap-{status.value}",),
        ),
        input_hash=f"input-{status.value}",
        status=status,
        completed_at=NOW,
        response_hash=(
            "historical-hash"
            if status is not PlannerRequestStatus.REJECTED
            else None
        ),
        validation_result=(
            {"result": status.value.lower()}
            if status is not PlannerRequestStatus.REJECTED
            else None
        ),
    )
    state = {
        "game_id": "game-1",
        "tables": {"logical_planner_requests": [_v8_request_row(missing)]},
    }

    with pytest.raises(ValueError, match="requires.*response_payload"):
        store.import_replay_state(state)
    assert store.get_planner_request(seed.planner_request_id) == seed


@pytest.mark.parametrize(
    "status",
    [
        PlannerRequestStatus.COMPLETED,
        PlannerRequestStatus.PARTIALLY_COMPLETED,
        PlannerRequestStatus.REJECTED,
    ],
)
def test_v7_terminal_missing_payload_replay_round_trip_is_stable(tmp_path, status):
    store = WorkflowStore(tmp_path / f"v7-terminal-replay-{status.value}.sqlite3")
    historical = _request(
        f"v7-terminal-replay-{status.value}",
        target=_legacy_target(
            group_id=None,
            gap_ids=(f"v7-terminal-gap-{status.value}",),
        ),
        status=status,
        completed_at=NOW,
        response_hash=(
            "historical-response"
            if status is not PlannerRequestStatus.REJECTED
            else None
        ),
        validation_result=(
            {"result": status.value.lower()}
            if status is not PlannerRequestStatus.REJECTED
            else None
        ),
    ).model_copy(update={"failure_category": f"historical-{status.value}"})

    store.import_replay_state(
        {
            "game_id": "game-1",
            "tables": {"logical_planner_requests": [_v7_request_row(historical)]},
        }
    )
    restored = store.get_planner_request(historical.planner_request_id)
    assert restored is not None
    assert restored.target == historical.target
    assert restored.status is status
    assert restored.response_payload is None
    assert restored.response_hash == historical.response_hash
    assert restored.validation_result == historical.validation_result
    assert restored.failure_category == historical.failure_category
    assert (
        restored.response_evidence_compatibility
        is PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD
    )

    first_export = store.export_replay_state("game-1")
    exported_request = first_export["tables"]["logical_planner_requests"][0]
    assert json.loads(exported_request["request_json"])[
        "response_evidence_compatibility"
    ] == PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD.value

    restored_store = WorkflowStore(
        tmp_path / f"v8-terminal-replay-{status.value}.sqlite3"
    )
    restored_store.import_replay_state(first_export)
    assert restored_store.get_planner_request(historical.planner_request_id) == restored
    assert restored_store.export_replay_state("game-1") == first_export


def test_v8_target_columns_cannot_borrow_old_json_response_exemption(tmp_path):
    store = WorkflowStore(tmp_path / "v8-old-json-no-exemption.sqlite3")
    seed = _request("v8-old-json-seed")
    store.save_planner_request(seed)
    missing = _request(
        "v8-old-json-missing",
        target=_legacy_target(
            group_id="v8-old-json-group",
            gap_ids=("v8-old-json-gap",),
        ),
        input_hash="v8-old-json-input",
        status=PlannerRequestStatus.REJECTED,
        completed_at=NOW,
    )
    row = _v8_request_row(missing)
    row["request_json"] = _legacy_request_json(missing)

    with pytest.raises(ValueError, match="requires.*response_payload"):
        store.import_replay_state(
            {
                "game_id": "game-1",
                "tables": {"logical_planner_requests": [row]},
            }
        )
    assert store.get_planner_request(seed.planner_request_id) == seed


@pytest.mark.parametrize("child_kind", ["provider", "information"])
def test_replay_rejects_child_relational_json_parent_split_before_delete(
    tmp_path,
    child_kind,
):
    store = WorkflowStore(tmp_path / f"replay-parent-split-{child_kind}.sqlite3")
    seed = _request("split-seed")
    parent_a = _request(
        "split-parent-a",
        target=_legacy_target(group_id="split-a", gap_ids=("split-gap-a",)),
        input_hash="split-input-a",
    )
    parent_b = _request(
        "split-parent-b",
        target=_legacy_target(group_id="split-b", gap_ids=("split-gap-b",)),
        input_hash="split-input-b",
    )
    store.save_planner_request(seed)
    tables = {
        "logical_planner_requests": [
            _v7_request_row(parent_a),
            _v7_request_row(parent_b),
        ]
    }
    if child_kind == "provider":
        attempt = ProviderAttempt(
            provider_attempt_id="split-attempt",
            planner_request_id=parent_a.planner_request_id,
            attempt_number=1,
            provider_request_id="split-provider-call",
            status=ProviderAttemptStatus.SUCCEEDED,
            started_at=NOW,
            completed_at=NOW,
            latency_seconds=0,
        )
        child_row = _v7_replay_state(parent_a, attempt=attempt)["tables"][
            "provider_attempts"
        ][0]
        child_row["planner_request_id"] = parent_b.planner_request_id
        tables["provider_attempts"] = [child_row]
        expected = "conflicts with attempt_json"
    else:
        round_record = InformationRound(
            information_round_id="split-round",
            planner_request_id=parent_a.planner_request_id,
            round_number=1,
            status=InformationRoundStatus.REQUESTED,
            requests=({"query": "research"},),
            requested_at=NOW,
        )
        child_row = _v7_replay_state(
            parent_a,
            round_record=round_record,
        )["tables"]["information_rounds"][0]
        child_row["planner_request_id"] = parent_b.planner_request_id
        tables["information_rounds"] = [child_row]
        expected = "conflicts with round_json"

    with pytest.raises(ValueError, match=expected):
        store.import_replay_state({"game_id": "game-1", "tables": tables})
    assert store.get_planner_request(seed.planner_request_id) == seed


def test_replay_missing_planner_parent_fails_before_delete(tmp_path):
    store = WorkflowStore(tmp_path / "replay-missing-parent.sqlite3")
    seed = _request("missing-parent-seed")
    store.save_planner_request(seed)
    orphan = ProviderAttempt(
        provider_attempt_id="orphan-attempt",
        planner_request_id="missing-request",
        attempt_number=1,
        provider_request_id="orphan-provider-call",
        status=ProviderAttemptStatus.SUCCEEDED,
        started_at=NOW,
        completed_at=NOW,
        latency_seconds=0,
    )
    orphan_row = _v7_replay_state(seed, attempt=orphan)["tables"][
        "provider_attempts"
    ][0]

    with pytest.raises(ValueError, match="references a missing"):
        store.import_replay_state(
            {
                "game_id": "game-1",
                "tables": {"provider_attempts": [orphan_row]},
            }
        )
    assert store.get_planner_request(seed.planner_request_id) == seed


def test_replay_provider_primary_key_collision_preserves_both_games(tmp_path):
    store = WorkflowStore(tmp_path / "provider-cross-game.sqlite3")
    seed_a = _request("provider-seed-a")
    parent_b = _request("provider-parent-b").model_copy(
        update={"game_session_id": "game-B"}
    )
    store.save_planner_request(seed_a)
    store.save_planner_request(parent_b)
    existing = ProviderAttempt(
        provider_attempt_id="shared-provider-attempt",
        planner_request_id=parent_b.planner_request_id,
        attempt_number=1,
        provider_request_id="provider-b",
        status=ProviderAttemptStatus.SUCCEEDED,
        started_at=NOW,
        completed_at=NOW,
        latency_seconds=0,
    )
    store.save_provider_attempt("game-B", existing)

    incoming_parent = _request(
        "provider-incoming-a",
        target=_legacy_target(group_id="incoming-a", gap_ids=("incoming-gap-a",)),
        input_hash="incoming-input-a",
    )
    incoming = existing.model_copy(
        update={
            "planner_request_id": incoming_parent.planner_request_id,
            "provider_request_id": "provider-a",
        }
    )
    state = _v7_replay_state(incoming_parent, attempt=incoming)

    with pytest.raises(ValueError, match="belongs to another game"):
        store.import_replay_state(state)
    assert store.get_planner_request(seed_a.planner_request_id) == seed_a
    assert store.get_planner_request(parent_b.planner_request_id) == parent_b
    assert store.list_provider_attempts(parent_b.planner_request_id) == [existing]


def test_replay_information_primary_key_collision_preserves_both_games(tmp_path):
    store = WorkflowStore(tmp_path / "information-cross-game.sqlite3")
    seed_a = _request("information-seed-a")
    parent_b = _request("information-parent-b").model_copy(
        update={"game_session_id": "game-B"}
    )
    store.save_planner_request(seed_a)
    store.save_planner_request(parent_b)
    existing = InformationRound(
        information_round_id="shared-information-round",
        planner_request_id=parent_b.planner_request_id,
        round_number=1,
        status=InformationRoundStatus.REQUESTED,
        requests=({"query": "research"},),
        requested_at=NOW,
    )
    store.save_information_round("game-B", existing)

    incoming_parent = _request(
        "information-incoming-a",
        target=_legacy_target(
            group_id="information-incoming-a",
            gap_ids=("information-incoming-gap-a",),
        ),
        input_hash="information-incoming-input-a",
    )
    incoming = existing.model_copy(
        update={"planner_request_id": incoming_parent.planner_request_id}
    )
    state = _v7_replay_state(incoming_parent, round_record=incoming)

    with pytest.raises(ValueError, match="belongs to another game"):
        store.import_replay_state(state)
    assert store.get_planner_request(seed_a.planner_request_id) == seed_a
    assert store.get_planner_request(parent_b.planner_request_id) == parent_b
    assert store.list_information_rounds(parent_b.planner_request_id) == [existing]


def test_v7_migration_preserves_real_foreign_key_children(tmp_path):
    path = tmp_path / "v7-real-foreign-keys.sqlite3"
    request = _request(
        "v7-fk-request",
        status=PlannerRequestStatus.COMPLETED,
        completed_at=NOW,
        response_hash="historical-response",
        validation_result={"result": "completed"},
    )
    attempt = ProviderAttempt(
        provider_attempt_id="v7-fk-attempt",
        planner_request_id=request.planner_request_id,
        attempt_number=1,
        provider_request_id="v7-fk-provider",
        status=ProviderAttemptStatus.SUCCEEDED,
        started_at=NOW,
        completed_at=NOW,
        latency_seconds=0,
    )
    round_record = InformationRound(
        information_round_id="v7-fk-round",
        planner_request_id=request.planner_request_id,
        round_number=1,
        status=InformationRoundStatus.REQUESTED,
        requests=({"query": "research"},),
        requested_at=NOW,
    )
    lease = PlanLease(
        plan_lease_id="v7-fk-lease",
        plan_id="v7-fk-plan",
        game_session_id="game-1",
        decision_gap_ids=("v7-fk-gap",),
        scope="research",
        plan_revision=1,
        source_planner_request_id=request.planner_request_id,
        status=PlanLeaseStatus.EXPIRED,
        approval_status=ApprovalStatus.NOT_REQUIRED,
        valid_from_turn=0,
        valid_until_turn=1,
        continuation_policy=ContinuationPolicy.REQUIRE_REVIEW,
        relevant_input_hash="v7-fk-input",
        last_validated_observation_id="v7-fk-observation",
        last_validation_result=LeaseValidationResult.EXPIRED,
    )
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(
            """
            CREATE TABLE logical_planner_requests (
                planner_request_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                decision_group_id TEXT,
                turn INTEGER NOT NULL,
                status TEXT NOT NULL,
                input_projection_hash TEXT NOT NULL,
                input_projection_version TEXT NOT NULL,
                decision_gap_ids_json TEXT NOT NULL,
                request_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                UNIQUE (game_id, decision_group_id, input_projection_hash)
            );
            CREATE TABLE provider_attempts (
                provider_attempt_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                planner_request_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                provider_request_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                UNIQUE (planner_request_id, attempt_number),
                FOREIGN KEY (planner_request_id)
                    REFERENCES logical_planner_requests(planner_request_id)
            );
            CREATE TABLE information_rounds (
                information_round_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                planner_request_id TEXT NOT NULL,
                round_number INTEGER NOT NULL,
                status TEXT NOT NULL,
                round_json TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                completed_at TEXT,
                UNIQUE (planner_request_id, round_number),
                FOREIGN KEY (planner_request_id)
                    REFERENCES logical_planner_requests(planner_request_id)
            );
            CREATE TABLE plan_leases (
                plan_lease_id TEXT PRIMARY KEY,
                game_id TEXT NOT NULL,
                scope TEXT NOT NULL,
                status TEXT NOT NULL,
                plan_revision INTEGER NOT NULL,
                relevant_input_hash TEXT NOT NULL,
                source_planner_request_id TEXT,
                lease_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (source_planner_request_id)
                    REFERENCES logical_planner_requests(planner_request_id)
            );
            """
        )
        request_row = _v7_request_row(request)
        columns = tuple(request_row)
        conn.execute(
            f"INSERT INTO logical_planner_requests "
            f"({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
            tuple(request_row[column] for column in columns),
        )
        conn.execute(
            "INSERT INTO provider_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                attempt.provider_attempt_id,
                "game-1",
                attempt.planner_request_id,
                attempt.attempt_number,
                attempt.provider_request_id,
                attempt.status.value,
                attempt.model_dump_json(),
                attempt.started_at.isoformat(),
                attempt.completed_at.isoformat(),
            ),
        )
        conn.execute(
            "INSERT INTO information_rounds VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                round_record.information_round_id,
                "game-1",
                round_record.planner_request_id,
                round_record.round_number,
                round_record.status.value,
                round_record.model_dump_json(),
                round_record.requested_at.isoformat(),
                None,
            ),
        )
        conn.execute(
            """
            INSERT INTO plan_leases(
                plan_lease_id, game_id, scope, status, plan_revision,
                relevant_input_hash, source_planner_request_id, lease_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lease.plan_lease_id,
                lease.game_session_id,
                lease.scope,
                lease.status.value,
                lease.plan_revision,
                lease.relevant_input_hash,
                lease.source_planner_request_id,
                lease.model_dump_json(),
            ),
        )
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        conn.execute("PRAGMA user_version=7")

    store = WorkflowStore(path)
    migrated_request = request.model_copy(
        update={
            "response_evidence_compatibility": (
                PlannerResponseEvidenceCompatibility.LEGACY_V7_MISSING_PAYLOAD
            )
        }
    )

    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 8
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    assert store.get_planner_request(request.planner_request_id) == migrated_request
    assert store.list_provider_attempts(request.planner_request_id) == [attempt]
    assert store.list_information_rounds(request.planner_request_id) == [
        round_record
    ]
    assert store.list_plan_leases("game-1") == [lease]

    exported = store.export_replay_state("game-1")
    replayed = WorkflowStore(tmp_path / "v8-after-real-v7-migration.sqlite3")
    replayed.import_replay_state(exported)
    assert replayed.get_planner_request(request.planner_request_id) == migrated_request
    replayed_export = replayed.export_replay_state("game-1")
    assert (
        replayed_export["tables"]["logical_planner_requests"]
        == exported["tables"]["logical_planner_requests"]
    )
