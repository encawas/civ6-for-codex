import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from civ6_workflow.domain import (
    AuthorityScopeSet,
    Mission,
    MissionGraph,
    MissionStatus,
    StrategicContract,
    StrategicContractCommit,
    SubjectRef,
    build_strategic_contract_id,
)
from civ6_workflow.store import WorkflowStore


NOW = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)


def _contract(
    game_id: str,
    revision: int,
    *,
    contract_id: str | None = None,
    mission: Mission | None = None,
    objectives: tuple[str, ...] = (),
    constraints: tuple[str, ...] = (),
    policy_snapshot: dict[str, object] | None = None,
) -> StrategicContract:
    return StrategicContract(
        contract_id=contract_id or build_strategic_contract_id(game_id),
        game_session_id=game_id,
        revision=revision,
        authority_scope_set=AuthorityScopeSet(
            mission_graph_scopes=() if mission is None else (mission.scope,)
        ),
        mission_graph=MissionGraph(missions=() if mission is None else (mission,)),
        strategic_objectives=objectives,
        global_constraints=constraints,
        created_from_observation_id=f"obs-{game_id}-{revision}",
        policy_snapshot=policy_snapshot or {},
    )


def _mission(
    game_id: str,
    contract_id: str,
    *,
    mission_id: str = "mission-research",
    mission_revision: int = 1,
    status: MissionStatus = MissionStatus.ACTIVE,
) -> Mission:
    return Mission(
        mission_id=mission_id,
        game_session_id=game_id,
        contract_id=contract_id,
        mission_revision=mission_revision,
        scope="research",
        subject=SubjectRef(subject_type="player", subject_id="player-1"),
        slot="player:research",
        objective="Select a safe opening technology",
        desired_outcome={"technology": "TECH_MINING"},
        status=status,
    )


def _commit(
    contract: StrategicContract,
    *,
    commit_id: str,
    base_revision: int,
    committed_at: datetime | None = None,
) -> StrategicContractCommit:
    return StrategicContractCommit(
        commit_id=commit_id,
        game_session_id=contract.game_session_id,
        contract_id=contract.contract_id,
        expected_base_revision=base_revision,
        contract=contract,
        committed_at=committed_at or NOW + timedelta(minutes=contract.revision),
        reason=f"commit revision {contract.revision}",
    )


def _create_root(store: WorkflowStore, game_id: str) -> StrategicContractCommit:
    commit = _commit(
        _contract(game_id, 1),
        commit_id=f"create-{game_id}",
        base_revision=0,
    )
    assert store.commit_strategic_contract_revision(commit) == commit.contract
    return commit


def _append_foundation_revision(
    store: WorkflowStore, game_id: str
) -> StrategicContractCommit:
    active = store.get_active_strategic_contract(game_id)
    assert active is not None
    commit = _commit(
        _contract(
            game_id,
            active.revision + 1,
            contract_id=active.contract_id,
            objectives=("Preserve the legacy execution authority",),
            constraints=("Do not claim MissionGraph scope authority",),
            policy_snapshot={"authority_mode": "legacy"},
        ),
        commit_id=f"foundation-{game_id}",
        base_revision=active.revision,
    )
    assert store.commit_strategic_contract_revision(commit) == commit.contract
    return commit


def test_new_game_has_one_stable_contract_root_and_empty_authority(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    commit = _create_root(store, "game-1")

    active = store.get_active_strategic_contract("game-1")
    assert active == commit.contract
    assert active.authority_scope_set.mission_graph_scopes == ()
    assert active.mission_graph.missions == ()

    assert store.commit_strategic_contract_revision(commit) == commit.contract
    with sqlite3.connect(store.path) as conn:
        assert (
            conn.execute("SELECT count(*) FROM strategic_contract_roots").fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                "SELECT count(*) FROM strategic_contract_revisions"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute("SELECT count(*) FROM strategic_contract_commits").fetchone()[
                0
            ]
            == 1
        )


def test_duplicate_creation_cannot_create_a_second_root(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    initial = _create_root(store, "game-1")
    competing = _commit(
        _contract("game-1", 1, contract_id="contract-competing"),
        commit_id="competing-create",
        base_revision=0,
    )

    with pytest.raises(ValueError, match="another StrategicContract root"):
        store.commit_strategic_contract_revision(competing)

    assert store.get_active_strategic_contract("game-1") == initial.contract
    assert store.list_strategic_contract_revisions("game-1") == [initial.contract]


def test_revisions_are_contiguous_and_history_is_immutable(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    initial = _create_root(store, "game-1")
    second = _append_foundation_revision(store, "game-1")

    assert store.get_active_strategic_contract("game-1") == second.contract
    assert store.list_strategic_contract_revisions("game-1") == [
        initial.contract,
        second.contract,
    ]
    assert store.get_strategic_contract_revision("game-1", 1) == initial.contract
    assert store.list_strategic_contract_commits("game-1") == [initial, second]


def test_stale_base_revision_is_rejected_without_partial_history(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    initial = _create_root(store, "game-1")
    _append_foundation_revision(store, "game-1")
    stale = _commit(
        _contract(
            "game-1",
            2,
            contract_id=initial.contract_id,
            objectives=("Conflicting stale objective",),
        ),
        commit_id="stale-commit",
        base_revision=1,
    )

    with pytest.raises(ValueError, match="stale StrategicContract base revision"):
        store.commit_strategic_contract_revision(stale)

    assert [
        item.revision for item in store.list_strategic_contract_revisions("game-1")
    ] == [
        1,
        2,
    ]
    assert len(store.list_strategic_contract_commits("game-1")) == 2


def test_commit_identity_is_idempotent_but_cannot_change_content(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    initial = _create_root(store, "game-1")
    second = _append_foundation_revision(store, "game-1")

    assert store.commit_strategic_contract_revision(initial) == initial.contract
    assert len(store.list_strategic_contract_revisions("game-1")) == 2

    changed = initial.model_copy(update={"reason": "different recovery content"})
    with pytest.raises(ValueError, match="reused with new content"):
        store.commit_strategic_contract_revision(changed)

    assert store.get_active_strategic_contract("game-1") == second.contract
    assert len(store.list_strategic_contract_commits("game-1")) == 2


def test_contract_revision_and_audit_commit_atomically(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    initial = _create_root(store, "game-1")
    second = _commit(
        _contract(
            "game-1",
            2,
            contract_id=initial.contract_id,
            objectives=("Keep contract history immutable",),
            constraints=("Keep authority scopes empty",),
            policy_snapshot={"foundation_revision": 2},
        ),
        commit_id="failing-revision",
        base_revision=1,
    )
    with sqlite3.connect(store.path) as conn:
        conn.execute(
            """
            CREATE TRIGGER fail_contract_audit
            BEFORE INSERT ON strategic_contract_commits
            WHEN NEW.committed_revision = 2
            BEGIN
                SELECT RAISE(ABORT, 'injected contract audit failure');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected contract audit failure"):
        store.commit_strategic_contract_revision(second)

    assert store.get_active_strategic_contract("game-1") == initial.contract
    assert store.list_strategic_contract_revisions("game-1") == [initial.contract]
    assert store.list_strategic_contract_commits("game-1") == [initial]


def test_contract_identity_cannot_cross_games(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    game_one = _create_root(store, "game-1")
    conflicting_root = _commit(
        _contract("game-2", 1, contract_id=game_one.contract_id),
        commit_id="create-game-2-conflict",
        base_revision=0,
    )
    with pytest.raises(ValueError, match="identity belongs to another game"):
        store.commit_strategic_contract_revision(conflicting_root)

    assert store.get_active_strategic_contract("game-2") is None


def test_startup_rejects_inconsistent_active_revision(tmp_path: Path):
    path = tmp_path / "workflow.sqlite3"
    store = WorkflowStore(path)
    _create_root(store, "game-1")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE strategic_contract_roots SET active_revision=2 WHERE game_id='game-1'"
        )

    with pytest.raises(ValueError, match="contiguous through active revision"):
        WorkflowStore(path)


def test_contract_history_and_audit_replay_round_trip_stably(tmp_path: Path):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    initial = _create_root(source, "game-1")
    second = _append_foundation_revision(source, "game-1")

    first_export = source.export_replay_state("game-1")
    restored = WorkflowStore(tmp_path / "restored.sqlite3")
    restored.import_replay_state(first_export)
    second_export = restored.export_replay_state("game-1")

    assert second_export == first_export
    assert restored.get_active_strategic_contract("game-1") == second.contract
    assert restored.list_strategic_contract_revisions("game-1") == [
        initial.contract,
        second.contract,
    ]
    assert restored.list_strategic_contract_commits("game-1") == [initial, second]

    second_restore = WorkflowStore(tmp_path / "second-restored.sqlite3")
    second_restore.import_replay_state(second_export)
    assert second_restore.export_replay_state("game-1") == first_export


def test_domain_models_retain_future_research_mission_shape():
    contract_id = build_strategic_contract_id("game-1")
    mission = _mission("game-1", contract_id)
    contract = _contract("game-1", 1, contract_id=contract_id, mission=mission)

    assert contract.authority_scope_set.mission_graph_scopes == ("research",)
    assert contract.mission_graph.missions == (mission,)


@pytest.mark.parametrize("include_mission", [False, True])
def test_revision_cannot_enable_scope_authority_or_persist_mission(
    tmp_path: Path, include_mission: bool
):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    initial = _create_root(store, "game-1")
    mission = _mission("game-1", initial.contract_id)
    candidate = StrategicContract(
        contract_id=initial.contract_id,
        game_session_id="game-1",
        revision=2,
        authority_scope_set=AuthorityScopeSet(mission_graph_scopes=("research",)),
        mission_graph=MissionGraph(missions=(mission,) if include_mission else ()),
        strategic_objectives=("Attempt to enable research authority",),
        created_from_observation_id="obs-game-1-2",
    )
    commit = _commit(candidate, commit_id="forbidden-revision", base_revision=1)
    before = store.export_replay_state("game-1")

    with pytest.raises(ValueError, match="foundation requires empty authority scope"):
        store.commit_strategic_contract_revision(commit)

    assert store.export_replay_state("game-1") == before
    assert store.get_active_strategic_contract("game-1") == initial.contract
    assert store.list_strategic_contract_revisions("game-1") == [initial.contract]
    assert store.list_strategic_contract_commits("game-1") == [initial]


def test_startup_rejects_manually_persisted_scope_and_mission(tmp_path: Path):
    path = tmp_path / "workflow.sqlite3"
    store = WorkflowStore(path)
    initial = _create_root(store, "game-1")
    valid_second = _append_foundation_revision(store, "game-1")
    mission = _mission("game-1", initial.contract_id)
    invalid_contract = _contract(
        "game-1", 2, contract_id=initial.contract_id, mission=mission
    )
    invalid_commit = _commit(
        invalid_contract,
        commit_id=valid_second.commit_id,
        base_revision=1,
        committed_at=valid_second.committed_at,
    )
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE strategic_contract_revisions SET contract_json=? "
            "WHERE game_id=? AND revision=2",
            (_dump_model(invalid_contract), "game-1"),
        )
        conn.execute(
            "UPDATE strategic_contract_commits SET commit_json=? WHERE commit_id=?",
            (_dump_model(invalid_commit), valid_second.commit_id),
        )

    with pytest.raises(ValueError, match="foundation requires empty authority scope"):
        WorkflowStore(path)


def test_replay_rejects_scope_and_mission_before_deleting_target(tmp_path: Path):
    source = WorkflowStore(tmp_path / "source.sqlite3")
    initial = _create_root(source, "game-1")
    valid_second = _append_foundation_revision(source, "game-1")
    state = source.export_replay_state("game-1")
    mission = _mission("game-1", initial.contract_id)
    invalid_contract = _contract(
        "game-1", 2, contract_id=initial.contract_id, mission=mission
    )
    invalid_commit = _commit(
        invalid_contract,
        commit_id=valid_second.commit_id,
        base_revision=1,
        committed_at=valid_second.committed_at,
    )
    revision_row = next(
        row
        for row in state["tables"]["strategic_contract_revisions"]
        if row["revision"] == 2
    )
    revision_row["contract_json"] = _dump_model(invalid_contract)
    commit_row = next(
        row
        for row in state["tables"]["strategic_contract_commits"]
        if row["committed_revision"] == 2
    )
    commit_row["commit_json"] = _dump_model(invalid_commit)

    target = WorkflowStore(tmp_path / "target.sqlite3")
    target_initial = _create_root(target, "game-1")
    before = target.export_replay_state("game-1")

    with pytest.raises(ValueError, match="foundation requires empty authority scope"):
        target.import_replay_state(state)

    assert target.export_replay_state("game-1") == before
    assert target.get_active_strategic_contract("game-1") == target_initial.contract


def test_initial_contract_cannot_claim_research_authority(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    contract_id = build_strategic_contract_id("game-1")
    mission = _mission("game-1", contract_id)
    nonempty_initial = _commit(
        _contract("game-1", 1, contract_id=contract_id, mission=mission),
        commit_id="nonempty-create",
        base_revision=0,
    )

    with pytest.raises(ValueError, match="foundation requires empty authority scope"):
        store.commit_strategic_contract_revision(nonempty_initial)

    assert store.get_active_strategic_contract("game-1") is None


def _dump_model(value: StrategicContract | StrategicContractCommit) -> str:
    return json.dumps(
        value.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
