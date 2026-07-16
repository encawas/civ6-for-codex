from pathlib import Path

from civ6_workflow.models import (
    AgentRequest,
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RuntimeSnapshot,
)
from civ6_workflow.observation_normalization import normalize_runtime_snapshot
from civ6_workflow.progression import ProgressionRuleCompiler
from civ6_workflow.recovery import recover_turn_rewind
from civ6_workflow.store import WorkflowStore


def _compile(compiler, snapshot):
    return getattr(compiler, "compile")(normalize_runtime_snapshot(snapshot))


def _snapshot(*, turn: int, research="None", civic="None") -> RuntimeSnapshot:
    return RuntimeSnapshot(
        turn=turn,
        game_id="game-1",
        overview={"turn": turn},
        tech_civics={
            "current_research": research,
            "current_civic": civic,
            "available_techs": [
                {"name": "Mining", "tech_type": "TECH_MINING"},
                {"name": "Pottery", "tech_type": "TECH_POTTERY"},
            ],
            "available_civics": [
                {"name": "Code of Laws", "civic_type": "CIVIC_CODE_OF_LAWS"},
                {"name": "Craftsmanship", "civic_type": "CIVIC_CRAFTSMANSHIP"},
            ],
        },
    )


def test_research_and_civic_queues_compile_to_verifiable_tasks(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        5,
        PlanBundle(
            summary="seed progression queues",
            strategy_updates={
                "research_queue": ["TECH_MINING", "TECH_POTTERY"],
                "civic_queue": ["CIVIC_CODE_OF_LAWS"],
            },
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"set_research", "set_civic"},
    )

    compiled = _compile(ProgressionRuleCompiler(store), _snapshot(turn=5))

    assert compiled.bundle is not None
    tasks = {task.action_type: task for task in compiled.bundle.tasks}
    assert tasks["set_research"].arguments == {"tech_or_civic": "TECH_MINING"}
    assert tasks["set_research"].preconditions == [
        {"type": "research_unselected"},
        {"type": "research_available", "tech_type": "TECH_MINING"},
    ]
    assert tasks["set_research"].postconditions == [
        {"type": "research_equals", "tech_type": "TECH_MINING"}
    ]
    assert tasks["set_civic"].arguments == {"tech_or_civic": "CIVIC_CODE_OF_LAWS"}
    assert tasks["set_civic"].postconditions == [
        {"type": "civic_equals", "civic_type": "CIVIC_CODE_OF_LAWS"}
    ]


def test_progression_queue_does_not_override_active_choice(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        5,
        PlanBundle(
            summary="seed research queue",
            strategy_updates={"research_queue": ["TECH_MINING"]},
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"set_research"},
    )

    compiled = _compile(
        ProgressionRuleCompiler(store),
        _snapshot(turn=5, research="Pottery", civic="Code of Laws"),
    )

    assert compiled.bundle is None
    assert compiled.events == []


def test_unavailable_progression_target_is_blocking(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        5,
        PlanBundle(
            summary="seed unavailable research",
            strategy_updates={"research_queue": ["TECH_WRITING"]},
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"set_research"},
    )

    compiled = _compile(ProgressionRuleCompiler(store), _snapshot(turn=5))

    event = next(
        event
        for event in compiled.events
        if event.event_type == "research_plan_target_unavailable"
    )
    assert event.blocking is True
    assert int(event.level) == 3
    assert event.payload["target"] == "TECH_WRITING"


def test_turn_rewind_clears_future_derived_state(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        20,
        PlanBundle(
            plan_id="future-plan",
            summary="future timeline plan",
            strategy_updates={"research_queue": ["TECH_MINING"]},
            tasks=[
                ProposedTask(
                    task_id="future-task",
                    action_type="set_research",
                    entity_type="research",
                    entity_id="TECH_MINING",
                    due_turn=20,
                    arguments={"tech_or_civic": "TECH_MINING"},
                    postconditions=[
                        {"type": "research_equals", "tech_type": "TECH_MINING"}
                    ],
                    reason="future timeline research",
                )
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"set_research"},
    )
    request = AgentRequest(
        turn=20,
        execution_mode=ExecutionMode.AUTO,
        trigger_events=[],
    )
    store.record_agent_run(
        "game-1",
        request,
        response=PlanBundle(summary="future response"),
        success=True,
        error=None,
        duration_seconds=0.1,
    )
    store.set_meta("last_game_id", "game-1")
    store.set_meta("last_observed_turn", 30)

    event = recover_turn_rewind(store, _snapshot(turn=10))

    assert event is not None
    assert event.event_type == "turn_rewind_detected"
    assert event.blocking is True
    assert store.current_context("game-1")["strategy"] == {}
    assert store.list_tasks("game-1") == []
    assert store.agent_called_for_turn("game-1", 20) is False
