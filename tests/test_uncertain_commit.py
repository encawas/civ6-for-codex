import asyncio

from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import (
    ActionResult,
    ExecutionMode,
    PlanBundle,
    ProposedTask,
    RuntimeSnapshot,
    TaskStatus,
)
from civ6_workflow.store import WorkflowStore


class _PlannerMustNotRun:
    def __init__(self):
        self.calls = 0

    async def plan(self, request):
        self.calls += 1
        raise AssertionError("uncertain commits must not be delegated or retried")


class _GameWithUnverifiedImprovement:
    def __init__(self):
        self.call_count = 0
        self.snapshot = RuntimeSnapshot(
            turn=15,
            game_id="game",
            overview={"turn": 15},
            cities=[],
            units=[
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_BUILDER",
                    "x": 4,
                    "y": 5,
                    "moves_remaining": 2,
                    "build_charges": 2,
                    "valid_improvements": ["IMPROVEMENT_MINE"],
                }
            ],
            blockers=[],
        )

    async def read_snapshot(self, *, include_units=False):
        self.call_count += 1
        snapshot = self.snapshot.model_copy(deep=True)
        if not include_units:
            snapshot.units = None
        return snapshot

    async def execute_task(self, task):
        self.call_count += 1
        # Simulate a server acknowledgement whose state change cannot be observed.
        return ActionResult(success=True, message="accepted")

    async def end_turn(self, reflections=None):
        self.call_count += 1
        return ActionResult(success=True, message="unexpected")

    async def list_tools(self):
        return {
            "get_notifications",
            "get_pending_diplomacy",
            "get_pending_trades",
            "set_city_production",
            "set_research",
            "unit_action",
            "end_turn",
        }


def _task():
    return ProposedTask(
        task_id="improve-9",
        action_type="builder_improve",
        entity_type="builder",
        entity_id=9,
        due_turn=15,
        arguments={"unit_id": 9, "improvement_type": "IMPROVEMENT_MINE"},
        preconditions=[
            {"type": "entity_exists", "entity_type": "builder", "entity_id": 9},
            {"type": "unit_at", "unit_id": 9, "x": 4, "y": 5},
            {"type": "unit_has_build_charge", "unit_id": 9},
            {
                "type": "unit_can_improve",
                "unit_id": 9,
                "improvement_type": "IMPROVEMENT_MINE",
            },
        ],
        postconditions=[
            {"type": "unit_build_charges_equals", "unit_id": 9, "charges": 1}
        ],
        reason="build approved mine",
    )


def test_unverified_irreversible_action_is_not_retried(tmp_path):
    store = WorkflowStore(tmp_path / "state.sqlite3")
    store.save_plan_bundle(
        "game",
        15,
        PlanBundle(summary="improve tile", tasks=[_task()]),
        mode=ExecutionMode.AUTO,
        auto_action_types={"builder_improve"},
    )
    planner = _PlannerMustNotRun()
    engine = WorkflowEngine(
        store=store,
        game=_GameWithUnverifiedImprovement(),
        planner=planner,
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=True,
            auto_action_types={"builder_improve"},
            allowed_action_types={"builder_improve"},
            allowed_tools={
                "set_city_production",
                "set_research",
                "unit_action",
                "end_turn",
            },
            verification_attempts=1,
            verification_delay_seconds=0,
        ),
    )

    first = asyncio.run(engine.tick())
    assert store.task_status("game", "improve-9") is TaskStatus.VERIFYING
    assert first.workflow_tick["outcome"] == "MUTATION_SENT"
    assert first.turn_ended is False
    assert first.agent_invoked is False
    assert planner.calls == 0

    second = asyncio.run(engine.tick())
    assert store.task_status("game", "improve-9") is TaskStatus.UNCERTAIN
    assert second.workflow_tick["outcome"] == "AWAITING_HUMAN"
    assert second.paused is True
    assert second.turn_ended is False
    assert planner.calls == 0
