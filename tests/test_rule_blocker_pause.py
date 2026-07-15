import asyncio
from pathlib import Path

from civ6_workflow.engine import EngineConfig, WorkflowEngine
from civ6_workflow.models import ActionResult, ExecutionMode, PlanBundle, RuntimeSnapshot
from civ6_workflow.store import WorkflowStore
from civ6_workflow.workflow_protocol import EventResolution, ResolutionDisposition


class _Planner:
    def __init__(self):
        self.calls = 0

    async def plan(self, request):
        self.calls += 1
        return PlanBundle(
            summary="no deterministic recovery task",
            requires_human_review=True,
            event_resolutions=[
                EventResolution(
                    event_dedupe_key=event.dedupe_key,
                    disposition=ResolutionDisposition.HUMAN_REVIEW,
                    reason="The invalid builder plan cannot be repaired deterministically.",
                )
                for event in request.trigger_events
                if event.blocking
            ],
        )


class _Game:
    def __init__(self):
        self.call_count = 0
        self.snapshot = RuntimeSnapshot(
            turn=12,
            game_id="game-1",
            overview={"turn": 12},
            cities=[],
            units=[
                {
                    "unit_id": 9,
                    "unit_type": "UNIT_BUILDER",
                    "x": 3,
                    "y": 4,
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
        return ActionResult(success=False, message="unexpected task")

    async def end_turn(self):
        self.call_count += 1
        return ActionResult(success=False, message="unexpected end turn")

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


def test_unresolved_rule_blocker_pauses_instead_of_polling_forever(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    store.save_plan_bundle(
        "game-1",
        12,
        PlanBundle(
            summary="invalid builder plan",
            builder_plan_updates=[
                {
                    "builder_key": "builder-9",
                    "assigned_unit_id": 9,
                    "path": [[3, 4]],
                }
            ],
        ),
        mode=ExecutionMode.AUTO,
        auto_action_types={"unit_move", "builder_improve"},
    )
    planner = _Planner()
    engine = WorkflowEngine(
        store=store,
        game=_Game(),
        planner=planner,
        config=EngineConfig(
            execution_mode=ExecutionMode.AUTO,
            auto_end_turn=False,
            auto_action_types={"unit_move", "builder_improve"},
            allowed_action_types={"unit_move", "builder_improve"},
            allowed_tools={
                "set_city_production",
                "set_research",
                "unit_action",
                "end_turn",
            },
            verification_delay_seconds=0,
        ),
    )

    first = asyncio.run(engine.tick())
    assert first.agent_invoked is True
    assert first.paused is True
    assert first.pause_reason == "Planner requested human review"
    assert planner.calls == 1

    second = asyncio.run(engine.tick())
    assert planner.calls == 1
    assert second.paused is True
    assert "already been called" in second.pause_reason
