from pathlib import Path

from civ6_workflow.gate import EventGate, GateConfig
from civ6_workflow.models import (
    AgentRequest,
    EventLevel,
    ExecutionMode,
    GameEvent,
    PlanBundle,
    ProposedTask,
    RiskLevel,
    TaskStatus,
)
from civ6_workflow.store import WorkflowStore


def test_failed_agent_run_does_not_block_explicit_retry(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    request = AgentRequest(
        turn=8,
        execution_mode=ExecutionMode.AUTO,
        trigger_events=[],
    )
    store.record_agent_run(
        "game-1",
        request,
        response=None,
        success=False,
        error="planner timeout",
        duration_seconds=180,
    )

    assert store.agent_called_for_turn("game-1", 8) is False

    assert store.agent_call_count_for_turn("game-1", 8) == 0
    store.record_agent_run(
        "game-1",
        request,
        response=PlanBundle(summary="retry succeeded"),
        success=True,
        error=None,
        duration_seconds=2,
    )

    assert store.agent_called_for_turn("game-1", 8) is True

    assert store.agent_call_count_for_turn("game-1", 8) == 1

    second_request = request.model_copy(update={"request_id": "req_second"})
    store.record_agent_run(
        "game-1",
        second_request,
        response=PlanBundle(summary="second success"),
        success=True,
        error=None,
        duration_seconds=1,
    )

    assert store.agent_call_count_for_turn("game-1", 8) == 2


def test_event_is_suppressed_inside_cooldown(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    gate = EventGate(store, GateConfig(default_cooldown_turns=2))

    first = GameEvent(
        event_type="housing_pressure",
        turn=10,
        level=EventLevel.L2,
        dedupe_key="housing:city:1",
    )
    assert gate.ingest("game-1", [first]).emitted

    duplicate = first.model_copy(update={"turn": 11})
    assert not gate.ingest("game-1", [duplicate]).emitted

    after_cooldown = first.model_copy(update={"turn": 12})
    assert gate.ingest("game-1", [after_cooldown]).emitted


def test_confirm_mode_requires_approval_for_every_new_task(tmp_path: Path):
    store = WorkflowStore(tmp_path / "workflow.sqlite3")
    bundle = PlanBundle(
        summary="hold all actions for explicit confirmation",
        tasks=[
            ProposedTask(
                task_id="safe-production",
                action_type="city_set_production",
                entity_type="city",
                entity_id=1,
                due_turn=20,
                arguments={
                    "city_id": 1,
                    "item_type": "UNIT",
                    "item_name": "UNIT_BUILDER",
                },
                postconditions=[
                    {
                        "type": "city_production_equals",
                        "city_id": 1,
                        "item_name": "UNIT_BUILDER",
                    }
                ],
                reason="approved city queue continuation",
            ),
            ProposedTask(
                task_id="risky-move",
                action_type="unit_move",
                entity_type="unit",
                entity_id=9,
                due_turn=20,
                arguments={"unit_id": 9, "target_x": 3, "target_y": 4},
                postconditions=[{"type": "unit_at", "unit_id": 9, "x": 3, "y": 4}],
                risk=RiskLevel.HIGH,
                requires_confirmation=True,
                reason="move a civilian near a threat",
            ),
        ],
    )
    store.save_plan_bundle(
        "game-1",
        20,
        bundle,
        mode=ExecutionMode.CONFIRM,
        auto_action_types={"city_set_production", "unit_move"},
    )

    records = {task.task_id: task for task in store.list_tasks("game-1")}
    assert records["safe-production"].status is TaskStatus.AWAITING_CONFIRMATION
    assert records["risky-move"].status is TaskStatus.AWAITING_CONFIRMATION
    assert store.approve_task("game-1", "safe-production") is True
    assert store.approve_task("game-1", "risky-move") is True
    records = {task.task_id: task for task in store.list_tasks("game-1")}
    assert records["safe-production"].status is TaskStatus.READY
    assert records["risky-move"].status is TaskStatus.READY
