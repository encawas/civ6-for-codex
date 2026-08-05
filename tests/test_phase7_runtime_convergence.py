from __future__ import annotations

import inspect

import pytest

from civ6_workflow import bootstrap
from civ6_workflow.planner_lifecycle import PlannerLifecycleRuntime
from civ6_workflow.store import WorkflowStore


class _Game:
    call_count = 0


class _Planner:
    async def plan(self, request):
        raise AssertionError("runtime convergence test does not call the Planner")


def test_planner_lifecycle_uses_a_narrow_runtime_service_container(tmp_path):
    store = WorkflowStore(tmp_path / "runtime.sqlite3")
    game = _Game()
    planner = _Planner()

    composition = bootstrap.compose_runtime(
        store=store,
        game=game,
        planner=planner,
    )
    coordinator = composition.runtime.strategic_workflow
    services = coordinator.planner_lifecycle.runtime

    assert isinstance(services, PlannerLifecycleRuntime)
    assert services is not composition.runtime
    assert services.store is store
    assert services.game is not game
    assert not hasattr(services.game, "execute_task")
    assert not hasattr(services.game, "end_turn")
    assert hasattr(services.game, "query_tool")
    assert hasattr(services.game, "read_snapshot")
    assert services.planner is planner
    assert not hasattr(composition.runtime, "planner_lifecycle")
    assert not hasattr(composition.runtime, "turn_compiler")
    for retired_name in (
        "_build_agent_request",
        "_validate_planner_bundle",
        "_set_backoff",
        "_clear_backoff",
    ):
        assert not hasattr(services, retired_name)


def test_planner_and_runtime_modules_have_one_way_dependencies():
    import civ6_workflow.runtime as engine_module
    import civ6_workflow.planner_lifecycle as planner_module

    engine_source = inspect.getsource(engine_module)
    planner_source = inspect.getsource(planner_module)

    assert "from .runtime import" not in planner_source
    assert "from .store import" not in planner_source
    assert "from .store import" not in engine_source
    assert "from .mcp_port import" not in engine_source
    assert ".compile_missions(" not in engine_source
    assert ".activate_turn_action_graph(" not in engine_source
    assert ".advance_mission_repair(" not in engine_source


@pytest.mark.parametrize(
    "factory",
    [
        bootstrap.compose_live_runtime,
        bootstrap.compose_recording_runtime,
        bootstrap.compose_replay_runtime,
    ],
)
def test_all_runtime_factories_delegate_to_the_canonical_composition_root(factory):
    source = inspect.getsource(factory)

    assert "compose_runtime(" in source
    assert "WorkflowRuntime(" not in source
