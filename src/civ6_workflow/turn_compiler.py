"""Deterministically compile the current research execution projection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .domain import (
    Mission,
    NormalizedObservation,
    SlotState,
    StrategicContract,
    TurnActionGraph,
    TurnActionNode,
    build_turn_action_graph_id,
    build_turn_action_node_id,
    research_mission_action,
    thaw_json,
)
from .models import ExecutionMode, RiskLevel, StoredTask, TaskStatus


@dataclass(frozen=True, slots=True)
class TurnCompilation:
    graph: TurnActionGraph
    nodes: tuple[TurnActionNode, ...]
    unavailable_target: str | None = None


class TurnCompiler:
    """Compile one current-turn graph from authoritative research facts."""

    @staticmethod
    def unavailable_target(
        observation: NormalizedObservation,
        mission: Mission,
    ) -> str | None:
        """Return the unavailable active target while research is unselected."""

        research_mission_action(mission)
        technology = str(thaw_json(mission.desired_outcome)["technology"])
        if observation.progression.current_research.state is not SlotState.EMPTY:
            return None
        available = {
            item.value for item in observation.progression.available_research_ids
        }
        return None if technology in available else technology

    def compile(
        self,
        observation: NormalizedObservation,
        contract: StrategicContract,
        mission: Mission,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
        compiled_at: datetime,
    ) -> TurnCompilation:
        action_type = research_mission_action(mission)
        desired = thaw_json(mission.desired_outcome)
        technology = str(desired["technology"])
        nodes: tuple[TurnActionNode, ...] = ()
        unavailable_target = self.unavailable_target(observation, mission)
        progression = observation.progression
        if progression.current_research.state is SlotState.EMPTY:
            available = {item.value for item in progression.available_research_ids}
            if technology in available:
                node_identity = {
                    "game_session_id": observation.game_session_id,
                    "turn_number": observation.turn_number,
                    "source_observation_id": observation.observation_id,
                    "source_contract_id": contract.contract_id,
                    "source_contract_revision": contract.revision,
                    "source_mission_id": mission.mission_id,
                    "source_mission_revision": mission.mission_revision,
                    "action_type": action_type,
                    "entity_id": technology,
                    "arguments": {"tech_or_civic": technology},
                    "target_turn": observation.turn_number,
                }
                node_id = build_turn_action_node_id(**node_identity)
                requires_confirmation = (
                    mode is not ExecutionMode.AUTO
                    or action_type not in auto_action_types
                )
                provisional = TurnActionNode(
                    node_id=node_id,
                    graph_id="pending",
                    source_observation_projection_hash=observation.projection_hash,
                    entity_type="research",
                    preconditions=(
                        {"type": "research_unselected"},
                        {"type": "research_available", "tech_type": technology},
                    ),
                    postconditions=(
                        {"type": "research_equals", "tech_type": technology},
                    ),
                    risk=RiskLevel.LOW.value,
                    requires_confirmation=requires_confirmation,
                    reason="Execute the active StrategicContract research Mission.",
                    idempotency_key=f"turn-action:{node_id}",
                    **node_identity,
                )
                nodes = (provisional,)
        graph_identity = {
            "game_session_id": observation.game_session_id,
            "turn_number": observation.turn_number,
            "source_observation_id": observation.observation_id,
            "source_observation_projection_hash": observation.projection_hash,
            "source_contract_id": contract.contract_id,
            "source_contract_revision": contract.revision,
            "node_ids": tuple(node.node_id for node in nodes),
        }
        graph_id = build_turn_action_graph_id(**graph_identity)
        nodes = tuple(node.model_copy(update={"graph_id": graph_id}) for node in nodes)
        graph = TurnActionGraph(
            graph_id=graph_id,
            compiled_at=compiled_at,
            **graph_identity,
        )
        return TurnCompilation(
            graph=graph,
            nodes=nodes,
            unavailable_target=unavailable_target,
        )


def turn_action_node_as_stored_task(
    node: TurnActionNode,
    *,
    status: TaskStatus,
    retry_count: int = 0,
    max_retries: int = 2,
    last_error: str | None = None,
    approved_by: str | None = None,
) -> StoredTask:
    """Adapt the canonical node to the proven action and verification surface."""

    return StoredTask(
        task_id=node.node_id,
        plan_id=node.graph_id,
        action_type=node.action_type,
        entity_type=node.entity_type,
        entity_id=node.entity_id,
        due_turn=node.target_turn,
        expires_turn=node.turn_number,
        arguments=dict(node.arguments),
        preconditions=[dict(item) for item in node.preconditions],
        postconditions=[dict(item) for item in node.postconditions],
        invalidators=[dict(item) for item in node.invalidators],
        risk=RiskLevel(node.risk),
        requires_confirmation=node.requires_confirmation,
        reason=node.reason,
        created_turn=node.turn_number,
        created_from_observation_id=node.source_observation_id,
        status=status,
        retry_count=retry_count,
        max_retries=max_retries,
        last_error=last_error,
        approved_by=approved_by,
        source_contract_id=node.source_contract_id,
        source_contract_revision=node.source_contract_revision,
        source_mission_id=node.source_mission_id,
        source_mission_revision=node.source_mission_revision,
    )
