"""Deterministically compile the current strategic execution projection."""

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
    city_roles_mission_plan,
    diplomacy_trade_mission_policy,
    settler_mission_plan,
    strategic_mission_action,
    tactical_emergency_mission_order,
    thaw_json,
)
from .models import ExecutionMode, RiskLevel, TurnActionExecution, TaskStatus


@dataclass(frozen=True, slots=True)
class TurnCompilation:
    graph: TurnActionGraph
    nodes: tuple[TurnActionNode, ...]
    unavailable_target: str | None = None
    unavailable_targets: tuple[tuple[str, str], ...] = ()


class TurnCompiler:
    """Compile one current-turn graph from authoritative Mission facts."""

    @staticmethod
    def unavailable_target(
        observation: NormalizedObservation,
        mission: Mission,
    ) -> str | None:
        """Return the unavailable active target while research is unselected."""

        if mission.scope == "settler":
            plan = settler_mission_plan(mission)
            unit = observation.unit(str(plan["unit_id"]))
            if unit is None:
                return f"settler:{plan['unit_id']}"
            return (
                None
                if TurnCompiler._settler_target_is_safe(unit.values, plan)
                else f"site:{plan['target_x']}:{plan['target_y']}"
            )
        if mission.scope == "city_roles":
            policy = city_roles_mission_plan(mission)
            for city_plan in policy["cities"]:
                if observation.city(str(city_plan["city_id"])) is None:
                    return f"city:{city_plan['city_id']}"
            return None
        if mission.scope == "diplomacy_trade":
            diplomacy_trade_mission_policy(mission)
            return None
        if mission.scope == "tactical_emergency":
            plan = tactical_emergency_mission_order(mission)
            unit = observation.unit(str(plan["unit_id"]))
            if unit is None:
                return f"unit:{plan['unit_id']}"
            if plan["target_turn"] < observation.turn_number:
                return f"turn:{plan['target_turn']}"
            if plan["target_turn"] > observation.turn_number:
                return None
            if unit.moves_remaining is None or unit.moves_remaining <= 0:
                return f"unit:{plan['unit_id']}:no-moves"
            if plan["order"]["kind"] == "move":
                values = thaw_json(unit.values)
                if type(values.get("x")) is not int or type(values.get("y")) is not int:
                    return f"unit:{plan['unit_id']}:position-unknown"
                targets = values.get("targets", ())
                if not isinstance(targets, list) or not any(
                    isinstance(target, dict)
                    and target.get("x") == plan["order"]["target_x"]
                    and target.get("y") == plan["order"]["target_y"]
                    and target.get("legal", True) is True
                    and target.get("reachable", True) is True
                    for target in targets
                ):
                    return (
                        f"tile:{plan['order']['target_x']}:{plan['order']['target_y']}"
                    )
            return None
        strategic_mission_action(mission)
        desired = thaw_json(mission.desired_outcome)
        target_key = "technology" if mission.scope == "research" else "civic"
        target = str(desired[target_key])
        slot = (
            observation.progression.current_research
            if mission.scope == "research"
            else observation.progression.current_civic
        )
        if slot.state is not SlotState.EMPTY:
            return None
        available = {
            item.value
            for item in (
                observation.progression.available_research_ids
                if mission.scope == "research"
                else observation.progression.available_civic_ids
            )
        }
        return None if target in available else target

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
        return self.compile_missions(
            observation,
            contract,
            (mission,),
            mode=mode,
            auto_action_types=auto_action_types,
            compiled_at=compiled_at,
        )

    def compile_missions(
        self,
        observation: NormalizedObservation,
        contract: StrategicContract,
        missions: tuple[Mission, ...],
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
        compiled_at: datetime,
    ) -> TurnCompilation:
        ordered_missions = tuple(sorted(missions, key=lambda item: item.mission_id))
        if len({mission.scope for mission in ordered_missions}) != len(
            ordered_missions
        ):
            raise ValueError("TurnActionGraph requires one active Mission per scope")
        provisional_nodes: list[TurnActionNode] = []
        unavailable_targets: list[tuple[str, str]] = []
        for mission in ordered_missions:
            node, unavailable = self._compile_mission_node(
                observation,
                contract,
                mission,
                mode=mode,
                auto_action_types=auto_action_types,
            )
            if node is not None:
                provisional_nodes.append(node)
            if unavailable is not None:
                unavailable_targets.append((mission.scope, unavailable))
        provisional_nodes.sort(key=lambda item: item.node_id)
        graph_identity = {
            "game_session_id": observation.game_session_id,
            "turn_number": observation.turn_number,
            "source_observation_id": observation.observation_id,
            "source_observation_projection_hash": observation.projection_hash,
            "source_contract_id": contract.contract_id,
            "source_contract_revision": contract.revision,
            "node_ids": tuple(node.node_id for node in provisional_nodes),
        }
        graph_id = build_turn_action_graph_id(**graph_identity)
        nodes = tuple(
            node.model_copy(update={"graph_id": graph_id}) for node in provisional_nodes
        )
        graph = TurnActionGraph(
            graph_id=graph_id,
            compiled_at=compiled_at,
            **graph_identity,
        )
        unavailable = tuple(unavailable_targets)
        return TurnCompilation(
            graph=graph,
            nodes=nodes,
            unavailable_target=(unavailable[0][1] if len(unavailable) == 1 else None),
            unavailable_targets=unavailable,
        )

    def _compile_mission_node(
        self,
        observation: NormalizedObservation,
        contract: StrategicContract,
        mission: Mission,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
    ) -> tuple[TurnActionNode | None, str | None]:
        if mission.scope == "settler":
            return self._compile_settler_node(
                observation,
                contract,
                mission,
                mode=mode,
                auto_action_types=auto_action_types,
            )
        if mission.scope == "city_roles":
            return self._compile_city_roles_node(
                observation,
                contract,
                mission,
                mode=mode,
                auto_action_types=auto_action_types,
            )
        if mission.scope == "diplomacy_trade":
            return self._compile_diplomacy_trade_node(
                observation,
                contract,
                mission,
                mode=mode,
                auto_action_types=auto_action_types,
            )
        if mission.scope == "tactical_emergency":
            return self._compile_tactical_emergency_node(
                observation,
                contract,
                mission,
                mode=mode,
                auto_action_types=auto_action_types,
            )
        action_type = strategic_mission_action(mission)
        desired = thaw_json(mission.desired_outcome)
        target_key = "technology" if mission.scope == "research" else "civic"
        target = str(desired[target_key])
        condition_prefix = "research" if mission.scope == "research" else "civic"
        condition_target_key = (
            "tech_type" if mission.scope == "research" else "civic_type"
        )
        unavailable_target = self.unavailable_target(observation, mission)
        progression = observation.progression
        slot = (
            progression.current_research
            if mission.scope == "research"
            else progression.current_civic
        )
        available = {
            item.value
            for item in (
                progression.available_research_ids
                if mission.scope == "research"
                else progression.available_civic_ids
            )
        }
        if slot.state is SlotState.EMPTY:
            if target in available:
                node_identity = {
                    "game_session_id": observation.game_session_id,
                    "turn_number": observation.turn_number,
                    "source_observation_id": observation.observation_id,
                    "source_contract_id": contract.contract_id,
                    "source_contract_revision": contract.revision,
                    "source_mission_id": mission.mission_id,
                    "source_mission_revision": mission.mission_revision,
                    "action_type": action_type,
                    "entity_id": target,
                    "arguments": {"tech_or_civic": target},
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
                    entity_type=mission.scope,
                    preconditions=(
                        {"type": f"{condition_prefix}_unselected"},
                        {
                            "type": f"{condition_prefix}_available",
                            condition_target_key: target,
                        },
                    ),
                    postconditions=(
                        {
                            "type": f"{condition_prefix}_equals",
                            condition_target_key: target,
                        },
                    ),
                    risk=RiskLevel.LOW.value,
                    requires_confirmation=requires_confirmation,
                    reason=(
                        f"Execute the active StrategicContract {mission.scope} Mission."
                    ),
                    idempotency_key=f"turn-action:{node_id}",
                    **node_identity,
                )
                return provisional, unavailable_target
        return None, unavailable_target

    def _compile_settler_node(
        self,
        observation: NormalizedObservation,
        contract: StrategicContract,
        mission: Mission,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
    ) -> tuple[TurnActionNode | None, str | None]:
        plan = settler_mission_plan(mission)
        unit_id = str(plan["unit_id"])
        unit = observation.unit(unit_id)
        unavailable = self.unavailable_target(observation, mission)
        if unit is None or unavailable is not None:
            return None, unavailable
        values = thaw_json(unit.values)
        x = values.get("x")
        y = values.get("y")
        if type(x) is not int or type(y) is not int:
            return None, f"settler:{unit_id}:position"
        target_x = int(plan["target_x"])
        target_y = int(plan["target_y"])
        at_target = (x, y) == (target_x, target_y)
        action_type = "unit_found_city" if at_target else "unit_move"
        arguments = (
            {"unit_id": plan["unit_id"]}
            if at_target
            else {
                "unit_id": plan["unit_id"],
                "target_x": target_x,
                "target_y": target_y,
            }
        )
        preconditions = [
            {
                "type": "entity_exists",
                "entity_type": "unit",
                "entity_id": plan["unit_id"],
            },
            {
                "type": "unit_type_contains",
                "unit_id": plan["unit_id"],
                "marker": "SETTLER",
            },
            {"type": "unit_at", "unit_id": plan["unit_id"], "x": x, "y": y},
        ]
        if not at_target:
            preconditions.append({"type": "unit_has_moves", "unit_id": plan["unit_id"]})
        postconditions = (
            (
                {"type": "unit_absent", "unit_id": plan["unit_id"]},
                {
                    "type": "city_count_at_least",
                    "count": int(plan["baseline_city_count"]) + 1,
                },
                {
                    "type": "city_at_target",
                    "x": target_x,
                    "y": target_y,
                    "owner": plan["owner"],
                },
            )
            if at_target
            else (
                {
                    "type": "unit_moved_from",
                    "unit_id": plan["unit_id"],
                    "x": x,
                    "y": y,
                },
            )
        )
        node_identity = {
            "game_session_id": observation.game_session_id,
            "turn_number": observation.turn_number,
            "source_observation_id": observation.observation_id,
            "source_contract_id": contract.contract_id,
            "source_contract_revision": contract.revision,
            "source_mission_id": mission.mission_id,
            "source_mission_revision": mission.mission_revision,
            "action_type": action_type,
            "entity_id": unit_id,
            "arguments": arguments,
            "target_turn": observation.turn_number,
        }
        node_id = build_turn_action_node_id(**node_identity)
        node = TurnActionNode(
            node_id=node_id,
            graph_id="pending",
            source_observation_projection_hash=observation.projection_hash,
            entity_type="unit",
            preconditions=tuple(preconditions),
            postconditions=postconditions,
            risk=RiskLevel.HIGH.value,
            requires_confirmation=(
                mode is not ExecutionMode.AUTO or action_type not in auto_action_types
            ),
            reason="Advance the active settlement Mission using fresh game facts.",
            idempotency_key=f"turn-action:{node_id}",
            **node_identity,
        )
        return node, None

    def _compile_city_roles_node(
        self,
        observation: NormalizedObservation,
        contract: StrategicContract,
        mission: Mission,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
    ) -> tuple[TurnActionNode | None, str | None]:
        policy = city_roles_mission_plan(mission)
        unavailable = self.unavailable_target(observation, mission)
        if unavailable is not None:
            return None, unavailable
        for city_plan in policy["cities"]:
            queue = city_plan["production_queue"]
            if not queue:
                continue
            city_id = city_plan["city_id"]
            city = observation.city(str(city_id))
            if city is None or city.production.state is not SlotState.EMPTY:
                continue
            item = queue[0]
            arguments = {
                "city_id": city_id,
                "item_type": item["item_type"],
                "item_name": item["item_name"],
            }
            for key in ("target_x", "target_y"):
                if key in item:
                    arguments[key] = item[key]
            node_identity = {
                "game_session_id": observation.game_session_id,
                "turn_number": observation.turn_number,
                "source_observation_id": observation.observation_id,
                "source_contract_id": contract.contract_id,
                "source_contract_revision": contract.revision,
                "source_mission_id": mission.mission_id,
                "source_mission_revision": mission.mission_revision,
                "action_type": "city_set_production",
                "entity_id": str(city_id),
                "arguments": arguments,
                "target_turn": observation.turn_number,
            }
            node_id = build_turn_action_node_id(**node_identity)
            node = TurnActionNode(
                node_id=node_id,
                graph_id="pending",
                source_observation_projection_hash=observation.projection_hash,
                entity_type="city",
                preconditions=(
                    {
                        "type": "entity_exists",
                        "entity_type": "city",
                        "entity_id": city_id,
                    },
                    {"type": "city_has_no_production", "city_id": city_id},
                ),
                postconditions=(
                    {
                        "type": "city_production_equals",
                        "city_id": city_id,
                        "item_name": item["item_name"],
                    },
                ),
                risk=RiskLevel.LOW.value,
                requires_confirmation=(
                    mode is not ExecutionMode.AUTO
                    or "city_set_production" not in auto_action_types
                ),
                reason="Advance the active city-role production policy.",
                idempotency_key=f"turn-action:{node_id}",
                **node_identity,
            )
            return node, None
        return None, None

    def _compile_tactical_emergency_node(
        self,
        observation: NormalizedObservation,
        contract: StrategicContract,
        mission: Mission,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
    ) -> tuple[TurnActionNode | None, str | None]:
        plan = tactical_emergency_mission_order(mission)
        unavailable = self.unavailable_target(observation, mission)
        if unavailable is not None:
            return None, unavailable
        if plan["target_turn"] > observation.turn_number:
            return None, None
        unit = observation.unit(str(plan["unit_id"]))
        if unit is None or unit.moves_remaining is None or unit.moves_remaining <= 0:
            return None, f"unit:{plan['unit_id']}:no-moves"
        values = thaw_json(unit.values)
        order = plan["order"]
        kind = order["kind"]
        action_type = {
            "move": "tactical_unit_move",
            "fortify": "tactical_unit_fortify",
            "skip": "tactical_unit_skip",
        }[kind]
        arguments = {"unit_id": plan["unit_id"]}
        if kind == "move":
            arguments.update(
                {"target_x": order["target_x"], "target_y": order["target_y"]}
            )
        preconditions = [
            {
                "type": "entity_exists",
                "entity_type": "unit",
                "entity_id": plan["unit_id"],
            },
            {"type": "unit_has_moves", "unit_id": plan["unit_id"]},
        ]
        if kind == "move":
            preconditions.append(
                {
                    "type": "unit_at",
                    "unit_id": plan["unit_id"],
                    "x": values.get("x"),
                    "y": values.get("y"),
                }
            )
            postconditions = [
                {
                    "type": "unit_at",
                    "unit_id": plan["unit_id"],
                    "x": order["target_x"],
                    "y": order["target_y"],
                }
            ]
        else:
            postconditions = [{"type": "unit_no_moves", "unit_id": plan["unit_id"]}]
        node_identity = {
            "game_session_id": observation.game_session_id,
            "turn_number": observation.turn_number,
            "source_observation_id": observation.observation_id,
            "source_contract_id": contract.contract_id,
            "source_contract_revision": contract.revision,
            "source_mission_id": mission.mission_id,
            "source_mission_revision": mission.mission_revision,
            "action_type": action_type,
            "entity_id": str(plan["unit_id"]),
            "arguments": arguments,
            "target_turn": int(plan["target_turn"]),
        }
        node_id = build_turn_action_node_id(**node_identity)
        node = TurnActionNode(
            node_id=node_id,
            graph_id="pending",
            source_observation_projection_hash=observation.projection_hash,
            entity_type="unit",
            preconditions=tuple(preconditions),
            postconditions=tuple(postconditions),
            risk=RiskLevel.HIGH.value,
            requires_confirmation=True,
            reason=(
                "Execute the reviewed tactical/emergency unit response for "
                f"turn {plan['target_turn']}."
            ),
            idempotency_key=f"turn-action:{node_id}",
            **node_identity,
        )
        return node, None

    def _compile_diplomacy_trade_node(
        self,
        observation: NormalizedObservation,
        contract: StrategicContract,
        mission: Mission,
        *,
        mode: ExecutionMode,
        auto_action_types: set[str],
    ) -> tuple[TurnActionNode | None, str | None]:
        policy = diplomacy_trade_mission_policy(mission)
        target_player_id = policy.get("envoy_player_id")
        if target_player_id is None:
            return None, None
        blocker_kind = "ENDTURN_BLOCKING_GIVE_INFLUENCE_TOKEN"
        if not any(
            blocker.blocker_type == blocker_kind for blocker in observation.blockers
        ):
            return None, None
        node_identity = {
            "game_session_id": observation.game_session_id,
            "turn_number": observation.turn_number,
            "source_observation_id": observation.observation_id,
            "source_contract_id": contract.contract_id,
            "source_contract_revision": contract.revision,
            "source_mission_id": mission.mission_id,
            "source_mission_revision": mission.mission_revision,
            "action_type": "send_envoy",
            "entity_id": str(target_player_id),
            "arguments": {"player_id": target_player_id},
            "target_turn": observation.turn_number,
        }
        node_id = build_turn_action_node_id(**node_identity)
        node = TurnActionNode(
            node_id=node_id,
            graph_id="pending",
            source_observation_projection_hash=observation.projection_hash,
            entity_type="city_state",
            preconditions=(
                {"type": "blocker_kind_present", "blocker_kind": blocker_kind},
            ),
            postconditions=({"type": "no_blocker_kind", "blocker_kind": blocker_kind},),
            risk=RiskLevel.HIGH.value,
            requires_confirmation=(
                mode is not ExecutionMode.AUTO or "send_envoy" not in auto_action_types
            ),
            reason="Send one reviewed envoy to clear the current turn blocker.",
            idempotency_key=f"turn-action:{node_id}",
            **node_identity,
        )
        return node, None

    @staticmethod
    def _settler_target_is_safe(values, plan) -> bool:
        thawed = thaw_json(values)
        targets = thawed.get("targets", thawed.get("candidate_targets", ()))
        if not isinstance(targets, list):
            return False
        for target in targets:
            if not isinstance(target, dict):
                continue
            if (
                target.get("x") == plan["target_x"]
                and target.get("y") == plan["target_y"]
                and target.get("legal") is True
                and target.get("reachable") is True
            ):
                return True
        return False


def turn_action_node_as_execution(
    node: TurnActionNode,
    *,
    status: TaskStatus,
    retry_count: int = 0,
    max_retries: int = 2,
    last_error: str | None = None,
    approved_by: str | None = None,
) -> TurnActionExecution:
    """Adapt the canonical node to the proven action and verification surface."""

    return TurnActionExecution(
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
