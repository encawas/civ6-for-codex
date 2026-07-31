"""Coordinate strategic repair and current-turn execution projection."""

from __future__ import annotations

from dataclasses import dataclass

from .models import EventLevel, ExecutionMode, GameEvent, RiskLevel, TickResult
from .observation_normalization import NormalizedRuntimeObservation
from .planner_lifecycle import PlannerLifecycleCoordinator
from .ports import WorkflowStorePort
from .turn_compiler import TurnCompiler


_UNIT_DETAIL_SCOPES = {"settler", "tactical_emergency"}
_DIPLOMACY_EVENTS = {
    "pending_diplomacy",
    "pending_trade_offer",
    "war_posture_required",
}
_TACTICAL_EVENTS = {
    "tactical_attack_opportunity",
    "emergency_defense_required",
    "emergency_response_window",
    "tactical_emergency_mission_target_unavailable",
}


@dataclass(frozen=True, slots=True)
class StrategicProjection:
    """Current strategic authority projection prepared for Runtime dispatch."""

    owned_scopes: frozenset[str]
    authoritative_events: tuple[GameEvent, ...]
    current_events: tuple[GameEvent, ...]
    lifecycle_tick: TickResult | None = None
    human_wait_events: tuple[GameEvent, ...] = ()
    human_wait_reason: str | None = None


class StrategicWorkflowCoordinator:
    """Own strategic lifecycle decisions outside the Runtime scheduler."""

    def __init__(
        self,
        *,
        store: WorkflowStorePort,
        planner_lifecycle: PlannerLifecycleCoordinator,
        turn_compiler: TurnCompiler,
    ):
        self.store = store
        self.planner_lifecycle = planner_lifecycle
        self.turn_compiler = turn_compiler

    def requires_unit_details(self, game_id: str) -> bool:
        active = self.store.get_active_strategic_contract(game_id)
        if active is None:
            return False
        scopes = set(active.authority_scope_set.mission_graph_scopes)
        return bool(scopes.intersection(_UNIT_DETAIL_SCOPES))

    async def prepare_projection(
        self,
        ctx,
        observation: NormalizedRuntimeObservation,
        *,
        snapshot_events: tuple[GameEvent, ...],
        mode: ExecutionMode,
        auto_action_types: set[str],
    ) -> StrategicProjection:
        lifecycle_tick = await self.planner_lifecycle.advance_mission_repair(
            ctx, observation
        )
        if lifecycle_tick is not None:
            return StrategicProjection(
                owned_scopes=frozenset(),
                authoritative_events=(),
                current_events=snapshot_events,
                lifecycle_tick=lifecycle_tick,
            )

        snapshot = observation.snapshot
        active_execution = self.store.active_execution_missions(snapshot.game_id)
        active_contract = self.store.get_active_strategic_contract(snapshot.game_id)
        execution_missions = () if active_execution is None else active_execution[1]
        if (
            active_execution is not None
            and active_contract is not None
            and active_execution[0] != active_contract
        ):
            raise RuntimeError(
                "execution Mission projection disagrees with active Contract"
            )
        owned_scopes = frozenset(
            ()
            if active_contract is None
            else active_contract.authority_scope_set.mission_graph_scopes
        )
        authoritative_events = self._mission_events(
            observation, active_contract, execution_missions
        )

        existing_graph_state = self.store.active_turn_action_graph(snapshot.game_id)
        reusable_graph = (
            active_contract is not None
            and bool(execution_missions)
            and existing_graph_state is not None
            and existing_graph_state[0].turn_number == snapshot.turn
            and existing_graph_state[0].source_observation_projection_hash
            == observation.canonical.projection_hash
            and existing_graph_state[0].source_contract_id
            == active_contract.contract_id
            and existing_graph_state[0].source_contract_revision
            == active_contract.revision
        )
        if active_contract is not None and execution_missions and not reusable_graph:
            compilation = self.turn_compiler.compile_missions(
                observation.canonical,
                active_contract,
                execution_missions,
                mode=mode,
                auto_action_types=auto_action_types,
                compiled_at=observation.canonical.observed_at,
            )
            self.store.activate_turn_action_graph(
                compilation.graph,
                compilation.nodes,
                activated_at=observation.canonical.observed_at,
            )

        all_events = (*authoritative_events, *snapshot_events)
        diplomacy_wait = tuple(
            event for event in all_events if event.event_type in _DIPLOMACY_EVENTS
        )
        tactical_wait = tuple(
            event
            for event in authoritative_events
            if event.event_type == "tactical_emergency_mission_target_unavailable"
        )
        human_wait_events: tuple[GameEvent, ...] = ()
        human_wait_reason = None
        if "diplomacy_trade" in owned_scopes and diplomacy_wait:
            human_wait_events = diplomacy_wait
            human_wait_reason = (
                "Diplomacy and trade responses require explicit human review."
            )
        elif "tactical_emergency" in owned_scopes and tactical_wait:
            human_wait_events = tactical_wait
            human_wait_reason = (
                "The active tactical/emergency Mission is no longer safely "
                "executable and requires explicit human review."
            )
        return StrategicProjection(
            owned_scopes=owned_scopes,
            authoritative_events=authoritative_events,
            current_events=self.filter_events(owned_scopes, all_events),
            human_wait_events=human_wait_events,
            human_wait_reason=human_wait_reason,
        )

    async def advance_planning(
        self,
        ctx,
        observation,
        agent_events,
        compatibility,
        *,
        current_events,
    ):
        return await self.planner_lifecycle.advance(
            ctx,
            observation,
            agent_events,
            compatibility,
            current_events=current_events,
        )

    @staticmethod
    def filter_events(owned_scopes: frozenset[str], events) -> tuple[GameEvent, ...]:
        filtered = tuple(events)
        if "city_roles" in owned_scopes:
            filtered = tuple(
                event
                for event in filtered
                if event.event_type
                not in {"city_role_required", "invalid_city_plan_item"}
            )
        if "settler" in owned_scopes:
            filtered = tuple(
                event
                for event in filtered
                if event.event_type != "settler_site_selection_required"
            )
        if "diplomacy_trade" in owned_scopes:
            filtered = tuple(
                event for event in filtered if event.event_type not in _DIPLOMACY_EVENTS
            )
        if "tactical_emergency" in owned_scopes:
            filtered = tuple(
                event for event in filtered if event.event_type not in _TACTICAL_EVENTS
            )
        return filtered

    def _mission_events(
        self, observation, active_contract, execution_missions
    ) -> tuple[GameEvent, ...]:
        if active_contract is None:
            return ()
        snapshot = observation.snapshot
        events: list[GameEvent] = []
        for mission in execution_missions:
            unavailable = self.turn_compiler.unavailable_target(
                observation.canonical, mission
            )
            if unavailable is None:
                continue
            scope = mission.scope
            payload = {
                "contract_id": active_contract.contract_id,
                "contract_revision": active_contract.revision,
                "mission_id": mission.mission_id,
                "mission_revision": mission.mission_revision,
            }
            if scope in {"research", "civic"}:
                target_key = "technology" if scope == "research" else "civic"
                available_ids = (
                    observation.canonical.progression.available_research_ids
                    if scope == "research"
                    else observation.canonical.progression.available_civic_ids
                )
                payload[target_key] = unavailable
                payload["available"] = sorted(item.value for item in available_ids)
            else:
                payload["target"] = unavailable
            events.append(
                GameEvent(
                    event_type=f"{scope}_mission_target_unavailable",
                    turn=snapshot.turn,
                    entity_type=scope,
                    entity_id=unavailable,
                    level=EventLevel.L3,
                    risk=RiskLevel.MEDIUM,
                    blocking=True,
                    payload=payload,
                    dedupe_key=(
                        f"{scope}_mission_target_unavailable:"
                        f"{active_contract.revision}:"
                        f"{mission.mission_revision}:{unavailable}"
                    ),
                )
            )
        return tuple(events)
