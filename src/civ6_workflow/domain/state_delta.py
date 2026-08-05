"""Deterministic comparison and Mission impact contracts."""

from __future__ import annotations

from enum import StrEnum
from hashlib import sha256
import json

from pydantic import Field

from .base import DomainModel, ImmutableJsonValue, thaw_json
from .contracts import MissionGraph
from .observations import NormalizedObservation, SlotState


class ObservationComparisonKind(StrEnum):
    INITIAL_BASELINE = "INITIAL_BASELINE"
    REBASELINE_REQUIRED = "REBASELINE_REQUIRED"
    NO_CHANGE = "NO_CHANGE"
    STATE_DELTA = "STATE_DELTA"


class StateDeltaChangeKind(StrEnum):
    FIELD_CHANGED = "FIELD_CHANGED"
    ENTITY_CREATED = "ENTITY_CREATED"
    ENTITY_DELETED = "ENTITY_DELETED"


class StateDeltaChange(DomainModel):
    change_kind: StateDeltaChangeKind
    scope: str = Field(min_length=1)
    field_path: str = Field(min_length=1)
    before: ImmutableJsonValue
    after: ImmutableJsonValue


class StateDelta(DomainModel):
    state_delta_id: str = Field(min_length=1)
    game_session_id: str = Field(min_length=1)
    baseline_observation_id: str = Field(min_length=1)
    current_observation_id: str = Field(min_length=1)
    baseline_turn: int = Field(ge=0)
    current_turn: int = Field(ge=0)
    normalization_version: str = Field(min_length=1)
    source_version: str = Field(min_length=1)
    changes: tuple[StateDeltaChange, ...]

    def model_post_init(self, __context: object) -> None:
        identities = tuple((item.scope, item.field_path) for item in self.changes)
        if identities != tuple(sorted(set(identities))):
            raise ValueError("StateDelta changes must be unique and sorted")
        expected = build_state_delta_id(
            self.game_session_id,
            self.baseline_observation_id,
            self.current_observation_id,
            self.changes,
        )
        if self.state_delta_id != expected:
            raise ValueError("StateDelta identity does not match its evidence")


class ObservationComparisonResult(DomainModel):
    kind: ObservationComparisonKind
    reason: str = Field(min_length=1)
    state_delta: StateDelta | None = None

    def model_post_init(self, __context: object) -> None:
        has_delta = self.state_delta is not None
        if has_delta != (self.kind is ObservationComparisonKind.STATE_DELTA):
            raise ValueError("only STATE_DELTA comparison results carry a StateDelta")


def build_state_delta_id(
    game_session_id: str,
    baseline_observation_id: str,
    current_observation_id: str,
    changes: tuple[StateDeltaChange, ...],
) -> str:
    payload = {
        "game_session_id": game_session_id,
        "baseline_observation_id": baseline_observation_id,
        "current_observation_id": current_observation_id,
        "changes": [item.model_dump(mode="json") for item in changes],
    }
    digest = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"state_delta_{digest}"


STRATEGIC_SCOPES = (
    "research",
    "civic",
    "opening_strategy",
    "settler",
    "city_roles",
    "diplomacy_trade",
    "tactical_emergency",
)


def _city_role_projection(city: object) -> dict[str, object]:
    return {
        "production": city.production.model_dump(mode="json"),
        "owner": city.values.get("owner"),
        "x": city.values.get("x"),
        "y": city.values.get("y"),
        "role": city.values.get("role", city.values.get("city_role")),
        "population": city.values.get("population"),
    }


def _settler_projection(unit: object) -> dict[str, object]:
    return {
        "unit_type": unit.unit_type,
        "x": unit.x,
        "y": unit.y,
        "moves_remaining": unit.moves_remaining,
    }


def _tactical_unit_projection(unit: object) -> dict[str, object]:
    return {
        "unit_type": unit.unit_type,
        "x": unit.x,
        "y": unit.y,
        "health": unit.health,
        "max_health": unit.max_health,
        "moves_remaining": unit.moves_remaining,
        "action_state": unit.action_state,
        "needs_promotion": unit.needs_promotion,
    }


def _blocker_projection(
    observation: NormalizedObservation,
    source_types: set[str],
) -> list[dict[str, object]]:
    projection: list[dict[str, object]] = []
    for blocker in observation.blockers:
        if blocker.source_type not in source_types:
            continue
        values = thaw_json(blocker.values)
        data = values.get("data")
        rows = data if isinstance(data, list) else [values]
        entries = [
            {
                "identity": row.get("blocker_id")
                or row.get("offer_id")
                or row.get("notification_id")
                or row.get("diplomacy_id")
                or row.get("request_id")
                or row.get("deal_id")
                or row.get("player_id")
                or row.get("other_player_id"),
                "status": row.get("status"),
                "action_required": row.get(
                    "is_action_required",
                    row.get(
                        "action_required",
                        row.get("actionRequired", row.get("blocking")),
                    ),
                ),
            }
            for row in rows
            if isinstance(row, dict)
        ]
        projection.append(
            {
                "source_type": blocker.source_type,
                "blocker_type": blocker.blocker_type,
                "entries": sorted(
                    entries,
                    key=lambda row: json.dumps(
                        row, sort_keys=True, separators=(",", ":"), default=str
                    ),
                ),
            }
        )
    return sorted(
        projection,
        key=lambda row: json.dumps(
            row, sort_keys=True, separators=(",", ":"), default=str
        ),
    )


class StateDeltaBuilder:
    """Compare only fields whose completeness is explicitly proven."""

    def compare(
        self,
        baseline: NormalizedObservation | None,
        current: NormalizedObservation,
    ) -> ObservationComparisonResult:
        if baseline is None:
            return ObservationComparisonResult(
                kind=ObservationComparisonKind.INITIAL_BASELINE,
                reason="no accepted Observation baseline exists",
            )
        if baseline.game_session_id != current.game_session_id:
            return ObservationComparisonResult(
                kind=ObservationComparisonKind.REBASELINE_REQUIRED,
                reason="Observation game session changed",
            )
        if baseline.normalization_version != current.normalization_version:
            return ObservationComparisonResult(
                kind=ObservationComparisonKind.REBASELINE_REQUIRED,
                reason="Observation normalization version is incompatible",
            )
        if baseline.source_version != current.source_version:
            return ObservationComparisonResult(
                kind=ObservationComparisonKind.REBASELINE_REQUIRED,
                reason="Observation source version is incompatible",
            )
        if current.turn_number < baseline.turn_number:
            return ObservationComparisonResult(
                kind=ObservationComparisonKind.REBASELINE_REQUIRED,
                reason="Observation turn regressed",
            )

        changes: list[StateDeltaChange] = []
        for scope, current_field, available_field, completeness_available in (
            (
                "research",
                "current_research",
                "available_research_ids",
                "available_research",
            ),
            ("civic", "current_civic", "available_civic_ids", "available_civics"),
        ):
            before = getattr(baseline.progression, current_field)
            after = getattr(current.progression, current_field)
            if (
                getattr(baseline.completeness, current_field)
                and getattr(current.completeness, current_field)
                and before.state is not SlotState.NOT_LOADED
                and after.state is not SlotState.NOT_LOADED
                and before != after
            ):
                changes.append(
                    StateDeltaChange(
                        change_kind=StateDeltaChangeKind.FIELD_CHANGED,
                        scope=scope,
                        field_path=f"progression.{current_field}",
                        before=before.model_dump(mode="json"),
                        after=after.model_dump(mode="json"),
                    )
                )
            if not (
                getattr(baseline.completeness, completeness_available)
                and getattr(current.completeness, completeness_available)
            ):
                continue
            before_available = sorted(
                item.value for item in getattr(baseline.progression, available_field)
            )
            after_available = sorted(
                item.value for item in getattr(current.progression, available_field)
            )
            if before_available != after_available:
                changes.append(
                    StateDeltaChange(
                        change_kind=StateDeltaChangeKind.FIELD_CHANGED,
                        scope=scope,
                        field_path=f"progression.{available_field}",
                        before=before_available,
                        after=after_available,
                    )
                )

        if baseline.completeness.cities and current.completeness.cities:
            self._append_entity_changes(
                changes,
                scope="city_roles",
                collection="cities",
                before={
                    city.entity_id.value: _city_role_projection(city)
                    for city in baseline.cities
                },
                after={
                    city.entity_id.value: _city_role_projection(city)
                    for city in current.cities
                },
            )

        if (
            baseline.completeness.units
            and current.completeness.units
            and baseline.units is not None
            and current.units is not None
        ):
            self._append_entity_changes(
                changes,
                scope="settler",
                collection="units",
                before={
                    unit.entity_id.value: _settler_projection(unit)
                    for unit in baseline.units
                    if "SETTLER" in unit.unit_type
                },
                after={
                    unit.entity_id.value: _settler_projection(unit)
                    for unit in current.units
                    if "SETTLER" in unit.unit_type
                },
            )
            self._append_entity_changes(
                changes,
                scope="tactical_emergency",
                collection="units",
                before={
                    unit.entity_id.value: _tactical_unit_projection(unit)
                    for unit in baseline.units
                },
                after={
                    unit.entity_id.value: _tactical_unit_projection(unit)
                    for unit in current.units
                },
            )

        if baseline.completeness.blockers and current.completeness.blockers:
            self._append_projection_change(
                changes,
                scope="diplomacy_trade",
                field_path="blockers.diplomacy_trade",
                before=_blocker_projection(
                    baseline,
                    {"pending_diplomacy", "pending_trades", "end_turn_blocker"},
                ),
                after=_blocker_projection(
                    current,
                    {"pending_diplomacy", "pending_trades", "end_turn_blocker"},
                ),
            )
            self._append_projection_change(
                changes,
                scope="tactical_emergency",
                field_path="blockers.tactical_emergency",
                before=_blocker_projection(
                    baseline,
                    {"end_turn_blocker", "tactical_emergency"},
                ),
                after=_blocker_projection(
                    current,
                    {"end_turn_blocker", "tactical_emergency"},
                ),
            )

        if not changes:
            if not any(
                current.completeness.supports_scope(scope) for scope in STRATEGIC_SCOPES
            ):
                return ObservationComparisonResult(
                    kind=ObservationComparisonKind.REBASELINE_REQUIRED,
                    reason="current Observation has no complete strategic scope",
                )
            return ObservationComparisonResult(
                kind=ObservationComparisonKind.NO_CHANGE,
                reason="no comparable strategic fact changed",
            )

        ordered = tuple(sorted(changes, key=lambda item: (item.scope, item.field_path)))
        delta = StateDelta(
            state_delta_id=build_state_delta_id(
                current.game_session_id,
                baseline.observation_id,
                current.observation_id,
                ordered,
            ),
            game_session_id=current.game_session_id,
            baseline_observation_id=baseline.observation_id,
            current_observation_id=current.observation_id,
            baseline_turn=baseline.turn_number,
            current_turn=current.turn_number,
            normalization_version=current.normalization_version,
            source_version=current.source_version,
            changes=ordered,
        )
        return ObservationComparisonResult(
            kind=ObservationComparisonKind.STATE_DELTA,
            reason="comparable strategic facts changed",
            state_delta=delta,
        )

    @staticmethod
    def _append_projection_change(
        changes: list[StateDeltaChange],
        *,
        scope: str,
        field_path: str,
        before: object,
        after: object,
    ) -> None:
        if before == after:
            return
        changes.append(
            StateDeltaChange(
                change_kind=StateDeltaChangeKind.FIELD_CHANGED,
                scope=scope,
                field_path=field_path,
                before=before,
                after=after,
            )
        )

    @staticmethod
    def _append_entity_changes(
        changes: list[StateDeltaChange],
        *,
        scope: str,
        collection: str,
        before: dict[str, object],
        after: dict[str, object],
    ) -> None:
        for entity_id in sorted(set(before) | set(after)):
            field_path = f"{collection}.{entity_id}"
            if entity_id not in before:
                changes.append(
                    StateDeltaChange(
                        change_kind=StateDeltaChangeKind.ENTITY_CREATED,
                        scope=scope,
                        field_path=field_path,
                        before=None,
                        after=after[entity_id],
                    )
                )
            elif entity_id not in after:
                changes.append(
                    StateDeltaChange(
                        change_kind=StateDeltaChangeKind.ENTITY_DELETED,
                        scope=scope,
                        field_path=field_path,
                        before=before[entity_id],
                        after=None,
                    )
                )
            elif before[entity_id] != after[entity_id]:
                changes.append(
                    StateDeltaChange(
                        change_kind=StateDeltaChangeKind.FIELD_CHANGED,
                        scope=scope,
                        field_path=field_path,
                        before=before[entity_id],
                        after=after[entity_id],
                    )
                )


class MissionImpactAnalyzer:
    """Expand directly changed scopes and entities to a Mission closure."""

    def affected_mission_ids(
        self,
        state_delta: StateDelta,
        mission_graph: MissionGraph,
    ) -> tuple[str, ...]:
        changed_scopes = {item.scope for item in state_delta.changes}
        direct = {
            mission.mission_id
            for mission in mission_graph.missions
            if mission.scope in changed_scopes
        }
        changed_unit_ids = {
            item.field_path.split(".", 1)[1]
            for item in state_delta.changes
            if item.scope in {"settler", "tactical_emergency"}
            and item.field_path.startswith("units.")
        }
        direct.update(
            mission.mission_id
            for mission in mission_graph.missions
            if mission.subject.subject_type == "unit"
            and mission.subject.subject_id in changed_unit_ids
        )
        if not direct:
            return ()

        missions = {mission.mission_id: mission for mission in mission_graph.missions}
        affected = set(direct)
        changed = True
        while changed:
            changed = False
            for mission in mission_graph.missions:
                shared_slot = any(
                    other_id in affected
                    and missions[other_id].subject == mission.subject
                    and missions[other_id].slot == mission.slot
                    for other_id in missions
                )
                if (
                    affected.intersection(mission.dependency_mission_ids) or shared_slot
                ) and mission.mission_id not in affected:
                    affected.add(mission.mission_id)
                    changed = True
                for dependency_id in mission.dependency_mission_ids:
                    if mission.mission_id in affected and dependency_id not in affected:
                        affected.add(dependency_id)
                        changed = True
        return tuple(sorted(affected))
