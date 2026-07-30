"""Deterministic comparison and Mission impact contracts."""

from __future__ import annotations

from enum import StrEnum
from hashlib import sha256
import json

from pydantic import Field

from .base import DomainModel, ImmutableJsonValue
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
        before = baseline.progression.current_research
        after = current.progression.current_research
        if (
            baseline.completeness.current_research
            and current.completeness.current_research
            and before.state is not SlotState.NOT_LOADED
            and after.state is not SlotState.NOT_LOADED
            and before != after
        ):
            changes.append(
                StateDeltaChange(
                    change_kind=StateDeltaChangeKind.FIELD_CHANGED,
                    scope="research",
                    field_path="progression.current_research",
                    before=before.model_dump(mode="json"),
                    after=after.model_dump(mode="json"),
                )
            )

        if (
            baseline.completeness.available_research
            and current.completeness.available_research
        ):
            before_available = tuple(
                sorted(
                    item.value for item in baseline.progression.available_research_ids
                )
            )
            after_available = tuple(
                sorted(
                    item.value for item in current.progression.available_research_ids
                )
            )
            if before_available != after_available:
                changes.append(
                    StateDeltaChange(
                        change_kind=StateDeltaChangeKind.FIELD_CHANGED,
                        scope="research",
                        field_path="progression.available_research_ids",
                        before=before_available,
                        after=after_available,
                    )
                )

        if not changes:
            if not current.completeness.supports_scope("research"):
                return ObservationComparisonResult(
                    kind=ObservationComparisonKind.REBASELINE_REQUIRED,
                    reason="current Observation is incomplete for research comparison",
                )
            return ObservationComparisonResult(
                kind=ObservationComparisonKind.NO_CHANGE,
                reason="no comparable research fact changed",
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
            reason="comparable research facts changed",
            state_delta=delta,
        )


class MissionImpactAnalyzer:
    """Expand direct research impacts to a deterministic Mission closure."""

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
