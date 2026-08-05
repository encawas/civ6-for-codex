"""Canonical game observations and adapter-owned normalization helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
import json
from typing import Annotated, Any
from uuid import uuid4

from pydantic import AfterValidator, Field, PlainSerializer, field_validator

from .base import (
    DomainModel,
    FrozenDict,
    ImmutableJsonObject,
    SourceVersions,
    thaw_json,
)


class SlotState(StrEnum):
    EMPTY = "EMPTY"
    OCCUPIED = "OCCUPIED"
    NOT_LOADED = "NOT_LOADED"


class SlotValue(DomainModel):
    state: SlotState
    value: str | None = None

    @field_validator("value")
    @classmethod
    def normalize_value(cls, value: str | None) -> str | None:
        return value.strip() if value is not None else None

    def model_post_init(self, __context: Any) -> None:
        if self.state is SlotState.OCCUPIED and not self.value:
            raise ValueError("an occupied slot requires a value")
        if self.state is not SlotState.OCCUPIED and self.value is not None:
            raise ValueError("only an occupied slot may carry a value")


EMPTY_SLOT_STRINGS = frozenset({"", "none", "nothing", "null"})
NORMALIZATION_VERSION = "civ6-observation/v1"
OBSERVATION_SOURCE_VERSION = "civ6-runtime-snapshot/v1"


def normalize_slot(value: Any, *, loaded: bool = True) -> SlotValue:
    """Translate upstream empty spellings into one canonical slot value."""

    if not loaded:
        return SlotValue(state=SlotState.NOT_LOADED)
    if value is None or value == {} or value == []:
        return SlotValue(state=SlotState.EMPTY)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.casefold() in EMPTY_SLOT_STRINGS:
            return SlotValue(state=SlotState.EMPTY)
        return SlotValue(state=SlotState.OCCUPIED, value=normalized)
    raise TypeError(f"unsupported slot value: {type(value).__name__}")


class EntityIdentifier(DomainModel):
    value: str = Field(min_length=1)
    external_value: str | int


class NormalizedCity(DomainModel):
    entity_id: EntityIdentifier
    production: SlotValue
    values: ImmutableJsonObject


class ProgressionState(DomainModel):
    current_research: SlotValue
    current_civic: SlotValue
    available_research_ids: tuple[EntityIdentifier, ...] = ()
    available_civic_ids: tuple[EntityIdentifier, ...] = ()


class UnitActionState(StrEnum):
    ACTIONABLE = "ACTIONABLE"
    EXHAUSTED = "EXHAUSTED"
    UNKNOWN = "UNKNOWN"


class NormalizedUnit(DomainModel):
    entity_id: EntityIdentifier
    unit_type: str
    action_state: UnitActionState
    moves_remaining: float | None = None
    x: int | None = None
    y: int | None = None
    health: int | None = None
    max_health: int | None = None
    build_charges: int | None = None
    needs_promotion: bool | None = None
    valid_improvements: tuple[str, ...] = ()
    values: ImmutableJsonObject


class NormalizedBlocker(DomainModel):
    source_type: str
    blocker_type: str | None = None
    values: ImmutableJsonObject


class UnitDetailReason(StrEnum):
    UNIT_BLOCKER = "UNIT_BLOCKER"
    ZERO_CITIES = "ZERO_CITIES"


class UnitSummary(DomainModel):
    details_loaded: bool
    reported_count: int | None = Field(default=None, ge=0)
    actionable_unit_ids: tuple[EntityIdentifier, ...] = ()
    detail_reasons: tuple[UnitDetailReason, ...] = ()

    @property
    def detail_required(self) -> bool:
        return bool(self.detail_reasons) and not self.details_loaded


class ObservationCompleteness(DomainModel):
    """Explicitly records which canonical projections are safe to compare."""

    cities: bool = False
    current_research: bool = False
    available_research: bool = False
    current_civic: bool = False
    available_civics: bool = False
    units: bool = False
    blockers: bool = False

    def supports_scope(self, scope: str) -> bool:
        if scope == "research":
            return self.current_research and self.available_research
        if scope == "civic":
            return self.current_civic and self.available_civics
        if scope == "opening_strategy":
            return (
                self.cities
                and self.current_research
                and self.available_research
                and self.current_civic
                and self.available_civics
            )
        if scope == "settler":
            return self.cities and self.units
        if scope == "city_roles":
            return self.cities
        if scope == "diplomacy_trade":
            return self.blockers
        if scope == "tactical_emergency":
            return self.units and self.blockers
        return False


class NormalizedObservation(DomainModel):
    observation_id: str = Field(default_factory=lambda: f"obs_{uuid4().hex}")
    game_session_id: str
    turn_number: int = Field(ge=0)
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    normalization_version: str = Field(
        default=NORMALIZATION_VERSION,
        min_length=1,
    )
    source_version: str = Field(
        default=OBSERVATION_SOURCE_VERSION,
        min_length=1,
    )
    completeness: ObservationCompleteness = ObservationCompleteness()
    # Persisted legacy name: this is the Adapter source snapshot, not raw HTTP/MCP.
    raw_observation: ImmutableJsonObject
    cities: tuple[NormalizedCity, ...] = ()
    progression: ProgressionState
    units: tuple[NormalizedUnit, ...] | None = None
    blockers: tuple[NormalizedBlocker, ...] = ()
    unit_summary: UnitSummary

    def model_post_init(self, __context: Any) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError(
                "NormalizedObservation observed_at must include a timezone"
            )

    def city(self, entity_id: str | int) -> NormalizedCity | None:
        expected = str(entity_id).strip()
        return next(
            (city for city in self.cities if city.entity_id.value == expected),
            None,
        )

    def unit(self, entity_id: str | int) -> NormalizedUnit | None:
        if self.units is None:
            return None
        expected = str(entity_id).strip()
        return next(
            (unit for unit in self.units if unit.entity_id.value == expected),
            None,
        )

    @property
    def semantic_projection(self) -> dict[str, Any]:
        """Return stable facts used by planning, execution and StateDelta."""

        return {
            "game_session_id": self.game_session_id,
            "turn_number": self.turn_number,
            "normalization_version": self.normalization_version,
            "source_version": self.source_version,
            "completeness": self.completeness.model_dump(mode="json"),
            "cities": [
                {
                    "entity_id": city.entity_id.value,
                    "production": city.production.model_dump(mode="json"),
                    "owner": city.values.get("owner"),
                    "x": city.values.get("x"),
                    "y": city.values.get("y"),
                    "role": city.values.get("role", city.values.get("city_role")),
                    "population": city.values.get("population"),
                }
                for city in self.cities
            ],
            "progression": self.progression.model_dump(mode="json"),
            "units": (
                None
                if self.units is None
                else [
                    {
                        "entity_id": unit.entity_id.value,
                        "unit_type": unit.unit_type,
                        "action_state": unit.action_state,
                        "moves_remaining": unit.moves_remaining,
                        "x": unit.x,
                        "y": unit.y,
                        "health": unit.health,
                        "max_health": unit.max_health,
                        "build_charges": unit.build_charges,
                        "needs_promotion": unit.needs_promotion,
                        "valid_improvements": unit.valid_improvements,
                        "owner": unit.values.get("owner"),
                    }
                    for unit in self.units
                ]
            ),
            "blockers": [
                _semantic_blocker_projection(blocker) for blocker in self.blockers
            ],
            "unit_summary": self.unit_summary.model_dump(mode="json"),
        }

    @property
    def source_snapshot_hash(self) -> str:
        return _json_hash(thaw_json(self.raw_observation))

    @property
    def projection_hash(self) -> str:
        """Hash only canonical semantic facts, excluding audit extensions."""

        return _json_hash(self.semantic_projection)


def _semantic_blocker_projection(
    blocker: NormalizedBlocker,
) -> dict[str, Any]:
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
    return {
        "source_type": blocker.source_type,
        "blocker_type": blocker.blocker_type,
        "entries": sorted(
            entries,
            key=lambda row: json.dumps(
                row, sort_keys=True, separators=(",", ":"), default=str
            ),
        ),
    }


def _json_hash(projection: Any) -> str:
    encoded = json.dumps(
        projection,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _freeze_string_map(value: dict[str, str]) -> FrozenDict:
    return FrozenDict(value)


ImmutableStringMap = Annotated[
    dict[str, str],
    AfterValidator(_freeze_string_map),
    PlainSerializer(thaw_json, return_type=dict[str, str]),
]


class Observation(DomainModel):
    """Revision envelope for persisted entity state, not current game-fact authority."""

    observation_id: str
    game_session_id: str
    turn_number: int = Field(ge=0)
    sequence: int = Field(ge=0)
    observed_at: datetime
    source_versions: SourceVersions
    base_state: ImmutableJsonObject
    entity_revisions: ImmutableStringMap = {}

    @property
    def projection_hash(self) -> str:
        projection = {
            "game_session_id": self.game_session_id,
            "turn_number": self.turn_number,
            "source_versions": self.source_versions.model_dump(mode="json"),
            "base_state": thaw_json(self.base_state),
            "entity_revisions": thaw_json(self.entity_revisions),
        }
        encoded = json.dumps(
            projection,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return sha256(encoded).hexdigest()
