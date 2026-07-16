"""Canonical game observations and adapter-owned normalization helpers."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import json
from typing import Annotated, Any, Literal
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


class NormalizedObservation(DomainModel):
    observation_id: str = Field(default_factory=lambda: f"obs_{uuid4().hex}")
    game_session_id: str
    turn_number: int = Field(ge=0)
    normalization_version: Literal["civ6-observation/v1"] = NORMALIZATION_VERSION
    raw_observation: ImmutableJsonObject
    cities: tuple[NormalizedCity, ...] = ()
    progression: ProgressionState
    units: tuple[NormalizedUnit, ...] | None = None
    blockers: tuple[NormalizedBlocker, ...] = ()
    unit_summary: UnitSummary

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
    def projection_hash(self) -> str:
        projection = self.model_dump(
            mode="json",
            exclude={"observation_id", "raw_observation"},
        )
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
