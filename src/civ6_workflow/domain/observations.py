"""Canonical game observations and adapter-owned normalization helpers."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import json
from typing import Annotated, Any

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
