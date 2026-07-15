"""Canonical game observations and adapter-owned normalization helpers."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import json
from typing import Any

from pydantic import Field, field_validator

from .base import DomainModel, JsonValue, SourceVersions


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


class Observation(DomainModel):
    observation_id: str
    game_session_id: str
    turn_number: int = Field(ge=0)
    sequence: int = Field(ge=0)
    observed_at: datetime
    source_versions: SourceVersions
    base_state: dict[str, JsonValue]
    entity_revisions: dict[str, str] = {}

    @property
    def projection_hash(self) -> str:
        projection = {
            "game_session_id": self.game_session_id,
            "turn_number": self.turn_number,
            "source_versions": self.source_versions.model_dump(mode="json"),
            "base_state": self.base_state,
            "entity_revisions": self.entity_revisions,
        }
        encoded = json.dumps(
            projection,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return sha256(encoded).hexdigest()
