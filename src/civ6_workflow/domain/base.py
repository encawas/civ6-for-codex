"""Shared primitives for the canonical workflow domain."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from copy import deepcopy
from enum import StrEnum
from types import MappingProxyType
from typing import Annotated, Any, Self, TypeAlias

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    JsonValue,
    PlainSerializer,
)


class FrozenDict(Mapping[str, Any]):
    """Hashable read-only mapping used by canonical identity and audit values."""

    __slots__ = ("_data",)

    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        data = {key: freeze_json(value) for key, value in (values or {}).items()}
        object.__setattr__(self, "_data", MappingProxyType(data))

    def __setattr__(self, name: str, value: Any) -> None:
        raise TypeError("FrozenDict is immutable")

    def __getitem__(self, key: str) -> FrozenJsonValue:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __hash__(self) -> int:
        return hash(tuple(sorted(self._data.items())))

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        return self


FrozenJsonValue: TypeAlias = (
    type(None) | bool | int | float | str | tuple["FrozenJsonValue", ...] | FrozenDict
)


def freeze_json(value: Any) -> FrozenJsonValue:
    if isinstance(value, FrozenDict):
        return value
    if isinstance(value, Mapping):
        return FrozenDict(value)
    if isinstance(value, (list, tuple)):
        return tuple(freeze_json(item) for item in value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def thaw_json(value: FrozenJsonValue) -> JsonValue:
    if isinstance(value, FrozenDict):
        return {key: thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [thaw_json(item) for item in value]
    return value


def _freeze_object(value: dict[str, JsonValue]) -> FrozenDict:
    return FrozenDict(value)


ImmutableJsonValue = Annotated[
    JsonValue,
    AfterValidator(freeze_json),
    PlainSerializer(thaw_json, return_type=JsonValue),
]
ImmutableJsonObject = Annotated[
    dict[str, JsonValue],
    AfterValidator(_freeze_object),
    PlainSerializer(thaw_json, return_type=dict[str, JsonValue]),
]


class DomainModel(BaseModel):
    """Strict canonical model with validated copies and immutable audit values."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Copy through validation so updates cannot bypass domain invariants."""

        payload = self.model_dump(mode="python")
        if update:
            payload.update(deepcopy(dict(update)) if deep else update)
        return type(self).model_validate(payload)


class ApprovalStatus(StrEnum):
    NOT_REQUIRED = "NOT_REQUIRED"
    REQUIRED = "REQUIRED"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class SubjectRef(DomainModel):
    subject_type: str
    subject_id: str


class Condition(DomainModel):
    condition_type: str
    schema_version: int = 1
    subject: SubjectRef | None = None
    parameters: ImmutableJsonObject = {}
    expected: ImmutableJsonValue = True


class SourceVersions(DomainModel):
    game_api: str
    normalization: str
    runtime: str


class RetryClassification(StrEnum):
    IDEMPOTENT_OR_DEDUPED = "IDEMPOTENT_OR_DEDUPED"
    SAFE_IF_PROVEN_NOT_SENT = "SAFE_IF_PROVEN_NOT_SENT"
    NEVER_BLIND_RETRY = "NEVER_BLIND_RETRY"
