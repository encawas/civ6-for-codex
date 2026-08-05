from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from hashlib import sha256
import json
from collections.abc import Mapping
from typing import Any

from pydantic import Field

from .base import DomainModel, ImmutableJsonObject


class TurnActionNodeStatus(StrEnum):
    AWAITING_APPROVAL = "awaiting_confirmation"
    READY = "ready"
    EXECUTING = "running"
    VERIFYING = "verifying"
    SUCCEEDED = "done"
    FAILED = "failed"
    ESCALATED = "escalated"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    UNCERTAIN = "uncertain"


ACTIVE_TURN_ACTION_NODE_STATUSES = frozenset(
    {
        TurnActionNodeStatus.AWAITING_APPROVAL,
        TurnActionNodeStatus.READY,
        TurnActionNodeStatus.EXECUTING,
        TurnActionNodeStatus.VERIFYING,
        TurnActionNodeStatus.UNCERTAIN,
    }
)


class TurnActionNode(DomainModel):
    node_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    game_session_id: str = Field(min_length=1)
    turn_number: int = Field(ge=0)
    source_observation_id: str = Field(min_length=1)
    source_observation_projection_hash: str = Field(min_length=64, max_length=64)
    source_contract_id: str = Field(min_length=1)
    source_contract_revision: int = Field(ge=1)
    source_mission_id: str = Field(min_length=1)
    source_mission_revision: int = Field(ge=1)
    action_type: str = Field(min_length=1)
    entity_type: str = Field(min_length=1)
    entity_id: str = Field(min_length=1)
    arguments: ImmutableJsonObject
    preconditions: tuple[ImmutableJsonObject, ...] = ()
    postconditions: tuple[ImmutableJsonObject, ...] = ()
    invalidators: tuple[ImmutableJsonObject, ...] = ()
    risk: str = Field(min_length=1)
    requires_confirmation: bool = False
    target_turn: int = Field(ge=0)
    dependency_node_ids: tuple[str, ...] = ()
    reason: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)

    def model_post_init(self, __context: object) -> None:
        if self.dependency_node_ids != tuple(sorted(set(self.dependency_node_ids))):
            raise ValueError("TurnActionNode dependencies must be unique and sorted")
        if self.node_id in self.dependency_node_ids:
            raise ValueError("TurnActionNode cannot depend on itself")
        from ..actions import ActionValidationError, resolve_action_spec

        try:
            resolve_action_spec(self.action_type).validate_node_policy(
                entity_type=self.entity_type,
                risk=self.risk,
                requires_confirmation=self.requires_confirmation,
            )
        except ActionValidationError as exc:
            raise ValueError(str(exc)) from exc
        expected = build_turn_action_node_id(
            game_session_id=self.game_session_id,
            turn_number=self.turn_number,
            source_observation_id=self.source_observation_id,
            source_observation_projection_hash=self.source_observation_projection_hash,
            source_contract_id=self.source_contract_id,
            source_contract_revision=self.source_contract_revision,
            source_mission_id=self.source_mission_id,
            source_mission_revision=self.source_mission_revision,
            action_type=self.action_type,
            entity_type=self.entity_type,
            entity_id=self.entity_id,
            arguments=self.arguments,
            preconditions=self.preconditions,
            postconditions=self.postconditions,
            invalidators=self.invalidators,
            risk=self.risk,
            requires_confirmation=self.requires_confirmation,
            dependency_node_ids=self.dependency_node_ids,
            reason=self.reason,
            target_turn=self.target_turn,
        )
        if self.node_id != expected:
            raise ValueError("TurnActionNode identity does not match its sources")
        if self.idempotency_key != f"turn-action:{self.node_id}":
            raise ValueError("TurnActionNode idempotency key is not canonical")


class TurnActionGraph(DomainModel):
    graph_id: str = Field(min_length=1)
    game_session_id: str = Field(min_length=1)
    turn_number: int = Field(ge=0)
    source_observation_id: str = Field(min_length=1)
    source_observation_projection_hash: str = Field(min_length=64, max_length=64)
    source_contract_id: str = Field(min_length=1)
    source_contract_revision: int = Field(ge=1)
    node_ids: tuple[str, ...] = ()
    compiled_at: datetime

    def model_post_init(self, __context: object) -> None:
        if self.node_ids != tuple(sorted(set(self.node_ids))):
            raise ValueError("TurnActionGraph node IDs must be unique and sorted")
        if self.compiled_at.tzinfo is None or self.compiled_at.utcoffset() is None:
            raise ValueError("TurnActionGraph compiled_at must include a timezone")
        expected = build_turn_action_graph_id(
            game_session_id=self.game_session_id,
            turn_number=self.turn_number,
            source_observation_id=self.source_observation_id,
            source_observation_projection_hash=self.source_observation_projection_hash,
            source_contract_id=self.source_contract_id,
            source_contract_revision=self.source_contract_revision,
            node_ids=self.node_ids,
        )
        if self.graph_id != expected:
            raise ValueError("TurnActionGraph identity does not match its sources")


def _canonical_hash(value: Any) -> str:
    def normalize(item: Any) -> Any:
        if isinstance(item, DomainModel):
            return normalize(item.model_dump(mode="json"))
        if isinstance(item, Mapping):
            return {str(key): normalize(value) for key, value in item.items()}
        if isinstance(item, (list, tuple)):
            return [normalize(value) for value in item]
        if isinstance(item, datetime):
            return item.isoformat()
        return item

    encoded = json.dumps(
        normalize(value),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def build_turn_action_node_id(**identity: Any) -> str:
    return f"turn_action_node_{_canonical_hash(identity)[:24]}"


def build_turn_action_graph_id(**identity: Any) -> str:
    return f"turn_action_graph_{_canonical_hash(identity)[:24]}"
