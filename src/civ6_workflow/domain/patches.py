"""Validated MissionGraph repair patches."""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any

from pydantic import Field

from .base import DomainModel
from .contracts import Mission, MissionStatus, validate_scope_activation_mission


class MissionGraphPatch(DomainModel):
    patch_id: str = Field(min_length=1)
    patch_hash: str = Field(min_length=64, max_length=64)
    game_session_id: str = Field(min_length=1)
    contract_id: str = Field(min_length=1)
    expected_base_revision: int = Field(ge=1)
    source_state_delta_id: str = Field(min_length=1)
    source_planner_request_id: str = Field(min_length=1)
    source_provider_attempt_id: str = Field(min_length=1)
    source_provider_attempt_number: int = Field(ge=1)
    affected_mission_ids: tuple[str, ...] = Field(min_length=1)
    mission_updates: tuple[Mission, ...] = Field(min_length=1)
    created_from_observation_id: str = Field(min_length=1)
    created_at: datetime

    def model_post_init(self, __context: object) -> None:
        if self.affected_mission_ids != tuple(sorted(set(self.affected_mission_ids))):
            raise ValueError("affected Mission IDs must be unique and sorted")
        update_ids = tuple(mission.mission_id for mission in self.mission_updates)
        if update_ids != tuple(sorted(set(update_ids))):
            raise ValueError("MissionGraphPatch updates must be unique and sorted")
        if set(update_ids) != set(self.affected_mission_ids):
            raise ValueError(
                "MissionGraphPatch must replace exactly the affected Mission Set"
            )
        if len({mission.scope for mission in self.mission_updates}) != 1:
            raise ValueError("MissionGraphPatch cannot combine strategic scopes")
        for mission in self.mission_updates:
            if (
                mission.game_session_id != self.game_session_id
                or mission.contract_id != self.contract_id
            ):
                raise ValueError("MissionGraphPatch Mission aggregate identity differs")
            if mission.status is not MissionStatus.ACTIVE:
                raise ValueError("MissionGraph repair Mission must remain ACTIVE")
            validate_scope_activation_mission(mission, mission.scope)
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("MissionGraphPatch created_at must include a timezone")
        if self.patch_id != build_mission_graph_patch_id(
            self.source_planner_request_id
        ):
            raise ValueError("MissionGraphPatch ID must derive from PlannerRequest")
        if self.patch_hash != mission_graph_patch_hash(self):
            raise ValueError("MissionGraphPatch hash does not match canonical content")


def build_mission_graph_patch_id(source_planner_request_id: str) -> str:
    digest = sha256(source_planner_request_id.encode("utf-8")).hexdigest()[:24]
    return f"mission_graph_patch_{digest}"


def mission_graph_patch_hash(value: MissionGraphPatch | dict[str, Any]) -> str:
    payload = (
        value.model_dump(mode="python", exclude={"patch_hash"})
        if isinstance(value, MissionGraphPatch)
        else {key: item for key, item in value.items() if key != "patch_hash"}
    )
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
        default=lambda item: (
            item.model_dump(mode="json")
            if isinstance(item, DomainModel)
            else item.isoformat()
        ),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def build_mission_graph_patch(**fields: Any) -> MissionGraphPatch:
    payload = dict(fields)
    payload["patch_hash"] = mission_graph_patch_hash(payload)
    return MissionGraphPatch.model_validate(payload)
