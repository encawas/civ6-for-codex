"""Versioned normalization boundary for runtime observations."""

from __future__ import annotations

from datetime import UTC, datetime
from copy import deepcopy
from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Any

from .domain.observations import (
    EntityIdentifier,
    NormalizedBlocker,
    NormalizedCity,
    NormalizedObservation,
    NormalizedUnit,
    ObservationCompleteness,
    ProgressionState,
    SlotState,
    SlotValue,
    UnitActionState,
    UnitDetailReason,
    UnitSummary,
    normalize_slot,
)
from .models import RuntimeSnapshot


@dataclass(frozen=True, slots=True)
class NormalizedRuntimeObservation:
    canonical: NormalizedObservation
    snapshot: RuntimeSnapshot

    @property
    def game_id(self) -> str:
        return self.canonical.game_session_id

    @property
    def turn(self) -> int:
        return self.canonical.turn_number


def normalize_runtime_snapshot(
    snapshot: RuntimeSnapshot,
    *,
    observed_at: datetime | None = None,
) -> NormalizedRuntimeObservation:
    raw = snapshot.model_dump(mode="json")
    cities, city_rows = _normalize_cities(snapshot.cities)
    progression, progress_payload = _normalize_progression(snapshot.tech_civics)
    units, unit_rows = _normalize_units(snapshot.units)
    blockers, blocker_rows = _normalize_blockers(snapshot.blockers)
    unit_summary = _unit_summary(
        snapshot.overview,
        cities,
        units,
        blockers,
        cities_loaded=snapshot.cities_loaded,
    )
    progress_source = snapshot.tech_civics
    canonical = NormalizedObservation(
        observed_at=observed_at or datetime.now(UTC),
        game_session_id=snapshot.game_id,
        turn_number=snapshot.turn,
        raw_observation=raw,
        completeness=ObservationCompleteness(
            cities=snapshot.cities_loaded,
            current_research=snapshot.tech_civics_loaded
            and (
                "current_research" in progress_source
                or "current_research_type" in progress_source
            ),
            available_research=snapshot.tech_civics_loaded
            and "available_techs" in progress_source,
            current_civic=snapshot.tech_civics_loaded
            and (
                "current_civic" in progress_source
                or "current_civic_type" in progress_source
            ),
            available_civics=snapshot.tech_civics_loaded
            and "available_civics" in progress_source,
            units=units is not None,
            blockers=snapshot.blockers_loaded,
        ),
        cities=tuple(cities),
        progression=progression,
        units=None if units is None else tuple(units),
        blockers=tuple(blockers),
        unit_summary=unit_summary,
    )
    # Entity payloads were already copied before normalization; a second deep copy
    # would duplicate the complete snapshot without adding isolation.
    normalized_snapshot = snapshot.model_copy(
        update={
            "cities": city_rows,
            "tech_civics": progress_payload,
            "units": unit_rows,
            "blockers": blocker_rows,
        }
    )
    return NormalizedRuntimeObservation(
        canonical=canonical,
        snapshot=normalized_snapshot,
    )


def normalize_entity_identifier(value: Any) -> EntityIdentifier:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError("entity identifier must be a string or integer")
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("entity identifier must not be empty")
    external: str | int = value if isinstance(value, int) else normalized
    return EntityIdentifier(value=normalized, external_value=external)


def _normalize_cities(
    value: Any,
) -> tuple[list[NormalizedCity], list[dict[str, Any]]]:
    normalized_by_id: dict[str, tuple[NormalizedCity, dict[str, Any]]] = {}
    for row in _collection_rows(value, "cities"):
        raw_id = row.get("city_id", row.get("id"))
        if raw_id is None:
            raise ValueError("city entry omitted a stable entity identifier")
        entity_id = normalize_entity_identifier(raw_id)
        production_loaded = "currently_building" in row or "producing" in row
        production = normalize_slot(
            row.get("currently_building", row.get("producing")),
            loaded=production_loaded,
        )
        normalized = deepcopy(row)
        normalized.update(
            {
                "city_id": entity_id.external_value,
                "currently_building": (
                    production.value if production.state is SlotState.OCCUPIED else None
                ),
                "x": _optional_int(row.get("x"), field="city.x"),
                "y": _optional_int(row.get("y"), field="city.y"),
            }
        )
        city = NormalizedCity(
            entity_id=entity_id,
            production=production,
            values=normalized,
        )
        _insert_unique(normalized_by_id, entity_id.value, (city, normalized), "city")
    ordered = [normalized_by_id[key] for key in sorted(normalized_by_id)]
    return [item[0] for item in ordered], [item[1] for item in ordered]


def _normalize_progression(
    value: dict[str, Any],
) -> tuple[ProgressionState, dict[str, Any]]:
    progress = value
    research_available, research_rows = _available_progression(
        progress,
        collection_key="available_techs",
        type_key="tech_type",
    )
    civic_available, civic_rows = _available_progression(
        progress,
        collection_key="available_civics",
        type_key="civic_type",
    )
    research = _progression_slot(
        progress,
        current_key="current_research",
        explicit_type_key="current_research_type",
        prefix="TECH_",
        available_rows=research_rows,
        type_key="tech_type",
    )
    civic = _progression_slot(
        progress,
        current_key="current_civic",
        explicit_type_key="current_civic_type",
        prefix="CIVIC_",
        available_rows=civic_rows,
        type_key="civic_type",
    )
    payload = deepcopy(progress)
    payload["current_research"] = research.value
    payload["current_research_type"] = research.value
    payload["current_civic"] = civic.value
    payload["current_civic_type"] = civic.value
    if "available_techs" in progress:
        payload["available_techs"] = research_rows
    if "available_civics" in progress:
        payload["available_civics"] = civic_rows
    return (
        ProgressionState(
            current_research=research,
            current_civic=civic,
            available_research_ids=tuple(research_available),
            available_civic_ids=tuple(civic_available),
        ),
        payload,
    )


def _progression_slot(
    progress: dict[str, Any],
    *,
    current_key: str,
    explicit_type_key: str,
    prefix: str,
    available_rows: list[dict[str, Any]],
    type_key: str,
) -> SlotValue:
    explicit = normalize_slot(
        progress.get(explicit_type_key),
        loaded=explicit_type_key in progress,
    )
    current = normalize_slot(
        progress.get(current_key),
        loaded=current_key in progress,
    )
    if explicit.state is SlotState.OCCUPIED:
        slot = explicit
    elif current.state is not SlotState.NOT_LOADED:
        slot = current
    else:
        slot = explicit
    if slot.state is not SlotState.OCCUPIED:
        return slot
    value = slot.value or ""
    if value.upper().startswith(prefix):
        return SlotValue(state=SlotState.OCCUPIED, value=value.upper())
    by_name = {
        str(row.get("name", "")).strip().casefold(): str(row[type_key]).strip()
        for row in available_rows
    }
    return SlotValue(
        state=SlotState.OCCUPIED,
        value=by_name.get(value.casefold(), value),
    )


def _available_progression(
    progress: dict[str, Any],
    *,
    collection_key: str,
    type_key: str,
) -> tuple[list[EntityIdentifier], list[dict[str, Any]]]:
    if collection_key not in progress:
        return [], []
    normalized_by_id: dict[str, tuple[EntityIdentifier, dict[str, Any]]] = {}
    for row in _collection_rows(progress[collection_key], collection_key):
        raw_id = row.get(type_key)
        if raw_id is None:
            raise ValueError(f"{collection_key} entry omitted {type_key}")
        entity_id = normalize_entity_identifier(raw_id)
        normalized_id = entity_id.value.upper()
        normalized = deepcopy(row)
        normalized.update(
            {
                type_key: normalized_id,
                "name": str(row.get("name", "")).strip(),
            }
        )
        identifier = EntityIdentifier(
            value=normalized_id,
            external_value=normalized_id,
        )
        _insert_unique(
            normalized_by_id,
            normalized_id,
            (identifier, normalized),
            collection_key,
        )
    ordered = [normalized_by_id[key] for key in sorted(normalized_by_id)]
    return [item[0] for item in ordered], [item[1] for item in ordered]


def _normalize_units(
    value: Any,
) -> tuple[list[NormalizedUnit] | None, list[dict[str, Any]] | None]:
    if value is None:
        return None, None
    normalized_by_id: dict[str, tuple[NormalizedUnit, dict[str, Any]]] = {}
    for row in _collection_rows(value, "units"):
        raw_id = row.get("unit_id", row.get("id"))
        if raw_id is None:
            raise ValueError("unit entry omitted a stable entity identifier")
        entity_id = normalize_entity_identifier(raw_id)
        unit_type = (
            str(row.get("unit_type", row.get("type", row.get("name", ""))))
            .strip()
            .upper()
        )
        if not unit_type:
            raise ValueError(f"unit {entity_id.value} omitted its unit type")
        moves = _optional_float(
            row.get("moves_remaining", row.get("moves")),
            field=f"unit {entity_id.value} moves_remaining",
        )
        if moves is None:
            action_state = UnitActionState.UNKNOWN
        elif moves > 0:
            action_state = UnitActionState.ACTIONABLE
        else:
            action_state = UnitActionState.EXHAUSTED
        x = _optional_int(row.get("x"), field=f"unit {entity_id.value} x")
        y = _optional_int(row.get("y"), field=f"unit {entity_id.value} y")
        health = _optional_int(
            row.get("health"), field=f"unit {entity_id.value} health"
        )
        max_health = _optional_int(
            row.get("max_health"), field=f"unit {entity_id.value} max_health"
        )
        build_charges = _optional_int(
            row.get("build_charges"),
            field=f"unit {entity_id.value} build_charges",
        )
        needs_promotion = _optional_bool(
            row.get("needs_promotion"),
            field=f"unit {entity_id.value} needs_promotion",
        )
        valid_improvements = tuple(
            sorted(
                {
                    str(item).strip().upper()
                    for item in (row.get("valid_improvements") or [])
                    if str(item).strip()
                }
            )
        )
        normalized = deepcopy(row)
        normalized.update(
            {
                "unit_id": entity_id.external_value,
                "unit_type": unit_type,
                "name": str(row.get("name", "")).strip(),
                "x": x,
                "y": y,
                "moves_remaining": moves,
                "health": health,
                "max_health": max_health,
                "needs_promotion": needs_promotion,
                "targets": deepcopy(row.get("targets") or []),
                "build_charges": build_charges,
                "valid_improvements": list(valid_improvements),
            }
        )
        for key in (
            "origin_city_id",
            "home_city_id",
            "produced_by_city_id",
            "city_id",
        ):
            if row.get(key) is not None:
                normalized[key] = normalize_entity_identifier(row[key]).external_value
        unit = NormalizedUnit(
            entity_id=entity_id,
            unit_type=unit_type,
            action_state=action_state,
            moves_remaining=moves,
            x=x,
            y=y,
            health=health,
            max_health=max_health,
            build_charges=build_charges,
            needs_promotion=needs_promotion,
            valid_improvements=valid_improvements,
            values=normalized,
        )
        _insert_unique(normalized_by_id, entity_id.value, (unit, normalized), "unit")
    ordered = [normalized_by_id[key] for key in sorted(normalized_by_id)]
    return [item[0] for item in ordered], [item[1] for item in ordered]


def _normalize_blockers(
    value: Any,
) -> tuple[list[NormalizedBlocker], list[dict[str, Any]]]:
    normalized_by_id: dict[str, tuple[NormalizedBlocker, dict[str, Any]]] = {}
    for row in _collection_rows(value, "blockers"):
        source_type = str(row.get("type", "unknown_blocker")).strip().casefold()
        raw_blocker_type = row.get("blocking_type")
        blocker_type = (
            str(raw_blocker_type).strip().upper()
            if raw_blocker_type is not None and str(raw_blocker_type).strip()
            else None
        )
        normalized = deepcopy(row)
        normalized["type"] = source_type
        if blocker_type is not None:
            normalized["blocking_type"] = blocker_type
        blocker = NormalizedBlocker(
            source_type=source_type,
            blocker_type=blocker_type,
            values=normalized,
        )
        identity = _blocker_identity(normalized)
        _insert_unique(
            normalized_by_id,
            identity,
            (blocker, normalized),
            "blocker",
        )
    ordered = [normalized_by_id[key] for key in sorted(normalized_by_id)]
    return [item[0] for item in ordered], [item[1] for item in ordered]


def _blocker_identity(row: dict[str, Any]) -> str:
    source = str(row.get("type", "unknown_blocker"))
    explicit = next(
        (
            str(row[key]).strip()
            for key in (
                "blocker_id",
                "offer_id",
                "notification_id",
                "player_id",
                "blocking_type",
            )
            if row.get(key) is not None and str(row[key]).strip()
        ),
        None,
    )
    if explicit is not None:
        return f"{source}:{explicit}"
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
    return f"{source}:{sha256(encoded.encode('utf-8')).hexdigest()}"


def _unit_summary(
    overview: Any,
    cities: list[NormalizedCity],
    units: list[NormalizedUnit] | None,
    blockers: list[NormalizedBlocker],
    *,
    cities_loaded: bool,
) -> UnitSummary:
    reasons: list[UnitDetailReason] = []
    if any(blocker.blocker_type == "ENDTURN_BLOCKING_UNITS" for blocker in blockers):
        reasons.append(UnitDetailReason.UNIT_BLOCKER)
    if cities_loaded and not cities:
        reasons.append(UnitDetailReason.ZERO_CITIES)
    overview_dict = overview if isinstance(overview, dict) else {}
    reported_count = next(
        (
            _optional_int(overview_dict[key], field=f"overview.{key}")
            for key in ("num_units", "unit_count")
            if overview_dict.get(key) is not None
        ),
        None,
    )
    if units is not None:
        reported_count = len(units)
    actionable = tuple(
        unit.entity_id
        for unit in (units or [])
        if unit.action_state is UnitActionState.ACTIONABLE
    )
    return UnitSummary(
        details_loaded=units is not None,
        reported_count=reported_count,
        actionable_unit_ids=actionable,
        detail_reasons=tuple(reasons),
    )


def _collection_rows(
    value: Any,
    collection_key: str,
) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        wrapper_keys = (collection_key, "items", "cities", "units")
        present = [key for key in wrapper_keys if key in value]
        if not present:
            if not value:
                return []
            raise TypeError(f"{collection_key} must be a list or a supported wrapper")
        value = value[present[0]]
    if not isinstance(value, list):
        raise TypeError(f"{collection_key} must be a list")
    if any(not isinstance(row, dict) for row in value):
        raise TypeError(f"{collection_key} entries must be objects")
    return value


def _insert_unique(
    target: dict[str, tuple[Any, dict[str, Any]]],
    identity: str,
    value: tuple[Any, dict[str, Any]],
    collection: str,
) -> None:
    previous = target.get(identity)
    if previous is None:
        target[identity] = value
        return
    if previous[1] != value[1]:
        raise ValueError(f"conflicting duplicate {collection} identity {identity}")


def _optional_int(value: Any, *, field: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field} must be an integer") from exc


def _optional_float(value: Any, *, field: str) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise TypeError(f"{field} must be numeric")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field} must be numeric") from exc


def _optional_bool(value: Any, *, field: str) -> bool | None:
    if value is None or value == "":
        return None
    if type(value) is bool:
        return value
    if type(value) is int and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    raise TypeError(f"{field} must be true, false, 1, or 0")
