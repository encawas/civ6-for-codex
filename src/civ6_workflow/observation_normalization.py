"""Versioned normalization boundary for legacy runtime observations."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .domain.observations import (
    EntityIdentifier,
    NormalizedBlocker,
    NormalizedCity,
    NormalizedObservation,
    NormalizedUnit,
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
    )
    canonical = NormalizedObservation(
        game_session_id=snapshot.game_id,
        turn_number=snapshot.turn,
        raw_observation=raw,
        cities=tuple(cities),
        progression=progression,
        units=None if units is None else tuple(units),
        blockers=tuple(blockers),
        unit_summary=unit_summary,
    )
    normalized_snapshot = snapshot.model_copy(
        update={
            "cities": city_rows,
            "tech_civics": progress_payload,
            "units": unit_rows,
            "blockers": blocker_rows,
        },
        deep=True,
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
    cities: list[NormalizedCity] = []
    payloads: list[dict[str, Any]] = []
    for row in _rows(value, "cities"):
        raw_id = row.get("city_id", row.get("id"))
        if raw_id is None:
            continue
        entity_id = normalize_entity_identifier(raw_id)
        production = normalize_slot(row.get("currently_building", row.get("producing")))
        normalized = deepcopy(row)
        normalized.update(
            {
                "city_id": entity_id.external_value,
                "currently_building": (
                    production.value if production.state is SlotState.OCCUPIED else None
                ),
            }
        )
        cities.append(
            NormalizedCity(
                entity_id=entity_id,
                production=production,
                values=normalized,
            )
        )
        payloads.append(normalized)
    return cities, payloads


def _normalize_progression(
    value: Any,
) -> tuple[ProgressionState, dict[str, Any]]:
    progress = value if isinstance(value, dict) else {}
    research_available, research_rows = _available_progression(
        progress.get("available_techs"),
        "tech_type",
    )
    civic_available, civic_rows = _available_progression(
        progress.get("available_civics"),
        "civic_type",
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
    payload["available_techs"] = research_rows
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
        if row.get(type_key)
    }
    return SlotValue(
        state=SlotState.OCCUPIED,
        value=by_name.get(value.casefold(), value),
    )


def _available_progression(
    value: Any,
    type_key: str,
) -> tuple[list[EntityIdentifier], list[dict[str, Any]]]:
    identifiers: list[EntityIdentifier] = []
    rows: list[dict[str, Any]] = []
    for row in _rows(value):
        raw_id = row.get(type_key)
        if raw_id is None:
            continue
        entity_id = normalize_entity_identifier(raw_id)
        normalized_id = entity_id.value.upper()
        normalized = deepcopy(row)
        normalized.update(
            {
                type_key: normalized_id,
                "name": str(row.get("name", "")).strip(),
            }
        )
        identifiers.append(
            EntityIdentifier(
                value=normalized_id,
                external_value=normalized_id,
            )
        )
        rows.append(normalized)
    return identifiers, rows


def _normalize_units(
    value: Any,
) -> tuple[list[NormalizedUnit] | None, list[dict[str, Any]] | None]:
    if value is None:
        return None, None
    units: list[NormalizedUnit] = []
    payloads: list[dict[str, Any]] = []
    for row in _rows(value, "units"):
        raw_id = row.get("unit_id", row.get("id"))
        if raw_id is None:
            continue
        entity_id = normalize_entity_identifier(raw_id)
        unit_type = (
            str(row.get("unit_type", row.get("type", row.get("name", ""))))
            .strip()
            .upper()
        )
        moves = _optional_float(row.get("moves_remaining", row.get("moves")))
        if moves is None:
            action_state = UnitActionState.UNKNOWN
        elif moves > 0:
            action_state = UnitActionState.ACTIONABLE
        else:
            action_state = UnitActionState.EXHAUSTED
        normalized = deepcopy(row)
        normalized.update(
            {
                "unit_id": entity_id.external_value,
                "unit_type": unit_type,
                "name": str(row.get("name", "")).strip(),
                "x": _optional_int(row.get("x")),
                "y": _optional_int(row.get("y")),
                "moves_remaining": moves,
                "health": _optional_int(row.get("health")),
                "max_health": _optional_int(row.get("max_health")),
                "needs_promotion": bool(row.get("needs_promotion")),
                "targets": (
                    deepcopy(row.get("targets", [])) if row.get("targets") else []
                ),
                "build_charges": _optional_int(row.get("build_charges")) or 0,
                "valid_improvements": [
                    str(item).strip().upper()
                    for item in (row.get("valid_improvements") or [])
                    if str(item).strip()
                ],
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
        units.append(
            NormalizedUnit(
                entity_id=entity_id,
                unit_type=unit_type,
                action_state=action_state,
                moves_remaining=moves,
                values=normalized,
            )
        )
        payloads.append(normalized)
    return units, payloads


def _normalize_blockers(
    value: Any,
) -> tuple[list[NormalizedBlocker], list[dict[str, Any]]]:
    blockers: list[NormalizedBlocker] = []
    payloads: list[dict[str, Any]] = []
    for row in value if isinstance(value, list) else []:
        if not isinstance(row, dict):
            continue
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
        blockers.append(
            NormalizedBlocker(
                source_type=source_type,
                blocker_type=blocker_type,
                values=normalized,
            )
        )
        payloads.append(normalized)
    return blockers, payloads


def _unit_summary(
    overview: Any,
    cities: list[NormalizedCity],
    units: list[NormalizedUnit] | None,
    blockers: list[NormalizedBlocker],
) -> UnitSummary:
    reasons: list[UnitDetailReason] = []
    if any(blocker.blocker_type == "ENDTURN_BLOCKING_UNITS" for blocker in blockers):
        reasons.append(UnitDetailReason.UNIT_BLOCKER)
    if not cities:
        reasons.append(UnitDetailReason.ZERO_CITIES)
    overview_dict = overview if isinstance(overview, dict) else {}
    reported_count = next(
        (
            _optional_int(overview_dict[key])
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


def _rows(
    value: Any,
    collection_key: str | None = None,
) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        keys = tuple(key for key in (collection_key, "items", "cities", "units") if key)
        value = next((value[key] for key in keys if key in value), [])
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
