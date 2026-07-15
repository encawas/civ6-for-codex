from __future__ import annotations

from typing import Any

from .models import GameEvent, RuntimeSnapshot


def project_agent_context(
    snapshot: RuntimeSnapshot,
    events: list[GameEvent],
    context: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], int]:
    """Build a bounded event-specific state packet for runtime planning.

    The planner never needs the complete empire snapshot for a single blocker.
    This projection keeps the stable overview and only adds domains implicated by
    the current event batch. Unit lists are filtered to actionable or explicitly
    referenced units and capped to prevent an ordinary end-turn blocker from
    producing an unbounded model request.
    """

    event_types = {event.event_type for event in events}
    blocking_types = {
        str(event.payload.get("blocking_type", ""))
        for event in events
        if isinstance(event.payload, dict)
    }
    entity_ids: dict[str, set[str]] = {}
    for event in events:
        if event.entity_type is None or event.entity_id is None:
            continue
        entity_ids.setdefault(event.entity_type, set()).add(str(event.entity_id))

    relevant_state: dict[str, Any] = {
        "turn": snapshot.turn,
        "game_id": snapshot.game_id,
        "overview": _compact_overview(snapshot.overview),
        "blockers": _matching_blockers(snapshot.blockers, events),
    }

    needs_city = bool(entity_ids.get("city")) or any(
        name in event_types
        for name in {"city_no_production", "invalid_city_plan_item"}
    ) or "ENDTURN_BLOCKING_PRODUCTION" in blocking_types
    needs_progress = bool(
        event_types
        & {
            "research_unavailable",
            "civic_unavailable",
            "research_selection_required",
            "civic_selection_required",
        }
    ) or bool(
        blocking_types
        & {"ENDTURN_BLOCKING_RESEARCH", "ENDTURN_BLOCKING_CIVIC"}
    )
    needs_units = _is_unit_event(event_types, blocking_types, entity_ids)
    needs_diplomacy = "pending_diplomacy" in event_types
    needs_trades = "pending_trade_offer" in event_types
    needs_notifications = "action_required_notification" in event_types

    if needs_city:
        relevant_state["cities"] = _filter_rows(
            snapshot.cities,
            keys=("city_id", "id"),
            ids=entity_ids.get("city", set()),
            limit=12,
        )
    if needs_progress:
        relevant_state["tech_civics"] = snapshot.tech_civics
    if needs_units:
        relevant_state["units"] = _relevant_units(
            snapshot.units,
            explicit_ids=(
                entity_ids.get("unit", set())
                | entity_ids.get("builder", set())
            ),
            limit=16,
        )
    if needs_diplomacy:
        relevant_state["diplomacy"] = snapshot.diplomacy
    if needs_trades:
        relevant_state["trades"] = snapshot.trades
    if needs_notifications:
        relevant_state["notifications"] = snapshot.notifications

    relevant_plans: dict[str, Any] = {
        "strategy": context.get("strategy", {}),
    }
    if needs_city:
        relevant_plans["cities"] = _filter_mapping(
            context.get("cities", {}), entity_ids.get("city", set()), limit=12
        )
    if needs_units:
        unit_ids = entity_ids.get("unit", set()) | entity_ids.get("builder", set())
        relevant_plans["units"] = _filter_mapping(
            context.get("units", {}), unit_ids, limit=16
        )
        relevant_plans["builders"] = _filter_builder_plans(
            context.get("builders", {}), unit_ids, limit=12
        )

    # Most event batches require one decision. Allow two tasks per event for a
    # plan update plus an immediate action, while retaining a small hard cap.
    max_tasks = min(8, max(1, len(events) * 2))
    return relevant_state, relevant_plans, max_tasks


def _compact_overview(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    keys = (
        "turn",
        "player_id",
        "civ_name",
        "leader_name",
        "gold",
        "gold_per_turn",
        "science_yield",
        "culture_yield",
        "faith",
        "current_research",
        "current_civic",
        "num_cities",
        "num_units",
        "score",
        "era_name",
        "era_score",
        "game_speed",
    )
    return {key: value[key] for key in keys if key in value}


def _matching_blockers(
    blockers: list[dict[str, Any]], events: list[GameEvent]
) -> list[dict[str, Any]]:
    event_blocking_types = {
        str(event.payload.get("blocking_type"))
        for event in events
        if isinstance(event.payload, dict) and event.payload.get("blocking_type")
    }
    event_types = {event.event_type for event in events}
    matched: list[dict[str, Any]] = []
    for blocker in blockers:
        blocker_type = str(blocker.get("type", ""))
        blocking_type = str(blocker.get("blocking_type", ""))
        if blocker_type in event_types or blocking_type in event_blocking_types:
            matched.append(blocker)
    return matched or blockers[:8]


def _is_unit_event(
    event_types: set[str],
    blocking_types: set[str],
    entity_ids: dict[str, set[str]],
) -> bool:
    return bool(
        entity_ids.get("unit")
        or entity_ids.get("builder")
        or "ENDTURN_BLOCKING_UNITS" in blocking_types
        or any(
            name.startswith(("unit_", "builder_", "settler_"))
            for name in event_types
        )
    )


def _rows(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, dict):
        value = value.get("items", value.get("units", value.get("cities", [])))
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _filter_rows(
    value: Any,
    *,
    keys: tuple[str, ...],
    ids: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    rows = _rows(value)
    if ids:
        rows = [
            row
            for row in rows
            if any(row.get(key) is not None and str(row[key]) in ids for key in keys)
        ]
    return rows[:limit]


def _relevant_units(
    value: Any,
    *,
    explicit_ids: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    rows = _rows(value)
    selected: list[dict[str, Any]] = []
    for row in rows:
        unit_id = row.get("unit_id", row.get("id"))
        explicit = unit_id is not None and str(unit_id) in explicit_ids
        moves = float(row.get("moves_remaining", row.get("moves", 0)) or 0)
        special = bool(row.get("needs_promotion")) or "SETTLER" in str(
            row.get("unit_type", row.get("name", ""))
        ).upper()
        if explicit or moves > 0 or special:
            selected.append(row)
        if len(selected) >= limit:
            break
    return selected


def _filter_mapping(value: Any, ids: set[str], *, limit: int) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    if ids:
        return {key: row for key, row in value.items() if str(key) in ids}
    return dict(list(value.items())[:limit])


def _filter_builder_plans(
    value: Any, unit_ids: set[str], *, limit: int
) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    matched: dict[str, Any] = {}
    for key, row in value.items():
        assigned = row.get("assigned_unit_id") if isinstance(row, dict) else None
        if not unit_ids or (assigned is not None and str(assigned) in unit_ids):
            matched[str(key)] = row
        if len(matched) >= limit:
            break
    return matched
