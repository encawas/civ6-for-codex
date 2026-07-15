"""Structured workflow endpoints for lmwilki/civ6-mcp.

This module is installed as a small overlay and mounted from ``web_api.create_app``.
It deliberately exposes read-only state only. Mutating actions continue to flow
through the upstream MCP tools and their existing game-rule validation.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from fastapi import FastAPI, Query, Request

from civ_mcp import lua as lq


def mount_workflow_routes(app: FastAPI) -> None:
    @app.get("/api/identity")
    async def identity(request: Request):
        civ, seed = await request.app.state.gs.get_game_identity()
        return {"civ": civ, "seed": seed}

    @app.get("/api/notifications")
    async def notifications(request: Request):
        data = await request.app.state.gs.get_notifications()
        return _to_dict(_action_required_notifications(data))

    @app.get("/api/end-turn-blockers")
    async def end_turn_blockers(request: Request):
        return await _end_turn_blockers(request.app.state.gs)

    @app.get("/api/pending-diplomacy")
    async def pending_diplomacy(request: Request):
        return _to_dict(await request.app.state.gs.get_diplomacy_sessions())

    @app.get("/api/pending-trades")
    async def pending_trades(request: Request):
        return _to_dict(await request.app.state.gs.get_pending_deals())

    @app.get("/api/tech-civics")
    async def tech_civics(request: Request):
        return _tech_civics_dict(await request.app.state.gs.get_tech_civics())

    @app.get("/api/workflow/snapshot")
    async def workflow_snapshot(
        request: Request,
        include_units: bool = Query(False),
    ):
        """Return the minimum structured state used by one workflow cycle.

        Calls are intentionally kept sequential because the upstream GameState
        shares one FireTuner connection. This endpoint still removes repeated
        HTTP setup/serialization and guarantees one coherent response contract.
        """

        gs = request.app.state.gs
        overview = await gs.get_game_overview()
        tech_civics_data = _tech_civics_dict(await gs.get_tech_civics())
        cities, city_warnings = await gs.get_cities()
        notifications_data = _action_required_notifications(await gs.get_notifications())
        blockers = await _end_turn_blockers(gs)
        diplomacy_data = await gs.get_diplomacy_sessions()
        trades_data = await gs.get_pending_deals()
        civ, seed = await gs.get_game_identity()
        units = await gs.get_units() if include_units else None
        return {
            "identity": {"civ": civ, "seed": seed},
            "overview": _to_dict(overview),
            "tech_civics": tech_civics_data,
            "cities": _to_dict(cities),
            "city_warnings": _to_dict(city_warnings),
            "units": _to_dict(units),
            "notifications": _to_dict(notifications_data),
            "end_turn_blockers": blockers,
            "pending_diplomacy": _to_dict(diplomacy_data),
            "pending_trades": _to_dict(trades_data),
        }


async def _end_turn_blockers(gs: Any) -> list[dict[str, str]]:
    lines = await gs.conn.execute_write(lq.build_end_turn_blocking_query())
    return [
        {"blocking_type": blocking_type, "message": message}
        for blocking_type, message in lq.parse_end_turn_blocking(lines)
    ]


def _tech_civics_dict(value: Any) -> dict[str, Any]:
    data = _to_dict(value)
    if not isinstance(data, dict):
        return {}
    data["current_research_type"] = _current_option_type(
        data.get("current_research"),
        data.get("available_techs"),
        "tech_type",
    )
    data["current_civic_type"] = _current_option_type(
        data.get("current_civic"),
        data.get("available_civics"),
        "civic_type",
    )
    return data


def _current_option_type(current_name: Any, values: Any, type_key: str) -> str | None:
    if current_name in (None, "", "None", "NONE", "none"):
        return None
    current_text = str(current_name)
    if current_text.startswith(("TECH_", "CIVIC_")):
        return current_text
    if not isinstance(values, list):
        return None
    for value in values:
        if not isinstance(value, dict):
            continue
        if str(value.get("name", "")) == current_text and value.get(type_key):
            return str(value[type_key])
    return None


def _action_required_notifications(values: Any) -> list[Any]:
    if not isinstance(values, (list, tuple)):
        return []
    return [
        value
        for value in values
        if bool(
            getattr(value, "is_action_required", False)
            if not isinstance(value, dict)
            else value.get("is_action_required", value.get("action_required", False))
        )
    ]


def _to_dict(obj: Any):
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, (list, tuple, set)):
        return [_to_dict(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _to_dict(value) for key, value in obj.items()}
    return obj
