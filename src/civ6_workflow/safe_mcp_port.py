from __future__ import annotations

from .mcp_port import Civ6GamePort as BaseCiv6GamePort


class SafeCiv6GamePort(BaseCiv6GamePort):
    """Fetch unit rows only when a unit blocker makes them necessary."""

    async def read_snapshot(self, *, include_units: bool = False):
        snapshot = await super().read_snapshot(include_units=include_units)
        if snapshot.units is not None or not self._has_unit_blocker(snapshot.blockers):
            return snapshot
        units = await self.state_api.get("/api/units")
        return snapshot.model_copy(update={"units": units})

    @staticmethod
    def _has_unit_blocker(blockers) -> bool:
        return any(
            isinstance(blocker, dict)
            and str(blocker.get("blocking_type", "")) == "ENDTURN_BLOCKING_UNITS"
            for blocker in blockers
        )
