"""Shared Runtime boundary errors without composition dependencies."""


class InjectedCrashBoundary(RuntimeError):
    """Raised when a deterministic crash checkpoint is triggered."""


class FatalTickPersistenceError(RuntimeError):
    """Raised when both a Tick and its SYSTEM_ERROR audit fail to persist."""


class PlannerProviderBudgetExceeded(RuntimeError):
    """Raised before a Provider call when its durable budget cannot be reserved."""
