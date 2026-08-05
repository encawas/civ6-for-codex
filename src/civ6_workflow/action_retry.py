"""Canonical task resolution for terminal failed action attempts."""

from __future__ import annotations

from dataclasses import dataclass

from .domain import ActionAttempt, AttemptStatus, RetryClassification
from .models import MutationDeliveryStatus, TaskStatus


@dataclass(frozen=True, slots=True)
class FailedAttemptResolution:
    task_status: TaskStatus
    retry_count: int
    reason: str


def resolve_failed_attempt(
    attempt: ActionAttempt,
    *,
    retry_count: int,
    max_retries: int,
    failure_reason: str | None = None,
) -> FailedAttemptResolution:
    """Resolve retry eligibility from durable evidence and the task retry budget.

    ``attempt_number`` is the immutable ordinal of every external delivery attempt.
    ``retry_count`` counts retry-eligible failures charged to ``max_retries``;
    it alone authorizes whether another attempt may be created.
    """

    if attempt.status is not AttemptStatus.FAILED:
        raise ValueError("failed-attempt resolution requires FAILED status")
    if retry_count < 0 or max_retries < 0:
        raise ValueError("retry counters must be non-negative")

    transport_delivery_status = (
        None
        if attempt.transport_result is None
        else attempt.transport_result.get("delivery_status")
    )
    tool_delivery_status = (
        None
        if attempt.tool_result is None
        else attempt.tool_result.get("delivery_status")
    )
    proven_not_committed = bool(
        {
            MutationDeliveryStatus.PROVEN_NOT_SENT.value,
            MutationDeliveryStatus.EXPLICITLY_REJECTED.value,
        }
        & {transport_delivery_status, tool_delivery_status}
    )
    retry_eligible = (
        attempt.retry_classification is RetryClassification.SAFE_IF_PROVEN_NOT_SENT
        and proven_not_committed
    )
    if not retry_eligible:
        return FailedAttemptResolution(
            task_status=TaskStatus.FAILED,
            retry_count=retry_count,
            reason=(
                failure_reason
                or "mutation was rejected, conflicting, or cannot be safely retried"
            ),
        )

    if retry_count >= max_retries:
        return FailedAttemptResolution(
            task_status=TaskStatus.ESCALATED,
            retry_count=retry_count,
            reason="safe retry limit reached",
        )
    next_retry_count = retry_count + 1
    return FailedAttemptResolution(
        task_status=TaskStatus.READY,
        retry_count=next_retry_count,
        reason="attempt is proven not committed",
    )
