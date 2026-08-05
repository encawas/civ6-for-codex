from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .models import ActionResult, MutationDeliveryStatus

_MAX_DIAGNOSTIC_CHARS = 1024
_CONNECTION_FAILURE = re.compile(
    r"(?:cannot connect to civ(?:ilization)?\s*6|connection (?:refused|closed)|"
    r"client is not connected)",
    re.IGNORECASE,
)
_RESULT_ERROR = re.compile(r"^(?:Error:\s*|ERR:)([A-Z0-9_]+)(?:\||:|\b)")
_END_TURN_REJECTION = (
    re.compile(r"^Empty reflections\s*:", re.IGNORECASE),
    re.compile(r"^Cannot end turn\b", re.IGNORECASE),
    re.compile(r"^End turn blocked\b", re.IGNORECASE),
    re.compile(r"^Turn paused\b", re.IGNORECASE),
)

_SUCCESS_GRAMMARS: dict[str, tuple[re.Pattern[str], ...]] = {
    "set_research": (
        re.compile(
            r"^(?:RESEARCHING(?:_GAMECORE)?|PROGRESSING(?:_GC)?)\b",
            re.IGNORECASE,
        ),
    ),
    "set_city_production": (re.compile(r"^PRODUCING\b", re.IGNORECASE),),
    "send_envoy": (re.compile(r"^(?:OK:)?ENVOY_SENT\b", re.IGNORECASE),),
    "unit_action": (
        re.compile(
            r"^(?:MOVING_TO|CAPTURE_MOVE|FOUNDED|FORTIFIED|"
            r"ALREADY_FORTIFIED|SLEEPING|SKIPPED)\b",
            re.IGNORECASE,
        ),
    ),
    "end_turn": (
        re.compile(r"^Turn\s+\d+\s*->\s*\d+\b", re.IGNORECASE),
        re.compile(r"^GAME OVER\b", re.IGNORECASE),
    ),
}

_SAFE_REJECTION_CODES: dict[str, frozenset[str]] = {
    "set_research": frozenset(
        {
            "TECH_NOT_FOUND",
            "TECHNOLOGY_NOT_FOUND",
            "CIVIC_NOT_FOUND",
            "ALREADY_COMPLETED",
        }
    ),
    "set_city_production": frozenset(
        {
            "CITY_NOT_FOUND",
            "MISSING_COORDS",
            "ITEM_NOT_FOUND",
            "CANNOT_PRODUCE",
            "TRADER_CAP",
        }
    ),
    "send_envoy": frozenset(
        {
            "INVALID_PLAYER",
            "NO_ENVOYS",
            "CANNOT_SEND",
        }
    ),
    "unit_action": frozenset(
        {
            "UNIT_NOT_FOUND",
            "INVALID_ACTION",
            "MISSING_COORDS",
            "NO_MOVES",
            "CANNOT_MOVE",
            "CANNOT_FOUND",
            "STACKING_CONFLICT",
            "CANNOT_FORTIFY",
            "FULL_HP",
            "CANNOT_HEAL",
        }
    ),
    "end_turn": frozenset(),
}


class DeliveryCategory(StrEnum):
    ACKNOWLEDGED = "acknowledged"
    LOCAL_REJECTED = "local_rejected"
    TOOL_BUSINESS_REJECTED = "tool_business_rejected"
    GAME_UNAVAILABLE = "game_unavailable"
    TOOL_EXECUTION_ERROR = "tool_execution_error"
    MCP_TRANSPORT_ERROR = "mcp_transport_error"
    MCP_CALL_TIMEOUT = "mcp_call_timeout"
    MALFORMED_RESULT = "malformed_result"


@dataclass(frozen=True, slots=True)
class McpToolEnvelope:
    tool_name: str
    structured_content: Any | None
    text_content: str
    is_error: bool
    raw_result_hash: str
    diagnostic_excerpt: str

    @classmethod
    def from_sdk_result(cls, tool_name: str, result: Any) -> "McpToolEnvelope":
        structured = getattr(result, "structuredContent", None)
        if structured is None:
            structured = getattr(result, "structured_content", None)
        text = _extract_text(getattr(result, "content", None))
        is_error = bool(
            getattr(result, "isError", False) or getattr(result, "is_error", False)
        )
        return cls._build(tool_name, structured, text, is_error)

    @classmethod
    def from_legacy_payload(cls, tool_name: str, raw: Any) -> "McpToolEnvelope":
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, str):
            return cls._build(tool_name, None, raw, False)
        return cls._build(tool_name, raw, "", False)

    @classmethod
    def _build(
        cls,
        tool_name: str,
        structured_content: Any | None,
        text_content: str,
        is_error: bool,
    ) -> "McpToolEnvelope":
        evidence = {
            "tool_name": tool_name,
            "structured_content": structured_content,
            "text_content": text_content,
            "is_error": is_error,
        }
        serialized = _stable_serialize(evidence)
        excerpt_source = text_content or serialized
        return cls(
            tool_name=tool_name,
            structured_content=structured_content,
            text_content=text_content,
            is_error=is_error,
            raw_result_hash=hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
            diagnostic_excerpt=_truncate(excerpt_source),
        )


class MutationToolAdapter:
    def interpret(self, envelope: McpToolEnvelope) -> ActionResult:
        result_text, payload, conflict = _result_evidence(envelope)
        base_details: dict[str, Any] = {
            "tool_name": envelope.tool_name,
            "raw_result_hash": envelope.raw_result_hash,
            "diagnostic_excerpt": envelope.diagnostic_excerpt,
        }
        if payload is not None:
            base_details["response_payload"] = payload

        if conflict:
            return self._result(
                MutationDeliveryStatus.UNKNOWN,
                DeliveryCategory.MALFORMED_RESULT,
                "structured and textual MCP results conflict",
                base_details,
            )

        payload_diagnostics = "\n".join(
            str(payload.get(key, ""))
            for key in ("error", "message")
            if payload is not None and payload.get(key)
        )
        combined = "\n".join(
            value
            for value in (result_text, envelope.text_content, payload_diagnostics)
            if isinstance(value, str) and value.strip()
        )
        if _CONNECTION_FAILURE.search(combined):
            return self._result(
                MutationDeliveryStatus.PROVEN_NOT_SENT,
                DeliveryCategory.GAME_UNAVAILABLE,
                _truncate(result_text or envelope.text_content or "game unavailable"),
                base_details,
            )

        rejection_code = _rejection_code(result_text)
        if rejection_code is not None and rejection_code in _SAFE_REJECTION_CODES.get(
            envelope.tool_name, ()
        ):
            base_details["code"] = rejection_code
            return self._result(
                MutationDeliveryStatus.EXPLICITLY_REJECTED,
                DeliveryCategory.TOOL_BUSINESS_REJECTED,
                _truncate(result_text or "tool rejected mutation"),
                base_details,
                blocked=True,
            )

        if (
            envelope.tool_name == "end_turn"
            and result_text
            and any(grammar.search(result_text) for grammar in _END_TURN_REJECTION)
        ):
            return self._result(
                MutationDeliveryStatus.EXPLICITLY_REJECTED,
                DeliveryCategory.TOOL_BUSINESS_REJECTED,
                _truncate(result_text),
                base_details,
                blocked=True,
            )

        if envelope.is_error:
            return self._result(
                MutationDeliveryStatus.UNKNOWN,
                DeliveryCategory.TOOL_EXECUTION_ERROR,
                _truncate(
                    result_text
                    or envelope.text_content
                    or "MCP tool returned an internal error"
                ),
                base_details,
            )

        if result_text and any(
            grammar.search(result_text)
            for grammar in _SUCCESS_GRAMMARS.get(envelope.tool_name, ())
        ):
            return self._result(
                MutationDeliveryStatus.ACKNOWLEDGED,
                DeliveryCategory.ACKNOWLEDGED,
                _truncate(result_text),
                base_details,
                success=True,
            )

        return self._result(
            MutationDeliveryStatus.UNKNOWN,
            DeliveryCategory.MALFORMED_RESULT,
            _truncate(
                result_text
                or envelope.text_content
                or "MCP tool returned no recognizable mutation result"
            ),
            base_details,
        )

    @staticmethod
    def local_failure(
        *,
        tool_name: str,
        category: DeliveryCategory,
        message: str,
        exception_type: str | None = None,
    ) -> ActionResult:
        details: dict[str, Any] = {
            "tool_name": tool_name,
            "category": category.value,
            "diagnostic_excerpt": _truncate(message),
        }
        if exception_type:
            details["exception_type"] = exception_type
        status = (
            MutationDeliveryStatus.PROVEN_NOT_SENT
            if category
            in {
                DeliveryCategory.LOCAL_REJECTED,
                DeliveryCategory.GAME_UNAVAILABLE,
            }
            else MutationDeliveryStatus.UNKNOWN
        )
        return ActionResult(
            success=False,
            message=_truncate(message),
            details=details,
            delivery_status=status,
        )

    @staticmethod
    def _result(
        status: MutationDeliveryStatus,
        category: DeliveryCategory,
        message: str,
        details: dict[str, Any],
        *,
        success: bool = False,
        blocked: bool = False,
    ) -> ActionResult:
        return ActionResult(
            success=success,
            blocked=blocked,
            message=message,
            details={**details, "category": category.value},
            delivery_status=status,
        )


def _result_evidence(
    envelope: McpToolEnvelope,
) -> tuple[str | None, dict[str, Any] | None, bool]:
    structured_result, structured_payload = _payload_result(envelope.structured_content)
    text_payload: Any | None = None
    text_result: str | None = None
    text = envelope.text_content.strip()
    if text:
        try:
            text_payload = json.loads(text)
        except json.JSONDecodeError:
            text_result = text
        else:
            text_result, _ = _payload_result(text_payload)

    conflict = bool(
        structured_result
        and text_result
        and _normalized_result(structured_result) != _normalized_result(text_result)
    )
    result = structured_result or text_result
    payload = structured_payload
    if payload is None and isinstance(text_payload, dict):
        payload = _compact_payload(text_payload)
    return result, payload, conflict


def _payload_result(raw: Any) -> tuple[str | None, dict[str, Any] | None]:
    if not isinstance(raw, dict):
        return None, None
    result = raw.get("result")
    if not isinstance(result, str) or not result.strip():
        return None, _compact_payload(raw)
    return result.strip(), _compact_payload(raw)


def _compact_payload(raw: dict[str, Any]) -> dict[str, Any]:
    allowed = {"result", "status", "code", "success", "blocked", "message", "error"}
    return {
        key: _truncate(value) if isinstance(value, str) else value
        for key, value in raw.items()
        if key in allowed and isinstance(value, (str, int, float, bool, type(None)))
    }


def _rejection_code(result_text: str | None) -> str | None:
    if not result_text:
        return None
    match = _RESULT_ERROR.search(result_text.strip())
    return None if match is None else match.group(1).upper()


def _normalized_result(value: str) -> str:
    return " ".join(value.split())


def _extract_text(content: Any) -> str:
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        text = getattr(block, "text", None)
        if text is not None:
            parts.append(str(text))
    return "\n".join(parts).strip()


def _stable_serialize(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=lambda item: f"<{type(item).__name__}>",
        )
    except (TypeError, ValueError, RecursionError):
        return f"<unserializable:{type(value).__name__}>"


def _truncate(value: str, limit: int = _MAX_DIAGNOSTIC_CHARS) -> str:
    clean = value.replace("\x00", "").strip()
    if len(clean) <= limit:
        return clean
    return clean[: limit - 16] + "...[truncated]"
