from __future__ import annotations

import re
from typing import Any

REDACTED = "[REDACTED]"
SENSITIVE_KEY_TOKENS = (
    "session",
    "cookie",
    "csrf",
    "xsrf",
    "authorization",
    "auth",
    "token",
    "secret",
    "password",
)

COOKIE_VALUE_PATTERN = re.compile(r"\b(sid|uid|xsrf|x-xsrf-token)=([^;\s]+)", re.IGNORECASE)
BEARER_PATTERN = re.compile(r"\b(bearer\s+)([A-Za-z0-9._~+\-/]+=*)", re.IGNORECASE)


def redact_string(value: str) -> str:
    masked = COOKIE_VALUE_PATTERN.sub(lambda m: f"{m.group(1)}={REDACTED}", value)
    masked = BEARER_PATTERN.sub(lambda m: f"{m.group(1)}{REDACTED}", masked)
    return masked


def _is_sensitive_key(key: str | None) -> bool:
    if not key:
        return False
    lowered = key.lower()
    return any(token in lowered for token in SENSITIVE_KEY_TOKENS)


def redact_payload(value: Any, *, key: str | None = None, depth: int = 0) -> Any:
    if depth > 6:
        return value

    if _is_sensitive_key(key):
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return REDACTED
        if isinstance(value, str):
            return REDACTED
        return REDACTED

    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for item_key, item_value in value.items():
            out[str(item_key)] = redact_payload(item_value, key=str(item_key), depth=depth + 1)
        return out

    if isinstance(value, list):
        return [redact_payload(item, key=key, depth=depth + 1) for item in value]

    if isinstance(value, str):
        return redact_string(value)

    return value
