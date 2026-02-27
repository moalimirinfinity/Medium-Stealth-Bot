from medium_stealth_bot.redaction import REDACTED, redact_payload


def test_redact_payload_masks_sensitive_keys() -> None:
    payload = {
        "medium_session": "sid=abc; uid=def",
        "nested": {"authorization": "Bearer test-token"},
        "safe": "ok",
    }
    sanitized = redact_payload(payload)
    assert sanitized["medium_session"] == REDACTED
    assert sanitized["nested"]["authorization"] == REDACTED
    assert sanitized["safe"] == "ok"


def test_redact_payload_masks_cookie_value_patterns() -> None:
    payload = {"message": "request failed sid=abc123 xsrf=xyz"}
    sanitized = redact_payload(payload)
    assert "abc123" not in sanitized["message"]
    assert "xyz" not in sanitized["message"]


def test_redact_payload_preserves_numeric_session_named_metrics() -> None:
    payload = {"kpis": {"timing_session_warmup_seconds_total": 1.25}}
    sanitized = redact_payload(payload)
    assert sanitized["kpis"]["timing_session_warmup_seconds_total"] == 1.25
