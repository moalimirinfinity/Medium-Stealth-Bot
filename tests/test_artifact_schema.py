from medium_stealth_bot.artifact_schema import validate_artifact_payload


def test_validate_artifact_payload_v1() -> None:
    payload = {
        "schema_version": 1,
        "run_id": "run_123",
        "command": "run",
        "tag_slug": "programming",
        "dry_run": True,
        "status": "success",
        "health": "healthy",
        "started_at": "2026-02-24T00:00:00+00:00",
        "ended_at": "2026-02-24T00:00:01+00:00",
        "duration_ms": 1000,
        "summary": {},
        "action_counts": {},
        "result_counts": {},
        "reason_counts": {},
        "kpis": {},
        "client_metrics": {},
        "error": None,
    }
    ok, issues = validate_artifact_payload(payload)
    assert ok is True
    assert issues == []


def test_validate_artifact_payload_unsupported_schema() -> None:
    ok, issues = validate_artifact_payload({"schema_version": 9})
    assert ok is False
    assert issues and issues[0].startswith("unsupported_schema_version")
