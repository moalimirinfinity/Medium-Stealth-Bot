import logging
import sys
from typing import Any

import structlog

from medium_stealth_bot.redaction import redact_payload


def _redact_event(_, __, event_dict):
    return redact_payload(event_dict)


def _short_id(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    if len(value) <= 12:
        return value
    return f"{value[:6]}...{value[-4:]}"


def _drop_keys(event_dict: dict[str, Any], keys: tuple[str, ...]) -> None:
    for key in keys:
        event_dict.pop(key, None)


def _drop_default(event_dict: dict[str, Any], key: str, *, equals: Any) -> None:
    if event_dict.get(key) == equals:
        event_dict.pop(key, None)


def _humanize_event(_, __, event_dict):
    event_name = str(event_dict.get("event", ""))

    if event_name == "graphql_batch_executed":
        event_dict["event"] = "GraphQL request complete"
        _drop_default(event_dict, "status_code", equals=200)
    elif event_name == "operation_result":
        operation = event_dict.get("operation", "operation")
        result = event_dict.get("result", "unknown")
        event_dict["event"] = f"{operation}: {result}"
        _drop_keys(event_dict, ("operation", "result", "decision", "max_retries"))
        _drop_default(event_dict, "status_code", equals=200)
        _drop_default(event_dict, "attempts", equals=1)
        _drop_default(event_dict, "error_count", equals=0)
    elif event_name == "candidate_decision":
        reason = event_dict.get("reason", "decision")
        event_dict["event"] = f"candidate: {reason}"
        _drop_keys(event_dict, ("operation", "decision", "result", "reason"))
        if "score" in event_dict and isinstance(event_dict["score"], (int, float)):
            event_dict["score"] = round(float(event_dict["score"]), 3)
    elif event_name == "session_warmup_sleep":
        event_dict["event"] = "Session warmup delay"
        _drop_keys(
            event_dict,
            ("min_session_warmup_seconds", "max_session_warmup_seconds"),
        )
    elif event_name == "pre_follow_read_sleep":
        event_dict["event"] = "Pre-follow read delay"
        _drop_keys(event_dict, ("min_read_wait_seconds", "max_read_wait_seconds"))
    elif event_name == "action_gap_sleep":
        event_dict["event"] = "Action cooldown"
        _drop_keys(
            event_dict,
            (
                "min_gap_seconds",
                "max_gap_seconds",
                "action_type",
                "target_user_id",
            ),
        )
    elif event_name == "probe_complete":
        event_dict["event"] = "Probe complete"
    elif event_name == "daily_cycle_complete":
        event_dict["event"] = "Daily cycle complete"
        _drop_keys(
            event_dict,
            (
                "action_counts",
                "action_limits",
                "action_remaining",
                "source_candidate_counts",
                "source_follow_verified_counts",
                "decision_reason_counts",
                "decision_result_counts",
                "kpis",
                "client_metrics",
            ),
        )
    elif event_name == "cleanup_only_complete":
        event_dict["event"] = "Cleanup-only complete"
        _drop_keys(
            event_dict,
            (
                "action_counts",
                "action_limits",
                "action_remaining",
                "decision_reason_counts",
                "decision_result_counts",
                "kpis",
                "client_metrics",
            ),
        )
    elif event_name == "run_artifact_written":
        event_dict["event"] = "Run artifact written"
        _drop_keys(event_dict, ("operation", "decision", "result", "target_id"))
    elif event_name == "reconcile_complete":
        event_dict["event"] = "Reconcile complete"
    elif event_name == "risk_halt_triggered":
        event_dict["event"] = "Safety halt triggered"
    elif event_name == "operation_contract_registry_loaded":
        event_dict["event"] = "Contract registry loaded"
        _drop_default(event_dict, "strict", equals=True)

    if "dry_run" in event_dict:
        event_dict["mode"] = "dry_run" if event_dict["dry_run"] else "live"
        event_dict.pop("dry_run", None)

    _drop_keys(event_dict, ("command", "run_id", "tag_slug"))
    _drop_default(event_dict, "mode", equals="dry_run")
    _drop_default(event_dict, "mode", equals="stealth")

    _drop_default(event_dict, "stubbed", equals=False)
    _drop_default(event_dict, "operation_count", equals=1)
    _drop_default(event_dict, "error_count", equals=0)
    _drop_default(event_dict, "target_id", equals=None)
    _drop_default(event_dict, "target_user_id", equals=None)

    if "target_id" in event_dict:
        event_dict["target_id"] = _short_id(event_dict["target_id"])
    if "target_user_id" in event_dict:
        event_dict["target_user_id"] = _short_id(event_dict["target_user_id"])
    if "latency_ms" in event_dict and isinstance(event_dict["latency_ms"], (int, float)):
        event_dict["latency_ms"] = round(float(event_dict["latency_ms"]), 1)
    if "delay_seconds" in event_dict and isinstance(event_dict["delay_seconds"], (int, float)):
        event_dict["delay_seconds"] = round(float(event_dict["delay_seconds"]), 2)

    return event_dict


def configure_logging(level: str = "INFO", log_format: str = "pretty") -> None:
    resolved_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=resolved_level, format="%(message)s", stream=sys.stdout)

    selected_format = log_format.strip().lower()
    if selected_format not in {"pretty", "json"}:
        selected_format = "pretty"

    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        _redact_event,
        structlog.processors.add_log_level,
    ]

    if selected_format == "json":
        processors.extend(
            [
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(sort_keys=True),
            ]
        )
    else:
        processors.extend(
            [
                _humanize_event,
                structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.dev.ConsoleRenderer(
                    sort_keys=False,
                    pad_level=False,
                    pad_event_to=0,
                ),
            ]
        )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(resolved_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
