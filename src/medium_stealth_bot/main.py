import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
import typer
from rich.console import Console
from rich.table import Table
from structlog import contextvars as structlog_contextvars

from medium_stealth_bot import __version__
from medium_stealth_bot.artifact_schema import validate_artifact_payload
from medium_stealth_bot.auth import (
    import_session_material_from_cookie_header,
    interactive_auth,
    upsert_env_file,
)
from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.contracts import ContractValidationReport, validate_contract_registry
from medium_stealth_bot.database import Database
from medium_stealth_bot.deployment import validate_production_profile
from medium_stealth_bot.logging import configure_logging
from medium_stealth_bot.logic import DailyRunner
from medium_stealth_bot.models import AuthSessionMaterial, DailyRunOutcome, ProbeSnapshot, ReconcileOutcome
from medium_stealth_bot.observability import new_run_id, read_latest_run_artifact, write_run_artifact
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.safety import RiskHaltError
from medium_stealth_bot.settings import AppSettings

app = typer.Typer(
    help="Local-first Medium automation CLI with guided workflows, safety guardrails, and run diagnostics.",
    no_args_is_help=True,
)
artifacts_app = typer.Typer(help="Inspect and validate run artifact payloads.")
app.add_typer(artifacts_app, name="artifacts")
console = Console()

_NOTICE_STYLES = {
    "info": "cyan",
    "success": "green",
    "warning": "yellow",
    "error": "red",
}
_NOTICE_PREFIX = {
    "info": "INFO",
    "success": "SUCCESS",
    "warning": "WARNING",
    "error": "ERROR",
}


def _print_notice(message: str, *, level: str = "info") -> None:
    style = _NOTICE_STYLES.get(level, "cyan")
    prefix = _NOTICE_PREFIX.get(level, "INFO")
    console.print(f"{prefix}: {message}", style=style)


def _format_metric_key(key: str) -> str:
    return key.replace("_", " ").replace("-", " ").strip().title()


def _resolve_value_header(data: dict[str, Any]) -> str:
    if data and all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in data.values()):
        return "Count"
    return "Value"


def _render_mapping_table(
    *,
    title: str,
    data: dict[str, Any],
    key_column: str = "Key",
    value_column: str | None = None,
) -> None:
    if not data:
        return

    table = Table(title=title)
    table.add_column(key_column)
    table.add_column(value_column or _resolve_value_header(data))
    for key, value in sorted(data.items(), key=lambda item: str(item[0])):
        if isinstance(value, dict):
            rendered_value = ", ".join(
                f"{nested_key}={nested_value}"
                for nested_key, nested_value in sorted(value.items(), key=lambda item: str(item[0]))
            )
            table.add_row(str(key), rendered_value or "{}")
            continue
        table.add_row(str(key), str(value))
    console.print(table)


def _mask(value: str | None, keep: int = 4) -> str:
    if not value:
        return "-"
    if len(value) <= keep * 2:
        return "*" * len(value)
    return f"{value[:keep]}...{value[-keep:]}"


def _quote_env_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _upsert_env_values(env_path: Path, updates: dict[str, str]) -> None:
    env_path.parent.mkdir(parents=True, exist_ok=True)
    existing_lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []

    written_keys: set[str] = set()
    output_lines: list[str] = []
    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in line:
            output_lines.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key not in updates:
            output_lines.append(line)
            continue
        output_lines.append(f"{key}={_quote_env_value(updates[key])}")
        written_keys.add(key)

    for key, value in updates.items():
        if key in written_keys:
            continue
        output_lines.append(f"{key}={_quote_env_value(value)}")

    env_path.write_text("\n".join(output_lines).rstrip() + "\n", encoding="utf-8")


def _bootstrap_settings(*, env_path: Path | None = None) -> AppSettings:
    settings = AppSettings(_env_file=env_path) if env_path else AppSettings()
    settings.ensure_directories()
    configure_logging(settings.log_level, settings.log_format)
    return settings


def _build_runner(settings: AppSettings) -> tuple[Database, ActionRepository]:
    database = Database(settings.db_path)
    database.initialize()
    repository = ActionRepository(database)
    return database, repository


def _require_session(settings: AppSettings, *, guidance: str = "uv run bot auth") -> None:
    if settings.has_session:
        return
    _print_notice(f"No MEDIUM_SESSION found. Run `{guidance}` first.", level="error")
    raise typer.Exit(code=1)


def _risk_halt_is_hard(settings: AppSettings) -> bool:
    return settings.risk_halt_mode == "hard"


def _risk_halt_exit_code(settings: AppSettings) -> int:
    return 2 if _risk_halt_is_hard(settings) else 0


def _risk_halt_notice_level(settings: AppSettings) -> str:
    return "error" if _risk_halt_is_hard(settings) else "warning"


def _risk_halt_summary(settings: AppSettings) -> str:
    if _risk_halt_is_hard(settings):
        return "Run halted by safety guardrails."
    return "Run paused by safety guardrails (soft mode)."


def _render_auth(material: AuthSessionMaterial) -> None:
    table = Table(title="Auth Capture")
    table.add_column("Key")
    table.add_column("Value")
    table.add_row("MEDIUM_SESSION", _mask(material.medium_session, keep=8))
    table.add_row("MEDIUM_CSRF", _mask(material.medium_csrf))
    table.add_row("MEDIUM_USER_REF", material.medium_user_ref or "-")
    table.add_row("Cookie Names", ", ".join(material.cookie_names))
    console.print(table)


def _render_probe(snapshot: ProbeSnapshot) -> None:
    table = Table(title=f"Probe Results ({snapshot.tag_slug})")
    table.add_column("Task")
    table.add_column("Operation")
    table.add_column("Status")
    table.add_column("Errors")
    table.add_column("Stubbed")

    for task_name, result in snapshot.results.items():
        table.add_row(
            task_name,
            result.operation_name,
            str(result.status_code),
            str(len(result.errors)),
            "yes" if result.stubbed else "no",
        )
    console.print(table)
    _print_notice(f"Probe duration: {snapshot.duration_ms}ms", level="info")


def _render_daily_run(outcome: DailyRunOutcome) -> None:
    mode_label = "dry-run" if outcome.dry_run else "live"

    if outcome.budget_exhausted:
        _print_notice(
            f"Daily budget exhausted (UTC day): {outcome.actions_today}/{outcome.max_actions_per_day}.",
            level="warning",
        )
        return
    _print_notice(
        f"Daily budget check passed (UTC day): {outcome.actions_today}/{outcome.max_actions_per_day} (mode={mode_label}).",
        level="success",
    )
    summary_table = Table(title="Daily Cycle Summary")
    summary_table.add_column("Metric")
    summary_table.add_column("Value")
    summary_table.add_row("Mode", mode_label)
    if outcome.session_passes > 1 or outcome.session_stop_reason:
        summary_table.add_row("Session Passes", str(outcome.session_passes))
        summary_table.add_row("Session Elapsed (s)", str(round(outcome.session_elapsed_seconds, 3)))
        summary_table.add_row("Session Stop Reason", outcome.session_stop_reason or "-")
        summary_table.add_row("Session Target Follows", str(outcome.session_target_follow_attempts or "-"))
        session_min_follows = outcome.kpis.get("session_target_follow_attempts_min")
        summary_table.add_row(
            "Session Min Follows",
            str(int(session_min_follows)) if isinstance(session_min_follows, (int, float)) else "-",
        )
        summary_table.add_row("Session Target Duration (m)", str(outcome.session_target_duration_minutes or "-"))
    summary_table.add_row(
        "Candidates Considered / Eligible",
        f"{outcome.considered_candidates} / {outcome.eligible_candidates}",
    )
    summary_table.add_row(
        "Follow Attempted / Verified",
        f"{outcome.follow_actions_attempted} / {outcome.follow_actions_verified}",
    )
    summary_table.add_row(
        "Clap Attempted / Verified",
        f"{outcome.clap_actions_attempted} / {outcome.clap_actions_verified}",
    )
    summary_table.add_row(
        "Cleanup Attempted / Verified",
        f"{outcome.cleanup_actions_attempted} / {outcome.cleanup_actions_verified}",
    )
    console.print(summary_table)

    if outcome.decision_result_counts:
        result_table = Table(title="Decision Result Counts")
        result_table.add_column("Result")
        result_table.add_column("Count")
        for result, count in sorted(outcome.decision_result_counts.items()):
            result_table.add_row(result, str(count))
        console.print(result_table)

    if outcome.action_counts_today:
        budget_table = Table(title="Per-Action Daily Budget")
        budget_table.add_column("Action")
        budget_table.add_column("Used")
        budget_table.add_column("Limit")
        budget_table.add_column("Remaining")
        for action_name, used in sorted(outcome.action_counts_today.items()):
            limit = outcome.action_limits_per_day.get(action_name, 0)
            remaining = outcome.action_remaining_per_day.get(action_name, 0)
            budget_table.add_row(action_name, str(used), str(limit), str(remaining))
        console.print(budget_table)

    if outcome.kpis:
        kpi_table = Table(title="KPI Summary")
        kpi_table.add_column("KPI")
        kpi_table.add_column("Value")
        for key, value in sorted(outcome.kpis.items()):
            kpi_table.add_row(_format_metric_key(key), str(value))
        console.print(kpi_table)

    if outcome.client_metrics:
        metrics = dict(outcome.client_metrics)
        status_counts = metrics.pop("status_counts", None)
        metrics_table = Table(title="Client Metrics")
        metrics_table.add_column("Metric")
        metrics_table.add_column("Value")
        for key, value in sorted(metrics.items()):
            metrics_table.add_row(_format_metric_key(key), str(value))
        console.print(metrics_table)
        if isinstance(status_counts, dict) and status_counts:
            _render_mapping_table(
                title="Client HTTP Status Counts",
                data={str(key): value for key, value in status_counts.items()},
                key_column="Status",
                value_column="Count",
            )

    if outcome.source_candidate_counts:
        source_table = Table(title="Source Candidate Counts")
        source_table.add_column("Source")
        source_table.add_column("Candidates")
        source_table.add_column("Verified Follows")
        for source, count in sorted(outcome.source_candidate_counts.items()):
            source_table.add_row(
                source,
                str(count),
                str(outcome.source_follow_verified_counts.get(source, 0)),
            )
        console.print(source_table)

    if outcome.decision_log:
        table = Table(title="Decision Log (sample)")
        table.add_column("#")
        table.add_column("Decision")
        for idx, item in enumerate(outcome.decision_log[:12], start=1):
            table.add_row(str(idx), item)
        console.print(table)

    if outcome.probe:
        _render_probe(outcome.probe)


def _render_reconcile_outcome(outcome: ReconcileOutcome) -> None:
    table = Table(title="Reconcile Outcome")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Mode", "dry-run" if outcome.dry_run else "live")
    table.add_row("Scanned Users", str(outcome.scanned_users))
    table.add_row("Updated Users", str(outcome.updated_users))
    table.add_row("Following", str(outcome.following_count))
    table.add_row("Not Following", str(outcome.not_following_count))
    table.add_row("Unknown", str(outcome.unknown_count))
    console.print(table)
    if outcome.decision_log:
        log_table = Table(title="Reconcile Decisions (sample)")
        log_table.add_column("#")
        log_table.add_column("Decision")
        for idx, item in enumerate(outcome.decision_log[:20], start=1):
            log_table.add_row(str(idx), item)
        console.print(log_table)


def _render_contract_validation(report: ContractValidationReport) -> None:
    summary = Table(title="Contract Registry Validation")
    summary.add_column("Metric")
    summary.add_column("Value")
    summary.add_row("Registry Path", str(report.registry_path))
    summary.add_row("Strict Mode", "true" if report.strict else "false")
    summary.add_row("Registry Operations", str(len(report.registry_operation_names)))
    summary.add_row("Implemented Operations", str(len(report.implemented_operation_names)))
    summary.add_row("Checks Passed", str(report.passed_count))
    summary.add_row("Checks Failed", str(report.failed_count))
    summary.add_row("Parity Missing In Code", str(len(report.missing_in_code)))
    summary.add_row("Parity Extra In Code", str(len(report.extra_in_code)))
    if report.execute_reads:
        summary.add_row("Live Reads Executed", str(report.live_executed_count))
        summary.add_row("Live Reads Passed", str(report.live_passed_count))
        summary.add_row("Live Reads Failed", str(report.live_failed_count))
        summary.add_row("Live Reads Skipped", str(report.live_skipped_count))
    summary.add_row("Overall Status", "PASS" if report.ok else "FAIL")
    console.print(summary)

    if report.load_error:
        _print_notice(f"Registry load error: {report.load_error}", level="error")
        return

    if report.missing_in_code:
        _print_notice("Missing operations in code: " + ", ".join(report.missing_in_code), level="error")
    if report.extra_in_code:
        _print_notice("Extra operations in code: " + ", ".join(report.extra_in_code), level="warning")

    failed = [item for item in report.checks if not item.ok]
    if failed:
        table = Table(title="Failed Operation Contract Checks")
        table.add_column("Operation")
        table.add_column("Issues")
        for item in failed:
            table.add_row(item.operation_name, ", ".join(item.issues))
        console.print(table)

    live_failed = [item for item in report.live_read_checks if item.status == "failed"]
    if live_failed:
        table = Table(title="Failed Live Read Checks")
        table.add_column("Operation")
        table.add_column("Detail")
        for item in live_failed:
            table.add_row(item.operation_name, item.detail)
        console.print(table)


def _render_risk_halt(exc: RiskHaltError, *, settings: AppSettings) -> None:
    _print_notice(_risk_halt_summary(settings), level=_risk_halt_notice_level(settings))
    if not _risk_halt_is_hard(settings):
        _print_notice(
            "Soft mode keeps the command exit status successful while pausing risky actions.",
            level="warning",
        )
    table = Table(title="Safety Halt")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Reason", exc.reason)
    table.add_row("Task", exc.task_name)
    table.add_row("Detail", exc.detail)
    table.add_row("Consecutive Failures", str(exc.consecutive_failures))
    console.print(table)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _derive_health(status: str, outcome: DailyRunOutcome | None) -> str:
    if status == "failed":
        return "failed"
    if status == "halted":
        return "degraded"
    if outcome is None:
        return "unknown"
    if outcome.budget_exhausted:
        return "degraded"
    if outcome.decision_result_counts.get("failed", 0) > 0:
        return "degraded"
    return "healthy"


def _build_run_artifact_payload(
    *,
    run_id: str,
    command: str = "run",
    started_at: datetime,
    ended_at: datetime,
    tag_slug: str,
    dry_run: bool,
    status: str,
    outcome: DailyRunOutcome | None,
    error: dict[str, str] | None,
) -> dict:
    duration_ms = int((ended_at - started_at).total_seconds() * 1000)
    payload: dict = {
        "schema_version": 1,
        "run_id": run_id,
        "command": command,
        "tag_slug": tag_slug,
        "dry_run": dry_run,
        "status": status,
        "health": _derive_health(status, outcome),
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_ms": duration_ms,
        "error": error,
        "summary": {},
        "action_counts": {},
        "result_counts": {},
        "reason_counts": {},
        "kpis": {},
        "client_metrics": {},
        "source_candidate_counts": {},
        "source_follow_verified_counts": {},
    }
    if outcome is None:
        return payload

    payload["summary"] = {
        "budget_exhausted": outcome.budget_exhausted,
        "actions_today": outcome.actions_today,
        "max_actions_per_day": outcome.max_actions_per_day,
        "session_passes": outcome.session_passes,
        "session_elapsed_seconds": outcome.session_elapsed_seconds,
        "session_stop_reason": outcome.session_stop_reason,
        "session_target_follow_attempts": outcome.session_target_follow_attempts,
        "session_target_duration_minutes": outcome.session_target_duration_minutes,
        "considered_candidates": outcome.considered_candidates,
        "eligible_candidates": outcome.eligible_candidates,
        "follow_actions_attempted": outcome.follow_actions_attempted,
        "follow_actions_verified": outcome.follow_actions_verified,
        "clap_actions_attempted": outcome.clap_actions_attempted,
        "clap_actions_verified": outcome.clap_actions_verified,
        "cleanup_actions_attempted": outcome.cleanup_actions_attempted,
        "cleanup_actions_verified": outcome.cleanup_actions_verified,
    }
    payload["action_counts"] = outcome.action_counts_today
    payload["result_counts"] = outcome.decision_result_counts
    payload["reason_counts"] = outcome.decision_reason_counts
    payload["kpis"] = outcome.kpis
    payload["client_metrics"] = outcome.client_metrics
    payload["source_candidate_counts"] = outcome.source_candidate_counts
    payload["source_follow_verified_counts"] = outcome.source_follow_verified_counts
    payload["decision_log_sample"] = outcome.decision_log[:40]
    if outcome.probe:
        payload["probe"] = {
            "tag_slug": outcome.probe.tag_slug,
            "duration_ms": outcome.probe.duration_ms,
            "task_count": len(outcome.probe.results),
            "error_tasks": [
                task_name
                for task_name, result in outcome.probe.results.items()
                if result.status_code != 200 or result.has_errors
            ],
        }
    return payload


def _render_status(artifact: dict, *, artifact_path: Path) -> None:
    dry_run = artifact.get("dry_run")
    mode = "dry-run" if dry_run is True else "live" if dry_run is False else "-"
    table = Table(title="Last Run Health")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Run ID", str(artifact.get("run_id", "-")))
    table.add_row("Status", str(artifact.get("status", "-")))
    table.add_row("Health", str(artifact.get("health", "-")))
    table.add_row("Tag", str(artifact.get("tag_slug", "-")))
    table.add_row("Mode", mode)
    table.add_row("Started", str(artifact.get("started_at", "-")))
    table.add_row("Ended", str(artifact.get("ended_at", "-")))
    table.add_row("Duration (ms)", str(artifact.get("duration_ms", "-")))
    table.add_row("Artifact", str(artifact_path))
    console.print(table)

    summary = artifact.get("summary")
    if isinstance(summary, dict) and summary:
        summary_table = Table(title="Run Summary")
        summary_table.add_column("Metric")
        summary_table.add_column("Value")
        for key in (
            "budget_exhausted",
            "actions_today",
            "max_actions_per_day",
            "session_passes",
            "session_elapsed_seconds",
            "session_stop_reason",
            "session_target_follow_attempts",
            "session_target_duration_minutes",
            "considered_candidates",
            "eligible_candidates",
            "follow_actions_attempted",
            "follow_actions_verified",
            "clap_actions_attempted",
            "clap_actions_verified",
            "cleanup_actions_attempted",
            "cleanup_actions_verified",
        ):
            if key in summary:
                summary_table.add_row(_format_metric_key(key), str(summary[key]))
        console.print(summary_table)

    for title, key in (
        ("Action Counts", "action_counts"),
        ("Decision Result Counts", "result_counts"),
        ("Decision Reason Counts", "reason_counts"),
        ("KPI Summary", "kpis"),
        ("Client Metrics", "client_metrics"),
        ("Source Candidate Counts", "source_candidate_counts"),
        ("Source Verified Follow Counts", "source_follow_verified_counts"),
    ):
        data = artifact.get(key)
        if isinstance(data, dict) and data:
            if key == "kpis":
                _render_mapping_table(
                    title=title,
                    data={_format_metric_key(str(item_key)): item_value for item_key, item_value in data.items()},
                    key_column="KPI",
                    value_column="Value",
                )
                continue
            if key == "client_metrics":
                metrics = dict(data)
                status_counts = metrics.pop("status_counts", None)
                _render_mapping_table(
                    title=title,
                    data={_format_metric_key(str(item_key)): item_value for item_key, item_value in metrics.items()},
                    key_column="Metric",
                    value_column="Value",
                )
                if isinstance(status_counts, dict) and status_counts:
                    _render_mapping_table(
                        title="Client HTTP Status Counts",
                        data={str(item_key): item_value for item_key, item_value in status_counts.items()},
                        key_column="Status",
                        value_column="Count",
                    )
                continue
            _render_mapping_table(title=title, data=data)

    error = artifact.get("error")
    if isinstance(error, dict) and error:
        error_table = Table(title="Last Error")
        error_table.add_column("Field")
        error_table.add_column("Value")
        for key in ("type", "message", "reason", "task_name", "detail"):
            value = error.get(key)
            if value is not None:
                error_table.add_row(_format_metric_key(key), str(value))
        console.print(error_table)


@app.command("version")
def version_command() -> None:
    console.print(f"medium-stealth-bot {__version__}")


def _resolve_cookie_header(
    *,
    cookie_header: str | None,
    cookie_file: Path | None,
) -> str:
    if cookie_header and cookie_file is not None:
        raise RuntimeError("Specify either --cookie-header or --cookie-file, not both.")
    if cookie_file is not None:
        return cookie_file.read_text(encoding="utf-8").strip()
    if cookie_header:
        return cookie_header.strip()
    return str(
        typer.prompt(
            "Paste full Cookie header from a signed-in https://medium.com request",
            default="",
        )
    ).strip()


def _import_auth_material(
    *,
    cookie_header: str,
    medium_csrf: str | None = None,
    medium_user_ref: str | None = None,
) -> AuthSessionMaterial:
    return import_session_material_from_cookie_header(
        cookie_header,
        medium_csrf=medium_csrf,
        medium_user_ref=medium_user_ref,
    )


@app.command("profile-validate")
def profile_validate_command(
    env_path: Path = typer.Option(
        Path(".env.production"),
        "--env-path",
        help="Production environment profile file to validate.",
    ),
) -> None:
    """
    Validate production profile baseline settings before scheduled runs.
    """
    if not env_path.exists():
        _print_notice(f"Profile file not found: {env_path}", level="error")
        raise typer.Exit(code=1)

    try:
        settings = _bootstrap_settings(env_path=env_path)
    except Exception as exc:  # noqa: BLE001
        _print_notice(f"Failed to load profile from {env_path}: {exc}", level="error")
        raise typer.Exit(code=1) from exc

    issues = validate_production_profile(settings)
    summary = Table(title=f"Production Profile Baseline Validation ({env_path})")
    summary.add_column("Rule")
    summary.add_column("Expected")
    summary.add_column("Actual")

    if issues:
        for issue in issues:
            summary.add_row(issue.key, issue.expected, issue.actual)
        console.print(summary)
        _print_notice("Production profile validation failed.", level="error")
        raise typer.Exit(code=1)

    summary.add_row("status", "all checks passed", "ok")
    console.print(summary)
    _print_notice(
        "Production profile validation passed. Safety behavior is controlled by .env values.",
        level="success",
    )


@app.command("auth")
def auth_command(
    write_env: bool = typer.Option(True, "--write-env/--no-write-env"),
    env_path: Path = typer.Option(Path(".env"), help="Destination `.env` file to update."),
    login_url: str = typer.Option("https://medium.com/m/signin", help="Login URL to open in Playwright."),
    fallback_import: bool = typer.Option(
        True,
        "--fallback-import/--no-fallback-import",
        help="Offer cookie-header import fallback when interactive browser auth fails.",
    ),
) -> None:
    """
    Open an interactive Playwright session for Medium login and capture session cookies.
    """
    settings = _bootstrap_settings()
    material: AuthSessionMaterial | None = None
    try:
        material = asyncio.run(interactive_auth(settings=settings, login_url=login_url))
    except Exception as exc:  # noqa: BLE001
        _print_notice(f"Interactive auth failed: {exc}", level="warning")
        if not fallback_import:
            raise typer.Exit(code=1) from exc

        do_import = typer.confirm(
            "Import cookies from an already signed-in browser session instead?",
            default=True,
        )
        if not do_import:
            raise typer.Exit(code=1) from exc

        cookie_header = _resolve_cookie_header(cookie_header=None, cookie_file=None)
        if not cookie_header:
            _print_notice("Cookie header cannot be empty.", level="error")
            raise typer.Exit(code=1) from exc

        try:
            material = _import_auth_material(cookie_header=cookie_header)
        except Exception as import_exc:  # noqa: BLE001
            _print_notice(f"Cookie import failed: {import_exc}", level="error")
            raise typer.Exit(code=1) from import_exc
        _print_notice("Cookie import fallback succeeded.", level="success")

    if material is None:
        _print_notice("Auth did not return session material.", level="error")
        raise typer.Exit(code=1)
    if write_env:
        upsert_env_file(env_path=env_path, material=material)
        _print_notice(f"Updated env file: {env_path}", level="success")
    _render_auth(material)


@app.command("auth-import")
def auth_import_command(
    cookie_header: str | None = typer.Option(
        None,
        "--cookie-header",
        help="Raw Cookie header string copied from a signed-in https://medium.com request.",
    ),
    cookie_file: Path | None = typer.Option(
        None,
        "--cookie-file",
        help="Path to a text file containing the raw Cookie header string.",
    ),
    medium_csrf: str | None = typer.Option(
        None,
        "--medium-csrf",
        help="Optional explicit MEDIUM_CSRF override.",
    ),
    medium_user_ref: str | None = typer.Option(
        None,
        "--medium-user-ref",
        help="Optional explicit MEDIUM_USER_REF override.",
    ),
    write_env: bool = typer.Option(True, "--write-env/--no-write-env"),
    env_path: Path = typer.Option(Path(".env"), help="Destination `.env` file to update."),
) -> None:
    """
    Import auth session cookies from an already signed-in browser session.
    """
    _bootstrap_settings()
    try:
        resolved_header = _resolve_cookie_header(cookie_header=cookie_header, cookie_file=cookie_file)
    except Exception as exc:  # noqa: BLE001
        _print_notice(f"Failed to read cookie input: {exc}", level="error")
        raise typer.Exit(code=1) from exc

    if not resolved_header:
        _print_notice("Cookie header is empty.", level="error")
        raise typer.Exit(code=1)

    try:
        material = _import_auth_material(
            cookie_header=resolved_header,
            medium_csrf=medium_csrf,
            medium_user_ref=medium_user_ref,
        )
    except Exception as exc:  # noqa: BLE001
        _print_notice(f"Cookie import failed: {exc}", level="error")
        raise typer.Exit(code=1) from exc

    if write_env:
        upsert_env_file(env_path=env_path, material=material)
        _print_notice(f"Updated env file: {env_path}", level="success")
    _render_auth(material)


@app.command("setup")
def setup_command(
    env_path: Path = typer.Option(
        Path(".env"),
        "--env-path",
        help="Environment file to read and write interactive runtime defaults.",
    ),
    auth_if_missing: bool = typer.Option(
        True,
        "--auth-if-missing/--no-auth-if-missing",
        help="Offer interactive auth capture when MEDIUM_SESSION is missing.",
    ),
) -> None:
    """
    Interactive setup wizard for common runtime defaults.
    """
    settings = _bootstrap_settings(env_path=env_path)

    _print_notice("Interactive setup wizard", level="info")
    _print_notice(f"Target env file: {env_path}", level="info")

    if auth_if_missing and not settings.has_session:
        do_auth = typer.confirm(
            "No MEDIUM_SESSION found. Launch interactive Medium auth now?",
            default=True,
        )
        if do_auth:
            material = asyncio.run(interactive_auth(settings=settings))
            upsert_env_file(env_path=env_path, material=material)
            _print_notice("Auth session material saved.", level="success")
            settings = _bootstrap_settings(env_path=env_path)

    client_mode = str(
        typer.prompt(
            "Client mode (stealth/fast)",
            default=settings.client_mode,
        )
    ).strip().lower()
    while client_mode not in {"stealth", "fast"}:
        _print_notice("Invalid mode. Choose 'stealth' or 'fast'.", level="warning")
        client_mode = str(typer.prompt("Client mode (stealth/fast)", default="stealth")).strip().lower()

    max_actions = int(typer.prompt("Max actions per day", default=settings.max_actions_per_day, type=int))
    max_subscribe = int(
        typer.prompt("Max subscribe actions per day", default=settings.max_subscribe_actions_per_day, type=int)
    )
    max_unfollow = int(
        typer.prompt("Max unfollow actions per day", default=settings.max_unfollow_actions_per_day, type=int)
    )
    max_follow_per_run = int(
        typer.prompt("Max follow actions per run", default=settings.max_follow_actions_per_run, type=int)
    )
    live_session_duration_minutes = int(
        typer.prompt(
            "Live session duration minutes",
            default=settings.live_session_duration_minutes,
            type=int,
        )
    )
    live_session_target_follow_attempts = int(
        typer.prompt(
            "Live session target follow attempts",
            default=settings.live_session_target_follow_attempts,
            type=int,
        )
    )
    live_session_min_follow_attempts = int(
        typer.prompt(
            "Live session minimum follow attempts (soft floor)",
            default=settings.live_session_min_follow_attempts,
            type=int,
        )
    )
    live_session_max_passes = int(
        typer.prompt(
            "Live session max passes",
            default=settings.live_session_max_passes,
            type=int,
        )
    )
    follow_candidate_limit = int(
        typer.prompt("Follow candidate limit per run", default=settings.follow_candidate_limit, type=int)
    )
    follow_cooldown_hours = int(
        typer.prompt("Follow cooldown hours", default=settings.follow_cooldown_hours, type=int)
    )
    discovery_depth = int(
        typer.prompt(
            "Discovery followers depth (1 or 2)",
            default=settings.discovery_followers_depth,
            type=int,
        )
    )
    while discovery_depth not in {1, 2}:
        _print_notice("Discovery depth must be 1 or 2.", level="warning")
        discovery_depth = int(typer.prompt("Discovery followers depth (1 or 2)", default=1, type=int))

    seed_followers_limit = int(
        typer.prompt("Seed followers fetch limit", default=settings.discovery_seed_followers_limit, type=int)
    )
    second_hop_seed_limit = int(
        typer.prompt("Second-hop seed limit", default=settings.discovery_second_hop_seed_limit, type=int)
    )
    seed_users_raw = str(
        typer.prompt(
            "Default seed users (comma-separated, supports @username or id:<user_id>)",
            default=settings.discovery_seed_users_raw,
        )
    ).strip()

    enable_pre_follow_clap = typer.confirm(
        "Enable pre-follow clap?",
        default=settings.enable_pre_follow_clap,
    )
    max_mutations_per_10_minutes = int(
        typer.prompt(
            "Max mutations per 10 minutes",
            default=settings.max_mutations_per_10_minutes,
            type=int,
        )
    )
    min_verify_gap_seconds = int(
        typer.prompt(
            "Min verify/read gap seconds",
            default=settings.min_verify_gap_seconds,
            type=int,
        )
    )
    max_verify_gap_seconds = int(
        typer.prompt(
            "Max verify/read gap seconds",
            default=settings.max_verify_gap_seconds,
            type=int,
        )
    )
    pass_cooldown_min_seconds = int(
        typer.prompt(
            "Pass cooldown min seconds",
            default=settings.pass_cooldown_min_seconds,
            type=int,
        )
    )
    pass_cooldown_max_seconds = int(
        typer.prompt(
            "Pass cooldown max seconds",
            default=settings.pass_cooldown_max_seconds,
            type=int,
        )
    )
    pacing_soft_degrade_cooldown_seconds = int(
        typer.prompt(
            "Soft-degrade cooldown seconds",
            default=settings.pacing_soft_degrade_cooldown_seconds,
            type=int,
        )
    )
    enable_pacing_auto_clamp = typer.confirm(
        "Enable pacing auto-clamp?",
        default=settings.enable_pacing_auto_clamp,
    )
    cleanup_unfollow_limit = int(
        typer.prompt(
            "Cleanup unfollow limit per run",
            default=settings.cleanup_unfollow_limit,
            type=int,
        )
    )
    cleanup_unfollow_whitelist_min_followers = int(
        typer.prompt(
            "Cleanup whitelist minimum followers (keep if >= value)",
            default=settings.cleanup_unfollow_whitelist_min_followers,
            type=int,
        )
    )

    updates = {
        "CLIENT_MODE": client_mode,
        "MAX_ACTIONS_PER_DAY": str(max_actions),
        "MAX_SUBSCRIBE_ACTIONS_PER_DAY": str(max_subscribe),
        "MAX_UNFOLLOW_ACTIONS_PER_DAY": str(max_unfollow),
        "MAX_FOLLOW_ACTIONS_PER_RUN": str(max_follow_per_run),
        "LIVE_SESSION_DURATION_MINUTES": str(max(1, live_session_duration_minutes)),
        "LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS": str(max(1, live_session_target_follow_attempts)),
        "LIVE_SESSION_MIN_FOLLOW_ATTEMPTS": str(
            min(max(1, live_session_min_follow_attempts), max(1, live_session_target_follow_attempts))
        ),
        "LIVE_SESSION_MAX_PASSES": str(max(1, live_session_max_passes)),
        "FOLLOW_CANDIDATE_LIMIT": str(follow_candidate_limit),
        "FOLLOW_COOLDOWN_HOURS": str(follow_cooldown_hours),
        "DISCOVERY_FOLLOWERS_DEPTH": str(discovery_depth),
        "DISCOVERY_SEED_FOLLOWERS_LIMIT": str(seed_followers_limit),
        "DISCOVERY_SECOND_HOP_SEED_LIMIT": str(second_hop_seed_limit),
        "DISCOVERY_SEED_USERS": seed_users_raw,
        "ENABLE_PRE_FOLLOW_CLAP": "true" if enable_pre_follow_clap else "false",
        "MAX_MUTATIONS_PER_10_MINUTES": str(max(1, max_mutations_per_10_minutes)),
        "MIN_VERIFY_GAP_SECONDS": str(max(0, min_verify_gap_seconds)),
        "MAX_VERIFY_GAP_SECONDS": str(max(max(0, min_verify_gap_seconds), max(0, max_verify_gap_seconds))),
        "PASS_COOLDOWN_MIN_SECONDS": str(max(0, pass_cooldown_min_seconds)),
        "PASS_COOLDOWN_MAX_SECONDS": str(max(max(0, pass_cooldown_min_seconds), max(0, pass_cooldown_max_seconds))),
        "PACING_SOFT_DEGRADE_COOLDOWN_SECONDS": str(max(0, pacing_soft_degrade_cooldown_seconds)),
        "ENABLE_PACING_AUTO_CLAMP": "true" if enable_pacing_auto_clamp else "false",
        "CLEANUP_UNFOLLOW_LIMIT": str(cleanup_unfollow_limit),
        "CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS": str(max(0, cleanup_unfollow_whitelist_min_followers)),
    }
    _upsert_env_values(env_path=env_path, updates=updates)

    summary = Table(title="Setup Profile Saved")
    summary.add_column("Key")
    summary.add_column("Value")
    for key, value in updates.items():
        summary.add_row(key, value if value else "(empty)")
    console.print(summary)
    _print_notice(
        "Next step: run `uv run bot start` for immediate live execution "
        "(or `uv run bot run --dry-run` for preview-only).",
        level="success",
    )


def _normalize_seed_user_refs(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    normalized = [item.strip() for item in values if item and item.strip()]
    return normalized or None


def _parse_seed_user_refs(raw: str) -> list[str] | None:
    text = raw.strip()
    if not text:
        return None
    return _normalize_seed_user_refs(text.split(","))


def _seed_refs_summary(seed_user_refs: list[str] | None) -> str:
    if not seed_user_refs:
        return "-"
    if len(seed_user_refs) <= 3:
        return ", ".join(seed_user_refs)
    return f"{', '.join(seed_user_refs[:3])} (+{len(seed_user_refs) - 3} more)"


def _render_start_menu(
    *,
    has_session: bool,
    tag_slug: str,
    seed_user_refs: list[str] | None,
    live_session_minutes: int,
    live_session_target_follows: int,
    live_session_min_follows: int,
    live_session_max_passes: int,
    max_mutations_per_10_minutes: int,
    min_verify_gap_seconds: int,
    max_verify_gap_seconds: int,
    pass_cooldown_min_seconds: int,
    pass_cooldown_max_seconds: int,
    pacing_soft_degrade_cooldown_seconds: int,
    enable_pacing_auto_clamp: bool,
    reconcile_limit: int,
    reconcile_page_size: int,
    cleanup_unfollow_limit: int,
    cleanup_whitelist_min_followers: int,
    newsletter_slug: str | None,
    newsletter_username: str | None,
) -> None:
    status_label = "ready" if has_session else "missing (use option 16 to refresh auth)"
    _print_notice(
        f"Session status: {status_label}",
        level="success" if has_session else "warning",
    )

    defaults = Table(title="Current Defaults")
    defaults.add_column("Key")
    defaults.add_column("Value")
    defaults.add_row("Tag", tag_slug)
    defaults.add_row("Seed Users", _seed_refs_summary(seed_user_refs))
    defaults.add_row("Live Session Duration (m)", str(live_session_minutes))
    defaults.add_row("Live Session Target Follows", str(live_session_target_follows))
    defaults.add_row("Live Session Min Follows", str(live_session_min_follows))
    defaults.add_row("Live Session Max Passes", str(live_session_max_passes))
    defaults.add_row("Max Mutations / 10m", str(max_mutations_per_10_minutes))
    defaults.add_row("Verify Gap (s)", f"{min_verify_gap_seconds}-{max_verify_gap_seconds}")
    defaults.add_row("Pass Cooldown (s)", f"{pass_cooldown_min_seconds}-{pass_cooldown_max_seconds}")
    defaults.add_row("Soft-Degrade Cooldown (s)", str(pacing_soft_degrade_cooldown_seconds))
    defaults.add_row("Pacing Auto-Clamp", "true" if enable_pacing_auto_clamp else "false")
    defaults.add_row("Cleanup Limit", str(cleanup_unfollow_limit))
    defaults.add_row("Cleanup Whitelist Followers >=", str(cleanup_whitelist_min_followers))
    defaults.add_row("Reconcile Limit", str(reconcile_limit))
    defaults.add_row("Reconcile Page Size", str(reconcile_page_size))
    defaults.add_row("Newsletter Slug", newsletter_slug or "-")
    defaults.add_row("Newsletter Username", newsletter_username or "-")
    console.print(defaults)

    menu = Table(title="Start Menu")
    menu.add_column("Option", justify="right")
    menu.add_column("Group")
    menu.add_column("Action")
    menu.add_row("1", "Execution", "Run live growth session (multi-cycle)")
    menu.add_row("2", "Execution", "Run live growth cycle (single pass)")
    menu.add_row("3", "Execution", "Run growth cycle (dry-run)")
    menu.add_row("4", "Execution", "Run dry-run preflight then live growth session")
    menu.add_row("5", "Maintenance", "Cleanup-only unfollow (live)")
    menu.add_row("6", "Maintenance", "Cleanup-only unfollow (dry-run)")
    menu.add_row("7", "Maintenance", "Reconcile follow states (live)")
    menu.add_row("8", "Maintenance", "Reconcile follow states (dry-run)")
    menu.add_row("9", "Diagnostics", "Probe GraphQL reads")
    menu.add_row("10", "Diagnostics", "Validate operation contracts (parity only)")
    menu.add_row("11", "Diagnostics", "Validate contracts + execute live read checks")
    menu.add_row("12", "Observability", "Show latest run status")
    menu.add_row("13", "Observability", "Validate latest run artifact schema")
    menu.add_row("14", "Config", "Edit defaults")
    menu.add_row("15", "Config", "Run setup wizard")
    menu.add_row("16", "Auth", "Refresh auth session")
    menu.add_row("17", "System", "Exit")
    console.print(menu)


def _run_start_menu(
    *,
    initial_tag_slug: str,
    initial_seed_user_refs: list[str] | None,
    initial_live_session_minutes: int,
    initial_live_session_target_follows: int,
    initial_live_session_min_follows: int,
    initial_live_session_max_passes: int,
    initial_max_mutations_per_10_minutes: int,
    initial_min_verify_gap_seconds: int,
    initial_max_verify_gap_seconds: int,
    initial_pass_cooldown_min_seconds: int,
    initial_pass_cooldown_max_seconds: int,
    initial_pacing_soft_degrade_cooldown_seconds: int,
    initial_enable_pacing_auto_clamp: bool,
    initial_reconcile_limit: int,
    initial_reconcile_page_size: int,
    initial_cleanup_unfollow_limit: int,
    initial_cleanup_whitelist_min_followers: int,
    initial_newsletter_slug: str | None,
    initial_newsletter_username: str | None,
) -> None:
    tag_slug = initial_tag_slug.strip() or "programming"
    seed_user_refs = _normalize_seed_user_refs(initial_seed_user_refs)
    live_session_minutes = max(1, initial_live_session_minutes)
    live_session_target_follows = max(1, initial_live_session_target_follows)
    live_session_min_follows = max(1, initial_live_session_min_follows)
    live_session_max_passes = max(1, initial_live_session_max_passes)
    max_mutations_per_10_minutes = max(1, initial_max_mutations_per_10_minutes)
    min_verify_gap_seconds = max(0, initial_min_verify_gap_seconds)
    max_verify_gap_seconds = max(min_verify_gap_seconds, initial_max_verify_gap_seconds)
    pass_cooldown_min_seconds = max(0, initial_pass_cooldown_min_seconds)
    pass_cooldown_max_seconds = max(pass_cooldown_min_seconds, initial_pass_cooldown_max_seconds)
    pacing_soft_degrade_cooldown_seconds = max(0, initial_pacing_soft_degrade_cooldown_seconds)
    enable_pacing_auto_clamp = initial_enable_pacing_auto_clamp
    reconcile_limit = max(1, initial_reconcile_limit)
    reconcile_page_size = min(500, max(1, initial_reconcile_page_size))
    cleanup_unfollow_limit = max(1, initial_cleanup_unfollow_limit)
    cleanup_whitelist_min_followers = max(0, initial_cleanup_whitelist_min_followers)
    newsletter_slug = (initial_newsletter_slug or "").strip()
    newsletter_username = (initial_newsletter_username or "").strip()
    valid_choices = {str(value) for value in range(1, 18)}
    valid_choices_hint = "1-17"

    while True:
        settings = _bootstrap_settings()
        if live_session_min_follows > live_session_target_follows:
            live_session_min_follows = live_session_target_follows
        _render_start_menu(
            has_session=settings.has_session,
            tag_slug=tag_slug,
            seed_user_refs=seed_user_refs,
            live_session_minutes=live_session_minutes,
            live_session_target_follows=live_session_target_follows,
            live_session_min_follows=live_session_min_follows,
            live_session_max_passes=live_session_max_passes,
            max_mutations_per_10_minutes=max_mutations_per_10_minutes,
            min_verify_gap_seconds=min_verify_gap_seconds,
            max_verify_gap_seconds=max_verify_gap_seconds,
            pass_cooldown_min_seconds=pass_cooldown_min_seconds,
            pass_cooldown_max_seconds=pass_cooldown_max_seconds,
            pacing_soft_degrade_cooldown_seconds=pacing_soft_degrade_cooldown_seconds,
            enable_pacing_auto_clamp=enable_pacing_auto_clamp,
            reconcile_limit=reconcile_limit,
            reconcile_page_size=reconcile_page_size,
            cleanup_unfollow_limit=cleanup_unfollow_limit,
            cleanup_whitelist_min_followers=cleanup_whitelist_min_followers,
            newsletter_slug=newsletter_slug or None,
            newsletter_username=newsletter_username or None,
        )

        choice = str(typer.prompt("Select option", default="1")).strip().lower()
        if choice in {"q", "quit", "exit"}:
            choice = "17"
        if choice not in valid_choices:
            _print_notice(f"Invalid option. Choose {valid_choices_hint} (or q to exit).", level="warning")
            continue

        def _execute(action_name: str, fn) -> bool:
            _print_notice(f"Running: {action_name}", level="info")
            try:
                fn()
                _print_notice(f"{action_name} completed.", level="success")
                return True
            except typer.Exit as exc:
                if exc.exit_code == 0:
                    _print_notice(f"{action_name} completed.", level="success")
                    return True
                _print_notice(f"{action_name} ended with exit code {exc.exit_code}.", level="warning")
                return False
            except Exception as exc:  # noqa: BLE001
                _print_notice(f"{action_name} failed: {exc}", level="error")
                return False

        if choice == "1":
            _execute(
                "run live growth session",
                lambda: run_command(
                    tag_slug=tag_slug,
                    live=True,
                    seed_user_refs=seed_user_refs,
                    session=True,
                    session_minutes=live_session_minutes,
                    target_follows=live_session_target_follows,
                    session_max_passes=live_session_max_passes,
                ),
            )
        elif choice == "2":
            _execute(
                "run live single cycle",
                lambda: run_command(
                    tag_slug=tag_slug,
                    live=True,
                    seed_user_refs=seed_user_refs,
                    session=False,
                ),
            )
        elif choice == "3":
            _execute(
                "run dry-run cycle",
                lambda: run_command(tag_slug=tag_slug, live=False, seed_user_refs=seed_user_refs),
            )
        elif choice == "4":
            preflight_ok = _execute(
                "dry-run preflight",
                lambda: run_command(tag_slug=tag_slug, live=False, seed_user_refs=seed_user_refs),
            )
            if preflight_ok:
                _execute(
                    "run live growth session",
                    lambda: run_command(
                        tag_slug=tag_slug,
                        live=True,
                        seed_user_refs=seed_user_refs,
                        session=True,
                        session_minutes=live_session_minutes,
                        target_follows=live_session_target_follows,
                        session_max_passes=live_session_max_passes,
                    ),
                )
        elif choice == "5":
            run_limit = int(
                typer.prompt(
                    "Cleanup-only unfollow limit for this run",
                    default=cleanup_unfollow_limit,
                    type=int,
                )
            )
            if run_limit < 1:
                _print_notice("Cleanup limit must be >= 1. Using current default.", level="warning")
                run_limit = cleanup_unfollow_limit
            else:
                cleanup_unfollow_limit = run_limit
            _execute(
                "cleanup-only unfollow (live)",
                lambda: cleanup_command(live=True, limit=run_limit),
            )
        elif choice == "6":
            run_limit = int(
                typer.prompt(
                    "Cleanup-only unfollow limit for this run",
                    default=cleanup_unfollow_limit,
                    type=int,
                )
            )
            if run_limit < 1:
                _print_notice("Cleanup limit must be >= 1. Using current default.", level="warning")
                run_limit = cleanup_unfollow_limit
            else:
                cleanup_unfollow_limit = run_limit
            _execute(
                "cleanup-only unfollow (dry-run)",
                lambda: cleanup_command(live=False, limit=run_limit),
            )
        elif choice == "7":
            _execute(
                "reconcile live",
                lambda: reconcile_command(live=True, max_users=reconcile_limit, page_size=reconcile_page_size),
            )
        elif choice == "8":
            _execute(
                "reconcile dry-run",
                lambda: reconcile_command(live=False, max_users=reconcile_limit, page_size=reconcile_page_size),
            )
        elif choice == "9":
            _execute("probe reads", lambda: probe_command(tag_slug=tag_slug))
        elif choice == "10":
            _execute(
                "validate contracts",
                lambda: contracts_command(
                    tag_slug=tag_slug,
                    strict=True,
                    execute_reads=False,
                    newsletter_slug=None,
                    newsletter_username=None,
                ),
            )
        elif choice == "11":
            _execute(
                "validate contracts with live reads",
                lambda: contracts_command(
                    tag_slug=tag_slug,
                    strict=True,
                    execute_reads=True,
                    newsletter_slug=newsletter_slug or None,
                    newsletter_username=newsletter_username or None,
                ),
            )
        elif choice == "12":
            _execute("show status", status_command)
        elif choice == "13":
            _execute("validate latest artifact", lambda: artifacts_validate_command(artifact_path=None))
        elif choice == "14":
            updated_tag = str(typer.prompt("Default tag", default=tag_slug)).strip()
            if updated_tag:
                tag_slug = updated_tag

            seed_default = ", ".join(seed_user_refs) if seed_user_refs else ""
            seed_input = str(
                typer.prompt(
                    "Default seed users (comma-separated, '-' to clear)",
                    default=seed_default,
                )
            ).strip()
            if seed_input == "-":
                seed_user_refs = None
            else:
                seed_user_refs = _parse_seed_user_refs(seed_input)

            session_minutes_value = int(
                typer.prompt(
                    "Default live session duration (minutes)",
                    default=live_session_minutes,
                    type=int,
                )
            )
            if session_minutes_value < 1:
                _print_notice("Live session duration must be >= 1. Keeping previous value.", level="warning")
            else:
                live_session_minutes = session_minutes_value

            session_target_value = int(
                typer.prompt(
                    "Default live session follow target (attempts)",
                    default=live_session_target_follows,
                    type=int,
                )
            )
            if session_target_value < 1:
                _print_notice("Live session follow target must be >= 1. Keeping previous value.", level="warning")
            else:
                live_session_target_follows = session_target_value

            session_min_value = int(
                typer.prompt(
                    "Default live session minimum follows (soft floor)",
                    default=live_session_min_follows,
                    type=int,
                )
            )
            if session_min_value < 1:
                _print_notice("Live session minimum follows must be >= 1. Keeping previous value.", level="warning")
            else:
                live_session_min_follows = session_min_value

            session_pass_value = int(
                typer.prompt(
                    "Default live session max passes",
                    default=live_session_max_passes,
                    type=int,
                )
            )
            if session_pass_value < 1:
                _print_notice("Live session max passes must be >= 1. Keeping previous value.", level="warning")
            else:
                live_session_max_passes = session_pass_value

            mutation_cap_value = int(
                typer.prompt(
                    "Default max mutations per 10 minutes",
                    default=max_mutations_per_10_minutes,
                    type=int,
                )
            )
            if mutation_cap_value < 1:
                _print_notice("Mutation cap must be >= 1. Keeping previous value.", level="warning")
            else:
                max_mutations_per_10_minutes = mutation_cap_value

            verify_min_value = int(
                typer.prompt(
                    "Default min verify/read gap seconds",
                    default=min_verify_gap_seconds,
                    type=int,
                )
            )
            if verify_min_value < 0:
                _print_notice("Verify gap minimum must be >= 0. Keeping previous value.", level="warning")
            else:
                min_verify_gap_seconds = verify_min_value

            verify_max_value = int(
                typer.prompt(
                    "Default max verify/read gap seconds",
                    default=max_verify_gap_seconds,
                    type=int,
                )
            )
            if verify_max_value < min_verify_gap_seconds:
                _print_notice(
                    "Verify gap maximum must be >= minimum. Keeping previous value.",
                    level="warning",
                )
            else:
                max_verify_gap_seconds = verify_max_value

            pass_cooldown_min_value = int(
                typer.prompt(
                    "Default pass cooldown min seconds",
                    default=pass_cooldown_min_seconds,
                    type=int,
                )
            )
            if pass_cooldown_min_value < 0:
                _print_notice("Pass cooldown minimum must be >= 0. Keeping previous value.", level="warning")
            else:
                pass_cooldown_min_seconds = pass_cooldown_min_value

            pass_cooldown_max_value = int(
                typer.prompt(
                    "Default pass cooldown max seconds",
                    default=pass_cooldown_max_seconds,
                    type=int,
                )
            )
            if pass_cooldown_max_value < pass_cooldown_min_seconds:
                _print_notice(
                    "Pass cooldown maximum must be >= minimum. Keeping previous value.",
                    level="warning",
                )
            else:
                pass_cooldown_max_seconds = pass_cooldown_max_value

            soft_degrade_value = int(
                typer.prompt(
                    "Default soft-degrade cooldown seconds",
                    default=pacing_soft_degrade_cooldown_seconds,
                    type=int,
                )
            )
            if soft_degrade_value < 0:
                _print_notice("Soft-degrade cooldown must be >= 0. Keeping previous value.", level="warning")
            else:
                pacing_soft_degrade_cooldown_seconds = soft_degrade_value

            enable_pacing_auto_clamp = typer.confirm(
                "Enable pacing auto-clamp by default?",
                default=enable_pacing_auto_clamp,
            )

            cleanup_limit_value = int(
                typer.prompt(
                    "Default cleanup-only unfollow limit",
                    default=cleanup_unfollow_limit,
                    type=int,
                )
            )
            if cleanup_limit_value < 1:
                _print_notice("Cleanup limit must be >= 1. Keeping previous value.", level="warning")
            else:
                cleanup_unfollow_limit = cleanup_limit_value

            whitelist_value = int(
                typer.prompt(
                    "Whitelist threshold: keep users with follower_count >=",
                    default=cleanup_whitelist_min_followers,
                    type=int,
                )
            )
            if whitelist_value < 0:
                _print_notice("Whitelist threshold must be >= 0. Keeping previous value.", level="warning")
            else:
                cleanup_whitelist_min_followers = whitelist_value

            limit_value = int(typer.prompt("Default reconcile limit", default=reconcile_limit, type=int))
            if limit_value < 1:
                _print_notice("Reconcile limit must be >= 1. Keeping previous value.", level="warning")
            else:
                reconcile_limit = limit_value

            page_size_value = int(typer.prompt("Default reconcile page size", default=reconcile_page_size, type=int))
            if page_size_value < 1 or page_size_value > 500:
                _print_notice(
                    "Reconcile page size must be between 1 and 500. Keeping previous value.",
                    level="warning",
                )
            else:
                reconcile_page_size = page_size_value

            slug_input = str(
                typer.prompt(
                    "Default newsletter slug for contracts live reads ('-' to clear)",
                    default=newsletter_slug,
                )
            ).strip()
            newsletter_slug = "" if slug_input == "-" else slug_input

            username_input = str(
                typer.prompt(
                    "Default newsletter username for contracts live reads ('-' to clear)",
                    default=newsletter_username,
                )
            ).strip()
            newsletter_username = "" if username_input == "-" else username_input

            _print_notice("Defaults updated.", level="success")
        elif choice == "15":
            _execute("run setup wizard", lambda: setup_command(env_path=Path(".env"), auth_if_missing=True))
            refreshed_settings = _bootstrap_settings()
            seed_user_refs = seed_user_refs or refreshed_settings.discovery_seed_users
            live_session_minutes = refreshed_settings.live_session_duration_minutes
            live_session_target_follows = refreshed_settings.live_session_target_follow_attempts
            live_session_min_follows = refreshed_settings.live_session_min_follow_attempts
            live_session_max_passes = refreshed_settings.live_session_max_passes
            max_mutations_per_10_minutes = refreshed_settings.max_mutations_per_10_minutes
            min_verify_gap_seconds = refreshed_settings.min_verify_gap_seconds
            max_verify_gap_seconds = refreshed_settings.max_verify_gap_seconds
            pass_cooldown_min_seconds = refreshed_settings.pass_cooldown_min_seconds
            pass_cooldown_max_seconds = refreshed_settings.pass_cooldown_max_seconds
            pacing_soft_degrade_cooldown_seconds = refreshed_settings.pacing_soft_degrade_cooldown_seconds
            enable_pacing_auto_clamp = refreshed_settings.enable_pacing_auto_clamp
            cleanup_unfollow_limit = max(1, refreshed_settings.cleanup_unfollow_limit)
            cleanup_whitelist_min_followers = max(0, refreshed_settings.cleanup_unfollow_whitelist_min_followers)
            if not newsletter_slug:
                newsletter_slug = refreshed_settings.contract_registry_live_newsletter_slug or ""
            if not newsletter_username:
                newsletter_username = refreshed_settings.contract_registry_live_newsletter_username or ""
        elif choice == "16":
            _execute(
                "refresh auth session",
                lambda: auth_command(
                    write_env=True,
                    env_path=Path(".env"),
                    login_url="https://medium.com/m/signin",
                ),
            )
        else:
            _print_notice("Exiting start menu.", level="success")
            return


@app.command("start")
def start_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for discovery/probe operations.",
    ),
    dry_run_first: bool = typer.Option(
        False,
        "--dry-run-first/--live-only",
        help="Optionally run a dry-run preflight before live execution.",
    ),
    seed_user_refs: list[str] | None = typer.Option(
        None,
        "--seed-user",
        help="Optional seed users. Repeat option. Falls back to DISCOVERY_SEED_USERS from .env.",
    ),
    interactive: bool = typer.Option(
        True,
        "--interactive/--quick-live",
        help="Open interactive numbered menu (default). Use --quick-live for direct execution.",
    ),
) -> None:
    """
    Guided start command with interactive menu and optional quick-live mode.
    """
    settings = _bootstrap_settings()
    resolved_seeds = _normalize_seed_user_refs(seed_user_refs if seed_user_refs else settings.discovery_seed_users)

    if interactive:
        _run_start_menu(
            initial_tag_slug=tag_slug,
            initial_seed_user_refs=resolved_seeds,
            initial_live_session_minutes=settings.live_session_duration_minutes,
            initial_live_session_target_follows=settings.live_session_target_follow_attempts,
            initial_live_session_min_follows=settings.live_session_min_follow_attempts,
            initial_live_session_max_passes=settings.live_session_max_passes,
            initial_max_mutations_per_10_minutes=settings.max_mutations_per_10_minutes,
            initial_min_verify_gap_seconds=settings.min_verify_gap_seconds,
            initial_max_verify_gap_seconds=settings.max_verify_gap_seconds,
            initial_pass_cooldown_min_seconds=settings.pass_cooldown_min_seconds,
            initial_pass_cooldown_max_seconds=settings.pass_cooldown_max_seconds,
            initial_pacing_soft_degrade_cooldown_seconds=settings.pacing_soft_degrade_cooldown_seconds,
            initial_enable_pacing_auto_clamp=settings.enable_pacing_auto_clamp,
            initial_reconcile_limit=settings.reconcile_scan_limit,
            initial_reconcile_page_size=settings.reconcile_page_size,
            initial_cleanup_unfollow_limit=max(1, settings.cleanup_unfollow_limit),
            initial_cleanup_whitelist_min_followers=settings.cleanup_unfollow_whitelist_min_followers,
            initial_newsletter_slug=settings.contract_registry_live_newsletter_slug,
            initial_newsletter_username=settings.contract_registry_live_newsletter_username,
        )
        return

    _require_session(settings, guidance="uv run bot auth (or uv run bot setup)")

    if dry_run_first:
        _print_notice("Step 1/2: running dry-run sanity check.", level="info")
        run_command(
            tag_slug=tag_slug,
            live=False,
            seed_user_refs=resolved_seeds,
        )
        _print_notice("Dry-run preflight complete; continuing to live session execution.", level="success")
    else:
        _print_notice("Step 1/1: running live growth session.", level="info")

    if dry_run_first:
        _print_notice("Step 2/2: running live growth session.", level="info")
    run_command(
        tag_slug=tag_slug,
        live=True,
        seed_user_refs=resolved_seeds,
    )


@app.command("probe")
def probe_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for probe requests.",
    ),
) -> None:
    """
    Execute paced read-only GraphQL probes for the given tag.
    """
    settings = _bootstrap_settings()
    _require_session(settings)

    run_id = new_run_id("probe")
    structlog_contextvars.bind_contextvars(run_id=run_id, command="probe", tag_slug=tag_slug)
    _print_notice(f"Starting probe run `{run_id}` for tag `{tag_slug}`.", level="info")
    try:
        _, repository = _build_runner(settings)

        async def _run() -> ProbeSnapshot:
            async with MediumAsyncClient(settings) as client:
                runner = DailyRunner(settings=settings, client=client, repository=repository)
                return await runner.probe(tag_slug=tag_slug)

        try:
            snapshot = asyncio.run(_run())
        except RiskHaltError as exc:
            _render_risk_halt(exc, settings=settings)
            halt_exit_code = _risk_halt_exit_code(settings)
            if halt_exit_code != 0:
                raise typer.Exit(code=halt_exit_code) from exc
            return
        _render_probe(snapshot)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("contracts")
def contracts_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for operation contract checks.",
    ),
    strict: bool = typer.Option(
        True,
        "--strict/--no-strict",
        help="Use strict registry mode (unknown operations fail validation).",
    ),
    execute_reads: bool = typer.Option(
        False,
        "--execute-reads/--no-execute-reads",
        help="Optionally execute read/state-verify operations against Medium (requires MEDIUM_SESSION).",
    ),
    newsletter_slug: str | None = typer.Option(
        None,
        "--newsletter-slug",
        help="Optional newsletter slug for live NewsletterV3ViewerEdge checks.",
    ),
    newsletter_username: str | None = typer.Option(
        None,
        "--newsletter-username",
        help="Optional newsletter username paired with --newsletter-slug for live NewsletterV3ViewerEdge checks.",
    ),
) -> None:
    """
    Validate implementation operation contracts against the canonical registry.
    """
    settings = _bootstrap_settings()
    if execute_reads:
        _require_session(settings)

    run_id = new_run_id("contracts")
    structlog_contextvars.bind_contextvars(run_id=run_id, command="contracts", tag_slug=tag_slug)
    _print_notice(
        "Starting contracts validation "
        f"(run_id={run_id}, strict={str(strict).lower()}, execute_reads={str(execute_reads).lower()}).",
        level="info",
    )
    try:
        report = validate_contract_registry(
            registry_path=settings.implementation_ops_registry_path,
            strict=strict,
            tag_slug=tag_slug,
            actor_user_id=settings.medium_user_ref,
            execute_reads=execute_reads,
            settings=settings,
            live_newsletter_slug=newsletter_slug or settings.contract_registry_live_newsletter_slug,
            live_newsletter_username=newsletter_username or settings.contract_registry_live_newsletter_username,
        )
        _render_contract_validation(report)
        if not report.ok:
            raise typer.Exit(code=1)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("run")
def run_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for discovery and follow actions.",
    ),
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Execute live mutations by default. Use --dry-run for preview-only execution.",
    ),
    seed_user_refs: list[str] | None = typer.Option(
        None,
        "--seed-user",
        help="Optional seed for followers discovery. Repeat option. Supports @username or user_id.",
    ),
    session: bool = typer.Option(
        True,
        "--session/--single-cycle",
        help="In live mode, run repeated growth cycles until session targets are reached. Use --single-cycle for one pass.",
    ),
    session_minutes: int | None = typer.Option(
        None,
        "--session-minutes",
        min=1,
        max=24 * 12,
        help="Optional live session max duration in minutes. Defaults to LIVE_SESSION_DURATION_MINUTES.",
    ),
    target_follows: int | None = typer.Option(
        None,
        "--target-follows",
        min=1,
        max=5000,
        help="Optional live session follow-attempt target. Defaults to LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS.",
    ),
    session_max_passes: int | None = typer.Option(
        None,
        "--session-max-passes",
        min=1,
        max=500,
        help="Optional cap on growth-cycle passes in one live session. Defaults to LIVE_SESSION_MAX_PASSES.",
    ),
) -> None:
    """
    Run growth automation in either single-cycle or live session mode.
    """
    settings = _bootstrap_settings()
    _require_session(settings)

    run_id = new_run_id("run")
    structlog_contextvars.bind_contextvars(
        run_id=run_id,
        command="run",
        tag_slug=tag_slug,
        mode="live" if live else "dry_run",
    )
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    outcome: DailyRunOutcome | None = None
    artifact_path: Path | None = None
    live_session_enabled = live and session
    mode_label = "live-session" if live_session_enabled else "live-single" if live else "dry-run"
    _print_notice(f"Starting run `{run_id}` (mode={mode_label}, tag={tag_slug}).", level="info")

    try:
        try:
            _, repository = _build_runner(settings)

            async def _run() -> DailyRunOutcome:
                async with MediumAsyncClient(settings) as client:
                    runner = DailyRunner(settings=settings, client=client, repository=repository)
                    if live_session_enabled:
                        resolved_session_minutes = session_minutes or settings.live_session_duration_minutes
                        resolved_target_follows = target_follows or settings.live_session_target_follow_attempts
                        resolved_min_follows = min(
                            settings.live_session_min_follow_attempts,
                            resolved_target_follows,
                        )
                        resolved_session_max_passes = session_max_passes or settings.live_session_max_passes
                        _print_notice(
                            "Live session targets: "
                            f"duration={resolved_session_minutes}m, "
                            f"follow_attempts_min={resolved_min_follows}, "
                            f"follow_attempts={resolved_target_follows}, "
                            f"max_passes={resolved_session_max_passes}.",
                            level="info",
                        )
                        return await runner.run_live_session(
                            tag_slug=tag_slug,
                            seed_user_refs=seed_user_refs or None,
                            target_follow_attempts=resolved_target_follows,
                            max_duration_minutes=resolved_session_minutes,
                            max_passes=resolved_session_max_passes,
                        )
                    return await runner.run_daily_cycle(
                        tag_slug=tag_slug,
                        dry_run=not live,
                        seed_user_refs=seed_user_refs or None,
                    )

            outcome = asyncio.run(_run())
        except RiskHaltError as exc:
            status = "halted"
            exit_code = _risk_halt_exit_code(settings)
            error_payload = {
                "type": "RiskHaltError",
                "message": str(exc),
                "reason": exc.reason,
                "task_name": exc.task_name,
                "detail": exc.detail,
            }
            _render_risk_halt(exc, settings=settings)
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            exit_code = 1
            error_payload = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            _print_notice(f"Run failed: {exc}", level="error")

        ended_at = _utc_now()
        artifact_payload = _build_run_artifact_payload(
            run_id=run_id,
            command="run",
            started_at=started_at,
            ended_at=ended_at,
            tag_slug=tag_slug,
            dry_run=not live,
            status=status,
            outcome=outcome,
            error=error_payload,
        )
        payload_ok, payload_issues = validate_artifact_payload(artifact_payload)
        if not payload_ok:
            raise RuntimeError(f"run artifact payload schema validation failed: {', '.join(payload_issues)}")
        artifact_path = write_run_artifact(
            artifacts_dir=settings.run_artifacts_dir,
            run_id=run_id,
            payload=artifact_payload,
        )
        log.info(
            "run_artifact_written",
            operation="run_artifact",
            target_id=None,
            decision="persist",
            result="ok",
            artifact_path=str(artifact_path),
            status=status,
        )

        if outcome is not None:
            _render_daily_run(outcome)
        _print_notice(f"Run artifact saved: {artifact_path}", level="success")

        if exit_code != 0:
            raise typer.Exit(code=exit_code)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("cleanup")
def cleanup_command(
    live: bool = typer.Option(
        False,
        "--live/--dry-run",
        help="Run cleanup-only unfollow in dry-run mode by default. Use --live to execute mutations.",
    ),
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        max=5000,
        help="Optional max cleanup unfollow attempts for this run. Defaults to CLEANUP_UNFOLLOW_LIMIT.",
    ),
) -> None:
    """
    Run cleanup-only unfollow maintenance for overdue non-followback users.
    """
    settings = _bootstrap_settings()
    _require_session(settings)

    run_id = new_run_id("cleanup")
    structlog_contextvars.bind_contextvars(
        run_id=run_id,
        command="cleanup",
        mode="live" if live else "dry_run",
    )
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    outcome: DailyRunOutcome | None = None
    artifact_path: Path | None = None
    resolved_limit = limit if limit is not None else settings.cleanup_unfollow_limit
    mode_label = "live" if live else "dry-run"
    _print_notice(
        f"Starting cleanup-only run `{run_id}` (mode={mode_label}, limit={resolved_limit}).",
        level="info",
    )

    try:
        try:
            _, repository = _build_runner(settings)

            async def _run() -> DailyRunOutcome:
                async with MediumAsyncClient(settings) as client:
                    runner = DailyRunner(settings=settings, client=client, repository=repository)
                    return await runner.run_cleanup_only(
                        dry_run=not live,
                        max_unfollows=resolved_limit,
                    )

            outcome = asyncio.run(_run())
        except RiskHaltError as exc:
            status = "halted"
            exit_code = _risk_halt_exit_code(settings)
            error_payload = {
                "type": "RiskHaltError",
                "message": str(exc),
                "reason": exc.reason,
                "task_name": exc.task_name,
                "detail": exc.detail,
            }
            _render_risk_halt(exc, settings=settings)
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            exit_code = 1
            error_payload = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            _print_notice(f"Cleanup-only run failed: {exc}", level="error")

        ended_at = _utc_now()
        artifact_payload = _build_run_artifact_payload(
            run_id=run_id,
            command="cleanup",
            started_at=started_at,
            ended_at=ended_at,
            tag_slug="cleanup_only",
            dry_run=not live,
            status=status,
            outcome=outcome,
            error=error_payload,
        )
        payload_ok, payload_issues = validate_artifact_payload(artifact_payload)
        if not payload_ok:
            raise RuntimeError(f"cleanup artifact payload schema validation failed: {', '.join(payload_issues)}")
        artifact_path = write_run_artifact(
            artifacts_dir=settings.run_artifacts_dir,
            run_id=run_id,
            payload=artifact_payload,
        )
        log.info(
            "run_artifact_written",
            operation="run_artifact",
            target_id=None,
            decision="persist",
            result="ok",
            artifact_path=str(artifact_path),
            status=status,
        )

        if outcome is not None:
            _render_daily_run(outcome)
        _print_notice(f"Run artifact saved: {artifact_path}", level="success")

        if exit_code != 0:
            raise typer.Exit(code=exit_code)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("reconcile")
def reconcile_command(
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Persist reconciliation updates by default. Use --dry-run for read-only validation.",
    ),
    max_users: int = typer.Option(
        200,
        "--limit",
        min=1,
        max=5000,
        help="Max users to reconcile in this execution.",
    ),
    page_size: int = typer.Option(
        50,
        "--page-size",
        min=1,
        max=500,
        help="Pagination window for candidate reconciliation scans.",
    ),
) -> None:
    """
    Reconcile local follow state against live UserViewerEdge checks.
    """
    settings = _bootstrap_settings()
    _require_session(settings)

    run_id = new_run_id("reconcile")
    structlog_contextvars.bind_contextvars(
        run_id=run_id,
        command="reconcile",
        mode="live" if live else "dry_run",
    )
    _print_notice(
        f"Starting reconcile run `{run_id}` (mode={'live' if live else 'dry-run'}, limit={max_users}, page_size={page_size}).",
        level="info",
    )
    try:
        _, repository = _build_runner(settings)

        async def _run() -> ReconcileOutcome:
            async with MediumAsyncClient(settings) as client:
                runner = DailyRunner(settings=settings, client=client, repository=repository)
                return await runner.reconcile_follow_states(
                    dry_run=not live,
                    max_users=max_users,
                    page_size=page_size,
                )

        try:
            outcome = asyncio.run(_run())
        except RiskHaltError as exc:
            _render_risk_halt(exc, settings=settings)
            halt_exit_code = _risk_halt_exit_code(settings)
            if halt_exit_code != 0:
                raise typer.Exit(code=halt_exit_code) from exc
            return
        _render_reconcile_outcome(outcome)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("status")
def status_command() -> None:
    """
    Show last-run diagnostic health from the latest run artifact.
    """
    settings = _bootstrap_settings()
    latest = read_latest_run_artifact(settings.run_artifacts_dir)
    if not latest:
        _print_notice(
            f"No run artifacts found in {settings.run_artifacts_dir}. Execute `uv run bot run` first.",
            level="warning",
        )
        return
    artifact, source = latest
    ok, issues = validate_artifact_payload(artifact)
    if not ok:
        _print_notice(
            "Latest artifact schema is invalid or unsupported. "
            f"Issues: {', '.join(issues)}",
            level="error",
        )
        raise typer.Exit(code=1)
    _render_status(artifact, artifact_path=source)


@artifacts_app.command("validate")
def artifacts_validate_command(
    artifact_path: Path | None = typer.Option(
        None,
        "--path",
        help="Optional path to a run artifact. Defaults to latest artifact in RUN_ARTIFACTS_DIR.",
    ),
) -> None:
    """
    Validate run artifact schema compatibility and contract shape.
    """
    settings = _bootstrap_settings()
    if artifact_path is None:
        latest = read_latest_run_artifact(settings.run_artifacts_dir)
        if not latest:
            _print_notice(f"No artifacts found in {settings.run_artifacts_dir}.", level="warning")
            raise typer.Exit(code=1)
        artifact, source = latest
    else:
        source = artifact_path
        try:
            import json

            payload = json.loads(source.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            _print_notice(f"Failed to read artifact: {exc}", level="error")
            raise typer.Exit(code=1) from exc
        artifact = payload if isinstance(payload, dict) else {}

    ok, issues = validate_artifact_payload(artifact)
    if not ok:
        _print_notice(
            f"Artifact validation failed for {source}: {', '.join(issues)}",
            level="error",
        )
        raise typer.Exit(code=1)
    _print_notice(f"Artifact validation passed: {source}", level="success")


if __name__ == "__main__":
    app()
