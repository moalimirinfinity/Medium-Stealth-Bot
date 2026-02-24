import asyncio
from datetime import datetime, timezone
from pathlib import Path

import structlog
import typer
from rich.console import Console
from rich.table import Table
from structlog import contextvars as structlog_contextvars

from medium_stealth_bot import __version__
from medium_stealth_bot.auth import interactive_auth, upsert_env_file
from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.contracts import ContractValidationReport, validate_contract_registry
from medium_stealth_bot.database import Database
from medium_stealth_bot.logging import configure_logging
from medium_stealth_bot.logic import DailyRunner
from medium_stealth_bot.models import AuthSessionMaterial, DailyRunOutcome, ProbeSnapshot
from medium_stealth_bot.observability import new_run_id, read_latest_run_artifact, write_run_artifact
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.safety import RiskHaltError
from medium_stealth_bot.settings import AppSettings

app = typer.Typer(
    help="Medium Stealth Bot scaffold (dual-mode GraphQL client + Playwright auth + Pydantic settings).",
    no_args_is_help=True,
)
console = Console()


def _mask(value: str | None, keep: int = 4) -> str:
    if not value:
        return "-"
    if len(value) <= keep * 2:
        return "*" * len(value)
    return f"{value[:keep]}...{value[-keep:]}"


def _bootstrap_settings() -> AppSettings:
    settings = AppSettings()
    settings.ensure_directories()
    configure_logging(settings.log_level)
    return settings


def _build_runner(settings: AppSettings) -> tuple[Database, ActionRepository]:
    database = Database(settings.db_path)
    database.initialize()
    repository = ActionRepository(database)
    return database, repository


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
    console.print(f"Probe duration: {snapshot.duration_ms}ms")


def _render_daily_run(outcome: DailyRunOutcome) -> None:
    if outcome.budget_exhausted:
        console.print(
            f"Daily budget exhausted (UTC day): {outcome.actions_today}/{outcome.max_actions_per_day}.",
            style="yellow",
        )
        return
    mode_label = "dry-run" if outcome.dry_run else "live"
    console.print(
        f"Daily budget check passed (UTC day): {outcome.actions_today}/{outcome.max_actions_per_day} (mode={mode_label}).",
        style="green",
    )
    console.print(
        "Candidates considered/eligible: "
        f"{outcome.considered_candidates}/{outcome.eligible_candidates}"
    )
    console.print(
        "Follow attempted/verified: "
        f"{outcome.follow_actions_attempted}/{outcome.follow_actions_verified}"
    )
    console.print(
        "Cleanup attempted/verified: "
        f"{outcome.cleanup_actions_attempted}/{outcome.cleanup_actions_verified}"
    )
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
    if outcome.decision_log:
        table = Table(title="Decision Log (sample)")
        table.add_column("#")
        table.add_column("Decision")
        for idx, item in enumerate(outcome.decision_log[:12], start=1):
            table.add_row(str(idx), item)
        console.print(table)
    if outcome.probe:
        _render_probe(outcome.probe)


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
        console.print(f"Registry load error: {report.load_error}", style="red")
        return

    if report.missing_in_code:
        console.print("Missing operations in code: " + ", ".join(report.missing_in_code), style="red")
    if report.extra_in_code:
        console.print("Extra operations in code: " + ", ".join(report.extra_in_code), style="yellow")

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


def _render_risk_halt(exc: RiskHaltError) -> None:
    console.print("Run halted by safety guardrails.", style="red")
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
    if status in {"failed", "halted"}:
        return "failed"
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
        "command": "run",
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
    }
    if outcome is None:
        return payload

    payload["summary"] = {
        "budget_exhausted": outcome.budget_exhausted,
        "actions_today": outcome.actions_today,
        "max_actions_per_day": outcome.max_actions_per_day,
        "considered_candidates": outcome.considered_candidates,
        "eligible_candidates": outcome.eligible_candidates,
        "follow_actions_attempted": outcome.follow_actions_attempted,
        "follow_actions_verified": outcome.follow_actions_verified,
        "cleanup_actions_attempted": outcome.cleanup_actions_attempted,
        "cleanup_actions_verified": outcome.cleanup_actions_verified,
    }
    payload["action_counts"] = outcome.action_counts_today
    payload["result_counts"] = outcome.decision_result_counts
    payload["reason_counts"] = outcome.decision_reason_counts
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
            "considered_candidates",
            "eligible_candidates",
            "follow_actions_attempted",
            "follow_actions_verified",
            "cleanup_actions_attempted",
            "cleanup_actions_verified",
        ):
            if key in summary:
                summary_table.add_row(key, str(summary[key]))
        console.print(summary_table)

    for title, key in (
        ("Action Counts", "action_counts"),
        ("Decision Result Counts", "result_counts"),
        ("Decision Reason Counts", "reason_counts"),
    ):
        data = artifact.get(key)
        if isinstance(data, dict) and data:
            counts = Table(title=title)
            counts.add_column("Key")
            counts.add_column("Count")
            for item_key, item_value in sorted(data.items()):
                counts.add_row(str(item_key), str(item_value))
            console.print(counts)

    error = artifact.get("error")
    if isinstance(error, dict) and error:
        error_table = Table(title="Last Error")
        error_table.add_column("Field")
        error_table.add_column("Value")
        for key in ("type", "message", "reason", "task_name", "detail"):
            value = error.get(key)
            if value is not None:
                error_table.add_row(key, str(value))
        console.print(error_table)


@app.command("version")
def version_command() -> None:
    console.print(f"medium-stealth-bot {__version__}")


@app.command("auth")
def auth_command(
    write_env: bool = typer.Option(True, "--write-env/--no-write-env"),
    env_path: Path = typer.Option(Path(".env"), help="Destination .env file to update."),
    login_url: str = typer.Option("https://medium.com/m/signin", help="Login URL to open in Playwright."),
) -> None:
    """
    Open an interactive Playwright session for Medium login and capture session cookies.
    """
    settings = _bootstrap_settings()
    material = asyncio.run(interactive_auth(settings=settings, login_url=login_url))
    if write_env:
        upsert_env_file(env_path=env_path, material=material)
        console.print(f"Updated env file: {env_path}")
    _render_auth(material)


@app.command("probe")
def probe_command(
    tag_slug: str = typer.Option("programming", "--tag"),
) -> None:
    """
    Execute parallel read-only GraphQL probes for the given tag.
    """
    settings = _bootstrap_settings()
    if not settings.has_session:
        raise typer.BadParameter("No MEDIUM_SESSION found. Run `uv run bot auth` first.")

    run_id = new_run_id("probe")
    structlog_contextvars.bind_contextvars(run_id=run_id, command="probe", tag_slug=tag_slug)
    try:
        _, repository = _build_runner(settings)

        async def _run() -> ProbeSnapshot:
            async with MediumAsyncClient(settings) as client:
                runner = DailyRunner(settings=settings, client=client, repository=repository)
                return await runner.probe(tag_slug=tag_slug)

        try:
            snapshot = asyncio.run(_run())
        except RiskHaltError as exc:
            _render_risk_halt(exc)
            raise typer.Exit(code=2) from exc
        _render_probe(snapshot)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("contracts")
def contracts_command(
    tag_slug: str = typer.Option("programming", "--tag"),
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
) -> None:
    """
    Validate implementation operation contracts against the canonical registry.
    """
    settings = _bootstrap_settings()
    run_id = new_run_id("contracts")
    structlog_contextvars.bind_contextvars(run_id=run_id, command="contracts", tag_slug=tag_slug)
    try:
        report = validate_contract_registry(
            registry_path=settings.implementation_ops_registry_path,
            strict=strict,
            tag_slug=tag_slug,
            actor_user_id=settings.medium_user_ref,
            execute_reads=execute_reads,
            settings=settings,
            live_newsletter_slug=newsletter_slug or settings.contract_registry_live_newsletter_slug,
        )
        _render_contract_validation(report)
        if not report.ok:
            raise typer.Exit(code=1)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("run")
def run_command(
    tag_slug: str = typer.Option("programming", "--tag"),
    dry_run: bool = typer.Option(
        True,
        "--dry-run/--live",
        help="Run decision/action pipeline in dry-run mode or execute live mutations.",
    ),
    seed_user_refs: list[str] | None = typer.Option(
        None,
        "--seed-user",
        help="Optional seed for followers discovery. Repeat option. Supports @username or user_id.",
    ),
) -> None:
    """
    Run one daily-cycle pass (discovery + scoring + follow pipeline + cleanup).
    """
    settings = _bootstrap_settings()
    if not settings.has_session:
        raise typer.BadParameter("No MEDIUM_SESSION found. Run `uv run bot auth` first.")

    run_id = new_run_id("run")
    structlog_contextvars.bind_contextvars(
        run_id=run_id,
        command="run",
        tag_slug=tag_slug,
        mode="dry_run" if dry_run else "live",
    )
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    outcome: DailyRunOutcome | None = None
    artifact_path: Path | None = None

    try:
        try:
            _, repository = _build_runner(settings)

            async def _run() -> DailyRunOutcome:
                async with MediumAsyncClient(settings) as client:
                    runner = DailyRunner(settings=settings, client=client, repository=repository)
                    return await runner.run_daily_cycle(
                        tag_slug=tag_slug,
                        dry_run=dry_run,
                        seed_user_refs=seed_user_refs or None,
                    )

            outcome = asyncio.run(_run())
        except RiskHaltError as exc:
            status = "halted"
            exit_code = 2
            error_payload = {
                "type": "RiskHaltError",
                "message": str(exc),
                "reason": exc.reason,
                "task_name": exc.task_name,
                "detail": exc.detail,
            }
            _render_risk_halt(exc)
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            exit_code = 1
            error_payload = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            console.print(f"Run failed: {exc}", style="red")

        ended_at = _utc_now()
        artifact_payload = _build_run_artifact_payload(
            run_id=run_id,
            started_at=started_at,
            ended_at=ended_at,
            tag_slug=tag_slug,
            dry_run=dry_run,
            status=status,
            outcome=outcome,
            error=error_payload,
        )
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
        console.print(f"Run artifact: {artifact_path}")

        if exit_code != 0:
            raise typer.Exit(code=exit_code)
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
        console.print(
            f"No run artifacts found in {settings.run_artifacts_dir}. Execute `uv run bot run --dry-run` first.",
            style="yellow",
        )
        return
    artifact, source = latest
    _render_status(artifact, artifact_path=source)


if __name__ == "__main__":
    app()
