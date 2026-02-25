import asyncio
from datetime import datetime, timezone
from pathlib import Path

import structlog
import typer
from rich.console import Console
from rich.table import Table
from structlog import contextvars as structlog_contextvars

from medium_stealth_bot import __version__
from medium_stealth_bot.artifact_schema import validate_artifact_payload
from medium_stealth_bot.auth import interactive_auth, upsert_env_file
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
    help="Medium Stealth Bot scaffold (dual-mode GraphQL client + Playwright auth + Pydantic settings).",
    no_args_is_help=True,
)
artifacts_app = typer.Typer(help="Run artifact diagnostics and schema checks.")
app.add_typer(artifacts_app, name="artifacts")
console = Console()


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
        "Clap attempted/verified: "
        f"{outcome.clap_actions_attempted}/{outcome.clap_actions_verified}"
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
    if outcome.kpis:
        kpi_table = Table(title="KPI Summary")
        kpi_table.add_column("KPI")
        kpi_table.add_column("Value")
        for key, value in sorted(outcome.kpis.items()):
            kpi_table.add_row(key, str(value))
        console.print(kpi_table)
    if outcome.client_metrics:
        metrics_table = Table(title="Client Metrics")
        metrics_table.add_column("Metric")
        metrics_table.add_column("Value")
        for key, value in sorted(outcome.client_metrics.items()):
            metrics_table.add_row(key, str(value))
        console.print(metrics_table)
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
                summary_table.add_row(key, str(summary[key]))
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


@app.command("profile-validate")
def profile_validate_command(
    env_path: Path = typer.Option(
        Path(".env.production"),
        "--env-path",
        help="Production env profile file to validate.",
    ),
) -> None:
    """
    Validate production profile baseline settings before scheduled runs.
    """
    if not env_path.exists():
        console.print(f"Profile file not found: {env_path}", style="red")
        raise typer.Exit(code=1)

    try:
        settings = _bootstrap_settings(env_path=env_path)
    except Exception as exc:  # noqa: BLE001
        console.print(f"Failed to load profile from {env_path}: {exc}", style="red")
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
        console.print("Production profile validation failed.", style="red")
        raise typer.Exit(code=1)

    summary.add_row("status", "all checks passed", "ok")
    console.print(summary)
    console.print(
        "Production profile validation passed. Safety behavior is controlled by .env values.",
        style="green",
    )


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


@app.command("setup")
def setup_command(
    env_path: Path = typer.Option(
        Path(".env"),
        "--env-path",
        help="Env file to read and write interactive runtime defaults.",
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

    console.print("Interactive setup wizard")
    console.print(f"Target env file: {env_path}")

    if auth_if_missing and not settings.has_session:
        do_auth = typer.confirm(
            "No MEDIUM_SESSION found. Launch interactive Medium auth now?",
            default=True,
        )
        if do_auth:
            material = asyncio.run(interactive_auth(settings=settings))
            upsert_env_file(env_path=env_path, material=material)
            console.print("Auth session material saved.", style="green")
            settings = _bootstrap_settings(env_path=env_path)

    client_mode = str(
        typer.prompt(
            "Client mode (stealth/fast)",
            default=settings.client_mode,
        )
    ).strip().lower()
    while client_mode not in {"stealth", "fast"}:
        console.print("Invalid mode. Choose 'stealth' or 'fast'.", style="yellow")
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
        console.print("Discovery depth must be 1 or 2.", style="yellow")
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
    cleanup_unfollow_limit = int(
        typer.prompt(
            "Cleanup unfollow limit per run",
            default=settings.cleanup_unfollow_limit,
            type=int,
        )
    )

    updates = {
        "CLIENT_MODE": client_mode,
        "MAX_ACTIONS_PER_DAY": str(max_actions),
        "MAX_SUBSCRIBE_ACTIONS_PER_DAY": str(max_subscribe),
        "MAX_UNFOLLOW_ACTIONS_PER_DAY": str(max_unfollow),
        "MAX_FOLLOW_ACTIONS_PER_RUN": str(max_follow_per_run),
        "FOLLOW_CANDIDATE_LIMIT": str(follow_candidate_limit),
        "FOLLOW_COOLDOWN_HOURS": str(follow_cooldown_hours),
        "DISCOVERY_FOLLOWERS_DEPTH": str(discovery_depth),
        "DISCOVERY_SEED_FOLLOWERS_LIMIT": str(seed_followers_limit),
        "DISCOVERY_SECOND_HOP_SEED_LIMIT": str(second_hop_seed_limit),
        "DISCOVERY_SEED_USERS": seed_users_raw,
        "ENABLE_PRE_FOLLOW_CLAP": "true" if enable_pre_follow_clap else "false",
        "CLEANUP_UNFOLLOW_LIMIT": str(cleanup_unfollow_limit),
    }
    _upsert_env_values(env_path=env_path, updates=updates)

    summary = Table(title="Setup Profile Saved")
    summary.add_column("Key")
    summary.add_column("Value")
    for key, value in updates.items():
        summary.add_row(key, value if value else "(empty)")
    console.print(summary)
    console.print(
        "Next step: run `uv run bot start` for immediate live execution "
        "(or `uv run bot run --dry-run` for preview-only).",
        style="green",
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
    reconcile_limit: int,
    reconcile_page_size: int,
    newsletter_slug: str | None,
    newsletter_username: str | None,
) -> None:
    status_style = "green" if has_session else "yellow"
    status_label = "ready" if has_session else "missing (choose option 13: auth)"
    console.print(f"Session status: {status_label}", style=status_style)

    defaults = Table(title="Current Defaults")
    defaults.add_column("Key")
    defaults.add_column("Value")
    defaults.add_row("Tag", tag_slug)
    defaults.add_row("Seed Users", _seed_refs_summary(seed_user_refs))
    defaults.add_row("Reconcile Limit", str(reconcile_limit))
    defaults.add_row("Reconcile Page Size", str(reconcile_page_size))
    defaults.add_row("Newsletter Slug", newsletter_slug or "-")
    defaults.add_row("Newsletter Username", newsletter_username or "-")
    console.print(defaults)

    menu = Table(title="Start Menu")
    menu.add_column("Option", justify="right")
    menu.add_column("Action")
    menu.add_row("1", "Run growth cycle (live)")
    menu.add_row("2", "Run growth cycle (dry-run)")
    menu.add_row("3", "Run dry-run preflight then live growth cycle")
    menu.add_row("4", "Reconcile follow states (live)")
    menu.add_row("5", "Reconcile follow states (dry-run)")
    menu.add_row("6", "Probe GraphQL reads")
    menu.add_row("7", "Validate operation contracts (parity only)")
    menu.add_row("8", "Validate contracts + execute live read checks")
    menu.add_row("9", "Show latest run status")
    menu.add_row("10", "Validate latest run artifact schema")
    menu.add_row("11", "Edit defaults")
    menu.add_row("12", "Run setup wizard")
    menu.add_row("13", "Refresh auth session")
    menu.add_row("14", "Exit")
    console.print(menu)


def _run_start_menu(
    *,
    initial_tag_slug: str,
    initial_seed_user_refs: list[str] | None,
    initial_reconcile_limit: int,
    initial_reconcile_page_size: int,
    initial_newsletter_slug: str | None,
    initial_newsletter_username: str | None,
) -> None:
    tag_slug = initial_tag_slug.strip() or "programming"
    seed_user_refs = _normalize_seed_user_refs(initial_seed_user_refs)
    reconcile_limit = max(1, initial_reconcile_limit)
    reconcile_page_size = min(500, max(1, initial_reconcile_page_size))
    newsletter_slug = (initial_newsletter_slug or "").strip()
    newsletter_username = (initial_newsletter_username or "").strip()
    valid_choices = {str(value) for value in range(1, 15)}

    while True:
        settings = _bootstrap_settings()
        _render_start_menu(
            has_session=settings.has_session,
            tag_slug=tag_slug,
            seed_user_refs=seed_user_refs,
            reconcile_limit=reconcile_limit,
            reconcile_page_size=reconcile_page_size,
            newsletter_slug=newsletter_slug or None,
            newsletter_username=newsletter_username or None,
        )

        choice = str(typer.prompt("Select option", default="1")).strip().lower()
        if choice in {"q", "quit", "exit"}:
            choice = "14"
        if choice not in valid_choices:
            console.print("Invalid option. Choose a number from 1 to 14.", style="yellow")
            continue

        def _execute(action_name: str, fn) -> bool:
            console.print(f"Running: {action_name}", style="cyan")
            try:
                fn()
                return True
            except typer.Exit as exc:
                if exc.exit_code == 0:
                    return True
                console.print(f"{action_name} ended with exit code {exc.exit_code}.", style="yellow")
                return False
            except Exception as exc:  # noqa: BLE001
                console.print(f"{action_name} failed: {exc}", style="red")
                return False

        if choice == "1":
            _execute(
                "run live cycle",
                lambda: run_command(tag_slug=tag_slug, live=True, seed_user_refs=seed_user_refs),
            )
        elif choice == "2":
            _execute(
                "run dry-run cycle",
                lambda: run_command(tag_slug=tag_slug, live=False, seed_user_refs=seed_user_refs),
            )
        elif choice == "3":
            preflight_ok = _execute(
                "dry-run preflight",
                lambda: run_command(tag_slug=tag_slug, live=False, seed_user_refs=seed_user_refs),
            )
            if preflight_ok:
                _execute(
                    "run live cycle",
                    lambda: run_command(tag_slug=tag_slug, live=True, seed_user_refs=seed_user_refs),
                )
        elif choice == "4":
            _execute(
                "reconcile live",
                lambda: reconcile_command(live=True, max_users=reconcile_limit, page_size=reconcile_page_size),
            )
        elif choice == "5":
            _execute(
                "reconcile dry-run",
                lambda: reconcile_command(live=False, max_users=reconcile_limit, page_size=reconcile_page_size),
            )
        elif choice == "6":
            _execute("probe reads", lambda: probe_command(tag_slug=tag_slug))
        elif choice == "7":
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
        elif choice == "8":
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
        elif choice == "9":
            _execute("show status", status_command)
        elif choice == "10":
            _execute("validate latest artifact", lambda: artifacts_validate_command(artifact_path=None))
        elif choice == "11":
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

            limit_value = int(typer.prompt("Default reconcile limit", default=reconcile_limit, type=int))
            if limit_value < 1:
                console.print("Reconcile limit must be >= 1. Keeping previous value.", style="yellow")
            else:
                reconcile_limit = limit_value

            page_size_value = int(typer.prompt("Default reconcile page size", default=reconcile_page_size, type=int))
            if page_size_value < 1 or page_size_value > 500:
                console.print("Reconcile page size must be between 1 and 500. Keeping previous value.", style="yellow")
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

            console.print("Defaults updated.", style="green")
        elif choice == "12":
            _execute("run setup wizard", lambda: setup_command(env_path=Path(".env"), auth_if_missing=True))
            refreshed_settings = _bootstrap_settings()
            if not seed_user_refs and refreshed_settings.discovery_seed_users:
                seed_user_refs = refreshed_settings.discovery_seed_users
            if not newsletter_slug and refreshed_settings.contract_registry_live_newsletter_slug:
                newsletter_slug = refreshed_settings.contract_registry_live_newsletter_slug
            if not newsletter_username and refreshed_settings.contract_registry_live_newsletter_username:
                newsletter_username = refreshed_settings.contract_registry_live_newsletter_username
        elif choice == "13":
            _execute(
                "refresh auth session",
                lambda: auth_command(
                    write_env=True,
                    env_path=Path(".env"),
                    login_url="https://medium.com/m/signin",
                ),
            )
        else:
            console.print("Exiting start menu.", style="green")
            return


@app.command("start")
def start_command(
    tag_slug: str = typer.Option("programming", "--tag"),
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
            initial_reconcile_limit=settings.reconcile_scan_limit,
            initial_reconcile_page_size=settings.reconcile_page_size,
            initial_newsletter_slug=settings.contract_registry_live_newsletter_slug,
            initial_newsletter_username=settings.contract_registry_live_newsletter_username,
        )
        return

    if not settings.has_session:
        raise typer.BadParameter("No MEDIUM_SESSION found. Run `uv run bot auth` or `uv run bot setup` first.")

    if dry_run_first:
        console.print("Step 1/2: running dry-run sanity check", style="cyan")
        run_command(
            tag_slug=tag_slug,
            live=False,
            seed_user_refs=resolved_seeds,
        )
        console.print("Dry-run preflight complete; continuing to live execution.", style="green")
    else:
        console.print("Step 1/1: running live cycle", style="cyan")

    if dry_run_first:
        console.print("Step 2/2: running live cycle", style="cyan")
    run_command(
        tag_slug=tag_slug,
        live=True,
        seed_user_refs=resolved_seeds,
    )


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
            live_newsletter_username=newsletter_username or settings.contract_registry_live_newsletter_username,
        )
        _render_contract_validation(report)
        if not report.ok:
            raise typer.Exit(code=1)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("run")
def run_command(
    tag_slug: str = typer.Option("programming", "--tag"),
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
        mode="live" if live else "dry_run",
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
                        dry_run=not live,
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
        console.print(f"Run artifact: {artifact_path}")

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
    if not settings.has_session:
        raise typer.BadParameter("No MEDIUM_SESSION found. Run `uv run bot auth` first.")

    run_id = new_run_id("reconcile")
    structlog_contextvars.bind_contextvars(
        run_id=run_id,
        command="reconcile",
        mode="live" if live else "dry_run",
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
            _render_risk_halt(exc)
            raise typer.Exit(code=2) from exc
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
        console.print(
            f"No run artifacts found in {settings.run_artifacts_dir}. Execute `uv run bot run` first.",
            style="yellow",
        )
        return
    artifact, source = latest
    ok, issues = validate_artifact_payload(artifact)
    if not ok:
        console.print(
            "Latest artifact schema is invalid or unsupported. "
            f"Issues: {', '.join(issues)}",
            style="red",
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
            console.print(f"No artifacts found in {settings.run_artifacts_dir}.", style="yellow")
            raise typer.Exit(code=1)
        artifact, source = latest
    else:
        source = artifact_path
        try:
            import json

            payload = json.loads(source.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            console.print(f"Failed to read artifact: {exc}", style="red")
            raise typer.Exit(code=1) from exc
        artifact = payload if isinstance(payload, dict) else {}

    ok, issues = validate_artifact_payload(artifact)
    if not ok:
        console.print(
            f"Artifact validation failed for {source}: {', '.join(issues)}",
            style="red",
        )
        raise typer.Exit(code=1)
    console.print(f"Artifact validation passed: {source}", style="green")


if __name__ == "__main__":
    app()
