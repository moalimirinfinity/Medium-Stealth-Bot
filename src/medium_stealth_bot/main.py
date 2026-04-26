import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import structlog
import typer
from rich.console import Console
from rich.table import Table
from structlog import contextvars as structlog_contextvars
from typer.models import OptionInfo

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
from medium_stealth_bot.models import (
    AuthSessionMaterial,
    DailyRunOutcome,
    GraphSyncOutcome,
    GrowthDiscoveryMode,
    GrowthMode,
    GrowthPolicy,
    GrowthSource,
    ProbeSnapshot,
    ReconcileOutcome,
)
from medium_stealth_bot.observability import new_run_id, read_latest_run_artifact, write_run_artifact
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.safety import RiskHaltError
from medium_stealth_bot.settings import AppSettings

app = typer.Typer(
    help="Local-first Medium automation CLI with guided workflows, safety guardrails, and run diagnostics.",
    no_args_is_help=True,
)
artifacts_app = typer.Typer(help="Inspect and validate run artifact payloads.")
growth_app = typer.Typer(help="Growth workflows and strategy-oriented command aliases.")
unfollow_app = typer.Typer(help="Unfollow and cleanup workflow aliases.")
maintenance_app = typer.Typer(help="Maintenance workflow aliases.")
diagnostics_app = typer.Typer(help="Diagnostics and contract-check aliases.")
observe_app = typer.Typer(help="Run observability and artifact inspection aliases.")
app.add_typer(artifacts_app, name="artifacts")
app.add_typer(growth_app, name="growth")
app.add_typer(unfollow_app, name="unfollow")
app.add_typer(maintenance_app, name="maintenance")
app.add_typer(diagnostics_app, name="diagnostics")
app.add_typer(observe_app, name="observe")
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

_START_MENU_SECTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Discovery", "Discover, score, evaluate, and queue growth candidates."),
    ("2", "Growth", "Execute follow growth from execution-ready queue."),
    ("3", "Unfollow", "Run cleanup-only unfollow workflows."),
    ("4", "Maintenance", "Reconcile and sync local social graph state."),
    ("5", "Diagnostics", "Probe Medium reads and validate contracts."),
    ("6", "Observability", "Inspect the latest run status and queue/artifacts."),
    ("7", "Settings/Auth", "Edit defaults, run setup, or refresh auth."),
    ("8", "Exit", "Leave the interactive start menu."),
)

_START_MENU_SECTION_STYLES: dict[str, str] = {
    "Discovery": "bright_cyan",
    "Growth": "cyan",
    "Growth Source": "cyan",
    "Discovery Runtime": "bright_cyan",
    "Growth Policy": "cyan",
    "Growth Runtime": "cyan",
    "Unfollow": "yellow",
    "Maintenance": "blue",
    "Diagnostics": "magenta",
    "Observability": "green",
    "Settings/Auth": "bright_cyan",
    "Exit": "red",
}

_GROWTH_SOURCE_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Topic/Recommended", "Blend topic stories, recommendations, and who-to-follow."),
    ("2", "Seed Followers", "Discover followers from your configured seed users."),
    ("3", "Target-User Followers", "Harvest followers of a supplied user or id."),
    ("4", "Publication/Adjacency", "Pull authors from topic-curated publication adjacency."),
    ("5", "Responders", "Discover users leaving responses on recent tag posts."),
    ("6", "Back", "Return to the start sections"),
)
_GROWTH_SOURCE_MENU_CHOICE_MAP: dict[str, GrowthSource] = {
    "1": GrowthSource.TOPIC_RECOMMENDED,
    "2": GrowthSource.SEED_FOLLOWERS,
    "3": GrowthSource.TARGET_USER_FOLLOWERS,
    "4": GrowthSource.PUBLICATION_ADJACENT,
    "5": GrowthSource.RESPONDERS,
}

_GROWTH_POLICY_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Follow-Only", "Follow suitable users without pre-follow engagement."),
    ("2", "Warm-Engage", "Read and clap before following."),
    ("3", "Warm-Engage + Comment", "Read, clap, and comment before following."),
    ("4", "Warm-Engage + Highlight", "Read, clap, and highlight a deliberate span before following."),
    ("5", "Back", "Return to growth source selection"),
)

_DISCOVERY_RUNTIME_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Persist", "Run discovery and persist execution-ready queue entries."),
    ("2", "Dry-run", "Preview discovery decisions without persisting queue updates."),
    ("3", "Back", "Return to source selection"),
)

_GROWTH_RUNTIME_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Session", "Run a live multi-cycle growth session."),
    ("2", "Single Pass", "Run one live queue-drain growth cycle."),
    ("3", "Preflight", "Run one dry-run queue-drain growth cycle."),
    ("4", "Hybrid", "Run preflight, then continue into a live session."),
    ("5", "Back", "Return to growth policy selection"),
)

_UNFOLLOW_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Live", "Run cleanup-only unfollow"),
    ("2", "Dry-run", "Preview cleanup-only unfollow"),
    ("3", "Back", "Return to the start sections"),
)

_MAINTENANCE_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Live", "Reconcile follow states"),
    ("2", "Dry-run", "Preview follow-state reconciliation"),
    ("3", "Sync", "Sync social graph cache"),
    ("4", "DB Hygiene (Dry-run)", "Preview DB pruning impact with current retention policy."),
    ("5", "DB Hygiene (Live)", "Apply DB pruning retention policy."),
    ("6", "Back", "Return to the start sections"),
)

_DIAGNOSTICS_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Read", "Probe GraphQL reads"),
    ("2", "Validate", "Validate operation contracts (parity only)"),
    ("3", "Validate+Read", "Validate contracts and run live read checks"),
    ("4", "Back", "Return to the start sections"),
)

_OBSERVABILITY_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Inspect", "Show latest run status"),
    ("2", "Queue", "Show growth queue ready/deferred counts"),
    ("3", "Validate", "Validate latest run artifact schema"),
    ("4", "Back", "Return to the start sections"),
)

_SETTINGS_MENU_OPTIONS: tuple[tuple[str, str, str], ...] = (
    ("1", "Config", "Edit start-menu defaults"),
    ("2", "Setup", "Run setup wizard"),
    ("3", "Auth", "Refresh auth session"),
    ("4", "Back", "Return to the start sections"),
)
_CANONICAL_GROWTH_POLICIES: tuple[GrowthPolicy, ...] = (
    GrowthPolicy.FOLLOW_ONLY,
    GrowthPolicy.WARM_ENGAGE,
    GrowthPolicy.WARM_ENGAGE_COMMENT,
    GrowthPolicy.WARM_ENGAGE_HIGHLIGHT,
)
_GROWTH_POLICY_CHOICES = ", ".join(policy.value for policy in _CANONICAL_GROWTH_POLICIES)
_GROWTH_POLICY_HELP = (
    "Growth policy. `follow-only`, `warm-engage`, `warm-engage-plus-comment`, "
    "or `warm-engage-plus-highlight`. Deprecated alias: `warm-engage-plus-rare-comment`."
)
_GROWTH_SOURCE_CHOICES = ", ".join(source.value for source in GrowthSource)


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


def _format_growth_mode(mode: GrowthMode | str | None) -> str:
    if mode is None:
        return "-"
    value = mode.value if isinstance(mode, GrowthMode) else str(mode)
    return value.replace("-", " ").replace("_", " ").strip().title()


def _format_growth_policy(policy: GrowthPolicy | str | None) -> str:
    if policy is None:
        return "-"
    value = policy.value if isinstance(policy, GrowthPolicy) else str(policy)
    return value.replace("-", " ").replace("_", " ").strip().title()


def _format_growth_source(source: GrowthSource | str | None) -> str:
    if source is None:
        return "-"
    value = source.value if isinstance(source, GrowthSource) else str(source)
    return value.replace("-", " ").replace("_", " ").strip().title()


def _format_growth_sources(sources: list[GrowthSource] | list[str] | None) -> str:
    if not sources:
        return "-"
    labels = [_format_growth_source(source) for source in sources]
    return ", ".join(labels)


def _format_discovery_mode(mode: GrowthDiscoveryMode | str | None) -> str:
    if mode is None:
        return "-"
    value = mode.value if isinstance(mode, GrowthDiscoveryMode) else str(mode)
    return value.replace("-", " ").replace("_", " ").strip().title()


def _prompt_growth_policy_choice(prompt_text: str, *, default: GrowthPolicy) -> GrowthPolicy:
    while True:
        raw_value = str(typer.prompt(prompt_text, default=default.value)).strip().lower()
        try:
            return _canonical_growth_policy(GrowthPolicy(raw_value))
        except ValueError:
            _print_notice(
                "Invalid growth policy. Choose one of "
                f"{_GROWTH_POLICY_CHOICES}; deprecated alias "
                f"{GrowthPolicy.WARM_ENGAGE_RARE_COMMENT.value} is still accepted.",
                level="warning",
            )


def _parse_growth_sources(raw_value: str) -> list[GrowthSource]:
    parsed: list[GrowthSource] = []
    for token in raw_value.split(","):
        normalized = token.strip().lower()
        if not normalized:
            continue
        parsed.append(GrowthSource(normalized))
    deduped: list[GrowthSource] = []
    for source in parsed:
        if source not in deduped:
            deduped.append(source)
    return deduped


def _prompt_growth_sources_choice(prompt_text: str, *, default: list[GrowthSource]) -> list[GrowthSource]:
    default_value = ",".join(source.value for source in default)
    while True:
        raw_value = str(typer.prompt(prompt_text, default=default_value)).strip().lower()
        try:
            sources = _parse_growth_sources(raw_value)
        except ValueError:
            _print_notice(f"Invalid growth source. Choose from {_GROWTH_SOURCE_CHOICES}.", level="warning")
            continue
        if not sources:
            _print_notice("At least one growth source is required.", level="warning")
            continue
        return sources


def _prompt_growth_sources_from_menu(*, default: list[GrowthSource]) -> list[GrowthSource] | str | None:
    menu_sources = _GROWTH_SOURCE_MENU_CHOICE_MAP
    default_tokens = [key for key, source in menu_sources.items() if source in default]
    default_value = ",".join(default_tokens) if default_tokens else "1"
    while True:
        _render_start_submenu(
            title="Growth Source",
            options=_GROWTH_SOURCE_MENU_OPTIONS,
            style=_START_MENU_SECTION_STYLES.get("Growth Source", "white"),
        )
        raw_choice = str(
            typer.prompt(
                "Select option(s) (comma-separated)",
                default=default_value,
            )
        ).strip().lower()
        if raw_choice in {"b", "back", "6"}:
            return None
        if raw_choice in {"q", "quit", "exit"}:
            return "exit"
        tokens = [token.strip() for token in raw_choice.split(",") if token.strip()]
        if not tokens:
            _print_notice("At least one growth source is required.", level="warning")
            continue
        invalid = [token for token in tokens if token not in menu_sources]
        if invalid:
            _print_notice("Invalid growth source selection. Choose from 1-5, or `b`/`q`.", level="warning")
            continue
        deduped_sources: list[GrowthSource] = []
        for token in tokens:
            source = menu_sources[token]
            if source not in deduped_sources:
                deduped_sources.append(source)
        return deduped_sources


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


def _canonical_growth_policy(growth_policy: GrowthPolicy) -> GrowthPolicy:
    if growth_policy == GrowthPolicy.WARM_ENGAGE_RARE_COMMENT:
        return GrowthPolicy.WARM_ENGAGE_COMMENT
    return growth_policy


def _resolve_growth_policy_option(
    *,
    growth_policy: GrowthPolicy | None,
    mode: GrowthMode | None,
    default: GrowthPolicy,
) -> GrowthPolicy:
    if growth_policy is not None:
        return _canonical_growth_policy(growth_policy)
    if mode == GrowthMode.SIMPLE:
        return GrowthPolicy.FOLLOW_ONLY
    if mode == GrowthMode.SMART:
        return GrowthPolicy.WARM_ENGAGE_COMMENT
    return _canonical_growth_policy(default)


def _runtime_settings_for_growth_policy(
    settings: AppSettings,
    *,
    growth_policy: GrowthPolicy,
) -> tuple[AppSettings, bool]:
    resolved_policy = _canonical_growth_policy(growth_policy)
    if resolved_policy not in {GrowthPolicy.WARM_ENGAGE_COMMENT, GrowthPolicy.WARM_ENGAGE_HIGHLIGHT}:
        return settings, False
    updates: dict[str, object] = {}
    if resolved_policy == GrowthPolicy.WARM_ENGAGE_COMMENT and not settings.enable_pre_follow_comment:
        updates["enable_pre_follow_comment"] = True
    if resolved_policy == GrowthPolicy.WARM_ENGAGE_HIGHLIGHT and not settings.enable_pre_follow_highlight:
        updates["enable_pre_follow_highlight"] = True
    if not updates:
        return settings, False
    overridden = settings.model_copy(update=updates)
    return overridden, True


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


def _render_growth_queue_status(queue_counts: dict[str, int], *, title: str = "Growth Queue Status") -> None:
    table = Table(title=title)
    table.add_column("Metric")
    table.add_column("Count")
    table.add_row("Ready", str(queue_counts.get("ready", 0)))
    table.add_row("Queued (Execution-Ready)", str(queue_counts.get("queued", 0)))
    queued_held = int(queue_counts.get("queued_held", 0) or 0)
    if queued_held:
        table.add_row("Queued (Held)", str(queued_held))
    table.add_row("Deferred (All)", str(queue_counts.get("deferred", 0)))
    table.add_row("Deferred (Due)", str(queue_counts.get("deferred_due", 0)))
    table.add_row("Deferred (Future)", str(queue_counts.get("deferred_future", 0)))
    deferred_held = int(queue_counts.get("deferred_held", 0) or 0)
    if deferred_held:
        table.add_row("Deferred (Held)", str(deferred_held))
    table.add_row("Total", str(queue_counts.get("total", 0)))
    console.print(table)


def _render_db_hygiene_status(result: dict[str, int | bool], *, mode: str) -> None:
    table = Table(title=f"DB Hygiene ({mode})")
    table.add_column("Scope")
    table.add_column("Rows")
    table.add_row("Queue: Followed", str(int(result.get("queue_followed", 0))))
    table.add_row("Queue: Rejected", str(int(result.get("queue_rejected", 0))))
    table.add_row("Queue: Stale Queued/Deferred", str(int(result.get("queue_stale", 0))))
    table.add_row("Action Log", str(int(result.get("action_log", 0))))
    table.add_row("Graph Sync Runs", str(int(result.get("graph_sync_runs", 0))))
    table.add_row("Candidate Reconciliation", str(int(result.get("candidate_reconciliation", 0))))
    table.add_row("Follow Cycle (Terminal)", str(int(result.get("follow_cycle_terminal", 0))))
    table.add_row("Snapshots", str(int(result.get("snapshots", 0))))
    table.add_row("Total", str(int(result.get("total", 0))))
    table.add_row(
        "Vacuum",
        "yes" if bool(result.get("vacuum_performed", False)) else "no",
    )
    console.print(table)


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
    if outcome.growth_policy is not None:
        summary_table.add_row("Growth Policy", _format_growth_policy(outcome.growth_policy))
    if outcome.growth_sources:
        summary_table.add_row("Growth Sources", _format_growth_sources(outcome.growth_sources))
    if outcome.target_user_refs:
        summary_table.add_row("Target Users", _seed_refs_summary(outcome.target_user_refs))
    if outcome.target_user_scan_limit is not None:
        summary_table.add_row("Target User Scan Limit", str(outcome.target_user_scan_limit))
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
    queue_ready = outcome.kpis.get("growth_queue_ready")
    queue_deferred = outcome.kpis.get("growth_queue_deferred")
    if all(isinstance(value, (int, float)) for value in (queue_ready, queue_deferred)):
        summary_table.add_row(
            "Queue Ready/Deferred",
            f"{int(queue_ready)} / {int(queue_deferred)}",
        )
    discovery_enabled_value = outcome.kpis.get("growth_discovery_enabled")
    discovery_enabled = (
        bool(discovery_enabled_value)
        if isinstance(discovery_enabled_value, (int, float))
        else bool(outcome.discovered_candidates)
    )
    if discovery_enabled:
        summary_table.add_row(
            "Discovered / Queued",
            f"{outcome.discovered_candidates} / {outcome.screened_candidates}",
        )
        summary_table.add_row(
            "Queued / Execution-Ready",
            f"{outcome.screened_candidates} / {outcome.eligible_candidates}",
        )
    else:
        summary_table.add_row(
            "Queue Fetched / Selected",
            f"{outcome.screened_candidates} / {outcome.eligible_candidates}",
        )
    summary_table.add_row(
        "Executed / Followed",
        f"{outcome.executed_candidates} / {outcome.followed_candidates}",
    )
    summary_table.add_row(
        "Follow Attempted / Verified",
        f"{outcome.follow_actions_attempted} / {outcome.follow_actions_verified}",
    )
    summary_table.add_row(
        "Clap Attempted / Verified",
        f"{outcome.clap_actions_attempted} / {outcome.clap_actions_verified}",
    )
    if (
        outcome.growth_policy in {GrowthPolicy.WARM_ENGAGE_COMMENT, GrowthPolicy.WARM_ENGAGE_HIGHLIGHT}
        or outcome.public_touch_actions_attempted > 0
        or outcome.public_touch_actions_verified > 0
    ):
        summary_table.add_row(
            "Public Touch Attempted / Verified",
            f"{outcome.public_touch_actions_attempted} / {outcome.public_touch_actions_verified}",
        )
    if (
        outcome.growth_policy == GrowthPolicy.WARM_ENGAGE_COMMENT
        or outcome.comment_actions_attempted > 0
        or outcome.comment_actions_verified > 0
    ):
        summary_table.add_row(
            "Comment Attempted / Verified",
            f"{outcome.comment_actions_attempted} / {outcome.comment_actions_verified}",
        )
    if (
        outcome.growth_policy == GrowthPolicy.WARM_ENGAGE_HIGHLIGHT
        or outcome.highlight_actions_attempted > 0
        or outcome.highlight_actions_verified > 0
    ):
        summary_table.add_row(
            "Highlight Attempted / Verified",
            f"{outcome.highlight_actions_attempted} / {outcome.highlight_actions_verified}",
        )
    if outcome.cleanup_only_mode or outcome.cleanup_actions_attempted > 0 or outcome.cleanup_actions_verified > 0:
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
        source_table = Table(title="Source Screened Candidate Counts")
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

    if outcome.conversion_by_source:
        _render_mapping_table(
            title="Followback Conversion By Source",
            data=outcome.conversion_by_source,
            key_column="Source",
            value_column="Metrics",
        )

    if outcome.conversion_by_policy:
        _render_mapping_table(
            title="Followback Conversion By Policy",
            data=outcome.conversion_by_policy,
            key_column="Policy",
            value_column="Metrics",
        )

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


def _render_graph_sync_outcome(outcome: GraphSyncOutcome) -> None:
    table = Table(title="Graph Sync")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Mode", outcome.mode)
    table.add_row("Dry Run", "true" if outcome.dry_run else "false")
    table.add_row("Skipped", "true" if outcome.skipped else "false")
    if outcome.skipped:
        table.add_row("Skip Reason", outcome.skip_reason or "-")
    table.add_row("Run ID", str(outcome.run_id or "-"))
    table.add_row("Followers Synced", str(outcome.followers_count))
    table.add_row("Following Synced", str(outcome.following_count))
    table.add_row("Users Upserted", str(outcome.users_upserted_count))
    table.add_row("Imported Pending", str(outcome.imported_pending_count))
    table.add_row("Following Source", outcome.used_following_source or "-")
    table.add_row("Duration (ms)", str(outcome.duration_ms))
    console.print(table)


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
    client_metrics_override: dict[str, Any] | None = None,
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
        "client_metrics": client_metrics_override or {},
        "source_candidate_counts": {},
        "source_follow_verified_counts": {},
        "policy_follow_verified_counts": {},
    }
    if outcome is None:
        return payload

    payload["summary"] = {
        "budget_exhausted": outcome.budget_exhausted,
        "actions_today": outcome.actions_today,
        "max_actions_per_day": outcome.max_actions_per_day,
        "cleanup_only_mode": outcome.cleanup_only_mode,
        "growth_policy": outcome.growth_policy.value if outcome.growth_policy is not None else None,
        "growth_sources": [source.value for source in outcome.growth_sources],
        "growth_mode": outcome.growth_mode.value if outcome.growth_mode is not None else None,
        "discovery_mode": outcome.discovery_mode.value if outcome.discovery_mode is not None else None,
        "target_user_refs": outcome.target_user_refs,
        "target_user_scan_limit": outcome.target_user_scan_limit,
        "session_passes": outcome.session_passes,
        "session_elapsed_seconds": outcome.session_elapsed_seconds,
        "session_stop_reason": outcome.session_stop_reason,
        "session_target_follow_attempts": outcome.session_target_follow_attempts,
        "session_target_duration_minutes": outcome.session_target_duration_minutes,
        "discovered_candidates": outcome.discovered_candidates,
        "screened_candidates": outcome.screened_candidates,
        "executed_candidates": outcome.executed_candidates,
        "followed_candidates": outcome.followed_candidates,
        "considered_candidates": outcome.considered_candidates,
        "eligible_candidates": outcome.eligible_candidates,
        "follow_actions_attempted": outcome.follow_actions_attempted,
        "follow_actions_verified": outcome.follow_actions_verified,
        "clap_actions_attempted": outcome.clap_actions_attempted,
        "clap_actions_verified": outcome.clap_actions_verified,
        "public_touch_actions_attempted": outcome.public_touch_actions_attempted,
        "public_touch_actions_verified": outcome.public_touch_actions_verified,
        "comment_actions_attempted": outcome.comment_actions_attempted,
        "comment_actions_verified": outcome.comment_actions_verified,
        "highlight_actions_attempted": outcome.highlight_actions_attempted,
        "highlight_actions_verified": outcome.highlight_actions_verified,
        "cleanup_actions_attempted": outcome.cleanup_actions_attempted,
        "cleanup_actions_verified": outcome.cleanup_actions_verified,
    }
    payload["action_counts"] = outcome.action_counts_today
    payload["result_counts"] = outcome.decision_result_counts
    payload["reason_counts"] = outcome.decision_reason_counts
    payload["kpis"] = outcome.kpis
    payload["client_metrics"] = outcome.client_metrics or client_metrics_override or {}
    payload["source_candidate_counts"] = outcome.source_candidate_counts
    payload["source_follow_verified_counts"] = outcome.source_follow_verified_counts
    payload["policy_follow_verified_counts"] = outcome.policy_follow_verified_counts
    payload["conversion_by_source"] = outcome.conversion_by_source
    payload["conversion_by_policy"] = outcome.conversion_by_policy
    payload["conversion_by_source_policy"] = outcome.conversion_by_source_policy
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


def _build_standard_artifact_payload(
    *,
    run_id: str,
    command: str,
    started_at: datetime,
    ended_at: datetime,
    tag_slug: str,
    dry_run: bool | None,
    status: str,
    summary: dict[str, Any] | None = None,
    action_counts: dict[str, int] | None = None,
    result_counts: dict[str, int] | None = None,
    reason_counts: dict[str, int] | None = None,
    kpis: dict[str, float | int] | None = None,
    client_metrics: dict[str, Any] | None = None,
    error: dict[str, Any] | None = None,
) -> dict[str, Any]:
    duration_ms = int((ended_at - started_at).total_seconds() * 1000)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "command": command,
        "tag_slug": tag_slug,
        "dry_run": dry_run,
        "status": status,
        "health": "failed" if status == "failed" else "degraded" if status == "halted" else "healthy",
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_ms": duration_ms,
        "summary": summary or {},
        "action_counts": action_counts or {},
        "result_counts": result_counts or {},
        "reason_counts": reason_counts or {},
        "kpis": kpis or {},
        "client_metrics": client_metrics or {},
        "error": error,
    }
    return payload


def _render_status(artifact: dict, *, artifact_path: Path, settings: AppSettings | None = None) -> None:
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
            "growth_policy",
            "growth_sources",
            "growth_mode",
            "discovery_mode",
            "target_user_refs",
            "target_user_scan_limit",
            "session_passes",
            "session_elapsed_seconds",
            "session_stop_reason",
            "session_target_follow_attempts",
            "session_target_duration_minutes",
            "discovered_candidates",
            "screened_candidates",
            "executed_candidates",
            "followed_candidates",
            "considered_candidates",
            "eligible_candidates",
            "follow_actions_attempted",
            "follow_actions_verified",
            "clap_actions_attempted",
            "clap_actions_verified",
            "public_touch_actions_attempted",
            "public_touch_actions_verified",
            "comment_actions_attempted",
            "comment_actions_verified",
            "highlight_actions_attempted",
            "highlight_actions_verified",
            "cleanup_actions_attempted",
            "cleanup_actions_verified",
        ):
            if key in summary:
                value = summary[key]
                if key == "growth_sources" and isinstance(value, list):
                    rendered_value = _format_growth_sources([str(item) for item in value])
                elif key == "growth_policy":
                    rendered_value = _format_growth_policy(str(value))
                elif key == "growth_mode":
                    rendered_value = _format_growth_mode(str(value))
                elif key == "discovery_mode":
                    rendered_value = _format_discovery_mode(str(value))
                elif key == "target_user_refs" and isinstance(value, list):
                    rendered_value = _seed_refs_summary([str(item) for item in value])
                else:
                    rendered_value = str(value)
                summary_table.add_row(_format_metric_key(key), rendered_value)
        console.print(summary_table)

    for title, key in (
        ("Action Counts", "action_counts"),
        ("Decision Result Counts", "result_counts"),
        ("Decision Reason Counts", "reason_counts"),
        ("KPI Summary", "kpis"),
        ("Client Metrics", "client_metrics"),
        ("Source Screened Candidate Counts", "source_candidate_counts"),
        ("Source Verified Follow Counts", "source_follow_verified_counts"),
        ("Policy Verified Follow Counts", "policy_follow_verified_counts"),
        ("Followback Conversion By Source", "conversion_by_source"),
        ("Followback Conversion By Policy", "conversion_by_policy"),
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

    if settings is None:
        return

    try:
        _, repository = _build_runner(settings)
        _render_growth_queue_status(
            repository.growth_queue_state_counts(),
            title="Growth Queue Control Panel",
        )
    except Exception as exc:  # noqa: BLE001
        _print_notice(f"Failed to load queue status: {exc}", level="warning")

    growth_defaults = Table(title="Configured Growth Defaults")
    growth_defaults.add_column("Key")
    growth_defaults.add_column("Value")
    growth_defaults.add_row("Growth Policy", _format_growth_policy(settings.default_growth_policy))
    growth_defaults.add_row("Growth Sources", _format_growth_sources(settings.default_growth_sources))
    growth_defaults.add_row("Target-User Followers Scan Limit", str(settings.target_user_followers_scan_limit))
    growth_defaults.add_row("Discovery Eligible / Run", str(settings.discovery_eligible_per_run))
    growth_defaults.add_row("Growth Candidate DB Cap", str(settings.growth_candidate_queue_max_size))
    growth_defaults.add_row("Follow Cooldown (h)", str(settings.follow_cooldown_hours))
    growth_defaults.add_row(
        "Candidate Followers Range",
        f"{settings.candidate_min_followers}-{settings.candidate_max_followers if settings.candidate_max_followers > 0 else 'unbounded'}",
    )
    growth_defaults.add_row(
        "Candidate Following Range",
        f"{settings.candidate_min_following}-{settings.candidate_max_following if settings.candidate_max_following > 0 else 'unbounded'}",
    )
    growth_defaults.add_row("Max Following/Follower Ratio", str(settings.max_following_follower_ratio))
    growth_defaults.add_row("Require Candidate Bio", "true" if settings.require_candidate_bio else "false")
    growth_defaults.add_row("Require Candidate Latest Post", "true" if settings.require_candidate_latest_post else "false")
    growth_defaults.add_row("Candidate Recent Activity (d)", str(settings.candidate_recent_activity_days))
    growth_defaults.add_row("Discovery Followers Depth", str(settings.discovery_followers_depth))
    growth_defaults.add_row("Discovery Seed Followers Limit", str(settings.discovery_seed_followers_limit))
    growth_defaults.add_row("Discovery Second-Hop Seed Limit", str(settings.discovery_second_hop_seed_limit))
    growth_defaults.add_row(
        "Queue Buffer Target",
        (
            f"min={settings.growth_queue_buffer_target_min}, "
            f"max={settings.growth_queue_buffer_target_max}, "
            f"x{settings.growth_queue_buffer_target_multiplier}"
        ),
    )
    growth_defaults.add_row(
        "Queue Fetch Limit",
        (
            f"min={settings.growth_queue_fetch_limit_min}, "
            f"max={settings.growth_queue_fetch_limit_max}, "
            f"x{settings.growth_queue_fetch_limit_multiplier}"
        ),
    )
    growth_defaults.add_row("Queue Due Deferred Reserve", str(round(settings.growth_queue_due_deferred_reserve_ratio, 3)))
    growth_defaults.add_row(
        "Queue Retry Started (s)",
        (
            f"floor={settings.growth_queue_retry_started_floor_seconds}, "
            f"x{settings.growth_queue_retry_started_cooldown_multiplier}"
        ),
    )
    growth_defaults.add_row(
        "Queue Retry Short (s)",
        f"floor={settings.growth_queue_retry_short_floor_seconds}, x{settings.growth_queue_retry_short_cooldown_multiplier}",
    )
    growth_defaults.add_row(
        "Queue Retry Medium (s)",
        f"floor={settings.growth_queue_retry_medium_floor_seconds}, x{settings.growth_queue_retry_medium_cooldown_multiplier}",
    )
    growth_defaults.add_row(
        "Queue Retry Long (s)",
        f"floor={settings.growth_queue_retry_long_floor_seconds}, x{settings.growth_queue_retry_long_cooldown_multiplier}",
    )
    growth_defaults.add_row(
        "Queue Prune TTL (d)",
        (
            f"followed={settings.growth_queue_prune_followed_after_days}, "
            f"rejected={settings.growth_queue_prune_rejected_after_days}, "
            f"stale={settings.growth_queue_prune_stale_after_days}"
        ),
    )
    growth_defaults.add_row(
        "DB Hygiene TTL (d)",
        (
            f"action_log={settings.db_hygiene_action_log_retention_days}, "
            f"graph_sync_runs={settings.db_hygiene_graph_sync_runs_retention_days}, "
            f"candidate_reconciliation={settings.db_hygiene_candidate_reconciliation_retention_days}, "
            f"follow_cycle={settings.db_hygiene_follow_cycle_terminal_retention_days}, "
            f"snapshots={settings.db_hygiene_snapshots_retention_days}"
        ),
    )
    growth_defaults.add_row("DB Hygiene Vacuum", "true" if settings.db_hygiene_vacuum_after_cleanup else "false")
    growth_defaults.add_row("Pre-Follow Clap", "true" if settings.enable_pre_follow_clap else "false")
    growth_defaults.add_row("Pre-Follow Comment", "true" if settings.enable_pre_follow_comment else "false")
    growth_defaults.add_row("Pre-Follow Comment Probability", str(round(settings.pre_follow_comment_probability, 3)))
    growth_defaults.add_row("Pre-Follow Highlight", "true" if settings.enable_pre_follow_highlight else "false")
    growth_defaults.add_row("Pre-Follow Highlight Probability", str(round(settings.pre_follow_highlight_probability, 3)))
    growth_defaults.add_row("Live Session Duration (m)", str(settings.live_session_duration_minutes))
    growth_defaults.add_row("Live Session Target Follows", str(settings.live_session_target_follow_attempts))
    growth_defaults.add_row("Live Session Min Follows", str(settings.live_session_min_follow_attempts))
    growth_defaults.add_row("Live Session Max Passes", str(settings.live_session_max_passes))
    growth_defaults.add_row("Max Follow Actions Per Cycle", str(settings.max_follow_actions_per_run))
    growth_defaults.add_row("Max Subscribe Actions Per Day", str(settings.max_subscribe_actions_per_day))
    growth_defaults.add_row("Max Comment Actions Per Day", str(settings.max_comment_actions_per_day))
    growth_defaults.add_row("Max Highlight Actions Per Day", str(settings.max_highlight_actions_per_day))
    growth_defaults.add_row("Max Mutations / 10m", str(settings.max_mutations_per_10_minutes))
    growth_defaults.add_row("Verify Gap (s)", f"{settings.min_verify_gap_seconds}-{settings.max_verify_gap_seconds}")
    growth_defaults.add_row("Pass Cooldown (s)", f"{settings.pass_cooldown_min_seconds}-{settings.pass_cooldown_max_seconds}")
    growth_defaults.add_row("Soft-Degrade Cooldown (s)", str(settings.pacing_soft_degrade_cooldown_seconds))
    growth_defaults.add_row("Pacing Auto-Clamp", "true" if settings.enable_pacing_auto_clamp else "false")
    console.print(growth_defaults)


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
    max_comment = int(
        typer.prompt("Max comment actions per day", default=settings.max_comment_actions_per_day, type=int)
    )
    max_highlight = int(
        typer.prompt("Max highlight actions per day", default=settings.max_highlight_actions_per_day, type=int)
    )
    max_follow_per_run = int(
        typer.prompt("Max follow actions per run", default=settings.max_follow_actions_per_run, type=int)
    )
    default_growth_policy = _prompt_growth_policy_choice(
        f"Default growth policy ({_GROWTH_POLICY_CHOICES})",
        default=settings.default_growth_policy,
    )
    default_growth_sources = _prompt_growth_sources_choice(
        f"Default growth sources ({_GROWTH_SOURCE_CHOICES}; comma-separated)",
        default=list(settings.default_growth_sources),
    )
    candidate_min_followers = int(
        typer.prompt("Candidate minimum followers", default=settings.candidate_min_followers, type=int)
    )
    candidate_max_followers = int(
        typer.prompt("Candidate maximum followers (0 disables)", default=settings.candidate_max_followers, type=int)
    )
    candidate_min_following = int(
        typer.prompt("Candidate minimum following", default=settings.candidate_min_following, type=int)
    )
    candidate_max_following = int(
        typer.prompt("Candidate maximum following (0 disables)", default=settings.candidate_max_following, type=int)
    )
    max_following_follower_ratio = float(
        typer.prompt(
            "Candidate maximum following/follower ratio",
            default=settings.max_following_follower_ratio,
            type=float,
        )
    )
    require_candidate_bio = typer.confirm(
        "Require candidate bio?",
        default=settings.require_candidate_bio,
    )
    require_candidate_latest_post = typer.confirm(
        "Require candidate latest post?",
        default=settings.require_candidate_latest_post,
    )
    candidate_recent_activity_days = int(
        typer.prompt(
            "Candidate recent activity days (0 disables)",
            default=settings.candidate_recent_activity_days,
            type=int,
        )
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
    discovery_eligible_per_run = int(
        typer.prompt("Discovery eligible users per run", default=settings.discovery_eligible_per_run, type=int)
    )
    growth_candidate_queue_max_size = int(
        typer.prompt("Growth candidate DB cap", default=settings.growth_candidate_queue_max_size, type=int)
    )
    growth_queue_buffer_target_min = int(
        typer.prompt(
            "Queue buffer target min",
            default=settings.growth_queue_buffer_target_min,
            type=int,
        )
    )
    growth_queue_buffer_target_max = int(
        typer.prompt(
            "Queue buffer target max",
            default=settings.growth_queue_buffer_target_max,
            type=int,
        )
    )
    growth_queue_buffer_target_multiplier = int(
        typer.prompt(
            "Queue buffer target multiplier",
            default=settings.growth_queue_buffer_target_multiplier,
            type=int,
        )
    )
    growth_queue_fetch_limit_min = int(
        typer.prompt(
            "Queue fetch limit min",
            default=settings.growth_queue_fetch_limit_min,
            type=int,
        )
    )
    growth_queue_fetch_limit_max = int(
        typer.prompt(
            "Queue fetch limit max",
            default=settings.growth_queue_fetch_limit_max,
            type=int,
        )
    )
    growth_queue_fetch_limit_multiplier = int(
        typer.prompt(
            "Queue fetch limit multiplier",
            default=settings.growth_queue_fetch_limit_multiplier,
            type=int,
        )
    )
    growth_queue_due_deferred_reserve_ratio = float(
        typer.prompt(
            "Queue due-deferred reserve ratio (0.0-0.9)",
            default=settings.growth_queue_due_deferred_reserve_ratio,
            type=float,
        )
    )
    growth_queue_due_deferred_reserve_ratio = max(0.0, min(0.9, growth_queue_due_deferred_reserve_ratio))
    growth_queue_retry_started_floor_seconds = int(
        typer.prompt(
            "Queue retry started floor seconds",
            default=settings.growth_queue_retry_started_floor_seconds,
            type=int,
        )
    )
    growth_queue_retry_started_cooldown_multiplier = int(
        typer.prompt(
            "Queue retry started cooldown multiplier",
            default=settings.growth_queue_retry_started_cooldown_multiplier,
            type=int,
        )
    )
    growth_queue_retry_short_floor_seconds = int(
        typer.prompt(
            "Queue retry short floor seconds",
            default=settings.growth_queue_retry_short_floor_seconds,
            type=int,
        )
    )
    growth_queue_retry_short_cooldown_multiplier = int(
        typer.prompt(
            "Queue retry short cooldown multiplier",
            default=settings.growth_queue_retry_short_cooldown_multiplier,
            type=int,
        )
    )
    growth_queue_retry_medium_floor_seconds = int(
        typer.prompt(
            "Queue retry medium floor seconds",
            default=settings.growth_queue_retry_medium_floor_seconds,
            type=int,
        )
    )
    growth_queue_retry_medium_cooldown_multiplier = int(
        typer.prompt(
            "Queue retry medium cooldown multiplier",
            default=settings.growth_queue_retry_medium_cooldown_multiplier,
            type=int,
        )
    )
    growth_queue_retry_long_floor_seconds = int(
        typer.prompt(
            "Queue retry long floor seconds",
            default=settings.growth_queue_retry_long_floor_seconds,
            type=int,
        )
    )
    growth_queue_retry_long_cooldown_multiplier = int(
        typer.prompt(
            "Queue retry long cooldown multiplier",
            default=settings.growth_queue_retry_long_cooldown_multiplier,
            type=int,
        )
    )
    growth_queue_prune_followed_after_days = int(
        typer.prompt(
            "Queue prune followed-after days (0 keeps none)",
            default=settings.growth_queue_prune_followed_after_days,
            type=int,
        )
    )
    growth_queue_prune_rejected_after_days = int(
        typer.prompt(
            "Queue prune rejected-after days (0 keeps none)",
            default=settings.growth_queue_prune_rejected_after_days,
            type=int,
        )
    )
    growth_queue_prune_stale_after_days = int(
        typer.prompt(
            "Queue prune stale queued/deferred-after days (0 keeps none)",
            default=settings.growth_queue_prune_stale_after_days,
            type=int,
        )
    )
    target_user_followers_scan_limit = int(
        typer.prompt(
            "Target-user followers scan limit per source user",
            default=settings.target_user_followers_scan_limit,
            type=int,
        )
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
    enable_pre_follow_comment = typer.confirm(
        "Enable optional pre-follow comment for comment policy?",
        default=settings.enable_pre_follow_comment,
    )
    pre_follow_comment_probability = float(
        typer.prompt(
            "Pre-follow comment probability per comment-policy candidate (0.0-1.0)",
            default=settings.pre_follow_comment_probability,
            type=float,
        )
    )
    pre_follow_comment_probability = max(0.0, min(1.0, pre_follow_comment_probability))
    enable_pre_follow_highlight = typer.confirm(
        "Enable optional pre-follow highlight for highlight policy?",
        default=settings.enable_pre_follow_highlight,
    )
    pre_follow_highlight_probability = float(
        typer.prompt(
            "Pre-follow highlight probability per highlight-policy candidate (0.0-1.0)",
            default=settings.pre_follow_highlight_probability,
            type=float,
        )
    )
    pre_follow_highlight_probability = max(0.0, min(1.0, pre_follow_highlight_probability))
    comment_templates_default = " || ".join(settings.pre_follow_comment_templates)
    pre_follow_comment_templates_raw = str(
        typer.prompt(
            "Pre-follow comment templates (`||` separated)",
            default=comment_templates_default,
        )
    ).strip()
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
    graph_sync_auto_enabled = typer.confirm(
        "Enable graph sync auto-run for growth, unfollow, and reconcile flows?",
        default=settings.graph_sync_auto_enabled,
    )
    graph_sync_freshness_window_minutes = int(
        typer.prompt(
            "Graph sync freshness window minutes",
            default=settings.graph_sync_freshness_window_minutes,
            type=int,
        )
    )
    graph_sync_full_pagination = typer.confirm(
        "Use full pagination during graph sync?",
        default=settings.graph_sync_full_pagination,
    )
    graph_sync_enable_graphql_following = typer.confirm(
        "Enable GraphQL strategy for following import?",
        default=settings.graph_sync_enable_graphql_following,
    )
    graph_sync_enable_scrape_fallback = typer.confirm(
        "Enable scrape fallback for following import?",
        default=settings.graph_sync_enable_scrape_fallback,
    )
    graph_sync_scrape_page_timeout_seconds = int(
        typer.prompt(
            "Graph sync scrape timeout seconds",
            default=settings.graph_sync_scrape_page_timeout_seconds,
            type=int,
        )
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
        "MAX_COMMENT_ACTIONS_PER_DAY": str(max(0, max_comment)),
        "MAX_HIGHLIGHT_ACTIONS_PER_DAY": str(max(0, max_highlight)),
        "MAX_FOLLOW_ACTIONS_PER_RUN": str(max_follow_per_run),
        "DEFAULT_GROWTH_POLICY": default_growth_policy.value,
        "DEFAULT_GROWTH_SOURCES": ",".join(source.value for source in default_growth_sources),
        "LIVE_SESSION_DURATION_MINUTES": str(max(1, live_session_duration_minutes)),
        "LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS": str(max(1, live_session_target_follow_attempts)),
        "LIVE_SESSION_MIN_FOLLOW_ATTEMPTS": str(
            min(max(1, live_session_min_follow_attempts), max(1, live_session_target_follow_attempts))
        ),
        "LIVE_SESSION_MAX_PASSES": str(max(1, live_session_max_passes)),
        "DISCOVERY_ELIGIBLE_PER_RUN": str(max(1, discovery_eligible_per_run)),
        "GROWTH_CANDIDATE_QUEUE_MAX_SIZE": str(max(1, growth_candidate_queue_max_size)),
        "GROWTH_QUEUE_BUFFER_TARGET_MIN": str(max(1, growth_queue_buffer_target_min)),
        "GROWTH_QUEUE_BUFFER_TARGET_MAX": str(
            max(max(1, growth_queue_buffer_target_min), max(1, growth_queue_buffer_target_max))
        ),
        "GROWTH_QUEUE_BUFFER_TARGET_MULTIPLIER": str(max(1, growth_queue_buffer_target_multiplier)),
        "GROWTH_QUEUE_FETCH_LIMIT_MIN": str(max(1, growth_queue_fetch_limit_min)),
        "GROWTH_QUEUE_FETCH_LIMIT_MAX": str(
            max(max(1, growth_queue_fetch_limit_min), max(1, growth_queue_fetch_limit_max))
        ),
        "GROWTH_QUEUE_FETCH_LIMIT_MULTIPLIER": str(max(1, growth_queue_fetch_limit_multiplier)),
        "GROWTH_QUEUE_DUE_DEFERRED_RESERVE_RATIO": str(max(0.0, min(0.9, growth_queue_due_deferred_reserve_ratio))),
        "GROWTH_QUEUE_RETRY_STARTED_FLOOR_SECONDS": str(max(0, growth_queue_retry_started_floor_seconds)),
        "GROWTH_QUEUE_RETRY_STARTED_COOLDOWN_MULTIPLIER": str(max(0, growth_queue_retry_started_cooldown_multiplier)),
        "GROWTH_QUEUE_RETRY_SHORT_FLOOR_SECONDS": str(max(0, growth_queue_retry_short_floor_seconds)),
        "GROWTH_QUEUE_RETRY_SHORT_COOLDOWN_MULTIPLIER": str(max(0, growth_queue_retry_short_cooldown_multiplier)),
        "GROWTH_QUEUE_RETRY_MEDIUM_FLOOR_SECONDS": str(
            max(max(0, growth_queue_retry_short_floor_seconds), max(0, growth_queue_retry_medium_floor_seconds))
        ),
        "GROWTH_QUEUE_RETRY_MEDIUM_COOLDOWN_MULTIPLIER": str(max(0, growth_queue_retry_medium_cooldown_multiplier)),
        "GROWTH_QUEUE_RETRY_LONG_FLOOR_SECONDS": str(
            max(max(0, growth_queue_retry_medium_floor_seconds), max(0, growth_queue_retry_long_floor_seconds))
        ),
        "GROWTH_QUEUE_RETRY_LONG_COOLDOWN_MULTIPLIER": str(max(0, growth_queue_retry_long_cooldown_multiplier)),
        "GROWTH_QUEUE_PRUNE_FOLLOWED_AFTER_DAYS": str(max(0, growth_queue_prune_followed_after_days)),
        "GROWTH_QUEUE_PRUNE_REJECTED_AFTER_DAYS": str(max(0, growth_queue_prune_rejected_after_days)),
        "GROWTH_QUEUE_PRUNE_STALE_AFTER_DAYS": str(max(0, growth_queue_prune_stale_after_days)),
        "TARGET_USER_FOLLOWERS_SCAN_LIMIT": str(max(1, target_user_followers_scan_limit)),
        "FOLLOW_COOLDOWN_HOURS": str(max(1, follow_cooldown_hours)),
        "CANDIDATE_MIN_FOLLOWERS": str(max(0, candidate_min_followers)),
        "CANDIDATE_MAX_FOLLOWERS": str(max(0, candidate_max_followers)),
        "CANDIDATE_MIN_FOLLOWING": str(max(0, candidate_min_following)),
        "CANDIDATE_MAX_FOLLOWING": str(max(0, candidate_max_following)),
        "MAX_FOLLOWING_FOLLOWER_RATIO": str(max(max(0.0, settings.min_following_follower_ratio), max_following_follower_ratio)),
        "REQUIRE_CANDIDATE_BIO": "true" if require_candidate_bio else "false",
        "REQUIRE_CANDIDATE_LATEST_POST": "true" if require_candidate_latest_post else "false",
        "CANDIDATE_RECENT_ACTIVITY_DAYS": str(max(0, candidate_recent_activity_days)),
        "DISCOVERY_FOLLOWERS_DEPTH": str(discovery_depth),
        "DISCOVERY_SEED_FOLLOWERS_LIMIT": str(seed_followers_limit),
        "DISCOVERY_SECOND_HOP_SEED_LIMIT": str(second_hop_seed_limit),
        "DISCOVERY_SEED_USERS": seed_users_raw,
        "ENABLE_PRE_FOLLOW_CLAP": "true" if enable_pre_follow_clap else "false",
        "ENABLE_PRE_FOLLOW_COMMENT": "true" if enable_pre_follow_comment else "false",
        "PRE_FOLLOW_COMMENT_PROBABILITY": str(pre_follow_comment_probability),
        "ENABLE_PRE_FOLLOW_HIGHLIGHT": "true" if enable_pre_follow_highlight else "false",
        "PRE_FOLLOW_HIGHLIGHT_PROBABILITY": str(pre_follow_highlight_probability),
        "PRE_FOLLOW_COMMENT_TEMPLATES": pre_follow_comment_templates_raw,
        "MAX_MUTATIONS_PER_10_MINUTES": str(max(1, max_mutations_per_10_minutes)),
        "MIN_VERIFY_GAP_SECONDS": str(max(0, min_verify_gap_seconds)),
        "MAX_VERIFY_GAP_SECONDS": str(max(max(0, min_verify_gap_seconds), max(0, max_verify_gap_seconds))),
        "PASS_COOLDOWN_MIN_SECONDS": str(max(0, pass_cooldown_min_seconds)),
        "PASS_COOLDOWN_MAX_SECONDS": str(max(max(0, pass_cooldown_min_seconds), max(0, pass_cooldown_max_seconds))),
        "PACING_SOFT_DEGRADE_COOLDOWN_SECONDS": str(max(0, pacing_soft_degrade_cooldown_seconds)),
        "ENABLE_PACING_AUTO_CLAMP": "true" if enable_pacing_auto_clamp else "false",
        "GRAPH_SYNC_AUTO_ENABLED": "true" if graph_sync_auto_enabled else "false",
        "GRAPH_SYNC_FRESHNESS_WINDOW_MINUTES": str(max(0, graph_sync_freshness_window_minutes)),
        "GRAPH_SYNC_FULL_PAGINATION": "true" if graph_sync_full_pagination else "false",
        "GRAPH_SYNC_ENABLE_GRAPHQL_FOLLOWING": "true" if graph_sync_enable_graphql_following else "false",
        "GRAPH_SYNC_ENABLE_SCRAPE_FALLBACK": "true" if graph_sync_enable_scrape_fallback else "false",
        "GRAPH_SYNC_SCRAPE_PAGE_TIMEOUT_SECONDS": str(max(5, graph_sync_scrape_page_timeout_seconds)),
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


def _coerce_optioninfo(value: Any, *, default: Any) -> Any:
    return default if isinstance(value, OptionInfo) else value


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
    queue_ready_count: int,
    queue_deferred_count: int,
    queue_rejected_count: int,
    queue_followed_count: int,
    tag_slug: str,
    seed_user_refs: list[str] | None,
    live_session_minutes: int,
    live_session_target_follows: int,
    live_session_min_follows: int,
    live_session_max_passes: int,
    max_mutations_per_10_minutes: int,
    max_follow_actions_per_run: int,
    max_subscribe_actions_per_day: int,
    max_comment_actions_per_day: int,
    min_verify_gap_seconds: int,
    max_verify_gap_seconds: int,
    pass_cooldown_min_seconds: int,
    pass_cooldown_max_seconds: int,
    pacing_soft_degrade_cooldown_seconds: int,
    enable_pacing_auto_clamp: bool,
    growth_policy: GrowthPolicy,
    growth_sources: list[GrowthSource],
    target_user_followers_scan_limit: int,
    discovery_eligible_per_run: int,
    growth_candidate_queue_max_size: int,
    follow_cooldown_hours: int,
    candidate_min_followers: int,
    candidate_max_followers: int,
    candidate_min_following: int,
    candidate_max_following: int,
    max_following_follower_ratio: float,
    require_candidate_bio: bool,
    require_candidate_latest_post: bool,
    candidate_recent_activity_days: int,
    discovery_followers_depth: int,
    discovery_seed_followers_limit: int,
    discovery_second_hop_seed_limit: int,
    growth_queue_buffer_target_min: int,
    growth_queue_buffer_target_max: int,
    growth_queue_buffer_target_multiplier: int,
    growth_queue_fetch_limit_min: int,
    growth_queue_fetch_limit_max: int,
    growth_queue_fetch_limit_multiplier: int,
    growth_queue_due_deferred_reserve_ratio: float,
    growth_queue_retry_started_floor_seconds: int,
    growth_queue_retry_started_cooldown_multiplier: int,
    growth_queue_retry_short_floor_seconds: int,
    growth_queue_retry_short_cooldown_multiplier: int,
    growth_queue_retry_medium_floor_seconds: int,
    growth_queue_retry_medium_cooldown_multiplier: int,
    growth_queue_retry_long_floor_seconds: int,
    growth_queue_retry_long_cooldown_multiplier: int,
    growth_queue_prune_followed_after_days: int,
    growth_queue_prune_rejected_after_days: int,
    growth_queue_prune_stale_after_days: int,
    enable_pre_follow_clap: bool,
    enable_pre_follow_comment: bool,
    pre_follow_comment_probability: float,
    enable_pre_follow_highlight: bool,
    pre_follow_highlight_probability: float,
    pre_follow_comment_templates_raw: str,
    graph_sync_auto_enabled: bool,
    graph_sync_freshness_window_minutes: int,
    graph_sync_full_pagination: bool,
    graph_sync_enable_graphql_following: bool,
    graph_sync_enable_scrape_fallback: bool,
    graph_sync_scrape_page_timeout_seconds: int,
    reconcile_limit: int,
    reconcile_page_size: int,
    cleanup_unfollow_limit: int,
    cleanup_whitelist_min_followers: int,
    newsletter_slug: str | None,
    newsletter_username: str | None,
) -> None:
    status_label = (
        "ready"
        if has_session
        else "missing (use Settings/Auth -> Refresh auth session or `uv run bot auth`)"
    )
    _print_notice(
        f"Session status: {status_label}",
        level="success" if has_session else "warning",
    )

    defaults = Table(title="Current Defaults")
    defaults.add_column("Key")
    defaults.add_column("Value")
    defaults.add_row("Tag", tag_slug)
    defaults.add_row("Seed Users", _seed_refs_summary(seed_user_refs))
    defaults.add_row("Growth Policy", _format_growth_policy(growth_policy))
    defaults.add_row("Growth Sources", _format_growth_sources(growth_sources))
    defaults.add_row("Queue Ready", str(queue_ready_count))
    defaults.add_row("Queue Deferred", str(queue_deferred_count))
    defaults.add_row("Queue Rejected", str(queue_rejected_count))
    defaults.add_row("Queue Followed", str(queue_followed_count))
    defaults.add_row("Target-User Followers Scan Limit", str(target_user_followers_scan_limit))
    defaults.add_row("Discovery Eligible / Run", str(discovery_eligible_per_run))
    defaults.add_row("Growth Candidate DB Cap", str(growth_candidate_queue_max_size))
    defaults.add_row("Follow Cooldown (h)", str(follow_cooldown_hours))
    defaults.add_row(
        "Candidate Followers Range",
        f"{candidate_min_followers}-{candidate_max_followers if candidate_max_followers > 0 else 'unbounded'}",
    )
    defaults.add_row(
        "Candidate Following Range",
        f"{candidate_min_following}-{candidate_max_following if candidate_max_following > 0 else 'unbounded'}",
    )
    defaults.add_row("Max Following/Follower Ratio", str(max_following_follower_ratio))
    defaults.add_row("Require Candidate Bio", "true" if require_candidate_bio else "false")
    defaults.add_row("Require Candidate Latest Post", "true" if require_candidate_latest_post else "false")
    defaults.add_row("Candidate Recent Activity (d)", str(candidate_recent_activity_days))
    defaults.add_row("Discovery Followers Depth", str(discovery_followers_depth))
    defaults.add_row("Discovery Seed Followers Limit", str(discovery_seed_followers_limit))
    defaults.add_row("Discovery Second-Hop Seed Limit", str(discovery_second_hop_seed_limit))
    defaults.add_row("Pre-Follow Clap", "true" if enable_pre_follow_clap else "false")
    defaults.add_row("Pre-Follow Comment", "true" if enable_pre_follow_comment else "false")
    defaults.add_row("Pre-Follow Comment Probability", str(round(pre_follow_comment_probability, 3)))
    defaults.add_row("Pre-Follow Highlight", "true" if enable_pre_follow_highlight else "false")
    defaults.add_row("Pre-Follow Highlight Probability", str(round(pre_follow_highlight_probability, 3)))
    defaults.add_row(
        "Pre-Follow Comment Templates",
        str(len([item for item in pre_follow_comment_templates_raw.split("||") if item.strip()])),
    )
    defaults.add_row(
        "Queue Buffer Target",
        (
            f"min={growth_queue_buffer_target_min}, "
            f"max={growth_queue_buffer_target_max}, "
            f"x{growth_queue_buffer_target_multiplier}"
        ),
    )
    defaults.add_row(
        "Queue Fetch Limit",
        (
            f"min={growth_queue_fetch_limit_min}, "
            f"max={growth_queue_fetch_limit_max}, "
            f"x{growth_queue_fetch_limit_multiplier}"
        ),
    )
    defaults.add_row(
        "Queue Due Deferred Reserve",
        str(round(growth_queue_due_deferred_reserve_ratio, 3)),
    )
    defaults.add_row(
        "Queue Retry Started (s)",
        (
            f"floor={growth_queue_retry_started_floor_seconds}, "
            f"x{growth_queue_retry_started_cooldown_multiplier}"
        ),
    )
    defaults.add_row(
        "Queue Retry Short (s)",
        f"floor={growth_queue_retry_short_floor_seconds}, x{growth_queue_retry_short_cooldown_multiplier}",
    )
    defaults.add_row(
        "Queue Retry Medium (s)",
        f"floor={growth_queue_retry_medium_floor_seconds}, x{growth_queue_retry_medium_cooldown_multiplier}",
    )
    defaults.add_row(
        "Queue Retry Long (s)",
        f"floor={growth_queue_retry_long_floor_seconds}, x{growth_queue_retry_long_cooldown_multiplier}",
    )
    defaults.add_row(
        "Queue Prune TTL (d)",
        (
            f"followed={growth_queue_prune_followed_after_days}, "
            f"rejected={growth_queue_prune_rejected_after_days}, "
            f"stale={growth_queue_prune_stale_after_days}"
        ),
    )
    defaults.add_row("Live Session Duration (m)", str(live_session_minutes))
    defaults.add_row("Live Session Target Follows", str(live_session_target_follows))
    defaults.add_row("Live Session Min Follows", str(live_session_min_follows))
    defaults.add_row("Live Session Max Passes", str(live_session_max_passes))
    defaults.add_row("Max Follow Actions Per Cycle", str(max_follow_actions_per_run))
    defaults.add_row("Max Subscribe Actions Per Day", str(max_subscribe_actions_per_day))
    defaults.add_row("Max Comment Actions Per Day", str(max_comment_actions_per_day))
    defaults.add_row("Max Mutations / 10m", str(max_mutations_per_10_minutes))
    defaults.add_row("Verify Gap (s)", f"{min_verify_gap_seconds}-{max_verify_gap_seconds}")
    defaults.add_row("Pass Cooldown (s)", f"{pass_cooldown_min_seconds}-{pass_cooldown_max_seconds}")
    defaults.add_row("Soft-Degrade Cooldown (s)", str(pacing_soft_degrade_cooldown_seconds))
    defaults.add_row("Pacing Auto-Clamp", "true" if enable_pacing_auto_clamp else "false")
    defaults.add_row("Graph Sync Auto", "true" if graph_sync_auto_enabled else "false")
    defaults.add_row("Graph Sync Freshness (m)", str(graph_sync_freshness_window_minutes))
    defaults.add_row("Graph Sync Full Pagination", "true" if graph_sync_full_pagination else "false")
    defaults.add_row(
        "GraphQL Following Source",
        "true" if graph_sync_enable_graphql_following else "false",
    )
    defaults.add_row("Scrape Following Fallback", "true" if graph_sync_enable_scrape_fallback else "false")
    defaults.add_row("Graph Sync Scrape Timeout (s)", str(graph_sync_scrape_page_timeout_seconds))
    defaults.add_row("Cleanup Limit", str(cleanup_unfollow_limit))
    defaults.add_row("Cleanup Whitelist Followers >=", str(cleanup_whitelist_min_followers))
    defaults.add_row("Reconcile Limit", str(reconcile_limit))
    defaults.add_row("Reconcile Page Size", str(reconcile_page_size))
    defaults.add_row("Newsletter Slug", newsletter_slug or "-")
    defaults.add_row("Newsletter Username", newsletter_username or "-")
    console.print(defaults)

    menu = Table(title="Start Menu", header_style="bold white")
    menu.add_column("Option", justify="right", style="bold cyan", no_wrap=True)
    menu.add_column("Section", no_wrap=True)
    menu.add_column("Purpose")
    for option, section, purpose in _START_MENU_SECTIONS:
        section_style = _START_MENU_SECTION_STYLES.get(section, "white")
        menu.add_row(
            option,
            f"[bold {section_style}]{section}[/bold {section_style}]",
            purpose,
        )
    console.print(menu)
    _print_notice("Choose a section number (or `q`) and press Enter.", level="info")


def _render_start_submenu(
    *,
    title: str,
    options: tuple[tuple[str, str, str], ...],
    style: str,
) -> None:
    menu = Table(title=f"{title} Menu", header_style="bold white")
    menu.add_column("Option", justify="right", style=f"bold {style}", no_wrap=True)
    menu.add_column("Mode", style="italic", no_wrap=True)
    menu.add_column("Action")
    for option, mode_label, action in options:
        menu.add_row(option, mode_label, action)
    console.print(menu)
    _print_notice("Choose an option number (`b` to go back, `q` to exit).", level="info")


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
    initial_growth_policy: GrowthPolicy,
    initial_growth_sources: list[GrowthSource],
    initial_target_user_followers_scan_limit: int,
    initial_discovery_eligible_per_run: int,
    initial_growth_candidate_queue_max_size: int,
    initial_follow_cooldown_hours: int,
    initial_candidate_min_followers: int,
    initial_candidate_max_followers: int,
    initial_candidate_min_following: int,
    initial_candidate_max_following: int,
    initial_max_following_follower_ratio: float,
    initial_require_candidate_bio: bool,
    initial_require_candidate_latest_post: bool,
    initial_candidate_recent_activity_days: int,
    initial_discovery_followers_depth: int,
    initial_discovery_seed_followers_limit: int,
    initial_discovery_second_hop_seed_limit: int,
    initial_growth_queue_buffer_target_min: int,
    initial_growth_queue_buffer_target_max: int,
    initial_growth_queue_buffer_target_multiplier: int,
    initial_growth_queue_fetch_limit_min: int,
    initial_growth_queue_fetch_limit_max: int,
    initial_growth_queue_fetch_limit_multiplier: int,
    initial_growth_queue_due_deferred_reserve_ratio: float,
    initial_growth_queue_retry_started_floor_seconds: int,
    initial_growth_queue_retry_started_cooldown_multiplier: int,
    initial_growth_queue_retry_short_floor_seconds: int,
    initial_growth_queue_retry_short_cooldown_multiplier: int,
    initial_growth_queue_retry_medium_floor_seconds: int,
    initial_growth_queue_retry_medium_cooldown_multiplier: int,
    initial_growth_queue_retry_long_floor_seconds: int,
    initial_growth_queue_retry_long_cooldown_multiplier: int,
    initial_growth_queue_prune_followed_after_days: int,
    initial_growth_queue_prune_rejected_after_days: int,
    initial_growth_queue_prune_stale_after_days: int,
    initial_enable_pre_follow_clap: bool,
    initial_enable_pre_follow_comment: bool,
    initial_pre_follow_comment_probability: float,
    initial_enable_pre_follow_highlight: bool,
    initial_pre_follow_highlight_probability: float,
    initial_pre_follow_comment_templates_raw: str,
    initial_graph_sync_auto_enabled: bool,
    initial_graph_sync_freshness_window_minutes: int,
    initial_graph_sync_full_pagination: bool,
    initial_graph_sync_enable_graphql_following: bool,
    initial_graph_sync_enable_scrape_fallback: bool,
    initial_graph_sync_scrape_page_timeout_seconds: int,
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
    growth_policy = initial_growth_policy
    growth_sources = list(initial_growth_sources)
    target_user_followers_scan_limit = max(1, initial_target_user_followers_scan_limit)
    discovery_eligible_per_run = max(1, initial_discovery_eligible_per_run)
    growth_candidate_queue_max_size = max(1, initial_growth_candidate_queue_max_size)
    follow_cooldown_hours = max(1, initial_follow_cooldown_hours)
    candidate_min_followers = max(0, initial_candidate_min_followers)
    candidate_max_followers = max(0, initial_candidate_max_followers)
    candidate_min_following = max(0, initial_candidate_min_following)
    candidate_max_following = max(0, initial_candidate_max_following)
    max_following_follower_ratio = max(0.0, initial_max_following_follower_ratio)
    require_candidate_bio = initial_require_candidate_bio
    require_candidate_latest_post = initial_require_candidate_latest_post
    candidate_recent_activity_days = max(0, initial_candidate_recent_activity_days)
    discovery_followers_depth = 2 if initial_discovery_followers_depth == 2 else 1
    discovery_seed_followers_limit = max(1, initial_discovery_seed_followers_limit)
    discovery_second_hop_seed_limit = max(1, initial_discovery_second_hop_seed_limit)
    growth_queue_buffer_target_min = max(1, initial_growth_queue_buffer_target_min)
    growth_queue_buffer_target_max = max(growth_queue_buffer_target_min, initial_growth_queue_buffer_target_max)
    growth_queue_buffer_target_multiplier = max(1, initial_growth_queue_buffer_target_multiplier)
    growth_queue_fetch_limit_min = max(1, initial_growth_queue_fetch_limit_min)
    growth_queue_fetch_limit_max = max(growth_queue_fetch_limit_min, initial_growth_queue_fetch_limit_max)
    growth_queue_fetch_limit_multiplier = max(1, initial_growth_queue_fetch_limit_multiplier)
    growth_queue_due_deferred_reserve_ratio = max(0.0, min(0.9, initial_growth_queue_due_deferred_reserve_ratio))
    growth_queue_retry_started_floor_seconds = max(0, initial_growth_queue_retry_started_floor_seconds)
    growth_queue_retry_started_cooldown_multiplier = max(0, initial_growth_queue_retry_started_cooldown_multiplier)
    growth_queue_retry_short_floor_seconds = max(0, initial_growth_queue_retry_short_floor_seconds)
    growth_queue_retry_short_cooldown_multiplier = max(0, initial_growth_queue_retry_short_cooldown_multiplier)
    growth_queue_retry_medium_floor_seconds = max(
        growth_queue_retry_short_floor_seconds,
        initial_growth_queue_retry_medium_floor_seconds,
    )
    growth_queue_retry_medium_cooldown_multiplier = max(0, initial_growth_queue_retry_medium_cooldown_multiplier)
    growth_queue_retry_long_floor_seconds = max(
        growth_queue_retry_medium_floor_seconds,
        initial_growth_queue_retry_long_floor_seconds,
    )
    growth_queue_retry_long_cooldown_multiplier = max(0, initial_growth_queue_retry_long_cooldown_multiplier)
    growth_queue_prune_followed_after_days = max(0, initial_growth_queue_prune_followed_after_days)
    growth_queue_prune_rejected_after_days = max(0, initial_growth_queue_prune_rejected_after_days)
    growth_queue_prune_stale_after_days = max(0, initial_growth_queue_prune_stale_after_days)
    enable_pre_follow_clap = initial_enable_pre_follow_clap
    enable_pre_follow_comment = initial_enable_pre_follow_comment
    pre_follow_comment_probability = max(0.0, min(1.0, initial_pre_follow_comment_probability))
    enable_pre_follow_highlight = initial_enable_pre_follow_highlight
    pre_follow_highlight_probability = max(0.0, min(1.0, initial_pre_follow_highlight_probability))
    pre_follow_comment_templates_raw = initial_pre_follow_comment_templates_raw
    target_user_refs_for_growth: list[str] | None = None
    graph_sync_auto_enabled = initial_graph_sync_auto_enabled
    graph_sync_freshness_window_minutes = max(0, initial_graph_sync_freshness_window_minutes)
    graph_sync_full_pagination = initial_graph_sync_full_pagination
    graph_sync_enable_graphql_following = initial_graph_sync_enable_graphql_following
    graph_sync_enable_scrape_fallback = initial_graph_sync_enable_scrape_fallback
    graph_sync_scrape_page_timeout_seconds = max(5, initial_graph_sync_scrape_page_timeout_seconds)
    reconcile_limit = max(1, initial_reconcile_limit)
    reconcile_page_size = min(500, max(1, initial_reconcile_page_size))
    cleanup_unfollow_limit = max(1, initial_cleanup_unfollow_limit)
    cleanup_whitelist_min_followers = max(0, initial_cleanup_whitelist_min_followers)
    newsletter_slug = (initial_newsletter_slug or "").strip()
    newsletter_username = (initial_newsletter_username or "").strip()
    valid_choices = {option for option, _, _ in _START_MENU_SECTIONS}
    sorted_choices = sorted(valid_choices, key=lambda value: int(value))
    valid_choices_hint = f"{sorted_choices[0]}-{sorted_choices[-1]}"

    while True:
        settings = _bootstrap_settings()
        queue_counts = {
            "ready": 0,
            "deferred": 0,
            "rejected": 0,
            "followed": 0,
        }
        try:
            _, queue_repository = _build_runner(settings)
            queue_counts = queue_repository.growth_queue_state_counts()
        except Exception:
            pass
        if live_session_min_follows > live_session_target_follows:
            live_session_min_follows = live_session_target_follows
        _render_start_menu(
            has_session=settings.has_session,
            queue_ready_count=int(queue_counts.get("ready", 0)),
            queue_deferred_count=int(queue_counts.get("deferred", 0)),
            queue_rejected_count=int(queue_counts.get("rejected", 0)),
            queue_followed_count=int(queue_counts.get("followed", 0)),
            tag_slug=tag_slug,
            seed_user_refs=seed_user_refs,
            live_session_minutes=live_session_minutes,
            live_session_target_follows=live_session_target_follows,
            live_session_min_follows=live_session_min_follows,
            live_session_max_passes=live_session_max_passes,
            max_mutations_per_10_minutes=max_mutations_per_10_minutes,
            max_follow_actions_per_run=settings.max_follow_actions_per_run,
            max_subscribe_actions_per_day=settings.max_subscribe_actions_per_day,
            max_comment_actions_per_day=settings.max_comment_actions_per_day,
            min_verify_gap_seconds=min_verify_gap_seconds,
            max_verify_gap_seconds=max_verify_gap_seconds,
            pass_cooldown_min_seconds=pass_cooldown_min_seconds,
            pass_cooldown_max_seconds=pass_cooldown_max_seconds,
            pacing_soft_degrade_cooldown_seconds=pacing_soft_degrade_cooldown_seconds,
            enable_pacing_auto_clamp=enable_pacing_auto_clamp,
            growth_policy=growth_policy,
            growth_sources=growth_sources,
            target_user_followers_scan_limit=target_user_followers_scan_limit,
            discovery_eligible_per_run=discovery_eligible_per_run,
            growth_candidate_queue_max_size=growth_candidate_queue_max_size,
            follow_cooldown_hours=follow_cooldown_hours,
            candidate_min_followers=candidate_min_followers,
            candidate_max_followers=candidate_max_followers,
            candidate_min_following=candidate_min_following,
            candidate_max_following=candidate_max_following,
            max_following_follower_ratio=max_following_follower_ratio,
            require_candidate_bio=require_candidate_bio,
            require_candidate_latest_post=require_candidate_latest_post,
            candidate_recent_activity_days=candidate_recent_activity_days,
            discovery_followers_depth=discovery_followers_depth,
            discovery_seed_followers_limit=discovery_seed_followers_limit,
            discovery_second_hop_seed_limit=discovery_second_hop_seed_limit,
            growth_queue_buffer_target_min=growth_queue_buffer_target_min,
            growth_queue_buffer_target_max=growth_queue_buffer_target_max,
            growth_queue_buffer_target_multiplier=growth_queue_buffer_target_multiplier,
            growth_queue_fetch_limit_min=growth_queue_fetch_limit_min,
            growth_queue_fetch_limit_max=growth_queue_fetch_limit_max,
            growth_queue_fetch_limit_multiplier=growth_queue_fetch_limit_multiplier,
            growth_queue_due_deferred_reserve_ratio=growth_queue_due_deferred_reserve_ratio,
            growth_queue_retry_started_floor_seconds=growth_queue_retry_started_floor_seconds,
            growth_queue_retry_started_cooldown_multiplier=growth_queue_retry_started_cooldown_multiplier,
            growth_queue_retry_short_floor_seconds=growth_queue_retry_short_floor_seconds,
            growth_queue_retry_short_cooldown_multiplier=growth_queue_retry_short_cooldown_multiplier,
            growth_queue_retry_medium_floor_seconds=growth_queue_retry_medium_floor_seconds,
            growth_queue_retry_medium_cooldown_multiplier=growth_queue_retry_medium_cooldown_multiplier,
            growth_queue_retry_long_floor_seconds=growth_queue_retry_long_floor_seconds,
            growth_queue_retry_long_cooldown_multiplier=growth_queue_retry_long_cooldown_multiplier,
            growth_queue_prune_followed_after_days=growth_queue_prune_followed_after_days,
            growth_queue_prune_rejected_after_days=growth_queue_prune_rejected_after_days,
            growth_queue_prune_stale_after_days=growth_queue_prune_stale_after_days,
            enable_pre_follow_clap=enable_pre_follow_clap,
            enable_pre_follow_comment=enable_pre_follow_comment,
            pre_follow_comment_probability=pre_follow_comment_probability,
            enable_pre_follow_highlight=enable_pre_follow_highlight,
            pre_follow_highlight_probability=pre_follow_highlight_probability,
            pre_follow_comment_templates_raw=pre_follow_comment_templates_raw,
            graph_sync_auto_enabled=graph_sync_auto_enabled,
            graph_sync_freshness_window_minutes=graph_sync_freshness_window_minutes,
            graph_sync_full_pagination=graph_sync_full_pagination,
            graph_sync_enable_graphql_following=graph_sync_enable_graphql_following,
            graph_sync_enable_scrape_fallback=graph_sync_enable_scrape_fallback,
            graph_sync_scrape_page_timeout_seconds=graph_sync_scrape_page_timeout_seconds,
            reconcile_limit=reconcile_limit,
            reconcile_page_size=reconcile_page_size,
            cleanup_unfollow_limit=cleanup_unfollow_limit,
            cleanup_whitelist_min_followers=cleanup_whitelist_min_followers,
            newsletter_slug=newsletter_slug or None,
            newsletter_username=newsletter_username or None,
        )

        choice = str(typer.prompt("Select option", default="1")).strip().lower()
        if choice in {"q", "quit", "exit"}:
            choice = "8"
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

        def _prompt_cleanup_run_limit() -> int:
            nonlocal cleanup_unfollow_limit
            run_limit = int(
                typer.prompt(
                    "Cleanup-only unfollow limit for this run",
                    default=cleanup_unfollow_limit,
                    type=int,
                )
            )
            if run_limit < 1:
                _print_notice("Cleanup limit must be >= 1. Using current default.", level="warning")
                return cleanup_unfollow_limit
            cleanup_unfollow_limit = run_limit
            return run_limit

        def _refresh_defaults_from_settings(refreshed_settings: AppSettings) -> None:
            nonlocal seed_user_refs
            nonlocal live_session_minutes
            nonlocal live_session_target_follows
            nonlocal live_session_min_follows
            nonlocal live_session_max_passes
            nonlocal max_mutations_per_10_minutes
            nonlocal min_verify_gap_seconds
            nonlocal max_verify_gap_seconds
            nonlocal pass_cooldown_min_seconds
            nonlocal pass_cooldown_max_seconds
            nonlocal pacing_soft_degrade_cooldown_seconds
            nonlocal enable_pacing_auto_clamp
            nonlocal growth_policy
            nonlocal growth_sources
            nonlocal target_user_followers_scan_limit
            nonlocal discovery_eligible_per_run
            nonlocal growth_candidate_queue_max_size
            nonlocal follow_cooldown_hours
            nonlocal candidate_min_followers
            nonlocal candidate_max_followers
            nonlocal candidate_min_following
            nonlocal candidate_max_following
            nonlocal max_following_follower_ratio
            nonlocal require_candidate_bio
            nonlocal require_candidate_latest_post
            nonlocal candidate_recent_activity_days
            nonlocal discovery_followers_depth
            nonlocal discovery_seed_followers_limit
            nonlocal discovery_second_hop_seed_limit
            nonlocal growth_queue_buffer_target_min
            nonlocal growth_queue_buffer_target_max
            nonlocal growth_queue_buffer_target_multiplier
            nonlocal growth_queue_fetch_limit_min
            nonlocal growth_queue_fetch_limit_max
            nonlocal growth_queue_fetch_limit_multiplier
            nonlocal growth_queue_due_deferred_reserve_ratio
            nonlocal growth_queue_retry_started_floor_seconds
            nonlocal growth_queue_retry_started_cooldown_multiplier
            nonlocal growth_queue_retry_short_floor_seconds
            nonlocal growth_queue_retry_short_cooldown_multiplier
            nonlocal growth_queue_retry_medium_floor_seconds
            nonlocal growth_queue_retry_medium_cooldown_multiplier
            nonlocal growth_queue_retry_long_floor_seconds
            nonlocal growth_queue_retry_long_cooldown_multiplier
            nonlocal growth_queue_prune_followed_after_days
            nonlocal growth_queue_prune_rejected_after_days
            nonlocal growth_queue_prune_stale_after_days
            nonlocal enable_pre_follow_clap
            nonlocal enable_pre_follow_comment
            nonlocal pre_follow_comment_probability
            nonlocal enable_pre_follow_highlight
            nonlocal pre_follow_highlight_probability
            nonlocal pre_follow_comment_templates_raw
            nonlocal graph_sync_auto_enabled
            nonlocal graph_sync_freshness_window_minutes
            nonlocal graph_sync_full_pagination
            nonlocal graph_sync_enable_graphql_following
            nonlocal graph_sync_enable_scrape_fallback
            nonlocal graph_sync_scrape_page_timeout_seconds
            nonlocal cleanup_unfollow_limit
            nonlocal cleanup_whitelist_min_followers
            nonlocal newsletter_slug
            nonlocal newsletter_username

            seed_user_refs = _normalize_seed_user_refs(refreshed_settings.discovery_seed_users)
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
            growth_policy = refreshed_settings.default_growth_policy
            growth_sources = list(refreshed_settings.default_growth_sources)
            target_user_followers_scan_limit = refreshed_settings.target_user_followers_scan_limit
            discovery_eligible_per_run = refreshed_settings.discovery_eligible_per_run
            growth_candidate_queue_max_size = refreshed_settings.growth_candidate_queue_max_size
            follow_cooldown_hours = refreshed_settings.follow_cooldown_hours
            candidate_min_followers = refreshed_settings.candidate_min_followers
            candidate_max_followers = refreshed_settings.candidate_max_followers
            candidate_min_following = refreshed_settings.candidate_min_following
            candidate_max_following = refreshed_settings.candidate_max_following
            max_following_follower_ratio = refreshed_settings.max_following_follower_ratio
            require_candidate_bio = refreshed_settings.require_candidate_bio
            require_candidate_latest_post = refreshed_settings.require_candidate_latest_post
            candidate_recent_activity_days = refreshed_settings.candidate_recent_activity_days
            discovery_followers_depth = refreshed_settings.discovery_followers_depth
            discovery_seed_followers_limit = refreshed_settings.discovery_seed_followers_limit
            discovery_second_hop_seed_limit = refreshed_settings.discovery_second_hop_seed_limit
            growth_queue_buffer_target_min = refreshed_settings.growth_queue_buffer_target_min
            growth_queue_buffer_target_max = refreshed_settings.growth_queue_buffer_target_max
            growth_queue_buffer_target_multiplier = refreshed_settings.growth_queue_buffer_target_multiplier
            growth_queue_fetch_limit_min = refreshed_settings.growth_queue_fetch_limit_min
            growth_queue_fetch_limit_max = refreshed_settings.growth_queue_fetch_limit_max
            growth_queue_fetch_limit_multiplier = refreshed_settings.growth_queue_fetch_limit_multiplier
            growth_queue_due_deferred_reserve_ratio = refreshed_settings.growth_queue_due_deferred_reserve_ratio
            growth_queue_retry_started_floor_seconds = refreshed_settings.growth_queue_retry_started_floor_seconds
            growth_queue_retry_started_cooldown_multiplier = (
                refreshed_settings.growth_queue_retry_started_cooldown_multiplier
            )
            growth_queue_retry_short_floor_seconds = refreshed_settings.growth_queue_retry_short_floor_seconds
            growth_queue_retry_short_cooldown_multiplier = refreshed_settings.growth_queue_retry_short_cooldown_multiplier
            growth_queue_retry_medium_floor_seconds = refreshed_settings.growth_queue_retry_medium_floor_seconds
            growth_queue_retry_medium_cooldown_multiplier = (
                refreshed_settings.growth_queue_retry_medium_cooldown_multiplier
            )
            growth_queue_retry_long_floor_seconds = refreshed_settings.growth_queue_retry_long_floor_seconds
            growth_queue_retry_long_cooldown_multiplier = refreshed_settings.growth_queue_retry_long_cooldown_multiplier
            growth_queue_prune_followed_after_days = refreshed_settings.growth_queue_prune_followed_after_days
            growth_queue_prune_rejected_after_days = refreshed_settings.growth_queue_prune_rejected_after_days
            growth_queue_prune_stale_after_days = refreshed_settings.growth_queue_prune_stale_after_days
            enable_pre_follow_clap = refreshed_settings.enable_pre_follow_clap
            enable_pre_follow_comment = refreshed_settings.enable_pre_follow_comment
            pre_follow_comment_probability = refreshed_settings.pre_follow_comment_probability
            enable_pre_follow_highlight = refreshed_settings.enable_pre_follow_highlight
            pre_follow_highlight_probability = refreshed_settings.pre_follow_highlight_probability
            pre_follow_comment_templates_raw = refreshed_settings.pre_follow_comment_templates_raw
            graph_sync_auto_enabled = refreshed_settings.graph_sync_auto_enabled
            graph_sync_freshness_window_minutes = refreshed_settings.graph_sync_freshness_window_minutes
            graph_sync_full_pagination = refreshed_settings.graph_sync_full_pagination
            graph_sync_enable_graphql_following = refreshed_settings.graph_sync_enable_graphql_following
            graph_sync_enable_scrape_fallback = refreshed_settings.graph_sync_enable_scrape_fallback
            graph_sync_scrape_page_timeout_seconds = refreshed_settings.graph_sync_scrape_page_timeout_seconds
            cleanup_unfollow_limit = max(1, refreshed_settings.cleanup_unfollow_limit)
            cleanup_whitelist_min_followers = max(0, refreshed_settings.cleanup_unfollow_whitelist_min_followers)
            if not newsletter_slug:
                newsletter_slug = refreshed_settings.contract_registry_live_newsletter_slug or ""
            if not newsletter_username:
                newsletter_username = refreshed_settings.contract_registry_live_newsletter_username or ""

        def _select_from_submenu(
            *,
            title: str,
            options: tuple[tuple[str, str, str], ...],
            default_choice: str = "1",
        ) -> str | None:
            submenu_choices = {option for option, _, _ in options}
            back_choice = options[-1][0]
            style = _START_MENU_SECTION_STYLES.get(title, "white")
            while True:
                if title == "Growth Policy":
                    template_count = len(
                        [item for item in pre_follow_comment_templates_raw.split("||") if item.strip()]
                    )
                    comment_default_label = "enabled" if enable_pre_follow_comment else "runtime-enabled"
                    _print_notice(
                        "Public-touch config: "
                        f"comments={comment_default_label}, "
                        f"comment_probability={round(pre_follow_comment_probability, 3)}, "
                        f"comment_budget_per_day={settings.max_comment_actions_per_day}, "
                        f"highlights={'enabled' if enable_pre_follow_highlight else 'runtime-enabled'}, "
                        f"highlight_probability={round(pre_follow_highlight_probability, 3)}, "
                        f"highlight_budget_per_day={settings.max_highlight_actions_per_day}, "
                        f"templates={template_count}.",
                        level="info",
                    )
                _render_start_submenu(title=title, options=options, style=style)
                submenu_choice = str(typer.prompt("Select option", default=default_choice)).strip().lower()
                if submenu_choice in {"b", "back"}:
                    return None
                if submenu_choice in {"q", "quit", "exit"}:
                    return "exit"
                if submenu_choice not in submenu_choices:
                    allowed = ", ".join(sorted(submenu_choices, key=int))
                    _print_notice(f"Invalid option. Choose one of {allowed}, `b`, or `q`.", level="warning")
                    continue
                if submenu_choice == back_choice:
                    return None
                return submenu_choice

        def _edit_defaults() -> None:
            nonlocal tag_slug
            nonlocal seed_user_refs
            nonlocal live_session_minutes
            nonlocal live_session_target_follows
            nonlocal live_session_min_follows
            nonlocal live_session_max_passes
            nonlocal max_mutations_per_10_minutes
            nonlocal min_verify_gap_seconds
            nonlocal max_verify_gap_seconds
            nonlocal pass_cooldown_min_seconds
            nonlocal pass_cooldown_max_seconds
            nonlocal pacing_soft_degrade_cooldown_seconds
            nonlocal enable_pacing_auto_clamp
            nonlocal growth_policy
            nonlocal growth_sources
            nonlocal target_user_followers_scan_limit
            nonlocal discovery_eligible_per_run
            nonlocal growth_candidate_queue_max_size
            nonlocal follow_cooldown_hours
            nonlocal candidate_min_followers
            nonlocal candidate_max_followers
            nonlocal candidate_min_following
            nonlocal candidate_max_following
            nonlocal max_following_follower_ratio
            nonlocal require_candidate_bio
            nonlocal require_candidate_latest_post
            nonlocal candidate_recent_activity_days
            nonlocal discovery_followers_depth
            nonlocal discovery_seed_followers_limit
            nonlocal discovery_second_hop_seed_limit
            nonlocal growth_queue_buffer_target_min
            nonlocal growth_queue_buffer_target_max
            nonlocal growth_queue_buffer_target_multiplier
            nonlocal growth_queue_fetch_limit_min
            nonlocal growth_queue_fetch_limit_max
            nonlocal growth_queue_fetch_limit_multiplier
            nonlocal growth_queue_due_deferred_reserve_ratio
            nonlocal growth_queue_retry_started_floor_seconds
            nonlocal growth_queue_retry_started_cooldown_multiplier
            nonlocal growth_queue_retry_short_floor_seconds
            nonlocal growth_queue_retry_short_cooldown_multiplier
            nonlocal growth_queue_retry_medium_floor_seconds
            nonlocal growth_queue_retry_medium_cooldown_multiplier
            nonlocal growth_queue_retry_long_floor_seconds
            nonlocal growth_queue_retry_long_cooldown_multiplier
            nonlocal growth_queue_prune_followed_after_days
            nonlocal growth_queue_prune_rejected_after_days
            nonlocal growth_queue_prune_stale_after_days
            nonlocal enable_pre_follow_clap
            nonlocal enable_pre_follow_comment
            nonlocal pre_follow_comment_probability
            nonlocal enable_pre_follow_highlight
            nonlocal pre_follow_highlight_probability
            nonlocal pre_follow_comment_templates_raw
            nonlocal graph_sync_auto_enabled
            nonlocal graph_sync_freshness_window_minutes
            nonlocal graph_sync_full_pagination
            nonlocal graph_sync_enable_graphql_following
            nonlocal graph_sync_enable_scrape_fallback
            nonlocal graph_sync_scrape_page_timeout_seconds
            nonlocal reconcile_limit
            nonlocal reconcile_page_size
            nonlocal cleanup_unfollow_limit
            nonlocal cleanup_whitelist_min_followers
            nonlocal newsletter_slug
            nonlocal newsletter_username

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

            growth_policy = _prompt_growth_policy_choice(
                f"Default growth policy ({_GROWTH_POLICY_CHOICES})",
                default=growth_policy,
            )
            growth_sources = _prompt_growth_sources_choice(
                f"Default growth sources ({_GROWTH_SOURCE_CHOICES}; comma-separated)",
                default=growth_sources,
            )

            target_user_scan_limit_value = int(
                typer.prompt(
                    "Default target-user followers scan limit",
                    default=target_user_followers_scan_limit,
                    type=int,
                )
            )
            if target_user_scan_limit_value < 1:
                _print_notice("Target-user followers scan limit must be >= 1. Keeping previous value.", level="warning")
            else:
                target_user_followers_scan_limit = target_user_scan_limit_value

            discovery_eligible_per_run_value = int(
                typer.prompt(
                    "Discovery eligible users per run",
                    default=discovery_eligible_per_run,
                    type=int,
                )
            )
            if discovery_eligible_per_run_value < 1:
                _print_notice("Discovery eligible users per run must be >= 1. Keeping previous value.", level="warning")
            else:
                discovery_eligible_per_run = discovery_eligible_per_run_value

            growth_candidate_queue_max_size_value = int(
                typer.prompt(
                    "Growth candidate DB cap",
                    default=growth_candidate_queue_max_size,
                    type=int,
                )
            )
            if growth_candidate_queue_max_size_value < 1:
                _print_notice("Growth candidate DB cap must be >= 1. Keeping previous value.", level="warning")
            else:
                growth_candidate_queue_max_size = growth_candidate_queue_max_size_value

            follow_cooldown_hours_value = int(
                typer.prompt(
                    "Follow cooldown hours",
                    default=follow_cooldown_hours,
                    type=int,
                )
            )
            if follow_cooldown_hours_value < 1:
                _print_notice("Follow cooldown hours must be >= 1. Keeping previous value.", level="warning")
            else:
                follow_cooldown_hours = follow_cooldown_hours_value

            candidate_min_followers_value = int(
                typer.prompt(
                    "Candidate minimum followers",
                    default=candidate_min_followers,
                    type=int,
                )
            )
            if candidate_min_followers_value < 0:
                _print_notice("Candidate minimum followers must be >= 0. Keeping previous value.", level="warning")
            else:
                candidate_min_followers = candidate_min_followers_value

            candidate_max_followers_value = int(
                typer.prompt(
                    "Candidate maximum followers (0 disables)",
                    default=candidate_max_followers,
                    type=int,
                )
            )
            if candidate_max_followers_value < 0:
                _print_notice("Candidate maximum followers must be >= 0. Keeping previous value.", level="warning")
            else:
                candidate_max_followers = candidate_max_followers_value

            candidate_min_following_value = int(
                typer.prompt(
                    "Candidate minimum following",
                    default=candidate_min_following,
                    type=int,
                )
            )
            if candidate_min_following_value < 0:
                _print_notice("Candidate minimum following must be >= 0. Keeping previous value.", level="warning")
            else:
                candidate_min_following = candidate_min_following_value

            candidate_max_following_value = int(
                typer.prompt(
                    "Candidate maximum following (0 disables)",
                    default=candidate_max_following,
                    type=int,
                )
            )
            if candidate_max_following_value < 0:
                _print_notice("Candidate maximum following must be >= 0. Keeping previous value.", level="warning")
            else:
                candidate_max_following = candidate_max_following_value

            max_following_follower_ratio_value = float(
                typer.prompt(
                    "Candidate maximum following/follower ratio",
                    default=max_following_follower_ratio,
                    type=float,
                )
            )
            if max_following_follower_ratio_value < 0:
                _print_notice(
                    "Max following/follower ratio must be >= 0. Keeping previous value.",
                    level="warning",
                )
            else:
                max_following_follower_ratio = max_following_follower_ratio_value

            require_candidate_bio = typer.confirm(
                "Require candidate bio?",
                default=require_candidate_bio,
            )
            require_candidate_latest_post = typer.confirm(
                "Require candidate latest post?",
                default=require_candidate_latest_post,
            )

            candidate_recent_activity_days_value = int(
                typer.prompt(
                    "Candidate recent activity days (0 disables)",
                    default=candidate_recent_activity_days,
                    type=int,
                )
            )
            if candidate_recent_activity_days_value < 0:
                _print_notice("Recent activity days must be >= 0. Keeping previous value.", level="warning")
            else:
                candidate_recent_activity_days = candidate_recent_activity_days_value

            discovery_depth_value = int(
                typer.prompt(
                    "Discovery followers depth (1 or 2)",
                    default=discovery_followers_depth,
                    type=int,
                )
            )
            if discovery_depth_value not in {1, 2}:
                _print_notice("Discovery depth must be 1 or 2. Keeping previous value.", level="warning")
            else:
                discovery_followers_depth = discovery_depth_value

            discovery_seed_followers_limit_value = int(
                typer.prompt(
                    "Seed followers fetch limit",
                    default=discovery_seed_followers_limit,
                    type=int,
                )
            )
            if discovery_seed_followers_limit_value < 1:
                _print_notice("Seed followers limit must be >= 1. Keeping previous value.", level="warning")
            else:
                discovery_seed_followers_limit = discovery_seed_followers_limit_value

            discovery_second_hop_seed_limit_value = int(
                typer.prompt(
                    "Second-hop seed limit",
                    default=discovery_second_hop_seed_limit,
                    type=int,
                )
            )
            if discovery_second_hop_seed_limit_value < 1:
                _print_notice("Second-hop seed limit must be >= 1. Keeping previous value.", level="warning")
            else:
                discovery_second_hop_seed_limit = discovery_second_hop_seed_limit_value

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

            max_follow_actions_per_run_value = int(
                typer.prompt(
                    "Max follow actions per cycle",
                    default=settings.max_follow_actions_per_run,
                    type=int,
                )
            )
            if max_follow_actions_per_run_value < 0:
                _print_notice("Max follow actions per cycle must be >= 0. Keeping previous value.", level="warning")
                max_follow_actions_per_run_value = settings.max_follow_actions_per_run

            max_subscribe_actions_per_day_value = int(
                typer.prompt(
                    "Max subscribe actions per day",
                    default=settings.max_subscribe_actions_per_day,
                    type=int,
                )
            )
            if max_subscribe_actions_per_day_value < 0:
                _print_notice("Max subscribe actions per day must be >= 0. Keeping previous value.", level="warning")
                max_subscribe_actions_per_day_value = settings.max_subscribe_actions_per_day

            max_comment_actions_per_day_value = int(
                typer.prompt(
                    "Max comment actions per day",
                    default=settings.max_comment_actions_per_day,
                    type=int,
                )
            )
            if max_comment_actions_per_day_value < 0:
                _print_notice("Max comment actions per day must be >= 0. Keeping previous value.", level="warning")
                max_comment_actions_per_day_value = settings.max_comment_actions_per_day

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

            enable_pre_follow_clap = typer.confirm(
                "Enable pre-follow clap?",
                default=enable_pre_follow_clap,
            )
            enable_pre_follow_comment = typer.confirm(
                "Enable optional pre-follow comment for comment policy?",
                default=enable_pre_follow_comment,
            )
            pre_follow_comment_probability_value = float(
                typer.prompt(
                    "Pre-follow comment probability per comment-policy candidate (0.0-1.0)",
                    default=pre_follow_comment_probability,
                    type=float,
                )
            )
            if pre_follow_comment_probability_value < 0.0 or pre_follow_comment_probability_value > 1.0:
                _print_notice("Pre-follow comment probability must be between 0 and 1. Keeping previous value.", level="warning")
            else:
                pre_follow_comment_probability = pre_follow_comment_probability_value

            enable_pre_follow_highlight = typer.confirm(
                "Enable optional pre-follow highlight for highlight policy?",
                default=enable_pre_follow_highlight,
            )
            pre_follow_highlight_probability_value = float(
                typer.prompt(
                    "Pre-follow highlight probability per highlight-policy candidate (0.0-1.0)",
                    default=pre_follow_highlight_probability,
                    type=float,
                )
            )
            if pre_follow_highlight_probability_value < 0.0 or pre_follow_highlight_probability_value > 1.0:
                _print_notice("Pre-follow highlight probability must be between 0 and 1. Keeping previous value.", level="warning")
            else:
                pre_follow_highlight_probability = pre_follow_highlight_probability_value

            pre_follow_comment_templates_input = str(
                typer.prompt(
                    "Pre-follow comment templates (`||` separated, '-' to clear)",
                    default=pre_follow_comment_templates_raw,
                )
            ).strip()
            if pre_follow_comment_templates_input == "-":
                pre_follow_comment_templates_raw = ""
            elif pre_follow_comment_templates_input:
                pre_follow_comment_templates_raw = pre_follow_comment_templates_input

            queue_buffer_min_value = int(
                typer.prompt(
                    "Queue buffer target min",
                    default=growth_queue_buffer_target_min,
                    type=int,
                )
            )
            if queue_buffer_min_value < 1:
                _print_notice("Queue buffer min must be >= 1. Keeping previous value.", level="warning")
            else:
                growth_queue_buffer_target_min = queue_buffer_min_value

            queue_buffer_max_value = int(
                typer.prompt(
                    "Queue buffer target max",
                    default=growth_queue_buffer_target_max,
                    type=int,
                )
            )
            if queue_buffer_max_value < growth_queue_buffer_target_min:
                _print_notice("Queue buffer max must be >= buffer min. Keeping previous value.", level="warning")
            else:
                growth_queue_buffer_target_max = queue_buffer_max_value

            queue_buffer_multiplier_value = int(
                typer.prompt(
                    "Queue buffer multiplier",
                    default=growth_queue_buffer_target_multiplier,
                    type=int,
                )
            )
            if queue_buffer_multiplier_value < 1:
                _print_notice("Queue buffer multiplier must be >= 1. Keeping previous value.", level="warning")
            else:
                growth_queue_buffer_target_multiplier = queue_buffer_multiplier_value

            queue_fetch_min_value = int(
                typer.prompt(
                    "Queue fetch limit min",
                    default=growth_queue_fetch_limit_min,
                    type=int,
                )
            )
            if queue_fetch_min_value < 1:
                _print_notice("Queue fetch min must be >= 1. Keeping previous value.", level="warning")
            else:
                growth_queue_fetch_limit_min = queue_fetch_min_value

            queue_fetch_max_value = int(
                typer.prompt(
                    "Queue fetch limit max",
                    default=growth_queue_fetch_limit_max,
                    type=int,
                )
            )
            if queue_fetch_max_value < growth_queue_fetch_limit_min:
                _print_notice("Queue fetch max must be >= fetch min. Keeping previous value.", level="warning")
            else:
                growth_queue_fetch_limit_max = queue_fetch_max_value

            queue_fetch_multiplier_value = int(
                typer.prompt(
                    "Queue fetch multiplier",
                    default=growth_queue_fetch_limit_multiplier,
                    type=int,
                )
            )
            if queue_fetch_multiplier_value < 1:
                _print_notice("Queue fetch multiplier must be >= 1. Keeping previous value.", level="warning")
            else:
                growth_queue_fetch_limit_multiplier = queue_fetch_multiplier_value

            due_deferred_reserve_ratio_value = float(
                typer.prompt(
                    "Queue due-deferred reserve ratio (0.0-0.9)",
                    default=growth_queue_due_deferred_reserve_ratio,
                    type=float,
                )
            )
            if due_deferred_reserve_ratio_value < 0.0 or due_deferred_reserve_ratio_value > 0.9:
                _print_notice("Queue due-deferred reserve ratio must be between 0.0 and 0.9. Keeping previous value.", level="warning")
            else:
                growth_queue_due_deferred_reserve_ratio = due_deferred_reserve_ratio_value

            retry_started_floor_value = int(
                typer.prompt(
                    "Queue retry started floor seconds",
                    default=growth_queue_retry_started_floor_seconds,
                    type=int,
                )
            )
            if retry_started_floor_value < 0:
                _print_notice("Queue retry started floor must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_retry_started_floor_seconds = retry_started_floor_value

            retry_started_multiplier_value = int(
                typer.prompt(
                    "Queue retry started cooldown multiplier",
                    default=growth_queue_retry_started_cooldown_multiplier,
                    type=int,
                )
            )
            if retry_started_multiplier_value < 0:
                _print_notice("Queue retry started multiplier must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_retry_started_cooldown_multiplier = retry_started_multiplier_value

            retry_short_floor_value = int(
                typer.prompt(
                    "Queue retry short floor seconds",
                    default=growth_queue_retry_short_floor_seconds,
                    type=int,
                )
            )
            if retry_short_floor_value < 0:
                _print_notice("Queue retry short floor must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_retry_short_floor_seconds = retry_short_floor_value

            retry_short_multiplier_value = int(
                typer.prompt(
                    "Queue retry short cooldown multiplier",
                    default=growth_queue_retry_short_cooldown_multiplier,
                    type=int,
                )
            )
            if retry_short_multiplier_value < 0:
                _print_notice("Queue retry short multiplier must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_retry_short_cooldown_multiplier = retry_short_multiplier_value

            retry_medium_floor_value = int(
                typer.prompt(
                    "Queue retry medium floor seconds",
                    default=growth_queue_retry_medium_floor_seconds,
                    type=int,
                )
            )
            if retry_medium_floor_value < growth_queue_retry_short_floor_seconds:
                _print_notice("Queue retry medium floor must be >= short floor. Keeping previous value.", level="warning")
            else:
                growth_queue_retry_medium_floor_seconds = retry_medium_floor_value

            retry_medium_multiplier_value = int(
                typer.prompt(
                    "Queue retry medium cooldown multiplier",
                    default=growth_queue_retry_medium_cooldown_multiplier,
                    type=int,
                )
            )
            if retry_medium_multiplier_value < 0:
                _print_notice("Queue retry medium multiplier must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_retry_medium_cooldown_multiplier = retry_medium_multiplier_value

            retry_long_floor_value = int(
                typer.prompt(
                    "Queue retry long floor seconds",
                    default=growth_queue_retry_long_floor_seconds,
                    type=int,
                )
            )
            if retry_long_floor_value < growth_queue_retry_medium_floor_seconds:
                _print_notice("Queue retry long floor must be >= medium floor. Keeping previous value.", level="warning")
            else:
                growth_queue_retry_long_floor_seconds = retry_long_floor_value

            retry_long_multiplier_value = int(
                typer.prompt(
                    "Queue retry long cooldown multiplier",
                    default=growth_queue_retry_long_cooldown_multiplier,
                    type=int,
                )
            )
            if retry_long_multiplier_value < 0:
                _print_notice("Queue retry long multiplier must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_retry_long_cooldown_multiplier = retry_long_multiplier_value

            prune_followed_days_value = int(
                typer.prompt(
                    "Queue prune followed-after days (0 keeps none)",
                    default=growth_queue_prune_followed_after_days,
                    type=int,
                )
            )
            if prune_followed_days_value < 0:
                _print_notice("Queue prune followed-after days must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_prune_followed_after_days = prune_followed_days_value

            prune_rejected_days_value = int(
                typer.prompt(
                    "Queue prune rejected-after days (0 keeps none)",
                    default=growth_queue_prune_rejected_after_days,
                    type=int,
                )
            )
            if prune_rejected_days_value < 0:
                _print_notice("Queue prune rejected-after days must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_prune_rejected_after_days = prune_rejected_days_value

            prune_stale_days_value = int(
                typer.prompt(
                    "Queue prune stale queued/deferred-after days (0 keeps none)",
                    default=growth_queue_prune_stale_after_days,
                    type=int,
                )
            )
            if prune_stale_days_value < 0:
                _print_notice("Queue prune stale days must be >= 0. Keeping previous value.", level="warning")
            else:
                growth_queue_prune_stale_after_days = prune_stale_days_value

            graph_sync_auto_enabled = typer.confirm(
                "Enable graph sync auto-run for growth, unfollow, and reconcile flows?",
                default=graph_sync_auto_enabled,
            )

            sync_freshness_value = int(
                typer.prompt(
                    "Graph sync freshness window (minutes)",
                    default=graph_sync_freshness_window_minutes,
                    type=int,
                )
            )
            if sync_freshness_value < 0:
                _print_notice("Graph sync freshness must be >= 0. Keeping previous value.", level="warning")
            else:
                graph_sync_freshness_window_minutes = sync_freshness_value

            graph_sync_full_pagination = typer.confirm(
                "Use full pagination during graph sync?",
                default=graph_sync_full_pagination,
            )

            graph_sync_enable_graphql_following = typer.confirm(
                "Enable GraphQL strategy for following import?",
                default=graph_sync_enable_graphql_following,
            )
            graph_sync_enable_scrape_fallback = typer.confirm(
                "Enable scrape fallback when following GraphQL is unavailable?",
                default=graph_sync_enable_scrape_fallback,
            )

            scrape_timeout_value = int(
                typer.prompt(
                    "Graph sync scrape timeout seconds",
                    default=graph_sync_scrape_page_timeout_seconds,
                    type=int,
                )
            )
            if scrape_timeout_value < 5:
                _print_notice("Graph sync scrape timeout must be >= 5 seconds. Keeping previous value.", level="warning")
            else:
                graph_sync_scrape_page_timeout_seconds = scrape_timeout_value

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

            page_size_value = int(
                typer.prompt(
                    "Default reconcile page size",
                    default=reconcile_page_size,
                    type=int,
                )
            )
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

            if live_session_min_follows > live_session_target_follows:
                live_session_min_follows = live_session_target_follows
            max_verify_gap_seconds = max(min_verify_gap_seconds, max_verify_gap_seconds)
            pass_cooldown_max_seconds = max(pass_cooldown_min_seconds, pass_cooldown_max_seconds)
            growth_queue_buffer_target_max = max(growth_queue_buffer_target_min, growth_queue_buffer_target_max)
            growth_queue_fetch_limit_max = max(growth_queue_fetch_limit_min, growth_queue_fetch_limit_max)
            growth_queue_due_deferred_reserve_ratio = max(0.0, min(0.9, growth_queue_due_deferred_reserve_ratio))
            growth_queue_retry_medium_floor_seconds = max(
                growth_queue_retry_short_floor_seconds,
                growth_queue_retry_medium_floor_seconds,
            )
            growth_queue_retry_long_floor_seconds = max(
                growth_queue_retry_medium_floor_seconds,
                growth_queue_retry_long_floor_seconds,
            )
            growth_queue_prune_followed_after_days = max(0, growth_queue_prune_followed_after_days)
            growth_queue_prune_rejected_after_days = max(0, growth_queue_prune_rejected_after_days)
            growth_queue_prune_stale_after_days = max(0, growth_queue_prune_stale_after_days)

            updates = {
                "DISCOVERY_SEED_USERS": ",".join(seed_user_refs or []),
                "DEFAULT_GROWTH_POLICY": growth_policy.value,
                "DEFAULT_GROWTH_SOURCES": ",".join(source.value for source in growth_sources),
                "TARGET_USER_FOLLOWERS_SCAN_LIMIT": str(target_user_followers_scan_limit),
                "DISCOVERY_ELIGIBLE_PER_RUN": str(discovery_eligible_per_run),
                "GROWTH_CANDIDATE_QUEUE_MAX_SIZE": str(growth_candidate_queue_max_size),
                "FOLLOW_COOLDOWN_HOURS": str(follow_cooldown_hours),
                "CANDIDATE_MIN_FOLLOWERS": str(candidate_min_followers),
                "CANDIDATE_MAX_FOLLOWERS": str(candidate_max_followers),
                "CANDIDATE_MIN_FOLLOWING": str(candidate_min_following),
                "CANDIDATE_MAX_FOLLOWING": str(candidate_max_following),
                "MAX_FOLLOWING_FOLLOWER_RATIO": str(max_following_follower_ratio),
                "REQUIRE_CANDIDATE_BIO": "true" if require_candidate_bio else "false",
                "REQUIRE_CANDIDATE_LATEST_POST": "true" if require_candidate_latest_post else "false",
                "CANDIDATE_RECENT_ACTIVITY_DAYS": str(candidate_recent_activity_days),
                "DISCOVERY_FOLLOWERS_DEPTH": str(discovery_followers_depth),
                "DISCOVERY_SEED_FOLLOWERS_LIMIT": str(discovery_seed_followers_limit),
                "DISCOVERY_SECOND_HOP_SEED_LIMIT": str(discovery_second_hop_seed_limit),
                "ENABLE_PRE_FOLLOW_CLAP": "true" if enable_pre_follow_clap else "false",
                "ENABLE_PRE_FOLLOW_COMMENT": "true" if enable_pre_follow_comment else "false",
                "PRE_FOLLOW_COMMENT_PROBABILITY": str(pre_follow_comment_probability),
                "ENABLE_PRE_FOLLOW_HIGHLIGHT": "true" if enable_pre_follow_highlight else "false",
                "PRE_FOLLOW_HIGHLIGHT_PROBABILITY": str(pre_follow_highlight_probability),
                "PRE_FOLLOW_COMMENT_TEMPLATES": pre_follow_comment_templates_raw,
                "LIVE_SESSION_DURATION_MINUTES": str(live_session_minutes),
                "LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS": str(live_session_target_follows),
                "LIVE_SESSION_MIN_FOLLOW_ATTEMPTS": str(live_session_min_follows),
                "LIVE_SESSION_MAX_PASSES": str(live_session_max_passes),
                "MAX_FOLLOW_ACTIONS_PER_RUN": str(max_follow_actions_per_run_value),
                "MAX_SUBSCRIBE_ACTIONS_PER_DAY": str(max_subscribe_actions_per_day_value),
                "MAX_COMMENT_ACTIONS_PER_DAY": str(max_comment_actions_per_day_value),
                "MAX_MUTATIONS_PER_10_MINUTES": str(max_mutations_per_10_minutes),
                "MIN_VERIFY_GAP_SECONDS": str(min_verify_gap_seconds),
                "MAX_VERIFY_GAP_SECONDS": str(max_verify_gap_seconds),
                "PASS_COOLDOWN_MIN_SECONDS": str(pass_cooldown_min_seconds),
                "PASS_COOLDOWN_MAX_SECONDS": str(pass_cooldown_max_seconds),
                "PACING_SOFT_DEGRADE_COOLDOWN_SECONDS": str(pacing_soft_degrade_cooldown_seconds),
                "ENABLE_PACING_AUTO_CLAMP": "true" if enable_pacing_auto_clamp else "false",
                "GROWTH_QUEUE_BUFFER_TARGET_MIN": str(growth_queue_buffer_target_min),
                "GROWTH_QUEUE_BUFFER_TARGET_MAX": str(growth_queue_buffer_target_max),
                "GROWTH_QUEUE_BUFFER_TARGET_MULTIPLIER": str(growth_queue_buffer_target_multiplier),
                "GROWTH_QUEUE_FETCH_LIMIT_MIN": str(growth_queue_fetch_limit_min),
                "GROWTH_QUEUE_FETCH_LIMIT_MAX": str(growth_queue_fetch_limit_max),
                "GROWTH_QUEUE_FETCH_LIMIT_MULTIPLIER": str(growth_queue_fetch_limit_multiplier),
                "GROWTH_QUEUE_DUE_DEFERRED_RESERVE_RATIO": str(growth_queue_due_deferred_reserve_ratio),
                "GROWTH_QUEUE_RETRY_STARTED_FLOOR_SECONDS": str(growth_queue_retry_started_floor_seconds),
                "GROWTH_QUEUE_RETRY_STARTED_COOLDOWN_MULTIPLIER": str(
                    growth_queue_retry_started_cooldown_multiplier
                ),
                "GROWTH_QUEUE_RETRY_SHORT_FLOOR_SECONDS": str(growth_queue_retry_short_floor_seconds),
                "GROWTH_QUEUE_RETRY_SHORT_COOLDOWN_MULTIPLIER": str(growth_queue_retry_short_cooldown_multiplier),
                "GROWTH_QUEUE_RETRY_MEDIUM_FLOOR_SECONDS": str(growth_queue_retry_medium_floor_seconds),
                "GROWTH_QUEUE_RETRY_MEDIUM_COOLDOWN_MULTIPLIER": str(growth_queue_retry_medium_cooldown_multiplier),
                "GROWTH_QUEUE_RETRY_LONG_FLOOR_SECONDS": str(growth_queue_retry_long_floor_seconds),
                "GROWTH_QUEUE_RETRY_LONG_COOLDOWN_MULTIPLIER": str(growth_queue_retry_long_cooldown_multiplier),
                "GROWTH_QUEUE_PRUNE_FOLLOWED_AFTER_DAYS": str(growth_queue_prune_followed_after_days),
                "GROWTH_QUEUE_PRUNE_REJECTED_AFTER_DAYS": str(growth_queue_prune_rejected_after_days),
                "GROWTH_QUEUE_PRUNE_STALE_AFTER_DAYS": str(growth_queue_prune_stale_after_days),
                "GRAPH_SYNC_AUTO_ENABLED": "true" if graph_sync_auto_enabled else "false",
                "GRAPH_SYNC_FRESHNESS_WINDOW_MINUTES": str(graph_sync_freshness_window_minutes),
                "GRAPH_SYNC_FULL_PAGINATION": "true" if graph_sync_full_pagination else "false",
                "GRAPH_SYNC_ENABLE_GRAPHQL_FOLLOWING": "true" if graph_sync_enable_graphql_following else "false",
                "GRAPH_SYNC_ENABLE_SCRAPE_FALLBACK": "true" if graph_sync_enable_scrape_fallback else "false",
                "GRAPH_SYNC_SCRAPE_PAGE_TIMEOUT_SECONDS": str(graph_sync_scrape_page_timeout_seconds),
                "CLEANUP_UNFOLLOW_LIMIT": str(cleanup_unfollow_limit),
                "CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS": str(cleanup_whitelist_min_followers),
                "RECONCILE_SCAN_LIMIT": str(reconcile_limit),
                "RECONCILE_PAGE_SIZE": str(reconcile_page_size),
                "CONTRACT_REGISTRY_LIVE_NEWSLETTER_SLUG": newsletter_slug,
                "CONTRACT_REGISTRY_LIVE_NEWSLETTER_USERNAME": newsletter_username,
            }
            _upsert_env_values(env_path=Path(".env"), updates=updates)
            _refresh_defaults_from_settings(_bootstrap_settings())
            _print_notice("Defaults updated and saved to .env.", level="success")

        def _prompt_target_user_refs_for_run() -> list[str] | None:
            nonlocal target_user_refs_for_growth
            default_value = ", ".join(target_user_refs_for_growth) if target_user_refs_for_growth else ""
            raw_value = str(
                typer.prompt(
                    "Target user(s) whose followers should be evaluated (comma-separated)",
                    default=default_value,
                )
            ).strip()
            refs = _parse_seed_user_refs(raw_value)
            if not refs:
                _print_notice("At least one target user is required for follower-harvest growth.", level="warning")
                return None
            target_user_refs_for_growth = refs
            return refs

        def _prompt_target_user_scan_limit_for_run() -> int:
            nonlocal target_user_followers_scan_limit
            requested_limit = int(
                typer.prompt(
                    "Followers to scan per target user for this run",
                    default=target_user_followers_scan_limit,
                    type=int,
                )
            )
            if requested_limit < 1:
                _print_notice("Target-user followers scan limit must be >= 1. Using current default.", level="warning")
                return target_user_followers_scan_limit
            target_user_followers_scan_limit = requested_limit
            return requested_limit

        def _prompt_seed_user_refs_for_run() -> list[str] | None:
            nonlocal seed_user_refs
            default_value = ", ".join(seed_user_refs) if seed_user_refs else ""
            raw_value = str(
                typer.prompt(
                    "Seed user(s) for follower discovery (comma-separated)",
                    default=default_value,
                )
            ).strip()
            refs = _parse_seed_user_refs(raw_value)
            if not refs:
                _print_notice("At least one seed user is required for seed-follower discovery.", level="warning")
                return None
            seed_user_refs = refs
            return refs

        unfollow_actions: dict[str, tuple[str, Callable[[], None]]] = {
            "1": (
                "cleanup-only unfollow (live)",
                lambda: cleanup_command(
                    live=True,
                    limit=_prompt_cleanup_run_limit(),
                    auto_sync=graph_sync_auto_enabled,
                ),
            ),
            "2": (
                "cleanup-only unfollow (dry-run)",
                lambda: cleanup_command(
                    live=False,
                    limit=_prompt_cleanup_run_limit(),
                    auto_sync=graph_sync_auto_enabled,
                ),
            ),
        }

        maintenance_actions: dict[str, tuple[str, Callable[[], None]]] = {
            "1": (
                "reconcile live",
                lambda: reconcile_command(
                    live=True,
                    max_users=reconcile_limit,
                    page_size=reconcile_page_size,
                    auto_sync=graph_sync_auto_enabled,
                ),
            ),
            "2": (
                "reconcile dry-run",
                lambda: reconcile_command(
                    live=False,
                    max_users=reconcile_limit,
                    page_size=reconcile_page_size,
                    auto_sync=graph_sync_auto_enabled,
                ),
            ),
            "3": ("sync social graph cache", lambda: sync_command(live=True, force=True)),
            "4": ("db hygiene dry-run", lambda: db_hygiene_command(live=False, vacuum=False)),
            "5": ("db hygiene live", lambda: db_hygiene_command(live=True, vacuum=False)),
        }

        diagnostics_actions: dict[str, tuple[str, Callable[[], None]]] = {
            "1": ("probe reads", lambda: probe_command(tag_slug=tag_slug)),
            "2": (
                "validate contracts",
                lambda: contracts_command(
                    tag_slug=tag_slug,
                    strict=True,
                    execute_reads=False,
                    newsletter_slug=None,
                    newsletter_username=None,
                ),
            ),
            "3": (
                "validate contracts with live reads",
                lambda: contracts_command(
                    tag_slug=tag_slug,
                    strict=True,
                    execute_reads=True,
                    newsletter_slug=newsletter_slug or None,
                    newsletter_username=newsletter_username or None,
                ),
            ),
        }

        observability_actions: dict[str, tuple[str, Callable[[], None]]] = {
            "1": ("show status", lambda: status_command(emit_artifact=True)),
            "2": ("show queue status", queue_command),
            "3": (
                "validate latest artifact",
                lambda: artifacts_validate_command(artifact_path=None, emit_artifact=True),
            ),
        }

        settings_actions: dict[str, tuple[str, Callable[[], None]]] = {
            "2": ("run setup wizard", lambda: setup_command(env_path=Path(".env"), auth_if_missing=True)),
            "3": (
                "refresh auth session",
                lambda: auth_command(
                    write_env=True,
                    env_path=Path(".env"),
                    login_url="https://medium.com/m/signin",
                ),
            ),
        }

        if choice == "1":
            selected_growth_sources_choice = _prompt_growth_sources_from_menu(default=growth_sources)
            if selected_growth_sources_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if selected_growth_sources_choice is None:
                continue

            selected_growth_sources = list(selected_growth_sources_choice)
            selected_target_user_refs: list[str] | None = None
            selected_target_user_scan_limit: int | None = None
            selected_seed_user_refs = seed_user_refs if GrowthSource.SEED_FOLLOWERS in selected_growth_sources else None

            if GrowthSource.SEED_FOLLOWERS in selected_growth_sources:
                selected_seed_user_refs = _prompt_seed_user_refs_for_run()
                if not selected_seed_user_refs:
                    continue
            if GrowthSource.TARGET_USER_FOLLOWERS in selected_growth_sources:
                selected_target_user_refs = _prompt_target_user_refs_for_run()
                if not selected_target_user_refs:
                    continue
                selected_target_user_scan_limit = _prompt_target_user_scan_limit_for_run()

            discovery_runtime_choice = _select_from_submenu(
                title="Discovery Runtime",
                options=_DISCOVERY_RUNTIME_MENU_OPTIONS,
            )
            if discovery_runtime_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if discovery_runtime_choice is None:
                continue

            growth_sources = selected_growth_sources
            if selected_seed_user_refs is not None:
                seed_user_refs = selected_seed_user_refs

            discovery_live = discovery_runtime_choice == "1"
            discovery_mode_label = "persist" if discovery_live else "dry-run"
            _execute(
                f"run discovery {discovery_mode_label} ({_format_growth_sources(selected_growth_sources)})",
                lambda: discover_command(
                    tag_slug=tag_slug,
                    live=discovery_live,
                    growth_sources=selected_growth_sources,
                    seed_user_refs=selected_seed_user_refs,
                    target_user_refs=selected_target_user_refs,
                    target_user_scan_limit=selected_target_user_scan_limit,
                    auto_sync=graph_sync_auto_enabled,
                ),
            )
            continue

        if choice == "2":
            growth_policy_choice = _select_from_submenu(title="Growth Policy", options=_GROWTH_POLICY_MENU_OPTIONS)
            if growth_policy_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if growth_policy_choice is None:
                continue
            selected_growth_policy = {
                "1": GrowthPolicy.FOLLOW_ONLY,
                "2": GrowthPolicy.WARM_ENGAGE,
                "3": GrowthPolicy.WARM_ENGAGE_COMMENT,
                "4": GrowthPolicy.WARM_ENGAGE_HIGHLIGHT,
            }[growth_policy_choice]
            growth_policy = selected_growth_policy

            growth_runtime_choice = _select_from_submenu(title="Growth Runtime", options=_GROWTH_RUNTIME_MENU_OPTIONS)
            if growth_runtime_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if growth_runtime_choice is None:
                continue

            def _run_growth(*, live: bool, session: bool) -> None:
                run_command(
                    tag_slug=tag_slug,
                    live=live,
                    growth_policy=selected_growth_policy,
                    session=session,
                    session_minutes=live_session_minutes if session and live else None,
                    target_follows=live_session_target_follows if session and live else None,
                    session_max_passes=live_session_max_passes if session and live else None,
                    auto_sync=graph_sync_auto_enabled,
                )

            growth_label = _format_growth_policy(selected_growth_policy)
            if growth_runtime_choice == "4":
                preflight_ok = _execute(
                    f"dry-run preflight ({growth_label})",
                    lambda: _run_growth(live=False, session=False),
                )
                if preflight_ok:
                    _execute(
                        f"run live growth session ({growth_label})",
                        lambda: _run_growth(live=True, session=True),
                    )
                continue
            growth_execution_actions: dict[str, tuple[str, Callable[[], None]]] = {
                "1": ("run live growth session", lambda: _run_growth(live=True, session=True)),
                "2": ("run live single cycle", lambda: _run_growth(live=True, session=False)),
                "3": ("run dry-run cycle", lambda: _run_growth(live=False, session=False)),
            }
            action_name, handler = growth_execution_actions[growth_runtime_choice]
            _execute(f"{action_name} ({growth_label})", handler)
            continue

        if choice == "3":
            unfollow_choice = _select_from_submenu(title="Unfollow", options=_UNFOLLOW_MENU_OPTIONS)
            if unfollow_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if unfollow_choice is None:
                continue
            action_name, handler = unfollow_actions[unfollow_choice]
            _execute(action_name, handler)
            continue

        if choice == "4":
            maintenance_choice = _select_from_submenu(title="Maintenance", options=_MAINTENANCE_MENU_OPTIONS)
            if maintenance_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if maintenance_choice is None:
                continue
            action_name, handler = maintenance_actions[maintenance_choice]
            _execute(action_name, handler)
            continue

        if choice == "5":
            diagnostics_choice = _select_from_submenu(title="Diagnostics", options=_DIAGNOSTICS_MENU_OPTIONS)
            if diagnostics_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if diagnostics_choice is None:
                continue
            action_name, handler = diagnostics_actions[diagnostics_choice]
            _execute(action_name, handler)
            continue

        if choice == "6":
            observability_choice = _select_from_submenu(title="Observability", options=_OBSERVABILITY_MENU_OPTIONS)
            if observability_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if observability_choice is None:
                continue
            action_name, handler = observability_actions[observability_choice]
            _execute(action_name, handler)
            continue

        if choice == "7":
            settings_choice = _select_from_submenu(title="Settings/Auth", options=_SETTINGS_MENU_OPTIONS)
            if settings_choice == "exit":
                _print_notice("Exiting start menu.", level="success")
                return
            if settings_choice is None:
                continue
            if settings_choice == "1":
                _edit_defaults()
                continue
            action_name, handler = settings_actions[settings_choice]
            _execute(action_name, handler)
            if settings_choice == "2":
                refreshed_settings = _bootstrap_settings()
                _refresh_defaults_from_settings(refreshed_settings)
            continue

        if choice == "8":
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
    growth_policy: GrowthPolicy | None = typer.Option(
        None,
        "--policy",
        case_sensitive=False,
        help=_GROWTH_POLICY_HELP,
    ),
    growth_sources: list[GrowthSource] | None = typer.Option(
        None,
        "--source",
        case_sensitive=False,
        help="Growth source. Repeat option to combine sources in quick-live mode.",
    ),
    mode: GrowthMode | None = typer.Option(
        None,
        "--mode",
        case_sensitive=False,
        help="Legacy compatibility alias for growth policy.",
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
    quick_live_discovery_inputs = bool(growth_sources or seed_user_refs)
    resolved_seeds = _normalize_seed_user_refs(seed_user_refs if seed_user_refs else settings.discovery_seed_users)
    resolved_growth_policy = _resolve_growth_policy_option(
        growth_policy=growth_policy,
        mode=mode,
        default=settings.default_growth_policy,
    )
    resolved_growth_sources = list(growth_sources or settings.default_growth_sources)

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
            initial_growth_policy=resolved_growth_policy,
            initial_growth_sources=resolved_growth_sources,
            initial_target_user_followers_scan_limit=settings.target_user_followers_scan_limit,
            initial_discovery_eligible_per_run=settings.discovery_eligible_per_run,
            initial_growth_candidate_queue_max_size=settings.growth_candidate_queue_max_size,
            initial_follow_cooldown_hours=settings.follow_cooldown_hours,
            initial_candidate_min_followers=settings.candidate_min_followers,
            initial_candidate_max_followers=settings.candidate_max_followers,
            initial_candidate_min_following=settings.candidate_min_following,
            initial_candidate_max_following=settings.candidate_max_following,
            initial_max_following_follower_ratio=settings.max_following_follower_ratio,
            initial_require_candidate_bio=settings.require_candidate_bio,
            initial_require_candidate_latest_post=settings.require_candidate_latest_post,
            initial_candidate_recent_activity_days=settings.candidate_recent_activity_days,
            initial_discovery_followers_depth=settings.discovery_followers_depth,
            initial_discovery_seed_followers_limit=settings.discovery_seed_followers_limit,
            initial_discovery_second_hop_seed_limit=settings.discovery_second_hop_seed_limit,
            initial_growth_queue_buffer_target_min=settings.growth_queue_buffer_target_min,
            initial_growth_queue_buffer_target_max=settings.growth_queue_buffer_target_max,
            initial_growth_queue_buffer_target_multiplier=settings.growth_queue_buffer_target_multiplier,
            initial_growth_queue_fetch_limit_min=settings.growth_queue_fetch_limit_min,
            initial_growth_queue_fetch_limit_max=settings.growth_queue_fetch_limit_max,
            initial_growth_queue_fetch_limit_multiplier=settings.growth_queue_fetch_limit_multiplier,
            initial_growth_queue_due_deferred_reserve_ratio=settings.growth_queue_due_deferred_reserve_ratio,
            initial_growth_queue_retry_started_floor_seconds=settings.growth_queue_retry_started_floor_seconds,
            initial_growth_queue_retry_started_cooldown_multiplier=settings.growth_queue_retry_started_cooldown_multiplier,
            initial_growth_queue_retry_short_floor_seconds=settings.growth_queue_retry_short_floor_seconds,
            initial_growth_queue_retry_short_cooldown_multiplier=settings.growth_queue_retry_short_cooldown_multiplier,
            initial_growth_queue_retry_medium_floor_seconds=settings.growth_queue_retry_medium_floor_seconds,
            initial_growth_queue_retry_medium_cooldown_multiplier=settings.growth_queue_retry_medium_cooldown_multiplier,
            initial_growth_queue_retry_long_floor_seconds=settings.growth_queue_retry_long_floor_seconds,
            initial_growth_queue_retry_long_cooldown_multiplier=settings.growth_queue_retry_long_cooldown_multiplier,
            initial_growth_queue_prune_followed_after_days=settings.growth_queue_prune_followed_after_days,
            initial_growth_queue_prune_rejected_after_days=settings.growth_queue_prune_rejected_after_days,
            initial_growth_queue_prune_stale_after_days=settings.growth_queue_prune_stale_after_days,
            initial_enable_pre_follow_clap=settings.enable_pre_follow_clap,
            initial_enable_pre_follow_comment=settings.enable_pre_follow_comment,
            initial_pre_follow_comment_probability=settings.pre_follow_comment_probability,
            initial_enable_pre_follow_highlight=settings.enable_pre_follow_highlight,
            initial_pre_follow_highlight_probability=settings.pre_follow_highlight_probability,
            initial_pre_follow_comment_templates_raw=settings.pre_follow_comment_templates_raw,
            initial_graph_sync_auto_enabled=settings.graph_sync_auto_enabled,
            initial_graph_sync_freshness_window_minutes=settings.graph_sync_freshness_window_minutes,
            initial_graph_sync_full_pagination=settings.graph_sync_full_pagination,
            initial_graph_sync_enable_graphql_following=settings.graph_sync_enable_graphql_following,
            initial_graph_sync_enable_scrape_fallback=settings.graph_sync_enable_scrape_fallback,
            initial_graph_sync_scrape_page_timeout_seconds=settings.graph_sync_scrape_page_timeout_seconds,
            initial_reconcile_limit=settings.reconcile_scan_limit,
            initial_reconcile_page_size=settings.reconcile_page_size,
            initial_cleanup_unfollow_limit=max(1, settings.cleanup_unfollow_limit),
            initial_cleanup_whitelist_min_followers=settings.cleanup_unfollow_whitelist_min_followers,
            initial_newsletter_slug=settings.contract_registry_live_newsletter_slug,
            initial_newsletter_username=settings.contract_registry_live_newsletter_username,
        )
        return

    _require_session(settings, guidance="uv run bot auth (or uv run bot setup)")
    if quick_live_discovery_inputs:
        _print_notice(
            "Quick-live start runs queue execution only. Run `uv run bot discover` to build queue candidates.",
            level="warning",
        )

    if dry_run_first:
        _print_notice("Step 1/2: running dry-run sanity check.", level="info")
        run_command(
            tag_slug=tag_slug,
            live=False,
            growth_policy=resolved_growth_policy,
        )
        _print_notice("Dry-run preflight complete; continuing to live session execution.", level="success")
    else:
        _print_notice("Step 1/1: running live growth session.", level="info")

    if dry_run_first:
        _print_notice("Step 2/2: running live growth session.", level="info")
    run_command(
        tag_slug=tag_slug,
        live=True,
        growth_policy=resolved_growth_policy,
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
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    snapshot: ProbeSnapshot | None = None
    artifact_path: Path | None = None
    _print_notice(f"Starting probe run `{run_id}` for tag `{tag_slug}`.", level="info")
    try:
        try:
            _, repository = _build_runner(settings)

            async def _run() -> ProbeSnapshot:
                async with MediumAsyncClient(settings) as client:
                    runner = DailyRunner(settings=settings, client=client, repository=repository)
                    return await runner.probe(tag_slug=tag_slug)

            snapshot = asyncio.run(_run())
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
            _print_notice(f"Probe run failed: {exc}", level="error")

        ended_at = _utc_now()
        summary: dict[str, Any] = {}
        kpis: dict[str, float | int] = {}
        if snapshot is not None:
            failed_tasks = sum(
                1
                for result in snapshot.results.values()
                if result.status_code != 200 or result.has_errors
            )
            summary = {
                "duration_ms": snapshot.duration_ms,
                "task_count": len(snapshot.results),
                "failed_task_count": failed_tasks,
            }
            kpis = {
                "probe_task_count": len(snapshot.results),
                "probe_failed_task_count": failed_tasks,
            }
        artifact_payload = _build_standard_artifact_payload(
            run_id=run_id,
            command="probe",
            started_at=started_at,
            ended_at=ended_at,
            tag_slug=tag_slug,
            dry_run=True,
            status=status,
            summary=summary,
            kpis=kpis,
            error=error_payload,
        )
        payload_ok, payload_issues = validate_artifact_payload(artifact_payload)
        if not payload_ok:
            raise RuntimeError(f"probe artifact payload schema validation failed: {', '.join(payload_issues)}")
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

        if snapshot is not None:
            _render_probe(snapshot)
        _print_notice(f"Run artifact saved: {artifact_path}", level="success")
        if exit_code != 0:
            raise typer.Exit(code=exit_code)
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
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    report: ContractValidationReport | None = None
    artifact_path: Path | None = None
    _print_notice(
        "Starting contracts validation "
        f"(run_id={run_id}, strict={str(strict).lower()}, execute_reads={str(execute_reads).lower()}).",
        level="info",
    )
    try:
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
            if not report.ok:
                status = "failed"
                exit_code = 1
                error_payload = {
                    "type": "ContractValidationError",
                    "message": "contract_validation_failed",
                }
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            exit_code = 1
            error_payload = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            _print_notice(f"Contracts validation failed: {exc}", level="error")

        ended_at = _utc_now()
        summary: dict[str, Any] = {}
        kpis: dict[str, float | int] = {}
        if report is not None:
            summary = {
                "registry_operations": len(report.registry_operation_names),
                "implemented_operations": len(report.implemented_operation_names),
                "checks_passed": report.passed_count,
                "checks_failed": report.failed_count,
                "live_reads_executed": report.live_executed_count,
                "live_reads_failed": report.live_failed_count,
                "overall_ok": 1 if report.ok else 0,
            }
            kpis = {
                "contracts_failed_count": report.failed_count,
                "contracts_live_failed_count": report.live_failed_count,
                "contracts_missing_in_code": len(report.missing_in_code),
                "contracts_extra_in_code": len(report.extra_in_code),
            }

        artifact_payload = _build_standard_artifact_payload(
            run_id=run_id,
            command="contracts",
            started_at=started_at,
            ended_at=ended_at,
            tag_slug=tag_slug,
            dry_run=not execute_reads,
            status=status,
            summary=summary,
            kpis=kpis,
            error=error_payload,
        )
        payload_ok, payload_issues = validate_artifact_payload(artifact_payload)
        if not payload_ok:
            raise RuntimeError(f"contracts artifact payload schema validation failed: {', '.join(payload_issues)}")
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
        if report is not None:
            _render_contract_validation(report)
        _print_notice(f"Run artifact saved: {artifact_path}", level="success")
        if exit_code != 0:
            raise typer.Exit(code=exit_code)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("discover")
def discover_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for discovery reads.",
    ),
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Persist discovery queue updates by default. Use --dry-run for read-only discovery preview.",
    ),
    growth_sources: list[GrowthSource] | None = typer.Option(
        None,
        "--source",
        case_sensitive=False,
        help="Discovery source. Repeat option to combine sources.",
    ),
    discovery_mode: GrowthDiscoveryMode = typer.Option(
        GrowthDiscoveryMode.GENERAL,
        "--discovery-mode",
        case_sensitive=False,
        help="Legacy alias to select target-user-only discovery.",
    ),
    seed_user_refs: list[str] | None = typer.Option(
        None,
        "--seed-user",
        help="Optional seed users for seed-follower discovery. Repeat option.",
    ),
    target_user_refs: list[str] | None = typer.Option(
        None,
        "--target-user",
        help="Target user(s) whose followers should be discovered. Repeat option.",
    ),
    target_user_scan_limit: int | None = typer.Option(
        None,
        "--target-user-scan-limit",
        min=1,
        max=500,
        help="Max followers to scan per target user. Defaults to TARGET_USER_FOLLOWERS_SCAN_LIMIT.",
    ),
    auto_sync: bool | None = typer.Option(
        None,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before discovery execution. Defaults to GRAPH_SYNC_AUTO_ENABLED.",
    ),
) -> None:
    """
    Run discovery-only pipeline: collect, score, filter, evaluate, and persist execution-ready queue rows.
    """
    growth_sources = _coerce_optioninfo(growth_sources, default=None)
    discovery_mode = _coerce_optioninfo(discovery_mode, default=GrowthDiscoveryMode.GENERAL)
    seed_user_refs = _coerce_optioninfo(seed_user_refs, default=None)
    target_user_refs = _coerce_optioninfo(target_user_refs, default=None)
    target_user_scan_limit = _coerce_optioninfo(target_user_scan_limit, default=None)
    auto_sync = _coerce_optioninfo(auto_sync, default=None)

    settings = _bootstrap_settings()
    _require_session(settings)
    resolved_auto_sync = settings.graph_sync_auto_enabled if auto_sync is None else bool(auto_sync)
    resolved_growth_sources = list(
        growth_sources
        or ([GrowthSource.TARGET_USER_FOLLOWERS] if discovery_mode == GrowthDiscoveryMode.TARGET_USER_FOLLOWERS else [])
        or settings.default_growth_sources
    )
    resolved_target_user_refs = _normalize_seed_user_refs(target_user_refs)
    if GrowthSource.TARGET_USER_FOLLOWERS in resolved_growth_sources and not resolved_target_user_refs:
        _print_notice("Target-user-followers discovery requires at least one `--target-user`.", level="error")
        raise typer.Exit(code=2)

    run_id = new_run_id("discover")
    structlog_contextvars.bind_contextvars(
        run_id=run_id,
        command="discover",
        tag_slug=tag_slug,
        mode="live" if live else "dry_run",
        growth_sources=[source.value for source in resolved_growth_sources],
    )
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    outcome: DailyRunOutcome | None = None
    artifact_path: Path | None = None
    sync_outcome: GraphSyncOutcome | None = None
    runtime_client_metrics: dict[str, Any] = {}
    _print_notice(
        "Starting discovery "
        f"`{run_id}` (mode={'live' if live else 'dry-run'}, "
        f"sources={','.join(source.value for source in resolved_growth_sources)}, tag={tag_slug}).",
        level="info",
    )
    if resolved_target_user_refs:
        _print_notice(
            "Target users: "
            f"{_seed_refs_summary(resolved_target_user_refs)} "
            f"(scan_limit={target_user_scan_limit or settings.target_user_followers_scan_limit} per target).",
            level="info",
        )

    try:
        try:
            _, repository = _build_runner(settings)

            async def _run() -> DailyRunOutcome:
                nonlocal sync_outcome
                nonlocal runtime_client_metrics
                async with MediumAsyncClient(settings) as client:
                    try:
                        runner = DailyRunner(settings=settings, client=client, repository=repository)
                        if resolved_auto_sync:
                            sync_outcome_local = await runner.sync_social_graph(
                                dry_run=not live,
                                mode="auto",
                                force=False,
                            )
                            if not sync_outcome_local.skipped:
                                _render_graph_sync_outcome(sync_outcome_local)
                            sync_outcome = sync_outcome_local
                        return await runner.run_discovery_cycle(
                            tag_slug=tag_slug,
                            dry_run=not live,
                            seed_user_refs=seed_user_refs or None,
                            growth_sources=resolved_growth_sources,
                            discovery_mode=discovery_mode,
                            target_user_refs=resolved_target_user_refs,
                            target_user_scan_limit=target_user_scan_limit,
                        )
                    finally:
                        runtime_client_metrics = client.metrics_snapshot()

            outcome = asyncio.run(_run())
            if outcome is not None and sync_outcome is not None:
                outcome.kpis["graph_sync_skipped"] = 1 if sync_outcome.skipped else 0
                outcome.kpis["graph_sync_followers_count"] = sync_outcome.followers_count
                outcome.kpis["graph_sync_following_count"] = sync_outcome.following_count
                outcome.kpis["graph_sync_users_upserted_count"] = sync_outcome.users_upserted_count
                outcome.kpis["graph_sync_imported_pending_count"] = sync_outcome.imported_pending_count
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
            _print_notice(f"Discovery run failed: {exc}", level="error")

        ended_at = _utc_now()
        artifact_payload = _build_run_artifact_payload(
            run_id=run_id,
            command="discover",
            started_at=started_at,
            ended_at=ended_at,
            tag_slug=tag_slug,
            dry_run=not live,
            status=status,
            outcome=outcome,
            error=error_payload,
            client_metrics_override=runtime_client_metrics,
        )
        payload_ok, payload_issues = validate_artifact_payload(artifact_payload)
        if not payload_ok:
            raise RuntimeError(f"discover artifact payload schema validation failed: {', '.join(payload_issues)}")
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


@app.command("run")
def run_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for queue-driven growth execution context.",
    ),
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Execute live mutations by default. Use --dry-run for preview-only execution.",
    ),
    growth_policy: GrowthPolicy | None = typer.Option(
        None,
        "--policy",
        case_sensitive=False,
        help=_GROWTH_POLICY_HELP,
    ),
    growth_sources: list[GrowthSource] | None = typer.Option(
        None,
        "--source",
        case_sensitive=False,
        help="Legacy option. Discovery sources are ignored here; use `uv run bot discover`.",
    ),
    mode: GrowthMode | None = typer.Option(
        None,
        "--mode",
        case_sensitive=False,
        help="Legacy compatibility alias for growth policy.",
    ),
    discovery_mode: GrowthDiscoveryMode = typer.Option(
        GrowthDiscoveryMode.GENERAL,
        "--discovery-mode",
        case_sensitive=False,
        help="Legacy option. Discovery mode is ignored here; use `uv run bot discover`.",
    ),
    seed_user_refs: list[str] | None = typer.Option(
        None,
        "--seed-user",
        help="Legacy option. Seed users are ignored here; use `uv run bot discover`.",
    ),
    target_user_refs: list[str] | None = typer.Option(
        None,
        "--target-user",
        help="Legacy option. Target users are ignored here; use `uv run bot discover`.",
    ),
    target_user_scan_limit: int | None = typer.Option(
        None,
        "--target-user-scan-limit",
        min=1,
        max=500,
        help="Legacy option. Target-user scan limit is ignored here; use `uv run bot discover`.",
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
    auto_sync: bool | None = typer.Option(
        None,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before execution. Defaults to GRAPH_SYNC_AUTO_ENABLED.",
    ),
) -> None:
    """
    Run queue-driven growth execution only (no discovery mutations).
    """
    growth_policy = _coerce_optioninfo(growth_policy, default=None)
    growth_sources = _coerce_optioninfo(growth_sources, default=None)
    mode = _coerce_optioninfo(mode, default=None)
    discovery_mode = _coerce_optioninfo(discovery_mode, default=GrowthDiscoveryMode.GENERAL)
    seed_user_refs = _coerce_optioninfo(seed_user_refs, default=None)
    target_user_refs = _coerce_optioninfo(target_user_refs, default=None)
    target_user_scan_limit = _coerce_optioninfo(target_user_scan_limit, default=None)
    session = _coerce_optioninfo(session, default=True)
    session_minutes = _coerce_optioninfo(session_minutes, default=None)
    target_follows = _coerce_optioninfo(target_follows, default=None)
    session_max_passes = _coerce_optioninfo(session_max_passes, default=None)
    auto_sync = _coerce_optioninfo(auto_sync, default=None)

    settings = _bootstrap_settings()
    _require_session(settings)
    resolved_growth_policy = _resolve_growth_policy_option(
        growth_policy=growth_policy,
        mode=mode,
        default=settings.default_growth_policy,
    )
    runtime_settings, forced_public_touch_enable = _runtime_settings_for_growth_policy(
        settings,
        growth_policy=resolved_growth_policy,
    )
    if forced_public_touch_enable:
        _print_notice(
            "Policy coherence: enabling the selected pre-follow public touch for this run (runtime override only).",
            level="info",
        )
    if resolved_growth_policy in {GrowthPolicy.WARM_ENGAGE_COMMENT, GrowthPolicy.WARM_ENGAGE_HIGHLIGHT}:
        _print_notice(
            "Public-touch config: "
            f"comments={'enabled' if runtime_settings.enable_pre_follow_comment else 'disabled'}, "
            f"comment_probability={round(runtime_settings.pre_follow_comment_probability, 3)}, "
            f"comment_budget_per_day={runtime_settings.max_comment_actions_per_day}, "
            f"highlights={'enabled' if runtime_settings.enable_pre_follow_highlight else 'disabled'}, "
            f"highlight_probability={round(runtime_settings.pre_follow_highlight_probability, 3)}, "
            f"highlight_budget_per_day={runtime_settings.max_highlight_actions_per_day}.",
            level="info",
        )
    resolved_auto_sync = runtime_settings.graph_sync_auto_enabled if auto_sync is None else bool(auto_sync)
    ignored_discovery_inputs = bool(
        growth_sources
        or seed_user_refs
        or target_user_refs
        or target_user_scan_limit is not None
        or discovery_mode != GrowthDiscoveryMode.GENERAL
    )
    if ignored_discovery_inputs:
        _print_notice(
            "Discovery inputs are ignored in growth execution mode. Run `uv run bot discover` first.",
            level="warning",
        )

    run_id = new_run_id("run")
    structlog_contextvars.bind_contextvars(
        run_id=run_id,
        command="run",
        tag_slug=tag_slug,
        mode="live" if live else "dry_run",
        growth_policy=resolved_growth_policy.value,
    )
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    outcome: DailyRunOutcome | None = None
    artifact_path: Path | None = None
    sync_outcome: GraphSyncOutcome | None = None
    runtime_client_metrics: dict[str, Any] = {}
    live_session_enabled = live and session
    execution_mode_label = "live-session" if live_session_enabled else "live-single" if live else "dry-run"
    _print_notice(
        "Starting run "
        f"`{run_id}` (execution={execution_mode_label}, policy={resolved_growth_policy.value}, "
        f"queue=execution-ready, tag={tag_slug}).",
        level="info",
    )

    try:
        try:
            _, repository = _build_runner(runtime_settings)

            async def _run() -> DailyRunOutcome:
                nonlocal sync_outcome
                nonlocal runtime_client_metrics
                async with MediumAsyncClient(runtime_settings) as client:
                    try:
                        runner = DailyRunner(settings=runtime_settings, client=client, repository=repository)
                        if resolved_auto_sync:
                            sync_outcome_local = await runner.sync_social_graph(
                                dry_run=not live,
                                mode="auto",
                                force=False,
                            )
                            if not sync_outcome_local.skipped:
                                _render_graph_sync_outcome(sync_outcome_local)
                            sync_outcome = sync_outcome_local
                        if live_session_enabled:
                            resolved_session_minutes = session_minutes or runtime_settings.live_session_duration_minutes
                            resolved_target_follows = target_follows or runtime_settings.live_session_target_follow_attempts
                            resolved_min_follows = min(
                                runtime_settings.live_session_min_follow_attempts,
                                resolved_target_follows,
                            )
                            resolved_session_max_passes = session_max_passes or runtime_settings.live_session_max_passes
                            _print_notice(
                                "Live session targets: "
                                f"duration={resolved_session_minutes}m, "
                                f"follow_attempts_min={resolved_min_follows}, "
                                f"follow_attempts={resolved_target_follows}, "
                                f"max_passes={resolved_session_max_passes}, "
                                f"policy={resolved_growth_policy.value}, "
                                "queue=execution-ready.",
                                level="info",
                            )
                            return await runner.run_live_session(
                                tag_slug=tag_slug,
                                target_follow_attempts=resolved_target_follows,
                                max_duration_minutes=resolved_session_minutes,
                                max_passes=resolved_session_max_passes,
                                growth_policy=resolved_growth_policy,
                                discovery_enabled=False,
                            )
                        return await runner.run_growth_queue_cycle(
                            tag_slug=tag_slug,
                            dry_run=not live,
                            growth_policy=resolved_growth_policy,
                        )
                    finally:
                        runtime_client_metrics = client.metrics_snapshot()

            outcome = asyncio.run(_run())
            if outcome is not None and sync_outcome is not None:
                outcome.kpis["graph_sync_skipped"] = 1 if sync_outcome.skipped else 0
                outcome.kpis["graph_sync_followers_count"] = sync_outcome.followers_count
                outcome.kpis["graph_sync_following_count"] = sync_outcome.following_count
                outcome.kpis["graph_sync_users_upserted_count"] = sync_outcome.users_upserted_count
                outcome.kpis["graph_sync_imported_pending_count"] = sync_outcome.imported_pending_count
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
            client_metrics_override=runtime_client_metrics,
        )
        payload_ok, payload_issues = validate_artifact_payload(artifact_payload)
        if not payload_ok:
            raise RuntimeError(f"run artifact payload schema validation failed: {', '.join(payload_issues)}")
        artifact_path = write_run_artifact(
            artifacts_dir=runtime_settings.run_artifacts_dir,
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
            queue_ready_before = outcome.kpis.get("growth_queue_ready_before_discovery")
            if (
                isinstance(queue_ready_before, (int, float))
                and int(queue_ready_before) <= 0
                and outcome.executed_candidates == 0
            ):
                _print_notice(
                    "Growth queue is empty. Run `uv run bot discover` first.",
                    level="warning",
                )
        _print_notice(f"Run artifact saved: {artifact_path}", level="success")

        if exit_code != 0:
            raise typer.Exit(code=exit_code)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("queue")
def queue_command() -> None:
    """
    Show current growth queue counts (ready/deferred).
    """
    settings = _bootstrap_settings()
    _, repository = _build_runner(settings)
    _render_growth_queue_status(repository.growth_queue_state_counts())


@app.command("db-hygiene")
def db_hygiene_command(
    live: bool = typer.Option(
        False,
        "--live/--dry-run",
        help="Preview deletions in dry-run mode by default. Use --live to apply cleanup.",
    ),
    vacuum: bool = typer.Option(
        False,
        "--vacuum",
        help="Force VACUUM after live cleanup. DB_HYGIENE_VACUUM_AFTER_CLEANUP is also respected.",
    ),
) -> None:
    """
    Prune stale operational data with retention windows and optional VACUUM.
    """
    settings = _bootstrap_settings()
    _, repository = _build_runner(settings)
    resolved_vacuum = bool(live and (vacuum or settings.db_hygiene_vacuum_after_cleanup))
    result = repository.run_db_hygiene(
        action_log_retention_days=settings.db_hygiene_action_log_retention_days,
        graph_sync_runs_retention_days=settings.db_hygiene_graph_sync_runs_retention_days,
        candidate_reconciliation_retention_days=settings.db_hygiene_candidate_reconciliation_retention_days,
        follow_cycle_terminal_retention_days=settings.db_hygiene_follow_cycle_terminal_retention_days,
        snapshots_retention_days=settings.db_hygiene_snapshots_retention_days,
        queue_followed_after_days=settings.growth_queue_prune_followed_after_days,
        queue_rejected_after_days=settings.growth_queue_prune_rejected_after_days,
        queue_stale_after_days=settings.growth_queue_prune_stale_after_days,
        dry_run=not live,
        vacuum=resolved_vacuum,
    )
    _render_db_hygiene_status(result, mode="live" if live else "dry-run")
    if live:
        _print_notice(
            f"DB hygiene completed. Deleted {int(result.get('total', 0))} row(s).",
            level="success",
        )
        return
    _print_notice("Dry-run complete. Re-run with --live to apply cleanup.", level="warning")


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
    auto_sync: bool = typer.Option(
        True,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before cleanup execution.",
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
    sync_outcome: GraphSyncOutcome | None = None
    runtime_client_metrics: dict[str, Any] = {}
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
                nonlocal sync_outcome
                nonlocal runtime_client_metrics
                async with MediumAsyncClient(settings) as client:
                    try:
                        runner = DailyRunner(settings=settings, client=client, repository=repository)
                        if auto_sync:
                            sync_outcome_local = await runner.sync_social_graph(
                                dry_run=not live,
                                mode="auto",
                                force=False,
                            )
                            if not sync_outcome_local.skipped:
                                _render_graph_sync_outcome(sync_outcome_local)
                            sync_outcome = sync_outcome_local
                        return await runner.run_cleanup_only(
                            dry_run=not live,
                            max_unfollows=resolved_limit,
                        )
                    finally:
                        runtime_client_metrics = client.metrics_snapshot()

            outcome = asyncio.run(_run())
            if outcome is not None and sync_outcome is not None:
                outcome.kpis["graph_sync_skipped"] = 1 if sync_outcome.skipped else 0
                outcome.kpis["graph_sync_followers_count"] = sync_outcome.followers_count
                outcome.kpis["graph_sync_following_count"] = sync_outcome.following_count
                outcome.kpis["graph_sync_users_upserted_count"] = sync_outcome.users_upserted_count
                outcome.kpis["graph_sync_imported_pending_count"] = sync_outcome.imported_pending_count
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
            client_metrics_override=runtime_client_metrics,
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


@app.command("sync")
def sync_command(
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Persist graph sync cache in both modes; dry-run labels observability mode only.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Bypass freshness window and force a full graph sync.",
    ),
    full: bool = typer.Option(
        True,
        "--full/--respect-pagination-config",
        help="Use full pagination to fetch complete followers/following sets.",
    ),
) -> None:
    """
    Sync own followers/following graph into local cache for faster decisions.
    """
    settings = _bootstrap_settings()
    _require_session(settings)

    run_id = new_run_id("sync")
    structlog_contextvars.bind_contextvars(
        run_id=run_id,
        command="sync",
        mode="live" if live else "dry_run",
    )
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    outcome: GraphSyncOutcome | None = None
    artifact_path: Path | None = None
    runtime_client_metrics: dict[str, Any] = {}
    _print_notice(
        f"Starting graph sync `{run_id}` "
        f"(mode={'live' if live else 'dry-run'}, force={str(force).lower()}, full={str(full).lower()}).",
        level="info",
    )
    try:
        try:
            _, repository = _build_runner(settings)
            if full:
                settings.graph_sync_full_pagination = True

            async def _run() -> GraphSyncOutcome:
                nonlocal runtime_client_metrics
                async with MediumAsyncClient(settings) as client:
                    try:
                        runner = DailyRunner(settings=settings, client=client, repository=repository)
                        return await runner.sync_social_graph(
                            dry_run=not live,
                            mode="manual",
                            force=force,
                        )
                    finally:
                        runtime_client_metrics = client.metrics_snapshot()

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
            _print_notice(f"Graph sync failed: {exc}", level="error")

        ended_at = _utc_now()
        summary: dict[str, Any] = {}
        kpis: dict[str, float | int] = {}
        if outcome is not None:
            summary = {
                "skipped": outcome.skipped,
                "skip_reason": outcome.skip_reason,
                "followers_count": outcome.followers_count,
                "following_count": outcome.following_count,
                "users_upserted_count": outcome.users_upserted_count,
                "imported_pending_count": outcome.imported_pending_count,
                "duration_ms": outcome.duration_ms,
                "following_source": outcome.used_following_source or "-",
            }
            kpis = {
                "graph_sync_skipped": 1 if outcome.skipped else 0,
                "graph_sync_followers_count": outcome.followers_count,
                "graph_sync_following_count": outcome.following_count,
                "graph_sync_users_upserted_count": outcome.users_upserted_count,
                "graph_sync_imported_pending_count": outcome.imported_pending_count,
            }
        artifact_payload = _build_standard_artifact_payload(
            run_id=run_id,
            command="sync",
            started_at=started_at,
            ended_at=ended_at,
            tag_slug="social_graph",
            dry_run=not live,
            status=status,
            summary=summary,
            kpis=kpis,
            client_metrics=runtime_client_metrics,
            error=error_payload,
        )
        payload_ok, payload_issues = validate_artifact_payload(artifact_payload)
        if not payload_ok:
            raise RuntimeError(f"sync artifact payload schema validation failed: {', '.join(payload_issues)}")
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
            _render_graph_sync_outcome(outcome)
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
    auto_sync: bool = typer.Option(
        True,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before reconcile execution.",
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
    log = structlog.get_logger(__name__)
    started_at = _utc_now()
    status = "success"
    error_payload: dict[str, str] | None = None
    exit_code = 0
    outcome: ReconcileOutcome | None = None
    sync_outcome: GraphSyncOutcome | None = None
    artifact_path: Path | None = None
    runtime_client_metrics: dict[str, Any] = {}
    _print_notice(
        f"Starting reconcile run `{run_id}` (mode={'live' if live else 'dry-run'}, limit={max_users}, page_size={page_size}).",
        level="info",
    )
    try:
        try:
            _, repository = _build_runner(settings)

            async def _run() -> ReconcileOutcome:
                nonlocal sync_outcome
                nonlocal runtime_client_metrics
                async with MediumAsyncClient(settings) as client:
                    try:
                        runner = DailyRunner(settings=settings, client=client, repository=repository)
                        if auto_sync:
                            sync_outcome_local = await runner.sync_social_graph(
                                dry_run=not live,
                                mode="auto",
                                force=False,
                            )
                            if not sync_outcome_local.skipped:
                                _render_graph_sync_outcome(sync_outcome_local)
                            sync_outcome = sync_outcome_local
                        return await runner.reconcile_follow_states(
                            dry_run=not live,
                            max_users=max_users,
                            page_size=page_size,
                        )
                    finally:
                        runtime_client_metrics = client.metrics_snapshot()

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
            _print_notice(f"Reconcile run failed: {exc}", level="error")

        ended_at = _utc_now()
        summary: dict[str, Any] = {}
        kpis: dict[str, float | int] = {}
        if outcome is not None:
            summary = {
                "scanned_users": outcome.scanned_users,
                "updated_users": outcome.updated_users,
                "following_count": outcome.following_count,
                "not_following_count": outcome.not_following_count,
                "unknown_count": outcome.unknown_count,
            }
            kpis = {
                "reconcile_following_count": outcome.following_count,
                "reconcile_not_following_count": outcome.not_following_count,
                "reconcile_unknown_count": outcome.unknown_count,
            }
        if sync_outcome is not None:
            kpis["graph_sync_skipped"] = 1 if sync_outcome.skipped else 0
            kpis["graph_sync_followers_count"] = sync_outcome.followers_count
            kpis["graph_sync_following_count"] = sync_outcome.following_count
            kpis["graph_sync_users_upserted_count"] = sync_outcome.users_upserted_count
            kpis["graph_sync_imported_pending_count"] = sync_outcome.imported_pending_count
        artifact_payload = _build_standard_artifact_payload(
            run_id=run_id,
            command="reconcile",
            started_at=started_at,
            ended_at=ended_at,
            tag_slug="reconcile",
            dry_run=not live,
            status=status,
            summary=summary,
            kpis=kpis,
            client_metrics=runtime_client_metrics,
            error=error_payload,
        )
        payload_ok, payload_issues = validate_artifact_payload(artifact_payload)
        if not payload_ok:
            raise RuntimeError(f"reconcile artifact payload schema validation failed: {', '.join(payload_issues)}")
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
            _render_reconcile_outcome(outcome)
        _print_notice(f"Run artifact saved: {artifact_path}", level="success")
        if exit_code != 0:
            raise typer.Exit(code=exit_code)
    finally:
        structlog_contextvars.clear_contextvars()


@app.command("status")
def status_command(
    emit_artifact: bool = typer.Option(
        False,
        "--emit-artifact/--no-emit-artifact",
        help="Emit a status command artifact in addition to rendering.",
    ),
) -> None:
    """
    Show last-run diagnostic health from the latest run artifact.
    """
    settings = _bootstrap_settings()
    latest = read_latest_run_artifact(
        settings.run_artifacts_dir,
        exclude_commands={"status", "artifacts_validate"},
    )
    if not latest:
        _print_notice(
            f"No run artifacts found in {settings.run_artifacts_dir}. Execute `uv run bot discover` or `uv run bot run` first.",
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
    _render_status(artifact, artifact_path=source, settings=settings)
    if not emit_artifact:
        return

    run_id = new_run_id("status")
    started_at = _utc_now()
    ended_at = _utc_now()
    summary = {
        "source_artifact": str(source),
        "source_status": str(artifact.get("status", "-")),
        "source_health": str(artifact.get("health", "-")),
    }
    payload = _build_standard_artifact_payload(
        run_id=run_id,
        command="status",
        started_at=started_at,
        ended_at=ended_at,
        tag_slug=str(artifact.get("tag_slug", "status")),
        dry_run=None,
        status="success",
        summary=summary,
    )
    payload_ok, payload_issues = validate_artifact_payload(payload)
    if not payload_ok:
        raise RuntimeError(f"status artifact payload schema validation failed: {', '.join(payload_issues)}")
    path = write_run_artifact(
        artifacts_dir=settings.run_artifacts_dir,
        run_id=run_id,
        payload=payload,
    )
    _print_notice(f"Run artifact saved: {path}", level="success")


@artifacts_app.command("validate")
def artifacts_validate_command(
    artifact_path: Path | None = typer.Option(
        None,
        "--path",
        help="Optional path to a run artifact. Defaults to latest artifact in RUN_ARTIFACTS_DIR.",
    ),
    emit_artifact: bool = typer.Option(
        False,
        "--emit-artifact/--no-emit-artifact",
        help="Emit an artifacts-validate command artifact in addition to validation output.",
    ),
) -> None:
    """
    Validate run artifact schema compatibility and contract shape.
    """
    settings = _bootstrap_settings()
    if artifact_path is None:
        latest = read_latest_run_artifact(
            settings.run_artifacts_dir,
            exclude_commands={"status", "artifacts_validate"},
        )
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
    if not emit_artifact:
        return

    run_id = new_run_id("artifacts_validate")
    started_at = _utc_now()
    ended_at = _utc_now()
    payload = _build_standard_artifact_payload(
        run_id=run_id,
        command="artifacts_validate",
        started_at=started_at,
        ended_at=ended_at,
        tag_slug="artifacts",
        dry_run=True,
        status="success",
        summary={"validated_artifact": str(source)},
    )
    payload_ok, payload_issues = validate_artifact_payload(payload)
    if not payload_ok:
        raise RuntimeError(f"artifacts_validate payload schema validation failed: {', '.join(payload_issues)}")
    path = write_run_artifact(
        artifacts_dir=settings.run_artifacts_dir,
        run_id=run_id,
        payload=payload,
    )
    _print_notice(f"Run artifact saved: {path}", level="success")


@growth_app.command("discover")
def growth_discover_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for discovery reads.",
    ),
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Persist discovery queue updates by default. Use --dry-run for read-only preview.",
    ),
    growth_sources: list[GrowthSource] | None = typer.Option(
        None,
        "--source",
        case_sensitive=False,
        help="Discovery source. Repeat option to combine sources.",
    ),
    discovery_mode: GrowthDiscoveryMode = typer.Option(
        GrowthDiscoveryMode.GENERAL,
        "--discovery-mode",
        case_sensitive=False,
        help="Legacy alias to select target-user-only discovery.",
    ),
    seed_user_refs: list[str] | None = typer.Option(
        None,
        "--seed-user",
        help="Optional seed users for seed-follower discovery. Repeat option.",
    ),
    target_user_refs: list[str] | None = typer.Option(
        None,
        "--target-user",
        help="Target user(s) whose followers should be discovered. Repeat option.",
    ),
    target_user_scan_limit: int | None = typer.Option(
        None,
        "--target-user-scan-limit",
        min=1,
        max=500,
        help="Max followers to scan per target user. Defaults to TARGET_USER_FOLLOWERS_SCAN_LIMIT.",
    ),
    auto_sync: bool | None = typer.Option(
        None,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before discovery execution. Defaults to GRAPH_SYNC_AUTO_ENABLED.",
    ),
) -> None:
    """
    Discovery-only alias: fill the queue with execution-ready candidates.
    """
    discover_command(
        tag_slug=tag_slug,
        live=live,
        growth_sources=growth_sources,
        discovery_mode=discovery_mode,
        seed_user_refs=seed_user_refs,
        target_user_refs=target_user_refs,
        target_user_scan_limit=target_user_scan_limit,
        auto_sync=auto_sync,
    )


@growth_app.command("queue")
def growth_queue_command() -> None:
    """
    Show growth queue counts.
    """
    queue_command()


@growth_app.command("session")
def growth_session_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for queue-driven growth execution context.",
    ),
    growth_policy: GrowthPolicy | None = typer.Option(
        None,
        "--policy",
        case_sensitive=False,
        help=_GROWTH_POLICY_HELP,
    ),
    mode: GrowthMode | None = typer.Option(
        None,
        "--mode",
        case_sensitive=False,
        help="Legacy compatibility alias for growth policy.",
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
    auto_sync: bool | None = typer.Option(
        None,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before execution. Defaults to GRAPH_SYNC_AUTO_ENABLED.",
    ),
) -> None:
    """
    Run a live multi-cycle growth session.
    """
    run_command(
        tag_slug=tag_slug,
        live=True,
        growth_policy=growth_policy,
        mode=mode,
        session=True,
        session_minutes=session_minutes,
        target_follows=target_follows,
        session_max_passes=session_max_passes,
        auto_sync=auto_sync,
    )


@growth_app.command("cycle")
def growth_cycle_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for queue-driven growth execution context.",
    ),
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Execute live mutations by default. Use --dry-run for preview-only execution.",
    ),
    growth_policy: GrowthPolicy | None = typer.Option(
        None,
        "--policy",
        case_sensitive=False,
        help=_GROWTH_POLICY_HELP,
    ),
    mode: GrowthMode | None = typer.Option(
        None,
        "--mode",
        case_sensitive=False,
        help="Legacy compatibility alias for growth policy.",
    ),
    auto_sync: bool | None = typer.Option(
        None,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before execution. Defaults to GRAPH_SYNC_AUTO_ENABLED.",
    ),
) -> None:
    """
    Run a single growth cycle in live or dry-run mode.
    """
    run_command(
        tag_slug=tag_slug,
        live=live,
        growth_policy=growth_policy,
        mode=mode,
        session=False,
        auto_sync=auto_sync,
    )


@growth_app.command("preflight")
def growth_preflight_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for queue-driven growth execution context.",
    ),
    growth_policy: GrowthPolicy | None = typer.Option(
        None,
        "--policy",
        case_sensitive=False,
        help=_GROWTH_POLICY_HELP,
    ),
    mode: GrowthMode | None = typer.Option(
        None,
        "--mode",
        case_sensitive=False,
        help="Legacy compatibility alias for growth policy.",
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
    auto_sync: bool | None = typer.Option(
        None,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before execution. Defaults to GRAPH_SYNC_AUTO_ENABLED.",
    ),
) -> None:
    """
    Run a dry-run preflight, then continue into a live growth session.
    """
    _print_notice("Step 1/2: running dry-run growth preflight.", level="info")
    run_command(
        tag_slug=tag_slug,
        live=False,
        growth_policy=growth_policy,
        mode=mode,
        session=False,
        auto_sync=auto_sync,
    )
    _print_notice("Step 2/2: running live growth session.", level="info")
    run_command(
        tag_slug=tag_slug,
        live=True,
        growth_policy=growth_policy,
        mode=mode,
        session=True,
        session_minutes=session_minutes,
        target_follows=target_follows,
        session_max_passes=session_max_passes,
        auto_sync=auto_sync,
    )


@growth_app.command("followers")
def growth_followers_command(
    target_user_refs: list[str] = typer.Option(
        ...,
        "--target-user",
        help="Target user(s) whose followers should be evaluated. Repeat option. Supports @username or id:<user_id>.",
    ),
    target_user_scan_limit: int | None = typer.Option(
        None,
        "--scan-limit",
        min=1,
        max=500,
        help="Max followers to scan per target user. Defaults to TARGET_USER_FOLLOWERS_SCAN_LIMIT.",
    ),
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Persist discovery queue updates by default. Use --dry-run for read-only preview.",
    ),
    auto_sync: bool = typer.Option(
        False,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before execution.",
    ),
) -> None:
    """
    Discovery alias: harvest followers of supplied target users into the growth queue.
    """
    discover_command(
        tag_slug="target_user_followers",
        live=live,
        growth_sources=[GrowthSource.TARGET_USER_FOLLOWERS],
        discovery_mode=GrowthDiscoveryMode.TARGET_USER_FOLLOWERS,
        target_user_refs=target_user_refs,
        target_user_scan_limit=target_user_scan_limit,
        auto_sync=auto_sync,
    )


@unfollow_app.command("cleanup")
def unfollow_cleanup_command(
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
    auto_sync: bool = typer.Option(
        True,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before cleanup execution.",
    ),
) -> None:
    """
    Run cleanup-only unfollow maintenance for overdue non-followback users.
    """
    cleanup_command(live=live, limit=limit, auto_sync=auto_sync)


@maintenance_app.command("reconcile")
def maintenance_reconcile_command(
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
    auto_sync: bool = typer.Option(
        True,
        "--auto-sync/--no-auto-sync",
        help="Refresh local social-graph cache before reconcile execution.",
    ),
) -> None:
    """
    Reconcile local follow state against live UserViewerEdge checks.
    """
    reconcile_command(
        live=live,
        max_users=max_users,
        page_size=page_size,
        auto_sync=auto_sync,
    )


@maintenance_app.command("sync")
def maintenance_sync_command(
    live: bool = typer.Option(
        True,
        "--live/--dry-run",
        help="Persist graph sync cache in both modes; dry-run labels observability mode only.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Bypass freshness window and force a full graph sync.",
    ),
    full: bool = typer.Option(
        True,
        "--full/--respect-pagination-config",
        help="Use full pagination to fetch complete followers/following sets.",
    ),
) -> None:
    """
    Sync own followers/following graph into local cache for faster decisions.
    """
    sync_command(live=live, force=force, full=full)


@maintenance_app.command("db-hygiene")
def maintenance_db_hygiene_command(
    live: bool = typer.Option(
        False,
        "--live/--dry-run",
        help="Preview deletions in dry-run mode by default. Use --live to apply cleanup.",
    ),
    vacuum: bool = typer.Option(
        False,
        "--vacuum",
        help="Force VACUUM after live cleanup. DB_HYGIENE_VACUUM_AFTER_CLEANUP is also respected.",
    ),
) -> None:
    """
    Maintenance alias: prune stale operational DB rows with retention windows.
    """
    db_hygiene_command(live=live, vacuum=vacuum)


@diagnostics_app.command("probe")
def diagnostics_probe_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for read-only probe operations.",
    ),
) -> None:
    """
    Probe read-only GraphQL operations for connectivity and contract health.
    """
    probe_command(tag_slug=tag_slug)


@diagnostics_app.command("contracts")
def diagnostics_contracts_command(
    tag_slug: str = typer.Option(
        "programming",
        "--tag",
        help="Topic tag slug used for optional live read execution context.",
    ),
    strict: bool = typer.Option(
        True,
        "--strict/--allow-soft-fail",
        help="Fail on missing registry parity by default.",
    ),
    execute_reads: bool = typer.Option(
        False,
        "--execute-reads/--no-execute-reads",
        help="Execute safe live read checks in addition to parity validation.",
    ),
    newsletter_slug: str | None = typer.Option(
        None,
        "--newsletter-slug",
        help="Newsletter slug required for live NewsletterV3 viewer-edge checks.",
    ),
    newsletter_username: str | None = typer.Option(
        None,
        "--newsletter-username",
        help="Optional newsletter owner username for live viewer-edge checks.",
    ),
) -> None:
    """
    Validate implementation operations against the capture registry.
    """
    contracts_command(
        tag_slug=tag_slug,
        strict=strict,
        execute_reads=execute_reads,
        newsletter_slug=newsletter_slug,
        newsletter_username=newsletter_username,
    )


@observe_app.command("status")
def observe_status_command(
    emit_artifact: bool = typer.Option(
        False,
        "--emit-artifact/--no-emit-artifact",
        help="Emit a status command artifact in addition to rendering.",
    ),
) -> None:
    """
    Show last-run diagnostic health from the latest run artifact.
    """
    status_command(emit_artifact=emit_artifact)


@observe_app.command("queue")
def observe_queue_command() -> None:
    """
    Show current growth queue counts.
    """
    queue_command()


@observe_app.command("validate-artifact")
def observe_validate_artifact_command(
    artifact_path: Path | None = typer.Option(
        None,
        "--path",
        help="Optional path to a run artifact. Defaults to latest artifact in RUN_ARTIFACTS_DIR.",
    ),
    emit_artifact: bool = typer.Option(
        False,
        "--emit-artifact/--no-emit-artifact",
        help="Emit an artifacts-validate command artifact in addition to validation output.",
    ),
) -> None:
    """
    Validate run artifact schema compatibility and contract shape.
    """
    artifacts_validate_command(artifact_path=artifact_path, emit_artifact=emit_artifact)


if __name__ == "__main__":
    app()
