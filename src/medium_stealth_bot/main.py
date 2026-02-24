import asyncio
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from medium_stealth_bot import __version__
from medium_stealth_bot.auth import interactive_auth, upsert_env_file
from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.database import Database
from medium_stealth_bot.logging import configure_logging
from medium_stealth_bot.logic import DailyRunner
from medium_stealth_bot.models import AuthSessionMaterial, DailyRunOutcome, ProbeSnapshot
from medium_stealth_bot.repository import ActionRepository
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
    if outcome.decision_log:
        table = Table(title="Decision Log (sample)")
        table.add_column("#")
        table.add_column("Decision")
        for idx, item in enumerate(outcome.decision_log[:12], start=1):
            table.add_row(str(idx), item)
        console.print(table)
    if outcome.probe:
        _render_probe(outcome.probe)


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

    _, repository = _build_runner(settings)

    async def _run() -> ProbeSnapshot:
        async with MediumAsyncClient(settings) as client:
            runner = DailyRunner(settings=settings, client=client, repository=repository)
            return await runner.probe(tag_slug=tag_slug)

    snapshot = asyncio.run(_run())
    _render_probe(snapshot)


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
    _render_daily_run(outcome)


if __name__ == "__main__":
    app()
