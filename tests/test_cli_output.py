from pathlib import Path

import pytest
from typer.testing import CliRunner

import medium_stealth_bot.main as cli_main
from medium_stealth_bot.logging import configure_logging
from medium_stealth_bot.main import app
from medium_stealth_bot.safety import RiskHaltError

runner = CliRunner()


def _base_env(tmp_path: Path) -> dict[str, str]:
    data_dir = tmp_path / ".data"
    return {
        "DATA_DIR": str(data_dir),
        "DB_PATH": str(data_dir / "medium-stealth-bot.db"),
        "RUN_ARTIFACTS_DIR": str(data_dir / "runs"),
        "PLAYWRIGHT_PROFILE_DIR": str(data_dir / "playwright-profile"),
    }


@pytest.fixture(autouse=True)
def _reset_structlog_output() -> None:
    configure_logging("INFO", "pretty")
    yield
    configure_logging("INFO", "pretty")


def test_root_help_has_clear_description(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["--help"], env=_base_env(tmp_path))
    assert result.exit_code == 0
    assert "Local-first Medium automation CLI" in result.stdout
    assert "cleanup" in result.stdout


def test_run_without_session_prints_actionable_error(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["run", "--dry-run"], env=_base_env(tmp_path))
    assert result.exit_code == 1
    assert "ERROR: No MEDIUM_SESSION found." in result.stdout
    assert "uv run bot auth" in result.stdout


def test_cleanup_without_session_prints_actionable_error(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["cleanup", "--dry-run"], env=_base_env(tmp_path))
    assert result.exit_code == 1
    assert "ERROR: No MEDIUM_SESSION found." in result.stdout
    assert "uv run bot auth" in result.stdout


def test_status_without_artifacts_shows_next_step(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["status"], env=_base_env(tmp_path))
    assert result.exit_code == 0
    assert "WARNING: No run artifacts found" in result.stdout
    assert "Execute `uv run bot run`" in result.stdout
    assert "first." in result.stdout


def test_artifacts_validate_missing_path_is_clear(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        ["artifacts", "validate", "--path", str(tmp_path / "missing-artifact.json")],
        env=_base_env(tmp_path),
    )
    assert result.exit_code == 1
    assert "ERROR: Failed to read artifact:" in result.stdout


def test_auth_import_writes_env(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    env_path = tmp_path / ".env"
    result = runner.invoke(
        app,
        [
            "auth-import",
            "--cookie-header",
            "sid=session123; uid=user123; xsrf=csrf123",
            "--env-path",
            str(env_path),
        ],
        env=_base_env(tmp_path),
    )
    assert result.exit_code == 0
    assert "SUCCESS: Updated env file:" in result.stdout
    text = env_path.read_text(encoding="utf-8")
    assert "MEDIUM_SESSION=" in text
    assert "sid=session123" in text
    assert "MEDIUM_CSRF=" in text
    assert "MEDIUM_USER_REF=" in text


def test_auth_import_requires_sid_cookie(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app,
        ["auth-import", "--cookie-header", "uid=user123; xsrf=csrf123"],
        env=_base_env(tmp_path),
    )
    assert result.exit_code == 1
    assert "ERROR: Cookie import failed:" in result.stdout
    assert "sid cookie not found" in result.stdout


def test_run_soft_halt_exits_zero_and_warns(tmp_path: Path, monkeypatch) -> None:
    class FakeClient:
        def __init__(self, settings):
            self.settings = settings

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeRunner:
        def __init__(self, settings, client, repository):
            self.settings = settings
            self.client = client
            self.repository = repository

        async def run_daily_cycle(self, *, tag_slug: str, dry_run: bool, seed_user_refs):
            raise RiskHaltError(
                reason="challenge_detected",
                task_name="topic_latest_stories",
                detail="status=503",
                consecutive_failures=0,
            )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_main, "_build_runner", lambda settings: (None, object()))
    monkeypatch.setattr(cli_main, "MediumAsyncClient", FakeClient)
    monkeypatch.setattr(cli_main, "DailyRunner", FakeRunner)

    env = _base_env(tmp_path) | {
        "MEDIUM_SESSION": "sid=fake",
        "RISK_HALT_MODE": "soft",
    }
    result = runner.invoke(app, ["run", "--dry-run"], env=env)
    assert result.exit_code == 0
    assert "WARNING: Run paused by safety guardrails (soft mode)." in result.stdout


def test_run_hard_halt_exits_nonzero(tmp_path: Path, monkeypatch) -> None:
    class FakeClient:
        def __init__(self, settings):
            self.settings = settings

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

    class FakeRunner:
        def __init__(self, settings, client, repository):
            self.settings = settings
            self.client = client
            self.repository = repository

        async def run_daily_cycle(self, *, tag_slug: str, dry_run: bool, seed_user_refs):
            raise RiskHaltError(
                reason="challenge_detected",
                task_name="topic_latest_stories",
                detail="status=503",
                consecutive_failures=0,
            )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_main, "_build_runner", lambda settings: (None, object()))
    monkeypatch.setattr(cli_main, "MediumAsyncClient", FakeClient)
    monkeypatch.setattr(cli_main, "DailyRunner", FakeRunner)

    env = _base_env(tmp_path) | {
        "MEDIUM_SESSION": "sid=fake",
        "RISK_HALT_MODE": "hard",
    }
    result = runner.invoke(app, ["run", "--dry-run"], env=env)
    assert result.exit_code == 2
    assert "ERROR: Run halted by safety guardrails." in result.stdout
