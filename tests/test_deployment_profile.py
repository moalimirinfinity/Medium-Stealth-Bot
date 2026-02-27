from medium_stealth_bot.deployment import validate_production_profile
from medium_stealth_bot.settings import AppSettings


def test_validate_production_profile_passes_for_safe_defaults() -> None:
    settings = AppSettings(
        APP_ENV="prod",
        LIVE_SESSION_DURATION_MINUTES="120",
        LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS="120",
        LIVE_SESSION_MIN_FOLLOW_ATTEMPTS="80",
        MAX_FOLLOW_ACTIONS_PER_RUN="30",
        MAX_MUTATIONS_PER_10_MINUTES="24",
        MIN_ACTION_GAP_SECONDS="20",
        MIN_VERIFY_GAP_SECONDS="2",
        PASS_COOLDOWN_MIN_SECONDS="15",
        ENABLE_PACING_AUTO_CLAMP="true",
    )

    assert validate_production_profile(settings) == []


def test_validate_production_profile_reports_rule_violations() -> None:
    settings = AppSettings(
        _env_file=None,
        APP_ENV="dev",
    )

    issues = validate_production_profile(settings)
    issue_keys = {item.key for item in issues}

    assert "APP_ENV" in issue_keys
    assert "LIVE_SESSION_DURATION_MINUTES" in issue_keys
