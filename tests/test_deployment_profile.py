from medium_stealth_bot.deployment import validate_production_profile
from medium_stealth_bot.settings import AppSettings


def test_validate_production_profile_passes_for_safe_defaults() -> None:
    settings = AppSettings(
        APP_ENV="prod",
        CLIENT_MODE="stealth",
        DAY_BOUNDARY_POLICY="utc",
        CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS="true",
        ENABLE_CHALLENGE_HALT="true",
        ENABLE_SESSION_EXPIRY_HALT="true",
        RISK_HALT_CONSECUTIVE_FAILURES="2",
        MAX_ACTIONS_PER_DAY="30",
        MAX_SUBSCRIBE_ACTIONS_PER_DAY="15",
        MAX_UNFOLLOW_ACTIONS_PER_DAY="10",
        MAX_CLAP_ACTIONS_PER_DAY="20",
        MAX_FOLLOW_ACTIONS_PER_RUN="5",
        MIN_ACTION_GAP_SECONDS="45",
        MIN_READ_WAIT_SECONDS="45",
    )

    assert validate_production_profile(settings) == []


def test_validate_production_profile_reports_rule_violations() -> None:
    settings = AppSettings(
        APP_ENV="dev",
        CLIENT_MODE="fast",
        CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS="false",
        ENABLE_CHALLENGE_HALT="false",
        ENABLE_SESSION_EXPIRY_HALT="false",
        RISK_HALT_CONSECUTIVE_FAILURES="4",
        MAX_ACTIONS_PER_DAY="50",
        MAX_SUBSCRIBE_ACTIONS_PER_DAY="30",
        MAX_UNFOLLOW_ACTIONS_PER_DAY="20",
        MAX_CLAP_ACTIONS_PER_DAY="40",
        MIN_ACTION_GAP_SECONDS="30",
        MIN_READ_WAIT_SECONDS="30",
    )

    issues = validate_production_profile(settings)
    issue_keys = {item.key for item in issues}

    assert "APP_ENV" in issue_keys
    assert "CLIENT_MODE" in issue_keys
    assert "CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS" in issue_keys
    assert "ENABLE_CHALLENGE_HALT" in issue_keys
    assert "ENABLE_SESSION_EXPIRY_HALT" in issue_keys
    assert "RISK_HALT_CONSECUTIVE_FAILURES" in issue_keys
    assert "MAX_ACTIONS_PER_DAY" in issue_keys
    assert "MAX_SUBSCRIBE_ACTIONS_PER_DAY" in issue_keys
    assert "MAX_UNFOLLOW_ACTIONS_PER_DAY" in issue_keys
    assert "MAX_CLAP_ACTIONS_PER_DAY" in issue_keys
    assert "MIN_ACTION_GAP_SECONDS" in issue_keys
    assert "MIN_READ_WAIT_SECONDS" in issue_keys
