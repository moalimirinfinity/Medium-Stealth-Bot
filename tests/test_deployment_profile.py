from medium_stealth_bot.deployment import validate_production_profile
from medium_stealth_bot.settings import AppSettings


def test_validate_production_profile_passes_for_safe_defaults() -> None:
    settings = AppSettings(
        APP_ENV="prod",
        CLIENT_MODE="fast",
        CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS="false",
        ENABLE_CHALLENGE_HALT="false",
        ENABLE_SESSION_EXPIRY_HALT="false",
        RISK_HALT_CONSECUTIVE_FAILURES="12",
        MAX_ACTIONS_PER_DAY="1200",
        MAX_SUBSCRIBE_ACTIONS_PER_DAY="900",
        MAX_UNFOLLOW_ACTIONS_PER_DAY="800",
        MAX_CLAP_ACTIONS_PER_DAY="1500",
        MAX_FOLLOW_ACTIONS_PER_RUN="250",
        MIN_ACTION_GAP_SECONDS="5",
        MIN_READ_WAIT_SECONDS="5",
    )

    assert validate_production_profile(settings) == []


def test_validate_production_profile_reports_rule_violations() -> None:
    settings = AppSettings(
        APP_ENV="dev",
    )

    issues = validate_production_profile(settings)
    issue_keys = {item.key for item in issues}

    assert issue_keys == {"APP_ENV"}
