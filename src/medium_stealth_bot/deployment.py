from __future__ import annotations

from dataclasses import dataclass

from medium_stealth_bot.settings import AppSettings


@dataclass(frozen=True)
class ProfileValidationIssue:
    key: str
    expected: str
    actual: str


def validate_production_profile(settings: AppSettings) -> list[ProfileValidationIssue]:
    issues: list[ProfileValidationIssue] = []

    def expect_equal(key: str, actual: object, expected: object) -> None:
        if actual != expected:
            issues.append(
                ProfileValidationIssue(
                    key=key,
                    expected=f"== {expected}",
                    actual=str(actual),
                )
            )

    def expect_true(key: str, value: bool) -> None:
        if not value:
            issues.append(
                ProfileValidationIssue(
                    key=key,
                    expected="== true",
                    actual=str(value).lower(),
                )
            )

    def expect_max(key: str, value: int, maximum: int) -> None:
        if value > maximum:
            issues.append(
                ProfileValidationIssue(
                    key=key,
                    expected=f"<= {maximum}",
                    actual=str(value),
                )
            )

    def expect_min(key: str, value: int, minimum: int) -> None:
        if value < minimum:
            issues.append(
                ProfileValidationIssue(
                    key=key,
                    expected=f">= {minimum}",
                    actual=str(value),
                )
            )

    expect_equal("APP_ENV", settings.app_env.lower(), "prod")
    expect_equal("CLIENT_MODE", settings.client_mode, "stealth")
    expect_equal("DAY_BOUNDARY_POLICY", settings.day_boundary_policy, "utc")

    expect_true("CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS", settings.contract_registry_validate_response_fields)
    expect_true("ENABLE_CHALLENGE_HALT", settings.enable_challenge_halt)
    expect_true("ENABLE_SESSION_EXPIRY_HALT", settings.enable_session_expiry_halt)

    expect_max("RISK_HALT_CONSECUTIVE_FAILURES", settings.risk_halt_consecutive_failures, 2)
    expect_max("MAX_ACTIONS_PER_DAY", settings.max_actions_per_day, 30)
    expect_max("MAX_SUBSCRIBE_ACTIONS_PER_DAY", settings.max_subscribe_actions_per_day, 15)
    expect_max("MAX_UNFOLLOW_ACTIONS_PER_DAY", settings.max_unfollow_actions_per_day, 10)
    expect_max("MAX_CLAP_ACTIONS_PER_DAY", settings.max_clap_actions_per_day, 20)
    expect_max("MAX_FOLLOW_ACTIONS_PER_RUN", settings.max_follow_actions_per_run, 5)

    expect_min("MIN_ACTION_GAP_SECONDS", settings.min_action_gap_seconds, 45)
    expect_min("MIN_READ_WAIT_SECONDS", settings.min_read_wait_seconds, 45)

    return issues
