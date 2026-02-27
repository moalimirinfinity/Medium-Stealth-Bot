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

    def expect_at_most(key: str, actual: int, maximum: int) -> None:
        if actual > maximum:
            issues.append(
                ProfileValidationIssue(
                    key=key,
                    expected=f"<= {maximum}",
                    actual=str(actual),
                )
            )

    def expect_at_least(key: str, actual: int, minimum: int) -> None:
        if actual < minimum:
            issues.append(
                ProfileValidationIssue(
                    key=key,
                    expected=f">= {minimum}",
                    actual=str(actual),
                )
            )

    expect_equal("APP_ENV", settings.app_env.lower(), "prod")
    expect_equal("ENABLE_PACING_AUTO_CLAMP", settings.enable_pacing_auto_clamp, True)
    expect_at_least("LIVE_SESSION_DURATION_MINUTES", settings.live_session_duration_minutes, 120)
    expect_at_most("LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS", settings.live_session_target_follow_attempts, 120)
    expect_at_most("LIVE_SESSION_MIN_FOLLOW_ATTEMPTS", settings.live_session_min_follow_attempts, 120)
    expect_at_most("MAX_FOLLOW_ACTIONS_PER_RUN", settings.max_follow_actions_per_run, 30)
    expect_at_most("MAX_MUTATIONS_PER_10_MINUTES", settings.max_mutations_per_10_minutes, 24)
    expect_at_least("MIN_ACTION_GAP_SECONDS", settings.min_action_gap_seconds, 20)
    expect_at_least("MIN_VERIFY_GAP_SECONDS", settings.min_verify_gap_seconds, 2)
    expect_at_least("PASS_COOLDOWN_MIN_SECONDS", settings.pass_cooldown_min_seconds, 15)
    if settings.live_session_min_follow_attempts > settings.live_session_target_follow_attempts:
        issues.append(
            ProfileValidationIssue(
                key="LIVE_SESSION_MIN_FOLLOW_ATTEMPTS",
                expected="<= LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS",
                actual=str(settings.live_session_min_follow_attempts),
            )
        )

    return issues
