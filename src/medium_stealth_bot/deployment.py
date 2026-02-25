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

    expect_equal("APP_ENV", settings.app_env.lower(), "prod")

    return issues
