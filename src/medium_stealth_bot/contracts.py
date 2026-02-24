from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from pathlib import Path

from medium_stealth_bot import operations
from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.contract_registry import load_operation_contract_registry
from medium_stealth_bot.models import GraphQLOperation
from medium_stealth_bot.settings import AppSettings

FALLBACK_USER_ID = "cf6627889e92"
FALLBACK_NEWSLETTER_SLUG = "example-newsletter"
FALLBACK_NEWSLETTER_V3_ID = "example-newsletter-v3-id"
FALLBACK_TARGET_USER_ID = "example-target-user-id"
FALLBACK_POST_ID = "example-post-id"


@dataclass
class OperationContractCheck:
    operation_name: str
    ok: bool
    issues: list[str] = field(default_factory=list)


@dataclass
class ContractValidationReport:
    registry_path: Path
    strict: bool
    execute_reads: bool
    registry_operation_names: list[str]
    implemented_operation_names: list[str]
    missing_in_code: list[str]
    extra_in_code: list[str]
    checks: list[OperationContractCheck]
    live_read_checks: list["LiveReadCheck"] = field(default_factory=list)
    load_error: str | None = None

    @property
    def passed_count(self) -> int:
        return sum(1 for item in self.checks if item.ok)

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.checks if not item.ok)

    @property
    def ok(self) -> bool:
        return (
            self.load_error is None
            and not self.missing_in_code
            and not self.extra_in_code
            and self.failed_count == 0
            and self.live_failed_count == 0
        )

    @property
    def live_passed_count(self) -> int:
        return sum(1 for item in self.live_read_checks if item.status == "passed")

    @property
    def live_failed_count(self) -> int:
        return sum(1 for item in self.live_read_checks if item.status == "failed")

    @property
    def live_skipped_count(self) -> int:
        return sum(1 for item in self.live_read_checks if item.status == "skipped")

    @property
    def live_executed_count(self) -> int:
        return self.live_passed_count + self.live_failed_count


@dataclass
class LiveReadCheck:
    operation_name: str
    status: str
    detail: str = ""


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _operations_file_path() -> Path:
    return Path(__file__).resolve().with_name("operations.py")


def _implemented_operation_names() -> list[str]:
    source = _operations_file_path().read_text(encoding="utf-8")
    names = sorted(set(re.findall(r'operationName="([^"]+)"', source)))
    return names


def _sample_operation_builders(tag_slug: str, actor_user_id: str | None) -> dict[str, GraphQLOperation]:
    user_id = actor_user_id or FALLBACK_USER_ID
    return {
        "UseBaseCacheControlQuery": operations.use_base_cache_control(),
        "TopicLatestStorieQuery": operations.topic_latest_stories(tag_slug),
        "TopicWhoToFollowPubishersQuery": operations.topic_who_to_follow_publishers(tag_slug=tag_slug),
        "WhoToFollowModuleQuery": operations.who_to_follow_module(),
        "UserFollowers": operations.user_followers(user_id=user_id, limit=8),
        "UserViewerEdge": operations.user_viewer_edge(user_id),
        "NewsletterV3ViewerEdge": operations.newsletter_v3_viewer_edge(FALLBACK_NEWSLETTER_SLUG),
        "UserLatestPostQuery": operations.user_latest_post(user_id=user_id),
        "SubscribeNewsletterV3Mutation": operations.subscribe_newsletter_v3(FALLBACK_NEWSLETTER_V3_ID),
        "UnsubscribeNewsletterV3Mutation": operations.unsubscribe_newsletter_v3(FALLBACK_NEWSLETTER_V3_ID),
        "UnfollowUserMutation": operations.unfollow_user(FALLBACK_TARGET_USER_ID),
        "ClapMutation": operations.clap_post(FALLBACK_POST_ID, user_id, num_claps=1),
        "PublishPostThreadedResponse": operations.publish_threaded_response(FALLBACK_POST_ID, "hello"),
    }


def _live_read_operation_builders(
    *,
    tag_slug: str,
    actor_user_id: str | None,
    newsletter_slug: str | None,
) -> tuple[dict[str, GraphQLOperation], dict[str, str]]:
    builders: dict[str, GraphQLOperation] = {
        "UseBaseCacheControlQuery": operations.use_base_cache_control(),
        "TopicLatestStorieQuery": operations.topic_latest_stories(tag_slug),
        "TopicWhoToFollowPubishersQuery": operations.topic_who_to_follow_publishers(tag_slug=tag_slug),
        "WhoToFollowModuleQuery": operations.who_to_follow_module(),
    }
    skipped: dict[str, str] = {}

    if actor_user_id:
        builders["UserFollowers"] = operations.user_followers(user_id=actor_user_id, limit=8)
        builders["UserViewerEdge"] = operations.user_viewer_edge(actor_user_id)
        builders["UserLatestPostQuery"] = operations.user_latest_post(user_id=actor_user_id)
    else:
        skipped["UserFollowers"] = "missing_actor_user_id"
        skipped["UserViewerEdge"] = "missing_actor_user_id"
        skipped["UserLatestPostQuery"] = "missing_actor_user_id"

    if newsletter_slug:
        builders["NewsletterV3ViewerEdge"] = operations.newsletter_v3_viewer_edge(newsletter_slug)
    else:
        skipped["NewsletterV3ViewerEdge"] = "missing_newsletter_slug"

    return builders, skipped


async def _run_live_read_checks(
    *,
    settings: AppSettings,
    registry_operation_names: list[str],
    read_operation_names: list[str],
    tag_slug: str,
    actor_user_id: str | None,
    newsletter_slug: str | None,
) -> list[LiveReadCheck]:
    if not settings.has_session:
        return [
            LiveReadCheck(
                operation_name="__session__",
                status="failed",
                detail="MEDIUM_SESSION missing; cannot execute --execute-reads checks.",
            )
        ]

    builders, skipped = _live_read_operation_builders(
        tag_slug=tag_slug,
        actor_user_id=actor_user_id,
        newsletter_slug=newsletter_slug,
    )

    checks: list[LiveReadCheck] = []
    async with MediumAsyncClient(settings) as client:
        for operation_name in registry_operation_names:
            if operation_name not in read_operation_names:
                continue
            if operation_name in skipped:
                checks.append(
                    LiveReadCheck(
                        operation_name=operation_name,
                        status="skipped",
                        detail=skipped[operation_name],
                    )
                )
                continue
            sample = builders.get(operation_name)
            if sample is None:
                checks.append(
                    LiveReadCheck(
                        operation_name=operation_name,
                        status="skipped",
                        detail="no_live_builder",
                    )
                )
                continue
            try:
                result = await client.execute(sample)
            except Exception as exc:  # noqa: BLE001
                checks.append(
                    LiveReadCheck(
                        operation_name=operation_name,
                        status="failed",
                        detail=f"exception:{exc}",
                    )
                )
                continue

            if result.status_code == 200 and not result.has_errors:
                checks.append(
                    LiveReadCheck(
                        operation_name=operation_name,
                        status="passed",
                        detail="ok",
                    )
                )
                continue

            checks.append(
                LiveReadCheck(
                    operation_name=operation_name,
                    status="failed",
                    detail=f"status={result.status_code} errors={len(result.errors)}",
                )
            )

    return checks


def validate_contract_registry(
    *,
    registry_path: Path,
    strict: bool,
    tag_slug: str = "programming",
    actor_user_id: str | None = None,
    execute_reads: bool = False,
    settings: AppSettings | None = None,
    live_newsletter_slug: str | None = None,
) -> ContractValidationReport:
    registry_resolved = registry_path
    if not registry_resolved.is_absolute():
        registry_resolved = (_project_root() / registry_resolved).resolve()

    implemented = _implemented_operation_names()
    try:
        registry = load_operation_contract_registry(path=registry_resolved, strict=strict)
    except Exception as exc:  # noqa: BLE001
        return ContractValidationReport(
            registry_path=registry_resolved,
            strict=strict,
            execute_reads=execute_reads,
            registry_operation_names=[],
            implemented_operation_names=implemented,
            missing_in_code=[],
            extra_in_code=[],
            checks=[],
            load_error=str(exc),
        )

    registry_names = sorted(registry.registry.operation_map().keys())
    missing_in_code = sorted(set(registry_names) - set(implemented))
    extra_in_code = sorted(set(implemented) - set(registry_names))

    samples = _sample_operation_builders(tag_slug=tag_slug, actor_user_id=actor_user_id)
    checks: list[OperationContractCheck] = []
    for operation_name in registry_names:
        sample = samples.get(operation_name)
        if sample is None:
            checks.append(
                OperationContractCheck(
                    operation_name=operation_name,
                    ok=False,
                    issues=["missing_sample_operation_builder"],
                )
            )
            continue
        issues = registry.validate_request(sample)
        checks.append(
            OperationContractCheck(
                operation_name=operation_name,
                ok=not issues,
                issues=issues,
            )
        )

    live_checks: list[LiveReadCheck] = []
    if execute_reads:
        if settings is None:
            live_checks.append(
                LiveReadCheck(
                    operation_name="__settings__",
                    status="failed",
                    detail="settings_required_for_execute_reads",
                )
            )
        else:
            registry_map = registry.registry.operation_map()
            read_operation_names = [
                name
                for name in registry_names
                if "read" in registry_map[name].classification
            ]
            live_checks = asyncio.run(
                _run_live_read_checks(
                    settings=settings,
                    registry_operation_names=registry_names,
                    read_operation_names=read_operation_names,
                    tag_slug=tag_slug,
                    actor_user_id=actor_user_id,
                    newsletter_slug=live_newsletter_slug,
                )
            )

    return ContractValidationReport(
        registry_path=registry_resolved,
        strict=strict,
        execute_reads=execute_reads,
        registry_operation_names=registry_names,
        implemented_operation_names=implemented,
        missing_in_code=missing_in_code,
        extra_in_code=extra_in_code,
        checks=checks,
        live_read_checks=live_checks,
    )
