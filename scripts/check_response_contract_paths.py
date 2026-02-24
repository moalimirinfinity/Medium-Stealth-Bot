#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

from medium_stealth_bot import operations
from medium_stealth_bot.contract_registry import load_operation_contract_registry

FALLBACK_USER_ID = "cf6627889e92"
FALLBACK_NEWSLETTER_SLUG = "example-newsletter"
FALLBACK_NEWSLETTER_V3_ID = "example-newsletter-v3-id"
FALLBACK_TARGET_USER_ID = "example-target-user-id"
FALLBACK_POST_ID = "example-post-id"


def sample_operation_builders() -> dict[str, str]:
    samples = {
        "UseBaseCacheControlQuery": operations.use_base_cache_control(),
        "TopicLatestStorieQuery": operations.topic_latest_stories("programming"),
        "TopicWhoToFollowPubishersQuery": operations.topic_who_to_follow_publishers(tag_slug="programming"),
        "WhoToFollowModuleQuery": operations.who_to_follow_module(),
        "UserFollowers": operations.user_followers(user_id=FALLBACK_USER_ID, limit=8),
        "UserViewerEdge": operations.user_viewer_edge(FALLBACK_USER_ID),
        "NewsletterV3ViewerEdge": operations.newsletter_v3_viewer_edge(FALLBACK_NEWSLETTER_SLUG),
        "UserLatestPostQuery": operations.user_latest_post(user_id=FALLBACK_USER_ID),
        "SubscribeNewsletterV3Mutation": operations.subscribe_newsletter_v3(FALLBACK_NEWSLETTER_V3_ID),
        "UnsubscribeNewsletterV3Mutation": operations.unsubscribe_newsletter_v3(FALLBACK_NEWSLETTER_V3_ID),
        "UnfollowUserMutation": operations.unfollow_user(FALLBACK_TARGET_USER_ID),
        "ClapMutation": operations.clap_post(FALLBACK_POST_ID, FALLBACK_USER_ID, num_claps=1),
        "PublishPostThreadedResponse": operations.publish_threaded_response(FALLBACK_POST_ID, "hello"),
    }
    return {name: operation.query for name, operation in samples.items()}


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    registry_path = project_root / "captures" / "final" / "implementation_ops_2026-02-24.json"
    registry = load_operation_contract_registry(path=registry_path, strict=True)
    queries = sample_operation_builders()

    errors: list[str] = []
    for contract in registry.registry.core_operations:
        operation_name = contract.operation_name
        query = queries.get(operation_name)
        if not query:
            errors.append(f"missing_query_builder:{operation_name}")
            continue

        for path in contract.expected_top_level_response_fields:
            for token in path.split("."):
                if token and token not in query:
                    errors.append(
                        f"response_field_path_not_present_in_query:{operation_name}:path={path}:token={token}"
                    )
                    break

    if errors:
        print("Response contract path check failed:")
        for item in errors:
            print(f"- {item}")
        return 1

    print("Response contract path check passed")
    print(f"- registry: {registry_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
