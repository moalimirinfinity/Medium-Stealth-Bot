import asyncio
import random
import re
import time
from datetime import datetime, timezone

import structlog

from medium_stealth_bot import operations
from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.models import (
    CandidateDecision,
    CandidateSource,
    CandidateUser,
    DailyRunOutcome,
    GraphQLError,
    GraphQLResult,
    NewsletterState,
    ProbeSnapshot,
    RelationshipConfidence,
    UserFollowState,
)
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.settings import AppSettings


class DailyRunner:
    def __init__(self, settings: AppSettings, client: MediumAsyncClient, repository: ActionRepository):
        self.settings = settings
        self.client = client
        self.repository = repository
        self.log = structlog.get_logger(__name__)

    async def probe(self, tag_slug: str = "programming") -> ProbeSnapshot:
        started = datetime.now(timezone.utc)
        start_time = time.perf_counter()

        task_map: dict[str, asyncio.Task[GraphQLResult]] = {
            "base_cache": asyncio.create_task(self._execute_safe("base_cache", operations.use_base_cache_control())),
            "topic_latest_stories": asyncio.create_task(
                self._execute_safe("topic_latest_stories", operations.topic_latest_stories(tag_slug))
            ),
            "topic_who_to_follow": asyncio.create_task(
                self._execute_safe(
                    "topic_who_to_follow",
                    operations.topic_who_to_follow_publishers(tag_slug=tag_slug, first=5),
                )
            ),
            "who_to_follow_module": asyncio.create_task(
                self._execute_safe("who_to_follow_module", operations.who_to_follow_module())
            ),
        }

        # MEDIUM_USER_REF contract is user_id-only, so this check is safe when set.
        if self.settings.medium_user_ref:
            task_map["user_viewer_edge"] = asyncio.create_task(
                self._execute_safe("user_viewer_edge", operations.user_viewer_edge(self.settings.medium_user_ref))
            )

        resolved = await asyncio.gather(*task_map.values())
        results = dict(zip(task_map.keys(), resolved, strict=True))

        duration_ms = int((time.perf_counter() - start_time) * 1000)
        self.log.info("probe_complete", tag_slug=tag_slug, duration_ms=duration_ms, task_count=len(task_map))
        return ProbeSnapshot(
            tag_slug=tag_slug,
            started_at=started,
            duration_ms=duration_ms,
            results=results,
        )

    async def run_daily_cycle(
        self,
        *,
        tag_slug: str = "programming",
        dry_run: bool = True,
        seed_user_refs: list[str] | None = None,
    ) -> DailyRunOutcome:
        actions_today_start = self.repository.actions_today_utc()
        max_actions = self.settings.max_actions_per_day
        if actions_today_start >= max_actions:
            self.log.info(
                "budget_exhausted",
                actions_today=actions_today_start,
                max_actions=max_actions,
                day_boundary_policy=self.settings.day_boundary_policy,
            )
            return DailyRunOutcome(
                budget_exhausted=True,
                actions_today=actions_today_start,
                max_actions_per_day=max_actions,
                dry_run=dry_run,
                probe=None,
            )

        probe = await self.probe(tag_slug=tag_slug)

        seed_refs = (seed_user_refs or []) + self.settings.discovery_seed_users
        candidates = await self._build_candidates(probe=probe, seed_user_refs=seed_refs)
        candidates = candidates[: self.settings.follow_candidate_limit]

        decisions: list[CandidateDecision] = []
        eligible = await self._evaluate_candidates(
            candidates,
            decisions=decisions,
            persist_observations=not dry_run,
        )

        remaining_budget = max(0, max_actions - actions_today_start)
        follow_slots = min(self.settings.max_follow_actions_per_run, remaining_budget, len(eligible))
        follow_attempted, follow_verified = await self._execute_follow_pipeline(
            eligible_candidates=eligible,
            max_to_run=follow_slots,
            dry_run=dry_run,
            decisions=decisions,
        )
        remaining_budget = max(0, remaining_budget - follow_attempted)

        cleanup_cap = min(self.settings.cleanup_unfollow_limit, remaining_budget)
        cleanup_attempted, cleanup_verified = await self._execute_cleanup_pipeline(
            dry_run=dry_run,
            max_to_run=cleanup_cap,
            decisions=decisions,
        )

        actions_today_end = self.repository.actions_today_utc() if not dry_run else actions_today_start
        self.log.info(
            "daily_cycle_complete",
            dry_run=dry_run,
            considered_candidates=len(candidates),
            eligible_candidates=len(eligible),
            follow_attempted=follow_attempted,
            follow_verified=follow_verified,
            cleanup_attempted=cleanup_attempted,
            cleanup_verified=cleanup_verified,
            actions_today=actions_today_end,
            max_actions=max_actions,
            day_boundary_policy=self.settings.day_boundary_policy,
        )
        return DailyRunOutcome(
            budget_exhausted=False,
            actions_today=actions_today_end,
            max_actions_per_day=max_actions,
            dry_run=dry_run,
            considered_candidates=len(candidates),
            eligible_candidates=len(eligible),
            follow_actions_attempted=follow_attempted,
            follow_actions_verified=follow_verified,
            cleanup_actions_attempted=cleanup_attempted,
            cleanup_actions_verified=cleanup_verified,
            decision_log=[
                f"{item.reason} (id={item.user_id})"
                for item in decisions[:80]
            ],
            probe=probe,
        )

    async def _execute_safe(self, task_name: str, operation) -> GraphQLResult:
        try:
            return await self.client.execute(operation)
        except Exception as exc:  # noqa: BLE001
            self.log.warning("graphql_operation_failed", task_name=task_name, error=str(exc))
            return GraphQLResult(
                operationName=operation.operation_name,
                statusCode=0,
                data=None,
                errors=[GraphQLError(message=str(exc))],
                raw={"exception": str(exc)},
            )

    async def _build_candidates(
        self,
        *,
        probe: ProbeSnapshot,
        seed_user_refs: list[str],
    ) -> list[CandidateUser]:
        pool: dict[str, CandidateUser] = {}

        self._extract_topic_latest_candidates(probe, pool)
        self._extract_topic_who_to_follow_candidates(probe, pool)
        await self._extract_seed_followers_candidates(seed_user_refs, pool)

        for candidate in pool.values():
            candidate.matched_keywords = self._match_keywords(candidate.bio)
            ratio = self._following_follower_ratio(candidate)
            keyword_bonus = 0.35 * len(candidate.matched_keywords)
            source_bonus = 0.2 * len(candidate.sources)
            newsletter_bonus = 0.2 if candidate.newsletter_v3_id else 0.0
            candidate.score = ratio + keyword_bonus + source_bonus + newsletter_bonus

        ordered = sorted(pool.values(), key=lambda item: item.score, reverse=True)
        self.log.info("candidates_built", count=len(ordered), seed_sources=len(seed_user_refs))
        return ordered

    def _extract_topic_latest_candidates(self, probe: ProbeSnapshot, pool: dict[str, CandidateUser]) -> None:
        result = probe.results.get("topic_latest_stories")
        if not result or not result.data:
            return
        edges = (
            result.data.get("tagFromSlug", {})
            .get("posts", {})
            .get("edges", [])
        )
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node", {})
            if not isinstance(node, dict):
                continue
            creator = node.get("creator")
            if not isinstance(creator, dict):
                continue
            candidate = self._candidate_from_user_node(
                creator,
                source=CandidateSource.TOPIC_LATEST_STORIES,
                latest_post_id=node.get("id") if isinstance(node.get("id"), str) else None,
            )
            if candidate:
                self._merge_candidate(pool, candidate)

    def _extract_topic_who_to_follow_candidates(self, probe: ProbeSnapshot, pool: dict[str, CandidateUser]) -> None:
        result = probe.results.get("topic_who_to_follow")
        if not result or not result.data:
            return
        edges = result.data.get("recommendedPublishers", {}).get("edges", [])
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if not isinstance(node, dict) or node.get("__typename") != "User":
                continue
            candidate = self._candidate_from_user_node(node, source=CandidateSource.TOPIC_WHO_TO_FOLLOW)
            if candidate:
                self._merge_candidate(pool, candidate)

    async def _extract_seed_followers_candidates(
        self,
        seed_user_refs: list[str],
        pool: dict[str, CandidateUser],
    ) -> None:
        for seed_ref in seed_user_refs:
            user_id, username = self._parse_user_ref(seed_ref)
            if not user_id and not username:
                continue
            result = await self._execute_safe(
                "seed_user_followers",
                operations.user_followers(
                    user_id=user_id,
                    username=username,
                    limit=self.settings.discovery_seed_followers_limit,
                ),
            )
            first_hop_users = self._extract_users_from_followers_result(result)
            for node in first_hop_users:
                candidate = self._candidate_from_user_node(node, source=CandidateSource.SEED_FOLLOWERS)
                if candidate:
                    self._merge_candidate(pool, candidate)

            if self.settings.discovery_followers_depth < 2:
                continue

            second_hop_roots = [
                item.get("id")
                for item in first_hop_users
                if isinstance(item, dict) and isinstance(item.get("id"), str)
            ][: self.settings.discovery_second_hop_seed_limit]
            for root_id in second_hop_roots:
                hop_result = await self._execute_safe(
                    "seed_user_followers_second_hop",
                    operations.user_followers(
                        user_id=root_id,
                        limit=self.settings.discovery_seed_followers_limit,
                    ),
                )
                for node in self._extract_users_from_followers_result(hop_result):
                    candidate = self._candidate_from_user_node(node, source=CandidateSource.SEED_FOLLOWERS)
                    if candidate:
                        self._merge_candidate(pool, candidate)

    @staticmethod
    def _parse_user_ref(ref: str) -> tuple[str | None, str | None]:
        normalized = ref.strip()
        if not normalized:
            return None, None
        if normalized.startswith("id:"):
            value = normalized[3:].strip()
            return (value or None), None
        if normalized.startswith("username:"):
            value = normalized[9:].strip().lstrip("@")
            return None, (value or None)
        if normalized.startswith("@"):
            return None, normalized[1:]
        if re.fullmatch(r"[0-9a-f]{8,24}", normalized.lower()):
            return normalized, None
        return None, normalized

    def _candidate_from_user_node(
        self,
        node: dict,
        *,
        source: CandidateSource,
        latest_post_id: str | None = None,
    ) -> CandidateUser | None:
        user_id = node.get("id")
        if not isinstance(user_id, str) or not user_id:
            return None
        newsletter_v3 = node.get("newsletterV3")
        social_stats = node.get("socialStats")
        follower_count = None
        following_count = None
        if isinstance(social_stats, dict):
            follower_count = self._to_int(social_stats.get("followerCount"))
            following_count = self._to_int(social_stats.get("followingCount"))
        return CandidateUser(
            user_id=user_id,
            username=node.get("username") if isinstance(node.get("username"), str) else None,
            name=node.get("name") if isinstance(node.get("name"), str) else None,
            bio=node.get("bio") if isinstance(node.get("bio"), str) else None,
            newsletter_v3_id=newsletter_v3.get("id") if isinstance(newsletter_v3, dict) else None,
            follower_count=follower_count,
            following_count=following_count,
            latest_post_id=latest_post_id,
            sources=[source],
        )

    @staticmethod
    def _to_int(value) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    @staticmethod
    def _merge_candidate(pool: dict[str, CandidateUser], candidate: CandidateUser) -> None:
        existing = pool.get(candidate.user_id)
        if existing is None:
            pool[candidate.user_id] = candidate
            return

        if not existing.username and candidate.username:
            existing.username = candidate.username
        if not existing.name and candidate.name:
            existing.name = candidate.name
        if not existing.bio and candidate.bio:
            existing.bio = candidate.bio
        if not existing.newsletter_v3_id and candidate.newsletter_v3_id:
            existing.newsletter_v3_id = candidate.newsletter_v3_id
        if existing.follower_count is None and candidate.follower_count is not None:
            existing.follower_count = candidate.follower_count
        if existing.following_count is None and candidate.following_count is not None:
            existing.following_count = candidate.following_count
        if not existing.latest_post_id and candidate.latest_post_id:
            existing.latest_post_id = candidate.latest_post_id
        for source in candidate.sources:
            if source not in existing.sources:
                existing.sources.append(source)

    async def _evaluate_candidates(
        self,
        candidates: list[CandidateUser],
        *,
        decisions: list[CandidateDecision],
        persist_observations: bool,
    ) -> list[CandidateUser]:
        eligible: list[CandidateUser] = []
        for candidate in candidates:
            ratio = self._following_follower_ratio(candidate)
            if ratio < self.settings.min_following_follower_ratio:
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=False,
                        reason=f"skip:ratio_below_threshold ratio={ratio:.2f}",
                        score=candidate.score,
                    )
                )
                continue

            if self.settings.require_bio_keyword_match and not candidate.matched_keywords:
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=False,
                        reason="skip:no_keyword_match",
                        score=candidate.score,
                    )
                )
                continue

            if not candidate.newsletter_v3_id:
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=False,
                        reason="skip:no_newsletter_v3_id",
                        score=candidate.score,
                    )
                )
                continue

            if self.repository.is_blacklisted(candidate.user_id):
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=False,
                        reason="skip:blacklisted",
                        score=candidate.score,
                    )
                )
                continue

            if self.repository.has_recent_action(
                candidate.user_id,
                within_hours=self.settings.follow_cooldown_hours,
                action_types=("follow_subscribe_attempt", "follow_verified", "cleanup_unfollow"),
            ):
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=False,
                        reason="skip:cooldown_active",
                        score=candidate.score,
                    )
                )
                continue

            local_state = self.repository.get_relationship_state(candidate.user_id)
            if local_state and local_state.user_follow_state == UserFollowState.FOLLOWING:
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=False,
                        reason="skip:already_following_local_state",
                        score=candidate.score,
                    )
                )
                continue

            edge_result = await self._execute_safe("candidate_user_viewer_edge", operations.user_viewer_edge(candidate.user_id))
            is_following = self._extract_is_following(edge_result)
            if is_following is True:
                if persist_observations:
                    self.repository.upsert_relationship_state(
                        candidate.user_id,
                        newsletter_state=NewsletterState.UNKNOWN,
                        user_follow_state=UserFollowState.FOLLOWING,
                        confidence=RelationshipConfidence.OBSERVED,
                        source_operation="UserViewerEdge",
                        verified_now=True,
                    )
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=False,
                        reason="skip:already_following_live_check",
                        score=candidate.score,
                    )
                )
                continue
            if is_following is None:
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=False,
                        reason="skip:live_check_unavailable",
                        score=candidate.score,
                    )
                )
                continue

            eligible.append(candidate)
            decisions.append(
                CandidateDecision(
                    user_id=candidate.user_id,
                    username=candidate.username,
                    eligible=True,
                    reason="eligible",
                    score=candidate.score,
                )
            )

        return eligible

    async def _execute_follow_pipeline(
        self,
        *,
        eligible_candidates: list[CandidateUser],
        max_to_run: int,
        dry_run: bool,
        decisions: list[CandidateDecision],
    ) -> tuple[int, int]:
        attempted = 0
        verified = 0
        for candidate in eligible_candidates[:max_to_run]:
            attempted += 1

            if dry_run:
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=True,
                        reason="dry_run:planned_follow",
                        score=candidate.score,
                    )
                )
                continue

            self.repository.upsert_user_profile(
                candidate.user_id,
                username=candidate.username,
                newsletter_id=candidate.newsletter_v3_id,
                bio=candidate.bio,
            )

            await self._maybe_pre_follow_clap(candidate)

            mutation = await self._execute_safe(
                "follow_subscribe_mutation",
                operations.subscribe_newsletter_v3(candidate.newsletter_v3_id),
            )
            mutation_ok = mutation.status_code == 200 and not mutation.has_errors
            self.repository.record_action(
                "follow_subscribe_attempt",
                candidate.user_id,
                "ok" if mutation_ok else "failed",
            )
            if not mutation_ok:
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=True,
                        reason="follow_failed:mutation_error",
                        score=candidate.score,
                    )
                )
                continue

            verify = await self._execute_safe("follow_verify_user_viewer_edge", operations.user_viewer_edge(candidate.user_id))
            is_following = self._extract_is_following(verify)
            if is_following is True:
                verified += 1
                self.repository.upsert_relationship_state(
                    candidate.user_id,
                    newsletter_state=NewsletterState.SUBSCRIBED,
                    user_follow_state=UserFollowState.FOLLOWING,
                    confidence=RelationshipConfidence.OBSERVED,
                    source_operation="SubscribeNewsletterV3Mutation",
                    verified_now=True,
                )
                self.repository.mark_follow_cycle_started(
                    user_id=candidate.user_id,
                    username=candidate.username,
                    source="SubscribeNewsletterV3Mutation",
                    grace_days=self.settings.unfollow_nonreciprocal_after_days,
                )
                self.repository.record_action("follow_verified", candidate.user_id, "verified_following")
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=True,
                        reason="follow_success:verified_following",
                        score=candidate.score,
                    )
                )
            else:
                self.repository.upsert_relationship_state(
                    candidate.user_id,
                    newsletter_state=NewsletterState.SUBSCRIBED,
                    user_follow_state=UserFollowState.NOT_FOLLOWING if is_following is False else UserFollowState.UNKNOWN,
                    confidence=RelationshipConfidence.OBSERVED,
                    source_operation="UserViewerEdge",
                    verified_now=is_following is not None,
                )
                self.repository.record_action("follow_verified", candidate.user_id, "verification_failed")
                decisions.append(
                    CandidateDecision(
                        user_id=candidate.user_id,
                        username=candidate.username,
                        eligible=True,
                        reason="follow_failed:verification_failed",
                        score=candidate.score,
                    )
                )

            if attempted < max_to_run:
                await self._sleep_action_gap()

        return attempted, verified

    async def _maybe_pre_follow_clap(self, candidate: CandidateUser) -> None:
        if not self.settings.enable_pre_follow_clap:
            return

        post_id = candidate.latest_post_id
        if not post_id:
            latest_post = await self._execute_safe(
                "pre_follow_latest_post",
                operations.user_latest_post(user_id=candidate.user_id, username=candidate.username),
            )
            post_id = self._extract_latest_post_id(latest_post)
            if post_id:
                candidate.latest_post_id = post_id

        if not post_id:
            self.repository.record_action("clap_pre_follow", candidate.user_id, "skipped_no_post")
            return

        if self.settings.pre_follow_read_wait_seconds > 0:
            await self._sleep_read_delay()

        clap_count = random.randint(self.settings.min_clap_count, self.settings.max_clap_count)
        clap_result = await self._execute_safe(
            "clap_pre_follow",
            operations.clap_post(post_id, candidate.user_id, num_claps=clap_count),
        )
        clap_ok = clap_result.status_code == 200 and not clap_result.has_errors
        self.repository.record_action(
            "clap_pre_follow",
            candidate.user_id,
            f"{'ok' if clap_ok else 'failed'}:{clap_count}",
        )

    async def _execute_cleanup_pipeline(
        self,
        *,
        dry_run: bool,
        max_to_run: int,
        decisions: list[CandidateDecision],
    ) -> tuple[int, int]:
        if max_to_run <= 0:
            return 0, 0

        due = self.repository.pending_nonreciprocal_candidates(
            grace_days=self.settings.unfollow_nonreciprocal_after_days,
            limit=max_to_run,
        )
        if not due:
            return 0, 0
        if not self.settings.medium_user_ref:
            self.log.warning("cleanup_skipped_missing_medium_user_ref")
            return 0, 0

        follower_ids = await self._fetch_own_follower_ids(self.settings.own_followers_scan_limit)
        attempted = 0
        verified = 0

        for row in due:
            user_id = row["user_id"]
            username = row.get("username")
            if user_id in follower_ids:
                if not dry_run:
                    self.repository.mark_followed_back(user_id)
                    self.repository.record_action("cleanup_followed_back", user_id, "kept")
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:kept_followed_back",
                    )
                )
                continue

            attempted += 1
            if dry_run:
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:dry_run_unfollow_nonreciprocal",
                    )
                )
                continue

            mutation = await self._execute_safe("cleanup_unfollow", operations.unfollow_user(user_id))
            mutation_ok = mutation.status_code == 200 and not mutation.has_errors
            if not mutation_ok:
                self.repository.record_action("cleanup_unfollow", user_id, "mutation_failed")
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:unfollow_mutation_failed",
                    )
                )
                continue

            verify = await self._execute_safe("cleanup_verify", operations.user_viewer_edge(user_id))
            is_following = self._extract_is_following(verify)
            if is_following is False:
                verified += 1
                self.repository.mark_nonreciprocal_unfollowed(user_id)
                self.repository.record_action("cleanup_unfollow", user_id, "verified_not_following")
                self.repository.upsert_relationship_state(
                    user_id,
                    newsletter_state=NewsletterState.UNKNOWN,
                    user_follow_state=UserFollowState.NOT_FOLLOWING,
                    confidence=RelationshipConfidence.OBSERVED,
                    source_operation="UnfollowUserMutation",
                    verified_now=True,
                )
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:unfollow_verified",
                    )
                )
            else:
                self.repository.mark_cleanup_checked(user_id)
                self.repository.record_action("cleanup_unfollow", user_id, "verification_uncertain")
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:verification_uncertain",
                    )
                )

            if attempted < max_to_run:
                await self._sleep_action_gap()

        return attempted, verified

    async def _fetch_own_follower_ids(self, limit: int) -> set[str]:
        if not self.settings.medium_user_ref:
            return set()
        result = await self._execute_safe(
            "own_followers_scan",
            operations.user_followers(user_id=self.settings.medium_user_ref, limit=limit),
        )
        users = self._extract_users_from_followers_result(result)
        return {item["id"] for item in users if isinstance(item, dict) and isinstance(item.get("id"), str)}

    @staticmethod
    def _extract_users_from_followers_result(result: GraphQLResult) -> list[dict]:
        if not result.data:
            return []
        user_result = result.data.get("userResult", {})
        if not isinstance(user_result, dict):
            return []
        conn = user_result.get("followersUserConnection", {})
        if not isinstance(conn, dict):
            return []
        users = conn.get("users", [])
        return users if isinstance(users, list) else []

    @staticmethod
    def _extract_latest_post_id(result: GraphQLResult) -> str | None:
        if not result.data:
            return None
        user_result = result.data.get("userResult", {})
        if not isinstance(user_result, dict):
            return None
        homepage = user_result.get("homepagePostsConnection", {})
        if not isinstance(homepage, dict):
            return None
        posts = homepage.get("posts", [])
        if not isinstance(posts, list) or not posts:
            return None
        first = posts[0]
        if not isinstance(first, dict):
            return None
        post_id = first.get("id")
        return post_id if isinstance(post_id, str) else None

    @staticmethod
    def _extract_is_following(result: GraphQLResult) -> bool | None:
        if not result.data:
            return None
        user = result.data.get("user")
        if not isinstance(user, dict):
            return None
        edge = user.get("viewerEdge")
        if not isinstance(edge, dict):
            return None
        value = edge.get("isFollowing")
        if isinstance(value, bool):
            return value
        return None

    def _following_follower_ratio(self, candidate: CandidateUser) -> float:
        followers = candidate.follower_count or 0
        following = candidate.following_count or 0
        if followers <= 0:
            return 1.0 if following > 0 else 0.0
        return following / followers

    def _match_keywords(self, bio: str | None) -> list[str]:
        if not bio:
            return []
        normalized = bio.lower()
        return [keyword for keyword in self.settings.bio_keywords if keyword in normalized]

    async def _sleep_action_gap(self) -> None:
        if self.settings.max_action_gap_seconds <= 0:
            return
        low = float(self.settings.min_action_gap_seconds)
        high = float(self.settings.max_action_gap_seconds)
        if high <= low:
            await asyncio.sleep(low)
            return
        # Clamp a gaussian sample so action intervals are non-uniform but bounded.
        mean = (low + high) / 2.0
        stddev = max((high - low) / 6.0, 0.1)
        sampled = random.gauss(mean, stddev)
        await asyncio.sleep(max(low, min(high, sampled)))

    async def _sleep_read_delay(self) -> None:
        base = float(self.settings.pre_follow_read_wait_seconds)
        low = max(0.0, min(float(self.settings.min_read_wait_seconds), base))
        high = max(float(self.settings.max_read_wait_seconds), base)
        if high <= low:
            await asyncio.sleep(base)
            return
        sampled = random.uniform(low, high)
        await asyncio.sleep(max(low, min(high, sampled)))
