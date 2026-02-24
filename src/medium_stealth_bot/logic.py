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
from medium_stealth_bot.safety import RiskGuard
from medium_stealth_bot.settings import AppSettings
from medium_stealth_bot.timing import HumanTimingController

ACTION_SUBSCRIBE = "follow_subscribe_attempt"
ACTION_UNFOLLOW = "cleanup_unfollow"
ACTION_CLAP = "clap_pre_follow"
ACTION_CLAP_SKIPPED = "clap_pre_follow_skipped"
TRACKED_DAILY_ACTION_TYPES: tuple[str, ...] = (
    ACTION_SUBSCRIBE,
    ACTION_UNFOLLOW,
    ACTION_CLAP,
)


class DailyRunner:
    def __init__(self, settings: AppSettings, client: MediumAsyncClient, repository: ActionRepository):
        self.settings = settings
        self.client = client
        self.repository = repository
        self.log = structlog.get_logger(__name__)
        self.risk_guard = RiskGuard(settings=settings, log=self.log)
        self.timing = HumanTimingController(settings=settings)

    async def probe(self, tag_slug: str = "programming") -> ProbeSnapshot:
        await self._maybe_sleep_session_warmup()
        started = datetime.now(timezone.utc)
        start_time = time.perf_counter()

        task_map: dict[str, asyncio.Task[GraphQLResult]] = {
            "base_cache": asyncio.create_task(self._execute_with_retry("base_cache", operations.use_base_cache_control())),
            "topic_latest_stories": asyncio.create_task(
                self._execute_with_retry("topic_latest_stories", operations.topic_latest_stories(tag_slug))
            ),
            "topic_who_to_follow": asyncio.create_task(
                self._execute_with_retry(
                    "topic_who_to_follow",
                    operations.topic_who_to_follow_publishers(tag_slug=tag_slug, first=5),
                )
            ),
            "who_to_follow_module": asyncio.create_task(
                self._execute_with_retry("who_to_follow_module", operations.who_to_follow_module())
            ),
        }

        # MEDIUM_USER_REF contract is user_id-only, so this check is safe when set.
        if self.settings.medium_user_ref:
            task_map["user_viewer_edge"] = asyncio.create_task(
                self._execute_with_retry("user_viewer_edge", operations.user_viewer_edge(self.settings.medium_user_ref))
            )

        try:
            resolved = await asyncio.gather(*task_map.values())
        except Exception:
            for task in task_map.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*task_map.values(), return_exceptions=True)
            raise
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
        actions_today_start = self.repository.actions_today_utc(TRACKED_DAILY_ACTION_TYPES)
        max_actions = self.settings.max_actions_per_day
        action_counts = self.repository.action_counts_today_utc(TRACKED_DAILY_ACTION_TYPES)
        action_limits = {
            ACTION_SUBSCRIBE: self.settings.max_subscribe_actions_per_day,
            ACTION_UNFOLLOW: self.settings.max_unfollow_actions_per_day,
            ACTION_CLAP: self.settings.max_clap_actions_per_day,
        }
        action_remaining = {
            action_type: max(0, action_limits[action_type] - action_counts.get(action_type, 0))
            for action_type in TRACKED_DAILY_ACTION_TYPES
        }
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
                action_counts_today=action_counts,
                action_limits_per_day=action_limits,
                action_remaining_per_day=action_remaining,
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
        follow_slots = min(
            self.settings.max_follow_actions_per_run,
            remaining_budget,
            len(eligible),
            action_remaining[ACTION_SUBSCRIBE],
        )
        follow_attempted, follow_verified, clap_attempted = await self._execute_follow_pipeline(
            eligible_candidates=eligible,
            max_to_run=follow_slots,
            clap_budget_remaining=action_remaining[ACTION_CLAP],
            dry_run=dry_run,
            decisions=decisions,
        )
        action_counts[ACTION_SUBSCRIBE] += follow_attempted
        action_counts[ACTION_CLAP] += clap_attempted
        action_remaining[ACTION_SUBSCRIBE] = max(0, action_limits[ACTION_SUBSCRIBE] - action_counts[ACTION_SUBSCRIBE])
        action_remaining[ACTION_CLAP] = max(0, action_limits[ACTION_CLAP] - action_counts[ACTION_CLAP])
        remaining_budget = max(0, remaining_budget - follow_attempted - clap_attempted)

        cleanup_cap = min(self.settings.cleanup_unfollow_limit, remaining_budget, action_remaining[ACTION_UNFOLLOW])
        cleanup_attempted, cleanup_verified = await self._execute_cleanup_pipeline(
            dry_run=dry_run,
            max_to_run=cleanup_cap,
            decisions=decisions,
        )
        action_counts[ACTION_UNFOLLOW] += cleanup_attempted
        action_remaining[ACTION_UNFOLLOW] = max(0, action_limits[ACTION_UNFOLLOW] - action_counts[ACTION_UNFOLLOW])
        decision_reason_counts, decision_result_counts = self._summarize_decisions(decisions)
        self._emit_decision_logs(decisions)

        actions_today_end = (
            self.repository.actions_today_utc(TRACKED_DAILY_ACTION_TYPES)
            if not dry_run
            else actions_today_start
        )
        self.log.info(
            "daily_cycle_complete",
            dry_run=dry_run,
            considered_candidates=len(candidates),
            eligible_candidates=len(eligible),
            follow_attempted=follow_attempted,
            follow_verified=follow_verified,
            clap_attempted=clap_attempted,
            cleanup_attempted=cleanup_attempted,
            cleanup_verified=cleanup_verified,
            actions_today=actions_today_end,
            max_actions=max_actions,
            action_counts=action_counts,
            action_limits=action_limits,
            action_remaining=action_remaining,
            decision_reason_counts=decision_reason_counts,
            decision_result_counts=decision_result_counts,
            day_boundary_policy=self.settings.day_boundary_policy,
        )
        return DailyRunOutcome(
            budget_exhausted=False,
            actions_today=actions_today_end,
            max_actions_per_day=max_actions,
            action_counts_today=action_counts,
            action_limits_per_day=action_limits,
            action_remaining_per_day=action_remaining,
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
            decision_reason_counts=decision_reason_counts,
            decision_result_counts=decision_result_counts,
            probe=probe,
        )

    async def _execute_safe(self, task_name: str, operation) -> GraphQLResult:
        try:
            return await self.client.execute(operation)
        except Exception as exc:  # noqa: BLE001
            return GraphQLResult(
                operationName=operation.operation_name,
                statusCode=0,
                data=None,
                errors=[GraphQLError(message=str(exc))],
                raw={"exception": str(exc)},
            )

    async def _execute_with_retry(self, task_name: str, operation) -> GraphQLResult:
        max_retries = self._retry_budget_for_task(task_name)
        target_id = self._operation_target_id(operation)
        attempt = 0
        while True:
            result = await self._execute_safe(task_name, operation)
            retryable = self._is_retryable_result(result)
            final_attempt = attempt >= max_retries or not retryable
            self.risk_guard.evaluate_result(
                task_name=task_name,
                result=result,
                is_final_attempt=final_attempt,
            )
            if final_attempt:
                result_label = self._operation_result_label(result)
                log_method = self.log.info if result_label == "ok" else self.log.warning
                log_method(
                    "operation_result",
                    operation=task_name,
                    target_id=target_id,
                    decision="execute",
                    result=result_label,
                    status_code=result.status_code,
                    error_count=len(result.errors),
                    attempts=attempt + 1,
                    max_retries=max_retries,
                )
                return result
            delay = self._retry_delay_seconds(attempt)
            self.log.warning(
                "operation_result",
                operation=task_name,
                target_id=target_id,
                decision="retry",
                result="retry_scheduled",
                attempt=attempt + 1,
                max_retries=max_retries,
                delay_seconds=round(delay, 3),
                status_code=result.status_code,
                error_count=len(result.errors),
            )
            await asyncio.sleep(delay)
            attempt += 1

    def _retry_budget_for_task(self, task_name: str) -> int:
        lowered = task_name.lower()
        if any(token in lowered for token in ("mutation", "subscribe", "unfollow", "clap")):
            return self.settings.mutation_max_retries
        if any(token in lowered for token in ("verify", "viewer_edge")):
            return self.settings.verify_max_retries
        return self.settings.query_max_retries

    def _retry_delay_seconds(self, attempt: int) -> float:
        base = self.settings.retry_base_delay_seconds
        if base <= 0:
            return 0.0
        raw = min(self.settings.retry_max_delay_seconds, base * (2**attempt))
        jitter = random.uniform(0.0, base)
        return min(self.settings.retry_max_delay_seconds, raw + jitter)

    @staticmethod
    def _is_retryable_result(result: GraphQLResult) -> bool:
        if result.status_code in {0, 408, 425, 429, 500, 502, 503, 504}:
            return True
        if not result.has_errors:
            return False
        transient_tokens = (
            "timeout",
            "temporar",
            "rate limit",
            "network",
            "socket",
            "tls",
            "unavailable",
            "internal server error",
        )
        for error in result.errors:
            message = error.message.lower()
            if any(token in message for token in transient_tokens):
                return True
        return False

    @staticmethod
    def _operation_result_label(result: GraphQLResult) -> str:
        return "ok" if result.status_code == 200 and not result.has_errors else "failed"

    @staticmethod
    def _operation_target_id(operation) -> str | None:
        variables = getattr(operation, "variables", {})
        if not isinstance(variables, dict):
            return None
        keys = (
            "userId",
            "targetUserId",
            "newsletterV3Id",
            "newsletterId",
            "postId",
            "id",
            "username",
            "slug",
        )
        for key in keys:
            value = variables.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    async def _build_candidates(
        self,
        *,
        probe: ProbeSnapshot,
        seed_user_refs: list[str],
    ) -> list[CandidateUser]:
        pool: dict[str, CandidateUser] = {}

        self._extract_topic_latest_candidates(probe, pool)
        self._extract_topic_who_to_follow_candidates(probe, pool)
        self._extract_who_to_follow_module_candidates(probe, pool)
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

    def _extract_who_to_follow_module_candidates(self, probe: ProbeSnapshot, pool: dict[str, CandidateUser]) -> None:
        result = probe.results.get("who_to_follow_module")
        if not result or not result.data:
            return
        edges = result.data.get("recommendedPublishers", {}).get("edges", [])
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if not isinstance(node, dict) or node.get("__typename") != "User":
                continue
            candidate = self._candidate_from_user_node(node, source=CandidateSource.WHO_TO_FOLLOW_MODULE)
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
            result = await self._execute_with_retry(
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
                hop_result = await self._execute_with_retry(
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
                action_types=(ACTION_SUBSCRIBE, "follow_verified", ACTION_UNFOLLOW),
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

            edge_result = await self._execute_with_retry(
                "candidate_user_viewer_edge",
                operations.user_viewer_edge(candidate.user_id),
            )
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
        clap_budget_remaining: int,
        dry_run: bool,
        decisions: list[CandidateDecision],
    ) -> tuple[int, int, int]:
        attempted = 0
        verified = 0
        clap_attempted = 0
        for candidate in eligible_candidates[:max_to_run]:
            attempted += 1

            if dry_run:
                if self.settings.enable_pre_follow_clap and clap_budget_remaining > 0:
                    clap_budget_remaining -= 1
                    clap_attempted += 1
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

            clap_used = await self._maybe_pre_follow_clap(candidate, clap_budget_remaining=clap_budget_remaining)
            if clap_used:
                clap_budget_remaining -= 1
                clap_attempted += 1

            await self._sleep_action_gap(action_type=ACTION_SUBSCRIBE, target_user_id=candidate.user_id)
            mutation = await self._execute_with_retry(
                "follow_subscribe_mutation",
                operations.subscribe_newsletter_v3(candidate.newsletter_v3_id),
            )
            mutation_ok = mutation.status_code == 200 and not mutation.has_errors
            self.repository.record_action(
                ACTION_SUBSCRIBE,
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

            verify = await self._execute_with_retry(
                "follow_verify_user_viewer_edge",
                operations.user_viewer_edge(candidate.user_id),
            )
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

        return attempted, verified, clap_attempted

    async def _maybe_pre_follow_clap(self, candidate: CandidateUser, *, clap_budget_remaining: int) -> bool:
        if not self.settings.enable_pre_follow_clap:
            return False
        if clap_budget_remaining <= 0:
            self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "budget_exhausted")
            return False

        post_id = candidate.latest_post_id
        if not post_id:
            latest_post = await self._execute_with_retry(
                "pre_follow_latest_post",
                operations.user_latest_post(user_id=candidate.user_id, username=candidate.username),
            )
            post_id = self._extract_latest_post_id(latest_post)
            if post_id:
                candidate.latest_post_id = post_id

        if not post_id:
            self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "no_post")
            return False

        if self.settings.pre_follow_read_wait_seconds > 0:
            await self._sleep_read_delay(target_user_id=candidate.user_id)

        actor_user_id = self.settings.medium_user_ref
        if not actor_user_id:
            self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "missing_actor_user_id")
            return False

        clap_count = random.randint(self.settings.min_clap_count, self.settings.max_clap_count)
        await self._sleep_action_gap(action_type=ACTION_CLAP, target_user_id=candidate.user_id)
        clap_result = await self._execute_with_retry(
            "clap_pre_follow",
            operations.clap_post(post_id, actor_user_id, num_claps=clap_count),
        )
        clap_ok = clap_result.status_code == 200 and not clap_result.has_errors
        self.repository.record_action(
            ACTION_CLAP,
            candidate.user_id,
            f"{'ok' if clap_ok else 'failed'}:{clap_count}",
        )
        return True

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

            await self._sleep_action_gap(action_type=ACTION_UNFOLLOW, target_user_id=user_id)
            mutation = await self._execute_with_retry("cleanup_unfollow", operations.unfollow_user(user_id))
            mutation_ok = mutation.status_code == 200 and not mutation.has_errors
            if not mutation_ok:
                self.repository.record_action(ACTION_UNFOLLOW, user_id, "mutation_failed")
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:unfollow_mutation_failed",
                    )
                )
                continue

            verify = await self._execute_with_retry("cleanup_verify", operations.user_viewer_edge(user_id))
            is_following = self._extract_is_following(verify)
            if is_following is False:
                verified += 1
                self.repository.mark_nonreciprocal_unfollowed(user_id)
                self.repository.record_action(ACTION_UNFOLLOW, user_id, "verified_not_following")
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
                self.repository.record_action(ACTION_UNFOLLOW, user_id, "verification_uncertain")
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:verification_uncertain",
                    )
                )

        return attempted, verified

    async def _fetch_own_follower_ids(self, limit: int) -> set[str]:
        if not self.settings.medium_user_ref:
            return set()
        result = await self._execute_with_retry(
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

    async def _sleep_action_gap(self, *, action_type: str, target_user_id: str) -> None:
        delay = await self.timing.sleep_action_gap()
        if delay <= 0:
            return
        self.log.info(
            "action_gap_sleep",
            action_type=action_type,
            target_user_id=target_user_id,
            delay_seconds=round(delay, 3),
            min_gap_seconds=self.settings.min_action_gap_seconds,
            max_gap_seconds=self.settings.max_action_gap_seconds,
        )

    async def _sleep_read_delay(self, *, target_user_id: str) -> None:
        delay = await self.timing.sleep_read_delay()
        if delay <= 0:
            return
        self.log.info(
            "pre_follow_read_sleep",
            target_user_id=target_user_id,
            delay_seconds=round(delay, 3),
            min_read_wait_seconds=self.settings.min_read_wait_seconds,
            max_read_wait_seconds=self.settings.max_read_wait_seconds,
        )

    async def _maybe_sleep_session_warmup(self) -> None:
        delay = await self.timing.maybe_sleep_session_warmup()
        if delay <= 0:
            return
        self.log.info(
            "session_warmup_sleep",
            delay_seconds=round(delay, 3),
            min_session_warmup_seconds=self.settings.min_session_warmup_seconds,
            max_session_warmup_seconds=self.settings.max_session_warmup_seconds,
        )

    def _emit_decision_logs(self, decisions: list[CandidateDecision]) -> None:
        for item in decisions:
            self.log.info(
                "candidate_decision",
                operation="decision_pipeline",
                target_id=item.user_id,
                decision="eligible" if item.eligible else "skip",
                result=self._decision_result_label(item),
                reason=item.reason,
                score=round(item.score, 4),
            )

    def _summarize_decisions(self, decisions: list[CandidateDecision]) -> tuple[dict[str, int], dict[str, int]]:
        reason_counts: dict[str, int] = {}
        result_counts: dict[str, int] = {}
        for item in decisions:
            reason_counts[item.reason] = reason_counts.get(item.reason, 0) + 1
            result = self._decision_result_label(item)
            result_counts[result] = result_counts.get(result, 0) + 1
        return reason_counts, result_counts

    @staticmethod
    def _decision_result_label(item: CandidateDecision) -> str:
        reason = item.reason
        if reason.startswith("follow_success") or reason == "cleanup:unfollow_verified":
            return "success"
        if reason.startswith("dry_run:") or reason == "cleanup:dry_run_unfollow_nonreciprocal":
            return "planned"
        if reason.startswith("skip:") or reason == "cleanup:kept_followed_back":
            return "skipped"
        if "failed" in reason or "uncertain" in reason or "unavailable" in reason:
            return "failed"
        if reason == "eligible":
            return "eligible"
        if reason.startswith("cleanup:"):
            return "cleanup"
        return "info"
