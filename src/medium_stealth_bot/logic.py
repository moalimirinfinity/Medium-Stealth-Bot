import asyncio
import math
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import structlog

from medium_stealth_bot import operations
from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.comment_templates import build_comment_template_pool
from medium_stealth_bot.graph_sync import GraphSyncService
from medium_stealth_bot.models import (
    CandidateDecision,
    CandidateScoreBreakdown,
    CandidateSource,
    CandidateUser,
    DailyRunOutcome,
    GrowthDiscoveryMode,
    GrowthMode,
    GrowthPolicy,
    GrowthSource,
    GraphSyncOutcome,
    GraphQLError,
    GraphQLResult,
    NewsletterState,
    ProbeSnapshot,
    ReconcileOutcome,
    RelationshipConfidence,
    UserFollowState,
)
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.safety import RiskGuard, RiskHaltError
from medium_stealth_bot.settings import AppSettings
from medium_stealth_bot.timing import HumanTimingController
from medium_stealth_bot.typed_payloads import (
    UserNode,
    parse_clap_count,
    parse_create_quote_id,
    parse_delete_quote_success,
    parse_delete_response_success,
    parse_latest_post_preview,
    parse_recent_post_contexts,
    parse_post_response_creators,
    parse_publish_threaded_response_id,
    parse_recommended_publishers_users,
    parse_topic_latest_story_creators,
    parse_topic_curated_list_users,
    parse_user_followers_next_from,
    parse_user_followers_users,
    parse_user_viewer_last_post_created_at,
    parse_user_viewer_user_node,
    parse_user_viewer_follower_count,
    parse_user_viewer_is_following,
    parse_viewer_clap_count,
)

ACTION_SUBSCRIBE = "follow_subscribe_attempt"
ACTION_UNFOLLOW = "cleanup_unfollow"
ACTION_CLAP = "clap_pre_follow"
ACTION_CLAP_SKIPPED = "clap_pre_follow_skipped"
ACTION_PUBLIC_TOUCH_SKIPPED = "public_touch_pre_follow_skipped"
ACTION_COMMENT = "comment_pre_follow"
ACTION_COMMENT_SKIPPED = "comment_pre_follow_skipped"
ACTION_HIGHLIGHT = "highlight_pre_follow"
ACTION_HIGHLIGHT_SKIPPED = "highlight_pre_follow_skipped"
ACTION_UNDO_CLAP = "cleanup_undo_clap"
ACTION_DELETE_COMMENT = "cleanup_delete_comment"
ACTION_DELETE_HIGHLIGHT = "cleanup_delete_highlight"
ACTION_FOLLOW_VERIFIED = "follow_verified"
FOLLOW_COOLDOWN_ACTION_TYPES: tuple[str, ...] = (
    ACTION_SUBSCRIBE,
    ACTION_FOLLOW_VERIFIED,
    ACTION_UNFOLLOW,
)
TRACKED_DAILY_ACTION_TYPES: tuple[str, ...] = (
    ACTION_SUBSCRIBE,
    ACTION_UNFOLLOW,
    ACTION_CLAP,
    ACTION_COMMENT,
)
RECENT_POST_CONTEXT_LIMIT = 3
LEAD_CLOSING_PARAGRAPH_WINDOW = 2
HIGHLIGHT_SENTENCE_MIN_CHARS = 45
HIGHLIGHT_SENTENCE_MAX_CHARS = 240
HIGHLIGHT_SENTENCE_MIN_WORDS = 8
HIGHLIGHT_SENTENCE_MAX_WORDS = 45


@dataclass(slots=True)
class ParagraphContext:
    name: str
    text: str
    paragraph_type: str | int | None
    index: int
    section: str


@dataclass(slots=True)
class SentenceSpan:
    paragraph_name: str
    text: str
    start_offset: int
    end_offset: int
    section: str


@dataclass(slots=True)
class PostContext:
    recent_rank: int
    post_id: str
    post_title: str | None
    post_version_id: str | None
    paragraphs: list[ParagraphContext]
    paragraph_text_by_name: dict[str, str]
    sentence_spans: list[SentenceSpan]


@dataclass(slots=True)
class PublicTouchPlan:
    touch_type: str
    comment_text: str | None = None
    sentence_span: SentenceSpan | None = None


class DailyRunner:
    def __init__(self, settings: AppSettings, client: MediumAsyncClient, repository: ActionRepository):
        self.settings = settings
        self.client = client
        self.repository = repository
        self.log = structlog.get_logger(__name__)
        self.risk_guard = RiskGuard(settings=settings, log=self.log)
        self.timing = HumanTimingController(settings=settings)
        self._in_live_session = False
        self._session_follow_cap_override: int | None = None
        self._session_mutations_enabled_override: bool | None = None
        self._mutations_suspended_until_monotonic = 0.0
        self._comment_mutation_supported = True
        self._highlight_mutation_supported = True
        self._persist_decision_observations = True
        self._discovery_learning_cache: dict[str, object] | None = None
        self._active_growth_policy_for_scoring: GrowthPolicy = settings.default_growth_policy
        self._candidate_recent_posts_cache: dict[str, list[PostContext]] = {}
        self._normalize_pacing_configuration()

    async def _maybe_recover_stealth_preflight_challenge(self, *, dry_run: bool) -> None:
        if dry_run or self.settings.client_mode != "stealth":
            return
        task_name = "stealth_preflight_base_cache"
        attempts = 2
        last_result: GraphQLResult | None = None
        for attempt in range(1, attempts + 1):
            last_result = await self._execute_safe(task_name, operations.use_base_cache_control())
            challenge_detail = self.risk_guard.detect_challenge_detail(last_result)
            session_detail = self.risk_guard.detect_session_expiry_detail(last_result)
            if challenge_detail is None and session_detail is None:
                if attempt > 1:
                    self.log.info(
                        "stealth_preflight_recovered",
                        attempt=attempt,
                        status_code=last_result.status_code,
                    )
                return
            detail = challenge_detail or session_detail or f"status={last_result.status_code}"
            self.log.warning(
                "stealth_preflight_blocked",
                attempt=attempt,
                attempts=attempts,
                status_code=last_result.status_code,
                detail=detail,
            )
            if attempt >= attempts:
                self.risk_guard.evaluate_result(
                    task_name=task_name,
                    result=last_result,
                    is_final_attempt=True,
                )
                return
            await self.client.reset_transport()
            await asyncio.sleep(self._retry_delay_seconds(attempt - 1))

    async def probe(self, tag_slug: str = "programming") -> ProbeSnapshot:
        self._assert_operator_not_stopped(task_name="probe")
        await self._maybe_sleep_session_warmup()
        started = datetime.now(timezone.utc)
        start_time = time.perf_counter()
        read_operations: list[tuple[str, object]] = [
            ("base_cache", operations.use_base_cache_control()),
            ("topic_latest_stories", operations.topic_latest_stories(tag_slug)),
            ("topic_who_to_follow", operations.topic_who_to_follow_publishers(tag_slug=tag_slug, first=5)),
            ("who_to_follow_module", operations.who_to_follow_module()),
        ]
        # MEDIUM_USER_REF contract is user_id-only, so this check is safe when set.
        if self.settings.medium_user_ref:
            read_operations.append(("user_viewer_edge", operations.user_viewer_edge(self.settings.medium_user_ref)))
        results: dict[str, GraphQLResult] = {}
        for task_name, operation in read_operations:
            results[task_name] = await self._execute_with_retry(task_name, operation)

        duration_ms = int((time.perf_counter() - start_time) * 1000)
        self.log.info("probe_complete", tag_slug=tag_slug, duration_ms=duration_ms, task_count=len(read_operations))
        return ProbeSnapshot(
            tag_slug=tag_slug,
            started_at=started,
            duration_ms=duration_ms,
            results=results,
        )

    async def sync_social_graph(
        self,
        *,
        dry_run: bool,
        mode: str = "auto",
        force: bool = False,
    ) -> GraphSyncOutcome:
        self._assert_operator_not_stopped(task_name="sync_social_graph")
        service = GraphSyncService(
            settings=self.settings,
            client=self.client,
            repository=self.repository,
        )
        return await service.sync(
            dry_run=dry_run,
            mode=mode,
            force=force,
        )

    async def run_daily_cycle(
        self,
        *,
        tag_slug: str = "programming",
        dry_run: bool = True,
        seed_user_refs: list[str] | None = None,
        growth_policy: GrowthPolicy | None = None,
        growth_sources: list[GrowthSource] | None = None,
        growth_mode: GrowthMode | None = None,
        discovery_mode: GrowthDiscoveryMode | None = None,
        target_user_refs: list[str] | None = None,
        target_user_scan_limit: int | None = None,
        discovery_enabled: bool = True,
        execution_enabled: bool = True,
    ) -> DailyRunOutcome:
        self._assert_operator_not_stopped(task_name="run_daily_cycle")
        if not discovery_enabled and not execution_enabled:
            raise ValueError("At least one of discovery_enabled or execution_enabled must be true.")
        if discovery_enabled and execution_enabled:
            raise ValueError("Discovery and growth execution must run separately.")
        await self._maybe_recover_stealth_preflight_challenge(dry_run=dry_run)
        resolved_growth_policy = self._resolve_growth_policy(growth_policy=growth_policy, growth_mode=growth_mode)
        self._active_growth_policy_for_scoring = resolved_growth_policy
        self._discovery_learning_cache = None
        resolved_growth_sources = (
            self._resolve_growth_sources(
                growth_sources=growth_sources,
                discovery_mode=discovery_mode,
            )
            if discovery_enabled
            else []
        )
        resolved_growth_mode = self._legacy_growth_mode_for_policy(resolved_growth_policy)
        resolved_discovery_mode = self._legacy_discovery_mode_for_sources(resolved_growth_sources)
        resolved_target_user_refs = target_user_refs or []
        resolved_target_user_scan_limit = (
            self._resolve_target_user_scan_limit(target_user_scan_limit)
            if GrowthSource.TARGET_USER_FOLLOWERS in resolved_growth_sources
            else None
        )
        if GrowthSource.TARGET_USER_FOLLOWERS in resolved_growth_sources and not resolved_target_user_refs:
            raise ValueError("target_user_refs are required for target-user-followers discovery.")
        if not self._in_live_session:
            self.timing.reset_session_state()
            self.timing.reset_metrics()
        self.timing.set_simulation_mode(dry_run)
        self._persist_decision_observations = not dry_run
        actions_today_start = self.repository.actions_today_utc(TRACKED_DAILY_ACTION_TYPES)
        max_actions = self.settings.max_actions_per_day
        action_counts = self.repository.action_counts_today_utc(TRACKED_DAILY_ACTION_TYPES)
        action_limits = {
            ACTION_SUBSCRIBE: self.settings.max_subscribe_actions_per_day,
            ACTION_UNFOLLOW: self.settings.max_unfollow_actions_per_day,
            ACTION_CLAP: self.settings.max_clap_actions_per_day,
            ACTION_COMMENT: self.settings.max_comment_actions_per_day,
        }
        action_remaining = {
            action_type: max(0, action_limits[action_type] - action_counts.get(action_type, 0))
            for action_type in TRACKED_DAILY_ACTION_TYPES
        }
        if execution_enabled and actions_today_start >= max_actions:
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
                cleanup_only_mode=False,
                growth_policy=resolved_growth_policy,
                growth_sources=resolved_growth_sources,
                growth_mode=resolved_growth_mode,
                discovery_mode=resolved_discovery_mode,
                target_user_refs=resolved_target_user_refs,
                target_user_scan_limit=resolved_target_user_scan_limit,
                action_counts_today=action_counts,
                action_limits_per_day=action_limits,
                action_remaining_per_day=action_remaining,
                dry_run=dry_run,
                probe=None,
                client_metrics=self.client.metrics_snapshot(),
            )

        decisions: list[CandidateDecision] = []
        remaining_budget = max(0, max_actions - actions_today_start)
        follow_limit_for_cycle = self._resolved_follow_limit_for_cycle()
        mutations_enabled = self._mutations_enabled_for_cycle(dry_run=dry_run)
        if not mutations_enabled and not dry_run:
            self.log.info("pacing_mutations_suspended_for_cycle")
        max_follow_attempts_for_cycle = min(
            follow_limit_for_cycle,
            remaining_budget,
            action_remaining[ACTION_SUBSCRIBE],
        )
        if not mutations_enabled and not dry_run:
            max_follow_attempts_for_cycle = 0

        probe: ProbeSnapshot | None = None
        discovery_candidates: list[CandidateUser] = []
        discovery_source_candidate_counts: dict[str, int] = {}
        source_candidate_counts: dict[str, int] = {}
        discovery_buffered: list[CandidateUser] = []
        queue_cleaned_counts = {"queued_held": 0, "deferred_held": 0, "terminal": 0, "total": 0}
        queue_pruned_counts = {"followed": 0, "rejected": 0, "stale": 0, "total": 0}
        queue_cleaned_counts = self.repository.purge_non_actionable_growth_candidates(dry_run=dry_run)
        if not dry_run:
            queue_pruned_counts = self.repository.prune_growth_candidate_queue(
                followed_after_days=self.settings.growth_queue_prune_followed_after_days,
                rejected_after_days=self.settings.growth_queue_prune_rejected_after_days,
                stale_after_days=self.settings.growth_queue_prune_stale_after_days,
            )
        queue_counts_before = self.repository.growth_queue_state_counts()
        queue_ready_before = queue_counts_before["ready"]
        queue_ready_after = queue_ready_before
        effective_queue_total = max(0, queue_counts_before["total"] - (queue_cleaned_counts["total"] if dry_run else 0))
        queue_capacity = max(0, self.settings.growth_candidate_queue_max_size - effective_queue_total)
        discovery_target = (
            min(self.settings.discovery_eligible_per_run, queue_capacity)
            if discovery_enabled
            else 0
        )
        discovery_scan_limit = self._discovery_candidate_scan_limit(discovery_target) if discovery_target > 0 else 0
        discovery_enqueued = 0
        discovery_replenished = False

        if discovery_enabled and discovery_target > 0:
            probe = await self.probe(tag_slug=tag_slug) if self._growth_sources_need_probe(resolved_growth_sources) else None
            seed_refs = (seed_user_refs or []) + self.settings.discovery_seed_users
            existing_growth_candidate_ids = self.repository.growth_candidate_user_ids()
            discovery_candidates = await self._build_candidates(
                tag_slug=tag_slug,
                probe=probe,
                seed_user_refs=seed_refs,
                growth_sources=resolved_growth_sources,
                target_user_refs=resolved_target_user_refs,
                target_user_scan_limit=resolved_target_user_scan_limit,
                candidate_scan_limit=discovery_scan_limit,
            )
            discovery_source_candidate_counts = self._source_counts(discovery_candidates)
            discovery_screened = self._screen_discovery_candidates(
                discovery_candidates,
                decisions=decisions,
                persist_observations=not dry_run,
                existing_growth_candidate_ids=existing_growth_candidate_ids,
            )
            discovery_buffered = await self._evaluate_discovery_candidates(
                discovery_screened,
                decisions=decisions,
                persist_observations=not dry_run,
                max_eligible=discovery_target,
            )
            discovery_enqueued = len(discovery_buffered)
            discovery_replenished = True
            if not dry_run:
                discovery_enqueued = self.repository.upsert_growth_candidate_buffer(
                    discovery_buffered,
                    queue_reason="eligible:execution_ready",
                    max_total=self.settings.growth_candidate_queue_max_size,
                )
                queue_ready_after = self.repository.growth_queue_ready_count()
            else:
                queue_ready_after = len(discovery_buffered)
        elif discovery_enabled:
            self.log.info(
                "discovery_queue_capacity_exhausted",
                queue_total=queue_counts_before["total"],
                queue_max=self.settings.growth_candidate_queue_max_size,
            )

        if discovery_enabled and not execution_enabled:
            execution_pool = list(discovery_buffered)
            execution_pool_limit = len(execution_pool)
        else:
            execution_pool_limit = self._execution_queue_fetch_limit(max_follow_attempts_for_cycle)
            if execution_pool_limit <= 0:
                execution_pool = []
            elif dry_run and discovery_enabled:
                execution_pool = discovery_buffered[:execution_pool_limit]
            else:
                execution_pool = self.repository.queued_growth_candidates(
                    limit=execution_pool_limit,
                    due_deferred_reserve_ratio=self.settings.growth_queue_due_deferred_reserve_ratio,
                )
        source_candidate_counts = self._source_counts(execution_pool)
        discovered_candidates = len(discovery_candidates)
        screened_candidates = len(execution_pool)
        considered_candidates = screened_candidates
        if execution_pool_limit <= 0:
            eligible = []
        else:
            # Queue-only growth trusts discovery as the eligibility boundary.
            # Growth only applies budgets, pacing, action preparation, and the
            # final live follow-state gate immediately before mutation.
            eligible = list(execution_pool)

        if execution_enabled:
            follow_slots = min(max_follow_attempts_for_cycle, len(eligible))
            (
                follow_attempted,
                follow_verified,
                clap_attempted,
                clap_verified,
                public_touch_attempted,
                public_touch_verified,
                comment_attempted,
                comment_verified,
                highlight_attempted,
                highlight_verified,
                source_follow_verified_counts,
            ) = (
                await self._execute_follow_pipeline(
                    eligible_candidates=eligible,
                    max_to_run=follow_slots,
                    clap_budget_remaining=action_remaining[ACTION_CLAP],
                    comment_budget_remaining=action_remaining[ACTION_COMMENT],
                    dry_run=dry_run,
                    decisions=decisions,
                    growth_policy=resolved_growth_policy,
                )
            )
        else:
            follow_attempted = 0
            follow_verified = 0
            clap_attempted = 0
            clap_verified = 0
            public_touch_attempted = 0
            public_touch_verified = 0
            comment_attempted = 0
            comment_verified = 0
            highlight_attempted = 0
            highlight_verified = 0
            source_follow_verified_counts = {}
        executed_candidates = follow_attempted
        followed_candidates = follow_verified
        action_counts[ACTION_SUBSCRIBE] += follow_attempted
        action_counts[ACTION_CLAP] += clap_attempted
        action_counts[ACTION_COMMENT] += public_touch_attempted
        action_remaining[ACTION_SUBSCRIBE] = max(0, action_limits[ACTION_SUBSCRIBE] - action_counts[ACTION_SUBSCRIBE])
        action_remaining[ACTION_CLAP] = max(0, action_limits[ACTION_CLAP] - action_counts[ACTION_CLAP])
        action_remaining[ACTION_COMMENT] = max(0, action_limits[ACTION_COMMENT] - action_counts[ACTION_COMMENT])
        cleanup_attempted = 0
        cleanup_verified = 0
        decision_reason_counts, decision_result_counts = self._summarize_decisions(decisions)
        self._emit_decision_logs(decisions)

        actions_today_end = (
            self.repository.actions_today_utc(TRACKED_DAILY_ACTION_TYPES)
            if not dry_run
            else actions_today_start
        )
        if not dry_run:
            queue_ready_after = self.repository.growth_queue_ready_count()
        queue_counts = self.repository.growth_queue_state_counts()

        kpis = self._build_kpis(
            follow_attempted=follow_attempted,
            follow_verified=follow_verified,
            cleanup_attempted=cleanup_attempted,
            cleanup_verified=cleanup_verified,
            eligible_candidates=len(eligible),
            clap_attempted=clap_attempted,
            clap_verified=clap_verified,
            public_touch_attempted=public_touch_attempted,
            public_touch_verified=public_touch_verified,
            comment_attempted=comment_attempted,
            comment_verified=comment_verified,
            highlight_attempted=highlight_attempted,
            highlight_verified=highlight_verified,
        )
        kpis.update(self.repository.follow_cycle_kpis())
        (
            conversion_by_source,
            conversion_by_policy,
            conversion_by_source_policy,
        ) = self.repository.follow_cycle_conversion_breakdowns()
        kpis["selected_growth_sources_count"] = len(resolved_growth_sources)
        kpis["selected_growth_policy_follow_verified"] = follow_verified
        kpis["growth_discovery_enabled"] = 1 if discovery_enabled else 0
        kpis["growth_execution_enabled"] = 1 if execution_enabled else 0
        kpis["growth_queue_ready_before_discovery"] = queue_ready_before
        kpis["growth_queue_ready_after_discovery"] = queue_ready_after
        kpis["growth_queue_due_deferred_reserve_ratio"] = round(
            self.settings.growth_queue_due_deferred_reserve_ratio,
            3,
        )
        kpis["growth_queue_discovery_target"] = discovery_target
        kpis["growth_queue_discovery_scan_limit"] = discovery_scan_limit
        kpis["growth_queue_candidate_cap"] = self.settings.growth_candidate_queue_max_size
        kpis["growth_queue_capacity_before_discovery"] = queue_capacity
        kpis["growth_queue_discovery_enqueued"] = discovery_enqueued
        kpis["growth_queue_discovery_candidates"] = discovered_candidates
        kpis["growth_queue_execution_pool"] = screened_candidates
        kpis["growth_queue_considered_candidates"] = considered_candidates
        kpis["growth_queue_screened_candidates"] = screened_candidates
        kpis["growth_queue_executed_candidates"] = executed_candidates
        kpis["growth_queue_followed_candidates"] = followed_candidates
        kpis["growth_queue_replenished"] = 1 if discovery_replenished else 0
        kpis["growth_queue_cleaned_queued_held"] = queue_cleaned_counts["queued_held"]
        kpis["growth_queue_cleaned_deferred_held"] = queue_cleaned_counts["deferred_held"]
        kpis["growth_queue_cleaned_terminal"] = queue_cleaned_counts["terminal"]
        kpis["growth_queue_cleaned_total"] = queue_cleaned_counts["total"]
        kpis["growth_queue_pruned_followed"] = queue_pruned_counts["followed"]
        kpis["growth_queue_pruned_rejected"] = queue_pruned_counts["rejected"]
        kpis["growth_queue_pruned_stale"] = queue_pruned_counts["stale"]
        kpis["growth_queue_pruned_total"] = queue_pruned_counts["total"]
        kpis["growth_queue_ready"] = queue_counts["ready"]
        kpis["growth_queue_queued"] = queue_counts["queued"]
        kpis["growth_queue_queued_held"] = queue_counts.get("queued_held", 0)
        kpis["growth_queue_deferred"] = queue_counts["deferred"]
        kpis["growth_queue_deferred_due"] = queue_counts["deferred_due"]
        kpis["growth_queue_deferred_future"] = queue_counts["deferred_future"]
        kpis["growth_queue_deferred_held"] = queue_counts.get("deferred_held", 0)
        kpis["growth_queue_rejected"] = queue_counts["rejected"]
        kpis["growth_queue_followed"] = queue_counts["followed"]
        kpis["growth_queue_total"] = queue_counts["total"]
        kpis["growth_funnel_discovered"] = discovered_candidates
        kpis["growth_funnel_screened"] = screened_candidates
        kpis["growth_funnel_eligible"] = len(eligible)
        kpis["growth_funnel_executed"] = executed_candidates
        kpis["growth_funnel_followed"] = followed_candidates
        kpis["growth_funnel_discovered_to_screened_rate"] = round(
            (screened_candidates / discovered_candidates) if discovered_candidates > 0 else 0.0,
            4,
        )
        kpis["growth_funnel_screened_to_eligible_rate"] = round(
            (len(eligible) / screened_candidates) if screened_candidates > 0 else 0.0,
            4,
        )
        kpis["growth_funnel_screened_to_executed_rate"] = round(
            (executed_candidates / screened_candidates) if screened_candidates > 0 else 0.0,
            4,
        )
        kpis["growth_funnel_executed_to_followed_rate"] = round(
            (followed_candidates / executed_candidates) if executed_candidates > 0 else 0.0,
            4,
        )
        kpis["growth_funnel_screened_to_followed_rate"] = round(
            (followed_candidates / screened_candidates) if screened_candidates > 0 else 0.0,
            4,
        )
        kpis["growth_funnel_discovered_to_followed_rate"] = round(
            (followed_candidates / discovered_candidates) if discovered_candidates > 0 else 0.0,
            4,
        )
        for source, count in sorted(discovery_source_candidate_counts.items()):
            kpis[f"growth_discovery_source_candidates__{source}"] = count
        for source, count in sorted(source_candidate_counts.items()):
            kpis[f"growth_execution_source_candidates__{source}"] = count
            kpis[f"growth_screened_source_candidates__{source}"] = count
        kpis.update(self.timing.metrics_snapshot())
        kpis["pacing_mutations_enabled"] = 1 if mutations_enabled else 0
        client_metrics = self.client.metrics_snapshot()

        self.log.info(
            "daily_cycle_complete",
            dry_run=dry_run,
            discovery_enabled=discovery_enabled,
            execution_enabled=execution_enabled,
            discovered_candidates=discovered_candidates,
            screened_candidates=screened_candidates,
            executed_candidates=executed_candidates,
            followed_candidates=followed_candidates,
            considered_candidates=considered_candidates,
            eligible_candidates=len(eligible),
            queue_ready_before=queue_ready_before,
            queue_ready_after=queue_ready_after,
            queue_counts=queue_counts,
            queue_cleaned_counts=queue_cleaned_counts,
            queue_pruned_counts=queue_pruned_counts,
            discovery_enqueued=discovery_enqueued,
            execution_pool=len(execution_pool),
            follow_attempted=follow_attempted,
            follow_verified=follow_verified,
            clap_attempted=clap_attempted,
            clap_verified=clap_verified,
            public_touch_attempted=public_touch_attempted,
            public_touch_verified=public_touch_verified,
            comment_attempted=comment_attempted,
            comment_verified=comment_verified,
            highlight_attempted=highlight_attempted,
            highlight_verified=highlight_verified,
            cleanup_attempted=cleanup_attempted,
            cleanup_verified=cleanup_verified,
            actions_today=actions_today_end,
            max_actions=max_actions,
            action_counts=action_counts,
            action_limits=action_limits,
            action_remaining=action_remaining,
            discovery_source_candidate_counts=discovery_source_candidate_counts,
            source_candidate_counts=source_candidate_counts,
            source_screened_candidate_counts=source_candidate_counts,
            source_follow_verified_counts=source_follow_verified_counts,
            follow_limit_for_cycle=follow_limit_for_cycle,
            growth_policy=resolved_growth_policy.value,
            growth_sources=[source.value for source in resolved_growth_sources],
            growth_mode=resolved_growth_mode.value,
            discovery_mode=resolved_discovery_mode.value,
            target_user_refs=resolved_target_user_refs,
            target_user_scan_limit=resolved_target_user_scan_limit,
            mutations_enabled=mutations_enabled,
            decision_reason_counts=decision_reason_counts,
            decision_result_counts=decision_result_counts,
            kpis=kpis,
            client_metrics=client_metrics,
            day_boundary_policy=self.settings.day_boundary_policy,
        )
        return DailyRunOutcome(
            budget_exhausted=False,
            actions_today=actions_today_end,
            max_actions_per_day=max_actions,
            cleanup_only_mode=False,
            growth_policy=resolved_growth_policy,
            growth_sources=resolved_growth_sources,
            growth_mode=resolved_growth_mode,
            discovery_mode=resolved_discovery_mode,
            target_user_refs=resolved_target_user_refs,
            target_user_scan_limit=resolved_target_user_scan_limit,
            action_counts_today=action_counts,
            action_limits_per_day=action_limits,
            action_remaining_per_day=action_remaining,
            dry_run=dry_run,
            discovered_candidates=discovered_candidates,
            screened_candidates=screened_candidates,
            executed_candidates=executed_candidates,
            followed_candidates=followed_candidates,
            considered_candidates=considered_candidates,
            eligible_candidates=len(eligible),
            follow_actions_attempted=follow_attempted,
            follow_actions_verified=follow_verified,
            clap_actions_attempted=clap_attempted,
            clap_actions_verified=clap_verified,
            public_touch_actions_attempted=public_touch_attempted,
            public_touch_actions_verified=public_touch_verified,
            comment_actions_attempted=comment_attempted,
            comment_actions_verified=comment_verified,
            highlight_actions_attempted=highlight_attempted,
            highlight_actions_verified=highlight_verified,
            cleanup_actions_attempted=cleanup_attempted,
            cleanup_actions_verified=cleanup_verified,
            source_candidate_counts=source_candidate_counts,
            source_follow_verified_counts=source_follow_verified_counts,
            policy_follow_verified_counts={resolved_growth_policy.value: follow_verified},
            conversion_by_source=conversion_by_source,
            conversion_by_policy=conversion_by_policy,
            conversion_by_source_policy=conversion_by_source_policy,
            kpis=kpis,
            client_metrics=client_metrics,
            decision_log=[
                f"{item.reason} (id={item.user_id})"
                for item in decisions[:80]
            ],
            decision_reason_counts=decision_reason_counts,
            decision_result_counts=decision_result_counts,
            probe=probe,
        )

    async def run_discovery_cycle(
        self,
        *,
        tag_slug: str = "programming",
        dry_run: bool = True,
        seed_user_refs: list[str] | None = None,
        growth_sources: list[GrowthSource] | None = None,
        discovery_mode: GrowthDiscoveryMode | None = None,
        target_user_refs: list[str] | None = None,
        target_user_scan_limit: int | None = None,
    ) -> DailyRunOutcome:
        return await self.run_daily_cycle(
            tag_slug=tag_slug,
            dry_run=dry_run,
            seed_user_refs=seed_user_refs,
            growth_policy=self.settings.default_growth_policy,
            growth_sources=growth_sources,
            discovery_mode=discovery_mode,
            target_user_refs=target_user_refs,
            target_user_scan_limit=target_user_scan_limit,
            discovery_enabled=True,
            execution_enabled=False,
        )

    async def run_growth_queue_cycle(
        self,
        *,
        tag_slug: str = "programming",
        dry_run: bool = True,
        growth_policy: GrowthPolicy | None = None,
        growth_mode: GrowthMode | None = None,
    ) -> DailyRunOutcome:
        return await self.run_daily_cycle(
            tag_slug=tag_slug,
            dry_run=dry_run,
            growth_policy=growth_policy,
            growth_mode=growth_mode,
            growth_sources=[],
            discovery_enabled=False,
            execution_enabled=True,
        )

    async def run_live_session(
        self,
        *,
        tag_slug: str = "programming",
        seed_user_refs: list[str] | None = None,
        target_follow_attempts: int | None = None,
        max_duration_minutes: int | None = None,
        max_passes: int | None = None,
        growth_policy: GrowthPolicy | None = None,
        growth_sources: list[GrowthSource] | None = None,
        growth_mode: GrowthMode | None = None,
        discovery_mode: GrowthDiscoveryMode | None = None,
        target_user_refs: list[str] | None = None,
        target_user_scan_limit: int | None = None,
        discovery_enabled: bool = False,
    ) -> DailyRunOutcome:
        self._assert_operator_not_stopped(task_name="run_live_session")
        resolved_growth_policy = self._resolve_growth_policy(growth_policy=growth_policy, growth_mode=growth_mode)
        resolved_growth_sources = (
            self._resolve_growth_sources(
                growth_sources=growth_sources,
                discovery_mode=discovery_mode,
            )
            if discovery_enabled
            else []
        )
        resolved_growth_mode = self._legacy_growth_mode_for_policy(resolved_growth_policy)
        resolved_discovery_mode = self._legacy_discovery_mode_for_sources(resolved_growth_sources)
        resolved_target_user_refs = target_user_refs or []
        resolved_target_user_scan_limit = (
            self._resolve_target_user_scan_limit(target_user_scan_limit)
            if GrowthSource.TARGET_USER_FOLLOWERS in resolved_growth_sources
            else None
        )
        if GrowthSource.TARGET_USER_FOLLOWERS in resolved_growth_sources and not resolved_target_user_refs:
            raise ValueError("target_user_refs are required for target-user-followers discovery.")

        resolved_target_follows = max(1, target_follow_attempts or self.settings.live_session_target_follow_attempts)
        resolved_min_follows = max(1, self.settings.live_session_min_follow_attempts)
        resolved_min_follows = min(resolved_min_follows, resolved_target_follows)
        resolved_duration_minutes = max(1, max_duration_minutes or self.settings.live_session_duration_minutes)
        configured_max_passes = max(1, max_passes or self.settings.live_session_max_passes)
        baseline_follow_cap = max(1, self.settings.max_follow_actions_per_run)
        resolved_max_passes = configured_max_passes
        max_duration_seconds = float(resolved_duration_minutes * 60)

        started_at = time.perf_counter()
        pass_count = 0
        stop_reason: str | None = None
        last_outcome: DailyRunOutcome | None = None

        total_discovered = 0
        total_screened = 0
        total_executed = 0
        total_followed = 0
        total_considered = 0
        total_eligible = 0
        total_follow_attempted = 0
        total_follow_verified = 0
        total_clap_attempted = 0
        total_clap_verified = 0
        total_public_touch_attempted = 0
        total_public_touch_verified = 0
        total_comment_attempted = 0
        total_comment_verified = 0
        total_highlight_attempted = 0
        total_highlight_verified = 0
        total_cleanup_attempted = 0
        total_cleanup_verified = 0
        total_source_candidate_counts: dict[str, int] = {}
        total_source_follow_verified_counts: dict[str, int] = {}
        total_reason_counts: dict[str, int] = {}
        total_result_counts: dict[str, int] = {}
        decision_log_sample: list[str] = []
        pacing_degrade_events = 0
        suspended_seconds_total = 0.0
        prior_mutation_window_hits = self.timing.mutation_window_limit_hits

        self.timing.reset_session_state()
        self.timing.reset_metrics()
        self.timing.set_simulation_mode(False)
        self._mutations_suspended_until_monotonic = 0.0
        self._in_live_session = True
        try:
            while pass_count < resolved_max_passes:
                elapsed_before_pass = time.perf_counter() - started_at
                if elapsed_before_pass >= max_duration_seconds:
                    stop_reason = "duration_reached"
                    break
                if total_follow_attempted >= resolved_target_follows:
                    stop_reason = "follow_target_reached"
                    break

                pass_count += 1
                remaining_hard_target = max(0, resolved_target_follows - total_follow_attempted)
                passes_remaining_including_current = max(1, resolved_max_passes - pass_count + 1)
                remaining_soft_floor = max(0, resolved_min_follows - total_follow_attempted)
                adaptive_floor_cap = (
                    max(1, math.ceil(remaining_soft_floor / passes_remaining_including_current))
                    if remaining_soft_floor > 0
                    else 1
                )
                follow_cap_this_pass = min(
                    remaining_hard_target,
                    max(baseline_follow_cap, adaptive_floor_cap),
                )
                now_monotonic = time.monotonic()
                mutations_enabled = now_monotonic >= self._mutations_suspended_until_monotonic
                if not mutations_enabled:
                    follow_cap_this_pass = 0
                self._session_follow_cap_override = follow_cap_this_pass
                self._session_mutations_enabled_override = mutations_enabled
                self.log.info(
                    "live_session_pass_start",
                    pass_index=pass_count,
                    target_follow_attempts=resolved_target_follows,
                    target_follow_attempts_min=resolved_min_follows,
                    max_duration_minutes=resolved_duration_minutes,
                    max_passes=resolved_max_passes,
                    elapsed_seconds=round(elapsed_before_pass, 3),
                    follow_cap_this_pass=follow_cap_this_pass,
                    growth_policy=resolved_growth_policy.value,
                    growth_sources=[source.value for source in resolved_growth_sources],
                    growth_mode=resolved_growth_mode.value,
                    discovery_mode=resolved_discovery_mode.value,
                    target_user_refs=resolved_target_user_refs,
                    target_user_scan_limit=resolved_target_user_scan_limit,
                    mutations_enabled=mutations_enabled,
                )
                outcome = await self.run_daily_cycle(
                    tag_slug=tag_slug,
                    dry_run=False,
                    seed_user_refs=seed_user_refs,
                    growth_policy=resolved_growth_policy,
                    growth_sources=resolved_growth_sources,
                    growth_mode=resolved_growth_mode,
                    discovery_mode=resolved_discovery_mode,
                    target_user_refs=resolved_target_user_refs,
                    target_user_scan_limit=resolved_target_user_scan_limit,
                    discovery_enabled=discovery_enabled,
                    execution_enabled=True,
                )
                last_outcome = outcome

                total_discovered += outcome.discovered_candidates
                total_screened += outcome.screened_candidates
                total_executed += outcome.executed_candidates
                total_followed += outcome.followed_candidates
                total_considered += outcome.considered_candidates
                total_eligible += outcome.eligible_candidates
                total_follow_attempted += outcome.follow_actions_attempted
                total_follow_verified += outcome.follow_actions_verified
                total_clap_attempted += outcome.clap_actions_attempted
                total_clap_verified += outcome.clap_actions_verified
                total_public_touch_attempted += outcome.public_touch_actions_attempted
                total_public_touch_verified += outcome.public_touch_actions_verified
                total_comment_attempted += outcome.comment_actions_attempted
                total_comment_verified += outcome.comment_actions_verified
                total_highlight_attempted += outcome.highlight_actions_attempted
                total_highlight_verified += outcome.highlight_actions_verified
                total_cleanup_attempted += outcome.cleanup_actions_attempted
                total_cleanup_verified += outcome.cleanup_actions_verified
                self._merge_int_counts(total_source_candidate_counts, outcome.source_candidate_counts)
                self._merge_int_counts(total_source_follow_verified_counts, outcome.source_follow_verified_counts)
                self._merge_int_counts(total_reason_counts, outcome.decision_reason_counts)
                self._merge_int_counts(total_result_counts, outcome.decision_result_counts)
                self._append_decision_log_sample(decision_log_sample, outcome.decision_log, max_size=120)

                elapsed_after_pass = time.perf_counter() - started_at
                expected_upper = max(
                    1,
                    math.ceil((min(elapsed_after_pass, max_duration_seconds) / max_duration_seconds) * resolved_target_follows),
                )
                mutation_window_hits_now = self.timing.mutation_window_limit_hits
                mutation_window_hit_delta = max(0, mutation_window_hits_now - prior_mutation_window_hits)
                prior_mutation_window_hits = mutation_window_hits_now
                should_soft_degrade = mutation_window_hit_delta > 0 or total_follow_attempted > expected_upper
                if should_soft_degrade and self.settings.pacing_soft_degrade_cooldown_seconds > 0:
                    now_for_degrade = time.monotonic()
                    candidate_suspend_until = now_for_degrade + float(self.settings.pacing_soft_degrade_cooldown_seconds)
                    if candidate_suspend_until > self._mutations_suspended_until_monotonic:
                        added = candidate_suspend_until - max(now_for_degrade, self._mutations_suspended_until_monotonic)
                        suspended_seconds_total += max(0.0, added)
                        self._mutations_suspended_until_monotonic = candidate_suspend_until
                    pacing_degrade_events += 1
                    self.log.warning(
                        "live_session_soft_degrade_activated",
                        pass_index=pass_count,
                        follow_attempted_total=total_follow_attempted,
                        expected_upper=expected_upper,
                        mutation_window_hit_delta=mutation_window_hit_delta,
                        suspend_until_seconds=round(
                            max(0.0, self._mutations_suspended_until_monotonic - now_for_degrade),
                            3,
                        ),
                    )

                self.log.info(
                    "live_session_pass_complete",
                    pass_index=pass_count,
                    elapsed_seconds=round(elapsed_after_pass, 3),
                    follow_attempted_this_pass=outcome.follow_actions_attempted,
                    follow_attempted_total=total_follow_attempted,
                    follow_verified_total=total_follow_verified,
                    public_touch_attempted_this_pass=outcome.public_touch_actions_attempted,
                    public_touch_attempted_total=total_public_touch_attempted,
                    public_touch_verified_total=total_public_touch_verified,
                    comment_attempted_this_pass=outcome.comment_actions_attempted,
                    comment_attempted_total=total_comment_attempted,
                    comment_verified_total=total_comment_verified,
                    highlight_attempted_this_pass=outcome.highlight_actions_attempted,
                    highlight_attempted_total=total_highlight_attempted,
                    highlight_verified_total=total_highlight_verified,
                    cleanup_attempted_this_pass=outcome.cleanup_actions_attempted,
                    cleanup_verified_total=total_cleanup_verified,
                    budget_exhausted=outcome.budget_exhausted,
                )

                if (
                    not discovery_enabled
                    and outcome.screened_candidates == 0
                    and outcome.eligible_candidates == 0
                    and outcome.follow_actions_attempted == 0
                ):
                    stop_reason = "queue_empty"
                    break
                if outcome.budget_exhausted:
                    stop_reason = "budget_exhausted"
                    break
                if total_follow_attempted >= resolved_target_follows:
                    stop_reason = "follow_target_reached"
                    break
                if elapsed_after_pass >= max_duration_seconds:
                    stop_reason = "duration_reached"
                    break
                if pass_count < resolved_max_passes:
                    await self._sleep_pass_cooldown()

            if stop_reason is None:
                stop_reason = "max_passes_reached"
        finally:
            self._in_live_session = False
            self._session_follow_cap_override = None
            self._session_mutations_enabled_override = None
            self._mutations_suspended_until_monotonic = 0.0

        elapsed_total = round(time.perf_counter() - started_at, 3)
        if last_outcome is None:
            actions_today = self.repository.actions_today_utc(TRACKED_DAILY_ACTION_TYPES)
            action_counts = self.repository.action_counts_today_utc(TRACKED_DAILY_ACTION_TYPES)
            action_limits = {
                ACTION_SUBSCRIBE: self.settings.max_subscribe_actions_per_day,
                ACTION_UNFOLLOW: self.settings.max_unfollow_actions_per_day,
                ACTION_CLAP: self.settings.max_clap_actions_per_day,
                ACTION_COMMENT: self.settings.max_comment_actions_per_day,
            }
            action_remaining = {
                action_type: max(0, action_limits[action_type] - action_counts.get(action_type, 0))
                for action_type in TRACKED_DAILY_ACTION_TYPES
            }
            kpis = self._build_kpis(
                follow_attempted=0,
                follow_verified=0,
                cleanup_attempted=0,
                cleanup_verified=0,
                eligible_candidates=0,
                clap_attempted=0,
                clap_verified=0,
                comment_attempted=0,
                comment_verified=0,
            )
            kpis.update(self.repository.follow_cycle_kpis())
            (
                conversion_by_source,
                conversion_by_policy,
                conversion_by_source_policy,
            ) = self.repository.follow_cycle_conversion_breakdowns()
            kpis.update(
                {
                    "growth_funnel_discovered": 0,
                    "growth_funnel_screened": 0,
                    "growth_funnel_eligible": 0,
                    "growth_funnel_executed": 0,
                    "growth_funnel_followed": 0,
                    "session_passes": pass_count,
                    "session_elapsed_seconds": elapsed_total,
                    "session_target_follow_attempts": resolved_target_follows,
                    "session_target_follow_attempts_min": resolved_min_follows,
                    "session_target_duration_minutes": resolved_duration_minutes,
                    "session_soft_floor_met": 0,
                    "session_soft_floor_remaining": resolved_min_follows,
                    "session_pacing_degrade_events": pacing_degrade_events,
                    "session_mutation_suspended_seconds_total": round(suspended_seconds_total, 3),
                }
            )
            kpis.update(self.timing.metrics_snapshot())
            return DailyRunOutcome(
                budget_exhausted=actions_today >= self.settings.max_actions_per_day,
                actions_today=actions_today,
                max_actions_per_day=self.settings.max_actions_per_day,
                cleanup_only_mode=False,
                growth_policy=resolved_growth_policy,
                growth_sources=resolved_growth_sources,
                growth_mode=resolved_growth_mode,
                discovery_mode=resolved_discovery_mode,
                target_user_refs=resolved_target_user_refs,
                target_user_scan_limit=resolved_target_user_scan_limit,
                action_counts_today=action_counts,
                action_limits_per_day=action_limits,
                action_remaining_per_day=action_remaining,
                dry_run=False,
                conversion_by_source=conversion_by_source,
                conversion_by_policy=conversion_by_policy,
                conversion_by_source_policy=conversion_by_source_policy,
                kpis=kpis,
                client_metrics=self.client.metrics_snapshot(),
                session_passes=pass_count,
                session_elapsed_seconds=elapsed_total,
                session_stop_reason=stop_reason,
                session_target_follow_attempts=resolved_target_follows,
                session_target_duration_minutes=resolved_duration_minutes,
            )

        kpis = self._build_kpis(
            follow_attempted=total_follow_attempted,
            follow_verified=total_follow_verified,
            cleanup_attempted=total_cleanup_attempted,
            cleanup_verified=total_cleanup_verified,
            eligible_candidates=total_eligible,
            clap_attempted=total_clap_attempted,
            clap_verified=total_clap_verified,
            public_touch_attempted=total_public_touch_attempted,
            public_touch_verified=total_public_touch_verified,
            comment_attempted=total_comment_attempted,
            comment_verified=total_comment_verified,
            highlight_attempted=total_highlight_attempted,
            highlight_verified=total_highlight_verified,
        )
        kpis.update(self.repository.follow_cycle_kpis())
        (
            conversion_by_source,
            conversion_by_policy,
            conversion_by_source_policy,
        ) = self.repository.follow_cycle_conversion_breakdowns()
        kpis.update(
            {
                "growth_funnel_discovered": total_discovered,
                "growth_funnel_screened": total_screened,
                "growth_funnel_eligible": total_eligible,
                "growth_funnel_executed": total_executed,
                "growth_funnel_followed": total_followed,
                "growth_funnel_discovered_to_screened_rate": round(
                    (total_screened / total_discovered) if total_discovered > 0 else 0.0,
                    4,
                ),
                "growth_funnel_screened_to_eligible_rate": round(
                    (total_eligible / total_screened) if total_screened > 0 else 0.0,
                    4,
                ),
                "growth_funnel_screened_to_executed_rate": round(
                    (total_executed / total_screened) if total_screened > 0 else 0.0,
                    4,
                ),
                "growth_funnel_executed_to_followed_rate": round(
                    (total_followed / total_executed) if total_executed > 0 else 0.0,
                    4,
                ),
                "growth_funnel_screened_to_followed_rate": round(
                    (total_followed / total_screened) if total_screened > 0 else 0.0,
                    4,
                ),
                "growth_funnel_discovered_to_followed_rate": round(
                    (total_followed / total_discovered) if total_discovered > 0 else 0.0,
                    4,
                ),
                "session_passes": pass_count,
                "session_elapsed_seconds": elapsed_total,
                "session_target_follow_attempts": resolved_target_follows,
                "session_target_follow_attempts_min": resolved_min_follows,
                "session_target_duration_minutes": resolved_duration_minutes,
                "session_soft_floor_met": 1 if total_follow_attempted >= resolved_min_follows else 0,
                "session_soft_floor_remaining": max(0, resolved_min_follows - total_follow_attempted),
                "session_pacing_degrade_events": pacing_degrade_events,
                "session_mutation_suspended_seconds_total": round(suspended_seconds_total, 3),
            }
        )
        kpis.update(self.timing.metrics_snapshot())

        aggregated_outcome = DailyRunOutcome(
            budget_exhausted=last_outcome.budget_exhausted,
            actions_today=last_outcome.actions_today,
            max_actions_per_day=last_outcome.max_actions_per_day,
            cleanup_only_mode=False,
            growth_policy=resolved_growth_policy,
            growth_sources=resolved_growth_sources,
            growth_mode=resolved_growth_mode,
            discovery_mode=resolved_discovery_mode,
            target_user_refs=resolved_target_user_refs,
            target_user_scan_limit=resolved_target_user_scan_limit,
            action_counts_today=last_outcome.action_counts_today,
            action_limits_per_day=last_outcome.action_limits_per_day,
            action_remaining_per_day=last_outcome.action_remaining_per_day,
            dry_run=False,
            discovered_candidates=total_discovered,
            screened_candidates=total_screened,
            executed_candidates=total_executed,
            followed_candidates=total_followed,
            considered_candidates=total_considered,
            eligible_candidates=total_eligible,
            follow_actions_attempted=total_follow_attempted,
            follow_actions_verified=total_follow_verified,
            clap_actions_attempted=total_clap_attempted,
            clap_actions_verified=total_clap_verified,
            public_touch_actions_attempted=total_public_touch_attempted,
            public_touch_actions_verified=total_public_touch_verified,
            comment_actions_attempted=total_comment_attempted,
            comment_actions_verified=total_comment_verified,
            highlight_actions_attempted=total_highlight_attempted,
            highlight_actions_verified=total_highlight_verified,
            cleanup_actions_attempted=total_cleanup_attempted,
            cleanup_actions_verified=total_cleanup_verified,
            source_candidate_counts=total_source_candidate_counts,
            source_follow_verified_counts=total_source_follow_verified_counts,
            policy_follow_verified_counts={resolved_growth_policy.value: total_follow_verified},
            conversion_by_source=conversion_by_source,
            conversion_by_policy=conversion_by_policy,
            conversion_by_source_policy=conversion_by_source_policy,
            kpis=kpis,
            client_metrics=self.client.metrics_snapshot(),
            decision_log=decision_log_sample,
            decision_reason_counts=total_reason_counts,
            decision_result_counts=total_result_counts,
            probe=last_outcome.probe,
            session_passes=pass_count,
            session_elapsed_seconds=elapsed_total,
            session_stop_reason=stop_reason,
            session_target_follow_attempts=resolved_target_follows,
            session_target_duration_minutes=resolved_duration_minutes,
        )

        self.log.info(
            "live_session_complete",
            passes=pass_count,
            elapsed_seconds=elapsed_total,
            stop_reason=stop_reason,
            discovered_candidates=total_discovered,
            screened_candidates=total_screened,
            executed_candidates=total_executed,
            followed_candidates=total_followed,
            follow_attempted=total_follow_attempted,
            follow_verified=total_follow_verified,
            public_touch_attempted=total_public_touch_attempted,
            public_touch_verified=total_public_touch_verified,
            comment_attempted=total_comment_attempted,
            comment_verified=total_comment_verified,
            highlight_attempted=total_highlight_attempted,
            highlight_verified=total_highlight_verified,
            follow_target_min=resolved_min_follows,
            growth_policy=resolved_growth_policy.value,
            growth_sources=[source.value for source in resolved_growth_sources],
            growth_mode=resolved_growth_mode.value,
            discovery_mode=resolved_discovery_mode.value,
            target_user_refs=resolved_target_user_refs,
            target_user_scan_limit=resolved_target_user_scan_limit,
            cleanup_attempted=total_cleanup_attempted,
            cleanup_verified=total_cleanup_verified,
            session_pacing_degrade_events=pacing_degrade_events,
            action_counts=aggregated_outcome.action_counts_today,
            action_limits=aggregated_outcome.action_limits_per_day,
            action_remaining=aggregated_outcome.action_remaining_per_day,
        )
        return aggregated_outcome

    async def run_cleanup_only(
        self,
        *,
        dry_run: bool = True,
        max_unfollows: int | None = None,
    ) -> DailyRunOutcome:
        self._assert_operator_not_stopped(task_name="run_cleanup_only")
        await self._maybe_recover_stealth_preflight_challenge(dry_run=dry_run)
        self.timing.reset_session_state()
        self.timing.reset_metrics()
        self.timing.set_simulation_mode(dry_run)

        actions_today_start = self.repository.actions_today_utc(TRACKED_DAILY_ACTION_TYPES)
        max_actions = self.settings.max_actions_per_day
        action_counts = self.repository.action_counts_today_utc(TRACKED_DAILY_ACTION_TYPES)
        action_limits = {
            ACTION_SUBSCRIBE: self.settings.max_subscribe_actions_per_day,
            ACTION_UNFOLLOW: self.settings.max_unfollow_actions_per_day,
            ACTION_CLAP: self.settings.max_clap_actions_per_day,
            ACTION_COMMENT: self.settings.max_comment_actions_per_day,
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
                cleanup_only_mode=True,
                growth_mode=None,
                action_counts_today=action_counts,
                action_limits_per_day=action_limits,
                action_remaining_per_day=action_remaining,
                dry_run=dry_run,
                probe=None,
                client_metrics=self.client.metrics_snapshot(),
            )

        decisions: list[CandidateDecision] = []
        remaining_budget = max(0, max_actions - actions_today_start)
        cleanup_limit = max(0, max_unfollows) if max_unfollows is not None else self.settings.cleanup_unfollow_limit
        cleanup_cap = min(
            cleanup_limit,
            remaining_budget,
            action_remaining[ACTION_UNFOLLOW],
        )
        mutations_enabled = self._mutations_enabled_for_cycle(dry_run=dry_run)
        if not mutations_enabled and not dry_run:
            cleanup_cap = 0

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

        kpis = self._build_kpis(
            follow_attempted=0,
            follow_verified=0,
            cleanup_attempted=cleanup_attempted,
            cleanup_verified=cleanup_verified,
            eligible_candidates=0,
            clap_attempted=0,
            clap_verified=0,
            comment_attempted=0,
            comment_verified=0,
        )
        kpis.update(self.repository.follow_cycle_kpis())
        kpis.update(self.timing.metrics_snapshot())
        kpis["pacing_mutations_enabled"] = 1 if mutations_enabled else 0
        client_metrics = self.client.metrics_snapshot()

        self.log.info(
            "cleanup_only_complete",
            dry_run=dry_run,
            cleanup_attempted=cleanup_attempted,
            cleanup_verified=cleanup_verified,
            actions_today=actions_today_end,
            max_actions=max_actions,
            action_counts=action_counts,
            action_limits=action_limits,
            action_remaining=action_remaining,
            mutations_enabled=mutations_enabled,
            decision_reason_counts=decision_reason_counts,
            decision_result_counts=decision_result_counts,
            kpis=kpis,
            client_metrics=client_metrics,
            day_boundary_policy=self.settings.day_boundary_policy,
        )
        return DailyRunOutcome(
            budget_exhausted=False,
            actions_today=actions_today_end,
            max_actions_per_day=max_actions,
            cleanup_only_mode=True,
            growth_mode=None,
            action_counts_today=action_counts,
            action_limits_per_day=action_limits,
            action_remaining_per_day=action_remaining,
            dry_run=dry_run,
            cleanup_actions_attempted=cleanup_attempted,
            cleanup_actions_verified=cleanup_verified,
            kpis=kpis,
            client_metrics=client_metrics,
            decision_log=[
                f"{item.reason} (id={item.user_id})"
                for item in decisions[:80]
            ],
            decision_reason_counts=decision_reason_counts,
            decision_result_counts=decision_result_counts,
            probe=None,
        )

    async def reconcile_follow_states(
        self,
        *,
        dry_run: bool,
        max_users: int,
        page_size: int,
    ) -> ReconcileOutcome:
        self._assert_operator_not_stopped(task_name="reconcile_follow_states")
        await self._maybe_recover_stealth_preflight_challenge(dry_run=dry_run)
        self.timing.reset_session_state()
        self.timing.reset_metrics()
        self.timing.set_simulation_mode(dry_run)
        scanned = 0
        updated = 0
        following_count = 0
        not_following_count = 0
        unknown_count = 0
        decision_log: list[str] = []
        rows = self._collect_reconciliation_worklist(max_users=max_users, page_size=page_size)

        for row in rows:
            self._assert_operator_not_stopped(task_name="reconcile_follow_states")
            user_id = row.get("user_id")
            if not isinstance(user_id, str) or not user_id:
                continue
            scanned += 1
            username = row.get("username")

            result = await self._execute_with_retry(
                "reconcile_user_viewer_edge",
                operations.user_viewer_edge(user_id),
            )
            is_following = parse_user_viewer_is_following(result)
            if is_following is True:
                following_count += 1
                decision_log.append(f"reconcile:following id={user_id}")
                if not dry_run:
                    self.repository.upsert_relationship_state(
                        user_id,
                        newsletter_state=NewsletterState.UNKNOWN,
                        user_follow_state=UserFollowState.FOLLOWING,
                        confidence=RelationshipConfidence.OBSERVED,
                        source_operation="UserViewerEdge",
                        verified_now=True,
                    )
                    self.repository.mark_candidate_reconciled(user_id, UserFollowState.FOLLOWING)
                    updated += 1
                continue

            if is_following is False:
                not_following_count += 1
                decision_log.append(f"reconcile:not_following id={user_id}")
                if not dry_run:
                    self.repository.upsert_relationship_state(
                        user_id,
                        newsletter_state=NewsletterState.UNKNOWN,
                        user_follow_state=UserFollowState.NOT_FOLLOWING,
                        confidence=RelationshipConfidence.OBSERVED,
                        source_operation="UserViewerEdge",
                        verified_now=True,
                    )
                    self.repository.mark_candidate_reconciled(user_id, UserFollowState.NOT_FOLLOWING)
                    updated += 1
                continue

            unknown_count += 1
            decision_log.append(f"reconcile:unknown id={user_id}")

        self.log.info(
            "reconcile_complete",
            dry_run=dry_run,
            scanned_users=scanned,
            updated_users=updated,
            following_count=following_count,
            not_following_count=not_following_count,
            unknown_count=unknown_count,
        )
        return ReconcileOutcome(
            dry_run=dry_run,
            scanned_users=scanned,
            updated_users=updated,
            following_count=following_count,
            not_following_count=not_following_count,
            unknown_count=unknown_count,
            decision_log=decision_log,
        )

    def _collect_reconciliation_worklist(
        self,
        *,
        max_users: int,
        page_size: int,
    ) -> list[dict[str, str | None]]:
        collected: list[dict[str, str | None]] = []
        seen_ids: set[str] = set()
        offset = 0
        effective_page_size = max(1, page_size)

        while len(collected) < max_users:
            remaining = max_users - len(collected)
            page_limit = min(effective_page_size, remaining)
            rows = self.repository.reconciliation_candidates_page(limit=page_limit, offset=offset)
            if not rows:
                break

            for row in rows:
                user_id = row.get("user_id")
                if not isinstance(user_id, str) or not user_id or user_id in seen_ids:
                    continue
                seen_ids.add(user_id)
                collected.append(row)
                if len(collected) >= max_users:
                    break

            offset += len(rows)
            if len(rows) < page_limit:
                break

        return collected

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
            if not self._is_mutation_task(task_name):
                await self._sleep_verify_gap(task_name=task_name, target_id=target_id)
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

    @staticmethod
    def _is_mutation_task(task_name: str) -> bool:
        lowered = task_name.lower()
        mutation_tokens = ("mutation", "subscribe", "unfollow", "clap", "comment", "highlight", "quote")
        return any(token in lowered for token in mutation_tokens)

    def _retry_budget_for_task(self, task_name: str) -> int:
        lowered = task_name.lower()
        if any(token in lowered for token in ("mutation", "subscribe", "unfollow", "clap", "comment", "highlight", "quote")):
            return self.settings.mutation_max_retries
        if any(token in lowered for token in ("verify", "viewer_edge", "reconcile")):
            return self.settings.verify_max_retries
        return self.settings.query_max_retries

    def _retry_delay_seconds(self, attempt: int) -> float:
        base = self.settings.retry_base_delay_seconds
        if base <= 0:
            return 0.0
        raw = min(self.settings.retry_max_delay_seconds, base * (2**attempt))
        jitter = random.uniform(0.0, base)
        adaptive_multiplier = 1.0 + (
            self.settings.adaptive_retry_failure_multiplier * self.risk_guard.consecutive_failures
        )
        adjusted = (raw + jitter) * adaptive_multiplier
        return min(self.settings.retry_max_delay_seconds, adjusted)

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
            "targetPostId",
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
        tag_slug: str,
        probe: ProbeSnapshot | None,
        seed_user_refs: list[str],
        growth_sources: list[GrowthSource],
        target_user_refs: list[str],
        target_user_scan_limit: int | None,
        candidate_scan_limit: int,
    ) -> list[CandidateUser]:
        pool: dict[str, CandidateUser] = {}

        if GrowthSource.TOPIC_RECOMMENDED in growth_sources:
            if probe is not None:
                self._extract_topic_latest_candidates(probe, pool)
                self._extract_topic_who_to_follow_candidates(probe, pool)
                self._extract_who_to_follow_module_candidates(probe, pool)
        if GrowthSource.SEED_FOLLOWERS in growth_sources:
            await self._extract_seed_followers_candidates(
                seed_user_refs,
                pool,
                scan_limit=candidate_scan_limit,
            )
        if GrowthSource.TARGET_USER_FOLLOWERS in growth_sources:
            await self._extract_target_user_followers_candidates(
                target_user_refs=target_user_refs,
                pool=pool,
                per_target_scan_limit=max(
                    self._resolve_target_user_scan_limit(target_user_scan_limit),
                    candidate_scan_limit,
                ),
            )
        if GrowthSource.PUBLICATION_ADJACENT in growth_sources:
            await self._extract_topic_curated_candidates(tag_slug=tag_slug, pool=pool)
        if GrowthSource.RESPONDERS in growth_sources:
            await self._extract_topic_responder_candidates(tag_slug=tag_slug, probe=probe, pool=pool)

        for candidate in pool.values():
            self._refresh_candidate_scoring(candidate)

        ordered = sorted(pool.values(), key=lambda item: item.score, reverse=True)
        self.log.info(
            "candidates_built",
            count=len(ordered),
            growth_sources=[source.value for source in growth_sources],
            seed_sources=len(seed_user_refs),
            target_sources=len(target_user_refs),
            target_user_scan_limit=target_user_scan_limit,
            candidate_scan_limit=candidate_scan_limit,
        )
        return ordered

    def _extract_topic_latest_candidates(self, probe: ProbeSnapshot, pool: dict[str, CandidateUser]) -> None:
        result = probe.results.get("topic_latest_stories")
        if not result:
            return
        for creator, latest_post_id, latest_post_title in parse_topic_latest_story_creators(result):
            candidate = self._candidate_from_user_node(
                creator,
                source=CandidateSource.TOPIC_LATEST_STORIES,
                latest_post_id=latest_post_id,
                latest_post_title=latest_post_title,
            )
            if candidate:
                self._merge_candidate(pool, candidate)

    def _extract_topic_who_to_follow_candidates(self, probe: ProbeSnapshot, pool: dict[str, CandidateUser]) -> None:
        result = probe.results.get("topic_who_to_follow")
        if not result:
            return
        for user_node in parse_recommended_publishers_users(result):
            candidate = self._candidate_from_user_node(user_node, source=CandidateSource.TOPIC_WHO_TO_FOLLOW)
            if candidate:
                self._merge_candidate(pool, candidate)

    def _extract_who_to_follow_module_candidates(self, probe: ProbeSnapshot, pool: dict[str, CandidateUser]) -> None:
        result = probe.results.get("who_to_follow_module")
        if not result:
            return
        for user_node in parse_recommended_publishers_users(result):
            candidate = self._candidate_from_user_node(user_node, source=CandidateSource.WHO_TO_FOLLOW_MODULE)
            if candidate:
                self._merge_candidate(pool, candidate)

    async def _extract_seed_followers_candidates(
        self,
        seed_user_refs: list[str],
        pool: dict[str, CandidateUser],
        *,
        scan_limit: int,
    ) -> None:
        per_seed_limit = max(self.settings.discovery_seed_followers_limit, scan_limit)
        for seed_ref in seed_user_refs:
            first_hop_users = await self._fetch_user_followers_nodes(
                user_ref=seed_ref,
                limit=per_seed_limit,
                task_name="seed_user_followers",
            )
            for node in first_hop_users:
                candidate = self._candidate_from_user_node(node, source=CandidateSource.SEED_FOLLOWERS)
                if candidate:
                    self._merge_candidate(pool, candidate)

            if self.settings.discovery_followers_depth < 2:
                continue

            second_hop_roots = [item.id for item in first_hop_users if isinstance(item.id, str)][: self.settings.discovery_second_hop_seed_limit]
            for root_id in second_hop_roots:
                hop_result = await self._execute_with_retry(
                    "seed_user_followers_second_hop",
                    operations.user_followers(
                        user_id=root_id,
                        limit=self.settings.discovery_seed_followers_limit,
                    ),
                )
                for node in parse_user_followers_users(hop_result):
                    candidate = self._candidate_from_user_node(node, source=CandidateSource.SEED_FOLLOWERS)
                    if candidate:
                        self._merge_candidate(pool, candidate)

    async def _extract_topic_curated_candidates(
        self,
        *,
        tag_slug: str,
        pool: dict[str, CandidateUser],
    ) -> None:
        result = await self._execute_with_retry(
            "topic_curated_list",
            operations.topic_curated_list(
                tag_slug,
                item_limit=self.settings.topic_curated_list_item_limit,
            ),
        )
        for creator, latest_post_id, latest_post_title in parse_topic_curated_list_users(result):
            candidate = self._candidate_from_user_node(
                creator,
                source=CandidateSource.TOPIC_CURATED_LIST,
                latest_post_id=latest_post_id,
                latest_post_title=latest_post_title,
            )
            if candidate:
                self._merge_candidate(pool, candidate)

    async def _extract_topic_responder_candidates(
        self,
        *,
        tag_slug: str,
        probe: ProbeSnapshot | None,
        pool: dict[str, CandidateUser],
    ) -> None:
        post_ids: list[str] = []
        if probe is not None:
            result = probe.results.get("topic_latest_stories")
            if result is not None:
                post_ids = [
                    post_id
                    for _, post_id, _ in parse_topic_latest_story_creators(result)
                    if isinstance(post_id, str) and post_id
                ]
        if not post_ids:
            fallback = await self._execute_with_retry(
                "responder_topic_latest",
                operations.topic_latest_stories(tag_slug),
            )
            post_ids = [
                post_id
                for _, post_id, _ in parse_topic_latest_story_creators(fallback)
                if isinstance(post_id, str) and post_id
            ]

        for post_id in post_ids[: self.settings.responder_posts_per_run]:
            result = await self._execute_with_retry(
                "post_responses",
                operations.post_responses(
                    post_id=post_id,
                    limit=self.settings.responder_candidates_per_post,
                ),
            )
            for creator in parse_post_response_creators(result):
                candidate = self._candidate_from_user_node(creator, source=CandidateSource.POST_RESPONDERS)
                if candidate:
                    self._merge_candidate(pool, candidate)

    async def _extract_target_user_followers_candidates(
        self,
        *,
        target_user_refs: list[str],
        pool: dict[str, CandidateUser],
        per_target_scan_limit: int,
    ) -> None:
        for target_ref in target_user_refs:
            for node in await self._fetch_user_followers_nodes(
                user_ref=target_ref,
                limit=per_target_scan_limit,
                task_name="target_user_followers",
            ):
                candidate = self._candidate_from_user_node(node, source=CandidateSource.TARGET_USER_FOLLOWERS)
                if candidate:
                    self._merge_candidate(pool, candidate)

    async def _fetch_user_followers_nodes(
        self,
        *,
        user_ref: str,
        limit: int,
        task_name: str,
    ) -> list[UserNode]:
        user_id, username = self._parse_user_ref(user_ref)
        if not user_id and not username:
            return []

        remaining = max(1, limit)
        cursor: str | None = None
        seen_cursors: set[str] = set()
        collected: dict[str, UserNode] = {}

        while remaining > 0:
            page_limit = min(operations.USER_FOLLOWERS_MAX_LIMIT, remaining)
            result = await self._execute_with_retry(
                task_name,
                operations.user_followers(
                    user_id=user_id,
                    username=username,
                    limit=page_limit,
                    paging_from=cursor,
                ),
            )
            for node in parse_user_followers_users(result):
                if node.id not in collected:
                    collected[node.id] = node
            remaining = max(0, limit - len(collected))
            if remaining <= 0:
                break
            next_cursor = parse_user_followers_next_from(result)
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return list(collected.values())[:limit]

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
        node: UserNode,
        *,
        source: CandidateSource,
        latest_post_id: str | None = None,
        latest_post_title: str | None = None,
    ) -> CandidateUser | None:
        user_id = node.id
        if not user_id:
            return None
        follower_count = node.social_stats.follower_count if node.social_stats else None
        following_count = node.social_stats.following_count if node.social_stats else None
        newsletter_v3_id = node.newsletter_v3.id if node.newsletter_v3 else None
        return CandidateUser(
            user_id=user_id,
            username=node.username,
            name=node.name,
            bio=node.bio,
            newsletter_v3_id=newsletter_v3_id,
            follower_count=follower_count,
            following_count=following_count,
            latest_post_id=latest_post_id,
            latest_post_title=latest_post_title,
            sources=[source],
        )

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
        if not existing.latest_post_title and candidate.latest_post_title:
            existing.latest_post_title = candidate.latest_post_title
        if not existing.last_post_created_at and candidate.last_post_created_at:
            existing.last_post_created_at = candidate.last_post_created_at
        for source in candidate.sources:
            if source not in existing.sources:
                existing.sources.append(source)

    def _screen_discovery_candidates(
        self,
        candidates: list[CandidateUser],
        *,
        decisions: list[CandidateDecision],
        persist_observations: bool,
        existing_growth_candidate_ids: set[str],
    ) -> list[CandidateUser]:
        buffered: list[CandidateUser] = []
        for candidate in candidates:
            if candidate.user_id in existing_growth_candidate_ids:
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:already_in_growth_queue",
                    needs_reconcile=False,
                )
                continue

            if self.repository.is_blacklisted(candidate.user_id):
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:blacklisted",
                )
                continue

            if self.repository.has_recent_action(
                candidate.user_id,
                within_hours=self.settings.follow_cooldown_hours,
                action_types=FOLLOW_COOLDOWN_ACTION_TYPES,
            ):
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:cooldown_active",
                )
                continue

            local_state = self.repository.get_relationship_state(candidate.user_id)
            if local_state and local_state.user_follow_state == UserFollowState.FOLLOWING:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:already_following_local_state",
                    needs_reconcile=False,
                )
                continue

            if candidate.follower_count is not None and candidate.following_count is not None:
                ratio = self._following_follower_ratio(candidate)
                if ratio < self.settings.min_following_follower_ratio:
                    if persist_observations:
                        self.repository.remove_growth_candidate(candidate.user_id)
                    self._append_decision(
                        decisions,
                        candidate,
                        eligible=False,
                        reason=f"skip:ratio_below_threshold ratio={ratio:.2f}",
                    )
                    continue
                if ratio > self.settings.max_following_follower_ratio:
                    if persist_observations:
                        self.repository.remove_growth_candidate(candidate.user_id)
                    self._append_decision(
                        decisions,
                        candidate,
                        eligible=False,
                        reason=f"skip:ratio_above_threshold ratio={ratio:.2f}",
                    )
                    continue

            volume_filter_reason = self._candidate_volume_filter_reason(candidate)
            if volume_filter_reason:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason=volume_filter_reason,
                )
                continue

            self._refresh_candidate_scoring(candidate)

            negative_filter_reason = self._candidate_negative_filter_reason(candidate)
            if negative_filter_reason:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason=negative_filter_reason,
                )
                continue

            if (
                self.settings.candidate_recent_activity_days > 0
                and candidate.last_post_created_at
                and not self._candidate_has_recent_activity(candidate)
            ):
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:inactive_author",
                )
                continue

            buffered.append(candidate)
            self._append_decision(
                decisions,
                candidate,
                eligible=True,
                reason="discovery:locally_screened",
            )

        return buffered

    async def _evaluate_discovery_candidates(
        self,
        candidates: list[CandidateUser],
        *,
        decisions: list[CandidateDecision],
        persist_observations: bool,
        max_eligible: int | None = None,
    ) -> list[CandidateUser]:
        eligible: list[CandidateUser] = []
        for candidate in candidates:
            if max_eligible is not None and len(eligible) >= max_eligible:
                break

            if self.repository.is_blacklisted(candidate.user_id):
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:blacklisted",
                )
                continue

            cooldown_retry_after = self.repository.recent_action_retry_after(
                candidate.user_id,
                within_hours=self.settings.follow_cooldown_hours,
                action_types=FOLLOW_COOLDOWN_ACTION_TYPES,
            )
            if cooldown_retry_after is not None:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:cooldown_active",
                )
                continue

            local_state = self.repository.get_relationship_state(candidate.user_id)
            if local_state and local_state.user_follow_state == UserFollowState.FOLLOWING:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:already_following_local_state",
                    needs_reconcile=False,
                )
                continue

            edge_result = await self._execute_with_retry(
                "candidate_user_viewer_edge",
                operations.user_viewer_edge(candidate.user_id),
            )
            self._hydrate_candidate_from_user_viewer_edge(candidate, edge_result)
            self._refresh_candidate_scoring(candidate)
            is_following = parse_user_viewer_is_following(edge_result)
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
                    self.repository.mark_candidate_reconciled(candidate.user_id, UserFollowState.FOLLOWING)
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:already_following_live_check",
                    needs_reconcile=False,
                )
                continue
            if is_following is None:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:live_check_unavailable",
                )
                continue

            if persist_observations:
                self.repository.upsert_relationship_state(
                    candidate.user_id,
                    newsletter_state=NewsletterState.UNKNOWN,
                    user_follow_state=UserFollowState.NOT_FOLLOWING,
                    confidence=RelationshipConfidence.OBSERVED,
                    source_operation="UserViewerEdge",
                    verified_now=True,
                )
                self.repository.mark_candidate_reconciled(candidate.user_id, UserFollowState.NOT_FOLLOWING)

            ratio = self._following_follower_ratio(candidate)
            if ratio < self.settings.min_following_follower_ratio:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason=f"skip:ratio_below_threshold ratio={ratio:.2f}",
                )
                continue
            if ratio > self.settings.max_following_follower_ratio:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason=f"skip:ratio_above_threshold ratio={ratio:.2f}",
                )
                continue

            volume_filter_reason = self._candidate_volume_filter_reason(candidate)
            if volume_filter_reason:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason=volume_filter_reason,
                )
                continue

            if self.settings.require_candidate_bio and not (candidate.bio or "").strip():
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:no_bio",
                )
                continue

            self._refresh_candidate_scoring(candidate)
            negative_filter_reason = self._candidate_negative_filter_reason(candidate)
            if negative_filter_reason:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason=negative_filter_reason,
                )
                continue

            if self.settings.require_bio_keyword_match and not candidate.matched_keywords:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:no_keyword_match",
                )
                continue

            if not candidate.newsletter_v3_id:
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:no_newsletter_v3_id",
                )
                continue

            if self.settings.require_candidate_latest_post:
                post_id = await self._resolve_candidate_latest_post_id(candidate)
                self._refresh_candidate_scoring(candidate)
                if not post_id:
                    if persist_observations:
                        self.repository.remove_growth_candidate(candidate.user_id)
                    self._append_decision(
                        decisions,
                        candidate,
                        eligible=False,
                        reason="skip:no_latest_post",
                    )
                    continue

            if not self._candidate_has_recent_activity(candidate):
                if persist_observations:
                    self.repository.remove_growth_candidate(candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:inactive_author",
                )
                continue

            eligible.append(candidate)
            self._append_decision(
                decisions,
                candidate,
                eligible=True,
                reason="eligible:execution_ready",
            )

        return eligible

    async def _execute_follow_pipeline(
        self,
        *,
        eligible_candidates: list[CandidateUser],
        max_to_run: int,
        clap_budget_remaining: int,
        comment_budget_remaining: int,
        dry_run: bool,
        decisions: list[CandidateDecision],
        growth_policy: GrowthPolicy,
    ) -> tuple[int, int, int, int, int, int, int, int, int, int, dict[str, int]]:
        attempted = 0
        verified = 0
        clap_attempted = 0
        clap_verified = 0
        public_touch_attempted = 0
        public_touch_verified = 0
        comment_attempted = 0
        comment_verified = 0
        highlight_attempted = 0
        highlight_verified = 0
        source_follow_verified_counts: dict[str, int] = {}
        clap_enabled = self._pre_follow_clap_enabled(growth_policy)
        public_touch_enabled = self._pre_follow_public_touch_enabled(growth_policy)
        for candidate in eligible_candidates[:max_to_run]:
            self._assert_operator_not_stopped(task_name="follow_pipeline")

            if dry_run:
                attempted += 1
                should_resolve_post_context = (
                    (clap_enabled and clap_budget_remaining > 0)
                    or (public_touch_enabled and comment_budget_remaining > 0)
                )
                post_context = (
                    await self._resolve_candidate_post_context(
                        candidate,
                        include_extended_context=public_touch_enabled and comment_budget_remaining > 0,
                    )
                    if should_resolve_post_context
                    else None
                )
                if clap_enabled and clap_budget_remaining > 0 and post_context is not None:
                    clap_budget_remaining -= 1
                    clap_attempted += 1
                    await self._sleep_action_gap(action_type=ACTION_CLAP, target_user_id=candidate.user_id)
                if public_touch_enabled and comment_budget_remaining > 0:
                    touch_plan = (
                        self._plan_pre_follow_public_touch(
                            candidate,
                            growth_policy=growth_policy,
                            public_touch_budget_remaining=comment_budget_remaining,
                            post_context=post_context,
                            dry_run=True,
                        )
                        if post_context is not None
                        else None
                    )
                else:
                    touch_plan = None
                if touch_plan is not None:
                    comment_budget_remaining -= 1
                    public_touch_attempted += 1
                    if touch_plan.touch_type == ACTION_COMMENT:
                        comment_attempted += 1
                    elif touch_plan.touch_type == ACTION_HIGHLIGHT:
                        highlight_attempted += 1
                    await self._sleep_action_gap(action_type=touch_plan.touch_type, target_user_id=candidate.user_id)
                await self._sleep_action_gap(action_type=ACTION_SUBSCRIBE, target_user_id=candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=True,
                    reason="dry_run:planned_follow",
                )
                continue

            can_execute_mutation = await self._pre_mutation_follow_state_guard(candidate, decisions=decisions)
            if not can_execute_mutation:
                continue
            attempted += 1

            self.repository.upsert_user_profile(
                candidate.user_id,
                username=candidate.username,
                name=candidate.name,
                follower_count=candidate.follower_count,
                following_count=candidate.following_count,
                newsletter_id=candidate.newsletter_v3_id,
                bio=candidate.bio,
            )
            self.repository.mark_growth_candidate_queue_state(
                candidate.user_id,
                queue_state="deferred",
                reason="follow_attempt:started",
                candidate=candidate,
                observed_follow_state=UserFollowState.NOT_FOLLOWING,
                attempted=True,
                retry_after_at=self._growth_queue_deferred_retry_at(
                    user_id=candidate.user_id,
                    reason="follow_attempt:started",
                ),
            )

            (
                clap_used,
                clap_is_verified,
                public_touch_used,
                public_touch_is_verified,
                touch_type,
            ) = await self._execute_pre_follow_engagement(
                candidate,
                growth_policy=growth_policy,
                clap_budget_remaining=clap_budget_remaining,
                comment_budget_remaining=comment_budget_remaining,
            )
            if clap_used:
                clap_budget_remaining -= 1
                clap_attempted += 1
            if clap_is_verified:
                clap_verified += 1
            if public_touch_used:
                comment_budget_remaining -= 1
                public_touch_attempted += 1
                if touch_type == ACTION_COMMENT:
                    comment_attempted += 1
                elif touch_type == ACTION_HIGHLIGHT:
                    highlight_attempted += 1
            if public_touch_is_verified:
                public_touch_verified += 1
                if touch_type == ACTION_COMMENT:
                    comment_verified += 1
                elif touch_type == ACTION_HIGHLIGHT:
                    highlight_verified += 1

            await self._sleep_action_gap(action_type=ACTION_SUBSCRIBE, target_user_id=candidate.user_id)
            mutation = await self._execute_with_retry(
                "follow_subscribe_mutation",
                operations.subscribe_newsletter_v3(candidate.newsletter_v3_id),
            )
            mutation_ok = mutation.status_code == 200 and not mutation.has_errors
            subscribe_action_key = self._daily_action_key(ACTION_SUBSCRIBE, candidate.user_id)
            self.repository.record_action(
                ACTION_SUBSCRIBE,
                candidate.user_id,
                "ok" if mutation_ok else "failed",
                action_key=subscribe_action_key,
            )
            if not mutation_ok:
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=True,
                    reason="follow_failed:mutation_error",
                )
                self.repository.mark_growth_candidate_queue_state(
                    candidate.user_id,
                    queue_state="deferred",
                    reason="follow_failed:mutation_error",
                    candidate=candidate,
                    observed_follow_state=UserFollowState.NOT_FOLLOWING,
                    retry_after_at=self._growth_queue_deferred_retry_at(
                        user_id=candidate.user_id,
                        reason="follow_failed:mutation_error",
                    ),
                )
                continue

            verify = await self._execute_with_retry(
                "follow_verify_user_viewer_edge",
                operations.user_viewer_edge(candidate.user_id),
            )
            is_following = parse_user_viewer_is_following(verify)
            follow_verify_key = self._daily_action_key(ACTION_FOLLOW_VERIFIED, candidate.user_id)
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
                    source=self._primary_growth_source_value(candidate),
                    grace_days=self.settings.unfollow_nonreciprocal_after_days,
                    growth_policy=growth_policy.value,
                    growth_sources=self._candidate_growth_source_values(candidate),
                    score_breakdown=candidate.score_breakdown,
                )
                self.repository.mark_candidate_reconciled(candidate.user_id, UserFollowState.FOLLOWING)
                self.repository.record_action(
                    ACTION_FOLLOW_VERIFIED,
                    candidate.user_id,
                    "verified_following",
                    action_key=follow_verify_key,
                )
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=True,
                    reason="follow_success:verified_following",
                    needs_reconcile=False,
                )
                self.repository.remove_growth_candidate(candidate.user_id)
                for key in self._candidate_growth_source_values(candidate):
                    source_follow_verified_counts[key] = source_follow_verified_counts.get(key, 0) + 1
            else:
                self.repository.upsert_relationship_state(
                    candidate.user_id,
                    newsletter_state=NewsletterState.SUBSCRIBED,
                    user_follow_state=UserFollowState.NOT_FOLLOWING if is_following is False else UserFollowState.UNKNOWN,
                    confidence=RelationshipConfidence.OBSERVED,
                    source_operation="UserViewerEdge",
                    verified_now=is_following is not None,
                )
                if is_following is False:
                    self.repository.mark_candidate_reconciled(candidate.user_id, UserFollowState.NOT_FOLLOWING)
                self.repository.record_action(
                    ACTION_FOLLOW_VERIFIED,
                    candidate.user_id,
                    "verification_failed",
                    action_key=follow_verify_key,
                )
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=True,
                    reason="follow_failed:verification_failed",
                )
                self.repository.mark_growth_candidate_queue_state(
                    candidate.user_id,
                    queue_state="deferred",
                    reason="follow_failed:verification_failed",
                    candidate=candidate,
                    observed_follow_state=UserFollowState.NOT_FOLLOWING if is_following is False else None,
                    retry_after_at=self._growth_queue_deferred_retry_at(
                        user_id=candidate.user_id,
                        reason="follow_failed:verification_failed",
                    ),
                )

        return (
            attempted,
            verified,
            clap_attempted,
            clap_verified,
            public_touch_attempted,
            public_touch_verified,
            comment_attempted,
            comment_verified,
            highlight_attempted,
            highlight_verified,
            source_follow_verified_counts,
        )

    async def _pre_mutation_follow_state_guard(
        self,
        candidate: CandidateUser,
        *,
        decisions: list[CandidateDecision],
    ) -> bool:
        verify = await self._execute_with_retry(
            "follow_pre_mutation_user_viewer_edge",
            operations.user_viewer_edge(candidate.user_id),
        )
        self._hydrate_candidate_from_user_viewer_edge(candidate, verify)
        is_following = parse_user_viewer_is_following(verify)
        if is_following is True:
            self.repository.upsert_relationship_state(
                candidate.user_id,
                newsletter_state=NewsletterState.UNKNOWN,
                user_follow_state=UserFollowState.FOLLOWING,
                confidence=RelationshipConfidence.OBSERVED,
                source_operation="UserViewerEdge",
                verified_now=True,
            )
            self.repository.mark_candidate_reconciled(candidate.user_id, UserFollowState.FOLLOWING)
            self.repository.remove_growth_candidate(candidate.user_id)
            self._append_decision(
                decisions,
                candidate,
                eligible=False,
                reason="skip:already_following_pre_mutation_check",
                needs_reconcile=False,
            )
            return False
        if is_following is None:
            self.repository.mark_growth_candidate_queue_state(
                candidate.user_id,
                queue_state="deferred",
                reason="skip:pre_mutation_check_unavailable",
                candidate=candidate,
                retry_after_at=self._growth_queue_deferred_retry_at(
                    user_id=candidate.user_id,
                    reason="skip:live_check_unavailable",
                ),
            )
            self._append_decision(
                decisions,
                candidate,
                eligible=False,
                reason="skip:pre_mutation_check_unavailable",
            )
            return False
        self.repository.upsert_relationship_state(
            candidate.user_id,
            newsletter_state=NewsletterState.UNKNOWN,
            user_follow_state=UserFollowState.NOT_FOLLOWING,
            confidence=RelationshipConfidence.OBSERVED,
            source_operation="UserViewerEdge",
            verified_now=True,
        )
        self.repository.mark_candidate_reconciled(candidate.user_id, UserFollowState.NOT_FOLLOWING)
        return True

    async def _execute_pre_follow_engagement(
        self,
        candidate: CandidateUser,
        *,
        growth_policy: GrowthPolicy,
        clap_budget_remaining: int,
        comment_budget_remaining: int,
    ) -> tuple[bool, bool, bool, bool, str | None]:
        clap_enabled = self._pre_follow_clap_enabled(growth_policy)
        public_touch_enabled = self._pre_follow_public_touch_enabled(growth_policy)
        if not clap_enabled and not public_touch_enabled:
            return False, False, False, False, None

        clap_should_attempt = False
        touch_should_plan = False

        if clap_enabled:
            if clap_budget_remaining <= 0:
                self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "budget_exhausted")
            elif not self.settings.medium_user_ref:
                self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "missing_actor_user_id")
            else:
                clap_should_attempt = True

        if public_touch_enabled:
            if comment_budget_remaining <= 0:
                self.repository.record_action(ACTION_PUBLIC_TOUCH_SKIPPED, candidate.user_id, "budget_exhausted")
            else:
                touch_should_plan = True

        if not clap_should_attempt and not touch_should_plan:
            return False, False, False, False, None

        post_context = await self._resolve_candidate_post_context(
            candidate,
            include_extended_context=touch_should_plan,
        )
        if not post_context:
            if clap_should_attempt:
                self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "no_post")
            if touch_should_plan:
                self.repository.record_action(ACTION_PUBLIC_TOUCH_SKIPPED, candidate.user_id, "no_post")
            return False, False, False, False, None

        touch_plan = None
        if touch_should_plan:
            touch_plan = self._plan_pre_follow_public_touch(
                candidate,
                growth_policy=growth_policy,
                public_touch_budget_remaining=comment_budget_remaining,
                post_context=post_context,
                dry_run=False,
            )

        if not clap_should_attempt and touch_plan is None:
            return False, False, False, False, None

        if self.settings.pre_follow_read_wait_seconds > 0:
            await self._sleep_read_delay(target_user_id=candidate.user_id)

        clap_verified = False
        touch_verified = False
        touch_type: str | None = touch_plan.touch_type if touch_plan else None
        if clap_should_attempt:
            clap_verified = await self._perform_pre_follow_clap(candidate, post_id=post_context.post_id)
        if touch_plan is not None:
            if touch_plan.touch_type == ACTION_COMMENT and touch_plan.comment_text:
                touch_verified = await self._perform_pre_follow_comment(
                    candidate,
                    post_id=post_context.post_id,
                    comment_text=touch_plan.comment_text,
                )
            elif touch_plan.touch_type == ACTION_HIGHLIGHT and touch_plan.sentence_span is not None:
                touch_verified = await self._perform_pre_follow_highlight(
                    candidate,
                    post_context=post_context,
                    sentence_span=touch_plan.sentence_span,
                )

        return clap_should_attempt, clap_verified, touch_plan is not None, touch_verified, touch_type

    async def _resolve_candidate_latest_post_id(self, candidate: CandidateUser) -> str | None:
        context = await self._resolve_candidate_post_context(candidate, include_extended_context=False)
        return context.post_id if context else None

    async def _resolve_candidate_post_context(
        self,
        candidate: CandidateUser,
        *,
        include_extended_context: bool,
    ) -> PostContext | None:
        _ = include_extended_context
        contexts = await self._resolve_candidate_recent_post_contexts(candidate)
        if contexts:
            selected = self._select_best_recent_post_context(contexts)
            if selected is not None:
                candidate.latest_post_id = selected.post_id
                if selected.post_title:
                    candidate.latest_post_title = selected.post_title
                return selected

        # Fallback keeps screening behavior intact when extended post context is unavailable.
        latest_post = await self._execute_with_retry(
            "pre_follow_latest_post",
            operations.user_latest_post(user_id=candidate.user_id, username=candidate.username),
        )
        post_id, post_title = parse_latest_post_preview(latest_post)
        if not post_id:
            cached_post_id = candidate.latest_post_id
            if not cached_post_id:
                return None
            paragraphs = self._build_post_paragraph_contexts([], fallback_title=candidate.latest_post_title)
            paragraph_text_by_name = {paragraph.name: paragraph.text for paragraph in paragraphs}
            return PostContext(
                recent_rank=0,
                post_id=cached_post_id,
                post_title=candidate.latest_post_title,
                post_version_id=None,
                paragraphs=paragraphs,
                paragraph_text_by_name=paragraph_text_by_name,
                sentence_spans=self._candidate_sentence_spans(paragraphs),
            )
        candidate.latest_post_id = post_id
        if post_title:
            candidate.latest_post_title = post_title
        paragraphs = self._build_post_paragraph_contexts([], fallback_title=post_title)
        paragraph_text_by_name = {paragraph.name: paragraph.text for paragraph in paragraphs}
        return PostContext(
            recent_rank=0,
            post_id=post_id,
            post_title=post_title,
            post_version_id=None,
            paragraphs=paragraphs,
            paragraph_text_by_name=paragraph_text_by_name,
            sentence_spans=self._candidate_sentence_spans(paragraphs),
        )

    async def _resolve_candidate_recent_post_contexts(self, candidate: CandidateUser) -> list[PostContext]:
        cached = self._candidate_recent_posts_cache.get(candidate.user_id)
        if cached is not None:
            return cached

        context_result = await self._execute_with_retry(
            "pre_follow_recent_posts_context",
            operations.user_latest_post_context(user_id=candidate.user_id, username=candidate.username),
        )
        contexts: list[PostContext] = []
        for rank, (post_id, post_title, post_version_id, raw_paragraphs) in enumerate(
            parse_recent_post_contexts(context_result)[:RECENT_POST_CONTEXT_LIMIT]
        ):
            paragraphs = self._build_post_paragraph_contexts(raw_paragraphs, fallback_title=post_title)
            paragraph_text_by_name = {paragraph.name: paragraph.text for paragraph in paragraphs}
            contexts.append(
                PostContext(
                    recent_rank=rank,
                    post_id=post_id,
                    post_title=post_title,
                    post_version_id=post_version_id,
                    paragraphs=paragraphs,
                    paragraph_text_by_name=paragraph_text_by_name,
                    sentence_spans=self._candidate_sentence_spans(paragraphs),
                )
            )

        if contexts:
            self._candidate_recent_posts_cache[candidate.user_id] = contexts
        return contexts

    def _build_post_paragraph_contexts(
        self,
        raw_paragraphs: list[tuple[str, str | int | None, str]],
        *,
        fallback_title: str | None,
    ) -> list[ParagraphContext]:
        ordered: list[tuple[str, str | int | None, str]] = []
        seen_names: set[str] = set()
        for paragraph_name, paragraph_type, paragraph_text in raw_paragraphs:
            name = (paragraph_name or "").strip()
            text = self._normalized_paragraph_text(paragraph_text)
            if not name or not text or name in seen_names:
                continue
            seen_names.add(name)
            ordered.append((name, paragraph_type, text))

        if not ordered:
            fallback_text = self._normalized_paragraph_text(fallback_title)
            if fallback_text:
                ordered.append(("p000", None, fallback_text))

        total = len(ordered)
        lead_indices = set(range(min(LEAD_CLOSING_PARAGRAPH_WINDOW, total)))
        closing_start = max(0, total - LEAD_CLOSING_PARAGRAPH_WINDOW)
        closing_indices = set(range(closing_start, total))

        paragraphs: list[ParagraphContext] = []
        for index, (name, paragraph_type, text) in enumerate(ordered):
            in_lead = index in lead_indices
            in_closing = index in closing_indices
            if in_lead and in_closing:
                section = "lead_closing"
            elif in_lead:
                section = "lead"
            elif in_closing:
                section = "closing"
            else:
                section = "body"
            paragraphs.append(
                ParagraphContext(
                    name=name,
                    text=text,
                    paragraph_type=paragraph_type,
                    index=index,
                    section=section,
                )
            )
        return paragraphs

    @staticmethod
    def _normalized_paragraph_text(value: str | None) -> str:
        return " ".join((value or "").split()).strip()

    @staticmethod
    def _looks_like_code_text(text: str) -> bool:
        normalized = text.strip().lower()
        if not normalized:
            return False
        if "```" in normalized or "`" in normalized:
            return True
        if re.search(r"\b(def|class|function)\s+[a-z_]", normalized):
            return True
        if re.search(r"\b(import|from)\s+[a-z_]", normalized):
            return True
        if re.search(r"\b(const|let|var)\s+[a-z_$][\w$]*\s*=", normalized):
            return True
        symbol_count = sum(1 for char in normalized if char in "{}[]();=<>")
        return len(normalized) >= 40 and (symbol_count / max(1, len(normalized))) >= 0.12

    @staticmethod
    def _looks_like_list_text(text: str) -> bool:
        normalized = text.strip()
        if not normalized:
            return False
        return bool(
            re.match(r"^([-*•]\s+|\d+[.)]\s+)", normalized)
            or re.search(r"(?:\s[-*•]\s+\w+){2,}", normalized)
        )

    @staticmethod
    def _looks_like_quote_text(text: str) -> bool:
        normalized = text.strip()
        if not normalized:
            return False
        if normalized.startswith(">"):
            return True
        return bool(re.match(r'^[\"“][^\"”]{12,}[\"”]$', normalized))

    def _is_non_prose_text(self, text: str) -> bool:
        return (
            self._looks_like_code_text(text)
            or self._looks_like_list_text(text)
            or self._looks_like_quote_text(text)
        )

    @staticmethod
    def _paragraph_type_is_non_prose(paragraph_type: str | int | None) -> bool:
        if paragraph_type is None:
            return False
        normalized = str(paragraph_type).strip().upper()
        if not normalized:
            return False
        non_prose_types = {
            "IMG",
            "IFRAME",
            "MIXTAPE_EMBED",
            "PRE",
            "CODE_BLOCK",
            "CODE",
            "BLOCKQUOTE",
            "BQ",
            "QUOTE",
            "OLI",
            "ULI",
            "LI",
            "HR",
            "SEPARATOR",
        }
        return normalized in non_prose_types or normalized.endswith("_LI")

    def _paragraph_is_highlight_eligible(self, paragraph: ParagraphContext) -> bool:
        if paragraph.section not in {"lead", "closing", "lead_closing"}:
            return False
        if self._paragraph_type_is_non_prose(paragraph.paragraph_type):
            return False
        text = paragraph.text.strip()
        if not text:
            return False
        if len(text) < HIGHLIGHT_SENTENCE_MIN_CHARS:
            return False
        if self._is_non_prose_text(text):
            return False
        return True

    def _is_highlight_sentence_eligible(self, sentence: str) -> bool:
        if len(sentence) < HIGHLIGHT_SENTENCE_MIN_CHARS or len(sentence) > HIGHLIGHT_SENTENCE_MAX_CHARS:
            return False
        word_count = len(re.findall(r"\b[\w'-]+\b", sentence))
        if word_count < HIGHLIGHT_SENTENCE_MIN_WORDS or word_count > HIGHLIGHT_SENTENCE_MAX_WORDS:
            return False
        if self._is_non_prose_text(sentence):
            return False
        return True

    def _candidate_sentence_spans(self, paragraphs: list[ParagraphContext]) -> list[SentenceSpan]:
        spans: list[SentenceSpan] = []
        for paragraph in paragraphs:
            if not self._paragraph_is_highlight_eligible(paragraph):
                continue
            matches = list(re.finditer(r"[^.!?]+[.!?]", paragraph.text))
            if not matches:
                matches = list(re.finditer(r"[^.!?]+", paragraph.text))
            for match in matches:
                raw_sentence = match.group(0)
                stripped_sentence = self._normalized_paragraph_text(raw_sentence)
                if not self._is_highlight_sentence_eligible(stripped_sentence):
                    continue
                leading = len(raw_sentence) - len(raw_sentence.lstrip())
                trailing = len(raw_sentence) - len(raw_sentence.rstrip())
                start_offset = match.start() + leading
                end_offset = match.end() - trailing
                if end_offset <= start_offset:
                    continue
                spans.append(
                    SentenceSpan(
                        paragraph_name=paragraph.name,
                        text=stripped_sentence,
                        start_offset=start_offset,
                        end_offset=end_offset,
                        section=paragraph.section,
                    )
                )
        return spans

    def _select_pre_follow_highlight_span(self, post_context: PostContext) -> SentenceSpan | None:
        if not post_context.sentence_spans:
            return None
        lead_spans = [span for span in post_context.sentence_spans if span.section in {"lead", "lead_closing"}]
        closing_spans = [span for span in post_context.sentence_spans if span.section in {"closing", "lead_closing"}]
        if lead_spans and closing_spans:
            pool = lead_spans if random.random() < 0.5 else closing_spans
            return random.choice(pool)
        if lead_spans:
            return random.choice(lead_spans)
        if closing_spans:
            return random.choice(closing_spans)
        return None

    def _select_best_recent_post_context(self, contexts: list[PostContext]) -> PostContext | None:
        best: PostContext | None = None
        best_score = float("-inf")
        for context in contexts[:RECENT_POST_CONTEXT_LIMIT]:
            score = self._score_recent_post_context(context)
            if best is None or score > best_score:
                best = context
                best_score = score
                continue
            if math.isclose(score, best_score, rel_tol=1e-9, abs_tol=1e-9) and best is not None:
                if context.recent_rank < best.recent_rank:
                    best = context
                    best_score = score
        return best

    def _score_recent_post_context(self, context: PostContext) -> float:
        freshness_score = max(0.0, 1.0 - (context.recent_rank * 0.35))
        commentability_score = self._commentability_score(context)
        if not context.post_version_id:
            highlightability_score = 0.0
        else:
            sentence_count = len(context.sentence_spans)
            highlightability_score = 0.0
            if sentence_count == 1:
                highlightability_score = 0.6
            elif sentence_count >= 2:
                highlightability_score = 1.0
        return (freshness_score * 0.55) + (commentability_score * 0.25) + (highlightability_score * 0.20)

    def _commentability_score(self, context: PostContext) -> float:
        prose_paragraphs = [paragraph for paragraph in context.paragraphs if not self._is_non_prose_text(paragraph.text)]
        total_words = sum(len(re.findall(r"\b[\w'-]+\b", paragraph.text)) for paragraph in prose_paragraphs)
        if total_words >= 180:
            score = 1.0
        elif total_words >= 100:
            score = 0.82
        elif total_words >= 50:
            score = 0.64
        elif total_words >= 20:
            score = 0.42
        else:
            score = 0.2 if context.post_title else 0.0

        has_lead = any(paragraph.section in {"lead", "lead_closing"} for paragraph in prose_paragraphs)
        has_closing = any(paragraph.section in {"closing", "lead_closing"} for paragraph in prose_paragraphs)
        if has_lead and has_closing:
            score += 0.1
        return min(1.0, score)

    def _post_context_section_excerpt(self, post_context: PostContext | None, *, section: str) -> str | None:
        if post_context is None:
            return None
        section_set = {"lead", "lead_closing"} if section == "lead" else {"closing", "lead_closing"}
        for span in post_context.sentence_spans:
            if span.section in section_set:
                return span.text
        for paragraph in post_context.paragraphs:
            if paragraph.section not in section_set or self._is_non_prose_text(paragraph.text):
                continue
            snippet_match = re.search(r"[^.!?]+[.!?]", paragraph.text)
            snippet = snippet_match.group(0) if snippet_match else paragraph.text
            normalized = self._normalized_paragraph_text(snippet)
            if not normalized:
                continue
            if len(normalized) > 160:
                truncated = normalized[:160].rstrip()
                if " " in truncated:
                    truncated = truncated.rsplit(" ", 1)[0]
                normalized = f"{truncated.strip()}..."
            return normalized
        return None

    def _plan_pre_follow_public_touch(
        self,
        candidate: CandidateUser,
        *,
        growth_policy: GrowthPolicy,
        public_touch_budget_remaining: int,
        post_context: PostContext | None,
        dry_run: bool,
    ) -> PublicTouchPlan | None:
        if not self._pre_follow_public_touch_enabled(growth_policy):
            return None
        if public_touch_budget_remaining <= 0:
            self.repository.record_action(ACTION_PUBLIC_TOUCH_SKIPPED, candidate.user_id, "budget_exhausted")
            return None

        plans: list[PublicTouchPlan] = []
        comment_plan = self._build_comment_touch_plan(
            candidate,
            growth_policy=growth_policy,
            post_context=post_context,
        )
        if comment_plan is not None:
            plans.append(comment_plan)

        highlight_plan = self._build_highlight_touch_plan(
            candidate,
            growth_policy=growth_policy,
            post_context=post_context,
            dry_run=dry_run,
        )
        if highlight_plan is not None:
            plans.append(highlight_plan)

        if not plans:
            return None
        return random.choice(plans)

    def _build_comment_touch_plan(
        self,
        candidate: CandidateUser,
        *,
        growth_policy: GrowthPolicy,
        post_context: PostContext | None,
    ) -> PublicTouchPlan | None:
        if not self._pre_follow_comment_enabled(growth_policy):
            return None
        if not self._comment_mutation_supported:
            self.repository.record_action(ACTION_COMMENT_SKIPPED, candidate.user_id, "api_drift_detected")
            return None
        if not self._should_attempt_pre_follow_comment():
            self.repository.record_action(ACTION_COMMENT_SKIPPED, candidate.user_id, "probability_gate")
            return None
        comment_text = self._select_pre_follow_comment_text(candidate, post_context=post_context)
        if not comment_text:
            self.repository.record_action(ACTION_COMMENT_SKIPPED, candidate.user_id, "no_template")
            return None
        return PublicTouchPlan(touch_type=ACTION_COMMENT, comment_text=comment_text)

    def _build_highlight_touch_plan(
        self,
        candidate: CandidateUser,
        *,
        growth_policy: GrowthPolicy,
        post_context: PostContext | None,
        dry_run: bool,
    ) -> PublicTouchPlan | None:
        if not self._pre_follow_highlight_enabled(growth_policy):
            return None
        if not self._highlight_mutation_supported:
            self.repository.record_action(ACTION_HIGHLIGHT_SKIPPED, candidate.user_id, "api_drift_detected")
            return None
        if not self._should_attempt_pre_follow_highlight():
            self.repository.record_action(ACTION_HIGHLIGHT_SKIPPED, candidate.user_id, "probability_gate")
            return None
        if post_context is None:
            if dry_run:
                return PublicTouchPlan(touch_type=ACTION_HIGHLIGHT, sentence_span=None)
            self.repository.record_action(ACTION_HIGHLIGHT_SKIPPED, candidate.user_id, "no_post_context")
            return None
        if not post_context.post_version_id:
            self.repository.record_action(ACTION_HIGHLIGHT_SKIPPED, candidate.user_id, "missing_post_version")
            return None
        span = self._select_pre_follow_highlight_span(post_context)
        if span is None:
            self.repository.record_action(ACTION_HIGHLIGHT_SKIPPED, candidate.user_id, "no_sentence_span")
            return None
        return PublicTouchPlan(touch_type=ACTION_HIGHLIGHT, sentence_span=span)

    async def _perform_pre_follow_clap(self, candidate: CandidateUser, *, post_id: str) -> bool:
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
        observed_clap_count = parse_clap_count(clap_result)
        viewer_clap_count = parse_viewer_clap_count(clap_result)
        clap_verified = clap_ok and observed_clap_count is not None and observed_clap_count >= 1
        clap_action_key = self._daily_action_key(ACTION_CLAP, candidate.user_id, extra=post_id)
        if clap_verified:
            status_label = (
                f"verified:num_claps={clap_count};"
                f"viewer_clap_count={'' if viewer_clap_count is None else viewer_clap_count};"
                f"post_clap_count={'' if observed_clap_count is None else observed_clap_count}"
            )
        else:
            status_label = f"failed:num_claps={clap_count}"
        self.repository.record_action(
            ACTION_CLAP,
            candidate.user_id,
            status_label,
            action_key=clap_action_key,
        )
        return clap_verified

    async def _perform_pre_follow_comment(
        self,
        candidate: CandidateUser,
        *,
        post_id: str,
        comment_text: str,
    ) -> bool:
        await self._sleep_action_gap(action_type=ACTION_COMMENT, target_user_id=candidate.user_id)
        comment_result = await self._execute_with_retry(
            "comment_pre_follow",
            operations.publish_threaded_response(post_id, comment_text),
        )
        comment_id = parse_publish_threaded_response_id(comment_result)
        drift_detected = self._comment_api_drift_detected(comment_result)
        if drift_detected:
            self._comment_mutation_supported = False
            self.log.warning(
                "comment_mutation_api_drift_detected",
                status_code=comment_result.status_code,
                error_messages=[error.message for error in comment_result.errors],
            )
        comment_verified = comment_result.status_code == 200 and not comment_result.has_errors and bool(comment_id)
        comment_action_key = self._daily_action_key(ACTION_COMMENT, candidate.user_id, extra=post_id)
        if comment_verified:
            status_label = f"verified:{comment_id}"
        elif drift_detected:
            status_label = "failed:api_drift"
        else:
            status_label = "failed"
        self.repository.record_action(
            ACTION_COMMENT,
            candidate.user_id,
            status_label,
            action_key=comment_action_key,
        )
        return comment_verified

    async def _perform_pre_follow_highlight(
        self,
        candidate: CandidateUser,
        *,
        post_context: PostContext,
        sentence_span: SentenceSpan,
    ) -> bool:
        if not post_context.post_version_id:
            self.repository.record_action(ACTION_HIGHLIGHT_SKIPPED, candidate.user_id, "missing_post_version")
            return False

        await self._sleep_action_gap(action_type=ACTION_HIGHLIGHT, target_user_id=candidate.user_id)
        highlight_result = await self._execute_with_retry(
            "highlight_pre_follow",
            operations.create_quote_highlight(
                target_post_id=post_context.post_id,
                target_post_version_id=post_context.post_version_id,
                target_paragraph_names=[sentence_span.paragraph_name],
                start_offset=sentence_span.start_offset,
                end_offset=sentence_span.end_offset,
            ),
        )
        quote_id = parse_create_quote_id(highlight_result)
        drift_detected = self._highlight_api_drift_detected(highlight_result)
        if drift_detected:
            self._highlight_mutation_supported = False
            self.log.warning(
                "highlight_mutation_api_drift_detected",
                status_code=highlight_result.status_code,
                error_messages=[error.message for error in highlight_result.errors],
            )

        # Keep highlight attempts in the same daily-budget stream as comments.
        budget_action_key = self._daily_action_key(ACTION_COMMENT, candidate.user_id, extra=post_context.post_id)
        self.repository.record_action(
            ACTION_COMMENT,
            candidate.user_id,
            "shadow:highlight",
            action_key=budget_action_key,
        )

        highlight_verified = highlight_result.status_code == 200 and not highlight_result.has_errors and bool(quote_id)
        highlight_action_key = self._daily_action_key(ACTION_HIGHLIGHT, candidate.user_id, extra=post_context.post_id)
        if highlight_verified and quote_id:
            status_label = f"verified:quote_id={quote_id};post_id={post_context.post_id}"
        elif drift_detected:
            status_label = f"failed:api_drift;post_id={post_context.post_id}"
        else:
            status_label = f"failed:post_id={post_context.post_id}"
        self.repository.record_action(
            ACTION_HIGHLIGHT,
            candidate.user_id,
            status_label,
            action_key=highlight_action_key,
        )
        return highlight_verified

    def _resolve_growth_policy(
        self,
        *,
        growth_policy: GrowthPolicy | None,
        growth_mode: GrowthMode | None,
    ) -> GrowthPolicy:
        if growth_policy is not None:
            return growth_policy
        if growth_mode == GrowthMode.SIMPLE:
            return GrowthPolicy.FOLLOW_ONLY
        if growth_mode == GrowthMode.SMART:
            return GrowthPolicy.WARM_ENGAGE_RARE_COMMENT
        return self.settings.default_growth_policy

    def _resolve_growth_sources(
        self,
        *,
        growth_sources: list[GrowthSource] | None,
        discovery_mode: GrowthDiscoveryMode | None,
    ) -> list[GrowthSource]:
        if growth_sources:
            deduped: list[GrowthSource] = []
            for source in growth_sources:
                if source not in deduped:
                    deduped.append(source)
            return deduped
        if discovery_mode == GrowthDiscoveryMode.TARGET_USER_FOLLOWERS:
            return [GrowthSource.TARGET_USER_FOLLOWERS]
        return list(self.settings.default_growth_sources)

    @staticmethod
    def _legacy_growth_mode_for_policy(growth_policy: GrowthPolicy) -> GrowthMode:
        return GrowthMode.SIMPLE if growth_policy == GrowthPolicy.FOLLOW_ONLY else GrowthMode.SMART

    @staticmethod
    def _legacy_discovery_mode_for_sources(growth_sources: list[GrowthSource]) -> GrowthDiscoveryMode:
        if growth_sources == [GrowthSource.TARGET_USER_FOLLOWERS]:
            return GrowthDiscoveryMode.TARGET_USER_FOLLOWERS
        return GrowthDiscoveryMode.GENERAL

    @staticmethod
    def _growth_sources_need_probe(growth_sources: list[GrowthSource]) -> bool:
        return any(source in {GrowthSource.TOPIC_RECOMMENDED, GrowthSource.RESPONDERS} for source in growth_sources)

    def _resolve_target_user_scan_limit(self, target_user_scan_limit: int | None) -> int:
        resolved = target_user_scan_limit or self.settings.target_user_followers_scan_limit
        return max(1, resolved)

    def _discovery_candidate_scan_limit(self, eligible_target: int) -> int:
        if eligible_target <= 0:
            return 0
        floor = max(eligible_target, self.settings.growth_queue_buffer_target_min)
        ceiling = max(floor, self.settings.growth_queue_buffer_target_max)
        scaled = eligible_target * max(1, self.settings.growth_queue_buffer_target_multiplier)
        return min(ceiling, max(floor, scaled))

    def _execution_queue_fetch_limit(self, max_follow_attempts_for_cycle: int) -> int:
        if max_follow_attempts_for_cycle <= 0:
            return 0
        floor = max(1, self.settings.growth_queue_fetch_limit_min)
        ceiling = max(floor, self.settings.growth_queue_fetch_limit_max)
        scaled = max_follow_attempts_for_cycle * max(1, self.settings.growth_queue_fetch_limit_multiplier)
        return min(ceiling, max(floor, scaled))

    def _growth_queue_deferred_retry_at(self, *, user_id: str, reason: str) -> str:
        if reason == "skip:cooldown_active":
            retry_after = self.repository.recent_action_retry_after(
                user_id,
                within_hours=self.settings.follow_cooldown_hours,
                action_types=FOLLOW_COOLDOWN_ACTION_TYPES,
            )
            if retry_after is not None:
                return retry_after

        retry_seconds = self._growth_queue_deferred_retry_seconds(reason)
        scheduled = datetime.now(timezone.utc) + timedelta(seconds=retry_seconds)
        return scheduled.strftime("%Y-%m-%d %H:%M:%S")

    def _growth_queue_deferred_retry_seconds(self, reason: str) -> int:
        pass_cooldown_max = max(
            self.settings.pass_cooldown_min_seconds,
            self.settings.pass_cooldown_max_seconds,
        )
        short_retry = max(
            self.settings.growth_queue_retry_short_floor_seconds,
            pass_cooldown_max * self.settings.growth_queue_retry_short_cooldown_multiplier,
        )
        medium_retry = max(
            self.settings.growth_queue_retry_medium_floor_seconds,
            pass_cooldown_max * self.settings.growth_queue_retry_medium_cooldown_multiplier,
        )
        long_retry = max(
            self.settings.growth_queue_retry_long_floor_seconds,
            pass_cooldown_max * self.settings.growth_queue_retry_long_cooldown_multiplier,
        )

        if reason == "follow_attempt:started":
            return max(
                self.settings.growth_queue_retry_started_floor_seconds,
                pass_cooldown_max * self.settings.growth_queue_retry_started_cooldown_multiplier,
            )
        if reason == "skip:live_check_unavailable":
            return short_retry
        if reason == "follow_failed:mutation_error":
            return medium_retry
        if reason == "follow_failed:verification_failed":
            return long_retry
        return short_retry

    def _pre_follow_clap_enabled(self, growth_policy: GrowthPolicy) -> bool:
        return (
            growth_policy in {GrowthPolicy.WARM_ENGAGE, GrowthPolicy.WARM_ENGAGE_RARE_COMMENT}
            and self.settings.enable_pre_follow_clap
        )

    @staticmethod
    def _pre_follow_public_touch_enabled(growth_policy: GrowthPolicy) -> bool:
        return growth_policy == GrowthPolicy.WARM_ENGAGE_RARE_COMMENT

    def _pre_follow_comment_enabled(self, growth_policy: GrowthPolicy) -> bool:
        return growth_policy == GrowthPolicy.WARM_ENGAGE_RARE_COMMENT and self.settings.enable_pre_follow_comment

    def _pre_follow_highlight_enabled(self, growth_policy: GrowthPolicy) -> bool:
        return growth_policy == GrowthPolicy.WARM_ENGAGE_RARE_COMMENT and self.settings.enable_pre_follow_highlight

    def _should_attempt_pre_follow_comment(self) -> bool:
        return random.random() < self.settings.pre_follow_comment_probability

    def _should_attempt_pre_follow_highlight(self) -> bool:
        return random.random() < self.settings.pre_follow_highlight_probability

    def _select_pre_follow_comment_text(
        self,
        candidate: CandidateUser,
        *,
        post_context: PostContext | None,
    ) -> str | None:
        lead_excerpt = self._post_context_section_excerpt(post_context, section="lead")
        closing_excerpt = self._post_context_section_excerpt(post_context, section="closing")
        templates = build_comment_template_pool(
            candidate_title=(post_context.post_title if post_context and post_context.post_title else candidate.latest_post_title),
            candidate_bio=candidate.bio,
            post_lead_text=lead_excerpt,
            post_closing_text=closing_excerpt,
            base_templates=self.settings.pre_follow_comment_templates,
        )
        if not templates:
            return None
        return random.choice(templates)

    @staticmethod
    def _comment_api_drift_detected(result: GraphQLResult) -> bool:
        if not result.errors:
            return False
        drift_tokens = (
            'unknown argument "sorttype"',
            'field "insert" is not defined by type "delta"',
            'value "public" does not exist in "responsedistributiontype" enum',
            "malformed deltas",
        )
        return any(
            any(token in error.message.lower() for token in drift_tokens)
            for error in result.errors
        )

    @staticmethod
    def _highlight_api_drift_detected(result: GraphQLResult) -> bool:
        if not result.errors:
            return False
        drift_tokens = (
            'unknown argument "targetpostversionid"',
            'field "createquote" is not defined',
            'unknown type "streamitemquotetype"',
            'unknown argument "targetparagraphnames"',
        )
        return any(
            any(token in error.message.lower() for token in drift_tokens)
            for error in result.errors
        )

    def _hydrate_candidate_from_user_viewer_edge(self, candidate: CandidateUser, result: GraphQLResult) -> None:
        user_node = parse_user_viewer_user_node(result)
        if user_node is not None:
            enriched = self._candidate_from_user_node(
                user_node,
                source=candidate.sources[0] if candidate.sources else CandidateSource.TOPIC_LATEST_STORIES,
                latest_post_id=candidate.latest_post_id,
                latest_post_title=candidate.latest_post_title,
            )
            if enriched is not None:
                self._merge_candidate({candidate.user_id: candidate}, enriched)
        last_post_created_at = parse_user_viewer_last_post_created_at(result)
        if last_post_created_at:
            candidate.last_post_created_at = last_post_created_at

    def _candidate_volume_filter_reason(self, candidate: CandidateUser) -> str | None:
        follower_count = candidate.follower_count
        following_count = candidate.following_count
        if follower_count is not None and follower_count < self.settings.candidate_min_followers:
            return "skip:followers_below_min"
        if (
            self.settings.candidate_max_followers > 0
            and follower_count is not None
            and follower_count > self.settings.candidate_max_followers
        ):
            return "skip:followers_above_max"
        if following_count is not None and following_count < self.settings.candidate_min_following:
            return "skip:following_below_min"
        if (
            self.settings.candidate_max_following > 0
            and following_count is not None
            and following_count > self.settings.candidate_max_following
        ):
            return "skip:following_above_max"
        return None

    def _refresh_candidate_scoring(self, candidate: CandidateUser) -> None:
        candidate.score = self._score_candidate(candidate)

    def _score_candidate(self, candidate: CandidateUser) -> float:
        topic_matches = self._candidate_topic_matches(candidate)
        primary_topic_keywords = self._positive_primary_topic_keywords(topic_matches)
        secondary_topic_keywords = self._positive_secondary_topic_keywords(topic_matches)
        negative_keywords = self._negative_topic_keywords(topic_matches)
        candidate.matched_keywords = primary_topic_keywords

        followback_score = self._candidate_followback_likelihood_score(candidate)
        topic_score = self._candidate_topic_fit_score(topic_matches)
        source_score = self._candidate_source_affinity_score(candidate)
        newsletter_score = 1.0 if candidate.newsletter_v3_id else 0.0
        presence_score = self._candidate_presence_score(candidate)
        activity_score = self._candidate_activity_score(candidate)
        raw_components = {
            "followback": round(followback_score, 6),
            "topic": round(topic_score, 6),
            "source": round(source_score, 6),
            "newsletter": round(newsletter_score, 6),
            "presence": round(presence_score, 6),
            "activity": round(activity_score, 6),
        }
        weighted_components = {
            "followback": round(self.settings.score_weight_ratio * followback_score, 6),
            "topic": round(self.settings.score_weight_keyword * topic_score, 6),
            "source": round(self.settings.score_weight_source * source_score, 6),
            "newsletter": round(self.settings.score_weight_newsletter * newsletter_score, 6),
            "presence": round(self.settings.score_weight_presence * presence_score, 6),
            "activity": round(self.settings.score_weight_activity * activity_score, 6),
        }
        base_score = round(sum(weighted_components.values()), 6)
        penalty_total = round(
            min(base_score, self.settings.negative_topic_penalty * len(negative_keywords)),
            6,
        )
        score_before_learning = round(max(0.0, base_score - penalty_total), 6)
        score_breakdown = CandidateScoreBreakdown(
            raw_components=raw_components,
            weighted_components=weighted_components,
            base_score=base_score,
            penalty_total=penalty_total,
            score_before_learning=score_before_learning,
            final_score=score_before_learning,
            matched_keywords=topic_matches,
            primary_topic_keywords=primary_topic_keywords,
            secondary_topic_keywords=secondary_topic_keywords,
            negative_keywords=negative_keywords,
            source_weights=self._candidate_source_weights(candidate),
            ratio_band=self._candidate_ratio_band(candidate),
            presence_band=self._score_band(presence_score),
            activity_band=self._score_band(activity_score),
        )
        (
            learning_multiplier,
            learning_keys,
            learning_bucket_samples,
            learning_bucket_rates,
        ) = self._candidate_learning_adjustment(candidate, score_breakdown)
        final_score = round(score_before_learning * learning_multiplier, 6)
        score_breakdown.learning_multiplier = learning_multiplier
        score_breakdown.learning_keys = learning_keys
        score_breakdown.learning_bucket_samples = learning_bucket_samples
        score_breakdown.learning_bucket_rates = learning_bucket_rates
        score_breakdown.final_score = final_score
        if negative_keywords:
            score_breakdown.filter_reasons.append("penalty:negative_topic_keyword")
        candidate.score_breakdown = score_breakdown
        return final_score

    def _candidate_followback_likelihood_score(self, candidate: CandidateUser) -> float:
        ratio_score = self._candidate_ratio_fit_score(candidate)
        following_score = self._candidate_following_activity_score(candidate)
        audience_score = self._candidate_audience_fit_score(candidate)
        return round((ratio_score * 0.55) + (following_score * 0.25) + (audience_score * 0.20), 6)

    def _candidate_ratio_fit_score(self, candidate: CandidateUser) -> float:
        if candidate.follower_count is None or candidate.following_count is None:
            return 0.35
        ratio = self._following_follower_ratio(candidate)
        if ratio <= 0:
            return 0.0

        # Prefer users who follow enough people to plausibly follow back, but
        # penalize extreme following/follower ratios that usually represent
        # low-signal or indiscriminate accounts.
        if 0.8 <= ratio <= 3.0:
            return 1.0
        if ratio < 0.8:
            lower = max(0.0, self.settings.min_following_follower_ratio)
            if ratio <= lower:
                return 0.15 if math.isclose(ratio, lower) else 0.0
            span = max(0.001, 0.8 - lower)
            return min(1.0, 0.35 + ((ratio - lower) / span) * 0.65)

        if ratio <= 8.0:
            return max(0.45, 1.0 - ((ratio - 3.0) / 5.0) * 0.55)
        upper = max(8.0, self.settings.max_following_follower_ratio)
        if ratio >= upper:
            return 0.08
        return max(0.08, 0.45 - ((ratio - 8.0) / max(0.001, upper - 8.0)) * 0.37)

    @staticmethod
    def _candidate_following_activity_score(candidate: CandidateUser) -> float:
        following_count = candidate.following_count
        if following_count is None:
            return 0.5
        if following_count <= 0:
            return 0.1
        if following_count < 50:
            return 0.45
        if following_count <= 1200:
            return 1.0
        if following_count <= 5000:
            return 0.75
        return 0.45

    @staticmethod
    def _candidate_audience_fit_score(candidate: CandidateUser) -> float:
        follower_count = candidate.follower_count
        if follower_count is None:
            return 0.5
        if follower_count < 25:
            return 0.2
        if follower_count <= 500:
            return 1.0
        if follower_count <= 2000:
            return 0.85
        if follower_count <= 10000:
            return 0.55
        if follower_count <= 100000:
            return 0.3
        return 0.15

    @staticmethod
    def _candidate_topic_fit_score(topic_matches: dict[str, list[str]]) -> float:
        points = (
            len(topic_matches.get("strong_primary", [])) * 1.0
            + len(topic_matches.get("standard_primary", [])) * 0.65
            + len(topic_matches.get("soft_primary", [])) * 0.35
            + len(topic_matches.get("strong_secondary", [])) * 0.25
            + len(topic_matches.get("standard_secondary", [])) * 0.16
            + len(topic_matches.get("soft_secondary", [])) * 0.10
        )
        return round(min(1.0, points / 3.0), 6)

    def _candidate_topic_matches(self, candidate: CandidateUser) -> dict[str, list[str]]:
        primary_texts = (candidate.bio, candidate.latest_post_title)
        secondary_texts = (candidate.name, candidate.username)
        return {
            "strong_primary": self._match_keywords_in_texts(primary_texts, self.settings.topic_strong_keywords),
            "standard_primary": self._match_keywords_in_texts(primary_texts, self.settings.bio_keywords),
            "soft_primary": self._match_keywords_in_texts(primary_texts, self.settings.topic_soft_keywords),
            "strong_secondary": self._match_keywords_in_texts(secondary_texts, self.settings.topic_strong_keywords),
            "standard_secondary": self._match_keywords_in_texts(secondary_texts, self.settings.bio_keywords),
            "soft_secondary": self._match_keywords_in_texts(secondary_texts, self.settings.topic_soft_keywords),
            "negative_primary": self._match_keywords_in_texts(primary_texts, self.settings.topic_negative_keywords),
            "negative_secondary": self._match_keywords_in_texts(secondary_texts, self.settings.topic_negative_keywords),
        }

    @staticmethod
    def _positive_primary_topic_keywords(topic_matches: dict[str, list[str]]) -> list[str]:
        return DailyRunner._unique_ordered(
            [
                *topic_matches.get("strong_primary", []),
                *topic_matches.get("standard_primary", []),
                *topic_matches.get("soft_primary", []),
            ]
        )

    @staticmethod
    def _positive_secondary_topic_keywords(topic_matches: dict[str, list[str]]) -> list[str]:
        return DailyRunner._unique_ordered(
            [
                *topic_matches.get("strong_secondary", []),
                *topic_matches.get("standard_secondary", []),
                *topic_matches.get("soft_secondary", []),
            ]
        )

    @staticmethod
    def _negative_topic_keywords(topic_matches: dict[str, list[str]]) -> list[str]:
        return DailyRunner._unique_ordered(
            [
                *topic_matches.get("negative_primary", []),
                *topic_matches.get("negative_secondary", []),
            ]
        )

    def _candidate_source_affinity_score(self, candidate: CandidateUser) -> float:
        source_weights = self._candidate_source_weights(candidate)
        if not source_weights:
            return 0.0
        best = max(source_weights.values())
        repeated_source_bonus = 0.08 * max(0, len(source_weights) - 1)
        return round(min(1.0, best + repeated_source_bonus), 6)

    def _candidate_source_weights(self, candidate: CandidateUser) -> dict[str, float]:
        source_scores = {
            CandidateSource.SEED_FOLLOWERS: 0.9,
            CandidateSource.TARGET_USER_FOLLOWERS: 0.85,
            CandidateSource.POST_RESPONDERS: 0.8,
            CandidateSource.TOPIC_CURATED_LIST: 0.72,
            CandidateSource.TOPIC_LATEST_STORIES: 0.65,
            CandidateSource.TOPIC_WHO_TO_FOLLOW: 0.58,
            CandidateSource.WHO_TO_FOLLOW_MODULE: 0.45,
        }
        weights: dict[str, float] = {}
        for source in candidate.sources:
            weights[source.value] = round(self._source_quality_score(source, source_scores.get(source, 0.35)), 6)
        return weights

    def _source_quality_score(self, source: CandidateSource, default_score: float) -> float:
        overrides = self.settings.discovery_source_quality_weights
        candidate_key = self._source_quality_key(source.value)
        growth_key = self._source_quality_key(self._growth_source_for_candidate_source(source).value)
        return max(0.0, min(1.0, overrides.get(candidate_key, overrides.get(growth_key, default_score))))

    def _candidate_presence_score(self, candidate: CandidateUser) -> float:
        score = 0.0
        if candidate.username:
            score += 0.08
        if candidate.name:
            score += 0.08
        if self._candidate_has_meaningful_bio(candidate):
            score += 0.24
        elif candidate.bio and candidate.bio.strip():
            score += 0.1
        if candidate.newsletter_v3_id:
            score += 0.16
        if candidate.follower_count is not None and candidate.following_count is not None:
            score += 0.14
        if candidate.follower_count is not None and candidate.follower_count >= self.settings.candidate_min_followers:
            score += 0.08
        if candidate.latest_post_id or candidate.latest_post_title:
            score += 0.14
        if candidate.last_post_created_at:
            score += 0.08 + (self._candidate_activity_score(candidate) * 0.10)
        return min(1.0, score)

    @staticmethod
    def _candidate_has_meaningful_bio(candidate: CandidateUser) -> bool:
        bio = (candidate.bio or "").strip()
        if not bio:
            return False
        tokens = re.findall(r"[a-z0-9]+", bio.lower())
        return len(tokens) >= 4 and len(bio) >= 24

    def _candidate_activity_score(self, candidate: CandidateUser) -> float:
        timestamp = self._parse_iso_datetime(candidate.last_post_created_at)
        if timestamp is None:
            return 0.45 if candidate.latest_post_id or candidate.latest_post_title else 0.0
        age_days = max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds() / 86400)
        if age_days <= 14:
            return 1.0
        if age_days <= 30:
            return 0.85
        if age_days <= 90:
            return 0.6
        if age_days <= 180:
            return 0.35
        return 0.12

    def _candidate_learning_adjustment(
        self,
        candidate: CandidateUser,
        score_breakdown: CandidateScoreBreakdown,
    ) -> tuple[float, list[str], dict[str, int], dict[str, float]]:
        if not self.settings.discovery_learning_enabled:
            return 1.0, [], {}, {}
        model = self._discovery_learning_model()
        global_completed = int(model.get("global_completed", 0))
        global_rate = float(model.get("global_rate", 0.0))
        buckets = model.get("buckets", {})
        if global_completed < self.settings.discovery_learning_min_completed or global_rate <= 0:
            return 1.0, [], {}, {}
        if not isinstance(buckets, dict):
            return 1.0, [], {}, {}

        learning_keys = self._candidate_learning_keys(candidate, score_breakdown)
        samples: dict[str, int] = {}
        rates: dict[str, float] = {}
        deltas: list[float] = []
        prior = self.settings.discovery_learning_prior_strength
        for key in learning_keys:
            bucket = buckets.get(key)
            if not isinstance(bucket, dict):
                continue
            completed = int(bucket.get("completed", 0))
            followed_back = int(bucket.get("followed_back", 0))
            if completed < self.settings.discovery_learning_min_completed:
                continue
            smoothed_rate = (followed_back + (global_rate * prior)) / (completed + prior)
            samples[key] = completed
            rates[key] = round(smoothed_rate, 4)
            deltas.append((smoothed_rate / global_rate) - 1.0)

        if not deltas:
            return 1.0, learning_keys, samples, rates
        average_delta = sum(deltas) / len(deltas)
        max_delta = self.settings.discovery_learning_max_delta
        bounded_delta = max(-max_delta, min(max_delta, average_delta))
        return round(1.0 + bounded_delta, 6), learning_keys, samples, rates

    def _discovery_learning_model(self) -> dict[str, object]:
        cached = getattr(self, "_discovery_learning_cache", None)
        if cached is not None:
            return cached
        model: dict[str, object] = {
            "global_completed": 0,
            "global_followed_back": 0,
            "global_rate": 0.0,
            "buckets": {},
        }
        repository = getattr(self, "repository", None)
        if repository is None or not self.settings.discovery_learning_enabled:
            self._discovery_learning_cache = model
            return model

        buckets: dict[str, dict[str, int]] = {}
        global_completed = 0
        global_followed_back = 0
        for row in repository.discovery_learning_rows(lookback_days=self.settings.discovery_learning_lookback_days):
            status = str(row.get("cleanup_status") or "")
            if status not in {"followed_back", "unfollowed_nonreciprocal"}:
                continue
            success = status == "followed_back"
            global_completed += 1
            if success:
                global_followed_back += 1
            for key in self._learning_keys_from_outcome_row(row):
                bucket = buckets.setdefault(key, {"completed": 0, "followed_back": 0})
                bucket["completed"] += 1
                if success:
                    bucket["followed_back"] += 1

        model["global_completed"] = global_completed
        model["global_followed_back"] = global_followed_back
        model["global_rate"] = round((global_followed_back / global_completed) if global_completed else 0.0, 6)
        model["buckets"] = buckets
        self._discovery_learning_cache = model
        return model

    def _learning_keys_from_outcome_row(self, row: dict[str, str | None]) -> list[str]:
        keys: list[str] = []
        growth_sources = self._split_serialized_values(row.get("growth_sources"))
        if not growth_sources:
            growth_sources = self._split_serialized_values(row.get("follow_source"))
        growth_policy = (row.get("growth_policy") or "unknown").strip() or "unknown"
        for source in growth_sources:
            keys.append(self._learning_key("source", source))
            keys.append(self._learning_key("source_policy", f"{source}|{growth_policy}"))
        keys.append(self._learning_key("policy", growth_policy))

        score_breakdown = self._score_breakdown_from_raw(row.get("score_breakdown_json"))
        if score_breakdown is not None:
            for keyword in score_breakdown.primary_topic_keywords:
                keys.append(self._learning_key("topic", keyword))
            if score_breakdown.ratio_band:
                keys.append(self._learning_key("ratio_band", score_breakdown.ratio_band))
            if score_breakdown.presence_band:
                keys.append(self._learning_key("presence_band", score_breakdown.presence_band))
            if score_breakdown.activity_band:
                keys.append(self._learning_key("activity_band", score_breakdown.activity_band))
        return self._unique_ordered(keys)

    def _candidate_learning_keys(self, candidate: CandidateUser, score_breakdown: CandidateScoreBreakdown) -> list[str]:
        keys: list[str] = []
        growth_policy = getattr(self, "_active_growth_policy_for_scoring", self.settings.default_growth_policy)
        policy_value = growth_policy.value if isinstance(growth_policy, GrowthPolicy) else str(growth_policy)
        for source in self._candidate_growth_source_values(candidate):
            keys.append(self._learning_key("source", source))
            keys.append(self._learning_key("source_policy", f"{source}|{policy_value}"))
        keys.append(self._learning_key("policy", policy_value))
        for keyword in score_breakdown.primary_topic_keywords:
            keys.append(self._learning_key("topic", keyword))
        if score_breakdown.ratio_band:
            keys.append(self._learning_key("ratio_band", score_breakdown.ratio_band))
        if score_breakdown.presence_band:
            keys.append(self._learning_key("presence_band", score_breakdown.presence_band))
        if score_breakdown.activity_band:
            keys.append(self._learning_key("activity_band", score_breakdown.activity_band))
        return self._unique_ordered(keys)

    @staticmethod
    def _score_breakdown_from_raw(raw_score_breakdown: str | None) -> CandidateScoreBreakdown | None:
        if not raw_score_breakdown:
            return None
        try:
            return CandidateScoreBreakdown.model_validate_json(raw_score_breakdown)
        except ValueError:
            return None

    @staticmethod
    def _split_serialized_values(raw_value: str | None) -> list[str]:
        if not raw_value:
            return []
        return [item.strip() for item in raw_value.split(",") if item.strip()]

    @staticmethod
    def _learning_key(prefix: str, value: str) -> str:
        normalized = value.strip().lower()
        return f"{prefix}:{normalized}"

    @staticmethod
    def _source_quality_key(value: str) -> str:
        return value.strip().lower().replace("-", "_")

    def _candidate_ratio_band(self, candidate: CandidateUser) -> str:
        if candidate.follower_count is None or candidate.following_count is None:
            return "unknown"
        ratio = self._following_follower_ratio(candidate)
        if ratio < self.settings.min_following_follower_ratio:
            return "below_threshold"
        if ratio < 0.8:
            return "low"
        if ratio <= 3.0:
            return "balanced"
        if ratio <= 8.0:
            return "high"
        if ratio <= self.settings.max_following_follower_ratio:
            return "noisy"
        return "above_threshold"

    @staticmethod
    def _score_band(score: float) -> str:
        if score >= 0.8:
            return "high"
        if score >= 0.5:
            return "medium"
        if score > 0.0:
            return "low"
        return "none"

    @staticmethod
    def _unique_ordered(items: list[str]) -> list[str]:
        values: list[str] = []
        for item in items:
            normalized = item.strip().lower()
            if normalized and normalized not in values:
                values.append(normalized)
        return values

    def _candidate_has_recent_activity(self, candidate: CandidateUser) -> bool:
        if self.settings.candidate_recent_activity_days <= 0:
            return True
        timestamp = self._parse_iso_datetime(candidate.last_post_created_at)
        if timestamp is None:
            return False
        threshold = datetime.now(timezone.utc) - timedelta(days=self.settings.candidate_recent_activity_days)
        return timestamp >= threshold

    def _candidate_negative_filter_reason(self, candidate: CandidateUser) -> str | None:
        if not self.settings.discovery_reject_negative_keywords:
            return None
        score_breakdown = candidate.score_breakdown
        if score_breakdown is not None and score_breakdown.negative_keywords:
            return "skip:negative_topic_keyword"
        return None

    @staticmethod
    def _parse_iso_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.isdigit():
            raw = int(normalized)
            if raw > 10_000_000_000:
                raw = raw / 1000
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _growth_source_for_candidate_source(source: CandidateSource) -> GrowthSource:
        mapping = {
            CandidateSource.TOPIC_LATEST_STORIES: GrowthSource.TOPIC_RECOMMENDED,
            CandidateSource.TOPIC_WHO_TO_FOLLOW: GrowthSource.TOPIC_RECOMMENDED,
            CandidateSource.WHO_TO_FOLLOW_MODULE: GrowthSource.TOPIC_RECOMMENDED,
            CandidateSource.TOPIC_CURATED_LIST: GrowthSource.PUBLICATION_ADJACENT,
            CandidateSource.SEED_FOLLOWERS: GrowthSource.SEED_FOLLOWERS,
            CandidateSource.TARGET_USER_FOLLOWERS: GrowthSource.TARGET_USER_FOLLOWERS,
            CandidateSource.POST_RESPONDERS: GrowthSource.RESPONDERS,
        }
        return mapping[source]

    def _candidate_growth_source_values(self, candidate: CandidateUser) -> list[str]:
        return self._candidate_growth_source_values_static(candidate)

    def _primary_growth_source_value(self, candidate: CandidateUser) -> str:
        values = self._candidate_growth_source_values(candidate)
        return values[0] if values else "unknown"

    async def _execute_cleanup_pipeline(
        self,
        *,
        dry_run: bool,
        max_to_run: int,
        decisions: list[CandidateDecision],
    ) -> tuple[int, int]:
        if max_to_run <= 0:
            return 0, 0

        self.repository.upsert_imported_follow_cycle_pending_from_following_cache()
        due_pool = self.repository.pending_nonreciprocal_candidates(
            grace_days=self.settings.unfollow_nonreciprocal_after_days,
            limit=max(max_to_run, max_to_run * 5),
        )
        if not due_pool:
            return 0, 0
        if not self.settings.medium_user_ref:
            self.log.warning("cleanup_skipped_missing_medium_user_ref")
            return 0, 0

        own_following_ids = self.repository.cached_own_following_ids()
        if not own_following_ids:
            self.log.warning("cleanup_skipped_missing_following_cache")
            return 0, 0

        due: list[dict[str, str | None]] = []
        for row in due_pool:
            user_id = row["user_id"]
            username = row.get("username")
            if user_id in own_following_ids:
                due.append(row)
                if len(due) >= max_to_run:
                    break
                continue

            decisions.append(
                CandidateDecision(
                    user_id=user_id,
                    username=username,
                    eligible=False,
                    reason="cleanup:skip_not_in_following_cache",
                )
            )
            if dry_run:
                continue
            self.repository.mark_cleanup_skipped(user_id)
            self.repository.upsert_relationship_state(
                user_id,
                newsletter_state=NewsletterState.UNKNOWN,
                user_follow_state=UserFollowState.NOT_FOLLOWING,
                confidence=RelationshipConfidence.INFERRED,
                source_operation="OwnFollowingCache",
                verified_now=False,
            )
            self.repository.mark_candidate_reconciled(user_id, UserFollowState.NOT_FOLLOWING)

        if not due:
            return 0, 0

        follower_ids = self.repository.cached_own_follower_ids()
        if not follower_ids:
            follower_ids = await self._fetch_own_follower_ids(self.settings.own_followers_scan_limit)
        attempted = 0
        verified = 0
        configured_min_gap = float(self.settings.cleanup_unfollow_min_gap_seconds)
        configured_max_gap = float(self.settings.cleanup_unfollow_max_gap_seconds)
        cleanup_min_gap_seconds = min(4.0, max(1.0, configured_min_gap))
        cleanup_max_gap_seconds = min(4.0, max(cleanup_min_gap_seconds, configured_max_gap))

        for row in due:
            self._assert_operator_not_stopped(task_name="cleanup_pipeline")
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

            whitelist_min_followers = max(0, self.settings.cleanup_unfollow_whitelist_min_followers)
            if whitelist_min_followers > 0:
                profile_result = await self._execute_with_retry(
                    "cleanup_target_viewer_edge",
                    operations.user_viewer_edge(user_id),
                )
                follower_count = parse_user_viewer_follower_count(profile_result)
                if follower_count is not None and follower_count >= whitelist_min_followers:
                    if not dry_run:
                        self.repository.mark_cleanup_whitelist_kept(user_id)
                        self.repository.record_action(
                            "cleanup_whitelist_kept",
                            user_id,
                            f"follower_count={follower_count}",
                        )
                    decisions.append(
                        CandidateDecision(
                            user_id=user_id,
                            username=username,
                            eligible=False,
                            reason=f"cleanup:kept_whitelist_follower_count={follower_count}",
                        )
                    )
                    continue

            attempted += 1
            if dry_run:
                await self._sleep_action_gap(
                    action_type=ACTION_UNFOLLOW,
                    target_user_id=user_id,
                    min_gap_seconds=cleanup_min_gap_seconds,
                    max_gap_seconds=cleanup_max_gap_seconds,
                )
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:dry_run_unfollow_nonreciprocal",
                    )
                )
                continue

            await self._sleep_action_gap(
                action_type=ACTION_UNFOLLOW,
                target_user_id=user_id,
                min_gap_seconds=cleanup_min_gap_seconds,
                max_gap_seconds=cleanup_max_gap_seconds,
            )
            mutation = await self._execute_with_retry("cleanup_unfollow", operations.unfollow_user(user_id))
            mutation_ok = mutation.status_code == 200 and not mutation.has_errors
            unfollow_action_key = self._daily_action_key(ACTION_UNFOLLOW, user_id)
            if not mutation_ok:
                self.repository.record_action(
                    ACTION_UNFOLLOW,
                    user_id,
                    "mutation_failed",
                    action_key=unfollow_action_key,
                )
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
            is_following = parse_user_viewer_is_following(verify)
            if is_following is False:
                verified += 1
                self.repository.mark_nonreciprocal_unfollowed(user_id)
                self.repository.record_action(
                    ACTION_UNFOLLOW,
                    user_id,
                    "verified_not_following",
                    action_key=unfollow_action_key,
                )
                self.repository.upsert_relationship_state(
                    user_id,
                    newsletter_state=NewsletterState.UNKNOWN,
                    user_follow_state=UserFollowState.NOT_FOLLOWING,
                    confidence=RelationshipConfidence.OBSERVED,
                    source_operation="UnfollowUserMutation",
                    verified_now=True,
                )
                self.repository.mark_candidate_reconciled(user_id, UserFollowState.NOT_FOLLOWING)
                await self._rollback_public_engagement(user_id=user_id)
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
                self.repository.record_action(
                    ACTION_UNFOLLOW,
                    user_id,
                    "verification_uncertain",
                    action_key=unfollow_action_key,
                )
                decisions.append(
                    CandidateDecision(
                        user_id=user_id,
                        username=username,
                        eligible=False,
                        reason="cleanup:verification_uncertain",
                    )
                )

        return attempted, verified

    async def _rollback_public_engagement(self, *, user_id: str) -> None:
        await self._undo_verified_claps(user_id=user_id)
        await self._delete_verified_highlights(user_id=user_id)
        await self._delete_verified_comments(user_id=user_id)

    async def _undo_verified_claps(self, *, user_id: str) -> None:
        actor_user_id = self.settings.medium_user_ref
        if not actor_user_id:
            return

        for row in self.repository.verified_actions_for_target(
            target_id=user_id,
            action_type=ACTION_CLAP,
            rollback_action_type=ACTION_UNDO_CLAP,
        ):
            post_id = self._action_key_extra(row.get("action_key"))
            clap_count = self._verified_clap_count(row.get("status"))
            if not post_id or clap_count is None or clap_count <= 0:
                continue

            rollback_key = self._rollback_action_key(ACTION_UNDO_CLAP, row.get("action_key"))
            await self._sleep_action_gap(action_type=ACTION_UNDO_CLAP, target_user_id=user_id)
            rollback = await self._execute_with_retry(
                "cleanup_undo_clap",
                operations.undo_clap_post(post_id, actor_user_id, clap_count),
            )
            viewer_clap_count = parse_viewer_clap_count(rollback)
            rollback_ok = rollback.status_code == 200 and not rollback.has_errors
            rollback_verified = rollback_ok and (viewer_clap_count == 0 or viewer_clap_count is None)
            status_label = (
                f"verified:viewer_clap_count={'' if viewer_clap_count is None else viewer_clap_count}"
                if rollback_verified
                else (
                    f"failed:viewer_clap_count={'' if viewer_clap_count is None else viewer_clap_count};"
                    f"requested={clap_count}"
                )
            )
            self.repository.record_action(
                ACTION_UNDO_CLAP,
                user_id,
                status_label,
                action_key=rollback_key,
            )

    async def _delete_verified_comments(self, *, user_id: str) -> None:
        for row in self.repository.verified_actions_for_target(
            target_id=user_id,
            action_type=ACTION_COMMENT,
            rollback_action_type=ACTION_DELETE_COMMENT,
        ):
            comment_id = self._verified_comment_id(row.get("status"))
            if not comment_id:
                continue

            rollback_key = self._rollback_action_key(ACTION_DELETE_COMMENT, row.get("action_key"))
            await self._sleep_action_gap(action_type=ACTION_DELETE_COMMENT, target_user_id=user_id)
            rollback = await self._execute_with_retry(
                "cleanup_delete_comment",
                operations.delete_response(comment_id),
            )
            rollback_ok = rollback.status_code == 200 and not rollback.has_errors
            rollback_deleted = parse_delete_response_success(rollback) is True
            rollback_verified = rollback_ok and rollback_deleted
            status_label = f"verified:{comment_id}" if rollback_verified else f"failed:{comment_id}"
            self.repository.record_action(
                ACTION_DELETE_COMMENT,
                user_id,
                status_label,
                action_key=rollback_key,
            )

    async def _delete_verified_highlights(self, *, user_id: str) -> None:
        for row in self.repository.verified_actions_for_target(
            target_id=user_id,
            action_type=ACTION_HIGHLIGHT,
            rollback_action_type=ACTION_DELETE_HIGHLIGHT,
        ):
            quote_id, post_id = self._verified_highlight_payload(
                status=row.get("status"),
                action_key=row.get("action_key"),
            )
            if not quote_id or not post_id:
                continue

            rollback_key = self._rollback_action_key(ACTION_DELETE_HIGHLIGHT, row.get("action_key"))
            await self._sleep_action_gap(action_type=ACTION_DELETE_HIGHLIGHT, target_user_id=user_id)
            rollback = await self._execute_with_retry(
                "cleanup_delete_highlight",
                operations.delete_quote(target_post_id=post_id, target_quote_id=quote_id),
            )
            rollback_ok = rollback.status_code == 200 and not rollback.has_errors
            rollback_deleted = parse_delete_quote_success(rollback) is True
            rollback_verified = rollback_ok and rollback_deleted
            status_label = (
                f"verified:quote_id={quote_id};post_id={post_id}"
                if rollback_verified
                else f"failed:quote_id={quote_id};post_id={post_id}"
            )
            self.repository.record_action(
                ACTION_DELETE_HIGHLIGHT,
                user_id,
                status_label,
                action_key=rollback_key,
            )

    async def _fetch_own_follower_ids(self, limit: int) -> set[str]:
        if not self.settings.medium_user_ref:
            return set()
        result = await self._execute_with_retry(
            "own_followers_scan",
            operations.user_followers(user_id=self.settings.medium_user_ref, limit=limit),
        )
        users = parse_user_followers_users(result)
        return {item.id for item in users if isinstance(item.id, str)}

    def _following_follower_ratio(self, candidate: CandidateUser) -> float:
        followers = candidate.follower_count or 0
        following = candidate.following_count or 0
        if followers <= 0:
            return 1.0 if following > 0 else 0.0
        return following / followers

    def _match_keywords(self, bio: str | None) -> list[str]:
        return self._match_keywords_in_text(bio)

    def _match_candidate_keywords(self, candidate: CandidateUser) -> list[str]:
        topic_matches = self._candidate_topic_matches(candidate)
        return self._positive_primary_topic_keywords(topic_matches)

    def _match_keywords_in_texts(self, texts: tuple[str | None, ...], keywords: list[str]) -> list[str]:
        matched: list[str] = []
        for text in texts:
            for keyword in self._match_keywords_in_text(text, keywords=keywords):
                if keyword not in matched:
                    matched.append(keyword)
        return matched

    def _match_keywords_in_text(self, text: str | None, *, keywords: list[str] | None = None) -> list[str]:
        if not text:
            return []
        text_tokens = re.findall(r"[a-z0-9]+", text.lower())
        if not text_tokens:
            return []
        token_set = set(text_tokens)
        token_text = " ".join(text_tokens)
        matches: list[str] = []
        for keyword in (self.settings.bio_keywords if keywords is None else keywords):
            keyword_tokens = re.findall(r"[a-z0-9]+", keyword.lower())
            if not keyword_tokens:
                continue
            if len(keyword_tokens) == 1:
                matched = keyword_tokens[0] in token_set
            else:
                keyword_phrase = " ".join(keyword_tokens)
                matched = bool(re.search(rf"(?<!\S){re.escape(keyword_phrase)}(?!\S)", token_text))
            if matched and keyword not in matches:
                matches.append(keyword)
        return matches

    def _resolved_follow_limit_for_cycle(self) -> int:
        raw_limit = (
            self.settings.max_follow_actions_per_run
            if self._session_follow_cap_override is None
            else self._session_follow_cap_override
        )
        return max(0, int(raw_limit))

    def _mutations_enabled_for_cycle(self, *, dry_run: bool) -> bool:
        if dry_run:
            return True
        if self._session_mutations_enabled_override is None:
            return True
        return self._session_mutations_enabled_override

    def _normalize_pacing_configuration(self) -> None:
        if not self.settings.enable_pacing_auto_clamp:
            return
        adjustments: dict[str, dict[str, int]] = {}

        def _clamp_int(name: str, current: int, maximum: int) -> int:
            if current <= maximum:
                return current
            adjustments[name] = {"from": current, "to": maximum}
            return maximum

        min_follows = self.settings.live_session_min_follow_attempts
        target_follows = self.settings.live_session_target_follow_attempts
        if min_follows > target_follows:
            adjustments["LIVE_SESSION_MIN_FOLLOW_ATTEMPTS"] = {"from": min_follows, "to": target_follows}
            self.settings.live_session_min_follow_attempts = target_follows

        follow_per_run = self.settings.max_follow_actions_per_run
        self.settings.max_follow_actions_per_run = _clamp_int(
            "MAX_FOLLOW_ACTIONS_PER_RUN",
            follow_per_run,
            self.settings.live_session_target_follow_attempts,
        )

        session_duration = max(1, self.settings.live_session_duration_minutes)
        follow_hard_cap = max(1, self.settings.live_session_target_follow_attempts)
        follows_per_10 = max(1, math.ceil(follow_hard_cap / max(1.0, session_duration / 10.0)))
        derived_mutation_cap = max(1, follows_per_10 * 2 + 4)
        self.settings.max_mutations_per_10_minutes = _clamp_int(
            "MAX_MUTATIONS_PER_10_MINUTES",
            self.settings.max_mutations_per_10_minutes,
            derived_mutation_cap,
        )
        if adjustments:
            self.log.warning(
                "pacing_config_autoclamp_applied",
                adjustments=adjustments,
                duration_minutes=self.settings.live_session_duration_minutes,
                target_follow_attempts=self.settings.live_session_target_follow_attempts,
            )

    async def _sleep_action_gap(
        self,
        *,
        action_type: str,
        target_user_id: str,
        min_gap_seconds: float | None = None,
        max_gap_seconds: float | None = None,
    ) -> None:
        effective_min_gap = float(self.settings.min_action_gap_seconds) if min_gap_seconds is None else max(0.0, min_gap_seconds)
        effective_max_gap = float(self.settings.max_action_gap_seconds) if max_gap_seconds is None else max(0.0, max_gap_seconds)
        if effective_max_gap < effective_min_gap:
            effective_max_gap = effective_min_gap
        delay = await self.timing.sleep_action_gap(
            min_gap_seconds=effective_min_gap,
            max_gap_seconds=effective_max_gap,
        )
        if delay <= 0:
            return
        self.log.info(
            "action_gap_sleep",
            action_type=action_type,
            target_user_id=target_user_id,
            delay_seconds=round(delay, 3),
            min_gap_seconds=round(effective_min_gap, 3),
            max_gap_seconds=round(effective_max_gap, 3),
            max_mutations_per_10_minutes=self.settings.max_mutations_per_10_minutes,
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

    async def _sleep_verify_gap(self, *, task_name: str, target_id: str | None) -> None:
        delay = await self.timing.sleep_verify_gap()
        if delay <= 0:
            return
        self.log.info(
            "verify_gap_sleep",
            operation=task_name,
            target_id=target_id,
            delay_seconds=round(delay, 3),
            min_verify_gap_seconds=self.settings.min_verify_gap_seconds,
            max_verify_gap_seconds=self.settings.max_verify_gap_seconds,
        )

    async def _sleep_pass_cooldown(self) -> None:
        delay = await self.timing.sleep_pass_cooldown()
        if delay <= 0:
            return
        self.log.info(
            "pass_cooldown_sleep",
            delay_seconds=round(delay, 3),
            min_pass_cooldown_seconds=self.settings.pass_cooldown_min_seconds,
            max_pass_cooldown_seconds=self.settings.pass_cooldown_max_seconds,
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

    def _append_decision(
        self,
        decisions: list[CandidateDecision],
        candidate: CandidateUser,
        *,
        eligible: bool,
        reason: str,
        needs_reconcile: bool = True,
    ) -> None:
        decision = CandidateDecision(
            user_id=candidate.user_id,
            username=candidate.username,
            eligible=eligible,
            reason=reason,
            score=candidate.score,
        )
        decisions.append(decision)
        if self._persist_decision_observations:
            self.repository.upsert_candidate_reconciliation(
                user_id=candidate.user_id,
                username=candidate.username,
                newsletter_v3_id=candidate.newsletter_v3_id,
                source_labels=[source.value for source in candidate.sources],
                score=candidate.score,
                score_breakdown=candidate.score_breakdown,
                decision_reason=reason,
                eligible=eligible,
                needs_reconcile=needs_reconcile,
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
        if (
            reason.startswith("skip:")
            or reason.startswith("cleanup:skip_")
            or reason == "cleanup:kept_followed_back"
            or reason.startswith("cleanup:kept_whitelist")
        ):
            return "skipped"
        if "failed" in reason or "uncertain" in reason or "unavailable" in reason:
            return "failed"
        if reason == "eligible" or reason.startswith("eligible:"):
            return "eligible"
        if reason.startswith("cleanup:"):
            return "cleanup"
        return "info"

    @staticmethod
    def _source_counts(candidates: list[CandidateUser]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for candidate in candidates:
            for source in DailyRunner._candidate_growth_source_values_static(candidate):
                key = source
                counts[key] = counts.get(key, 0) + 1
        return counts

    @staticmethod
    def _candidate_growth_source_values_static(candidate: CandidateUser) -> list[str]:
        mapping = {
            CandidateSource.TOPIC_LATEST_STORIES: GrowthSource.TOPIC_RECOMMENDED.value,
            CandidateSource.TOPIC_WHO_TO_FOLLOW: GrowthSource.TOPIC_RECOMMENDED.value,
            CandidateSource.WHO_TO_FOLLOW_MODULE: GrowthSource.TOPIC_RECOMMENDED.value,
            CandidateSource.TOPIC_CURATED_LIST: GrowthSource.PUBLICATION_ADJACENT.value,
            CandidateSource.SEED_FOLLOWERS: GrowthSource.SEED_FOLLOWERS.value,
            CandidateSource.TARGET_USER_FOLLOWERS: GrowthSource.TARGET_USER_FOLLOWERS.value,
            CandidateSource.POST_RESPONDERS: GrowthSource.RESPONDERS.value,
        }
        values: list[str] = []
        for source in candidate.sources:
            resolved = mapping.get(source)
            if resolved and resolved not in values:
                values.append(resolved)
        return values

    @staticmethod
    def _merge_int_counts(target: dict[str, int], incoming: dict[str, int]) -> None:
        for key, value in incoming.items():
            target[key] = target.get(key, 0) + int(value)

    @staticmethod
    def _append_decision_log_sample(target: list[str], incoming: list[str], *, max_size: int) -> None:
        if len(target) >= max_size:
            return
        remaining = max_size - len(target)
        target.extend(incoming[:remaining])

    def _build_kpis(
        self,
        *,
        follow_attempted: int,
        follow_verified: int,
        cleanup_attempted: int,
        cleanup_verified: int,
        eligible_candidates: int,
        clap_attempted: int,
        clap_verified: int,
        public_touch_attempted: int = 0,
        public_touch_verified: int = 0,
        comment_attempted: int = 0,
        comment_verified: int = 0,
        highlight_attempted: int = 0,
        highlight_verified: int = 0,
    ) -> dict[str, float | int]:
        follow_verify_rate = (follow_verified / follow_attempted) if follow_attempted > 0 else 0.0
        cleanup_verify_rate = (cleanup_verified / cleanup_attempted) if cleanup_attempted > 0 else 0.0
        eligible_conversion_rate = (follow_verified / eligible_candidates) if eligible_candidates > 0 else 0.0
        clap_verify_rate = (clap_verified / clap_attempted) if clap_attempted > 0 else 0.0
        public_touch_verify_rate = (public_touch_verified / public_touch_attempted) if public_touch_attempted > 0 else 0.0
        comment_verify_rate = (comment_verified / comment_attempted) if comment_attempted > 0 else 0.0
        highlight_verify_rate = (highlight_verified / highlight_attempted) if highlight_attempted > 0 else 0.0
        return {
            "follow_verify_rate": round(follow_verify_rate, 4),
            "cleanup_verify_rate": round(cleanup_verify_rate, 4),
            "eligible_conversion_rate": round(eligible_conversion_rate, 4),
            "clap_verify_rate": round(clap_verify_rate, 4),
            "public_touch_verify_rate": round(public_touch_verify_rate, 4),
            "comment_verify_rate": round(comment_verify_rate, 4),
            "highlight_verify_rate": round(highlight_verify_rate, 4),
            "net_follow_delta": follow_verified - cleanup_verified,
        }

    @staticmethod
    def _daily_action_key(action_type: str, user_id: str, *, extra: str | None = None) -> str:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        parts = [action_type, user_id, day, str(time.time_ns())]
        if extra:
            parts.append(extra)
        return ":".join(parts)

    @staticmethod
    def _rollback_action_key(action_type: str, original_action_key: str | None) -> str | None:
        if not original_action_key:
            return None
        return f"{action_type}:{original_action_key}"

    @staticmethod
    def _action_key_extra(action_key: str | None) -> str | None:
        if not action_key:
            return None
        parts = action_key.split(":", 4)
        if len(parts) >= 5:
            return parts[4] or None
        if len(parts) == 4:
            return parts[3] or None
        if len(parts) < 4:
            return None
        return parts[3] or None

    @staticmethod
    def _verified_comment_id(status: str | None) -> str | None:
        if not status or not status.startswith("verified:"):
            return None
        value = status.split(":", 1)[1].strip()
        return value or None

    @staticmethod
    def _verified_highlight_payload(
        *,
        status: str | None,
        action_key: str | None,
    ) -> tuple[str | None, str | None]:
        if not status or not status.startswith("verified:"):
            return None, None
        quote_match = re.search(r"quote_id=([^;]+)", status)
        post_match = re.search(r"post_id=([^;]+)", status)
        quote_id = quote_match.group(1).strip() if quote_match else None
        post_id = post_match.group(1).strip() if post_match else DailyRunner._action_key_extra(action_key)
        return (quote_id or None), (post_id or None)

    @staticmethod
    def _verified_clap_count(status: str | None) -> int | None:
        if not status or not status.startswith("verified:"):
            return None
        match = re.search(r"num_claps=(\d+)", status)
        if not match:
            return None
        return int(match.group(1))

    def _assert_operator_not_stopped(self, *, task_name: str) -> None:
        if not self.settings.operator_kill_switch:
            return
        raise RiskHaltError(
            reason="operator_kill_switch",
            task_name=task_name,
            detail="OPERATOR_KILL_SWITCH=true",
            consecutive_failures=self.risk_guard.consecutive_failures,
        )
