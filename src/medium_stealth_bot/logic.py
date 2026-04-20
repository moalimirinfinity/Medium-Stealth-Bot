import asyncio
import math
import random
import re
import time
from datetime import datetime, timezone

import structlog

from medium_stealth_bot import operations
from medium_stealth_bot.client import MediumAsyncClient
from medium_stealth_bot.graph_sync import GraphSyncService
from medium_stealth_bot.models import (
    CandidateDecision,
    CandidateSource,
    CandidateUser,
    GrowthDiscoveryMode,
    DailyRunOutcome,
    GrowthMode,
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
    parse_delete_response_success,
    parse_latest_post_id,
    parse_publish_threaded_response_id,
    parse_recommended_publishers_users,
    parse_topic_latest_story_creators,
    parse_user_followers_next_from,
    parse_user_followers_users,
    parse_viewer_clap_count,
    parse_user_viewer_follower_count,
    parse_user_viewer_is_following,
)

ACTION_SUBSCRIBE = "follow_subscribe_attempt"
ACTION_UNFOLLOW = "cleanup_unfollow"
ACTION_CLAP = "clap_pre_follow"
ACTION_CLAP_SKIPPED = "clap_pre_follow_skipped"
ACTION_COMMENT = "comment_pre_follow"
ACTION_COMMENT_SKIPPED = "comment_pre_follow_skipped"
ACTION_UNDO_CLAP = "cleanup_undo_clap"
ACTION_DELETE_COMMENT = "cleanup_delete_comment"
ACTION_FOLLOW_VERIFIED = "follow_verified"
TRACKED_DAILY_ACTION_TYPES: tuple[str, ...] = (
    ACTION_SUBSCRIBE,
    ACTION_UNFOLLOW,
    ACTION_CLAP,
    ACTION_COMMENT,
)


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
        self._normalize_pacing_configuration()

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
        growth_mode: GrowthMode | None = None,
        discovery_mode: GrowthDiscoveryMode | None = None,
        target_user_refs: list[str] | None = None,
        target_user_scan_limit: int | None = None,
    ) -> DailyRunOutcome:
        self._assert_operator_not_stopped(task_name="run_daily_cycle")
        resolved_growth_mode = self._resolve_growth_mode(growth_mode)
        resolved_discovery_mode = self._resolve_discovery_mode(discovery_mode)
        resolved_target_user_refs = target_user_refs or []
        resolved_target_user_scan_limit = (
            self._resolve_target_user_scan_limit(target_user_scan_limit)
            if resolved_discovery_mode == GrowthDiscoveryMode.TARGET_USER_FOLLOWERS
            else None
        )
        if resolved_discovery_mode == GrowthDiscoveryMode.TARGET_USER_FOLLOWERS and not resolved_target_user_refs:
            raise ValueError("target_user_refs are required for target-user-followers discovery.")
        if not self._in_live_session:
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
                cleanup_only_mode=False,
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

        probe = (
            await self.probe(tag_slug=tag_slug)
            if resolved_discovery_mode == GrowthDiscoveryMode.GENERAL
            else None
        )

        seed_refs = (
            (seed_user_refs or []) + self.settings.discovery_seed_users
            if resolved_discovery_mode == GrowthDiscoveryMode.GENERAL
            else []
        )
        candidates = await self._build_candidates(
            probe=probe,
            seed_user_refs=seed_refs,
            discovery_mode=resolved_discovery_mode,
            target_user_refs=resolved_target_user_refs,
            target_user_scan_limit=resolved_target_user_scan_limit,
        )
        candidates = candidates[: self._candidate_limit_for_discovery(
            discovery_mode=resolved_discovery_mode,
            target_user_scan_limit=resolved_target_user_scan_limit,
            target_user_refs=resolved_target_user_refs,
        )]

        decisions: list[CandidateDecision] = []
        eligible = await self._evaluate_candidates(
            candidates,
            decisions=decisions,
            persist_observations=not dry_run,
        )

        source_candidate_counts = self._source_counts(candidates)

        remaining_budget = max(0, max_actions - actions_today_start)
        follow_limit_for_cycle = self._resolved_follow_limit_for_cycle()
        mutations_enabled = self._mutations_enabled_for_cycle(dry_run=dry_run)
        if not mutations_enabled and not dry_run:
            self.log.info("pacing_mutations_suspended_for_cycle")
        follow_slots = min(
            follow_limit_for_cycle,
            remaining_budget,
            len(eligible),
            action_remaining[ACTION_SUBSCRIBE],
        )
        if not mutations_enabled and not dry_run:
            follow_slots = 0
        (
            follow_attempted,
            follow_verified,
            clap_attempted,
            clap_verified,
            comment_attempted,
            comment_verified,
            source_follow_verified_counts,
        ) = (
            await self._execute_follow_pipeline(
                eligible_candidates=eligible,
                max_to_run=follow_slots,
                clap_budget_remaining=action_remaining[ACTION_CLAP],
                comment_budget_remaining=action_remaining[ACTION_COMMENT],
                dry_run=dry_run,
                decisions=decisions,
                growth_mode=resolved_growth_mode,
            )
        )
        action_counts[ACTION_SUBSCRIBE] += follow_attempted
        action_counts[ACTION_CLAP] += clap_attempted
        action_counts[ACTION_COMMENT] += comment_attempted
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

        kpis = self._build_kpis(
            follow_attempted=follow_attempted,
            follow_verified=follow_verified,
            cleanup_attempted=cleanup_attempted,
            cleanup_verified=cleanup_verified,
            eligible_candidates=len(eligible),
            clap_attempted=clap_attempted,
            clap_verified=clap_verified,
            comment_attempted=comment_attempted,
            comment_verified=comment_verified,
        )
        kpis.update(self.repository.follow_cycle_kpis())
        kpis.update(self.timing.metrics_snapshot())
        kpis["pacing_mutations_enabled"] = 1 if mutations_enabled else 0
        client_metrics = self.client.metrics_snapshot()

        self.log.info(
            "daily_cycle_complete",
            dry_run=dry_run,
            considered_candidates=len(candidates),
            eligible_candidates=len(eligible),
            follow_attempted=follow_attempted,
            follow_verified=follow_verified,
            clap_attempted=clap_attempted,
            clap_verified=clap_verified,
            comment_attempted=comment_attempted,
            comment_verified=comment_verified,
            cleanup_attempted=cleanup_attempted,
            cleanup_verified=cleanup_verified,
            actions_today=actions_today_end,
            max_actions=max_actions,
            action_counts=action_counts,
            action_limits=action_limits,
            action_remaining=action_remaining,
            source_candidate_counts=source_candidate_counts,
            source_follow_verified_counts=source_follow_verified_counts,
            follow_limit_for_cycle=follow_limit_for_cycle,
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
            growth_mode=resolved_growth_mode,
            discovery_mode=resolved_discovery_mode,
            target_user_refs=resolved_target_user_refs,
            target_user_scan_limit=resolved_target_user_scan_limit,
            action_counts_today=action_counts,
            action_limits_per_day=action_limits,
            action_remaining_per_day=action_remaining,
            dry_run=dry_run,
            considered_candidates=len(candidates),
            eligible_candidates=len(eligible),
            follow_actions_attempted=follow_attempted,
            follow_actions_verified=follow_verified,
            clap_actions_attempted=clap_attempted,
            clap_actions_verified=clap_verified,
            comment_actions_attempted=comment_attempted,
            comment_actions_verified=comment_verified,
            cleanup_actions_attempted=cleanup_attempted,
            cleanup_actions_verified=cleanup_verified,
            source_candidate_counts=source_candidate_counts,
            source_follow_verified_counts=source_follow_verified_counts,
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

    async def run_live_session(
        self,
        *,
        tag_slug: str = "programming",
        seed_user_refs: list[str] | None = None,
        target_follow_attempts: int | None = None,
        max_duration_minutes: int | None = None,
        max_passes: int | None = None,
        growth_mode: GrowthMode | None = None,
        discovery_mode: GrowthDiscoveryMode | None = None,
        target_user_refs: list[str] | None = None,
        target_user_scan_limit: int | None = None,
    ) -> DailyRunOutcome:
        self._assert_operator_not_stopped(task_name="run_live_session")
        resolved_growth_mode = self._resolve_growth_mode(growth_mode)
        resolved_discovery_mode = self._resolve_discovery_mode(discovery_mode)
        resolved_target_user_refs = target_user_refs or []
        resolved_target_user_scan_limit = (
            self._resolve_target_user_scan_limit(target_user_scan_limit)
            if resolved_discovery_mode == GrowthDiscoveryMode.TARGET_USER_FOLLOWERS
            else None
        )
        if resolved_discovery_mode == GrowthDiscoveryMode.TARGET_USER_FOLLOWERS and not resolved_target_user_refs:
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

        total_considered = 0
        total_eligible = 0
        total_follow_attempted = 0
        total_follow_verified = 0
        total_clap_attempted = 0
        total_clap_verified = 0
        total_comment_attempted = 0
        total_comment_verified = 0
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
                    growth_mode=resolved_growth_mode,
                    discovery_mode=resolved_discovery_mode,
                    target_user_refs=resolved_target_user_refs,
                    target_user_scan_limit=resolved_target_user_scan_limit,
                )
                last_outcome = outcome

                total_considered += outcome.considered_candidates
                total_eligible += outcome.eligible_candidates
                total_follow_attempted += outcome.follow_actions_attempted
                total_follow_verified += outcome.follow_actions_verified
                total_clap_attempted += outcome.clap_actions_attempted
                total_clap_verified += outcome.clap_actions_verified
                total_comment_attempted += outcome.comment_actions_attempted
                total_comment_verified += outcome.comment_actions_verified
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
                    comment_attempted_this_pass=outcome.comment_actions_attempted,
                    comment_attempted_total=total_comment_attempted,
                    comment_verified_total=total_comment_verified,
                    cleanup_attempted_this_pass=outcome.cleanup_actions_attempted,
                    cleanup_verified_total=total_cleanup_verified,
                    budget_exhausted=outcome.budget_exhausted,
                )

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
            kpis.update(
                {
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
                growth_mode=resolved_growth_mode,
                discovery_mode=resolved_discovery_mode,
                target_user_refs=resolved_target_user_refs,
                target_user_scan_limit=resolved_target_user_scan_limit,
                action_counts_today=action_counts,
                action_limits_per_day=action_limits,
                action_remaining_per_day=action_remaining,
                dry_run=False,
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
            comment_attempted=total_comment_attempted,
            comment_verified=total_comment_verified,
        )
        kpis.update(self.repository.follow_cycle_kpis())
        kpis.update(
            {
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
            growth_mode=resolved_growth_mode,
            discovery_mode=resolved_discovery_mode,
            target_user_refs=resolved_target_user_refs,
            target_user_scan_limit=resolved_target_user_scan_limit,
            action_counts_today=last_outcome.action_counts_today,
            action_limits_per_day=last_outcome.action_limits_per_day,
            action_remaining_per_day=last_outcome.action_remaining_per_day,
            dry_run=False,
            considered_candidates=total_considered,
            eligible_candidates=total_eligible,
            follow_actions_attempted=total_follow_attempted,
            follow_actions_verified=total_follow_verified,
            clap_actions_attempted=total_clap_attempted,
            clap_actions_verified=total_clap_verified,
            comment_actions_attempted=total_comment_attempted,
            comment_actions_verified=total_comment_verified,
            cleanup_actions_attempted=total_cleanup_attempted,
            cleanup_actions_verified=total_cleanup_verified,
            source_candidate_counts=total_source_candidate_counts,
            source_follow_verified_counts=total_source_follow_verified_counts,
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
            follow_attempted=total_follow_attempted,
            follow_verified=total_follow_verified,
            comment_attempted=total_comment_attempted,
            comment_verified=total_comment_verified,
            follow_target_min=resolved_min_follows,
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
        mutation_tokens = ("mutation", "subscribe", "unfollow", "clap", "comment")
        return any(token in lowered for token in mutation_tokens)

    def _retry_budget_for_task(self, task_name: str) -> int:
        lowered = task_name.lower()
        if any(token in lowered for token in ("mutation", "subscribe", "unfollow", "clap", "comment")):
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
        probe: ProbeSnapshot | None,
        seed_user_refs: list[str],
        discovery_mode: GrowthDiscoveryMode,
        target_user_refs: list[str],
        target_user_scan_limit: int | None,
    ) -> list[CandidateUser]:
        pool: dict[str, CandidateUser] = {}

        if discovery_mode == GrowthDiscoveryMode.GENERAL:
            if probe is not None:
                self._extract_topic_latest_candidates(probe, pool)
                self._extract_topic_who_to_follow_candidates(probe, pool)
                self._extract_who_to_follow_module_candidates(probe, pool)
            await self._extract_seed_followers_candidates(seed_user_refs, pool)
        else:
            await self._extract_target_user_followers_candidates(
                target_user_refs=target_user_refs,
                pool=pool,
                per_target_scan_limit=self._resolve_target_user_scan_limit(target_user_scan_limit),
            )

        for candidate in pool.values():
            candidate.matched_keywords = self._match_keywords(candidate.bio)
            ratio = self._following_follower_ratio(candidate)
            keyword_bonus = self.settings.score_weight_keyword * len(candidate.matched_keywords)
            source_bonus = self.settings.score_weight_source * len(candidate.sources)
            newsletter_bonus = self.settings.score_weight_newsletter if candidate.newsletter_v3_id else 0.0
            candidate.score = (
                (self.settings.score_weight_ratio * ratio)
                + keyword_bonus
                + source_bonus
                + newsletter_bonus
            )

        ordered = sorted(pool.values(), key=lambda item: item.score, reverse=True)
        self.log.info(
            "candidates_built",
            count=len(ordered),
            discovery_mode=discovery_mode.value,
            seed_sources=len(seed_user_refs),
            target_sources=len(target_user_refs),
            target_user_scan_limit=target_user_scan_limit,
        )
        return ordered

    def _extract_topic_latest_candidates(self, probe: ProbeSnapshot, pool: dict[str, CandidateUser]) -> None:
        result = probe.results.get("topic_latest_stories")
        if not result:
            return
        for creator, latest_post_id in parse_topic_latest_story_creators(result):
            candidate = self._candidate_from_user_node(
                creator,
                source=CandidateSource.TOPIC_LATEST_STORIES,
                latest_post_id=latest_post_id,
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
            first_hop_users = parse_user_followers_users(result)
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
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason=f"skip:ratio_below_threshold ratio={ratio:.2f}",
                )
                continue

            if self.settings.require_bio_keyword_match and not candidate.matched_keywords:
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:no_keyword_match",
                )
                continue

            if not candidate.newsletter_v3_id:
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:no_newsletter_v3_id",
                )
                continue

            if self.repository.is_blacklisted(candidate.user_id):
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
                action_types=(ACTION_SUBSCRIBE, ACTION_FOLLOW_VERIFIED, ACTION_UNFOLLOW),
            ):
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:cooldown_active",
                )
                continue

            local_state = self.repository.get_relationship_state(candidate.user_id)
            if local_state and local_state.user_follow_state == UserFollowState.FOLLOWING:
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
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:already_following_live_check",
                    needs_reconcile=False,
                )
                continue
            if is_following is None:
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=False,
                    reason="skip:live_check_unavailable",
                )
                continue

            eligible.append(candidate)
            self._append_decision(
                decisions,
                candidate,
                eligible=True,
                reason="eligible",
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
        growth_mode: GrowthMode,
    ) -> tuple[int, int, int, int, int, int, dict[str, int]]:
        attempted = 0
        verified = 0
        clap_attempted = 0
        clap_verified = 0
        comment_attempted = 0
        comment_verified = 0
        source_follow_verified_counts: dict[str, int] = {}
        clap_enabled = self._pre_follow_clap_enabled(growth_mode)
        comment_enabled = self._pre_follow_comment_enabled(growth_mode)
        for candidate in eligible_candidates[:max_to_run]:
            self._assert_operator_not_stopped(task_name="follow_pipeline")
            attempted += 1

            if dry_run:
                if clap_enabled and clap_budget_remaining > 0:
                    clap_budget_remaining -= 1
                    clap_attempted += 1
                    await self._sleep_action_gap(action_type=ACTION_CLAP, target_user_id=candidate.user_id)
                if (
                    comment_enabled
                    and comment_budget_remaining > 0
                    and self.settings.pre_follow_comment_templates
                    and self._should_attempt_pre_follow_comment()
                ):
                    comment_budget_remaining -= 1
                    comment_attempted += 1
                    await self._sleep_action_gap(action_type=ACTION_COMMENT, target_user_id=candidate.user_id)
                await self._sleep_action_gap(action_type=ACTION_SUBSCRIBE, target_user_id=candidate.user_id)
                self._append_decision(
                    decisions,
                    candidate,
                    eligible=True,
                    reason="dry_run:planned_follow",
                )
                continue

            self.repository.upsert_user_profile(
                candidate.user_id,
                username=candidate.username,
                name=candidate.name,
                follower_count=candidate.follower_count,
                following_count=candidate.following_count,
                newsletter_id=candidate.newsletter_v3_id,
                bio=candidate.bio,
            )

            (
                clap_used,
                clap_is_verified,
                comment_used,
                comment_is_verified,
            ) = await self._execute_pre_follow_engagement(
                candidate,
                growth_mode=growth_mode,
                clap_budget_remaining=clap_budget_remaining,
                comment_budget_remaining=comment_budget_remaining,
            )
            if clap_used:
                clap_budget_remaining -= 1
                clap_attempted += 1
            if clap_is_verified:
                clap_verified += 1
            if comment_used:
                comment_budget_remaining -= 1
                comment_attempted += 1
            if comment_is_verified:
                comment_verified += 1

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
                    source="SubscribeNewsletterV3Mutation",
                    grace_days=self.settings.unfollow_nonreciprocal_after_days,
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
                for source in candidate.sources:
                    key = source.value
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

        return (
            attempted,
            verified,
            clap_attempted,
            clap_verified,
            comment_attempted,
            comment_verified,
            source_follow_verified_counts,
        )

    async def _execute_pre_follow_engagement(
        self,
        candidate: CandidateUser,
        *,
        growth_mode: GrowthMode,
        clap_budget_remaining: int,
        comment_budget_remaining: int,
    ) -> tuple[bool, bool, bool, bool]:
        clap_enabled = self._pre_follow_clap_enabled(growth_mode)
        comment_enabled = self._pre_follow_comment_enabled(growth_mode)
        if not clap_enabled and not comment_enabled:
            return False, False, False, False

        clap_should_attempt = False
        comment_should_attempt = False
        comment_text: str | None = None

        if clap_enabled:
            if clap_budget_remaining <= 0:
                self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "budget_exhausted")
            elif not self.settings.medium_user_ref:
                self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "missing_actor_user_id")
            else:
                clap_should_attempt = True

        if comment_enabled:
            if comment_budget_remaining <= 0:
                self.repository.record_action(ACTION_COMMENT_SKIPPED, candidate.user_id, "budget_exhausted")
            elif not self._should_attempt_pre_follow_comment():
                self.repository.record_action(ACTION_COMMENT_SKIPPED, candidate.user_id, "probability_gate")
            else:
                comment_text = self._select_pre_follow_comment_text()
                if not comment_text:
                    self.repository.record_action(ACTION_COMMENT_SKIPPED, candidate.user_id, "no_template")
                else:
                    comment_should_attempt = True

        if not clap_should_attempt and not comment_should_attempt:
            return False, False, False, False

        post_id = await self._resolve_candidate_latest_post_id(candidate)
        if not post_id:
            if clap_should_attempt:
                self.repository.record_action(ACTION_CLAP_SKIPPED, candidate.user_id, "no_post")
            if comment_should_attempt:
                self.repository.record_action(ACTION_COMMENT_SKIPPED, candidate.user_id, "no_post")
            return False, False, False, False

        if self.settings.pre_follow_read_wait_seconds > 0:
            await self._sleep_read_delay(target_user_id=candidate.user_id)

        clap_verified = False
        comment_verified = False
        if clap_should_attempt:
            clap_verified = await self._perform_pre_follow_clap(candidate, post_id=post_id)
        if comment_should_attempt and comment_text:
            comment_verified = await self._perform_pre_follow_comment(
                candidate,
                post_id=post_id,
                comment_text=comment_text,
            )
        return clap_should_attempt, clap_verified, comment_should_attempt, comment_verified

    async def _resolve_candidate_latest_post_id(self, candidate: CandidateUser) -> str | None:
        post_id = candidate.latest_post_id
        if post_id:
            return post_id
        latest_post = await self._execute_with_retry(
            "pre_follow_latest_post",
            operations.user_latest_post(user_id=candidate.user_id, username=candidate.username),
        )
        post_id = parse_latest_post_id(latest_post)
        if post_id:
            candidate.latest_post_id = post_id
        return post_id

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
        comment_verified = comment_result.status_code == 200 and not comment_result.has_errors and bool(comment_id)
        comment_action_key = self._daily_action_key(ACTION_COMMENT, candidate.user_id, extra=post_id)
        status_label = f"verified:{comment_id}" if comment_verified else "failed"
        self.repository.record_action(
            ACTION_COMMENT,
            candidate.user_id,
            status_label,
            action_key=comment_action_key,
        )
        return comment_verified

    def _resolve_growth_mode(self, growth_mode: GrowthMode | None) -> GrowthMode:
        return growth_mode or self.settings.default_growth_mode

    @staticmethod
    def _resolve_discovery_mode(discovery_mode: GrowthDiscoveryMode | None) -> GrowthDiscoveryMode:
        return discovery_mode or GrowthDiscoveryMode.GENERAL

    def _resolve_target_user_scan_limit(self, target_user_scan_limit: int | None) -> int:
        resolved = target_user_scan_limit or self.settings.target_user_followers_scan_limit
        return max(1, resolved)

    def _candidate_limit_for_discovery(
        self,
        *,
        discovery_mode: GrowthDiscoveryMode,
        target_user_scan_limit: int | None,
        target_user_refs: list[str],
    ) -> int:
        if discovery_mode == GrowthDiscoveryMode.TARGET_USER_FOLLOWERS:
            return self._resolve_target_user_scan_limit(target_user_scan_limit) * max(1, len(target_user_refs))
        return self.settings.follow_candidate_limit

    @staticmethod
    def _smart_growth_enabled(growth_mode: GrowthMode) -> bool:
        return growth_mode == GrowthMode.SMART

    def _pre_follow_clap_enabled(self, growth_mode: GrowthMode) -> bool:
        return self._smart_growth_enabled(growth_mode) and self.settings.enable_pre_follow_clap

    def _pre_follow_comment_enabled(self, growth_mode: GrowthMode) -> bool:
        return self._smart_growth_enabled(growth_mode) and self.settings.enable_pre_follow_comment

    def _should_attempt_pre_follow_comment(self) -> bool:
        return random.random() < self.settings.pre_follow_comment_probability

    def _select_pre_follow_comment_text(self) -> str | None:
        templates = self.settings.pre_follow_comment_templates
        if not templates:
            return None
        return random.choice(templates)

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
        if not bio:
            return []
        normalized = bio.lower()
        return [keyword for keyword in self.settings.bio_keywords if keyword in normalized]

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
        self.repository.upsert_candidate_reconciliation(
            user_id=candidate.user_id,
            username=candidate.username,
            newsletter_v3_id=candidate.newsletter_v3_id,
            source_labels=[source.value for source in candidate.sources],
            score=candidate.score,
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
        if reason == "eligible":
            return "eligible"
        if reason.startswith("cleanup:"):
            return "cleanup"
        return "info"

    @staticmethod
    def _source_counts(candidates: list[CandidateUser]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for candidate in candidates:
            for source in candidate.sources:
                key = source.value
                counts[key] = counts.get(key, 0) + 1
        return counts

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
        comment_attempted: int,
        comment_verified: int,
    ) -> dict[str, float | int]:
        follow_verify_rate = (follow_verified / follow_attempted) if follow_attempted > 0 else 0.0
        cleanup_verify_rate = (cleanup_verified / cleanup_attempted) if cleanup_attempted > 0 else 0.0
        eligible_conversion_rate = (follow_verified / eligible_candidates) if eligible_candidates > 0 else 0.0
        clap_verify_rate = (clap_verified / clap_attempted) if clap_attempted > 0 else 0.0
        comment_verify_rate = (comment_verified / comment_attempted) if comment_attempted > 0 else 0.0
        return {
            "follow_verify_rate": round(follow_verify_rate, 4),
            "cleanup_verify_rate": round(cleanup_verify_rate, 4),
            "eligible_conversion_rate": round(eligible_conversion_rate, 4),
            "clap_verify_rate": round(clap_verify_rate, 4),
            "comment_verify_rate": round(comment_verify_rate, 4),
            "net_follow_delta": follow_verified - cleanup_verified,
        }

    @staticmethod
    def _daily_action_key(action_type: str, user_id: str, *, extra: str | None = None) -> str:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        parts = [action_type, user_id, day]
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
        parts = action_key.split(":", 3)
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
