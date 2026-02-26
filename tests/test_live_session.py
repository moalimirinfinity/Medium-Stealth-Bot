import asyncio
from pathlib import Path

from medium_stealth_bot.database import Database
from medium_stealth_bot.logic import DailyRunner
from medium_stealth_bot.models import DailyRunOutcome
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.settings import AppSettings


class StubClient:
    def metrics_snapshot(self):
        return {
            "mode": "fixture",
            "request_count": 0,
            "avg_latency_ms": 0.0,
            "status_counts": {},
            "result_failures": 0,
        }


def _cycle_outcome(*, actions_today: int, follow_attempted: int, follow_verified: int) -> DailyRunOutcome:
    return DailyRunOutcome(
        budget_exhausted=False,
        actions_today=actions_today,
        max_actions_per_day=500,
        action_counts_today={
            "follow_subscribe_attempt": actions_today,
            "cleanup_unfollow": 0,
            "clap_pre_follow": actions_today,
        },
        action_limits_per_day={
            "follow_subscribe_attempt": 500,
            "cleanup_unfollow": 100,
            "clap_pre_follow": 500,
        },
        action_remaining_per_day={
            "follow_subscribe_attempt": max(0, 500 - actions_today),
            "cleanup_unfollow": 100,
            "clap_pre_follow": max(0, 500 - actions_today),
        },
        dry_run=False,
        considered_candidates=20,
        eligible_candidates=10,
        follow_actions_attempted=follow_attempted,
        follow_actions_verified=follow_verified,
        clap_actions_attempted=follow_attempted,
        clap_actions_verified=follow_attempted,
        cleanup_actions_attempted=0,
        cleanup_actions_verified=0,
        source_candidate_counts={"topic_latest_stories": 20},
        source_follow_verified_counts={"topic_latest_stories": follow_verified},
        decision_log=[f"follow_success:verified_following (id=user-{actions_today})"],
        decision_reason_counts={"follow_success:verified_following": follow_verified},
        decision_result_counts={"success": follow_verified},
        client_metrics={
            "mode": "fixture",
            "request_count": 0,
            "avg_latency_ms": 0.0,
            "status_counts": {},
            "result_failures": 0,
        },
    )


def test_live_session_stops_when_follow_target_reached(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        LIVE_SESSION_DURATION_MINUTES=60,
        LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS=5,
        LIVE_SESSION_MAX_PASSES=8,
    )
    database = Database(tmp_path / "live-session.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    planned = [
        _cycle_outcome(actions_today=3, follow_attempted=3, follow_verified=3),
        _cycle_outcome(actions_today=5, follow_attempted=2, follow_verified=2),
    ]

    async def fake_run_daily_cycle(*, tag_slug: str, dry_run: bool, seed_user_refs):
        assert dry_run is False
        assert tag_slug == "programming"
        return planned.pop(0)

    monkeypatch.setattr(runner, "run_daily_cycle", fake_run_daily_cycle)

    outcome = asyncio.run(runner.run_live_session(tag_slug="programming", seed_user_refs=None))

    assert outcome.follow_actions_attempted == 5
    assert outcome.follow_actions_verified == 5
    assert outcome.clap_actions_attempted == 5
    assert outcome.session_passes == 2
    assert outcome.session_stop_reason == "follow_target_reached"
    assert outcome.session_target_follow_attempts == 5
    assert outcome.session_target_duration_minutes == 60
