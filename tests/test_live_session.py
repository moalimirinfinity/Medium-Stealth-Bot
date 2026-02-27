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
        PASS_COOLDOWN_MIN_SECONDS=0,
        PASS_COOLDOWN_MAX_SECONDS=0,
        PACING_SOFT_DEGRADE_COOLDOWN_SECONDS=0,
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


def test_live_session_no_action_progress_runs_until_max_passes(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        LIVE_SESSION_DURATION_MINUTES=60,
        LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS=100,
        LIVE_SESSION_MAX_PASSES=3,
        MAX_FOLLOW_ACTIONS_PER_RUN=100,
        PASS_COOLDOWN_MIN_SECONDS=0,
        PASS_COOLDOWN_MAX_SECONDS=0,
        PACING_SOFT_DEGRADE_COOLDOWN_SECONDS=0,
    )
    database = Database(tmp_path / "live-session-no-progress.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    async def fake_run_daily_cycle(*, tag_slug: str, dry_run: bool, seed_user_refs):
        assert dry_run is False
        assert tag_slug == "programming"
        return _cycle_outcome(actions_today=0, follow_attempted=0, follow_verified=0)

    monkeypatch.setattr(runner, "run_daily_cycle", fake_run_daily_cycle)

    outcome = asyncio.run(runner.run_live_session(tag_slug="programming", seed_user_refs=None))

    assert outcome.follow_actions_attempted == 0
    assert outcome.session_passes == 3
    assert outcome.session_stop_reason == "max_passes_reached"


def test_live_session_respects_configured_max_passes_cap(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        LIVE_SESSION_DURATION_MINUTES=60,
        LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS=100,
        LIVE_SESSION_MIN_FOLLOW_ATTEMPTS=1,
        LIVE_SESSION_MAX_PASSES=2,
        MAX_FOLLOW_ACTIONS_PER_RUN=10,
        PASS_COOLDOWN_MIN_SECONDS=0,
        PASS_COOLDOWN_MAX_SECONDS=0,
        PACING_SOFT_DEGRADE_COOLDOWN_SECONDS=0,
    )
    database = Database(tmp_path / "live-session-pass-cap.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    actions_today = 0

    async def fake_run_daily_cycle(*, tag_slug: str, dry_run: bool, seed_user_refs):
        nonlocal actions_today
        assert dry_run is False
        assert tag_slug == "programming"
        follows = max(0, int(runner._session_follow_cap_override or 0))
        actions_today += follows
        return _cycle_outcome(actions_today=actions_today, follow_attempted=follows, follow_verified=follows)

    monkeypatch.setattr(runner, "run_daily_cycle", fake_run_daily_cycle)
    outcome = asyncio.run(runner.run_live_session(tag_slug="programming", seed_user_refs=None))

    assert outcome.session_passes == 2
    assert outcome.session_stop_reason == "max_passes_reached"
    assert outcome.follow_actions_attempted == 20
    assert outcome.follow_actions_attempted < 100


def test_live_session_hard_cap_is_never_exceeded(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        LIVE_SESSION_DURATION_MINUTES=120,
        LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS=120,
        LIVE_SESSION_MIN_FOLLOW_ATTEMPTS=80,
        LIVE_SESSION_MAX_PASSES=3,
        MAX_FOLLOW_ACTIONS_PER_RUN=100,
        PASS_COOLDOWN_MIN_SECONDS=0,
        PASS_COOLDOWN_MAX_SECONDS=0,
        PACING_SOFT_DEGRADE_COOLDOWN_SECONDS=0,
    )
    database = Database(tmp_path / "live-session-hard-cap.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    actions_today = 0

    async def fake_run_daily_cycle(*, tag_slug: str, dry_run: bool, seed_user_refs):
        nonlocal actions_today
        assert dry_run is False
        assert tag_slug == "programming"
        follows = max(0, int(runner._session_follow_cap_override or 0))
        actions_today += follows
        return _cycle_outcome(actions_today=actions_today, follow_attempted=follows, follow_verified=follows)

    monkeypatch.setattr(runner, "run_daily_cycle", fake_run_daily_cycle)
    outcome = asyncio.run(runner.run_live_session(tag_slug="programming", seed_user_refs=None))

    assert outcome.follow_actions_attempted == 120
    assert outcome.follow_actions_attempted <= 120
    assert outcome.session_stop_reason == "follow_target_reached"


def test_live_session_soft_floor_can_be_met_without_hard_cap(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        LIVE_SESSION_DURATION_MINUTES=120,
        LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS=120,
        LIVE_SESSION_MIN_FOLLOW_ATTEMPTS=80,
        LIVE_SESSION_MAX_PASSES=2,
        MAX_FOLLOW_ACTIONS_PER_RUN=40,
        PASS_COOLDOWN_MIN_SECONDS=0,
        PASS_COOLDOWN_MAX_SECONDS=0,
        PACING_SOFT_DEGRADE_COOLDOWN_SECONDS=0,
    )
    database = Database(tmp_path / "live-session-soft-floor.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    actions_today = 0

    async def fake_run_daily_cycle(*, tag_slug: str, dry_run: bool, seed_user_refs):
        nonlocal actions_today
        follows = 0
        if actions_today < 80:
            follows = max(0, int(runner._session_follow_cap_override or 0))
            follows = min(follows, 80 - actions_today)
        actions_today += follows
        return _cycle_outcome(actions_today=actions_today, follow_attempted=follows, follow_verified=follows)

    monkeypatch.setattr(runner, "run_daily_cycle", fake_run_daily_cycle)
    outcome = asyncio.run(runner.run_live_session(tag_slug="programming", seed_user_refs=None))

    assert outcome.follow_actions_attempted == 80
    assert outcome.kpis.get("session_soft_floor_met") == 1
    assert outcome.kpis.get("session_soft_floor_remaining") == 0
    assert outcome.session_stop_reason == "max_passes_reached"


def test_live_session_soft_degrade_disables_mutations_for_cooldown(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        LIVE_SESSION_DURATION_MINUTES=60,
        LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS=30,
        LIVE_SESSION_MIN_FOLLOW_ATTEMPTS=10,
        LIVE_SESSION_MAX_PASSES=2,
        MAX_FOLLOW_ACTIONS_PER_RUN=30,
        PASS_COOLDOWN_MIN_SECONDS=0,
        PASS_COOLDOWN_MAX_SECONDS=0,
        PACING_SOFT_DEGRADE_COOLDOWN_SECONDS=120,
    )
    database = Database(tmp_path / "live-session-soft-degrade.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    actions_today = 0

    async def fake_run_daily_cycle(*, tag_slug: str, dry_run: bool, seed_user_refs):
        nonlocal actions_today
        if runner._session_mutations_enabled_override is False:
            return _cycle_outcome(actions_today=actions_today, follow_attempted=0, follow_verified=0)
        follows = max(0, int(runner._session_follow_cap_override or 0))
        actions_today += follows
        return _cycle_outcome(actions_today=actions_today, follow_attempted=follows, follow_verified=follows)

    monkeypatch.setattr(runner, "run_daily_cycle", fake_run_daily_cycle)
    outcome = asyncio.run(runner.run_live_session(tag_slug="programming", seed_user_refs=None))

    assert outcome.follow_actions_attempted > 0
    assert outcome.kpis.get("session_pacing_degrade_events", 0) >= 1
    assert outcome.kpis.get("session_mutation_suspended_seconds_total", 0) > 0


def test_pacing_autoclamp_reduces_aggressive_values(tmp_path: Path) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        LIVE_SESSION_DURATION_MINUTES=120,
        LIVE_SESSION_TARGET_FOLLOW_ATTEMPTS=120,
        LIVE_SESSION_MIN_FOLLOW_ATTEMPTS=180,
        MAX_FOLLOW_ACTIONS_PER_RUN=500,
        MAX_MUTATIONS_PER_10_MINUTES=500,
        ENABLE_PACING_AUTO_CLAMP=True,
    )
    database = Database(tmp_path / "live-session-autoclamp.db")
    database.initialize()
    repository = ActionRepository(database)
    DailyRunner(settings=settings, client=StubClient(), repository=repository)

    # hard target ceiling
    assert settings.live_session_min_follow_attempts == 120
    assert settings.max_follow_actions_per_run == 120
    # derived cap for 120 follows / 120 minutes -> 10 follows / 10 minutes => (10 * 2) + 4
    assert settings.max_mutations_per_10_minutes == 24
