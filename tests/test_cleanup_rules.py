import asyncio
from pathlib import Path

from medium_stealth_bot.database import Database
from medium_stealth_bot.logic import DailyRunner
from medium_stealth_bot.models import CandidateDecision, GraphQLResult
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


def test_pending_nonreciprocal_candidates_treats_missing_followed_at_as_overdue(tmp_path: Path) -> None:
    database = Database(tmp_path / "cleanup.db")
    database.initialize()
    repository = ActionRepository(database)

    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO follow_cycle (
                user_id, username, followed_at, follow_source, follow_deadline_at, cleanup_status, updated_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)
            """,
            ("user-missing-followed-at", "ghost", "", "legacy_import", None),
        )
        connection.execute(
            """
            INSERT INTO follow_cycle (
                user_id, username, followed_at, follow_source, follow_deadline_at, cleanup_status, updated_at
            )
            VALUES (?, ?, ?, ?, datetime('now', 'utc', '+30 day'), 'pending', CURRENT_TIMESTAMP)
            """,
            ("user-missing-followed-at-future-deadline", "ghost2", "", "legacy_import"),
        )
        connection.commit()

    due = repository.pending_nonreciprocal_candidates(grace_days=7, limit=10)
    due_ids = {row["user_id"] for row in due}
    assert "user-missing-followed-at" in due_ids
    assert "user-missing-followed-at-future-deadline" in due_ids


def test_cleanup_pipeline_keeps_users_above_whitelist_threshold(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS=2000,
    )
    database = Database(tmp_path / "cleanup-whitelist.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    run_id = repository.begin_graph_sync_run(mode="manual")
    repository.replace_own_following_snapshot(
        [{"user_id": "user-high-followers", "username": "high-follower"}],
        run_id=run_id,
    )
    repository.complete_graph_sync_run(run_id, status="success")

    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO follow_cycle (
                user_id, username, followed_at, follow_source, follow_deadline_at, cleanup_status, updated_at
            )
            VALUES (?, ?, datetime('now', 'utc', '-30 day'), ?, datetime('now', 'utc', '-7 day'), 'pending', CURRENT_TIMESTAMP)
            """,
            ("user-high-followers", "high-follower", "seed_follow"),
        )
        connection.commit()

    async def fake_fetch_own_follower_ids(limit: int) -> set[str]:
        return set()

    async def fake_execute_with_retry(task_name: str, operation) -> GraphQLResult:
        if task_name == "cleanup_target_viewer_edge":
            return GraphQLResult(
                operationName="UserViewerEdge",
                statusCode=200,
                data={
                    "user": {
                        "id": "user-high-followers",
                        "socialStats": {"followerCount": 2501},
                        "viewerEdge": {"isFollowing": True},
                    }
                },
                errors=[],
                raw={},
            )
        raise AssertionError(f"Unexpected operation call: {task_name}")

    monkeypatch.setattr(runner, "_fetch_own_follower_ids", fake_fetch_own_follower_ids)
    monkeypatch.setattr(runner, "_execute_with_retry", fake_execute_with_retry)

    decisions: list[CandidateDecision] = []
    attempted, verified = asyncio.run(
        runner._execute_cleanup_pipeline(
            dry_run=False,
            max_to_run=5,
            decisions=decisions,
        )
    )

    assert attempted == 0
    assert verified == 0
    assert any(item.reason.startswith("cleanup:kept_whitelist") for item in decisions)

    with database.connect() as connection:
        row = connection.execute(
            "SELECT cleanup_status FROM follow_cycle WHERE user_id = ?",
            ("user-high-followers",),
        ).fetchone()
        assert row is not None
        assert row["cleanup_status"] == "kept_whitelist"


def test_cleanup_pipeline_uses_cached_followers_before_live_fetch(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS=2000,
    )
    database = Database(tmp_path / "cleanup-cache.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    run_id = repository.begin_graph_sync_run(mode="manual")
    repository.replace_own_following_snapshot(
        [{"user_id": "user-followed-back", "username": "cached-follower"}],
        run_id=run_id,
    )
    repository.replace_own_followers_snapshot(
        [{"user_id": "user-followed-back", "username": "cached-follower"}],
        run_id=run_id,
    )
    repository.complete_graph_sync_run(run_id, status="success")

    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO follow_cycle (
                user_id, username, followed_at, follow_source, follow_deadline_at, cleanup_status, updated_at
            )
            VALUES (?, ?, datetime('now', 'utc', '-30 day'), ?, datetime('now', 'utc', '-7 day'), 'pending', CURRENT_TIMESTAMP)
            """,
            ("user-followed-back", "cached-follower", "seed_follow"),
        )
        connection.commit()

    async def fail_fetch_own_follower_ids(limit: int) -> set[str]:  # pragma: no cover - assertion path
        raise AssertionError("fallback fetch should not be used when cache is populated")

    async def fail_execute_with_retry(task_name: str, operation) -> GraphQLResult:  # pragma: no cover - assertion path
        raise AssertionError(f"no live calls expected, got {task_name}")

    monkeypatch.setattr(runner, "_fetch_own_follower_ids", fail_fetch_own_follower_ids)
    monkeypatch.setattr(runner, "_execute_with_retry", fail_execute_with_retry)

    decisions: list[CandidateDecision] = []
    attempted, verified = asyncio.run(
        runner._execute_cleanup_pipeline(
            dry_run=False,
            max_to_run=5,
            decisions=decisions,
        )
    )

    assert attempted == 0
    assert verified == 0
    assert any(item.reason == "cleanup:kept_followed_back" for item in decisions)

    with database.connect() as connection:
        row = connection.execute(
            "SELECT cleanup_status FROM follow_cycle WHERE user_id = ?",
            ("user-followed-back",),
        ).fetchone()
        assert row is not None
        assert row["cleanup_status"] == "followed_back"


def test_cleanup_pipeline_skips_rows_missing_from_following_cache(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
    )
    database = Database(tmp_path / "cleanup-skip-cache.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    run_id = repository.begin_graph_sync_run(mode="manual")
    repository.replace_own_following_snapshot(
        [{"user_id": "still-following", "username": "active"}],
        run_id=run_id,
    )
    repository.replace_own_followers_snapshot(
        [{"user_id": "still-following", "username": "active"}],
        run_id=run_id,
    )
    repository.complete_graph_sync_run(run_id, status="success")

    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO follow_cycle (
                user_id, username, followed_at, follow_source, follow_deadline_at, cleanup_status, updated_at
            )
            VALUES (?, ?, datetime('now', 'utc', '-30 day'), ?, datetime('now', 'utc', '-7 day'), 'pending', CURRENT_TIMESTAMP)
            """,
            ("not-in-following-cache", "ghost", "seed_follow"),
        )
        connection.commit()

    async def fail_fetch_own_follower_ids(limit: int) -> set[str]:  # pragma: no cover - assertion path
        raise AssertionError("no follower fetch expected")

    async def fail_execute_with_retry(task_name: str, operation) -> GraphQLResult:  # pragma: no cover - assertion path
        raise AssertionError(f"no live calls expected, got {task_name}")

    monkeypatch.setattr(runner, "_fetch_own_follower_ids", fail_fetch_own_follower_ids)
    monkeypatch.setattr(runner, "_execute_with_retry", fail_execute_with_retry)

    decisions: list[CandidateDecision] = []
    attempted, verified = asyncio.run(
        runner._execute_cleanup_pipeline(
            dry_run=False,
            max_to_run=3,
            decisions=decisions,
        )
    )

    assert attempted == 0
    assert verified == 0
    assert any(item.reason == "cleanup:skip_not_in_following_cache" for item in decisions)

    with database.connect() as connection:
        row = connection.execute(
            "SELECT cleanup_status FROM follow_cycle WHERE user_id = ?",
            ("not-in-following-cache",),
        ).fetchone()
        assert row is not None
        assert row["cleanup_status"] == "skipped"


def test_cleanup_pipeline_clamps_unfollow_gap_to_one_to_four_seconds(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        CLEANUP_UNFOLLOW_MIN_GAP_SECONDS=0,
        CLEANUP_UNFOLLOW_MAX_GAP_SECONDS=12,
        CLEANUP_UNFOLLOW_WHITELIST_MIN_FOLLOWERS=0,
    )
    database = Database(tmp_path / "cleanup-gap.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    run_id = repository.begin_graph_sync_run(mode="manual")
    repository.replace_own_following_snapshot(
        [{"user_id": "target-user", "username": "target"}],
        run_id=run_id,
    )
    repository.complete_graph_sync_run(run_id, status="success")

    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO follow_cycle (
                user_id, username, followed_at, follow_source, follow_deadline_at, cleanup_status, updated_at
            )
            VALUES (?, ?, datetime('now', 'utc', '-30 day'), ?, datetime('now', 'utc', '-7 day'), 'pending', CURRENT_TIMESTAMP)
            """,
            ("target-user", "target", "seed_follow"),
        )
        connection.commit()

    async def fake_fetch_own_follower_ids(limit: int) -> set[str]:
        return set()

    async def fake_execute_with_retry(task_name: str, operation) -> GraphQLResult:
        if task_name == "cleanup_unfollow":
            return GraphQLResult(
                operationName="UnfollowUserMutation",
                statusCode=200,
                data={"unfollowUser": {"id": "target-user"}},
                errors=[],
                raw={},
            )
        if task_name == "cleanup_verify":
            return GraphQLResult(
                operationName="UserViewerEdge",
                statusCode=200,
                data={"user": {"viewerEdge": {"isFollowing": False}}},
                errors=[],
                raw={},
            )
        raise AssertionError(f"Unexpected operation call: {task_name}")

    captured: dict[str, float] = {}

    async def fake_sleep_action_gap(
        *,
        action_type: str,
        target_user_id: str,
        min_gap_seconds: float | None = None,
        max_gap_seconds: float | None = None,
    ) -> None:
        captured["min_gap_seconds"] = float(min_gap_seconds or 0.0)
        captured["max_gap_seconds"] = float(max_gap_seconds or 0.0)

    monkeypatch.setattr(runner, "_fetch_own_follower_ids", fake_fetch_own_follower_ids)
    monkeypatch.setattr(runner, "_execute_with_retry", fake_execute_with_retry)
    monkeypatch.setattr(runner, "_sleep_action_gap", fake_sleep_action_gap)

    decisions: list[CandidateDecision] = []
    attempted, verified = asyncio.run(
        runner._execute_cleanup_pipeline(
            dry_run=False,
            max_to_run=1,
            decisions=decisions,
        )
    )

    assert attempted == 1
    assert verified == 1
    assert captured["min_gap_seconds"] == 1.0
    assert captured["max_gap_seconds"] == 4.0
