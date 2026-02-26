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
        connection.commit()

    due = repository.pending_nonreciprocal_candidates(grace_days=7, limit=10)
    due_ids = {row["user_id"] for row in due}
    assert "user-missing-followed-at" in due_ids


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
