import asyncio
from pathlib import Path

from medium_stealth_bot.database import Database
from medium_stealth_bot.logic import DailyRunner
from medium_stealth_bot.models import GraphQLResult
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.settings import AppSettings


class StubClient:
    async def execute(self, operation):
        return GraphQLResult(
            operationName=operation.operation_name,
            statusCode=200,
            data={"user": {"viewerEdge": {"isFollowing": False}}},
            errors=[],
            raw={},
        )

    def metrics_snapshot(self):
        return {
            "mode": "fixture",
            "request_count": 0,
            "avg_latency_ms": 0.0,
            "status_counts": {},
            "result_failures": 0,
        }


def test_reconcile_scans_all_candidates_across_pages(tmp_path: Path) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-user-id",
        MIN_VERIFY_GAP_SECONDS=0,
        MAX_VERIFY_GAP_SECONDS=0,
    )
    database = Database(tmp_path / "reconcile-paging.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    with database.connect() as connection:
        for index in range(5):
            connection.execute(
                """
                INSERT INTO follow_cycle (
                    user_id, username, followed_at, follow_source, follow_deadline_at, cleanup_status, updated_at
                )
                VALUES (?, ?, datetime('now', 'utc', '-10 day'), ?, datetime('now', 'utc', '-5 day'), 'pending', CURRENT_TIMESTAMP)
                """,
                (f"user-{index}", f"user{index}", "seed"),
            )
        connection.commit()

    outcome = asyncio.run(
        runner.reconcile_follow_states(
            dry_run=True,
            max_users=5,
            page_size=2,
        )
    )

    assert outcome.scanned_users == 5
    assert outcome.updated_users == 0
    assert outcome.following_count == 0
    assert outcome.not_following_count == 5
    assert outcome.unknown_count == 0
    assert len(outcome.decision_log) == 5
