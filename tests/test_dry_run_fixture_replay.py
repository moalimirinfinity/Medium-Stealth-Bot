import asyncio
import json
from pathlib import Path

from medium_stealth_bot.database import Database
from medium_stealth_bot.logic import DailyRunner
from medium_stealth_bot.models import GraphQLResult, ProbeSnapshot
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


def _fixture_probe() -> ProbeSnapshot:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "probe_snapshot_minimal.json"
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    return ProbeSnapshot.model_validate(payload)


def test_dry_run_replay_is_deterministic(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="e347e420a0cc",
        ENABLE_PRE_FOLLOW_CLAP=False,
        MIN_SESSION_WARMUP_SECONDS=0,
        MAX_SESSION_WARMUP_SECONDS=0,
        MIN_ACTION_GAP_SECONDS=0,
        MAX_ACTION_GAP_SECONDS=0,
        FOLLOW_CANDIDATE_LIMIT=5,
        MAX_FOLLOW_ACTIONS_PER_RUN=1,
    )
    database = Database(tmp_path / "replay.db")
    database.initialize()
    repository = ActionRepository(database)
    runner = DailyRunner(settings=settings, client=StubClient(), repository=repository)

    fixture_probe = _fixture_probe()

    async def fake_probe(tag_slug: str = "programming") -> ProbeSnapshot:
        return fixture_probe

    async def fake_execute_with_retry(task_name: str, operation) -> GraphQLResult:
        if task_name == "candidate_user_viewer_edge":
            return GraphQLResult(
                operationName="UserViewerEdge",
                statusCode=200,
                data={"user": {"viewerEdge": {"isFollowing": False}}},
                errors=[],
                raw={},
                stubbed=False,
            )
        if task_name == "own_followers_scan":
            return GraphQLResult(
                operationName="UserFollowers",
                statusCode=200,
                data={"userResult": {"followersUserConnection": {"users": []}}},
                errors=[],
                raw={},
                stubbed=False,
            )
        return GraphQLResult(
            operationName=operation.operation_name,
            statusCode=200,
            data={},
            errors=[],
            raw={},
            stubbed=False,
        )

    monkeypatch.setattr(runner, "probe", fake_probe)
    monkeypatch.setattr(runner, "_execute_with_retry", fake_execute_with_retry)

    outcome = asyncio.run(runner.run_daily_cycle(tag_slug="programming", dry_run=True))

    assert outcome.follow_actions_attempted == 1
    assert outcome.follow_actions_verified == 0
    assert outcome.decision_reason_counts.get("dry_run:planned_follow") == 1
    assert outcome.decision_result_counts.get("planned") == 1
    assert outcome.client_metrics.get("mode") == "fixture"
