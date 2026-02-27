import asyncio
from pathlib import Path

import pytest

from medium_stealth_bot.database import Database
from medium_stealth_bot.graph_sync import GraphSyncService
from medium_stealth_bot.models import GraphQLError, GraphQLResult
from medium_stealth_bot.repository import ActionRepository
from medium_stealth_bot.settings import AppSettings


def _build_repository(tmp_path: Path) -> ActionRepository:
    database = Database(tmp_path / "graph-sync-service.db")
    database.initialize()
    return ActionRepository(database)


def _seed_successful_sync(repository: ActionRepository) -> int:
    run_id = repository.begin_graph_sync_run(mode="auto", source_path="registry.json")
    repository.complete_graph_sync_run(run_id, status="success", followers_count=1, following_count=1)
    return run_id


def test_graph_sync_service_skips_when_auto_disabled(tmp_path: Path) -> None:
    repository = _build_repository(tmp_path)
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        GRAPH_SYNC_AUTO_ENABLED=False,
    )
    service = GraphSyncService(settings=settings, client=object(), repository=repository)

    outcome = asyncio.run(service.sync(dry_run=True, mode="auto", force=False))

    assert outcome.skipped is True
    assert outcome.skip_reason == "auto_sync_disabled"


def test_graph_sync_service_skips_when_cache_is_fresh(tmp_path: Path) -> None:
    repository = _build_repository(tmp_path)
    _seed_successful_sync(repository)
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        GRAPH_SYNC_FRESHNESS_WINDOW_MINUTES=5,
    )
    service = GraphSyncService(settings=settings, client=object(), repository=repository)

    outcome = asyncio.run(service.sync(dry_run=True, mode="auto", force=False))

    assert outcome.skipped is True
    assert outcome.skip_reason == "fresh_cache_window"


def test_graph_sync_service_force_bypasses_freshness(tmp_path: Path, monkeypatch) -> None:
    repository = _build_repository(tmp_path)
    _seed_successful_sync(repository)
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        GRAPH_SYNC_FRESHNESS_WINDOW_MINUTES=30,
    )
    service = GraphSyncService(settings=settings, client=object(), repository=repository)

    async def fake_followers() -> list[dict[str, object]]:
        return [{"user_id": "f1", "username": "follower-1"}]

    async def fake_following() -> tuple[list[dict[str, object]], str]:
        return ([{"user_id": "g1", "username": "following-1"}], "graphql")

    monkeypatch.setattr(service, "_fetch_followers_rows_graphql", fake_followers)
    monkeypatch.setattr(service, "_fetch_following_rows", fake_following)

    outcome = asyncio.run(service.sync(dry_run=True, mode="auto", force=True))

    assert outcome.skipped is False
    assert outcome.followers_count == 1
    assert outcome.following_count == 1
    assert outcome.imported_pending_count == 1
    assert outcome.used_following_source == "graphql"
    assert repository.cached_own_follower_ids() == {"f1"}
    assert repository.cached_own_following_ids() == {"g1"}


def test_graph_sync_following_graphql_clamps_page_limit(tmp_path: Path) -> None:
    repository = _build_repository(tmp_path)

    class FakeClient:
        def __init__(self) -> None:
            self.limits: list[int] = []

        async def execute(self, operation):
            self.limits.append(int(operation.variables["paging"]["limit"]))
            return GraphQLResult(
                operationName=operation.operation_name,
                statusCode=200,
                data={
                    "userResult": {
                        "followingUserConnection": {
                            "users": [{"id": "u1", "username": "user1"}],
                            "pagingInfo": {"next": None},
                        }
                    }
                },
                errors=[],
                raw={},
            )

    fake_client = FakeClient()
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        MEDIUM_USER_REF="actor-id",
        OWN_FOLLOWERS_SCAN_LIMIT=200,
    )
    service = GraphSyncService(settings=settings, client=fake_client, repository=repository)

    rows = asyncio.run(service._fetch_following_rows_graphql(operation_name="UserFollowing"))

    assert len(rows) == 1
    assert fake_client.limits == [25]


def test_graph_sync_assert_result_ok_includes_error_message() -> None:
    result = GraphQLResult(
        operationName="UserFollowers",
        statusCode=200,
        data=None,
        errors=[GraphQLError(message='Variable "paging.limit" invalid')],
        raw={},
    )

    with pytest.raises(RuntimeError, match="paging.limit"):
        GraphSyncService._assert_result_ok(result, task_name="graph_sync_followers_graphql")


def test_graph_sync_following_scrape_empty_uses_cached_ids(tmp_path: Path, monkeypatch) -> None:
    repository = _build_repository(tmp_path)
    seed_run = repository.begin_graph_sync_run(mode="manual", source_path="seed")
    repository.replace_own_following_snapshot(
        [{"user_id": "cached-a", "username": "cached_user"}],
        run_id=seed_run,
    )
    repository.complete_graph_sync_run(seed_run, status="success")

    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        GRAPH_SYNC_ENABLE_GRAPHQL_FOLLOWING=False,
        GRAPH_SYNC_ENABLE_SCRAPE_FALLBACK=True,
    )
    service = GraphSyncService(settings=settings, client=object(), repository=repository)

    async def fake_scrape_empty() -> list[dict[str, object]]:
        return []

    monkeypatch.setattr(service, "_fetch_following_rows_scrape", fake_scrape_empty)

    rows, source = asyncio.run(service._fetch_following_rows())

    assert source == "scrape_cached"
    assert rows == [{"user_id": "cached-a"}]


def test_graph_sync_following_scrape_retries_then_uses_rows(tmp_path: Path, monkeypatch) -> None:
    repository = _build_repository(tmp_path)
    settings = AppSettings(
        _env_file=None,
        MEDIUM_SESSION="sid=fake",
        GRAPH_SYNC_ENABLE_GRAPHQL_FOLLOWING=False,
        GRAPH_SYNC_ENABLE_SCRAPE_FALLBACK=True,
    )
    service = GraphSyncService(settings=settings, client=object(), repository=repository)

    calls = {"count": 0}

    async def fake_scrape_flaky() -> list[dict[str, object]]:
        calls["count"] += 1
        if calls["count"] == 1:
            return []
        return [{"user_id": "fresh-a", "username": "fresh_user"}]

    monkeypatch.setattr(service, "_fetch_following_rows_scrape", fake_scrape_flaky)

    rows, source = asyncio.run(service._fetch_following_rows())

    assert calls["count"] == 2
    assert source == "scrape"
    assert rows == [{"user_id": "fresh-a", "username": "fresh_user"}]
