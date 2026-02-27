import asyncio
from pathlib import Path

from medium_stealth_bot.database import Database
from medium_stealth_bot.graph_sync import GraphSyncService
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
