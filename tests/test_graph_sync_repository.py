from pathlib import Path

from medium_stealth_bot.database import Database
from medium_stealth_bot.repository import ActionRepository


def test_graph_sync_run_lifecycle_updates_state(tmp_path: Path) -> None:
    database = Database(tmp_path / "graph-sync.db")
    database.initialize()
    repository = ActionRepository(database)

    run_id = repository.begin_graph_sync_run(mode="auto", source_path="registry.json")
    repository.complete_graph_sync_run(
        run_id,
        status="success",
        followers_count=10,
        following_count=20,
        imported_pending_count=3,
    )

    with database.connect() as connection:
        row = connection.execute(
            "SELECT status, followers_count, following_count, imported_pending_count FROM graph_sync_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        assert row is not None
        assert row["status"] == "success"
        assert row["followers_count"] == 10
        assert row["following_count"] == 20
        assert row["imported_pending_count"] == 3

        state = connection.execute(
            "SELECT last_run_id, last_followers_count, last_following_count FROM graph_sync_state WHERE id = 1"
        ).fetchone()
        assert state is not None
        assert state["last_run_id"] == run_id
        assert state["last_followers_count"] == 10
        assert state["last_following_count"] == 20


def test_replace_social_snapshot_deletes_stale_rows(tmp_path: Path) -> None:
    database = Database(tmp_path / "graph-sync-snapshot.db")
    database.initialize()
    repository = ActionRepository(database)

    run_1 = repository.begin_graph_sync_run(mode="manual")
    repository.replace_own_followers_snapshot(
        [
            {"user_id": "u1", "username": "user1"},
            {"user_id": "u2", "username": "user2"},
        ],
        run_id=run_1,
    )
    repository.complete_graph_sync_run(run_1, status="success")

    run_2 = repository.begin_graph_sync_run(mode="manual")
    repository.replace_own_followers_snapshot(
        [
            {"user_id": "u2", "username": "user2-updated"},
            {"user_id": "u3", "username": "user3"},
        ],
        run_id=run_2,
    )
    repository.complete_graph_sync_run(run_2, status="success")

    with database.connect() as connection:
        rows = connection.execute(
            "SELECT user_id, username FROM own_followers_cache ORDER BY user_id"
        ).fetchall()
        assert [row["user_id"] for row in rows] == ["u2", "u3"]
        assert rows[0]["username"] == "user2-updated"


def test_imported_following_rows_become_pending_follow_cycle(tmp_path: Path) -> None:
    database = Database(tmp_path / "graph-sync-import.db")
    database.initialize()
    repository = ActionRepository(database)

    run_id = repository.begin_graph_sync_run(mode="manual")
    repository.replace_own_following_snapshot(
        [
            {"user_id": "existing-follow-cycle", "username": "old"},
            {"user_id": "new-following-row", "username": "newuser"},
        ],
        run_id=run_id,
    )
    repository.complete_graph_sync_run(run_id, status="success")

    with database.connect() as connection:
        connection.execute(
            """
            INSERT INTO follow_cycle (
                user_id, username, followed_at, follow_source, follow_deadline_at, cleanup_status, updated_at
            )
            VALUES (?, ?, datetime('now', 'utc', '-10 day'), ?, datetime('now', 'utc', '-3 day'), 'pending', CURRENT_TIMESTAMP)
            """,
            ("existing-follow-cycle", "old", "seed"),
        )
        connection.commit()

    inserted = repository.upsert_imported_follow_cycle_pending_from_following_cache()
    assert inserted == 1

    with database.connect() as connection:
        rows = connection.execute(
            """
            SELECT user_id, followed_at, follow_source, cleanup_status
            FROM follow_cycle
            ORDER BY user_id
            """
        ).fetchall()
        assert len(rows) == 2
        new_row = [row for row in rows if row["user_id"] == "new-following-row"][0]
        assert new_row["followed_at"] == ""
        assert new_row["follow_source"] == "imported_following_cache"
        assert new_row["cleanup_status"] == "pending"
