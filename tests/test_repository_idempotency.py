import sqlite3
from pathlib import Path

from medium_stealth_bot.database import Database
from medium_stealth_bot.repository import ActionRepository


def test_record_action_idempotency_key(tmp_path: Path) -> None:
    db_path = tmp_path / "actions.db"
    database = Database(db_path)
    database.initialize()
    repository = ActionRepository(database)

    inserted_first = repository.record_action(
        "follow_subscribe_attempt",
        "user-1",
        "ok",
        action_key="follow_subscribe_attempt:user-1:2026-02-24",
    )
    inserted_second = repository.record_action(
        "follow_subscribe_attempt",
        "user-1",
        "ok",
        action_key="follow_subscribe_attempt:user-1:2026-02-24",
    )

    assert inserted_first is True
    assert inserted_second is False

    connection = sqlite3.connect(db_path)
    try:
        count = int(connection.execute("SELECT COUNT(*) FROM action_log").fetchone()[0])
        assert count == 1
    finally:
        connection.close()
