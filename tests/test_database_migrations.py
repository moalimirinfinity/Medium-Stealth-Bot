import sqlite3
from pathlib import Path

from medium_stealth_bot.database import Database


def test_database_migrations_are_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "bot.db"
    db = Database(db_path)
    db.initialize()
    db.initialize()

    connection = sqlite3.connect(db_path)
    try:
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        assert user_version >= 5

        rows = connection.execute("SELECT version, checksum FROM schema_migrations ORDER BY version").fetchall()
        versions = [int(row[0]) for row in rows]
        assert versions == [1, 2, 3, 4, 5, 6]

        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(action_log)").fetchall()
        }
        assert "action_key" in columns
        assert "occurred_day_utc" in columns

        user_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(users)").fetchall()
        }
        assert "name" in user_columns
        assert "follower_count" in user_columns
        assert "following_count" in user_columns
    finally:
        connection.close()
