import sqlite3
from pathlib import Path

SCHEMA_V1_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    username TEXT,
    newsletter_id TEXT,
    bio TEXT,
    last_scraped_at DATETIME
);

CREATE TABLE IF NOT EXISTS relationships (
    user_id TEXT PRIMARY KEY,
    state TEXT CHECK(state IN ('following', 'blocking', 'muted', 'none')),
    updated_at DATETIME
);

CREATE TABLE IF NOT EXISTS action_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT,
    target_id TEXT,
    status TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE UNIQUE,
    follower_count INTEGER,
    following_count INTEGER
);
""".strip()

RELATIONSHIP_STATE_V2_SQL = """
CREATE TABLE IF NOT EXISTS relationship_state (
    user_id TEXT PRIMARY KEY,
    newsletter_state TEXT NOT NULL DEFAULT 'unknown'
        CHECK(newsletter_state IN ('subscribed', 'unsubscribed', 'unknown')),
    user_follow_state TEXT NOT NULL DEFAULT 'unknown'
        CHECK(user_follow_state IN ('following', 'not_following', 'unknown')),
    confidence TEXT NOT NULL DEFAULT 'observed'
        CHECK(confidence IN ('observed', 'inferred', 'stubbed')),
    last_source_operation TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_verified_at DATETIME
);

CREATE INDEX IF NOT EXISTS idx_relationship_state_user_follow_state
    ON relationship_state(user_follow_state);

CREATE INDEX IF NOT EXISTS idx_relationship_state_newsletter_state
    ON relationship_state(newsletter_state);
""".strip()

FOLLOW_CYCLE_V3_SQL = """
CREATE TABLE IF NOT EXISTS follow_cycle (
    user_id TEXT PRIMARY KEY,
    username TEXT,
    followed_at DATETIME NOT NULL,
    follow_source TEXT,
    follow_deadline_at DATETIME,
    followed_back_at DATETIME,
    cleanup_status TEXT NOT NULL DEFAULT 'pending'
        CHECK(cleanup_status IN ('pending', 'followed_back', 'unfollowed_nonreciprocal', 'kept_whitelist', 'skipped')),
    last_checked_at DATETIME,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_follow_cycle_status_followed_at
    ON follow_cycle(cleanup_status, followed_at);

CREATE TABLE IF NOT EXISTS blacklist (
    user_id TEXT PRIMARY KEY,
    reason TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
""".strip()


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


class Database:
    TARGET_SCHEMA_VERSION = 3

    def __init__(self, path: Path):
        self.path = path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            self._apply_migrations(connection)
            connection.commit()

    @staticmethod
    def _schema_version(connection: sqlite3.Connection) -> int:
        row = connection.execute("PRAGMA user_version").fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    def _set_schema_version(connection: sqlite3.Connection, version: int) -> None:
        connection.execute(f"PRAGMA user_version = {version}")

    def _apply_migrations(self, connection: sqlite3.Connection) -> None:
        version = self._schema_version(connection)

        if version < 1:
            connection.executescript(SCHEMA_V1_SQL)
            self._set_schema_version(connection, 1)
            version = 1

        if version < 2:
            self._migrate_relationship_state_v2(connection)
            self._set_schema_version(connection, 2)
            version = 2

        if version < 3:
            self._migrate_follow_cycle_v3(connection)
            self._set_schema_version(connection, 3)

    def _migrate_relationship_state_v2(self, connection: sqlite3.Connection) -> None:
        connection.executescript(RELATIONSHIP_STATE_V2_SQL)

        if not _table_exists(connection, "relationships"):
            return

        connection.execute(
            """
            INSERT OR IGNORE INTO relationship_state (
                user_id,
                newsletter_state,
                user_follow_state,
                confidence,
                last_source_operation,
                updated_at,
                last_verified_at
            )
            SELECT
                user_id,
                'unknown',
                CASE
                    WHEN state = 'following' THEN 'following'
                    WHEN state IN ('none', 'blocking', 'muted') THEN 'not_following'
                    ELSE 'unknown'
                END,
                'inferred',
                'legacy_relationships_state',
                COALESCE(updated_at, CURRENT_TIMESTAMP),
                NULL
            FROM relationships
            """
        )

    def _migrate_follow_cycle_v3(self, connection: sqlite3.Connection) -> None:
        connection.executescript(FOLLOW_CYCLE_V3_SQL)

        if not _table_exists(connection, "action_log"):
            return

        connection.execute(
            """
            INSERT OR IGNORE INTO follow_cycle (
                user_id,
                followed_at,
                follow_source,
                follow_deadline_at,
                cleanup_status,
                updated_at
            )
            SELECT
                target_id,
                MIN(timestamp) AS followed_at,
                'legacy_action_log',
                datetime(MIN(timestamp), '+7 day'),
                'pending',
                CURRENT_TIMESTAMP
            FROM action_log
            WHERE action_type = 'follow_verified'
              AND target_id IS NOT NULL
              AND TRIM(target_id) != ''
            GROUP BY target_id
            """
        )
