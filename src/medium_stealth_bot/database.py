import hashlib
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

MIGRATION_PATTERN = re.compile(r"^(\d{3})_(.+)\.sql$")


@dataclass(frozen=True)
class SqlMigration:
    version: int
    name: str
    path: Path
    sql: str
    checksum: str


class Database:
    """SQLite wrapper with file-based numbered SQL migrations."""

    TARGET_SCHEMA_VERSION = 6

    def __init__(self, path: Path):
        self.path = path
        self._migrations_dir = Path(__file__).resolve().with_name("migrations")

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            self._ensure_schema_migrations_table(connection)
            migrations = self._load_migrations()
            self._apply_migrations(connection, migrations)
            self._ensure_action_log_legacy_columns(connection)
            self._sync_action_log_occurred_day_utc(connection)
            self._set_schema_version(connection, self._latest_migration_version(migrations))
            connection.commit()

    @staticmethod
    def _schema_version(connection: sqlite3.Connection) -> int:
        row = connection.execute("PRAGMA user_version").fetchone()
        return int(row[0]) if row else 0

    @staticmethod
    def _set_schema_version(connection: sqlite3.Connection, version: int) -> None:
        connection.execute(f"PRAGMA user_version = {version}")

    def _ensure_schema_migrations_table(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _load_migrations(self) -> list[SqlMigration]:
        if not self._migrations_dir.exists():
            return []
        migrations: list[SqlMigration] = []
        for path in sorted(self._migrations_dir.glob("*.sql")):
            match = MIGRATION_PATTERN.match(path.name)
            if not match:
                continue
            version = int(match.group(1))
            name = match.group(2)
            sql = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
            migrations.append(
                SqlMigration(
                    version=version,
                    name=name,
                    path=path,
                    sql=sql,
                    checksum=checksum,
                )
            )
        return migrations

    def _apply_migrations(self, connection: sqlite3.Connection, migrations: list[SqlMigration]) -> None:
        for migration in migrations:
            row = connection.execute(
                "SELECT version, checksum FROM schema_migrations WHERE version = ?",
                (migration.version,),
            ).fetchone()
            if row is not None:
                existing_checksum = str(row["checksum"])
                if existing_checksum != migration.checksum:
                    raise RuntimeError(
                        "Migration checksum mismatch for "
                        f"version={migration.version} ({migration.path.name})."
                    )
                continue

            connection.executescript(migration.sql)
            connection.execute(
                """
                INSERT INTO schema_migrations (version, name, checksum)
                VALUES (?, ?, ?)
                """,
                (migration.version, migration.name, migration.checksum),
            )

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _ensure_action_log_legacy_columns(self, connection: sqlite3.Connection) -> None:
        if not self._table_exists(connection, "action_log"):
            return
        rows = connection.execute("PRAGMA table_info(action_log)").fetchall()
        existing = {str(row["name"]) for row in rows}
        if "action_key" not in existing:
            connection.execute("ALTER TABLE action_log ADD COLUMN action_key TEXT")
        if "occurred_day_utc" not in existing:
            connection.execute("ALTER TABLE action_log ADD COLUMN occurred_day_utc TEXT")
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_action_log_action_key
                ON action_log(action_key)
                WHERE action_key IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_action_log_target_time
                ON action_log(target_id, timestamp)
            """
        )

    @staticmethod
    def _sync_action_log_occurred_day_utc(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            UPDATE action_log
            SET occurred_day_utc = date(COALESCE(timestamp, CURRENT_TIMESTAMP), 'utc')
            WHERE occurred_day_utc IS NULL
            """
        )

    @staticmethod
    def _latest_migration_version(migrations: list[SqlMigration]) -> int:
        return max((item.version for item in migrations), default=0)
