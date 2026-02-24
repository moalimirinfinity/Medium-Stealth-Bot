from medium_stealth_bot.database import Database


class ActionRepository:
    def __init__(self, database: Database):
        self.database = database

    def actions_today(self) -> int:
        query = """
        SELECT COUNT(*) AS count
        FROM action_log
        WHERE timestamp >= datetime('now', 'start of day')
        """
        with self.database.connect() as connection:
            row = connection.execute(query).fetchone()
            return int(row["count"])

    def record_action(self, action_type: str, target_id: str | None, status: str) -> None:
        query = """
        INSERT INTO action_log (action_type, target_id, status)
        VALUES (?, ?, ?)
        """
        with self.database.connect() as connection:
            connection.execute(query, (action_type, target_id, status))
            connection.commit()
