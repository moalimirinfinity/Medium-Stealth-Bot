from medium_stealth_bot.database import Database
from medium_stealth_bot.models import (
    CanonicalRelationshipState,
    CandidateSource,
    CandidateUser,
    NewsletterState,
    RelationshipConfidence,
    UserFollowState,
)


class ActionRepository:
    def __init__(self, database: Database):
        self.database = database

    @staticmethod
    def _serialize_source_labels(source_labels: list[str]) -> str | None:
        normalized = sorted({label.strip() for label in source_labels if label and label.strip()})
        return ",".join(normalized) if normalized else None

    @staticmethod
    def _parse_candidate_sources(raw_source_labels: str | None) -> list[CandidateSource]:
        parsed: list[CandidateSource] = []
        if not raw_source_labels:
            return parsed
        for item in raw_source_labels.split(","):
            value = item.strip()
            if not value:
                continue
            try:
                source = CandidateSource(value)
            except ValueError:
                continue
            if source not in parsed:
                parsed.append(source)
        return parsed

    def _candidate_from_growth_queue_row(self, row) -> CandidateUser:
        return CandidateUser(
            user_id=row["user_id"],
            username=row["username"],
            name=row["name"],
            bio=row["bio"],
            newsletter_v3_id=row["newsletter_v3_id"],
            follower_count=row["follower_count"],
            following_count=row["following_count"],
            latest_post_id=row["latest_post_id"],
            latest_post_title=row["latest_post_title"],
            last_post_created_at=row["last_post_created_at"],
            score=float(row["queued_score"] or 0.0),
            sources=self._parse_candidate_sources(row["source_labels"]),
        )

    @staticmethod
    def _utc_modifier_hours(hours: int) -> str:
        return f"-{hours} hours"

    @staticmethod
    def _utc_modifier_days(days: int) -> str:
        return f"-{days} days"

    @staticmethod
    def _utc_add_hours_modifier(hours: int) -> str:
        return f"+{hours} hours"

    def actions_today_utc(self, action_types: tuple[str, ...] | None = None) -> int:
        # Product rule: daily budgets are calculated on UTC calendar day boundaries.
        query = """
        SELECT COUNT(*) AS count
        FROM action_log
        WHERE timestamp >= datetime('now', 'utc', 'start of day')
          AND timestamp < datetime('now', 'utc', 'start of day', '+1 day')
        """
        params: tuple[object, ...] = ()
        if action_types:
            placeholders = ", ".join(["?"] * len(action_types))
            query += f"\n  AND action_type IN ({placeholders})"
            params = action_types
        with self.database.connect() as connection:
            row = connection.execute(query, params).fetchone()
            return int(row["count"])

    def actions_today(self) -> int:
        # Backward-compatible alias for older call sites.
        return self.actions_today_utc()

    def action_counts_today_utc(self, action_types: tuple[str, ...]) -> dict[str, int]:
        if not action_types:
            return {}
        placeholders = ", ".join(["?"] * len(action_types))
        query = f"""
        SELECT action_type, COUNT(*) AS count
        FROM action_log
        WHERE timestamp >= datetime('now', 'utc', 'start of day')
          AND timestamp < datetime('now', 'utc', 'start of day', '+1 day')
          AND action_type IN ({placeholders})
        GROUP BY action_type
        """
        counts = {action_type: 0 for action_type in action_types}
        with self.database.connect() as connection:
            rows = connection.execute(query, action_types).fetchall()
            for row in rows:
                counts[str(row["action_type"])] = int(row["count"])
        return counts

    def record_action(
        self,
        action_type: str,
        target_id: str | None,
        status: str,
        *,
        action_key: str | None = None,
    ) -> bool:
        query = """
        INSERT OR IGNORE INTO action_log (action_type, target_id, status, action_key, occurred_day_utc)
        VALUES (?, ?, ?, ?, date('now', 'utc'))
        """
        with self.database.connect() as connection:
            cursor = connection.execute(query, (action_type, target_id, status, action_key))
            connection.commit()
            return cursor.rowcount > 0

    def has_recent_action(
        self,
        user_id: str,
        *,
        within_hours: int,
        action_types: tuple[str, ...] | None = None,
    ) -> bool:
        if action_types:
            placeholders = ", ".join(["?"] * len(action_types))
            query = f"""
            SELECT 1
            FROM action_log
            WHERE target_id = ?
              AND action_type IN ({placeholders})
              AND timestamp >= datetime('now', 'utc', ?)
            LIMIT 1
            """
            params: tuple[object, ...] = (user_id, *action_types, self._utc_modifier_hours(within_hours))
        else:
            query = """
            SELECT 1
            FROM action_log
            WHERE target_id = ?
              AND timestamp >= datetime('now', 'utc', ?)
            LIMIT 1
            """
            params = (user_id, self._utc_modifier_hours(within_hours))

        with self.database.connect() as connection:
            row = connection.execute(query, params).fetchone()
            return row is not None

    def recent_action_retry_after(
        self,
        user_id: str,
        *,
        within_hours: int,
        action_types: tuple[str, ...] | None = None,
    ) -> str | None:
        if action_types:
            placeholders = ", ".join(["?"] * len(action_types))
            query = f"""
            SELECT datetime(timestamp, ?) AS retry_after_at
            FROM action_log
            WHERE target_id = ?
              AND action_type IN ({placeholders})
              AND timestamp >= datetime('now', 'utc', ?)
            ORDER BY datetime(timestamp) DESC, id DESC
            LIMIT 1
            """
            params: tuple[object, ...] = (
                self._utc_add_hours_modifier(within_hours),
                user_id,
                *action_types,
                self._utc_modifier_hours(within_hours),
            )
        else:
            query = """
            SELECT datetime(timestamp, ?) AS retry_after_at
            FROM action_log
            WHERE target_id = ?
              AND timestamp >= datetime('now', 'utc', ?)
            ORDER BY datetime(timestamp) DESC, id DESC
            LIMIT 1
            """
            params = (
                self._utc_add_hours_modifier(within_hours),
                user_id,
                self._utc_modifier_hours(within_hours),
            )

        with self.database.connect() as connection:
            row = connection.execute(query, params).fetchone()
            if row is None:
                return None
            retry_after_at = row["retry_after_at"]
            return str(retry_after_at) if retry_after_at else None

    def verified_actions_for_target(
        self,
        *,
        target_id: str,
        action_type: str,
        rollback_action_type: str | None = None,
    ) -> list[dict[str, str | None]]:
        query = """
        SELECT source.action_key, source.status, source.timestamp
        FROM action_log AS source
        WHERE source.target_id = ?
          AND source.action_type = ?
          AND source.status LIKE 'verified:%'
        """
        params: list[object] = [target_id, action_type]
        if rollback_action_type:
            query += """
          AND NOT EXISTS (
              SELECT 1
              FROM action_log AS rollback
              WHERE rollback.action_type = ?
                AND rollback.status LIKE 'verified:%'
                AND rollback.action_key = ? || ':' || source.action_key
          )
            """
            params.extend([rollback_action_type, rollback_action_type])
        query += "\n        ORDER BY source.timestamp DESC, source.id DESC"
        with self.database.connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
            return [
                {
                    "action_key": row["action_key"],
                    "status": row["status"],
                    "timestamp": row["timestamp"],
                }
                for row in rows
            ]

    def is_blacklisted(self, user_id: str) -> bool:
        query = "SELECT 1 FROM blacklist WHERE user_id = ? LIMIT 1"
        with self.database.connect() as connection:
            row = connection.execute(query, (user_id,)).fetchone()
            return row is not None

    def add_blacklist(self, user_id: str, reason: str | None = None) -> None:
        query = """
        INSERT INTO blacklist (user_id, reason, created_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            reason = excluded.reason
        """
        with self.database.connect() as connection:
            connection.execute(query, (user_id, reason))
            connection.commit()

    def upsert_user_profile(
        self,
        user_id: str,
        *,
        username: str | None = None,
        name: str | None = None,
        follower_count: int | None = None,
        following_count: int | None = None,
        newsletter_id: str | None = None,
        bio: str | None = None,
    ) -> None:
        query = """
        INSERT INTO users (
            user_id,
            username,
            name,
            follower_count,
            following_count,
            newsletter_id,
            bio,
            last_scraped_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, users.username),
            name = COALESCE(excluded.name, users.name),
            follower_count = COALESCE(excluded.follower_count, users.follower_count),
            following_count = COALESCE(excluded.following_count, users.following_count),
            newsletter_id = COALESCE(excluded.newsletter_id, users.newsletter_id),
            bio = COALESCE(excluded.bio, users.bio),
            last_scraped_at = CURRENT_TIMESTAMP
        """
        with self.database.connect() as connection:
            connection.execute(
                query,
                (
                    user_id,
                    username,
                    name,
                    follower_count,
                    following_count,
                    newsletter_id,
                    bio,
                ),
            )
            connection.commit()

    def begin_graph_sync_run(
        self,
        *,
        mode: str,
        source_path: str | None = None,
    ) -> int:
        query = """
        INSERT INTO graph_sync_runs (mode, source_path, status, started_at)
        VALUES (?, ?, 'running', CURRENT_TIMESTAMP)
        """
        with self.database.connect() as connection:
            cursor = connection.execute(query, (mode, source_path))
            connection.commit()
            return int(cursor.lastrowid)

    def complete_graph_sync_run(
        self,
        run_id: int,
        *,
        status: str,
        followers_count: int = 0,
        following_count: int = 0,
        imported_pending_count: int = 0,
        error_message: str | None = None,
    ) -> None:
        query = """
        UPDATE graph_sync_runs
        SET status = ?,
            ended_at = CURRENT_TIMESTAMP,
            followers_count = ?,
            following_count = ?,
            imported_pending_count = ?,
            error_message = ?
        WHERE id = ?
        """
        with self.database.connect() as connection:
            connection.execute(
                query,
                (
                    status,
                    max(0, followers_count),
                    max(0, following_count),
                    max(0, imported_pending_count),
                    error_message,
                    run_id,
                ),
            )
            if status == "success":
                connection.execute(
                    """
                    UPDATE graph_sync_state
                    SET last_success_at = CURRENT_TIMESTAMP,
                        last_run_id = ?,
                        last_mode = (
                            SELECT mode
                            FROM graph_sync_runs
                            WHERE id = ?
                        ),
                        last_followers_count = ?,
                        last_following_count = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                    """,
                    (
                        run_id,
                        run_id,
                        max(0, followers_count),
                        max(0, following_count),
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE graph_sync_state
                    SET last_run_id = ?,
                        last_mode = (
                            SELECT mode
                            FROM graph_sync_runs
                            WHERE id = ?
                        ),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                    """,
                    (run_id, run_id),
                )
            connection.commit()

    def latest_graph_sync_success_at(self) -> str | None:
        query = "SELECT last_success_at FROM graph_sync_state WHERE id = 1"
        with self.database.connect() as connection:
            row = connection.execute(query).fetchone()
            if row is None:
                return None
            value = row["last_success_at"]
            return str(value) if value else None

    def _replace_social_snapshot(
        self,
        *,
        table_name: str,
        rows: list[dict[str, object]],
        run_id: int,
    ) -> int:
        if table_name not in {"own_followers_cache", "own_following_cache"}:
            raise ValueError(f"Unsupported social cache table: {table_name}")

        upsert_query = f"""
        INSERT INTO {table_name} (
            user_id,
            username,
            name,
            bio,
            follower_count,
            following_count,
            newsletter_v3_id,
            first_seen_at,
            last_seen_at,
            last_sync_run_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, {table_name}.username),
            name = COALESCE(excluded.name, {table_name}.name),
            bio = COALESCE(excluded.bio, {table_name}.bio),
            follower_count = COALESCE(excluded.follower_count, {table_name}.follower_count),
            following_count = COALESCE(excluded.following_count, {table_name}.following_count),
            newsletter_v3_id = COALESCE(excluded.newsletter_v3_id, {table_name}.newsletter_v3_id),
            last_seen_at = CURRENT_TIMESTAMP,
            last_sync_run_id = excluded.last_sync_run_id
        """

        with self.database.connect() as connection:
            connection.execute("DROP TABLE IF EXISTS temp_graph_sync_ids")
            connection.execute("CREATE TEMP TABLE temp_graph_sync_ids (user_id TEXT PRIMARY KEY)")

            seen_ids: set[str] = set()
            payload: list[tuple[object, ...]] = []
            for row in rows:
                user_id = str(row.get("user_id") or "").strip()
                if not user_id or user_id in seen_ids:
                    continue
                seen_ids.add(user_id)
                payload.append(
                    (
                        user_id,
                        row.get("username"),
                        row.get("name"),
                        row.get("bio"),
                        row.get("follower_count"),
                        row.get("following_count"),
                        row.get("newsletter_v3_id"),
                        run_id,
                    )
                )

            if payload:
                connection.executemany(
                    "INSERT INTO temp_graph_sync_ids (user_id) VALUES (?)",
                    [(item[0],) for item in payload],
                )
                connection.executemany(upsert_query, payload)
                connection.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE user_id NOT IN (SELECT user_id FROM temp_graph_sync_ids)
                    """
                )
            else:
                connection.execute(f"DELETE FROM {table_name}")
            connection.execute("DROP TABLE IF EXISTS temp_graph_sync_ids")
            connection.commit()
            return len(payload)

    def replace_own_followers_snapshot(
        self,
        rows: list[dict[str, object]],
        *,
        run_id: int,
    ) -> int:
        return self._replace_social_snapshot(
            table_name="own_followers_cache",
            rows=rows,
            run_id=run_id,
        )

    def replace_own_following_snapshot(
        self,
        rows: list[dict[str, object]],
        *,
        run_id: int,
    ) -> int:
        return self._replace_social_snapshot(
            table_name="own_following_cache",
            rows=rows,
            run_id=run_id,
        )

    def upsert_users_from_social_caches(self) -> int:
        query = """
        INSERT INTO users (
            user_id,
            username,
            name,
            follower_count,
            following_count,
            last_scraped_at
        )
        SELECT
            merged.user_id,
            merged.username,
            merged.name,
            merged.follower_count,
            merged.following_count,
            CURRENT_TIMESTAMP
        FROM (
            SELECT user_id, username, name, follower_count, following_count
            FROM own_followers_cache
            UNION ALL
            SELECT user_id, username, name, follower_count, following_count
            FROM own_following_cache
        ) AS merged
        WHERE merged.user_id IS NOT NULL
          AND TRIM(merged.user_id) != ''
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, users.username),
            name = COALESCE(excluded.name, users.name),
            follower_count = COALESCE(excluded.follower_count, users.follower_count),
            following_count = COALESCE(excluded.following_count, users.following_count),
            last_scraped_at = CURRENT_TIMESTAMP
        """
        with self.database.connect() as connection:
            connection.execute(query)
            row = connection.execute("SELECT changes() AS count").fetchone()
            connection.commit()
            if row is None:
                return 0
            return int(row["count"])

    def cached_own_follower_ids(self) -> set[str]:
        query = "SELECT user_id FROM own_followers_cache"
        with self.database.connect() as connection:
            rows = connection.execute(query).fetchall()
            return {str(row["user_id"]) for row in rows if row["user_id"]}

    def cached_own_following_ids(self) -> set[str]:
        query = "SELECT user_id FROM own_following_cache"
        with self.database.connect() as connection:
            rows = connection.execute(query).fetchall()
            return {str(row["user_id"]) for row in rows if row["user_id"]}

    def upsert_imported_follow_cycle_pending_from_following_cache(self) -> int:
        query = """
        INSERT INTO follow_cycle (
            user_id,
            username,
            followed_at,
            follow_source,
            follow_deadline_at,
            cleanup_status,
            updated_at
        )
        SELECT
            c.user_id,
            c.username,
            '',
            'imported_following_cache',
            NULL,
            'pending',
            CURRENT_TIMESTAMP
        FROM own_following_cache c
        LEFT JOIN follow_cycle f
            ON f.user_id = c.user_id
        WHERE f.user_id IS NULL
        """
        with self.database.connect() as connection:
            connection.execute(query)
            row = connection.execute("SELECT changes() AS count").fetchone()
            connection.commit()
            if row is None:
                return 0
            return int(row["count"])

    def upsert_candidate_reconciliation(
        self,
        *,
        user_id: str,
        username: str | None,
        newsletter_v3_id: str | None,
        source_labels: list[str],
        score: float,
        decision_reason: str,
        eligible: bool,
        needs_reconcile: bool = True,
    ) -> None:
        normalized_sources = sorted({label.strip() for label in source_labels if label.strip()})
        query = """
        INSERT INTO candidate_reconciliation (
            user_id,
            username,
            newsletter_v3_id,
            source_labels,
            last_score,
            last_decision_reason,
            eligible,
            needs_reconcile,
            seen_count,
            first_seen_at,
            last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, candidate_reconciliation.username),
            newsletter_v3_id = COALESCE(excluded.newsletter_v3_id, candidate_reconciliation.newsletter_v3_id),
            source_labels = CASE
                WHEN candidate_reconciliation.source_labels IS NULL OR candidate_reconciliation.source_labels = ''
                    THEN excluded.source_labels
                WHEN excluded.source_labels IS NULL OR excluded.source_labels = ''
                    THEN candidate_reconciliation.source_labels
                ELSE candidate_reconciliation.source_labels || ',' || excluded.source_labels
            END,
            last_score = excluded.last_score,
            last_decision_reason = excluded.last_decision_reason,
            eligible = excluded.eligible,
            needs_reconcile = CASE
                WHEN excluded.needs_reconcile = 1 THEN 1
                ELSE candidate_reconciliation.needs_reconcile
            END,
            seen_count = candidate_reconciliation.seen_count + 1,
            last_seen_at = CURRENT_TIMESTAMP
        """
        with self.database.connect() as connection:
            connection.execute(
                query,
                (
                    user_id,
                    username,
                    newsletter_v3_id,
                    ",".join(normalized_sources),
                    score,
                    decision_reason,
                    1 if eligible else 0,
                    1 if needs_reconcile else 0,
                ),
            )
            connection.commit()

    def mark_candidate_reconciled(self, user_id: str, follow_state: UserFollowState) -> None:
        query = """
        UPDATE candidate_reconciliation
        SET needs_reconcile = 0,
            last_observed_follow_state = ?,
            last_reconciled_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """
        with self.database.connect() as connection:
            connection.execute(query, (follow_state.value, user_id))
            connection.commit()

    def growth_queue_ready_count(self) -> int:
        return self.growth_queue_state_counts()["ready"]

    def growth_queue_state_counts(self) -> dict[str, int]:
        query = """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN queue_state = 'queued' THEN 1 ELSE 0 END) AS queued,
            SUM(
                CASE
                    WHEN queue_state = 'deferred'
                     AND (
                         retry_after_at IS NULL
                         OR datetime(retry_after_at) <= datetime('now', 'utc')
                     )
                    THEN 1
                    ELSE 0
                END
            ) AS deferred_due,
            SUM(
                CASE
                    WHEN queue_state = 'deferred'
                     AND retry_after_at IS NOT NULL
                     AND datetime(retry_after_at) > datetime('now', 'utc')
                    THEN 1
                    ELSE 0
                END
            ) AS deferred_future,
            SUM(CASE WHEN queue_state = 'rejected' THEN 1 ELSE 0 END) AS rejected,
            SUM(CASE WHEN queue_state = 'followed' THEN 1 ELSE 0 END) AS followed
        FROM growth_candidate_queue
        """
        with self.database.connect() as connection:
            row = connection.execute(query).fetchone()
            queued = int(row["queued"] or 0) if row is not None else 0
            deferred_due = int(row["deferred_due"] or 0) if row is not None else 0
            deferred_future = int(row["deferred_future"] or 0) if row is not None else 0
            rejected = int(row["rejected"] or 0) if row is not None else 0
            followed = int(row["followed"] or 0) if row is not None else 0
            total = int(row["total"] or 0) if row is not None else 0
            deferred = deferred_due + deferred_future
            ready = queued + deferred_due
            return {
                "ready": ready,
                "queued": queued,
                "deferred": deferred,
                "deferred_due": deferred_due,
                "deferred_future": deferred_future,
                "rejected": rejected,
                "followed": followed,
                "total": total,
            }

    def upsert_growth_candidate_buffer(
        self,
        candidates: list[CandidateUser],
        *,
        queue_reason: str,
    ) -> int:
        if not candidates:
            return 0

        user_query = """
        INSERT INTO users (
            user_id,
            username,
            name,
            follower_count,
            following_count,
            newsletter_id,
            bio,
            last_scraped_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, users.username),
            name = COALESCE(excluded.name, users.name),
            follower_count = COALESCE(excluded.follower_count, users.follower_count),
            following_count = COALESCE(excluded.following_count, users.following_count),
            newsletter_id = COALESCE(excluded.newsletter_id, users.newsletter_id),
            bio = COALESCE(excluded.bio, users.bio),
            last_scraped_at = CURRENT_TIMESTAMP
        """
        queue_query = """
        INSERT INTO growth_candidate_queue (
            user_id,
            username,
            name,
            bio,
            newsletter_v3_id,
            source_labels,
            queued_score,
            follower_count,
            following_count,
            latest_post_id,
            latest_post_title,
            last_post_created_at,
            queue_state,
            retry_after_at,
            last_reason,
            discover_count,
            first_discovered_at,
            last_discovered_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', NULL, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, growth_candidate_queue.username),
            name = COALESCE(excluded.name, growth_candidate_queue.name),
            bio = COALESCE(excluded.bio, growth_candidate_queue.bio),
            newsletter_v3_id = COALESCE(excluded.newsletter_v3_id, growth_candidate_queue.newsletter_v3_id),
            source_labels = COALESCE(excluded.source_labels, growth_candidate_queue.source_labels),
            queued_score = excluded.queued_score,
            follower_count = COALESCE(excluded.follower_count, growth_candidate_queue.follower_count),
            following_count = COALESCE(excluded.following_count, growth_candidate_queue.following_count),
            latest_post_id = COALESCE(excluded.latest_post_id, growth_candidate_queue.latest_post_id),
            latest_post_title = COALESCE(excluded.latest_post_title, growth_candidate_queue.latest_post_title),
            last_post_created_at = COALESCE(excluded.last_post_created_at, growth_candidate_queue.last_post_created_at),
            queue_state = 'queued',
            retry_after_at = NULL,
            last_reason = excluded.last_reason,
            discover_count = growth_candidate_queue.discover_count + 1,
            last_discovered_at = CURRENT_TIMESTAMP
        """

        user_rows: list[tuple[object, ...]] = []
        queue_rows: list[tuple[object, ...]] = []
        seen_ids: set[str] = set()
        for candidate in candidates:
            if candidate.user_id in seen_ids:
                continue
            seen_ids.add(candidate.user_id)
            serialized_sources = self._serialize_source_labels([source.value for source in candidate.sources])
            user_rows.append(
                (
                    candidate.user_id,
                    candidate.username,
                    candidate.name,
                    candidate.follower_count,
                    candidate.following_count,
                    candidate.newsletter_v3_id,
                    candidate.bio,
                )
            )
            queue_rows.append(
                (
                    candidate.user_id,
                    candidate.username,
                    candidate.name,
                    candidate.bio,
                    candidate.newsletter_v3_id,
                    serialized_sources,
                    candidate.score,
                    candidate.follower_count,
                    candidate.following_count,
                    candidate.latest_post_id,
                    candidate.latest_post_title,
                    candidate.last_post_created_at,
                    queue_reason,
                )
            )

        with self.database.connect() as connection:
            connection.executemany(user_query, user_rows)
            connection.executemany(queue_query, queue_rows)
            connection.commit()
        return len(queue_rows)

    def queued_growth_candidates(
        self,
        *,
        limit: int,
        due_deferred_reserve_ratio: float = 0.0,
    ) -> list[CandidateUser]:
        if limit <= 0:
            return []
        bounded_limit = limit
        reserve_ratio = min(0.9, max(0.0, due_deferred_reserve_ratio))
        reserved_due_limit = 0
        if reserve_ratio > 0:
            reserved_due_limit = max(1, int(round(bounded_limit * reserve_ratio)))
            reserved_due_limit = min(bounded_limit, reserved_due_limit)

        base_select = """
        SELECT
            user_id,
            username,
            name,
            bio,
            newsletter_v3_id,
            source_labels,
            queued_score,
            follower_count,
            following_count,
            latest_post_id,
            latest_post_title,
            last_post_created_at
        FROM growth_candidate_queue
        """
        queued_query = (
            base_select
            + """
        WHERE queue_state = 'queued'
        ORDER BY
            queued_score DESC,
            datetime(last_discovered_at) DESC,
            datetime(first_discovered_at) ASC
        LIMIT ?
        """
        )
        due_deferred_query = (
            base_select
            + """
        WHERE queue_state = 'deferred'
          AND (
              retry_after_at IS NULL
              OR datetime(retry_after_at) <= datetime('now', 'utc')
          )
        ORDER BY
            datetime(COALESCE(retry_after_at, last_discovered_at)) ASC,
            queued_score DESC,
            datetime(last_discovered_at) DESC,
            datetime(first_discovered_at) ASC
        LIMIT ? OFFSET ?
        """
        )

        with self.database.connect() as connection:
            due_rows = (
                connection.execute(due_deferred_query, (reserved_due_limit, 0)).fetchall()
                if reserved_due_limit > 0
                else []
            )
            queued_limit = max(0, bounded_limit - len(due_rows))
            queued_rows = (
                connection.execute(queued_query, (queued_limit,)).fetchall()
                if queued_limit > 0
                else []
            )
            filled = len(due_rows) + len(queued_rows)
            extra_due_rows = []
            if filled < bounded_limit:
                extra_due_rows = connection.execute(
                    due_deferred_query,
                    (bounded_limit - filled, len(due_rows)),
                ).fetchall()

            rows = [*due_rows, *queued_rows, *extra_due_rows]
            return [self._candidate_from_growth_queue_row(row) for row in rows]

    def prune_growth_candidate_queue(
        self,
        *,
        followed_after_days: int,
        rejected_after_days: int,
        stale_after_days: int,
        dry_run: bool = False,
    ) -> dict[str, int]:
        followed_after_days = max(0, followed_after_days)
        rejected_after_days = max(0, rejected_after_days)
        stale_after_days = max(0, stale_after_days)
        prune_plan: tuple[tuple[str, str, tuple[object, ...]], ...] = (
            (
                "followed",
                """
                queue_state = 'followed'
                  AND datetime(
                      COALESCE(
                          last_followed_at,
                          last_attempted_at,
                          last_screened_at,
                          last_discovered_at,
                          first_discovered_at
                      )
                  ) <= datetime('now', 'utc', ?)
                """,
                (self._utc_modifier_days(followed_after_days),),
            ),
            (
                "rejected",
                """
                queue_state = 'rejected'
                  AND datetime(
                      COALESCE(
                          last_screened_at,
                          last_attempted_at,
                          last_discovered_at,
                          first_discovered_at
                      )
                  ) <= datetime('now', 'utc', ?)
                """,
                (self._utc_modifier_days(rejected_after_days),),
            ),
            (
                "stale",
                """
                queue_state IN ('queued', 'deferred')
                  AND datetime(
                      COALESCE(
                          last_attempted_at,
                          last_screened_at,
                          last_discovered_at,
                          first_discovered_at
                      )
                  ) <= datetime('now', 'utc', ?)
                """,
                (self._utc_modifier_days(stale_after_days),),
            ),
        )
        counts = {"followed": 0, "rejected": 0, "stale": 0}
        with self.database.connect() as connection:
            for key, where_clause, params in prune_plan:
                row = connection.execute(
                    f"""
                    SELECT COUNT(*) AS count
                    FROM growth_candidate_queue
                    WHERE {where_clause}
                    """,
                    params,
                ).fetchone()
                count = int(row["count"]) if row is not None else 0
                if dry_run:
                    counts[key] = count
                    continue
                if count <= 0:
                    counts[key] = 0
                    continue
                connection.execute(
                    f"""
                    DELETE FROM growth_candidate_queue
                    WHERE {where_clause}
                    """,
                    params,
                )
                deleted_row = connection.execute("SELECT changes() AS count").fetchone()
                counts[key] = int(deleted_row["count"]) if deleted_row is not None else 0

            if not dry_run:
                connection.commit()

        counts["total"] = counts["followed"] + counts["rejected"] + counts["stale"]
        return counts

    def run_db_hygiene(
        self,
        *,
        action_log_retention_days: int,
        graph_sync_runs_retention_days: int,
        candidate_reconciliation_retention_days: int,
        follow_cycle_terminal_retention_days: int,
        snapshots_retention_days: int,
        queue_followed_after_days: int,
        queue_rejected_after_days: int,
        queue_stale_after_days: int,
        dry_run: bool = True,
        vacuum: bool = False,
    ) -> dict[str, int | bool]:
        queue_counts = self.prune_growth_candidate_queue(
            followed_after_days=queue_followed_after_days,
            rejected_after_days=queue_rejected_after_days,
            stale_after_days=queue_stale_after_days,
            dry_run=dry_run,
        )

        action_log_retention_days = max(0, action_log_retention_days)
        graph_sync_runs_retention_days = max(0, graph_sync_runs_retention_days)
        candidate_reconciliation_retention_days = max(0, candidate_reconciliation_retention_days)
        follow_cycle_terminal_retention_days = max(0, follow_cycle_terminal_retention_days)
        snapshots_retention_days = max(0, snapshots_retention_days)

        prune_plan: tuple[tuple[str, str, str, tuple[object, ...]], ...] = (
            (
                "action_log",
                "action_log",
                """
                datetime(COALESCE(timestamp, CURRENT_TIMESTAMP))
                    <= datetime('now', 'utc', ?)
                """,
                (self._utc_modifier_days(action_log_retention_days),),
            ),
            (
                "graph_sync_runs",
                "graph_sync_runs",
                """
                status != 'running'
                AND datetime(COALESCE(ended_at, started_at))
                    <= datetime('now', 'utc', ?)
                AND id NOT IN (
                    SELECT last_sync_run_id
                    FROM own_followers_cache
                    WHERE last_sync_run_id IS NOT NULL
                    UNION
                    SELECT last_sync_run_id
                    FROM own_following_cache
                    WHERE last_sync_run_id IS NOT NULL
                    UNION
                    SELECT last_run_id
                    FROM graph_sync_state
                    WHERE last_run_id IS NOT NULL
                )
                """,
                (self._utc_modifier_days(graph_sync_runs_retention_days),),
            ),
            (
                "candidate_reconciliation",
                "candidate_reconciliation",
                """
                COALESCE(needs_reconcile, 0) = 0
                AND datetime(COALESCE(last_reconciled_at, last_seen_at, first_seen_at))
                    <= datetime('now', 'utc', ?)
                AND NOT EXISTS (
                    SELECT 1
                    FROM growth_candidate_queue q
                    WHERE q.user_id = candidate_reconciliation.user_id
                      AND q.queue_state IN ('queued', 'deferred')
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM follow_cycle f
                    WHERE f.user_id = candidate_reconciliation.user_id
                      AND f.cleanup_status = 'pending'
                )
                """,
                (self._utc_modifier_days(candidate_reconciliation_retention_days),),
            ),
            (
                "follow_cycle_terminal",
                "follow_cycle",
                """
                cleanup_status IN ('followed_back', 'unfollowed_nonreciprocal', 'kept_whitelist', 'skipped')
                AND datetime(COALESCE(updated_at, last_checked_at, followed_back_at, followed_at))
                    <= datetime('now', 'utc', ?)
                """,
                (self._utc_modifier_days(follow_cycle_terminal_retention_days),),
            ),
            (
                "snapshots",
                "snapshots",
                """
                date(COALESCE(date, CURRENT_TIMESTAMP))
                    <= date('now', 'utc', ?)
                """,
                (self._utc_modifier_days(snapshots_retention_days),),
            ),
        )
        deleted_counts: dict[str, int] = {
            "action_log": 0,
            "graph_sync_runs": 0,
            "candidate_reconciliation": 0,
            "follow_cycle_terminal": 0,
            "snapshots": 0,
        }
        with self.database.connect() as connection:
            for key, table_name, where_clause, params in prune_plan:
                row = connection.execute(
                    f"""
                    SELECT COUNT(*) AS count
                    FROM {table_name}
                    WHERE {where_clause}
                    """,
                    params,
                ).fetchone()
                count = int(row["count"]) if row is not None else 0
                if dry_run:
                    deleted_counts[key] = count
                    continue
                if count <= 0:
                    deleted_counts[key] = 0
                    continue
                connection.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE {where_clause}
                    """,
                    params,
                )
                deleted_row = connection.execute("SELECT changes() AS count").fetchone()
                deleted_counts[key] = int(deleted_row["count"]) if deleted_row is not None else 0

            if not dry_run:
                connection.commit()
                if vacuum:
                    connection.execute("VACUUM")

        result: dict[str, int | bool] = {
            "queue_followed": int(queue_counts.get("followed", 0)),
            "queue_rejected": int(queue_counts.get("rejected", 0)),
            "queue_stale": int(queue_counts.get("stale", 0)),
            "action_log": int(deleted_counts.get("action_log", 0)),
            "graph_sync_runs": int(deleted_counts.get("graph_sync_runs", 0)),
            "candidate_reconciliation": int(deleted_counts.get("candidate_reconciliation", 0)),
            "follow_cycle_terminal": int(deleted_counts.get("follow_cycle_terminal", 0)),
            "snapshots": int(deleted_counts.get("snapshots", 0)),
        }
        result["total"] = sum(
            int(result[key])
            for key in (
                "queue_followed",
                "queue_rejected",
                "queue_stale",
                "action_log",
                "graph_sync_runs",
                "candidate_reconciliation",
                "follow_cycle_terminal",
                "snapshots",
            )
        )
        result["dry_run"] = dry_run
        result["vacuum_performed"] = bool(vacuum and not dry_run)
        return result

    def mark_growth_candidate_queue_state(
        self,
        user_id: str,
        *,
        queue_state: str,
        reason: str,
        candidate: CandidateUser | None = None,
        observed_follow_state: UserFollowState | None = None,
        attempted: bool = False,
        followed: bool = False,
        retry_after_at: str | None = None,
    ) -> None:
        if queue_state in {"followed", "rejected"}:
            self.remove_growth_candidate(user_id)
            return

        query = """
        UPDATE growth_candidate_queue
        SET queue_state = ?,
            last_reason = ?,
            username = COALESCE(?, username),
            name = COALESCE(?, name),
            bio = COALESCE(?, bio),
            newsletter_v3_id = COALESCE(?, newsletter_v3_id),
            source_labels = COALESCE(?, source_labels),
            queued_score = COALESCE(?, queued_score),
            follower_count = COALESCE(?, follower_count),
            following_count = COALESCE(?, following_count),
            latest_post_id = COALESCE(?, latest_post_id),
            latest_post_title = COALESCE(?, latest_post_title),
            last_post_created_at = COALESCE(?, last_post_created_at),
            retry_after_at = CASE
                WHEN ? = 'deferred' THEN COALESCE(?, retry_after_at)
                ELSE NULL
            END,
            last_screened_at = CURRENT_TIMESTAMP,
            last_attempted_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE last_attempted_at END,
            last_followed_at = CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE last_followed_at END,
            attempt_count = attempt_count + CASE WHEN ? THEN 1 ELSE 0 END,
            follow_verified_count = follow_verified_count + CASE WHEN ? THEN 1 ELSE 0 END,
            last_observed_follow_state = COALESCE(?, last_observed_follow_state)
        WHERE user_id = ?
        """
        serialized_sources = None
        if candidate is not None:
            serialized_sources = self._serialize_source_labels([source.value for source in candidate.sources])
        with self.database.connect() as connection:
            connection.execute(
                query,
                (
                    queue_state,
                    reason,
                    candidate.username if candidate is not None else None,
                    candidate.name if candidate is not None else None,
                    candidate.bio if candidate is not None else None,
                    candidate.newsletter_v3_id if candidate is not None else None,
                    serialized_sources,
                    candidate.score if candidate is not None else None,
                    candidate.follower_count if candidate is not None else None,
                    candidate.following_count if candidate is not None else None,
                    candidate.latest_post_id if candidate is not None else None,
                    candidate.latest_post_title if candidate is not None else None,
                    candidate.last_post_created_at if candidate is not None else None,
                    queue_state,
                    retry_after_at,
                    1 if attempted else 0,
                    1 if followed else 0,
                    1 if attempted else 0,
                    1 if followed else 0,
                    observed_follow_state.value if observed_follow_state is not None else None,
                    user_id,
                ),
            )
            connection.commit()

    def remove_growth_candidate(self, user_id: str) -> None:
        query = """
        DELETE FROM growth_candidate_queue
        WHERE user_id = ?
        """
        with self.database.connect() as connection:
            connection.execute(query, (user_id,))
            connection.commit()

    def reconciliation_candidates_page(self, *, limit: int, offset: int = 0) -> list[dict[str, str | None]]:
        query = """
        SELECT user_id, MAX(username) AS username, MAX(rank_ts) AS rank_ts
        FROM (
            SELECT user_id, username, last_seen_at AS rank_ts
            FROM candidate_reconciliation
            WHERE needs_reconcile = 1

            UNION

            SELECT user_id, username, updated_at AS rank_ts
            FROM follow_cycle
            WHERE cleanup_status = 'pending'
        )
        GROUP BY user_id
        ORDER BY rank_ts DESC
        LIMIT ? OFFSET ?
        """
        with self.database.connect() as connection:
            rows = connection.execute(query, (limit, offset)).fetchall()
            return [{"user_id": row["user_id"], "username": row["username"]} for row in rows]

    def follow_cycle_kpis(self) -> dict[str, float | int]:
        query = """
        SELECT cleanup_status, COUNT(*) AS count
        FROM follow_cycle
        GROUP BY cleanup_status
        """
        counts: dict[str, int] = {}
        with self.database.connect() as connection:
            rows = connection.execute(query).fetchall()
            for row in rows:
                counts[str(row["cleanup_status"])] = int(row["count"])

        followed_back = counts.get("followed_back", 0)
        nonreciprocal = counts.get("unfollowed_nonreciprocal", 0)
        kept_whitelist = counts.get("kept_whitelist", 0)
        completed = followed_back + nonreciprocal
        follow_back_rate = (followed_back / completed) if completed > 0 else 0.0

        return {
            "follow_cycle_total": sum(counts.values()),
            "follow_cycle_pending": counts.get("pending", 0),
            "follow_cycle_followed_back": followed_back,
            "follow_cycle_unfollowed_nonreciprocal": nonreciprocal,
            "follow_cycle_kept_whitelist": kept_whitelist,
            "follow_back_rate": round(follow_back_rate, 4),
        }

    def follow_cycle_conversion_breakdowns(self) -> tuple[dict[str, dict[str, float | int]], dict[str, dict[str, float | int]], dict[str, dict[str, float | int]]]:
        query = """
        SELECT cleanup_status, growth_policy, growth_sources
        FROM follow_cycle
        """
        with self.database.connect() as connection:
            rows = connection.execute(query).fetchall()

        def _blank_metrics() -> dict[str, float | int]:
            return {
                "total": 0,
                "completed": 0,
                "followed_back": 0,
                "nonreciprocal": 0,
                "follow_back_rate": 0.0,
            }

        def _update(bucket: dict[str, dict[str, float | int]], key: str, cleanup_status: str) -> None:
            metrics = bucket.setdefault(key, _blank_metrics())
            metrics["total"] = int(metrics["total"]) + 1
            if cleanup_status == "followed_back":
                metrics["completed"] = int(metrics["completed"]) + 1
                metrics["followed_back"] = int(metrics["followed_back"]) + 1
            elif cleanup_status == "unfollowed_nonreciprocal":
                metrics["completed"] = int(metrics["completed"]) + 1
                metrics["nonreciprocal"] = int(metrics["nonreciprocal"]) + 1

        by_source: dict[str, dict[str, float | int]] = {}
        by_policy: dict[str, dict[str, float | int]] = {}
        by_source_policy: dict[str, dict[str, float | int]] = {}

        for row in rows:
            cleanup_status = str(row["cleanup_status"] or "").strip() or "unknown"
            growth_policy = str(row["growth_policy"] or "").strip() or "unknown"
            raw_sources = str(row["growth_sources"] or "").strip()
            growth_sources = [item.strip() for item in raw_sources.split(",") if item.strip()]
            if not growth_sources:
                growth_sources = ["unknown"]

            _update(by_policy, growth_policy, cleanup_status)
            for source in growth_sources:
                _update(by_source, source, cleanup_status)
                _update(by_source_policy, f"{source}|{growth_policy}", cleanup_status)

        for bucket in (by_source, by_policy, by_source_policy):
            for metrics in bucket.values():
                completed = int(metrics["completed"])
                followed_back = int(metrics["followed_back"])
                metrics["follow_back_rate"] = round((followed_back / completed) if completed > 0 else 0.0, 4)

        return by_source, by_policy, by_source_policy

    def mark_follow_cycle_started(
        self,
        *,
        user_id: str,
        username: str | None,
        source: str,
        grace_days: int,
        growth_policy: str | None = None,
        growth_sources: list[str] | None = None,
    ) -> None:
        query = """
        INSERT INTO follow_cycle (
            user_id,
            username,
            followed_at,
            follow_source,
            growth_policy,
            growth_sources,
            follow_deadline_at,
            cleanup_status,
            updated_at
        )
        VALUES (
            ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, datetime('now', 'utc', ?), 'pending', CURRENT_TIMESTAMP
        )
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, follow_cycle.username),
            followed_at = CURRENT_TIMESTAMP,
            follow_source = excluded.follow_source,
            growth_policy = excluded.growth_policy,
            growth_sources = excluded.growth_sources,
            follow_deadline_at = datetime('now', 'utc', ?),
            cleanup_status = 'pending',
            updated_at = CURRENT_TIMESTAMP
        """
        modifier = f"+{grace_days} day"
        serialized_sources = ",".join(sorted({item.strip() for item in (growth_sources or []) if item and item.strip()})) or None
        with self.database.connect() as connection:
            connection.execute(
                query,
                (
                    user_id,
                    username,
                    source,
                    growth_policy,
                    serialized_sources,
                    modifier,
                    modifier,
                ),
            )
            connection.commit()

    def mark_followed_back(self, user_id: str) -> None:
        query = """
        UPDATE follow_cycle
        SET cleanup_status = 'followed_back',
            followed_back_at = CURRENT_TIMESTAMP,
            last_checked_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """
        with self.database.connect() as connection:
            connection.execute(query, (user_id,))
            connection.commit()

    def mark_cleanup_checked(self, user_id: str) -> None:
        query = """
        UPDATE follow_cycle
        SET last_checked_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """
        with self.database.connect() as connection:
            connection.execute(query, (user_id,))
            connection.commit()

    def mark_nonreciprocal_unfollowed(self, user_id: str) -> None:
        query = """
        UPDATE follow_cycle
        SET cleanup_status = 'unfollowed_nonreciprocal',
            last_checked_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """
        with self.database.connect() as connection:
            connection.execute(query, (user_id,))
            connection.commit()

    def mark_cleanup_whitelist_kept(self, user_id: str) -> None:
        query = """
        UPDATE follow_cycle
        SET cleanup_status = 'kept_whitelist',
            last_checked_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """
        with self.database.connect() as connection:
            connection.execute(query, (user_id,))
            connection.commit()

    def mark_cleanup_skipped(self, user_id: str) -> None:
        query = """
        UPDATE follow_cycle
        SET cleanup_status = 'skipped',
            last_checked_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """
        with self.database.connect() as connection:
            connection.execute(query, (user_id,))
            connection.commit()

    def pending_nonreciprocal_candidates(self, *, grace_days: int, limit: int) -> list[dict[str, str | None]]:
        query = """
        SELECT user_id, username, followed_at
        FROM follow_cycle
        WHERE cleanup_status = 'pending'
          AND (
              followed_at IS NULL
              OR TRIM(COALESCE(followed_at, '')) = ''
              OR datetime(followed_at) IS NULL
              OR COALESCE(
                  datetime(follow_deadline_at),
                  datetime(followed_at, ?)
              ) <= datetime('now', 'utc')
          )
        ORDER BY
          CASE
              WHEN followed_at IS NULL OR TRIM(COALESCE(followed_at, '')) = '' OR datetime(followed_at) IS NULL THEN 0
              ELSE 1
          END ASC,
          datetime(followed_at) ASC
        LIMIT ?
        """
        fallback_deadline_modifier = f"+{grace_days} day"
        with self.database.connect() as connection:
            rows = connection.execute(
                query,
                (fallback_deadline_modifier, limit),
            ).fetchall()
            return [
                {"user_id": row["user_id"], "username": row["username"], "followed_at": row["followed_at"]}
                for row in rows
            ]

    def upsert_relationship_state(
        self,
        user_id: str,
        newsletter_state: NewsletterState,
        user_follow_state: UserFollowState,
        *,
        confidence: RelationshipConfidence = RelationshipConfidence.OBSERVED,
        source_operation: str | None = None,
        verified_now: bool = True,
    ) -> None:
        query = """
        INSERT INTO relationship_state (
            user_id,
            newsletter_state,
            user_follow_state,
            confidence,
            last_source_operation,
            updated_at,
            last_verified_at
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END)
        ON CONFLICT(user_id) DO UPDATE SET
            newsletter_state = excluded.newsletter_state,
            user_follow_state = excluded.user_follow_state,
            confidence = excluded.confidence,
            last_source_operation = excluded.last_source_operation,
            updated_at = CURRENT_TIMESTAMP,
            last_verified_at = CASE
                WHEN ? THEN CURRENT_TIMESTAMP
                ELSE relationship_state.last_verified_at
            END
        """
        with self.database.connect() as connection:
            connection.execute(
                query,
                (
                    user_id,
                    newsletter_state.value,
                    user_follow_state.value,
                    confidence.value,
                    source_operation,
                    1 if verified_now else 0,
                    1 if verified_now else 0,
                ),
            )
            connection.commit()

    def get_relationship_state(self, user_id: str) -> CanonicalRelationshipState | None:
        query = """
        SELECT
            user_id,
            newsletter_state,
            user_follow_state,
            confidence,
            last_source_operation,
            updated_at,
            last_verified_at
        FROM relationship_state
        WHERE user_id = ?
        """
        with self.database.connect() as connection:
            row = connection.execute(query, (user_id,)).fetchone()
            if row is None:
                return None
            return CanonicalRelationshipState(
                user_id=row["user_id"],
                newsletter_state=NewsletterState(row["newsletter_state"]),
                user_follow_state=UserFollowState(row["user_follow_state"]),
                confidence=RelationshipConfidence(row["confidence"]),
                last_source_operation=row["last_source_operation"],
                updated_at=row["updated_at"],
                last_verified_at=row["last_verified_at"],
            )
