from medium_stealth_bot.database import Database
from medium_stealth_bot.models import (
    CanonicalRelationshipState,
    NewsletterState,
    RelationshipConfidence,
    UserFollowState,
)


class ActionRepository:
    def __init__(self, database: Database):
        self.database = database

    @staticmethod
    def _utc_modifier_hours(hours: int) -> str:
        return f"-{hours} hours"

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
        newsletter_id: str | None = None,
        bio: str | None = None,
    ) -> None:
        query = """
        INSERT INTO users (user_id, username, newsletter_id, bio, last_scraped_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, users.username),
            newsletter_id = COALESCE(excluded.newsletter_id, users.newsletter_id),
            bio = COALESCE(excluded.bio, users.bio),
            last_scraped_at = CURRENT_TIMESTAMP
        """
        with self.database.connect() as connection:
            connection.execute(query, (user_id, username, newsletter_id, bio))
            connection.commit()

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

    def mark_follow_cycle_started(
        self,
        *,
        user_id: str,
        username: str | None,
        source: str,
        grace_days: int,
    ) -> None:
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
        VALUES (
            ?, ?, CURRENT_TIMESTAMP, ?, datetime('now', 'utc', ?), 'pending', CURRENT_TIMESTAMP
        )
        ON CONFLICT(user_id) DO UPDATE SET
            username = COALESCE(excluded.username, follow_cycle.username),
            followed_at = CURRENT_TIMESTAMP,
            follow_source = excluded.follow_source,
            follow_deadline_at = datetime('now', 'utc', ?),
            cleanup_status = 'pending',
            updated_at = CURRENT_TIMESTAMP
        """
        modifier = f"+{grace_days} day"
        with self.database.connect() as connection:
            connection.execute(query, (user_id, username, source, modifier, modifier))
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

    def pending_nonreciprocal_candidates(self, *, grace_days: int, limit: int) -> list[dict[str, str | None]]:
        query = """
        SELECT user_id, username, followed_at
        FROM follow_cycle
        WHERE cleanup_status = 'pending'
          AND COALESCE(
              follow_deadline_at,
              datetime(followed_at, ?),
              datetime('now', 'utc', ?)
          ) <= datetime('now', 'utc')
        ORDER BY followed_at ASC
        LIMIT ?
        """
        fallback_deadline_modifier = f"+{grace_days} day"
        unknown_followed_at_fallback_modifier = f"-{grace_days} day"
        with self.database.connect() as connection:
            rows = connection.execute(
                query,
                (fallback_deadline_modifier, unknown_followed_at_fallback_modifier, limit),
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
