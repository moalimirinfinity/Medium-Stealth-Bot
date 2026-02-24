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
GROUP BY target_id;
