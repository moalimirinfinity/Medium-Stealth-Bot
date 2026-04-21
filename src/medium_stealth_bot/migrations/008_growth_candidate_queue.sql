CREATE TABLE IF NOT EXISTS growth_candidate_queue (
    user_id TEXT PRIMARY KEY,
    username TEXT,
    name TEXT,
    bio TEXT,
    newsletter_v3_id TEXT,
    source_labels TEXT,
    queued_score REAL NOT NULL DEFAULT 0,
    follower_count INTEGER,
    following_count INTEGER,
    latest_post_id TEXT,
    latest_post_title TEXT,
    last_post_created_at TEXT,
    queue_state TEXT NOT NULL DEFAULT 'queued'
        CHECK(queue_state IN ('queued', 'deferred', 'rejected', 'followed')),
    last_reason TEXT,
    discover_count INTEGER NOT NULL DEFAULT 1,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    follow_verified_count INTEGER NOT NULL DEFAULT 0,
    first_discovered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_discovered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_screened_at DATETIME,
    last_attempted_at DATETIME,
    last_followed_at DATETIME,
    last_observed_follow_state TEXT
        CHECK(last_observed_follow_state IN ('following', 'not_following', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_growth_candidate_queue_state_score
    ON growth_candidate_queue(queue_state, queued_score DESC, last_discovered_at DESC);

CREATE INDEX IF NOT EXISTS idx_growth_candidate_queue_last_discovered
    ON growth_candidate_queue(last_discovered_at DESC);
