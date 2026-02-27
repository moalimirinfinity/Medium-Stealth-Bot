CREATE TABLE IF NOT EXISTS graph_sync_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL CHECK(mode IN ('auto', 'manual', 'command')),
    source_path TEXT,
    status TEXT NOT NULL CHECK(status IN ('running', 'success', 'failed', 'skipped')),
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at DATETIME,
    followers_count INTEGER NOT NULL DEFAULT 0,
    following_count INTEGER NOT NULL DEFAULT 0,
    imported_pending_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_graph_sync_runs_started_at
    ON graph_sync_runs(started_at DESC);

CREATE TABLE IF NOT EXISTS own_followers_cache (
    user_id TEXT PRIMARY KEY,
    username TEXT,
    name TEXT,
    bio TEXT,
    follower_count INTEGER,
    following_count INTEGER,
    newsletter_v3_id TEXT,
    first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_sync_run_id INTEGER,
    FOREIGN KEY(last_sync_run_id) REFERENCES graph_sync_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_own_followers_cache_last_seen
    ON own_followers_cache(last_seen_at DESC);

CREATE TABLE IF NOT EXISTS own_following_cache (
    user_id TEXT PRIMARY KEY,
    username TEXT,
    name TEXT,
    bio TEXT,
    follower_count INTEGER,
    following_count INTEGER,
    newsletter_v3_id TEXT,
    first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_sync_run_id INTEGER,
    FOREIGN KEY(last_sync_run_id) REFERENCES graph_sync_runs(id)
);

CREATE INDEX IF NOT EXISTS idx_own_following_cache_last_seen
    ON own_following_cache(last_seen_at DESC);

CREATE TABLE IF NOT EXISTS graph_sync_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    last_success_at DATETIME,
    last_run_id INTEGER,
    last_mode TEXT,
    last_followers_count INTEGER NOT NULL DEFAULT 0,
    last_following_count INTEGER NOT NULL DEFAULT 0,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(last_run_id) REFERENCES graph_sync_runs(id)
);

INSERT OR IGNORE INTO graph_sync_state (
    id,
    last_success_at,
    last_run_id,
    last_mode,
    last_followers_count,
    last_following_count,
    updated_at
) VALUES (1, NULL, NULL, NULL, 0, 0, CURRENT_TIMESTAMP);
