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
    action_key TEXT,
    occurred_day_utc TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE UNIQUE,
    follower_count INTEGER,
    following_count INTEGER
);
