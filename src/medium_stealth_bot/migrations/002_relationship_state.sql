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
WHERE EXISTS (
    SELECT 1
    FROM sqlite_master
    WHERE type = 'table' AND name = 'relationships'
);
