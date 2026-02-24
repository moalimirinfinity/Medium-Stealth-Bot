CREATE TABLE IF NOT EXISTS candidate_reconciliation (
    user_id TEXT PRIMARY KEY,
    username TEXT,
    newsletter_v3_id TEXT,
    source_labels TEXT,
    last_score REAL,
    last_decision_reason TEXT,
    eligible INTEGER NOT NULL DEFAULT 0,
    needs_reconcile INTEGER NOT NULL DEFAULT 1,
    seen_count INTEGER NOT NULL DEFAULT 1,
    first_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_reconciled_at DATETIME,
    last_observed_follow_state TEXT CHECK(last_observed_follow_state IN ('following', 'not_following', 'unknown'))
);

CREATE INDEX IF NOT EXISTS idx_candidate_reconciliation_needs_reconcile
    ON candidate_reconciliation(needs_reconcile, last_seen_at);
