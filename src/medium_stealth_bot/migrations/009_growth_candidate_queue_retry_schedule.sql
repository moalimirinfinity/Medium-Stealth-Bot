ALTER TABLE growth_candidate_queue ADD COLUMN retry_after_at DATETIME;

CREATE INDEX IF NOT EXISTS idx_growth_candidate_queue_ready_window
    ON growth_candidate_queue(queue_state, retry_after_at, queued_score DESC, last_discovered_at DESC);
