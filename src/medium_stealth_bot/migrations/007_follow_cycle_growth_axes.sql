ALTER TABLE follow_cycle ADD COLUMN growth_policy TEXT;
ALTER TABLE follow_cycle ADD COLUMN growth_sources TEXT;

CREATE INDEX IF NOT EXISTS idx_follow_cycle_growth_policy
    ON follow_cycle(growth_policy);
