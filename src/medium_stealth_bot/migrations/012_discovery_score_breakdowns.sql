ALTER TABLE growth_candidate_queue ADD COLUMN score_breakdown_json TEXT;
ALTER TABLE candidate_reconciliation ADD COLUMN last_score_breakdown_json TEXT;
ALTER TABLE follow_cycle ADD COLUMN score_breakdown_json TEXT;
