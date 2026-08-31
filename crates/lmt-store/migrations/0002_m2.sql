ALTER TABLE mirror_schedule_state ADD COLUMN schedule_hash TEXT;
ALTER TABLE nodes ADD COLUMN max_concurrent_runs INTEGER NOT NULL DEFAULT 1 CHECK(max_concurrent_runs > 0);
ALTER TABLE runs ADD COLUMN scheduled_for_at_ms INTEGER;
ALTER TABLE runs ADD COLUMN retry_due_at_ms INTEGER;

CREATE INDEX idx_schedule_next_due
    ON mirror_schedule_state(next_due_at_ms)
    WHERE next_due_at_ms IS NOT NULL;
CREATE INDEX idx_schedule_due_intent
    ON mirror_schedule_state(catch_up_since_ms)
    WHERE catch_up_pending = 1;
CREATE INDEX idx_runs_retry_due
    ON runs(retry_due_at_ms)
    WHERE retry_due_at_ms IS NOT NULL AND state = 'running';
