ALTER TABLE node_credentials ADD COLUMN label TEXT;
ALTER TABLE node_credentials ADD COLUMN last_used_at_ms INTEGER;

ALTER TABLE nodes ADD COLUMN bound_agent_id TEXT;
ALTER TABLE nodes ADD COLUMN agent_boot_id TEXT;

ALTER TABLE attempt_logs ADD COLUMN expired_at_ms INTEGER;

CREATE INDEX idx_node_credentials_history
    ON node_credentials(node_name, created_at_ms DESC, credential_id DESC);
CREATE INDEX idx_runs_created_id
    ON runs(created_at_ms DESC, id DESC);
CREATE INDEX idx_runs_trigger_created_id
    ON runs(trigger, created_at_ms DESC, id DESC);
CREATE INDEX idx_attempt_logs_retention
    ON attempt_logs(updated_at_ms, run_id, attempt_no)
    WHERE complete = 1 AND expired_at_ms IS NULL;
