CREATE TABLE operational_counters(
 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
 stored_log_bytes INTEGER NOT NULL CHECK(stored_log_bytes >= 0)
) STRICT;

INSERT INTO operational_counters(singleton, stored_log_bytes)
VALUES(1, (SELECT COALESCE(SUM(stored_bytes), 0) FROM attempt_logs WHERE expired_at_ms IS NULL));

CREATE TRIGGER attempt_logs_counter_insert
AFTER INSERT ON attempt_logs
WHEN NEW.expired_at_ms IS NULL
BEGIN
 UPDATE operational_counters SET stored_log_bytes = stored_log_bytes + NEW.stored_bytes WHERE singleton = 1;
END;

CREATE TRIGGER attempt_logs_counter_update
AFTER UPDATE OF stored_bytes, expired_at_ms ON attempt_logs
BEGIN
 UPDATE operational_counters
 SET stored_log_bytes = stored_log_bytes
   + CASE WHEN NEW.expired_at_ms IS NULL THEN NEW.stored_bytes ELSE 0 END
   - CASE WHEN OLD.expired_at_ms IS NULL THEN OLD.stored_bytes ELSE 0 END
 WHERE singleton = 1;
END;

CREATE TRIGGER attempt_logs_counter_delete
AFTER DELETE ON attempt_logs
WHEN OLD.expired_at_ms IS NULL
BEGIN
 UPDATE operational_counters SET stored_log_bytes = stored_log_bytes - OLD.stored_bytes WHERE singleton = 1;
END;
