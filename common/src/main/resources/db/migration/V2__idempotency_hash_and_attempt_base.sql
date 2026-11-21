-- Hash of the submit request body, so a reused Idempotency-Key with a different body returns 409.
ALTER TABLE job ADD COLUMN request_hash VARCHAR(64);

-- Attempts made before the last manual requeue. Retries left = max_retries - (attempt_count - attempt_base).
-- attempt_count itself never goes down: it is the fencing token and the job_attempt.attempt_no.
ALTER TABLE job_run ADD COLUMN attempt_base INT NOT NULL DEFAULT 0;
