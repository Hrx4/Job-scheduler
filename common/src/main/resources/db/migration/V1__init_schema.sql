-- job: what to run (one row per submitted job)
CREATE TABLE job (
    id               UUID PRIMARY KEY,
    idempotency_key  VARCHAR(128) UNIQUE,
    type             VARCHAR(64)  NOT NULL,
    payload          JSONB        NOT NULL,
    cron_expr        VARCHAR(64),                       -- null for one-time jobs
    time_zone        VARCHAR(64)  NOT NULL DEFAULT 'UTC',
    priority         SMALLINT     NOT NULL DEFAULT 1,   -- 0 HIGH, 1 NORMAL, 2 LOW
    max_retries      INT          NOT NULL DEFAULT 3,
    timeout_seconds  INT          NOT NULL DEFAULT 300,
    status           VARCHAR(16)  NOT NULL,             -- ACTIVE, PAUSED, CANCELLED
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- list endpoint: filter by status/type, newest first
CREATE INDEX idx_job_list ON job (status, type, created_at DESC);

-- job_run: each time a job should run (one for one-time, one per fire time for cron)
CREATE TABLE job_run (
    id                UUID PRIMARY KEY,
    job_id            UUID         NOT NULL REFERENCES job(id),
    due_at            TIMESTAMPTZ  NOT NULL,            -- fire time this run belongs to
    status            VARCHAR(16)  NOT NULL,            -- PENDING, QUEUED, RUNNING, SUCCEEDED, DEAD, CANCELLED
    attempt_count     INT          NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ  NOT NULL,
    priority          SMALLINT     NOT NULL,
    lease_owner       VARCHAR(64),
    lease_expires_at  TIMESTAMPTZ,
    version           BIGINT       NOT NULL DEFAULT 0,  -- optimistic locking
    UNIQUE (job_id, due_at)                             -- one run per fire time, even if two schedulers race
);

-- scheduler hot query: due runs, best priority first
CREATE INDEX idx_run_due ON job_run (next_attempt_at, priority)
    WHERE status = 'PENDING';

-- reaper query: runs whose lease has expired
CREATE INDEX idx_run_lease ON job_run (lease_expires_at)
    WHERE status IN ('QUEUED', 'RUNNING');

-- dead-letter listing
CREATE INDEX idx_run_dead ON job_run (job_id)
    WHERE status = 'DEAD';

-- job_attempt: each try of a run, kept for history
CREATE TABLE job_attempt (
    id           BIGSERIAL PRIMARY KEY,
    run_id       UUID         NOT NULL REFERENCES job_run(id),
    attempt_no   INT          NOT NULL,
    worker_id    VARCHAR(64)  NOT NULL,
    started_at   TIMESTAMPTZ  NOT NULL,
    finished_at  TIMESTAMPTZ,
    outcome      VARCHAR(16),                           -- SUCCEEDED, FAILED, TIMED_OUT
    error        TEXT,
    UNIQUE (run_id, attempt_no)
);
