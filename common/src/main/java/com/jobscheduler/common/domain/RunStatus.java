package com.jobscheduler.common.domain;

/**
 * Status of one job_run.
 * PENDING -> QUEUED -> RUNNING -> SUCCEEDED, or back to PENDING on failure, or DEAD after the last retry.
 * CANCELLED can be reached from any non-final state.
 */
public enum RunStatus {
    PENDING,
    QUEUED,
    RUNNING,
    SUCCEEDED,
    DEAD,
    CANCELLED;

    public boolean isFinal() {
        return this == SUCCEEDED || this == DEAD || this == CANCELLED;
    }
}
