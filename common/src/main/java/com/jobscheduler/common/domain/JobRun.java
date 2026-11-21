package com.jobscheduler.common.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

/** One occurrence of a job that should execute. */
public record JobRun(
        UUID id,
        UUID jobId,
        OffsetDateTime dueAt,
        RunStatus status,
        int attemptCount,
        int attemptBase,
        OffsetDateTime nextAttemptAt,
        Priority priority,
        String leaseOwner,
        OffsetDateTime leaseExpiresAt) {

    public static JobRun pending(UUID jobId, OffsetDateTime dueAt, Priority priority) {
        return new JobRun(UUID.randomUUID(), jobId, dueAt, RunStatus.PENDING, 0, 0, dueAt, priority, null, null);
    }

    /** Attempts used since the run was last (re)queued. Compared against the job's maxRetries. */
    public int attemptsUsed() {
        return attemptCount - attemptBase;
    }
}
