package com.jobscheduler.common.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

/** One try of a run, kept for history. */
public record JobAttempt(
        long id,
        UUID runId,
        int attemptNo,
        String workerId,
        OffsetDateTime startedAt,
        OffsetDateTime finishedAt,
        AttemptOutcome outcome,
        String error) {
}
