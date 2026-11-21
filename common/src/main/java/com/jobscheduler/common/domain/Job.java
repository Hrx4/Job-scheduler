package com.jobscheduler.common.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

/** A job definition: what to run. Payload is raw JSON text. */
public record Job(
        UUID id,
        String idempotencyKey,
        String requestHash,
        String type,
        String payload,
        String cronExpr,
        String timeZone,
        Priority priority,
        int maxRetries,
        int timeoutSeconds,
        JobStatus status,
        OffsetDateTime createdAt) {

    public boolean isRecurring() {
        return cronExpr != null;
    }
}
