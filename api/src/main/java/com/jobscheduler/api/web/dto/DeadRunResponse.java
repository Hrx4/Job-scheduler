package com.jobscheduler.api.web.dto;

import java.time.OffsetDateTime;
import java.util.UUID;

public record DeadRunResponse(
        UUID runId,
        UUID jobId,
        String type,
        OffsetDateTime dueAt,
        int attemptCount,
        String lastError) {
}
