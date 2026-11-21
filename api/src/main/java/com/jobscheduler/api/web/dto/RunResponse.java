package com.jobscheduler.api.web.dto;

import com.jobscheduler.common.domain.AttemptOutcome;
import com.jobscheduler.common.domain.RunStatus;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public record RunResponse(
        UUID runId,
        UUID jobId,
        OffsetDateTime dueAt,
        RunStatus status,
        int attemptCount,
        OffsetDateTime nextAttemptAt,
        String leaseOwner,
        OffsetDateTime leaseExpiresAt,
        List<Attempt> attempts) {

    public record Attempt(
            int attemptNo,
            String workerId,
            OffsetDateTime startedAt,
            OffsetDateTime finishedAt,
            AttemptOutcome outcome,
            String error) {
    }
}
