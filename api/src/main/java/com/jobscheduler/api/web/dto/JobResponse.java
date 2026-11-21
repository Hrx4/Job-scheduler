package com.jobscheduler.api.web.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobscheduler.common.domain.JobStatus;
import com.jobscheduler.common.domain.Priority;
import com.jobscheduler.common.domain.RunStatus;
import java.time.OffsetDateTime;
import java.util.UUID;

/** Job definition. nextRunAt and latestRun are filled on single-job reads, not in lists. */
public record JobResponse(
        UUID jobId,
        String type,
        JsonNode payload,
        String cron,
        String timeZone,
        Priority priority,
        int maxRetries,
        int timeoutSeconds,
        JobStatus status,
        OffsetDateTime createdAt,
        OffsetDateTime nextRunAt,
        RunSummary latestRun) {

    public record RunSummary(UUID runId, RunStatus status, OffsetDateTime dueAt, int attemptCount) {
    }
}
