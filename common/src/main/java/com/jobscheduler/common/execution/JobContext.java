package com.jobscheduler.common.execution;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

public record JobContext(UUID runId, UUID jobId, int attempt, JsonNode payload) {

    /** Stable across retries and redeliveries of the same run. */
    public String idempotencyKey() {
        return runId.toString();
    }
}
