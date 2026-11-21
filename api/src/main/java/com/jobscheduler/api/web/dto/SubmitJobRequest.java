package com.jobscheduler.api.web.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobscheduler.common.domain.Priority;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Body of POST /api/v1/jobs. Omitted schedule means run once, now. */
public record SubmitJobRequest(
        @NotBlank @Size(max = 64) String type,
        @NotNull JsonNode payload,
        @Valid ScheduleSpec schedule,
        Priority priority,
        @Min(0) @Max(25) Integer maxRetries,
        @Min(1) @Max(86_400) Integer timeoutSeconds) {
}
