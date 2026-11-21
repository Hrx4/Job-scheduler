package com.jobscheduler.api.web.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.time.Instant;

/**
 * {"kind": "ONCE", "runAt": "2026-12-01T10:00:00Z"} (runAt optional, default now) or
 * {"kind": "CRON", "cron": "0 0 9 * * MON-FRI", "timeZone": "Asia/Kolkata"} (timeZone optional, default UTC).
 */
public record ScheduleSpec(
        @NotNull Kind kind,
        Instant runAt,
        @Size(max = 64) String cron,
        @Size(max = 64) String timeZone) {

    public enum Kind { ONCE, CRON }
}
