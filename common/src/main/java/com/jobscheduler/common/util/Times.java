package com.jobscheduler.common.util;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

/** UTC timestamps truncated to Postgres precision (microseconds), so values round-trip exactly. */
public final class Times {
    private Times() {
    }

    public static OffsetDateTime now() {
        return OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
    }

    public static OffsetDateTime utc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS);
    }
}
