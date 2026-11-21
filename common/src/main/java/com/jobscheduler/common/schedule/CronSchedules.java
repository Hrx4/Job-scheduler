package com.jobscheduler.common.schedule;

import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import org.springframework.scheduling.support.CronExpression;

/** Cron helpers. Uses Spring's 6-field format: second minute hour day-of-month month day-of-week. */
public final class CronSchedules {
    private CronSchedules() {
    }

    public static boolean isValid(String cron) {
        return cron != null && CronExpression.isValidExpression(cron);
    }

    public static boolean isValidZone(String zone) {
        try {
            ZoneId.of(zone);
            return true;
        } catch (DateTimeException e) {
            return false;
        }
    }

    /** First fire time strictly after {@code after}, evaluated in {@code zone}, returned in UTC. */
    public static Optional<OffsetDateTime> next(String cron, String zone, OffsetDateTime after) {
        ZonedDateTime next = CronExpression.parse(cron).next(after.atZoneSameInstant(ZoneId.of(zone)));
        return Optional.ofNullable(next).map(z -> z.toOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC));
    }
}
