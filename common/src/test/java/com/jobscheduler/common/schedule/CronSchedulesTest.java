package com.jobscheduler.common.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

class CronSchedulesTest {

    private static final String WEEKDAYS_9AM = "0 0 9 * * MON-FRI";

    @Test
    void evaluatesInJobTimeZone() {
        // Thu 2026-11-19 15:30 IST -> next is Fri 09:00 IST = 03:30 UTC
        var next = CronSchedules.next(WEEKDAYS_9AM, "Asia/Kolkata", OffsetDateTime.parse("2026-11-19T10:00:00Z"));
        assertThat(next).contains(OffsetDateTime.parse("2026-11-20T03:30:00Z"));
    }

    @Test
    void skipsWeekend() {
        var next = CronSchedules.next(WEEKDAYS_9AM, "Asia/Kolkata", OffsetDateTime.parse("2026-11-20T10:00:00Z"));
        assertThat(next).contains(OffsetDateTime.parse("2026-11-23T03:30:00Z"));
    }

    @Test
    void validates() {
        assertThat(CronSchedules.isValid(WEEKDAYS_9AM)).isTrue();
        assertThat(CronSchedules.isValid("0 9 * * *")).isFalse(); // 5-field unix cron is not accepted
        assertThat(CronSchedules.isValid(null)).isFalse();
        assertThat(CronSchedules.isValidZone("Asia/Kolkata")).isTrue();
        assertThat(CronSchedules.isValidZone("Mars/Base")).isFalse();
    }
}
