package com.jobscheduler.common.execution;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BackoffTest {

    private final Backoff backoff = new Backoff(10_000, 600_000);

    @Test
    void doublesEachAttemptWithJitter() {
        for (int i = 0; i < 100; i++) {
            assertThat(backoff.delay(1).toMillis()).isBetween(10_000L, 20_000L);
            assertThat(backoff.delay(2).toMillis()).isBetween(20_000L, 30_000L);
            assertThat(backoff.delay(4).toMillis()).isBetween(80_000L, 90_000L);
        }
    }

    @Test
    void cappedAndNoOverflow() {
        assertThat(backoff.delay(20).toMillis()).isBetween(600_000L, 610_000L);
        assertThat(backoff.delay(1000).toMillis()).isBetween(600_000L, 610_000L);
    }
}
