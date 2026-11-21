package com.jobscheduler.common.execution;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** delay = min(base * 2^(attempt-1), cap) + random(0, base). Jitter stops failures retrying in lockstep. */
@Component
public class Backoff {

    private final long baseMs;
    private final long capMs;

    public Backoff(@Value("${jobs.retry.base-ms:10000}") long baseMs,
                   @Value("${jobs.retry.cap-ms:600000}") long capMs) {
        this.baseMs = baseMs;
        this.capMs = capMs;
    }

    /** @param attempt 1-based number of the attempt that just failed */
    public Duration delay(int attempt) {
        int shift = Math.min(Math.max(attempt - 1, 0), 30);
        long exponential = Math.min(baseMs << shift, capMs);
        long jitter = ThreadLocalRandom.current().nextLong(baseMs + 1);
        return Duration.ofMillis(exponential + jitter);
    }
}
