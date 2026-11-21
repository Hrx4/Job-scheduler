package com.jobscheduler.common.execution;

/**
 * Runs one job type. Must be idempotent: the same run can be delivered more than once,
 * so pass {@link JobContext#idempotencyKey()} to anything with side effects.
 *
 * <p>Throw {@link NonRetryableException} for errors a retry cannot fix (bad payload, 4xx).
 * Any other exception is retried with backoff. Long handlers should respond to interrupts:
 * the thread is interrupted on timeout.
 */
public interface JobHandler {

    String type();

    void handle(JobContext ctx) throws Exception;
}
