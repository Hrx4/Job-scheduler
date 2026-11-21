package com.jobscheduler.common.execution;

/** Thrown by a handler when retrying cannot help. The run goes straight to DEAD. */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
