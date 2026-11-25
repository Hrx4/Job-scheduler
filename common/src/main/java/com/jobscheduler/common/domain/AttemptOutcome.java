package com.jobscheduler.common.domain;

/** Outcome of one job_attempt. */
public enum AttemptOutcome {
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    /** Worker stopped the handler because it no longer owned the run: cancelled, or lease lost. */
    ABANDONED
}
