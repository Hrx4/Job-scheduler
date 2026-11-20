package com.jobscheduler.common.messaging;

/** Kafka topic names shared by scheduler and worker. */
public final class Topics {
    public static final String JOBS = "jobs";
    public static final int JOBS_PARTITIONS = 12;

    private Topics() {
    }
}
