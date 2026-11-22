package com.jobscheduler.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/** Scans repositories, the shared RunExecutor and the job handlers from common. */
@SpringBootApplication(scanBasePackages = {
        "com.jobscheduler.worker",
        "com.jobscheduler.common.repo",
        "com.jobscheduler.common.execution",
        "com.jobscheduler.common.handlers"})
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }

    /**
     * If the claim itself throws (Postgres briefly down), retry the record 5 times, 1 s apart, then skip it.
     * Skipping is safe: the run is still QUEUED with a lease, so the reaper returns it to PENDING.
     */
    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, 5));
    }
}
