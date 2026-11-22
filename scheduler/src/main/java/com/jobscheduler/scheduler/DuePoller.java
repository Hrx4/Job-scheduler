package com.jobscheduler.scheduler;

import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.messaging.Topics;
import com.jobscheduler.common.repo.JobRepository;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.schedule.NextRunPlanner;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Moves due runs from Postgres into Kafka.
 *
 * <ol>
 *   <li>One transaction: PENDING -> QUEUED (with a lease) for a batch, and plan the next cron run.</li>
 *   <li>After commit: publish one message per run to the jobs topic, key = value = runId.</li>
 * </ol>
 *
 * Publishing after commit means a crash in between leaves a run QUEUED with no message.
 * The QUEUED lease handles that: once it expires the reaper puts the run back to PENDING.
 */
@Component
public class DuePoller {

    private static final Logger log = LoggerFactory.getLogger(DuePoller.class);

    private final RunRepository runs;
    private final JobRepository jobs;
    private final NextRunPlanner planner;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;
    private final int queuedLeaseSeconds;
    private final Counter queuedTotal;
    private final Counter publishFailures;

    public DuePoller(RunRepository runs, JobRepository jobs, NextRunPlanner planner, TransactionTemplate tx,
                     KafkaTemplate<String, String> kafka, MeterRegistry meters,
                     @Value("${scheduler.batch-size:500}") int batchSize,
                     @Value("${scheduler.queued-lease-seconds:60}") int queuedLeaseSeconds) {
        this.runs = runs;
        this.jobs = jobs;
        this.planner = planner;
        this.tx = tx;
        this.kafka = kafka;
        this.batchSize = batchSize;
        this.queuedLeaseSeconds = queuedLeaseSeconds;
        this.queuedTotal = meters.counter("runs_queued_total");
        this.publishFailures = meters.counter("runs_publish_failures_total");
    }

    @Scheduled(fixedDelayString = "${scheduler.poll-interval-ms:500}")
    public void poll() {
        List<JobRun> queued = tx.execute(s -> queueBatch());
        if (queued == null || queued.isEmpty()) {
            return;
        }
        queuedTotal.increment(queued.size());
        publish(queued);
    }

    private List<JobRun> queueBatch() {
        List<JobRun> queued = runs.queueDue(queuedLeaseSeconds, batchSize);
        for (JobRun run : queued) {
            if (run.attemptCount() == 0) { // first try of this fire time, not a retry or requeue
                jobs.findById(run.jobId()).ifPresent(job -> planner.planNext(job, run.dueAt()));
            }
        }
        return queued;
    }

    /** Sends the whole batch, then waits for acks so the next poll does not pile up unsent messages. */
    private void publish(List<JobRun> queued) {
        CompletableFuture<?>[] sends = queued.stream()
                .map(run -> {
                    String runId = run.id().toString();
                    return kafka.send(Topics.JOBS, runId, runId).whenComplete((result, error) -> {
                        if (error != null) {
                            publishFailures.increment();
                            log.warn("Publish of run {} failed; reaper will re-queue it after the lease", runId, error);
                        }
                    });
                })
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(sends).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            // Each failure is already logged above; the runs stay QUEUED until their lease expires.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
