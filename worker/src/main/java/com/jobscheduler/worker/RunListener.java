package com.jobscheduler.worker;

import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.execution.RunExecutor;
import com.jobscheduler.common.messaging.Topics;
import com.jobscheduler.common.repo.RunRepository;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes runIds from Kafka and runs them.
 *
 * <ol>
 *   <li>No free handler thread? nack: Kafka redelivers this record shortly. Nothing is claimed.</li>
 *   <li>Claim in Postgres (QUEUED -> RUNNING). This is the guard against duplicate delivery.</li>
 *   <li>Commit the Kafka offset, only after the claim. A crash before the claim means redelivery.</li>
 *   <li>Run the handler on the handler pool, so the consumer thread keeps polling.</li>
 * </ol>
 */
@Component
public class RunListener {

    private static final Logger log = LoggerFactory.getLogger(RunListener.class);
    private static final Duration BACK_OFF_WHEN_FULL = Duration.ofMillis(200);

    private final RunRepository runs;
    private final RunExecutor executor;
    private final WorkerIdentity identity;
    private final MeterRegistry meters;
    private final int leaseSeconds;
    private final Semaphore slots;
    private final ExecutorService pool;

    public RunListener(RunRepository runs, RunExecutor executor, WorkerIdentity identity, MeterRegistry meters,
                       @Value("${jobs.lease-seconds:30}") int leaseSeconds,
                       @Value("${worker.handler-threads:8}") int handlerThreads) {
        this.runs = runs;
        this.executor = executor;
        this.identity = identity;
        this.meters = meters;
        this.leaseSeconds = leaseSeconds;
        this.slots = new Semaphore(handlerThreads);
        this.pool = Executors.newFixedThreadPool(handlerThreads);
        log.info("Worker {} ready with {} handler threads", identity.id(), handlerThreads);
    }

    @KafkaListener(topics = Topics.JOBS, concurrency = "${worker.consumers:3}")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (!slots.tryAcquire()) {
            ack.nack(BACK_OFF_WHEN_FULL);
            return;
        }
        boolean handedOff = false;
        try {
            UUID runId = parseRunId(record.value());
            if (runId == null) {
                ack.acknowledge(); // poison message: drop it, it can never be claimed
                return;
            }
            Optional<JobRun> claimed = runs.claimQueued(runId, identity.id(), leaseSeconds);
            ack.acknowledge();
            if (claimed.isEmpty()) {
                meters.counter("worker_claims_total", "result", "skipped").increment();
                log.debug("Run {} not QUEUED (duplicate, cancelled or taken); skipped", runId);
                return;
            }
            meters.counter("worker_claims_total", "result", "claimed").increment();
            JobRun run = claimed.get();
            pool.execute(() -> {
                try {
                    executor.execute(run, identity.id());
                } catch (Exception e) {
                    // Run stays RUNNING until its lease expires and the reaper (weeks 5-6) recovers it.
                    log.error("Run {} failed outside its handler", run.id(), e);
                } finally {
                    slots.release();
                }
            });
            handedOff = true;
        } finally {
            if (!handedOff) {
                slots.release();
            }
        }
    }

    private static UUID parseRunId(String value) {
        try {
            return UUID.fromString(value);
        } catch (RuntimeException e) {
            log.warn("Dropping message with invalid runId: {}", value);
            return null;
        }
    }

    /** Runs after the listener containers stop, so no new work arrives while in-flight runs finish. */
    @PreDestroy
    void stop() throws InterruptedException {
        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            log.warn("In-flight runs did not finish before shutdown; their leases will expire");
            pool.shutdownNow();
        }
    }
}
