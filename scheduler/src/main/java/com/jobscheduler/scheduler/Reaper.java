package com.jobscheduler.scheduler;

import com.jobscheduler.common.domain.RunStatus;
import com.jobscheduler.common.repo.AttemptRepository;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.repo.RunRepository.ReapedRun;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Crash recovery. Every few seconds, takes back runs whose lease expired:
 * QUEUED (publish lost) or RUNNING (worker died) -> PENDING, or DEAD if that was the last attempt.
 * The DuePoller then re-queues PENDING runs as usual, so recovery needs no extra delivery path.
 *
 * <p>Worst case from worker crash to rerun: lease (30 s from the last heartbeat) + reaper interval (5 s)
 * + poll interval (0.5 s) + Kafka delivery. Measured with scripts/chaos-kill-worker.ps1 (ADR-009).
 */
@Component
public class Reaper {

    private static final Logger log = LoggerFactory.getLogger(Reaper.class);

    private final RunRepository runs;
    private final AttemptRepository attempts;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;
    private final int batchSize;

    public Reaper(RunRepository runs, AttemptRepository attempts, TransactionTemplate tx, MeterRegistry meters,
                  @Value("${scheduler.reaper-batch-size:500}") int batchSize) {
        this.runs = runs;
        this.attempts = attempts;
        this.tx = tx;
        this.meters = meters;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${scheduler.reaper-interval-ms:5000}")
    public void reap() {
        List<ReapedRun> reaped = tx.execute(s -> {
            List<ReapedRun> batch = runs.reapExpired(batchSize);
            for (ReapedRun r : batch) {
                if (r.from() == RunStatus.RUNNING) {
                    attempts.markLeaseExpired(r.runId(), r.attemptCount(), r.previousOwner());
                }
            }
            return batch;
        });
        if (reaped == null) {
            return;
        }
        for (ReapedRun r : reaped) {
            meters.counter("reaper_recovered_total", "from", r.from().name(), "to", r.to().name()).increment();
            log.warn("Reaped run {}: {} -> {} (lease held by {}, attempt {})",
                    r.runId(), r.from(), r.to(), r.previousOwner(), r.attemptCount());
        }
    }
}
