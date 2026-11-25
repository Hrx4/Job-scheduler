package com.jobscheduler.api.localworker;

import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.execution.RunExecutor;
import com.jobscheduler.common.repo.JobRepository;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.schedule.NextRunPlanner;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Weeks 1-2 stand-in for scheduler + Kafka + workers: polls Postgres for due runs and runs them in this process.
 * Turned off (local-worker.enabled=false) once the scheduler and worker services take over.
 */
@Component
@ConditionalOnProperty(name = "local-worker.enabled", havingValue = "true")
public class LocalWorker {

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);

    private final RunRepository runs;
    private final JobRepository jobs;
    private final NextRunPlanner planner;
    private final RunExecutor executor;
    private final TransactionTemplate tx;
    private final int leaseSeconds;
    private final Semaphore slots;
    private final ExecutorService pool;
    private final String workerId = "local-" + UUID.randomUUID().toString().substring(0, 8);

    public LocalWorker(RunRepository runs, JobRepository jobs, NextRunPlanner planner, RunExecutor executor,
                       TransactionTemplate tx,
                       @Value("${local-worker.threads:8}") int threads,
                       @Value("${jobs.lease-seconds:30}") int leaseSeconds) {
        this.runs = runs;
        this.jobs = jobs;
        this.planner = planner;
        this.executor = executor;
        this.tx = tx;
        this.leaseSeconds = leaseSeconds;
        this.slots = new Semaphore(threads);
        this.pool = Executors.newFixedThreadPool(threads);
        log.info("In-process worker {} started with {} threads", workerId, threads);
    }

    @Scheduled(fixedDelayString = "${local-worker.poll-interval-ms:500}")
    public void poll() {
        int free = slots.availablePermits();
        if (free == 0) {
            return;
        }
        List<JobRun> claimed = tx.execute(s -> claim(free));
        for (JobRun run : claimed) {
            slots.acquireUninterruptibly();
            pool.execute(() -> {
                try {
                    executor.execute(run, workerId);
                } catch (Exception e) {
                    // Run stays RUNNING until the reaper (weeks 5-6) recovers its expired lease.
                    log.error("Run {} failed outside its handler", run.id(), e);
                } finally {
                    slots.release();
                }
            });
        }
    }

    /** Claim and plan the next cron run in one transaction, so a fire time is never lost or doubled. */
    private List<JobRun> claim(int limit) {
        List<JobRun> claimed = runs.claimDue(workerId, leaseSeconds, limit);
        for (JobRun run : claimed) {
            if (run.attemptCount() == 1) { // first try of this fire time, not a retry or requeue
                jobs.findById(run.jobId()).ifPresent(job -> planner.planNext(job, run.dueAt()));
            }
        }
        return claimed;
    }

    @PreDestroy
    void stop() throws InterruptedException {
        pool.shutdown();
        if (!pool.awaitTermination(15, TimeUnit.SECONDS)) {
            log.warn("In-flight runs did not finish before shutdown");
            pool.shutdownNow();
        }
    }
}
