package com.jobscheduler.common.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.common.domain.AttemptOutcome;
import com.jobscheduler.common.domain.Job;
import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.domain.RunStatus;
import com.jobscheduler.common.repo.AttemptRepository;
import com.jobscheduler.common.repo.JobRepository;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.util.Times;
import jakarta.annotation.PreDestroy;
import java.time.OffsetDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Runs one already-claimed run (status RUNNING, leased to this worker) to completion:
 * records the attempt, calls the handler with a timeout, then does a fenced state update.
 * Shared by the in-process worker now and the Kafka worker later.
 */
@Component
public class RunExecutor {

    private static final Logger log = LoggerFactory.getLogger(RunExecutor.class);
    private static final int MAX_ERROR_LENGTH = 4000;

    private final JobRepository jobs;
    private final RunRepository runs;
    private final AttemptRepository attempts;
    private final HandlerRegistry handlers;
    private final Backoff backoff;
    private final ObjectMapper mapper;
    private final TransactionTemplate tx;
    private final ExecutorService handlerPool = Executors.newVirtualThreadPerTaskExecutor();

    public RunExecutor(JobRepository jobs, RunRepository runs, AttemptRepository attempts,
                       HandlerRegistry handlers, Backoff backoff, ObjectMapper mapper, TransactionTemplate tx) {
        this.jobs = jobs;
        this.runs = runs;
        this.attempts = attempts;
        this.handlers = handlers;
        this.backoff = backoff;
        this.mapper = mapper;
        this.tx = tx;
    }

    public void execute(JobRun run, String workerId) {
        MDC.put("runId", run.id().toString());
        try {
            Job job = jobs.findById(run.jobId())
                    .orElseThrow(() -> new IllegalStateException("Job not found: " + run.jobId()));
            long attemptId = attempts.start(run.id(), run.attemptCount(), workerId);
            Result result = invoke(job, run);
            tx.executeWithoutResult(s -> complete(job, run, workerId, attemptId, result));
        } finally {
            MDC.remove("runId");
        }
    }

    private Result invoke(Job job, JobRun run) {
        Future<?> future;
        try {
            JobHandler handler = handlers.get(job.type());
            JobContext ctx = new JobContext(run.id(), job.id(), run.attemptCount(), mapper.readTree(job.payload()));
            future = handlerPool.submit(() -> {
                handler.handle(ctx);
                return null;
            });
        } catch (Exception e) {
            return Result.failed(e);
        }
        try {
            future.get(job.timeoutSeconds(), TimeUnit.SECONDS);
            return Result.SUCCESS;
        } catch (TimeoutException e) {
            future.cancel(true);
            return new Result(AttemptOutcome.TIMED_OUT, "Timed out after " + job.timeoutSeconds() + " s", true);
        } catch (ExecutionException e) {
            return Result.failed(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            return new Result(AttemptOutcome.FAILED, "Worker interrupted", true);
        }
    }

    private void complete(Job job, JobRun run, String workerId, long attemptId, Result result) {
        attempts.finish(attemptId, result.outcome(), result.error());

        RunStatus next;
        OffsetDateTime nextAttemptAt = null;
        if (result.outcome() == AttemptOutcome.SUCCEEDED) {
            next = RunStatus.SUCCEEDED;
        } else if (result.retryable() && run.attemptsUsed() <= job.maxRetries()) {
            next = RunStatus.PENDING;
            nextAttemptAt = Times.now().plus(backoff.delay(run.attemptsUsed()));
        } else {
            next = RunStatus.DEAD;
        }

        boolean owned = runs.complete(run.id(), workerId, run.attemptCount(), next, nextAttemptAt);
        if (!owned) {
            log.warn("Attempt {} lost its lease or the run was cancelled; result {} discarded",
                    run.attemptCount(), result.outcome());
        } else if (next == RunStatus.PENDING) {
            log.info("Attempt {} {}; retry at {}: {}", run.attemptCount(), result.outcome(), nextAttemptAt, result.error());
        } else {
            log.info("Attempt {} {} -> run {}{}", run.attemptCount(), result.outcome(), next,
                    result.error() == null ? "" : ": " + result.error());
        }
    }

    @PreDestroy
    void shutdown() {
        handlerPool.shutdownNow();
    }

    private record Result(AttemptOutcome outcome, String error, boolean retryable) {
        static final Result SUCCESS = new Result(AttemptOutcome.SUCCEEDED, null, false);

        static Result failed(Throwable t) {
            String msg = t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage());
            if (msg.length() > MAX_ERROR_LENGTH) {
                msg = msg.substring(0, MAX_ERROR_LENGTH);
            }
            return new Result(AttemptOutcome.FAILED, msg, !(t instanceof NonRetryableException));
        }
    }
}
