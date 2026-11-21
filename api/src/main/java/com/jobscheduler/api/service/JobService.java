package com.jobscheduler.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobscheduler.api.web.ApiException;
import com.jobscheduler.api.web.Cursor;
import com.jobscheduler.api.web.dto.DeadRunResponse;
import com.jobscheduler.api.web.dto.JobResponse;
import com.jobscheduler.api.web.dto.PageResponse;
import com.jobscheduler.api.web.dto.RunResponse;
import com.jobscheduler.api.web.dto.ScheduleSpec;
import com.jobscheduler.api.web.dto.SubmitJobRequest;
import com.jobscheduler.common.domain.Job;
import com.jobscheduler.common.domain.JobAttempt;
import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.domain.JobStatus;
import com.jobscheduler.common.domain.Priority;
import com.jobscheduler.common.domain.RunStatus;
import com.jobscheduler.common.execution.HandlerRegistry;
import com.jobscheduler.common.repo.AttemptRepository;
import com.jobscheduler.common.repo.JobQuery;
import com.jobscheduler.common.repo.JobRepository;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.schedule.CronSchedules;
import com.jobscheduler.common.schedule.NextRunPlanner;
import com.jobscheduler.common.util.Times;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobService {

    static final int MAX_PAYLOAD_BYTES = 64 * 1024;
    static final int DEFAULT_MAX_RETRIES = 3;
    static final int DEFAULT_TIMEOUT_SECONDS = 300;
    static final int MAX_PAGE = 200;

    private final JobRepository jobs;
    private final RunRepository runs;
    private final AttemptRepository attempts;
    private final DeadLetterRepository deadLetters;
    private final HandlerRegistry handlers;
    private final NextRunPlanner planner;
    private final ObjectMapper mapper;
    private final MeterRegistry meters;

    public JobService(JobRepository jobs, RunRepository runs, AttemptRepository attempts,
                      DeadLetterRepository deadLetters, HandlerRegistry handlers, NextRunPlanner planner,
                      ObjectMapper mapper, MeterRegistry meters) {
        this.jobs = jobs;
        this.runs = runs;
        this.attempts = attempts;
        this.deadLetters = deadLetters;
        this.handlers = handlers;
        this.planner = planner;
        this.mapper = mapper;
        this.meters = meters;
    }

    public record SubmitResult(JobResponse job, boolean created) {
    }

    /** Inserts the job and its first run in one transaction. Same key + same body returns the original job. */
    @Transactional
    public SubmitResult submit(String idempotencyKey, SubmitJobRequest req) {
        if (idempotencyKey != null && (idempotencyKey.isBlank() || idempotencyKey.length() > 128)) {
            throw ApiException.badRequest("INVALID_IDEMPOTENCY_KEY", "Idempotency-Key must be 1-128 characters");
        }
        String hash = idempotencyKey == null ? null : hash(req);
        if (idempotencyKey != null) {
            Optional<Job> existing = jobs.findByIdempotencyKey(idempotencyKey);
            if (existing.isPresent()) {
                return replay(existing.get(), hash);
            }
        }

        if (!handlers.supports(req.type())) {
            throw ApiException.badRequest("UNKNOWN_TYPE",
                    "Unknown job type " + req.type() + "; known types: " + handlers.types());
        }
        String payload = req.payload().toString();
        if (payload.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES) {
            throw ApiException.badRequest("PAYLOAD_TOO_LARGE", "payload is over 64 KB");
        }

        ScheduleSpec s = req.schedule();
        String cron = null;
        String zone = "UTC";
        OffsetDateTime firstDue;
        if (s == null || s.kind() == ScheduleSpec.Kind.ONCE) {
            firstDue = s == null || s.runAt() == null ? Times.now() : Times.utc(s.runAt());
        } else {
            if (!CronSchedules.isValid(s.cron())) {
                throw ApiException.badRequest("INVALID_CRON",
                        "cron must be a 6-field expression: second minute hour day-of-month month day-of-week");
            }
            if (s.timeZone() != null) {
                if (!CronSchedules.isValidZone(s.timeZone())) {
                    throw ApiException.badRequest("INVALID_TIME_ZONE", "Unknown time zone " + s.timeZone());
                }
                zone = s.timeZone();
            }
            cron = s.cron();
            firstDue = CronSchedules.next(cron, zone, Times.now())
                    .orElseThrow(() -> ApiException.badRequest("INVALID_CRON", "cron expression never fires"));
        }

        Priority priority = req.priority() == null ? Priority.NORMAL : req.priority();
        Job job = new Job(UUID.randomUUID(), idempotencyKey, hash, req.type(), payload, cron, zone, priority,
                Objects.requireNonNullElse(req.maxRetries(), DEFAULT_MAX_RETRIES),
                Objects.requireNonNullElse(req.timeoutSeconds(), DEFAULT_TIMEOUT_SECONDS),
                JobStatus.ACTIVE, Times.now());

        if (!jobs.insertIfAbsent(job)) {
            // A concurrent request with the same key committed first.
            return replay(jobs.findByIdempotencyKey(idempotencyKey).orElseThrow(), hash);
        }
        runs.insert(JobRun.pending(job.id(), firstDue, priority));
        meters.counter("jobs_submitted_total", "type", job.type()).increment();
        return new SubmitResult(toDetail(job), true);
    }

    public JobResponse get(UUID jobId) {
        return toDetail(requireJob(jobId));
    }

    public PageResponse<JobResponse> list(JobStatus status, String type, OffsetDateTime from, OffsetDateTime to,
                                          String cursor, int limit) {
        Cursor c = Cursor.decode(cursor);
        int size = clamp(limit);
        List<Job> page = jobs.list(new JobQuery(status, type, from, to,
                c == null ? null : c.at(), c == null ? null : c.id(), size));
        String next = page.size() == size
                ? new Cursor(page.get(size - 1).createdAt(), page.get(size - 1).id()).encode()
                : null;
        return new PageResponse<>(page.stream().map(j -> toResponse(j, null, null)).toList(), next);
    }

    public List<RunResponse> runs(UUID jobId, int limit) {
        requireJob(jobId);
        List<JobRun> page = runs.findByJobId(jobId, clamp(limit));
        Map<UUID, List<JobAttempt>> byRun = attempts.findByRunIds(page.stream().map(JobRun::id).toList())
                .stream().collect(Collectors.groupingBy(JobAttempt::runId));
        return page.stream().map(r -> toRunResponse(r, byRun.getOrDefault(r.id(), List.of()))).toList();
    }

    /** Pending runs never start; a running run's result is discarded by the fenced completion. */
    @Transactional
    public JobResponse cancel(UUID jobId) {
        Job job = requireJob(jobId);
        if (job.status() != JobStatus.CANCELLED) {
            jobs.transition(jobId, List.of(JobStatus.ACTIVE, JobStatus.PAUSED), JobStatus.CANCELLED);
            runs.cancelOpenRuns(jobId);
        }
        return get(jobId);
    }

    @Transactional
    public JobResponse pause(UUID jobId) {
        Job job = requireRecurring(jobId);
        if (job.status() != JobStatus.PAUSED
                && !jobs.transition(jobId, List.of(JobStatus.ACTIVE), JobStatus.PAUSED)) {
            throw ApiException.conflict("INVALID_STATE", "Job is " + job.status());
        }
        return get(jobId);
    }

    /** Resumes from now: a run whose time passed during the pause is skipped, not fired late. */
    @Transactional
    public JobResponse resume(UUID jobId) {
        Job job = requireRecurring(jobId);
        if (job.status() == JobStatus.ACTIVE) {
            return get(jobId);
        }
        if (!jobs.transition(jobId, List.of(JobStatus.PAUSED), JobStatus.ACTIVE)) {
            throw ApiException.conflict("INVALID_STATE", "Job is " + job.status());
        }
        OffsetDateTime now = Times.now();
        Optional<OffsetDateTime> pendingAt = runs.findNextPendingAt(jobId);
        if (pendingAt.isEmpty() || pendingAt.get().isBefore(now)) {
            runs.cancelPendingRuns(jobId);
            planner.planNext(job, now);
        }
        return get(jobId);
    }

    public PageResponse<DeadRunResponse> deadLetter(String type, String cursor, int limit) {
        int size = clamp(limit);
        List<DeadRunResponse> page = deadLetters.find(type, Cursor.decode(cursor), size);
        String next = page.size() == size
                ? new Cursor(page.get(size - 1).dueAt(), page.get(size - 1).runId()).encode()
                : null;
        return new PageResponse<>(page, next);
    }

    /** DEAD -> PENDING with a fresh retry budget. */
    @Transactional
    public RunResponse requeue(UUID runId) {
        JobRun run = runs.findById(runId).orElseThrow(() -> ApiException.notFound("Run", runId));
        if (run.status() != RunStatus.DEAD) {
            throw ApiException.conflict("NOT_DEAD", "Run is " + run.status() + "; only DEAD runs can be requeued");
        }
        if (requireJob(run.jobId()).status() == JobStatus.CANCELLED) {
            throw ApiException.conflict("JOB_CANCELLED", "Job " + run.jobId() + " is cancelled");
        }
        if (!runs.requeueDead(runId)) {
            throw ApiException.conflict("NOT_DEAD", "Run changed state; try again");
        }
        JobRun updated = runs.findById(runId).orElseThrow();
        return toRunResponse(updated, attempts.findByRunIds(List.of(runId)));
    }

    // ---- helpers ----

    private SubmitResult replay(Job existing, String hash) {
        if (!Objects.equals(existing.requestHash(), hash)) {
            throw ApiException.conflict("IDEMPOTENCY_KEY_REUSED",
                    "Idempotency-Key was already used with a different request body");
        }
        return new SubmitResult(toDetail(existing), false);
    }

    private Job requireJob(UUID jobId) {
        return jobs.findById(jobId).orElseThrow(() -> ApiException.notFound("Job", jobId));
    }

    private Job requireRecurring(UUID jobId) {
        Job job = requireJob(jobId);
        if (!job.isRecurring()) {
            throw ApiException.conflict("NOT_RECURRING", "Only recurring jobs can be paused or resumed");
        }
        return job;
    }

    private JobResponse toDetail(Job job) {
        JobRun latest = runs.findLatest(job.id()).orElse(null);
        OffsetDateTime nextRunAt = job.status() == JobStatus.ACTIVE
                ? runs.findNextPendingAt(job.id()).orElse(null)
                : null;
        return toResponse(job, latest, nextRunAt);
    }

    private JobResponse toResponse(Job job, JobRun latest, OffsetDateTime nextRunAt) {
        JobResponse.RunSummary summary = latest == null ? null
                : new JobResponse.RunSummary(latest.id(), latest.status(), latest.dueAt(), latest.attemptCount());
        return new JobResponse(job.id(), job.type(), readJson(job.payload()), job.cronExpr(),
                job.isRecurring() ? job.timeZone() : null, job.priority(), job.maxRetries(), job.timeoutSeconds(),
                job.status(), job.createdAt(), nextRunAt, summary);
    }

    private static RunResponse toRunResponse(JobRun r, List<JobAttempt> attemptList) {
        return new RunResponse(r.id(), r.jobId(), r.dueAt(), r.status(), r.attemptCount(), r.nextAttemptAt(),
                r.leaseOwner(), r.leaseExpiresAt(),
                attemptList.stream().map(a -> new RunResponse.Attempt(a.attemptNo(), a.workerId(),
                        a.startedAt(), a.finishedAt(), a.outcome(), a.error())).toList());
    }

    private JsonNode readJson(String json) {
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored payload is not valid JSON", e);
        }
    }

    private String hash(SubmitJobRequest req) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(mapper.writeValueAsBytes(req));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException | JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static int clamp(int limit) {
        return Math.max(1, Math.min(limit, MAX_PAGE));
    }
}
