package com.jobscheduler.common.repo;

import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.domain.Priority;
import com.jobscheduler.common.domain.RunStatus;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RunRepository {

    public static final RowMapper<JobRun> MAPPER = (rs, i) -> new JobRun(
            rs.getObject("id", UUID.class),
            rs.getObject("job_id", UUID.class),
            rs.getObject("due_at", OffsetDateTime.class),
            RunStatus.valueOf(rs.getString("status")),
            rs.getInt("attempt_count"),
            rs.getInt("attempt_base"),
            rs.getObject("next_attempt_at", OffsetDateTime.class),
            Priority.fromCode(rs.getShort("priority")),
            rs.getString("lease_owner"),
            rs.getObject("lease_expires_at", OffsetDateTime.class));

    private final NamedParameterJdbcTemplate jdbc;

    public RunRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts a PENDING run. Returns false if a run for the same (job, due time) already exists. */
    public boolean insert(JobRun run) {
        var p = new MapSqlParameterSource()
                .addValue("id", run.id())
                .addValue("jobId", run.jobId())
                .addValue("dueAt", run.dueAt())
                .addValue("status", run.status().name())
                .addValue("next", run.nextAttemptAt())
                .addValue("priority", run.priority().code());
        return jdbc.update("""
                INSERT INTO job_run (id, job_id, due_at, status, next_attempt_at, priority)
                VALUES (:id, :jobId, :dueAt, :status, :next, :priority)
                ON CONFLICT (job_id, due_at) DO NOTHING
                """, p) == 1;
    }

    public Optional<JobRun> findById(UUID id) {
        return jdbc.query("SELECT * FROM job_run WHERE id = :id", Map.of("id", id), MAPPER)
                .stream().findFirst();
    }

    /** Newest due time first. */
    public List<JobRun> findByJobId(UUID jobId, int limit) {
        return jdbc.query(
                "SELECT * FROM job_run WHERE job_id = :jobId ORDER BY due_at DESC LIMIT :limit",
                Map.of("jobId", jobId, "limit", limit), MAPPER);
    }

    public Optional<JobRun> findLatest(UUID jobId) {
        return findByJobId(jobId, 1).stream().findFirst();
    }

    /** Earliest time a PENDING run of this job is due to be tried. */
    public Optional<OffsetDateTime> findNextPendingAt(UUID jobId) {
        List<OffsetDateTime> rows = jdbc.query(
                "SELECT min(next_attempt_at) FROM job_run WHERE job_id = :jobId AND status = 'PENDING'",
                Map.of("jobId", jobId), (rs, i) -> rs.getObject(1, OffsetDateTime.class));
        return rows.isEmpty() ? Optional.empty() : Optional.ofNullable(rows.get(0));
    }

    /** Cancels every non-final run. A RUNNING run's worker is fenced off when it tries to complete. */
    public int cancelOpenRuns(UUID jobId) {
        return jdbc.update("""
                UPDATE job_run SET status = 'CANCELLED', lease_owner = NULL, lease_expires_at = NULL,
                       version = version + 1
                WHERE job_id = :jobId AND status IN ('PENDING', 'QUEUED', 'RUNNING')
                """, Map.of("jobId", jobId));
    }

    public int cancelPendingRuns(UUID jobId) {
        return jdbc.update("""
                UPDATE job_run SET status = 'CANCELLED', version = version + 1
                WHERE job_id = :jobId AND status = 'PENDING'
                """, Map.of("jobId", jobId));
    }

    /**
     * Scheduler step: moves due runs PENDING -> QUEUED so they can be published to Kafka.
     * The lease on QUEUED covers the dual-write gap: if the publish never happens, the reaper
     * returns the run to PENDING after the lease expires.
     * SKIP LOCKED lets several scheduler instances poll at once without taking the same row.
     */
    public List<JobRun> queueDue(int leaseSeconds, int limit) {
        var p = new MapSqlParameterSource()
                .addValue("lease", leaseSeconds)
                .addValue("limit", limit);
        return jdbc.query("""
                UPDATE job_run SET status = 'QUEUED', lease_owner = NULL,
                       lease_expires_at = now() + make_interval(secs => :lease),
                       version = version + 1
                WHERE id IN (
                    SELECT r.id FROM job_run r JOIN job j ON j.id = r.job_id
                    WHERE r.status = 'PENDING' AND r.next_attempt_at <= now() AND j.status = 'ACTIVE'
                    ORDER BY r.priority, r.next_attempt_at
                    LIMIT :limit
                    FOR UPDATE OF r SKIP LOCKED)
                RETURNING *
                """, p, MAPPER);
    }

    /**
     * Worker step: claim one run delivered by Kafka. QUEUED -> RUNNING, lease to this worker, attempt + 1.
     * Empty result = someone else has it, it was cancelled, or this is a duplicate delivery: skip the message.
     * The new attempt_count is the fencing token for every later write by this worker.
     */
    public Optional<JobRun> claimQueued(UUID runId, String workerId, int leaseSeconds) {
        var p = new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("worker", workerId)
                .addValue("lease", leaseSeconds);
        return jdbc.query("""
                UPDATE job_run SET status = 'RUNNING', lease_owner = :worker,
                       lease_expires_at = now() + make_interval(secs => :lease),
                       attempt_count = attempt_count + 1, version = version + 1
                WHERE id = :id AND status = 'QUEUED'
                RETURNING *
                """, p, MAPPER).stream().findFirst();
    }

    /**
     * Claims due runs straight from PENDING to RUNNING for an in-process worker (no Kafka).
     * SKIP LOCKED lets several pollers run at once without taking the same row.
     * Runs of PAUSED or CANCELLED jobs are skipped.
     */
    public List<JobRun> claimDue(String workerId, int leaseSeconds, int limit) {
        var p = new MapSqlParameterSource()
                .addValue("worker", workerId)
                .addValue("lease", leaseSeconds)
                .addValue("limit", limit);
        return jdbc.query("""
                UPDATE job_run SET status = 'RUNNING', lease_owner = :worker,
                       lease_expires_at = now() + make_interval(secs => :lease),
                       attempt_count = attempt_count + 1, version = version + 1
                WHERE id IN (
                    SELECT r.id FROM job_run r JOIN job j ON j.id = r.job_id
                    WHERE r.status = 'PENDING' AND r.next_attempt_at <= now() AND j.status = 'ACTIVE'
                    ORDER BY r.priority, r.next_attempt_at
                    LIMIT :limit
                    FOR UPDATE OF r SKIP LOCKED)
                RETURNING *
                """, p, MAPPER);
    }

    /**
     * Heartbeat: push the lease forward while the handler runs. Same fencing WHERE as completion.
     * Returns false if this worker no longer owns the attempt (run cancelled, or lease expired and
     * someone else took over): the caller must stop the handler.
     */
    public boolean extendLease(UUID runId, String workerId, int attempt, int leaseSeconds) {
        var p = new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("worker", workerId)
                .addValue("attempt", attempt)
                .addValue("lease", leaseSeconds);
        return jdbc.update("""
                UPDATE job_run SET lease_expires_at = now() + make_interval(secs => :lease)
                WHERE id = :id AND status = 'RUNNING' AND lease_owner = :worker AND attempt_count = :attempt
                """, p) == 1;
    }

    /**
     * Fenced completion: only succeeds if this worker still owns this attempt.
     * Returns false if the lease was lost or the run was cancelled; the caller must discard its result.
     *
     * @param nextAttemptAt when going back to PENDING for a retry; null otherwise
     */
    public boolean complete(UUID runId, String workerId, int attempt, RunStatus status, OffsetDateTime nextAttemptAt) {
        var p = new MapSqlParameterSource()
                .addValue("id", runId)
                .addValue("worker", workerId)
                .addValue("attempt", attempt)
                .addValue("status", status.name())
                .addValue("next", nextAttemptAt);
        String setNext = nextAttemptAt != null ? ", next_attempt_at = :next" : "";
        return jdbc.update("""
                UPDATE job_run SET status = :status, lease_owner = NULL, lease_expires_at = NULL,
                       version = version + 1%s
                WHERE id = :id AND status = 'RUNNING' AND lease_owner = :worker AND attempt_count = :attempt
                """.formatted(setNext), p) == 1;
    }

    /** A run the reaper took back: what it was, what it is now, and who held it. */
    public record ReapedRun(UUID runId, UUID jobId, RunStatus from, RunStatus to, String previousOwner,
                            int attemptCount) {
    }

    /**
     * Reaper: runs whose lease expired go back to PENDING (due now), so the normal scheduler path
     * re-queues them.
     * <ul>
     *   <li>QUEUED + expired: the Kafka publish was lost, or no worker claimed it. No attempt was used.</li>
     *   <li>RUNNING + expired: the worker died or stalled past its lease. That attempt counts; if it was
     *       the last one allowed, the run goes to DEAD instead.</li>
     * </ul>
     * SKIP LOCKED lets every scheduler instance run the reaper safely.
     */
    public List<ReapedRun> reapExpired(int limit) {
        return jdbc.query("""
                WITH expired AS (
                    SELECT r.id, r.status, r.lease_owner, r.attempt_count, r.attempt_base, j.max_retries
                    FROM job_run r JOIN job j ON j.id = r.job_id
                    WHERE r.status IN ('QUEUED', 'RUNNING') AND r.lease_expires_at < now()
                    ORDER BY r.lease_expires_at
                    LIMIT :limit
                    FOR UPDATE OF r SKIP LOCKED)
                UPDATE job_run r SET
                       status = CASE WHEN e.status = 'RUNNING' AND e.attempt_count - e.attempt_base > e.max_retries
                                     THEN 'DEAD' ELSE 'PENDING' END,
                       next_attempt_at = now(), lease_owner = NULL, lease_expires_at = NULL,
                       version = r.version + 1
                FROM expired e
                WHERE r.id = e.id
                RETURNING r.id, r.job_id, e.status AS from_status, r.status AS to_status,
                          e.lease_owner AS previous_owner, r.attempt_count
                """, Map.of("limit", limit), (rs, i) -> new ReapedRun(
                rs.getObject("id", UUID.class),
                rs.getObject("job_id", UUID.class),
                RunStatus.valueOf(rs.getString("from_status")),
                RunStatus.valueOf(rs.getString("to_status")),
                rs.getString("previous_owner"),
                rs.getInt("attempt_count")));
    }

    /** DEAD -> PENDING with a fresh retry budget. attempt_count is kept: it is the fencing token. */
    public boolean requeueDead(UUID runId) {
        return jdbc.update("""
                UPDATE job_run SET status = 'PENDING', attempt_base = attempt_count, next_attempt_at = now(),
                       lease_owner = NULL, lease_expires_at = NULL, version = version + 1
                WHERE id = :id AND status = 'DEAD'
                """, Map.of("id", runId)) == 1;
    }
}
