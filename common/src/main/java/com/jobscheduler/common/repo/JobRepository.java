package com.jobscheduler.common.repo;

import com.jobscheduler.common.domain.Job;
import com.jobscheduler.common.domain.JobStatus;
import com.jobscheduler.common.domain.Priority;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JobRepository {

    static final RowMapper<Job> MAPPER = (rs, i) -> new Job(
            rs.getObject("id", UUID.class),
            rs.getString("idempotency_key"),
            rs.getString("request_hash"),
            rs.getString("type"),
            rs.getString("payload"),
            rs.getString("cron_expr"),
            rs.getString("time_zone"),
            Priority.fromCode(rs.getShort("priority")),
            rs.getInt("max_retries"),
            rs.getInt("timeout_seconds"),
            JobStatus.valueOf(rs.getString("status")),
            rs.getObject("created_at", OffsetDateTime.class));

    private final NamedParameterJdbcTemplate jdbc;

    public JobRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Inserts the job. Returns false if its idempotency key is already taken. */
    public boolean insertIfAbsent(Job job) {
        var p = new MapSqlParameterSource()
                .addValue("id", job.id())
                .addValue("key", job.idempotencyKey(), Types.VARCHAR)
                .addValue("hash", job.requestHash(), Types.VARCHAR)
                .addValue("type", job.type())
                .addValue("payload", job.payload())
                .addValue("cron", job.cronExpr(), Types.VARCHAR)
                .addValue("tz", job.timeZone())
                .addValue("priority", job.priority().code())
                .addValue("maxRetries", job.maxRetries())
                .addValue("timeout", job.timeoutSeconds())
                .addValue("status", job.status().name())
                .addValue("createdAt", job.createdAt());
        return jdbc.update("""
                INSERT INTO job (id, idempotency_key, request_hash, type, payload, cron_expr, time_zone,
                                 priority, max_retries, timeout_seconds, status, created_at)
                VALUES (:id, :key, :hash, :type, CAST(:payload AS jsonb), :cron, :tz,
                        :priority, :maxRetries, :timeout, :status, :createdAt)
                ON CONFLICT (idempotency_key) DO NOTHING
                """, p) == 1;
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.query("SELECT * FROM job WHERE id = :id", Map.of("id", id), MAPPER)
                .stream().findFirst();
    }

    public Optional<Job> findByIdempotencyKey(String key) {
        return jdbc.query("SELECT * FROM job WHERE idempotency_key = :key", Map.of("key", key), MAPPER)
                .stream().findFirst();
    }

    /** Newest first, keyset-paginated on (created_at, id). */
    public List<Job> list(JobQuery q) {
        var sql = new StringBuilder("SELECT * FROM job WHERE TRUE");
        var p = new MapSqlParameterSource("limit", q.limit());
        if (q.status() != null) {
            sql.append(" AND status = :status");
            p.addValue("status", q.status().name());
        }
        if (q.type() != null) {
            sql.append(" AND type = :type");
            p.addValue("type", q.type());
        }
        if (q.from() != null) {
            sql.append(" AND created_at >= :from");
            p.addValue("from", q.from());
        }
        if (q.to() != null) {
            sql.append(" AND created_at < :to");
            p.addValue("to", q.to());
        }
        if (q.cursorCreatedAt() != null) {
            sql.append(" AND (created_at, id) < (:cAt, :cId)");
            p.addValue("cAt", q.cursorCreatedAt()).addValue("cId", q.cursorId());
        }
        sql.append(" ORDER BY created_at DESC, id DESC LIMIT :limit");
        return jdbc.query(sql.toString(), p, MAPPER);
    }

    /** Moves the job to {@code to} only if it is currently in one of {@code from}. */
    public boolean transition(UUID id, Collection<JobStatus> from, JobStatus to) {
        var p = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("to", to.name())
                .addValue("from", from.stream().map(Enum::name).toList());
        return jdbc.update("UPDATE job SET status = :to WHERE id = :id AND status IN (:from)", p) == 1;
    }
}
