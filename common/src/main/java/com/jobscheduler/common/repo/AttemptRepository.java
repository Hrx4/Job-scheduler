package com.jobscheduler.common.repo;

import com.jobscheduler.common.domain.AttemptOutcome;
import com.jobscheduler.common.domain.JobAttempt;
import com.jobscheduler.common.util.Times;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class AttemptRepository {

    static final RowMapper<JobAttempt> MAPPER = (rs, i) -> {
        String outcome = rs.getString("outcome");
        return new JobAttempt(
                rs.getLong("id"),
                rs.getObject("run_id", UUID.class),
                rs.getInt("attempt_no"),
                rs.getString("worker_id"),
                rs.getObject("started_at", OffsetDateTime.class),
                rs.getObject("finished_at", OffsetDateTime.class),
                outcome == null ? null : AttemptOutcome.valueOf(outcome),
                rs.getString("error"));
    };

    private final NamedParameterJdbcTemplate jdbc;

    public AttemptRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Records the start of an attempt and returns its id. */
    public long start(UUID runId, int attemptNo, String workerId) {
        var p = new MapSqlParameterSource()
                .addValue("run", runId)
                .addValue("no", attemptNo)
                .addValue("worker", workerId)
                .addValue("started", Times.now());
        Long id = jdbc.queryForObject("""
                INSERT INTO job_attempt (run_id, attempt_no, worker_id, started_at)
                VALUES (:run, :no, :worker, :started)
                RETURNING id
                """, p, Long.class);
        return id;
    }

    public void finish(long attemptId, AttemptOutcome outcome, String error) {
        var p = new MapSqlParameterSource()
                .addValue("id", attemptId)
                .addValue("finished", Times.now())
                .addValue("outcome", outcome.name())
                .addValue("error", error, Types.VARCHAR);
        jdbc.update("""
                UPDATE job_attempt SET finished_at = :finished, outcome = :outcome, error = :error
                WHERE id = :id
                """, p);
    }

    /** Attempts of the given runs, ordered by run then attempt number. */
    public List<JobAttempt> findByRunIds(Collection<UUID> runIds) {
        if (runIds.isEmpty()) {
            return List.of();
        }
        return jdbc.query(
                "SELECT * FROM job_attempt WHERE run_id IN (:ids) ORDER BY run_id, attempt_no",
                Map.of("ids", runIds), MAPPER);
    }
}
