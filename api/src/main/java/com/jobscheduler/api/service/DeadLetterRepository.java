package com.jobscheduler.api.service;

import com.jobscheduler.api.web.Cursor;
import com.jobscheduler.api.web.dto.DeadRunResponse;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Read model for the dead-letter list: DEAD runs with their job type and last error. */
@Repository
public class DeadLetterRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public DeadLetterRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<DeadRunResponse> find(String type, Cursor cursor, int limit) {
        var sql = new StringBuilder("""
                SELECT r.id, r.job_id, j.type, r.due_at, r.attempt_count,
                       (SELECT a.error FROM job_attempt a WHERE a.run_id = r.id
                        ORDER BY a.attempt_no DESC LIMIT 1) AS last_error
                FROM job_run r JOIN job j ON j.id = r.job_id
                WHERE r.status = 'DEAD'
                """);
        var p = new MapSqlParameterSource("limit", limit);
        if (type != null) {
            sql.append(" AND j.type = :type");
            p.addValue("type", type);
        }
        if (cursor != null) {
            sql.append(" AND (r.due_at, r.id) < (:cAt, :cId)");
            p.addValue("cAt", cursor.at()).addValue("cId", cursor.id());
        }
        sql.append(" ORDER BY r.due_at DESC, r.id DESC LIMIT :limit");
        return jdbc.query(sql.toString(), p, (rs, i) -> new DeadRunResponse(
                rs.getObject("id", UUID.class),
                rs.getObject("job_id", UUID.class),
                rs.getString("type"),
                rs.getObject("due_at", OffsetDateTime.class),
                rs.getInt("attempt_count"),
                rs.getString("last_error")));
    }
}
