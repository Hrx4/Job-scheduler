package com.jobscheduler.common.repo;

import com.jobscheduler.common.domain.JobStatus;
import java.time.OffsetDateTime;
import java.util.UUID;

/** Filters for listing jobs. Null fields are ignored. Cursor is (createdAt, id) of the last row seen. */
public record JobQuery(
        JobStatus status,
        String type,
        OffsetDateTime from,
        OffsetDateTime to,
        OffsetDateTime cursorCreatedAt,
        UUID cursorId,
        int limit) {
}
