package com.jobscheduler.api.web.dto;

import java.util.List;

/** nextCursor is absent on the last page. */
public record PageResponse<T>(List<T> items, String nextCursor) {
}
