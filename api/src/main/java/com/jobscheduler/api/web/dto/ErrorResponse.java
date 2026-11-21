package com.jobscheduler.api.web.dto;

/** The one error shape every endpoint returns. */
public record ErrorResponse(String code, String message, String traceId) {
}
