package com.jobscheduler.common.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobscheduler.common.execution.JobContext;
import com.jobscheduler.common.execution.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Fake report generator, handy for testing.
 * Payload: {"durationMs": 500, "failUntilAttempt": 0}: sleeps durationMs,
 * then fails on every attempt up to and including failUntilAttempt.
 */
@Component
public class GenerateReportHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(GenerateReportHandler.class);

    @Override
    public String type() {
        return "GENERATE_REPORT";
    }

    @Override
    public void handle(JobContext ctx) throws Exception {
        JsonNode p = ctx.payload();
        Thread.sleep(p.path("durationMs").asLong(500));
        if (ctx.attempt() <= p.path("failUntilAttempt").asInt(0)) {
            throw new IllegalStateException("Simulated failure on attempt " + ctx.attempt());
        }
        log.info("Report {} generated", p.path("reportId").asText("?"));
    }
}
