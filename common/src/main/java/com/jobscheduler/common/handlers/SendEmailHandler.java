package com.jobscheduler.common.handlers;

import com.jobscheduler.common.execution.JobContext;
import com.jobscheduler.common.execution.JobHandler;
import com.jobscheduler.common.execution.NonRetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Fake email sender. Payload: {"to": "...", "subject": "..."}. */
@Component
public class SendEmailHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(SendEmailHandler.class);

    @Override
    public String type() {
        return "SEND_EMAIL";
    }

    @Override
    public void handle(JobContext ctx) throws Exception {
        if (!ctx.payload().hasNonNull("to")) {
            throw new NonRetryableException("payload.to is required");
        }
        // A real provider call would send ctx.idempotencyKey() so a redelivery never sends twice.
        Thread.sleep(100);
        log.info("Sent email to {} (idempotency key {})", ctx.payload().get("to").asText(), ctx.idempotencyKey());
    }
}
