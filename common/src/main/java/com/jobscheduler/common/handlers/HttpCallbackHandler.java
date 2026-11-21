package com.jobscheduler.common.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobscheduler.common.execution.JobContext;
import com.jobscheduler.common.execution.JobHandler;
import com.jobscheduler.common.execution.NonRetryableException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.springframework.stereotype.Component;

/**
 * POSTs payload.body to payload.url with an Idempotency-Key header.
 * 4xx (except 408/429) is non-retryable; 5xx and network errors are retried.
 */
@Component
public class HttpCallbackHandler implements JobHandler {

    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Override
    public String type() {
        return "HTTP_CALLBACK";
    }

    @Override
    public void handle(JobContext ctx) throws Exception {
        JsonNode p = ctx.payload();
        URI uri;
        try {
            uri = URI.create(p.path("url").asText(""));
        } catch (IllegalArgumentException e) {
            throw new NonRetryableException("payload.url is not a valid URL", e);
        }
        if (!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())) {
            throw new NonRetryableException("payload.url must be http or https");
        }
        JsonNode body = p.path("body");
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", ctx.idempotencyKey())
                .POST(HttpRequest.BodyPublishers.ofString(body.isMissingNode() ? "{}" : body.toString()))
                .build();

        int status = client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        if (status >= 400 && status < 500 && status != 408 && status != 429) {
            throw new NonRetryableException("Callback returned " + status);
        }
        if (status >= 400) {
            throw new IllegalStateException("Callback returned " + status);
        }
    }
}
