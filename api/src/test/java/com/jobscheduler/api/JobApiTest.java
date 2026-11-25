package com.jobscheduler.api;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "local-worker.enabled=true", // API tests run jobs in-process, no Kafka needed
                "local-worker.poll-interval-ms=100",
                "jobs.lease-seconds=2",            // short lease + fast heartbeat so tests run in seconds
                "jobs.heartbeat-interval-ms=300",
                "jobs.retry.base-ms=50",
                "jobs.retry.cap-ms=200"
        })
@Testcontainers(disabledWithoutDocker = true)
@Import(JobApiTest.Containers.class)
class JobApiTest {

    /** Container is a Spring bean, so it stops after the app context, not before the poller. */
    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        PostgreSQLContainer<?> postgres() {
            return new PostgreSQLContainer<>("postgres:16-alpine");
        }
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    void oneTimeJobRunsToSuccess() {
        var res = post("/api/v1/jobs", null, """
                {"type": "GENERATE_REPORT", "payload": {"durationMs": 50}}""");
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(res.getHeaders().getLocation()).isNotNull();
        String jobId = res.getBody().get("jobId").asText();

        await().atMost(10, SECONDS).until(() -> latestRun(jobId).path("status").asText().equals("SUCCEEDED"));

        JsonNode run = get("/api/v1/jobs/" + jobId + "/runs").getBody().get(0);
        assertThat(run.get("attemptCount").asInt()).isEqualTo(1);
        assertThat(run.get("attempts").get(0).get("outcome").asText()).isEqualTo("SUCCEEDED");
    }

    @Test
    void sameIdempotencyKeyReturnsOriginalJob() {
        String key = "test-" + UUID.randomUUID();
        String body = """
                {"type": "SEND_EMAIL", "payload": {"to": "a@example.com"},
                 "schedule": {"kind": "ONCE", "runAt": "%s"}}""".formatted(inOneHour());

        var first = post("/api/v1/jobs", key, body);
        var second = post("/api/v1/jobs", key, body);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("jobId")).isEqualTo(first.getBody().get("jobId"));

        var different = post("/api/v1/jobs", key, body.replace("a@example.com", "b@example.com"));
        assertThat(different.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(different.getBody().get("code").asText()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
    }

    @Test
    void failingJobRetriesThenGoesDeadAndCanBeRequeued() {
        String jobId = post("/api/v1/jobs", null, """
                {"type": "GENERATE_REPORT", "payload": {"durationMs": 10, "failUntilAttempt": 99},
                 "maxRetries": 1}""").getBody().get("jobId").asText();

        await().atMost(10, SECONDS).until(() -> latestRun(jobId).path("status").asText().equals("DEAD"));
        JsonNode run = get("/api/v1/jobs/" + jobId + "/runs").getBody().get(0);
        String runId = run.get("runId").asText();
        assertThat(run.get("attemptCount").asInt()).isEqualTo(2); // 1 try + 1 retry
        assertThat(run.get("attempts").get(1).get("error").asText()).contains("Simulated failure");

        JsonNode dead = get("/api/v1/dead-letter?type=GENERATE_REPORT&limit=200").getBody().get("items");
        assertThat(dead.findValuesAsText("runId")).contains(runId);

        var requeued = post("/api/v1/runs/" + runId + "/requeue", null, null);
        assertThat(requeued.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Fresh budget: two more attempts, numbered 3 and 4.
        await().atMost(10, SECONDS).until(() -> {
            JsonNode latest = latestRun(jobId);
            return latest.path("status").asText().equals("DEAD") && latest.path("attemptCount").asInt() == 4;
        });
    }

    @Test
    void attemptOverTimeoutIsTimedOut() {
        String jobId = post("/api/v1/jobs", null, """
                {"type": "GENERATE_REPORT", "payload": {"durationMs": 10000},
                 "timeoutSeconds": 1, "maxRetries": 0}""").getBody().get("jobId").asText();

        await().atMost(10, SECONDS).until(() -> latestRun(jobId).path("status").asText().equals("DEAD"));
        JsonNode attempt = get("/api/v1/jobs/" + jobId + "/runs").getBody().get(0).get("attempts").get(0);
        assertThat(attempt.get("outcome").asText()).isEqualTo("TIMED_OUT");
    }

    @Test
    void heartbeatKeepsLeaseAliveForJobLongerThanTheLease() {
        // 4 s job, 2 s lease: without heartbeats the lease would expire mid-run.
        String jobId = post("/api/v1/jobs", null, """
                {"type": "GENERATE_REPORT", "payload": {"durationMs": 4000}}""").getBody().get("jobId").asText();

        await().atMost(10, SECONDS).until(() -> latestRun(jobId).path("status").asText().equals("RUNNING"));
        Instant firstLease = Instant.parse(firstRun(jobId).get("leaseExpiresAt").asText());
        await().atMost(3, SECONDS).until(() ->
                Instant.parse(firstRun(jobId).get("leaseExpiresAt").asText()).isAfter(firstLease));

        await().atMost(10, SECONDS).until(() -> latestRun(jobId).path("status").asText().equals("SUCCEEDED"));
        assertThat(firstRun(jobId).get("attempts")).hasSize(1);
    }

    @Test
    void cancellingRunningJobStopsItAtNextHeartbeat() {
        String jobId = post("/api/v1/jobs", null, """
                {"type": "GENERATE_REPORT", "payload": {"durationMs": 30000}, "timeoutSeconds": 60}""")
                .getBody().get("jobId").asText();
        await().atMost(10, SECONDS).until(() -> latestRun(jobId).path("status").asText().equals("RUNNING"));

        post("/api/v1/jobs/" + jobId + "/cancel", null, null);

        // Stopped within ~1 heartbeat (300 ms), not after the 30 s job or 60 s timeout.
        await().atMost(3, SECONDS).until(() ->
                firstRun(jobId).get("attempts").get(0).path("outcome").asText().equals("ABANDONED"));
        assertThat(firstRun(jobId).get("status").asText()).isEqualTo("CANCELLED");
    }

    @Test
    void cronJobFiresRepeatedlyAndCanBePausedAndResumed() {
        String jobId = post("/api/v1/jobs", null, """
                {"type": "GENERATE_REPORT", "payload": {"durationMs": 10},
                 "schedule": {"kind": "CRON", "cron": "* * * * * *"}}""").getBody().get("jobId").asText();

        await().atMost(10, SECONDS).until(() -> succeededRuns(jobId) >= 2);

        var paused = post("/api/v1/jobs/" + jobId + "/pause", null, null);
        assertThat(paused.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(paused.getBody().get("status").asText()).isEqualTo("PAUSED");

        var resumed = post("/api/v1/jobs/" + jobId + "/resume", null, null);
        assertThat(resumed.getBody().get("status").asText()).isEqualTo("ACTIVE");
        assertThat(resumed.getBody().get("nextRunAt")).isNotNull();
        int before = succeededRuns(jobId);
        await().atMost(10, SECONDS).until(() -> succeededRuns(jobId) > before);

        post("/api/v1/jobs/" + jobId + "/cancel", null, null);
    }

    @Test
    void cancelledJobNeverRuns() {
        String jobId = post("/api/v1/jobs", null, """
                {"type": "SEND_EMAIL", "payload": {"to": "a@example.com"},
                 "schedule": {"kind": "ONCE", "runAt": "%s"}}""".formatted(inOneHour()))
                .getBody().get("jobId").asText();

        var cancelled = post("/api/v1/jobs/" + jobId + "/cancel", null, null);
        assertThat(cancelled.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(cancelled.getBody().get("status").asText()).isEqualTo("CANCELLED");
        assertThat(cancelled.getBody().get("latestRun").get("status").asText()).isEqualTo("CANCELLED");
    }

    @Test
    void listIsCursorPaginated() {
        String type = "HTTP_CALLBACK"; // only this test uses it
        for (int i = 0; i < 3; i++) {
            post("/api/v1/jobs", null, """
                    {"type": "%s", "payload": {"url": "http://localhost:1"},
                     "schedule": {"kind": "ONCE", "runAt": "%s"}}""".formatted(type, inOneHour()));
        }
        JsonNode page1 = get("/api/v1/jobs?type=" + type + "&limit=2").getBody();
        assertThat(page1.get("items")).hasSize(2);
        String cursor = page1.get("nextCursor").asText();

        JsonNode page2 = get("/api/v1/jobs?type=" + type + "&limit=2&cursor=" + cursor).getBody();
        assertThat(page2.get("items")).hasSize(1);
        assertThat(page2.has("nextCursor")).isFalse();

        List<String> ids = new ArrayList<>(page1.findValuesAsText("jobId"));
        ids.addAll(page2.findValuesAsText("jobId"));
        assertThat(ids).doesNotHaveDuplicates().hasSize(3);
    }

    @Test
    void rejectsBadRequests() {
        assertError(post("/api/v1/jobs", null, """
                {"type": "NOPE", "payload": {}}"""), HttpStatus.BAD_REQUEST, "UNKNOWN_TYPE");
        assertError(post("/api/v1/jobs", null, """
                {"type": "SEND_EMAIL", "payload": {}, "schedule": {"kind": "CRON", "cron": "0 9 * * *"}}"""),
                HttpStatus.BAD_REQUEST, "INVALID_CRON");
        assertError(post("/api/v1/jobs", null, """
                {"type": "SEND_EMAIL"}"""), HttpStatus.BAD_REQUEST, "VALIDATION_FAILED");
        assertError(post("/api/v1/jobs", null, "not json"), HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST");
        assertError(get("/api/v1/jobs/" + UUID.randomUUID()), HttpStatus.NOT_FOUND, "NOT_FOUND");

        var noKey = rest.getForEntity("/api/v1/jobs", JsonNode.class);
        assertError(noKey, HttpStatus.UNAUTHORIZED, "UNAUTHORIZED");
    }

    // ---- helpers ----

    private ResponseEntity<JsonNode> post(String path, String idempotencyKey, String json) {
        HttpHeaders headers = headers();
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        return rest.exchange(path, HttpMethod.POST, new HttpEntity<>(json, headers), JsonNode.class);
    }

    private ResponseEntity<JsonNode> get(String path) {
        return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(headers()), JsonNode.class);
    }

    private static HttpHeaders headers() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", "dev-key");
        return headers;
    }

    private JsonNode latestRun(String jobId) {
        return get("/api/v1/jobs/" + jobId).getBody().path("latestRun");
    }

    private JsonNode firstRun(String jobId) {
        return get("/api/v1/jobs/" + jobId + "/runs").getBody().get(0);
    }

    private int succeededRuns(String jobId) {
        int n = 0;
        for (JsonNode run : get("/api/v1/jobs/" + jobId + "/runs?limit=200").getBody()) {
            if (run.get("status").asText().equals("SUCCEEDED")) {
                n++;
            }
        }
        return n;
    }

    private static String inOneHour() {
        return Instant.now().plus(1, ChronoUnit.HOURS).toString();
    }

    private static void assertError(ResponseEntity<JsonNode> res, HttpStatus status, String code) {
        assertThat(res.getStatusCode()).isEqualTo(status);
        assertThat(res.getBody().get("code").asText()).isEqualTo(code);
        assertThat(res.getBody().get("traceId").asText()).isNotBlank();
    }
}
