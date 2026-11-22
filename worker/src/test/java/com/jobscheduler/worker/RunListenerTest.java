package com.jobscheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.common.domain.Job;
import com.jobscheduler.common.domain.JobAttempt;
import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.domain.JobStatus;
import com.jobscheduler.common.domain.Priority;
import com.jobscheduler.common.domain.RunStatus;
import com.jobscheduler.common.messaging.Topics;
import com.jobscheduler.common.repo.AttemptRepository;
import com.jobscheduler.common.repo.JobRepository;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.util.Times;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** The scheduler is not running here: the test plays its part by marking runs QUEUED and publishing runIds. */
@SpringBootTest(properties = {"jobs.retry.base-ms=60000", "jobs.retry.cap-ms=60000"})
@Testcontainers(disabledWithoutDocker = true)
@Import(RunListenerTest.Containers.class)
class RunListenerTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        PostgreSQLContainer<?> postgres() {
            return new PostgreSQLContainer<>("postgres:16-alpine");
        }

        @Bean
        @ServiceConnection
        KafkaContainer kafka() {
            return new KafkaContainer("apache/kafka:3.8.0");
        }

        /** In production the scheduler creates the topic. */
        @Bean
        NewTopic jobsTopic() {
            return TopicBuilder.name(Topics.JOBS).partitions(Topics.JOBS_PARTITIONS).replicas(1).build();
        }
    }

    @Autowired
    JobRepository jobs;
    @Autowired
    RunRepository runs;
    @Autowired
    AttemptRepository attempts;
    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    WorkerIdentity identity;

    @Test
    void queuedRunIsClaimedAndSucceeds() {
        JobRun run = queuedRun("{\"durationMs\": 10}");
        publish(run);

        await().atMost(Duration.ofSeconds(20)).until(() -> status(run) == RunStatus.SUCCEEDED);
        List<JobAttempt> history = attempts.findByRunIds(List.of(run.id()));
        assertThat(history).hasSize(1);
        assertThat(history.get(0).workerId()).isEqualTo(identity.id());
    }

    @Test
    void duplicateDeliveryRunsTheJobOnce() {
        JobRun run = queuedRun("{\"durationMs\": 300}");
        publish(run);
        publish(run);
        publish(run);

        await().atMost(Duration.ofSeconds(20)).until(() -> status(run) == RunStatus.SUCCEEDED);
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                .until(() -> attempts.findByRunIds(List.of(run.id())).size() == 1);
    }

    @Test
    void messageForRunThatIsNotQueuedIsSkipped() {
        JobRun run = queuedRun("{\"durationMs\": 10}");
        jdbc.update("UPDATE job_run SET status = 'CANCELLED' WHERE id = ?", run.id());
        publish(run);

        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4))
                .until(() -> status(run) == RunStatus.CANCELLED);
        assertThat(attempts.findByRunIds(List.of(run.id()))).isEmpty();
    }

    @Test
    void failedAttemptGoesBackToPendingForTheScheduler() {
        JobRun run = queuedRun("{\"durationMs\": 10, \"failUntilAttempt\": 1}");
        publish(run);

        await().atMost(Duration.ofSeconds(20)).until(() -> status(run) == RunStatus.PENDING);
        JobRun after = runs.findById(run.id()).orElseThrow();
        assertThat(after.attemptCount()).isEqualTo(1);
        assertThat(after.leaseOwner()).isNull();
        assertThat(after.nextAttemptAt()).isAfter(Times.now()); // backoff delay; scheduler re-queues it later
    }

    // ---- helpers ----

    private JobRun queuedRun(String payload) {
        Job job = new Job(UUID.randomUUID(), null, null, "GENERATE_REPORT", payload, null, "UTC",
                Priority.NORMAL, 3, 30, JobStatus.ACTIVE, Times.now());
        jobs.insertIfAbsent(job);
        JobRun run = JobRun.pending(job.id(), Times.now(), job.priority());
        runs.insert(run);
        jdbc.update("UPDATE job_run SET status = 'QUEUED', lease_expires_at = now() + interval '60 seconds' "
                + "WHERE id = ?", run.id());
        return run;
    }

    private void publish(JobRun run) {
        kafka.send(Topics.JOBS, run.id().toString(), run.id().toString());
    }

    private RunStatus status(JobRun run) {
        return runs.findById(run.id()).orElseThrow().status();
    }
}
