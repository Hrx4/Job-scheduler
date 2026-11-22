package com.jobscheduler.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.common.domain.Job;
import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.domain.JobStatus;
import com.jobscheduler.common.domain.Priority;
import com.jobscheduler.common.domain.RunStatus;
import com.jobscheduler.common.messaging.Topics;
import com.jobscheduler.common.repo.JobRepository;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.util.Times;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(properties = "scheduler.poll-interval-ms=100")
@Testcontainers(disabledWithoutDocker = true)
@Import(DuePollerTest.Containers.class)
class DuePollerTest {

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
    }

    @Autowired
    JobRepository jobs;
    @Autowired
    RunRepository runs;
    @Autowired
    KafkaContainer kafka;

    @Test
    void dueRunIsQueuedAndPublishedToKafka() {
        JobRun run = insertJobWithRun(null, JobStatus.ACTIVE, Times.now());

        await().atMost(Duration.ofSeconds(10)).until(() -> status(run) == RunStatus.QUEUED);
        JobRun queued = runs.findById(run.id()).orElseThrow();
        assertThat(queued.leaseExpiresAt()).isAfter(Times.now()); // the QUEUED lease is set

        Set<String> keys = ConcurrentHashMap.newKeySet();
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(Topics.JOBS));
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    assertThat(r.value()).isEqualTo(r.key()); // message = runId
                    keys.add(r.key());
                });
                return keys.contains(run.id().toString());
            });
        }
    }

    @Test
    void cronRunPlansNextFireTimeWhenQueued() {
        JobRun run = insertJobWithRun("0 0 * * * *", JobStatus.ACTIVE, Times.now().minusMinutes(1));

        await().atMost(Duration.ofSeconds(10)).until(() -> status(run) == RunStatus.QUEUED);
        List<JobRun> all = runs.findByJobId(run.jobId(), 10);
        assertThat(all).hasSize(2);
        JobRun next = all.get(0); // newest due time first
        assertThat(next.status()).isEqualTo(RunStatus.PENDING);
        assertThat(next.dueAt()).isAfter(Times.now()); // misfire policy: next is in the future
        assertThat(next.dueAt().getMinute()).isZero();
    }

    @Test
    void futureAndPausedRunsAreNotQueued() {
        JobRun future = insertJobWithRun(null, JobStatus.ACTIVE, Times.now().plusHours(1));
        JobRun paused = insertJobWithRun("0 0 * * * *", JobStatus.PAUSED, Times.now());

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() ->
                status(future) == RunStatus.PENDING && status(paused) == RunStatus.PENDING);
    }

    // ---- helpers ----

    private JobRun insertJobWithRun(String cron, JobStatus status, java.time.OffsetDateTime dueAt) {
        Job job = new Job(UUID.randomUUID(), null, null, "GENERATE_REPORT", "{}", cron, "UTC",
                Priority.NORMAL, 3, 300, status, Times.now());
        jobs.insertIfAbsent(job);
        JobRun run = JobRun.pending(job.id(), dueAt, job.priority());
        runs.insert(run);
        return run;
    }

    private RunStatus status(JobRun run) {
        return runs.findById(run.id()).orElseThrow().status();
    }

    private KafkaConsumer<String, String> consumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }
}
