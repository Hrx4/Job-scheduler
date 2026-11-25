package com.jobscheduler.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jobscheduler.common.domain.AttemptOutcome;
import com.jobscheduler.common.domain.Job;
import com.jobscheduler.common.domain.JobAttempt;
import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.domain.JobStatus;
import com.jobscheduler.common.domain.Priority;
import com.jobscheduler.common.domain.RunStatus;
import com.jobscheduler.common.repo.AttemptRepository;
import com.jobscheduler.common.repo.JobRepository;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.util.Times;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Simulates a dead worker by writing the rows it would leave behind: RUNNING with an expired lease
 * and an unfinished attempt. Same config as DuePollerTest, so the Spring context and containers are shared.
 */
@SpringBootTest(properties = {"scheduler.poll-interval-ms=100", "scheduler.reaper-interval-ms=200"})
@Testcontainers(disabledWithoutDocker = true)
@Import(DuePollerTest.Containers.class)
class ReaperTest {

    @Autowired
    JobRepository jobs;
    @Autowired
    RunRepository runs;
    @Autowired
    AttemptRepository attempts;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    MeterRegistry meters;

    @Test
    void runOfDeadWorkerIsRecoveredAndQueuedAgain() {
        JobRun run = runHeldByDeadWorker(3, 1);

        // Reaper -> PENDING, then the poller re-queues it with a fresh lease and no owner.
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            JobRun r = runs.findById(run.id()).orElseThrow();
            return r.status() == RunStatus.QUEUED && r.leaseOwner() == null;
        });
        assertThat(runs.findById(run.id()).orElseThrow().attemptCount()).isEqualTo(1); // next claim makes it 2

        List<JobAttempt> history = attempts.findByRunIds(List.of(run.id()));
        assertThat(history).hasSize(1);
        assertThat(history.get(0).outcome()).isEqualTo(AttemptOutcome.LEASE_EXPIRED);
        assertThat(history.get(0).error()).contains("dead-worker");
        assertThat(history.get(0).finishedAt()).isNotNull();
    }

    @Test
    void deadWorkerOnLastAttemptSendsRunToDead() {
        JobRun run = runHeldByDeadWorker(1, 2); // maxRetries 1 = 2 attempts allowed; attempt 2 was the last

        await().atMost(Duration.ofSeconds(10)).until(() -> status(run) == RunStatus.DEAD);
        assertThat(attempts.findByRunIds(List.of(run.id())).get(0).outcome())
                .isEqualTo(AttemptOutcome.LEASE_EXPIRED);
    }

    @Test
    void queuedRunWhosePublishWasLostIsQueuedAgain() {
        double before = recovered("QUEUED");
        JobRun run = insertRun(3);
        // QUEUED with an expired lease = published message lost (or never claimed).
        jdbc.update("UPDATE job_run SET status = 'QUEUED', lease_expires_at = now() - interval '1 second' "
                + "WHERE id = ?", run.id());

        await().atMost(Duration.ofSeconds(10)).until(() -> {
            JobRun r = runs.findById(run.id()).orElseThrow();
            return r.status() == RunStatus.QUEUED && r.leaseExpiresAt().isAfter(Times.now());
        });
        assertThat(runs.findById(run.id()).orElseThrow().attemptCount()).isZero(); // no attempt was used
        assertThat(recovered("QUEUED")).isGreaterThan(before);
    }

    @Test
    void runWithLiveLeaseIsLeftAlone() {
        JobRun run = insertRun(3);
        jdbc.update("UPDATE job_run SET status = 'RUNNING', lease_owner = 'alive-worker', attempt_count = 1, "
                + "lease_expires_at = now() + interval '1 minute' WHERE id = ?", run.id());

        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3)).until(() -> {
            JobRun r = runs.findById(run.id()).orElseThrow();
            return r.status() == RunStatus.RUNNING && "alive-worker".equals(r.leaseOwner());
        });
    }

    // ---- helpers ----

    /** The rows a crashed worker leaves: RUNNING, expired lease, attempt started but never finished. */
    private JobRun runHeldByDeadWorker(int maxRetries, int attempt) {
        JobRun run = insertRun(maxRetries);
        jdbc.update("UPDATE job_run SET status = 'RUNNING', lease_owner = 'dead-worker', attempt_count = ?, "
                + "lease_expires_at = now() - interval '1 second' WHERE id = ?", attempt, run.id());
        attempts.start(run.id(), attempt, "dead-worker");
        return run;
    }

    private JobRun insertRun(int maxRetries) {
        Job job = new Job(UUID.randomUUID(), null, null, "GENERATE_REPORT", "{}", null, "UTC",
                Priority.NORMAL, maxRetries, 300, JobStatus.ACTIVE, Times.now());
        jobs.insertIfAbsent(job);
        // Due in the future so the poller ignores it until the test puts it in the state it wants.
        JobRun run = JobRun.pending(job.id(), Times.now().plusHours(1), job.priority());
        runs.insert(run);
        return run;
    }

    private RunStatus status(JobRun run) {
        return runs.findById(run.id()).orElseThrow().status();
    }

    private double recovered(String from) {
        return meters.find("reaper_recovered_total").tag("from", from).counters().stream()
                .mapToDouble(c -> c.count()).sum();
    }
}
