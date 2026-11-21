package com.jobscheduler.common.schedule;

import com.jobscheduler.common.domain.Job;
import com.jobscheduler.common.domain.JobRun;
import com.jobscheduler.common.repo.RunRepository;
import com.jobscheduler.common.util.Times;
import java.time.OffsetDateTime;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * Inserts the next run of a recurring job.
 *
 * <p>Misfire policy: never fire a backlog. The next fire time is computed after max(now, previous fire time),
 * so if the system was down through several fire times, the overdue run fires once and the schedule then
 * skips to the next future time.
 */
@Component
public class NextRunPlanner {

    private final RunRepository runs;

    public NextRunPlanner(RunRepository runs) {
        this.runs = runs;
    }

    /** Call inside the transaction that claims or queues the previous run. Safe to race: UNIQUE(job_id, due_at). */
    public Optional<JobRun> planNext(Job job, OffsetDateTime previousFireTime) {
        if (!job.isRecurring()) {
            return Optional.empty();
        }
        OffsetDateTime now = Times.now();
        OffsetDateTime after = previousFireTime.isAfter(now) ? previousFireTime : now;
        return CronSchedules.next(job.cronExpr(), job.timeZone(), after).map(due -> {
            JobRun run = JobRun.pending(job.id(), due, job.priority());
            runs.insert(run);
            return run;
        });
    }
}
