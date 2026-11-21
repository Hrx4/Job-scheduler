package com.jobscheduler.api.web;

import com.jobscheduler.api.service.JobService;
import com.jobscheduler.api.web.dto.DeadRunResponse;
import com.jobscheduler.api.web.dto.JobResponse;
import com.jobscheduler.api.web.dto.PageResponse;
import com.jobscheduler.api.web.dto.RunResponse;
import com.jobscheduler.api.web.dto.SubmitJobRequest;
import com.jobscheduler.common.domain.JobStatus;
import jakarta.validation.Valid;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class JobController {

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    /** 201 for a new job, 200 when the Idempotency-Key matches an earlier identical request. */
    @PostMapping("/jobs")
    public ResponseEntity<JobResponse> submit(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody SubmitJobRequest request) {
        JobService.SubmitResult result = service.submit(idempotencyKey, request);
        if (result.created()) {
            return ResponseEntity.created(URI.create("/api/v1/jobs/" + result.job().jobId())).body(result.job());
        }
        return ResponseEntity.ok(result.job());
    }

    @GetMapping("/jobs/{jobId}")
    public JobResponse get(@PathVariable UUID jobId) {
        return service.get(jobId);
    }

    @GetMapping("/jobs")
    public PageResponse<JobResponse> list(
            @RequestParam(required = false) JobStatus status,
            @RequestParam(required = false) String type,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "50") int limit) {
        return service.list(status, type, from, to, cursor, limit);
    }

    /** Runs of a job, newest first, each with its attempts. */
    @GetMapping("/jobs/{jobId}/runs")
    public List<RunResponse> runs(@PathVariable UUID jobId, @RequestParam(defaultValue = "20") int limit) {
        return service.runs(jobId, limit);
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public ResponseEntity<JobResponse> cancel(@PathVariable UUID jobId) {
        return ResponseEntity.accepted().body(service.cancel(jobId));
    }

    @PostMapping("/jobs/{jobId}/pause")
    public JobResponse pause(@PathVariable UUID jobId) {
        return service.pause(jobId);
    }

    @PostMapping("/jobs/{jobId}/resume")
    public JobResponse resume(@PathVariable UUID jobId) {
        return service.resume(jobId);
    }

    @GetMapping("/dead-letter")
    public PageResponse<DeadRunResponse> deadLetter(
            @RequestParam(required = false) String type,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "50") int limit) {
        return service.deadLetter(type, cursor, limit);
    }

    @PostMapping("/runs/{runId}/requeue")
    public ResponseEntity<RunResponse> requeue(@PathVariable UUID runId) {
        return ResponseEntity.accepted().body(service.requeue(runId));
    }
}
