# Architecture Decision Records

One entry per decision, written when it is made. Each answers: problem, options, evidence, choice, trade-off.

- **ADR-001 to 006** are design decisions, defended with reasoning.
- **ADR-007 to 012** are observation-based. They stay **Open** until the experiment has run. Only real measured numbers go in "Data".
- **ADR-013 and up** are decisions made while building.

| ADR | Decision | Status |
|-----|----------|--------|
| 001 | Postgres is source of truth; Kafka only delivers | Accepted |
| 002 | At-least-once delivery + idempotent handlers | Accepted |
| 003 | Three tables: job / job_run / job_attempt | Accepted |
| 004 | Lease with expiry, not a lock held until the job ends | Accepted |
| 005 | Several schedulers polling with SKIP LOCKED, no leader | Accepted |
| 006 | Misfire policy: run once, then skip to next fire time | Accepted |
| 007 | Postgres lease vs Redis lock | Open: needs experiment |
| 008 | Polling interval and batch size | Open: needs experiment |
| 009 | Lease length and heartbeat interval | Open: needs experiment |
| 010 | One topic vs a topic per priority | Open: needs experiment |
| 011 | Kafka partitions vs worker count | Open: needs experiment |
| 012 | Default job timeout | Open: needs experiment |
| 013 | `attempt_base` instead of resetting `attempt_count` on requeue | Accepted |
| 014 | Idempotency key stores a request hash; reuse with a different body is 409 | Accepted |
| 015 | Plain JDBC (`NamedParameterJdbcTemplate`), not JPA | Accepted |
| 016 | Kafka message carries only the `runId` | Accepted |
| 017 | Worker: claim on the consumer thread, run on a handler pool, nack when full | Accepted |

---

## Design decisions

### ADR-001: Postgres is the source of truth; Kafka only delivers
- **Context:** job state must survive crashes and be queryable ("all failed jobs of type X").
- **Options:** keep state in Kafka, in Redis, or in Postgres.
- **Reasoning:** state must be queryable, updated in a transaction, and durable. Kafka cannot query or update one message. Redis durability depends on config.
- **Decision:** Postgres holds all job state. Kafka only carries `runId`s to workers. If the two disagree, Postgres wins.
- **Trade-off:** Postgres becomes the bottleneck at very high scale. The dual-write problem (DB commit, then Kafka publish) must be handled: v1 uses a lease on `QUEUED` plus the reaper, v2 a transactional outbox.

### ADR-002: At-least-once delivery with idempotent handlers
- **Context:** a run can reach a worker twice, through Kafka redelivery or a reaper re-queue.
- **Options:** exactly-once vs at-least-once.
- **Reasoning:** exactly-once across a network plus outside side effects (email, API calls) cannot be done in general. So pick a guarantee we can keep and make duplicates harmless.
- **Decision:** at-least-once. Workers claim with a conditional update before work. Handlers get `runId` as an idempotency key and pass it downstream (`HTTP_CALLBACK` sends it as the `Idempotency-Key` header).
- **Trade-off:** every handler author must think about idempotency.

### ADR-003: Three tables: job / job_run / job_attempt
- **Context:** recurring jobs, retries and history all need storing.
- **Options:** one jobs table vs split tables.
- **Reasoning:** with one table, a cron job overwrites its own history and loses earlier retry errors.
- **Decision:** `job` holds what to run, `job_run` holds each fire time, `job_attempt` holds each try.
- **Trade-off:** more rows and joins. Archiving or partitioning keeps the busy table small.

### ADR-004: Lease with expiry, not a lock held until the job ends
- **Context:** only one worker may run a run at a time, and a crashed worker must not block it forever.
- **Options:** lock with no expiry vs lease.
- **Reasoning:** a lock with no expiry stays held forever if the worker crashes. A lease expires on its own, which makes crash recovery possible.
- **Decision:** `lease_owner` + `lease_expires_at`, taken with a conditional update.
- **Trade-off:** a slow but alive worker can lose its lease. So we need the chain *lease → heartbeat → fencing token on every completion write*. Fenced completion is already in `RunRepository.complete` (`WHERE lease_owner = ? AND attempt_count = ?`).
- **Follow-up, "what if the worker is slow, not dead?":** heartbeats keep extending the lease. If it still expires, another worker takes over with a higher `attempt_count`. The slow worker's completion write then matches 0 rows and its result is thrown away.

### ADR-005: Several schedulers polling with SKIP LOCKED, no leader
- **Context:** the scheduler must not be a single point of failure.
- **Options:** one elected leader (ZooKeeper, advisory lock) vs many pollers with `FOR UPDATE SKIP LOCKED`.
- **Reasoning:** no election to build, no failover to handle, and every instance does useful work.
- **Decision:** every instance polls. `SKIP LOCKED` stops two pollers from grabbing the same row.
- **Trade-off:** every instance polls the database. Batch size and a 500 ms interval keep that in check (tuned in ADR-008).

### ADR-006: Misfire policy: run once, then skip to the next fire time
- **Context:** the system was down through several cron fire times.
- **Options:** replay every missed fire vs run once.
- **Reasoning:** after 2 hours down, an every-minute job would fire 120 times at once and could overwhelm downstream systems.
- **Decision:** the overdue run fires once. The next fire time is computed after `max(now, previous fire time)` (`NextRunPlanner`). Resume after a pause also computes from now.
- **Trade-off:** some fire times are skipped on purpose. This is a product decision, so it is stated in the README.

---

## Observation-based decisions (fill in after each experiment)

### ADR-007: Postgres lease vs Redis lock
- **Context:** workers need exclusive ownership of a run.
- **Options:** Postgres conditional update; Redis `SET key worker NX PX 30000`.
- **Experiment:** the same load test (e.g. 100k jobs) with each strategy.
- **Measure:** throughput, p99 claim latency, duplicate executions.
- **How to decide:** keep Postgres unless it misses the target load. Note the load where you would switch.
- **Data:** _TBD_
- **Decision:** _TBD_
- **Trade-off:** _TBD_

### ADR-008: Polling interval and batch size
- **Experiment:** interval 100 ms / 500 ms / 2 s × batch 50 / 500 / 2000.
- **Measure:** job start delay (`job_start_delay_seconds`), database CPU.
- **How to decide:** the smallest database load that still meets p99 start delay < 2 s.
- **Data:** _TBD_
- **Decision:** _TBD_
- **Trade-off:** _TBD_

### ADR-009: Lease length and heartbeat interval
- **Experiment:** kill a worker mid-job, then shorten the lease.
- **Measure:** time until the job reruns; false takeovers of slow but healthy jobs.
- **How to decide:** a short lease gives faster recovery but more false takeovers and heartbeat writes. Pick the balance.
- **Data:** _TBD_
- **Decision:** _TBD_
- **Trade-off:** _TBD_

### ADR-010: One topic vs a topic per priority
- **Experiment:** build a low-priority backlog, then submit high-priority jobs.
- **Measure:** p99 start delay of high-priority jobs under each design.
- **How to decide:** split topics only if the single topic delays high-priority jobs badly.
- **Data:** _TBD_
- **Decision:** _TBD_
- **Trade-off:** _TBD_

### ADR-011: Kafka partitions vs worker count
- **Experiment:** scale workers from 1 to 12.
- **Measure:** throughput per worker count.
- **How to decide:** find where throughput stops growing, and whether partitions or the database is the limit.
- **Data:** _TBD_
- **Decision:** _TBD_
- **Trade-off:** _TBD_

### ADR-012: Default job timeout
- **Experiment:** run real job types and read p99 duration in Grafana.
- **Measure:** p99 `job_attempt_duration_seconds` per job type.
- **How to decide:** set the timeout to a few times p99, not a guess. The current default of 300 s is a placeholder.
- **Data:** _TBD_
- **Decision:** _TBD_
- **Trade-off:** _TBD_

---

## Decisions made while building

### ADR-013: `attempt_base` instead of resetting `attempt_count` on requeue
- **Context:** the design says a manual requeue of a DEAD run resets its attempts.
- **Options:** set `attempt_count = 0`; or keep the count and record where the new budget starts.
- **Reasoning:** `attempt_count` is the fencing token, and it is also `job_attempt.attempt_no` (`UNIQUE(run_id, attempt_no)`). Resetting it would let a stale worker from before the requeue pass the fencing check. It would also collide with existing attempt rows.
- **Decision:** add `job_run.attempt_base`. Requeue sets `attempt_base = attempt_count`. Retries are allowed while `attempt_count - attempt_base <= max_retries`.
- **Trade-off:** one extra column, and the retry rule is a little less obvious. Attempt history stays whole and fencing stays correct.

### ADR-014: Idempotency key stores a request hash
- **Context:** a client retries `POST /jobs` with the same `Idempotency-Key`.
- **Options:** return the original job no matter what; or compare bodies.
- **Reasoning:** the same key with a different body is a client bug. Silently returning the old job hides it.
- **Decision:** store a SHA-256 of the request in `job.request_hash`. Same key + same body returns 200 with the original job. Same key + different body returns 409 `IDEMPOTENCY_KEY_REUSED`. Concurrent duplicates are settled by `ON CONFLICT (idempotency_key) DO NOTHING`.
- **Trade-off:** JSON key order changes the hash, so a client that re-serializes in a different order gets a 409. That is acceptable for v1.

### ADR-015: Plain JDBC, not JPA
- **Context:** the hot paths are `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING *`, conditional fenced updates, and `ON CONFLICT DO NOTHING`.
- **Options:** Spring Data JPA / Hibernate vs `NamedParameterJdbcTemplate`.
- **Reasoning:** the correctness of the system lives in those exact SQL statements, and the rows-affected count is the signal ("0 rows = fenced off"). An ORM hides both.
- **Decision:** hand-written SQL in `common/repo`.
- **Trade-off:** more boilerplate (row mappers, dynamic filter SQL). No lazy loading or dirty checking, which we do not want here anyway.

### ADR-016: Kafka message carries only the `runId`
- **Context:** the scheduler publishes each queued run for workers.
- **Options:** send the full job (type, payload, attempt) in the message; or send only `runId`.
- **Reasoning:** a full message is a second copy of the state that can go stale. Between publish and consume, the job may be cancelled or the run re-queued by the reaper. The worker must re-read Postgres to claim the run anyway (ADR-001, ADR-002), so extra fields add nothing but drift.
- **Decision:** key = value = `runId`. Keying by `runId` spreads runs evenly over the 12 partitions.
- **Trade-off:** one extra Postgres read per run on the worker. If that read ever shows up as the bottleneck (ADR-011), revisit by adding immutable fields (type, payload) to the message.

### ADR-017: Worker claims on the consumer thread, runs on a handler pool, nacks when full
- **Context:** jobs can run for minutes (timeout up to 24 h). A Kafka consumer that stops calling `poll()` for longer than `max.poll.interval.ms` (5 min) is kicked from the group, and that triggers a rebalance storm.
- **Options:** (a) run the handler on the consumer thread; (b) hand off to a pool and block when the pool is full; (c) hand off to a pool and `nack` the record when the pool is full.
- **Reasoning:** (a) and (b) both stop polling while long jobs run. With (c) the consumer thread only does fast work (claim + offset commit), so it never stalls. When all handler threads are busy, it claims nothing and `nack`s. Kafka redelivers that record about 200 ms later, and Postgres state never changes.
- **Decision:** (c). Order per message: free slot? → claim (`QUEUED → RUNNING`) → commit offset → run on the pool. The offset is committed only after the claim. If the worker crashes before the claim, the message is redelivered. If it crashes after, the run's lease expires.
- **Trade-off:** a full worker keeps re-reading the same records, which is wasted polls but no wrong state. In-flight runs no longer pin a partition, so one instance can run more jobs than it has partitions. Handler-thread count vs consumer count is tuned in ADR-011.
