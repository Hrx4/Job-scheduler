# Distributed Job Scheduler

Accepts jobs over REST, runs each at least once on a pool of workers, never on two workers at once.
Postgres is the source of truth; Kafka only delivers runs to workers.

Stack: Java 21, Spring Boot 3, PostgreSQL, Kafka, Redis, Docker Compose, Prometheus + Grafana, Testcontainers.

## Modules

| Module      | Role                                                         | Port |
|-------------|--------------------------------------------------------------|------|
| `common`    | Domain enums, shared SQL, Flyway migrations                  | –    |
| `api`       | REST API, idempotency keys, runs Flyway on startup           | 8080 |
| `scheduler` | Polls due runs (SKIP LOCKED) → Kafka, reaper, cron next-run  | 8081 |
| `worker`    | Consumes runs, claims, runs handler, heartbeats, fencing     | 8082 |

## Run locally

Needs JDK 21 and Docker.

```bash
docker compose up -d                       # postgres, kafka, redis, prometheus, grafana
mvn clean install
mvn -pl api spring-boot:run                # start api first (migrates DB)
mvn -pl scheduler spring-boot:run
mvn -pl worker spring-boot:run
```

Or everything in containers: `docker compose --profile apps up -d --build`.

| UI         | URL                     |
|------------|-------------------------|
| Prometheus | http://localhost:9090   |
| Grafana    | http://localhost:3000 (admin / admin) |

## API

All `/api/v1/**` calls need `X-API-Key` (default `dev-key`, set with `API_KEYS`).

```bash
curl -X POST localhost:8080/api/v1/jobs -H "X-API-Key: dev-key" -H "Content-Type: application/json" \
  -H "Idempotency-Key: report-4412" \
  -d '{"type":"GENERATE_REPORT","payload":{"reportId":4412},
       "schedule":{"kind":"CRON","cron":"0 0 9 * * MON-FRI","timeZone":"Asia/Kolkata"}}'
```

| Method | Path                             | Notes                                          |
|--------|----------------------------------|------------------------------------------------|
| POST   | `/api/v1/jobs`                   | 201 new, 200 same key + body, 409 key reused   |
| GET    | `/api/v1/jobs/{id}`              | with `latestRun` and `nextRunAt`               |
| GET    | `/api/v1/jobs?status&type&from&to&cursor&limit` | cursor-paginated, newest first  |
| GET    | `/api/v1/jobs/{id}/runs`         | runs with attempts                             |
| POST   | `/api/v1/jobs/{id}/cancel`       | 202                                            |
| POST   | `/api/v1/jobs/{id}/pause`, `/resume` | recurring jobs only                        |
| GET    | `/api/v1/dead-letter?type&cursor`| DEAD runs with last error                      |
| POST   | `/api/v1/runs/{id}/requeue`      | 202, fresh retry budget                        |

Errors: `{ "code", "message", "traceId" }`.

Built-in job types (demo handlers): `SEND_EMAIL` (`to`), `GENERATE_REPORT` (`durationMs`,
`failUntilAttempt` for testing retries), `HTTP_CALLBACK` (`url`, `body`; sends `Idempotency-Key: <runId>`).

Cron uses Spring's 6-field format: `second minute hour day-of-month month day-of-week`.

## Policies

- **Retries:** delay = min(10 s · 2^(attempt-1), 10 min) + random(0, 10 s). `NonRetryableException` goes straight to DEAD.
- **Misfires:** if fire times were missed, run once now, then skip to the next future fire time. No backlog.
- **Resume:** next fire time is computed from now; times missed while paused are skipped.
- **Requeue:** `attempt_count` never goes down (it is the fencing token). Requeue sets `attempt_base`
  so the run gets a fresh `maxRetries` budget.

## Build plan

| Weeks | Build                                                                      | Guarantee                       |
|-------|----------------------------------------------------------------------------|---------------------------------|
| 1–2   | API, three tables (Flyway), single in-process worker, Docker Compose       | Jobs durable and queryable      |
| 3–4   | Scheduler poller (SKIP LOCKED), Kafka, many workers, retries, DEAD state   | Survive failures, scale out     |
| 5–6   | Claim + fencing, heartbeats, reaper, timeouts; Redis lock variant          | No double run, crash recovery   |
| 7     | Cron, misfire policy, pause/resume, idempotency keys; Testcontainers tests | Recurring jobs, safe retries    |
| 8     | Metrics, Grafana dashboard, k6 load test, chaos test                       | Measured numbers                |
