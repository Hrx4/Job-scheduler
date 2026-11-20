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

## Build plan

| Weeks | Build                                                                      | Guarantee                       |
|-------|----------------------------------------------------------------------------|---------------------------------|
| 1–2   | API, three tables (Flyway), single in-process worker, Docker Compose       | Jobs durable and queryable      |
| 3–4   | Scheduler poller (SKIP LOCKED), Kafka, many workers, retries, DEAD state   | Survive failures, scale out     |
| 5–6   | Claim + fencing, heartbeats, reaper, timeouts; Redis lock variant          | No double run, crash recovery   |
| 7     | Cron, misfire policy, pause/resume, idempotency keys; Testcontainers tests | Recurring jobs, safe retries    |
| 8     | Metrics, Grafana dashboard, k6 load test, chaos test                       | Measured numbers                |
