package com.jobscheduler.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * ADR-007 experiment: Postgres lease vs Redis lock, same load, same machine.
 *
 * <p>Load: RUNS queued runs, each delivered twice (like Kafka redelivery), THREADS claimers racing.
 * Duplicate = a run claimed more than once. Not part of the normal build; run with:
 * <pre>mvn -pl worker test -Dtest=LockBenchmarkTest -Dbenchmark=true -Dsurefire.failIfNoSpecifiedTests=false</pre>
 * Optional: -Dbench.runs=20000 -Dbench.threads=24 -Dbench.rounds=3
 */
@EnabledIfSystemProperty(named = "benchmark", matches = "true")
@Testcontainers
class LockBenchmarkTest {

    static final int RUNS = Integer.getInteger("bench.runs", 20_000);
    static final int THREADS = Integer.getInteger("bench.threads", 24);
    static final int ROUNDS = Integer.getInteger("bench.rounds", 3);

    /** Same statement as RunRepository.claimQueued: lock and state change in one round trip. */
    static final String PG_LEASE_CLAIM = """
            UPDATE job_run SET status = 'RUNNING', lease_owner = ?,
                   lease_expires_at = now() + interval '30 seconds',
                   attempt_count = attempt_count + 1, version = version + 1
            WHERE id = ? AND status = 'QUEUED'""";

    /** Redis design: Redis is the only guard, Postgres just records the new state. */
    static final String PG_STATE_WRITE = """
            UPDATE job_run SET status = 'RUNNING', lease_owner = ?,
                   lease_expires_at = now() + interval '30 seconds',
                   attempt_count = attempt_count + 1, version = version + 1
            WHERE id = ?""";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static HikariDataSource dataSource;
    static JdbcTemplate jdbc;
    static RedisClient redisClient;
    static StatefulRedisConnection<String, String> redisConn;
    static List<UUID> runIds;

    interface Claim {
        boolean claim(UUID runId, String workerId);
    }

    record Result(String variant, int round, double deliveriesPerSec, double p50Ms, double p99Ms,
                  int claims, int duplicates) {
    }

    @BeforeAll
    static void setUp() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(postgres.getJdbcUrl());
        dataSource.setUsername(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        dataSource.setMaximumPoolSize(THREADS);
        Flyway.configure().dataSource(dataSource).load().migrate();
        jdbc = new JdbcTemplate(dataSource);

        UUID jobId = UUID.randomUUID();
        jdbc.update("INSERT INTO job (id, type, payload, status) VALUES (?, 'BENCH', '{}', 'ACTIVE')", jobId);
        jdbc.update("""
                INSERT INTO job_run (id, job_id, due_at, status, next_attempt_at, priority)
                SELECT gen_random_uuid(), ?, now() + make_interval(secs => g), 'QUEUED', now(), 1
                FROM generate_series(1, ?) g""", jobId, RUNS);
        runIds = jdbc.queryForList("SELECT id FROM job_run", UUID.class);

        redisClient = RedisClient.create(RedisURI.create(redis.getHost(), redis.getMappedPort(6379)));
        redisConn = redisClient.connect();
    }

    @AfterAll
    static void tearDown() {
        redisConn.close();
        redisClient.shutdown();
        dataSource.close();
    }

    @Test
    void compareLockStrategies() throws Exception {
        Claim pgLease = (id, w) -> jdbc.update(PG_LEASE_CLAIM, w, id) == 1;
        Claim redisLockOnly = (id, w) ->
                "OK".equals(redisConn.sync().set("lock:run:" + id, w, SetArgs.Builder.nx().px(30_000)));
        Claim redisPlusPg = (id, w) -> redisLockOnly.claim(id, w) && jdbc.update(PG_STATE_WRITE, w, id) == 1;

        List<Result> results = new ArrayList<>();
        for (int round = 1; round <= ROUNDS; round++) {
            results.add(run("postgres lease (lock + state, 1 trip)", pgLease, false, round, true));
            results.add(run("redis lock only (SET NX PX)", redisLockOnly, false, round, false));
            results.add(run("redis lock + postgres state write", redisPlusPg, false, round, true));
            results.add(run("redis + postgres, lock loss injected", redisPlusPg, true, round, true));
        }

        System.out.printf("%nLock benchmark: %d runs x 2 deliveries, %d threads, %d rounds%n", RUNS, THREADS, ROUNDS);
        System.out.printf("%-40s %5s %12s %8s %8s %7s %6s%n",
                "variant", "round", "deliveries/s", "p50 ms", "p99 ms", "claims", "dupes");
        for (Result r : results) {
            System.out.printf("%-40s %5d %12.0f %8.2f %8.2f %7d %6d%n",
                    r.variant(), r.round(), r.deliveriesPerSec(), r.p50Ms(), r.p99Ms(), r.claims(), r.duplicates());
        }

        // Guarantees that must hold regardless of speed.
        results.stream().filter(r -> !r.variant().contains("lock loss"))
                .forEach(r -> assertThat(r.duplicates()).as(r.variant()).isZero());
    }

    private Result run(String variant, Claim claim, boolean injectLockLoss, int round, boolean usesDb)
            throws Exception {
        reset();
        List<UUID> deliveries = new ArrayList<>(RUNS * 2);
        deliveries.addAll(runIds);
        deliveries.addAll(runIds);
        Collections.shuffle(deliveries, new Random(round));
        ConcurrentLinkedQueue<UUID> queue = new ConcurrentLinkedQueue<>(deliveries);

        long[] latencies = new long[deliveries.size()];
        AtomicInteger next = new AtomicInteger();
        AtomicInteger claims = new AtomicInteger();

        // "Failover that lost the keys": wipe Redis on a separate connection every 20 ms.
        ScheduledExecutorService chaos = Executors.newSingleThreadScheduledExecutor();
        StatefulRedisConnection<String, String> chaosConn = redisClient.connect();
        if (injectLockLoss) {
            chaos.scheduleAtFixedRate(() -> chaosConn.sync().flushall(), 20, 20, TimeUnit.MILLISECONDS);
        }

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        long start = System.nanoTime();
        for (int t = 0; t < THREADS; t++) {
            String workerId = "w" + t;
            pool.execute(() -> {
                UUID id;
                while ((id = queue.poll()) != null) {
                    long s = System.nanoTime();
                    boolean ok = claim.claim(id, workerId);
                    latencies[next.getAndIncrement()] = System.nanoTime() - s;
                    if (ok) {
                        claims.incrementAndGet();
                    }
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);
        double seconds = (System.nanoTime() - start) / 1e9;
        chaos.shutdownNow();
        chaosConn.close();

        int duplicates = usesDb
                ? jdbc.queryForObject("SELECT count(*) FROM job_run WHERE attempt_count > 1", Integer.class)
                : claims.get() - RUNS;
        Arrays.sort(latencies);
        return new Result(variant, round, deliveries.size() / seconds,
                latencies[latencies.length / 2] / 1e6, latencies[(int) (latencies.length * 0.99)] / 1e6,
                claims.get(), duplicates);
    }

    private void reset() {
        jdbc.update("UPDATE job_run SET status = 'QUEUED', attempt_count = 0, lease_owner = NULL, "
                + "lease_expires_at = NULL");
        jdbc.execute("VACUUM ANALYZE job_run");
        redisConn.sync().flushall();
    }
}
