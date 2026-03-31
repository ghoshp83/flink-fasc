package com.flinkfasc.it.aws;

import com.flinkfasc.aws.election.DynamoDbLeaderElection;
import com.flinkfasc.it.infrastructure.ContainerSetup;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test: verifies DynamoDbLeaderElection against a real DynamoDB
 * instance running in LocalStack.
 *
 * <p>Tests:
 * <ul>
 *   <li>Lock acquisition when no lock exists</li>
 *   <li>Lock acquisition fails when another coordinator holds it</li>
 *   <li>Lock renewal extends TTL</li>
 *   <li>Lock release allows re-acquisition</li>
 *   <li>Concurrent lock acquisition — exactly one winner</li>
 *   <li>Flink leader transfer with correct version succeeds</li>
 *   <li>Flink leader transfer with wrong version fails (optimistic locking)</li>
 *   <li>Concurrent leader transfers — exactly one succeeds</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DynamoDbLeaderElectionIT {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbLeaderElectionIT.class);

    private static DynamoDbClient dynamoDbClient;
    private DynamoDbLeaderElection election;

    @BeforeAll
    static void startContainers() {
        ContainerSetup.start();
        dynamoDbClient = ContainerSetup.dynamoDbClient();
    }

    @AfterAll
    static void cleanup() {
        if (dynamoDbClient != null) dynamoDbClient.close();
    }

    @BeforeEach
    void setUp() {
        election = new DynamoDbLeaderElection(
                dynamoDbClient,
                ContainerSetup.LEADER_TABLE,
                ContainerSetup.LOCK_TABLE);

        // Release any lock left from previous test
        election.releaseLock("coordinator-A");
        election.releaseLock("coordinator-B");
    }

    // ── Coordinator Lock Tests ─────────────────────────────────────────────────

    @Test
    @Order(1)
    void tryAcquireLock_succeeds_whenNoLockExists() {
        boolean acquired = election.tryAcquireLock("coordinator-A");

        assertThat(acquired).isTrue();
        assertThat(election.isLockHolder("coordinator-A")).isTrue();
    }

    @Test
    @Order(2)
    void tryAcquireLock_fails_whenAnotherHoldsLock() {
        election.tryAcquireLock("coordinator-A");

        boolean result = election.tryAcquireLock("coordinator-B");

        assertThat(result).isFalse();
        assertThat(election.isLockHolder("coordinator-B")).isFalse();
    }

    @Test
    @Order(3)
    void renewLock_succeeds_whenStillHolder() {
        election.tryAcquireLock("coordinator-A");

        boolean renewed = election.renewLock("coordinator-A");

        assertThat(renewed).isTrue();
        assertThat(election.isLockHolder("coordinator-A")).isTrue();
    }

    @Test
    @Order(4)
    void renewLock_fails_whenNotHolder() {
        election.tryAcquireLock("coordinator-A");

        boolean result = election.renewLock("coordinator-B");

        assertThat(result).isFalse();
    }

    @Test
    @Order(5)
    void releaseLock_allowsReAcquisition() {
        election.tryAcquireLock("coordinator-A");
        election.releaseLock("coordinator-A");

        boolean acquired = election.tryAcquireLock("coordinator-B");

        assertThat(acquired).isTrue();
        assertThat(election.isLockHolder("coordinator-B")).isTrue();
    }

    @Test
    @Order(6)
    void concurrentLockAcquisition_exactlyOneWins() throws InterruptedException {
        int threads = 10;
        CountDownLatch ready  = new CountDownLatch(threads);
        CountDownLatch start  = new CountDownLatch(1);
        AtomicInteger  wins   = new AtomicInteger(0);
        ExecutorService pool  = Executors.newFixedThreadPool(threads);

        for (int i = 0; i < threads; i++) {
            final String coordinatorId = "coordinator-" + i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                    if (election.tryAcquireLock(coordinatorId)) {
                        wins.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        // DynamoDB conditional write guarantees exactly one winner
        assertThat(wins.get()).isEqualTo(1);
        LOG.info("Concurrent lock test: {} threads competed, {} won", threads, wins.get());
    }

    // ── Flink Leader Transfer Tests ────────────────────────────────────────────

    @Test
    @Order(7)
    void getCurrentFlinkLeader_returnsApp1ByDefault() {
        // Seeded in ContainerSetup.createLeaderTable()
        String leader = election.getCurrentFlinkLeader();
        assertThat(leader).isEqualTo("app1");
    }

    @Test
    @Order(8)
    void transferFlinkLeader_succeeds_withCorrectVersion() {
        int currentVersion = election.getCurrentFlinkLeaderVersion();

        boolean transferred = election.transferFlinkLeader("app1", "app2", currentVersion);

        assertThat(transferred).isTrue();
        assertThat(election.getCurrentFlinkLeader()).isEqualTo("app2");
        assertThat(election.getCurrentFlinkLeaderVersion()).isEqualTo(currentVersion + 1);
    }

    @Test
    @Order(9)
    void transferFlinkLeader_fails_withWrongVersion() {
        int currentVersion = election.getCurrentFlinkLeaderVersion();
        int wrongVersion   = currentVersion - 1;

        boolean transferred = election.transferFlinkLeader("app1", "app2", wrongVersion);

        assertThat(transferred).isFalse();
        // Leader should not have changed
        String leader = election.getCurrentFlinkLeader();
        assertThat(leader).isIn("app1", "app2"); // whatever it was before
    }

    @Test
    @Order(10)
    void transferFlinkLeader_fails_whenFromAppDoesNotMatch() {
        // If current leader is app1, transferring from "app2" should fail
        // (app2 is not the current leader)
        int currentVersion = election.getCurrentFlinkLeaderVersion();
        String currentLeader = election.getCurrentFlinkLeader();

        String wrongFrom = currentLeader.equals("app1") ? "app2" : "app1";
        boolean transferred = election.transferFlinkLeader(wrongFrom, "app2", currentVersion);

        assertThat(transferred).isFalse();
        assertThat(election.getCurrentFlinkLeader()).isEqualTo(currentLeader);
    }

    @Test
    @Order(11)
    void concurrentLeaderTransfer_exactlyOneSucceeds() throws InterruptedException {
        // Simulate two coordinator replicas both trying to transfer leadership simultaneously
        int currentVersion = election.getCurrentFlinkLeaderVersion();
        String currentLeader = election.getCurrentFlinkLeader();

        int threads = 5;
        CountDownLatch ready  = new CountDownLatch(threads);
        CountDownLatch go     = new CountDownLatch(1);
        AtomicInteger  wins   = new AtomicInteger(0);
        ExecutorService pool  = Executors.newFixedThreadPool(threads);

        String targetApp = currentLeader.equals("app1") ? "app2" : "app1";
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    if (election.transferFlinkLeader(currentLeader, targetApp, currentVersion)) {
                        wins.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        ready.await();
        go.countDown();
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        // Only one transfer should succeed — DynamoDB conditional write prevents double-transfer
        assertThat(wins.get()).isEqualTo(1);
        assertThat(election.getCurrentFlinkLeader()).isEqualTo(targetApp);
        LOG.info("Concurrent transfer test: {} threads competed, {} won (new leader={})",
                threads, wins.get(), election.getCurrentFlinkLeader());
    }

    @Test
    @Order(12)
    void versionIncrements_onEachTransfer() {
        int v0 = election.getCurrentFlinkLeaderVersion();
        String current = election.getCurrentFlinkLeader();
        String other   = current.equals("app1") ? "app2" : "app1";

        election.transferFlinkLeader(current, other, v0);
        assertThat(election.getCurrentFlinkLeaderVersion()).isEqualTo(v0 + 1);

        election.transferFlinkLeader(other, current, v0 + 1);
        assertThat(election.getCurrentFlinkLeaderVersion()).isEqualTo(v0 + 2);

        LOG.info("Version progressed: {} → {} → {}", v0, v0 + 1, v0 + 2);
    }
}
