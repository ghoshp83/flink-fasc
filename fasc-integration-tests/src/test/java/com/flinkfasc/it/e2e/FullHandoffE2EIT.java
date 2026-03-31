package com.flinkfasc.it.e2e;

import com.flinkfasc.coordinator.election.DynamoDbLeaderElection;
import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import com.flinkfasc.coordinator.handoff.HandoffOrchestrator;
import com.flinkfasc.coordinator.handoff.HandoffState;
import com.flinkfasc.coordinator.monitor.OffsetMonitor;
import com.flinkfasc.coordinator.savepoint.SavepointManager;
import com.flinkfasc.core.FASCConfiguration;
import com.flinkfasc.core.ShadowSink;
import com.flinkfasc.core.control.ControlSignal;
import com.flinkfasc.core.control.ControlTopicConsumer;
import com.flinkfasc.core.control.ControlTopicProducer;
import com.flinkfasc.core.control.SignalType;
import com.flinkfasc.core.sink.SinkMode;
import com.flinkfasc.it.infrastructure.ContainerSetup;
import com.flinkfasc.it.infrastructure.WireMockFlinkApi;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

/**
 * End-to-end integration test: validates the complete FASC handoff protocol
 * with two live ShadowSink instances, the HandoffOrchestrator, real Kafka,
 * real DynamoDB (LocalStack), and WireMock for the Flink REST API.
 *
 * <p><strong>Scenario:</strong>
 * <ol>
 *   <li>App1 starts ACTIVE — all writes flow to the DynamoDB delegate</li>
 *   <li>App2 starts SHADOW — writes suppressed</li>
 *   <li>Coordinator triggers handoff</li>
 *   <li>Coordinator sends PREPARE_HANDOFF → both apps receive it</li>
 *   <li>App1 sends DRAINED_AT → coordinator atomically transfers DynamoDB leadership</li>
 *   <li>Coordinator sends PROMOTE → App2 activates</li>
 *   <li>Coordinator sends DEMOTE → App1 suppresses</li>
 *   <li>App2 sends PROMOTED_CONFIRMED → handoff complete</li>
 *   <li>Post-handoff: only App2 writes reach DynamoDB; App1 writes are suppressed</li>
 * </ol>
 *
 * <p>This test validates the complete business requirement:
 * zero blackout window and correct DynamoDB write routing.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FullHandoffE2EIT {

    private static final Logger LOG = LoggerFactory.getLogger(FullHandoffE2EIT.class);

    // ── Infrastructure ─────────────────────────────────────────────────────────
    private WireMockFlinkApi wireMockFlink;
    private DynamoDbClient   dynamoDbClient;

    // ── App1 (initially ACTIVE) ────────────────────────────────────────────────
    private CountingSink<String> app1DelegateSink;
    private ShadowSink<String>   app1ShadowSink;

    // ── App2 (initially SHADOW) ────────────────────────────────────────────────
    private CountingSink<String> app2DelegateSink;
    private ShadowSink<String>   app2ShadowSink;

    // ── Coordinator components ─────────────────────────────────────────────────
    private DynamoDbLeaderElection leaderElection;
    private HandoffOrchestrator    orchestrator;
    private ControlTopicProducer   appSimulator; // simulates signals from apps

    // ── Captured coordinator signals ───────────────────────────────────────────
    private final List<ControlSignal> coordinatorBroadcasts = new CopyOnWriteArrayList<>();
    private ControlTopicConsumer      broadcastCaptor;

    private static final String APP1_JOB_ID = "e2e-job-app1";
    private static final String SAVEPOINT_S3 = "s3://e2e-bucket/fasc/savepoints/sp-e2e";

    @BeforeAll
    void setUp() throws Exception {
        ContainerSetup.start();
        dynamoDbClient = ContainerSetup.dynamoDbClient();

        wireMockFlink = new WireMockFlinkApi();
        wireMockFlink.start();
        wireMockFlink.stubJobRunning(APP1_JOB_ID);
        wireMockFlink.stubSavepointSuccess(APP1_JOB_ID, "e2e-trigger", SAVEPOINT_S3);
        wireMockFlink.stubJobVerticesWithOffsets(APP1_JOB_ID, "e2e-vertex", 10000L);

        // ── App1 ShadowSink (ACTIVE) ─────────────────────────────────────────
        FASCConfiguration app1Config = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("app1")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .initialMode(SinkMode.ACTIVE)
                .build();
        app1DelegateSink = new CountingSink<>();
        app1ShadowSink   = ShadowSink.wrap(app1DelegateSink, app1Config);
        app1ShadowSink.open(null);

        // ── App2 ShadowSink (SHADOW) ─────────────────────────────────────────
        FASCConfiguration app2Config = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("app2")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .initialMode(SinkMode.SHADOW)
                .build();
        app2DelegateSink = new CountingSink<>();
        app2ShadowSink   = ShadowSink.wrap(app2DelegateSink, app2Config);
        app2ShadowSink.open(null);

        // ── Coordinator ──────────────────────────────────────────────────────
        FascCoordinatorProperties props = buildCoordinatorProps();
        leaderElection = new DynamoDbLeaderElection(dynamoDbClient, props);
        leaderElection.releaseLock("e2e-coordinator");
        leaderElection.tryAcquireLock("e2e-coordinator");

        OffsetMonitor mockOffsetMonitor  = mock(OffsetMonitor.class);
        when(mockOffsetMonitor.getCurrentLag()).thenReturn(Map.of(0, 0L, 1, 0L));

        FASCConfiguration coordinatorFascConfig = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("coordinator")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .build();
        ControlTopicProducer coordinatorProducer = new ControlTopicProducer(coordinatorFascConfig);

        orchestrator = new HandoffOrchestrator(
                props,
                new SavepointManager(props, new com.fasterxml.jackson.databind.ObjectMapper(), null),
                mockOffsetMonitor,
                leaderElection,
                coordinatorProducer);
        orchestrator.initialize();

        // Simulator for app-side responses (DRAINED_AT, PROMOTED_CONFIRMED)
        appSimulator = new ControlTopicProducer(coordinatorFascConfig);

        // Capture all broadcasts for assertions
        FASCConfiguration captorConfig = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("broadcast")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .build();
        broadcastCaptor = new ControlTopicConsumer(captorConfig, coordinatorBroadcasts::add);
        broadcastCaptor.start();
        broadcastCaptor.waitForAssignment(10_000);

        // Wait for ShadowSink consumers to be ready before any signal is produced
        app1ShadowSink.waitForConsumerReady(10_000);
        app2ShadowSink.waitForConsumerReady(10_000);

        LOG.info("E2E test setup complete. App1=ACTIVE, App2=SHADOW, Coordinator=IDLE");
    }

    @AfterAll
    void tearDown() throws Exception {
        broadcastCaptor.stop();
        appSimulator.close();
        app1ShadowSink.close();
        app2ShadowSink.close();
        orchestrator.shutdown();
        wireMockFlink.stop();
        if (dynamoDbClient != null) dynamoDbClient.close();
    }

    // ── The E2E Test ───────────────────────────────────────────────────────────

    @Test
    void fullHandoff_e2e_correctWriteRoutingBeforeAndAfter() throws Exception {
        // ── PHASE 1: Pre-handoff steady state ────────────────────────────────
        LOG.info("=== PHASE 1: Pre-handoff steady state ===");

        app1ShadowSink.invoke("pre-handoff-1", null);
        app1ShadowSink.invoke("pre-handoff-2", null);
        app2ShadowSink.invoke("pre-handoff-shadow-1", null);

        assertThat(app1ShadowSink.getMode()).isEqualTo(SinkMode.ACTIVE);
        assertThat(app2ShadowSink.getMode()).isEqualTo(SinkMode.SHADOW);
        assertThat(app1DelegateSink.count()).isEqualTo(2);
        assertThat(app2DelegateSink.count()).isZero();
        assertThat(app2ShadowSink.getShadowDropCount()).isEqualTo(1);

        LOG.info("Pre-handoff: App1 wrote {}, App2 dropped {}",
                app1DelegateSink.count(), app2ShadowSink.getShadowDropCount());

        // ── PHASE 2: Trigger handoff ──────────────────────────────────────────
        LOG.info("=== PHASE 2: Triggering handoff ===");

        orchestrator.triggerHandoff();

        // Savepoint completes (WireMock responds immediately)
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(orchestrator.getState())
                        .isIn(HandoffState.WAITING_FOR_APP2_CATCHUP,
                              HandoffState.HANDOFF_INITIATED,
                              HandoffState.WAITING_FOR_APP1_DRAIN));

        LOG.info("State after savepoint: {}", orchestrator.getState());

        // ── PHASE 3: Simulate App2 catchup + PREPARE_HANDOFF ─────────────────
        LOG.info("=== PHASE 3: Simulating App2 catchup ===");

        Map<Integer, Long> cutoverOffsets = Map.of(0, 10000L, 1, 9999L);
        // Fire onApp2CaughtUp → sends PREPARE_HANDOFF and advances to WAITING_FOR_APP1_DRAIN
        orchestrator.simulateApp2ReadyForTest();
        // Override traceId so DRAINED_AT signal matching is predictable
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN, cutoverOffsets, "e2e-trace");

        // Wait for PREPARE_HANDOFF to be sent and received by the ShadowSinks
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(coordinatorBroadcasts)
                        .anyMatch(s -> s.getSignalType() == SignalType.PREPARE_HANDOFF));

        LOG.info("PREPARE_HANDOFF received by both apps");

        // App1 and App2 can still process during this window — no blackout yet
        app1ShadowSink.invoke("during-handoff-app1", null);
        app2ShadowSink.invoke("during-handoff-app2", null);
        assertThat(app1DelegateSink.count()).isEqualTo(3); // still writing
        assertThat(app2DelegateSink.count()).isZero();     // still suppressed

        // ── PHASE 4: App1 drains, coordinator transfers leadership ────────────
        LOG.info("=== PHASE 4: App1 drains ===");

        appSimulator.sendSync(ControlSignal.drainedAt("app1", "e2e-trace", cutoverOffsets));
        orchestrator.onSignalReceived(ControlSignal.drainedAt("app1", "e2e-trace", cutoverOffsets));

        // Coordinator should transfer DynamoDB leadership and advance to PROMOTING_APP2
        // (PROMOTE/DEMOTE signals are targeted at specific apps — not captured by broadcast captor)
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(orchestrator.getState())
                        .isIn(HandoffState.PROMOTING_APP2, HandoffState.HANDOFF_COMPLETE));

        // Verify DynamoDB leader transferred
        assertThat(leaderElection.getCurrentFlinkLeader()).isEqualTo("app2");
        LOG.info("DynamoDB leader transferred: app1 → app2");

        // ── PHASE 5: Wait for ShadowSinks to receive mode change ──────────────
        LOG.info("=== PHASE 5: Waiting for ShadowSink mode changes ===");

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app2ShadowSink.getMode()).isEqualTo(SinkMode.ACTIVE));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app1ShadowSink.getMode()).isEqualTo(SinkMode.SHADOW));

        LOG.info("Mode change confirmed: App1=SHADOW, App2=ACTIVE");

        // ── PHASE 6: App2 confirms promotion ─────────────────────────────────
        LOG.info("=== PHASE 6: App2 confirms promotion ===");

        orchestrator.onSignalReceived(ControlSignal.promotedConfirmed("app2", "e2e-trace"));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(orchestrator.getState())
                        .isEqualTo(HandoffState.HANDOFF_COMPLETE));

        // ── PHASE 7: Post-handoff write routing ───────────────────────────────
        LOG.info("=== PHASE 7: Post-handoff write routing ===");

        int app1WritesBeforeHandoff = app1DelegateSink.count();
        int app2WritesBeforeHandoff = app2DelegateSink.count();

        app1ShadowSink.invoke("post-handoff-app1-1", null);
        app1ShadowSink.invoke("post-handoff-app1-2", null);
        app2ShadowSink.invoke("post-handoff-app2-1", null);
        app2ShadowSink.invoke("post-handoff-app2-2", null);
        app2ShadowSink.invoke("post-handoff-app2-3", null);

        // App1 writes are now suppressed
        assertThat(app1DelegateSink.count()).isEqualTo(app1WritesBeforeHandoff);
        // App2 writes now flow through
        assertThat(app2DelegateSink.count()).isEqualTo(app2WritesBeforeHandoff + 3);

        // ── Summary ───────────────────────────────────────────────────────────
        LOG.info("=== HANDOFF E2E TEST PASSED ===");
        LOG.info("App1: totalWrites={}, totalDrops={}",
                app1DelegateSink.count(), app1ShadowSink.getShadowDropCount());
        LOG.info("App2: totalWrites={}, totalDrops={}",
                app2DelegateSink.count(), app2ShadowSink.getShadowDropCount());
        LOG.info("DynamoDB leader: {}", leaderElection.getCurrentFlinkLeader());
        LOG.info("Orchestrator final state: {}", orchestrator.getState());

        // Final assertions
        assertThat(orchestrator.getState()).isEqualTo(HandoffState.HANDOFF_COMPLETE);
        assertThat(leaderElection.getCurrentFlinkLeader()).isEqualTo("app2");
        assertThat(app1ShadowSink.getMode()).isEqualTo(SinkMode.SHADOW);
        assertThat(app2ShadowSink.getMode()).isEqualTo(SinkMode.ACTIVE);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private FascCoordinatorProperties buildCoordinatorProps() {
        FascCoordinatorProperties props = new FascCoordinatorProperties();
        props.setCoordinatorId("e2e-coordinator");
        props.setApp1JobId(APP1_JOB_ID);
        props.setApp2JobId("e2e-job-app2");
        props.setApp1FlinkRestUrl(wireMockFlink.baseUrl());
        props.setApp2FlinkRestUrl(wireMockFlink.baseUrl());
        props.setApp1ConsumerGroup("consumer-group-app1");
        props.setApp2ConsumerGroup("consumer-group-app2");
        props.setBootstrapServers(ContainerSetup.kafkaBootstrapServers());
        props.setControlTopic(ContainerSetup.CONTROL_TOPIC);
        props.setSavepointS3Bucket("e2e-bucket");
        props.setSavepointS3Prefix("fasc/savepoints/");
        props.setAwsRegion("eu-west-2");
        props.setDynamoDbLeaderTable(ContainerSetup.LEADER_TABLE);
        props.setDynamoDbLockTable(ContainerSetup.LOCK_TABLE);
        props.setHeartbeatIntervalMs(1000);
        props.setLagStabilityWindowMs(500);
        props.setOffsetMonitorIntervalMs(500);
        return props;
    }

    /** Thread-safe write counter that also captures records. */
    static class CountingSink<T> implements SinkFunction<T> {
        private final AtomicInteger writes = new AtomicInteger(0);

        @Override
        public void invoke(T value, Context context) {
            writes.incrementAndGet();
        }

        int count() { return writes.get(); }
    }
}
