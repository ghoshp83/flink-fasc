package com.flinkfasc.it.coordinator;

import com.flinkfasc.coordinator.election.DynamoDbLeaderElection;
import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import com.flinkfasc.coordinator.handoff.HandoffOrchestrator;
import com.flinkfasc.coordinator.handoff.HandoffState;
import com.flinkfasc.coordinator.monitor.OffsetMonitor;
import com.flinkfasc.coordinator.savepoint.SavepointManager;
import com.flinkfasc.coordinator.savepoint.SavepointMetadata;
import com.flinkfasc.core.FASCConfiguration;
import com.flinkfasc.core.control.ControlSignal;
import com.flinkfasc.core.control.ControlTopicConsumer;
import com.flinkfasc.core.control.ControlTopicProducer;
import com.flinkfasc.core.control.SignalType;
import com.flinkfasc.core.sink.SinkMode;
import com.flinkfasc.it.infrastructure.ContainerSetup;
import com.flinkfasc.it.infrastructure.WireMockFlinkApi;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Integration test: verifies the HandoffOrchestrator state machine runs correctly
 * with real Kafka (control topic signals) and real DynamoDB (leader election via LocalStack).
 *
 * <p>The Flink REST API (SavepointManager) is mocked with WireMock.
 * The OffsetMonitor is mocked (we control when App2 is "ready").
 *
 * <p>Tests:
 * <ul>
 *   <li>Full happy-path handoff: IDLE → ... → HANDOFF_COMPLETE</li>
 *   <li>Handoff rejected when not lock holder</li>
 *   <li>Handoff rejected when already in progress</li>
 *   <li>Abort triggered on drain timeout</li>
 *   <li>Abort triggered when DynamoDB leader transfer fails</li>
 *   <li>Signal with wrong traceId is ignored</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HandoffOrchestratorIT {

    private static final Logger LOG = LoggerFactory.getLogger(HandoffOrchestratorIT.class);

    private static WireMockFlinkApi wireMockFlink;
    private static DynamoDbClient   dynamoDbClient;

    private DynamoDbLeaderElection leaderElection;
    private SavepointManager       savepointManager;
    private OffsetMonitor          offsetMonitor;
    private ControlTopicProducer   controlProducer;
    private HandoffOrchestrator    orchestrator;

    // Capture signals the orchestrator sends to apps
    private final List<ControlSignal> sentSignals = new CopyOnWriteArrayList<>();
    private ControlTopicConsumer signalCaptor;

    private static final String APP1_JOB_ID  = "job-app1-abc123";
    private static final String SAVEPOINT_S3 = "s3://test-bucket/fasc/savepoints/sp-001";
    private static final String TRIGGER_ID   = "trigger-001";
    private static final String VERTEX_ID    = "vertex-source-001";

    @BeforeAll
    static void startInfrastructure() {
        ContainerSetup.start();
        dynamoDbClient = ContainerSetup.dynamoDbClient();

        wireMockFlink = new WireMockFlinkApi();
        wireMockFlink.start();
        LOG.info("WireMock Flink API started on port {}", wireMockFlink.port());
    }

    @AfterAll
    static void tearDownInfrastructure() {
        wireMockFlink.stop();
        if (dynamoDbClient != null) dynamoDbClient.close();
    }

    @BeforeEach
    void setUp() {
        wireMockFlink.resetStubs();
        sentSignals.clear();

        // Stub WireMock for happy path by default
        wireMockFlink.stubJobRunning(APP1_JOB_ID);
        wireMockFlink.stubSavepointSuccess(APP1_JOB_ID, TRIGGER_ID, SAVEPOINT_S3);
        wireMockFlink.stubJobVerticesWithOffsets(APP1_JOB_ID, VERTEX_ID, 5000L);

        // Real DynamoDB leader election (LocalStack)
        FascCoordinatorProperties props = buildProperties();
        leaderElection = new DynamoDbLeaderElection(dynamoDbClient, props);
        leaderElection.releaseLock("coordinator-test");

        // Mock OffsetMonitor — we'll trigger readiness manually
        offsetMonitor = mock(OffsetMonitor.class);
        when(offsetMonitor.getCurrentLag()).thenReturn(Map.of(0, 0L, 1, 0L));
        when(offsetMonitor.isApp2Ready()).thenReturn(false); // starts not ready

        // Real SavepointManager pointing to WireMock
        savepointManager = new SavepointManager(props, new com.fasterxml.jackson.databind.ObjectMapper(), null);

        // Real ControlTopicProducer
        FASCConfiguration fascConfig = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("coordinator")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .build();
        controlProducer = new ControlTopicProducer(fascConfig);

        // Capture signals sent by the orchestrator
        FASCConfiguration captorConfig = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("broadcast")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .build();
        signalCaptor = new ControlTopicConsumer(captorConfig, sentSignals::add);
        signalCaptor.start();
        try { signalCaptor.waitForAssignment(10_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        orchestrator = new HandoffOrchestrator(
                props, savepointManager, offsetMonitor, leaderElection, controlProducer);
        orchestrator.initialize();
    }

    @AfterEach
    void tearDown() {
        signalCaptor.stop();
        controlProducer.close();
        orchestrator.shutdown();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void initialState_isIdle() {
        assertThat(orchestrator.getState()).isEqualTo(HandoffState.IDLE);
    }

    @Test
    @Order(2)
    void triggerHandoff_whenNotLockHolder_throws() {
        // Don't acquire lock — should throw
        assertThatThrownBy(() -> orchestrator.triggerHandoff())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lock");

        assertThat(orchestrator.getState()).isEqualTo(HandoffState.IDLE);
    }

    @Test
    @Order(3)
    void triggerHandoff_whenAlreadyInProgress_throws() throws Exception {
        leaderElection.tryAcquireLock("coordinator-test");

        // First trigger starts the savepoint phase (async)
        orchestrator.triggerHandoff();

        // Second trigger should be rejected
        assertThatThrownBy(() -> orchestrator.triggerHandoff())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("in progress");
    }

    @Test
    @Order(4)
    void fullHandoff_happyPath_completesSuccessfully() throws Exception {
        leaderElection.tryAcquireLock("coordinator-test");

        // Step 1: Trigger handoff
        orchestrator.triggerHandoff();

        // Step 2: Savepoint phase should complete quickly (WireMock responds immediately)
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(orchestrator.getState())
                        .isEqualTo(HandoffState.WAITING_FOR_APP2_CATCHUP));

        // Step 3: Simulate App2 catching up (trigger the readiness callback)
        when(offsetMonitor.isApp2Ready()).thenReturn(true);
        when(offsetMonitor.getCurrentLag()).thenReturn(Map.of(0, 0L, 1, 0L));
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP2_CATCHUP,
                Map.of(0, 5000L, 1, 4999L), "test-trace");

        // Manually trigger the onApp2CaughtUp callback (in real system this fires from OffsetMonitor)
        orchestrator.simulateApp2ReadyForTest();

        // Step 4: Move to handoff initiated — PREPARE_HANDOFF should be sent
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(sentSignals)
                        .anyMatch(s -> s.getSignalType() == SignalType.PREPARE_HANDOFF));

        // Step 5: Simulate App1 sending DRAINED_AT
        Map<Integer, Long> offsets = Map.of(0, 5000L, 1, 4999L);
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN, offsets, "test-trace");
        orchestrator.onSignalReceived(ControlSignal.drainedAt("app1", "test-trace", offsets));

        // Step 6: Coordinator should transfer DynamoDB leadership and advance to PROMOTING_APP2
        // (PROMOTE signal is targeted at "app2" — not captured by the broadcast signalCaptor)
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(orchestrator.getState())
                        .isIn(HandoffState.PROMOTING_APP2, HandoffState.HANDOFF_COMPLETE));

        // Verify DynamoDB leader was transferred
        assertThat(leaderElection.getCurrentFlinkLeader()).isEqualTo("app2");

        // Step 7: Simulate App2 sending PROMOTED_CONFIRMED
        orchestrator.onSignalReceived(ControlSignal.promotedConfirmed("app2", "test-trace"));

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(orchestrator.getState())
                        .isEqualTo(HandoffState.HANDOFF_COMPLETE));

        LOG.info("Handoff completed successfully. New leader: {}",
                leaderElection.getCurrentFlinkLeader());
    }

    @Test
    @Order(5)
    void drainTimeout_causesHandoffAbort() {
        leaderElection.tryAcquireLock("coordinator-test");
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN,
                Map.of(0, 5000L), "trace-timeout");

        // Trigger drain timeout
        orchestrator.onDrainTimeout();

        assertThat(orchestrator.getState()).isEqualTo(HandoffState.HANDOFF_FAILED);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(sentSignals)
                        .anyMatch(s -> s.getSignalType() == SignalType.HANDOFF_ABORT));
    }

    @Test
    @Order(6)
    void signalWithWrongTraceId_isIgnored() {
        leaderElection.tryAcquireLock("coordinator-test");
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN,
                Map.of(0, 5000L), "correct-trace");

        // Send DRAINED_AT with wrong traceId
        orchestrator.onSignalReceived(ControlSignal.drainedAt("app1", "WRONG-trace",
                Map.of(0, 5000L)));

        // State should not advance
        assertThat(orchestrator.getState()).isEqualTo(HandoffState.WAITING_FOR_APP1_DRAIN);
    }

    @Test
    @Order(7)
    void leaderTransferFailure_causesHandoffAbort() {
        leaderElection.tryAcquireLock("coordinator-test");

        // Corrupt the version to cause conditional check failure
        Map<Integer, Long> offsets = Map.of(0, 5000L);
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN, offsets, "trace-fail");

        // Change the version in DynamoDB so the transfer will fail
        int currentVersion = leaderElection.getCurrentFlinkLeaderVersion();
        // Transfer once to increment version (makes our stored version stale)
        leaderElection.transferFlinkLeader(
                leaderElection.getCurrentFlinkLeader(), "app2", currentVersion);

        // Now orchestrator tries to transfer with old version — should fail → abort
        orchestrator.onSignalReceived(ControlSignal.drainedAt("app1", "trace-fail", offsets));

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(orchestrator.getState())
                        .isEqualTo(HandoffState.HANDOFF_FAILED));
    }

    @Test
    @Order(8)
    void heartbeats_areSentPeriodically() throws InterruptedException {
        leaderElection.tryAcquireLock("coordinator-test");

        // Wait for at least 2 heartbeats (sent every 5s by @Scheduled, but we set 100ms in test props)
        Thread.sleep(1500);

        assertThat(sentSignals)
                .filteredOn(s -> s.getSignalType() == SignalType.HEARTBEAT)
                .isNotEmpty();
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private FascCoordinatorProperties buildProperties() {
        FascCoordinatorProperties props = new FascCoordinatorProperties();
        props.setCoordinatorId("coordinator-test");
        props.setApp1JobId(APP1_JOB_ID);
        props.setApp2JobId("job-app2-xyz");
        props.setApp1FlinkRestUrl(wireMockFlink.baseUrl());
        props.setApp2FlinkRestUrl(wireMockFlink.baseUrl());
        props.setApp1ConsumerGroup("consumer-group-app1");
        props.setApp2ConsumerGroup("consumer-group-app2");
        props.setBootstrapServers(ContainerSetup.kafkaBootstrapServers());
        props.setControlTopic(ContainerSetup.CONTROL_TOPIC);
        props.setSavepointS3Bucket("test-bucket");
        props.setSavepointS3Prefix("fasc/savepoints/");
        props.setAwsRegion("eu-west-2");
        props.setDynamoDbLeaderTable(ContainerSetup.LEADER_TABLE);
        props.setDynamoDbLockTable(ContainerSetup.LOCK_TABLE);
        props.setHeartbeatIntervalMs(500); // fast heartbeats for testing
        props.setLagStabilityWindowMs(1000);
        props.setOffsetMonitorIntervalMs(500);
        return props;
    }
}
