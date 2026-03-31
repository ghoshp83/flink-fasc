package com.flinkfasc.it.core;

import com.flinkfasc.core.FASCConfiguration;
import com.flinkfasc.core.control.ControlSignal;
import com.flinkfasc.core.control.ControlTopicConsumer;
import com.flinkfasc.core.control.ControlTopicProducer;
import com.flinkfasc.core.control.SignalType;
import com.flinkfasc.core.sink.SinkMode;
import com.flinkfasc.it.infrastructure.ContainerSetup;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test: verifies that ControlTopicProducer and ControlTopicConsumer
 * correctly round-trip all signal types through a real Kafka broker (Testcontainers).
 *
 * <p>Tests:
 * <ul>
 *   <li>All 7 signal types serialize/deserialize correctly end-to-end</li>
 *   <li>Consumer filters signals by targetAppId (ignores signals for other apps)</li>
 *   <li>Broadcast signals (targetAppId="broadcast") are received by all consumers</li>
 *   <li>Multiple concurrent signals are delivered in order</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ControlTopicRoundTripIT {

    private static final Logger LOG = LoggerFactory.getLogger(ControlTopicRoundTripIT.class);

    private static FASCConfiguration app1Config;
    private static FASCConfiguration app2Config;

    private ControlTopicProducer producer;
    private ControlTopicConsumer app1Consumer;
    private ControlTopicConsumer app2Consumer;

    private final List<ControlSignal> app1Received = new CopyOnWriteArrayList<>();
    private final List<ControlSignal> app2Received = new CopyOnWriteArrayList<>();

    @BeforeAll
    static void startContainers() {
        ContainerSetup.start();

        app1Config = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("app1")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .initialMode(SinkMode.ACTIVE)
                .build();

        app2Config = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("app2")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .initialMode(SinkMode.SHADOW)
                .build();
    }

    @BeforeEach
    void setUp() throws InterruptedException {
        app1Received.clear();
        app2Received.clear();

        producer     = new ControlTopicProducer(app1Config);
        app1Consumer = new ControlTopicConsumer(app1Config, signal -> app1Received.add(signal));
        app2Consumer = new ControlTopicConsumer(app2Config, signal -> app2Received.add(signal));

        app1Consumer.start();
        app2Consumer.start();

        // Wait for both consumers to receive partition assignment before any test produces messages.
        // CI environments (GitHub Actions) can be slow — use generous timeouts.
        app1Consumer.waitForAssignment(30_000);
        app2Consumer.waitForAssignment(30_000);
        // Allow one extra poll cycle so the consumer is actively polling before we produce
        Thread.sleep(2_000);
    }

    @AfterEach
    void tearDown() {
        app1Consumer.stop();
        app2Consumer.stop();
        producer.close();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void heartbeat_isReceivedByBothApps() {
        ControlSignal heartbeat = ControlSignal.heartbeat("coordinator-1");
        producer.sendSync(heartbeat);

        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(app1Received).anyMatch(s -> s.getSignalType() == SignalType.HEARTBEAT);
                    assertThat(app2Received).anyMatch(s -> s.getSignalType() == SignalType.HEARTBEAT);
                });
    }

    @Test
    @Order(2)
    void promote_isReceivedOnlyByApp2() {
        ControlSignal promote = ControlSignal.promote("coordinator-1", "trace-001", "app2");
        producer.sendSync(promote);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                    assertThat(app2Received).anyMatch(s -> s.getSignalType() == SignalType.PROMOTE));

        // app1 should NOT receive a signal targeted at app2
        // (give a short wait to confirm nothing arrives)
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        assertThat(app1Received)
                .filteredOn(s -> s.getSignalType() == SignalType.PROMOTE)
                .isEmpty();
    }

    @Test
    @Order(3)
    void demote_isReceivedOnlyByApp1() {
        ControlSignal demote = ControlSignal.demote("coordinator-1", "trace-002", "app1");
        producer.sendSync(demote);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                    assertThat(app1Received).anyMatch(s -> s.getSignalType() == SignalType.DEMOTE));

        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        assertThat(app2Received)
                .filteredOn(s -> s.getSignalType() == SignalType.DEMOTE)
                .isEmpty();
    }

    @Test
    @Order(4)
    void prepareHandoff_isBroadcastToBothApps() {
        Map<Integer, Long> offsets = Map.of(0, 5000L, 1, 4999L, 23, 5100L);
        ControlSignal prepareHandoff = ControlSignal.prepareHandoff("coordinator-1", "trace-003", offsets);
        producer.sendSync(prepareHandoff);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(app1Received)
                            .anyMatch(s -> s.getSignalType() == SignalType.PREPARE_HANDOFF);
                    assertThat(app2Received)
                            .anyMatch(s -> s.getSignalType() == SignalType.PREPARE_HANDOFF);
                });

        // Verify cutover offsets survived round-trip correctly
        ControlSignal received = app2Received.stream()
                .filter(s -> s.getSignalType() == SignalType.PREPARE_HANDOFF)
                .findFirst().orElseThrow();
        assertThat(received.getCutoverOffsets()).containsEntry(0, 5000L);
        assertThat(received.getCutoverOffsets()).containsEntry(23, 5100L);
        assertThat(received.getTraceId()).isEqualTo("trace-003");
    }

    @Test
    @Order(5)
    void drainedAt_preservesOffsetsAndSource() {
        Map<Integer, Long> offsets = Map.of(0, 6000L, 1, 5999L);
        ControlSignal drainedAt = ControlSignal.drainedAt("app1", "trace-004", offsets);
        producer.sendSync(drainedAt);

        // The coordinator would consume this — in this test app1's consumer receives it
        // (since app1 also listens to the control topic in real setup)
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                    assertThat(app1Received)
                            .anyMatch(s -> s.getSignalType() == SignalType.DRAINED_AT
                                       && "app1".equals(s.getSourceAppId())
                                       && s.getCutoverOffsets().get(0) == 6000L));
    }

    @Test
    @Order(6)
    void abort_isReceivedByBothApps() {
        ControlSignal abort = ControlSignal.abort("coordinator-1", "trace-005", "drain timeout after 60s");
        producer.sendSync(abort);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertThat(app1Received).anyMatch(s -> s.getSignalType() == SignalType.HANDOFF_ABORT);
                    assertThat(app2Received).anyMatch(s -> s.getSignalType() == SignalType.HANDOFF_ABORT);
                });

        ControlSignal receivedAbort = app1Received.stream()
                .filter(s -> s.getSignalType() == SignalType.HANDOFF_ABORT)
                .findFirst().orElseThrow();
        assertThat(receivedAbort.getReason()).isEqualTo("drain timeout after 60s");
    }

    @Test
    @Order(7)
    void multipleSignals_areDeliveredInOrder() {
        List<SignalType> expectedOrder = List.of(
                SignalType.HEARTBEAT,
                SignalType.PREPARE_HANDOFF,
                SignalType.PROMOTE
        );

        producer.sendSync(ControlSignal.heartbeat("coordinator-1"));
        producer.sendSync(ControlSignal.prepareHandoff("coordinator-1", "trace-seq", Map.of()));
        producer.sendSync(ControlSignal.promote("coordinator-1", "trace-seq", "app2"));

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() ->
                    assertThat(app2Received.stream()
                            .map(ControlSignal::getSignalType)
                            .toList())
                            .containsSubsequence(expectedOrder));
    }

    @Test
    @Order(8)
    void consumerStop_stopsPolling() throws InterruptedException {
        // Stop app2 consumer
        app2Consumer.stop();

        // Send a signal — app2 should NOT receive it after stopping
        Thread.sleep(500);
        int countBefore = app2Received.size();
        producer.sendSync(ControlSignal.heartbeat("coordinator-1"));
        Thread.sleep(1000);

        assertThat(app2Received.size()).isEqualTo(countBefore);
    }
}
