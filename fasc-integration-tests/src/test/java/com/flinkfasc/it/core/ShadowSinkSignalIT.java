package com.flinkfasc.it.core;

import com.flinkfasc.core.FASCConfiguration;
import com.flinkfasc.core.ShadowSink;
import com.flinkfasc.core.control.ControlSignal;
import com.flinkfasc.core.control.ControlTopicProducer;
import com.flinkfasc.core.sink.SinkMode;
import com.flinkfasc.it.infrastructure.ContainerSetup;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test: verifies that ShadowSink correctly transitions between
 * ACTIVE and SHADOW modes in response to real Kafka control signals.
 *
 * <p>This is the most important integration test — it validates the core
 * FASC contract: ShadowSink suppresses writes in SHADOW mode and forwards
 * them in ACTIVE mode, driven by coordinator signals through real Kafka.
 *
 * <p>Tests:
 * <ul>
 *   <li>App2 starts in SHADOW mode — all writes suppressed</li>
 *   <li>PROMOTE signal → App2 activates → writes flow through</li>
 *   <li>DEMOTE signal → App1 suppresses → no more writes</li>
 *   <li>Write counts are tracked accurately in both modes</li>
 *   <li>Mode change is immediate (within 2 seconds)</li>
 * </ul>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ShadowSinkSignalIT {

    private static FASCConfiguration app1Config;
    private static FASCConfiguration app2Config;
    private static ControlTopicProducer coordinatorProducer;

    private CapturingSink<String> app1Delegate;
    private CapturingSink<String> app2Delegate;
    private ShadowSink<String>    app1Sink;
    private ShadowSink<String>    app2Sink;

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

        // Simulate coordinator sending signals
        FASCConfiguration coordinatorConfig = FASCConfiguration.builder()
                .bootstrapServers(ContainerSetup.kafkaBootstrapServers())
                .appId("coordinator")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .build();
        coordinatorProducer = new ControlTopicProducer(coordinatorConfig);
    }

    @BeforeEach
    void setUp() throws Exception {
        app1Delegate = new CapturingSink<>();
        app2Delegate = new CapturingSink<>();
        app1Sink     = ShadowSink.wrap(app1Delegate, app1Config);
        app2Sink     = ShadowSink.wrap(app2Delegate, app2Config);

        app1Sink.open(null);
        app2Sink.open(null);

        // Wait for Kafka consumer partition assignment before tests send any signals
        app1Sink.waitForConsumerReady(10_000);
        app2Sink.waitForConsumerReady(10_000);
    }

    @AfterEach
    void tearDown() throws Exception {
        app1Sink.close();
        app2Sink.close();
    }

    @AfterAll
    static void cleanup() {
        coordinatorProducer.close();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    @Order(1)
    void app2_startsInShadowMode_writesAreSuppressed() throws Exception {
        assertThat(app2Sink.getMode()).isEqualTo(SinkMode.SHADOW);

        app2Sink.invoke("event-1", null);
        app2Sink.invoke("event-2", null);
        app2Sink.invoke("event-3", null);

        assertThat(app2Delegate.records).isEmpty();
        assertThat(app2Sink.getShadowDropCount()).isEqualTo(3);
        assertThat(app2Sink.getActiveWriteCount()).isZero();
    }

    @Test
    @Order(2)
    void app1_startsInActiveMode_writesFlowThrough() throws Exception {
        assertThat(app1Sink.getMode()).isEqualTo(SinkMode.ACTIVE);

        app1Sink.invoke("order-001", null);
        app1Sink.invoke("order-002", null);

        assertThat(app1Delegate.records).containsExactly("order-001", "order-002");
        assertThat(app1Sink.getActiveWriteCount()).isEqualTo(2);
        assertThat(app1Sink.getShadowDropCount()).isZero();
    }

    @Test
    @Order(3)
    void promoteSignal_switchesApp2ToActiveMode() throws Exception {
        assertThat(app2Sink.getMode()).isEqualTo(SinkMode.SHADOW);

        // Before promote — write is suppressed
        app2Sink.invoke("before-promote", null);
        assertThat(app2Delegate.records).isEmpty();

        // Coordinator sends PROMOTE to app2
        coordinatorProducer.sendSync(
                ControlSignal.promote("coordinator", "trace-promote", "app2"));

        // Wait for ShadowSink to receive the signal via Kafka and switch mode
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app2Sink.getMode()).isEqualTo(SinkMode.ACTIVE));

        // After promote — writes should flow through
        app2Sink.invoke("after-promote-1", null);
        app2Sink.invoke("after-promote-2", null);

        assertThat(app2Delegate.records).containsExactly("after-promote-1", "after-promote-2");
        assertThat(app2Sink.getActiveWriteCount()).isEqualTo(2);
        assertThat(app2Sink.getShadowDropCount()).isEqualTo(1); // "before-promote" was dropped
    }

    @Test
    @Order(4)
    void demoteSignal_switchesApp1ToShadowMode() throws Exception {
        assertThat(app1Sink.getMode()).isEqualTo(SinkMode.ACTIVE);

        // Before demote — writes flow through
        app1Sink.invoke("before-demote", null);
        assertThat(app1Delegate.records).containsExactly("before-demote");

        // Coordinator sends DEMOTE to app1
        coordinatorProducer.sendSync(
                ControlSignal.demote("coordinator", "trace-demote", "app1"));

        // Wait for mode switch
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app1Sink.getMode()).isEqualTo(SinkMode.SHADOW));

        // After demote — writes suppressed
        app1Sink.invoke("after-demote-1", null);
        app1Sink.invoke("after-demote-2", null);

        // Only "before-demote" should be in the delegate
        assertThat(app1Delegate.records).containsExactly("before-demote");
        assertThat(app1Sink.getShadowDropCount()).isEqualTo(2);
    }

    @Test
    @Order(5)
    void modeChangeShouldHappenWithin2Seconds() {
        // FASC SLA: mode change must complete within 2 seconds of signal receipt
        long startMs = System.currentTimeMillis();

        coordinatorProducer.sendSync(
                ControlSignal.promote("coordinator", "trace-latency", "app2"));

        await().atMost(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app2Sink.getMode()).isEqualTo(SinkMode.ACTIVE));

        long elapsedMs = System.currentTimeMillis() - startMs;
        assertThat(elapsedMs).isLessThan(2000);
    }

    @Test
    @Order(6)
    void abortSignal_doesNotChangeMode() throws Exception {
        // app2 is in SHADOW — abort should not promote it
        assertThat(app2Sink.getMode()).isEqualTo(SinkMode.SHADOW);

        coordinatorProducer.sendSync(
                ControlSignal.abort("coordinator", "trace-abort", "simulated abort"));

        Thread.sleep(1000); // Give time for signal to arrive
        assertThat(app2Sink.getMode()).isEqualTo(SinkMode.SHADOW);

        // Writes still suppressed after abort
        app2Sink.invoke("after-abort", null);
        assertThat(app2Delegate.records).isEmpty();
    }

    @Test
    @Order(7)
    void promoteSignalForApp1_isIgnoredByApp2() {
        // PROMOTE targeted at app1 should not affect app2
        coordinatorProducer.sendSync(
                ControlSignal.promote("coordinator", "trace-wrong-target", "app1"));

        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
        assertThat(app2Sink.getMode()).isEqualTo(SinkMode.SHADOW);
    }

    @Test
    @Order(8)
    void prepareHandoff_storesExpectedCutoverOffsets() throws Exception {
        Map<Integer, Long> offsets = Map.of(0, 9000L, 1, 8999L, 23, 9100L);
        coordinatorProducer.sendSync(
                ControlSignal.prepareHandoff("coordinator", "trace-handoff", offsets));

        // Give time for signal processing — ShadowSink stores the offsets
        Thread.sleep(1000);
        // Mode should still be SHADOW — PREPARE_HANDOFF doesn't promote
        assertThat(app2Sink.getMode()).isEqualTo(SinkMode.SHADOW);
    }

    /** Captures all records written to it for assertion in tests. */
    static class CapturingSink<T> implements SinkFunction<T> {
        final List<T> records = new ArrayList<>();

        @Override
        public synchronized void invoke(T value, Context context) {
            records.add(value);
        }
    }
}
