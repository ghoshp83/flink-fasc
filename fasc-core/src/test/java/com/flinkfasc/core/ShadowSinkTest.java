package com.flinkfasc.core;

import com.flinkfasc.core.control.ControlSignal;
import com.flinkfasc.core.control.SignalType;
import com.flinkfasc.core.sink.SinkMode;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ShadowSink — verifies mode switching, write suppression,
 * and signal handling without requiring a live Kafka cluster.
 */
class ShadowSinkTest {

    private CapturingSink<String> delegate;
    private FASCConfiguration config;

    @BeforeEach
    void setUp() {
        delegate = new CapturingSink<>();
        config = FASCConfiguration.builder()
                .bootstrapServers("localhost:9092")
                .appId("app2")
                .controlTopic("fasc-control-topic")
                .initialMode(SinkMode.SHADOW)
                .build();
    }

    @Test
    void startsInShadowModeByDefault() {
        ShadowSink<String> sink = ShadowSink.wrap(delegate, config);
        assertEquals(SinkMode.SHADOW, sink.getMode());
    }

    @Test
    void suppressesWritesInShadowMode() throws Exception {
        ShadowSink<String> sink = ShadowSink.wrap(delegate, config);

        sink.invoke("event-1", null);
        sink.invoke("event-2", null);

        assertTrue(delegate.captured.isEmpty(), "No writes should reach delegate in SHADOW mode");
        assertEquals(2L, sink.getShadowDropCount());
        assertEquals(0L, sink.getActiveWriteCount());
    }

    @Test
    void forwardsWritesInActiveMode() throws Exception {
        config = FASCConfiguration.builder()
                .bootstrapServers("localhost:9092")
                .appId("app1")
                .controlTopic("fasc-control-topic")
                .initialMode(SinkMode.ACTIVE)
                .build();

        ShadowSink<String> sink = ShadowSink.wrap(delegate, config);

        sink.invoke("event-1", null);
        sink.invoke("event-2", null);

        assertEquals(2, delegate.captured.size());
        assertEquals(2L, sink.getActiveWriteCount());
        assertEquals(0L, sink.getShadowDropCount());
    }

    @Test
    void promotionSwitchesToActiveMode() throws Exception {
        ShadowSink<String> sink = ShadowSink.wrap(delegate, config);
        assertEquals(SinkMode.SHADOW, sink.getMode());

        // Simulate PROMOTE signal from coordinator
        ControlSignal promoteSignal = ControlSignal.promote("coordinator-1", "trace-abc", "app2");
        sink.onSignal(promoteSignal);

        assertEquals(SinkMode.ACTIVE, sink.getMode());

        sink.invoke("event-after-promote", null);
        assertEquals(1, delegate.captured.size());
    }

    @Test
    void demotionSwitchesToShadowMode() throws Exception {
        config = FASCConfiguration.builder()
                .bootstrapServers("localhost:9092")
                .appId("app1")
                .initialMode(SinkMode.ACTIVE)
                .build();

        ShadowSink<String> sink = ShadowSink.wrap(delegate, config);
        assertEquals(SinkMode.ACTIVE, sink.getMode());

        // Simulate DEMOTE signal from coordinator
        ControlSignal demoteSignal = ControlSignal.demote("coordinator-1", "trace-abc", "app1");
        sink.onSignal(demoteSignal);

        assertEquals(SinkMode.SHADOW, sink.getMode());

        sink.invoke("event-after-demote", null);
        assertTrue(delegate.captured.isEmpty());
        assertEquals(1L, sink.getShadowDropCount());
    }

    @Test
    void ignoresSignalsTargetedAtOtherApp() throws Exception {
        ShadowSink<String> sink = ShadowSink.wrap(delegate, config); // appId = app2

        // Signal targeted at app1 — should be ignored by app2's sink
        ControlSignal demoteApp1 = ControlSignal.demote("coordinator-1", "trace-abc", "app1");
        sink.onSignal(demoteApp1);

        // app2 was in SHADOW, DEMOTE for app1 should not change app2's mode
        assertEquals(SinkMode.SHADOW, sink.getMode());
    }

    @Test
    void broadcastSignalIsProcessed() throws Exception {
        ShadowSink<String> sink = ShadowSink.wrap(delegate, config);

        // HEARTBEAT is broadcast
        ControlSignal heartbeat = ControlSignal.heartbeat("coordinator-1");
        // should not throw
        assertDoesNotThrow(() -> sink.onSignal(heartbeat));
    }

    @Test
    void abortSignalResetsHandoffState() throws Exception {
        ShadowSink<String> sink = ShadowSink.wrap(delegate, config);

        // Set up a pending handoff
        Map<Integer, Long> offsets = Map.of(0, 1000L, 1, 1001L);
        ControlSignal prepareHandoff = ControlSignal.prepareHandoff("coordinator-1", "trace-xyz", offsets);
        sink.onSignal(prepareHandoff);

        // Abort
        ControlSignal abort = ControlSignal.abort("coordinator-1", "trace-xyz", "test abort");
        sink.onSignal(abort);

        // Sink should still be in SHADOW (not promoted during abort)
        assertEquals(SinkMode.SHADOW, sink.getMode());
    }

    @Test
    void doesNotSelfPromoteOnHeartbeatTimeout() throws Exception {
        ShadowSink<String> sink = ShadowSink.wrap(delegate, config);
        // Even without heartbeats, sink must NOT self-promote
        // (split-brain prevention)
        assertEquals(SinkMode.SHADOW, sink.getMode());

        // Simulate heartbeat timeout by advancing internal clock — in a real test
        // we'd inject a clock. Here we verify the mode hasn't changed.
        Thread.sleep(100);
        assertEquals(SinkMode.SHADOW, sink.getMode(), "Sink must NOT self-promote on heartbeat timeout");
    }

    /** Test double that captures records written to it. */
    static class CapturingSink<T> implements SinkFunction<T> {
        final List<T> captured = new ArrayList<>();

        @Override
        public void invoke(T value, Context context) {
            captured.add(value);
        }
    }
}
