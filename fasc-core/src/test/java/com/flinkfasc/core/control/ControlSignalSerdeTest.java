package com.flinkfasc.core.control;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies ControlSignal serialization round-trips correctly.
 */
class ControlSignalSerdeTest {

    private final ControlSignalSerde serde = new ControlSignalSerde();

    @Test
    void roundTrip_prepareHandoff() {
        Map<Integer, Long> offsets = Map.of(0, 5000L, 1, 4998L, 23, 5100L);
        ControlSignal original = ControlSignal.prepareHandoff("coordinator-1", "trace-001", offsets);

        byte[] bytes = serde.serialize(original);
        ControlSignal deserialized = serde.deserialize(bytes);

        assertEquals(SignalType.PREPARE_HANDOFF, deserialized.getSignalType());
        assertEquals("broadcast", deserialized.getTargetAppId());
        assertEquals("coordinator-1", deserialized.getCoordinatorId());
        assertEquals("trace-001", deserialized.getTraceId());
        assertEquals(5000L, deserialized.getCutoverOffsets().get(0));
        assertEquals(5100L, deserialized.getCutoverOffsets().get(23));
    }

    @Test
    void roundTrip_promote() {
        ControlSignal original = ControlSignal.promote("coordinator-1", "trace-002", "app2");

        byte[] bytes = serde.serialize(original);
        ControlSignal deserialized = serde.deserialize(bytes);

        assertEquals(SignalType.PROMOTE, deserialized.getSignalType());
        assertEquals("app2", deserialized.getTargetAppId());
    }

    @Test
    void roundTrip_drainedAt() {
        Map<Integer, Long> offsets = Map.of(0, 6000L, 1, 5999L);
        ControlSignal original = ControlSignal.drainedAt("app1", "trace-003", offsets);

        byte[] bytes = serde.serialize(original);
        ControlSignal deserialized = serde.deserialize(bytes);

        assertEquals(SignalType.DRAINED_AT, deserialized.getSignalType());
        assertEquals("app1", deserialized.getSourceAppId());
        assertEquals(6000L, deserialized.getCutoverOffsets().get(0));
    }

    @Test
    void roundTrip_heartbeat() {
        ControlSignal original = ControlSignal.heartbeat("coordinator-1");

        byte[] bytes = serde.serialize(original);
        ControlSignal deserialized = serde.deserialize(bytes);

        assertEquals(SignalType.HEARTBEAT, deserialized.getSignalType());
        assertEquals("broadcast", deserialized.getTargetAppId());
    }

    @Test
    void roundTrip_abort() {
        ControlSignal original = ControlSignal.abort("coordinator-1", "trace-004", "drain timeout");

        byte[] bytes = serde.serialize(original);
        ControlSignal deserialized = serde.deserialize(bytes);

        assertEquals(SignalType.HANDOFF_ABORT, deserialized.getSignalType());
        assertEquals("drain timeout", deserialized.getReason());
    }

    @Test
    void deserializeInvalidBytes_throwsRuntimeException() {
        byte[] garbage = "not-json".getBytes();
        assertThrows(RuntimeException.class, () -> serde.deserialize(garbage));
    }

    @Test
    void serializedBytesAreNonEmpty() {
        ControlSignal signal = ControlSignal.heartbeat("coordinator-1");
        byte[] bytes = serde.serialize(signal);
        assertTrue(bytes.length > 0);
    }
}
