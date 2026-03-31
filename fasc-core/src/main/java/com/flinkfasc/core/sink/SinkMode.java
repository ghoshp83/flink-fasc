package com.flinkfasc.core.sink;

/**
 * Represents the operational mode of a {@link com.flinkfasc.core.ShadowSink}.
 *
 * <p>ACTIVE mode means the sink writes through to the downstream store (e.g. DynamoDB).
 * SHADOW mode means all incoming records are silently dropped — the application continues
 * processing state but produces no visible side-effects.
 *
 * <p>Only one of the two Flink applications (app1 / app2) should be ACTIVE at any given time.
 * Transitions between modes are driven by {@link com.flinkfasc.core.control.ControlSignal}
 * messages published to the FASC control Kafka topic by the external coordinator.
 */
public enum SinkMode {

    /**
     * The sink forwards every record to the delegate {@link org.apache.flink.streaming.api.functions.sink.SinkFunction}.
     * Exactly one app should be ACTIVE at any instant.
     */
    ACTIVE,

    /**
     * The sink swallows every record without forwarding it.
     * The application keeps building internal Flink state so it can be promoted to ACTIVE
     * with a consistent view of the world at the handoff offset.
     */
    SHADOW
}
