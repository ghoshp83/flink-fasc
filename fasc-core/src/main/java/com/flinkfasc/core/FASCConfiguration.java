package com.flinkfasc.core;

import com.flinkfasc.core.sink.SinkMode;

import java.io.Serializable;
import java.util.Objects;

/**
 * Immutable configuration object for the Flink Application State Coordinator (FASC) library.
 *
 * <p>Use the nested {@link Builder} to construct instances programmatically, or call
 * {@link #fromEnv()} to read configuration from environment variables — the preferred
 * approach in containerised deployments on EKS / ECS.
 *
 * <p>All instances are {@link Serializable} so that they can be embedded inside Flink
 * operators and shipped to task-managers without extra ceremony.
 *
 * <h3>Minimum required fields</h3>
 * <ul>
 *   <li>{@code bootstrapServers} — MSK (or any Kafka) bootstrap broker list</li>
 *   <li>{@code appId} — unique identifier for this application instance ({@code "app1"} or
 *       {@code "app2"})</li>
 * </ul>
 */
public final class FASCConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    // -------------------------------------------------------------------------
    // Environment variable names
    // -------------------------------------------------------------------------

    /** Environment variable key for the Kafka bootstrap servers. */
    public static final String ENV_BOOTSTRAP_SERVERS = "FASC_BOOTSTRAP_SERVERS";

    /** Environment variable key for the application identifier. */
    public static final String ENV_APP_ID = "FASC_APP_ID";

    /** Environment variable key for the control topic name. */
    public static final String ENV_CONTROL_TOPIC = "FASC_CONTROL_TOPIC";

    /** Environment variable key for the initial sink mode ("ACTIVE" or "SHADOW"). */
    public static final String ENV_INITIAL_MODE = "FASC_INITIAL_MODE";

    // -------------------------------------------------------------------------
    // Defaults
    // -------------------------------------------------------------------------

    /** Default Kafka topic used by FASC for coordinator ↔ app signalling. */
    public static final String DEFAULT_CONTROL_TOPIC = "fasc-control-topic";

    /** Default heartbeat timeout — if no heartbeat arrives within this window the
     *  app logs an error but does NOT self-promote (split-brain prevention). */
    public static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 30_000L;

    /** Default interval at which the coordinator emits heartbeat signals. */
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000L;

    /** Default sink mode — new apps start in shadow so state can warm up. */
    public static final SinkMode DEFAULT_INITIAL_MODE = SinkMode.SHADOW;

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    /** Comma-separated list of Kafka bootstrap broker addresses. Required. */
    private final String bootstrapServers;

    /** Kafka topic used for FASC control messages. */
    private final String controlTopic;

    /**
     * Unique identifier for this Flink application instance.
     * Convention: {@code "app1"} for the primary, {@code "app2"} for the standby.
     * Required.
     */
    private final String appId;

    /**
     * How long (ms) to wait for a heartbeat before logging an error.
     * The app does NOT self-promote on timeout — the coordinator must remain the
     * sole authority that drives mode transitions.
     */
    private final long heartbeatTimeoutMs;

    /** How often (ms) the coordinator sends heartbeat signals. */
    private final long heartbeatIntervalMs;

    /** The mode the {@link ShadowSink} starts in on first open. */
    private final SinkMode initialMode;

    /**
     * Kafka consumer group ID for the control topic subscriber.
     * Defaults to {@code "fasc-control-consumer-<appId>"}.
     */
    private final String consumerGroupId;

    // -------------------------------------------------------------------------
    // Private constructor — use Builder or fromEnv()
    // -------------------------------------------------------------------------

    private FASCConfiguration(Builder b) {
        this.bootstrapServers   = b.bootstrapServers;
        this.controlTopic       = b.controlTopic;
        this.appId              = b.appId;
        this.heartbeatTimeoutMs = b.heartbeatTimeoutMs;
        this.heartbeatIntervalMs= b.heartbeatIntervalMs;
        this.initialMode        = b.initialMode;
        this.consumerGroupId    = b.consumerGroupId != null
                ? b.consumerGroupId
                : "fasc-control-consumer-" + b.appId;
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /**
     * Returns a new {@link Builder} for fluent construction.
     *
     * @return a fresh builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Constructs a {@link FASCConfiguration} by reading well-known environment variables.
     *
     * <table>
     *   <tr><th>Variable</th><th>Required</th><th>Default</th></tr>
     *   <tr><td>FASC_BOOTSTRAP_SERVERS</td><td>yes</td><td>—</td></tr>
     *   <tr><td>FASC_APP_ID</td><td>yes</td><td>—</td></tr>
     *   <tr><td>FASC_CONTROL_TOPIC</td><td>no</td><td>fasc-control-topic</td></tr>
     *   <tr><td>FASC_INITIAL_MODE</td><td>no</td><td>SHADOW</td></tr>
     * </table>
     *
     * @return a validated {@link FASCConfiguration} instance
     * @throws IllegalArgumentException if a required variable is absent or invalid
     */
    public static FASCConfiguration fromEnv() {
        Builder b = new Builder();

        String servers = System.getenv(ENV_BOOTSTRAP_SERVERS);
        if (servers != null && !servers.isEmpty()) {
            b.bootstrapServers(servers);
        }

        String appId = System.getenv(ENV_APP_ID);
        if (appId != null && !appId.isEmpty()) {
            b.appId(appId);
        }

        String topic = System.getenv(ENV_CONTROL_TOPIC);
        if (topic != null && !topic.isEmpty()) {
            b.controlTopic(topic);
        }

        String mode = System.getenv(ENV_INITIAL_MODE);
        if (mode != null && !mode.isEmpty()) {
            b.initialMode(SinkMode.valueOf(mode.toUpperCase()));
        }

        FASCConfiguration cfg = b.build();
        cfg.validate();
        return cfg;
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    /**
     * Validates that all required fields are present and that values are in range.
     *
     * @throws IllegalArgumentException if any required field is missing or invalid
     */
    public void validate() {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException(
                    "FASCConfiguration: bootstrapServers is required. "
                    + "Set it via Builder.bootstrapServers() or env var " + ENV_BOOTSTRAP_SERVERS);
        }
        if (appId == null || appId.isBlank()) {
            throw new IllegalArgumentException(
                    "FASCConfiguration: appId is required. "
                    + "Set it via Builder.appId() or env var " + ENV_APP_ID);
        }
        if (heartbeatTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "FASCConfiguration: heartbeatTimeoutMs must be > 0, got " + heartbeatTimeoutMs);
        }
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "FASCConfiguration: heartbeatIntervalMs must be > 0, got " + heartbeatIntervalMs);
        }
        if (heartbeatIntervalMs >= heartbeatTimeoutMs) {
            throw new IllegalArgumentException(
                    "FASCConfiguration: heartbeatIntervalMs (" + heartbeatIntervalMs
                    + ") must be less than heartbeatTimeoutMs (" + heartbeatTimeoutMs + ")");
        }
        if (initialMode == null) {
            throw new IllegalArgumentException(
                    "FASCConfiguration: initialMode must not be null");
        }
    }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    /** @return comma-separated Kafka bootstrap broker list */
    public String getBootstrapServers() { return bootstrapServers; }

    /** @return the Kafka topic name for FASC control messages */
    public String getControlTopic()     { return controlTopic; }

    /** @return this application's unique identifier */
    public String getAppId()            { return appId; }

    /** @return heartbeat timeout in milliseconds */
    public long getHeartbeatTimeoutMs() { return heartbeatTimeoutMs; }

    /** @return heartbeat interval in milliseconds */
    public long getHeartbeatIntervalMs(){ return heartbeatIntervalMs; }

    /** @return the mode the ShadowSink starts in */
    public SinkMode getInitialMode()    { return initialMode; }

    /** @return the Kafka consumer group ID for the control topic */
    public String getConsumerGroupId()  { return consumerGroupId; }

    // -------------------------------------------------------------------------
    // Object overrides
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return "FASCConfiguration{"
                + "appId='" + appId + '\''
                + ", controlTopic='" + controlTopic + '\''
                + ", bootstrapServers='" + bootstrapServers + '\''
                + ", initialMode=" + initialMode
                + ", heartbeatIntervalMs=" + heartbeatIntervalMs
                + ", heartbeatTimeoutMs=" + heartbeatTimeoutMs
                + ", consumerGroupId='" + consumerGroupId + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FASCConfiguration)) return false;
        FASCConfiguration that = (FASCConfiguration) o;
        return heartbeatTimeoutMs == that.heartbeatTimeoutMs
                && heartbeatIntervalMs == that.heartbeatIntervalMs
                && Objects.equals(bootstrapServers, that.bootstrapServers)
                && Objects.equals(controlTopic, that.controlTopic)
                && Objects.equals(appId, that.appId)
                && initialMode == that.initialMode
                && Objects.equals(consumerGroupId, that.consumerGroupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bootstrapServers, controlTopic, appId,
                heartbeatTimeoutMs, heartbeatIntervalMs, initialMode, consumerGroupId);
    }

    // =========================================================================
    // Builder
    // =========================================================================

    /**
     * Fluent builder for {@link FASCConfiguration}.
     */
    public static final class Builder {

        private String   bootstrapServers;
        private String   controlTopic       = DEFAULT_CONTROL_TOPIC;
        private String   appId;
        private long     heartbeatTimeoutMs = DEFAULT_HEARTBEAT_TIMEOUT_MS;
        private long     heartbeatIntervalMs= DEFAULT_HEARTBEAT_INTERVAL_MS;
        private SinkMode initialMode        = DEFAULT_INITIAL_MODE;
        private String   consumerGroupId;

        private Builder() {}

        /**
         * Sets the Kafka bootstrap broker list. Required.
         *
         * @param bootstrapServers comma-separated list, e.g.
         *        {@code "b-1.msk.example.com:9092,b-2.msk.example.com:9092"}
         * @return this builder
         */
        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
            return this;
        }

        /**
         * Sets the Kafka control topic name.
         *
         * @param controlTopic topic name (default: {@value FASCConfiguration#DEFAULT_CONTROL_TOPIC})
         * @return this builder
         */
        public Builder controlTopic(String controlTopic) {
            this.controlTopic = Objects.requireNonNull(controlTopic, "controlTopic");
            return this;
        }

        /**
         * Sets the application identifier. Required. Convention: {@code "app1"} / {@code "app2"}.
         *
         * @param appId application identifier
         * @return this builder
         */
        public Builder appId(String appId) {
            this.appId = Objects.requireNonNull(appId, "appId");
            return this;
        }

        /**
         * Sets the heartbeat timeout. If no heartbeat arrives within this window the sink
         * logs an error but does NOT self-promote.
         *
         * @param heartbeatTimeoutMs timeout in milliseconds (default: 30 000)
         * @return this builder
         */
        public Builder heartbeatTimeoutMs(long heartbeatTimeoutMs) {
            this.heartbeatTimeoutMs = heartbeatTimeoutMs;
            return this;
        }

        /**
         * Sets the heartbeat interval (informational — used only for validation).
         *
         * @param heartbeatIntervalMs interval in milliseconds (default: 5 000)
         * @return this builder
         */
        public Builder heartbeatIntervalMs(long heartbeatIntervalMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            return this;
        }

        /**
         * Sets the initial sink mode.
         *
         * @param initialMode {@link SinkMode#ACTIVE} or {@link SinkMode#SHADOW} (default: SHADOW)
         * @return this builder
         */
        public Builder initialMode(SinkMode initialMode) {
            this.initialMode = Objects.requireNonNull(initialMode, "initialMode");
            return this;
        }

        /**
         * Overrides the Kafka consumer group ID. If not set it defaults to
         * {@code "fasc-control-consumer-<appId>"}.
         *
         * @param consumerGroupId Kafka consumer group identifier
         * @return this builder
         */
        public Builder consumerGroupId(String consumerGroupId) {
            this.consumerGroupId = Objects.requireNonNull(consumerGroupId, "consumerGroupId");
            return this;
        }

        /**
         * Builds and returns the {@link FASCConfiguration}. Does NOT call {@link #validate()}
         * automatically — call it explicitly when needed (e.g. at application startup).
         *
         * @return a new immutable {@link FASCConfiguration}
         */
        public FASCConfiguration build() {
            return new FASCConfiguration(this);
        }
    }
}
