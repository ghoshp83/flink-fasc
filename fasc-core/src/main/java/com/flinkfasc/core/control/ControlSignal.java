package com.flinkfasc.core.control;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a single message on the FASC control Kafka topic.
 *
 * <p>Instances are created exclusively via the static factory methods — one per
 * {@link SignalType} — to ensure every message carries the mandatory fields for
 * that signal type and to keep call-sites readable.
 *
 * <p>The class is a standard Jackson-serialisable POJO. Unknown JSON properties
 * are silently ignored ({@code @JsonIgnoreProperties(ignoreUnknown = true)}) so
 * that older consumers remain compatible with newer coordinator versions that may
 * add extra fields.
 *
 * <p>Null-valued optional fields (e.g. {@code cutoverOffsets}, {@code reason}) are
 * omitted from the serialised JSON ({@code @JsonInclude(NON_NULL)}) to keep
 * messages compact.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ControlSignal implements Serializable {

    private static final long serialVersionUID = 1L;

    // -------------------------------------------------------------------------
    // Well-known target identifiers
    // -------------------------------------------------------------------------

    /** Target app ID used when a signal must be delivered to all applications. */
    public static final String BROADCAST = "broadcast";

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    /** The type of this signal. Never null. */
    @JsonProperty("signalType")
    private SignalType signalType;

    /**
     * The intended recipient.  Either a specific {@code appId} (e.g. {@code "app1"},
     * {@code "app2"}) or the constant {@link #BROADCAST} for signals that all apps
     * must process.
     */
    @JsonProperty("targetAppId")
    private String targetAppId;

    /** The identity of the sender — either the coordinator ID or an app ID. */
    @JsonProperty("sourceAppId")
    private String sourceAppId;

    /**
     * Per-partition cutover offsets (Kafka partition → offset).
     * Present only in {@link SignalType#PREPARE_HANDOFF} and {@link SignalType#DRAINED_AT}.
     */
    @JsonProperty("cutoverOffsets")
    private Map<Integer, Long> cutoverOffsets;

    /** Stable identifier for the coordinator instance that sent this signal. */
    @JsonProperty("coordinatorId")
    private String coordinatorId;

    /** Wall-clock time when the signal was created (epoch milliseconds). */
    @JsonProperty("timestamp")
    private long timestamp;

    /**
     * UUID assigned when the handoff trace begins. Propagated on every signal
     * belonging to the same handoff cycle so that logs can be correlated end-to-end.
     * Null for standalone signals such as {@link SignalType#HEARTBEAT}.
     */
    @JsonProperty("traceId")
    private String traceId;

    /**
     * Human-readable reason string. Present only in {@link SignalType#HANDOFF_ABORT}
     * to explain why the handoff was cancelled.
     */
    @JsonProperty("reason")
    private String reason;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** No-arg constructor required by Jackson. */
    @JsonCreator
    public ControlSignal() {}

    /** Full constructor — prefer the static factory methods instead. */
    public ControlSignal(
            SignalType signalType,
            String targetAppId,
            String sourceAppId,
            Map<Integer, Long> cutoverOffsets,
            String coordinatorId,
            long timestamp,
            String traceId,
            String reason) {
        this.signalType      = Objects.requireNonNull(signalType, "signalType");
        this.targetAppId     = targetAppId;
        this.sourceAppId     = sourceAppId;
        this.cutoverOffsets  = cutoverOffsets;
        this.coordinatorId   = coordinatorId;
        this.timestamp       = timestamp;
        this.traceId         = traceId;
        this.reason          = reason;
    }

    // -------------------------------------------------------------------------
    // Static factory methods
    // -------------------------------------------------------------------------

    /**
     * Creates a {@link SignalType#PREPARE_HANDOFF} broadcast signal.
     *
     * <p>This is the first signal in a handoff cycle. Both apps receive it and
     * start tracking whether they have processed all records up to
     * {@code cutoverOffsets}.
     *
     * @param coordinatorId stable ID of the coordinator sending this signal
     * @param traceId       UUID that will be attached to every subsequent signal
     *                      in this handoff cycle
     * @param offsets       per-partition Kafka offsets that define the cutover point
     * @return a new {@link ControlSignal}
     */
    public static ControlSignal prepareHandoff(
            String coordinatorId,
            String traceId,
            Map<Integer, Long> offsets) {
        return new ControlSignal(
                SignalType.PREPARE_HANDOFF,
                BROADCAST,
                coordinatorId,
                new HashMap<>(Objects.requireNonNull(offsets, "offsets")),
                coordinatorId,
                System.currentTimeMillis(),
                traceId,
                null);
    }

    /**
     * Creates a {@link SignalType#PROMOTE} signal directed at a specific app.
     *
     * <p>Upon receipt the target app transitions its {@link com.flinkfasc.core.ShadowSink}
     * from {@code SHADOW} to {@code ACTIVE} and immediately starts writing to DynamoDB.
     *
     * @param coordinatorId stable ID of the coordinator
     * @param traceId       handoff trace ID
     * @param targetAppId   the app that should become ACTIVE (typically {@code "app2"})
     * @return a new {@link ControlSignal}
     */
    public static ControlSignal promote(
            String coordinatorId,
            String traceId,
            String targetAppId) {
        return new ControlSignal(
                SignalType.PROMOTE,
                targetAppId,
                coordinatorId,
                null,
                coordinatorId,
                System.currentTimeMillis(),
                traceId,
                null);
    }

    /**
     * Creates a {@link SignalType#DEMOTE} signal directed at a specific app.
     *
     * <p>Upon receipt the target app transitions its {@link com.flinkfasc.core.ShadowSink}
     * from {@code ACTIVE} to {@code SHADOW}. The coordinator must send this only
     * <em>after</em> receiving {@link SignalType#PROMOTED_CONFIRMED} from the
     * newly-active app.
     *
     * @param coordinatorId stable ID of the coordinator
     * @param traceId       handoff trace ID
     * @param targetAppId   the app that should become SHADOW (typically {@code "app1"})
     * @return a new {@link ControlSignal}
     */
    public static ControlSignal demote(
            String coordinatorId,
            String traceId,
            String targetAppId) {
        return new ControlSignal(
                SignalType.DEMOTE,
                targetAppId,
                coordinatorId,
                null,
                coordinatorId,
                System.currentTimeMillis(),
                traceId,
                null);
    }

    /**
     * Creates a {@link SignalType#DRAINED_AT} acknowledgement from an app to the coordinator.
     *
     * <p>Sent by app1 after it has processed all Kafka records whose offsets are
     * {@code ≤ cutoverOffsets}. The actual offsets reached are included so the
     * coordinator can verify they meet or exceed the target set in
     * {@link SignalType#PREPARE_HANDOFF}.
     *
     * @param appId   the application sending the acknowledgement (e.g. {@code "app1"})
     * @param traceId handoff trace ID
     * @param offsets actual per-partition offsets that have been processed
     * @return a new {@link ControlSignal}
     */
    public static ControlSignal drainedAt(
            String appId,
            String traceId,
            Map<Integer, Long> offsets) {
        return new ControlSignal(
                SignalType.DRAINED_AT,
                BROADCAST,            // coordinator is a broadcast listener
                appId,
                new HashMap<>(Objects.requireNonNull(offsets, "offsets")),
                null,
                System.currentTimeMillis(),
                traceId,
                null);
    }

    /**
     * Creates a {@link SignalType#PROMOTED_CONFIRMED} acknowledgement from an app.
     *
     * <p>Sent by app2 once its {@link com.flinkfasc.core.ShadowSink} has successfully
     * transitioned to {@code ACTIVE} and the first write to DynamoDB has been attempted.
     * On receipt the coordinator sends {@link SignalType#DEMOTE} to app1.
     *
     * @param appId   the application confirming its promotion (e.g. {@code "app2"})
     * @param traceId handoff trace ID
     * @return a new {@link ControlSignal}
     */
    public static ControlSignal promotedConfirmed(String appId, String traceId) {
        return new ControlSignal(
                SignalType.PROMOTED_CONFIRMED,
                BROADCAST,
                appId,
                null,
                null,
                System.currentTimeMillis(),
                traceId,
                null);
    }

    /**
     * Creates a {@link SignalType#HEARTBEAT} signal broadcast to all apps.
     *
     * <p>Heartbeats serve as liveness pings. Apps update their
     * {@code lastHeartbeatMs} timestamp on receipt. If the interval between
     * heartbeats exceeds
     * {@link com.flinkfasc.core.FASCConfiguration#getHeartbeatTimeoutMs()}, the app
     * logs an error but does NOT self-promote.
     *
     * @param coordinatorId stable ID of the coordinator
     * @return a new {@link ControlSignal}
     */
    public static ControlSignal heartbeat(String coordinatorId) {
        return new ControlSignal(
                SignalType.HEARTBEAT,
                BROADCAST,
                coordinatorId,
                null,
                coordinatorId,
                System.currentTimeMillis(),
                null,
                null);
    }

    /**
     * Creates a {@link SignalType#HANDOFF_ABORT} broadcast signal.
     *
     * <p>Both apps revert all pending handoff state on receipt (clear stored
     * cutover offsets, cancel any drain-completion timers).
     *
     * @param coordinatorId stable ID of the coordinator
     * @param traceId       handoff trace ID of the aborted cycle
     * @param reason        human-readable explanation of why the handoff was aborted
     * @return a new {@link ControlSignal}
     */
    public static ControlSignal abort(
            String coordinatorId,
            String traceId,
            String reason) {
        return new ControlSignal(
                SignalType.HANDOFF_ABORT,
                BROADCAST,
                coordinatorId,
                null,
                coordinatorId,
                System.currentTimeMillis(),
                traceId,
                reason);
    }

    // -------------------------------------------------------------------------
    // Getters and setters
    // -------------------------------------------------------------------------

    /** @return the signal type */
    public SignalType getSignalType() { return signalType; }

    /** @param signalType the signal type */
    public void setSignalType(SignalType signalType) { this.signalType = signalType; }

    /** @return the target app ID or {@link #BROADCAST} */
    public String getTargetAppId() { return targetAppId; }

    /** @param targetAppId the target app ID */
    public void setTargetAppId(String targetAppId) { this.targetAppId = targetAppId; }

    /** @return the source app or coordinator ID */
    public String getSourceAppId() { return sourceAppId; }

    /** @param sourceAppId the source app or coordinator ID */
    public void setSourceAppId(String sourceAppId) { this.sourceAppId = sourceAppId; }

    /** @return per-partition cutover offsets, or {@code null} if not applicable */
    public Map<Integer, Long> getCutoverOffsets() { return cutoverOffsets; }

    /** @param cutoverOffsets per-partition cutover offsets */
    public void setCutoverOffsets(Map<Integer, Long> cutoverOffsets) {
        this.cutoverOffsets = cutoverOffsets;
    }

    /** @return the coordinator instance identifier */
    public String getCoordinatorId() { return coordinatorId; }

    /** @param coordinatorId the coordinator instance identifier */
    public void setCoordinatorId(String coordinatorId) { this.coordinatorId = coordinatorId; }

    /** @return signal creation timestamp (epoch milliseconds) */
    public long getTimestamp() { return timestamp; }

    /** @param timestamp signal creation timestamp (epoch milliseconds) */
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    /** @return the end-to-end handoff trace ID, or {@code null} */
    public String getTraceId() { return traceId; }

    /** @param traceId the end-to-end handoff trace ID */
    public void setTraceId(String traceId) { this.traceId = traceId; }

    /** @return the abort reason string, or {@code null} if not applicable */
    public String getReason() { return reason; }

    /** @param reason the abort reason string */
    public void setReason(String reason) { this.reason = reason; }

    // -------------------------------------------------------------------------
    // Object overrides
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return "ControlSignal{"
                + "signalType=" + signalType
                + ", targetAppId='" + targetAppId + '\''
                + ", sourceAppId='" + sourceAppId + '\''
                + ", coordinatorId='" + coordinatorId + '\''
                + ", traceId='" + traceId + '\''
                + ", timestamp=" + timestamp
                + (cutoverOffsets != null ? ", cutoverOffsets=" + cutoverOffsets : "")
                + (reason != null ? ", reason='" + reason + '\'' : "")
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ControlSignal)) return false;
        ControlSignal that = (ControlSignal) o;
        return timestamp == that.timestamp
                && signalType == that.signalType
                && Objects.equals(targetAppId, that.targetAppId)
                && Objects.equals(sourceAppId, that.sourceAppId)
                && Objects.equals(traceId, that.traceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(signalType, targetAppId, sourceAppId, traceId, timestamp);
    }
}
