package com.flinkfasc.coordinator.savepoint;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable snapshot of all metadata associated with a FASC-triggered Flink savepoint.
 *
 * <p>An instance of this class is persisted to DynamoDB immediately after a savepoint
 * completes, so that if the coordinator crashes mid-handoff the new leader can resume
 * from the last-known savepoint rather than triggering a fresh one.
 *
 * <p>The {@code kafkaOffsets} map records the per-partition committed Kafka offsets
 * <em>at the time the savepoint was taken</em>.  These offsets are used as the
 * {@link com.flinkfasc.coordinator.handoff.HandoffOrchestrator} agreed cutover offsets
 * — App2 must reach these offsets before it is safe to drain App1.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SavepointMetadata {

    /** Internal FASC-generated identifier for this savepoint record (UUID). */
    @JsonProperty("savepointId")
    private final String savepointId;

    /** Fully-qualified S3 URI of the savepoint directory (e.g. {@code s3://bucket/prefix/...}). */
    @JsonProperty("s3Location")
    private final String s3Location;

    /**
     * Per-partition Kafka offsets committed by the source job at savepoint time.
     * Key = partition index (0–23 for a 24-partition topic).
     * Value = last committed offset for that partition.
     */
    @JsonProperty("kafkaOffsets")
    private final Map<Integer, Long> kafkaOffsets;

    /** Job ID of the application whose savepoint was taken (typically App1). */
    @JsonProperty("sourceJobId")
    private final String sourceJobId;

    /**
     * Job ID of the application that will be bootstrapped from this savepoint
     * (typically App2, populated after the handoff is complete).
     */
    @JsonProperty("targetJobId")
    private final String targetJobId;

    /** Unix epoch milliseconds when the savepoint trigger was submitted. */
    @JsonProperty("takenAtMs")
    private final long takenAtMs;

    /** Current lifecycle status of this savepoint. */
    @JsonProperty("status")
    private final SavepointStatus status;

    /**
     * All-args constructor used by Jackson deserialisation and internal factory methods.
     *
     * @param savepointId unique identifier for this savepoint record
     * @param s3Location  S3 URI of the completed savepoint
     * @param kafkaOffsets per-partition committed Kafka offsets at savepoint time
     * @param sourceJobId job ID of the source (snapshotted) application
     * @param targetJobId job ID of the application bootstrapping from this savepoint
     * @param takenAtMs   epoch millis when the savepoint was triggered
     * @param status      current lifecycle status
     */
    @JsonCreator
    public SavepointMetadata(
            @JsonProperty("savepointId")  String savepointId,
            @JsonProperty("s3Location")   String s3Location,
            @JsonProperty("kafkaOffsets") Map<Integer, Long> kafkaOffsets,
            @JsonProperty("sourceJobId")  String sourceJobId,
            @JsonProperty("targetJobId")  String targetJobId,
            @JsonProperty("takenAtMs")    long takenAtMs,
            @JsonProperty("status")       SavepointStatus status) {
        this.savepointId  = savepointId;
        this.s3Location   = s3Location;
        this.kafkaOffsets = kafkaOffsets != null
                ? Collections.unmodifiableMap(new HashMap<>(kafkaOffsets))
                : Collections.emptyMap();
        this.sourceJobId  = sourceJobId;
        this.targetJobId  = targetJobId;
        this.takenAtMs    = takenAtMs;
        this.status       = status;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    /**
     * Returns a new {@link Builder} for constructing {@code SavepointMetadata} instances.
     *
     * @return a fresh builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a copy of this instance with the given {@link SavepointStatus}.
     *
     * @param newStatus the updated status
     * @return a new {@code SavepointMetadata} with the status replaced
     */
    public SavepointMetadata withStatus(SavepointStatus newStatus) {
        return new SavepointMetadata(savepointId, s3Location, kafkaOffsets,
                sourceJobId, targetJobId, takenAtMs, newStatus);
    }

    /**
     * Returns a copy of this instance with the given S3 location and COMPLETED status.
     *
     * @param s3Location the resolved S3 savepoint URI
     * @return a new completed {@code SavepointMetadata}
     */
    public SavepointMetadata withS3LocationCompleted(String s3Location) {
        return new SavepointMetadata(savepointId, s3Location, kafkaOffsets,
                sourceJobId, targetJobId, takenAtMs, SavepointStatus.COMPLETED);
    }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    /** @return internal FASC savepoint identifier */
    public String getSavepointId()  { return savepointId; }

    /** @return fully-qualified S3 URI of the savepoint directory */
    public String getS3Location()   { return s3Location; }

    /** @return unmodifiable map of partition → committed Kafka offset at savepoint time */
    public Map<Integer, Long> getKafkaOffsets() { return kafkaOffsets; }

    /** @return Flink job ID of the source application */
    public String getSourceJobId()  { return sourceJobId; }

    /** @return Flink job ID of the target application */
    public String getTargetJobId()  { return targetJobId; }

    /** @return epoch millis when the savepoint trigger was submitted */
    public long getTakenAtMs()      { return takenAtMs; }

    /** @return current lifecycle status */
    public SavepointStatus getStatus() { return status; }

    // -------------------------------------------------------------------------
    // Object overrides
    // -------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SavepointMetadata)) return false;
        SavepointMetadata that = (SavepointMetadata) o;
        return takenAtMs == that.takenAtMs
                && Objects.equals(savepointId, that.savepointId)
                && Objects.equals(s3Location, that.s3Location)
                && Objects.equals(sourceJobId, that.sourceJobId)
                && Objects.equals(targetJobId, that.targetJobId)
                && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(savepointId, s3Location, sourceJobId, targetJobId, takenAtMs, status);
    }

    @Override
    public String toString() {
        return "SavepointMetadata{"
                + "savepointId='" + savepointId + '\''
                + ", s3Location='" + s3Location + '\''
                + ", sourceJobId='" + sourceJobId + '\''
                + ", targetJobId='" + targetJobId + '\''
                + ", takenAtMs=" + takenAtMs
                + ", status=" + status
                + ", partitions=" + kafkaOffsets.size()
                + '}';
    }

    // =========================================================================
    // Builder
    // =========================================================================

    /**
     * Fluent builder for {@link SavepointMetadata}.
     */
    public static final class Builder {

        private String savepointId;
        private String s3Location;
        private Map<Integer, Long> kafkaOffsets = new HashMap<>();
        private String sourceJobId;
        private String targetJobId;
        private long takenAtMs = System.currentTimeMillis();
        private SavepointStatus status = SavepointStatus.IN_PROGRESS;

        private Builder() {}

        /** @param savepointId unique FASC savepoint identifier; returns this builder */
        public Builder savepointId(String savepointId)   { this.savepointId = savepointId; return this; }
        /** @param s3Location S3 URI of the savepoint directory; returns this builder */
        public Builder s3Location(String s3Location)     { this.s3Location = s3Location; return this; }
        /** @param kafkaOffsets per-partition committed offsets; returns this builder */
        public Builder kafkaOffsets(Map<Integer, Long> kafkaOffsets) { this.kafkaOffsets = kafkaOffsets; return this; }
        /** @param sourceJobId Flink job ID of the source app; returns this builder */
        public Builder sourceJobId(String sourceJobId)   { this.sourceJobId = sourceJobId; return this; }
        /** @param targetJobId Flink job ID of the target app; returns this builder */
        public Builder targetJobId(String targetJobId)   { this.targetJobId = targetJobId; return this; }
        /** @param takenAtMs epoch millis when the savepoint was triggered; returns this builder */
        public Builder takenAtMs(long takenAtMs)         { this.takenAtMs = takenAtMs; return this; }
        /** @param status initial lifecycle status; returns this builder */
        public Builder status(SavepointStatus status)    { this.status = status; return this; }

        /**
         * Builds and returns the {@link SavepointMetadata} instance.
         *
         * @return a new immutable {@code SavepointMetadata}
         */
        public SavepointMetadata build() {
            return new SavepointMetadata(savepointId, s3Location, kafkaOffsets,
                    sourceJobId, targetJobId, takenAtMs, status);
        }
    }
}
