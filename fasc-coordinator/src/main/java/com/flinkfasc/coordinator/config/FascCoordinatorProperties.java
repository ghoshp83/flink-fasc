package com.flinkfasc.coordinator.config;

import com.flinkfasc.aws.config.FascAwsProperties;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Strongly-typed configuration properties for the FASC coordinator, bound from the
 * {@code fasc.*} namespace in {@code application.yml} (or environment variables).
 *
 * <p>All fields are mutable so that Spring Boot's property-binding infrastructure can
 * inject values without reflection hacks. Callers should treat the populated bean as
 * effectively immutable after the application context is refreshed.
 *
 * <p>Example {@code application.yml} snippet:
 * <pre>{@code
 * fasc:
 *   bootstrapServers: b-1.msk.example.com:9092
 *   app1JobId: abc123
 *   app2JobId: def456
 *   savepointS3Bucket: my-flink-savepoints
 * }</pre>
 */
@Validated
@ConfigurationProperties(prefix = "fasc")
public class FascCoordinatorProperties implements FascAwsProperties {

    // -------------------------------------------------------------------------
    // Kafka
    // -------------------------------------------------------------------------

    /**
     * Comma-separated list of Kafka (MSK) bootstrap broker addresses.
     * <p>Required. Example: {@code b-1.msk.us-east-1.amazonaws.com:9092,b-2...}
     */
    @NotBlank(message = "fasc.bootstrapServers is required")
    private String bootstrapServers;

    /**
     * Name of the Kafka topic used by FASC for coordinator ↔ app control signals.
     * Defaults to {@code fasc-control-topic}.
     */
    private String controlTopic = "fasc-control-topic";

    // -------------------------------------------------------------------------
    // Coordinator identity
    // -------------------------------------------------------------------------

    /**
     * Stable identifier for this coordinator instance.
     * In EKS deployments, set this to {@code ${HOSTNAME}} so each pod has a unique ID.
     * Defaults to {@code fasc-coordinator}.
     */
    private String coordinatorId = "fasc-coordinator";

    // -------------------------------------------------------------------------
    // Flink job identifiers
    // -------------------------------------------------------------------------

    /**
     * Flink job ID (32-character hex string) for App1 — the current active writer.
     * Required. Example: {@code 00000000000000000000000000000001}
     */
    @NotBlank(message = "fasc.app1JobId is required")
    private String app1JobId;

    /**
     * Flink job ID (32-character hex string) for App2 — the standby.
     * Required. Example: {@code 00000000000000000000000000000002}
     */
    @NotBlank(message = "fasc.app2JobId is required")
    private String app2JobId;

    // -------------------------------------------------------------------------
    // Flink REST endpoints
    // -------------------------------------------------------------------------

    /**
     * Base URL of the App1 Flink JobManager REST API.
     * Defaults to {@code http://app1-jobmanager:8081}.
     */
    private String app1FlinkRestUrl = "http://app1-jobmanager:8081";

    /**
     * Base URL of the App2 Flink JobManager REST API.
     * Defaults to {@code http://app2-jobmanager:8081}.
     */
    private String app2FlinkRestUrl = "http://app2-jobmanager:8081";

    // -------------------------------------------------------------------------
    // Kafka consumer groups
    // -------------------------------------------------------------------------

    /**
     * Name of the Kafka business topic that App1 and App2 both consume.
     * The offset monitor tracks consumer-group lag on this topic.
     * Defaults to {@code business-topic}.
     */
    private String businessTopic = "business-topic";

    /**
     * Kafka consumer group ID used by App1 to read from the source topic.
     * Used by the offset monitor to compute lag relative to App2.
     */
    private String app1ConsumerGroup;

    /**
     * Kafka consumer group ID used by App2 to read from the source topic.
     * Promotion is blocked until App2's committed offsets match App1's.
     */
    private String app2ConsumerGroup;

    // -------------------------------------------------------------------------
    // Offset monitoring
    // -------------------------------------------------------------------------

    /**
     * How often (in ms) the offset monitor polls Kafka AdminClient for consumer-group lag.
     * Defaults to {@code 5000} ms.
     */
    private long offsetMonitorIntervalMs = 5_000L;

    /**
     * Maximum per-partition lag (in messages) that App2 is allowed to have before the
     * coordinator considers it ready for promotion.
     * <p>A value of {@code 0} means App2 must have fully caught up.
     */
    private long lagReadinessThreshold = 0L;

    /**
     * Duration (in ms) for which App2's lag must remain at or below
     * {@link #lagReadinessThreshold} before the coordinator triggers the handoff.
     * Prevents spurious promotions caused by brief lag spikes. Defaults to {@code 10000} ms.
     */
    private long lagStabilityWindowMs = 10_000L;

    // -------------------------------------------------------------------------
    // Heartbeat
    // -------------------------------------------------------------------------

    /**
     * How often (in ms) the coordinator publishes HEARTBEAT signals to both apps.
     * Apps that do not receive a heartbeat within their configured timeout will log an error
     * but will NOT self-promote (split-brain prevention). Defaults to {@code 5000} ms.
     */
    private long heartbeatIntervalMs = 5_000L;

    // -------------------------------------------------------------------------
    // S3 / savepoints
    // -------------------------------------------------------------------------

    /**
     * S3 bucket where Flink savepoints are written.
     * Required. Example: {@code my-flink-savepoints}
     */
    @NotBlank(message = "fasc.savepointS3Bucket is required")
    private String savepointS3Bucket;

    /**
     * S3 key prefix under which savepoints are stored.
     * Defaults to {@code fasc/savepoints/}.
     */
    private String savepointS3Prefix = "fasc/savepoints/";

    // -------------------------------------------------------------------------
    // AWS
    // -------------------------------------------------------------------------

    /**
     * AWS region used for DynamoDB and S3 operations.
     * Defaults to {@code eu-west-2}.
     */
    private String awsRegion = "eu-west-2";

    // -------------------------------------------------------------------------
    // DynamoDB tables
    // -------------------------------------------------------------------------

    /**
     * DynamoDB table name used for atomic Flink-app leader elections
     * (i.e., which app is the active DynamoDB writer).
     * Defaults to {@code fasc-leader}.
     */
    private String dynamoDbLeaderTable = "fasc-leader";

    /**
     * DynamoDB table name used for coordinator-level distributed locking
     * (i.e., which coordinator pod is the active orchestrator).
     * Defaults to {@code fasc-coordinator-lock}.
     */
    private String dynamoDbLockTable = "fasc-coordinator-lock";

    /**
     * DynamoDB table name used to persist savepoint metadata for crash recovery.
     * Defaults to {@code fasc-savepoint-metadata}.
     */
    private String savepointMetadataTable = "fasc-savepoint-metadata";

    // =========================================================================
    // Getters and setters — required by Spring Boot property binding
    // =========================================================================

    /** @return comma-separated Kafka bootstrap broker addresses */
    public String getBootstrapServers() { return bootstrapServers; }
    /** @param bootstrapServers comma-separated Kafka broker list */
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    /** @return FASC control Kafka topic name */
    public String getControlTopic() { return controlTopic; }
    /** @param controlTopic control topic name */
    public void setControlTopic(String controlTopic) { this.controlTopic = controlTopic; }

    /** @return stable identifier for this coordinator pod */
    public String getCoordinatorId() { return coordinatorId; }
    /** @param coordinatorId coordinator pod identifier */
    public void setCoordinatorId(String coordinatorId) { this.coordinatorId = coordinatorId; }

    /** @return Flink job ID for App1 */
    public String getApp1JobId() { return app1JobId; }
    /** @param app1JobId Flink job ID for App1 */
    public void setApp1JobId(String app1JobId) { this.app1JobId = app1JobId; }

    /** @return Flink job ID for App2 */
    public String getApp2JobId() { return app2JobId; }
    /** @param app2JobId Flink job ID for App2 */
    public void setApp2JobId(String app2JobId) { this.app2JobId = app2JobId; }

    /** @return App1 Flink REST base URL */
    public String getApp1FlinkRestUrl() { return app1FlinkRestUrl; }
    /** @param app1FlinkRestUrl App1 Flink REST base URL */
    public void setApp1FlinkRestUrl(String app1FlinkRestUrl) { this.app1FlinkRestUrl = app1FlinkRestUrl; }

    /** @return App2 Flink REST base URL */
    public String getApp2FlinkRestUrl() { return app2FlinkRestUrl; }
    /** @param app2FlinkRestUrl App2 Flink REST base URL */
    public void setApp2FlinkRestUrl(String app2FlinkRestUrl) { this.app2FlinkRestUrl = app2FlinkRestUrl; }

    /** @return Kafka business topic name */
    public String getBusinessTopic() { return businessTopic; }
    /** @param businessTopic Kafka business topic name */
    public void setBusinessTopic(String businessTopic) { this.businessTopic = businessTopic; }

    /** @return Kafka consumer group ID for App1 */
    public String getApp1ConsumerGroup() { return app1ConsumerGroup; }
    /** @param app1ConsumerGroup Kafka consumer group for App1 */
    public void setApp1ConsumerGroup(String app1ConsumerGroup) { this.app1ConsumerGroup = app1ConsumerGroup; }

    /** @return Kafka consumer group ID for App2 */
    public String getApp2ConsumerGroup() { return app2ConsumerGroup; }
    /** @param app2ConsumerGroup Kafka consumer group for App2 */
    public void setApp2ConsumerGroup(String app2ConsumerGroup) { this.app2ConsumerGroup = app2ConsumerGroup; }

    /** @return offset monitor polling interval in milliseconds */
    public long getOffsetMonitorIntervalMs() { return offsetMonitorIntervalMs; }
    /** @param offsetMonitorIntervalMs polling interval in ms */
    public void setOffsetMonitorIntervalMs(long offsetMonitorIntervalMs) { this.offsetMonitorIntervalMs = offsetMonitorIntervalMs; }

    /** @return maximum per-partition lag allowed for promotion readiness */
    public long getLagReadinessThreshold() { return lagReadinessThreshold; }
    /** @param lagReadinessThreshold max lag threshold */
    public void setLagReadinessThreshold(long lagReadinessThreshold) { this.lagReadinessThreshold = lagReadinessThreshold; }

    /** @return duration (ms) lag must remain stable before promotion */
    public long getLagStabilityWindowMs() { return lagStabilityWindowMs; }
    /** @param lagStabilityWindowMs stability window in ms */
    public void setLagStabilityWindowMs(long lagStabilityWindowMs) { this.lagStabilityWindowMs = lagStabilityWindowMs; }

    /** @return heartbeat publishing interval in milliseconds */
    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    /** @param heartbeatIntervalMs heartbeat interval in ms */
    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }

    /** @return S3 bucket for Flink savepoints */
    public String getSavepointS3Bucket() { return savepointS3Bucket; }
    /** @param savepointS3Bucket S3 bucket name */
    public void setSavepointS3Bucket(String savepointS3Bucket) { this.savepointS3Bucket = savepointS3Bucket; }

    /** @return S3 key prefix for savepoints */
    public String getSavepointS3Prefix() { return savepointS3Prefix; }
    /** @param savepointS3Prefix S3 key prefix */
    public void setSavepointS3Prefix(String savepointS3Prefix) { this.savepointS3Prefix = savepointS3Prefix; }

    /** @return AWS region */
    public String getAwsRegion() { return awsRegion; }
    /** @param awsRegion AWS region code */
    public void setAwsRegion(String awsRegion) { this.awsRegion = awsRegion; }

    /** @return DynamoDB table name for Flink app leader election */
    public String getDynamoDbLeaderTable() { return dynamoDbLeaderTable; }
    /** @param dynamoDbLeaderTable DynamoDB leader table name */
    public void setDynamoDbLeaderTable(String dynamoDbLeaderTable) { this.dynamoDbLeaderTable = dynamoDbLeaderTable; }

    /** @return DynamoDB table name for coordinator distributed lock */
    public String getDynamoDbLockTable() { return dynamoDbLockTable; }
    /** @param dynamoDbLockTable DynamoDB lock table name */
    public void setDynamoDbLockTable(String dynamoDbLockTable) { this.dynamoDbLockTable = dynamoDbLockTable; }

    /** @return DynamoDB table name for savepoint metadata */
    public String getSavepointMetadataTable() { return savepointMetadataTable; }
    /** @param savepointMetadataTable DynamoDB savepoint metadata table name */
    public void setSavepointMetadataTable(String savepointMetadataTable) { this.savepointMetadataTable = savepointMetadataTable; }

    // =========================================================================
    // FascAwsProperties interface — allows AwsConfig to activate AWS SDK beans
    // =========================================================================

    /** {@inheritDoc} — delegates to {@link #getAwsRegion()} */
    @Override
    public String getLeaderTable() { return dynamoDbLeaderTable; }

    /** {@inheritDoc} — delegates to {@link #getDynamoDbLockTable()} */
    @Override
    public String getLockTable() { return dynamoDbLockTable; }

    /** {@inheritDoc} — delegates to {@link #getSavepointMetadataTable()} */
    @Override
    public String getSavepointTable() { return savepointMetadataTable; }

    @Override
    public String toString() {
        return "FascCoordinatorProperties{"
                + "coordinatorId='" + coordinatorId + '\''
                + ", controlTopic='" + controlTopic + '\''
                + ", app1JobId='" + app1JobId + '\''
                + ", app2JobId='" + app2JobId + '\''
                + ", app1FlinkRestUrl='" + app1FlinkRestUrl + '\''
                + ", app2FlinkRestUrl='" + app2FlinkRestUrl + '\''
                + ", offsetMonitorIntervalMs=" + offsetMonitorIntervalMs
                + ", lagReadinessThreshold=" + lagReadinessThreshold
                + ", lagStabilityWindowMs=" + lagStabilityWindowMs
                + ", heartbeatIntervalMs=" + heartbeatIntervalMs
                + ", awsRegion='" + awsRegion + '\''
                + ", dynamoDbLeaderTable='" + dynamoDbLeaderTable + '\''
                + ", dynamoDbLockTable='" + dynamoDbLockTable + '\''
                + '}';
    }
}
