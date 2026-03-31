package com.flinkfasc.aws.savepoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Manages savepoint lifecycle metadata for the FASC system: path generation for S3 and
 * metadata persistence in DynamoDB.
 *
 * <h3>DynamoDB schema</h3>
 * Single-item table where every {@code PutItem} overwrites with PK {@code "savepoint-latest"},
 * ensuring only the most recent savepoint metadata is held. Fields stored:
 * <ul>
 *   <li>{@code PK} (String) — {@code "savepoint-latest"} (partition key)</li>
 *   <li>{@code savepointId} (String) — unique savepoint identifier</li>
 *   <li>{@code savepointPath} (String) — fully-qualified S3 path</li>
 *   <li>{@code appId} (String) — application that triggered the savepoint</li>
 *   <li>{@code jobId} (String) — Flink job identifier</li>
 *   <li>{@code status} (String) — e.g. {@code "PENDING"}, {@code "COMPLETED"}, {@code "FAILED"}</li>
 *   <li>{@code triggeredAt} (Number) — epoch-millis when the savepoint was requested</li>
 *   <li>{@code completedAt} (Number) — epoch-millis when the savepoint completed (0 if pending)</li>
 * </ul>
 *
 * <h3>S3 path format</h3>
 * {@code s3://<bucket>/<prefix>/sp-<epoch-millis>-<uuid>}
 *
 * <p>This class has no Spring annotations so that {@code fasc-aws} stays free of the Spring
 * compile-time dependency. The coordinator module wires it as a {@code @Bean} in
 * {@code AwsConfig}.
 */
public class S3SavepointStorage {

    private static final Logger LOG = LoggerFactory.getLogger(S3SavepointStorage.class);

    /** Partition key name and its single fixed value. */
    private static final String ATTR_PK             = "PK";
    private static final String PK_SAVEPOINT_LATEST = "savepoint-latest";

    /** Attribute names kept as constants to avoid typos. */
    private static final String ATTR_SAVEPOINT_ID   = "savepointId";
    private static final String ATTR_SAVEPOINT_PATH = "savepointPath";
    private static final String ATTR_APP_ID         = "appId";
    private static final String ATTR_JOB_ID         = "jobId";
    private static final String ATTR_STATUS         = "status";
    private static final String ATTR_TRIGGERED_AT   = "triggeredAt";
    private static final String ATTR_COMPLETED_AT   = "completedAt";

    private final DynamoDbClient dynamoDbClient;

    /**
     * Constructs an {@code S3SavepointStorage}.
     *
     * @param dynamoDbClient pre-configured AWS SDK v2 DynamoDB synchronous client; must not be null
     */
    public S3SavepointStorage(DynamoDbClient dynamoDbClient) {
        this.dynamoDbClient = Objects.requireNonNull(dynamoDbClient, "dynamoDbClient must not be null");
    }

    // -------------------------------------------------------------------------
    // Path generation
    // -------------------------------------------------------------------------

    /**
     * Generates a unique, time-ordered S3 path for a new savepoint.
     *
     * <p>The path format is:
     * <pre>
     * s3://&lt;s3Bucket&gt;/&lt;s3Prefix&gt;/sp-&lt;epoch-millis&gt;-&lt;uuid&gt;
     * </pre>
     * The timestamp prefix allows lexicographic sorting by time, and the UUID suffix
     * guarantees global uniqueness even in the face of clock skew.
     *
     * @param s3Bucket S3 bucket name (without the {@code s3://} scheme)
     * @param s3Prefix path prefix within the bucket (trailing slash is tolerated and normalised)
     * @return a fully-qualified S3 savepoint path string
     */
    public String generateSavepointPath(String s3Bucket, String s3Prefix) {
        Objects.requireNonNull(s3Bucket, "s3Bucket must not be null");
        Objects.requireNonNull(s3Prefix, "s3Prefix must not be null");

        // Normalise prefix: strip leading/trailing slashes so the concat always produces
        // exactly one '/' between components.
        String prefix = s3Prefix.replaceAll("^/+", "").replaceAll("/+$", "");

        long   epochMs = Instant.now().toEpochMilli();
        String uuid    = UUID.randomUUID().toString().replace("-", "");

        String path;
        if (prefix.isEmpty()) {
            path = String.format("s3://%s/sp-%d-%s", s3Bucket, epochMs, uuid);
        } else {
            path = String.format("s3://%s/%s/sp-%d-%s", s3Bucket, prefix, epochMs, uuid);
        }

        LOG.debug("Generated savepoint path: {}", path);
        return path;
    }

    // -------------------------------------------------------------------------
    // Metadata persistence
    // -------------------------------------------------------------------------

    /**
     * Stores savepoint metadata in DynamoDB, overwriting any previous record.
     *
     * <p>The item always uses PK {@code "savepoint-latest"} so that {@link #getLatestSavepointMetadata}
     * can fetch it with a single {@code GetItem} call. Each successful call replaces the
     * previous record, meaning only the most recent savepoint is retained in this table.
     * If long-term history is required, callers should write to a separate history table
     * before calling this method.
     *
     * <p>The {@code metadata} map must contain at minimum {@code savepointId},
     * {@code savepointPath}, {@code appId}, {@code jobId}, {@code status}, and
     * {@code triggeredAt} keys with non-null values. Additional keys are stored as-is.
     *
     * @param metadata       attribute map representing the savepoint; see class-level Javadoc
     *                       for the expected fields
     * @param dynamoDbTable  name of the DynamoDB table to write to
     * @throws NullPointerException if {@code metadata} or {@code dynamoDbTable} is null
     */
    public void storeSavepointMetadata(Map<String, AttributeValue> metadata, String dynamoDbTable) {
        Objects.requireNonNull(metadata,      "metadata must not be null");
        Objects.requireNonNull(dynamoDbTable, "dynamoDbTable must not be null");

        // Build the full item: start with the caller-supplied attributes and add/overwrite PK.
        Map<String, AttributeValue> item = new HashMap<>(metadata);
        item.put(ATTR_PK, strAttr(PK_SAVEPOINT_LATEST));

        PutItemRequest request = PutItemRequest.builder()
                .tableName(dynamoDbTable)
                .item(item)
                .build();

        dynamoDbClient.putItem(request);
        LOG.info("Stored savepoint metadata in table={} savepointId={}",
                 dynamoDbTable, strValue(metadata.get(ATTR_SAVEPOINT_ID)));
    }

    /**
     * Retrieves the most recently stored savepoint metadata from DynamoDB.
     *
     * <p>Returns the raw DynamoDB attribute map so the caller (typically the coordinator)
     * can map it to its own domain object without introducing a circular dependency.
     *
     * @param dynamoDbTable name of the DynamoDB table to read from
     * @return the attribute map of the latest savepoint item, or {@code null} if no savepoint
     *         has been stored yet
     */
    public Map<String, AttributeValue> getLatestSavepointMetadata(String dynamoDbTable) {
        Objects.requireNonNull(dynamoDbTable, "dynamoDbTable must not be null");

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(ATTR_PK, strAttr(PK_SAVEPOINT_LATEST));

        GetItemRequest request = GetItemRequest.builder()
                .tableName(dynamoDbTable)
                .key(key)
                .consistentRead(true)
                .build();

        GetItemResponse response = dynamoDbClient.getItem(request);
        if (!response.hasItem() || response.item().isEmpty()) {
            LOG.debug("getLatestSavepointMetadata: no savepoint found in table={}", dynamoDbTable);
            return null;
        }

        Map<String, AttributeValue> item = response.item();
        LOG.debug("Retrieved latest savepoint metadata from table={} savepointId={}",
                  dynamoDbTable, strValue(item.get(ATTR_SAVEPOINT_ID)));
        return item;
    }

    /**
     * Updates the {@code status} field of the latest savepoint record in DynamoDB.
     *
     * <p>Does not verify that the item exists; if the item is absent the {@code UpdateItem}
     * will create a new skeletal item containing only {@code PK} and {@code status}.
     * Callers should ensure {@link #storeSavepointMetadata} has been called first.
     *
     * @param savepointId   the savepoint identifier, used only for logging; the actual PK
     *                      is always {@code "savepoint-latest"}
     * @param status        the new status string (e.g. {@code "COMPLETED"}, {@code "FAILED"})
     * @param dynamoDbTable name of the DynamoDB table to update
     * @throws NullPointerException if any argument is null
     */
    public void updateSavepointStatus(String savepointId, String status, String dynamoDbTable) {
        Objects.requireNonNull(savepointId,   "savepointId must not be null");
        Objects.requireNonNull(status,        "status must not be null");
        Objects.requireNonNull(dynamoDbTable, "dynamoDbTable must not be null");

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(ATTR_PK, strAttr(PK_SAVEPOINT_LATEST));

        long nowMs = Instant.now().toEpochMilli();

        Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(":status",      strAttr(status));
        exprValues.put(":completedAt", numAttr(nowMs));

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(dynamoDbTable)
                .key(key)
                .updateExpression("SET #st = :status, completedAt = :completedAt")
                .expressionAttributeNames(Map.of("#st", ATTR_STATUS))
                .expressionAttributeValues(exprValues)
                .build();

        dynamoDbClient.updateItem(request);
        LOG.info("Updated savepoint status in table={} savepointId={} status={} completedAt={}",
                 dynamoDbTable, savepointId, status, nowMs);
    }

    // -------------------------------------------------------------------------
    // Convenience factory methods for building metadata maps
    // -------------------------------------------------------------------------

    /**
     * Convenience factory that builds a complete savepoint metadata attribute map from
     * individual fields.
     *
     * <p>Callers can pass the returned map directly to
     * {@link #storeSavepointMetadata(Map, String)}.
     *
     * @param savepointId   unique identifier for this savepoint (e.g. UUID string)
     * @param savepointPath fully-qualified S3 path returned by {@link #generateSavepointPath}
     * @param appId         Flink application identifier that triggered the savepoint
     * @param jobId         Flink job identifier
     * @param status        initial status, typically {@code "PENDING"}
     * @return an attribute map ready for DynamoDB storage
     */
    public static Map<String, AttributeValue> buildMetadata(String savepointId,
                                                            String savepointPath,
                                                            String appId,
                                                            String jobId,
                                                            String status) {
        long triggeredAt = Instant.now().toEpochMilli();

        Map<String, AttributeValue> metadata = new HashMap<>();
        metadata.put(ATTR_SAVEPOINT_ID,   strAttr(savepointId));
        metadata.put(ATTR_SAVEPOINT_PATH, strAttr(savepointPath));
        metadata.put(ATTR_APP_ID,         strAttr(appId));
        metadata.put(ATTR_JOB_ID,         strAttr(jobId));
        metadata.put(ATTR_STATUS,         strAttr(status));
        metadata.put(ATTR_TRIGGERED_AT,   numAttr(triggeredAt));
        metadata.put(ATTR_COMPLETED_AT,   numAttr(0L));
        return metadata;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /** Creates a string {@link AttributeValue}. */
    private static AttributeValue strAttr(String value) {
        return AttributeValue.builder().s(value).build();
    }

    /** Creates a numeric {@link AttributeValue} from a {@code long}. */
    private static AttributeValue numAttr(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    /** Extracts the string value from an {@link AttributeValue}, returning {@code null} if absent. */
    private static String strValue(AttributeValue av) {
        return (av != null) ? av.s() : null;
    }
}
