package com.flinkfasc.aws.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * DynamoDB-backed implementation of coordinator leader-election and Flink application
 * leader tracking for the FASC system.
 *
 * <h3>DynamoDB tables</h3>
 * <dl>
 *   <dt>{@code fasc-coordinator-lock} (default, overridable)</dt>
 *   <dd>Holds a single item with PK {@code "coordinator-lock"}. Contains
 *       {@code coordinatorId}, {@code acquiredAt}, {@code ttlEpoch} (DynamoDB TTL attribute),
 *       and {@code version}. Optimistic locking via {@code version} ensures only one
 *       coordinator renews at a time.</dd>
 *   <dt>{@code fasc-leader} (default, overridable)</dt>
 *   <dd>Holds a single item with PK {@code "active-flink-app"}. Contains {@code appId},
 *       {@code jobId}, {@code version}, and {@code updatedAt}. Conditional writes prevent
 *       split-brain during handoff.</dd>
 * </dl>
 *
 * <p>All conditional-write failures are reported as {@code false} return values rather than
 * thrown exceptions, so callers can implement retry/back-off without exception handling.
 *
 * <p>This class is intentionally <em>not</em> annotated with any Spring stereotype so that
 * the {@code fasc-aws} module remains free of the Spring dependency. The coordinator module
 * wires it as a {@code @Bean} in {@code AwsConfig}.
 */
public class DynamoDbLeaderElection {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbLeaderElection.class);

    /** Partition key name shared by both tables. */
    private static final String ATTR_PK = "PK";

    /** Coordinator lock table attributes. */
    private static final String ATTR_COORDINATOR_ID = "coordinatorId";
    private static final String ATTR_ACQUIRED_AT    = "acquiredAt";
    private static final String ATTR_TTL_EPOCH      = "ttlEpoch";
    private static final String ATTR_VERSION        = "version";

    /** Leader table attributes. */
    private static final String ATTR_APP_ID     = "appId";
    private static final String ATTR_JOB_ID     = "jobId";
    private static final String ATTR_UPDATED_AT = "updatedAt";

    /** Partition key values. */
    private static final String PK_COORDINATOR_LOCK = "coordinator-lock";
    private static final String PK_ACTIVE_FLINK_APP = "active-flink-app";

    /** How many seconds a lock is valid before it must be renewed. */
    private static final long LOCK_TTL_SECONDS = 30L;

    private final DynamoDbClient dynamoDbClient;
    private final String leaderTable;
    private final String lockTable;

    /**
     * Constructs a {@code DynamoDbLeaderElection}.
     *
     * @param dynamoDbClient pre-configured AWS SDK v2 DynamoDB synchronous client; must not be null
     * @param leaderTable    name of the DynamoDB table that tracks the active Flink application
     * @param lockTable      name of the DynamoDB table used for coordinator mutual exclusion
     */
    public DynamoDbLeaderElection(DynamoDbClient dynamoDbClient,
                                  String leaderTable,
                                  String lockTable) {
        this.dynamoDbClient = Objects.requireNonNull(dynamoDbClient, "dynamoDbClient must not be null");
        this.leaderTable    = Objects.requireNonNull(leaderTable,    "leaderTable must not be null");
        this.lockTable      = Objects.requireNonNull(lockTable,      "lockTable must not be null");
    }

    // -------------------------------------------------------------------------
    // Coordinator lock operations
    // -------------------------------------------------------------------------

    /**
     * Attempts to acquire the coordinator lock for the given {@code coordinatorId}.
     *
     * <p>Uses a conditional {@code PutItem} that succeeds only when the lock item does not
     * exist ({@code attribute_not_exists(PK)}) or the TTL has expired
     * ({@code ttlEpoch < :now}), preventing two coordinators from believing they hold the
     * lock simultaneously.
     *
     * @param coordinatorId unique identifier of the coordinator replica that wants the lock
     * @return {@code true} if the lock was acquired; {@code false} if another coordinator
     *         already holds a valid lock
     */
    public boolean tryAcquireLock(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");

        long nowEpoch  = Instant.now().getEpochSecond();
        long ttlEpoch  = nowEpoch + LOCK_TTL_SECONDS;

        Map<String, AttributeValue> item = new HashMap<>();
        item.put(ATTR_PK,             strAttr(PK_COORDINATOR_LOCK));
        item.put(ATTR_COORDINATOR_ID, strAttr(coordinatorId));
        item.put(ATTR_ACQUIRED_AT,    numAttr(nowEpoch));
        item.put(ATTR_TTL_EPOCH,      numAttr(ttlEpoch));
        item.put(ATTR_VERSION,        numAttr(1L));

        Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(":now", numAttr(nowEpoch));

        PutItemRequest request = PutItemRequest.builder()
                .tableName(lockTable)
                .item(item)
                .conditionExpression("attribute_not_exists(PK) OR ttlEpoch < :now")
                .expressionAttributeValues(exprValues)
                .build();

        try {
            dynamoDbClient.putItem(request);
            LOG.info("Coordinator lock acquired by coordinatorId={}", coordinatorId);
            return true;
        } catch (ConditionalCheckFailedException e) {
            LOG.debug("Lock acquisition failed for coordinatorId={} — another coordinator holds the lock", coordinatorId);
            return false;
        }
    }

    /**
     * Renews the coordinator lock, extending its TTL by {@value #LOCK_TTL_SECONDS} seconds.
     *
     * <p>The conditional {@code UpdateItem} verifies that the caller still owns the lock
     * ({@code coordinatorId = :myId}) and that the version has not been bumped by a
     * concurrent renew ({@code version = :currentVersion}), providing optimistic locking.
     *
     * @param coordinatorId the coordinator that currently holds the lock
     * @return {@code true} if the lock was successfully renewed; {@code false} if the lock
     *         was lost or stolen between acquisition and renewal
     */
    public boolean renewLock(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");

        // Read the current version first so we can use it in the condition.
        Map<String, AttributeValue> currentItem = getLockItem();
        if (currentItem == null) {
            LOG.warn("renewLock: lock item not found for coordinatorId={}", coordinatorId);
            return false;
        }

        long currentVersion = longValue(currentItem.get(ATTR_VERSION));
        long newTtlEpoch    = Instant.now().getEpochSecond() + LOCK_TTL_SECONDS;

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(ATTR_PK, strAttr(PK_COORDINATOR_LOCK));

        Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(":myId",           strAttr(coordinatorId));
        exprValues.put(":currentVersion", numAttr(currentVersion));
        exprValues.put(":newVersion",     numAttr(currentVersion + 1));
        exprValues.put(":newTtlEpoch",    numAttr(newTtlEpoch));

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(lockTable)
                .key(key)
                .updateExpression("SET #ver = :newVersion, ttlEpoch = :newTtlEpoch")
                .conditionExpression("coordinatorId = :myId AND #ver = :currentVersion")
                .expressionAttributeNames(Map.of("#ver", ATTR_VERSION))
                .expressionAttributeValues(exprValues)
                .build();

        try {
            dynamoDbClient.updateItem(request);
            LOG.debug("Lock renewed for coordinatorId={}, new version={}", coordinatorId, currentVersion + 1);
            return true;
        } catch (ConditionalCheckFailedException e) {
            LOG.warn("Lock renewal failed for coordinatorId={} — lock was lost or version mismatch", coordinatorId);
            return false;
        }
    }

    /**
     * Releases the coordinator lock held by the specified coordinator.
     *
     * <p>Uses a conditional {@code DeleteItem} that only deletes the item when the stored
     * {@code coordinatorId} matches the caller, preventing an expired-and-re-acquired lock
     * from being deleted by a stale caller.
     *
     * @param coordinatorId the coordinator that currently holds the lock
     */
    public void releaseLock(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(ATTR_PK, strAttr(PK_COORDINATOR_LOCK));

        Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(":myId", strAttr(coordinatorId));

        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(lockTable)
                .key(key)
                .conditionExpression("coordinatorId = :myId")
                .expressionAttributeValues(exprValues)
                .build();

        try {
            dynamoDbClient.deleteItem(request);
            LOG.info("Coordinator lock released by coordinatorId={}", coordinatorId);
        } catch (ConditionalCheckFailedException e) {
            LOG.warn("releaseLock: condition failed for coordinatorId={} — lock may have already expired or been re-acquired", coordinatorId);
        }
    }

    /**
     * Returns whether the given coordinator currently holds a valid (non-expired) lock.
     *
     * @param coordinatorId the coordinator to check
     * @return {@code true} if the item exists in DynamoDB, the stored {@code coordinatorId}
     *         matches, and {@code ttlEpoch} is in the future; {@code false} otherwise
     */
    public boolean isLockHolder(String coordinatorId) {
        Objects.requireNonNull(coordinatorId, "coordinatorId must not be null");

        Map<String, AttributeValue> item = getLockItem();
        if (item == null) {
            return false;
        }

        String storedId  = strValue(item.get(ATTR_COORDINATOR_ID));
        long   ttlEpoch  = longValue(item.get(ATTR_TTL_EPOCH));
        long   nowEpoch  = Instant.now().getEpochSecond();

        boolean holds = coordinatorId.equals(storedId) && ttlEpoch > nowEpoch;
        LOG.debug("isLockHolder: coordinatorId={} holds={} ttlEpoch={} now={}", coordinatorId, holds, ttlEpoch, nowEpoch);
        return holds;
    }

    // -------------------------------------------------------------------------
    // Flink leader operations
    // -------------------------------------------------------------------------

    /**
     * Atomically transfers the active Flink application role from {@code fromAppId} to
     * {@code toAppId} using an optimistic-locking conditional update.
     *
     * <p>The write succeeds only when the current {@code appId} in the leader table equals
     * {@code fromAppId} and the {@code version} equals {@code currentVersion}, preventing
     * concurrent transitions from producing split-brain writes.
     *
     * @param fromAppId      the application that is currently ACTIVE
     * @param toAppId        the application that should become ACTIVE
     * @param currentVersion the expected current version in DynamoDB; obtained via
     *                       {@link #getCurrentFlinkLeaderVersion()}
     * @return {@code true} if the transfer succeeded; {@code false} if the condition failed
     *         (e.g. another coordinator already performed the transfer)
     */
    public boolean transferFlinkLeader(String fromAppId, String toAppId, int currentVersion) {
        Objects.requireNonNull(fromAppId, "fromAppId must not be null");
        Objects.requireNonNull(toAppId,   "toAppId must not be null");

        long nowMs = Instant.now().toEpochMilli();

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(ATTR_PK, strAttr(PK_ACTIVE_FLINK_APP));

        Map<String, AttributeValue> exprValues = new HashMap<>();
        exprValues.put(":fromAppId",      strAttr(fromAppId));
        exprValues.put(":toAppId",        strAttr(toAppId));
        exprValues.put(":currentVersion", numAttr(currentVersion));
        exprValues.put(":newVersion",     numAttr(currentVersion + 1));
        exprValues.put(":updatedAt",      numAttr(nowMs));

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(leaderTable)
                .key(key)
                .updateExpression("SET appId = :toAppId, #ver = :newVersion, updatedAt = :updatedAt")
                .conditionExpression("appId = :fromAppId AND #ver = :currentVersion")
                .expressionAttributeNames(Map.of("#ver", ATTR_VERSION))
                .expressionAttributeValues(exprValues)
                .build();

        try {
            dynamoDbClient.updateItem(request);
            LOG.info("Flink leader transferred from={} to={} newVersion={}", fromAppId, toAppId, currentVersion + 1);
            return true;
        } catch (ConditionalCheckFailedException e) {
            LOG.warn("transferFlinkLeader: condition failed — fromAppId={} currentVersion={} may be stale", fromAppId, currentVersion);
            return false;
        }
    }

    /**
     * Retrieves the {@code appId} of the currently active Flink application from the
     * leader table.
     *
     * @return the {@code appId} string, or {@code null} if the item does not exist
     */
    public String getCurrentFlinkLeader() {
        Map<String, AttributeValue> item = getLeaderItem();
        if (item == null) {
            LOG.debug("getCurrentFlinkLeader: no item found in leader table");
            return null;
        }
        return strValue(item.get(ATTR_APP_ID));
    }

    /**
     * Retrieves the current optimistic-locking version of the leader record.
     *
     * <p>Callers should pass this value to {@link #transferFlinkLeader} to ensure
     * conditional writes use an up-to-date version number and avoid lost updates.
     *
     * @return the current version as an {@code int}, or {@code 0} if the item does not exist
     */
    public int getCurrentFlinkLeaderVersion() {
        Map<String, AttributeValue> item = getLeaderItem();
        if (item == null) {
            LOG.debug("getCurrentFlinkLeaderVersion: no item found in leader table, returning 0");
            return 0;
        }
        return (int) longValue(item.get(ATTR_VERSION));
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Fetches the coordinator lock item from DynamoDB, or {@code null} if absent.
     */
    private Map<String, AttributeValue> getLockItem() {
        return getItem(lockTable, PK_COORDINATOR_LOCK);
    }

    /**
     * Fetches the leader item from DynamoDB, or {@code null} if absent.
     */
    private Map<String, AttributeValue> getLeaderItem() {
        return getItem(leaderTable, PK_ACTIVE_FLINK_APP);
    }

    /**
     * Generic strongly-consistent {@code GetItem} helper.
     *
     * @param tableName DynamoDB table name
     * @param pkValue   value of the {@code PK} partition key
     * @return the item attribute map, or {@code null} if the item does not exist
     */
    private Map<String, AttributeValue> getItem(String tableName, String pkValue) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(ATTR_PK, strAttr(pkValue));

        GetItemRequest request = GetItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .consistentRead(true)
                .build();

        GetItemResponse response = dynamoDbClient.getItem(request);
        if (!response.hasItem() || response.item().isEmpty()) {
            return null;
        }
        return response.item();
    }

    /** Creates a string {@link AttributeValue}. */
    private static AttributeValue strAttr(String value) {
        return AttributeValue.builder().s(value).build();
    }

    /** Creates a numeric {@link AttributeValue} from a {@code long}. */
    private static AttributeValue numAttr(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    /** Creates a numeric {@link AttributeValue} from an {@code int}. */
    private static AttributeValue numAttr(int value) {
        return AttributeValue.builder().n(Integer.toString(value)).build();
    }

    /** Extracts the string value from an {@link AttributeValue}, returning {@code null} if absent. */
    private static String strValue(AttributeValue av) {
        return (av != null) ? av.s() : null;
    }

    /** Extracts the numeric value from an {@link AttributeValue}, returning {@code 0} if absent. */
    private static long longValue(AttributeValue av) {
        if (av == null || av.n() == null) {
            return 0L;
        }
        return Long.parseLong(av.n());
    }
}
