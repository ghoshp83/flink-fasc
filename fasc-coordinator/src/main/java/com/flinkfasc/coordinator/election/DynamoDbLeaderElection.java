package com.flinkfasc.coordinator.election;

import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
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

/**
 * DynamoDB-backed implementation of {@link LeaderElection}.
 *
 * <p>Coordinator lock TTL is set to 30 seconds and renewed every
 * {@link FascCoordinatorProperties#getHeartbeatIntervalMs()} milliseconds.
 * A coordinator that fails to renew within that window loses the lock automatically
 * once the TTL expires (DynamoDB TTL scan runs within ~48 hours, but the coordinator
 * checks the TTL value itself on read to detect expiry within seconds).
 */
@Component
public class DynamoDbLeaderElection implements LeaderElection {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbLeaderElection.class);

    /** DynamoDB primary key value for the coordinator lock item. */
    private static final String COORDINATOR_LOCK_PK = "COORDINATOR_LOCK";
    /** DynamoDB primary key value for the Flink app leader item. */
    private static final String FLINK_LEADER_PK = "FLINK_LEADER";
    /** Lock TTL in seconds — must be longer than the heartbeat/renew interval. */
    private static final long LOCK_TTL_SECONDS = 30L;

    private final DynamoDbClient dynamoDb;
    private final FascCoordinatorProperties props;

    /**
     * Constructs a new {@code DynamoDbLeaderElection}.
     *
     * @param dynamoDb DynamoDB client (injected by Spring)
     * @param props    coordinator configuration properties
     */
    public DynamoDbLeaderElection(DynamoDbClient dynamoDb, FascCoordinatorProperties props) {
        this.dynamoDb = dynamoDb;
        this.props = props;
    }

    // =========================================================================
    // Coordinator lock
    // =========================================================================

    /**
     * {@inheritDoc}
     *
     * <p>Attempts a conditional {@code PutItem} that succeeds only when no unexpired
     * lock exists.  The condition is: {@code attribute_not_exists(pk) OR ttl < :now}.
     */
    @Override
    public boolean tryAcquireLock(String coordinatorId) {
        long ttlEpoch = Instant.now().getEpochSecond() + LOCK_TTL_SECONDS;

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK",            AttributeValue.fromS(COORDINATOR_LOCK_PK));
        item.put("coordinatorId", AttributeValue.fromS(coordinatorId));
        item.put("ttl",           AttributeValue.fromN(Long.toString(ttlEpoch)));

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":now", AttributeValue.fromN(Long.toString(Instant.now().getEpochSecond())));

        try {
            dynamoDb.putItem(PutItemRequest.builder()
                    .tableName(props.getDynamoDbLockTable())
                    .item(item)
                    .conditionExpression("attribute_not_exists(pk) OR #ttl < :now")
                    .expressionAttributeNames(Map.of("#ttl", "ttl"))
                    .expressionAttributeValues(expressionValues)
                    .build());
            log.info("Coordinator lock acquired by {}", coordinatorId);
            return true;
        } catch (ConditionalCheckFailedException e) {
            log.debug("Coordinator lock not available for {} — another coordinator holds it", coordinatorId);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Uses a conditional {@code UpdateItem} that verifies {@code coordinatorId} matches.
     */
    @Override
    public boolean renewLock(String coordinatorId) {
        long newTtlEpoch = Instant.now().getEpochSecond() + LOCK_TTL_SECONDS;

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK", AttributeValue.fromS(COORDINATOR_LOCK_PK));

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":coordinatorId", AttributeValue.fromS(coordinatorId));
        expressionValues.put(":newTtl",        AttributeValue.fromN(Long.toString(newTtlEpoch)));

        try {
            dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(props.getDynamoDbLockTable())
                    .key(key)
                    .updateExpression("SET #ttl = :newTtl")
                    .conditionExpression("coordinatorId = :coordinatorId")
                    .expressionAttributeNames(Map.of("#ttl", "ttl"))
                    .expressionAttributeValues(expressionValues)
                    .build());
            log.debug("Coordinator lock renewed by {}", coordinatorId);
            return true;
        } catch (ConditionalCheckFailedException e) {
            log.warn("Failed to renew coordinator lock for {} — lock was taken by another coordinator", coordinatorId);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void releaseLock(String coordinatorId) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK", AttributeValue.fromS(COORDINATOR_LOCK_PK));

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":coordinatorId", AttributeValue.fromS(coordinatorId));

        try {
            dynamoDb.deleteItem(DeleteItemRequest.builder()
                    .tableName(props.getDynamoDbLockTable())
                    .key(key)
                    .conditionExpression("coordinatorId = :coordinatorId")
                    .expressionAttributeValues(expressionValues)
                    .build());
            log.info("Coordinator lock released by {}", coordinatorId);
        } catch (ConditionalCheckFailedException e) {
            log.debug("releaseLock no-op: lock was already held by a different coordinator");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLockHolder(String coordinatorId) {
        GetItemResponse response = dynamoDb.getItem(GetItemRequest.builder()
                .tableName(props.getDynamoDbLockTable())
                .key(Map.of("PK", AttributeValue.fromS(COORDINATOR_LOCK_PK)))
                .build());

        if (!response.hasItem()) {
            return false;
        }
        Map<String, AttributeValue> item = response.item();
        String storedId = item.getOrDefault("coordinatorId", AttributeValue.fromS("")).s();
        long ttl = Long.parseLong(item.getOrDefault("ttl", AttributeValue.fromN("0")).n());
        long now = Instant.now().getEpochSecond();

        return coordinatorId.equals(storedId) && ttl > now;
    }

    // =========================================================================
    // Flink app leader
    // =========================================================================

    /**
     * {@inheritDoc}
     *
     * <p>Uses a conditional {@code UpdateItem} with {@code activeAppId = :fromAppId AND version = :currentVersion}.
     * On success the version is atomically incremented by 1.
     */
    @Override
    public boolean transferFlinkLeader(String fromAppId, String toAppId, int currentVersion) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK", AttributeValue.fromS(FLINK_LEADER_PK));

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":fromAppId",       AttributeValue.fromS(fromAppId));
        expressionValues.put(":toAppId",         AttributeValue.fromS(toAppId));
        expressionValues.put(":currentVersion",  AttributeValue.fromN(Integer.toString(currentVersion)));
        expressionValues.put(":increment",       AttributeValue.fromN("1"));

        try {
            dynamoDb.updateItem(UpdateItemRequest.builder()
                    .tableName(props.getDynamoDbLeaderTable())
                    .key(key)
                    .updateExpression("SET activeAppId = :toAppId, #ver = #ver + :increment")
                    .conditionExpression("activeAppId = :fromAppId AND #ver = :currentVersion")
                    .expressionAttributeNames(Map.of("#ver", "version"))
                    .expressionAttributeValues(expressionValues)
                    .build());
            log.info("Flink leader transferred: {} -> {} (version {} -> {})", fromAppId, toAppId,
                    currentVersion, currentVersion + 1);
            return true;
        } catch (ConditionalCheckFailedException e) {
            log.error("Flink leader transfer FAILED — conditional check rejected. "
                    + "fromAppId={}, toAppId={}, expectedVersion={}", fromAppId, toAppId, currentVersion);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCurrentFlinkLeader() {
        GetItemResponse response = dynamoDb.getItem(GetItemRequest.builder()
                .tableName(props.getDynamoDbLeaderTable())
                .key(Map.of("PK", AttributeValue.fromS(FLINK_LEADER_PK)))
                .build());

        if (!response.hasItem()) {
            return null;
        }
        AttributeValue val = response.item().get("activeAppId");
        return val != null ? val.s() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getCurrentFlinkLeaderVersion() {
        GetItemResponse response = dynamoDb.getItem(GetItemRequest.builder()
                .tableName(props.getDynamoDbLeaderTable())
                .key(Map.of("PK", AttributeValue.fromS(FLINK_LEADER_PK)))
                .build());

        if (!response.hasItem()) {
            return -1;
        }
        AttributeValue val = response.item().get("version");
        return val != null ? Integer.parseInt(val.n()) : -1;
    }
}
