package com.flinkfasc.coordinator.savepoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * DynamoDB-backed implementation of {@link SavepointStore}.
 *
 * <h3>DynamoDB table schema</h3>
 * <pre>
 *   Table name : configured via {@code fasc.dynamoDbLeaderTable} (reuses the leader table
 *                with different item types; can be changed to a dedicated table)
 *   PK (String): "SAVEPOINT#{savepointId}"
 *   GSI PK     : "SOURCE#{sourceJobId}" — for querying latest by source job
 *   GSI SK     : takenAtMs (Number) — for sorting by recency
 *   payload    : full JSON blob of the SavepointMetadata
 * </pre>
 *
 * <p>The full metadata is serialised to JSON and stored in a single {@code payload} attribute
 * to avoid maintaining individual DynamoDB attribute mappings for every field.
 */
@Component
public class DynamoDbSavepointStore implements SavepointStore {

    private static final Logger log = LoggerFactory.getLogger(DynamoDbSavepointStore.class);

    private static final String SAVEPOINT_TABLE_SUFFIX = "-savepoints";
    private static final String PK_PREFIX    = "SAVEPOINT#";
    private static final String GSI_PK_PREFIX = "SOURCE#";
    private static final String GSI_NAME     = "sourceJobId-takenAtMs-index";

    private final DynamoDbClient dynamoDb;
    private final FascCoordinatorProperties props;
    private final ObjectMapper objectMapper;

    /**
     * Constructs a new {@code DynamoDbSavepointStore}.
     *
     * @param dynamoDb     DynamoDB client
     * @param props        coordinator configuration properties
     * @param objectMapper Jackson mapper for JSON serialisation
     */
    public DynamoDbSavepointStore(DynamoDbClient dynamoDb,
                                   FascCoordinatorProperties props,
                                   ObjectMapper objectMapper) {
        this.dynamoDb     = dynamoDb;
        this.props        = props;
        this.objectMapper = objectMapper;
    }

    private String tableName() {
        return props.getDynamoDbLeaderTable() + SAVEPOINT_TABLE_SUFFIX;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void save(SavepointMetadata metadata) {
        try {
            String payload = objectMapper.writeValueAsString(metadata);

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk",          AttributeValue.fromS(PK_PREFIX + metadata.getSavepointId()));
            item.put("sourceGsi",   AttributeValue.fromS(GSI_PK_PREFIX + metadata.getSourceJobId()));
            item.put("takenAtMs",   AttributeValue.fromN(Long.toString(metadata.getTakenAtMs())));
            item.put("status",      AttributeValue.fromS(metadata.getStatus().name()));
            item.put("payload",     AttributeValue.fromS(payload));

            dynamoDb.putItem(PutItemRequest.builder()
                    .tableName(tableName())
                    .item(item)
                    .build());

            log.info("Savepoint stored: id={} status={} s3={}",
                    metadata.getSavepointId(), metadata.getStatus(), metadata.getS3Location());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialise SavepointMetadata for DynamoDB", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SavepointMetadata findLatestCompleted(String sourceJobId) {
        QueryResponse response = dynamoDb.query(QueryRequest.builder()
                .tableName(tableName())
                .indexName(GSI_NAME)
                .keyConditionExpression("sourceGsi = :gsiPk")
                .filterExpression("#st = :completed")
                .expressionAttributeNames(Map.of("#st", "status"))
                .expressionAttributeValues(Map.of(
                        ":gsiPk",     AttributeValue.fromS(GSI_PK_PREFIX + sourceJobId),
                        ":completed", AttributeValue.fromS(SavepointStatus.COMPLETED.name())))
                .scanIndexForward(false) // descending by sort key
                .limit(5)
                .build());

        return response.items().stream()
                .map(item -> deserialise(item.get("payload")))
                .filter(m -> m != null && m.getStatus() == SavepointStatus.COMPLETED)
                .max(Comparator.comparingLong(SavepointMetadata::getTakenAtMs))
                .orElse(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SavepointMetadata findById(String savepointId) {
        GetItemResponse response = dynamoDb.getItem(GetItemRequest.builder()
                .tableName(tableName())
                .key(Map.of("pk", AttributeValue.fromS(PK_PREFIX + savepointId)))
                .build());

        if (!response.hasItem()) {
            return null;
        }
        return deserialise(response.item().get("payload"));
    }

    private SavepointMetadata deserialise(AttributeValue payloadAttr) {
        if (payloadAttr == null || payloadAttr.s() == null) {
            return null;
        }
        try {
            return objectMapper.readValue(payloadAttr.s(), SavepointMetadata.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialise SavepointMetadata from DynamoDB payload", e);
            return null;
        }
    }
}
