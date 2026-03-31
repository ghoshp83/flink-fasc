package com.flinkfasc.examples;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Flink sink that writes {@link CustomerState} to DynamoDB.
 *
 * <p>Uses a conditional write to ensure idempotency:
 * only updates if {@code lastUpdatedMs} is greater than the stored value,
 * preventing out-of-order updates from corrupting state.
 *
 * <p>This sink is wrapped by {@link com.flinkfasc.core.ShadowSink} in the
 * main Flink job — writes are suppressed when the app is in SHADOW mode.
 */
public class DynamoDbSink extends RichSinkFunction<CustomerState> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSink.class);

    private final String tableName;
    private final String region;
    private transient DynamoDbClient dynamoDbClient;

    public DynamoDbSink(String tableName, String region) {
        this.tableName = tableName;
        this.region    = region;
    }

    @Override
    public void open(Configuration parameters) {
        this.dynamoDbClient = DynamoDbClient.builder()
                .region(Region.of(region))
                .build();
        LOG.info("DynamoDbSink opened. table={} region={}", tableName, region);
    }

    @Override
    public void invoke(CustomerState state, Context context) {
        Map<String, AttributeValue> item = buildItem(state);
        try {
            // Conditional write: only update if our lastUpdatedMs > stored value
            // This ensures idempotency — safe for FASC duplicate processing
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .conditionExpression(
                        "attribute_not_exists(customerId) OR lastUpdatedMs < :newTs")
                    .expressionAttributeValues(Map.of(
                        ":newTs", AttributeValue.builder().n(String.valueOf(state.getLastUpdatedMs())).build()
                    ))
                    .build();

            dynamoDbClient.putItem(request);
            LOG.debug("Written customerId={} orderCount={} totalAmount={}",
                      state.getCustomerId(), state.getOrderCount(), state.getTotalAmount());

        } catch (ConditionalCheckFailedException e) {
            // A newer record already exists — this write is a stale duplicate, safe to skip
            LOG.debug("Skipping stale write for customerId={} (newer record exists)", state.getCustomerId());
        } catch (DynamoDbException e) {
            LOG.error("DynamoDB write failed for customerId={}: {}", state.getCustomerId(), e.getMessage());
            throw new RuntimeException("DynamoDB write failed", e);
        }
    }

    private Map<String, AttributeValue> buildItem(CustomerState state) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("customerId",    AttributeValue.builder().s(state.getCustomerId()).build());
        item.put("orderCount",    AttributeValue.builder().n(String.valueOf(state.getOrderCount())).build());
        item.put("totalAmount",   AttributeValue.builder().n(String.valueOf(state.getTotalAmount())).build());
        item.put("lastEventType", AttributeValue.builder().s(state.getLastEventType()).build());
        item.put("lastUpdatedMs", AttributeValue.builder().n(String.valueOf(state.getLastUpdatedMs())).build());
        item.put("stateVersion",  AttributeValue.builder().n(String.valueOf(state.getStateVersion())).build());
        return item;
    }

    @Override
    public void close() {
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
    }
}
