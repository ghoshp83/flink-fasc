package com.flinkfasc.aws.election;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DynamoDbLeaderElection using mocked DynamoDbClient.
 * Integration tests against real DynamoDB use LocalStack (see DynamoDbLeaderElectionIT).
 */
@ExtendWith(MockitoExtension.class)
class DynamoDbLeaderElectionTest {

    @Mock
    private DynamoDbClient dynamoDbClient;

    private DynamoDbLeaderElection leaderElection;

    private static final String LEADER_TABLE = "fasc-leader";
    private static final String LOCK_TABLE = "fasc-coordinator-lock";

    @BeforeEach
    void setUp() {
        leaderElection = new DynamoDbLeaderElection(dynamoDbClient, LEADER_TABLE, LOCK_TABLE);
    }

    @Test
    void tryAcquireLock_succeeds_whenNoExistingLock() {
        when(dynamoDbClient.putItem(any(PutItemRequest.class)))
                .thenReturn(PutItemResponse.builder().build());

        boolean result = leaderElection.tryAcquireLock("coordinator-pod-001");

        assertTrue(result);
        verify(dynamoDbClient).putItem(any(PutItemRequest.class));
    }

    @Test
    void tryAcquireLock_fails_whenAnotherHoldsLock() {
        when(dynamoDbClient.putItem(any(PutItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder()
                        .message("The conditional request failed")
                        .build());

        boolean result = leaderElection.tryAcquireLock("coordinator-pod-001");

        assertFalse(result);
    }

    @Test
    void renewLock_succeeds_whenStillHolder() {
        // renewLock reads current version first via getItem, then updates
        GetItemResponse lockItem = GetItemResponse.builder()
                .item(Map.of(
                        "PK",            AttributeValue.builder().s("coordinator-lock").build(),
                        "coordinatorId", AttributeValue.builder().s("coordinator-pod-001").build(),
                        "version",       AttributeValue.builder().n("3").build()
                ))
                .build();
        when(dynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(lockItem);
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        boolean result = leaderElection.renewLock("coordinator-pod-001");

        assertTrue(result);
    }

    @Test
    void renewLock_fails_whenLockStolen() {
        // renewLock reads current version first via getItem, then tries to update
        GetItemResponse lockItem = GetItemResponse.builder()
                .item(Map.of(
                        "PK",            AttributeValue.builder().s("coordinator-lock").build(),
                        "coordinatorId", AttributeValue.builder().s("other-coordinator").build(),
                        "version",       AttributeValue.builder().n("5").build()
                ))
                .build();
        when(dynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(lockItem);
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder()
                        .message("The conditional request failed")
                        .build());

        boolean result = leaderElection.renewLock("coordinator-pod-001");

        assertFalse(result);
    }

    @Test
    void transferFlinkLeader_succeeds_withCorrectVersion() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenReturn(UpdateItemResponse.builder().build());

        boolean result = leaderElection.transferFlinkLeader("app1", "app2", 5);

        assertTrue(result);
        verify(dynamoDbClient).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void transferFlinkLeader_fails_whenVersionMismatch() {
        when(dynamoDbClient.updateItem(any(UpdateItemRequest.class)))
                .thenThrow(ConditionalCheckFailedException.builder()
                        .message("The conditional request failed")
                        .build());

        boolean result = leaderElection.transferFlinkLeader("app1", "app2", 5);

        assertFalse(result);
    }

    @Test
    void getCurrentFlinkLeader_returnsAppId() {
        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of(
                        "PK", AttributeValue.builder().s("active-flink-app").build(),
                        "appId", AttributeValue.builder().s("app1").build(),
                        "version", AttributeValue.builder().n("7").build()
                ))
                .build();
        when(dynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        String leader = leaderElection.getCurrentFlinkLeader();

        assertEquals("app1", leader);
    }

    @Test
    void getCurrentFlinkLeaderVersion_returnsVersion() {
        GetItemResponse response = GetItemResponse.builder()
                .item(Map.of(
                        "PK", AttributeValue.builder().s("active-flink-app").build(),
                        "appId", AttributeValue.builder().s("app1").build(),
                        "version", AttributeValue.builder().n("7").build()
                ))
                .build();
        when(dynamoDbClient.getItem(any(GetItemRequest.class))).thenReturn(response);

        int version = leaderElection.getCurrentFlinkLeaderVersion();

        assertEquals(7, version);
    }

    @Test
    void releaseLock_callsDeleteItem() {
        when(dynamoDbClient.deleteItem(any(DeleteItemRequest.class)))
                .thenReturn(DeleteItemResponse.builder().build());

        assertDoesNotThrow(() -> leaderElection.releaseLock("coordinator-pod-001"));
        verify(dynamoDbClient).deleteItem(any(DeleteItemRequest.class));
    }
}
