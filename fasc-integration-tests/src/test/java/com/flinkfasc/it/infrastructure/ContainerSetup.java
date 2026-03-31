package com.flinkfasc.it.infrastructure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;

import java.time.Duration;

/**
 * Shared Testcontainers setup for all FASC integration tests.
 *
 * <p>Uses lazily-initialized static containers so they are started once per JVM
 * and reused across all test classes. Containers are not instantiated until
 * {@link #start()} is called, avoiding Docker detection at class-load time.
 *
 * <p>Containers:
 * <ul>
 *   <li><b>KAFKA</b> — Confluent Kafka 7.4 for the fasc-control-topic and business-topic</li>
 *   <li><b>LOCALSTACK</b> — LocalStack 3.x for DynamoDB, S3, and CloudWatch</li>
 * </ul>
 *
 * <p>Usage in test classes:
 * <pre>{@code
 * class MyIT {
 *     @BeforeAll
 *     static void setup() {
 *         ContainerSetup.start(); // idempotent — safe to call multiple times
 *     }
 *
 *     @Test
 *     void myTest() {
 *         String brokers = ContainerSetup.kafkaBootstrapServers();
 *         DynamoDbClient dynamo = ContainerSetup.dynamoDbClient();
 *     }
 * }
 * }</pre>
 */
public final class ContainerSetup {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerSetup.class);

    // Lazily initialized — not created until start() is called
    private static KafkaContainer        kafka;
    private static LocalStackContainer   localstack;

    // Topic names
    public static final String CONTROL_TOPIC  = "fasc-control-topic";
    public static final String BUSINESS_TOPIC = "business-topic";

    // DynamoDB table names (must match application config)
    public static final String LEADER_TABLE             = "fasc-leader";
    public static final String LOCK_TABLE               = "fasc-coordinator-lock";
    public static final String SAVEPOINT_METADATA_TABLE = "fasc-savepoint-metadata";

    private static volatile boolean started = false;

    private ContainerSetup() {}

    /**
     * Starts all containers. Idempotent — safe to call multiple times.
     * Also creates required Kafka topics and DynamoDB tables.
     */
    public static synchronized void start() {
        if (started) return;

        LOG.info("Starting integration test containers...");

        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                .withStartupTimeout(Duration.ofMinutes(3));

        localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0"))
                .withServices(
                        LocalStackContainer.Service.DYNAMODB,
                        LocalStackContainer.Service.S3,
                        LocalStackContainer.Service.CLOUDWATCH)
                .withStartupTimeout(Duration.ofMinutes(3));

        kafka.start();
        localstack.start();
        started = true;  // set before createDynamoDbTables so dynamoDbClient() guard passes

        createDynamoDbTables();
        LOG.info("Containers started. Kafka={} LocalStack={}",
                kafka.getBootstrapServers(),
                localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB));
    }

    /** Returns the Kafka bootstrap servers string for connecting producers/consumers. */
    public static String kafkaBootstrapServers() {
        ensureStarted();
        return kafka.getBootstrapServers();
    }

    /**
     * Creates a DynamoDB client connected to LocalStack.
     * Each call returns a new client — caller is responsible for closing it.
     */
    public static DynamoDbClient dynamoDbClient() {
        ensureStarted();
        return DynamoDbClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .region(Region.of(localstack.getRegion()))
                .httpClient(UrlConnectionHttpClient.create())
                .build();
    }

    /**
     * Creates a CloudWatch client connected to LocalStack.
     * Each call returns a new client — caller is responsible for closing it.
     */
    public static CloudWatchClient cloudWatchClient() {
        ensureStarted();
        return CloudWatchClient.builder()
                .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.CLOUDWATCH))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .region(Region.of(localstack.getRegion()))
                .httpClient(UrlConnectionHttpClient.create())
                .build();
    }

    private static void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("ContainerSetup.start() must be called before accessing containers");
        }
    }

    // ── DynamoDB table creation ────────────────────────────────────────────────

    private static void createDynamoDbTables() {
        try (DynamoDbClient client = dynamoDbClient()) {
            createLeaderTable(client);
            createLockTable(client);
            createSavepointMetadataTable(client);
        }
        LOG.info("DynamoDB tables created in LocalStack");
    }

    private static void createLeaderTable(DynamoDbClient client) {
        try {
            client.createTable(CreateTableRequest.builder()
                    .tableName(LEADER_TABLE)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("PK").attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder()
                            .attributeName("PK").keyType(KeyType.HASH).build())
                    .build());

            // Seed for fasc-aws DynamoDbLeaderElection (uses PK="active-flink-app", field="appId")
            client.putItem(PutItemRequest.builder()
                    .tableName(LEADER_TABLE)
                    .item(java.util.Map.of(
                            "PK",        software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("active-flink-app").build(),
                            "appId",     software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("app1").build(),
                            "version",   software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n("1").build(),
                            "updatedAt", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(String.valueOf(System.currentTimeMillis())).build()
                    ))
                    .build());
            // Seed for fasc-coordinator DynamoDbLeaderElection (uses PK="FLINK_LEADER", field="activeAppId")
            client.putItem(PutItemRequest.builder()
                    .tableName(LEADER_TABLE)
                    .item(java.util.Map.of(
                            "PK",          software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("FLINK_LEADER").build(),
                            "activeAppId", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("app1").build(),
                            "version",     software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n("1").build(),
                            "updatedAt",   software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(String.valueOf(System.currentTimeMillis())).build()
                    ))
                    .build());
        } catch (ResourceInUseException e) {
            LOG.debug("Table {} already exists", LEADER_TABLE);
        }
    }

    private static void createLockTable(DynamoDbClient client) {
        try {
            client.createTable(CreateTableRequest.builder()
                    .tableName(LOCK_TABLE)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("PK").attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder()
                            .attributeName("PK").keyType(KeyType.HASH).build())
                    .build());
        } catch (ResourceInUseException e) {
            LOG.debug("Table {} already exists", LOCK_TABLE);
        }
    }

    private static void createSavepointMetadataTable(DynamoDbClient client) {
        try {
            client.createTable(CreateTableRequest.builder()
                    .tableName(SAVEPOINT_METADATA_TABLE)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("PK").attributeType(ScalarAttributeType.S).build())
                    .keySchema(KeySchemaElement.builder()
                            .attributeName("PK").keyType(KeyType.HASH).build())
                    .build());
        } catch (ResourceInUseException e) {
            LOG.debug("Table {} already exists", SAVEPOINT_METADATA_TABLE);
        }
    }
}
