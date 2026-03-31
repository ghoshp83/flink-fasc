package com.flinkfasc.aws.config;

import com.flinkfasc.aws.election.DynamoDbLeaderElection;
import com.flinkfasc.aws.metrics.CloudWatchMetricsReporter;
import com.flinkfasc.aws.savepoint.S3SavepointStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Duration;

/**
 * Spring {@link Configuration} that creates all AWS SDK v2 client {@link Bean}s required
 * by the FASC coordinator.
 *
 * <h3>Activation</h3>
 * All beans in this class are guarded by:
 * <pre>
 *   fasc.aws.enabled=true   (application.yml or environment variable FASC_AWS_ENABLED=true)
 * </pre>
 * When running integration tests without real AWS credentials, set the property to
 * {@code false} (or omit it, since the default is {@code false}) and supply mock beans
 * in a {@code @TestConfiguration}.
 *
 * <h3>Dependency direction</h3>
 * {@code fasc-aws} must not depend on {@code fasc-coordinator} at compile time (the
 * coordinator module depends on this one). The {@link FascAwsProperties} interface defined
 * in this package mirrors the subset of {@code FascCoordinatorProperties} required by AWS
 * bean construction. In the coordinator's Spring context, {@code FascCoordinatorProperties}
 * implements {@code FascAwsProperties} so the beans are wired transparently.
 *
 * <h3>Retry configuration</h3>
 * DynamoDB client is configured with up to 3 retries using equal-jitter exponential
 * back-off (base 100 ms, max 5 s) to handle transient throttling without overwhelming
 * the service.
 */
@Configuration
@ConditionalOnProperty(name = "fasc.aws.enabled", havingValue = "true")
public class AwsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(AwsConfig.class);

    /** Default CloudWatch metrics namespace. */
    private static final String CLOUDWATCH_NAMESPACE = "FlinkFASC";

    /** Maximum number of DynamoDB SDK retries (on top of the initial attempt). */
    private static final int DYNAMODB_MAX_RETRIES = 3;

    /** Base back-off duration for the first retry interval. */
    private static final Duration RETRY_BASE_DELAY = Duration.ofMillis(100);

    /** Maximum back-off cap to prevent runaway retry delays. */
    private static final Duration RETRY_MAX_DELAY = Duration.ofSeconds(5);

    // -------------------------------------------------------------------------
    // AWS client beans
    // -------------------------------------------------------------------------

    /**
     * Creates the DynamoDB synchronous client used for coordinator lock operations,
     * leader-table writes, and savepoint metadata persistence.
     *
     * <p>Uses the {@link DefaultCredentialsProvider} chain (IAM role → ECS task role →
     * EC2 instance profile → environment variables → shared credentials file) so the
     * service works on EKS/ECS without hardcoded credentials.
     *
     * @param props the AWS configuration properties supplying the target region
     * @return a fully-configured {@link DynamoDbClient}
     */
    @Bean
    public DynamoDbClient dynamoDbClient(FascAwsProperties props) {
        String regionName = props.getAwsRegion();
        LOG.info("Creating DynamoDbClient for region={} maxRetries={}", regionName, DYNAMODB_MAX_RETRIES);

        RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(DYNAMODB_MAX_RETRIES)
                .backoffStrategy(
                    EqualJitterBackoffStrategy.builder()
                        .baseDelay(RETRY_BASE_DELAY)
                        .maxBackoffTime(RETRY_MAX_DELAY)
                        .build()
                )
                .build();

        return DynamoDbClient.builder()
                .region(Region.of(regionName))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(c -> c.retryPolicy(retryPolicy))
                .build();
    }

    /**
     * Creates the CloudWatch synchronous client used for metrics publication.
     *
     * <p>Uses the default AWS credentials provider chain. CloudWatch does not require the
     * same retry tuning as DynamoDB — the SDK default retry policy is sufficient since
     * metric loss is acceptable.
     *
     * @param props the AWS configuration properties supplying the target region
     * @return a fully-configured {@link CloudWatchClient}
     */
    @Bean
    public CloudWatchClient cloudWatchClient(FascAwsProperties props) {
        String regionName = props.getAwsRegion();
        LOG.info("Creating CloudWatchClient for region={}", regionName);

        return CloudWatchClient.builder()
                .region(Region.of(regionName))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    // -------------------------------------------------------------------------
    // FASC service beans
    // -------------------------------------------------------------------------

    /**
     * Creates the {@link DynamoDbLeaderElection} bean that handles coordinator mutual
     * exclusion and Flink leader tracking.
     *
     * <p>Table names are read from {@link FascAwsProperties}:
     * <ul>
     *   <li>{@link FascAwsProperties#getLeaderTable()} — active Flink app tracking</li>
     *   <li>{@link FascAwsProperties#getLockTable()} — coordinator lock</li>
     * </ul>
     *
     * @param client pre-created {@link DynamoDbClient} bean
     * @param props  configuration properties
     * @return a ready-to-use {@link DynamoDbLeaderElection} instance
     */
    @Bean
    public DynamoDbLeaderElection dynamoDbLeaderElection(DynamoDbClient client,
                                                          FascAwsProperties props) {
        LOG.info("Creating DynamoDbLeaderElection leaderTable={} lockTable={}",
                 props.getLeaderTable(), props.getLockTable());
        return new DynamoDbLeaderElection(client, props.getLeaderTable(), props.getLockTable());
    }

    /**
     * Creates the {@link CloudWatchMetricsReporter} bean that publishes FASC metrics.
     *
     * <p>The metrics namespace is fixed to {@value #CLOUDWATCH_NAMESPACE} so that all FASC
     * deployments share a consistent namespace in CloudWatch dashboards and alarms.
     *
     * @param client pre-created {@link CloudWatchClient} bean
     * @return a ready-to-use {@link CloudWatchMetricsReporter} instance
     */
    @Bean
    public CloudWatchMetricsReporter cloudWatchMetricsReporter(CloudWatchClient client) {
        LOG.info("Creating CloudWatchMetricsReporter namespace={}", CLOUDWATCH_NAMESPACE);
        return new CloudWatchMetricsReporter(client, CLOUDWATCH_NAMESPACE);
    }

    /**
     * Creates the {@link S3SavepointStorage} bean that generates S3 savepoint paths and
     * persists savepoint metadata in DynamoDB.
     *
     * @param client pre-created {@link DynamoDbClient} bean
     * @return a ready-to-use {@link S3SavepointStorage} instance
     */
    @Bean
    public S3SavepointStorage s3SavepointStorage(DynamoDbClient client) {
        LOG.info("Creating S3SavepointStorage");
        return new S3SavepointStorage(client);
    }
}
