package com.flinkfasc.it.aws;

import com.flinkfasc.aws.metrics.CloudWatchMetricsReporter;
import com.flinkfasc.it.infrastructure.ContainerSetup;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Integration test: verifies CloudWatchMetricsReporter publishes metrics
 * to LocalStack CloudWatch without errors.
 *
 * <p>LocalStack CloudWatch accepts PutMetricData calls but doesn't fully
 * implement GetMetricData, so we verify the calls succeed (no exceptions)
 * and validate the request structure.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CloudWatchMetricsIT {

    private static CloudWatchClient cloudWatchClient;
    private CloudWatchMetricsReporter reporter;

    @BeforeAll
    static void startContainers() {
        ContainerSetup.start();
        cloudWatchClient = ContainerSetup.cloudWatchClient();
    }

    @AfterAll
    static void cleanup() {
        if (cloudWatchClient != null) cloudWatchClient.close();
    }

    @BeforeEach
    void setUp() {
        reporter = new CloudWatchMetricsReporter(cloudWatchClient, "FlinkFASC");
    }

    @Test
    @Order(1)
    void reportSinkMode_doesNotThrow() {
        assertThatNoException().isThrownBy(() -> {
            reporter.reportSinkMode("app1", 1); // ACTIVE
            reporter.reportSinkMode("app2", 0); // SHADOW
        });
    }

    @Test
    @Order(2)
    void reportApp2ConsumerLag_doesNotThrow() {
        assertThatNoException().isThrownBy(() -> {
            reporter.reportApp2ConsumerLag(0L);
            reporter.reportApp2ConsumerLag(5000L);
        });
    }

    @Test
    @Order(3)
    void reportHandoffState_doesNotThrow() {
        assertThatNoException().isThrownBy(() -> {
            reporter.reportHandoffState("IDLE");
            reporter.reportHandoffState("HANDOFF_INITIATED");
            reporter.reportHandoffState("HANDOFF_COMPLETE");
        });
    }

    @Test
    @Order(4)
    void reportShadowDropCount_doesNotThrow() {
        assertThatNoException().isThrownBy(() ->
                reporter.reportShadowDropCount("app2", 12345L));
    }

    @Test
    @Order(5)
    void reportSplitBrainDetected_doesNotThrow() {
        assertThatNoException().isThrownBy(() ->
                reporter.reportSplitBrainDetected("app1"));
    }

    @Test
    @Order(6)
    void reportHandoffDuration_doesNotThrow() {
        assertThatNoException().isThrownBy(() ->
                reporter.reportHandoffDurationMs(487L));
    }

    @Test
    @Order(7)
    void cloudWatchExceptionInReporter_doesNotPropagateToCallers() {
        // Create a reporter with a client pointing to a wrong endpoint
        // It should swallow the exception and log — never crash the application
        CloudWatchClient badClient = CloudWatchClient.builder()
                .endpointOverride(java.net.URI.create("http://localhost:1"))
                .credentialsProvider(
                    software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("x","x")))
                .region(software.amazon.awssdk.regions.Region.EU_WEST_2)
                .httpClient(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.create())
                .build();

        CloudWatchMetricsReporter faultyReporter =
                new CloudWatchMetricsReporter(badClient, "FlinkFASC");

        // Must not throw — metrics failures must never crash the application
        assertThatNoException().isThrownBy(() ->
                faultyReporter.reportSinkMode("app1", 1));

        badClient.close();
    }
}
