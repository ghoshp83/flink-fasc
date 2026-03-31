package com.flinkfasc.aws.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.CloudWatchException;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Publishes FASC operational metrics to Amazon CloudWatch using AWS SDK v2.
 *
 * <p>All metrics are published under the configurable namespace (default: {@code "FlinkFASC"}).
 * Every {@code report*} method sends a single {@code PutMetricData} call. Failures are
 * caught, logged at ERROR level, and <em>not</em> re-thrown — metrics must never cause the
 * calling application to crash.
 *
 * <h3>Metric catalogue</h3>
 * <table border="1">
 *   <tr><th>Metric name</th><th>Unit</th><th>Dimension(s)</th><th>Description</th></tr>
 *   <tr><td>SinkMode</td><td>None</td><td>AppId</td><td>0 = SHADOW, 1 = ACTIVE</td></tr>
 *   <tr><td>App2ConsumerLagMs</td><td>Milliseconds</td><td>—</td><td>App2 consumer group lag behind App1</td></tr>
 *   <tr><td>HandoffState</td><td>None</td><td>State</td><td>Ordinal of the current HandoffState enum value</td></tr>
 *   <tr><td>ShadowDropCount</td><td>Count</td><td>AppId</td><td>Number of records dropped in SHADOW mode</td></tr>
 *   <tr><td>SplitBrainDetected</td><td>Count</td><td>AppId</td><td>Always 1.0 when published; alerts on any value</td></tr>
 *   <tr><td>HandoffDurationMs</td><td>Milliseconds</td><td>—</td><td>Wall-clock duration of the last handoff</td></tr>
 * </table>
 */
public class CloudWatchMetricsReporter {

    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchMetricsReporter.class);

    private final CloudWatchClient cloudWatchClient;
    private final String namespace;

    /**
     * Constructs a {@code CloudWatchMetricsReporter}.
     *
     * @param cloudWatchClient pre-configured AWS SDK v2 CloudWatch synchronous client; must not be null
     * @param namespace        CloudWatch metrics namespace (e.g. {@code "FlinkFASC"})
     */
    public CloudWatchMetricsReporter(CloudWatchClient cloudWatchClient, String namespace) {
        this.cloudWatchClient = Objects.requireNonNull(cloudWatchClient, "cloudWatchClient must not be null");
        this.namespace        = Objects.requireNonNull(namespace,        "namespace must not be null");
    }

    // -------------------------------------------------------------------------
    // Public reporting methods
    // -------------------------------------------------------------------------

    /**
     * Reports the current sink mode for a Flink application.
     *
     * <p>Publishes the {@code SinkMode} metric with dimension {@code AppId=appId}.
     * The value encodes the mode as an integer: {@code 0} = SHADOW, {@code 1} = ACTIVE.
     *
     * @param appId unique application identifier (e.g. {@code "app1"} or {@code "app2"})
     * @param mode  sink mode ordinal — {@code 0} for SHADOW, {@code 1} for ACTIVE
     */
    public void reportSinkMode(String appId, int mode) {
        LOG.debug("Reporting SinkMode={} for appId={}", mode, appId);
        publish(
            datum("SinkMode", mode, StandardUnit.NONE,
                  dimension("AppId", appId))
        );
    }

    /**
     * Reports the App2 consumer group lag in milliseconds relative to App1's position.
     *
     * <p>Publishes the {@code App2ConsumerLagMs} metric with no additional dimensions.
     * Monitor this value to determine when App2 has caught up sufficiently for a safe handoff.
     *
     * @param lagMs consumer group lag in milliseconds; must be &gt;= 0
     */
    public void reportApp2ConsumerLag(long lagMs) {
        LOG.debug("Reporting App2ConsumerLagMs={}", lagMs);
        publish(
            datum("App2ConsumerLagMs", lagMs, StandardUnit.MILLISECONDS)
        );
    }

    /**
     * Reports the current handoff state machine state.
     *
     * <p>Publishes the {@code HandoffState} metric. The value is the ordinal of the
     * {@code HandoffState} enum value; the {@code State} dimension holds the human-readable
     * name for easier CloudWatch Insights queries.
     *
     * @param state the name of the current {@code HandoffState} enum constant
     *              (e.g. {@code "IDLE"}, {@code "DRAINING"}, {@code "PROMOTING"})
     */
    public void reportHandoffState(String state) {
        // Derive the numeric ordinal from the enum name for consistent metric semantics.
        // If the state string doesn't match a known HandoffState, fall back to -1.
        int ordinal = HandoffStateOrdinal.ordinalOf(state);
        LOG.debug("Reporting HandoffState={} (ordinal={})", state, ordinal);
        publish(
            datum("HandoffState", ordinal, StandardUnit.NONE,
                  dimension("State", state))
        );
    }

    /**
     * Reports the cumulative number of records dropped in SHADOW mode for an application.
     *
     * <p>Publishes the {@code ShadowDropCount} metric with dimension {@code AppId=appId}.
     * A high and growing count is expected when an application is in SHADOW mode; an
     * unexpected drop count for the ACTIVE application indicates a configuration error.
     *
     * @param appId unique application identifier
     * @param count number of records dropped since the last report period
     */
    public void reportShadowDropCount(String appId, long count) {
        LOG.debug("Reporting ShadowDropCount={} for appId={}", count, appId);
        publish(
            datum("ShadowDropCount", count, StandardUnit.COUNT,
                  dimension("AppId", appId))
        );
    }

    /**
     * Reports that a split-brain condition was detected for the given application.
     *
     * <p>Publishes the {@code SplitBrainDetected} metric with value {@code 1.0} and
     * dimension {@code AppId=appId}. A CloudWatch alarm on any non-zero value of this
     * metric should be wired to an immediate alerting path.
     *
     * @param appId the application that detected the split-brain condition
     */
    public void reportSplitBrainDetected(String appId) {
        LOG.warn("Split-brain detected for appId={} — publishing CloudWatch alarm metric", appId);
        publish(
            datum("SplitBrainDetected", 1.0, StandardUnit.COUNT,
                  dimension("AppId", appId))
        );
    }

    /**
     * Reports the wall-clock duration of the most recent handoff in milliseconds.
     *
     * <p>Publishes the {@code HandoffDurationMs} metric with no additional dimensions.
     * Use this to track handoff performance against the 2-second SLA.
     *
     * @param durationMs handoff duration in milliseconds from PREPARE_HANDOFF to PROMOTED_CONFIRMED
     */
    public void reportHandoffDurationMs(long durationMs) {
        LOG.debug("Reporting HandoffDurationMs={}", durationMs);
        publish(
            datum("HandoffDurationMs", durationMs, StandardUnit.MILLISECONDS)
        );
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Sends a single {@link MetricDatum} to CloudWatch.
     *
     * <p>Wraps the call in a try-catch so that any {@link CloudWatchException} or
     * unexpected runtime error is logged and swallowed — metric publishing must never
     * cause the caller to fail.
     *
     * @param metricDatum the datum to publish
     */
    private void publish(MetricDatum metricDatum) {
        try {
            PutMetricDataRequest request = PutMetricDataRequest.builder()
                    .namespace(namespace)
                    .metricData(List.of(metricDatum))
                    .build();
            cloudWatchClient.putMetricData(request);
            LOG.debug("Published metric: {}", metricDatum.metricName());
        } catch (CloudWatchException e) {
            LOG.error("CloudWatch PutMetricData failed for metric={} namespace={}: {} ({})",
                      metricDatum.metricName(), namespace, e.getMessage(), e.awsErrorDetails().errorCode(), e);
        } catch (Exception e) {
            LOG.error("Unexpected error publishing metric={} namespace={}: {}",
                      metricDatum.metricName(), namespace, e.getMessage(), e);
        }
    }

    /**
     * Builds a {@link MetricDatum} with a {@code long} value and optional dimensions.
     */
    private static MetricDatum datum(String metricName, long value, StandardUnit unit, Dimension... dimensions) {
        return datum(metricName, (double) value, unit, dimensions);
    }

    /**
     * Builds a {@link MetricDatum} with an {@code int} value and optional dimensions.
     */
    private static MetricDatum datum(String metricName, int value, StandardUnit unit, Dimension... dimensions) {
        return datum(metricName, (double) value, unit, dimensions);
    }

    /**
     * Builds a {@link MetricDatum} with a {@code double} value and optional dimensions.
     */
    private static MetricDatum datum(String metricName, double value, StandardUnit unit, Dimension... dimensions) {
        MetricDatum.Builder builder = MetricDatum.builder()
                .metricName(metricName)
                .value(value)
                .unit(unit)
                .timestamp(Instant.now());

        if (dimensions != null && dimensions.length > 0) {
            builder.dimensions(dimensions);
        }

        return builder.build();
    }

    /**
     * Creates a CloudWatch {@link Dimension}.
     *
     * @param name  dimension name
     * @param value dimension value
     * @return a new {@link Dimension} instance
     */
    private static Dimension dimension(String name, String value) {
        return Dimension.builder().name(name).value(value).build();
    }

    // -------------------------------------------------------------------------
    // Inner helper: HandoffState ordinal mapping
    // -------------------------------------------------------------------------

    /**
     * Maps HandoffState name strings to integer ordinals without introducing a compile-time
     * dependency on the coordinator module's enum class.
     *
     * <p>The mapping must stay in sync with the {@code HandoffState} enum defined in
     * {@code fasc-coordinator}.
     */
    private static final class HandoffStateOrdinal {

        private HandoffStateOrdinal() {}

        /**
         * Returns the ordinal value for a given HandoffState name.
         *
         * @param stateName name of the HandoffState enum constant
         * @return the ordinal (0-based), or {@code -1} if the name is unrecognised
         */
        static int ordinalOf(String stateName) {
            if (stateName == null) {
                return -1;
            }
            switch (stateName.toUpperCase()) {
                case "IDLE":       return 0;
                case "PREPARING":  return 1;
                case "DRAINING":   return 2;
                case "PROMOTING":  return 3;
                case "DEMOTING":   return 4;
                case "COMPLETED":  return 5;
                case "ABORTED":    return 6;
                default:           return -1;
            }
        }
    }
}
