package com.flinkfasc.coordinator.savepoint;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import org.apache.hc.core5.http.ParseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Manages Flink savepoint lifecycle via the Flink JobManager REST API.
 *
 * <p>Uses Apache HttpClient 5 for all HTTP interactions. The coordinator calls this
 * service during the {@code SAVEPOINT_IN_PROGRESS} phase of the handoff state machine.
 *
 * <h3>Flink REST endpoints used</h3>
 * <ul>
 *   <li>{@code POST /jobs/{jobId}/savepoints} — triggers an asynchronous savepoint</li>
 *   <li>{@code GET  /jobs/{jobId}/savepoints/{triggerId}} — polls for completion</li>
 *   <li>{@code GET  /jobs/{jobId}/vertices} — discovers the source vertex ID</li>
 *   <li>{@code GET  /jobs/{jobId}/vertices/{vertexId}/subtasks/metrics} — reads Kafka offsets</li>
 *   <li>{@code GET  /jobs/{jobId}} — checks job health (status == "RUNNING")</li>
 * </ul>
 */
@Service
public class SavepointManager {

    private static final Logger log = LoggerFactory.getLogger(SavepointManager.class);

    /** Maximum time to wait for a savepoint to complete before declaring failure. */
    private static final long SAVEPOINT_POLL_TIMEOUT_MS = 5 * 60 * 1_000L; // 5 minutes
    /** Polling interval between savepoint status checks. */
    private static final long SAVEPOINT_POLL_INTERVAL_MS = 2_000L;

    private final FascCoordinatorProperties props;
    private final ObjectMapper objectMapper;
    private final SavepointStore savepointStore;

    /**
     * Constructs a new {@code SavepointManager}.
     *
     * @param props          coordinator configuration properties
     * @param objectMapper   Jackson mapper for JSON parsing
     * @param savepointStore persistence store for savepoint metadata
     */
    public SavepointManager(FascCoordinatorProperties props,
                             ObjectMapper objectMapper,
                             SavepointStore savepointStore) {
        this.props          = props;
        this.objectMapper   = objectMapper;
        this.savepointStore = savepointStore;
    }

    // =========================================================================
    // Savepoint trigger & poll
    // =========================================================================

    /**
     * Triggers a savepoint for the given Flink job and polls until completion or failure.
     *
     * <p>The savepoint is written to the path:
     * {@code s3://<savepointS3Bucket>/<savepointS3Prefix><jobId>/}.
     *
     * <p>Polling continues for up to 5 minutes, checking every 2 seconds.
     * If the Flink API returns {@code "COMPLETED"} the resolved S3 location is returned.
     * If it returns {@code "FAILED"} or the timeout is exceeded, a
     * {@link SavepointTriggerException} is thrown.
     *
     * @param restBaseUrl base URL of the Flink JobManager REST API for the target job's cluster
     * @param jobId       Flink job ID (32-character hex string)
     * @param s3TargetDir S3 path (without trailing slash) to write the savepoint to
     * @return completed {@link SavepointMetadata} with S3 location and COMPLETED status
     * @throws SavepointTriggerException if the savepoint fails or times out
     */
    public SavepointMetadata triggerSavepoint(String restBaseUrl, String jobId, String s3TargetDir) {
        log.info("Triggering savepoint for job {} → {}", jobId, s3TargetDir);

        String triggerId = postSavepointTrigger(restBaseUrl, jobId, s3TargetDir);
        log.info("Savepoint trigger ID: {} for job {}", triggerId, jobId);

        String s3Location = pollUntilComplete(restBaseUrl, jobId, triggerId);
        log.info("Savepoint completed for job {}: {}", jobId, s3Location);

        return SavepointMetadata.builder()
                .savepointId(UUID.randomUUID().toString())
                .s3Location(s3Location)
                .sourceJobId(jobId)
                .takenAtMs(System.currentTimeMillis())
                .status(SavepointStatus.COMPLETED)
                .build();
    }

    /**
     * POST to Flink REST to trigger the savepoint.
     *
     * @return the Flink trigger ID (request-id) for polling
     */
    private String postSavepointTrigger(String restBaseUrl, String jobId, String s3TargetDir) {
        String url = restBaseUrl + "/jobs/" + jobId + "/savepoints";
        String body = "{\"target-directory\":\"" + s3TargetDir + "\",\"cancel-job\":false}";

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(url);
            post.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));

            try (CloseableHttpResponse response = client.execute(post)) {
                int statusCode = response.getCode();
                String responseBody = EntityUtils.toString(response.getEntity());

                if (statusCode != 202) {
                    throw new SavepointTriggerException(
                            "Savepoint trigger returned HTTP " + statusCode + ": " + responseBody);
                }

                JsonNode root = objectMapper.readTree(responseBody);
                String requestId = root.path("request-id").asText(null);
                if (requestId == null || requestId.isEmpty()) {
                    throw new SavepointTriggerException(
                            "Savepoint trigger response missing 'request-id': " + responseBody);
                }
                return requestId;
            }
        } catch (IOException | ParseException e) {
            throw new SavepointTriggerException("HTTP error triggering savepoint for job " + jobId, e);
        }
    }

    /**
     * Polls the Flink REST API until the savepoint is COMPLETED or FAILED,
     * or until the timeout is exceeded.
     *
     * <p>Expected response structure:
     * <pre>{@code
     * {
     *   "status": {"id": "COMPLETED"},
     *   "operation": {"location": "s3://bucket/prefix/..."}
     * }
     * }</pre>
     *
     * @return the resolved S3 location of the completed savepoint
     */
    private String pollUntilComplete(String restBaseUrl, String jobId, String triggerId) {
        String url = restBaseUrl + "/jobs/" + jobId + "/savepoints/" + triggerId;
        long deadline = System.currentTimeMillis() + SAVEPOINT_POLL_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            try {
                TimeUnit.MILLISECONDS.sleep(SAVEPOINT_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SavepointTriggerException("Interrupted while waiting for savepoint " + triggerId);
            }

            try (CloseableHttpClient client = HttpClients.createDefault()) {
                HttpGet get = new HttpGet(url);
                try (CloseableHttpResponse response = client.execute(get)) {
                    String body = EntityUtils.toString(response.getEntity());
                    JsonNode root = objectMapper.readTree(body);

                    String statusId = root.path("status").path("id").asText("IN_PROGRESS");
                    log.debug("Savepoint {} status: {}", triggerId, statusId);

                    if ("COMPLETED".equalsIgnoreCase(statusId)) {
                        String location = root.path("operation").path("location").asText(null);
                        if (location == null || location.isEmpty()) {
                            throw new SavepointTriggerException(
                                    "Savepoint COMPLETED but location missing in response: " + body);
                        }
                        return location;
                    }
                    if ("FAILED".equalsIgnoreCase(statusId)) {
                        String failureCause = root.path("operation")
                                .path("failure-cause").path("class").asText("unknown");
                        throw new SavepointTriggerException(
                                "Savepoint FAILED for job " + jobId + ": " + failureCause);
                    }
                    // IN_PROGRESS — keep polling
                }
            } catch (IOException | ParseException e) {
                log.warn("HTTP error polling savepoint {}, will retry: {}", triggerId, e.getMessage());
            }
        }

        throw new SavepointTriggerException(
                "Savepoint " + triggerId + " did not complete within "
                + (SAVEPOINT_POLL_TIMEOUT_MS / 1000) + " seconds");
    }

    // =========================================================================
    // Kafka offset discovery via Flink metrics
    // =========================================================================

    /**
     * Retrieves the per-partition Kafka source offsets committed by the given Flink job,
     * by querying the Flink REST metrics API.
     *
     * <p>The method first discovers the source vertex ID via {@code GET /jobs/{jobId}/vertices},
     * then reads per-subtask {@code KafkaSourceReader.currentOffset} metrics.
     *
     * @param restBaseUrl base URL of the Flink JobManager REST API
     * @param jobId       Flink job ID
     * @return map of partition index → last committed offset; empty map if unavailable
     */
    public Map<Integer, Long> getJobKafkaOffsets(String restBaseUrl, String jobId) {
        Map<Integer, Long> offsets = new HashMap<>();

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            String sourceVertexId = findSourceVertexId(client, restBaseUrl, jobId);
            if (sourceVertexId == null) {
                log.warn("No source vertex found for job {}", jobId);
                return offsets;
            }

            String metricsUrl = restBaseUrl
                    + "/jobs/" + jobId
                    + "/vertices/" + sourceVertexId
                    + "/subtasks/metrics?get=KafkaSourceReader.currentOffset";

            HttpGet get = new HttpGet(metricsUrl);
            try (CloseableHttpResponse response = client.execute(get)) {
                String body = EntityUtils.toString(response.getEntity());
                JsonNode root = objectMapper.readTree(body);

                // Response is an array: [{"id":"KafkaSourceReader.currentOffset","value":"42"}, ...]
                // The metric ID includes the partition number in practice, but here we parse
                // the combined aggregated format. For multi-partition support we use the
                // /subtasks/{subtaskIndex}/metrics endpoint per subtask.
                if (root.isArray()) {
                    for (JsonNode metric : root) {
                        String id = metric.path("id").asText("");
                        String value = metric.path("value").asText("0");
                        // Parse partition from metric id, e.g. "KafkaSourceReader.p0.currentOffset"
                        int partition = parsePartitionFromMetricId(id);
                        if (partition >= 0) {
                            offsets.put(partition, Long.parseLong(value));
                        }
                    }
                }
            }
        } catch (IOException | ParseException e) {
            log.error("Failed to retrieve Kafka offsets for job {}: {}", jobId, e.getMessage());
        }

        return offsets;
    }

    /**
     * Finds the first source operator vertex ID for a given Flink job.
     *
     * <p>Iterates over {@code GET /jobs/{jobId}/vertices} and returns the ID of the
     * first vertex whose name contains "Source" (case-insensitive).
     *
     * @param client      pre-created HTTP client
     * @param restBaseUrl Flink REST base URL
     * @param jobId       Flink job ID
     * @return vertex ID string or {@code null} if not found
     */
    private String findSourceVertexId(CloseableHttpClient client,
                                       String restBaseUrl,
                                       String jobId) throws IOException, ParseException {
        String url = restBaseUrl + "/jobs/" + jobId + "/vertices";
        HttpGet get = new HttpGet(url);

        try (CloseableHttpResponse response = client.execute(get)) {
            String body = EntityUtils.toString(response.getEntity());
            JsonNode root = objectMapper.readTree(body);
            JsonNode vertices = root.path("vertices");

            if (vertices.isArray()) {
                for (JsonNode vertex : vertices) {
                    String name = vertex.path("name").asText("");
                    if (name.toLowerCase().contains("source")) {
                        return vertex.path("id").asText(null);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Parses the partition index from a Flink Kafka metric ID.
     *
     * <p>Expected formats:
     * <ul>
     *   <li>{@code KafkaSourceReader.KafkaSourceReader-p0.currentOffset} → partition 0</li>
     *   <li>{@code KafkaSourceReader.currentOffset} → returns {@code -1} (aggregate, skip)</li>
     * </ul>
     *
     * @param metricId Flink metric identifier string
     * @return zero-based partition index, or {@code -1} if the ID is not partition-specific
     */
    private int parsePartitionFromMetricId(String metricId) {
        // Matches patterns like "...p0..." or "partition-0"
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("(?:partition[\\-_]|[\\-_]p)(\\d+)", java.util.regex.Pattern.CASE_INSENSITIVE)
                .matcher(metricId);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        }
        return -1;
    }

    // =========================================================================
    // High-level bootstrap
    // =========================================================================

    /**
     * Takes a savepoint of App1, records Kafka offsets at snapshot time, stores the
     * metadata in the savepoint store, and returns it to the caller.
     *
     * <p>The caller (typically {@link com.flinkfasc.coordinator.handoff.HandoffOrchestrator})
     * uses the returned metadata's S3 location to start App2 via a Flink REST submit call
     * or Kubernetes Job restart with the savepoint path injected as a startup parameter.
     *
     * @param app1JobId Flink job ID for App1
     * @return completed {@link SavepointMetadata} for App2 bootstrap; never {@code null}
     * @throws SavepointTriggerException if the savepoint fails
     */
    public SavepointMetadata bootstrapApp2FromApp1(String app1JobId) {
        String s3TargetDir = "s3://" + props.getSavepointS3Bucket()
                + "/" + props.getSavepointS3Prefix()
                + app1JobId + "/";

        // Step 1 — trigger and await savepoint on App1
        SavepointMetadata metadata = triggerSavepoint(props.getApp1FlinkRestUrl(), app1JobId, s3TargetDir);

        // Step 2 — capture Kafka offsets at savepoint time (best-effort enrichment)
        Map<Integer, Long> kafkaOffsets = getJobKafkaOffsets(props.getApp1FlinkRestUrl(), app1JobId);
        metadata = SavepointMetadata.builder()
                .savepointId(metadata.getSavepointId())
                .s3Location(metadata.getS3Location())
                .kafkaOffsets(kafkaOffsets)
                .sourceJobId(app1JobId)
                .targetJobId(props.getApp2JobId())
                .takenAtMs(metadata.getTakenAtMs())
                .status(SavepointStatus.COMPLETED)
                .build();

        // Step 3 — persist to DynamoDB for crash recovery
        if (savepointStore != null) {
            savepointStore.save(metadata);
        }

        log.info("App2 bootstrap savepoint ready: {} (offsets captured for {} partitions)",
                metadata.getS3Location(), kafkaOffsets.size());
        return metadata;
    }

    // =========================================================================
    // Health check
    // =========================================================================

    /**
     * Checks whether the Flink job is currently in {@code RUNNING} status.
     *
     * <p>Calls {@code GET /jobs/{jobId}} and checks the top-level {@code "state"} field.
     *
     * @param jobRestUrl base URL of the Flink JobManager REST API for this job's cluster
     * @param jobId      Flink job ID to check
     * @return {@code true} if the job state is {@code "RUNNING"}; {@code false} otherwise
     */
    public boolean isJobHealthy(String jobRestUrl, String jobId) {
        String url = jobRestUrl + "/jobs/" + jobId;
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet get = new HttpGet(url);
            try (CloseableHttpResponse response = client.execute(get)) {
                String body = EntityUtils.toString(response.getEntity());
                JsonNode root = objectMapper.readTree(body);
                String state = root.path("state").asText("");
                return "RUNNING".equalsIgnoreCase(state);
            }
        } catch (IOException | ParseException e) {
            log.warn("Health check failed for job {} at {}: {}", jobId, jobRestUrl, e.getMessage());
            return false;
        }
    }

    // =========================================================================
    // Exception type
    // =========================================================================

    /**
     * Unchecked exception thrown when a savepoint operation fails or times out.
     */
    public static class SavepointTriggerException extends RuntimeException {

        /** @param message error description */
        public SavepointTriggerException(String message) {
            super(message);
        }

        /**
         * @param message error description
         * @param cause   underlying cause
         */
        public SavepointTriggerException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
