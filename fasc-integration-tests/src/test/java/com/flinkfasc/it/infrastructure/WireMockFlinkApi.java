package com.flinkfasc.it.infrastructure;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

/**
 * WireMock-based mock for the Flink REST API.
 *
 * <p>The FASC {@code SavepointManager} calls the Flink REST API to:
 * <ol>
 *   <li>Trigger a savepoint: {@code POST /jobs/{jobId}/savepoints}</li>
 *   <li>Poll savepoint status: {@code GET /jobs/{jobId}/savepoints/{triggerId}}</li>
 *   <li>Get job Kafka offsets: {@code GET /jobs/{jobId}/vertices}</li>
 *   <li>Check job health: {@code GET /jobs/{jobId}}</li>
 * </ol>
 *
 * <p>This mock stubs all those endpoints with realistic responses so coordinator
 * integration tests don't need a running Flink cluster.
 *
 * <p>Usage:
 * <pre>{@code
 * WireMockFlinkApi mock = new WireMockFlinkApi();
 * mock.start();
 * // configure test URL: http://localhost:{mock.port()}
 * mock.stubSavepointSuccess("job-abc", "trigger-001", "s3://bucket/fasc/sp-001");
 * // ... run test ...
 * mock.stop();
 * }</pre>
 */
public class WireMockFlinkApi {

    private final WireMockServer server;

    public WireMockFlinkApi() {
        this.server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop();
    }

    public int port() {
        return server.port();
    }

    public String baseUrl() {
        return "http://localhost:" + server.port();
    }

    // ── Savepoint stubs ────────────────────────────────────────────────────────

    /**
     * Stubs a successful savepoint trigger + immediate completion.
     * POST /jobs/{jobId}/savepoints → 202 with triggerId
     * GET  /jobs/{jobId}/savepoints/{triggerId} → COMPLETED with s3Location
     */
    public void stubSavepointSuccess(String jobId, String triggerId, String s3Location) {
        // Trigger savepoint
        server.stubFor(post(urlEqualTo("/jobs/" + jobId + "/savepoints"))
                .willReturn(aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"request-id\": \"" + triggerId + "\"}")));

        // Poll status — immediately COMPLETED
        server.stubFor(get(urlEqualTo("/jobs/" + jobId + "/savepoints/" + triggerId))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {
                                  "status": { "id": "COMPLETED" },
                                  "operation": {
                                    "location": "%s"
                                  }
                                }
                                """.formatted(s3Location))));
    }

    /**
     * Stubs a savepoint that initially returns IN_PROGRESS then COMPLETED on second poll.
     */
    public void stubSavepointWithDelay(String jobId, String triggerId, String s3Location) {
        server.stubFor(post(urlEqualTo("/jobs/" + jobId + "/savepoints"))
                .willReturn(aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"request-id\": \"" + triggerId + "\"}")));

        server.stubFor(get(urlEqualTo("/jobs/" + jobId + "/savepoints/" + triggerId))
                .inScenario("savepoint-" + triggerId)
                .whenScenarioStateIs("Started")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"status\": {\"id\": \"IN_PROGRESS\"}}"))
                .willSetStateTo("Done"));

        server.stubFor(get(urlEqualTo("/jobs/" + jobId + "/savepoints/" + triggerId))
                .inScenario("savepoint-" + triggerId)
                .whenScenarioStateIs("Done")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {
                                  "status": { "id": "COMPLETED" },
                                  "operation": { "location": "%s" }
                                }
                                """.formatted(s3Location))));
    }

    /**
     * Stubs a savepoint that fails.
     */
    public void stubSavepointFailure(String jobId, String triggerId) {
        server.stubFor(post(urlEqualTo("/jobs/" + jobId + "/savepoints"))
                .willReturn(aResponse()
                        .withStatus(202)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"request-id\": \"" + triggerId + "\"}")));

        server.stubFor(get(urlEqualTo("/jobs/" + jobId + "/savepoints/" + triggerId))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {
                                  "status": { "id": "FAILED" },
                                  "operation": { "failure-cause": { "class": "java.io.IOException", "stack-trace": "Simulated failure" } }
                                }
                                """)));
    }

    // ── Job health stubs ───────────────────────────────────────────────────────

    /**
     * Stubs a healthy RUNNING job.
     */
    public void stubJobRunning(String jobId) {
        server.stubFor(get(urlEqualTo("/jobs/" + jobId))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {
                                  "jid": "%s",
                                  "name": "BusinessFlinkJob",
                                  "state": "RUNNING",
                                  "start-time": %d
                                }
                                """.formatted(jobId, System.currentTimeMillis() - 60_000))));
    }

    /**
     * Stubs a job vertices response for offset tracking.
     * Returns one source vertex with realistic Kafka offset metrics.
     */
    public void stubJobVerticesWithOffsets(String jobId, String vertexId, long offsetPerPartition) {
        server.stubFor(get(urlEqualTo("/jobs/" + jobId + "/vertices"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                                {
                                  "vertices": [
                                    { "id": "%s", "name": "MSK Source", "parallelism": 24 }
                                  ]
                                }
                                """.formatted(vertexId))));

        // Build metrics response with 24 partitions
        StringBuilder metricsBody = new StringBuilder("[");
        for (int i = 0; i < 24; i++) {
            if (i > 0) metricsBody.append(",");
            metricsBody.append("""
                    {"id":"KafkaSourceReader.currentOffset.partition-%d","value":"%d"}
                    """.formatted(i, offsetPerPartition + i));
        }
        metricsBody.append("]");

        server.stubFor(get(urlPathEqualTo("/jobs/" + jobId + "/vertices/" + vertexId + "/subtasks/metrics"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(metricsBody.toString())));
    }

    /** Resets all stubs — call between test cases if needed. */
    public void resetStubs() {
        server.resetAll();
    }
}
