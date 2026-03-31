package com.flinkfasc.coordinator.api;

import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import com.flinkfasc.coordinator.election.LeaderElection;
import com.flinkfasc.coordinator.handoff.HandoffOrchestrator;
import com.flinkfasc.coordinator.handoff.HandoffState;
import com.flinkfasc.coordinator.monitor.OffsetMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

/**
 * REST API for the FASC Coordinator.
 *
 * <p>Endpoints:
 * <ul>
 *   <li>GET  /fasc/status  — full coordinator + handoff status</li>
 *   <li>POST /fasc/handoff — trigger a handoff (returns 202 with traceId)</li>
 *   <li>GET  /fasc/health  — liveness probe (always 200)</li>
 *   <li>GET  /fasc/ready   — readiness probe (200 if lock holder + healthy)</li>
 * </ul>
 */
@RestController
@RequestMapping("/fasc")
public class FascApiController {

    private static final Logger LOG = LoggerFactory.getLogger(FascApiController.class);

    private final HandoffOrchestrator     orchestrator;
    private final OffsetMonitor           offsetMonitor;
    private final LeaderElection          leaderElection;
    private final FascCoordinatorProperties props;
    private final Instant                 startedAt = Instant.now();

    public FascApiController(HandoffOrchestrator orchestrator,
                             OffsetMonitor offsetMonitor,
                             LeaderElection leaderElection,
                             FascCoordinatorProperties props) {
        this.orchestrator  = orchestrator;
        this.offsetMonitor = offsetMonitor;
        this.leaderElection = leaderElection;
        this.props         = props;
    }

    /** Full status — current state, lag, leader, lock ownership. */
    @GetMapping("/status")
    public StatusResponse status() {
        StatusResponse resp = new StatusResponse();
        resp.setHandoffState(orchestrator.getState());
        resp.setCurrentFlinkLeader(leaderElection.getCurrentFlinkLeader());
        resp.setApp2MaxLagMs(offsetMonitor.getMaxLag());
        resp.setApp2Ready(offsetMonitor.isApp2Ready());
        resp.setCoordinatorId(props.getCoordinatorId());
        resp.setLockHolder(leaderElection.isLockHolder(props.getCoordinatorId()));
        resp.setUptimeMs(Instant.now().toEpochMilli() - startedAt.toEpochMilli());
        return resp;
    }

    /**
     * Trigger a handoff. Returns 202 Accepted immediately with a traceId.
     * The handoff runs asynchronously — poll /fasc/status to track progress.
     */
    @PostMapping("/handoff")
    public ResponseEntity<Map<String, String>> triggerHandoff(@RequestBody HandoffRequest request) {
        LOG.info("Handoff requested by={} reason={}", request.getRequestedBy(), request.getReason());
        try {
            orchestrator.triggerHandoff();
            String traceId = orchestrator.getActiveTraceId();
            return ResponseEntity.accepted()
                    .body(Map.of("status", "ACCEPTED", "traceId", traceId,
                                 "message", "Handoff initiated. Poll /fasc/status for progress."));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("status", "REJECTED", "reason", e.getMessage()));
        }
    }

    /** Kubernetes liveness probe — always returns 200 if the JVM is alive. */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    /**
     * Kubernetes readiness probe — 200 only if this coordinator holds the lock
     * and is in a healthy state. 503 otherwise (pod won't receive traffic).
     */
    @GetMapping("/ready")
    public ResponseEntity<Map<String, String>> ready() {
        boolean lockHolder = leaderElection.isLockHolder(props.getCoordinatorId());
        HandoffState currentState = orchestrator.getState();
        boolean healthy = lockHolder && currentState != HandoffState.HANDOFF_FAILED;

        if (healthy) {
            return ResponseEntity.ok(Map.of(
                    "status", "READY",
                    "lockHolder", String.valueOf(lockHolder),
                    "handoffState", currentState.name()));
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of(
                            "status", "NOT_READY",
                            "lockHolder", String.valueOf(lockHolder),
                            "handoffState", currentState.name()));
        }
    }
}
