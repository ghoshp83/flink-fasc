package com.flinkfasc.coordinator.handoff;

import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import com.flinkfasc.coordinator.election.LeaderElection;
import com.flinkfasc.coordinator.monitor.OffsetMonitor;
import com.flinkfasc.coordinator.savepoint.SavepointManager;
import com.flinkfasc.coordinator.savepoint.SavepointMetadata;
import com.flinkfasc.coordinator.savepoint.SavepointStatus;
import com.flinkfasc.core.control.ControlSignal;
import com.flinkfasc.core.control.ControlTopicProducer;
import com.flinkfasc.core.control.SignalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Core state machine for FASC handoff coordination.
 *
 * <p>Manages the full lifecycle:
 * <ol>
 *   <li>Triggering a savepoint on App1 and bootstrapping App2 from it</li>
 *   <li>Waiting for App2 consumer lag to reach zero (via {@link OffsetMonitor})</li>
 *   <li>Sending PREPARE_HANDOFF and waiting for App1's DRAINED_AT signal</li>
 *   <li>Atomically transferring DynamoDB leadership via {@link LeaderElection}</li>
 *   <li>Sending PROMOTE to App2 and confirming with PROMOTED_CONFIRMED</li>
 * </ol>
 *
 * <p>Only one handoff may be in progress at a time.
 * Only the coordinator that holds the DynamoDB lock may initiate a handoff.
 */
@Service
public class HandoffOrchestrator {

    private static final Logger LOG = LoggerFactory.getLogger(HandoffOrchestrator.class);

    private static final long DRAIN_TIMEOUT_MS    = 60_000;
    private static final long PROMOTE_TIMEOUT_MS  = 30_000;

    private final FascCoordinatorProperties props;
    private final SavepointManager          savepointManager;
    private final OffsetMonitor             offsetMonitor;
    private final LeaderElection            leaderElection;
    private final ControlTopicProducer      controlProducer;

    private final AtomicReference<HandoffState> state      = new AtomicReference<>(HandoffState.IDLE);
    private volatile Map<Integer, Long>         cutoverOffsets = Collections.emptyMap();
    private volatile String                     activeTraceId  = null;
    private volatile Instant                    handoffStartedAt = null;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "fasc-handoff-scheduler"));
    private ScheduledFuture<?> drainTimeoutFuture;

    public HandoffOrchestrator(FascCoordinatorProperties props,
                               SavepointManager savepointManager,
                               OffsetMonitor offsetMonitor,
                               LeaderElection leaderElection,
                               ControlTopicProducer controlProducer) {
        this.props            = props;
        this.savepointManager = savepointManager;
        this.offsetMonitor    = offsetMonitor;
        this.leaderElection   = leaderElection;
        this.controlProducer  = controlProducer;
    }

    @PostConstruct
    public void initialize() {
        offsetMonitor.onApp2Ready(this::onApp2CaughtUp);
        long heartbeatIntervalMs = props.getHeartbeatIntervalMs() > 0
                ? props.getHeartbeatIntervalMs() : 5000;
        scheduler.scheduleWithFixedDelay(
                this::sendHeartbeats, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("HandoffOrchestrator initialised. State=IDLE, coordinatorId={}, heartbeatIntervalMs={}",
                 props.getCoordinatorId(), heartbeatIntervalMs);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /** Returns the current state machine state. */
    public HandoffState getState() {
        return state.get();
    }

    /** Returns the traceId of the currently active (or most recent) handoff. */
    public String getActiveTraceId() {
        return activeTraceId;
    }

    /**
     * Triggers a handoff. Only valid when state=IDLE and this coordinator holds the lock.
     *
     * @throws IllegalStateException if not IDLE or not lock holder
     */
    public void triggerHandoff() {
        if (!leaderElection.isLockHolder(props.getCoordinatorId())) {
            throw new IllegalStateException("This coordinator does not hold the lock — cannot initiate handoff");
        }
        if (!state.compareAndSet(HandoffState.IDLE, HandoffState.SAVEPOINT_IN_PROGRESS)) {
            throw new IllegalStateException("Handoff already in progress: " + state.get());
        }

        activeTraceId    = UUID.randomUUID().toString();
        handoffStartedAt = Instant.now();
        LOG.info("[{}] Handoff triggered. Transitioning IDLE → SAVEPOINT_IN_PROGRESS", activeTraceId);

        CompletableFuture.runAsync(this::executeSavepointPhase, scheduler)
                .exceptionally(ex -> { abortHandoff("Savepoint phase failed: " + ex.getMessage()); return null; });
    }

    /**
     * Handles an incoming {@link ControlSignal} from the fasc-control-topic.
     * Called by the coordinator's Kafka consumer.
     */
    public void onSignalReceived(ControlSignal signal) {
        // Ignore signals not related to the active trace
        if (activeTraceId != null &&
            signal.getTraceId() != null &&
            !signal.getTraceId().equals(activeTraceId) &&
            signal.getSignalType() != SignalType.HEARTBEAT) {
            LOG.debug("Ignoring signal with stale traceId={} (active={})", signal.getTraceId(), activeTraceId);
            return;
        }

        LOG.info("[{}] Received signal: type={} from={}", activeTraceId,
                 signal.getSignalType(), signal.getSourceAppId());

        switch (signal.getSignalType()) {
            case DRAINED_AT:
                handleDrainedAt(signal);
                break;
            case PROMOTED_CONFIRMED:
                handlePromotedConfirmed(signal);
                break;
            default:
                LOG.debug("Orchestrator ignoring signal type: {}", signal.getSignalType());
        }
    }

    /** Called by drain timeout scheduler if App1 doesn't drain in time. */
    public void onDrainTimeout() {
        LOG.error("[{}] App1 drain timeout after {} ms. Aborting handoff.", activeTraceId, DRAIN_TIMEOUT_MS);
        abortHandoff("App1 drain timeout");
    }

    // -------------------------------------------------------------------------
    // Package-private for testing
    // -------------------------------------------------------------------------

    /** For testing: directly fires the App2-ready callback to advance the state machine. */
    public void simulateApp2ReadyForTest() {
        onApp2CaughtUp();
    }

    public void setStateForTest(HandoffState newState, Map<Integer, Long> offsets, String traceId) {
        this.state.set(newState);
        this.cutoverOffsets   = offsets;
        this.activeTraceId    = traceId;
        if (this.handoffStartedAt == null) {
            this.handoffStartedAt = Instant.now();
        }
    }

    // -------------------------------------------------------------------------
    // State machine transitions (private)
    // -------------------------------------------------------------------------

    private void executeSavepointPhase() {
        try {
            LOG.info("[{}] Taking savepoint of App1 (jobId={})", activeTraceId, props.getApp1JobId());
            SavepointMetadata meta = savepointManager.bootstrapApp2FromApp1(props.getApp1JobId());

            if (meta == null || meta.getStatus() != SavepointStatus.COMPLETED) {
                abortHandoff("Savepoint did not complete successfully");
                return;
            }

            LOG.info("[{}] Savepoint complete: {}. Transitioning → WAITING_FOR_APP2_CATCHUP",
                     activeTraceId, meta.getS3Location());
            state.set(HandoffState.WAITING_FOR_APP2_CATCHUP);
            offsetMonitor.resetReadiness();

        } catch (Exception e) {
            abortHandoff("Exception during savepoint phase: " + e.getMessage());
        }
    }

    private void onApp2CaughtUp() {
        if (!state.compareAndSet(HandoffState.WAITING_FOR_APP2_CATCHUP, HandoffState.HANDOFF_INITIATED)) {
            LOG.warn("[{}] App2 ready callback fired but state is {} — ignoring", activeTraceId, state.get());
            return;
        }

        // Capture current App1 offsets as the agreed cutover point
        cutoverOffsets = offsetMonitor.getCurrentLag().isEmpty()
                ? Collections.emptyMap()
                : buildCurrentOffsetsSnapshot();

        LOG.info("[{}] App2 caught up. Transitioning → HANDOFF_INITIATED. " +
                 "Sending PREPARE_HANDOFF with cutoverOffsets={}", activeTraceId, cutoverOffsets);

        controlProducer.sendSync(ControlSignal.prepareHandoff(
                props.getCoordinatorId(), activeTraceId, cutoverOffsets));

        state.set(HandoffState.WAITING_FOR_APP1_DRAIN);
        LOG.info("[{}] Transitioning → WAITING_FOR_APP1_DRAIN. Drain timeout={}ms",
                 activeTraceId, DRAIN_TIMEOUT_MS);

        // Schedule drain timeout
        drainTimeoutFuture = scheduler.schedule(this::onDrainTimeout, DRAIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void handleDrainedAt(ControlSignal signal) {
        if (state.get() != HandoffState.WAITING_FOR_APP1_DRAIN) {
            LOG.warn("[{}] Received DRAINED_AT but state is {} — ignoring", activeTraceId, state.get());
            return;
        }
        if (drainTimeoutFuture != null) drainTimeoutFuture.cancel(false);

        LOG.info("[{}] App1 drained at offsets={}. Transitioning → PROMOTING_APP2",
                 activeTraceId, signal.getCutoverOffsets());
        state.set(HandoffState.PROMOTING_APP2);
        promoteApp2();
    }

    private void promoteApp2() {
        int currentVersion = leaderElection.getCurrentFlinkLeaderVersion();
        boolean transferred = leaderElection.transferFlinkLeader("app1", "app2", currentVersion);

        if (!transferred) {
            abortHandoff("DynamoDB leader transfer failed — version mismatch or concurrent update");
            return;
        }

        LOG.info("[{}] DynamoDB leader transferred app1→app2. Sending PROMOTE + DEMOTE signals.", activeTraceId);

        controlProducer.sendSync(ControlSignal.promote(props.getCoordinatorId(), activeTraceId, "app2"));
        controlProducer.sendSync(ControlSignal.demote(props.getCoordinatorId(), activeTraceId, "app1"));

        // Schedule promotion confirmation timeout
        scheduler.schedule(() -> {
            if (state.get() == HandoffState.PROMOTING_APP2) {
                LOG.warn("[{}] No PROMOTED_CONFIRMED received within {}ms — " +
                         "assuming App2 promoted successfully (DynamoDB write already succeeded).",
                         activeTraceId, PROMOTE_TIMEOUT_MS);
                finaliseHandoff();
            }
        }, PROMOTE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void handlePromotedConfirmed(ControlSignal signal) {
        if (state.get() != HandoffState.PROMOTING_APP2) {
            LOG.warn("[{}] Received PROMOTED_CONFIRMED but state is {} — ignoring",
                     activeTraceId, state.get());
            return;
        }
        LOG.info("[{}] PROMOTED_CONFIRMED received from App2.", activeTraceId);
        finaliseHandoff();
    }

    private void finaliseHandoff() {
        long durationMs = Instant.now().toEpochMilli() - handoffStartedAt.toEpochMilli();
        state.set(HandoffState.HANDOFF_COMPLETE);
        LOG.info("[{}] HANDOFF COMPLETE in {}ms. App2 is now the active DynamoDB writer.",
                 activeTraceId, durationMs);

        // Reset to IDLE after a short cooldown so the next handoff can be triggered
        scheduler.schedule(() -> {
            state.compareAndSet(HandoffState.HANDOFF_COMPLETE, HandoffState.IDLE);
            LOG.info("[{}] State reset to IDLE. Ready for next handoff.", activeTraceId);
        }, 5, TimeUnit.SECONDS);
    }

    private void abortHandoff(String reason) {
        LOG.error("[{}] HANDOFF ABORTED: {}", activeTraceId, reason);
        state.set(HandoffState.HANDOFF_FAILED);
        try {
            controlProducer.send(ControlSignal.abort(props.getCoordinatorId(), activeTraceId, reason));
        } catch (Exception e) {
            LOG.error("[{}] Failed to send HANDOFF_ABORT signal", activeTraceId, e);
        }
    }

    private Map<Integer, Long> buildCurrentOffsetsSnapshot() {
        // Returns the current App1 committed offsets as the cutover point
        // In production this would call savepointManager.getJobKafkaOffsets(app1JobId)
        return Collections.unmodifiableMap(offsetMonitor.getCurrentLag());
    }

    public void sendHeartbeats() {
        if (leaderElection.isLockHolder(props.getCoordinatorId())) {
            controlProducer.send(ControlSignal.heartbeat(props.getCoordinatorId()));
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
