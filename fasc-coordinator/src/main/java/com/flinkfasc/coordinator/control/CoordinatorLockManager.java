package com.flinkfasc.coordinator.control;

import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import com.flinkfasc.coordinator.election.LeaderElection;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Manages the coordinator-level DynamoDB distributed lock lifecycle.
 *
 * <h3>Purpose</h3>
 * Two coordinator replicas run on EKS for high availability. Only the one that holds
 * the DynamoDB lock acts as the active orchestrator; the other is a hot standby.
 * This component handles:
 * <ol>
 *   <li><b>Acquisition</b> — attempts to acquire the lock on startup; retries until
 *       successful or the JVM exits.</li>
 *   <li><b>Renewal</b> — extends the lock TTL on every heartbeat interval so that
 *       the lock is not reclaimed by the standby while the active coordinator is healthy.</li>
 *   <li><b>Loss detection</b> — if renewal fails, the coordinator closes the Spring
 *       application context gracefully so Kubernetes restarts the pod cleanly.</li>
 * </ol>
 *
 * <h3>TTL safety margin</h3>
 * The lock TTL is set to 30 s in {@link LeaderElection}. The renewal interval
 * ({@code fasc.heartbeatIntervalMs}, default 5 s) is well within the TTL, so a
 * transient DynamoDB hiccup (< 25 s) can be absorbed without losing the lock.
 *
 * <h3>Standby behaviour</h3>
 * If this replica does not acquire the lock on startup, it enters standby mode —
 * it still starts the control-topic consumer (to track heartbeats) but the
 * {@link com.flinkfasc.coordinator.handoff.HandoffOrchestrator} rejects
 * {@code triggerHandoff()} calls because {@link LeaderElection#isLockHolder} returns
 * {@code false}.  Standby replicas check for lock availability on every renewal tick
 * and promote themselves if the active coordinator's lock expires.
 */
@Component
public class CoordinatorLockManager {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLockManager.class);

    /** How long (ms) to wait between lock-acquisition retries at startup. */
    private static final long ACQUISITION_RETRY_MS = 5_000L;
    /** Maximum startup retry attempts before giving up and running as a standby. */
    private static final int  MAX_ACQUISITION_ATTEMPTS = 5;

    private final LeaderElection             leaderElection;
    private final FascCoordinatorProperties  props;
    private final ConfigurableApplicationContext applicationContext;

    /** Whether this replica currently holds the coordinator lock. */
    private volatile boolean lockHolder = false;

    /**
     * Constructs the lock manager.
     *
     * @param leaderElection     DynamoDB-backed leader/lock implementation
     * @param props              coordinator configuration (coordinatorId, heartbeat interval)
     * @param applicationContext Spring context used for graceful shutdown on lock loss
     */
    public CoordinatorLockManager(LeaderElection leaderElection,
                                  FascCoordinatorProperties props,
                                  ConfigurableApplicationContext applicationContext) {
        this.leaderElection     = leaderElection;
        this.props              = props;
        this.applicationContext = applicationContext;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Attempts to acquire the coordinator lock on startup.
     *
     * <p>Retries up to {@value #MAX_ACQUISITION_ATTEMPTS} times with a
     * {@value #ACQUISITION_RETRY_MS} ms pause between attempts. If the lock cannot be
     * acquired after all retries, this replica starts in standby mode — it will
     * promote itself on the next renewal tick once the active lock expires.
     */
    @PostConstruct
    public void acquireOnStartup() {
        String coordinatorId = props.getCoordinatorId();
        LOG.info("Attempting to acquire coordinator lock as coordinatorId={}", coordinatorId);

        for (int attempt = 1; attempt <= MAX_ACQUISITION_ATTEMPTS; attempt++) {
            if (leaderElection.tryAcquireLock(coordinatorId)) {
                lockHolder = true;
                LOG.info("Coordinator lock acquired on attempt {}/{} — this replica is ACTIVE", attempt, MAX_ACQUISITION_ATTEMPTS);
                return;
            }
            LOG.info("Lock acquisition attempt {}/{} failed — another coordinator holds the lock", attempt, MAX_ACQUISITION_ATTEMPTS);
            if (attempt < MAX_ACQUISITION_ATTEMPTS) {
                try {
                    Thread.sleep(ACQUISITION_RETRY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted during lock acquisition retry");
                    return;
                }
            }
        }

        LOG.warn("Could not acquire coordinator lock after {} attempts — starting as STANDBY. "
                + "Will promote when active coordinator lock expires.", MAX_ACQUISITION_ATTEMPTS);
    }

    /**
     * Periodically renews the coordinator lock and attempts to acquire it if this
     * replica is currently in standby mode.
     *
     * <p>If renewal fails for the active replica (e.g. DynamoDB returned a condition
     * failure meaning another coordinator stole the lock), the coordinator exits so
     * Kubernetes can restart it cleanly.
     */
    @Scheduled(fixedDelayString = "${fasc.heartbeatIntervalMs:5000}")
    public void renewOrPromote() {
        String coordinatorId = props.getCoordinatorId();

        if (lockHolder) {
            boolean renewed = leaderElection.renewLock(coordinatorId);
            if (!renewed) {
                LOG.error("COORDINATOR LOCK LOST for coordinatorId={} — another coordinator may have taken over. "
                        + "Initiating graceful shutdown so Kubernetes can restart this pod.", coordinatorId);
                lockHolder = false;
                // Close the Spring context gracefully; Kubernetes will restart the pod cleanly.
                // Using ApplicationContext.close() instead of System.exit() allows @PreDestroy
                // hooks and shutdown listeners to run (e.g. releasing Kafka consumers).
                applicationContext.close();
            }
            LOG.debug("Coordinator lock renewed for coordinatorId={}", coordinatorId);
        } else {
            // Standby replica — attempt to acquire if the active lock has expired
            if (leaderElection.tryAcquireLock(coordinatorId)) {
                lockHolder = true;
                LOG.info("Standby coordinator promoted to ACTIVE — lock acquired for coordinatorId={}", coordinatorId);
            }
        }
    }

    /**
     * Returns {@code true} if this coordinator replica currently holds the lock.
     * Used by health checks and the REST API ready-probe.
     *
     * @return {@code true} if this replica is the active coordinator
     */
    public boolean isLockHolder() {
        return lockHolder;
    }

    /**
     * Releases the coordinator lock on shutdown so the standby replica can promote
     * immediately rather than waiting for TTL expiry.
     */
    @PreDestroy
    public void releaseOnShutdown() {
        if (lockHolder) {
            LOG.info("Releasing coordinator lock on shutdown for coordinatorId={}", props.getCoordinatorId());
            leaderElection.releaseLock(props.getCoordinatorId());
            lockHolder = false;
        }
    }
}
