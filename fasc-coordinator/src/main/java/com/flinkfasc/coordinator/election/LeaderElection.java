package com.flinkfasc.coordinator.election;

/**
 * Distributed leader-election abstraction backed by DynamoDB conditional writes.
 *
 * <p>Two distinct leadership concepts are managed here:
 * <ol>
 *   <li><b>Coordinator lock</b> — determines which coordinator pod (out of two EKS replicas)
 *       is the active orchestrator that drives the handoff state machine.  Only one pod
 *       executes handoff logic at a time; the other is a hot standby.</li>
 *   <li><b>Flink app leader</b> — determines which Flink application (App1 or App2) is
 *       currently allowed to write to DynamoDB (i.e., whose {@code ShadowSink} is in
 *       {@code ACTIVE} mode).  The atomic {@link #transferFlinkLeader} method uses a
 *       DynamoDB conditional expression to guarantee exactly-once transfer.</li>
 * </ol>
 *
 * <h3>DynamoDB schema — coordinator lock table</h3>
 * <pre>
 *   PK: "COORDINATOR_LOCK" (String)
 *   coordinatorId: String
 *   ttl: Number (epoch seconds, auto-renewed by the lock holder)
 * </pre>
 *
 * <h3>DynamoDB schema — Flink leader table</h3>
 * <pre>
 *   PK: "FLINK_LEADER" (String)
 *   activeAppId: String   ("app1" | "app2")
 *   version: Number       (monotonically increasing, used for optimistic concurrency)
 * </pre>
 */
public interface LeaderElection {

    // =========================================================================
    // Coordinator lock operations
    // =========================================================================

    /**
     * Attempts to acquire the coordinator distributed lock for the given coordinator ID.
     *
     * <p>Uses a DynamoDB {@code PutItem} with a {@code attribute_not_exists(PK)} condition
     * so that only one coordinator can hold the lock at a time.  If the existing lock's TTL
     * has expired, the item is overwritten.
     *
     * @param coordinatorId stable identifier of this coordinator pod (e.g. the Pod hostname)
     * @return {@code true} if the lock was acquired; {@code false} if another coordinator
     *         already holds the lock and its TTL has not expired
     */
    boolean tryAcquireLock(String coordinatorId);

    /**
     * Renews the TTL on an already-held coordinator lock.
     *
     * <p>Uses a DynamoDB {@code UpdateItem} with a condition that verifies the
     * {@code coordinatorId} attribute still matches — prevents a restarted pod from
     * inadvertently extending a lock it no longer holds.
     *
     * @param coordinatorId identifier of the coordinator that currently holds the lock
     * @return {@code true} if the TTL was successfully refreshed; {@code false} if the lock
     *         was lost (e.g. expired and acquired by a competing pod) between renewals
     */
    boolean renewLock(String coordinatorId);

    /**
     * Releases the coordinator lock held by the given coordinator ID.
     *
     * <p>Uses a conditional {@code DeleteItem}; no-ops if the lock is already held by
     * a different coordinator (race-condition safety).
     *
     * @param coordinatorId identifier of the coordinator releasing the lock
     */
    void releaseLock(String coordinatorId);

    /**
     * Returns whether the given coordinator ID currently holds the coordinator lock.
     *
     * <p>Performs a {@code GetItem} and checks both the {@code coordinatorId} attribute
     * and that the TTL has not expired.
     *
     * @param coordinatorId coordinator identifier to check
     * @return {@code true} if this coordinator is the current lock holder
     */
    boolean isLockHolder(String coordinatorId);

    // =========================================================================
    // Flink app leader operations
    // =========================================================================

    /**
     * Atomically transfers the Flink application leader role from one app to another.
     *
     * <p>Executes a DynamoDB {@code UpdateItem} with a conditional expression that
     * requires both:
     * <ul>
     *   <li>{@code activeAppId = :fromAppId} — the expected current leader</li>
     *   <li>{@code version = :currentVersion} — optimistic concurrency guard</li>
     * </ul>
     * If the condition fails (concurrent transfer, stale version), the method returns
     * {@code false} and the caller should abort the handoff.
     *
     * <p>On success, {@code version} is incremented atomically.
     *
     * @param fromAppId      the currently active app ID (must match the stored value)
     * @param toAppId        the app ID that should become the new leader
     * @param currentVersion the version the caller read before initiating the handoff
     * @return {@code true} if the transfer succeeded; {@code false} if the conditional
     *         write was rejected
     */
    boolean transferFlinkLeader(String fromAppId, String toAppId, int currentVersion);

    /**
     * Returns the app ID of the current Flink application leader.
     *
     * @return {@code "app1"} or {@code "app2"}; {@code null} if no leader record exists
     */
    String getCurrentFlinkLeader();

    /**
     * Returns the current version counter of the Flink leader record.
     *
     * <p>The version is used by the coordinator as an optimistic-concurrency token when
     * calling {@link #transferFlinkLeader}.  Callers should read the version immediately
     * before attempting the transfer to minimise the window for a stale-version error.
     *
     * @return the current version number, or {@code -1} if no leader record exists
     */
    int getCurrentFlinkLeaderVersion();
}
