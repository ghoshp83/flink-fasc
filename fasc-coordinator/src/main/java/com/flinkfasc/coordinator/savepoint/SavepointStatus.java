package com.flinkfasc.coordinator.savepoint;

/**
 * Lifecycle status of a Flink savepoint triggered by the FASC coordinator.
 *
 * <p>Savepoints transition through these states in order:
 * <ol>
 *   <li>{@link #IN_PROGRESS} — the savepoint trigger has been POSTed to the Flink REST API
 *       and polling is underway.</li>
 *   <li>{@link #COMPLETED} — Flink reported {@code "COMPLETED"} and the S3 location is known.</li>
 *   <li>{@link #FAILED} — Flink reported {@code "FAILED"} or the coordinator's poll timeout
 *       (5 minutes) was exceeded.</li>
 * </ol>
 */
public enum SavepointStatus {

    /** The savepoint trigger has been submitted; polling for completion is ongoing. */
    IN_PROGRESS,

    /** The savepoint completed successfully; the S3 location is available. */
    COMPLETED,

    /** The savepoint failed or the polling timeout was exceeded. */
    FAILED
}
