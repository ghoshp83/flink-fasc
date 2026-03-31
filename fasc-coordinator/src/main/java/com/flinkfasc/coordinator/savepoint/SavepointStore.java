package com.flinkfasc.coordinator.savepoint;

/**
 * Persistence interface for {@link SavepointMetadata} records.
 *
 * <p>Implementations write to DynamoDB so that if the active coordinator pod crashes
 * mid-handoff, the newly elected coordinator can read the last savepoint and resume
 * the protocol rather than taking a fresh savepoint from scratch.
 */
public interface SavepointStore {

    /**
     * Persists (or upserts) a savepoint metadata record.
     *
     * @param metadata the savepoint to store; must not be {@code null}
     */
    void save(SavepointMetadata metadata);

    /**
     * Retrieves the most recently completed savepoint for the given source job ID.
     *
     * @param sourceJobId Flink job ID of the application that was snapshotted
     * @return the most recently stored {@link SavepointMetadata} with
     *         {@link SavepointStatus#COMPLETED}, or {@code null} if none exists
     */
    SavepointMetadata findLatestCompleted(String sourceJobId);

    /**
     * Retrieves a savepoint record by its unique FASC savepoint identifier.
     *
     * @param savepointId the FASC-generated savepoint ID (UUID)
     * @return the matching {@link SavepointMetadata}, or {@code null} if not found
     */
    SavepointMetadata findById(String savepointId);
}
