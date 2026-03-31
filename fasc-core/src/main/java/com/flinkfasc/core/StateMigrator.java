package com.flinkfasc.core;

/**
 * Pluggable interface for migrating Flink operator state between schema versions
 * during a FASC savepoint bootstrap.
 *
 * <p>When App1 (v1) produces a savepoint and App2 (v2) has a different state schema,
 * a registered {@code StateMigrator} is applied during App2's savepoint restore,
 * allowing rolling upgrades with state schema evolution.
 *
 * <p>Example usage:
 * <pre>{@code
 * public class CustomerStateV1ToV2Migrator implements StateMigrator<CustomerStateV1, CustomerStateV2> {
 *     public CustomerStateV2 migrate(CustomerStateV1 old, int fromVersion, int toVersion) {
 *         CustomerStateV2 newState = new CustomerStateV2();
 *         newState.setCustomerId(old.getCustomerId());
 *         newState.setOrderCount(old.getOrderCount());
 *         newState.setTotalAmount(old.getTotalAmount());
 *         newState.setTierLevel("STANDARD");  // new field with default
 *         return newState;
 *     }
 *     public int getFromVersion() { return 1; }
 *     public int getToVersion()   { return 2; }
 * }
 * }</pre>
 *
 * @param <OLD> the state type of the previous version
 * @param <NEW> the state type of the new version
 */
public interface StateMigrator<OLD, NEW> {

    /**
     * Migrates a state value from {@code fromVersion} to {@code toVersion}.
     *
     * <p>Implementations must be deterministic and side-effect-free.
     *
     * @param oldState    the state value from the previous version
     * @param fromVersion the schema version of oldState
     * @param toVersion   the target schema version
     * @return the migrated state value
     */
    NEW migrate(OLD oldState, int fromVersion, int toVersion);

    /** Returns the schema version this migrator reads from. */
    int getFromVersion();

    /** Returns the schema version this migrator produces. */
    int getToVersion();
}
