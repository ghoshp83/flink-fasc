package com.flinkfasc.examples;

import java.io.Serializable;
import java.util.Objects;

/**
 * The business state accumulated per customer across all processed events.
 *
 * <p>One {@code CustomerState} instance is stored in Flink's keyed state
 * (backed by RocksDB) for each unique {@code customerId}.
 * {@link BusinessProcessFunction} reads and updates this object on every
 * incoming {@link BusinessEvent}.
 *
 * <p>The {@code stateVersion} field is a monotonically-increasing counter that
 * can be used by a future {@code StateMigrator} to detect when a checkpoint was
 * taken from an older schema version and apply an upgrade strategy.
 *
 * <h3>DynamoDB mapping</h3>
 * <ul>
 *   <li>{@code customerId} — partition key (String)</li>
 *   <li>All remaining fields — non-key attributes</li>
 * </ul>
 */
public class CustomerState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Current schema version. Increment when the structure of this class changes. */
    public static final int CURRENT_STATE_VERSION = 1;

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    /** Customer identifier — mirrors {@link BusinessEvent#getCustomerId()}. */
    private String customerId;

    /** Total number of non-cancelled orders processed for this customer. */
    private int orderCount;

    /** Running sum of {@link BusinessEvent#getAmount()} for all active orders. */
    private double totalAmount;

    /**
     * The {@code eventType} of the most recently processed {@link BusinessEvent}.
     * Useful for debugging and as a quick audit trail in DynamoDB.
     */
    private String lastEventType;

    /**
     * Event time (epoch milliseconds) of the most recently processed
     * {@link BusinessEvent}.  Updated from {@code record.getTimestamp()} — not
     * from {@code System.currentTimeMillis()} — so that both the active and
     * shadow applications converge to the same value for the same event.
     */
    private long lastUpdatedMs;

    /**
     * Monotonically-increasing schema version written with every state update.
     * Allows a future {@code StateMigrator} to detect stale snapshots and apply
     * an upgrade path before the next DynamoDB write.
     */
    private int stateVersion;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** No-arg constructor required by Flink serialisation frameworks. */
    public CustomerState() {}

    /**
     * Convenience constructor for initialising a fresh state object when a
     * customer is seen for the first time.
     *
     * @param customerId the customer identifier
     */
    public CustomerState(String customerId) {
        this.customerId    = customerId;
        this.orderCount    = 0;
        this.totalAmount   = 0.0;
        this.lastEventType = null;
        this.lastUpdatedMs = 0L;
        this.stateVersion  = CURRENT_STATE_VERSION;
    }

    // -------------------------------------------------------------------------
    // Getters and setters
    // -------------------------------------------------------------------------

    /** @return the customer identifier */
    public String getCustomerId() { return customerId; }

    /** @param customerId the customer identifier */
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    /** @return total number of non-cancelled orders for this customer */
    public int getOrderCount() { return orderCount; }

    /** @param orderCount total non-cancelled order count */
    public void setOrderCount(int orderCount) { this.orderCount = orderCount; }

    /** @return running total monetary amount across all active orders */
    public double getTotalAmount() { return totalAmount; }

    /** @param totalAmount running total amount */
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

    /** @return event type of the last processed event */
    public String getLastEventType() { return lastEventType; }

    /** @param lastEventType event type of the last processed event */
    public void setLastEventType(String lastEventType) { this.lastEventType = lastEventType; }

    /**
     * Returns the event-time timestamp (epoch milliseconds) of the last processed
     * event.  This is used as the conditional-write guard in {@link DynamoDbSink}:
     * a DynamoDB put succeeds only if the stored value is strictly less than this
     * value, making writes idempotent even when the same event is replayed during
     * a FASC handoff.
     *
     * @return last event's timestamp in epoch milliseconds
     */
    public long getLastUpdatedMs() { return lastUpdatedMs; }

    /** @param lastUpdatedMs event-time timestamp of the last processed event */
    public void setLastUpdatedMs(long lastUpdatedMs) { this.lastUpdatedMs = lastUpdatedMs; }

    /**
     * Returns the schema version of this state object.
     *
     * @return schema version (currently {@value #CURRENT_STATE_VERSION})
     */
    public int getStateVersion() { return stateVersion; }

    /** @param stateVersion schema version */
    public void setStateVersion(int stateVersion) { this.stateVersion = stateVersion; }

    // -------------------------------------------------------------------------
    // Object overrides
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return "CustomerState{"
                + "customerId='" + customerId + '\''
                + ", orderCount=" + orderCount
                + ", totalAmount=" + totalAmount
                + ", lastEventType='" + lastEventType + '\''
                + ", lastUpdatedMs=" + lastUpdatedMs
                + ", stateVersion=" + stateVersion
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CustomerState)) return false;
        CustomerState that = (CustomerState) o;
        return orderCount == that.orderCount
                && Double.compare(that.totalAmount, totalAmount) == 0
                && lastUpdatedMs == that.lastUpdatedMs
                && stateVersion == that.stateVersion
                && Objects.equals(customerId, that.customerId)
                && Objects.equals(lastEventType, that.lastEventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerId, orderCount, totalAmount,
                lastEventType, lastUpdatedMs, stateVersion);
    }
}
