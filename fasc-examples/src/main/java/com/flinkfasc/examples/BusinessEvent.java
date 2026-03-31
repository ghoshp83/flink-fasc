package com.flinkfasc.examples;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a single business event consumed from the MSK Kafka topic.
 *
 * <p>Events are produced externally (e.g. from an order management system) and
 * serialised as JSON on the Kafka topic.  The {@link BusinessEventDeserializer}
 * deserialises the raw bytes into this POJO.
 *
 * <p>The {@code eventTimestamp} field (epoch milliseconds) is used as the Flink
 * <em>event time</em> timestamp; it must be present in every message so that the
 * watermark strategy and keyed state updates remain deterministic across both
 * the active and shadow application instances.
 *
 * <h3>Supported event types</h3>
 * <ul>
 *   <li>{@code ORDER_CREATED} — new order placed by the customer</li>
 *   <li>{@code ORDER_UPDATED} — existing order modified (amount changed, etc.)</li>
 *   <li>{@code ORDER_CANCELLED} — order cancelled; amount should be subtracted</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BusinessEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Unique identifier for this event (UUID). */
    @JsonProperty("eventId")
    private String eventId;

    /** Identifier of the customer who triggered the event. Used as the Flink key. */
    @JsonProperty("customerId")
    private String customerId;

    /**
     * Type of the business event.
     * Expected values: {@code ORDER_CREATED}, {@code ORDER_UPDATED},
     * {@code ORDER_CANCELLED}.
     */
    @JsonProperty("eventType")
    private String eventType;

    /** Monetary amount associated with this event (e.g. order value). */
    @JsonProperty("amount")
    private double amount;

    /**
     * Event time expressed as milliseconds since the Unix epoch.
     * This value is assigned as the Flink record timestamp so that all state
     * updates are driven by event time rather than processing time.
     */
    @JsonProperty("eventTimestamp")
    private long eventTimestamp;

    /**
     * Opaque JSON string carrying additional domain-specific data.
     * Stored as-is in {@link CustomerState#getLastEventType()} for auditing.
     */
    @JsonProperty("payload")
    private String payload;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** No-arg constructor required by Jackson and Flink serialisation. */
    public BusinessEvent() {}

    /**
     * Full constructor — convenience for tests and producers.
     *
     * @param eventId        unique event identifier
     * @param customerId     customer identifier (Flink partition key)
     * @param eventType      one of ORDER_CREATED / ORDER_UPDATED / ORDER_CANCELLED
     * @param amount         monetary amount
     * @param eventTimestamp event time in epoch milliseconds
     * @param payload        additional JSON payload string (may be null)
     */
    @JsonCreator
    public BusinessEvent(
            @JsonProperty("eventId")        String eventId,
            @JsonProperty("customerId")     String customerId,
            @JsonProperty("eventType")      String eventType,
            @JsonProperty("amount")         double amount,
            @JsonProperty("eventTimestamp") long eventTimestamp,
            @JsonProperty("payload")        String payload) {
        this.eventId         = eventId;
        this.customerId      = customerId;
        this.eventType       = eventType;
        this.amount          = amount;
        this.eventTimestamp  = eventTimestamp;
        this.payload         = payload;
    }

    // -------------------------------------------------------------------------
    // Getters and setters
    // -------------------------------------------------------------------------

    /** @return unique event identifier */
    public String getEventId() { return eventId; }

    /** @param eventId unique event identifier */
    public void setEventId(String eventId) { this.eventId = eventId; }

    /** @return customer identifier used as the Flink stream key */
    public String getCustomerId() { return customerId; }

    /** @param customerId customer identifier */
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    /** @return event type string (ORDER_CREATED / ORDER_UPDATED / ORDER_CANCELLED) */
    public String getEventType() { return eventType; }

    /** @param eventType event type string */
    public void setEventType(String eventType) { this.eventType = eventType; }

    /** @return monetary amount for this event */
    public double getAmount() { return amount; }

    /** @param amount monetary amount */
    public void setAmount(double amount) { this.amount = amount; }

    /**
     * Returns the event timestamp in epoch milliseconds.
     *
     * <p>This is the value extracted by the {@code WatermarkStrategy} in
     * {@link BusinessFlinkJob} and stamped onto the Flink record as the event-time
     * timestamp. Using event time — rather than {@code System.currentTimeMillis()} —
     * ensures that both the active and shadow instances compute identical state
     * updates for the same logical stream position.
     *
     * @return event time as epoch milliseconds
     */
    public long getEventTimestamp() { return eventTimestamp; }

    /** @param eventTimestamp event time in epoch milliseconds */
    public void setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    /** @return additional JSON payload string, or {@code null} */
    public String getPayload() { return payload; }

    /** @param payload additional JSON payload string */
    public void setPayload(String payload) { this.payload = payload; }

    // -------------------------------------------------------------------------
    // Object overrides
    // -------------------------------------------------------------------------

    @Override
    public String toString() {
        return "BusinessEvent{"
                + "eventId='" + eventId + '\''
                + ", customerId='" + customerId + '\''
                + ", eventType='" + eventType + '\''
                + ", amount=" + amount
                + ", eventTimestamp=" + eventTimestamp
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BusinessEvent)) return false;
        BusinessEvent that = (BusinessEvent) o;
        return Double.compare(that.amount, amount) == 0
                && eventTimestamp == that.eventTimestamp
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(customerId, that.customerId)
                && Objects.equals(eventType, that.eventType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, customerId, eventType, amount, eventTimestamp);
    }
}
