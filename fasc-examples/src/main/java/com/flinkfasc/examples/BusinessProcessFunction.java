package com.flinkfasc.examples;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful Flink operator that accumulates per-customer business state from
 * a stream of {@link BusinessEvent} records.
 *
 * <p>Keyed on {@link BusinessEvent#getCustomerId()}, one {@link CustomerState}
 * instance is maintained in RocksDB-backed {@link ValueState} for each customer.
 * The state is updated deterministically from the event's own timestamp
 * ({@code context.timestamp()}) so that both the active and shadow FASC
 * application instances converge to the same state for the same logical stream
 * position, enabling a lossless, zero-downtime handoff.
 *
 * <h3>Event-type semantics</h3>
 * <ul>
 *   <li>{@code ORDER_CREATED} — increments {@code orderCount}, adds {@code amount}
 *       to {@code totalAmount}.</li>
 *   <li>{@code ORDER_UPDATED} — adjusts {@code totalAmount} by the new
 *       {@code amount} minus the assumed previous amount; this example uses a
 *       simplified model where the full delta is in {@code amount}.</li>
 *   <li>{@code ORDER_CANCELLED} — decrements {@code orderCount} (floor 0),
 *       subtracts {@code amount} from {@code totalAmount} (floor 0.0).</li>
 *   <li>Any other type — logged as a warning and ignored.</li>
 * </ul>
 *
 * <h3>FASC determinism contract</h3>
 * This function MUST NOT call {@code System.currentTimeMillis()} for any field
 * that is subsequently written to DynamoDB.  All timestamps are sourced from
 * {@code context.timestamp()} (Flink event time), which is derived from
 * {@link BusinessEvent#getEventTimestamp()} via the watermark strategy defined
 * in {@link BusinessFlinkJob}.
 */
public class BusinessProcessFunction
        extends KeyedProcessFunction<String, BusinessEvent, CustomerState> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(BusinessProcessFunction.class);

    // -------------------------------------------------------------------------
    // State descriptor name — must be stable across deployments / savepoints
    // -------------------------------------------------------------------------

    private static final String STATE_DESCRIPTOR_NAME = "customer-state";

    // -------------------------------------------------------------------------
    // Keyed state handle — one entry per customerId
    // -------------------------------------------------------------------------

    /** Per-customer accumulated state backed by RocksDB on each TaskManager. */
    private transient ValueState<CustomerState> customerState;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Initialises the {@link ValueState} descriptor.
     *
     * <p>The descriptor name {@value #STATE_DESCRIPTOR_NAME} is intentionally
     * fixed so that savepoints taken from any previous version of this class can
     * be restored after a rolling upgrade without requiring state migration.
     *
     * @param parameters Flink operator configuration (unused)
     */
    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<CustomerState> descriptor = new ValueStateDescriptor<>(
                STATE_DESCRIPTOR_NAME,
                TypeInformation.of(CustomerState.class));
        customerState = getRuntimeContext().getState(descriptor);
    }

    // -------------------------------------------------------------------------
    // Processing
    // -------------------------------------------------------------------------

    /**
     * Processes a single {@link BusinessEvent}, updates the per-customer
     * {@link CustomerState}, and emits the updated state downstream.
     *
     * <p>The updated state is forwarded to the {@code ShadowSink} wrapping
     * {@link DynamoDbSink}.  In ACTIVE mode the sink writes through; in SHADOW
     * mode the write is suppressed so the application can build identical state
     * without producing DynamoDB side-effects until a FASC handoff completes.
     *
     * @param event     the incoming business event
     * @param context   Flink process context — provides event-time timestamp
     * @param collector output collector
     * @throws Exception propagated Flink state access exception
     */
    @Override
    public void processElement(
            BusinessEvent event,
            Context context,
            Collector<CustomerState> collector) throws Exception {

        // Load current state (null on first encounter for this customer).
        CustomerState state = customerState.value();
        if (state == null) {
            state = new CustomerState(event.getCustomerId());
            LOG.debug("Initialising new CustomerState for customerId={}", event.getCustomerId());
        }

        // Update state based on event type.
        // CRITICAL: use context.timestamp() (event time) — NOT System.currentTimeMillis().
        // This ensures both active and shadow instances produce identical state for the
        // same logical stream offset, which is the foundation of the FASC handoff guarantee.
        final long eventTime = context.timestamp();

        switch (event.getEventType()) {
            case "ORDER_CREATED":
                state.setOrderCount(state.getOrderCount() + 1);
                state.setTotalAmount(state.getTotalAmount() + event.getAmount());
                break;

            case "ORDER_UPDATED":
                // Simplified delta model: amount carries the signed adjustment.
                state.setTotalAmount(Math.max(0.0, state.getTotalAmount() + event.getAmount()));
                break;

            case "ORDER_CANCELLED":
                state.setOrderCount(Math.max(0, state.getOrderCount() - 1));
                state.setTotalAmount(Math.max(0.0, state.getTotalAmount() - event.getAmount()));
                break;

            default:
                LOG.warn("Unknown eventType='{}' for customerId='{}' — skipping state update",
                        event.getEventType(), event.getCustomerId());
                return;
        }

        // Record last event metadata using event time for determinism.
        state.setLastEventType(event.getEventType());
        state.setLastUpdatedMs(eventTime);
        state.setStateVersion(CustomerState.CURRENT_STATE_VERSION);

        // Persist updated state back into RocksDB.
        customerState.update(state);

        // Emit to downstream ShadowSink (DynamoDbSink wrapped by FASC).
        collector.collect(state);

        LOG.debug("Processed event eventId='{}' type='{}' for customerId='{}' → {}",
                event.getEventId(), event.getEventType(), event.getCustomerId(), state);
    }
}
