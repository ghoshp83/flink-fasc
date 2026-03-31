package com.flinkfasc.core.control;

/**
 * Enumeration of all signal types exchanged on the FASC control Kafka topic.
 *
 * <p>The control protocol is deliberately one-directional in terms of authority:
 * the external coordinator is the <em>sole</em> decision-maker for mode transitions.
 * Apps only send acknowledgement / observability signals back to the coordinator
 * ({@link #DRAINED_AT}, {@link #PROMOTED_CONFIRMED}).
 *
 * <h3>Handoff sequence (happy path)</h3>
 * <pre>
 *  Coordinator  ──PREPARE_HANDOFF(cutoverOffsets)──►  app1 + app2
 *  app1  ──DRAINED_AT(offsets)──►  Coordinator         (app1 has processed up to C)
 *  Coordinator  ──PROMOTE──►  app2                     (app2 becomes ACTIVE)
 *  app2  ──PROMOTED_CONFIRMED──►  Coordinator
 *  Coordinator  ──DEMOTE──►  app1                      (app1 becomes SHADOW)
 * </pre>
 *
 * <h3>Abort path</h3>
 * If anything goes wrong the coordinator broadcasts {@link #HANDOFF_ABORT} and both
 * apps reset to their pre-handoff state.
 */
public enum SignalType {

    /**
     * Coordinator → both apps: a handoff is about to begin.
     * The signal carries the per-partition cutover offset {@code C}; both apps must
     * reach offset {@code C} before app1 can be demoted and app2 promoted.
     */
    PREPARE_HANDOFF,

    /**
     * Coordinator → app2: activate the downstream (DynamoDB) sink.
     * app2 transitions from {@code SHADOW} → {@code ACTIVE} and responds with
     * {@link #PROMOTED_CONFIRMED}.
     */
    PROMOTE,

    /**
     * Coordinator → app1: suppress the downstream sink.
     * app1 transitions from {@code ACTIVE} → {@code SHADOW}.
     * Sent only after the coordinator has received {@link #PROMOTED_CONFIRMED} from app2,
     * guaranteeing zero downtime.
     */
    DEMOTE,

    /**
     * app1 → coordinator: app1 has finished processing all records up to the
     * cutover offset supplied in {@link #PREPARE_HANDOFF}.
     * Carries the actual per-partition offsets reached so the coordinator can verify
     * they meet or exceed the target.
     */
    DRAINED_AT,

    /**
     * app2 → coordinator: app2 has successfully activated its sink and is now writing
     * to DynamoDB. The coordinator may now send {@link #DEMOTE} to app1.
     */
    PROMOTED_CONFIRMED,

    /**
     * Coordinator → apps: liveness ping.
     * Apps track the timestamp of the last received heartbeat. If no heartbeat arrives
     * within {@link com.flinkfasc.core.FASCConfiguration#getHeartbeatTimeoutMs()} the app
     * logs an error but does NOT self-promote (split-brain prevention).
     */
    HEARTBEAT,

    /**
     * Coordinator → both apps: abort an in-progress handoff.
     * Both apps must revert to their state before the {@link #PREPARE_HANDOFF} signal.
     * The signal carries an optional {@code reason} string for diagnostics.
     */
    HANDOFF_ABORT
}
