package com.flinkfasc.coordinator.handoff;

/**
 * States of the {@link HandoffOrchestrator} state machine.
 *
 * <pre>
 * IDLE
 *  │  triggerHandoff()
 *  ▼
 * SAVEPOINT_IN_PROGRESS
 *  │  savepoint completed, App2 started from savepoint
 *  ▼
 * WAITING_FOR_APP2_CATCHUP
 *  │  App2 consumer lag = 0, stable for lagStabilityWindowMs
 *  ▼
 * HANDOFF_INITIATED
 *  │  PREPARE_HANDOFF signal sent to both apps, cutoverOffset chosen
 *  ▼
 * WAITING_FOR_APP1_DRAIN
 *  │  DRAINED_AT received from App1
 *  ▼
 * PROMOTING_APP2
 *  │  DynamoDB leader transfer succeeded, PROMOTE sent to App2
 *  ▼
 * HANDOFF_COMPLETE ──► IDLE (after cooldown)
 *
 * Any state ──► HANDOFF_FAILED (on error or abort)
 * </pre>
 */
public enum HandoffState {

    /** No handoff in progress. Normal steady state. */
    IDLE,

    /** Triggering savepoint on App1 and starting App2 from that savepoint. */
    SAVEPOINT_IN_PROGRESS,

    /** App2 started. Waiting for App2 consumer lag to reach zero. */
    WAITING_FOR_APP2_CATCHUP,

    /** PREPARE_HANDOFF signal sent. Both apps acknowledged. Waiting for App1 to drain. */
    HANDOFF_INITIATED,

    /** Waiting for App1 to finish processing up to cutoverOffset and send DRAINED_AT. */
    WAITING_FOR_APP1_DRAIN,

    /** DynamoDB leader written to App2. PROMOTE sent. Waiting for PROMOTED_CONFIRMED. */
    PROMOTING_APP2,

    /** Handoff complete. App2 is now the active DynamoDB writer. */
    HANDOFF_COMPLETE,

    /** Handoff failed or was aborted. Manual investigation required before retrying. */
    HANDOFF_FAILED
}
