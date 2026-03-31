package com.flinkfasc.coordinator.control;

import com.flinkfasc.core.FASCConfiguration;
import com.flinkfasc.core.control.ControlTopicConsumer;
import com.flinkfasc.coordinator.handoff.HandoffOrchestrator;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Spring {@link SmartLifecycle} component that subscribes to the FASC control topic
 * and routes incoming signals to {@link HandoffOrchestrator#onSignalReceived}.
 *
 * <h3>Why this component exists</h3>
 * {@link HandoffOrchestrator} drives the handoff state machine but it is agnostic to
 * how signals arrive — it exposes {@code onSignalReceived()} as a simple method.
 * This component wires the real Kafka-backed {@link ControlTopicConsumer} into Spring's
 * lifecycle so that:
 * <ul>
 *   <li>The consumer starts after all other beans are fully initialised (phase 0).</li>
 *   <li>The consumer stops cleanly when the application context is closed.</li>
 * </ul>
 *
 * <h3>Signals handled</h3>
 * <ul>
 *   <li>{@code DRAINED_AT} — App1 confirms it has stopped writing; triggers DynamoDB
 *       leadership transfer and PROMOTE/DEMOTE signals.</li>
 *   <li>{@code PROMOTED_CONFIRMED} — App2 confirms it has switched to ACTIVE; finalises
 *       the handoff state machine.</li>
 *   <li>All other signal types (e.g. HEARTBEAT from a standby coordinator) are silently
 *       ignored by {@link HandoffOrchestrator#onSignalReceived}.</li>
 * </ul>
 *
 * <h3>Consumer identity</h3>
 * The consumer uses {@code appId="coordinator"} so it receives:
 * <ul>
 *   <li>Signals with {@code targetAppId="coordinator"} (none currently defined, reserved)</li>
 *   <li>Broadcast signals with {@code targetAppId="broadcast"} (e.g. HEARTBEAT)</li>
 * </ul>
 * DRAINED_AT and PROMOTED_CONFIRMED are broadcast — the coordinator's consumer receives them.
 */
@Component
public class CoordinatorControlListener implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorControlListener.class);

    private final HandoffOrchestrator      orchestrator;
    private final FASCConfiguration        fascConfig;

    private volatile ControlTopicConsumer  consumer;
    private volatile boolean               running = false;

    /**
     * Constructs the listener.
     *
     * @param orchestrator          the handoff state machine to route signals to
     * @param coordinatorFascConfiguration the coordinator's Kafka identity configuration
     */
    public CoordinatorControlListener(HandoffOrchestrator orchestrator,
                                      FASCConfiguration coordinatorFascConfiguration) {
        this.orchestrator = orchestrator;
        this.fascConfig   = coordinatorFascConfiguration;
    }

    // -------------------------------------------------------------------------
    // SmartLifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts the Kafka consumer. Called by Spring after all beans are initialised.
     * Waits up to 15 seconds for partition assignment before returning so that no
     * signal is missed in the critical startup window.
     */
    @Override
    public void start() {
        LOG.info("Starting CoordinatorControlListener — subscribing to control topic={}", fascConfig.getControlTopic());
        consumer = new ControlTopicConsumer(fascConfig, orchestrator::onSignalReceived);
        consumer.start();

        try {
            consumer.waitForAssignment(15_000);
            LOG.info("CoordinatorControlListener partition assignment received — ready to process signals");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for control topic partition assignment");
        }

        running = true;
    }

    /** Stops the Kafka consumer. Called during application context shutdown. */
    @Override
    public void stop() {
        LOG.info("Stopping CoordinatorControlListener");
        running = false;
        ControlTopicConsumer c = this.consumer;
        if (c != null) {
            c.stop();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns {@code Integer.MAX_VALUE - 100} so that this listener starts after all
     * other beans (including the orchestrator's {@code @PostConstruct}) and stops before them.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /** Async stop — delegates to synchronous {@link #stop()} then calls the callback. */
    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    /** Alias used by {@link PreDestroy} to ensure cleanup even without Spring lifecycle. */
    @PreDestroy
    public void close() {
        if (running) {
            stop();
        }
    }
}
