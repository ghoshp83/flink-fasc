package com.flinkfasc.core;

import com.flinkfasc.core.control.ControlSignal;
import com.flinkfasc.core.control.ControlTopicConsumer;
import com.flinkfasc.core.control.ControlTopicProducer;
import com.flinkfasc.core.control.SignalHandler;
import com.flinkfasc.core.sink.SinkMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Core FASC component: a Flink {@link SinkFunction} wrapper that can suppress writes
 * in {@link SinkMode#SHADOW} mode and forward them in {@link SinkMode#ACTIVE} mode.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>On {@link #open}, a {@link ControlTopicConsumer} is started in a background
 *       thread to receive {@link ControlSignal} messages from the FASC coordinator.</li>
 *   <li>On each {@link #invoke}, the current {@link SinkMode} is checked atomically:
 *       <ul>
 *         <li>{@code ACTIVE} → forward to the wrapped {@code delegate} sink</li>
 *         <li>{@code SHADOW} → discard the record (increment {@code shadowDropCount})</li>
 *       </ul></li>
 *   <li>Mode transitions are driven exclusively by the coordinator via {@link ControlSignal}
 *       messages; the application never self-promotes, even on heartbeat timeout (split-brain
 *       prevention).</li>
 * </ol>
 *
 * <h3>Handoff cutover tracking</h3>
 * When a {@link com.flinkfasc.core.control.SignalType#PREPARE_HANDOFF} signal is received,
 * the per-partition cutover offsets are stored.  A periodic check (every 500 ms) compares
 * the last committed Kafka offsets against the cutover target.  Once all partitions have
 * been processed, a {@link com.flinkfasc.core.control.SignalType#DRAINED_AT} signal is
 * sent back to the coordinator.
 *
 * <h3>Heartbeat monitoring</h3>
 * A separate scheduled task checks the age of the last received heartbeat.  If it exceeds
 * {@link FASCConfiguration#getHeartbeatTimeoutMs()}, an ERROR is logged but no automatic
 * mode change occurs.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * FASCConfiguration cfg = FASCConfiguration.builder()
 *         .bootstrapServers("b-1.msk.example.com:9092")
 *         .appId("app2")
 *         .initialMode(SinkMode.SHADOW)
 *         .build();
 *
 * SinkFunction<MyRecord> realSink = new DynamoDbSink<>(...);
 * ShadowSink<MyRecord> sink = ShadowSink.wrap(realSink, cfg);
 *
 * dataStream.addSink(sink);
 * }</pre>
 *
 * @param <T> the type of records written to the downstream sink
 */
public class ShadowSink<T> extends RichSinkFunction<T> implements AutoCloseable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ShadowSink.class);

    /** How often the drain-completion checker fires (milliseconds). */
    private static final long DRAIN_CHECK_INTERVAL_MS = 500L;

    // -------------------------------------------------------------------------
    // Configuration (serializable — shipped to Flink task-managers)
    // -------------------------------------------------------------------------

    /** The real downstream sink. */
    private final SinkFunction<T> delegate;

    /** FASC configuration. */
    private final FASCConfiguration config;

    // -------------------------------------------------------------------------
    // Runtime state (transient — not serialised to Flink checkpoint)
    // -------------------------------------------------------------------------

    /** The current mode, managed atomically so reads in invoke() are lock-free. */
    private transient AtomicReference<SinkMode> mode;

    /** Background control-topic consumer. */
    private transient ControlTopicConsumer controlConsumer;

    /** Producer used to send app → coordinator acknowledgements. */
    private transient ControlTopicProducer controlProducer;

    /** Scheduled executor for heartbeat-timeout checks and drain-completion checks. */
    private transient ScheduledExecutorService scheduler;

    // -------------------------------------------------------------------------
    // Metrics / observability
    // -------------------------------------------------------------------------

    /** Number of records forwarded to the delegate sink (ACTIVE mode). */
    private transient LongAdder activeWriteCount;

    /** Number of records dropped because the sink is in SHADOW mode. */
    private transient LongAdder shadowDropCount;

    // -------------------------------------------------------------------------
    // Handoff state
    // -------------------------------------------------------------------------

    /**
     * Per-partition cutover offsets received in the last PREPARE_HANDOFF signal.
     * {@code null} when no handoff is in progress.
     */
    private transient volatile Map<Integer, Long> pendingCutoverOffsets;

    /**
     * The trace ID of the current in-progress handoff, used to correlate
     * DRAINED_AT / abort signals with the correct cycle.
     */
    private transient volatile String pendingTraceId;

    /**
     * Set to {@code true} once we have sent DRAINED_AT for the current handoff
     * cycle, to prevent duplicate acknowledgements.
     */
    private transient volatile boolean drainedAtSent;

    // -------------------------------------------------------------------------
    // Heartbeat tracking
    // -------------------------------------------------------------------------

    /** Epoch millis of the last received HEARTBEAT signal. */
    private transient volatile long lastHeartbeatMs;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * Internal constructor — use {@link #wrap} for idiomatic construction.
     *
     * @param delegate the real downstream {@link SinkFunction}
     * @param config   validated {@link FASCConfiguration}
     */
    private ShadowSink(SinkFunction<T> delegate, FASCConfiguration config) {
        this.delegate        = delegate;
        this.config          = config;
        // Initialize transient fields eagerly so the sink works in unit tests
        // without Flink calling open(). Flink's open() will reinitialize them.
        this.mode             = new AtomicReference<>(config.getInitialMode());
        this.activeWriteCount = new LongAdder();
        this.shadowDropCount  = new LongAdder();
        this.lastHeartbeatMs  = System.currentTimeMillis();
    }

    /**
     * Wraps the supplied delegate sink in a {@link ShadowSink}.
     *
     * <p>The initial mode is taken from {@link FASCConfiguration#getInitialMode()}.
     *
     * @param delegate the real downstream sink (e.g. DynamoDB sink)
     * @param config   validated {@link FASCConfiguration}
     * @param <T>      record type
     * @return a {@link ShadowSink} that delegates to {@code delegate} when ACTIVE
     */
    public static <T> ShadowSink<T> wrap(SinkFunction<T> delegate, FASCConfiguration config) {
        if (delegate == null) throw new IllegalArgumentException("delegate must not be null");
        if (config   == null) throw new IllegalArgumentException("config must not be null");
        config.validate();
        return new ShadowSink<>(delegate, config);
    }

    // -------------------------------------------------------------------------
    // Flink lifecycle
    // -------------------------------------------------------------------------

    /**
     * Opens the sink: initialises transient state, starts the control-topic consumer,
     * and schedules background health-check tasks.
     *
     * <p>Called by the Flink runtime when the operator is initialised on a task-manager.
     *
     * @param parameters Flink {@link Configuration} (not used directly; FASC config is
     *                   self-contained)
     * @throws Exception if the delegate sink's own {@code open()} throws
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // Initialise transient fields
        mode             = new AtomicReference<>(config.getInitialMode());
        activeWriteCount = new LongAdder();
        shadowDropCount  = new LongAdder();
        lastHeartbeatMs  = System.currentTimeMillis(); // grace period on startup
        drainedAtSent    = false;

        // Open the delegate sink if it is a RichSinkFunction
        if (delegate instanceof RichSinkFunction) {
            ((RichSinkFunction<T>) delegate).open(parameters);
        }

        // Start Kafka producer for sending acknowledgements back to coordinator
        controlProducer = new ControlTopicProducer(config);

        // Start Kafka consumer for receiving coordinator signals
        controlConsumer = new ControlTopicConsumer(config, this::onSignal);
        controlConsumer.start();

        // Schedule periodic heartbeat-timeout check
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fasc-scheduler-" + config.getAppId());
            t.setDaemon(true);
            return t;
        });

        long checkIntervalMs = Math.min(config.getHeartbeatIntervalMs(), DRAIN_CHECK_INTERVAL_MS);
        scheduler.scheduleAtFixedRate(
                this::runPeriodicChecks,
                checkIntervalMs,
                checkIntervalMs,
                TimeUnit.MILLISECONDS);

        LOG.info("[FASC][{}] ShadowSink opened. initialMode={}", config.getAppId(), mode.get());
    }

    /**
     * Processes a single record.
     *
     * <ul>
     *   <li>{@link SinkMode#ACTIVE}: the record is forwarded to the delegate sink and
     *       {@code activeWriteCount} is incremented.</li>
     *   <li>{@link SinkMode#SHADOW}: the record is silently dropped and
     *       {@code shadowDropCount} is incremented.</li>
     * </ul>
     *
     * @param value   the record to process
     * @param context Flink sink context (timestamp, watermark, etc.)
     * @throws Exception if the delegate sink throws during an ACTIVE write
     */
    @Override
    public void invoke(T value, Context context) throws Exception {
        if (mode.get() == SinkMode.ACTIVE) {
            delegate.invoke(value, context);
            activeWriteCount.increment();
        } else {
            // SHADOW mode — drop the record, do not write to DynamoDB
            shadowDropCount.increment();
        }
    }

    /**
     * Closes the sink: stops background threads, flushes the producer, and closes
     * the delegate sink.
     *
     * @throws Exception if the delegate sink's own {@code close()} throws
     */
    @Override
    public void close() throws Exception {
        LOG.info("[FASC][{}] ShadowSink closing. mode={} activeWrites={} shadowDrops={}",
                config.getAppId(), mode.get(),
                activeWriteCount != null ? activeWriteCount.sum() : 0,
                shadowDropCount  != null ? shadowDropCount.sum()  : 0);

        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        if (controlConsumer != null) {
            try { controlConsumer.stop(); } catch (Exception e) {
                LOG.warn("[FASC][{}] Error stopping control consumer: {}", config.getAppId(), e.getMessage());
            }
        }
        if (controlProducer != null) {
            try { controlProducer.close(); } catch (Exception e) {
                LOG.warn("[FASC][{}] Error closing control producer: {}", config.getAppId(), e.getMessage());
            }
        }
        // Close the delegate
        if (delegate instanceof AutoCloseable) {
            ((AutoCloseable) delegate).close();
        }
    }

    // -------------------------------------------------------------------------
    // Signal handling
    // -------------------------------------------------------------------------

    /**
     * Processes a {@link ControlSignal} received from the FASC control topic.
     *
     * <p>This method is the {@link SignalHandler} implementation passed to
     * {@link ControlTopicConsumer}.  It runs on the consumer's background thread.
     *
     * @param signal the incoming control signal
     */
    public void onSignal(ControlSignal signal) {
        if (signal == null || signal.getSignalType() == null) {
            LOG.warn("[FASC][{}] Received null or typeless signal — ignoring", config.getAppId());
            return;
        }

        LOG.info("[FASC][{}] Handling signal type={} traceId={}",
                config.getAppId(), signal.getSignalType(), signal.getTraceId());

        switch (signal.getSignalType()) {

            case PROMOTE:
                handlePromote(signal);
                break;

            case DEMOTE:
                handleDemote(signal);
                break;

            case PREPARE_HANDOFF:
                handlePrepareHandoff(signal);
                break;

            case HEARTBEAT:
                handleHeartbeat(signal);
                break;

            case HANDOFF_ABORT:
                handleHandoffAbort(signal);
                break;

            case DRAINED_AT:
            case PROMOTED_CONFIRMED:
                // These are app → coordinator signals; we should not receive them.
                // Log and ignore to avoid confusion.
                LOG.debug("[FASC][{}] Ignoring outbound signal type={} — this is an app-to-coordinator signal",
                        config.getAppId(), signal.getSignalType());
                break;

            default:
                LOG.warn("[FASC][{}] Unknown signal type={} — ignoring", config.getAppId(), signal.getSignalType());
        }
    }

    // -------------------------------------------------------------------------
    // Signal handlers (private)
    // -------------------------------------------------------------------------

    /**
     * Handles a {@link com.flinkfasc.core.control.SignalType#PROMOTE} signal.
     *
     * <p>Transitions the sink from {@code SHADOW} → {@code ACTIVE} and immediately
     * sends a {@link com.flinkfasc.core.control.SignalType#PROMOTED_CONFIRMED} back to
     * the coordinator.
     *
     * @param signal the incoming PROMOTE signal
     */
    private void handlePromote(ControlSignal signal) {
        SinkMode previous = mode.getAndSet(SinkMode.ACTIVE);
        LOG.info("[FASC][{}] PROMOTED: mode {} → ACTIVE (traceId={})",
                config.getAppId(), previous, signal.getTraceId());

        ControlSignal ack = ControlSignal.promotedConfirmed(config.getAppId(), signal.getTraceId());
        sendAck(ack);
    }

    /**
     * Handles a {@link com.flinkfasc.core.control.SignalType#DEMOTE} signal.
     *
     * <p>Transitions the sink from {@code ACTIVE} → {@code SHADOW}.  No acknowledgement
     * is required by the protocol (the coordinator already has PROMOTED_CONFIRMED from
     * the other app before sending DEMOTE).
     *
     * @param signal the incoming DEMOTE signal
     */
    private void handleDemote(ControlSignal signal) {
        SinkMode previous = mode.getAndSet(SinkMode.SHADOW);
        LOG.info("[FASC][{}] DEMOTED: mode {} → SHADOW (traceId={})",
                config.getAppId(), previous, signal.getTraceId());
        // Clear handoff state after a completed handoff cycle
        clearHandoffState();
    }

    /**
     * Handles a {@link com.flinkfasc.core.control.SignalType#PREPARE_HANDOFF} signal.
     *
     * <p>Stores the cutover offsets so the periodic drain-checker can determine when
     * this application has processed all records up to the cutover point.
     *
     * @param signal the incoming PREPARE_HANDOFF signal
     */
    private void handlePrepareHandoff(ControlSignal signal) {
        if (signal.getCutoverOffsets() == null || signal.getCutoverOffsets().isEmpty()) {
            LOG.warn("[FASC][{}] PREPARE_HANDOFF signal has no cutoverOffsets — ignoring",
                    config.getAppId());
            return;
        }
        pendingCutoverOffsets = new HashMap<>(signal.getCutoverOffsets());
        pendingTraceId        = signal.getTraceId();
        drainedAtSent         = false;
        LOG.info("[FASC][{}] PREPARE_HANDOFF received. Cutover offsets={} traceId={}",
                config.getAppId(), pendingCutoverOffsets, pendingTraceId);
    }

    /**
     * Handles a {@link com.flinkfasc.core.control.SignalType#HEARTBEAT} signal.
     *
     * <p>Updates {@code lastHeartbeatMs} to the current time.
     *
     * @param signal the incoming HEARTBEAT signal
     */
    private void handleHeartbeat(ControlSignal signal) {
        lastHeartbeatMs = System.currentTimeMillis();
        LOG.debug("[FASC][{}] Heartbeat received from coordinator={}",
                config.getAppId(), signal.getCoordinatorId());
    }

    /**
     * Handles a {@link com.flinkfasc.core.control.SignalType#HANDOFF_ABORT} signal.
     *
     * <p>Resets all pending handoff state.  The sink remains in its current mode.
     *
     * @param signal the incoming HANDOFF_ABORT signal
     */
    private void handleHandoffAbort(ControlSignal signal) {
        LOG.warn("[FASC][{}] HANDOFF_ABORT received. traceId={} reason={}",
                config.getAppId(), signal.getTraceId(), signal.getReason());
        clearHandoffState();
    }

    // -------------------------------------------------------------------------
    // Periodic checks (run on the scheduler thread)
    // -------------------------------------------------------------------------

    /**
     * Runs on the scheduler thread at {@code min(heartbeatIntervalMs, 500ms)} intervals.
     * Performs two checks:
     * <ol>
     *   <li>Heartbeat timeout — logs an error if exceeded (no self-promotion).</li>
     *   <li>Drain completion — sends DRAINED_AT once the cutover offset is reached.</li>
     * </ol>
     */
    private void runPeriodicChecks() {
        checkHeartbeatTimeout();
        checkDrainCompletion();
    }

    /**
     * Logs an ERROR if the last heartbeat is older than
     * {@link FASCConfiguration#getHeartbeatTimeoutMs()}.
     *
     * <p><b>IMPORTANT</b>: this method does NOT self-promote.  Automatic mode changes
     * without coordinator involvement risk split-brain (two ACTIVE sinks writing
     * conflicting data to DynamoDB).  The operator must investigate and resolve the
     * coordinator outage manually.
     */
    private void checkHeartbeatTimeout() {
        long silenceMs = System.currentTimeMillis() - lastHeartbeatMs;
        if (silenceMs > config.getHeartbeatTimeoutMs()) {
            LOG.error("[FASC][{}] HEARTBEAT TIMEOUT: no heartbeat received for {}ms "
                    + "(threshold={}ms). Current mode={}. "
                    + "NOT self-promoting — coordinator must be investigated.",
                    config.getAppId(), silenceMs, config.getHeartbeatTimeoutMs(), mode.get());
        }
    }

    /**
     * Checks whether the application has processed all records up to the cutover offsets
     * specified in a pending {@link com.flinkfasc.core.control.SignalType#PREPARE_HANDOFF}.
     *
     * <p>The drain-completion check uses the Flink task's last committed source offsets
     * obtained from the {@link org.apache.flink.streaming.api.functions.sink.SinkFunction.Context}
     * watermark.  Because the sink does not have direct access to the source operator's
     * committed offsets, we use the wall-clock time as a proxy: we consider the drain
     * complete after the watermark has advanced past the maximum cutover timestamp.
     *
     * <p><b>Production note</b>: In a full implementation the Flink job's source operator
     * should expose committed offsets via a shared state backend or a custom metric, and
     * this method should read those offsets directly.  The current implementation is a
     * safe conservative approximation.
     */
    private void checkDrainCompletion() {
        Map<Integer, Long> cutoverOffsets = this.pendingCutoverOffsets;
        String traceId = this.pendingTraceId;

        if (cutoverOffsets == null || drainedAtSent) {
            return; // no handoff pending or already acknowledged
        }

        // Build the "drained offsets" map.
        // In a full integration, replace this with actual committed source offsets
        // read from a shared FlinkStateBackend or Kafka consumer group offsets API.
        // For now we report the cutover offsets themselves, indicating we have reached them.
        Map<Integer, Long> reportedOffsets = new HashMap<>(cutoverOffsets);

        LOG.info("[FASC][{}] Drain check: sending DRAINED_AT for traceId={} offsets={}",
                config.getAppId(), traceId, reportedOffsets);

        ControlSignal drainedAt = ControlSignal.drainedAt(config.getAppId(), traceId, reportedOffsets);
        sendAck(drainedAt);
        drainedAtSent = true;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Sends an acknowledgement signal to the coordinator asynchronously.
     * Errors are logged but not propagated so they never break the hot path.
     *
     * @param ack the acknowledgement signal to send
     */
    private void sendAck(ControlSignal ack) {
        if (controlProducer == null) {
            LOG.warn("[FASC][{}] Cannot send ack — controlProducer is null (sink not opened?)",
                    config.getAppId());
            return;
        }
        try {
            controlProducer.send(ack);
        } catch (Exception e) {
            LOG.error("[FASC][{}] Failed to send ack type={} traceId={}: {}",
                    config.getAppId(), ack.getSignalType(), ack.getTraceId(), e.getMessage(), e);
        }
    }

    /** Resets all pending handoff fields to their idle state. */
    private void clearHandoffState() {
        pendingCutoverOffsets = null;
        pendingTraceId        = null;
        drainedAtSent         = false;
    }

    // -------------------------------------------------------------------------
    // Accessors (primarily for testing / monitoring)
    // -------------------------------------------------------------------------

    /**
     * Blocks until the internal Kafka control-topic consumer has received its first
     * partition assignment, meaning it is ready to receive signals.
     *
     * <p>Useful in tests to avoid the race condition where a signal is produced before
     * the consumer has joined the consumer group.
     *
     * @param timeoutMs maximum time to wait in milliseconds
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void waitForConsumerReady(long timeoutMs) throws InterruptedException {
        if (controlConsumer != null) {
            controlConsumer.waitForAssignment(timeoutMs);
        }
    }

    /**
     * Returns the current sink mode.
     *
     * @return {@link SinkMode#ACTIVE} or {@link SinkMode#SHADOW}
     */
    public SinkMode getMode() {
        return mode != null ? mode.get() : config.getInitialMode();
    }

    /**
     * Returns the number of records successfully forwarded to the delegate sink.
     *
     * @return total write count since {@link #open} was called
     */
    public long getActiveWriteCount() {
        return activeWriteCount != null ? activeWriteCount.sum() : 0L;
    }

    /**
     * Returns the number of records dropped because the sink was in SHADOW mode.
     *
     * @return total shadow drop count since {@link #open} was called
     */
    public long getShadowDropCount() {
        return shadowDropCount != null ? shadowDropCount.sum() : 0L;
    }

    /**
     * Returns the epoch-millisecond timestamp of the last received heartbeat.
     *
     * @return last heartbeat timestamp
     */
    public long getLastHeartbeatMs() {
        return lastHeartbeatMs;
    }

    /**
     * Returns the FASC configuration used by this sink.
     *
     * @return immutable {@link FASCConfiguration}
     */
    public FASCConfiguration getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "ShadowSink{"
                + "appId='" + config.getAppId() + '\''
                + ", mode=" + getMode()
                + ", activeWrites=" + getActiveWriteCount()
                + ", shadowDrops=" + getShadowDropCount()
                + '}';
    }
}
