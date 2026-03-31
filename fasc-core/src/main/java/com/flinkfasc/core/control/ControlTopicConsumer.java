package com.flinkfasc.core.control;

import com.flinkfasc.core.FASCConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Subscribes to the FASC Kafka control topic and invokes a {@link SignalHandler} for
 * each {@link ControlSignal} that is relevant to this application instance.
 *
 * <h3>Filtering</h3>
 * A signal is delivered to the handler if and only if:
 * <ul>
 *   <li>{@code signal.getTargetAppId()} equals this app's {@link FASCConfiguration#getAppId()}, or</li>
 *   <li>{@code signal.getTargetAppId()} equals {@link ControlSignal#BROADCAST}.</li>
 * </ul>
 * All other signals are silently discarded.
 *
 * <h3>Error handling</h3>
 * Deserialisation errors are logged at WARN level and the offending record is skipped;
 * the poll loop continues uninterrupted.  This prevents a single malformed message from
 * stalling the control plane.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>Construct with a {@link FASCConfiguration} and a {@link SignalHandler}.</li>
 *   <li>Call {@link #start()} to spin up the background polling thread.</li>
 *   <li>Call {@link #stop()} (or {@link #close()}) to stop the thread and close the consumer.</li>
 * </ol>
 *
 * <h3>Thread safety</h3>
 * {@link KafkaConsumer} is NOT thread-safe; all consumer operations are confined to the
 * dedicated background thread.  {@link #stop()} is safe to call from any thread — it uses
 * {@link KafkaConsumer#wakeup()} to interrupt the poll call cleanly.
 */
public class ControlTopicConsumer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ControlTopicConsumer.class);

    /** How long {@link KafkaConsumer#poll} blocks waiting for records. */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    /** How long {@link #stop()} waits for the polling thread to terminate. */
    private static final long STOP_TIMEOUT_MS = 10_000L;

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private final FASCConfiguration     config;
    private final SignalHandler         handler;
    private final ControlSignalSerde    serde;

    /** Set to true before calling wakeup so that WakeupException is treated as shutdown. */
    private final AtomicBoolean         stopping = new AtomicBoolean(false);

    /** Latch released when the polling thread has fully exited. */
    private final CountDownLatch        stopLatch = new CountDownLatch(1);

    /** Latch released after the first successful poll (partition assignment received). */
    private final CountDownLatch        assignedLatch = new CountDownLatch(1);

    /** The underlying Kafka consumer — accessed only from the polling thread. */
    private volatile KafkaConsumer<String, byte[]> consumer;

    /** The background polling thread. */
    private volatile Thread             pollingThread;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * Creates a new consumer.  Call {@link #start()} to begin processing.
     *
     * @param config  validated {@link FASCConfiguration}; must not be {@code null}
     * @param handler callback for accepted signals; must not be {@code null}
     */
    public ControlTopicConsumer(FASCConfiguration config, SignalHandler handler) {
        if (config == null)  throw new IllegalArgumentException("config must not be null");
        if (handler == null) throw new IllegalArgumentException("handler must not be null");
        this.config  = config;
        this.handler = handler;
        this.serde   = new ControlSignalSerde();
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts the background Kafka polling thread.
     *
     * <p>The thread is started as a daemon so it does not prevent JVM shutdown.
     * Calling {@link #start()} more than once throws {@link IllegalStateException}.
     *
     * @throws IllegalStateException if already started
     */
    public synchronized void start() {
        if (pollingThread != null) {
            throw new IllegalStateException(
                    "ControlTopicConsumer for appId=" + config.getAppId() + " is already started");
        }

        consumer = new KafkaConsumer<>(buildProperties(config));
        consumer.subscribe(Collections.singletonList(config.getControlTopic()));

        pollingThread = new Thread(this::pollLoop,
                "fasc-control-consumer-" + config.getAppId());
        pollingThread.setDaemon(true);
        pollingThread.start();

        LOG.info("[FASC][{}] ControlTopicConsumer started, subscribed to topic={}",
                config.getAppId(), config.getControlTopic());
    }

    /**
     * Stops the background polling thread and closes the Kafka consumer.
     *
     * <p>Blocks until the thread terminates or the stop timeout ({@value #STOP_TIMEOUT_MS} ms)
     * elapses.  Safe to call from any thread.
     */
    public void stop() {
        if (stopping.compareAndSet(false, true)) {
            LOG.info("[FASC][{}] ControlTopicConsumer stopping...", config.getAppId());
            KafkaConsumer<String, byte[]> c = this.consumer;
            if (c != null) {
                // KafkaConsumer.wakeup() is the only thread-safe consumer method
                c.wakeup();
            }
            try {
                if (!stopLatch.await(STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    LOG.warn("[FASC][{}] ControlTopicConsumer polling thread did not stop within {}ms",
                            config.getAppId(), STOP_TIMEOUT_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("[FASC][{}] Interrupted while waiting for ControlTopicConsumer to stop",
                        config.getAppId());
            }
            LOG.info("[FASC][{}] ControlTopicConsumer stopped.", config.getAppId());
        }
    }

    /**
     * Blocks until the consumer has received its first partition assignment (i.e., it has
     * successfully joined the consumer group and is ready to receive messages).
     *
     * <p>Call this after {@link #start()} before producing messages in tests to avoid the
     * race condition where a message is produced before the consumer is positioned.
     *
     * @param timeoutMs maximum time to wait in milliseconds
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void waitForAssignment(long timeoutMs) throws InterruptedException {
        if (!assignedLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            LOG.warn("[FASC][{}] Consumer partition assignment not received within {}ms",
                    config.getAppId(), timeoutMs);
        }
    }

    /** Alias for {@link #stop()} — satisfies {@link AutoCloseable}. */
    @Override
    public void close() {
        stop();
    }

    // -------------------------------------------------------------------------
    // Poll loop (runs on background thread)
    // -------------------------------------------------------------------------

    /**
     * The main polling loop executed on the dedicated background thread.
     * Continues until {@link #stop()} triggers a {@link WakeupException}.
     */
    private void pollLoop() {
        try {
            while (!stopping.get()) {
                ConsumerRecords<String, byte[]> records;
                try {
                    records = consumer.poll(POLL_TIMEOUT);
                } catch (WakeupException e) {
                    // Expected shutdown path
                    if (stopping.get()) break;
                    throw e;
                }
                // Release the ready latch once partition assignment has been received
                assignedLatch.countDown();

                for (ConsumerRecord<String, byte[]> record : records) {
                    processRecord(record);
                }
            }
        } catch (WakeupException e) {
            // Normal shutdown — no action needed
            LOG.debug("[FASC][{}] Poll loop woken up during shutdown", config.getAppId());
        } catch (Exception e) {
            LOG.error("[FASC][{}] Unexpected error in ControlTopicConsumer poll loop: {}",
                    config.getAppId(), e.getMessage(), e);
        } finally {
            safeClose();
            stopLatch.countDown();
        }
    }

    /**
     * Processes a single Kafka record: deserialises it, applies the app-ID filter, and
     * — if it passes — invokes the {@link SignalHandler}.
     *
     * @param record the raw Kafka consumer record
     */
    private void processRecord(ConsumerRecord<String, byte[]> record) {
        ControlSignal signal;
        try {
            signal = serde.deserialize(record.value());
        } catch (ControlSignalSerde.ControlSignalSerdeException e) {
            LOG.warn("[FASC][{}] Skipping unreadable control record at partition={} offset={}: {}",
                    config.getAppId(), record.partition(), record.offset(), e.getMessage());
            return;
        }

        // Apply filter: accept signals addressed to this app or broadcast to all
        String target = signal.getTargetAppId();
        if (!config.getAppId().equals(target) && !ControlSignal.BROADCAST.equals(target)) {
            LOG.trace("[FASC][{}] Ignoring signal type={} addressed to '{}'",
                    config.getAppId(), signal.getSignalType(), target);
            return;
        }

        LOG.info("[FASC][{}] Received signal type={} source={} traceId={} partition={} offset={}",
                config.getAppId(), signal.getSignalType(), signal.getSourceAppId(),
                signal.getTraceId(), record.partition(), record.offset());

        try {
            handler.handle(signal);
        } catch (Exception e) {
            // The handler must not crash the poll loop — log and continue
            LOG.error("[FASC][{}] SignalHandler threw an exception for signal type={} traceId={}: {}",
                    config.getAppId(), signal.getSignalType(), signal.getTraceId(),
                    e.getMessage(), e);
        }
    }

    /**
     * Closes the Kafka consumer, suppressing any exceptions so they do not
     * mask the original exception in {@link #pollLoop}.
     */
    private void safeClose() {
        KafkaConsumer<String, byte[]> c = this.consumer;
        if (c != null) {
            try {
                c.close();
                LOG.debug("[FASC][{}] KafkaConsumer closed successfully", config.getAppId());
            } catch (Exception e) {
                LOG.warn("[FASC][{}] Error closing KafkaConsumer: {}",
                        config.getAppId(), e.getMessage(), e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds Kafka consumer {@link Properties} from the supplied configuration.
     *
     * @param config source FASC configuration
     * @return ready-to-use consumer properties
     */
    private static Properties buildProperties(FASCConfiguration config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           config.getConsumerGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        // Start from latest — we only care about signals sent while this instance is running
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "latest");
        // Disable auto-commit — control signals are small and we process them immediately
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // Client ID for monitoring
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "fasc-consumer-" + config.getAppId());
        // Relatively short session timeout to detect coordinator / app failures quickly
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        return props;
    }
}
