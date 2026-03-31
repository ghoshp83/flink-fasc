package com.flinkfasc.core.control;

import com.flinkfasc.core.FASCConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Sends {@link ControlSignal} messages to the FASC Kafka control topic.
 *
 * <p>This class wraps a plain Kafka {@link KafkaProducer} so that it can be used in
 * non-Flink contexts (e.g. the external coordinator process) as well as inside Flink
 * operators.  Serialisation is delegated to {@link ControlSignalSerde}.
 *
 * <h3>Kafka message layout</h3>
 * <ul>
 *   <li><b>Topic</b>: value of {@link FASCConfiguration#getControlTopic()}</li>
 *   <li><b>Key</b>: {@code signal.getTargetAppId()} — enables per-app partitioning and
 *       ordered delivery within a target</li>
 *   <li><b>Value</b>: UTF-8 JSON produced by {@link ControlSignalSerde#serialize}</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * The underlying {@link KafkaProducer} is thread-safe; multiple threads may call
 * {@link #send} concurrently without external synchronisation.
 *
 * <h3>Lifecycle</h3>
 * Call {@link #close()} when the producer is no longer needed to flush pending messages
 * and release resources.
 */
public class ControlTopicProducer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ControlTopicProducer.class);

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    /**
     * Producer {@code acks} setting.  "all" ensures the broker leader and all
     * in-sync replicas acknowledge before the send future completes.
     */
    private static final String ACKS_ALL = "all";

    /** Number of retries on transient network errors. */
    private static final int RETRIES = 3;

    /** Linger time (ms) — 0 ensures low-latency delivery of control messages. */
    private static final int LINGER_MS = 0;

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private final String                controlTopic;
    private final KafkaProducer<String, byte[]> producer;
    private final ControlSignalSerde    serde;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    /**
     * Creates a new producer using the supplied FASC configuration.
     *
     * @param config validated {@link FASCConfiguration}; must not be {@code null}
     */
    public ControlTopicProducer(FASCConfiguration config) {
        this(config, buildProperties(config));
    }

    /**
     * Package-private constructor used in tests to inject custom Kafka properties.
     *
     * @param config     validated {@link FASCConfiguration}
     * @param properties Kafka producer properties (may contain test-specific overrides)
     */
    ControlTopicProducer(FASCConfiguration config, Properties properties) {
        this.controlTopic = config.getControlTopic();
        this.serde        = new ControlSignalSerde();
        this.producer     = new KafkaProducer<>(properties);
        LOG.info("[FASC][{}] ControlTopicProducer initialised for topic={}",
                config.getAppId(), controlTopic);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Sends the given {@link ControlSignal} asynchronously to the control topic.
     *
     * <p>The Kafka message key is set to {@code signal.getTargetAppId()} so that all
     * signals directed at the same application land on the same partition (ordered
     * delivery).  Broadcast signals (key = {@code "broadcast"}) are distributed
     * round-robin.
     *
     * <p>Any Kafka send errors are logged at ERROR level but are not re-thrown, keeping
     * the hot-path non-blocking.  For guaranteed delivery use {@link #sendSync}.
     *
     * @param signal the signal to send; must not be {@code null}
     */
    public void send(ControlSignal signal) {
        if (signal == null) {
            throw new IllegalArgumentException("ControlTopicProducer.send: signal must not be null");
        }
        byte[] valueBytes = serde.serialize(signal);
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(controlTopic, signal.getTargetAppId(), valueBytes);

        LOG.info("[FASC] Sending signal type={} target={} traceId={} coordinatorId={}",
                signal.getSignalType(), signal.getTargetAppId(),
                signal.getTraceId(), signal.getCoordinatorId());

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOG.error("[FASC] Failed to send signal type={} traceId={} to topic={} — {}",
                        signal.getSignalType(), signal.getTraceId(), controlTopic,
                        exception.getMessage(), exception);
            } else {
                LOG.debug("[FASC] Signal type={} traceId={} delivered to partition={} offset={}",
                        signal.getSignalType(), signal.getTraceId(),
                        metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Sends the given {@link ControlSignal} synchronously and waits for the broker
     * acknowledgement before returning.
     *
     * <p>Use this for control-plane operations where you need to be certain the
     * message has been committed before proceeding (e.g. coordinator sending
     * {@link SignalType#PROMOTE}).
     *
     * @param signal the signal to send; must not be {@code null}
     * @throws RuntimeException if the Kafka send or broker acknowledgement fails
     */
    public void sendSync(ControlSignal signal) {
        if (signal == null) {
            throw new IllegalArgumentException("ControlTopicProducer.sendSync: signal must not be null");
        }
        byte[] valueBytes = serde.serialize(signal);
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(controlTopic, signal.getTargetAppId(), valueBytes);

        LOG.info("[FASC] Sending (sync) signal type={} target={} traceId={} coordinatorId={}",
                signal.getSignalType(), signal.getTargetAppId(),
                signal.getTraceId(), signal.getCoordinatorId());

        Future<RecordMetadata> future = producer.send(record);
        try {
            RecordMetadata metadata = future.get();
            LOG.info("[FASC] Signal type={} traceId={} committed to partition={} offset={}",
                    signal.getSignalType(), signal.getTraceId(),
                    metadata.partition(), metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while waiting for Kafka ack for signal type=" + signal.getSignalType(), e);
        } catch (ExecutionException e) {
            throw new RuntimeException(
                    "Kafka send failed for signal type=" + signal.getSignalType()
                    + " traceId=" + signal.getTraceId(), e.getCause());
        }
    }

    /**
     * Flushes any pending messages and closes the underlying {@link KafkaProducer}.
     * Subsequent calls to {@link #send} or {@link #sendSync} will throw an exception.
     */
    @Override
    public void close() {
        LOG.info("[FASC] Closing ControlTopicProducer for topic={}", controlTopic);
        try {
            producer.flush();
        } finally {
            producer.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds the Kafka producer {@link Properties} from a {@link FASCConfiguration}.
     *
     * @param config source configuration
     * @return ready-to-use Kafka producer properties
     */
    private static Properties buildProperties(FASCConfiguration config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  config.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,     ACKS_ALL);
        props.put(ProducerConfig.RETRIES_CONFIG,  String.valueOf(RETRIES));
        props.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(LINGER_MS));
        // Idempotent producer avoids duplicate messages on retry
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // Client ID aids monitoring / debugging in MSK
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fasc-producer-" + config.getAppId());
        return props;
    }
}
