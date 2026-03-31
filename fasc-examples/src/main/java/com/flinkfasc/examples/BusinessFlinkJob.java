package com.flinkfasc.examples;

import com.flinkfasc.core.FASCConfiguration;
import com.flinkfasc.core.ShadowSink;
import com.flinkfasc.core.sink.SinkMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Reference implementation of a FASC-enabled Flink job.
 *
 * <p>This job demonstrates the complete FASC integration pattern:
 * <ol>
 *   <li>Load {@link FASCConfiguration} from environment variables</li>
 *   <li>Configure RocksDB state backend + S3 checkpointing</li>
 *   <li>Read from MSK using a separate consumer group per app instance</li>
 *   <li>Apply business logic with event-time processing (critical for determinism)</li>
 *   <li>Wrap the DynamoDB sink with {@link ShadowSink} — the only FASC integration point</li>
 * </ol>
 *
 * <p><strong>Environment variables required:</strong>
 * <pre>
 * FASC_BOOTSTRAP_SERVERS   MSK bootstrap brokers (TLS endpoint)
 * FASC_APP_ID              "app1" or "app2"
 * FASC_CONTROL_TOPIC       fasc-control-topic (default)
 * FASC_INITIAL_MODE        ACTIVE (app1) or SHADOW (app2)
 * KAFKA_BOOTSTRAP_SERVERS  MSK bootstrap brokers for business topic
 * KAFKA_BUSINESS_TOPIC     business-topic
 * DYNAMODB_TABLE_NAME      DynamoDB business data table name
 * AWS_REGION               eu-west-2
 * FLINK_CHECKPOINT_DIR     s3://bucket/fasc/checkpoints/{appId}/
 * </pre>
 */
public class BusinessFlinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(BusinessFlinkJob.class);

    public static void main(String[] args) throws Exception {

        // ── 1. FASC Configuration ─────────────────────────────────────────────
        FASCConfiguration fascConfig = FASCConfiguration.fromEnv();
        fascConfig.validate();

        LOG.info("Starting BusinessFlinkJob: appId={} initialMode={}",
                 fascConfig.getAppId(), fascConfig.getInitialMode());

        // ── 2. Flink Environment ──────────────────────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // RocksDB state backend with incremental checkpoints
        // Incremental checkpoints significantly reduce checkpoint size for large states
        EmbeddedRocksDBStateBackend rocksDb = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDb);

        // Checkpoint storage in S3 — shared bucket, per-app prefix
        String checkpointDir = System.getenv("FLINK_CHECKPOINT_DIR");
        if (checkpointDir != null && !checkpointDir.isEmpty()) {
            env.getCheckpointConfig().setCheckpointStorage(
                new FileSystemCheckpointStorage(checkpointDir));
        }

        // Checkpointing: exactly-once, every 30 seconds
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // ── 3. MSK Kafka Source ───────────────────────────────────────────────
        // CRITICAL: each app uses its own consumer group to consume ALL 24 partitions
        // independently. They must NOT share a consumer group.
        String consumerGroup = "consumer-group-" + fascConfig.getAppId();

        KafkaSource<BusinessEvent> kafkaSource = KafkaSource.<BusinessEvent>builder()
                .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
                .setTopics(System.getenv("KAFKA_BUSINESS_TOPIC"))
                .setGroupId(consumerGroup)
                .setStartingOffsets(
                    // Resume from last committed offset; on first start fall back to earliest
                    OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new BusinessEventDeserializer())
                .build();

        // ── 4. Watermark Strategy — event time (CRITICAL for determinism) ─────
        // Both apps must produce identical state from identical input messages.
        // Using event time guarantees this regardless of when each app processes the event.
        // NEVER use System.currentTimeMillis() in business logic.
        WatermarkStrategy<BusinessEvent> watermarkStrategy = WatermarkStrategy
                .<BusinessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.getEventTimestamp())
                .withIdleness(Duration.ofSeconds(30));

        // ── 5. Business Processing Pipeline ──────────────────────────────────
        DataStream<CustomerState> processedStream = env
                .fromSource(kafkaSource, watermarkStrategy,
                            "MSK-Source[" + fascConfig.getAppId() + "]")
                .keyBy(BusinessEvent::getCustomerId)
                .process(new BusinessProcessFunction())
                .name("BusinessStateProcessor[" + fascConfig.getAppId() + "]");

        // ── 6. FASC ShadowSink — the only integration point ───────────────────
        // Wrapping the real DynamoDB sink with ShadowSink is the single change
        // required to enable FASC in an existing Flink job.
        //
        // In ACTIVE mode:  writes flow through to DynamoDB as normal
        // In SHADOW mode:  writes are suppressed (counted but not sent)
        // Mode is controlled dynamically by the FASC coordinator via Kafka signals
        DynamoDbSink dynamoDbSink = new DynamoDbSink(
                System.getenv("DYNAMODB_TABLE_NAME"),
                System.getenv("AWS_REGION"));

        processedStream
                .addSink(ShadowSink.wrap(dynamoDbSink, fascConfig))
                .name("ShadowSink→DynamoDB[" + fascConfig.getAppId() + "]");

        // ── 7. Execute ────────────────────────────────────────────────────────
        String jobName = "BusinessFlinkJob[" + fascConfig.getAppId() + "]";
        LOG.info("Submitting job: {}", jobName);
        env.execute(jobName);
    }
}
