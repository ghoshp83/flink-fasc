package com.flinkfasc.coordinator.monitor;

import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Monitors the per-partition Kafka consumer lag between App1 and App2.
 *
 * <p>Compares committed offsets of {@code consumer-group-app1} and
 * {@code consumer-group-app2} on the business topic. When App2's lag is zero
 * and has been stable for {@code lagStabilityWindowMs}, all registered
 * {@link #onApp2Ready(Runnable)} callbacks are invoked exactly once.
 */
@Service
public class OffsetMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(OffsetMonitor.class);

    private final FascCoordinatorProperties props;
    private AdminClient adminClient;

    /** Per-partition lag: partition → (app1Offset - app2Offset). */
    private final Map<Integer, Long> currentLag = new ConcurrentHashMap<>();

    /** Timestamp when lag first reached zero. Null while lag > 0. Atomic to avoid race on check-then-set. */
    private final AtomicReference<Instant> lagZeroSince = new AtomicReference<>(null);

    /** Whether the readiness callbacks have already been fired for current cycle. */
    private final AtomicBoolean readinessFired = new AtomicBoolean(false);

    private final List<Runnable> readinessCallbacks = new CopyOnWriteArrayList<>();

    public OffsetMonitor(FascCoordinatorProperties props) {
        this.props = props;
    }

    @PostConstruct
    public void init() {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "15000");
        this.adminClient = AdminClient.create(adminProps);
        LOG.info("OffsetMonitor initialised. Monitoring groups: {} vs {}",
                props.getApp1ConsumerGroup(), props.getApp2ConsumerGroup());
    }

    /**
     * Register a callback invoked once when App2 is ready (lag = 0, stable).
     * The callback fires at most once per readiness cycle. It is cleared after
     * {@link #resetReadiness()} is called (e.g. when a new handoff begins).
     */
    public void onApp2Ready(Runnable callback) {
        readinessCallbacks.add(callback);
    }

    /** Clears the readiness state — call this when starting a new handoff cycle. */
    public void resetReadiness() {
        lagZeroSince.set(null);
        readinessFired.set(false);
        LOG.info("OffsetMonitor readiness state reset");
    }

    /**
     * Returns the maximum consumer-group lag across all partitions.
     * A value of 0 means App2 has caught up to App1's position on every partition.
     */
    public long getMaxLag() {
        if (currentLag.isEmpty()) return Long.MAX_VALUE;
        return currentLag.values().stream().mapToLong(Long::longValue).max().orElse(0L);
    }

    /** Returns a snapshot of per-partition lag values. */
    public Map<Integer, Long> getCurrentLag() {
        return Collections.unmodifiableMap(currentLag);
    }

    /**
     * Returns true if App2 lag = 0 on all partitions AND has been stable
     * for at least {@code lagStabilityWindowMs}.
     */
    public boolean isApp2Ready() {
        Instant zeroSince = lagZeroSince.get();
        if (zeroSince == null) return false;
        long stableMs = Instant.now().toEpochMilli() - zeroSince.toEpochMilli();
        return stableMs >= props.getLagStabilityWindowMs();
    }

    @Scheduled(fixedDelayString = "${fasc.offsetMonitorIntervalMs:5000}")
    public void monitorOffsets() {
        try {
            Map<Integer, Long> app1Offsets = fetchCommittedOffsets(props.getApp1ConsumerGroup());
            Map<Integer, Long> app2Offsets = fetchCommittedOffsets(props.getApp2ConsumerGroup());

            if (app1Offsets.isEmpty() || app2Offsets.isEmpty()) {
                LOG.debug("One or both consumer groups have no committed offsets yet");
                return;
            }

            long maxLag = 0;
            for (Map.Entry<Integer, Long> entry : app1Offsets.entrySet()) {
                int partition   = entry.getKey();
                long app1Offset = entry.getValue();
                long app2Offset = app2Offsets.getOrDefault(partition, 0L);
                long lag        = Math.max(0, app1Offset - app2Offset);
                currentLag.put(partition, lag);
                maxLag = Math.max(maxLag, lag);
            }

            LOG.debug("Consumer lag — maxLag={}, partitions={}", maxLag, currentLag);

            if (maxLag == 0) {
                // Atomically set lagZeroSince only if it was null (first time lag hits zero)
                if (lagZeroSince.compareAndSet(null, Instant.now())) {
                    LOG.info("App2 consumer lag reached zero. Starting stability window ({} ms)",
                             props.getLagStabilityWindowMs());
                }
                if (!readinessFired.get() && isApp2Ready() && readinessFired.compareAndSet(false, true)) {
                    LOG.info("App2 is READY — lag=0 stable for {} ms. Firing readiness callbacks.",
                             props.getLagStabilityWindowMs());
                    readinessCallbacks.forEach(cb -> {
                        try { cb.run(); } catch (Exception e) {
                            LOG.error("Readiness callback threw exception", e);
                        }
                    });
                }
            } else {
                if (lagZeroSince.getAndSet(null) != null) {
                    LOG.info("App2 lag increased again (lag={}). Resetting stability window.", maxLag);
                }
                readinessFired.set(false);
            }

        } catch (Exception e) {
            LOG.error("Error monitoring offsets", e);
        }
    }

    private Map<Integer, Long> fetchCommittedOffsets(String consumerGroup) {
        try {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(consumerGroup);
            Map<Integer, Long> offsets = new HashMap<>();
            // Bounded timeout prevents this from blocking the scheduler thread indefinitely
            // if Kafka AdminClient is slow or the broker is temporarily unreachable.
            result.partitionsToOffsetAndMetadata()
                  .get(10, TimeUnit.SECONDS)
                  .forEach((tp, oam) -> {
                      if (tp.topic().equals(getBusinessTopic())) {
                          offsets.put(tp.partition(), oam.offset());
                      }
                  });
            return offsets;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyMap();
        } catch (TimeoutException e) {
            LOG.warn("Timed out fetching offsets for consumer group {} (>10 s) — skipping this tick", consumerGroup);
            return Collections.emptyMap();
        } catch (ExecutionException e) {
            LOG.warn("Failed to fetch offsets for consumer group {}: {}", consumerGroup, e.getMessage());
            return Collections.emptyMap();
        }
    }

    private String getBusinessTopic() {
        return props.getBusinessTopic() != null ? props.getBusinessTopic() : "business-topic";
    }

    @PreDestroy
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
