package com.flinkfasc.coordinator.config;

import com.flinkfasc.core.FASCConfiguration;
import com.flinkfasc.core.control.ControlTopicProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring {@link Configuration} that creates the coordinator's Kafka control-topic beans.
 *
 * <p>The coordinator publishes {@link com.flinkfasc.core.control.ControlSignal}s to the
 * fasc-control-topic (via {@link ControlTopicProducer}) and subscribes to it to receive
 * responses from the Flink apps (via
 * {@link com.flinkfasc.coordinator.control.CoordinatorControlListener}).
 *
 * <h3>Why a separate {@code FASCConfiguration} bean?</h3>
 * {@link FASCConfiguration} is the fasc-core value object that bundles bootstrap servers,
 * control-topic name, and app identity. The coordinator identifies itself as {@code "coordinator"}
 * so that broadcast signals are routed correctly and log lines are identifiable.
 */
@Configuration
public class CoordinatorKafkaConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorKafkaConfiguration.class);

    /**
     * Builds the {@link FASCConfiguration} for the coordinator identity.
     *
     * @param props coordinator configuration (bootstrap servers, control topic)
     * @return a fully populated {@link FASCConfiguration}
     */
    @Bean
    public FASCConfiguration coordinatorFascConfiguration(FascCoordinatorProperties props) {
        LOG.info("Creating coordinator FASCConfiguration: bootstrapServers={} controlTopic={} appId=coordinator",
                props.getBootstrapServers(), props.getControlTopic());
        return FASCConfiguration.builder()
                .bootstrapServers(props.getBootstrapServers())
                .appId("coordinator")
                .controlTopic(props.getControlTopic())
                .build();
    }

    /**
     * Creates the {@link ControlTopicProducer} used by {@link com.flinkfasc.coordinator.handoff.HandoffOrchestrator}
     * to publish control signals to Flink apps.
     *
     * @param coordinatorFascConfiguration the coordinator's FASC identity bean
     * @return a ready-to-use {@link ControlTopicProducer}
     */
    @Bean
    public ControlTopicProducer controlTopicProducer(FASCConfiguration coordinatorFascConfiguration) {
        LOG.info("Creating ControlTopicProducer for coordinator");
        return new ControlTopicProducer(coordinatorFascConfiguration);
    }
}
