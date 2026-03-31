package com.flinkfasc.coordinator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.flinkfasc.coordinator.config.FascCoordinatorProperties;

/**
 * Entry point for the FASC (Flink Active-Standby Coordinator) Spring Boot service.
 *
 * <p>This microservice orchestrates zero-downtime, zero-message-loss handoffs between two
 * Flink applications (App1 and App2) by:
 * <ol>
 *   <li>Taking a savepoint of the active application (App1)</li>
 *   <li>Bootstrapping the standby application (App2) from that savepoint</li>
 *   <li>Monitoring Kafka consumer-group lag until App2 catches up to App1</li>
 *   <li>Atomically transferring the DynamoDB writer role via a conditional DynamoDB write</li>
 *   <li>Signalling App1 to drain and App2 to promote via the FASC control Kafka topic</li>
 * </ol>
 *
 * <p>Two replicas run on EKS; only the one that holds the DynamoDB coordinator lock
 * executes handoff logic — the other is a hot standby that takes over if the lock
 * holder becomes unhealthy.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(FascCoordinatorProperties.class)
public class FascCoordinatorApplication {

    /**
     * Application entry point.
     *
     * @param args command-line arguments (passed through to Spring Boot)
     */
    public static void main(String[] args) {
        SpringApplication.run(FascCoordinatorApplication.class, args);
    }
}
