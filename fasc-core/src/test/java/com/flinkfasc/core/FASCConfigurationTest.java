package com.flinkfasc.core;

import com.flinkfasc.core.sink.SinkMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FASCConfigurationTest {

    @Test
    void builderSetsAllFields() {
        FASCConfiguration config = FASCConfiguration.builder()
                .bootstrapServers("broker1:9094,broker2:9094")
                .appId("app1")
                .controlTopic("my-control-topic")
                .heartbeatTimeoutMs(60_000)
                .heartbeatIntervalMs(10_000)
                .initialMode(SinkMode.ACTIVE)
                .build();

        assertEquals("broker1:9094,broker2:9094", config.getBootstrapServers());
        assertEquals("app1", config.getAppId());
        assertEquals("my-control-topic", config.getControlTopic());
        assertEquals(60_000, config.getHeartbeatTimeoutMs());
        assertEquals(10_000, config.getHeartbeatIntervalMs());
        assertEquals(SinkMode.ACTIVE, config.getInitialMode());
    }

    @Test
    void defaultsAreAppliedWhenNotSet() {
        FASCConfiguration config = FASCConfiguration.builder()
                .bootstrapServers("broker1:9094")
                .appId("app2")
                .build();

        assertEquals("fasc-control-topic", config.getControlTopic());
        assertEquals(30_000, config.getHeartbeatTimeoutMs());
        assertEquals(5_000, config.getHeartbeatIntervalMs());
        assertEquals(SinkMode.SHADOW, config.getInitialMode());
        assertEquals("fasc-control-consumer-app2", config.getConsumerGroupId());
    }

    @Test
    void validate_missingBootstrapServers_throws() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                FASCConfiguration.builder()
                        .appId("app1")
                        .build()
                        .validate()
        );
        assertTrue(ex.getMessage().contains("bootstrapServers"));
    }

    @Test
    void validate_missingAppId_throws() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                FASCConfiguration.builder()
                        .bootstrapServers("broker1:9094")
                        .build()
                        .validate()
        );
        assertTrue(ex.getMessage().contains("appId"));
    }

    @Test
    void validate_validConfig_doesNotThrow() {
        assertDoesNotThrow(() ->
                FASCConfiguration.builder()
                        .bootstrapServers("broker1:9094")
                        .appId("app1")
                        .build()
                        .validate()
        );
    }
}
