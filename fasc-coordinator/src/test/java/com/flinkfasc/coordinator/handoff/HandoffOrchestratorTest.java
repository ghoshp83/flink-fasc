package com.flinkfasc.coordinator.handoff;

import com.flinkfasc.coordinator.config.FascCoordinatorProperties;
import com.flinkfasc.coordinator.election.LeaderElection;
import com.flinkfasc.coordinator.monitor.OffsetMonitor;
import com.flinkfasc.coordinator.savepoint.SavepointManager;
import com.flinkfasc.coordinator.savepoint.SavepointMetadata;
import com.flinkfasc.coordinator.savepoint.SavepointStatus;
import com.flinkfasc.core.control.ControlSignal;
import com.flinkfasc.core.control.ControlTopicProducer;
import com.flinkfasc.core.control.SignalType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for HandoffOrchestrator state machine.
 * All external dependencies are mocked.
 */
@ExtendWith(MockitoExtension.class)
class HandoffOrchestratorTest {

    @Mock SavepointManager savepointManager;
    @Mock OffsetMonitor offsetMonitor;
    @Mock LeaderElection leaderElection;
    @Mock ControlTopicProducer controlProducer;

    private HandoffOrchestrator orchestrator;
    private FascCoordinatorProperties props;

    @BeforeEach
    void setUp() {
        props = new FascCoordinatorProperties();
        props.setApp1JobId("job-app1-abc");
        props.setApp2JobId("job-app2-xyz");
        props.setCoordinatorId("test-coordinator");
        props.setSavepointS3Bucket("my-bucket");
        props.setSavepointS3Prefix("fasc/savepoints/");

        orchestrator = new HandoffOrchestrator(
                props, savepointManager, offsetMonitor, leaderElection, controlProducer
        );
    }

    @Test
    void initialStateIsIdle() {
        assertEquals(HandoffState.IDLE, orchestrator.getState());
    }

    @Test
    void triggerHandoff_whenIdle_transitionsToSavepointInProgress() throws Exception {
        when(leaderElection.isLockHolder("test-coordinator")).thenReturn(true);
        SavepointMetadata mockMeta = SavepointMetadata.builder()
                .s3Location("s3://my-bucket/fasc/savepoints/sp-001")
                .kafkaOffsets(Map.of(0, 1000L, 1, 999L))
                .status(SavepointStatus.COMPLETED)
                .build();
        when(savepointManager.bootstrapApp2FromApp1(anyString())).thenReturn(mockMeta);

        orchestrator.triggerHandoff();

        verify(savepointManager, times(1)).bootstrapApp2FromApp1("job-app1-abc");
    }

    @Test
    void triggerHandoff_whenNotLockHolder_throws() {
        when(leaderElection.isLockHolder("test-coordinator")).thenReturn(false);

        assertThrows(IllegalStateException.class, () -> orchestrator.triggerHandoff());
        assertEquals(HandoffState.IDLE, orchestrator.getState());
    }

    @Test
    void triggerHandoff_whenAlreadyInProgress_throws() throws Exception {
        when(leaderElection.isLockHolder("test-coordinator")).thenReturn(true);
        when(savepointManager.bootstrapApp2FromApp1(anyString()))
                .thenAnswer(inv -> {
                    Thread.sleep(200);
                    return null;
                });

        // Start first handoff in background
        Thread t = new Thread(() -> {
            try { orchestrator.triggerHandoff(); } catch (Exception ignored) {}
        });
        t.start();
        Thread.sleep(50); // let it start

        // Second trigger should fail
        assertThrows(IllegalStateException.class, () -> orchestrator.triggerHandoff());
        t.join(500);
    }

    @Test
    void onDrainedAt_triggersApp2Promotion() {
        // Set up orchestrator in HANDOFF_INITIATED state with agreed offsets
        Map<Integer, Long> cutoverOffsets = Map.of(0, 2000L, 1, 1999L);
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN, cutoverOffsets, "trace-test");

        when(leaderElection.getCurrentFlinkLeaderVersion()).thenReturn(5);
        when(leaderElection.transferFlinkLeader("app1", "app2", 5)).thenReturn(true);

        ControlSignal drainedAt = ControlSignal.drainedAt("app1", "trace-test", cutoverOffsets);
        orchestrator.onSignalReceived(drainedAt);

        verify(leaderElection).transferFlinkLeader("app1", "app2", 5);
        verify(controlProducer).sendSync(argThat(s -> s.getSignalType() == SignalType.PROMOTE));
        verify(controlProducer).sendSync(argThat(s -> s.getSignalType() == SignalType.DEMOTE));
    }

    @Test
    void onPromotedConfirmed_completesHandoff() {
        orchestrator.setStateForTest(HandoffState.PROMOTING_APP2, Map.of(), "trace-test");

        ControlSignal confirmed = ControlSignal.promotedConfirmed("app2", "trace-test");
        orchestrator.onSignalReceived(confirmed);

        assertEquals(HandoffState.HANDOFF_COMPLETE, orchestrator.getState());
    }

    @Test
    void onDrainTimeout_abortsHandoff() {
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN, Map.of(), "trace-test");

        orchestrator.onDrainTimeout();

        assertEquals(HandoffState.HANDOFF_FAILED, orchestrator.getState());
        verify(controlProducer).send(argThat(s -> s.getSignalType() == SignalType.HANDOFF_ABORT));
    }

    @Test
    void leaderTransferFails_abortsHandoff() {
        Map<Integer, Long> cutoverOffsets = Map.of(0, 2000L);
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN, cutoverOffsets, "trace-test");

        when(leaderElection.getCurrentFlinkLeaderVersion()).thenReturn(5);
        when(leaderElection.transferFlinkLeader("app1", "app2", 5)).thenReturn(false);

        ControlSignal drainedAt = ControlSignal.drainedAt("app1", "trace-test", cutoverOffsets);
        orchestrator.onSignalReceived(drainedAt);

        assertEquals(HandoffState.HANDOFF_FAILED, orchestrator.getState());
    }

    @Test
    void signalsFromWrongTraceId_areIgnored() {
        Map<Integer, Long> cutoverOffsets = Map.of(0, 2000L);
        orchestrator.setStateForTest(HandoffState.WAITING_FOR_APP1_DRAIN, cutoverOffsets, "trace-correct");

        // Signal with wrong traceId
        ControlSignal wrongTrace = ControlSignal.drainedAt("app1", "trace-WRONG", cutoverOffsets);
        orchestrator.onSignalReceived(wrongTrace);

        // State should not change
        assertEquals(HandoffState.WAITING_FOR_APP1_DRAIN, orchestrator.getState());
        verifyNoInteractions(leaderElection);
    }
}
