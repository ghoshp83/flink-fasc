package com.flinkfasc.coordinator.api;

import com.flinkfasc.coordinator.handoff.HandoffState;

public class StatusResponse {
    private HandoffState handoffState;
    private String       currentFlinkLeader;
    private long         app2MaxLagMs;
    private boolean      app2Ready;
    private String       coordinatorId;
    private boolean      isLockHolder;
    private long         uptimeMs;
    private String       lastHandoffTraceId;

    public HandoffState getHandoffState()         { return handoffState; }
    public void setHandoffState(HandoffState s)   { this.handoffState = s; }
    public String getCurrentFlinkLeader()         { return currentFlinkLeader; }
    public void setCurrentFlinkLeader(String s)   { this.currentFlinkLeader = s; }
    public long getApp2MaxLagMs()                 { return app2MaxLagMs; }
    public void setApp2MaxLagMs(long l)           { this.app2MaxLagMs = l; }
    public boolean isApp2Ready()                  { return app2Ready; }
    public void setApp2Ready(boolean b)           { this.app2Ready = b; }
    public String getCoordinatorId()              { return coordinatorId; }
    public void setCoordinatorId(String s)        { this.coordinatorId = s; }
    public boolean isLockHolder()                 { return isLockHolder; }
    public void setLockHolder(boolean b)          { this.isLockHolder = b; }
    public long getUptimeMs()                     { return uptimeMs; }
    public void setUptimeMs(long l)               { this.uptimeMs = l; }
    public String getLastHandoffTraceId()         { return lastHandoffTraceId; }
    public void setLastHandoffTraceId(String s)   { this.lastHandoffTraceId = s; }
}
