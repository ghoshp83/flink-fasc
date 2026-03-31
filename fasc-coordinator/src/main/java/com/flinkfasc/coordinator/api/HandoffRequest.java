package com.flinkfasc.coordinator.api;

public class HandoffRequest {
    private String requestedBy;
    private String reason;

    public String getRequestedBy() { return requestedBy; }
    public void setRequestedBy(String requestedBy) { this.requestedBy = requestedBy; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
}
