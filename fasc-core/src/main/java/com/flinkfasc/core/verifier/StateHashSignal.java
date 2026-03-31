package com.flinkfasc.core.verifier;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Published by an app to the fasc-control-topic to allow the coordinator
 * to verify state consistency between App1 and App2 before handoff.
 *
 * <p>Both apps process the same probe key and publish their resulting state hash.
 * If the hashes match, states are consistent and handoff can proceed safely.
 */
public class StateHashSignal implements Serializable {

    private final String appId;
    private final String stateKey;
    private final String stateHash;
    private final long kafkaOffset;
    private final long timestamp;
    private final String traceId;

    @JsonCreator
    public StateHashSignal(
            @JsonProperty("appId")       String appId,
            @JsonProperty("stateKey")    String stateKey,
            @JsonProperty("stateHash")   String stateHash,
            @JsonProperty("kafkaOffset") long kafkaOffset,
            @JsonProperty("timestamp")   long timestamp,
            @JsonProperty("traceId")     String traceId) {
        this.appId       = appId;
        this.stateKey    = stateKey;
        this.stateHash   = stateHash;
        this.kafkaOffset = kafkaOffset;
        this.timestamp   = timestamp;
        this.traceId     = traceId;
    }

    public String getAppId()       { return appId; }
    public String getStateKey()    { return stateKey; }
    public String getStateHash()   { return stateHash; }
    public long   getKafkaOffset() { return kafkaOffset; }
    public long   getTimestamp()   { return timestamp; }
    public String getTraceId()     { return traceId; }

    @Override
    public String toString() {
        return "StateHashSignal{appId='" + appId + "', stateKey='" + stateKey +
               "', stateHash='" + stateHash + "', kafkaOffset=" + kafkaOffset +
               ", traceId='" + traceId + "'}";
    }
}
