# Flink Active-Standby Coordinator (FASC) — Production Design
**Date:** 2026-03-29
**Status:** Final Design — Selected Approach
**Project Name:** `flink-fasc`
**Goal:** Zero-downtime, zero-message-loss upgrade protocol for two Flink applications
sharing business state, deployable on EKS, open-sourceable.

---

## Table of Contents
1. [Problem Statement](#1-problem-statement)
2. [Business Requirements](#2-business-requirements)
3. [Design Evolution — Why FASC](#3-design-evolution--why-fasc)
4. [FASC Architecture Overview](#4-fasc-architecture-overview)
5. [Core Components — Detailed Design](#5-core-components--detailed-design)
6. [Kafka Topics](#6-kafka-topics)
7. [Three-Phase Production Protocol](#7-three-phase-production-protocol)
8. [Upgrade Flow — Step by Step](#8-upgrade-flow--step-by-step)
9. [Edge Cases and Failure Handling](#9-edge-cases-and-failure-handling)
10. [AWS Infrastructure](#10-aws-infrastructure)
11. [Metrics and Observability](#11-metrics-and-observability)
12. [Open-Source Design](#12-open-source-design)
13. [Next Steps](#13-next-steps)

---

## 1. Problem Statement

Two Flink apps (App1 — Active, App2 — Standby) consume the same MSK Kafka topic
(24 partitions). Each app accumulates business state over time via Flink's state
backend (RocksDB). During a blue-green upgrade swap:

- App2 starts with a **fresh empty state** (`stateSB`)
- App1 has **months of accumulated business state** (`stateX`)
- State mismatch → App2 makes wrong business decisions → **incorrect DynamoDB writes**

**Root cause:** There is no mechanism to transfer or synchronize state between two
independent Flink jobs. Flink's internal state (RocksDB) is not accessible externally.
Kafka topic retention (typically 7 days) cannot rebuild state accumulated over months.

---

## 2. Business Requirements

| Requirement | Constraint |
|---|---|
| No Downtime | Zero interruption during upgrades |
| No Message Loss | Every Kafka message must be processed |
| Latency | < 2 seconds end-to-end |
| Duplicate Tolerance | DynamoDB writes are idempotent — replays are safe |
| Deployment | EKS (self-managed Flink) for full operational control |

---

## 3. Design Evolution — Why FASC

Four approaches were evaluated. Each solved part of the problem but had gaps:

| Option | Approach | Fatal Gap |
|---|---|---|
| Option 1 | Flink native savepoints (blue-green) | Manual, no warm standby, brief blackout window |
| Option 2 | Externalized state (Redis/DynamoDB) | Adds latency, violates 2s SLA |
| Option 3 | State-delta streaming via Kafka (FSSP) | Complex custom operator, dual-write consistency risk |
| Option 4 | Dual consumer groups + gateway sink control | Kafka retention cannot rebuild full historical state |

**FASC combines the correct pieces from Options 1 and 4:**
- **Savepoint bootstrap** (Option 1) → solves the state initialization problem
- **Shadow mode + offset-based handoff** (Option 4) → solves the ongoing sync and switchover
- **No state-delta streaming** → no custom operator, no dual-write risk
- **Coordinator microservice** → manages the full lifecycle on EKS

### Why Kafka Replay Alone (Option 4) Fails

```
Business state built over:   6 months of Kafka messages
Kafka topic retention:        7 days  (standard MSK default)

App2 replays 7 days → builds 7-day state
App1 has state     → built from 6 months

Result: States are NOT identical even after App2 consumer lag = 0
```

### Why Savepoint Bootstrap + Shadow Mode Succeeds

```
App2 state after savepoint bootstrap  = App1 state at offset X    [savepoint guarantee]
App2 processes same messages X → C    = same Kafka, same order     [Kafka guarantee]
Business logic is deterministic        = event time, no wall clock  [design constraint]
                                                ↓
App2 state at offset C  ≡  App1 state at offset C                 [mathematically guaranteed]
```

Kafka retention is irrelevant. State comes from the savepoint, not from replaying history.

---

## 4. FASC Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  AWS Cloud — EKS Deployment                                                  │
│                                                                              │
│  MSK Cluster                                                                 │
│  ┌─────────────────────────────────────────────────────────┐                │
│  │  business-topic       (24 partitions, 7-day retention)  │                │
│  │  fasc-control-topic   (1  partition,  24h retention)    │                │
│  └─────────────────────────────────────────────────────────┘                │
│          │                              │                                    │
│          │  consumer-group-app1         │  consumer-group-app2              │
│          │  (all 24 partitions)         │  (all 24 partitions)              │
│          v                              v                                    │
│  ┌──────────────────┐         ┌──────────────────────────────────────────┐  │
│  │  App1 (ACTIVE)   │         │  App2 (SHADOW / WARM STANDBY)            │  │
│  │                  │         │                                          │  │
│  │  Business Logic  │         │  Business Logic (same code)              │  │
│  │  RocksDB State   │         │  RocksDB State  ← from App1 savepoint    │  │
│  │                  │         │                                          │  │
│  │  ShadowSink      │         │  ShadowSink                              │  │
│  │  [ACTIVE]        │         │  [SUPPRESSED]                            │  │
│  │       │          │         │       │ (writes suppressed)              │  │
│  └───────┼──────────┘         └───────┼──────────────────────────────────┘  │
│          │                            │                                      │
│          │  DynamoDB writes            │  reads fasc-control-topic           │
│          v                            │                                      │
│  ┌────────────────┐                   │                                      │
│  │   DynamoDB     │◄──────────────────┘  (writes only when PROMOTED)        │
│  │  Business Table│                                                          │
│  │  FASC Leader   │◄─── conditional writes from FASC Coordinator            │
│  │  Table         │                                                          │
│  └────────────────┘                                                          │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  FASC Coordinator (EKS — 2 replicas, one active via DynamoDB lock)   │   │
│  │                                                                      │   │
│  │  SavepointManager   OffsetTracker   HandoffOrchestrator   LeaderElect│   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  S3                                                                          │
│  ┌─────────────────────────────────────────────┐                           │
│  │  s3://bucket/fasc/savepoints/               │                           │
│  │  s3://bucket/fasc/checkpoints/app1/         │                           │
│  │  s3://bucket/fasc/checkpoints/app2/         │                           │
│  └─────────────────────────────────────────────┘                           │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Core Components — Detailed Design

---

### 5.1 ShadowSink (Flink Library — inside App1 and App2)

A **drop-in Flink sink wrapper** that adds active/shadow mode control to any existing
sink. This is the core library contribution of FASC.

**How it works:**
```
User's existing code:
    stream.addSink(new DynamoDbSink(...))

With FASC:
    stream.addSink(ShadowSink.wrap(new DynamoDbSink(...), fascConfig))
```

**Internals:**

```
ShadowSink<T> implements SinkFunction<T>
    │
    ├── inner: SinkFunction<T>          ← the real sink (DynamoDB, etc.)
    ├── mode: AtomicEnum {ACTIVE, SHADOW}
    ├── controlTopicConsumer            ← polls fasc-control-topic
    │
    invoke(record, context):
        if mode == ACTIVE:
            inner.invoke(record, context)    ← real write
        else:
            metrics.shadowDropCounter.inc()  ← counted, not written
    │
    onControlMessage(signal):
        PROMOTE   → mode = ACTIVE
        DEMOTE    → mode = SHADOW
        HEARTBEAT → update lastCoordinatorContact
```

**Key behaviors:**
- Mode transitions are instantaneous and thread-safe (`AtomicReference`)
- If coordinator heartbeat is lost for > 30s, ShadowSink raises an alert but
  **does NOT self-promote** — avoids split-brain on coordinator failure
- Exposes `fasc.sink.mode` metric (0=SHADOW, 1=ACTIVE) to CloudWatch
- Works with any Flink sink: DynamoDB, S3, Elasticsearch, Kafka, JDBC

---

### 5.2 SavepointManager (inside FASC Coordinator)

Automates the savepoint lifecycle for App1 → App2 bootstrap.

**Savepoint Bootstrap Protocol:**

```
Step 1: Coordinator calls Flink REST API on App1:
        POST /jobs/{jobId}/savepoints
        Body: { "target-directory": "s3://bucket/fasc/savepoints/sp-{timestamp}" }

Step 2: Poll until savepoint is COMPLETED:
        GET /jobs/{jobId}/savepoints/{savepointId}
        → returns { "status": "COMPLETED", "location": "s3://..." }
        → also returns: last committed Kafka offset per partition

Step 3: Record savepoint metadata in DynamoDB:
        { PK: "fasc-savepoint-latest",
          s3Location: "s3://...",
          kafkaOffsets: { "0": 10234, "1": 10891, ... },  ← per partition
          timestamp: ...,
          app1JobId: "..." }

Step 4: Start App2 with savepoint:
        Flink job submission with --fromSavepoint s3://...
        App2 restores RocksDB state from savepoint
        App2 resumes Kafka consumption from the exact offsets in savepoint metadata
        App2 ShadowSink starts in SHADOW mode
```

**Critical guarantee:**
The savepoint captures both the RocksDB state AND the Kafka consumer offsets atomically
(this is native Flink behavior). App2 starts with identical state AND picks up from the
exact Kafka position where App1's savepoint was taken — no messages skipped, no duplicates
beyond what DynamoDB idempotency handles.

---

### 5.3 OffsetTracker (inside FASC Coordinator)

Continuously monitors the Kafka offset gap between App1 and App2.

**Data sources:**
- MSK CloudWatch metric: `SumOffsetLag` per consumer group
- Flink REST API: `/jobs/{jobId}/vertices/{vertexId}/subtasks` → per-subtask metrics
  including `KafkaSourceReader.currentOffset`

**Lag calculation:**
```
For each of 24 partitions:
    lag(partition) = app1_committed_offset(partition) - app2_committed_offset(partition)

total_lag = max(lag across all partitions)   ← worst partition determines readiness
```

**Readiness criteria for handoff:**
```
total_lag == 0
AND stable for 10 consecutive seconds
AND no App2 checkpoint failures in last 60s
AND App2 heartbeat healthy
```

---

### 5.4 HandoffOrchestrator (inside FASC Coordinator)

Executes the atomic handoff sequence. This is the most sensitive component.

**Handoff state machine:**

```
IDLE
  │
  │  upgrade trigger received
  v
SAVEPOINT_IN_PROGRESS
  │
  │  savepoint completed, App2 started
  v
WAITING_FOR_APP2_CATCHUP
  │
  │  lag = 0, stable 10s
  v
HANDOFF_INITIATED
  │
  │  Step A: pick cutover offset C = current App1 high watermark
  │  Step B: send PREPARE_HANDOFF(cutoverOffset=C) to both apps via control topic
  v
WAITING_FOR_APP1_DRAIN
  │
  │  App1 signals DRAINED_AT(C)  ← finished processing all messages up to C
  v
PROMOTING_APP2
  │
  │  Step C: DynamoDB conditional write  leader: app1→app2  (atomic)
  │  Step D: send PROMOTE to App2 via control topic
  │  Step E: send DEMOTE  to App1 via control topic
  v
HANDOFF_COMPLETE
  │
  │  both apps confirm mode change
  v
IDLE  (App2 now ACTIVE, App1 now SHADOW or stopped for upgrade)
```

**PREPARE_HANDOFF signal handling in App1:**
```
On receiving PREPARE_HANDOFF(cutoverOffset=C):
    1. Complete processing of all in-flight messages
    2. After processing message at offset C on all 24 partitions:
       → ShadowSink.demote()   ← stop writing to DynamoDB
       → publish DRAINED_AT(C) to fasc-control-topic
    3. App1 enters SHADOW mode — still consuming Kafka but not writing
```

**PROMOTE signal handling in App2:**
```
On receiving PROMOTE:
    1. Verify own committed offset >= C on all partitions
    2. ShadowSink.promote()   ← start writing to DynamoDB
    3. Publish PROMOTED_CONFIRMED to fasc-control-topic
```

---

### 5.5 LeaderElection (inside FASC Coordinator)

Prevents split-brain when 2 coordinator replicas are running.

**DynamoDB schema:**
```
Table: fasc-coordinator-lock
  PK:            "coordinator-lock"
  coordinatorId: "coordinator-pod-abc123"    ← Kubernetes pod name
  acquiredAt:    1711700000000
  ttlEpoch:      1711700030000               ← 30s TTL, must renew every 10s
  version:       42                          ← optimistic lock

Table: fasc-leader
  PK:            "active-flink-app"
  appId:         "app1"                      ← which app is currently ACTIVE sink
  jobId:         "flink-job-xyz"
  version:       7
```

**Lock acquisition:**
```
PutItem with ConditionExpression:
    attribute_not_exists(PK) OR ttlEpoch < :now

If ConditionCheckFailedException → another coordinator is active → standby mode
If success → this coordinator is active → start orchestration
Renew every 10s via UpdateItem with version condition
```

---

## 6. Kafka Topics

| Topic | Partitions | Retention | Config | Purpose |
|---|---|---|---|---|
| `business-topic` | 24 | 7 days | Normal | Source data — consumed by both apps |
| `fasc-control-topic` | 1 | 24 hours | Normal | Coordinator ↔ App signals |

**fasc-control-topic message schema (JSON):**
```json
{
  "signalType":      "PREPARE_HANDOFF | PROMOTE | DEMOTE | DRAINED_AT | PROMOTED_CONFIRMED | HEARTBEAT",
  "targetAppId":     "app1 | app2 | broadcast",
  "cutoverOffset":   { "0": 10500, "1": 10499, ... },
  "coordinatorId":   "coordinator-pod-abc123",
  "timestamp":       1711700000000
}
```

**Why only 2 topics (not 3 like FSSP)?**
FASC does not need a `state-delta-topic` because state synchronization is handled
by the savepoint bootstrap. Ongoing sync is guaranteed by both apps consuming the
same Kafka messages from the same offset. This eliminates the most complex part
of the FSSP design.

---

## 7. Three-Phase Production Protocol

### Phase 1: Bootstrap (one-time, on App2 first start)

```
Coordinator                App1 (Active)               App2 (not yet started)
     │                          │
     │──── trigger savepoint ──>│
     │                          │── write RocksDB state + Kafka offsets to S3
     │<── savepoint complete ───│
     │    s3://fasc/sp-001,
     │    offsets: {0:5000, 1:4998, ...24 partitions}
     │
     │── record savepoint metadata in DynamoDB
     │
     │── submit App2 Flink job ──────────────────────────────────────>│
     │    --fromSavepoint s3://fasc/sp-001                            │
     │    ShadowSink mode = SHADOW                                     │
     │                                                                 │── restore RocksDB from savepoint
     │                                                                 │── resume Kafka from offsets in savepoint
     │                                                                 │── ShadowSink SUPPRESSED
```

### Phase 2: Warm Standby (steady state — can last days/weeks)

```
MSK business-topic
        │
        ├── consumer-group-app1 ──> App1 (ACTIVE) ──> DynamoDB ✓
        │
        └── consumer-group-app2 ──> App2 (SHADOW) ──> DynamoDB ✗ (suppressed)
                                         │
                                    same state as App1
                                    (same messages from same offset)

Coordinator: monitors App2 lag, ready to orchestrate handoff on demand
```

### Phase 3: Handoff (triggered by operator for upgrade)

```
Timeline ────────────────────────────────────────────────────────────>

T+0s   Operator triggers upgrade via FASC API or Kubernetes annotation

T+1s   Coordinator confirms App2 lag = 0 (caught up)
       Picks cutoverOffset C = current App1 high watermark

T+2s   Coordinator publishes PREPARE_HANDOFF(C) to fasc-control-topic

T+3s   App1: processes messages up to offset C on all 24 partitions
             ShadowSink.demote() — stops DynamoDB writes
             publishes DRAINED_AT(C)

T+4s   Coordinator: receives DRAINED_AT(C) from App1
                    DynamoDB conditional write: leader app1 → app2
                    publishes PROMOTE to fasc-control-topic

T+4.2s App2: receives PROMOTE
             verifies own offset >= C on all partitions
             ShadowSink.promote() — starts DynamoDB writes
             publishes PROMOTED_CONFIRMED

T+4.5s Coordinator: receives PROMOTED_CONFIRMED
                    handoff complete
                    App1 can now be upgraded (rolling update on EKS)

Total blackout: ~0ms
DynamoDB write gap: ~200-500ms max (App1 stops at T+3, App2 starts at T+4.2)
Since DynamoDB is idempotent, any messages processed by both apps during
the overlap are harmless.
```

---

## 8. Upgrade Flow — Step by Step

This is the complete operator runbook for upgrading App1 to a new version.

```
Step 1: Deploy new App1 version to EKS (as a pending rollout, not yet active)

Step 2: Trigger FASC handoff:
        kubectl annotate deployment app1 fasc.io/trigger-handoff="true"
        OR
        curl -X POST http://fasc-coordinator/api/handoff

Step 3: FASC Coordinator executes Phase 3 (Handoff) automatically
        → App2 becomes ACTIVE (~4.5 seconds total)

Step 4: App1 is now in SHADOW mode
        Perform EKS rolling update on App1 pod:
        kubectl rollout restart deployment/app1

Step 5: App1 restarts with new code, ShadowSink starts in SHADOW mode
        App1 resumes consuming business-topic from its last checkpoint
        App1 catches up to App2's current offset

Step 6: (Optional) Trigger reverse handoff App2 → App1 if desired
        OR leave App2 as permanent ACTIVE until next upgrade cycle

Step 7: Done. No downtime. No message loss.
```

---

## 9. Edge Cases and Failure Handling

### 9.1 App2 Not Ready When Handoff Is Triggered
```
Coordinator checks: App2 lag > 0
Action: Reject handoff request, return HTTP 409 with estimated wait time
Operator: Wait for App2 to catch up, retry
```

### 9.2 App1 Crashes Before Sending DRAINED_AT
```
Detection: Coordinator heartbeat timeout on App1 (30s)
Action:
  1. Coordinator checks App2 offset >= last known App1 offset
  2. If yes → force-promote App2 (App1 is dead, no split-brain risk)
  3. If no  → wait for App2 to catch up, then force-promote
  4. App1 crash means no new DynamoDB writes from App1 → safe to promote App2
```

### 9.3 Coordinator Crashes During Handoff
```
State is persisted in DynamoDB at each step of the state machine.
On coordinator restart:
  1. Read current state from DynamoDB fasc-leader table
  2. If state = HANDOFF_INITIATED → resume from that step
  3. If state = PROMOTING_APP2   → check if App2 is already ACTIVE
  4. DynamoDB conditional write ensures idempotency — safe to retry
```

### 9.4 Split-Brain (Both Apps Think They Are ACTIVE)
```
Prevention:
  - ShadowSink only self-promotes on explicit PROMOTE signal from coordinator
  - Coordinator only sends PROMOTE after successful DynamoDB conditional write
  - DynamoDB conditional write is atomic — only one coordinator can win

Detection (defense in depth):
  - Each DynamoDB write from ShadowSink includes:
    ConditionExpression: fasc_leader = :myAppId
  - If App1 writes after being demoted, condition fails → write rejected
  - CloudWatch alarm on fasc_leader_check_failed metric
```

### 9.5 Business Logic Is Non-Deterministic (Silent State Divergence)
```
Risk: Wall-clock timestamps, external API calls, or random values in state
      cause App1 and App2 states to diverge silently.

Detection:
  - FASC includes an optional StateVerifier component
  - Before handoff, both apps process a synthetic "probe" message
  - Both publish their resulting state hash to fasc-control-topic
  - Coordinator compares hashes — if mismatch, aborts handoff and alerts

Prevention (design contract for application developers):
  - Must use Flink event time (record.getEventTime()), not System.currentTimeMillis()
  - No external API calls inside processElement() without deterministic caching
  - No random number generation in state transitions
  - Document this as a FASC usage constraint in the library README
```

### 9.6 Kafka Retention Expires for Old Offsets
```
Scenario: App2 was stopped for a week. On restart, its saved Kafka offset
          is before the retention window — Kafka has deleted those messages.

Action:
  1. FASC detects OffsetOutOfRangeException in App2
  2. Automatically triggers a fresh savepoint of App1
  3. Restarts App2 from the new savepoint
  4. No manual intervention needed
```

### 9.7 State Schema Change in New App Version
```
App1 v1 state schema != App2 v2 state schema

FASC provides a StateMigrator interface:

  interface StateMigrator<OLD, NEW> {
      NEW migrate(OLD oldState, int fromVersion, int toVersion);
  }

App developers register their migrator:
  fascConfig.withStateMigrator(new MyStateMigrator())

FASC applies migration during savepoint restore in App2.
This enables rolling upgrades with state schema evolution —
a key differentiator for production usability.
```

---

## 10. AWS Infrastructure

```
┌────────────────────────────────────────────────────────────────────────────┐
│  VPC                                                                       │
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │  EKS Cluster                                                         │ │
│  │                                                                      │ │
│  │  ┌─────────────────────┐    ┌─────────────────────┐                 │ │
│  │  │  App1 Deployment    │    │  App2 Deployment    │                 │ │
│  │  │  (Flink on EKS)     │    │  (Flink on EKS)     │                 │ │
│  │  │  JobManager  1 pod  │    │  JobManager  1 pod  │                 │ │
│  │  │  TaskManager N pods │    │  TaskManager N pods │                 │ │
│  │  └─────────────────────┘    └─────────────────────┘                 │ │
│  │                                                                      │ │
│  │  ┌────────────────────────────────────────────────────────────────┐ │ │
│  │  │  FASC Coordinator Deployment  (2 replicas)                     │ │ │
│  │  │  - replica 1: ACTIVE (holds DynamoDB lock)                     │ │ │
│  │  │  - replica 2: STANDBY (waiting to acquire lock)                │ │ │
│  │  └────────────────────────────────────────────────────────────────┘ │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
│  ┌──────────────────────────────┐   ┌──────────────────────────────────┐ │
│  │  MSK Cluster                 │   │  DynamoDB                        │ │
│  │  business-topic (24p)        │   │  fasc-leader          (pk only)  │ │
│  │  fasc-control-topic (1p)     │   │  fasc-coordinator-lock(pk only)  │ │
│  └──────────────────────────────┘   │  business-data-table             │ │
│                                     └──────────────────────────────────┘ │
│  ┌──────────────────────────────┐                                         │
│  │  S3                          │   ┌──────────────────────────────────┐ │
│  │  fasc/savepoints/            │   │  CloudWatch                      │ │
│  │  fasc/checkpoints/app1/      │   │  - fasc.sink.mode                │ │
│  │  fasc/checkpoints/app2/      │   │  - fasc.app2.consumer.lag        │ │
│  └──────────────────────────────┘   │  - fasc.handoff.state            │ │
│                                     │  - fasc.split_brain_detected     │ │
│                                     └──────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────────┘
```

**AWS Services:**
| Service | Role | Notes |
|---|---|---|
| EKS | Runs App1, App2, FASC Coordinator | Full control over Flink config |
| MSK | Business topic + control topic | 24 partitions, standard retention |
| DynamoDB | Business sink + FASC leader election + coordinator lock | On-demand capacity |
| S3 | Flink checkpoints + FASC savepoints | Versioned bucket |
| CloudWatch | Metrics and alerting | Custom FASC metrics namespace |
| IAM | Pod identity (IRSA) for EKS pods | Least-privilege per component |
| ECR | Container images for App1, App2, Coordinator | Immutable tags |

**EKS Flink Deployment Notes:**
- Use the official `apache/flink` Docker image as base
- Flink HA via `KubernetesHaServicesFactory` (native Kubernetes HA)
- Checkpoints written to S3 every 30s (`execution.checkpointing.interval: 30s`)
- `execution.checkpointing.mode: EXACTLY_ONCE`
- RocksDB state backend with incremental checkpoints enabled

---

## 11. Metrics and Observability

### Application Metrics (from ShadowSink + FASC library)

| Metric | Source | Alert Threshold | Meaning |
|---|---|---|---|
| `fasc.sink.mode` | App1, App2 | Unexpected value | 0=SHADOW, 1=ACTIVE |
| `fasc.sink.shadow_drop_count` | App2 | Sudden drop to 0 | Records suppressed in SHADOW mode |
| `fasc.app2.consumer_lag_max` | Coordinator | > 5000 for 60s | App2 falling behind App1 |
| `fasc.handoff.state` | Coordinator | Stuck > 30s | Handoff state machine stalled |
| `fasc.split_brain_detected` | App1/App2 | > 0 | DynamoDB leader check failed |
| `fasc.savepoint.duration_ms` | Coordinator | > 60000 | Savepoint taking too long |
| `fasc.state_hash_mismatch` | Coordinator | > 0 | State divergence detected pre-handoff |

### Dashboard — Key Signals

```
Normal steady state:
  App1: fasc.sink.mode = 1 (ACTIVE)   fasc.consumer.lag = 0
  App2: fasc.sink.mode = 0 (SHADOW)   fasc.consumer.lag ≈ 0 (healthy catchup)

During handoff (should resolve within 5s):
  fasc.handoff.state: IDLE → HANDOFF_INITIATED → PROMOTING_APP2 → IDLE

Post-handoff:
  App1: fasc.sink.mode = 0 (SHADOW)
  App2: fasc.sink.mode = 1 (ACTIVE)
```

---

## 12. Open-Source Design

**Project:** `flink-fasc` — Flink Active-Standby Coordinator

**What makes this novel and worth open-sourcing:**
- No existing open-source project provides automated warm-standby for Flink
- ShadowSink is a universal drop-in for any Flink sink
- Offset-aware handoff protocol is the first of its kind
- StateMigrator enables rolling upgrades with state schema evolution
- Deployment-agnostic: works on EKS, ECS, AWS Managed Flink, standalone

### Repository Structure

```
flink-fasc/
│
├── fasc-core/                        # Pure Flink library (no AWS dependency)
│   ├── ShadowSink.java               # Drop-in sink wrapper
│   ├── FASCConfiguration.java        # Builder-pattern config
│   ├── StateMigrator.java            # Interface for schema evolution
│   ├── OffsetTracker.java            # Kafka offset tracking utilities
│   └── StateVerifier.java            # Pre-handoff state hash verification
│
├── fasc-coordinator/                 # Coordinator microservice (Spring Boot)
│   ├── SavepointManager.java         # Savepoint trigger + bootstrap automation
│   ├── HandoffOrchestrator.java      # Handoff state machine
│   ├── LeaderElection.java           # Abstract leader election interface
│   ├── OffsetMonitor.java            # Consumer lag monitoring
│   └── FascApi.java                  # REST API (trigger handoff, status)
│
├── fasc-aws/                         # AWS-specific implementations
│   ├── DynamoDbLeaderElection.java   # LeaderElection via DynamoDB
│   ├── CloudWatchMetricsReporter.java
│   └── MskOffsetMonitor.java         # MSK CloudWatch lag metrics
│
├── fasc-k8s/                         # Kubernetes integration
│   ├── helm/                         # Helm chart for coordinator
│   ├── FlinkJobController.java       # K8s operator for Flink job lifecycle
│   └── HandoffAnnotationWatcher.java # Watch for fasc.io/trigger-handoff annotation
│
├── fasc-examples/
│   ├── simple-stateful-job/          # Word count with FASC
│   └── aws-msk-dynamodb/             # Full AWS reference implementation
│
└── fasc-docs/
    ├── PROTOCOL.md                   # Formal protocol specification
    ├── USAGE.md                      # Developer guide
    └── ARCHITECTURE.md               # System design rationale
```

### Pluggable Interfaces

| Interface | Default Implementation | Alternatives |
|---|---|---|
| `LeaderElection` | `DynamoDbLeaderElection` | ZooKeeper, etcd, Redis |
| `SavepointStorage` | `S3SavepointStorage` | HDFS, local (dev) |
| `ControlTransport` | `KafkaControlTransport` | gRPC, HTTP |
| `MetricsReporter` | `CloudWatchMetricsReporter` | Prometheus, Datadog |
| `StateMigrator` | No-op (user implements) | User-defined per version |

### Developer Integration (5 lines of code)

```java
// Existing Flink job — before FASC
stream
    .keyBy(Event::getKey)
    .process(new MyBusinessFunction())
    .addSink(new DynamoDbSink(config));

// After FASC — only line 4 changes
stream
    .keyBy(Event::getKey)
    .process(new MyBusinessFunction())
    .addSink(ShadowSink.wrap(new DynamoDbSink(config), FASCConfig.fromEnv()));
```

---

## 13. Next Steps

### Design
- [ ] Finalize `ShadowSink` Java API — method signatures, config options
- [ ] Finalize `fasc-control-topic` message schema (Avro vs JSON vs Protobuf)
- [ ] Design `StateVerifier` — state hash algorithm, sampling strategy
- [ ] Define `StateMigrator` interface and version negotiation protocol
- [ ] Write formal PROTOCOL.md specification

### Implementation
- [ ] Implement `ShadowSink` core with mode switching and heartbeat monitoring
- [ ] Implement `SavepointManager` using Flink REST API
- [ ] Implement `HandoffOrchestrator` state machine
- [ ] Implement `DynamoDbLeaderElection`
- [ ] Build FASC Coordinator Spring Boot service
- [ ] Build Helm chart for EKS deployment

### Validation
- [ ] Prototype with a simple Flink stateful job on local Kubernetes (kind/minikube)
- [ ] Benchmark: handoff latency under 24-partition MSK load
- [ ] Chaos testing: kill App1 mid-handoff, kill coordinator mid-handoff
- [ ] Verify state hash match between App1 and App2 before each handoff
- [ ] Load test: confirm < 2s end-to-end latency is maintained during handoff

### Open-Source
- [ ] Create GitHub repository `flink-fasc`
- [ ] Write CONTRIBUTING.md and CODE_OF_CONDUCT.md
- [ ] Submit design proposal to Apache Flink community (FLIP)
- [ ] Write introductory blog post with the problem statement and solution
