# FASC Developer Guide — From Zero to Running

This guide walks you through the entire FASC codebase step by step.
If you can read Java and have heard of Kafka, you can follow this.

---

## Table of Contents

1. [What Problem Does FASC Solve?](#1-what-problem-does-fasc-solve)
2. [Project Structure — Where Everything Lives](#2-project-structure--where-everything-lives)
3. [How to Build and Run Tests](#3-how-to-build-and-run-tests)
4. [Module Deep Dive — Class by Class](#4-module-deep-dive--class-by-class)
5. [How the Handoff Actually Works](#5-how-the-handoff-actually-works)
6. [Adding FASC to Your Own Flink Job](#6-adding-fasc-to-your-own-flink-job)
7. [Running Locally with Docker](#7-running-locally-with-docker)
8. [Common Mistakes and How to Avoid Them](#8-common-mistakes-and-how-to-avoid-them)

---

## 1. What Problem Does FASC Solve?

Imagine you have a Flink application that has been running for 6 months.
It has accumulated business state (customer orders, running totals, etc.)
in its internal RocksDB state backend. Now you need to deploy a new version.

**The naive approach fails:**

```
App1 (running 6 months) → has 6 months of business state in RocksDB
App2 (just started)     → has ZERO state

You switch traffic to App2 → App2 makes WRONG business decisions
because it doesn't have the 6 months of accumulated context.

Kafka retention is only 7 days, so App2 can't rebuild 6 months of state
by replaying messages.
```

**FASC solves this with 3 phases:**

```
Phase 1: Take a savepoint of App1 → gives you a snapshot of ALL RocksDB state
         Start App2 FROM that savepoint → App2 now has App1's EXACT state

Phase 2: Both apps consume the same Kafka messages
         App1 writes to DynamoDB (ACTIVE mode)
         App2 processes but doesn't write (SHADOW mode)

Phase 3: When App2 has caught up to App1's exact Kafka offset:
         → Tell App1 to stop writing
         → Tell App2 to start writing
         → This switch takes ~500ms with zero data loss
```

---

## 2. Project Structure — Where Everything Lives

```
flink-fasc/
│
├── fasc-core/                          ← THE FLINK LIBRARY (add to your job)
│   └── src/main/java/com/flinkfasc/core/
│       ├── ShadowSink.java             ← Wraps your sink, adds ACTIVE/SHADOW mode
│       ├── FASCConfiguration.java      ← Config (Kafka brokers, app ID, etc.)
│       ├── StateMigrator.java          ← Interface for state schema evolution
│       ├── control/
│       │   ├── ControlSignal.java      ← JSON messages on the control topic
│       │   ├── ControlSignalSerde.java ← Serialize/deserialize signals
│       │   ├── ControlTopicConsumer.java← Reads signals from Kafka
│       │   ├── ControlTopicProducer.java← Writes signals to Kafka
│       │   ├── SignalHandler.java      ← Callback interface
│       │   └── SignalType.java         ← PROMOTE, DEMOTE, HEARTBEAT, etc.
│       ├── sink/
│       │   └── SinkMode.java           ← Enum: ACTIVE or SHADOW
│       └── verifier/
│           ├── StateVerifier.java      ← SHA-256 hash of state for comparison
│           └── StateHashSignal.java    ← Carries the hash in a control signal
│
├── fasc-coordinator/                   ← THE COORDINATOR SERVICE (deploy on EKS)
│   └── src/main/java/com/flinkfasc/coordinator/
│       ├── FascCoordinatorApplication.java  ← Spring Boot main class
│       ├── api/
│       │   ├── FascApiController.java       ← REST: /fasc/handoff, /fasc/status
│       │   ├── HandoffRequest.java          ← POST body DTO
│       │   └── StatusResponse.java          ← GET response DTO
│       ├── config/
│       │   ├── FascCoordinatorProperties.java ← All config (YAML + env vars)
│       │   └── CoordinatorKafkaConfiguration.java
│       ├── control/
│       │   ├── CoordinatorControlListener.java ← Listens to control signals
│       │   └── CoordinatorLockManager.java     ← DynamoDB distributed lock
│       ├── election/
│       │   ├── LeaderElection.java             ← Interface
│       │   └── DynamoDbLeaderElection.java     ← Implementation
│       ├── handoff/
│       │   ├── HandoffOrchestrator.java        ← THE STATE MACHINE (most important!)
│       │   └── HandoffState.java               ← 8-state enum
│       ├── monitor/
│       │   └── OffsetMonitor.java              ← Tracks consumer lag
│       └── savepoint/
│           ├── SavepointManager.java           ← Flink REST API integration
│           ├── SavepointMetadata.java          ← Savepoint info DTO
│           ├── SavepointStatus.java            ← IN_PROGRESS / COMPLETED / FAILED
│           ├── SavepointStore.java             ← Interface
│           └── DynamoDbSavepointStore.java     ← Persists metadata
│
├── fasc-aws/                           ← AWS-SPECIFIC IMPLEMENTATIONS
│   └── src/main/java/com/flinkfasc/aws/
│       ├── config/
│       │   ├── AwsConfig.java                  ← Spring bean factory
│       │   └── FascAwsProperties.java          ← Properties interface
│       ├── election/
│       │   └── DynamoDbLeaderElection.java     ← Lock via DynamoDB CAS
│       ├── metrics/
│       │   └── CloudWatchMetricsReporter.java  ← Publishes to CloudWatch
│       └── savepoint/
│           └── S3SavepointStorage.java         ← S3 path + metadata
│
├── fasc-examples/                      ← REFERENCE FLINK JOB
│   └── src/main/java/com/flinkfasc/examples/
│       ├── BusinessFlinkJob.java               ← Main entry point
│       ├── BusinessEvent.java                  ← Domain event
│       ├── BusinessEventDeserializer.java      ← JSON → BusinessEvent
│       ├── BusinessProcessFunction.java        ← Stateful processing
│       ├── CustomerState.java                  ← State stored in RocksDB
│       └── DynamoDbSink.java                   ← Writes to DynamoDB
│
├── fasc-integration-tests/             ← TESTCONTAINERS TESTS
│   └── src/test/java/com/flinkfasc/it/
│       ├── e2e/FullHandoffE2EIT.java           ← End-to-end handoff test
│       ├── core/
│       │   ├── ControlTopicRoundTripIT.java    ← Signal send/receive
│       │   └── ShadowSinkSignalIT.java         ← Mode switching via Kafka
│       ├── aws/
│       │   ├── DynamoDbLeaderElectionIT.java   ← Real DynamoDB (LocalStack)
│       │   └── CloudWatchMetricsIT.java        ← Real CloudWatch (LocalStack)
│       ├── coordinator/
│       │   └── HandoffOrchestratorIT.java      ← State machine integration
│       └── infrastructure/
│           ├── ContainerSetup.java             ← Testcontainers bootstrap
│           └── WireMockFlinkApi.java           ← Mock Flink REST API
│
├── fasc-k8s/                           ← KUBERNETES DEPLOYMENT
│   └── helm/fasc-coordinator/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/                          ← deployment, service, hpa, etc.
│
├── terraform/                          ← AWS INFRASTRUCTURE
│   ├── environments/eu-west-2/                 ← Main Terraform config
│   └── modules/
│       ├── eks/                                ← EKS cluster
│       ├── msk/                                ← MSK Kafka cluster
│       ├── dynamodb/                           ← DynamoDB tables
│       ├── s3/                                 ← S3 bucket
│       └── iam/                                ← IAM roles (IRSA)
│
└── docs/
    ├── ARCHITECTURE.md                         ← Diagrams (this companion doc)
    └── DEVELOPER_GUIDE.md                      ← You are here
```

---

## 3. How to Build and Run Tests

### Prerequisites

- Java 11+ (JDK, not just JRE)
- Maven 3.8+
- Docker (needed for integration tests)

### Build

```bash
# Clone the repo
git clone https://github.com/ghoshp83/flink-fasc.git
cd flink-fasc

# Compile everything (fast, ~3 seconds)
mvn clean compile -DskipTests

# Run unit tests only (~5 seconds)
mvn test -DskipITs

# Run ALL tests including integration tests (~2 minutes, needs Docker)
mvn test -pl fasc-integration-tests -am -Pintegration-tests
```

### What the tests cover

| Test Class | What It Tests | Infrastructure |
|---|---|---|
| `ControlSignalSerdeTest` | JSON serialization of all signal types | None (pure unit) |
| `FASCConfigurationTest` | Config builder, validation, env vars | None (pure unit) |
| `ShadowSinkTest` | Mode switching, signal handling, heartbeat | Mockito mocks |
| `DynamoDbLeaderElectionTest` | Lock acquire/renew/release | Mockito mocks |
| `HandoffOrchestratorTest` | Full state machine transitions | Mockito mocks |
| `ControlTopicRoundTripIT` | Send signal → receive signal via Kafka | Testcontainers Kafka |
| `ShadowSinkSignalIT` | ShadowSink mode change via real Kafka | Testcontainers Kafka |
| `DynamoDbLeaderElectionIT` | Leader election with real DynamoDB | LocalStack |
| `CloudWatchMetricsIT` | Metrics publishing to real CloudWatch | LocalStack |
| `HandoffOrchestratorIT` | State machine with real Kafka + WireMock Flink API | Kafka + WireMock |
| `FullHandoffE2EIT` | Complete handoff: savepoint → catchup → promote | All containers |

---

## 4. Module Deep Dive — Class by Class

### 4.1 The Most Important Class: `ShadowSink`

This is the one class that lives inside your Flink job. Everything else runs externally.

```
                        Your Flink Pipeline
┌─────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Kafka   │────>│ BusinessProcess  │────>│   ShadowSink    │
│  Source  │     │   Function       │     │                 │
└─────────┘     │ (your code,      │     │  ┌───────────┐  │
                │  RocksDB state)  │     │  │ YourSink  │  │
                └──────────────────┘     │  │(DynamoDB) │  │
                                         │  └───────────┘  │
                                         │                 │
                                         │  mode: ACTIVE   │──> DynamoDB writes
                                         │  mode: SHADOW   │──> writes SUPPRESSED
                                         │                 │
                                         │  ┌───────────┐  │
                                         │  │ Control   │  │<── fasc-control-topic
                                         │  │ Consumer  │  │    (PROMOTE/DEMOTE)
                                         │  └───────────┘  │
                                         └─────────────────┘
```

**Key methods:**

```java
// How you use it (1 line change to your existing code):
stream.addSink(ShadowSink.wrap(yourExistingSink, fascConfig));

// What happens internally:
public void invoke(T record, Context context) {
    if (currentMode == SinkMode.ACTIVE) {
        innerSink.invoke(record, context);  // Real write to DynamoDB
    } else {
        shadowDropCount++;                   // Counted, not written
    }
}

// Mode changes are atomic and instant:
// When PROMOTE signal arrives → mode flips to ACTIVE
// When DEMOTE signal arrives  → mode flips to SHADOW
```

### 4.2 The Brain: `HandoffOrchestrator`

This runs in the coordinator service and manages the handoff state machine.

```
       triggerHandoff()                   Handoff State Machine
            │
            v
    ┌───────────────┐     savepoint      ┌─────────────────────────┐
    │     IDLE      │─────complete──────>│  WAITING_FOR_APP2_CATCHUP│
    └───────────────┘                    └───────────┬─────────────┘
                                                     │ lag=0 stable
                                                     v
                                         ┌─────────────────────────┐
                                         │    HANDOFF_INITIATED    │
                                         └───────────┬─────────────┘
                                                     │ PREPARE_HANDOFF sent
                                                     v
                                         ┌─────────────────────────┐
                                         │  WAITING_FOR_APP1_DRAIN │
                                         └───────────┬─────────────┘
                                                     │ DRAINED_AT received
                                                     v
                                         ┌─────────────────────────┐
                                         │     PROMOTING_APP2      │
                                         └───────────┬─────────────┘
                                                     │ DynamoDB leader swap
                                                     │ PROMOTE sent
                                                     │ PROMOTED_CONFIRMED received
                                                     v
                                         ┌─────────────────────────┐
                                         │    HANDOFF_COMPLETE     │
                                         └─────────────────────────┘
```

### 4.3 Control Signals — The Language Apps Speak

All communication between the coordinator and the Flink apps happens via
a single Kafka topic (`fasc-control-topic`). Each message is a JSON `ControlSignal`:

```json
{
  "signalType": "PROMOTE",
  "targetAppId": "app2",
  "cutoverOffsets": {"0": 10500, "1": 10499},
  "coordinatorId": "coordinator-pod-abc123",
  "traceId": "handoff-2026-03-31-abc",
  "timestamp": 1711700000000
}
```

**Signal types and who sends them:**

```
Signal              Sender          Receiver        Meaning
─────────────────────────────────────────────────────────────────
HEARTBEAT           Coordinator  →  Both apps       "I'm alive"
PREPARE_HANDOFF     Coordinator  →  App1            "Stop writing at offset C"
DRAINED_AT          App1         →  Coordinator     "I've stopped at offset C"
PROMOTE             Coordinator  →  App2            "Start writing now"
DEMOTE              Coordinator  →  App1            "Stop writing now"
PROMOTED_CONFIRMED  App2         →  Coordinator     "I'm writing now"
HANDOFF_ABORT       Coordinator  →  Both apps       "Cancel handoff"
```

### 4.4 Split-Brain Prevention

The most critical safety property: **only one app writes to DynamoDB at any time**.

```
Protection Layer 1: DynamoDB Conditional Write (fasc-leader table)
──────────────────────────────────────────────────────────────────
Before sending PROMOTE, the coordinator does:

    UpdateItem(
        PK = "active-flink-app",
        SET activeAppId = "app2", version = N+1
        CONDITION: version = N              ← atomic compare-and-swap
    )

If two coordinators try this simultaneously, only ONE succeeds.
The other gets ConditionalCheckFailedException.

Protection Layer 2: ShadowSink Never Self-Promotes
──────────────────────────────────────────────────────────────────
If the coordinator dies, ShadowSink logs an error but stays in
its current mode. It NEVER promotes itself to ACTIVE.
This prevents: "coordinator died → both apps think they're active"

Protection Layer 3: DynamoDB Write Condition in Business Sink
──────────────────────────────────────────────────────────────────
Each DynamoDB write from the business sink includes:
    ConditionExpression: fasc_leader = :myAppId

If an app writes AFTER being demoted, the condition fails → write rejected.
```

---

## 5. How the Handoff Actually Works

Here's what happens second by second when you trigger a handoff:

```
Second 0: You call POST /fasc/handoff {"reason": "upgrading app1 to v2"}
          Coordinator sets state = SAVEPOINT_IN_PROGRESS

Second 1: Coordinator calls App1's Flink REST API:
          POST http://app1-jobmanager:8081/jobs/abc123/savepoints
          This triggers Flink to write ALL of App1's RocksDB state to S3.

Second 2-60: Flink writes the savepoint to S3.
             For a job with 10GB of state, this takes ~30-60 seconds.
             For a job with 100MB, it takes ~2 seconds.

Second 61: Savepoint complete! Coordinator records metadata in DynamoDB.
           Coordinator restarts App2 from this savepoint.
           App2 now has App1's EXACT state at the savepoint offset.
           State = WAITING_FOR_APP2_CATCHUP

Second 62+: App2 starts consuming from Kafka where the savepoint left off.
            Both apps process the same messages.
            Coordinator polls Kafka AdminClient every 5 seconds:
              lag = max(app1_offset - app2_offset) across 24 partitions

Second N: lag = 0 for 10 consecutive seconds → App2 is ready!
          State = HANDOFF_INITIATED

Second N+0.0: Coordinator sends PREPARE_HANDOFF to fasc-control-topic
Second N+0.1: App1 receives signal, drains in-flight records, stops writing
Second N+0.2: App1 sends DRAINED_AT back to control topic
Second N+0.3: Coordinator does atomic DynamoDB write: leader = app2
Second N+0.4: Coordinator sends PROMOTE to App2, DEMOTE to App1
Second N+0.5: App2 flips ShadowSink to ACTIVE, starts writing to DynamoDB

Total blackout: ~200-500ms (between App1 stops and App2 starts writing)
Messages lost: ZERO (both apps processed them, App2 just wasn't writing)
```

---

## 6. Adding FASC to Your Own Flink Job

### Step 1: Add Maven dependency

```xml
<dependency>
    <groupId>com.flinkfasc</groupId>
    <artifactId>fasc-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Step 2: Wrap your sink (literally 2 lines)

```java
// BEFORE FASC:
stream.addSink(new YourDynamoDbSink(config));

// AFTER FASC:
FASCConfiguration fascConfig = FASCConfiguration.fromEnv();
stream.addSink(ShadowSink.wrap(new YourDynamoDbSink(config), fascConfig));
```

### Step 3: Set environment variables

```bash
# For App1 (the active writer):
FASC_BOOTSTRAP_SERVERS=b-1.msk.example.com:9092
FASC_APP_ID=app1
FASC_CONTROL_TOPIC=fasc-control-topic
FASC_INITIAL_MODE=ACTIVE

# For App2 (the shadow/standby):
FASC_BOOTSTRAP_SERVERS=b-1.msk.example.com:9092
FASC_APP_ID=app2
FASC_CONTROL_TOPIC=fasc-control-topic
FASC_INITIAL_MODE=SHADOW
```

### Step 4: Follow the design contract

Your business logic MUST be deterministic for FASC to work:

```java
// GOOD — uses event time from the record
long timestamp = context.timestamp();  // Flink event time

// BAD — uses wall clock (App1 and App2 will compute different values!)
long timestamp = System.currentTimeMillis();

// GOOD — pure computation from input
int result = event.getAmount() * event.getQuantity();

// BAD — external call (App1 and App2 might get different responses!)
int result = externalService.getPrice(event.getProductId());

// GOOD — deterministic state transitions
state.setOrderCount(state.getOrderCount() + 1);

// BAD — random values in state
state.setSessionId(UUID.randomUUID().toString());
```

---

## 7. Running Locally with Docker

You can run the integration tests locally to see FASC in action:

```bash
# This starts real Kafka and LocalStack (DynamoDB) containers:
mvn test -pl fasc-integration-tests -am -Pintegration-tests

# Watch the logs — you'll see:
#   1. Kafka container starting
#   2. LocalStack (DynamoDB) container starting
#   3. Control signals being sent and received
#   4. ShadowSink mode transitions (SHADOW → ACTIVE)
#   5. Leader election via DynamoDB
#   6. Full handoff state machine execution
```

---

## 8. Common Mistakes and How to Avoid Them

### Mistake 1: Using `System.currentTimeMillis()` in business logic

```java
// This breaks FASC because App1 and App2 will compute different timestamps
state.setLastUpdated(System.currentTimeMillis()); // WRONG

// Use Flink's event time instead
state.setLastUpdated(context.timestamp());         // CORRECT
```

### Mistake 2: Both apps using the same consumer group

```bash
# WRONG — both apps compete for the same partitions
FASC_APP_ID=app1  CONSUMER_GROUP=my-group
FASC_APP_ID=app2  CONSUMER_GROUP=my-group  # Same group!

# CORRECT — each app has its own consumer group
FASC_APP_ID=app1  CONSUMER_GROUP=consumer-group-app1
FASC_APP_ID=app2  CONSUMER_GROUP=consumer-group-app2
```

### Mistake 3: Not using conditional writes in DynamoDB

```java
// WRONG — duplicate writes during handoff will silently overwrite
dynamoDbClient.putItem(request);

// CORRECT — conditional write ensures idempotency
PutItemRequest request = PutItemRequest.builder()
    .conditionExpression("attribute_not_exists(PK) OR lastUpdatedMs < :newTs")
    .build();
```

### Mistake 4: Forgetting to deploy 2 coordinator replicas

The coordinator itself is a single point of failure if you only run 1 replica.
Always deploy 2 replicas — one acquires the DynamoDB lock (active), the other
waits as a hot standby.

```yaml
# In values.yaml:
replicaCount: 2   # NEVER set this to 1 in production
```

### Mistake 5: Not setting up the control topic

The `fasc-control-topic` must exist before starting the apps:

```bash
kafka-topics.sh --create \
  --topic fasc-control-topic \
  --partitions 1 \
  --replication-factor 3 \
  --bootstrap-server $BOOTSTRAP_SERVERS
```

Why 1 partition? Because control signals must be consumed in order.
If you use multiple partitions, signals can arrive out of order and break the protocol.
