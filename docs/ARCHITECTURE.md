# FASC Architecture Diagrams

This document contains all architecture and data-flow diagrams for the
Flink Active-Standby Coordinator (FASC) project. All diagrams use Mermaid
and render natively on GitHub.

---

## 1. High-Level System Architecture

```mermaid
graph TB
    subgraph MSK["AWS MSK Cluster"]
        BT["business-topic<br/>(24 partitions, 7-day retention)"]
        CT["fasc-control-topic<br/>(1 partition, 24h retention)"]
    end

    subgraph App1["App1 — Active Flink Job"]
        A1S["KafkaSource<br/>consumer-group-app1"]
        A1P["BusinessProcessFunction<br/>(RocksDB State)"]
        A1SS["ShadowSink<br/>mode = ACTIVE"]
    end

    subgraph App2["App2 — Standby Flink Job"]
        A2S["KafkaSource<br/>consumer-group-app2"]
        A2P["BusinessProcessFunction<br/>(RocksDB State from Savepoint)"]
        A2SS["ShadowSink<br/>mode = SHADOW"]
    end

    subgraph Coordinator["FASC Coordinator (EKS, 2 replicas)"]
        HO["HandoffOrchestrator<br/>(State Machine)"]
        SM["SavepointManager<br/>(Flink REST API)"]
        OM["OffsetMonitor<br/>(Kafka AdminClient)"]
        LE["LeaderElection<br/>(DynamoDB CAS)"]
        CL["ControlListener<br/>(Kafka Consumer)"]
        LM["LockManager<br/>(DynamoDB TTL Lock)"]
        API["REST API<br/>/fasc/handoff<br/>/fasc/status"]
    end

    subgraph AWS["AWS Services"]
        DDB[("DynamoDB<br/>Business Data")]
        S3[("S3<br/>Savepoints")]
        DDBl[("DynamoDB<br/>fasc-leader")]
        DDBk[("DynamoDB<br/>fasc-coordinator-lock")]
        CW["CloudWatch<br/>Metrics"]
    end

    BT --> A1S --> A1P --> A1SS
    BT --> A2S --> A2P --> A2SS
    A1SS -->|"DynamoDB writes"| DDB
    A2SS -.->|"suppressed in SHADOW"| DDB
    CT <-->|"control signals"| CL
    CT <-->|"HEARTBEAT / DRAINED_AT"| A1SS
    CT <-->|"PROMOTE / DEMOTE"| A2SS
    SM -->|"REST: trigger savepoint"| App1
    SM -->|"REST: restore from savepoint"| App2
    SM -->|"metadata"| S3
    OM -->|"lag poll"| MSK
    LE -->|"conditional write"| DDBl
    LM -->|"TTL lock"| DDBk
    HO --> SM
    HO --> OM
    HO --> LE
    HO --> CL
    HO --> LM
    HO -->|"metrics"| CW
    API --> HO

    style App1 fill:#d4edda,stroke:#28a745
    style App2 fill:#fff3cd,stroke:#ffc107
    style Coordinator fill:#cce5ff,stroke:#004085
    style AWS fill:#f8d7da,stroke:#721c24
```

---

## 2. End-to-End Handoff Sequence

```mermaid
sequenceDiagram
    participant OPS as Ops / CI Pipeline
    participant API as FASC REST API
    participant COORD as HandoffOrchestrator
    participant APP1 as App1 (Active)
    participant APP2 as App2 (Shadow)
    participant KAFKA as MSK Kafka
    participant DDB as DynamoDB
    participant S3 as S3

    Note over APP1,KAFKA: Steady State — App1 ACTIVE, App2 SHADOW

    OPS->>API: POST /fasc/handoff<br/>{"reason": "upgrade app1"}
    API->>COORD: triggerHandoff()
    Note over COORD: State: IDLE → SAVEPOINT_IN_PROGRESS

    rect rgb(230, 245, 255)
        Note right of COORD: Phase 1: Savepoint Bootstrap
        COORD->>APP1: Flink REST: POST /jobs/{id}/savepoints
        APP1->>S3: Write RocksDB state + Kafka offsets
        S3-->>COORD: savepoint path (s3://bucket/fasc/sp-...)
        COORD->>APP2: Flink REST: restart from savepoint
        Note over APP2: Restores App1 state at offset X
    end

    Note over COORD: State: → WAITING_FOR_APP2_CATCHUP

    rect rgb(255, 248, 220)
        Note right of COORD: Phase 2: Offset Convergence
        loop Every 5s — OffsetMonitor
            COORD->>KAFKA: listConsumerGroupOffsets(app1, app2)
            KAFKA-->>COORD: per-partition lag
        end
        Note over COORD: Lag=0 stable for 10s → Ready
    end

    Note over COORD: State: → HANDOFF_INITIATED

    rect rgb(220, 255, 220)
        Note right of COORD: Phase 3: Atomic Handoff (~500ms)
        COORD->>KAFKA: PREPARE_HANDOFF{cutoverOffsets, traceId}
        KAFKA->>APP1: PREPARE_HANDOFF signal
        APP1->>APP1: Drain in-flight, checkpoint
        APP1->>KAFKA: DRAINED_AT{offsets, traceId}
        KAFKA->>COORD: DRAINED_AT received
        Note over COORD: State: → PROMOTING_APP2

        COORD->>DDB: UpdateItem fasc-leader<br/>WHERE version=N<br/>SET activeApp=app2, version=N+1
        Note over DDB: Atomic conditional write

        COORD->>KAFKA: PROMOTE{targetAppId=app2}
        COORD->>KAFKA: DEMOTE{targetAppId=app1}
        KAFKA->>APP2: PROMOTE signal
        KAFKA->>APP1: DEMOTE signal

        APP2->>APP2: ShadowSink → ACTIVE
        APP2->>DDB: write(record, version=N+1)
        APP2->>KAFKA: PROMOTED_CONFIRMED{traceId}
    end

    Note over COORD: State: → HANDOFF_COMPLETE
    COORD-->>API: {state: HANDOFF_COMPLETE}
    API-->>OPS: Handoff complete — upgrade App1 now
```

---

## 3. Handoff State Machine

```mermaid
stateDiagram-v2
    [*] --> IDLE

    IDLE --> SAVEPOINT_IN_PROGRESS : triggerHandoff()
    SAVEPOINT_IN_PROGRESS --> WAITING_FOR_APP2_CATCHUP : savepoint complete,<br/>App2 restored
    WAITING_FOR_APP2_CATCHUP --> HANDOFF_INITIATED : lag=0 stable for 10s
    HANDOFF_INITIATED --> WAITING_FOR_APP1_DRAIN : PREPARE_HANDOFF sent
    WAITING_FOR_APP1_DRAIN --> PROMOTING_APP2 : DRAINED_AT received
    PROMOTING_APP2 --> HANDOFF_COMPLETE : PROMOTED_CONFIRMED received
    HANDOFF_COMPLETE --> IDLE : auto-reset after 30s

    SAVEPOINT_IN_PROGRESS --> HANDOFF_FAILED : savepoint timeout (5min)
    WAITING_FOR_APP2_CATCHUP --> HANDOFF_FAILED : App2 not ready (timeout)
    WAITING_FOR_APP1_DRAIN --> HANDOFF_FAILED : drain timeout (60s)
    PROMOTING_APP2 --> HANDOFF_FAILED : promote timeout (30s)
    HANDOFF_FAILED --> IDLE : manual reset or auto-retry
```

---

## 4. Module Dependency Graph

```mermaid
graph LR
    subgraph "Your Flink Job"
        JOB["BusinessFlinkJob"]
    end

    subgraph "FASC Libraries"
        CORE["fasc-core<br/>(ShadowSink, ControlSignal,<br/>FASCConfiguration)"]
        AWS["fasc-aws<br/>(DynamoDB, CloudWatch,<br/>S3 Storage)"]
        COORD["fasc-coordinator<br/>(Spring Boot Service)"]
    end

    subgraph "Infrastructure"
        K8S["fasc-k8s<br/>(Helm Chart)"]
        TF["terraform/<br/>(AWS Infra)"]
    end

    JOB -->|"depends on"| CORE
    COORD -->|"depends on"| CORE
    COORD -->|"depends on"| AWS
    K8S -->|"deploys"| COORD
    TF -->|"provisions"| K8S

    style CORE fill:#d4edda,stroke:#28a745
    style AWS fill:#fff3cd,stroke:#ffc107
    style COORD fill:#cce5ff,stroke:#004085
    style JOB fill:#e2e3e5,stroke:#383d41
```

---

## 5. ShadowSink Internal Flow

```mermaid
flowchart TD
    A["invoke(record, context)"] --> B{mode == ACTIVE?}
    B -->|Yes| C["inner.invoke(record, context)<br/>→ DynamoDB write"]
    B -->|No| D["shadowDropCounter.inc()<br/>→ record suppressed"]

    E["ControlTopicConsumer<br/>polls fasc-control-topic"] --> F{Signal Type?}
    F -->|PROMOTE| G["mode = ACTIVE<br/>(AtomicReference swap)"]
    F -->|DEMOTE| H["mode = SHADOW<br/>(AtomicReference swap)"]
    F -->|PREPARE_HANDOFF| I["Set cutoverOffset<br/>Start drain tracking"]
    F -->|HEARTBEAT| J["Update lastCoordinatorContact"]

    K["Heartbeat Watchdog<br/>(every 10s)"] --> L{Heartbeat stale > 30s?}
    L -->|Yes| M["LOG.error — alert<br/>DO NOT self-promote"]
    L -->|No| N["Healthy — continue"]

    style A fill:#e2e3e5
    style C fill:#d4edda
    style D fill:#fff3cd
    style G fill:#d4edda
    style H fill:#fff3cd
    style M fill:#f8d7da
```

---

## 6. DynamoDB Leader Election — Split-Brain Prevention

```mermaid
sequenceDiagram
    participant C1 as Coordinator Replica 1
    participant DDB as DynamoDB (fasc-leader)
    participant C2 as Coordinator Replica 2

    Note over C1,C2: Only one coordinator can hold the lock

    C1->>DDB: PutItem(PK="COORDINATOR_LOCK",<br/>coordinatorId="pod-1",<br/>ttlEpoch=now+30s)<br/>Condition: NOT EXISTS or ttl expired
    DDB-->>C1: Success → pod-1 is ACTIVE

    C2->>DDB: PutItem(PK="COORDINATOR_LOCK",<br/>coordinatorId="pod-2")<br/>Condition: NOT EXISTS or ttl expired
    DDB-->>C2: ConditionalCheckFailed → STANDBY

    loop Every 5s
        C1->>DDB: UpdateItem(version=N+1,<br/>ttlEpoch=now+30s)<br/>Condition: version=N
        DDB-->>C1: Success → lock renewed
    end

    Note over C1: pod-1 crashes!

    Note over DDB: TTL expires after 30s

    C2->>DDB: PutItem(PK="COORDINATOR_LOCK",<br/>coordinatorId="pod-2")<br/>Condition: ttl expired
    DDB-->>C2: Success → pod-2 promoted to ACTIVE
```

---

## 7. AWS Infrastructure Layout

```mermaid
graph TB
    subgraph VPC["AWS VPC (eu-west-2)"]
        subgraph EKS["EKS Cluster"]
            subgraph NS1["namespace: flink-app1"]
                JM1["App1 JobManager<br/>(1 pod)"]
                TM1["App1 TaskManagers<br/>(N pods)"]
            end
            subgraph NS2["namespace: flink-app2"]
                JM2["App2 JobManager<br/>(1 pod)"]
                TM2["App2 TaskManagers<br/>(N pods)"]
            end
            subgraph NS3["namespace: fasc"]
                FC1["Coordinator<br/>replica-1 (ACTIVE)"]
                FC2["Coordinator<br/>replica-2 (STANDBY)"]
            end
        end

        subgraph MSK["MSK Cluster (3 brokers)"]
            B1["broker-1<br/>kafka.m5.xlarge"]
            B2["broker-2<br/>kafka.m5.xlarge"]
            B3["broker-3<br/>kafka.m5.xlarge"]
        end

        subgraph DDB["DynamoDB"]
            T1["business-data-table"]
            T2["fasc-leader"]
            T3["fasc-coordinator-lock"]
            T4["fasc-savepoint-metadata"]
        end

        S3B[("S3 Bucket<br/>fasc/savepoints/<br/>fasc/checkpoints/")]
        CW["CloudWatch<br/>FlinkFASC namespace"]
    end

    subgraph IAM["IAM (IRSA)"]
        R1["flink-app-role<br/>(MSK + DynamoDB + S3)"]
        R2["fasc-coordinator-role<br/>(DynamoDB + S3 + MSK)"]
    end

    JM1 & TM1 --> B1 & B2 & B3
    JM2 & TM2 --> B1 & B2 & B3
    FC1 & FC2 --> B1 & B2 & B3
    TM1 --> T1
    TM2 -.-> T1
    FC1 --> T2 & T3 & T4
    FC1 --> S3B
    FC1 --> CW
    R1 --> NS1 & NS2
    R2 --> NS3

    style EKS fill:#e8f4fd,stroke:#0073bb
    style MSK fill:#fef3e2,stroke:#ff9900
    style DDB fill:#e8f8e8,stroke:#3b7d23
```

---

## 8. Kafka Offset Convergence (Why Savepoint Bootstrap Matters)

```mermaid
graph LR
    subgraph Problem["Without FASC — State Mismatch"]
        P1["App1: 6 months of state<br/>(stateX)"] --> P2["Kafka retention: 7 days"]
        P2 --> P3["App2: replays 7 days<br/>(stateSB)"]
        P3 --> P4["stateX ≠ stateSB<br/>WRONG DynamoDB writes"]
    end

    subgraph Solution["With FASC — Guaranteed Equivalence"]
        S1["App1: savepoint at offset X<br/>(captures full RocksDB state)"] --> S2["App2: restores from savepoint<br/>(has App1 state at offset X)"]
        S2 --> S3["App2: processes X → C<br/>(same Kafka, same order)"]
        S3 --> S4["App2 state at C ≡ App1 state at C<br/>CORRECT — mathematically proven"]
    end

    style P4 fill:#f8d7da,stroke:#721c24
    style S4 fill:#d4edda,stroke:#28a745
```

---

## 9. Control Signal Protocol

```mermaid
sequenceDiagram
    participant COORD as Coordinator
    participant KAFKA as fasc-control-topic
    participant APP1 as App1 ShadowSink
    participant APP2 as App2 ShadowSink

    Note over COORD,APP2: Normal Operation
    loop Every 5s
        COORD->>KAFKA: HEARTBEAT{coordinatorId, timestamp}
        KAFKA->>APP1: HEARTBEAT → update lastContact
        KAFKA->>APP2: HEARTBEAT → update lastContact
    end

    Note over COORD,APP2: Handoff Triggered
    COORD->>KAFKA: PREPARE_HANDOFF{cutoverOffsets, traceId}
    KAFKA->>APP1: PREPARE_HANDOFF → start draining
    KAFKA->>APP2: PREPARE_HANDOFF → noted, continue shadow

    APP1->>KAFKA: DRAINED_AT{offsets, traceId}
    KAFKA->>COORD: DRAINED_AT → proceed to promote

    COORD->>KAFKA: PROMOTE{targetAppId=app2, traceId}
    COORD->>KAFKA: DEMOTE{targetAppId=app1, traceId}
    KAFKA->>APP2: PROMOTE → ShadowSink.mode = ACTIVE
    KAFKA->>APP1: DEMOTE → ShadowSink.mode = SHADOW

    APP2->>KAFKA: PROMOTED_CONFIRMED{traceId}
    KAFKA->>COORD: PROMOTED_CONFIRMED → handoff complete
```
