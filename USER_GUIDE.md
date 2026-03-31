# flink-fasc User Guide: Testing and CI/CD

This guide covers how to run the full test suite locally, how to write new tests, and how to
set up a complete CI/CD pipeline that builds, tests, and deploys flink-fasc to AWS EKS.

---

## Table of Contents

1. [Testing Overview](#1-testing-overview)
2. [Prerequisites](#2-prerequisites)
3. [Running Tests Locally](#3-running-tests-locally)
4. [Understanding Each Test Class](#4-understanding-each-test-class)
5. [Writing New Tests](#5-writing-new-tests)
6. [Manual End-to-End Testing on AWS](#6-manual-end-to-end-testing-on-aws)
7. [CI/CD Pipeline](#7-cicd-pipeline)
8. [Deployment Pipeline](#8-deployment-pipeline)
9. [Release Process](#9-release-process)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Testing Overview

flink-fasc has three levels of tests:

```
┌──────────────────────────────────────────────────────────────────────┐
│  Level 3 — Manual E2E (AWS)                                          │
│  Real MSK + Real EKS + Real DynamoDB                                 │
│  Runs on demand (pre-release)                                        │
├──────────────────────────────────────────────────────────────────────┤
│  Level 2 — Integration Tests (Testcontainers)                        │
│  Real Kafka + LocalStack (DynamoDB/S3) + WireMock (Flink REST)       │
│  fasc-integration-tests/  — runs in CI on every PR                  │
├──────────────────────────────────────────────────────────────────────┤
│  Level 1 — Unit Tests                                                │
│  Pure Java — no Docker, no AWS                                       │
│  fasc-core/, fasc-coordinator/, fasc-aws/  — fastest feedback loop  │
└──────────────────────────────────────────────────────────────────────┘
```

| Test Suite | What it tests | Runtime | Requires Docker |
|---|---|---|---|
| Unit tests | ShadowSink logic, state machine transitions, signal parsing | ~10s | No |
| Integration tests | Full handoff protocol, Kafka signals, DynamoDB leader election | ~3–5 min | Yes |
| Manual E2E (AWS) | Real MSK, Flink, EKS, DynamoDB — production-like | ~20 min | No (uses real AWS) |

---

## 2. Prerequisites

### For Unit Tests

```bash
# Java 11 or higher
java -version   # must show 11+

# Maven 3.8+
mvn -version    # must show 3.8+
```

### For Integration Tests

Everything above, plus:

```bash
# Docker (Desktop or Engine)
docker info     # must show a running daemon
docker pull confluentinc/cp-kafka:7.4.0   # pre-pull to speed up first run
docker pull localstack/localstack:3.0
```

> **Tip:** On Linux, add your user to the `docker` group to avoid `sudo`:
> `sudo usermod -aG docker $USER && newgrp docker`

### For Manual AWS E2E Tests

```bash
# AWS CLI configured with a profile that has access to your test environment
aws sts get-caller-identity

# kubectl connected to your EKS cluster
kubectl cluster-info

# Helm 3
helm version

# Flink CLI (optional — for submitting test jobs)
flink --version
```

---

## 3. Running Tests Locally

### 3.1 Unit Tests Only (fastest — no Docker)

```bash
# Run unit tests in all modules except integration tests
mvn clean test -pl fasc-core,fasc-aws,fasc-coordinator -am
```

Expected output:
```
[INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 0 (fasc-core)
[INFO] Tests run: 8,  Failures: 0, Errors: 0, Skipped: 0 (fasc-aws)
[INFO] Tests run: 21, Failures: 0, Errors: 0, Skipped: 0 (fasc-coordinator)
[INFO] BUILD SUCCESS
```

### 3.2 Integration Tests (requires Docker)

```bash
# Run all integration tests — this starts Kafka + LocalStack containers
mvn clean verify -pl fasc-integration-tests -am

# Alternatively, enable via the Maven profile
mvn clean verify -Pintegration-tests
```

Expected containers to start:
```
Pulling confluentinc/cp-kafka:7.4.0  ...  ✓
Pulling localstack/localstack:3.0     ...  ✓
Kafka started on localhost:XXXXX      ✓
LocalStack started on localhost:YYYYY ✓
DynamoDB tables created               ✓
```

Expected test results:
```
Tests run: 44, Failures: 0, Errors: 0, Skipped: 0
  ControlTopicRoundTripIT      (4 tests)  — Kafka control protocol
  ShadowSinkSignalIT           (6 tests)  — ShadowSink mode transitions
  DynamoDbLeaderElectionIT     (8 tests)  — Leader election correctness
  CloudWatchMetricsIT          (4 tests)  — Metrics publishing
  HandoffOrchestratorIT        (16 tests) — State machine with real Kafka + DynamoDB
  FullHandoffE2EIT             (1 test)   — Complete 7-phase protocol validation
```

### 3.3 Run a Specific Test Class

```bash
# Run only the E2E test
mvn test -pl fasc-integration-tests -Dtest=FullHandoffE2EIT

# Run only HandoffOrchestrator tests
mvn test -pl fasc-integration-tests -Dtest=HandoffOrchestratorIT

# Run only the DynamoDB leader election tests
mvn test -pl fasc-integration-tests -Dtest=DynamoDbLeaderElectionIT
```

### 3.4 Run a Single Test Method

```bash
# Run one specific test within a class
mvn test -pl fasc-integration-tests \
    -Dtest=HandoffOrchestratorIT#fullHandoff_happyPath_completesSuccessfully

# Run the full E2E test
mvn test -pl fasc-integration-tests \
    -Dtest=FullHandoffE2EIT#fullHandoff_e2e_correctWriteRoutingBeforeAndAfter
```

### 3.5 Full Build (all modules + all tests)

```bash
mvn clean verify
```

This runs: compile → unit tests → package → integration tests for all modules.

### 3.6 Skip Tests (build only)

```bash
mvn clean package -DskipTests
```

---

## 4. Understanding Each Test Class

### 4.1 `ControlTopicRoundTripIT`

**What:** Publishes each `SignalType` to a real Kafka topic and verifies the consumer
receives it with correct fields intact.

**Infrastructure:** Kafka (Testcontainers)

**Key assertions:**
- All signal types survive a full Kafka round-trip (serialization + deserialization)
- `traceId`, `sourceAppId`, `targetAppId`, `cutoverOffsets` fields are preserved
- Consumer filter correctly drops signals not targeted at the receiving app
- Broadcast signals (HEARTBEAT, PREPARE_HANDOFF) are received by all consumers

```
Signal flow tested:
  Producer → Kafka topic → Consumer filter → Handler assertion
```

---

### 4.2 `ShadowSinkSignalIT`

**What:** Verifies `ShadowSink` mode transitions when PROMOTE/DEMOTE signals arrive
on the control topic.

**Infrastructure:** Kafka (Testcontainers)

**Key assertions:**
- `ShadowSink` starts in the configured initial mode (`ACTIVE` or `SHADOW`)
- PROMOTE signal switches mode from SHADOW → ACTIVE
- DEMOTE signal switches mode from ACTIVE → SHADOW
- Mode switch is reflected in `getMode()` within 5 seconds
- Writes in ACTIVE mode reach the delegate sink; writes in SHADOW mode are counted but not forwarded
- `getShadowDropCount()` increments for each suppressed write
- Heartbeat loss does **not** cause self-promotion (split-brain prevention)

---

### 4.3 `DynamoDbLeaderElectionIT`

**What:** Verifies all DynamoDB conditional-write operations for leader election.

**Infrastructure:** LocalStack DynamoDB (Testcontainers)

**Key assertions:**
- `tryAcquireLock(coordinatorId)` acquires an unclaimed lock
- A second `tryAcquireLock()` call fails while the first holder is active
- `renewLock()` succeeds for the current holder and resets TTL
- `renewLock()` fails for a non-holder (simulates split-brain scenario)
- `releaseLock()` clears the lock item — next `tryAcquireLock()` succeeds immediately
- `transferFlinkLeader("app1","app2", version)` succeeds when version matches
- `transferFlinkLeader()` fails (returns false) on version mismatch — concurrent update protection
- `getCurrentFlinkLeader()` returns the correct active app after transfer

---

### 4.4 `CloudWatchMetricsIT`

**What:** Verifies that FASC metrics are published to CloudWatch with correct namespace
and dimensions.

**Infrastructure:** LocalStack CloudWatch (Testcontainers)

**Key assertions:**
- `SinkMode` metric published with correct value (0=SHADOW, 1=ACTIVE)
- `ShadowDropCount` increments with each suppressed write
- `HandoffDurationMs` published on handoff completion
- Metrics appear under namespace `FlinkFASC`

---

### 4.5 `HandoffOrchestratorIT`

**What:** Tests the `HandoffOrchestrator` state machine with real Kafka signals and real
DynamoDB leader election. The Flink REST API is mocked with WireMock.

**Infrastructure:** Kafka + LocalStack DynamoDB + WireMock (all Testcontainers)

**Test cases:**

| Test | Verifies |
|---|---|
| `fullHandoff_happyPath_completesSuccessfully` | All state transitions from IDLE → HANDOFF_COMPLETE via real Kafka signals |
| `triggerHandoff_rejectsWhenNotLockHolder` | `IllegalStateException` when standby coordinator attempts handoff |
| `triggerHandoff_rejectsWhenAlreadyInProgress` | `IllegalStateException` on concurrent handoff attempt |
| `drainTimeout_abortsHandoff` | HANDOFF_FAILED state when App1 doesn't drain within 60s |
| `dynamoDbTransferFailure_abortsHandoff` | HANDOFF_FAILED when DynamoDB leader transfer fails (version mismatch) |
| `staleTraceId_signalIgnored` | Signal with wrong traceId does not advance state machine |
| `heartbeats_areSentPeriodically` | HEARTBEAT signals appear on control topic at configured interval |
| _(+ 9 more)_ | Edge cases: null signals, PROMOTING_APP2 timeout, reset-to-IDLE after cooldown |

---

### 4.6 `FullHandoffE2EIT`

**What:** The complete protocol validation. Two live `ShadowSink` instances, the full
`HandoffOrchestrator`, real Kafka, real DynamoDB (LocalStack), and WireMock for Flink REST.

**Infrastructure:** Kafka + LocalStack DynamoDB + WireMock

**7-phase test flow:**

```
Phase 1 — Pre-handoff steady state
  Assert: App1 writes reach delegate sink; App2 writes are suppressed

Phase 2 — Trigger handoff
  Assert: State reaches WAITING_FOR_APP2_CATCHUP after savepoint completes

Phase 3 — App2 catch-up + PREPARE_HANDOFF
  Assert: PREPARE_HANDOFF appears on control topic
  Assert: Both apps can still process records (no blackout window)

Phase 4 — App1 drains → DynamoDB leadership transfer
  Assert: DynamoDB leader = "app2" after coordinator conditional write
  Assert: State reaches PROMOTING_APP2

Phase 5 — ShadowSink mode changes
  Assert: App2 ShadowSink switches to ACTIVE within 5 seconds
  Assert: App1 ShadowSink switches to SHADOW within 5 seconds

Phase 6 — App2 confirms promotion
  Assert: State = HANDOFF_COMPLETE after PROMOTED_CONFIRMED signal

Phase 7 — Post-handoff write routing
  Assert: App1 writes are suppressed (App1 is now SHADOW)
  Assert: App2 writes reach the delegate sink (App2 is now ACTIVE)
```

This is the most important test — it proves the business guarantee:
**zero blackout window and correct DynamoDB write routing end-to-end.**

---

## 5. Writing New Tests

### 5.1 Adding a Unit Test

Place in the same module as the class under test:

```java
// fasc-coordinator/src/test/java/com/flinkfasc/coordinator/handoff/MyNewTest.java
class HandoffOrchestratorUnitTest {

    @Test
    void myScenario() {
        // Arrange — use Mockito for dependencies
        OffsetMonitor mockMonitor = mock(OffsetMonitor.class);
        LeaderElection mockLE = mock(LeaderElection.class);
        when(mockLE.isLockHolder("test-coord")).thenReturn(true);

        HandoffOrchestrator orchestrator = new HandoffOrchestrator(
                buildTestProps(), mockSavepointMgr(), mockMonitor, mockLE, mockProducer());
        orchestrator.initialize();

        // Act
        orchestrator.triggerHandoff();

        // Assert
        assertThat(orchestrator.getState())
                .isIn(HandoffState.SAVEPOINT_IN_PROGRESS, HandoffState.WAITING_FOR_APP2_CATCHUP);
    }
}
```

### 5.2 Adding an Integration Test

Create in `fasc-integration-tests/src/test/java/com/flinkfasc/it/`:

```java
// Step 1: Call ContainerSetup.start() in @BeforeAll — it's idempotent
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MyNewProtocolIT {

    @BeforeAll
    void setup() {
        ContainerSetup.start();   // starts Kafka + LocalStack (shared across all tests)
    }

    @Test
    void myScenario() {
        String brokers = ContainerSetup.kafkaBootstrapServers();
        DynamoDbClient dynamo = ContainerSetup.dynamoDbClient();

        // Build components under test with real infrastructure
        FASCConfiguration config = FASCConfiguration.builder()
                .bootstrapServers(brokers)
                .appId("test-app")
                .controlTopic(ContainerSetup.CONTROL_TOPIC)
                .build();

        // ... write your test
    }
}
```

**Naming convention:** End the class name with `IT` so Maven Surefire picks it up.

### 5.3 Adding a WireMock Stub for Flink REST

```java
WireMockFlinkApi wireMock = new WireMockFlinkApi();
wireMock.start();

// Immediate savepoint completion
wireMock.stubSavepointSuccess("my-job-id", "trigger-001", "s3://bucket/sp-001");

// Savepoint with IN_PROGRESS → COMPLETED delay
wireMock.stubSavepointWithDelay("my-job-id", "trigger-002", "s3://bucket/sp-002");

// Savepoint failure
wireMock.stubSavepointFailure("my-job-id", "trigger-003");

// Custom stub for a new endpoint
wireMock.getServer().stubFor(
    get(urlEqualTo("/jobs/my-job-id/checkpoints"))
        .willReturn(aResponse().withStatus(200).withBody("{...}")));
```

### 5.4 Using Awaitility for Async Assertions

All state transitions are asynchronous. Always use `await()` — never `Thread.sleep()`:

```java
// Good — waits up to 10s, checks every 100ms
await().atMost(10, TimeUnit.SECONDS)
       .untilAsserted(() -> assertThat(orchestrator.getState())
               .isEqualTo(HandoffState.HANDOFF_COMPLETE));

// Good — wait for a Kafka signal to arrive
await().atMost(5, TimeUnit.SECONDS)
       .untilAsserted(() -> assertThat(receivedSignals)
               .anyMatch(s -> s.getSignalType() == SignalType.PROMOTE));

// Bad — flaky under load
Thread.sleep(2000);
assertThat(orchestrator.getState()).isEqualTo(HandoffState.HANDOFF_COMPLETE);
```

---

## 6. Manual End-to-End Testing on AWS

This validates the system against real AWS infrastructure before a production release.

### 6.1 Infrastructure Setup

```bash
# 1. Provision AWS infrastructure
cd terraform/environments/eu-west-2
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars: cluster_name, msk_broker_count, etc.
terraform init && terraform apply -var-file=terraform.tfvars

# 2. Note outputs needed for next steps
terraform output msk_bootstrap_brokers
terraform output fasc_coordinator_role_arn
terraform output savepoint_bucket_name
```

### 6.2 Deploy FASC Coordinator

```bash
# Build coordinator Docker image
mvn clean package -DskipTests -pl fasc-coordinator -am
docker build -t flink-fasc/coordinator:e2e-test fasc-coordinator/
docker tag flink-fasc/coordinator:e2e-test <your-ecr-repo>/flink-fasc/coordinator:e2e-test
docker push <your-ecr-repo>/flink-fasc/coordinator:e2e-test

# Deploy to EKS
helm upgrade --install fasc-coordinator fasc-k8s/helm/fasc-coordinator \
  --namespace fasc --create-namespace \
  --set image.repository=<your-ecr-repo>/flink-fasc/coordinator \
  --set image.tag=e2e-test \
  --set env.FASC_BOOTSTRAP_SERVERS=$(terraform output -raw msk_bootstrap_brokers) \
  --set env.FASC_SAVEPOINT_S3_BUCKET=$(terraform output -raw savepoint_bucket_name) \
  --set env.FASC_APP1_JOB_ID=<app1-flink-job-id> \
  --set env.FASC_APP2_JOB_ID=<app2-flink-job-id> \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=$(terraform output -raw fasc_coordinator_role_arn)

# Verify coordinator pods are healthy
kubectl -n fasc get pods
# Expected: 2 pods, both Running, one Ready (the lock holder)

kubectl -n fasc logs deployment/fasc-coordinator | grep "ACTIVE\|STANDBY"
# Expected: one pod logs "ACTIVE", the other logs "STANDBY"
```

### 6.3 Submit Flink Jobs

```bash
# Build the Flink job JAR
mvn clean package -DskipTests -pl fasc-examples -am

# Submit App1 (Active)
aws flink create-application \
  --application-name fasc-app1 \
  --runtime-environment FLINK-1_18 \
  --service-execution-role <flink-execution-role-arn> \
  --application-configuration '{
    "FlinkApplicationConfiguration": {
      "CheckpointConfiguration": {"ConfigurationType": "CUSTOM", "CheckpointingEnabled": true}
    },
    "EnvironmentProperties": {
      "PropertyGroups": [{
        "PropertyGroupId": "kinesis.analytics.flink.run.options",
        "PropertyMap": {
          "FASC_APP_ID": "app1",
          "FASC_INITIAL_MODE": "ACTIVE",
          "FASC_BOOTSTRAP_SERVERS": "<msk-brokers>",
          "FASC_CONTROL_TOPIC": "fasc-control-topic"
        }
      }]
    }
  }'

# Note the App1 Flink job ID, then submit App2 similarly with FASC_APP_ID=app2, FASC_INITIAL_MODE=SHADOW
```

### 6.4 Run the Manual E2E Test Sequence

```bash
# Set coordinator endpoint
COORD=http://$(kubectl -n fasc get svc fasc-coordinator -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

# Step 1: Verify baseline health
curl -s $COORD/fasc/status | jq .
# Expected:
# {
#   "handoffState": "IDLE",
#   "currentFlinkLeader": "app1",
#   "app2MaxLagMs": 12345,    ← decreasing as App2 catches up
#   "app2Ready": false,
#   "lockHolder": true
# }

# Step 2: Wait for App2 to fully catch up
watch -n5 'curl -s $COORD/fasc/status | jq "{state:.handoffState, lag:.app2MaxLagMs, ready:.app2Ready}"'
# Wait until app2Ready = true (lag = 0 stable for 10s)

# Step 3: Trigger handoff
curl -X POST $COORD/fasc/handoff \
  -H "Content-Type: application/json" \
  -d '{"requestedBy":"manual-e2e-test","reason":"E2E validation test run"}' | jq .
# Expected: {"status":"ACCEPTED","traceId":"<uuid>","message":"..."}

# Step 4: Monitor handoff progress (should complete in < 2 seconds)
watch -n1 'curl -s $COORD/fasc/status | jq .handoffState'
# Expected progression:
#   SAVEPOINT_IN_PROGRESS
#   WAITING_FOR_APP2_CATCHUP
#   HANDOFF_INITIATED
#   WAITING_FOR_APP1_DRAIN
#   PROMOTING_APP2
#   HANDOFF_COMPLETE
#   IDLE  (after 5s cooldown)

# Step 5: Verify DynamoDB leadership transferred
curl -s $COORD/fasc/status | jq .currentFlinkLeader
# Expected: "app2"

# Step 6: Verify write routing in DynamoDB
# Query DynamoDB for recent writes — should see app2's writes, no app1 writes
aws dynamodb scan --table-name <your-business-table> \
  --filter-expression "updatedBy = :app" \
  --expression-attribute-values '{":app":{"S":"app2"}}' \
  --query "Count"
```

### 6.5 Manual Failure Injection Tests

```bash
# Test: Coordinator failover (kill the active pod — standby should promote)
ACTIVE_POD=$(kubectl -n fasc get pods -o json | jq -r \
  '.items[] | select(.metadata.annotations["fasc.lockHolder"]=="true") | .metadata.name')
kubectl -n fasc delete pod $ACTIVE_POD
# Expected: standby pod acquires DynamoDB lock within 30s (TTL window)
# Expected: no data loss (App1/App2 continue processing independently)

# Test: Coordinator network partition (simulate by scaling coordinator to 0)
kubectl -n fasc scale deployment/fasc-coordinator --replicas=0
# Expected: Apps continue running in their current mode (no self-promotion)
# Expected: Flink apps log "Coordinator heartbeat missed" warnings
kubectl -n fasc scale deployment/fasc-coordinator --replicas=2

# Test: App2 lag spike during handoff
# Deliberately throttle App2 and verify handoff waits for lag to stabilize
# (Reduce App2 Flink parallelism temporarily, observe lagStabilityWindowMs behavior)
```

---

## 7. CI/CD Pipeline

### 7.1 Pipeline Overview

```
git push / PR opened
        │
        ▼
┌───────────────────────────────────────────────────────────────────┐
│  GitHub Actions: .github/workflows/ci.yml                         │
│                                                                   │
│  Job 1: build-and-test (matrix: Java 11, Java 17)                │
│    ├── mvn clean package -DskipTests             (~2 min)         │
│    ├── mvn test (unit tests)                     (~30 sec)        │
│    └── mvn verify -pl fasc-integration-tests     (~4 min)         │
│                                                                   │
│  Job 2: code-quality (after Job 1 passes)                         │
│    └── OWASP dependency-check (CVE scan)         (~3 min)         │
│                                                                   │
│  Job 3: docker-build (main branch only)                           │
│    └── docker build fasc-coordinator             (~2 min)         │
└───────────────────────────────────────────────────────────────────┘
        │
        │  (on tag push: v*)
        ▼
┌───────────────────────────────────────────────────────────────────┐
│  GitHub Actions: .github/workflows/release.yml                    │
│                                                                   │
│    ├── mvn clean verify (full test suite)                         │
│    ├── docker build + push to ECR                                 │
│    ├── helm upgrade (staging environment)                         │
│    ├── run smoke tests against staging                            │
│    └── helm upgrade (production, manual approval required)        │
└───────────────────────────────────────────────────────────────────┘
```

### 7.2 Setting Up GitHub Actions

#### Required GitHub Secrets

Go to **GitHub → Repository → Settings → Secrets and variables → Actions** and add:

| Secret | Value | Used by |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | IAM access key for CI role | docker-build, release |
| `AWS_SECRET_ACCESS_KEY` | IAM secret for CI role | docker-build, release |
| `AWS_REGION` | e.g. `eu-west-2` | docker-build, release |
| `ECR_REGISTRY` | e.g. `123456789.dkr.ecr.eu-west-2.amazonaws.com` | docker-build, release |
| `EKS_CLUSTER_NAME` | e.g. `fasc-eks-cluster` | release deployment |
| `STAGING_FASC_BOOTSTRAP_SERVERS` | MSK brokers for staging | smoke tests |
| `PROD_FASC_BOOTSTRAP_SERVERS` | MSK brokers for production | production deploy |

#### IAM Permissions for the CI Role

The CI role needs these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ECRPushPull",
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EKSDescribe",
      "Effect": "Allow",
      "Action": ["eks:DescribeCluster"],
      "Resource": "arn:aws:eks:eu-west-2:*:cluster/fasc-*"
    }
  ]
}
```

### 7.3 The CI Workflow (Built In)

The workflow at `.github/workflows/ci.yml` runs automatically on every push and PR.
No additional setup is needed beyond the secrets above for the Docker build step.

To verify it works:

```bash
# Push a branch and check GitHub Actions tab
git checkout -b test/ci-check
git commit --allow-empty -m "test: verify CI pipeline"
git push origin test/ci-check
# Open GitHub → Actions → watch the workflow run
```

### 7.4 Creating the Release Workflow

Create `.github/workflows/release.yml` for automated release deployments:

```yaml
# .github/workflows/release.yml
name: Release

on:
  push:
    tags:
      - 'v*'   # Triggers on v1.0.0, v1.2.3, etc.

jobs:
  release:
    name: Build, Push, Deploy
    runs-on: ubuntu-latest
    environment: production   # requires manual approval in GitHub Environments

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-java@v4
        with:
          java-version: '11'
          distribution: temurin
          cache: maven

      - name: Full test suite
        run: mvn clean verify -B

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ secrets.AWS_REGION }}

      - name: Login to ECR
        id: login-ecr
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push coordinator Docker image
        env:
          ECR_REGISTRY: ${{ secrets.ECR_REGISTRY }}
          IMAGE_TAG: ${{ github.ref_name }}   # e.g. v1.2.3
        run: |
          docker build -t $ECR_REGISTRY/flink-fasc/coordinator:$IMAGE_TAG fasc-coordinator/
          docker tag  $ECR_REGISTRY/flink-fasc/coordinator:$IMAGE_TAG \
                      $ECR_REGISTRY/flink-fasc/coordinator:latest
          docker push $ECR_REGISTRY/flink-fasc/coordinator:$IMAGE_TAG
          docker push $ECR_REGISTRY/flink-fasc/coordinator:latest

      - name: Deploy to staging
        env:
          IMAGE_TAG: ${{ github.ref_name }}
        run: |
          aws eks update-kubeconfig --name ${{ secrets.EKS_CLUSTER_NAME }} --region ${{ secrets.AWS_REGION }}
          helm upgrade --install fasc-coordinator fasc-k8s/helm/fasc-coordinator \
            --namespace fasc-staging --create-namespace \
            --set image.tag=$IMAGE_TAG \
            --set env.FASC_BOOTSTRAP_SERVERS=${{ secrets.STAGING_FASC_BOOTSTRAP_SERVERS }} \
            --wait --timeout=5m

      - name: Smoke test staging
        run: |
          COORD_URL=http://$(kubectl -n fasc-staging get svc fasc-coordinator \
            -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
          # Wait for coordinator to be ready
          for i in {1..12}; do
            STATUS=$(curl -s -o /dev/null -w "%{http_code}" $COORD_URL/fasc/ready)
            if [ "$STATUS" = "200" ]; then echo "Coordinator ready"; break; fi
            sleep 5
          done
          # Verify healthy state
          curl -f -s $COORD_URL/fasc/status | jq -e '.handoffState == "IDLE"'
          echo "Smoke test passed"

      - name: Deploy to production
        if: success()
        env:
          IMAGE_TAG: ${{ github.ref_name }}
        run: |
          helm upgrade fasc-coordinator fasc-k8s/helm/fasc-coordinator \
            --namespace fasc --reuse-values \
            --set image.tag=$IMAGE_TAG \
            --set env.FASC_BOOTSTRAP_SERVERS=${{ secrets.PROD_FASC_BOOTSTRAP_SERVERS }} \
            --wait --timeout=5m
```

### 7.5 Branch Protection Rules

Configure in **GitHub → Repository → Settings → Branches → Add rule for `main`**:

```
Branch name pattern: main
☑ Require a pull request before merging
☑ Require status checks to pass before merging
  - build-and-test (Java 11)
  - build-and-test (Java 17)
  - code-quality
☑ Require branches to be up to date before merging
☑ Do not allow bypassing the above settings
```

---

## 8. Deployment Pipeline

### 8.1 Coordinator Rolling Update (Zero-Downtime)

The coordinator deployment uses 2 replicas with a DynamoDB TTL lock. A rolling update
maintains availability because:

1. New pod starts → tries to acquire DynamoDB lock → fails (current pod holds it)
2. New pod runs as hot standby
3. Kubernetes terminates old pod → `@PreDestroy` releases DynamoDB lock
4. New pod acquires lock within 5s → becomes active coordinator

```bash
# Trigger a rolling update with a new image tag
kubectl -n fasc set image deployment/fasc-coordinator \
    coordinator=<ecr-repo>/flink-fasc/coordinator:v1.2.3

# Watch the rollout
kubectl -n fasc rollout status deployment/fasc-coordinator

# Verify no handoff was interrupted
kubectl -n fasc logs deployment/fasc-coordinator | grep "HANDOFF\|ACTIVE\|STANDBY" | tail -20
```

### 8.2 Upgrading a Flink Job (The Full FASC Flow)

This is the primary use case for flink-fasc — upgrading App1 without downtime:

```bash
COORD=http://<fasc-coordinator-endpoint>

# 1. Verify system is healthy and App2 is in warm-standby
curl -s $COORD/fasc/status | jq '{state:.handoffState, ready:.app2Ready, leader:.currentFlinkLeader}'

# 2. Wait for App2 to be ready (if not already)
watch -n5 'curl -s $COORD/fasc/status | jq .app2Ready'

# 3. Trigger handoff — App2 becomes active, App1 enters shadow mode
TRACE=$(curl -s -X POST $COORD/fasc/handoff \
  -H "Content-Type: application/json" \
  -d "{\"requestedBy\":\"cicd-pipeline\",\"reason\":\"Upgrade App1 to $NEW_VERSION\"}" \
  | jq -r .traceId)

echo "Handoff initiated with traceId: $TRACE"

# 4. Wait for handoff to complete (typically < 2 seconds)
for i in {1..30}; do
  STATE=$(curl -s $COORD/fasc/status | jq -r .handoffState)
  echo "State: $STATE"
  if [ "$STATE" = "IDLE" ]; then echo "Handoff complete"; break; fi
  sleep 1
done

# 5. Verify App2 is now the leader
curl -s $COORD/fasc/status | jq .currentFlinkLeader   # should be "app2"

# 6. Upgrade App1 (it is now in SHADOW mode — safe to restart)
aws flink update-application --application-name fasc-app1 \
  --current-application-version-id $CURRENT_VERSION \
  --application-configuration-update '{...new config...}'

# 7. Wait for App1 to be running and warmed up in SHADOW mode
aws flink describe-application --application-name fasc-app1 \
  | jq .ApplicationDetail.ApplicationStatus   # wait for RUNNING

# 8. Trigger reverse handoff — App1 becomes active again
curl -X POST $COORD/fasc/handoff \
  -H "Content-Type: application/json" \
  -d "{\"requestedBy\":\"cicd-pipeline\",\"reason\":\"Restore App1 after upgrade\"}"
```

### 8.3 Rollback Procedure

If a deployment goes wrong after handoff:

```bash
# Check what went wrong
kubectl -n fasc logs deployment/fasc-coordinator --tail=100

# If App2 misbehaves after promotion — trigger immediate reverse handoff
curl -X POST $COORD/fasc/handoff \
  -H "Content-Type: application/json" \
  -d '{"requestedBy":"ops-on-call","reason":"ROLLBACK: App2 post-promotion issue"}'

# If coordinator is unreachable — manually update DynamoDB leader
# (emergency only — bypasses FASC protocol)
aws dynamodb update-item --table-name fasc-leader \
  --key '{"PK":{"S":"FLINK_LEADER"}}' \
  --update-expression "SET activeAppId = :app1, version = :v" \
  --condition-expression "version = :current" \
  --expression-attribute-values '{
    ":app1":{"S":"app1"},
    ":v":{"N":"99"},
    ":current":{"N":"<current-version>"}
  }'
# Then restart both Flink apps so they re-read the DynamoDB leader state
```

---

## 9. Release Process

### 9.1 Versioning

flink-fasc uses [Semantic Versioning](https://semver.org/):
- `MAJOR.MINOR.PATCH` (e.g. `1.2.3`)
- `MAJOR` bump: protocol-breaking changes (new signal types, DynamoDB schema changes)
- `MINOR` bump: new features, new modules (backwards compatible)
- `PATCH` bump: bug fixes, dependency updates

### 9.2 Cutting a Release

```bash
# 1. Ensure main is green (all CI checks pass)
# 2. Update version in all pom.xml files
mvn versions:set -DnewVersion=1.2.3 -DgenerateBackupPoms=false
mvn versions:commit

# 3. Update CHANGELOG.md (see below)

# 4. Commit and tag
git add -A
git commit -m "release: bump version to 1.2.3"
git tag -a v1.2.3 -m "Release 1.2.3 — <summary of changes>"
git push origin main --tags

# 5. The release.yml GitHub Actions workflow triggers automatically on tag push
#    Watch it at: GitHub → Actions → Release
```

### 9.3 CHANGELOG Format

Maintain a `CHANGELOG.md` at the repo root:

```markdown
## [1.2.3] - 2026-04-15

### Fixed
- CoordinatorLockManager: use ApplicationContext.close() instead of System.exit()
- OffsetMonitor: atomic compareAndSet for lagZeroSince to prevent race condition

### Added
- FascCoordinatorProperties: @NotBlank validation on required fields

### Changed
- HandoffOrchestrator: getActiveTraceId() now exposed publicly for REST API
```

---

## 10. Troubleshooting

### Integration Tests Fail with "Could not find a valid Docker environment"

```bash
# Check Docker is running
docker info

# On macOS/Windows — ensure Docker Desktop is started

# On Linux — check socket permissions
ls -la /var/run/docker.sock
# If permission denied:
sudo chmod 666 /var/run/docker.sock
# Or add user to docker group (preferred):
sudo usermod -aG docker $USER && newgrp docker
```

### Integration Tests Time Out on Container Startup

```bash
# Pre-pull images to speed up first run
docker pull confluentinc/cp-kafka:7.4.0
docker pull localstack/localstack:3.0

# If behind a corporate proxy, configure Docker proxy:
# ~/.docker/config.json:
# {"proxies": {"default": {"httpProxy": "http://proxy:3128"}}}
```

### `HANDOFF_FAILED` State in Tests

Check logs for the specific failure reason:

```bash
# Run test with debug logging
mvn test -pl fasc-integration-tests -Dtest=FullHandoffE2EIT \
    -Dlogback.configurationFile=src/test/resources/logback-test.xml
```

Common causes:
- `Savepoint did not complete successfully` → WireMock stub not set up, or returning FAILED status
- `DynamoDB leader transfer failed` → version mismatch; check `createLeaderTable` seeded the right version
- `App1 drain timeout` → `DRAINED_AT` signal not sent; check traceId matches

### Coordinator Pod Not Becoming Active

```bash
kubectl -n fasc logs <pod-name> | grep "lock\|ACTIVE\|STANDBY"

# Common issues:
# "Lock acquisition attempt 1/5 failed" — DynamoDB table not created, or wrong table name
# "Lock renewal failed" — DynamoDB connectivity issue (check IAM/IRSA role)
```

### App2 ShadowSink Not Receiving Signals

```bash
# Check Kafka topic has the right partitions and retention
aws kafka describe-cluster --cluster-arn <arn> | jq .ClusterInfo.BrokerNodeGroupInfo

# Verify control topic exists
kafka-topics.sh --bootstrap-server <brokers> --list | grep fasc-control-topic

# Check consumer group lag for the ShadowSink consumer
kafka-consumer-groups.sh --bootstrap-server <brokers> \
    --group fasc-sink-app2 --describe
```

### High Consumer Lag in OffsetMonitor

If App2's lag is not decreasing:

```bash
# Check App2 Flink job is actually running
aws flink describe-application --application-name fasc-app2 \
    | jq .ApplicationDetail.ApplicationStatus

# Check consumer group is active
kafka-consumer-groups.sh --bootstrap-server <brokers> \
    --group consumer-group-app2 --describe
# If state is "Dead" — App2 is not running or crashed

# Check Flink TaskManager logs for backpressure
kubectl logs <app2-taskmanager-pod> | grep "backpressure\|checkpoint\|error"
```
