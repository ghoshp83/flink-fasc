# Contributing to flink-fasc

Thank you for your interest in contributing! `flink-fasc` is an open-source protocol for
zero-downtime Flink upgrades. Contributions are welcome — from bug fixes and documentation
to new transport backends and leader election implementations.

---

## Development Setup

### Prerequisites

- Java 11+ (11 recommended for Flink 1.18 compatibility)
- Maven 3.8+
- Docker (required for integration tests via Testcontainers)
- An AWS account (optional — integration tests use Testcontainers for Kafka/LocalStack)

### Build

```bash
git clone https://github.com/ghoshp83/flink-fasc.git
cd flink-fasc

# Build without tests
mvn clean package -DskipTests

# Run unit tests only
mvn clean verify -DskipITs

# Run all tests (requires Docker running)
mvn clean verify
```

### Module Structure

```
flink-fasc/
├── fasc-core/                  # Pure Flink library (ShadowSink, control protocol)
├── fasc-coordinator/           # Spring Boot coordinator service
├── fasc-aws/                   # AWS SDK v2 implementations
├── fasc-examples/              # Reference Flink job
├── fasc-integration-tests/     # Testcontainers integration tests
├── fasc-k8s/                   # Helm chart
└── terraform/                  # AWS infrastructure
```

---

## How to Contribute

### Reporting Bugs

Open a GitHub issue with:
1. A clear title describing the problem
2. Steps to reproduce (include Flink version, Java version, AWS region)
3. Expected vs actual behaviour
4. Relevant logs (coordinator logs, Flink TaskManager logs)

### Feature Requests

Open a GitHub issue tagged `enhancement`. For significant changes (new transport backends,
protocol changes), open a discussion first to align on design before investing in implementation.

### Pull Requests

1. Fork the repository and create a feature branch from `main`
2. Make your changes with tests (see Testing below)
3. Ensure `mvn clean verify` passes
4. Open a PR with a clear description of the change and why it's needed

**PR requirements:**
- Unit tests for new business logic
- Integration test update if the FASC protocol or coordinator behaviour changes
- No reduction in test coverage
- Javadoc for public API changes in `fasc-core`

---

## Testing

### Unit Tests

```bash
mvn clean test
```

Standard JUnit 5 tests. Mock AWS SDK calls with Mockito.

### Integration Tests

```bash
mvn clean verify
```

Uses Testcontainers 1.21+ to spin up:
- Apache Kafka (for control topic and business topic)
- LocalStack (for DynamoDB — leader election, savepoint metadata)

Docker must be running. Tests are tagged `@Tag("integration")` and run in the
`fasc-integration-tests` module.

### Adding an Integration Test

Integration tests live in `fasc-integration-tests/src/test/java/`. Follow the existing
patterns in `HandoffOrchestratorIT` and `FullHandoffE2EIT`.

---

## Design Principles

When contributing, please respect these core design constraints:

1. **`fasc-core` must have zero AWS dependencies** — it is a pure Flink library
2. **The FASC protocol is deterministic** — any change to state transition logic requires
   updating the proof in [DESIGN.md](../DESIGN.md)
3. **No split-brain** — all changes to leader election must preserve the DynamoDB
   conditional write safety guarantee
4. **ShadowSink must be transparent** — adding FASC to an existing job should require
   exactly one line change

---

## Adding a New Transport Backend

The control protocol is transport-agnostic. To add Kinesis or Redis Streams:

1. Implement `ControlTopicProducer` and `ControlTopicConsumer` interfaces in a new
   `fasc-kinesis` or `fasc-redis` module
2. Provide a `FASCConfiguration` builder extension for the new transport
3. Add integration tests using the appropriate Testcontainers module
4. Document in README.md and DESIGN.md

---

## Adding a New Leader Election Backend

To support non-AWS deployments (etcd, ZooKeeper, Consul):

1. Implement the `LeaderElection` interface in a new `fasc-etcd` (or similar) module
2. The implementation must provide the same TTL-based mutual exclusion guarantees as
   `DynamoDbLeaderElection`
3. Document the TTL and renewal semantics clearly

---

## Code Style

- Standard Java conventions (4-space indent, 120-char line limit)
- Checkstyle configuration: `checkstyle.xml` (if present, run with `mvn checkstyle:check`)
- No wildcard imports
- Logger name matches class name (`LoggerFactory.getLogger(ClassName.class)`)
- Prefer explicit types over `var` for readability

---

## Commit Messages

Follow conventional commits format:
```
feat(core): add ShadowSink drain callback for graceful shutdown
fix(coordinator): use ApplicationContext.close() instead of System.exit on lock loss
docs: add DynamoDB cost analysis to README
test(it): add FullHandoffE2EIT phase-4 state assertion
```

---

## Community

- GitHub Issues: bug reports and feature requests
- GitHub Discussions: design questions, usage questions, "how do I..." questions
- PRs: code contributions

We follow the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/).
Be respectful, constructive, and welcoming to contributors of all experience levels.
