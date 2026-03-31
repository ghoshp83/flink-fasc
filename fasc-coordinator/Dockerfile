# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy parent POM and all module POMs first (layer-cache friendly).
# Include fasc-integration-tests and fasc-examples POMs so Maven can resolve
# the reactor, even though we only build fasc-coordinator.
COPY pom.xml .
COPY fasc-core/pom.xml              fasc-core/pom.xml
COPY fasc-aws/pom.xml               fasc-aws/pom.xml
COPY fasc-coordinator/pom.xml       fasc-coordinator/pom.xml
COPY fasc-examples/pom.xml          fasc-examples/pom.xml
COPY fasc-integration-tests/pom.xml fasc-integration-tests/pom.xml

# Download all dependencies (cached as a layer if POMs don't change)
RUN mvn dependency:go-offline -pl fasc-coordinator -am -q || true

# Copy source (only the modules needed to build fasc-coordinator)
COPY fasc-core/src       fasc-core/src
COPY fasc-aws/src        fasc-aws/src
COPY fasc-coordinator/src fasc-coordinator/src

# Build the coordinator fat JAR (skip tests — tests run in CI)
RUN mvn package -pl fasc-coordinator -am -DskipTests -q

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine

# Non-root user for security
RUN addgroup -S fasc && adduser -S fasc -G fasc
USER fasc

WORKDIR /app

COPY --from=builder /build/fasc-coordinator/target/fasc-coordinator-*.jar app.jar

EXPOSE 8080

# JVM tuning for containers:
#   MaxRAMPercentage: use up to 75% of container memory for heap
#   G1GC: recommended for low-latency coordinator workloads
#   ExitOnOutOfMemoryError: fail fast on OOM so Kubernetes restarts the pod
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 \
               -XX:+UseG1GC \
               -XX:+ExitOnOutOfMemoryError \
               -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
