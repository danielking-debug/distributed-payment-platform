# =============================================================================
# M7.1 — Multi-stage image for distributed-payment-platform
# Build:  docker build -t distributed-payment-platform:0.1.0 .
# Run:    requires Postgres, Kafka, Redis (Compose or later Kubernetes)
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: compile and package
# Official Maven + JDK 21 image (no apt install of Maven on every build).
# Tests are skipped: the suite is validated outside image builds; Testcontainers
# must not run during docker build.
# -----------------------------------------------------------------------------
FROM maven:3.9.9-eclipse-temurin-21-jammy AS build

WORKDIR /workspace

# Copy pom first, then sources (improves layer reuse when only src changes)
COPY pom.xml .
COPY src ./src

RUN mvn -B -DskipTests package

# -----------------------------------------------------------------------------
# Stage 2: minimal JRE runtime
# -----------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS runtime

# Non-root user (numeric UID works with Kubernetes runAsNonRoot)
RUN groupadd --system --gid 1001 app \
    && useradd --system --uid 1001 --gid app --home-dir /app --shell /usr/sbin/nologin app

WORKDIR /app

# pom.xml <finalName>distributed-payment-platform</finalName>
# → /workspace/target/distributed-payment-platform.jar
COPY --from=build /workspace/target/distributed-payment-platform.jar /app/app.jar

RUN chown app:app /app/app.jar

USER 1001:1001

EXPOSE 8080

# JVM is PID 1 (true exec form). Container-aware heap leaves room for
# metaspace, thread stacks, and direct buffers under a memory limit.
ENTRYPOINT ["java", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-jar", \
    "/app/app.jar"]
