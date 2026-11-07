# Observability — Milestone 6 Phase 1

Foundation only: Micrometer Tracing (OpenTelemetry bridge), W3C propagation, OTLP export, log correlation, Prometheus registry. **No domain spans or Kafka instrumentation yet** (Phase 2).

## Dependencies

- `micrometer-registry-prometheus` → `/actuator/prometheus`
- `micrometer-tracing-bridge-otel` → W3C trace context + MDC `traceId` / `spanId`
- `opentelemetry-exporter-otlp` → OTLP HTTP export (Boot-managed versions)

## Configuration (`application.yml`)

| Property | Default | Purpose |
|----------|---------|---------|
| `management.tracing.sampling.probability` | `1.0` (`TRACING_SAMPLING_PROBABILITY`) | Local/dev: sample all traces |
| `management.tracing.propagation.type` | `w3c` | W3C Trace Context |
| `management.otlp.tracing.endpoint` | `http://localhost:4318/v1/traces` (`OTEL_EXPORTER_OTLP_ENDPOINT`) | OTLP HTTP to Jaeger/Collector |
| Console log pattern | includes `traceId` / `spanId` from MDC | Correlate logs with traces when a span is active |

Jaeger is **optional**. If nothing listens on the OTLP endpoint, the application still starts; export errors are non-fatal.

## Optional Jaeger (Compose profile)

```bash
docker compose --profile observability up -d jaeger
# UI: http://localhost:16686
```

Default app OTLP URL matches Jaeger’s OTLP HTTP port (`4318`).

Without Jaeger:

```bash
docker compose up -d   # postgres, kafka, redis only — no jaeger
mvn spring-boot:run
```

## Quick checks

```bash
# Prometheus exposition (registry must be on the classpath)
curl -s http://localhost:8080/actuator/prometheus | head

# Health still works without Jaeger
curl -s http://localhost:8080/actuator/health
```

HTTP spans appear automatically for controller traffic once Phase 1 is running; **Kafka/outbox custom spans are Phase 2**.

Phase 2 (Kafka/outbox): see [observability-m6-phase2.md](observability-m6-phase2.md).
