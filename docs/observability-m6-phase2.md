# Observability — Milestone 6 Phase 2 (Kafka & outbox tracing)

## Two-trace architecture (Option A)

No `traceparent` is stored on `outbox_events` (no schema change).

```text
Trace 1 (HTTP)
  POST /payments → write payment + outbox row → response
  (request ends; span closes)

Trace 2 (async processing) — new root
  outbox.publish          (scheduler; attribute payment.id)
    └─ Kafka produce      (Spring Kafka template observation; W3C inject)
         └─ Kafka consume (Spring Kafka listener observation; W3C extract)
              └─ payment.process  (attribute payment.id)
```

**These are two correlated traces, not one continuous end-to-end trace.**

Correlate in Jaeger (or logs) by searching the **`payment.id`** span attribute (equals payment UUID / outbox `aggregate_id`).

## Kafka W3C propagation

Enabled without custom interceptors:

| Setting | Effect |
|---------|--------|
| `spring.kafka.template.observation-enabled: true` | Producer spans + inject `traceparent` (and related) headers |
| `spring.kafka.listener.observation-enabled: true` | Consumer spans + extract headers |
| `KafkaConsumerConfig` `setObservationEnabled(true)` | Same for the explicit container factory bean |

Requires Phase 1 Micrometer Tracing + OTel bridge so propagators are on the classpath.

## Domain spans

| Name | Where | Notes |
|------|--------|------|
| `outbox.publish` | `OutboxPublisher` | New root per event; `payment.id`, `messaging.destination` |
| `payment.process` | `PaymentProcessingService` | Child of consumer span when context is present |
| `dlt.replay` | `DltReplayService` | Manual replay; producer headers on republish |

Sensitive fields (amounts, account owner names, full payloads) are **not** added as span attributes.

## How to verify locally

```bash
docker compose --profile observability up -d
mvn spring-boot:run

# Submit a payment; note paymentId from response
curl -s -X POST http://localhost:8080/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"fromAccountId":"...","toAccountId":"...","amount":10.00,"currency":"USD"}'

# Jaeger UI: http://localhost:16686
# Search service distributed-payment-platform, tag payment.id=<uuid>
# Expect separate HTTP trace and outbox/consumer trace sharing that tag
```

Logs include `[traceId=… spanId=…]` when a span is active (Phase 1 pattern).

## Limitations

- HTTP and processing are **not** parent/child without storing context on the outbox row (deferred).
- If Jaeger is down, export fails non-fatally; app still runs.
- Notification listeners rely on native consumer observation only (no extra domain span).
- Unit tests use `ObservationRegistry.NOOP` or `TestObservationRegistry`; full header round-trip needs a live broker + tracer (manual / IT).
