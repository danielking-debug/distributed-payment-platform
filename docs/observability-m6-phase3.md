# Observability — Milestone 6 Phase 3 (Domain metrics)

Low-cardinality Micrometer metrics for payment-domain outcomes. Exposed via existing `/actuator/prometheus`.

## Component

`PaymentLedgerMetrics` — single facade over `MeterRegistry`. Tag values are fixed enums only.

## Metrics

| Name | Type | Tags | When |
|------|------|------|------|
| `payment_ledger.payments.submitted` | Counter | `outcome=created\|replayed\|conflict\|rejected` | Submit path |
| `payment_ledger.payments.processed` | Counter | `outcome=completed\|failed\|skipped_duplicate\|skipped_not_pending` | Ledger processor |
| `payment_ledger.payments.processing` | Timer | `outcome=` (same as processed) | Processor duration |
| `payment_ledger.outbox.publish` | Counter | `outcome=success\|failure` | Outbox row publish |
| `payment_ledger.reconciliation.runs` | Counter | `outcome=success` | Successful `run()` completion |
| `payment_ledger.reconciliation` | Timer | (none) | Duration of every `run()` attempt (success or exception) |
| `payment_ledger.reconciliation.findings` | Counter | `severity=CRITICAL\|WARNING\|ANOMALY` | Each finding |
| `payment_ledger.dlt.replay` | Counter | `outcome=published\|rejected\|publish_failed` | Manual DLT replay |

## Rules

- Never use payment IDs, account IDs, or idempotency keys as tags.
- Rejection is recorded once at `PaymentSubmissionService.validateIdempotencyKey`.
- Conflict is recorded once at the service throw site.
- Reconciliation duration always; run success only after successful completion.

## Not implemented

- **Automatic DLT publish counters** (`loggingDeadLetterRecoverer`): optional in plan; omitted because wiring metrics into the static recoverer factory without changing retry/DLT semantics is awkward, and distinguishing “attempt vs durable DLT publish” is less reliable than the manual replay path.
- Kafka per-retry attempt counters (framework metrics + DLT path remain the signal).
- Outbox depth gauges.

## Verification

```bash
curl -s http://localhost:8080/actuator/prometheus | grep payment_ledger
```
