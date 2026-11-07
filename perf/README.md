# Performance evidence (Milestone 5 — complete)

This directory holds **benchmark tooling and recorded local observations**.
It does **not** define production capacity.

**All latency, throughput, and scale numbers are machine- and environment-dependent.**
Do not treat a single laptop run as production capacity. Do not invent results.

**Milestone 5 status: complete** (Slice A evidence tooling + Slice B recon optimization + chunk-fix for PostgreSQL bind limits + balance-cache correctness).

## Layout

```text
perf/
  README.md                 # this file
  k6/                       # k6 load scripts (scenarios A–F)
  sql/                      # EXPLAIN + inspection SQL
  results/                  # templates + recorded runs
```

Existing legacy scripts under `loadtest/` are separate; prefer `perf/` for M5.

## Slice overview

| Slice | Scope | Production code? |
|-------|--------|------------------|
| **A** | k6 scripts, EXPLAIN/inspection SQL, result template, how to measure | No (tooling/docs only) |
| **B** | Reconciliation N+1 → bulk fetch; then chunked bulk fetch (1,000 IDs); balance served from Postgres only | Yes (recon + `AccountService` balance path) |
| **Chunk-fix** | Bulk `IN` / `findAllById` partitioned so each query stays under PostgreSQL’s 65,535-parameter limit | Yes (`ReconciliationService`) |

Slice A is **not** “future work.” Slice B recon batching and chunking are **implemented and verified**.

## Prerequisites

| Component | Required for | Notes |
|-----------|--------------|-------|
| JDK 21 + Maven | Unit tests / running the app | Existing project |
| **Local PostgreSQL** | Submit path, recon, SQL EXPLAIN | Docker **not** required for unit tests |
| Redis | App startup if health checks expect it | As per local setup |
| Kafka | Measuring **async processing** throughput / lag | Optional for submit-only latency |
| [k6](https://k6.io/docs/get-started/installation/) | HTTP load scenarios | Install on the host |
| `psql` | EXPLAIN / inspection scripts | Client for local Postgres |
| Docker | `PaymentFlowIntegrationTest` / Testcontainers | Optional; unit tests run without it |

Unit tests (no Docker required for this subset):

```bash
mvn -B -Dtest=ReconciliationServiceTest,AccountServiceBalanceCacheTest,DltReplayServiceTest,DltSupportTest,KafkaConsumerRetryPropertiesTest,PaymentStateTransitionTest,PaymentProcessingServiceTest,IdempotencyServiceTest,RequestFingerprintTest,PaymentSubmissionServiceTest test
```

Full local suite (when Docker is available for integration tests):

```bash
mvn -B test
```

## Confirmed verification (chunk-fix / M5 complete)

### Automated tests

| Suite | Result |
|-------|--------|
| `ReconciliationServiceTest` (focused) | **21** tests, **0** failures, **0** errors |
| Full local Maven suite | **86** tests, **0** failures, **0** errors, **0** skipped |
| Build | **BUILD SUCCESS** |

Chunk boundaries (empty / &lt;1 chunk / exact 1,000 / multi-chunk merge and findings) are covered by **unit tests**. There is **no** automated integration test that inserts &gt;65k payment rows solely to stress recon.

### Scenario F — reconciliation k6 (final chunk-fix run)

Local observation after chunked bulk fetch (`BULK_ID_CHUNK_SIZE = 1_000`):

| Metric | Value |
|--------|-------|
| Requests | 5 reconciliation runs |
| Checks | 10/10 passed |
| HTTP failure rate | 0% |
| Average response time | **6.10 s** |
| Median (p50) | **5.99 s** |
| p95 | **6.81 s** |
| Maximum | **6.94 s** |

**Prior failure (pre–chunk-fix):** with ~66,397 candidates in the lookback, a single bulk `IN` list exceeded PostgreSQL’s **65,535** bind-parameter limit (`Given query has 66,397 parameters`).

**Fix:** partition candidate IDs into batches of **1,000** for ledger and processed-event bulk loads; merge results in memory; keep existing `checkPayment` rules.

Recorded detail: [results/M5-chunk-fix-scenario-F.md](results/M5-chunk-fix-scenario-F.md).

### Database integrity after load / recon (local)

| Metric | Count |
|--------|------:|
| Payments | 66,397 |
| Completed | 63,751 |
| Failed | 2,646 |
| Pending | 0 |
| Ledger entries | 127,502 |
| Processed events | 66,397 |
| Completed with invalid ledger | 0 |
| Failed with ledger entries | 0 |
| Payments missing processed events | 0 |
| Completed same-account payments | 0 |

## Hot-account contention (Scenario C)

Use the same VUs/duration and compare modes:

```bash
k6 run -e MODE=disjoint    -e VUS=10 -e DURATION=30s perf/k6/c_hot_account.js
k6 run -e MODE=distributed -e VUS=10 -e DURATION=30s perf/k6/c_hot_account.js
k6 run -e MODE=same_source -e VUS=10 -e DURATION=30s perf/k6/c_hot_account.js
k6 run -e MODE=same_pair   -e VUS=10 -e DURATION=30s perf/k6/c_hot_account.js
```

**Expected behavior:** `same_source` / `same_pair` show **higher p95/p99** than `disjoint` because `PaymentProcessingService` takes ordered `SELECT … FOR UPDATE` locks on shared accounts. That is correctness-preserving serialization, not a measurement bug—**document the regression; do not hide it.**

Copy measured figures into `perf/results/` using the template; do not invent p95 values in this README.

## k6 scenarios (how to run)

```bash
export BASE_URL=http://localhost:8080
```

| Script | Measures | Example |
|--------|----------|---------|
| `perf/k6/a_submit.js` | A – submission latency/throughput | `k6 run -e VUS=1 -e DURATION=30s perf/k6/a_submit.js` |
| `perf/k6/b_normal_concurrent.js` | B – staged concurrent submit | `k6 run -e VUS=10 -e DURATION=60s perf/k6/b_normal_concurrent.js` |
| `perf/k6/c_hot_account.js` | C – lock contention modes | See above |
| `perf/k6/d_idempotency.js` | D – repeated same key (HTTP replay) | `k6 run -e VUS=10 -e DURATION=30s perf/k6/d_idempotency.js` |
| `perf/k6/e_mixed_workload.js` | E – mix of amounts | `k6 run -e VUS=10 -e FAIL_RATIO=0.2 -e DURATION=60s perf/k6/e_mixed_workload.js` |
| `perf/k6/f_reconciliation.js` | F – recon endpoint duration | `k6 run -e VUS=1 -e ITERATIONS=5 perf/k6/f_reconciliation.js` |

Suggested VU progression: **1 → 5 → 10 → 25 → 50 only if stable**.

### What scenarios do **not** prove alone

- **A–E** primarily measure `POST /payments` accept latency unless Kafka + DB status are measured separately.
- **D (idempotency)** demonstrates **HTTP-level** replay (same payment id / no extra accept path work as designed). It is **not** by itself a proof of exactly-once ledger posting; that relies on `processed_payment_events`, the processing transaction, and integration/unit tests.
- **F** measures recon API duration under the current lookback data volume on the local machine.

## EXPLAIN / inspection SQL

**Non-production DB only.**

```bash
export DB_URL_PSQL=postgresql://pll:pll@localhost:5432/payment_ledger
psql "$DB_URL_PSQL" -f perf/sql/01_explain_hot_paths.sql
psql "$DB_URL_PSQL" -f perf/sql/02_inspect_schema_and_stats.sql
```

Do not fabricate or commit EXPLAIN output as universal truth; plans depend on data volume and Postgres version.

## Recording results

1. Copy `perf/results/RESULT_TEMPLATE.md` or add a dated file under `perf/results/`.
2. Record environment, scenario, k6 summary, and interpretation.
3. Prefer facts over slogans (“faster”, “high volume”) without numbers.

## Limitations (honest)

- **Candidate selection** still loads all lookback payments into memory before chunked bulk fetches.
- Large lookbacks need **multiple bounded** ledger/processed queries (by design after the 65,535 fix).
- Benchmarks are **local-machine observations**, not production SLOs or capacity guarantees.
- Idempotency k6 results show **HTTP replay behavior**, not a full exactly-once ledger proof by themselves.
- No automated test seeds &gt;65k rows only for recon; parameter safety is enforced by **chunk size + unit tests**.

## Explicit non-goals of the M5 evidence pack

- No claim of production TPS or “% improvement” beyond measured local runs
- No OpenTelemetry / Grafana / Kubernetes as part of this folder’s scope
- No fabricated EXPLAIN plans or invented p95 figures

Milestone 5 implementation and verification are **complete**. Further optimization (indexes, partitioning, recon candidate streaming) would be a **new** milestone driven by new measurements—not unfinished M5 work.
