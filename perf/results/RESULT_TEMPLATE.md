# Benchmark result template

**Do not invent numbers.** Fill this in after a real run.

Milestone 5 is complete; see `perf/README.md` and `perf/results/M5-chunk-fix-scenario-F.md` for confirmed Scenario F / integrity figures. Preserve measured same_source p95 regressions when recording Scenario C.

## Environment

| Field | Value |
|-------|-------|
| Date (UTC) | |
| Operator | |
| Machine / OS | |
| CPU / RAM | |
| Java version | `java -version` |
| PostgreSQL version | `SELECT version();` |
| Kafka available? | yes / no |
| Redis available? | yes / no |
| App base URL | |
| Hikari maximum-pool-size | |
| Kafka consumer concurrency | (default 3 for ledger-processor) |
| Notes | |

## Scenario

| Field | Value |
|-------|-------|
| Script | e.g. `perf/k6/c_hot_account.js` |
| MODE (if any) | disjoint / distributed / same_source / same_pair |
| VUs | |
| Duration / iterations | |
| Think time | |

## k6 summary (copy from k6 output)

| Metric | Value |
|--------|-------|
| http_reqs | |
| http_req_duration p50 | |
| http_req_duration p95 | |
| http_req_duration p99 | |
| http_req_failed rate | |
| checks pass rate | |

## Processing (if Kafka running; measure separately)

| Metric | Value | How measured |
|--------|-------|--------------|
| Payments submitted | | count in DB or k6 |
| Payments COMPLETED | | `SELECT count(*) FROM payments WHERE status='COMPLETED'` |
| Payments FAILED | | |
| Approx processing throughput | | completed / wall time |
| Consumer lag | | Kafka tooling if available |

## Reconciliation (if scenario F)

| Metric | Value |
|--------|-------|
| Run duration (k6 http_req_duration) | |
| paymentsScanned | from response body |
| findingCount | from response body |

## EXPLAIN notes

Link or paste key lines from `EXPLAIN (ANALYZE, BUFFERS)` for relevant queries (optional attachment under `perf/results/`).

## Interpretation (facts only)

- What was stable?
- What degraded (e.g. same_source vs disjoint)?
- Any errors? root cause if known?
