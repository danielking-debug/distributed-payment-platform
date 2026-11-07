# Milestone 5 — Scenario F (reconciliation) after chunk-fix

**Do not invent numbers.** Values below are confirmed local observations from the chunk-fix verification.

## Environment notes

| Field | Value |
|-------|-------|
| Milestone | M5 complete (Slice A tooling + Slice B recon bulk/chunk + balance correctness) |
| Recon bulk ID chunk size | 1,000 |
| Prior failure | Single bulk query with 66,397 bind parameters exceeded PostgreSQL limit 65,535 |
| Automated &gt;65k-row recon IT | **Not present** — chunk boundaries covered by unit tests |

## Automated tests (same verification window)

| Suite | Result |
|-------|--------|
| `ReconciliationServiceTest` | 21 tests, 0 failures, 0 errors |
| Full local Maven suite | 86 tests, 0 failures, 0 errors, 0 skipped |
| Build | BUILD SUCCESS |

## Scenario F (k6)

| Field | Value |
|-------|-------|
| Script | `perf/k6/f_reconciliation.js` |
| Reconciliation requests | 5 |
| Checks passed | 10/10 |
| HTTP failure rate | 0% |
| Average response time | 6.10 s |
| Median (p50) | 5.99 s |
| p95 | 6.81 s |
| Maximum | 6.94 s |

## Final database integrity (local)

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

## Interpretation (facts)

- Chunked bulk fetch removed the hard failure at 65,535 parameters.
- Scenario F completed with zero HTTP failures under this local data volume.
- Integrity counts above are consistent with a clean completed/failed ledger and processed-event set for the observed population.
- Candidate selection still materializes the full lookback set in memory; only the **IN-list** path is chunked.
- Numbers are environment-specific; not a production capacity claim.

## Scenario C (hot account) — documentation note

`same_source` / `same_pair` modes are expected to show **higher p95** than `disjoint` due to ordered account row locks. Record measured p95 values from your runs in additional result files; do not omit the regression when comparing modes.
