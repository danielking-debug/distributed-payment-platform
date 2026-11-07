-- M5 Slice A: EXPLAIN scripts for hot-path and recon queries.
-- Run against a NON-PRODUCTION PostgreSQL database only.
-- Do not commit EXPLAIN output; record results under perf/results/ if desired.
--
-- Usage example:
--   psql "$DB_URL" -f perf/sql/01_explain_hot_paths.sql
--
-- Replace sample UUIDs with real ids from your DB when needed.

\echo '=== PostgreSQL version ==='
SELECT version();

\echo '=== Q: payment by idempotency_key ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM payments
WHERE idempotency_key = 'example-idempotency-key-not-found';

\echo '=== Q: account by id (planner shape; FOR UPDATE separately below) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM accounts
WHERE id = 'a257995f-df03-5e89-9b00-2939712931d3';

\echo '=== Q: account FOR UPDATE (run inside a transaction) ==='
BEGIN;
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM accounts
WHERE id = 'a257995f-df03-5e89-9b00-2939712931d3'
FOR UPDATE;
ROLLBACK;

\echo '=== Q: outbox unpublished batch (matches OutboxEventRepository) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM outbox_events
WHERE published_at IS NULL
ORDER BY created_at ASC
LIMIT 200
FOR UPDATE SKIP LOCKED;

\echo '=== Q: reconciliation lookback (created_at >= now - 24h) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM payments
WHERE created_at >= NOW() - INTERVAL '24 hours';

\echo '=== Q: stuck PENDING (status + created_at) ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM payments
WHERE status = 'PENDING'
  AND created_at <= NOW() - INTERVAL '30 minutes';

\echo '=== Q: ledger entries by payment_id ==='
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM ledger_entries
WHERE payment_id = '00000000-0000-0000-0000-000000000001';
