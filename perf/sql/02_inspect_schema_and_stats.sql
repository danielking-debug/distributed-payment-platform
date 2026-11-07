-- Database inspection for baseline evidence (non-production).
--   psql "$DB_URL" -f perf/sql/02_inspect_schema_and_stats.sql

\echo '=== version ==='
SELECT version();

\echo '=== table sizes ==='
SELECT relname AS table_name,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       pg_size_pretty(pg_relation_size(c.oid)) AS heap_size,
       n_live_tup AS approx_live_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE n.nspname = 'public' AND c.relkind = 'r'
ORDER BY pg_total_relation_size(c.oid) DESC;

\echo '=== row counts (exact; may be slow on large tables) ==='
SELECT 'accounts' AS t, COUNT(*) FROM accounts
UNION ALL SELECT 'payments', COUNT(*) FROM payments
UNION ALL SELECT 'ledger_entries', COUNT(*) FROM ledger_entries
UNION ALL SELECT 'outbox_events', COUNT(*) FROM outbox_events
UNION ALL SELECT 'processed_payment_events', COUNT(*) FROM processed_payment_events;

\echo '=== indexes ==='
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;

\echo '=== active connections ==='
SELECT count(*) AS total,
       count(*) FILTER (WHERE state = 'active') AS active,
       count(*) FILTER (WHERE state = 'idle') AS idle,
       count(*) FILTER (WHERE wait_event_type IS NOT NULL) AS waiting
FROM pg_stat_activity
WHERE datname = current_database();

\echo '=== lock snapshot (sample) ==='
SELECT locktype, relation::regclass, mode, granted, pid
FROM pg_locks
WHERE database = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY granted, relation
LIMIT 50;

\echo '=== payments by status ==='
SELECT status, COUNT(*) FROM payments GROUP BY status ORDER BY status;
