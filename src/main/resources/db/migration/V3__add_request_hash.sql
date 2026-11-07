-- Request fingerprint for idempotency-key payload binding.
-- Survives Redis TTL/failure so the same key cannot be reused with a different payment body.
ALTER TABLE payments
    ADD COLUMN request_hash VARCHAR(64);

COMMENT ON COLUMN payments.request_hash IS
    'SHA-256 hex of fromAccountId|toAccountId|amount|currency for Idempotency-Key binding';
