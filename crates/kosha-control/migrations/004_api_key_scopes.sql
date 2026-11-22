-- Scoped, expiring, hashed API keys (#346).
--
-- Additive: only new columns and indexes on kosha.api_keys. Embedded in the
-- server binary and re-run on every startup after 003 (see
-- kosha-control/src/postgres.rs), so every statement is idempotent. `make
-- db-migrate` also runs it as the RDS admin before 002 hands ownership of
-- the tables to the `kosha` role.
--
-- Keys are looked up by `key_hash` (lowercase hex SHA-256 of the key) only.
-- The legacy `api_key` column stays the primary key but no longer holds a
-- usable key: new rows store `redacted:<prefix>:<id>` there, and at startup
-- the server hashes every row whose `key_hash` is NULL (keys created before
-- this migration) and overwrites its plaintext the same way
-- (`kosha_control::api_keys::pg::backfill_key_hashes`).
--
-- `gen_random_uuid()` is built in from Postgres 13.

ALTER TABLE kosha.api_keys ADD COLUMN IF NOT EXISTS id UUID DEFAULT gen_random_uuid();
UPDATE kosha.api_keys SET id = gen_random_uuid() WHERE id IS NULL;
ALTER TABLE kosha.api_keys ALTER COLUMN id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_id ON kosha.api_keys (id);

ALTER TABLE kosha.api_keys ADD COLUMN IF NOT EXISTS key_hash TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_key_hash ON kosha.api_keys (key_hash);

ALTER TABLE kosha.api_keys ADD COLUMN IF NOT EXISTS key_prefix TEXT NOT NULL DEFAULT '';
-- Every key created before #346 could do everything in its tenant.
ALTER TABLE kosha.api_keys
    ADD COLUMN IF NOT EXISTS scopes TEXT[] NOT NULL DEFAULT '{read,write,admin}';
-- NULL: every namespace of the tenant.
ALTER TABLE kosha.api_keys ADD COLUMN IF NOT EXISTS namespaces TEXT[];
-- NULL: never expires.
ALTER TABLE kosha.api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE kosha.api_keys ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;
-- The key this one replaced (`POST /v1/admin/api-keys/{id}/rotate`).
ALTER TABLE kosha.api_keys ADD COLUMN IF NOT EXISTS rotated_from UUID;
