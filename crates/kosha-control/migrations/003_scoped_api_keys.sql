-- Scoped, expiring, hashed-at-rest API keys (#346).
--
-- Additive: existing rows keep working with full access. Embedded in the
-- server binary and re-run on every startup after 001 (see
-- kosha-control/src/postgres.rs), so every statement is idempotent.
-- `make db-migrate` also runs it as the RDS admin before 002.
--
-- New columns on kosha.api_keys:
--   key_hash      lowercase hex SHA-256 of the key. What the server matches
--                 a presented key against; also the key's public id.
--   scopes        comma-separated subset of read,write,admin. The default
--                 is all three, which is what every key could do before.
--   namespaces    NULL = every namespace of the tenant; otherwise the
--                 unqualified namespace names the key may touch.
--   expires_at    NULL = never expires.
--   last_used_at  written by the server about once per key refresh interval.
--
-- This migration does NOT remove plaintext from the `api_key` column of
-- existing rows: a server build from before #346 still reads that column,
-- and blanking it during a rolling deploy would lock tenants out of the old
-- pods. Once every pod runs a build with #346, remove the plaintext with
-- `POST /v1/admin/api-keys/scrub-plaintext`. Keys created by a #346 build
-- never store plaintext: their `api_key` column holds ' sha256:<key_hash>'
-- (leading space, so no request header can ever present it as a key).
--
-- The ALTER and CREATE INDEX are skipped when already applied, so a server
-- role that does not own the table can still start once an admin has run
-- this file, and startup takes no ACCESS EXCLUSIVE lock in the steady state.
DO $$
BEGIN
    IF (SELECT count(*) FROM information_schema.columns
        WHERE table_schema = 'kosha' AND table_name = 'api_keys'
          AND column_name IN ('key_hash', 'scopes', 'namespaces', 'expires_at', 'last_used_at')) < 5
    THEN
        ALTER TABLE kosha.api_keys
            ADD COLUMN IF NOT EXISTS key_hash     TEXT,
            ADD COLUMN IF NOT EXISTS scopes       TEXT NOT NULL DEFAULT 'read,write,admin',
            ADD COLUMN IF NOT EXISTS namespaces   TEXT[],
            ADD COLUMN IF NOT EXISTS expires_at   TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;
    END IF;
END
$$;

-- Backfill hashes for rows written before this migration, or by an older
-- server build that is still running during a rolling deploy.
UPDATE kosha.api_keys
   SET key_hash = encode(sha256(convert_to(api_key, 'UTF8')), 'hex')
 WHERE key_hash IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes
                   WHERE schemaname = 'kosha' AND indexname = 'idx_api_keys_key_hash')
    THEN
        CREATE UNIQUE INDEX idx_api_keys_key_hash ON kosha.api_keys (key_hash);
    END IF;
END
$$;
