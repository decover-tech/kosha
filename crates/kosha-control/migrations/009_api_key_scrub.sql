-- Automatic plaintext scrub of API keys (#346).
--
-- Additive and idempotent. Embedded in the server binary and re-run on
-- every startup (see kosha-control/src/postgres.rs); `make db-migrate` also
-- runs it as the RDS admin before 002.
--
-- Rows created before hashed keys (003_scoped_api_keys) still hold the key
-- itself in kosha.api_keys.api_key, because a server build from before
-- #346 matches that column and would lock tenants out mid-rollout if it
-- were blanked. Servers now remove that plaintext themselves once it is
-- safe (`PgStore::auto_scrub_plaintext`), and this one-row table is how
-- they agree on when that is:
--
--   hash_only_since  when the database last showed a sign of a pre-#346
--                    server: set by the first server of a build with the
--                    automatic scrub (not by this migration, so applying
--                    migrations ahead of a deploy starts no clock), and
--                    set again whenever a server finds a row without a
--                    key_hash, which only a pre-#346 build inserts. The
--                    scrub runs KOSHA_API_KEY_SCRUB_GRACE_SECS after it.
--   last_scrub_at,   when the automatic scrub last ran and how many rows
--   last_scrubbed    it rewrote (for operators; nothing reads them).
--
-- The single row is keyed by a constant TRUE so it cannot be duplicated.
CREATE TABLE IF NOT EXISTS kosha.api_key_scrub (
    id              BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    hash_only_since TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_scrub_at   TIMESTAMPTZ,
    last_scrubbed   BIGINT
);
