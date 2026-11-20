-- Writer lease and fencing token per namespace (#254).
--
-- Additive: no existing table changes. Embedded in the server binary and
-- re-run on every startup after 001 (see kosha-control/src/postgres.rs), so
-- every statement is idempotent. `make db-migrate` also runs it as the RDS
-- admin before 002 hands ownership of the tables to the `kosha` role.
--
-- One row per namespace that ever had a leased writer. `token` only grows:
-- a takeover by a new holder (after the old lease expired) sets it to the
-- old value + 1. Rows are never deleted; a released lease just expires and
-- keeps its token for the next holder to exceed. Every manifest CAS made
-- by a leased writer reads this row FOR SHARE in its own transaction and is
-- rejected when the token differs from the one the writer presents.
CREATE TABLE IF NOT EXISTS kosha.writer_leases (
    namespace_id TEXT PRIMARY KEY,
    holder       TEXT NOT NULL,          -- "<host>/<pid>/<nonce>" of the writer process
    token        BIGINT NOT NULL,
    expires_at   TIMESTAMPTZ NOT NULL
);
