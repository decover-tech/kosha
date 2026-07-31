-- 001 runs as the RDS admin (see 000_bootstrap_database.sql and Makefile's
-- db-migrate), so the schema/tables it creates end up owned by that admin —
-- not the `kosha` role the server actually connects as. But 001 is also
-- embedded into the server binary and re-run on every startup (see
-- kosha-control/src/postgres.rs), executed as `kosha`. A non-owner role
-- can't run privilege-changing statements on objects it doesn't own, so
-- ownership has to move to `kosha` itself rather than just granting it
-- access. Run once per RDS instance as admin, after 001. Safe to re-run.

ALTER SCHEMA kosha OWNER TO kosha;
ALTER TABLE kosha.namespaces OWNER TO kosha;
ALTER TABLE kosha.manifests OWNER TO kosha;
ALTER TABLE kosha.api_keys OWNER TO kosha;
ALTER TABLE IF EXISTS kosha.segment_gc OWNER TO kosha;
