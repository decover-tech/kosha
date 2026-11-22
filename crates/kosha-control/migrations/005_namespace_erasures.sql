-- Namespace erasure records (#347).
--
-- Additive: no existing table changes. Embedded in the server binary and
-- re-run on every startup after 001 and 003 (see kosha-control/src/postgres.rs),
-- so every statement is idempotent. `make db-migrate` also runs it as the
-- RDS admin before 002 hands ownership of the tables to the `kosha` role.
--
-- One row per completed hard delete (`DELETE /v1/namespaces/{ns}`): when
-- it was requested and finished, how many S3 objects went, and whether a
-- listing afterwards verified the namespace's prefixes empty. Rows outlive
-- the namespace on purpose: they are the evidence of the erasure. They
-- hold no document data, only the namespace and tenant names.
CREATE TABLE IF NOT EXISTS kosha.namespace_erasures (
    id                 TEXT PRIMARY KEY,         -- "er_…", the delete's erasure_id
    namespace          TEXT NOT NULL,            -- name as the tenant addressed it
    tenant             TEXT NOT NULL,
    requested_at       TIMESTAMPTZ NOT NULL,
    completed_at       TIMESTAMPTZ NOT NULL,
    s3_objects_deleted BIGINT NOT NULL DEFAULT 0,
    verified_empty     BOOLEAN NOT NULL,
    local_reclaimed    BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS namespace_erasures_tenant_ns
    ON kosha.namespace_erasures (tenant, namespace, completed_at DESC);
