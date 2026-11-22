-- Usage metering events per tenant and namespace (#348).
--
-- Additive: no existing table changes. Embedded in the server binary and
-- re-run on every startup after 001 (see kosha-control/src/postgres.rs), so
-- every statement is idempotent. `make db-migrate` also runs it as the RDS
-- admin before 002 hands ownership of the tables to the `kosha` role.
--
-- One row per sealed meter window, written by the pod that counted it
-- (`kosha-server/src/usage.rs`). `event_id` is deterministic (a hash of
-- the source pod, window, tenant, namespace and meter), so a flush retried
-- after an error or a timeout inserts each event at most once:
-- `INSERT ... ON CONFLICT (event_id) DO NOTHING`. Rows are never updated.
--
-- `quantity` is a count for the counter meters (write_bytes, write_docs,
-- queries, query_bytes_returned, query_bytes_scanned) and a sampled value
-- for the snapshot meters (storage_bytes, vector_dim_docs), whose sum over
-- hourly samples is a byte-hour (or dim-doc-hour) figure.
CREATE TABLE IF NOT EXISTS kosha.usage_events (
    event_id     TEXT PRIMARY KEY,         -- unique: makes appends idempotent
    tenant       TEXT NOT NULL,
    namespace    TEXT NOT NULL,            -- unqualified (no `<tenant>/` prefix)
    meter        TEXT NOT NULL,
    quantity     BIGINT NOT NULL CHECK (quantity >= 0),
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    source_pod   TEXT NOT NULL,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS usage_events_tenant_window
    ON kosha.usage_events (tenant, window_start);
