-- Usage metering events per tenant and namespace (#348).
--
-- Additive: no existing table changes. Embedded in the server binary and
-- re-run on every startup after 001 and 003 (see
-- kosha-control/src/postgres.rs), so every statement is idempotent.
-- `make db-migrate` also runs it as the RDS admin before 002 hands ownership
-- of the tables to the `kosha` role.
--
-- One row per (event, meter). Rows are only ever inserted, with
-- ON CONFLICT DO NOTHING on the primary key: a pod that resends a batch
-- after a crash changes nothing, so the table can be summed without
-- deduplication. Summaries are computed from these rows on read; nothing
-- here is a running total.
--
-- `meter` is free text (queries, bytes_returned, bytes_fetched,
-- bytes_written, documents_written, stored_bytes, ...), so a new meter needs
-- no migration. `stored_bytes` is a gauge: one row per namespace per hour,
-- with a deterministic event id (`stored/<tenant>/<namespace>/<hour>`).
CREATE TABLE IF NOT EXISTS kosha.usage_events (
    event_id       TEXT NOT NULL,
    meter          TEXT NOT NULL,
    tenant_id      TEXT NOT NULL,
    namespace      TEXT NOT NULL,          -- tenant-relative name; shards roll up to their root
    quantity       BIGINT NOT NULL,
    occurred_at_ms BIGINT NOT NULL,        -- Unix milliseconds, as stamped by the emitting pod
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, meter)
);

-- Every read is a time window, for one tenant or for all of them.
CREATE INDEX IF NOT EXISTS idx_usage_events_tenant_time
    ON kosha.usage_events (tenant_id, occurred_at_ms);
CREATE INDEX IF NOT EXISTS idx_usage_events_time
    ON kosha.usage_events (occurred_at_ms);
