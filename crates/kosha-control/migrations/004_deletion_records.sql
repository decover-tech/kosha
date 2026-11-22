-- Deletion records (#347): the durable trail of a namespace or tenant hard
-- delete, for GDPR / DPA erasure commitments.
--
-- Additive: no existing table changes, no existing row is touched. Embedded
-- in the server binary and re-run on every startup after 001 and 003 (see
-- kosha-control/src/postgres.rs), so every statement is idempotent.
-- `make db-migrate` also runs it as the RDS admin before 002 hands ownership
-- of the tables to the `kosha` role.
--
-- A row is inserted, `pending`, in the transaction that removes the
-- namespace's rows from kosha.namespaces / kosha.manifests. It becomes
-- `completed` only after every object under the subject's storage prefix was
-- deleted and a fresh listing of that prefix came back empty. A row that
-- stays `pending` is a delete whose storage reclaim failed or was
-- interrupted; `POST /v1/admin/deletions/resume` retries it.
--
-- Rows hold identifiers and counters only, never document content, and are
-- never deleted: they are the evidence that an erasure finished.
CREATE TABLE IF NOT EXISTS kosha.deletion_records (
    id              TEXT PRIMARY KEY,          -- "del-<uuid>"
    scope           TEXT NOT NULL CHECK (scope IN ('namespace', 'tenant')),
    subject         TEXT NOT NULL,             -- namespace id or tenant id
    tenant_id       TEXT,                      -- NULL for a pre-#262 bare namespace
    parent_id       TEXT,                      -- the tenant deletion this one is part of
    last_version    BIGINT NOT NULL DEFAULT 0, -- version the namespace retired at
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'completed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    objects_deleted BIGINT NOT NULL DEFAULT 0,
    last_error      TEXT,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

-- The resume sweep reads pending rows, oldest first.
CREATE INDEX IF NOT EXISTS idx_deletion_records_pending
    ON kosha.deletion_records (requested_at)
    WHERE state = 'pending';

-- Per-tenant listings (admin).
CREATE INDEX IF NOT EXISTS idx_deletion_records_tenant
    ON kosha.deletion_records (tenant_id, requested_at);
