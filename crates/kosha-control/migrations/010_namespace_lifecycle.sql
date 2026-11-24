-- Namespace lifecycle, part 2 (#347): deletion records for branches and
-- migrated namespaces, per-pod acknowledgement of cache purges, exports.
--
-- Additive: no existing row changes meaning. Embedded in the server binary
-- and re-run on every startup after 007 (see kosha-control/src/postgres.rs),
-- so every statement is idempotent. `make db-migrate` also runs it as the
-- RDS admin before 002 hands ownership of the tables to the `kosha` role.
-- The ALTERs and CREATE INDEXes on kosha.deletion_records are skipped once
-- applied, so a server role that does not own that table can still start
-- after an admin ran this file, and startup takes no ACCESS EXCLUSIVE lock
-- in the steady state.
--
-- Rolling deploy: a pod running an older build cannot decode a record with
-- scope 'branch' or 'migrated'. Its purge pass and admin listings report an
-- error until it is upgraded; nothing is deleted on its account.

-- ── kosha.deletion_records ────────────────────────────────────────────────
--   scope 'branch'     a branch deleted on its own (DELETE .../branch)
--   scope 'migrated'   the bare source id of `kosha-server migrate-namespace`
--   acks_required      the record completes only once every live purge pod
--                      acknowledged (KOSHA_DELETION_REQUIRE_ACKS)
--   storage_verified   storage reclaim finished and was verified; with
--                      acks_required the record stays pending until then
--   exports_deleted    exports of the subject deleted with it
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'kosha' AND t.relname = 'deletion_records'
          AND c.conname = 'deletion_records_scope_check'
          AND pg_get_constraintdef(c.oid) LIKE '%migrated%')
    THEN
        ALTER TABLE kosha.deletion_records
            DROP CONSTRAINT IF EXISTS deletion_records_scope_check;
        ALTER TABLE kosha.deletion_records
            ADD CONSTRAINT deletion_records_scope_check
            CHECK (scope IN ('namespace', 'tenant', 'branch', 'migrated'));
    END IF;

    IF (SELECT count(*) FROM information_schema.columns
        WHERE table_schema = 'kosha' AND table_name = 'deletion_records'
          AND column_name IN ('acks_required', 'storage_verified', 'exports_deleted')) < 3
    THEN
        ALTER TABLE kosha.deletion_records
            ADD COLUMN IF NOT EXISTS acks_required    BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS storage_verified BOOLEAN NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS exports_deleted  BIGINT NOT NULL DEFAULT 0;
    END IF;

    -- Every pod's purge pass pages through all records in this order
    -- (ControlStore::deletions_after). Both functions are immutable, so the
    -- expression can be indexed; the old millisecond expression on a
    -- timestamptz could not, and every pass sorted the whole table.
    IF NOT EXISTS (SELECT 1 FROM pg_indexes
                   WHERE schemaname = 'kosha' AND indexname = 'idx_deletion_records_position')
    THEN
        CREATE INDEX idx_deletion_records_position ON kosha.deletion_records
            ((date_trunc('milliseconds', requested_at AT TIME ZONE 'UTC')), id);
    END IF;
END
$$;

-- ── Pod registry ──────────────────────────────────────────────────────────
-- One row per pod that runs the deletion purge (KOSHA_DELETION_PURGE_INTERVAL_MS),
-- refreshed on every purge pass. A pod is live while its heartbeat is newer
-- than KOSHA_POD_LIVENESS_MS; a record with acks_required waits for every
-- live pod's acknowledgement. Rows of pods that went away just stop being
-- live.
CREATE TABLE IF NOT EXISTS kosha.pods (
    pod_id            TEXT PRIMARY KEY,
    role              TEXT NOT NULL,
    purge_enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- A pod's word that it purged its cached copies of a deletion's subject.
-- Identifiers only; never deleted, like the records themselves.
CREATE TABLE IF NOT EXISTS kosha.deletion_acks (
    deletion_id TEXT NOT NULL REFERENCES kosha.deletion_records (id),
    pod_id      TEXT NOT NULL,
    acked_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (deletion_id, pod_id)
);

-- ── Exports ───────────────────────────────────────────────────────────────
-- One row per export job (POST /v1/namespaces/{ns}/exports). `pins` holds
-- the manifests the export reads (JSON) while it is pending or running:
-- segment GC and the local GC keep what they reference. `upload_url` is the
-- customer's presigned PUT URL, kept only until the delivery ends and never
-- returned by the API. Rows go when the namespace or tenant is deleted.
CREATE TABLE IF NOT EXISTS kosha.exports (
    id              TEXT PRIMARY KEY,               -- "exp-<uuid>"
    tenant_id       TEXT NOT NULL,
    namespace       TEXT NOT NULL,                  -- internal id <tenant>/<name>
    state           TEXT NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'running', 'completed', 'failed', 'expired')),
    format          TEXT NOT NULL DEFAULT 'jsonl.gz',
    include_vectors BOOLEAN NOT NULL DEFAULT TRUE,
    versions        TEXT NOT NULL DEFAULT '[]',     -- JSON [[namespace, version], ...]
    pins            TEXT,                           -- JSON; NULL once inactive
    parts           TEXT NOT NULL DEFAULT '[]',     -- JSON [{path, bytes, documents}]
    documents       BIGINT NOT NULL DEFAULT 0,
    bytes           BIGINT NOT NULL DEFAULT 0,
    error           TEXT,
    upload_url      TEXT,
    delivery_host   TEXT,
    delivery_state  TEXT CHECK (delivery_state IN ('pending', 'delivered', 'failed')),
    delivery_error  TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    claimed_by      TEXT,
    claimed_at      TIMESTAMPTZ,
    ttl_ms          BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_exports_tenant_namespace
    ON kosha.exports (tenant_id, namespace, created_at);

-- Segment GC reads the pins of active exports on every run.
CREATE INDEX IF NOT EXISTS idx_exports_active
    ON kosha.exports (created_at)
    WHERE state IN ('pending', 'running');
