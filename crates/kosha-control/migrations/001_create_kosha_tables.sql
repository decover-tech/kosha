-- Kosha control plane tables.
-- These live in a dedicated `kosha` schema within the shared Decover RDS.
-- See DESIGN.md §6.3 (Manifest) and §5 (Control Plane).

CREATE SCHEMA IF NOT EXISTS kosha;

-- ── Namespace registry ────────────────────────────────────────────────────
-- One row per namespace.  Created on first write or explicit CreateNamespace.
CREATE TABLE IF NOT EXISTS kosha.namespaces (
    id          TEXT PRIMARY KEY,          -- e.g. "acme-corp/my-index"
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Manifest store ────────────────────────────────────────────────────────
-- Current segment manifest for each namespace.  Updated atomically on flush
-- and compaction.  The version field enables compare-and-swap semantics.
CREATE TABLE IF NOT EXISTS kosha.manifests (
    namespace_id    TEXT PRIMARY KEY REFERENCES kosha.namespaces(id),
    version         BIGINT NOT NULL DEFAULT 0,
    segments_json   TEXT NOT NULL DEFAULT '[]',  -- JSON array of ManifestEntry
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for listing recently-active namespaces.
CREATE INDEX IF NOT EXISTS idx_manifests_updated_at
    ON kosha.manifests (updated_at DESC);

-- ── API keys (customer/tenant auth) ─────────────────────────────────────────
-- Each customer gets one or more API keys that map to a tenant id.
-- The tenant id is used as a namespace prefix for isolation.
CREATE TABLE IF NOT EXISTS kosha.api_keys (
    api_key     TEXT PRIMARY KEY,          -- e.g. "sk-acme-corp-abc123"
    tenant_id   TEXT NOT NULL,             -- e.g. "acme-corp"
    description TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at  TIMESTAMPTZ               -- NULL = active
);

-- Index for looking up keys by tenant (admin listings).
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant
    ON kosha.api_keys (tenant_id);

-- ── Cross-replica segment hydration leases ──────────────────────────────────
-- One row per segment file currently being hydrated from S3 by some query
-- replica. See kosha-control's `hydration_lease` module (postgres feature)
-- for the claim/release protocol this backs, and kosha-server's
-- `ensure_segments_local`/`hydrate_files` for how it's used. `expires_at`
-- bounds how long a claim survives a replica that crashed or stalled
-- mid-fetch before another replica may take over.
CREATE TABLE IF NOT EXISTS kosha.hydration_leases (
    segment_key TEXT PRIMARY KEY,          -- relative path, e.g. "ns/seg-1/doc_store.bin"
    owner_addr  TEXT NOT NULL,             -- "host:port" of the owning replica
    claimed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);
