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
