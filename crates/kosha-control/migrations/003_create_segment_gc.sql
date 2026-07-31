-- Orphaned S3 segment GC queue (DESIGN.md §6.3 / §7).
-- Segments drop out of the live manifest on replace/compact/migrate --replace;
-- they are marked here and deleted from S3 after a grace period.

CREATE TABLE IF NOT EXISTS kosha.segment_gc (
    namespace_id              TEXT NOT NULL,
    segment_id                TEXT NOT NULL,
    unreferenced_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    unreferenced_by_version   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace_id, segment_id)
);

CREATE INDEX IF NOT EXISTS idx_segment_gc_unreferenced_at
    ON kosha.segment_gc (unreferenced_at);
