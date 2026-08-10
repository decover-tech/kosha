//! A posting (partition): a centroid plus the vectors currently assigned to
//! it. Mirrors SPANN/SPFresh's "posting list" — see README.md.
//!
//! **NPA (nearest partition assignment)**: the index property LIRE exists to
//! maintain — every live vector should live in the posting whose centroid is
//! nearest to it. A single insert/delete never breaks this by more than a
//! small, local margin; `Split`/`Merge`/`Reassign` (see `ops/`) are what
//! restore it cheaply instead of a full rebuild.

/// Slab index into `ClusterIndex::postings`. Stable across a posting's
/// lifetime; reused (via the free-list) once a posting is merged away. This
/// is what makes the paper's convergence bound (posting count is
/// monotonically non-decreasing across splits, and bounded above by the
/// total vector count) a literal property of the slab rather than something
/// argued separately — see `ClusterIndex::alloc_slot`.
pub(crate) type PostingId = usize;

/// One vector entry in a posting. `deleted`+`version` implement the paper's
/// tombstone-with-version-byte design (§4.1), simplified for a
/// single-threaded index: no CAS is needed since there's no concurrent
/// writer to race with (see README.md's simplifications list).
#[derive(Debug, Clone)]
pub(crate) struct PostingEntry {
    pub(crate) id: u32,
    pub(crate) vector: Vec<f32>,
    pub(crate) deleted: bool,
    pub(crate) version: u8,
}

#[derive(Debug, Clone)]
pub(crate) struct Posting {
    pub(crate) centroid: Vec<f32>,
    pub(crate) entries: Vec<PostingEntry>,
}

impl Posting {
    pub(crate) fn live_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.deleted).count()
    }

    pub(crate) fn live_entries(&self) -> impl Iterator<Item = &PostingEntry> {
        self.entries.iter().filter(|e| !e.deleted)
    }

    /// Physically drops tombstoned entries — the paper's designated GC
    /// point (performed at the start of every split, and explicitly via
    /// `ClusterIndex::rebalance`).
    pub(crate) fn gc(&mut self) {
        self.entries.retain(|e| !e.deleted);
    }
}
