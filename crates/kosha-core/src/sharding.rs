//! Namespace sharding (#207): a namespace created with a fixed shard count
//! `N > 1` keeps no segments of its own. Every document is routed by id to
//! one of `N` shard namespaces, each an ordinary namespace with its own
//! manifest, segment set, WAL checkpoint and compaction — so flush,
//! publish, S3 sync, compaction and manifest following need no special
//! case. Reads scatter over the shards and gather into one result.
//!
//! Layout, on disk, in S3 and in the control store:
//!
//! ```text
//! {root}                 manifest with `shards: Some(N)`, no segments
//! {root}/_shard-0000     shard 0's manifest and segments
//! ...
//! {root}/_shard-{N-1}
//! ```
//!
//! Shard namespaces nest under the root's prefix, so a shard segment's
//! path relative to the root is `_shard-XXXX/{segment_id}`. That is what
//! lets [`merge_shard_manifests`] present all shards as one manifest whose
//! entries use those relative ids: every reader resolves a segment as
//! `{data_dir or S3 prefix}/{namespace}/{segment_id}`, so the merged view
//! reads exactly the files the shard wrote, and the searcher scores the
//! union with namespace-wide BM25 statistics, one top-K, one `search_after`
//! order and one capped total-hit count — the same result as one shard
//! holding the same documents.
//!
//! A user namespace name cannot contain `/` (the server rejects it), so a
//! shard namespace can never collide with one a client names.

use std::collections::HashMap;
use std::sync::Arc;

use crate::{Manifest, ManifestEntry, NamespaceId, SegmentId};

/// Largest shard count a namespace can be created with.
pub const MAX_SHARDS: u32 = 256;

/// Path component a shard namespace adds under its root.
const SHARD_COMPONENT_PREFIX: &str = "_shard-";

/// The `/`-joined name component of shard `index`, e.g. `_shard-0003`.
pub fn shard_component(index: u32) -> String {
    format!("{SHARD_COMPONENT_PREFIX}{index:04}")
}

/// The namespace holding shard `index` of `root`.
pub fn shard_namespace(root: &NamespaceId, index: u32) -> NamespaceId {
    NamespaceId(format!("{}/{}", root.0, shard_component(index)))
}

/// Every shard namespace of `root`, in shard order.
pub fn shard_namespaces(root: &NamespaceId, shards: u32) -> Vec<NamespaceId> {
    (0..shards).map(|i| shard_namespace(root, i)).collect()
}

/// `Some(index)` when `ns` is a shard namespace (`.../_shard-XXXX`).
pub fn shard_index_of(ns: &NamespaceId) -> Option<u32> {
    let (_, last) = ns.0.rsplit_once('/')?;
    parse_component(last)
}

fn parse_component(component: &str) -> Option<u32> {
    let digits = component.strip_prefix(SHARD_COMPONENT_PREFIX)?;
    (digits.len() == 4 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

/// 64-bit FNV-1a. Fixed here rather than borrowed from `std` because
/// routing must never change across Rust releases, platforms or restarts:
/// a document's shard is where its earlier versions live.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Jump consistent hash (Lamping & Veach, 2014). Growing the bucket count
/// from `n` to `n + 1` moves only ~1/(n+1) of the keys, which keeps a later
/// manual reshard cheap.
fn jump_consistent_hash(mut key: u64, buckets: u32) -> u32 {
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < i64::from(buckets) {
        b = j;
        key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        j = ((b + 1) as f64 * ((1u64 << 31) as f64 / ((key >> 33) + 1) as f64)) as i64;
    }
    b as u32
}

/// The shard document `doc_id` lives in, for a namespace of `shards`
/// shards. Stable: depends only on the id bytes and the shard count.
pub fn shard_for_document(doc_id: &str, shards: u32) -> u32 {
    if shards <= 1 {
        return 0;
    }
    jump_consistent_hash(fnv1a64(doc_id.as_bytes()), shards)
}

/// The merged-view id of shard `index`'s segment `segment`.
pub fn merged_segment_id(index: u32, segment: &SegmentId) -> SegmentId {
    SegmentId(format!("{}/{}", shard_component(index), segment.0))
}

/// Inverse of [`merged_segment_id`].
pub fn split_merged_segment_id(segment: &SegmentId) -> Option<(u32, SegmentId)> {
    let (component, rest) = segment.0.split_once('/')?;
    Some((parse_component(component)?, SegmentId(rest.to_string())))
}

/// One read-only manifest over every shard of `root` (see the module
/// docs). `shards[i]` is shard `i`'s current manifest, `None` when that
/// shard has never been written. The merged version is the root's version
/// plus every shard's: it moves whenever any of them publishes, which is
/// what the version-keyed caches (hydration verdicts, result cache) need.
pub fn merge_shard_manifests(root: &Manifest, shards: &[Option<Arc<Manifest>>]) -> Manifest {
    let segment_count = shards.iter().flatten().map(|m| m.segments.len()).sum();
    let mut merged = Manifest {
        version: root.version,
        segments: Vec::with_capacity(segment_count),
        segment_footers: HashMap::new(),
        footer_crc32: HashMap::new(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: HashMap::new(),
        deletion_vectors: HashMap::new(),
        schema: root.schema.clone(),
        shards: root.shards,
    };
    for (index, shard) in shards.iter().enumerate() {
        let Some(shard) = shard else { continue };
        let index = index as u32;
        merged.version += shard.version;
        let rekey = |segment: &SegmentId| merged_segment_id(index, segment);
        for entry in &shard.segments {
            merged.segments.push(ManifestEntry {
                segment_id: rekey(&entry.segment_id),
                doc_count: entry.doc_count,
            });
        }
        // Footers keep their own `segment_id` (the shard-local one): only
        // the map key changes, so this shares the parsed footer, blooms
        // and all, instead of copying it.
        for (segment, footer) in &shard.segment_footers {
            merged
                .segment_footers
                .insert(rekey(segment), Arc::clone(footer));
        }
        for (segment, crc) in &shard.footer_crc32 {
            merged.footer_crc32.insert(rekey(segment), *crc);
        }
        for (segment, ids) in &shard.tombstones {
            merged.tombstones.insert(rekey(segment), ids.clone());
        }
        for (segment, vector) in &shard.deletion_vectors {
            merged
                .deletion_vectors
                .insert(rekey(segment), vector.clone());
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_is_stable_and_spreads_documents() {
        // Pinned values: changing the hash would strand every existing
        // document in a shard its id no longer routes to.
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        let pinned: Vec<u32> = ["doc-0", "doc-1", "doc-2", "doc-3", "doc-4"]
            .iter()
            .map(|id| shard_for_document(id, 8))
            .collect();
        assert_eq!(pinned, vec![0, 6, 4, 2, 2]);
        let mut counts = [0usize; 8];
        for i in 0..8000 {
            let shard = shard_for_document(&format!("doc-{i}"), 8);
            assert!(shard < 8);
            counts[shard as usize] += 1;
        }
        for count in counts {
            assert!((700..1300).contains(&count), "unbalanced: {counts:?}");
        }
        assert_eq!(shard_for_document("anything", 1), 0);
        assert_eq!(shard_for_document("anything", 0), 0);
    }

    #[test]
    fn jump_hash_moves_few_keys_when_growing() {
        let moved = (0..10_000)
            .filter(|i| {
                let id = format!("k{i}");
                shard_for_document(&id, 4) != shard_for_document(&id, 5)
            })
            .count();
        // Ideal is 1/5 of the keys.
        assert!((1500..2500).contains(&moved), "moved {moved}");
    }

    #[test]
    fn shard_names_round_trip() {
        let root = NamespaceId("acme/docs".into());
        let shard = shard_namespace(&root, 3);
        assert_eq!(shard.0, "acme/docs/_shard-0003");
        assert_eq!(shard_index_of(&shard), Some(3));
        assert_eq!(shard_index_of(&root), None);
        assert_eq!(shard_index_of(&NamespaceId("x/_shard-12".into())), None);
        let seg = SegmentId("acme_docs-abc-000001".into());
        let merged = merged_segment_id(3, &seg);
        assert_eq!(merged.0, "_shard-0003/acme_docs-abc-000001");
        assert_eq!(split_merged_segment_id(&merged), Some((3, seg.clone())));
        assert_eq!(split_merged_segment_id(&seg), None);
    }

    #[test]
    fn merge_rekeys_everything_and_sums_versions() {
        let root = Manifest {
            version: 1,
            segments: vec![],
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
            shards: Some(2),
        };
        let mut shard = root.clone();
        shard.shards = None;
        shard.version = 5;
        shard.segments = vec![ManifestEntry {
            segment_id: SegmentId("s".into()),
            doc_count: 3,
        }];
        shard
            .tombstones
            .insert(SegmentId("s".into()), [1u32].into_iter().collect());
        shard.footer_crc32.insert(SegmentId("s".into()), 9);
        let merged = merge_shard_manifests(&root, &[None, Some(Arc::new(shard))]);
        assert_eq!(merged.version, 6);
        assert_eq!(merged.shards, Some(2));
        assert_eq!(merged.segments.len(), 1);
        let id = SegmentId("_shard-0001/s".into());
        assert_eq!(merged.segments[0].segment_id, id);
        assert!(merged.tombstones[&id].contains(&1));
        assert_eq!(merged.footer_crc32[&id], 9);
    }
}
