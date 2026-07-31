//! Mark-and-sweep GC for orphaned S3 segments (DESIGN.md §6.3 / §7).
//!
//! Writers mark segment IDs that leave the live manifest. After a grace
//! period, a sweeper deletes the S3 prefix iff the ID is still absent from
//! the current Postgres/in-memory manifest. A reconcile pass lists S3 and
//! marks any unreferenced prefixes the mark path missed (e.g. migrate debt).

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kosha_core::{ControlStore, NamespaceId, SegmentId};
use serde::Serialize;

#[cfg(feature = "s3")]
use crate::s3_storage::S3Storage;

/// Default grace before deleting an unreferenced segment (24h).
pub const DEFAULT_GRACE_SECS: u64 = 24 * 60 * 60;

/// Default background sweep interval (1h). `0` disables the loop.
pub const DEFAULT_INTERVAL_SECS: u64 = 60 * 60;

#[derive(Debug, Clone, Default, Serialize)]
pub struct GcReport {
    pub marked: usize,
    pub deleted_segments: usize,
    pub deleted_objects: usize,
    pub skipped_live: usize,
    pub errors: Vec<String>,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct GcOptions {
    pub grace_secs: u64,
    pub namespace: Option<NamespaceId>,
    pub reconcile: bool,
    pub dry_run: bool,
}

impl Default for GcOptions {
    fn default() -> Self {
        Self {
            grace_secs: grace_secs_from_env(),
            namespace: None,
            reconcile: false,
            dry_run: false,
        }
    }
}

pub fn grace_secs_from_env() -> u64 {
    std::env::var("KOSHA_SEGMENT_GC_GRACE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_GRACE_SECS)
}

pub fn interval_secs_from_env() -> u64 {
    std::env::var("KOSHA_SEGMENT_GC_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_INTERVAL_SECS)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Diff previous vs current manifest segment sets and mark dropped IDs for GC.
pub fn mark_dropped_segments(
    store: &mut dyn ControlStore,
    namespace: &NamespaceId,
    previous: Option<&kosha_core::Manifest>,
    current: &kosha_core::Manifest,
) -> Result<usize, kosha_core::KoshaError> {
    let Some(prev) = previous else {
        return Ok(0);
    };
    let live: HashSet<&str> = current
        .segments
        .iter()
        .map(|e| e.segment_id.0.as_str())
        .collect();
    let dropped: Vec<SegmentId> = prev
        .segments
        .iter()
        .filter(|e| !live.contains(e.segment_id.0.as_str()))
        .map(|e| e.segment_id.clone())
        .collect();
    let count = dropped.len();
    if count > 0 {
        store.mark_segments_for_gc(namespace, &dropped, current.version)?;
    }
    Ok(count)
}

/// Sweep due GC marks (and optionally reconcile S3 orphans into the mark table).
#[cfg(feature = "s3")]
pub fn run_gc(
    store: &mut dyn ControlStore,
    s3: Option<&S3Storage>,
    options: &GcOptions,
) -> GcReport {
    let mut report = GcReport {
        dry_run: options.dry_run,
        ..GcReport::default()
    };

    if options.reconcile {
        if let Some(s3) = s3 {
            match reconcile_orphans(store, s3, options, &mut report) {
                Ok(n) => report.marked += n,
                Err(e) => report.errors.push(format!("reconcile: {e}")),
            }
        } else {
            report
                .errors
                .push("reconcile requested but S3 storage is not configured".into());
        }
    }

    let cutoff = now_unix().saturating_sub(options.grace_secs as i64);
    let candidates = match store.list_gc_candidates(cutoff) {
        Ok(c) => c,
        Err(e) => {
            report.errors.push(format!("list_gc_candidates: {e}"));
            return report;
        }
    };

    for entry in candidates {
        if let Some(ref only) = options.namespace {
            if &entry.namespace_id != only {
                continue;
            }
        }

        let live = store
            .manifest_cloned(&entry.namespace_id)
            .map(|m| {
                m.segments
                    .iter()
                    .any(|s| s.segment_id == entry.segment_id)
            })
            .unwrap_or(false);
        if live {
            report.skipped_live += 1;
            if let Err(e) = store.clear_gc_mark(&entry.namespace_id, &entry.segment_id) {
                report
                    .errors
                    .push(format!("clear live mark {}: {e}", entry.segment_id.0));
            }
            continue;
        }

        if options.dry_run {
            report.deleted_segments += 1;
            continue;
        }

        if let Some(s3) = s3 {
            match s3.delete_segment_prefix(&entry.namespace_id.0, &entry.segment_id.0) {
                Ok(objects) => {
                    report.deleted_segments += 1;
                    report.deleted_objects += objects;
                    if let Err(e) = store.clear_gc_mark(&entry.namespace_id, &entry.segment_id) {
                        report.errors.push(format!(
                            "clear mark after delete {}: {e}",
                            entry.segment_id.0
                        ));
                    }
                }
                Err(e) => report.errors.push(format!(
                    "delete {}/{}: {e}",
                    entry.namespace_id.0, entry.segment_id.0
                )),
            }
        } else {
            // Local-only / no S3: clear the mark so the queue does not grow forever.
            report.deleted_segments += 1;
            if let Err(e) = store.clear_gc_mark(&entry.namespace_id, &entry.segment_id) {
                report
                    .errors
                    .push(format!("clear mark {}: {e}", entry.segment_id.0));
            }
        }
    }

    report
}

#[cfg(not(feature = "s3"))]
pub fn run_gc(
    store: &mut dyn ControlStore,
    _s3: Option<&()>,
    options: &GcOptions,
) -> GcReport {
    let mut report = GcReport {
        dry_run: options.dry_run,
        ..GcReport::default()
    };
    let cutoff = now_unix().saturating_sub(options.grace_secs as i64);
    let candidates = match store.list_gc_candidates(cutoff) {
        Ok(c) => c,
        Err(e) => {
            report.errors.push(format!("list_gc_candidates: {e}"));
            return report;
        }
    };
    for entry in candidates {
        if let Some(ref only) = options.namespace {
            if &entry.namespace_id != only {
                continue;
            }
        }
        let live = store
            .manifest_cloned(&entry.namespace_id)
            .map(|m| {
                m.segments
                    .iter()
                    .any(|s| s.segment_id == entry.segment_id)
            })
            .unwrap_or(false);
        if live {
            report.skipped_live += 1;
            let _ = store.clear_gc_mark(&entry.namespace_id, &entry.segment_id);
            continue;
        }
        report.deleted_segments += 1;
        if !options.dry_run {
            let _ = store.clear_gc_mark(&entry.namespace_id, &entry.segment_id);
        }
    }
    report
}

#[cfg(feature = "s3")]
fn reconcile_orphans(
    store: &mut dyn ControlStore,
    s3: &S3Storage,
    options: &GcOptions,
    _report: &mut GcReport,
) -> Result<usize, String> {
    let namespaces: Vec<NamespaceId> = if let Some(ref ns) = options.namespace {
        vec![ns.clone()]
    } else {
        store.list_namespaces()
    };

    let cutoff = now_unix().saturating_sub(options.grace_secs as i64);
    let mut marked = 0usize;

    for ns in namespaces {
        let live: HashSet<String> = store
            .manifest_cloned(&ns)
            .map(|m| m.segments.iter().map(|e| e.segment_id.0.clone()).collect())
            .unwrap_or_default();
        let remote = s3
            .list_segment_ids(&ns.0)
            .map_err(|e| format!("list {}: {e}", ns.0))?;

        let mut to_mark = Vec::new();
        for (segment_id, newest_unix) in remote {
            if live.contains(&segment_id) {
                continue;
            }
            // Only enqueue orphans that look settled (newer than grace would
            // risk racing an in-flight upload-before-manifest publish).
            if newest_unix > 0 && newest_unix > cutoff {
                continue;
            }
            to_mark.push(SegmentId(segment_id));
        }
        if to_mark.is_empty() {
            continue;
        }
        if options.dry_run {
            marked += to_mark.len();
            continue;
        }
        let version = store.manifest_cloned(&ns).map(|m| m.version).unwrap_or(0);
        store
            .mark_segments_for_gc(&ns, &to_mark, version)
            .map_err(|e| e.to_string())?;
        marked += to_mark.len();
    }
    Ok(marked)
}

/// Background loop: sweep on an interval. Interval `0` exits immediately.
pub fn spawn_background_loop<F>(interval_secs: u64, mut tick: F)
where
    F: FnMut() + Send + 'static,
{
    if interval_secs == 0 {
        println!("segment GC: background loop disabled (KOSHA_SEGMENT_GC_INTERVAL_SECS=0)");
        return;
    }
    println!(
        "segment GC: background loop every {interval_secs}s grace={}s",
        grace_secs_from_env()
    );
    std::thread::Builder::new()
        .name("kosha-segment-gc".into())
        .spawn(move || loop {
            std::thread::sleep(Duration::from_secs(interval_secs));
            tick();
        })
        .ok();
}

/// Helper used by the server background tick and admin handler.
pub fn run_gc_locked(
    controller: &Mutex<Box<dyn ControlStore>>,
    #[cfg(feature = "s3")] s3: Option<&S3Storage>,
    #[cfg(not(feature = "s3"))] s3: Option<&()>,
    options: &GcOptions,
) -> GcReport {
    let mut store = controller.lock().unwrap();
    run_gc(store.as_mut(), s3, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_control::Controller;
    use kosha_core::{Manifest, ManifestEntry};

    fn manifest(version: u64, segs: &[&str]) -> Manifest {
        Manifest {
            version,
            segments: segs
                .iter()
                .map(|id| ManifestEntry {
                    segment_id: SegmentId((*id).into()),
                    doc_count: 1,
                })
                .collect(),
        }
    }

    #[test]
    fn mark_dropped_only_marks_removed_ids() {
        let mut store = Controller::new();
        let ns = NamespaceId("ns".into());
        let prev = manifest(1, &["a", "b"]);
        let curr = manifest(2, &["b", "c"]);
        let n = mark_dropped_segments(&mut store, &ns, Some(&prev), &curr).unwrap();
        assert_eq!(n, 1);
        let due = store.list_gc_candidates(i64::MAX).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].segment_id.0, "a");
    }

    #[test]
    fn sweep_skips_ids_that_became_live_again() {
        let mut store = Controller::new();
        let ns = NamespaceId("ns".into());
        store
            .mark_segments_for_gc(&ns, &[SegmentId("zombie".into())], 1)
            .unwrap();
        store
            .save_manifest(
                &ns,
                &manifest(2, &["zombie"]),
            )
            .unwrap();

        let report = run_gc(
            &mut store,
            None,
            &GcOptions {
                grace_secs: 0,
                namespace: None,
                reconcile: false,
                dry_run: false,
            },
        );
        assert_eq!(report.skipped_live, 1);
        assert_eq!(report.deleted_segments, 0);
        assert!(store.list_gc_candidates(i64::MAX).unwrap().is_empty());
    }

    #[test]
    fn sweep_clears_due_marks_without_s3() {
        let mut store = Controller::new();
        let ns = NamespaceId("ns".into());
        store
            .mark_segments_for_gc(&ns, &[SegmentId("old".into())], 1)
            .unwrap();
        store.save_manifest(&ns, &manifest(2, &["new"])).unwrap();

        let report = run_gc(
            &mut store,
            None,
            &GcOptions {
                grace_secs: 0,
                ..GcOptions::default()
            },
        );
        assert_eq!(report.deleted_segments, 1);
        assert!(store.list_gc_candidates(i64::MAX).unwrap().is_empty());
    }
}
