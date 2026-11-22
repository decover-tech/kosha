//! Purging a deleted namespace's cached copies on every pod (#347).
//!
//! `DELETE /v1/namespaces/{ns}` and `DELETE /v1/admin/tenants/{tenant}` run
//! on one pod. They remove the control-plane rows, that pod's own state and
//! the objects in storage, and leave a deletion record
//! ([`kosha_core::deletion`]). Every other pod still holds what it cached:
//! segment files (and byte-range spans) under its data directory, parsed
//! segments, corpus statistics, result pages. This module is the part each
//! pod runs for itself. It is opt-in: `KOSHA_DELETION_PURGE_INTERVAL_MS`.
//!
//! **What a pass does.** It reads the deletion records it has not handled
//! yet (all of them on the first pass after boot, so a pod that was down
//! during a delete catches up), and for each record's subject:
//!
//! 1. drops everything this process caches in memory for the subject and
//!    the namespaces below it;
//! 2. removes the subject's segment directories from the local disk,
//!    except the ones the subject's *current* incarnation references.
//!
//! **Re-creation.** A namespace re-created under the same name writes into
//! the same directory, so the directory cannot simply be removed. Segment
//! ids are never reused across incarnations (flushes, upserts and
//! compactions all name segments with a fresh UUID), so the incarnations
//! are told apart by segment: a directory is kept when a manifest that is
//! current says so, and removed otherwise. "Current" is decided with the
//! retired version the control store kept for the deleted incarnation
//! (`DeletionRecord::last_version`; a new incarnation starts above it):
//!
//! - every segment a manifest in the control store lists for the namespace
//!   (or for a branch that shares its segments) is kept;
//! - what this process holds for the namespace is read under the writer's
//!   locks (`Indexer::while_quiesced`), so no flush, upsert or compaction
//!   is half way. Its segments are kept when its manifest is above the
//!   retired version and the control store has the namespace. A writer
//!   holding unpublished work for a namespace that looks deleted keeps its
//!   files too, and the sweep is repeated later;
//! - a directory written to within [`PurgeTuning::write_margin`] of the
//!   start of the sweep is left for the next sweep. A segment published
//!   after the control store was read can only have reached this disk
//!   after the sweep started, so this is what keeps it;
//! - a directory with a pinned or in-flight fetch is left for the next
//!   sweep.
//!
//! **What "completed" on a record means.** Unchanged: the control store,
//! the pod that ran the delete and object storage. A record carries no
//! per-pod acknowledgement, because the control store has no list of pods
//! to wait for. What holds instead, for a pod with the purge enabled that
//! can read the control store:
//!
//! - a record is handled by the first pass that starts after the record's
//!   transaction commits, so within one interval plus the length of a pass;
//! - every subject is swept a second time [`PurgeTuning::settle`] later
//!   (again while anything was left or failed), which removes what a search
//!   or fetch already running during the first sweep wrote afterwards;
//! - a pod that was down handles every record in its first pass after boot.
//!
//! Not covered: a pod with the purge off (the default); parsed segments a
//! search already running still holds, until that search ends; a pod that
//! cannot read the control store, until it can; freed disk blocks and the
//! operating system's page cache. `GET /v1/stats` reports each pod's
//! progress under `deletion_purge`.
//!
//! **Retrying pending records.** `KOSHA_DELETION_RESUME_INTERVAL_MS`
//! (opt-in, writer pods only) retries the storage reclaim of pending
//! *namespace* records at boot and on a timer, with a growing pause per
//! record. Tenant records are not retried automatically: a pending tenant
//! record deletes whatever the tenant id holds at the time it runs.

use super::*;
use kosha_core::deletion::{
    DeletionFilter, DeletionPosition, DeletionRecord, DeletionScope, DeletionState,
};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

/// How far behind the newest record it has seen a pass re-reads. A record
/// is stamped when its transaction starts and visible when it commits, so
/// this must exceed the longest a delete's transaction can stay open.
const REREAD_WINDOW_MS: u64 = 10 * 60 * 1000;

/// A record younger than this (by this pod's clock) gets its second sweep
/// even when the first found nothing to wait for: something already
/// running may still write. Older records (a boot catch-up) do not, so a
/// long history does not queue one sweep per record.
const RECENT_RECORD_MS: u64 = 60 * 60 * 1000;

/// The longest pause between automatic retries of one pending record.
const RESUME_MAX_BACKOFF: Duration = Duration::from_secs(3600);

/// `KOSHA_DELETION_PURGE_INTERVAL_MS` and `KOSHA_DELETION_RESUME_INTERVAL_MS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct PurgeSettings {
    /// How often this pod purges the caches of deleted namespaces; `None`
    /// when off.
    pub interval: Option<Duration>,
    /// How often a writer pod retries pending namespace deletions; `None`
    /// when off.
    pub resume_interval: Option<Duration>,
}

impl PurgeSettings {
    /// `Err` is the message to refuse to start with.
    pub(crate) fn from_env() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).ok();
        Self::parse(
            var("KOSHA_DELETION_PURGE_INTERVAL_MS").as_deref(),
            var("KOSHA_DELETION_RESUME_INTERVAL_MS").as_deref(),
        )
    }

    pub(crate) fn parse(purge: Option<&str>, resume: Option<&str>) -> Result<Self, String> {
        Ok(Self {
            interval: parse_interval("KOSHA_DELETION_PURGE_INTERVAL_MS", purge)?,
            resume_interval: parse_interval("KOSHA_DELETION_RESUME_INTERVAL_MS", resume)?,
        })
    }
}

/// Unset, empty or `0` is off. Anything that is not a whole number of
/// milliseconds is an error: a typo must not silently leave erasure off.
fn parse_interval(name: &str, raw: Option<&str>) -> Result<Option<Duration>, String> {
    match raw.map(str::trim) {
        None | Some("") => Ok(None),
        Some(raw) => match raw.parse::<u64>() {
            Ok(0) => Ok(None),
            Ok(ms) => Ok(Some(Duration::from_millis(ms))),
            Err(_) => Err(format!(
                "{name}={raw:?} is not a number of milliseconds (0 or unset turns it off)"
            )),
        },
    }
}

/// The parts of a pass that are fixed in production and shortened in tests.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PurgeTuning {
    /// How long after a sweep its subject is swept again.
    pub settle: Duration,
    /// A directory written to this close to (or after) the start of a
    /// sweep is not removed by it.
    pub write_margin: Duration,
    /// Records read per control-store query.
    pub page: u32,
}

impl Default for PurgeTuning {
    fn default() -> Self {
        Self {
            settle: Duration::from_secs(60),
            write_margin: Duration::from_secs(5),
            page: 500,
        }
    }
}

/// A subject waiting for another sweep.
#[derive(Debug, Clone)]
struct Resweep {
    scope: DeletionScope,
    subject: String,
    /// The highest retired version among the subject's records.
    floor: Option<u64>,
    due: Instant,
}

#[derive(Default)]
struct Progress {
    /// The newest `requested_at_ms` among handled records.
    watermark_ms: Option<u64>,
    /// Handled record ids inside the re-read window, with their stamp.
    seen: HashMap<String, u64>,
    queue: Vec<Resweep>,
    /// Whether a pass has read the record list to its end.
    caught_up: bool,
    last_pass_at_ms: Option<u64>,
    last_error: Option<String>,
    /// Pending records and when each may be retried next.
    resume_backoff: HashMap<String, (Instant, Duration)>,
}

/// One pod's purge bookkeeping and counters.
#[derive(Default)]
pub(crate) struct PurgeState {
    /// One pass at a time.
    pass: Mutex<()>,
    progress: Mutex<Progress>,
    interval_ms: AtomicU64,
    passes: AtomicU64,
    records_handled: AtomicU64,
    sweeps: AtomicU64,
    segment_dirs_removed: AtomicU64,
    bytes_removed: AtomicU64,
    parsed_segments_dropped: AtomicU64,
    left_for_later: AtomicU64,
    errors: AtomicU64,
    resume_attempts: AtomicU64,
    resume_completed: AtomicU64,
}

impl PurgeState {
    /// The `deletion_purge` block of `GET /v1/stats`.
    pub(crate) fn to_json(&self) -> serde_json::Value {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let progress = self.progress.lock().unwrap();
        let interval = load(&self.interval_ms);
        serde_json::json!({
            "enabled": interval > 0,
            "interval_ms": (interval > 0).then_some(interval),
            "caught_up": progress.caught_up,
            "last_pass_at_ms": progress.last_pass_at_ms,
            "watermark_ms": progress.watermark_ms,
            "pending_sweeps": progress.queue.len(),
            "last_error": progress.last_error,
            "kosha_deletion_purge_passes_total": load(&self.passes),
            "kosha_deletion_purge_records_total": load(&self.records_handled),
            "kosha_deletion_purge_sweeps_total": load(&self.sweeps),
            "kosha_deletion_purge_segment_dirs_removed_total": load(&self.segment_dirs_removed),
            "kosha_deletion_purge_bytes_removed_total": load(&self.bytes_removed),
            "kosha_deletion_purge_parsed_segments_dropped_total":
                load(&self.parsed_segments_dropped),
            "kosha_deletion_purge_deferred_total": load(&self.left_for_later),
            "kosha_deletion_purge_errors_total": load(&self.errors),
            "kosha_deletion_resume_attempts_total": load(&self.resume_attempts),
            "kosha_deletion_resume_completed_total": load(&self.resume_completed),
        })
    }
}

/// What one sweep of one subject did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct SweepOutcome {
    pub segment_dirs_removed: u64,
    pub bytes_removed: u64,
    pub parsed_segments_dropped: u64,
    /// Directories (or whole namespaces) left for a later sweep: written to
    /// too recently, pinned, being fetched, or held by unpublished writer
    /// state.
    pub deferred: u64,
    pub error: Option<String>,
}

impl SweepOutcome {
    fn unfinished(&self) -> bool {
        self.deferred > 0 || self.error.is_some()
    }

    fn fail(&mut self, error: impl std::fmt::Display) {
        self.error.get_or_insert(error.to_string());
    }
}

/// What one pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct PassReport {
    /// Records handled for the first time.
    pub records: u64,
    /// Subjects swept, first and repeat sweeps together.
    pub sweeps: u64,
    pub segment_dirs_removed: u64,
    pub deferred: u64,
    pub errors: Vec<String>,
}

/// The segment directories (`{home namespace}/{segment}`, relative to the
/// data directory) that current manifests reference.
#[derive(Default)]
struct Keep {
    dirs: HashSet<String>,
}

impl Keep {
    fn note(&mut self, ns: &NamespaceId, manifest: &kosha_core::Manifest) {
        for entry in &manifest.segments {
            self.dirs
                .insert(manifest.segment_rel_dir(ns, &entry.segment_id));
        }
        // Retired segments stay until the local GC decides no search can
        // still read them (#239).
        for retired in &manifest.retired {
            self.dirs
                .insert(format!("{}/{}", ns.0, retired.segment_id.0));
        }
    }
}

fn first_component(id: &str) -> &str {
    id.split_once('/').map_or(id, |(first, _)| first)
}

fn modified_since(path: &std::path::Path, cutoff: SystemTime) -> bool {
    // Unreadable metadata counts as recent: leave the directory alone.
    match path.metadata().and_then(|meta| meta.modified()) {
        Ok(modified) => modified >= cutoff,
        Err(_) => true,
    }
}

impl AppState {
    /// A pod that follows manifests published elsewhere and never writes.
    fn follows_only(&self) -> bool {
        self.role == role::Role::Query || self.ingest_host.is_some()
    }

    /// Start the purge thread (and, on a writer pod, the retry thread) when
    /// configured. The first pass runs at once: that is the boot catch-up.
    pub(crate) fn spawn_deletion_purge(self: &Arc<Self>, settings: PurgeSettings) {
        match settings.interval {
            None => println!(
                "deletion purge disabled (KOSHA_DELETION_PURGE_INTERVAL_MS unset or 0): cached \
                 copies of deleted namespaces leave this pod only by eviction"
            ),
            Some(interval) => {
                println!("deletion purge: every {} ms", interval.as_millis());
                self.deletion_purge
                    .interval_ms
                    .store(interval.as_millis() as u64, Ordering::Relaxed);
                let weak = Arc::downgrade(self);
                std::thread::Builder::new()
                    .name("kosha-deletion-purge".into())
                    .spawn(move || loop {
                        let Some(state) = weak.upgrade() else {
                            return;
                        };
                        state.run_deletion_purge_pass(&PurgeTuning::default());
                        drop(state);
                        std::thread::sleep(interval);
                    })
                    .expect("failed to spawn deletion purge thread");
            }
        }
        let Some(interval) = settings.resume_interval else {
            return;
        };
        if self.follows_only() || self.role == role::Role::Indexer {
            println!(
                "KOSHA_DELETION_RESUME_INTERVAL_MS is ignored on a {} pod: only a writer pod \
                 retries pending deletions",
                self.role
            );
            return;
        }
        println!(
            "pending namespace deletions are retried every {} ms",
            interval.as_millis()
        );
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("kosha-deletion-resume".into())
            .spawn(move || loop {
                let Some(state) = weak.upgrade() else {
                    return;
                };
                state.run_deletion_resume_pass(interval);
                drop(state);
                std::thread::sleep(interval);
            })
            .expect("failed to spawn deletion resume thread");
    }

    /// One purge pass: handle the deletion records this pod has not handled
    /// yet, then repeat the sweeps that are due. See the module doc.
    pub(crate) fn run_deletion_purge_pass(&self, tuning: &PurgeTuning) -> PassReport {
        let purge = &self.deletion_purge;
        let _one = purge.pass.lock().unwrap();
        let mut report = PassReport::default();
        let now_ms = unix_millis();

        // ── Read what is new ───────────────────────────────────────────
        let mut cursor = purge
            .progress
            .lock()
            .unwrap()
            .watermark_ms
            .map(|newest| DeletionPosition::at_ms(newest.saturating_sub(REREAD_WINDOW_MS)));
        let mut fresh: Vec<DeletionRecord> = Vec::new();
        let mut read_to_end = false;
        loop {
            let page = self
                .controller
                .lock()
                .unwrap()
                .deletions_after(cursor.as_ref(), tuning.page);
            match page {
                Ok(page) => {
                    let full = page.len() >= tuning.page.max(1) as usize;
                    if let Some(last) = page.last() {
                        cursor = Some(last.position());
                    }
                    let progress = purge.progress.lock().unwrap();
                    fresh.extend(
                        page.into_iter()
                            .filter(|record| !progress.seen.contains_key(&record.id)),
                    );
                    if !full {
                        read_to_end = true;
                        break;
                    }
                }
                Err(e) => {
                    report.errors.push(format!("reading deletion records: {e}"));
                    break;
                }
            }
        }

        // ── First sweep of every new subject ───────────────────────────
        // Several records can name one subject (deleted, re-created,
        // deleted again): one sweep covers them, against the highest
        // retired version.
        let mut subjects: BTreeMap<(bool, String), Option<u64>> = BTreeMap::new();
        for record in &fresh {
            let tenant = record.scope == DeletionScope::Tenant;
            let floor = subjects
                .entry((tenant, record.subject.clone()))
                .or_default();
            if !tenant {
                *floor = Some(floor.unwrap_or(0).max(record.last_version));
            }
        }
        let recent = fresh
            .iter()
            .filter(|record| record.requested_at_ms.saturating_add(RECENT_RECORD_MS) >= now_ms)
            .map(|record| record.subject.as_str())
            .collect::<HashSet<_>>();
        let mut requeue: Vec<Resweep> = Vec::new();
        for ((tenant, subject), floor) in &subjects {
            let scope = if *tenant {
                DeletionScope::Tenant
            } else {
                DeletionScope::Namespace
            };
            let outcome = self.sweep_deleted_subject(scope, subject, *floor, tuning);
            self.note_sweep(&outcome, subject, &mut report);
            if outcome.unfinished() || recent.contains(subject.as_str()) {
                requeue.push(Resweep {
                    scope,
                    subject: subject.clone(),
                    floor: *floor,
                    due: Instant::now() + tuning.settle,
                });
            }
        }

        // ── Repeat sweeps that are due ─────────────────────────────────
        let due: Vec<Resweep> = {
            let mut progress = purge.progress.lock().unwrap();
            let now = Instant::now();
            // A subject swept just now waits for its new turn instead.
            progress.queue.retain(|queued| {
                !requeue
                    .iter()
                    .any(|new| new.scope == queued.scope && new.subject == queued.subject)
            });
            let (due, later) = std::mem::take(&mut progress.queue)
                .into_iter()
                .partition(|queued| queued.due <= now);
            progress.queue = later;
            due
        };
        for queued in due {
            let outcome =
                self.sweep_deleted_subject(queued.scope, &queued.subject, queued.floor, tuning);
            self.note_sweep(&outcome, &queued.subject, &mut report);
            if outcome.unfinished() {
                requeue.push(Resweep {
                    due: Instant::now() + tuning.settle,
                    ..queued
                });
            }
        }

        // ── Remember ───────────────────────────────────────────────────
        report.records = fresh.len() as u64;
        purge
            .records_handled
            .fetch_add(report.records, Ordering::Relaxed);
        purge.passes.fetch_add(1, Ordering::Relaxed);
        purge
            .errors
            .fetch_add(report.errors.len() as u64, Ordering::Relaxed);
        let mut progress = purge.progress.lock().unwrap();
        for record in &fresh {
            progress
                .seen
                .insert(record.id.clone(), record.requested_at_ms);
            progress.watermark_ms = Some(
                progress
                    .watermark_ms
                    .unwrap_or(0)
                    .max(record.requested_at_ms),
            );
        }
        if let Some(newest) = progress.watermark_ms {
            let oldest_reread = newest.saturating_sub(REREAD_WINDOW_MS);
            progress.seen.retain(|_, stamp| *stamp >= oldest_reread);
        }
        progress.queue.extend(requeue);
        progress.caught_up |= read_to_end;
        progress.last_pass_at_ms = Some(now_ms);
        progress.last_error = report.errors.first().cloned();
        report
    }

    fn note_sweep(&self, outcome: &SweepOutcome, subject: &str, report: &mut PassReport) {
        let purge = &self.deletion_purge;
        purge.sweeps.fetch_add(1, Ordering::Relaxed);
        purge
            .segment_dirs_removed
            .fetch_add(outcome.segment_dirs_removed, Ordering::Relaxed);
        purge
            .bytes_removed
            .fetch_add(outcome.bytes_removed, Ordering::Relaxed);
        purge
            .parsed_segments_dropped
            .fetch_add(outcome.parsed_segments_dropped, Ordering::Relaxed);
        purge
            .left_for_later
            .fetch_add(outcome.deferred, Ordering::Relaxed);
        report.sweeps += 1;
        report.segment_dirs_removed += outcome.segment_dirs_removed;
        report.deferred += outcome.deferred;
        if let Some(error) = &outcome.error {
            eprintln!("WARN: deletion purge of {subject}: {error}; it is retried");
            report.errors.push(format!("{subject}: {error}"));
        }
        if outcome.segment_dirs_removed > 0 || outcome.parsed_segments_dropped > 0 {
            println!(
                "deletion purge: subject={subject} segment_dirs_removed={} bytes_removed={} \
                 parsed_segments_dropped={} deferred={}",
                outcome.segment_dirs_removed,
                outcome.bytes_removed,
                outcome.parsed_segments_dropped,
                outcome.deferred
            );
        }
    }

    /// Drop what this process caches in memory for `subject` and every
    /// namespace below it. Safe for a namespace that exists again: all of
    /// it is rebuilt from the manifest and the files on the next search.
    fn purge_deleted_from_memory(&self, subject: &str) -> u64 {
        let below = format!("{subject}/");
        let matches = |ns: &str| ns == subject || ns.starts_with(&below);
        let dropped = self.searcher.forget_namespaces(matches) as u64;
        let mut names: HashSet<String> = HashSet::from([subject.to_string()]);
        names.extend(
            self.indexer
                .namespaces()
                .into_iter()
                .map(|ns| ns.0)
                .filter(|ns| matches(ns)),
        );
        self.completed_hydrations
            .lock()
            .unwrap()
            .retain(|(ns, _), _| {
                if matches(ns) {
                    names.insert(ns.clone());
                }
                !matches(ns)
            });
        self.posting_blob_presence
            .lock()
            .unwrap()
            .retain(|ns, _| !matches(ns));
        self.shard_views.lock().unwrap().retain(|ns, _| {
            if matches(&ns.0) {
                names.insert(ns.0.clone());
            }
            !matches(&ns.0)
        });
        {
            let mut results = self.result_cache.lock().unwrap();
            let cached: HashSet<String> = results
                .entries
                .keys()
                .filter(|(ns, _, _)| matches(ns))
                .map(|(ns, _, _)| ns.clone())
                .collect();
            for ns in &cached {
                results.forget_namespace(ns);
            }
        }
        // The unindexed tail (#255) and its transient segment.
        for ns in names {
            self.drop_cached_tail(&NamespaceId(ns));
        }
        dropped
    }

    /// Sweep one deleted subject: memory, then disk. `floor` is the highest
    /// retired version among a namespace's deletion records.
    pub(crate) fn sweep_deleted_subject(
        &self,
        scope: DeletionScope,
        subject: &str,
        floor: Option<u64>,
        tuning: &PurgeTuning,
    ) -> SweepOutcome {
        let mut outcome = SweepOutcome::default();
        let trimmed = subject.trim_matches('/');
        // Subjects come from the control store; one that is not a plain
        // relative path names nothing this pod could have cached.
        let subject_dir = match self.cache.path_for(trimmed) {
            Ok(dir) if !trimmed.is_empty() && trimmed == subject => dir,
            _ => return outcome,
        };
        // The cutoff is taken before anything is read: whatever was
        // published after the reads below reached this disk after it.
        let cutoff = SystemTime::now()
            .checked_sub(tuning.write_margin)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        outcome.parsed_segments_dropped = self.purge_deleted_from_memory(subject);
        if !subject_dir.is_dir() {
            return outcome;
        }

        let stored: HashMap<NamespaceId, u64> = {
            let listed = self.controller.lock().unwrap().manifest_versions();
            match listed {
                Ok(listed) => listed.into_iter().collect(),
                Err(e) => {
                    outcome.fail(format!("control store: {e}"));
                    return outcome;
                }
            }
        };
        let below = format!("{subject}/");
        let in_subject = |ns: &str| ns == subject || ns.starts_with(&below);
        // Segments under the subject can only be referenced while a
        // namespace exists there again (a delete is refused while a branch
        // shares the namespace's segments). Then every manifest of the
        // tenant is read: a branch can list a segment its parent dropped.
        let mut keep = Keep::default();
        if stored.keys().any(|ns| in_subject(&ns.0)) {
            let tenant = first_component(subject);
            let tenant_below = format!("{tenant}/");
            for ns in stored
                .keys()
                .filter(|ns| ns.0 == tenant || ns.0.starts_with(&tenant_below))
            {
                let manifest = self.controller.lock().unwrap().try_manifest_cloned(ns);
                match manifest {
                    Ok(Some(manifest)) => keep.note(ns, &manifest),
                    Ok(None) => {}
                    Err(e) => {
                        outcome.fail(format!("control store: reading '{}': {e}", ns.0));
                        return outcome;
                    }
                }
            }
        }

        let mut namespaces: Vec<NamespaceId> = Vec::new();
        match scope {
            DeletionScope::Namespace => namespaces.push(NamespaceId(subject.to_string())),
            DeletionScope::Tenant => {
                // A pre-#262 namespace named like the tenant keeps its
                // segments directly under `{tenant}/`, where this would read
                // them as namespaces. The tenant delete refuses that case;
                // so does the purge.
                let bare = NamespaceId(subject.to_string());
                if stored.contains_key(&bare) || self.indexer.manifest_version(&bare).is_some() {
                    outcome.fail(format!(
                        "an unscoped namespace named '{subject}' shares the tenant's directory; \
                         its files are not swept"
                    ));
                    return outcome;
                }
                match std::fs::read_dir(&subject_dir) {
                    Ok(entries) => {
                        for entry in entries.flatten() {
                            let Some(name) = entry.file_name().to_str().map(String::from) else {
                                continue;
                            };
                            if entry.path().is_dir() {
                                namespaces.push(NamespaceId(format!("{subject}/{name}")));
                            }
                        }
                    }
                    Err(e) => outcome.fail(format!("reading {subject}/: {e}")),
                }
            }
        }
        // Shards (#207) are namespaces of their own, nested under their
        // root; each is swept alone, before the root.
        let mut ordered: Vec<NamespaceId> = Vec::new();
        for ns in namespaces {
            if let Ok(entries) = std::fs::read_dir(self.data_dir.join(&ns.0)) {
                for entry in entries.flatten() {
                    let Some(name) = entry.file_name().to_str().map(String::from) else {
                        continue;
                    };
                    let shard = NamespaceId(format!("{}/{name}", ns.0));
                    if entry.path().is_dir()
                        && kosha_core::sharding::shard_index_of(&shard).is_some()
                    {
                        ordered.push(shard);
                    }
                }
            }
            ordered.push(ns);
        }
        for ns in &ordered {
            // A record's retired version is the subject's own; a shard or a
            // tenant's namespace is judged by whether the store has it.
            let floor = floor.filter(|_| ns.0 == subject);
            self.sweep_namespace_dir(ns, floor, &stored, &keep, cutoff, &mut outcome);
        }
        // Whatever is empty now goes (never a directory with content).
        for ns in &ordered {
            let _ = std::fs::remove_dir(self.data_dir.join(&ns.0));
        }
        let _ = std::fs::remove_dir(&subject_dir);
        if outcome.segment_dirs_removed > 0 {
            // A hydration verdict recorded while the files went must not
            // outlive them.
            self.completed_hydrations
                .lock()
                .unwrap()
                .retain(|(ns, _), _| !in_subject(ns));
        }
        outcome
    }

    /// Remove the segment directories of `ns` that no current manifest
    /// references. Runs under the writer's locks for `ns`.
    fn sweep_namespace_dir(
        &self,
        ns: &NamespaceId,
        floor: Option<u64>,
        stored: &HashMap<NamespaceId, u64>,
        keep: &Keep,
        cutoff: SystemTime,
        outcome: &mut SweepOutcome,
    ) {
        let ns_dir = self.data_dir.join(&ns.0);
        let in_store = stored.contains_key(ns);
        let follower = self.follows_only();
        self.indexer.while_quiesced(ns, |local| {
            let mut live = in_store;
            let mut local_keep = Keep::default();
            let local_version = local.as_ref().map(|local| local.manifest.version);
            if let Some(local) = &local {
                // What this process holds is the deleted incarnation's when
                // its version is at or below the retired one, or when the
                // store no longer has the namespace.
                let deleted =
                    floor.is_some_and(|floor| local.manifest.version <= floor) || !in_store;
                if !deleted {
                    local_keep.note(ns, &local.manifest);
                } else if !follower && local.unpublished {
                    // A writer with unpublished work: it may be writes that
                    // arrived after the delete and have not registered the
                    // namespace again yet. Keep its files and come back.
                    local_keep.note(ns, &local.manifest);
                    live = true;
                    outcome.deferred += 1;
                }
            }
            let entries = match std::fs::read_dir(&ns_dir) {
                Ok(entries) => entries,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
                Err(e) => {
                    outcome.fail(format!("reading {}/: {e}", ns.0));
                    return;
                }
            };
            // Never a directory that is, or holds, a namespace the store
            // or this writer knows.
            let known_here = self.indexer.namespaces();
            let nested_below = |rel: &str| {
                let below = format!("{rel}/");
                stored
                    .keys()
                    .chain(known_here.iter())
                    .any(|other| other.0 == rel || other.0.starts_with(&below))
            };
            for entry in entries.flatten() {
                let Some(name) = entry.file_name().to_str().map(String::from) else {
                    continue;
                };
                let rel = format!("{}/{name}", ns.0);
                let path = entry.path();
                if !path.is_dir() {
                    // A stray file next to the segments of a namespace that
                    // is gone.
                    if !live {
                        if let Err(e) = self.cache.evict(&rel) {
                            outcome.fail(format!("removing {rel}: {e}"));
                        }
                    }
                    continue;
                }
                if kosha_core::sharding::shard_index_of(&NamespaceId(rel.clone())).is_some()
                    || keep.dirs.contains(&rel)
                    || local_keep.dirs.contains(&rel)
                    || nested_below(&rel)
                {
                    continue;
                }
                // A strong search may be building the namespace's unindexed
                // tail (#255) right now; stale ones are removed at boot.
                if live && name.starts_with(kosha_core::TAIL_SEGMENT_PREFIX) {
                    continue;
                }
                if modified_since(&path, cutoff) {
                    outcome.deferred += 1;
                    continue;
                }
                #[cfg(feature = "s3")]
                if self.in_flight_segments.lock().unwrap().contains_key(&path) {
                    outcome.deferred += 1;
                    continue;
                }
                // A follower can install a newer manifest while this runs;
                // then what it lists is no longer what was read above.
                if self.indexer.manifest_version(ns) != local_version {
                    outcome.deferred += 1;
                    return;
                }
                match self.cache.evict_dir_unless_pinned(&rel) {
                    Ok(Some(bytes)) => {
                        outcome.segment_dirs_removed += 1;
                        outcome.bytes_removed += bytes;
                        #[cfg(feature = "s3")]
                        self.synced_segments
                            .lock()
                            .unwrap()
                            .remove(&(ns.clone(), kosha_core::SegmentId(name)));
                    }
                    // Pinned by a fetch or a search in flight.
                    Ok(None) => outcome.deferred += 1,
                    Err(e) => outcome.fail(format!("removing {rel}: {e}")),
                }
            }
        });
    }

    /// Retry the storage reclaim of pending namespace deletions (writer
    /// pods). Each record that stays pending waits twice as long before its
    /// next try, from `interval` up to an hour, so records that cannot
    /// complete (a namespace that exists again) do not cost a control-store
    /// round trip per tick. Returns how many records completed.
    pub(crate) fn run_deletion_resume_pass(&self, interval: Duration) -> usize {
        let purge = &self.deletion_purge;
        let pending = self
            .controller
            .lock()
            .unwrap()
            .deletions(&DeletionFilter::pending());
        let pending = match pending {
            Ok(pending) => pending,
            Err(e) => {
                eprintln!("WARN: deletion resume: reading pending records: {e}");
                return 0;
            }
        };
        let now = Instant::now();
        let listed: HashSet<&str> = pending.iter().map(|record| record.id.as_str()).collect();
        purge
            .progress
            .lock()
            .unwrap()
            .resume_backoff
            .retain(|id, _| listed.contains(id.as_str()));
        let mut completed = 0;
        for record in pending
            .iter()
            .filter(|record| record.scope == DeletionScope::Namespace)
        {
            let waiting = purge
                .progress
                .lock()
                .unwrap()
                .resume_backoff
                .get(&record.id)
                .is_some_and(|(due, _)| *due > now);
            if waiting {
                continue;
            }
            purge.resume_attempts.fetch_add(1, Ordering::Relaxed);
            let after = self.resume_namespace_deletion(record);
            let mut progress = purge.progress.lock().unwrap();
            if after.state == DeletionState::Completed {
                completed += 1;
                purge.resume_completed.fetch_add(1, Ordering::Relaxed);
                progress.resume_backoff.remove(&record.id);
                println!(
                    "deletion resume: {} ({}) completed",
                    record.id, record.subject
                );
            } else {
                let pause = progress
                    .resume_backoff
                    .get(&record.id)
                    .map_or(interval, |(_, pause)| (*pause * 2).min(RESUME_MAX_BACKOFF));
                progress
                    .resume_backoff
                    .insert(record.id.clone(), (now + pause, pause));
            }
        }
        completed
    }
}

#[cfg(test)]
#[path = "deletion_purge_tests.rs"]
mod tests;
