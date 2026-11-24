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
//!   files too, and the sweep is repeated later. A writer asks the control
//!   store about the namespace again under those locks: a first publish
//!   that registered it after the sweep's listing was read (so the listing
//!   is stale) leaves the namespace for the next sweep;
//! - a directory written to within [`PurgeTuning::write_margin`] of the
//!   start of the sweep is left for the next sweep. A segment published
//!   after the control store was read can only have reached this disk
//!   after the sweep started, so this is what keeps it;
//! - a directory with a pinned or in-flight fetch is left for the next
//!   sweep.
//!
//! **What "completed" on a record means.** The control store, the pod that
//! ran the delete and object storage, and, for a record written with
//! `acks_required` (`KOSHA_DELETION_REQUIRE_ACKS`), every live purge pod.
//! Each pass heartbeats this pod into the pod registry; after a clean sweep
//! of a subject it acknowledges the subject's records that ask for it
//! (a tenant only once its storage reclaim is done) and completes a record
//! once no live pod is missing ([`AppState::finalize_deletion_if_acked`]).
//! A pod whose heartbeat is older than `KOSHA_POD_LIVENESS_MS` is not
//! waited for. What holds for a pod with the purge enabled that can read
//! the control store, acknowledgements or not:
//!
//! - a record is handled by the first pass that starts after the record's
//!   transaction commits, so within one interval plus the length of a pass;
//! - every subject is swept a second time [`PurgeTuning::settle`] later
//!   (again while anything was left or failed), which removes what a search
//!   or fetch already running during the first sweep wrote afterwards;
//! - a tenant is swept again every [`PurgeTuning::settle`] for as long as
//!   its record is pending, and once more after the record completes. A
//!   tenant record is written before the tenant's namespaces go, and a
//!   sweep keeps what the control store still lists; the sweep after
//!   completion is the one that removes what got no record of its own
//!   (the tenant's branches, directories no namespace was registered for);
//! - a pod that was down handles every record in its first pass after boot.
//!
//! Not covered: a pod with the purge off (the default); parsed segments a
//! search already running still holds, until that search ends; a pod that
//! cannot read the control store, until it can; freed disk blocks and the
//! operating system's page cache. A branch deleted on its own has a record
//! of scope `branch` (#347): its sweep keeps what any manifest of the
//! tenant still lists, since a branch of the branch can list segments in
//! its prefix. `GET /v1/stats` reports
//! each pod's progress under `deletion_purge`.
//!
//! **Retrying pending records.** `KOSHA_DELETION_RESUME_INTERVAL_MS`
//! (opt-in, writer pods only) retries the storage reclaim of pending
//! namespace, branch and tenant records at boot and on a timer, with a
//! growing pause per record. A pending tenant record is a standing order:
//! a retry deletes whatever the tenant id holds at the time it runs. A
//! record that waits only for acknowledgements is not retried, and a
//! `migrated` one is finished by `migrate-namespace`.
//!
//! A retry deletes the namespace's whole object-storage prefix, and it runs
//! unattended at boot, which is when two writer pods can overlap. So each
//! retry first takes the namespace's writer lease (#254) and is skipped
//! when another writer holds it: that writer may be publishing a new
//! incarnation this process cannot see. Holding the lease, no other
//! writer's manifest CAS for the namespace can commit between the "not
//! re-created" check and the prefix delete. A lease taken only for the
//! retry is released after it. With `KOSHA_WRITER_LEASE=off` there is no
//! such guard: do not enable the retry where two writer pods can run at
//! once.

use super::*;
use kosha_core::deletion::{
    DeletionFilter, DeletionPosition, DeletionRecord, DeletionScope, PodHeartbeat,
};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// This pod's id in the pod registry and on its acknowledgements:
/// `KOSHA_POD_ID`, else `HOSTNAME` (the pod name on Kubernetes), else a
/// random id for the life of the process.
pub(crate) fn pod_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| {
        ["KOSHA_POD_ID", "HOSTNAME"]
            .iter()
            .filter_map(|name| std::env::var(name).ok())
            .map(|value| value.trim().to_string())
            .find(|value| !value.is_empty())
            .unwrap_or_else(|| format!("pod-{}-{}", std::process::id(), unix_millis()))
    })
}

/// A subject's records in one pass: its scope, the highest retired
/// version, the tenant records whose reclaim is not done, and the records
/// waiting for this pod's acknowledgement.
type FreshSubject = (DeletionScope, Option<u64>, Vec<String>, Vec<String>);

/// The order subjects are swept in: namespaces and branches before the
/// tenants they belong to.
fn scope_order(scope: DeletionScope) -> u8 {
    match scope {
        DeletionScope::Namespace => 0,
        DeletionScope::Branch => 1,
        DeletionScope::Migrated => 2,
        DeletionScope::Tenant => 3,
    }
}
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PurgeSettings {
    /// How often this pod purges the caches of deleted namespaces; `None`
    /// when off.
    pub interval: Option<Duration>,
    /// How often a writer pod retries pending namespace deletions; `None`
    /// when off.
    pub resume_interval: Option<Duration>,
    /// `KOSHA_DELETION_REQUIRE_ACKS`: deletions recorded on this pod
    /// complete only once every live purge pod acknowledged them.
    pub require_acks: bool,
    /// `KOSHA_POD_LIVENESS_MS`: a pod whose last heartbeat is older is not
    /// waited for.
    pub liveness: Duration,
    /// `KOSHA_NAMESPACE_DELETE_CONFIRM`: `DELETE /v1/namespaces/{ns}`
    /// requires `?confirm=<ns>`.
    pub confirm_deletes: bool,
}

/// Default `KOSHA_POD_LIVENESS_MS`.
const DEFAULT_LIVENESS: Duration = Duration::from_secs(5 * 60);

fn parse_switch(name: &str, raw: Option<&str>) -> Result<bool, String> {
    match raw.map(str::trim) {
        None | Some("") | Some("off") | Some("0") | Some("false") => Ok(false),
        Some("on") | Some("1") | Some("true") => Ok(true),
        Some(other) => Err(format!("{name}={other:?}: expected on or off")),
    }
}

impl Default for PurgeSettings {
    fn default() -> Self {
        Self {
            interval: None,
            resume_interval: None,
            require_acks: false,
            liveness: DEFAULT_LIVENESS,
            confirm_deletes: false,
        }
    }
}

impl PurgeSettings {
    /// `Err` is the message to refuse to start with.
    pub(crate) fn from_env() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).ok();
        let mut settings = Self::parse(
            var("KOSHA_DELETION_PURGE_INTERVAL_MS").as_deref(),
            var("KOSHA_DELETION_RESUME_INTERVAL_MS").as_deref(),
        )?;
        settings.require_acks = parse_switch(
            "KOSHA_DELETION_REQUIRE_ACKS",
            var("KOSHA_DELETION_REQUIRE_ACKS").as_deref(),
        )?;
        settings.confirm_deletes = parse_switch(
            "KOSHA_NAMESPACE_DELETE_CONFIRM",
            var("KOSHA_NAMESPACE_DELETE_CONFIRM").as_deref(),
        )?;
        if let Some(liveness) = parse_interval(
            "KOSHA_POD_LIVENESS_MS",
            var("KOSHA_POD_LIVENESS_MS").as_deref(),
        )? {
            settings.liveness = liveness;
        }
        settings.check()?;
        Ok(settings)
    }

    /// Settings that cannot work together. Acknowledgements come from purge
    /// passes, so a pod that requires them must run the purge itself, and
    /// its heartbeat must come well inside the liveness window.
    pub(crate) fn check(&self) -> Result<(), String> {
        if !self.require_acks {
            return Ok(());
        }
        let Some(interval) = self.interval else {
            return Err(
                "KOSHA_DELETION_REQUIRE_ACKS=on needs KOSHA_DELETION_PURGE_INTERVAL_MS: \
                        acknowledgements come from purge passes"
                    .into(),
            );
        };
        if interval * 3 > self.liveness {
            return Err(format!(
                "KOSHA_POD_LIVENESS_MS ({} ms) must be at least three purge intervals ({} ms): \
                 a live pod would read as dead between heartbeats",
                self.liveness.as_millis(),
                interval.as_millis() * 3
            ));
        }
        Ok(())
    }

    pub(crate) fn parse(purge: Option<&str>, resume: Option<&str>) -> Result<Self, String> {
        Ok(Self {
            interval: parse_interval("KOSHA_DELETION_PURGE_INTERVAL_MS", purge)?,
            resume_interval: parse_interval("KOSHA_DELETION_RESUME_INTERVAL_MS", resume)?,
            ..Self::default()
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
    /// Tenant records of the subject whose storage reclaim is not yet
    /// seen done. While any is pending the subject stays queued: the
    /// tenant's namespaces are still in the control store, and what goes
    /// with them without a record of its own is only removable after the
    /// reclaim finished.
    pending: Vec<String>,
    /// Records of the subject that wait for this pod's acknowledgement
    /// (`acks_required`), not yet acknowledged.
    unacked: Vec<String>,
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
    resume_fenced: AtomicU64,
    /// `KOSHA_DELETION_REQUIRE_ACKS`: deletions recorded on this pod wait
    /// for every live purge pod's acknowledgement.
    require_acks: AtomicBool,
    /// `KOSHA_POD_LIVENESS_MS`.
    liveness_ms: AtomicU64,
    /// `KOSHA_NAMESPACE_DELETE_CONFIRM`.
    confirm_deletes: AtomicBool,
    acks_written: AtomicU64,
    records_finalized: AtomicU64,
    heartbeat_errors: AtomicU64,
    /// When this pod last looked for records that wait only for
    /// acknowledgements.
    last_finalize_scan: Mutex<Option<Instant>>,
    /// This pod's id when not the process-wide [`pod_id`] (tests run
    /// several pods in one process).
    pub(crate) pod_id: Mutex<Option<String>>,
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
            "kosha_deletion_resume_fenced_total": load(&self.resume_fenced),
            "pod_id": self.pod_id.lock().unwrap().clone().unwrap_or_else(|| pod_id().to_string()),
            "require_acks": self.require_acks.load(Ordering::Relaxed),
            "kosha_deletion_acks_written_total": load(&self.acks_written),
            "kosha_deletion_records_finalized_total": load(&self.records_finalized),
            "kosha_deletion_heartbeat_errors_total": load(&self.heartbeat_errors),
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
    pub(crate) fn follows_only(&self) -> bool {
        self.role == role::Role::Query || self.ingest_host.is_some()
    }

    /// Start the purge thread (and, on a writer pod, the retry thread) when
    /// configured. The first pass runs at once: that is the boot catch-up.
    pub(crate) fn spawn_deletion_purge(self: &Arc<Self>, settings: PurgeSettings) {
        self.configure_deletion_purge(&settings);
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
                // The heartbeat runs on its own, so a pass that takes longer
                // than the liveness window (a large tenant sweep, a slow
                // disk) does not make this live pod look dead.
                let every = interval
                    .min(settings.liveness / 4)
                    .max(Duration::from_millis(100));
                let weak = Arc::downgrade(self);
                std::thread::Builder::new()
                    .name("kosha-pod-heartbeat".into())
                    .spawn(move || loop {
                        std::thread::sleep(every);
                        let Some(state) = weak.upgrade() else {
                            return;
                        };
                        state.heartbeat();
                    })
                    .expect("failed to spawn pod heartbeat thread");
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
            "pending deletions (namespaces, branches, tenants) are retried every {} ms",
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
        // Before reading records: a pod that is live waits nobody out.
        self.heartbeat();

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
        let mut subjects: BTreeMap<(u8, String), FreshSubject> = BTreeMap::new();
        for record in &fresh {
            let (scope, floor, pending, unacked) = subjects
                .entry((scope_order(record.scope), record.subject.clone()))
                .or_insert((record.scope, None, Vec::new(), Vec::new()));
            if *scope != DeletionScope::Tenant {
                *floor = Some(floor.unwrap_or(0).max(record.last_version));
            }
            // Read before the sweep below: a record seen done here was done
            // before its subject is swept. A tenant always waits for its
            // reclaim (its namespaces are still listed until then); any
            // other record only when this pod is to vouch for it: an
            // acknowledgement covers storage verified by the writer too.
            if !record.storage_done() && (*scope == DeletionScope::Tenant || record.acks_required) {
                pending.push(record.id.clone());
            }
            if record.acks_required && !record.is_completed() {
                unacked.push(record.id.clone());
            }
        }
        let recent = fresh
            .iter()
            .filter(|record| record.requested_at_ms.saturating_add(RECENT_RECORD_MS) >= now_ms)
            .map(|record| record.subject.as_str())
            .collect::<HashSet<_>>();
        let mut requeue: Vec<Resweep> = Vec::new();
        for ((_, subject), (scope, floor, pending, unacked)) in subjects {
            let outcome = self.sweep_deleted_subject(scope, &subject, floor, tuning);
            self.note_sweep(&outcome, &subject, &mut report);
            // A recent deletion is acknowledged only after its settle
            // sweep, which removes what a search or fetch already running
            // during this one wrote afterwards.
            let unacked = if outcome.unfinished() || recent.contains(subject.as_str()) {
                unacked
            } else {
                self.acknowledge_swept(&unacked, &pending)
            };
            if outcome.unfinished()
                || recent.contains(subject.as_str())
                || !pending.is_empty()
                || !unacked.is_empty()
            {
                requeue.push(Resweep {
                    scope,
                    subject,
                    floor,
                    pending,
                    unacked,
                    due: Instant::now() + tuning.settle,
                });
            }
        }

        // ── Repeat sweeps that are due ─────────────────────────────────
        let due: Vec<Resweep> = {
            let mut progress = purge.progress.lock().unwrap();
            let now = Instant::now();
            // A subject swept just now waits for its new turn instead, and
            // keeps waiting for the records its queued turn waited for.
            progress.queue.retain_mut(|queued| {
                let Some(new) = requeue
                    .iter_mut()
                    .find(|new| new.scope == queued.scope && new.subject == queued.subject)
                else {
                    return true;
                };
                new.pending.append(&mut queued.pending);
                for id in queued.unacked.drain(..) {
                    if !new.unacked.contains(&id) {
                        new.unacked.push(id);
                    }
                }
                false
            });
            let (due, later) = std::mem::take(&mut progress.queue)
                .into_iter()
                .partition(|queued| queued.due <= now);
            progress.queue = later;
            due
        };
        for queued in due {
            // Before the sweep: once a record reads completed here, this
            // sweep ran after it and is the subject's last on its account.
            let pending = self.deletions_still_pending(&queued.pending);
            let outcome =
                self.sweep_deleted_subject(queued.scope, &queued.subject, queued.floor, tuning);
            self.note_sweep(&outcome, &queued.subject, &mut report);
            let unacked = if outcome.unfinished() {
                queued.unacked.clone()
            } else {
                self.acknowledge_swept(&queued.unacked, &pending)
            };
            if outcome.unfinished() || !pending.is_empty() || !unacked.is_empty() {
                requeue.push(Resweep {
                    due: Instant::now() + tuning.settle,
                    pending,
                    unacked,
                    ..queued
                });
            }
        }
        self.finalize_waiting_deletions(tuning);

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

    /// The records among `ids` whose storage reclaim is not done. A record
    /// that cannot be read counts as pending; one that no longer exists
    /// does not.
    fn deletions_still_pending(&self, ids: &[String]) -> Vec<String> {
        ids.iter()
            .filter(|id| {
                let record = self.controller.lock().unwrap().deletion(id);
                match record {
                    Ok(Some(record)) => !record.storage_done(),
                    Ok(None) => false,
                    Err(_) => true,
                }
            })
            .cloned()
            .collect()
    }

    /// After a clean sweep: acknowledge every record of `unacked` that is
    /// not in `pending` (a tenant whose reclaim is still running is swept
    /// again before this pod vouches for it), then try to complete each.
    /// Returns the records still unacknowledged (a failed write).
    fn acknowledge_swept(&self, unacked: &[String], pending: &[String]) -> Vec<String> {
        let purge = &self.deletion_purge;
        let liveness = self.pod_liveness_ms();
        let mut left = Vec::new();
        for id in unacked {
            if pending.contains(id) {
                left.push(id.clone());
                continue;
            }
            let acked = self
                .controller
                .lock()
                .unwrap()
                .acknowledge_deletion(id, &self.pod_id());
            match acked {
                Ok(()) => {
                    purge.acks_written.fetch_add(1, Ordering::Relaxed);
                    self.finalize_deletion_if_acked(id, liveness);
                }
                Err(e) => {
                    eprintln!(
                        "WARN: deletion purge: acknowledging {id} failed: {e}; it is retried"
                    );
                    left.push(id.clone());
                }
            }
        }
        left
    }

    /// Complete record `id` if it waits only for acknowledgements and every
    /// live pod has given one.
    pub(crate) fn finalize_deletion_if_acked(
        &self,
        id: &str,
        liveness_ms: u64,
    ) -> Option<DeletionRecord> {
        let finalized = self
            .controller
            .lock()
            .unwrap()
            .finalize_deletion(id, liveness_ms);
        match finalized {
            Ok(record) => {
                if record.is_completed() && record.acks_required {
                    // Counted by whoever sees it complete; a record
                    // finalized twice is still one record.
                    self.deletion_purge
                        .records_finalized
                        .fetch_add(1, Ordering::Relaxed);
                }
                Some(record)
            }
            Err(e) => {
                eprintln!("WARN: deletion {id}: finalizing failed: {e}");
                None
            }
        }
    }

    /// Every `settle`, complete the records that wait only for
    /// acknowledgements: a pod that died stops counting once its heartbeat
    /// is older than the liveness window, which no acknowledgement of this
    /// pod would notice.
    fn finalize_waiting_deletions(&self, tuning: &PurgeTuning) {
        let purge = &self.deletion_purge;
        {
            let mut last = purge.last_finalize_scan.lock().unwrap();
            if last.is_some_and(|at| at.elapsed() < tuning.settle) {
                return;
            }
            *last = Some(Instant::now());
        }
        let pending = self
            .controller
            .lock()
            .unwrap()
            .deletions(&DeletionFilter::pending());
        let Ok(pending) = pending else {
            return;
        };
        let liveness = self.pod_liveness_ms();
        for record in pending.iter().filter(|record| record.awaiting_acks()) {
            self.finalize_deletion_if_acked(&record.id, liveness);
        }
    }

    /// Refresh this pod's row in the pod registry.
    fn heartbeat(&self) {
        let heartbeat = PodHeartbeat {
            pod_id: self.pod_id(),
            role: self.role.to_string(),
            purge_enabled: true,
        };
        let written = self.controller.lock().unwrap().heartbeat_pod(&heartbeat);
        if let Err(e) = written {
            self.deletion_purge
                .heartbeat_errors
                .fetch_add(1, Ordering::Relaxed);
            eprintln!("WARN: deletion purge: pod heartbeat failed: {e}");
        }
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
    pub(crate) fn purge_deleted_from_memory(&self, subject: &str) -> u64 {
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
        // A deleted branch's prefix can hold segments a remaining branch
        // of it still lists (its delete reclaims only the unreferenced
        // ones), so for a branch they are always read.
        let mut keep = Keep::default();
        if scope == DeletionScope::Branch || stored.keys().any(|ns| in_subject(&ns.0)) {
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
            DeletionScope::Namespace | DeletionScope::Branch | DeletionScope::Migrated => {
                namespaces.push(NamespaceId(subject.to_string()))
            }
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
    ///
    /// `stored` was listed before those locks were taken, possibly long
    /// before (a tenant sweep locks each namespace in turn). On a writer
    /// that knows `ns`, whether the store has it is therefore read again
    /// under the locks: a namespace whose first publish landed in between
    /// would otherwise read as deleted and fully published, and lose its
    /// only local copy. The order makes the second read sufficient: what
    /// this writer holds is captured first, so anything it reports as
    /// published was committed before the store is asked.
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
            if local.is_some() && !follower {
                let current = self.controller.lock().unwrap().manifest_version(ns);
                match current {
                    Ok(current) if current.is_some() == in_store => {}
                    // The listing is stale for this namespace (registered or
                    // dropped since), and so is `keep`: the next sweep reads
                    // both again.
                    Ok(_) => {
                        outcome.deferred += 1;
                        return;
                    }
                    Err(e) => {
                        outcome.fail(format!("control store: reading '{}': {e}", ns.0));
                        return;
                    }
                }
            }
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
    ///
    /// This runs unattended, at boot too, when another writer pod may still
    /// be alive, and a retry deletes the namespace's whole object-storage
    /// prefix on the strength of "not in the control store, untouched
    /// here". Another writer can be about to publish a new incarnation
    /// there. So each retry first takes the namespace's writer lease
    /// (#254): refused while another writer holds it (the record waits its
    /// pause), and once taken no other writer's CAS for the namespace can
    /// commit, so the check stays true until the prefix is gone. A lease
    /// taken only for this is released afterwards.
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
        // A tenant's pass re-checks its namespaces, so those go first; a
        // record that waits only for acknowledgements has nothing to retry,
        // and a migrated source is finished by the migration tool.
        let mut retried: Vec<&DeletionRecord> = pending
            .iter()
            .filter(|record| !record.storage_done() && record.scope != DeletionScope::Migrated)
            .collect();
        retried.sort_by_key(|record| record.scope == DeletionScope::Tenant);
        for record in retried {
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
            let ns = NamespaceId(record.subject.clone());
            // A tenant has no single writer lease: its pass deletes each
            // namespace through the delete path, refuses while any is left
            // and re-checks after sweeping the prefix.
            if record.scope == DeletionScope::Tenant {
                purge.resume_attempts.fetch_add(1, Ordering::Relaxed);
                let after = self.delete_tenant(auth::global(), record).record;
                self.note_resume(record, after.storage_done(), interval, now, &mut completed);
                continue;
            }
            let held_before = self.writer_leases.held_before(&ns);
            let done = match self.writer_leases.token(&ns, &self.controller) {
                Ok(_) => {
                    purge.resume_attempts.fetch_add(1, Ordering::Relaxed);
                    let after = match record.scope {
                        DeletionScope::Branch => self.resume_branch_deletion(record),
                        _ => self.resume_namespace_deletion(record),
                    };
                    if !held_before {
                        self.writer_leases.release(&ns, &self.controller);
                    }
                    after.storage_done()
                }
                Err(error) => {
                    purge.resume_fenced.fetch_add(1, Ordering::Relaxed);
                    if !matches!(error, KoshaError::Fenced { .. }) {
                        eprintln!(
                            "WARN: deletion resume: {} ({}): taking the writer lease failed: \
                             {error}; it is retried",
                            record.id, record.subject
                        );
                    }
                    false
                }
            };
            self.note_resume(record, done, interval, now, &mut completed);
        }
        completed
    }

    /// Count a retry, and set the record's next pause when it stays pending.
    fn note_resume(
        &self,
        record: &DeletionRecord,
        done: bool,
        interval: Duration,
        now: Instant,
        completed: &mut usize,
    ) {
        let purge = &self.deletion_purge;
        let mut progress = purge.progress.lock().unwrap();
        if done {
            *completed += 1;
            purge.resume_completed.fetch_add(1, Ordering::Relaxed);
            progress.resume_backoff.remove(&record.id);
            println!(
                "deletion resume: {} ({} {}) storage reclaimed",
                record.id,
                record.scope.as_str(),
                record.subject
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

    /// Store the settings a pass and the delete handlers read.
    pub(crate) fn configure_deletion_purge(&self, settings: &PurgeSettings) {
        let purge = &self.deletion_purge;
        purge
            .require_acks
            .store(settings.require_acks, Ordering::Relaxed);
        purge
            .liveness_ms
            .store(settings.liveness.as_millis() as u64, Ordering::Relaxed);
        purge
            .confirm_deletes
            .store(settings.confirm_deletes, Ordering::Relaxed);
    }

    /// This pod's id in the pod registry ([`pod_id`]).
    pub(crate) fn pod_id(&self) -> String {
        self.deletion_purge
            .pod_id
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| pod_id().to_string())
    }

    /// Whether deletions recorded on this pod wait for acknowledgements.
    pub(crate) fn deletion_requires_acks(&self) -> bool {
        self.deletion_purge.require_acks.load(Ordering::Relaxed)
    }

    /// `KOSHA_POD_LIVENESS_MS`, in milliseconds.
    pub(crate) fn pod_liveness_ms(&self) -> u64 {
        match self.deletion_purge.liveness_ms.load(Ordering::Relaxed) {
            0 => DEFAULT_LIVENESS.as_millis() as u64,
            ms => ms,
        }
    }

    /// Whether `DELETE /v1/namespaces/{ns}` requires `?confirm=<ns>`.
    pub(crate) fn namespace_delete_needs_confirm(&self) -> bool {
        self.deletion_purge.confirm_deletes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[path = "deletion_purge_tests.rs"]
mod tests;
