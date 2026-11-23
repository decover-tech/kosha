//! Stored-bytes reconciliation (#348): the hourly `stored_bytes` usage
//! snapshots against what object storage actually holds.
//!
//! - `GET /v1/admin/usage/reconcile` runs it on a pod (admin key).
//! - `kosha-server reconcile-usage` runs it as a one-shot process for a
//!   CronJob: the report as JSON on stdout, exit 0 when every namespace is
//!   within tolerance, 1 when one is not, 2 when it could not run.
//!
//! # What is compared
//!
//! The snapshot (`usage::run_snapshot_tick`) is, per root namespace, the
//! bytes of every object under `<namespace>/<segment>/` for each segment
//! the namespace's manifest lists and owns, shards added up under their
//! root. A segment a branch inherits is left to the namespace whose prefix
//! holds it. The comparison uses the same definition, so each object under
//! a namespace's prefix lands in exactly one class:
//!
//! - **referenced**: under a segment the manifest lists and owns. This is
//!   the number compared with the snapshot. It includes superseded
//!   deletion bitmaps inside such a segment (`superseded_deletion_bytes`),
//!   because the snapshot counts them too until GC removes them.
//! - **referenced_unmetered**: referenced by a branch's manifest but not
//!   counted by any snapshot: a segment a branch still lists after the
//!   namespace that wrote it dropped it, and a deletion bitmap a branch
//!   wrote for a segment it inherits.
//! - **orphaned**: nothing references it. `retired` (a manifest recorded
//!   dropping the segment; GC removes it after its grace), `untracked` (no
//!   manifest ever listed it: a crashed flush, an upload not published
//!   yet, a superseded bitmap of an inherited segment) and `stray` (a file
//!   directly under the namespace's prefix).
//!
//! Storage that belongs to no namespace the control plane knows is reported
//! too, per `<tenant>/<name>` prefix, together with the deletion record
//! (#347) that explains it when there is one. Its objects are classed the
//! same way, with no manifest of its own: what a branch's manifest still
//! lists is `referenced_unmetered` (deleting a branch keeps the segments a
//! descendant branch inherited from it, under the deleted branch's prefix),
//! everything else is orphaned.
//!
//! Deleting a branch and `migrate-namespace` remove a namespace without a
//! deletion record. Such a namespace passes as `removed` (nothing under its
//! prefix) or `held_by_branch` (only objects a branch still lists) unless
//! its latest snapshot is stamped with the current hour: only that says a
//! writer may still be metering it.
//!
//! # Cost and memory
//!
//! Listings only, never an object body. Every listing is consumed one page
//! at a time ([`ObjectInventory`]); what is held is one namespace's segment
//! ids, the branch manifests' references, and one report row per namespace.
//! Each namespace's prefix is listed once, plus one delimiter listing of
//! the storage root and of every tenant prefix to find what is not known.

use super::*;
use kosha_control::usage::{meter, UsageStore, HOUR_MS};
use kosha_core::branch::References;
use kosha_core::deletion::{DeletionFilter, DeletionRecord, DeletionScope, DeletionState};
use kosha_core::gc::deletion_generation_of;
use kosha_core::Manifest;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// Default `KOSHA_USAGE_RECONCILE_TOLERANCE`: 5% of the larger side.
const DEFAULT_TOLERANCE: f64 = 0.05;
/// Default `KOSHA_USAGE_RECONCILE_TOLERANCE_BYTES`: 1 MiB. A snapshot is up
/// to an hour older than the listing, and one flush is a large fraction of
/// a small namespace.
const DEFAULT_TOLERANCE_BYTES: u64 = 1024 * 1024;
/// Default `KOSHA_USAGE_RECONCILE_MAX_SNAPSHOT_AGE`. The snapshot of hour
/// `H` is stamped `H:00` and taken up to one snapshot tick later, so the
/// newest one is between zero and a little over one hour old.
const DEFAULT_MAX_SNAPSHOT_AGE: Duration = Duration::from_secs(3 * 3600);
/// Shorter than this and a healthy namespace's newest snapshot can be
/// outside the window.
const MIN_MAX_SNAPSHOT_AGE: Duration = Duration::from_secs(3600);
/// How many deletion records are read to explain storage and snapshots of
/// namespaces the control plane no longer has.
const DELETION_RECORD_LIMIT: u32 = 100_000;
/// How many prefixes unknown to the control plane get a row of their own.
/// The rest are counted (`unknown_prefixes_not_listed`), not summed.
const MAX_UNKNOWN_PREFIXES: usize = 1_000;

const TOLERANCE_ENV: &str = "KOSHA_USAGE_RECONCILE_TOLERANCE";
const TOLERANCE_BYTES_ENV: &str = "KOSHA_USAGE_RECONCILE_TOLERANCE_BYTES";
const MAX_SNAPSHOT_AGE_ENV: &str = "KOSHA_USAGE_RECONCILE_MAX_SNAPSHOT_AGE";

// ─── Options ─────────────────────────────────────────────────────────────────

/// Settings of one reconciliation run.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Options {
    /// One tenant; every tenant (and the pre-#262 bare namespaces) when
    /// `None`.
    pub tenant: Option<String>,
    /// Allowed difference as a fraction of the larger of the two numbers.
    pub tolerance: f64,
    /// Allowed difference in bytes, whatever the fraction allows.
    pub tolerance_bytes: u64,
    /// A snapshot older than this does not count as one.
    pub max_snapshot_age_ms: u64,
    pub now_ms: u64,
}

fn parse_tolerance(raw: &str) -> Result<f64, String> {
    raw.trim()
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite() && (0.0..=1.0).contains(v))
        .ok_or_else(|| format!("'{raw}' is not a fraction between 0 and 1 (e.g. 0.05)"))
}

fn parse_bytes(raw: &str) -> Result<u64, String> {
    raw.trim()
        .parse::<u64>()
        .map_err(|_| format!("'{raw}' is not a number of bytes"))
}

fn parse_max_age(raw: &str) -> Result<u64, String> {
    let age = segment_gc::parse_duration(raw)
        .ok_or_else(|| format!("'{raw}' is not a duration (e.g. 10800, 180m, 3h)"))?;
    if age < MIN_MAX_SNAPSHOT_AGE {
        return Err(format!(
            "'{raw}' is shorter than one hour: snapshots are hourly, so every namespace would \
             read as having none"
        ));
    }
    Ok(age.as_millis() as u64)
}

impl Options {
    /// The defaults, overridden by the `KOSHA_USAGE_RECONCILE_*` variables
    /// `get` returns. A variable that is set but invalid is an error: a
    /// run with a tolerance nobody asked for would pass or fail for the
    /// wrong reason.
    pub(crate) fn from_lookup(
        get: impl Fn(&str) -> Option<String>,
        now_ms: u64,
    ) -> Result<Self, String> {
        let mut options = Self {
            tenant: None,
            tolerance: DEFAULT_TOLERANCE,
            tolerance_bytes: DEFAULT_TOLERANCE_BYTES,
            max_snapshot_age_ms: DEFAULT_MAX_SNAPSHOT_AGE.as_millis() as u64,
            now_ms,
        };
        for (name, key) in [
            (TOLERANCE_ENV, "tolerance"),
            (TOLERANCE_BYTES_ENV, "tolerance_bytes"),
            (MAX_SNAPSHOT_AGE_ENV, "max_snapshot_age"),
        ] {
            if let Some(value) = get(name).filter(|v| !v.trim().is_empty()) {
                options
                    .set(key, &value)
                    .map_err(|e| format!("invalid {name}: {e}"))?;
            }
        }
        Ok(options)
    }

    fn from_env(now_ms: u64) -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok(), now_ms)
    }

    /// Set one option by its request-parameter name (the CLI flags are the
    /// same names with dashes). `Ok(false)` for a name that is not one.
    fn set(&mut self, key: &str, value: &str) -> Result<bool, String> {
        match key {
            "tenant" => {
                if value.is_empty() || value.contains('/') {
                    return Err(format!("'{value}' is not a tenant id"));
                }
                self.tenant = Some(value.to_string());
            }
            "tolerance" => self.tolerance = parse_tolerance(value)?,
            "tolerance_bytes" => self.tolerance_bytes = parse_bytes(value)?,
            "max_snapshot_age" => self.max_snapshot_age_ms = parse_max_age(value)?,
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// The difference allowed between `a` and `b`.
    fn allowed(&self, a: u64, b: u64) -> u64 {
        let relative = (a.max(b) as f64 * self.tolerance).ceil();
        // `as` saturates, which is what a tolerance wants.
        (relative as u64).max(self.tolerance_bytes)
    }
}

// ─── Object storage, one page at a time ──────────────────────────────────────

/// One immediate child of a directory: names are relative to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Child<'a> {
    Dir(&'a str),
    File(&'a str, u64),
}

/// Object storage as reconciliation reads it: sizes and names only, handed
/// over as the listing produces them so nothing proportional to the number
/// of objects is ever held. Paths are logical (`<namespace>/<segment>/<file>`),
/// `""` is the storage root.
pub(crate) trait ObjectInventory {
    /// `s3` or `local`, for the report.
    fn backend(&self) -> &'static str;
    /// Every object under the directory `dir`, recursively:
    /// `visit(path, size)`.
    fn walk(&self, dir: &str, visit: &mut dyn FnMut(&str, u64)) -> Result<(), KoshaError>;
    /// The immediate children of `dir`.
    fn children(&self, dir: &str, visit: &mut dyn FnMut(Child<'_>)) -> Result<(), KoshaError>;
}

/// A local directory tree as object storage: what a node without S3 (and
/// the tests) keeps its segments in.
pub(crate) struct LocalInventory(pub PathBuf);

impl LocalInventory {
    fn read_dir(dir: &std::path::Path) -> Result<Option<std::fs::ReadDir>, KoshaError> {
        match std::fs::read_dir(dir) {
            Ok(entries) => Ok(Some(entries)),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn walk_dir(
        dir: &std::path::Path,
        logical: &mut String,
        visit: &mut dyn FnMut(&str, u64),
    ) -> Result<(), KoshaError> {
        let Some(entries) = Self::read_dir(dir)? else {
            return Ok(());
        };
        for entry in entries {
            let entry = entry?;
            let meta = match entry.metadata() {
                Ok(meta) => meta,
                // Removed between the listing and the stat.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            let len = logical.len();
            if !logical.is_empty() {
                logical.push('/');
            }
            logical.push_str(&entry.file_name().to_string_lossy());
            let result = if meta.is_dir() {
                Self::walk_dir(&entry.path(), logical, visit)
            } else {
                visit(logical, meta.len());
                Ok(())
            };
            logical.truncate(len);
            result?;
        }
        Ok(())
    }
}

impl ObjectInventory for LocalInventory {
    fn backend(&self) -> &'static str {
        "local"
    }

    fn walk(&self, dir: &str, visit: &mut dyn FnMut(&str, u64)) -> Result<(), KoshaError> {
        let dir = dir.trim_matches('/');
        let mut logical = dir.to_string();
        Self::walk_dir(&self.0.join(dir), &mut logical, visit)
    }

    fn children(&self, dir: &str, visit: &mut dyn FnMut(Child<'_>)) -> Result<(), KoshaError> {
        let Some(entries) = Self::read_dir(&self.0.join(dir.trim_matches('/')))? else {
            return Ok(());
        };
        for entry in entries {
            let entry = entry?;
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if meta.is_dir() {
                visit(Child::Dir(&name));
            } else {
                visit(Child::File(&name, meta.len()));
            }
        }
        Ok(())
    }
}

#[cfg(feature = "s3")]
impl ObjectInventory for s3_storage::S3Storage {
    fn backend(&self) -> &'static str {
        "s3"
    }

    fn walk(&self, dir: &str, visit: &mut dyn FnMut(&str, u64)) -> Result<(), KoshaError> {
        self.visit_listing(dir, true, &mut |entry| {
            if let s3_storage::Listed::Object { path, size } = entry {
                visit(path, size);
            }
        })
    }

    fn children(&self, dir: &str, visit: &mut dyn FnMut(Child<'_>)) -> Result<(), KoshaError> {
        let dir = dir.trim_matches('/');
        let name_of = |path: &'_ str| -> Option<usize> {
            if dir.is_empty() {
                return Some(0);
            }
            path.strip_prefix(dir)?
                .starts_with('/')
                .then_some(dir.len() + 1)
        };
        self.visit_listing(dir, false, &mut |entry| match entry {
            s3_storage::Listed::Object { path, size } => {
                if let Some(at) = name_of(path) {
                    visit(Child::File(&path[at..], size));
                }
            }
            s3_storage::Listed::Dir { path } => {
                if let Some(at) = name_of(path) {
                    visit(Child::Dir(&path[at..]));
                }
            }
        })
    }
}

// ─── The control plane, as reconciliation reads it ──────────────────────────

/// The control-store reads a run needs. Every one is fallible: a namespace
/// list that silently came back empty would report the whole bucket as
/// unknown storage.
pub(crate) trait ControlView {
    fn namespaces(&self) -> Result<Vec<NamespaceId>, KoshaError>;
    fn manifest(&self, ns: &NamespaceId) -> Result<Option<Manifest>, KoshaError>;
    /// Deletion records, oldest first, at most `limit`.
    fn deletions(&self, limit: u32) -> Result<Vec<DeletionRecord>, KoshaError>;
}

impl ControlView for Mutex<Box<dyn ControlStore>> {
    fn namespaces(&self) -> Result<Vec<NamespaceId>, KoshaError> {
        let control = self.lock().unwrap();
        // `manifest_versions` is the fallible read; `list_namespaces` adds
        // a namespace that has no manifest yet.
        let mut all: BTreeSet<String> = control
            .manifest_versions()?
            .into_iter()
            .map(|(ns, _)| ns.0)
            .collect();
        all.extend(control.list_namespaces().into_iter().map(|ns| ns.0));
        Ok(all.into_iter().map(NamespaceId).collect())
    }

    fn manifest(&self, ns: &NamespaceId) -> Result<Option<Manifest>, KoshaError> {
        self.lock().unwrap().try_manifest_cloned(ns)
    }

    fn deletions(&self, limit: u32) -> Result<Vec<DeletionRecord>, KoshaError> {
        self.lock().unwrap().deletions(&DeletionFilter {
            limit: Some(limit),
            ..DeletionFilter::default()
        })
    }
}

// ─── The report ──────────────────────────────────────────────────────────────

/// What object storage holds under one namespace's prefix, by class (see
/// the module doc). `bytes` is the sum of the three classes.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Storage {
    pub objects: u64,
    pub bytes: u64,
    /// Under segments the manifest lists and owns: what a snapshot measures.
    pub referenced_objects: u64,
    pub referenced_bytes: u64,
    /// The part of `referenced_bytes` that is superseded deletion bitmaps.
    pub superseded_deletion_bytes: u64,
    /// Referenced by a branch's manifest, counted by no snapshot.
    pub referenced_unmetered_objects: u64,
    pub referenced_unmetered_bytes: u64,
    /// Referenced by no manifest: `retired + untracked + stray`.
    pub orphaned_objects: u64,
    pub orphaned_bytes: u64,
    pub retired_bytes: u64,
    pub untracked_bytes: u64,
    pub stray_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Referenced { superseded_deletion: bool },
    ReferencedUnmetered,
    Retired,
    Untracked,
    Stray,
}

impl Storage {
    fn add(&mut self, class: Class, size: u64) {
        self.objects += 1;
        self.bytes = self.bytes.saturating_add(size);
        let (objects, bytes) = match class {
            Class::Referenced {
                superseded_deletion,
            } => {
                if superseded_deletion {
                    self.superseded_deletion_bytes =
                        self.superseded_deletion_bytes.saturating_add(size);
                }
                (&mut self.referenced_objects, &mut self.referenced_bytes)
            }
            Class::ReferencedUnmetered => (
                &mut self.referenced_unmetered_objects,
                &mut self.referenced_unmetered_bytes,
            ),
            Class::Retired | Class::Untracked | Class::Stray => {
                let detail = match class {
                    Class::Retired => &mut self.retired_bytes,
                    Class::Untracked => &mut self.untracked_bytes,
                    _ => &mut self.stray_bytes,
                };
                *detail = detail.saturating_add(size);
                (&mut self.orphaned_objects, &mut self.orphaned_bytes)
            }
        };
        *objects += 1;
        *bytes = bytes.saturating_add(size);
    }

    fn absorb(&mut self, other: &Storage) {
        let pairs: [(&mut u64, u64); 12] = [
            (&mut self.objects, other.objects),
            (&mut self.bytes, other.bytes),
            (&mut self.referenced_objects, other.referenced_objects),
            (&mut self.referenced_bytes, other.referenced_bytes),
            (
                &mut self.superseded_deletion_bytes,
                other.superseded_deletion_bytes,
            ),
            (
                &mut self.referenced_unmetered_objects,
                other.referenced_unmetered_objects,
            ),
            (
                &mut self.referenced_unmetered_bytes,
                other.referenced_unmetered_bytes,
            ),
            (&mut self.orphaned_objects, other.orphaned_objects),
            (&mut self.orphaned_bytes, other.orphaned_bytes),
            (&mut self.retired_bytes, other.retired_bytes),
            (&mut self.untracked_bytes, other.untracked_bytes),
            (&mut self.stray_bytes, other.stray_bytes),
        ];
        for (total, value) in pairs {
            *total = total.saturating_add(value);
        }
    }
}

/// What the control plane says about a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ControlPlane {
    /// It lists the namespace.
    Live,
    /// It does not, and a deletion record covers it.
    Deleted,
    /// It does not, and nothing says why.
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Status {
    /// Snapshot and referenced bytes agree within the tolerance.
    Match,
    /// Both exist and differ by more than the tolerance.
    Mismatch,
    /// Objects exist under the prefix and there is no recent snapshot: the
    /// storage is not being metered.
    StorageWithoutSnapshot,
    /// A recent snapshot says bytes are stored and the prefix is empty.
    SnapshotWithoutStorage,
    /// A deleted namespace whose prefix still holds objects.
    DeletedWithStorage,
    /// A deleted namespace that was snapshotted again in an hour that began
    /// after its deletion: a writer is still metering it.
    SnapshotAfterDeletion,
    /// A deleted namespace with an empty prefix (its last snapshots are
    /// still inside the window).
    Deleted,
    /// A namespace with no objects and no snapshot.
    Empty,
    /// A namespace the control plane no longer lists and no deletion record
    /// covers (a deleted branch, the source of `migrate-namespace`), with
    /// an empty prefix and a last snapshot from an earlier hour: it is
    /// gone and its snapshots are ageing out of the window.
    Removed,
    /// A prefix the control plane no longer lists whose every object a
    /// branch's manifest still lists (what deleting a branch keeps for its
    /// descendants), and no snapshot from the current hour.
    HeldByBranch,
}

impl Status {
    fn within_tolerance(self) -> bool {
        matches!(
            self,
            Status::Match
                | Status::Deleted
                | Status::Empty
                | Status::Removed
                | Status::HeldByBranch
        )
    }
}

/// The deletion record (#347) that explains a namespace the control plane
/// no longer lists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DeletionRef {
    pub id: String,
    pub scope: DeletionScope,
    pub state: DeletionState,
    pub requested_at_ms: u64,
    pub completed_at_ms: Option<u64>,
}

impl DeletionRef {
    fn of(record: &DeletionRecord) -> Self {
        Self {
            id: record.id.clone(),
            scope: record.scope,
            state: record.state,
            requested_at_ms: record.requested_at_ms,
            completed_at_ms: record.completed_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NamespaceReport {
    /// `""` for a pre-#262 bare namespace, as in the usage events.
    pub tenant: String,
    pub namespace: String,
    pub control_plane: ControlPlane,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion: Option<DeletionRef>,
    /// The latest `stored_bytes` snapshot inside the window, and the hour
    /// it is stamped with.
    pub snapshot_bytes: Option<u64>,
    pub snapshot_at_ms: Option<u64>,
    pub storage: Storage,
    /// `storage.referenced_bytes - snapshot_bytes`, when there is a
    /// snapshot to compare with.
    pub difference_bytes: Option<i64>,
    /// The difference this run allowed for these two numbers.
    pub allowed_difference_bytes: Option<u64>,
    pub status: Status,
    pub within_tolerance: bool,
}

/// Objects that belong to no namespace prefix (a file directly in the
/// storage root or in a tenant's directory). Reported, not judged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Unattributed {
    pub prefix: String,
    pub objects: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Totals {
    /// Sum of the snapshots of the namespaces that have one.
    pub snapshot_bytes: u64,
    pub storage: Storage,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Report {
    pub generated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    pub storage_backend: &'static str,
    pub tolerance: f64,
    pub tolerance_bytes: u64,
    pub max_snapshot_age_seconds: u64,
    /// False when any namespace is out of tolerance or flagged, or when
    /// there were more unknown prefixes than the report lists.
    pub within_tolerance: bool,
    pub namespaces_checked: usize,
    pub namespaces_out_of_tolerance: usize,
    pub totals: Totals,
    pub namespaces: Vec<NamespaceReport>,
    pub unattributed: Vec<Unattributed>,
    /// Top-level prefixes that are not namespaces (`_wal`, `_warmset`, ...):
    /// named, not listed.
    pub skipped_prefixes: Vec<String>,
    /// Prefixes unknown to the control plane beyond the per-run cap.
    pub unknown_prefixes_not_listed: u64,
    pub warnings: Vec<String>,
}

// ─── The run ─────────────────────────────────────────────────────────────────

/// `ns` without its `/_shard-NNNN` component: the namespace its usage is
/// recorded under (`usage::usage_identity`).
fn shard_root(ns: &NamespaceId) -> &str {
    match kosha_core::sharding::shard_index_of(ns) {
        Some(_) => {
            ns.0.rsplit_once('/')
                .map_or(ns.0.as_str(), |(root, _)| root)
        }
        None => &ns.0,
    }
}

/// The storage prefix of the namespace usage records as `(tenant, name)`.
fn prefix_of(tenant: &str, namespace: &str) -> String {
    if tenant.is_empty() {
        namespace.to_string()
    } else {
        format!("{tenant}/{namespace}")
    }
}

/// One namespace (a root or one of its shards) as its manifest describes
/// it: just the segment ids.
struct Member {
    id: String,
    /// Segments the manifest lists and this namespace's prefix holds.
    owned: HashSet<String>,
    /// Owned segment -> its current deletion generation.
    current_deletes: HashMap<String, String>,
    /// Segments a manifest version recorded dropping.
    retired: HashSet<String>,
}

impl Member {
    fn of(id: &NamespaceId, manifest: Option<&Manifest>) -> Self {
        let mut member = Self {
            id: id.0.clone(),
            owned: HashSet::new(),
            current_deletes: HashMap::new(),
            retired: HashSet::new(),
        };
        let Some(manifest) = manifest else {
            return member;
        };
        for entry in &manifest.segments {
            if !manifest.is_inherited_segment(id, &entry.segment_id) {
                member.owned.insert(entry.segment_id.0.clone());
            }
        }
        for (segment, vector) in &manifest.deletion_vectors {
            if member.owned.contains(&segment.0) && !vector.is_inherited(id) {
                member
                    .current_deletes
                    .insert(segment.0.clone(), vector.generation.clone());
            }
        }
        for retired in &manifest.retired {
            if retired.deletion_generation.is_none() {
                member.retired.insert(retired.segment_id.0.clone());
            }
        }
        member
    }

    /// The class of the object `rest` (`<segment>/<file>`, relative to this
    /// namespace's prefix).
    fn classify(&self, branches: &References, rest: &str) -> Class {
        let Some((segment, inner)) = rest.split_once('/') else {
            return Class::Stray;
        };
        let generation = deletion_generation_of(inner);
        if self.owned.contains(segment) {
            return Class::Referenced {
                superseded_deletion: generation.is_some_and(|generation| {
                    self.current_deletes.get(segment).map(String::as_str) != Some(generation)
                }),
            };
        }
        let dir = format!("{}/{segment}", self.id);
        let held = if generation.is_some() {
            // A deletion bitmap is referenced one file at a time.
            branches.holds_file(&format!("{dir}/{inner}"))
        } else {
            branches.holds_segment_dir(&dir)
        };
        if held {
            Class::ReferencedUnmetered
        } else if self.retired.contains(segment) {
            Class::Retired
        } else {
            Class::Untracked
        }
    }
}

/// The namespace ids the control plane knows, and which path components
/// under any path lead to one.
struct Known {
    ids: HashSet<String>,
    /// path -> the next components that lead to a known id.
    children: HashMap<String, HashSet<String>>,
}

impl Known {
    fn of(ids: &[NamespaceId]) -> Self {
        let mut known = Self {
            ids: ids.iter().map(|id| id.0.clone()).collect(),
            children: HashMap::new(),
        };
        for id in ids {
            let mut at = 0;
            while let Some(slash) = id.0[at..].find('/') {
                let parent = &id.0[..at + slash];
                let rest = &id.0[at + slash + 1..];
                let child = rest.split('/').next().unwrap_or(rest);
                known
                    .children
                    .entry(parent.to_string())
                    .or_default()
                    .insert(child.to_string());
                at += slash + 1;
            }
        }
        known
    }

    /// Whether `<parent>/<child>` is, or leads to, a known namespace.
    fn leads_to_namespace(&self, parent: &str, child: &str) -> bool {
        self.children
            .get(parent)
            .is_some_and(|children| children.contains(child))
    }

    /// Whether namespaces live under the top-level directory `top`, other
    /// than as shards of a bare namespace of that name.
    fn is_tenant(&self, top: &str) -> bool {
        self.children.get(top).is_some_and(|children| {
            children.iter().any(|child| {
                kosha_core::sharding::shard_index_of(&NamespaceId(format!("{top}/{child}")))
                    .is_none()
            })
        })
    }
}

/// Deletion records by what they cover; the newest wins.
#[derive(Default)]
struct Deletions {
    namespaces: HashMap<String, DeletionRecord>,
    tenants: HashMap<String, DeletionRecord>,
}

impl Deletions {
    fn of(records: Vec<DeletionRecord>) -> Self {
        let mut deletions = Self::default();
        // Oldest first, so a later record of the same subject replaces it.
        for record in records {
            let map = match record.scope {
                DeletionScope::Namespace => &mut deletions.namespaces,
                DeletionScope::Tenant => &mut deletions.tenants,
            };
            map.insert(record.subject.clone(), record);
        }
        deletions
    }

    fn covering(&self, tenant: &str, namespace: &str) -> Option<&DeletionRecord> {
        self.namespaces
            .get(&prefix_of(tenant, namespace))
            .or_else(|| self.tenants.get(tenant).filter(|_| !tenant.is_empty()))
    }

    fn knows_tenant(&self, tenant: &str) -> bool {
        self.tenants.contains_key(tenant)
            || self
                .namespaces
                .values()
                .any(|record| record.tenant_id.as_deref() == Some(tenant))
    }
}

/// List `root`'s prefix once and class every object of the root and its
/// shards.
fn scan_root(
    root: &str,
    members: &[Member],
    known: &Known,
    branches: &References,
    inventory: &dyn ObjectInventory,
) -> Result<Storage, KoshaError> {
    let empty_root = Member::of(&NamespaceId(root.to_string()), None);
    let root_member = members
        .iter()
        .find(|member| member.id == root)
        .unwrap_or(&empty_root);
    let shards: HashMap<&str, &Member> = members
        .iter()
        .filter(|member| member.id != root)
        .filter_map(|member| Some((member.id.strip_prefix(root)?.strip_prefix('/')?, member)))
        .collect();
    let dir = format!("{root}/");
    let mut storage = Storage::default();
    inventory.walk(root, &mut |path, size| {
        let Some(rest) = path.strip_prefix(&dir) else {
            return;
        };
        if let Some((first, after)) = rest.split_once('/') {
            if let Some(shard) = shards.get(first) {
                storage.add(shard.classify(branches, after), size);
                return;
            }
            // Another namespace's prefix (a tenant's namespace under a bare
            // namespace of the tenant's name): it has its own row.
            if known.leads_to_namespace(root, first) {
                return;
            }
        }
        storage.add(root_member.classify(branches, rest), size);
    })?;
    Ok(storage)
}

/// Everything under a prefix no manifest covers. A branch's manifest can
/// still list objects here: deleting a branch keeps, under its own prefix,
/// the segments and bitmaps a descendant branch inherited from it
/// (`AppState::delete_branch`). Those are referenced; the rest is orphaned.
fn scan_unknown(
    prefix: &str,
    branches: &References,
    inventory: &dyn ObjectInventory,
) -> Result<Storage, KoshaError> {
    // No manifest: nothing is owned or retired, so `classify` only asks
    // the branch references.
    let root = Member::of(&NamespaceId(prefix.to_string()), None);
    let dir = format!("{prefix}/");
    let mut storage = Storage::default();
    inventory.walk(prefix, &mut |path, size| {
        let Some(rest) = path.strip_prefix(&dir) else {
            return;
        };
        let class = match rest.split_once('/') {
            // A shard of the namespace that was here.
            Some((first, after))
                if kosha_core::sharding::shard_index_of(&NamespaceId(format!(
                    "{prefix}/{first}"
                )))
                .is_some() =>
            {
                Member::of(&NamespaceId(format!("{prefix}/{first}")), None)
                    .classify(branches, after)
            }
            _ => root.classify(branches, rest),
        };
        storage.add(class, size);
    })?;
    Ok(storage)
}

/// `(tenant, namespace) -> (hour start, bytes)` of a `stored_bytes` snapshot.
type Snapshots = HashMap<(String, String), (u64, u64)>;

/// The latest `stored_bytes` snapshot of each namespace inside the window.
fn latest_snapshots(usage: &dyn UsageStore, options: &Options) -> Result<Snapshots, KoshaError> {
    // Rounded down to the start of an hour: snapshots are stamped with the
    // start of theirs, so a window of exactly one hour opened at 07:02
    // would otherwise begin after the 06:00 snapshot while the 07:00 one
    // is not taken yet.
    let from_ms = options.now_ms.saturating_sub(options.max_snapshot_age_ms) / HOUR_MS * HOUR_MS;
    let to_ms = options.now_ms.saturating_add(1);
    let mut latest = Snapshots::new();
    for bucket in usage.hourly(options.tenant.as_deref(), from_ms, to_ms)? {
        if bucket.meter != meter::STORED_BYTES {
            continue;
        }
        let entry = latest
            .entry((bucket.tenant, bucket.namespace))
            .or_insert((bucket.hour_start_ms, bucket.quantity));
        if bucket.hour_start_ms >= entry.0 {
            *entry = (bucket.hour_start_ms, bucket.quantity);
        }
    }
    Ok(latest)
}

fn judge(
    tenant: String,
    namespace: String,
    control_plane: ControlPlane,
    deletion: Option<&DeletionRecord>,
    snapshot: Option<(u64, u64)>,
    storage: Storage,
    options: &Options,
) -> NamespaceReport {
    let compared = snapshot.map(|(_, bytes)| {
        let difference = i128::from(storage.referenced_bytes) - i128::from(bytes);
        let difference = difference.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64;
        (difference, options.allowed(storage.referenced_bytes, bytes))
    });
    // The byte floor of the tolerance is for two measurements of the same
    // segments taken minutes apart. A snapshot of something against no
    // referenced bytes at all is not that: an `unknown` prefix (whose
    // referenced bytes are always 0) holding untracked objects, or a live
    // namespace whose prefix is empty. Those never read `match`, however
    // small the snapshot.
    let nothing_referenced =
        snapshot.is_some_and(|(_, bytes)| bytes > 0) && storage.referenced_bytes == 0;
    let has_storage = storage.objects > 0;
    // A namespace can leave the control plane without a deletion record
    // (branch deletion, `migrate-namespace`). Its last snapshots stay in
    // the window for a while and a descendant branch can keep objects under
    // its prefix; neither is a fault. What would be one is a writer that
    // still meters it, and that shows as a snapshot stamped with the
    // current hour, so only an earlier one (or none) is passed here.
    let hour_start_ms = options.now_ms / HOUR_MS * HOUR_MS;
    let left_quietly =
        control_plane == ControlPlane::Unknown && snapshot.is_none_or(|(at, _)| at < hour_start_ms);
    let only_branch_held = has_storage && storage.objects == storage.referenced_unmetered_objects;
    // A snapshot is stamped with the start of its hour, so one taken in the
    // hour of the deletion (before it) is never later than the request.
    let snapshot_after_deletion = matches!(
        (snapshot, deletion),
        (Some((at, _)), Some(record)) if at > record.requested_at_ms
    );
    let status = match (control_plane, compared) {
        (ControlPlane::Deleted, _) if has_storage => Status::DeletedWithStorage,
        (ControlPlane::Deleted, _) if snapshot_after_deletion => Status::SnapshotAfterDeletion,
        (ControlPlane::Deleted, _) => Status::Deleted,
        _ if left_quietly && only_branch_held => Status::HeldByBranch,
        (_, Some(_)) if left_quietly && !has_storage => Status::Removed,
        (_, None) if has_storage => Status::StorageWithoutSnapshot,
        (_, None) => Status::Empty,
        (_, Some((difference, allowed)))
            if !nothing_referenced && difference.unsigned_abs() <= allowed =>
        {
            Status::Match
        }
        (_, Some(_)) if has_storage => Status::Mismatch,
        (_, Some(_)) => Status::SnapshotWithoutStorage,
    };
    NamespaceReport {
        tenant,
        namespace,
        control_plane,
        deletion: deletion.map(DeletionRef::of),
        snapshot_bytes: snapshot.map(|(_, bytes)| bytes),
        snapshot_at_ms: snapshot.map(|(at, _)| at),
        storage,
        difference_bytes: compared.map(|(difference, _)| difference),
        allowed_difference_bytes: compared.map(|(_, allowed)| allowed),
        status,
        within_tolerance: status.within_tolerance(),
    }
}

/// Whether a top-level name is reserved for internal prefixes (`_wal`,
/// `_usage`, `_warmset`) rather than a tenant: new tenant ids cannot start
/// with `_` or `.`.
fn is_internal_name(name: &str) -> bool {
    name.starts_with('_') || name.starts_with('.')
}

/// Compare the stored-bytes snapshots with object storage. `Err` when the
/// control store, the usage store or a listing could not be read: a partial
/// report would flag (or clear) namespaces for the wrong reason.
pub(crate) fn reconcile(
    control: &dyn ControlView,
    usage: &dyn UsageStore,
    inventory: &dyn ObjectInventory,
    options: &Options,
) -> Result<Report, String> {
    let control_error = |e: KoshaError| format!("control store: {e}");
    let listing_error = |prefix: &str, e: KoshaError| format!("listing {prefix}/: {e}");
    let mut warnings = Vec::new();

    let ids = control.namespaces().map_err(control_error)?;
    let known = Known::of(&ids);
    let in_scope = |tenant: &str| options.tenant.as_deref().is_none_or(|only| only == tenant);

    // Roots in scope, each with its shards.
    let mut roots: BTreeMap<String, Vec<NamespaceId>> = BTreeMap::new();
    for id in &ids {
        let root = shard_root(id);
        if in_scope(&usage::usage_identity(id).0) {
            roots.entry(root.to_string()).or_default().push(id.clone());
        }
    }

    // What branch manifests reference (#208): a branch's segments can live
    // under another namespace's prefix. A branch is a sibling of its
    // parent, so a tenant's branches are among that tenant's namespaces.
    let mut branches = References::default();
    for id in roots.values().flatten() {
        if let Some(manifest) = control.manifest(id).map_err(control_error)? {
            if manifest.branch.is_some() {
                branches
                    .add(id, &manifest)
                    .map_err(|e| format!("branch references of {}: {e}", id.0))?;
            }
        }
    }

    let mut snapshots =
        latest_snapshots(usage, options).map_err(|e| format!("usage store: {e}"))?;
    let deletions = match control.deletions(DELETION_RECORD_LIMIT) {
        Ok(records) => {
            if records.len() >= DELETION_RECORD_LIMIT as usize {
                warnings.push(format!(
                    "only the oldest {DELETION_RECORD_LIMIT} deletion records were read; a \
                     namespace deleted since may be reported as unknown"
                ));
            }
            Deletions::of(records)
        }
        Err(e) => {
            warnings.push(format!(
                "deletion records could not be read, so deleted namespaces are reported as \
                 unknown: {e}"
            ));
            Deletions::default()
        }
    };

    let mut rows: BTreeMap<(String, String), NamespaceReport> = BTreeMap::new();

    // 1. Every namespace the control plane lists.
    for (root, member_ids) in &roots {
        let mut members = Vec::with_capacity(member_ids.len());
        for id in member_ids {
            let manifest = control.manifest(id).map_err(control_error)?;
            members.push(Member::of(id, manifest.as_ref()));
        }
        let storage = scan_root(root, &members, &known, &branches, inventory)
            .map_err(|e| listing_error(root, e))?;
        let (tenant, namespace) = usage::usage_identity(&NamespaceId(root.clone()));
        let snapshot = snapshots.remove(&(tenant.clone(), namespace.clone()));
        rows.insert(
            (tenant.clone(), namespace.clone()),
            judge(
                tenant,
                namespace,
                ControlPlane::Live,
                None,
                snapshot,
                storage,
                options,
            ),
        );
    }

    // 2. Storage under prefixes the control plane does not list.
    let mut unknown: Vec<(String, String)> = Vec::new();
    let mut unattributed: Vec<Unattributed> = Vec::new();
    let mut skipped_prefixes = Vec::new();
    let mut not_listed = 0u64;
    let stray = |prefix: &str, files: (u64, u64), out: &mut Vec<Unattributed>| {
        if files.0 > 0 {
            out.push(Unattributed {
                prefix: prefix.to_string(),
                objects: files.0,
                bytes: files.1,
            });
        }
    };
    let mut tops: Vec<String> = Vec::new();
    let mut root_files = (0u64, 0u64);
    inventory
        .children("", &mut |child| match child {
            Child::Dir(name) => tops.push(name.to_string()),
            Child::File(_, size) => {
                root_files = (root_files.0 + 1, root_files.1.saturating_add(size))
            }
        })
        .map_err(|e| listing_error("", e))?;
    tops.sort();
    if options.tenant.is_none() {
        stray("", root_files, &mut unattributed);
    }
    for top in tops {
        // A bare namespace was listed above, with everything under it; a
        // tenant filter leaves every other tenant's directory alone.
        if known.ids.contains(&top) || !in_scope(&top) {
            continue;
        }
        let is_tenant = known.is_tenant(&top) || deletions.knows_tenant(&top);
        if !is_tenant {
            if deletions.namespaces.contains_key(&top) {
                // A deleted bare namespace.
                if options.tenant.is_none() {
                    unknown.push((String::new(), top));
                }
                continue;
            }
            if is_internal_name(&top) {
                skipped_prefixes.push(top);
                continue;
            }
        }
        // A tenant's directory (or a prefix nothing explains, read as one):
        // each child is a namespace prefix.
        let mut files = (0u64, 0u64);
        inventory
            .children(&top, &mut |child| match child {
                Child::Dir(name) => {
                    if known.ids.contains(&format!("{top}/{name}")) {
                        return;
                    }
                    if unknown.len() < MAX_UNKNOWN_PREFIXES {
                        unknown.push((top.clone(), name.to_string()));
                    } else {
                        not_listed += 1;
                    }
                }
                Child::File(_, size) => files = (files.0 + 1, files.1.saturating_add(size)),
            })
            .map_err(|e| listing_error(&top, e))?;
        stray(&format!("{top}/"), files, &mut unattributed);
    }
    // 3. Snapshots of namespaces the control plane does not list and no
    //    listing above came across.
    let mut snapshot_only: Vec<(String, String)> = snapshots.keys().cloned().collect();
    snapshot_only.sort();
    for key in snapshot_only {
        if !unknown.contains(&key) {
            unknown.push(key);
        }
    }
    for (tenant, namespace) in unknown {
        let prefix = prefix_of(&tenant, &namespace);
        let storage =
            scan_unknown(&prefix, &branches, inventory).map_err(|e| listing_error(&prefix, e))?;
        let snapshot = snapshots.remove(&(tenant.clone(), namespace.clone()));
        if storage.objects == 0 && snapshot.is_none() {
            continue;
        }
        let deletion = deletions.covering(&tenant, &namespace);
        let control_plane = if deletion.is_some() {
            ControlPlane::Deleted
        } else {
            ControlPlane::Unknown
        };
        rows.insert(
            (tenant.clone(), namespace.clone()),
            judge(
                tenant,
                namespace,
                control_plane,
                deletion,
                snapshot,
                storage,
                options,
            ),
        );
    }
    if not_listed > 0 {
        warnings.push(format!(
            "{not_listed} more prefix(es) unknown to the control plane were not summed (the \
             report lists at most {MAX_UNKNOWN_PREFIXES})"
        ));
    }

    let namespaces: Vec<NamespaceReport> = rows.into_values().collect();
    let mut totals = Totals::default();
    for row in &namespaces {
        totals.snapshot_bytes = totals
            .snapshot_bytes
            .saturating_add(row.snapshot_bytes.unwrap_or(0));
        totals.storage.absorb(&row.storage);
    }
    let out_of_tolerance = namespaces
        .iter()
        .filter(|row| !row.within_tolerance)
        .count();
    Ok(Report {
        generated_at_ms: options.now_ms,
        tenant: options.tenant.clone(),
        storage_backend: inventory.backend(),
        tolerance: options.tolerance,
        tolerance_bytes: options.tolerance_bytes,
        max_snapshot_age_seconds: options.max_snapshot_age_ms / 1_000,
        within_tolerance: out_of_tolerance == 0 && not_listed == 0,
        namespaces_checked: namespaces.len(),
        namespaces_out_of_tolerance: out_of_tolerance,
        totals,
        namespaces,
        unattributed,
        skipped_prefixes,
        unknown_prefixes_not_listed: not_listed,
        warnings,
    })
}

// ─── Admin route ─────────────────────────────────────────────────────────────

impl AppState {
    /// The object storage a reconciliation lists: S3 when configured,
    /// otherwise the local data directory.
    fn with_usage_inventory<T>(&self, f: impl FnOnce(&dyn ObjectInventory) -> T) -> T {
        #[cfg(feature = "s3")]
        if let Some(ref s3) = self.s3_storage {
            return f(s3);
        }
        f(&LocalInventory(self.data_dir.clone()))
    }
}

/// The target's query parameters, URL-decoded.
fn query_params(request_line: &str) -> Vec<(String, String)> {
    let Some(target) = request_line.split_whitespace().nth(1) else {
        return Vec::new();
    };
    let Some((_, query)) = target.split_once('?') else {
        return Vec::new();
    };
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (url_decode(key), url_decode(value))
        })
        .collect()
}

/// `GET /v1/admin/usage/reconcile[?tenant=&tolerance=&tolerance_bytes=&max_snapshot_age=]`:
/// the reconciliation report. Answers 200 whether or not everything is
/// within tolerance (`within_tolerance` says); 503 when it could not run.
///
/// Read-only, and it runs on the pod that receives it: every pod reads the
/// same control store, usage store and bucket. This pod's own spooled
/// events are delivered first, as for `GET /v1/admin/usage`.
pub(crate) fn handle_admin_reconcile(request_line: &str, state: &AppState) -> String {
    let Some(meter) = state.usage.as_ref() else {
        return json_error(
            404,
            "usage metering is off on this pod (KOSHA_USAGE_METERING=on enables it)",
        );
    };
    let mut options = match Options::from_env(unix_millis()) {
        Ok(options) => options,
        Err(e) => return json_error(500, &e),
    };
    for (key, value) in query_params(request_line) {
        match options.set(&key, &value) {
            Ok(true) => {}
            Ok(false) => return json_error(400, &format!("unknown parameter '{key}'")),
            Err(e) => return json_error(400, &format!("invalid '{key}': {e}")),
        }
    }
    let _ = meter.flush();
    let store = meter.store();
    let report = state.with_usage_inventory(|inventory| {
        reconcile(&state.controller, store.as_ref(), inventory, &options)
    });
    match report {
        Ok(report) => json_ok(&report),
        Err(e) => {
            eprintln!("WARN: usage reconciliation failed: {e}");
            json_error(503, &format!("usage reconciliation could not run: {e}"))
        }
    }
}

// ─── Command line ────────────────────────────────────────────────────────────

const USAGE: &str = "usage: kosha-server reconcile-usage [--tenant <tenant>] \
[--tolerance <fraction>] [--tolerance-bytes <bytes>] [--max-snapshot-age <duration>] [--pretty]

Compares the stored_bytes usage snapshots with object storage and prints the
report as JSON. Exit status: 0 within tolerance, 1 out of tolerance, 2 the
run failed. Reads DATABASE_URL, the KOSHA_S3_* variables (or KOSHA_DATA_DIR
without a bucket) and KOSHA_USAGE_RECONCILE_*.";

/// Exit status of a run that found something out of tolerance.
const EXIT_OUT_OF_TOLERANCE: i32 = 1;
/// Exit status of a run that could not finish: nothing was compared.
const EXIT_FAILED: i32 = 2;

#[derive(Debug, PartialEq)]
pub(crate) struct Args {
    options: Options,
    pretty: bool,
}

/// Parse `reconcile-usage`'s arguments (after the subcommand) over the
/// environment's defaults. `Ok(None)` for `--help`.
pub(crate) fn parse_args(
    args: impl IntoIterator<Item = String>,
    mut options: Options,
) -> Result<Option<Args>, String> {
    let mut pretty = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(None);
        }
        if arg == "--pretty" {
            pretty = true;
            continue;
        }
        let Some(flag) = arg.strip_prefix("--") else {
            return Err(format!("unexpected argument '{arg}'\n{USAGE}"));
        };
        let (name, value) = match flag.split_once('=') {
            Some((name, value)) => (name, value.to_string()),
            None => (
                flag,
                args.next()
                    .ok_or_else(|| format!("--{flag} needs a value\n{USAGE}"))?,
            ),
        };
        match options.set(&name.replace('-', "_"), &value) {
            Ok(true) => {}
            Ok(false) => return Err(format!("unknown flag '--{name}'\n{USAGE}")),
            Err(e) => return Err(format!("invalid --{name}: {e}")),
        }
    }
    Ok(Some(Args { options, pretty }))
}

/// Run one reconciliation and write the report to `out` as JSON. `Ok` says
/// whether everything was within tolerance.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
pub(crate) fn execute(
    control: &dyn ControlView,
    usage: &dyn UsageStore,
    inventory: &dyn ObjectInventory,
    args: &Args,
    out: &mut dyn std::io::Write,
) -> Result<bool, String> {
    let report = reconcile(control, usage, inventory, &args.options)?;
    let json = if args.pretty {
        serde_json::to_string_pretty(&report)
    } else {
        serde_json::to_string(&report)
    }
    .map_err(|e| format!("encoding the report: {e}"))?;
    writeln!(out, "{json}").map_err(|e| format!("writing the report: {e}"))?;
    // The summary goes to stderr so stdout stays one JSON document.
    for row in report.namespaces.iter().filter(|row| !row.within_tolerance) {
        eprintln!(
            "out of tolerance: {} status={} snapshot_bytes={} referenced_bytes={} \
             orphaned_bytes={}",
            prefix_of(&row.tenant, &row.namespace),
            serde_json::to_value(row.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default(),
            row.snapshot_bytes
                .map_or_else(|| "none".to_string(), |bytes| bytes.to_string()),
            row.storage.referenced_bytes,
            row.storage.orphaned_bytes,
        );
    }
    for warning in &report.warnings {
        eprintln!("WARN: {warning}");
    }
    eprintln!(
        "reconcile-usage: {} namespace(s), {} out of tolerance, snapshots {} bytes, \
         referenced {} bytes, orphaned {} bytes",
        report.namespaces_checked,
        report.namespaces_out_of_tolerance,
        report.totals.snapshot_bytes,
        report.totals.storage.referenced_bytes,
        report.totals.storage.orphaned_bytes,
    );
    Ok(report.within_tolerance)
}

pub(crate) fn exit_code(result: Result<bool, String>) -> i32 {
    match result {
        Ok(true) => 0,
        Ok(false) => EXIT_OUT_OF_TOLERANCE,
        Err(error) => {
            eprintln!("reconcile-usage failed: {error}");
            EXIT_FAILED
        }
    }
}

/// `kosha-server reconcile-usage ...`: run and return the exit code.
pub(crate) fn main(args: impl IntoIterator<Item = String>) -> i32 {
    exit_code(run(args))
}

fn run(args: impl IntoIterator<Item = String>) -> Result<bool, String> {
    let Some(args) = parse_args(args, Options::from_env(unix_millis())?)? else {
        println!("{USAGE}");
        return Ok(true);
    };
    #[cfg(feature = "postgres")]
    {
        let url = std::env::var("DATABASE_URL")
            .ok()
            .filter(|url| !url.is_empty())
            .ok_or(
                "DATABASE_URL is not set: reconcile-usage reads the namespaces, deletion \
                 records and usage events from Postgres",
            )?;
        // Same TLS rule as the server (#343), before the first connection.
        crate::internal_tls::check_database_url_env()?;
        let control: Mutex<Box<dyn ControlStore>> =
            Mutex::new(Box::new(kosha_control::PgStore::connect_with_retry(&url)?));
        let usage = kosha_control::usage::PgUsageStore::new(&url)?;
        let mut stdout = std::io::stdout();
        #[cfg(feature = "s3")]
        if let Some(config) = crate::s3_storage::S3Config::try_from_env()? {
            let s3 = connect_s3(config)?;
            return execute(&control, &usage, &s3, &args, &mut stdout);
        }
        // Without a bucket the segments are on this node's disk. A CronJob
        // pod has no such disk: refuse rather than report every namespace
        // as stored nowhere.
        let data_dir = PathBuf::from(
            std::env::var("KOSHA_DATA_DIR").unwrap_or_else(|_| "/var/lib/kosha/data".into()),
        );
        if !data_dir.is_dir() {
            return Err(format!(
                "no object storage to list: KOSHA_S3_BUCKET is not set (or this binary has no \
                 `s3` feature) and KOSHA_DATA_DIR {} is not a directory",
                data_dir.display()
            ));
        }
        execute(
            &control,
            &usage,
            &LocalInventory(data_dir),
            &args,
            &mut stdout,
        )
    }
    #[cfg(not(feature = "postgres"))]
    {
        let _ = args;
        Err(
            "this binary was built without the `postgres` feature; reconcile-usage reads the \
             namespaces and usage events from Postgres"
                .into(),
        )
    }
}

#[cfg(all(feature = "s3", feature = "postgres"))]
fn connect_s3(config: crate::s3_storage::S3Config) -> Result<crate::s3_storage::S3Storage, String> {
    // Nothing is downloaded; the storage type just needs a local root.
    let local = std::env::temp_dir().join(format!("kosha-reconcile-usage-{}", std::process::id()));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    rt.block_on(crate::s3_storage::S3Storage::new(local, config))
        .map_err(|e| format!("S3: {e}"))
}
