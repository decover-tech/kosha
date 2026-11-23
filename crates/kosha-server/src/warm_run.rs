//! Usage-driven warming (#213): the `AppState` half — recording reads on
//! the search path, the boot pass, the re-warm thread, warm-set
//! persistence and `GET /v1/admin/warmup-status`. Policy and bookkeeping
//! live in [`crate::warm`].
//!
//! Everything here fails open. A warming error is logged and reported; a
//! segment warming did not fetch is hydrated by the search that needs it,
//! exactly as before warming existed. Only the boot pass gates `/readyz`.

use super::*;
use crate::warm::{
    AccessConfig, AccessTracker, Candidate, FileClass, NamespaceRates, PlanItem, Source,
    WarmConfig, WarmLedger, WarmReport, WarmSet,
};
use std::collections::{HashSet, VecDeque};
use std::path::Path;

/// Re-warm reports kept for the status endpoint.
const REWARM_REPORTS_KEPT: usize = 16;

/// Warming state on [`AppState`].
pub(crate) struct Warming {
    pub cfg: WarmConfig,
    pub tracker: AccessTracker,
    pub ledger: WarmLedger,
    pub boot: Mutex<Option<WarmReport>>,
    pub rewarms: Mutex<VecDeque<WarmReport>>,
    pub persist: Mutex<PersistStatus>,
    /// Namespaces the boot pass is warming (explicit plus usage), for
    /// `/readyz` while it runs.
    pub boot_namespaces: Mutex<Vec<String>>,
    /// The manifest the warmer last saw per namespace: the re-warm tick's
    /// baseline for "which segments are new". It cannot be the local
    /// manifest before its own refresh, because the manifest refresher
    /// (#232) installs new versions on its own schedule and would hide the
    /// segments they added.
    pub observed: Mutex<HashMap<String, Arc<kosha_core::Manifest>>>,
}

impl Warming {
    pub fn new(cfg: WarmConfig, access: AccessConfig) -> Self {
        Self {
            cfg,
            tracker: AccessTracker::new(access),
            ledger: WarmLedger::default(),
            boot: Mutex::new(None),
            rewarms: Mutex::new(VecDeque::new()),
            persist: Mutex::new(PersistStatus::default()),
            boot_namespaces: Mutex::new(Vec::new()),
            observed: Mutex::new(HashMap::new()),
        }
    }

    pub fn from_env() -> Self {
        Self::new(WarmConfig::from_env(), AccessConfig::from_env())
    }
}

/// Warm-set persistence, for the status endpoint.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub(crate) struct PersistStatus {
    /// Namespaces in the warm set loaded at boot (`None`: none found).
    pub loaded_namespaces: Option<usize>,
    pub loaded_written_at_unix_ms: Option<u64>,
    pub last_written_unix_ms: Option<u64>,
    pub last_written_namespaces: usize,
    pub last_error: Option<String>,
}

/// What one warm batch did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BatchOutcome {
    /// Files downloaded (already-local files are not counted).
    pub files: u64,
    pub bytes: u64,
    /// Files of the batch still not local afterwards.
    pub missing: usize,
}

/// The object-storage side of warming, behind a trait so the planning,
/// re-warm and persistence logic runs in tests against a directory that
/// stands in for S3 (the same approach as `CompactionStorage`).
pub(crate) trait WarmStorage {
    /// A namespace's objects in object storage: `<segment>/<file>` and
    /// size.
    fn list_objects(&self, ns: &NamespaceId) -> Result<Vec<(String, u64)>, String>;
    /// Make `footer.json` local for every segment `manifest` carries no
    /// footer for, so the pass can plan from footers (see
    /// [`warm_fetches`]). Returns the files and bytes downloaded.
    fn fetch_footers(&self, ns: &NamespaceId, manifest: &kosha_core::Manifest) -> (u64, u64);
    /// Make `class` local for `segments` of `ns`. Holds a hydration permit
    /// for at most this one batch.
    fn warm_batch(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        class: FileClass,
        segments: &[String],
    ) -> BatchOutcome;
    /// The role's persisted warm set; `Ok(None)` when there is none yet.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    fn load_warm_set(&self, role: &str) -> Result<Option<WarmSet>, String>;
    fn store_warm_set(&self, role: &str, set: &WarmSet) -> Result<(), String>;
}

/// Group a namespace listing (`<segment>/<file>`, size) into bytes per
/// segment and [`FileClass`]. Deeper keys and unclassified files
/// (`_COMMIT`) are ignored.
pub(crate) fn group_class_sizes(
    objects: impl IntoIterator<Item = (String, u64)>,
) -> HashMap<(String, FileClass), u64> {
    let mut out = HashMap::new();
    for (relative, size) in objects {
        let Some((segment, file)) = relative.split_once('/') else {
            continue;
        };
        if segment.is_empty() || file.contains('/') {
            continue;
        }
        if let Some(class) = FileClass::of_file(file) {
            *out.entry((segment.to_string(), class)).or_insert(0) += size;
        }
    }
    out
}

/// Whether warm-up downloads `file` of `segment`: the one decision behind
/// both the bytes a pass plans for ([`warm_class_sizes`]) and what its
/// batches fetch (`AppState::warm_batch_s3`), taken from the same footer —
/// `manifest`'s copy, which [`AppState::run_warm_pass`] loads before
/// planning. Planning from anything else charged the budget for files
/// that were never downloaded and warmed fewer segments than it could.
///
/// - `doc_vectors.bin` only where readers need it whole
///   ([`kosha_segment::doc_vectors_kept_whole`]); an f32 sidecar is read by
///   range. With no footer it is not fetched, as the fetch has always done.
/// - Filter files never: the scaffolding probe is unfiltered, so the
///   scoring batch leaves them for the first filtered search.
pub(crate) fn warm_fetches(manifest: &kosha_core::Manifest, segment: &str, file: &str) -> bool {
    if file == kosha_segment::DOC_VECTORS_FILE {
        return manifest
            .segment_footer(&kosha_core::SegmentId(segment.to_string()))
            .is_some_and(kosha_segment::doc_vectors_kept_whole);
    }
    !(file == kosha_core::FILTERS_BIN_FILE || kosha_core::is_filter_column_file(file))
}

/// [`group_class_sizes`] of the files warm-up downloads ([`warm_fetches`]).
pub(crate) fn warm_class_sizes(
    objects: Vec<(String, u64)>,
    manifest: &kosha_core::Manifest,
) -> HashMap<(String, FileClass), u64> {
    group_class_sizes(objects.into_iter().filter(|(relative, _)| {
        relative
            .split_once('/')
            .is_none_or(|(segment, file)| warm_fetches(manifest, segment, file))
    }))
}

/// The objects (`<segment>/<file>`, size) of a local namespace directory.
pub(crate) fn local_objects(ns_dir: &Path) -> Vec<(String, u64)> {
    let mut objects = Vec::new();
    if let Ok(segments) = std::fs::read_dir(ns_dir) {
        for seg in segments.flatten() {
            let Ok(files) = std::fs::read_dir(seg.path()) else {
                continue;
            };
            let seg_name = seg.file_name().to_string_lossy().into_owned();
            for f in files.flatten() {
                if let Ok(m) = f.metadata() {
                    if m.is_file() {
                        objects.push((
                            format!("{seg_name}/{}", f.file_name().to_string_lossy()),
                            m.len(),
                        ));
                    }
                }
            }
        }
    }
    objects
}

impl WarmStorage for AppState {
    fn list_objects(&self, ns: &NamespaceId) -> Result<Vec<(String, u64)>, String> {
        #[cfg(feature = "s3")]
        if let Some(ref s3) = self.s3_storage {
            return s3
                .list_namespace_objects(&ns.0)
                .map_err(|e| format!("S3 size listing for '{}' failed: {e}", ns.0));
        }
        Ok(local_objects(&self.data_dir.join(&ns.0)))
    }

    fn fetch_footers(&self, ns: &NamespaceId, manifest: &kosha_core::Manifest) -> (u64, u64) {
        #[cfg(feature = "s3")]
        if self.s3_storage.is_some() {
            let (files, bytes) =
                self.fetch_missing_footers(ns, manifest, &RequestHydration::unlimited());
            return (files as u64, bytes);
        }
        // Local-only mode: every `footer.json` is already local.
        let _ = (ns, manifest);
        (0, 0)
    }

    fn warm_batch(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        class: FileClass,
        segments: &[String],
    ) -> BatchOutcome {
        #[cfg(feature = "s3")]
        if self.s3_storage.is_some() {
            return self.warm_batch_s3(ns, manifest, class, segments);
        }
        // Local-only mode: the data directory is the only copy, so there is
        // nothing to fetch.
        let _ = (ns, manifest, class, segments);
        BatchOutcome::default()
    }

    fn load_warm_set(&self, role: &str) -> Result<Option<WarmSet>, String> {
        let path = crate::warm::warm_set_path(role);
        #[cfg(feature = "s3")]
        if let Some(ref s3) = self.s3_storage {
            // List first: `read_remote` cannot tell a missing object (the
            // normal first boot) from a failed request.
            let (dir, name) = path.split_once('/').unwrap_or(("", &path));
            let listed = s3
                .list_remote_with_sizes(dir)
                .map_err(|e| format!("listing {dir}: {e}"))?;
            if !listed.iter().any(|(n, _)| n == name) {
                return Ok(None);
            }
            let bytes = s3
                .read_remote(&path)
                .map_err(|e| format!("reading {path}: {e}"))?;
            return serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| format!("parsing {path}: {e}"));
        }
        match std::fs::read(self.data_dir.join(&path)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| format!("parsing {path}: {e}")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("reading {path}: {e}")),
        }
    }

    fn store_warm_set(&self, role: &str, set: &WarmSet) -> Result<(), String> {
        let path = crate::warm::warm_set_path(role);
        let bytes = serde_json::to_vec(set).map_err(|e| e.to_string())?;
        #[cfg(feature = "s3")]
        if let Some(ref s3) = self.s3_storage {
            return s3
                .write(&path, &bytes)
                .map_err(|e| format!("writing {path}: {e}"));
        }
        LocalStorage::new(self.data_dir.clone())
            .write(&path, &bytes)
            .map_err(|e| format!("writing {path}: {e}"))
    }
}

/// One namespace offered to a warm pass.
pub(crate) struct WarmSpec {
    pub ns: NamespaceId,
    pub source: Source,
    /// Restrict to these segments (re-warm: the ones a publish added).
    pub only: Option<HashSet<String>>,
}

impl AppState {
    /// The s3 half of [`WarmStorage::warm_batch`]: the same fetch paths the
    /// static warmup used, plus span promotion (#245) — a segment warmed
    /// whole no longer needs its sparse spans, so they are dropped just as
    /// the background promoters drop them.
    #[cfg(feature = "s3")]
    fn warm_batch_s3(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        class: FileClass,
        segments: &[String],
    ) -> BatchOutcome {
        // A branch's inherited segments live under their home namespace's
        // prefix (#208).
        let seg_paths: Vec<PathBuf> = segments
            .iter()
            .map(|s| manifest.segment_dir(&self.data_dir, ns, &kosha_core::SegmentId(s.clone())))
            .collect();
        let request = RequestHydration::unlimited();
        match class {
            FileClass::Scoring => {
                // A manifest holding just this batch, so the pass (which
                // takes its own hydration permit) covers only these
                // segments. Footers are `Arc`s; deletions are irrelevant to
                // which files are fetched.
                let wanted: HashSet<&str> = segments.iter().map(String::as_str).collect();
                let keep = |id: &kosha_core::SegmentId| wanted.contains(id.0.as_str());
                let subset = kosha_core::Manifest {
                    version: manifest.version,
                    segments: manifest
                        .segments
                        .iter()
                        .filter(|e| keep(&e.segment_id))
                        .cloned()
                        .collect(),
                    segment_footers: manifest
                        .segment_footers
                        .iter()
                        .filter(|(id, _)| keep(id))
                        .map(|(id, f)| (id.clone(), Arc::clone(f)))
                        .collect(),
                    footer_crc32: manifest
                        .footer_crc32
                        .iter()
                        .filter(|(id, _)| keep(id))
                        .map(|(id, c)| (id.clone(), *c))
                        .collect(),
                    wal_checkpoint: manifest.wal_checkpoint,
                    wal_origin: None,
                    tombstones: HashMap::new(),
                    deletion_vectors: HashMap::new(),
                    schema: None,
                    retired: Default::default(),
                    shards: None,
                    branch: None,
                };
                let o = self.hydrate_segments_for_search(
                    ns,
                    &subset,
                    &warmup_probe_query(),
                    &request,
                    None,
                );
                BatchOutcome {
                    files: o.files_fetched as u64,
                    bytes: o.bytes_fetched,
                    missing: o.missing.len(),
                }
            }
            FileClass::Postings => {
                // Takes its own permit for the batch. Needs `inverted.idx`
                // local to tell split segments, which the scoring class,
                // always planned first, provides.
                let all_blobs = kosha_segment::all_posting_blob_files();
                let o = self.ensure_posting_blobs_local(
                    ns,
                    manifest.version,
                    &seg_paths,
                    &all_blobs,
                    &request,
                );
                if self.posting_read_mode == doc_spans::ReadMode::Ranged {
                    for seg in &seg_paths {
                        for blob in &all_blobs {
                            let shard = seg.join(blob);
                            let spans = seg
                                .join(format!("{blob}{}", kosha_segment::span_cache::SPANS_SUFFIX));
                            if spans.exists() && file_present_nonempty(&shard) {
                                self.posting_span_hydrator.finish_promotion(
                                    &self.cache,
                                    &self.span_writes,
                                    &shard,
                                    true,
                                );
                            }
                        }
                    }
                }
                BatchOutcome {
                    files: o.files_fetched as u64,
                    bytes: o.bytes_fetched,
                    missing: o.missing.len(),
                }
            }
            FileClass::DocStore => {
                // `doc_vectors.bin` by the decision the plan counted it by.
                let segment_of: HashMap<&Path, &str> = seg_paths
                    .iter()
                    .map(PathBuf::as_path)
                    .zip(segments.iter().map(String::as_str))
                    .collect();
                let (files, bytes) = {
                    let _permit = self.hydration_semaphore.acquire("warm_doc_store");
                    self.ensure_doc_store_local(&seg_paths, &request, |seg| {
                        segment_of.get(seg).is_some_and(|segment| {
                            warm_fetches(manifest, segment, kosha_segment::DOC_VECTORS_FILE)
                        })
                    })
                };
                let mut missing = 0;
                for seg in &seg_paths {
                    if crate::doc_store_promotion_landed(seg) {
                        self.span_hydrator.finish_promotion(
                            &self.cache,
                            &self.span_writes,
                            seg,
                            true,
                        );
                    } else {
                        missing += 1;
                    }
                }
                BatchOutcome {
                    files: files as u64,
                    bytes,
                    missing,
                }
            }
            FileClass::Vectors => {
                let (mut files, mut bytes) = (0usize, 0u64);
                // Every vector column's files (#204): `vector.idx` +
                // `vector.offsets`, then `vector.<n>.idx` + `vector.<n>.offsets`
                // as each segment's (already warmed) footer lists them.
                let mut by_file: std::collections::BTreeMap<String, Vec<PathBuf>> =
                    std::collections::BTreeMap::new();
                for seg in &seg_paths {
                    for file in kosha_segment::vector_index_files(seg) {
                        by_file.entry(file).or_default().push(seg.clone());
                    }
                }
                {
                    let _permit = self.hydration_semaphore.acquire("warm_vectors");
                    // Offsets sidecars first, as before: they are what an
                    // open reads.
                    let mut order: Vec<(&String, &Vec<PathBuf>)> = by_file.iter().collect();
                    order.sort_by_key(|(file, _)| !file.ends_with(".offsets"));
                    for (file, segs) in order {
                        let (f, b) = self.ensure_files_local(segs, file, &request);
                        files += f;
                        bytes += b;
                    }
                }
                let mut missing = 0;
                for (file, segs) in &by_file {
                    if !file.ends_with(".idx") {
                        continue;
                    }
                    for seg in segs {
                        let idx = seg.join(file);
                        if file_present_nonempty(&idx) {
                            self.posting_span_hydrator.finish_promotion(
                                &self.cache,
                                &self.span_writes,
                                &idx,
                                true,
                            );
                        } else {
                            missing += 1;
                        }
                    }
                }
                BatchOutcome {
                    files: files as u64,
                    bytes,
                    missing,
                }
            }
        }
    }

    /// Count one successful search's reads (#213): the scoring scaffolding
    /// and — when the query has terms / a vector — the postings / vectors
    /// of every manifest segment, the doc store of every segment holding a
    /// page document, and the bytes this search fetched per class.
    pub(crate) fn record_search_access(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        query: &SearchQuery,
        phases: &kosha_query::SearchPhaseStats,
        hydration: &HydrationOutcome,
    ) {
        if !self.warming.cfg.usage {
            return;
        }
        let reads_postings = query.wildcard.is_some()
            || query.match_phrase.is_some()
            || !query.query_text.trim().is_empty();
        let reads_vectors = query.knn.is_some();
        let mut reads: Vec<(&str, FileClass)> = Vec::with_capacity(
            manifest.segments.len() * (1 + reads_postings as usize + reads_vectors as usize)
                + phases.page_segments.len(),
        );
        for entry in &manifest.segments {
            let seg = entry.segment_id.0.as_str();
            reads.push((seg, FileClass::Scoring));
            if reads_postings {
                reads.push((seg, FileClass::Postings));
            }
            if reads_vectors {
                reads.push((seg, FileClass::Vectors));
            }
        }
        for dir in &phases.page_segments {
            if let Some(seg) = dir.file_name().and_then(|n| n.to_str()) {
                // A strong search's transient tail segment (#255) is local,
                // never published and gone once the tail changes.
                if seg.starts_with(crate::unindexed::TAIL_SEGMENT_PREFIX) {
                    continue;
                }
                reads.push((seg, FileClass::DocStore));
            }
        }
        let postings = hydration.postings.ranged_bytes + hydration.postings.whole_bytes;
        let bytes = [
            hydration.bytes_fetched.saturating_sub(postings),
            postings,
            hydration.page.ranged_bytes + hydration.page.whole_bytes,
            hydration.vectors.ranged_bytes + hydration.vectors.whole_bytes,
        ];
        self.warming.tracker.record(&ns.0, &reads, bytes);
    }

    /// Classes to warm for a namespace. Explicit namespaces get everything
    /// the `KOSHA_WARMUP_*` flags allow, as before #213. Usage namespaces
    /// get the scoring scaffolding plus each class they were observed
    /// reading: no point warming the vector index of a namespace nobody
    /// runs kNN against.
    fn warm_classes(&self, source: Source, rates: Option<&NamespaceRates>) -> [bool; 4] {
        let deep = self.warmup_posting_blobs;
        match source {
            Source::Explicit => [true, deep, deep, deep],
            Source::Usage => {
                let h = rates.map(|r| r.hits).unwrap_or_default();
                [
                    true,
                    deep && h.postings > 0.0,
                    deep && h.doc_store > 0.0,
                    deep && h.vectors > 0.0,
                ]
            }
        }
    }

    /// `KOSHA_WARMUP_NAMESPACES` first (always included), then hot
    /// namespaces from the tracker, deduplicated.
    fn hot_specs(&self) -> Vec<(NamespaceId, Source)> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for ns in &self.warmup_namespaces {
            if seen.insert(ns.clone()) {
                out.push((NamespaceId(ns.clone()), Source::Explicit));
            }
        }
        if self.warming.cfg.usage {
            let limits = &self.warming.cfg.limits;
            for n in self
                .warming
                .tracker
                .hot_namespaces(limits.top_namespaces, limits.min_score)
            {
                if seen.insert(n.namespace.clone()) {
                    out.push((NamespaceId(n.namespace), Source::Usage));
                }
            }
        }
        out
    }

    /// Plan and run one warm pass over `specs` within `available` bytes.
    ///
    /// Batches are small (8 segments, 2 for doc stores and vector indexes)
    /// and each holds a hydration permit only for itself, so live queries
    /// interleave with warming instead of queueing behind a whole namespace
    /// — the lesson of the original warmup's phase 2.
    pub(crate) fn run_warm_pass(
        &self,
        storage: &dyn WarmStorage,
        trigger: &str,
        specs: Vec<WarmSpec>,
        available: Option<u64>,
    ) -> WarmReport {
        let started = Instant::now();
        let mut report = WarmReport::new(trigger, self.warming.cfg.budget_bytes, available);
        let mut candidates = Vec::new();
        let mut manifests: HashMap<String, Arc<kosha_core::Manifest>> = HashMap::new();
        // Namespaces offered whole (not a re-warm's subset): when every
        // segment's scaffolding lands, the search path's hydration verdict
        // can be recorded, as the static warmup did.
        let mut whole: HashSet<String> = HashSet::new();
        // `footer.json` downloads per namespace, reported after the plan.
        let mut footer_fetches: Vec<(String, u64, u64)> = Vec::new();
        // A sharded namespace (#207) holds its segments in its shards,
        // ordinary namespaces nested under its prefix: warm each of them.
        let specs: Vec<WarmSpec> = specs
            .into_iter()
            .flat_map(|spec| match self.indexer.shard_count(&spec.ns) {
                None => vec![spec],
                Some(shards) => kosha_core::sharding::shard_namespaces(&spec.ns, shards)
                    .into_iter()
                    .enumerate()
                    .map(|(index, shard)| WarmSpec {
                        ns: shard,
                        source: spec.source,
                        // Merged-view ids (`_shard-XXXX/{id}`) back to the
                        // shard's own segment ids.
                        only: spec.only.as_ref().map(|only| {
                            only.iter()
                                .filter_map(|id| {
                                    kosha_core::sharding::split_merged_segment_id(
                                        &kosha_core::SegmentId(id.clone()),
                                    )
                                    .filter(|(i, _)| *i as usize == index)
                                    .map(|(_, local)| local.0)
                                })
                                .collect()
                        }),
                    })
                    .collect(),
            })
            .collect();
        for spec in specs {
            let Some(mut manifest) = self.indexer.manifest_shared(&spec.ns) else {
                report
                    .errors
                    .push(format!("namespace '{}' not found; skipped", spec.ns.0));
                continue;
            };
            if manifest.segments.is_empty() {
                continue;
            }
            // Plan from footers (`warm_fetches`). A query pod's manifest
            // comes without them (#241) until a search caches them, so a
            // boot pass loads them first: the footer phase every first
            // search runs anyway, one `footer.json` per segment.
            let (files, bytes) = storage.fetch_footers(&spec.ns, &manifest);
            if files > 0 {
                footer_fetches.push((spec.ns.0.clone(), files, bytes));
            }
            if !self.remember_local_footers(&spec.ns, &manifest).is_empty() {
                if let Some(with_footers) = self.indexer.manifest_shared(&spec.ns) {
                    manifest = with_footers;
                }
            }
            let sizes = match storage.list_objects(&spec.ns) {
                Ok(objects) => warm_class_sizes(objects, &manifest),
                Err(e) => {
                    report.errors.push(format!("{e}; '{}' skipped", spec.ns.0));
                    continue;
                }
            };
            let segments: Vec<String> = manifest
                .segments
                .iter()
                .map(|e| e.segment_id.0.clone())
                .filter(|s| spec.only.as_ref().is_none_or(|only| only.contains(s)))
                .collect();
            let rates = self.warming.tracker.namespace(&spec.ns.0);
            candidates.push(Candidate {
                namespace: spec.ns.0.clone(),
                source: spec.source,
                classes: self.warm_classes(spec.source, rates.as_ref()),
                segments,
                sizes,
                rates,
            });
            if spec.only.is_none() {
                whole.insert(spec.ns.0.clone());
            }
            manifests.insert(spec.ns.0, manifest);
        }

        let plan = crate::warm::plan(&candidates, available);
        report.add_plan(&plan);
        for (ns_name, files, bytes) in footer_fetches {
            report.fetched_files += files;
            report.fetched_bytes += bytes;
            let s = report.summary_mut(&ns_name);
            s.fetched_files += files;
            s.fetched_bytes += bytes;
        }

        // Namespaces whose vector indexes this pass warmed.
        let mut vector_namespaces: Vec<&str> = Vec::new();
        let mut scaffolded: HashMap<&str, usize> = HashMap::new();
        let mut i = 0;
        while i < plan.warm.len() {
            let first: &PlanItem = &plan.warm[i];
            let mut segments = Vec::new();
            let mut j = i;
            while j < plan.warm.len()
                && plan.warm[j].namespace == first.namespace
                && plan.warm[j].class == first.class
                && segments.len() < first.class.batch_len()
            {
                segments.push(plan.warm[j].segment.clone());
                j += 1;
            }
            let ns = NamespaceId(first.namespace.clone());
            let o = storage.warm_batch(&ns, &manifests[&first.namespace], first.class, &segments);
            if first.class == FileClass::Scoring {
                *scaffolded.entry(first.namespace.as_str()).or_insert(0) += segments.len();
                if o.missing > 0 {
                    whole.remove(&first.namespace);
                }
            }
            report.fetched_files += o.files;
            report.fetched_bytes += o.bytes;
            let s = report.summary_mut(&first.namespace);
            s.fetched_files += o.files;
            s.fetched_bytes += o.bytes;
            s.missing += o.missing;
            if first.class == FileClass::Vectors
                && !vector_namespaces.contains(&first.namespace.as_str())
            {
                vector_namespaces.push(&first.namespace);
            }
            i = j;
        }
        self.warming.ledger.charge(&plan.warm);

        // Mirror the search path's verdict caching for a namespace whose
        // whole scaffolding is now local. The probe query is unfiltered and
        // non-kNN, so the recorded verdict covers neither `filters.bin` nor
        // `vector.idx` (see `HydrationVerdict`): a later filtered or kNN
        // query still checks its own files.
        for ns_name in &whole {
            let manifest = &manifests[ns_name];
            if scaffolded.get(ns_name.as_str()).copied() == Some(manifest.segments.len()) {
                self.completed_hydrations
                    .lock()
                    .unwrap()
                    .entry((ns_name.clone(), manifest.version))
                    .or_default()
                    .merge(None, None);
            }
        }

        // Open segments into the searcher's live segment cache with their
        // vectors, as the static warmup's phase 5 did, so the first kNN wave
        // does not pay every first-touch `open_segment` itself. Only where
        // vector indexes were warmed: a text query opens segments without
        // vectors (a different cache entry), so for a namespace nobody runs
        // kNN against this would only spend segment-cache memory.
        if self.warmup_hydrate_segment_cache {
            for ns_name in vector_namespaces {
                let ns = NamespaceId(ns_name.to_string());
                if let Err(e) = self.searcher.warm_segments(&ns, &manifests[ns_name], true) {
                    report.errors.push(format!(
                        "'{ns_name}' segment cache warm failed: {e} (first kNN queries cold-open)"
                    ));
                }
            }
        }
        report.duration_ms = started.elapsed().as_millis() as u64;
        report.log();
        report
    }

    /// The readiness-gated boot pass: explicit namespaces plus the role's
    /// persisted warm set, hottest first, within the byte budget.
    #[cfg_attr(not(feature = "s3"), allow(dead_code))]
    pub(crate) fn run_boot_warmup(&self, storage: &dyn WarmStorage) -> WarmReport {
        let mut load_error = None;
        if self.warming.cfg.usage {
            match storage.load_warm_set(&self.warming.cfg.role) {
                Ok(Some(set)) => {
                    println!(
                        "warmup: loaded warm set for role '{}': {} namespace(s)",
                        self.warming.cfg.role,
                        set.namespaces.len()
                    );
                    self.warming.tracker.seed(&set);
                    let mut p = self.warming.persist.lock().unwrap();
                    p.loaded_namespaces = Some(set.namespaces.len());
                    p.loaded_written_at_unix_ms = Some(set.written_at_unix_ms);
                }
                Ok(None) => println!(
                    "warmup: no persisted warm set for role '{}' yet",
                    self.warming.cfg.role
                ),
                Err(e) => load_error = Some(format!("warm set not loaded: {e}")),
            }
        }
        let mut errors: Vec<String> = load_error.into_iter().collect();
        let mut specs = Vec::new();
        for (ns, source) in self.hot_specs() {
            // A query pod may not hold this namespace's manifest yet.
            if let Err(e) = self.refresh_query_namespace(&ns, false) {
                errors.push(format!("'{}' manifest refresh failed: {e}; skipped", ns.0));
                continue;
            }
            if let Some(manifest) = self.indexer.manifest_shared(&ns) {
                self.warming
                    .observed
                    .lock()
                    .unwrap()
                    .insert(ns.0.clone(), manifest);
            }
            specs.push(WarmSpec {
                ns,
                source,
                only: None,
            });
        }
        *self.warming.boot_namespaces.lock().unwrap() =
            specs.iter().map(|s| s.ns.0.clone()).collect();
        let budget = self.warming.cfg.budget_bytes;
        let mut report = self.run_warm_pass(storage, "boot", specs, budget);
        for e in &errors {
            eprintln!("warn: warmup[boot]: {e}");
        }
        errors.append(&mut report.errors);
        report.errors = errors;
        *self.warming.boot.lock().unwrap() = Some(report.clone());
        report
    }

    /// One re-warm poll (query pods). For every hot namespace whose
    /// control-plane manifest is ahead of the local one — the cheap
    /// `manifest_version` check — install the new manifest and warm the
    /// segments it added (a flush's new segment, a compaction's output)
    /// before a user query has to. Removed segments give their warm bytes
    /// back to the budget. Never touches readiness.
    pub(crate) fn run_rewarm_tick(&self, storage: &dyn WarmStorage) -> Option<WarmReport> {
        if self.ingest_host.is_none() || !self.is_ready() {
            return None;
        }
        let mut specs = Vec::new();
        for (ns, source) in self.hot_specs() {
            let Some(local) = self.indexer.manifest_shared(&ns) else {
                // Never loaded here: the first search (or boot) loads it.
                continue;
            };
            // Baseline: what this poller last saw, else the local manifest
            // (see `Warming::observed`).
            let before = self
                .warming
                .observed
                .lock()
                .unwrap()
                .get(&ns.0)
                .cloned()
                .unwrap_or_else(|| local.clone());
            let stored = match self.controller.lock().unwrap().manifest_version(&ns) {
                Ok(Some(stored)) => stored,
                Ok(None) => continue,
                Err(e) => {
                    eprintln!("warn: warmup[rewarm]: '{}' version check failed: {e}", ns.0);
                    continue;
                }
            };
            if local.version < stored {
                if let Err(e) = self.refresh_query_namespace(&ns, false) {
                    eprintln!(
                        "warn: warmup[rewarm]: '{}' manifest refresh failed: {e}",
                        ns.0
                    );
                    continue;
                }
            }
            let Some(after) = self.indexer.manifest_shared(&ns) else {
                continue;
            };
            self.warming
                .observed
                .lock()
                .unwrap()
                .insert(ns.0.clone(), after.clone());
            if after.version <= before.version {
                continue;
            }
            let old: HashSet<&str> = before
                .segments
                .iter()
                .map(|e| e.segment_id.0.as_str())
                .collect();
            let live: HashSet<String> = after
                .segments
                .iter()
                .map(|e| e.segment_id.0.clone())
                .collect();
            self.warming.ledger.retain_segments(&ns.0, &live);
            self.warming.tracker.retain_segments(&ns.0, &live);
            let added: HashSet<String> = live
                .iter()
                .filter(|s| !old.contains(s.as_str()))
                .cloned()
                .collect();
            println!(
                "warmup[rewarm]: '{}' manifest {} -> {}: {} new segment(s)",
                ns.0,
                before.version,
                after.version,
                added.len()
            );
            if !added.is_empty() {
                specs.push(WarmSpec {
                    ns,
                    source,
                    only: Some(added),
                });
            }
        }
        if specs.is_empty() {
            return None;
        }
        let available = self
            .warming
            .cfg
            .budget_bytes
            .map(|b| b.saturating_sub(self.warming.ledger.total()));
        let report = self.run_warm_pass(storage, "rewarm", specs, available);
        let mut kept = self.warming.rewarms.lock().unwrap();
        kept.push_back(report.clone());
        while kept.len() > REWARM_REPORTS_KEPT {
            kept.pop_front();
        }
        Some(report)
    }

    /// Write the role's warm set. Skipped while nothing is hot, so a pod
    /// that has not served traffic yet never overwrites a useful set with
    /// an empty one. Returns the namespaces written.
    pub(crate) fn persist_warm_set(&self, storage: &dyn WarmStorage) -> Result<usize, String> {
        let set = self
            .warming
            .tracker
            .warm_set(&self.warming.cfg.role, &self.warming.cfg.limits);
        if set.namespaces.is_empty() {
            return Ok(0);
        }
        let result = storage.store_warm_set(&self.warming.cfg.role, &set);
        let mut p = self.warming.persist.lock().unwrap();
        match result {
            Ok(()) => {
                p.last_written_unix_ms = Some(set.written_at_unix_ms);
                p.last_written_namespaces = set.namespaces.len();
                p.last_error = None;
                Ok(set.namespaces.len())
            }
            Err(e) => {
                p.last_error = Some(e.clone());
                Err(e)
            }
        }
    }

    /// Start the re-warm poller on a query pod (`KOSHA_INGEST_HOST` set):
    /// ingest pods write the segments themselves, so they are local there.
    pub(crate) fn spawn_rewarmer(self: &Arc<Self>) {
        if self.ingest_host.is_none() {
            return;
        }
        let Some(interval) = self.warming.cfg.rewarm_interval else {
            println!("warmup: re-warm on manifest change disabled (KOSHA_REWARM_INTERVAL_MS=0)");
            return;
        };
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("kosha-rewarm".into())
            .spawn(move || loop {
                std::thread::sleep(interval);
                let Some(state) = weak.upgrade() else { return };
                state.run_rewarm_tick(&*state);
            })
            .expect("failed to spawn re-warm thread");
    }

    /// Start the thread that persists the warm set every
    /// `KOSHA_WARMSET_PERSIST_SECS`.
    pub(crate) fn spawn_warm_set_persister(self: &Arc<Self>) {
        if !self.warming.cfg.usage {
            return;
        }
        let Some(interval) = self.warming.cfg.persist_interval else {
            return;
        };
        let weak = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("kosha-warmset".into())
            .spawn(move || loop {
                std::thread::sleep(interval);
                let Some(state) = weak.upgrade() else { return };
                if let Err(e) = state.persist_warm_set(&*state) {
                    eprintln!("warn: warm set not persisted: {e}");
                }
            })
            .expect("failed to spawn warm-set persistence thread");
    }

    /// `GET /v1/admin/warmup-status`.
    pub(crate) fn warmup_status_json(&self) -> serde_json::Value {
        let w = &self.warming;
        let warm_bytes = w.ledger.total();
        let namespaces: Vec<serde_json::Value> = w
            .tracker
            .namespaces()
            .into_iter()
            .take(50)
            .map(|n| {
                serde_json::json!({
                    "namespace": n.namespace,
                    "score": n.score(),
                    "hits": n.hits,
                    "bytes_fetched": n.bytes,
                    "segments_tracked": n.segments.len(),
                })
            })
            .collect();
        serde_json::json!({
            "ready": self.is_ready(),
            "config": {
                "explicit_namespaces": self.warmup_namespaces,
                "budget_bytes": w.cfg.budget_bytes,
                "usage": w.cfg.usage,
                "role": w.cfg.role,
                "warm_set_limits": w.cfg.limits,
                "persist_interval_secs": w.cfg.persist_interval.map(|d| d.as_secs()),
                "rewarm_interval_ms": w.cfg.rewarm_interval.map(|d| d.as_millis() as u64),
                "posting_blobs": self.warmup_posting_blobs,
                "hydrate_segment_cache": self.warmup_hydrate_segment_cache,
                "access_half_life_secs": w.tracker.config().half_life.as_secs(),
                "access_max_segments": w.tracker.config().max_segments,
            },
            "boot_namespaces": *w.boot_namespaces.lock().unwrap(),
            "warm_bytes": warm_bytes,
            "warm_items": w.ledger.len(),
            "budget_remaining_bytes": w.cfg.budget_bytes.map(|b| b.saturating_sub(warm_bytes)),
            "access": {
                "tracked_segments": w.tracker.tracked_segments(),
                "namespaces": namespaces,
            },
            "warm_set": *w.persist.lock().unwrap(),
            "boot": *w.boot.lock().unwrap(),
            "rewarms": w.rewarms.lock().unwrap().iter().collect::<Vec<_>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_groups_into_segment_classes() {
        let sizes = group_class_sizes(vec![
            ("s1/inverted.idx".to_string(), 10),
            ("s1/footer.json".to_string(), 1),
            ("s1/postings-00.bin".to_string(), 5),
            ("s1/postings-01.bin".to_string(), 7),
            ("s1/doc_store.bin".to_string(), 100),
            ("s1/_COMMIT".to_string(), 3),
            ("s2/vector.idx".to_string(), 50),
            ("s2/vector.offsets".to_string(), 2),
            ("s2/deeper/x.bin".to_string(), 99),
            ("stray".to_string(), 1),
        ]);
        assert_eq!(sizes[&("s1".into(), FileClass::Scoring)], 11);
        assert_eq!(sizes[&("s1".into(), FileClass::Postings)], 12);
        assert_eq!(sizes[&("s1".into(), FileClass::DocStore)], 100);
        assert_eq!(sizes[&("s2".into(), FileClass::Vectors)], 52);
        assert_eq!(sizes.len(), 4);
    }
}
