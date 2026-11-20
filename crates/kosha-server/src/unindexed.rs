//! Read-your-writes (#255): searching acknowledged-but-unindexed writes.
//!
//! A write is acknowledged once it is in the ingest pod's WAL and write
//! buffer; it reaches a segment only when the buffer is flushed and the
//! manifest published. Under `consistency: "strong"` a search also scores
//! that *unindexed tail* exhaustively, so a document is searchable the
//! moment `/index` returns, from the ingest pod and from every query pod.
//!
//! How a strong search runs:
//!
//! 1. It takes its manifest snapshot (version `V`) as before.
//! 2. It gets the namespace's [`UnindexedTail`] relative to `V`: from the
//!    local indexer on the ingest pod, or from the ingest pod's
//!    `GET /internal/namespaces/{ns}/unindexed?manifest_version=V` on a
//!    query pod. A tail is always relative to one manifest version; when the
//!    writer is at another one (a flush published between the snapshot and
//!    the fetch), the query pod refreshes its manifest and asks again, a few
//!    times, then fails the search rather than serve a result that misses or
//!    double-counts the flushed documents.
//! 3. The tail's documents are built into a transient segment with the
//!    normal segment writer, the analysis chains of `V`'s schema and the
//!    writer's BM25 parameters, and appended to a copy of the manifest. The
//!    indexed copies the tail supersedes (`masked`) are added to that copy's
//!    tombstones. The normal search then runs over it, so scoring, filters,
//!    kNN, aggregations, `search_after`, capped `total_hits` and namespace
//!    BM25 statistics (which include the tail documents) are exactly those
//!    of a flushed segment.
//! 4. The transient segment is kept per namespace while the tail's
//!    fingerprint is unchanged, so repeated strong searches over an
//!    unchanged tail neither refetch nor rebuild it (`if_none_match`).
//!
//! `consistency: "eventual"` skips all of this and searches the manifest
//! alone, as every search did before.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kosha_core::{KoshaError, Manifest, ManifestEntry, NamespaceId, SegmentId};
use kosha_write::UnindexedTail;

use crate::AppState;

/// Prefix of transient tail segment ids (and directories). Never published.
pub const TAIL_SEGMENT_PREFIX: &str = kosha_core::TAIL_SEGMENT_PREFIX;

/// How many times a strong search asks for a tail matching its manifest
/// version before giving up (each retry refreshes a query pod's manifest).
const TAIL_ATTEMPTS: u32 = 6;

/// `KOSHA_MAX_UNINDEXED_BYTES` default: turbopuffer's 2 GB unindexed cap.
pub const DEFAULT_MAX_UNINDEXED_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Seconds a backpressured writer is told to wait (`retry-after`).
const BACKPRESSURE_RETRY_AFTER_SECS: u64 = 1;

/// Request option `consistency`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// Search the manifest plus the unindexed tail: every acknowledged
    /// write is visible.
    Strong,
    /// Search the manifest only: writes become visible once flushed and
    /// published (about `KOSHA_FLUSH_MAX_AGE_MS`).
    Eventual,
}

impl Consistency {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "strong" => Some(Self::Strong),
            "eventual" => Some(Self::Eventual),
            _ => None,
        }
    }

    /// The `consistency` field of a search body (`None` when absent).
    pub fn from_json(value: Option<&serde_json::Value>) -> Result<Option<Self>, String> {
        match value {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::String(s)) => Self::parse(s).map(Some).ok_or_else(|| {
                format!("'consistency' must be \"strong\" or \"eventual\", got {s:?}")
            }),
            Some(other) => Err(format!(
                "'consistency' must be \"strong\" or \"eventual\", got {other}"
            )),
        }
    }

    /// `KOSHA_DEFAULT_CONSISTENCY`. `None` when unset (the built-in default
    /// applies — see [`AppState::default_consistency`]).
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("KOSHA_DEFAULT_CONSISTENCY").ok()?;
        if raw.trim().is_empty() {
            return None;
        }
        let parsed = Self::parse(&raw);
        if parsed.is_none() {
            eprintln!(
                "WARN: KOSHA_DEFAULT_CONSISTENCY={raw} is not \"strong\" or \"eventual\" — \
                 using the built-in default"
            );
        }
        parsed
    }
}

/// Caps on a namespace's unindexed backlog (`KOSHA_MAX_UNINDEXED_BYTES`,
/// `KOSHA_MAX_UNINDEXED_DOCS`). Past either, `/index` and `/replace` answer
/// 429 until the flushes catch up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnindexedCap {
    /// `0` disables the byte cap.
    pub max_bytes: u64,
    /// `None` disables the document cap (the default).
    pub max_docs: Option<usize>,
}

impl UnindexedCap {
    pub fn from_env() -> Self {
        let max_bytes = std::env::var("KOSHA_MAX_UNINDEXED_BYTES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(DEFAULT_MAX_UNINDEXED_BYTES);
        let max_docs = std::env::var("KOSHA_MAX_UNINDEXED_DOCS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0);
        Self {
            max_bytes,
            max_docs,
        }
    }

    /// Whether a backlog of `docs` documents / `bytes` bytes is at the cap.
    pub fn exceeded(&self, docs: usize, bytes: u64) -> bool {
        (self.max_bytes > 0 && bytes >= self.max_bytes)
            || self.max_docs.is_some_and(|max| docs >= max)
    }
}

/// A test hook standing in for the HTTP fetch of the ingest pod's tail:
/// `(namespace, manifest_version, if_none_match)`.
pub type TailFetcher =
    Box<dyn Fn(&NamespaceId, u64, Option<&str>) -> Result<UnindexedTail, KoshaError> + Send + Sync>;

/// A namespace's unindexed tail, built for searching: the transient
/// segment (if the tail has documents) and the masks. Its directory is
/// removed when the last search holding it finishes.
pub struct TailSegment {
    pub fingerprint: String,
    /// The manifest version the tail is relative to.
    pub base_version: u64,
    /// Documents in the transient segment: `exhaustive_search_count`.
    pub docs: u64,
    segment: Option<(SegmentId, Arc<kosha_core::Footer>)>,
    masked: HashMap<SegmentId, Vec<u32>>,
    dir: Option<PathBuf>,
    /// The last manifest built from this tail, with the base snapshot it
    /// was built on: rebuilt only when the base snapshot changes.
    augmented: Mutex<Option<(Arc<Manifest>, Arc<Manifest>)>>,
}

impl Drop for TailSegment {
    fn drop(&mut self) {
        if let Some(dir) = &self.dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

impl TailSegment {
    /// `base` plus the tail: the transient segment appended and the
    /// superseded indexed copies tombstoned.
    fn manifest_over(
        &self,
        base: &Arc<Manifest>,
        data_dir: &Path,
        namespace: &NamespaceId,
    ) -> Result<Arc<Manifest>, KoshaError> {
        let mut memo = self.augmented.lock().unwrap();
        if let Some((built_on, manifest)) = memo.as_ref() {
            if Arc::ptr_eq(built_on, base) {
                return Ok(Arc::clone(manifest));
            }
        }
        let mut manifest = Manifest::clone(base);
        // Masks go into `tombstones`; a segment whose deletion vector was
        // never expanded into it would otherwise lose those deletes (the
        // searcher only expands vectors with no tombstone entry).
        if manifest
            .deletion_vectors
            .keys()
            .any(|segment| !manifest.tombstones.contains_key(segment))
        {
            kosha_core::deletes::hydrate(&mut manifest, |segment, vector| {
                Ok(std::fs::read(
                    data_dir.join(vector.path(namespace, segment)?),
                )?)
            })?;
        }
        for (segment, seqs) in &self.masked {
            if manifest
                .segments
                .iter()
                .any(|entry| &entry.segment_id == segment)
            {
                manifest
                    .tombstones
                    .entry(segment.clone())
                    .or_default()
                    .extend(seqs.iter().copied());
            }
        }
        if let Some((segment_id, footer)) = &self.segment {
            manifest.segments.push(ManifestEntry {
                segment_id: segment_id.clone(),
                doc_count: footer.doc_count,
            });
            manifest.remember_segment_footer(Arc::clone(footer));
        }
        let manifest = Arc::new(manifest);
        *memo = Some((Arc::clone(base), Arc::clone(&manifest)));
        Ok(manifest)
    }
}

/// What one search runs over.
pub struct SearchView {
    /// The manifest snapshot: hydration, caches and the result-cache key.
    pub base: Arc<Manifest>,
    /// What the searcher scores: `base`, or `base` plus the tail.
    pub search: Arc<Manifest>,
    /// Keeps the transient segments' directories alive during the search:
    /// one tail, or one per shard of a sharded namespace (#207).
    pub tails: Vec<Arc<TailSegment>>,
    /// A strong search served as eventual, and why — see
    /// [`kosha_core::SearchResult::consistency_fallback`].
    pub fallback: Option<String>,
}

impl SearchView {
    pub fn exhaustive_search_count(&self) -> u64 {
        self.tails.iter().map(|tail| tail.docs).sum()
    }

    /// Whether the result depends only on `base` (and may be cached under
    /// its version).
    pub fn is_plain(&self) -> bool {
        self.tails.is_empty()
    }
}

/// Per-process state of the strong-consistency path.
pub struct UnindexedState {
    /// Process-unique part of transient segment ids.
    instance: String,
    next_segment: AtomicU64,
    /// The last tail built per namespace.
    tails: Mutex<HashMap<NamespaceId, Arc<TailSegment>>>,
    /// One tail build at a time: concurrent strong searches over a new tail
    /// wait for the first build instead of each building their own.
    build: Mutex<()>,
    /// HTTP client for the ingest pod's tail route (query pods only).
    http: Option<reqwest::blocking::Client>,
    /// Replaces the HTTP fetch in tests.
    pub fetcher: Option<TailFetcher>,
}

impl UnindexedState {
    pub fn new(query_role: bool) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let http = query_role.then(|| {
            reqwest::blocking::Client::builder()
                .timeout(Duration::from_millis(
                    std::env::var("KOSHA_TAIL_FETCH_TIMEOUT_MS")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .filter(|n| *n > 0)
                        .unwrap_or(10_000),
                ))
                .build()
                .expect("failed to build unindexed-tail HTTP client")
        });
        Self {
            instance: format!("{:x}{:x}", std::process::id(), nanos & 0xffff_ffff_ffff),
            next_segment: AtomicU64::new(0),
            tails: Mutex::new(HashMap::new()),
            build: Mutex::new(()),
            http,
            fetcher: None,
        }
    }
}

/// Remove transient tail segments a previous process left behind (they are
/// rebuilt on demand and never published). Namespaces are `tenant/name`,
/// so segment directories sit two or three levels down.
pub fn remove_stale_tail_segments(data_dir: &Path) {
    fn walk(dir: &Path, depth: usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() || path.file_name().is_some_and(|n| n == "_wal") {
                continue;
            }
            let is_tail = path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(TAIL_SEGMENT_PREFIX));
            if is_tail {
                let _ = std::fs::remove_dir_all(&path);
            } else if depth > 0 {
                walk(&path, depth - 1);
            }
        }
    }
    walk(data_dir, 3);
}

/// Percent-encode one path segment (namespaces contain `/`).
pub(crate) fn encode_path_segment(raw: &str) -> String {
    raw.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl AppState {
    /// The consistency of a search that did not ask for one:
    /// `KOSHA_DEFAULT_CONSISTENCY`, else `strong` — except on a query pod
    /// that cannot read the ingest pod's tail (no `KOSHA_INTERNAL_API_KEY`),
    /// which defaults to `eventual` rather than fail every search. An
    /// explicit `strong` there still fails, with a message saying why.
    pub fn default_consistency(&self) -> Consistency {
        if let Some(configured) = self.default_consistency {
            return configured;
        }
        let can_fetch = self.ingest_host.is_none()
            || self.unindexed.fetcher.is_some()
            || self.internal_service_key.is_some();
        if can_fetch {
            Consistency::Strong
        } else {
            Consistency::Eventual
        }
    }

    /// 429 for a write to a namespace whose unindexed backlog is at the cap
    /// (#255), with a `retry-after` hint. `None` when the write may proceed.
    pub fn unindexed_backpressure(&self, namespace: &NamespaceId) -> Option<String> {
        let (docs, bytes) = self.indexer.unindexed_usage(namespace);
        if !self.unindexed_cap.exceeded(docs, bytes) {
            return None;
        }
        let response = crate::json_error(
            429,
            &format!(
                "write backpressure: namespace '{}' has {docs} unindexed document(s) \
                 (~{bytes} bytes), at the cap (KOSHA_MAX_UNINDEXED_DOCS={}, \
                 KOSHA_MAX_UNINDEXED_BYTES={}); indexing is behind — retry after \
                 {BACKPRESSURE_RETRY_AFTER_SECS}s with backoff",
                namespace.0,
                self.unindexed_cap
                    .max_docs
                    .map_or("unset".to_string(), |n| n.to_string()),
                self.unindexed_cap.max_bytes,
            ),
        );
        Some(response.replacen(
            "\r\n",
            &format!("\r\nretry-after: {BACKPRESSURE_RETRY_AFTER_SECS}\r\n"),
            1,
        ))
    }

    /// The manifest (and, under `Strong`, the unindexed tail) a search of
    /// `namespace` runs over. `Ok(None)`: the namespace does not exist.
    /// `Err`: a ready-to-send error response — under `Strong` a tail that
    /// cannot be read consistently fails the search (503) instead of
    /// silently degrading to `eventual`.
    ///
    /// On a query replica each manifest involved is first brought within
    /// `max_staleness` of the control store. A sharded namespace (#207) is
    /// searched through one merged view of its shards; under `Strong` each
    /// shard contributes its own tail, relative to its own manifest.
    pub fn search_view(
        &self,
        namespace: &NamespaceId,
        consistency: Consistency,
        max_staleness: Option<Duration>,
    ) -> Result<Option<SearchView>, String> {
        self.ensure_query_manifest_fresh(namespace, max_staleness)
            .map_err(|e| crate::search_error_response(&e))?;
        let Some(root) = self.indexer.manifest_shared(namespace) else {
            return Ok(None);
        };
        let Some(shards) = root.shards.filter(|&n| n > 1) else {
            return self.namespace_view(namespace, consistency);
        };
        let shard_ids = kosha_core::sharding::shard_namespaces(namespace, shards);
        for shard in &shard_ids {
            self.ensure_query_manifest_fresh(shard, max_staleness)
                .map_err(|e| crate::search_error_response(&e))?;
        }
        if consistency == Consistency::Eventual {
            let merged = self.sharded_view(namespace, root);
            return Ok(Some(SearchView {
                search: Arc::clone(&merged),
                base: merged,
                tails: Vec::new(),
                fallback: None,
            }));
        }
        // Each shard's snapshot and tail, then one merged view over the
        // snapshots (hydration, caches) and one over the snapshots plus
        // tails (scoring). A shard never written has neither.
        let mut bases: Vec<Option<Arc<Manifest>>> = Vec::with_capacity(shard_ids.len());
        let mut searched: Vec<Option<Arc<Manifest>>> = Vec::with_capacity(shard_ids.len());
        let mut tails = Vec::new();
        let mut fallback = None;
        for shard in &shard_ids {
            match self.namespace_view(shard, consistency)? {
                Some(view) => {
                    bases.push(Some(view.base));
                    searched.push(Some(view.search));
                    tails.extend(view.tails);
                    fallback = fallback.or(view.fallback);
                }
                None => {
                    bases.push(None);
                    searched.push(None);
                }
            }
        }
        let base = Arc::new(kosha_core::sharding::merge_shard_manifests(&root, &bases));
        let search = if tails.is_empty() {
            Arc::clone(&base)
        } else {
            Arc::new(kosha_core::sharding::merge_shard_manifests(
                &root, &searched,
            ))
        };
        Ok(Some(SearchView {
            base,
            search,
            tails,
            fallback,
        }))
    }

    /// [`Self::search_view`] of one ordinary (or shard) namespace, whose
    /// manifest is already fresh enough.
    fn namespace_view(
        &self,
        namespace: &NamespaceId,
        consistency: Consistency,
    ) -> Result<Option<SearchView>, String> {
        let plain = |base: Arc<Manifest>| SearchView {
            search: Arc::clone(&base),
            base,
            tails: Vec::new(),
            fallback: None,
        };
        if consistency == Consistency::Eventual {
            return Ok(self.indexer.manifest_shared(namespace).map(plain));
        }
        let mut last_mismatch = None;
        for attempt in 0..TAIL_ATTEMPTS {
            if attempt > 0 {
                // 5, 10, 20, 40, 80 ms: a publish in flight is an S3 upload
                // plus one control-store CAS.
                std::thread::sleep(Duration::from_millis(5 << (attempt - 1).min(4)));
            }
            let Some(base) = self.indexer.manifest_shared(namespace) else {
                return Ok(None);
            };
            let cached = self
                .unindexed
                .tails
                .lock()
                .unwrap()
                .get(namespace)
                .filter(|tail| tail.base_version == base.version)
                .cloned();
            let known = cached.as_ref().map(|tail| tail.fingerprint.as_str());
            let tail = self
                .fetch_unindexed_tail(namespace, base.version, known)
                .map_err(|e| {
                    crate::json_error(
                        503,
                        &format!(
                            "strong consistency: cannot read the unindexed tail of '{}': {e} \
                             — retry, or search with \"consistency\": \"eventual\"",
                            namespace.0
                        ),
                    )
                })?;
            if tail.stale || tail.manifest_version != base.version {
                last_mismatch = Some((tail.manifest_version, base.version));
                if self.ingest_host.is_some() {
                    // The writer is ahead (a flush or delete published) or
                    // behind (restarting): re-read the stored manifest.
                    if let Err(e) = self.refresh_query_namespace(namespace, false) {
                        return Err(crate::search_error_response(&e));
                    }
                }
                continue;
            }
            if tail.not_modified {
                match cached {
                    Some(cached) if cached.fingerprint == tail.fingerprint => {
                        return self.view_with_tail(namespace, base, cached).map(Some);
                    }
                    // Evicted meanwhile: ask again for the full tail.
                    _ => continue,
                }
            }
            if tail.is_empty() {
                self.drop_cached_tail(namespace);
                return Ok(Some(plain(base)));
            }
            let built = self.install_tail(namespace, &base, tail).map_err(|e| {
                crate::json_error(
                    500,
                    &format!(
                        "strong consistency: cannot build the unindexed tail of '{}': {e}",
                        namespace.0
                    ),
                )
            })?;
            return self.view_with_tail(namespace, base, built).map(Some);
        }
        let (writer, local) = last_mismatch.unwrap_or_default();
        // The writer is ahead of the newest manifest in the control store:
        // it flushed (or compacted, or deleted) and has not published yet,
        // or publication is failing. Its tail is relative to a version no
        // reader can have, so the strong read is impossible until the
        // publication lands; serve the last published manifest instead,
        // as every search did before #255, and say so. (A writer that is
        // behind, e.g. restarting, is waited out with a 503 below.)
        if writer > local && self.ingest_host.is_some() {
            if let Some(base) = self.indexer.manifest_shared(namespace) {
                if base.version < writer {
                    let mut view = plain(base);
                    view.fallback = Some(format!(
                        "eventual: the writer of '{}' is at manifest version {writer}, which \
                         is not published yet; searched published version {}",
                        namespace.0, view.base.version
                    ));
                    return Ok(Some(view));
                }
            }
        }
        Err(crate::json_error(
            503,
            &format!(
                "strong consistency: the writer's unindexed tail of '{}' is relative to \
                 manifest version {writer} but this node searches version {local} (a \
                 publication is in progress or failing) — retry, or search with \
                 \"consistency\": \"eventual\"",
                namespace.0
            ),
        ))
    }

    fn view_with_tail(
        &self,
        namespace: &NamespaceId,
        base: Arc<Manifest>,
        tail: Arc<TailSegment>,
    ) -> Result<SearchView, String> {
        let search = tail
            .manifest_over(&base, &self.data_dir, namespace)
            .map_err(|e| crate::search_error_response(&e))?;
        Ok(SearchView {
            base,
            search,
            tails: vec![tail],
            fallback: None,
        })
    }

    /// The tail relative to `version`, from the local writer (ingest pod) or
    /// the ingest pod's internal route (query pod).
    fn fetch_unindexed_tail(
        &self,
        namespace: &NamespaceId,
        version: u64,
        known: Option<&str>,
    ) -> Result<UnindexedTail, String> {
        if let Some(fetch) = &self.unindexed.fetcher {
            return fetch(namespace, version, known).map_err(|e| e.to_string());
        }
        let Some(host) = &self.ingest_host else {
            return self
                .indexer
                .unindexed_tail(namespace, Some(version), known)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "the namespace is unknown to the writer".to_string());
        };
        let Some(key) = self.internal_service_key.as_deref() else {
            return Err("KOSHA_INTERNAL_API_KEY is not set on this query pod; it \
                        authenticates the read of the ingest pod's unindexed tail"
                .to_string());
        };
        let Some(client) = &self.unindexed.http else {
            return Err("no HTTP client for the ingest pod".to_string());
        };
        let mut url = format!(
            "{}/internal/namespaces/{}/unindexed?manifest_version={version}",
            host.trim_end_matches('/'),
            encode_path_segment(&namespace.0)
        );
        if let Some(known) = known {
            url.push_str("&if_none_match=");
            url.push_str(&encode_path_segment(known));
        }
        let response = client
            .get(&url)
            .header("authorization", format!("Bearer {key}"))
            .send()
            .map_err(|e| format!("ingest pod ({host}) unreachable: {e}"))?;
        let status = response.status();
        let body = response
            .text()
            .map_err(|e| format!("reading the ingest pod's answer: {e}"))?;
        if !status.is_success() {
            return Err(format!("ingest pod answered {status}: {body}"));
        }
        serde_json::from_str(&body).map_err(|e| format!("malformed tail from the ingest pod: {e}"))
    }

    /// Build `tail` into a transient segment and make it the namespace's
    /// cached tail (replacing, and forgetting, the previous one).
    fn install_tail(
        &self,
        namespace: &NamespaceId,
        base: &Manifest,
        tail: UnindexedTail,
    ) -> Result<Arc<TailSegment>, KoshaError> {
        let _build = self.unindexed.build.lock().unwrap();
        if let Some(existing) = self.unindexed.tails.lock().unwrap().get(namespace) {
            if existing.fingerprint == tail.fingerprint {
                return Ok(Arc::clone(existing));
            }
        }
        let docs = tail.documents.len() as u64;
        let (segment, dir) = if tail.documents.is_empty() {
            (None, None)
        } else {
            let id = SegmentId(format!(
                "{TAIL_SEGMENT_PREFIX}{}-{}",
                self.unindexed.instance,
                self.unindexed.next_segment.fetch_add(1, Ordering::Relaxed)
            ));
            let dir = self.data_dir.join(&namespace.0).join(&id.0);
            let _ = std::fs::remove_dir_all(&dir);
            let mut writer = kosha_segment::SegmentWriter::new(id.clone(), dir.clone())
                .with_analysis(&base.analysis_config());
            for doc in tail.documents {
                writer.add_document(doc.id, doc.fields);
            }
            let footer = match writer.finalize(self.indexer.bm25_params().clone()) {
                Ok(footer) => footer,
                Err(e) => {
                    let _ = std::fs::remove_dir_all(&dir);
                    return Err(e);
                }
            };
            (Some((id, Arc::new(footer))), Some(dir))
        };
        let built = Arc::new(TailSegment {
            fingerprint: tail.fingerprint,
            base_version: tail.manifest_version,
            docs,
            segment,
            masked: tail.masked,
            dir,
            augmented: Mutex::new(None),
        });
        let previous = self
            .unindexed
            .tails
            .lock()
            .unwrap()
            .insert(namespace.clone(), Arc::clone(&built));
        if let Some(previous) = previous {
            self.forget_tail(namespace, &previous);
        }
        Ok(built)
    }

    pub(crate) fn drop_cached_tail(&self, namespace: &NamespaceId) {
        let previous = self.unindexed.tails.lock().unwrap().remove(namespace);
        if let Some(previous) = previous {
            self.forget_tail(namespace, &previous);
        }
    }

    fn forget_tail(&self, namespace: &NamespaceId, tail: &TailSegment) {
        if let Some((segment_id, _)) = &tail.segment {
            self.searcher.forget_segment(namespace, segment_id);
            // A shard's tail (#207) is searched, and cached, through the
            // sharded root's merged view.
            if let Some((root, merged)) = crate::merged_view_alias(namespace, segment_id) {
                self.searcher.forget_segment(&root, &merged);
            }
        }
    }
}

// ─── GET /internal/namespaces/{ns}/unindexed ────────────────────────────────

/// The ingest pod's unindexed tail of a namespace, for a query pod's strong
/// search. Internal-credential only (checked by `route`). Query parameters:
/// `manifest_version` (the reader's version: on a mismatch only a `stale`
/// marker is returned) and `if_none_match` (a fingerprint the reader holds:
/// if still current only a `not_modified` marker is returned).
pub fn handle_internal_unindexed(namespace: &str, query: &str, state: &AppState) -> String {
    if state.ingest_host.is_some() {
        return crate::json_error(404, "not a writer: this pod holds no unindexed writes");
    }
    let mut manifest_version = None;
    let mut if_none_match = None;
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "manifest_version" => match value.parse::<u64>() {
                Ok(v) => manifest_version = Some(v),
                Err(_) => {
                    return crate::json_error(400, "'manifest_version' must be an integer");
                }
            },
            "if_none_match" => if_none_match = Some(crate::url_decode(value)),
            _ => {}
        }
    }
    let namespace = NamespaceId(namespace.to_string());
    match state
        .indexer
        .unindexed_tail(&namespace, manifest_version, if_none_match.as_deref())
    {
        Ok(Some(tail)) => crate::json_ok(&tail),
        Ok(None) => crate::json_error(404, &format!("namespace '{}' not found", namespace.0)),
        Err(e) => crate::json_error(500, &format!("unindexed tail error: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistency_parses_strictly() {
        assert_eq!(Consistency::parse("STRONG"), Some(Consistency::Strong));
        assert_eq!(Consistency::parse("eventual"), Some(Consistency::Eventual));
        assert_eq!(Consistency::parse("session"), None);
        assert_eq!(Consistency::from_json(None), Ok(None));
        assert!(Consistency::from_json(Some(&serde_json::json!(1))).is_err());
        assert!(Consistency::from_json(Some(&serde_json::json!("weak"))).is_err());
    }

    #[test]
    fn unindexed_cap_checks_both_limits() {
        let cap = UnindexedCap {
            max_bytes: 100,
            max_docs: Some(3),
        };
        assert!(!cap.exceeded(2, 99));
        assert!(cap.exceeded(3, 0));
        assert!(cap.exceeded(0, 100));
        let off = UnindexedCap {
            max_bytes: 0,
            max_docs: None,
        };
        assert!(!off.exceeded(usize::MAX, u64::MAX));
    }

    #[test]
    fn namespaces_are_encoded_as_one_path_segment() {
        assert_eq!(encode_path_segment("tenant/ns a"), "tenant%2Fns%20a");
        assert_eq!(crate::url_decode(&encode_path_segment("t/x-1")), "t/x-1");
    }
}
