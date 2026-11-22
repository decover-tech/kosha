//! Disk-cache read signal and query trace (#389, research; default off).
//!
//! Searches open segment files directly, so `kosha_cache::Cache` never saw
//! a read and its bound (`KOSHA_CACHE_MAX_BYTES`) evicted in insertion
//! order. Two opt-in knobs live here:
//!
//! - `KOSHA_CACHE_POLICY=sieve|s3fifo`: after each successful search, report
//!   the files it read to the cache as [`ReadHint`]s so a scan-resistant
//!   policy can keep re-read files. Unset (or `legacy`) = no hints, legacy
//!   eviction, byte-for-byte today's behaviour.
//! - `KOSHA_CACHE_TRACE_PATH=<file>`: append one JSON line per search
//!   describing what it read, for offline replay with
//!   `scripts/research/cache_sim/cache_sim.py` (format documented there and
//!   in `docs/research/389-cost-tiering.md`). Independent of the policy, so
//!   a trace can be recorded on the legacy baseline.
//!
//! Granularity: per segment and file class, except posting shards, which
//! are named exactly (each query term hashes to one of 256 shards; marking
//! all of them read would make every shard look hot).

use super::*;
use crate::warm::FileClass;
use kosha_cache::{CacheOptions, CachePolicy, ReadHint};
use std::io::Write as _;
use std::sync::OnceLock;

/// `FileClass` index of a segment-level file name, or `CLASS_ANY` for files
/// warming does not classify (`_COMMIT`, sidecars).
pub(crate) fn classify(name: &str) -> u8 {
    FileClass::of_file(name).map_or(kosha_cache::CLASS_ANY, |c| c.index() as u8)
}

/// Cache options from the environment: `KOSHA_CACHE_MAX_BYTES` as before,
/// plus the opt-in `KOSHA_CACHE_POLICY`. An unknown policy logs and falls
/// back to legacy rather than refusing to start.
pub(crate) fn cache_options_from_env(max_bytes: Option<u64>) -> CacheOptions {
    let raw = std::env::var("KOSHA_CACHE_POLICY").ok();
    let policy = match CachePolicy::parse(raw.as_deref()) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("WARN: {e}; using legacy eviction");
            None
        }
    };
    match (policy, max_bytes) {
        (Some(p), Some(max)) => println!(
            "disk cache policy: {} (max_mb={:.1}, research #389)",
            p.as_str(),
            max as f64 / 1048576.0
        ),
        (Some(p), None) => println!(
            "disk cache policy: {} ignored: KOSHA_CACHE_MAX_BYTES is unset, nothing is evicted",
            p.as_str()
        ),
        _ => {}
    }
    let exact_accounting =
        parse_accounting(std::env::var("KOSHA_CACHE_ACCOUNTING").ok().as_deref());
    if exact_accounting && policy.is_none() && max_bytes.is_some() {
        println!("disk cache accounting: exact (legacy eviction, research #389)");
    }
    CacheOptions {
        max_bytes,
        policy,
        classify: Some(classify),
        exact_accounting,
    }
}

/// `KOSHA_CACHE_ACCOUNTING`: `exact` turns on exact per-key accounting for
/// the legacy path (see `CacheOptions::exact_accounting`); unset, empty or
/// `legacy` keeps today's. Anything else logs and keeps today's.
pub(crate) fn parse_accounting(value: Option<&str>) -> bool {
    match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("legacy") => false,
        Some("exact") => true,
        Some(other) => {
            eprintln!(
                "WARN: unknown KOSHA_CACHE_ACCOUNTING={other:?} (expected legacy or exact); using legacy"
            );
            false
        }
    }
}

/// `KOSHA_CACHE_TRACE_PATH` writer, opened on first use.
fn trace_sink() -> Option<&'static Mutex<std::io::LineWriter<std::fs::File>>> {
    static SINK: OnceLock<Option<Mutex<std::io::LineWriter<std::fs::File>>>> = OnceLock::new();
    SINK.get_or_init(|| {
        let path = std::env::var("KOSHA_CACHE_TRACE_PATH").ok()?;
        if path.trim().is_empty() {
            return None;
        }
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(file) => {
                println!("cache trace: writing per-search reads to {path} (research #389)");
                Some(Mutex::new(std::io::LineWriter::new(file)))
            }
            Err(e) => {
                eprintln!("WARN: KOSHA_CACHE_TRACE_PATH={path}: {e}; trace disabled");
                None
            }
        }
    })
    .as_ref()
}

/// What one search read, at the granularity the cache can use.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct QueryReads {
    /// Segment directories relative to the data dir (`{ns}/{segment}`),
    /// manifest order.
    pub segments: Vec<String>,
    pub postings: bool,
    pub vectors: bool,
    /// Exact posting shard files, when known; `None` means every shard
    /// (wildcard / fuzzy) or unknown.
    pub posting_files: Option<Vec<String>>,
    /// Segment directories whose doc store served the returned page.
    pub page_segments: Vec<String>,
}

impl QueryReads {
    pub(crate) fn hints(&self) -> Vec<ReadHint<'_>> {
        let mut hints = Vec::with_capacity(
            self.segments.len()
                * (2 + self.vectors as usize + self.posting_files.as_ref().map_or(0, Vec::len))
                + self.page_segments.len(),
        );
        for dir in &self.segments {
            hints.push(ReadHint::Class {
                dir,
                class: FileClass::Scoring.index() as u8,
            });
            if self.postings {
                match &self.posting_files {
                    Some(files) => {
                        hints.extend(files.iter().map(|file| ReadHint::File { dir, file }))
                    }
                    None => hints.push(ReadHint::Class {
                        dir,
                        class: FileClass::Postings.index() as u8,
                    }),
                }
            }
            if self.vectors {
                hints.push(ReadHint::Class {
                    dir,
                    class: FileClass::Vectors.index() as u8,
                });
            }
        }
        for dir in &self.page_segments {
            hints.push(ReadHint::Class {
                dir,
                class: FileClass::DocStore.index() as u8,
            });
        }
        hints
    }

    /// One trace line (format v1, see the module docs). `segs` and `pages`
    /// are data-dir-relative segment directories, the same prefixes the
    /// cache keys use.
    pub(crate) fn trace_line(&self, at_ms: u128, ns: &str) -> String {
        let mut classes = vec!["scoring"];
        if self.postings {
            classes.push("postings");
        }
        if self.vectors {
            classes.push("vectors");
        }
        serde_json::json!({
            "v": 1,
            "t_ms": at_ms as u64,
            "ns": ns,
            "segs": self.segments,
            "classes": classes,
            "postings": self.posting_files,
            "pages": self.page_segments,
        })
        .to_string()
    }
}

impl AppState {
    /// Read stamp for [`Self::record_cache_reads`]; call before hydration.
    pub(crate) fn begin_cache_read(&self) -> u64 {
        self.cache.begin_read()
    }

    /// Report one successful search's reads to the disk-cache policy and
    /// the trace file. Does nothing (not even the shard computation) when
    /// neither knob is set.
    pub(crate) fn record_cache_reads(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        query: &SearchQuery,
        phases: &kosha_query::SearchPhaseStats,
        started_at: u64,
    ) {
        let trace = trace_sink();
        if !self.cache.tracks_reads() && trace.is_none() {
            return;
        }
        let reads = self.query_reads(ns, manifest, query, phases);
        self.cache.note_reads(started_at, &reads.hints());
        if let Some(sink) = trace {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            let line = reads.trace_line(now, &ns.0);
            if let Ok(mut out) = sink.lock() {
                let _ = writeln!(out, "{line}");
            }
        }
    }

    fn query_reads(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        query: &SearchQuery,
        phases: &kosha_query::SearchPhaseStats,
    ) -> QueryReads {
        let rel = |dir: &std::path::Path| -> Option<String> {
            dir.strip_prefix(&self.data_dir)
                .ok()
                .map(|p| p.to_string_lossy().replace('\\', "/"))
        };
        let segments: Vec<String> = manifest
            .segments
            .iter()
            .filter_map(|e| rel(&manifest.segment_dir(&self.data_dir, ns, &e.segment_id)))
            .collect();
        let postings = query.wildcard.is_some()
            || query.match_phrase.is_some()
            || !query.query_text.trim().is_empty();
        let posting_files = if postings {
            self.query_posting_files(ns, manifest, query)
        } else {
            None
        };
        let page_segments = phases
            .page_segments
            .iter()
            .filter(|dir| {
                !dir.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|s| s.starts_with(crate::unindexed::TAIL_SEGMENT_PREFIX))
            })
            .filter_map(|dir| rel(dir))
            .collect();
        QueryReads {
            segments,
            postings,
            vectors: query.knn.is_some(),
            posting_files,
            page_segments,
        }
    }

    /// The posting shards this query's terms hash to — the same set
    /// `ensure_query_posting_blobs` makes local. `None` = all shards.
    #[cfg(feature = "s3")]
    fn query_posting_files(
        &self,
        ns: &NamespaceId,
        manifest: &kosha_core::Manifest,
        query: &SearchQuery,
    ) -> Option<Vec<String>> {
        if query.wildcard.is_some() || query.fuzzy.is_some() {
            return None;
        }
        let analyses: Vec<Option<kosha_core::SegmentAnalysis>> = manifest
            .segments
            .iter()
            .map(|entry| {
                let dir = manifest.segment_dir(&self.data_dir, ns, &entry.segment_id);
                manifest
                    .footer_or_read(&entry.segment_id, || {
                        kosha_segment::SegmentReader::read_footer(&dir).ok()
                    })
                    .and_then(|footer| footer.analysis.clone())
            })
            .collect();
        let analysis = kosha_query::QueryAnalysis::new(query);
        Some(posting_blob_files_for_query(
            query,
            &analysis,
            analyses.iter().map(Option::as_ref),
        ))
    }

    #[cfg(not(feature = "s3"))]
    fn query_posting_files(
        &self,
        _ns: &NamespaceId,
        _manifest: &kosha_core::Manifest,
        _query: &SearchQuery,
    ) -> Option<Vec<String>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_matches_warm_file_classes() {
        assert_eq!(classify("footer.json"), FileClass::Scoring.index() as u8);
        assert_eq!(
            classify("postings-0a.bin"),
            FileClass::Postings.index() as u8
        );
        assert_eq!(classify("doc_store.bin"), FileClass::DocStore.index() as u8);
        assert_eq!(classify("vector.idx"), FileClass::Vectors.index() as u8);
        assert_eq!(classify("_COMMIT"), kosha_cache::CLASS_ANY);
    }

    #[test]
    fn hints_name_exact_shards_and_page_doc_stores() {
        let reads = QueryReads {
            segments: vec!["ns/a".into(), "ns/b".into()],
            postings: true,
            vectors: false,
            posting_files: Some(vec!["postings-01.bin".into()]),
            page_segments: vec!["ns/b".into()],
        };
        let hints = reads.hints();
        // Per segment: scoring class + one shard; plus one page doc store.
        assert_eq!(hints.len(), 5);
        assert!(hints.iter().any(|h| matches!(
            h,
            ReadHint::File {
                dir: "ns/a",
                file: "postings-01.bin"
            }
        )));
        let line: serde_json::Value = serde_json::from_str(&reads.trace_line(5, "ns")).unwrap();
        assert_eq!(line["v"], 1);
        assert_eq!(line["segs"][1], "ns/b");
        assert_eq!(line["classes"][1], "postings");
        assert_eq!(line["postings"][0], "postings-01.bin");
        assert_eq!(line["pages"][0], "ns/b");
    }

    /// End to end through the server: a tenant-scoped search
    /// (`{tenant}/{ns}/{segment}/{file}` on disk) must land its read hints
    /// on the cached files it read. The first #389 prototype grouped keys
    /// by their first two components, so on this layout every hint was
    /// dropped and the policies silently ran as FIFO.
    #[test]
    fn tenant_scoped_search_hints_reach_cached_entries() {
        use kosha_core::{Document, DocumentId, Field};
        let mut state = AppState::new(crate::tests::test_dir());
        let ns = NamespaceId(crate::tenant_namespace("default", "msmarco"));
        state
            .indexer
            .index_documents(
                ns.clone(),
                vec![Document {
                    id: DocumentId("d1".into()),
                    fields: vec![Field::text("body", "contract law basics")],
                }],
            )
            .unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
        state.publish_namespace(&ns).unwrap();
        // A bounded cache with a policy, adopting the published files.
        state.cache = kosha_cache::Cache::with_options(
            state.data_dir.clone(),
            CacheOptions {
                max_bytes: Some(1 << 40),
                policy: Some(CachePolicy::Sieve),
                classify: Some(classify),
                exact_accounting: false,
            },
        );
        let ns_dir = state.data_dir.join("default").join("msmarco");
        let segs: Vec<std::path::PathBuf> = std::fs::read_dir(&ns_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_dir() && p.join("footer.json").is_file())
            .collect();
        assert_eq!(segs.len(), 1, "one published segment under {ns_dir:?}");
        let seg_rel = segs[0]
            .strip_prefix(&state.data_dir)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let footer = format!("{seg_rel}/footer.json");
        assert_eq!(state.cache.policy_pending_reads(&footer), Some(0));

        let req = br#"{"query_text":"contract","no_cache":true}"#;
        let resp = crate::handle_search_post_with_ns("msmarco", "default", req, &state);
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(
            state.cache.policy_pending_reads(&footer).unwrap_or(0) > 0,
            "the search's scoring-class hint must reach {footer}"
        );
        let _ = std::fs::remove_dir_all(&state.data_dir);
    }

    /// Default (no `KOSHA_CACHE_POLICY`): the cache ignores hints, so
    /// eviction stays legacy insertion order.
    #[test]
    fn unset_policy_is_legacy() {
        let opts = CacheOptions {
            max_bytes: Some(10),
            policy: CachePolicy::parse(None).unwrap(),
            classify: Some(classify),
            exact_accounting: false,
        };
        let dir = std::env::temp_dir().join(format!(
            "kosha-server-cache-reads-legacy-{}",
            std::process::id()
        ));
        let cache = kosha_cache::Cache::with_options(dir.clone(), opts);
        assert!(!cache.tracks_reads());
        assert_eq!(cache.policy(), None);
        let _ = std::fs::remove_dir_all(dir);
    }
}
