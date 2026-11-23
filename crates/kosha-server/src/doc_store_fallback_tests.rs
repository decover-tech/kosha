//! End-to-end tests for the `doc_store.offsets` fallback (#179, #184),
//! against a minimal in-process S3 endpoint.
//!
//! Each test builds real segments with the indexer, uploads them to the
//! fake bucket, then runs a cold search on a second, empty query node whose
//! only source of segment files is that bucket. The bucket is then edited
//! into the broken shapes staging reported: a sidecar missing from S3, a
//! sidecar that disagrees with the footer, a manifest footer with a stale
//! doc count, and a legacy `footer.json` with no `format_version`.

use super::*;
use kosha_core::{Document, DocumentId, Field};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

const BUCKET: &str = "kosha-test";
const NS: &str = "offsets-fallback";

/// Path-style S3 that serves GetObject / HeadObject from a directory, 404s
/// (`NoSuchKey`) anything absent, and records every key it was asked for.
/// ListObjectsV2 returns the matching keys. Nothing else is implemented:
/// the search path needs nothing else.
pub(super) struct FakeS3 {
    endpoint: String,
    /// `{root}/{key}` is the object `key`.
    root: PathBuf,
    /// Every key fetched with GET, in order.
    pub(super) gets: Arc<Mutex<Vec<String>>>,
}

impl FakeS3 {
    pub(super) fn start(root: PathBuf) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gets: Arc<Mutex<Vec<String>>> = Arc::default();
        let (serve_root, serve_gets) = (root.clone(), Arc::clone(&gets));
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let (root, gets) = (serve_root.clone(), Arc::clone(&serve_gets));
                std::thread::spawn(move || {
                    let _ = serve_one(stream, &root, &gets);
                });
            }
        });
        Self {
            endpoint,
            root,
            gets,
        }
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    /// Keys fetched with GET so far that end with `suffix`.
    fn gets_ending_with(&self, suffix: &str) -> Vec<String> {
        self.gets
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.ends_with(suffix))
            .cloned()
            .collect()
    }
}

fn serve_one(
    mut stream: std::net::TcpStream,
    root: &Path,
    gets: &Mutex<Vec<String>>,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header == "\r\n" {
            break;
        }
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default();
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let path = path
        .trim_start_matches('/')
        .strip_prefix(BUCKET)
        .unwrap_or("")
        .trim_start_matches('/')
        .replace("%2F", "/");

    let respond = |stream: &mut std::net::TcpStream,
                   status: &str,
                   content_type: &str,
                   body: &[u8],
                   send_body: bool|
     -> std::io::Result<()> {
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-length: {}\r\ncontent-type: {content_type}\r\n\
             connection: close\r\n\r\n",
            body.len()
        )?;
        if send_body {
            stream.write_all(body)?;
        }
        stream.flush()
    };

    if query.contains("list-type=2") {
        let prefix = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("prefix="))
            .unwrap_or("")
            .replace("%2F", "/");
        let mut contents = String::new();
        for entry in walk(root) {
            let key = entry
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            if key.starts_with(&prefix) {
                let size = fs::metadata(&entry).map(|m| m.len()).unwrap_or(0);
                contents.push_str(&format!(
                    "<Contents><Key>{key}</Key><Size>{size}</Size></Contents>"
                ));
            }
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult>\
             <Name>{BUCKET}</Name><Prefix>{prefix}</Prefix><IsTruncated>false</IsTruncated>\
             {contents}</ListBucketResult>"
        );
        return respond(
            &mut stream,
            "200 OK",
            "application/xml",
            body.as_bytes(),
            true,
        );
    }

    let send_body = method != "HEAD";
    if method == "GET" {
        gets.lock().unwrap().push(path.clone());
    }
    match fs::read(root.join(&path)) {
        Ok(bytes) => respond(
            &mut stream,
            "200 OK",
            "application/octet-stream",
            &bytes,
            send_body,
        ),
        Err(_) => {
            let body = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchKey</Code>\
                 <Message>The specified key does not exist.</Message><Key>{path}</Key></Error>"
            );
            respond(
                &mut stream,
                "404 Not Found",
                "application/xml",
                body.as_bytes(),
                send_body,
            )
        }
    }
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}

pub(super) fn temp_dir(tag: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-test-offsets-fallback-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// A namespace of two flushed segments (`alpha` docs, then `beta` docs),
/// uploaded to a fresh fake bucket. Returns the bucket and the manifest the
/// writer produced, with the writer's own cached footers (format_version 2)
/// in it: the "modern manifest" shape.
fn uploaded_namespace() -> (FakeS3, kosha_core::Manifest) {
    let writer_dir = temp_dir("writer");
    let state = AppState::new(writer_dir.clone());
    let ns = NamespaceId(NS.into());
    for (word, count) in [("alpha", 6), ("beta", 4)] {
        let docs = (0..count)
            .map(|i| Document {
                id: DocumentId(format!("{word}-{i}")),
                fields: vec![Field::text("text", format!("{word} document number {i}"))],
            })
            .collect();
        state.indexer.index_documents(ns.clone(), docs).unwrap();
        state.indexer.flush_namespace(&ns).unwrap();
    }
    let manifest = state.indexer.manifest_cloned(&ns).unwrap();
    assert_eq!(manifest.segments.len(), 2, "one segment per flush");

    let bucket_root = temp_dir("bucket");
    for entry in &manifest.segments {
        let src = writer_dir.join(NS).join(&entry.segment_id.0);
        let dst = bucket_root.join(NS).join(&entry.segment_id.0);
        fs::create_dir_all(&dst).unwrap();
        for file in fs::read_dir(&src).unwrap().flatten() {
            if file.path().is_file() {
                fs::copy(file.path(), dst.join(file.file_name())).unwrap();
            }
        }
        assert!(
            dst.join("doc_store.offsets").exists(),
            "the writer emits the sidecar; tests remove it deliberately"
        );
    }
    (FakeS3::start(bucket_root), manifest)
}

/// A query node with an empty local cache, reading segments from `s3`, and
/// serving `manifest`.
fn cold_query_node(s3: &FakeS3, manifest: kosha_core::Manifest) -> AppState {
    cold_node(s3, NS, manifest)
}

/// [`cold_query_node`] for namespace `ns`.
pub(super) fn cold_node(s3: &FakeS3, ns: &str, manifest: kosha_core::Manifest) -> AppState {
    let mut state = AppState::new(temp_dir("query"));
    let cfg = s3_storage::S3Config {
        bucket: BUCKET.into(),
        prefix: String::new(),
        endpoint: Some(s3.endpoint.clone()),
        force_path_style: true,
        access_key: Some("test".into()),
        secret_key: Some("test".into()),
        region: Some("us-east-1".into()),
        sse: Default::default(),
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let storage = rt
        .block_on(s3_storage::S3Storage::new(state.data_dir.clone(), cfg))
        .unwrap();
    state.s3_storage = Some(storage);
    state
        .indexer
        .restore_manifest(NamespaceId(ns.into()), manifest);
    state
}

fn search(state: &AppState, text: &str) -> (u16, serde_json::Value) {
    let body = serde_json::json!({
        "namespace": NS,
        "query_text": text,
        "max_results": 20,
        "no_cache": true,
    });
    let response = handle_search_post(body.to_string().as_bytes(), state);
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap();
    (status, serde_json::from_str(body).unwrap_or_default())
}

fn hit_ids(result: &serde_json::Value) -> Vec<String> {
    let mut ids: Vec<String> = result["results"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|h| h["doc_id"].as_str().map(str::to_string))
        .collect();
    ids.sort();
    ids
}

fn expected_ids(word: &str, count: usize) -> Vec<String> {
    let mut ids: Vec<String> = (0..count).map(|i| format!("{word}-{i}")).collect();
    ids.sort();
    ids
}

/// Hydrate the scoring set for an unfiltered query, as a search would.
fn hydrate_scoring_set(state: &AppState, text: &str) -> HydrationOutcome {
    let ns = NamespaceId(NS.into());
    let manifest = state.indexer.manifest_cloned(&ns).unwrap();
    let query: kosha_core::SearchQuery =
        serde_json::from_value(serde_json::json!({"query_text": text, "max_results": 20})).unwrap();
    let request = Arc::new(RequestHydration::new(u64::MAX));
    state.hydrate_segments_for_search(&ns, &manifest, &query, &request, None)
}

fn fallbacks(state: &AppState) -> u64 {
    state
        .doc_store_fallbacks
        .load(std::sync::atomic::Ordering::Relaxed)
}

fn seg_key(manifest: &kosha_core::Manifest, i: usize, file: &str) -> String {
    format!("{NS}/{}/{file}", manifest.segments[i].segment_id.0)
}

/// Regression guard for the healthy case: a v1+ segment whose sidecar is in
/// S3 must take the Lazy path, fetching `doc_store.offsets` and never
/// `doc_store.bin` during scoring hydration.
#[test]
fn healthy_lazy_segments_fetch_only_the_sidecar() {
    let (s3, manifest) = uploaded_namespace();
    let state = cold_query_node(&s3, manifest.clone());

    let outcome = hydrate_scoring_set(&state, "document");
    assert!(outcome.missing.is_empty(), "{:?}", outcome.missing);
    assert_eq!(
        s3.gets_ending_with("/doc_store.offsets").len(),
        2,
        "each segment's sidecar is fetched"
    );
    assert!(
        s3.gets_ending_with("/doc_store.bin").is_empty(),
        "scoring a Lazy segment must not pull the bulk doc store: {:?}",
        s3.gets_ending_with("/doc_store.bin")
    );
    for entry in &manifest.segments {
        let seg = state.data_dir.join(NS).join(&entry.segment_id.0);
        assert!(!seg.join("doc_store.bin").exists());
    }
    assert_eq!(fallbacks(&state), 0);

    let (status, body) = search(&state, "alpha");
    assert_eq!(status, 200, "{body}");
    assert_eq!(hit_ids(&body), expected_ids("alpha", 6));
}

/// #179 / #184 (first report): `doc_store.offsets` absent from S3 for one
/// segment. Before the fix the cold search was a 503 "segment hydration
/// failed"; now it fetches that segment's `doc_store.bin`, WARNs, and
/// serves correct results. The healthy segment stays Lazy.
#[test]
fn missing_sidecar_in_s3_falls_back_to_doc_store_bin() {
    let (s3, manifest) = uploaded_namespace();
    fs::remove_file(s3.object_path(&seg_key(&manifest, 0, "doc_store.offsets"))).unwrap();
    let state = cold_query_node(&s3, manifest.clone());

    let outcome = hydrate_scoring_set(&state, "document");
    assert!(
        outcome.missing.is_empty(),
        "a missing sidecar must not fail hydration: {:?}",
        outcome.missing
    );
    assert_eq!(
        s3.gets_ending_with("/doc_store.bin"),
        vec![seg_key(&manifest, 0, "doc_store.bin")],
        "only the broken segment falls back to the bulk doc store"
    );
    assert_eq!(fallbacks(&state), 1);

    let (status, body) = search(&state, "alpha");
    assert_eq!(status, 200, "{body}");
    assert_eq!(hit_ids(&body), expected_ids("alpha", 6));

    // And on a node that searches without a separate hydration step first.
    let fresh = cold_query_node(&s3, manifest);
    let (status, body) = search(&fresh, "beta");
    assert_eq!(status, 200, "{body}");
    assert_eq!(hit_ids(&body), expected_ids("beta", 4));
}

/// #184 (comment): the sidecar is present but its doc count disagrees with
/// the footer, so the reader rejects it and falls back to parsing
/// `doc_store.bin`. Before the fix that file was never hydrated and the
/// search was a 500 "No such file or directory".
#[test]
fn sidecar_rejected_by_the_reader_falls_back_to_doc_store_bin() {
    let (s3, manifest) = uploaded_namespace();
    let key = seg_key(&manifest, 0, "doc_store.offsets");
    let mut sidecar = fs::read(s3.object_path(&key)).unwrap();
    let declared = u32::from_le_bytes(sidecar[..4].try_into().unwrap());
    sidecar[..4].copy_from_slice(&(declared + 5).to_le_bytes());
    fs::write(s3.object_path(&key), &sidecar).unwrap();
    let state = cold_query_node(&s3, manifest.clone());

    let (status, body) = search(&state, "alpha");
    assert_eq!(status, 200, "{body}");
    assert_eq!(hit_ids(&body), expected_ids("alpha", 6));
    assert_eq!(
        s3.gets_ending_with("/doc_store.bin"),
        vec![seg_key(&manifest, 0, "doc_store.bin")]
    );
    assert_eq!(fallbacks(&state), 1);
}

/// Staging's exact shape from #184: the manifest's cached footer claims more
/// documents than the segment holds, while `doc_store.offsets` and the
/// segment's own `footer.json` agree on the true count. The reader must not
/// trust the manifest copy over the segment: it reconciles against
/// `footer.json` and serves the right documents.
#[test]
fn stale_manifest_footer_doc_count_does_not_break_the_search() {
    let (s3, mut manifest) = uploaded_namespace();
    let id = manifest.segments[0].segment_id.clone();
    let mut stale = manifest.segment_footer(&id).unwrap().clone();
    let true_count = stale.doc_count;
    stale.doc_count = true_count + 5;
    manifest.segment_footers.insert(id.clone(), Arc::new(stale));
    let state = cold_query_node(&s3, manifest.clone());

    let (status, body) = search(&state, "alpha");
    assert_eq!(status, 200, "{body}");
    assert_eq!(hit_ids(&body), expected_ids("alpha", 6));
    assert!(
        state
            .data_dir
            .join(NS)
            .join(&id.0)
            .join("footer.json")
            .exists(),
        "the segment's own footer is fetched to reconcile against"
    );
    assert_eq!(fallbacks(&state), 1);
}

/// #184's misclassification: a legacy `footer.json` (no `format_version`)
/// whose manifest footer was parsed from that same legacy JSON. The segment
/// must be classified eager up front - fetching `doc_store.bin` and never
/// asking S3 for a sidecar that was never written - rather than relying on
/// the fallback.
#[test]
fn legacy_footer_without_format_version_is_classified_eager() {
    let (s3, mut manifest) = uploaded_namespace();
    let id = manifest.segments[0].segment_id.clone();
    let footer_path = s3.object_path(&seg_key(&manifest, 0, "footer.json"));
    let mut legacy: serde_json::Value =
        serde_json::from_slice(&fs::read(&footer_path).unwrap()).unwrap();
    legacy.as_object_mut().unwrap().remove("format_version");
    fs::write(&footer_path, serde_json::to_vec(&legacy).unwrap()).unwrap();
    fs::remove_file(s3.object_path(&seg_key(&manifest, 0, "doc_store.offsets"))).unwrap();
    let legacy_footer: kosha_core::Footer = serde_json::from_value(legacy).unwrap();
    manifest
        .segment_footers
        .insert(id.clone(), Arc::new(legacy_footer));
    let state = cold_query_node(&s3, manifest.clone());

    let outcome = hydrate_scoring_set(&state, "document");
    assert!(outcome.missing.is_empty(), "{:?}", outcome.missing);
    assert!(
        !s3.gets_ending_with("/doc_store.offsets").contains(&seg_key(
            &manifest,
            0,
            "doc_store.offsets"
        )),
        "a legacy segment must not be sent chasing a sidecar"
    );
    assert_eq!(
        s3.gets_ending_with("/doc_store.bin"),
        vec![seg_key(&manifest, 0, "doc_store.bin")]
    );
    assert_eq!(fallbacks(&state), 0, "classified eager, not rescued");

    let (status, body) = search(&state, "alpha");
    assert_eq!(status, 200, "{body}");
    assert_eq!(hit_ids(&body), expected_ids("alpha", 6));
}

/// A pod that already has a rejected sidecar on local disk and no
/// `doc_store.bin` (the state staging's query pods were left in) must
/// recover rather than 500 on every search: the scoring-completeness check
/// counts that segment incomplete, so hydration fetches the bulk file.
#[test]
fn locally_cached_rejected_sidecar_is_repaired_by_hydration() {
    let (s3, manifest) = uploaded_namespace();
    let state = cold_query_node(&s3, manifest.clone());
    let outcome = hydrate_scoring_set(&state, "document");
    assert!(outcome.missing.is_empty());

    let seg = state
        .data_dir
        .join(NS)
        .join(&manifest.segments[0].segment_id.0);
    let mut sidecar = fs::read(seg.join("doc_store.offsets")).unwrap();
    sidecar[..4].copy_from_slice(&999u32.to_le_bytes());
    fs::write(seg.join("doc_store.offsets"), &sidecar).unwrap();
    assert!(!seg.join("doc_store.bin").exists());

    let (status, body) = search(&state, "alpha");
    assert_eq!(status, 200, "{body}");
    assert_eq!(hit_ids(&body), expected_ids("alpha", 6));
    assert!(seg.join("doc_store.bin").exists());
}
