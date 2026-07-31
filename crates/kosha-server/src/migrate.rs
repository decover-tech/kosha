//! Direct OpenSearch → Kosha segment migration.
//!
//! This bypasses the HTTP ingest route, WAL, and per-1k auto-publish path:
//! sliced scroll workers feed one local `Indexer`, which builds large
//! immutable segments. Each completed segment is uploaded to S3 before its
//! manifest entry is published to Postgres.

use std::error::Error;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{
    sign, SignableBody, SignableRequest, SigningParams, SigningSettings,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use kosha_control::PgStore;
use kosha_core::{ControlStore, Document, DocumentId, Field, Manifest, NamespaceId};
use kosha_write::Indexer;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::s3_storage::{S3Config, S3Storage};

const DEFAULT_INDICES: &[&str] = &[
    "paragraph_index_hnsw",
    "page_index",
    "findings_index",
    "document_index",
    "completions_index",
    "cases_index",
];
const MIN_VECTOR_DIM: usize = 8;

type DynError = Box<dyn Error + Send + Sync>;
type Result<T> = std::result::Result<T, DynError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishMode {
    /// Replace the namespace manifest with only this run's segments (default).
    Replace,
    /// Keep existing segments and append this run's new segments (delta catch-up).
    Append,
}

#[derive(Debug, Clone)]
struct Args {
    indices: Vec<String>,
    namespace: Option<String>,
    workers: usize,
    scroll_size: usize,
    batch_size: usize,
    flush_docs: usize,
    limit: Option<usize>,
    keepalive: String,
    /// When set, only these OpenSearch `_id`s are copied (implies Append unless
    /// `--replace` is also passed).
    ids_file: Option<PathBuf>,
    mode: PublishMode,
}

impl Args {
    fn parse(argv: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = Self {
            indices: Vec::new(),
            namespace: None,
            workers: 4,
            scroll_size: 2_000,
            batch_size: 1_000,
            flush_docs: 20_000,
            limit: None,
            keepalive: "5m".into(),
            ids_file: None,
            mode: PublishMode::Replace,
        };
        let mut append = false;
        let mut replace = false;
        let mut it = argv.into_iter();
        while let Some(flag) = it.next() {
            let value = |it: &mut dyn Iterator<Item = String>, flag: &str| -> Result<String> {
                it.next()
                    .ok_or_else(|| format!("{flag} requires a value").into())
            };
            match flag.as_str() {
                "--index" => args.indices.push(value(&mut it, "--index")?),
                "--namespace" => args.namespace = Some(value(&mut it, "--namespace")?),
                "--workers" => args.workers = value(&mut it, "--workers")?.parse()?,
                "--scroll-size" => args.scroll_size = value(&mut it, "--scroll-size")?.parse()?,
                "--batch-size" => args.batch_size = value(&mut it, "--batch-size")?.parse()?,
                "--flush-docs" => args.flush_docs = value(&mut it, "--flush-docs")?.parse()?,
                "--limit" => args.limit = Some(value(&mut it, "--limit")?.parse()?),
                "--scroll-keepalive" => args.keepalive = value(&mut it, "--scroll-keepalive")?,
                "--ids-file" => {
                    args.ids_file = Some(PathBuf::from(value(&mut it, "--ids-file")?));
                }
                "--append" => append = true,
                "--replace" => replace = true,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown migrate argument: {other}").into()),
            }
        }
        if args.indices.is_empty() {
            args.indices = DEFAULT_INDICES.iter().map(|s| (*s).to_string()).collect();
        }
        if args.namespace.is_some() && args.indices.len() != 1 {
            return Err("--namespace requires exactly one --index".into());
        }
        if args.ids_file.is_some() && args.indices.len() != 1 {
            return Err("--ids-file requires exactly one --index".into());
        }
        if append && replace {
            return Err("--append and --replace are mutually exclusive".into());
        }
        args.mode = if replace {
            PublishMode::Replace
        } else if append || args.ids_file.is_some() {
            // Delta catch-up: keep prior segments and add only the new ones.
            PublishMode::Append
        } else {
            PublishMode::Replace
        };
        if args.workers == 0
            || args.scroll_size == 0
            || args.batch_size == 0
            || args.flush_docs == 0
        {
            return Err("workers, scroll-size, batch-size, and flush-docs must be > 0".into());
        }
        Ok(args)
    }
}

fn print_help() {
    println!(
        "Usage: kosha-server migrate [options]\n\
         \n\
         Directly build Kosha segments from OpenSearch and publish to S3/Postgres.\n\
         \n\
         By default a full run *replaces* the namespace manifest with newly-built\n\
         segments (safe re-run / full catch-up). For a delta catch-up of missing\n\
         docs only, pass --ids-file (implies --append).\n\
         \n\
         Options:\n\
           --index NAME              exact index/alias (repeatable; defaults to shared aliases)\n\
           --namespace NAME          override Kosha namespace (requires one --index)\n\
           --workers N               sliced-scroll readers per index (default 4)\n\
           --scroll-size N           hits per scroll page per worker (default 2000)\n\
           --batch-size N            docs sent to the segment writer at once (default 1000)\n\
           --flush-docs N            target docs per immutable segment (default 20000)\n\
           --limit N                 cap docs per index (smoke tests)\n\
           --scroll-keepalive VALUE  OpenSearch scroll keepalive (default 5m)\n\
           --ids-file PATH           only copy these OpenSearch _ids (one per line; implies --append)\n\
           --append                  append new segments to the existing manifest\n\
           --replace                 replace the namespace manifest (default without --ids-file)"
    );
}

#[derive(Clone)]
struct EsClient {
    base: String,
    region: String,
    credentials: SharedCredentialsProvider,
    runtime: Arc<tokio::runtime::Runtime>,
    http: reqwest::blocking::Client,
}

#[derive(Deserialize)]
struct CountResponse {
    count: usize,
}

#[derive(Deserialize)]
struct SearchResponse {
    #[serde(rename = "_scroll_id")]
    scroll_id: String,
    hits: Hits,
}

#[derive(Deserialize)]
struct Hits {
    hits: Vec<Hit>,
}

#[derive(Deserialize)]
struct Hit {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_source", default)]
    source: Map<String, Value>,
}

impl EsClient {
    fn from_env() -> Result<Self> {
        let base = std::env::var("ELASTICSEARCH_HOST")
            .map_err(|_| "ELASTICSEARCH_HOST is required for migrate")?
            .trim_end_matches('/')
            .to_string();
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".into());
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        );
        let credentials = runtime.block_on(async {
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_config::Region::new(region.clone()))
                .load()
                .await;
            config
                .credentials_provider()
                .ok_or("AWS credential provider is unavailable")
        })?;
        Ok(Self {
            base,
            region,
            credentials,
            runtime,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(300))
                .build()?,
        })
    }

    fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: http::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<T> {
        let url = format!("{}{}", self.base, path);
        let bytes = body
            .map(|value| serde_json::to_vec(&value))
            .transpose()?
            .unwrap_or_default();
        // The default provider caches credentials and refreshes expiring IRSA
        // sessions. Resolve for every request so multi-hour migrations do not
        // fail when the startup credentials expire.
        let credentials = self
            .runtime
            .block_on(self.credentials.provide_credentials())
            .map_err(|e| format!("failed to resolve AWS credentials: {e}"))?;
        let identity: Identity = credentials.into();
        let settings = SigningSettings::default();
        let params: SigningParams<'_> = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("es")
            .time(SystemTime::now())
            .settings(settings)
            .build()?
            .into();
        let headers = [("content-type", "application/json")];
        let signable = SignableRequest::new(
            method.as_str(),
            &url,
            headers.into_iter(),
            SignableBody::Bytes(&bytes),
        )?;
        let (instructions, _) = sign(signable, &params)?.into_parts();
        let mut signed = http::Request::builder()
            .method(method)
            .uri(&url)
            .header("content-type", "application/json")
            .body(bytes)?;
        instructions.apply_to_request_http1x(&mut signed);

        let (parts, body) = signed.into_parts();
        let response_method = parts.method.clone();
        let request = self
            .http
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers)
            .body(body);
        let response = request.send()?;
        let status = response.status();
        let response_body = response.text()?;
        if !status.is_success() {
            return Err(format!(
                "OpenSearch {} {} → {}: {}",
                response_method,
                path,
                status,
                response_body.chars().take(500).collect::<String>()
            )
            .into());
        }
        Ok(serde_json::from_str(&response_body)?)
    }

    fn count(&self, index: &str) -> Result<usize> {
        let response: CountResponse =
            self.request(http::Method::POST, &format!("/{index}/_count"), None)?;
        Ok(response.count)
    }

    /// Fetch docs by `_id` via OpenSearch `_mget` (delta catch-up path).
    fn mget(&self, index: &str, ids: &[String]) -> Result<Vec<Document>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        #[derive(Deserialize)]
        struct MgetResponse {
            docs: Vec<MgetDoc>,
        }
        #[derive(Deserialize)]
        struct MgetDoc {
            found: bool,
            #[serde(rename = "_id", default)]
            id: String,
            #[serde(rename = "_source", default)]
            source: Map<String, Value>,
        }
        let response: MgetResponse = self.request(
            http::Method::POST,
            &format!("/{index}/_mget"),
            Some(json!({ "ids": ids })),
        )?;
        Ok(response
            .docs
            .into_iter()
            .filter(|d| d.found)
            .map(|d| Document {
                id: DocumentId(d.id),
                fields: source_to_fields(d.source),
            })
            .collect())
    }

    fn open_scroll(
        &self,
        index: &str,
        size: usize,
        keepalive: &str,
        slice_id: usize,
        slice_max: usize,
    ) -> Result<SearchResponse> {
        // No `track_total_hits`: OpenSearch rejects it outright in a scroll
        // context, and per-index totals already come from `_count`.
        let mut body = json!({
            "size": size,
            "sort": ["_doc"],
            "query": {"match_all": {}},
        });
        if slice_max > 1 {
            body["slice"] = json!({"id": slice_id, "max": slice_max});
        }
        self.request(
            http::Method::POST,
            &format!("/{index}/_search?scroll={keepalive}"),
            Some(body),
        )
    }

    fn next_scroll(&self, scroll_id: &str, keepalive: &str) -> Result<SearchResponse> {
        self.request(
            http::Method::POST,
            "/_search/scroll",
            Some(json!({"scroll_id": scroll_id, "scroll": keepalive})),
        )
    }

    fn clear_scroll(&self, scroll_id: &str) {
        let result: Result<Value> = self.request(
            http::Method::DELETE,
            "/_search/scroll",
            Some(json!({"scroll_id": [scroll_id]})),
        );
        if let Err(error) = result {
            eprintln!("WARN: failed to clear OpenSearch scroll: {error}");
        }
    }
}

fn source_to_fields(source: Map<String, Value>) -> Vec<Field> {
    source
        .into_iter()
        .filter_map(|(name, value)| {
            if name.starts_with("__type__") {
                return None;
            }
            match value {
                Value::String(value) => Some(Field::text(name, value)),
                Value::Bool(value) => Some(Field::boolean(name, value)),
                Value::Number(value) => value.as_f64().map(|value| Field::float_val(name, value)),
                Value::Array(values)
                    if values.len() >= MIN_VECTOR_DIM && values.iter().all(Value::is_number) =>
                {
                    let vector = values
                        .iter()
                        .filter_map(Value::as_f64)
                        .map(|value| value as f32)
                        .collect();
                    Some(Field::vector(name, vector))
                }
                Value::Array(values) => serde_json::to_string(&values)
                    .ok()
                    .map(|value| Field::keyword(name, value)),
                Value::Null | Value::Object(_) => None,
            }
        })
        .collect()
}

fn hit_to_document(hit: Hit) -> Document {
    Document {
        id: DocumentId(hit.id),
        fields: source_to_fields(hit.source),
    }
}

fn read_ids_file(path: &PathBuf) -> Result<Vec<String>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read --ids-file {}: {e}", path.display()))?;
    let mut ids = Vec::new();
    for line in text.lines() {
        let id = line.trim();
        if id.is_empty() || id.starts_with('#') {
            continue;
        }
        ids.push(id.to_string());
    }
    Ok(ids)
}

fn copy_ids(
    es: &EsClient,
    index: &str,
    ids: &[String],
    batch_size: usize,
    sender: mpsc::SyncSender<Vec<Document>>,
) -> Result<()> {
    for chunk in ids.chunks(batch_size.max(1)) {
        let docs = es.mget(index, chunk)?;
        if !docs.is_empty() {
            sender.send(docs)?;
        }
    }
    Ok(())
}

fn scroll_index(
    es: &EsClient,
    index: &str,
    args: &Args,
    sender: mpsc::SyncSender<Vec<Document>>,
) -> Result<()> {
    let claimed = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(args.workers);
    for slice_id in 0..args.workers {
        let es = es.clone();
        let index = index.to_string();
        let args = args.clone();
        let sender = sender.clone();
        let claimed = Arc::clone(&claimed);
        handles.push(thread::spawn(move || -> Result<()> {
            let mut page = es.open_scroll(
                &index,
                args.scroll_size,
                &args.keepalive,
                slice_id,
                args.workers,
            )?;
            loop {
                if page.hits.hits.is_empty() {
                    break;
                }
                let mut documents = Vec::with_capacity(args.batch_size);
                for hit in page.hits.hits {
                    let position = claimed.fetch_add(1, Ordering::Relaxed);
                    if args.limit.is_some_and(|limit| position >= limit) {
                        break;
                    }
                    documents.push(hit_to_document(hit));
                    if documents.len() >= args.batch_size {
                        sender.send(std::mem::take(&mut documents))?;
                    }
                }
                if !documents.is_empty() {
                    sender.send(documents)?;
                }
                if args
                    .limit
                    .is_some_and(|limit| claimed.load(Ordering::Relaxed) >= limit)
                {
                    break;
                }
                page = es.next_scroll(&page.scroll_id, &args.keepalive)?;
            }
            es.clear_scroll(&page.scroll_id);
            Ok(())
        }));
    }
    drop(sender);
    for handle in handles {
        handle
            .join()
            .map_err(|_| "OpenSearch scroll worker panicked")??;
    }
    Ok(())
}

struct Publisher {
    data_dir: PathBuf,
    s3: S3Storage,
    store: PgStore,
    original_entries: usize,
    published_entries: usize,
    replacement: Manifest,
}

impl Publisher {
    fn new(
        data_dir: PathBuf,
        s3: S3Storage,
        store: PgStore,
        existing: &Manifest,
        mode: PublishMode,
    ) -> Self {
        let replacement = match mode {
            PublishMode::Replace => Manifest {
                version: existing.version,
                segments: Vec::new(),
            },
            PublishMode::Append => existing.clone(),
        };
        Self {
            data_dir,
            s3,
            store,
            // Indexer still holds restored prior segments; skip them when
            // selecting newly flushed entries to publish.
            original_entries: existing.segments.len(),
            published_entries: 0,
            replacement,
        }
    }

    fn publish_new(&mut self, indexer: &Indexer, namespace: &NamespaceId) -> Result<()> {
        let manifest = indexer
            .manifest_cloned(namespace)
            .ok_or("indexer did not produce a manifest")?;
        let start = self.original_entries + self.published_entries;
        for entry in manifest.segments.iter().skip(start).cloned() {
            let logical_dir = PathBuf::from(&namespace.0).join(&entry.segment_id.0);
            self.s3.sync_segment_dir(&logical_dir)?;
            self.replacement.version += 1;
            self.replacement.segments.push(entry.clone());
            self.store.save_manifest(namespace, &self.replacement)?;
            let local_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
            if let Err(error) = std::fs::remove_dir_all(&local_dir) {
                eprintln!(
                    "WARN: failed to remove uploaded segment cache {}: {error}",
                    local_dir.display()
                );
            }
            self.published_entries += 1;
            println!(
                "migrate: published namespace={} segment={} docs={} manifest_version={}",
                namespace.0, entry.segment_id.0, entry.doc_count, self.replacement.version
            );
        }
        Ok(())
    }
}

pub fn run(argv: impl IntoIterator<Item = String>) -> Result<()> {
    let args = Args::parse(argv)?;
    let es = EsClient::from_env()?;
    let data_dir = PathBuf::from(
        std::env::var("KOSHA_DATA_DIR").unwrap_or_else(|_| "/var/lib/kosha/data".into()),
    );
    std::fs::create_dir_all(&data_dir)?;
    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is required for migrate")?;
    let s3_config = S3Config::from_env().ok_or("KOSHA_S3_BUCKET is required for migrate")?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut s3 = rt.block_on(S3Storage::new(data_dir.clone(), s3_config))?;
    let mut store = PgStore::new(&database_url)?;

    println!(
        "migrate: indices={:?} workers={} scroll_size={} batch_size={} flush_docs={} limit={:?} mode={:?} ids_file={:?}",
        args.indices,
        args.workers,
        args.scroll_size,
        args.batch_size,
        args.flush_docs,
        args.limit,
        args.mode,
        args.ids_file,
    );

    let ids_file_ids = match &args.ids_file {
        Some(path) => Some(read_ids_file(path)?),
        None => None,
    };

    for index in &args.indices {
        let total = if let Some(ids) = &ids_file_ids {
            ids.len()
        } else {
            match es.count(index) {
                Ok(total) => total,
                Err(error) if error.to_string().contains("404") => {
                    eprintln!("WARN: index/alias {index:?} not found — skipping");
                    continue;
                }
                Err(error) => return Err(error),
            }
        };
        let planned = args.limit.map_or(total, |limit| limit.min(total));
        let namespace = NamespaceId(args.namespace.clone().unwrap_or_else(|| index.clone()));
        let mut indexer = Indexer::new(data_dir.clone())
            .with_wal(false)
            .with_flush_threshold(args.flush_docs);

        let existing = store.manifest_cloned(&namespace).unwrap_or(Manifest {
            version: 0,
            segments: Vec::new(),
        });
        // Restore to advance segment IDs beyond an existing/partial run.
        // Replace mode: Publisher publishes only this run's segments.
        // Append mode: Publisher keeps existing segments and adds new ones.
        indexer.restore_manifest(namespace.clone(), existing.clone());
        let mut publisher = Publisher::new(data_dir.clone(), s3, store, &existing, args.mode);
        let started = Instant::now();
        let (sender, receiver) = mpsc::sync_channel::<Vec<Document>>(args.workers * 2);
        let es_worker = es.clone();
        let index_worker = index.clone();
        let args_worker = args.clone();
        let ids_worker = ids_file_ids.clone();
        let producer = thread::spawn(move || -> Result<()> {
            if let Some(mut ids) = ids_worker {
                if let Some(limit) = args_worker.limit {
                    ids.truncate(limit);
                }
                copy_ids(
                    &es_worker,
                    &index_worker,
                    &ids,
                    args_worker.batch_size,
                    sender,
                )
            } else {
                scroll_index(&es_worker, &index_worker, &args_worker, sender)
            }
        });

        let mut copied = 0usize;
        while let Ok(documents) = receiver.recv() {
            copied += documents.len();
            let before = indexer
                .manifest(&namespace)
                .map_or(0, |manifest| manifest.segments.len());
            indexer.index_documents(namespace.clone(), documents)?;
            let after = indexer
                .manifest(&namespace)
                .map_or(0, |manifest| manifest.segments.len());
            if after > before {
                publisher.publish_new(&indexer, &namespace)?;
            }
            if copied % 10_000 < args.batch_size {
                println!(
                    "migrate: index={} {}/{} docs ({:.0} docs/sec)",
                    index,
                    copied,
                    planned,
                    copied as f64 / started.elapsed().as_secs_f64().max(0.001)
                );
            }
        }
        producer
            .join()
            .map_err(|_| "OpenSearch producer thread panicked")??;

        indexer.flush_namespace(&namespace)?;
        publisher.publish_new(&indexer, &namespace)?;
        println!(
            "migrate: completed index={} docs={} elapsed={:.1}s throughput={:.0} docs/sec mode={:?}",
            index,
            copied,
            started.elapsed().as_secs_f64(),
            copied as f64 / started.elapsed().as_secs_f64().max(0.001),
            args.mode,
        );

        // Move owned clients to the next namespace.
        s3 = publisher.s3;
        store = publisher.store;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::FieldType;
    use std::collections::HashSet;

    #[test]
    fn maps_es_fields_like_python_client() {
        let source = serde_json::from_value::<Map<String, Value>>(json!({
            "title": "hello",
            "active": true,
            "score": 42,
            "embedding": [1, 2, 3, 4, 5, 6, 7, 8],
            "tags": ["a", "b"],
            "nested": {"ignored": true},
            "__type__title": "text"
        }))
        .unwrap();
        let fields = source_to_fields(source);
        let names: HashSet<_> = fields.iter().map(|field| field.name.as_str()).collect();
        assert_eq!(
            names,
            HashSet::from(["title", "active", "score", "embedding", "tags"])
        );
        assert_eq!(
            fields
                .iter()
                .find(|field| field.name == "score")
                .unwrap()
                .field_type,
            FieldType::Float
        );
        assert_eq!(
            fields
                .iter()
                .find(|field| field.name == "embedding")
                .unwrap()
                .field_type,
            FieldType::Vector
        );
    }

    #[test]
    fn parses_defaults_and_repeated_indices() {
        let args = Args::parse([
            "--index".into(),
            "a".into(),
            "--index".into(),
            "b".into(),
            "--workers".into(),
            "8".into(),
        ])
        .unwrap();
        assert_eq!(args.indices, ["a", "b"]);
        assert_eq!(args.workers, 8);
        assert_eq!(args.flush_docs, 20_000);
        assert_eq!(args.mode, PublishMode::Replace);
    }

    #[test]
    fn ids_file_implies_append_mode() {
        let args = Args::parse([
            "--index".into(),
            "paragraph_index_hnsw".into(),
            "--ids-file".into(),
            "/tmp/missing-ids.txt".into(),
        ])
        .unwrap();
        assert_eq!(args.mode, PublishMode::Append);
        assert_eq!(
            args.ids_file.as_deref(),
            Some(std::path::Path::new("/tmp/missing-ids.txt"))
        );
    }
}
