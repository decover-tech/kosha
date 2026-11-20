//! S3 storage backend — optional, gated behind `s3` feature.
//!
//! Uses `aws-sdk-s3` for multi-part upload of large segments.
//! Kept separate from core so Kosha remains S3-free for open-source.
//!
//! Env knobs (resolved by the server before calling [`S3Storage::new`]):
//!   - `KOSHA_S3_BUCKET` (required to enable)
//!   - `KOSHA_S3_PREFIX` (optional key prefix)
//!   - `KOSHA_S3_ENDPOINT` or `AWS_ENDPOINT_URL` (MinIO / custom endpoint)
//!   - `KOSHA_S3_ACCESS_KEY` / `KOSHA_S3_SECRET_KEY` (or standard AWS_* creds)
//!   - `KOSHA_S3_FORCE_PATH_STYLE=true` (default when a custom endpoint is set)

use std::fmt::{self, Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kosha_core::{KoshaError, LocalStorage, ObjectMeta, StorageBackend};

use crate::hydration_budget::{HydrationBudget, RequestHydration};
use crate::segment_commit;

/// Join a configured key prefix with a logical (unprefixed) path, the same
/// way for every call site — `S3Storage::s3_key` and the concurrent fetch
/// path in `read_many` both delegate here so they can't drift apart.
fn join_s3_key(prefix: &str, path: &str) -> String {
    let path = path.trim_start_matches('/');
    if prefix.is_empty() {
        path.to_string()
    } else {
        format!("{}/{}", prefix.trim_end_matches('/'), path)
    }
}

/// The logical (unprefixed) path of an S3 key listed under `key_prefix`,
/// the inverse of [`join_s3_key`]. `None` for a key outside the prefix.
fn logical_path_of_key(key_prefix: &str, key: &str) -> Option<String> {
    let prefix = key_prefix.trim_end_matches('/');
    let rest = if prefix.is_empty() {
        key
    } else {
        key.strip_prefix(prefix)?.strip_prefix('/')?
    };
    let rest = rest.trim_start_matches('/');
    (!rest.is_empty()).then(|| rest.to_string())
}

/// Configuration for the S3-backed segment store.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub prefix: String,
    /// Custom endpoint (e.g. `http://minio:9000`). Empty → real AWS.
    pub endpoint: Option<String>,
    /// Force path-style URLs (`http://endpoint/bucket/key`). Required for MinIO.
    pub force_path_style: bool,
    /// Optional static credentials. When `None`, the AWS SDK default chain is used.
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub region: Option<String>,
}

impl S3Config {
    /// Resolve config from process environment.
    ///
    /// Returns `None` when `KOSHA_S3_BUCKET` is unset (S3 disabled).
    pub fn from_env() -> Option<Self> {
        let bucket = std::env::var("KOSHA_S3_BUCKET").ok()?;
        let prefix = std::env::var("KOSHA_S3_PREFIX").unwrap_or_default();
        let endpoint = std::env::var("KOSHA_S3_ENDPOINT")
            .ok()
            .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok())
            .filter(|s| !s.is_empty());
        let force_path_style = match std::env::var("KOSHA_S3_FORCE_PATH_STYLE") {
            Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"),
            // MinIO and most S3-compatible stores need path-style.
            Err(_) => endpoint.is_some(),
        };
        let access_key = std::env::var("KOSHA_S3_ACCESS_KEY")
            .ok()
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
            .filter(|s| !s.is_empty());
        let secret_key = std::env::var("KOSHA_S3_SECRET_KEY")
            .ok()
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
            .filter(|s| !s.is_empty());
        let region = std::env::var("AWS_DEFAULT_REGION")
            .ok()
            .or_else(|| std::env::var("AWS_REGION").ok())
            .or_else(|| {
                if endpoint.is_some() {
                    Some("us-east-1".into())
                } else {
                    None
                }
            });

        Some(Self {
            bucket,
            prefix,
            endpoint,
            force_path_style,
            access_key,
            secret_key,
            region,
        })
    }
}

/// S3-backed storage that wraps a local cache and uploads/downloads
/// segment files to/from an S3 bucket.
///
/// Write path:
///   1. Write to local cache (fast, synchronous)
///   2. Upload to S3 (async, via tokio::runtime::Runtime)
///
/// Read path:
///   1. Check local cache first
///   2. On miss, download from S3 to local cache, then serve
pub struct S3Storage {
    local: LocalStorage,
    bucket: String,
    prefix: String,
    rt: tokio::runtime::Runtime,
    client: aws_sdk_s3::Client,
}

impl Debug for S3Storage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Storage")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("local_root", &self.local.root)
            .finish()
    }
}

impl S3Storage {
    /// Create a new S3 storage backend from resolved [`S3Config`].
    pub async fn new(local_root: PathBuf, config: S3Config) -> Result<Self, KoshaError> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(ref region) = config.region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let (Some(key), Some(secret)) = (&config.access_key, &config.secret_key) {
            let creds = aws_sdk_s3::config::Credentials::new(key, secret, None, None, "kosha-env");
            loader = loader.credentials_provider(creds);
        }
        let shared = loader.load().await;

        let mut s3_builder = aws_sdk_s3::config::Builder::from(&shared);
        if let Some(ref endpoint) = config.endpoint {
            s3_builder = s3_builder.endpoint_url(endpoint);
        }
        if config.force_path_style {
            s3_builder = s3_builder.force_path_style(true);
        }
        let client = aws_sdk_s3::Client::from_conf(s3_builder.build());

        let local = LocalStorage::new(local_root);
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| KoshaError::NotFound(format!("failed to create tokio runtime: {e}")))?;

        Ok(Self {
            local,
            bucket: config.bucket,
            prefix: config.prefix,
            rt,
            client,
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn s3_key(&self, path: &str) -> String {
        join_s3_key(&self.prefix, path)
    }

    /// Upload a locally-built segment directory and commit it (#231): every
    /// data file first, in sorted order, then the `_COMMIT` marker last,
    /// listing each file's size and CRC32. Only a segment whose `_COMMIT`
    /// landed counts as durable (`segment_durability`), so a failure
    /// partway through leaves a prefix that reconciliation will re-upload.
    /// The marker is also written locally, so this node's later
    /// verification sees the same record S3 has.
    ///
    /// `logical_dir` is relative to the storage root, for example
    /// `paragraph_index_hnsw/paragraph_index_hnsw-000001`. Callers check
    /// that the directory holds a complete segment.
    pub fn sync_segment_dir(&self, logical_dir: &Path) -> Result<(), KoshaError> {
        let local_dir = Path::new(&self.local.root).join(logical_dir);
        let mut names: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&local_dir).map_err(KoshaError::Io)? {
            let entry = entry.map_err(KoshaError::Io)?;
            if !entry.path().is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            // A local `_COMMIT` from an earlier attempt is rebuilt, never
            // uploaded ahead of the files it vouches for. In-progress
            // atomic-write temps are never segment data: writer temps are
            // `.{name}.tmp.{pid}.{n}`, hydration temps `{stem}.tmp.{pid}.{n}`.
            // Deletion generations are uploaded with the manifest that
            // references them, not as segment data, so GC can drop a
            // superseded one without breaking this segment's `_COMMIT`.
            if name == segment_commit::COMMIT_FILE
                || name.starts_with('.')
                || name.contains(".tmp.")
                || kosha_core::gc::deletion_generation_of(&name).is_some()
            {
                continue;
            }
            names.push(name);
        }
        names.sort();

        // The data files go up concurrently, at most `upload_concurrency`
        // at a time; `_COMMIT` still goes last, alone, after every one of
        // them has succeeded, so the marker's meaning is unchanged. A
        // split-postings segment is a few large files plus up to 256 small
        // posting shards: uploaded one after another, a flush spent nearly
        // all of its time waiting on one S3 round trip per file with the
        // CPU idle.
        //
        // Each task reads its own file, so at most `upload_concurrency`
        // files are in memory at once, not the whole segment.
        let limit = Arc::new(tokio::sync::Semaphore::new(upload_concurrency()));
        let uploads: Vec<(String, PathBuf, String)> = names
            .iter()
            .map(|name| {
                let logical_path = logical_dir.join(name).to_string_lossy().into_owned();
                (
                    name.clone(),
                    local_dir.join(name),
                    self.s3_key(&logical_path),
                )
            })
            .collect();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let files = self.rt.block_on(async move {
            let mut tasks = tokio::task::JoinSet::new();
            for (name, path, key) in uploads {
                let client = client.clone();
                let bucket = bucket.clone();
                let limit = Arc::clone(&limit);
                tasks.spawn(async move {
                    let _permit = limit.acquire_owned().await;
                    let data = tokio::fs::read(&path).await.map_err(KoshaError::Io)?;
                    let entry = segment_commit::SegmentCommit::entry(&name, &data);
                    put_bytes(client, bucket, key, data).await?;
                    Ok::<_, KoshaError>(entry)
                });
            }
            // Every upload is attempted, so a retry has only the failed
            // files left to do at the S3 end; the first error is reported.
            let mut files = Vec::new();
            let mut first_error: Option<KoshaError> = None;
            while let Some(joined) = tasks.join_next().await {
                match joined {
                    Ok(Ok(entry)) => files.push(entry),
                    Ok(Err(e)) => {
                        first_error.get_or_insert(e);
                    }
                    Err(e) => {
                        first_error.get_or_insert(KoshaError::NotFound(format!(
                            "S3 upload task failed: {e}"
                        )));
                    }
                }
            }
            match first_error {
                Some(e) => Err(e),
                None => Ok(files),
            }
        })?;
        let commit = segment_commit::SegmentCommit::new(files).to_bytes();
        let commit_path = logical_dir
            .join(segment_commit::COMMIT_FILE)
            .to_string_lossy()
            .into_owned();
        self.local.write(&commit_path, &commit)?;
        self.upload_bytes(commit, &self.s3_key(&commit_path))
    }

    /// Upload a local file to S3. See `upload_bytes`.
    fn upload_to_s3(&self, local_path: &Path, s3_key: &str) -> Result<(), KoshaError> {
        let data = std::fs::read(local_path).map_err(KoshaError::Io)?;
        self.upload_bytes(data, s3_key)
    }

    /// Upload bytes to S3. Uses multi-part upload for payloads > 5MB.
    fn upload_bytes(&self, data: Vec<u8>, s3_key: &str) -> Result<(), KoshaError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = s3_key.to_string();
        self.rt
            .block_on(async move { put_bytes(client, bucket, key, data).await })
    }

    /// Download multiple logical paths concurrently, bounded by
    /// `max_concurrent` in-flight requests, each persisted to local cache
    /// exactly as [`StorageBackend::read`] does.
    ///
    /// Segment hydration (DESIGN.md §8 step 4 calls for retrieval "fanned
    /// out across the query node's worker pool") used to fetch one file at a
    /// time via repeated `read()` calls — total latency was the sum of every
    /// S3 GET across every segment/file in the manifest. This fans requests
    /// out as concurrent tasks on the storage runtime instead, so hydrating
    /// N missing files costs roughly `ceil(N / max_concurrent)` GET
    /// latencies rather than N.
    ///
    /// Returns one `(logical_path, result)` pair per input path, in
    /// completion order (not input order).
    ///
    /// Callers only ever need success/failure plus the file `fetch_one`
    /// leaves on disk — never the bytes themselves (`ensure_segments_local`
    /// just tells `Cache` a file landed; nothing here parses a segment).
    /// Returning `Result<(), _>` instead of `Result<Vec<u8>, _>` avoids two
    /// costs that scaled with the batch: the extra copy inside `fetch_one`
    /// just to hand back an owned `Vec<u8>` nobody read, and the resulting
    /// `results` vec holding every fetched file's full bytes in memory at
    /// once for the batch's whole lifetime (this function doesn't return
    /// until every spawned fetch has completed).
    pub fn read_many(
        &self,
        paths: &[String],
        max_concurrent: usize,
        budget: Option<&Arc<HydrationBudget>>,
        request: &Arc<RequestHydration>,
    ) -> Vec<(String, Result<FetchStats, KoshaError>)> {
        if paths.is_empty() {
            return Vec::new();
        }
        let local_root = self.local.root.clone();
        let bucket = self.bucket.clone();
        let prefix = self.prefix.clone();
        let client = self.client.clone();
        let budget = budget.cloned();
        let request = Arc::clone(request);
        let mut pending: std::collections::VecDeque<String> = paths.to_vec().into();
        let max_concurrent = max_concurrent.max(1);

        self.rt.block_on(async move {
            let mut set = tokio::task::JoinSet::new();
            let spawn_next =
                |set: &mut tokio::task::JoinSet<(String, Result<FetchStats, KoshaError>)>,
                 pending: &mut std::collections::VecDeque<String>| {
                    let Some(path) = pending.pop_front() else {
                        return;
                    };
                    let client = client.clone();
                    let bucket = bucket.clone();
                    let prefix = prefix.clone();
                    let local_root = local_root.clone();
                    let budget = budget.clone();
                    let request = Arc::clone(&request);
                    set.spawn(async move {
                        let result = fetch_one(
                            &client,
                            &bucket,
                            &prefix,
                            &local_root,
                            &path,
                            budget,
                            request,
                        )
                        .await;
                        (path, result)
                    });
                };

            for _ in 0..max_concurrent.min(pending.len()) {
                spawn_next(&mut set, &mut pending);
            }

            let mut results = Vec::with_capacity(paths.len());
            while let Some(joined) = set.join_next().await {
                if let Ok(pair) = joined {
                    results.push(pair);
                }
                spawn_next(&mut set, &mut pending);
            }
            results
        })
    }

    /// Ranged-read counterpart of `read_many`, for page materialization
    /// (#245): fetch each `(logical_path, offset, length)` range as one S3
    /// ranged GET (`Range: bytes=offset-offset+length-1`), KBs instead of
    /// the whole object, with the same concurrent fan-out and bounded retry
    /// as whole-file fetches. Bytes are returned, not written: the caller
    /// persists them into the sparse span cache
    /// (`kosha_segment::span_cache`), never at the file's real path, which
    /// every existence check treats as complete.
    ///
    /// Each range is charged to `request` and holds `length` bytes of the
    /// process-wide `budget` while in flight, like a whole-file fetch.
    ///
    /// Returns one result per input range, index-aligned. A range may come
    /// back shorter than asked when it runs past the end of the object
    /// (header probes rely on that); callers needing an exact length check
    /// it. A range whose file is already whole on local disk is served from
    /// disk — a concurrent whole-file hydration may have landed it.
    pub fn read_ranges(
        &self,
        ranges: &[(String, u64, u64)],
        max_concurrent: usize,
        budget: Option<&Arc<HydrationBudget>>,
        request: &Arc<RequestHydration>,
    ) -> Vec<Result<Vec<u8>, KoshaError>> {
        if ranges.is_empty() {
            return Vec::new();
        }
        let local_root = self.local.root.clone();
        let bucket = self.bucket.clone();
        let prefix = self.prefix.clone();
        let client = self.client.clone();
        let limit = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1)));
        let budget = budget.cloned();

        self.rt.block_on(async move {
            let mut set = tokio::task::JoinSet::new();
            for (idx, (path, offset, length)) in ranges.iter().enumerate() {
                let (client, bucket, prefix) = (client.clone(), bucket.clone(), prefix.clone());
                let local_root = local_root.clone();
                let path = path.clone();
                let (offset, length) = (*offset, *length);
                let limit = std::sync::Arc::clone(&limit);
                let budget = budget.clone();
                let request = Arc::clone(request);
                set.spawn(async move {
                    let _slot = limit.acquire_owned().await;
                    let result = fetch_range_one(
                        &client,
                        &bucket,
                        &prefix,
                        &local_root,
                        &path,
                        offset,
                        length,
                        budget,
                        &request,
                    )
                    .await;
                    (idx, path, result)
                });
            }

            let mut out: Vec<Result<Vec<u8>, KoshaError>> = (0..ranges.len())
                .map(|_| Err(KoshaError::NotFound("ranged GET did not complete".into())))
                .collect();
            while let Some(joined) = set.join_next().await {
                if let Ok((idx, path, result)) = joined {
                    if let Err(ref e) = result {
                        if !matches!(e, KoshaError::Overloaded(_)) {
                            eprintln!("WARN: ranged GET of {path} failed: {e}");
                        }
                    }
                    out[idx] = result;
                }
            }
            out
        })
    }

    /// Download a file from S3 to local cache.
    fn download_from_s3(&self, s3_key: &str, local_path: &Path) -> Result<Vec<u8>, KoshaError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = s3_key.to_string();

        let data = self.rt.block_on(async move {
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 get_object: {e}")))?;

            let bytes = resp
                .body
                .collect()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 read body: {e}")))?
                .into_bytes();
            Ok::<Vec<u8>, KoshaError>(bytes.to_vec())
        })?;

        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(local_path, &data).map_err(KoshaError::Io)?;

        Ok(data)
    }

    /// List object key basenames (and sizes) under a logical (unprefixed)
    /// directory path directly from S3, no local fallback — see the public
    /// `list_with_sizes`, the only caller. Sizes are 0 if S3 didn't report
    /// one (`Object::size()` is optional per the SDK).
    pub(crate) fn list_remote_with_sizes(
        &self,
        path: &str,
    ) -> Result<Vec<(String, u64)>, KoshaError> {
        let mut prefix = self.s3_key(path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix_for_strip = prefix.clone();

        self.rt.block_on(async move {
            let resp = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .send()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 list_objects_v2: {e}")))?;

            let mut out = Vec::new();
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let relative = key
                        .strip_prefix(&prefix_for_strip)
                        .unwrap_or(key)
                        .trim_start_matches('/');
                    // Only immediate children (files), not nested paths.
                    if !relative.is_empty() && !relative.contains('/') {
                        let size = obj.size().unwrap_or(0).max(0) as u64;
                        out.push((relative.to_string(), size));
                    }
                }
            }
            Ok(out)
        })
    }

    /// Immediate child "directories" under a logical path — the S3
    /// common-prefix listing (delimiter `/`), fully paginated. For a
    /// namespace path this returns its segment ids. `list_with_sizes`
    /// cannot serve this purpose: it returns only immediate child *files*
    /// (nested keys are filtered out) and reads a single unpaginated page —
    /// the import-namespace endpoint's original listing bug.
    pub fn list_child_dirs(&self, path: &str) -> Result<Vec<String>, KoshaError> {
        let mut prefix = self.s3_key(path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix_for_strip = prefix.clone();

        self.rt.block_on(async move {
            let mut out = Vec::new();
            let mut continuation: Option<String> = None;
            loop {
                let mut req = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&prefix)
                    .delimiter("/");
                if let Some(token) = continuation.take() {
                    req = req.continuation_token(token);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| KoshaError::NotFound(format!("S3 list_objects_v2: {e}")))?;
                for cp in resp.common_prefixes() {
                    if let Some(p) = cp.prefix() {
                        let child = p
                            .strip_prefix(&prefix_for_strip)
                            .unwrap_or(p)
                            .trim_end_matches('/');
                        if !child.is_empty() {
                            out.push(child.to_string());
                        }
                    }
                }
                match resp.next_continuation_token() {
                    Some(t) => continuation = Some(t.to_string()),
                    None => break,
                }
            }
            Ok(out)
        })
    }

    /// Total object bytes per segment under a namespace path, from one
    /// paginated ListObjectsV2 over the whole namespace prefix (no
    /// delimiter): about one request per 1,000 objects, instead of one LIST
    /// per segment or a `_COMMIT` GET each. Compaction plans merge groups
    /// from these sizes before hydrating anything (#242). `wants(file)`
    /// filters which files count (the files hydration would fetch).
    ///
    /// Keys deeper than `<segment>/<file>` are counted against their
    /// segment too. A segment with no listed objects is absent from the map.
    pub fn segment_sizes(
        &self,
        ns_path: &str,
        wants: impl Fn(&str) -> bool,
    ) -> Result<std::collections::HashMap<String, u64>, KoshaError> {
        Ok(sum_segment_sizes(
            self.list_namespace_objects(ns_path)?,
            wants,
        ))
    }

    /// Every object under a namespace prefix as `(<segment>/<file>, size)`,
    /// from one paginated LIST (about one request per 1,000 objects). The
    /// raw listing behind [`Self::segment_sizes`]; usage-driven warming
    /// (#213) sizes each file class of each segment from it.
    pub fn list_namespace_objects(&self, ns_path: &str) -> Result<Vec<(String, u64)>, KoshaError> {
        let mut prefix = self.s3_key(ns_path);
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let objects: Vec<(String, u64)> = self.rt.block_on(async move {
            let mut out = Vec::new();
            let mut continuation: Option<String> = None;
            loop {
                let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                if let Some(token) = continuation.take() {
                    req = req.continuation_token(token);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| KoshaError::NotFound(format!("S3 list_objects_v2: {e}")))?;
                for obj in resp.contents() {
                    if let Some(key) = obj.key() {
                        let relative = key.strip_prefix(&prefix).unwrap_or(key);
                        let size = obj.size().unwrap_or(0).max(0) as u64;
                        out.push((relative.trim_start_matches('/').to_string(), size));
                    }
                }
                match resp.next_continuation_token() {
                    Some(t) => continuation = Some(t.to_string()),
                    None => break,
                }
            }
            Ok::<_, KoshaError>(out)
        })?;
        Ok(objects)
    }

    /// Every object under a logical prefix, recursively and fully paginated,
    /// with size and `LastModified`: the listing segment GC (#239) decides
    /// from. S3 only, never the local cache: GC must judge what the bucket
    /// holds.
    pub fn list_objects_remote(&self, prefix: &str) -> Result<Vec<ObjectMeta>, KoshaError> {
        let key_prefix = self.s3_key(prefix);
        let root = self.prefix.clone();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.rt.block_on(async move {
            let mut out = Vec::new();
            let mut continuation: Option<String> = None;
            loop {
                let mut req = client.list_objects_v2().bucket(&bucket).prefix(&key_prefix);
                if let Some(token) = continuation.take() {
                    req = req.continuation_token(token);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| KoshaError::NotFound(format!("S3 list_objects_v2: {e}")))?;
                for obj in resp.contents() {
                    let Some(path) = obj.key().and_then(|k| logical_path_of_key(&root, k)) else {
                        continue;
                    };
                    out.push(ObjectMeta {
                        path,
                        size: obj.size().unwrap_or(0).max(0) as u64,
                        // An object S3 reports no time for is treated as
                        // brand new, so GC's age checks keep it.
                        last_modified_ms: obj
                            .last_modified()
                            .and_then(|t| t.to_millis().ok())
                            .map_or(u64::MAX, |ms| ms.max(0) as u64),
                    });
                }
                match resp.next_continuation_token() {
                    Some(t) if resp.is_truncated().unwrap_or(false) => {
                        continuation = Some(t.to_string())
                    }
                    _ => break,
                }
            }
            Ok(out)
        })
    }

    /// Delete one object from S3, surfacing failures (unlike the trait's
    /// `delete`, which predates GC and is best-effort remotely).
    pub fn delete_remote(&self, path: &str) -> Result<(), KoshaError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.s3_key(path);
        self.rt.block_on(async move {
            client
                .delete_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .map(|_| ())
                .map_err(|e| KoshaError::NotFound(format!("S3 delete_object {key}: {e}")))
        })
    }

    /// Ground-truth durability check for one segment prefix, unlike
    /// `StorageBackend::exists` (local disk only) or `list_with_sizes`/`list`
    /// (which fall back to local on an empty S3 listing, so they can't tell
    /// "durable in S3" from "only ever existed on this node's disk").
    ///
    /// Durable means a `_COMMIT` marker exists and every file it records is
    /// present at the recorded size. Segments uploaded before the marker
    /// existed instead need every required file present and non-empty. See
    /// `segment_commit` (#231). The old check here was "any object exists",
    /// which counted a segment whose upload died halfway as durable.
    ///
    /// Used by the ingest-pod boot-time reconciliation sweep
    /// (`AppState::new`) to find segments a prior process crashed or raced
    /// before fully uploading.
    pub fn segment_durability(&self, logical_dir: &str) -> segment_commit::Durability {
        let listing = match self.list_remote_with_sizes(logical_dir) {
            Ok(l) => l,
            Err(e) => {
                return segment_commit::Durability::Incomplete(format!("S3 list failed: {e}"))
            }
        };
        if !listing
            .iter()
            .any(|(n, _)| n == segment_commit::COMMIT_FILE)
        {
            return segment_commit::assess_listing(&listing, None);
        }
        let commit = self
            .read_remote(&format!("{logical_dir}/{}", segment_commit::COMMIT_FILE))
            .map_err(|e| e.to_string())
            .and_then(|bytes| segment_commit::SegmentCommit::from_bytes(&bytes));
        match &commit {
            Ok(c) => segment_commit::assess_listing(&listing, Some(Ok(c))),
            Err(e) => segment_commit::assess_listing(&listing, Some(Err(e.as_str()))),
        }
    }

    /// Fetch the remote `_COMMIT` of a segment, if S3 lists one. `Ok(None)`
    /// means S3 has no marker for it (a legacy segment); errors are list,
    /// GET or parse failures, which callers must not mistake for "absent".
    pub fn read_segment_commit(
        &self,
        logical_dir: &str,
    ) -> Result<Option<segment_commit::SegmentCommit>, KoshaError> {
        let listing = self.list_remote_with_sizes(logical_dir)?;
        if !listing
            .iter()
            .any(|(n, _)| n == segment_commit::COMMIT_FILE)
        {
            return Ok(None);
        }
        let bytes = self.read_remote(&format!("{logical_dir}/{}", segment_commit::COMMIT_FILE))?;
        segment_commit::SegmentCommit::from_bytes(&bytes)
            .map(Some)
            .map_err(KoshaError::CorruptSegment)
    }

    /// GET an object straight from S3, bypassing (and not populating) the
    /// local cache. For small control objects such as `_COMMIT`, where a
    /// stale local copy would defeat the point of asking S3.
    pub fn read_remote(&self, path: &str) -> Result<Vec<u8>, KoshaError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.s3_key(path);
        self.rt.block_on(async move {
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 get_object {key}: {e}")))?;
            let bytes = resp
                .body
                .collect()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 read body {key}: {e}")))?
                .into_bytes();
            Ok(bytes.to_vec())
        })
    }

    /// Like the `StorageBackend::list` trait method (same remote-first,
    /// local-fallback behavior for cold nodes), but also returns each
    /// entry's size in bytes — see `list_remote_with_sizes`.
    ///
    /// Local-fallback entries report size 0, which is correct for
    /// budgeting purposes even though it's not their real on-disk size:
    /// the fallback only fires when S3 has nothing to list, meaning these
    /// files are already present locally and `fetch_one` will skip
    /// downloading them entirely (see its own local-existence check) — so
    /// their actual *fetch* cost really is zero.
    ///
    /// Used by `ensure_segments_local` to project a hydration batch's total
    /// size before fetching anything, so the batch can be chunked to a byte
    /// budget instead of firing off every file at once regardless of size.
    pub fn list_with_sizes(&self, path: &str) -> Result<Vec<(String, u64)>, KoshaError> {
        // Prefer remote listing so cold nodes (empty local disk) can discover
        // segment files after a restart. Fall back to local on list failure.
        match self.list_remote_with_sizes(path) {
            Ok(entries) if !entries.is_empty() => Ok(entries),
            Ok(_) => local_list_with_zero_sizes(&self.local, path),
            Err(e) => {
                eprintln!("WARN: S3 list failed for '{path}': {e}; falling back to local");
                local_list_with_zero_sizes(&self.local, path)
            }
        }
    }
}

/// Bounded retries for one S3 GET, exponential backoff starting at 100ms.
///
/// Fixes: a throttled/transient GET during a hydration fan-out burst used
/// to WARN once and give up, leaving that segment permanently incomplete
/// until some *future* query happened to re-trigger hydration for it —
/// convergence to a fully-hydrated namespace was flaky, worse the busier
/// S3 was (i.e. worse exactly when hydration fan-out is largest). 4
/// attempts total, ~100+200+400ms of backoff between them — enough to ride
/// out a brief throttle without turning a genuinely-missing/permission-
/// denied object into a long hang.
const FETCH_MAX_ATTEMPTS: u32 = 4;

/// The backoff delay before each retry (not the initial attempt) — a plain
/// `Vec` rather than inlined into either retry loop so the schedule itself
/// (attempt count, doubling, starting value) has a direct, network-free
/// unit test.
fn retry_backoff_schedule() -> Vec<std::time::Duration> {
    let mut delay = std::time::Duration::from_millis(100);
    let mut schedule = Vec::new();
    for _ in 1..FETCH_MAX_ATTEMPTS {
        schedule.push(delay);
        delay *= 2;
    }
    schedule
}

/// Fetch an object (or one `Range` of it) into memory with bounded retry.
///
/// Whole-object hydration does **not** come through here — it streams to
/// disk via [`get_object_streamed_with_retry`] instead, so that nothing ever
/// holds a whole segment file in the heap. This path serves the ranged
/// page-materialize reads, whose spans are KBs and belong to exactly one
/// in-flight response.
///
/// The `range` is an HTTP `Range` header value
/// (e.g. `bytes=0-1023`) — `None` fetches the whole object. One function so
/// ranged page-materialize GETs share the whole-file path's retry/backoff
/// behavior instead of growing their own.
async fn get_object_with_retry_ranged(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    range: Option<&str>,
) -> Result<aws_sdk_s3::primitives::AggregatedBytes, KoshaError> {
    let mut backoffs = retry_backoff_schedule().into_iter();
    let mut last_err = None;
    for attempt in 1..=FETCH_MAX_ATTEMPTS {
        let result: Result<_, KoshaError> = async {
            let resp = client
                .get_object()
                .bucket(bucket)
                .key(key)
                .set_range(range.map(str::to_string))
                .send()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 get_object: {e}")))?;
            resp.body
                .collect()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 read body: {e}")))
        }
        .await;

        match result {
            Ok(bytes) => return Ok(bytes),
            Err(e) => {
                if let Some(delay) = backoffs.next() {
                    eprintln!(
                        "WARN: S3 get_object for {key} failed (attempt {attempt}/{FETCH_MAX_ATTEMPTS}): \
                         {e}; retrying in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                }
                last_err = Some(e);
            }
        }
    }
    // Loop only exits without an early return once every attempt failed —
    // last_err is always Some(_) at that point.
    Err(last_err.expect("at least one attempt runs, so an exhausted loop always has an error"))
}

/// Per-file timing for [`S3Storage::read_many`]: rank-orders where the
/// wall-clock of a hydration batch goes. The two phases used to be strictly
/// sequential (GET the whole object into memory, then write it); the body is
/// now streamed to disk, so they interleave. They are still reported
/// disjointly and still sum to the fetch's cost: `write_ms` is the time
/// spent inside local writes, `get_ms` the rest of the transfer (request,
/// bounded retry/backoff, waiting on the network). `bytes` is the
/// decompressed object length; `cached` marks a no-op local hit (every field
/// but `cached` is zero then). Aggregated by `hydrate_from_s3_budgeted` into
/// the greppable `hydrate_files timing:` line — the cold-read plan's "next
/// lever" instrument: is cold wall-clock GET-bound, write-bound, or
/// fan-out-bound?
#[derive(Debug, Clone, Copy, Default)]
pub struct FetchStats {
    pub get_ms: f64,
    pub write_ms: f64,
    pub bytes: u64,
    pub cached: bool,
}

/// Fetch one logical path for [`S3Storage::read_many`]: serve from local
/// disk if already present, otherwise GET from S3 (with bounded retry —
/// see [`get_object_streamed_with_retry`]) and persist it there.
///
/// Returns [`FetchStats`], not the bytes — every caller of `read_many` only
/// ever wants "is this file on disk now" plus rank-ordering timing, so a
/// cache hit here is a pure existence check (no read at all), and a cache
/// miss doesn't pay for an extra copy into an owned `Vec<u8>` nobody was
/// going to read either.
///
/// `budget`/`request` are the byte-level admission gate (see
/// [`crate::hydration_budget`]): the object's size is learned from the GET
/// response's `Content-Length` — no LIST round trip — then charged to the
/// request and reserved against the process-wide in-flight cap before its
/// body is streamed.
async fn fetch_one(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    local_root: &Path,
    path: &str,
    budget: Option<Arc<HydrationBudget>>,
    request: Arc<RequestHydration>,
) -> Result<FetchStats, KoshaError> {
    let local_path = local_root.join(path);
    // A zero-byte file is never a valid segment artifact — it's the residue
    // of an interrupted non-atomic write (pre-atomic-rename builds) and must
    // be refetched, not served as "cached": callers treat existence as
    // completeness, so serving it poisons every downstream read.
    match tokio::fs::metadata(&local_path).await {
        Ok(m) if m.len() > 0 => {
            return Ok(FetchStats {
                cached: true,
                ..Default::default()
            });
        }
        _ => {}
    }

    // A request already over its ceiling stops here rather than issuing yet
    // another GET — the point of the ceiling is to stop early, not to
    // discover at the end of the file list that it should have.
    if request.is_shed() {
        return Err(KoshaError::Overloaded(format!(
            "hydration of {path} skipped: request already over its byte ceiling"
        )));
    }

    let s3_key = join_s3_key(prefix, path);

    if let Some(parent) = local_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    // Write to a uniquely-named temp file and atomically rename into place:
    // readers must never observe a created-but-unwritten or partially
    // written file (concurrent searches read these paths the moment they
    // exist), and a fetch cancelled mid-write must leave nothing behind at
    // the final path. Unique suffix so two racing fetches of the same file
    // can't interleave writes into one temp file — both write complete
    // files, renames are atomic, either winner is valid.
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp_path = local_path.with_extension(format!(
        "tmp.{}.{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let t_transfer = std::time::Instant::now();
    let streamed =
        get_object_streamed_with_retry(client, bucket, &s3_key, &tmp_path, budget, &request).await;
    let transfer_ms = t_transfer.elapsed().as_secs_f64() * 1e3;
    let streamed = match streamed {
        Ok(s) => s,
        Err(e) => {
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(e);
        }
    };
    if let Err(e) = tokio::fs::rename(&tmp_path, &local_path).await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        request.refund(streamed.bytes);
        return Err(KoshaError::Io(e));
    }

    Ok(FetchStats {
        // Disjoint by construction: local write time is measured inside the
        // transfer window and subtracted back out, so the two columns of
        // `hydrate_files timing:` read exactly as they did before the body
        // was streamed.
        get_ms: (transfer_ms - streamed.write_ms).max(0.0),
        write_ms: streamed.write_ms,
        bytes: streamed.bytes,
        cached: false,
    })
}

/// What one streamed fetch cost: bytes landed on disk, and how much of the
/// transfer window went into local writes.
struct StreamedFetch {
    bytes: u64,
    write_ms: f64,
}

/// Stream a whole object to `tmp_path` with the same bounded retry as
/// [`get_object_with_retry_ranged`], charging its size against the hydration
/// budget once the response headers reveal it.
///
/// Retries cover the *whole* attempt (request + body). A body that fails
/// mid-stream leaves a partial temp file, which the next attempt truncates
/// via `File::create` — a half-written file must never be renamed into
/// place, where every existence check in the hydration path would read it as
/// complete.
async fn get_object_streamed_with_retry(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    tmp_path: &Path,
    budget: Option<Arc<HydrationBudget>>,
    request: &Arc<RequestHydration>,
) -> Result<StreamedFetch, KoshaError> {
    let mut backoffs = retry_backoff_schedule().into_iter();
    let mut last_err = None;
    for attempt in 1..=FETCH_MAX_ATTEMPTS {
        let result: Result<StreamedFetch, KoshaError> = async {
            let resp = client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 get_object: {e}")))?;

            // `Content-Length` is the size the scoring hydration path never
            // knew up front (it skips the per-segment LIST on purpose), and
            // it arrives with the response headers — before a single body
            // byte is buffered. This is the hook the byte budget hangs off.
            let projected = resp.content_length().unwrap_or(0).max(0) as u64;
            if !request.charge(projected) {
                return Err(KoshaError::Overloaded(format!(
                    "hydration of {key} would put this request over its byte ceiling"
                )));
            }
            let _permit = match budget.as_ref() {
                Some(budget) => {
                    let t_wait = std::time::Instant::now();
                    let permit = budget.acquire(projected);
                    request.record_wait(t_wait.elapsed());
                    match permit {
                        Some(permit) => Some(permit),
                        None => {
                            request.refund(projected);
                            request.mark_shed();
                            return Err(KoshaError::Overloaded(format!(
                                "hydration of {key} waited out the in-flight byte budget"
                            )));
                        }
                    }
                }
                None => None,
            };

            let streamed = stream_body_to_file(resp.body, tmp_path).await;
            match streamed {
                Ok(streamed) => {
                    // Charged from `Content-Length`; reconcile against what
                    // actually landed so the request's running total stays
                    // truthful even if a store under-reports.
                    if streamed.bytes < projected {
                        request.refund(projected - streamed.bytes);
                    } else if streamed.bytes > projected {
                        request.charge(streamed.bytes - projected);
                    }
                    Ok(streamed)
                }
                Err(e) => {
                    request.refund(projected);
                    Err(e)
                }
            }
        }
        .await;

        match result {
            Ok(streamed) => return Ok(streamed),
            Err(e) => {
                // A shed is a deliberate decision, not a transient failure —
                // retrying it would just burn the backoff schedule against a
                // ceiling that has not moved.
                if matches!(e, KoshaError::Overloaded(_)) {
                    return Err(e);
                }
                if let Some(delay) = backoffs.next() {
                    eprintln!(
                        "WARN: S3 get_object for {key} failed (attempt {attempt}/{FETCH_MAX_ATTEMPTS}): \
                         {e}; retrying in {delay:?}"
                    );
                    tokio::time::sleep(delay).await;
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.expect("at least one attempt runs, so an exhausted loop always has an error"))
}

/// Write a GET response body to `tmp_path` chunk by chunk, returning what it
/// cost.
///
/// The whole point is that the object is never resident in the process at
/// once. The previous `body.collect()` → `tokio::fs::write(tmp, raw)` held
/// every in-flight object entirely in the heap: at the fan-out this path
/// runs (`KOSHA_SCORING_HYDRATE_CONCURRENCY` = 64, up to
/// `KOSHA_MAX_CONCURRENT_HYDRATIONS` = 4 batches) a namespace of ~28 MB
/// `inverted.idx` files put gigabytes of transient buffers behind a single
/// cold search — the OOM this function exists to remove. Peak is now one
/// chunk per in-flight fetch, whatever the object's size.
async fn stream_body_to_file(
    mut body: aws_sdk_s3::primitives::ByteStream,
    tmp_path: &Path,
) -> Result<StreamedFetch, KoshaError> {
    use tokio::io::AsyncWriteExt;

    let t_create = std::time::Instant::now();
    let mut file = tokio::fs::File::create(tmp_path)
        .await
        .map_err(KoshaError::Io)?;
    let mut write_ms = t_create.elapsed().as_secs_f64() * 1e3;
    let mut bytes = 0u64;

    loop {
        match body.next().await {
            Some(Ok(chunk)) => {
                let t_write = std::time::Instant::now();
                file.write_all(&chunk).await.map_err(KoshaError::Io)?;
                write_ms += t_write.elapsed().as_secs_f64() * 1e3;
                bytes += chunk.len() as u64;
            }
            Some(Err(e)) => {
                return Err(KoshaError::NotFound(format!("S3 read body: {e}")));
            }
            None => break,
        }
    }

    // `tokio::fs::File` buffers, so the flush is what makes the bytes real
    // before the caller renames the temp file into place.
    let t_flush = std::time::Instant::now();
    file.flush().await.map_err(KoshaError::Io)?;
    write_ms += t_flush.elapsed().as_secs_f64() * 1e3;

    Ok(StreamedFetch { bytes, write_ms })
}

/// Fetch one byte range for [`S3Storage::read_ranges`]: from the local file
/// when the whole object is already on disk (files at their real path are
/// only ever renamed into place complete), otherwise as an S3 ranged GET.
#[allow(clippy::too_many_arguments)]
async fn fetch_range_one(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    local_root: &Path,
    path: &str,
    offset: u64,
    length: u64,
    budget: Option<Arc<HydrationBudget>>,
    request: &Arc<RequestHydration>,
) -> Result<Vec<u8>, KoshaError> {
    if length == 0 {
        return Ok(Vec::new());
    }

    let local_path = local_root.join(path);
    if let Ok(meta) = tokio::fs::metadata(&local_path).await {
        if meta.len() > offset {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            let mut file = tokio::fs::File::open(&local_path)
                .await
                .map_err(KoshaError::Io)?;
            file.seek(std::io::SeekFrom::Start(offset))
                .await
                .map_err(KoshaError::Io)?;
            let mut buf = vec![0u8; length.min(meta.len() - offset) as usize];
            file.read_exact(&mut buf).await.map_err(KoshaError::Io)?;
            return Ok(buf);
        }
    }

    if request.is_shed() || !request.charge(length) {
        return Err(KoshaError::Overloaded(format!(
            "ranged read of {path} would put this request over its byte ceiling"
        )));
    }
    let _permit = match budget {
        Some(budget) => {
            let t_wait = std::time::Instant::now();
            let permit = budget.acquire(length);
            request.record_wait(t_wait.elapsed());
            match permit {
                Some(permit) => Some(permit),
                None => {
                    request.refund(length);
                    request.mark_shed();
                    return Err(KoshaError::Overloaded(format!(
                        "ranged read of {path} waited out the in-flight byte budget"
                    )));
                }
            }
        }
        None => None,
    };

    let s3_key = join_s3_key(prefix, path);
    let range = format!("bytes={}-{}", offset, offset + length - 1);
    let bytes = match get_object_with_retry_ranged(client, bucket, &s3_key, Some(&range)).await {
        Ok(bytes) => bytes.into_bytes(),
        Err(e) => {
            request.refund(length);
            return Err(e);
        }
    };
    if bytes.is_empty() {
        return Err(KoshaError::NotFound(format!(
            "ranged GET {s3_key} [{range}] returned no bytes"
        )));
    }
    if (bytes.len() as u64) < length {
        request.refund(length - bytes.len() as u64);
    }
    Ok(bytes.to_vec())
}

/// Files of one segment uploaded at once (`KOSHA_UPLOAD_CONCURRENCY`,
/// default 16 — the same default hydration uses for downloads).
fn upload_concurrency() -> usize {
    std::env::var("KOSHA_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(16)
}

/// Upload bytes to one S3 key. Multi-part for payloads > 5MB.
async fn put_bytes(
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    data: Vec<u8>,
) -> Result<(), KoshaError> {
    let size = data.len();

    if size > 5 * 1024 * 1024 {
        let mpu = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| KoshaError::NotFound(format!("S3 create_multipart_upload: {e}")))?;

        let upload_id = mpu.upload_id().unwrap_or_default();
        let mut parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();
        let part_size = 5 * 1024 * 1024;
        let mut offset = 0;
        let mut part_number = 1;

        while offset < size {
            let end = std::cmp::min(offset + part_size, size);
            let part_data = &data[offset..end];

            let upload_part_res = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(aws_sdk_s3::primitives::ByteStream::from(Vec::from(
                    part_data,
                )))
                .send()
                .await
                .map_err(|e| KoshaError::NotFound(format!("S3 upload_part: {e}")))?;

            let etag = upload_part_res.e_tag().unwrap_or_default().to_string();
            parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .e_tag(etag)
                    .part_number(part_number)
                    .build(),
            );

            offset = end;
            part_number += 1;
        }

        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| KoshaError::NotFound(format!("S3 complete_multipart_upload: {e}")))?;
    } else {
        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data))
            .send()
            .await
            .map_err(|e| KoshaError::NotFound(format!("S3 put_object: {e}")))?;
    }
    Ok::<_, KoshaError>(())
}

impl StorageBackend for S3Storage {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError> {
        let local_path = Path::new(&self.local.root).join(path);
        if local_path.exists() {
            return std::fs::read(&local_path).map_err(KoshaError::Io);
        }
        let s3_key = self.s3_key(path);
        self.download_from_s3(&s3_key, &local_path)
    }

    fn write(&self, path: &str, data: &[u8]) -> Result<(), KoshaError> {
        self.local.write(path, data)?;
        let local_path = Path::new(&self.local.root).join(path);
        let s3_key = self.s3_key(path);
        self.upload_to_s3(&local_path, &s3_key)
    }

    fn exists(&self, path: &str) -> bool {
        self.local.exists(path)
    }

    fn delete(&self, path: &str) -> Result<(), KoshaError> {
        self.local.delete(path)?;
        let s3_key = self.s3_key(path);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = s3_key.clone();
        self.rt.block_on(async move {
            client
                .delete_object()
                .bucket(&bucket)
                .key(&key)
                .send()
                .await
                .ok();
        });
        Ok(())
    }

    fn list(&self, path: &str) -> Result<Vec<String>, KoshaError> {
        Ok(self
            .list_with_sizes(path)?
            .into_iter()
            .map(|(name, _size)| name)
            .collect())
    }

    fn create_dir_all(&self, path: &str) -> Result<(), KoshaError> {
        self.local.create_dir_all(path)
    }

    fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectMeta>, KoshaError> {
        self.list_objects_remote(prefix)
    }
}

/// The object store segment GC (#239) works against: S3 for listing and
/// deleting. A local copy of a deleted object is left to the node's local
/// GC, which knows whether a reader still has it open.
#[derive(Debug)]
pub struct S3GcStore<'a>(pub &'a S3Storage);

impl StorageBackend for S3GcStore<'_> {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError> {
        self.0.read_remote(path)
    }
    fn write(&self, _path: &str, _data: &[u8]) -> Result<(), KoshaError> {
        Err(KoshaError::NotFound("S3GcStore is read/delete only".into()))
    }
    fn exists(&self, path: &str) -> bool {
        self.0
            .list_objects_remote(path)
            .is_ok_and(|objects| objects.iter().any(|o| o.path == path))
    }
    fn delete(&self, path: &str) -> Result<(), KoshaError> {
        self.0.delete_remote(path)
    }
    fn list(&self, path: &str) -> Result<Vec<String>, KoshaError> {
        Ok(self
            .0
            .list_remote_with_sizes(path)?
            .into_iter()
            .map(|(name, _)| name)
            .collect())
    }
    fn create_dir_all(&self, _path: &str) -> Result<(), KoshaError> {
        Ok(())
    }
    fn list_objects(&self, prefix: &str) -> Result<Vec<ObjectMeta>, KoshaError> {
        self.0.list_objects_remote(prefix)
    }
}

fn local_list_with_zero_sizes(
    local: &LocalStorage,
    path: &str,
) -> Result<Vec<(String, u64)>, KoshaError> {
    Ok(local
        .list(path)?
        .into_iter()
        .map(|name| (name, 0))
        .collect())
}

/// Group `(<segment>/<file>, size)` listing rows into bytes per segment,
/// counting only files `wants` accepts (by basename). Rows with no segment
/// component (stray objects at the namespace root) are ignored. A segment
/// whose files are all filtered out still appears, with `0` bytes.
pub(crate) fn sum_segment_sizes(
    objects: Vec<(String, u64)>,
    wants: impl Fn(&str) -> bool,
) -> std::collections::HashMap<String, u64> {
    let mut sizes = std::collections::HashMap::new();
    for (relative, size) in objects {
        let Some((segment, rest)) = relative.split_once('/') else {
            continue;
        };
        if segment.is_empty() || rest.is_empty() {
            continue;
        }
        let file = rest.rsplit('/').next().unwrap_or(rest);
        let entry = sizes.entry(segment.to_string()).or_insert(0u64);
        if wants(file) {
            *entry = entry.saturating_add(size);
        }
    }
    sizes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sum_segment_sizes_groups_by_segment_and_filters_files() {
        let rows = vec![
            ("seg-a/doc_store.bin".to_string(), 100),
            ("seg-a/vector.idx".to_string(), 1_000),
            ("seg-a/_COMMIT".to_string(), 5),
            ("seg-b/inverted.idx".to_string(), 7),
            ("seg-c/vector.idx".to_string(), 9),
            ("stray.json".to_string(), 50),
            ("seg-d/".to_string(), 0),
        ];
        let sizes = sum_segment_sizes(rows.clone(), |_| true);
        assert_eq!(sizes["seg-a"], 1_105);
        assert_eq!(sizes["seg-b"], 7);
        assert_eq!(sizes.len(), 3, "stray root object and empty key ignored");
        let lexical = sum_segment_sizes(rows, |f| f != "vector.idx");
        assert_eq!(lexical["seg-a"], 105);
        assert_eq!(lexical["seg-c"], 0);
    }

    /// Regression for the BM25 benchmark re-run's bug #4
    /// (`scripts/bench/bm25_scale/RESULTS.md`): "hydration S3 GETs have no
    /// retry." A throttled/transient GET during a fan-out burst used to
    /// WARN once and leave the segment permanently incomplete. This checks
    /// the schedule both retry loops actually retry against
    /// (attempt count, doubling, starting value) without needing a live S3
    /// endpoint.
    #[test]
    fn retry_backoff_schedule_doubles_from_100ms_for_three_retries() {
        let schedule = retry_backoff_schedule();
        assert_eq!(
            schedule,
            vec![
                std::time::Duration::from_millis(100),
                std::time::Duration::from_millis(200),
                std::time::Duration::from_millis(400),
            ],
            "FETCH_MAX_ATTEMPTS=4 total attempts means 3 retries between them"
        );
    }

    /// The whole point of streaming: bytes land on disk without the object
    /// ever being resident in one buffer. Exercised through a real
    /// `ByteStream` (chunked, as an S3 body is) with no network involved.
    #[test]
    fn streams_body_to_file_without_buffering_the_whole_object() {
        let dir = std::env::temp_dir().join(format!("kosha-stream-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let tmp_path = dir.join("part.tmp");
        let payload: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let streamed = rt
            .block_on(stream_body_to_file(
                aws_sdk_s3::primitives::ByteStream::from(payload.clone()),
                &tmp_path,
            ))
            .expect("stream succeeds");

        assert_eq!(streamed.bytes, payload.len() as u64);
        assert_eq!(
            std::fs::read(&tmp_path).unwrap(),
            payload,
            "every chunk must land, in order"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A second attempt must not append to the first attempt's partial file:
    /// `File::create` truncates, so a retried fetch writes the object once,
    /// not twice. A doubled file would rename into place looking complete.
    #[test]
    fn a_retried_stream_truncates_the_previous_attempts_partial_file() {
        let dir = std::env::temp_dir().join(format!("kosha-retry-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let tmp_path = dir.join("part.tmp");
        std::fs::write(&tmp_path, vec![0xffu8; 4096]).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let payload = b"the real object".to_vec();
        let streamed = rt
            .block_on(stream_body_to_file(
                aws_sdk_s3::primitives::ByteStream::from(payload.clone()),
                &tmp_path,
            ))
            .expect("stream succeeds");

        assert_eq!(streamed.bytes, payload.len() as u64);
        assert_eq!(std::fs::read(&tmp_path).unwrap(), payload);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn listed_keys_map_back_to_logical_paths() {
        for prefix in ["", "kosha", "kosha/", "a/b/"] {
            let key = join_s3_key(prefix, "tenant/ns/seg-1/_COMMIT");
            assert_eq!(
                logical_path_of_key(prefix, &key).as_deref(),
                Some("tenant/ns/seg-1/_COMMIT"),
                "prefix {prefix:?}"
            );
        }
        assert_eq!(logical_path_of_key("kosha", "other/x"), None);
        assert_eq!(logical_path_of_key("kosha", "koshax/x"), None);
        assert_eq!(logical_path_of_key("kosha", "kosha/"), None);
    }

    #[test]
    fn s3_key_joins_prefix() {
        assert_eq!(join_s3_key("kosha/", "ns/seg/a.bin"), "kosha/ns/seg/a.bin");
        assert_eq!(join_s3_key("", "ns/seg/a.bin"), "ns/seg/a.bin");
    }

    #[test]
    fn s3_key_strips_leading_slash_from_path_and_trailing_from_prefix() {
        // read_many's fetch_one and S3Storage::s3_key must agree on this —
        // both delegate to join_s3_key so they can't drift apart.
        assert_eq!(join_s3_key("kosha", "/ns/seg/a.bin"), "kosha/ns/seg/a.bin");
        assert_eq!(join_s3_key("kosha/", "/ns/seg/a.bin"), "kosha/ns/seg/a.bin");
    }

    #[test]
    fn from_env_requires_bucket() {
        // Can't safely mutate process env in parallel tests; just exercise the
        // struct defaults path via a synthetic config.
        let cfg = S3Config {
            bucket: "b".into(),
            prefix: "p/".into(),
            endpoint: Some("http://localhost:9000".into()),
            force_path_style: true,
            access_key: Some("k".into()),
            secret_key: Some("s".into()),
            region: Some("us-east-1".into()),
        };
        assert_eq!(cfg.bucket, "b");
        assert!(cfg.force_path_style);
    }
}
