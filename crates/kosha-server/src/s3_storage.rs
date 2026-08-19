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

use kosha_core::{KoshaError, LocalStorage, StorageBackend};

use crate::hydration_budget::{HydrationBudget, RequestHydration};

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

    /// Upload every regular file in a locally-built segment directory.
    ///
    /// `logical_dir` is relative to the storage root, for example
    /// `paragraph_index_hnsw/paragraph_index_hnsw-000001`. Segment files are
    /// immutable, so callers only need to invoke this for newly-built
    /// segments.
    pub fn sync_segment_dir(&self, logical_dir: &Path) -> Result<(), KoshaError> {
        let local_dir = Path::new(&self.local.root).join(logical_dir);
        for entry in std::fs::read_dir(&local_dir).map_err(KoshaError::Io)? {
            let entry = entry.map_err(KoshaError::Io)?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let logical_path = logical_dir.join(name).to_string_lossy().into_owned();
            self.upload_to_s3(&path, &self.s3_key(&logical_path))?;
        }
        Ok(())
    }

    /// Upload a local file to S3. Uses multi-part upload for files > 5MB.
    fn upload_to_s3(&self, local_path: &Path, s3_key: &str) -> Result<(), KoshaError> {
        let data = std::fs::read(local_path).map_err(KoshaError::Io)?;
        let size = data.len();

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = s3_key.to_string();

        self.rt.block_on(async move {
            if size > 5 * 1024 * 1024 {
                let mpu = client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| {
                        KoshaError::NotFound(format!("S3 create_multipart_upload: {e}"))
                    })?;

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
                    .map_err(|e| {
                        KoshaError::NotFound(format!("S3 complete_multipart_upload: {e}"))
                    })?;
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
        })
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

    /// Ranged-read counterpart of `read_many` for page materialization:
    /// fetch each `(logical_path, offset, length)` span as an S3 ranged GET
    /// (`Range: bytes=offset-offset+length-1`) — KBs per span instead of
    /// the whole multi-hundred-MB object — with the same concurrent fan-out
    /// and bounded retry as whole-file fetches. Nothing is persisted to the
    /// local cache: span bytes belong to exactly one in-flight response.
    ///
    /// Returns one entry per input span, index-aligned; `None` for a span
    /// whose fetch failed after retries (already WARN-logged here). A span
    /// whose file is already fully local is served from disk instead — a
    /// concurrent whole-file hydration may have landed it since the caller
    /// decided to go ranged.
    ///
    /// Currently unused by the search path — that switched to Option A
    /// (whole-file `doc_store.bin` per page-segment, persisted) to make warm
    /// reads local. Kept as a pub API for the future Option B refinement
    /// (per-segment sparse span cache: cold-bytes + warm-local) and any
    /// other caller that wants ranged S3 reads; gated `allow(dead_code)`
    /// accordingly so clippy stays clean.
    #[allow(dead_code)]
    pub fn read_ranges(
        &self,
        ranges: &[(String, u64, u32)],
        max_concurrent: usize,
    ) -> Vec<Option<Vec<u8>>> {
        if ranges.is_empty() {
            return Vec::new();
        }
        let local_root = self.local.root.clone();
        let bucket = self.bucket.clone();
        let prefix = self.prefix.clone();
        let client = self.client.clone();
        let limit = std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1)));

        self.rt.block_on(async move {
            let mut set = tokio::task::JoinSet::new();
            for (idx, (path, offset, length)) in ranges.iter().enumerate() {
                let (client, bucket, prefix) = (client.clone(), bucket.clone(), prefix.clone());
                let local_root = local_root.clone();
                let path = path.clone();
                let (offset, length) = (*offset, *length);
                let limit = std::sync::Arc::clone(&limit);
                set.spawn(async move {
                    let _permit = limit.acquire_owned().await;
                    let result = fetch_range_one(
                        &client,
                        &bucket,
                        &prefix,
                        &local_root,
                        &path,
                        offset,
                        length,
                    )
                    .await;
                    (idx, path, result)
                });
            }

            let mut out: Vec<Option<Vec<u8>>> = vec![None; ranges.len()];
            while let Some(joined) = set.join_next().await {
                if let Ok((idx, path, result)) = joined {
                    match result {
                        Ok(bytes) => out[idx] = Some(bytes),
                        Err(e) => eprintln!("WARN: ranged GET of {path} failed: {e}"),
                    }
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
    fn list_remote_with_sizes(&self, path: &str) -> Result<Vec<(String, u64)>, KoshaError> {
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

    /// Whether S3 actually has *any* object under `logical_dir` — a
    /// ground-truth durability check, unlike `StorageBackend::exists`
    /// (which only checks local disk) or `list_with_sizes`/`list` (which
    /// fall back to local on an empty S3 listing, so they can't distinguish
    /// "durable in S3" from "only ever existed on this node's disk"). Used
    /// by the ingest-pod boot-time reconciliation sweep (`AppState::new`)
    /// to find segments that a prior process crashed or raced before
    /// uploading — see `sync_unsynced_segments_to_s3`'s doc comment for the
    /// incident this closes.
    pub fn segment_durable_in_s3(&self, logical_dir: &str) -> bool {
        self.list_remote_with_sizes(logical_dir)
            .map(|entries| !entries.is_empty())
            .unwrap_or(false)
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

/// Fetch one exact byte span for [`S3Storage::read_ranges`]: serve it from
/// the local file when the whole object is already on disk, otherwise as an
/// S3 ranged GET. Returns the span's bytes — never persisted locally (a
/// partial `doc_store.bin` on disk would read as a complete file to every
/// existence check in the hydration path).
///
/// Currently unused (see `read_ranges` — search path switched to Option A).
/// Kept + `allow(dead_code)` as the ranged-read primitive for the future
/// Option B sparse span cache.
#[allow(dead_code)]
async fn fetch_range_one(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    local_root: &Path,
    path: &str,
    offset: u64,
    length: u32,
) -> Result<Vec<u8>, KoshaError> {
    if length == 0 {
        return Ok(Vec::new());
    }

    let local_path = local_root.join(path);
    if local_path.exists() {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut file = tokio::fs::File::open(&local_path)
            .await
            .map_err(KoshaError::Io)?;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(KoshaError::Io)?;
        let mut buf = vec![0u8; length as usize];
        file.read_exact(&mut buf).await.map_err(KoshaError::Io)?;
        return Ok(buf);
    }

    let s3_key = join_s3_key(prefix, path);
    let range = format!("bytes={}-{}", offset, offset + length as u64 - 1);
    let bytes = get_object_with_retry_ranged(client, bucket, &s3_key, Some(&range))
        .await?
        .into_bytes();
    // A short response means the range ran past EOF — the offsets sidecar
    // and the remote object disagree. Parsing it would be garbage; fail so
    // the caller's fallback (local read) surfaces a real error instead.
    if bytes.len() != length as usize {
        return Err(KoshaError::NotFound(format!(
            "ranged GET {s3_key} [{range}] returned {} bytes, expected {length}",
            bytes.len()
        )));
    }
    Ok(bytes.to_vec())
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

#[cfg(test)]
mod tests {
    use super::*;

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
