//! `kosha-server migrate-namespace` (#344): move a namespace written before
//! tenant scoping (#262) from its bare id to `<tenant>/<name>`.
//!
//! Since #262 every client namespace resolves to `<tenant>/<name>`, and the
//! segment storage prefix embeds that id. Data written before #262 sits
//! under bare ids (`paragraph_index_hnsw`), which no tenant can address.
//! This offline subcommand (a Kubernetes Job, or by hand) moves one such
//! namespace:
//!
//! 1. take the writer lease (#254) on both ids, so no writer publishes to
//!    either while it runs, and refuse when a live writer holds one;
//! 2. refuse when the WAL holds records for the bare id past the manifest's
//!    checkpoint (they would replay under the bare id after the move);
//! 3. copy every object of the namespace's segments from `{from}/...` to
//!    `{to}/...` (server-side `CopyObject` on S3; each segment's `_COMMIT`
//!    marker last), skipping objects already copied at the same size;
//! 4. verify the destination holds every object at its size;
//! 5. mark the bare id discarded in every checked WAL stream through its
//!    highest LSN (the `discarded` meta entry a namespace DELETE writes),
//!    so a writer's recovery never replays its already-flushed records and
//!    republishes a stale bare namespace once its manifest is gone
//!    (`--skip-wal-check` cannot do this: that WAL is not reachable);
//! 6. re-key the control plane in one transaction
//!    ([`kosha_control::NamespaceRename`]), which fails when `to` exists or
//!    the manifest moved since step 2;
//! 7. reopen the manifest under `to` and check its segments and document
//!    count match;
//! 8. delete the copied source objects (skipped with `--keep-source`).
//!
//! Every step is safe to rerun: a run that died before step 6 starts over
//! and skips what is already copied; a run that died after it finds `to`
//! in the control plane and `from` gone, and only deletes source objects.
//! After the rename `to` is authoritative (a writer may already have
//! rewritten objects in place, such as a segment's `footer.json`), so a
//! resumed run never copies: it deletes the source objects that exist at
//! the destination and leaves any other in place.
//!
//! Only objects under the bare prefix that belong to segments the manifest
//! references are copied and deleted. The prefix `{from}/` can also hold
//! another tenant's namespaces (a tenant whose id equals the bare name), so
//! nothing here ever deletes the prefix wholesale.
//!
//! A rollout moves every bare namespace at once with the batch modes:
//! `--map <file>` (one `bare=tenant/name` per line, or JSON) or
//! `--all-unreachable --tenant <t>` (every bare id `b` to `<t>/<b>`). A batch
//! runs the steps above namespace by namespace, keeps going past one it
//! refuses, prints a summary table and exits non-zero if it refused any.
//! `--verify` re-checks a finished migration read-only: each destination
//! exists with its segments' objects, the bare id is gone, and no bare id
//! is left unreachable.
//!
//! Segment ids are opaque directory names and keep their names. The
//! manifest names namespaces in three places (branch parent, branch segment
//! homes, deletion-vector homes); the rename rewrites those that name
//! `from`. A namespace whose segments are referenced from another
//! namespace's manifest (a branch child) or a sharded root is refused.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kosha_control::NamespaceRename;
use kosha_core::{ControlStore, KoshaError, Manifest, NamespaceId};
use kosha_write::wal::WalStore;

const USAGE: &str = "usage: kosha-server migrate-namespace --from <bare> --to <tenant>/<name> \
[--dry-run] [--keep-source] [--skip-wal-check]
       kosha-server migrate-namespace (--map <file> | --all-unreachable --tenant <tenant>) \
[--dry-run] [--keep-source] [--skip-wal-check]
       kosha-server migrate-namespace --verify (--from <bare> --to <tenant>/<name> | --map <file> \
| --all-unreachable)
       kosha-server migrate-namespace --list-unreachable
A map file holds one `bare=tenant/name` per line (`#` comments), or JSON: \
{\"bare\": \"tenant/name\", ...} or [{\"from\": \"bare\", \"to\": \"tenant/name\"}, ...].";

/// Writer-lease TTL the migration holds; renewed while copying.
const LEASE_TTL: Duration = Duration::from_secs(300);
const LEASE_RENEW_EVERY: Duration = Duration::from_secs(60);

/// Parsed command line.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Args {
    pub from: Option<NamespaceId>,
    pub to: Option<NamespaceId>,
    pub dry_run: bool,
    pub keep_source: bool,
    pub list_unreachable: bool,
    /// Skip the WAL-tail check. Only for a writer that was flushed and
    /// stopped when its WAL is not reachable from here (a local WAL on the
    /// ingest pod's disk). The run then cannot mark the bare id discarded
    /// in that WAL either: any group file still holding a record of the
    /// bare id replays it on the writer's next boot (republishing a stale
    /// bare namespace), so that WAL must hold no such file by then. Prefer
    /// mounting the WAL and running without this flag.
    pub skip_wal_check: bool,
    /// Batch: move every `bare=tenant/name` pair in this file.
    pub map: Option<PathBuf>,
    /// Batch: move every bare namespace `b` to `<tenant>/<b>`.
    pub all_unreachable: bool,
    pub tenant: Option<String>,
    /// Re-check a finished migration instead of running one.
    pub verify: bool,
}

impl Args {
    pub(crate) fn is_batch(&self) -> bool {
        self.map.is_some() || self.all_unreachable
    }
}

/// One namespace to move: `(bare id, <tenant>/<name>)`.
pub(crate) type Pair = (NamespaceId, NamespaceId);

/// A namespace id without a tenant: written before #262, unreachable
/// through `/v1/namespaces/...`.
pub(crate) fn is_bare(id: &NamespaceId) -> bool {
    !id.0.is_empty() && !id.0.contains('/')
}

fn valid_component(part: &str) -> bool {
    !part.is_empty()
        && part != "."
        && part != ".."
        && !part.starts_with('_')
        && !part.chars().any(|c| c.is_control() || c == '\\')
}

/// `from` is a bare id and `to` is `<tenant>/<name>`.
fn validate_pair(from: &NamespaceId, to: &NamespaceId) -> Result<(), String> {
    if !is_bare(from) || !valid_component(&from.0) {
        return Err(format!(
            "--from {:?} must be a bare (pre-#262) namespace id, without a tenant",
            from.0
        ));
    }
    match to.0.split_once('/') {
        Some((tenant, name))
            if valid_component(tenant) && valid_component(name) && !name.contains('/') =>
        {
            Ok(())
        }
        _ => Err(format!(
            "--to {:?} must be <tenant>/<name>, one tenant and one name",
            to.0
        )),
    }
}

/// Parse `migrate-namespace`'s arguments (after the subcommand).
pub(crate) fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut parsed = Args::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) if flag.starts_with("--") => (flag.to_owned(), Some(value.into())),
            _ => (arg.clone(), None),
        };
        let mut value = |name: &str| -> Result<String, String> {
            inline
                .clone()
                .or_else(|| args.next())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("{name} needs a value\n{USAGE}"))
        };
        match flag.as_str() {
            "--from" => parsed.from = Some(NamespaceId(value("--from")?)),
            "--to" => parsed.to = Some(NamespaceId(value("--to")?)),
            "--map" => parsed.map = Some(PathBuf::from(value("--map")?)),
            "--tenant" => parsed.tenant = Some(value("--tenant")?),
            "--all-unreachable" => parsed.all_unreachable = true,
            "--verify" => parsed.verify = true,
            "--dry-run" => parsed.dry_run = true,
            "--keep-source" => parsed.keep_source = true,
            "--list-unreachable" => parsed.list_unreachable = true,
            "--skip-wal-check" => parsed.skip_wal_check = true,
            "-h" | "--help" => return Err(USAGE.into()),
            other => return Err(format!("unknown argument {other:?}\n{USAGE}")),
        }
    }
    if parsed.list_unreachable {
        return Ok(parsed);
    }
    let single = parsed.from.is_some() || parsed.to.is_some();
    let modes = [single, parsed.map.is_some(), parsed.all_unreachable]
        .iter()
        .filter(|m| **m)
        .count();
    if modes > 1 {
        return Err(format!(
            "use one of --from/--to, --map or --all-unreachable\n{USAGE}"
        ));
    }
    if parsed.tenant.is_some() && !parsed.all_unreachable {
        return Err(format!("--tenant goes with --all-unreachable\n{USAGE}"));
    }
    if parsed.verify && (parsed.dry_run || parsed.keep_source || parsed.skip_wal_check) {
        return Err(format!(
            "--verify only reads; it takes no --dry-run, --keep-source or --skip-wal-check\n{USAGE}"
        ));
    }
    if parsed.all_unreachable {
        match &parsed.tenant {
            Some(tenant) if valid_component(tenant) && !tenant.contains('/') => {}
            Some(tenant) => return Err(format!("--tenant {tenant:?} is not a valid tenant id")),
            // Verifying only checks that no bare id is left.
            None if parsed.verify => {}
            None => return Err(format!("--all-unreachable needs --tenant\n{USAGE}")),
        }
        return Ok(parsed);
    }
    if parsed.map.is_some() {
        return Ok(parsed);
    }
    let (Some(from), Some(to)) = (&parsed.from, &parsed.to) else {
        return Err(format!(
            "--from and --to (or --map, or --all-unreachable) are required\n{USAGE}"
        ));
    };
    validate_pair(from, to)?;
    Ok(parsed)
}

/// Parse a `--map` file: one `bare=tenant/name` per line (blank lines and
/// `#` comments skipped), or JSON, either `{"bare": "tenant/name", ...}`
/// or `[{"from": "bare", "to": "tenant/name"}, ...]`. Every pair is
/// validated, and no bare id or destination may appear twice.
pub(crate) fn parse_map(text: &str) -> Result<Vec<Pair>, String> {
    let trimmed = text.trim_start();
    let raw: Vec<(String, String, String)> = if trimmed.starts_with('{') {
        let object: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(text).map_err(|e| format!("map file: {e}"))?;
        object
            .into_iter()
            .map(|(from, to)| match to {
                serde_json::Value::String(to) => Ok((format!("key {from:?}"), from, to)),
                other => Err(format!("map file: {from:?} maps to {other}, not a string")),
            })
            .collect::<Result<_, _>>()?
    } else if trimmed.starts_with('[') {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Entry {
            from: String,
            to: String,
        }
        let entries: Vec<Entry> =
            serde_json::from_str(text).map_err(|e| format!("map file: {e}"))?;
        entries
            .into_iter()
            .enumerate()
            .map(|(i, e)| (format!("entry {}", i + 1), e.from, e.to))
            .collect()
    } else {
        let mut raw = Vec::new();
        for (number, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (from, to) = line.split_once('=').ok_or_else(|| {
                format!(
                    "map file line {}: {line:?} is not bare=tenant/name",
                    number + 1
                )
            })?;
            raw.push((
                format!("line {}", number + 1),
                from.trim().to_owned(),
                to.trim().to_owned(),
            ));
        }
        raw
    };
    let mut pairs = Vec::new();
    let (mut sources, mut targets) = (BTreeSet::new(), BTreeSet::new());
    for (at, from, to) in raw {
        let (from, to) = (NamespaceId(from), NamespaceId(to));
        validate_pair(&from, &to).map_err(|e| format!("map file {at}: {e}"))?;
        if !sources.insert(from.0.clone()) {
            return Err(format!("map file {at}: {:?} is mapped twice", from.0));
        }
        if !targets.insert(to.0.clone()) {
            return Err(format!("map file {at}: two namespaces map to {:?}", to.0));
        }
        pairs.push((from, to));
    }
    if pairs.is_empty() {
        return Err("map file holds no bare=tenant/name pairs".into());
    }
    Ok(pairs)
}

fn load_map(path: &Path) -> Result<Vec<Pair>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    parse_map(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// Bare namespace ids no tenant can reach, sorted.
pub(crate) fn unreachable_bare_namespaces(ids: &[NamespaceId]) -> Vec<NamespaceId> {
    let mut bare: Vec<_> = ids.iter().filter(|id| is_bare(id)).cloned().collect();
    bare.sort_by(|a, b| a.0.cmp(&b.0));
    bare
}

/// Boot-time warning (#344): list the bare namespaces this control plane
/// holds, which `/v1` clients cannot address.
pub(crate) fn warn_unreachable_bare_namespaces(ids: &[NamespaceId]) {
    let bare = unreachable_bare_namespaces(ids);
    if bare.is_empty() {
        return;
    }
    let names: Vec<&str> = bare.iter().map(|id| id.0.as_str()).collect();
    eprintln!(
        "WARN: {} namespace(s) predate tenant scoping (#262) and are unreachable through \
         /v1/namespaces: {}. Move them with `kosha-server migrate-namespace --from <bare> \
         --to <tenant>/<name>`, or all at once with `--map <file>` (k8s/README.md, \"Migrating pre-#262 namespaces\").",
        bare.len(),
        names.join(", ")
    );
}

// ─── Segment objects ─────────────────────────────────────────────────────────

/// The segment object store the migration copies within: S3, or the local
/// data directory when S3 is off. Paths are logical (`{ns}/{segment}/{file}`).
pub(crate) trait SegmentObjects {
    fn describe(&self) -> String;
    /// Every object under `{ns}/`, as `(path relative to {ns}/, size)`.
    fn list(&self, ns: &str) -> Result<Vec<(String, u64)>, String>;
    /// Copy one object (`size` bytes) from `from` to `to`; replaces `to`.
    fn copy(&self, from: &str, to: &str, size: u64) -> Result<(), String>;
    /// Delete objects; missing ones are not an error. Returns how many.
    fn delete(&self, paths: &[String]) -> Result<usize, String>;
}

/// Segments in a local data directory (`KOSHA_DATA_DIR`, S3 off).
pub(crate) struct LocalObjects {
    root: PathBuf,
}

impl LocalObjects {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, u64)>) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let rel = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        let kind = entry.file_type()?;
        if kind.is_dir() {
            walk(&entry.path(), &rel, out)?;
        } else if kind.is_file() {
            out.push((rel, entry.metadata()?.len()));
        }
    }
    Ok(())
}

impl SegmentObjects for LocalObjects {
    fn describe(&self) -> String {
        format!("local data dir {}", self.root.display())
    }

    fn list(&self, ns: &str) -> Result<Vec<(String, u64)>, String> {
        let mut out = Vec::new();
        walk(&self.root.join(ns), "", &mut out)
            .map_err(|e| format!("listing {}: {e}", self.root.join(ns).display()))?;
        Ok(out)
    }

    fn copy(&self, from: &str, to: &str, _size: u64) -> Result<(), String> {
        let source = self.root.join(from);
        let target = self.root.join(to);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
        }
        // Copy beside the target and rename, so a crash never leaves a
        // short file the rerun would mistake for a copy.
        let partial = target.with_extension("migrate-partial");
        std::fs::copy(&source, &partial)
            .and_then(|_| std::fs::rename(&partial, &target))
            .map_err(|e| format!("copy {} -> {}: {e}", source.display(), target.display()))
    }

    fn delete(&self, paths: &[String]) -> Result<usize, String> {
        let mut deleted = 0;
        let mut parents = BTreeSet::new();
        for path in paths {
            let file = self.root.join(path);
            match std::fs::remove_file(&file) {
                Ok(()) => deleted += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(format!("delete {}: {e}", file.display())),
            }
            let mut dir = file.parent().map(Path::to_path_buf);
            while let Some(d) = dir {
                if d == self.root || !d.starts_with(&self.root) {
                    break;
                }
                dir = d.parent().map(Path::to_path_buf);
                parents.insert(d);
            }
        }
        // Deepest first; a directory something else still uses stays.
        for dir in parents.iter().rev() {
            let _ = std::fs::remove_dir(dir);
        }
        Ok(deleted)
    }
}

/// `CopySource` header value: `{bucket}/{key}`, percent-encoded except for
/// unreserved characters and `/`.
#[cfg_attr(not(feature = "s3"), allow(dead_code))]
fn copy_source(bucket: &str, key: &str) -> String {
    let mut out = String::with_capacity(bucket.len() + key.len() + 1);
    for byte in format!("{bucket}/{key}").bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Byte ranges for an `UploadPartCopy` of `size` bytes in `part`-byte parts.
#[cfg_attr(not(feature = "s3"), allow(dead_code))]
fn part_ranges(size: u64, part: u64) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < size {
        let end = (start + part).min(size) - 1;
        ranges.push((start, end));
        start = end + 1;
    }
    ranges
}

#[cfg(feature = "s3")]
mod s3_objects {
    use super::*;
    use crate::s3_storage::{ApplySse, ObjectSse, S3Storage};

    /// `CopyObject` handles objects up to 5 GiB; larger ones are copied in
    /// parts with `UploadPartCopy`.
    const SINGLE_COPY_MAX: u64 = 5 * 1024 * 1024 * 1024;
    const COPY_PART_BYTES: u64 = 1024 * 1024 * 1024;

    /// Segments in the S3 bucket. Copies are server-side: no object bytes
    /// pass through this process.
    pub(crate) struct S3Objects<'a> {
        s3: &'a S3Storage,
        rt: tokio::runtime::Runtime,
    }

    impl<'a> S3Objects<'a> {
        pub(crate) fn new(s3: &'a S3Storage) -> Result<Self, String> {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| format!("tokio runtime: {e}"))?;
            Ok(Self { s3, rt })
        }

        async fn multipart_copy(
            client: &aws_sdk_s3::Client,
            bucket: &str,
            key: &str,
            source: &str,
            size: u64,
            sse: Option<&ObjectSse>,
        ) -> Result<(), String> {
            use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
            // Parts copied with UploadPartCopy inherit the upload's SSE.
            let upload = client
                .create_multipart_upload()
                .bucket(bucket)
                .key(key)
                .apply_sse(sse)
                .send()
                .await
                .map_err(|e| {
                    format!(
                        "create_multipart_upload {key}: {}",
                        aws_sdk_s3::error::DisplayErrorContext(&e)
                    )
                })?;
            let upload_id = upload
                .upload_id()
                .ok_or_else(|| format!("create_multipart_upload {key}: no upload id"))?
                .to_owned();
            let mut parts = Vec::new();
            let mut failure = None;
            for (index, (start, end)) in part_ranges(size, COPY_PART_BYTES).into_iter().enumerate()
            {
                let number = index as i32 + 1;
                match client
                    .upload_part_copy()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(number)
                    .copy_source(source)
                    .copy_source_range(format!("bytes={start}-{end}"))
                    .send()
                    .await
                {
                    Ok(out) => parts.push(
                        CompletedPart::builder()
                            .set_e_tag(
                                out.copy_part_result()
                                    .and_then(|r| r.e_tag())
                                    .map(Into::into),
                            )
                            .part_number(number)
                            .build(),
                    ),
                    Err(e) => {
                        failure = Some(format!(
                            "upload_part_copy {key} part {number}: {}",
                            aws_sdk_s3::error::DisplayErrorContext(&e)
                        ));
                        break;
                    }
                }
            }
            if failure.is_none() {
                let done = client
                    .complete_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(parts))
                            .build(),
                    )
                    .send()
                    .await;
                match done {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        failure = Some(format!(
                            "complete_multipart_upload {key}: {}",
                            aws_sdk_s3::error::DisplayErrorContext(&e)
                        ))
                    }
                }
            }
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            Err(failure.unwrap_or_default())
        }
    }

    impl SegmentObjects for S3Objects<'_> {
        fn describe(&self) -> String {
            format!(
                "s3://{}/{}",
                self.s3.bucket(),
                self.s3.prefix().trim_end_matches('/')
            )
        }

        fn list(&self, ns: &str) -> Result<Vec<(String, u64)>, String> {
            self.s3
                .list_namespace_objects(ns)
                .map_err(|e| format!("listing {ns}/: {e}"))
        }

        fn copy(&self, from: &str, to: &str, size: u64) -> Result<(), String> {
            let client = self.s3.client().clone();
            let bucket = self.s3.bucket().to_owned();
            let source = copy_source(&bucket, &self.s3.s3_key(from));
            let key = self.s3.s3_key(to);
            // A copy is a write (#343): without SSE headers CopyObject falls
            // back to the bucket default rather than keeping the source's
            // encryption. Resolve from the destination path, so a namespace
            // moved into `<tenant>/...` lands under that tenant's KMS key.
            let sse = self.s3.sse().for_path(to);
            self.rt.block_on(async move {
                if size > SINGLE_COPY_MAX {
                    return Self::multipart_copy(
                        &client,
                        &bucket,
                        &key,
                        &source,
                        size,
                        sse.as_ref(),
                    )
                    .await;
                }
                client
                    .copy_object()
                    .bucket(&bucket)
                    .key(&key)
                    .copy_source(&source)
                    .apply_sse(sse.as_ref())
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        format!(
                            "copy_object {source} -> {key}: {}",
                            aws_sdk_s3::error::DisplayErrorContext(&e)
                        )
                    })
            })
        }

        fn delete(&self, paths: &[String]) -> Result<usize, String> {
            use aws_sdk_s3::types::{Delete, ObjectIdentifier};
            let client = self.s3.client().clone();
            let bucket = self.s3.bucket().to_owned();
            let keys: Vec<String> = paths.iter().map(|p| self.s3.s3_key(p)).collect();
            self.rt.block_on(async move {
                let mut deleted = 0;
                for batch in keys.chunks(1000) {
                    let objects = batch
                        .iter()
                        .map(|key| ObjectIdentifier::builder().key(key).build())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| format!("delete batch: {e}"))?;
                    let delete = Delete::builder()
                        .set_objects(Some(objects))
                        .quiet(true)
                        .build()
                        .map_err(|e| format!("delete batch: {e}"))?;
                    let out = client
                        .delete_objects()
                        .bucket(&bucket)
                        .delete(delete)
                        .send()
                        .await
                        .map_err(|e| {
                            format!(
                                "delete_objects: {}",
                                aws_sdk_s3::error::DisplayErrorContext(&e)
                            )
                        })?;
                    if let Some(err) = out.errors().first() {
                        return Err(format!(
                            "delete_objects: {} of {} failed, first {}: {}",
                            out.errors().len(),
                            batch.len(),
                            err.key().unwrap_or("?"),
                            err.message().unwrap_or("unknown error")
                        ));
                    }
                    deleted += batch.len();
                }
                Ok(deleted)
            })
        }
    }
}

// ─── WAL tail ────────────────────────────────────────────────────────────────

/// The WAL meta entry recovery reads to skip a deleted namespace's records
/// (`DISCARDED_META` in `kosha_write::wal`): `{"namespace": through_lsn}`.
const DISCARDED_META: &str = "discarded";

fn invalid_data(message: String) -> KoshaError {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}

fn read_discarded(store: &dyn WalStore) -> Result<BTreeMap<String, u64>, KoshaError> {
    match store.read_meta(DISCARDED_META)? {
        None => Ok(BTreeMap::new()),
        Some(bytes) => serde_json::from_slice(&bytes)
            .map_err(|_| invalid_data("invalid WAL discard list".into())),
    }
}

/// What one WAL stream holds for a namespace.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WalScan {
    /// Records the manifest does not cover (see [`wal_scan`]).
    pub tail: usize,
    /// Highest LSN of any of the namespace's records, 0 when it has none.
    pub high: u64,
}

/// Scan `store` for `ns`'s records. The tail counts them the way recovery
/// replays them (`Indexer::restore_manifest`): when the manifest's
/// `wal_origin` names this stream (or no stream), records past
/// `wal_checkpoint`; otherwise every record. Records a namespace deletion
/// discarded are skipped, as recovery skips them.
pub(crate) fn wal_scan(
    store: &dyn WalStore,
    ns: &NamespaceId,
    manifest: &Manifest,
) -> Result<WalScan, KoshaError> {
    let origin = store
        .read_meta("origin")?
        .and_then(|bytes| String::from_utf8(bytes).ok());
    let checkpoint_applies = manifest
        .wal_origin
        .as_deref()
        .is_none_or(|o| origin.as_deref() == Some(o));
    let discarded_through = read_discarded(store)?.get(&ns.0).copied().unwrap_or(0);
    let mut scan = WalScan::default();
    for file in store.list()? {
        let bytes = store.read(file)?;
        let (records, _torn) = kosha_write::wal::decode_wal_file(&bytes, file)?;
        for (lsn, record) in &records {
            if record.namespace != *ns {
                continue;
            }
            scan.high = scan.high.max(*lsn);
            if *lsn > discarded_through && (!checkpoint_applies || *lsn > manifest.wal_checkpoint) {
                scan.tail += 1;
            }
        }
    }
    Ok(scan)
}

/// Records for `ns` in `store` that its manifest does not cover.
#[cfg(test)]
pub(crate) fn wal_tail_records(
    store: &dyn WalStore,
    ns: &NamespaceId,
    manifest: &Manifest,
) -> Result<usize, KoshaError> {
    wal_scan(store, ns, manifest).map(|scan| scan.tail)
}

/// Durably mark `ns` discarded through `through` in `store`, as a
/// namespace DELETE does (`WalWriter::discard_namespace`), so recovery
/// skips its records at or below that LSN. Once the namespace's manifest
/// is gone, nothing else stops recovery from replaying records that
/// manifest had already checkpointed. Returns whether the entry changed.
pub(crate) fn discard_wal_records(
    store: &dyn WalStore,
    ns: &NamespaceId,
    through: u64,
) -> Result<bool, KoshaError> {
    if through == 0 {
        return Ok(false);
    }
    let mut discarded = read_discarded(store)?;
    let entry = discarded.entry(ns.0.clone()).or_insert(0);
    if *entry >= through {
        return Ok(false);
    }
    *entry = through;
    let bytes = serde_json::to_vec(&discarded).map_err(|e| invalid_data(e.to_string()))?;
    store.write_meta(DISCARDED_META, &bytes)?;
    Ok(true)
}

// ─── The migration ───────────────────────────────────────────────────────────

/// What a run did (or, with `--dry-run`, would do).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Report {
    /// Objects of the namespace's segments under the bare prefix.
    pub objects: usize,
    pub bytes: u64,
    /// Objects copied by this run (the rest were already in place).
    pub copied: usize,
    /// Source objects deleted.
    pub deleted: usize,
    /// Objects under the bare prefix that no referenced segment owns; left
    /// in place.
    pub left_behind: usize,
    /// Documents in the manifest (the `/stats` count).
    pub documents: u64,
    /// The control plane was already re-keyed when this run started.
    pub resumed: bool,
}

/// Segment directories under `ns`'s own prefix that `manifest` references:
/// listed segments living there, retired ones not yet collected, and those
/// with deletion bitmaps or rewritten footers.
fn owned_segment_dirs(manifest: &Manifest, ns: &NamespaceId) -> BTreeSet<String> {
    let mut dirs = BTreeSet::new();
    for entry in &manifest.segments {
        if !manifest.is_inherited_segment(ns, &entry.segment_id) {
            dirs.insert(entry.segment_id.0.clone());
        }
    }
    for retired in &manifest.retired {
        dirs.insert(retired.segment_id.0.clone());
    }
    for (segment, vector) in &manifest.deletion_vectors {
        if !vector.is_inherited(ns) {
            dirs.insert(segment.0.clone());
        }
    }
    for segment in manifest.footer_crc32.keys() {
        if !manifest.is_inherited_segment(ns, segment) {
            dirs.insert(segment.0.clone());
        }
    }
    dirs
}

fn documents(manifest: &Manifest) -> u64 {
    manifest.segments.iter().map(|s| s.doc_count as u64).sum()
}

/// Other namespaces whose manifests read objects under `ns`'s prefix.
fn storage_dependents(
    ctrl: &dyn ControlStore,
    ns: &NamespaceId,
) -> Result<Vec<NamespaceId>, String> {
    let mut dependents = Vec::new();
    for other in ctrl.list_namespaces() {
        if &other == ns {
            continue;
        }
        let Some(manifest) = ctrl
            .try_manifest_cloned(&other)
            .map_err(|e| format!("reading {}'s manifest: {e}", other.0))?
        else {
            continue;
        };
        let homes = manifest
            .branch
            .iter()
            .flat_map(|branch| branch.segment_homes.values());
        let vectors = manifest
            .deletion_vectors
            .values()
            .filter_map(|v| v.namespace.as_ref());
        if homes.chain(vectors).any(|home| home == ns) {
            dependents.push(other);
        }
    }
    Ok(dependents)
}

/// One object to move.
struct Planned {
    rel: String,
    size: u64,
    in_place: bool,
}

fn plan_objects(
    objects: &dyn SegmentObjects,
    from: &NamespaceId,
    to: &NamespaceId,
    owned: &BTreeSet<String>,
) -> Result<(Vec<Planned>, usize), String> {
    let source = objects.list(&from.0)?;
    let target: HashMap<String, u64> = objects.list(&to.0)?.into_iter().collect();
    let mut planned = Vec::new();
    let mut left_behind = 0;
    for (rel, size) in source {
        let segment = rel.split('/').next().unwrap_or_default();
        if rel.contains('/') && owned.contains(segment) {
            let in_place = target.get(&rel) == Some(&size);
            planned.push(Planned {
                rel,
                size,
                in_place,
            });
        } else {
            left_behind += 1;
        }
    }
    // Data files first, every `_COMMIT` marker after them: a segment only
    // counts as durable at the destination once its marker landed.
    planned.sort_by(|a, b| {
        let commit = |p: &Planned| p.rel.ends_with(&format!("/{}", COMMIT_FILE));
        (commit(a), &a.rel).cmp(&(commit(b), &b.rel))
    });
    Ok((planned, left_behind))
}

/// The segment commit marker's file name (`segment_commit::COMMIT_FILE`,
/// which is only compiled with S3).
const COMMIT_FILE: &str = "_COMMIT";

fn copy_missing(
    objects: &dyn SegmentObjects,
    from: &NamespaceId,
    to: &NamespaceId,
    planned: &[Planned],
    out: &mut dyn Write,
    mut keep_alive: impl FnMut() -> Result<(), String>,
) -> Result<usize, String> {
    let todo: Vec<&Planned> = planned.iter().filter(|p| !p.in_place).collect();
    let mut last_renew = Instant::now();
    for (done, object) in todo.iter().enumerate() {
        objects.copy(
            &format!("{}/{}", from.0, object.rel),
            &format!("{}/{}", to.0, object.rel),
            object.size,
        )?;
        if last_renew.elapsed() >= LEASE_RENEW_EVERY {
            keep_alive()?;
            last_renew = Instant::now();
        }
        if (done + 1) % 500 == 0 {
            let _ = writeln!(out, "  copied {}/{} objects", done + 1, todo.len());
        }
    }
    Ok(todo.len())
}

/// Every planned object is at the destination at its size.
fn verify_copies(
    objects: &dyn SegmentObjects,
    to: &NamespaceId,
    planned: &[Planned],
) -> Result<(), String> {
    let target: HashMap<String, u64> = objects.list(&to.0)?.into_iter().collect();
    let (mut count, mut bytes) = (0usize, 0u64);
    for object in planned {
        match target.get(&object.rel) {
            Some(size) if *size == object.size => {
                count += 1;
                bytes += size;
            }
            Some(size) => {
                return Err(format!(
                    "verification failed: {}/{} is {size} bytes, the source {}",
                    to.0, object.rel, object.size
                ))
            }
            None => {
                return Err(format!(
                    "verification failed: {}/{} is missing",
                    to.0, object.rel
                ))
            }
        }
    }
    let expected: u64 = planned.iter().map(|p| p.size).sum();
    if count != planned.len() || bytes != expected {
        return Err(format!(
            "verification failed: {count} objects / {bytes} bytes at {}, expected {} / {expected}",
            to.0,
            planned.len()
        ));
    }
    Ok(())
}

/// The manifest under `to` lists the same segments and documents as the
/// one moved from `from`, and `from` is gone.
fn verify_renamed(
    ctrl: &dyn ControlStore,
    from: &NamespaceId,
    to: &NamespaceId,
    expected: &Manifest,
) -> Result<u64, String> {
    if ctrl
        .try_manifest_cloned(from)
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err(format!("{} still has a manifest after the rename", from.0));
    }
    let reopened = ctrl
        .try_manifest_cloned(to)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("{} has no manifest after the rename", to.0))?;
    let ids = |m: &Manifest| -> Vec<(String, u32)> {
        let mut ids: Vec<_> = m
            .segments
            .iter()
            .map(|s| (s.segment_id.0.clone(), s.doc_count))
            .collect();
        ids.sort();
        ids
    };
    if ids(&reopened) != ids(expected) || documents(&reopened) != documents(expected) {
        return Err(format!(
            "{} lists {} segments / {} documents after the rename, expected {} / {}",
            to.0,
            reopened.segments.len(),
            documents(&reopened),
            expected.segments.len(),
            documents(expected)
        ));
    }
    Ok(documents(&reopened))
}

fn lease_holder() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into());
    format!("kosha-migrate-namespace/{host}/{}", std::process::id())
}

/// Move `from` to `to`. `wal` is every WAL stream a writer of `from` may
/// have appended to, or `None` to skip that check.
pub(crate) fn migrate<C>(
    ctrl: &mut C,
    objects: &dyn SegmentObjects,
    wal: Option<&[Arc<dyn WalStore>]>,
    args: &Args,
    out: &mut dyn Write,
) -> Result<Report, String>
where
    C: ControlStore + NamespaceRename,
{
    let from = args.from.clone().ok_or("--from is required")?;
    let to = args.to.clone().ok_or("--to is required")?;
    let read = |ctrl: &C, id: &NamespaceId| {
        ctrl.try_manifest_cloned(id)
            .map_err(|e| format!("reading {}'s manifest: {e}", id.0))
    };
    let (source, target) = (read(ctrl, &from)?, read(ctrl, &to)?);
    let _ = writeln!(
        out,
        "migrate-namespace {} -> {} ({}){}",
        from.0,
        to.0,
        objects.describe(),
        if args.dry_run { " [dry run]" } else { "" }
    );
    match (source, target) {
        (Some(_), Some(_)) => Err(format!(
            "both {} and {} exist in the control plane; refusing to overwrite {}",
            from.0, to.0, to.0
        )),
        (None, None) => Err(format!("namespace {} does not exist", from.0)),
        (None, Some(manifest)) => finish_after_rename(objects, &from, &to, &manifest, args, out),
        (Some(_), None) => fresh(ctrl, objects, wal, &from, &to, args, out),
    }
}

/// The control plane already points at `to` (an earlier run died after
/// the rename, or ran with `--keep-source`): delete the source objects.
///
/// The rename only ran after every object was verified at the destination,
/// and since then `to` is the tenant's live namespace: a writer may have
/// rewritten objects in place (a segment's `footer.json`) or collected
/// them. So nothing is copied, whatever the sizes; only source objects
/// whose destination exists are deleted, and any other is left in place.
fn finish_after_rename(
    objects: &dyn SegmentObjects,
    from: &NamespaceId,
    to: &NamespaceId,
    manifest: &Manifest,
    args: &Args,
    out: &mut dyn Write,
) -> Result<Report, String> {
    let owned = owned_segment_dirs(manifest, to);
    let (candidates, mut left_behind) = plan_objects(objects, from, to, &owned)?;
    let target: BTreeSet<String> = objects
        .list(&to.0)?
        .into_iter()
        .map(|(rel, _)| rel)
        .collect();
    let (planned, missing): (Vec<Planned>, Vec<Planned>) = candidates
        .into_iter()
        .partition(|p| target.contains(&p.rel));
    left_behind += missing.len();
    let mut report = Report {
        objects: planned.len(),
        bytes: planned.iter().map(|p| p.size).sum(),
        left_behind,
        documents: documents(manifest),
        resumed: true,
        ..Default::default()
    };
    let _ = writeln!(
        out,
        "control plane already re-keyed to {} (version {}, {} documents); \
         {} source objects ({} bytes) under {}/ are also at the destination; nothing is \
         copied, the destination is authoritative",
        to.0, manifest.version, report.documents, report.objects, report.bytes, from.0
    );
    if !missing.is_empty() {
        let _ = writeln!(
            out,
            "{} source objects under {}/ have no destination object and are left alone",
            missing.len(),
            from.0
        );
    }
    if args.dry_run {
        return Ok(report);
    }
    report.deleted = delete_source(objects, from, &planned, args, out)?;
    Ok(report)
}

fn delete_source(
    objects: &dyn SegmentObjects,
    from: &NamespaceId,
    planned: &[Planned],
    args: &Args,
    out: &mut dyn Write,
) -> Result<usize, String> {
    if args.keep_source {
        let _ = writeln!(
            out,
            "--keep-source: left {} objects under {}/; rerun without it to delete them",
            planned.len(),
            from.0
        );
        return Ok(0);
    }
    let paths: Vec<String> = planned
        .iter()
        .map(|p| format!("{}/{}", from.0, p.rel))
        .collect();
    let deleted = objects.delete(&paths)?;
    let _ = writeln!(out, "deleted {deleted} source objects under {}/", from.0);
    Ok(deleted)
}

#[allow(clippy::too_many_arguments)]
fn fresh<C>(
    ctrl: &mut C,
    objects: &dyn SegmentObjects,
    wal: Option<&[Arc<dyn WalStore>]>,
    from: &NamespaceId,
    to: &NamespaceId,
    args: &Args,
    out: &mut dyn Write,
) -> Result<Report, String>
where
    C: ControlStore + NamespaceRename,
{
    let holder = lease_holder();
    let mut leases: Vec<(NamespaceId, u64)> = Vec::new();
    if !args.dry_run {
        for id in [from, to] {
            match ctrl.acquire_writer_lease(id, &holder, LEASE_TTL) {
                Ok(Some(token)) => leases.push((id.clone(), token)),
                Ok(None) => {
                    release(ctrl, &holder, &leases);
                    return Err(format!(
                        "a live writer holds the writer lease on {}; stop the ingest writer \
                         (scale it to 0) and rerun",
                        id.0
                    ));
                }
                Err(e) => {
                    release(ctrl, &holder, &leases);
                    return Err(format!("taking the writer lease on {}: {e}", id.0));
                }
            }
        }
    }
    let result = fresh_leased(ctrl, objects, wal, from, to, args, out, &holder, &leases);
    match &result {
        Ok((_, Some(token))) => release(ctrl, &holder, &[(to.clone(), *token)]),
        Ok((_, None)) => {}
        Err(_) => release(ctrl, &holder, &leases),
    }
    result.map(|(report, _)| report)
}

fn release<C: ControlStore + ?Sized>(ctrl: &mut C, holder: &str, leases: &[(NamespaceId, u64)]) {
    for (id, token) in leases {
        if let Err(e) = ctrl.release_writer_lease(id, holder, *token) {
            eprintln!("WARN: releasing the writer lease on {}: {e}", id.0);
        }
    }
}

/// [`fresh`] with the leases held. Returns the lease token now on `to`.
#[allow(clippy::too_many_arguments)]
fn fresh_leased<C>(
    ctrl: &mut C,
    objects: &dyn SegmentObjects,
    wal: Option<&[Arc<dyn WalStore>]>,
    from: &NamespaceId,
    to: &NamespaceId,
    args: &Args,
    out: &mut dyn Write,
    holder: &str,
    leases: &[(NamespaceId, u64)],
) -> Result<(Report, Option<u64>), String>
where
    C: ControlStore + NamespaceRename,
{
    // Read under the lease: from here on no leased writer can publish.
    let manifest = ctrl
        .try_manifest_cloned(from)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("namespace {} does not exist", from.0))?;
    if manifest.shards.is_some_and(|n| n > 1) {
        return Err(format!(
            "{} is a sharded namespace ({} shards); migrate-namespace does not move \
             sharded namespaces",
            from.0,
            manifest.shards.unwrap_or(0)
        ));
    }
    let dependents = storage_dependents(ctrl, from)?;
    if !dependents.is_empty() {
        let names: Vec<&str> = dependents.iter().map(|d| d.0.as_str()).collect();
        return Err(format!(
            "{} share segments stored under {}/ (branches); delete or migrate them first",
            names.join(", "),
            from.0
        ));
    }
    // Highest LSN of `from`'s records in each checked stream, marked
    // discarded before the rename.
    let mut wal_high: Vec<u64> = Vec::new();
    match wal {
        Some(streams) => {
            let mut tail = 0;
            for store in streams {
                let scan = wal_scan(store.as_ref(), from, &manifest)
                    .map_err(|e| format!("reading the WAL: {e}"))?;
                tail += scan.tail;
                wal_high.push(scan.high);
            }
            if tail > 0 {
                return Err(format!(
                    "the WAL holds {tail} record(s) for {} past its manifest checkpoint {}; \
                     start the writer so its idle flush publishes them, stop it, and rerun",
                    from.0, manifest.wal_checkpoint
                ));
            }
            let _ = writeln!(
                out,
                "WAL: {} stream(s) checked, no unflushed records for {}",
                streams.len(),
                from.0
            );
        }
        None => {
            let _ = writeln!(
                out,
                "WAL: check skipped (--skip-wal-check); the writer must have been flushed and \
                 stopped, and its WAL cannot be marked: a WAL file still holding records of {} \
                 replays them as a stale bare namespace on the writer's next boot",
                from.0
            );
        }
    }

    let owned = owned_segment_dirs(&manifest, from);
    let (planned, left_behind) = plan_objects(objects, from, to, &owned)?;
    let in_place = planned.iter().filter(|p| p.in_place).count();
    let mut report = Report {
        objects: planned.len(),
        bytes: planned.iter().map(|p| p.size).sum(),
        left_behind,
        documents: documents(&manifest),
        ..Default::default()
    };
    let _ = writeln!(
        out,
        "plan: copy {} objects ({} bytes, {} already in place) of {} segment dirs \
         from {}/ to {}/",
        report.objects,
        report.bytes,
        in_place,
        owned.len(),
        from.0,
        to.0
    );
    let _ = writeln!(
        out,
        "plan: re-key kosha.namespaces, kosha.manifests (version {}, {} segments, {} documents), \
         kosha.writer_leases and kosha.retired_namespaces from {} to {}",
        manifest.version,
        manifest.segments.len(),
        report.documents,
        from.0,
        to.0
    );
    if left_behind > 0 {
        let _ = writeln!(
            out,
            "plan: {left_behind} other objects under {}/ belong to no segment the manifest \
             references (another tenant's data, or garbage) and are left alone",
            from.0
        );
    }
    let _ = writeln!(
        out,
        "plan: {}",
        if args.keep_source {
            format!("keep the source objects under {}/", from.0)
        } else {
            format!("then delete the {} source objects", report.objects)
        }
    );
    if args.dry_run {
        return Ok((report, None));
    }
    if planned.is_empty() && !manifest.segments.is_empty() {
        return Err(format!(
            "{} lists {} segments but none of their objects were found under {}/ in {}",
            from.0,
            manifest.segments.len(),
            from.0,
            objects.describe()
        ));
    }

    report.copied = copy_missing(objects, from, to, &planned, out, || {
        for (id, token) in leases {
            match ctrl.renew_writer_lease(id, holder, *token, LEASE_TTL) {
                Ok(true) => {}
                Ok(false) => return Err(format!("lost the writer lease on {}", id.0)),
                Err(e) => return Err(format!("renewing the writer lease on {}: {e}", id.0)),
            }
        }
        Ok(())
    })?;
    verify_copies(objects, to, &planned)?;
    let _ = writeln!(
        out,
        "copied {} objects; verified {} objects / {} bytes at {}/",
        report.copied, report.objects, report.bytes, to.0
    );

    // Every record of `from` left in the checked streams is covered by its
    // manifest (the tail is empty). Once the rename removes that manifest,
    // recovery would replay them under the bare id and the idle flush would
    // republish a stale bare namespace, so mark them discarded, as a DELETE
    // does. This runs under the leases, just before the rename: should the
    // rename fail, `from` keeps a manifest whose checkpoint already covers
    // every record marked here, so nothing is lost; and a run that dies
    // after the rename leaves no unmarked window behind.
    if let Some(streams) = wal {
        let mut marked = 0;
        for (store, high) in streams.iter().zip(&wal_high) {
            if discard_wal_records(store.as_ref(), from, *high)
                .map_err(|e| format!("marking {}'s WAL records discarded: {e}", from.0))?
            {
                marked += 1;
            }
        }
        let _ = writeln!(
            out,
            "WAL: marked {}'s records discarded in {marked} stream(s)",
            from.0
        );
    }

    let outcome = ctrl
        .rename_namespace(from, to, manifest.version)
        .map_err(|e| format!("control-plane rename failed (nothing re-keyed): {e}"))?;
    let _ = writeln!(
        out,
        "control plane re-keyed: {} -> {} at version {}",
        from.0, to.0, outcome.manifest.version
    );
    report.documents = verify_renamed(ctrl, from, to, &manifest)?;
    let _ = writeln!(
        out,
        "verified: {} lists {} segments / {} documents",
        to.0,
        manifest.segments.len(),
        report.documents
    );
    report.deleted = delete_source(objects, from, &planned, args, out)?;
    Ok((report, outcome.lease_token))
}

// ─── Batch and verify ────────────────────────────────────────────────────────

/// What a batch did with one namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Moved,
    /// `--dry-run`: the plan passed every check a dry run makes.
    WouldMove,
    /// Nothing left to do (already migrated).
    Skipped,
    /// Refused or failed; the reason is the row's detail.
    Refused,
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Outcome::Moved => "moved",
            Outcome::WouldMove => "would move",
            Outcome::Skipped => "skipped",
            Outcome::Refused => "refused",
        }
    }
}

/// One line of the batch summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BatchRow {
    pub from: NamespaceId,
    pub to: NamespaceId,
    pub outcome: Outcome,
    pub documents: u64,
    pub objects: usize,
    pub detail: String,
}

/// The pairs a batch or `--verify` covers: the `--map` file, every bare
/// namespace mapped to `<tenant>/<bare>`, or the single `--from`/`--to`.
fn resolve_pairs(ctrl: &dyn ControlStore, args: &Args) -> Result<Vec<Pair>, String> {
    if let Some(path) = &args.map {
        return load_map(path);
    }
    if args.all_unreachable {
        let bare = unreachable_bare_namespaces(&ctrl.list_namespaces());
        return Ok(match &args.tenant {
            Some(tenant) => bare
                .into_iter()
                .map(|b| {
                    let to = NamespaceId(format!("{tenant}/{}", b.0));
                    (b, to)
                })
                .collect(),
            // `--verify --all-unreachable`: no destination to check, only
            // that nothing bare is left (each one left fails as a row).
            None => bare.into_iter().map(|b| (b.clone(), b)).collect(),
        });
    }
    match (&args.from, &args.to) {
        (Some(from), Some(to)) => Ok(vec![(from.clone(), to.clone())]),
        _ => Err(format!("--from and --to are required\n{USAGE}")),
    }
}

/// Run [`migrate`] for each pair in order, continuing past a namespace it
/// refuses, then print the summary table.
pub(crate) fn migrate_batch<C>(
    ctrl: &mut C,
    objects: &dyn SegmentObjects,
    wal: Option<&[Arc<dyn WalStore>]>,
    pairs: &[Pair],
    args: &Args,
    out: &mut dyn Write,
) -> Vec<BatchRow>
where
    C: ControlStore + NamespaceRename,
{
    let _ = writeln!(
        out,
        "migrate-namespace batch: {} namespace(s) ({}){}",
        pairs.len(),
        objects.describe(),
        if args.dry_run { " [dry run]" } else { "" }
    );
    let mut rows = Vec::with_capacity(pairs.len());
    for (index, (from, to)) in pairs.iter().enumerate() {
        let _ = writeln!(
            out,
            "\n[{}/{}] {} -> {}",
            index + 1,
            pairs.len(),
            from.0,
            to.0
        );
        let one = Args {
            from: Some(from.clone()),
            to: Some(to.clone()),
            dry_run: args.dry_run,
            keep_source: args.keep_source,
            skip_wal_check: args.skip_wal_check,
            ..Default::default()
        };
        let row = match validate_pair(from, to)
            .and_then(|()| migrate(ctrl, objects, wal, &one, out))
        {
            Ok(report) => {
                let (outcome, detail) =
                    if report.resumed && (report.objects == 0 || args.keep_source) {
                        (
                            Outcome::Skipped,
                            if report.objects == 0 {
                                "already migrated".to_owned()
                            } else {
                                format!("already migrated; {} source objects kept", report.objects)
                            },
                        )
                    } else if args.dry_run {
                        let detail = if report.resumed {
                            format!(
                                "already re-keyed; would delete {} source objects",
                                report.objects
                            )
                        } else {
                            format!("{} bytes to copy", report.bytes)
                        };
                        (Outcome::WouldMove, detail)
                    } else if report.resumed {
                        (
                            Outcome::Moved,
                            format!("resumed; deleted {} source objects", report.deleted),
                        )
                    } else {
                        (
                            Outcome::Moved,
                            format!("copied {}, deleted {}", report.copied, report.deleted),
                        )
                    };
                BatchRow {
                    from: from.clone(),
                    to: to.clone(),
                    outcome,
                    documents: report.documents,
                    objects: report.objects,
                    detail,
                }
            }
            Err(reason) => {
                let _ = writeln!(out, "refused: {reason}");
                BatchRow {
                    from: from.clone(),
                    to: to.clone(),
                    outcome: Outcome::Refused,
                    documents: 0,
                    objects: 0,
                    detail: reason,
                }
            }
        };
        rows.push(row);
    }
    print_batch_summary(&rows, args.dry_run, out);
    rows
}

/// Left-aligned columns, two spaces apart. A detail is cut at its first
/// line so a multi-line reason keeps the table readable.
fn print_table(header: &[&str], rows: &[Vec<String>], out: &mut dyn Write) {
    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let line = |cells: Vec<&str>| -> String {
        let last = cells.len() - 1;
        cells
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                if i == last {
                    (*cell).to_owned()
                } else {
                    format!("{cell:<width$}", width = widths[i])
                }
            })
            .collect::<Vec<_>>()
            .join("  ")
    };
    let _ = writeln!(out, "{}", line(header.to_vec()));
    for row in rows {
        let _ = writeln!(out, "{}", line(row.iter().map(String::as_str).collect()));
    }
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or_default().to_owned()
}

fn print_batch_summary(rows: &[BatchRow], dry_run: bool, out: &mut dyn Write) {
    let _ = writeln!(
        out,
        "\nsummary{}:",
        if dry_run {
            " [dry run, nothing changed]"
        } else {
            ""
        }
    );
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|r| {
            vec![
                r.from.0.clone(),
                r.to.0.clone(),
                r.outcome.label().to_owned(),
                r.documents.to_string(),
                r.objects.to_string(),
                first_line(&r.detail),
            ]
        })
        .collect();
    print_table(
        &["FROM", "TO", "RESULT", "DOCS", "OBJECTS", "DETAIL"],
        &cells,
        out,
    );
    let count = |o: Outcome| rows.iter().filter(|r| r.outcome == o).count();
    let _ = writeln!(
        out,
        "{} moved, {} would move, {} skipped, {} refused",
        count(Outcome::Moved),
        count(Outcome::WouldMove),
        count(Outcome::Skipped),
        count(Outcome::Refused)
    );
}

/// One line of the `--verify` report; `problem` set means it failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifyRow {
    pub from: NamespaceId,
    pub to: NamespaceId,
    pub documents: u64,
    pub segments: usize,
    pub problem: Option<String>,
    pub note: Option<String>,
}

/// Re-check one finished move: `from` has no manifest, `to` has one whose
/// document count is the sum of its segments', and every segment it lists
/// (other than those inherited from a branch parent) has objects under
/// `{to}/`. Source objects still under `{from}/` (a `--keep-source` run)
/// are noted, not failed.
fn verify_pair(
    ctrl: &dyn ControlStore,
    objects: &dyn SegmentObjects,
    from: &NamespaceId,
    to: &NamespaceId,
) -> VerifyRow {
    let mut row = VerifyRow {
        from: from.clone(),
        to: to.clone(),
        documents: 0,
        segments: 0,
        problem: None,
        note: None,
    };
    let manifest = |id: &NamespaceId| {
        ctrl.try_manifest_cloned(id)
            .map_err(|e| format!("reading {}'s manifest: {e}", id.0))
    };
    match manifest(from) {
        Err(e) => {
            row.problem = Some(e);
            return row;
        }
        Ok(Some(_)) => {
            row.problem = Some(format!(
                "{} is still in the control plane (not migrated)",
                from.0
            ));
            return row;
        }
        Ok(None) => {}
    }
    let target = match manifest(to) {
        Err(e) => {
            row.problem = Some(e);
            return row;
        }
        Ok(None) => {
            row.problem = Some(format!("{} does not exist", to.0));
            return row;
        }
        Ok(Some(target)) => target,
    };
    row.documents = documents(&target);
    let own: BTreeSet<String> = target
        .segments
        .iter()
        .filter(|s| !target.is_inherited_segment(to, &s.segment_id))
        .map(|s| s.segment_id.0.clone())
        .collect();
    row.segments = own.len();
    let per_segment = |ns: &NamespaceId| -> Result<HashMap<String, usize>, String> {
        let mut counts = HashMap::new();
        for (rel, _) in objects.list(&ns.0)? {
            if let Some((segment, _)) = rel.split_once('/') {
                if own.contains(segment) {
                    *counts.entry(segment.to_owned()).or_insert(0) += 1;
                }
            }
        }
        Ok(counts)
    };
    let at_target = match per_segment(to) {
        Ok(counts) => counts,
        Err(e) => {
            row.problem = Some(e);
            return row;
        }
    };
    let missing: Vec<&String> = own.iter().filter(|s| !at_target.contains_key(*s)).collect();
    if let Some(first) = missing.first() {
        row.problem = Some(format!(
            "{} of {} segments have no objects under {}/ (first: {first})",
            missing.len(),
            own.len(),
            to.0
        ));
        return row;
    }
    match per_segment(from) {
        Ok(left) if !left.is_empty() => {
            row.note = Some(format!(
                "{} source objects still under {}/ (--keep-source; rerun the move without it to \
                 delete them)",
                left.values().sum::<usize>(),
                from.0
            ));
        }
        Ok(_) => {}
        Err(e) => row.problem = Some(e),
    }
    row
}

/// `--verify`: re-check each pair, then that no bare namespace is left.
/// Returns the rows and the bare ids still unreachable.
pub(crate) fn verify(
    ctrl: &dyn ControlStore,
    objects: &dyn SegmentObjects,
    pairs: &[Pair],
    out: &mut dyn Write,
) -> (Vec<VerifyRow>, Vec<NamespaceId>) {
    let _ = writeln!(
        out,
        "migrate-namespace verify: {} namespace(s) ({})",
        pairs.len(),
        objects.describe()
    );
    let rows: Vec<VerifyRow> = pairs
        .iter()
        .map(|(from, to)| verify_pair(ctrl, objects, from, to))
        .collect();
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|r| {
            vec![
                r.from.0.clone(),
                r.to.0.clone(),
                if r.problem.is_some() { "FAILED" } else { "ok" }.to_owned(),
                r.documents.to_string(),
                r.segments.to_string(),
                first_line(r.problem.as_deref().or(r.note.as_deref()).unwrap_or("")),
            ]
        })
        .collect();
    print_table(
        &["FROM", "TO", "RESULT", "DOCS", "SEGMENTS", "DETAIL"],
        &cells,
        out,
    );
    let bare = unreachable_bare_namespaces(&ctrl.list_namespaces());
    if bare.is_empty() {
        let _ = writeln!(out, "unreachable bare namespaces: none");
    } else {
        let names: Vec<&str> = bare.iter().map(|b| b.0.as_str()).collect();
        let _ = writeln!(
            out,
            "unreachable bare namespaces: {} ({})",
            bare.len(),
            names.join(", ")
        );
    }
    (rows, bare)
}

/// Run what `args` asks for over a connected control plane and segment
/// store: one move, a batch, or `--verify`. A batch that refused any
/// namespace and a failed verify are errors (non-zero exit).
pub(crate) fn execute<C>(
    ctrl: &mut C,
    objects: &dyn SegmentObjects,
    wal: Option<&[Arc<dyn WalStore>]>,
    args: &Args,
    out: &mut dyn Write,
) -> Result<(), String>
where
    C: ControlStore + NamespaceRename,
{
    if args.verify {
        let pairs = resolve_pairs(ctrl, args)?;
        let (rows, bare) = verify(ctrl, objects, &pairs, out);
        let failed = rows.iter().filter(|r| r.problem.is_some()).count();
        if failed > 0 || !bare.is_empty() {
            return Err(format!(
                "verify failed: {failed} of {} namespace(s) failed, {} bare namespace(s) still \
                 unreachable",
                rows.len(),
                bare.len()
            ));
        }
        let _ = writeln!(out, "verify: OK");
        return Ok(());
    }
    if args.is_batch() {
        let pairs = resolve_pairs(ctrl, args)?;
        if pairs.is_empty() {
            let _ = writeln!(out, "no bare namespaces to migrate");
            return Ok(());
        }
        let rows = migrate_batch(ctrl, objects, wal, &pairs, args, out);
        let refused = rows
            .iter()
            .filter(|r| r.outcome == Outcome::Refused)
            .count();
        if refused > 0 {
            return Err(format!(
                "{refused} of {} namespace(s) refused; fix them and rerun the batch (finished \
                 ones are skipped)",
                rows.len()
            ));
        }
        return Ok(());
    }
    let report = migrate(ctrl, objects, wal, args, out)?;
    if args.dry_run {
        let _ = writeln!(out, "dry run: nothing changed");
    } else {
        let _ = writeln!(
            out,
            "done: {} documents now under {}; copied {} of {} objects, deleted {}",
            report.documents,
            args.to.as_ref().map_or("", |t| t.0.as_str()),
            report.copied,
            report.objects,
            report.deleted
        );
    }
    Ok(())
}

// ─── Command line ────────────────────────────────────────────────────────────

/// `kosha-server migrate-namespace ...`: run and return the exit code.
pub(crate) fn main(args: impl IntoIterator<Item = String>) -> i32 {
    match run(args) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("migrate-namespace failed: {error}");
            1
        }
    }
}

fn run(args: impl IntoIterator<Item = String>) -> Result<(), String> {
    let args = parse_args(args)?;
    #[cfg(feature = "postgres")]
    {
        let url = std::env::var("DATABASE_URL").map_err(|_| {
            "DATABASE_URL is not set: migrate-namespace re-keys the Postgres control plane \
             (an in-memory control plane holds nothing to migrate)"
                .to_string()
        })?;
        // Same TLS rule as the server (#343), before the first connection.
        crate::internal_tls::check_database_url_env()?;
        let mut ctrl = kosha_control::PgStore::connect_with_retry(&url)?;
        if args.list_unreachable {
            let bare = unreachable_bare_namespaces(&ctrl.list_namespaces());
            for id in &bare {
                let docs = ctrl
                    .try_manifest_cloned(id)
                    .map_err(|e| e.to_string())?
                    .map_or(0, |m| documents(&m));
                println!("{}\t{docs} documents", id.0);
            }
            eprintln!("{} bare namespace(s)", bare.len());
            return Ok(());
        }
        let data_dir = PathBuf::from(
            std::env::var("KOSHA_DATA_DIR").unwrap_or_else(|_| "/var/lib/kosha/data".into()),
        );
        let mut stdout = std::io::stdout();
        #[cfg(feature = "s3")]
        if let Some(config) = crate::s3_storage::S3Config::from_env() {
            let s3 = connect_s3(config)?;
            let objects = s3_objects::S3Objects::new(&s3)?;
            // `--verify` only reads the control plane and the segments.
            let wal = if args.verify {
                None
            } else {
                wal_streams(&args, &data_dir, Some(&s3))?
            };
            return execute(&mut ctrl, &objects, wal.as_deref(), &args, &mut stdout);
        }
        let objects = LocalObjects::new(data_dir.clone());
        let wal = if args.verify {
            None
        } else {
            wal_streams(
                &args,
                &data_dir,
                #[cfg(feature = "s3")]
                None,
            )?
        };
        execute(&mut ctrl, &objects, wal.as_deref(), &args, &mut stdout)
    }
    #[cfg(not(feature = "postgres"))]
    {
        let _ = args;
        Err(
            "this binary was built without the `postgres` feature; migrate-namespace \
             re-keys the Postgres control plane"
                .into(),
        )
    }
}

#[cfg(all(feature = "s3", feature = "postgres"))]
fn connect_s3(config: crate::s3_storage::S3Config) -> Result<crate::s3_storage::S3Storage, String> {
    let local =
        std::env::temp_dir().join(format!("kosha-migrate-namespace-{}", std::process::id()));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    rt.block_on(crate::s3_storage::S3Storage::new(local, config))
        .map_err(|e| format!("S3: {e}"))
}

/// The WAL streams to check, per `KOSHA_WAL_MODE`: every stream under
/// `_wal/` in the bucket (`s3`), or `KOSHA_DATA_DIR/_wal` (`local`). `None`
/// with `--skip-wal-check`.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
fn wal_streams(
    args: &Args,
    data_dir: &Path,
    #[cfg(feature = "s3")] s3: Option<&crate::s3_storage::S3Storage>,
) -> Result<Option<Vec<Arc<dyn WalStore>>>, String> {
    if args.skip_wal_check {
        return Ok(None);
    }
    let mode = std::env::var("KOSHA_WAL_MODE")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if mode == "s3" {
        #[cfg(feature = "s3")]
        {
            let s3 = s3.ok_or("KOSHA_WAL_MODE=s3 needs KOSHA_S3_BUCKET")?;
            let writers: BTreeSet<String> = s3
                .list_objects_remote("_wal/")
                .map_err(|e| format!("listing _wal/: {e}"))?
                .into_iter()
                .filter_map(|o| {
                    o.path
                        .strip_prefix("_wal/")
                        .and_then(|rest| rest.split('/').next())
                        .map(str::to_owned)
                })
                .collect();
            let mut streams: Vec<Arc<dyn WalStore>> = Vec::new();
            for writer in writers {
                let backend =
                    crate::s3_wal::S3WalBackend::new(s3, crate::s3_wal::timeout_from_env())
                        .map_err(|e| format!("S3 WAL client: {e}"))?;
                let store =
                    kosha_write::object_wal::ObjectWalStore::new(Arc::new(backend), &writer)
                        .map_err(|e| e.to_string())?;
                streams.push(Arc::new(store));
            }
            return Ok(Some(streams));
        }
        #[cfg(not(feature = "s3"))]
        return Err("KOSHA_WAL_MODE=s3 needs a build with the `s3` feature".into());
    }
    let dir = data_dir.join("_wal");
    if !dir.is_dir() {
        return Err(format!(
            "no local WAL at {} to check. Run with the ingest writer's KOSHA_DATA_DIR, or flush \
             the namespace, stop the writer and pass --skip-wal-check",
            dir.display()
        ));
    }
    Ok(Some(vec![Arc::new(kosha_write::wal::LocalWalStore::new(
        dir,
    ))]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_control::Controller;
    use kosha_core::{Document, DocumentId, Field, ManifestEntry, SegmentId};

    fn args(list: &[&str]) -> Result<Args, String> {
        parse_args(list.iter().map(|s| s.to_string()))
    }

    fn ns(id: &str) -> NamespaceId {
        NamespaceId(id.into())
    }

    #[test]
    fn parses_flags_in_both_forms() {
        let parsed = args(&["--from", "corpus", "--to=acme/corpus", "--dry-run"]).unwrap();
        assert_eq!(
            parsed,
            Args {
                from: Some(ns("corpus")),
                to: Some(ns("acme/corpus")),
                dry_run: true,
                ..Default::default()
            }
        );
        let parsed = args(&[
            "--keep-source",
            "--from=c",
            "--to",
            "t/n",
            "--skip-wal-check",
        ])
        .unwrap();
        assert!(parsed.keep_source && parsed.skip_wal_check && !parsed.dry_run);
        let listed = args(&["--list-unreachable"]).unwrap();
        assert!(listed.list_unreachable && listed.from.is_none());
    }

    #[test]
    fn rejects_bad_ids_and_arguments() {
        for bad in [
            &["--from", "corpus"][..],
            &["--to", "acme/corpus"],
            &["--from", "a/b", "--to", "acme/corpus"],
            &["--from", "corpus", "--to", "corpus"],
            &["--from", "corpus", "--to", "acme/x/y"],
            &["--from", "corpus", "--to", "/corpus"],
            &["--from", "corpus", "--to", "acme/"],
            &["--from", "corpus", "--to", "acme/_shard-0001"],
            &["--from", "..", "--to", "acme/x"],
            &["--from"],
            &["--from", "", "--to", "a/b"],
            &["--from", "c", "--to", "a/b", "--force"],
        ] {
            assert!(args(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn bare_namespaces_are_the_ones_without_a_tenant() {
        let ids = [
            ns("zeta"),
            ns("acme/docs"),
            ns("alpha"),
            ns("acme/docs/_shard-0001"),
        ];
        assert_eq!(
            unreachable_bare_namespaces(&ids),
            vec![ns("alpha"), ns("zeta")]
        );
    }

    #[test]
    fn copy_source_encodes_everything_but_unreserved_and_slash() {
        assert_eq!(
            copy_source("bucket", "segments/acme/my ns/seg-1/_COMMIT"),
            "bucket/segments/acme/my%20ns/seg-1/_COMMIT"
        );
        assert_eq!(copy_source("b", "a+b=c"), "b/a%2Bb%3Dc");
        assert_eq!(part_ranges(10, 4), vec![(0, 3), (4, 7), (8, 9)]);
        assert_eq!(part_ranges(8, 4), vec![(0, 3), (4, 7)]);
    }

    fn manifest_with(version: u64, segments: &[(&str, u32)]) -> Manifest {
        let mut manifest = Manifest::empty(version);
        for (id, docs) in segments {
            manifest.segments.push(ManifestEntry {
                segment_id: SegmentId((*id).into()),
                doc_count: *docs,
            });
        }
        manifest
    }

    #[test]
    fn wal_tail_counts_only_unflushed_records_of_the_namespace() {
        let dir = crate::tests::test_dir().join("_wal");
        let writer = kosha_write::wal::WalWriter::new(dir.clone());
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", "x")],
        };
        let flushed = writer.append(&ns("corpus"), &[doc("a")]).unwrap();
        writer.append(&ns("other"), &[doc("b")]).unwrap();
        let store = kosha_write::wal::LocalWalStore::new(dir);
        let mut manifest = manifest_with(1, &[]);
        manifest.wal_origin = Some(writer.origin().to_owned());
        manifest.wal_checkpoint = flushed;
        assert_eq!(
            wal_tail_records(&store, &ns("corpus"), &manifest).unwrap(),
            0
        );

        writer.append(&ns("corpus"), &[doc("c")]).unwrap();
        assert_eq!(
            wal_tail_records(&store, &ns("corpus"), &manifest).unwrap(),
            1
        );
        // A checkpoint from another WAL stream covers none of this one.
        manifest.wal_origin = Some("another-writer".into());
        assert_eq!(
            wal_tail_records(&store, &ns("corpus"), &manifest).unwrap(),
            2
        );
    }

    /// The whole flow over the local data directory, with an in-memory
    /// control plane standing in for Postgres: a bare namespace written
    /// the pre-#262 way becomes the tenant's namespace with the same
    /// document count, searchable through `/v1`, and the bare id is gone.
    #[test]
    fn bare_namespace_becomes_searchable_under_the_tenant() {
        use crate::{handle_flush, handle_index, route, AppState, StateOptions};

        let dir = crate::tests::test_dir();
        let bare = ns("corpus");
        let to = ns("acme/corpus");
        let options = |control: Controller| StateOptions {
            control_store: Some(Box::new(control)),
            role: Some(crate::role::Role::Standalone),
            writer_lease: Some(false),
            ..Default::default()
        };
        let v1 =
            |state: &AppState, line: &str, body: serde_json::Value| -> (u16, serde_json::Value) {
                let bytes = if body.is_null() {
                    Vec::new()
                } else {
                    serde_json::to_vec(&body).unwrap()
                };
                let response = route(
                    &format!("{line} HTTP/1.1\r\n"),
                    &HashMap::new(),
                    &bytes,
                    "acme",
                    state,
                );
                let code = response
                    .get(9..12)
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0);
                let body = response
                    .split_once("\r\n\r\n")
                    .and_then(|(_, b)| serde_json::from_str(b).ok())
                    .unwrap_or(serde_json::Value::Null);
                (code, body)
            };

        // Before #262: documents written under the bare id, in two segments.
        let state = AppState::with_options(dir.clone(), options(Controller::new()));
        for batch in [["d1", "d2", "d3"], ["d4", "d5", "d6"]] {
            let body = serde_json::to_vec(&kosha_core::IndexRequest {
                namespace: bare.clone(),
                documents: batch
                    .iter()
                    .map(|id| Document {
                        id: DocumentId((*id).into()),
                        fields: vec![Field::text("title", "quick brown fox")],
                    })
                    .collect(),
            })
            .unwrap();
            let indexed = handle_index(&body, &state);
            assert!(indexed.starts_with("HTTP/1.1 200"), "{indexed}");
            let flushed = handle_flush(br#"{"namespace":"corpus"}"#, &state);
            assert!(flushed.starts_with("HTTP/1.1 200"), "{flushed}");
        }
        // The tenant cannot see it.
        assert_eq!(
            v1(
                &state,
                "GET /v1/namespaces/corpus/stats",
                serde_json::Value::Null
            )
            .0,
            404
        );
        // What Postgres would hold across the restart.
        let manifest = state
            .controller
            .lock()
            .unwrap()
            .manifest_cloned(&bare)
            .unwrap();
        assert_eq!(documents(&manifest), 6);
        assert!(manifest.segments.len() >= 2, "{:?}", manifest.segments);
        drop(state);
        let mut control = Controller::new();
        control.save_manifest(&bare, &manifest).unwrap();
        // A tenant namespace whose id starts with the bare name: its
        // objects share the `corpus/` prefix and must not move.
        let neighbour_file = dir.join("corpus/docs/neighbour-000001/doc_store.bin");
        std::fs::create_dir_all(neighbour_file.parent().unwrap()).unwrap();
        std::fs::write(&neighbour_file, b"not ours").unwrap();

        let objects = LocalObjects::new(dir.clone());
        let wal: Vec<Arc<dyn WalStore>> = vec![Arc::new(kosha_write::wal::LocalWalStore::new(
            dir.join("_wal"),
        ))];
        let migrate_args = Args {
            from: Some(bare.clone()),
            to: Some(to.clone()),
            ..Default::default()
        };

        // A dry run changes nothing.
        let mut out = Vec::new();
        let planned = migrate(
            &mut control,
            &objects,
            Some(&wal),
            &Args {
                dry_run: true,
                ..migrate_args.clone()
            },
            &mut out,
        )
        .unwrap();
        assert!(planned.objects > 0 && planned.bytes > 0);
        assert_eq!(
            (planned.copied, planned.deleted, planned.left_behind),
            (0, 0, 1)
        );
        assert!(control.has_namespace(&bare) && !control.has_namespace(&to));
        assert!(objects.list("acme/corpus").unwrap().is_empty());
        let printed = String::from_utf8(out).unwrap();
        assert!(printed.contains("plan: copy"), "{printed}");

        // A collision is refused before anything moves.
        let mut colliding = Controller::new();
        colliding.save_manifest(&bare, &manifest).unwrap();
        colliding.create_namespace(to.clone()).unwrap();
        let refused = migrate(
            &mut colliding,
            &objects,
            Some(&wal),
            &migrate_args,
            &mut Vec::new(),
        );
        assert!(refused.unwrap_err().contains("refusing to overwrite"));

        let report = migrate(
            &mut control,
            &objects,
            Some(&wal),
            &migrate_args,
            &mut Vec::new(),
        )
        .unwrap();
        assert_eq!(report.copied, planned.objects);
        assert_eq!(report.deleted, planned.objects);
        assert_eq!(report.documents, 6);
        assert!(!control.has_namespace(&bare));
        assert!(control.has_namespace(&to));
        // The old prefix holds only the neighbour's object now.
        assert_eq!(
            objects.list("corpus").unwrap(),
            vec![("docs/neighbour-000001/doc_store.bin".to_string(), 8)]
        );
        // The lease was released: a writer can take `to` straight away.
        assert!(control
            .acquire_writer_lease(&to, "ingest", Duration::from_secs(5))
            .is_some());
        control.release_writer_lease(&to, "ingest", control.writer_lease_token(&to).unwrap());

        // Rerunning finds the work done and changes nothing.
        let again = migrate(
            &mut control,
            &objects,
            Some(&wal),
            &migrate_args,
            &mut Vec::new(),
        )
        .unwrap();
        assert!(again.resumed);
        assert_eq!((again.objects, again.copied, again.deleted), (0, 0, 0));

        // After a restart the tenant sees the same documents.
        let state = AppState::with_options(dir.clone(), options(control));
        let (code, stats) = v1(
            &state,
            "GET /v1/namespaces/corpus/stats",
            serde_json::Value::Null,
        );
        assert_eq!(code, 200, "{stats}");
        assert_eq!(stats["documents"], 6, "{stats}");
        let (code, found) = v1(
            &state,
            "POST /v1/namespaces/corpus/search",
            serde_json::json!({"query_text": "fox", "top_k": 10}),
        );
        assert_eq!(code, 200, "{found}");
        assert_eq!(found["total_hits"], 6, "{found}");
        assert!(!dir
            .join("corpus")
            .join(&manifest.segments[0].segment_id.0)
            .exists());
    }

    /// A run that died after the rename (here: right before deleting the
    /// source) is finished by the next one.
    #[test]
    fn rerun_after_the_rename_deletes_the_leftover_source() {
        let dir = crate::tests::test_dir();
        let objects = LocalObjects::new(dir.clone());
        for path in [
            "corpus/seg-000001/doc_store.bin",
            "corpus/seg-000001/_COMMIT",
        ] {
            let file = dir.join(path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"bytes").unwrap();
        }
        let mut control = Controller::new();
        control
            .save_manifest(&ns("corpus"), &manifest_with(3, &[("seg-000001", 4)]))
            .unwrap();
        let migrate_args = Args {
            from: Some(ns("corpus")),
            to: Some(ns("acme/corpus")),
            keep_source: true,
            ..Default::default()
        };
        let first = migrate(&mut control, &objects, None, &migrate_args, &mut Vec::new()).unwrap();
        assert_eq!((first.copied, first.deleted), (2, 0));
        // The `_COMMIT` marker is copied after the data file.
        assert!(dir.join("acme/corpus/seg-000001/_COMMIT").exists());

        let finish = Args {
            keep_source: false,
            ..migrate_args
        };
        let second = migrate(&mut control, &objects, None, &finish, &mut Vec::new()).unwrap();
        assert!(second.resumed);
        assert_eq!((second.objects, second.copied, second.deleted), (2, 0, 2));
        assert!(objects.list("corpus").unwrap().is_empty());
        assert_eq!(objects.list("acme/corpus").unwrap().len(), 2);
    }

    #[test]
    fn refuses_a_live_writer_an_unflushed_wal_and_branch_dependents() {
        let dir = crate::tests::test_dir();
        let objects = LocalObjects::new(dir.clone());
        let mut control = Controller::new();
        control
            .save_manifest(&ns("corpus"), &manifest_with(1, &[("seg-1", 1)]))
            .unwrap();
        let migrate_args = Args {
            from: Some(ns("corpus")),
            to: Some(ns("acme/corpus")),
            ..Default::default()
        };

        // A live writer holds the lease.
        let token = control
            .acquire_writer_lease(&ns("corpus"), "ingest-0", Duration::from_secs(60))
            .unwrap();
        let err =
            migrate(&mut control, &objects, None, &migrate_args, &mut Vec::new()).unwrap_err();
        assert!(err.contains("live writer"), "{err}");
        control.release_writer_lease(&ns("corpus"), "ingest-0", token);

        // Unflushed WAL records for the namespace.
        let wal_dir = dir.join("_wal");
        let writer = kosha_write::wal::WalWriter::new(wal_dir.clone());
        writer
            .append(
                &ns("corpus"),
                &[Document {
                    id: DocumentId("late".into()),
                    fields: vec![Field::text("text", "x")],
                }],
            )
            .unwrap();
        let wal: Vec<Arc<dyn WalStore>> =
            vec![Arc::new(kosha_write::wal::LocalWalStore::new(wal_dir))];
        let err = migrate(
            &mut control,
            &objects,
            Some(&wal),
            &migrate_args,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(err.contains("past its manifest checkpoint"), "{err}");
        // The refusal released both leases.
        assert!(control
            .acquire_writer_lease(&ns("acme/corpus"), "ingest-0", Duration::from_secs(1))
            .is_some());
        assert!(control.has_namespace(&ns("corpus")));

        // A branch that reads segments under `corpus/`.
        let mut child = manifest_with(1, &[("seg-1", 1)]);
        child.branch = Some(kosha_core::branch::BranchInfo {
            parent: ns("corpus"),
            parent_version: 1,
            segment_homes: [(SegmentId("seg-1".into()), ns("corpus"))]
                .into_iter()
                .collect(),
        });
        control.save_manifest(&ns("corpus-branch"), &child).unwrap();
        let mut fresh = Controller::new();
        fresh
            .save_manifest(&ns("corpus"), &manifest_with(1, &[("seg-1", 1)]))
            .unwrap();
        fresh.save_manifest(&ns("corpus-branch"), &child).unwrap();
        let err = migrate(&mut fresh, &objects, None, &migrate_args, &mut Vec::new()).unwrap_err();
        assert!(err.contains("corpus-branch"), "{err}");
    }

    /// A checkpointed record of the bare id that still sits in a WAL group
    /// file (shared with another namespace's unflushed record) passes the
    /// tail check, but must not replay once the bare manifest is gone.
    #[test]
    fn checkpointed_wal_records_do_not_resurrect_the_bare_namespace() {
        use crate::{AppState, StateOptions};

        let dir = crate::tests::test_dir();
        let wal_dir = dir.join("_wal");
        let doc = |id: &str| Document {
            id: DocumentId(id.into()),
            fields: vec![Field::text("text", "x")],
        };
        let writer = kosha_write::wal::WalWriter::new(wal_dir.clone());
        let lsns = writer
            .append_records(&[
                kosha_core::WalRecord::new(ns("corpus"), vec![doc("a")]),
                kosha_core::WalRecord::new(ns("other"), vec![doc("b")]),
            ])
            .unwrap();
        let mut manifest = manifest_with(2, &[]);
        manifest.wal_origin = Some(writer.origin().to_owned());
        manifest.wal_checkpoint = lsns[0];
        drop(writer);
        let store = kosha_write::wal::LocalWalStore::new(wal_dir.clone());
        assert_eq!(
            wal_tail_records(&store, &ns("corpus"), &manifest).unwrap(),
            0
        );
        let mut control = Controller::new();
        control.save_manifest(&ns("corpus"), &manifest).unwrap();

        let wal: Vec<Arc<dyn WalStore>> = vec![Arc::new(store)];
        let objects = LocalObjects::new(dir.clone());
        let migrate_args = Args {
            from: Some(ns("corpus")),
            to: Some(ns("acme/corpus")),
            ..Default::default()
        };
        let mut out = Vec::new();
        migrate(&mut control, &objects, Some(&wal), &migrate_args, &mut out).unwrap();
        let printed = String::from_utf8(out).unwrap();
        assert!(
            printed.contains("records discarded in 1 stream"),
            "{printed}"
        );

        // Recovery skips the bare id's record and still replays the other.
        let recovered = kosha_write::wal::WalWriter::recover(&wal_dir).unwrap();
        assert!(
            recovered.iter().all(|r| r.namespace != ns("corpus")),
            "{recovered:?}"
        );
        assert!(recovered.iter().any(|r| r.namespace == ns("other")));

        // A writer booting on that WAL does not bring `corpus` back.
        let state = AppState::with_options(
            dir.clone(),
            StateOptions {
                control_store: Some(Box::new(control)),
                role: Some(crate::role::Role::Standalone),
                writer_lease: Some(false),
                ..Default::default()
            },
        );
        assert!(state.indexer.pending_doc_count(&ns("other")) > 0);
        assert_eq!(state.indexer.pending_doc_count(&ns("corpus")), 0);
        assert!(!state.indexer.namespaces().contains(&ns("corpus")));
        let controller = state.controller.lock().unwrap();
        assert!(!controller.has_namespace(&ns("corpus")));
        assert!(controller.has_namespace(&ns("acme/corpus")));
    }

    /// After the rename the destination is the tenant's live namespace: a
    /// resumed run (`--keep-source`, then a plain rerun) never overwrites a
    /// destination object rewritten in place, and deletes only source
    /// objects whose destination exists.
    #[test]
    fn rerun_after_the_rename_never_overwrites_the_destination() {
        let dir = crate::tests::test_dir();
        let objects = LocalObjects::new(dir.clone());
        for (path, bytes) in [
            ("corpus/seg-000001/doc_store.bin", &b"bytes"[..]),
            ("corpus/seg-000001/footer.json", b"{\"old\":1}"),
            ("corpus/seg-000001/_COMMIT", b"commit"),
        ] {
            let file = dir.join(path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, bytes).unwrap();
        }
        let mut control = Controller::new();
        control
            .save_manifest(&ns("corpus"), &manifest_with(3, &[("seg-000001", 4)]))
            .unwrap();
        let migrate_args = Args {
            from: Some(ns("corpus")),
            to: Some(ns("acme/corpus")),
            keep_source: true,
            ..Default::default()
        };
        let first = migrate(&mut control, &objects, None, &migrate_args, &mut Vec::new()).unwrap();
        assert_eq!((first.copied, first.deleted), (3, 0));

        // The live namespace rewrites its footer in place (a different
        // size) and drops another object.
        let footer = dir.join("acme/corpus/seg-000001/footer.json");
        std::fs::write(&footer, b"{\"rewritten\":true,\"crc\":42}").unwrap();
        std::fs::remove_file(dir.join("acme/corpus/seg-000001/doc_store.bin")).unwrap();

        let finish = Args {
            keep_source: false,
            ..migrate_args
        };
        let dry = migrate(
            &mut control,
            &objects,
            None,
            &Args {
                dry_run: true,
                ..finish.clone()
            },
            &mut Vec::new(),
        )
        .unwrap();
        assert_eq!((dry.objects, dry.copied, dry.deleted), (2, 0, 0));
        let second = migrate(&mut control, &objects, None, &finish, &mut Vec::new()).unwrap();
        assert!(second.resumed);
        assert_eq!(
            (
                second.objects,
                second.copied,
                second.deleted,
                second.left_behind
            ),
            (2, 0, 2, 1)
        );
        // The rewritten footer is untouched and nothing was copied back.
        assert_eq!(
            std::fs::read(&footer).unwrap(),
            b"{\"rewritten\":true,\"crc\":42}"
        );
        assert!(!dir.join("acme/corpus/seg-000001/doc_store.bin").exists());
        // Only the source object without a destination copy is left.
        assert_eq!(
            objects.list("corpus").unwrap(),
            vec![("seg-000001/doc_store.bin".to_string(), 5)]
        );
    }

    #[test]
    fn parses_batch_and_verify_flags() {
        let parsed = args(&["--map", "/etc/map.txt", "--dry-run", "--keep-source"]).unwrap();
        assert_eq!(parsed.map, Some(PathBuf::from("/etc/map.txt")));
        assert!(parsed.is_batch() && parsed.dry_run && parsed.keep_source);
        let parsed = args(&["--all-unreachable", "--tenant=acme", "--skip-wal-check"]).unwrap();
        assert!(parsed.all_unreachable && parsed.is_batch() && parsed.skip_wal_check);
        assert_eq!(parsed.tenant.as_deref(), Some("acme"));
        let parsed = args(&["--verify", "--map=m.json"]).unwrap();
        assert!(parsed.verify && parsed.map.is_some());
        let parsed = args(&["--verify", "--from", "c", "--to", "t/c"]).unwrap();
        assert!(parsed.verify && !parsed.is_batch());
        // Verifying `--all-unreachable` needs no tenant: it checks no bare
        // id is left.
        assert!(args(&["--verify", "--all-unreachable"]).unwrap().verify);

        for bad in [
            &["--all-unreachable"][..],
            &["--all-unreachable", "--tenant", "a/b"],
            &["--all-unreachable", "--tenant", "_internal"],
            &["--tenant", "acme"],
            &["--map", "m", "--tenant", "acme"],
            &["--map", "m", "--from", "c", "--to", "t/c"],
            &["--map", "m", "--all-unreachable", "--tenant", "acme"],
            &["--map"],
            &["--verify", "--map", "m", "--dry-run"],
            &["--verify", "--map", "m", "--keep-source"],
            &["--verify", "--map", "m", "--skip-wal-check"],
            &["--verify"],
            &["--dry-run"],
        ] {
            assert!(args(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn parses_map_files_as_lines_or_json() {
        let pairs = parse_map("# rollout\n\nalpha = acme/alpha\nbeta=globex/b\n").unwrap();
        assert_eq!(
            pairs,
            vec![
                (ns("alpha"), ns("acme/alpha")),
                (ns("beta"), ns("globex/b"))
            ]
        );
        let object = parse_map(r#"{"alpha": "acme/alpha", "beta": "globex/b"}"#).unwrap();
        assert_eq!(object, pairs);
        let array = parse_map(
            r#"[{"from": "alpha", "to": "acme/alpha"}, {"from": "beta", "to": "globex/b"}]"#,
        )
        .unwrap();
        assert_eq!(array, pairs);

        for (bad, why) in [
            ("alpha acme/alpha", "is not bare=tenant/name"),
            ("alpha=acme", "must be <tenant>/<name>"),
            ("acme/x=acme/y", "must be a bare"),
            ("alpha=acme/a\nalpha=acme/b", "mapped twice"),
            ("alpha=acme/a\nbeta=acme/a", "two namespaces map to"),
            ("# nothing\n", "no bare=tenant/name pairs"),
            (r#"{"alpha": 3}"#, "not a string"),
            (r#"[{"from": "alpha"}]"#, "missing field"),
        ] {
            let err = parse_map(bad).unwrap_err();
            assert!(err.contains(why), "{bad:?}: {err}");
        }
    }

    /// A bare namespace with one committed segment on local disk.
    fn bare_with_segment(dir: &Path, control: &mut Controller, bare: &str, docs: u32) {
        for path in ["seg-000001/doc_store.bin", "seg-000001/_COMMIT"] {
            let file = dir.join(bare).join(path);
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"bytes").unwrap();
        }
        control
            .save_manifest(&ns(bare), &manifest_with(1, &[("seg-000001", docs)]))
            .unwrap();
    }

    fn printed(out: Vec<u8>) -> String {
        String::from_utf8(out).unwrap()
    }

    /// A dry run over the whole batch changes nothing: no lease, no copy,
    /// no rename.
    #[test]
    fn batch_dry_run_changes_nothing() {
        let dir = crate::tests::test_dir();
        let objects = LocalObjects::new(dir.clone());
        let mut control = Controller::new();
        bare_with_segment(&dir, &mut control, "alpha", 3);
        bare_with_segment(&dir, &mut control, "beta", 5);
        control.create_namespace(ns("acme/docs")).unwrap();
        let batch = args(&[
            "--all-unreachable",
            "--tenant",
            "acme",
            "--dry-run",
            "--skip-wal-check",
        ])
        .unwrap();

        let mut out = Vec::new();
        execute(&mut control, &objects, None, &batch, &mut out).unwrap();
        let text = printed(out);
        assert!(text.contains("[dry run, nothing changed]"), "{text}");
        assert!(
            text.contains("0 moved, 2 would move, 0 skipped, 0 refused"),
            "{text}"
        );
        assert!(text.contains("alpha  acme/alpha  would move  3"), "{text}");

        assert!(control.has_namespace(&ns("alpha")) && control.has_namespace(&ns("beta")));
        assert!(!control.has_namespace(&ns("acme/alpha")));
        assert!(!control.has_namespace(&ns("acme/beta")));
        assert!(objects.list("acme/alpha").unwrap().is_empty());
        assert!(objects.list("acme/beta").unwrap().is_empty());
        assert_eq!(objects.list("alpha").unwrap().len(), 2);
        // No lease was taken.
        assert!(control
            .acquire_writer_lease(&ns("alpha"), "ingest", Duration::from_secs(1))
            .is_some());
    }

    /// A batch from a map file over the local storage path: one namespace
    /// is refused (a live writer holds its lease), the other moves, the
    /// batch reports both and fails; `--verify` then flags the one left.
    /// Once the writer is gone, rerunning the batch skips the finished one
    /// and moves the other, and verify passes.
    #[test]
    fn batch_moves_one_refuses_the_other_and_verify_reports_it() {
        let dir = crate::tests::test_dir();
        let objects = LocalObjects::new(dir.clone());
        let mut control = Controller::new();
        bare_with_segment(&dir, &mut control, "alpha", 3);
        bare_with_segment(&dir, &mut control, "beta", 5);
        let map = dir.join("rollout.map");
        std::fs::write(&map, "alpha=acme/alpha\nbeta=globex/beta\n").unwrap();
        let wal_dir = dir.join("_wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let wal: Vec<Arc<dyn WalStore>> =
            vec![Arc::new(kosha_write::wal::LocalWalStore::new(wal_dir))];
        let batch = Args {
            map: Some(map.clone()),
            ..Default::default()
        };
        let verify_args = Args {
            map: Some(map),
            verify: true,
            ..Default::default()
        };

        let token = control
            .acquire_writer_lease(&ns("alpha"), "ingest-0", Duration::from_secs(60))
            .unwrap();
        let mut out = Vec::new();
        let err = execute(&mut control, &objects, Some(&wal), &batch, &mut out).unwrap_err();
        assert!(err.contains("1 of 2 namespace(s) refused"), "{err}");
        let text = printed(out);
        assert!(
            text.contains("1 moved, 0 would move, 0 skipped, 1 refused"),
            "{text}"
        );
        assert!(
            text.contains("refused: a live writer holds the writer lease on alpha"),
            "{text}"
        );
        let refused_row = text
            .lines()
            .find(|l| l.starts_with("alpha ") && l.contains("refused"))
            .unwrap_or_else(|| panic!("{text}"));
        assert!(refused_row.contains("live writer"), "{refused_row}");
        assert!(text
            .lines()
            .any(|l| l.starts_with("beta ") && l.contains(" moved ")));
        // alpha untouched, beta moved.
        assert!(control.has_namespace(&ns("alpha")));
        assert!(!control.has_namespace(&ns("acme/alpha")));
        assert!(objects.list("acme/alpha").unwrap().is_empty());
        assert!(!control.has_namespace(&ns("beta")));
        assert!(control.has_namespace(&ns("globex/beta")));
        assert!(objects.list("beta").unwrap().is_empty());
        assert_eq!(objects.list("globex/beta").unwrap().len(), 2);

        // Verify flags alpha, passes beta, and lists alpha as unreachable.
        let mut out = Vec::new();
        let err = execute(&mut control, &objects, None, &verify_args, &mut out).unwrap_err();
        assert!(err.contains("1 of 2 namespace(s) failed, 1 bare"), "{err}");
        let text = printed(out);
        assert!(
            text.contains(
                "alpha  acme/alpha   FAILED  0     0         alpha is still in the control plane"
            ),
            "{text}"
        );
        assert!(
            text.contains("beta   globex/beta  ok      5     1"),
            "{text}"
        );
        assert!(
            text.contains("unreachable bare namespaces: 1 (alpha)"),
            "{text}"
        );

        // The writer stops; the rerun skips beta and moves alpha.
        control.release_writer_lease(&ns("alpha"), "ingest-0", token);
        let mut out = Vec::new();
        execute(&mut control, &objects, Some(&wal), &batch, &mut out).unwrap();
        let text = printed(out);
        assert!(
            text.contains("1 moved, 0 would move, 1 skipped, 0 refused"),
            "{text}"
        );
        assert!(text
            .lines()
            .any(|l| l.starts_with("beta ") && l.contains("already migrated")));

        let mut out = Vec::new();
        execute(&mut control, &objects, None, &verify_args, &mut out).unwrap();
        let text = printed(out);
        assert!(
            text.contains("alpha  acme/alpha   ok      3     1"),
            "{text}"
        );
        assert!(text.contains("unreachable bare namespaces: none"), "{text}");
        assert!(text.contains("verify: OK"), "{text}");
    }

    /// An unflushed WAL tail refuses that namespace only; verify fails on a
    /// destination whose segment objects are gone and notes kept sources.
    #[test]
    fn batch_refuses_a_wal_tail_and_verify_checks_segments() {
        let dir = crate::tests::test_dir();
        let objects = LocalObjects::new(dir.clone());
        let mut control = Controller::new();
        bare_with_segment(&dir, &mut control, "alpha", 3);
        bare_with_segment(&dir, &mut control, "beta", 5);
        let wal_dir = dir.join("_wal");
        kosha_write::wal::WalWriter::new(wal_dir.clone())
            .append(
                &ns("alpha"),
                &[Document {
                    id: DocumentId("late".into()),
                    fields: vec![Field::text("text", "x")],
                }],
            )
            .unwrap();
        let wal: Vec<Arc<dyn WalStore>> =
            vec![Arc::new(kosha_write::wal::LocalWalStore::new(wal_dir))];
        let batch = args(&["--all-unreachable", "--tenant", "acme", "--keep-source"]).unwrap();
        let mut out = Vec::new();
        let err = execute(&mut control, &objects, Some(&wal), &batch, &mut out).unwrap_err();
        assert!(err.contains("1 of 2"), "{err}");
        let text = printed(out);
        assert!(text.contains("past its manifest checkpoint"), "{text}");
        assert!(control.has_namespace(&ns("acme/beta")));
        assert!(control.has_namespace(&ns("alpha")));

        // beta's sources were kept: verify passes it with a note.
        let check = args(&["--verify", "--from", "beta", "--to", "acme/beta"]).unwrap();
        let mut out = Vec::new();
        execute(&mut control, &objects, None, &check, &mut out).unwrap_err();
        let text = printed(out);
        assert!(
            text.contains("2 source objects still under beta/"),
            "{text}"
        );
        assert!(text.contains("acme/beta  ok"), "{text}");

        // A destination missing its segment objects fails.
        std::fs::remove_dir_all(dir.join("acme/beta/seg-000001")).unwrap();
        let (rows, bare) = verify(
            &control,
            &objects,
            &[(ns("beta"), ns("acme/beta"))],
            &mut Vec::new(),
        );
        assert_eq!(bare, vec![ns("alpha")]);
        assert!(
            rows[0]
                .problem
                .as_deref()
                .is_some_and(|p| p.contains("1 of 1 segments have no objects under acme/beta/")),
            "{rows:?}"
        );
    }
}
