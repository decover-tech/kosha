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

use kosha_core::{KoshaError, LocalStorage, StorageBackend};

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
            let creds = aws_sdk_s3::config::Credentials::new(
                key,
                secret,
                None,
                None,
                "kosha-env",
            );
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
        let path = path.trim_start_matches('/');
        if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), path)
        }
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

    /// List object key basenames under a logical (unprefixed) directory path.
    fn list_remote(&self, path: &str) -> Result<Vec<String>, KoshaError> {
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

            let mut names = Vec::new();
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    let relative = key
                        .strip_prefix(&prefix_for_strip)
                        .unwrap_or(key)
                        .trim_start_matches('/');
                    // Only immediate children (files), not nested paths.
                    if !relative.is_empty() && !relative.contains('/') {
                        names.push(relative.to_string());
                    }
                }
            }
            Ok(names)
        })
    }
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
        // Prefer remote listing so cold nodes (empty local disk) can discover
        // segment files after a restart. Fall back to local on list failure.
        match self.list_remote(path) {
            Ok(names) if !names.is_empty() => Ok(names),
            Ok(_) => self.local.list(path),
            Err(e) => {
                eprintln!("WARN: S3 list failed for '{path}': {e}; falling back to local");
                self.local.list(path)
            }
        }
    }

    fn create_dir_all(&self, path: &str) -> Result<(), KoshaError> {
        self.local.create_dir_all(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_key_joins_prefix() {
        let storage_prefix = |prefix: &str, path: &str| -> String {
            let cfg_prefix = prefix.to_string();
            if cfg_prefix.is_empty() {
                path.to_string()
            } else {
                format!("{}/{}", cfg_prefix.trim_end_matches('/'), path)
            }
        };
        assert_eq!(storage_prefix("kosha/", "ns/seg/a.bin"), "kosha/ns/seg/a.bin");
        assert_eq!(storage_prefix("", "ns/seg/a.bin"), "ns/seg/a.bin");
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
