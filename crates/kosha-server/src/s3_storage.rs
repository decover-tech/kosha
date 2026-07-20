//! S3 storage backend — optional, gated behind `s3` feature.
//!
//! Uses `aws-sdk-s3` for multi-part upload of large segments.
//! Kept separate from core so Kosha remains S3-free for open-source.

use std::fmt::{self, Debug, Formatter};
use std::path::{Path, PathBuf};

use kosha_core::{KoshaError, LocalStorage, StorageBackend};

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
            .finish()
    }
}

impl S3Storage {
    /// Create a new S3 storage backend.
    /// Reads AWS credentials from the environment (standard AWS SDK chain).
    pub async fn new(local_root: PathBuf, bucket: String, prefix: String) -> Result<Self, KoshaError> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = aws_sdk_s3::Client::new(&config);
        let local = LocalStorage::new(local_root);
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            KoshaError::NotFound(format!("failed to create tokio runtime: {e}"))
        })?;

        Ok(Self { local, bucket, prefix, rt, client })
    }

    fn s3_key(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), path)
        }
    }

    /// Upload a local file to S3. Uses multi-part upload for files > 5MB.
    fn upload_to_s3(&self, local_path: &Path, s3_key: &str) -> Result<(), KoshaError> {
        let data = std::fs::read(local_path)
            .map_err(KoshaError::Io)?;
        let size = data.len();

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = s3_key.to_string();

        self.rt.block_on(async move {
            if size > 5 * 1024 * 1024 {
                // Multi-part upload for large files
                let mpu = client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| KoshaError::NotFound(format!("S3 create_multipart_upload: {e}")))?;

                let upload_id = mpu.upload_id().unwrap_or_default();
                let mut parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();
                let part_size = 5 * 1024 * 1024; // 5MB parts
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
                        .body(aws_sdk_s3::primitives::ByteStream::from(Vec::from(part_data)))
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
                // Single PUT for small files
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

            let bytes = resp.body.collect().await
                .map_err(|e| KoshaError::NotFound(format!("S3 read body: {e}")))?
                .into_bytes();
            Ok::<Vec<u8>, KoshaError>(bytes.to_vec())
        })?;

        // Write to local cache
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(local_path, &data).ok();

        Ok(data)
    }
}

impl StorageBackend for S3Storage {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError> {
        let local_path = Path::new(&self.local.root).join(path);
        // Check local cache first
        if local_path.exists() {
            return std::fs::read(&local_path).map_err(KoshaError::Io);
        }
        // Download from S3
        let s3_key = self.s3_key(path);
        self.download_from_s3(&s3_key, &local_path)
    }

    fn write(&self, path: &str, data: &[u8]) -> Result<(), KoshaError> {
        // Write to local cache
        self.local.write(path, data)?;
        // Upload to S3
        let local_path = Path::new(&self.local.root).join(path);
        let s3_key = self.s3_key(path);
        self.upload_to_s3(&local_path, &s3_key)
    }

    fn exists(&self, path: &str) -> bool {
        self.local.exists(path)
    }

    fn delete(&self, path: &str) -> Result<(), KoshaError> {
        self.local.delete(path)?;
        // Also delete from S3
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
        self.local.list(path)
    }

    fn create_dir_all(&self, path: &str) -> Result<(), KoshaError> {
        self.local.create_dir_all(path)
    }
}
