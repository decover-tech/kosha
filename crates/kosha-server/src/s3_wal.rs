//! S3 backend for the object-store WAL (`KOSHA_WAL_MODE=s3`, #236).
//!
//! Implements [`WalObjectBackend`] with `aws-sdk-s3`: group commits are
//! created with `PutObject` + `If-None-Match: *` (HTTP 412, or 409 for a
//! concurrent conditional write, means another writer owns the key). Every
//! call has a deadline (`KOSHA_WAL_S3_TIMEOUT_MS`, default 10000): a WAL
//! put sits on the acknowledgement path, and an S3 call without a timeout
//! would hold every queued write. A timed-out put has an unknown outcome;
//! the WAL writer resolves it on its next put of the same key.
//!
//! Keys are `{KOSHA_S3_PREFIX}/{key}`, the same prefix as segments, e.g.
//! `{prefix}/_wal/{writer_id}/{lsn:020}.bin`.
//!
//! Every put carries the segment store's SSE headers (#343). A WAL stream
//! belongs to one ingest writer, not one tenant (a group commit holds
//! records of every tenant that wrote in that window), so WAL objects use
//! the default key, never a per-tenant one.
use std::fmt;
use std::future::Future;
use std::time::Duration;

use kosha_core::KoshaError;
use kosha_write::object_wal::WalObjectBackend;

use crate::s3_storage::{join_s3_key, log_s3_error, ApplySse, ObjectSse, S3Storage};

pub struct S3WalBackend {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    timeout: Duration,
    rt: tokio::runtime::Runtime,
    /// SSE headers for every put: the default key, see the module docs.
    sse: Option<ObjectSse>,
}

impl fmt::Debug for S3WalBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3WalBackend")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("timeout", &self.timeout)
            .field("sse", &self.sse.as_ref().map(|s| s.algorithm.as_str()))
            .finish()
    }
}

/// `KOSHA_WAL_S3_TIMEOUT_MS`, default 10 s.
pub fn timeout_from_env() -> Duration {
    std::env::var("KOSHA_WAL_S3_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(10))
}

impl S3WalBackend {
    /// Shares the segment store's client, bucket and prefix.
    pub fn new(s3: &S3Storage, timeout: Duration) -> Result<Self, KoshaError> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("kosha-wal-s3")
            .enable_all()
            .build()?;
        Ok(Self {
            client: s3.client().clone(),
            bucket: s3.bucket().to_owned(),
            prefix: s3.prefix().to_owned(),
            timeout,
            rt,
            sse: s3.sse().for_tenant(None),
        })
    }

    fn key(&self, key: &str) -> String {
        join_s3_key(&self.prefix, key)
    }

    /// Run one S3 call with the WAL deadline.
    fn call<T>(
        &self,
        what: &str,
        future: impl Future<Output = Result<T, KoshaError>>,
    ) -> Result<T, KoshaError> {
        let timeout = self.timeout;
        self.rt
            .block_on(async move { tokio::time::timeout(timeout, future).await })
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("S3 WAL {what} timed out after {timeout:?}"),
                )
                .into()
            })
            .and_then(|result| result)
    }

    fn put_object(&self, key: &str, bytes: &[u8], conditional: bool) -> Result<(), KoshaError> {
        let full = self.key(key);
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&full)
            .apply_sse(self.sse.as_ref())
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes.to_vec()));
        let request = if conditional {
            request.if_none_match("*")
        } else {
            request
        };
        self.call("put", async move {
            match request.send().await {
                Ok(_) => Ok(()),
                Err(error) => {
                    let status = error.raw_response().map(|r| r.status().as_u16());
                    let kind = match status {
                        Some(412 | 409) if conditional => std::io::ErrorKind::AlreadyExists,
                        _ => std::io::ErrorKind::Other,
                    };
                    // The chain (role ARN, bucket, request ids) and the key
                    // go to the pod log only: this message can reach a
                    // tenant's response body (`indexing error: …`). A
                    // conditional put losing to an existing object is the
                    // expected answer (callers treat it as success: the
                    // startup probe, `create_meta`, migration replays), so
                    // it is not logged.
                    let short = if kind == std::io::ErrorKind::AlreadyExists {
                        error.to_string()
                    } else {
                        log_s3_error("put_object", &full, &error)
                    };
                    Err(std::io::Error::new(
                        kind,
                        format!("S3 put_object (status {status:?}): {short}"),
                    )
                    .into())
                }
            }
        })
    }
}

fn s3_error(what: String) -> KoshaError {
    std::io::Error::other(what).into()
}

impl WalObjectBackend for S3WalBackend {
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<(), KoshaError> {
        self.put_object(key, bytes, true)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), KoshaError> {
        self.put_object(key, bytes, false)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KoshaError> {
        let full = self.key(key);
        let request = self.client.get_object().bucket(&self.bucket).key(&full);
        self.call("get", async move {
            let response = match request.send().await {
                Ok(response) => response,
                Err(error) => {
                    if error.as_service_error().is_some_and(|e| e.is_no_such_key())
                        || error.raw_response().map(|r| r.status().as_u16()) == Some(404)
                    {
                        return Ok(None);
                    }
                    return Err(s3_error(format!(
                        "S3 get_object: {}",
                        log_s3_error("get_object", &full, &error)
                    )));
                }
            };
            let bytes = response
                .body
                .collect()
                .await
                .map_err(|e| {
                    s3_error(format!(
                        "S3 read body: {}",
                        log_s3_error("read body", &full, &e)
                    ))
                })?
                .into_bytes();
            Ok(Some(bytes.to_vec()))
        })
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, KoshaError> {
        let full = self.key(prefix);
        let strip = join_s3_key(&self.prefix, "");
        let strip = if strip.is_empty() {
            String::new()
        } else {
            format!("{}/", strip.trim_end_matches('/'))
        };
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.call("list", async move {
            let mut keys = Vec::new();
            let mut pages = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&full)
                .into_paginator()
                .send();
            while let Some(page) = pages.next().await {
                let page = page.map_err(|e| {
                    s3_error(format!(
                        "S3 list_objects_v2: {}",
                        log_s3_error("list_objects_v2", &full, &e)
                    ))
                })?;
                for object in page.contents() {
                    if let Some(key) = object.key() {
                        keys.push(key.strip_prefix(&strip).unwrap_or(key).to_owned());
                    }
                }
            }
            Ok(keys)
        })
    }

    fn delete(&self, keys: &[String]) -> Result<(), KoshaError> {
        let keys: Vec<String> = keys.iter().map(|k| self.key(k)).collect();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        self.call("delete", async move {
            let mut tasks = tokio::task::JoinSet::new();
            for key in keys {
                let client = client.clone();
                let bucket = bucket.clone();
                tasks.spawn(async move {
                    client
                        .delete_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            s3_error(format!(
                                "S3 delete_object: {}",
                                log_s3_error("delete_object", &key, &e)
                            ))
                        })
                });
            }
            let mut first_error = None;
            while let Some(result) = tasks.join_next().await {
                let result = result.map_err(|e| s3_error(format!("S3 delete task: {e}")))?;
                if let Err(e) = result {
                    first_error.get_or_insert(e);
                }
            }
            first_error.map_or(Ok(()), Err)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3_storage::{capture, SseConfig};

    const DEFAULT_KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/default";

    fn backend(sse: SseConfig, tag: &str) -> (S3WalBackend, capture::CaptureRequests) {
        let root = std::env::temp_dir().join(format!("kosha-wal-sse-{tag}-{}", std::process::id()));
        let (storage, capture) = capture::storage(root, "kosha", sse);
        let backend = S3WalBackend::new(&storage, Duration::from_secs(10)).unwrap();
        (backend, capture)
    }

    #[test]
    fn wal_puts_use_the_default_key_never_a_tenant_key() {
        let sse = SseConfig::from_lookup(|name| match name {
            "KOSHA_S3_SSE" => Some("aws:kms".into()),
            "KOSHA_S3_SSE_KMS_KEY_ID" => Some(DEFAULT_KEY.into()),
            "KOSHA_S3_TENANT_KMS_KEYS" => Some("_wal=alias/wal,acme=alias/acme".into()),
            _ => None,
        })
        .unwrap();
        let (wal, capture) = backend(sse, "kms");
        assert!(wal
            .put_if_absent("_wal/ingest-0/00000000000000000001.bin", b"group")
            .is_err());
        assert!(wal.put("_wal/ingest-0/meta/sequence", b"1").is_err());
        let requests = capture.take();
        assert_eq!(requests.len(), 2, "{requests:?}");
        // The conditional put keeps its precondition alongside SSE.
        assert_eq!(requests[0].header("if-none-match"), Some("*"));
        assert_eq!(requests[1].header("if-none-match"), None);
        for request in &requests {
            assert!(
                request.uri.contains("/bucket/kosha/_wal/ingest-0/"),
                "{}",
                request.uri
            );
            assert_eq!(
                request.header("x-amz-server-side-encryption"),
                Some("aws:kms")
            );
            assert_eq!(
                request.header("x-amz-server-side-encryption-aws-kms-key-id"),
                Some(DEFAULT_KEY)
            );
        }
    }

    #[test]
    fn wal_puts_without_sse_send_no_encryption_headers() {
        let (wal, capture) = backend(SseConfig::default(), "none");
        assert!(wal.put_if_absent("_wal/ingest-0/a.bin", b"group").is_err());
        let requests = capture.take();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].sse_headers().is_empty(), "{:?}", requests[0]);
    }
}
