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
use std::fmt;
use std::future::Future;
use std::time::Duration;

use kosha_core::KoshaError;
use kosha_write::object_wal::WalObjectBackend;

use crate::s3_storage::{join_s3_key, S3Storage};

pub struct S3WalBackend {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    timeout: Duration,
    rt: tokio::runtime::Runtime,
}

impl fmt::Debug for S3WalBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3WalBackend")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("timeout", &self.timeout)
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
                    Err(std::io::Error::new(
                        kind,
                        format!(
                            "S3 put_object {full} (status {status:?}): {}",
                            aws_sdk_s3::error::DisplayErrorContext(&error)
                        ),
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
                        "S3 get_object {full}: {}",
                        aws_sdk_s3::error::DisplayErrorContext(&error)
                    )));
                }
            };
            let bytes = response
                .body
                .collect()
                .await
                .map_err(|e| s3_error(format!("S3 read body {full}: {e}")))?
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
                        "S3 list_objects_v2 {full}: {}",
                        aws_sdk_s3::error::DisplayErrorContext(&e)
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
                                "S3 delete_object {key}: {}",
                                aws_sdk_s3::error::DisplayErrorContext(&e)
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
