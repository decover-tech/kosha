//! Thin HTTP client for Kosha's v1 (and a few legacy) routes.

use kosha_core::{Document, IndexRequest, IndexResponse, NamespaceId, SearchQuery, SearchResult};
use reqwest::blocking::Client as HttpClient;
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::config::Connection;

#[derive(Debug)]
pub struct ClientError(pub String);

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A non-2xx response, kept apart from transport errors so callers can
/// explain a status (an admin 403, a Postgres-only route) in plain words.
#[derive(Debug)]
pub struct HttpError {
    pub method: Method,
    pub path: String,
    pub status: u16,
    /// The server's `{"error": "..."}` message, or the start of the body
    /// when it is not that shape.
    pub message: String,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} \u{2192} {}: {}",
            self.method, self.path, self.status, self.message
        )
    }
}

impl std::error::Error for HttpError {}

/// Every failure a request can end in.
#[derive(Debug)]
pub enum RequestError {
    Http(HttpError),
    Other(ClientError),
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestError::Http(e) => e.fmt(f),
            RequestError::Other(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for RequestError {}

impl From<ClientError> for RequestError {
    fn from(e: ClientError) -> Self {
        RequestError::Other(e)
    }
}

/// Pull the `error` field out of a JSON error body; fall back to the first
/// 500 characters of the body as text.
fn error_message(text: &str) -> String {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string))
        .unwrap_or_else(|| text.chars().take(500).collect())
}

impl std::error::Error for ClientError {}

pub struct Client {
    http: HttpClient,
    host: String,
    api_key: Option<String>,
}

impl Client {
    pub fn new(conn: &Connection) -> Result<Self, ClientError> {
        Self::with_timeout(conn, std::time::Duration::from_secs(60))
    }

    /// Like [`Client::new`] with a different per-request timeout, for
    /// admin calls that can legitimately run for minutes (`import`).
    pub fn with_timeout(
        conn: &Connection,
        timeout: std::time::Duration,
    ) -> Result<Self, ClientError> {
        let http = HttpClient::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| ClientError(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            http,
            host: conn.host.clone(),
            api_key: conn.api_key.clone(),
        })
    }

    pub fn health(&self) -> Result<Value, RequestError> {
        self.request(Method::GET, "/v1/healthz", None)
    }

    pub fn stats(&self) -> Result<Value, RequestError> {
        self.request(Method::GET, "/v1/stats", None)
    }

    pub fn namespace_stats(&self, namespace: &str) -> Result<Value, RequestError> {
        self.request(
            Method::GET,
            &format!("/v1/namespaces/{}/stats", encode_ns(namespace)),
            None,
        )
    }

    pub fn index_documents(
        &self,
        namespace: &str,
        documents: Vec<Document>,
    ) -> Result<IndexResponse, RequestError> {
        let body = IndexRequest {
            namespace: NamespaceId(namespace.to_string()),
            documents,
        };
        self.request(
            Method::POST,
            &format!("/v1/namespaces/{}/documents", encode_ns(namespace)),
            Some(serde_json::to_value(body).map_err(|e| ClientError(e.to_string()))?),
        )
    }

    pub fn search(
        &self,
        namespace: &str,
        query: SearchQuery,
    ) -> Result<SearchResult, RequestError> {
        let mut body = serde_json::to_value(&query).map_err(|e| ClientError(e.to_string()))?;
        if let Some(obj) = body.as_object_mut() {
            obj.insert("namespace".into(), Value::String(namespace.to_string()));
        }
        self.request(
            Method::POST,
            &format!("/v1/namespaces/{}/search", encode_ns(namespace)),
            Some(body),
        )
    }

    pub fn search_body(&self, namespace: &str, mut body: Value) -> Result<Value, RequestError> {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("namespace".into(), Value::String(namespace.to_string()));
        }
        self.request(
            Method::POST,
            &format!("/v1/namespaces/{}/search", encode_ns(namespace)),
            Some(body),
        )
    }

    pub fn flush(&self, namespace: Option<&str>) -> Result<Value, RequestError> {
        match namespace {
            Some(ns) => self.request(
                Method::POST,
                &format!("/v1/namespaces/{}/flush", encode_ns(ns)),
                Some(serde_json::json!({})),
            ),
            // Legacy route supports flush-all when namespace is omitted.
            None => self.request(Method::POST, "/flush", Some(serde_json::json!({}))),
        }
    }

    pub fn delete(&self, namespace: &str, filter: Value) -> Result<Value, RequestError> {
        self.request(
            Method::POST,
            &format!("/v1/namespaces/{}/delete", encode_ns(namespace)),
            Some(serde_json::json!({"filter": filter})),
        )
    }

    pub fn rebuild_filter_blooms(&self, namespace: &str) -> Result<Value, RequestError> {
        self.request(
            Method::POST,
            "/v1/admin/rebuild-filter-blooms",
            Some(serde_json::json!({"namespace": namespace})),
        )
    }

    pub fn backfill_offset_tables(&self, namespace: &str) -> Result<Value, RequestError> {
        self.request(
            Method::POST,
            "/v1/admin/backfill-offset-tables",
            Some(serde_json::json!({"namespace": namespace})),
        )
    }

    /// `POST /v1/namespaces/{ns}/exists` with `{"ids": [...]}`; answers
    /// `{"namespace", "existing": [...], "missing": [...]}`.
    pub fn exists(&self, namespace: &str, ids: &[String]) -> Result<Value, RequestError> {
        self.request(
            Method::POST,
            &format!("/v1/namespaces/{}/exists", encode_ns(namespace)),
            Some(serde_json::json!({ "ids": ids })),
        )
    }

    /// `POST /v1/admin/compact-namespace`. `body` carries `namespace`,
    /// `mode`, `max_segment_bytes` and `async`.
    pub fn compact_namespace(&self, body: Value) -> Result<Value, RequestError> {
        self.request(Method::POST, "/v1/admin/compact-namespace", Some(body))
    }

    /// `GET /v1/admin/compaction-status`.
    pub fn compaction_status(&self) -> Result<Value, RequestError> {
        self.request(Method::GET, "/v1/admin/compaction-status", None)
    }

    /// `GET /v1/admin/compaction-jobs/{job_id}`.
    pub fn compaction_job(&self, job_id: &str) -> Result<Value, RequestError> {
        self.request(
            Method::GET,
            &format!("/v1/admin/compaction-jobs/{}", urlencoding::encode(job_id)),
            None,
        )
    }

    /// `POST /v1/admin/import-namespace`: rebuild a namespace's manifest
    /// from the segment footers under its S3 prefix.
    pub fn import_namespace(
        &self,
        namespace: &str,
        overwrite: bool,
    ) -> Result<Value, RequestError> {
        self.request(
            Method::POST,
            "/v1/admin/import-namespace",
            Some(serde_json::json!({"namespace": namespace, "overwrite": overwrite})),
        )
    }

    /// `POST /v1/admin/api-keys`. `body` names the tenant and, optionally,
    /// the key's scopes, namespaces and expiry.
    pub fn create_api_key(&self, body: Value) -> Result<Value, RequestError> {
        self.request(Method::POST, "/v1/admin/api-keys", Some(body))
    }

    /// `GET /v1/admin/api-keys`: keys that are not revoked, without secrets.
    pub fn list_api_keys(&self, tenant_id: Option<&str>) -> Result<Value, RequestError> {
        let path = match tenant_id {
            Some(tenant) => format!(
                "/v1/admin/api-keys?tenant_id={}",
                urlencoding::encode(tenant)
            ),
            None => "/v1/admin/api-keys".to_string(),
        };
        self.request(Method::GET, &path, None)
    }

    /// `POST /v1/admin/api-keys/revoke`.
    pub fn revoke_api_key(&self, key_id: &str) -> Result<Value, RequestError> {
        self.request(
            Method::POST,
            "/v1/admin/api-keys/revoke",
            Some(serde_json::json!({ "key_id": key_id })),
        )
    }

    /// `POST /v1/admin/api-keys/rotate`. `body` names the key and,
    /// optionally, the overlap window and the new key's expiry.
    pub fn rotate_api_key(&self, body: Value) -> Result<Value, RequestError> {
        self.request(Method::POST, "/v1/admin/api-keys/rotate", Some(body))
    }

    /// `POST /v1/admin/api-keys/scrub-plaintext`.
    pub fn scrub_plaintext_api_keys(&self) -> Result<Value, RequestError> {
        self.request(Method::POST, "/v1/admin/api-keys/scrub-plaintext", None)
    }

    pub fn curl(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, RequestError> {
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        };
        self.request(method, &path, body)
    }

    fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<T, RequestError> {
        let url = format!("{}{}", self.host, path);
        let mut req = self.http.request(method.clone(), &url);
        req = req.header("content-type", "application/json");
        if let Some(key) = &self.api_key {
            req = req.header("Authorization", format!("Bearer {key}"));
            req = req.header("X-Api-Key", key);
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        let response = req
            .send()
            .map_err(|e| ClientError(format!("{method} {path} failed: {e}")))?;
        let status = response.status();
        let text = response
            .text()
            .map_err(|e| ClientError(format!("failed to read response body: {e}")))?;
        if !status.is_success() {
            return Err(RequestError::Http(HttpError {
                method,
                path: path.to_string(),
                status: status.as_u16(),
                message: error_message(&text),
            }));
        }
        if text.trim().is_empty() {
            // Some endpoints might return empty; normalize to null JSON.
            return serde_json::from_str("null")
                .map_err(|e| ClientError(format!("empty response parse error: {e}")).into());
        }
        serde_json::from_str(&text).map_err(|e| {
            ClientError(format!(
                "invalid JSON from {method} {path}: {e}; body={}",
                text.chars().take(200).collect::<String>()
            ))
            .into()
        })
    }
}

fn encode_ns(namespace: &str) -> String {
    urlencoding::encode(namespace).into_owned()
}

/// Parse a JSON document into a kosha-core `Document`.
///
/// Accepts either:
/// - native: `{"id":"…","fields":[…]}`
/// - shorthand: `{"id":"…","title":"…","count":3}` (strings→Text, ints→Integer,
///   floats→Float, bools→Boolean; other values JSON-encoded as Text)
pub fn parse_document(value: Value) -> Result<Document, ClientError> {
    if value.get("fields").is_some() {
        return serde_json::from_value(value)
            .map_err(|e| ClientError(format!("invalid document: {e}")));
    }
    let obj = value
        .as_object()
        .ok_or_else(|| ClientError("document must be a JSON object".into()))?;
    let id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ClientError("document missing string \"id\"".into()))?
        .to_string();
    let mut fields = Vec::new();
    for (name, val) in obj {
        if name == "id" {
            continue;
        }
        fields.push(shorthand_field(name, val)?);
    }
    Ok(Document {
        id: kosha_core::DocumentId(id),
        fields,
    })
}

fn shorthand_field(name: &str, value: &Value) -> Result<kosha_core::Field, ClientError> {
    use kosha_core::{Field, FieldType};
    match value {
        Value::String(s) => Ok(Field::text(name, s.clone())),
        Value::Number(n) if n.is_i64() => Ok(Field::integer(name, n.as_i64().unwrap())),
        Value::Number(n) => Ok(Field {
            name: name.into(),
            field_type: FieldType::Float,
            value: n.to_string(),
        }),
        Value::Bool(b) => Ok(Field {
            name: name.into(),
            field_type: FieldType::Boolean,
            value: b.to_string(),
        }),
        Value::Null => Err(ClientError(format!(
            "field {name:?} is null; omit it or use a concrete value"
        ))),
        other => Ok(Field::text(
            name,
            serde_json::to_string(other).map_err(|e| ClientError(e.to_string()))?,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::FieldType;

    #[test]
    fn error_message_prefers_the_servers_error_field() {
        assert_eq!(
            error_message(r#"{"error":"admin credential required"}"#),
            "admin credential required"
        );
        assert_eq!(error_message("plain text"), "plain text");
        let long = "x".repeat(600);
        assert_eq!(error_message(&long).len(), 500);
    }

    #[test]
    fn parses_native_document() {
        let doc = parse_document(serde_json::json!({
            "id": "d1",
            "fields": [{"name": "title", "field_type": "Text", "value": "hi"}]
        }))
        .unwrap();
        assert_eq!(doc.id.0, "d1");
        assert_eq!(doc.fields[0].field_type, FieldType::Text);
    }

    #[test]
    fn parses_shorthand_document() {
        let doc = parse_document(serde_json::json!({
            "id": "d1",
            "title": "hello",
            "count": 3,
            "score": 1.5,
            "ok": true
        }))
        .unwrap();
        assert_eq!(doc.fields.len(), 4);
        let types: Vec<_> = doc.fields.iter().map(|f| f.field_type).collect();
        assert!(types.contains(&FieldType::Text));
        assert!(types.contains(&FieldType::Integer));
        assert!(types.contains(&FieldType::Float));
        assert!(types.contains(&FieldType::Boolean));
    }
}
