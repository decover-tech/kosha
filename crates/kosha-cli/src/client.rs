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

impl std::error::Error for ClientError {}

pub struct Client {
    http: HttpClient,
    host: String,
    api_key: Option<String>,
}

impl Client {
    pub fn new(conn: &Connection) -> Result<Self, ClientError> {
        let http = HttpClient::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|e| ClientError(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            http,
            host: conn.host.clone(),
            api_key: conn.api_key.clone(),
        })
    }

    pub fn health(&self) -> Result<Value, ClientError> {
        self.request(Method::GET, "/v1/healthz", None)
    }

    pub fn stats(&self) -> Result<Value, ClientError> {
        self.request(Method::GET, "/v1/stats", None)
    }

    pub fn namespace_stats(&self, namespace: &str) -> Result<Value, ClientError> {
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
    ) -> Result<IndexResponse, ClientError> {
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

    pub fn search(&self, namespace: &str, query: SearchQuery) -> Result<SearchResult, ClientError> {
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

    pub fn search_body(&self, namespace: &str, mut body: Value) -> Result<Value, ClientError> {
        if let Some(obj) = body.as_object_mut() {
            obj.insert("namespace".into(), Value::String(namespace.to_string()));
        }
        self.request(
            Method::POST,
            &format!("/v1/namespaces/{}/search", encode_ns(namespace)),
            Some(body),
        )
    }

    pub fn flush(&self, namespace: Option<&str>) -> Result<Value, ClientError> {
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

    pub fn delete(&self, namespace: &str, filter: Value) -> Result<Value, ClientError> {
        self.request(
            Method::POST,
            &format!("/v1/namespaces/{}/delete", encode_ns(namespace)),
            Some(serde_json::json!({"filter": filter})),
        )
    }

    pub fn rebuild_filter_blooms(&self, namespace: &str) -> Result<Value, ClientError> {
        self.request(
            Method::POST,
            "/v1/admin/rebuild-filter-blooms",
            Some(serde_json::json!({"namespace": namespace})),
        )
    }

    pub fn curl(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, ClientError> {
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
    ) -> Result<T, ClientError> {
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
            let snippet: String = text.chars().take(500).collect();
            return Err(ClientError(format!(
                "{method} {path} → {status}: {snippet}"
            )));
        }
        if text.trim().is_empty() {
            // Some endpoints might return empty; normalize to null JSON.
            return serde_json::from_str("null")
                .map_err(|e| ClientError(format!("empty response parse error: {e}")));
        }
        serde_json::from_str(&text).map_err(|e| {
            ClientError(format!(
                "invalid JSON from {method} {path}: {e}; body={}",
                text.chars().take(200).collect::<String>()
            ))
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
