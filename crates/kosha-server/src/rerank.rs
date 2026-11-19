//! Rerank providers and per-provider metrics (issue #251).
//!
//! The rerank stage itself (ordering, paging, timeout fallback) is
//! `kosha_query::rerank`. This module supplies what the server needs
//! around it:
//!
//! - [`HttpRerankProvider`]: a Cohere-style `POST /rerank` client. Voyage,
//!   ZeroEntropy and most self-hosted rerank servers accept the same shape.
//! - [`RerankRegistry`]: the providers configured from the environment,
//!   looked up by `rerank.provider`, plus their latency / cost counters
//!   (reported under `"rerank"` in admin `/stats`).
//!
//! ## Configuration
//!
//! `KOSHA_RERANK_PROVIDERS` lists provider names (default `cohere`). For
//! each name `N` (upper-cased, non-alphanumerics as `_`):
//!
//! | Variable | Meaning |
//! |---|---|
//! | `KOSHA_RERANK_N_URL` | Endpoint. Default for `cohere`: `https://api.cohere.com/v2/rerank`; required otherwise. |
//! | `KOSHA_RERANK_N_API_KEY` | Sent as `Authorization: Bearer …`. `cohere` also reads `COHERE_API_KEY`. |
//! | `KOSHA_RERANK_N_MODEL` | Model used when a request names none. Default for `cohere`: `rerank-v3.5`. |
//!
//! `cohere` is registered only when it has an API key; another name is
//! registered when it has a URL (a key is optional, e.g. for an internal
//! model server). Keys are read from the environment only and never logged
//! or returned.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kosha_query::rerank::{
    RerankCall, RerankError, RerankOutcome, RerankProvider, RerankRequest, RerankResponse,
};

pub const COHERE_DEFAULT_URL: &str = "https://api.cohere.com/v2/rerank";
pub const COHERE_DEFAULT_MODEL: &str = "rerank-v3.5";

/// Cohere-style HTTP reranker.
///
/// Request: `{"model", "query", "documents": [text…], "top_n"}`.
/// Response: `{"results": [{"index", "relevance_score"}…]}` (Voyage's
/// `"data"` is accepted too), with optional
/// `meta.billed_units.search_units`.
pub struct HttpRerankProvider {
    name: String,
    url: String,
    api_key: Option<String>,
    default_model: Option<String>,
    client: reqwest::blocking::Client,
}

impl HttpRerankProvider {
    pub fn new(
        name: impl Into<String>,
        url: impl Into<String>,
        api_key: Option<String>,
        default_model: Option<String>,
    ) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            api_key,
            default_model,
            client: reqwest::blocking::Client::builder()
                .build()
                .expect("failed to build rerank HTTP client"),
        }
    }
}

impl std::fmt::Debug for HttpRerankProvider {
    // Hand-written so that the API key can never reach a log line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpRerankProvider")
            .field("name", &self.name)
            .field("url", &self.url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("default_model", &self.default_model)
            .finish()
    }
}

#[derive(serde::Deserialize)]
struct WireScore {
    index: usize,
    relevance_score: f64,
}

#[derive(serde::Deserialize, Default)]
struct WireBilled {
    search_units: Option<f64>,
}

#[derive(serde::Deserialize, Default)]
struct WireMeta {
    #[serde(default)]
    billed_units: Option<WireBilled>,
}

#[derive(serde::Deserialize)]
struct WireResponse {
    #[serde(default, alias = "data")]
    results: Option<Vec<WireScore>>,
    #[serde(default)]
    meta: Option<WireMeta>,
}

/// Parse a Cohere- or Voyage-style rerank response body.
fn parse_response(body: &str) -> Result<RerankResponse, RerankError> {
    let wire: WireResponse = serde_json::from_str(body)
        .map_err(|e| RerankError::Provider(format!("unreadable response: {e}")))?;
    let results = wire
        .results
        .ok_or_else(|| RerankError::Provider("response has no results".into()))?;
    Ok(RerankResponse {
        scores: results
            .into_iter()
            .map(|s| (s.index, s.relevance_score))
            .collect(),
        billed_units: wire
            .meta
            .and_then(|m| m.billed_units)
            .and_then(|b| b.search_units),
    })
}

impl RerankProvider for HttpRerankProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn rerank(
        &self,
        request: &RerankRequest,
        timeout: Duration,
    ) -> Result<RerankResponse, RerankError> {
        let mut body = serde_json::json!({
            "query": request.query,
            "documents": request.documents,
            "top_n": request.documents.len(),
        });
        if let Some(model) = request.model.as_ref().or(self.default_model.as_ref()) {
            body["model"] = serde_json::json!(model);
        }
        let mut call = self.client.post(&self.url).timeout(timeout).json(&body);
        if let Some(ref key) = self.api_key {
            call = call.bearer_auth(key);
        }
        let response = call.send().map_err(|e| {
            if e.is_timeout() {
                RerankError::Timeout
            } else if e.is_connect() {
                RerankError::Provider("connection failed".into())
            } else {
                // `without_url`: the URL may carry a key in its query.
                RerankError::Provider(format!("request failed: {}", e.without_url()))
            }
        })?;
        let status = response.status();
        let text = response.text().map_err(|e| {
            if e.is_timeout() {
                RerankError::Timeout
            } else {
                RerankError::Provider(format!("reading response: {}", e.without_url()))
            }
        })?;
        if !status.is_success() {
            let snippet: String = text.chars().take(200).collect();
            return Err(RerankError::Provider(format!(
                "HTTP {}: {snippet}",
                status.as_u16()
            )));
        }
        parse_response(&text)
    }
}

/// Upper bounds (ms) of the latency histogram buckets; the last bucket
/// is everything above.
pub const LATENCY_BUCKETS_MS: [u64; 9] = [10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];

/// Counters for one provider. Latency covers every call, including the
/// ones that timed out (they count at the timeout).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ProviderMetrics {
    pub calls: u64,
    pub ok: u64,
    pub timeouts: u64,
    pub errors: u64,
    /// Documents sent: the cost driver for per-document pricing.
    pub documents: u64,
    /// Sum of the billed units providers reported (Cohere search units).
    pub billed_units: f64,
    pub latency_ms_sum: f64,
    pub latency_ms_max: f64,
    /// Counts per [`LATENCY_BUCKETS_MS`] bucket, plus one overflow bucket.
    pub latency_buckets: [u64; LATENCY_BUCKETS_MS.len() + 1],
}

impl ProviderMetrics {
    fn record(&mut self, call: &RerankCall) {
        self.calls += 1;
        match call.outcome {
            RerankOutcome::Ok => self.ok += 1,
            RerankOutcome::Timeout => self.timeouts += 1,
            RerankOutcome::Error(_) => self.errors += 1,
        }
        self.documents += call.documents as u64;
        self.billed_units += call.billed_units.unwrap_or(0.0);
        let ms = call.latency.as_secs_f64() * 1e3;
        self.latency_ms_sum += ms;
        self.latency_ms_max = self.latency_ms_max.max(ms);
        let bucket = LATENCY_BUCKETS_MS
            .iter()
            .position(|&upper| ms <= upper as f64)
            .unwrap_or(LATENCY_BUCKETS_MS.len());
        self.latency_buckets[bucket] += 1;
    }

    fn to_json(&self) -> serde_json::Value {
        let mut buckets = serde_json::Map::new();
        for (i, count) in self.latency_buckets.iter().enumerate() {
            let label = LATENCY_BUCKETS_MS
                .get(i)
                .map_or_else(|| "+Inf".to_string(), |b| b.to_string());
            buckets.insert(label, serde_json::json!(count));
        }
        serde_json::json!({
            "calls": self.calls,
            "ok": self.ok,
            "timeouts": self.timeouts,
            "errors": self.errors,
            "documents": self.documents,
            "billed_units": self.billed_units,
            "latency_ms_sum": self.latency_ms_sum,
            "latency_ms_max": self.latency_ms_max,
            "latency_ms_mean": if self.calls == 0 { 0.0 } else { self.latency_ms_sum / self.calls as f64 },
            "latency_ms_le": buckets,
        })
    }
}

/// The configured providers, by name, and their metrics.
#[derive(Default)]
pub struct RerankRegistry {
    providers: HashMap<String, Arc<dyn RerankProvider>>,
    metrics: Mutex<BTreeMap<String, ProviderMetrics>>,
}

fn env_name(provider: &str) -> String {
    provider
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

impl RerankRegistry {
    pub fn from_env() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// See the module docs for the variables read.
    pub fn from_lookup(env: impl Fn(&str) -> Option<String>) -> Self {
        let get = |key: &str| {
            env(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let names = get("KOSHA_RERANK_PROVIDERS").unwrap_or_else(|| "cohere".into());
        let mut providers: Vec<Arc<dyn RerankProvider>> = Vec::new();
        for name in names.split(',').map(str::trim).filter(|n| !n.is_empty()) {
            let var = env_name(name);
            let is_cohere = name.eq_ignore_ascii_case("cohere");
            let mut key = get(&format!("KOSHA_RERANK_{var}_API_KEY"));
            if is_cohere && key.is_none() {
                key = get("COHERE_API_KEY");
            }
            let url = get(&format!("KOSHA_RERANK_{var}_URL"))
                .or_else(|| is_cohere.then(|| COHERE_DEFAULT_URL.to_string()));
            let model = get(&format!("KOSHA_RERANK_{var}_MODEL"))
                .or_else(|| is_cohere.then(|| COHERE_DEFAULT_MODEL.to_string()));
            match url {
                Some(_) if is_cohere && key.is_none() => {
                    // Without a key every call would fail; say so once at
                    // startup instead of degrading every request.
                    if env("KOSHA_RERANK_PROVIDERS").is_some() {
                        eprintln!(
                            "rerank: provider {name} not registered: set \
                             KOSHA_RERANK_{var}_API_KEY or COHERE_API_KEY"
                        );
                    }
                }
                Some(url) => {
                    providers.push(Arc::new(HttpRerankProvider::new(name, url, key, model)))
                }
                None => eprintln!(
                    "rerank: provider {name} not registered: KOSHA_RERANK_{var}_URL is not set"
                ),
            }
        }
        Self::with_providers(providers)
    }

    pub fn with_providers(providers: Vec<Arc<dyn RerankProvider>>) -> Self {
        Self {
            providers: providers
                .into_iter()
                .map(|p| (p.name().to_string(), p))
                .collect(),
            metrics: Mutex::default(),
        }
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn RerankProvider>> {
        self.providers.get(name)
    }

    /// Configured provider names, sorted (for error messages).
    pub fn names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.providers.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    pub fn record(&self, call: &RerankCall) {
        self.metrics
            .lock()
            .unwrap()
            .entry(call.provider.clone())
            .or_default()
            .record(call);
    }

    #[cfg(test)]
    pub fn metrics(&self, provider: &str) -> Option<ProviderMetrics> {
        self.metrics.lock().unwrap().get(provider).cloned()
    }

    /// `{"providers": [...], "metrics": {name: {...}}}` for `/stats`.
    pub fn stats_json(&self) -> serde_json::Value {
        let metrics: serde_json::Map<String, serde_json::Value> = self
            .metrics
            .lock()
            .unwrap()
            .iter()
            .map(|(name, m)| (name.clone(), m.to_json()))
            .collect();
        serde_json::json!({
            "providers": self.names(),
            "metrics": metrics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    /// A one-request HTTP server on loopback: records the request and
    /// answers with `status` / `body` after `delay`. No external network.
    fn stub_server(
        status: u16,
        body: &'static str,
        delay: Duration,
    ) -> (String, std::thread::JoinHandle<(String, String)>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/v2/rerank", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut head = String::new();
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = v.trim().parse().unwrap();
                }
                head.push_str(&line);
                if line == "\r\n" {
                    break;
                }
            }
            let mut req_body = vec![0; content_length];
            reader.read_exact(&mut req_body).unwrap();
            std::thread::sleep(delay);
            let mut stream = stream;
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            (head, String::from_utf8(req_body).unwrap())
        });
        (url, handle)
    }

    fn request() -> RerankRequest {
        RerankRequest {
            model: None,
            query: "termination for breach".into(),
            documents: vec!["a".into(), "b".into()],
        }
    }

    #[test]
    fn http_provider_speaks_cohere() {
        let (url, server) = stub_server(
            200,
            r#"{"id":"x","results":[{"index":1,"relevance_score":0.9},{"index":0,"relevance_score":0.1}],
               "meta":{"billed_units":{"search_units":1}}}"#,
            Duration::ZERO,
        );
        let p = HttpRerankProvider::new(
            "cohere",
            url,
            Some("sk-test".into()),
            Some("rerank-v3.5".into()),
        );
        let r = p.rerank(&request(), Duration::from_secs(5)).unwrap();
        assert_eq!(r.scores, vec![(1, 0.9), (0, 0.1)]);
        assert_eq!(r.billed_units, Some(1.0));
        let (head, body) = server.join().unwrap();
        assert!(head.starts_with("POST /v2/rerank "), "{head}");
        assert!(
            head.to_ascii_lowercase()
                .contains("authorization: bearer sk-test"),
            "{head}"
        );
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"model": "rerank-v3.5", "query": "termination for breach",
                               "documents": ["a", "b"], "top_n": 2})
        );
        // The key never shows up in debug output.
        assert!(!format!("{p:?}").contains("sk-test"));
    }

    #[test]
    fn http_provider_request_model_wins_and_key_is_optional() {
        let (url, server) = stub_server(
            200,
            r#"{"data":[{"index":0,"relevance_score":0.5},{"index":1,"relevance_score":0.4}]}"#,
            Duration::ZERO,
        );
        let p = HttpRerankProvider::new("internal", url, None, Some("default".into()));
        let mut req = request();
        req.model = Some("rerank-2".into());
        let r = p.rerank(&req, Duration::from_secs(5)).unwrap();
        assert_eq!(r.scores, vec![(0, 0.5), (1, 0.4)]);
        assert_eq!(r.billed_units, None);
        let (head, body) = server.join().unwrap();
        assert!(
            !head.to_ascii_lowercase().contains("authorization"),
            "{head}"
        );
        assert!(body.contains(r#""model":"rerank-2""#), "{body}");
    }

    #[test]
    fn http_provider_errors_and_timeouts() {
        let (url, server) = stub_server(429, r#"{"message":"rate limited"}"#, Duration::ZERO);
        let p = HttpRerankProvider::new("cohere", url, Some("k".into()), None);
        match p.rerank(&request(), Duration::from_secs(5)) {
            Err(RerankError::Provider(msg)) => {
                assert!(
                    msg.starts_with("HTTP 429") && msg.contains("rate limited"),
                    "{msg}"
                )
            }
            other => panic!("{other:?}"),
        }
        server.join().unwrap();

        let (url, server) = stub_server(200, r#"{"nope":1}"#, Duration::ZERO);
        let p = HttpRerankProvider::new("cohere", url, Some("k".into()), None);
        assert!(matches!(
            p.rerank(&request(), Duration::from_secs(5)),
            Err(RerankError::Provider(_))
        ));
        server.join().unwrap();

        let (url, server) = stub_server(200, r#"{"results":[]}"#, Duration::from_millis(800));
        let p = HttpRerankProvider::new("cohere", url, Some("k".into()), None);
        assert_eq!(
            p.rerank(&request(), Duration::from_millis(100)),
            Err(RerankError::Timeout)
        );
        server.join().unwrap();
    }

    #[test]
    fn registry_from_env() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(name, _)| *name == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        // Nothing set: cohere has no key, so nothing is registered.
        assert!(RerankRegistry::from_lookup(env(&[])).names().is_empty());
        let r = RerankRegistry::from_lookup(env(&[("COHERE_API_KEY", "k")]));
        assert_eq!(r.names(), ["cohere"]);
        let r = RerankRegistry::from_lookup(env(&[
            ("KOSHA_RERANK_PROVIDERS", "cohere, voyage,my-model,nourl"),
            ("KOSHA_RERANK_COHERE_API_KEY", "k"),
            (
                "KOSHA_RERANK_VOYAGE_URL",
                "https://api.voyageai.com/v1/rerank",
            ),
            ("KOSHA_RERANK_VOYAGE_API_KEY", "v"),
            (
                "KOSHA_RERANK_MY_MODEL_URL",
                "http://reranker.internal/rerank",
            ),
        ]));
        assert_eq!(r.names(), ["cohere", "my-model", "voyage"]);
        assert!(r.get("nourl").is_none());
    }

    #[test]
    fn metrics_count_outcomes_documents_cost_and_latency() {
        let r = RerankRegistry::default();
        let call = |ms: u64, outcome: RerankOutcome, billed: Option<f64>| RerankCall {
            provider: "cohere".into(),
            latency: Duration::from_millis(ms),
            documents: 50,
            outcome,
            billed_units: billed,
        };
        r.record(&call(40, RerankOutcome::Ok, Some(1.0)));
        r.record(&call(500, RerankOutcome::Timeout, None));
        r.record(&call(9_000, RerankOutcome::Error("HTTP 500".into()), None));
        let m = r.metrics("cohere").unwrap();
        assert_eq!((m.calls, m.ok, m.timeouts, m.errors), (3, 1, 1, 1));
        assert_eq!(m.documents, 150);
        assert_eq!(m.billed_units, 1.0);
        assert_eq!(m.latency_ms_max, 9_000.0);
        assert_eq!(m.latency_buckets[2], 1); // <= 50
        assert_eq!(m.latency_buckets[5], 1); // <= 500
        assert_eq!(m.latency_buckets[9], 1); // +Inf
        let stats = r.stats_json();
        assert_eq!(stats["metrics"]["cohere"]["calls"], 3);
        assert_eq!(stats["metrics"]["cohere"]["latency_ms_le"]["+Inf"], 1);
        assert!(r.metrics("other").is_none());
    }
}
