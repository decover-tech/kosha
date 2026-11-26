//! Native embedding (issue #209): the server turns text into vectors for
//! `Vector` fields whose namespace schema declares a model
//! (`"embedding": {"model", "dims", "provider"}`), both at ingest and for
//! `knn.query_text`, so the write path and the query path cannot drift
//! apart.
//!
//! - [`Embedder`]: one model. [`MockEmbedder`] is a deterministic hashed
//!   bag of words for tests and local development; [`HttpEmbedder`] calls
//!   an OpenAI-compatible `POST {base}/embeddings`. With the `onnx`
//!   feature, the local provider runs operator-configured ONNX models.
//! - [`EmbedderRegistry`]: the providers this pod has, configured from the
//!   environment, and the embedder serving each declared model.
//!
//! ## Configuration
//!
//! | Variable | Meaning |
//! |---|---|
//! | `KOSHA_EMBEDDING_PROVIDERS` | Comma list of enabled providers: `mock`, `http`, `onnx`. Empty (default): no native embedding on this pod. |
//! | `KOSHA_EMBEDDING_ONNX_MODELS` | JSON map from ONNX model IDs to their local artifact paths and sequence lengths. |
//! | `KOSHA_EMBEDDING_URL` | Base URL of the OpenAI-compatible server (`/embeddings` is appended). Required for `http`. |
//! | `KOSHA_EMBEDDING_API_KEY` | Sent as `Authorization: Bearer …`. Optional. |
//! | `KOSHA_EMBEDDING_TIMEOUT_MS` | Per-request timeout, default 10000. |
//! | `KOSHA_EMBEDDING_BATCH_SIZE` | Texts per provider request, default 64. |
//!
//! Ingest and query pods need the same configuration: a query pod embeds
//! `knn.query_text` itself. The API key is never logged or returned.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kosha_core::EmbeddingSpec;

#[cfg(feature = "onnx")]
mod onnx;

/// Why an embedding could not be produced. Each maps to an HTTP status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbedError {
    /// This pod has no provider for the declared model (501
    /// `not_implemented`, not retryable: the identical request fails the
    /// same way until an operator configures one, so a client that retries
    /// on `retryable:true` must not loop on it; ADR-350 §b).
    NotConfigured(String),
    /// The provider could not be reached or timed out (503).
    Unavailable(String),
    /// The provider answered, but not with usable vectors (502).
    Provider(String),
}

impl EmbedError {
    pub fn status(&self) -> u16 {
        match self {
            Self::NotConfigured(_) => 501,
            Self::Unavailable(_) => 503,
            Self::Provider(_) => 502,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::NotConfigured(m) | Self::Unavailable(m) | Self::Provider(m) => m,
        }
    }
}

impl std::fmt::Display for EmbedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

/// One embedding model.
pub trait Embedder: Send + Sync {
    /// The model id, as the schema's `embedding.model` names it.
    fn model_id(&self) -> &str;
    /// Dimension of every vector `embed` returns.
    fn dims(&self) -> usize;
    /// One vector of `dims()` per text, in order.
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError>;
}

/// Embed `texts` with `embedder` and check what came back: one vector per
/// text, each of the declared dimension and finite.
pub fn embed_checked(
    embedder: &dyn Embedder,
    spec: &EmbeddingSpec,
    texts: &[&str],
) -> Result<Vec<Vec<f32>>, EmbedError> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }
    let vectors = embedder.embed(texts)?;
    if vectors.len() != texts.len() {
        return Err(EmbedError::Provider(format!(
            "embedding model {} returned {} vectors for {} texts",
            embedder.model_id(),
            vectors.len(),
            texts.len()
        )));
    }
    for v in &vectors {
        if v.len() != spec.dims as usize {
            return Err(EmbedError::Provider(format!(
                "embedding model {} returned {} dimensions, schema declares {}",
                embedder.model_id(),
                v.len(),
                spec.dims
            )));
        }
        if v.iter().any(|x| !x.is_finite()) {
            return Err(EmbedError::Provider(format!(
                "embedding model {} returned a non-finite value",
                embedder.model_id()
            )));
        }
    }
    Ok(vectors)
}

// ─── Mock ───────────────────────────────────────────────────────────────────

/// Deterministic test embedder (provider `mock`): lowercase word unigrams
/// and bigrams hashed into `dims` signed buckets, then L2-normalized. Texts
/// that share words are near neighbours; the model id seeds the hash, so
/// two mock models are two different vector spaces. No semantics beyond
/// word overlap — never use it for real data.
#[derive(Debug, Clone)]
pub struct MockEmbedder {
    model: String,
    dims: usize,
}

impl MockEmbedder {
    pub fn new(model: impl Into<String>, dims: usize) -> Self {
        Self {
            model: model.into(),
            dims: dims.max(1),
        }
    }

    fn hash(&self, token: &str) -> u64 {
        // FNV-1a, seeded with the model id: stable across builds and
        // platforms, unlike `std`'s hasher.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in self.model.bytes().chain([0xff]).chain(token.bytes()) {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }

    fn embed_one(&self, text: &str) -> Vec<f32> {
        let lower = text.to_lowercase();
        let words: Vec<&str> = lower
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .collect();
        let mut v = vec![0f32; self.dims];
        let mut add = |token: &str, weight: f32| {
            let h = self.hash(token);
            let sign = if h >> 63 == 0 { 1.0 } else { -1.0 };
            v[(h % self.dims as u64) as usize] += sign * weight;
        };
        for w in &words {
            add(w, 1.0);
        }
        for pair in words.windows(2) {
            add(&format!("{} {}", pair[0], pair[1]), 0.5);
        }
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            v.iter_mut().for_each(|x| *x /= norm);
        } else {
            // No words (or every bucket cancelled): a fixed unit vector,
            // so cosine stays defined.
            v[0] = 1.0;
        }
        v
    }
}

impl Embedder for MockEmbedder {
    fn model_id(&self) -> &str {
        &self.model
    }
    fn dims(&self) -> usize {
        self.dims
    }
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts.iter().map(|t| self.embed_one(t)).collect())
    }
}

// ─── HTTP (OpenAI-compatible) ───────────────────────────────────────────────

/// Settings of the `http` provider.
#[derive(Clone)]
pub struct HttpConfig {
    /// Base URL; `/embeddings` is appended.
    pub base_url: String,
    pub api_key: Option<String>,
    pub timeout: Duration,
    /// Texts per request (at least 1).
    pub batch_size: usize,
}

impl std::fmt::Debug for HttpConfig {
    // Hand-written so that the API key can never reach a log line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConfig")
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field("timeout", &self.timeout)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

/// OpenAI-compatible embedding client: `POST {base}/embeddings` with
/// `{"model", "input": [text…]}`, answered by
/// `{"data": [{"index", "embedding": [f32…]}…]}`. Texts go in batches of
/// `batch_size`; every returned vector must have the declared dimension.
pub struct HttpEmbedder {
    model: String,
    dims: usize,
    url: String,
    api_key: Option<String>,
    batch_size: usize,
    client: reqwest::blocking::Client,
}

impl HttpEmbedder {
    pub fn new(model: impl Into<String>, dims: usize, config: &HttpConfig) -> Self {
        Self {
            model: model.into(),
            dims,
            url: format!("{}/embeddings", config.base_url.trim_end_matches('/')),
            api_key: config.api_key.clone(),
            batch_size: config.batch_size.max(1),
            client: reqwest::blocking::Client::builder()
                .timeout(config.timeout)
                .build()
                .expect("failed to build embedding HTTP client"),
        }
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let body = serde_json::json!({"model": self.model, "input": texts});
        let mut request = self.client.post(&self.url).json(&body);
        if let Some(ref key) = self.api_key {
            request = request.bearer_auth(key);
        }
        let response = request.send().map_err(|e| {
            EmbedError::Unavailable(format!(
                "embedding provider for model {} unreachable: {}",
                self.model,
                if e.is_timeout() {
                    "timed out".to_string()
                } else {
                    e.without_url().to_string()
                }
            ))
        })?;
        let status = response.status();
        let text = response.text().map_err(|e| {
            EmbedError::Unavailable(format!(
                "embedding provider for model {}: reading the response failed: {}",
                self.model,
                e.without_url()
            ))
        })?;
        if !status.is_success() {
            let snippet: String = text.chars().take(200).collect();
            return Err(EmbedError::Provider(format!(
                "embedding provider for model {} answered {}: {snippet}",
                self.model,
                status.as_u16()
            )));
        }
        #[derive(serde::Deserialize)]
        struct Item {
            #[serde(default)]
            index: Option<usize>,
            embedding: Vec<f32>,
        }
        #[derive(serde::Deserialize)]
        struct Wire {
            data: Vec<Item>,
        }
        let wire: Wire = serde_json::from_str(&text).map_err(|e| {
            EmbedError::Provider(format!(
                "embedding provider for model {}: unreadable response: {e}",
                self.model
            ))
        })?;
        if wire.data.len() != texts.len() {
            return Err(EmbedError::Provider(format!(
                "embedding provider for model {} returned {} vectors for {} texts",
                self.model,
                wire.data.len(),
                texts.len()
            )));
        }
        let mut out: Vec<Option<Vec<f32>>> = vec![None; texts.len()];
        for (position, item) in wire.data.into_iter().enumerate() {
            let index = item.index.unwrap_or(position);
            if item.embedding.len() != self.dims {
                return Err(EmbedError::Provider(format!(
                    "embedding provider for model {} returned {} dimensions, schema declares {}",
                    self.model,
                    item.embedding.len(),
                    self.dims
                )));
            }
            match out.get_mut(index) {
                Some(slot @ None) => *slot = Some(item.embedding),
                _ => {
                    return Err(EmbedError::Provider(format!(
                        "embedding provider for model {} returned a bad index {index}",
                        self.model
                    )))
                }
            }
        }
        Ok(out.into_iter().map(|v| v.unwrap_or_default()).collect())
    }
}

impl Embedder for HttpEmbedder {
    fn model_id(&self) -> &str {
        &self.model
    }
    fn dims(&self) -> usize {
        self.dims
    }
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let mut out = Vec::with_capacity(texts.len());
        for batch in texts.chunks(self.batch_size) {
            out.extend(self.embed_batch(batch)?);
        }
        Ok(out)
    }
}

// ─── Registry ───────────────────────────────────────────────────────────────

pub const PROVIDER_MOCK: &str = "mock";

/// `(provider, model, dims)` of an embedder the registry built.
type BuiltKey = (String, String, u32);
/// An embedder built on first use; `None` until a load succeeds.
type BuiltSlot = Arc<Mutex<Option<Arc<dyn Embedder>>>>;
pub const PROVIDER_HTTP: &str = "http";
#[cfg(feature = "onnx")]
const PROVIDER_ONNX: &str = "onnx";

/// The embedding providers this pod has, and the embedder serving each
/// model a schema declares. Embedders are built on first use and kept,
/// keyed by model id (and provider and dims, so a schema naming the same
/// model with another dimension never shares one).
#[derive(Default)]
pub struct EmbedderRegistry {
    mock: bool,
    http: Option<HttpConfig>,
    #[cfg(feature = "onnx")]
    onnx: HashMap<String, onnx::ModelConfig>,
    /// Embedders registered up front (tests), by model id. Consulted
    /// before the providers.
    fixed: HashMap<String, Arc<dyn Embedder>>,
    /// One slot per key: a load holds only its own slot's lock, so loads of
    /// one model are not duplicated and do not block other models.
    built: Mutex<HashMap<BuiltKey, BuiltSlot>>,
}

impl EmbedderRegistry {
    /// From `KOSHA_EMBEDDING_*` (see the module docs).
    pub fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub(crate) fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let mut registry = Self::default();
        let providers = get("KOSHA_EMBEDDING_PROVIDERS").unwrap_or_default();
        for provider in providers
            .split(',')
            .map(|p| p.trim().to_ascii_lowercase())
            .filter(|p| !p.is_empty())
        {
            match provider.as_str() {
                PROVIDER_MOCK => registry.mock = true,
                #[cfg(feature = "onnx")]
                PROVIDER_ONNX => {
                    registry.onnx = get("KOSHA_EMBEDDING_ONNX_MODELS")
                        .ok_or_else(|| "KOSHA_EMBEDDING_ONNX_MODELS is unset".to_owned())
                        .and_then(|raw| serde_json::from_str(&raw).map_err(|err| err.to_string()))
                        .unwrap_or_else(|err| {
                            eprintln!("kosha: ONNX embedding disabled: {err}");
                            HashMap::new()
                        });
                }
                PROVIDER_HTTP => {
                    let Some(base_url) =
                        get("KOSHA_EMBEDDING_URL").filter(|u| !u.trim().is_empty())
                    else {
                        eprintln!(
                            "kosha: KOSHA_EMBEDDING_PROVIDERS names http but KOSHA_EMBEDDING_URL is unset; http embedding disabled"
                        );
                        continue;
                    };
                    let timeout_ms = get("KOSHA_EMBEDDING_TIMEOUT_MS")
                        .and_then(|v| v.trim().parse::<u64>().ok())
                        .filter(|&ms| ms > 0)
                        .unwrap_or(10_000);
                    let batch_size = get("KOSHA_EMBEDDING_BATCH_SIZE")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                        .filter(|&n| n > 0)
                        .unwrap_or(64);
                    registry.http = Some(HttpConfig {
                        base_url: base_url.trim().to_string(),
                        api_key: get("KOSHA_EMBEDDING_API_KEY").filter(|k| !k.is_empty()),
                        timeout: Duration::from_millis(timeout_ms),
                        batch_size,
                    });
                }
                other => eprintln!("kosha: unknown embedding provider {other:?} ignored"),
            }
        }
        registry
    }

    /// A registry with only the `mock` provider enabled.
    #[cfg(test)]
    pub fn with_mock() -> Self {
        Self {
            mock: true,
            ..Self::default()
        }
    }

    /// Serve `embedder.model_id()` with `embedder`, whatever the
    /// providers (tests: a counting embedder).
    #[cfg(test)]
    pub fn with_embedder(mut self, embedder: Arc<dyn Embedder>) -> Self {
        self.fixed.insert(embedder.model_id().to_string(), embedder);
        self
    }

    /// Names of the enabled providers, for `/stats`.
    pub fn providers(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        if self.mock {
            names.push(PROVIDER_MOCK);
        }
        if self.http.is_some() {
            names.push(PROVIDER_HTTP);
        }
        #[cfg(feature = "onnx")]
        if !self.onnx.is_empty() {
            names.push(PROVIDER_ONNX);
        }
        names
    }

    /// The embedder for `spec`. `spec.provider` picks the provider; without
    /// one, keep the existing `http`, then `mock` precedence. ONNX models
    /// always name their provider explicitly.
    pub fn get(&self, spec: &EmbeddingSpec) -> Result<Arc<dyn Embedder>, EmbedError> {
        if let Some(embedder) = self.fixed.get(&spec.model) {
            if embedder.dims() != spec.dims as usize {
                return Err(EmbedError::NotConfigured(format!(
                    "embedding model {} on this pod has {} dimensions, schema declares {}",
                    spec.model,
                    embedder.dims(),
                    spec.dims
                )));
            }
            return Ok(Arc::clone(embedder));
        }
        let provider = match spec.provider.as_deref().map(str::to_ascii_lowercase) {
            Some(p) => p,
            None if self.http.is_some() => PROVIDER_HTTP.to_string(),
            None if self.mock => PROVIDER_MOCK.to_string(),
            None => {
                return Err(EmbedError::NotConfigured(format!(
                    "embedding model {} needs a configured provider on this pod (KOSHA_EMBEDDING_PROVIDERS; specify provider \"onnx\" for local ONNX models)",
                    spec.model
                )))
            }
        };
        let key = (provider.clone(), spec.model.clone(), spec.dims);
        let shared = Arc::clone(self.built.lock().unwrap().entry(key.clone()).or_default());
        let mut slot = shared.lock().unwrap();
        if let Some(embedder) = slot.as_ref() {
            return Ok(Arc::clone(embedder));
        }
        match self.build(spec, &provider) {
            Ok(embedder) => {
                *slot = Some(Arc::clone(&embedder));
                Ok(embedder)
            }
            Err(err) => {
                // Forget the empty slot so failed lookups (keys are schema
                // controlled) do not accumulate. A slot another caller is
                // loading is left to that caller.
                drop(slot);
                let mut built = self.built.lock().unwrap();
                if built.get(&key).is_some_and(|current| {
                    Arc::ptr_eq(current, &shared)
                        && current.try_lock().is_ok_and(|loaded| loaded.is_none())
                }) {
                    built.remove(&key);
                }
                Err(err)
            }
        }
    }

    fn build(&self, spec: &EmbeddingSpec, provider: &str) -> Result<Arc<dyn Embedder>, EmbedError> {
        Ok(match provider {
            PROVIDER_MOCK if self.mock => {
                Arc::new(MockEmbedder::new(&spec.model, spec.dims as usize))
            }
            PROVIDER_HTTP => match self.http {
                Some(ref config) => {
                    Arc::new(HttpEmbedder::new(&spec.model, spec.dims as usize, config))
                }
                None => return Err(not_enabled(spec, provider)),
            },
            #[cfg(feature = "onnx")]
            PROVIDER_ONNX => {
                let config = self.onnx.get(&spec.model).ok_or_else(|| {
                    EmbedError::NotConfigured(format!(
                        "embedding model {} has no entry in KOSHA_EMBEDDING_ONNX_MODELS",
                        spec.model
                    ))
                })?;
                Arc::new(onnx::OnnxEmbedder::load(spec, config)?)
            }
            _ => return Err(not_enabled(spec, provider)),
        })
    }
}

fn not_enabled(spec: &EmbeddingSpec, provider: &str) -> EmbedError {
    EmbedError::NotConfigured(format!(
        "embedding model {} needs provider {provider:?}, which is not enabled on this pod \
         (KOSHA_EMBEDDING_PROVIDERS)",
        spec.model
    ))
}

/// A field value that is a vector already, rather than text to embed:
/// `Some(Ok(v))` for a JSON number array, `Some(Err(()))` for another JSON
/// array (a late-interaction bag, left to the vector path as today),
/// `None` for text.
pub fn supplied_vector(value: &str) -> Option<Result<Vec<f32>, ()>> {
    let trimmed = value.trim_start();
    if !trimmed.starts_with('[') {
        return None;
    }
    if let Ok(v) = serde_json::from_str::<Vec<f32>>(value) {
        return Some(Ok(v));
    }
    if serde_json::from_str::<Vec<serde_json::Value>>(value).is_ok() {
        return Some(Err(()));
    }
    // `[draft] memo` and the like: text that happens to start with '['.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn spec(model: &str, dims: u32, provider: Option<&str>) -> EmbeddingSpec {
        EmbeddingSpec {
            model: model.into(),
            dims,
            distance_metric: None,
            provider: provider.map(str::to_string),
        }
    }

    fn cosine(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| x * y).sum()
    }

    #[test]
    fn mock_is_deterministic_normalized_and_word_sensitive() {
        let m = MockEmbedder::new("mock-a", 64);
        let v = m
            .embed(&[
                "red running shoes",
                "red running shoes",
                "blue denim jacket",
                "",
            ])
            .unwrap();
        assert_eq!(v[0], v[1]);
        for x in &v {
            assert_eq!(x.len(), 64);
            let norm: f32 = x.iter().map(|a| a * a).sum::<f32>().sqrt();
            assert!((norm - 1.0).abs() < 1e-5);
        }
        let near = m.embed(&["running shoes for trail"]).unwrap();
        assert!(cosine(&v[0], &near[0]) > cosine(&v[2], &near[0]));
        // Another model id is another space.
        let other = MockEmbedder::new("mock-b", 64)
            .embed(&["red running shoes"])
            .unwrap();
        assert_ne!(other[0], v[0]);
    }

    #[test]
    fn registry_from_env_and_provider_selection() {
        let empty = EmbedderRegistry::from_lookup(|_| None);
        assert!(empty.providers().is_empty());
        let err = empty.get(&spec("m", 8, None)).err().unwrap();
        assert_eq!(err.status(), 501);
        // 501 is `not_implemented`: not retryable, so an SDK does not loop.
        let code = crate::api_error::ErrorCode::from_status(err.status());
        assert_eq!(code.as_str(), "not_implemented");
        assert!(!code.retryable());

        let mock = EmbedderRegistry::from_lookup(|k| {
            (k == "KOSHA_EMBEDDING_PROVIDERS").then(|| "mock".to_string())
        });
        assert_eq!(mock.providers(), vec![PROVIDER_MOCK]);
        let e = mock.get(&spec("m", 8, None)).unwrap();
        assert_eq!((e.model_id(), e.dims()), ("m", 8));
        assert_eq!(
            mock.get(&spec("m", 8, Some("http")))
                .err()
                .unwrap()
                .status(),
            501
        );

        // http without a URL stays disabled.
        let http = EmbedderRegistry::from_lookup(|k| {
            (k == "KOSHA_EMBEDDING_PROVIDERS").then(|| "http,mock".to_string())
        });
        assert_eq!(http.providers(), vec![PROVIDER_MOCK]);

        let counting = Arc::new(MockEmbedder::new("fixed", 4));
        let fixed = EmbedderRegistry::default().with_embedder(counting);
        assert!(fixed.get(&spec("fixed", 4, None)).is_ok());
        assert_eq!(
            fixed.get(&spec("fixed", 5, None)).err().unwrap().status(),
            501
        );
    }

    #[test]
    fn supplied_vectors_are_told_from_text() {
        assert_eq!(supplied_vector("[1, 2.5]"), Some(Ok(vec![1.0, 2.5])));
        assert_eq!(supplied_vector("[[1.0], [2.0]]"), Some(Err(())));
        assert_eq!(supplied_vector("[draft] memo"), None);
        assert_eq!(supplied_vector("hello"), None);
    }

    /// A one-thread HTTP stub: answers each request with `respond(body)`
    /// (status, body), after `delay`. Records request bodies.
    struct Stub {
        url: String,
        bodies: Arc<Mutex<Vec<serde_json::Value>>>,
        auth: Arc<Mutex<Vec<String>>>,
    }

    fn stub(
        delay: Duration,
        respond: impl Fn(&serde_json::Value) -> (u16, String) + Send + 'static,
    ) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let auth = Arc::new(Mutex::new(Vec::new()));
        let (b, a) = (Arc::clone(&bodies), Arc::clone(&auth));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let (head_end, length) = loop {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break (None, 0);
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                        let length = head
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().ok())?
                            })
                            .unwrap_or(0);
                        for line in head.lines() {
                            if let Some((k, v)) = line.split_once(':') {
                                if k.eq_ignore_ascii_case("authorization") {
                                    a.lock().unwrap().push(v.trim().to_string());
                                }
                            }
                        }
                        break (Some(pos + 4), length);
                    }
                };
                let Some(start) = head_end else { continue };
                while buf.len() < start + length {
                    let n = stream.read(&mut chunk).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body: serde_json::Value =
                    serde_json::from_slice(&buf[start..]).unwrap_or(serde_json::Value::Null);
                b.lock().unwrap().push(body.clone());
                let (status, reply) = respond(&body);
                std::thread::sleep(delay);
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                );
            }
        });
        Stub { url, bodies, auth }
    }

    /// An OpenAI-style answer: each input's vector is `[len, i, 0, …]`
    /// of `dims`, listed in reverse order to exercise `index`.
    fn openai_reply(body: &serde_json::Value, dims: usize) -> (u16, String) {
        let inputs = body["input"].as_array().cloned().unwrap_or_default();
        let mut data: Vec<serde_json::Value> = inputs
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let mut v = vec![0f32; dims];
                v[0] = t.as_str().unwrap_or("").len() as f32;
                if dims > 1 {
                    v[1] = i as f32;
                }
                serde_json::json!({"object": "embedding", "index": i, "embedding": v})
            })
            .collect();
        data.reverse();
        (
            200,
            serde_json::json!({"object": "list", "data": data, "model": body["model"]}).to_string(),
        )
    }

    fn config(url: &str, timeout_ms: u64, batch: usize) -> HttpConfig {
        HttpConfig {
            base_url: url.to_string(),
            api_key: Some("sk-test".into()),
            timeout: Duration::from_millis(timeout_ms),
            batch_size: batch,
        }
    }

    #[test]
    fn http_embedder_batches_and_keeps_input_order() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let s = stub(Duration::ZERO, move |body| {
            c.fetch_add(1, Ordering::SeqCst);
            openai_reply(body, 3)
        });
        let e = HttpEmbedder::new("text-embed-test", 3, &config(&s.url, 5_000, 2));
        let texts = ["a", "bb", "ccc", "dddd", "eeeee"];
        let v = e.embed(&texts).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3, "5 texts in batches of 2");
        let lens: Vec<f32> = v.iter().map(|x| x[0]).collect();
        assert_eq!(lens, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let bodies = s.bodies.lock().unwrap();
        assert_eq!(bodies[0]["model"], "text-embed-test");
        assert_eq!(bodies[0]["input"], serde_json::json!(["a", "bb"]));
        assert_eq!(bodies[2]["input"], serde_json::json!(["eeeee"]));
        assert!(s.auth.lock().unwrap().iter().all(|a| a == "Bearer sk-test"));
    }

    #[test]
    fn http_embedder_rejects_wrong_dims() {
        let s = stub(Duration::ZERO, |body| openai_reply(body, 5));
        let e = HttpEmbedder::new("m", 3, &config(&s.url, 5_000, 8));
        let err = e.embed(&["x"]).unwrap_err();
        assert_eq!(err.status(), 502, "{err}");
        assert!(err.message().contains("5 dimensions"), "{err}");
    }

    #[test]
    fn http_embedder_non_200_is_a_provider_error() {
        let s = stub(Duration::ZERO, |_| {
            (500, r#"{"error": "boom"}"#.to_string())
        });
        let e = HttpEmbedder::new("m", 3, &config(&s.url, 5_000, 8));
        let err = e.embed(&["x"]).unwrap_err();
        assert_eq!(err.status(), 502);
        assert!(err.message().contains("500"), "{err}");
        assert!(!err.message().contains("sk-test"));
    }

    #[test]
    fn http_embedder_timeout_is_unavailable() {
        let s = stub(Duration::from_millis(800), |body| openai_reply(body, 3));
        let e = HttpEmbedder::new("m", 3, &config(&s.url, 100, 8));
        let started = std::time::Instant::now();
        let err = e.embed(&["x"]).unwrap_err();
        assert_eq!(err.status(), 503, "{err}");
        assert!(started.elapsed() < Duration::from_millis(700));
    }

    #[test]
    fn failed_lookups_leave_no_cache_entry() {
        let registry = EmbedderRegistry::with_mock();
        for dims in 1..=3 {
            assert!(registry.get(&spec("m", dims, Some("nope"))).is_err());
        }
        assert!(registry.built.lock().unwrap().is_empty());
        registry.get(&spec("m", 4, None)).unwrap();
        assert_eq!(registry.built.lock().unwrap().len(), 1);
    }

    #[test]
    fn http_provider_from_registry() {
        let s = stub(Duration::ZERO, |body| openai_reply(body, 4));
        let registry = EmbedderRegistry::from_lookup(|k| match k {
            "KOSHA_EMBEDDING_PROVIDERS" => Some("http".into()),
            "KOSHA_EMBEDDING_URL" => Some(s.url.clone()),
            "KOSHA_EMBEDDING_TIMEOUT_MS" => Some("2000".into()),
            _ => None,
        });
        let sp = spec("remote", 4, None);
        let e = registry.get(&sp).unwrap();
        let v = embed_checked(e.as_ref(), &sp, &["hello"]).unwrap();
        assert_eq!(v[0][0], 5.0);
        assert!(Arc::ptr_eq(&e, &registry.get(&sp).unwrap()), "built once");
    }
}
