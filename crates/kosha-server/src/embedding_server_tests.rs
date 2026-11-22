//! End-to-end tests of native embedding (#209) through the HTTP handlers,
//! with the deterministic mock embedder.

use super::embedding::{EmbedError, Embedder, EmbedderRegistry, MockEmbedder};
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const MODEL: &str = "mock-words";
const DIMS: u32 = 256;

/// [`MockEmbedder`] that counts the texts it embeds.
struct Counting {
    inner: MockEmbedder,
    texts: AtomicUsize,
    calls: AtomicUsize,
}

impl Counting {
    fn new(model: &str, dims: u32) -> Arc<Self> {
        Arc::new(Self {
            inner: MockEmbedder::new(model, dims as usize),
            texts: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
        })
    }
}

impl Embedder for Counting {
    fn model_id(&self) -> &str {
        self.inner.model_id()
    }
    fn dims(&self) -> usize {
        self.inner.dims()
    }
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.texts.fetch_add(texts.len(), Ordering::SeqCst);
        self.inner.embed(texts)
    }
}

fn test_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-test-embedding-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn state_with(registry: EmbedderRegistry) -> AppState {
    let mut state = AppState::new(test_dir());
    state.embedders = registry;
    state
}

fn call(state: &AppState, line: &str, body: serde_json::Value) -> (u16, serde_json::Value) {
    let response = route(
        &format!("{line} HTTP/1.1\r\n"),
        &HashMap::new(),
        &serde_json::to_vec(&body).unwrap(),
        "acme",
        state,
    );
    let status: u16 = response[9..12].parse().unwrap();
    let body = &response[response.find("\r\n\r\n").unwrap() + 4..];
    (
        status,
        serde_json::from_str(body).unwrap_or(serde_json::Value::Null),
    )
}

fn schema(model: &str, dims: u32) -> serde_json::Value {
    serde_json::json!({"fields": {"emb": {"type": "FIELD_TYPE_VECTOR",
        "embedding": {"model": model, "dims": dims, "provider": "mock"}}}})
}

fn put_schema(state: &AppState, ns: &str, schema: serde_json::Value) -> (u16, serde_json::Value) {
    call(state, &format!("POST /v1/namespaces/{ns}/schema"), schema)
}

/// Index `docs` as (id, text), the text sent for the embedded field `emb`
/// (as a Text value, the way the Python client sends a string).
fn index_texts(state: &AppState, ns: &str, docs: &[(&str, &str)]) -> (u16, serde_json::Value) {
    let documents: Vec<serde_json::Value> = docs
        .iter()
        .map(|(id, text)| {
            serde_json::json!({"id": id, "fields": [
                {"name": "title", "field_type": "Text", "value": text},
                {"name": "emb", "field_type": "Text", "value": text}]})
        })
        .collect();
    call(
        state,
        &format!("POST /v1/namespaces/{ns}/documents"),
        serde_json::json!({ "documents": documents }),
    )
}

fn flush(state: &AppState, ns: &str) {
    let id = NamespaceId(format!("acme/{ns}"));
    state.indexer.flush_namespace(&id).unwrap();
    state.publish_namespace(&id).unwrap();
}

fn knn_text(state: &AppState, ns: &str, text: &str) -> (u16, serde_json::Value) {
    call(
        state,
        &format!("POST /v1/namespaces/{ns}/search"),
        serde_json::json!({"query_text": "", "no_cache": true, "consistency": "strong",
            "knn": {"field": "emb", "query_text": text, "k": 4}}),
    )
}

fn ids(body: &serde_json::Value) -> Vec<String> {
    body["results"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|hit| hit["doc_id"].as_str().unwrap().to_string())
        .collect()
}

const DOCS: &[(&str, &str)] = &[
    ("shoes", "red trail running shoes"),
    ("jacket", "blue running jacket for winter"),
    ("cake", "chocolate cake recipe with cocoa"),
    ("bread", "sourdough bread recipe"),
];

#[test]
fn text_is_embedded_at_ingest_and_query_text_finds_neighbours() {
    let counting = Counting::new(MODEL, DIMS);
    let state = state_with(EmbedderRegistry::default().with_embedder(counting.clone()));
    let (status, body) = put_schema(&state, "shop", schema(MODEL, DIMS));
    assert_eq!(status, 200, "{body}");

    let (status, body) = index_texts(&state, "shop", DOCS);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        counting.calls.load(Ordering::SeqCst),
        1,
        "one batch per request"
    );
    assert_eq!(counting.texts.load(Ordering::SeqCst), DOCS.len());

    // Before a flush: the unindexed tail answers.
    let (status, body) = knn_text(&state, "shop", "running shoes");
    assert_eq!(status, 200, "{body}");
    let before = ids(&body);
    assert_eq!(&before[..2], ["shoes", "jacket"], "{body}");
    let (_, body) = knn_text(&state, "shop", "chocolate cocoa cake recipe");
    assert_eq!(ids(&body)[0], "cake", "{body}");

    flush(&state, "shop");
    let (status, body) = knn_text(&state, "shop", "running shoes");
    assert_eq!(status, 200, "{body}");
    assert_eq!(ids(&body), before, "same order from the segment: {body}");
    assert_eq!(body["knn_degraded_segments"], 0, "{body}");
    let (_, body) = knn_text(&state, "shop", "bread recipe");
    assert_eq!(&ids(&body)[..2], ["bread", "cake"], "{body}");

    // The segment records the model its vectors came from.
    let ns = NamespaceId("acme/shop".into());
    let manifest = state.indexer.manifest(&ns).unwrap();
    let entry = &manifest.segments[0];
    let footer = kosha_segment::SegmentReader::read_footer(&manifest.segment_dir(
        &state.data_dir,
        &ns,
        &entry.segment_id,
    ))
    .unwrap();
    let meta = &footer.vector_fields.unwrap()[0];
    assert_eq!((meta.name.as_str(), meta.dim), ("emb", DIMS));
    assert_eq!(meta.embedding_model.as_deref(), Some(MODEL));

    // A supplied vector of the declared dimension is taken as is; another
    // dimension is the client's error.
    let mut vector = vec![0f32; DIMS as usize];
    vector[0] = 1.0;
    let (status, body) = call(
        &state,
        "POST /v1/namespaces/shop/documents",
        serde_json::json!({"documents": [{"id": "raw", "fields": [
            {"name": "emb", "field_type": "Vector", "value": serde_json::to_string(&vector).unwrap()}]}]}),
    );
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(
        &state,
        "POST /v1/namespaces/shop/documents",
        serde_json::json!({"documents": [{"id": "bad", "fields": [
            {"name": "emb", "field_type": "Vector", "value": "[1.0, 2.0]"}]}]}),
    );
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        counting.texts.load(Ordering::SeqCst),
        DOCS.len() + 4,
        "supplied vectors are never embedded; four query texts were"
    );

    // query_text and vector together is a 400; query_text against a field
    // the schema declares no model for is a 400.
    let (status, _) = call(
        &state,
        "POST /v1/namespaces/shop/search",
        serde_json::json!({"query_text": "", "knn": {"field": "emb", "query_text": "x", "vector": [1.0]}}),
    );
    assert_eq!(status, 400);
    let (status, body) = call(
        &state,
        "POST /v1/namespaces/shop/search",
        serde_json::json!({"query_text": "", "knn": {"field": "other", "query_text": "x"}}),
    );
    assert_eq!(status, 400, "{body}");
}

#[test]
fn changing_the_model_of_an_indexed_field_is_a_conflict() {
    let state = state_with(EmbedderRegistry::with_mock());
    // No data yet: any change is allowed.
    assert_eq!(put_schema(&state, "pin", schema("mock-a", 64)).0, 200);
    assert_eq!(put_schema(&state, "pin", schema("mock-b", 64)).0, 200);
    assert_eq!(put_schema(&state, "pin", schema("mock-a", 32)).0, 200);

    // Buffered (unflushed) documents pin the model too.
    assert_eq!(index_texts(&state, "pin", &DOCS[..2]).0, 200);
    let (status, body) = put_schema(&state, "pin", schema("mock-b", 32));
    assert_eq!(status, 409, "{body}");

    flush(&state, "pin");
    let (status, body) = put_schema(&state, "pin", schema("mock-b", 32));
    assert_eq!(status, 409, "{body}");
    let message = body["error"].as_str().unwrap_or_default().to_string();
    assert!(
        message.contains("\"emb\"") && message.contains("mock-a") && message.contains("mock-b"),
        "{body}"
    );
    let (status, body) = put_schema(&state, "pin", schema("mock-a", 64));
    assert_eq!(status, 409, "a dims change too: {body}");
    // Same model again, or another provider for it: fine.
    let (status, body) = put_schema(&state, "pin", schema("mock-a", 32));
    assert_eq!((status, &body["changed"]), (200, &serde_json::json!(false)));
    let schema_text = serde_json::json!({"fields": {"emb": {"type": "FIELD_TYPE_VECTOR",
        "embedding": {"model": "mock-a", "dims": 32}}}});
    assert_eq!(put_schema(&state, "pin", schema_text).0, 200);

    // Invalid declarations are a 400.
    let (status, _) = put_schema(&state, "pin", schema("", 32));
    assert_eq!(status, 400);
    let (status, _) = put_schema(&state, "pin", schema("mock-a", 5000));
    assert_eq!(status, 400);
}

#[test]
fn a_segment_of_another_model_is_skipped_and_counted_degraded() {
    let state = state_with(EmbedderRegistry::with_mock());
    assert_eq!(put_schema(&state, "mix", schema("mock-a", 64)).0, 200);
    assert_eq!(index_texts(&state, "mix", DOCS).0, 200);
    flush(&state, "mix");
    // A manifest whose schema names another model than its segment
    // recorded (e.g. written by a node that skipped the check): queries
    // must not compare the two spaces.
    let ns = NamespaceId("acme/mix".into());
    let mut manifest = state.indexer.manifest(&ns).unwrap();
    manifest.schema = Some(serde_json::from_value(schema("mock-b", 64)).unwrap());
    manifest.version += 1;
    state.indexer.restore_manifest(ns.clone(), manifest);
    let (status, body) = knn_text(&state, "mix", "running shoes");
    assert_eq!(status, 200, "{body}");
    assert!(ids(&body).is_empty(), "{body}");
    assert_eq!(body["knn_degraded_segments"], 1, "{body}");
}

#[test]
fn a_degraded_knn_answer_is_not_result_cached_or_fully_cached() {
    let mut state = state_with(EmbedderRegistry::with_mock());
    state.dashboard = dashboard::Dashboard::new(true);
    assert_eq!(put_schema(&state, "deg", schema("mock-a", 64)).0, 200);
    assert_eq!(index_texts(&state, "deg", DOCS).0, 200);
    flush(&state, "deg");
    // As in the test above: the segment's model no longer matches, so
    // every kNN search skips it and counts it degraded.
    let ns = NamespaceId("acme/deg".into());
    let mut manifest = state.indexer.manifest(&ns).unwrap();
    manifest.schema = Some(serde_json::from_value(schema("mock-b", 64)).unwrap());
    manifest.version += 1;
    state.indexer.restore_manifest(ns.clone(), manifest);
    // Result cache on (no `no_cache`), timings requested: the shape the
    // bench warmup gate polls with.
    let search = || {
        call(
            &state,
            "POST /v1/namespaces/deg/search",
            serde_json::json!({"query_text": "", "timings": true,
                "knn": {"field": "emb", "query_text": "running shoes", "k": 4}}),
        )
    };
    for attempt in 0..2 {
        let (status, body) = search();
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["knn_degraded_segments"], 1, "{attempt}: {body}");
        // The repeat is searched again, not served from the result cache,
        // and neither reads as fully cached.
        assert_eq!(body["timings"]["cached"], false, "{attempt}: {body}");
        assert_eq!(body["timings"]["fully_cached"], false, "{attempt}: {body}");
    }
}

#[test]
fn namespaces_without_a_model_are_untouched() {
    let counting = Counting::new(MODEL, DIMS);
    let plain = state_with(EmbedderRegistry::default());
    let with_embedder = state_with(
        EmbedderRegistry::with_mock().with_embedder(counting.clone() as Arc<dyn Embedder>),
    );
    let docs = serde_json::json!({"documents": [
        {"id": "a", "fields": [{"name": "title", "field_type": "Text", "value": "alpha beta"},
            {"name": "emb", "field_type": "Vector", "value": "[1.0, 0.0, 0.0]"}]},
        {"id": "b", "fields": [{"name": "title", "field_type": "Text", "value": "beta gamma"},
            {"name": "emb", "field_type": "Vector", "value": "[0.0, 1.0, 0.0]"}]}]});
    let mut responses = Vec::new();
    for state in [&plain, &with_embedder] {
        // A schema with analyzers only, as namespaces have today.
        let analysis = serde_json::json!({"fields": {"title": {"analyzer": ["standard"]}}});
        let mut out = vec![put_schema(state, "plain", analysis)];
        out.push(call(
            state,
            "POST /v1/namespaces/plain/documents",
            docs.clone(),
        ));
        flush(state, "plain");
        for query in [
            serde_json::json!({"query_text": "beta", "no_cache": true}),
            serde_json::json!({"query_text": "", "no_cache": true,
                "knn": {"field": "emb", "vector": [0.9, 0.1, 0.0], "k": 2}}),
        ] {
            out.push(call(state, "POST /v1/namespaces/plain/search", query));
        }
        // query_text on a field with no declared model: a 400 either way.
        out.push(call(
            state,
            "POST /v1/namespaces/plain/search",
            serde_json::json!({"query_text": "", "knn": {"field": "emb", "query_text": "alpha"}}),
        ));
        responses.push(out);
    }
    assert_eq!(responses[0], responses[1]);
    assert_eq!(responses[0][3].1["results"][0]["doc_id"], "a");
    assert_eq!(responses[0][4].0, 400);
    assert_eq!(
        counting.calls.load(Ordering::SeqCst),
        0,
        "no embedder calls"
    );
}

#[test]
fn embedding_errors_map_to_status_codes() {
    // The schema declares a model this pod has no provider for: 503 on
    // both paths, and nothing is written.
    let state = state_with(EmbedderRegistry::default());
    assert_eq!(put_schema(&state, "noprov", schema("mock-a", 16)).0, 200);
    let (status, body) = index_texts(&state, "noprov", &DOCS[..1]);
    assert_eq!(status, 503, "{body}");
    assert!(state
        .indexer
        .manifest(&NamespaceId("acme/noprov".into()))
        .is_none_or(|m| m.segments.is_empty()));
    let (status, body) = knn_text(&state, "noprov", "shoes");
    assert_eq!(status, 503, "{body}");

    // An http provider that cannot be reached: 503, never a 500.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let state = state_with(EmbedderRegistry::from_lookup(|k| match k {
        "KOSHA_EMBEDDING_PROVIDERS" => Some("http".into()),
        "KOSHA_EMBEDDING_URL" => Some(url.clone()),
        "KOSHA_EMBEDDING_TIMEOUT_MS" => Some("500".into()),
        _ => None,
    }));
    let http_schema =
        serde_json::json!({"fields": {"emb": {"embedding": {"model": "remote", "dims": 8}}}});
    assert_eq!(put_schema(&state, "down", http_schema).0, 200);
    let (status, body) = index_texts(&state, "down", &DOCS[..1]);
    assert_eq!(status, 503, "{body}");
}
