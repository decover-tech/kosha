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
    let mut body: serde_json::Value = serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
    // Differs per request by design; the tests compare everything else.
    if let Some(obj) = body.as_object_mut() {
        obj.remove("request_id");
    }
    (status, body)
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

#[cfg(feature = "onnx")]
#[test]
fn onnx_embeds_ingest_and_search_with_a_persisted_model() {
    exercise_onnx_embedding(serde_json::Value::Null);
}

#[cfg(feature = "onnx")]
#[test]
fn onnx_ignores_saved_stride_equal_to_its_token_budget() {
    exercise_onnx_embedding(serde_json::json!({
        "max_length": 128, "stride": 4, "strategy": "LongestFirst"
    }));
}

#[cfg(feature = "onnx")]
#[test]
fn onnx_ignores_saved_stride_above_its_token_budget() {
    exercise_onnx_embedding(serde_json::json!({
        "max_length": 128, "stride": 8, "strategy": "LongestFirst"
    }));
}

#[cfg(feature = "onnx")]
#[test]
fn onnx_ignores_saved_pair_only_truncation() {
    exercise_onnx_embedding(serde_json::json!({
        "max_length": 128, "stride": 0, "strategy": "OnlySecond"
    }));
}

#[cfg(feature = "onnx")]
fn exercise_onnx_embedding(truncation: serde_json::Value) {
    use base64::Engine;

    // ONNX opset 13: mask IDs, gather two-dimensional word vectors, and sum
    // them into a sentence vector. Red and blue have orthogonal vectors.
    let graph = base64::engine::general_purpose::STANDARD
        .decode(concat!(
            "CAg6zwIKLAoJaW5wdXRfaWRzCg5hdHRlbnRpb25fbWFzaxIKbWFza2VkX2lkcyIDTXVsCiYKB3dlaWdodHMKCm1hc2tl",
            "ZF9pZHMSB3ZlY3RvcnMiBkdhdGhlcgo/Cgd2ZWN0b3JzCgRheGVzEhJzZW50ZW5jZV9lbWJlZGRpbmciCVJlZHVjZVN1",
            "bSoPCghrZWVwZGltcxgAoAECEg53b3JkX2VtYmVkZGluZyoxCgIEAhABIiAAAAAAAAAAAAAAgD8AAAAAAAAAAAAAgD8A",
            "AAAAAAAAAEIHd2VpZ2h0cyoOCgEBEAc6AQFCBGF4ZXNaIAoOYXR0ZW50aW9uX21hc2sSDgoMCAcSCAoCCAEKAggEWhsK",
            "CWlucHV0X2lkcxIOCgwIBxIICgIIAQoCCARiJAoSc2VudGVuY2VfZW1iZWRkaW5nEg4KDAgBEggKAggBCgIIAkICEA0=",
        ))
        .unwrap();
    let dir = test_dir();
    let model_path = dir.join("model.onnx");
    let tokenizer_path = dir.join("tokenizer.json");
    std::fs::write(&model_path, graph).unwrap();
    std::fs::write(
        &tokenizer_path,
        serde_json::json!({
            "version": "1.0",
            "truncation": truncation,
            "pre_tokenizer": {"type": "Whitespace"},
            "model": {"type": "WordLevel", "unk_token": "[UNK]",
                "vocab": {"[PAD]": 0, "red": 1, "blue": 2, "[UNK]": 3}}
        })
        .to_string(),
    )
    .unwrap();
    let model_config = serde_json::json!({"words@1": {
        "model_path": model_path, "tokenizer_path": tokenizer_path, "sequence_length": 4
    }});
    let state = state_with(EmbedderRegistry::from_lookup(|key| match key {
        "KOSHA_EMBEDDING_PROVIDERS" => Some("onnx".into()),
        "KOSHA_EMBEDDING_ONNX_MODELS" => Some(model_config.to_string()),
        _ => None,
    }));
    assert_eq!(state.embedders.providers(), ["onnx"]);
    let mut schema = serde_json::json!({"fields": {"emb": {"type": "FIELD_TYPE_VECTOR",
        "embedding": {"model": "words@1", "dims": 2, "distance_metric": "cosine", "provider": "onnx"}
    }}});
    let (status, body) = call(
        &state,
        "POST /v1/namespaces",
        serde_json::json!({"namespace": "local", "schema": schema}),
    );
    assert_eq!(status, 200, "{body}");
    let (status, body) = index_texts(&state, "local", &[("first", "blue"), ("second", "red")]);
    assert_eq!(status, 200, "{body}");
    flush(&state, "local");
    let (status, body) = knn_text(&state, "local", "red red red red red");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        ids(&body).first().map(String::as_str),
        Some("second"),
        "{body}"
    );
    let (status, body) = knn_text(&state, "local", "blue");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        ids(&body).first().map(String::as_str),
        Some("first"),
        "{body}"
    );

    let manifest = state
        .indexer
        .manifest(&NamespaceId("acme/local".into()))
        .unwrap();
    let persisted = serde_json::to_value(manifest).unwrap();
    let embedding = &persisted["schema"]["fields"]["emb"]["embedding"];
    assert_eq!(embedding["model"], "words@1");
    assert_eq!(embedding["distance_metric"], "cosine");
    schema["fields"]["emb"]["embedding"]["model"] = "words@2".into();
    let (status, body) = put_schema(&state, "local", schema);
    assert_eq!(status, 409, "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("requires a reindex"),
        "{body}"
    );
    std::fs::remove_dir_all(dir).unwrap();
}

const DOCS: &[(&str, &str)] = &[
    ("shoes", "red trail running shoes"),
    ("jacket", "blue running jacket for winter"),
    ("cake", "chocolate cake recipe with cocoa"),
    ("bread", "sourdough bread recipe"),
];

#[test]
fn embedding_schema_rejects_explicit_non_vector_fields() {
    let state = state_with(EmbedderRegistry::with_mock());
    let mut rejected = Vec::new();
    for (name, type_key, text_type) in [
        ("canonical", "type", serde_json::json!("FIELD_TYPE_TEXT")),
        ("alias", "field_type", serde_json::json!("text")),
        ("number", "type", serde_json::json!(1)),
    ] {
        let mut schema = serde_json::json!({"fields": {"emb": {
            "embedding": {"model": MODEL, "dims": DIMS}
        }}});
        schema["fields"]["emb"][type_key] = text_type;
        rejected.push(call(
            &state,
            "POST /v1/namespaces",
            serde_json::json!({"namespace": name, "schema": schema}),
        ));
        rejected.push(put_schema(&state, &format!("update-{name}"), schema));
    }
    let statuses: Vec<_> = rejected.iter().map(|(status, _)| *status).collect();
    assert_eq!(statuses, [400; 6]);
    for (_, body) in rejected {
        assert!(body["error"].as_str().unwrap().contains("vector"), "{body}");
    }
}

#[test]
fn an_untyped_embedding_field_is_a_vector_and_its_canonical_schema_round_trips() {
    let schema = serde_json::json!({"fields": {"emb": {
        "embedding": {"model": MODEL, "dims": DIMS}
    }}});
    let canonical = parse_namespace_schema(&schema).unwrap();
    assert_eq!(canonical["fields"]["emb"]["type"], "FIELD_TYPE_VECTOR");
    let state = state_with(EmbedderRegistry::with_mock());
    let (status, body) = put_schema(&state, "shop", canonical);
    assert_eq!(status, 200, "{body}");
}

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
    let mut unsupported = schema("mock-a", 32);
    unsupported["fields"]["emb"]["embedding"]["distance_metric"] = "euclidean".into();
    let (status, body) = put_schema(&state, "pin", unsupported);
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"].as_str().unwrap().contains("distance_metric"),
        "{body}"
    );
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
    // Skipped by design, not lost to an error: the searcher reports no
    // failed segment, which is what keeps the answer above a 200, not a 503.
    let manifest = state.indexer.manifest(&ns).unwrap();
    let query: SearchQuery = serde_json::from_value(serde_json::json!({
        "query_text": "",
        "knn": {"field": "emb", "vector": vec![0.125f32; 64], "k": 4, "embedding_model": "mock-b"},
    }))
    .unwrap();
    let (result, phases) = state
        .searcher
        .search_with_stats(&ns, &manifest, &query, None)
        .unwrap();
    assert_eq!(result.knn_degraded_segments, Some(1));
    assert!(phases.knn_failed_segments.is_empty());
    assert!(phases.knn_corrupt_segments.is_empty());
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
    // The schema declares a model this pod has no provider for: 501
    // `not_implemented`, not retryable, on both paths (ADR-350 §b: a
    // retry-on-retryable client would otherwise loop), and nothing is
    // written.
    let state = state_with(EmbedderRegistry::default());
    assert_eq!(put_schema(&state, "noprov", schema("mock-a", 16)).0, 200);
    let (status, body) = index_texts(&state, "noprov", &DOCS[..1]);
    assert_eq!(status, 501, "{body}");
    assert_eq!(body["code"], "not_implemented", "{body}");
    assert_eq!(body["retryable"], false, "{body}");
    assert!(state
        .indexer
        .manifest(&NamespaceId("acme/noprov".into()))
        .is_none_or(|m| m.segments.is_empty()));
    let (status, body) = knn_text(&state, "noprov", "shoes");
    assert_eq!(status, 501, "{body}");
    assert_eq!(body["code"], "not_implemented", "{body}");

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
