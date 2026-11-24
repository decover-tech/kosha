//! Rerank stage (issue #251): reorder the top `rerank.top_n` first-stage
//! hits with an external model, after RRF fusion.
//!
//! The searcher never calls a provider. A reranked request runs in three
//! steps:
//!
//! 1. the searcher runs [`SearchQuery::first_stage_query`] — the top
//!    `top_n` hits from offset 0, with the rerank field returned;
//! 2. [`apply`] sends the query text and each hit's field text to a
//!    [`RerankProvider`] under `rerank.timeout_ms`;
//! 3. [`apply`] orders the hits by the provider's score, then cuts the
//!    caller's page (`from` / `max_results`) and source filter.
//!
//! A timeout or provider error never fails the request: the page keeps the
//! first-stage order and `SearchResult.rerank_degraded` is `true`.
//! Totals and aggregations are the first stage's; reranking only reorders.
//!
//! [`Searcher::search_reranked`](crate::Searcher::search_reranked) runs
//! all three. The server runs them itself so that it can release its
//! scoring slot while the provider call is in flight.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kosha_core::{FieldType, SearchQuery, SearchResult, DEFAULT_RERANK_TIMEOUT_MS};

/// What a provider is asked to score.
#[derive(Debug, Clone, PartialEq)]
pub struct RerankRequest {
    /// `rerank.model`; `None` lets the provider use its default.
    pub model: Option<String>,
    pub query: String,
    /// One text per candidate, in first-stage order.
    pub documents: Vec<String>,
}

/// A provider's answer: a relevance score per document index.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RerankResponse {
    /// `(index into RerankRequest.documents, relevance score)`. Every
    /// index must appear exactly once.
    pub scores: Vec<(usize, f64)>,
    /// Units the provider billed for the call, when it reports them
    /// (Cohere: `meta.billed_units.search_units`).
    pub billed_units: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RerankError {
    /// The provider did not answer within the timeout.
    Timeout,
    /// Transport failure, non-2xx status or an unusable body. The message
    /// is returned to the client, so it must not contain credentials.
    Provider(String),
}

impl std::fmt::Display for RerankError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "timeout"),
            Self::Provider(msg) => write!(f, "provider error: {msg}"),
        }
    }
}

/// A reranking model behind some API.
///
/// `rerank` may block; [`apply`] runs it on its own thread and stops
/// waiting at `timeout` whatever the implementation does. Implementations
/// should still honor `timeout` themselves so that an abandoned call does
/// not hold a connection open.
pub trait RerankProvider: Send + Sync {
    /// The name requests use (`rerank.provider`) and metrics are keyed by.
    fn name(&self) -> &str;
    fn rerank(
        &self,
        request: &RerankRequest,
        timeout: Duration,
    ) -> Result<RerankResponse, RerankError>;
}

/// How a provider call ended, for metrics and logs.
#[derive(Debug, Clone, PartialEq)]
pub enum RerankOutcome {
    Ok,
    Timeout,
    Error(String),
}

/// One provider call: what [`apply`] did, for per-provider metrics.
#[derive(Debug, Clone, PartialEq)]
pub struct RerankCall {
    pub provider: String,
    pub latency: Duration,
    pub documents: usize,
    pub outcome: RerankOutcome,
    pub billed_units: Option<f64>,
}

/// The text sent to the provider for one hit: every value of `field`,
/// joined by newlines. A hit without the field is sent as `""` (it still
/// needs a score so that the page can be ordered).
fn field_text(fields: &[kosha_core::Field], field: &str) -> String {
    fields
        .iter()
        .filter(|f| f.name == field && f.field_type != FieldType::Vector)
        .map(|f| f.value.as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Run `provider` under `timeout`. The call runs on its own thread so that
/// a provider that ignores its timeout cannot hold the request past it.
fn call_with_timeout(
    provider: &Arc<dyn RerankProvider>,
    request: RerankRequest,
    timeout: Duration,
) -> Result<RerankResponse, RerankError> {
    let (tx, rx) = mpsc::sync_channel(1);
    let worker = Arc::clone(provider);
    let spawned = std::thread::Builder::new()
        .name("kosha-rerank".into())
        .spawn(move || {
            let _ = tx.send(worker.rerank(&request, timeout));
        });
    if let Err(e) = spawned {
        return Err(RerankError::Provider(format!("could not start call: {e}")));
    }
    match rx.recv_timeout(timeout) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => Err(RerankError::Timeout),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err(RerankError::Provider("provider call panicked".into()))
        }
    }
}

/// Check that `response` scores each of `n` documents exactly once with a
/// finite score, and return the scores by document index.
fn scores_by_index(response: &RerankResponse, n: usize) -> Result<Vec<f64>, RerankError> {
    let mut scores = vec![None; n];
    for &(index, score) in &response.scores {
        if !score.is_finite() {
            return Err(RerankError::Provider(format!(
                "non-finite score for document {index}"
            )));
        }
        match scores.get_mut(index) {
            Some(slot @ None) => *slot = Some(score),
            Some(Some(_)) => {
                return Err(RerankError::Provider(format!(
                    "document {index} scored twice"
                )))
            }
            None => {
                return Err(RerankError::Provider(format!(
                    "document index {index} out of range (sent {n})"
                )))
            }
        }
    }
    scores
        .into_iter()
        .enumerate()
        .map(|(i, s)| s.ok_or_else(|| RerankError::Provider(format!("no score for document {i}"))))
        .collect()
}

/// Rerank a first-stage result and cut the caller's page from it.
///
/// `first_stage` must be the result of `query.first_stage_query()`.
/// Returns the result unchanged (and no call) when `query` has no
/// `rerank`. With no candidates the provider is not called.
///
/// On success each hit has `score` = `rerank_score` = the provider's
/// score and `fused_score` = its first-stage score, ordered by the
/// provider's score (ties keep first-stage order). On timeout or error
/// the hits keep first-stage order and score, `fused_score` is set,
/// `rerank_score` is absent and `rerank_degraded` is `true`.
pub fn apply(
    query: &SearchQuery,
    mut first_stage: SearchResult,
    provider: &Arc<dyn RerankProvider>,
) -> (SearchResult, Option<RerankCall>) {
    let Some(ref spec) = query.rerank else {
        return (first_stage, None);
    };
    let mut hits = std::mem::take(&mut first_stage.results);
    for hit in &mut hits {
        hit.fused_score = Some(hit.score);
        hit.rerank_score = None;
    }

    let mut call = None;
    let mut degraded_reason = None;
    if !hits.is_empty() {
        let request = RerankRequest {
            model: spec.model.clone(),
            query: query.rerank_query_text().unwrap_or_default().to_string(),
            documents: hits
                .iter()
                .map(|h| field_text(&h.fields, &spec.field))
                .collect(),
        };
        let n = request.documents.len();
        let timeout = Duration::from_millis(spec.timeout_ms.unwrap_or(DEFAULT_RERANK_TIMEOUT_MS));
        let started = Instant::now();
        let result = call_with_timeout(provider, request, timeout)
            .and_then(|response| Ok((scores_by_index(&response, n)?, response.billed_units)));
        let latency = started.elapsed();
        let (outcome, billed_units) = match result {
            Ok((scores, billed_units)) => {
                for (hit, score) in hits.iter_mut().zip(scores) {
                    hit.score = score;
                    hit.rerank_score = Some(score);
                }
                // Stable: equal scores keep first-stage order.
                hits.sort_by(|a, b| b.score.total_cmp(&a.score));
                (RerankOutcome::Ok, billed_units)
            }
            Err(RerankError::Timeout) => {
                degraded_reason = Some(format!("timeout after {} ms", timeout.as_millis()));
                (RerankOutcome::Timeout, None)
            }
            Err(e) => {
                degraded_reason = Some(e.to_string());
                (RerankOutcome::Error(e.to_string()), None)
            }
        };
        call = Some(RerankCall {
            provider: provider.name().to_string(),
            latency,
            documents: n,
            outcome,
            billed_units,
        });
    }

    let page: Vec<_> = hits
        .into_iter()
        .skip(query.from)
        .take(query.max_results)
        .map(|mut hit| {
            // The first stage returned the rerank field whatever the
            // source filter said; apply the caller's filter now.
            if query.source_includes.is_some() || query.source_excludes.is_some() {
                hit.fields.retain(|f| query.returns_field(&f.name));
            }
            hit
        })
        .collect();
    first_stage.results = page;
    first_stage.rerank_degraded = Some(degraded_reason.is_some());
    first_stage.rerank_degraded_reason = degraded_reason;
    (first_stage, call)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, ScoredDocument, TotalHitsRelation};
    use std::sync::Mutex;

    /// Scores each document by how often `term` occurs in it; records the
    /// requests it saw. Optionally sleeps first or fails.
    pub(crate) struct MockProvider {
        pub term: String,
        pub delay: Duration,
        pub fail: Option<String>,
        pub seen: Mutex<Vec<RerankRequest>>,
    }

    impl MockProvider {
        pub(crate) fn counting(term: &str) -> Self {
            Self {
                term: term.into(),
                delay: Duration::ZERO,
                fail: None,
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl RerankProvider for MockProvider {
        fn name(&self) -> &str {
            "mock"
        }
        fn rerank(
            &self,
            request: &RerankRequest,
            _timeout: Duration,
        ) -> Result<RerankResponse, RerankError> {
            self.seen.lock().unwrap().push(request.clone());
            std::thread::sleep(self.delay);
            if let Some(ref msg) = self.fail {
                return Err(RerankError::Provider(msg.clone()));
            }
            Ok(RerankResponse {
                scores: request
                    .documents
                    .iter()
                    .enumerate()
                    .map(|(i, d)| (i, d.matches(self.term.as_str()).count() as f64))
                    .collect(),
                billed_units: Some(1.0),
            })
        }
    }

    struct Fixed(Vec<(usize, f64)>);
    impl RerankProvider for Fixed {
        fn name(&self) -> &str {
            "fixed"
        }
        fn rerank(&self, _: &RerankRequest, _: Duration) -> Result<RerankResponse, RerankError> {
            Ok(RerankResponse {
                scores: self.0.clone(),
                billed_units: None,
            })
        }
    }

    fn first_stage(texts: &[&str]) -> SearchResult {
        SearchResult {
            results: texts
                .iter()
                .enumerate()
                .map(|(i, t)| ScoredDocument {
                    doc_id: DocumentId(format!("d{i}")),
                    score: 1.0 / (i + 1) as f64,
                    fields: vec![Field::text("body", *t), Field::keyword("tag", "x")],
                    highlights: None,
                    rank_details: None,
                    fused_score: None,
                    rerank_score: None,
                    highlight: Default::default(),
                })
                .collect(),
            total_hits: 42,
            total_hits_relation: TotalHitsRelation::Eq,
            aggregations: None,
            knn_degraded_segments: None,
            rerank_degraded: None,
            rerank_degraded_reason: None,
            knn_mode: None,
            knn_filtered_candidates: None,
            exhaustive_search_count: Default::default(),
            consistency_fallback: None,
        }
    }

    fn query(json: &str) -> SearchQuery {
        serde_json::from_str(json).unwrap()
    }

    fn ids(r: &SearchResult) -> Vec<&str> {
        r.results.iter().map(|h| h.doc_id.0.as_str()).collect()
    }

    #[test]
    fn reorders_pages_and_reports_both_scores() {
        let q = query(
            r#"{"query_text": "notice", "from": 1, "max_results": 2,
                "source_excludes": ["body"],
                "rerank": {"provider": "mock", "field": "body", "model": "m1", "top_n": 4}}"#,
        );
        let mock = Arc::new(MockProvider::counting("x"));
        let provider: Arc<dyn RerankProvider> = mock.clone();
        let (r, call) = apply(&q, first_stage(&["a", "x x x", "x", "x x"]), &provider);
        // Provider order: d1 (3), d3 (2), d2 (1), d0 (0); page = [1, 3).
        assert_eq!(ids(&r), ["d3", "d2"]);
        assert_eq!(r.results[0].rerank_score, Some(2.0));
        assert_eq!(r.results[0].score, 2.0);
        assert_eq!(r.results[0].fused_score, Some(0.25));
        assert_eq!(r.results[1].fused_score, Some(1.0 / 3.0));
        assert_eq!(r.rerank_degraded, Some(false));
        assert_eq!(r.rerank_degraded_reason, None);
        assert_eq!(r.total_hits, 42, "totals are the first stage's");
        // The caller excluded the field the reranker read.
        assert!(r
            .results
            .iter()
            .all(|h| h.fields.iter().all(|f| f.name != "body")));
        let seen = mock.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].query, "notice");
        assert_eq!(seen[0].model.as_deref(), Some("m1"));
        assert_eq!(seen[0].documents, ["a", "x x x", "x", "x x"]);
        let call = call.unwrap();
        assert_eq!(
            (
                call.provider.as_str(),
                call.documents,
                call.outcome,
                call.billed_units
            ),
            ("mock", 4, RerankOutcome::Ok, Some(1.0))
        );
    }

    #[test]
    fn ties_keep_first_stage_order() {
        let q = query(r#"{"query_text": "q", "rerank": {"provider": "mock", "field": "body"}}"#);
        let provider: Arc<dyn RerankProvider> = Arc::new(MockProvider::counting("x"));
        let (r, _) = apply(&q, first_stage(&["x", "a", "x", "b"]), &provider);
        assert_eq!(ids(&r), ["d0", "d2", "d1", "d3"]);
    }

    #[test]
    fn timeout_returns_fused_order_with_degraded_flag() {
        let q = query(
            r#"{"query_text": "q", "max_results": 3,
                "rerank": {"provider": "mock", "field": "body", "timeout_ms": 50}}"#,
        );
        let provider: Arc<dyn RerankProvider> = Arc::new(MockProvider {
            delay: Duration::from_secs(5),
            ..MockProvider::counting("x")
        });
        let started = Instant::now();
        let (r, call) = apply(&q, first_stage(&["a", "x", "x x"]), &provider);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "did not wait for the provider: {:?}",
            started.elapsed()
        );
        assert_eq!(ids(&r), ["d0", "d1", "d2"]);
        assert_eq!(r.rerank_degraded, Some(true));
        assert_eq!(
            r.rerank_degraded_reason.as_deref(),
            Some("timeout after 50 ms")
        );
        for hit in &r.results {
            assert_eq!(hit.fused_score, Some(hit.score));
            assert_eq!(hit.rerank_score, None);
        }
        let call = call.unwrap();
        assert_eq!(call.outcome, RerankOutcome::Timeout);
        assert!(call.latency >= Duration::from_millis(50));
    }

    #[test]
    fn provider_errors_and_bad_answers_degrade() {
        let q = query(r#"{"query_text": "q", "rerank": {"provider": "p", "field": "body"}}"#);
        let texts = ["a", "b", "c"];
        let failing: Arc<dyn RerankProvider> = Arc::new(MockProvider {
            fail: Some("HTTP 429".into()),
            ..MockProvider::counting("x")
        });
        let bad_answers: Vec<Arc<dyn RerankProvider>> = vec![
            failing,
            Arc::new(Fixed(vec![(0, 1.0), (1, 2.0)])), // missing one
            Arc::new(Fixed(vec![(0, 1.0), (1, 2.0), (1, 3.0)])), // duplicate
            Arc::new(Fixed(vec![(0, 1.0), (1, 2.0), (7, 3.0)])), // out of range
            Arc::new(Fixed(vec![(0, 1.0), (1, f64::NAN), (2, 0.0)])),
        ];
        for provider in bad_answers {
            let (r, call) = apply(&q, first_stage(&texts), &provider);
            assert_eq!(
                ids(&r),
                ["d0", "d1", "d2"],
                "{:?}",
                r.rerank_degraded_reason
            );
            assert_eq!(r.rerank_degraded, Some(true));
            assert!(r
                .rerank_degraded_reason
                .unwrap()
                .starts_with("provider error:"));
            assert!(matches!(call.unwrap().outcome, RerankOutcome::Error(_)));
        }
    }

    #[test]
    fn no_candidates_no_call_and_no_rerank_is_a_no_op() {
        let q = query(r#"{"query_text": "q", "rerank": {"provider": "mock", "field": "body"}}"#);
        let mock = Arc::new(MockProvider::counting("x"));
        let provider: Arc<dyn RerankProvider> = mock.clone();
        let (r, call) = apply(&q, first_stage(&[]), &provider);
        assert!(r.results.is_empty() && call.is_none());
        assert_eq!(r.rerank_degraded, Some(false));
        assert!(mock.seen.lock().unwrap().is_empty());

        let plain = query(r#"{"query_text": "q"}"#);
        let (r, call) = apply(&plain, first_stage(&["a", "x"]), &provider);
        assert!(call.is_none());
        assert_eq!(r.rerank_degraded, None);
        assert_eq!(r.results[0].fused_score, None);
    }

    #[test]
    fn field_text_joins_values_and_skips_vectors() {
        let fields = vec![
            Field::text("body", "one"),
            Field::vector("body", vec![1.0]),
            Field::text("body", "two"),
            Field::text("other", "no"),
        ];
        assert_eq!(field_text(&fields, "body"), "one\ntwo");
        assert_eq!(field_text(&fields, "missing"), "");
    }
}
