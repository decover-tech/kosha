//! Read path / BM25 query engine (DESIGN.md §8, implementation plan Epic 5):
//! manifest fetch → segment pruning via footer stats → postings
//! intersection/union → BM25 scoring → top-K merge across segments.
//!
//! Phase 1 is lexical-only; the semantic branch, RRF fusion, and rerank hook
//! are Phase 2.

use std::collections::HashMap;
use std::path::PathBuf;

use kosha_core::{
    Bm25Params, KoshaError, Manifest, NamespaceId, ScoredDocument, SearchQuery, SearchResult,
};
use kosha_segment::{tokenize, SegmentReader};

// ─── BM25 scorer ────────────────────────────────────────────────────────────

/// BM25 scoring implementation.
///
/// Standard formula:
///
///   score(D, Q) = Σ IDF(t) · (tf(t,D) · (k₁ + 1)) / (tf(t,D) + k₁ · (1 - b + b · |D| / avgdl))
///
/// where:
///   IDF(t) = log(1 + (N - df(t) + 0.5) / (df(t) + 0.5))
///   tf(t,D) = frequency of term t in document D
///   |D| = length of document D (in tokens)
///   avgdl = average document length across the corpus
///   N = total number of documents in the corpus
///   df(t) = number of documents containing term t
///   k₁, b = BM25 tuning parameters
pub struct Bm25Scorer {
    num_docs: u32,
    avg_field_length: f64,
    params: Bm25Params,
}

impl Bm25Scorer {
    pub fn new(num_docs: u32, avg_field_length: f64, params: Bm25Params) -> Self {
        Self {
            num_docs,
            avg_field_length,
            params,
        }
    }

    /// Compute the BM25 score for a single term in a document.
    pub fn score_term(
        &self,
        term_frequency: u32,
        doc_frequency: u32,
        doc_field_length: u32,
    ) -> f64 {
        let n = self.num_docs as f64;
        let df = doc_frequency as f64;
        let tf = term_frequency as f64;
        let doc_len = doc_field_length as f64;
        let avgdl = self.avg_field_length;
        let k1 = self.params.k1;
        let b = self.params.b;

        if tf == 0.0 || df == 0.0 || n == 0.0 {
            return 0.0;
        }

        // IDF(t) = log(1 + (N - df + 0.5) / (df + 0.5))
        let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();

        // TF component: (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / avgdl))
        let tf_component = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * doc_len / avgdl));

        idf * tf_component
    }

    /// Score a document against all query terms given their term frequencies
    /// in that document.
    pub fn score(
        &self,
        query_term_frequencies: &HashMap<String, u32>,
        doc_frequencies: &HashMap<String, u32>,
        doc_field_length: u32,
    ) -> f64 {
        let mut total_score = 0.0;
        for (term, &tf) in query_term_frequencies {
            if tf == 0 {
                continue;
            }
            let df = doc_frequencies.get(term).copied().unwrap_or(0);
            total_score += self.score_term(tf, df, doc_field_length);
        }
        total_score
    }
}

// ─── Searcher ───────────────────────────────────────────────────────────────

/// Executes BM25 search queries across segments on disk.
pub struct Searcher {
    /// Root data directory.
    data_dir: PathBuf,
}

impl Searcher {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Execute a search query against the given namespace.
    ///
    /// Loads all segments listed in the manifest, scores each document,
    /// and returns the top-K results.
    pub fn search(
        &self,
        namespace: &NamespaceId,
        manifest: &Manifest,
        query: &SearchQuery,
    ) -> Result<SearchResult, KoshaError> {
        if manifest.segments.is_empty() {
            return Ok(SearchResult {
                results: Vec::new(),
                total_hits: 0,
            });
        }

        // Parse the query into terms.
        let query_terms = tokenize(&query.query_text);
        if query_terms.is_empty() {
            return Ok(SearchResult {
                results: Vec::new(),
                total_hits: 0,
            });
        }

        // Count query term frequency (how many times each term appears in the
        // query — usually 1 for simple queries, but could be >1 for repeated).
        let mut query_term_freqs: HashMap<String, u32> = HashMap::new();
        for term in &query_terms {
            *query_term_freqs.entry(term.clone()).or_default() += 1;
        }

        let mut all_results: Vec<ScoredDocument> = Vec::new();

        for entry in &manifest.segments {
            let seg_dir = self
                .data_dir
                .join(&namespace.0)
                .join(&entry.segment_id.0);
            if !seg_dir.exists() {
                continue;
            }

            let reader = SegmentReader::open(seg_dir)?;

            let scorer = Bm25Scorer::new(
                reader.doc_count(),
                reader.avg_field_length(),
                reader.bm25_params().clone(),
            );

            // Build doc_frequencies for each query term.
            let mut doc_frequencies: HashMap<String, u32> = HashMap::new();
            for term in &query_terms {
                if let Some(postings) = reader.postings(term) {
                    doc_frequencies.insert(term.clone(), postings.len() as u32);
                } else {
                    doc_frequencies.insert(term.clone(), 0);
                }
            }

            // Score each document that matches at least one query term.
            // We track which documents we've already scored to avoid duplicates
            // within this segment.
            let mut scored_in_segment: HashMap<u32, f64> = HashMap::new();

            for term in &query_terms {
                if let Some(postings) = reader.postings(term) {
                    let query_tf = query_term_freqs.get(term).copied().unwrap_or(1);
                    let query_tf_map: HashMap<String, u32> =
                        [(term.clone(), query_tf)].into_iter().collect();

                    for posting in postings {
                        let doc_rec = match reader.doc_record(posting.doc_id) {
                            Some(d) => d,
                            None => continue,
                        };

                        let score = scorer.score(
                            &query_tf_map,
                            &doc_frequencies,
                            doc_rec.field_length,
                        );

                        *scored_in_segment.entry(posting.doc_id).or_insert(0.0) += score;
                    }
                }
            }

            // Convert to ScoredDocument.
            for (doc_seq, score) in scored_in_segment {
                if let Some(doc_rec) = reader.doc_record(doc_seq) {
                    all_results.push(ScoredDocument {
                        doc_id: doc_rec.doc_id.clone(),
                        score,
                        fields: doc_rec.fields.clone(),
                    });
                }
            }
        }

        // Sort by score descending, take top-K.
        all_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        let total_hits = all_results.len();
        all_results.truncate(query.max_results);

        Ok(SearchResult {
            results: all_results,
            total_hits,
        })
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, ManifestEntry, SegmentId};
    use kosha_segment::SegmentWriter;

    fn build_test_segment(
        dir: &std::path::Path,
        namespace: &str,
        seg_id: &str,
        docs: Vec<(&str, &str)>,
    ) -> SegmentId {
        let seg_id = SegmentId(seg_id.to_string());
        let seg_dir = dir.join(namespace).join(seg_id.0.as_str());
        let mut writer = SegmentWriter::new(seg_id.clone(), seg_dir);

        for (id, text) in docs {
            writer.add_document(
                DocumentId(id.to_string()),
                vec![Field {
                    name: "title".into(),
                    text: text.to_string(),
                }],
            );
        }

        writer.finalize(Bm25Params::default()).unwrap();
        seg_id
    }

    #[test]
    fn bm25_scorer_basic() {
        let scorer = Bm25Scorer::new(5, 10.0, Bm25Params::default());

        // A term appearing in 2 of 5 docs, tf=3, doc_len=8.
        let score = scorer.score_term(3, 2, 8);
        assert!(score > 0.0, "BM25 score should be positive");

        // Zero tf = zero score.
        let score0 = scorer.score_term(0, 2, 8);
        assert!((score0 - 0.0).abs() < 1e-10);
    }

    #[test]
    fn search_single_segment() {
        let dir = std::env::temp_dir().join("kosha-test-search-001");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let seg_id = build_test_segment(
            &dir,
            &ns.0,
            "seg-001",
            vec![
                ("doc1", "quick brown fox"),
                ("doc2", "lazy dog"),
                ("doc3", "quick rabbit"),
            ],
        );

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: seg_id,
                doc_count: 3,
            }],
        };

        let searcher = Searcher::new(dir.clone());
        let query = SearchQuery {
            query_text: "quick".into(),
            max_results: 10,
            bm25_params: Bm25Params::default(),
        };

        let result = searcher.search(&ns, &manifest, &query).unwrap();
        assert_eq!(result.total_hits, 2); // doc1 and doc3 contain "quick"
        assert_eq!(result.results.len(), 2);

        // doc1 and doc3 should both be present.
        let doc_ids: Vec<&str> = result.results.iter().map(|r| r.doc_id.0.as_str()).collect();
        assert!(doc_ids.contains(&"doc1"));
        assert!(doc_ids.contains(&"doc3"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_multi_term() {
        let dir = std::env::temp_dir().join("kosha-test-search-002");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let seg_id = build_test_segment(
            &dir,
            &ns.0,
            "seg-002",
            vec![
                ("doc1", "quick brown fox"),
                ("doc2", "lazy brown dog"),
                ("doc3", "quick brown rabbit"),
            ],
        );

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: seg_id,
                doc_count: 3,
            }],
        };

        let searcher = Searcher::new(dir.clone());

        // Search for "brown quick" → matches doc1 and doc3 (both have both terms).
        let query = SearchQuery {
            query_text: "brown quick".into(),
            max_results: 10,
            bm25_params: Bm25Params::default(),
        };

        let result = searcher.search(&ns, &manifest, &query).unwrap();
        assert_eq!(result.total_hits, 3); // all three have at least one term
        assert_eq!(result.results.len(), 3);

        // doc3 has both "brown" and "quick", doc1 has both, doc2 only has "brown"
        // so doc1 and doc3 should score higher than doc2.
        assert!(result.results[0].score > result.results[2].score);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_empty_query_returns_no_results() {
        let dir = std::env::temp_dir().join("kosha-test-search-003");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let seg_id = build_test_segment(&dir, &ns.0, "seg-003", vec![("doc1", "hello world")]);
        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: seg_id,
                doc_count: 1,
            }],
        };

        let searcher = Searcher::new(dir.clone());
        let query = SearchQuery {
            query_text: "".into(),
            max_results: 10,
            bm25_params: Bm25Params::default(),
        };

        let result = searcher.search(&ns, &manifest, &query).unwrap();
        assert_eq!(result.total_hits, 0);
        assert!(result.results.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_top_k_limits_results() {
        let dir = std::env::temp_dir().join("kosha-test-search-004");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let seg_id = build_test_segment(
            &dir,
            &ns.0,
            "seg-004",
            vec![
                ("doc1", "apple"),
                ("doc2", "apple banana"),
                ("doc3", "apple banana cherry"),
            ],
        );

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: seg_id,
                doc_count: 3,
            }],
        };

        let searcher = Searcher::new(dir.clone());

        let query = SearchQuery {
            query_text: "apple".into(),
            max_results: 2,
            bm25_params: Bm25Params::default(),
        };

        let result = searcher.search(&ns, &manifest, &query).unwrap();
        assert_eq!(result.total_hits, 3);
        assert_eq!(result.results.len(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
