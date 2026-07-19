//! Read path / BM25 query engine (DESIGN.md §8, implementation plan Epic 5):
//! manifest fetch → segment pruning via footer stats → postings
//! intersection/union → BM25 scoring → filter → sort → top-K merge.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use kosha_core::{
    Bm25Params, FieldType, FilterClause, FilterStore, KoshaError, Manifest, NamespaceId,
    RangeBound, ScoredDocument, SearchQuery, SearchResult,
};
use kosha_segment::{tokenize, SegmentReader};

// ─── BM25 scorer ────────────────────────────────────────────────────────────

pub struct Bm25Scorer {
    num_docs: u32,
    avg_field_length: f64,
    params: Bm25Params,
}

impl Bm25Scorer {
    pub fn new(num_docs: u32, avg_field_length: f64, params: Bm25Params) -> Self {
        Self { num_docs, avg_field_length, params }
    }

    pub fn score_term(&self, term_frequency: u32, doc_frequency: u32, doc_field_length: u32) -> f64 {
        let n = self.num_docs as f64;
        let df = doc_frequency as f64;
        let tf = term_frequency as f64;
        let doc_len = doc_field_length as f64;
        let avgdl = self.avg_field_length;
        let k1 = self.params.k1;
        let b = self.params.b;

        if tf == 0.0 || df == 0.0 || n == 0.0 { return 0.0; }

        let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
        let tf_component = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * doc_len / avgdl));
        idf * tf_component
    }
}

// ─── Filter applier ─────────────────────────────────────────────────────────

pub struct FilterApplier;

impl FilterApplier {
    /// Apply a filter clause to a set of doc_seq values.
    /// Returns the subset of doc_seqs that pass the filter.
    pub fn apply(
        clause: &FilterClause,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        match clause {
            FilterClause::Term { term } => Self::apply_term(term, store, candidates),
            FilterClause::Terms { terms } => Self::apply_terms(terms, store, candidates),
            FilterClause::Range { range } => Self::apply_range(range, store, candidates),
            FilterClause::Bool { bool: b } => Self::apply_bool(b, store, candidates),
            FilterClause::MatchAll { .. } => Ok(candidates.clone()),
        }
    }

    fn apply_term(
        term: &HashMap<String, String>,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut result = HashSet::new();
        for (field, value) in term {
            let matching = Self::match_string_field(store, field, value, candidates);
            result.extend(matching);
        }
        Ok(result)
    }

    fn apply_terms(
        terms: &HashMap<String, Vec<String>>,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut result = HashSet::new();
        for (field, values) in terms {
            let value_set: HashSet<&str> = values.iter().map(|s| s.as_str()).collect();
            if let Some(entries) = store.string_fields.get(field) {
                for &(doc_seq, ref val) in entries {
                    if candidates.contains(&doc_seq) && value_set.contains(val.as_str()) {
                        result.insert(doc_seq);
                    }
                }
            }
        }
        Ok(result)
    }

    fn apply_range(
        range: &HashMap<String, RangeBound>,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut result = HashSet::new();
        for (field, bound) in range {
            // Try integers first, then floats, then strings.
            if let Some(entries) = store.integer_fields.get(field) {
                for &(doc_seq, val) in entries {
                    if candidates.contains(&doc_seq) && Self::check_i64_bound(val, bound) {
                        result.insert(doc_seq);
                    }
                }
            } else if let Some(entries) = store.float_fields.get(field) {
                for &(doc_seq, val) in entries {
                    if candidates.contains(&doc_seq) && Self::check_f64_bound(val, bound) {
                        result.insert(doc_seq);
                    }
                }
            } else if let Some(entries) = store.string_fields.get(field) {
                for &(doc_seq, ref val) in entries {
                    if candidates.contains(&doc_seq) && Self::check_str_bound(val, bound) {
                        result.insert(doc_seq);
                    }
                }
            }
        }
        Ok(result)
    }

    fn apply_bool(
        b: &kosha_core::BoolFilter,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut working: Option<HashSet<u32>> = None;

        // must: doc must pass ALL must clauses.
        if !b.must.is_empty() {
            let mut acc = candidates.clone();
            for clause in &b.must {
                let passed = Self::apply(clause, store, &acc)?;
                acc = passed;
            }
            working = Some(acc);
        }

        // filter: same semantics as must (score-neutral).
        // Sage uses "filter" in bool queries. We handle it via the same logic.
        // In ES, "filter" clauses are in a separate field. Our JSON structure
        // puts them in `filter`, but in the BoolFilter, must covers both must+filter.
        // The caller is responsible for putting filter clauses in must.

        // must_not: doc must pass NONE of the must_not clauses.
        if !b.must_not.is_empty() {
            let base = working.take().unwrap_or_else(|| candidates.clone());
            let mut excluded = HashSet::new();
            for clause in &b.must_not {
                let bad = Self::apply(clause, store, &base)?;
                excluded.extend(bad);
            }
            working = Some(base.difference(&excluded).copied().collect());
        }

        // should: doc must pass at least minimum_should_match.
        if !b.should.is_empty() {
            let base = working.take().unwrap_or_else(|| candidates.clone());
            let mut scores: HashMap<u32, usize> = HashMap::new();
            for clause in &b.should {
                let passed = Self::apply(clause, store, &base)?;
                for doc_seq in passed {
                    *scores.entry(doc_seq).or_default() += 1;
                }
            }
            let min = b.minimum_should_match;
            let passed: HashSet<u32> = scores.into_iter()
                .filter(|(_, count)| *count >= min)
                .map(|(doc_seq, _)| doc_seq)
                .collect();
            // Should clauses with no must/filter: the bool result is the should match.
            working = Some(if base.is_empty() { passed }
                else { base.intersection(&passed).copied().collect() });
        }

        Ok(working.unwrap_or_else(|| candidates.clone()))
    }

    fn match_string_field(store: &FilterStore, field: &str, value: &str, candidates: &HashSet<u32>) -> HashSet<u32> {
        let mut result = HashSet::new();
        if let Some(entries) = store.string_fields.get(field) {
            for &(doc_seq, ref val) in entries {
                if candidates.contains(&doc_seq) && val == value {
                    result.insert(doc_seq);
                }
            }
        }
        result
    }

    fn check_i64_bound(val: i64, bound: &RangeBound) -> bool {
        if let Some(ref gte) = bound.gte {
            if let Ok(b) = gte.parse::<i64>() { if val < b { return false; } }
        }
        if let Some(ref gt) = bound.gt {
            if let Ok(b) = gt.parse::<i64>() { if val <= b { return false; } }
        }
        if let Some(ref lte) = bound.lte {
            if let Ok(b) = lte.parse::<i64>() { if val > b { return false; } }
        }
        if let Some(ref lt) = bound.lt {
            if let Ok(b) = lt.parse::<i64>() { if val >= b { return false; } }
        }
        true
    }

    fn check_f64_bound(val: f64, bound: &RangeBound) -> bool {
        if let Some(ref gte) = bound.gte {
            if let Ok(b) = gte.parse::<f64>() { if val < b { return false; } }
        }
        if let Some(ref gt) = bound.gt {
            if let Ok(b) = gt.parse::<f64>() { if val <= b { return false; } }
        }
        if let Some(ref lte) = bound.lte {
            if let Ok(b) = lte.parse::<f64>() { if val > b { return false; } }
        }
        if let Some(ref lt) = bound.lt {
            if let Ok(b) = lt.parse::<f64>() { if val >= b { return false; } }
        }
        true
    }

    fn check_str_bound(val: &str, bound: &RangeBound) -> bool {
        // String comparison works for ISO 8601 dates and most use cases.
        if let Some(ref gte) = bound.gte { if val < gte.as_str() { return false; } }
        if let Some(ref gt) = bound.gt { if val <= gt.as_str() { return false; } }
        if let Some(ref lte) = bound.lte { if val > lte.as_str() { return false; } }
        if let Some(ref lt) = bound.lt { if val >= lt.as_str() { return false; } }
        true
    }
}

// ─── Highlight applier ──────────────────────────────────────────────────────

pub fn apply_highlight(text: &str, query_terms: &[String], pre_tag: &str, post_tag: &str) -> String {
    let mut result = text.to_string();
    for term in query_terms {
        let lower = term.to_lowercase();
        if let Some(start) = result.to_lowercase().find(&lower) {
            let end = start + lower.len();
            let before = &result[..start];
            let matched = &result[start..end];
            let after = &result[end..];
            result = format!("{}{}{}{}{}", before, pre_tag, matched, post_tag, after);
        }
    }
    result
}

// ─── Searcher ───────────────────────────────────────────────────────────────

pub struct Searcher {
    data_dir: PathBuf,
}

impl Searcher {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    pub fn search(
        &self,
        namespace: &NamespaceId,
        manifest: &Manifest,
        query: &SearchQuery,
    ) -> Result<SearchResult, KoshaError> {
        if manifest.segments.is_empty() {
            return Ok(SearchResult { results: Vec::new(), total_hits: 0 });
        }

        let query_terms = tokenize(&query.query_text);
        if query_terms.is_empty() && query.filter.is_none() {
            return Ok(SearchResult { results: Vec::new(), total_hits: 0 });
        }

        // If no query text but there's a filter, we still need to search —
        // score everything and let the filter pick.
        let has_only_filter = query_terms.is_empty() && query.filter.is_some();
        let mut all_results: Vec<ScoredDocument> = Vec::new();
        let mut all_doc_seqs: HashSet<u32> = HashSet::new();

        for entry in &manifest.segments {
            let seg_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
            if !seg_dir.exists() { continue; }

            let reader = SegmentReader::open(seg_dir)?;
            let store = reader.filter_store();
            let total_docs = reader.doc_count();

            if has_only_filter {
                let scorer = Bm25Scorer::new(total_docs, 1.0, query.bm25_params.clone());
                let all_candidates: HashSet<u32> = (0..total_docs).collect();
                let passed = FilterApplier::apply(
                    query.filter.as_ref().unwrap(), store, &all_candidates,
                )?;
                for doc_seq in passed {
                    if let Some(doc_rec) = reader.doc_record(doc_seq) {
                        let score = scorer.score_term(1, total_docs, doc_rec.field_length);
                        all_results.push(ScoredDocument {
                            doc_id: doc_rec.doc_id.clone(),
                            score,
                            fields: doc_rec.fields.clone(),
                            highlights: None,
                        });
                    }
                }
            } else {
                // Normal BM25 scoring.
                let scorer = Bm25Scorer::new(
                    total_docs, reader.avg_field_length(), reader.bm25_params().clone(),
                );

                let mut doc_frequencies: HashMap<String, u32> = HashMap::new();
                for term in &query_terms {
                    doc_frequencies.insert(term.clone(),
                        reader.postings(term).map(|p| p.len() as u32).unwrap_or(0));
                }

                let mut scored_in_segment: HashMap<u32, f64> = HashMap::new();

                for term in &query_terms {
                    if let Some(postings) = reader.postings(term) {
                        let df = doc_frequencies.get(term).copied().unwrap_or(0);
                        for posting in postings {
                            let doc_rec = match reader.doc_record(posting.doc_id) {
                                Some(d) => d, None => continue,
                            };
                            let score = scorer.score_term(
                                posting.term_frequency, df, doc_rec.field_length,
                            );
                            *scored_in_segment.entry(posting.doc_id).or_insert(0.0) += score;
                        }
                    }
                }

                for (doc_seq, score) in scored_in_segment {
                    if let Some(doc_rec) = reader.doc_record(doc_seq) {
                        all_results.push(ScoredDocument {
                            doc_id: doc_rec.doc_id.clone(),
                            score,
                            fields: doc_rec.fields.clone(),
                            highlights: None,
                        });
                        all_doc_seqs.insert(doc_seq);
                    }
                }

                // Apply filter.
                if let Some(ref clause) = query.filter {
                    let passed = FilterApplier::apply(clause, store, &all_doc_seqs)?;
                    all_results.retain(|r| {
                        (0..total_docs).any(|s| {
                            reader.doc_record(s)
                                .map_or(false, |d| d.doc_id == r.doc_id && passed.contains(&s))
                        })
                    });
                }
            }
        }

        // Apply highlighting.
        if let Some(ref highlight) = query.highlight {
            if !query_terms.is_empty() {
                let pre = highlight.pre_tags.first().map(|s| s.as_str()).unwrap_or("<b>");
                let post = highlight.post_tags.first().map(|s| s.as_str()).unwrap_or("</b>");
                for result in &mut all_results {
                    let mut highlights = Vec::new();
                    for field in &result.fields {
                        if field.name == highlight.field && field.field_type == FieldType::Text {
                            highlights.push(apply_highlight(&field.value, &query_terms, pre, post));
                        }
                    }
                    if !highlights.is_empty() {
                        result.highlights = Some(highlights);
                    }
                }
            }
        }

        // Sort.
        if !query.sort.is_empty() {
            // Multi-field sort: primary sort field first, secondary next, etc.
            let sort_specs = &query.sort;
            all_results.sort_by(|a, b| {
                for spec in sort_specs {
                    for (field, order) in &spec.fields {
                        let ord = match field.as_str() {
                            "_score" => b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal),
                            _ => {
                                let a_val = a.fields.iter().find(|f| f.name == *field).map(|f| &f.value);
                                let b_val = b.fields.iter().find(|f| f.name == *field).map(|f| &f.value);
                                let cmp = a_val.cmp(&b_val);
                                if order.order == "desc" { cmp.reverse() } else { cmp }
                            }
                        };
                        if ord != std::cmp::Ordering::Equal { return ord; }
                    }
                }
                std::cmp::Ordering::Equal
            });
        } else {
            // Default: sort by score descending.
            all_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        }

        let total_hits = all_results.len();

        // Apply pagination (from + max_results).
        let from = query.from.min(all_results.len());
        let to = (from + query.max_results).min(all_results.len());
        let page = all_results[from..to].to_vec();

        Ok(SearchResult { results: page, total_hits })
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
                vec![Field::text("title", text.to_string())],
            );
        }
        writer.finalize(Bm25Params::default()).unwrap();
        seg_id
    }

    fn mk_query(text: &str, max: usize) -> SearchQuery {
        SearchQuery {
            query_text: text.into(),
            max_results: max,
            from: 0,
            bm25_params: Bm25Params::default(),
            filter: None,
            sort: vec![],
            highlight: None,
        }
    }

    #[test]
    fn bm25_scorer_basic() {
        let scorer = Bm25Scorer::new(5, 10.0, Bm25Params::default());
        let score = scorer.score_term(3, 2, 8);
        assert!(score > 0.0);
        let score0 = scorer.score_term(0, 2, 8);
        assert!((score0 - 0.0).abs() < 1e-10);
    }

    #[test]
    fn filter_term_basic() {
        use kosha_core::FilterClause;
        let store = FilterStore::default();
        let mut string_fields = std::collections::HashMap::new();
        string_fields.insert("matterId".to_string(), vec![(0, "m-001".into()), (1, "m-001".into()), (2, "m-002".into())]);
        let store = FilterStore { string_fields, ..Default::default() };

        let clause: FilterClause = serde_json::from_str(r#"{"term": {"matterId": "m-001"}}"#).unwrap();
        let candidates: HashSet<u32> = [0, 1, 2].into();
        let passed = FilterApplier::apply(&clause, &store, &candidates).unwrap();
        assert_eq!(passed.len(), 2);
        assert!(passed.contains(&0));
        assert!(passed.contains(&1));
        assert!(!passed.contains(&2));
    }

    #[test]
    fn filter_terms_multi() {
        use kosha_core::FilterClause;
        let store = FilterStore {
            string_fields: HashMap::from([
                ("documentId".to_string(), vec![(0, "d1".into()), (1, "d2".into()), (2, "d3".into())]),
            ]),
            ..Default::default()
        };

        let clause: FilterClause = serde_json::from_str(r#"{"terms": {"documentId": ["d1", "d3"]}}"#).unwrap();
        let candidates: HashSet<u32> = [0, 1, 2].into();
        let passed = FilterApplier::apply(&clause, &store, &candidates).unwrap();
        assert_eq!(passed.len(), 2);
        assert!(passed.contains(&0));
        assert!(passed.contains(&2));
    }

    #[test]
    fn filter_bool_must_not() {
        use kosha_core::FilterClause;
        let store = FilterStore {
            string_fields: HashMap::from([
                ("status".to_string(), vec![(0, "active".into()), (1, "deleted".into()), (2, "active".into())]),
            ]),
            ..Default::default()
        };

        let clause: FilterClause = serde_json::from_str(
            r#"{"bool": {"must_not": [{"term": {"status": "deleted"}}]}}"#
        ).unwrap();
        let candidates: HashSet<u32> = [0, 1, 2].into();
        let passed = FilterApplier::apply(&clause, &store, &candidates).unwrap();
        assert_eq!(passed.len(), 2);
        assert!(passed.contains(&0));
        assert!(passed.contains(&2));
    }

    #[test]
    fn search_with_filter() {
        let dir = std::env::temp_dir().join("kosha-test-search-filter");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("seg-001");
        let mut writer = SegmentWriter::new(SegmentId("seg-001".into()), seg_dir);
        writer.add_document(
            DocumentId("d1".into()),
            vec![Field::text("title", "quick brown fox"), Field::keyword("matterId", "m-001")],
        );
        writer.add_document(
            DocumentId("d2".into()),
            vec![Field::text("title", "lazy dog"), Field::keyword("matterId", "m-001")],
        );
        writer.add_document(
            DocumentId("d3".into()),
            vec![Field::text("title", "quick rabbit"), Field::keyword("matterId", "m-002")],
        );
        writer.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry { segment_id: SegmentId("seg-001".into()), doc_count: 3 }],
        };
        let searcher = Searcher::new(dir.clone());

        // Search without filter → 2 hits for "quick".
        let q = mk_query("quick", 10);
        let r = searcher.search(&ns, &manifest, &q).unwrap();
        assert_eq!(r.total_hits, 2);

        // Search with filter: matterId=m-001 → still 2 hits.
        use kosha_core::FilterClause;
        let filter: FilterClause = serde_json::from_str(r#"{"term": {"matterId": "m-001"}}"#).unwrap();
        let q = SearchQuery {
            query_text: "quick".into(),
            max_results: 10, from: 0,
            bm25_params: Bm25Params::default(),
            filter: Some(filter),
            sort: vec![], highlight: None,
        };
        let r = searcher.search(&ns, &manifest, &q).unwrap();
        assert_eq!(r.total_hits, 1, "only d1 has both 'quick' and matterId=m-001");
        assert_eq!(r.results[0].doc_id.0, "d1");

        // Filter: matterId=m-002 → 1 hit (d3).
        let filter: FilterClause = serde_json::from_str(r#"{"term": {"matterId": "m-002"}}"#).unwrap();
        let q = SearchQuery {
            query_text: "quick".into(),
            max_results: 10, from: 0,
            bm25_params: Bm25Params::default(),
            filter: Some(filter),
            sort: vec![], highlight: None,
        };
        let r = searcher.search(&ns, &manifest, &q).unwrap();
        assert_eq!(r.total_hits, 1);
        assert_eq!(r.results[0].doc_id.0, "d3");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_without_query_text_uses_filter_only() {
        let dir = std::env::temp_dir().join("kosha-test-search-filter-only");
        let _ = std::fs::remove_dir_all(&dir);

        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("seg-001");
        let mut writer = SegmentWriter::new(SegmentId("seg-001".into()), seg_dir);
        writer.add_document(
            DocumentId("d1".into()),
            vec![Field::text("title", "quick brown fox"), Field::keyword("matterId", "m-001")],
        );
        writer.add_document(
            DocumentId("d2".into()),
            vec![Field::text("title", "lazy dog"), Field::keyword("matterId", "m-001")],
        );
        writer.add_document(
            DocumentId("d3".into()),
            vec![Field::text("title", "quick rabbit"), Field::keyword("matterId", "m-002")],
        );
        writer.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry { segment_id: SegmentId("seg-001".into()), doc_count: 3 }],
        };
        let searcher = Searcher::new(dir.clone());

        // Filter-only search (no query text).
        let filter: kosha_core::FilterClause = serde_json::from_str(
            r#"{"term": {"matterId": "m-001"}}"#
        ).unwrap();
        let q = SearchQuery {
            query_text: "".into(),
            max_results: 10, from: 0,
            bm25_params: Bm25Params::default(),
            filter: Some(filter),
            sort: vec![], highlight: None,
        };
        let r = searcher.search(&ns, &manifest, &q).unwrap();
        assert_eq!(r.total_hits, 2);
        let ids: Vec<&str> = r.results.iter().map(|r| r.doc_id.0.as_str()).collect();
        assert!(ids.contains(&"d1"));
        assert!(ids.contains(&"d2"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
