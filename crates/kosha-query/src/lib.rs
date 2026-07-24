use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use kosha_core::{
    AggBucket, AggBucketResult, AggCompositeBucket, AggCompositeResult, AggMetricResult,
    Aggregation, AggregationResults, Bm25Params, FieldType, FilterClause, FilterStore, KoshaError,
    Manifest, NamespaceId, ScoredDocument, SearchQuery, SearchResult, SortSpec,
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
        Self {
            num_docs,
            avg_field_length,
            params,
        }
    }

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
        let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
        let tf_component = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * doc_len / avgdl));
        idf * tf_component
    }
}

// ─── Wildcard matcher ───────────────────────────────────────────────────────

pub fn wildcard_terms(terms: &[&str], pattern: &str, case_insensitive: bool) -> Vec<String> {
    terms
        .iter()
        .filter(|t| simple_wildcard_match(t, pattern, case_insensitive))
        .map(|t| t.to_string())
        .collect()
}

fn simple_wildcard_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let text = if case_insensitive {
        text.to_lowercase()
    } else {
        text.to_string()
    };
    let pattern = if case_insensitive {
        pattern.to_lowercase()
    } else {
        pattern.to_string()
    };

    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut ti = 0;
    let mut pi = 0;
    let mut backtrack_t = 0usize;
    let mut backtrack_p = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '*' {
            backtrack_t = ti;
            backtrack_p = pi;
            pi += 1;
        } else if backtrack_p < p.len() {
            backtrack_t += 1;
            ti = backtrack_t;
            pi = backtrack_p + 1;
        } else {
            return false;
        }
    }

    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi >= p.len()
}

// ─── Match phrase scorer ────────────────────────────────────────────────────

pub fn match_phrase_score(
    postings_list: &[Vec<u32>], // positions for each query term, per doc
    slop: u32,
) -> f64 {
    if postings_list.is_empty() {
        return 0.0;
    }
    if postings_list.len() == 1 {
        return 1.0;
    }

    let first_positions = &postings_list[0];
    for &start_pos in first_positions {
        let mut matched = true;
        for (i, positions) in postings_list[1..].iter().enumerate() {
            let expected = start_pos + (i as u32) + 1;
            let found = positions.iter().any(|&p| {
                let dist = p.abs_diff(expected);
                dist <= slop
            });
            if !found {
                matched = false;
                break;
            }
        }
        if matched {
            return 1.0;
        }
    }
    0.0
}

// ─── Filter applier ─────────────────────────────────────────────────────────

pub struct FilterApplier;

impl FilterApplier {
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
            if let Some(entries) = store.string_fields.get(field) {
                for &(doc_seq, ref val) in entries {
                    if candidates.contains(&doc_seq) && val == value {
                        result.insert(doc_seq);
                    }
                }
            }
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
        range: &HashMap<String, kosha_core::RangeBound>,
        store: &FilterStore,
        candidates: &HashSet<u32>,
    ) -> Result<HashSet<u32>, KoshaError> {
        let mut result = HashSet::new();
        for (field, bound) in range {
            if let Some(entries) = store.integer_fields.get(field) {
                for &(doc_seq, val) in entries {
                    if candidates.contains(&doc_seq) && check_i64(val, bound) {
                        result.insert(doc_seq);
                    }
                }
            } else if let Some(entries) = store.float_fields.get(field) {
                for &(doc_seq, val) in entries {
                    if candidates.contains(&doc_seq) && check_f64(val, bound) {
                        result.insert(doc_seq);
                    }
                }
            } else if let Some(entries) = store.string_fields.get(field) {
                for &(doc_seq, ref val) in entries {
                    if candidates.contains(&doc_seq) && check_str(val, bound) {
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

        if !b.must.is_empty() {
            let mut acc = candidates.clone();
            for clause in &b.must {
                acc = Self::apply(clause, store, &acc)?;
            }
            working = Some(acc);
        }
        if !b.must_not.is_empty() {
            let base = working.take().unwrap_or_else(|| candidates.clone());
            let mut excluded = HashSet::new();
            for clause in &b.must_not {
                excluded.extend(Self::apply(clause, store, &base)?);
            }
            working = Some(base.difference(&excluded).copied().collect());
        }
        if !b.should.is_empty() {
            let base = working.take().unwrap_or_else(|| candidates.clone());
            let mut scores: HashMap<u32, usize> = HashMap::new();
            for clause in &b.should {
                for doc_seq in Self::apply(clause, store, &base)? {
                    *scores.entry(doc_seq).or_default() += 1;
                }
            }
            let passed: HashSet<u32> = scores
                .into_iter()
                .filter(|(_, c)| *c >= b.minimum_should_match)
                .map(|(d, _)| d)
                .collect();
            working = Some(if base.is_empty() {
                passed
            } else {
                base.intersection(&passed).copied().collect()
            });
        }
        Ok(working.unwrap_or_else(|| candidates.clone()))
    }
}

fn check_i64(val: i64, bound: &kosha_core::RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if let Ok(b) = gte.parse::<i64>() {
            if val < b {
                return false;
            }
        }
    }
    if let Some(ref gt) = bound.gt {
        if let Ok(b) = gt.parse::<i64>() {
            if val <= b {
                return false;
            }
        }
    }
    if let Some(ref lte) = bound.lte {
        if let Ok(b) = lte.parse::<i64>() {
            if val > b {
                return false;
            }
        }
    }
    if let Some(ref lt) = bound.lt {
        if let Ok(b) = lt.parse::<i64>() {
            if val >= b {
                return false;
            }
        }
    }
    true
}
fn check_f64(val: f64, bound: &kosha_core::RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if let Ok(b) = gte.parse::<f64>() {
            if val < b {
                return false;
            }
        }
    }
    if let Some(ref gt) = bound.gt {
        if let Ok(b) = gt.parse::<f64>() {
            if val <= b {
                return false;
            }
        }
    }
    if let Some(ref lte) = bound.lte {
        if let Ok(b) = lte.parse::<f64>() {
            if val > b {
                return false;
            }
        }
    }
    if let Some(ref lt) = bound.lt {
        if let Ok(b) = lt.parse::<f64>() {
            if val >= b {
                return false;
            }
        }
    }
    true
}
fn check_str(val: &str, bound: &kosha_core::RangeBound) -> bool {
    if let Some(ref gte) = bound.gte {
        if val < gte.as_str() {
            return false;
        }
    }
    if let Some(ref gt) = bound.gt {
        if val <= gt.as_str() {
            return false;
        }
    }
    if let Some(ref lte) = bound.lte {
        if val > lte.as_str() {
            return false;
        }
    }
    if let Some(ref lt) = bound.lt {
        if val >= lt.as_str() {
            return false;
        }
    }
    true
}

// ─── Highlight applier ──────────────────────────────────────────────────────

pub fn apply_highlight(
    text: &str,
    query_terms: &[String],
    pre_tag: &str,
    post_tag: &str,
) -> String {
    let mut result = text.to_string();
    for term in query_terms {
        let lower = term.to_lowercase();
        if let Some(start) = result.to_lowercase().find(&lower) {
            let (before, rest) = result.split_at(start);
            let (matched, after) = rest.split_at(lower.len());
            result = format!("{}{}{}{}{}", before, pre_tag, matched, post_tag, after);
        }
    }
    result
}

// ─── Searcher ───────────────────────────────────────────────────────────────

// ─── Cosine similarity ─────────────────────────────────────────────────────

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    (dot / (norm_a * norm_b)) as f64
}

/// Flat kNN search: compute cosine similarity against all stored vectors,
/// return top-K (doc_seq, score) pairs.
pub fn flat_knn(query_vector: &[f32], vectors: &[(u32, Vec<f32>)], k: usize) -> Vec<(u32, f64)> {
    let mut scores: Vec<(u32, f64)> = vectors
        .iter()
        .map(|(doc_seq, vec)| (*doc_seq, cosine_similarity(query_vector, vec)))
        .collect();
    scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scores.truncate(k);
    scores
}

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
        tombstones: Option<
            &std::collections::HashMap<kosha_core::SegmentId, std::collections::HashSet<u32>>,
        >,
    ) -> Result<SearchResult, KoshaError> {
        if manifest.segments.is_empty() {
            return Ok(SearchResult {
                results: Vec::new(),
                total_hits: 0,
                aggregations: None,
            });
        }

        let query_terms = tokenize(&query.query_text);

        let is_tombstoned = |seg_id: &kosha_core::SegmentId, doc_seq: u32| -> bool {
            tombstones.is_some_and(|t| t.get(seg_id).is_some_and(|seqs| seqs.contains(&doc_seq)))
        };
        let has_query =
            !query_terms.is_empty() || query.wildcard.is_some() || query.match_phrase.is_some();
        let has_only_filter = !has_query && query.filter.is_some();

        let mut all_results: Vec<ScoredDocument> = Vec::new();
        let mut all_aggs: HashMap<String, AggregationResults> = HashMap::new();

        for entry in &manifest.segments {
            let seg_dir = self.data_dir.join(&namespace.0).join(&entry.segment_id.0);
            if !seg_dir.exists() {
                continue;
            }

            let reader = SegmentReader::open(seg_dir)?;
            let total_docs = reader.doc_count();
            let store = &reader.filter_store;
            let scorer = Bm25Scorer::new(
                total_docs,
                reader.avg_field_length(),
                reader.bm25_params().clone(),
            );

            // ── Wildcard matching ──
            let is_wildcard_mode = query.wildcard.is_some();
            let effective_terms = if let Some(ref wc) = query.wildcard {
                let all_terms: Vec<&str> = reader.all_terms();
                wildcard_terms(&all_terms, &wc.pattern, wc.case_insensitive)
            } else if !query_terms.is_empty() {
                query_terms.clone()
            } else {
                Vec::new()
            };

            // ── Match phrase ──
            let phrase_match = if let Some(ref mp) = query.match_phrase {
                let phrase_terms = tokenize(&mp.phrase);
                Some((phrase_terms, mp.slop))
            } else {
                None
            };

            let phrase_tokenized = phrase_match.as_ref().map(|p| &p.0);

            // ── BM25 scoring with positions for phrase ──
            if !effective_terms.is_empty() || phrase_tokenized.is_some() {
                let terms_for_bm25 = if let Some(ref pt) = phrase_tokenized {
                    pt
                } else {
                    &effective_terms
                };

                let term_postings: Vec<(&str, &[kosha_core::Posting])> = terms_for_bm25
                    .iter()
                    .filter_map(|t| reader.postings(t).map(|p| (t.as_str(), p)))
                    .collect();

                let mut doc_frequencies: HashMap<&str, u32> = HashMap::new();
                for (t, p) in &term_postings {
                    doc_frequencies.insert(t, p.len() as u32);
                }

                // ── Postings AND/OR: AND for multi-term queries, OR for wildcard ──
                let mut scored: HashMap<u32, f64> = HashMap::new();
                let use_and =
                    term_postings.len() > 1 && !is_wildcard_mode && phrase_tokenized.is_none();

                if !use_and {
                    // OR mode (wildcard, phrase, or single term): score any matching doc.
                    for (term, postings) in &term_postings {
                        let df = doc_frequencies.get(term).copied().unwrap_or(0);
                        for posting in *postings {
                            if let Some(doc_rec) = reader.doc_record(posting.doc_id) {
                                let score = scorer.score_term(
                                    posting.term_frequency,
                                    df,
                                    doc_rec.field_length,
                                );
                                *scored.entry(posting.doc_id).or_insert(0.0) += score;
                            }
                        }
                    }
                } else {
                    // Multi-term: intersect doc_ids, then score intersection.
                    // Start with the shortest postings list for efficiency.
                    let mut candidates: Vec<(u32, Vec<Vec<u32>>)> = {
                        let shortest = term_postings.iter().min_by_key(|(_, p)| p.len()).unwrap();
                        shortest.1.iter().map(|p| (p.doc_id, Vec::new())).collect()
                    };
                    let _doc_freqs: HashMap<u32, HashMap<&str, u32>> = HashMap::new();

                    // For each candidate doc, verify it appears in ALL other postings lists.
                    for (_term, postings) in &term_postings {
                        let term_docs: HashMap<u32, &kosha_core::Posting> =
                            postings.iter().map(|p| (p.doc_id, p)).collect();
                        candidates.retain(|(doc_id, _)| term_docs.contains_key(doc_id));
                        if candidates.is_empty() {
                            break;
                        }
                        // Store positions for phrase matching.
                        for (doc_id, positions) in &mut candidates {
                            if let Some(posting) = term_docs.get(doc_id) {
                                positions.push(posting.positions.clone());
                            }
                        }
                    }

                    // Score surviving candidates.
                    for (doc_id, _doc_positions) in &candidates {
                        if let Some(doc_rec) = reader.doc_record(*doc_id) {
                            let mut total_score = 0.0;
                            for (term, postings) in &term_postings {
                                let df = doc_frequencies.get(term).copied().unwrap_or(0);
                                let posting = postings.iter().find(|p| p.doc_id == *doc_id);
                                if let Some(p) = posting {
                                    total_score += scorer.score_term(
                                        p.term_frequency,
                                        df,
                                        doc_rec.field_length,
                                    );
                                }
                            }
                            scored.insert(*doc_id, total_score);
                        }
                    }
                }

                // Apply phrase matching (filter out docs that don't match the phrase).
                if let Some((ref phrase_terms, slop)) = phrase_match {
                    let doc_ids: Vec<u32> = scored.keys().copied().collect();
                    for doc_id in doc_ids {
                        let mut term_positions: Vec<Vec<u32>> = Vec::new();
                        for pt in phrase_terms {
                            if let Some(postings) = reader.postings(pt) {
                                if let Some(p) = postings.iter().find(|p| p.doc_id == doc_id) {
                                    term_positions.push(p.positions.clone());
                                }
                            }
                        }
                        if term_positions.len() < phrase_terms.len() {
                            // Not all phrase terms appear in this doc.
                            scored.remove(&doc_id);
                            continue;
                        }
                        let phrase_score = match_phrase_score(&term_positions, slop);
                        if phrase_score == 0.0 {
                            scored.remove(&doc_id);
                        } else {
                            // Boost the score for matching the phrase.
                            *scored.get_mut(&doc_id).unwrap() *= 1.0 + phrase_score * 0.5;
                        }
                    }
                }

                // Apply filter to this segment's scored docs *before* merging
                // into all_results. Filtering all_results inside the segment
                // loop previously dropped hits from earlier segments.
                let passed_filter: Option<HashSet<u32>> = if let Some(ref clause) = query.filter {
                    let candidates: HashSet<u32> = scored.keys().copied().collect();
                    Some(FilterApplier::apply(clause, store, &candidates)?)
                } else {
                    None
                };

                for (doc_seq, score) in scored {
                    if is_tombstoned(&entry.segment_id, doc_seq) {
                        continue;
                    }
                    if let Some(ref passed) = passed_filter {
                        if !passed.contains(&doc_seq) {
                            continue;
                        }
                    }
                    if let Some(doc_rec) = reader.doc_record(doc_seq) {
                        all_results.push(ScoredDocument {
                            doc_id: doc_rec.doc_id.clone(),
                            score,
                            fields: doc_rec.fields.clone(),
                            highlights: None,
                        });
                    }
                }
            } else if has_only_filter {
                let all_candidates: HashSet<u32> = (0..total_docs).collect();
                let passed =
                    FilterApplier::apply(query.filter.as_ref().unwrap(), store, &all_candidates)?;
                for doc_seq in passed {
                    if is_tombstoned(&entry.segment_id, doc_seq) {
                        continue;
                    }
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
            }

            // ── kNN search (HNSW when available, flat fallback) ──
            if let Some(ref knn) = query.knn {
                if !reader.vector_store.vectors.is_empty() {
                    let knn_results: Vec<(u32, f64)> = if let Some(ref hnsw) = reader.hnsw_map {
                        let query_point = kosha_segment::CosinePoint(knn.vector.clone());
                        let mut search = instant_distance::Search::default();
                        hnsw.search(&query_point, &mut search)
                            .take(knn.k)
                            .map(|item| (*item.value, (1.0 - item.distance as f64).max(0.0)))
                            .collect()
                    } else {
                        flat_knn(&knn.vector, &reader.vector_store.vectors, knn.k)
                    };
                    // Merge with existing BM25 results or use kNN results directly.
                    let has_bm25 = !all_results.is_empty();
                    if has_bm25 {
                        // BM25 + kNN hybrid: add kNN score as a boost factor.
                        let knn_scores: HashMap<u32, f64> = knn_results.into_iter().collect();
                        for (doc_seq, knn_score) in &knn_scores {
                            let doc_id = (0..total_docs)
                                .filter_map(|s| reader.doc_record(s))
                                .find(|d| d.doc_seq == *doc_seq)
                                .map(|d| d.doc_id.clone());
                            if let Some(ref did) = doc_id {
                                if let Some(existing) =
                                    all_results.iter_mut().find(|r| r.doc_id.0 == did.0)
                                {
                                    // Boost existing BM25 score with kNN score.
                                    existing.score = existing.score * 0.5 + knn_score * 0.5 * 100.0;
                                }
                            }
                        }
                    } else {
                        // Pure kNN search.
                        for (doc_seq, score) in knn_results {
                            if is_tombstoned(&entry.segment_id, doc_seq) {
                                continue;
                            }
                            if let Some(doc_rec) = reader.doc_record(doc_seq) {
                                all_results.push(ScoredDocument {
                                    doc_id: doc_rec.doc_id.clone(),
                                    score: (score + 1.0) * 10.0, // scale to BM25-like range
                                    fields: doc_rec.fields.clone(),
                                    highlights: None,
                                });
                            }
                        }
                    }
                }
            }

            // ── Aggregations ──
            for (agg_name, agg) in &query.aggs {
                match agg {
                    Aggregation::Terms { terms } => {
                        let result = compute_single_aggregation(store, &terms.field);
                        all_aggs.insert(agg_name.clone(), result);
                    }
                    Aggregation::Cardinality { cardinality } => {
                        let result = compute_cardinality(store, &cardinality.field);
                        all_aggs.insert(agg_name.clone(), result);
                    }
                    Aggregation::Composite { composite } => {
                        let result = compute_composite(store, composite);
                        all_aggs.insert(agg_name.clone(), result);
                    }
                }
            }
        }

        // ── Highlighting ──
        if let Some(ref highlight) = query.highlight {
            if !query_terms.is_empty() {
                let pre = highlight
                    .pre_tags
                    .first()
                    .map(|s| s.as_str())
                    .unwrap_or("<b>");
                let post = highlight
                    .post_tags
                    .first()
                    .map(|s| s.as_str())
                    .unwrap_or("</b>");
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

        // ── Sort ──
        if !query.sort.is_empty() {
            all_results.sort_by(|a, b| compare_sort_keys(a, b, &query.sort));
        } else {
            all_results.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.doc_id.0.cmp(&b.doc_id.0))
            });
        }

        // ── search_after (cursor pagination) ──
        // Apply after sorting so the cursor advances in sort order. When
        // search_after is set, `from` is ignored (OpenSearch semantics).
        let mut page_start = query.from.min(all_results.len());
        if let Some(ref after) = query.search_after {
            if !after.is_empty() {
                page_start = all_results
                    .iter()
                    .position(|r| is_strictly_after_cursor(r, after, &query.sort))
                    .unwrap_or(all_results.len());
            }
        }

        let total_hits = all_results.len();
        let from = page_start.min(all_results.len());
        let to = (from + query.max_results).min(all_results.len());
        let page = all_results[from..to].to_vec();

        // Merge aggregations across segments.
        let merged_aggs = if all_aggs.is_empty() {
            None
        } else {
            // Use the first segment's aggs (all segments have the same data).
            Some(all_aggs.into_values().next().unwrap_or(AggregationResults {
                per_document: None,
                total_documents: None,
                matched_docs: None,
                extra: HashMap::new(),
            }))
        };

        Ok(SearchResult {
            results: page,
            total_hits,
            aggregations: merged_aggs,
        })
    }
}

// ─── Aggregation functions ──────────────────────────────────────────────────

pub fn compute_single_aggregation(store: &FilterStore, field: &str) -> AggregationResults {
    let mut counts: HashMap<String, usize> = HashMap::new();

    if let Some(entries) = store.string_fields.get(field) {
        for (_, val) in entries {
            *counts.entry(val.clone()).or_default() += 1;
        }
    }

    let mut buckets: Vec<AggBucket> = counts
        .into_iter()
        .map(|(k, c)| AggBucket {
            key: k,
            doc_count: c,
        })
        .collect();
    buckets.sort_by_key(|b| std::cmp::Reverse(b.doc_count));

    AggregationResults {
        per_document: Some(AggBucketResult { buckets }),
        total_documents: None,
        matched_docs: None,
        extra: HashMap::new(),
    }
}

pub fn compute_cardinality(store: &FilterStore, field: &str) -> AggregationResults {
    let count = store
        .string_fields
        .get(field)
        .map(|entries| {
            let unique: HashSet<&str> = entries.iter().map(|(_, v)| v.as_str()).collect();
            unique.len()
        })
        .unwrap_or(0);

    AggregationResults {
        per_document: None,
        total_documents: Some(AggMetricResult { value: count }),
        matched_docs: None,
        extra: HashMap::new(),
    }
}

pub fn compute_composite(
    store: &FilterStore,
    composite: &kosha_core::AggComposite,
) -> AggregationResults {
    let mut buckets = Vec::new();
    if let Some(source) = composite.sources.first() {
        for (agg_name, terms_spec) in &source.source {
            let field = &terms_spec.terms.field;
            if let Some(entries) = store.string_fields.get(field) {
                let mut seen: HashMap<&str, usize> = HashMap::new();
                for (_, val) in entries {
                    *seen.entry(val.as_str()).or_default() += 1;
                }
                let _ = agg_name;
                for (key, count) in seen {
                    if buckets.len() >= composite.size {
                        break;
                    }
                    let mut key_map = HashMap::new();
                    key_map.insert(field.clone(), key.to_string());
                    buckets.push(AggCompositeBucket {
                        key: key_map,
                        doc_count: count,
                    });
                }
            }
        }
    }

    let after_key = buckets.last().map(|b| b.key.clone());

    AggregationResults {
        per_document: None,
        total_documents: None,
        matched_docs: Some(AggCompositeResult { buckets, after_key }),
        extra: HashMap::new(),
    }
}

fn field_sort_value<'a>(doc: &'a ScoredDocument, field: &str) -> String {
    match field {
        "_score" => format!("{}", doc.score),
        "_id" => doc.doc_id.0.clone(),
        _ => doc
            .fields
            .iter()
            .find(|f| f.name == field)
            .map(|f| f.value.clone())
            .unwrap_or_default(),
    }
}

/// True when `doc` sorts strictly after the search_after cursor.
fn is_strictly_after_cursor(doc: &ScoredDocument, after: &[String], sort: &[SortSpec]) -> bool {
    if sort.is_empty() {
        // Default ranking: score desc, then _id asc. Cursor is typically [_id]
        // when callers only paginate by id (Decover embed updater).
        if after.len() == 1 {
            return doc.doc_id.0.as_str() > after[0].as_str();
        }
        return false;
    }

    let mut idx = 0usize;
    for spec in sort {
        for (field, order) in &spec.fields {
            if idx >= after.len() {
                return true;
            }
            let val = field_sort_value(doc, field);
            let cursor = &after[idx];
            let cmp = if field == "_score" {
                let val_f: f64 = val.parse().unwrap_or(0.0);
                let cur_f: f64 = cursor.parse().unwrap_or(0.0);
                val_f
                    .partial_cmp(&cur_f)
                    .unwrap_or(std::cmp::Ordering::Equal)
            } else {
                val.as_str().cmp(cursor.as_str())
            };
            // Interpret comparison in the field's sort direction: "greater"
            // means further along the result list.
            let directed = if order.order == "desc" {
                cmp.reverse()
            } else {
                cmp
            };
            match directed {
                std::cmp::Ordering::Greater => return true,
                std::cmp::Ordering::Less => return false,
                std::cmp::Ordering::Equal => idx += 1,
            }
        }
    }
    false
}

fn compare_sort_keys(
    a: &ScoredDocument,
    b: &ScoredDocument,
    sort: &[SortSpec],
) -> std::cmp::Ordering {
    for spec in sort {
        for (field, order) in &spec.fields {
            let ord = match field.as_str() {
                "_score" => a
                    .score
                    .partial_cmp(&b.score)
                    .unwrap_or(std::cmp::Ordering::Equal),
                "_id" => a.doc_id.0.cmp(&b.doc_id.0),
                _ => {
                    let a_val = a.fields.iter().find(|f| f.name == *field).map(|f| &f.value);
                    let b_val = b.fields.iter().find(|f| f.name == *field).map(|f| &f.value);
                    a_val.cmp(&b_val)
                }
            };
            let ord = if order.order == "desc" {
                ord.reverse()
            } else {
                ord
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
    }
    a.doc_id.0.cmp(&b.doc_id.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_core::{DocumentId, Field, ManifestEntry, SegmentId, SortOrder};
    use kosha_segment::SegmentWriter;

    #[expect(dead_code)]
    fn mk_query(text: &str, max: usize) -> SearchQuery {
        SearchQuery {
            query_text: text.into(),
            max_results: max,
            from: 0,
            bm25_params: Bm25Params::default(),
            filter: None,
            sort: vec![],
            search_after: None,
            highlight: None,
            aggs: HashMap::new(),
            wildcard: None,
            match_phrase: None,
            knn: None,
        }
    }

    #[test]
    fn wildcard_matching_works() {
        let terms = vec!["hello", "world", "help", "helm", "held"];
        let matched = wildcard_terms(&terms, "hel*", true);
        assert_eq!(matched.len(), 4);
        assert!(matched.contains(&"hello".to_string()));
        assert!(matched.contains(&"help".to_string()));
        assert!(matched.contains(&"helm".to_string()));
        assert!(matched.contains(&"held".to_string()));
    }

    #[test]
    fn match_phrase_no_slop() {
        // Positions: doc has "quick" at 0, "brown" at 1, "fox" at 2.
        let postings = vec![vec![0u32], vec![1u32], vec![2u32]];
        let score = match_phrase_score(&postings, 0);
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn match_phrase_with_slop() {
        // Positions: "quick" at 0, "fox" at 2 (skipping "brown").
        let postings = vec![vec![0u32], vec![2u32]];
        let score = match_phrase_score(&postings, 1);
        assert!((score - 1.0).abs() < 1e-6, "slop=1 should match gap of 2");
    }

    #[test]
    fn match_phrase_no_match() {
        // Positions: "quick" at 0, "fox" at 5 (too far).
        let postings = vec![vec![0u32], vec![5u32]];
        let score = match_phrase_score(&postings, 2);
        assert_eq!(score, 0.0, "slop=2 should not match gap of 5");
    }

    #[test]
    fn aggregate_terms() {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "documentId".to_string(),
            vec![(0, "d1".into()), (1, "d2".into()), (2, "d1".into())],
        );
        let result = compute_single_aggregation(&store, "documentId");
        let per_doc = result.per_document.unwrap();
        assert_eq!(per_doc.buckets.len(), 2);
        assert_eq!(per_doc.buckets[0].key, "d1");
        assert_eq!(per_doc.buckets[0].doc_count, 2);
    }

    #[test]
    fn cardinality_aggregate() {
        let mut store = FilterStore::default();
        store.string_fields.insert(
            "documentId".to_string(),
            vec![(0, "d1".into()), (1, "d2".into()), (2, "d1".into())],
        );
        let result = compute_cardinality(&store, "documentId");
        let total = result.total_documents.unwrap();
        assert_eq!(total.value, 2);
    }

    #[test]
    fn search_with_wildcard() {
        let dir = std::env::temp_dir().join("kosha-test-wildcard");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        w.add_document(
            DocumentId("d1".into()),
            vec![Field::text("t", "hello world")],
        );
        w.add_document(
            DocumentId("d2".into()),
            vec![Field::text("t", "help others")],
        );
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 2,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let q = SearchQuery {
            query_text: "".into(),
            max_results: 10,
            from: 0,
            bm25_params: Bm25Params::default(),
            filter: None,
            sort: vec![],
            search_after: None,
            highlight: None,
            aggs: HashMap::new(),
            wildcard: Some(kosha_core::WildcardQuery {
                field: "t".into(),
                pattern: "hel*".into(),
                case_insensitive: true,
            }),
            match_phrase: None,
            knn: None,
        };
        let r = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(r.total_hits, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_after_by_id_paginates() {
        let dir = std::env::temp_dir().join("kosha-test-search-after");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());
        let seg_dir = dir.join(&ns.0).join("s1");
        let mut w = SegmentWriter::new(SegmentId("s1".into()), seg_dir);
        for i in 0..5 {
            w.add_document(
                DocumentId(format!("doc-{i}")),
                vec![
                    Field::text("content", "shared token"),
                    Field::text("documentId", "same-doc"),
                ],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();

        let manifest = Manifest {
            version: 1,
            segments: vec![ManifestEntry {
                segment_id: SegmentId("s1".into()),
                doc_count: 5,
            }],
        };
        let searcher = Searcher::new(dir.clone());
        let mut id_sort = std::collections::HashMap::new();
        id_sort.insert(
            "_id".into(),
            SortOrder {
                order: "asc".into(),
            },
        );

        let page1 = searcher
            .search(
                &ns,
                &manifest,
                &SearchQuery {
                    query_text: "shared".into(),
                    max_results: 2,
                    from: 0,
                    bm25_params: Bm25Params::default(),
                    filter: None,
                    sort: vec![SortSpec {
                        fields: id_sort.clone(),
                    }],
                    search_after: None,
                    highlight: None,
                    aggs: HashMap::new(),
                    wildcard: None,
                    match_phrase: None,
                    knn: None,
                },
                None,
            )
            .unwrap();
        assert_eq!(page1.results.len(), 2);
        assert_eq!(page1.results[0].doc_id.0, "doc-0");
        assert_eq!(page1.results[1].doc_id.0, "doc-1");

        let page2 = searcher
            .search(
                &ns,
                &manifest,
                &SearchQuery {
                    query_text: "shared".into(),
                    max_results: 2,
                    from: 0,
                    bm25_params: Bm25Params::default(),
                    filter: None,
                    sort: vec![SortSpec { fields: id_sort }],
                    search_after: Some(vec![page1.results[1].doc_id.0.clone()]),
                    highlight: None,
                    aggs: HashMap::new(),
                    wildcard: None,
                    match_phrase: None,
                    knn: None,
                },
                None,
            )
            .unwrap();
        assert_eq!(page2.results.len(), 2);
        assert_eq!(page2.results[0].doc_id.0, "doc-2");
        assert_eq!(page2.results[1].doc_id.0, "doc-3");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn filter_keeps_hits_across_segments() {
        let dir = std::env::temp_dir().join("kosha-test-filter-multiseg");
        let _ = std::fs::remove_dir_all(&dir);
        let ns = NamespaceId("test".into());

        for (seg, doc_id, matter) in [("s1", "a", "m1"), ("s2", "b", "m1"), ("s3", "c", "m2")] {
            let seg_dir = dir.join(&ns.0).join(seg);
            let mut w = SegmentWriter::new(SegmentId(seg.into()), seg_dir);
            w.add_document(
                DocumentId(doc_id.into()),
                vec![
                    Field::text("content", "shared token"),
                    Field::text("matterId", matter),
                ],
            );
            w.finalize(Bm25Params::default()).unwrap();
        }

        let manifest = Manifest {
            version: 1,
            segments: vec![
                ManifestEntry {
                    segment_id: SegmentId("s1".into()),
                    doc_count: 1,
                },
                ManifestEntry {
                    segment_id: SegmentId("s2".into()),
                    doc_count: 1,
                },
                ManifestEntry {
                    segment_id: SegmentId("s3".into()),
                    doc_count: 1,
                },
            ],
        };
        let searcher = Searcher::new(dir.clone());
        let q = SearchQuery {
            query_text: "shared".into(),
            max_results: 10,
            from: 0,
            bm25_params: Bm25Params::default(),
            filter: Some(kosha_core::FilterClause::Term {
                term: std::collections::HashMap::from([("matterId".into(), "m1".into())]),
            }),
            sort: vec![],
            search_after: None,
            highlight: None,
            aggs: HashMap::new(),
            wildcard: None,
            match_phrase: None,
            knn: None,
        };
        let r = searcher.search(&ns, &manifest, &q, None).unwrap();
        assert_eq!(
            r.total_hits, 2,
            "BM25+filter must keep hits from every segment"
        );
        let ids: std::collections::HashSet<_> =
            r.results.iter().map(|d| d.doc_id.0.as_str()).collect();
        assert!(ids.contains("a"));
        assert!(ids.contains("b"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
