//! Microbenchmark for GitHub issue #37.
//!
//! Isolates the two candidate costs in the warm-cache BM25 result-assembly
//! path before we commit to a fix:
//!
//! 1. `fields.clone()` for every matching document (not just the page)
//! 2. sorting the full `Vec<ScoredDocument>` before truncating to `max_results`
//!
//! Also measures the proposed alternative: score-only accumulation, sort of
//! lightweight `(doc_id, score)` tuples, then materialize fields for the
//! returned page only.
//!
//! Run with:
//! ```text
//! cargo bench -p kosha-query --bench clone_vs_sort
//! ```

use std::cmp::Ordering;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kosha_core::{DocumentId, Field, ScoredDocument};

/// Hit count from the live staging benchmark in issue #37
/// (`paragraph_index_hnsw` "contract").
const HIT_COUNT: usize = 25_053;
const PAGE_SIZE: usize = 5;

#[derive(Clone)]
struct ScoreOnlyCandidate {
    doc_id: DocumentId,
    score: f64,
    /// Index into the resident source field store (stands in for doc_seq).
    source_idx: u32,
}

fn make_fields(i: usize) -> Vec<Field> {
    // Roughly mirrors a paragraph doc: long content text + metadata fields
    // (custodian, dates, bates, matter id) that get cloned today.
    let content = format!(
        "This is paragraph {i} about a contract dispute involving breach of \
         warranty, indemnity, and termination clauses. {}",
        "lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(20)
    );
    vec![
        Field::text("content", content),
        Field::keyword("custodian", format!("user-{}", i % 50)),
        Field::keyword("documentId", format!("doc-{}", i / 3)),
        Field::keyword("bates", format!("BATES{i:08}")),
        Field::date_val("date", "2024-06-15T00:00:00Z"),
        Field::keyword("matterId", format!("matter-{}", i % 10)),
    ]
}

fn score_desc_id_asc(a_score: f64, a_id: &str, b_score: f64, b_id: &str) -> Ordering {
    b_score
        .partial_cmp(&a_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a_id.cmp(b_id))
}

fn clone_all_hits(source: &[Vec<Field>], scores: &[f64]) -> Vec<ScoredDocument> {
    let mut out = Vec::with_capacity(source.len());
    for (i, fields) in source.iter().enumerate() {
        out.push(ScoredDocument {
            doc_id: DocumentId(format!("id-{i}")),
            score: scores[i],
            fields: fields.clone(),
            highlights: None,
        });
    }
    out
}

fn sort_scored(docs: &mut [ScoredDocument]) {
    docs.sort_by(|a, b| score_desc_id_asc(a.score, &a.doc_id.0, b.score, &b.doc_id.0));
}

fn collect_score_only(scores: &[f64]) -> Vec<ScoreOnlyCandidate> {
    let mut out = Vec::with_capacity(scores.len());
    for (i, &score) in scores.iter().enumerate() {
        out.push(ScoreOnlyCandidate {
            doc_id: DocumentId(format!("id-{i}")),
            score,
            source_idx: i as u32,
        });
    }
    out
}

fn sort_score_only(cands: &mut [ScoreOnlyCandidate]) {
    cands.sort_by(|a, b| score_desc_id_asc(a.score, &a.doc_id.0, b.score, &b.doc_id.0));
}

fn materialize_page(source: &[Vec<Field>], cands: &[ScoreOnlyCandidate], page: usize) -> Vec<ScoredDocument> {
    cands
        .iter()
        .take(page)
        .map(|c| ScoredDocument {
            doc_id: c.doc_id.clone(),
            score: c.score,
            fields: source[c.source_idx as usize].clone(),
            highlights: None,
        })
        .collect()
}

fn bench_clone_vs_sort(c: &mut Criterion) {
    let source: Vec<Vec<Field>> = (0..HIT_COUNT).map(make_fields).collect();
    let scores: Vec<f64> = (0..HIT_COUNT)
        .map(|i| (HIT_COUNT - i) as f64 * 0.137)
        .collect();

    let bytes_per_doc: u64 = source[0]
        .iter()
        .map(|f| (f.name.len() + f.value.len()) as u64)
        .sum();
    let total_field_bytes = bytes_per_doc * HIT_COUNT as u64;

    let mut group = c.benchmark_group("issue37_result_assembly");
    group.throughput(Throughput::Bytes(total_field_bytes));
    group.sample_size(30);

    // ── Current path pieces ──────────────────────────────────────────────
    group.bench_function(BenchmarkId::new("clone_all_fields", HIT_COUNT), |b| {
        b.iter(|| black_box(clone_all_hits(black_box(&source), black_box(&scores))))
    });

    // Pre-clone once so sort timing is not contaminated by allocation.
    let mut precloned = clone_all_hits(&source, &scores);
    group.bench_function(BenchmarkId::new("sort_full_scored_docs", HIT_COUNT), |b| {
        b.iter(|| {
            // Re-shuffle via reverse so each iteration actually sorts.
            precloned.reverse();
            sort_scored(black_box(&mut precloned));
            black_box(precloned[0].score)
        })
    });

    group.bench_function(
        BenchmarkId::new("current_clone_then_sort_then_page", HIT_COUNT),
        |b| {
            b.iter(|| {
                let mut docs = clone_all_hits(black_box(&source), black_box(&scores));
                sort_scored(&mut docs);
                let page = docs[..PAGE_SIZE].to_vec();
                black_box(page)
            })
        },
    );

    // ── Proposed path pieces ─────────────────────────────────────────────
    group.bench_function(BenchmarkId::new("collect_score_only", HIT_COUNT), |b| {
        b.iter(|| black_box(collect_score_only(black_box(&scores))))
    });

    let mut precollected = collect_score_only(&scores);
    group.bench_function(BenchmarkId::new("sort_score_only", HIT_COUNT), |b| {
        b.iter(|| {
            precollected.reverse();
            sort_score_only(black_box(&mut precollected));
            black_box(precollected[0].score)
        })
    });

    group.bench_function(
        BenchmarkId::new("proposed_score_only_sort_materialize_page", HIT_COUNT),
        |b| {
            b.iter(|| {
                let mut cands = collect_score_only(black_box(&scores));
                sort_score_only(&mut cands);
                let page = materialize_page(black_box(&source), &cands, PAGE_SIZE);
                black_box(page)
            })
        },
    );

    group.finish();
}

criterion_group!(benches, bench_clone_vs_sort);
criterion_main!(benches);
