//! Profile remaining warm-cache BM25 costs after issue #37's assembly fix.
//!
//! Confirms where time goes once `fields.clone()` is deferred, and provides
//! current-vs-proposed pairs so the next fix can use Criterion baselines.
//!
//! ## Measured (25,053 hits, release, local)
//!
//! Phase breakdown (single-term, one-shot):
//! - collect candidates (`DocumentId` clone): dominant (~50%)
//! - score via `doc_record`: ~25%
//! - `select_nth` top-k: ~20%
//! - materialize page: negligible
//!
//! Current vs proposed (Criterion medians):
//! - `and_score_via_find` ~196 ms vs `and_score_via_hashmap` ~2.3 ms (~85×)
//! - `rank_sort_all` ~2.4 ms vs `rank_select_nth_page` ~0.42 ms (~6×; already landed)
//! - `score_or_via_doc_record` ≈ `score_or_via_field_lengths` (no win — HashMap
//!   insert dominates; side `field_lengths[]` alone is not justified yet)
//!
//! ## Before / after validation for a future fix
//! ```text
//! cargo bench -p kosha-query --bench scoring_profile -- --save-baseline before
//! # apply fix
//! cargo bench -p kosha-query --bench scoring_profile -- --baseline before
//! ```
//! Require Criterion's `Performance has improved` (p < 0.05) before landing.

use std::collections::HashMap;
use std::hint::black_box;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kosha_core::{Bm25Params, DocumentId, Field, Posting, SegmentId};
use kosha_query::Bm25Scorer;
use kosha_segment::{SegmentReader, SegmentWriter};

const HIT_COUNT: usize = 25_053;
const PAGE_SIZE: usize = 5;

fn build_segment(dir: &PathBuf) -> SegmentReader {
    let _ = std::fs::remove_dir_all(dir);
    let mut w = SegmentWriter::new(SegmentId("s1".into()), dir.clone());
    for i in 0..HIT_COUNT {
        // Both terms appear in every doc so AND intersection ≈ HIT_COUNT.
        let content = format!(
            "contract dispute paragraph {i}: breach of warranty, indemnity, \
             and termination. {}",
            "lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(20)
        );
        w.add_document(
            DocumentId(format!("id-{i}")),
            vec![
                Field::text("content", content),
                Field::keyword("custodian", format!("user-{}", i % 50)),
                Field::keyword("documentId", format!("doc-{}", i / 3)),
                Field::keyword("bates", format!("BATES{i:08}")),
                Field::date_val("date", "2024-06-15T00:00:00Z"),
                Field::keyword("matterId", format!("matter-{}", i % 10)),
            ],
        );
    }
    w.finalize(Bm25Params::default()).unwrap();
    SegmentReader::open_with_options(dir.clone(), false).unwrap()
}

#[derive(Clone)]
struct Cand {
    doc_id: DocumentId,
    score: f64,
    doc_seq: u32,
}

fn score_or_via_doc_record(
    reader: &SegmentReader,
    scorer: &Bm25Scorer,
    postings: &[Posting],
    df: u32,
) -> HashMap<u32, f64> {
    let mut scored = HashMap::with_capacity(postings.len());
    for posting in postings {
        if let Some(doc_rec) = reader.doc_record(posting.doc_id) {
            let score = scorer.score_term(posting.term_frequency, df, doc_rec.field_length);
            *scored.entry(posting.doc_id).or_insert(0.0) += score;
        }
    }
    scored
}

fn score_or_via_field_lengths(
    field_lengths: &[u32],
    scorer: &Bm25Scorer,
    postings: &[Posting],
    df: u32,
) -> HashMap<u32, f64> {
    let mut scored = HashMap::with_capacity(postings.len());
    for posting in postings {
        let len = field_lengths[posting.doc_id as usize];
        let score = scorer.score_term(posting.term_frequency, df, len);
        *scored.entry(posting.doc_id).or_insert(0.0) += score;
    }
    scored
}

/// Current AND scoring: after intersection, `.find()` each term's postings list.
fn and_score_via_find(
    reader: &SegmentReader,
    scorer: &Bm25Scorer,
    term_postings: &[(&[Posting], u32)],
) -> HashMap<u32, f64> {
    let mut and_candidates: Vec<u32> = {
        let shortest = term_postings.iter().min_by_key(|(p, _)| p.len()).unwrap();
        shortest.0.iter().map(|p| p.doc_id).collect()
    };
    for (postings, _) in term_postings {
        let term_docs: HashMap<u32, &Posting> = postings.iter().map(|p| (p.doc_id, p)).collect();
        and_candidates.retain(|doc_id| term_docs.contains_key(doc_id));
    }

    let mut scored = HashMap::with_capacity(and_candidates.len());
    for doc_id in and_candidates {
        if let Some(doc_rec) = reader.doc_record(doc_id) {
            let mut total = 0.0;
            for (postings, df) in term_postings {
                if let Some(p) = postings.iter().find(|p| p.doc_id == doc_id) {
                    total += scorer.score_term(p.term_frequency, *df, doc_rec.field_length);
                }
            }
            scored.insert(doc_id, total);
        }
    }
    scored
}

/// Proposed AND scoring: reuse the per-term HashMap built during intersection.
fn and_score_via_hashmap(
    field_lengths: &[u32],
    scorer: &Bm25Scorer,
    term_postings: &[(&[Posting], u32)],
) -> HashMap<u32, f64> {
    let term_maps: Vec<HashMap<u32, &Posting>> = term_postings
        .iter()
        .map(|(postings, _)| postings.iter().map(|p| (p.doc_id, p)).collect())
        .collect();

    let mut and_candidates: Vec<u32> = {
        let shortest = term_maps.iter().min_by_key(|m| m.len()).unwrap();
        shortest.keys().copied().collect()
    };
    for map in &term_maps {
        and_candidates.retain(|doc_id| map.contains_key(doc_id));
    }

    let mut scored = HashMap::with_capacity(and_candidates.len());
    for doc_id in and_candidates {
        let len = field_lengths[doc_id as usize];
        let mut total = 0.0;
        for (map, (_, df)) in term_maps.iter().zip(term_postings.iter()) {
            if let Some(p) = map.get(&doc_id) {
                total += scorer.score_term(p.term_frequency, *df, len);
            }
        }
        scored.insert(doc_id, total);
    }
    scored
}

fn collect_candidates(reader: &SegmentReader, scored: &HashMap<u32, f64>) -> Vec<Cand> {
    let mut cands = Vec::with_capacity(scored.len());
    for (&doc_seq, &score) in scored {
        if let Some(rec) = reader.doc_record(doc_seq) {
            cands.push(Cand {
                doc_id: rec.doc_id.clone(),
                score,
                doc_seq,
            });
        }
    }
    cands
}

fn select_nth_page(cands: &mut [Cand], page: usize) {
    if page == 0 || cands.is_empty() {
        return;
    }
    let to = page.min(cands.len());
    let cmp = |a: &Cand, b: &Cand| {
        b.score
            .partial_cmp(&a.score)
            .unwrap()
            .then_with(|| a.doc_id.0.cmp(&b.doc_id.0))
    };
    if to < cands.len() {
        cands.select_nth_unstable_by(to - 1, cmp);
        cands[..to].sort_by(cmp);
    } else {
        cands.sort_by(cmp);
    }
}

fn sort_all(cands: &mut [Cand]) {
    cands.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap()
            .then_with(|| a.doc_id.0.cmp(&b.doc_id.0))
    });
}

fn materialize_page(reader: &SegmentReader, cands: &[Cand], page: usize) -> usize {
    let mut bytes = 0usize;
    for c in cands.iter().take(page) {
        if let Some(rec) = reader.doc_record(c.doc_seq) {
            let fields = rec.fields.clone();
            bytes += fields
                .iter()
                .map(|f| f.name.len() + f.value.len())
                .sum::<usize>();
            black_box(fields);
        }
    }
    bytes
}

fn bench_scoring_profile(c: &mut Criterion) {
    let dir = std::env::temp_dir().join("kosha-bench-scoring-profile");
    let reader = build_segment(&dir);
    let scorer = Bm25Scorer::new(
        reader.doc_count(),
        reader.avg_field_length(),
        reader.bm25_params().clone(),
    );

    let contract = reader.postings("contract").expect("contract postings");
    let dispute = reader.postings("dispute").expect("dispute postings");
    assert!(contract.len() >= HIT_COUNT);
    assert!(dispute.len() >= HIT_COUNT);

    let field_lengths: Vec<u32> = (0..reader.doc_count())
        .map(|i| reader.doc_record(i).unwrap().field_length)
        .collect();

    let term_postings: [(&[Posting], u32); 2] = [
        (contract, contract.len() as u32),
        (dispute, dispute.len() as u32),
    ];

    // One-shot phase breakdown (printed once) so the profile is readable
    // without summing Criterion samples by hand.
    {
        use std::time::Instant;
        let t0 = Instant::now();
        let scored = score_or_via_doc_record(&reader, &scorer, contract, contract.len() as u32);
        let score_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let t1 = Instant::now();
        let mut cands = collect_candidates(&reader, &scored);
        let collect_ms = t1.elapsed().as_secs_f64() * 1000.0;

        let t2 = Instant::now();
        select_nth_page(&mut cands, PAGE_SIZE);
        let rank_ms = t2.elapsed().as_secs_f64() * 1000.0;

        let t3 = Instant::now();
        let _ = materialize_page(&reader, &cands, PAGE_SIZE);
        let mat_ms = t3.elapsed().as_secs_f64() * 1000.0;

        let total = score_ms + collect_ms + rank_ms + mat_ms;
        eprintln!(
            "\n=== warm single-term phase breakdown (n={HIT_COUNT}, page={PAGE_SIZE}) ===\n\
             score via doc_record:  {score_ms:8.2} ms  ({:5.1}%)\n\
             collect candidates:    {collect_ms:8.2} ms  ({:5.1}%)\n\
             select_nth top-k:      {rank_ms:8.2} ms  ({:5.1}%)\n\
             materialize page:      {mat_ms:8.2} ms  ({:5.1}%)\n\
             total:                 {total:8.2} ms\n",
            100.0 * score_ms / total,
            100.0 * collect_ms / total,
            100.0 * rank_ms / total,
            100.0 * mat_ms / total,
        );
    }

    let mut group = c.benchmark_group("issue37_remaining_hotspots");
    group.throughput(Throughput::Elements(HIT_COUNT as u64));
    group.sample_size(30);

    // ── Single-term OR scoring: doc_record vs field_lengths ───────────────
    group.bench_function("score_or_via_doc_record", |b| {
        b.iter(|| {
            black_box(score_or_via_doc_record(
                black_box(&reader),
                black_box(&scorer),
                black_box(contract),
                contract.len() as u32,
            ))
        })
    });
    group.bench_function("score_or_via_field_lengths", |b| {
        b.iter(|| {
            black_box(score_or_via_field_lengths(
                black_box(&field_lengths),
                black_box(&scorer),
                black_box(contract),
                contract.len() as u32,
            ))
        })
    });

    // ── Multi-term AND scoring: .find() vs HashMap ────────────────────────
    group.bench_function("and_score_via_find", |b| {
        b.iter(|| {
            black_box(and_score_via_find(
                black_box(&reader),
                black_box(&scorer),
                black_box(&term_postings),
            ))
        })
    });
    group.bench_function("and_score_via_hashmap", |b| {
        b.iter(|| {
            black_box(and_score_via_hashmap(
                black_box(&field_lengths),
                black_box(&scorer),
                black_box(&term_postings),
            ))
        })
    });

    // ── Ranking: full sort vs select_nth (already landed in #37) ──────────
    let scored = score_or_via_doc_record(&reader, &scorer, contract, contract.len() as u32);
    let base_cands = collect_candidates(&reader, &scored);
    group.bench_function("rank_sort_all", |b| {
        b.iter_batched(
            || base_cands.clone(),
            |mut cands| {
                sort_all(&mut cands);
                black_box(cands[0].score)
            },
            criterion::BatchSize::LargeInput,
        )
    });
    group.bench_function("rank_select_nth_page", |b| {
        b.iter_batched(
            || base_cands.clone(),
            |mut cands| {
                select_nth_page(&mut cands, PAGE_SIZE);
                black_box(cands[0].score)
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.bench_function("materialize_page5", |b| {
        let mut cands = base_cands.clone();
        select_nth_page(&mut cands, PAGE_SIZE);
        b.iter(|| {
            black_box(materialize_page(
                black_box(&reader),
                black_box(&cands),
                PAGE_SIZE,
            ))
        })
    });

    group.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

criterion_group!(benches, bench_scoring_profile);
criterion_main!(benches);
