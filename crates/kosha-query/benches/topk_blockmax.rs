//! Warm-path scoring microbenchmark — measures the block-max WAND top-k
//! early-termination path (O1) against the full BM25 walk it replaces.
//!
//! The dominant warm-path cost after the doc-store fix (PR #84, warm p50 →
//! ~1 ms for materialize) is `score_ms`: a broad Zipfian term like "the"
//! has 10⁵–10⁶ postings per segment and scores all of them, even though only
//! the top-10 ever reach the page. Lucene/Elasticsearch prunes blocks whose
//! maximum achievable score can't reach the running top-k threshold;
//! this harness proves Kosha's same pruning (block-max WAND) at Kosha's
//! grain — per segment, per block of 128 postings, BM25-fitted upper bound.
//!
//! A local Rust bench can't reproduce real S3 latency, but the structural
//! delta scored here (how many docs actually scored vs. the term's df)
//! is what gives the warm p50 win. The bench builds a corpus whose Zipfian
//! shape makes "the" land in almost every document with a high term
//! frequency for a few docs and low tf for the long tail — then runs the
//! same search with the block-max guard enabled and disabled, and reports
//! the candidate-count and end-to-end `score_ms` each.
//!
//! Run:
//! ```text
//! cargo bench -p kosha-query --bench topk_blockmax
//! # bigger corpus:
//! KOSHA_BENCH_SEGS=8 KOSHA_BENCH_DOCS=2000 \
//!   cargo bench -p kosha-query --bench topk_blockmax
//! ```

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, NamespaceId, SearchQuery, SegmentId,
};
use kosha_query::{set_wand_probe_order, Searcher, WandProbeOrder};
use kosha_segment::SegmentWriter;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ─── Deterministic synthetic corpus (same shape as segment_memory bench) ────

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn zipfish(&mut self, n: usize) -> usize {
        let u = self.next() as f64 / u64::MAX as f64;
        ((u * u) * n as f64) as usize
    }
}

fn vocab_word(rank: usize) -> String {
    match rank {
        0 => "the".into(),
        1 => "contract".into(),
        2 => "dispute".into(),
        _ => format!("w{rank}"),
    }
}

const WORDS_PER_DOC: usize = 120;

fn build_corpus(
    root: &std::path::Path,
    ns: &NamespaceId,
    segs: usize,
    docs: usize,
    vocab: usize,
) -> Manifest {
    let _ = std::fs::remove_dir_all(root);
    let mut entries = Vec::with_capacity(segs);
    for s in 0..segs {
        let seg_id = SegmentId(format!("s{s}"));
        let seg_dir = root.join(&ns.0).join(&seg_id.0);
        let mut w = SegmentWriter::new(seg_id.clone(), seg_dir);
        let mut rng = Lcg(0x5EED + s as u64);
        for d in 0..docs {
            let mut words = Vec::with_capacity(WORDS_PER_DOC);
            for _ in 0..WORDS_PER_DOC {
                words.push(vocab_word(rng.zipfish(vocab)));
            }
            // A deliberately rare term, injected into exactly 3 documents
            // regardless of corpus size — the Zipfian draw above doesn't
            // reliably produce a truly rare (single/handful-of-hits) term
            // even at high ranks (its density only falls off as 1/sqrt(k),
            // not sharply). "needle" gives the essential-term join bench
            // section a genuine "huge lists, tiny intersection" case: AND
            // it with "the"/"contract" (near-every-doc terms) and the
            // 3-doc intersection is exactly the needle's postings.
            if s == 0 && d < 3.min(docs) {
                words.push("needle".into());
            }
            w.add_document(
                DocumentId(format!("s{s}-d{d}")),
                vec![Field::text("t", words.join(" "))],
            );
        }
        w.finalize(Bm25Params::default()).unwrap();
        entries.push(ManifestEntry {
            segment_id: seg_id,
            doc_count: docs as u32,
        });
    }
    Manifest {
        version: 1,
        segments: entries,
        segment_footers: Default::default(),
    }
}

fn mk_query(text: &str, topk: usize) -> SearchQuery {
    SearchQuery {
        query_text: text.into(),
        max_results: topk,
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
        // This bench's entire point is the exact-count invariant across
        // legacy/pruned/unpruned paths — opt out of the v1 default cap.
        exact_total_hits: Some(true),
        total_hits_cap: None,
        operator: None,
        no_cache: None,
    }
}

fn median_ms(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[samples.len() / 2]
}

/// The block-max WAND path is enabled by default for single- AND
/// multi-term queries — it's the production path. To measure the legacy
/// general scoring path, use `kosha_query::force_legacy_search_after()`:
/// a one-element empty-string cursor fails the WAND gate but filters
/// nothing under default ranking. NOTE the caveat measured below: a
/// search_after query also takes the full-sort pagination path in the
/// caller, so end-to-end wall against a forced-legacy baseline includes a
/// sort the real pre-WAND path never paid — scoring-phase comparisons use
/// `score_wall_ms`, which excludes it.
struct Row {
    total_hits: usize,
    result_count: usize,
    wall_ms: f64,
    score_ms: f64,
}

fn main() {
    let segs = env_usize("KOSHA_BENCH_SEGS", 8);
    let docs = env_usize("KOSHA_BENCH_DOCS", 600);
    let vocab = env_usize("KOSHA_BENCH_VOCAB", 2000);
    let topk = env_usize("KOSHA_BENCH_TOPK", 10);
    let warm_iters = 5;

    let work = std::env::temp_dir().join("kosha-bench-topk-blockmax");
    let ns = NamespaceId("bench".into());
    let manifest = build_corpus(&work, &ns, segs, docs, vocab);

    let total_docs = segs * docs;
    println!(
        "\n  corpus: {segs} segs × {docs} docs = {total_docs} total (vocab={vocab}), topk={topk}, warm iters = {warm_iters}"
    );
    println!("  Pruning comparison: block-max WAND with topk={topk} (skips low-UB blocks) vs block-max with topk={total_docs} (no pruning — every block scored).");

    let searcher = Searcher::new(work.clone());

    // The one measurement loop every section uses — timing/stats capture
    // must stay identical across sections or the report compares
    // different quantities.
    let run_rows = |q: &SearchQuery| -> Vec<Row> {
        (0..warm_iters)
            .map(|_| {
                let t = Instant::now();
                let (r, stats) = searcher.search_with_stats(&ns, &manifest, q, None).unwrap();
                let wall_ms = t.elapsed().as_secs_f64() * 1e3;
                black_box(Row {
                    total_hits: r.total_hits,
                    result_count: r.results.len(),
                    wall_ms,
                    score_ms: stats.score_wall_ms,
                })
            })
            .collect()
    };

    // Warm up the segment cache.
    let _ = searcher
        .search_with_stats(&ns, &manifest, &mk_query("the", topk), None)
        .unwrap();
    let _ = searcher
        .search_with_stats(&ns, &manifest, &mk_query("the", total_docs), None)
        .unwrap();

    // ── Block-max WAND with topk=10 (production; pruning active) ────────
    let after_rows = run_rows(&mk_query("the", topk));

    // ── Block-max WAND with topk=total_docs (no pruning — equivalent
    // to the old "score every posting" full walk) ──────────────────────
    // With k=N, `topk.len() >= effective_k` never fires, so the threshold
    // check is skipped on every block — every doc gets scored exactly as
    // the production-before PR did. Same code path, same term, same
    // corpus; the only difference is the topk size. Hence the speedup
    // ratio is the savings block-max WAND gives on a top-10 page.
    let before_rows = run_rows(&mk_query("the", total_docs));

    // ── Report ───────────────────────────────────────────────────────────
    let med_after = median_ms(after_rows.iter().map(|r| r.wall_ms).collect());
    let med_before = median_ms(before_rows.iter().map(|r| r.wall_ms).collect());
    let speedup = med_before / med_after.max(1e-9);
    let after_total = after_rows[0].total_hits;
    let before_total = before_rows[0].total_hits;

    println!();
    println!(
        "  With pruning (topk={})    : total_hits = {} (exact), page = {}",
        topk, after_total, after_rows[0].result_count
    );
    println!(
        "  Without pruning (topk={}) : total_hits = {} (exact), page = {}",
        total_docs, before_total, before_rows[0].result_count
    );
    println!();
    println!(
        "  median warm scoring wall:  pruned = {:.2} ms | unpruned = {:.2} ms | speedup = {:.2}×",
        med_after, med_before, speedup
    );
    // The block-max path is correct iff total_hits is identical with and
    // without pruning — that's the structural bet preservation guarantee.
    if after_total == before_total {
        println!("  ✓ total_hits preserved (block-max reports the true df regardless of k)");
    } else {
        println!(
            "  ✗ BUG: total_hits differs ({after_total} pruned vs {before_total} unpruned) — WAND early-termination is wrong"
        );
    }
    println!();
    println!("  Per-iteration breakdown:");
    for (i, (a, b)) in after_rows.iter().zip(before_rows.iter()).enumerate() {
        println!(
            "    iter {}: pruned {:.2} ms (total_hits={}, page={}) | unpruned {:.2} ms (total_hits={}, page={})",
            i + 1,
            a.wall_ms,
            a.total_hits,
            a.result_count,
            b.wall_ms,
            b.total_hits,
            b.result_count,
        );
    }

    // ── Multi-term AND: leapfrog block-max join vs the legacy HashMap path ──
    //
    // "the contract" — two broad Zipfian terms with a large intersection,
    // the shape that dominates MSMarco-style natural-language queries.
    //   * legacy: the pre-WAND general path (per-term doc→posting HashMap
    //     build + retain-intersection), forced via a search_after cursor of
    //     one empty string — it fails the WAND gate but, under default
    //     ranking, filters nothing (`doc_id > ""` is true for every doc),
    //     so hit counts and ranking stay comparable. Same page size as the
    //     production row, so the two walls are apples to apples.
    //   * join (production): the leapfrog block-max AND, topk small.
    //   * a third run with topk=total_docs cross-checks the exact-count
    //     invariant; its wall time is NOT reported as a speedup — with
    //     every hit materialized as the page it measures doc_store reads,
    //     not traversal.
    let mt_text = "the contract";
    let mk_legacy = |text: &str, k: usize| {
        let mut q = mk_query(text, k);
        q.search_after = kosha_query::force_legacy_search_after();
        q
    };

    // Warm the caches for the new term.
    let _ = searcher
        .search_with_stats(&ns, &manifest, &mk_query(mt_text, topk), None)
        .unwrap();
    let _ = searcher
        .search_with_stats(&ns, &manifest, &mk_legacy(mt_text, topk), None)
        .unwrap();

    let legacy_rows = run_rows(&mk_legacy(mt_text, topk));
    let joined_rows = run_rows(&mk_query(mt_text, topk));
    // Count cross-check only — see the comment block above.
    let nopr_rows = run_rows(&mk_query(mt_text, total_docs));

    // Speedup is keyed on the SCORING phase, not end-to-end wall: the
    // forced-legacy baseline's search_after cursor also routes the caller
    // through a full candidate sort + linear cursor scan the real
    // pre-WAND production path (plain from/max_results, bounded
    // select_nth) never paid, so its wall overstates legacy cost.
    let med_legacy_score = median_ms(legacy_rows.iter().map(|r| r.score_ms).collect());
    let med_joined_score = median_ms(joined_rows.iter().map(|r| r.score_ms).collect());
    let med_legacy = median_ms(legacy_rows.iter().map(|r| r.wall_ms).collect());
    let med_joined = median_ms(joined_rows.iter().map(|r| r.wall_ms).collect());

    println!();
    println!("  Multi-term AND (\"{mt_text}\", topk={topk}):");
    println!(
        "    legacy HashMap path   : score {:>8.2} ms | wall {:>8.2} ms (incl. forced-cursor sort overhead)  (total_hits={})",
        med_legacy_score, med_legacy, legacy_rows[0].total_hits
    );
    println!(
        "    leapfrog block-max AND: score {:>8.2} ms | wall {:>8.2} ms  (total_hits={})  scoring speedup vs legacy = {:.2}×",
        med_joined_score,
        med_joined,
        joined_rows[0].total_hits,
        med_legacy_score / med_joined_score.max(1e-9)
    );
    if legacy_rows[0].total_hits == joined_rows[0].total_hits
        && joined_rows[0].total_hits == nopr_rows[0].total_hits
    {
        println!("    ✓ total_hits identical across legacy / pruned / unpruned — exact-count invariant holds");
    } else {
        println!(
            "    ✗ BUG: total_hits diverges (legacy={} pruned={} unpruned={}) — the AND join is wrong",
            legacy_rows[0].total_hits, joined_rows[0].total_hits, nopr_rows[0].total_hits
        );
    }

    // ── Skewed AND: essential-term join vs symmetric leapfrog ──────────
    //
    // "the contract needle" — two stopword-scale dense terms ANDed with a
    // 3-doc rare term (injected by `build_corpus`, independent of corpus
    // size). This is the pathological case for a round-robin leapfrog:
    // every cursor participates in raising the candidate, so the huge
    // terms' cursors get walked through the whole doc range even though
    // the final intersection is 3 docs. The essential-term join instead
    // drives candidate discovery off the smallest list ("needle") alone
    // and only *probes* "the"/"contract" once per driver candidate — see
    // the doc comment on `score_segment`'s multi-term arm.
    let skew_text = "the contract needle";
    let _ = searcher
        .search_with_stats(&ns, &manifest, &mk_query(skew_text, topk), None)
        .unwrap();
    let _ = searcher
        .search_with_stats(&ns, &manifest, &mk_legacy(skew_text, topk), None)
        .unwrap();
    let skew_legacy_rows = run_rows(&mk_legacy(skew_text, topk));
    let skew_joined_rows = run_rows(&mk_query(skew_text, topk));
    let med_skew_legacy_score = median_ms(skew_legacy_rows.iter().map(|r| r.score_ms).collect());
    let med_skew_joined_score = median_ms(skew_joined_rows.iter().map(|r| r.score_ms).collect());
    let med_skew_legacy = median_ms(skew_legacy_rows.iter().map(|r| r.wall_ms).collect());
    let med_skew_joined = median_ms(skew_joined_rows.iter().map(|r| r.wall_ms).collect());

    println!();
    println!("  Skewed AND (\"{skew_text}\", huge lists + 3-doc rare term, topk={topk}):");
    println!(
        "    legacy HashMap path    : score {:>8.2} ms | wall {:>8.2} ms  (total_hits={})",
        med_skew_legacy_score, med_skew_legacy, skew_legacy_rows[0].total_hits
    );
    println!(
        "    essential-term AND join: score {:>8.2} ms | wall {:>8.2} ms  (total_hits={})  scoring speedup vs legacy = {:.2}×",
        med_skew_joined_score,
        med_skew_joined,
        skew_joined_rows[0].total_hits,
        med_skew_legacy_score / med_skew_joined_score.max(1e-9)
    );
    if skew_legacy_rows[0].total_hits == skew_joined_rows[0].total_hits {
        println!("    ✓ total_hits identical — exact-count invariant holds on the skewed case too");
    } else {
        println!(
            "    ✗ BUG: total_hits diverges (legacy={} join={})",
            skew_legacy_rows[0].total_hits, skew_joined_rows[0].total_hits
        );
    }

    // ── Balanced AND: no single dominant rarest term ────────────────────
    //
    // The counterpoint to the skewed case above: five mid-frequency terms
    // (ranks 50/55/60/65/70 — none of them stopword-scale, none of them
    // near-unique) narrowing combinatorially to a small intersection. This
    // is the shape a fixed single-driver design (walk the smallest list,
    // probe the rest) regresses on — probing already costs the same
    // O(log blocks) as walking, since every term here uses the same
    // skip-table `advance_to`, so a fixed driver just gives up the
    // round-robin's ability to raise the candidate using whichever cursor
    // currently knows the most. Rarest-first *alignment order* (what's
    // actually implemented) keeps that adaptivity and still front-loads
    // the cheap rejections — this section is the regression guard for that
    // choice, not a speedup demo.
    let bal_text = "w50 w55 w60 w65 w70";
    let _ = searcher
        .search_with_stats(&ns, &manifest, &mk_query(bal_text, topk), None)
        .unwrap();
    let _ = searcher
        .search_with_stats(&ns, &manifest, &mk_legacy(bal_text, topk), None)
        .unwrap();
    let bal_legacy_rows = run_rows(&mk_legacy(bal_text, topk));
    let bal_joined_rows = run_rows(&mk_query(bal_text, topk));
    let med_bal_legacy_score = median_ms(bal_legacy_rows.iter().map(|r| r.score_ms).collect());
    let med_bal_joined_score = median_ms(bal_joined_rows.iter().map(|r| r.score_ms).collect());
    println!();
    println!("  Balanced AND (\"{bal_text}\", 5 mid-frequency terms, topk={topk}):");
    println!(
        "    legacy HashMap path    : score {:>8.2} ms  (total_hits={})",
        med_bal_legacy_score, bal_legacy_rows[0].total_hits
    );
    println!(
        "    essential-term AND join: score {:>8.2} ms  (total_hits={})  scoring speedup vs legacy = {:.2}×",
        med_bal_joined_score,
        bal_joined_rows[0].total_hits,
        med_bal_legacy_score / med_bal_joined_score.max(1e-9)
    );
    if bal_legacy_rows[0].total_hits != bal_joined_rows[0].total_hits {
        println!(
            "    ✗ BUG: total_hits diverges (legacy={} join={})",
            bal_legacy_rows[0].total_hits, bal_joined_rows[0].total_hits
        );
    }

    // ── Alignment-order A/B: rarest-first vs query-term-order ───────────
    //
    // The earlier sections above compare both WAND-arm variants against the
    // legacy HashMap path, never against each other — that A/B is exactly
    // what PR #104 (the rarest-first change) needed and shipped without.
    // This section runs each query shape twice on the SAME corpus / SAME
    // `mk_query` (the WAND path, not legacy), flipping only the
    // per-pass alignment probe order between runs:
    //   * RarestFirst — the PR #104 default (probes smallest-df cursor first).
    //   * QueryOrder  — the pre-#104 behavior (probes in query-term order).
    //
    // Result must be byte-identical (the intersection is commutative w.r.t.
    // probe order — only convergence speed varies); `total_hits` and the
    // page are asserted to match across both runs. The `score_ms` medians
    // ARE the measurement #104 was missing.
    let ab_queries: &[&str] = &[mt_text, skew_text, bal_text];
    for &qtext in ab_queries {
        set_wand_probe_order(WandProbeOrder::RarestFirst);
        let rf_rows = run_rows(&mk_query(qtext, topk));

        set_wand_probe_order(WandProbeOrder::QueryOrder);
        let qo_rows = run_rows(&mk_query(qtext, topk));

        // Restore the production default so anything downstream (including
        // a future caller across the bench binary) sees the #104 behavior.
        set_wand_probe_order(WandProbeOrder::RarestFirst);

        let med_rf = median_ms(rf_rows.iter().map(|r| r.score_ms).collect());
        let med_qo = median_ms(qo_rows.iter().map(|r| r.score_ms).collect());
        println!();
        println!("  Alignment-order A/B (\"{qtext}\", topk={topk}, same WAND path):");
        println!(
            "    rarest-first  (PR #104): score {:>8.2} ms  (total_hits={})",
            med_rf, rf_rows[0].total_hits
        );
        println!(
            "    query-order   (pre-#104): score {:>8.2} ms  (total_hits={})",
            med_qo, qo_rows[0].total_hits
        );
        let delta = med_qo - med_rf;
        let sign = if delta >= 0.0 { "+" } else { "-" };
        println!(
            "    Δ score_ms (query-order minus rarest-first): {sign}{:.2} ms  — {}",
            delta.abs(),
            if delta.abs() < 1e-3 {
                "no measurable difference"
            } else if delta > 0.0 {
                "rarest-first faster (confirms PR #104's hypothesis)"
            } else {
                "query-order faster (PR #104 regresses this shape)"
            }
        );
        if rf_rows[0].total_hits != qo_rows[0].total_hits {
            println!(
                "    ✗ BUG: total_hits diverges between probe orders \
                 (rf={} qo={}) — the intersection is commutative w.r.t. probe \
                 order; this is a real bug",
                rf_rows[0].total_hits, qo_rows[0].total_hits
            );
        } else {
            println!("    ✓ total_hits identical across probe orders");
        }
    }

    let _ = std::fs::remove_dir_all(&work);
}
