//! Docid reordering by recursive graph bisection (#385) must not change
//! results: BM25 inputs are permutation invariant and ties break on `_id`,
//! so the same corpus written in ingest order and in BP order returns the
//! same top-k ids and scores for every query, with and without a filter.
//!
//! Covers both writers that reorder:
//! - the offline builder's path (`SegmentWriter::bp_order`, documents added
//!   in that order), and
//! - compaction (`Indexer::with_doc_reorder`), including tombstones taken
//!   before the merge and deletes after it, so `doc_seq`-keyed deletion
//!   state is exercised.
//!
//! It also prints the postings bytes and scoring blocks decoded for both
//! orders (run with `--nocapture`); those numbers are informational, the
//! assertions are about parity. Everything is in one `#[test]` because the
//! blocks-decoded counter is process wide.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use kosha_core::{Bm25Params, Document, DocumentId, Field, NamespaceId, SearchQuery, SegmentId};
use kosha_query::Searcher;
use kosha_segment::{BpConfig, DocReorder, SegmentReader, SegmentWriter};
use kosha_write::{CompactOptions, Indexer};

/// A synthetic topic-model corpus (not MSMarco).
#[derive(Clone, Copy)]
struct Shape {
    docs: usize,
    per_segment: usize,
    topics: usize,
    topic_vocab: usize,
    background_vocab: usize,
    min_len: usize,
    max_extra_len: usize,
    queries: usize,
}

const PARITY: Shape = Shape {
    docs: 6_000,
    per_segment: 2_000,
    topics: 40,
    topic_vocab: 150,
    background_vocab: 1_500,
    min_len: 12,
    max_extra_len: 40,
    queries: 150,
};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    /// Roughly Zipfian rank in `0..n`.
    fn zipf(&mut self, n: usize) -> usize {
        let u = (self.next() >> 11) as f64 / (1u64 << 53) as f64;
        (((n as f64 + 1.0).powf(u) - 1.0) as usize).min(n - 1)
    }
}

fn topic_word(topic: usize, rank: usize) -> String {
    format!("t{topic}w{rank}")
}

fn background_word(rank: usize) -> String {
    format!("bg{rank}")
}

/// Topic-clustered documents in random (ingest) order. Every 25th document
/// repeats an earlier one's text under a new id, so equal scores occur and
/// the `_id` tie-break is exercised.
fn corpus(shape: Shape) -> Vec<(DocumentId, Vec<Field>)> {
    let mut rng = Rng(0x5EED_0385);
    let mut texts: Vec<String> = Vec::with_capacity(shape.docs);
    let mut docs = Vec::with_capacity(shape.docs);
    for i in 0..shape.docs {
        let text = if i % 25 == 24 {
            texts[rng.below(texts.len())].clone()
        } else {
            let topic = rng.below(shape.topics);
            let len = shape.min_len + rng.below(shape.max_extra_len);
            (0..len)
                .map(|_| {
                    if rng.below(10) < 7 {
                        topic_word(topic, rng.zipf(shape.topic_vocab))
                    } else {
                        background_word(rng.zipf(shape.background_vocab))
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        };
        texts.push(text.clone());
        let kind = ["a", "b", "c"][i % 3];
        docs.push((
            DocumentId(format!("doc-{i:06}")),
            vec![
                Field::text("text", text),
                Field::keyword("kind", kind),
                Field::integer("n", i as i64),
                // Distinct per document, so `stored_records` catches a
                // vector (doc_vectors.bin sidecar) left on the wrong doc_seq.
                Field::vector("emb", vec![i as f32, 1.0, (i % 7) as f32, 0.5]),
            ],
        ));
    }
    docs
}

/// 1-12 term queries: mostly one topic's words, plus background words.
fn queries(shape: Shape) -> Vec<String> {
    let mut rng = Rng(0xD0C5_0385);
    (0..shape.queries)
        .map(|q| {
            let terms = 1 + q % 12;
            let topic = rng.below(shape.topics);
            (0..terms)
                .map(|_| {
                    if rng.below(4) < 3 {
                        topic_word(topic, rng.zipf(shape.topic_vocab))
                    } else {
                        background_word(rng.zipf(shape.background_vocab))
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

fn manifest_for(segments: &[(SegmentId, u32)]) -> kosha_core::Manifest {
    let segments: Vec<serde_json::Value> = segments
        .iter()
        .map(|(id, n)| serde_json::json!({"segment_id": id.0, "doc_count": n}))
        .collect();
    serde_json::from_value(serde_json::json!({"version": 1, "segments": segments})).unwrap()
}

/// Writes `docs` as offline-builder segments of `per_segment` documents,
/// BP-ordered when `bp`.
fn build_offline(
    data_dir: &Path,
    ns: &str,
    docs: &[(DocumentId, Vec<Field>)],
    per_segment: usize,
    bp: bool,
) -> kosha_core::Manifest {
    let mut segs = Vec::new();
    for (i, chunk) in docs.chunks(per_segment).enumerate() {
        let seg_id = SegmentId(format!("{ns}-offline-{i:06}"));
        let mut w = SegmentWriter::new(seg_id.clone(), data_dir.join(ns).join(&seg_id.0));
        let order: Vec<u32> = if bp {
            w.bp_order(
                chunk.iter().map(|(_, f)| f.as_slice()),
                &BpConfig::default(),
            )
        } else {
            (0..chunk.len() as u32).collect()
        };
        for old in order {
            let (id, fields) = &chunk[old as usize];
            w.add_document(id.clone(), fields.clone());
        }
        let footer = w.finalize(Bm25Params::default()).unwrap();
        segs.push((seg_id, footer.doc_count));
    }
    manifest_for(&segs)
}

type Hits = Vec<(String, f64)>;

fn search_all(
    searcher: &Searcher,
    ns: &NamespaceId,
    manifest: &kosha_core::Manifest,
    tombstones: Option<&std::collections::HashMap<SegmentId, std::collections::HashSet<u32>>>,
    queries: &[String],
    filter: Option<&str>,
    operator: &str,
) -> Vec<Hits> {
    queries
        .iter()
        .map(|q| {
            let mut body =
                serde_json::json!({"query_text": q, "max_results": 10, "operator": operator});
            if let Some(kind) = filter {
                body["filter"] = serde_json::json!({"term": {"kind": kind}});
            }
            let query: SearchQuery = serde_json::from_value(body).unwrap();
            searcher
                .search(ns, manifest, &query, tombstones)
                .unwrap()
                .results
                .into_iter()
                .map(|d| (d.doc_id.0, d.score))
                .collect()
        })
        .collect()
}

/// `min_nonempty_pct`: the share of queries that must return hits, so a
/// broken fixture cannot pass by returning nothing in both orders.
fn assert_same_hits(
    what: &str,
    queries: &[String],
    off: &[Hits],
    bp: &[Hits],
    min_nonempty_pct: usize,
) {
    let mut nonempty = 0;
    for ((q, a), b) in queries.iter().zip(off).zip(bp) {
        let ids_a: Vec<&str> = a.iter().map(|(id, _)| id.as_str()).collect();
        let ids_b: Vec<&str> = b.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids_a, ids_b, "{what}: top-k ids differ for {q:?}");
        for ((id, sa), (_, sb)) in a.iter().zip(b) {
            assert!(
                (sa - sb).abs() <= 1e-9 * sa.abs().max(1.0),
                "{what}: score of {id} differs for {q:?}: {sa} vs {sb}"
            );
        }
        nonempty += usize::from(!a.is_empty());
    }
    assert!(
        nonempty * 100 >= queries.len() * min_nonempty_pct,
        "{what}: too many empty pages ({nonempty} of {} non-empty)",
        queries.len()
    );
}

/// Bytes of the postings files (`inverted*`, `postings-*`) under `dir`.
fn postings_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    for seg in std::fs::read_dir(dir).unwrap().flatten() {
        if !seg.path().is_dir() {
            continue;
        }
        for f in std::fs::read_dir(seg.path()).unwrap().flatten() {
            let name = f.file_name().to_string_lossy().into_owned();
            if name.starts_with("inverted") || name.starts_with("postings") {
                total += f.metadata().unwrap().len();
            }
        }
    }
    total
}

/// Every stored record of a namespace's segments, by id: the reordered
/// segments must hold exactly the same documents and fields.
fn stored_records(
    data_dir: &Path,
    ns: &str,
    manifest: &kosha_core::Manifest,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for e in &manifest.segments {
        let reader = SegmentReader::open(data_dir.join(ns).join(&e.segment_id.0)).unwrap();
        for seq in 0..reader.doc_count() {
            let rec = reader.doc_record_full(seq).unwrap().unwrap();
            out.insert(
                rec.doc_id.0.clone(),
                serde_json::to_string(&rec.fields).unwrap(),
            );
        }
    }
    out
}

fn tmp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kosha-bp-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn bp_reordered_segments_return_identical_top_k() {
    let docs = corpus(PARITY);
    let queries = queries(PARITY);

    // ── Offline builder path ────────────────────────────────────────────
    let dir = tmp("offline");
    let off_m = build_offline(&dir, "off", &docs, PARITY.per_segment, false);
    let bp_m = build_offline(&dir, "bp", &docs, PARITY.per_segment, true);
    let off_ns = NamespaceId("off".into());
    let bp_ns = NamespaceId("bp".into());
    assert_eq!(
        stored_records(&dir, "off", &off_m),
        stored_records(&dir, "bp", &bp_m)
    );

    let searcher = Searcher::new(dir.clone());
    // Warm both namespaces first, so the counted passes below run
    // against the same cache state.
    search_all(&searcher, &off_ns, &off_m, None, &queries, None, "or");
    search_all(&searcher, &bp_ns, &bp_m, None, &queries, None, "or");
    let c0 = kosha_segment::scoring_blocks_decoded();
    let off_hits = search_all(&searcher, &off_ns, &off_m, None, &queries, None, "or");
    let c1 = kosha_segment::scoring_blocks_decoded();
    let bp_hits = search_all(&searcher, &bp_ns, &bp_m, None, &queries, None, "or");
    let c2 = kosha_segment::scoring_blocks_decoded();
    assert_same_hits("offline or", &queries, &off_hits, &bp_hits, 95);
    // The default operator (AND) too; long queries come back empty there.
    let a = search_all(&searcher, &off_ns, &off_m, None, &queries, None, "and");
    let b = search_all(&searcher, &bp_ns, &bp_m, None, &queries, None, "and");
    assert_same_hits("offline and", &queries, &a, &b, 20);
    for kind in ["a", "c"] {
        let a = search_all(&searcher, &off_ns, &off_m, None, &queries, Some(kind), "or");
        let b = search_all(&searcher, &bp_ns, &bp_m, None, &queries, Some(kind), "or");
        assert_same_hits("offline or+filter", &queries, &a, &b, 90);
    }
    let (pb_off, pb_bp) = (
        postings_bytes(&dir.join("off")),
        postings_bytes(&dir.join("bp")),
    );
    eprintln!(
        "offline builder, {} docs in {} segments: postings bytes off {pb_off} bp {pb_bp} ({:+.1}%); \
         scoring blocks decoded for {} queries: off {} bp {}",
        PARITY.docs,
        off_m.segments.len(),
        (pb_bp as f64 / pb_off as f64 - 1.0) * 100.0,
        queries.len(),
        c1 - c0,
        c2 - c1,
    );
    let _ = std::fs::remove_dir_all(&dir);

    // ── Compaction path ─────────────────────────────────────────────────
    let compacted = |mode: DocReorder| {
        let dir = tmp(&format!("compact-{}", mode.as_str()));
        let ns = NamespaceId("c".into());
        let idx = Indexer::new(dir.clone())
            .with_flush_threshold(PARITY.per_segment)
            .with_doc_reorder(mode)
            .with_doc_reorder_compaction_max_docs(u64::MAX);
        let batch: Vec<Document> = docs
            .iter()
            .map(|(id, fields)| Document {
                id: id.clone(),
                fields: fields.clone(),
            })
            .collect();
        idx.index_documents(ns.clone(), batch).unwrap();
        idx.flush_namespace(&ns).unwrap();
        // Deleted before the merge: dropped from its output.
        let m = idx.manifest(&ns).unwrap();
        let before: kosha_core::FilterClause =
            serde_json::from_value(serde_json::json!({"range": {"n": {"lt": "300"}}})).unwrap();
        assert_eq!(idx.delete_by_query(&ns, &m, &before).unwrap(), 300);
        let r = idx
            .compact_namespace_with_options(&ns, CompactOptions::full())
            .unwrap();
        assert!(r.merged);
        // Deleted after the merge: tombstones keyed by the merged doc_seqs.
        let m = idx.manifest(&ns).unwrap();
        assert_eq!(m.segments.len(), 1);
        let after: kosha_core::FilterClause = serde_json::from_value(
            serde_json::json!({"range": {"n": {"gte": "3000", "lt": "3500"}}}),
        )
        .unwrap();
        assert_eq!(idx.delete_by_query(&ns, &m, &after).unwrap(), 500);
        let m = idx.manifest(&ns).unwrap();
        let ts = idx.get_tombstones(&ns).unwrap();
        // The tombstoned doc_seqs name exactly the deleted documents.
        let seg = &m.segments[0].segment_id;
        let reader = SegmentReader::open(dir.join("c").join(&seg.0)).unwrap();
        let mut deleted: Vec<String> = ts[seg]
            .iter()
            .map(|&s| reader.doc_record_full(s).unwrap().unwrap().doc_id.0)
            .collect();
        deleted.sort();
        let want: Vec<String> = (3000..3500).map(|i| format!("doc-{i:06}")).collect();
        assert_eq!(
            deleted, want,
            "{mode:?}: tombstones name the wrong documents"
        );
        let searcher = Searcher::new(dir.clone());
        let hits = search_all(&searcher, &ns, &m, Some(&ts), &queries, None, "or");
        let filtered = search_all(&searcher, &ns, &m, Some(&ts), &queries, Some("b"), "or");
        let bytes = postings_bytes(&dir.join("c"));
        let stored = stored_records(&dir, "c", &m);
        let _ = std::fs::remove_dir_all(&dir);
        (hits, filtered, bytes, stored)
    };
    let (off_hits, off_f, off_bytes, off_stored) = compacted(DocReorder::Off);
    let (bp_hits, bp_f, bp_bytes, bp_stored) = compacted(DocReorder::Bp);
    assert_eq!(off_stored, bp_stored, "compaction: stored records differ");
    assert_same_hits("compaction", &queries, &off_hits, &bp_hits, 95);
    assert_same_hits("compaction+filter", &queries, &off_f, &bp_f, 90);
    for hits in &bp_hits {
        for (id, _) in hits {
            let n: usize = id.trim_start_matches("doc-").parse().unwrap();
            assert!(
                n >= 300 && !(3000..3500).contains(&n),
                "deleted {id} returned"
            );
        }
    }
    eprintln!(
        "compaction, {} live docs in 1 segment: postings bytes off {off_bytes} bp {bp_bytes} ({:+.1}%)",
        PARITY.docs - 300,
        (bp_bytes as f64 / off_bytes as f64 - 1.0) * 100.0,
    );
}

/// Not a correctness test: builds a larger synthetic corpus (two 50k-doc
/// segments, offline-builder path) in both orders and prints BP time,
/// postings bytes, and per query-length bucket the scoring blocks decoded
/// and mean in-process search time. Synthetic data, so the numbers say
/// nothing about MSMarco; the bench VM measures that. Run with
/// `cargo test --release -p kosha-query --test bp_reorder_parity -- --ignored --nocapture`.
#[test]
#[ignore]
fn bp_reorder_scale_report() {
    let shape = Shape {
        docs: 100_000,
        per_segment: 50_000,
        topics: 500,
        topic_vocab: 400,
        background_vocab: 30_000,
        min_len: 30,
        max_extra_len: 50,
        queries: 1_200,
    };
    let docs = corpus(shape);
    let queries = queries(shape);
    let dir = tmp("scale");
    let t = std::time::Instant::now();
    let off_m = build_offline(&dir, "off", &docs, shape.per_segment, false);
    let off_build = t.elapsed().as_secs_f64();
    let t = std::time::Instant::now();
    let bp_m = build_offline(&dir, "bp", &docs, shape.per_segment, true);
    let bp_build = t.elapsed().as_secs_f64();
    let (pb_off, pb_bp) = (
        postings_bytes(&dir.join("off")),
        postings_bytes(&dir.join("bp")),
    );
    eprintln!(
        "{} docs, {} per segment: build off {off_build:.1}s bp {bp_build:.1}s; \
         postings bytes off {pb_off} bp {pb_bp} ({:+.1}%)",
        shape.docs,
        shape.per_segment,
        (pb_bp as f64 / pb_off as f64 - 1.0) * 100.0
    );
    let searcher = Searcher::new(dir.clone());
    let run = |ns: &str, m: &kosha_core::Manifest| -> Vec<(u64, f64, Hits)> {
        let ns = NamespaceId(ns.into());
        queries
            .iter()
            .map(|q| {
                let body = serde_json::json!({
                    "query_text": q, "max_results": 10, "operator": "or", "no_cache": true
                });
                let query: SearchQuery = serde_json::from_value(body).unwrap();
                let c = kosha_segment::scoring_blocks_decoded();
                let t = std::time::Instant::now();
                let r = searcher.search(&ns, m, &query, None).unwrap();
                let secs = t.elapsed().as_secs_f64();
                let blocks = kosha_segment::scoring_blocks_decoded() - c;
                let hits = r
                    .results
                    .into_iter()
                    .map(|d| (d.doc_id.0, d.score))
                    .collect();
                (blocks, secs, hits)
            })
            .collect()
    };
    // Warm pass for each, then the measured pass.
    run("off", &off_m);
    run("bp", &bp_m);
    let off = run("off", &off_m);
    let bp = run("bp", &bp_m);
    let off_hits: Vec<Hits> = off.iter().map(|r| r.2.clone()).collect();
    let bp_hits: Vec<Hits> = bp.iter().map(|r| r.2.clone()).collect();
    assert_same_hits("scale", &queries, &off_hits, &bp_hits, 95);
    for (lo, hi) in [(1, 2), (3, 4), (5, 8), (9, 12)] {
        let pick = |rows: &[(u64, f64, Hits)]| {
            let sel: Vec<&(u64, f64, Hits)> = rows
                .iter()
                .zip(&queries)
                .filter(|(_, q)| (lo..=hi).contains(&q.split(' ').count()))
                .map(|(r, _)| r)
                .collect();
            let n = sel.len() as f64;
            (
                sel.iter().map(|r| r.0 as f64).sum::<f64>() / n,
                sel.iter().map(|r| r.1).sum::<f64>() / n * 1e3,
                sel.len(),
            )
        };
        let (ob, ot, n) = pick(&off);
        let (bb, bt, _) = pick(&bp);
        eprintln!(
            "{lo}-{hi} terms ({n} queries): blocks decoded/query off {ob:.1} bp {bb:.1} ({:+.1}%); \
             mean search ms off {ot:.3} bp {bt:.3}",
            (bb / ob - 1.0) * 100.0
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
