//! Offline parallel segment builder.
//!
//! Segments are immutable and independent, so bulk ingest doesn't need a
//! server: this tool streams a corpus (fetch_msmarco.py text shards, plus
//! optional fetch_msmarco_embeddings.py embedding shards) and builds
//! segments concurrently across worker threads via `SegmentWriter` — the
//! exact writer the server's flush path uses, so the output is
//! byte-compatible (v5 postings, KIZC, SPFresh v2 vector indexes, offsets
//! sidecars, footers). Upload the output dir to the namespace's S3 prefix
//! and attach it with `POST /v1/admin/import-namespace`.
//!
//! Why: server-side ingest serializes on the per-namespace lock and builds
//! flush segments inside `/index` requests — the 10M vector corpus loaded
//! at ~485 docs/sec (~6h). Offline, segment builds are embarrassingly
//! parallel; the same corpus builds in roughly the time of
//! `total_build_cpu / cores`.
//!
//! Embedding shards are consumed in lockstep with text shards and every
//! doc's id is verified against the `.ids` sidecar — misalignment aborts.

use std::io::{BufRead, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use clap::Parser;
use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::SegmentWriter;

const EMB_DIM: usize = 1024;

#[derive(Parser, Debug)]
#[command(
    name = "kosha-build-segments",
    about = "Build immutable segments from corpus shards, in parallel, without a server"
)]
struct Args {
    /// Directory containing shard-*.ndjson files ({"id": ..., "text": ...})
    #[arg(long)]
    shards_dir: PathBuf,
    /// Optional directory containing emb-*.f32 / emb-*.ids embedding shards
    #[arg(long)]
    embeddings_dir: Option<PathBuf>,
    /// Namespace the segments belong to (names the output subdirectory and
    /// the segment ids)
    #[arg(long)]
    namespace: String,
    /// Output root; segments land at <out_dir>/<namespace>/<segment-id>/
    #[arg(long)]
    out_dir: PathBuf,
    /// Documents per segment (the server-side flush-threshold equivalent)
    #[arg(long, default_value_t = 50_000)]
    docs_per_segment: usize,
    /// Worker threads (segment builds run one per thread)
    #[arg(long, default_value_t = 0)]
    threads: usize,
    /// Stop after this many documents (0 = the whole corpus)
    #[arg(long, default_value_t = 0)]
    max_docs: usize,
    /// Vector field name attached to each doc when embeddings are provided
    #[arg(long, default_value = "text_emb")]
    vector_field: String,
    #[arg(long, default_value_t = 1.2)]
    bm25_k1: f64,
    #[arg(long, default_value_t = 0.75)]
    bm25_b: f64,
}

#[derive(serde::Deserialize)]
struct ShardDoc {
    id: String,
    text: String,
}

fn sorted_files(dir: &Path, prefix: &str, suffix: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(prefix) && n.ends_with(suffix))
        })
        .collect();
    files.sort();
    files
}

/// Iterator over (doc_id, vector) pairs across embedding shard pairs.
struct EmbReader {
    shards: Vec<(PathBuf, PathBuf)>, // (.f32, .ids)
    shard_idx: usize,
    reader: Option<std::io::BufReader<std::fs::File>>,
    ids: Vec<String>,
    id_idx: usize,
}

impl EmbReader {
    fn new(dir: &Path) -> Self {
        let f32s = sorted_files(dir, "emb-", ".f32");
        assert!(
            !f32s.is_empty(),
            "no emb-*.f32 shards under {}",
            dir.display()
        );
        let shards = f32s
            .into_iter()
            .map(|f| {
                let ids = f.with_extension("ids");
                assert!(ids.is_file(), "missing ids sidecar for {}", f.display());
                (f, ids)
            })
            .collect();
        Self {
            shards,
            shard_idx: 0,
            reader: None,
            ids: Vec::new(),
            id_idx: 0,
        }
    }

    fn next_record(&mut self) -> Option<(String, Vec<f32>)> {
        loop {
            if self.reader.is_none() {
                let (f32_path, ids_path) = self.shards.get(self.shard_idx)?;
                self.ids = std::fs::read_to_string(ids_path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", ids_path.display()))
                    .lines()
                    .map(str::to_string)
                    .collect();
                self.id_idx = 0;
                let expected = self.ids.len() as u64 * (EMB_DIM as u64) * 4;
                let actual = std::fs::metadata(f32_path).map(|m| m.len()).unwrap_or(0);
                assert!(
                    actual == expected,
                    "embedding shard {} is {actual} bytes, expected {expected} for {} ids",
                    f32_path.display(),
                    self.ids.len()
                );
                self.reader = Some(std::io::BufReader::with_capacity(
                    8 << 20,
                    std::fs::File::open(f32_path)
                        .unwrap_or_else(|e| panic!("open {}: {e}", f32_path.display())),
                ));
            }
            if self.id_idx >= self.ids.len() {
                self.reader = None;
                self.shard_idx += 1;
                continue;
            }
            let mut buf = [0u8; EMB_DIM * 4];
            self.reader
                .as_mut()
                .unwrap()
                .read_exact(&mut buf)
                .expect("truncated embedding shard");
            let vec: Vec<f32> = buf
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect();
            let id = std::mem::take(&mut self.ids[self.id_idx]);
            self.id_idx += 1;
            return Some((id, vec));
        }
    }
}

type Batch = (usize, Vec<(DocumentId, Vec<Field>)>);

fn main() {
    let args = Args::parse();
    let threads = if args.threads == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(8)
    } else {
        args.threads
    };
    let bm25 = Bm25Params {
        k1: args.bm25_k1,
        b: args.bm25_b,
    };
    let ns_out = args.out_dir.join(&args.namespace);
    std::fs::create_dir_all(&ns_out).expect("create out dir");

    let t0 = std::time::Instant::now();
    // Bounded channel: readers stay at most `threads` batches ahead, so
    // peak memory is ~2×threads in-flight segments' worth of documents.
    let (tx, rx) = mpsc::sync_channel::<Batch>(threads);
    let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));

    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let rx = std::sync::Arc::clone(&rx);
            let ns_out = ns_out.clone();
            let ns = args.namespace.clone();
            let bm25 = bm25.clone();
            std::thread::spawn(move || {
                let mut built = 0usize;
                loop {
                    let batch = { rx.lock().unwrap().recv() };
                    let Ok((seg_idx, docs)) = batch else { break };
                    let seg_id = SegmentId(format!("{ns}-offline-{seg_idx:06}"));
                    let seg_dir = ns_out.join(&seg_id.0);
                    let n = docs.len();
                    let t = std::time::Instant::now();
                    let mut w = SegmentWriter::new(seg_id.clone(), seg_dir);
                    for (id, fields) in docs {
                        w.add_document(id, fields);
                    }
                    w.finalize(bm25.clone())
                        .unwrap_or_else(|e| panic!("finalize {} failed: {e}", seg_id.0));
                    built += 1;
                    println!(
                        "segment {} sealed: {n} docs in {:.1}s",
                        seg_id.0,
                        t.elapsed().as_secs_f64()
                    );
                }
                built
            })
        })
        .collect();

    // ── stream shards, verify embedding alignment, dispatch batches ──────
    let mut emb = args.embeddings_dir.as_deref().map(EmbReader::new);
    let mut batch: Vec<(DocumentId, Vec<Field>)> = Vec::with_capacity(args.docs_per_segment);
    let mut seg_idx = 0usize;
    let mut total = 0usize;
    'outer: for shard in sorted_files(&args.shards_dir, "shard-", ".ndjson") {
        let file =
            std::fs::File::open(&shard).unwrap_or_else(|e| panic!("open {}: {e}", shard.display()));
        for line in std::io::BufReader::with_capacity(8 << 20, file).lines() {
            let line = line.expect("read shard line");
            if line.is_empty() {
                continue;
            }
            let doc: ShardDoc = serde_json::from_str(&line)
                .unwrap_or_else(|e| panic!("bad ndjson in {}: {e}", shard.display()));
            let mut fields = vec![Field::text("text", &doc.text)];
            if let Some(ref mut emb) = emb {
                let (emb_id, vec) = emb
                    .next_record()
                    .expect("embeddings exhausted before text corpus");
                assert!(
                    emb_id == doc.id,
                    "embedding/text misalignment: text doc {:?} paired with embedding {:?}",
                    doc.id,
                    emb_id
                );
                fields.push(Field::vector(args.vector_field.clone(), vec));
            }
            batch.push((DocumentId(doc.id), fields));
            total += 1;
            if batch.len() >= args.docs_per_segment {
                tx.send((seg_idx, std::mem::take(&mut batch))).unwrap();
                seg_idx += 1;
                batch.reserve(args.docs_per_segment);
            }
            if args.max_docs > 0 && total >= args.max_docs {
                break 'outer;
            }
        }
    }
    if !batch.is_empty() {
        tx.send((seg_idx, batch)).unwrap();
        seg_idx += 1;
    }
    drop(tx);
    let built: usize = workers.into_iter().map(|w| w.join().unwrap()).sum();
    assert_eq!(built, seg_idx, "worker/segment count mismatch");

    println!(
        "done: {total} docs -> {seg_idx} segments under {} in {:.1}s ({:.0} docs/sec)",
        ns_out.display(),
        t0.elapsed().as_secs_f64(),
        total as f64 / t0.elapsed().as_secs_f64().max(1e-9),
    );
    println!(
        "attach with: aws s3 sync {} s3://<bucket>/segments/{}/ && POST /v1/admin/import-namespace {{\"namespace\": \"{}\"}}",
        ns_out.display(),
        args.namespace,
        args.namespace
    );
}
