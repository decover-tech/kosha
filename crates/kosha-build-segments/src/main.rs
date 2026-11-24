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
//!
//! `--cluster-layout namespace` (#380, research, default off) assigns
//! documents to segments by vector region instead of input order: see
//! [`head`] (default) and [`cluster`]. The segment format is the same either way.

mod watchdog;

use std::io::{BufRead, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use clap::Parser;
use kosha_build_segments::{cluster, head};
use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::{BpConfig, DocReorder, SegmentWriter, VectorQuantizationConfig};

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
    /// Fail builds taking longer than this multiple of the median peer build
    /// time (0 disables the watchdog).
    #[arg(long, default_value_t = 10)]
    segment_watchdog_multiplier: u32,
    /// Minimum segment watchdog timeout, in seconds. Until three segments
    /// seal, the timeout is the floor times the watchdog multiplier.
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
    segment_watchdog_floor_secs: u64,
    /// Stop after this many documents (0 = the whole corpus)
    #[arg(long, default_value_t = 0)]
    max_docs: usize,
    /// Vector field name attached to each doc when embeddings are provided
    #[arg(long, default_value = "text_emb")]
    vector_field: String,
    /// Dimension of the embedding shards (msmarco embeddings are 1024)
    #[arg(long, default_value_t = 1024)]
    emb_dim: usize,
    /// Store vector postings as `f32` or `i8` (#210). Unset follows
    /// KOSHA_VECTOR_QUANTIZATION / KOSHA_VECTOR_QUANTIZATION_FIELDS, as the
    /// server's writers do.
    #[arg(long)]
    vector_quantization: Option<String>,
    /// Reorder each segment's documents before writing it (#385): `off`
    /// keeps ingest order, `bp` uses recursive graph bisection over the
    /// documents' terms, `posting` (#456, research) numbers documents
    /// posting by posting of `--vector-field`'s index, so each posting is
    /// one contiguous doc_seq range. Unset follows KOSHA_DOC_REORDER
    /// (default off). The output format is unchanged either way.
    #[arg(long)]
    doc_reorder: Option<String>,
    #[arg(long, default_value_t = 1.2)]
    bm25_k1: f64,
    #[arg(long, default_value_t = 0.75)]
    bm25_b: f64,
    /// How documents are assigned to segments. `input` (default): cut the
    /// corpus into `--docs-per-segment` runs in input order, as always.
    /// `namespace` (#380, research): route every document through a
    /// namespace-level clustering trained on a sample of the embeddings
    /// (`--cluster-method`), so each segment holds one region of the vector
    /// space. Needs `--embeddings-dir`. Segments use the unchanged format.
    #[arg(long, default_value = "input")]
    cluster_layout: String,
    /// `head` (default): two-level spherical k-means head of
    /// `--cluster-head-per-segment` clusters per segment, packed whole into
    /// segments in a nearest-neighbour order. `tree`: balanced hyperplane
    /// partition tree with exactly equal leaves (can cut a dense region).
    #[arg(long, default_value = "head")]
    cluster_method: String,
    /// Head clusters per segment for `--cluster-method head`
    #[arg(long, default_value_t = 16)]
    cluster_head_per_segment: usize,
    /// Segments (leaves) for `--cluster-layout namespace`; 0 = one per
    /// `--docs-per-segment` documents, the same count `input` produces.
    #[arg(long, default_value_t = 0)]
    cluster_leaves: usize,
    /// Embeddings sampled (evenly strided over the corpus) for training
    #[arg(long, default_value_t = 131_072)]
    cluster_sample: usize,
    /// k-means (head) or balanced 2-means (tree) iterations
    #[arg(long, default_value_t = 10)]
    cluster_iters: usize,
    #[arg(long, default_value_t = 380)]
    cluster_seed: u64,
    /// Where routed documents are spilled (one file per leaf) between the
    /// routing pass and the segment builds; default
    /// `<out_dir>/<namespace>.cluster-spill`. Needs about the corpus's text
    /// plus f32 vector size free; each leaf's file is deleted once its
    /// segment has been handed to a builder.
    #[arg(long)]
    spill_dir: Option<PathBuf>,
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
    dim: usize,
    shards: Vec<(PathBuf, PathBuf)>, // (.f32, .ids)
    shard_idx: usize,
    reader: Option<std::io::BufReader<std::fs::File>>,
    ids: Vec<String>,
    id_idx: usize,
}

impl EmbReader {
    fn new(dir: &Path, dim: usize) -> Self {
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
            dim,
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
                let expected = self.ids.len() as u64 * (self.dim as u64) * 4;
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
            let mut buf = vec![0u8; self.dim * 4];
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

/// Unit-normalized embeddings at `sample` evenly strided positions of the
/// first `limit` (0 = all) vectors across the `emb-*.f32` shards, read by
/// seeking, plus the number of vectors considered.
fn sample_embeddings(
    dir: &Path,
    dim: usize,
    limit: usize,
    sample: usize,
) -> (Vec<Vec<f32>>, usize) {
    use std::io::{Seek, SeekFrom};
    let row = (dim * 4) as u64;
    let shards: Vec<(PathBuf, usize)> = sorted_files(dir, "emb-", ".f32")
        .into_iter()
        .map(|f| {
            let len = std::fs::metadata(&f).map(|m| m.len()).unwrap_or(0);
            (f, (len / row) as usize)
        })
        .collect();
    let mut total: usize = shards.iter().map(|s| s.1).sum();
    if limit > 0 {
        total = total.min(limit);
    }
    let take = sample.min(total);
    let mut out = Vec::with_capacity(take);
    let (mut shard, mut shard_start) = (0usize, 0usize);
    let mut file: Option<std::fs::File> = None;
    let mut buf = vec![0u8; dim * 4];
    for i in 0..take {
        let global = (i as u128 * total as u128 / take as u128) as usize;
        while global >= shard_start + shards[shard].1 {
            shard_start += shards[shard].1;
            shard += 1;
            file = None;
        }
        let f = file.get_or_insert_with(|| {
            std::fs::File::open(&shards[shard].0)
                .unwrap_or_else(|e| panic!("open {}: {e}", shards[shard].0.display()))
        });
        f.seek(SeekFrom::Start((global - shard_start) as u64 * row))
            .expect("seek embedding shard");
        f.read_exact(&mut buf).expect("read embedding sample");
        let mut v: Vec<f32> = buf
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect();
        cluster::normalize(&mut v);
        out.push(v);
    }
    (out, total)
}

/// Streams the text shards (and embeddings in lockstep, verifying ids),
/// calling `emit` per document until the corpus or `max_docs` runs out.
/// Returns the number of documents emitted.
fn stream_corpus(args: &Args, mut emit: impl FnMut(String, String, Option<Vec<f32>>)) -> usize {
    let mut emb = args
        .embeddings_dir
        .as_deref()
        .map(|dir| EmbReader::new(dir, args.emb_dim));
    let mut total = 0usize;
    for shard in sorted_files(&args.shards_dir, "shard-", ".ndjson") {
        let file =
            std::fs::File::open(&shard).unwrap_or_else(|e| panic!("open {}: {e}", shard.display()));
        for line in std::io::BufReader::with_capacity(8 << 20, file).lines() {
            let line = line.expect("read shard line");
            if line.is_empty() {
                continue;
            }
            let doc: ShardDoc = serde_json::from_str(&line)
                .unwrap_or_else(|e| panic!("bad ndjson in {}: {e}", shard.display()));
            let vector = emb.as_mut().map(|emb| {
                let (emb_id, vec) = emb
                    .next_record()
                    .expect("embeddings exhausted before text corpus");
                assert!(
                    emb_id == doc.id,
                    "embedding/text misalignment: text doc {:?} paired with embedding {:?}",
                    doc.id,
                    emb_id
                );
                vec
            });
            emit(doc.id, doc.text, vector);
            total += 1;
            if args.max_docs > 0 && total >= args.max_docs {
                return total;
            }
        }
    }
    total
}

fn doc_fields(args: &Args, text: &str, vector: Option<Vec<f32>>) -> Vec<Field> {
    let mut fields = vec![Field::text("text", text)];
    if let Some(vec) = vector {
        fields.push(Field::vector(args.vector_field.clone(), vec));
    }
    fields
}

/// A trained namespace-level router: document vector -> segment (leaf).
enum Router {
    Tree(cluster::ClusterTree),
    Head(head::Head),
}

impl Router {
    fn route(&self, v: &[f32]) -> usize {
        match self {
            Router::Tree(t) => t.route(v),
            Router::Head(h) => h.route(v),
        }
    }

    fn write_json(&self, path: &Path, extra: serde_json::Value) -> std::io::Result<()> {
        match self {
            Router::Tree(t) => t.write_json(path, extra),
            Router::Head(h) => h.write_json(path, extra),
        }
    }
}

/// `--cluster-layout namespace`: train the router, route and spill
/// every document by leaf, then send each non-empty leaf to the builders
/// as one segment, in leaf order. Returns `(documents, segments)`.
fn dispatch_namespace_clustered(args: &Args, tx: &mpsc::SyncSender<Batch>) -> (usize, usize) {
    let emb_dir = args
        .embeddings_dir
        .as_deref()
        .expect("--cluster-layout namespace needs --embeddings-dir");
    let t = std::time::Instant::now();
    let (sample, corpus) =
        sample_embeddings(emb_dir, args.emb_dim, args.max_docs, args.cluster_sample);
    let sampled = sample.len();
    let leaves = if args.cluster_leaves > 0 {
        args.cluster_leaves
    } else {
        corpus.div_ceil(args.docs_per_segment.max(1)).max(1)
    };
    let router = match args.cluster_method.as_str() {
        "tree" => Router::Tree(cluster::ClusterTree::build(
            &sample,
            args.emb_dim,
            cluster::TreeParams {
                leaves,
                iters: args.cluster_iters,
                seed: args.cluster_seed,
            },
        )),
        "head" => Router::Head(head::Head::build(
            &sample,
            args.emb_dim,
            head::HeadParams {
                leaves,
                clusters: leaves * args.cluster_head_per_segment.max(1),
                iters: args.cluster_iters,
                seed: args.cluster_seed,
            },
        )),
        other => panic!("--cluster-method {other:?}: expected head or tree"),
    };
    drop(sample);
    println!(
        "cluster {}: {leaves} leaves{} from a {sampled}-vector sample of {corpus} in {:.1}s",
        args.cluster_method,
        match &router {
            Router::Head(h) => format!(", {} head clusters", h.clusters()),
            Router::Tree(_) => String::new(),
        },
        t.elapsed().as_secs_f64()
    );

    let spill_dir = args.spill_dir.clone().unwrap_or_else(|| {
        args.out_dir
            .join(format!("{}.cluster-spill", args.namespace))
    });
    std::fs::create_dir_all(&spill_dir).expect("create spill dir");
    let spill_path = |leaf: usize| spill_dir.join(format!("leaf-{leaf:06}.bin"));
    let mut spills: Vec<std::io::BufWriter<std::fs::File>> = (0..leaves)
        .map(|leaf| {
            std::io::BufWriter::with_capacity(
                256 << 10,
                std::fs::File::create(spill_path(leaf)).expect("create spill file"),
            )
        })
        .collect();
    let mut leaf_docs = vec![0usize; leaves];
    let t = std::time::Instant::now();
    let total = stream_corpus(args, |id, text, vector| {
        let vector = vector.expect("embeddings present");
        let leaf = router.route(&vector);
        leaf_docs[leaf] += 1;
        cluster::write_record(&mut spills[leaf], &id, &text, &vector).expect("write spill");
    });
    for mut w in spills {
        std::io::Write::flush(&mut w).expect("flush spill");
    }
    let (min, max) = (
        leaf_docs.iter().min().copied().unwrap_or(0),
        leaf_docs.iter().max().copied().unwrap_or(0),
    );
    println!(
        "routed {total} docs into {leaves} leaves in {:.1}s (docs/leaf min {min}, max {max}, mean {:.0})",
        t.elapsed().as_secs_f64(),
        total as f64 / leaves as f64
    );

    // Leaf order = router order: neighbouring segment ids hold neighbouring
    // regions. Empty leaves produce no segment; ids stay contiguous.
    let mut seg_idx = 0usize;
    let mut leaf_segment: Vec<Option<usize>> = vec![None; leaves];
    for (leaf, slot) in leaf_segment.iter_mut().enumerate() {
        let path = spill_path(leaf);
        let bytes = std::fs::read(&path).expect("read spill");
        let _ = std::fs::remove_file(&path);
        if bytes.is_empty() {
            continue;
        }
        let docs: Vec<(DocumentId, Vec<Field>)> = cluster::read_records(&bytes, args.emb_dim)
            .into_iter()
            .map(|(id, text, vec)| (DocumentId(id), doc_fields(args, &text, Some(vec))))
            .collect();
        drop(bytes);
        *slot = Some(seg_idx);
        tx.send((seg_idx, docs)).unwrap();
        seg_idx += 1;
    }
    let _ = std::fs::remove_dir(&spill_dir);

    let tree_path = args.out_dir.join(format!(
        "{}.cluster-{}.json",
        args.namespace, args.cluster_method
    ));
    router
        .write_json(
            &tree_path,
            serde_json::json!({
                "namespace": args.namespace,
                "docs": total,
                "sample": sampled,
                "iters": args.cluster_iters,
                "seed": args.cluster_seed,
                "leaf_docs": leaf_docs,
                "leaf_segment": leaf_segment,
                "segment_id_format": format!("{}-offline-{{:06}}", args.namespace),
            }),
        )
        .expect("write cluster tree");
    println!(
        "cluster {} written to {} (outside the namespace dir: not uploaded)",
        args.cluster_method,
        tree_path.display()
    );
    (total, seg_idx)
}

fn main() {
    let args = Args::parse();
    match args.cluster_layout.as_str() {
        "input" => {}
        "namespace" => {
            assert!(
                args.embeddings_dir.is_some(),
                "--cluster-layout namespace needs --embeddings-dir"
            );
            assert!(
                matches!(args.cluster_method.as_str(), "head" | "tree"),
                "--cluster-method {:?}: expected head or tree",
                args.cluster_method
            );
        }
        other => panic!("--cluster-layout {other:?}: expected input or namespace"),
    }
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
    let quantization = args.vector_quantization.as_deref().map(|raw| {
        let q = kosha_core::VectorQuantization::parse(raw)
            .unwrap_or_else(|| panic!("--vector-quantization {raw:?}: expected f32 or i8"));
        VectorQuantizationConfig::all(q)
    });
    let doc_reorder = match args.doc_reorder.as_deref() {
        None => DocReorder::from_env(),
        Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "none" => DocReorder::Off,
            "bp" => DocReorder::Bp,
            "posting" => DocReorder::Posting,
            _ => panic!("--doc-reorder {raw:?}: expected off, bp or posting"),
        },
    };
    println!("doc reorder: {}", doc_reorder.as_str());
    let ns_out = args.out_dir.join(&args.namespace);
    std::fs::create_dir_all(&ns_out).expect("create out dir");

    let t0 = std::time::Instant::now();
    // Bounded channel: readers stay at most `threads` batches ahead, so
    // peak memory is ~2×threads in-flight segments' worth of documents.
    let (tx, rx) = mpsc::sync_channel::<Batch>(threads);
    let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));

    let watchdog = (args.segment_watchdog_multiplier > 0).then(|| {
        std::sync::Arc::new(watchdog::Watchdog::new(
            args.segment_watchdog_multiplier,
            Duration::from_secs(args.segment_watchdog_floor_secs),
        ))
    });
    let monitor = watchdog.as_ref().map(|watchdog| {
        let watchdog = watchdog.clone();
        let (stop, stopped) = mpsc::channel();
        // Supervise independently of both blocking batch sends and worker joins.
        let monitor = std::thread::spawn(move || {
            if let Some(timeout) = watchdog.monitor(stopped) {
                eprintln!(
                    "segment watchdog: {} has been building for {:.1}s (timeout {:.1}s); aborting build",
                    timeout.segment.0,
                    timeout.elapsed.as_secs_f64(),
                    timeout.limit.as_secs_f64()
                );
                // A stuck worker cannot be joined or cancelled; fail the CLI.
                std::process::exit(1);
            }
        });
        (stop, monitor)
    });

    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let rx = std::sync::Arc::clone(&rx);
            let ns_out = ns_out.clone();
            let ns = args.namespace.clone();
            let bm25 = bm25.clone();
            let quantization = quantization.clone();
            let vector_field = args.vector_field.clone();
            let watchdog = watchdog.clone();
            std::thread::spawn(move || {
                let mut built = 0usize;
                loop {
                    let batch = { rx.lock().unwrap().recv() };
                    let Ok((seg_idx, docs)) = batch else { break };
                    let seg_id = SegmentId(format!("{ns}-offline-{seg_idx:06}"));
                    let seg_dir = ns_out.join(&seg_id.0);
                    let n = docs.len();
                    let t = std::time::Instant::now();
                    let watched = watchdog.as_ref().map(|w| w.start(seg_id.clone(), t));
                    let mut w = SegmentWriter::new(seg_id.clone(), seg_dir);
                    if let Some(q) = &quantization {
                        w = w.with_vector_quantization(q.clone());
                    }
                    // doc_seq follows add order, so adding in a new order
                    // remaps every per-doc structure at once (#385, #456).
                    let tr = std::time::Instant::now();
                    let fields_of = || docs.iter().map(|(_, fields)| fields.as_slice());
                    let (order, seeds) = match doc_reorder {
                        DocReorder::Off => (None, None),
                        DocReorder::Bp => {
                            (Some(w.bp_order(fields_of(), &BpConfig::default())), None)
                        }
                        DocReorder::Posting => match w.posting_order(fields_of(), &vector_field) {
                            Some(p) => (Some(p.order), Some(p.seeds)),
                            None => (None, None),
                        },
                    };
                    let reorder_secs = tr.elapsed().as_secs_f64();
                    match order {
                        Some(order) => {
                            let mut slots: Vec<Option<(DocumentId, Vec<Field>)>> =
                                docs.into_iter().map(Some).collect();
                            for old in order {
                                let (id, fields) = slots[old as usize].take().expect("permutation");
                                w.add_document(id, fields);
                            }
                        }
                        None => {
                            for (id, fields) in docs {
                                w.add_document(id, fields);
                            }
                        }
                    }
                    if let Some(seeds) = seeds {
                        w.seed_vector_postings(&vector_field, seeds);
                    }
                    let vector_builds = w.vector_index_builds();
                    w.finalize(bm25.clone())
                        .unwrap_or_else(|e| panic!("finalize {} failed: {e}", seg_id.0));
                    let elapsed = t.elapsed();
                    if let Some(watched) = watched {
                        watched.sealed(elapsed);
                    }
                    built += 1;
                    println!(
                        "segment {} sealed: {n} docs in {:.1}s (reorder {:.1}s)",
                        seg_id.0,
                        elapsed.as_secs_f64(),
                        reorder_secs
                    );
                    for build in vector_builds.lock().unwrap().iter() {
                        println!(
                            "segment {} vector field {:?}: {} postings, imbalance factor {:.4}",
                            seg_id.0, build.field, build.postings, build.imbalance_factor
                        );
                    }
                }
                built
            })
        })
        .collect();

    // ── stream shards, verify embedding alignment, dispatch batches ──────
    let (total, seg_idx) = match args.cluster_layout.as_str() {
        "input" => {
            let mut batch: Vec<(DocumentId, Vec<Field>)> =
                Vec::with_capacity(args.docs_per_segment);
            let mut seg_idx = 0usize;
            let total = stream_corpus(&args, |id, text, vector| {
                batch.push((DocumentId(id), doc_fields(&args, &text, vector)));
                if batch.len() >= args.docs_per_segment {
                    tx.send((seg_idx, std::mem::take(&mut batch))).unwrap();
                    seg_idx += 1;
                    batch.reserve(args.docs_per_segment);
                }
            });
            if !batch.is_empty() {
                tx.send((seg_idx, batch)).unwrap();
                seg_idx += 1;
            }
            (total, seg_idx)
        }
        "namespace" => dispatch_namespace_clustered(&args, &tx),
        other => panic!("--cluster-layout {other:?}: expected input or namespace"),
    };
    drop(tx);
    let built: usize = workers.into_iter().map(|w| w.join().unwrap()).sum();
    if let Some((stop, monitor)) = monitor {
        drop(stop);
        monitor.join().unwrap();
    }
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
    // #385 compares index size across doc orders, so the build log carries
    // it (a log line only; nothing is written).
    let (postings_bytes, inverted_bytes) = index_bytes(&ns_out);
    println!(
        "index bytes: postings {postings_bytes} (postings-*.bin) + inverted {inverted_bytes} (inverted.idx) = {}",
        postings_bytes + inverted_bytes
    );
}

/// Total bytes of `postings-*.bin` and of `inverted.idx` across the segment
/// directories directly under `ns_out`.
fn index_bytes(ns_out: &Path) -> (u64, u64) {
    let (mut postings, mut inverted) = (0u64, 0u64);
    let Ok(segments) = std::fs::read_dir(ns_out) else {
        return (0, 0);
    };
    for seg in segments.flatten() {
        let Ok(files) = std::fs::read_dir(seg.path()) else {
            continue;
        };
        for f in files.flatten() {
            let name = f.file_name();
            let name = name.to_string_lossy();
            let len = f.metadata().map(|m| m.len()).unwrap_or(0);
            if name == "inverted.idx" {
                inverted += len;
            } else if name.starts_with("postings-") && name.ends_with(".bin") {
                postings += len;
            }
        }
    }
    (postings, inverted)
}
