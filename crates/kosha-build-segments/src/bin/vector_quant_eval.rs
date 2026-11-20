//! Measures i8 vector quantization (#210) on two segment sets built by
//! `kosha-build-segments` from the same corpus, one with
//! `--vector-quantization f32` and one with `i8`.
//!
//! ```text
//! # optional: a synthetic corpus when the msmarco embeddings are not local
//! kosha-vector-quant-eval gen --out /tmp/corpus --docs 100000 --dim 384
//! kosha-build-segments --shards-dir /tmp/corpus --embeddings-dir /tmp/corpus \
//!     --emb-dim 384 --namespace f32 --out-dir /tmp/segs --vector-quantization f32
//! kosha-build-segments ... --namespace i8 --vector-quantization i8
//! KOSHA_VECTOR_POSTINGS_CACHE_MAX_BYTES=100000000000 \
//!   kosha-vector-quant-eval eval --embeddings-dir /tmp/corpus --dim 384 \
//!     --f32 /tmp/segs/f32 --i8 /tmp/segs/i8
//! ```
//!
//! Reports, per set: `vector.idx` bytes per vector, total segment bytes,
//! the bytes a cold kNN must hydrate (`knn_vector_files`), the decoded
//! posting cache once every posting is warm, and recall@10 against exact
//! f32 brute force over the raw embeddings — for the f32 index, for i8
//! without rescoring, and for i8 rescored at several factors.

use std::collections::HashMap;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::{Parser, Subcommand};
use kosha_segment::SegmentReader;

#[derive(Parser)]
#[command(name = "kosha-vector-quant-eval")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write a synthetic clustered corpus in the msmarco shard layout
    /// (`shard-000.ndjson`, `emb-000.f32`, `emb-000.ids`).
    Gen {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 100_000)]
        docs: usize,
        #[arg(long, default_value_t = 384)]
        dim: usize,
        /// Gaussian clusters the vectors are drawn around
        #[arg(long, default_value_t = 1_000)]
        clusters: usize,
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
    /// Compare an f32 and an i8 segment set built from the same corpus.
    Eval {
        #[arg(long)]
        embeddings_dir: PathBuf,
        #[arg(long, default_value_t = 1024)]
        dim: usize,
        /// Namespace directory of the f32 build (`<out-dir>/<namespace>`)
        #[arg(long)]
        f32: PathBuf,
        /// Namespace directory of the i8 build
        #[arg(long)]
        i8: PathBuf,
        #[arg(long, default_value = "text_emb")]
        field: String,
        #[arg(long, default_value_t = 200)]
        queries: usize,
        /// Postings probed per segment (`num_candidates` per segment)
        #[arg(long, default_value_t = 32)]
        nprobe: usize,
        #[arg(long, default_value_t = 10)]
        k: usize,
        #[arg(long, default_value_t = 7)]
        seed: u64,
    },
}

/// xorshift64* — deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn uniform(&mut self) -> f32 {
        ((self.next_u64() >> 40) as f32 + 0.5) / (1u64 << 24) as f32
    }
    /// Standard normal (Box-Muller).
    fn normal(&mut self) -> f32 {
        let (u, v) = (self.uniform(), self.uniform());
        (-2.0 * u.ln()).sqrt() * (std::f32::consts::TAU * v).cos()
    }
}

fn normalize(v: &mut [f32]) {
    let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        v.iter_mut().for_each(|x| *x /= n);
    }
}

fn gen(out: &Path, docs: usize, dim: usize, clusters: usize, seed: u64) {
    std::fs::create_dir_all(out).expect("create out dir");
    let mut rng = Rng(seed | 1);
    let centers: Vec<Vec<f32>> = (0..clusters)
        .map(|_| {
            let mut c: Vec<f32> = (0..dim).map(|_| rng.normal()).collect();
            normalize(&mut c);
            c
        })
        .collect();
    let mut text = BufWriter::new(std::fs::File::create(out.join("shard-000.ndjson")).unwrap());
    let mut emb = BufWriter::new(std::fs::File::create(out.join("emb-000.f32")).unwrap());
    let mut ids = BufWriter::new(std::fs::File::create(out.join("emb-000.ids")).unwrap());
    // Per-component noise sized so a member sits at cosine ~0.7 from its
    // center: tight enough to cluster, loose enough that neighbours are
    // not trivially separated — the regime real text embeddings live in.
    let noise = 1.0 / (dim as f32).sqrt();
    for i in 0..docs {
        let c = &centers[(rng.next_u64() % clusters as u64) as usize];
        let mut v: Vec<f32> = c.iter().map(|x| x + noise * rng.normal()).collect();
        normalize(&mut v);
        let id = format!("d{i:07}");
        writeln!(
            text,
            "{}",
            serde_json::json!({"id": id, "text": format!("doc {i}")})
        )
        .unwrap();
        for x in &v {
            emb.write_all(&x.to_le_bytes()).unwrap();
        }
        writeln!(ids, "{id}").unwrap();
    }
    println!(
        "wrote {docs} x {dim}-dim vectors around {clusters} centers to {}",
        out.display()
    );
}

/// Every `emb-*.f32` shard of `dir`, in order: `(ids, vectors)`.
fn load_embeddings(dir: &Path, dim: usize) -> (Vec<String>, Vec<Vec<f32>>) {
    let mut shards: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("emb-") && n.ends_with(".f32"))
        })
        .collect();
    shards.sort();
    let (mut all_ids, mut all_vecs) = (Vec::new(), Vec::new());
    for shard in shards {
        let ids = std::fs::read_to_string(shard.with_extension("ids")).unwrap();
        let mut bytes = Vec::new();
        std::fs::File::open(&shard)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        for (id, chunk) in ids.lines().zip(bytes.chunks_exact(dim * 4)) {
            all_ids.push(id.to_string());
            all_vecs.push(
                chunk
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|b| f32::from_le_bytes(*b))
                    .collect(),
            );
        }
    }
    (all_ids, all_vecs)
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn dir_bytes(dir: &Path, filter: impl Fn(&str) -> bool) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .filter(|e| filter(&e.file_name().to_string_lossy()))
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

fn segment_dirs(ns_dir: &Path) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(ns_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("footer.json").exists())
        .collect();
    dirs.sort();
    dirs
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

struct SetStats {
    vectors: u64,
    idx_bytes: u64,
    offsets_bytes: u64,
    doc_vectors_bytes: u64,
    total_bytes: u64,
    cold_knn_bytes: u64,
}

fn set_stats(segs: &[PathBuf], field: &str) -> SetStats {
    let mut s = SetStats {
        vectors: 0,
        idx_bytes: 0,
        offsets_bytes: 0,
        doc_vectors_bytes: 0,
        total_bytes: 0,
        cold_knn_bytes: 0,
    };
    for seg in segs {
        let footer = SegmentReader::read_footer(seg).unwrap();
        for meta in footer.vector_fields.iter().flatten() {
            if meta.name == field {
                s.vectors += meta.count as u64;
            }
        }
        s.idx_bytes += dir_bytes(seg, |n| n.starts_with("vector.") && n.ends_with(".idx"));
        s.offsets_bytes += dir_bytes(seg, |n| n.starts_with("vector.") && n.ends_with(".offsets"));
        s.doc_vectors_bytes += dir_bytes(seg, |n| n == kosha_segment::DOC_VECTORS_FILE);
        s.total_bytes += dir_bytes(seg, |_| true);
        for (file, _) in kosha_segment::knn_vector_files(seg, field) {
            s.cold_knn_bytes += std::fs::metadata(seg.join(file)).map_or(0, |m| m.len());
        }
    }
    s
}

/// Namespace top-`k` doc ids for `query`, merging each segment's top `k`.
fn search(
    readers: &[SegmentReader],
    field: &str,
    query: &[f32],
    k: usize,
    nprobe: usize,
) -> Vec<String> {
    let mut hits: Vec<(f64, String)> = Vec::new();
    for reader in readers {
        let seg_hits = reader
            .knn_search_filtered(field, query, k, nprobe, None)
            .expect("lazy index")
            .expect("probe");
        for (seq, score) in seg_hits {
            hits.push((score, reader.doc_meta(seq).unwrap().doc_id.to_string()));
        }
    }
    hits.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    hits.into_iter().take(k).map(|(_, id)| id).collect()
}

#[allow(clippy::too_many_arguments)]
fn eval(
    embeddings_dir: &Path,
    dim: usize,
    f32_dir: &Path,
    i8_dir: &Path,
    field: &str,
    queries: usize,
    nprobe: usize,
    k: usize,
    seed: u64,
) {
    let (ids, vectors) = load_embeddings(embeddings_dir, dim);
    println!("corpus: {} x {dim}-dim vectors", vectors.len());
    // Queries: a corpus vector plus noise, so each has real near
    // neighbours but is not itself in the corpus.
    let mut rng = Rng(seed | 1);
    let query_vecs: Vec<Vec<f32>> = (0..queries)
        .map(|_| {
            let base = &vectors[(rng.next_u64() % vectors.len() as u64) as usize];
            let mut q: Vec<f32> = base
                .iter()
                .map(|x| x + 0.5 * rng.normal() / (dim as f32).sqrt())
                .collect();
            normalize(&mut q);
            q
        })
        .collect();
    let truth: Vec<Vec<String>> = query_vecs
        .iter()
        .map(|q| {
            let mut scored: Vec<(f32, usize)> = vectors
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let n = dot(v, v).sqrt();
                    (if n > 0.0 { dot(q, v) / n } else { 0.0 }, i)
                })
                .collect();
            scored.select_nth_unstable_by(k - 1, |a, b| b.0.partial_cmp(&a.0).unwrap());
            scored.truncate(k);
            scored.into_iter().map(|(_, i)| ids[i].clone()).collect()
        })
        .collect();
    let recall = |got: &[Vec<String>]| -> f64 {
        got.iter()
            .zip(&truth)
            .map(|(g, t)| g.iter().filter(|id| t.contains(id)).count() as f64 / k as f64)
            .sum::<f64>()
            / got.len() as f64
    };

    let f32_segs = segment_dirs(f32_dir);
    let i8_segs = segment_dirs(i8_dir);
    let (fs, is) = (set_stats(&f32_segs, field), set_stats(&i8_segs, field));
    println!(
        "\n## Storage ({} f32 / {} i8 segments)\n",
        f32_segs.len(),
        i8_segs.len()
    );
    println!("| | f32 | i8 | ratio |");
    println!("|---|---|---|---|");
    let row = |name: &str, a: u64, b: u64| {
        println!(
            "| {name} | {} | {} | {:.2}x |",
            mib(a),
            mib(b),
            a as f64 / b.max(1) as f64
        );
    };
    let per_vec = |s: &SetStats| s.idx_bytes as f64 / s.vectors.max(1) as f64;
    println!(
        "| vector.idx bytes / vector | {:.1} | {:.1} | {:.2}x |",
        per_vec(&fs),
        per_vec(&is),
        per_vec(&fs) / per_vec(&is)
    );
    row("vector.idx total", fs.idx_bytes, is.idx_bytes);
    row("vector.offsets total", fs.offsets_bytes, is.offsets_bytes);
    row(
        "doc_vectors.bin total",
        fs.doc_vectors_bytes,
        is.doc_vectors_bytes,
    );
    row("all segment files", fs.total_bytes, is.total_bytes);
    row(
        "cold kNN hydrate (knn_vector_files)",
        fs.cold_knn_bytes,
        is.cold_knn_bytes,
    );

    let open = |segs: &[PathBuf], factor: usize| -> Vec<SegmentReader> {
        segs.iter()
            .map(|s| {
                SegmentReader::open(s.clone())
                    .unwrap()
                    .with_vector_rescore_factor(factor)
            })
            .collect()
    };
    // Run every query twice in a row (the cache admits a posting on its
    // second miss within a 256-posting window) and report what the decoded
    // posting cache then holds: the warm working set of this workload.
    let warm_bytes = |readers: &[SegmentReader]| -> u64 {
        for q in &query_vecs {
            for _ in 0..2 {
                search(readers, field, q, k, nprobe);
            }
        }
        readers
            .iter()
            .map(|r| r.vector_cache_resident_bytes() as u64)
            .sum()
    };
    let f32_readers = open(&f32_segs, 0);
    let i8_readers = open(&i8_segs, 0);
    println!("\n## Memory\n");
    println!(
        "(per-segment cap KOSHA_VECTOR_POSTINGS_CACHE_MAX_BYTES={})\n",
        std::env::var("KOSHA_VECTOR_POSTINGS_CACHE_MAX_BYTES")
            .unwrap_or_else(|_| "default 32 MiB".into())
    );
    println!("| | f32 | i8 | ratio |");
    println!("|---|---|---|---|");
    let (fw, iw) = (warm_bytes(&f32_readers), warm_bytes(&i8_readers));
    row(
        &format!("posting cache after the {queries} queries (nprobe {nprobe})"),
        fw,
        iw,
    );

    println!("\n## Recall@{k} vs f32 brute force ({queries} queries)\n");
    println!("| index | nprobe/segment | recall@{k} | ms/query |");
    println!("|---|---|---|---|");
    let run = |name: &str, readers: &[SegmentReader], nprobe: usize| {
        let t = Instant::now();
        let got: Vec<Vec<String>> = query_vecs
            .iter()
            .map(|q| search(readers, field, q, k, nprobe))
            .collect();
        let ms = t.elapsed().as_secs_f64() * 1000.0 / queries as f64;
        let probe = if nprobe == usize::MAX {
            "all".to_string()
        } else {
            nprobe.to_string()
        };
        println!("| {name} | {probe} | {:.4} | {ms:.2} |", recall(&got));
    };
    let mut by_factor: HashMap<usize, Vec<SegmentReader>> = HashMap::new();
    for factor in [1, 2, 4, 8] {
        by_factor.insert(factor, open(&i8_segs, factor));
    }
    for probe in [nprobe, usize::MAX] {
        run("f32", &f32_readers, probe);
        run("i8, no rescoring", &i8_readers, probe);
        for factor in [1, 2, 4, 8] {
            run(
                &format!("i8, rescore x{factor}"),
                &by_factor[&factor],
                probe,
            );
        }
    }
}

fn main() {
    match Cli::parse().command {
        Command::Gen {
            out,
            docs,
            dim,
            clusters,
            seed,
        } => gen(&out, docs, dim, clusters, seed),
        Command::Eval {
            embeddings_dir,
            dim,
            f32,
            i8,
            field,
            queries,
            nprobe,
            k,
            seed,
        } => eval(
            &embeddings_dir,
            dim,
            &f32,
            &i8,
            &field,
            queries,
            nprobe,
            k,
            seed,
        ),
    }
}
