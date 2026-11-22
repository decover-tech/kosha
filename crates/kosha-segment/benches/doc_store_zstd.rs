//! Doc-store size and record-read latency, raw vs per-record zstd with a
//! per-segment trained dictionary (research #388).
//!
//! Builds one segment per configuration from the same passages and
//! reports, per configuration:
//!   - `doc_store.bin` bytes (and bytes/doc, ratio vs raw), plus the
//!     `doc_store.offsets` bytes, which carry the dictionary trailer. A
//!     compressed file also ends with a second copy of the dictionary (so
//!     the offsets can be rebuilt), so "rec B/doc" and "rec ratio" count
//!     the stored records alone, the bytes ranged page reads fetch;
//!   - build time of `finalize` (includes dictionary training and the
//!     compression pass);
//!   - `doc_record_full` latency on random documents with the file in the
//!     OS page cache (p50/p99, per record), and a 10-document "page"
//!     (p50/p99), which is what `materialize_ms` pays per query;
//!   - `doc_record_from_bytes` on spans already in memory, i.e. decode +
//!     parse only, without the read syscall.
//!
//! Passages: `KOSHA_BENCH_PASSAGES_DIR` names a directory of `.txt` files
//! that are cut into MSMarco-sized passages (40-80 words, ~330 bytes);
//! without it, synthetic Zipf-distributed text is used, which compresses
//! very differently from real prose, so prefer real text.
//!
//! ```text
//! KOSHA_BENCH_PASSAGES_DIR=/path/to/txt KOSHA_BENCH_DOCS=50000 \
//!   cargo bench -p kosha-segment --bench doc_store_zstd
//! ```
//!
//! Local numbers are from the machine the bench runs on (page-cache reads,
//! no NVMe or S3); the bench VM decides the real `materialize_ms`.

use std::path::{Path, PathBuf};
use std::time::Instant;

use kosha_core::{Bm25Params, DocumentId, Field, SegmentId};
use kosha_segment::{DocStoreZstdConfig, SegmentReader, SegmentWriter};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Passages cut from real text files, or synthetic ones.
fn passages(count: usize) -> (Vec<String>, String) {
    let mut rng = Lcg(0x388);
    if let Ok(dir) = std::env::var("KOSHA_BENCH_PASSAGES_DIR") {
        let mut words: Vec<String> = Vec::new();
        let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
            .expect("KOSHA_BENCH_PASSAGES_DIR readable")
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "txt"))
            .collect();
        files.sort();
        for f in &files {
            let text = std::fs::read_to_string(f).unwrap_or_default();
            words.extend(
                text.split_whitespace()
                    .filter(|w| w.chars().any(char::is_alphabetic))
                    .map(str::to_string),
            );
        }
        assert!(words.len() > 1000, "too little text in {dir}");
        let mut out = Vec::with_capacity(count);
        let mut at = 0;
        while out.len() < count {
            let len = 40 + rng.below(41);
            if at + len > words.len() {
                at = rng.below(words.len() / 2);
            }
            out.push(words[at..at + len].join(" "));
            at += len;
        }
        let source = format!(
            "{} words from {} .txt files in KOSHA_BENCH_PASSAGES_DIR (passages wrap and repeat \
             past {} words)",
            words.len(),
            files.len(),
            words.len()
        );
        return (out, source);
    }
    // Synthetic: Zipf-ish draw over a 20k-word vocabulary.
    let vocab: Vec<String> = (0..20_000)
        .map(|i| {
            let mut w = String::new();
            let mut x = i as u64 * 2654435761 + 17;
            for _ in 0..(3 + i % 7) {
                w.push((b'a' + (x % 26) as u8) as char);
                x /= 7;
                x += i as u64;
            }
            w
        })
        .collect();
    let out = (0..count)
        .map(|_| {
            let len = 40 + rng.below(41);
            (0..len)
                .map(|_| {
                    let r = rng.below(1 << 20) as f64 / (1u64 << 20) as f64;
                    let idx = ((vocab.len() as f64).powf(r) as usize).min(vocab.len() - 1);
                    vocab[idx].as_str()
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect();
    (
        out,
        "synthetic Zipf text (set KOSHA_BENCH_PASSAGES_DIR for real prose)".into(),
    )
}

fn build(dir: &Path, passages: &[String], zstd: Option<DocStoreZstdConfig>) -> f64 {
    let _ = std::fs::remove_dir_all(dir);
    let mut w = SegmentWriter::new(SegmentId("bench".into()), dir.to_path_buf())
        .with_field_index(false)
        .with_doc_store_zstd(zstd);
    for (i, p) in passages.iter().enumerate() {
        // MSMarco-shaped: a numeric id and one `text` field.
        w.add_document(
            DocumentId(format!("{}", 7_000_000 + i)),
            vec![Field::text("text", p.clone())],
        );
    }
    let t = Instant::now();
    w.finalize(Bm25Params::default()).unwrap();
    t.elapsed().as_secs_f64() * 1e3
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() as f64 - 1.0) * p).round() as usize]
}

fn main() {
    let docs = env_usize("KOSHA_BENCH_DOCS", 50_000);
    let reads = env_usize("KOSHA_BENCH_READS", 20_000);
    let (passages, source) = passages(docs);
    let raw_text: usize = passages.iter().map(String::len).sum();
    println!(
        "doc_store_zstd bench: {docs} docs, {:.0} B mean passage, source: {source}",
        raw_text as f64 / docs as f64
    );
    let root =
        std::env::temp_dir().join(format!("kosha-bench-docstore-zstd-{}", std::process::id()));
    let configs: Vec<(&str, Option<DocStoreZstdConfig>)> = vec![
        ("raw (default)", None),
        (
            "zstd-3 no dict",
            Some(DocStoreZstdConfig::with_dict_bytes(3, 0)),
        ),
        (
            "zstd-3 dict 16K",
            Some(DocStoreZstdConfig::with_dict_bytes(3, 16 * 1024)),
        ),
        (
            "zstd-3 dict 64K",
            Some(DocStoreZstdConfig::with_dict_bytes(3, 64 * 1024)),
        ),
        (
            "zstd-3 dict 112K",
            Some(DocStoreZstdConfig::with_dict_bytes(3, 112 * 1024)),
        ),
        (
            "zstd-9 dict 64K",
            Some(DocStoreZstdConfig::with_dict_bytes(9, 64 * 1024)),
        ),
        (
            "zstd-19 dict 64K",
            Some(DocStoreZstdConfig::with_dict_bytes(19, 64 * 1024)),
        ),
    ];
    println!(
        "{:<18} {:>12} {:>8} {:>7} {:>9} {:>9} {:>10} {:>9} {:>9} {:>9} {:>10} {:>10} {:>9} {:>9}",
        "config",
        "doc_store B",
        "B/doc",
        "ratio",
        "rec B/doc",
        "rec ratio",
        "offsets B",
        "build ms",
        "rec p50us",
        "rec p99us",
        "page10 p50",
        "page10 p99",
        "dec p50us",
        "dec p99us"
    );
    let mut raw_size = 0u64;
    for (name, config) in configs {
        let dir = root.join(name.replace([' ', '(', ')'], "_"));
        let build_ms = build(&dir, &passages, config);
        let size = std::fs::metadata(dir.join("doc_store.bin")).unwrap().len();
        let offsets = std::fs::metadata(dir.join("doc_store.offsets"))
            .unwrap()
            .len();
        if config.is_none() {
            raw_size = size;
        }
        let reader = SegmentReader::open(dir.clone()).unwrap();
        // Stored records alone: where the last record ends.
        let records = reader.doc_store_len().unwrap();
        let mut rng = Lcg(42);
        // Warm the page cache and the decoder pool.
        for seq in 0..reader.doc_count().min(2000) {
            reader.doc_record_full(seq).unwrap();
        }
        let n = reader.doc_count() as usize;
        let mut per_rec = Vec::with_capacity(reads);
        for _ in 0..reads {
            let seq = rng.below(n) as u32;
            let t = Instant::now();
            let rec = reader.doc_record_full(seq).unwrap().unwrap();
            per_rec.push(t.elapsed().as_secs_f64() * 1e6);
            std::hint::black_box(rec);
        }
        let mut pages = Vec::with_capacity(reads / 10);
        for _ in 0..reads / 10 {
            let seqs: Vec<u32> = (0..10).map(|_| rng.below(n) as u32).collect();
            let t = Instant::now();
            for &seq in &seqs {
                std::hint::black_box(reader.doc_record_full(seq).unwrap());
            }
            pages.push(t.elapsed().as_secs_f64() * 1e3);
        }
        let store = std::fs::read(dir.join("doc_store.bin")).unwrap();
        let mut decode = Vec::with_capacity(reads);
        for _ in 0..reads {
            let seq = rng.below(n) as u32;
            let (off, len) = reader.doc_span(seq).unwrap();
            let bytes = &store[off as usize..off as usize + len as usize];
            let t = Instant::now();
            let rec = reader.doc_record_from_bytes(seq, bytes).unwrap();
            decode.push(t.elapsed().as_secs_f64() * 1e6);
            std::hint::black_box(rec);
        }
        for v in [&mut per_rec, &mut pages, &mut decode] {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        }
        println!(
            "{:<18} {:>12} {:>8.1} {:>6.2}x {:>9.1} {:>8.2}x {:>10} {:>9.0} {:>9.2} {:>9.2} {:>9.3}ms {:>9.3}ms {:>9.2} {:>9.2}",
            name,
            size,
            size as f64 / n as f64,
            raw_size as f64 / size as f64,
            records as f64 / n as f64,
            raw_size as f64 / records as f64,
            offsets,
            build_ms,
            pct(&per_rec, 0.5),
            pct(&per_rec, 0.99),
            pct(&pages, 0.5),
            pct(&pages, 0.99),
            pct(&decode, 0.5),
            pct(&decode, 0.99),
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
    let _ = std::fs::remove_dir_all(&root);
}
