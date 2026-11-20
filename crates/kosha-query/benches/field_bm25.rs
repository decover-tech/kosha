//! Per-field indexes and multi-field BM25 (issue #249): what they cost.
//!
//! Builds the same synthetic two-field corpus (`title` ~6 words, `content`
//! ~60 words, Zipfian vocabulary) with the per-field index on and off, and
//! reports segment build time, inverted-index bytes on disk, and warm
//! query latency for combined-index queries (block-max WAND paths) next
//! to field-restricted ones (exhaustive per-field path).
//!
//! ```text
//! cargo bench -p kosha-query --bench field_bm25
//! KOSHA_BENCH_DOCS=200000 cargo bench -p kosha-query --bench field_bm25
//! ```

use std::path::{Path, PathBuf};
use std::time::Instant;

use kosha_core::{
    Bm25Params, DocumentId, Field, Manifest, ManifestEntry, MatchPhraseQuery, NamespaceId,
    SearchQuery, SegmentId,
};
use kosha_query::Searcher;
use kosha_segment::SegmentWriter;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// Zipf-ish word id in `0..vocab`.
    fn word(&mut self, vocab: u64) -> u64 {
        let u = (self.next() % 1_000_000) as f64 / 1_000_000.0;
        ((vocab as f64).powf(u) - 1.0) as u64
    }

    fn text(&mut self, words: usize, vocab: u64) -> String {
        (0..words)
            .map(|_| format!("w{}", self.word(vocab)))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

const SEGMENTS: usize = 4;

fn build(root: &Path, docs: usize, field_index: bool) -> (Manifest, f64, u64) {
    let _ = std::fs::remove_dir_all(root);
    let mut manifest = Manifest {
        version: 1,
        segments: Vec::new(),
        segment_footers: Default::default(),
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
        schema: None,
        retired: Default::default(),
        shards: None,
        branch: None,
    };
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let start = Instant::now();
    let per_segment = docs / SEGMENTS;
    for s in 0..SEGMENTS {
        let id = SegmentId(format!("s{s}"));
        let mut writer = SegmentWriter::new(id.clone(), root.join("ns").join(&id.0))
            .with_field_index(field_index);
        for i in 0..per_segment {
            writer.add_document(
                DocumentId(format!("d{s}-{i}")),
                vec![
                    Field::text("title", rng.text(6, 5_000)),
                    Field::text("content", rng.text(60, 50_000)),
                ],
            );
        }
        let footer = writer.finalize(Bm25Params::default()).unwrap();
        manifest.segments.push(ManifestEntry {
            segment_id: id,
            doc_count: footer.doc_count,
        });
    }
    let build_secs = start.elapsed().as_secs_f64();
    let mut inverted_bytes = 0u64;
    for s in 0..SEGMENTS {
        for entry in std::fs::read_dir(root.join("ns").join(format!("s{s}"))).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "inverted.idx" || name.starts_with("postings-") {
                inverted_bytes += entry.metadata().unwrap().len();
            }
        }
    }
    (manifest, build_secs, inverted_bytes)
}

fn query(text: &str, fields: &[&str]) -> SearchQuery {
    serde_json::from_value(serde_json::json!({
        "query_text": text,
        "max_results": 10,
        "fields": fields,
        "no_cache": true,
    }))
    .unwrap()
}

fn phrase(text: &str, field: &str) -> SearchQuery {
    let mut q = query("", &[]);
    q.match_phrase = Some(MatchPhraseQuery {
        field: field.into(),
        phrase: text.into(),
        slop: 0,
        fields: Vec::new(),
    });
    q
}

/// Median warm latency in ms over `runs` searches (after one warm-up).
fn time(searcher: &Searcher, manifest: &Manifest, q: &SearchQuery, runs: usize) -> (f64, usize) {
    let ns = NamespaceId("ns".into());
    let hits = searcher.search(&ns, manifest, q, None).unwrap().total_hits;
    let mut samples: Vec<f64> = (0..runs)
        .map(|_| {
            let start = Instant::now();
            std::hint::black_box(searcher.search(&ns, manifest, q, None).unwrap());
            start.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (samples[samples.len() / 2], hits)
}

fn main() {
    let docs: usize = std::env::var("KOSHA_BENCH_DOCS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let base = std::env::temp_dir().join(format!("kosha-field-bm25-{}", std::process::id()));
    let off_root: PathBuf = base.join("off");
    let on_root: PathBuf = base.join("on");
    let (_, off_secs, off_bytes) = build(&off_root, docs, false);
    let (manifest, on_secs, on_bytes) = build(&on_root, docs, true);
    println!("{docs} docs (title ~6 words, content ~60 words) in {SEGMENTS} segments");
    println!(
        "{:<24} {:>12} {:>16}",
        "field index", "build s", "inverted MB"
    );
    println!(
        "{:<24} {:>12.2} {:>16.1}",
        "off (pre-#249)",
        off_secs,
        off_bytes as f64 / 1e6
    );
    println!(
        "{:<24} {:>12.2} {:>16.1}   ({:.2}x build, {:.2}x bytes)",
        "on",
        on_secs,
        on_bytes as f64 / 1e6,
        on_secs / off_secs,
        on_bytes as f64 / off_bytes as f64
    );

    let searcher = Searcher::new(on_root.clone());
    println!();
    println!(
        "{:<44} {:>10} {:>10}",
        "warm query (field index on)", "p50 ms", "hits"
    );
    let cases: Vec<(&str, SearchQuery)> = vec![
        ("w3 (combined, WAND)", query("w3", &[])),
        ("w3 fields [content]", query("w3", &["content"])),
        (
            "w3 fields [title^2, content]",
            query("w3", &["title^2", "content"]),
        ),
        ("w3 w40 (combined, WAND AND)", query("w3 w40", &[])),
        (
            "w3 w40 fields [title^2, content]",
            query("w3 w40", &["title^2", "content"]),
        ),
        ("w900 (combined, rare)", query("w900", &[])),
        (
            "w900 fields [title^2, content]",
            query("w900", &["title^2", "content"]),
        ),
        ("phrase \"w1 w2\" (any field)", phrase("w1 w2", "_all")),
        ("phrase \"w1 w2\" in content", phrase("w1 w2", "content")),
    ];
    for (label, q) in cases {
        let (p50, hits) = time(&searcher, &manifest, &q, 15);
        println!("{label:<44} {p50:>10.2} {hits:>10}");
    }
    let _ = std::fs::remove_dir_all(&base);
}
