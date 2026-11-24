//! The late-interaction evaluation harness (#205) end to end on a tiny
//! synthetic fixture: generate a corpus in the documented input format,
//! evaluate it, and check the report is complete and sane.

use std::path::Path;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_kosha-late-interaction-eval");

fn run(args: &[&str]) -> String {
    let out = Command::new(BIN)
        .args(args)
        .output()
        .expect("run the harness");
    assert!(
        out.status.success(),
        "{args:?} failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn path(p: &Path) -> &str {
    p.to_str().unwrap()
}

#[test]
fn harness_runs_on_a_synthetic_fixture() {
    let root = std::env::temp_dir().join(format!("kosha-li-eval-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let corpus = root.join("corpus");
    let work = root.join("work");
    let json = root.join("report.json");
    run(&[
        "gen",
        "--out",
        path(&corpus),
        "--docs",
        "240",
        "--queries",
        "24",
        "--dim",
        "16",
        "--tokens",
        "12",
    ]);
    for f in ["docs.jsonl", "queries.jsonl", "qrels.tsv"] {
        assert!(corpus.join(f).exists(), "{f}");
    }
    // A judgment for a document that is not in the corpus, and a query
    // without judgments: both are tolerated.
    let mut qrels = std::fs::read_to_string(corpus.join("qrels.tsv")).unwrap();
    qrels.push_str("Q0\tmissing-doc\t2\n");
    std::fs::write(corpus.join("qrels.tsv"), qrels).unwrap();
    let mut queries = std::fs::read_to_string(corpus.join("queries.jsonl")).unwrap();
    queries.push_str("{\"id\": \"unjudged\", \"tokens\": [[1.0, 0.0]]}\n");
    std::fs::write(corpus.join("queries.jsonl"), queries).unwrap();

    let stdout = run(&[
        "eval",
        "--corpus",
        path(&corpus),
        "--work-dir",
        path(&work),
        "--segment-docs",
        "100",
        "--nprobe",
        "4",
        "--num-candidates",
        "20",
        "--json",
        path(&json),
    ]);
    assert!(stdout.contains("exact MaxSim"), "{stdout}");

    let report: serde_json::Value = serde_json::from_slice(&std::fs::read(&json).unwrap()).unwrap();
    assert_eq!(report["docs"], 240);
    assert_eq!(report["dim"], 16);
    assert_eq!(report["segments"], 3, "240 docs at 100 per segment");
    assert_eq!(report["k"], 10);
    assert_eq!(report["queries_evaluated"], 24);
    assert_eq!(report["queries_without_judgments"], 1);
    let metric = |method: &str, name: &str| report[method][name].as_f64().unwrap();
    for method in ["late_interaction", "exact_maxsim", "single_vector_pooled"] {
        for name in ["recall_at_k", "ndcg_at_k", "overlap_with_exact"] {
            let v = metric(method, name);
            assert!((0.0..=1.0).contains(&v), "{method}.{name} = {v}");
        }
    }
    assert_eq!(metric("exact_maxsim", "overlap_with_exact"), 1.0);
    // Each query is a noisy copy of part of its relevant document, so
    // exact MaxSim finds nearly all of them; the index is close to exact;
    // a mean-pooled vector blurs the document's topics and ranks it lower.
    let exact = metric("exact_maxsim", "recall_at_k");
    let li = metric("late_interaction", "recall_at_k");
    assert!(exact >= 0.9, "exact MaxSim recall@10 {exact}");
    assert!(li >= exact - 0.15, "late interaction {li} vs exact {exact}");
    let exact_ndcg = metric("exact_maxsim", "ndcg_at_k");
    let pooled_ndcg = metric("single_vector_pooled", "ndcg_at_k");
    assert!(
        pooled_ndcg < exact_ndcg - 0.1,
        "pooled nDCG@10 {pooled_ndcg} vs exact {exact_ndcg}"
    );
    assert!(metric("late_interaction", "overlap_with_exact") >= 0.7);
    assert!(metric("single_vector_pooled", "overlap_with_exact") < 0.9);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn trec_qrels_and_skip_exact_are_supported() {
    let root = std::env::temp_dir().join(format!("kosha-li-eval-trec-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let docs = [
        ("a", vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]]),
        ("b", vec![vec![0.0, 0.0, 1.0]]),
        ("c", vec![vec![0.7, 0.7, 0.0], vec![0.0, 0.0, 1.0]]),
    ];
    let lines: Vec<String> = docs
        .iter()
        .map(|(id, t)| serde_json::json!({"id": id, "tokens": t}).to_string())
        .collect();
    std::fs::write(root.join("docs.jsonl"), lines.join("\n")).unwrap();
    std::fs::write(
        root.join("queries.jsonl"),
        serde_json::json!({"id": "q", "tokens": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]}).to_string(),
    )
    .unwrap();
    // TREC: query 0 doc relevance; graded.
    std::fs::write(root.join("qrels.tsv"), "q 0 a 2\nq 0 c 1\nq 0 b 0\n").unwrap();
    let json = root.join("r.json");
    run(&[
        "eval",
        "--corpus",
        path(&root),
        "--k",
        "2",
        "--skip-exact",
        "--json",
        path(&json),
    ]);
    let report: serde_json::Value = serde_json::from_slice(&std::fs::read(&json).unwrap()).unwrap();
    assert!(report["exact_maxsim"].is_null());
    assert!(report["late_interaction"]["overlap_with_exact"].is_null());
    // MaxSim ranks a (2.0) then c (~1.41): both relevant, ideal order.
    assert_eq!(report["late_interaction"]["recall_at_k"], 1.0);
    assert!((report["late_interaction"]["ndcg_at_k"].as_f64().unwrap() - 1.0).abs() < 1e-9);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_non_empty_work_dir_is_refused_and_left_intact() {
    let root = std::env::temp_dir().join(format!("kosha-li-eval-work-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let corpus = root.join("corpus");
    run(&[
        "gen",
        "--out",
        path(&corpus),
        "--docs",
        "20",
        "--queries",
        "2",
        "--dim",
        "8",
        "--tokens",
        "4",
    ]);
    let out = Command::new(BIN)
        .args([
            "eval",
            "--corpus",
            path(&corpus),
            "--work-dir",
            path(&corpus),
        ])
        .output()
        .expect("run the harness");
    assert!(
        !out.status.success(),
        "a non-empty --work-dir must be refused"
    );
    for f in ["docs.jsonl", "queries.jsonl", "qrels.tsv"] {
        assert!(corpus.join(f).exists(), "{f} was deleted");
    }
    let _ = std::fs::remove_dir_all(&root);
}
