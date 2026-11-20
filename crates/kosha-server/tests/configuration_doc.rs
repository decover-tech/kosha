//! Keeps `docs/configuration.md` in step with the code (#188): every
//! `KOSHA_*` environment variable named in a workspace `.rs` file must have
//! an entry in the doc, and every variable the doc names must still appear
//! in the code. `KOSHA_BENCH_*` (benchmark harness only) is exempt from the
//! first check.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

/// Every complete `KOSHA_[A-Z0-9_]+` token in `text`. A token ending in `_`
/// is a prefix (`KOSHA_BENCH_*`), not a variable, and is skipped.
fn kosha_vars(text: &str) -> BTreeSet<String> {
    let bytes = text.as_bytes();
    let mut out = BTreeSet::new();
    let mut from = 0;
    while let Some(pos) = text[from..].find("KOSHA_") {
        let start = from + pos;
        let preceded_by_ident = start > 0 && {
            let b = bytes[start - 1];
            b.is_ascii_alphanumeric() || b == b'_'
        };
        let mut end = start + "KOSHA_".len();
        while end < bytes.len()
            && (bytes[end].is_ascii_uppercase()
                || bytes[end].is_ascii_digit()
                || bytes[end] == b'_')
        {
            end += 1;
        }
        let token = &text[start..end];
        if !preceded_by_ident && token.len() > "KOSHA_".len() && !token.ends_with('_') {
            out.insert(token.to_string());
        }
        from = end;
    }
    out
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn vars_in_code(root: &Path) -> BTreeSet<String> {
    let mut files = Vec::new();
    rust_files(&root.join("crates"), &mut files);
    let this_file = Path::new(file!()).file_name().unwrap();
    files
        .iter()
        .filter(|p| p.file_name() != Some(this_file))
        .flat_map(|p| kosha_vars(&std::fs::read_to_string(p).expect("read .rs file")))
        .collect()
}

fn vars_in_doc(root: &Path) -> BTreeSet<String> {
    kosha_vars(
        &std::fs::read_to_string(root.join("docs/configuration.md"))
            .expect("docs/configuration.md exists"),
    )
}

#[test]
fn every_kosha_env_var_in_code_is_documented() {
    let root = workspace_root();
    let doc = vars_in_doc(&root);
    let missing: Vec<_> = vars_in_code(&root)
        .into_iter()
        .filter(|v| !v.starts_with("KOSHA_BENCH_") && !doc.contains(v))
        .collect();
    assert!(
        missing.is_empty(),
        "add these variables to docs/configuration.md (name, purpose, default, units): {missing:?}"
    );
}

#[test]
fn every_documented_kosha_env_var_exists_in_code() {
    let root = workspace_root();
    let code = vars_in_code(&root);
    let stale: Vec<_> = vars_in_doc(&root)
        .into_iter()
        .filter(|v| !code.contains(v))
        .collect();
    assert!(
        stale.is_empty(),
        "docs/configuration.md names variables no .rs file mentions: {stale:?}"
    );
}

#[test]
fn token_scanner_finds_whole_names_only() {
    let vars = kosha_vars("`KOSHA_A_B`, KOSHA_BENCH_* and MY_KOSHA_X; env(\"KOSHA_C1\")");
    assert_eq!(
        vars.into_iter().collect::<Vec<_>>(),
        vec!["KOSHA_A_B".to_string(), "KOSHA_C1".to_string()]
    );
}
