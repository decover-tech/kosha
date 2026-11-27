//! Exact agreement with ripgrep on a corpus of edge cases. Skipped when
//! `rg` is not installed.

use std::fs;
use std::path::{Path, PathBuf};

use kosha_grep::corpus::{ripgrep, Corpus};
use kosha_grep::{Grep, GrepOptions};

fn rg() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|d| d.join("rg"))
        .find(|p| p.is_file())
}

fn write_corpus(root: &Path) {
    let _ = fs::remove_dir_all(root);
    fs::create_dir_all(root.join("src/nested")).unwrap();
    let files: &[(&str, &[u8])] = &[
        ("src/main.rs", b"fn main() {\n    let colour = 123;\n    println!(\"Hello, {}\", colour);\n}\n"),
        ("src/nested/lib.rs", b"pub fn color() -> u32 { 555 }\n// TODO: fix 555-1234\n\n\n"),
        ("crlf.txt", b"windows line\r\nanother\r\n\r\nend"),
        ("unicode.txt", "Caf\u{e9} \u{c9}cole \u{e9}cole\nKelvin \u{212a} and kelvin k\nstra\u{df}e STRASSE\nlong \u{17f} s\n".as_bytes()),
        ("invalid.bin", b"ok text\n\xff\xfe broken \xff utf8\nhello\x00nul byte\n"),
        ("bom.txt", b"\xef\xbb\xbfbom first line\nsecond\n"),
        ("empty.txt", b""),
        ("only-newline.txt", b"\n"),
        ("no-trailing.txt", b"first\nlast hello"),
        (".hidden", b"hidden hello\n"),
        (".gitignore", b"ignored.txt\n"),
        ("ignored.txt", b"ignored but searched hello\n"),
        ("short.txt", b"ab\nabc\na\n"),
    ];
    for (name, body) in files {
        fs::write(root.join(name), body).unwrap();
    }
}

const PATTERNS: &[(&str, bool)] = &[
    ("hello", false),
    ("hello", true),
    ("Hello", false),
    ("colou?r", false),
    ("color|colour", false),
    (r"\bfn\b", false),
    (r"fn\s+\w+", false),
    (r"\d{3}-\d{4}", false),
    (r"\d+", false),
    ("^$", false),
    ("^", false),
    (".", false),
    (r"\w+", false),
    (r"\r$", false),
    ("line$", false),
    ("^last", false),
    ("Caf\u{e9}", false),
    ("(?i)\u{e9}cole", false),
    ("\u{e9}cole", true),
    ("kelvin", true),
    ("(?i)k", false),
    ("(?i)strasse", false),
    ("(?i)long s", false),
    (r"(?-u:\xff)", false),
    (r"\x00", false),
    ("broken", false),
    ("bom", false),
    ("^bom", false),
    ("ab", false),
    ("abc", false),
    ("[a-c]{3}", false),
    ("TODO|FIXME", false),
    ("(?i)todo: fix", false),
    ("nothing matches this zzq", false),
    (r"[^\x00-\x7f]", false),
];

#[test]
fn agrees_with_ripgrep() {
    let Some(rg) = rg() else {
        eprintln!("rg not on PATH; skipping");
        return;
    };
    let root = std::env::temp_dir().join(format!("kosha-grep-rg-{}", std::process::id()));
    write_corpus(&root);
    // Whole files, and blocks small enough to cut most files many times.
    for block in [None, Some(1), Some(8), Some(64)] {
        let mut texts = Vec::new();
        let corpus = Corpus::load(&root, block, |t| texts.push(t)).unwrap();
        let grep = Grep::new(&corpus.index);
        for &(pattern, ci) in PATTERNS {
            let opts = GrepOptions {
                case_insensitive: ci,
                ..Default::default()
            };
            let ours = grep
                .run(pattern, &opts, |d| {
                    Ok::<_, std::io::Error>(texts[d as usize].as_slice())
                })
                .unwrap();
            assert!(ours.truncated.is_none());
            let expected = ripgrep(&rg, &root, pattern, ci).unwrap();
            // Guards against a pattern agreeing only because both find nothing.
            // `^bom` must not match: with no transcoding the BOM bytes come first.
            let none = ["nothing matches this zzq", "^bom"].contains(&pattern);
            assert_eq!(
                expected.is_empty(),
                none,
                "{pattern:?}: rg found {}",
                expected.len()
            );
            assert_eq!(
                corpus.hits(&ours.matches),
                expected,
                "{pattern:?} (-i: {ci}, block: {block:?})"
            );
            // Ranged reads from disk return the same documents.
            let cold = grep.run(pattern, &opts, |d| corpus.read(d)).unwrap();
            assert_eq!(cold.matches, ours.matches);
        }
    }
    fs::remove_dir_all(&root).unwrap();
}
