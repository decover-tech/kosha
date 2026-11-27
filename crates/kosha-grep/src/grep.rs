//! Candidate selection and exact verification.

use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use roaring::RoaringBitmap;

use crate::index::TrigramIndex;
use crate::plan::{has_newline_literal, parse, plan_hir, TrigramQuery};

/// Limits on one grep. Documents are read in id order, so a truncated
/// result is the deterministic prefix of the full one.
#[derive(Clone, Debug, Default)]
pub struct GrepOptions {
    /// `rg -i`.
    pub case_insensitive: bool,
    /// Stop before reading more than this many document bytes.
    pub max_bytes: Option<u64>,
    /// Stop once this much time has passed.
    pub max_time: Option<Duration>,
    /// Stop once this many lines matched.
    pub max_matches: Option<usize>,
}

/// One matching line, as `rg --line-number` reports it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct GrepMatch {
    pub doc: u32,
    /// 1-based.
    pub line_number: u64,
    /// Without its `\n`.
    pub line: Vec<u8>,
}

/// Which budget cut a grep short.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Truncation {
    Bytes,
    Time,
    Matches,
}

#[derive(Clone, Debug)]
pub struct GrepOutcome {
    pub matches: Vec<GrepMatch>,
    pub query: TrigramQuery,
    /// Documents the index could not rule out (after deletes).
    pub candidates: u64,
    /// Candidates fetched and verified.
    pub docs_read: u64,
    pub bytes_read: u64,
    /// `Some` when a budget stopped the grep before every candidate was
    /// verified; `matches` is then incomplete.
    pub truncated: Option<Truncation>,
}

#[derive(Debug)]
pub enum GrepError {
    Regex(regex::Error),
    Syntax(Box<regex_syntax::Error>),
    /// The pattern holds a literal `\n`, which never matches inside a line.
    Newline,
    Io(u32, io::Error),
}

impl fmt::Display for GrepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrepError::Regex(e) => write!(f, "{e}"),
            GrepError::Syntax(e) => write!(f, "{e}"),
            GrepError::Newline => f.write_str(r#"the literal "\n" is not allowed in a regex"#),
            GrepError::Io(doc, e) => write!(f, "reading document {doc}: {e}"),
        }
    }
}

impl std::error::Error for GrepError {}

/// Grep over one index (a segment), with its tombstones.
pub struct Grep<'a> {
    index: &'a TrigramIndex,
    deleted: Option<&'a RoaringBitmap>,
}

impl<'a> Grep<'a> {
    pub fn new(index: &'a TrigramIndex) -> Self {
        Grep {
            index,
            deleted: None,
        }
    }

    /// Documents to treat as deleted.
    pub fn with_deleted(mut self, deleted: &'a RoaringBitmap) -> Self {
        self.deleted = Some(deleted);
        self
    }

    /// The documents `query` cannot rule out.
    pub fn candidates(&self, query: &TrigramQuery) -> RoaringBitmap {
        let mut c = self.eval(query);
        if let Some(deleted) = self.deleted {
            c -= deleted;
        }
        c
    }

    fn eval(&self, query: &TrigramQuery) -> RoaringBitmap {
        match query {
            TrigramQuery::All => self.index.all(),
            TrigramQuery::Trigram(t) => self.index.postings(*t).cloned().unwrap_or_default(),
            TrigramQuery::And(qs) => {
                // Smallest posting lists first, stopping once empty.
                let mut parts: Vec<RoaringBitmap> = qs.iter().map(|q| self.eval(q)).collect();
                parts.sort_by_key(RoaringBitmap::len);
                let mut iter = parts.into_iter();
                let mut acc = iter.next().unwrap_or_else(|| self.index.all());
                for p in iter {
                    if acc.is_empty() {
                        break;
                    }
                    acc &= p;
                }
                acc
            }
            TrigramQuery::Or(qs) => qs
                .iter()
                .fold(RoaringBitmap::new(), |acc, q| acc | self.eval(q)),
        }
    }

    /// Runs `pattern`, fetching candidate documents with `fetch`.
    pub fn run<F, T>(
        &self,
        pattern: &str,
        opts: &GrepOptions,
        mut fetch: F,
    ) -> Result<GrepOutcome, GrepError>
    where
        F: FnMut(u32) -> io::Result<T>,
        T: AsRef<[u8]>,
    {
        let start = Instant::now();
        let hir = parse(pattern, opts.case_insensitive).map_err(GrepError::Syntax)?;
        if has_newline_literal(&hir) {
            return Err(GrepError::Newline);
        }
        let re = regex::bytes::RegexBuilder::new(pattern)
            .case_insensitive(opts.case_insensitive)
            .build()
            .map_err(GrepError::Regex)?;
        let query = plan_hir(&hir);
        let candidates = self.candidates(&query);
        let mut out = GrepOutcome {
            matches: Vec::new(),
            query,
            candidates: candidates.len(),
            docs_read: 0,
            bytes_read: 0,
            truncated: None,
        };
        'docs: for doc in &candidates {
            if opts.max_time.is_some_and(|t| start.elapsed() >= t) {
                out.truncated = Some(Truncation::Time);
                break;
            }
            let len = self.index.doc_len(doc);
            if opts.max_bytes.is_some_and(|max| out.bytes_read + len > max) {
                out.truncated = Some(Truncation::Bytes);
                break;
            }
            let text = fetch(doc).map_err(|e| GrepError::Io(doc, e))?;
            let text = text.as_ref();
            out.docs_read += 1;
            out.bytes_read += text.len() as u64;
            for (i, line) in lines(text).enumerate() {
                if re.is_match(line) {
                    if opts.max_matches.is_some_and(|m| out.matches.len() >= m) {
                        out.truncated = Some(Truncation::Matches);
                        break 'docs;
                    }
                    out.matches.push(GrepMatch {
                        doc,
                        line_number: i as u64 + 1,
                        line: line.to_vec(),
                    });
                }
            }
        }
        Ok(out)
    }
}

/// Lines as ripgrep counts them: split on `\n`, with no empty line after a
/// final `\n` and none at all in an empty document.
fn lines(text: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = text.strip_suffix(b"\n").unwrap_or(text);
    let empty = text.is_empty();
    body.split(|&b| b == b'\n').filter(move |_| !empty)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus() -> (TrigramIndex, Vec<Vec<u8>>) {
        let docs: Vec<Vec<u8>> = vec![
            b"fn main() {\n    println!(\"hello\");\n}\n".to_vec(),
            b"HELLO WORLD\n\nlast line no newline".to_vec(),
            b"nothing here\n".to_vec(),
            "caf\u{e9} \u{c9}cole\n".as_bytes().to_vec(),
            Vec::new(),
        ];
        let mut idx = TrigramIndex::new();
        for (i, d) in docs.iter().enumerate() {
            idx.add(format!("d{i}"), d);
        }
        (idx, docs)
    }

    fn run(pattern: &str, opts: GrepOptions) -> GrepOutcome {
        let (idx, docs) = corpus();
        Grep::new(&idx)
            .run(pattern, &opts, |d| {
                Ok::<_, io::Error>(docs[d as usize].as_slice())
            })
            .unwrap()
    }

    fn hits(o: &GrepOutcome) -> Vec<(u32, u64)> {
        o.matches.iter().map(|m| (m.doc, m.line_number)).collect()
    }

    #[test]
    fn selective_reads_only_candidates() {
        let o = run("hello", GrepOptions::default());
        assert_eq!(hits(&o), vec![(0, 2)]);
        // `HELLO` holds the folded trigrams too; verification rejects it.
        assert_eq!((o.candidates, o.docs_read), (2, 2));
        let o = run(
            "hello",
            GrepOptions {
                case_insensitive: true,
                ..Default::default()
            },
        );
        assert_eq!(hits(&o), vec![(0, 2), (1, 1)]);
    }

    #[test]
    fn line_numbering_matches_ripgrep() {
        assert_eq!(hits(&run("^$", GrepOptions::default())), vec![(1, 2)]);
        assert_eq!(hits(&run("newline$", GrepOptions::default())), vec![(1, 3)]);
    }

    #[test]
    fn unicode_case_folding() {
        let o = run("(?i)\u{e9}cole", GrepOptions::default());
        assert_eq!(hits(&o), vec![(3, 1)]);
        assert!(o.query.is_selective());
    }

    #[test]
    fn unselective_scans_within_budget() {
        let o = run(
            r"\w+",
            GrepOptions {
                max_bytes: Some(60),
                ..Default::default()
            },
        );
        assert_eq!(o.truncated, Some(Truncation::Bytes));
        assert_eq!(o.docs_read, 1);
        assert!(o.bytes_read <= 60);
        let o = run(
            r"\w+",
            GrepOptions {
                max_matches: Some(2),
                ..Default::default()
            },
        );
        assert_eq!(
            (o.truncated, o.matches.len()),
            (Some(Truncation::Matches), 2)
        );
        let o = run(
            r"\w+",
            GrepOptions {
                max_time: Some(Duration::ZERO),
                ..Default::default()
            },
        );
        assert_eq!((o.truncated, o.docs_read), (Some(Truncation::Time), 0));
        let o = run(r"\w+", GrepOptions::default());
        assert_eq!((o.truncated, o.candidates), (None, 5));
    }

    #[test]
    fn deletes_apply() {
        let (idx, docs) = corpus();
        let deleted: RoaringBitmap = [0].into_iter().collect();
        let o = Grep::new(&idx)
            .with_deleted(&deleted)
            .run("hello", &GrepOptions::default(), |d| {
                Ok::<_, io::Error>(docs[d as usize].as_slice())
            })
            .unwrap();
        assert!(o.matches.is_empty());
        assert_eq!(o.candidates, 1);
    }

    #[test]
    fn rejects_what_ripgrep_rejects() {
        let (idx, _) = corpus();
        let g = Grep::new(&idx);
        let fail = |_| -> io::Result<Vec<u8>> { unreachable!() };
        assert!(matches!(
            g.run("a\nb", &GrepOptions::default(), fail),
            Err(GrepError::Newline)
        ));
        assert!(matches!(
            g.run("(", &GrepOptions::default(), fail),
            Err(GrepError::Syntax(_))
        ));
        // Backreferences and lookaround: unsupported by `regex`, as by rg's default engine.
        assert!(g.run(r"(a)\1", &GrepOptions::default(), fail).is_err());
        assert!(g.run(r"a(?=b)", &GrepOptions::default(), fail).is_err());
    }
}
