//! A directory as a grep corpus, and ripgrep as the oracle.

use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;

use walkdir::WalkDir;

use crate::{GrepMatch, TrigramIndex};

/// Every regular file under a root, indexed, in walk order. Symlinks are
/// skipped and hidden or ignored files are kept, as `rg --hidden
/// --no-ignore` does.
///
/// With a block size, each file is indexed as blocks of whole lines of
/// about that many bytes, so a candidate is a byte range of a file (one
/// ranged read) rather than the whole file.
pub struct Corpus {
    pub root: PathBuf,
    pub index: TrigramIndex,
    /// Paths relative to `root`.
    pub paths: Vec<PathBuf>,
    /// By document id.
    pub blocks: Vec<Block>,
}

/// One indexed document: a run of whole lines of a file.
#[derive(Clone, Debug)]
pub struct Block {
    pub file: u32,
    /// 1-based line number of the block's first line.
    pub first_line: u64,
    pub start: u64,
    pub end: u64,
}

impl Corpus {
    /// Builds the index, handing each document's bytes to `keep` (e.g. to
    /// hold them in memory for warm runs). `block_bytes` of `None` indexes
    /// whole files.
    pub fn load(
        root: &Path,
        block_bytes: Option<usize>,
        mut keep: impl FnMut(Vec<u8>),
    ) -> io::Result<Corpus> {
        let mut index = TrigramIndex::new();
        let mut paths = Vec::new();
        let mut blocks = Vec::new();
        for entry in WalkDir::new(root).sort_by_file_name() {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let rel = entry
                .path()
                .strip_prefix(root)
                .unwrap_or(entry.path())
                .to_path_buf();
            let text = fs::read(entry.path())?;
            let file = paths.len() as u32;
            for (first_line, range) in split(&text, block_bytes) {
                let body = &text[range.clone()];
                index.add(format!("{}#{first_line}", rel.to_string_lossy()), body);
                blocks.push(Block {
                    file,
                    first_line,
                    start: range.start as u64,
                    end: range.end as u64,
                });
                keep(body.to_vec());
            }
            paths.push(rel);
        }
        Ok(Corpus {
            root: root.to_path_buf(),
            index,
            paths,
            blocks,
        })
    }

    /// Reads document `doc` from disk: one ranged read.
    pub fn read(&self, doc: u32) -> io::Result<Vec<u8>> {
        let b = &self.blocks[doc as usize];
        let mut f = fs::File::open(self.root.join(&self.paths[b.file as usize]))?;
        f.seek(SeekFrom::Start(b.start))?;
        let mut buf = vec![0; (b.end - b.start) as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// `(path, line number)` of each match.
    pub fn hits(&self, matches: &[GrepMatch]) -> BTreeSet<(PathBuf, u64)> {
        matches
            .iter()
            .map(|m| {
                let b = &self.blocks[m.doc as usize];
                (
                    self.paths[b.file as usize].clone(),
                    b.first_line + m.line_number - 1,
                )
            })
            .collect()
    }
}

/// `(first line, byte range)` of each block of `text`: whole lines, cut once
/// a block reaches `block_bytes`. An empty text is one empty block.
fn split(text: &[u8], block_bytes: Option<usize>) -> Vec<(u64, Range<usize>)> {
    let Some(size) = block_bytes else {
        return vec![(1, 0..text.len())];
    };
    let mut out = Vec::new();
    let (mut start, mut first_line, mut line) = (0, 1, 1);
    for (i, &b) in text.iter().enumerate() {
        if b == b'\n' {
            line += 1;
            if i + 1 - start >= size {
                out.push((first_line, start..i + 1));
                (start, first_line) = (i + 1, line);
            }
        }
    }
    if start < text.len() || out.is_empty() {
        out.push((first_line, start..text.len()));
    }
    out
}

/// The ripgrep invocation the prototype must agree with: every file as
/// text, no ignore files, no config, no BOM transcoding.
pub fn ripgrep_command(rg: &Path, root: &Path, pattern: &str, case_insensitive: bool) -> Command {
    let mut cmd = Command::new(rg);
    cmd.args([
        "--no-config",
        "--text",
        "--hidden",
        "--no-ignore",
        "--encoding",
        "none",
        "--no-heading",
        "--with-filename",
        "--line-number",
        "--null",
        "--no-messages",
    ]);
    if case_insensitive {
        cmd.arg("--ignore-case");
    }
    cmd.arg("--regexp").arg(pattern).arg(root);
    cmd
}

/// `(path relative to root, line number)` of every line ripgrep matches.
pub fn ripgrep(
    rg: &Path,
    root: &Path,
    pattern: &str,
    case_insensitive: bool,
) -> io::Result<BTreeSet<(PathBuf, u64)>> {
    let out = ripgrep_command(rg, root, pattern, case_insensitive).output()?;
    match out.status.code() {
        Some(0 | 1) => {}
        _ => {
            return Err(io::Error::other(format!(
                "rg failed on {pattern:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            )))
        }
    }
    let mut hits = BTreeSet::new();
    for record in out.stdout.split(|&b| b == b'\n').filter(|r| !r.is_empty()) {
        let nul = record
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| io::Error::other("rg output without --null"))?;
        let path = Path::new(&*String::from_utf8_lossy(&record[..nul])).to_path_buf();
        let rest = &record[nul + 1..];
        let colon = rest
            .iter()
            .position(|&b| b == b':')
            .ok_or_else(|| io::Error::other("rg output without a line number"))?;
        let line: u64 = std::str::from_utf8(&rest[..colon])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| io::Error::other("bad rg line number"))?;
        hits.insert((path.strip_prefix(root).unwrap_or(&path).to_path_buf(), line));
    }
    Ok(hits)
}
