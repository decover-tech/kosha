//! `POST /v1/namespaces/{namespace}/read` (#586): one stored field of one
//! document, whole or as a line, page or byte range, sized for an agent's
//! context window.
//!
//! The document is found with the search path (a seek on `_id`, or a
//! `term` on a keyword path field), so sharding, branches, consistency and the ranged
//! doc-store reads of #245 apply unchanged: only the document's own record
//! is fetched. The range is cut from the stored value here; pages are the
//! form-feed (`\f`) separated parts of the value, as `pdftotext` writes
//! them.

use serde::Deserialize;

use crate::api_error::{api_error, ErrorCode};
use crate::{handle_search_post_with_ns, json_ok, response_body_json, AppState};

/// Bytes returned when the request sets no `max_bytes`.
pub(crate) const DEFAULT_MAX_BYTES: usize = 64 * 1024;
/// The largest `max_bytes` a request may ask for.
pub(crate) const MAX_MAX_BYTES: usize = 1024 * 1024;

const PAGE_BREAK: char = '\x0c';

#[derive(Debug, Deserialize)]
struct ReadRequest {
    id: Option<String>,
    path: Option<String>,
    path_field: Option<String>,
    field: Option<String>,
    lines: Option<LineRange>,
    page: Option<u32>,
    bytes: Option<ByteRange>,
    max_bytes: Option<u32>,
}

/// 1-based, inclusive; `end` defaults to the last line.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LineRange {
    pub start: u32,
    pub end: Option<u32>,
}

/// 0-based, end-exclusive; `end` defaults to the end of the value.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ByteRange {
    pub start: u32,
    pub end: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Range {
    Whole,
    Lines(LineRange),
    Page(u32),
    Bytes(ByteRange),
}

/// The cut a range selects, with the references an agent cites it by.
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub(crate) struct Slice<'a> {
    pub text: &'a str,
    /// First and last 1-based line the text touches; absent for an empty cut.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_line: Option<u32>,
    pub total_lines: u32,
    pub byte_start: u32,
    pub byte_end: u32,
    pub total_bytes: u32,
    /// The 1-based page the text starts on.
    pub page: u32,
    pub total_pages: u32,
    /// The cut was longer than `max_bytes` and was shortened (at a line
    /// end when one fits); read on from `byte_end`.
    pub truncated: bool,
}

/// Byte offset of the start of every line. A final newline does not start
/// another line, and an empty value has none.
fn line_starts(text: &str) -> Vec<usize> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut starts = vec![0];
    starts.extend(
        text.match_indices('\n')
            .map(|(i, _)| i + 1)
            .filter(|&i| i < text.len()),
    );
    starts
}

/// `[start, end)` byte span of every page. A final form feed ends the last
/// page rather than starting an empty one.
fn page_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = 0;
    for (i, _) in text.match_indices(PAGE_BREAK) {
        spans.push((start, i));
        start = i + 1;
    }
    if start < text.len() || spans.is_empty() {
        spans.push((start, text.len()));
    }
    spans
}

fn floor_char(text: &str, mut i: usize) -> usize {
    i = i.min(text.len());
    while !text.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn ceil_char(text: &str, mut i: usize) -> usize {
    i = i.min(text.len());
    while !text.is_char_boundary(i) {
        i += 1;
    }
    i
}

/// 1-based line holding byte `offset`.
fn line_of(starts: &[usize], offset: usize) -> u32 {
    starts.partition_point(|&s| s <= offset) as u32
}

/// Cut `range` out of `text`, at most `max_bytes` long. A range past the
/// end is an empty cut, not an error, so an agent can page until it is
/// empty.
pub(crate) fn slice(text: &str, range: Range, max_bytes: usize) -> Result<Slice<'_>, String> {
    let starts = line_starts(text);
    let pages = page_spans(text);
    let (a, b) = match range {
        Range::Whole => (0, text.len()),
        Range::Lines(LineRange { start, end }) => {
            if start == 0 {
                return Err("`lines.start` is 1-based and must be at least 1".into());
            }
            if end.is_some_and(|end| end < start) {
                return Err("`lines.end` must be at least `lines.start`".into());
            }
            let first = start as usize - 1;
            let last = end.map_or(starts.len(), |end| (end as usize).min(starts.len()));
            match starts.get(first) {
                Some(&a) => (a, starts.get(last).copied().unwrap_or(text.len())),
                None => (text.len(), text.len()),
            }
        }
        Range::Page(page) => {
            if page == 0 {
                return Err("`page` is 1-based and must be at least 1".into());
            }
            pages
                .get(page as usize - 1)
                .copied()
                .unwrap_or((text.len(), text.len()))
        }
        Range::Bytes(ByteRange { start, end }) => {
            if end.is_some_and(|end| end < start) {
                return Err("`bytes.end` must be at least `bytes.start`".into());
            }
            let a = floor_char(text, start as usize);
            let b = ceil_char(text, end.map_or(text.len(), |end| end as usize));
            (a, b.max(a))
        }
    };
    let (b, truncated) = if b - a > max_bytes {
        let limit = floor_char(text, a + max_bytes);
        let at_line = text[a..limit].rfind('\n').map(|i| a + i + 1);
        (at_line.filter(|&i| i > a).unwrap_or(limit), true)
    } else {
        (b, false)
    };
    let (start_line, end_line) = if b > a {
        (Some(line_of(&starts, a)), Some(line_of(&starts, b - 1)))
    } else {
        (None, None)
    };
    let page = pages
        .partition_point(|&(_, end)| end < a)
        .min(pages.len() - 1) as u32
        + 1;
    Ok(Slice {
        text: &text[a..b],
        start_line,
        end_line,
        total_lines: starts.len() as u32,
        byte_start: a as u32,
        byte_end: b as u32,
        total_bytes: text.len() as u32,
        page,
        total_pages: pages.len() as u32,
        truncated,
    })
}

fn invalid(message: &str) -> String {
    api_error(ErrorCode::InvalidArgument, message)
}

pub(crate) fn handle_read_with_ns(
    namespace: &str,
    tenant: &str,
    body: &[u8],
    state: &AppState,
) -> String {
    let req: ReadRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return invalid(&format!("invalid read request: {e}")),
    };
    let Some(field) = req.field.as_deref().filter(|f| !f.is_empty()) else {
        return invalid("missing `field`: the stored field to read");
    };
    let range = match (req.lines, req.page, req.bytes) {
        (None, None, None) => Range::Whole,
        (Some(lines), None, None) => Range::Lines(lines),
        (None, Some(page), None) => Range::Page(page),
        (None, None, Some(bytes)) => Range::Bytes(bytes),
        _ => return invalid("set at most one of `lines`, `page` and `bytes`"),
    };
    let max_bytes = match req.max_bytes {
        None => DEFAULT_MAX_BYTES,
        Some(0) => return invalid("`max_bytes` must be at least 1"),
        Some(n) if n as usize > MAX_MAX_BYTES => {
            return invalid(&format!("`max_bytes` must be at most {MAX_MAX_BYTES}"))
        }
        Some(n) => n as usize,
    };
    // Filters cannot match `_id`, so an id is a one-hit seek instead: sort
    // by `_id` descending from just after it, and the first hit is the id
    // itself when the document exists.
    let search = match (&req.id, &req.path) {
        (Some(id), None) => serde_json::json!({
            "query_text": "",
            "max_results": 1,
            "filter": {"match_all": {}},
            "sort": [{"_id": {"order": "desc"}}],
            "search_after": [format!("{id}\0")],
            "source_includes": [field],
        }),
        (None, Some(path)) => {
            let path_field = req.path_field.as_deref().unwrap_or("path");
            serde_json::json!({
                "query_text": "",
                "max_results": 2,
                "filter": {"term": {path_field: path}},
                "source_includes": [field],
            })
        }
        _ => return invalid("set exactly one of `id` and `path`"),
    };
    let response =
        handle_search_post_with_ns(namespace, tenant, search.to_string().as_bytes(), state);
    if !response.starts_with("HTTP/1.1 200") {
        return response;
    }
    let found = response_body_json(&response);
    let mut hits = found["results"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if let Some(id) = &req.id {
        hits = &hits[..hits
            .iter()
            .take_while(|h| h["doc_id"] == id.as_str())
            .count()];
    }
    let hit = match hits {
        [] => {
            let what = req.id.as_ref().map_or_else(
                || format!("path `{}`", req.path.as_deref().unwrap_or_default()),
                |id| format!("id `{id}`"),
            );
            return api_error(ErrorCode::NotFound, &format!("no document with {what}"));
        }
        [hit] => hit,
        _ => {
            return invalid(&format!(
                "path `{}` names more than one document; read by `id`",
                req.path.as_deref().unwrap_or_default()
            ))
        }
    };
    let doc_id = hit["doc_id"].as_str().unwrap_or_default();
    let value = hit["fields"]
        .as_array()
        .and_then(|fields| fields.iter().find(|f| f["name"] == field))
        .and_then(|f| f["value"].as_str());
    let Some(value) = value else {
        return api_error(
            ErrorCode::NotFound,
            &format!("document `{doc_id}` has no stored field `{field}`"),
        );
    };
    match slice(value, range, max_bytes) {
        Ok(cut) => {
            let mut out = serde_json::to_value(&cut).unwrap_or_default();
            out["id"] = serde_json::json!(doc_id);
            out["field"] = serde_json::json!(field);
            json_ok(&out)
        }
        Err(e) => invalid(&e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOC: &str = "one\ntwo\nthree\x0cfour\nfive\x0csix\n";

    fn lines(start: u32, end: Option<u32>) -> Range {
        Range::Lines(LineRange { start, end })
    }

    #[test]
    fn lines_pages_and_bytes_cut_with_references() {
        let s = slice(DOC, lines(2, Some(3)), 1000).unwrap();
        assert_eq!(s.text, "two\nthree\x0cfour\n");
        assert_eq!(
            (s.start_line, s.end_line, s.total_lines),
            (Some(2), Some(3), 4)
        );
        assert_eq!((s.page, s.total_pages), (1, 3));
        let s = slice(DOC, lines(4, None), 1000).unwrap();
        assert_eq!(s.text, "five\x0csix\n");
        assert_eq!(
            (s.start_line, s.page, s.byte_end, s.total_bytes),
            (Some(4), 2, 28, 28)
        );
        let s = slice(DOC, Range::Page(2), 1000).unwrap();
        assert_eq!(s.text, "four\nfive");
        assert_eq!((s.page, s.start_line, s.end_line), (2, Some(3), Some(4)));
        let s = slice(
            DOC,
            Range::Bytes(ByteRange {
                start: 4,
                end: Some(7),
            }),
            1000,
        )
        .unwrap();
        assert_eq!(
            (s.text, s.start_line, s.end_line),
            ("two", Some(2), Some(2))
        );
        assert_eq!(slice(DOC, Range::Whole, 1000).unwrap().text, DOC);
    }

    #[test]
    fn past_the_end_is_empty_and_bad_ranges_are_errors() {
        for range in [
            lines(9, None),
            Range::Page(4),
            Range::Bytes(ByteRange {
                start: 99,
                end: None,
            }),
        ] {
            let s = slice(DOC, range, 1000).unwrap();
            assert_eq!(
                (s.text, s.start_line, s.byte_start),
                ("", None, 28),
                "{range:?}"
            );
        }
        assert!(slice(DOC, lines(0, None), 10).is_err());
        assert!(slice(DOC, lines(3, Some(2)), 10).is_err());
        assert!(slice(DOC, Range::Page(0), 10).is_err());
        assert!(slice(
            DOC,
            Range::Bytes(ByteRange {
                start: 5,
                end: Some(4)
            }),
            10
        )
        .is_err());
        let empty = slice("", Range::Whole, 10).unwrap();
        assert_eq!(
            (empty.total_lines, empty.total_pages, empty.page),
            (0, 1, 1)
        );
        // A trailing form feed ends the last page instead of adding one.
        assert_eq!(
            slice("a\x0cb\x0c", Range::Whole, 10).unwrap().total_pages,
            2
        );
    }

    #[test]
    fn max_bytes_truncates_at_a_line_end_and_never_splits_a_char() {
        let s = slice(DOC, Range::Whole, 10).unwrap();
        assert_eq!((s.text, s.truncated, s.byte_end), ("one\ntwo\n", true, 8));
        let s = slice("abcdefgh", Range::Whole, 3).unwrap();
        assert_eq!((s.text, s.truncated), ("abc", true));
        let s = slice("é€x", Range::Whole, 4).unwrap();
        assert_eq!(s.text, "é");
        let s = slice(
            "é€x",
            Range::Bytes(ByteRange {
                start: 1,
                end: Some(3),
            }),
            9,
        )
        .unwrap();
        assert_eq!((s.text, s.byte_start, s.byte_end), ("é€", 0, 5));
    }
}
