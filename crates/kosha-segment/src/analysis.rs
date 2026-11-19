//! Analyzer implementations (issues #220, #221). The serializable
//! descriptions they are built from live in [`kosha_core::analysis`].
//!
//! One function, [`Analyzer::analyze_with_positions`], produces both the
//! terms and their positions, so the indexing path and the query path (and
//! phrase matching, which depends on positions) can never disagree about
//! what a token is.

use std::borrow::Cow;
use std::collections::HashSet;

use kosha_core::{AnalyzerSpec, TokenFilterSpec, TokenizerKind};
use unicode_segmentation::UnicodeSegmentation;

/// A built analysis chain.
pub struct Analyzer {
    spec: AnalyzerSpec,
    filters: Vec<Filter>,
}

enum Filter {
    Lowercase,
    Stop(HashSet<String>),
    Stem(rust_stemmers::Stemmer),
}

impl std::fmt::Debug for Analyzer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Analyzer")
            .field("spec", &self.spec)
            .finish()
    }
}

impl Analyzer {
    pub fn new(spec: &AnalyzerSpec) -> Self {
        let filters = spec
            .filters
            .iter()
            .map(|filter| match filter {
                TokenFilterSpec::Lowercase => Filter::Lowercase,
                TokenFilterSpec::Stop { words } => Filter::Stop(words.iter().cloned().collect()),
                // `AnalyzerSpec::resolve` only produces `english`; a footer
                // naming anything else came from a newer binary, and
                // English is still the best available reading of it.
                TokenFilterSpec::Stem { .. } => Filter::Stem(rust_stemmers::Stemmer::create(
                    rust_stemmers::Algorithm::English,
                )),
            })
            .collect();
        Self {
            spec: spec.clone(),
            filters,
        }
    }

    pub fn spec(&self) -> &AnalyzerSpec {
        &self.spec
    }

    /// Tokens with their positions. Positions count tokenizer output, so
    /// a token a `stop` filter drops leaves a gap — a phrase query
    /// analyzed by the same chain carries the same gap and still matches.
    pub fn analyze_with_positions(&self, text: &str) -> Vec<(String, u32)> {
        if self.spec.is_legacy() {
            // Byte-for-byte the pre-#220 tokenizer.
            return legacy_tokenize_with_positions(text);
        }
        let mut out = Vec::new();
        let mut emit = |token: &str, position: u32| {
            if let Some(token) = self.filter(token) {
                out.push((token, position));
            }
        };
        match self.spec.tokenizer {
            TokenizerKind::Standard if self.spec.version >= 2 => {
                for (position, word) in unicode_words(text).enumerate() {
                    emit(word, position as u32);
                }
            }
            TokenizerKind::Standard => {
                for (word, position) in legacy_words(text) {
                    emit(word, position);
                }
            }
            TokenizerKind::Whitespace => {
                for (position, word) in text.split_whitespace().enumerate() {
                    emit(word, position as u32);
                }
            }
            TokenizerKind::Keyword => {
                if !text.is_empty() {
                    emit(text, 0);
                }
            }
        }
        out
    }

    pub fn analyze(&self, text: &str) -> Vec<String> {
        self.analyze_with_positions(text)
            .into_iter()
            .map(|(token, _)| token)
            .collect()
    }

    fn filter(&self, token: &str) -> Option<String> {
        // Borrowed until a filter actually changes the token: most tokens
        // are already lowercase and are not stemmed to something else.
        let mut token = Cow::Borrowed(token);
        for filter in &self.filters {
            match filter {
                Filter::Lowercase => {
                    let unchanged = if token.is_ascii() {
                        !token.bytes().any(|b| b.is_ascii_uppercase())
                    } else {
                        false
                    };
                    if !unchanged {
                        token = Cow::Owned(token.to_lowercase());
                    }
                }
                Filter::Stop(words) => {
                    if words.contains(token.as_ref()) {
                        return None;
                    }
                }
                Filter::Stem(stemmer) => {
                    if let Cow::Owned(stemmed) = stemmer.stem(&token) {
                        token = Cow::Owned(stemmed);
                    }
                }
            }
            if token.is_empty() {
                return None;
            }
        }
        Some(token.into_owned())
    }
}

/// Legacy (`standard@1`) words: whitespace split, ASCII punctuation
/// trimmed from the edges; a word that trims to nothing takes no position.
fn legacy_words(text: &str) -> impl Iterator<Item = (&str, u32)> {
    text.split_whitespace()
        .scan(0u32, |pos, word| {
            let w = word.trim_matches(|c: char| c.is_ascii_punctuation());
            if w.is_empty() {
                Some(None)
            } else {
                let p = *pos;
                *pos += 1;
                Some(Some((w, p)))
            }
        })
        .flatten()
}

pub(crate) fn legacy_tokenize_with_positions(text: &str) -> Vec<(String, u32)> {
    legacy_words(text)
        .map(|(w, p)| (w.to_lowercase(), p))
        .collect()
}

/// `standard@2` words: UAX #29 word boundaries (`unicode_words`, which
/// drops punctuation and whitespace segments), then each word split once
/// more at a `.` or `:` between two non-digits. UAX #29 deliberately
/// keeps `naïve.café` and `foo:bar` as one word (those are its MidNumLet
/// and MidLetter classes, meant for abbreviations and Swedish); text
/// scraped from HTML, CSV and mail bodies is far more often missing a
/// space there (#221). Decimal numbers (`3.14`, `v1.2`) stay whole.
///
/// Whitespace is always a UAX #29 word boundary, so the text is cut there
/// first and each all-ASCII chunk — nearly all of them in most corpora —
/// is segmented by [`ascii_words`], a direct implementation of the UAX #29
/// rules restricted to ASCII; only chunks with non-ASCII characters pay
/// for the full Unicode tables. `ascii_matches_unicode_segmentation`
/// checks the two agree.
fn unicode_words(text: &str) -> impl Iterator<Item = &str> {
    text.split_ascii_whitespace()
        .flat_map(|chunk| {
            if chunk.is_ascii() {
                ChunkWords::Ascii(ascii_words(chunk))
            } else {
                ChunkWords::Unicode(chunk.unicode_words())
            }
        })
        .flat_map(split_interior_separators)
}

/// One whitespace-free chunk's words, without boxing per chunk.
enum ChunkWords<A, U> {
    Ascii(A),
    Unicode(U),
}

impl<'a, A, U> Iterator for ChunkWords<A, U>
where
    A: Iterator<Item = &'a str>,
    U: Iterator<Item = &'a str>,
{
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        match self {
            ChunkWords::Ascii(words) => words.next(),
            ChunkWords::Unicode(words) => words.next(),
        }
    }
}

/// The reference implementation [`unicode_words`] must agree with.
#[cfg(test)]
fn unicode_words_reference(text: &str) -> impl Iterator<Item = &str> {
    text.unicode_words().flat_map(split_interior_separators)
}

/// UAX #29 `unicode_words` for ASCII text without whitespace. Letters,
/// digits and `_` always join (WB5, WB8–WB10, WB13a/b). Between two
/// letters, `'` `.` `:` join (WB6/WB7: MidLetter, MidNumLet, Single_Quote);
/// between two digits, `'` `.` `,` `;` join (WB11/WB12: MidNum, MidNumLet,
/// Single_Quote). Everything else is a boundary, and a segment is a word
/// only if it holds a letter or digit.
fn ascii_words(chunk: &str) -> impl Iterator<Item = &str> {
    let bytes = chunk.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    std::iter::from_fn(move || {
        while i < bytes.len() {
            if !is_word(bytes[i]) {
                i += 1;
                continue;
            }
            let start = i;
            i += 1;
            loop {
                if i < bytes.len() && is_word(bytes[i]) {
                    i += 1;
                    continue;
                }
                if i + 1 < bytes.len() {
                    let (prev, mid, next) = (bytes[i - 1], bytes[i], bytes[i + 1]);
                    let letters = prev.is_ascii_alphabetic() && next.is_ascii_alphabetic();
                    let digits = prev.is_ascii_digit() && next.is_ascii_digit();
                    if (letters && matches!(mid, b'\'' | b'.' | b':'))
                        || (digits && matches!(mid, b'\'' | b'.' | b',' | b';'))
                    {
                        i += 2;
                        continue;
                    }
                }
                break;
            }
            let word = &chunk[start..i];
            if word.bytes().any(|b| b.is_ascii_alphanumeric()) {
                return Some(word);
            }
        }
        None
    })
}

fn is_interior_separator(c: char) -> bool {
    matches!(c, '.' | ':' | '\u{FF0E}' | '\u{FE13}' | '\u{2024}')
}

fn split_interior_separators(word: &str) -> InteriorSplit<'_> {
    InteriorSplit {
        rest: word,
        // Nearly every word has no separator: skip the char walk.
        plain: !word.contains(is_interior_separator),
    }
}

/// Iterator over the pieces of one UAX #29 word; see [`unicode_words`].
struct InteriorSplit<'a> {
    rest: &'a str,
    plain: bool,
}

impl<'a> Iterator for InteriorSplit<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.plain {
            self.plain = false;
            let word = std::mem::take(&mut self.rest);
            return (!word.is_empty()).then_some(word);
        }
        while !self.rest.is_empty() {
            let word = self.rest;
            let mut prev: Option<char> = None;
            let mut cut: Option<(usize, usize)> = None;
            let mut chars = word.char_indices().peekable();
            while let Some((i, c)) = chars.next() {
                if is_interior_separator(c) {
                    let next = chars.peek().map(|&(_, n)| n);
                    let numeric =
                        prev.is_some_and(char::is_numeric) && next.is_some_and(char::is_numeric);
                    if !numeric {
                        cut = Some((i, i + c.len_utf8()));
                        break;
                    }
                }
                prev = Some(c);
            }
            match cut {
                Some((end, resume)) => {
                    self.rest = &word[resume..];
                    if end > 0 {
                        return Some(&word[..end]);
                    }
                }
                None => {
                    self.rest = "";
                    return Some(word);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(names: &[&str]) -> AnalyzerSpec {
        let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        AnalyzerSpec::resolve(&names, None).unwrap()
    }

    fn tokens(names: &[&str], text: &str) -> Vec<String> {
        Analyzer::new(&spec(names)).analyze(text)
    }

    /// The #221 table.
    #[test]
    fn unicode_standard_tokenizer_table() {
        let cases: &[(&str, &[&str])] = &[
            ("hello,world", &["hello", "world"]),
            ("foo(bar)", &["foo", "bar"]),
            ("state-of-the-art", &["state", "of", "the", "art"]),
            ("naïve.café", &["naïve", "café"]),
            ("日本語テキスト", &["日", "本", "語", "テキスト"]),
            // Unicode punctuation the legacy predicate ignored.
            ("«quoted»—dash。end、x", &["quoted", "dash", "end", "x"]),
            ("pi is 3.14, v1.2", &["pi", "is", "3.14", "v1.2"]),
            ("don't stop", &["don't", "stop"]),
            ("foo:bar", &["foo", "bar"]),
        ];
        for (input, expected) in cases {
            assert_eq!(tokens(&["standard@2"], input), *expected, "input {input:?}");
        }
        // And what the legacy tokenizer does with the same rows.
        assert_eq!(tokens(&["standard"], "hello,world"), ["hello,world"]);
        assert_eq!(tokens(&["standard"], "foo(bar)"), ["foo(bar"]);
        assert_eq!(tokens(&["standard"], "naïve.café"), ["naïve.café"]);
    }

    #[test]
    fn ascii_matches_unicode_segmentation() {
        // Deterministic xorshift fuzz over the characters UAX #29 treats
        // specially in ASCII, plus a few non-ASCII ones.
        let alphabet: Vec<char> = "aZ09_ '.:,;-()!?/\"\t\nxé日\u{301}—".chars().collect();
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..50_000 {
            let len = (next() % 16) as usize;
            let text: String = (0..len)
                .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                .collect();
            assert_eq!(
                unicode_words(&text).collect::<Vec<_>>(),
                unicode_words_reference(&text).collect::<Vec<_>>(),
                "{text:?}"
            );
        }
        for text in [
            "don't 'tis rock'n'roll 1,000.5 1;2 10:30 a.1 a'1 __init__ _ v1.2.3 e.g. U.S.A.",
            "foo_bar-baz x__y 3.14.15 1'000 a:b:c ab..cd 1..2",
        ] {
            assert_eq!(
                unicode_words(text).collect::<Vec<_>>(),
                unicode_words_reference(text).collect::<Vec<_>>(),
                "{text:?}"
            );
        }
    }

    #[test]
    fn unicode_lowercasing_is_sane() {
        // Turkish dotted capital I lowercases to `i` + combining dot
        // (locale-independent Unicode lowercase), German ß is kept.
        assert_eq!(tokens(&["standard@2"], "İstanbul"), ["i\u{307}stanbul"]);
        assert_eq!(
            tokens(&["standard@2"], "Straße STRASSE"),
            ["straße", "strasse"]
        );
        assert_eq!(tokens(&["standard@2"], "ÉCOLE"), ["école"]);
    }

    #[test]
    fn positions_stay_in_lockstep_with_tokens() {
        for names in [
            &["standard"][..],
            &["standard@2"],
            &["standard@2", "stop", "stem"],
            &["whitespace"],
            &["keyword"],
        ] {
            let analyzer = Analyzer::new(&spec(names));
            for text in [
                "The quick, brown fox",
                "hello,world foo(bar) 日本語",
                "  ",
                "",
            ] {
                let with_positions = analyzer.analyze_with_positions(text);
                let plain = analyzer.analyze(text);
                assert_eq!(
                    with_positions
                        .iter()
                        .map(|(t, _)| t.clone())
                        .collect::<Vec<_>>(),
                    plain
                );
                assert!(with_positions.windows(2).all(|w| w[0].1 < w[1].1));
            }
        }
    }

    #[test]
    fn stop_filter_leaves_position_gaps() {
        let analyzer = Analyzer::new(&spec(&["standard", "stop"]));
        assert_eq!(
            analyzer.analyze_with_positions("State of the Art"),
            vec![("state".to_string(), 0), ("art".to_string(), 3)]
        );
    }

    #[test]
    fn stemming_and_keyword_and_whitespace() {
        assert_eq!(
            tokens(&["standard", "stem"], "Running runs"),
            ["run", "run"]
        );
        assert_eq!(tokens(&["keyword"], "ABC-123 Rev B"), ["ABC-123 Rev B"]);
        assert_eq!(tokens(&["lowercase"], "ABC-123 Rev B"), ["abc-123 rev b"]);
        assert_eq!(tokens(&["whitespace"], "ABC-123, Rev"), ["ABC-123,", "Rev"]);
        assert!(tokens(&["keyword"], "").is_empty());
    }

    #[test]
    fn legacy_chain_matches_the_old_free_functions() {
        let analyzer = Analyzer::new(&AnalyzerSpec::legacy());
        for text in ["Hello, World!", "... a -- b", "naïve.café x(y)", ""] {
            assert_eq!(
                analyzer.analyze_with_positions(text),
                crate::tokenize_with_positions(text)
            );
        }
    }
}
