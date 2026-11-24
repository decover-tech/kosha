//! Fuzzy (typo-tolerant) term matching (issue #201).
//!
//! A `fuzzy` clause names a value that is analyzed per segment, exactly as
//! `query_text` is (#220), and each of its tokens is expanded against the
//! segment's term dictionary into the terms within `fuzziness` edits
//! (Levenshtein distance; with `transpositions`, an adjacent swap counts
//! as one edit — the "optimal string alignment" distance Lucene uses).
//!
//! # Expansion
//!
//! Every inverted-index format keeps its term table sorted (bytewise, which
//! for UTF-8 is code-point order), so the dictionary already is the leaves
//! of a trie: consecutive terms share prefixes. [`expand`] walks it with a
//! Levenshtein automaton in its dynamic-programming form: one DP row per
//! character of the term, rows shared between consecutive terms for their
//! common prefix, and a whole run of terms skipped (by binary search) as
//! soon as a row's minimum exceeds the edit budget — no term with that
//! prefix can come back within it. `prefix_length` narrows the walk to the
//! terms that start with the token's first characters before any of this.
//! No sidecar is needed, so segments already on S3 expand the same way.
//!
//! # Bloom pruning
//!
//! Testing a token's expansions against the footer term bloom would mean
//! enumerating every string within an edit of it — hundreds of probes for
//! one edit, so a 1%-false-positive bloom almost never says no. Instead,
//! segments written since #201 carry a second bloom over the **deletion
//! neighbourhood** of their vocabulary (`Footer::fuzzy_bloom`, built by
//! [`build_fuzzy_bloom`]): every term plus every string one character
//! deletion away from it. Two strings are within one edit (substitution,
//! insertion, deletion or adjacent swap) only if their neighbourhoods
//! meet, so a one-edit token needs just `len + 1` probes
//! ([`deletion_neighbourhood`]); a segment whose fuzzy bloom rejects all
//! of them for some required token is skipped without being opened. An
//! exact (zero-edit) token is tested against the ordinary term bloom.
//! Two edits, segments written before #201, and segments too large for a
//! useful neighbourhood bloom cannot be pruned: they are opened and the
//! dictionary decides.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Most edits any fuzzy clause may ask for (Lucene's limit too: the
/// automaton grows quickly with the distance).
pub const MAX_FUZZY_EDITS: u8 = 2;
/// Default `max_expansions` (Elasticsearch's).
pub const DEFAULT_MAX_EXPANSIONS: usize = 50;
/// Upper bound on `max_expansions`: every expansion is one postings list
/// read and merged per segment.
pub const MAX_MAX_EXPANSIONS: usize = 1024;
/// Most distinct strings a segment's deletion neighbourhood may hold for
/// [`build_fuzzy_bloom`] to write a bloom over it: about 4,000 terms,
/// ~40 KB of bloom at the 1% target rate. Footers are kept in memory for
/// every segment of a manifest, so the neighbourhood bloom is only written
/// for the small segments it pays off on (fresh flushes, small matters);
/// a larger segment holds near neighbours of most tokens anyway.
pub const MAX_FUZZY_BLOOM_ENTRIES: usize = 32_768;

/// `fuzziness`: an explicit edit distance (`0`, `1`, `2`) or `"auto"`.
/// `"auto"` (the default) is Elasticsearch's `AUTO:3,6`: tokens shorter
/// than 3 characters must match exactly, 3–5 allow one edit, longer ones
/// two. `"auto:low,high"` moves the thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fuzziness {
    Edits(u8),
    Auto { low: u32, high: u32 },
}

impl Default for Fuzziness {
    fn default() -> Self {
        Self::Auto { low: 3, high: 6 }
    }
}

impl Fuzziness {
    /// The edit budget for a token of `chars` characters.
    pub fn max_edits(&self, chars: usize) -> u8 {
        match *self {
            Self::Edits(n) => n.min(MAX_FUZZY_EDITS),
            Self::Auto { low, high } => {
                if chars < low as usize {
                    0
                } else if chars < high as usize {
                    1
                } else {
                    2
                }
            }
        }
    }

    fn parse(text: &str) -> Result<Self, String> {
        let t = text.trim();
        if let Ok(n) = t.parse::<u64>() {
            return Self::from_number(n);
        }
        let lower = t.to_ascii_lowercase();
        if lower == "auto" {
            return Ok(Self::default());
        }
        if let Some(rest) = lower.strip_prefix("auto:") {
            let mut parts = rest.split(',').map(str::trim);
            let (Some(low), Some(high), None) = (parts.next(), parts.next(), parts.next()) else {
                return Err(format!("fuzziness {text:?}: expected \"auto:low,high\""));
            };
            let (Ok(low), Ok(high)) = (low.parse::<u32>(), high.parse::<u32>()) else {
                return Err(format!("fuzziness {text:?}: low and high must be integers"));
            };
            if low > high {
                return Err(format!("fuzziness {text:?}: low must be <= high"));
            }
            return Ok(Self::Auto { low, high });
        }
        Err(format!(
            "fuzziness must be 0, 1, 2, \"auto\" or \"auto:low,high\", got {text:?}"
        ))
    }

    fn from_number(n: u64) -> Result<Self, String> {
        if n > MAX_FUZZY_EDITS as u64 {
            return Err(format!(
                "fuzziness must be at most {MAX_FUZZY_EDITS} edits, got {n}"
            ));
        }
        Ok(Self::Edits(n as u8))
    }
}

impl Serialize for Fuzziness {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match *self {
            Self::Edits(n) => s.serialize_u8(n),
            Self::Auto { low: 3, high: 6 } => s.serialize_str("auto"),
            Self::Auto { low, high } => s.serialize_str(&format!("auto:{low},{high}")),
        }
    }
}

impl<'de> Deserialize<'de> for Fuzziness {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Number(u64),
            Text(String),
        }
        match Raw::deserialize(d)? {
            Raw::Number(n) => Fuzziness::from_number(n),
            Raw::Text(t) => Fuzziness::parse(&t),
        }
        .map_err(serde::de::Error::custom)
    }
}

/// The `fuzzy` clause of a search request.
///
/// ```json
/// "fuzzy": {"field": "content", "value": "recieve", "fuzziness": 1,
///           "prefix_length": 1, "max_expansions": 50}
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FuzzyQuery {
    /// The `Text` field to match in. `""`, `_all` or `*` means the
    /// combined index of every `Text` field. A segment written before
    /// per-field indexes (#249) answers from its combined index.
    #[serde(default)]
    pub field: String,
    /// The text to match. Analyzed per segment with the chain the field
    /// was indexed with; every token it yields is expanded. Several tokens
    /// combine with the request's `operator` (AND by default).
    pub value: String,
    #[serde(default)]
    pub fuzziness: Fuzziness,
    /// Leading characters of each token that must match exactly. Larger
    /// values make expansion cheaper.
    #[serde(default)]
    pub prefix_length: usize,
    /// Most terms one token expands to per segment, nearest first (fewest
    /// edits, then term order). The exact term, when present, is always
    /// among them.
    #[serde(default = "default_max_expansions")]
    pub max_expansions: usize,
    /// Count swapping two adjacent characters as one edit (`recieve` →
    /// `receive`). Default `true`.
    #[serde(default = "default_transpositions")]
    pub transpositions: bool,
}

fn default_max_expansions() -> usize {
    DEFAULT_MAX_EXPANSIONS
}
fn default_transpositions() -> bool {
    true
}

impl FuzzyQuery {
    pub fn validate(&self) -> Result<(), String> {
        if self.value.trim().is_empty() {
            return Err("fuzzy.value must not be empty".into());
        }
        if self.max_expansions == 0 || self.max_expansions > MAX_MAX_EXPANSIONS {
            return Err(format!(
                "fuzzy.max_expansions must be between 1 and {MAX_MAX_EXPANSIONS}, got {}",
                self.max_expansions
            ));
        }
        let field = self.field.trim();
        if field.contains('^') || (field.contains('*') && field != "*") || field.contains('\0') {
            return Err(format!(
                "fuzzy.field {:?}: boosts and field patterns are not supported",
                self.field
            ));
        }
        Ok(())
    }

    /// The field the clause is restricted to, or `None` for the combined
    /// index (`""`, `_all`, `*`).
    pub fn restricted_field(&self) -> Option<&str> {
        let field = self.field.trim();
        (!(field.is_empty() || field == "*" || field == crate::fields::ALL_FIELD)).then_some(field)
    }

    /// Expansion parameters for one analyzed token.
    pub fn params_for(&self, token: &str) -> ExpandParams {
        ExpandParams {
            max_edits: self.fuzziness.max_edits(token.chars().count()),
            prefix_length: self.prefix_length,
            transpositions: self.transpositions,
            max_expansions: self.max_expansions,
        }
    }
}

/// How one token is expanded; see [`FuzzyQuery::params_for`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpandParams {
    pub max_edits: u8,
    pub prefix_length: usize,
    pub transpositions: bool,
    pub max_expansions: usize,
}

/// A term the token expanded to: its inverted-index key (with the field
/// prefix, when the walk was over one field's keys) and its distance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expansion {
    pub key: String,
    pub edits: u8,
}

/// Work done by one [`expand`], for benchmarks and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExpandStats {
    /// Terms in the walked range (after `prefix_length` narrowing).
    pub range_terms: usize,
    /// Terms whose DP rows were (at least partly) computed.
    pub terms_visited: usize,
    /// DP rows computed.
    pub rows: usize,
    /// Matches before the `max_expansions` cut.
    pub matched: usize,
}

/// A sorted (bytewise) term dictionary that can be read by index.
pub trait SortedTerms {
    fn term_count(&self) -> usize;
    fn term(&self, i: usize) -> &str;
}

impl SortedTerms for [&str] {
    fn term_count(&self) -> usize {
        self.len()
    }
    fn term(&self, i: usize) -> &str {
        self[i]
    }
}

impl SortedTerms for Vec<&str> {
    fn term_count(&self) -> usize {
        self.len()
    }
    fn term(&self, i: usize) -> &str {
        self[i]
    }
}

/// First index in `lo..hi` at which `pred` turns false (`pred` must be
/// true on a prefix of the range and false after it).
fn partition_point<T: SortedTerms + ?Sized>(
    dict: &T,
    mut lo: usize,
    mut hi: usize,
    pred: impl Fn(&str) -> bool,
) -> usize {
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(dict.term(mid)) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Every term of `dict` under `key_prefix` within `params.max_edits` of
/// `token`, nearest first, at most `params.max_expansions` of them.
///
/// `key_prefix` is `""` for the combined index (per-field keys, which all
/// start with NUL, are left out) or a field's key prefix
/// ([`crate::fields::field_term_key`]`(field, "")`) to walk that field's
/// terms; returned keys include it.
///
/// A non-exact match must leave at least one character of the shorter of
/// the two strings untouched (Lucene's rule): `"ox"` is not one edit from
/// `"o"`, whatever the budget, or every short token would match every
/// other short term.
pub fn expand<T: SortedTerms + ?Sized>(
    dict: &T,
    key_prefix: &str,
    token: &str,
    params: &ExpandParams,
    stats: &mut ExpandStats,
) -> Vec<Expansion> {
    let q: Vec<char> = token.chars().collect();
    if q.is_empty() {
        return Vec::new();
    }
    let k = params.max_edits.min(MAX_FUZZY_EDITS);
    let plen = params.prefix_length.min(q.len());
    let mut search_prefix = String::with_capacity(key_prefix.len() + token.len());
    search_prefix.push_str(key_prefix);
    search_prefix.extend(&q[..plen]);
    let qs = &q[plen..];
    let m = qs.len();

    let n = dict.term_count();
    let mut lo = partition_point(dict, 0, n, |t| t < search_prefix.as_str());
    if key_prefix.is_empty() {
        // Per-field keys sort first (they start with NUL); never combined terms.
        lo = lo.max(partition_point(dict, 0, n, |t| t.starts_with('\0')));
    }
    let hi = partition_point(dict, lo, n, |t| t.starts_with(search_prefix.as_str()));
    stats.range_terms += hi - lo;

    let cap = k.saturating_add(1);
    // rows[d] = DP row after `d` characters of the current path.
    let mut rows: Vec<Vec<u8>> = vec![(0..=m).map(|j| (j as u8).min(cap)).collect()];
    let mut path: Vec<char> = Vec::new();
    let mut out: Vec<Expansion> = Vec::new();

    let mut i = lo;
    while i < hi {
        let term = dict.term(i);
        let suffix = &term[search_prefix.len()..];
        stats.terms_visited += 1;
        // Reuse the rows of the prefix this term shares with the last path.
        let mut common = 0usize;
        let mut chars = suffix.chars();
        let mut pending: Option<char> = None;
        for &p in &path {
            match chars.next() {
                Some(c) if c == p => common += 1,
                other => {
                    pending = other;
                    break;
                }
            }
        }
        path.truncate(common);
        rows.truncate(common + 1);
        let mut dead = false;
        let rest = pending.into_iter().chain(chars);
        for c in rest {
            let depth = path.len(); // rows[depth] is the current last row
            let prev = &rows[depth];
            let mut row = vec![0u8; m + 1];
            row[0] = ((depth + 1) as u8).min(cap);
            let mut row_min = row[0];
            for j in 1..=m {
                let cost = u8::from(qs[j - 1] != c);
                let mut v = (prev[j] + 1).min(row[j - 1] + 1).min(prev[j - 1] + cost);
                if params.transpositions
                    && depth >= 1
                    && j >= 2
                    && qs[j - 1] == path[depth - 1]
                    && qs[j - 2] == c
                {
                    v = v.min(rows[depth - 1][j - 2] + 1);
                }
                row[j] = v.min(cap);
                row_min = row_min.min(row[j]);
            }
            stats.rows += 1;
            path.push(c);
            rows.push(row);
            if row_min > k {
                dead = true;
                break;
            }
        }
        if dead {
            // No term continuing this path can come back within budget:
            // skip every term that starts with it.
            let mut dead_prefix = search_prefix.clone();
            dead_prefix.extend(path.iter());
            i = partition_point(dict, i + 1, hi, |t| t.starts_with(dead_prefix.as_str()));
            continue;
        }
        let d = rows[path.len()][m];
        if d <= k {
            let term_chars = plen + path.len();
            if d == 0 || (d as usize) < q.len().min(term_chars) {
                out.push(Expansion {
                    key: term.to_string(),
                    edits: d,
                });
            }
        }
        i += 1;
    }
    stats.matched += out.len();
    out.sort_by(|a, b| a.edits.cmp(&b.edits).then_with(|| a.key.cmp(&b.key)));
    out.truncate(params.max_expansions.max(1));
    out
}

/// Optimal-string-alignment distance between `a` and `b` (reference
/// implementation for tests and callers outside the dictionary walk).
pub fn edit_distance(a: &str, b: &str, transpositions: bool) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut v = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);
            if transpositions && i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                v = v.min(d[i - 2][j - 2] + 1);
            }
            d[i][j] = v;
        }
    }
    d[a.len()][b.len()]
}

/// `token` and every string one character deletion away from it, deleting
/// only after the first `prefix_length` characters. The probes a one-edit
/// fuzzy token tests against a segment's fuzzy bloom.
pub fn deletion_neighbourhood(token: &str, prefix_length: usize) -> Vec<String> {
    let chars: Vec<(usize, char)> = token.char_indices().collect();
    let mut out = vec![token.to_string()];
    for &(at, c) in chars.iter().skip(prefix_length) {
        let mut variant = String::with_capacity(token.len());
        variant.push_str(&token[..at]);
        variant.push_str(&token[at + c.len_utf8()..]);
        if !variant.is_empty() && !out.contains(&variant) {
            out.push(variant);
        }
    }
    out
}

/// A bloom over the deletion neighbourhoods of `terms` (combined-index
/// terms, no per-field keys), or `None` when that holds more than
/// [`MAX_FUZZY_BLOOM_ENTRIES`] distinct strings.
///
/// Sound for one edit with any `prefix_length`: if `t` is within one edit
/// of `q`, some string is in both neighbourhoods — `q` itself (`t` has an
/// extra character), `t` (`q` has one), or the string with the differing
/// character removed from both (a substitution) or with one character of
/// a swapped pair removed (a transposition). The deletion on `q`'s side is
/// always at or after the edit, so it is past an exact prefix.
pub fn build_fuzzy_bloom<'a>(
    terms: impl IntoIterator<Item = &'a str>,
) -> Option<crate::BloomFilter> {
    let mut entries: std::collections::HashSet<String> = std::collections::HashSet::new();
    for term in terms {
        for variant in deletion_neighbourhood(term, 0) {
            entries.insert(variant);
        }
        if entries.len() > MAX_FUZZY_BLOOM_ENTRIES {
            return None;
        }
    }
    Some(crate::BloomFilter::build(
        entries.iter().map(String::as_str),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(max_edits: u8) -> ExpandParams {
        ExpandParams {
            max_edits,
            prefix_length: 0,
            transpositions: true,
            max_expansions: 50,
        }
    }

    fn keys(out: &[Expansion]) -> Vec<(&str, u8)> {
        out.iter().map(|e| (e.key.as_str(), e.edits)).collect()
    }

    fn sorted(mut terms: Vec<&str>) -> Vec<&str> {
        terms.sort_unstable();
        terms.dedup();
        terms
    }

    #[test]
    fn fuzziness_parses_numbers_auto_and_rejects_the_rest() {
        let parse = |json: &str| serde_json::from_str::<Fuzziness>(json);
        assert_eq!(parse("1").unwrap(), Fuzziness::Edits(1));
        assert_eq!(parse("\"2\"").unwrap(), Fuzziness::Edits(2));
        assert_eq!(parse("\"AUTO\"").unwrap(), Fuzziness::default());
        assert_eq!(
            parse("\"auto:4,8\"").unwrap(),
            Fuzziness::Auto { low: 4, high: 8 }
        );
        assert!(parse("3").is_err());
        assert!(parse("\"fuzzy\"").is_err());
        assert!(parse("\"auto:8,4\"").is_err());
        assert!(parse("-1").is_err());
        assert_eq!(
            serde_json::to_string(&Fuzziness::default()).unwrap(),
            "\"auto\""
        );
        let auto = Fuzziness::default();
        assert_eq!(
            (
                auto.max_edits(2),
                auto.max_edits(3),
                auto.max_edits(5),
                auto.max_edits(6)
            ),
            (0, 1, 1, 2)
        );
    }

    #[test]
    fn fuzzy_query_defaults_and_validation() {
        let q: FuzzyQuery = serde_json::from_str(r#"{"value": "recieve"}"#).unwrap();
        assert_eq!(q.fuzziness, Fuzziness::default());
        assert_eq!(q.max_expansions, DEFAULT_MAX_EXPANSIONS);
        assert!(q.transpositions);
        assert_eq!(q.prefix_length, 0);
        assert!(q.validate().is_ok());
        assert_eq!(q.restricted_field(), None);
        assert!(serde_json::from_str::<FuzzyQuery>(r#"{"value": "x", "fuzzyness": 1}"#).is_err());
        let bad = FuzzyQuery {
            max_expansions: 0,
            ..q.clone()
        };
        assert!(bad.validate().is_err());
        let empty = FuzzyQuery {
            value: "  ".into(),
            ..q.clone()
        };
        assert!(empty.validate().is_err());
        let boosted = FuzzyQuery {
            field: "title^2".into(),
            ..q
        };
        assert!(boosted.validate().is_err());
    }

    #[test]
    fn transposition_is_one_edit() {
        assert_eq!(edit_distance("recieve", "receive", true), 1);
        assert_eq!(edit_distance("recieve", "receive", false), 2);
        assert_eq!(edit_distance("", "abc", true), 3);
    }

    #[test]
    fn one_edit_finds_receive_for_recieve_and_not_unrelated_terms() {
        let dict = sorted(vec![
            "re", "eve", "rice", "sieve", "receipt", "receive", "receiver", "recipe", "deceive",
            "zebra",
        ]);
        let mut stats = ExpandStats::default();
        let out = expand(&dict, "", "recieve", &params(1), &mut stats);
        assert_eq!(keys(&out), vec![("receive", 1)]);
        // Without transpositions it is two edits away.
        let mut p = params(1);
        p.transpositions = false;
        assert!(expand(&dict, "", "recieve", &p, &mut stats).is_empty());
        let two = expand(&dict, "", "recieve", &params(2), &mut stats);
        assert_eq!(
            keys(&two),
            vec![
                ("receive", 1),
                ("deceive", 2),
                ("receiver", 2),
                ("recipe", 2)
            ]
        );
    }

    #[test]
    fn exact_term_comes_first_and_survives_max_expansions() {
        let dict = sorted(vec![
            "cat", "bat", "car", "cart", "cast", "at", "cats", "hat",
        ]);
        let mut p = params(1);
        p.max_expansions = 2;
        let out = expand(&dict, "", "cat", &p, &mut ExpandStats::default());
        assert_eq!(keys(&out), vec![("cat", 0), ("at", 1)]);
    }

    #[test]
    fn short_tokens_never_match_by_replacing_everything() {
        let dict = sorted(vec!["o", "ox", "of", "x", "a"]);
        let out = expand(&dict, "", "ox", &params(2), &mut ExpandStats::default());
        // "o"/"x" are one edit away but would leave nothing of "o"/"x"
        // matched; "of" keeps the "o".
        assert_eq!(keys(&out), vec![("ox", 0), ("of", 1)]);
    }

    #[test]
    fn prefix_length_restricts_the_walk() {
        let dict = sorted(vec!["receive", "deceive", "recieve", "relieve"]);
        let mut p = params(1);
        p.prefix_length = 3;
        let mut stats = ExpandStats::default();
        let out = expand(&dict, "", "recieve", &p, &mut stats);
        assert_eq!(keys(&out), vec![("recieve", 0), ("receive", 1)]);
        assert_eq!(stats.range_terms, 2, "only rec* is walked");
        p.prefix_length = 100;
        let exact = expand(&dict, "", "recieve", &p, &mut ExpandStats::default());
        assert_eq!(keys(&exact), vec![("recieve", 0)]);
    }

    #[test]
    fn field_keys_are_walked_only_under_their_prefix() {
        let title = crate::fields::field_term_key("title", "");
        let body_receive = crate::fields::field_term_key("body", "receive");
        let title_receive = crate::fields::field_term_key("title", "receive");
        let title_norms = crate::fields::field_norms_key("title");
        let dict = sorted(vec![
            body_receive.as_str(),
            title_receive.as_str(),
            title_norms.as_str(),
            "receive",
            "recipe",
        ]);
        let combined = expand(
            &dict,
            "",
            "recieve",
            &params(1),
            &mut ExpandStats::default(),
        );
        assert_eq!(keys(&combined), vec![("receive", 1)]);
        let field = expand(
            &dict,
            &title,
            "recieve",
            &params(1),
            &mut ExpandStats::default(),
        );
        assert_eq!(keys(&field), vec![(title_receive.as_str(), 1)]);
    }

    #[test]
    fn walk_matches_brute_force_on_a_generated_dictionary() {
        // Deterministic pseudo-random vocabulary over a small alphabet, so
        // prefixes are heavily shared and the dead-prefix skip is exercised.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let letters = ['a', 'b', 'c', 'd', 'é'];
        let owned: Vec<String> = (0..3000)
            .map(|_| {
                let len = 1 + (next() % 7) as usize;
                (0..len)
                    .map(|_| letters[(next() % letters.len() as u64) as usize])
                    .collect()
            })
            .collect();
        let dict = sorted(owned.iter().map(String::as_str).collect());
        for token in ["abc", "dacb", "ééa", "a", "bbbbbb", "cadé"] {
            for edits in 0..=2u8 {
                for prefix_length in [0usize, 1, 2] {
                    for transpositions in [true, false] {
                        let p = ExpandParams {
                            max_edits: edits,
                            prefix_length,
                            transpositions,
                            max_expansions: usize::MAX,
                        };
                        let got = expand(&dict, "", token, &p, &mut ExpandStats::default());
                        let qc: Vec<char> = token.chars().collect();
                        let pl = prefix_length.min(qc.len());
                        let prefix: String = qc[..pl].iter().collect();
                        let mut want: Vec<(&str, u8)> = dict
                            .iter()
                            .filter(|t| t.starts_with(prefix.as_str()))
                            .filter_map(|t| {
                                let d = edit_distance(
                                    &t[prefix.len()..],
                                    &token[prefix.len()..],
                                    transpositions,
                                );
                                let shorter = qc.len().min(t.chars().count());
                                (d <= edits as usize && (d == 0 || d < shorter))
                                    .then_some((*t, d as u8))
                            })
                            .collect();
                        want.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(b.0)));
                        assert_eq!(
                            keys(&got),
                            want,
                            "token {token} edits {edits} prefix {prefix_length} transpositions {transpositions}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn deletion_neighbourhoods_meet_for_every_one_edit_match() {
        assert_eq!(deletion_neighbourhood("abc", 0), ["abc", "bc", "ac", "ab"]);
        assert_eq!(deletion_neighbourhood("aab", 1), ["aab", "ab", "aa"]);
        assert_eq!(deletion_neighbourhood("é", 0), ["é"]);
        // Every one-edit expansion, with and without a prefix, shares a
        // neighbourhood string with the token — checked exhaustively on
        // a generated dictionary.
        let letters = ['a', 'b', 'c', 'é'];
        let mut dict: Vec<String> = Vec::new();
        for len in 1..=4usize {
            let mut idx = vec![0usize; len];
            loop {
                dict.push(idx.iter().map(|&i| letters[i]).collect());
                let mut k = 0;
                while k < len {
                    idx[k] += 1;
                    if idx[k] < letters.len() {
                        break;
                    }
                    idx[k] = 0;
                    k += 1;
                }
                if k == len {
                    break;
                }
            }
        }
        let refs = sorted(dict.iter().map(String::as_str).collect());
        let bloomed: std::collections::HashSet<String> = refs
            .iter()
            .flat_map(|t| deletion_neighbourhood(t, 0))
            .collect();
        for token in ["abc", "cab", "éa", "bbbb", "a"] {
            for prefix_length in [0usize, 1, 2] {
                let mut p = params(1);
                p.prefix_length = prefix_length;
                p.max_expansions = usize::MAX;
                let expansions = expand(&refs, "", token, &p, &mut ExpandStats::default());
                assert!(!expansions.is_empty());
                let probes = deletion_neighbourhood(token, prefix_length);
                for e in &expansions {
                    let own: Vec<String> = deletion_neighbourhood(&e.key, 0);
                    assert!(
                        probes.iter().any(|p| own.contains(p)),
                        "{token} -> {} (prefix {prefix_length}) not covered",
                        e.key
                    );
                }
                assert!(probes.iter().any(|p| bloomed.contains(p)));
            }
        }
    }

    #[test]
    fn fuzzy_bloom_prunes_and_is_skipped_when_too_large() {
        let bloom = build_fuzzy_bloom(["receive", "parcel"]).unwrap();
        let admits = |token: &str| {
            deletion_neighbourhood(token, 0)
                .iter()
                .any(|p| bloom.may_contain(p))
        };
        assert!(admits("recieve"));
        assert!(admits("parcle"));
        assert!(admits("receive"));
        assert!(!admits("banana"));
        let many: Vec<String> = (0..20_000).map(|i| format!("term{i:06}")).collect();
        assert!(build_fuzzy_bloom(many.iter().map(String::as_str)).is_none());
    }
}
