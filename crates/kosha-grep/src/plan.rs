//! Regex to trigram query planning.
//!
//! [`plan`] walks the regex's HIR and derives a [`TrigramQuery`] that every
//! line matching the regex must satisfy. Each node yields an [`Info`]: the
//! exact set of strings it can match when that set is small, and otherwise
//! a query that is a necessary condition for it. Concatenation takes the
//! cross product of exact sets (so trigrams spanning node boundaries are
//! kept), alternation unions them, and anything unbounded (`.`, `\w`, `*`)
//! ends the exact run and contributes only what was already required.
//!
//! Literals are folded to ASCII lowercase, as the index is, so the plan is a
//! superset for case-sensitive and case-insensitive patterns alike.

use std::collections::BTreeSet;

use regex_syntax::hir::{Class, Hir, HirKind};

/// Largest exact set kept before it is collapsed into a query.
const MAX_EXACT: usize = 64;
/// Largest character class expanded into an exact set.
const MAX_CLASS: usize = 16;

/// A necessary condition on a document's trigram set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrigramQuery {
    /// No constraint: every document is a candidate.
    All,
    Trigram([u8; 3]),
    And(Vec<TrigramQuery>),
    Or(Vec<TrigramQuery>),
}

impl TrigramQuery {
    fn and(self, other: TrigramQuery) -> TrigramQuery {
        match (self, other) {
            (TrigramQuery::All, q) | (q, TrigramQuery::All) => q,
            (TrigramQuery::And(mut a), TrigramQuery::And(b)) => {
                a.extend(b);
                TrigramQuery::And(a)
            }
            (TrigramQuery::And(mut a), q) | (q, TrigramQuery::And(mut a)) => {
                a.push(q);
                TrigramQuery::And(a)
            }
            (a, b) => TrigramQuery::And(vec![a, b]),
        }
    }

    fn or(self, other: TrigramQuery) -> TrigramQuery {
        match (self, other) {
            (TrigramQuery::All, _) | (_, TrigramQuery::All) => TrigramQuery::All,
            (TrigramQuery::Or(mut a), TrigramQuery::Or(b)) => {
                a.extend(b);
                TrigramQuery::Or(a)
            }
            (TrigramQuery::Or(mut a), q) | (q, TrigramQuery::Or(mut a)) => {
                a.push(q);
                TrigramQuery::Or(a)
            }
            (a, b) => TrigramQuery::Or(vec![a, b]),
        }
    }

    /// Whether the query constrains anything (is not a full scan).
    pub fn is_selective(&self) -> bool {
        !matches!(self, TrigramQuery::All)
    }

    /// Number of trigram leaves, for reporting.
    pub fn trigram_count(&self) -> usize {
        match self {
            TrigramQuery::All => 0,
            TrigramQuery::Trigram(_) => 1,
            TrigramQuery::And(qs) | TrigramQuery::Or(qs) => {
                qs.iter().map(Self::trigram_count).sum()
            }
        }
    }
}

type Exact = BTreeSet<Vec<u8>>;

struct Info {
    /// Every string the node can match, when known and small.
    exact: Option<Exact>,
    /// A necessary condition for the node beyond `exact`.
    query: TrigramQuery,
}

impl Info {
    fn exact(set: Exact) -> Info {
        Info {
            exact: Some(set),
            query: TrigramQuery::All,
        }
    }

    fn any() -> Info {
        Info {
            exact: None,
            query: TrigramQuery::All,
        }
    }

    fn empty_string() -> Info {
        Info::exact(BTreeSet::from([Vec::new()]))
    }

    fn into_query(self) -> TrigramQuery {
        match self.exact {
            Some(set) => self.query.and(exact_query(&set)),
            None => self.query,
        }
    }
}

/// The trigram query for `pattern` under `case_insensitive`. `Err` only for
/// a pattern the regex parser rejects.
pub fn plan(
    pattern: &str,
    case_insensitive: bool,
) -> Result<TrigramQuery, Box<regex_syntax::Error>> {
    Ok(plan_hir(&parse(pattern, case_insensitive)?))
}

pub(crate) fn parse(
    pattern: &str,
    case_insensitive: bool,
) -> Result<Hir, Box<regex_syntax::Error>> {
    // `utf8(false)` accepts what `regex::bytes` accepts, e.g. `(?-u:\xFF)`.
    regex_syntax::ParserBuilder::new()
        .case_insensitive(case_insensitive)
        .utf8(false)
        .build()
        .parse(pattern)
        .map_err(Box::new)
}

pub(crate) fn plan_hir(hir: &Hir) -> TrigramQuery {
    info(hir).into_query()
}

/// Whether the pattern holds a literal newline, which line-oriented search
/// can never match; ripgrep rejects such patterns without `-U`.
pub(crate) fn has_newline_literal(hir: &Hir) -> bool {
    match hir.kind() {
        HirKind::Literal(lit) => lit.0.contains(&b'\n'),
        HirKind::Capture(c) => has_newline_literal(&c.sub),
        HirKind::Repetition(r) => has_newline_literal(&r.sub),
        HirKind::Concat(subs) | HirKind::Alternation(subs) => subs.iter().any(has_newline_literal),
        HirKind::Empty | HirKind::Look(_) | HirKind::Class(_) => false,
    }
}

fn info(hir: &Hir) -> Info {
    match hir.kind() {
        HirKind::Empty | HirKind::Look(_) => Info::empty_string(),
        HirKind::Literal(lit) => Info::exact(BTreeSet::from([fold(&lit.0)])),
        HirKind::Class(class) => class_info(class),
        HirKind::Capture(cap) => info(&cap.sub),
        HirKind::Repetition(rep) => {
            if rep.min == 0 {
                // `x?` is `x` or nothing; `x*` is unbounded.
                if rep.max == Some(1) {
                    let sub = info(&rep.sub);
                    if let Some(mut set) = sub.exact {
                        set.insert(Vec::new());
                        return Info::exact(set);
                    }
                }
                return Info::any();
            }
            let sub = info(&rep.sub);
            if rep.min == 1 && rep.max == Some(1) {
                return sub;
            }
            // At least one copy: whatever one copy requires.
            Info {
                exact: None,
                query: sub.into_query(),
            }
        }
        HirKind::Concat(subs) => concat_info(subs),
        HirKind::Alternation(subs) => {
            let infos: Vec<Info> = subs.iter().map(info).collect();
            if infos.iter().all(|i| i.exact.is_some()) {
                let mut union = Exact::new();
                for i in &infos {
                    union.extend(i.exact.iter().flatten().cloned());
                }
                if union.len() <= MAX_EXACT {
                    return Info::exact(union);
                }
            }
            let query = infos
                .into_iter()
                .map(Info::into_query)
                .reduce(TrigramQuery::or)
                .unwrap_or(TrigramQuery::All);
            Info { exact: None, query }
        }
    }
}

fn concat_info(subs: &[Hir]) -> Info {
    let mut query = TrigramQuery::All;
    // The exact strings of the run of children since the last unbounded one.
    let mut run: Exact = BTreeSet::from([Vec::new()]);
    // Whether the run covers the whole concatenation so far.
    let mut whole = true;
    for sub in subs {
        let child = info(sub);
        query = query.and(child.query);
        match child.exact {
            Some(set) => {
                if run.len() * set.len() <= MAX_EXACT {
                    run = run
                        .iter()
                        .flat_map(|a| set.iter().map(move |b| [a.as_slice(), b].concat()))
                        .collect();
                } else {
                    query = query.and(exact_query(&run));
                    run = set;
                    whole = false;
                }
            }
            None => {
                query = query.and(exact_query(&run));
                run = BTreeSet::from([Vec::new()]);
                whole = false;
            }
        }
    }
    if whole {
        Info {
            exact: Some(run),
            query,
        }
    } else {
        Info {
            exact: None,
            query: query.and(exact_query(&run)),
        }
    }
}

fn class_info(class: &Class) -> Info {
    let mut set = Exact::new();
    match class {
        Class::Unicode(c) => {
            let mut n = 0usize;
            for r in c.ranges() {
                n += (r.end() as usize).saturating_sub(r.start() as usize) + 1;
                if n > MAX_CLASS * 4 {
                    return Info::any();
                }
                for ch in r.start()..=r.end() {
                    let mut buf = [0; 4];
                    set.insert(fold(ch.encode_utf8(&mut buf).as_bytes()));
                }
            }
        }
        Class::Bytes(c) => {
            let mut n = 0usize;
            for r in c.ranges() {
                n += usize::from(r.end() - r.start()) + 1;
                if n > MAX_CLASS * 4 {
                    return Info::any();
                }
                for b in r.start()..=r.end() {
                    set.insert(vec![b.to_ascii_lowercase()]);
                }
            }
        }
    }
    if set.is_empty() || set.len() > MAX_CLASS {
        return Info::any();
    }
    Info::exact(set)
}

/// `Or` over the strings of `And` over each string's trigrams. Any string
/// shorter than three bytes constrains nothing, so neither does the set.
fn exact_query(set: &Exact) -> TrigramQuery {
    let mut out: Option<TrigramQuery> = None;
    for s in set {
        if s.len() < 3 {
            return TrigramQuery::All;
        }
        let mut grams: Vec<[u8; 3]> = s.windows(3).map(|w| [w[0], w[1], w[2]]).collect();
        grams.sort_unstable();
        grams.dedup();
        let q = grams
            .into_iter()
            .map(TrigramQuery::Trigram)
            .reduce(TrigramQuery::and)
            .unwrap_or(TrigramQuery::All);
        out = Some(match out {
            None => q,
            Some(prev) => prev.or(q),
        });
    }
    out.unwrap_or(TrigramQuery::All)
}

fn fold(bytes: &[u8]) -> Vec<u8> {
    bytes.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> TrigramQuery {
        let b = s.as_bytes();
        TrigramQuery::Trigram([b[0], b[1], b[2]])
    }

    #[test]
    fn literal_needs_all_its_trigrams() {
        assert_eq!(
            plan("Hello", false).unwrap(),
            TrigramQuery::And(vec![t("ell"), t("hel"), t("llo")])
        );
    }

    #[test]
    fn case_insensitive_folds_like_the_index() {
        assert_eq!(
            plan("(?i)hello", false).unwrap(),
            plan("hello", false).unwrap()
        );
        assert_eq!(plan("hello", true).unwrap(), plan("HELLO", false).unwrap());
    }

    #[test]
    fn alternation_is_an_or() {
        assert_eq!(
            plan("foo|bar", false).unwrap(),
            TrigramQuery::Or(vec![t("bar"), t("foo")])
        );
    }

    #[test]
    fn small_classes_expand_across_boundaries() {
        // `ab[cd]` must hold `abc` or `abd`.
        assert_eq!(
            plan("ab[cd]", false).unwrap(),
            TrigramQuery::Or(vec![t("abc"), t("abd")])
        );
    }

    #[test]
    fn unbounded_parts_split_runs() {
        assert_eq!(
            plan("foo.*bar", false).unwrap(),
            TrigramQuery::And(vec![t("foo"), t("bar")])
        );
        assert_eq!(
            plan(r"fn\s+main", false).unwrap(),
            TrigramQuery::And(vec![t("ain"), t("mai")])
        );
    }

    #[test]
    fn unselective_patterns_scan() {
        for p in [".", r"\w+", "^$", "ab", "a.c", "(foo)?", "x*", "[a-z]{3}"] {
            assert_eq!(plan(p, false).unwrap(), TrigramQuery::All, "{p}");
        }
    }

    #[test]
    fn repetition_needs_one_copy() {
        assert_eq!(plan("(abc)+", false).unwrap(), t("abc"));
        assert_eq!(plan("(abc){2,}", false).unwrap(), t("abc"));
    }

    #[test]
    fn optional_parts_branch() {
        // `colou?r` is `color` or `colour`.
        let q = plan("colou?r", false).unwrap();
        assert!(
            matches!(q, TrigramQuery::Or(ref v) if v.len() == 2),
            "{q:?}"
        );
    }

    #[test]
    fn non_ascii_keeps_raw_bytes() {
        let q = plan("café", false).unwrap();
        assert!(q.is_selective());
        // Case-insensitive non-ASCII expands both cases, never folds them.
        assert!(plan("(?i)ÉCOLE", false).unwrap().is_selective());
    }
}
