//! Indexed grep: a research prototype for #588.
//!
//! Exact, ripgrep-compatible regex search over raw text, accelerated by a
//! non-positional trigram index (Russ Cox, "Regular Expression Matching with
//! a Trigram Index", 2012):
//!
//! 1. [`TrigramIndex`] maps every trigram of each document's raw bytes
//!    (ASCII-lowercased, never spanning a newline) to a roaring bitmap of
//!    the documents containing it.
//! 2. [`plan`] turns a regex into a [`TrigramQuery`]: an AND/OR tree of
//!    trigrams that every matching document must satisfy. It is a necessary
//!    condition only, so the candidate set is a superset of the true one.
//! 3. [`Grep::run`] intersects posting lists into candidates, fetches each
//!    candidate and verifies it line by line with the `regex` crate, the
//!    engine ripgrep itself uses. Results are exact: the index only decides
//!    what is read, never what matches.
//!
//! A pattern with no required trigram (`.`, `\w+`, `^$`) plans to
//! [`TrigramQuery::All`] and degrades to a scan of every document, bounded
//! by [`GrepOptions`] byte and time budgets; hitting one is reported in
//! [`GrepOutcome::truncated`], never silently.

pub mod corpus;
mod grep;
mod index;
mod plan;

pub use grep::{Grep, GrepError, GrepMatch, GrepOptions, GrepOutcome, Truncation};
pub use index::{positional_bytes, trigrams, IndexStats, TrigramIndex};
pub use plan::{plan, TrigramQuery};
