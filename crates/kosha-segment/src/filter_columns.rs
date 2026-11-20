//! Columnar filter index (#244): `filters.toc` plus one `filters-NNN.col`
//! file per filter column, replacing the row-oriented `filters.bin`.
//!
//! `filters.bin` holds every field's `(doc_seq, value)` pairs in one blob,
//! so a filter, sort or aggregation on one field had to fetch and parse
//! every field of the segment. Here each `(field, kind)` column is its own
//! file, so a query node hydrates `filters.toc` plus only the columns the
//! query names (`SearchQuery::filter_column_fields`).
//!
//! The format is chosen per segment by `Footer::filter_columns` (`Some` =
//! columnar, `None` = `filters.bin`), never by which files happen to be
//! on disk. Writers emit it only with `KOSHA_FILTERS_WRITE_VERSION=2`; see
//! [`filters_write_version`] for the rollout and rollback rules.
//!
//! ## `filters.toc`
//!
//! ```text
//! [magic u32 = TOC_MAGIC][version u32 = 1][crc32 u32 of the JSON]
//! [JSON: FiltersToc { doc_count, columns: [FilterColumnMeta] }]
//! ```
//!
//! The JSON lists the same entries as `Footer::filter_columns`. The footer
//! copy lets the hydration planner pick column files without a round trip;
//! the TOC keeps a segment directory self-describing.
//!
//! ## `filters-NNN.col`
//!
//! ```text
//! header (28 bytes):
//!   [magic u32 = COLUMN_MAGIC][version u16 = 1][kind u8][reserved u8]
//!   [doc_count u32][entry_count u32][body_len u64]
//!   [crc32 u32 of the 24 header bytes before it, then the body]
//! body:
//!   [presence_len u32][presence: roaring bitmap of docs with a value]
//!   [docs: entry_count x u32]            doc_seq per entry, non-decreasing
//!   string kind:
//!     [term_count u32]
//!     [term_offsets: (term_count + 1) x u64] into the pool
//!     [pool: sorted, distinct UTF-8 terms]
//!     [ords: entry_count x u32]          dictionary ordinal per entry
//!     [bitmap_offsets: (term_count + 1) x u64] into the bitmap area
//!     [bitmaps: one roaring bitmap of docs per term]
//!   integer / float kind:
//!     [values: entry_count x 8 bytes]    i64 / f64 LE, parallel to docs
//!     [zone_block u32][zone_count u32]
//!     [zones: zone_count x (min 8 bytes, max 8 bytes)]
//! ```
//!
//! Entries keep the writer's order (doc order, then field order within a
//! document), so a document with two values for a field keeps both and
//! the compatibility adapter rebuilds exactly the `FilterStore` that
//! `filters.bin` would have produced. Zone maps cover blocks of
//! [`ZONE_BLOCK`] entries; float zones ignore NaN.
//!
//! Every decode checks the magic, version, kind, CRC and every length and
//! ordinal, and fails with `KoshaError::CorruptSegment`: a damaged column
//! must fail the query, never quietly match nothing.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use kosha_core::{
    FilterColumnKind, FilterColumnMeta, FilterColumnStats, FilterStatValue, FilterStore,
    KoshaError, FILTER_STAT_MAX_STRING_BYTES,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

/// `"KFCL"` little-endian.
pub const COLUMN_MAGIC: u32 = 0x4C43_464B;
/// `"KFTC"` little-endian.
pub const TOC_MAGIC: u32 = 0x4354_464B;
pub const COLUMN_VERSION: u16 = 1;
pub const TOC_VERSION: u32 = 1;
const COLUMN_HEADER_LEN: usize = 28;
const TOC_HEADER_LEN: usize = 12;
/// Entries per numeric zone-map block.
pub const ZONE_BLOCK: usize = 4096;

/// Filter index format this process writes: `1` (`filters.bin`, the
/// default) or `2` (columnar). Read from `KOSHA_FILTERS_WRITE_VERSION`.
///
/// Rollout: every reader in this release reads both formats. Keep the gate
/// at `1` until every query and write node runs a release that reads `2`,
/// then flip writers. Rollback: set it back to `1`; compaction rewrites
/// columnar segments as `filters.bin` as it merges them. Columnar segments
/// already in S3 stay unreadable by a pre-#244 binary (it finds no
/// `filters.bin` and its filters match nothing), so do not roll back past
/// this release while any remain.
pub fn filters_write_version() -> u32 {
    static VERSION: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *VERSION.get_or_init(|| match std::env::var("KOSHA_FILTERS_WRITE_VERSION") {
        Ok(v) => match v.trim() {
            "1" | "" => 1,
            "2" => 2,
            other => {
                eprintln!(
                    "WARN: KOSHA_FILTERS_WRITE_VERSION={other} not supported (expected 1 or 2); \
                     writing filters.bin"
                );
                1
            }
        },
        Err(_) => 1,
    })
}

fn corrupt(what: &str, msg: impl std::fmt::Display) -> KoshaError {
    KoshaError::CorruptSegment(format!("{what}: {msg}"))
}

fn kind_byte(kind: FilterColumnKind) -> u8 {
    match kind {
        FilterColumnKind::String => 0,
        FilterColumnKind::Integer => 1,
        FilterColumnKind::Float => 2,
    }
}

// ─── In-memory columns ─────────────────────────────────────────────────────

/// One decoded filter column.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterColumn {
    String(StringColumn),
    Integer(NumericColumn<i64>),
    Float(NumericColumn<f64>),
}

impl FilterColumn {
    pub fn kind(&self) -> FilterColumnKind {
        match self {
            Self::String(_) => FilterColumnKind::String,
            Self::Integer(_) => FilterColumnKind::Integer,
            Self::Float(_) => FilterColumnKind::Float,
        }
    }

    /// Documents holding at least one value.
    pub fn present(&self) -> &RoaringBitmap {
        match self {
            Self::String(c) => &c.present,
            Self::Integer(c) => &c.present,
            Self::Float(c) => &c.present,
        }
    }

    fn docs(&self) -> &[u32] {
        match self {
            Self::String(c) => &c.docs,
            Self::Integer(c) => &c.docs,
            Self::Float(c) => &c.docs,
        }
    }

    /// Number of `(doc, value)` entries.
    pub fn len(&self) -> usize {
        self.docs().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Add this column's entries to `store` under `name`, as `filters.bin`
    /// would have — the compatibility adapter for `FilterApplier`, sorts
    /// and aggregations.
    pub fn append_to(&self, name: &str, store: &mut FilterStore) {
        match self {
            Self::String(c) => {
                store.string_fields.insert(
                    name.to_string(),
                    c.entries().map(|(d, v)| (d, v.to_string())).collect(),
                );
            }
            Self::Integer(c) => {
                store
                    .integer_fields
                    .insert(name.to_string(), c.entries().collect());
            }
            Self::Float(c) => {
                store
                    .float_fields
                    .insert(name.to_string(), c.entries().collect());
            }
        }
    }
}

/// A keyword/bool/date/text column: a sorted dictionary, one roaring
/// bitmap of documents per term, and each entry's dictionary ordinal (for
/// sorts and aggregations).
///
/// Bitmaps stay in their serialized form and are decoded per lookup: a
/// near-unique field (ids, paths) has as many terms as documents, and a
/// decoded `RoaringBitmap` per term would cost far more memory than the
/// column itself.
#[derive(Debug, Clone, PartialEq)]
pub struct StringColumn {
    pool: String,
    /// `term_count + 1` offsets into `pool`.
    term_offsets: Vec<u32>,
    docs: Vec<u32>,
    ords: Vec<u32>,
    /// `term_count + 1` offsets into `bitmap_area`.
    bitmap_offsets: Vec<u32>,
    /// Per term: a 4-byte doc_seq when the term is in exactly one document,
    /// otherwise a serialized roaring bitmap (never 4 bytes long).
    bitmap_area: Vec<u8>,
    present: RoaringBitmap,
    /// Element index over list-valued terms, built on first use (#244
    /// filter evaluation). Not part of the on-disk format.
    lists: LazyListIndex,
}

/// Which dictionary terms are JSON lists, and what they hold.
///
/// A list value is stored as its JSON text (one dictionary term), but a
/// `term` filter or a terms aggregation treats it as one value per element
/// (`kosha_core::list_elements`). This index answers "which list terms
/// contain element `e`" and "which terms count as no value" without
/// re-parsing JSON per query.
#[derive(Debug, Default)]
pub struct ListIndex {
    /// Element -> ordinals of list terms (text starting with `[`) holding
    /// it, ascending.
    pub elements: HashMap<String, Vec<u32>>,
    /// Ordinals of terms that parse as a list with no usable element
    /// (`[]`, `[null]`, ` []`): `exists` treats them as no value.
    pub empty: Vec<u32>,
}

/// A `OnceLock<ListIndex>` that is invisible to `Clone`/`PartialEq`: it is
/// a cache of what the dictionary already says.
#[derive(Debug, Default)]
struct LazyListIndex(std::sync::OnceLock<ListIndex>);

impl Clone for LazyListIndex {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl PartialEq for LazyListIndex {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

fn offset_u32(n: usize, what: &str) -> Result<u32, KoshaError> {
    u32::try_from(n).map_err(|_| {
        KoshaError::Io(std::io::Error::other(format!(
            "filter column {what} is {n} bytes, over the 4 GiB column format limit"
        )))
    })
}

impl StringColumn {
    pub fn term_count(&self) -> usize {
        self.term_offsets.len().saturating_sub(1)
    }

    /// Dictionary term `ord`.
    pub fn term(&self, ord: u32) -> &str {
        let i = ord as usize;
        &self.pool[self.term_offsets[i] as usize..self.term_offsets[i + 1] as usize]
    }

    /// Ordinal of `value` in the sorted dictionary.
    pub fn ordinal(&self, value: &str) -> Option<u32> {
        let (mut lo, mut hi) = (0usize, self.term_count());
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.term(mid as u32).cmp(value) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid as u32),
            }
        }
        None
    }

    /// Documents whose value equals `value`.
    pub fn term_bitmap(&self, value: &str) -> Option<RoaringBitmap> {
        self.ordinal(value).map(|ord| self.bitmap(ord))
    }

    /// Documents holding dictionary term `ord`. Validated at decode time,
    /// so this cannot fail.
    pub fn bitmap(&self, ord: u32) -> RoaringBitmap {
        decode_term_bitmap(self.term_bytes(ord)).expect("term bitmaps are validated at decode")
    }

    fn term_bytes(&self, ord: u32) -> &[u8] {
        let i = ord as usize;
        &self.bitmap_area[self.bitmap_offsets[i] as usize..self.bitmap_offsets[i + 1] as usize]
    }

    /// Per-entry doc_seqs (non-decreasing).
    pub fn docs(&self) -> &[u32] {
        &self.docs
    }

    /// Per-entry dictionary ordinals, parallel to [`Self::docs`].
    pub fn ords(&self) -> &[u32] {
        &self.ords
    }

    /// `(doc_seq, value)` in entry order.
    pub fn entries(&self) -> impl Iterator<Item = (u32, &str)> + '_ {
        self.docs
            .iter()
            .zip(&self.ords)
            .map(|(&d, &o)| (d, self.term(o)))
    }

    /// Build from `(doc_seq, value)` entries in doc order.
    pub fn from_entries(entries: &[(u32, String)]) -> Result<Self, KoshaError> {
        let mut terms: Vec<&str> = entries.iter().map(|(_, v)| v.as_str()).collect();
        terms.sort_unstable();
        terms.dedup();
        let mut pool = String::with_capacity(terms.iter().map(|t| t.len()).sum());
        let mut term_offsets = Vec::with_capacity(terms.len() + 1);
        term_offsets.push(0u32);
        for t in &terms {
            pool.push_str(t);
            term_offsets.push(offset_u32(pool.len(), "dictionary")?);
        }
        let ord_of: HashMap<&str, u32> = terms
            .iter()
            .enumerate()
            .map(|(i, t)| (*t, i as u32))
            .collect();
        let mut term_docs: Vec<Vec<u32>> = vec![Vec::new(); terms.len()];
        let mut present = RoaringBitmap::new();
        let mut docs = Vec::with_capacity(entries.len());
        let mut ords = Vec::with_capacity(entries.len());
        for (doc, value) in entries {
            let ord = ord_of[value.as_str()];
            docs.push(*doc);
            ords.push(ord);
            let list = &mut term_docs[ord as usize];
            if list.last() != Some(doc) {
                list.push(*doc);
            }
            present.insert(*doc);
        }
        let mut bitmap_area = Vec::new();
        let mut bitmap_offsets = Vec::with_capacity(terms.len() + 1);
        bitmap_offsets.push(0u32);
        for list in term_docs {
            if let [single] = list[..] {
                bitmap_area.extend_from_slice(&single.to_le_bytes());
            } else {
                let bitmap: RoaringBitmap = list.into_iter().collect();
                bitmap
                    .serialize_into(&mut bitmap_area)
                    .expect("writing to a Vec cannot fail");
            }
            bitmap_offsets.push(offset_u32(bitmap_area.len(), "bitmap area")?);
        }
        Ok(Self {
            pool,
            term_offsets,
            docs,
            ords,
            bitmap_offsets,
            bitmap_area,
            present,
            lists: LazyListIndex::default(),
        })
    }

    /// The list-element index (see [`ListIndex`]), built on first call by
    /// one pass over the dictionary.
    pub fn list_index(&self) -> &ListIndex {
        self.lists.0.get_or_init(|| {
            let mut index = ListIndex::default();
            for ord in 0..self.term_count() as u32 {
                let term = self.term(ord);
                if !term.trim_start().starts_with('[') {
                    continue;
                }
                let Some(elements) = kosha_core::list_elements(term) else {
                    continue;
                };
                // A list of objects (a nested container, #206) has no
                // plain elements but is a value, as in
                // `kosha_core::evaluate_filter`.
                if elements.is_empty() && kosha_core::nested::parse_container(term).is_none() {
                    index.empty.push(ord);
                }
                // Element matching only applies to text that starts with
                // `[` itself, as in `kosha_core::evaluate_filter`.
                if term.starts_with('[') {
                    for element in elements {
                        let ords = index.elements.entry(element).or_default();
                        if ords.last() != Some(&ord) {
                            ords.push(ord);
                        }
                    }
                }
            }
            index
        })
    }

    /// Dictionary ordinals of terms in `lo..hi` byte order (`None` bounds
    /// are open): `lower_inclusive`/`upper_inclusive` pick `>=`/`>` and
    /// `<=`/`<`.
    pub fn ordinal_range(
        &self,
        lo: Option<(&str, bool)>,
        hi: Option<(&str, bool)>,
    ) -> std::ops::Range<u32> {
        let n = self.term_count();
        let point = |pred: &dyn Fn(&str) -> bool| -> usize {
            let (mut a, mut b) = (0usize, n);
            while a < b {
                let mid = (a + b) / 2;
                if pred(self.term(mid as u32)) {
                    a = mid + 1;
                } else {
                    b = mid;
                }
            }
            a
        };
        let start = match lo {
            None => 0,
            Some((bound, true)) => point(&|t| t < bound),
            Some((bound, false)) => point(&|t| t <= bound),
        };
        let end = match hi {
            None => n,
            Some((bound, true)) => point(&|t| t <= bound),
            Some((bound, false)) => point(&|t| t < bound),
        };
        start as u32..end.max(start) as u32
    }

    /// Visit the documents holding dictionary term `ord`, without building
    /// a bitmap for single-document terms.
    pub fn for_each_doc(&self, ord: u32, mut f: impl FnMut(u32)) {
        let bytes = self.term_bytes(ord);
        if let Ok(single) = <[u8; 4]>::try_from(bytes) {
            f(u32::from_le_bytes(single));
        } else {
            for doc in self.bitmap(ord) {
                f(doc);
            }
        }
    }

    /// Documents holding at least one value.
    pub fn present(&self) -> &RoaringBitmap {
        &self.present
    }

    fn stats(&self) -> FilterColumnStats {
        let bound = |ord: u32| {
            let t = self.term(ord);
            (t.len() <= FILTER_STAT_MAX_STRING_BYTES).then(|| FilterStatValue::String(t.into()))
        };
        let n = self.term_count() as u32;
        FilterColumnStats {
            values: self.docs.len() as u64,
            docs: self.present.len(),
            distinct: n as u64,
            min: if n > 0 { bound(0) } else { None },
            max: if n > 0 { bound(n - 1) } else { None },
            non_finite: false,
        }
    }
}

/// A term's documents from its bitmap-area bytes: 4 bytes is one raw
/// doc_seq (a serialized roaring bitmap is never 4 bytes long).
fn decode_term_bitmap(bytes: &[u8]) -> std::io::Result<RoaringBitmap> {
    if let Ok(single) = <[u8; 4]>::try_from(bytes) {
        let mut b = RoaringBitmap::new();
        b.insert(u32::from_le_bytes(single));
        return Ok(b);
    }
    RoaringBitmap::deserialize_from(bytes)
}

/// A numeric value type a [`NumericColumn`] can hold.
pub trait ColumnValue: Copy + PartialOrd + std::fmt::Debug {
    const KIND: FilterColumnKind;
    fn to_le(self) -> [u8; 8];
    fn from_le(b: [u8; 8]) -> Self;
    /// Whether the value takes part in min/max (false for NaN).
    fn comparable(self) -> bool;
    fn stat(self) -> Option<FilterStatValue>;
    fn bits(self) -> u64;
}

impl ColumnValue for i64 {
    const KIND: FilterColumnKind = FilterColumnKind::Integer;
    fn to_le(self) -> [u8; 8] {
        self.to_le_bytes()
    }
    fn from_le(b: [u8; 8]) -> Self {
        i64::from_le_bytes(b)
    }
    fn comparable(self) -> bool {
        true
    }
    fn stat(self) -> Option<FilterStatValue> {
        Some(FilterStatValue::Integer(self))
    }
    fn bits(self) -> u64 {
        self as u64
    }
}

impl ColumnValue for f64 {
    const KIND: FilterColumnKind = FilterColumnKind::Float;
    fn to_le(self) -> [u8; 8] {
        self.to_le_bytes()
    }
    fn from_le(b: [u8; 8]) -> Self {
        f64::from_le_bytes(b)
    }
    fn comparable(self) -> bool {
        !self.is_nan()
    }
    fn stat(self) -> Option<FilterStatValue> {
        self.is_finite().then_some(FilterStatValue::Float(self))
    }
    fn bits(self) -> u64 {
        self.to_bits()
    }
}

/// An integer or float column: values in entry (doc) order, per-block
/// min/max zone maps and a presence bitmap.
#[derive(Debug, Clone, PartialEq)]
pub struct NumericColumn<T> {
    docs: Vec<u32>,
    values: Vec<T>,
    /// `(min, max)` per [`ZONE_BLOCK`] entries, NaN ignored. A block of
    /// only NaNs has NaN bounds, which compare false with everything.
    zones: Vec<(T, T)>,
    present: RoaringBitmap,
}

impl<T: ColumnValue> NumericColumn<T> {
    pub fn docs(&self) -> &[u32] {
        &self.docs
    }

    pub fn values(&self) -> &[T] {
        &self.values
    }

    /// `(min, max)` per block of [`ZONE_BLOCK`] entries.
    pub fn zones(&self) -> &[(T, T)] {
        &self.zones
    }

    pub fn entries(&self) -> impl Iterator<Item = (u32, T)> + '_ {
        self.docs.iter().copied().zip(self.values.iter().copied())
    }

    /// Build from `(doc_seq, value)` entries in doc order.
    pub fn from_entries(entries: &[(u32, T)]) -> Self {
        let docs: Vec<u32> = entries.iter().map(|(d, _)| *d).collect();
        let values: Vec<T> = entries.iter().map(|(_, v)| *v).collect();
        let zones = values.chunks(ZONE_BLOCK).map(zone_of).collect();
        let mut present = RoaringBitmap::new();
        present.extend(docs.iter().copied());
        Self {
            docs,
            values,
            zones,
            present,
        }
    }

    fn stats(&self) -> FilterColumnStats {
        let mut min: Option<T> = None;
        let mut max: Option<T> = None;
        let mut non_finite = false;
        for &v in &self.values {
            if v.stat().is_none() {
                non_finite = true;
                continue;
            }
            if min.is_none_or(|m| v < m) {
                min = Some(v);
            }
            if max.is_none_or(|m| v > m) {
                max = Some(v);
            }
        }
        let distinct: std::collections::HashSet<u64> =
            self.values.iter().map(|v| v.bits()).collect();
        FilterColumnStats {
            values: self.values.len() as u64,
            docs: self.present.len(),
            distinct: distinct.len() as u64,
            min: min.and_then(ColumnValue::stat),
            max: max.and_then(ColumnValue::stat),
            non_finite,
        }
    }
}

/// `(min, max)` of a block, ignoring NaN. An all-NaN block yields the
/// block's first value twice (NaN), which compares false with everything.
fn zone_of<T: ColumnValue>(block: &[T]) -> (T, T) {
    let mut it = block.iter().copied().filter(|v| v.comparable());
    match it.next() {
        None => (block[0], block[0]),
        Some(first) => it.fold((first, first), |(lo, hi), v| {
            (if v < lo { v } else { lo }, if v > hi { v } else { hi })
        }),
    }
}

// ─── Encoding ──────────────────────────────────────────────────────────────

/// Body flag: every present document has exactly one entry, so the
/// per-entry doc_seqs are the presence bitmap in order and are not stored.
const FLAG_SINGLE_VALUED: u8 = 1;

/// Bytes per dictionary ordinal for `term_count` terms.
fn ord_width(term_count: usize) -> usize {
    match term_count {
        0..=0x100 => 1,
        0x101..=0x1_0000 => 2,
        0x1_0001..=0x100_0000 => 3,
        _ => 4,
    }
}

/// Serialize `column` as a `filters-NNN.col` file for a segment of
/// `doc_count` documents.
pub fn encode_column(column: &FilterColumn, doc_count: u32) -> Vec<u8> {
    let mut body = Vec::new();
    let present = column.present();
    body.extend_from_slice(&(present.serialized_size() as u32).to_le_bytes());
    present
        .serialize_into(&mut body)
        .expect("writing to a Vec cannot fail");
    let docs = column.docs();
    let single_valued = docs.len() as u64 == present.len();
    body.push(if single_valued { FLAG_SINGLE_VALUED } else { 0 });
    if !single_valued {
        for d in docs {
            body.extend_from_slice(&d.to_le_bytes());
        }
    }
    match column {
        FilterColumn::String(c) => {
            body.extend_from_slice(&(c.term_count() as u32).to_le_bytes());
            for off in &c.term_offsets {
                body.extend_from_slice(&off.to_le_bytes());
            }
            body.extend_from_slice(c.pool.as_bytes());
            let width = ord_width(c.term_count());
            for o in &c.ords {
                body.extend_from_slice(&o.to_le_bytes()[..width]);
            }
            for off in &c.bitmap_offsets {
                body.extend_from_slice(&off.to_le_bytes());
            }
            body.extend_from_slice(&c.bitmap_area);
        }
        FilterColumn::Integer(c) => encode_numeric(&mut body, c),
        FilterColumn::Float(c) => encode_numeric(&mut body, c),
    }
    let mut out = Vec::with_capacity(COLUMN_HEADER_LEN + body.len());
    out.extend_from_slice(&COLUMN_MAGIC.to_le_bytes());
    out.extend_from_slice(&COLUMN_VERSION.to_le_bytes());
    out.push(kind_byte(column.kind()));
    out.push(0);
    out.extend_from_slice(&doc_count.to_le_bytes());
    out.extend_from_slice(&(docs.len() as u32).to_le_bytes());
    out.extend_from_slice(&(body.len() as u64).to_le_bytes());
    let crc = column_crc(&out, &body);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// CRC32 over the header fields before the checksum, then the body.
fn column_crc(header: &[u8], body: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&header[..COLUMN_HEADER_LEN - 4]);
    h.update(body);
    h.finalize()
}

fn encode_numeric<T: ColumnValue>(body: &mut Vec<u8>, c: &NumericColumn<T>) {
    for v in &c.values {
        body.extend_from_slice(&v.to_le());
    }
    body.extend_from_slice(&(ZONE_BLOCK as u32).to_le_bytes());
    body.extend_from_slice(&(c.zones.len() as u32).to_le_bytes());
    for (lo, hi) in &c.zones {
        body.extend_from_slice(&lo.to_le());
        body.extend_from_slice(&hi.to_le());
    }
}

/// Per-segment stats of `column`, for the TOC and the footer.
pub fn column_stats(column: &FilterColumn) -> FilterColumnStats {
    match column {
        FilterColumn::String(c) => c.stats(),
        FilterColumn::Integer(c) => c.stats(),
        FilterColumn::Float(c) => c.stats(),
    }
}

// ─── Decoding ──────────────────────────────────────────────────────────────

struct Cursor<'a> {
    data: &'a [u8],
    what: &'a str,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], KoshaError> {
        if self.data.len() < n {
            return Err(corrupt(
                self.what,
                format!("truncated: need {n} more bytes, have {}", self.data.len()),
            ));
        }
        let (head, rest) = self.data.split_at(n);
        self.data = rest;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, KoshaError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, KoshaError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, KoshaError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn b8(&mut self) -> Result<[u8; 8], KoshaError> {
        Ok(self.take(8)?.try_into().unwrap())
    }
    /// `n` items of `size` bytes, checked against what is left before any
    /// allocation, so a corrupt count cannot ask for gigabytes.
    fn array(&mut self, n: usize, size: usize) -> Result<&'a [u8], KoshaError> {
        let bytes = n
            .checked_mul(size)
            .ok_or_else(|| corrupt(self.what, "length overflow"))?;
        self.take(bytes)
    }
    /// `n` little-endian unsigned ints of `width` (1..=4) bytes each.
    fn uints(&mut self, n: usize, width: usize) -> Result<Vec<u32>, KoshaError> {
        Ok(self
            .array(n, width)?
            .chunks_exact(width)
            .map(|c| {
                let mut b = [0u8; 4];
                b[..width].copy_from_slice(c);
                u32::from_le_bytes(b)
            })
            .collect())
    }
    fn bitmap(&mut self, len: usize) -> Result<RoaringBitmap, KoshaError> {
        let bytes = self.take(len)?;
        RoaringBitmap::deserialize_from(bytes)
            .map_err(|e| corrupt(self.what, format!("bitmap: {e}")))
    }
}

/// Offsets that must start at 0 and never decrease.
fn check_offsets(offsets: &[u32], what: &str, name: &str) -> Result<(), KoshaError> {
    if offsets.first() != Some(&0) || offsets.windows(2).any(|w| w[0] > w[1]) {
        return Err(corrupt(what, format!("{name} offsets out of order")));
    }
    Ok(())
}

/// Decode a `filters-NNN.col` file. `expected` is the kind the TOC lists
/// for it and `doc_count` the segment's document count; a mismatch with
/// either is corruption.
pub fn decode_column(
    data: &[u8],
    expected: FilterColumnKind,
    doc_count: u32,
    what: &str,
) -> Result<FilterColumn, KoshaError> {
    let mut header = Cursor { data, what };
    let head = header.take(COLUMN_HEADER_LEN)?;
    let mut h = Cursor { data: head, what };
    if h.u32()? != COLUMN_MAGIC {
        return Err(corrupt(what, "bad magic"));
    }
    let version = u16::from_le_bytes(h.take(2)?.try_into().unwrap());
    if version != COLUMN_VERSION {
        return Err(corrupt(
            what,
            format!("unsupported column version {version}"),
        ));
    }
    let kind = h.u8()?;
    if h.u8()? != 0 {
        return Err(corrupt(what, "reserved header byte set"));
    }
    if kind != kind_byte(expected) {
        return Err(corrupt(
            what,
            format!("kind byte {kind}, TOC says {expected:?}"),
        ));
    }
    let file_docs = h.u32()?;
    if file_docs != doc_count {
        return Err(corrupt(
            what,
            format!("written for {file_docs} documents, segment has {doc_count}"),
        ));
    }
    let entry_count = h.u32()? as usize;
    let body_len = h.u64()?;
    let crc = h.u32()?;
    let body = header.data;
    if body.len() as u64 != body_len {
        return Err(corrupt(
            what,
            format!("body is {} bytes, header says {body_len}", body.len()),
        ));
    }
    if column_crc(head, body) != crc {
        return Err(corrupt(what, "checksum mismatch"));
    }

    let mut c = Cursor { data: body, what };
    let presence_len = c.u32()? as usize;
    let present = c.bitmap(presence_len)?;
    if present.max().is_some_and(|d| d >= doc_count) {
        return Err(corrupt(what, "doc_seq past the segment's doc count"));
    }
    let flags = c.u8()?;
    if flags & !FLAG_SINGLE_VALUED != 0 {
        return Err(corrupt(what, format!("unknown flags {flags:#x}")));
    }
    let docs: Vec<u32> = if flags & FLAG_SINGLE_VALUED != 0 {
        if present.len() != entry_count as u64 {
            return Err(corrupt(
                what,
                "single-valued column's presence disagrees with entries",
            ));
        }
        present.iter().collect()
    } else {
        let docs = c.uints(entry_count, 4)?;
        if docs.windows(2).any(|w| w[0] > w[1]) {
            return Err(corrupt(what, "doc_seqs out of order"));
        }
        let mut distinct = docs.clone();
        distinct.dedup();
        if present.len() != distinct.len() as u64 || !distinct.iter().all(|d| present.contains(*d))
        {
            return Err(corrupt(what, "presence bitmap disagrees with entries"));
        }
        docs
    };

    let column = match expected {
        FilterColumnKind::String => FilterColumn::String(decode_string(&mut c, docs, present)?),
        FilterColumnKind::Integer => FilterColumn::Integer(decode_numeric(&mut c, docs, present)?),
        FilterColumnKind::Float => FilterColumn::Float(decode_numeric(&mut c, docs, present)?),
    };
    if !c.data.is_empty() {
        return Err(corrupt(what, format!("{} trailing bytes", c.data.len())));
    }
    Ok(column)
}

fn decode_string(
    c: &mut Cursor<'_>,
    docs: Vec<u32>,
    present: RoaringBitmap,
) -> Result<StringColumn, KoshaError> {
    let what = c.what;
    let term_count = c.u32()? as usize;
    let term_offsets = c.uints(
        term_count
            .checked_add(1)
            .ok_or_else(|| corrupt(what, "term count overflow"))?,
        4,
    )?;
    check_offsets(&term_offsets, what, "term")?;
    let pool_len = *term_offsets.last().unwrap() as usize;
    let pool = std::str::from_utf8(c.take(pool_len)?)
        .map_err(|e| corrupt(what, format!("dictionary is not UTF-8: {e}")))?
        .to_string();
    if term_offsets
        .iter()
        .any(|&o| !pool.is_char_boundary(o as usize))
    {
        return Err(corrupt(what, "term offset splits a character"));
    }
    let ords = c.uints(docs.len(), ord_width(term_count))?;
    if ords.iter().any(|&o| o as usize >= term_count) {
        return Err(corrupt(what, "ordinal past the dictionary"));
    }
    let bitmap_offsets = c.uints(term_count + 1, 4)?;
    check_offsets(&bitmap_offsets, what, "bitmap")?;
    let area_len = *bitmap_offsets.last().unwrap() as usize;
    let bitmap_area = c.take(area_len)?.to_vec();
    let column = StringColumn {
        pool,
        term_offsets,
        docs,
        ords,
        bitmap_offsets,
        bitmap_area,
        present,
        lists: LazyListIndex::default(),
    };
    for i in 1..column.term_count() {
        if column.term(i as u32 - 1) >= column.term(i as u32) {
            return Err(corrupt(what, "dictionary not sorted and distinct"));
        }
    }
    // Every term bitmap must decode and hold exactly the documents whose
    // entries carry that ordinal, so later lookups cannot fail or lie.
    let mut expected: Vec<Vec<u32>> = vec![Vec::new(); column.term_count()];
    for (&d, &o) in column.docs.iter().zip(&column.ords) {
        let list = &mut expected[o as usize];
        if list.last() != Some(&d) {
            list.push(d);
        }
    }
    for (ord, mut want) in expected.into_iter().enumerate() {
        want.sort_unstable();
        want.dedup();
        let got = decode_term_bitmap(column.term_bytes(ord as u32))
            .map_err(|e| corrupt(what, format!("bitmap of term {ord}: {e}")))?;
        if got.len() != want.len() as u64 || !want.iter().all(|d| got.contains(*d)) {
            return Err(corrupt(
                what,
                format!("bitmap of term {ord} disagrees with entries"),
            ));
        }
    }
    Ok(column)
}

fn decode_numeric<T: ColumnValue>(
    c: &mut Cursor<'_>,
    docs: Vec<u32>,
    present: RoaringBitmap,
) -> Result<NumericColumn<T>, KoshaError> {
    let what = c.what;
    let values: Vec<T> = c
        .array(docs.len(), 8)?
        .as_chunks::<8>()
        .0
        .iter()
        .map(|b| T::from_le(*b))
        .collect();
    let block = c.u32()? as usize;
    if block != ZONE_BLOCK {
        return Err(corrupt(
            what,
            format!("zone block {block}, expected {ZONE_BLOCK}"),
        ));
    }
    let zone_count = c.u32()? as usize;
    if zone_count != values.len().div_ceil(ZONE_BLOCK) {
        return Err(corrupt(what, "zone count disagrees with entries"));
    }
    let mut zones = Vec::with_capacity(zone_count);
    for _ in 0..zone_count {
        zones.push((T::from_le(c.b8()?), T::from_le(c.b8()?)));
    }
    // Filter evaluation trusts the zones to skip blocks and to take whole
    // blocks without comparing values, so they must be exactly what the
    // values say.
    for ((lo, hi), block) in zones.iter().zip(values.chunks(ZONE_BLOCK)) {
        let (want_lo, want_hi) = zone_of(block);
        if lo.bits() != want_lo.bits() || hi.bits() != want_hi.bits() {
            return Err(corrupt(what, "zone map disagrees with values"));
        }
    }
    Ok(NumericColumn {
        docs,
        values,
        zones,
        present,
    })
}

// ─── TOC ───────────────────────────────────────────────────────────────────

/// Parsed `filters.toc`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FiltersToc {
    pub doc_count: u32,
    pub columns: Vec<FilterColumnMeta>,
}

impl FiltersToc {
    pub fn encode(&self) -> Vec<u8> {
        let json = serde_json::to_vec(self).expect("FiltersToc serializes");
        let mut out = Vec::with_capacity(TOC_HEADER_LEN + json.len());
        out.extend_from_slice(&TOC_MAGIC.to_le_bytes());
        out.extend_from_slice(&TOC_VERSION.to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&json).to_le_bytes());
        out.extend_from_slice(&json);
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self, KoshaError> {
        let what = kosha_core::FILTERS_TOC_FILE;
        let mut c = Cursor { data, what };
        if c.u32()? != TOC_MAGIC {
            return Err(corrupt(what, "bad magic"));
        }
        let version = c.u32()?;
        if version != TOC_VERSION {
            return Err(corrupt(what, format!("unsupported version {version}")));
        }
        let crc = c.u32()?;
        if crc32fast::hash(c.data) != crc {
            return Err(corrupt(what, "checksum mismatch"));
        }
        let toc: Self = serde_json::from_slice(c.data).map_err(|e| corrupt(what, e))?;
        let mut ids = BTreeSet::new();
        let mut keys = BTreeSet::new();
        for col in &toc.columns {
            if !ids.insert(col.id) || !keys.insert((col.name.as_str(), col.kind)) {
                return Err(corrupt(what, format!("duplicate column {:?}", col.name)));
            }
        }
        Ok(toc)
    }

    pub fn find(&self, name: &str, kind: FilterColumnKind) -> Option<&FilterColumnMeta> {
        self.columns
            .iter()
            .find(|c| c.name == name && c.kind == kind)
    }
}

/// Build and encode every column of a segment's filter values, one at a
/// time (only one encoded column is in memory at once), handing each to
/// `sink` as `(meta, file bytes)`. Ids are assigned over `(kind, name)`
/// sorted. Returns the directory, in id order.
pub fn build_columns(
    doc_count: u32,
    strings: &HashMap<String, Vec<(u32, String)>>,
    integers: &HashMap<String, Vec<(u32, i64)>>,
    floats: &HashMap<String, Vec<(u32, f64)>>,
    mut sink: impl FnMut(&FilterColumnMeta, &[u8]) -> Result<(), KoshaError>,
) -> Result<Vec<FilterColumnMeta>, KoshaError> {
    let mut keys: Vec<(FilterColumnKind, &str)> = strings
        .keys()
        .map(|n| (FilterColumnKind::String, n.as_str()))
        .chain(
            integers
                .keys()
                .map(|n| (FilterColumnKind::Integer, n.as_str())),
        )
        .chain(floats.keys().map(|n| (FilterColumnKind::Float, n.as_str())))
        .collect();
    keys.sort_unstable();
    let mut metas = Vec::with_capacity(keys.len());
    for (id, (kind, name)) in keys.into_iter().enumerate() {
        let column = match kind {
            FilterColumnKind::String => {
                FilterColumn::String(StringColumn::from_entries(&strings[name])?)
            }
            FilterColumnKind::Integer => {
                FilterColumn::Integer(NumericColumn::from_entries(&integers[name]))
            }
            FilterColumnKind::Float => {
                FilterColumn::Float(NumericColumn::from_entries(&floats[name]))
            }
        };
        let bytes = encode_column(&column, doc_count);
        let meta = FilterColumnMeta {
            name: name.to_string(),
            kind,
            id: id as u32,
            bytes: bytes.len() as u64,
            stats: column_stats(&column),
        };
        drop(column);
        sink(&meta, &bytes)?;
        metas.push(meta);
    }
    Ok(metas)
}

/// Read a whole file; `Ok(None)` when it is absent or empty (not hydrated
/// yet — never cached by callers).
pub(crate) fn read_present(path: &Path) -> Result<Option<Vec<u8>>, KoshaError> {
    match std::fs::read(path) {
        Ok(bytes) if bytes.is_empty() => Ok(None),
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Whether every filter file `footer` names is present (non-empty) in
/// `segment_dir`: `filters.bin` for a legacy segment, `filters.toc` and
/// every column for a columnar one.
pub fn filter_files_present(segment_dir: &Path, footer: &kosha_core::Footer) -> bool {
    footer
        .filter_files_for(None)
        .iter()
        .all(|f| std::fs::metadata(segment_dir.join(f)).is_ok_and(|m| m.is_file() && m.len() > 0))
}

#[cfg(test)]
mod tests {
    #[test]
    fn zone_maps_must_agree_with_values() {
        let entries: Vec<(u32, i64)> = (0..5000).map(|i| (i, i as i64)).collect();
        let encoded = encode_column(
            &FilterColumn::Integer(NumericColumn::from_entries(&entries)),
            5000,
        );
        assert!(decode_column(&encoded, FilterColumnKind::Integer, 5000, "ok").is_ok());
        // The last zone's max is the body's last 8 bytes. Lower it and
        // re-sign the CRC: the file is self-consistent but lies, which
        // would make evaluation skip or wholesale-take the wrong blocks.
        let mut bad = encoded.clone();
        let n = bad.len();
        bad[n - 8..].copy_from_slice(&100i64.to_le_bytes());
        let crc = column_crc(&bad[..COLUMN_HEADER_LEN], &bad[COLUMN_HEADER_LEN..]);
        bad[COLUMN_HEADER_LEN - 4..COLUMN_HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
        let err = decode_column(&bad, FilterColumnKind::Integer, 5000, "bad").unwrap_err();
        assert!(err.to_string().contains("zone map"), "{err}");
    }

    use super::*;

    fn strings() -> Vec<(u32, String)> {
        vec![
            (0, "b".into()),
            (0, "a".into()),
            (2, "b".into()),
            (5, "".into()),
            (7, "zeta".into()),
        ]
    }

    #[test]
    fn string_column_round_trips_with_bitmaps_and_ordinals() {
        let col = FilterColumn::String(StringColumn::from_entries(&strings()).unwrap());
        let bytes = encode_column(&col, 8);
        let back = decode_column(&bytes, FilterColumnKind::String, 8, "t").unwrap();
        assert_eq!(back, col);
        let FilterColumn::String(s) = &back else {
            panic!()
        };
        assert_eq!(s.term_count(), 4);
        assert_eq!(s.term(0), "");
        assert_eq!(
            s.term_bitmap("b").unwrap().iter().collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert!(s.term_bitmap("nope").is_none());
        assert_eq!(
            s.entries()
                .map(|(d, v)| (d, v.to_string()))
                .collect::<Vec<_>>(),
            strings()
        );
        assert_eq!(back.present().iter().collect::<Vec<_>>(), vec![0, 2, 5, 7]);
        let stats = column_stats(&back);
        assert_eq!(stats.values, 5);
        assert_eq!(stats.docs, 4);
        assert_eq!(stats.distinct, 4);
        assert_eq!(stats.min, Some(FilterStatValue::String("".into())));
        assert_eq!(stats.max, Some(FilterStatValue::String("zeta".into())));
    }

    #[test]
    fn numeric_columns_round_trip_with_zone_maps() {
        let ints: Vec<(u32, i64)> = (0..10_000u32).map(|d| (d, (d as i64) * 3 - 7)).collect();
        let col = FilterColumn::Integer(NumericColumn::from_entries(&ints));
        let back = decode_column(
            &encode_column(&col, 10_000),
            FilterColumnKind::Integer,
            10_000,
            "t",
        )
        .unwrap();
        assert_eq!(back, col);
        let FilterColumn::Integer(n) = &back else {
            panic!()
        };
        assert_eq!(n.zones().len(), 3);
        assert_eq!(n.zones()[0], (-7, 4095 * 3 - 7));
        assert_eq!(n.zones()[2], (8192 * 3 - 7, 9999 * 3 - 7));
        let stats = column_stats(&back);
        assert_eq!(stats.min, Some(FilterStatValue::Integer(-7)));
        assert_eq!(stats.max, Some(FilterStatValue::Integer(9999 * 3 - 7)));
        assert_eq!(stats.distinct, 10_000);

        let floats = vec![(1, 2.5), (1, f64::NAN), (3, -1.0), (4, f64::INFINITY)];
        let col = FilterColumn::Float(NumericColumn::from_entries(&floats));
        let back = decode_column(&encode_column(&col, 5), FilterColumnKind::Float, 5, "t").unwrap();
        let FilterColumn::Float(f) = &back else {
            panic!()
        };
        assert_eq!(f.values().len(), 4);
        assert!(f.values()[1].is_nan());
        assert_eq!(f.zones()[0], (-1.0, f64::INFINITY));
        let stats = column_stats(&back);
        assert!(stats.non_finite);
        assert_eq!(stats.min, Some(FilterStatValue::Float(-1.0)));
        assert_eq!(stats.max, Some(FilterStatValue::Float(2.5)));
    }

    #[test]
    fn empty_column_round_trips() {
        for col in [
            FilterColumn::String(StringColumn::from_entries(&[]).unwrap()),
            FilterColumn::Integer(NumericColumn::from_entries(&[])),
            FilterColumn::Float(NumericColumn::from_entries(&[])),
        ] {
            let back = decode_column(&encode_column(&col, 0), col.kind(), 0, "t").unwrap();
            assert_eq!(back, col);
            assert!(back.is_empty());
            let stats = column_stats(&back);
            assert_eq!((stats.values, stats.min), (0, None));
        }
    }

    #[test]
    fn corrupt_or_truncated_columns_fail_closed() {
        let col = FilterColumn::String(StringColumn::from_entries(&strings()).unwrap());
        let bytes = encode_column(&col, 8);
        // Every truncation is an error, never a shorter column.
        for len in 0..bytes.len() {
            assert!(
                decode_column(&bytes[..len], FilterColumnKind::String, 8, "t").is_err(),
                "truncated to {len} bytes must fail"
            );
        }
        // Every single flipped bit is caught (header fields or CRC).
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x10;
            assert!(
                decode_column(&bad, FilterColumnKind::String, 8, "t").is_err(),
                "flipped byte {i} must fail"
            );
        }
        // Wrong kind or doc count is corruption too.
        assert!(decode_column(&bytes, FilterColumnKind::Integer, 8, "t").is_err());
        assert!(decode_column(&bytes, FilterColumnKind::String, 9, "t").is_err());
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(decode_column(&extra, FilterColumnKind::String, 8, "t").is_err());
    }

    #[test]
    fn toc_round_trips_and_rejects_damage() {
        let mut strings_map = HashMap::new();
        strings_map.insert("a/b ../x".to_string(), strings());
        let mut ints = HashMap::new();
        ints.insert("a/b ../x".to_string(), vec![(1, 5i64)]);
        let built = build_columns(8, &strings_map, &ints, &HashMap::new(), |_, _| Ok(())).unwrap();
        assert_eq!(built.len(), 2, "same name, two kinds: two columns");
        let toc = FiltersToc {
            doc_count: 8,
            columns: built,
        };
        let bytes = toc.encode();
        assert_eq!(FiltersToc::decode(&bytes).unwrap(), toc);
        assert_eq!(toc.columns[0].file_name(), "filters-000.col");
        assert!(toc.find("a/b ../x", FilterColumnKind::Integer).is_some());
        for len in 0..bytes.len() {
            assert!(FiltersToc::decode(&bytes[..len]).is_err());
        }
        let mut bad = bytes.clone();
        let last = bad.len() - 2;
        bad[last] ^= 1;
        assert!(FiltersToc::decode(&bad).is_err());
    }
}
