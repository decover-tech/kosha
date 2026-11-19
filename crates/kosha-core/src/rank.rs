//! Score modifiers: `rank_by` (issue #212).
//!
//! A `rank_by` expression blends document attributes into the relevance
//! score: a **field-value** boost and **decay** functions (linear,
//! exponential, gaussian) over `Integer`, `Float`, `Date` and `GeoPoint`
//! fields. Every input comes from a segment's filter columns
//! (`filters.bin`), so computing a modifier never reads the doc store.
//!
//! Modifiers are applied in a **rescore window**: the query is first ranked
//! by its text (or fused, or vector) score exactly as without `rank_by`,
//! the best `window_size` hits are kept, and only those are rescored and
//! re-sorted. The first phase is the unmodified query, so the block-max
//! WAND upper bounds it prunes with stay valid: an unbounded modifier is
//! never folded into a block maximum.
//!
//! This module holds the request types, their validation and compilation
//! (`origin: "now"` is resolved once per request), the value parsers
//! (dates, durations, distances, geo points) and the per-document
//! evaluation against a [`FilterStore`].

use serde::{Deserialize, Serialize};

use crate::{list_elements, lookup_field, FilterStore};

/// Window when the request does not set `window_size` (it is never less
/// than `max_results`).
pub const DEFAULT_RANK_BY_WINDOW: usize = 100;
/// Largest accepted `window_size`.
pub const MAX_RANK_BY_WINDOW: usize = 10_000;
/// Mean Earth radius in meters (IUGG), as used by Elasticsearch's arc
/// distance.
pub const EARTH_RADIUS_M: f64 = 6_371_008.771_4;

/// `SearchQuery::rank_by`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankBySpec {
    /// One or more functions; combined with `score_mode`.
    pub functions: Vec<ScoreFunction>,
    /// How the functions' weighted values combine. Default `sum`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_mode: Option<ScoreMode>,
    /// How the combined value is applied to the text score. Default
    /// `multiply`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boost_mode: Option<BoostMode>,
    /// Multiplies the text (or fused / vector) score before it is combined.
    /// Default 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_weight: Option<f64>,
    /// How many of the best hits by text score are rescored. Default
    /// `max(100, max_results)`; at most 10,000. Hits ranked below the
    /// window by text score are not returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_size: Option<usize>,
}

/// One modifier: exactly one of `decay` / `field_value`, times `weight`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScoreFunction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decay: Option<DecaySpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_value: Option<FieldValueSpec>,
    /// Default 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecayFunction {
    Linear,
    Exp,
    Gauss,
}

/// A decay over the distance between a document's value and `origin`
/// (Elasticsearch `function_score` decay semantics): 1 within `offset`
/// of the origin, `decay` at `offset + scale`, falling towards 0 beyond.
///
/// The kind of distance follows `origin`:
/// * a JSON number: numeric (`Integer` / `Float` fields); `scale` and
///   `offset` are numbers;
/// * a string (`"now"`, an ISO-8601 date or date-time, or epoch millis as
///   a string): time (`Date` fields, or integers holding epoch millis);
///   `scale` / `offset` are durations such as `"30d"`, `"12h"` or a number
///   of milliseconds;
/// * a geo point (`{"lat": .., "lon": ..}`, `"lat,lon"` is not accepted
///   here because it would be ambiguous with a date): great-circle distance
///   (`GeoPoint` fields); `scale` / `offset` are distances such as `"5km"`
///   or a number of meters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecaySpec {
    pub field: String,
    pub function: DecayFunction,
    pub origin: serde_json::Value,
    pub scale: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<serde_json::Value>,
    /// The value at `offset + scale`; in `(0, 1)`. Default 0.5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decay: Option<f64>,
    /// The function's value for a document without the field. Default 0
    /// (no credit), which under `boost_mode: multiply` zeroes the hit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldValueModifier {
    None,
    Log,
    Log1p,
    Log2p,
    Ln,
    Ln1p,
    Ln2p,
    Square,
    Sqrt,
    Reciprocal,
}

/// `modifier(factor × value)` of a numeric field (Elasticsearch
/// `field_value_factor`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldValueSpec {
    pub field: String,
    /// Default 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor: Option<f64>,
    /// Default `none`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modifier: Option<FieldValueModifier>,
    /// The value used for a document without the field (before `factor`
    /// and `modifier`). Default 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScoreMode {
    #[default]
    Sum,
    Multiply,
    Avg,
    Max,
    Min,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BoostMode {
    /// `text_weight × text × F`.
    #[default]
    Multiply,
    /// `text_weight × text + F`.
    Sum,
    /// `F` alone (the text score only chose the window).
    Replace,
}

// ─── Compiled form ─────────────────────────────────────────────────────────

/// What a decay measures distance in.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Axis {
    Numeric(f64),
    /// Epoch milliseconds.
    Time(f64),
    Geo {
        lat: f64,
        lon: f64,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum Compiled {
    Decay {
        field: String,
        function: DecayFunction,
        axis: Axis,
        scale: f64,
        offset: f64,
        decay: f64,
        missing: f64,
    },
    FieldValue {
        field: String,
        factor: f64,
        modifier: FieldValueModifier,
        missing: f64,
    },
}

/// A validated `rank_by` with `"now"` resolved, ready to evaluate.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledRankBy {
    functions: Vec<(Compiled, f64)>,
    score_mode: ScoreMode,
    boost_mode: BoostMode,
    text_weight: f64,
    /// The rescore window for the request it was compiled for.
    pub window: usize,
}

fn invalid(msg: impl Into<String>) -> String {
    format!("rank_by: {}", msg.into())
}

fn finite(name: &str, v: f64) -> Result<f64, String> {
    if v.is_finite() {
        Ok(v)
    } else {
        Err(invalid(format!("{name} must be a finite number")))
    }
}

impl RankBySpec {
    /// The rescore window for a request returning `max_results` hits.
    pub fn window(&self, max_results: usize) -> usize {
        self.window_size
            .unwrap_or(DEFAULT_RANK_BY_WINDOW.max(max_results))
    }

    /// True when the expression depends on the time it runs at
    /// (`origin: "now"`): its responses must not be cached.
    pub fn is_time_dependent(&self) -> bool {
        self.functions.iter().any(|f| {
            f.decay
                .as_ref()
                .is_some_and(|d| d.origin.as_str().is_some_and(|s| s.trim() == "now"))
        })
    }

    /// Every field the expression reads.
    pub fn fields(&self) -> impl Iterator<Item = &str> {
        self.functions.iter().filter_map(|f| {
            f.decay
                .as_ref()
                .map(|d| d.field.as_str())
                .or(f.field_value.as_ref().map(|v| v.field.as_str()))
        })
    }

    /// Validate and resolve. `now_ms` is what `"now"` means (epoch ms).
    pub fn compile(&self, max_results: usize, now_ms: i64) -> Result<CompiledRankBy, String> {
        if self.functions.is_empty() {
            return Err(invalid("functions must not be empty"));
        }
        let window = self.window(max_results);
        if window == 0 || window > MAX_RANK_BY_WINDOW {
            return Err(invalid(format!(
                "window_size must be between 1 and {MAX_RANK_BY_WINDOW}, got {window}"
            )));
        }
        let text_weight = finite("text_weight", self.text_weight.unwrap_or(1.0))?;
        let mut functions = Vec::with_capacity(self.functions.len());
        for function in &self.functions {
            let weight = finite("weight", function.weight.unwrap_or(1.0))?;
            let compiled = match (&function.decay, &function.field_value) {
                (Some(decay), None) => compile_decay(decay, now_ms)?,
                (None, Some(value)) => compile_field_value(value)?,
                _ => {
                    return Err(invalid(
                        "each function needs exactly one of \"decay\" or \"field_value\"",
                    ))
                }
            };
            functions.push((compiled, weight));
        }
        Ok(CompiledRankBy {
            functions,
            score_mode: self.score_mode.unwrap_or_default(),
            boost_mode: self.boost_mode.unwrap_or_default(),
            text_weight,
            window,
        })
    }
}

fn check_field(field: &str) -> Result<(), String> {
    let f = field.trim();
    if f.is_empty() || f == "_id" || f == "_score" {
        return Err(invalid(format!(
            "field must name a stored Integer, Float, Date or GeoPoint field, not {field:?}"
        )));
    }
    Ok(())
}

fn compile_field_value(spec: &FieldValueSpec) -> Result<Compiled, String> {
    check_field(&spec.field)?;
    Ok(Compiled::FieldValue {
        field: spec.field.clone(),
        factor: finite("field_value.factor", spec.factor.unwrap_or(1.0))?,
        modifier: spec.modifier.unwrap_or(FieldValueModifier::None),
        missing: finite("field_value.missing", spec.missing.unwrap_or(0.0))?,
    })
}

fn compile_decay(spec: &DecaySpec, now_ms: i64) -> Result<Compiled, String> {
    check_field(&spec.field)?;
    let decay = spec.decay.unwrap_or(0.5);
    if !(decay.is_finite() && decay > 0.0 && decay < 1.0) {
        return Err(invalid(format!(
            "decay.decay must be strictly between 0 and 1, got {decay}"
        )));
    }
    let missing = finite("decay.missing", spec.missing.unwrap_or(0.0))?;
    let axis = match &spec.origin {
        serde_json::Value::Number(n) => Axis::Numeric(
            n.as_f64()
                .filter(|v| v.is_finite())
                .ok_or_else(|| invalid("decay.origin must be a finite number"))?,
        ),
        serde_json::Value::String(s) if s.trim() == "now" => Axis::Time(now_ms as f64),
        serde_json::Value::String(s) => Axis::Time(parse_date_millis(s).ok_or_else(|| {
            invalid(format!(
                "decay.origin {s:?} is not \"now\", an ISO-8601 date / date-time, or epoch millis"
            ))
        })? as f64),
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            let (lat, lon) = parse_geo_json(&spec.origin).ok_or_else(|| {
                invalid(
                    "decay.origin must be {\"lat\": .., \"lon\": ..} or [lon, lat] for a geo decay",
                )
            })?;
            Axis::Geo { lat, lon }
        }
        _ => {
            return Err(invalid(
                "decay.origin must be a number, a date, \"now\" or a geo point",
            ))
        }
    };
    let unit = |v: &serde_json::Value, what: &str| -> Result<f64, String> {
        let parsed = match axis {
            Axis::Numeric(_) => match v {
                serde_json::Value::Number(n) => n.as_f64(),
                serde_json::Value::String(s) => s.trim().parse::<f64>().ok(),
                _ => None,
            },
            Axis::Time(_) => parse_duration_millis(v),
            Axis::Geo { .. } => parse_distance_meters(v),
        };
        match parsed {
            Some(x) if x.is_finite() && x >= 0.0 => Ok(x),
            _ => Err(invalid(format!(
                "decay.{what} {v} is not a valid {} for this origin",
                match axis {
                    Axis::Numeric(_) => "number",
                    Axis::Time(_) => "duration (e.g. \"30d\", \"12h\", or milliseconds)",
                    Axis::Geo { .. } => "distance (e.g. \"5km\", \"300m\", or meters)",
                }
            ))),
        }
    };
    let scale = unit(&spec.scale, "scale")?;
    if scale <= 0.0 {
        return Err(invalid("decay.scale must be > 0"));
    }
    let offset = match &spec.offset {
        Some(v) => unit(v, "offset")?,
        None => 0.0,
    };
    Ok(Compiled::Decay {
        field: spec.field.clone(),
        function: spec.function,
        axis,
        scale,
        offset,
        decay,
        missing,
    })
}

/// The decay curve at `distance` (Elasticsearch formulas).
pub fn decay_value(
    function: DecayFunction,
    distance: f64,
    scale: f64,
    offset: f64,
    decay: f64,
) -> f64 {
    let d = (distance.abs() - offset).max(0.0);
    match function {
        DecayFunction::Gauss => {
            let sigma_sq = -(scale * scale) / (2.0 * decay.ln());
            (-(d * d) / (2.0 * sigma_sq)).exp()
        }
        DecayFunction::Exp => ((decay.ln() / scale) * d).exp(),
        DecayFunction::Linear => {
            let s = scale / (1.0 - decay);
            ((s - d) / s).max(0.0)
        }
    }
}

fn apply_modifier(modifier: FieldValueModifier, x: f64) -> f64 {
    match modifier {
        FieldValueModifier::None => x,
        FieldValueModifier::Log => x.log10(),
        FieldValueModifier::Log1p => (x + 1.0).log10(),
        FieldValueModifier::Log2p => (x + 2.0).log10(),
        FieldValueModifier::Ln => x.ln(),
        FieldValueModifier::Ln1p => x.ln_1p(),
        FieldValueModifier::Ln2p => (x + 2.0).ln(),
        FieldValueModifier::Square => x * x,
        FieldValueModifier::Sqrt => x.sqrt(),
        FieldValueModifier::Reciprocal => 1.0 / x,
    }
}

// ─── Evaluation over filter columns ────────────────────────────────────────

/// One field's filter column, located once per segment.
#[derive(Clone, Copy)]
enum Column<'a> {
    Str(&'a [(u32, String)]),
    Int(&'a [(u32, i64)]),
    Float(&'a [(u32, f64)]),
    Absent,
}

/// A column plus whether its entries are in `doc_seq` order (they are for
/// every segment the writer produces; an unordered column is scanned).
#[derive(Clone, Copy)]
struct ColumnView<'a> {
    column: Column<'a>,
    sorted: bool,
}

fn entries_for<T>(entries: &[(u32, T)], sorted: bool, doc_seq: u32) -> impl Iterator<Item = &T> {
    let (start, end) = if sorted {
        let start = entries.partition_point(|(d, _)| *d < doc_seq);
        let len = entries[start..].partition_point(|(d, _)| *d == doc_seq);
        (start, start + len)
    } else {
        (0, entries.len())
    };
    entries[start..end]
        .iter()
        .filter(move |(d, _)| *d == doc_seq)
        .map(|(_, v)| v)
}

impl<'a> ColumnView<'a> {
    fn new(store: &'a FilterStore, field: &str) -> Self {
        fn sorted<T>(e: &[(u32, T)]) -> bool {
            e.windows(2).all(|w| w[0].0 <= w[1].0)
        }
        if let Some(e) = lookup_field(&store.integer_fields, field) {
            return Self {
                column: Column::Int(e),
                sorted: sorted(e),
            };
        }
        if let Some(e) = lookup_field(&store.float_fields, field) {
            return Self {
                column: Column::Float(e),
                sorted: sorted(e),
            };
        }
        if let Some(e) = lookup_field(&store.string_fields, field) {
            return Self {
                column: Column::Str(e),
                sorted: sorted(e),
            };
        }
        Self {
            column: Column::Absent,
            sorted: true,
        }
    }

    /// Every value `doc_seq` has, as the axis wants it. A string value
    /// holding a JSON list contributes each element.
    fn values(&self, doc_seq: u32, axis: Option<Axis>, out: &mut Vec<Value>) {
        out.clear();
        let read_str = |s: &str, out: &mut Vec<Value>| {
            let mut one = |s: &str| {
                if let Some(v) = parse_str_value(s, axis) {
                    out.push(v);
                }
            };
            // A list value is a multi-valued field. Geo points are stored
            // one entry per point as "lat,lon", and `parse_geo_str` reads
            // the JSON forms itself, so lists are only split off the geo axis.
            if s.starts_with('[') && !matches!(axis, Some(Axis::Geo { .. })) {
                if let Some(elements) = list_elements(s) {
                    elements.iter().for_each(|e| one(e));
                    return;
                }
            }
            one(s);
        };
        match self.column {
            Column::Int(e) => {
                out.extend(entries_for(e, self.sorted, doc_seq).map(|v| Value::Num(*v as f64)))
            }
            Column::Float(e) => {
                out.extend(entries_for(e, self.sorted, doc_seq).map(|v| Value::Num(*v)))
            }
            Column::Str(e) => {
                for v in entries_for(e, self.sorted, doc_seq) {
                    read_str(v, out);
                }
            }
            Column::Absent => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Value {
    Num(f64),
    Geo(f64, f64),
}

fn parse_str_value(s: &str, axis: Option<Axis>) -> Option<Value> {
    match axis {
        Some(Axis::Geo { .. }) => parse_geo_str(s).map(|(lat, lon)| Value::Geo(lat, lon)),
        Some(Axis::Time(_)) => parse_date_millis(s).map(|ms| Value::Num(ms as f64)),
        _ => s
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite())
            .map(Value::Num),
    }
}

/// Evaluates a [`CompiledRankBy`] for the documents of one segment.
pub struct SegmentRanker<'a> {
    rank_by: &'a CompiledRankBy,
    columns: Vec<ColumnView<'a>>,
    scratch: Vec<Value>,
}

impl CompiledRankBy {
    /// Bind to one segment's filter columns. Each column is located (and its
    /// order checked) once here, not per document.
    pub fn for_segment<'a>(&'a self, store: &'a FilterStore) -> SegmentRanker<'a> {
        let columns = self
            .functions
            .iter()
            .map(|(f, _)| match f {
                Compiled::Decay { field, .. } | Compiled::FieldValue { field, .. } => {
                    ColumnView::new(store, field)
                }
            })
            .collect();
        SegmentRanker {
            rank_by: self,
            columns,
            scratch: Vec::new(),
        }
    }

    /// Combine a base score with the functions' values.
    pub fn combine(&self, base: f64, values: &[f64]) -> f64 {
        let weighted = self.functions.iter().zip(values).map(|((_, w), v)| w * v);
        let combined = match self.score_mode {
            ScoreMode::Sum => weighted.sum(),
            ScoreMode::Multiply => weighted.product(),
            ScoreMode::Max => weighted.fold(f64::NEG_INFINITY, f64::max),
            ScoreMode::Min => weighted.fold(f64::INFINITY, f64::min),
            ScoreMode::Avg => {
                let total_weight: f64 = self.functions.iter().map(|(_, w)| w).sum();
                let sum: f64 = weighted.sum();
                if total_weight == 0.0 {
                    0.0
                } else {
                    sum / total_weight
                }
            }
        };
        let base = self.text_weight * base;
        let score = match self.boost_mode {
            BoostMode::Multiply => base * combined,
            BoostMode::Sum => base + combined,
            BoostMode::Replace => combined,
        };
        if score.is_finite() {
            score
        } else {
            0.0
        }
    }
}

impl SegmentRanker<'_> {
    /// The modified score of `doc_seq` whose unmodified score is `base`.
    pub fn score(&mut self, doc_seq: u32, base: f64) -> f64 {
        let mut values = Vec::with_capacity(self.rank_by.functions.len());
        for (i, (function, _)) in self.rank_by.functions.iter().enumerate() {
            let column = self.columns[i];
            let value = match function {
                Compiled::Decay {
                    function,
                    axis,
                    scale,
                    offset,
                    decay,
                    missing,
                    ..
                } => {
                    column.values(doc_seq, Some(*axis), &mut self.scratch);
                    // A multi-valued field scores by its closest value.
                    self.scratch
                        .iter()
                        .filter_map(|v| {
                            let distance = match (*axis, *v) {
                                (Axis::Numeric(o), Value::Num(x))
                                | (Axis::Time(o), Value::Num(x)) => x - o,
                                (Axis::Geo { lat, lon }, Value::Geo(a, b)) => {
                                    haversine_meters(lat, lon, a, b)
                                }
                                _ => return None,
                            };
                            Some(decay_value(*function, distance, *scale, *offset, *decay))
                        })
                        .filter(|v| v.is_finite())
                        .fold(None, |best: Option<f64>, v| {
                            Some(best.map_or(v, |b| b.max(v)))
                        })
                        .unwrap_or(*missing)
                }
                Compiled::FieldValue {
                    factor,
                    modifier,
                    missing,
                    ..
                } => {
                    column.values(doc_seq, None, &mut self.scratch);
                    let raw: Vec<f64> = self
                        .scratch
                        .iter()
                        .filter_map(|v| match v {
                            Value::Num(x) => Some(*x),
                            Value::Geo(..) => None,
                        })
                        .collect();
                    let compute = |x: f64| apply_modifier(*modifier, factor * x);
                    // A multi-valued field scores by its best value.
                    raw.into_iter()
                        .map(compute)
                        .filter(|v| v.is_finite())
                        .fold(None, |best: Option<f64>, v| {
                            Some(best.map_or(v, |b| b.max(v)))
                        })
                        .unwrap_or_else(|| {
                            let v = compute(*missing);
                            if v.is_finite() {
                                v
                            } else {
                                0.0
                            }
                        })
                }
            };
            values.push(value);
        }
        self.rank_by.combine(base, &values)
    }
}

// ─── Value parsers ─────────────────────────────────────────────────────────

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's
/// `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn digits(s: &str, n: usize) -> Option<(i64, &str)> {
    if s.len() < n || !s.as_bytes()[..n].iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some((s[..n].parse().ok()?, &s[n..]))
}

/// Epoch milliseconds of an ISO-8601 / RFC 3339 date or date-time
/// (`2026-11-19`, `2026-11-19T08:30:00Z`, `2026-11-19 08:30:00.250+05:30`;
/// no zone means UTC), or of a plain integer taken as epoch milliseconds.
pub fn parse_date_millis(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(ms) = s.parse::<i64>() {
        // A bare year ("2026") is also all digits; four digits or fewer is
        // read as a year rather than a few milliseconds after the epoch.
        if s.trim_start_matches('-').len() > 4 {
            return Some(ms);
        }
        return Some(days_from_civil(ms, 1, 1) * 86_400_000);
    }
    let (year, rest) = digits(s, 4)?;
    let rest = rest.strip_prefix('-')?;
    let (month, rest) = digits(rest, 2)?;
    let (day, rest) = match rest.strip_prefix('-') {
        Some(r) => digits(r, 2)?,
        None if rest.is_empty() => (1, rest),
        None => return None,
    };
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let mut ms = days_from_civil(year, month, day) * 86_400_000;
    let rest = match rest.chars().next() {
        None => return Some(ms),
        Some('T' | 't' | ' ') => &rest[1..],
        Some(_) => return None,
    };
    let (hour, rest) = digits(rest, 2)?;
    let rest = rest.strip_prefix(':')?;
    let (minute, mut rest) = digits(rest, 2)?;
    let mut second = 0;
    let mut frac_ms = 0i64;
    if let Some(r) = rest.strip_prefix(':') {
        let (sec, r) = digits(r, 2)?;
        second = sec;
        rest = r;
        if let Some(r) = rest.strip_prefix(['.', ',']) {
            let n = r.bytes().take_while(u8::is_ascii_digit).count();
            if n == 0 {
                return None;
            }
            let frac = &r[..n];
            let padded: String = frac.chars().chain("000".chars()).take(3).collect();
            frac_ms = padded.parse().ok()?;
            rest = &r[n..];
        }
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    ms += ((hour * 60 + minute) * 60 + second) * 1000 + frac_ms;
    match rest {
        "" | "Z" | "z" => Some(ms),
        zone => {
            let sign = match zone.as_bytes()[0] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            let zone = &zone[1..];
            let (zh, zone) = digits(zone, 2)?;
            let zone = zone.strip_prefix(':').unwrap_or(zone);
            let (zm, zone) = if zone.is_empty() {
                (0, zone)
            } else {
                digits(zone, 2)?
            };
            if !zone.is_empty() {
                return None;
            }
            Some(ms - sign * (zh * 60 + zm) * 60_000)
        }
    }
}

fn split_unit(s: &str) -> Option<(f64, &str)> {
    let s = s.trim();
    let end = s
        .find(|c: char| {
            !(c.is_ascii_digit() || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E')
        })
        .unwrap_or(s.len());
    // `e` doubles as an exponent marker; no unit here starts with one.
    let number: f64 = s[..end].trim().parse().ok()?;
    Some((number, s[end..].trim()))
}

/// Milliseconds in a duration: a JSON number (milliseconds) or a string
/// with a unit (`ms`, `s`, `m`, `h`, `d`, `w`).
pub fn parse_duration_millis(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => {
            let (n, unit) = split_unit(s)?;
            let per = match unit {
                "" | "ms" => 1.0,
                "s" => 1_000.0,
                "m" => 60_000.0,
                "h" => 3_600_000.0,
                "d" => 86_400_000.0,
                "w" => 7.0 * 86_400_000.0,
                _ => return None,
            };
            Some(n * per)
        }
        _ => None,
    }
}

/// Meters in a distance: a JSON number (meters) or a string with a unit
/// (`m`, `km`, `cm`, `mi`, `yd`, `ft`, `in`, `nmi`).
pub fn parse_distance_meters(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => {
            let (n, unit) = split_unit(s)?;
            let per = match unit {
                "" | "m" | "meters" => 1.0,
                "km" | "kilometers" => 1_000.0,
                "cm" => 0.01,
                "mm" => 0.001,
                "mi" | "miles" => 1_609.344,
                "yd" => 0.9144,
                "ft" => 0.3048,
                "in" => 0.0254,
                "nmi" | "NM" => 1_852.0,
                _ => return None,
            };
            Some(n * per)
        }
        _ => None,
    }
}

fn valid_geo(lat: f64, lon: f64) -> Option<(f64, f64)> {
    (lat.is_finite()
        && lon.is_finite()
        && (-90.0..=90.0).contains(&lat)
        && (-180.0..=180.0).contains(&lon))
    .then_some((lat, lon))
}

/// A geo point as JSON: `{"lat": .., "lon": ..}` or GeoJSON `[lon, lat]`.
pub fn parse_geo_json(v: &serde_json::Value) -> Option<(f64, f64)> {
    match v {
        serde_json::Value::Object(o) => {
            let num = |k: &str| match o.get(k)? {
                serde_json::Value::Number(n) => n.as_f64(),
                serde_json::Value::String(s) => s.trim().parse().ok(),
                _ => None,
            };
            if o.len() != 2 {
                return None;
            }
            valid_geo(num("lat")?, num("lon")?)
        }
        serde_json::Value::Array(a) if a.len() == 2 => valid_geo(a[1].as_f64()?, a[0].as_f64()?),
        _ => None,
    }
}

/// A geo point as stored text: `"lat,lon"`, or the JSON forms of
/// [`parse_geo_json`].
pub fn parse_geo_str(s: &str) -> Option<(f64, f64)> {
    let s = s.trim();
    if s.starts_with('{') || s.starts_with('[') {
        return parse_geo_json(&serde_json::from_str(s).ok()?);
    }
    let (lat, lon) = s.split_once(',')?;
    valid_geo(lat.trim().parse().ok()?, lon.trim().parse().ok()?)
}

/// The canonical filter-column text of a geo point: `"lat,lon"`.
pub fn canonical_geo(lat: f64, lon: f64) -> String {
    format!("{lat},{lon}")
}

/// Great-circle distance in meters (haversine).
pub fn haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dp = (lat2 - lat1).to_radians();
    let dl = (lon2 - lon1).to_radians();
    let a = (dp / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().min(1.0).asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: f64 = 86_400_000.0;

    #[test]
    fn dates_parse_to_epoch_millis() {
        assert_eq!(parse_date_millis("1970-01-01"), Some(0));
        assert_eq!(parse_date_millis("1970-01-02"), Some(86_400_000));
        assert_eq!(
            parse_date_millis("2000-03-01T00:00:00Z"),
            Some(951_868_800_000)
        );
        assert_eq!(
            parse_date_millis("2026-11-19T10:00:00+02:00"),
            parse_date_millis("2026-11-19T08:00:00Z")
        );
        assert_eq!(
            parse_date_millis("2026-11-19 08:00:00.5"),
            Some(parse_date_millis("2026-11-19T08:00:00Z").unwrap() + 500)
        );
        assert_eq!(parse_date_millis("1763540000000"), Some(1_763_540_000_000));
        assert_eq!(parse_date_millis("2026"), parse_date_millis("2026-01-01"));
        assert_eq!(
            parse_date_millis("2026-11"),
            parse_date_millis("2026-11-01")
        );
        assert_eq!(parse_date_millis("1969-12-31"), Some(-86_400_000));
        for bad in [
            "",
            "yesterday",
            "2026-13-01",
            "2026-11-19T25:00",
            "2026/11/19",
            "2026-11-19Tx",
        ] {
            assert_eq!(parse_date_millis(bad), None, "{bad}");
        }
    }

    #[test]
    fn units() {
        use serde_json::json;
        assert_eq!(parse_duration_millis(&json!("30d")), Some(30.0 * DAY));
        assert_eq!(parse_duration_millis(&json!("1.5h")), Some(5_400_000.0));
        assert_eq!(parse_duration_millis(&json!(250)), Some(250.0));
        assert_eq!(parse_duration_millis(&json!("3 fortnights")), None);
        assert_eq!(parse_distance_meters(&json!("5km")), Some(5000.0));
        assert_eq!(parse_distance_meters(&json!("2mi")), Some(3218.688));
        assert_eq!(parse_distance_meters(&json!(12.5)), Some(12.5));
        assert_eq!(parse_distance_meters(&json!("10 parsecs")), None);
    }

    #[test]
    fn geo_points_and_distances() {
        assert_eq!(parse_geo_str("40.7128,-74.006"), Some((40.7128, -74.006)));
        assert_eq!(parse_geo_str(r#"{"lat": 1.5, "lon": 2}"#), Some((1.5, 2.0)));
        assert_eq!(
            parse_geo_str("[2, 1.5]"),
            Some((1.5, 2.0)),
            "GeoJSON is [lon, lat]"
        );
        assert_eq!(parse_geo_str("91,0"), None);
        assert_eq!(parse_geo_str("0,181"), None);
        assert_eq!(parse_geo_str("nowhere"), None);
        // New York to London, ~5570 km.
        let d = haversine_meters(40.7128, -74.006, 51.5074, -0.1278);
        assert!((d - 5_570_000.0).abs() < 10_000.0, "{d}");
        assert_eq!(haversine_meters(10.0, 10.0, 10.0, 10.0), 0.0);
        // One degree of latitude is ~111.2 km.
        let d = haversine_meters(0.0, 0.0, 1.0, 0.0);
        assert!((d - 111_195.0).abs() < 10.0, "{d}");
    }

    #[test]
    fn decay_curves_match_their_definition() {
        for f in [
            DecayFunction::Linear,
            DecayFunction::Exp,
            DecayFunction::Gauss,
        ] {
            assert_eq!(decay_value(f, 0.0, 10.0, 0.0, 0.5), 1.0);
            // Within the offset: no decay.
            assert_eq!(decay_value(f, 4.0, 10.0, 5.0, 0.5), 1.0);
            // At offset + scale the value is `decay`, on either side.
            assert!(
                (decay_value(f, 15.0, 10.0, 5.0, 0.5) - 0.5).abs() < 1e-12,
                "{f:?}"
            );
            assert!(
                (decay_value(f, -15.0, 10.0, 5.0, 0.3) - 0.3).abs() < 1e-12,
                "{f:?}"
            );
            // Monotone non-increasing in distance.
            let mut prev = 1.0;
            for d in 0..100 {
                let v = decay_value(f, d as f64, 10.0, 0.0, 0.5);
                assert!(v <= prev && v >= 0.0);
                prev = v;
            }
        }
        // Linear reaches exactly 0 at scale / (1 - decay).
        assert_eq!(
            decay_value(DecayFunction::Linear, 20.0, 10.0, 0.0, 0.5),
            0.0
        );
        assert_eq!(
            decay_value(DecayFunction::Linear, 50.0, 10.0, 0.0, 0.5),
            0.0
        );
    }

    fn spec(json: serde_json::Value) -> RankBySpec {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn compile_validates() {
        use serde_json::json;
        let bad = [
            json!({"functions": []}),
            json!({"functions": [{}]}),
            json!({"functions": [{"decay": {"field": "d", "function": "exp", "origin": "now", "scale": "1d"},
                                  "field_value": {"field": "n"}}]}),
            json!({"functions": [{"decay": {"field": "d", "function": "exp", "origin": "now", "scale": "0d"}}]}),
            json!({"functions": [{"decay": {"field": "d", "function": "exp", "origin": "now", "scale": "1d", "decay": 1.0}}]}),
            json!({"functions": [{"decay": {"field": "d", "function": "exp", "origin": "now", "scale": "1d", "decay": 0}}]}),
            json!({"functions": [{"decay": {"field": "d", "function": "exp", "origin": "last tuesday", "scale": "1d"}}]}),
            json!({"functions": [{"decay": {"field": "d", "function": "exp", "origin": "now", "scale": "5km"}}]}),
            json!({"functions": [{"decay": {"field": "g", "function": "gauss", "origin": {"lat": 1, "lon": 2}, "scale": "1d"}}]}),
            json!({"functions": [{"decay": {"field": "g", "function": "gauss", "origin": {"lat": 100, "lon": 2}, "scale": "1km"}}]}),
            json!({"functions": [{"decay": {"field": "n", "function": "linear", "origin": 5, "scale": "ten"}}]}),
            json!({"functions": [{"decay": {"field": "n", "function": "linear", "origin": 5, "scale": 1, "offset": -1}}]}),
            json!({"functions": [{"field_value": {"field": "_score"}}]}),
            json!({"functions": [{"field_value": {"field": "n"}}], "window_size": 0}),
            json!({"functions": [{"field_value": {"field": "n"}}], "window_size": 10001}),
        ];
        for b in bad {
            assert!(spec(b.clone()).compile(10, 0).is_err(), "{b}");
        }
        // Unknown keys are rejected at parse time.
        for b in [
            json!({"functions": [], "bogus": 1}),
            json!({"functions": [{"decay": {"field": "d", "function": "cubic", "origin": 1, "scale": 1}}]}),
            json!({"functions": [{"field_value": {"field": "n", "modifier": "cube"}}]}),
        ] {
            assert!(
                serde_json::from_value::<RankBySpec>(b.clone()).is_err(),
                "{b}"
            );
        }
        let ok = spec(json!({"functions": [
            {"decay": {"field": "d", "function": "exp", "origin": "now", "scale": "30d"}},
            {"decay": {"field": "g", "function": "gauss", "origin": [2, 1], "scale": "1km", "offset": 100}},
            {"field_value": {"field": "n", "modifier": "log1p"}, "weight": 0.5}
        ]}));
        let compiled = ok.compile(10, 1_000).unwrap();
        assert_eq!(compiled.window, DEFAULT_RANK_BY_WINDOW);
        assert_eq!(
            ok.compile(500, 0).unwrap().window,
            500,
            "never below max_results"
        );
        assert!(ok.is_time_dependent());
        assert_eq!(ok.fields().collect::<Vec<_>>(), ["d", "g", "n"]);
    }

    fn store() -> FilterStore {
        let mut s = FilterStore::default();
        s.string_fields.insert(
            "published".into(),
            vec![
                (0, "2026-11-18".into()),
                (1, "2025-11-19".into()),
                (3, r#"["2020-01-01", "2026-11-19"]"#.into()),
            ],
        );
        s.string_fields
            .insert("loc".into(), vec![(0, "0,0".into()), (2, "0,1".into())]);
        s.integer_fields
            .insert("votes".into(), vec![(0, 9), (1, 99)]);
        s.float_fields.insert("rating".into(), vec![(2, 4.5)]);
        s
    }

    #[test]
    fn evaluates_from_filter_columns() {
        use serde_json::json;
        let store = store();
        let now = parse_date_millis("2026-11-19").unwrap();
        let recency = spec(json!({"functions": [
            {"decay": {"field": "published", "function": "exp", "origin": "now", "scale": "1d", "decay": 0.5}}
        ]}))
        .compile(10, now)
        .unwrap();
        let mut ranker = recency.for_segment(&store);
        assert!(
            (ranker.score(0, 10.0) - 5.0).abs() < 1e-9,
            "one day old halves"
        );
        assert!(ranker.score(1, 10.0) < 1e-6, "a year old decays to ~0");
        assert_eq!(ranker.score(2, 10.0), 0.0, "missing scores 0 by default");
        assert!(
            (ranker.score(3, 10.0) - 10.0).abs() < 1e-9,
            "closest of a multi-value"
        );

        let votes = spec(json!({
            "functions": [{"field_value": {"field": "votes", "modifier": "log1p", "missing": 0}}],
            "boost_mode": "sum", "text_weight": 2
        }))
        .compile(10, 0)
        .unwrap();
        let mut ranker = votes.for_segment(&store);
        assert!((ranker.score(0, 1.0) - (2.0 + 1.0)).abs() < 1e-9);
        assert!((ranker.score(1, 1.0) - (2.0 + 2.0)).abs() < 1e-9);
        assert!((ranker.score(2, 1.0) - 2.0).abs() < 1e-9);

        let geo = spec(json!({
            "functions": [{"decay": {"field": "loc", "function": "linear", "origin": {"lat": 0, "lon": 0}, "scale": "111.195km", "decay": 0.5}}],
            "boost_mode": "replace"
        }))
        .compile(10, 0)
        .unwrap();
        let mut ranker = geo.for_segment(&store);
        assert_eq!(ranker.score(0, 3.0), 1.0);
        assert!(
            (ranker.score(2, 3.0) - 0.5).abs() < 1e-3,
            "{}",
            ranker.score(2, 3.0)
        );

        let float_and_mode = spec(json!({
            "functions": [
                {"field_value": {"field": "rating", "missing": 1}, "weight": 2},
                {"field_value": {"field": "votes", "missing": 1}}
            ],
            "score_mode": "multiply"
        }))
        .compile(10, 0)
        .unwrap();
        let mut ranker = float_and_mode.for_segment(&store);
        assert!((ranker.score(2, 1.0) - 9.0).abs() < 1e-9, "2 * 4.5 * 1");
        assert!((ranker.score(0, 1.0) - 18.0).abs() < 1e-9, "2 * 1 * 9");

        // An absent field everywhere is `missing`, not an error.
        let absent = spec(json!({"functions": [{"field_value": {"field": "nope", "missing": 3}}]}))
            .compile(10, 0)
            .unwrap();
        assert_eq!(absent.for_segment(&store).score(0, 2.0), 6.0);
    }

    #[test]
    fn geo_distance_clause_round_trips_and_evaluates() {
        use crate::{evaluate_filter, FilterClause};
        let clause: FilterClause = serde_json::from_str(
            r#"{"geo_distance": {"distance": "120km", "distance_type": "arc", "loc": [0, 0]}}"#,
        )
        .unwrap();
        // Serialized (filter-set cache keys, result-cache keys) in meters,
        // and parses back to the same clause.
        let json = serde_json::to_string(&clause).unwrap();
        assert!(json.contains("120000"), "{json}");
        let again: FilterClause = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&again).unwrap(), json);
        // doc 0 at the origin, doc 2 ~111 km away; doc 1 has no point.
        let set = evaluate_filter(&clause, &store(), 4);
        assert_eq!(set.iter().collect::<Vec<_>>(), [0, 2]);
        let tight: FilterClause =
            serde_json::from_str(r#"{"geo_distance": {"distance": 100000, "loc": "0,0"}}"#)
                .unwrap();
        assert_eq!(
            evaluate_filter(&tight, &store(), 4)
                .iter()
                .collect::<Vec<_>>(),
            [0]
        );
        // A non-geo column never matches.
        let wrong: FilterClause =
            serde_json::from_str(r#"{"geo_distance": {"distance": "1000km", "published": "0,0"}}"#)
                .unwrap();
        assert!(evaluate_filter(&wrong, &store(), 4).is_empty());
    }

    #[test]
    fn unordered_columns_are_scanned() {
        let mut s = FilterStore::default();
        s.integer_fields
            .insert("n".into(), vec![(5, 50), (1, 10), (3, 30)]);
        let c = spec(serde_json::json!({
            "functions": [{"field_value": {"field": "n"}}], "boost_mode": "replace"
        }))
        .compile(10, 0)
        .unwrap();
        let mut r = c.for_segment(&s);
        assert_eq!(r.score(1, 0.0), 10.0);
        assert_eq!(r.score(3, 0.0), 30.0);
        assert_eq!(r.score(5, 0.0), 50.0);
    }
}
