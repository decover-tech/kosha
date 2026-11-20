//! `histogram` and `date_histogram` aggregation bodies (issues #219, #300,
//! #301, #302).
//!
//! Every bucket is identified internally by an `i64` index, so partial
//! results merge by plain integer equality across segments and the gaps
//! between the lowest and the highest bucket are filled by counting
//! indices. What differs between the kinds of interval is only the mapping
//! between a value and its index, and from an index back to the bucket key:
//!
//! - `histogram` and a `date_histogram` `fixed_interval` (or a `1m` / `1h`
//!   calendar interval, or `1d` / `1w` without a DST-observing time zone):
//!   index `floor((v - offset) / interval)`, key
//!   `index * interval + offset` — the formula Elasticsearch uses.
//! - `calendar_interval` `1M`, `1q`, `1y` (and `1d` / `1w` in a time zone
//!   with daylight saving): the index is the civil month, quarter, year,
//!   day or Monday-started week of the value's wall-clock date in the
//!   bucket time zone, counted from 1970; the key is the UTC instant at
//!   which that civil period starts in the zone. Consecutive indices are
//!   consecutive calendar periods, so gap filling steps by calendar unit,
//!   and a `1d` bucket on a daylight-saving day is 23 or 25 hours long.
//! - A sub-day interval in a time zone with daylight saving rounds as
//!   Elasticsearch does: the bucket of `v` starts at the latest instant
//!   `<= v` whose wall-clock time is a multiple of the interval. Across a
//!   fall-back transition the repeated wall-clock hour is two separate
//!   buckets (`01:00-04:00` and `01:00-05:00` in New York), and across a
//!   spring-forward one the skipped hour has no bucket. The index is
//!   `2 * floor(local / interval) + occurrence`, where `occurrence` is 1
//!   only for the second instant of a repeated wall-clock time; an index
//!   whose wall-clock start never happens (or happens only once) has no
//!   bucket and is skipped when gaps are filled.
//!
//! `offset` composes with `time_zone` as in Elasticsearch: the value is
//! shifted by `-offset` in UTC, rounded in the zone, and the key shifted
//! back by `+offset`.
//!
//! `extended_bounds` widens the gap-filled range (with `min_doc_count: 0`)
//! to the buckets holding `min` and `max`; `hard_bounds` drops values
//! outside `[min, max]` and clips the gap-filled range to it. Date bounds
//! are date strings (a string without its own UTC offset is wall-clock
//! time in `time_zone`) or epoch milliseconds.
//!
//! `keyed`, `order`, `missing` and `format` are not supported.

use serde::{Deserialize, Serialize};

use crate::datetime;

/// Most buckets one `histogram` / `date_histogram` returns, gap-filled
/// buckets included. A request past it is a 400 rather than an unbounded
/// response (Elasticsearch's equivalent is `search.max_buckets`).
pub const MAX_HISTOGRAM_BUCKETS: u64 = 10_000;

const MS_PER_MINUTE: i64 = 60_000;
const MS_PER_HOUR: i64 = 3_600_000;
const MS_PER_DAY: i64 = 86_400_000;
const MS_PER_WEEK: i64 = 7 * MS_PER_DAY;
/// 1970-01-01 was a Thursday; ISO weeks (and Elasticsearch's `1w`
/// calendar interval) start on Monday, 1970-01-05.
const MONDAY_OFFSET_MS: i64 = 4 * MS_PER_DAY;
/// Upper bound on any interval or offset, about 10,000 years: keeps every
/// bucket computation inside `i64` without checked arithmetic.
const MAX_DURATION_MS: i64 = 10_000 * 366 * MS_PER_DAY;
/// Largest UTC offset a fixed `time_zone` may name (as in `java.time`).
const MAX_ZONE_OFFSET_MS: i64 = 18 * MS_PER_HOUR;
/// Civil years the calendar arithmetic is clamped to; far beyond any
/// epoch-millisecond `i64` (about ±292 million years).
const MAX_CIVIL_YEAR: i64 = 1_000_000_000;

/// `extended_bounds` / `hard_bounds` of a `histogram`: either end may be
/// left out.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct HistogramBounds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,
}

impl HistogramBounds {
    fn check(&self, option: &str) -> Result<(), String> {
        for v in [self.min, self.max].into_iter().flatten() {
            if !v.is_finite() {
                return Err(format!("`histogram` `{option}` must be finite numbers"));
            }
        }
        if let (Some(min), Some(max)) = (self.min, self.max) {
            if min > max {
                return Err(format!(
                    "`histogram` `{option}` `min` ({min}) is greater than `max` ({max})"
                ));
            }
        }
        Ok(())
    }
}

/// Body of a `histogram` aggregation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AggHistogram {
    pub field: String,
    /// Bucket width; must be a finite number greater than 0.
    pub interval: f64,
    /// Shifts the bucket boundaries: buckets start at `offset + k * interval`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub offset: f64,
    /// Buckets with fewer documents are left out. The default, 0, also
    /// fills the gaps between the lowest and the highest non-empty bucket
    /// with zero-count buckets, as in Elasticsearch.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub min_doc_count: u64,
    /// With `min_doc_count: 0`, also fill empty buckets out to the ones
    /// holding `min` and `max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extended_bounds: Option<HistogramBounds>,
    /// Values outside `[min, max]` are dropped; no bucket is returned
    /// (or gap-filled) beyond the ones holding `min` and `max`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_bounds: Option<HistogramBounds>,
}

fn is_zero(v: &f64) -> bool {
    *v == 0.0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

impl AggHistogram {
    pub fn validate(&self) -> Result<(), String> {
        self.plan().map(|_| ())
    }

    /// The bucketing this body describes, or a message for the 400.
    pub fn plan(&self) -> Result<NumericHistogram, String> {
        if !self.interval.is_finite() || self.interval <= 0.0 {
            return Err(format!(
                "`histogram` `interval` must be a number greater than 0, got {}",
                self.interval
            ));
        }
        if !self.offset.is_finite() {
            return Err("`histogram` `offset` must be a finite number".into());
        }
        let extended = self.extended_bounds.unwrap_or_default();
        extended.check("extended_bounds")?;
        let hard = self.hard_bounds.unwrap_or_default();
        hard.check("hard_bounds")?;
        Ok(NumericHistogram {
            interval: self.interval,
            offset: self.offset,
            extended,
            hard,
        })
    }
}

/// How a `histogram` maps numbers to bucket indices and back.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NumericHistogram {
    pub interval: f64,
    pub offset: f64,
    extended: HistogramBounds,
    hard: HistogramBounds,
}

impl NumericHistogram {
    /// The index of the bucket `value` falls in, `None` when `hard_bounds`
    /// excludes it.
    pub fn bucket(&self, value: f64) -> Option<i64> {
        let below = self.hard.min.is_some_and(|min| value < min);
        let above = self.hard.max.is_some_and(|max| value > max);
        (!below && !above).then(|| histogram_bucket(value, self.interval, self.offset))
    }

    /// The key of bucket `index`.
    pub fn key(&self, index: i64) -> f64 {
        histogram_key(index, self.interval, self.offset)
    }

    /// The indices to gap-fill (with `min_doc_count: 0`): the observed
    /// range widened by `extended_bounds` and clipped by `hard_bounds`.
    pub fn fill_range(&self, observed: Option<(i64, i64)>) -> Option<(i64, i64)> {
        let index = |v: f64| histogram_bucket(v, self.interval, self.offset);
        fill_range(
            observed,
            [self.extended.min.map(index), self.extended.max.map(index)],
            [self.hard.min.map(index), self.hard.max.map(index)],
        )
    }
}

/// `observed` widened to `extended` and clipped to `hard` (each a
/// `[min, max]` pair of bucket indices, either end optional).
fn fill_range(
    observed: Option<(i64, i64)>,
    extended: [Option<i64>; 2],
    hard: [Option<i64>; 2],
) -> Option<(i64, i64)> {
    let ends = || {
        observed
            .into_iter()
            .flat_map(|(lo, hi)| [lo, hi])
            .chain(extended.into_iter().flatten())
    };
    let mut lo = ends().min()?;
    let mut hi = ends().max()?;
    if let Some(min) = hard[0] {
        lo = lo.max(min);
    }
    if let Some(max) = hard[1] {
        hi = hi.min(max);
    }
    (lo <= hi).then_some((lo, hi))
}

/// A duration as Elasticsearch writes it in `offset`: a string such as
/// `"6h"` / `"-30m"`, or a number of milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DurationValue {
    Millis(i64),
    Text(String),
}

/// `extended_bounds` / `hard_bounds` of a `date_histogram`: each end a
/// date string or epoch milliseconds, either may be left out.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct DateHistogramBounds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<serde_json::Value>,
}

/// Body of a `date_histogram` aggregation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AggDateHistogram {
    pub field: String,
    /// Fixed-length interval: a positive whole number and a unit, one of
    /// `ms`, `s`, `m`, `h`, `d` (`"30s"`, `"5m"`, `"1h"`, `"7d"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixed_interval: Option<String>,
    /// Calendar interval, a single unit: `1m` / `minute`, `1h` / `hour`,
    /// `1d` / `day`, `1w` / `week` (weeks start on Monday), `1M` / `month`,
    /// `1q` / `quarter`, `1y` / `year`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calendar_interval: Option<String>,
    /// Shifts the bucket boundaries, e.g. `"+6h"` for days that start at
    /// 06:00. A duration string (sign optional) or milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<DurationValue>,
    /// Zone the buckets are rounded in: an IANA name (`"Europe/Paris"`),
    /// a UTC offset (`"+05:30"`, `"-08:00"`), or `"UTC"` / `"Z"`. Default
    /// UTC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
    /// See [`AggHistogram::min_doc_count`].
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub min_doc_count: u64,
    /// See [`AggHistogram::extended_bounds`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extended_bounds: Option<DateHistogramBounds>,
    /// See [`AggHistogram::hard_bounds`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_bounds: Option<DateHistogramBounds>,
}

/// A `date_histogram` interval, after parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Interval {
    /// `width` milliseconds, buckets starting at `base + k * width` on the
    /// wall clock.
    Fixed {
        width: i64,
        base: i64,
    },
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl AggDateHistogram {
    pub fn validate(&self) -> Result<(), String> {
        self.plan().map(|_| ())
    }

    /// The bucketing this body describes, or a message for the 400.
    pub fn plan(&self) -> Result<DateHistogram, String> {
        let interval =
            match (&self.fixed_interval, &self.calendar_interval) {
                (Some(_), Some(_)) => return Err(
                    "`date_histogram` takes one of `fixed_interval` or `calendar_interval`, not \
                     both"
                        .into(),
                ),
                (None, None) => return Err(
                    "`date_histogram` needs `fixed_interval` (e.g. \"1h\", \"30s\", \"7d\") or \
                     `calendar_interval` (e.g. \"1d\", \"1M\")"
                        .into(),
                ),
                (Some(fixed), None) => Interval::Fixed {
                    width: parse_fixed_interval(fixed)?,
                    base: 0,
                },
                (None, Some(calendar)) => parse_calendar_interval(calendar)?,
            };
        let offset = match &self.offset {
            None => 0,
            Some(DurationValue::Millis(ms)) => *ms,
            Some(DurationValue::Text(text)) => parse_offset(text)?,
        };
        if offset.abs() > MAX_DURATION_MS {
            return Err(format!("`date_histogram` `offset` {offset}ms is too large"));
        }
        let zone = match &self.time_zone {
            None => Zone::Utc,
            Some(name) => Zone::parse(name)?,
        };
        // Days and weeks have a fixed length unless the zone's offset
        // changes; then they are civil periods like months.
        let interval = match (interval, &zone) {
            (Interval::Day, Zone::Utc | Zone::Fixed(_)) => Interval::Fixed {
                width: MS_PER_DAY,
                base: 0,
            },
            (Interval::Week, Zone::Utc | Zone::Fixed(_)) => Interval::Fixed {
                width: MS_PER_WEEK,
                base: MONDAY_OFFSET_MS,
            },
            (interval, _) => interval,
        };
        let bounds = |b: &Option<DateHistogramBounds>, option: &str| -> Result<_, String> {
            let Some(b) = b else {
                return Ok([None, None]);
            };
            let min = b
                .min
                .as_ref()
                .map(|v| date_bound(v, &zone, option, "min"))
                .transpose()?;
            let max = b
                .max
                .as_ref()
                .map(|v| date_bound(v, &zone, option, "max"))
                .transpose()?;
            if let (Some(lo), Some(hi)) = (min, max) {
                if lo > hi {
                    return Err(format!(
                        "`date_histogram` `{option}` `min` ({}) is after `max` ({})",
                        datetime::format_epoch_millis(lo),
                        datetime::format_epoch_millis(hi)
                    ));
                }
            }
            Ok([min, max])
        };
        let extended = bounds(&self.extended_bounds, "extended_bounds")?;
        let hard = bounds(&self.hard_bounds, "hard_bounds")?;
        Ok(DateHistogram {
            interval,
            zone,
            offset,
            extended,
            hard,
        })
    }
}

/// A `date_histogram` bound as epoch milliseconds: epoch milliseconds, or
/// a date string, wall-clock time in `zone` unless it names its offset.
fn date_bound(
    value: &serde_json::Value,
    zone: &Zone,
    option: &str,
    end: &str,
) -> Result<i64, String> {
    let parsed = match value {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(text) => match datetime::parse_epoch_millis(text) {
            Some(ms) if datetime::has_explicit_offset(text) => Some(ms),
            Some(local) => Some(zone.local_to_utc(local)),
            None => text.trim().parse::<i64>().ok(),
        },
        _ => None,
    };
    parsed.ok_or_else(|| {
        format!(
            "`date_histogram` `{option}` `{end}` must be a date (e.g. \"2024-01-31\" or \
             \"2024-01-31T12:00:00Z\") or whole epoch milliseconds, got {value}"
        )
    })
}

/// Milliseconds in `<positive whole number><unit>`, unit one of `ms`, `s`,
/// `m`, `h`, `d`.
pub fn parse_fixed_interval(text: &str) -> Result<i64, String> {
    let bad = |why: &str| {
        Err(format!(
            "invalid `fixed_interval` {text:?}: {why}; expected a positive whole number followed \
             by one of ms, s, m, h, d (e.g. \"30s\", \"5m\", \"1h\", \"7d\")"
        ))
    };
    let digits_end = text
        .bytes()
        .position(|b| !b.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, unit) = text.split_at(digits_end);
    if number.is_empty() {
        return bad("it does not start with a number");
    }
    let unit_ms: i64 = match unit {
        "ms" => 1,
        "s" => 1000,
        "m" => MS_PER_MINUTE,
        "h" => MS_PER_HOUR,
        "d" => MS_PER_DAY,
        "w" => return bad("weeks are a calendar unit — use \"7d\", or `calendar_interval: \"1w\"` for Monday-aligned weeks"),
        "M" | "q" | "y" => {
            return bad(
                "months, quarters and years vary in length — use `calendar_interval` \"1M\", \
                 \"1q\" or \"1y\"",
            )
        }
        "" => return bad("the unit is missing"),
        _ => return bad("unknown unit"),
    };
    let Ok(n) = number.parse::<i64>() else {
        return bad("the number is too large");
    };
    if n == 0 {
        return bad("the interval must be greater than 0");
    }
    match n.checked_mul(unit_ms) {
        Some(ms) if ms <= MAX_DURATION_MS => Ok(ms),
        _ => bad("the interval is too large"),
    }
}

/// A `calendar_interval`: a single unit, as in Elasticsearch.
fn parse_calendar_interval(text: &str) -> Result<Interval, String> {
    Ok(match text {
        "1m" | "minute" => Interval::Fixed {
            width: MS_PER_MINUTE,
            base: 0,
        },
        "1h" | "hour" => Interval::Fixed {
            width: MS_PER_HOUR,
            base: 0,
        },
        "1d" | "day" => Interval::Day,
        "1w" | "week" => Interval::Week,
        "1M" | "month" => Interval::Month,
        "1q" | "quarter" => Interval::Quarter,
        "1y" | "year" => Interval::Year,
        _ => {
            let multiple = text.len() > 1
                && text[..text.len() - 1].bytes().all(|b| b.is_ascii_digit())
                && text.ends_with(['m', 'h', 'd', 'w', 'M', 'q', 'y']);
            let why = if multiple {
                "calendar intervals take a single unit"
            } else {
                "unknown calendar interval"
            };
            return Err(format!(
                "invalid `calendar_interval` {text:?}: {why}; supported values are 1m, 1h, 1d, \
                 1w, 1M, 1q and 1y (minute, hour, day, week, month, quarter, year). For a \
                 multiple of a fixed-length unit use `fixed_interval` (e.g. \"5m\", \"12h\", \
                 \"7d\")"
            ));
        }
    })
}

/// A signed duration for `offset`: `[+|-]<number><unit>`; 0 is allowed.
fn parse_offset(text: &str) -> Result<i64, String> {
    let (sign, rest) = match text.as_bytes().first() {
        Some(b'-') => (-1, &text[1..]),
        Some(b'+') => (1, &text[1..]),
        _ => (1, text),
    };
    let number = rest.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit = &rest[number.len()..];
    // A zero offset ("0", "0h", ...) is valid, though not a valid interval.
    if !number.is_empty()
        && number.bytes().all(|b| b == b'0')
        && matches!(unit, "" | "ms" | "s" | "m" | "h" | "d")
    {
        return Ok(0);
    }
    let ms = parse_fixed_interval(rest).map_err(|_| {
        format!(
            "invalid `date_histogram` `offset` {text:?}: expected an optionally signed duration \
             such as \"+6h\" or \"-30m\" (units ms, s, m, h, d), or milliseconds"
        )
    })?;
    Ok(sign * ms)
}

/// The time zone a `date_histogram` rounds in.
#[derive(Debug, Clone)]
pub enum Zone {
    Utc,
    /// A constant UTC offset in milliseconds (never 0).
    Fixed(i64),
    /// A zone whose offset changes (daylight saving, history).
    Named {
        name: String,
        tz: jiff::tz::TimeZone,
    },
}

impl PartialEq for Zone {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Zone::Utc, Zone::Utc) => true,
            (Zone::Fixed(a), Zone::Fixed(b)) => a == b,
            (Zone::Named { name: a, .. }, Zone::Named { name: b, .. }) => a == b,
            _ => false,
        }
    }
}

impl Zone {
    /// `UTC` / `Z`, a `±HH[:MM]` / `±HHMM` offset, or an IANA zone name
    /// (looked up in the tz database bundled into the binary, so it works
    /// without `/usr/share/zoneinfo`).
    pub fn parse(text: &str) -> Result<Zone, String> {
        let fixed = |ms: i64| if ms == 0 { Zone::Utc } else { Zone::Fixed(ms) };
        if text == "Z" || text == "UTC" {
            return Ok(Zone::Utc);
        }
        if text.starts_with(['+', '-']) {
            return parse_utc_offset(text).map(fixed).ok_or_else(|| {
                format!(
                    "invalid `time_zone` {text:?}: a UTC offset must be `±HH:MM`, `±HHMM` or \
                     `±HH`, at most 18 hours (e.g. \"+05:30\", \"-08:00\")"
                )
            });
        }
        let tz = jiff::tz::TimeZone::get(text).map_err(|_| {
            format!(
                "unknown `time_zone` {text:?}: expected an IANA zone name (e.g. \
                 \"Europe/Paris\", \"America/New_York\"), a UTC offset (e.g. \"+05:30\", \
                 \"-08:00\"), or \"UTC\""
            )
        })?;
        // A zone that never changes offset (`Etc/UTC`, `Etc/GMT+5`) is a
        // fixed offset.
        let transitions = tz.preceding(jiff::Timestamp::MAX).next().is_some();
        if !transitions {
            let offset = tz.to_offset(jiff::Timestamp::UNIX_EPOCH).seconds();
            return Ok(fixed(i64::from(offset) * 1000));
        }
        Ok(Zone::Named {
            name: text.to_string(),
            tz,
        })
    }

    /// The zone's UTC offset in milliseconds at the instant `utc`.
    pub fn offset_at(&self, utc: i64) -> i64 {
        match self {
            Zone::Utc => 0,
            Zone::Fixed(offset) => *offset,
            Zone::Named { tz, .. } => match timestamp(utc) {
                Some(ts) => i64::from(tz.to_offset(ts).seconds()) * 1000,
                None => 0,
            },
        }
    }

    /// The instants (at most two, ascending) whose wall-clock time in this
    /// zone is `local` (wall-clock milliseconds since 1970-01-01T00:00):
    /// none in a spring-forward gap, two in a fall-back overlap.
    fn instants_of(&self, local: i64) -> [Option<i64>; 2] {
        if let Zone::Utc | Zone::Fixed(_) = self {
            return [Some(local.saturating_sub(self.offset_at(local))), None];
        }
        let mut found: Vec<i64> = Vec::with_capacity(3);
        // The offsets in force within a day either side cover every offset
        // `local` can correspond to (offsets are under a day).
        for probe in [
            local.saturating_sub(MS_PER_DAY),
            local,
            local.saturating_add(MS_PER_DAY),
        ] {
            let offset = self.offset_at(probe);
            let utc = local.saturating_sub(offset);
            if self.offset_at(utc) == offset && !found.contains(&utc) {
                found.push(utc);
            }
        }
        found.sort_unstable();
        [found.first().copied(), found.get(1).copied()]
    }

    /// The instant of wall-clock time `local`: the earlier one in an
    /// overlap, and in a gap the instant the gap ends (the wall-clock time
    /// read with the offset before the transition) — `java.time`'s and
    /// Elasticsearch's choice.
    pub fn local_to_utc(&self, local: i64) -> i64 {
        match self.instants_of(local) {
            [Some(first), _] => first,
            _ => {
                let before = self
                    .offset_at(local.saturating_sub(MS_PER_DAY))
                    .min(self.offset_at(local.saturating_add(MS_PER_DAY)));
                local.saturating_sub(before)
            }
        }
    }

    /// The latest offset transition at or before `utc`.
    fn previous_transition(&self, utc: i64) -> Option<i64> {
        let Zone::Named { tz, .. } = self else {
            return None;
        };
        let after = timestamp(utc.saturating_add(1))?;
        tz.preceding(after)
            .next()
            .map(|t| t.timestamp().as_millisecond())
    }
}

/// `utc` milliseconds as a `jiff` timestamp, clamped into its range.
fn timestamp(utc: i64) -> Option<jiff::Timestamp> {
    let lo = jiff::Timestamp::MIN.as_millisecond();
    let hi = jiff::Timestamp::MAX.as_millisecond();
    jiff::Timestamp::from_millisecond(utc.clamp(lo, hi)).ok()
}

/// `±H`, `±HH`, `±HH:MM` or `±HHMM` as milliseconds east of UTC.
fn parse_utc_offset(text: &str) -> Option<i64> {
    let (sign, rest) = match text.as_bytes().first()? {
        b'+' => (1, &text[1..]),
        b'-' => (-1, &text[1..]),
        _ => return None,
    };
    if !rest.bytes().all(|b| b.is_ascii_digit() || b == b':') {
        return None;
    }
    let (hours, minutes) = match rest.len() {
        1 | 2 => (rest, "0"),
        4 if !rest.contains(':') => rest.split_at(2),
        5 if rest.as_bytes()[2] == b':' => (&rest[..2], &rest[3..]),
        _ => return None,
    };
    let (hours, minutes): (i64, i64) = (hours.parse().ok()?, minutes.parse().ok()?);
    let ms = (hours * 60 + minutes) * MS_PER_MINUTE;
    (minutes < 60 && ms <= MAX_ZONE_OFFSET_MS).then_some(sign * ms)
}

/// How a `date_histogram` maps epoch milliseconds to bucket indices and
/// back (see the module docs).
#[derive(Debug, Clone, PartialEq)]
pub struct DateHistogram {
    interval: Interval,
    zone: Zone,
    offset: i64,
    extended: [Option<i64>; 2],
    hard: [Option<i64>; 2],
}

impl DateHistogram {
    /// The index of the bucket epoch-millisecond `value` falls in, `None`
    /// when `hard_bounds` excludes it.
    pub fn bucket(&self, value: i64) -> Option<i64> {
        let below = self.hard[0].is_some_and(|min| value < min);
        let above = self.hard[1].is_some_and(|max| value > max);
        (!below && !above).then(|| self.index(value))
    }

    fn index(&self, value: i64) -> i64 {
        let shifted = value.saturating_sub(self.offset);
        match (self.interval, &self.zone) {
            (Interval::Fixed { width, .. }, Zone::Named { .. }) => {
                self.round_wall_clock(width, shifted)
            }
            (Interval::Fixed { width, base }, zone) => {
                let local = shifted.saturating_add(zone.offset_at(shifted));
                date_histogram_bucket(local, width, base)
            }
            (civil, zone) => civil_index(civil, shifted.saturating_add(zone.offset_at(shifted))),
        }
    }

    /// Elasticsearch's rounding of a sub-day (or fixed) interval in a zone
    /// with transitions: the bucket starts at the latest instant `<= utc`
    /// whose wall-clock time is a multiple of `width`.
    fn round_wall_clock(&self, width: i64, utc: i64) -> i64 {
        let encode = |n: i64, occurrence: i64| n.saturating_mul(2).saturating_add(occurrence);
        let mut at = utc;
        for _ in 0..8 {
            let local = at.saturating_add(self.zone.offset_at(at));
            let n = local.div_euclid(width);
            let start = match self.zone.instants_of(n.saturating_mul(width)) {
                [_, Some(second)] if second <= at => Some((second, 1)),
                [Some(first), _] if first <= at => Some((first, 0)),
                _ => None,
            };
            let transition = self.zone.previous_transition(at);
            match (start, transition) {
                (Some((start, occurrence)), t) if t.is_none_or(|t| t <= start) => {
                    return encode(n, occurrence)
                }
                // The wall-clock start was skipped, or lies before the
                // latest transition: round from just before the transition
                // instead (Elasticsearch's `JavaTimeNotToMidnightRounding`).
                (_, Some(t)) if t <= at => at = t - 1,
                _ => break,
            }
        }
        let local = utc.saturating_add(self.zone.offset_at(utc));
        encode(local.div_euclid(width), 0)
    }

    /// The epoch-millisecond key of bucket `index`, `None` for an index no
    /// bucket has (a wall-clock start skipped by, or not repeated in, a
    /// daylight-saving transition).
    pub fn key(&self, index: i64) -> Option<i64> {
        let start = match (self.interval, &self.zone) {
            (Interval::Fixed { width, .. }, Zone::Named { .. }) => {
                let local = index.div_euclid(2).saturating_mul(width);
                self.zone.instants_of(local)[index.rem_euclid(2) as usize]?
            }
            (Interval::Fixed { width, base }, zone) => {
                let local = date_histogram_key(index, width, base);
                local.saturating_sub(zone.offset_at(local))
            }
            (civil, zone) => zone.local_to_utc(civil_start(civil, index)),
        };
        Some(start.saturating_add(self.offset))
    }

    /// `key_as_string` for `key`: the key's wall-clock time in the zone,
    /// with its offset (`Z` for UTC, as without a `time_zone`).
    pub fn key_as_string(&self, key: i64) -> String {
        datetime::format_epoch_millis_at_offset(key, self.zone.offset_at(key))
    }

    /// How many indices there are per bucket at most: 2 where an index
    /// encodes which occurrence of a repeated wall-clock time it is, 1
    /// otherwise. The bucket cap is checked against `span / stride` before
    /// anything is allocated, and exactly once the keys are known.
    pub fn index_stride(&self) -> u64 {
        match (self.interval, &self.zone) {
            (Interval::Fixed { .. }, Zone::Named { .. }) => 2,
            _ => 1,
        }
    }

    /// The indices to gap-fill (with `min_doc_count: 0`): the observed
    /// range widened by `extended_bounds` and clipped by `hard_bounds`.
    pub fn fill_range(&self, observed: Option<(i64, i64)>) -> Option<(i64, i64)> {
        let index = |end: Option<i64>| end.map(|v| self.index(v));
        fill_range(
            observed,
            [index(self.extended[0]), index(self.extended[1])],
            [index(self.hard[0]), index(self.hard[1])],
        )
    }
}

/// Index of the civil period holding wall-clock time `local`.
fn civil_index(interval: Interval, local: i64) -> i64 {
    match interval {
        Interval::Day => local.div_euclid(MS_PER_DAY),
        Interval::Week => (i128::from(local) - i128::from(MONDAY_OFFSET_MS))
            .div_euclid(i128::from(MS_PER_WEEK)) as i64,
        Interval::Month | Interval::Quarter | Interval::Year => {
            let (year, month, _) = datetime::civil_from_days(local.div_euclid(MS_PER_DAY));
            let months = (year - 1970) * 12 + i64::from(month) - 1;
            match interval {
                Interval::Month => months,
                Interval::Quarter => months.div_euclid(3),
                _ => year - 1970,
            }
        }
        Interval::Fixed { width, base } => date_histogram_bucket(local, width, base),
    }
}

/// Wall-clock start of civil period `index`, saturating.
fn civil_start(interval: Interval, index: i64) -> i64 {
    let months = match interval {
        Interval::Day => return date_histogram_key(index, MS_PER_DAY, 0),
        Interval::Week => return date_histogram_key(index, MS_PER_WEEK, MONDAY_OFFSET_MS),
        Interval::Fixed { width, base } => return date_histogram_key(index, width, base),
        Interval::Month => i128::from(index),
        Interval::Quarter => i128::from(index) * 3,
        Interval::Year => i128::from(index) * 12,
    };
    let max_months = i128::from(MAX_CIVIL_YEAR) * 12;
    let months = months.clamp(-max_months, max_months) as i64;
    let year = 1970 + months.div_euclid(12);
    let month = months.rem_euclid(12) as u32 + 1;
    let ms = i128::from(datetime::days_from_civil(year, month, 1)) * i128::from(MS_PER_DAY);
    ms.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

/// Index of the bucket `value` falls in: `floor((value - offset) /
/// interval)`, saturating at the `i64` range for absurd values.
pub fn histogram_bucket(value: f64, interval: f64, offset: f64) -> i64 {
    ((value - offset) / interval).floor() as i64
}

/// The key of bucket `index`: `index * interval + offset`.
pub fn histogram_key(index: i64, interval: f64, offset: f64) -> f64 {
    index as f64 * interval + offset
}

/// Index of the bucket epoch-millisecond `value` falls in.
pub fn date_histogram_bucket(value: i64, interval: i64, offset: i64) -> i64 {
    // i128: `value - offset` cannot overflow, whatever the stored value.
    (i128::from(value) - i128::from(offset)).div_euclid(i128::from(interval)) as i64
}

/// The epoch-millisecond key of bucket `index`, saturating.
pub fn date_histogram_key(index: i64, interval: i64, offset: i64) -> i64 {
    let key = i128::from(index) * i128::from(interval) + i128::from(offset);
    key.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datetime::{format_epoch_millis, parse_epoch_millis};

    fn ms(text: &str) -> i64 {
        parse_epoch_millis(text).unwrap_or_else(|| panic!("bad date {text}"))
    }

    #[test]
    fn fixed_intervals_parse() {
        for (text, ms) in [
            ("1ms", 1),
            ("250ms", 250),
            ("30s", 30_000),
            ("5m", 300_000),
            ("1h", 3_600_000),
            ("7d", 604_800_000),
            ("90m", 5_400_000),
        ] {
            assert_eq!(parse_fixed_interval(text), Ok(ms), "{text}");
        }
    }

    #[test]
    fn garbage_fixed_intervals_are_rejected() {
        for text in [
            "1 banana",
            "banana",
            "",
            "0",
            "0s",
            "-1h",
            "+1h",
            "1",
            "1.5h",
            "1 h",
            " 1h",
            "1h ",
            "1w",
            "1M",
            "1y",
            "1q",
            "h",
            "99999999999999999999d",
            "100000000d",
        ] {
            let err = parse_fixed_interval(text).unwrap_err();
            assert!(err.contains("fixed_interval"), "{text}: {err}");
        }
    }

    fn plan(json: serde_json::Value) -> Result<DateHistogram, String> {
        serde_json::from_value::<AggDateHistogram>(json)
            .map_err(|e| e.to_string())?
            .plan()
    }

    /// The plan of a `date_histogram` on `t` with these extra options.
    fn dh(extra: serde_json::Value) -> DateHistogram {
        let mut body = serde_json::json!({"field": "t"});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        plan(body).unwrap()
    }

    /// `key_as_string` of the bucket holding `date`.
    fn bucket_of(p: &DateHistogram, date: &str) -> String {
        let key = p.key(p.bucket(ms(date)).unwrap()).unwrap();
        p.key_as_string(key)
    }

    #[test]
    fn calendar_intervals_supported_and_rejected() {
        let ok = |ci: &str| plan(serde_json::json!({"field": "t", "calendar_interval": ci}));
        let fixed = |width, base| Interval::Fixed { width, base };
        assert_eq!(ok("1m").unwrap().interval, fixed(60_000, 0));
        assert_eq!(ok("hour").unwrap().interval, fixed(3_600_000, 0));
        assert_eq!(ok("1d").unwrap().interval, fixed(MS_PER_DAY, 0));
        assert_eq!(
            ok("1w").unwrap().interval,
            fixed(MS_PER_WEEK, MONDAY_OFFSET_MS)
        );
        for (text, interval) in [
            ("1M", Interval::Month),
            ("month", Interval::Month),
            ("1q", Interval::Quarter),
            ("quarter", Interval::Quarter),
            ("1y", Interval::Year),
            ("year", Interval::Year),
        ] {
            assert_eq!(ok(text).unwrap().interval, interval, "{text}");
        }
        for bad in ["2d", "2M", "3q", "10y", "banana", "1Y", "M", ""] {
            let err = ok(bad).unwrap_err();
            assert!(err.contains("calendar_interval"), "{bad}: {err}");
            assert!(err.contains("fixed_interval"), "{bad}: {err}");
        }
        assert!(ok("2M").unwrap_err().contains("single unit"));
        let err = plan(serde_json::json!({"field": "t"})).unwrap_err();
        assert!(err.contains("fixed_interval"), "{err}");
        let err = plan(
            serde_json::json!({"field": "t", "fixed_interval": "1h", "calendar_interval": "1h"}),
        )
        .unwrap_err();
        assert!(err.contains("not both"), "{err}");
    }

    #[test]
    fn offsets_parse() {
        let off = |o: serde_json::Value| {
            plan(serde_json::json!({"field": "t", "fixed_interval": "1d", "offset": o}))
                .map(|p| p.offset)
        };
        assert_eq!(off("+6h".into()), Ok(21_600_000));
        assert_eq!(off("6h".into()), Ok(21_600_000));
        assert_eq!(off("-30m".into()), Ok(-1_800_000));
        assert_eq!(off("0".into()), Ok(0));
        assert_eq!(off(1500.into()), Ok(1500));
        assert!(off("tomorrow".into()).unwrap_err().contains("offset"));
        assert!(off("+".into()).is_err());
    }

    fn numeric(interval: f64, offset: f64) -> AggHistogram {
        AggHistogram {
            field: "f".into(),
            interval,
            offset,
            min_doc_count: 0,
            extended_bounds: None,
            hard_bounds: None,
        }
    }

    #[test]
    fn histogram_validation() {
        assert!(numeric(1.0, 0.0).validate().is_ok());
        assert!(numeric(0.5, -3.0).validate().is_ok());
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(numeric(bad, 0.0).validate().is_err(), "{bad}");
        }
        assert!(numeric(1.0, f64::NAN).validate().is_err());
        let bounds = |min, max| {
            Some(HistogramBounds {
                min: Some(min),
                max: Some(max),
            })
        };
        let mut h = numeric(1.0, 0.0);
        h.extended_bounds = bounds(9.0, 1.0);
        let err = h.validate().unwrap_err();
        assert!(
            err.contains("`extended_bounds` `min` (9) is greater"),
            "{err}"
        );
        let mut h = numeric(1.0, 0.0);
        h.hard_bounds = bounds(1.0, f64::INFINITY);
        assert!(h.validate().unwrap_err().contains("hard_bounds"));
        let mut h = numeric(1.0, 0.0);
        h.hard_bounds = bounds(1.0, 1.0);
        assert!(h.validate().is_ok());
    }

    #[test]
    fn bucket_arithmetic_handles_negatives_boundaries_and_offsets() {
        // interval 10, offset 0.
        for (v, key) in [
            (0.0, 0.0),
            (9.999, 0.0),
            (10.0, 10.0),
            (-0.001, -10.0),
            (-10.0, -10.0),
            (-10.5, -20.0),
        ] {
            let b = histogram_bucket(v, 10.0, 0.0);
            assert_eq!(histogram_key(b, 10.0, 0.0), key, "{v}");
        }
        // interval 10, offset 3: buckets [.., -7), [-7, 3), [3, 13), ..
        for (v, key) in [
            (3.0, 3.0),
            (2.999, -7.0),
            (12.9, 3.0),
            (13.0, 13.0),
            (-7.0, -7.0),
            (-7.1, -17.0),
        ] {
            let b = histogram_bucket(v, 10.0, 3.0);
            assert_eq!(histogram_key(b, 10.0, 3.0), key, "{v}");
        }
        // Dates: hourly, and with a -1ms value before the epoch.
        let h = 3_600_000;
        assert_eq!(
            date_histogram_key(date_histogram_bucket(-1, h, 0), h, 0),
            -h
        );
        assert_eq!(date_histogram_key(date_histogram_bucket(0, h, 0), h, 0), 0);
        assert_eq!(date_histogram_key(date_histogram_bucket(h, h, 0), h, 0), h);
        assert_eq!(
            date_histogram_key(date_histogram_bucket(h - 1, h, 0), h, 0),
            0
        );
        // Offset 15 minutes.
        let o = 900_000;
        assert_eq!(date_histogram_key(date_histogram_bucket(o, h, o), h, o), o);
        assert_eq!(
            date_histogram_key(date_histogram_bucket(o - 1, h, o), h, o),
            o - h
        );
        // Extreme values saturate instead of overflowing.
        let _ = date_histogram_bucket(i64::MIN, 1, MAX_DURATION_MS);
        let _ = date_histogram_key(i64::MAX, MAX_DURATION_MS, MAX_DURATION_MS);
    }

    #[test]
    fn month_quarter_and_year_buckets_on_utc_calendar_boundaries() {
        let month = dh(serde_json::json!({"calendar_interval": "1M"}));
        let quarter = dh(serde_json::json!({"calendar_interval": "quarter"}));
        let year = dh(serde_json::json!({"calendar_interval": "1y"}));
        for (date, m, q, y) in [
            // Leap year: Feb 29 is still February.
            (
                "2024-02-29T23:59:59.999Z",
                "2024-02-01",
                "2024-01-01",
                "2024-01-01",
            ),
            (
                "2024-03-01T00:00:00Z",
                "2024-03-01",
                "2024-01-01",
                "2024-01-01",
            ),
            // Non-leap years, including the 1900 century rule.
            (
                "2023-02-28T12:00:00Z",
                "2023-02-01",
                "2023-01-01",
                "2023-01-01",
            ),
            ("2023-03-01", "2023-03-01", "2023-01-01", "2023-01-01"),
            ("1900-02-28", "1900-02-01", "1900-01-01", "1900-01-01"),
            ("1900-03-01", "1900-03-01", "1900-01-01", "1900-01-01"),
            ("2000-02-29", "2000-02-01", "2000-01-01", "2000-01-01"),
            // Quarter boundaries.
            (
                "2024-03-31T23:59:59.999Z",
                "2024-03-01",
                "2024-01-01",
                "2024-01-01",
            ),
            ("2024-04-01", "2024-04-01", "2024-04-01", "2024-01-01"),
            ("2024-05-15", "2024-05-01", "2024-04-01", "2024-01-01"),
            // Year boundary.
            (
                "2023-12-31T23:59:59.999Z",
                "2023-12-01",
                "2023-10-01",
                "2023-01-01",
            ),
            ("2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"),
            // Before the epoch.
            (
                "1969-12-31T23:59:59.999Z",
                "1969-12-01",
                "1969-10-01",
                "1969-01-01",
            ),
            ("1969-02-14", "1969-02-01", "1969-01-01", "1969-01-01"),
            ("1970-01-01", "1970-01-01", "1970-01-01", "1970-01-01"),
        ] {
            let at = |p: &DateHistogram, want: &str| {
                assert_eq!(
                    bucket_of(p, date),
                    format!("{want}T00:00:00.000Z"),
                    "{date}"
                );
            };
            at(&month, m);
            at(&quarter, q);
            at(&year, y);
        }
        // Consecutive indices are consecutive months, so a gap fill steps
        // by calendar month (28, 29, 30 and 31 days).
        let lo = month.bucket(ms("2023-12-10")).unwrap();
        let hi = month.bucket(ms("2024-05-10")).unwrap();
        let keys: Vec<String> = (lo..=hi)
            .map(|i| format_epoch_millis(month.key(i).unwrap()))
            .collect();
        assert_eq!(
            keys,
            [
                "2023-12-01T00:00:00.000Z",
                "2024-01-01T00:00:00.000Z",
                "2024-02-01T00:00:00.000Z",
                "2024-03-01T00:00:00.000Z",
                "2024-04-01T00:00:00.000Z",
                "2024-05-01T00:00:00.000Z",
            ]
        );
        let lo = quarter.bucket(ms("1969-11-01")).unwrap();
        let hi = quarter.bucket(ms("1970-08-01")).unwrap();
        let keys: Vec<String> = (lo..=hi)
            .map(|i| format_epoch_millis(quarter.key(i).unwrap()))
            .collect();
        assert_eq!(
            keys,
            [
                "1969-10-01T00:00:00.000Z",
                "1970-01-01T00:00:00.000Z",
                "1970-04-01T00:00:00.000Z",
                "1970-07-01T00:00:00.000Z",
            ]
        );
        // Extreme values do not overflow.
        for v in [i64::MIN, i64::MAX, 0] {
            for p in [&month, &quarter, &year] {
                let _ = p.key(p.bucket(v).unwrap());
            }
        }
    }

    #[test]
    fn month_buckets_with_an_offset() {
        // Months starting on the 2nd day at 06:00.
        let p = dh(serde_json::json!({"calendar_interval": "1M", "offset": "+30h"}));
        assert_eq!(
            bucket_of(&p, "2024-03-02T06:00:00Z"),
            "2024-03-02T06:00:00.000Z"
        );
        assert_eq!(
            bucket_of(&p, "2024-03-02T05:59:59Z"),
            "2024-02-02T06:00:00.000Z"
        );
    }

    #[test]
    fn fixed_offset_time_zones_shift_boundaries_and_label_keys() {
        let ist = dh(serde_json::json!({"calendar_interval": "1d", "time_zone": "+05:30"}));
        assert_eq!(
            bucket_of(&ist, "2024-03-10T20:00:00Z"),
            "2024-03-11T00:00:00.000+05:30"
        );
        assert_eq!(
            ist.key(ist.bucket(ms("2024-03-10T20:00:00Z")).unwrap()),
            Some(ms("2024-03-10T18:30:00Z"))
        );
        assert_eq!(
            bucket_of(&ist, "2024-03-10T18:29:59.999Z"),
            "2024-03-10T00:00:00.000+05:30"
        );
        let pst = dh(serde_json::json!({"fixed_interval": "1d", "time_zone": "-08:00"}));
        assert_eq!(
            bucket_of(&pst, "2024-03-10T07:59:59Z"),
            "2024-03-09T00:00:00.000-08:00"
        );
        assert_eq!(
            bucket_of(&pst, "2024-03-10T08:00:00Z"),
            "2024-03-10T00:00:00.000-08:00"
        );
        let month = dh(serde_json::json!({"calendar_interval": "1M", "time_zone": "-0800"}));
        assert_eq!(
            bucket_of(&month, "2024-03-01T07:00:00Z"),
            "2024-02-01T00:00:00.000-08:00"
        );
        // `UTC`, `Z`, `+00:00` and `Etc/UTC` are plain UTC: same output as
        // no `time_zone`.
        for tz in ["UTC", "Z", "+00:00", "-00", "Etc/UTC"] {
            let p = dh(serde_json::json!({"fixed_interval": "1h", "time_zone": tz}));
            assert_eq!(p.zone, Zone::Utc, "{tz}");
            assert_eq!(
                bucket_of(&p, "2024-03-10T07:59:59Z"),
                "2024-03-10T07:00:00.000Z"
            );
        }
        // Zone parsing.
        assert_eq!(Zone::parse("+5"), Ok(Zone::Fixed(5 * MS_PER_HOUR)));
        assert_eq!(Zone::parse("-0930"), Ok(Zone::Fixed(-570 * MS_PER_MINUTE)));
        assert_eq!(Zone::parse("+18:00"), Ok(Zone::Fixed(18 * MS_PER_HOUR)));
        for bad in [
            "+19:00",
            "+05:60",
            "+5:30",
            "+05:3",
            "+",
            "Mars/Olympus_Mons",
            "",
            "utc+1",
        ] {
            assert!(Zone::parse(bad).unwrap_err().contains("time_zone"), "{bad}");
        }
    }

    /// Length of the `calendar_interval` bucket holding `date`, in hours.
    fn bucket_hours(p: &DateHistogram, date: &str) -> f64 {
        let index = p.bucket(ms(date)).unwrap();
        let start = p.key(index).unwrap();
        let end = p.key(index + 1).unwrap();
        (end - start) as f64 / 3_600_000.0
    }

    #[test]
    fn day_buckets_across_dst_are_23_and_25_hours() {
        let ny =
            dh(serde_json::json!({"calendar_interval": "1d", "time_zone": "America/New_York"}));
        assert_eq!(bucket_hours(&ny, "2024-03-10T12:00:00Z"), 23.0);
        assert_eq!(bucket_hours(&ny, "2024-11-03T12:00:00Z"), 25.0);
        assert_eq!(bucket_hours(&ny, "2024-06-01T12:00:00Z"), 24.0);
        assert_eq!(
            bucket_of(&ny, "2024-03-10T12:00:00Z"),
            "2024-03-10T00:00:00.000-05:00"
        );
        assert_eq!(
            bucket_of(&ny, "2024-03-11T03:59:59Z"),
            "2024-03-10T00:00:00.000-05:00"
        );
        assert_eq!(
            bucket_of(&ny, "2024-03-11T04:00:00Z"),
            "2024-03-11T00:00:00.000-04:00"
        );
        assert_eq!(
            bucket_of(&ny, "2024-11-04T04:59:59Z"),
            "2024-11-03T00:00:00.000-04:00"
        );
        let paris =
            dh(serde_json::json!({"calendar_interval": "day", "time_zone": "Europe/Paris"}));
        assert_eq!(bucket_hours(&paris, "2024-03-31T12:00:00Z"), 23.0);
        assert_eq!(bucket_hours(&paris, "2024-10-27T12:00:00Z"), 25.0);
        assert_eq!(
            bucket_of(&paris, "2024-03-30T23:00:00Z"),
            "2024-03-31T00:00:00.000+01:00"
        );
        assert_eq!(
            bucket_of(&paris, "2024-04-01T12:00:00Z"),
            "2024-04-01T00:00:00.000+02:00"
        );
        // Weeks and months in a DST zone follow the local calendar too.
        let week = dh(serde_json::json!({"calendar_interval": "1w", "time_zone": "Europe/Paris"}));
        assert_eq!(
            bucket_hours(&week, "2024-03-31T12:00:00Z"),
            7.0 * 24.0 - 1.0
        );
        assert_eq!(
            bucket_of(&week, "2024-03-31T12:00:00Z"),
            "2024-03-25T00:00:00.000+01:00"
        );
        let month = dh(serde_json::json!({"calendar_interval": "1M", "time_zone": "Europe/Paris"}));
        assert_eq!(
            bucket_of(&month, "2024-03-31T22:30:00Z"),
            "2024-04-01T00:00:00.000+02:00"
        );
        assert_eq!(
            bucket_hours(&month, "2024-03-15T00:00:00Z"),
            31.0 * 24.0 - 1.0
        );
    }

    /// Keys of every bucket between those of `from` and `to`, as a gap
    /// fill emits them: indices without a bucket skipped, sorted by key.
    fn filled(p: &DateHistogram, from: &str, to: &str) -> Vec<String> {
        let (lo, hi) = (p.bucket(ms(from)).unwrap(), p.bucket(ms(to)).unwrap());
        let mut keys: Vec<i64> = (lo..=hi).filter_map(|i| p.key(i)).collect();
        keys.sort_unstable();
        keys.into_iter().map(|k| p.key_as_string(k)).collect()
    }

    #[test]
    fn hour_buckets_across_dst_follow_elasticsearch() {
        let ny =
            dh(serde_json::json!({"calendar_interval": "1h", "time_zone": "America/New_York"}));
        // Fall back: the repeated 01:00 hour is two buckets, one per
        // offset, each an hour long.
        assert_eq!(
            bucket_of(&ny, "2024-11-03T05:30:00Z"),
            "2024-11-03T01:00:00.000-04:00"
        );
        assert_eq!(
            bucket_of(&ny, "2024-11-03T06:30:00Z"),
            "2024-11-03T01:00:00.000-05:00"
        );
        assert_eq!(
            filled(&ny, "2024-11-03T04:00:00Z", "2024-11-03T08:00:00Z"),
            [
                "2024-11-03T00:00:00.000-04:00",
                "2024-11-03T01:00:00.000-04:00",
                "2024-11-03T01:00:00.000-05:00",
                "2024-11-03T02:00:00.000-05:00",
                "2024-11-03T03:00:00.000-05:00",
            ]
        );
        // Spring forward: there is no 02:00 bucket.
        assert_eq!(
            filled(&ny, "2024-03-10T06:00:00Z", "2024-03-10T08:00:00Z"),
            [
                "2024-03-10T01:00:00.000-05:00",
                "2024-03-10T03:00:00.000-04:00",
                "2024-03-10T04:00:00.000-04:00",
            ]
        );
        // A fixed 30m interval across the fall-back overlap, sorted.
        let half =
            dh(serde_json::json!({"fixed_interval": "30m", "time_zone": "America/New_York"}));
        assert_eq!(
            filled(&half, "2024-11-03T05:00:00Z", "2024-11-03T06:59:00Z"),
            [
                "2024-11-03T01:00:00.000-04:00",
                "2024-11-03T01:30:00.000-04:00",
                "2024-11-03T01:00:00.000-05:00",
                "2024-11-03T01:30:00.000-05:00",
            ]
        );
    }

    #[test]
    fn rounding_in_dst_zones_is_consistent() {
        // Every minute across both 2024 transitions in two zones, for
        // several intervals: the bucket key is at or before the value, the
        // key's own bucket is the same bucket, and keys never decrease.
        let mut values = Vec::new();
        for day in ["2024-03-10", "2024-03-31", "2024-10-27", "2024-11-03"] {
            let start = ms(day) - 6 * MS_PER_HOUR;
            values.extend((0..36 * 60).map(|m| start + m * MS_PER_MINUTE + 17));
        }
        for tz in ["America/New_York", "Europe/Paris", "Australia/Lord_Howe"] {
            for interval in [
                serde_json::json!({"fixed_interval": "1h"}),
                serde_json::json!({"fixed_interval": "30m"}),
                serde_json::json!({"fixed_interval": "7m"}),
                serde_json::json!({"fixed_interval": "1d"}),
                serde_json::json!({"fixed_interval": "3h", "offset": "+10m"}),
                serde_json::json!({"calendar_interval": "1h"}),
                serde_json::json!({"calendar_interval": "1d"}),
                serde_json::json!({"calendar_interval": "1w"}),
                serde_json::json!({"calendar_interval": "1M"}),
            ] {
                let mut body = interval.clone();
                body["time_zone"] = tz.into();
                let p = dh(body);
                let mut last = i64::MIN;
                for &v in &values {
                    let index = p.bucket(v).unwrap();
                    let key = p.key(index).unwrap();
                    assert!(key <= v, "{tz} {interval}: {v}");
                    assert_eq!(p.bucket(key), Some(index), "{tz} {interval}: {v}");
                    assert!(key >= last, "{tz} {interval}: {v}");
                    last = key;
                }
            }
        }
    }

    #[test]
    fn offset_composes_with_time_zone() {
        // Paris days starting at 06:00 local.
        let p = dh(
            serde_json::json!({"calendar_interval": "1d", "time_zone": "Europe/Paris", "offset": "+6h"}),
        );
        assert_eq!(
            bucket_of(&p, "2024-06-15T03:59:59Z"),
            "2024-06-14T06:00:00.000+02:00"
        );
        assert_eq!(
            bucket_of(&p, "2024-06-15T04:00:00Z"),
            "2024-06-15T06:00:00.000+02:00"
        );
    }

    #[test]
    fn bounds_widen_and_clip_the_fill_range() {
        let mut h = numeric(10.0, 0.0);
        h.extended_bounds = Some(HistogramBounds {
            min: Some(-5.0),
            max: Some(95.0),
        });
        let p = h.plan().unwrap();
        assert_eq!(p.fill_range(Some((2, 3))), Some((-1, 9)));
        assert_eq!(p.fill_range(Some((-4, 30))), Some((-4, 30)));
        assert_eq!(p.fill_range(None), Some((-1, 9)));
        h.hard_bounds = Some(HistogramBounds {
            min: Some(0.0),
            max: Some(50.0),
        });
        let p = h.plan().unwrap();
        assert_eq!(p.fill_range(Some((2, 3))), Some((0, 5)));
        assert_eq!(p.bucket(-0.1), None);
        assert_eq!(p.bucket(50.1), None);
        assert_eq!(p.bucket(0.0), Some(0));
        assert_eq!(p.bucket(50.0), Some(5));
        // Only `max` given.
        h.extended_bounds = None;
        h.hard_bounds = Some(HistogramBounds {
            min: None,
            max: Some(25.0),
        });
        let p = h.plan().unwrap();
        assert_eq!(p.fill_range(Some((-3, 9))), Some((-3, 2)));
        assert_eq!(p.bucket(-1e9), Some(-100_000_000));

        let d = dh(serde_json::json!({
            "calendar_interval": "1d",
            "time_zone": "-08:00",
            "extended_bounds": {"min": "2024-03-01", "max": 1_710_057_600_000_i64},
            "hard_bounds": {"min": "2024-02-28T08:00:00Z", "max": "2024-03-20"}
        }));
        // A bare date is local midnight in the zone.
        assert_eq!(d.extended[0], Some(ms("2024-03-01T08:00:00Z")));
        assert_eq!(d.hard[1], Some(ms("2024-03-20T08:00:00Z")));
        let day = |date: &str| d.bucket(ms(date)).unwrap();
        assert_eq!(
            d.fill_range(Some((
                day("2024-03-05T12:00:00Z"),
                day("2024-03-06T12:00:00Z")
            ))),
            Some((day("2024-03-01T12:00:00Z"), day("2024-03-10T12:00:00Z")))
        );
        assert_eq!(d.bucket(ms("2024-02-28T07:59:59Z")), None);
        assert_eq!(d.bucket(ms("2024-03-20T08:00:00.001Z")), None);
        assert!(d.bucket(ms("2024-03-20T08:00:00Z")).is_some());
    }

    #[test]
    fn bad_bounds_and_zones_are_rejected() {
        let err = |extra: serde_json::Value| {
            let mut body = serde_json::json!({"field": "t", "fixed_interval": "1d"});
            body.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            plan(body).unwrap_err()
        };
        let e = err(serde_json::json!({"time_zone": "Mars/Olympus_Mons"}));
        assert!(
            e.contains("unknown `time_zone` \"Mars/Olympus_Mons\""),
            "{e}"
        );
        let e =
            err(serde_json::json!({"extended_bounds": {"min": "2024-03-02", "max": "2024-03-01"}}));
        assert!(e.contains("`extended_bounds` `min`"), "{e}");
        assert!(e.contains("is after `max`"), "{e}");
        let e = err(serde_json::json!({"hard_bounds": {"min": "yesterday"}}));
        assert!(e.contains("`hard_bounds` `min` must be a date"), "{e}");
        let e = err(serde_json::json!({"hard_bounds": {"max": true}}));
        assert!(e.contains("`hard_bounds` `max` must be a date"), "{e}");
        let e = err(serde_json::json!({"hard_bounds": {"max": 1.5}}));
        assert!(e.contains("`hard_bounds` `max` must be a date"), "{e}");
        let e = err(serde_json::json!({"extended_bounds": {"min": 0, "low": 1}}));
        assert!(e.contains("unknown field `low`"), "{e}");
        // Epoch milliseconds as a string are accepted, as in Elasticsearch.
        let p = dh(serde_json::json!({"fixed_interval": "1d", "hard_bounds": {"min": "86400000"}}));
        assert_eq!(p.hard[0], Some(MS_PER_DAY));
    }
}
