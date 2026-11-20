//! `histogram` and `date_histogram` aggregation bodies (issue #219).
//!
//! Both put a value `v` into the bucket whose key is
//! `floor((v - offset) / interval) * interval + offset`, the formula
//! Elasticsearch uses. The bucket is identified internally by its index
//! `floor((v - offset) / interval)` so that keys are exact, merge by plain
//! integer equality across segments, and gaps between the lowest and the
//! highest observed bucket can be filled by counting.
//!
//! `date_histogram` supports `fixed_interval` (`ms`, `s`, `m`, `h`, `d`
//! units) and the `calendar_interval` units whose length never varies in
//! UTC (`1m`, `1h`, `1d`, `1w`). Month, quarter and year calendar intervals
//! are rejected with an error naming `fixed_interval`; there is no
//! `time_zone` support, so every bucket boundary is in UTC.

use serde::{Deserialize, Serialize};

/// Most buckets one `histogram` / `date_histogram` returns, gap-filled
/// buckets included. A request past it is a 400 rather than an unbounded
/// response (Elasticsearch's equivalent is `search.max_buckets`).
pub const MAX_HISTOGRAM_BUCKETS: u64 = 10_000;

const MS_PER_DAY: i64 = 86_400_000;
const MS_PER_WEEK: i64 = 7 * MS_PER_DAY;
/// 1970-01-01 was a Thursday; ISO weeks (and Elasticsearch's `1w`
/// calendar interval) start on Monday, 1970-01-05.
const MONDAY_OFFSET_MS: i64 = 4 * MS_PER_DAY;
/// Upper bound on any interval or offset, about 10,000 years: keeps every
/// bucket computation inside `i64` without checked arithmetic.
const MAX_DURATION_MS: i64 = 10_000 * 366 * MS_PER_DAY;

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
}

fn is_zero(v: &f64) -> bool {
    *v == 0.0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

impl AggHistogram {
    pub fn validate(&self) -> Result<(), String> {
        if !self.interval.is_finite() || self.interval <= 0.0 {
            return Err(format!(
                "`histogram` `interval` must be a number greater than 0, got {}",
                self.interval
            ));
        }
        if !self.offset.is_finite() {
            return Err("`histogram` `offset` must be a finite number".into());
        }
        Ok(())
    }
}

/// A duration as Elasticsearch writes it in `offset`: a string such as
/// `"6h"` / `"-30m"`, or a number of milliseconds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum DurationValue {
    Millis(i64),
    Text(String),
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
    /// Calendar interval: only the units that have a fixed length in UTC
    /// are supported — `1m` / `minute`, `1h` / `hour`, `1d` / `day`, and
    /// `1w` / `week` (weeks start on Monday). `1M`, `1q` and `1y` are
    /// rejected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calendar_interval: Option<String>,
    /// Shifts the bucket boundaries, e.g. `"+6h"` for days that start at
    /// 06:00 UTC. A duration string (sign optional) or milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<DurationValue>,
    /// See [`AggHistogram::min_doc_count`].
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub min_doc_count: u64,
}

impl AggDateHistogram {
    /// `(interval, offset)` in milliseconds, or a message for the 400.
    pub fn resolve(&self) -> Result<(i64, i64), String> {
        let (interval, base_offset) = match (&self.fixed_interval, &self.calendar_interval) {
            (Some(_), Some(_)) => {
                return Err(
                    "`date_histogram` takes one of `fixed_interval` or `calendar_interval`, not \
                     both"
                        .into(),
                )
            }
            (None, None) => {
                return Err(
                    "`date_histogram` needs `fixed_interval` (e.g. \"1h\", \"30s\", \"7d\")".into(),
                )
            }
            (Some(fixed), None) => (parse_fixed_interval(fixed)?, 0),
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
        Ok((interval, base_offset + offset))
    }

    pub fn validate(&self) -> Result<(), String> {
        self.resolve().map(|_| ())
    }
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
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => MS_PER_DAY,
        "w" => return bad("weeks are a calendar unit — use \"7d\", or `calendar_interval: \"1w\"` for Monday-aligned weeks"),
        "M" | "q" | "y" => {
            return bad("months, quarters and years vary in length and are not supported")
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

/// `(interval, offset)` for a supported `calendar_interval`.
fn parse_calendar_interval(text: &str) -> Result<(i64, i64), String> {
    match text {
        "1m" | "minute" => Ok((60_000, 0)),
        "1h" | "hour" => Ok((3_600_000, 0)),
        "1d" | "day" => Ok((MS_PER_DAY, 0)),
        "1w" | "week" => Ok((MS_PER_WEEK, MONDAY_OFFSET_MS)),
        "1M" | "month" | "1q" | "quarter" | "1y" | "year" => Err(format!(
            "`calendar_interval` {text:?} is not supported: months, quarters and years vary in \
             length. Supported calendar intervals are 1m, 1h, 1d and 1w; for other widths use \
             `fixed_interval` (e.g. \"30d\")"
        )),
        _ => Err(format!(
            "invalid `calendar_interval` {text:?}: supported values are 1m, 1h, 1d and 1w \
             (minute, hour, day, week); for any other width use `fixed_interval` (e.g. \"5m\", \
             \"12h\", \"7d\")"
        )),
    }
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

    fn dh(json: serde_json::Value) -> Result<(i64, i64), String> {
        serde_json::from_value::<AggDateHistogram>(json)
            .map_err(|e| e.to_string())?
            .resolve()
    }

    #[test]
    fn calendar_intervals_supported_and_rejected() {
        let ok = |ci: &str| dh(serde_json::json!({"field": "t", "calendar_interval": ci}));
        assert_eq!(ok("1m"), Ok((60_000, 0)));
        assert_eq!(ok("hour"), Ok((3_600_000, 0)));
        assert_eq!(ok("1d"), Ok((MS_PER_DAY, 0)));
        assert_eq!(ok("1w"), Ok((MS_PER_WEEK, MONDAY_OFFSET_MS)));
        for bad in ["1M", "month", "1q", "1y", "year", "2d", "banana"] {
            let err = ok(bad).unwrap_err();
            assert!(err.contains("fixed_interval"), "{bad}: {err}");
        }
        let err = dh(serde_json::json!({"field": "t"})).unwrap_err();
        assert!(err.contains("fixed_interval"), "{err}");
        let err = dh(
            serde_json::json!({"field": "t", "fixed_interval": "1h", "calendar_interval": "1h"}),
        )
        .unwrap_err();
        assert!(err.contains("not both"), "{err}");
    }

    #[test]
    fn offsets_parse() {
        let off = |o: serde_json::Value| {
            dh(serde_json::json!({"field": "t", "fixed_interval": "1d", "offset": o}))
        };
        assert_eq!(off("+6h".into()), Ok((MS_PER_DAY, 21_600_000)));
        assert_eq!(off("6h".into()), Ok((MS_PER_DAY, 21_600_000)));
        assert_eq!(off("-30m".into()), Ok((MS_PER_DAY, -1_800_000)));
        assert_eq!(off("0".into()), Ok((MS_PER_DAY, 0)));
        assert_eq!(off(1500.into()), Ok((MS_PER_DAY, 1500)));
        assert!(off("tomorrow".into()).unwrap_err().contains("offset"));
        assert!(off("+".into()).is_err());
    }

    #[test]
    fn histogram_validation() {
        let h = |i: f64, o: f64| AggHistogram {
            field: "f".into(),
            interval: i,
            offset: o,
            min_doc_count: 0,
        };
        assert!(h(1.0, 0.0).validate().is_ok());
        assert!(h(0.5, -3.0).validate().is_ok());
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(h(bad, 0.0).validate().is_err(), "{bad}");
        }
        assert!(h(1.0, f64::NAN).validate().is_err());
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
}
