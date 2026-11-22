//! Date values as numbers.
//!
//! `Date` fields are stored in the segment's string filter columns exactly
//! as they were written (`filters.bin` has no date column type), so numeric
//! work over them — the `min` / `max` / `sum` / `avg` aggregations — parses
//! the stored text into milliseconds since the Unix epoch, the numeric
//! representation Elasticsearch uses for dates, and formats results back
//! for `value_as_string`.
//!
//! Accepted forms (the ISO-8601 / RFC 3339 subset Kosha's writers produce):
//! `YYYY-MM-DD`, optionally followed by `T` (or a space) and
//! `HH:MM[:SS[.fraction]]`, optionally followed by `Z` or a `±HH:MM` /
//! `±HHMM` / `±HH` offset. A time without an offset is taken as UTC.

/// Days from 1970-01-01 to the given proleptic Gregorian date (Howard
/// Hinnant's `days_from_civil`).
pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = i64::from(month);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`].
pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        _ => 28,
    }
}

/// Parse `n` ASCII digits at the start of `s`.
fn digits(s: &[u8], n: usize) -> Option<u32> {
    if s.len() < n {
        return None;
    }
    let mut v = 0u32;
    for &b in &s[..n] {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u32::from(b - b'0');
    }
    Some(v)
}

/// Milliseconds since the Unix epoch for a date / date-time string, or
/// `None` when the text is not a date in one of the accepted forms (see the
/// module docs).
pub fn parse_epoch_millis(text: &str) -> Option<i64> {
    let s = text.trim().as_bytes();
    let year = i64::from(digits(s, 4)?);
    if s.get(4) != Some(&b'-') || s.get(7) != Some(&b'-') {
        return None;
    }
    let month = digits(&s[5..], 2)?;
    let day = digits(&s[8..], 2)?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    let mut millis = days_from_civil(year, month, day) * 86_400_000;
    let mut rest = &s[10..];
    if rest.is_empty() {
        return Some(millis);
    }
    if rest[0] != b'T' && rest[0] != b't' && rest[0] != b' ' {
        return None;
    }
    rest = &rest[1..];
    let hour = digits(rest, 2)?;
    if rest.get(2) != Some(&b':') {
        return None;
    }
    let minute = digits(&rest[3..], 2)?;
    rest = &rest[5..];
    let mut second = 0;
    let mut frac_millis = 0i64;
    if rest.first() == Some(&b':') {
        second = digits(&rest[1..], 2)?;
        rest = &rest[3..];
        if rest.first() == Some(&b'.') || rest.first() == Some(&b',') {
            rest = &rest[1..];
            let n = rest.iter().take_while(|b| b.is_ascii_digit()).count();
            if n == 0 {
                return None;
            }
            // Milliseconds: the first three digits, truncated.
            let mut scale = 100;
            for &b in rest.iter().take(n.min(3)) {
                frac_millis += i64::from(b - b'0') * scale;
                scale /= 10;
            }
            rest = &rest[n..];
        }
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    millis +=
        (i64::from(hour) * 3600 + i64::from(minute) * 60 + i64::from(second)) * 1000 + frac_millis;
    match rest {
        [] | [b'Z'] | [b'z'] => Some(millis),
        [sign @ (b'+' | b'-'), offset @ ..] => {
            let oh = digits(offset, 2)?;
            let om = match &offset[2..] {
                [] => 0,
                [b':', m @ ..] if m.len() == 2 => digits(m, 2)?,
                m if m.len() == 2 => digits(m, 2)?,
                _ => return None,
            };
            if oh > 23 || om > 59 {
                return None;
            }
            let offset_millis = (i64::from(oh) * 60 + i64::from(om)) * 60_000;
            // Local time = UTC + offset, so UTC = local - offset.
            Some(if *sign == b'+' {
                millis - offset_millis
            } else {
                millis + offset_millis
            })
        }
        _ => None,
    }
}

/// `YYYY-MM-DDTHH:MM:SS.sssZ` for milliseconds since the Unix epoch — the
/// form Elasticsearch gives a date aggregation's `value_as_string`.
pub fn format_epoch_millis(millis: i64) -> String {
    let days = millis.div_euclid(86_400_000);
    let in_day = millis.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    let (h, m, s, ms) = (
        in_day / 3_600_000,
        in_day / 60_000 % 60,
        in_day / 1000 % 60,
        in_day % 1000,
    );
    format!("{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}.{ms:03}Z")
}

/// Like [`format_epoch_millis`], but as the wall-clock time at UTC offset
/// `offset_millis`, with that offset written out:
/// `2024-03-10T00:00:00.000-08:00`. A zero offset is written `Z`, so the
/// output is then exactly [`format_epoch_millis`]'s. An offset with
/// seconds (historical local mean time) is written `±HH:MM:SS`.
pub fn format_epoch_millis_at_offset(millis: i64, offset_millis: i64) -> String {
    if offset_millis == 0 {
        return format_epoch_millis(millis);
    }
    let mut text = format_epoch_millis(millis.saturating_add(offset_millis));
    text.pop(); // the `Z`
    let sign = if offset_millis < 0 { '-' } else { '+' };
    let secs = (offset_millis / 1000).unsigned_abs();
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    if s == 0 {
        text.push_str(&format!("{sign}{h:02}:{m:02}"));
    } else {
        text.push_str(&format!("{sign}{h:02}:{m:02}:{s:02}"));
    }
    text
}

/// Whether a date string [`parse_epoch_millis`] accepts names its own UTC
/// offset (`Z`, `+05:30`, ...), as opposed to being a bare local date or
/// date-time.
pub fn has_explicit_offset(text: &str) -> bool {
    text.trim()
        .get(10..)
        .is_some_and(|rest| rest.contains(['Z', 'z', '+', '-']))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dates_and_date_times() {
        assert_eq!(parse_epoch_millis("1970-01-01"), Some(0));
        assert_eq!(parse_epoch_millis("1970-01-02"), Some(86_400_000));
        assert_eq!(
            parse_epoch_millis("2024-06-15T00:00:00Z"),
            Some(1_718_409_600_000)
        );
        assert_eq!(
            parse_epoch_millis("2024-06-15T02:30:00+02:30"),
            Some(1_718_409_600_000)
        );
        assert_eq!(
            parse_epoch_millis("2024-06-14T19:00:00-0500"),
            Some(1_718_409_600_000)
        );
        assert_eq!(
            parse_epoch_millis("2024-06-15 00:00:01.25"),
            Some(1_718_409_601_250)
        );
        assert_eq!(parse_epoch_millis("2024-02-29"), Some(1_709_164_800_000));
        assert_eq!(parse_epoch_millis("1969-12-31T23:59:59.999Z"), Some(-1));
    }

    #[test]
    fn rejects_non_dates() {
        for bad in [
            "",
            "2024",
            "12.5",
            "2023-02-29",
            "2024-13-01",
            "2024-06-15T25:00",
            "2024-06-15Tnoon",
            "2024-06-15T00:00:00Q",
            "hello",
        ] {
            assert_eq!(parse_epoch_millis(bad), None, "{bad}");
        }
    }

    #[test]
    fn formats_round_trip() {
        for text in [
            "1970-01-01T00:00:00.000Z",
            "2024-06-15T12:34:56.789Z",
            "1969-12-31T23:59:59.999Z",
            "2000-02-29T00:00:00.000Z",
        ] {
            let millis = parse_epoch_millis(text).unwrap();
            assert_eq!(format_epoch_millis(millis), text);
        }
    }

    #[test]
    fn formats_at_an_offset() {
        let t = parse_epoch_millis("2024-03-10T08:00:00Z").unwrap();
        assert_eq!(
            format_epoch_millis_at_offset(t, -8 * 3_600_000),
            "2024-03-10T00:00:00.000-08:00"
        );
        assert_eq!(
            format_epoch_millis_at_offset(t, 19_800_000),
            "2024-03-10T13:30:00.000+05:30"
        );
        assert_eq!(
            format_epoch_millis_at_offset(t, 0),
            "2024-03-10T08:00:00.000Z"
        );
        assert_eq!(
            format_epoch_millis_at_offset(t, -(4 * 3600 + 56 * 60 + 2) * 1000),
            "2024-03-10T03:03:58.000-04:56:02"
        );
        assert!(has_explicit_offset("2024-03-10T08:00:00Z"));
        assert!(has_explicit_offset("2024-03-10T08:00:00-05:00"));
        assert!(!has_explicit_offset("2024-03-10T08:00:00"));
        assert!(!has_explicit_offset("2024-03-10"));
    }

    #[test]
    fn civil_round_trip_including_leap_years_and_pre_epoch() {
        for (y, m, d) in [
            (1970, 1, 1),
            (1969, 12, 31),
            (2000, 2, 29),
            (1900, 2, 28),
            (1900, 3, 1),
            (2024, 2, 29),
            (2023, 3, 1),
            (1600, 1, 1),
            (-1, 12, 31),
        ] {
            assert_eq!(civil_from_days(days_from_civil(y, m, d)), (y, m, d));
        }
        assert_eq!(
            days_from_civil(1900, 3, 1) - days_from_civil(1900, 2, 28),
            1
        );
        assert_eq!(
            days_from_civil(2000, 3, 1) - days_from_civil(2000, 2, 28),
            2
        );
    }
}
