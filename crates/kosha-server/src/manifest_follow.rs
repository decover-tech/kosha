//! Query replicas following published manifests (#232).
//!
//! A query-role pod (`KOSHA_INGEST_HOST` set) serves searches from its own
//! in-memory manifests, which the ingest pod publishes to the control store.
//! A background refresher (`AppState::spawn_manifest_refresher`) polls every
//! namespace's stored version in one query every `KOSHA_MANIFEST_REFRESH_MS`
//! and installs the namespaces that advanced. This module is the
//! bookkeeping half: when each namespace was last known to be current, how
//! far behind it may be, and the control-plane error counters `/v1/stats`
//! reports.
//!
//! Staleness ("lag") of a namespace is the time since the last moment its
//! local manifest was confirmed to be at least the stored version. While
//! the control plane is unreachable nothing is confirmed, so lag grows and
//! the pod keeps serving its last-known-good manifest (DESIGN.md §17).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use kosha_core::NamespaceId;

/// Default refresher cadence (`KOSHA_MANIFEST_REFRESH_MS`).
pub const DEFAULT_REFRESH_MS: u64 = 500;

/// At most one control-plane warning per this window; the rest are counted.
const WARN_EVERY: Duration = Duration::from_secs(10);

/// `KOSHA_MANIFEST_REFRESH_MS` (default 500; `0` disables the background
/// refresher, and query pods then check the control store before every
/// search, the pre-#232 behavior).
pub fn refresh_interval_from_env() -> Option<Duration> {
    parse_refresh_interval(std::env::var("KOSHA_MANIFEST_REFRESH_MS").ok().as_deref())
}

fn parse_refresh_interval(raw: Option<&str>) -> Option<Duration> {
    let ms = match raw.map(str::trim) {
        None | Some("") => DEFAULT_REFRESH_MS,
        Some(raw) => raw.parse().unwrap_or_else(|_| {
            eprintln!(
                "warn: KOSHA_MANIFEST_REFRESH_MS={raw:?} is not an integer; \
                 using {DEFAULT_REFRESH_MS}"
            );
            DEFAULT_REFRESH_MS
        }),
    };
    (ms > 0).then(|| Duration::from_millis(ms))
}

/// Parse the `max_staleness_seconds` request knob. `Ok(None)` when absent.
pub fn parse_max_staleness(value: Option<&serde_json::Value>) -> Result<Option<Duration>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let seconds = value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.trim().parse().ok()))
        .ok_or_else(|| "max_staleness_seconds must be a number".to_string())?;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err("max_staleness_seconds must be a non-negative number".into());
    }
    Ok(Some(Duration::from_secs_f64(seconds.min(1e9))))
}

#[derive(Clone, Debug)]
pub struct FollowStatus {
    /// The stored version as of the last successful read.
    pub stored_version: u64,
    /// Wall clock of the last confirmation, for `/v1/stats`.
    pub last_refresh_at: SystemTime,
    /// Monotonic clock of the last confirmation, for lag.
    confirmed: Instant,
}

pub struct ManifestFollow {
    /// Background refresher cadence; `None` when disabled.
    pub interval: Option<Duration>,
    namespaces: Mutex<HashMap<NamespaceId, FollowStatus>>,
    errors_total: AtomicU64,
    consecutive_errors: AtomicU64,
    last_error: Mutex<Option<(SystemTime, Instant, String)>>,
    last_poll_ok_at: Mutex<Option<SystemTime>>,
    warn: Mutex<(Option<Instant>, u64)>,
}

impl ManifestFollow {
    pub fn new(interval: Option<Duration>) -> Self {
        Self {
            interval,
            namespaces: Mutex::new(HashMap::new()),
            errors_total: AtomicU64::new(0),
            consecutive_errors: AtomicU64::new(0),
            last_error: Mutex::new(None),
            last_poll_ok_at: Mutex::new(None),
            warn: Mutex::new((None, 0)),
        }
    }

    /// The namespace's local manifest was at least `stored_version` as of
    /// `observed_at` (the moment just before the version was read).
    pub fn confirm(&self, ns: &NamespaceId, stored_version: u64, observed_at: Instant) {
        let mut map = self.namespaces.lock().unwrap();
        let wall = SystemTime::now()
            .checked_sub(observed_at.elapsed())
            .unwrap_or_else(SystemTime::now);
        match map.get_mut(ns) {
            // A slower refresh that read earlier must not move it backwards.
            Some(status) if status.confirmed >= observed_at => {}
            Some(status) => {
                status.stored_version = status.stored_version.max(stored_version);
                status.confirmed = observed_at;
                status.last_refresh_at = wall;
            }
            None => {
                map.insert(
                    ns.clone(),
                    FollowStatus {
                        stored_version,
                        last_refresh_at: wall,
                        confirmed: observed_at,
                    },
                );
            }
        }
    }

    /// Record a newer stored version seen but not (yet) installed.
    pub fn note_stored_version(&self, ns: &NamespaceId, stored_version: u64) {
        if let Some(status) = self.namespaces.lock().unwrap().get_mut(ns) {
            status.stored_version = status.stored_version.max(stored_version);
        }
    }

    /// Stop following a namespace (deleted from the control store): the
    /// next search on it is a cold load.
    pub fn forget(&self, ns: &NamespaceId) {
        self.namespaces.lock().unwrap().remove(ns);
    }

    /// Time since the namespace was last confirmed current; `None` if never.
    pub fn lag(&self, ns: &NamespaceId, now: Instant) -> Option<Duration> {
        self.namespaces
            .lock()
            .unwrap()
            .get(ns)
            .map(|status| now.saturating_duration_since(status.confirmed))
    }

    pub fn status(&self, ns: &NamespaceId) -> Option<FollowStatus> {
        self.namespaces.lock().unwrap().get(ns).cloned()
    }

    pub fn record_poll_ok(&self) {
        self.consecutive_errors.store(0, Ordering::Relaxed);
        *self.last_poll_ok_at.lock().unwrap() = Some(SystemTime::now());
    }

    /// Count a control-plane error and log it, rate-limited to one line per
    /// `WARN_EVERY` (with how many were suppressed since the last one).
    pub fn record_error(&self, context: &str, error: &dyn std::fmt::Display) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        let consecutive = self.consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
        let message = format!("{context}: {error}");
        *self.last_error.lock().unwrap() = Some((SystemTime::now(), Instant::now(), message));
        let mut warn = self.warn.lock().unwrap();
        let now = Instant::now();
        if warn
            .0
            .is_none_or(|last| now.duration_since(last) >= WARN_EVERY)
        {
            eprintln!(
                "warn: manifest refresh: {context}: {error} — serving last-known-good \
                 manifests ({consecutive} consecutive error(s), {} suppressed since last warning)",
                warn.1
            );
            *warn = (Some(now), 0);
        } else {
            warn.1 += 1;
        }
    }

    /// Whether a control-plane error happened within `window` of `now`: a
    /// search that would refresh synchronously serves last-known-good
    /// instead of queueing behind a control store that is timing out.
    pub fn failing_recently(&self, now: Instant, window: Duration) -> bool {
        self.consecutive_errors.load(Ordering::Relaxed) > 0
            && self
                .last_error
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|(_, at, _)| now.saturating_duration_since(*at) < window)
    }

    /// The pod-level block of `/v1/stats`.
    pub fn summary_json(&self, now: Instant) -> serde_json::Value {
        let max_lag = self
            .namespaces
            .lock()
            .unwrap()
            .values()
            .map(|status| {
                now.saturating_duration_since(status.confirmed)
                    .as_secs_f64()
            })
            .fold(0.0, f64::max);
        let last_error = self.last_error.lock().unwrap();
        serde_json::json!({
            "enabled": self.interval.is_some(),
            "interval_ms": self.interval.map(|d| d.as_millis() as u64),
            "control_plane_errors_total": self.errors_total.load(Ordering::Relaxed),
            "consecutive_errors": self.consecutive_errors.load(Ordering::Relaxed),
            "last_error": last_error.as_ref().map(|(_, _, m)| m.clone()),
            "last_error_at": last_error.as_ref().map(|(at, _, _)| unix_seconds(*at)),
            "last_poll_ok_at": self.last_poll_ok_at.lock().unwrap().map(unix_seconds),
            "max_manifest_lag_seconds": max_lag,
        })
    }
}

pub fn unix_seconds(at: SystemTime) -> f64 {
    at.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_interval_defaults_and_disables() {
        assert_eq!(
            parse_refresh_interval(None),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            parse_refresh_interval(Some("")),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            parse_refresh_interval(Some("250")),
            Some(Duration::from_millis(250))
        );
        assert_eq!(parse_refresh_interval(Some("0")), None);
        assert_eq!(
            parse_refresh_interval(Some("junk")),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn max_staleness_knob_parsing() {
        assert_eq!(parse_max_staleness(None), Ok(None));
        assert_eq!(
            parse_max_staleness(Some(&serde_json::Value::Null)),
            Ok(None)
        );
        assert_eq!(
            parse_max_staleness(Some(&serde_json::json!(0))),
            Ok(Some(Duration::ZERO))
        );
        assert_eq!(
            parse_max_staleness(Some(&serde_json::json!(1.5))),
            Ok(Some(Duration::from_millis(1500)))
        );
        assert_eq!(
            parse_max_staleness(Some(&serde_json::json!("2"))),
            Ok(Some(Duration::from_secs(2)))
        );
        assert!(parse_max_staleness(Some(&serde_json::json!(-1))).is_err());
        assert!(parse_max_staleness(Some(&serde_json::json!("x"))).is_err());
    }

    #[test]
    fn lag_grows_until_confirmed_and_never_moves_backwards() {
        let follow = ManifestFollow::new(Some(Duration::from_millis(500)));
        let ns = NamespaceId("ns".into());
        let t0 = Instant::now();
        assert_eq!(follow.lag(&ns, t0), None);
        follow.confirm(&ns, 3, t0);
        let later = t0 + Duration::from_secs(5);
        assert_eq!(follow.lag(&ns, later), Some(Duration::from_secs(5)));
        // An older observation (a slow concurrent refresh) is ignored.
        follow.confirm(&ns, 2, t0 - Duration::from_secs(1));
        assert_eq!(follow.lag(&ns, later), Some(Duration::from_secs(5)));
        assert_eq!(follow.status(&ns).unwrap().stored_version, 3);
        follow.confirm(&ns, 4, t0 + Duration::from_secs(4));
        assert_eq!(follow.lag(&ns, later), Some(Duration::from_secs(1)));
        follow.note_stored_version(&ns, 9);
        assert_eq!(follow.status(&ns).unwrap().stored_version, 9);
    }

    #[test]
    fn errors_are_counted_and_cleared_by_a_good_poll() {
        let follow = ManifestFollow::new(None);
        follow.record_error("poll", &"connection refused");
        follow.record_error("poll", &"connection refused");
        let now = Instant::now();
        assert!(follow.failing_recently(now, Duration::from_secs(1)));
        let summary = follow.summary_json(now);
        assert_eq!(summary["control_plane_errors_total"], 2);
        assert_eq!(summary["consecutive_errors"], 2);
        follow.record_poll_ok();
        assert!(!follow.failing_recently(now, Duration::from_secs(1)));
        assert_eq!(follow.summary_json(now)["consecutive_errors"], 0);
    }
}
