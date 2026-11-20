//! Segment garbage collection settings and counters (#239).
//!
//! The deciding logic lives in `kosha_core::gc` (object-store sweep) and
//! `kosha_write::Indexer::reclaim_local` (local disk). This module holds
//! what the server adds: env configuration, the counters `/v1/stats`
//! reports, and request parsing for `POST /v1/admin/gc`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Deserialize;

/// Default `KOSHA_GC_GRACE`: how long a retired segment or deletion
/// generation stays in the object store after the publish that dropped it.
/// Must exceed the longest a query replica can keep serving an older
/// manifest (it re-checks the version before every search; an idle replica
/// or a long search can hold an older one for longer).
pub const DEFAULT_GRACE: Duration = Duration::from_secs(3600);
/// Default `KOSHA_GC_ORPHAN_GRACE`: how old an object no manifest ever
/// referenced must be before GC deletes it. Long, because an upload whose
/// publish has not committed yet looks exactly like an orphan.
pub const DEFAULT_ORPHAN_GRACE: Duration = Duration::from_secs(24 * 3600);
/// Default `KOSHA_LOCAL_GC_GRACE`: how long a retired segment's local
/// files stay after the publish that dropped it, on top of waiting for every
/// search that could still read them.
pub const DEFAULT_LOCAL_GRACE: Duration = Duration::from_secs(300);
/// Default `KOSHA_LOCAL_GC_INTERVAL_MS`.
pub const DEFAULT_LOCAL_INTERVAL: Duration = Duration::from_secs(60);

/// Parse a duration: plain seconds (`"3600"`) or a number with a unit
/// suffix `ms`, `s`, `m`, `h`, `d` (`"5m"`, `"1h"`).
pub fn parse_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    let split = raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len());
    let (number, unit) = raw.split_at(split);
    let n: u64 = number.parse().ok()?;
    let ms = match unit.trim() {
        "" | "s" => n.checked_mul(1000)?,
        "ms" => n,
        "m" => n.checked_mul(60_000)?,
        "h" => n.checked_mul(3_600_000)?,
        "d" => n.checked_mul(86_400_000)?,
        _ => return None,
    };
    Some(Duration::from_millis(ms))
}

fn env_duration(name: &str, default: Duration) -> Duration {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => parse_duration(&v).unwrap_or_else(|| {
            eprintln!(
                "WARN: {name}={v} is not a duration (e.g. 3600, 90s, 5m, 1h) — using {}s",
                default.as_secs()
            );
            default
        }),
        _ => default,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcSettings {
    pub grace: Duration,
    pub orphan_grace: Duration,
    pub local_grace: Duration,
    /// `None` disables the local reclamation thread
    /// (`KOSHA_LOCAL_GC_INTERVAL_MS=0`).
    pub local_interval: Option<Duration>,
}

impl GcSettings {
    pub fn from_env() -> Self {
        let local_interval = match std::env::var("KOSHA_LOCAL_GC_INTERVAL_MS") {
            Ok(v) if !v.trim().is_empty() => match v.trim().parse::<u64>() {
                Ok(0) => None,
                Ok(ms) => Some(Duration::from_millis(ms)),
                Err(_) => {
                    eprintln!(
                        "WARN: KOSHA_LOCAL_GC_INTERVAL_MS={v} is not a number of milliseconds \
                         — using {}",
                        DEFAULT_LOCAL_INTERVAL.as_millis()
                    );
                    Some(DEFAULT_LOCAL_INTERVAL)
                }
            },
            _ => Some(DEFAULT_LOCAL_INTERVAL),
        };
        Self {
            grace: env_duration("KOSHA_GC_GRACE", DEFAULT_GRACE),
            orphan_grace: env_duration("KOSHA_GC_ORPHAN_GRACE", DEFAULT_ORPHAN_GRACE),
            local_grace: env_duration("KOSHA_LOCAL_GC_GRACE", DEFAULT_LOCAL_GRACE),
            local_interval,
        }
    }
}

/// Counters for `/v1/stats` and the GC response.
#[derive(Debug, Default)]
pub struct GcStats {
    pub bytes_reclaimed: AtomicU64,
    pub segments_deleted: AtomicU64,
    pub deletion_vectors_deleted: AtomicU64,
    pub runs: AtomicU64,
    pub last_run_ms: AtomicU64,
    pub local_bytes_reclaimed: AtomicU64,
    pub local_segments_removed: AtomicU64,
    pub local_deletion_vectors_removed: AtomicU64,
}

impl GcStats {
    pub fn record_run(&self, report: &kosha_core::gc::NamespaceGcReport) {
        self.bytes_reclaimed
            .fetch_add(report.bytes_reclaimed, Ordering::Relaxed);
        self.segments_deleted
            .fetch_add(report.segments_deleted as u64, Ordering::Relaxed);
        self.deletion_vectors_deleted
            .fetch_add(report.deletion_vectors_deleted as u64, Ordering::Relaxed);
    }

    pub fn record_local(&self, report: &kosha_write::LocalReclaimReport) {
        self.local_bytes_reclaimed
            .fetch_add(report.bytes_removed, Ordering::Relaxed);
        self.local_segments_removed
            .fetch_add(report.segments_removed as u64, Ordering::Relaxed);
        self.local_deletion_vectors_removed
            .fetch_add(report.deletion_vectors_removed as u64, Ordering::Relaxed);
    }

    /// Metric-named counters and gauges. `retired_segments` and
    /// `local_pending` are gauges the caller reads from the indexer.
    pub fn to_json(&self, retired_segments: usize, local_pending: usize) -> serde_json::Value {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        serde_json::json!({
            "kosha_gc_bytes_reclaimed_total": load(&self.bytes_reclaimed),
            "kosha_gc_segments_deleted_total": load(&self.segments_deleted),
            "kosha_gc_deletion_vectors_deleted_total": load(&self.deletion_vectors_deleted),
            "kosha_gc_runs_total": load(&self.runs),
            "kosha_gc_last_run_ms": load(&self.last_run_ms),
            "kosha_retired_segments": retired_segments,
            "kosha_local_gc_pending": local_pending,
            "kosha_local_gc_bytes_reclaimed_total": load(&self.local_bytes_reclaimed),
            "kosha_local_gc_segments_removed_total": load(&self.local_segments_removed),
            "kosha_local_gc_deletion_vectors_removed_total":
                load(&self.local_deletion_vectors_removed),
        })
    }
}

/// `POST /v1/admin/gc` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcRequest {
    /// One namespace; every namespace when absent.
    #[serde(default)]
    pub namespace: Option<String>,
    /// Report without deleting. Defaults to `true`: deleting is opt-in.
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
    /// Override `KOSHA_GC_GRACE` for this run.
    #[serde(default)]
    pub grace_seconds: Option<u64>,
    /// Override `KOSHA_GC_ORPHAN_GRACE` for this run.
    #[serde(default)]
    pub orphan_grace_seconds: Option<u64>,
}

fn default_dry_run() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse_with_and_without_units() {
        assert_eq!(parse_duration("3600"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("90s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("2d"), Some(Duration::from_secs(172_800)));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("0"), Some(Duration::ZERO));
        assert_eq!(parse_duration("h"), None);
        assert_eq!(parse_duration("5 weeks"), None);
        assert_eq!(parse_duration("-1"), None);
    }

    #[test]
    fn gc_requests_default_to_dry_run() {
        let req: GcRequest = serde_json::from_str("{}").unwrap();
        assert!(req.dry_run);
        assert!(req.namespace.is_none());
        let req: GcRequest =
            serde_json::from_str(r#"{"namespace":"t/ns","dry_run":false}"#).unwrap();
        assert!(!req.dry_run);
        assert!(serde_json::from_str::<GcRequest>(r#"{"dryrun":false}"#).is_err());
    }
}
