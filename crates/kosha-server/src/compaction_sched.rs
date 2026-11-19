//! Automatic background compaction (#242, part 1): the scheduler's
//! bookkeeping, separate from the merge itself.
//!
//! Until this existed nothing called `needs_compaction`. Compaction ran only
//! via `POST /v1/admin/compact-namespace` or a CronJob that ships suspended,
//! while the idle-flush timer (#241) cuts about one segment per
//! `KOSHA_FLUSH_MAX_AGE_MS` for any namespace written more slowly than
//! `KOSHA_FLUSH_THRESHOLD` docs per interval. Segment count therefore grew
//! without bound.
//!
//! This module is pure state and selection. It decides *which* namespaces
//! get a tiered pass on a tick and records what every pass (scheduled or
//! requested through the API) did. The thread, the merge and the publish
//! live in `main.rs` (`spawn_compaction_scheduler`, `run_compaction_job`).
//!
//! Rules:
//! - **Global budget**: at most `max_concurrent` passes run at once,
//!   counting API-requested jobs too. A pass hydrates and rewrites segments
//!   on the single write pod, competing with ingest.
//! - **One pass per namespace at a time.** The indexer's per-namespace
//!   compact lock would serialize them anyway; refusing a second job keeps
//!   the job list truthful.
//! - **Fairness**: due namespaces are ordered by when they last finished a
//!   pass (never-compacted first), so a namespace with 1,500 segments can't
//!   starve the others. It gets one pass per turn like everyone else.
//! - **No busy retries**:
//!   - A pass that merged nothing (every candidate is non-local, or over
//!     the byte cap) parks the namespace until its manifest version changes.
//!   - A failed pass backs off exponentially, from one interval up to
//!     [`MAX_FAILURE_BACKOFF`].

use kosha_core::NamespaceId;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Default for `KOSHA_COMPACTION_INTERVAL_MS`: how often the scheduler
/// looks for namespaces that need compaction.
pub const DEFAULT_INTERVAL_MS: u64 = 30_000;
/// Default for `KOSHA_COMPACTION_MAX_CONCURRENT`.
pub const DEFAULT_MAX_CONCURRENT: usize = 1;
/// Cap on the exponential backoff after consecutive failed passes.
pub const MAX_FAILURE_BACKOFF: Duration = Duration::from_secs(600);
/// Finished jobs kept for `GET /v1/admin/compaction-jobs/{id}`.
pub const FINISHED_JOBS_RETAINED: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerConfig {
    /// `None` disables the scheduler (`KOSHA_COMPACTION_INTERVAL_MS=0`).
    pub interval: Option<Duration>,
    pub max_concurrent: usize,
}

impl SchedulerConfig {
    pub fn from_env() -> Self {
        Self::parse(
            std::env::var("KOSHA_COMPACTION_INTERVAL_MS")
                .ok()
                .as_deref(),
            std::env::var("KOSHA_COMPACTION_MAX_CONCURRENT")
                .ok()
                .as_deref(),
        )
    }

    /// Unparsable values fall back to the defaults with a warning rather
    /// than silently disabling compaction. Only an explicit `0` interval
    /// disables it.
    pub fn parse(interval_ms: Option<&str>, max_concurrent: Option<&str>) -> Self {
        let interval_ms = match interval_ms.map(str::trim) {
            None | Some("") => DEFAULT_INTERVAL_MS,
            Some(v) => v.parse::<u64>().unwrap_or_else(|_| {
                eprintln!(
                    "WARN: KOSHA_COMPACTION_INTERVAL_MS={v} not a number of milliseconds — \
                     using {DEFAULT_INTERVAL_MS}"
                );
                DEFAULT_INTERVAL_MS
            }),
        };
        let max_concurrent = match max_concurrent.map(str::trim) {
            None | Some("") => DEFAULT_MAX_CONCURRENT,
            Some(v) => match v.parse::<usize>() {
                Ok(n) if n > 0 => n,
                _ => {
                    eprintln!(
                        "WARN: KOSHA_COMPACTION_MAX_CONCURRENT={v} must be a positive integer — \
                         using {DEFAULT_MAX_CONCURRENT}"
                    );
                    DEFAULT_MAX_CONCURRENT
                }
            },
        };
        Self {
            interval: (interval_ms > 0).then(|| Duration::from_millis(interval_ms)),
            max_concurrent,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Trigger {
    Scheduler,
    Api,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Running,
    Succeeded,
    Failed,
}

/// What one compaction pass did. `merged == false` with no error means
/// there was nothing the pass could merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PassOutcome {
    pub merged: bool,
    pub segments_before: usize,
    pub segments_after: usize,
    pub segments_merged: usize,
    pub max_segment_bytes: u64,
    /// Segments the pass wanted but could not hydrate; they were left
    /// unmerged.
    pub not_hydrated: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobRecord {
    pub job_id: String,
    pub namespace: String,
    pub mode: &'static str,
    pub trigger: Trigger,
    pub state: JobState,
    /// Unix epoch milliseconds.
    pub started_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<PassOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip)]
    started: Option<Instant>,
}

/// Per-namespace counters, reported by `GET /v1/admin/compaction-status`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct NamespaceStats {
    pub passes: u64,
    pub merges: u64,
    pub segments_merged_total: u64,
    pub failures: u64,
    pub consecutive_failures: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_job: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_job: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_finished_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Manifest version at which the last pass found nothing to merge. The
    /// scheduler skips the namespace until its manifest moves past it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_at_version: Option<u64>,
    #[serde(skip)]
    last_finished: Option<Instant>,
    #[serde(skip)]
    retry_after: Option<Instant>,
}

/// What the scheduler needs to know about a namespace on a tick.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub namespace: NamespaceId,
    pub manifest_version: u64,
    pub needs_compaction: bool,
}

#[derive(Default)]
struct Inner {
    jobs: HashMap<String, JobRecord>,
    finished: VecDeque<String>,
    namespaces: HashMap<NamespaceId, NamespaceStats>,
    running: usize,
    next_job: u64,
    ticks: u64,
}

pub struct CompactionRegistry {
    config: SchedulerConfig,
    inner: Mutex<Inner>,
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl CompactionRegistry {
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Register a running job for `namespace`. Returns `Err(job_id)` of the
    /// job already running there; the caller must not start another.
    pub fn try_start(
        &self,
        namespace: &NamespaceId,
        mode: &'static str,
        trigger: Trigger,
        now: Instant,
    ) -> Result<String, String> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(running) = inner
            .namespaces
            .get(namespace)
            .and_then(|s| s.running_job.clone())
        {
            return Err(running);
        }
        inner.next_job += 1;
        let job_id = format!("cj-{}-{}", epoch_ms(), inner.next_job);
        inner.jobs.insert(
            job_id.clone(),
            JobRecord {
                job_id: job_id.clone(),
                namespace: namespace.0.clone(),
                mode,
                trigger,
                state: JobState::Running,
                started_at_ms: epoch_ms(),
                finished_at_ms: None,
                duration_ms: None,
                result: None,
                error: None,
                started: Some(now),
            },
        );
        inner.running += 1;
        inner
            .namespaces
            .entry(namespace.clone())
            .or_default()
            .running_job = Some(job_id.clone());
        Ok(job_id)
    }

    /// Record a finished pass. `manifest_version` is the namespace's version
    /// after the pass (unchanged when nothing merged).
    pub fn finish(
        &self,
        job_id: &str,
        outcome: Result<PassOutcome, String>,
        manifest_version: u64,
        now: Instant,
    ) {
        let interval = self.config.interval.unwrap_or(Duration::from_secs(30));
        let mut inner = self.inner.lock().unwrap();
        let Some(job) = inner.jobs.get_mut(job_id) else {
            return;
        };
        if job.state != JobState::Running {
            return;
        }
        let namespace = NamespaceId(job.namespace.clone());
        job.finished_at_ms = Some(epoch_ms());
        job.duration_ms = job
            .started
            .map(|s| now.saturating_duration_since(s).as_millis() as u64);
        match &outcome {
            Ok(result) => {
                job.state = JobState::Succeeded;
                job.result = Some(result.clone());
            }
            Err(error) => {
                job.state = JobState::Failed;
                job.error = Some(error.clone());
            }
        }
        let finished_at_ms = job.finished_at_ms;
        inner.running = inner.running.saturating_sub(1);
        inner.finished.push_back(job_id.to_string());
        while inner.finished.len() > FINISHED_JOBS_RETAINED {
            if let Some(old) = inner.finished.pop_front() {
                inner.jobs.remove(&old);
            }
        }

        let stats = inner.namespaces.entry(namespace).or_default();
        if stats.running_job.as_deref() == Some(job_id) {
            stats.running_job = None;
        }
        stats.passes += 1;
        stats.last_job = Some(job_id.to_string());
        stats.last_finished = Some(now);
        stats.last_finished_at_ms = finished_at_ms;
        match outcome {
            Ok(result) => {
                stats.consecutive_failures = 0;
                stats.retry_after = None;
                stats.last_error = None;
                if result.merged {
                    stats.merges += 1;
                    stats.segments_merged_total += result.segments_merged as u64;
                    stats.idle_at_version = None;
                } else {
                    stats.idle_at_version = Some(manifest_version);
                }
            }
            Err(error) => {
                stats.failures += 1;
                stats.consecutive_failures += 1;
                stats.last_error = Some(error);
                let shift = stats.consecutive_failures.saturating_sub(1).min(16);
                let backoff = interval
                    .saturating_mul(1u32 << shift)
                    .min(MAX_FAILURE_BACKOFF);
                stats.retry_after = Some(now + backoff);
            }
        }
    }

    /// The namespaces to start a scheduled pass on this tick, in order.
    /// Respects the global budget (`max_concurrent` minus passes already
    /// running), skips namespaces that are running, parked at an unchanged
    /// manifest version, or backing off after a failure, and puts the
    /// namespaces that waited longest first.
    pub fn select_due(&self, candidates: &[Candidate], now: Instant) -> Vec<NamespaceId> {
        let mut inner = self.inner.lock().unwrap();
        inner.ticks += 1;
        let budget = self.config.max_concurrent.saturating_sub(inner.running);
        if budget == 0 {
            return Vec::new();
        }
        let mut due: Vec<(Option<Instant>, &NamespaceId)> = candidates
            .iter()
            .filter(|c| c.needs_compaction)
            .filter_map(|c| {
                let stats = inner.namespaces.get(&c.namespace);
                if let Some(s) = stats {
                    if s.running_job.is_some()
                        || s.idle_at_version == Some(c.manifest_version)
                        || s.retry_after.is_some_and(|t| now < t)
                    {
                        return None;
                    }
                }
                Some((stats.and_then(|s| s.last_finished), &c.namespace))
            })
            .collect();
        // `None` (never finished a pass) sorts first; ties by name for
        // determinism.
        due.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1 .0.cmp(&b.1 .0)));
        due.into_iter()
            .take(budget)
            .map(|(_, ns)| ns.clone())
            .collect()
    }

    pub fn job(&self, job_id: &str) -> Option<JobRecord> {
        self.inner.lock().unwrap().jobs.get(job_id).cloned()
    }

    /// Snapshot for `GET /v1/admin/compaction-status`. `live` adds each
    /// namespace's current segment count and `needs_compaction`.
    pub fn status_json(&self, enabled: bool, live: &[(Candidate, usize)]) -> serde_json::Value {
        let inner = self.inner.lock().unwrap();
        let namespaces: Vec<serde_json::Value> = live
            .iter()
            .map(|(c, segments)| {
                let stats = inner
                    .namespaces
                    .get(&c.namespace)
                    .cloned()
                    .unwrap_or_default();
                serde_json::json!({
                    "namespace": c.namespace.0,
                    "segments": segments,
                    "manifest_version": c.manifest_version,
                    "needs_compaction": c.needs_compaction,
                    "stats": stats,
                })
            })
            .collect();
        let mut recent: Vec<&JobRecord> = inner
            .finished
            .iter()
            .rev()
            .take(20)
            .filter_map(|id| inner.jobs.get(id))
            .collect();
        let mut running: Vec<&JobRecord> = inner
            .jobs
            .values()
            .filter(|j| j.state == JobState::Running)
            .collect();
        running.sort_by(|a, b| a.job_id.cmp(&b.job_id));
        recent.truncate(20);
        serde_json::json!({
            "scheduler": {
                "enabled": enabled,
                "interval_ms": self.config.interval.map(|d| d.as_millis() as u64),
                "max_concurrent": self.config.max_concurrent,
                "running": inner.running,
                "ticks": inner.ticks,
            },
            "namespaces": namespaces,
            "running_jobs": running,
            "recent_jobs": recent,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns(name: &str) -> NamespaceId {
        NamespaceId(name.into())
    }

    fn cand(name: &str, version: u64, needs: bool) -> Candidate {
        Candidate {
            namespace: ns(name),
            manifest_version: version,
            needs_compaction: needs,
        }
    }

    fn registry(max_concurrent: usize) -> CompactionRegistry {
        CompactionRegistry::new(SchedulerConfig {
            interval: Some(Duration::from_secs(30)),
            max_concurrent,
        })
    }

    fn merged(n: usize) -> PassOutcome {
        PassOutcome {
            merged: true,
            segments_before: 40,
            segments_after: 40 - n + 1,
            segments_merged: n,
            max_segment_bytes: 0,
            not_hydrated: Vec::new(),
        }
    }

    fn noop() -> PassOutcome {
        PassOutcome {
            merged: false,
            segments_before: 9,
            segments_after: 9,
            segments_merged: 0,
            max_segment_bytes: 0,
            not_hydrated: Vec::new(),
        }
    }

    #[test]
    fn parse_config_defaults_disable_and_garbage() {
        let d = SchedulerConfig::parse(None, None);
        assert_eq!(d.interval, Some(Duration::from_millis(DEFAULT_INTERVAL_MS)));
        assert_eq!(d.max_concurrent, DEFAULT_MAX_CONCURRENT);
        assert_eq!(SchedulerConfig::parse(Some("0"), None).interval, None);
        assert_eq!(
            SchedulerConfig::parse(Some("5000"), Some("3")),
            SchedulerConfig {
                interval: Some(Duration::from_millis(5000)),
                max_concurrent: 3
            }
        );
        let garbage = SchedulerConfig::parse(Some("soon"), Some("0"));
        assert_eq!(
            garbage.interval,
            Some(Duration::from_millis(DEFAULT_INTERVAL_MS))
        );
        assert_eq!(garbage.max_concurrent, DEFAULT_MAX_CONCURRENT);
    }

    #[test]
    fn select_due_skips_namespaces_that_do_not_need_compaction() {
        let r = registry(4);
        let due = r.select_due(&[cand("a", 1, false), cand("b", 1, true)], Instant::now());
        assert_eq!(due, vec![ns("b")]);
    }

    #[test]
    fn global_budget_counts_running_jobs_including_api_jobs() {
        let r = registry(2);
        let now = Instant::now();
        r.try_start(&ns("manual"), "full", Trigger::Api, now)
            .unwrap();
        let due = r.select_due(
            &[cand("a", 1, true), cand("b", 1, true), cand("c", 1, true)],
            now,
        );
        assert_eq!(due.len(), 1, "one slot left of two");
        r.try_start(&due[0], "tiered", Trigger::Scheduler, now)
            .unwrap();
        assert!(r.select_due(&[cand("b", 1, true)], now).is_empty());
    }

    #[test]
    fn one_job_per_namespace() {
        let r = registry(4);
        let now = Instant::now();
        let first = r
            .try_start(&ns("a"), "tiered", Trigger::Scheduler, now)
            .unwrap();
        assert_eq!(
            r.try_start(&ns("a"), "full", Trigger::Api, now),
            Err(first.clone())
        );
        assert!(r.select_due(&[cand("a", 1, true)], now).is_empty());
        r.finish(&first, Ok(merged(8)), 2, now);
        assert!(r.try_start(&ns("a"), "full", Trigger::Api, now).is_ok());
    }

    /// A namespace with 1,500 segments must not hog the budget: after its
    /// pass, a namespace that has never been compacted goes first.
    #[test]
    fn fairness_orders_by_last_finished_never_run_first() {
        let r = registry(1);
        let t0 = Instant::now();
        let all = [cand("big", 1, true), cand("small", 1, true)];

        let first = r.select_due(&all, t0);
        assert_eq!(first, vec![ns("big")], "ties broken by name");
        let job = r
            .try_start(&first[0], "tiered", Trigger::Scheduler, t0)
            .unwrap();
        r.finish(&job, Ok(merged(32)), 2, t0);

        let all = [cand("big", 2, true), cand("small", 1, true)];
        let second = r.select_due(&all, t0 + Duration::from_secs(1));
        assert_eq!(second, vec![ns("small")]);
        let job = r
            .try_start(&second[0], "tiered", Trigger::Scheduler, t0)
            .unwrap();
        r.finish(&job, Ok(merged(8)), 2, t0 + Duration::from_secs(2));

        let third = r.select_due(&all, t0 + Duration::from_secs(3));
        assert_eq!(third, vec![ns("big")], "big finished longer ago");
    }

    #[test]
    fn noop_pass_parks_namespace_until_manifest_changes() {
        let r = registry(1);
        let now = Instant::now();
        let job = r
            .try_start(&ns("a"), "tiered", Trigger::Scheduler, now)
            .unwrap();
        r.finish(&job, Ok(noop()), 7, now);
        assert!(r.select_due(&[cand("a", 7, true)], now).is_empty());
        assert_eq!(r.select_due(&[cand("a", 8, true)], now), vec![ns("a")]);
    }

    #[test]
    fn failures_back_off_exponentially_and_reset_on_success() {
        let r = registry(1);
        let t0 = Instant::now();
        let fail = |at: Instant| {
            let job = r
                .try_start(&ns("a"), "tiered", Trigger::Scheduler, at)
                .unwrap();
            r.finish(&job, Err("s3 down".into()), 1, at);
        };
        fail(t0);
        let c = [cand("a", 1, true)];
        assert!(r.select_due(&c, t0 + Duration::from_secs(29)).is_empty());
        let t1 = t0 + Duration::from_secs(30);
        assert_eq!(r.select_due(&c, t1), vec![ns("a")]);
        fail(t1);
        assert!(r.select_due(&c, t1 + Duration::from_secs(59)).is_empty());
        let t2 = t1 + Duration::from_secs(60);
        assert_eq!(r.select_due(&c, t2), vec![ns("a")]);

        let job = r
            .try_start(&ns("a"), "tiered", Trigger::Scheduler, t2)
            .unwrap();
        r.finish(&job, Ok(merged(8)), 2, t2);
        assert_eq!(
            r.select_due(&[cand("a", 2, true)], t2),
            vec![ns("a")],
            "success clears the backoff"
        );
    }

    #[test]
    fn backoff_is_capped() {
        let r = registry(1);
        let mut t = Instant::now();
        for _ in 0..20 {
            let job = r
                .try_start(&ns("a"), "tiered", Trigger::Scheduler, t)
                .unwrap();
            r.finish(&job, Err("boom".into()), 1, t);
            t += MAX_FAILURE_BACKOFF;
        }
        assert_eq!(r.select_due(&[cand("a", 1, true)], t), vec![ns("a")]);
    }

    #[test]
    fn job_records_and_status_report_outcomes() {
        let r = registry(1);
        let now = Instant::now();
        let ok = r
            .try_start(&ns("a"), "tiered", Trigger::Scheduler, now)
            .unwrap();
        assert_eq!(r.job(&ok).unwrap().state, JobState::Running);
        r.finish(&ok, Ok(merged(8)), 2, now + Duration::from_millis(1500));
        let rec = r.job(&ok).unwrap();
        assert_eq!(rec.state, JobState::Succeeded);
        assert_eq!(rec.duration_ms, Some(1500));
        assert_eq!(rec.result.unwrap().segments_merged, 8);

        let bad = r.try_start(&ns("b"), "full", Trigger::Api, now).unwrap();
        r.finish(&bad, Err("boom".into()), 1, now);
        let json = serde_json::to_value(r.job(&bad).unwrap()).unwrap();
        assert_eq!(json["state"], "failed");
        assert_eq!(json["trigger"], "api");
        assert_eq!(json["error"], "boom");

        let status = r.status_json(true, &[(cand("a", 2, false), 33)]);
        assert_eq!(status["scheduler"]["running"], 0);
        assert_eq!(status["namespaces"][0]["segments"], 33);
        assert_eq!(status["namespaces"][0]["stats"]["merges"], 1);
        assert_eq!(status["namespaces"][0]["stats"]["segments_merged_total"], 8);
        assert_eq!(status["recent_jobs"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn finished_jobs_are_bounded() {
        let r = registry(1);
        let now = Instant::now();
        let mut first = None;
        for _ in 0..(FINISHED_JOBS_RETAINED + 5) {
            let job = r
                .try_start(&ns("a"), "tiered", Trigger::Scheduler, now)
                .unwrap();
            first.get_or_insert(job.clone());
            r.finish(&job, Ok(noop()), 1, now);
        }
        assert!(r.job(first.as_ref().unwrap()).is_none(), "oldest evicted");
    }
}
