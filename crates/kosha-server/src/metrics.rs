//! Prometheus metrics for `GET /metrics` (#223).
//!
//! One [`Metrics`] lives on `AppState`. It owns a `prometheus_client`
//! [`Registry`] and typed handles for every series the request path
//! updates. Every handle the search path touches is resolved once, here,
//! at construction, so recording a sample is an atomic add: no family
//! lookup, no lock. The histogram is our own [`AtomicHistogram`] because
//! `prometheus_client`'s histogram takes a mutex per observation.
//!
//! Values the server already keeps as atomics elsewhere (cache bytes, NVMe
//! evictions, checksum failures, the search queue's waiters) are not
//! double-counted: the scrape handler reads them into a [`Scrape`] and the
//! registered [`ScrapeCollector`] encodes that snapshot.
//!
//! The metric set and its labels are documented in `docs/metrics.md`; keep
//! the two in step.

// Hydration and lease recording is reached only from the S3 and Postgres
// code paths; builds without them still export those series, at zero.
#![cfg_attr(not(all(feature = "s3", feature = "postgres")), allow(dead_code))]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric, MetricEncoder, NoLabelSet};
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::metrics::{MetricType, TypedMetric};
use prometheus_client::registry::Registry;

/// `content-type` of the Prometheus text exposition format, version 0.0.4.
pub(crate) const CONTENT_TYPE: &str = prometheus_client::encoding::prometheus_text::CONTENT_TYPE;

/// Histogram upper bounds, in seconds. Chosen for this service's range: a
/// warm search answers in well under a millisecond, a cold one that
/// hydrates from S3 takes hundreds of milliseconds to minutes. The
/// Prometheus defaults (5 ms .. 10 s) lose both ends.
pub(crate) const LATENCY_BUCKETS: [f64; 17] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0, 300.0,
];

/// Who may read `GET /metrics` (`KOSHA_METRICS`, `KOSHA_METRICS_AUTH`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Exposure {
    /// `KOSHA_METRICS=off`: the route answers 404.
    Off,
    /// The default: no credential, like `/healthz` and `/readyz`.
    Open,
    /// `KOSHA_METRICS_AUTH=admin`: the admin credential is required.
    Admin,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MetricsConfig {
    pub exposure: Exposure,
    /// `KOSHA_METRICS_NAMESPACE_LABELS=on`: label
    /// `kosha_search_requests_total` by namespace as well as outcome.
    pub namespace_labels: bool,
    /// `KOSHA_METRICS_TENANT_LABELS=on`: label
    /// `kosha_tenant_rejections_total` by tenant as well as reason (#341),
    /// and the `kosha_cache_*` families by tenant as well as cache (#342).
    pub tenant_labels: bool,
}

impl MetricsConfig {
    pub(crate) fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok();
        Self {
            tenant_labels: flag_on(var("KOSHA_METRICS_TENANT_LABELS").as_deref()),
            ..Self::parse(
                var("KOSHA_METRICS").as_deref(),
                var("KOSHA_METRICS_AUTH").as_deref(),
                var("KOSHA_METRICS_NAMESPACE_LABELS").as_deref(),
            )
        }
    }

    fn parse(enabled: Option<&str>, auth: Option<&str>, namespace_labels: Option<&str>) -> Self {
        let is = |v: Option<&str>, words: &[&str]| {
            v.map(|v| v.trim().to_ascii_lowercase())
                .is_some_and(|v| words.contains(&v.as_str()))
        };
        let exposure = if is(enabled, &["off", "false", "0"]) {
            Exposure::Off
        } else {
            match auth.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
                None | Some("") | Some("none") | Some("off") => Exposure::Open,
                Some("admin") => Exposure::Admin,
                Some(other) => {
                    // Fail closed: a typo must not publish what the operator
                    // meant to protect.
                    eprintln!(
                        "WARN: KOSHA_METRICS_AUTH={other:?} is not `none` or `admin`; \
                         requiring the admin credential for /metrics"
                    );
                    Exposure::Admin
                }
            }
        };
        Self {
            exposure,
            namespace_labels: flag_on(namespace_labels),
            tenant_labels: false,
        }
    }
}

/// `on`, `true` or `1`, in any case.
fn flag_on(value: Option<&str>) -> bool {
    value
        .map(|v| v.trim().to_ascii_lowercase())
        .is_some_and(|v| ["on", "true", "1"].contains(&v.as_str()))
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            exposure: Exposure::Open,
            namespace_labels: false,
            tenant_labels: false,
        }
    }
}

// ─── Atomic histogram ───────────────────────────────────────────────────────

/// A fixed-bucket histogram over [`LATENCY_BUCKETS`] whose `observe` is two
/// relaxed atomic adds. Clones share their counts.
#[derive(Clone, Debug, Default)]
pub(crate) struct AtomicHistogram {
    inner: Arc<HistogramInner>,
}

#[derive(Debug, Default)]
struct HistogramInner {
    /// Per-bucket (not cumulative) counts; the last slot is `+Inf`.
    buckets: [AtomicU64; LATENCY_BUCKETS.len() + 1],
    /// Sum of observations in nanoseconds, so it can be an integer atomic.
    sum_nanos: AtomicU64,
}

impl AtomicHistogram {
    pub(crate) fn observe_secs(&self, secs: f64) {
        let secs = if secs.is_finite() { secs.max(0.0) } else { 0.0 };
        let idx = LATENCY_BUCKETS
            .iter()
            .position(|bound| secs <= *bound)
            .unwrap_or(LATENCY_BUCKETS.len());
        self.inner.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.inner
            .sum_nanos
            .fetch_add((secs * 1e9) as u64, Ordering::Relaxed);
    }

    pub(crate) fn observe_ms(&self, ms: f64) {
        self.observe_secs(ms / 1e3);
    }
}

impl TypedMetric for AtomicHistogram {
    const TYPE: MetricType = MetricType::Histogram;
}

impl EncodeMetric for AtomicHistogram {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        let counts: Vec<u64> = self
            .inner
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        let buckets: Vec<(f64, u64)> = LATENCY_BUCKETS
            .iter()
            .copied()
            .chain(std::iter::once(f64::INFINITY))
            .zip(counts.iter().copied())
            .collect();
        // `_count` is the bucket total, so it always equals the `+Inf`
        // bucket even while observations land mid-scrape.
        let count = counts.iter().sum();
        let sum = self.inner.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9;
        encoder.encode_histogram::<NoLabelSet>(sum, count, &buckets, None)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}

// ─── Typed handles ──────────────────────────────────────────────────────────

type Labels = Vec<(&'static str, String)>;

fn labels(pairs: &[(&'static str, &str)]) -> Labels {
    pairs.iter().map(|(k, v)| (*k, (*v).to_string())).collect()
}

/// `kosha_search_requests_total{outcome}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SearchOutcome {
    Ok,
    /// Shed with HTTP 429: the scoring queue was full, or the request hit a
    /// hydration or memory ceiling.
    Rejected,
    /// Shed with HTTP 429 after waiting `KOSHA_SEARCH_QUEUE_TIMEOUT_MS` for
    /// a scoring slot.
    Timeout,
    /// Any other non-200 answer (bad request, missing namespace, storage
    /// failure).
    Error,
}

impl SearchOutcome {
    const ALL: [SearchOutcome; 4] = [Self::Ok, Self::Rejected, Self::Timeout, Self::Error];

    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Rejected => "rejected",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }

    /// Classify a finished search from its response and why, if at all, the
    /// scoring queue shed it.
    pub(crate) fn classify(response: &str, shed: Option<QueueShed>) -> Self {
        match (response.get(9..12).unwrap_or(""), shed) {
            (_, Some(QueueShed::Timeout)) => Self::Timeout,
            (_, Some(QueueShed::Full | QueueShed::TenantFull)) => Self::Rejected,
            ("200", None) => Self::Ok,
            ("429", None) => Self::Rejected,
            _ => Self::Error,
        }
    }
}

/// Why the search scoring queue shed a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueueShed {
    /// `KOSHA_SEARCH_QUEUE_DEPTH` requests were already waiting.
    Full,
    /// The request's tenant already had
    /// `KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT` requests waiting.
    TenantFull,
    /// The request waited `KOSHA_SEARCH_QUEUE_TIMEOUT_MS` without a slot.
    Timeout,
}

/// Where a hydrated segment file came from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HydrationSource {
    S3,
    Peer,
    /// Already in the local cache when the fetch ran.
    Local,
}

/// Result of one cross-replica hydration lease claim.
#[derive(Clone, Copy, Debug)]
pub(crate) enum LeaseOutcome {
    /// This replica owns the fetch.
    Won,
    /// Another replica owned it and served the bytes.
    StreamedFromPeer,
    /// Another replica owned it but did not serve the bytes within the
    /// peer-fetch budget (or the fetch failed); the file came from S3.
    Timeout,
}

/// One tenant's figures in one cache (one of [`CACHES`]).
#[derive(Clone, Debug)]
pub(crate) struct CacheTenantSample {
    pub cache: &'static str,
    pub stats: kosha_core::tenancy::TenantCacheStats,
}

/// The caches `kosha_cache_*` reports, in exposition order.
const CACHES: [&str; 5] = ["nvme", "segment", "result", "corpus_stats", "ceiling"];

/// Caches bounded by entry count, whose bytes are not tracked: they report
/// hits, misses and evictions but no `kosha_cache_bytes` series.
const ENTRY_BOUNDED_CACHES: [&str; 2] = ["corpus_stats", "ceiling"];

/// Point-in-time values read from the server's own atomics at scrape time.
#[derive(Clone, Debug, Default)]
pub(crate) struct Scrape {
    pub search_queue_depth: u64,
    pub nvme_cache_bytes: u64,
    pub segment_cache_bytes: u64,
    pub nvme_evictions: u64,
    pub segment_checksum_failures: u64,
    /// Per-tenant bytes, hits, misses and evictions of every cache (#342).
    pub cache_tenants: Vec<CacheTenantSample>,
    /// The background span writer's totals (`span_writes`).
    pub span_writes: crate::span_writes::SpanWriteStats,
}

/// Encodes the latest [`Scrape`]. Counters only move forward (`fetch_max`)
/// so two overlapping scrapes can never publish a decrease.
#[derive(Debug, Default)]
struct ScrapeCollector {
    queue_depth: AtomicU64,
    nvme_bytes: AtomicU64,
    segment_bytes: AtomicU64,
    nvme_evictions: AtomicU64,
    checksum_failures: AtomicU64,
    /// `kosha_span_writes` by outcome: written, dropped, inline, failed.
    span_writes: [AtomicU64; 4],
    span_dropped_bytes: AtomicU64,
    span_queued_bytes: AtomicU64,
    span_held_bytes: AtomicU64,
    cache_tenants: Mutex<Vec<CacheTenantSample>>,
    /// Label the `kosha_cache_*` families by tenant (#342).
    tenant_labels: bool,
}

impl ScrapeCollector {
    fn store(&self, s: &Scrape) {
        *self.cache_tenants.lock().unwrap_or_else(|e| e.into_inner()) = s.cache_tenants.clone();
        self.queue_depth
            .store(s.search_queue_depth, Ordering::Relaxed);
        self.nvme_bytes.store(s.nvme_cache_bytes, Ordering::Relaxed);
        self.segment_bytes
            .store(s.segment_cache_bytes, Ordering::Relaxed);
        self.nvme_evictions
            .fetch_max(s.nvme_evictions, Ordering::Relaxed);
        self.checksum_failures
            .fetch_max(s.segment_checksum_failures, Ordering::Relaxed);
        let w = &s.span_writes;
        for (counter, value) in self
            .span_writes
            .iter()
            .zip([w.written, w.dropped, w.inline, w.failed])
        {
            counter.fetch_max(value, Ordering::Relaxed);
        }
        self.span_dropped_bytes
            .fetch_max(w.dropped_bytes, Ordering::Relaxed);
        self.span_queued_bytes
            .store(w.queued_bytes, Ordering::Relaxed);
        self.span_held_bytes.store(w.held_bytes, Ordering::Relaxed);
    }
}

impl Collector for ScrapeCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);

        let depth = ConstGauge::new(load(&self.queue_depth));
        depth.encode(encoder.encode_descriptor(
            "kosha_search_queue_depth",
            "Searches waiting for a scoring slot.",
            None,
            depth.metric_type(),
        )?)?;

        let tenants = self
            .cache_tenants
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        // One family per figure. With tenant labels, one series per cache
        // and tenant; without, one per cache, summed over tenants — except
        // the NVMe and segment bytes and NVMe evictions, which come from the
        // caches' own totals as they did before #342.
        type Pick = fn(&kosha_core::tenancy::TenantCacheStats) -> u64;
        let families: [(&str, &str, MetricType, Pick); 4] = [
            (
                "kosha_cache_bytes",
                "Bytes held by each cache.",
                MetricType::Gauge,
                |s| s.bytes,
            ),
            (
                "kosha_cache_evictions",
                "Entries evicted from each cache (files or directories for nvme).",
                MetricType::Counter,
                |s| s.evictions,
            ),
            (
                "kosha_cache_hits",
                "Cache lookups that found their entry.",
                MetricType::Counter,
                |s| s.hits,
            ),
            (
                "kosha_cache_misses",
                "Cache lookups that did not find their entry.",
                MetricType::Counter,
                |s| s.misses,
            ),
        ];
        for (name, help, metric_type, pick) in families {
            let mut family = encoder.encode_descriptor(name, help, None, metric_type)?;
            for cache in CACHES {
                if name == "kosha_cache_bytes" && ENTRY_BOUNDED_CACHES.contains(&cache) {
                    continue;
                }
                if self.tenant_labels {
                    for t in tenants.iter().filter(|t| t.cache == cache) {
                        let set = [("cache", cache), ("tenant", t.stats.tenant.as_str())];
                        encode_value(&mut family, &set, metric_type, pick(&t.stats))?;
                    }
                    continue;
                }
                let value = match (name, cache) {
                    ("kosha_cache_bytes", "nvme") => load(&self.nvme_bytes),
                    ("kosha_cache_bytes", "segment") => load(&self.segment_bytes),
                    ("kosha_cache_evictions", "nvme") => load(&self.nvme_evictions),
                    _ => tenants
                        .iter()
                        .filter(|t| t.cache == cache)
                        .map(|t| pick(&t.stats))
                        .sum(),
                };
                encode_value(&mut family, &[("cache", cache)], metric_type, value)?;
            }
        }

        let failures = ConstCounter::new(load(&self.checksum_failures));
        failures.encode(encoder.encode_descriptor(
            "kosha_segment_checksum_failures",
            "Hydrated segment files that failed their _COMMIT size or CRC check.",
            None,
            failures.metric_type(),
        )?)?;

        let mut family = encoder.encode_descriptor(
            "kosha_span_writes",
            "Fetched spans by what became of their write: written by the background \
             writer, dropped (queue full; held by the fetching request), written inline \
             (queue and held cap full), or failed.",
            None,
            MetricType::Counter,
        )?;
        for (outcome, value) in ["written", "dropped", "inline", "failed"]
            .into_iter()
            .zip(&self.span_writes)
        {
            encode_value(
                &mut family,
                &[("outcome", outcome)],
                MetricType::Counter,
                load(value),
            )?;
        }
        let dropped = ConstCounter::new(load(&self.span_dropped_bytes));
        dropped.encode(encoder.encode_descriptor(
            "kosha_span_writes_dropped_bytes",
            "Bytes of span writes dropped because the write queue was full.",
            None,
            dropped.metric_type(),
        )?)?;
        for (name, help, value) in [
            (
                "kosha_span_write_queue_bytes",
                "Span bytes queued for the background writer.",
                &self.span_queued_bytes,
            ),
            (
                "kosha_span_held_bytes",
                "Bytes of dropped span writes held in memory by the requests that fetched them.",
                &self.span_held_bytes,
            ),
        ] {
            let gauge = ConstGauge::new(load(value));
            gauge.encode(encoder.encode_descriptor(name, help, None, gauge.metric_type())?)?;
        }
        Ok(())
    }
}

/// One sample of a gauge or counter family under `labels`.
fn encode_value(
    family: &mut prometheus_client::encoding::MetricEncoder<'_>,
    labels: &[(&str, &str)],
    metric_type: MetricType,
    value: u64,
) -> Result<(), std::fmt::Error> {
    let labels = labels.to_vec();
    let encoder = family.encode_family(&labels)?;
    match metric_type {
        MetricType::Counter => ConstCounter::new(value).encode(encoder),
        _ => ConstGauge::new(value).encode(encoder),
    }
}

/// The server's metrics: a registry plus a resolved handle for every series
/// the request path updates.
#[derive(Debug)]
pub(crate) struct Metrics {
    pub config: MetricsConfig,
    registry: Registry,
    scrape: Arc<ScrapeCollector>,
    /// Serializes `render` so one scrape's snapshot is the one it encodes.
    render_lock: Mutex<()>,

    search_requests: Family<Labels, Counter>,
    search_outcomes: [Counter; 4],
    search_duration: AtomicHistogram,
    phase_queue: AtomicHistogram,
    phase_hydrate: AtomicHistogram,
    phase_score: AtomicHistogram,
    queue_rejections: Counter,
    index_documents: Counter,
    flushes: Counter,
    writer_fenced: Counter,
    hydrations: [Counter; 3],
    lease_outcomes: [Counter; 3],
    knn_degraded_segments: Counter,
    tenant_rejections: Family<Labels, Counter>,
    tenant_rejection_reasons: [Counter; crate::tenant_limits::RejectReason::ALL.len()],
    legacy_requests: Family<Labels, Counter>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(MetricsConfig::default())
    }
}

impl Metrics {
    pub(crate) fn from_env() -> Self {
        Self::new(MetricsConfig::from_env())
    }

    pub(crate) fn new(config: MetricsConfig) -> Self {
        let mut registry = Registry::default();

        let search_requests = Family::<Labels, Counter>::default();
        registry.register(
            "kosha_search_requests",
            "Search requests by outcome",
            search_requests.clone(),
        );
        // Resolved once so recording is an atomic add. With namespace
        // labels on these unlabelled series are never created; each request
        // resolves its `{namespace, outcome}` series instead.
        let search_outcomes = SearchOutcome::ALL.map(|o| {
            if config.namespace_labels {
                Counter::default()
            } else {
                search_requests
                    .get_or_create(&labels(&[("outcome", o.as_str())]))
                    .clone()
            }
        });

        let search_duration = AtomicHistogram::default();
        registry.register(
            "kosha_search_duration_seconds",
            "Wall-clock time of successful search requests",
            search_duration.clone(),
        );

        let phases = Family::<Labels, AtomicHistogram>::default();
        registry.register(
            "kosha_search_phase_seconds",
            "Time successful searches spent in each phase",
            phases.clone(),
        );
        let phase = |name: &str| phases.get_or_create(&labels(&[("phase", name)])).clone();
        let (phase_queue, phase_hydrate, phase_score) =
            (phase("queue"), phase("hydrate"), phase("score"));

        let counter = |registry: &mut Registry, name: &str, help: &str| {
            let c = Counter::default();
            registry.register(name, help, c.clone());
            c
        };
        let queue_rejections = counter(
            &mut registry,
            "kosha_search_queue_rejections",
            "Searches the scoring queue shed, because it was full or the wait timed out",
        );
        let index_documents = counter(
            &mut registry,
            "kosha_index_documents",
            "Documents accepted by index and replace requests",
        );
        let flushes = counter(&mut registry, "kosha_flush", "Successful flush requests");
        let writer_fenced = counter(
            &mut registry,
            "kosha_writer_fenced",
            "Namespaces this writer was fenced from after losing its writer lease",
        );

        let hydration_family = Family::<Labels, Counter>::default();
        registry.register(
            "kosha_hydrations",
            "Segment files made local for a request, by source",
            hydration_family.clone(),
        );
        let hydrations = ["s3", "peer", "local"].map(|s| {
            hydration_family
                .get_or_create(&labels(&[("source", s)]))
                .clone()
        });

        let lease_family = Family::<Labels, Counter>::default();
        registry.register(
            "kosha_hydration_lease_outcome",
            "Cross-replica hydration lease claims, by outcome",
            lease_family.clone(),
        );
        let lease_outcomes = ["won", "streamed_from_peer", "timeout"].map(|o| {
            lease_family
                .get_or_create(&labels(&[("outcome", o)]))
                .clone()
        });

        let knn_degraded_segments = counter(
            &mut registry,
            "kosha_knn_degraded_segments",
            "Segments a kNN search could not search by vector and skipped",
        );

        let tenant_rejections = Family::<Labels, Counter>::default();
        registry.register(
            "kosha_tenant_rejections",
            "Requests refused by a per-tenant limit or quota, by reason",
            tenant_rejections.clone(),
        );
        // Like the search outcomes: resolved once unless tenant labels are
        // on, in which case each rejection resolves its `{reason, tenant}`.
        let tenant_rejection_reasons = crate::tenant_limits::RejectReason::ALL.map(|r| {
            if config.tenant_labels {
                Counter::default()
            } else {
                tenant_rejections
                    .get_or_create(&labels(&[("reason", r.as_str())]))
                    .clone()
            }
        });

        let legacy_requests = Family::<Labels, Counter>::default();
        registry.register(
            "kosha_legacy_requests",
            "Requests served on a deprecated unversioned route, by route (#350)",
            legacy_requests.clone(),
        );

        let scrape = Arc::new(ScrapeCollector {
            tenant_labels: config.tenant_labels,
            ..ScrapeCollector::default()
        });
        registry.register_collector(Box::new(Arc::clone(&scrape)));

        Self {
            config,
            registry,
            scrape,
            render_lock: Mutex::new(()),
            search_requests,
            search_outcomes,
            search_duration,
            phase_queue,
            phase_hydrate,
            phase_score,
            queue_rejections,
            index_documents,
            flushes,
            writer_fenced,
            hydrations,
            lease_outcomes,
            knn_degraded_segments,
            tenant_rejections,
            tenant_rejection_reasons,
            legacy_requests,
        }
    }

    /// One request served on a legacy unversioned route (#350), so the
    /// cutover to `/v1` can be watched until it reads zero. `tenant`
    /// labels the series only with `KOSHA_METRICS_TENANT_LABELS=on`.
    pub(crate) fn record_legacy_request(&self, route: &str, tenant: &str) {
        let series = if self.config.tenant_labels {
            labels(&[("route", route), ("tenant", tenant)])
        } else {
            labels(&[("route", route)])
        };
        self.legacy_requests.get_or_create(&series).inc();
    }

    /// One request refused by a per-tenant limit (#341). `tenant` labels
    /// the series only with `KOSHA_METRICS_TENANT_LABELS=on`.
    pub(crate) fn record_tenant_rejection(
        &self,
        reason: crate::tenant_limits::RejectReason,
        tenant: &str,
    ) {
        if self.config.tenant_labels {
            self.tenant_rejections
                .get_or_create(&labels(&[("reason", reason.as_str()), ("tenant", tenant)]))
                .inc();
        } else {
            self.tenant_rejection_reasons[reason as usize].inc();
        }
    }

    /// Record one finished search request. `namespace` is consulted only
    /// with `KOSHA_METRICS_NAMESPACE_LABELS=on`.
    pub(crate) fn record_search(
        &self,
        outcome: SearchOutcome,
        elapsed_secs: f64,
        namespace: Option<&str>,
    ) {
        if self.config.namespace_labels {
            self.search_requests
                .get_or_create(&labels(&[
                    ("namespace", namespace.unwrap_or("")),
                    ("outcome", outcome.as_str()),
                ]))
                .inc();
        } else {
            self.search_outcomes[outcome as usize].inc();
        }
        if outcome == SearchOutcome::Ok {
            self.search_duration.observe_secs(elapsed_secs);
        }
    }

    /// Phase timings and degraded kNN segments of one scored search.
    pub(crate) fn record_search_phases(
        &self,
        hydrate_ms: f64,
        queue_ms: f64,
        score_ms: f64,
        knn_degraded_segments: u32,
    ) {
        self.phase_hydrate.observe_ms(hydrate_ms);
        self.phase_queue.observe_ms(queue_ms);
        self.phase_score.observe_ms(score_ms);
        if knn_degraded_segments > 0 {
            self.knn_degraded_segments
                .inc_by(u64::from(knn_degraded_segments));
        }
    }

    pub(crate) fn record_queue_shed(&self) {
        self.queue_rejections.inc();
    }

    pub(crate) fn record_indexed(&self, documents: u64) {
        self.index_documents.inc_by(documents);
    }

    pub(crate) fn record_flush(&self) {
        self.flushes.inc();
    }

    /// This writer lost a namespace's writer lease and fenced it (#254).
    pub(crate) fn record_writer_fenced(&self) {
        self.writer_fenced.inc();
    }

    /// `kosha_writer_fenced_total` so far.
    #[cfg(test)]
    pub(crate) fn writer_fenced_count(&self) -> u64 {
        self.writer_fenced.get()
    }

    pub(crate) fn record_hydrations(&self, source: HydrationSource, files: u64) {
        if files > 0 {
            self.hydrations[source as usize].inc_by(files);
        }
    }

    pub(crate) fn record_lease(&self, outcome: LeaseOutcome) {
        self.lease_outcomes[outcome as usize].inc();
    }

    /// The registry in Prometheus text format (0.0.4), with `scrape` as
    /// the current value of every scrape-time series.
    pub(crate) fn render(&self, scrape: &Scrape) -> String {
        let _guard = self.render_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.scrape.store(scrape);
        let mut out = String::new();
        // Writing into a `String` cannot fail.
        let _ = prometheus_client::encoding::prometheus_text::encode(&mut out, &self.registry);
        out
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap, HashSet};

    /// One parsed exposition sample.
    #[derive(Debug, Clone)]
    pub(crate) struct Sample {
        pub name: String,
        pub labels: BTreeMap<String, String>,
        pub value: f64,
    }

    /// A minimal Prometheus text-format (0.0.4) parser, strict enough to
    /// catch what `promtool check metrics` would: every sample belongs to a
    /// family announced by `# HELP` and `# TYPE` before it, no family is
    /// announced twice, label syntax is well formed and values parse.
    pub(crate) fn parse_exposition(text: &str) -> Result<Vec<Sample>, String> {
        let mut help: HashSet<String> = HashSet::new();
        let mut types: HashMap<String, String> = HashMap::new();
        let mut samples = Vec::new();
        for (n, line) in text.lines().enumerate() {
            let at = |msg: String| format!("line {}: {msg}: {line:?}", n + 1);
            if line.is_empty() {
                continue;
            }
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let name = rest.split(' ').next().unwrap_or("");
                if !help.insert(name.to_string()) {
                    return Err(at(format!("duplicate HELP for {name}")));
                }
                continue;
            }
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let mut parts = rest.split(' ');
                let (name, kind) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
                if !["counter", "gauge", "histogram", "summary", "untyped"].contains(&kind) {
                    return Err(at(format!("unknown type {kind:?}")));
                }
                if !help.contains(name) {
                    return Err(at(format!("TYPE before HELP for {name}")));
                }
                if types.insert(name.to_string(), kind.to_string()).is_some() {
                    return Err(at(format!("duplicate TYPE for {name}")));
                }
                continue;
            }
            if line.starts_with('#') {
                continue;
            }
            let (series, value) = line.rsplit_once(' ').ok_or_else(|| at("no value".into()))?;
            let value: f64 = match value {
                "+Inf" => f64::INFINITY,
                "-Inf" => f64::NEG_INFINITY,
                v => v.parse().map_err(|_| at(format!("bad value {v:?}")))?,
            };
            let (name, labels) = match series.split_once('{') {
                None => (series.to_string(), BTreeMap::new()),
                Some((name, rest)) => {
                    let body = rest
                        .strip_suffix('}')
                        .ok_or_else(|| at("unterminated labels".into()))?;
                    let mut labels = BTreeMap::new();
                    for pair in body.split(',').filter(|p| !p.is_empty()) {
                        let (k, v) = pair
                            .split_once('=')
                            .ok_or_else(|| at(format!("bad label {pair:?}")))?;
                        let v = v
                            .strip_prefix('"')
                            .and_then(|v| v.strip_suffix('"'))
                            .ok_or_else(|| at(format!("unquoted label value {v:?}")))?;
                        labels.insert(k.to_string(), v.to_string());
                    }
                    (name.to_string(), labels)
                }
            };
            if !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
            {
                return Err(at(format!("bad metric name {name:?}")));
            }
            // The family a sample belongs to: itself, or for a histogram
            // its name without the `_bucket`/`_sum`/`_count` suffix.
            let family = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|s| {
                    name.strip_suffix(s)
                        .filter(|base| types.get(*base).is_some_and(|t| t == "histogram"))
                })
                .unwrap_or(&name);
            if !types.contains_key(family) || !help.contains(family) {
                return Err(at(format!("sample {name} has no HELP/TYPE header")));
            }
            samples.push(Sample {
                name,
                labels,
                value,
            });
        }
        Ok(samples)
    }

    /// The value of the sample named `name` whose labels include `labels`.
    pub(crate) fn value(samples: &[Sample], name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        samples
            .iter()
            .find(|s| {
                s.name == name
                    && labels
                        .iter()
                        .all(|(k, v)| s.labels.get(*k).map(String::as_str) == Some(*v))
            })
            .map(|s| s.value)
    }

    fn rendered(metrics: &Metrics) -> Vec<Sample> {
        let text = metrics.render(&Scrape {
            search_queue_depth: 3,
            nvme_cache_bytes: 4096,
            segment_cache_bytes: 1024,
            nvme_evictions: 7,
            segment_checksum_failures: 2,
            cache_tenants: vec![
                cache_sample("nvme", "acme", 4000, 5, 1, 4),
                cache_sample("nvme", "beta", 96, 1, 0, 3),
                cache_sample("result", "beta", 100, 2, 3, 1),
                cache_sample("corpus_stats", "acme", 0, 8, 2, 1),
                cache_sample("ceiling", "beta", 0, 4, 1, 0),
            ],
            span_writes: crate::span_writes::SpanWriteStats {
                written: 9,
                dropped: 2,
                dropped_bytes: 4096,
                held_bytes: 1024,
                ..Default::default()
            },
        });
        parse_exposition(&text).unwrap_or_else(|e| panic!("{e}\n{text}"))
    }

    fn cache_sample(
        cache: &'static str,
        tenant: &str,
        bytes: u64,
        hits: u64,
        misses: u64,
        evictions: u64,
    ) -> CacheTenantSample {
        CacheTenantSample {
            cache,
            stats: kosha_core::tenancy::TenantCacheStats {
                tenant: tenant.into(),
                bytes,
                hits,
                misses,
                evictions,
            },
        }
    }

    #[test]
    fn cache_figures_by_cache_and_gated_tenant_label() {
        // #342. Without tenant labels: one series per cache, summed over
        // tenants (NVMe/segment bytes and NVMe evictions from the caches'
        // own totals).
        let samples = rendered(&Metrics::default());
        let get = |samples: &[Sample], name: &str, labels: &[(&str, &str)]| {
            value(samples, name, labels).unwrap_or_else(|| panic!("{name} {labels:?}"))
        };
        assert_eq!(
            get(&samples, "kosha_cache_hits_total", &[("cache", "nvme")]),
            6.0
        );
        assert_eq!(
            get(&samples, "kosha_cache_misses_total", &[("cache", "nvme")]),
            1.0
        );
        assert_eq!(
            get(&samples, "kosha_cache_hits_total", &[("cache", "segment")]),
            0.0
        );
        assert_eq!(
            get(&samples, "kosha_cache_bytes", &[("cache", "result")]),
            100.0
        );
        assert_eq!(
            get(
                &samples,
                "kosha_cache_evictions_total",
                &[("cache", "result")]
            ),
            1.0
        );
        assert_eq!(
            get(
                &samples,
                "kosha_cache_evictions_total",
                &[("cache", "nvme")]
            ),
            7.0
        );
        assert!(samples.iter().all(|s| !s.labels.contains_key("tenant")));
        // The entry-bounded manifest caches report traffic, not bytes.
        assert_eq!(
            get(
                &samples,
                "kosha_cache_hits_total",
                &[("cache", "corpus_stats")]
            ),
            8.0
        );
        assert_eq!(
            get(
                &samples,
                "kosha_cache_evictions_total",
                &[("cache", "corpus_stats")]
            ),
            1.0
        );
        assert_eq!(
            get(
                &samples,
                "kosha_cache_misses_total",
                &[("cache", "ceiling")]
            ),
            1.0
        );
        assert!(value(&samples, "kosha_cache_bytes", &[("cache", "ceiling")]).is_none());

        let labelled = Metrics::new(MetricsConfig {
            tenant_labels: true,
            ..MetricsConfig::default()
        });
        let samples = rendered(&labelled);
        let acme = [("cache", "nvme"), ("tenant", "acme")];
        assert_eq!(get(&samples, "kosha_cache_bytes", &acme), 4000.0);
        assert_eq!(get(&samples, "kosha_cache_hits_total", &acme), 5.0);
        assert_eq!(get(&samples, "kosha_cache_misses_total", &acme), 1.0);
        assert_eq!(get(&samples, "kosha_cache_evictions_total", &acme), 4.0);
        let beta = [("cache", "result"), ("tenant", "beta")];
        assert_eq!(get(&samples, "kosha_cache_misses_total", &beta), 3.0);
        assert!(samples
            .iter()
            .filter(|s| s.name.starts_with("kosha_cache_"))
            .all(|s| s.labels.contains_key("tenant")));
    }

    #[test]
    fn every_documented_metric_renders_with_headers() {
        let metrics = Metrics::default();
        metrics.record_search(SearchOutcome::Ok, 0.002, None);
        metrics.record_search_phases(1.0, 0.1, 0.7, 2);
        let samples = rendered(&metrics);
        let names: HashSet<&str> = samples.iter().map(|s| s.name.as_str()).collect();
        for name in [
            "kosha_search_requests_total",
            "kosha_search_duration_seconds_bucket",
            "kosha_search_duration_seconds_sum",
            "kosha_search_duration_seconds_count",
            "kosha_search_phase_seconds_bucket",
            "kosha_search_queue_depth",
            "kosha_search_queue_rejections_total",
            "kosha_index_documents_total",
            "kosha_flush_total",
            "kosha_writer_fenced_total",
            "kosha_cache_bytes",
            "kosha_cache_evictions_total",
            "kosha_hydrations_total",
            "kosha_hydration_lease_outcome_total",
            "kosha_knn_degraded_segments_total",
            "kosha_segment_checksum_failures_total",
            "kosha_span_writes_total",
            "kosha_span_writes_dropped_bytes_total",
            "kosha_span_write_queue_bytes",
            "kosha_span_held_bytes",
            "kosha_tenant_rejections_total",
        ] {
            assert!(names.contains(name), "{name} missing from {names:?}");
        }
        // Every name the exposition uses is listed in docs/metrics.md.
        let doc = include_str!("../../../docs/metrics.md");
        for s in &samples {
            let family = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|x| s.name.strip_suffix(x))
                .unwrap_or(&s.name);
            assert!(doc.contains(family), "{family} is not in docs/metrics.md");
        }
        assert_eq!(value(&samples, "kosha_search_queue_depth", &[]), Some(3.0));
        assert_eq!(
            value(
                &samples,
                "kosha_span_writes_total",
                &[("outcome", "dropped")]
            ),
            Some(2.0)
        );
        assert_eq!(
            value(&samples, "kosha_span_writes_dropped_bytes_total", &[]),
            Some(4096.0)
        );
        assert_eq!(value(&samples, "kosha_span_held_bytes", &[]), Some(1024.0));
        assert_eq!(
            value(&samples, "kosha_cache_bytes", &[("cache", "nvme")]),
            Some(4096.0)
        );
        assert_eq!(
            value(&samples, "kosha_cache_bytes", &[("cache", "segment")]),
            Some(1024.0)
        );
        assert_eq!(
            value(
                &samples,
                "kosha_cache_evictions_total",
                &[("cache", "nvme")]
            ),
            Some(7.0)
        );
        assert_eq!(
            value(&samples, "kosha_knn_degraded_segments_total", &[]),
            Some(2.0)
        );
        // Every fixed label value is exported from the start, at zero.
        for outcome in ["ok", "rejected", "timeout", "error"] {
            assert!(value(
                &samples,
                "kosha_search_requests_total",
                &[("outcome", outcome)]
            )
            .is_some());
        }
        for phase in ["queue", "hydrate", "score"] {
            assert!(value(
                &samples,
                "kosha_search_phase_seconds_count",
                &[("phase", phase)]
            )
            .is_some());
        }
    }

    #[test]
    fn histogram_has_every_bucket_and_inf() {
        let metrics = Metrics::default();
        for secs in [0.0001, 0.0007, 0.3, 400.0] {
            metrics.record_search(SearchOutcome::Ok, secs, None);
        }
        let samples = rendered(&metrics);
        let buckets: Vec<&Sample> = samples
            .iter()
            .filter(|s| s.name == "kosha_search_duration_seconds_bucket")
            .collect();
        assert_eq!(buckets.len(), LATENCY_BUCKETS.len() + 1);
        assert_eq!(buckets.last().unwrap().labels["le"], "+Inf");
        // Cumulative and non-decreasing, ending at `_count`.
        let counts: Vec<f64> = buckets.iter().map(|s| s.value).collect();
        assert!(counts.windows(2).all(|w| w[0] <= w[1]), "{counts:?}");
        assert_eq!(counts[0], 1.0, "0.1 ms lands in le=0.0005");
        assert_eq!(counts[1], 2.0, "0.7 ms lands in le=0.001");
        assert_eq!(*counts.last().unwrap(), 4.0);
        assert_eq!(
            value(&samples, "kosha_search_duration_seconds_count", &[]),
            Some(4.0)
        );
        let sum = value(&samples, "kosha_search_duration_seconds_sum", &[]).unwrap();
        assert!((sum - 400.3008).abs() < 1e-6, "{sum}");
        // The `le` values are exactly the documented buckets.
        for (s, bound) in buckets.iter().zip(LATENCY_BUCKETS) {
            assert_eq!(s.labels["le"].parse::<f64>().unwrap(), bound);
        }
    }

    #[test]
    fn scrape_counters_never_go_backwards() {
        let metrics = Metrics::default();
        let mut scrape = Scrape {
            nvme_evictions: 10,
            ..Scrape::default()
        };
        metrics.render(&scrape);
        scrape.nvme_evictions = 4;
        let samples = parse_exposition(&metrics.render(&scrape)).unwrap();
        assert_eq!(
            value(
                &samples,
                "kosha_cache_evictions_total",
                &[("cache", "nvme")]
            ),
            Some(10.0)
        );
    }

    #[test]
    fn namespace_labels_apply_to_search_requests_only() {
        let metrics = Metrics::new(MetricsConfig {
            namespace_labels: true,
            ..MetricsConfig::default()
        });
        metrics.record_search(SearchOutcome::Ok, 0.01, Some("t/a"));
        metrics.record_search(SearchOutcome::Error, 0.01, Some("t/b"));
        let samples = rendered(&metrics);
        assert_eq!(
            value(
                &samples,
                "kosha_search_requests_total",
                &[("namespace", "t/a"), ("outcome", "ok")]
            ),
            Some(1.0)
        );
        assert!(samples
            .iter()
            .filter(|s| s.name.starts_with("kosha_search_requests"))
            .all(|s| s.labels.contains_key("namespace")));
        assert!(samples
            .iter()
            .filter(|s| !s.name.starts_with("kosha_search_requests"))
            .all(|s| !s.labels.contains_key("namespace")));
    }

    #[test]
    fn tenant_rejections_by_reason_and_gated_tenant_label() {
        use crate::tenant_limits::RejectReason;
        let metrics = Metrics::default();
        metrics.record_tenant_rejection(RejectReason::TenantQps, "acme");
        let samples = rendered(&metrics);
        // Every reason is exported from the start; no tenant label.
        for reason in RejectReason::ALL {
            assert!(value(
                &samples,
                "kosha_tenant_rejections_total",
                &[("reason", reason.as_str())]
            )
            .is_some());
        }
        assert_eq!(
            value(
                &samples,
                "kosha_tenant_rejections_total",
                &[("reason", "tenant_qps_exceeded")]
            ),
            Some(1.0)
        );
        assert!(samples.iter().all(|s| !s.labels.contains_key("tenant")));

        let labelled = Metrics::new(MetricsConfig {
            tenant_labels: true,
            ..MetricsConfig::default()
        });
        labelled.record_tenant_rejection(RejectReason::DocQuota, "acme");
        let samples = rendered(&labelled);
        assert_eq!(
            value(
                &samples,
                "kosha_tenant_rejections_total",
                &[("reason", "doc_quota_exceeded"), ("tenant", "acme")]
            ),
            Some(1.0)
        );
        assert!(samples
            .iter()
            .filter(|s| s.name.starts_with("kosha_tenant_rejections"))
            .all(|s| s.labels.contains_key("tenant")));
    }

    #[test]
    fn outcome_classification() {
        let ok = "HTTP/1.1 200 OK\r\n\r\n{}";
        let shed = "HTTP/1.1 429 Too Many Requests\r\n\r\n{}";
        let bad = "HTTP/1.1 400 Bad Request\r\n\r\n{}";
        let failed = "HTTP/1.1 503 Service Unavailable\r\n\r\n{}";
        assert_eq!(SearchOutcome::classify(ok, None), SearchOutcome::Ok);
        assert_eq!(SearchOutcome::classify(shed, None), SearchOutcome::Rejected);
        assert_eq!(
            SearchOutcome::classify(shed, Some(QueueShed::Full)),
            SearchOutcome::Rejected
        );
        assert_eq!(
            SearchOutcome::classify(shed, Some(QueueShed::TenantFull)),
            SearchOutcome::Rejected
        );
        assert_eq!(
            SearchOutcome::classify(shed, Some(QueueShed::Timeout)),
            SearchOutcome::Timeout
        );
        assert_eq!(SearchOutcome::classify(bad, None), SearchOutcome::Error);
        assert_eq!(SearchOutcome::classify(failed, None), SearchOutcome::Error);
    }

    #[test]
    fn config_parsing() {
        let c = MetricsConfig::parse(None, None, None);
        assert_eq!(c.exposure, Exposure::Open);
        assert!(!c.namespace_labels);
        assert_eq!(
            MetricsConfig::parse(Some("off"), Some("admin"), None).exposure,
            Exposure::Off
        );
        assert_eq!(
            MetricsConfig::parse(Some("on"), Some("admin"), None).exposure,
            Exposure::Admin
        );
        assert_eq!(
            MetricsConfig::parse(None, Some("bogus"), None).exposure,
            Exposure::Admin,
            "an unknown auth mode fails closed"
        );
        assert!(MetricsConfig::parse(None, None, Some("on")).namespace_labels);
    }

    /// `PROMTOOL_METRICS_DUMP=<path>`: write a sample exposition for CI's
    /// `promtool check metrics` step. A no-op otherwise.
    #[test]
    fn dump_exposition_for_promtool() {
        let Ok(path) = std::env::var("PROMTOOL_METRICS_DUMP") else {
            return;
        };
        let metrics = Metrics::default();
        metrics.record_search(SearchOutcome::Ok, 0.004, None);
        metrics.record_search_phases(2.0, 0.0, 1.5, 1);
        metrics.record_hydrations(HydrationSource::S3, 3);
        metrics.record_lease(LeaseOutcome::Won);
        std::fs::write(path, metrics.render(&Scrape::default())).unwrap();
    }
}
