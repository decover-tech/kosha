//! Per-tenant quotas and rate limits (#341).
//!
//! A [`TenantLimits`] says what one tenant may consume on this pod: query
//! rate and concurrency, write rate in documents and bytes, how many
//! namespaces it may hold, documents and bytes per namespace, vector
//! dimensions and request body size. A [`LimitsProvider`] answers "what
//! are tenant X's limits"; [`ConfigLimitsProvider`] reloads operator
//! overrides, which are layered over the tenant's billing plan defaults.
//!
//! [`TenantLimiter`] enforces them with lock-free token buckets (GCRA over
//! one `AtomicU64`) and an atomic concurrency counter per tenant. With no
//! provider configured it is inert: every check returns at once and nothing
//! is limited, which is the pre-#341 behaviour.
//!
//! Enforcement is per pod. Two query replicas each admit a tenant's full
//! qps; cluster-wide limits are out of scope here.
//!
//! The file format:
//!
//! ```json
//! {
//!   "default_plan": "standard",
//!   "plans": {
//!     "standard": {"qps": 50, "concurrent_queries": 8, "write_docs_per_sec": 2000,
//!                  "write_bytes_per_sec": 16777216, "namespaces": 100,
//!                  "docs_per_namespace": 10000000, "vector_dims": 4096,
//!                  "max_body_bytes": 33554432, "weight": 1}
//!   },
//!   "tenants": {"acme": {"plan": "standard", "qps": 200, "weight": 4}}
//! }
//! ```
//!
//! A tenant's limits are its plan's (its own `plan`, else
//! `KOSHA_TENANT_DEFAULT_PLAN`, else the file's `default_plan`) with the
//! fields of its `tenants` entry laid over them. A tenant with neither a
//! plan nor an entry is unlimited. Every field is optional; an absent field
//! is not limited.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde::Deserialize;

/// What one tenant may consume on this pod. `None` = not limited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TenantLimits {
    /// Search requests per second. The bucket holds one second's worth
    /// (at least one request), so a tenant may burst to `qps` at once.
    pub qps: Option<f64>,
    /// Search requests in flight at once.
    pub concurrent_queries: Option<u32>,
    /// Documents per second accepted by index and replace requests.
    pub write_docs_per_sec: Option<f64>,
    /// Request body bytes per second on write routes.
    pub write_bytes_per_sec: Option<f64>,
    /// Namespaces the tenant may hold (checked when one is created:
    /// explicitly, as a branch, or by a first write).
    pub namespaces: Option<u64>,
    /// Documents per namespace, committed plus not yet flushed.
    pub docs_per_namespace: Option<u64>,
    /// Estimated stored bytes per namespace, including unflushed writes.
    pub bytes_per_namespace: Option<u64>,
    /// Largest vector dimension a document may carry.
    pub vector_dims: Option<u32>,
    /// Largest request body, below the pod-wide
    /// `KOSHA_MAX_BODY_BYTES`.
    pub max_body_bytes: Option<u64>,
    /// Share of the search queue when several tenants wait (deficit round
    /// robin). Default 1.
    pub weight: Option<u32>,
}

impl TenantLimits {
    /// `self` with every field `over` sets replaced by `over`'s value.
    fn overlay(self, over: &TenantLimits) -> TenantLimits {
        TenantLimits {
            qps: over.qps.or(self.qps),
            concurrent_queries: over.concurrent_queries.or(self.concurrent_queries),
            write_docs_per_sec: over.write_docs_per_sec.or(self.write_docs_per_sec),
            write_bytes_per_sec: over.write_bytes_per_sec.or(self.write_bytes_per_sec),
            namespaces: over.namespaces.or(self.namespaces),
            docs_per_namespace: over.docs_per_namespace.or(self.docs_per_namespace),
            bytes_per_namespace: over.bytes_per_namespace.or(self.bytes_per_namespace),
            vector_dims: over.vector_dims.or(self.vector_dims),
            max_body_bytes: over.max_body_bytes.or(self.max_body_bytes),
            weight: over.weight.or(self.weight),
        }
    }

    fn validate(&self, what: &str) -> Result<(), String> {
        for (name, rate) in [
            ("qps", self.qps),
            ("write_docs_per_sec", self.write_docs_per_sec),
            ("write_bytes_per_sec", self.write_bytes_per_sec),
        ] {
            if let Some(rate) = rate {
                if !(rate.is_finite() && rate > 0.0) {
                    return Err(format!("{what}: {name} must be a positive number"));
                }
            }
        }
        if self.weight == Some(0) {
            return Err(format!("{what}: weight must be at least 1"));
        }
        Ok(())
    }
}

/// One `tenants` entry: a plan name plus overrides. The limit fields are
/// listed here (not a flattened [`TenantLimits`]) because serde cannot
/// combine `flatten` with `deny_unknown_fields`, and an unknown key must be
/// an error: a typo would otherwise leave the tenant on its plan, or
/// unlimited when it has none.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TenantEntry {
    #[serde(default)]
    pub plan: Option<String>,
    pub qps: Option<f64>,
    pub concurrent_queries: Option<u32>,
    pub write_docs_per_sec: Option<f64>,
    pub write_bytes_per_sec: Option<f64>,
    pub namespaces: Option<u64>,
    pub docs_per_namespace: Option<u64>,
    pub bytes_per_namespace: Option<u64>,
    pub vector_dims: Option<u32>,
    pub max_body_bytes: Option<u64>,
    pub weight: Option<u32>,
}

impl TenantEntry {
    /// The limit fields this entry sets.
    fn overrides(&self) -> TenantLimits {
        TenantLimits {
            qps: self.qps,
            concurrent_queries: self.concurrent_queries,
            write_docs_per_sec: self.write_docs_per_sec,
            write_bytes_per_sec: self.write_bytes_per_sec,
            namespaces: self.namespaces,
            docs_per_namespace: self.docs_per_namespace,
            bytes_per_namespace: self.bytes_per_namespace,
            vector_dims: self.vector_dims,
            max_body_bytes: self.max_body_bytes,
            weight: self.weight,
        }
    }
}

/// The parsed `KOSHA_TENANT_LIMITS_FILE`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LimitsConfig {
    #[serde(default)]
    pub default_plan: Option<String>,
    #[serde(default)]
    pub plans: HashMap<String, TenantLimits>,
    #[serde(default)]
    pub tenants: HashMap<String, TenantEntry>,
}

impl LimitsConfig {
    /// Parse and validate. `default_plan` (from `KOSHA_TENANT_DEFAULT_PLAN`)
    /// replaces the file's own `default_plan` when set.
    pub(crate) fn parse(text: &str, default_plan: Option<&str>) -> Result<Self, String> {
        let mut config: LimitsConfig =
            serde_json::from_str(text).map_err(|e| format!("invalid tenant limits JSON: {e}"))?;
        if let Some(plan) = default_plan.map(str::trim).filter(|p| !p.is_empty()) {
            config.default_plan = Some(plan.to_string());
        }
        if let Some(plan) = &config.default_plan {
            if !config.plans.contains_key(plan) {
                return Err(format!("default plan {plan:?} is not defined in `plans`"));
            }
        }
        for (name, plan) in &config.plans {
            plan.validate(&format!("plan {name:?}"))?;
        }
        for (tenant, entry) in &config.tenants {
            if let Some(plan) = &entry.plan {
                if !config.plans.contains_key(plan) {
                    return Err(format!(
                        "tenant {tenant:?} names plan {plan:?}, which is not defined in `plans`"
                    ));
                }
            }
            entry.overrides().validate(&format!("tenant {tenant:?}"))?;
        }
        Ok(config)
    }

    /// The resolved limits of `tenant`, or `None` when nothing applies.
    pub(crate) fn resolve(&self, tenant: &str) -> Option<TenantLimits> {
        let entry = self.tenants.get(tenant);
        let plan = entry
            .and_then(|e| e.plan.as_ref())
            .or(self.default_plan.as_ref())
            .and_then(|p| self.plans.get(p));
        match (plan, entry) {
            (None, None) => None,
            (plan, entry) => {
                let base = plan.copied().unwrap_or_default();
                Some(match entry {
                    Some(e) => base.overlay(&e.overrides()),
                    None => base,
                })
            }
        }
    }
}

/// Where a tenant's limits come from.
pub(crate) trait LimitsProvider: Send + Sync {
    /// `tenant`'s limits right now, or `None` if it is not limited.
    fn limits(&self, tenant: &str) -> Option<TenantLimits>;
}

/// Limits from a JSON file (`KOSHA_TENANT_LIMITS_FILE`), reloaded every
/// `KOSHA_TENANT_LIMITS_RELOAD_SECS`. A reload that fails to read or parse
/// keeps the previous configuration.
pub(crate) struct ConfigLimitsProvider {
    config: RwLock<Arc<LimitsConfig>>,
    path: Option<PathBuf>,
    default_plan: Option<String>,
}

impl ConfigLimitsProvider {
    /// A fixed configuration, never reloaded.
    #[cfg(test)]
    pub(crate) fn from_config(config: LimitsConfig) -> Self {
        Self {
            config: RwLock::new(Arc::new(config)),
            path: None,
            default_plan: None,
        }
    }

    /// Load `path` once. The caller starts reloading with
    /// [`ConfigLimitsProvider::spawn_reloader`].
    pub(crate) fn load(path: PathBuf, default_plan: Option<String>) -> Result<Self, String> {
        let text = std::fs::read_to_string(&path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let config = LimitsConfig::parse(&text, default_plan.as_deref())
            .map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Self {
            config: RwLock::new(Arc::new(config)),
            path: Some(path),
            default_plan,
        })
    }

    /// Re-read the file. `Ok(true)` when the configuration changed.
    pub(crate) fn reload(&self) -> Result<bool, String> {
        let Some(path) = &self.path else {
            return Ok(false);
        };
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let fresh = LimitsConfig::parse(&text, self.default_plan.as_deref())
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let mut current = self.config.write().unwrap_or_else(|e| e.into_inner());
        let changed = format!("{:?}", **current) != format!("{fresh:?}");
        if changed {
            *current = Arc::new(fresh);
        }
        Ok(changed)
    }

    /// Reload every `interval` on a background thread for the life of the
    /// process.
    pub(crate) fn spawn_reloader(self: &Arc<Self>, interval: Duration) {
        let provider = Arc::clone(self);
        let _ = std::thread::Builder::new()
            .name("kosha-tenant-limits".into())
            .spawn(move || loop {
                std::thread::sleep(interval);
                match provider.reload() {
                    Ok(true) => println!("tenant limits: reloaded"),
                    Ok(false) => {}
                    Err(e) => {
                        eprintln!("WARN: tenant limits: reload failed, keeping previous: {e}")
                    }
                }
            });
    }

    fn snapshot(&self) -> Arc<LimitsConfig> {
        Arc::clone(&self.config.read().unwrap_or_else(|e| e.into_inner()))
    }
}

impl LimitsProvider for ConfigLimitsProvider {
    fn limits(&self, tenant: &str) -> Option<TenantLimits> {
        self.snapshot().resolve(tenant)
    }
}

/// Plan quota defaults with the operator's limits laid over them. See
/// [`TenantLimiter::with_plan_defaults`].
struct LayeredLimits {
    defaults: Arc<dyn LimitsProvider>,
    overrides: Option<Arc<dyn LimitsProvider>>,
}

impl LimitsProvider for LayeredLimits {
    fn limits(&self, tenant: &str) -> Option<TenantLimits> {
        let over = self.overrides.as_ref().and_then(|p| p.limits(tenant));
        match (self.defaults.limits(tenant), over) {
            (Some(defaults), Some(over)) => Some(defaults.overlay(&over)),
            (defaults, over) => over.or(defaults),
        }
    }
}

// ─── Token bucket ───────────────────────────────────────────────────────────

/// A token bucket as a generic cell rate algorithm: one atomic holding the
/// theoretical arrival time (nanoseconds since the limiter's epoch). Taking
/// tokens is a compare-and-swap; there is no lock and no refill thread. The
/// rate and burst are passed on every call, so a reloaded limit applies to
/// the next request.
#[derive(Debug, Default)]
pub(crate) struct TokenBucket {
    tat: AtomicU64,
}

impl TokenBucket {
    /// Take `cost` tokens at `now` (nanoseconds) from a bucket refilling at
    /// `rate` tokens per second and holding at most `burst` tokens. `Err`
    /// carries how long until the request would fit, in nanoseconds.
    ///
    /// A request costing more than the whole bucket is admitted only from
    /// a full bucket and leaves it in debt, so an oversized batch is slowed
    /// down instead of being refused forever.
    pub(crate) fn try_take(&self, now: u64, rate: f64, burst: f64, cost: f64) -> Result<(), u64> {
        let interval = 1e9 / rate;
        let increment = (cost.max(0.0) * interval) as u64;
        let tolerance = (burst.max(1.0) * interval) as u64;
        let mut tat = self.tat.load(Ordering::Relaxed);
        loop {
            let new_tat = tat.max(now).saturating_add(increment);
            let limit = now.saturating_add(tolerance);
            let wait = if increment <= tolerance {
                new_tat.saturating_sub(limit)
            } else {
                tat.saturating_sub(now)
            };
            if wait > 0 {
                return Err(wait);
            }
            match self
                .tat
                .compare_exchange_weak(tat, new_tat, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return Ok(()),
                Err(current) => tat = current,
            }
        }
    }
}

// ─── Enforcement ────────────────────────────────────────────────────────────

/// Why a request was refused, as the `reason` of the error body and the
/// `reason` label of `kosha_tenant_rejections_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RejectReason {
    TenantQps,
    TenantConcurrency,
    TenantWriteRate,
    NamespaceQuota,
    DocQuota,
    StorageQuota,
    VectorDims,
    BodyTooLarge,
    /// The tenant already has `KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT`
    /// searches waiting for a scoring slot.
    TenantQueueFull,
}

impl RejectReason {
    pub(crate) const ALL: [RejectReason; 9] = [
        Self::TenantQps,
        Self::TenantConcurrency,
        Self::TenantWriteRate,
        Self::NamespaceQuota,
        Self::DocQuota,
        Self::StorageQuota,
        Self::VectorDims,
        Self::BodyTooLarge,
        Self::TenantQueueFull,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TenantQps => "tenant_qps_exceeded",
            Self::TenantConcurrency => "tenant_concurrency_exceeded",
            Self::TenantWriteRate => "tenant_write_rate_exceeded",
            Self::NamespaceQuota => "namespace_quota_exceeded",
            Self::DocQuota => "doc_quota_exceeded",
            Self::StorageQuota => "storage_quota_exceeded",
            Self::VectorDims => "vector_dims_exceeded",
            Self::BodyTooLarge => "body_too_large",
            Self::TenantQueueFull => "tenant_queue_full",
        }
    }

    /// The API error code (#350). Rates are retryable 429s; quotas are
    /// 403s and the body cap a 413, which retrying cannot fix.
    pub(crate) fn code(self) -> crate::ErrorCode {
        use crate::ErrorCode;
        match self {
            Self::TenantQps => ErrorCode::TenantQpsExceeded,
            Self::TenantConcurrency => ErrorCode::TenantConcurrencyExceeded,
            Self::TenantWriteRate => ErrorCode::TenantWriteRateExceeded,
            Self::TenantQueueFull => ErrorCode::TenantQueueFull,
            Self::NamespaceQuota => ErrorCode::NamespaceQuotaExceeded,
            Self::DocQuota => ErrorCode::DocQuotaExceeded,
            Self::StorageQuota => ErrorCode::PermissionDenied,
            Self::VectorDims => ErrorCode::VectorDimsExceeded,
            Self::BodyTooLarge => ErrorCode::PayloadTooLarge,
        }
    }

    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::TenantQps => "tenant query rate limit exceeded",
            Self::TenantConcurrency => "tenant concurrent query limit exceeded",
            Self::TenantWriteRate => "tenant write rate limit exceeded",
            Self::NamespaceQuota => "tenant namespace quota exceeded",
            Self::DocQuota => "namespace document quota exceeded",
            Self::StorageQuota => "namespace storage quota exceeded",
            Self::VectorDims => "vector dimension limit exceeded",
            Self::BodyTooLarge => "request body exceeds the tenant's limit",
            Self::TenantQueueFull => "tenant search queue is full",
        }
    }
}

/// How long a client refused by a quota (not a rate) is told to wait: the
/// quota may be raised by a reload, or the tenant may free capacity.
pub(crate) const QUOTA_RETRY_AFTER_MS: u64 = 60_000;

/// How long a client refused for concurrency is told to wait.
const CONCURRENCY_RETRY_AFTER_MS: u64 = 1_000;

/// How long a client whose search the scoring queue shed is told to wait.
pub(crate) const QUEUE_SHED_RETRY_AFTER_MS: u64 = 1_000;

/// A refused request: why, the limit it hit, and when to retry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Rejection {
    pub reason: RejectReason,
    pub limit: f64,
    pub retry_after_ms: u64,
}

impl Rejection {
    pub(crate) fn quota(reason: RejectReason, limit: u64) -> Self {
        Self {
            reason,
            limit: limit as f64,
            retry_after_ms: QUOTA_RETRY_AFTER_MS,
        }
    }

    /// A search shed because its tenant's share of the scoring queue
    /// (`limit` waiters) is full.
    pub(crate) fn tenant_queue_full(limit: usize) -> Self {
        Self {
            reason: RejectReason::TenantQueueFull,
            limit: limit as f64,
            retry_after_ms: QUEUE_SHED_RETRY_AFTER_MS,
        }
    }

    /// The limit as JSON: an integer when it is whole.
    pub(crate) fn limit_json(&self) -> serde_json::Value {
        if self.limit.fract() == 0.0 && self.limit.abs() < 9.0e15 {
            serde_json::json!(self.limit as u64)
        } else {
            serde_json::json!(self.limit)
        }
    }
}

/// One tenant's live counters.
#[derive(Debug, Default)]
struct TenantCounters {
    qps: TokenBucket,
    write_docs: TokenBucket,
    write_bytes: TokenBucket,
    in_flight: AtomicU32,
    storage_admission: Mutex<()>,
}

/// Holds one of a tenant's concurrent-query slots until dropped.
pub(crate) struct QueryGuard {
    counters: Option<Arc<TenantCounters>>,
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        if let Some(c) = &self.counters {
            c.in_flight.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Enforces each tenant's [`TenantLimits`] on this pod. See the module doc.
pub(crate) struct TenantLimiter {
    provider: Option<Arc<dyn LimitsProvider>>,
    tenants: RwLock<HashMap<String, Arc<TenantCounters>>>,
    epoch: Instant,
}

impl Default for TenantLimiter {
    fn default() -> Self {
        Self::disabled()
    }
}

impl TenantLimiter {
    /// Nothing is limited.
    pub(crate) fn disabled() -> Self {
        Self {
            provider: None,
            tenants: RwLock::new(HashMap::new()),
            epoch: Instant::now(),
        }
    }

    pub(crate) fn new(provider: Arc<dyn LimitsProvider>) -> Self {
        Self {
            provider: Some(provider),
            ..Self::disabled()
        }
    }

    /// From `KOSHA_TENANT_LIMITS_FILE`, `KOSHA_TENANT_DEFAULT_PLAN` and
    /// `KOSHA_TENANT_LIMITS_RELOAD_SECS`. No file: disabled. A file that
    /// cannot be read or parsed is an error, so a typo cannot silently turn
    /// limits off.
    pub(crate) fn from_env() -> Result<Self, String> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let default_plan = var("KOSHA_TENANT_DEFAULT_PLAN");
        let Some(path) = var("KOSHA_TENANT_LIMITS_FILE") else {
            if default_plan.is_some() {
                return Err(
                    "KOSHA_TENANT_DEFAULT_PLAN is set but KOSHA_TENANT_LIMITS_FILE is not; \
                     plans are defined in the limits file"
                        .into(),
                );
            }
            return Ok(Self::disabled());
        };
        let provider = Arc::new(ConfigLimitsProvider::load(
            PathBuf::from(path.trim()),
            default_plan,
        )?);
        let reload_secs = var("KOSHA_TENANT_LIMITS_RELOAD_SECS")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(30);
        if reload_secs > 0 {
            provider.spawn_reloader(Duration::from_secs(reload_secs));
        }
        println!(
            "tenant limits: loaded {} (reload every {reload_secs}s)",
            provider
                .path
                .as_ref()
                .map_or(String::new(), |p| p.display().to_string())
        );
        Ok(Self::new(provider))
    }

    /// `self` with `plans` as the defaults underneath it (#349): a tenant's
    /// limits are what `plans` gives it (its billing plan's quotas), with
    /// every field this limiter's own provider (the limits file) sets laid
    /// over them. Without a limits file the plan quotas apply as they are.
    pub(crate) fn with_plan_defaults(self, plans: Arc<dyn LimitsProvider>) -> Self {
        Self {
            provider: Some(Arc::new(LayeredLimits {
                defaults: plans,
                overrides: self.provider,
            })),
            tenants: self.tenants,
            epoch: self.epoch,
        }
    }

    /// Whether any limits are configured.
    pub(crate) fn enabled(&self) -> bool {
        self.provider.is_some()
    }

    /// `tenant`'s limits, or `None` when it is not limited.
    pub(crate) fn limits(&self, tenant: &str) -> Option<TenantLimits> {
        self.provider.as_ref()?.limits(tenant)
    }

    /// `tenant`'s search queue weight (1 unless its plan says otherwise).
    pub(crate) fn weight(&self, tenant: &str) -> u32 {
        self.limits(tenant)
            .and_then(|l| l.weight)
            .unwrap_or(1)
            .max(1)
    }

    /// The plan weights as a standalone function, for the caches' tenant
    /// shares (#342): a tenant's cache share follows its search queue
    /// weight, and follows the limits file across reloads. `None` when no
    /// limits are configured (every tenant weighs 1).
    pub(crate) fn weight_fn(&self) -> Option<kosha_core::tenancy::TenantWeightFn> {
        let provider = Arc::clone(self.provider.as_ref()?);
        Some(Arc::new(move |tenant: &str| {
            provider
                .limits(tenant)
                .and_then(|l| l.weight)
                .unwrap_or(1)
                .max(1)
        }))
    }

    fn now(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    fn counters(&self, tenant: &str) -> Arc<TenantCounters> {
        if let Some(c) = self
            .tenants
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(tenant)
        {
            return Arc::clone(c);
        }
        let mut map = self.tenants.write().unwrap_or_else(|e| e.into_inner());
        Arc::clone(map.entry(tenant.to_string()).or_default())
    }

    /// Admit one search: a concurrent-query slot (held by the returned
    /// guard) and one qps token.
    pub(crate) fn admit_query(&self, tenant: &str) -> Result<QueryGuard, Rejection> {
        let Some(limits) = self.limits(tenant) else {
            return Ok(QueryGuard { counters: None });
        };
        if limits.qps.is_none() && limits.concurrent_queries.is_none() {
            return Ok(QueryGuard { counters: None });
        }
        let counters = self.counters(tenant);
        let guard = match limits.concurrent_queries {
            Some(max) => {
                let previous = counters.in_flight.fetch_add(1, Ordering::AcqRel);
                let guard = QueryGuard {
                    counters: Some(Arc::clone(&counters)),
                };
                if previous >= max {
                    drop(guard);
                    return Err(Rejection {
                        reason: RejectReason::TenantConcurrency,
                        limit: f64::from(max),
                        retry_after_ms: CONCURRENCY_RETRY_AFTER_MS,
                    });
                }
                guard
            }
            None => QueryGuard { counters: None },
        };
        if let Some(qps) = limits.qps {
            counters
                .qps
                .try_take(self.now(), qps, qps, 1.0)
                .map_err(|wait| Rejection {
                    reason: RejectReason::TenantQps,
                    limit: qps,
                    retry_after_ms: nanos_to_ms(wait),
                })?;
        }
        Ok(guard)
    }

    /// Admit a request body: the tenant's body size cap, then its bytes
    /// per second for write requests only.
    pub(crate) fn admit_body(
        &self,
        tenant: &str,
        bytes: usize,
        write: bool,
    ) -> Result<(), Rejection> {
        let Some(limits) = self.limits(tenant) else {
            return Ok(());
        };
        if let Some(max) = limits.max_body_bytes {
            if bytes as u64 > max {
                return Err(Rejection::quota(RejectReason::BodyTooLarge, max));
            }
        }
        if let Some(rate) = limits.write_bytes_per_sec.filter(|_| write) {
            self.counters(tenant)
                .write_bytes
                .try_take(self.now(), rate, rate, bytes as f64)
                .map_err(|wait| Rejection {
                    reason: RejectReason::TenantWriteRate,
                    limit: rate,
                    retry_after_ms: nanos_to_ms(wait),
                })?;
        }
        Ok(())
    }

    /// Serialize the quota check and write against other quota-checked
    /// writes by this tenant. Each tenant has its own lock.
    pub(crate) fn with_storage_admission<R>(&self, tenant: &str, f: impl FnOnce() -> R) -> R {
        let counters = self.counters(tenant);
        let _guard = counters
            .storage_admission
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        f()
    }

    /// Admit `docs` documents against the tenant's write docs per second.
    pub(crate) fn admit_write_docs(&self, tenant: &str, docs: usize) -> Result<(), Rejection> {
        let Some(rate) = self.limits(tenant).and_then(|l| l.write_docs_per_sec) else {
            return Ok(());
        };
        self.counters(tenant)
            .write_docs
            .try_take(self.now(), rate, rate, docs as f64)
            .map_err(|wait| Rejection {
                reason: RejectReason::TenantWriteRate,
                limit: rate,
                retry_after_ms: nanos_to_ms(wait),
            })
    }
}

fn nanos_to_ms(nanos: u64) -> u64 {
    nanos.div_ceil(1_000_000).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: u64 = 1_000_000_000;

    #[test]
    fn bucket_allows_a_burst_then_refills_at_the_rate() {
        let bucket = TokenBucket::default();
        let t0 = 10 * SEC;
        for i in 0..10 {
            assert!(bucket.try_take(t0, 10.0, 10.0, 1.0).is_ok(), "token {i}");
        }
        // Empty: the next token is 100 ms away.
        let wait = bucket.try_take(t0, 10.0, 10.0, 1.0).unwrap_err();
        assert_eq!(wait, SEC / 10);
        // Refused requests take nothing.
        assert!(bucket.try_take(t0, 10.0, 10.0, 1.0).is_err());
        // 100 ms later exactly one more fits.
        assert!(bucket.try_take(t0 + SEC / 10, 10.0, 10.0, 1.0).is_ok());
        assert!(bucket.try_take(t0 + SEC / 10, 10.0, 10.0, 1.0).is_err());
        // Idle for a long time: refills only to the burst, not beyond.
        let later = t0 + 100 * SEC;
        for _ in 0..10 {
            assert!(bucket.try_take(later, 10.0, 10.0, 1.0).is_ok());
        }
        assert!(bucket.try_take(later, 10.0, 10.0, 1.0).is_err());
    }

    #[test]
    fn bucket_weighs_cost_and_admits_an_oversized_request_into_debt() {
        let bucket = TokenBucket::default();
        let t0 = SEC;
        // 1000 bytes/s: a 600-byte request then a 500-byte one waits 100 ms.
        assert!(bucket.try_take(t0, 1000.0, 1000.0, 600.0).is_ok());
        assert_eq!(
            bucket.try_take(t0, 1000.0, 1000.0, 500.0).unwrap_err(),
            SEC / 10
        );
        // Bigger than the whole bucket: only from a full bucket.
        let full = t0 + 5 * SEC;
        assert!(bucket.try_take(full, 1000.0, 1000.0, 3000.0).is_ok());
        // ... and it leaves 3 s of debt, of which 2 s exceed the burst.
        assert_eq!(
            bucket.try_take(full, 1000.0, 1000.0, 1.0).unwrap_err(),
            2 * SEC + SEC / 1000
        );
    }

    #[test]
    fn bucket_is_safe_under_contention() {
        let bucket = Arc::new(TokenBucket::default());
        let admitted = Arc::new(AtomicU32::new(0));
        let now = 50 * SEC;
        std::thread::scope(|s| {
            for _ in 0..8 {
                let (bucket, admitted) = (Arc::clone(&bucket), Arc::clone(&admitted));
                s.spawn(move || {
                    for _ in 0..100 {
                        if bucket.try_take(now, 100.0, 100.0, 1.0).is_ok() {
                            admitted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(admitted.load(Ordering::Relaxed), 100);
    }

    fn config() -> LimitsConfig {
        LimitsConfig::parse(
            r#"{
              "default_plan": "free",
              "plans": {
                "free": {"qps": 2, "concurrent_queries": 1, "namespaces": 3},
                "pro": {"qps": 100, "weight": 4}
              },
              "tenants": {
                "acme": {"plan": "pro", "vector_dims": 8},
                "solo": {"qps": 7}
              }
            }"#,
            None,
        )
        .unwrap()
    }

    #[test]
    fn plans_defaults_and_overrides_resolve() {
        let c = config();
        let anon = c.resolve("anyone").unwrap();
        assert_eq!(anon.qps, Some(2.0));
        assert_eq!(anon.namespaces, Some(3));
        let acme = c.resolve("acme").unwrap();
        assert_eq!(acme.qps, Some(100.0));
        assert_eq!(acme.weight, Some(4));
        assert_eq!(acme.vector_dims, Some(8));
        assert_eq!(acme.namespaces, None);
        // No plan of its own: the default plan, overridden.
        let solo = c.resolve("solo").unwrap();
        assert_eq!(solo.qps, Some(7.0));
        assert_eq!(solo.concurrent_queries, Some(1));

        // No default plan: an unlisted tenant is unlimited.
        let c = LimitsConfig::parse(r#"{"plans": {"p": {"qps": 1}}}"#, None).unwrap();
        assert_eq!(c.resolve("x"), None);
        // KOSHA_TENANT_DEFAULT_PLAN supplies one.
        let c = LimitsConfig::parse(r#"{"plans": {"p": {"qps": 1}}}"#, Some("p")).unwrap();
        assert_eq!(c.resolve("x").unwrap().qps, Some(1.0));
    }

    #[test]
    fn invalid_configs_are_refused() {
        for bad in [
            r#"{"default_plan": "nope", "plans": {}}"#,
            r#"{"plans": {}, "tenants": {"a": {"plan": "nope"}}}"#,
            r#"{"plans": {"p": {"qps": 0}}}"#,
            r#"{"plans": {"p": {"qps": -1}}}"#,
            r#"{"plans": {"p": {"weight": 0}}}"#,
            r#"{"plans": {"p": {"qpz": 1}}}"#,
            // A typo in a tenant's override must not leave it unlimited.
            r#"{"plans":{},"tenants":{"a":{"qpz":1}}}"#,
            r#"{"plans": {"p": {"qps": 1}}, "tenants": {"a": {"plan": "p", "wieght": 2}}}"#,
            r#"{"plan": {}}"#,
            "not json",
        ] {
            assert!(LimitsConfig::parse(bad, None).is_err(), "{bad}");
        }
        assert!(LimitsConfig::parse("{}", Some("missing")).is_err());
    }

    #[test]
    fn limiter_enforces_concurrency_and_qps_per_tenant() {
        let limiter = TenantLimiter::new(Arc::new(ConfigLimitsProvider::from_config(config())));
        // free: one in flight.
        let held = limiter.admit_query("a").unwrap();
        let rejected = limiter.admit_query("a").err().unwrap();
        assert_eq!(rejected.reason, RejectReason::TenantConcurrency);
        assert_eq!(rejected.limit, 1.0);
        // Another tenant is unaffected.
        assert!(limiter.admit_query("b").is_ok());
        drop(held);
        // free: 2 qps; "a" spent one token, the rejected one took none.
        assert!(limiter.admit_query("a").is_ok());
        let qps = limiter.admit_query("a").err().unwrap();
        assert_eq!(qps.reason, RejectReason::TenantQps);
        assert!(qps.retry_after_ms > 0 && qps.retry_after_ms <= 500);
        assert_eq!(qps.reason.code(), crate::ErrorCode::TenantQpsExceeded);
        assert_eq!(qps.limit_json(), serde_json::json!(2));
    }

    #[test]
    fn disabled_limiter_limits_nothing() {
        let limiter = TenantLimiter::disabled();
        assert!(!limiter.enabled());
        let guards: Vec<_> = (0..1000)
            .map(|_| limiter.admit_query("t").unwrap())
            .collect();
        assert_eq!(guards.len(), 1000);
        assert!(limiter.admit_body("t", usize::MAX, true).is_ok());
        assert!(limiter.admit_write_docs("t", usize::MAX).is_ok());
        assert_eq!(limiter.weight("t"), 1);
    }

    #[test]
    fn write_body_checks_size_then_rate() {
        let c = LimitsConfig::parse(
            r#"{"default_plan": "p", "plans": {"p": {"max_body_bytes": 100,
                "write_bytes_per_sec": 150, "write_docs_per_sec": 10}}}"#,
            None,
        )
        .unwrap();
        let limiter = TenantLimiter::new(Arc::new(ConfigLimitsProvider::from_config(c)));
        let big = limiter.admit_body("t", 101, true).unwrap_err();
        assert_eq!(big.reason, RejectReason::BodyTooLarge);
        assert_eq!(big.retry_after_ms, QUOTA_RETRY_AFTER_MS);
        assert!(limiter.admit_body("t", 100, true).is_ok());
        let rate = limiter.admit_body("t", 100, true).unwrap_err();
        assert_eq!(rate.reason, RejectReason::TenantWriteRate);
        assert!(limiter.admit_write_docs("t", 10).is_ok());
        assert_eq!(
            limiter.admit_write_docs("t", 1).unwrap_err().reason,
            RejectReason::TenantWriteRate
        );
    }

    #[test]
    fn file_provider_reloads_and_keeps_the_last_good_config() {
        let dir = std::env::temp_dir().join(format!(
            "kosha-tenant-limits-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("limits.json");
        std::fs::write(&path, r#"{"plans": {"p": {"qps": 1}}}"#).unwrap();
        let provider = ConfigLimitsProvider::load(path.clone(), Some("p".into())).unwrap();
        assert_eq!(provider.limits("t").unwrap().qps, Some(1.0));
        assert!(!provider.reload().unwrap());
        std::fs::write(&path, r#"{"plans": {"p": {"qps": 5}}}"#).unwrap();
        assert!(provider.reload().unwrap());
        assert_eq!(provider.limits("t").unwrap().qps, Some(5.0));
        std::fs::write(&path, "{broken").unwrap();
        assert!(provider.reload().is_err());
        assert_eq!(provider.limits("t").unwrap().qps, Some(5.0));
        // A typo in a tenant's entry fails the reload too.
        std::fs::write(
            &path,
            r#"{"plans": {"p": {"qps": 9}}, "tenants": {"t": {"qpz": 1}}}"#,
        )
        .unwrap();
        assert!(provider.reload().is_err());
        assert_eq!(provider.limits("t").unwrap().qps, Some(5.0));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
