//! Billing on a pod (#349): the plan catalog, the gate that refuses
//! requests of tenants over their free tier or behind on payment, the plan
//! quota defaults for the tenant limiter, and the billing API. Off unless
//! `KOSHA_BILLING=on`. The rules themselves (rating, the free tier, the
//! non-payment states, the monthly job) are in `kosha_control::billing`.
//!
//! # Enforcement
//!
//! Every pod keeps a snapshot of each tenant's standing (plan, payment
//! state, used-up free-tier meters), rebuilt from the billing and usage
//! stores every `KOSHA_BILLING_REFRESH_SECS`. `dispatch` checks it right
//! after authentication ([`gate`]) and answers `402 Payment Required` with
//! a JSON body `{error, reason, plan, state, ...}` when the request is not
//! allowed. Service credentials (admin, pod-to-pod) are never gated.
//!
//! So enforcement lags: a tenant keeps going past its free allowance, or
//! after a state change made on another pod, for up to one refresh interval
//! (plus the usage flush interval for usage). A pod that cannot read the
//! stores keeps its last snapshot; a pod that never could restricts nobody.
//!
//! # Routes
//!
//! - `GET /v1/billing`: the calling tenant's plan, standing, month-to-date
//!   usage with the charge accrued so far, and invoices.
//! - `GET /v1/admin/billing/accounts`, `POST /v1/admin/billing/accounts`
//!   (`{tenant, plan}`: put a tenant on a plan, the upgrade path).
//! - `GET /v1/admin/billing/invoices[?tenant=]`.
//! - `POST /v1/admin/billing/close-period` (`{period: "YYYY-MM"}`, default
//!   last month): the monthly rating job.
//! - `POST /v1/admin/billing/sync`: copy payment results from the provider.
//! - `POST /v1/admin/billing/invoices/mark-paid` (`{invoice_id}`): record a
//!   payment collected outside the provider.
//! - `POST /v1/admin/billing/dunning`: one dunning pass now (retries and
//!   notices, `kosha_control::dunning`).
//! - `POST /v1/admin/billing/catalog/reload`: re-read the plans file.
//! - `POST /v1/billing/checkout` and `POST /v1/billing/portal` (a tenant
//!   key with the `admin` scope): a hosted page to put a payment method on
//!   file and move to the catalog's `upgrade_plan`, and the provider's
//!   customer portal. [`start_checkout`] and [`start_portal`] are the same
//!   operations as plain functions for the web console.
//! - `POST /v1/billing/webhooks/stripe`: Stripe's webhook endpoint. Not
//!   authenticated with a key: every delivery must carry a valid
//!   `Stripe-Signature` (`kosha_control::stripe_webhook`). Answered in
//!   `dispatch` before authentication ([`webhook_response`]).
//!
//! `close-period`, `sync` and dunning run from the opt-in scheduler
//! (`billing_scheduler.rs`, `KOSHA_BILLING_SCHEDULER=on`), from a CronJob
//! calling the routes, or by hand. All of them are safe to repeat and to
//! run on two pods at once. Webhooks report payments as they happen;
//! `sync` (polling) stays as the fallback for a missed delivery.
//!
//! # Plan catalog reload
//!
//! The catalog is swapped whole, after it parses and validates, and only
//! when every plan an account is on is still in it; otherwise the pod
//! keeps the catalog it has and reports why (`catalog_error` in
//! `GET /v1/admin/billing/accounts`, `kosha_billing_catalog_reloads_total`).
//! `POST /v1/admin/billing/catalog/reload` reloads on the pod that serves
//! it; `KOSHA_BILLING_PLANS_WATCH=on` makes every pod check the file's
//! modification time and size at each standings refresh.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use kosha_control::billing::{
    rate, total_cents, AccountState, Biller, BillingPeriod, BillingProvider, BillingStore, Block,
    Catalog, CheckoutRequest, Invoice, InvoiceStatus, MemoryBillingStore, Operation, PlanQuotas,
    ProviderInvoiceStatus, ProviderSession, TenantStanding,
};
use kosha_control::billing_ledger::{
    change_plan, handle_webhook_event, BillingContext, BillingLedger, MemoryBillingLedger,
};
use kosha_control::dunning::{run_dunning, DunningReport, LogNotifier, Notifier};
use kosha_control::stripe_webhook;
use kosha_control::usage::UsageStore;
use kosha_core::KoshaError;

use crate::tenant_limits::{LimitsProvider, TenantLimits};
use crate::{
    is_search_route, is_write_path, json_error, json_ok, parse_method_and_path, unix_millis,
    valid_name_component, AppState, SERVICE_TENANT,
};

/// The time source: Unix milliseconds. Tests substitute their own.
pub(crate) type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

type Standings = BTreeMap<String, TenantStanding>;

/// Billing on this pod. See the module doc.
pub(crate) struct Billing {
    catalog: RwLock<Arc<Catalog>>,
    /// Where the catalog came from (`None` in tests that hand one over).
    plans_file: Option<PathBuf>,
    /// Check `plans_file` for changes at every refresh.
    watch_plans: bool,
    /// Modification time and size of `plans_file` when last read.
    plans_stamp: Mutex<Option<(SystemTime, u64)>>,
    last_catalog_error: Mutex<Option<String>>,
    pub(crate) store: Arc<dyn BillingStore>,
    usage: Arc<dyn UsageStore>,
    pub(crate) provider: Option<Arc<dyn BillingProvider>>,
    pub(crate) ledger: Arc<dyn BillingLedger>,
    pub(crate) notifier: Arc<dyn Notifier>,
    clock: Clock,
    standings: RwLock<Arc<Standings>>,
    refresh_interval: Duration,
    last_refresh_error: Mutex<Option<String>>,
    /// Whether a refresh has ever succeeded. Until one has, this pod does
    /// not know which tenants have accounts, so it must not hand out the
    /// default plan's quotas either (see `LimitsProvider for Billing`).
    refreshed: AtomicBool,
    /// `KOSHA_STRIPE_WEBHOOK_SECRET`: the route is off without one.
    webhook: Option<WebhookConfig>,
    /// `KOSHA_BILLING_RETURN_URL`: checkout and portal are off without it.
    return_url: Option<String>,
    /// `KOSHA_BILLING_SCHEDULER=on`.
    pub(crate) scheduler: Option<crate::billing_scheduler::SchedulerConfig>,
    pub(crate) stats: Arc<BillingStats>,
}

/// The Stripe webhook endpoint's settings.
#[derive(Clone)]
pub(crate) struct WebhookConfig {
    /// Every accepted signing secret (more than one while rolling).
    pub secrets: Vec<String>,
    pub tolerance_secs: u64,
}

impl std::fmt::Debug for WebhookConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookConfig")
            .field("secrets", &self.secrets.len())
            .field("tolerance_secs", &self.tolerance_secs)
            .finish()
    }
}

/// Counters for `/metrics` (`pipeline_metrics.rs`).
#[derive(Debug, Default)]
pub(crate) struct BillingStats {
    pub webhooks_processed: AtomicU64,
    pub webhooks_duplicate: AtomicU64,
    pub webhooks_bad_signature: AtomicU64,
    pub webhooks_invalid: AtomicU64,
    pub webhooks_failed: AtomicU64,
    pub scheduler_ran: AtomicU64,
    pub scheduler_not_leader: AtomicU64,
    pub scheduler_failed: AtomicU64,
    /// Unix ms of the last tick this pod ran as the leader.
    pub scheduler_last_run_ms: AtomicU64,
    /// Billing-clock ms of the scheduler's last payment sync; 0 never.
    pub last_sync_ms: AtomicU64,
    pub notices_sent: AtomicU64,
    pub notices_failed: AtomicU64,
    pub payment_retries: AtomicU64,
    pub payments_collected: AtomicU64,
    pub catalog_reloads: AtomicU64,
    pub catalog_reloads_rejected: AtomicU64,
    pub refresh_failures: AtomicU64,
}

/// Whether `base` may be sent the Stripe secret key: `https`, or plain
/// `http` to this machine only (a local stub).
pub(crate) fn stripe_base_allowed(base: &str) -> bool {
    if base.starts_with("https://") {
        return true;
    }
    let Some(rest) = base.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    // No userinfo tricks: `http://localhost@elsewhere` is not local.
    if authority.contains('@') {
        return false;
    }
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(v6),
        None => authority.split(':').next().unwrap_or(authority),
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn flag_on(raw: Option<&str>) -> bool {
    raw.is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "on" | "true" | "1"))
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

impl Billing {
    pub(crate) fn new(
        catalog: Catalog,
        store: Arc<dyn BillingStore>,
        usage: Arc<dyn UsageStore>,
        provider: Option<Arc<dyn BillingProvider>>,
        clock: Clock,
    ) -> Self {
        Self {
            catalog: RwLock::new(Arc::new(catalog)),
            plans_file: None,
            watch_plans: false,
            plans_stamp: Mutex::new(None),
            last_catalog_error: Mutex::new(None),
            store,
            usage,
            provider,
            ledger: Arc::new(MemoryBillingLedger::new()),
            notifier: Arc::new(LogNotifier),
            clock,
            standings: RwLock::new(Arc::new(Standings::new())),
            refresh_interval: Duration::from_secs(60),
            last_refresh_error: Mutex::new(None),
            refreshed: AtomicBool::new(false),
            webhook: None,
            return_url: None,
            scheduler: None,
            stats: Arc::new(BillingStats::default()),
        }
    }

    /// Use `ledger` (plan changes, webhook ids, dunning state).
    pub(crate) fn with_ledger(mut self, ledger: Arc<dyn BillingLedger>) -> Self {
        self.ledger = ledger;
        self
    }

    /// Send dunning notices through `notifier`.
    pub(crate) fn with_notifier(mut self, notifier: Arc<dyn Notifier>) -> Self {
        self.notifier = notifier;
        self
    }

    /// Accept Stripe webhooks signed with one of `secrets`.
    pub(crate) fn with_webhook_secrets(
        mut self,
        secrets: Vec<String>,
        tolerance_secs: u64,
    ) -> Self {
        self.webhook = Some(WebhookConfig {
            secrets,
            tolerance_secs,
        });
        self
    }

    /// Serve checkout and portal, sending users back to `return_url`.
    pub(crate) fn with_return_url(mut self, return_url: &str) -> Self {
        self.return_url = Some(return_url.trim_end_matches('/').to_string());
        self
    }

    /// Read the catalog from `path` on reload (and watch it with `watch`).
    pub(crate) fn with_plans_file(mut self, path: PathBuf, watch: bool) -> Self {
        *self.plans_stamp.lock().unwrap() = file_stamp(&path);
        self.plans_file = Some(path);
        self.watch_plans = watch;
        self
    }

    /// The catalog in force.
    pub(crate) fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog.read().unwrap_or_else(|e| e.into_inner()))
    }

    /// Billing as the environment asks for it, or `None` (the default) when
    /// `KOSHA_BILLING` is not on. `usage` is the pod's usage store; billing
    /// without usage metering is a configuration error, as is a missing or
    /// invalid plans file.
    pub(crate) fn from_env(usage: Option<Arc<dyn UsageStore>>) -> Result<Option<Self>, String> {
        if !flag_on(std::env::var("KOSHA_BILLING").ok().as_deref()) {
            return Ok(None);
        }
        let usage = usage.ok_or(
            "KOSHA_BILLING=on needs usage metering: set KOSHA_USAGE_METERING=on on every pod",
        )?;
        let path = env_nonempty("KOSHA_BILLING_PLANS_FILE")
            .ok_or("KOSHA_BILLING=on needs KOSHA_BILLING_PLANS_FILE (the plan catalog)")?;
        let catalog = read_catalog(std::path::Path::new(&path))?;
        let provider: Option<Arc<dyn BillingProvider>> =
            match env_nonempty("KOSHA_BILLING_PROVIDER").as_deref() {
                None | Some("none") => None,
                Some("stripe") => {
                    let api_key = env_nonempty("KOSHA_STRIPE_API_KEY")
                        .ok_or("KOSHA_BILLING_PROVIDER=stripe needs KOSHA_STRIPE_API_KEY")?;
                    let base = env_nonempty("KOSHA_STRIPE_API_BASE")
                        .unwrap_or_else(|| "https://api.stripe.com".to_string());
                    if !stripe_base_allowed(&base) {
                        return Err(
                            "KOSHA_STRIPE_API_BASE must be an https:// URL (plain http:// is \
                             accepted for localhost only): the Stripe secret key is sent to it"
                                .into(),
                        );
                    }
                    Some(Arc::new(StripeProvider::new(
                        &base,
                        &api_key,
                        catalog.dunning.payment_terms_days,
                    )?))
                }
                Some(other) => {
                    return Err(format!(
                        "KOSHA_BILLING_PROVIDER={other:?}: expected 'stripe' or 'none'"
                    ))
                }
            };
        let (store, ledger) = stores_from_env()?;
        let mut billing = Self::new(catalog, store, usage, provider, Arc::new(unix_millis))
            .with_ledger(ledger)
            .with_notifier(crate::billing_notify::notifier_from_env()?)
            .with_plans_file(
                PathBuf::from(&path),
                flag_on(std::env::var("KOSHA_BILLING_PLANS_WATCH").ok().as_deref()),
            );
        billing.refresh_interval = Duration::from_secs(
            env_nonempty("KOSHA_BILLING_REFRESH_SECS")
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|s| *s > 0)
                .unwrap_or(60),
        );
        if let Some(raw) = env_nonempty("KOSHA_STRIPE_WEBHOOK_SECRET") {
            let secrets: Vec<String> = raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if secrets.is_empty() {
                return Err("KOSHA_STRIPE_WEBHOOK_SECRET has no secret in it".into());
            }
            let tolerance = match env_nonempty("KOSHA_STRIPE_WEBHOOK_TOLERANCE_SECS") {
                None => stripe_webhook::DEFAULT_TOLERANCE_SECS,
                Some(v) => v.parse::<u64>().ok().filter(|s| *s > 0).ok_or(
                    "KOSHA_STRIPE_WEBHOOK_TOLERANCE_SECS must be a positive number of seconds",
                )?,
            };
            billing = billing.with_webhook_secrets(secrets, tolerance);
        }
        if let Some(url) = env_nonempty("KOSHA_BILLING_RETURN_URL") {
            if origin_of(&url).is_none() {
                return Err(
                    "KOSHA_BILLING_RETURN_URL must be an https:// URL (plain http:// for \
                     localhost only)"
                        .into(),
                );
            }
            billing = billing.with_return_url(&url);
        }
        billing.scheduler = crate::billing_scheduler::SchedulerConfig::from_env()?;
        let catalog = billing.catalog();
        println!(
            "billing: on ({} plan(s) from {path}, default plan {}, provider {}, refresh every {}s, \
             notices via {}, webhooks {}, self-service {}, scheduler {})",
            catalog.plans.len(),
            catalog.default_plan.as_deref().unwrap_or("none"),
            billing.provider.as_ref().map_or("none", |p| p.name()),
            billing.refresh_interval.as_secs(),
            billing.notifier.name(),
            if billing.webhook.is_some() { "on" } else { "off" },
            match (&billing.return_url, &catalog.upgrade_plan) {
                (Some(_), Some(plan)) => format!("on (upgrade plan {plan})"),
                (Some(_), None) => "portal only (no upgrade_plan in the catalog)".into(),
                _ => "off".into(),
            },
            if billing.scheduler.is_some() { "on" } else { "off" },
        );
        if let Err(e) = billing.refresh() {
            eprintln!("WARN: billing: initial refresh failed; nobody is restricted until one succeeds: {e}");
        }
        Ok(Some(billing))
    }

    pub(crate) fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    pub(crate) fn biller<'a>(&'a self, catalog: &'a Catalog) -> Biller<'a> {
        Biller {
            catalog,
            store: self.store.as_ref(),
            usage: self.usage.as_ref(),
            provider: self.provider.as_deref(),
            plan_changes: Some(self.ledger.as_ref()),
        }
    }

    pub(crate) fn context<'a>(&'a self, catalog: &'a Catalog) -> BillingContext<'a> {
        BillingContext {
            catalog,
            store: self.store.as_ref(),
            ledger: self.ledger.as_ref(),
            provider: self.provider.as_deref(),
        }
    }

    /// One dunning pass now (see `kosha_control::dunning`).
    pub(crate) fn run_dunning(&self) -> Result<DunningReport, KoshaError> {
        let catalog = self.catalog();
        let report = run_dunning(
            &self.context(&catalog),
            self.notifier.as_ref(),
            self.now_ms(),
        )?;
        let stats = &self.stats;
        stats
            .notices_sent
            .fetch_add(report.notices_sent.len() as u64, Ordering::Relaxed);
        stats
            .notices_failed
            .fetch_add(report.notices_failed.len() as u64, Ordering::Relaxed);
        stats
            .payment_retries
            .fetch_add(report.retries.len() as u64, Ordering::Relaxed);
        stats
            .payments_collected
            .fetch_add(report.collected.len() as u64, Ordering::Relaxed);
        for (id, error) in &report.notices_failed {
            eprintln!("WARN: billing: notice {id} not sent: {error}");
        }
        if !report.collected.is_empty() || !report.retries.is_empty() {
            self.refresh_logged();
        }
        Ok(report)
    }

    /// Re-read the plans file and swap the catalog in if it is valid and
    /// keeps every plan an account is on. `Ok(false)` when there is no
    /// file to read; on `Err` the catalog in force is kept.
    pub(crate) fn reload_catalog(&self) -> Result<bool, String> {
        let Some(path) = &self.plans_file else {
            return Ok(false);
        };
        let stamp = file_stamp(path);
        let result = read_catalog(path).and_then(|catalog| {
            let accounts = self
                .store
                .accounts()
                .map_err(|e| format!("cannot check the accounts against it: {e}"))?;
            let orphaned: Vec<String> = accounts
                .iter()
                .filter(|a| catalog.plan(&a.plan).is_none())
                .map(|a| format!("{} ({})", a.tenant, a.plan))
                .collect();
            if !orphaned.is_empty() {
                return Err(format!(
                    "{}: plans in use are missing; move these tenants first: {}",
                    path.display(),
                    orphaned.join(", ")
                ));
            }
            Ok(catalog)
        });
        match result {
            Ok(catalog) => {
                *self.catalog.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(catalog);
                *self.plans_stamp.lock().unwrap() = stamp;
                *self.last_catalog_error.lock().unwrap() = None;
                self.stats.catalog_reloads.fetch_add(1, Ordering::Relaxed);
                println!("billing: plan catalog reloaded from {}", path.display());
                self.refresh_logged();
                Ok(true)
            }
            Err(e) => {
                // Remember the stamp: a watcher must not retry the same
                // bad file every refresh.
                *self.plans_stamp.lock().unwrap() = stamp;
                *self.last_catalog_error.lock().unwrap() = Some(e.clone());
                self.stats
                    .catalog_reloads_rejected
                    .fetch_add(1, Ordering::Relaxed);
                eprintln!("WARN: billing: plan catalog not reloaded, keeping the current one: {e}");
                Err(e)
            }
        }
    }

    /// With `KOSHA_BILLING_PLANS_WATCH=on`: reload when the file's
    /// modification time or size changed since it was last read.
    fn reload_if_changed(&self) {
        let Some(path) = self.plans_file.as_ref().filter(|_| self.watch_plans) else {
            return;
        };
        if file_stamp(path) != *self.plans_stamp.lock().unwrap() {
            let _ = self.reload_catalog();
        }
    }

    /// Rebuild the standings snapshot. On error the previous snapshot
    /// stays in force.
    pub(crate) fn refresh(&self) -> Result<(), KoshaError> {
        let catalog = self.catalog();
        match self.biller(&catalog).standings(self.now_ms()) {
            Ok(standings) => {
                *self.standings.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(standings);
                *self.last_refresh_error.lock().unwrap() = None;
                self.refreshed.store(true, Ordering::Release);
                Ok(())
            }
            Err(e) => {
                self.stats.refresh_failures.fetch_add(1, Ordering::Relaxed);
                *self.last_refresh_error.lock().unwrap() = Some(e.to_string());
                Err(e)
            }
        }
    }

    /// Refresh after a change made on this pod; a failure is logged and
    /// the next periodic refresh retries.
    pub(crate) fn refresh_logged(&self) {
        if let Err(e) = self.refresh() {
            eprintln!("WARN: billing: refresh failed, keeping previous standings: {e}");
        }
    }

    fn snapshot(&self) -> Arc<Standings> {
        Arc::clone(&self.standings.read().unwrap_or_else(|e| e.into_inner()))
    }

    /// `tenant`'s standing as this pod enforces it, if billing knows it.
    pub(crate) fn standing(&self, tenant: &str) -> Option<TenantStanding> {
        self.snapshot().get(tenant).cloned()
    }

    /// The name of the plan `tenant` is on: its standing's, else the
    /// catalog default.
    fn plan_name(&self, tenant: &str) -> Option<String> {
        self.snapshot()
            .get(tenant)
            .map(|s| s.plan.clone())
            .or_else(|| self.catalog().default_plan.clone())
    }
}

/// Modification time and size of `path`, when it can be read.
fn file_stamp(path: &std::path::Path) -> Option<(SystemTime, u64)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.modified().ok()?, meta.len()))
}

/// Read and validate a catalog file.
fn read_catalog(path: &std::path::Path) -> Result<Catalog, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    Catalog::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
}

type Stores = (Arc<dyn BillingStore>, Arc<dyn BillingLedger>);

fn stores_from_env() -> Result<Stores, String> {
    #[cfg(feature = "postgres")]
    if let Some(url) = env_nonempty("DATABASE_URL") {
        let store = kosha_control::billing::PgBillingStore::new(&url)?;
        let ledger = kosha_control::billing_ledger::PgBillingLedger::new(&url)?;
        println!(
            "billing: store is postgres (kosha.billing_accounts, kosha.billing_invoices and \
             the billing ledger of migration 011)"
        );
        return Ok((Arc::new(store), Arc::new(ledger)));
    }
    println!(
        "billing: store is in-memory (no DATABASE_URL): accounts and invoices are per pod \
         and lost on restart"
    );
    Ok((
        Arc::new(MemoryBillingStore::new()),
        Arc::new(MemoryBillingLedger::new()),
    ))
}

// ─── Plan quota defaults ────────────────────────────────────────────────────

fn limits_of(quotas: &PlanQuotas) -> TenantLimits {
    TenantLimits {
        qps: quotas.qps,
        concurrent_queries: quotas.concurrent_queries,
        write_docs_per_sec: quotas.write_docs_per_sec,
        write_bytes_per_sec: quotas.write_bytes_per_sec,
        namespaces: quotas.namespaces,
        docs_per_namespace: quotas.docs_per_namespace,
        vector_dims: quotas.vector_dims,
        max_body_bytes: quotas.max_body_bytes,
        weight: quotas.weight,
    }
}

/// A tenant's limits are its billing plan's `limits`. The tenant limiter
/// lays the operator's limits file over these
/// (`TenantLimiter::with_plan_defaults`).
///
/// No plan quotas at all until the first refresh has succeeded: with an
/// empty snapshot every tenant looks like it has no account, and a paying
/// tenant would be held to the default (typically free) plan's quotas.
impl LimitsProvider for Billing {
    fn limits(&self, tenant: &str) -> Option<TenantLimits> {
        if !self.refreshed.load(Ordering::Acquire) {
            return None;
        }
        let plan = self.plan_name(tenant)?;
        let catalog = self.catalog();
        let quotas = &catalog.plan(&plan)?.limits;
        (*quotas != PlanQuotas::default()).then(|| limits_of(quotas))
    }
}

// ─── The gate ───────────────────────────────────────────────────────────────

/// What `request_line` does, as far as billing cares.
pub(crate) fn operation(request_line: &str) -> Operation {
    let Some((method, target)) = parse_method_and_path(request_line) else {
        return Operation::Other;
    };
    let path = target.split('?').next().unwrap_or(target);
    if method == "GET" && (path == "/v1/billing" || path == "/v1/usage") {
        return Operation::Account;
    }
    // Paying is never blocked: a suspended tenant must be able to put a
    // payment method on file.
    if method == "POST" && (path == CHECKOUT_PATH || path == PORTAL_PATH) {
        return Operation::Account;
    }
    if is_search_route(request_line) {
        return Operation::Query;
    }
    if !is_write_path(request_line) {
        return Operation::Other;
    }
    let deletes = method == "DELETE"
        || path == "/delete"
        || (path.starts_with("/v1/namespaces/") && path.ends_with("/delete"));
    if deletes {
        Operation::Delete
    } else {
        Operation::Write
    }
}

/// The `402 Payment Required` for a request billing refuses.
fn payment_required(standing: &TenantStanding, block: &Block) -> String {
    // Logged once, by `log_request`, from the body's reason/plan fields.
    let mut body = serde_json::json!({
        "error": block.message(),
        "reason": block.reason(),
        "plan": standing.plan,
        "state": standing.state,
    });
    if let Block::FreeTier(excess) = block {
        body["meter"] = serde_json::json!(excess.meter);
        body["used"] = serde_json::json!(excess.used);
        body["included"] = serde_json::json!(excess.included);
    }
    let body = body.to_string();
    format!(
        "HTTP/1.1 402 Payment Required\r\ncontent-length: {}\r\ncontent-type: application/json\r\n\r\n{body}",
        body.len()
    )
}

/// The billing check on an authenticated request: `Some(response)` when
/// `tenant` may not make it. `None` with billing off, for service
/// credentials, and for tenants billing has nothing against.
pub(crate) fn gate(state: &AppState, tenant: &str, request_line: &str) -> Option<String> {
    let billing = state.billing.as_ref()?;
    if tenant == SERVICE_TENANT {
        return None;
    }
    let standings = billing.snapshot();
    let standing = standings.get(tenant)?;
    let block = standing.block(operation(request_line))?;
    Some(payment_required(standing, &block))
}

impl AppState {
    /// Keep the standings snapshot fresh for the life of the process.
    pub(crate) fn spawn_billing_refresher(self: &Arc<Self>) {
        let Some(billing) = self.billing.clone() else {
            return;
        };
        let _ = std::thread::Builder::new()
            .name("kosha-billing".into())
            .spawn(move || loop {
                std::thread::sleep(billing.refresh_interval);
                billing.reload_if_changed();
                billing.refresh_logged();
            });
    }
}

// ─── Routes ─────────────────────────────────────────────────────────────────

/// A store or provider failure. The detail (driver and SQL text, provider
/// messages) is for the operator: logged, and shown on admin routes only.
fn unavailable(error: &KoshaError, admin: bool) -> String {
    eprintln!("WARN: billing: {error}");
    match (error, admin) {
        (KoshaError::InvalidQuery(message), _) => json_error(400, message),
        (_, true) => json_error(503, &format!("billing unavailable: {error}")),
        (_, false) => json_error(503, "billing unavailable"),
    }
}

fn json_body(body: &[u8]) -> Result<serde_json::Value, String> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(serde_json::json!({}));
    }
    serde_json::from_slice(body).map_err(|e| json_error(400, &format!("invalid JSON body: {e}")))
}

/// `POST`: a tenant starts a self-service checkout.
pub(crate) const CHECKOUT_PATH: &str = "/v1/billing/checkout";
/// `POST`: a tenant opens the provider's customer portal.
pub(crate) const PORTAL_PATH: &str = "/v1/billing/portal";
/// `POST`: Stripe's webhook deliveries (answered in `dispatch`, before
/// authentication).
pub(crate) const STRIPE_WEBHOOK_PATH: &str = "/v1/billing/webhooks/stripe";

pub(crate) const ROUTES: [(&str, &str); 11] = [
    ("GET", "/v1/billing"),
    ("POST", "/v1/billing/checkout"),
    ("POST", "/v1/billing/portal"),
    ("GET", "/v1/admin/billing/accounts"),
    ("POST", "/v1/admin/billing/accounts"),
    ("GET", "/v1/admin/billing/invoices"),
    ("POST", "/v1/admin/billing/close-period"),
    ("POST", "/v1/admin/billing/sync"),
    ("POST", "/v1/admin/billing/invoices/mark-paid"),
    ("POST", "/v1/admin/billing/dunning"),
    ("POST", "/v1/admin/billing/catalog/reload"),
];

/// The billing routes. `None` when `request_line` is not one of them.
pub(crate) fn route(
    request_line: &str,
    body: &[u8],
    tenant: &str,
    state: &AppState,
) -> Option<String> {
    let (method, target) = parse_method_and_path(request_line)?;
    let path = target.split('?').next().unwrap_or(target);
    if !ROUTES.contains(&(method, path)) {
        return None;
    }
    let Some(billing) = state.billing.as_deref() else {
        return Some(json_error(
            404,
            "billing is off on this pod (KOSHA_BILLING=on enables it)",
        ));
    };
    Some(match (method, path) {
        ("GET", "/v1/billing") => handle_tenant_billing(billing, tenant),
        ("POST", "/v1/billing/checkout") => handle_checkout(billing, tenant, body),
        ("POST", "/v1/billing/portal") => handle_portal(billing, tenant, body),
        ("GET", "/v1/admin/billing/accounts") => handle_list_accounts(billing),
        ("POST", "/v1/admin/billing/accounts") => handle_put_account(billing, body),
        ("GET", "/v1/admin/billing/invoices") => handle_list_invoices(billing, request_line),
        ("POST", "/v1/admin/billing/close-period") => handle_close_period(billing, body, state),
        ("POST", "/v1/admin/billing/sync") => handle_sync(billing),
        ("POST", "/v1/admin/billing/dunning") => match billing.run_dunning() {
            Ok(report) => json_ok(&report),
            Err(e) => unavailable(&e, true),
        },
        ("POST", "/v1/admin/billing/catalog/reload") => match billing.reload_catalog() {
            Ok(true) => json_ok(&serde_json::json!({"reloaded": true})),
            Ok(false) => json_error(409, "no plans file to reload (KOSHA_BILLING_PLANS_FILE)"),
            Err(e) => json_error(422, &e),
        },
        _ => handle_mark_paid(billing, body),
    })
}

fn invoice_json(invoice: &Invoice) -> serde_json::Value {
    serde_json::to_value(invoice).unwrap_or(serde_json::Value::Null)
}

/// `GET /v1/billing`.
fn handle_tenant_billing(billing: &Billing, tenant: &str) -> String {
    let now_ms = billing.now_ms();
    let catalog = billing.catalog();
    let Some(plan_name) = billing.plan_name(tenant) else {
        return json_error(404, "no billing plan applies to this tenant");
    };
    let Some(plan) = catalog.plan(&plan_name) else {
        return json_error(404, "no billing plan applies to this tenant");
    };
    let read = || -> Result<_, KoshaError> {
        Ok((
            billing.biller(&catalog).month_to_date(tenant, now_ms)?,
            billing.store.invoices(Some(tenant))?,
            billing.ledger.payment_method(tenant)?,
        ))
    };
    let (totals, invoices, method) = match read() {
        Ok(read) => read,
        Err(e) => return unavailable(&e, false),
    };
    let period = BillingPeriod::containing(now_ms);
    let lines = rate(plan, &totals, period.hours());
    let standing = billing.standing(tenant);
    json_ok(&serde_json::json!({
        "tenant": tenant,
        "plan": plan_name,
        "kind": plan.kind,
        "state": standing.as_ref().map_or(AccountState::Active, |s| s.state),
        "free_tier_exceeded": standing.map(|s| s.free_tier_exceeded).unwrap_or_default(),
        "period": period.to_string(),
        "usage": totals,
        // What the month would cost if it ended now. Storage accrues: it
        // is byte-hours so far over the hours of the whole month.
        "accrued": {"lines": lines, "total_cents": total_cents(&lines)},
        "invoices": invoices.iter().map(invoice_json).collect::<Vec<_>>(),
        // Whether a payment method is on file (never which one).
        "payment_method_on_file": method.is_some_and(|m| m.payment_method.is_some()),
        "upgrade_plan": catalog.upgrade_plan,
    }))
}

/// `GET /v1/admin/billing/accounts`: every account with the standing this
/// pod enforces, and the pod's refresh state.
fn handle_list_accounts(billing: &Billing) -> String {
    let accounts = match billing.store.accounts() {
        Ok(accounts) => accounts,
        Err(e) => return unavailable(&e, true),
    };
    let standings = billing.snapshot();
    let catalog = billing.catalog();
    let accounts: Vec<serde_json::Value> = accounts
        .iter()
        .map(|account| {
            serde_json::json!({
                "tenant": account.tenant,
                "plan": account.plan,
                "plan_known": catalog.plan(&account.plan).is_some(),
                "provider_customer_id": account.provider_customer_id,
                "created_at_ms": account.created_at_ms,
                "standing": standings.get(&account.tenant),
            })
        })
        .collect();
    json_ok(&serde_json::json!({
        "accounts": accounts,
        "default_plan": catalog.default_plan,
        "upgrade_plan": catalog.upgrade_plan,
        "dunning": catalog.dunning,
        "plans": catalog.plans,
        "provider": billing.provider.as_ref().map(|p| p.name().to_string()),
        "notifier": billing.notifier.name(),
        "webhooks": billing.webhook.is_some(),
        "scheduler": billing.scheduler.is_some(),
        "last_refresh_error": *billing.last_refresh_error.lock().unwrap(),
        "catalog_error": *billing.last_catalog_error.lock().unwrap(),
    }))
}

/// `POST /v1/admin/billing/accounts` `{tenant, plan}`.
fn handle_put_account(billing: &Billing, body: &[u8]) -> String {
    let body = match json_body(body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let (Some(tenant), Some(plan)) = (body["tenant"].as_str(), body["plan"].as_str()) else {
        return json_error(400, "expected {\"tenant\": ..., \"plan\": ...}");
    };
    if !valid_name_component(tenant) || tenant == SERVICE_TENANT {
        return json_error(400, "invalid tenant");
    }
    if billing.catalog().plan(plan).is_none() {
        return json_error(400, &format!("unknown plan {plan:?}"));
    }
    // Recorded as a plan change, so the month it happens in is prorated.
    let put = || {
        change_plan(
            billing.store.as_ref(),
            billing.ledger.as_ref(),
            tenant,
            plan,
            None,
            "admin",
            billing.now_ms(),
        )
    };
    match put() {
        Ok(account) => {
            billing.refresh_logged();
            json_ok(&serde_json::json!({
                "account": account,
                "standing": billing.standing(tenant),
            }))
        }
        Err(e) => unavailable(&e, true),
    }
}

/// `GET /v1/admin/billing/invoices[?tenant=]`.
fn handle_list_invoices(billing: &Billing, request_line: &str) -> String {
    let tenant = crate::usage::query_param(request_line, "tenant").filter(|t| !t.is_empty());
    match billing.store.invoices(tenant.as_deref()) {
        Ok(invoices) => json_ok(&serde_json::json!({
            "invoices": invoices.iter().map(invoice_json).collect::<Vec<_>>(),
        })),
        Err(e) => unavailable(&e, true),
    }
}

/// `POST /v1/admin/billing/close-period` `{period}`: the monthly job.
fn handle_close_period(billing: &Billing, body: &[u8], state: &AppState) -> String {
    let body = match json_body(body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let now_ms = billing.now_ms();
    let period = match body.get("period") {
        None | Some(serde_json::Value::Null) => BillingPeriod::containing(now_ms).previous(),
        Some(value) => match value.as_str().map(BillingPeriod::parse) {
            Some(Ok(period)) => period,
            Some(Err(e)) => return json_error(400, &e),
            None => return json_error(400, "'period' must be a string: YYYY-MM"),
        },
    };
    // Deliver what this pod has spooled first. Events still spooled on
    // other pods are not on the invoice; close a period only once every
    // pod has flushed past its end.
    if let Some(meter) = state.usage.as_ref() {
        let _ = meter.flush();
    }
    let catalog = billing.catalog();
    match billing.biller(&catalog).close_period(period, now_ms) {
        Ok(report) => {
            billing.refresh_logged();
            json_ok(&report)
        }
        Err(e) => unavailable(&e, true),
    }
}

/// `POST /v1/admin/billing/sync`.
fn handle_sync(billing: &Billing) -> String {
    let catalog = billing.catalog();
    match billing.biller(&catalog).sync_payments(billing.now_ms()) {
        Ok(report) => {
            billing.refresh_logged();
            json_ok(&report)
        }
        Err(e) => unavailable(&e, true),
    }
}

/// `POST /v1/admin/billing/invoices/mark-paid` `{invoice_id}`.
fn handle_mark_paid(billing: &Billing, body: &[u8]) -> String {
    let body = match json_body(body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    let Some(id) = body["invoice_id"].as_str() else {
        return json_error(400, "expected {\"invoice_id\": ...}");
    };
    let mark = || -> Result<Option<Invoice>, KoshaError> {
        let Some(invoice) = billing.store.invoice(id)? else {
            return Ok(None);
        };
        if invoice.status == InvoiceStatus::Open {
            billing
                .store
                .set_invoice_status(id, InvoiceStatus::Paid, Some(billing.now_ms()))?;
        }
        billing.store.invoice(id)
    };
    match mark() {
        Ok(None) => json_error(404, "no such invoice"),
        Ok(Some(invoice)) if invoice.status != InvoiceStatus::Paid => {
            json_error(409, "the invoice is void and cannot be paid")
        }
        Ok(Some(invoice)) => {
            billing.refresh_logged();
            json_ok(&serde_json::json!({"invoice": invoice_json(&invoice)}))
        }
        Err(e) => unavailable(&e, true),
    }
}

// ─── Self-service: checkout and portal ──────────────────────────────────────

/// Why a self-service call was refused: an HTTP status and a message safe
/// to show the tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SelfServiceError {
    pub status: u16,
    pub message: String,
}

impl SelfServiceError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn response(&self) -> String {
        json_error(self.status, &self.message)
    }
}

/// `scheme://host[:port]` of `url` when it may be a redirect target:
/// `https`, or plain `http` to this machine (local development).
pub(crate) fn origin_of(url: &str) -> Option<&str> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_len];
    if authority.is_empty() || authority.contains('@') || authority.contains('\\') {
        return None;
    }
    if url.starts_with("http://") && !stripe_base_allowed(url) {
        return None;
    }
    Some(&url[..url.len() - rest.len() + authority_len])
}

/// `requested`, when it is on the same origin as the configured return
/// URL, else that URL with `default_query`. A tenant cannot make the
/// provider redirect its users anywhere else.
fn redirect_url(
    configured: &str,
    requested: Option<&str>,
    default_query: &str,
) -> Result<String, SelfServiceError> {
    match requested {
        None => Ok(format!("{configured}{default_query}")),
        Some(url) if origin_of(url).is_some() && origin_of(url) == origin_of(configured) => {
            Ok(url.to_string())
        }
        Some(_) => Err(SelfServiceError::new(
            400,
            "redirect URLs must be on the configured billing return URL's origin",
        )),
    }
}

/// The provider customer of `tenant`, created when it has none. It is
/// recorded on the tenant's account only when it has one, or would be
/// billed on the default plan anyway: starting a checkout must not put a
/// tenant on a plan. A completed checkout records it in any case.
fn customer_for(
    billing: &Billing,
    provider: &dyn BillingProvider,
    catalog: &Catalog,
    tenant: &str,
) -> Result<String, KoshaError> {
    let account = billing.store.account(tenant)?;
    if let Some(customer) = account
        .as_ref()
        .and_then(|a| a.provider_customer_id.clone())
    {
        return Ok(customer);
    }
    let customer = provider.create_customer(tenant)?;
    if let Some(plan) = catalog.plan_name_for(account.as_ref()) {
        billing
            .store
            .set_provider_customer(tenant, plan, &customer, billing.now_ms())?;
    }
    Ok(customer)
}

/// Start a self-service checkout for `tenant`: a hosted provider page
/// where the user enters a payment method; completing it (reported by the
/// `checkout.session.completed` webhook) moves the tenant to the catalog's
/// `upgrade_plan` and puts the payment method on file. `success_url` and
/// `cancel_url` default to `KOSHA_BILLING_RETURN_URL` with
/// `?checkout=success` / `?checkout=cancel` and must be on its origin.
///
/// For the web console: call it for the signed-in user's tenant (after
/// checking the user may manage billing) and redirect to the returned
/// `url`.
pub(crate) fn start_checkout(
    billing: &Billing,
    tenant: &str,
    success_url: Option<&str>,
    cancel_url: Option<&str>,
) -> Result<ProviderSession, SelfServiceError> {
    let catalog = billing.catalog();
    let (Some(return_url), Some(provider)) = (&billing.return_url, &billing.provider) else {
        return Err(SelfServiceError::new(
            404,
            "self-service billing is off on this pod (KOSHA_BILLING_RETURN_URL and a \
             payment provider enable it)",
        ));
    };
    let Some(plan) = catalog.upgrade_plan.clone() else {
        return Err(SelfServiceError::new(
            404,
            "no plan is offered for self-service upgrade",
        ));
    };
    if !valid_name_component(tenant) || tenant == SERVICE_TENANT {
        return Err(SelfServiceError::new(400, "invalid tenant"));
    }
    let request = CheckoutRequest {
        tenant: tenant.to_string(),
        plan,
        success_url: redirect_url(return_url, success_url, "?checkout=success")?,
        cancel_url: redirect_url(return_url, cancel_url, "?checkout=cancel")?,
    };
    let session = customer_for(billing, provider.as_ref(), &catalog, tenant)
        .and_then(|customer| provider.create_checkout_session(&customer, &request));
    session.map_err(|e| {
        eprintln!("WARN: billing: checkout for {tenant} failed: {e}");
        SelfServiceError::new(503, "billing provider unavailable")
    })
}

/// Open the provider's customer portal for `tenant` (payment methods,
/// invoices); `return_url` defaults to `KOSHA_BILLING_RETURN_URL` and must
/// be on its origin. For the web console, like [`start_checkout`].
pub(crate) fn start_portal(
    billing: &Billing,
    tenant: &str,
    return_url: Option<&str>,
) -> Result<ProviderSession, SelfServiceError> {
    let (Some(configured), Some(provider)) = (&billing.return_url, &billing.provider) else {
        return Err(SelfServiceError::new(
            404,
            "self-service billing is off on this pod (KOSHA_BILLING_RETURN_URL and a \
             payment provider enable it)",
        ));
    };
    if !valid_name_component(tenant) || tenant == SERVICE_TENANT {
        return Err(SelfServiceError::new(400, "invalid tenant"));
    }
    let back = redirect_url(configured, return_url, "")?;
    let customer = match billing.store.account(tenant) {
        Ok(account) => account.and_then(|a| a.provider_customer_id),
        Err(e) => {
            eprintln!("WARN: billing: {e}");
            return Err(SelfServiceError::new(503, "billing unavailable"));
        }
    };
    let Some(customer) = customer else {
        return Err(SelfServiceError::new(
            409,
            "no billing customer for this tenant yet: start a checkout first",
        ));
    };
    provider
        .create_portal_session(&customer, &back)
        .map_err(|e| {
            eprintln!("WARN: billing: portal for {tenant} failed: {e}");
            SelfServiceError::new(503, "billing provider unavailable")
        })
}

fn session_response(result: Result<ProviderSession, SelfServiceError>) -> String {
    match result {
        Ok(session) => json_ok(&serde_json::json!({"id": session.id, "url": session.url})),
        Err(e) => e.response(),
    }
}

/// `POST /v1/billing/checkout` `{success_url?, cancel_url?}`.
fn handle_checkout(billing: &Billing, tenant: &str, body: &[u8]) -> String {
    let body = match json_body(body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    session_response(start_checkout(
        billing,
        tenant,
        body["success_url"].as_str(),
        body["cancel_url"].as_str(),
    ))
}

/// `POST /v1/billing/portal` `{return_url?}`.
fn handle_portal(billing: &Billing, tenant: &str, body: &[u8]) -> String {
    let body = match json_body(body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    session_response(start_portal(billing, tenant, body["return_url"].as_str()))
}

// ─── Stripe webhooks ────────────────────────────────────────────────────────

/// The answer to a Stripe webhook delivery, or `None` when the request is
/// not one. Called by `dispatch` before authentication: the signature is
/// the credential. 404 when billing or the endpoint is off; 400 for a bad
/// signature or body (Stripe retries, then gives up and reports it); 503
/// when the event could not be applied (Stripe delivers it again); 200
/// otherwise, a repeat delivery included.
pub(crate) fn webhook_response(
    state: &AppState,
    request_line: &str,
    headers: &std::collections::HashMap<String, String>,
    body: &[u8],
) -> Option<String> {
    let (method, target) = parse_method_and_path(request_line)?;
    let path = target.split('?').next().unwrap_or(target);
    if method != "POST" || path != STRIPE_WEBHOOK_PATH {
        return None;
    }
    let Some(billing) = state.billing.as_deref() else {
        return Some(json_error(404, "billing is off on this pod"));
    };
    let Some(config) = &billing.webhook else {
        return Some(json_error(
            404,
            "Stripe webhooks are off on this pod (KOSHA_STRIPE_WEBHOOK_SECRET enables them)",
        ));
    };
    Some(handle_stripe_webhook(
        billing,
        config,
        headers.get("stripe-signature").map(String::as_str),
        body,
    ))
}

fn handle_stripe_webhook(
    billing: &Billing,
    config: &WebhookConfig,
    signature: Option<&str>,
    body: &[u8],
) -> String {
    let stats = &billing.stats;
    let now_ms = billing.now_ms();
    if let Err(e) = stripe_webhook::verify(
        signature,
        body,
        &config.secrets,
        now_ms / 1_000,
        config.tolerance_secs,
    ) {
        stats.webhooks_bad_signature.fetch_add(1, Ordering::Relaxed);
        eprintln!("WARN: billing: Stripe webhook refused: {e}");
        return json_error(400, &e.to_string());
    }
    let event = match stripe_webhook::parse_event(body) {
        Ok(event) => event,
        Err(e) => {
            stats.webhooks_invalid.fetch_add(1, Ordering::Relaxed);
            eprintln!("WARN: billing: Stripe webhook refused: {e}");
            return json_error(400, &e);
        }
    };
    let catalog = billing.catalog();
    match handle_webhook_event(&billing.context(&catalog), &event, now_ms) {
        Ok(outcome) => {
            if outcome.duplicate {
                stats.webhooks_duplicate.fetch_add(1, Ordering::Relaxed);
            } else {
                stats.webhooks_processed.fetch_add(1, Ordering::Relaxed);
                println!(
                    "billing: Stripe event {} {}: {}{}",
                    outcome.event_id,
                    outcome.event_type,
                    outcome.outcome,
                    outcome
                        .tenant
                        .as_deref()
                        .map(|t| format!(" (tenant {t})"))
                        .unwrap_or_default()
                );
                // A payment, a void or a plan change can change a
                // tenant's standing: enforce it now on this pod.
                if outcome.outcome != "ignored" && outcome.outcome != "unchanged" {
                    billing.refresh_logged();
                }
            }
            json_ok(&serde_json::json!({
                "received": true,
                "duplicate": outcome.duplicate,
                "outcome": outcome.outcome,
            }))
        }
        Err(e) => {
            stats.webhooks_failed.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "WARN: billing: Stripe event {} not applied, Stripe will deliver it again: {e}",
                event.id
            );
            json_error(503, "event not applied; retry later")
        }
    }
}

// ─── Stripe ─────────────────────────────────────────────────────────────────

/// A [`BillingProvider`] that speaks Stripe's REST API: one customer per
/// tenant, and per invoice a draft invoice, one invoice item per charged
/// line, then finalization (`collection_method=send_invoice`, so Stripe
/// emails the invoice and collects through its hosted page).
///
/// Every `POST` carries an `Idempotency-Key` derived from the tenant or
/// our invoice id, so a retried call returns the first call's result.
/// Stripe keeps idempotency keys for about a day: a retry later than that
/// can create a second customer or invoice at Stripe.
pub(crate) struct StripeProvider {
    client: reqwest::blocking::Client,
    base: String,
    api_key: String,
    days_until_due: u64,
}

fn stripe_error(what: &str, detail: impl std::fmt::Display) -> KoshaError {
    KoshaError::ControlPlane(format!("stripe: {what}: {detail}"))
}

/// `application/x-www-form-urlencoded` of `fields`.
fn form_encode(fields: &[(&str, String)]) -> String {
    let encode = |text: &str| {
        let mut out = String::with_capacity(text.len());
        for byte in text.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(byte as char)
                }
                _ => out.push_str(&format!("%{byte:02X}")),
            }
        }
        out
    };
    fields
        .iter()
        .map(|(name, value)| format!("{}={}", encode(name), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

impl StripeProvider {
    pub(crate) fn new(base: &str, api_key: &str, days_until_due: u64) -> Result<Self, String> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| format!("stripe: cannot build HTTP client: {e}"))?;
        Ok(Self {
            client,
            base: base.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            days_until_due: days_until_due.max(1),
        })
    }

    fn finish(
        what: &str,
        response: reqwest::Result<reqwest::blocking::Response>,
    ) -> Result<serde_json::Value, KoshaError> {
        // `without_url`: the error text must not carry more than it needs.
        let response = response.map_err(|e| stripe_error(what, e.without_url()))?;
        let status = response.status();
        let body: serde_json::Value = response
            .json()
            .map_err(|e| stripe_error(what, format!("HTTP {status}: unreadable body: {e}")))?;
        if !status.is_success() {
            let message = body["error"]["message"].as_str().unwrap_or("no message");
            return Err(stripe_error(what, format!("HTTP {status}: {message}")));
        }
        Ok(body)
    }

    fn post(
        &self,
        what: &str,
        path: &str,
        fields: &[(&str, String)],
        idempotency_key: &str,
    ) -> Result<serde_json::Value, KoshaError> {
        let response = self
            .client
            .post(format!("{}{path}", self.base))
            .bearer_auth(&self.api_key)
            .header("Idempotency-Key", idempotency_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_encode(fields))
            .send();
        Self::finish(what, response)
    }

    fn get(&self, what: &str, path: &str) -> Result<serde_json::Value, KoshaError> {
        let response = self
            .client
            .get(format!("{}{path}", self.base))
            .bearer_auth(&self.api_key)
            .send();
        Self::finish(what, response)
    }

    fn status_of(
        what: &str,
        body: &serde_json::Value,
    ) -> Result<ProviderInvoiceStatus, KoshaError> {
        // draft, open and uncollectible are all still owed.
        Ok(match body["status"].as_str() {
            Some("paid") => ProviderInvoiceStatus::Paid,
            Some("void") => ProviderInvoiceStatus::Void,
            Some(_) => ProviderInvoiceStatus::Open,
            None => return Err(stripe_error(what, "response has no status")),
        })
    }

    fn session_of(what: &str, body: &serde_json::Value) -> Result<ProviderSession, KoshaError> {
        let url = body["url"]
            .as_str()
            .filter(|url| url.starts_with("https://") || url.starts_with("http://"))
            .ok_or_else(|| stripe_error(what, "response has no url"))?;
        Ok(ProviderSession {
            id: Self::id_of(what, body)?,
            url: url.to_string(),
        })
    }

    fn id_of(what: &str, body: &serde_json::Value) -> Result<String, KoshaError> {
        body["id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| stripe_error(what, "response has no id"))
    }

    /// A Stripe object id is used in a URL path; refuse anything that is
    /// not one.
    fn path_id<'a>(what: &str, id: &'a str) -> Result<&'a str, KoshaError> {
        if !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            Ok(id)
        } else {
            Err(stripe_error(what, "not a Stripe object id"))
        }
    }
}

impl BillingProvider for StripeProvider {
    fn name(&self) -> &str {
        "stripe"
    }

    fn create_customer(&self, tenant: &str) -> Result<String, KoshaError> {
        let what = "create customer";
        let body = self.post(
            what,
            "/v1/customers",
            &[
                ("name", tenant.to_string()),
                ("metadata[kosha_tenant]", tenant.to_string()),
            ],
            &format!("kosha-customer-{tenant}"),
        )?;
        Self::id_of(what, &body)
    }

    fn create_invoice(&self, customer: &str, invoice: &Invoice) -> Result<String, KoshaError> {
        let key = format!("kosha-invoice-{}", invoice.id);
        let what = "create invoice";
        // A customer with a default payment method (a completed checkout)
        // is charged automatically; anyone else is sent the invoice to pay
        // on Stripe's hosted page, due when ours is.
        let customer_id = Self::path_id(what, customer)?;
        let automatic = self
            .get("read customer", &format!("/v1/customers/{customer_id}"))?
            .get("invoice_settings")
            .and_then(|s| s.get("default_payment_method"))
            .is_some_and(|m| !m.is_null());
        let days = match invoice
            .due_at_ms
            .saturating_sub(invoice.issued_at_ms)
            .div_ceil(86_400_000)
        {
            0 => self.days_until_due,
            days => days,
        };
        let mut fields = vec![("customer", customer.to_string())];
        if automatic {
            fields.push(("collection_method", "charge_automatically".to_string()));
        } else {
            fields.push(("collection_method", "send_invoice".to_string()));
            fields.push(("days_until_due", days.to_string()));
        }
        let draft = self.post(
            what,
            "/v1/invoices",
            &[
                fields,
                vec![
                    ("auto_advance", "false".to_string()),
                    ("pending_invoice_items_behavior", "exclude".to_string()),
                    ("currency", "usd".to_string()),
                    (
                        "description",
                        format!("Kosha usage {} ({})", invoice.period, invoice.plan),
                    ),
                    ("metadata[kosha_invoice_id]", invoice.id.clone()),
                    ("metadata[kosha_tenant]", invoice.tenant.clone()),
                ],
            ]
            .concat(),
            &key,
        )?;
        let stripe_id = Self::id_of(what, &draft)?;
        let stripe_id = Self::path_id(what, &stripe_id)?;
        for line in invoice.lines.iter().filter(|line| line.amount_cents > 0) {
            self.post(
                "create invoice item",
                "/v1/invoiceitems",
                &[
                    ("customer", customer.to_string()),
                    ("invoice", stripe_id.to_string()),
                    ("amount", line.amount_cents.to_string()),
                    ("currency", "usd".to_string()),
                    (
                        "description",
                        format!(
                            "{}: {} billable of {} ({} included)",
                            line.meter, line.billable, line.quantity, line.included
                        ),
                    ),
                    ("metadata[kosha_meter]", line.meter.clone()),
                ],
                // A prorated invoice has one line per meter and plan.
                &match &line.plan {
                    None => format!("{key}-line-{}", line.meter),
                    Some(plan) => format!("{key}-line-{plan}-{}", line.meter),
                },
            )?;
        }
        self.post(
            "finalize invoice",
            &format!("/v1/invoices/{stripe_id}/finalize"),
            &[("auto_advance", "true".to_string())],
            &format!("{key}-finalize"),
        )?;
        Ok(stripe_id.to_string())
    }

    fn invoice_status(
        &self,
        provider_invoice_id: &str,
    ) -> Result<ProviderInvoiceStatus, KoshaError> {
        let what = "read invoice";
        let id = Self::path_id(what, provider_invoice_id)?;
        let body = self.get(what, &format!("/v1/invoices/{id}"))?;
        Self::status_of(what, &body)
    }

    fn retry_payment(
        &self,
        provider_invoice_id: &str,
        attempt: u32,
    ) -> Result<ProviderInvoiceStatus, KoshaError> {
        let what = "pay invoice";
        let id = Self::path_id(what, provider_invoice_id)?;
        // A declined card is an HTTP 402 with Stripe's reason: an `Err`.
        let body = self.post(
            what,
            &format!("/v1/invoices/{id}/pay"),
            &[],
            &format!("kosha-pay-{id}-{attempt}"),
        )?;
        Self::status_of(what, &body)
    }

    fn create_checkout_session(
        &self,
        customer: &str,
        request: &CheckoutRequest,
    ) -> Result<ProviderSession, KoshaError> {
        let what = "create checkout session";
        let body = self.post(
            what,
            "/v1/checkout/sessions",
            &[
                ("mode", "setup".to_string()),
                ("customer", customer.to_string()),
                ("payment_method_types[0]", "card".to_string()),
                ("success_url", request.success_url.clone()),
                ("cancel_url", request.cancel_url.clone()),
                ("client_reference_id", request.tenant.clone()),
                ("metadata[kosha_tenant]", request.tenant.clone()),
                ("metadata[kosha_plan]", request.plan.clone()),
                (
                    "setup_intent_data[metadata][kosha_tenant]",
                    request.tenant.clone(),
                ),
            ],
            // Each call is a new session.
            &format!("kosha-checkout-{}-{}", request.tenant, unique_suffix()),
        )?;
        Self::session_of(what, &body)
    }

    fn create_portal_session(
        &self,
        customer: &str,
        return_url: &str,
    ) -> Result<ProviderSession, KoshaError> {
        let what = "create portal session";
        let body = self.post(
            what,
            "/v1/billing_portal/sessions",
            &[
                ("customer", customer.to_string()),
                ("return_url", return_url.to_string()),
            ],
            &format!("kosha-portal-{customer}-{}", unique_suffix()),
        )?;
        Self::session_of(what, &body)
    }

    fn finish_checkout(
        &self,
        customer: &str,
        setup_intent: Option<&str>,
    ) -> Result<Option<String>, KoshaError> {
        let what = "finish checkout";
        let Some(setup_intent) = setup_intent else {
            return Ok(None);
        };
        let intent = Self::path_id(what, setup_intent)?;
        let body = self.get(what, &format!("/v1/setup_intents/{intent}"))?;
        let method = body["payment_method"]
            .as_str()
            .or_else(|| body["payment_method"]["id"].as_str())
            .map(str::to_string);
        let Some(method) = method else {
            return Ok(None);
        };
        let customer_id = Self::path_id(what, customer)?;
        self.post(
            what,
            &format!("/v1/customers/{customer_id}"),
            &[("invoice_settings[default_payment_method]", method.clone())],
            &format!("kosha-default-pm-{customer_id}-{method}"),
        )?;
        Ok(Some(method))
    }
}

/// A suffix that makes an idempotency key unique to one call.
fn unique_suffix() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "{:x}-{:x}-{:x}",
        unix_millis(),
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}
