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
//!
//! Nothing here schedules the monthly job: an operator (or a CronJob) calls
//! `close-period` and `sync`. Both are safe to repeat.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use kosha_control::billing::{
    rate, total_cents, AccountState, Biller, BillingAccount, BillingPeriod, BillingProvider,
    BillingStore, Block, Catalog, Invoice, InvoiceStatus, MemoryBillingStore, Operation,
    PlanQuotas, ProviderInvoiceStatus, TenantStanding,
};
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
    catalog: Catalog,
    store: Arc<dyn BillingStore>,
    usage: Arc<dyn UsageStore>,
    provider: Option<Arc<dyn BillingProvider>>,
    clock: Clock,
    standings: RwLock<Arc<Standings>>,
    refresh_interval: Duration,
    last_refresh_error: Mutex<Option<String>>,
    /// Whether a refresh has ever succeeded. Until one has, this pod does
    /// not know which tenants have accounts, so it must not hand out the
    /// default plan's quotas either (see `LimitsProvider for Billing`).
    refreshed: AtomicBool,
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
            catalog,
            store,
            usage,
            provider,
            clock,
            standings: RwLock::new(Arc::new(Standings::new())),
            refresh_interval: Duration::from_secs(60),
            last_refresh_error: Mutex::new(None),
            refreshed: AtomicBool::new(false),
        }
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
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("cannot read {path}: {e}"))?;
        let catalog = Catalog::parse(&text).map_err(|e| format!("{path}: {e}"))?;
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
        let mut billing = Self::new(
            catalog,
            store_from_env()?,
            usage,
            provider,
            Arc::new(unix_millis),
        );
        billing.refresh_interval = Duration::from_secs(
            env_nonempty("KOSHA_BILLING_REFRESH_SECS")
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|s| *s > 0)
                .unwrap_or(60),
        );
        println!(
            "billing: on ({} plan(s) from {path}, default plan {}, provider {}, refresh every {}s)",
            billing.catalog.plans.len(),
            billing.catalog.default_plan.as_deref().unwrap_or("none"),
            billing.provider.as_ref().map_or("none", |p| p.name()),
            billing.refresh_interval.as_secs()
        );
        if let Err(e) = billing.refresh() {
            eprintln!("WARN: billing: initial refresh failed; nobody is restricted until one succeeds: {e}");
        }
        Ok(Some(billing))
    }

    fn now_ms(&self) -> u64 {
        (self.clock)()
    }

    fn biller(&self) -> Biller<'_> {
        Biller {
            catalog: &self.catalog,
            store: self.store.as_ref(),
            usage: self.usage.as_ref(),
            provider: self.provider.as_deref(),
        }
    }

    /// Rebuild the standings snapshot. On error the previous snapshot
    /// stays in force.
    pub(crate) fn refresh(&self) -> Result<(), KoshaError> {
        match self.biller().standings(self.now_ms()) {
            Ok(standings) => {
                *self.standings.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(standings);
                *self.last_refresh_error.lock().unwrap() = None;
                self.refreshed.store(true, Ordering::Release);
                Ok(())
            }
            Err(e) => {
                *self.last_refresh_error.lock().unwrap() = Some(e.to_string());
                Err(e)
            }
        }
    }

    /// Refresh after a change made on this pod; a failure is logged and
    /// the next periodic refresh retries.
    fn refresh_logged(&self) {
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
            .or_else(|| self.catalog.default_plan.clone())
    }
}

fn store_from_env() -> Result<Arc<dyn BillingStore>, String> {
    #[cfg(feature = "postgres")]
    if let Some(url) = env_nonempty("DATABASE_URL") {
        let store = kosha_control::billing::PgBillingStore::new(&url)?;
        println!("billing: store is postgres (kosha.billing_accounts, kosha.billing_invoices)");
        return Ok(Arc::new(store));
    }
    println!(
        "billing: store is in-memory (no DATABASE_URL): accounts and invoices are per pod \
         and lost on restart"
    );
    Ok(Arc::new(MemoryBillingStore::new()))
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
        let quotas = &self.catalog.plan(&plan)?.limits;
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

pub(crate) const ROUTES: [(&str, &str); 7] = [
    ("GET", "/v1/billing"),
    ("GET", "/v1/admin/billing/accounts"),
    ("POST", "/v1/admin/billing/accounts"),
    ("GET", "/v1/admin/billing/invoices"),
    ("POST", "/v1/admin/billing/close-period"),
    ("POST", "/v1/admin/billing/sync"),
    ("POST", "/v1/admin/billing/invoices/mark-paid"),
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
        ("GET", "/v1/admin/billing/accounts") => handle_list_accounts(billing),
        ("POST", "/v1/admin/billing/accounts") => handle_put_account(billing, body),
        ("GET", "/v1/admin/billing/invoices") => handle_list_invoices(billing, request_line),
        ("POST", "/v1/admin/billing/close-period") => handle_close_period(billing, body, state),
        ("POST", "/v1/admin/billing/sync") => handle_sync(billing),
        _ => handle_mark_paid(billing, body),
    })
}

fn invoice_json(invoice: &Invoice) -> serde_json::Value {
    serde_json::to_value(invoice).unwrap_or(serde_json::Value::Null)
}

/// `GET /v1/billing`.
fn handle_tenant_billing(billing: &Billing, tenant: &str) -> String {
    let now_ms = billing.now_ms();
    let Some(plan_name) = billing.plan_name(tenant) else {
        return json_error(404, "no billing plan applies to this tenant");
    };
    let Some(plan) = billing.catalog.plan(&plan_name) else {
        return json_error(404, "no billing plan applies to this tenant");
    };
    let read = || -> Result<_, KoshaError> {
        Ok((
            billing.biller().month_to_date(tenant, now_ms)?,
            billing.store.invoices(Some(tenant))?,
        ))
    };
    let (totals, invoices) = match read() {
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
    let accounts: Vec<serde_json::Value> = accounts
        .iter()
        .map(|account| {
            serde_json::json!({
                "tenant": account.tenant,
                "plan": account.plan,
                "plan_known": billing.catalog.plan(&account.plan).is_some(),
                "provider_customer_id": account.provider_customer_id,
                "created_at_ms": account.created_at_ms,
                "standing": standings.get(&account.tenant),
            })
        })
        .collect();
    json_ok(&serde_json::json!({
        "accounts": accounts,
        "default_plan": billing.catalog.default_plan,
        "plans": billing.catalog.plans,
        "provider": billing.provider.as_ref().map(|p| p.name().to_string()),
        "last_refresh_error": *billing.last_refresh_error.lock().unwrap(),
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
    if billing.catalog.plan(plan).is_none() {
        return json_error(400, &format!("unknown plan {plan:?}"));
    }
    let put = || -> Result<Option<BillingAccount>, KoshaError> {
        let existing = billing.store.account(tenant)?;
        billing.store.put_account(&BillingAccount {
            tenant: tenant.to_string(),
            plan: plan.to_string(),
            provider_customer_id: existing.and_then(|a| a.provider_customer_id),
            created_at_ms: billing.now_ms(),
        })?;
        billing.store.account(tenant)
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
    match billing.biller().close_period(period, now_ms) {
        Ok(report) => {
            billing.refresh_logged();
            json_ok(&report)
        }
        Err(e) => unavailable(&e, true),
    }
}

/// `POST /v1/admin/billing/sync`.
fn handle_sync(billing: &Billing) -> String {
    match billing.biller().sync_payments(billing.now_ms()) {
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
        let draft = self.post(
            what,
            "/v1/invoices",
            &[
                ("customer", customer.to_string()),
                ("collection_method", "send_invoice".to_string()),
                ("days_until_due", self.days_until_due.to_string()),
                ("auto_advance", "false".to_string()),
                ("pending_invoice_items_behavior", "exclude".to_string()),
                ("currency", "usd".to_string()),
                (
                    "description",
                    format!("Kosha usage {} ({})", invoice.period, invoice.plan),
                ),
                ("metadata[kosha_invoice_id]", invoice.id.clone()),
            ],
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
                &format!("{key}-line-{}", line.meter),
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
        let response = self
            .client
            .get(format!("{}/v1/invoices/{id}", self.base))
            .bearer_auth(&self.api_key)
            .send();
        let body = Self::finish(what, response)?;
        // draft, open and uncollectible are all still owed.
        Ok(match body["status"].as_str() {
            Some("paid") => ProviderInvoiceStatus::Paid,
            Some("void") => ProviderInvoiceStatus::Void,
            Some(_) => ProviderInvoiceStatus::Open,
            None => return Err(stripe_error(what, "response has no status")),
        })
    }
}
