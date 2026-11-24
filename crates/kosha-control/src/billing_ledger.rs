//! What billing keeps besides accounts and invoices (#349): plan changes
//! (for proration), processed webhook events (idempotency), payment
//! failures and retries, dunning notices sent, each tenant's payment method
//! on file, finished scheduled jobs, and the scheduler's lease. Tables of
//! migration 011; an in-memory twin for tests and local runs.
//!
//! Also the two operations that write several of these at once:
//! [`change_plan`] (an operator's or a checkout's plan change) and
//! [`handle_webhook_event`] (one verified Stripe event).
//!
//! # Webhook idempotency
//!
//! A delivery first claims its event id (an insert that only one caller
//! wins); only the winner applies the event, then records what it did.
//! Another delivery of the same event, concurrent or later, finds the
//! claim and is acknowledged as a duplicate. When applying fails the claim
//! is released, so Stripe's redelivery applies it. A claim left behind by
//! a process that died while applying is taken over after
//! [`WEBHOOK_CLAIM_STALE_MS`]. Applying is idempotent as well (an invoice
//! only leaves `open`, a plan change to the current plan records nothing,
//! a payment method is set to a value), so a takeover of a claim whose
//! work did complete does no harm.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use kosha_core::KoshaError;
use serde::Serialize;

use crate::billing::{
    BillingAccount, BillingProvider, BillingStore, Catalog, InvoiceStatus, PlanChange, PlanHistory,
    PlanKind,
};
use crate::stripe_webhook::{InvoiceRef, StripeEvent, WebhookAction};

/// Failed collection attempts of one invoice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaymentFailure {
    pub invoice_id: String,
    pub tenant: String,
    /// Failures reported (by the provider's events or by Kosha's own
    /// retries).
    pub failures: u32,
    pub first_failed_at_ms: u64,
    pub last_failed_at_ms: u64,
    /// Kosha's own retries so far.
    pub retries: u32,
    pub last_retry_at_ms: Option<u64>,
    pub last_reason: Option<String>,
}

/// A dunning notice, once claimed for sending.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NoticeRecord {
    /// `<invoice id>/<kind>`: each notice of an invoice goes out once.
    pub id: String,
    pub tenant: String,
    pub invoice_id: Option<String>,
    pub kind: String,
    pub sent_at_ms: u64,
}

/// A tenant's default payment method at the provider, as last reported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PaymentMethodRecord {
    pub tenant: String,
    /// `None`: the tenant has none on file any more.
    pub payment_method: Option<String>,
    pub updated_at_ms: u64,
}

/// Billing's bookkeeping beyond accounts and invoices. See the module doc.
pub trait BillingLedger: PlanHistory {
    fn record_plan_change(&self, change: &PlanChange) -> Result<(), KoshaError>;

    /// Claim a webhook event for processing at `now_ms`. `true`: this
    /// caller applies it. `false`: it was processed, or another delivery
    /// holds a claim on it younger than `stale_ms`.
    fn claim_webhook_event(
        &self,
        event_id: &str,
        event_type: &str,
        now_ms: u64,
        stale_ms: u64,
    ) -> Result<bool, KoshaError>;

    /// Record what a claimed event did.
    fn finish_webhook_event(
        &self,
        event_id: &str,
        outcome: &str,
        at_ms: u64,
    ) -> Result<(), KoshaError>;

    /// Give back the claim of an event that could not be applied, so a
    /// redelivery applies it. A finished event is left alone.
    fn release_webhook_event(&self, event_id: &str) -> Result<(), KoshaError>;

    /// Count a failed collection of `invoice_id`.
    fn record_payment_failure(
        &self,
        invoice_id: &str,
        tenant: &str,
        at_ms: u64,
        reason: Option<&str>,
    ) -> Result<(), KoshaError>;

    /// Claim Kosha's retry number `attempt` (from 1) of `invoice_id`:
    /// `true` for the one caller that moves the count from `attempt - 1`
    /// to `attempt`, so two dunning passes never retry one invoice twice.
    /// `false` for an invoice with no failure recorded.
    fn claim_payment_retry(
        &self,
        invoice_id: &str,
        attempt: u32,
        at_ms: u64,
    ) -> Result<bool, KoshaError>;

    /// The failure records of invoices that are still open.
    fn payment_failures(&self) -> Result<Vec<PaymentFailure>, KoshaError>;

    /// Claim a notice before sending it. `false` (and nothing changes)
    /// when it was claimed before: at most one pod sends it, once.
    fn claim_notice(&self, notice: &NoticeRecord) -> Result<bool, KoshaError>;

    /// Give a claimed notice back after it could not be sent, so the next
    /// run tries again.
    fn release_notice(&self, notice_id: &str) -> Result<(), KoshaError>;

    /// Notices claimed at or after `since_ms`.
    fn notices(&self, since_ms: u64) -> Result<Vec<NoticeRecord>, KoshaError>;

    fn set_payment_method(
        &self,
        tenant: &str,
        payment_method: Option<&str>,
        at_ms: u64,
    ) -> Result<(), KoshaError>;

    fn payment_method(&self, tenant: &str) -> Result<Option<PaymentMethodRecord>, KoshaError>;

    /// Whether a scheduled job (`close-period/2026-10`) has completed.
    fn job_done(&self, job: &str) -> Result<bool, KoshaError>;

    fn mark_job_done(&self, job: &str, at_ms: u64, detail: &str) -> Result<(), KoshaError>;

    /// Take or renew the lease `name` for `holder` until `now_ms + ttl_ms`.
    /// `true` when `holder` has it: it was free, expired, or already
    /// `holder`'s. Pods' clocks must agree to well within the TTL.
    fn try_lease(
        &self,
        name: &str,
        holder: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<bool, KoshaError>;
}

// ─── In memory ──────────────────────────────────────────────────────────────

#[derive(Default)]
struct MemoryLedgerInner {
    plan_changes: Vec<PlanChange>,
    webhook_events: BTreeMap<String, (String, String, u64)>,
    failures: BTreeMap<String, PaymentFailure>,
    notices: BTreeMap<String, NoticeRecord>,
    payment_methods: BTreeMap<String, PaymentMethodRecord>,
    jobs: BTreeMap<String, (u64, String)>,
    leases: BTreeMap<String, (String, u64)>,
}

/// In-memory [`BillingLedger`]: tests and local runs. Not shared between
/// pods.
#[derive(Default)]
pub struct MemoryBillingLedger {
    inner: Mutex<MemoryLedgerInner>,
}

impl MemoryBillingLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Processed webhook events: `(id, type, outcome)`.
    pub fn webhook_events(&self) -> Vec<(String, String, String)> {
        self.inner
            .lock()
            .unwrap()
            .webhook_events
            .iter()
            .map(|(id, (kind, outcome, _))| (id.clone(), kind.clone(), outcome.clone()))
            .collect()
    }
}

impl PlanHistory for MemoryBillingLedger {
    fn plan_changes(
        &self,
        tenant: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<PlanChange>, KoshaError> {
        let mut changes: Vec<PlanChange> = self
            .inner
            .lock()
            .unwrap()
            .plan_changes
            .iter()
            .filter(|c| c.tenant == tenant && c.at_ms >= from_ms && c.at_ms < to_ms)
            .cloned()
            .collect();
        changes.sort_by_key(|c| c.at_ms);
        Ok(changes)
    }
}

impl BillingLedger for MemoryBillingLedger {
    fn record_plan_change(&self, change: &PlanChange) -> Result<(), KoshaError> {
        self.inner.lock().unwrap().plan_changes.push(change.clone());
        Ok(())
    }

    fn claim_webhook_event(
        &self,
        event_id: &str,
        event_type: &str,
        now_ms: u64,
        stale_ms: u64,
    ) -> Result<bool, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        if let Some((_, outcome, at)) = inner.webhook_events.get(event_id) {
            let stale = outcome == PROCESSING && at.saturating_add(stale_ms) < now_ms;
            if !stale {
                return Ok(false);
            }
        }
        inner.webhook_events.insert(
            event_id.to_string(),
            (event_type.to_string(), PROCESSING.to_string(), now_ms),
        );
        Ok(true)
    }

    fn finish_webhook_event(
        &self,
        event_id: &str,
        outcome: &str,
        at_ms: u64,
    ) -> Result<(), KoshaError> {
        if let Some(entry) = self.inner.lock().unwrap().webhook_events.get_mut(event_id) {
            entry.1 = outcome.to_string();
            entry.2 = at_ms;
        }
        Ok(())
    }

    fn release_webhook_event(&self, event_id: &str) -> Result<(), KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        if inner
            .webhook_events
            .get(event_id)
            .is_some_and(|(_, outcome, _)| outcome == PROCESSING)
        {
            inner.webhook_events.remove(event_id);
        }
        Ok(())
    }

    fn record_payment_failure(
        &self,
        invoice_id: &str,
        tenant: &str,
        at_ms: u64,
        reason: Option<&str>,
    ) -> Result<(), KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        let record = inner
            .failures
            .entry(invoice_id.to_string())
            .or_insert_with(|| PaymentFailure {
                invoice_id: invoice_id.to_string(),
                tenant: tenant.to_string(),
                failures: 0,
                first_failed_at_ms: at_ms,
                last_failed_at_ms: at_ms,
                retries: 0,
                last_retry_at_ms: None,
                last_reason: None,
            });
        record.failures = record.failures.saturating_add(1);
        record.last_failed_at_ms = record.last_failed_at_ms.max(at_ms);
        if reason.is_some() {
            record.last_reason = reason.map(str::to_string);
        }
        Ok(())
    }

    fn claim_payment_retry(
        &self,
        invoice_id: &str,
        attempt: u32,
        at_ms: u64,
    ) -> Result<bool, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        match inner.failures.get_mut(invoice_id) {
            Some(record) if attempt >= 1 && record.retries == attempt - 1 => {
                record.retries = attempt;
                record.last_retry_at_ms = Some(at_ms);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn payment_failures(&self) -> Result<Vec<PaymentFailure>, KoshaError> {
        // The memory ledger does not see invoices; the caller keeps the
        // open ones.
        Ok(self
            .inner
            .lock()
            .unwrap()
            .failures
            .values()
            .cloned()
            .collect())
    }

    fn claim_notice(&self, notice: &NoticeRecord) -> Result<bool, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.notices.contains_key(&notice.id) {
            return Ok(false);
        }
        inner.notices.insert(notice.id.clone(), notice.clone());
        Ok(true)
    }

    fn release_notice(&self, notice_id: &str) -> Result<(), KoshaError> {
        self.inner.lock().unwrap().notices.remove(notice_id);
        Ok(())
    }

    fn notices(&self, since_ms: u64) -> Result<Vec<NoticeRecord>, KoshaError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .notices
            .values()
            .filter(|n| n.sent_at_ms >= since_ms)
            .cloned()
            .collect())
    }

    fn set_payment_method(
        &self,
        tenant: &str,
        payment_method: Option<&str>,
        at_ms: u64,
    ) -> Result<(), KoshaError> {
        self.inner.lock().unwrap().payment_methods.insert(
            tenant.to_string(),
            PaymentMethodRecord {
                tenant: tenant.to_string(),
                payment_method: payment_method.map(str::to_string),
                updated_at_ms: at_ms,
            },
        );
        Ok(())
    }

    fn payment_method(&self, tenant: &str) -> Result<Option<PaymentMethodRecord>, KoshaError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .payment_methods
            .get(tenant)
            .cloned())
    }

    fn job_done(&self, job: &str) -> Result<bool, KoshaError> {
        Ok(self.inner.lock().unwrap().jobs.contains_key(job))
    }

    fn mark_job_done(&self, job: &str, at_ms: u64, detail: &str) -> Result<(), KoshaError> {
        self.inner
            .lock()
            .unwrap()
            .jobs
            .entry(job.to_string())
            .or_insert((at_ms, detail.to_string()));
        Ok(())
    }

    fn try_lease(
        &self,
        name: &str,
        holder: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<bool, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        let expires = now_ms.saturating_add(ttl_ms);
        match inner.leases.get_mut(name) {
            Some((current, until)) if current != holder && *until > now_ms => Ok(false),
            Some(lease) => {
                *lease = (holder.to_string(), expires);
                Ok(true)
            }
            None => {
                inner
                    .leases
                    .insert(name.to_string(), (holder.to_string(), expires));
                Ok(true)
            }
        }
    }
}

// ─── Operations ─────────────────────────────────────────────────────────────

/// How long a webhook claim may stay unfinished before another delivery
/// takes it over (its processor is presumed dead).
pub const WEBHOOK_CLAIM_STALE_MS: u64 = 5 * 60_000;

/// The `outcome` of a claimed, not yet finished webhook event.
const PROCESSING: &str = "processing";

/// Put `tenant` on `plan`, keeping (or setting) its provider customer,
/// and record the change for proration when the plan is different. The
/// caller has checked that the plan is in the catalog.
///
/// The account and the plan history are separate writes (different
/// tables, possibly different stores), so they cannot share a
/// transaction. The change is recorded first and the account written
/// second, and the call is safe to retry:
///
/// - The record fails: nothing changed; the caller (an operator, or Stripe
///   redelivering the checkout event) retries from scratch.
/// - The account write fails after the record: the retry finds the
///   history's latest change already going from the account's plan to
///   `plan` and only writes the account; the change keeps the time it was
///   asked for.
/// - An account already on `plan` whose history ends elsewhere (written
///   by an older build that recorded after the account) gets the missing
///   change recorded now.
pub fn change_plan(
    store: &dyn BillingStore,
    ledger: &dyn BillingLedger,
    tenant: &str,
    plan: &str,
    provider_customer_id: Option<&str>,
    source: &str,
    now_ms: u64,
) -> Result<Option<BillingAccount>, KoshaError> {
    let existing = store.account(tenant)?;
    let before = existing.as_ref().map(|a| a.plan.clone());
    let latest = ledger.plan_changes(tenant, 0, u64::MAX)?.pop();
    let from_plan = if before.as_deref() != Some(plan) {
        // A change recorded by an earlier attempt whose account write
        // failed is not recorded twice.
        let pending = latest
            .as_ref()
            .is_some_and(|c| c.to_plan == plan && c.from_plan == before);
        (!pending).then_some(before)
    } else {
        match latest {
            Some(change) if change.to_plan != plan => Some(Some(change.to_plan)),
            _ => None,
        }
    };
    if let Some(from_plan) = from_plan {
        ledger.record_plan_change(&PlanChange {
            tenant: tenant.to_string(),
            at_ms: now_ms,
            from_plan,
            to_plan: plan.to_string(),
            source: source.to_string(),
        })?;
    }
    store.put_account(&BillingAccount {
        tenant: tenant.to_string(),
        plan: plan.to_string(),
        provider_customer_id: provider_customer_id.map(str::to_string).or_else(|| {
            existing
                .as_ref()
                .and_then(|a| a.provider_customer_id.clone())
        }),
        created_at_ms: now_ms,
    })?;
    store.account(tenant)
}

/// What billing has to work with when it applies an event.
pub struct BillingContext<'a> {
    pub catalog: &'a Catalog,
    pub store: &'a dyn BillingStore,
    pub ledger: &'a dyn BillingLedger,
    pub provider: Option<&'a dyn BillingProvider>,
}

/// The result of one webhook delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WebhookOutcome {
    pub event_id: String,
    pub event_type: String,
    /// What was done (`paid`, `payment_failed`, `plan_changed`, `ignored`,
    /// ...), or `duplicate` for an event processed before.
    pub outcome: String,
    /// The tenant the event concerned, when one was found.
    pub tenant: Option<String>,
    pub duplicate: bool,
}

/// Apply one verified event, unless it was processed before. `Err` when a
/// store or the provider failed: nothing is recorded, and the caller
/// answers with an error so that Stripe delivers the event again.
pub fn handle_webhook_event(
    ctx: &BillingContext<'_>,
    event: &StripeEvent,
    now_ms: u64,
) -> Result<WebhookOutcome, KoshaError> {
    let mut outcome = WebhookOutcome {
        event_id: event.id.clone(),
        event_type: event.event_type.clone(),
        outcome: "duplicate".into(),
        tenant: None,
        duplicate: true,
    };
    if !ctx.ledger.claim_webhook_event(
        &event.id,
        &event.event_type,
        now_ms,
        WEBHOOK_CLAIM_STALE_MS,
    )? {
        return Ok(outcome);
    }
    let (done, tenant) = match apply_webhook(ctx, &crate::stripe_webhook::action_of(event), now_ms)
    {
        Ok(applied) => applied,
        Err(e) => {
            // A redelivery must apply it. If the release fails too, the
            // claim goes stale and is taken over.
            let _ = ctx.ledger.release_webhook_event(&event.id);
            return Err(e);
        }
    };
    ctx.ledger.finish_webhook_event(&event.id, &done, now_ms)?;
    outcome.outcome = done;
    outcome.tenant = tenant;
    outcome.duplicate = false;
    Ok(outcome)
}

/// Find our invoice for a provider invoice: by the id Kosha put in its
/// metadata, checked against the provider id when ours has one; else by
/// the provider id alone.
fn find_invoice(
    store: &dyn BillingStore,
    reference: &InvoiceRef,
) -> Result<Option<crate::billing::Invoice>, KoshaError> {
    if let Some(id) = &reference.kosha_invoice_id {
        if let Some(invoice) = store.invoice(id)? {
            let matches = invoice
                .provider_invoice_id
                .as_deref()
                .is_none_or(|pid| pid == reference.provider_invoice_id);
            return Ok(matches.then_some(invoice));
        }
    }
    Ok(store.invoices(None)?.into_iter().find(|invoice| {
        invoice.provider_invoice_id.as_deref() == Some(reference.provider_invoice_id.as_str())
    }))
}

/// The tenant whose account has `customer` as its provider customer.
fn tenant_of_customer(
    store: &dyn BillingStore,
    customer: &str,
) -> Result<Option<String>, KoshaError> {
    Ok(store
        .accounts()?
        .into_iter()
        .find(|a| a.provider_customer_id.as_deref() == Some(customer))
        .map(|a| a.tenant))
}

/// Apply `action`. Returns what was done and the tenant concerned.
pub fn apply_webhook(
    ctx: &BillingContext<'_>,
    action: &WebhookAction,
    now_ms: u64,
) -> Result<(String, Option<String>), KoshaError> {
    let done = |what: &str, tenant: Option<String>| Ok((what.to_string(), tenant));
    match action {
        WebhookAction::InvoicePaid {
            invoice,
            paid_at_ms,
        } => {
            let Some(ours) = find_invoice(ctx.store, invoice)? else {
                return done("unknown_invoice", None);
            };
            if ours.provider_invoice_id.is_none() {
                ctx.store
                    .set_provider_invoice(&ours.id, &invoice.provider_invoice_id)?;
            }
            if ours.status != InvoiceStatus::Open {
                return done("already_settled", Some(ours.tenant));
            }
            // A paid time from the future (clock skew) is clamped to now.
            let paid_at = if *paid_at_ms == 0 {
                now_ms
            } else {
                (*paid_at_ms).min(now_ms)
            };
            ctx.store
                .set_invoice_status(&ours.id, InvoiceStatus::Paid, Some(paid_at))?;
            done("paid", Some(ours.tenant))
        }
        WebhookAction::InvoicePaymentFailed {
            invoice, reason, ..
        } => {
            let Some(ours) = find_invoice(ctx.store, invoice)? else {
                return done("unknown_invoice", None);
            };
            if ours.status != InvoiceStatus::Open {
                return done("already_settled", Some(ours.tenant));
            }
            ctx.ledger
                .record_payment_failure(&ours.id, &ours.tenant, now_ms, reason.as_deref())?;
            done("payment_failed", Some(ours.tenant))
        }
        WebhookAction::InvoiceFinalized { invoice } => {
            let Some(ours) = find_invoice(ctx.store, invoice)? else {
                return done("unknown_invoice", None);
            };
            if ours.provider_invoice_id.is_none() {
                ctx.store
                    .set_provider_invoice(&ours.id, &invoice.provider_invoice_id)?;
            }
            done("finalized", Some(ours.tenant))
        }
        WebhookAction::InvoiceVoided { invoice } => {
            let Some(ours) = find_invoice(ctx.store, invoice)? else {
                return done("unknown_invoice", None);
            };
            if ours.status != InvoiceStatus::Open {
                return done("already_settled", Some(ours.tenant));
            }
            ctx.store
                .set_invoice_status(&ours.id, InvoiceStatus::Void, None)?;
            done("voided", Some(ours.tenant))
        }
        WebhookAction::CheckoutCompleted {
            tenant,
            plan,
            customer,
            setup_intent,
            ..
        } => {
            let tenant = Some(tenant.clone());
            let name = tenant.as_deref().unwrap_or_default();
            // Only the catalog's upgrade plan is sold by checkout; a
            // session started before the catalog changed is refused.
            if ctx.catalog.upgrade_plan.as_deref() != Some(plan.as_str())
                || ctx.catalog.plan(plan).is_none()
            {
                return done("plan_not_offered", tenant);
            }
            let account = ctx.store.account(name)?;
            if let Some(existing) = account
                .as_ref()
                .and_then(|a| a.provider_customer_id.as_deref())
            {
                if existing != customer {
                    return done("customer_mismatch", tenant);
                }
            }
            let method = match ctx.provider {
                Some(provider) => provider.finish_checkout(customer, setup_intent.as_deref())?,
                None => None,
            };
            if method.is_some() {
                ctx.ledger
                    .set_payment_method(name, method.as_deref(), now_ms)?;
            }
            // An enterprise account keeps its plan: checkout only adds a
            // payment method to it.
            let keep = account.as_ref().is_some_and(|a| {
                ctx.catalog
                    .plan(&a.plan)
                    .is_some_and(|p| p.kind == PlanKind::Enterprise)
            });
            let target = match (&account, keep) {
                (Some(a), true) => a.plan.clone(),
                _ => plan.clone(),
            };
            change_plan(
                ctx.store,
                ctx.ledger,
                name,
                &target,
                Some(customer),
                "checkout",
                now_ms,
            )?;
            done(
                if keep {
                    "payment_method_added"
                } else {
                    "plan_changed"
                },
                tenant,
            )
        }
        WebhookAction::CustomerPaymentMethod {
            customer,
            default_payment_method,
            changed,
        } => {
            let Some(tenant) = tenant_of_customer(ctx.store, customer)? else {
                return done("unknown_customer", None);
            };
            if !changed {
                return done("unchanged", Some(tenant));
            }
            ctx.ledger
                .set_payment_method(&tenant, default_payment_method.as_deref(), now_ms)?;
            done("payment_method_updated", Some(tenant))
        }
        WebhookAction::PaymentMethodDetached {
            customer,
            payment_method,
        } => {
            let Some(tenant) = tenant_of_customer(ctx.store, customer)? else {
                return done("unknown_customer", None);
            };
            let current = ctx.ledger.payment_method(&tenant)?;
            if current.and_then(|r| r.payment_method).as_deref() == Some(payment_method.as_str()) {
                ctx.ledger.set_payment_method(&tenant, None, now_ms)?;
                return done("payment_method_removed", Some(tenant));
            }
            done("unchanged", Some(tenant))
        }
        WebhookAction::Ignored { .. } => done("ignored", None),
    }
}

/// The ids of `notices` grouped by invoice.
pub(crate) fn notices_by_invoice(notices: &[NoticeRecord]) -> BTreeMap<String, BTreeSet<String>> {
    let mut by_invoice: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for notice in notices {
        if let Some(invoice) = &notice.invoice_id {
            by_invoice
                .entry(invoice.clone())
                .or_default()
                .insert(notice.kind.clone());
        }
    }
    by_invoice
}

// ─── Postgres ───────────────────────────────────────────────────────────────

#[cfg(feature = "postgres")]
pub use pg::PgBillingLedger;

#[cfg(feature = "postgres")]
mod pg {
    use super::{BillingLedger, NoticeRecord, PaymentFailure, PaymentMethodRecord};
    use crate::billing::{PlanChange, PlanHistory};
    use kosha_core::KoshaError;
    use std::time::Duration;

    fn control_plane_error(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    fn to_i64(value: u64) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }

    fn to_u64(value: i64) -> u64 {
        u64::try_from(value).unwrap_or(0)
    }

    fn to_u32(value: i32) -> u32 {
        u32::try_from(value).unwrap_or(0)
    }

    /// The tables of migration 011 behind their own small pool, like the
    /// billing store.
    pub struct PgBillingLedger {
        pool: sqlx::PgPool,
        rt: tokio::runtime::Runtime,
    }

    impl PgBillingLedger {
        /// A ledger that connects on first use. Assumes the tables exist:
        /// `PgStore::new` applies migration 011 at startup.
        pub fn new(database_url: &str) -> Result<Self, String> {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|e| format!("failed to create tokio runtime: {e}"))?;
            let pool = {
                let _guard = rt.enter();
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(2)
                    .acquire_timeout(Duration::from_secs(5))
                    .connect_lazy(database_url)
                    .map_err(|e| format!("invalid postgres url: {e}"))?
            };
            Ok(Self { pool, rt })
        }

        fn execute(
            &self,
            query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
        ) -> Result<u64, KoshaError> {
            self.rt.block_on(async {
                Ok(query
                    .execute(&self.pool)
                    .await
                    .map_err(control_plane_error)?
                    .rows_affected())
            })
        }
    }

    type ChangeRow = (String, i64, Option<String>, String, String);
    type FailureRow = (
        String,
        String,
        i32,
        i64,
        i64,
        i32,
        Option<i64>,
        Option<String>,
    );
    type NoticeRow = (String, String, Option<String>, String, i64);

    impl PlanHistory for PgBillingLedger {
        fn plan_changes(
            &self,
            tenant: &str,
            from_ms: u64,
            to_ms: u64,
        ) -> Result<Vec<PlanChange>, KoshaError> {
            self.rt.block_on(async {
                let rows: Vec<ChangeRow> = sqlx::query_as(
                    "SELECT tenant_id, changed_at_ms, from_plan, to_plan, source \
                     FROM kosha.billing_plan_changes \
                     WHERE tenant_id = $1 AND changed_at_ms >= $2 AND changed_at_ms < $3 \
                     ORDER BY changed_at_ms, id",
                )
                .bind(tenant)
                .bind(to_i64(from_ms))
                .bind(to_i64(to_ms))
                .fetch_all(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(rows
                    .into_iter()
                    .map(|(tenant, at, from_plan, to_plan, source)| PlanChange {
                        tenant,
                        at_ms: to_u64(at),
                        from_plan,
                        to_plan,
                        source,
                    })
                    .collect())
            })
        }
    }

    impl BillingLedger for PgBillingLedger {
        fn record_plan_change(&self, change: &PlanChange) -> Result<(), KoshaError> {
            self.execute(
                sqlx::query(
                    "INSERT INTO kosha.billing_plan_changes \
                     (tenant_id, changed_at_ms, from_plan, to_plan, source) \
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(&change.tenant)
                .bind(to_i64(change.at_ms))
                .bind(&change.from_plan)
                .bind(&change.to_plan)
                .bind(&change.source),
            )?;
            Ok(())
        }

        fn claim_webhook_event(
            &self,
            event_id: &str,
            event_type: &str,
            now_ms: u64,
            stale_ms: u64,
        ) -> Result<bool, KoshaError> {
            // One statement: insert the claim, or take over a claim whose
            // processor died; a finished or live claim is left alone and
            // no row is affected.
            Ok(self.execute(
                sqlx::query(
                    "INSERT INTO kosha.billing_webhook_events \
                     (event_id, event_type, outcome, processed_at_ms) \
                     VALUES ($1, $2, 'processing', $3) \
                     ON CONFLICT (event_id) DO UPDATE SET \
                         processed_at_ms = EXCLUDED.processed_at_ms \
                     WHERE kosha.billing_webhook_events.outcome = 'processing' \
                       AND kosha.billing_webhook_events.processed_at_ms < $4",
                )
                .bind(event_id)
                .bind(event_type)
                .bind(to_i64(now_ms))
                .bind(to_i64(now_ms.saturating_sub(stale_ms))),
            )? == 1)
        }

        fn finish_webhook_event(
            &self,
            event_id: &str,
            outcome: &str,
            at_ms: u64,
        ) -> Result<(), KoshaError> {
            self.execute(
                sqlx::query(
                    "UPDATE kosha.billing_webhook_events \
                     SET outcome = $2, processed_at_ms = $3 WHERE event_id = $1",
                )
                .bind(event_id)
                .bind(outcome)
                .bind(to_i64(at_ms)),
            )?;
            Ok(())
        }

        fn release_webhook_event(&self, event_id: &str) -> Result<(), KoshaError> {
            self.execute(
                sqlx::query(
                    "DELETE FROM kosha.billing_webhook_events \
                     WHERE event_id = $1 AND outcome = 'processing'",
                )
                .bind(event_id),
            )?;
            Ok(())
        }

        fn record_payment_failure(
            &self,
            invoice_id: &str,
            tenant: &str,
            at_ms: u64,
            reason: Option<&str>,
        ) -> Result<(), KoshaError> {
            self.execute(
                sqlx::query(
                    "INSERT INTO kosha.billing_payment_failures \
                     (invoice_id, tenant_id, failures, first_failed_at_ms, last_failed_at_ms, \
                      retries, last_reason) VALUES ($1, $2, 1, $3, $3, 0, $4) \
                     ON CONFLICT (invoice_id) DO UPDATE SET \
                         failures = kosha.billing_payment_failures.failures + 1, \
                         last_failed_at_ms = GREATEST( \
                             kosha.billing_payment_failures.last_failed_at_ms, \
                             EXCLUDED.last_failed_at_ms), \
                         last_reason = COALESCE(EXCLUDED.last_reason, \
                             kosha.billing_payment_failures.last_reason)",
                )
                .bind(invoice_id)
                .bind(tenant)
                .bind(to_i64(at_ms))
                .bind(reason),
            )?;
            Ok(())
        }

        fn claim_payment_retry(
            &self,
            invoice_id: &str,
            attempt: u32,
            at_ms: u64,
        ) -> Result<bool, KoshaError> {
            if attempt == 0 {
                return Ok(false);
            }
            // Compare-and-set on the retry count: one winner per attempt.
            Ok(self.execute(
                sqlx::query(
                    "UPDATE kosha.billing_payment_failures \
                     SET retries = $2, last_retry_at_ms = $3 \
                     WHERE invoice_id = $1 AND retries = $4",
                )
                .bind(invoice_id)
                .bind(i32::try_from(attempt).unwrap_or(i32::MAX))
                .bind(to_i64(at_ms))
                .bind(i32::try_from(attempt - 1).unwrap_or(i32::MAX)),
            )? == 1)
        }

        fn payment_failures(&self) -> Result<Vec<PaymentFailure>, KoshaError> {
            self.rt.block_on(async {
                let rows: Vec<FailureRow> = sqlx::query_as(
                    "SELECT f.invoice_id, f.tenant_id, f.failures, f.first_failed_at_ms, \
                            f.last_failed_at_ms, f.retries, f.last_retry_at_ms, f.last_reason \
                     FROM kosha.billing_payment_failures f \
                     JOIN kosha.billing_invoices i ON i.invoice_id = f.invoice_id \
                     WHERE i.status = 'open' ORDER BY f.invoice_id",
                )
                .fetch_all(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(rows
                    .into_iter()
                    .map(
                        |(invoice_id, tenant, failures, first, last, retries, retry_at, reason)| {
                            PaymentFailure {
                                invoice_id,
                                tenant,
                                failures: to_u32(failures),
                                first_failed_at_ms: to_u64(first),
                                last_failed_at_ms: to_u64(last),
                                retries: to_u32(retries),
                                last_retry_at_ms: retry_at.map(to_u64),
                                last_reason: reason,
                            }
                        },
                    )
                    .collect())
            })
        }

        fn claim_notice(&self, notice: &NoticeRecord) -> Result<bool, KoshaError> {
            Ok(self.execute(
                sqlx::query(
                    "INSERT INTO kosha.billing_notices \
                     (notice_id, tenant_id, invoice_id, kind, sent_at_ms) \
                     VALUES ($1, $2, $3, $4, $5) ON CONFLICT (notice_id) DO NOTHING",
                )
                .bind(&notice.id)
                .bind(&notice.tenant)
                .bind(&notice.invoice_id)
                .bind(&notice.kind)
                .bind(to_i64(notice.sent_at_ms)),
            )? == 1)
        }

        fn release_notice(&self, notice_id: &str) -> Result<(), KoshaError> {
            self.execute(
                sqlx::query("DELETE FROM kosha.billing_notices WHERE notice_id = $1")
                    .bind(notice_id),
            )?;
            Ok(())
        }

        fn notices(&self, since_ms: u64) -> Result<Vec<NoticeRecord>, KoshaError> {
            self.rt.block_on(async {
                let rows: Vec<NoticeRow> = sqlx::query_as(
                    "SELECT notice_id, tenant_id, invoice_id, kind, sent_at_ms \
                     FROM kosha.billing_notices WHERE sent_at_ms >= $1 ORDER BY notice_id",
                )
                .bind(to_i64(since_ms))
                .fetch_all(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(rows
                    .into_iter()
                    .map(|(id, tenant, invoice_id, kind, at)| NoticeRecord {
                        id,
                        tenant,
                        invoice_id,
                        kind,
                        sent_at_ms: to_u64(at),
                    })
                    .collect())
            })
        }

        fn set_payment_method(
            &self,
            tenant: &str,
            payment_method: Option<&str>,
            at_ms: u64,
        ) -> Result<(), KoshaError> {
            self.execute(
                sqlx::query(
                    "INSERT INTO kosha.billing_payment_methods \
                     (tenant_id, payment_method, updated_at_ms) VALUES ($1, $2, $3) \
                     ON CONFLICT (tenant_id) DO UPDATE SET \
                         payment_method = EXCLUDED.payment_method, \
                         updated_at_ms = EXCLUDED.updated_at_ms",
                )
                .bind(tenant)
                .bind(payment_method)
                .bind(to_i64(at_ms)),
            )?;
            Ok(())
        }

        fn payment_method(&self, tenant: &str) -> Result<Option<PaymentMethodRecord>, KoshaError> {
            self.rt.block_on(async {
                let row: Option<(String, Option<String>, i64)> = sqlx::query_as(
                    "SELECT tenant_id, payment_method, updated_at_ms \
                     FROM kosha.billing_payment_methods WHERE tenant_id = $1",
                )
                .bind(tenant)
                .fetch_optional(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(row.map(|(tenant, payment_method, at)| PaymentMethodRecord {
                    tenant,
                    payment_method,
                    updated_at_ms: to_u64(at),
                }))
            })
        }

        fn job_done(&self, job: &str) -> Result<bool, KoshaError> {
            self.rt.block_on(async {
                let row: Option<(String,)> =
                    sqlx::query_as("SELECT job FROM kosha.billing_jobs WHERE job = $1")
                        .bind(job)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(control_plane_error)?;
                Ok(row.is_some())
            })
        }

        fn mark_job_done(&self, job: &str, at_ms: u64, detail: &str) -> Result<(), KoshaError> {
            self.execute(
                sqlx::query(
                    "INSERT INTO kosha.billing_jobs (job, done_at_ms, detail) \
                     VALUES ($1, $2, $3) ON CONFLICT (job) DO NOTHING",
                )
                .bind(job)
                .bind(to_i64(at_ms))
                .bind(detail),
            )?;
            Ok(())
        }

        fn try_lease(
            &self,
            name: &str,
            holder: &str,
            now_ms: u64,
            ttl_ms: u64,
        ) -> Result<bool, KoshaError> {
            // One statement: take a free or expired lease, or renew our
            // own; a live lease of another holder is left alone and no
            // row comes back.
            Ok(self.execute(
                sqlx::query(
                    "INSERT INTO kosha.billing_leases (name, holder, expires_at_ms) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (name) DO UPDATE SET \
                         holder = EXCLUDED.holder, expires_at_ms = EXCLUDED.expires_at_ms \
                     WHERE kosha.billing_leases.holder = EXCLUDED.holder \
                        OR kosha.billing_leases.expires_at_ms <= $4",
                )
                .bind(name)
                .bind(holder)
                .bind(to_i64(now_ms.saturating_add(ttl_ms)))
                .bind(to_i64(now_ms)),
            )? == 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::{FakeBillingProvider, Invoice, MemoryBillingStore};
    use crate::stripe_webhook::parse_event;

    fn catalog() -> Catalog {
        Catalog::parse(
            r#"{"default_plan": "free", "upgrade_plan": "payg",
                "plans": {"free": {"kind": "free"},
                          "payg": {"kind": "usage", "base_fee_cents": 100},
                          "ent": {"kind": "enterprise"}}}"#,
        )
        .unwrap()
    }

    fn invoice(id: &str, tenant: &str, provider_id: Option<&str>) -> Invoice {
        Invoice {
            id: id.into(),
            tenant: tenant.into(),
            period: "2026-10".into(),
            period_start_ms: 0,
            period_end_ms: 1,
            plan: "payg".into(),
            lines: Vec::new(),
            total_cents: 500,
            issued_at_ms: 1,
            due_at_ms: 2,
            status: InvoiceStatus::Open,
            paid_at_ms: None,
            provider_invoice_id: provider_id.map(str::to_string),
        }
    }

    fn event(id: &str, event_type: &str, object: serde_json::Value) -> StripeEvent {
        parse_event(
            serde_json::json!({
                "id": id, "type": event_type, "created": 1_000,
                "data": {"object": object}
            })
            .to_string()
            .as_bytes(),
        )
        .unwrap()
    }

    #[test]
    fn events_apply_once_and_only_to_open_invoices() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let ledger = MemoryBillingLedger::new();
        store
            .insert_invoice(&invoice("acme/2026-10", "acme", Some("in_1")))
            .unwrap();
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: None,
        };
        let failed = event(
            "evt_f",
            "invoice.payment_failed",
            serde_json::json!({"id": "in_1", "metadata": {"kosha_invoice_id": "acme/2026-10"},
                               "last_payment_error": {"message": "insufficient funds"}}),
        );
        let first = handle_webhook_event(&ctx, &failed, 10).unwrap();
        assert_eq!(
            (
                first.outcome.as_str(),
                first.duplicate,
                first.tenant.as_deref()
            ),
            ("payment_failed", false, Some("acme"))
        );
        // Delivered again: acknowledged, not counted again.
        let again = handle_webhook_event(&ctx, &failed, 11).unwrap();
        assert!(again.duplicate);
        let failures = ledger.payment_failures().unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].failures, 1);
        assert_eq!(
            failures[0].last_reason.as_deref(),
            Some("insufficient funds")
        );

        // Found by provider id alone when the metadata is missing.
        let paid = event(
            "evt_p",
            "invoice.paid",
            serde_json::json!({"id": "in_1", "status_transitions": {"paid_at": 0}}),
        );
        assert_eq!(
            handle_webhook_event(&ctx, &paid, 20).unwrap().outcome,
            "paid"
        );
        let stored = store.invoice("acme/2026-10").unwrap().unwrap();
        assert_eq!(
            (stored.status, stored.paid_at_ms),
            (InvoiceStatus::Paid, Some(20))
        );
        // A void after the payment changes nothing.
        let voided = event(
            "evt_v",
            "invoice.voided",
            serde_json::json!({"id": "in_1", "metadata": {"kosha_invoice_id": "acme/2026-10"}}),
        );
        assert_eq!(
            handle_webhook_event(&ctx, &voided, 30).unwrap().outcome,
            "already_settled"
        );
        assert_eq!(
            store.invoice("acme/2026-10").unwrap().unwrap().status,
            InvoiceStatus::Paid
        );
        // Metadata naming our invoice but another provider invoice: not
        // ours to settle.
        store
            .insert_invoice(&invoice("beta/2026-10", "beta", Some("in_2")))
            .unwrap();
        let forged = event(
            "evt_x",
            "invoice.paid",
            serde_json::json!({"id": "in_9", "metadata": {"kosha_invoice_id": "beta/2026-10"}}),
        );
        assert_eq!(
            handle_webhook_event(&ctx, &forged, 40).unwrap().outcome,
            "unknown_invoice"
        );
        assert_eq!(
            store.invoice("beta/2026-10").unwrap().unwrap().status,
            InvoiceStatus::Open
        );
        assert_eq!(ledger.webhook_events().len(), 4);
    }

    #[test]
    fn finalized_records_a_provider_id_a_crashed_submission_lost() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let ledger = MemoryBillingLedger::new();
        store
            .insert_invoice(&invoice("acme/2026-10", "acme", None))
            .unwrap();
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: None,
        };
        let finalized = event(
            "evt_1",
            "invoice.finalized",
            serde_json::json!({"id": "in_7", "metadata": {"kosha_invoice_id": "acme/2026-10"}}),
        );
        assert_eq!(
            handle_webhook_event(&ctx, &finalized, 1).unwrap().outcome,
            "finalized"
        );
        assert_eq!(
            store
                .invoice("acme/2026-10")
                .unwrap()
                .unwrap()
                .provider_invoice_id
                .as_deref(),
            Some("in_7")
        );
    }

    #[test]
    fn a_completed_checkout_sets_the_payment_method_and_changes_the_plan() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let ledger = MemoryBillingLedger::new();
        let provider = FakeBillingProvider::new();
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: Some(&provider),
        };
        let session = |id: &str, tenant: &str, plan: &str, customer: &str| {
            event(
                id,
                "checkout.session.completed",
                serde_json::json!({"id": format!("cs_{id}"), "mode": "setup",
                    "customer": customer, "setup_intent": "seti_1",
                    "metadata": {"kosha_tenant": tenant, "kosha_plan": plan}}),
            )
        };
        let done =
            handle_webhook_event(&ctx, &session("e1", "acme", "payg", "cus_a"), 500).unwrap();
        assert_eq!(done.outcome, "plan_changed");
        let account = store.account("acme").unwrap().unwrap();
        assert_eq!(
            (
                account.plan.as_str(),
                account.provider_customer_id.as_deref()
            ),
            ("payg", Some("cus_a"))
        );
        assert_eq!(
            ledger
                .payment_method("acme")
                .unwrap()
                .unwrap()
                .payment_method
                .as_deref(),
            Some("pm_fake_seti_1")
        );
        assert_eq!(
            provider.default_payment_method("cus_a").as_deref(),
            Some("pm_fake_seti_1")
        );
        let changes = ledger.plan_changes("acme", 0, u64::MAX).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(
            (
                changes[0].from_plan.as_deref(),
                changes[0].to_plan.as_str(),
                changes[0].at_ms
            ),
            (None, "payg", 500)
        );
        // A plan checkout does not sell, and another customer's session
        // for this tenant, are refused.
        assert_eq!(
            handle_webhook_event(&ctx, &session("e2", "acme", "ent", "cus_a"), 600)
                .unwrap()
                .outcome,
            "plan_not_offered"
        );
        assert_eq!(
            handle_webhook_event(&ctx, &session("e3", "acme", "payg", "cus_other"), 600)
                .unwrap()
                .outcome,
            "customer_mismatch"
        );
        // Checkout again on the same plan: no second change recorded.
        handle_webhook_event(&ctx, &session("e4", "acme", "payg", "cus_a"), 700).unwrap();
        assert_eq!(ledger.plan_changes("acme", 0, u64::MAX).unwrap().len(), 1);
        // An enterprise account keeps its plan.
        store
            .put_account(&BillingAccount {
                tenant: "big".into(),
                plan: "ent".into(),
                provider_customer_id: Some("cus_b".into()),
                created_at_ms: 1,
            })
            .unwrap();
        assert_eq!(
            handle_webhook_event(&ctx, &session("e5", "big", "payg", "cus_b"), 800)
                .unwrap()
                .outcome,
            "payment_method_added"
        );
        assert_eq!(store.account("big").unwrap().unwrap().plan, "ent");

        // The portal changes the default payment method, then detaches it.
        let updated = event(
            "e6",
            "customer.updated",
            serde_json::json!({"id": "cus_a", "invoice_settings": {"default_payment_method": "pm_2"}}),
        );
        let mut updated = updated;
        updated.previous =
            serde_json::json!({"invoice_settings": {"default_payment_method": "pm_fake_seti_1"}});
        assert_eq!(
            handle_webhook_event(&ctx, &updated, 900).unwrap().outcome,
            "payment_method_updated"
        );
        let mut detached = event(
            "e7",
            "payment_method.detached",
            serde_json::json!({"id": "pm_2"}),
        );
        detached.previous = serde_json::json!({"customer": "cus_a"});
        assert_eq!(
            handle_webhook_event(&ctx, &detached, 950).unwrap().outcome,
            "payment_method_removed"
        );
        assert_eq!(
            ledger
                .payment_method("acme")
                .unwrap()
                .unwrap()
                .payment_method,
            None
        );
    }

    #[test]
    fn the_lease_has_one_holder_until_it_expires() {
        let ledger = MemoryBillingLedger::new();
        assert!(ledger.try_lease("billing", "pod-a", 0, 100).unwrap());
        assert!(!ledger.try_lease("billing", "pod-b", 50, 100).unwrap());
        assert!(ledger.try_lease("billing", "pod-a", 60, 100).unwrap());
        assert!(!ledger.try_lease("billing", "pod-b", 159, 100).unwrap());
        assert!(ledger.try_lease("billing", "pod-b", 160, 100).unwrap());
        assert!(!ledger.try_lease("billing", "pod-a", 200, 100).unwrap());
    }

    /// A memory ledger whose next `fail_plan_changes` plan-change writes
    /// fail.
    #[derive(Default)]
    struct FailingLedger {
        inner: MemoryBillingLedger,
        fail_plan_changes: Mutex<u32>,
    }

    impl PlanHistory for FailingLedger {
        fn plan_changes(&self, t: &str, f: u64, to: u64) -> Result<Vec<PlanChange>, KoshaError> {
            self.inner.plan_changes(t, f, to)
        }
    }

    impl BillingLedger for FailingLedger {
        fn record_plan_change(&self, change: &PlanChange) -> Result<(), KoshaError> {
            let mut fail = self.fail_plan_changes.lock().unwrap();
            if *fail > 0 {
                *fail -= 1;
                return Err(KoshaError::ControlPlane("injected: ledger down".into()));
            }
            self.inner.record_plan_change(change)
        }
        fn claim_webhook_event(
            &self,
            a: &str,
            b: &str,
            c: u64,
            d: u64,
        ) -> Result<bool, KoshaError> {
            self.inner.claim_webhook_event(a, b, c, d)
        }
        fn finish_webhook_event(&self, a: &str, b: &str, c: u64) -> Result<(), KoshaError> {
            self.inner.finish_webhook_event(a, b, c)
        }
        fn release_webhook_event(&self, a: &str) -> Result<(), KoshaError> {
            self.inner.release_webhook_event(a)
        }
        fn record_payment_failure(
            &self,
            a: &str,
            b: &str,
            c: u64,
            d: Option<&str>,
        ) -> Result<(), KoshaError> {
            self.inner.record_payment_failure(a, b, c, d)
        }
        fn claim_payment_retry(&self, a: &str, b: u32, c: u64) -> Result<bool, KoshaError> {
            self.inner.claim_payment_retry(a, b, c)
        }
        fn payment_failures(&self) -> Result<Vec<PaymentFailure>, KoshaError> {
            self.inner.payment_failures()
        }
        fn claim_notice(&self, n: &NoticeRecord) -> Result<bool, KoshaError> {
            self.inner.claim_notice(n)
        }
        fn release_notice(&self, id: &str) -> Result<(), KoshaError> {
            self.inner.release_notice(id)
        }
        fn notices(&self, since: u64) -> Result<Vec<NoticeRecord>, KoshaError> {
            self.inner.notices(since)
        }
        fn set_payment_method(&self, t: &str, m: Option<&str>, at: u64) -> Result<(), KoshaError> {
            self.inner.set_payment_method(t, m, at)
        }
        fn payment_method(&self, t: &str) -> Result<Option<PaymentMethodRecord>, KoshaError> {
            self.inner.payment_method(t)
        }
        fn job_done(&self, j: &str) -> Result<bool, KoshaError> {
            self.inner.job_done(j)
        }
        fn mark_job_done(&self, j: &str, at: u64, d: &str) -> Result<(), KoshaError> {
            self.inner.mark_job_done(j, at, d)
        }
        fn try_lease(&self, n: &str, h: &str, now: u64, ttl: u64) -> Result<bool, KoshaError> {
            self.inner.try_lease(n, h, now, ttl)
        }
    }

    /// Review finding 2: a plan change whose history write failed after
    /// the account was written is recorded by the retry, so the month is
    /// still prorated.
    #[test]
    fn a_failed_plan_change_record_is_repaired_by_the_retry() {
        let store = MemoryBillingStore::new();
        let ledger = FailingLedger::default();
        change_plan(&store, &ledger, "acme", "free", None, "admin", 10).unwrap();
        *ledger.fail_plan_changes.lock().unwrap() = 1;
        assert!(change_plan(&store, &ledger, "acme", "payg", None, "admin", 20).is_err());
        // Nothing half-done: the change is recorded before the account.
        assert_eq!(store.account("acme").unwrap().unwrap().plan, "free");
        assert_eq!(ledger.plan_changes("acme", 0, u64::MAX).unwrap().len(), 1);
        change_plan(&store, &ledger, "acme", "payg", None, "admin", 30).unwrap();
        let changes = ledger.plan_changes("acme", 0, u64::MAX).unwrap();
        assert_eq!(changes.len(), 2);
        assert_eq!(
            (
                changes[1].from_plan.as_deref(),
                changes[1].to_plan.as_str(),
                changes[1].at_ms
            ),
            (Some("free"), "payg", 30)
        );
        // A real no-op records nothing.
        change_plan(&store, &ledger, "acme", "payg", None, "admin", 40).unwrap();
        assert_eq!(ledger.plan_changes("acme", 0, u64::MAX).unwrap().len(), 2);

        // The account write fails after the change was recorded: the retry
        // writes the account and does not record the change again.
        let flaky = FlakyAccounts::default();
        change_plan(&flaky, &ledger, "beta", "free", None, "admin", 10).unwrap();
        *flaky.fail_puts.lock().unwrap() = 1;
        assert!(change_plan(&flaky, &ledger, "beta", "payg", None, "admin", 20).is_err());
        assert_eq!(flaky.account("beta").unwrap().unwrap().plan, "free");
        change_plan(&flaky, &ledger, "beta", "payg", None, "admin", 25).unwrap();
        assert_eq!(flaky.account("beta").unwrap().unwrap().plan, "payg");
        let changes = ledger.plan_changes("beta", 0, u64::MAX).unwrap();
        assert_eq!(
            changes
                .iter()
                .map(|c| (c.to_plan.as_str(), c.at_ms))
                .collect::<Vec<_>>(),
            vec![("free", 10), ("payg", 20)]
        );

        // An account left on the new plan without its record (an older
        // build wrote the account first) is repaired.
        store
            .put_account(&BillingAccount {
                tenant: "acme".into(),
                plan: "free".into(),
                provider_customer_id: None,
                created_at_ms: 0,
            })
            .unwrap();
        change_plan(&store, &ledger, "acme", "free", None, "admin", 50).unwrap();
        let changes = ledger.plan_changes("acme", 0, u64::MAX).unwrap();
        assert_eq!(
            (
                changes.len(),
                changes[2].from_plan.as_deref(),
                changes[2].to_plan.as_str()
            ),
            (3, Some("payg"), "free")
        );
    }

    /// A memory store whose next `fail_puts` account writes fail.
    #[derive(Default)]
    struct FlakyAccounts {
        inner: MemoryBillingStore,
        fail_puts: Mutex<u32>,
    }

    impl BillingStore for FlakyAccounts {
        fn account(&self, t: &str) -> Result<Option<BillingAccount>, KoshaError> {
            self.inner.account(t)
        }
        fn accounts(&self) -> Result<Vec<BillingAccount>, KoshaError> {
            self.inner.accounts()
        }
        fn put_account(&self, a: &BillingAccount) -> Result<(), KoshaError> {
            let mut fail = self.fail_puts.lock().unwrap();
            if *fail > 0 {
                *fail -= 1;
                return Err(KoshaError::ControlPlane("injected: store down".into()));
            }
            self.inner.put_account(a)
        }
        fn set_provider_customer(
            &self,
            t: &str,
            p: &str,
            c: &str,
            n: u64,
        ) -> Result<(), KoshaError> {
            self.inner.set_provider_customer(t, p, c, n)
        }
        fn insert_invoice(&self, i: &crate::billing::Invoice) -> Result<bool, KoshaError> {
            self.inner.insert_invoice(i)
        }
        fn invoice(&self, id: &str) -> Result<Option<crate::billing::Invoice>, KoshaError> {
            self.inner.invoice(id)
        }
        fn invoices(&self, t: Option<&str>) -> Result<Vec<crate::billing::Invoice>, KoshaError> {
            self.inner.invoices(t)
        }
        fn set_provider_invoice(&self, id: &str, p: &str) -> Result<bool, KoshaError> {
            self.inner.set_provider_invoice(id, p)
        }
        fn set_invoice_status(
            &self,
            id: &str,
            st: InvoiceStatus,
            at: Option<u64>,
        ) -> Result<bool, KoshaError> {
            self.inner.set_invoice_status(id, st, at)
        }
    }

    /// Review finding 7: a webhook event is claimed before it is applied.
    /// A concurrent or later delivery is a duplicate; a failed apply
    /// releases the claim so the redelivery applies it; a claim whose
    /// processor died is taken over once stale.
    #[test]
    fn webhook_events_are_claimed_before_they_are_applied() {
        let ledger = MemoryBillingLedger::new();
        assert!(ledger
            .claim_webhook_event("evt_1", "invoice.paid", 100, 1_000)
            .unwrap());
        // In flight: another delivery does not apply it.
        assert!(!ledger
            .claim_webhook_event("evt_1", "invoice.paid", 200, 1_000)
            .unwrap());
        ledger.release_webhook_event("evt_1").unwrap();
        assert!(ledger
            .claim_webhook_event("evt_1", "invoice.paid", 300, 1_000)
            .unwrap());
        ledger.finish_webhook_event("evt_1", "paid", 310).unwrap();
        // Finished: never again, however old, and a release is a no-op.
        ledger.release_webhook_event("evt_1").unwrap();
        assert!(!ledger
            .claim_webhook_event("evt_1", "invoice.paid", 99_999, 1_000)
            .unwrap());
        // A dead processor's claim is taken over once stale.
        assert!(ledger
            .claim_webhook_event("evt_2", "invoice.paid", 0, 1_000)
            .unwrap());
        assert!(!ledger
            .claim_webhook_event("evt_2", "invoice.paid", 1_000, 1_000)
            .unwrap());
        assert!(ledger
            .claim_webhook_event("evt_2", "invoice.paid", 1_001, 1_000)
            .unwrap());

        // Through `handle_webhook_event`: concurrent deliveries apply once.
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let ledger = FailingLedger::default();
        store
            .insert_invoice(&invoice("acme/2026-10", "acme", Some("in_1")))
            .unwrap();
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: None,
        };
        let failed = event(
            "evt_f",
            "invoice.payment_failed",
            serde_json::json!({"id": "in_1", "metadata": {"kosha_invoice_id": "acme/2026-10"}}),
        );
        let applied: Vec<bool> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| !handle_webhook_event(&ctx, &failed, 10).unwrap().duplicate)
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(applied.iter().filter(|a| **a).count(), 1);
        assert_eq!(ledger.payment_failures().unwrap()[0].failures, 1);

        // A checkout whose plan change cannot be recorded: an error (503
        // to Stripe), the claim released, and the redelivery applies it.
        let checkout = event(
            "evt_c",
            "checkout.session.completed",
            serde_json::json!({"id": "cs_1", "mode": "setup", "customer": "cus_a",
                "metadata": {"kosha_tenant": "acme", "kosha_plan": "payg"}}),
        );
        *ledger.fail_plan_changes.lock().unwrap() = 1;
        assert!(handle_webhook_event(&ctx, &checkout, 20).is_err());
        let retried = handle_webhook_event(&ctx, &checkout, 30).unwrap();
        assert!(!retried.duplicate);
        assert_eq!(ledger.plan_changes("acme", 0, u64::MAX).unwrap().len(), 1);
    }
}
