//! Dunning (#349): what happens between a payment that did not arrive and
//! the account being restricted or recovered. [`run_dunning`] is one pass:
//! the scheduler runs it every tick, and `POST /v1/admin/billing/dunning`
//! runs it on demand. It is safe to run on several pods at once.
//!
//! # State
//!
//! The restriction itself is the existing state machine in `billing`
//! (`account_state`): an invoice unpaid past its due date makes the
//! account `past_due`, then `read_only`, then `suspended`, and paying (or
//! voiding) the invoice restores `active`. Dunning drives it from outside:
//! it collects (retries) and it tells the customer, and the state follows
//! from the invoices.
//!
//! # Retries
//!
//! A failed payment (an `invoice.payment_failed` webhook, or one of these
//! retries failing) is recorded per invoice. When the tenant has a payment
//! method on file, the invoice is retried on the policy's schedule
//! (`retry_after_days`, counted from the first failure, each entry after
//! the previous retry), and also at once after the tenant puts a new
//! payment method on file. A retry that collects marks the invoice paid
//! right away. Turn Kosha's retries off (`retry_after_days: []`) when the
//! provider's own automatic retries (Stripe Smart Retries) are on, or both
//! will try.
//!
//! # Notices
//!
//! Each open invoice moves through stages: `payment_failed` (a collection
//! failed), `overdue` (past its due date), `read_only` and `suspended`
//! (the account restricted; never for enterprise plans, which are not
//! restricted). Each stage's notice goes out once per invoice, through the
//! [`Notifier`]; when several stages were reached since the last pass only
//! the most severe one is sent (the others are recorded as skipped). When
//! an invoice that had any notice is paid, a `recovered` notice follows.
//! A notice is claimed in the ledger before it is sent and released when
//! sending fails, so it is sent at most once and retried next pass.

use std::collections::BTreeMap;

use kosha_core::KoshaError;
use serde::Serialize;

use crate::billing::{
    account_state, AccountState, Invoice, InvoiceStatus, PlanKind, ProviderInvoiceStatus,
};
use crate::billing_ledger::{notices_by_invoice, BillingContext, NoticeRecord, PaymentFailure};

const DAY_MS: u64 = 86_400_000;
/// How far back `recovered` notices look for notices of a paid invoice.
const RECOVERY_LOOKBACK_MS: u64 = 120 * DAY_MS;

/// The stages of an unpaid invoice, least severe first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NoticeKind {
    PaymentFailed,
    Overdue,
    ReadOnly,
    Suspended,
    /// The invoice was paid after one of the above.
    Recovered,
}

impl NoticeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            NoticeKind::PaymentFailed => "payment_failed",
            NoticeKind::Overdue => "overdue",
            NoticeKind::ReadOnly => "read_only",
            NoticeKind::Suspended => "suspended",
            NoticeKind::Recovered => "recovered",
        }
    }
}

/// One message to a tenant about an invoice. Serialized as the HTTP
/// notifier's JSON body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Notice {
    /// `<invoice id>/<kind>`: stable, so a receiver can de-duplicate.
    pub id: String,
    pub tenant: String,
    pub kind: NoticeKind,
    pub invoice_id: String,
    /// `YYYY-MM`.
    pub period: String,
    pub amount_cents: u64,
    /// ISO 4217, lower case. Always `usd` today.
    pub currency: String,
    pub due_at_ms: u64,
    /// The account's state when the notice was made.
    pub state: AccountState,
    /// When the next automatic retry is due, if one is scheduled.
    pub next_retry_at_ms: Option<u64>,
    /// The provider's reason for the last failure, when it gave one.
    pub failure_reason: Option<String>,
    /// A plain-English sentence for the tenant.
    pub message: String,
    pub at_ms: u64,
}

/// Where notices go. `notify` returns once the notice is handed over;
/// `Err` means it was not, and it is offered again on the next pass.
pub trait Notifier: Send + Sync {
    /// A short name for logs and API answers.
    fn name(&self) -> &str;
    fn notify(&self, notice: &Notice) -> Result<(), KoshaError>;
}

/// The default [`Notifier`]: one log line per notice on stdout.
#[derive(Debug, Default, Clone, Copy)]
pub struct LogNotifier;

impl Notifier for LogNotifier {
    fn name(&self) -> &str {
        "log"
    }

    fn notify(&self, notice: &Notice) -> Result<(), KoshaError> {
        println!(
            "billing notice: tenant={} kind={} invoice={} amount_cents={} state={:?}: {}",
            notice.tenant,
            notice.kind.as_str(),
            notice.invoice_id,
            notice.amount_cents,
            notice.state,
            notice.message
        );
        Ok(())
    }
}

/// What one [`run_dunning`] pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DunningReport {
    /// Open invoices looked at.
    pub open_invoices: usize,
    /// Notice ids sent.
    pub notices_sent: Vec<String>,
    /// Notice ids recorded without sending (a more severe one went out).
    pub notices_skipped: Vec<String>,
    /// `(notice id, error)`; offered again next pass.
    pub notices_failed: Vec<(String, String)>,
    /// `(invoice id, "paid" | "open" | error)` per retry made.
    pub retries: Vec<(String, String)>,
    /// Invoices a retry collected.
    pub collected: Vec<String>,
    /// `(invoice id, error)` for reads and writes that failed.
    pub errors: Vec<(String, String)>,
}

/// When `failure`'s next retry is due under `schedule`, or `None` when the
/// schedule is used up.
pub fn next_retry_at(failure: &PaymentFailure, schedule: &[u64]) -> Option<u64> {
    let done = failure.retries as usize;
    if done >= schedule.len() {
        return None;
    }
    let offset_days: u64 = schedule[..=done].iter().sum();
    Some(
        failure
            .first_failed_at_ms
            .saturating_add(offset_days.saturating_mul(DAY_MS)),
    )
}

fn message(kind: NoticeKind, invoice: &Invoice, ctx: &BillingContext<'_>) -> String {
    let amount = format!(
        "${}.{:02}",
        invoice.total_cents / 100,
        invoice.total_cents % 100
    );
    let policy = &ctx.catalog.dunning;
    match kind {
        NoticeKind::PaymentFailed => format!(
            "The payment of {amount} for your {} invoice failed. Update your payment method \
             to avoid interruption.",
            invoice.period
        ),
        NoticeKind::Overdue => format!(
            "Your {} invoice of {amount} is past due. Writes will be refused {} days after \
             the due date and the account suspended after {} days.",
            invoice.period, policy.read_only_after_days, policy.suspend_after_days
        ),
        NoticeKind::ReadOnly => format!(
            "Your {} invoice of {amount} is overdue: the account is read-only until it is paid.",
            invoice.period
        ),
        NoticeKind::Suspended => format!(
            "Your {} invoice of {amount} is overdue: the account is suspended until it is paid.",
            invoice.period
        ),
        NoticeKind::Recovered => format!(
            "Thank you: your {} invoice of {amount} is paid and the account is fully active.",
            invoice.period
        ),
    }
}

/// One dunning pass at `now_ms`: retry what is due, then send the notices
/// that are due. See the module doc. Fails only when the invoices cannot
/// be read; per-invoice failures are in the report.
pub fn run_dunning(
    ctx: &BillingContext<'_>,
    notifier: &dyn Notifier,
    now_ms: u64,
) -> Result<DunningReport, KoshaError> {
    let mut report = DunningReport::default();
    let invoices = ctx.store.invoices(None)?;
    let accounts: BTreeMap<String, crate::billing::BillingAccount> = ctx
        .store
        .accounts()?
        .into_iter()
        .map(|a| (a.tenant.clone(), a))
        .collect();
    let failures: BTreeMap<String, PaymentFailure> = ctx
        .ledger
        .payment_failures()?
        .into_iter()
        .map(|f| (f.invoice_id.clone(), f))
        .collect();
    let policy = &ctx.catalog.dunning;

    // Retries first: a collected invoice needs no notice.
    let mut collected: Vec<String> = Vec::new();
    if let Some(provider) = ctx.provider {
        for invoice in invoices.iter().filter(|i| i.status == InvoiceStatus::Open) {
            let (Some(failure), Some(provider_id)) = (
                failures.get(&invoice.id),
                invoice.provider_invoice_id.as_deref(),
            ) else {
                continue;
            };
            let method = match ctx.ledger.payment_method(&invoice.tenant) {
                Ok(method) => method,
                Err(e) => {
                    report.errors.push((invoice.id.clone(), e.to_string()));
                    continue;
                }
            };
            let Some(method) = method.filter(|m| m.payment_method.is_some()) else {
                continue; // nothing to charge
            };
            let last_attempt = failure
                .last_retry_at_ms
                .unwrap_or(0)
                .max(failure.last_failed_at_ms);
            let scheduled =
                next_retry_at(failure, &policy.retry_after_days).is_some_and(|at| now_ms >= at);
            let new_method = method.updated_at_ms > last_attempt;
            if !scheduled && !new_method {
                continue;
            }
            let attempt = failure.retries.saturating_add(1);
            // Claimed before the provider is asked: a second pass running
            // at the same time (the scheduler and the admin route) loses
            // the claim and does not charge again.
            match ctx.ledger.claim_payment_retry(&invoice.id, attempt, now_ms) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    report.errors.push((invoice.id.clone(), e.to_string()));
                    continue;
                }
            }
            let result = provider.retry_payment(provider_id, attempt);
            match result {
                Ok(ProviderInvoiceStatus::Paid) => {
                    match ctx.store.set_invoice_status(
                        &invoice.id,
                        InvoiceStatus::Paid,
                        Some(now_ms),
                    ) {
                        Ok(_) => {
                            report.retries.push((invoice.id.clone(), "paid".into()));
                            collected.push(invoice.id.clone());
                        }
                        Err(e) => report.errors.push((invoice.id.clone(), e.to_string())),
                    }
                }
                Ok(ProviderInvoiceStatus::Void) => {
                    let _ = ctx
                        .store
                        .set_invoice_status(&invoice.id, InvoiceStatus::Void, None);
                    report.retries.push((invoice.id.clone(), "void".into()));
                }
                Ok(ProviderInvoiceStatus::Open) => {
                    report.retries.push((invoice.id.clone(), "open".into()));
                }
                Err(e) => {
                    let reason = e.to_string();
                    if let Err(e) = ctx.ledger.record_payment_failure(
                        &invoice.id,
                        &invoice.tenant,
                        now_ms,
                        Some(&reason),
                    ) {
                        report.errors.push((invoice.id.clone(), e.to_string()));
                    }
                    report.retries.push((invoice.id.clone(), reason));
                }
            }
        }
    }
    report.collected = collected.clone();

    let sent = ctx
        .ledger
        .notices(now_ms.saturating_sub(RECOVERY_LOOKBACK_MS))?;
    let sent_by_invoice = notices_by_invoice(&sent);
    let failures: BTreeMap<String, PaymentFailure> = ctx
        .ledger
        .payment_failures()?
        .into_iter()
        .map(|f| (f.invoice_id.clone(), f))
        .collect();
    let send = |kind: NoticeKind,
                invoice: &Invoice,
                state: AccountState,
                failure: Option<&PaymentFailure>,
                deliver: bool,
                report: &mut DunningReport| {
        let id = format!("{}/{}", invoice.id, kind.as_str());
        let record = NoticeRecord {
            id: id.clone(),
            tenant: invoice.tenant.clone(),
            invoice_id: Some(invoice.id.clone()),
            kind: kind.as_str().to_string(),
            sent_at_ms: now_ms,
        };
        match ctx.ledger.claim_notice(&record) {
            Ok(true) => {}
            Ok(false) => return,
            Err(e) => {
                report.notices_failed.push((id, e.to_string()));
                return;
            }
        }
        if !deliver {
            report.notices_skipped.push(id);
            return;
        }
        let notice = Notice {
            id: id.clone(),
            tenant: invoice.tenant.clone(),
            kind,
            invoice_id: invoice.id.clone(),
            period: invoice.period.clone(),
            amount_cents: invoice.total_cents,
            currency: "usd".into(),
            due_at_ms: invoice.due_at_ms,
            state,
            next_retry_at_ms: failure.and_then(|f| next_retry_at(f, &policy.retry_after_days)),
            failure_reason: failure.and_then(|f| f.last_reason.clone()),
            message: message(kind, invoice, ctx),
            at_ms: now_ms,
        };
        match notifier.notify(&notice) {
            Ok(()) => report.notices_sent.push(id),
            Err(e) => {
                let _ = ctx.ledger.release_notice(&id);
                report.notices_failed.push((id, e.to_string()));
            }
        }
    };

    for invoice in &invoices {
        let open = invoice.status == InvoiceStatus::Open && !collected.contains(&invoice.id);
        let already = sent_by_invoice.get(&invoice.id);
        if !open {
            // Paid after a notice: tell the tenant it is over.
            let dunned = already.is_some_and(|kinds| {
                !kinds.is_empty() && !kinds.contains(NoticeKind::Recovered.as_str())
            });
            let paid = invoice.status == InvoiceStatus::Paid || collected.contains(&invoice.id);
            if dunned && paid {
                send(
                    NoticeKind::Recovered,
                    invoice,
                    AccountState::Active,
                    None,
                    true,
                    &mut report,
                );
            }
            continue;
        }
        if invoice.total_cents == 0 {
            continue;
        }
        report.open_invoices += 1;
        // The plan the account is on now, as the standings that enforce
        // the state use; a tenant on no known plan is not restricted, so
        // it gets no restriction notice (treated like enterprise).
        let kind = ctx
            .catalog
            .plan_name_for(accounts.get(&invoice.tenant))
            .and_then(|name| ctx.catalog.plan(name))
            .map_or(PlanKind::Enterprise, |p| p.kind);
        let state = account_state(kind, Some(invoice.due_at_ms), now_ms, policy);
        let failure = failures.get(&invoice.id);
        let mut reached = Vec::new();
        if failure.is_some() {
            reached.push(NoticeKind::PaymentFailed);
        }
        match state {
            AccountState::Active => {}
            AccountState::PastDue => reached.push(NoticeKind::Overdue),
            AccountState::ReadOnly => reached.extend([NoticeKind::Overdue, NoticeKind::ReadOnly]),
            AccountState::Suspended => reached.extend([
                NoticeKind::Overdue,
                NoticeKind::ReadOnly,
                NoticeKind::Suspended,
            ]),
        }
        let unsent: Vec<NoticeKind> = reached
            .into_iter()
            .filter(|k| already.is_none_or(|kinds| !kinds.contains(k.as_str())))
            .collect();
        let Some(most_severe) = unsent.iter().max().copied() else {
            continue;
        };
        for kind in unsent {
            send(
                kind,
                invoice,
                state,
                failure,
                kind == most_severe,
                &mut report,
            );
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::{
        BillingAccount, BillingProvider, BillingStore, Catalog, FakeBillingProvider,
        MemoryBillingStore,
    };
    use crate::billing_ledger::{BillingLedger, MemoryBillingLedger};
    use std::sync::Mutex;

    /// Records what it is given; can be made to fail.
    #[derive(Default)]
    struct Recorder {
        notices: Mutex<Vec<Notice>>,
        down: Mutex<bool>,
    }

    impl Notifier for Recorder {
        fn name(&self) -> &str {
            "recorder"
        }
        fn notify(&self, notice: &Notice) -> Result<(), KoshaError> {
            if *self.down.lock().unwrap() {
                return Err(KoshaError::ControlPlane("injected: mail relay down".into()));
            }
            self.notices.lock().unwrap().push(notice.clone());
            Ok(())
        }
    }

    impl Recorder {
        fn kinds(&self) -> Vec<NoticeKind> {
            self.notices
                .lock()
                .unwrap()
                .iter()
                .map(|n| n.kind)
                .collect()
        }
    }

    fn catalog() -> Catalog {
        Catalog::parse(
            r#"{"dunning": {"payment_terms_days": 14, "read_only_after_days": 7,
                            "suspend_after_days": 21, "retry_after_days": [3, 5]},
                "plans": {"payg": {"kind": "usage"}, "ent": {"kind": "enterprise"}}}"#,
        )
        .unwrap()
    }

    fn submitted(
        store: &MemoryBillingStore,
        provider: &FakeBillingProvider,
        plan: &str,
    ) -> Invoice {
        let invoice = Invoice {
            id: "acme/2026-10".into(),
            tenant: "acme".into(),
            period: "2026-10".into(),
            period_start_ms: 0,
            period_end_ms: 1,
            plan: plan.into(),
            lines: Vec::new(),
            total_cents: 12_345,
            issued_at_ms: 0,
            due_at_ms: 14 * DAY_MS,
            status: InvoiceStatus::Open,
            paid_at_ms: None,
            provider_invoice_id: None,
        };
        store.insert_invoice(&invoice).unwrap();
        store
            .put_account(&BillingAccount {
                tenant: "acme".into(),
                plan: plan.into(),
                provider_customer_id: Some("cus_fake_acme".into()),
                created_at_ms: 0,
            })
            .unwrap();
        let customer = provider.create_customer("acme").unwrap();
        let id = provider.create_invoice(&customer, &invoice).unwrap();
        store.set_provider_invoice(&invoice.id, &id).unwrap();
        store.invoice(&invoice.id).unwrap().unwrap()
    }

    #[test]
    fn retry_schedule_counts_from_the_first_failure() {
        let mut failure = PaymentFailure {
            invoice_id: "i".into(),
            tenant: "t".into(),
            failures: 1,
            first_failed_at_ms: 1_000,
            last_failed_at_ms: 1_000,
            retries: 0,
            last_retry_at_ms: None,
            last_reason: None,
        };
        let schedule = [3, 5, 7];
        assert_eq!(next_retry_at(&failure, &schedule), Some(1_000 + 3 * DAY_MS));
        failure.retries = 2;
        assert_eq!(
            next_retry_at(&failure, &schedule),
            Some(1_000 + 15 * DAY_MS)
        );
        failure.retries = 3;
        assert_eq!(next_retry_at(&failure, &schedule), None);
        assert_eq!(next_retry_at(&failure, &[]), None);
    }

    /// A payment fails, the retries fail on schedule, the account goes
    /// read-only then suspended with one notice per stage, the tenant puts
    /// a working card on file, the immediate retry collects, and a
    /// recovery notice goes out.
    #[test]
    fn a_failed_payment_is_retried_noticed_restricted_and_recovered() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let ledger = MemoryBillingLedger::new();
        let provider = FakeBillingProvider::new();
        let notifier = Recorder::default();
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: Some(&provider),
        };
        let invoice = submitted(&store, &provider, "payg");
        provider.set_declining("cus_fake_acme", true);
        ledger
            .set_payment_method("acme", Some("pm_expired"), 0)
            .unwrap();
        // Collection at issue time failed (the webhook recorded it).
        ledger
            .record_payment_failure(&invoice.id, "acme", DAY_MS, Some("card expired"))
            .unwrap();

        let pass = |at_days: u64| run_dunning(&ctx, &notifier, at_days * DAY_MS).unwrap();
        let first = pass(1);
        assert_eq!(first.notices_sent, vec!["acme/2026-10/payment_failed"]);
        assert!(first.retries.is_empty());
        // Day 4: first scheduled retry, declined.
        let day4 = pass(4);
        assert_eq!(day4.retries.len(), 1, "{day4:?}");
        assert!(day4.retries[0].1.contains("declined"));
        assert!(pass(5).retries.is_empty());
        // Day 9: the second and last retry.
        assert_eq!(pass(9).retries.len(), 1);
        assert!(pass(12).retries.is_empty());
        // Due on day 14; overdue, then read-only on day 21, suspended on 35.
        assert_eq!(pass(14).notices_sent, vec!["acme/2026-10/overdue"]);
        assert!(pass(15).notices_sent.is_empty());
        assert_eq!(pass(21).notices_sent, vec!["acme/2026-10/read_only"]);
        let suspended = pass(35);
        assert_eq!(suspended.notices_sent, vec!["acme/2026-10/suspended"]);
        let last = notifier.notices.lock().unwrap().last().cloned().unwrap();
        assert_eq!(last.state, AccountState::Suspended);
        assert_eq!(last.amount_cents, 12_345);
        assert_eq!(
            last.failure_reason
                .as_deref()
                .map(|r| r.contains("declined")),
            Some(true)
        );
        assert_eq!(provider.retries(), 2);

        // A new card: retried at once, collected, recovered.
        provider.set_declining("cus_fake_acme", false);
        ledger
            .set_payment_method("acme", Some("pm_new"), 36 * DAY_MS)
            .unwrap();
        let recovered = pass(36);
        assert_eq!(recovered.collected, vec!["acme/2026-10"]);
        assert_eq!(recovered.notices_sent, vec!["acme/2026-10/recovered"]);
        assert_eq!(
            store.invoice("acme/2026-10").unwrap().unwrap().status,
            InvoiceStatus::Paid
        );
        assert!(pass(37).notices_sent.is_empty());
        assert_eq!(
            notifier.kinds(),
            vec![
                NoticeKind::PaymentFailed,
                NoticeKind::Overdue,
                NoticeKind::ReadOnly,
                NoticeKind::Suspended,
                NoticeKind::Recovered
            ]
        );
    }

    #[test]
    fn a_late_pass_sends_only_the_most_severe_notice_and_failed_sends_retry() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let ledger = MemoryBillingLedger::new();
        let provider = FakeBillingProvider::new();
        let notifier = Recorder::default();
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: None,
        };
        submitted(&store, &provider, "payg");
        *notifier.down.lock().unwrap() = true;
        // The scheduler was off for weeks: only the suspension is sent,
        // the stages before it are recorded as skipped. Sending fails.
        let failed = run_dunning(&ctx, &notifier, 40 * DAY_MS).unwrap();
        assert_eq!(failed.notices_failed.len(), 1);
        assert_eq!(failed.notices_skipped.len(), 2);
        assert!(failed.notices_sent.is_empty());
        *notifier.down.lock().unwrap() = false;
        // The failed notice is offered again (no provider: no retries).
        let late = run_dunning(&ctx, &notifier, 41 * DAY_MS).unwrap();
        assert_eq!(late.notices_sent, vec!["acme/2026-10/suspended"]);
        assert!(late.notices_skipped.is_empty());
        assert_eq!(notifier.kinds(), vec![NoticeKind::Suspended]);
    }

    #[test]
    fn an_enterprise_invoice_is_dunned_but_never_restricted() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let ledger = MemoryBillingLedger::new();
        let provider = FakeBillingProvider::new();
        let notifier = Recorder::default();
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: None,
        };
        submitted(&store, &provider, "ent");
        for day in [15, 30, 60] {
            run_dunning(&ctx, &notifier, day * DAY_MS).unwrap();
        }
        assert_eq!(notifier.kinds(), vec![NoticeKind::Overdue]);
    }

    /// Review finding 6: restriction notices follow the plan the account
    /// is on now (what the standings enforce), not the plan the invoice was
    /// rated on.
    #[test]
    fn notices_follow_the_accounts_current_plan() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let ledger = MemoryBillingLedger::new();
        let provider = FakeBillingProvider::new();
        let notifier = Recorder::default();
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: None,
        };
        // Rated on payg, then moved to enterprise: never restricted.
        submitted(&store, &provider, "payg");
        store
            .put_account(&BillingAccount {
                tenant: "acme".into(),
                plan: "ent".into(),
                provider_customer_id: None,
                created_at_ms: 0,
            })
            .unwrap();
        for day in [15, 30, 60] {
            run_dunning(&ctx, &notifier, day * DAY_MS).unwrap();
        }
        assert_eq!(notifier.kinds(), vec![NoticeKind::Overdue]);
    }

    /// Review finding 7: a retry is claimed before the provider is asked,
    /// so two passes at once (scheduler and admin route) charge once.
    #[test]
    fn a_retry_is_claimed_once() {
        let ledger = MemoryBillingLedger::new();
        assert!(
            !ledger.claim_payment_retry("inv", 1, 5).unwrap(),
            "no failure yet"
        );
        ledger
            .record_payment_failure("inv", "acme", 1, None)
            .unwrap();
        assert!(ledger.claim_payment_retry("inv", 1, 5).unwrap());
        assert!(!ledger.claim_payment_retry("inv", 1, 6).unwrap());
        assert!(ledger.claim_payment_retry("inv", 2, 7).unwrap());
        let failure = ledger.payment_failures().unwrap().pop().unwrap();
        assert_eq!((failure.retries, failure.last_retry_at_ms), (2, Some(7)));

        // Two passes racing over one due retry: the provider is asked once.
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let provider = FakeBillingProvider::new();
        let notifier = Recorder::default();
        let invoice = submitted(&store, &provider, "payg");
        let ledger = MemoryBillingLedger::new();
        ledger.set_payment_method("acme", Some("pm"), 0).unwrap();
        ledger
            .record_payment_failure(&invoice.id, "acme", DAY_MS, None)
            .unwrap();
        provider.set_declining("cus_fake_acme", true);
        let ctx = BillingContext {
            catalog: &catalog,
            store: &store,
            ledger: &ledger,
            provider: Some(&provider),
        };
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| run_dunning(&ctx, &notifier, 5 * DAY_MS).unwrap());
            }
        });
        assert_eq!(provider.retries(), 1);
    }
}
