//! Opt-in database tests for the billing store (#349). Use a dedicated
//! disposable database:
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test postgres_billing -- --ignored
#![cfg(feature = "postgres")]
use kosha_control::billing::{
    Biller, BillingAccount, BillingPeriod, BillingStore, Catalog, FakeBillingProvider, Invoice,
    InvoiceStatus, LineItem, MemoryBillingStore, PgBillingStore,
};
use kosha_control::usage::{meter, PgUsageStore, UsageEvent, UsageStore};
use kosha_control::PgStore;

fn url() -> String {
    std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL")
}

/// A tenant no other run of these tests has used.
fn fresh_tenant() -> String {
    format!("billing-test-{}", uuid::Uuid::new_v4())
}

/// Applies the migrations (007 among them), then opens the billing store.
fn store() -> PgBillingStore {
    PgStore::new(&url()).unwrap();
    PgBillingStore::new(&url()).unwrap()
}

fn invoice(tenant: &str, period: &str, total: u64) -> Invoice {
    Invoice {
        id: format!("{tenant}/{period}"),
        tenant: tenant.into(),
        period: period.into(),
        period_start_ms: 10,
        period_end_ms: 20,
        plan: "payg".into(),
        lines: vec![LineItem {
            meter: "queries".into(),
            quantity: 7,
            included: 2,
            billable: 5,
            unit: 1,
            unit_price_cents: total / 5,
            amount_cents: total,
            plan: None,
        }],
        total_cents: total,
        issued_at_ms: 30,
        due_at_ms: 40,
        status: InvoiceStatus::Open,
        paid_at_ms: None,
        provider_invoice_id: None,
    }
}

/// The same calls against Postgres and the in-memory store give the same
/// answers, so the memory store the unit tests use stands in for Postgres.
#[test]
#[ignore = "requires KOSHA_TEST_DATABASE_URL"]
fn postgres_store_behaves_like_the_memory_store() {
    let pg = store();
    let memory = MemoryBillingStore::new();
    let tenant = fresh_tenant();
    let other = fresh_tenant();
    for s in [&pg as &dyn BillingStore, &memory] {
        assert_eq!(s.account(&tenant).unwrap(), None);
        s.put_account(&BillingAccount {
            tenant: tenant.clone(),
            plan: "free".into(),
            provider_customer_id: None,
            created_at_ms: 111,
        })
        .unwrap();
        // An update changes the plan and customer, not the creation time.
        s.put_account(&BillingAccount {
            tenant: tenant.clone(),
            plan: "payg".into(),
            provider_customer_id: Some("cus_1".into()),
            created_at_ms: 999,
        })
        .unwrap();
        assert_eq!(
            s.account(&tenant).unwrap(),
            Some(BillingAccount {
                tenant: tenant.clone(),
                plan: "payg".into(),
                provider_customer_id: Some("cus_1".into()),
                created_at_ms: 111,
            })
        );
        assert!(s.accounts().unwrap().iter().any(|a| a.tenant == tenant));
        // A plan change that does not name the customer keeps it, and
        // recording a customer keeps the plan.
        s.put_account(&BillingAccount {
            tenant: tenant.clone(),
            plan: "ent".into(),
            provider_customer_id: None,
            created_at_ms: 5,
        })
        .unwrap();
        s.set_provider_customer(&tenant, "free", "cus_2", 6)
            .unwrap();
        assert_eq!(
            s.account(&tenant).unwrap(),
            Some(BillingAccount {
                tenant: tenant.clone(),
                plan: "ent".into(),
                provider_customer_id: Some("cus_2".into()),
                created_at_ms: 111,
            })
        );
        // Without an account, recording a customer creates one.
        let third = fresh_tenant();
        s.set_provider_customer(&third, "payg", "cus_3", 7).unwrap();
        assert_eq!(
            s.account(&third).unwrap(),
            Some(BillingAccount {
                tenant: third.clone(),
                plan: "payg".into(),
                provider_customer_id: Some("cus_3".into()),
                created_at_ms: 7,
            })
        );

        assert!(s.insert_invoice(&invoice(&tenant, "2026-11", 500)).unwrap());
        assert!(s.insert_invoice(&invoice(&tenant, "2026-10", 250)).unwrap());
        assert!(s.insert_invoice(&invoice(&other, "2026-10", 5)).unwrap());
        // A second insert for the same tenant and period changes nothing.
        assert!(!s
            .insert_invoice(&invoice(&tenant, "2026-10", 99_999))
            .unwrap());
        let mine = s.invoices(Some(&tenant)).unwrap();
        assert_eq!(
            mine,
            vec![
                invoice(&tenant, "2026-10", 250),
                invoice(&tenant, "2026-11", 500)
            ]
        );
        assert!(s.invoices(None).unwrap().len() >= 3);

        let id = format!("{tenant}/2026-10");
        assert!(s.set_provider_invoice(&id, "in_9").unwrap());
        assert!(s
            .set_invoice_status(&id, InvoiceStatus::Paid, Some(77))
            .unwrap());
        let paid = s.invoice(&id).unwrap().unwrap();
        assert_eq!(
            (
                paid.status,
                paid.paid_at_ms,
                paid.provider_invoice_id.as_deref()
            ),
            (InvoiceStatus::Paid, Some(77), Some("in_9"))
        );
        assert_eq!(paid.lines, invoice(&tenant, "2026-10", 250).lines);
        assert!(s
            .set_invoice_status(&id, InvoiceStatus::Void, Some(77))
            .unwrap());
        assert_eq!(s.invoice(&id).unwrap().unwrap().paid_at_ms, None);

        let missing = format!("{tenant}/1999-01");
        assert_eq!(s.invoice(&missing).unwrap(), None);
        assert!(!s.set_provider_invoice(&missing, "in_9").unwrap());
        assert!(!s
            .set_invoice_status(&missing, InvoiceStatus::Paid, Some(1))
            .unwrap());
    }
}

/// The monthly job over Postgres on both sides: usage events in
/// `kosha.usage_events`, the invoice in `kosha.billing_invoices`, and a
/// second run (as from another pod) that bills nothing more.
#[test]
#[ignore = "requires KOSHA_TEST_DATABASE_URL"]
fn a_month_of_usage_in_postgres_is_invoiced_once() {
    let billing = store();
    let usage = PgUsageStore::new(&url()).unwrap();
    let provider = FakeBillingProvider::new();
    let catalog = Catalog::parse(
        r#"{"plans": {"payg": {"kind": "usage", "base_fee_cents": 1000,
             "meters": {"queries": {"included": 1000, "unit": 1000, "unit_price_cents": 25}}}}}"#,
    )
    .unwrap();
    let tenant = fresh_tenant();
    let period = BillingPeriod::parse("2026-10").unwrap();
    billing
        .put_account(&BillingAccount {
            tenant: tenant.clone(),
            plan: "payg".into(),
            provider_customer_id: None,
            created_at_ms: 0,
        })
        .unwrap();
    let events: Vec<UsageEvent> = (0..period.hours())
        .map(|hour| UsageEvent {
            id: format!("{tenant}/q/{hour}"),
            tenant: tenant.clone(),
            namespace: "docs".into(),
            at_ms: period.start_ms() + hour * 3_600_000,
            meters: [(meter::QUERIES.to_string(), 10)].into(),
        })
        .collect();
    usage.record(&events).unwrap();

    let biller = Biller {
        catalog: &catalog,
        store: &billing,
        usage: &usage,
        provider: Some(&provider),
        plan_changes: None,
    };
    let now = period.end_ms() + 1;
    let id = format!("{tenant}/2026-10");
    let report = biller.close_period(period, now).unwrap();
    assert!(report.created.contains(&id), "{report:?}");
    // 7,440 searches, 6,440 over the included 1,000, at 25 per 1,000 = 161;
    // plus the base fee.
    let invoice = billing.invoice(&id).unwrap().unwrap();
    assert_eq!(invoice.total_cents, 1000 + 161);
    assert_eq!(invoice.status, InvoiceStatus::Open);
    let provider_id = invoice.provider_invoice_id.clone().expect("submitted");

    let again = biller.close_period(period, now + 1).unwrap();
    assert!(!again.created.contains(&id), "{again:?}");
    assert_eq!(billing.invoice(&id).unwrap().unwrap(), invoice);
    let mine: Vec<_> = provider
        .submitted()
        .into_iter()
        .filter(|s| s.invoice.tenant == tenant)
        .collect();
    assert_eq!(mine.len(), 1);
    assert_eq!(mine[0].invoice.total_cents, 1161);

    assert!(provider.pay(&provider_id));
    let sync = biller.sync_payments(now + 2).unwrap();
    assert!(sync.paid.contains(&id), "{sync:?}");
    let paid = billing.invoice(&id).unwrap().unwrap();
    assert_eq!(
        (paid.status, paid.paid_at_ms),
        (InvoiceStatus::Paid, Some(now + 2))
    );
}

/// The ledger of migration 011 against the memory ledger: plan changes,
/// webhook ids, payment failures (only those of open invoices), notices,
/// payment methods, jobs and the scheduler lease.
#[test]
#[ignore = "requires KOSHA_TEST_DATABASE_URL"]
fn postgres_ledger_behaves_like_the_memory_ledger() {
    use kosha_control::billing::PlanChange;
    use kosha_control::billing_ledger::{
        BillingLedger, MemoryBillingLedger, NoticeRecord, PgBillingLedger,
    };
    let billing = store();
    let pg = PgBillingLedger::new(&url()).unwrap();
    let memory = MemoryBillingLedger::new();
    let tenant = fresh_tenant();
    let open = invoice(&tenant, "2026-10", 500);
    let mut paid = invoice(&tenant, "2026-09", 500);
    paid.status = InvoiceStatus::Paid;
    billing.insert_invoice(&open).unwrap();
    billing.insert_invoice(&paid).unwrap();
    let ledgers: [&dyn BillingLedger; 2] = [&pg, &memory];
    for ledger in ledgers {
        for (at, from, to) in [(30, Some("free"), "payg"), (10, None, "free")] {
            ledger
                .record_plan_change(&PlanChange {
                    tenant: tenant.clone(),
                    at_ms: at,
                    from_plan: from.map(str::to_string),
                    to_plan: to.into(),
                    source: "admin".into(),
                })
                .unwrap();
        }
        let changes = ledger.plan_changes(&tenant, 0, 100).unwrap();
        assert_eq!(
            changes.iter().map(|c| c.at_ms).collect::<Vec<_>>(),
            vec![10, 30]
        );
        assert_eq!(ledger.plan_changes(&tenant, 11, 30).unwrap(), vec![]);

        let event = format!("evt_{tenant}");
        assert!(ledger
            .claim_webhook_event(&event, "invoice.paid", 5, 100)
            .unwrap());
        assert!(!ledger
            .claim_webhook_event(&event, "invoice.paid", 6, 100)
            .unwrap());
        ledger.release_webhook_event(&event).unwrap();
        assert!(ledger
            .claim_webhook_event(&event, "invoice.paid", 7, 100)
            .unwrap());
        ledger.finish_webhook_event(&event, "paid", 8).unwrap();
        assert!(!ledger
            .claim_webhook_event(&event, "invoice.paid", 10_000, 100)
            .unwrap());
        let stale = format!("evt_stale_{tenant}");
        assert!(ledger.claim_webhook_event(&stale, "x", 0, 100).unwrap());
        assert!(ledger.claim_webhook_event(&stale, "x", 101, 100).unwrap());

        for (invoice, at) in [(&open.id, 100), (&open.id, 200), (&paid.id, 50)] {
            ledger
                .record_payment_failure(invoice, &tenant, at, Some("declined"))
                .unwrap();
        }
        assert!(ledger.claim_payment_retry(&open.id, 1, 300).unwrap());
        assert!(!ledger.claim_payment_retry(&open.id, 1, 301).unwrap());
        let failure = ledger
            .payment_failures()
            .unwrap()
            .into_iter()
            .find(|f| f.invoice_id == open.id)
            .unwrap();
        assert_eq!(
            (
                failure.failures,
                failure.first_failed_at_ms,
                failure.last_failed_at_ms,
                failure.retries,
                failure.last_retry_at_ms,
                failure.last_reason.as_deref()
            ),
            (2, 100, 200, 1, Some(300), Some("declined"))
        );

        let notice = NoticeRecord {
            id: format!("{}/overdue", open.id),
            tenant: tenant.clone(),
            invoice_id: Some(open.id.clone()),
            kind: "overdue".into(),
            sent_at_ms: 1_000,
        };
        assert!(ledger.claim_notice(&notice).unwrap());
        assert!(!ledger.claim_notice(&notice).unwrap());
        assert!(ledger
            .notices(1_000)
            .unwrap()
            .iter()
            .any(|n| n.id == notice.id));
        ledger.release_notice(&notice.id).unwrap();
        assert!(ledger.claim_notice(&notice).unwrap());

        assert_eq!(ledger.payment_method(&tenant).unwrap(), None);
        ledger.set_payment_method(&tenant, Some("pm_1"), 7).unwrap();
        ledger.set_payment_method(&tenant, None, 8).unwrap();
        let method = ledger.payment_method(&tenant).unwrap().unwrap();
        assert_eq!((method.payment_method, method.updated_at_ms), (None, 8));

        let job = format!("close-period/{tenant}");
        assert!(!ledger.job_done(&job).unwrap());
        ledger.mark_job_done(&job, 1, "{}").unwrap();
        ledger.mark_job_done(&job, 2, "{}").unwrap();
        assert!(ledger.job_done(&job).unwrap());

        let lease = format!("lease-{tenant}");
        assert!(ledger.try_lease(&lease, "a", 0, 100).unwrap());
        assert!(!ledger.try_lease(&lease, "b", 50, 100).unwrap());
        assert!(ledger.try_lease(&lease, "a", 60, 100).unwrap());
        assert!(ledger.try_lease(&lease, "b", 160, 100).unwrap());
    }
    // The memory ledger keeps the paid invoice's failure; Postgres only
    // returns open invoices'.
    assert!(pg
        .payment_failures()
        .unwrap()
        .iter()
        .all(|f| f.invoice_id != paid.id));
}
