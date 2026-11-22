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
