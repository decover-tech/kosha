//! #349 tests: the billing API and the billing gate on a pod, over
//! in-memory usage and billing stores and the fake provider; and the
//! Stripe provider's requests against a local stub.

use super::*;
use billing::{Billing, StripeProvider};
use kosha_control::billing::{
    AccountState, BillingAccount, BillingPeriod, BillingProvider, BillingStore, Catalog,
    FakeBillingProvider, Invoice, InvoiceStatus, LineItem, MemoryBillingStore, Operation,
    ProviderInvoiceStatus,
};
use kosha_control::usage::{meter, MemoryUsageStore, UsageEvent, UsageStore, HOUR_MS};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use usage::{UsageConfig, UsageMeter};

const DAY_MS: u64 = 24 * HOUR_MS;
const GIB: u64 = 1 << 30;
const ADMIN_KEY: &str = "admin-secret";

const CATALOG: &str = r#"{
  "default_plan": "free",
  "plans": {
    "free": {"kind": "free",
             "meters": {"queries": {"included": 3}},
             "limits": {"namespaces": 1, "qps": 1000, "bytes_per_namespace": 1048576}},
    "payg": {"kind": "usage", "base_fee_cents": 2500,
             "meters": {
               "queries": {"included": 1000000, "unit": 1000000, "unit_price_cents": 400},
               "bytes_written": {"unit": 1073741824, "unit_price_cents": 200},
               "stored_bytes": {"included": 1073741824, "unit": 1073741824,
                                "unit_price_cents": 33}}}
  }
}"#;

/// A [`MemoryBillingStore`] whose reads can be made to fail.
#[derive(Default)]
struct FlakyBilling {
    inner: MemoryBillingStore,
    down: AtomicBool,
}

impl FlakyBilling {
    fn check(&self) -> Result<(), KoshaError> {
        if self.down.load(Ordering::SeqCst) {
            Err(KoshaError::ControlPlane(
                "injected: relation kosha.billing_accounts".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl BillingStore for FlakyBilling {
    fn account(&self, tenant: &str) -> Result<Option<BillingAccount>, KoshaError> {
        self.check()?;
        self.inner.account(tenant)
    }
    fn accounts(&self) -> Result<Vec<BillingAccount>, KoshaError> {
        self.check()?;
        self.inner.accounts()
    }
    fn put_account(&self, account: &BillingAccount) -> Result<(), KoshaError> {
        self.check()?;
        self.inner.put_account(account)
    }
    fn set_provider_customer(
        &self,
        tenant: &str,
        plan_if_new: &str,
        provider_customer_id: &str,
        now_ms: u64,
    ) -> Result<(), KoshaError> {
        self.check()?;
        self.inner
            .set_provider_customer(tenant, plan_if_new, provider_customer_id, now_ms)
    }
    fn insert_invoice(&self, invoice: &Invoice) -> Result<bool, KoshaError> {
        self.check()?;
        self.inner.insert_invoice(invoice)
    }
    fn invoice(&self, id: &str) -> Result<Option<Invoice>, KoshaError> {
        self.check()?;
        self.inner.invoice(id)
    }
    fn invoices(&self, tenant: Option<&str>) -> Result<Vec<Invoice>, KoshaError> {
        self.check()?;
        self.inner.invoices(tenant)
    }
    fn set_provider_invoice(&self, id: &str, provider_id: &str) -> Result<bool, KoshaError> {
        self.check()?;
        self.inner.set_provider_invoice(id, provider_id)
    }
    fn set_invoice_status(
        &self,
        id: &str,
        status: InvoiceStatus,
        paid_at_ms: Option<u64>,
    ) -> Result<bool, KoshaError> {
        self.check()?;
        self.inner.set_invoice_status(id, status, paid_at_ms)
    }
}

/// A standalone server with metering and billing on, and everything a
/// test needs to reach behind it.
struct Fixture {
    state: Arc<AppState>,
    usage: Arc<MemoryUsageStore>,
    store: Arc<FlakyBilling>,
    provider: Arc<FakeBillingProvider>,
    /// The billing clock, in Unix milliseconds.
    now: Arc<AtomicU64>,
}

fn fixture(catalog: &str, with_provider: bool, start_ms: u64) -> Fixture {
    fixture_with(catalog, with_provider, start_ms, |_| {})
}

/// [`fixture`], with `configure` run on the billing before the pod starts.
fn fixture_with(
    catalog: &str,
    with_provider: bool,
    start_ms: u64,
    configure: impl FnOnce(&mut Billing),
) -> Fixture {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-test-billing-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        unix_millis()
    ));
    let usage = Arc::new(MemoryUsageStore::new());
    let store = Arc::new(FlakyBilling::default());
    let provider = Arc::new(FakeBillingProvider::new());
    let now = Arc::new(AtomicU64::new(start_ms));
    let clock = {
        let now = now.clone();
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let mut billing = Billing::new(
        Catalog::parse(catalog).unwrap(),
        store.clone(),
        usage.clone(),
        with_provider.then(|| provider.clone() as Arc<dyn BillingProvider>),
        clock,
    );
    configure(&mut billing);
    let mut state = AppState::with_options(
        dir.clone(),
        StateOptions {
            usage: Some(UsageMeter::new(usage.clone(), UsageConfig::under(&dir))),
            billing: Some(billing),
            ..Default::default()
        },
    );
    state.admin_auth_key = Some(ADMIN_KEY.into());
    Fixture {
        state: Arc::new(state),
        usage,
        store,
        provider,
        now,
    }
}

fn status_of(response: &str) -> u16 {
    response
        .get(9..12)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

fn body_of(response: &str) -> serde_json::Value {
    let body = response.split_once("\r\n\r\n").expect("no body").1;
    serde_json::from_str(body).unwrap_or(serde_json::Value::Null)
}

fn bytes_of(body: &serde_json::Value) -> Vec<u8> {
    if body.is_null() {
        Vec::new()
    } else {
        serde_json::to_vec(body).unwrap()
    }
}

impl Fixture {
    fn billing(&self) -> &Billing {
        self.state.billing.as_ref().unwrap()
    }

    fn refresh(&self) {
        self.billing().refresh().unwrap();
    }

    fn advance(&self, ms: u64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }

    /// A tenant's request as `dispatch` treats it once authenticated: the
    /// billing gate, then the router.
    fn call(&self, tenant: &str, line: &str, body: serde_json::Value) -> String {
        let line = format!("{line} HTTP/1.1\r\n");
        billing::gate(&self.state, tenant, &line).unwrap_or_else(|| {
            route(
                &line,
                &HashMap::new(),
                &bytes_of(&body),
                tenant,
                &self.state,
            )
        })
    }

    /// An operator's request, with the admin credential.
    fn admin(&self, line: &str, body: serde_json::Value) -> String {
        let headers = HashMap::from([("authorization".into(), format!("Bearer {ADMIN_KEY}"))]);
        route(
            &format!("{line} HTTP/1.1\r\n"),
            &headers,
            &bytes_of(&body),
            SERVICE_TENANT,
            &self.state,
        )
    }

    fn admin_ok(&self, line: &str, body: serde_json::Value) -> serde_json::Value {
        let response = self.admin(line, body);
        assert_eq!(status_of(&response), 200, "{response}");
        body_of(&response)
    }

    fn record(&self, tenant: &str, id: &str, at_ms: u64, meters: &[(&str, u64)]) {
        self.usage
            .record(&[UsageEvent {
                id: format!("{tenant}/{id}"),
                tenant: tenant.into(),
                namespace: "docs".into(),
                at_ms,
                meters: meters.iter().map(|(m, q)| (m.to_string(), *q)).collect(),
            }])
            .unwrap();
    }
}

fn doc(id: &str, text: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "fields": [{"name": "title", "field_type": "Text", "value": text}]})
}

fn docs(id: &str) -> serde_json::Value {
    serde_json::json!({"documents": [doc(id, "quick fox")]})
}

fn oct() -> BillingPeriod {
    BillingPeriod::parse("2026-10").unwrap()
}

/// The issue's "done when" through the server's own API: an operator puts
/// a customer on the usage plan, a month of synthetic usage accrues, the
/// monthly job invoices the expected amount and hands it to the provider,
/// non-payment restricts the tenant step by step, and the payment clears
/// it.
#[test]
fn a_customer_is_charged_for_a_month_and_restricted_until_it_pays() {
    let oct = oct();
    // Mid-September: the customer signs up.
    let f = fixture(CATALOG, true, oct.start_ms() - 15 * DAY_MS);
    let put = f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "payg"}),
    );
    assert_eq!(put["account"]["plan"], "payg");
    assert_eq!(put["standing"]["state"], "active");

    // October: every hour 3 GiB stored and 2,000 searches; 1 GiB written a
    // day.
    for hour in 0..oct.hours() {
        let at = oct.start_ms() + hour * HOUR_MS;
        f.record(
            "acme",
            &format!("s{hour}"),
            at,
            &[(meter::STORED_BYTES, 3 * GIB)],
        );
        f.record(
            "acme",
            &format!("q{hour}"),
            at + 1,
            &[(meter::QUERIES, 2_000)],
        );
        if hour % 24 == 0 {
            f.record(
                "acme",
                &format!("w{hour}"),
                at + 2,
                &[(meter::BYTES_WRITTEN, GIB)],
            );
        }
    }

    // Mid-October the tenant can see what it has run up, and the month
    // cannot be closed yet.
    f.now.store(oct.start_ms() + 15 * DAY_MS, Ordering::SeqCst);
    let mid = body_of(&f.call("acme", "GET /v1/billing", serde_json::Value::Null));
    assert_eq!(mid["plan"], "payg");
    assert_eq!(mid["period"], "2026-10");
    assert_eq!(mid["usage"]["queries"], 15 * 24 * 2_000);
    assert_eq!(mid["invoices"], serde_json::json!([]));
    let early = f.admin(
        "POST /v1/admin/billing/close-period",
        serde_json::json!({"period": "2026-10"}),
    );
    assert_eq!(status_of(&early), 400, "{early}");

    // 1 November, 02:00: the monthly job, with no period named, closes
    // October.
    f.now.store(oct.end_ms() + 2 * HOUR_MS, Ordering::SeqCst);
    let closed_at = oct.end_ms() + 2 * HOUR_MS;
    let report = f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    assert_eq!(report["period"], "2026-10");
    assert_eq!(report["created"], serde_json::json!(["acme/2026-10"]));
    assert_eq!(report["submitted"], 1);
    assert_eq!(report["errors"], serde_json::json!([]));

    //   base fee                                                 2500
    //   queries: 1,488,000, 488,000 over 1M at 400 per 1M         195
    //   bytes_written: 31 GiB at 200                             6200
    //   stored_bytes: average 3 GiB, 2 over the included, at 33    66
    const EXPECTED_CENTS: u64 = 2500 + 195 + 6200 + 66;
    let bill = body_of(&f.call("acme", "GET /v1/billing", serde_json::Value::Null));
    let invoice = &bill["invoices"][0];
    assert_eq!(invoice["id"], "acme/2026-10");
    assert_eq!(invoice["total_cents"], EXPECTED_CENTS);
    assert_eq!(invoice["status"], "open");
    assert_eq!(invoice["due_at_ms"], closed_at + 14 * DAY_MS);
    let amounts: Vec<(String, u64)> = invoice["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| {
            (
                l["meter"].as_str().unwrap().to_string(),
                l["amount_cents"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        amounts,
        vec![
            ("base_fee".to_string(), 2500),
            ("bytes_written".to_string(), 6200),
            ("queries".to_string(), 195),
            ("stored_bytes".to_string(), 66),
        ]
    );

    // The provider was asked to collect exactly that.
    let submitted = f.provider.submitted();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].invoice.total_cents, EXPECTED_CENTS);
    assert_eq!(submitted[0].customer, "cus_fake_acme");

    // A second run of the job changes nothing.
    let again = f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::json!({"period": "2026-10"}),
    );
    assert_eq!(
        (again["created"].clone(), again["existing"].clone()),
        (serde_json::json!([]), serde_json::json!(1))
    );
    assert_eq!(f.provider.create_invoice_calls(), 1);

    // Unpaid. Until a week past due nothing is restricted.
    let write = |f: &Fixture| f.call("acme", "POST /v1/namespaces/docs/documents", docs("d1"));
    let search = |f: &Fixture| {
        f.call(
            "acme",
            "POST /v1/namespaces/docs/search",
            serde_json::json!({"query_text": "fox"}),
        )
    };
    f.now.store(closed_at + (14 + 6) * DAY_MS, Ordering::SeqCst);
    f.refresh();
    assert_eq!(
        f.billing().standing("acme").unwrap().state,
        AccountState::PastDue
    );
    assert_eq!(status_of(&write(&f)), 200);
    assert_eq!(status_of(&search(&f)), 200);

    // A week past due: read-only. Writes are refused, searches and
    // deletes are not.
    f.advance(DAY_MS);
    f.refresh();
    let refused = write(&f);
    assert_eq!(status_of(&refused), 402, "{refused}");
    let body = body_of(&refused);
    assert_eq!(body["reason"], "billing_read_only");
    assert_eq!(body["state"], "read_only");
    assert_eq!(body["plan"], "payg");
    assert_eq!(status_of(&search(&f)), 200);
    let delete = f.call(
        "acme",
        "POST /v1/namespaces/docs/delete",
        serde_json::json!({"ids": ["d1"]}),
    );
    assert_ne!(status_of(&delete), 402, "{delete}");
    // Another tenant is not affected.
    assert_eq!(
        status_of(&f.call("globex", "POST /v1/namespaces/docs/documents", docs("g1"))),
        200
    );

    // Three weeks past due: suspended. Only billing and usage answer.
    f.advance(14 * DAY_MS);
    f.refresh();
    for refused in [
        write(&f),
        search(&f),
        f.call("acme", "GET /v1/namespaces", serde_json::Value::Null),
        f.call(
            "acme",
            "DELETE /v1/namespaces/docs",
            serde_json::Value::Null,
        ),
    ] {
        assert_eq!(status_of(&refused), 402, "{refused}");
        assert_eq!(body_of(&refused)["reason"], "billing_suspended");
    }
    let bill = f.call("acme", "GET /v1/billing", serde_json::Value::Null);
    assert_eq!(status_of(&bill), 200, "{bill}");
    assert_eq!(body_of(&bill)["state"], "suspended");
    assert_eq!(
        status_of(&f.call("acme", "GET /v1/usage", serde_json::Value::Null)),
        200
    );
    // The operator is never locked out.
    assert_eq!(
        billing::gate(&f.state, SERVICE_TENANT, "POST /v1/admin/gc HTTP/1.1\r\n"),
        None
    );

    // The customer pays; the next sync restores the account.
    let sync = f.admin_ok("POST /v1/admin/billing/sync", serde_json::Value::Null);
    assert_eq!(sync["paid"], serde_json::json!([]));
    assert!(f.provider.pay(&submitted[0].provider_invoice_id));
    let sync = f.admin_ok("POST /v1/admin/billing/sync", serde_json::Value::Null);
    assert_eq!(sync["paid"], serde_json::json!(["acme/2026-10"]));
    assert_eq!(status_of(&write(&f)), 200);
    assert_eq!(status_of(&search(&f)), 200);
    let listed = f.admin_ok(
        "GET /v1/admin/billing/invoices?tenant=acme",
        serde_json::Value::Null,
    );
    assert_eq!(listed["invoices"][0]["status"], "paid");
    assert_eq!(
        listed["invoices"][0]["paid_at_ms"],
        f.now.load(Ordering::SeqCst)
    );
}

fn http(
    addr: std::net::SocketAddr,
    line: &str,
    key: Option<&str>,
    body: serde_json::Value,
) -> String {
    let body = bytes_of(&body);
    let auth = key.map_or(String::new(), |k| format!("authorization: Bearer {k}\r\n"));
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    stream
        .write_all(
            format!(
                "{line} HTTP/1.1\r\nhost: x\r\n{auth}content-length: {}\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .unwrap();
    stream.write_all(&body).unwrap();
    http::read_response_string(&mut stream).unwrap()
}

/// The free tier through a listening server, so through `dispatch`
/// itself: real writes and searches are metered, the allowance runs out,
/// searches get 402 while writes go on, the plan's namespace quota holds,
/// and moving the tenant to a paid plan lifts the block. Auth is off in
/// tests, so the tenant is `dev`, on the default (free) plan.
#[test]
fn free_tier_is_enforced_in_dispatch_and_an_upgrade_lifts_it() {
    let f = fixture(CATALOG, true, unix_millis());
    // Real usage is stamped with the real time.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let state = f.state.clone();
        std::thread::spawn(move || serve(listener, state, Duration::from_secs(30)));
    }
    let search = || {
        http(
            addr,
            "POST /v1/namespaces/docs/search",
            None,
            serde_json::json!({"query_text": "fox", "no_cache": true}),
        )
    };
    let settle = |f: &Fixture| {
        f.state.usage.as_ref().unwrap().flush().unwrap();
        f.now.store(unix_millis(), Ordering::SeqCst);
        f.refresh();
    };

    let wrote = http(addr, "POST /v1/namespaces/docs/documents", None, docs("d1"));
    assert_eq!(status_of(&wrote), 200, "{wrote}");
    for _ in 0..2 {
        assert_eq!(status_of(&search()), 200);
    }
    settle(&f);
    // Two of three used: still allowed.
    assert_eq!(status_of(&search()), 200);
    settle(&f);

    let refused = search();
    assert_eq!(status_of(&refused), 402, "{refused}");
    assert!(
        refused.contains("content-type: application/json"),
        "{refused}"
    );
    let body = body_of(&refused);
    assert_eq!(body["reason"], "free_tier_exceeded");
    assert_eq!(
        (
            &body["meter"],
            &body["used"],
            &body["included"],
            &body["plan"]
        ),
        (
            &serde_json::json!("queries"),
            &serde_json::json!(3),
            &serde_json::json!(3),
            &serde_json::json!("free")
        )
    );
    // A refused search is not metered.
    settle(&f);
    assert_eq!(
        f.billing().standing("dev").unwrap().free_tier_exceeded[0].used,
        3
    );

    // Writes are a different meter, and the tenant can see where it
    // stands.
    let wrote = http(addr, "POST /v1/namespaces/docs/documents", None, docs("d2"));
    assert_eq!(status_of(&wrote), 200, "{wrote}");
    let bill = http(addr, "GET /v1/billing", None, serde_json::Value::Null);
    assert_eq!(status_of(&bill), 200, "{bill}");
    let bill = body_of(&bill);
    assert_eq!(
        (&bill["plan"], &bill["kind"]),
        (&serde_json::json!("free"), &serde_json::json!("free"))
    );
    assert_eq!(bill["free_tier_exceeded"][0]["meter"], "queries");
    assert_eq!(bill["accrued"]["total_cents"], 0);

    // The plan's quota defaults reach the tenant limiter: the free plan
    // holds one namespace.
    let second = http(
        addr,
        "POST /v1/namespaces/other/documents",
        None,
        docs("d1"),
    );
    // A quota, not a rate: 403 since #350.
    assert_eq!(status_of(&second), 403, "{second}");
    assert_eq!(body_of(&second)["code"], "namespace_quota_exceeded");
    assert_eq!(body_of(&second)["reason"], "namespace_quota_exceeded");

    // A tenant key cannot change its own plan.
    let forbidden = http(
        addr,
        "POST /v1/admin/billing/accounts",
        None,
        serde_json::json!({"tenant": "dev", "plan": "payg"}),
    );
    assert_eq!(status_of(&forbidden), 403, "{forbidden}");

    // The upgrade path: the operator moves the tenant to the usage plan.
    let upgraded = http(
        addr,
        "POST /v1/admin/billing/accounts",
        Some(ADMIN_KEY),
        serde_json::json!({"tenant": "dev", "plan": "payg"}),
    );
    assert_eq!(status_of(&upgraded), 200, "{upgraded}");
    assert_eq!(status_of(&search()), 200);
    let second = http(
        addr,
        "POST /v1/namespaces/other/documents",
        None,
        docs("d1"),
    );
    assert_eq!(status_of(&second), 200, "{second}");
}

#[test]
fn billing_off_changes_nothing() {
    let state = AppState::with_options(crate::tests::test_dir(), StateOptions::default());
    assert!(state.billing.is_none());
    assert!(!state.tenant_limits.enabled());
    for line in [
        "POST /v1/namespaces/docs/documents HTTP/1.1\r\n",
        "POST /v1/namespaces/docs/search HTTP/1.1\r\n",
    ] {
        assert_eq!(billing::gate(&state, "acme", line), None);
    }
    let response = route(
        "GET /v1/billing HTTP/1.1\r\n",
        &HashMap::new(),
        &[],
        "acme",
        &state,
    );
    assert_eq!(status_of(&response), 404, "{response}");
    assert!(response.contains("billing is off"), "{response}");
}

/// With billing on but no default plan, a tenant nobody put on a plan is
/// neither restricted nor limited nor invoiced.
#[test]
fn a_tenant_without_a_plan_is_left_alone() {
    let catalog = r#"{"plans": {
        "free": {"kind": "free", "meters": {"queries": {"included": 0}},
                 "limits": {"namespaces": 1}}}}"#;
    let f = fixture(catalog, true, oct().end_ms() + 1);
    f.record("acme", "q", oct().start_ms(), &[(meter::QUERIES, 50)]);
    f.refresh();
    assert_eq!(f.billing().standing("acme"), None);
    assert_eq!(f.state.tenant_limits.limits("acme"), None);
    assert_eq!(
        status_of(&f.call("acme", "POST /v1/namespaces/a/documents", docs("d"))),
        200
    );
    assert_eq!(
        status_of(&f.call("acme", "POST /v1/namespaces/b/documents", docs("d"))),
        200
    );
    let bill = f.call("acme", "GET /v1/billing", serde_json::Value::Null);
    assert_eq!(status_of(&bill), 404, "{bill}");
    let report = f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    assert_eq!(report["created"], serde_json::json!([]));

    // Put on the free plan with nothing included, it is blocked at once
    // on this pod.
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "free"}),
    );
    let refused = f.call(
        "acme",
        "POST /v1/namespaces/a/search",
        serde_json::json!({"query_text": "fox"}),
    );
    assert_eq!(status_of(&refused), 402, "{refused}");
    assert_eq!(
        f.state.tenant_limits.limits("acme").unwrap().namespaces,
        Some(1)
    );
}

/// The operator's limits file wins over a plan's quota defaults field by
/// field; the plan fills in what the file leaves unset.
#[test]
fn limits_file_overrides_plan_quotas_field_by_field() {
    let f = fixture(CATALOG, false, oct().start_ms());
    let file = tenant_limits::LimitsConfig::parse(
        r#"{"tenants": {"acme": {"qps": 50, "vector_dims": 8, "bytes_per_namespace": 2097152}}}"#,
        None,
    )
    .unwrap();
    let file_only = tenant_limits::TenantLimiter::new(Arc::new(
        tenant_limits::ConfigLimitsProvider::from_config(file),
    ));
    let limiter = file_only.with_plan_defaults(f.state.billing.clone().unwrap());

    // A pod that has not read the billing store yet does not know who has
    // an account, so nobody gets the default plan's quotas: only the file.
    let acme = limiter.limits("acme").unwrap();
    assert_eq!(
        (acme.qps, acme.namespaces, acme.vector_dims),
        (Some(50.0), None, Some(8))
    );
    assert_eq!(limiter.limits("globex"), None);
    f.store.down.store(true, Ordering::SeqCst);
    assert!(f.billing().refresh().is_err());
    assert_eq!(limiter.limits("globex"), None);
    f.store.down.store(false, Ordering::SeqCst);
    f.refresh();

    // On the default (free) plan: namespaces 1, qps 1000.
    let acme = limiter.limits("acme").unwrap();
    assert_eq!(
        (acme.qps, acme.namespaces, acme.vector_dims),
        (Some(50.0), Some(1), Some(8))
    );
    let other = limiter.limits("globex").unwrap();
    assert_eq!(acme.bytes_per_namespace, Some(2097152));
    assert_eq!(other.bytes_per_namespace, Some(1048576));
    assert_eq!(
        (other.qps, other.namespaces, other.vector_dims),
        (Some(1000.0), Some(1), None)
    );

    // On a plan without quotas only the file applies.
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "payg"}),
    );
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "globex", "plan": "payg"}),
    );
    let acme = limiter.limits("acme").unwrap();
    assert_eq!((acme.qps, acme.namespaces), (Some(50.0), None));
    assert_eq!(limiter.limits("globex"), None);
}

/// The Stripe secret key goes to the configured base URL, so that must be
/// https, or this machine.
#[test]
fn stripe_base_must_be_https_or_local() {
    for ok in [
        "https://api.stripe.com",
        "https://stripe-proxy.example:8443/x",
        "http://127.0.0.1:12111",
        "http://localhost",
        "http://[::1]:9000/",
    ] {
        assert!(billing::stripe_base_allowed(ok), "{ok}");
    }
    for bad in [
        "http://api.stripe.com",
        "http://localhost@evil.example",
        "http://localhost.evil.example",
        "http://127.0.0.1.evil.example:80",
        "ftp://localhost",
        "api.stripe.com",
        "",
    ] {
        assert!(!billing::stripe_base_allowed(bad), "{bad}");
    }
}

#[test]
fn requests_are_classified_for_the_gate() {
    for (line, expected) in [
        ("GET /v1/billing", Operation::Account),
        ("GET /v1/usage?from=1", Operation::Account),
        ("POST /v1/namespaces/a/search", Operation::Query),
        ("GET /search?ns=a&q=b", Operation::Query),
        ("POST /search", Operation::Query),
        ("POST /v1/namespaces/a/documents", Operation::Write),
        ("POST /v1/namespaces", Operation::Write),
        ("POST /v1/namespaces/a/flush", Operation::Write),
        ("POST /v1/namespaces/a/schema", Operation::Write),
        ("POST /v1/namespaces/a/branch", Operation::Write),
        ("POST /index", Operation::Write),
        ("POST /replace", Operation::Write),
        ("POST /v1/namespaces/a/delete", Operation::Delete),
        ("POST /delete", Operation::Delete),
        ("DELETE /v1/namespaces/a", Operation::Delete),
        ("DELETE /v1/namespaces/a/branch", Operation::Delete),
        ("GET /v1/namespaces", Operation::Other),
        ("POST /v1/namespaces/a/exists", Operation::Other),
        ("GET /v1/namespaces/a/stats", Operation::Other),
        // Not the tenant's own account read.
        ("POST /v1/billing", Operation::Other),
        ("GET /v1/billing/x", Operation::Other),
    ] {
        assert_eq!(
            billing::operation(&format!("{line} HTTP/1.1\r\n")),
            expected,
            "{line}"
        );
    }
}

/// Without a provider invoices are recorded and settled by hand; and the
/// admin routes refuse what they cannot do.
#[test]
fn manual_invoicing_and_admin_validation() {
    let oct = oct();
    let f = fixture(CATALOG, false, oct.start_ms() - DAY_MS);
    for (body, why) in [
        (serde_json::json!({"tenant": "acme"}), "expected"),
        (
            serde_json::json!({"tenant": "acme", "plan": "gold"}),
            "unknown plan",
        ),
        (
            serde_json::json!({"tenant": "a/b", "plan": "payg"}),
            "invalid tenant",
        ),
        (
            serde_json::json!({"tenant": "service", "plan": "payg"}),
            "invalid tenant",
        ),
    ] {
        let response = f.admin("POST /v1/admin/billing/accounts", body);
        assert_eq!(status_of(&response), 400, "{response}");
        assert!(response.contains(why), "{response}");
    }
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "payg"}),
    );
    f.now.store(oct.end_ms(), Ordering::SeqCst);
    for bad in [
        serde_json::json!({"period": "October"}),
        serde_json::json!({"period": 10}),
    ] {
        let response = f.admin("POST /v1/admin/billing/close-period", bad);
        assert_eq!(status_of(&response), 400, "{response}");
    }
    // A tenant credential does not reach the admin routes.
    let response = f.call(
        "acme",
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    assert_eq!(status_of(&response), 403, "{response}");

    let report = f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    assert_eq!(report["created"], serde_json::json!(["acme/2026-10"]));
    assert_eq!(report["submitted"], 0);
    assert!(f.provider.submitted().is_empty());
    let accounts = f.admin_ok("GET /v1/admin/billing/accounts", serde_json::Value::Null);
    assert_eq!(accounts["provider"], serde_json::Value::Null);
    assert_eq!(accounts["accounts"][0]["tenant"], "acme");
    assert_eq!(accounts["accounts"][0]["plan_known"], true);
    assert_eq!(accounts["plans"]["payg"]["base_fee_cents"], 2500);

    // Long unpaid: suspended.
    f.advance(60 * DAY_MS);
    f.refresh();
    assert_eq!(
        status_of(&f.call("acme", "GET /v1/namespaces", serde_json::Value::Null)),
        402
    );
    let missing = f.admin(
        "POST /v1/admin/billing/invoices/mark-paid",
        serde_json::json!({"invoice_id": "acme/1999-01"}),
    );
    assert_eq!(status_of(&missing), 404, "{missing}");
    let paid = f.admin_ok(
        "POST /v1/admin/billing/invoices/mark-paid",
        serde_json::json!({"invoice_id": "acme/2026-10"}),
    );
    assert_eq!(paid["invoice"]["status"], "paid");
    assert_eq!(paid["invoice"]["total_cents"], 2500);
    assert_eq!(
        status_of(&f.call("acme", "GET /v1/namespaces", serde_json::Value::Null)),
        200
    );
    // Marking it again is a no-op; a void invoice cannot be paid.
    let again = f.admin_ok(
        "POST /v1/admin/billing/invoices/mark-paid",
        serde_json::json!({"invoice_id": "acme/2026-10"}),
    );
    assert_eq!(
        again["invoice"]["paid_at_ms"],
        paid["invoice"]["paid_at_ms"]
    );
    f.store
        .set_invoice_status("acme/2026-10", InvoiceStatus::Void, None)
        .unwrap();
    let void = f.admin(
        "POST /v1/admin/billing/invoices/mark-paid",
        serde_json::json!({"invoice_id": "acme/2026-10"}),
    );
    assert_eq!(status_of(&void), 409, "{void}");
}

/// A pod that cannot reach the billing store keeps enforcing what it last
/// knew, and does not show the store's error text to a tenant.
#[test]
fn a_store_outage_keeps_the_last_standings() {
    let oct = oct();
    let f = fixture(CATALOG, false, oct.start_ms() - DAY_MS);
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "payg"}),
    );
    f.now.store(oct.end_ms(), Ordering::SeqCst);
    f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    f.advance(60 * DAY_MS);
    f.refresh();
    let suspended = |f: &Fixture| {
        status_of(&f.call("acme", "GET /v1/namespaces", serde_json::Value::Null)) == 402
    };
    assert!(suspended(&f));

    f.store.down.store(true, Ordering::SeqCst);
    assert!(f.billing().refresh().is_err());
    assert!(suspended(&f));
    let bill = f.call("acme", "GET /v1/billing", serde_json::Value::Null);
    assert_eq!(status_of(&bill), 503, "{bill}");
    assert!(!bill.contains("relation"), "{bill}");
    let admin = f.admin("GET /v1/admin/billing/invoices", serde_json::Value::Null);
    assert_eq!(status_of(&admin), 503, "{admin}");
    assert!(admin.contains("relation"), "{admin}");

    f.store.down.store(false, Ordering::SeqCst);
    let accounts = f.admin_ok("GET /v1/admin/billing/accounts", serde_json::Value::Null);
    assert!(
        accounts["last_refresh_error"]
            .as_str()
            .is_some_and(|e| e.contains("relation")),
        "{accounts}"
    );
    f.refresh();
    assert!(suspended(&f));
}

// ─── Stripe ─────────────────────────────────────────────────────────────────

/// One request the stub received.
#[derive(Debug, Clone)]
struct StubRequest {
    method: String,
    path: String,
    authorization: String,
    idempotency_key: String,
    form: BTreeMap<String, String>,
}

/// A local HTTP server standing in for Stripe: it records each request
/// and answers with what the real API would, for the calls the provider
/// makes. `invoice_status` is the status `GET /v1/invoices/{id}` reports.
fn stripe_stub(invoice_status: &'static str) -> (String, Arc<Mutex<Vec<StubRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut parts = line.split_whitespace();
            let method = parts.next().unwrap_or_default().to_string();
            let path = parts.next().unwrap_or_default().to_string();
            let mut headers = HashMap::new();
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let Some((name, value)) = header.trim_end().split_once(':') else {
                    break;
                };
                headers.insert(name.to_ascii_lowercase(), value.trim().to_string());
            }
            let length: usize = headers
                .get("content-length")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let form = String::from_utf8(body)
                .unwrap()
                .split('&')
                .filter_map(|pair| pair.split_once('='))
                .map(|(k, v)| (url_decode(k), url_decode(v)))
                .collect();
            let (status, answer) = match (method.as_str(), path.as_str()) {
                ("POST", "/v1/customers") => (200, serde_json::json!({"id": "cus_stub1"})),
                ("POST", "/v1/invoices") => (
                    200,
                    serde_json::json!({"id": "in_stub1", "status": "draft"}),
                ),
                ("POST", "/v1/invoiceitems") => (200, serde_json::json!({"id": "ii_stub"})),
                ("POST", "/v1/invoices/in_stub1/finalize") => {
                    (200, serde_json::json!({"id": "in_stub1", "status": "open"}))
                }
                ("GET", "/v1/invoices/in_stub1") => (
                    200,
                    serde_json::json!({"id": "in_stub1", "status": invoice_status}),
                ),
                _ => (
                    404,
                    serde_json::json!({"error": {"message": "No such invoice: 'in_missing'"}}),
                ),
            };
            seen.lock().unwrap().push(StubRequest {
                method,
                path,
                authorization: headers.get("authorization").cloned().unwrap_or_default(),
                idempotency_key: headers.get("idempotency-key").cloned().unwrap_or_default(),
                form,
            });
            let answer = answer.to_string();
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{answer}",
                    answer.len()
                )
                .as_bytes(),
            );
        }
    });
    (base, requests)
}

/// The Stripe provider's requests, against a local stub (no call leaves
/// the machine, and nothing here shows that Stripe accepts them): a draft
/// invoice, one item per charged line in cents, finalization, each with a
/// stable idempotency key; and the status mapping.
#[test]
fn stripe_provider_sends_the_invoice_as_items_in_cents() {
    let (base, requests) = stripe_stub("paid");
    let stripe = StripeProvider::new(&base, "sk_test_stub", 14).unwrap();
    assert_eq!(stripe.name(), "stripe");
    assert_eq!(stripe.create_customer("acme & co").unwrap(), "cus_stub1");

    let line = |meter: &str, amount_cents: u64| LineItem {
        meter: meter.into(),
        quantity: 10,
        included: 4,
        billable: 6,
        unit: 1,
        unit_price_cents: 1,
        amount_cents,
    };
    let invoice = Invoice {
        id: "acme/2026-10".into(),
        tenant: "acme".into(),
        period: "2026-10".into(),
        period_start_ms: 0,
        period_end_ms: 1,
        plan: "payg".into(),
        lines: vec![
            line("base_fee", 2500),
            line("bytes_written", 0),
            line("queries", 195),
        ],
        total_cents: 2695,
        issued_at_ms: 2,
        due_at_ms: 3,
        status: InvoiceStatus::Open,
        paid_at_ms: None,
        provider_invoice_id: None,
    };
    assert_eq!(
        stripe.create_invoice("cus_stub1", &invoice).unwrap(),
        "in_stub1"
    );
    assert_eq!(
        stripe.invoice_status("in_stub1").unwrap(),
        ProviderInvoiceStatus::Paid
    );

    let requests = requests.lock().unwrap().clone();
    let calls: Vec<(&str, &str, &str)> = requests
        .iter()
        .map(|r| {
            (
                r.method.as_str(),
                r.path.as_str(),
                r.idempotency_key.as_str(),
            )
        })
        .collect();
    assert_eq!(
        calls,
        vec![
            ("POST", "/v1/customers", "kosha-customer-acme & co"),
            ("POST", "/v1/invoices", "kosha-invoice-acme/2026-10"),
            (
                "POST",
                "/v1/invoiceitems",
                "kosha-invoice-acme/2026-10-line-base_fee"
            ),
            (
                "POST",
                "/v1/invoiceitems",
                "kosha-invoice-acme/2026-10-line-queries"
            ),
            (
                "POST",
                "/v1/invoices/in_stub1/finalize",
                "kosha-invoice-acme/2026-10-finalize"
            ),
            ("GET", "/v1/invoices/in_stub1", ""),
        ]
    );
    assert!(requests
        .iter()
        .all(|r| r.authorization == "Bearer sk_test_stub"));
    // Form values survive encoding.
    assert_eq!(requests[0].form["name"], "acme & co");
    assert_eq!(requests[0].form["metadata[kosha_tenant]"], "acme & co");
    let draft = &requests[1].form;
    assert_eq!(draft["customer"], "cus_stub1");
    assert_eq!(draft["collection_method"], "send_invoice");
    assert_eq!(draft["days_until_due"], "14");
    assert_eq!(draft["metadata[kosha_invoice_id]"], "acme/2026-10");
    // The items add up to the invoice total; the zero line is not sent.
    let items: Vec<(&str, &str, &str)> = requests[2..4]
        .iter()
        .map(|r| {
            (
                r.form["invoice"].as_str(),
                r.form["amount"].as_str(),
                r.form["currency"].as_str(),
            )
        })
        .collect();
    assert_eq!(
        items,
        vec![("in_stub1", "2500", "usd"), ("in_stub1", "195", "usd")]
    );

    // An error answer carries Stripe's message and not the API key; an id
    // that is not an object id never reaches a URL.
    let missing = stripe.invoice_status("in_missing").unwrap_err().to_string();
    assert!(
        missing.contains("404") && missing.contains("No such invoice"),
        "{missing}"
    );
    assert!(!missing.contains("sk_test_stub"), "{missing}");
    let unsafe_id = stripe.invoice_status("in_1/../../customers").unwrap_err();
    assert!(
        unsafe_id.to_string().contains("not a Stripe object id"),
        "{unsafe_id}"
    );

    // Anything not paid or void is still owed.
    let (base, _) = stripe_stub("uncollectible");
    let stripe = StripeProvider::new(&base, "sk_test_stub", 14).unwrap();
    assert_eq!(
        stripe.invoice_status("in_stub1").unwrap(),
        ProviderInvoiceStatus::Open
    );
    // An unreachable API is an error, not a status.
    let stripe = StripeProvider::new("http://127.0.0.1:1", "sk_test_stub", 14).unwrap();
    assert!(stripe.invoice_status("in_stub1").is_err());
}

const WEBHOOK_SECRET: &str = "whsec_test_secret";

/// A `Stripe-Signature` header for `body`, signed at `at_ms`.
fn stripe_signature(body: &[u8], secret: &str, at_ms: u64) -> String {
    use hmac::{Hmac, Mac};
    let t = at_ms / 1000;
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{t}.").as_bytes());
    mac.update(body);
    let hex: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("t={t},v1={hex}")
}

fn stripe_event(kind: &str, stripe_id: &str, ours: Option<&str>) -> Vec<u8> {
    stripe_event_for(kind, stripe_id, ours, None)
}

fn stripe_event_for(
    kind: &str,
    stripe_id: &str,
    ours: Option<&str>,
    customer: Option<&str>,
) -> Vec<u8> {
    let metadata = match ours {
        Some(id) => serde_json::json!({"kosha_invoice_id": id}),
        None => serde_json::json!({}),
    };
    serde_json::to_vec(&serde_json::json!({
        "id": "evt_1",
        "object": "event",
        "type": kind,
        "data": {"object": {
            "id": stripe_id, "object": "invoice", "customer": customer, "metadata": metadata,
        }},
    }))
    .unwrap()
}

impl Fixture {
    /// A delivery to the Stripe webhook through the pod's dispatch, with no
    /// API key.
    fn webhook(&self, body: &[u8], signature: Option<&str>) -> (u16, serde_json::Value) {
        let mut headers = HashMap::new();
        if let Some(signature) = signature {
            headers.insert("stripe-signature".to_string(), signature.to_string());
        }
        let response = dispatch_request(
            &self.state,
            http::Request {
                method: "POST".into(),
                target: billing::STRIPE_WEBHOOK_PATH.into(),
                headers,
                body: body.to_vec(),
            },
        );
        let raw = response.into_raw_string();
        (status_of(&raw), body_of(&raw))
    }
}

#[test]
fn stripe_signatures_are_checked() {
    let body = br#"{"type":"invoice.paid"}"#;
    let now = 1_790_000_000_000;
    let good = stripe_signature(body, WEBHOOK_SECRET, now);
    assert_eq!(
        billing::verify_stripe_signature(&good, body, WEBHOOK_SECRET, now + 60_000),
        Ok(())
    );
    // A rotated secret: Stripe sends one v1 per active secret.
    let other = stripe_signature(body, "whsec_old", now);
    let both = format!("{good},{}", other.split_once(',').unwrap().1);
    assert!(billing::verify_stripe_signature(&both, body, WEBHOOK_SECRET, now).is_ok());
    let v1 = good.split_once(',').unwrap().1;
    let flood = format!("{good},{}", [v1; 4].join(","));
    assert!(billing::verify_stripe_signature(&flood, body, WEBHOOK_SECRET, now).is_err());
    for (header, at) in [
        (good.as_str(), now + 6 * 60_000),
        (other.as_str(), now),
        ("v1=00", now),
        ("t=1790000000", now),
        ("", now),
    ] {
        assert!(
            billing::verify_stripe_signature(header, body, WEBHOOK_SECRET, at).is_err(),
            "{header}"
        );
    }
    assert!(billing::verify_stripe_signature(
        &good,
        br#"{"type":"invoice.voided"}"#,
        WEBHOOK_SECRET,
        now
    )
    .is_err());
}

#[test]
fn a_stripe_webhook_marks_an_invoice_paid_and_lifts_the_restriction() {
    let oct = oct();
    let f = fixture_with(CATALOG, true, oct.start_ms(), |b| {
        b.stripe_webhook_secret = Some(WEBHOOK_SECRET.into());
    });
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "payg"}),
    );
    f.record(
        "acme",
        "q",
        oct.start_ms() + 1,
        &[(meter::QUERIES, 2_000_000)],
    );
    f.now.store(oct.end_ms() + HOUR_MS, Ordering::SeqCst);
    f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    let stripe_id = f.provider.submitted()[0].provider_invoice_id.clone();

    // Unpaid well past its due date: suspended.
    f.now.store(oct.end_ms() + 40 * DAY_MS, Ordering::SeqCst);
    f.refresh();
    let standing = body_of(&f.call("acme", "GET /v1/billing", serde_json::Value::Null));
    assert_eq!(standing["state"], "suspended", "{standing}");

    let now = f.now.load(Ordering::SeqCst);
    // Unsigned, badly signed or stale deliveries change nothing.
    let paid = stripe_event("invoice.paid", &stripe_id, Some("acme/2026-10"));
    assert_eq!(f.webhook(&paid, None).0, 400);
    let forged = stripe_signature(&paid, "whsec_wrong", now);
    assert_eq!(f.webhook(&paid, Some(&forged)).0, 400);
    let stale = stripe_signature(&paid, WEBHOOK_SECRET, now - 10 * 60_000);
    assert_eq!(f.webhook(&paid, Some(&stale)).0, 400);
    assert_eq!(
        f.store.invoice("acme/2026-10").unwrap().unwrap().status,
        InvoiceStatus::Open
    );

    // Events for other objects, or invoices that are not ours, are
    // acknowledged and ignored.
    let other = stripe_event("customer.created", "cus_1", None);
    let (status, body) = f.webhook(&other, Some(&stripe_signature(&other, WEBHOOK_SECRET, now)));
    assert_eq!(
        (status, body["invoice"].clone()),
        (200, serde_json::Value::Null)
    );
    let foreign = stripe_event("invoice.paid", "in_someone_else", None);
    let (status, body) = f.webhook(
        &foreign,
        Some(&stripe_signature(&foreign, WEBHOOK_SECRET, now)),
    );
    assert_eq!(
        (status, body["invoice"].clone()),
        (200, serde_json::Value::Null)
    );
    let mismatched = stripe_event("invoice.paid", "in_someone_else", Some("acme/2026-10"));
    let (status, body) = f.webhook(
        &mismatched,
        Some(&stripe_signature(&mismatched, WEBHOOK_SECRET, now)),
    );
    assert_eq!(
        (status, body["invoice"].clone()),
        (200, serde_json::Value::Null)
    );

    // The real one: paid at once, and the standing is refreshed with it.
    let (status, body) = f.webhook(&paid, Some(&stripe_signature(&paid, WEBHOOK_SECRET, now)));
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["invoice"], "acme/2026-10");
    assert_eq!(body["status"], "paid");
    let invoice = f.store.invoice("acme/2026-10").unwrap().unwrap();
    assert_eq!(invoice.status, InvoiceStatus::Paid);
    assert_eq!(invoice.paid_at_ms, Some(now));
    let standing = body_of(&f.call("acme", "GET /v1/billing", serde_json::Value::Null));
    assert_eq!(standing["state"], "active", "{standing}");

    // A redelivery is harmless.
    let (status, body) = f.webhook(&paid, Some(&stripe_signature(&paid, WEBHOOK_SECRET, now)));
    assert_eq!(
        (status, body["invoice"].clone()),
        (200, serde_json::Value::Null)
    );
}

#[test]
fn the_stripe_webhook_is_off_without_a_secret() {
    let f = fixture(CATALOG, true, oct().start_ms());
    let body = stripe_event("invoice.paid", "in_1", None);
    let sig = stripe_signature(&body, WEBHOOK_SECRET, oct().start_ms());
    assert_eq!(f.webhook(&body, Some(&sig)).0, 404);
    assert!(billing::is_stripe_webhook(
        "POST /webhooks/stripe HTTP/1.1\r\n"
    ));
    assert!(!billing::is_stripe_webhook(
        "GET /webhooks/stripe HTTP/1.1\r\n"
    ));
    assert!(!billing::is_stripe_webhook(
        "POST /webhooks/stripe/x HTTP/1.1\r\n"
    ));
}

#[test]
fn the_scheduler_closes_last_month_once_and_syncs_payments() {
    let oct = oct();
    let f = fixture_with(CATALOG, true, oct.start_ms(), |b| {
        b.schedule = Some(billing::Schedule {
            interval: std::time::Duration::from_secs(60),
            close_delay: std::time::Duration::from_secs(2 * 3600),
        });
    });
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "payg"}),
    );
    f.record(
        "acme",
        "q",
        oct.start_ms() + 1,
        &[(meter::QUERIES, 2_000_000)],
    );
    let flushes = AtomicU64::new(0);
    let flush = || {
        flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };

    // During October, and in the first two hours of November, September
    // (empty) and then October are not closed early.
    f.now.store(oct.start_ms() + 10 * DAY_MS, Ordering::SeqCst);
    let run = f.billing().run_scheduled(flush);
    assert!(
        run.closed.is_some(),
        "September closes once October is 2h old"
    );
    assert!(run.closed.unwrap().created.is_empty());
    f.now.store(oct.end_ms() + HOUR_MS, Ordering::SeqCst);
    let run = f.billing().run_scheduled(flush);
    assert!(run.closed.is_none());
    assert!(f.store.invoices(None).unwrap().is_empty());

    // Past the delay, but this pod's usage cannot be delivered: October
    // waits rather than being invoiced without it.
    f.now.store(oct.end_ms() + 3 * HOUR_MS, Ordering::SeqCst);
    let run = f.billing().run_scheduled(|| Err("spool unwritable".into()));
    assert!(run.closed.is_none());
    assert!(f.store.invoices(None).unwrap().is_empty());

    // Past the delay: October is closed and submitted, once.
    let run = f.billing().run_scheduled(flush);
    let closed = run.closed.expect("closes October");
    assert_eq!(closed.created, vec!["acme/2026-10".to_string()]);
    assert_eq!(closed.submitted, 1);
    assert_eq!(
        flushes.load(Ordering::SeqCst),
        2,
        "usage flushed before each close"
    );
    f.advance(HOUR_MS);
    let run = f.billing().run_scheduled(flush);
    assert!(run.closed.is_none());
    assert_eq!(f.provider.submitted().len(), 1);
    assert_eq!(run.synced.unwrap().checked, 1);

    // The provider collects; the next run records it.
    assert!(f
        .provider
        .pay(&f.provider.submitted()[0].provider_invoice_id));
    f.advance(HOUR_MS);
    let run = f.billing().run_scheduled(flush);
    assert_eq!(run.synced.unwrap().paid, vec!["acme/2026-10".to_string()]);
    assert_eq!(
        f.store.invoice("acme/2026-10").unwrap().unwrap().status,
        InvoiceStatus::Paid
    );
}

#[test]
fn the_scheduler_retries_a_close_that_failed() {
    let oct = oct();
    let f = fixture_with(CATALOG, true, oct.start_ms(), |b| {
        b.schedule = Some(billing::Schedule {
            interval: std::time::Duration::from_secs(60),
            close_delay: std::time::Duration::from_secs(3600),
        });
    });
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "payg"}),
    );
    f.now.store(oct.end_ms() + 2 * HOUR_MS, Ordering::SeqCst);
    f.provider.set_failing(true);
    let run = f.billing().run_scheduled(|| Ok(()));
    assert_eq!(run.closed.unwrap().errors.len(), 1);
    f.provider.set_failing(false);
    let run = f.billing().run_scheduled(|| Ok(()));
    let closed = run.closed.expect("closed again after the failure");
    assert!(closed.errors.is_empty(), "{:?}", closed.errors);
    assert_eq!(f.provider.submitted().len(), 1);
}

#[test]
fn billing_off_or_unscheduled_runs_nothing() {
    let f = fixture(CATALOG, true, oct().end_ms() + 2 * DAY_MS);
    let run = f
        .billing()
        .run_scheduled(|| panic!("no flush when unscheduled"));
    assert!(run.closed.is_none() && run.synced.is_none());
}

/// The issue's "done when" against Stripe itself, in test mode: a month of
/// synthetic usage is closed by the scheduler, the invoice Stripe holds
/// totals what Kosha rated, and once Stripe records the payment the next
/// run copies it back. Opt in with a test-mode key:
/// `STRIPE_TEST_SECRET_KEY=sk_test_... cargo test -p kosha-server
/// stripe_test_mode -- --ignored`.
#[test]
#[ignore = "calls Stripe; needs STRIPE_TEST_SECRET_KEY (sk_test_...)"]
fn stripe_test_mode_charges_a_month_of_synthetic_usage() {
    let key = std::env::var("STRIPE_TEST_SECRET_KEY").expect("STRIPE_TEST_SECRET_KEY");
    assert!(
        key.starts_with("sk_test_"),
        "only a test-mode key may be used"
    );
    let base = "https://api.stripe.com";
    let oct = oct();
    let tenant = format!("kosha-e2e-{}", unix_millis());
    let usage = Arc::new(MemoryUsageStore::new());
    let store = Arc::new(MemoryBillingStore::new());
    let now = Arc::new(AtomicU64::new(oct.start_ms()));
    let clock: billing::Clock = {
        let now = now.clone();
        Arc::new(move || now.load(Ordering::SeqCst))
    };
    let mut billing = Billing::new(
        Catalog::parse(CATALOG).unwrap(),
        store.clone(),
        usage.clone(),
        Some(Arc::new(StripeProvider::new(base, &key, 30).unwrap())),
        clock,
    );
    billing.schedule = Some(billing::Schedule {
        interval: std::time::Duration::from_secs(60),
        close_delay: std::time::Duration::from_secs(3600),
    });
    store
        .put_account(&BillingAccount {
            tenant: tenant.clone(),
            plan: "payg".into(),
            provider_customer_id: None,
            created_at_ms: oct.start_ms(),
        })
        .unwrap();
    let mut events = Vec::new();
    for hour in 0..oct.hours() {
        let at = oct.start_ms() + hour * HOUR_MS;
        let mut meters = BTreeMap::from([
            (meter::STORED_BYTES.to_string(), 3 * GIB),
            (meter::QUERIES.to_string(), 2_000),
        ]);
        if hour % 24 == 0 {
            meters.insert(meter::BYTES_WRITTEN.to_string(), GIB);
        }
        events.push(UsageEvent {
            id: format!("{tenant}/{hour}"),
            tenant: tenant.clone(),
            namespace: "docs".into(),
            at_ms: at,
            meters,
        });
    }
    usage.record(&events).unwrap();

    now.store(oct.end_ms() + 2 * HOUR_MS, Ordering::SeqCst);
    let closed = billing
        .run_scheduled(|| Ok(()))
        .closed
        .expect("closes October");
    assert!(closed.errors.is_empty(), "{:?}", closed.errors);
    assert_eq!(closed.submitted, 1);
    const EXPECTED_CENTS: u64 = 2500 + 195 + 6200 + 66;
    let invoice = store
        .invoice(&format!("{tenant}/2026-10"))
        .unwrap()
        .unwrap();
    assert_eq!(invoice.total_cents, EXPECTED_CENTS);
    let stripe_id = invoice.provider_invoice_id.clone().expect("sent to Stripe");

    let client = reqwest::blocking::Client::new();
    let at_stripe: serde_json::Value = client
        .get(format!("{base}/v1/invoices/{stripe_id}"))
        .bearer_auth(&key)
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(at_stripe["status"], "open", "{at_stripe}");
    assert_eq!(at_stripe["currency"], "usd");
    assert_eq!(at_stripe["amount_due"], EXPECTED_CENTS, "{at_stripe}");
    assert_eq!(
        at_stripe["metadata"]["kosha_invoice_id"],
        format!("{tenant}/2026-10")
    );

    let paid: serde_json::Value = client
        .post(format!("{base}/v1/invoices/{stripe_id}/pay"))
        .bearer_auth(&key)
        .header("content-type", "application/x-www-form-urlencoded")
        .body("paid_out_of_band=true")
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(paid["status"], "paid", "{paid}");
    now.fetch_add(HOUR_MS, Ordering::SeqCst);
    let synced = billing.run_scheduled(|| Ok(())).synced.expect("synced");
    assert_eq!(synced.paid, vec![format!("{tenant}/2026-10")]);
}

#[test]
fn a_retry_hands_over_a_default_plan_invoice_the_provider_refused() {
    let catalog = CATALOG.replace(r#""default_plan": "free""#, r#""default_plan": "payg""#);
    let oct = oct();
    let f = fixture(&catalog, true, oct.end_ms() + 2 * HOUR_MS);
    f.record("solo", "q", oct.start_ms() + 1, &[(meter::QUERIES, 10)]);
    f.provider.set_failing(true);
    let first = f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    assert_eq!(first["created"], serde_json::json!(["solo/2026-10"]));
    assert_eq!(first["submitted"], 0);
    f.provider.set_failing(false);
    f.advance(HOUR_MS);
    let retry = f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    assert_eq!(retry["existing"], 1, "{retry}");
    assert_eq!(retry["submitted"], 1, "{retry}");
    assert_eq!(f.provider.submitted().len(), 1);
}

#[test]
fn a_payment_reported_before_the_provider_id_is_stored_is_recorded() {
    let oct = oct();
    let f = fixture_with(CATALOG, true, oct.start_ms(), |b| {
        b.stripe_webhook_secret = Some(WEBHOOK_SECRET.into());
    });
    f.admin_ok(
        "POST /v1/admin/billing/accounts",
        serde_json::json!({"tenant": "acme", "plan": "payg"}),
    );
    f.store
        .set_provider_customer("acme", "payg", "cus_acme", oct.start_ms())
        .unwrap();
    f.now.store(oct.end_ms() + HOUR_MS, Ordering::SeqCst);
    f.provider.set_failing(true);
    f.admin_ok(
        "POST /v1/admin/billing/close-period",
        serde_json::Value::Null,
    );
    assert_eq!(
        f.store
            .invoice("acme/2026-10")
            .unwrap()
            .unwrap()
            .provider_invoice_id,
        None
    );
    let now = f.now.load(Ordering::SeqCst);
    let customer = f
        .store
        .account("acme")
        .unwrap()
        .unwrap()
        .provider_customer_id
        .unwrap();
    // Without our id, or from another customer (another deployment on the
    // same Stripe account), the event is not matched; with both, it is.
    for (ours, cus) in [
        (None, Some(customer.as_str())),
        (Some("acme/2026-10"), Some("cus_other_deployment")),
        (Some("acme/2026-10"), None),
    ] {
        let event = stripe_event_for("invoice.paid", "in_early", ours, cus);
        let (_, body) = f.webhook(&event, Some(&stripe_signature(&event, WEBHOOK_SECRET, now)));
        assert_eq!(body["invoice"], serde_json::Value::Null, "{ours:?} {cus:?}");
    }
    let paid = stripe_event_for(
        "invoice.paid",
        "in_early",
        Some("acme/2026-10"),
        Some(&customer),
    );
    let (status, body) = f.webhook(&paid, Some(&stripe_signature(&paid, WEBHOOK_SECRET, now)));
    assert_eq!(
        (status, body["status"].clone()),
        (200, "paid".into()),
        "{body}"
    );
    let invoice = f.store.invoice("acme/2026-10").unwrap().unwrap();
    assert_eq!(invoice.status, InvoiceStatus::Paid);
    assert_eq!(invoice.provider_invoice_id.as_deref(), Some("in_early"));
}
