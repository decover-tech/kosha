//! #349 "done when", end to end against a fake Stripe: a test customer
//! upgrades through checkout, is charged for a month of synthetic usage,
//! fails to pay, is dunned and restricted, pays, and is restored; every
//! payment result reaches Kosha through signed webhook deliveries to the
//! real `dispatch`, not through polling.
//!
//! The fake is a local HTTP server that keeps Stripe's objects (customers,
//! invoices with their items, checkout sessions, setup intents) and, like
//! Stripe, queues an event for each state change. The test delivers the
//! queue to Kosha's webhook endpoint, signed with the endpoint secret, when
//! it chooses, so the order of events is deterministic.

use super::*;
use billing::{Billing, StripeProvider};
use billing_scheduler::SchedulerConfig;
use kosha_control::billing::{BillingPeriod, Catalog, InvoiceStatus};
use kosha_control::billing_ledger::{BillingLedger, MemoryBillingLedger};
use kosha_control::dunning::{Notice, NoticeKind, Notifier};
use kosha_control::stripe_webhook::sign;
use kosha_control::usage::{meter, MemoryUsageStore, UsageEvent, UsageStore, HOUR_MS};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use usage::{UsageConfig, UsageMeter};

const DAY_MS: u64 = 24 * HOUR_MS;
const GIB: u64 = 1 << 30;
const ADMIN_KEY: &str = "admin-secret";
const WEBHOOK_SECRET: &str = "whsec_test_endpoint";

const CATALOG: &str = r#"{
  "default_plan": "free",
  "upgrade_plan": "payg",
  "plans": {
    "free": {"kind": "free", "meters": {"queries": {"included": 1000}}},
    "payg": {"kind": "usage", "base_fee_cents": 2500,
             "meters": {
               "queries": {"included": 1000000, "unit": 1000000, "unit_price_cents": 400},
               "bytes_written": {"unit": 1073741824, "unit_price_cents": 200},
               "stored_bytes": {"included": 1073741824, "unit": 1073741824,
                                "unit_price_cents": 33}}}
  }
}"#;

// ─── The fake Stripe ────────────────────────────────────────────────────────

#[derive(Default)]
struct FakeInvoice {
    customer: String,
    status: String,
    collection_method: String,
    metadata: BTreeMap<String, String>,
    items: Vec<u64>,
}

#[derive(Default)]
struct Stripe {
    /// Idempotency key -> the object id the first request created.
    keys: BTreeMap<String, String>,
    /// Customer -> default payment method.
    customers: BTreeMap<String, Option<String>>,
    invoices: BTreeMap<String, FakeInvoice>,
    /// Checkout session -> (customer, metadata).
    sessions: BTreeMap<String, (String, BTreeMap<String, String>)>,
    /// Cards that are declined.
    declining: bool,
    events: Vec<serde_json::Value>,
    next: u64,
    requests: Vec<String>,
}

struct FakeStripe {
    base: String,
    state: Arc<Mutex<Stripe>>,
    clock: Arc<AtomicU64>,
}

fn form_of(body: &[u8]) -> BTreeMap<String, String> {
    String::from_utf8_lossy(body)
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(k, v)| (url_decode(k), url_decode(v)))
        .collect()
}

fn metadata_of(form: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    form.iter()
        .filter_map(|(k, v)| {
            k.strip_prefix("metadata[")
                .and_then(|k| k.strip_suffix(']'))
                .map(|k| (k.to_string(), v.clone()))
        })
        .collect()
}

impl Stripe {
    fn id(&mut self, prefix: &str) -> String {
        self.next += 1;
        format!("{prefix}_{}", self.next)
    }

    fn invoice_json(&self, id: &str) -> serde_json::Value {
        let invoice = &self.invoices[id];
        serde_json::json!({
            "id": id, "object": "invoice", "customer": invoice.customer,
            "status": invoice.status, "collection_method": invoice.collection_method,
            "metadata": invoice.metadata,
            "amount_due": invoice.items.iter().sum::<u64>(),
            "status_transitions": {"paid_at": null},
        })
    }

    fn event(&mut self, event_type: &str, object: serde_json::Value, previous: serde_json::Value) {
        let id = self.id("evt");
        let mut data = serde_json::json!({"object": object});
        if !previous.is_null() {
            data["previous_attributes"] = previous;
        }
        self.events.push(
            serde_json::json!({"id": id, "type": event_type, "livemode": false, "data": data}),
        );
    }

    /// Try to collect `id` from its customer's default payment method.
    fn charge(&mut self, id: &str) -> bool {
        let customer = self.invoices[id].customer.clone();
        let has_card = self.customers.get(&customer).cloned().flatten().is_some();
        if has_card && !self.declining {
            self.invoices.get_mut(id).unwrap().status = "paid".into();
            let invoice = self.invoice_json(id);
            self.event("invoice.paid", invoice, serde_json::Value::Null);
            true
        } else {
            let mut invoice = self.invoice_json(id);
            invoice["last_payment_error"] =
                serde_json::json!({"message": "Your card was declined."});
            self.event("invoice.payment_failed", invoice, serde_json::Value::Null);
            false
        }
    }

    fn handle(
        &mut self,
        method: &str,
        path: &str,
        key: Option<&str>,
        form: &BTreeMap<String, String>,
    ) -> (u16, serde_json::Value) {
        self.requests.push(format!("{method} {path}"));
        if let Some(existing) = key.and_then(|k| self.keys.get(k)).cloned() {
            if path == "/v1/customers" {
                return (200, serde_json::json!({"id": existing}));
            }
        }
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        match (method, parts.as_slice()) {
            ("POST", ["v1", "customers"]) => {
                let id = self.id("cus");
                self.customers.insert(id.clone(), None);
                if let Some(key) = key {
                    self.keys.insert(key.to_string(), id.clone());
                }
                (200, serde_json::json!({"id": id}))
            }
            ("GET", ["v1", "customers", id]) => match self.customers.get(*id) {
                Some(pm) => (
                    200,
                    serde_json::json!({"id": id, "invoice_settings": {"default_payment_method": pm}}),
                ),
                None => (
                    404,
                    serde_json::json!({"error": {"message": "No such customer"}}),
                ),
            },
            ("POST", ["v1", "customers", id]) => {
                let id = id.to_string();
                let pm = form
                    .get("invoice_settings[default_payment_method]")
                    .cloned();
                let before = self.customers.insert(id.clone(), pm.clone()).flatten();
                self.event(
                    "customer.updated",
                    serde_json::json!({"id": id, "invoice_settings": {"default_payment_method": pm}}),
                    serde_json::json!({"invoice_settings": {"default_payment_method": before}}),
                );
                (200, serde_json::json!({"id": id}))
            }
            ("POST", ["v1", "invoices"]) => {
                let id = self.id("in");
                self.invoices.insert(
                    id.clone(),
                    FakeInvoice {
                        customer: form["customer"].clone(),
                        status: "draft".into(),
                        collection_method: form["collection_method"].clone(),
                        metadata: metadata_of(form),
                        items: Vec::new(),
                    },
                );
                (200, self.invoice_json(&id))
            }
            ("POST", ["v1", "invoiceitems"]) => {
                let invoice = self.invoices.get_mut(&form["invoice"]).unwrap();
                invoice.items.push(form["amount"].parse().unwrap());
                let id = self.id("ii");
                (200, serde_json::json!({"id": id}))
            }
            ("POST", ["v1", "invoices", id, "finalize"]) => {
                let id = id.to_string();
                self.invoices.get_mut(&id).unwrap().status = "open".into();
                let invoice = self.invoice_json(&id);
                self.event("invoice.finalized", invoice, serde_json::Value::Null);
                if self.invoices[&id].collection_method == "charge_automatically" {
                    self.charge(&id);
                }
                (200, self.invoice_json(&id))
            }
            ("POST", ["v1", "invoices", id, "pay"]) => {
                let id = id.to_string();
                if self.charge(&id) {
                    (200, self.invoice_json(&id))
                } else {
                    (
                        402,
                        serde_json::json!({"error": {"message": "Your card was declined."}}),
                    )
                }
            }
            ("GET", ["v1", "invoices", id]) => (200, self.invoice_json(id)),
            ("POST", ["v1", "checkout", "sessions"]) => {
                assert_eq!(form["mode"], "setup");
                let id = self.id("cs");
                self.sessions
                    .insert(id.clone(), (form["customer"].clone(), metadata_of(form)));
                (
                    200,
                    serde_json::json!({"id": id, "url": format!("https://checkout.stripe.test/{id}")}),
                )
            }
            ("POST", ["v1", "billing_portal", "sessions"]) => {
                let id = self.id("bps");
                (
                    200,
                    serde_json::json!({"id": id, "url": format!("https://billing.stripe.test/{id}")}),
                )
            }
            ("GET", ["v1", "setup_intents", id]) => (
                200,
                serde_json::json!({"id": id, "payment_method": format!("pm_for_{id}")}),
            ),
            _ => (
                404,
                serde_json::json!({"error": {"message": "unknown route"}}),
            ),
        }
    }
}

impl FakeStripe {
    fn start(clock: Arc<AtomicU64>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(Stripe::default()));
        let shared = state.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                if reader.read_line(&mut line).is_err() {
                    continue;
                }
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
                assert_eq!(
                    headers.get("authorization").map(String::as_str),
                    Some("Bearer sk_test_fake")
                );
                let length: usize = headers
                    .get("content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let (status, answer) = shared.lock().unwrap().handle(
                    &method,
                    &path,
                    headers.get("idempotency-key").map(String::as_str),
                    &form_of(&body),
                );
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
        Self { base, state, clock }
    }

    fn stripe(&self) -> std::sync::MutexGuard<'_, Stripe> {
        self.state.lock().unwrap()
    }

    /// The events not delivered yet, as Stripe would send them: each
    /// stamped and signed at the clock's current time.
    fn take_events(&self) -> Vec<(Vec<u8>, String)> {
        let created = self.clock.load(Ordering::SeqCst) / 1_000;
        std::mem::take(&mut self.stripe().events)
            .into_iter()
            .map(|mut event| {
                event["created"] = created.into();
                let body = event.to_string().into_bytes();
                let signature = sign(WEBHOOK_SECRET, created, &body);
                (body, signature)
            })
            .collect()
    }
}

// ─── Kosha under test ───────────────────────────────────────────────────────

#[derive(Default)]
struct Recorder(Mutex<Vec<Notice>>);

impl Notifier for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }
    fn notify(&self, notice: &Notice) -> Result<(), KoshaError> {
        self.0.lock().unwrap().push(notice.clone());
        Ok(())
    }
}

fn status_of(response: &str) -> u16 {
    response
        .get(9..12)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

fn body_of(response: &str) -> serde_json::Value {
    let body = response.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    serde_json::from_str(body).unwrap_or(serde_json::Value::Null)
}

fn http(addr: SocketAddr, line: &str, headers: &[(&str, &str)], body: &[u8]) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let extra: String = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();
    stream
        .write_all(
            format!(
                "{line} HTTP/1.1\r\nhost: x\r\n{extra}content-length: {}\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .unwrap();
    stream.write_all(body).unwrap();
    http::read_response_string(&mut stream).unwrap()
}

fn json(value: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&value).unwrap()
}

struct Kosha {
    addr: SocketAddr,
    state: Arc<AppState>,
    usage: Arc<MemoryUsageStore>,
    ledger: Arc<MemoryBillingLedger>,
    notices: Arc<Recorder>,
    clock: Arc<AtomicU64>,
}

impl Kosha {
    fn billing(&self) -> &Billing {
        self.state.billing.as_deref().unwrap()
    }

    fn deliver(&self, stripe: &FakeStripe) -> Vec<serde_json::Value> {
        stripe
            .take_events()
            .into_iter()
            .map(|(body, signature)| {
                let response = http(
                    self.addr,
                    "POST /v1/billing/webhooks/stripe",
                    &[("stripe-signature", &signature)],
                    &body,
                );
                assert_eq!(status_of(&response), 200, "{response}");
                body_of(&response)
            })
            .collect()
    }

    fn tick(&self) -> billing_scheduler::TickReport {
        let config = SchedulerConfig {
            interval: Duration::from_secs(60),
            close_delay_ms: 2 * HOUR_MS,
            sync_interval_ms: 1_000 * DAY_MS,
            holder: "test-pod".into(),
        };
        billing_scheduler::tick(&self.state, self.billing(), &config)
    }

    fn set_clock(&self, ms: u64) {
        self.clock.store(ms, Ordering::SeqCst);
    }

    fn write(&self) -> String {
        http(
            self.addr,
            "POST /v1/namespaces/docs/documents",
            &[],
            &json(serde_json::json!({"documents": [
                {"id": "d1", "fields": [{"name": "title", "field_type": "Text", "value": "quick fox"}]}
            ]})),
        )
    }

    fn search(&self) -> String {
        http(
            self.addr,
            "POST /v1/namespaces/docs/search",
            &[],
            &json(serde_json::json!({"query_text": "fox", "no_cache": true})),
        )
    }
}

fn kosha(stripe_base: &str, clock: Arc<AtomicU64>) -> Kosha {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kosha-test-stripe-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        unix_millis()
    ));
    let usage = Arc::new(MemoryUsageStore::new());
    let ledger = Arc::new(MemoryBillingLedger::new());
    let notices = Arc::new(Recorder::default());
    let billing_clock = {
        let clock = clock.clone();
        Arc::new(move || clock.load(Ordering::SeqCst))
    };
    let billing = Billing::new(
        Catalog::parse(CATALOG).unwrap(),
        Arc::new(kosha_control::billing::MemoryBillingStore::new()),
        usage.clone(),
        Some(Arc::new(
            StripeProvider::new(stripe_base, "sk_test_fake", 14).unwrap(),
        )),
        billing_clock,
    )
    .with_ledger(ledger.clone())
    .with_notifier(notices.clone())
    .with_webhook_secrets(vec!["whsec_old_rolled".into(), WEBHOOK_SECRET.into()], 300)
    .with_return_url("https://console.example/billing");
    let mut state = AppState::with_options(
        dir.clone(),
        StateOptions {
            usage: Some(UsageMeter::new(usage.clone(), UsageConfig::under(&dir))),
            billing: Some(billing),
            ..Default::default()
        },
    );
    state.admin_auth_key = Some(ADMIN_KEY.into());
    let state = Arc::new(state);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let state = state.clone();
        std::thread::spawn(move || serve(listener, state, Duration::from_secs(30)));
    }
    Kosha {
        addr,
        state,
        usage,
        ledger,
        notices,
        clock,
    }
}

/// October 2030: 3 GiB stored every hour, 2,000 searches an hour, 1 GiB
/// written a day.
fn synthetic_month(tenant: &str, period: BillingPeriod) -> Vec<UsageEvent> {
    let mut events = Vec::new();
    for hour in 0..period.hours() {
        let at = period.start_ms() + hour * HOUR_MS;
        let mut add = |id: String, at: u64, meters: &[(&str, u64)]| {
            events.push(UsageEvent {
                id,
                tenant: tenant.into(),
                namespace: "docs".into(),
                at_ms: at,
                meters: meters.iter().map(|(m, q)| (m.to_string(), *q)).collect(),
            })
        };
        add(
            format!("stored/{tenant}/docs/{}", at / HOUR_MS),
            at,
            &[(meter::STORED_BYTES, 3 * GIB)],
        );
        add(format!("q/{hour}"), at + 60_000, &[(meter::QUERIES, 2_000)]);
        if hour % 24 == 0 {
            add(
                format!("w/{hour}"),
                at + 120_000,
                &[(meter::BYTES_WRITTEN, GIB), (meter::DOCUMENTS_WRITTEN, 10)],
            );
        }
    }
    events
}

/// The issue's "done when", through the webhook path. Auth is off in
/// tests, so the tenant is `dev`.
#[test]
fn a_customer_is_charged_for_a_month_through_webhooks_end_to_end() {
    let oct = BillingPeriod::parse("2030-10").unwrap();
    let clock = Arc::new(AtomicU64::new(oct.start_ms() - 6 * DAY_MS));
    let stripe = FakeStripe::start(clock.clone());
    let k = kosha(&stripe.base, clock);
    assert_eq!(status_of(&k.write()), 200);
    assert_eq!(
        status_of(&http(k.addr, "POST /v1/namespaces/docs/flush", &[], b"{}")),
        200
    );

    // ── September 25: self-service upgrade through checkout ──────────────
    let checkout = http(
        k.addr,
        "POST /v1/billing/checkout",
        &[],
        &json(serde_json::json!({"success_url": "https://console.example/billing/done"})),
    );
    assert_eq!(status_of(&checkout), 200, "{checkout}");
    let session = body_of(&checkout);
    assert!(session["url"]
        .as_str()
        .unwrap()
        .starts_with("https://checkout.stripe.test/cs_"));
    // A redirect off the console's origin is refused.
    let elsewhere = http(
        k.addr,
        "POST /v1/billing/checkout",
        &[],
        &json(serde_json::json!({"success_url": "https://evil.example/x"})),
    );
    assert_eq!(status_of(&elsewhere), 400, "{elsewhere}");
    let (customer, metadata) = stripe.stripe().sessions[session["id"].as_str().unwrap()].clone();
    assert_eq!(metadata["kosha_tenant"], "dev");
    assert_eq!(metadata["kosha_plan"], "payg");
    // The user completes the hosted page: Stripe reports the session.
    stripe.stripe().event(
        "checkout.session.completed",
        serde_json::json!({"id": session["id"], "object": "checkout.session", "mode": "setup",
                           "customer": customer, "setup_intent": "seti_1",
                           "metadata": metadata}),
        serde_json::Value::Null,
    );
    let outcomes = k.deliver(&stripe);
    assert_eq!(outcomes[0]["outcome"], "plan_changed");
    // Kosha made the collected card the customer's default, and Stripe
    // reported that back too.
    assert_eq!(
        stripe.stripe().customers[&customer].as_deref(),
        Some("pm_for_seti_1")
    );
    assert_eq!(k.deliver(&stripe)[0]["outcome"], "payment_method_updated");
    let account = k.billing().store.account("dev").unwrap().unwrap();
    assert_eq!(account.plan, "payg");
    assert_eq!(
        account.provider_customer_id.as_deref(),
        Some(customer.as_str())
    );
    let changes =
        kosha_control::billing::PlanHistory::plan_changes(k.ledger.as_ref(), "dev", 0, u64::MAX)
            .unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].source, "checkout");

    // ── October: a month of synthetic usage ──────────────────────────────
    k.usage.record(&synthetic_month("dev", oct)).unwrap();

    // ── November 1, 03:00: the scheduler closes October ──────────────────
    // The card on file is declined.
    stripe.stripe().declining = true;
    let issued = oct.end_ms() + 3 * HOUR_MS;
    k.set_clock(issued);
    let first = k.tick();
    assert!(first.leader);
    let closed = first.closed.expect("October closed");
    assert_eq!(closed.created, vec!["dev/2030-10"]);
    assert_eq!(closed.submitted, 1);
    // Rated: $25 base + 488,000 queries over the 1M included ($1.95) +
    // 31 GiB written ($62) + 2 GiB stored over the 1 GiB included ($0.66).
    let invoice = k.billing().store.invoice("dev/2030-10").unwrap().unwrap();
    assert_eq!(invoice.total_cents, 2_500 + 195 + 6_200 + 66);
    let stripe_id = invoice.provider_invoice_id.clone().unwrap();
    {
        let s = stripe.stripe();
        let charged = &s.invoices[&stripe_id];
        assert_eq!(charged.items.iter().sum::<u64>(), 8_961);
        assert_eq!(charged.collection_method, "charge_automatically");
        assert_eq!(charged.metadata["kosha_invoice_id"], "dev/2030-10");
        assert_eq!(charged.customer, customer);
    }
    // The next tick does not close the month again.
    k.set_clock(issued + HOUR_MS);
    assert!(k.tick().closed.is_none());
    assert_eq!(stripe.stripe().invoices.len(), 1);

    // Stripe reports the finalization and the declined charge.
    let outcomes = k.deliver(&stripe);
    let kinds: Vec<&str> = outcomes
        .iter()
        .map(|o| o["outcome"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, vec!["finalized", "payment_failed"]);
    // A redelivery of the same events is acknowledged and not applied.
    let failed_event = {
        let s = stripe.stripe();
        assert!(s.events.is_empty());
        drop(s);
        // Re-queue the last payment_failed event as Stripe's retry.
        let mut s = stripe.stripe();
        let mut invoice = s.invoice_json(&stripe_id);
        invoice["last_payment_error"] = serde_json::json!({"message": "Your card was declined."});
        s.event("invoice.payment_failed", invoice, serde_json::Value::Null);
        s.events[0].clone()
    };
    let replay_body = {
        let mut event = failed_event.clone();
        event["created"] = (issued / 1_000).into();
        event.to_string().into_bytes()
    };
    let delivered = k.deliver(&stripe);
    assert_eq!(delivered[0]["duplicate"], false);
    let replay = http(
        k.addr,
        "POST /v1/billing/webhooks/stripe",
        &[(
            "stripe-signature",
            &sign(
                WEBHOOK_SECRET,
                k.clock.load(Ordering::SeqCst) / 1_000,
                &replay_body,
            ),
        )],
        &replay_body,
    );
    assert_eq!(body_of(&replay)["duplicate"], true, "{replay}");
    // Forged, stale and unsigned deliveries are refused.
    for signature in [
        sign("whsec_attacker", issued / 1_000, &replay_body),
        sign(WEBHOOK_SECRET, issued / 1_000 - 3_600, &replay_body),
    ] {
        let refused = http(
            k.addr,
            "POST /v1/billing/webhooks/stripe",
            &[("stripe-signature", &signature)],
            &replay_body,
        );
        assert_eq!(status_of(&refused), 400, "{refused}");
    }
    let unsigned = http(
        k.addr,
        "POST /v1/billing/webhooks/stripe",
        &[],
        &replay_body,
    );
    assert_eq!(status_of(&unsigned), 400);
    assert_eq!(
        k.billing()
            .stats
            .webhooks_bad_signature
            .load(Ordering::Relaxed),
        3
    );
    let failures = k.ledger.payment_failures().unwrap();
    assert_eq!(failures[0].failures, 2, "two distinct failure events");

    // Not due yet: the tenant is not restricted, but it is told.
    let tick = k.tick();
    assert_eq!(
        tick.dunning.unwrap().notices_sent,
        vec!["dev/2030-10/payment_failed"]
    );
    assert_eq!(status_of(&k.write()), 200);

    // Three days on, the first retry is declined (and reported).
    k.set_clock(issued + 3 * DAY_MS + HOUR_MS);
    let retry = k.tick().dunning.unwrap();
    assert_eq!(retry.retries.len(), 1);
    assert!(retry.collected.is_empty());
    k.deliver(&stripe);

    // Seven days past due: read-only.
    k.set_clock(invoice.due_at_ms + 7 * DAY_MS + HOUR_MS);
    let restricted = k.tick().dunning.unwrap();
    assert_eq!(restricted.notices_sent, vec!["dev/2030-10/read_only"]);
    k.deliver(&stripe);
    let refused = k.write();
    assert_eq!(status_of(&refused), 402, "{refused}");
    assert_eq!(body_of(&refused)["reason"], "billing_read_only");
    assert_eq!(status_of(&k.search()), 200);
    // Paying stays possible: the portal opens.
    let portal = http(k.addr, "POST /v1/billing/portal", &[], b"{}");
    assert_eq!(status_of(&portal), 200, "{portal}");

    // ── The customer pays on Stripe's hosted page ─────────────────────────
    {
        let mut s = stripe.stripe();
        s.invoices.get_mut(&stripe_id).unwrap().status = "paid".into();
        let mut paid = s.invoice_json(&stripe_id);
        paid["status_transitions"]["paid_at"] = (k.clock.load(Ordering::SeqCst) / 1_000).into();
        s.event("invoice.paid", paid, serde_json::Value::Null);
    }
    let paid = k.deliver(&stripe);
    assert_eq!(paid[0]["outcome"], "paid");
    // No polling happened since the first tick: the webhook settled it.
    let stored = k.billing().store.invoice("dev/2030-10").unwrap().unwrap();
    assert_eq!(stored.status, InvoiceStatus::Paid);
    // The standing was refreshed by the delivery: writes work at once.
    assert_eq!(status_of(&k.write()), 200);
    let tick = k.tick().dunning.unwrap();
    assert_eq!(tick.notices_sent, vec!["dev/2030-10/recovered"]);
    let kinds: Vec<NoticeKind> = k.notices.0.lock().unwrap().iter().map(|n| n.kind).collect();
    assert_eq!(
        kinds,
        vec![
            NoticeKind::PaymentFailed,
            NoticeKind::ReadOnly,
            NoticeKind::Recovered
        ]
    );
    let billing = body_of(&http(k.addr, "GET /v1/billing", &[], b""));
    assert_eq!(billing["state"], "active");
    assert_eq!(billing["payment_method_on_file"], true);
}

/// Without a secret the endpoint is off; checkout and portal need the
/// admin scope and a provider; a suspended tenant can still reach them.
#[test]
fn webhook_and_self_service_routes_are_gated() {
    let clock = Arc::new(AtomicU64::new(
        BillingPeriod::parse("2030-10").unwrap().start_ms(),
    ));
    let stripe = FakeStripe::start(clock.clone());
    let k = kosha(&stripe.base, clock);
    // The portal before any checkout: no customer yet.
    let portal = http(k.addr, "POST /v1/billing/portal", &[], b"{}");
    assert_eq!(status_of(&portal), 409, "{portal}");
    // Billing's gate never blocks paying.
    assert_eq!(
        billing::operation("POST /v1/billing/checkout HTTP/1.1\r\n"),
        kosha_control::billing::Operation::Account
    );
    // A query string or another method is not the webhook.
    assert!(billing::webhook_response(
        &k.state,
        "GET /v1/billing/webhooks/stripe HTTP/1.1\r\n",
        &HashMap::new(),
        b""
    )
    .is_none());
    // Scopes: a read-only key cannot start a checkout.
    use kosha_control::{Scope, Scopes};
    let read_only = auth::Principal {
        tenant: "acme".into(),
        scopes: Scopes::of(&[Scope::Read]),
        namespaces: None,
    };
    assert!(authz::authorize(&read_only, "POST", "/v1/billing/checkout", b"").is_err());
    assert!(authz::authorize(&read_only, "POST", "/v1/billing/webhooks/stripe", b"").is_ok());
}

/// The catalog reload swaps a valid catalog in, keeps the old one on an
/// invalid file or one that drops a plan an account is on.
#[test]
fn the_plan_catalog_reloads_only_when_valid() {
    let dir = std::env::temp_dir().join(format!(
        "kosha-test-catalog-{}-{}",
        std::process::id(),
        unix_millis()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("plans.json");
    std::fs::write(&path, CATALOG).unwrap();
    let billing = Billing::new(
        Catalog::parse(CATALOG).unwrap(),
        Arc::new(kosha_control::billing::MemoryBillingStore::new()),
        Arc::new(MemoryUsageStore::new()),
        None,
        Arc::new(unix_millis),
    )
    .with_plans_file(path.clone(), true);
    billing
        .store
        .put_account(&kosha_control::billing::BillingAccount {
            tenant: "acme".into(),
            plan: "payg".into(),
            provider_customer_id: None,
            created_at_ms: 0,
        })
        .unwrap();
    // A new price.
    std::fs::write(&path, CATALOG.replace("2500", "3000")).unwrap();
    assert_eq!(billing.reload_catalog(), Ok(true));
    assert_eq!(billing.catalog().plan("payg").unwrap().base_fee_cents, 3000);
    // Invalid JSON, and a catalog without the plan `acme` is on.
    for bad in ["{not json", r#"{"plans": {"free": {"kind": "free"}}}"#] {
        std::fs::write(&path, bad).unwrap();
        assert!(billing.reload_catalog().is_err());
        assert_eq!(billing.catalog().plan("payg").unwrap().base_fee_cents, 3000);
    }
    assert_eq!(
        billing
            .stats
            .catalog_reloads_rejected
            .load(Ordering::Relaxed),
        2
    );
    let _ = std::fs::remove_dir_all(&dir);
}
