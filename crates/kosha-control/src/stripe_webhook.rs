//! Stripe webhooks (#349): verifying a delivery and reading the events
//! billing acts on. Pure: no I/O. The server's route calls [`verify`] on
//! the raw body, then [`parse_event`], then hands the [`WebhookAction`] to
//! `billing_ledger::apply_webhook`.
//!
//! # Signatures
//!
//! Stripe signs each delivery with the endpoint's signing secret
//! (`whsec_...`) and sends `Stripe-Signature: t=<unix seconds>,v1=<hex>`.
//! The signed payload is `"{t}.{raw body}"` and `v1` is its HMAC-SHA256
//! under the secret, as bytes of the whole secret string. A delivery is
//! accepted when:
//!
//! - `t` is within the tolerance of this pod's clock (default 300 s, as
//!   Stripe's own libraries use), in either direction, so a captured
//!   delivery cannot be replayed later; and
//! - one of its `v1` signatures matches one of the configured secrets.
//!   While an endpoint secret is being rolled Stripe signs with both the
//!   old and the new secret and sends two `v1` values, and the operator
//!   can configure both secrets on the pods, so either order of rollout
//!   works. `v0` (test-mode legacy) and unknown schemes are ignored.
//!
//! Comparison is constant-time in the signature bytes. The body is used
//! exactly as received: re-serializing the JSON would change the bytes.
//!
//! Replays inside the tolerance are handled by idempotency, not here:
//! every event id is recorded once processed and a repeat is acknowledged
//! without acting on it again.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// The default timestamp tolerance, seconds.
pub const DEFAULT_TOLERANCE_SECS: u64 = 300;

/// HMAC-SHA256 (RFC 2104) of `message` under `key`.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner);
    outer.finalize().into()
}

/// Lower-case hex of `bytes`.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `a == b` in time that depends only on the lengths.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The `Stripe-Signature` header value for `body` signed at `timestamp`
/// with `secret`: what Stripe sends. For tests and fakes.
pub fn sign(secret: &str, timestamp: u64, body: &[u8]) -> String {
    format!("t={timestamp},v1={}", v1_signature(secret, timestamp, body))
}

fn v1_signature(secret: &str, timestamp: u64, body: &[u8]) -> String {
    let mut payload = timestamp.to_string().into_bytes();
    payload.push(b'.');
    payload.extend_from_slice(body);
    hex(&hmac_sha256(secret.as_bytes(), &payload))
}

/// Why a delivery was refused. The text is safe to log and to answer
/// with: it never carries a secret or an expected signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// No `Stripe-Signature` header, or one without `t=` or any `v1=`.
    Malformed,
    /// `t` is further from now than the tolerance.
    Stale,
    /// No `v1` matches any configured secret.
    Mismatch,
    /// No signing secret is configured.
    NoSecret,
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            VerifyError::Malformed => "missing or malformed Stripe-Signature header",
            VerifyError::Stale => "Stripe-Signature timestamp outside the tolerance",
            VerifyError::Mismatch => "no Stripe-Signature v1 signature matches",
            VerifyError::NoSecret => "no webhook signing secret is configured",
        })
    }
}

/// Check a delivery: `header` is the `Stripe-Signature` value, `body` the
/// raw request body, `secrets` the accepted signing secrets, `now_secs`
/// this pod's clock. `Ok` carries the signed timestamp.
pub fn verify(
    header: Option<&str>,
    body: &[u8],
    secrets: &[String],
    now_secs: u64,
    tolerance_secs: u64,
) -> Result<u64, VerifyError> {
    if secrets.is_empty() {
        return Err(VerifyError::NoSecret);
    }
    let header = header.ok_or(VerifyError::Malformed)?;
    let mut timestamp = None;
    let mut signatures = Vec::new();
    for item in header.split(',') {
        let Some((scheme, value)) = item.trim().split_once('=') else {
            continue;
        };
        match scheme {
            "t" => timestamp = value.parse::<u64>().ok(),
            "v1" => signatures.push(value.to_ascii_lowercase()),
            _ => {}
        }
    }
    let timestamp = timestamp.ok_or(VerifyError::Malformed)?;
    if signatures.is_empty() {
        return Err(VerifyError::Malformed);
    }
    if now_secs.abs_diff(timestamp) > tolerance_secs {
        return Err(VerifyError::Stale);
    }
    // Every pair is compared, so the time taken does not say which secret
    // or which signature matched.
    let mut matched = false;
    for secret in secrets {
        let expected = v1_signature(secret, timestamp, body);
        for signature in &signatures {
            matched |= constant_time_eq(expected.as_bytes(), signature.as_bytes());
        }
    }
    if matched {
        Ok(timestamp)
    } else {
        Err(VerifyError::Mismatch)
    }
}

/// An event as Stripe delivers it, reduced to what billing reads.
#[derive(Debug, Clone, PartialEq)]
pub struct StripeEvent {
    /// `evt_...`: the idempotency key of the delivery.
    pub id: String,
    pub event_type: String,
    /// Unix seconds.
    pub created: u64,
    pub livemode: bool,
    /// `data.object`.
    pub object: Value,
    /// `data.previous_attributes`, when the event has them.
    pub previous: Value,
}

/// Parse a verified body. `Err` for a body that is not a Stripe event.
pub fn parse_event(body: &[u8]) -> Result<StripeEvent, String> {
    let value: Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid event JSON: {e}"))?;
    let id = value["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or("event has no id")?
        .to_string();
    let event_type = value["type"]
        .as_str()
        .ok_or("event has no type")?
        .to_string();
    if !value["data"]["object"].is_object() {
        return Err("event has no data.object".into());
    }
    Ok(StripeEvent {
        id,
        event_type,
        created: value["created"].as_u64().unwrap_or(0),
        livemode: value["livemode"].as_bool().unwrap_or(false),
        object: value["data"]["object"].clone(),
        previous: value["data"]["previous_attributes"].clone(),
    })
}

/// A Stripe invoice as an event names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvoiceRef {
    /// `in_...`.
    pub provider_invoice_id: String,
    /// Our invoice id (`<tenant>/<YYYY-MM>`), from the invoice's
    /// `metadata.kosha_invoice_id`; `None` for an invoice Kosha did not
    /// create.
    pub kosha_invoice_id: Option<String>,
    pub customer: Option<String>,
}

/// What billing does about an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookAction {
    /// `invoice.paid`: record the payment.
    InvoicePaid {
        invoice: InvoiceRef,
        paid_at_ms: u64,
    },
    /// `invoice.payment_failed`: a collection attempt failed.
    InvoicePaymentFailed {
        invoice: InvoiceRef,
        attempt_count: u64,
        /// Why, as the provider put it (`last_finalization_error` or the
        /// charge's failure message), when it said.
        reason: Option<String>,
    },
    /// `invoice.finalized`: the invoice is final at Stripe. Records its
    /// id if the run that submitted it did not get to.
    InvoiceFinalized { invoice: InvoiceRef },
    /// `invoice.voided`: nothing is owed.
    InvoiceVoided { invoice: InvoiceRef },
    /// `checkout.session.completed` in `setup` mode: a payment method was
    /// collected for `tenant`, who asked to move to `plan`.
    CheckoutCompleted {
        session_id: String,
        tenant: String,
        plan: String,
        customer: String,
        setup_intent: Option<String>,
    },
    /// `customer.updated` (and `customer.created`): the customer's default
    /// payment method, `None` when it has none. `changed` is false when
    /// the update did not touch it.
    CustomerPaymentMethod {
        customer: String,
        default_payment_method: Option<String>,
        changed: bool,
    },
    /// `payment_method.detached`: the payment method left `customer`.
    PaymentMethodDetached {
        customer: String,
        payment_method: String,
    },
    /// Anything else: acknowledged and recorded, nothing done. Kosha
    /// rates and invoices itself, so the `customer.subscription.*` events
    /// (Stripe Billing subscriptions) do not drive its model.
    Ignored { reason: String },
}

fn str_field(value: &Value, field: &str) -> Option<String> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// An id-or-expanded-object field (`customer`, `payment_method`, ...).
fn id_field(value: &Value, field: &str) -> Option<String> {
    str_field(value, field).or_else(|| str_field(&value[field], "id"))
}

fn invoice_ref(object: &Value) -> Option<InvoiceRef> {
    Some(InvoiceRef {
        provider_invoice_id: str_field(object, "id")?,
        kosha_invoice_id: str_field(&object["metadata"], "kosha_invoice_id"),
        customer: id_field(object, "customer"),
    })
}

/// What `event` asks of billing.
pub fn action_of(event: &StripeEvent) -> WebhookAction {
    let object = &event.object;
    let ignored = |reason: &str| WebhookAction::Ignored {
        reason: reason.to_string(),
    };
    match event.event_type.as_str() {
        "invoice.paid" | "invoice.payment_succeeded" => {
            let Some(invoice) = invoice_ref(object) else {
                return ignored("invoice without id");
            };
            let paid_at_secs = object["status_transitions"]["paid_at"]
                .as_u64()
                .unwrap_or(event.created);
            WebhookAction::InvoicePaid {
                invoice,
                paid_at_ms: paid_at_secs.saturating_mul(1_000),
            }
        }
        "invoice.payment_failed" => {
            let Some(invoice) = invoice_ref(object) else {
                return ignored("invoice without id");
            };
            let reason = str_field(&object["last_finalization_error"], "message")
                .or_else(|| str_field(&object["last_payment_error"], "message"));
            WebhookAction::InvoicePaymentFailed {
                invoice,
                attempt_count: object["attempt_count"].as_u64().unwrap_or(1),
                reason,
            }
        }
        "invoice.finalized" => match invoice_ref(object) {
            Some(invoice) => WebhookAction::InvoiceFinalized { invoice },
            None => ignored("invoice without id"),
        },
        "invoice.voided" => match invoice_ref(object) {
            Some(invoice) => WebhookAction::InvoiceVoided { invoice },
            None => ignored("invoice without id"),
        },
        "checkout.session.completed" => {
            if object["mode"].as_str() != Some("setup") {
                return ignored("checkout session not in setup mode");
            }
            let metadata = &object["metadata"];
            let (Some(session_id), Some(tenant), Some(plan), Some(customer)) = (
                str_field(object, "id"),
                str_field(metadata, "kosha_tenant"),
                str_field(metadata, "kosha_plan"),
                id_field(object, "customer"),
            ) else {
                return ignored("checkout session not started by Kosha");
            };
            WebhookAction::CheckoutCompleted {
                session_id,
                tenant,
                plan,
                customer,
                setup_intent: id_field(object, "setup_intent"),
            }
        }
        "customer.created" | "customer.updated" => {
            let Some(customer) = str_field(object, "id") else {
                return ignored("customer without id");
            };
            let previous = &event.previous;
            let changed = event.event_type == "customer.created"
                || previous.get("invoice_settings").is_some();
            WebhookAction::CustomerPaymentMethod {
                customer,
                default_payment_method: id_field(
                    &object["invoice_settings"],
                    "default_payment_method",
                ),
                changed,
            }
        }
        "payment_method.detached" => {
            // A detached payment method no longer names its customer; the
            // event's previous attributes do.
            let (Some(payment_method), Some(customer)) = (
                str_field(object, "id"),
                id_field(&event.previous, "customer"),
            ) else {
                return ignored("payment method without a previous customer");
            };
            WebhookAction::PaymentMethodDetached {
                customer,
                payment_method,
            }
        }
        other if other.starts_with("customer.subscription.") => {
            ignored("subscriptions are not used: Kosha rates and invoices usage itself")
        }
        other => WebhookAction::Ignored {
            reason: format!("event type {other} is not used"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 4231 test cases 1, 2 and 6 (a key longer than the block).
    #[test]
    fn hmac_matches_rfc_4231() {
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    const SECRET: &str = "whsec_test_secret";
    const BODY: &[u8] = br#"{"id":"evt_1","type":"invoice.paid"}"#;

    #[test]
    fn a_delivery_is_accepted_only_when_signed_and_fresh() {
        let secrets = vec![SECRET.to_string()];
        let header = sign(SECRET, 1_000, BODY);
        assert_eq!(verify(Some(&header), BODY, &secrets, 1_000, 300), Ok(1_000));
        // At the edge of the tolerance, either side.
        assert!(verify(Some(&header), BODY, &secrets, 1_300, 300).is_ok());
        assert!(verify(Some(&header), BODY, &secrets, 700, 300).is_ok());
        assert_eq!(
            verify(Some(&header), BODY, &secrets, 1_301, 300),
            Err(VerifyError::Stale)
        );
        // One byte of the body changed.
        assert_eq!(
            verify(
                Some(&header),
                br#"{"id":"evt_2","type":"invoice.paid"}"#,
                &secrets,
                1_000,
                300
            ),
            Err(VerifyError::Mismatch)
        );
        // Re-signed with another timestamp: the old signature does not
        // carry over.
        let moved = header.replace("t=1000", "t=1001");
        assert_eq!(
            verify(Some(&moved), BODY, &secrets, 1_000, 300),
            Err(VerifyError::Mismatch)
        );
        let wrong = sign("whsec_other", 1_000, BODY);
        assert_eq!(
            verify(Some(&wrong), BODY, &secrets, 1_000, 300),
            Err(VerifyError::Mismatch)
        );
        for malformed in [
            None,
            Some(""),
            Some("t=1000"),
            Some("v1=abc"),
            Some("t=x,v1=ab"),
        ] {
            assert_eq!(
                verify(malformed, BODY, &secrets, 1_000, 300),
                Err(VerifyError::Malformed),
                "{malformed:?}"
            );
        }
        assert_eq!(
            verify(Some(&header), BODY, &[], 1_000, 300),
            Err(VerifyError::NoSecret)
        );
        // Upper-case hex and spaces after commas are tolerated.
        let loose = header
            .replace(",v1=", ", v1=")
            .to_ascii_uppercase()
            .replace("T=", "t=")
            .replace("V1=", "v1=");
        assert!(verify(Some(&loose), BODY, &secrets, 1_000, 300).is_ok());
    }

    /// Rolling the endpoint secret: Stripe sends one `v1` per active
    /// secret, and the pods may know either or both.
    #[test]
    fn secret_rotation_accepts_any_matching_pair() {
        let old = "whsec_old";
        let new = "whsec_new";
        let both = format!(
            "t=50,v1={},v1={},v0=ignored",
            v1_signature(old, 50, BODY),
            v1_signature(new, 50, BODY)
        );
        for secrets in [vec![old], vec![new], vec![new, old], vec!["whsec_x", new]] {
            let secrets: Vec<String> = secrets.into_iter().map(String::from).collect();
            assert!(
                verify(Some(&both), BODY, &secrets, 50, 300).is_ok(),
                "{secrets:?}"
            );
        }
        // A delivery signed with the new secret only, to a pod that still
        // has only the old one, is refused.
        let new_only = sign(new, 50, BODY);
        assert_eq!(
            verify(Some(&new_only), BODY, &[old.to_string()], 50, 300),
            Err(VerifyError::Mismatch)
        );
        assert!(verify(
            Some(&new_only),
            BODY,
            &[old.to_string(), new.to_string()],
            50,
            300
        )
        .is_ok());
    }

    #[test]
    fn constant_time_eq_compares_whole_slices() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    fn event(event_type: &str, object: Value, previous: Value) -> StripeEvent {
        StripeEvent {
            id: "evt_1".into(),
            event_type: event_type.into(),
            created: 1_700_000_000,
            livemode: false,
            object,
            previous,
        }
    }

    #[test]
    fn events_map_to_what_billing_does() {
        let invoice = serde_json::json!({
            "id": "in_1", "customer": "cus_1",
            "metadata": {"kosha_invoice_id": "acme/2026-10"},
            "status_transitions": {"paid_at": 1_700_000_100u64},
            "attempt_count": 2,
            "last_payment_error": {"message": "card declined"},
        });
        let r = InvoiceRef {
            provider_invoice_id: "in_1".into(),
            kosha_invoice_id: Some("acme/2026-10".into()),
            customer: Some("cus_1".into()),
        };
        assert_eq!(
            action_of(&event("invoice.paid", invoice.clone(), Value::Null)),
            WebhookAction::InvoicePaid {
                invoice: r.clone(),
                paid_at_ms: 1_700_000_100_000
            }
        );
        assert_eq!(
            action_of(&event(
                "invoice.payment_failed",
                invoice.clone(),
                Value::Null
            )),
            WebhookAction::InvoicePaymentFailed {
                invoice: r.clone(),
                attempt_count: 2,
                reason: Some("card declined".into())
            }
        );
        assert_eq!(
            action_of(&event("invoice.voided", invoice.clone(), Value::Null)),
            WebhookAction::InvoiceVoided { invoice: r.clone() }
        );
        assert_eq!(
            action_of(&event("invoice.finalized", invoice, Value::Null)),
            WebhookAction::InvoiceFinalized { invoice: r }
        );
        let session = serde_json::json!({
            "id": "cs_1", "mode": "setup", "customer": "cus_1", "setup_intent": "seti_1",
            "metadata": {"kosha_tenant": "acme", "kosha_plan": "payg"},
        });
        assert_eq!(
            action_of(&event(
                "checkout.session.completed",
                session.clone(),
                Value::Null
            )),
            WebhookAction::CheckoutCompleted {
                session_id: "cs_1".into(),
                tenant: "acme".into(),
                plan: "payg".into(),
                customer: "cus_1".into(),
                setup_intent: Some("seti_1".into()),
            }
        );
        let mut payment = session;
        payment["mode"] = "payment".into();
        assert!(matches!(
            action_of(&event("checkout.session.completed", payment, Value::Null)),
            WebhookAction::Ignored { .. }
        ));
        let customer = serde_json::json!({
            "id": "cus_1", "invoice_settings": {"default_payment_method": {"id": "pm_1"}}
        });
        assert_eq!(
            action_of(&event(
                "customer.updated",
                customer.clone(),
                serde_json::json!({"invoice_settings": {"default_payment_method": null}})
            )),
            WebhookAction::CustomerPaymentMethod {
                customer: "cus_1".into(),
                default_payment_method: Some("pm_1".into()),
                changed: true
            }
        );
        assert_eq!(
            action_of(&event(
                "customer.updated",
                customer,
                serde_json::json!({"email": "x"})
            )),
            WebhookAction::CustomerPaymentMethod {
                customer: "cus_1".into(),
                default_payment_method: Some("pm_1".into()),
                changed: false
            }
        );
        assert_eq!(
            action_of(&event(
                "payment_method.detached",
                serde_json::json!({"id": "pm_1", "customer": null}),
                serde_json::json!({"customer": "cus_1"})
            )),
            WebhookAction::PaymentMethodDetached {
                customer: "cus_1".into(),
                payment_method: "pm_1".into()
            }
        );
        for ignored in ["customer.subscription.updated", "charge.refunded"] {
            assert!(matches!(
                action_of(&event(ignored, serde_json::json!({"id": "x"}), Value::Null)),
                WebhookAction::Ignored { .. }
            ));
        }
    }

    #[test]
    fn parse_event_needs_an_id_a_type_and_an_object() {
        let ok = parse_event(
            br#"{"id":"evt_1","type":"invoice.paid","created":5,"livemode":true,"data":{"object":{"id":"in_1"}}}"#,
        )
        .unwrap();
        assert_eq!(
            (ok.id.as_str(), ok.created, ok.livemode),
            ("evt_1", 5, true)
        );
        for bad in [
            &b"not json"[..],
            br#"{"type":"x","data":{"object":{}}}"#,
            br#"{"id":"evt_1","data":{"object":{}}}"#,
            br#"{"id":"evt_1","type":"x","data":{}}"#,
        ] {
            assert!(parse_event(bad).is_err());
        }
    }
}
