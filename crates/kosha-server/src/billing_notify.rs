//! Where dunning notices go (#349): the log (the default) or an HTTP
//! webhook (`KOSHA_BILLING_NOTIFY_URL`). Both implement
//! `kosha_control::dunning::Notifier`; an email sender can implement the
//! same trait and be handed to `Billing::with_notifier`.
//!
//! The webhook receives each notice as a JSON `POST` (the fields of
//! `kosha_control::dunning::Notice`) with `x-kosha-notice-id` (the notice
//! id, stable across retries, so the receiver can de-duplicate) and, when
//! `KOSHA_BILLING_NOTIFY_SECRET` is set, `x-kosha-signature: t=<unix
//! seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">`: the scheme Stripe uses,
//! so the receiver can verify it the same way. Any 2xx answer is delivery;
//! anything else (or no answer within 10 s) leaves the notice to the next
//! dunning pass.

use std::sync::Arc;
use std::time::Duration;

use kosha_control::dunning::{LogNotifier, Notice, Notifier};
use kosha_control::stripe_webhook::sign;
use kosha_core::KoshaError;

use crate::billing::stripe_base_allowed;

/// Posts notices to an HTTP endpoint. See the module doc.
pub(crate) struct HttpNotifier {
    client: reqwest::blocking::Client,
    url: String,
    secret: Option<String>,
}

impl HttpNotifier {
    /// `url` must be `https://` (plain `http://` to this machine only).
    pub(crate) fn new(url: &str, secret: Option<String>) -> Result<Self, String> {
        if !stripe_base_allowed(url) {
            return Err(
                "KOSHA_BILLING_NOTIFY_URL must be an https:// URL (plain http:// is accepted \
                 for localhost only): notices name tenants and amounts"
                    .into(),
            );
        }
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("billing notices: cannot build HTTP client: {e}"))?;
        Ok(Self {
            client,
            url: url.to_string(),
            secret,
        })
    }
}

impl Notifier for HttpNotifier {
    fn name(&self) -> &str {
        "http"
    }

    fn notify(&self, notice: &Notice) -> Result<(), KoshaError> {
        let body = serde_json::to_vec(notice).map_err(KoshaError::Serde)?;
        let mut request = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .header("x-kosha-notice-id", &notice.id);
        if let Some(secret) = &self.secret {
            let now = crate::unix_millis() / 1_000;
            request = request.header("x-kosha-signature", sign(secret, now, &body));
        }
        let response = request.body(body).send().map_err(|e| {
            KoshaError::ControlPlane(format!("billing notice webhook: {}", e.without_url()))
        })?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(KoshaError::ControlPlane(format!(
                "billing notice webhook answered HTTP {}",
                response.status()
            )))
        }
    }
}

/// The notifier the environment asks for: the HTTP webhook when
/// `KOSHA_BILLING_NOTIFY_URL` is set, else the log.
pub(crate) fn notifier_from_env() -> Result<Arc<dyn Notifier>, String> {
    let get = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    match get("KOSHA_BILLING_NOTIFY_URL") {
        Some(url) => Ok(Arc::new(HttpNotifier::new(
            &url,
            get("KOSHA_BILLING_NOTIFY_SECRET"),
        )?)),
        None => Ok(Arc::new(LogNotifier)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_control::billing::AccountState;
    use kosha_control::dunning::NoticeKind;
    use kosha_control::stripe_webhook::verify;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;

    fn notice() -> Notice {
        Notice {
            id: "acme/2026-10/overdue".into(),
            tenant: "acme".into(),
            kind: NoticeKind::Overdue,
            invoice_id: "acme/2026-10".into(),
            period: "2026-10".into(),
            amount_cents: 1_234,
            currency: "usd".into(),
            due_at_ms: 5,
            state: AccountState::PastDue,
            next_retry_at_ms: None,
            failure_reason: None,
            message: "overdue".into(),
            at_ms: 6,
        }
    }

    /// One request: headers and body; answers `status`.
    /// Headers and body of each request received.
    type Received = Arc<Mutex<Vec<(Vec<(String, String)>, Vec<u8>)>>>;

    fn receiver(status: u16) -> (String, Received) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/hooks/billing", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = seen.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let mut headers = Vec::new();
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    let Some((name, value)) = header.trim_end().split_once(':') else {
                        break;
                    };
                    headers.push((name.to_ascii_lowercase(), value.trim().to_string()));
                }
                let length: usize = headers
                    .iter()
                    .find(|(n, _)| n == "content-length")
                    .and_then(|(_, v)| v.parse().ok())
                    .unwrap_or(0);
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                record.lock().unwrap().push((headers, body));
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                    )
                    .as_bytes(),
                );
            }
        });
        (url, seen)
    }

    #[test]
    fn notices_are_posted_signed_and_a_refusal_is_an_error() {
        let (url, seen) = receiver(204);
        let notifier = HttpNotifier::new(&url, Some("notify-secret".into())).unwrap();
        notifier.notify(&notice()).unwrap();
        let (headers, body) = seen.lock().unwrap()[0].clone();
        let header = |name: &str| {
            headers
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(
            header("x-kosha-notice-id").as_deref(),
            Some("acme/2026-10/overdue")
        );
        let signature = header("x-kosha-signature").unwrap();
        let now = crate::unix_millis() / 1_000;
        assert!(verify(Some(&signature), &body, &["notify-secret".into()], now, 60).is_ok());
        let sent: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(sent["kind"], "overdue");
        assert_eq!(sent["amount_cents"], 1_234);

        let (url, _) = receiver(500);
        let failing = HttpNotifier::new(&url, None).unwrap();
        assert!(failing.notify(&notice()).is_err());
        assert!(HttpNotifier::new("http://notify.example.com/x", None).is_err());
        assert!(HttpNotifier::new("https://notify.example.com/x", None).is_ok());
    }
}
