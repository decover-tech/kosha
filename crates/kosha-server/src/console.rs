//! Self-serve web console (#345 part 2), behind `KOSHA_CONSOLE` (default
//! off).
//!
//! * **Static assets** under `crates/kosha-server/console/`, compiled into
//!   the binary and served at `GET /console` and `GET /console/*`, with the
//!   same strict security headers as the dashboard.
//! * **A JSON API** under `/console/api/`, answered before API-key
//!   authentication: its calls carry a console session instead.
//!
//! ```text
//! POST /console/api/signin          {email, display_name?}   emails a sign-in link
//! POST /console/api/signin/verify   {token}                  -> session_token
//! POST /console/api/signout
//! GET  /console/api/me                                       user + organizations
//! *    /console/api/orgs[/...]                               the /v1/admin/orgs routes
//! ```
//!
//! Every call but the first two needs `Authorization: Bearer <session>`.
//! The `orgs` routes are the organization admin API's, run as the
//! signed-in user ([`Actor::User`]), so its role checks and audit log apply
//! unchanged: a user can create an organization (becoming its owner), then
//! projects, tenants and API keys in it, as their role allows.
//!
//! Sign-in links go to `KOSHA_CONSOLE_EMAIL_WEBHOOK` as a JSON POST
//! (`{to, subject, text, link}`) for a mail service to deliver. Without it
//! the link is written to the server log, for local development only. The
//! link is `KOSHA_CONSOLE_URL` (this server's public origin, never taken
//! from the request's `Host`) + `/console/#signin=<token>`.

use std::collections::HashMap;
use std::time::Duration;

use kosha_control::orgs::{Actor, OrgError, OrgStore};
use kosha_control::signin;
use serde_json::{json, Value};

use crate::auth::KeyStore;
use crate::json_ok;
use crate::org_admin::{self, OrgBackend};

const INDEX_HTML: &str = include_str!("../console/index.html");
const APP_JS: &str = include_str!("../console/app.js");
const STYLE_CSS: &str = include_str!("../console/style.css");

/// Sent on every `/console` response. `form-action 'none'`: the page posts
/// nothing itself, its calls are `fetch`es to `/console/api/`.
const SECURITY_HEADERS: &str = "content-security-policy: default-src 'self'; \
     frame-ancestors 'none'; base-uri 'none'; form-action 'none'\r\n\
     x-frame-options: DENY\r\n\
     x-content-type-options: nosniff\r\n\
     referrer-policy: no-referrer\r\n\
     cache-control: no-store\r\n";

fn flag_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "on" | "true" | "1" | "yes" | "enabled"
        )
    })
}

/// Where a sign-in link goes.
#[derive(Debug, Clone)]
pub(crate) enum Mailer {
    /// `KOSHA_CONSOLE_EMAIL_WEBHOOK`.
    Webhook(String),
    /// No webhook: the server log (development).
    Log,
    /// Tests: kept for the test to read.
    #[cfg(test)]
    Capture(std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>),
}

pub(crate) struct Console {
    enabled: bool,
    public_url: String,
    mailer: Mailer,
}

impl Console {
    pub(crate) fn new(enabled: bool, public_url: &str, mailer: Mailer) -> Self {
        Self {
            enabled,
            public_url: public_url.trim_end_matches('/').to_string(),
            mailer,
        }
    }

    pub(crate) fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let mailer = match var("KOSHA_CONSOLE_EMAIL_WEBHOOK") {
            Some(url) => Mailer::Webhook(url.trim().to_string()),
            None => Mailer::Log,
        };
        Self::new(
            flag_enabled(var("KOSHA_CONSOLE").as_deref()),
            var("KOSHA_CONSOLE_URL").as_deref().unwrap_or(""),
            mailer,
        )
    }

    /// The response for a `/console` request, or `None` when `request_line`
    /// is not one. While the console is off every `/console` path is a 404.
    pub(crate) fn handle(
        &self,
        request_line: &str,
        headers: &HashMap<String, String>,
        body: &[u8],
        orgs: &OrgBackend,
        keys: Option<&KeyStore>,
    ) -> Option<String> {
        let mut parts = request_line.split_whitespace();
        let method = parts.next()?;
        let target = parts.next()?;
        let path = target.split('#').next().unwrap_or(target);
        let (path, query) = path.split_once('?').unwrap_or((path, ""));
        let rest = if path == "/console" {
            ""
        } else {
            path.strip_prefix("/console/")?
        };
        if !self.enabled {
            return Some(not_found());
        }
        if let Some(api) = rest.strip_prefix("api/") {
            let segments: Vec<String> = api.split('/').map(crate::url_decode).collect();
            let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
            let result = orgs
                .store()
                .map_err(OrgError::Store)
                .and_then(|store| self.api(method, &segments, query, headers, body, &*store, keys));
            return Some(result.unwrap_or_else(|e| org_admin::error_response(&e)));
        }
        if method != "GET" {
            return Some(not_found());
        }
        let (content_type, body) = match rest {
            "" | "index.html" => ("text/html; charset=utf-8", INDEX_HTML),
            "app.js" => ("text/javascript; charset=utf-8", APP_JS),
            "style.css" => ("text/css; charset=utf-8", STYLE_CSS),
            _ => return Some(not_found()),
        };
        Some(response("200 OK", content_type, body))
    }

    #[allow(clippy::too_many_arguments)]
    fn api(
        &self,
        method: &str,
        segments: &[&str],
        query: &str,
        headers: &HashMap<String, String>,
        body: &[u8],
        store: &dyn OrgStore,
        keys: Option<&KeyStore>,
    ) -> Result<String, OrgError> {
        match (method, segments) {
            ("POST", ["signin"]) => {
                let body = org_admin::parse_body(body)?;
                let email = org_admin::required(&body, "email")?;
                let name = org_admin::optional(&body, "display_name")?.unwrap_or("");
                let issued = signin::request_signin(store, email, name)?;
                self.send_link(&issued.email, &issued.token)?;
                Ok(json_ok(&json!({
                    "sent": true,
                    "expires_at_ms": issued.expires_at_ms,
                })))
            }
            ("POST", ["signin", "verify"]) => {
                let body = org_admin::parse_body(body)?;
                let token = org_admin::required(&body, "token")?;
                let signed_in = signin::complete_signin(store, token)?;
                Ok(json_ok(&signed_in))
            }
            ("POST", ["signout"]) => {
                signin::sign_out(store, bearer(headers).unwrap_or(""))?;
                Ok(json_ok(&json!({})))
            }
            ("GET", ["me"]) => {
                let user = self.user(headers, store)?;
                let orgs = signin::list_user_orgs(store, &user.id)?;
                Ok(json_ok(&json!({ "user": user, "orgs": orgs })))
            }
            (_, ["orgs", ..]) => {
                let user = self.user(headers, store)?;
                let actor = Actor::User(user.id);
                org_admin::dispatch(method, segments, query, &actor, body, store, keys)
            }
            _ => Err(OrgError::NotFound("no such console route".into())),
        }
    }

    fn user(
        &self,
        headers: &HashMap<String, String>,
        store: &dyn OrgStore,
    ) -> Result<kosha_control::orgs::User, OrgError> {
        let token = bearer(headers)
            .ok_or_else(|| OrgError::Unauthorized("sign in first: no session".into()))?;
        signin::session_user(store, token)
    }

    fn send_link(&self, email: &str, token: &str) -> Result<(), OrgError> {
        let link = format!("{}/console/#signin={token}", self.public_url);
        match &self.mailer {
            Mailer::Log => {
                eprintln!(
                    "console: no KOSHA_CONSOLE_EMAIL_WEBHOOK; sign-in link for {email}: {link}"
                );
                Ok(())
            }
            Mailer::Webhook(url) => {
                let text = format!(
                    "Sign in to Kosha with this link. It works once, for 15 minutes:\n\n{link}\n\n\
                     If you did not ask for it, ignore this email."
                );
                let payload = json!({
                    "to": email,
                    "subject": "Your Kosha sign-in link",
                    "text": text,
                    "link": link,
                });
                deliver(url, &payload).map_err(|e| {
                    eprintln!("console: sign-in email webhook failed: {e}");
                    OrgError::Store("could not send the sign-in email; try again".into())
                })
            }
            #[cfg(test)]
            Mailer::Capture(sent) => {
                sent.lock()
                    .unwrap()
                    .push((email.to_string(), token.to_string()));
                Ok(())
            }
        }
    }
}

fn deliver(url: &str, payload: &Value) -> Result<(), String> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let response = client
        .post(url)
        .json(payload)
        .send()
        .map_err(|e| e.to_string())?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("webhook answered {}", response.status()))
    }
}

fn bearer(headers: &HashMap<String, String>) -> Option<&str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .and_then(|(_, v)| v.trim().strip_prefix("Bearer "))
        .map(str::trim)
}

fn not_found() -> String {
    response("404 Not Found", "text/plain; charset=utf-8", "not found\n")
}

fn response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-length: {}\r\ncontent-type: {content_type}\r\n\
         {SECURITY_HEADERS}\r\n{body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    type Sent = Arc<Mutex<Vec<(String, String)>>>;

    struct Harness {
        console: Console,
        orgs: OrgBackend,
        keys: KeyStore,
        sent: Sent,
    }

    impl Harness {
        fn new(enabled: bool) -> Self {
            let sent: Sent = Arc::default();
            Harness {
                console: Console::new(
                    enabled,
                    "https://kosha.example/",
                    Mailer::Capture(sent.clone()),
                ),
                orgs: OrgBackend::in_memory(),
                keys: KeyStore::with_keys(HashMap::new()),
                sent,
            }
        }

        fn raw(&self, session: Option<&str>, method: &str, path: &str, body: Value) -> String {
            let mut headers = HashMap::new();
            if let Some(session) = session {
                headers.insert("Authorization".to_string(), format!("Bearer {session}"));
            }
            let body = if body.is_null() {
                Vec::new()
            } else {
                body.to_string().into_bytes()
            };
            self.console
                .handle(
                    &format!("{method} {path} HTTP/1.1"),
                    &headers,
                    &body,
                    &self.orgs,
                    Some(&self.keys),
                )
                .expect("a /console path")
        }

        fn call(
            &self,
            session: Option<&str>,
            method: &str,
            path: &str,
            body: Value,
        ) -> (u16, Value) {
            parse(&self.raw(session, method, path, body))
        }

        fn ok(&self, session: Option<&str>, method: &str, path: &str, body: Value) -> Value {
            let (status, value) = self.call(session, method, path, body);
            assert_eq!(status, 200, "{method} {path}: {value}");
            value
        }

        /// Sign `email` in through the emailed link; returns the session.
        fn sign_in(&self, email: &str) -> String {
            self.ok(
                None,
                "POST",
                "/console/api/signin",
                json!({ "email": email, "display_name": "Ada" }),
            );
            let token = self.sent.lock().unwrap().last().unwrap().1.clone();
            let signed_in = self.ok(
                None,
                "POST",
                "/console/api/signin/verify",
                json!({ "token": token }),
            );
            signed_in["session_token"].as_str().unwrap().to_string()
        }
    }

    fn parse(response: &str) -> (u16, Value) {
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let status = head.split(' ').nth(1).unwrap().parse().unwrap();
        (status, serde_json::from_str(body).unwrap_or(Value::Null))
    }

    #[test]
    fn off_by_default_and_other_paths_pass_through() {
        assert!(!flag_enabled(None));
        assert!(flag_enabled(Some(" ON ")));
        let h = Harness::new(false);
        assert_eq!(h.call(None, "GET", "/console", Value::Null).0, 404);
        assert_eq!(
            h.call(
                None,
                "POST",
                "/console/api/signin",
                json!({"email": "a@b.co"})
            )
            .0,
            404
        );
        assert!(h.sent.lock().unwrap().is_empty());
        let on = Harness::new(true);
        let headers = HashMap::new();
        for line in [
            "GET /consoles HTTP/1.1",
            "GET /v1/namespaces HTTP/1.1",
            "GET /dashboard HTTP/1.1",
        ] {
            assert!(on
                .console
                .handle(line, &headers, b"", &on.orgs, None)
                .is_none());
        }
    }

    #[test]
    fn serves_assets_with_security_headers() {
        let h = Harness::new(true);
        for (path, kind) in [
            ("/console", "text/html"),
            ("/console/", "text/html"),
            ("/console/app.js", "text/javascript"),
            ("/console/style.css", "text/css"),
        ] {
            let raw = h.raw(None, "GET", path, Value::Null);
            assert!(raw.starts_with("HTTP/1.1 200"), "{path}");
            assert!(raw.contains(&format!("content-type: {kind}")), "{path}");
            assert!(raw.contains("content-security-policy: default-src 'self'"));
            assert!(raw.contains("x-frame-options: DENY"));
        }
        assert!(h
            .raw(None, "GET", "/console/nope.js", Value::Null)
            .starts_with("HTTP/1.1 404"));
    }

    #[test]
    fn sign_up_to_api_key_without_an_operator() {
        let h = Harness::new(true);
        let session = h.sign_in("Ada@Example.com");
        let sent = h.sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "ada@example.com");

        let me = h.ok(Some(&session), "GET", "/console/api/me", Value::Null);
        assert_eq!(me["user"]["email"], "ada@example.com");
        assert_eq!(me["user"]["display_name"], "Ada");
        assert_eq!(me["orgs"], json!([]));
        let user_id = me["user"]["id"].as_str().unwrap().to_string();

        let org = h.ok(
            Some(&session),
            "POST",
            "/console/api/orgs",
            json!({ "slug": "acme", "name": "Acme" }),
        );
        let org_id = org["id"].as_str().unwrap();
        let me = h.ok(Some(&session), "GET", "/console/api/me", Value::Null);
        assert_eq!(me["orgs"][0]["slug"], "acme");
        assert_eq!(me["orgs"][0]["role"], "owner");

        let base = format!("/console/api/orgs/{org_id}");
        let project = h.ok(
            Some(&session),
            "POST",
            &format!("{base}/projects"),
            json!({ "slug": "search", "name": "Search" }),
        );
        let tenant = h.ok(
            Some(&session),
            "POST",
            &format!("{base}/tenants"),
            json!({ "project_id": project["id"] }),
        );
        let tenant_id = tenant["tenant_id"].as_str().unwrap();
        let key = h.ok(
            Some(&session),
            "POST",
            &format!("{base}/tenants/{tenant_id}/api-keys"),
            json!({ "description": "ci" }),
        );
        assert!(!key["api_key"].as_str().unwrap().is_empty());

        let audit = h.ok(
            Some(&session),
            "GET",
            &format!("{base}/audit-log"),
            Value::Null,
        );
        let actors: Vec<&str> = audit["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["actor"].as_str().unwrap())
            .collect();
        assert!(!actors.is_empty());
        assert!(actors.iter().all(|a| *a == format!("user:{user_id}")));
    }

    #[test]
    fn links_are_single_use_and_sessions_end_at_sign_out() {
        let h = Harness::new(true);
        let session = h.sign_in("bob@example.com");
        let token = h.sent.lock().unwrap()[0].1.clone();
        let (status, _) = h.call(
            None,
            "POST",
            "/console/api/signin/verify",
            json!({ "token": token }),
        );
        assert_eq!(status, 401);
        assert_eq!(
            h.call(
                None,
                "POST",
                "/console/api/signin/verify",
                json!({ "token": "kst_forged" })
            )
            .0,
            401
        );

        h.ok(Some(&session), "POST", "/console/api/signout", Value::Null);
        assert_eq!(
            h.call(Some(&session), "GET", "/console/api/me", Value::Null)
                .0,
            401
        );
    }

    #[test]
    fn org_routes_need_a_session_and_respect_roles() {
        let h = Harness::new(true);
        assert_eq!(h.call(None, "GET", "/console/api/me", Value::Null).0, 401);
        // An admin API key is not a console session.
        assert_eq!(
            h.call(
                Some("admin-secret"),
                "POST",
                "/console/api/orgs",
                json!({"slug": "x", "name": "X"})
            )
            .0,
            401
        );

        let owner = h.sign_in("owner@example.com");
        let org = h.ok(
            Some(&owner),
            "POST",
            "/console/api/orgs",
            json!({ "slug": "acme", "name": "Acme" }),
        );
        let org_id = org["id"].as_str().unwrap();
        let outsider = h.sign_in("eve@example.com");
        assert_eq!(
            h.call(
                Some(&outsider),
                "GET",
                &format!("/console/api/orgs/{org_id}/members"),
                Value::Null
            )
            .0,
            403
        );
        // Creating an organization for someone else is the operator's.
        let me = h.ok(Some(&owner), "GET", "/console/api/me", Value::Null);
        assert_eq!(
            h.call(
                Some(&outsider),
                "POST",
                "/console/api/orgs",
                json!({ "slug": "other", "name": "Other", "owner_user_id": me["user"]["id"] })
            )
            .0,
            403
        );
    }

    #[test]
    fn a_second_link_within_a_minute_is_refused() {
        let h = Harness::new(true);
        let body = json!({ "email": "carol@example.com" });
        h.ok(None, "POST", "/console/api/signin", body.clone());
        let (status, value) = h.call(None, "POST", "/console/api/signin", body);
        assert_eq!(status, 429, "{value}");
        assert_eq!(h.sent.lock().unwrap().len(), 1);
        assert_eq!(
            h.call(
                None,
                "POST",
                "/console/api/signin",
                json!({ "email": "not-an-email" })
            )
            .0,
            400
        );
    }
}
