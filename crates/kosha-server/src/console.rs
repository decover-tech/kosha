//! The self-service console (#345 part 2): sign-up, email verification,
//! password login and reset, Google/GitHub/OIDC sign-in, sessions, and the
//! organization API a signed-in user drives, served under `/v1/console/`,
//! plus the web UI at `/console`.
//!
//! **Off by default.** `KOSHA_CONSOLE=enabled` serves both; otherwise every
//! `/console` and `/v1/console/` path is a 404. Self-serve sign-up is a
//! second switch, `KOSHA_SIGNUP=enabled` (optionally limited to
//! `KOSHA_SIGNUP_EMAIL_DOMAINS`): with it off, accounts come from the
//! operator (`POST /v1/admin/users`), invites, or SSO.
//!
//! **Sessions authenticate the console only.** `dispatch` answers these
//! routes before API-key authentication and never lets a session reach a
//! data-plane route; the data plane keeps using API keys. The session is an
//! opaque random token in the `kosha_session` cookie (`HttpOnly; Secure;
//! SameSite=Lax; Path=/v1/console`), stored hashed, with an absolute
//! lifetime (`KOSHA_SESSION_TTL_HOURS`, default 24) and an idle timeout
//! (`KOSHA_SESSION_IDLE_MINUTES`, default 120). Logout revokes it.
//!
//! **CSRF.** Every request that is not a `GET` must carry
//! `X-Kosha-Console: 1` (a header a cross-site form cannot set and a
//! cross-site script cannot send without a CORS preflight, which this
//! server never grants), and, when the browser sends `Origin`, come from
//! `KOSHA_CONSOLE_PUBLIC_URL`'s origin. `Sec-Fetch-Site: cross-site` is
//! refused outright. The two `GET` callbacks that change state (OAuth and
//! SSO) are bound to the browser that started the sign-in by a `state`
//! that must equal the `kosha_oauth` cookie set at the start.
//!
//! **Attempt limits.** Login, sign-up, verification resends and password
//! resets are counted per client IP and per address in fixed windows
//! (429 with `Retry-After` beyond them). The counters are in this process:
//! with several pods behind a load balancer the effective limit is the
//! per-pod limit times the pods. The IP is the TCP peer, or the right-most
//! entry of `KOSHA_CONSOLE_CLIENT_IP_HEADER` (e.g. `x-forwarded-for`) when
//! a trusted proxy sets it.
//!
//! **Multi-pod.** The console's state is the control plane's (Postgres),
//! so any pod can serve it; it is not forwarded to the ingest pod (that
//! would drop `Set-Cookie`). A key created on one pod works there at once
//! and on the others after their next key refresh
//! (`KOSHA_API_KEY_REFRESH_SECS`). Without Postgres everything lives in one
//! process: single-pod development only.
//!
//! Links in emails and OAuth redirect URIs are built from
//! `KOSHA_CONSOLE_PUBLIC_URL`, never from the request's `Host` (a spoofed
//! `Host` must not turn a reset mail into a link to someone else's site).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use kosha_control::identity::{
    self, ExternalIdentity, IdentityStore, LoginError, LoginFlow, Session, SignUp, SsoSettings,
};
use kosha_control::orgs::{self, Actor, OrgError, Role};
use kosha_control::Scopes;
use serde_json::{json, Value};

use crate::console_crypto::{constant_time_eq, pkce_challenge, random_url_token, SecretKey};
use crate::console_oauth::{self, AuthRequest, Discovery, GitHub, Google, OidcClient};
use crate::mailer::Mailer;
use crate::{http, url_decode, AppState};

pub(crate) const SESSION_COOKIE: &str = "kosha_session";
const OAUTH_COOKIE: &str = "kosha_oauth";
pub(crate) const CSRF_HEADER: &str = "x-kosha-console";
/// How long a started OAuth/SSO sign-in may take.
const LOGIN_FLOW_TTL_MS: i64 = 10 * 60 * 1000;

const INDEX_HTML: &str = include_str!("../console/index.html");
const APP_JS: &str = include_str!("../console/app.js");
const STYLE_CSS: &str = include_str!("../console/style.css");

/// Sent on every `/console` page and asset.
const PAGE_SECURITY_HEADERS: &str = "content-security-policy: default-src 'self'; \
     frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'\r\n\
     x-frame-options: DENY\r\n\
     x-content-type-options: nosniff\r\n\
     referrer-policy: no-referrer\r\n\
     cache-control: no-cache\r\n";

fn flag(name: &str) -> Result<bool, String> {
    match std::env::var(name)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "" | "off" | "false" | "0" | "no" | "disabled" => Ok(false),
        "on" | "true" | "1" | "yes" | "enabled" => Ok(true),
        other => Err(format!("invalid {name} '{other}' (expected enabled or disabled)")),
    }
}

fn env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn env_number(name: &str, default: i64) -> Result<i64, String> {
    match env(name) {
        None => Ok(default),
        Some(v) => v
            .parse::<i64>()
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| format!("invalid {name} '{v}' (expected a positive integer)")),
    }
}

// ── Attempt limits ──────────────────────────────────────────────────────────

/// Fixed-window counters, in this process.
pub(crate) struct RateLimiter {
    windows: Mutex<HashMap<String, (u64, u32)>>,
}

/// The most counters kept; beyond it expired ones are dropped, and if that
/// is not enough, all of them (a flood of distinct keys must not grow
/// memory without bound).
const MAX_COUNTERS: usize = 100_000;

impl RateLimiter {
    fn new() -> Self {
        Self {
            windows: Mutex::new(HashMap::new()),
        }
    }

    /// Count one attempt for `key`; `Err(retry_after_secs)` beyond `limit`
    /// per `window_secs`.
    pub(crate) fn check(&self, key: &str, limit: u32, window_secs: u64) -> Result<(), u64> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let window = now / window_secs;
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        if windows.len() >= MAX_COUNTERS {
            windows.retain(|k, (w, _)| {
                let secs = k.rsplit('|').next().and_then(|s| s.parse().ok()).unwrap_or(1);
                *w == now / secs
            });
            if windows.len() >= MAX_COUNTERS {
                windows.clear();
            }
        }
        let entry = windows
            .entry(format!("{key}|{window_secs}"))
            .or_insert((window, 0));
        if entry.0 != window {
            *entry = (window, 0);
        }
        entry.1 += 1;
        if entry.1 > limit {
            Err((window + 1) * window_secs - now)
        } else {
            Ok(())
        }
    }
}

// ── Configuration ───────────────────────────────────────────────────────────

/// The console's configuration and per-process state. One per process, on
/// `AppState`.
pub(crate) struct Console {
    enabled: bool,
    signup: bool,
    signup_domains: Vec<String>,
    /// `https://host[:port]`, no trailing slash.
    public_url: String,
    pub(crate) mailer: Arc<dyn Mailer>,
    session_ttl_ms: i64,
    session_idle_ms: i64,
    secret_key: Option<SecretKey>,
    google: Option<Google>,
    github: Option<GitHub>,
    oidc: OidcClient,
    limiter: RateLimiter,
    client_ip_header: Option<String>,
}

impl Console {
    pub(crate) fn disabled() -> Self {
        Self {
            enabled: false,
            signup: false,
            signup_domains: Vec::new(),
            public_url: String::new(),
            mailer: Arc::new(crate::mailer::LogMailer),
            session_ttl_ms: 24 * 3600 * 1000,
            session_idle_ms: 120 * 60 * 1000,
            secret_key: None,
            google: None,
            github: None,
            oidc: OidcClient::new(),
            limiter: RateLimiter::new(),
            client_ip_header: None,
        }
    }

    /// An enabled console for tests: sign-up on, the given mailer.
    #[cfg(test)]
    pub(crate) fn for_tests(public_url: &str, mailer: Arc<dyn Mailer>) -> Self {
        Self {
            enabled: true,
            signup: true,
            public_url: public_url.to_string(),
            mailer,
            secret_key: Some(SecretKey::parse(&"42".repeat(32)).unwrap()),
            ..Self::disabled()
        }
    }

    #[cfg(test)]
    pub(crate) fn with_google(mut self, google: Google) -> Self {
        self.google = Some(google);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_github(mut self, github: GitHub) -> Self {
        self.github = Some(github);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_signup(mut self, on: bool, domains: &[&str]) -> Self {
        self.signup = on;
        self.signup_domains = domains.iter().map(|d| d.to_string()).collect();
        self
    }

    /// From the environment. Exits the process on a configuration error
    /// while the console is enabled: a half-configured login surface must
    /// not come up.
    pub(crate) fn from_env() -> Self {
        match Self::try_from_env() {
            Ok(console) => console,
            Err(e) => {
                eprintln!("FATAL: console: {e}");
                std::process::exit(2);
            }
        }
    }

    fn try_from_env() -> Result<Self, String> {
        if !flag("KOSHA_CONSOLE")? {
            return Ok(Self::disabled());
        }
        let public_url = env("KOSHA_CONSOLE_PUBLIC_URL").ok_or(
            "KOSHA_CONSOLE=enabled needs KOSHA_CONSOLE_PUBLIC_URL (the https:// address users \
             reach the console at; email links and OAuth redirects are built from it)",
        )?;
        let public_url = public_url.trim_end_matches('/').to_string();
        let local = public_url.starts_with("http://localhost")
            || public_url.starts_with("http://127.0.0.1");
        let host = public_url.split_once("://").map_or("", |(_, h)| h);
        if !(public_url.starts_with("https://") || local) || host.is_empty() || host.contains('/') {
            return Err(format!(
                "KOSHA_CONSOLE_PUBLIC_URL '{public_url}' must be an https:// origin (no path); \
                 http:// is accepted only for localhost"
            ));
        }
        let (mailer, mailer_description) = crate::mailer::from_env()?;
        let secret_key = env("KOSHA_CONSOLE_SECRET_KEY")
            .map(|k| SecretKey::parse(&k))
            .transpose()?;
        let pair = |id: &str, secret: &str| match (env(id), env(secret)) {
            (Some(i), Some(s)) => Ok(Some((i, s))),
            (None, None) => Ok(None),
            _ => Err(format!("set both {id} and {secret}, or neither")),
        };
        let google = pair(
            "KOSHA_OAUTH_GOOGLE_CLIENT_ID",
            "KOSHA_OAUTH_GOOGLE_CLIENT_SECRET",
        )?
        .map(|(client_id, client_secret)| Google {
            client_id,
            client_secret,
            issuer: console_oauth::GOOGLE_ISSUER.to_string(),
        });
        let github = pair(
            "KOSHA_OAUTH_GITHUB_CLIENT_ID",
            "KOSHA_OAUTH_GITHUB_CLIENT_SECRET",
        )?
        .map(|(client_id, client_secret)| GitHub {
            client_id,
            client_secret,
            authorize_url: "https://github.com/login/oauth/authorize".into(),
            token_url: "https://github.com/login/oauth/access_token".into(),
            api_url: "https://api.github.com".into(),
        });
        let signup = flag("KOSHA_SIGNUP")?;
        let signup_domains: Vec<String> = env("KOSHA_SIGNUP_EMAIL_DOMAINS")
            .unwrap_or_default()
            .split(',')
            .map(|d| d.trim().to_ascii_lowercase())
            .filter(|d| !d.is_empty())
            .collect();
        let console = Self {
            enabled: true,
            signup,
            signup_domains,
            public_url,
            mailer,
            session_ttl_ms: env_number("KOSHA_SESSION_TTL_HOURS", 24)? * 3600 * 1000,
            session_idle_ms: env_number("KOSHA_SESSION_IDLE_MINUTES", 120)? * 60 * 1000,
            secret_key,
            google,
            github,
            oidc: OidcClient::new(),
            limiter: RateLimiter::new(),
            client_ip_header: env("KOSHA_CONSOLE_CLIENT_IP_HEADER").map(|h| h.to_ascii_lowercase()),
        };
        println!(
            "console: enabled at {}/console (sign-up {}, mailer {mailer_description}, \
             google {}, github {}, organization SSO {})",
            console.public_url,
            if console.signup { "on" } else { "off" },
            on_off(console.google.is_some()),
            on_off(console.github.is_some()),
            on_off(console.secret_key.is_some()),
        );
        if mailer_description.starts_with("log") {
            eprintln!(
                "WARN: console: KOSHA_MAILER=log prints verification, reset and invite links \
                 to the server log; use KOSHA_MAILER=smtp outside development"
            );
        }
        Ok(console)
    }
}

fn on_off(on: bool) -> &'static str {
    if on {
        "on"
    } else {
        "off"
    }
}

// ── Responses ───────────────────────────────────────────────────────────────

fn raw(status: u16, content_type: &str, extra_headers: &str, body: &str) -> http::Response {
    http::Response::from_raw(format!(
        "HTTP/1.1 {status} X\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\
         {extra_headers}\r\n{body}",
        body.len()
    ))
}

const API_HEADERS: &str = "cache-control: no-store\r\nx-content-type-options: nosniff\r\n";

fn reply_with(status: u16, body: &Value, cookies: &[String]) -> http::Response {
    let mut headers = API_HEADERS.to_string();
    for cookie in cookies {
        headers.push_str(&format!("set-cookie: {cookie}\r\n"));
    }
    raw(status, "application/json", &headers, &body.to_string())
}

fn reply(status: u16, body: Value) -> http::Response {
    reply_with(status, &body, &[])
}

fn error(status: u16, message: &str) -> http::Response {
    reply(status, json!({ "error": message }))
}

fn org_error(e: &OrgError) -> http::Response {
    let status = match e {
        OrgError::Invalid(_) => 400,
        OrgError::Forbidden(_) => 403,
        OrgError::NotFound(_) => 404,
        OrgError::Conflict(_) => 409,
        OrgError::Store(m) => {
            eprintln!("ERROR: console: control plane: {m}");
            return error(500, "control plane error");
        }
    };
    error(status, &e.to_string())
}

fn oe(e: OrgError) -> http::Response {
    org_error(&e)
}

fn too_many(retry_after: u64) -> http::Response {
    raw(
        429,
        "application/json",
        &format!("{API_HEADERS}retry-after: {retry_after}\r\n"),
        &json!({ "error": "too many attempts; try again later" }).to_string(),
    )
}

/// A redirect (OAuth), optionally setting cookies.
fn redirect(location: &str, cookies: &[String]) -> http::Response {
    let mut headers = format!("{API_HEADERS}location: {location}\r\n");
    for cookie in cookies {
        headers.push_str(&format!("set-cookie: {cookie}\r\n"));
    }
    raw(302, "text/plain; charset=utf-8", &headers, "")
}

fn session_cookie(token: &str, max_age_secs: i64) -> String {
    format!(
        "{SESSION_COOKIE}={token}; Path=/v1/console; HttpOnly; Secure; SameSite=Lax; \
         Max-Age={max_age_secs}"
    )
}

fn clear_cookie(name: &str, path: &str) -> String {
    format!("{name}=; Path={path}; HttpOnly; Secure; SameSite=Lax; Max-Age=0")
}

fn oauth_cookie(state: &str) -> String {
    format!(
        "{OAUTH_COOKIE}={state}; Path=/v1/console; HttpOnly; Secure; SameSite=Lax; Max-Age={}",
        LOGIN_FLOW_TTL_MS / 1000
    )
}

// ── Requests ────────────────────────────────────────────────────────────────

struct Req<'a> {
    method: &'a str,
    /// Path segments after `/v1/console/`, URL-decoded.
    segments: Vec<String>,
    query: &'a str,
    headers: &'a HashMap<String, String>,
    body: &'a [u8],
}

impl Req<'_> {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn cookie(&self, name: &str) -> Option<&str> {
        self.header("cookie")?.split(';').find_map(|pair| {
            let (k, v) = pair.trim().split_once('=')?;
            (k == name && !v.is_empty()).then_some(v)
        })
    }

    fn query(&self, name: &str) -> Option<String> {
        self.query.split('&').find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == name).then(|| url_decode(v))
        })
    }

    fn json(&self) -> Result<Value, http::Response> {
        if self.body.iter().all(u8::is_ascii_whitespace) {
            return Ok(json!({}));
        }
        match serde_json::from_slice::<Value>(self.body) {
            Ok(v) if v.is_object() => Ok(v),
            _ => Err(error(400, "body must be a JSON object")),
        }
    }
}

fn field<'a>(body: &'a Value, name: &str) -> Result<Option<&'a str>, http::Response> {
    match body.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(error(400, &format!("'{name}' must be a string"))),
    }
}

fn required<'a>(body: &'a Value, name: &str) -> Result<&'a str, http::Response> {
    field(body, name)?.ok_or_else(|| error(400, &format!("missing '{name}'")))
}

fn role_field(body: &Value) -> Result<Role, http::Response> {
    Role::parse(required(body, "role")?)
        .ok_or_else(|| error(400, "invalid 'role': one of owner, admin, member, read-only"))
}

/// The client address the attempt limits count by.
fn client_ip(console: &Console, req: &Req<'_>) -> String {
    if let Some(name) = &console.client_ip_header {
        if let Some(value) = req.header(name) {
            if let Some(last) = value.rsplit(',').map(str::trim).find(|v| !v.is_empty()) {
                return last.to_string();
            }
        }
    }
    req.header(http::PEER_IP_HEADER)
        .unwrap_or("unknown")
        .to_string()
}

/// The CSRF checks for a request that is not a `GET` (see module docs).
fn check_csrf(console: &Console, req: &Req<'_>) -> Result<(), http::Response> {
    if req.header(CSRF_HEADER).map(str::trim) != Some("1") {
        return Err(error(
            403,
            "missing 'X-Kosha-Console: 1' header (cross-site request refused)",
        ));
    }
    if req.header("sec-fetch-site") == Some("cross-site") {
        return Err(error(403, "cross-site request refused"));
    }
    if let Some(origin) = req.header("origin") {
        if origin.trim_end_matches('/') != console.public_url {
            return Err(error(403, "request from another origin refused"));
        }
    }
    Ok(())
}

fn limit(
    console: &Console,
    key: &str,
    per: u32,
    window_secs: u64,
) -> Result<(), http::Response> {
    console
        .limiter
        .check(key, per, window_secs)
        .map_err(too_many)
}

// ── Entry point ─────────────────────────────────────────────────────────────

/// The response for a `/console` or `/v1/console/` request, or `None` when
/// the request is not one. Called by `dispatch` before API-key
/// authentication.
pub(crate) fn handle(
    state: &AppState,
    request_line: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> Option<http::Response> {
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?;
    let target = parts.next()?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    if path == "/console" || path.starts_with("/console/") {
        return Some(static_response(method, path, state.console.enabled));
    }
    let rest = if path == "/v1/console" {
        ""
    } else {
        path.strip_prefix("/v1/console/")?
    };
    if !state.console.enabled {
        return Some(error(404, "not found"));
    }
    let req = Req {
        method,
        segments: rest.split('/').map(url_decode).collect(),
        query,
        headers,
        body,
    };
    if method != "GET" {
        if let Err(response) = check_csrf(&state.console, &req) {
            return Some(response);
        }
    }
    let store = match state.orgs.identity_store(state.auth()) {
        Ok(store) => store,
        Err(e) => {
            eprintln!("ERROR: console: {e}");
            return Some(error(503, "control plane unavailable"));
        }
    };
    let ctx = Ctx {
        state,
        console: &state.console,
        store: &*store,
        req,
    };
    Some(ctx.route().unwrap_or_else(|response| response))
}

fn static_response(method: &str, path: &str, enabled: bool) -> http::Response {
    let page = |status: u16, content_type: &str, body: &str| {
        raw(status, content_type, PAGE_SECURITY_HEADERS, body)
    };
    if !enabled || method != "GET" {
        return page(404, "text/plain; charset=utf-8", "not found\n");
    }
    match path {
        "/console" | "/console/" | "/console/index.html" => {
            page(200, "text/html; charset=utf-8", INDEX_HTML)
        }
        "/console/app.js" => page(200, "text/javascript; charset=utf-8", APP_JS),
        "/console/style.css" => page(200, "text/css; charset=utf-8", STYLE_CSS),
        _ => page(404, "text/plain; charset=utf-8", "not found\n"),
    }
}

type Answer = Result<http::Response, http::Response>;

struct Ctx<'a> {
    state: &'a AppState,
    console: &'a Console,
    store: &'a dyn IdentityStore,
    req: Req<'a>,
}

/// The signed-in user of a request.
struct SignedIn {
    session: Session,
    user: orgs::User,
}

impl SignedIn {
    fn actor(&self) -> Actor {
        Actor::User(self.user.id.clone())
    }
}

impl Ctx<'_> {
    fn route(&self) -> Answer {
        let segs: Vec<&str> = self.req.segments.iter().map(String::as_str).collect();
        match (self.req.method, segs.as_slice()) {
            ("GET", ["config"]) => Ok(self.config()),
            ("POST", ["signup"]) => self.signup(),
            ("POST", ["verify-email"]) => self.verify_email(),
            ("POST", ["resend-verification"]) => self.resend_verification(),
            ("POST", ["login"]) => self.login(),
            ("POST", ["logout"]) => self.logout(),
            ("POST", ["password-reset"]) => self.password_reset(),
            ("POST", ["password-reset", "confirm"]) => self.password_reset_confirm(),
            ("GET", ["oauth", provider, "start"]) => self.oauth_start(provider),
            ("GET", ["oauth", provider, "callback"]) => Ok(self.oauth_callback(provider)),
            ("GET", ["sso", "start"]) => self.sso_start(),
            ("GET", ["sso", "callback"]) => Ok(self.oauth_callback("sso")),
            ("GET", ["me"]) => self.me(),
            ("POST", ["invites", "accept"]) => self.accept_invite(),
            ("POST", ["orgs"]) => self.create_org(),
            (_, ["orgs", org, rest @ ..]) => self.org_route(org, rest),
            _ => Err(error(404, "not found")),
        }
    }

    fn signed_in(&self) -> Result<SignedIn, http::Response> {
        let unauthorized = || error(401, "sign in first");
        let token = self.req.cookie(SESSION_COOKIE).ok_or_else(unauthorized)?;
        match identity::session(self.store, token, self.console.session_idle_ms) {
            Ok(Some((session, user))) => Ok(SignedIn { session, user }),
            Ok(None) => Err(unauthorized()),
            Err(e) => Err(org_error(&e)),
        }
    }

    fn ip(&self) -> String {
        client_ip(self.console, &self.req)
    }

    /// Mail, logging (never failing the request on) a delivery error: the
    /// answer must not depend on whether the address exists.
    fn mail(&self, to: &str, subject: &str, body: &str) {
        if let Err(e) = self.console.mailer.send(to, subject, body) {
            eprintln!("WARN: console: mail to {to} failed: {e}");
        }
    }

    fn link(&self, page: &str, token: &str) -> String {
        format!("{}/console#/{page}?token={token}", self.console.public_url)
    }

    fn config(&self) -> http::Response {
        reply(
            200,
            json!({
                "signup": self.console.signup,
                "signup_email_domains": self.console.signup_domains,
                "providers": {
                    "google": self.console.google.is_some(),
                    "github": self.console.github.is_some(),
                },
                "sso": self.console.secret_key.is_some(),
                "min_password_length": identity::MIN_PASSWORD_LEN,
                // #349: set when Stripe Checkout / Portal routes exist.
                "billing_actions": billing_actions_available(),
            }),
        )
    }

    // ── Sign-up, verification, login, reset ─────────────────────────────

    fn signup(&self) -> Answer {
        let body = self.req.json()?;
        let email = required(&body, "email")?;
        let password = required(&body, "password")?;
        let name = field(&body, "display_name")?.unwrap_or("");
        let invite = field(&body, "invite_token")?;
        let normalized = email.trim().to_lowercase();
        limit(self.console, &format!("signup-ip:{}", self.ip()), 20, 3600)?;
        limit(self.console, &format!("signup-email:{normalized}"), 5, 3600)?;
        // An invited address may sign up while open sign-up is off.
        let invited = match invite {
            Some(token) => {
                identity::invite_email(self.store, token).map_err(|e| org_error(&e))?
                    == Some(normalized.clone())
            }
            None => false,
        };
        if !invited {
            if !self.console.signup {
                return Err(error(403, "sign-up is not open on this server"));
            }
            if !identity::domain_allowed(&normalized, &self.console.signup_domains) {
                return Err(error(403, "sign-up is not open for this email domain"));
            }
        }
        match identity::sign_up(self.store, email, password, name).map_err(|e| org_error(&e))? {
            SignUp::Created { user, verify_token } => self.mail(
                &user.email,
                "Verify your email address for Kosha",
                &format!(
                    "Welcome to Kosha.\n\nOpen this link to verify your email address \
                     (it works for 24 hours, once):\n\n{}\n\nIf you did not sign up, ignore \
                     this message.\n",
                    self.link("verify", &verify_token)
                ),
            ),
            SignUp::Existing { user } => self.mail(
                &user.email,
                "Your Kosha account",
                &format!(
                    "Someone tried to sign up for Kosha with this address, which already has \
                     an account.\n\nIf it was you, sign in at {}/console, or reset your \
                     password there. Otherwise ignore this message.\n",
                    self.console.public_url
                ),
            ),
        }
        Ok(reply(
            202,
            json!({
                "status": "check_email",
                "message": "If this address can be used, a verification link is on its way.",
            }),
        ))
    }

    fn verify_email(&self) -> Answer {
        let body = self.req.json()?;
        let token = required(&body, "token")?;
        limit(self.console, &format!("verify-ip:{}", self.ip()), 30, 3600)?;
        let user = identity::verify_email(self.store, token).map_err(|e| org_error(&e))?;
        Ok(reply(200, json!({ "verified": true, "email": user.email })))
    }

    fn resend_verification(&self) -> Answer {
        let body = self.req.json()?;
        let email = required(&body, "email")?;
        limit(self.console, &format!("resend-ip:{}", self.ip()), 20, 3600)?;
        limit(
            self.console,
            &format!("resend-email:{}", email.trim().to_lowercase()),
            3,
            3600,
        )?;
        if let Some((user, token)) =
            identity::resend_verification(self.store, email).map_err(|e| org_error(&e))?
        {
            self.mail(
                &user.email,
                "Verify your email address for Kosha",
                &format!(
                    "Open this link to verify your email address (it works for 24 hours, \
                     once):\n\n{}\n",
                    self.link("verify", &token)
                ),
            );
        }
        Ok(reply(202, json!({ "status": "check_email" })))
    }

    fn login(&self) -> Answer {
        let body = self.req.json()?;
        let email = required(&body, "email")?;
        let password = required(&body, "password")?;
        limit(self.console, &format!("login-ip:{}", self.ip()), 50, 900)?;
        limit(
            self.console,
            &format!("login-email:{}", email.trim().to_lowercase()),
            10,
            900,
        )?;
        let user = match identity::password_login(self.store, email, password) {
            Ok(user) => user,
            Err(LoginError::InvalidCredentials) => {
                return Err(error(401, "invalid email or password"))
            }
            Err(LoginError::Unverified) => {
                return Err(reply(
                    403,
                    json!({
                        "error": "verify your email address first",
                        "reason": "email_unverified",
                    }),
                ))
            }
            Err(LoginError::Store(m)) => return Err(org_error(&OrgError::Store(m))),
        };
        self.open_session(&user, "password")
    }

    fn open_session(&self, user: &orgs::User, method: &str) -> Answer {
        let token = identity::create_session(self.store, &user.id, method, self.console.session_ttl_ms)
            .map_err(|e| org_error(&e))?;
        Ok(reply_with(
            200,
            &json!({ "user": user, "auth_method": method }),
            &[session_cookie(&token, self.console.session_ttl_ms / 1000)],
        ))
    }

    fn logout(&self) -> Answer {
        if let Some(token) = self.req.cookie(SESSION_COOKIE) {
            identity::revoke_session(self.store, token).map_err(|e| org_error(&e))?;
        }
        Ok(reply_with(
            200,
            &json!({ "signed_out": true }),
            &[clear_cookie(SESSION_COOKIE, "/v1/console")],
        ))
    }

    fn password_reset(&self) -> Answer {
        let body = self.req.json()?;
        let email = required(&body, "email")?;
        limit(self.console, &format!("reset-ip:{}", self.ip()), 20, 3600)?;
        limit(
            self.console,
            &format!("reset-email:{}", email.trim().to_lowercase()),
            3,
            3600,
        )?;
        if let Some((user, token)) =
            identity::start_password_reset(self.store, email).map_err(|e| org_error(&e))?
        {
            self.mail(
                &user.email,
                "Reset your Kosha password",
                &format!(
                    "Open this link to choose a new password (it works for one hour, once):\
                     \n\n{}\n\nIf you did not ask for this, ignore this message: your \
                     password is unchanged.\n",
                    self.link("reset", &token)
                ),
            );
        }
        Ok(reply(202, json!({ "status": "check_email" })))
    }

    fn password_reset_confirm(&self) -> Answer {
        let body = self.req.json()?;
        let token = required(&body, "token")?;
        let password = required(&body, "password")?;
        limit(self.console, &format!("reset-confirm-ip:{}", self.ip()), 30, 3600)?;
        identity::finish_password_reset(self.store, token, password).map_err(|e| org_error(&e))?;
        Ok(reply(200, json!({ "password_changed": true })))
    }

    // ── OAuth / OIDC ────────────────────────────────────────────────────

    fn redirect_uri(&self, provider: &str) -> String {
        match provider {
            "sso" => format!("{}/v1/console/sso/callback", self.console.public_url),
            p => format!("{}/v1/console/oauth/{p}/callback", self.console.public_url),
        }
    }

    /// Record a flow and redirect the browser to the provider.
    fn begin(
        &self,
        provider: &str,
        org_id: Option<String>,
        endpoint: &str,
        client_id: &str,
        oidc: bool,
    ) -> Answer {
        limit(self.console, &format!("oauth-ip:{}", self.ip()), 60, 60)?;
        let state = random_url_token(32);
        let flow = LoginFlow {
            provider: provider.to_string(),
            org_id,
            pkce_verifier: random_url_token(32),
            nonce: random_url_token(32),
            expires_at_ms: orgs_now_ms() + LOGIN_FLOW_TTL_MS,
        };
        identity::begin_login_flow(self.store, &state, &flow).map_err(|e| org_error(&e))?;
        let url = console_oauth::authorize_url(
            endpoint,
            &AuthRequest {
                client_id,
                redirect_uri: &self.redirect_uri(provider),
                state: &state,
                nonce: oidc.then_some(flow.nonce.as_str()),
                code_challenge: &pkce_challenge(&flow.pkce_verifier),
                scope: if oidc {
                    "openid email profile"
                } else {
                    "read:user user:email"
                },
            },
        );
        Ok(redirect(&url, &[oauth_cookie(&state)]))
    }

    fn oauth_start(&self, provider: &str) -> Answer {
        match provider {
            "google" => {
                let google = self
                    .console
                    .google
                    .as_ref()
                    .ok_or_else(|| error(404, "Google sign-in is not configured"))?;
                let d = self
                    .console
                    .oidc
                    .discover(&google.issuer)
                    .map_err(|e| provider_down(&e))?;
                self.begin(
                    "google",
                    None,
                    &d.authorization_endpoint,
                    &google.client_id,
                    true,
                )
            }
            "github" => {
                let gh = self
                    .console
                    .github
                    .as_ref()
                    .ok_or_else(|| error(404, "GitHub sign-in is not configured"))?;
                self.begin("github", None, &gh.authorize_url, &gh.client_id, false)
            }
            _ => Err(error(404, "unknown sign-in provider")),
        }
    }

    fn sso_start(&self) -> Answer {
        let slug = self
            .req
            .query("org")
            .ok_or_else(|| error(400, "missing '?org=<organization slug>'"))?;
        let secret_key = self
            .console
            .secret_key
            .as_ref()
            .ok_or_else(|| error(404, "single sign-on is not enabled on this server"))?;
        let not_configured = || error(404, "this organization has no single sign-on");
        let org = identity::org_by_slug(self.store, &slug).map_err(|_| not_configured())?;
        let config = identity::sso_config_for_login(self.store, &org.id)
            .map_err(|e| org_error(&e))?
            .ok_or_else(not_configured)?;
        // Fail now rather than at the callback if the secret is unusable.
        secret_key
            .open(&org.id, &config.client_secret_sealed)
            .map_err(|e| provider_down(&e))?;
        let d = self
            .console
            .oidc
            .discover(&config.issuer)
            .map_err(|e| provider_down(&e))?;
        self.begin(
            "sso",
            Some(org.id),
            &d.authorization_endpoint,
            &config.client_id,
            true,
        )
    }

    /// The end of every provider sign-in: always a redirect into the UI,
    /// with a session cookie or an error message.
    fn oauth_callback(&self, provider: &str) -> http::Response {
        let clear = clear_cookie(OAUTH_COOKIE, "/v1/console");
        let fail = |message: &str| {
            redirect(
                &format!(
                    "/console#/login?error={}",
                    console_oauth::encode(message)
                ),
                std::slice::from_ref(&clear),
            )
        };
        match self.finish_oauth(provider) {
            Ok((user, method, landing)) => {
                match identity::create_session(
                    self.store,
                    &user.id,
                    &method,
                    self.console.session_ttl_ms,
                ) {
                    Ok(token) => redirect(
                        &landing,
                        &[
                            session_cookie(&token, self.console.session_ttl_ms / 1000),
                            clear.clone(),
                        ],
                    ),
                    Err(e) => {
                        eprintln!("ERROR: console: session: {e}");
                        fail("sign-in failed; try again")
                    }
                }
            }
            Err(message) => fail(&message),
        }
    }

    /// Check the callback against its flow and the provider; the user, the
    /// session's auth method and where to land.
    fn finish_oauth(&self, provider: &str) -> Result<(orgs::User, String, String), String> {
        if let Some(e) = self.req.query("error") {
            return Err(format!("the provider refused the sign-in ({e})"));
        }
        let state = self.req.query("state").ok_or("missing state")?;
        let code = self.req.query("code").ok_or("missing code")?;
        // The flow must have started in this browser.
        let cookie = self.req.cookie(OAUTH_COOKIE).unwrap_or("");
        if !constant_time_eq(cookie.as_bytes(), state.as_bytes()) {
            return Err("this sign-in was started in another browser; start again".into());
        }
        let flow = identity::finish_login_flow(self.store, &state).map_err(|e| e.to_string())?;
        if flow.provider != provider {
            return Err("sign-in provider mismatch; start again".into());
        }
        let redirect_uri = self.redirect_uri(provider);
        let oidc = &self.console.oidc;
        let now = orgs_now_ms() / 1000;
        let log = |e: String| {
            eprintln!("WARN: console: {provider} sign-in failed: {e}");
            e
        };
        match provider {
            "google" => {
                let g = self.console.google.as_ref().ok_or("Google sign-in is off")?;
                let d = oidc.discover(&g.issuer).map_err(log)?;
                let id_token = oidc
                    .exchange_code(&d, &g.client_id, &g.client_secret, &code, &redirect_uri, &flow.pkce_verifier)
                    .map_err(log)?;
                let issuers = [g.issuer.as_str(), "accounts.google.com"];
                let v = oidc
                    .verify_id_token(&id_token, &d, &issuers, &g.client_id, &flow.nonce, now)
                    .map_err(log)?;
                let user = self.global_login("google", v)?;
                Ok((user, "google".into(), "/console#/".into()))
            }
            "github" => {
                let gh = self.console.github.as_ref().ok_or("GitHub sign-in is off")?;
                let v = oidc
                    .github_user(gh, &code, &redirect_uri, &flow.pkce_verifier)
                    .map_err(log)?;
                let user = self.global_login("github", v)?;
                Ok((user, "github".into(), "/console#/".into()))
            }
            "sso" => {
                let org_id = flow.org_id.clone().ok_or("missing organization")?;
                let key = self.console.secret_key.as_ref().ok_or("single sign-on is off")?;
                let config = identity::sso_config_for_login(self.store, &org_id)
                    .map_err(|e| e.to_string())?
                    .ok_or("this organization has no single sign-on")?;
                let secret = key.open(&org_id, &config.client_secret_sealed).map_err(log)?;
                let d: Discovery = oidc.discover(&config.issuer).map_err(log)?;
                let id_token = oidc
                    .exchange_code(&d, &config.client_id, &secret, &code, &redirect_uri, &flow.pkce_verifier)
                    .map_err(log)?;
                let issuers = [config.issuer.as_str()];
                let v = oidc
                    .verify_id_token(&id_token, &d, &issuers, &config.client_id, &flow.nonce, now)
                    .map_err(log)?;
                let provider = format!("sso:{org_id}");
                let user = identity::sso_login(
                    self.store,
                    &org_id,
                    &ExternalIdentity {
                        provider: provider.clone(),
                        subject: v.subject,
                        email: v.email,
                        display_name: v.name,
                    },
                )
                .map_err(|e| e.to_string())?;
                Ok((user, provider, format!("/console#/orgs/{org_id}")))
            }
            _ => Err("unknown sign-in provider".into()),
        }
    }

    /// Google or GitHub: link or create (when sign-up allows the address).
    fn global_login(
        &self,
        provider: &str,
        v: console_oauth::VerifiedUser,
    ) -> Result<orgs::User, String> {
        let email = v.email.trim().to_lowercase();
        let allow_create = self.console.signup
            && identity::domain_allowed(&email, &self.console.signup_domains);
        identity::external_login(
            self.store,
            &ExternalIdentity {
                provider: provider.to_string(),
                subject: v.subject,
                email: v.email,
                display_name: v.name,
            },
            allow_create,
        )
        .map_err(|e| e.to_string())
    }

    // ── Signed-in user ──────────────────────────────────────────────────

    fn me(&self) -> Answer {
        let me = self.signed_in()?;
        let orgs = identity::user_orgs(self.store, &me.user.id).map_err(|e| org_error(&e))?;
        Ok(reply(
            200,
            json!({
                "user": me.user,
                "auth_method": me.session.auth_method,
                "orgs": orgs,
            }),
        ))
    }

    fn create_org(&self) -> Answer {
        let me = self.signed_in()?;
        identity::session_is_global(&me.session).map_err(|e| org_error(&e))?;
        let body = self.req.json()?;
        let slug = required(&body, "slug")?;
        let name = required(&body, "name")?;
        let org = orgs::create_org(self.store, &me.actor(), slug, name, None)
            .map_err(|e| org_error(&e))?;
        Ok(reply(200, json!(org)))
    }

    fn accept_invite(&self) -> Answer {
        let me = self.signed_in()?;
        identity::session_is_global(&me.session).map_err(|e| org_error(&e))?;
        let body = self.req.json()?;
        let token = required(&body, "token")?;
        let (org, role) =
            identity::accept_invite(self.store, &me.user.id, token).map_err(|e| org_error(&e))?;
        Ok(reply(200, json!({ "org": org, "role": role })))
    }

    // ── Organization routes ─────────────────────────────────────────────

    fn org_route(&self, org: &str, rest: &[&str]) -> Answer {
        let me = self.signed_in()?;
        identity::session_may_act_in(self.store, &me.session, org).map_err(|e| org_error(&e))?;
        let actor = me.actor();
        let store = self.store;
        let ok = |value: Value| Ok(reply(200, value));
        match (self.req.method, rest) {
            ("GET", []) => {
                let org = orgs::get_org(store, &actor, org).map_err(oe)?;
                let role = identity::user_orgs(store, &me.user.id)
                    .map_err(|e| org_error(&e))?
                    .into_iter()
                    .find(|o| o.org.id == org.id);
                ok(json!({ "org": org, "membership": role }))
            }
            ("GET", ["members"]) => match orgs::list_members(store, &actor, org) {
                Ok(members) => ok(json!({ "members": members })),
                Err(e) => Err(oe(e)),
            },
            ("POST", ["invites"]) => self.invite(&actor, org),
            ("POST", ["members", user, "role"]) => {
                let role = role_field(&self.req.json()?)?;
                orgs::set_member_role(store, &actor, org, user, role).map_err(oe)?;
                ok(json!({ "user_id": user, "role": role }))
            }
            ("DELETE", ["members", user]) => {
                // Safe default: a removed member's console keys stop working.
                let revoke_keys = self.req.query("revoke_keys").as_deref() != Some("false");
                let removed = identity::remove_member(store, &actor, org, user, revoke_keys)
                    .map_err(oe)?;
                for key in &removed.revoked_keys {
                    self.forget_key(key);
                }
                ok(json!({ "removed": true, "revoked_keys": removed.revoked_keys }))
            }
            ("GET", ["projects"]) => match orgs::list_projects(store, &actor, org) {
                Ok(projects) => ok(json!({ "projects": projects })),
                Err(e) => Err(oe(e)),
            },
            ("POST", ["projects"]) => {
                let body = self.req.json()?;
                let project = orgs::create_project(
                    store,
                    &actor,
                    org,
                    required(&body, "slug")?,
                    required(&body, "name")?,
                )
                .map_err(oe)?;
                ok(json!(project))
            }
            ("GET", ["tenants"]) => match orgs::list_tenants(store, &actor, org) {
                Ok(tenants) => ok(json!({ "tenants": tenants })),
                Err(e) => Err(oe(e)),
            },
            ("POST", ["tenants"]) => {
                let body = self.req.json()?;
                // A user never picks a tenant id (see `orgs::link_tenant`).
                if body.get("tenant_id").is_some() {
                    return Err(error(400, "a tenant id is generated; omit 'tenant_id'"));
                }
                let project = field(&body, "project_id")?;
                let tenant = orgs::link_tenant(store, &actor, org, None, project).map_err(oe)?;
                ok(json!(tenant))
            }
            ("GET", ["api-keys"]) => match identity::list_org_api_keys(store, &actor, org) {
                Ok(keys) => ok(json!({ "api_keys": keys })),
                Err(e) => Err(oe(e)),
            },
            ("POST", ["api-keys"]) => self.create_key(&actor, org),
            ("POST", ["api-keys", key_id, "revoke"]) => {
                let key = identity::revoke_org_api_key(store, &actor, org, key_id).map_err(oe)?;
                self.forget_key(&key.key_id);
                ok(json!({ "revoked": true, "key_id": key.key_id }))
            }
            ("GET", ["namespaces"]) => self.namespaces(&actor, org),
            ("GET", ["usage"]) => self.tenant_read(&actor, org, "usage"),
            ("GET", ["billing"]) => self.tenant_read(&actor, org, "billing"),
            ("GET", ["audit-log"]) => {
                let number = |name: &str| -> Result<Option<i64>, http::Response> {
                    self.req
                        .query(name)
                        .map(|v| v.parse::<i64>().map_err(|_| error(400, &format!("invalid '{name}'"))))
                        .transpose()
                };
                let limit = number("limit")?.map(|n| n.clamp(1, i64::from(orgs::MAX_LIST)) as u32);
                let before = number("before")?;
                let events = orgs::list_audit(store, &actor, org, limit, before).map_err(oe)?;
                let page = limit.unwrap_or(orgs::DEFAULT_AUDIT_PAGE);
                let next = events
                    .last()
                    .filter(|_| events.len() as u32 == page)
                    .map(|e| e.id);
                ok(json!({ "events": events, "next_before": next }))
            }
            ("GET", ["sso"]) => self.get_sso(&actor, org),
            ("POST", ["sso"]) => self.put_sso(&actor, org),
            ("DELETE", ["sso"]) => {
                let removed = identity::delete_sso_config(store, &actor, org).map_err(oe)?;
                ok(json!({ "removed": removed }))
            }
            _ => Err(error(404, "not found")),
        }
    }

    fn invite(&self, actor: &Actor, org: &str) -> Answer {
        let body = self.req.json()?;
        let email = required(&body, "email")?;
        let role = role_field(&body)?;
        limit(self.console, &format!("invite-org:{org}"), 50, 3600)?;
        let invite =
            identity::invite_member(self.store, actor, org, email, role).map_err(|e| org_error(&e))?;
        self.mail(
            &invite.email,
            &format!("You are invited to {} on Kosha", invite.org.name),
            &format!(
                "You have been invited to join the organization \"{}\" on Kosha as {}.\n\n\
                 Open this link to accept (it works for 7 days, once). If you have no \
                 account yet, you can create one with this address from the same page:\n\n\
                 {}\n",
                invite.org.name,
                invite.role.as_str(),
                self.link("invite", &invite.token)
            ),
        );
        Ok(reply(
            200,
            json!({
                "invited": invite.email,
                "role": invite.role,
                "expires_at_ms": invite.expires_at_ms,
            }),
        ))
    }

    fn create_key(&self, actor: &Actor, org: &str) -> Answer {
        let body = self.req.json()?;
        let tenant = required(&body, "tenant_id")?;
        let description = field(&body, "description")?.unwrap_or("");
        let scopes = match body.get("scopes") {
            None | Some(Value::Null) => Scopes::ALL,
            Some(Value::Array(names)) => {
                let names: Option<Vec<&str>> = names.iter().map(Value::as_str).collect();
                let names = names.ok_or_else(|| error(400, "'scopes' must be strings"))?;
                Scopes::parse(names).map_err(|e| error(400, &e))?
            }
            Some(_) => return Err(error(400, "'scopes' must be an array")),
        };
        let expires_at = match body.get("expires_in_days") {
            None | Some(Value::Null) => None,
            Some(v) => {
                let days = v
                    .as_i64()
                    .filter(|d| (1..=3650).contains(d))
                    .ok_or_else(|| error(400, "'expires_in_days' must be 1-3650"))?;
                Some(orgs_now_ms() / 1000 + days * 86_400)
            }
        };
        let Some(keys) = self.key_table() else {
            return Err(error(400, "API keys are disabled (KOSHA_AUTH=disabled)"));
        };
        let created = identity::create_org_api_key(
            self.store,
            actor,
            org,
            tenant,
            description,
            scopes,
            expires_at,
        )
        .map_err(|e| org_error(&e))?;
        // Works on this pod at once; other pods pick it up on refresh.
        keys.upsert_db_key(kosha_control::ApiKeyRecord {
            key_hash: created.listed.key_id.clone(),
            tenant_id: created.listed.tenant_id.clone(),
            description: created.listed.description.clone(),
            scopes,
            namespaces: None,
            expires_at,
            last_used_at: None,
            created_at: crate::auth::unix_now(),
        });
        Ok(reply(
            200,
            json!({
                // The only time the key is shown.
                "api_key": created.key.api_key,
                "key": created.listed,
            }),
        ))
    }

    fn key_table(&self) -> Option<&crate::auth::KeyStore> {
        match self.state.auth() {
            crate::auth::Auth::Keys(keys) => Some(keys),
            crate::auth::Auth::Disabled => None,
        }
    }

    fn forget_key(&self, key_hash: &str) {
        if let Some(keys) = self.key_table() {
            keys.remove_db_key(key_hash);
        }
    }

    /// The organization's tenant ids, after checking the actor may view it.
    fn tenants(&self, actor: &Actor, org: &str) -> Result<Vec<String>, http::Response> {
        Ok(orgs::list_tenants(self.store, actor, org)
            .map_err(|e| org_error(&e))?
            .into_iter()
            .map(|t| t.tenant_id)
            .collect())
    }

    /// Every namespace of every tenant of the organization, with document
    /// counts (from the manifests) and stored bytes.
    fn namespaces(&self, actor: &Actor, org: &str) -> Answer {
        let mut out = Vec::new();
        for tenant in self.tenants(actor, org)? {
            let listed = crate::route(
                "GET /v1/namespaces HTTP/1.1",
                &HashMap::new(),
                b"",
                &tenant,
                self.state,
            );
            let body: Value = listed
                .split_once("\r\n\r\n")
                .and_then(|(_, b)| serde_json::from_str(b).ok())
                .unwrap_or(Value::Null);
            for ns in body["namespaces"].as_array().into_iter().flatten() {
                let name = ns["name"].as_str().unwrap_or_default();
                let id = kosha_core::NamespaceId(crate::tenant_namespace(&tenant, name));
                out.push(json!({
                    "tenant_id": tenant,
                    "name": name,
                    "documents": ns["documents"],
                    "segments": ns["segments"],
                    "stored_bytes": crate::usage::namespace_stored_bytes(self.state, &id),
                }));
            }
        }
        Ok(reply(200, json!({ "namespaces": out })))
    }

    /// `GET /v1/usage` or `GET /v1/billing` for one of the organization's
    /// tenants (`?tenant=`), answered by the existing tenant routes.
    fn tenant_read(&self, actor: &Actor, org: &str, what: &str) -> Answer {
        let tenant = self
            .req
            .query("tenant")
            .ok_or_else(|| error(400, "missing '?tenant='"))?;
        if !self.tenants(actor, org)?.contains(&tenant) {
            return Err(error(404, "no such tenant in this organization"));
        }
        let mut line = format!("GET /v1/{what}");
        if what == "usage" {
            let window: Vec<String> = ["from", "to"]
                .iter()
                .filter_map(|k| {
                    let v = self.req.query(k)?;
                    v.bytes().all(|b| b.is_ascii_digit()).then(|| format!("{k}={v}"))
                })
                .collect();
            if !window.is_empty() {
                line.push('?');
                line.push_str(&window.join("&"));
            }
        }
        line.push_str(" HTTP/1.1");
        let answer = crate::route(&line, &HashMap::new(), b"", &tenant, self.state);
        let (head, body) = answer.split_once("\r\n\r\n").unwrap_or(("", "{}"));
        let status: u16 = head
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);
        let mut value: Value = serde_json::from_str(body).unwrap_or(Value::Null);
        if what == "billing" && status == 200 {
            value["actions"] = billing_actions(self.state, org, &tenant)
                .unwrap_or(Value::Null);
        }
        Ok(reply(status, value))
    }

    fn sso_view(&self, config: Option<identity::SsoConfig>) -> Value {
        match config {
            None => json!({
                "configured": false,
                "redirect_uri": self.redirect_uri("sso"),
            }),
            Some(c) => json!({
                "configured": true,
                "issuer": c.issuer,
                "client_id": c.client_id,
                "client_secret_set": true,
                "allowed_domains": c.allowed_domains,
                "sso_required": c.sso_required,
                "updated_at_ms": c.updated_at_ms,
                "redirect_uri": self.redirect_uri("sso"),
            }),
        }
    }

    fn get_sso(&self, actor: &Actor, org: &str) -> Answer {
        let config = identity::get_sso_config(self.store, actor, org).map_err(|e| org_error(&e))?;
        Ok(reply(200, self.sso_view(config)))
    }

    fn put_sso(&self, actor: &Actor, org: &str) -> Answer {
        let key = self.console.secret_key.as_ref().ok_or_else(|| {
            error(
                400,
                "single sign-on needs KOSHA_CONSOLE_SECRET_KEY on the server (it seals the \
                 client secret)",
            )
        })?;
        let body = self.req.json()?;
        let issuer = required(&body, "issuer")?;
        let client_id = required(&body, "client_id")?;
        let secret = field(&body, "client_secret")?.filter(|s| !s.is_empty());
        let domains = match body.get("allowed_domains") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(a)) => a
                .iter()
                .map(|d| d.as_str().map(str::to_string))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| error(400, "'allowed_domains' must be strings"))?,
            Some(_) => return Err(error(400, "'allowed_domains' must be an array")),
        };
        let required_sso = match body.get("sso_required") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => return Err(error(400, "'sso_required' must be a boolean")),
        };
        // Authorize before contacting the issuer, so a non-admin cannot make
        // the server fetch URLs of their choosing.
        identity::get_sso_config(self.store, actor, org).map_err(|e| org_error(&e))?;
        self.console
            .oidc
            .discover(issuer.trim().trim_end_matches('/'))
            .map_err(|e| error(400, &format!("the issuer's discovery document: {e}")))?;
        let config = identity::put_sso_config(
            self.store,
            actor,
            org,
            &SsoSettings {
                issuer: issuer.to_string(),
                client_id: client_id.to_string(),
                client_secret_sealed: secret.map(|s| key.seal(org, s)),
                allowed_domains: domains,
                sso_required: required_sso,
            },
        )
        .map_err(|e| org_error(&e))?;
        Ok(reply(200, self.sso_view(Some(config))))
    }
}

fn provider_down(e: &str) -> http::Response {
    eprintln!("WARN: console: identity provider: {e}");
    error(502, "the identity provider could not be reached; try again later")
}

fn orgs_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

// ── Integration hooks ───────────────────────────────────────────────────────

/// Whether the console can offer Checkout/Portal buttons.
///
/// TODO(#349, Stripe Checkout/Portal): return true once
/// [`billing_actions`] returns links.
pub(crate) fn billing_actions_available() -> bool {
    false
}

/// What the console's plan page can do for `tenant_id` of `org_id`, added
/// as `actions` to `GET /v1/console/orgs/{org}/billing`. The UI shows a
/// "Manage billing" placeholder while this is `None`.
///
/// TODO(#349, Stripe Checkout/Portal): return e.g.
/// `{"checkout_url": ..., "portal_url": ...}` from the billing provider
/// (and flip [`billing_actions_available`]). Only owners and admins should
/// get a portal link; the caller has already checked the user may view the
/// organization.
pub(crate) fn billing_actions(_state: &AppState, _org_id: &str, _tenant_id: &str) -> Option<Value> {
    None
}

/// Record a data-plane event (a namespace deleted, a tenant deleted) in the
/// audit log of the organization `tenant` belongs to. Best effort: the
/// event has already happened, so a failed write is logged, not returned.
/// Nothing is written for a tenant no organization owns.
pub(crate) fn audit_tenant_event(
    state: &AppState,
    tenant: &str,
    actor: &str,
    action: &'static str,
    target_type: &'static str,
    target_id: &str,
    detail: Value,
) {
    let result = state
        .orgs
        .identity_store(state.auth())
        .map_err(OrgError::Store)
        .and_then(|store| {
            identity::record_tenant_event(
                &*store,
                tenant,
                actor,
                action,
                target_type,
                target_id,
                detail,
            )
        });
    if let Err(e) = result {
        eprintln!("WARN: audit: could not record {action} for tenant {tenant}: {e}");
    }
}

#[cfg(test)]
#[path = "console_tests.rs"]
mod tests;
