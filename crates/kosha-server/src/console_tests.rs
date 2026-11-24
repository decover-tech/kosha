//! The console (#345 part 2) through the server's real `dispatch`: a
//! browser with a cookie jar, a capturing mailer, fake identity providers,
//! and a real API-key table, so a key the console hands out is proven to
//! work on the data plane.

use super::*;
use crate::console_oauth::tests::{FakeHttp, TestKey};
use crate::mailer::CapturingMailer;
use crate::{auth, http, org_admin::OrgBackend};

const PUBLIC: &str = "https://console.example";
const PASSWORD: &str = "correct horse battery";

/// One server with the console on, sign-up open, in-memory control plane
/// and its own key table.
struct Server {
    state: AppState,
    mail: Arc<CapturingMailer>,
}

impl Server {
    fn new() -> Self {
        Self::with(|console| console)
    }

    fn with(configure: impl FnOnce(Console) -> Console) -> Self {
        let mut state = AppState::new(crate::tests::test_dir());
        state.admin_auth_key = Some("admin-secret".into());
        state.orgs = OrgBackend::in_memory();
        state.auth_override = Some(auth::Auth::Keys(auth::KeyStore::with_keys(HashMap::new())));
        let mail = Arc::new(CapturingMailer::default());
        state.console = configure(Console::for_tests(PUBLIC, mail.clone()));
        Server { state, mail }
    }

    fn browser(&self) -> Browser<'_> {
        Browser {
            server: self,
            cookies: HashMap::new(),
            ip: "203.0.113.7".into(),
        }
    }

    /// The token in the last mail to `to` whose link goes to `page`.
    fn token_from_mail(&self, to: &str, page: &str) -> String {
        let marker = format!("{PUBLIC}/console#/{page}?token=");
        let (_, _, body) = self
            .mail
            .sent()
            .into_iter()
            .rev()
            .find(|(t, _, b)| t == to && b.contains(&marker))
            .unwrap_or_else(|| panic!("no {page} mail to {to}: {:?}", self.mail.sent()));
        let at = body.find(&marker).unwrap() + marker.len();
        body[at..]
            .chars()
            .take_while(char::is_ascii_hexdigit)
            .collect()
    }

    fn mails_to(&self, to: &str) -> Vec<(String, String)> {
        self.mail
            .sent()
            .into_iter()
            .filter(|(t, _, _)| t == to)
            .map(|(_, s, b)| (s, b))
            .collect()
    }

    /// A data-plane request with an API key.
    fn data(&self, key: &str, method: &str, path: &str, body: Value) -> (u16, Value) {
        let headers = HashMap::from([("authorization".to_string(), format!("Bearer {key}"))]);
        let body = if body.is_null() {
            Vec::new()
        } else {
            body.to_string().into_bytes()
        };
        send(&self.state, method, path, headers, body).0
    }
}

fn send(
    state: &AppState,
    method: &str,
    target: &str,
    headers: HashMap<String, String>,
    body: Vec<u8>,
) -> ((u16, Value), Vec<String>, Option<String>) {
    let response = crate::dispatch(
        state,
        http::Request {
            method: method.into(),
            target: target.into(),
            headers,
            body,
        },
    );
    let raw = response.into_raw_string();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    let mut cookies = Vec::new();
    let mut location = None;
    for line in head.lines().skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            match k.trim().to_ascii_lowercase().as_str() {
                "set-cookie" => cookies.push(v.trim().to_string()),
                "location" => location = Some(v.trim().to_string()),
                _ => {}
            }
        }
    }
    let value = serde_json::from_str(body).unwrap_or(Value::String(body.to_string()));
    ((status, value), cookies, location)
}

/// A browser: keeps cookies, sends the CSRF header and its Origin.
struct Browser<'a> {
    server: &'a Server,
    cookies: HashMap<String, String>,
    ip: String,
}

impl Browser<'_> {
    fn request(&mut self, method: &str, path: &str, body: Value) -> (u16, Value, Option<String>) {
        let mut headers = HashMap::from([
            (CSRF_HEADER.to_string(), "1".to_string()),
            ("origin".to_string(), PUBLIC.to_string()),
            (http::PEER_IP_HEADER.to_string(), self.ip.clone()),
        ]);
        if !self.cookies.is_empty() {
            let jar: Vec<String> = self
                .cookies
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect();
            headers.insert("cookie".into(), jar.join("; "));
        }
        let body = if body.is_null() {
            Vec::new()
        } else {
            body.to_string().into_bytes()
        };
        let ((status, value), set, location) =
            send(&self.server.state, method, path, headers, body);
        for cookie in set {
            assert!(
                cookie.contains("HttpOnly") && cookie.contains("Secure"),
                "{cookie}"
            );
            assert!(cookie.contains("SameSite=Lax"), "{cookie}");
            let pair = cookie.split(';').next().unwrap();
            let (name, value) = pair.split_once('=').unwrap();
            if value.is_empty() {
                self.cookies.remove(name);
            } else {
                self.cookies.insert(name.to_string(), value.to_string());
            }
        }
        (status, value, location)
    }

    fn call(&mut self, method: &str, path: &str, body: Value) -> (u16, Value) {
        let (status, value, _) = self.request(method, path, body);
        (status, value)
    }

    fn ok(&mut self, method: &str, path: &str, body: Value) -> Value {
        let (status, value) = self.call(method, path, body);
        assert!(
            status == 200 || status == 202,
            "{method} {path}: {status} {value}"
        );
        value
    }

    /// Sign up, verify from the mail, sign in.
    fn join(&mut self, email: &str) -> Value {
        let (status, body) = self.call(
            "POST",
            "/v1/console/signup",
            json!({ "email": email, "password": PASSWORD, "display_name": "Ada" }),
        );
        assert_eq!(status, 202, "{body}");
        let token = self.server.token_from_mail(email, "verify");
        self.ok(
            "POST",
            "/v1/console/verify-email",
            json!({ "token": token }),
        );
        self.ok(
            "POST",
            "/v1/console/login",
            json!({ "email": email, "password": PASSWORD }),
        )
    }
}

/// The "Done when" of #345: an outside user signs up, verifies the address
/// from the mail, signs in, creates an organization, project and tenant,
/// gets a key, and indexes and finds a document with it. Nobody touches a
/// database or holds the admin key.
#[test]
fn an_outside_user_goes_from_sign_up_to_a_search_hit() {
    let server = Server::new();
    let mut ada = server.browser();

    // Sign up; the verification link arrives by mail.
    let (status, body) = ada.call(
        "POST",
        "/v1/console/signup",
        json!({ "email": "Ada@Example.com", "password": PASSWORD, "display_name": "Ada" }),
    );
    assert_eq!(status, 202, "{body}");
    let token = server.token_from_mail("ada@example.com", "verify");
    assert_eq!(token.len(), 64);
    // Not before the address is verified.
    let (status, body) = ada.call(
        "POST",
        "/v1/console/login",
        json!({ "email": "ada@example.com", "password": PASSWORD }),
    );
    assert_eq!((status, &body["reason"]), (403, &json!("email_unverified")));
    ada.ok(
        "POST",
        "/v1/console/verify-email",
        json!({ "token": token }),
    );
    // The link works once.
    assert_eq!(
        ada.call(
            "POST",
            "/v1/console/verify-email",
            json!({ "token": token })
        )
        .0,
        400
    );

    // Sign in: a session cookie, nothing else.
    let login = ada.ok(
        "POST",
        "/v1/console/login",
        json!({ "email": "ada@example.com", "password": PASSWORD }),
    );
    assert_eq!(login["user"]["email"], json!("ada@example.com"));
    assert!(ada.cookies.contains_key(SESSION_COOKIE));
    let me = ada.ok("GET", "/v1/console/me", Value::Null);
    assert_eq!(me["orgs"], json!([]));

    // Organization (Ada owns it), project, tenant.
    let org = ada.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "acme", "name": "Acme" }),
    );
    let org_id = org["id"].as_str().unwrap().to_string();
    let base = format!("/v1/console/orgs/{org_id}");
    let me = ada.ok("GET", "/v1/console/me", Value::Null);
    assert_eq!(me["orgs"][0]["role"], json!("owner"));
    let project = ada.ok(
        "POST",
        &format!("{base}/projects"),
        json!({ "slug": "search", "name": "Search" }),
    );
    let tenant = ada.ok(
        "POST",
        &format!("{base}/tenants"),
        json!({ "project_id": project["id"] }),
    );
    let tenant_id = tenant["tenant_id"].as_str().unwrap().to_string();

    // A key, shown once.
    let created = ada.ok(
        "POST",
        &format!("{base}/api-keys"),
        json!({ "tenant_id": tenant_id, "description": "ci" }),
    );
    let api_key = created["api_key"].as_str().unwrap().to_string();
    assert!(api_key.starts_with("sk-"));
    let listed = ada.ok("GET", &format!("{base}/api-keys"), Value::Null);
    assert_eq!(listed["api_keys"].as_array().unwrap().len(), 1);
    assert!(!listed.to_string().contains(&api_key));

    // Index and search with that key, on the data plane.
    let doc = json!({ "documents": [{ "id": "d1", "fields": [
        { "name": "body", "field_type": "Text", "value": "kosha finds needles in haystacks" }
    ]}]});
    let (status, body) = server.data(&api_key, "POST", "/v1/namespaces/docs/documents", doc);
    assert_eq!(status, 200, "{body}");
    let (status, body) = server.data(&api_key, "POST", "/v1/namespaces/docs/flush", json!({}));
    assert_eq!(status, 200, "{body}");
    let (status, body) = server.data(
        &api_key,
        "POST",
        "/v1/namespaces/docs/search",
        json!({ "query_text": "needles", "top_k": 5, "no_cache": true }),
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["results"][0]["doc_id"], json!("d1"), "{body}");

    // The console lists the namespace with its document count.
    let namespaces = ada.ok("GET", &format!("{base}/namespaces"), Value::Null);
    assert_eq!(
        namespaces["namespaces"][0]["name"],
        json!("docs"),
        "{namespaces}"
    );
    assert_eq!(namespaces["namespaces"][0]["documents"], json!(1));
    assert_eq!(namespaces["namespaces"][0]["tenant_id"], json!(tenant_id));

    // A made-up key is refused, and a session never opens the data plane.
    assert_eq!(
        server
            .data("sk-made-up", "GET", "/v1/namespaces", Value::Null)
            .0,
        401
    );
    let cookie = format!("{SESSION_COOKIE}={}", ada.cookies[SESSION_COOKIE]);
    let ((status, _), _, _) = send(
        &server.state,
        "GET",
        "/v1/namespaces",
        HashMap::from([("cookie".to_string(), cookie)]),
        Vec::new(),
    );
    assert_eq!(status, 401);

    // The audit log tells the story, without the key.
    let log = ada.ok("GET", &format!("{base}/audit-log"), Value::Null);
    let actions: Vec<&str> = log["events"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert_eq!(
        actions,
        [
            "org.created",
            "project.created",
            "tenant.linked",
            "api_key.created"
        ]
    );
    assert!(!log.to_string().contains(&api_key));

    // Deleting the namespace with the tenant key is audited too.
    let (status, body) = server.data(&api_key, "DELETE", "/v1/namespaces/docs", Value::Null);
    assert_eq!(status, 200, "{body}");
    let log = ada.ok("GET", &format!("{base}/audit-log"), Value::Null);
    assert_eq!(log["events"][0]["action"], json!("namespace.deleted"));
    assert_eq!(log["events"][0]["target_id"], json!("docs"));
    assert_eq!(
        log["events"][0]["actor"],
        json!(format!("tenant:{tenant_id}"))
    );

    // Sign out: the cookie no longer works, even if replayed.
    let stolen = ada.cookies.clone();
    ada.ok("POST", "/v1/console/logout", Value::Null);
    assert!(!ada.cookies.contains_key(SESSION_COOKIE));
    ada.cookies = stolen;
    assert_eq!(ada.call("GET", "/v1/console/me", Value::Null).0, 401);
}

#[test]
fn nothing_reveals_whether_an_address_is_registered() {
    let server = Server::new();
    let mut ada = server.browser();
    ada.join("ada@example.com");
    let mut eve = server.browser();
    eve.ip = "198.51.100.9".into();

    // Sign-up: the same answer; the owner gets a heads-up instead.
    let a = eve.call(
        "POST",
        "/v1/console/signup",
        json!({ "email": "ada@example.com", "password": "another long password" }),
    );
    let b = eve.call(
        "POST",
        "/v1/console/signup",
        json!({ "email": "nobody@example.com", "password": "another long password" }),
    );
    assert_eq!(a, (202, b.1.clone()));
    let mails = server.mails_to("ada@example.com");
    assert_eq!(mails.last().unwrap().0, "Your Kosha account");

    // Login: one error for unknown address and wrong password.
    let unknown = eve.call(
        "POST",
        "/v1/console/login",
        json!({ "email": "ghost@example.com", "password": PASSWORD }),
    );
    let wrong = eve.call(
        "POST",
        "/v1/console/login",
        json!({ "email": "ada@example.com", "password": "not the password!" }),
    );
    assert_eq!(unknown, wrong);
    assert_eq!(unknown.0, 401);

    // Reset: the same answer, a mail only for the real one.
    let before = server.mail.sent().len();
    let a = eve.call(
        "POST",
        "/v1/console/password-reset",
        json!({ "email": "ada@example.com" }),
    );
    let b = eve.call(
        "POST",
        "/v1/console/password-reset",
        json!({ "email": "ghost@example.com" }),
    );
    assert_eq!(a, b);
    assert_eq!(a.0, 202);
    assert_eq!(server.mail.sent().len(), before + 1);
}

#[test]
fn password_reset_is_single_use_and_ends_sessions() {
    let server = Server::new();
    let mut ada = server.browser();
    ada.join("ada@example.com");
    let mut other = server.browser();
    other.ok(
        "POST",
        "/v1/console/login",
        json!({ "email": "ada@example.com", "password": PASSWORD }),
    );

    ada.ok(
        "POST",
        "/v1/console/password-reset",
        json!({ "email": "ada@example.com" }),
    );
    let token = server.token_from_mail("ada@example.com", "reset");
    assert_eq!(
        ada.call(
            "POST",
            "/v1/console/password-reset/confirm",
            json!({ "token": token, "password": "short" })
        )
        .0,
        400
    );
    ada.ok(
        "POST",
        "/v1/console/password-reset/confirm",
        json!({ "token": token, "password": "a brand new passphrase" }),
    );
    assert_eq!(
        ada.call(
            "POST",
            "/v1/console/password-reset/confirm",
            json!({ "token": token, "password": "yet another passphrase" })
        )
        .0,
        400
    );
    // Every session of the user ended; only the new password works.
    assert_eq!(other.call("GET", "/v1/console/me", Value::Null).0, 401);
    assert_eq!(ada.call("GET", "/v1/console/me", Value::Null).0, 401);
    assert_eq!(
        ada.call(
            "POST",
            "/v1/console/login",
            json!({ "email": "ada@example.com", "password": PASSWORD })
        )
        .0,
        401
    );
    ada.ok(
        "POST",
        "/v1/console/login",
        json!({ "email": "ada@example.com", "password": "a brand new passphrase" }),
    );
}

#[test]
fn state_changing_requests_need_the_csrf_header_and_our_origin() {
    let server = Server::new();
    let mut ada = server.browser();
    ada.join("ada@example.com");
    let cookie = format!("{SESSION_COOKIE}={}", ada.cookies[SESSION_COOKIE]);
    let body = json!({ "slug": "acme", "name": "Acme" })
        .to_string()
        .into_bytes();
    let attempt = |extra: &[(&str, &str)]| {
        let mut headers = HashMap::from([("cookie".to_string(), cookie.clone())]);
        for (k, v) in extra {
            headers.insert(k.to_string(), v.to_string());
        }
        send(
            &server.state,
            "POST",
            "/v1/console/orgs",
            headers,
            body.clone(),
        )
        .0
         .0
    };
    assert_eq!(attempt(&[]), 403, "no header");
    assert_eq!(
        attempt(&[(CSRF_HEADER, "1"), ("origin", "https://evil.example")]),
        403
    );
    assert_eq!(
        attempt(&[(CSRF_HEADER, "1"), ("sec-fetch-site", "cross-site")]),
        403
    );
    assert_eq!(attempt(&[(CSRF_HEADER, "1"), ("origin", PUBLIC)]), 200);
    // Reads need a session, not the header.
    let ((status, _), _, _) = send(
        &server.state,
        "GET",
        "/v1/console/me",
        HashMap::new(),
        Vec::new(),
    );
    assert_eq!(status, 401);
}

#[test]
fn sign_up_is_off_by_default_and_can_be_limited_to_domains() {
    let closed = Server::with(|c| c.with_signup(false, &[]));
    let mut b = closed.browser();
    let (status, _) = b.call(
        "POST",
        "/v1/console/signup",
        json!({ "email": "ada@example.com", "password": PASSWORD }),
    );
    assert_eq!(status, 403);
    assert!(closed.mail.sent().is_empty());
    assert_eq!(
        b.call("GET", "/v1/console/config", Value::Null).1["signup"],
        json!(false)
    );

    let corp = Server::with(|c| c.with_signup(true, &["corp.example"]));
    let mut b = corp.browser();
    let refused = b.call(
        "POST",
        "/v1/console/signup",
        json!({ "email": "ada@example.com", "password": PASSWORD }),
    );
    assert_eq!(refused.0, 403);
    let accepted = b.call(
        "POST",
        "/v1/console/signup",
        json!({ "email": "ada@corp.example", "password": PASSWORD }),
    );
    assert_eq!(accepted.0, 202);
    // Passwords below the minimum are refused.
    let short = b.call(
        "POST",
        "/v1/console/signup",
        json!({ "email": "bob@corp.example", "password": "short" }),
    );
    assert_eq!(short.0, 400);
}

#[test]
fn login_attempts_are_limited_per_address() {
    let server = Server::new();
    let mut eve = server.browser();
    let mut statuses = Vec::new();
    for i in 0..12 {
        // A new IP each time: the per-address limit still applies.
        eve.ip = format!("192.0.2.{i}");
        statuses.push(
            eve.call(
                "POST",
                "/v1/console/login",
                json!({ "email": "ada@example.com", "password": "guess guess guess" }),
            )
            .0,
        );
    }
    assert_eq!(&statuses[..10], &[401; 10]);
    assert_eq!(&statuses[10..], &[429, 429]);
}

#[test]
fn the_console_is_off_unless_enabled() {
    let mut state = AppState::new(crate::tests::test_dir());
    state.console = Console::disabled();
    for (method, path) in [
        ("GET", "/console"),
        ("GET", "/console/app.js"),
        ("GET", "/v1/console/config"),
        ("POST", "/v1/console/login"),
    ] {
        let ((status, _), _, _) = send(&state, method, path, HashMap::new(), Vec::new());
        assert_eq!(status, 404, "{method} {path}");
    }

    let server = Server::new();
    let response = crate::dispatch(
        &server.state,
        http::Request {
            method: "GET".into(),
            target: "/console".into(),
            headers: HashMap::new(),
            body: Vec::new(),
        },
    );
    assert_eq!(response.status(), 200);
    let csp = response.header("content-security-policy").unwrap();
    assert!(csp.starts_with("default-src 'self'"), "{csp}");
    assert!(csp.contains("frame-ancestors 'none'"));
    assert_eq!(response.header("x-frame-options"), Some("DENY"));
}

#[test]
fn console_assets_load_nothing_from_elsewhere() {
    for asset in [INDEX_HTML, APP_JS, STYLE_CSS] {
        assert!(!asset.contains("http://") && !asset.contains("https://"));
    }
    // CSP `default-src 'self'` blocks inline code.
    assert!(!INDEX_HTML.contains("<script>"));
    assert!(!INDEX_HTML.contains("<style"));
    assert!(!INDEX_HTML.contains(" style="));
    assert!(
        !APP_JS.contains("innerHTML"),
        "build DOM nodes, not HTML strings"
    );
    assert!(!APP_JS.contains("eval("));
}

/// Invites work while open sign-up is off; removing a member revokes the
/// keys they created unless asked not to.
#[test]
fn invited_members_join_and_their_keys_go_when_they_leave() {
    let server = Server::with(|c| c.with_signup(false, &[]));
    // The first user comes from the operator (sign-up is off) and sets a
    // password with a reset link.
    let admin = HashMap::from([(
        "authorization".to_string(),
        "Bearer admin-secret".to_string(),
    )]);
    let ((status, _), _, _) = send(
        &server.state,
        "POST",
        "/v1/admin/users",
        admin,
        json!({ "email": "ada@example.com" })
            .to_string()
            .into_bytes(),
    );
    assert_eq!(status, 200);
    let mut ada = server.browser();
    ada.ok(
        "POST",
        "/v1/console/password-reset",
        json!({ "email": "ada@example.com" }),
    );
    let token = server.token_from_mail("ada@example.com", "reset");
    ada.ok(
        "POST",
        "/v1/console/password-reset/confirm",
        json!({ "token": token, "password": PASSWORD }),
    );
    ada.ok(
        "POST",
        "/v1/console/login",
        json!({ "email": "ada@example.com", "password": PASSWORD }),
    );
    let org = ada.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "acme", "name": "Acme" }),
    );
    let base = format!("/v1/console/orgs/{}", org["id"].as_str().unwrap());
    let tenant = ada.ok("POST", &format!("{base}/tenants"), Value::Null);
    let tenant_id = tenant["tenant_id"].as_str().unwrap();

    for (email, keep_keys) in [("bob@example.com", false), ("cy@example.com", true)] {
        ada.ok(
            "POST",
            &format!("{base}/invites"),
            json!({ "email": email, "role": "member" }),
        );
        let invite = server.token_from_mail(email, "invite");
        let mut member = server.browser();
        // Sign-up is closed, except for the invited address.
        let (status, _) = member.call(
            "POST",
            "/v1/console/signup",
            json!({ "email": "stranger@example.com", "password": PASSWORD, "invite_token": invite }),
        );
        assert_eq!(status, 403);
        let (status, body) = member.call(
            "POST",
            "/v1/console/signup",
            json!({ "email": email, "password": PASSWORD, "invite_token": invite }),
        );
        assert_eq!(status, 202, "{body}");
        let verify = server.token_from_mail(email, "verify");
        member.ok(
            "POST",
            "/v1/console/verify-email",
            json!({ "token": verify }),
        );
        member.ok(
            "POST",
            "/v1/console/login",
            json!({ "email": email, "password": PASSWORD }),
        );
        let joined = member.ok(
            "POST",
            "/v1/console/invites/accept",
            json!({ "token": invite }),
        );
        assert_eq!(joined["role"], json!("member"));
        assert_eq!(
            member
                .call(
                    "POST",
                    "/v1/console/invites/accept",
                    json!({ "token": invite })
                )
                .0,
            400,
            "an invite works once"
        );
        let user_id = member.ok("GET", "/v1/console/me", Value::Null)["user"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        // A member creates a key; they cannot revoke Ada's, Ada can revoke theirs.
        let key = member.ok(
            "POST",
            &format!("{base}/api-keys"),
            json!({ "tenant_id": tenant_id, "scopes": ["read", "write"] }),
        );
        let api_key = key["api_key"].as_str().unwrap().to_string();
        assert_eq!(key["key"]["created_by"], json!(user_id));
        assert_eq!(key["key"]["scopes"], json!(["read", "write"]));
        let adas = ada.ok(
            "POST",
            &format!("{base}/api-keys"),
            json!({ "tenant_id": tenant_id }),
        );
        let adas_id = adas["key"]["key_id"].as_str().unwrap();
        assert_eq!(
            member
                .call(
                    "POST",
                    &format!("{base}/api-keys/{adas_id}/revoke"),
                    Value::Null
                )
                .0,
            403
        );
        // Members cannot manage members or read the audit log.
        assert_eq!(
            member
                .call("GET", &format!("{base}/audit-log"), Value::Null)
                .0,
            403
        );
        assert_eq!(
            server
                .data(&api_key, "GET", "/v1/namespaces", Value::Null)
                .0,
            200
        );

        let query = if keep_keys { "?revoke_keys=false" } else { "" };
        let removed = ada.ok(
            "DELETE",
            &format!("{base}/members/{user_id}{query}"),
            Value::Null,
        );
        let status = server
            .data(&api_key, "GET", "/v1/namespaces", Value::Null)
            .0;
        if keep_keys {
            assert_eq!(removed["revoked_keys"], json!([]));
            assert_eq!(status, 200);
        } else {
            assert_eq!(removed["revoked_keys"].as_array().unwrap().len(), 1);
            assert_eq!(status, 401, "a removed member's key stops working");
        }
        assert_eq!(member.call("GET", &base, Value::Null).0, 403);
    }
}

#[test]
fn roles_bound_what_the_console_can_do() {
    let server = Server::new();
    let mut ada = server.browser();
    ada.join("ada@example.com");
    let mut ro = server.browser();
    ro.join("ro@example.com");
    let org = ada.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "acme", "name": "Acme" }),
    );
    let base = format!("/v1/console/orgs/{}", org["id"].as_str().unwrap());
    let tenant = ada.ok("POST", &format!("{base}/tenants"), Value::Null);
    // An outsider sees nothing.
    assert_eq!(ro.call("GET", &base, Value::Null).0, 403);
    assert_eq!(
        ro.call("GET", &format!("{base}/api-keys"), Value::Null).0,
        403
    );
    ada.ok(
        "POST",
        &format!("{base}/invites"),
        json!({ "email": "ro@example.com", "role": "read-only" }),
    );
    let invite = server.token_from_mail("ro@example.com", "invite");
    ro.ok(
        "POST",
        "/v1/console/invites/accept",
        json!({ "token": invite }),
    );
    // Read-only reads, never writes.
    ro.ok("GET", &format!("{base}/api-keys"), Value::Null);
    ro.ok("GET", &format!("{base}/members"), Value::Null);
    for (method, path, body) in [
        (
            "POST",
            format!("{base}/api-keys"),
            json!({ "tenant_id": tenant["tenant_id"] }),
        ),
        ("POST", format!("{base}/tenants"), Value::Null),
        (
            "POST",
            format!("{base}/projects"),
            json!({ "slug": "p", "name": "P" }),
        ),
        (
            "POST",
            format!("{base}/invites"),
            json!({ "email": "x@example.com", "role": "member" }),
        ),
        ("GET", format!("{base}/sso"), Value::Null),
    ] {
        assert_eq!(ro.call(method, &path, body).0, 403, "{method} {path}");
    }
    // Users never pick tenant ids.
    assert_eq!(
        ada.call(
            "POST",
            &format!("{base}/tenants"),
            json!({ "tenant_id": "victim" })
        )
        .0,
        400
    );
}

// ── Providers ───────────────────────────────────────────────────────────────

fn query_param(url: &str, name: &str) -> String {
    let query = url.split_once('?').unwrap().1;
    query
        .split('&')
        .find_map(|p| p.strip_prefix(&format!("{name}=")))
        .map(url_decode)
        .unwrap_or_else(|| panic!("no {name} in {url}"))
}

/// A fake OIDC provider whose token endpoint checks PKCE and returns an ID
/// token for `who` with the nonce of the authorization request.
struct FakeIdp {
    http: FakeHttp,
    /// (code_challenge, nonce) of the last authorization request.
    expect: Arc<Mutex<(String, String)>>,
    who: Arc<Mutex<Value>>,
}

impl FakeIdp {
    fn start(client_id: &'static str) -> FakeIdp {
        Self::start_with(client_id, "")
    }

    /// `issuer_suffix`: appended to the declared issuer (Auth0 declares
    /// `https://tenant.auth0.com/`, with the slash).
    fn start_with(client_id: &'static str, issuer_suffix: &'static str) -> FakeIdp {
        let key = Arc::new(TestKey::new("k1"));
        let expect = Arc::new(Mutex::new((String::new(), String::new())));
        let who = Arc::new(Mutex::new(json!({})));
        let issuer = Arc::new(Mutex::new(String::new()));
        let (e, w, i, k) = (expect.clone(), who.clone(), issuer.clone(), key.clone());
        let http = FakeHttp::start(Arc::new(move |method, path, headers, body| {
            let base = i.lock().unwrap().clone();
            let iss = format!("{base}{issuer_suffix}");
            match (method, path) {
                ("GET", "/.well-known/openid-configuration") => (
                    200,
                    json!({
                        "issuer": iss,
                        "authorization_endpoint": format!("{base}/authorize"),
                        "token_endpoint": format!("{base}/token"),
                        "jwks_uri": format!("{base}/jwks"),
                    })
                    .to_string(),
                ),
                ("GET", "/jwks") => (200, json!({ "keys": [k.jwk()] }).to_string()),
                ("POST", "/token") => {
                    let (challenge, nonce) = e.lock().unwrap().clone();
                    let verifier = body
                        .split('&')
                        .find_map(|p| p.strip_prefix("code_verifier="))
                        .unwrap_or("");
                    if crate::console_crypto::pkce_challenge(verifier) != challenge {
                        return (400, json!({ "error": "invalid_grant" }).to_string());
                    }
                    // Client authentication (Basic).
                    assert!(headers.contains_key("authorization"), "{headers:?}");
                    let mut claims = w.lock().unwrap().clone();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    claims["iss"] = json!(iss);
                    claims["aud"] = json!(client_id);
                    claims["nonce"] = json!(nonce);
                    claims["iat"] = json!(now);
                    claims["exp"] = json!(now + 300);
                    (200, json!({ "id_token": k.id_token(&claims) }).to_string())
                }
                _ => (404, "{}".into()),
            }
        }));
        *issuer.lock().unwrap() = http.url.clone();
        FakeIdp { http, expect, who }
    }

    /// Run a browser through start → provider → callback.
    fn sign_in(&self, browser: &mut Browser<'_>, start: &str, who: Value) -> (u16, Option<String>) {
        *self.who.lock().unwrap() = who;
        let (status, body, location) = browser.request("GET", start, Value::Null);
        assert_eq!(status, 302, "{body}");
        let location = location.unwrap();
        assert!(
            location.starts_with(&format!("{}/authorize?", self.http.url)),
            "{location}"
        );
        assert_eq!(query_param(&location, "code_challenge_method"), "S256");
        *self.expect.lock().unwrap() = (
            query_param(&location, "code_challenge"),
            query_param(&location, "nonce"),
        );
        let state = query_param(&location, "state");
        let callback = query_param(&location, "redirect_uri");
        let path = callback.strip_prefix(PUBLIC).unwrap();
        let (status, _, location) = browser.request(
            "GET",
            &format!("{path}?code=c-1&state={state}"),
            Value::Null,
        );
        (status, location)
    }
}

fn person(sub: &str, email: &str) -> Value {
    json!({ "sub": sub, "email": email, "email_verified": true, "name": "Pat" })
}

#[test]
fn google_sign_in_creates_or_links_users_by_verified_email() {
    let idp = FakeIdp::start("google-client");
    let issuer = idp.http.url.clone();
    let server = Server::with(|c| {
        c.with_google(crate::console_oauth::Google {
            client_id: "google-client".into(),
            client_secret: "google-secret".into(),
            issuer,
        })
    });
    assert_eq!(
        server
            .browser()
            .ok("GET", "/v1/console/config", Value::Null)["providers"]["google"],
        json!(true)
    );

    // A new address signs up through Google.
    let mut pat = server.browser();
    let (status, location) = idp.sign_in(
        &mut pat,
        "/v1/console/oauth/google/start",
        person("g-1", "pat@example.com"),
    );
    assert_eq!((status, location.as_deref()), (302, Some("/console#/")));
    let me = pat.ok("GET", "/v1/console/me", Value::Null);
    assert_eq!(me["user"]["email"], json!("pat@example.com"));
    assert_eq!(me["auth_method"], json!("google"));
    assert!(
        !pat.cookies.contains_key("kosha_oauth"),
        "flow cookie cleared"
    );

    // Someone registered ada@ with a password but never verified it; Ada
    // then signs in with Google: she gets the account, they lose the password.
    let mut squatter = server.browser();
    squatter.call(
        "POST",
        "/v1/console/signup",
        json!({ "email": "ada@example.com", "password": "squatter password" }),
    );
    let mut ada = server.browser();
    idp.sign_in(
        &mut ada,
        "/v1/console/oauth/google/start",
        person("g-2", "ada@example.com"),
    );
    assert_eq!(
        ada.ok("GET", "/v1/console/me", Value::Null)["user"]["email"],
        json!("ada@example.com")
    );
    let verify = server.token_from_mail("ada@example.com", "verify");
    squatter.call(
        "POST",
        "/v1/console/verify-email",
        json!({ "token": verify }),
    );
    assert_eq!(
        squatter
            .call(
                "POST",
                "/v1/console/login",
                json!({ "email": "ada@example.com", "password": "squatter password" })
            )
            .0,
        401
    );

    // An unverified address at the provider is refused.
    let mut eve = server.browser();
    let mut unverified = person("g-3", "eve@example.com");
    unverified["email_verified"] = json!(false);
    let (_, location) = idp.sign_in(&mut eve, "/v1/console/oauth/google/start", unverified);
    assert!(location.unwrap().starts_with("/console#/login?error="));
    assert_eq!(eve.call("GET", "/v1/console/me", Value::Null).0, 401);
}

#[test]
fn a_callback_must_come_back_to_the_browser_that_started_it() {
    let idp = FakeIdp::start("google-client");
    let issuer = idp.http.url.clone();
    let server = Server::with(|c| {
        c.with_google(crate::console_oauth::Google {
            client_id: "google-client".into(),
            client_secret: "google-secret".into(),
            issuer,
        })
    });
    // The attacker starts a sign-in and hands the callback to a victim.
    let mut attacker = server.browser();
    let (_, _, location) = attacker.request("GET", "/v1/console/oauth/google/start", Value::Null);
    let state = query_param(&location.unwrap(), "state");
    let mut victim = server.browser();
    let (status, _, location) = victim.request(
        "GET",
        &format!("/v1/console/oauth/google/callback?code=c-1&state={state}"),
        Value::Null,
    );
    assert_eq!(status, 302);
    assert!(location.unwrap().contains("error="));
    assert_eq!(victim.call("GET", "/v1/console/me", Value::Null).0, 401);
}

#[test]
fn github_sign_in_uses_the_verified_primary_address() {
    let gh = FakeHttp::start(Arc::new(|method, path, _headers, _body| {
        match (method, path) {
            ("POST", "/login/oauth/access_token") => (200, r#"{"access_token":"gho_1"}"#.into()),
            ("GET", "/user") => (200, r#"{"id":77,"login":"pat","name":"Pat"}"#.into()),
            ("GET", "/user/emails") => (
                200,
                r#"[{"email":"pat@example.com","primary":true,"verified":true}]"#.into(),
            ),
            _ => (404, "{}".into()),
        }
    }));
    let url = gh.url.clone();
    let server = Server::with(|c| {
        c.with_github(crate::console_oauth::GitHub {
            client_id: "gh-client".into(),
            client_secret: "gh-secret".into(),
            authorize_url: format!("{url}/login/oauth/authorize"),
            token_url: format!("{url}/login/oauth/access_token"),
            api_url: url.clone(),
        })
    });
    let mut pat = server.browser();
    let (status, _, location) = pat.request("GET", "/v1/console/oauth/github/start", Value::Null);
    assert_eq!(status, 302);
    let location = location.unwrap();
    assert!(location.contains("code_challenge="), "{location}");
    let state = query_param(&location, "state");
    let (status, _, location) = pat.request(
        "GET",
        &format!("/v1/console/oauth/github/callback?code=c-1&state={state}"),
        Value::Null,
    );
    assert_eq!((status, location.as_deref()), (302, Some("/console#/")));
    let me = pat.ok("GET", "/v1/console/me", Value::Null);
    assert_eq!(me["user"]["email"], json!("pat@example.com"));
    assert_eq!(me["auth_method"], json!("github"));
    // A replayed callback finds its flow used.
    let (_, _, location) = pat.request(
        "GET",
        &format!("/v1/console/oauth/github/callback?code=c-1&state={state}"),
        Value::Null,
    );
    assert!(location.unwrap().contains("error="));
}

/// Organization SSO: configured by the owner, signs members in (and
/// in-domain newcomers as read-only), sessions are scoped to that
/// organization, and "SSO required" binds everyone but owners.
#[test]
fn organization_sso_is_scoped_to_its_organization() {
    let idp = FakeIdp::start("acme-client");
    let server = Server::new();
    let mut ada = server.browser();
    ada.join("ada@acme.example");
    let org = ada.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "acme", "name": "Acme" }),
    );
    let org_id = org["id"].as_str().unwrap().to_string();
    let base = format!("/v1/console/orgs/{org_id}");
    let other = ada.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "other", "name": "Other" }),
    );
    let other_base = format!("/v1/console/orgs/{}", other["id"].as_str().unwrap());
    let sso = ada.ok(
        "POST",
        &format!("{base}/sso"),
        json!({
            "issuer": idp.http.url,
            "client_id": "acme-client",
            "client_secret": "acme-secret",
            "allowed_domains": ["acme.example"],
        }),
    );
    assert_eq!(sso["configured"], json!(true));
    assert_eq!(
        sso["redirect_uri"],
        json!(format!("{PUBLIC}/v1/console/sso/callback"))
    );
    assert!(!sso.to_string().contains("acme-secret"));

    // A newcomer in the domain: a just-in-time read-only member.
    let mut kim = server.browser();
    let (status, location) = idp.sign_in(
        &mut kim,
        "/v1/console/sso/start?org=acme",
        person("okta-1", "kim@acme.example"),
    );
    assert_eq!(status, 302);
    assert_eq!(location.unwrap(), format!("/console#/orgs/{org_id}"));
    let me = kim.ok("GET", "/v1/console/me", Value::Null);
    assert_eq!(me["orgs"][0]["role"], json!("read-only"));
    assert_eq!(me["auth_method"], json!(format!("sso:{org_id}")));
    kim.ok("GET", &base, Value::Null);
    // The session cannot create organizations.
    assert_eq!(
        kim.call(
            "POST",
            "/v1/console/orgs",
            json!({ "slug": "kim", "name": "K" })
        )
        .0,
        403
    );

    // Outside the domain and not a member: refused.
    let mut eve = server.browser();
    let (_, location) = idp.sign_in(
        &mut eve,
        "/v1/console/sso/start?org=acme",
        person("okta-2", "eve@elsewhere.example"),
    );
    assert!(location.unwrap().contains("error="));

    // Review finding 1: the IdP asserting the owner's address does not sign
    // in as the owner (an admin could point SSO at an IdP of their own).
    let mut mallory = server.browser();
    let (_, location) = idp.sign_in(
        &mut mallory,
        "/v1/console/sso/start?org=acme",
        person("okta-3", "ada@acme.example"),
    );
    assert!(
        location.unwrap().contains("connect"),
        "tells to connect SSO"
    );
    assert_eq!(mallory.call("GET", "/v1/console/me", Value::Null).0, 401);

    // Ada connects SSO from her own (password) session, which she keeps.
    let (status, location) = idp.sign_in(
        &mut ada,
        "/v1/console/sso/start?org=acme&connect=1",
        person("okta-3", "ada@acme.example"),
    );
    assert_eq!(status, 302);
    assert_eq!(location.unwrap(), format!("/console#/orgs/{org_id}/sso"));
    assert_eq!(
        ada.ok("GET", "/v1/console/me", Value::Null)["auth_method"],
        json!("password")
    );
    // Connecting needs a signed-in member.
    let mut nobody = server.browser();
    let (status, _, _) = nobody.request(
        "GET",
        "/v1/console/sso/start?org=acme&connect=1",
        Value::Null,
    );
    assert_eq!(status, 401);

    // Now SSO signs Ada in, and that session only opens Acme.
    let mut ada_sso = server.browser();
    idp.sign_in(
        &mut ada_sso,
        "/v1/console/sso/start?org=acme",
        person("okta-3", "ada@acme.example"),
    );
    ada_sso.ok("GET", &base, Value::Null);
    assert_eq!(ada_sso.call("GET", &other_base, Value::Null).0, 403);

    // Requiring SSO: an admin's password session is refused, the owner's not.
    let user_id = |b: &mut Browser<'_>| {
        b.ok("GET", "/v1/console/me", Value::Null)["user"]["id"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let mut bo = server.browser();
    bo.join("bo@acme.example");
    let bo_id = user_id(&mut bo);
    ada.ok(
        "POST",
        &format!("{base}/invites"),
        json!({ "email": "bo@acme.example", "role": "admin" }),
    );
    let invite = server.token_from_mail("bo@acme.example", "invite");
    bo.ok(
        "POST",
        "/v1/console/invites/accept",
        json!({ "token": invite }),
    );
    // Only an owner may require SSO.
    assert_eq!(
        bo.call(
            "POST",
            &format!("{base}/sso"),
            json!({ "issuer": idp.http.url, "client_id": "acme-client",
                    "allowed_domains": ["acme.example"], "sso_required": true }),
        )
        .0,
        403
    );
    ada.ok(
        "POST",
        &format!("{base}/sso"),
        json!({ "issuer": idp.http.url, "client_id": "acme-client",
                "allowed_domains": ["acme.example"], "sso_required": true }),
    );
    assert_eq!(bo.call("GET", &base, Value::Null).0, 403);
    ada.ok("GET", &base, Value::Null);
    assert!(!bo_id.is_empty());

    let log = ada.ok("GET", &format!("{base}/audit-log"), Value::Null);
    let actions: Vec<&str> = log["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert!(actions.contains(&"sso.configured"), "{actions:?}");
    assert!(actions.contains(&"sso.connected"), "{actions:?}");
    assert!(!log.to_string().contains("acme-secret"));

    // Review finding 3: pointing SSO elsewhere needs a new secret.
    let elsewhere = FakeIdp::start("acme-client");
    let (status, _) = ada.call(
        "POST",
        &format!("{base}/sso"),
        json!({ "issuer": elsewhere.http.url, "client_id": "acme-client",
                "allowed_domains": ["acme.example"] }),
    );
    assert_eq!(status, 400);

    // Review finding 7: removing SSO ends the sessions it opened.
    kim.ok("GET", &base, Value::Null);
    ada.ok("DELETE", &format!("{base}/sso"), Value::Null);
    assert_eq!(kim.call("GET", &base, Value::Null).0, 401);
    assert_eq!(ada_sso.call("GET", &base, Value::Null).0, 401);
    ada.ok("GET", &base, Value::Null);
}

/// Review finding 6: an issuer declared with a trailing slash (Auth0)
/// works: ID tokens are checked against the issuer discovery declared.
#[test]
fn an_issuer_with_a_trailing_slash_signs_in() {
    let idp = FakeIdp::start_with("acme-client", "/");
    let server = Server::new();
    let mut ada = server.browser();
    ada.join("ada@acme.example");
    let org = ada.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "acme", "name": "Acme" }),
    );
    let base = format!("/v1/console/orgs/{}", org["id"].as_str().unwrap());
    let sso = ada.ok(
        "POST",
        &format!("{base}/sso"),
        json!({ "issuer": idp.http.url, "client_id": "acme-client",
                "client_secret": "s", "allowed_domains": ["acme.example"] }),
    );
    assert_eq!(sso["issuer"], json!(format!("{}/", idp.http.url)));
    let mut kim = server.browser();
    let (status, location) = idp.sign_in(
        &mut kim,
        "/v1/console/sso/start?org=acme",
        person("okta-1", "kim@acme.example"),
    );
    assert_eq!(status, 302);
    assert!(!location.unwrap().contains("error="));
    kim.ok("GET", &base, Value::Null);
}

// ── Billing (#349 wired into the console) ───────────────────────────────────

/// A catalog with a self-service upgrade plan.
const BILLING_CATALOG: &str = r#"{
  "default_plan": "free",
  "upgrade_plan": "payg",
  "plans": {
    "free": {"kind": "free", "meters": {"queries": {"included": 1000}}},
    "payg": {"kind": "usage", "base_fee_cents": 2500,
             "meters": {"queries": {"unit": 1000000, "unit_price_cents": 400}}}
  }
}"#;

/// Collects the notices the inner notifier took.
#[derive(Default)]
struct Notices(std::sync::Mutex<Vec<kosha_control::dunning::Notice>>);

impl kosha_control::dunning::Notifier for Notices {
    fn name(&self) -> &str {
        "notices"
    }
    fn notify(
        &self,
        notice: &kosha_control::dunning::Notice,
    ) -> Result<(), kosha_core::KoshaError> {
        self.0.lock().unwrap().push(notice.clone());
        Ok(())
    }
}

fn billing_server() -> Server {
    let mut server = Server::new();
    let billing = crate::billing::Billing::new(
        kosha_control::billing::Catalog::parse(BILLING_CATALOG).unwrap(),
        Arc::new(kosha_control::billing::MemoryBillingStore::new()),
        Arc::new(kosha_control::usage::MemoryUsageStore::new()),
        Some(Arc::new(kosha_control::billing::FakeBillingProvider::new())),
        Arc::new(|| 1_900_000_000_000),
    )
    .with_return_url(&format!("{PUBLIC}/console"));
    server.state.billing = Some(Arc::new(billing));
    server
}

/// Owners and admins can open Checkout and the portal for their own
/// organization's tenants from the console; read-only members cannot, and
/// no one can for a tenant of another organization.
#[test]
fn the_console_starts_checkout_and_portal_for_managers_only() {
    let server = billing_server();
    let mut ada = server.browser();
    ada.join("ada@example.com");
    let config = ada.ok("GET", "/v1/console/config", Value::Null);
    assert_eq!(config["billing_actions"], true, "{config}");
    let org = ada.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "acme", "name": "Acme" }),
    );
    let base = format!("/v1/console/orgs/{}", org["id"].as_str().unwrap());
    let tenant = ada.ok("POST", &format!("{base}/tenants"), Value::Null);
    let tenant = tenant["tenant_id"].as_str().unwrap().to_string();

    let billing = ada.ok(
        "GET",
        &format!("{base}/billing?tenant={tenant}"),
        Value::Null,
    );
    assert_eq!(
        billing["actions"],
        json!({ "checkout": true, "portal": true })
    );

    // Before a checkout there is no customer, so no portal.
    let (status, _) = ada.call(
        "POST",
        &format!("{base}/billing/portal"),
        json!({ "tenant": tenant }),
    );
    assert_eq!(status, 409);
    let session = ada.ok(
        "POST",
        &format!("{base}/billing/checkout"),
        json!({ "tenant": tenant }),
    );
    assert!(
        session["url"].as_str().is_some_and(|u| !u.is_empty()),
        "{session}"
    );
    ada.ok(
        "POST",
        &format!("{base}/billing/portal"),
        json!({ "tenant": tenant }),
    );

    // A read-only member may look but not pay.
    let mut ro = server.browser();
    ro.join("ro@example.com");
    ada.ok(
        "POST",
        &format!("{base}/invites"),
        json!({ "email": "ro@example.com", "role": "read-only" }),
    );
    let invite = server.token_from_mail("ro@example.com", "invite");
    ro.ok(
        "POST",
        "/v1/console/invites/accept",
        json!({ "token": invite }),
    );
    ro.ok(
        "GET",
        &format!("{base}/billing?tenant={tenant}"),
        Value::Null,
    );
    for action in ["checkout", "portal"] {
        let (status, body) = ro.call(
            "POST",
            &format!("{base}/billing/{action}"),
            json!({ "tenant": tenant }),
        );
        assert_eq!(status, 403, "{action}: {body}");
    }

    // Another organization's tenant is not reachable through this one.
    let mut eve = server.browser();
    eve.join("eve@example.com");
    let other = eve.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "evil", "name": "Evil" }),
    );
    let other_base = format!("/v1/console/orgs/{}", other["id"].as_str().unwrap());
    let (status, _) = eve.call(
        "POST",
        &format!("{other_base}/billing/checkout"),
        json!({ "tenant": tenant }),
    );
    assert_eq!(status, 404);
}

/// Without a provider and return URL the console offers no payment
/// buttons.
#[test]
fn the_console_offers_no_billing_actions_without_self_service() {
    let server = Server::new();
    let mut ada = server.browser();
    ada.join("ada@example.com");
    let config = ada.ok("GET", "/v1/console/config", Value::Null);
    assert_eq!(config["billing_actions"], false);
}

/// Dunning notices reach the organization's owners and admins by email,
/// after the configured notifier took them; members do not get them.
#[test]
fn dunning_notices_are_emailed_to_owners_and_admins() {
    let server = billing_server();
    let mut ada = server.browser();
    ada.join("ada@example.com");
    let org = ada.ok(
        "POST",
        "/v1/console/orgs",
        json!({ "slug": "acme", "name": "Acme" }),
    );
    let base = format!("/v1/console/orgs/{}", org["id"].as_str().unwrap());
    let tenant = ada.ok("POST", &format!("{base}/tenants"), Value::Null);
    let tenant = tenant["tenant_id"].as_str().unwrap().to_string();
    for (email, role) in [("bob@example.com", "admin"), ("mel@example.com", "member")] {
        let mut b = server.browser();
        b.join(email);
        ada.ok(
            "POST",
            &format!("{base}/invites"),
            json!({ "email": email, "role": role }),
        );
        let invite = server.token_from_mail(email, "invite");
        b.ok(
            "POST",
            "/v1/console/invites/accept",
            json!({ "token": invite }),
        );
    }

    let Server { state, mail } = server;
    let state = Arc::new(state);
    let inner = Arc::new(Notices::default());
    let notifier = crate::billing_notify::EmailNotifier::new(inner.clone());
    let notice = kosha_control::dunning::Notice {
        id: "in_1/payment_failed".into(),
        tenant: tenant.clone(),
        kind: kosha_control::dunning::NoticeKind::PaymentFailed,
        invoice_id: "in_1".into(),
        period: "2030-10".into(),
        amount_cents: 8961,
        currency: "usd".into(),
        due_at_ms: 0,
        state: kosha_control::billing::AccountState::Active,
        next_retry_at_ms: None,
        failure_reason: Some("card_declined".into()),
        message: "Your payment failed.".into(),
        at_ms: 0,
    };
    let billing_mail = |to: &str| {
        mail.sent()
            .into_iter()
            .filter(|(t, s, _)| t == to && s.starts_with("Kosha billing"))
            .count()
    };
    // Not attached yet: only the inner notifier hears about it.
    use kosha_control::dunning::Notifier;
    notifier.notify(&notice).unwrap();
    assert_eq!(billing_mail("ada@example.com"), 0);
    notifier.attach(&state);
    notifier.notify(&notice).unwrap();
    assert_eq!(inner.0.lock().unwrap().len(), 2);
    assert_eq!(billing_mail("ada@example.com"), 1);
    assert_eq!(billing_mail("bob@example.com"), 1);
    assert_eq!(billing_mail("mel@example.com"), 0);
    let (_, _, body) = mail
        .sent()
        .into_iter()
        .rfind(|(t, _, _)| t == "ada@example.com")
        .unwrap();
    assert!(
        body.contains("89.61 USD") && body.contains("card_declined"),
        "{body}"
    );
}
