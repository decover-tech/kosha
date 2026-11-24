//! OAuth 2.0 / OpenID Connect sign-in for the console (#345): Google and
//! any organization's OIDC provider (authorization code + PKCE, `state`,
//! `nonce`, ID-token verification against the provider's JWKS), and
//! GitHub (plain OAuth 2.0 + PKCE, then its user and emails APIs).
//!
//! What an ID token must satisfy before its user is signed in:
//! - its header names `RS256` or `ES256` (`none`, `HS*` and everything
//!   else are refused: an HMAC "signature" keyed with a public key is the
//!   classic forgery) and its `kid` is in the provider's JWKS;
//! - the signature verifies with that key;
//! - `iss` is the configured issuer (for Google also its historical
//!   `accounts.google.com`), `aud` contains our client id (and `azp`, when
//!   present with several audiences, is our client id), `exp` is in the
//!   future and `iat` not far in it (60 s of clock skew either way), and
//!   `nonce` is the one this sign-in sent;
//! - `email` is present and `email_verified` is true.
//!
//! The discovery document and the JWKS are cached per issuer for an hour.
//! A token signed with a `kid` the cached JWKS lacks refetches it (at most
//! once a minute), so a provider's key rotation is picked up at once.
//!
//! GitHub gives no ID token: the access token fetches `GET /user` (the
//! stable numeric id is the subject) and `GET /user/emails`, and only the
//! primary address, and only if GitHub marks it verified, is accepted.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::console_crypto::{b64url_decode, constant_time_eq};

/// How long discovery documents and JWKS are trusted.
const CACHE_TTL: Duration = Duration::from_secs(3600);
/// The least time between two JWKS fetches for an unknown `kid`.
const REFETCH_INTERVAL: Duration = Duration::from_secs(60);
/// Clock skew allowed on `exp` and `iat`.
const SKEW_SECS: i64 = 60;

/// What a verified sign-in yields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedUser {
    pub subject: String,
    pub email: String,
    pub name: String,
}

/// The endpoints of an OIDC provider (from its discovery document).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Discovery {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub jwks_uri: String,
    /// `client_secret_basic` unless the provider supports only
    /// `client_secret_post`.
    pub secret_in_body: bool,
}

/// A configured GitHub OAuth app (or a GitHub Enterprise host).
#[derive(Debug, Clone)]
pub(crate) struct GitHub {
    pub client_id: String,
    pub client_secret: String,
    pub authorize_url: String,
    pub token_url: String,
    pub api_url: String,
}

/// Google: an OIDC provider with a well-known issuer.
#[derive(Debug, Clone)]
pub(crate) struct Google {
    pub client_id: String,
    pub client_secret: String,
    pub issuer: String,
}

pub(crate) const GOOGLE_ISSUER: &str = "https://accounts.google.com";

/// The parameters of one authorization request.
pub(crate) struct AuthRequest<'a> {
    pub client_id: &'a str,
    pub redirect_uri: &'a str,
    pub state: &'a str,
    pub nonce: Option<&'a str>,
    pub code_challenge: &'a str,
    pub scope: &'a str,
}

/// Percent-encode a query value (RFC 3986 unreserved characters kept).
pub(crate) fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// An `application/x-www-form-urlencoded` body.
fn form_body(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// The URL to send the browser to.
pub(crate) fn authorize_url(endpoint: &str, r: &AuthRequest<'_>) -> String {
    let mut url = format!(
        "{endpoint}{}response_type=code&client_id={}&redirect_uri={}&scope={}&state={}\
         &code_challenge={}&code_challenge_method=S256",
        if endpoint.contains('?') { '&' } else { '?' },
        encode(r.client_id),
        encode(r.redirect_uri),
        encode(r.scope),
        encode(r.state),
        encode(r.code_challenge),
    );
    if let Some(nonce) = r.nonce {
        url.push_str(&format!("&nonce={}", encode(nonce)));
    }
    url
}

/// Discovery and JWKS fetching with caches. Every request goes through
/// [`crate::console_net`]: https only, public addresses only, pinned to the
/// checked address, no proxy, no redirects.
pub(crate) struct OidcClient {
    /// Tests only: admit `http` and loopback providers.
    allow_insecure: bool,
    discovery: Mutex<HashMap<String, (Instant, Discovery)>>,
    jwks: Mutex<HashMap<String, JwksEntry>>,
}

#[derive(Clone)]
struct JwksEntry {
    fetched: Instant,
    keys: Vec<Value>,
    /// When a token with an unknown `kid` last forced a refetch.
    forced: Option<Instant>,
}

impl OidcClient {
    pub(crate) fn new() -> Self {
        Self {
            allow_insecure: false,
            discovery: Mutex::new(HashMap::new()),
            jwks: Mutex::new(HashMap::new()),
        }
    }

    /// A client that also talks to `http://127.0.0.1` fakes.
    #[cfg(test)]
    pub(crate) fn insecure_for_tests() -> Self {
        Self {
            allow_insecure: true,
            ..Self::new()
        }
    }

    /// A request to `url` after the address checks.
    fn request(
        &self,
        method: reqwest::Method,
        url: &str,
    ) -> Result<reqwest::blocking::RequestBuilder, String> {
        let target = crate::console_net::check_url(url, self.allow_insecure)?;
        let client = crate::console_net::client_for(&target)?;
        Ok(client.request(method, target.url))
    }

    fn get_json(&self, url: &str, bearer: Option<&str>) -> Result<Value, String> {
        let mut request = self
            .request(reqwest::Method::GET, url)?
            .header("accept", "application/json");
        if let Some(token) = bearer {
            request = request.bearer_auth(token);
        }
        let response = request.send().map_err(|e| format!("GET {url}: {e}"))?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("GET {url}: HTTP {status}"));
        }
        response.json().map_err(|e| format!("GET {url}: {e}"))
    }

    /// The provider's discovery document. Its `issuer` must be the one
    /// asked for (OIDC Discovery §4.3).
    pub(crate) fn discover(&self, issuer: &str) -> Result<Discovery, String> {
        let issuer = issuer.trim_end_matches('/');
        if let Some((at, d)) = self.discovery.lock().unwrap().get(issuer) {
            if at.elapsed() < CACHE_TTL {
                return Ok(d.clone());
            }
        }
        let doc = self.get_json(&format!("{issuer}/.well-known/openid-configuration"), None)?;
        let field = |name: &str| {
            doc.get(name)
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| format!("discovery document has no '{name}'"))
        };
        let declared = field("issuer")?;
        if declared.trim_end_matches('/') != issuer {
            return Err(format!(
                "discovery document is for issuer '{declared}', not '{issuer}'"
            ));
        }
        // Every endpoint the server or the browser will use: https (the
        // server's own fetches are checked again, address and all, when
        // they are made).
        for name in ["authorization_endpoint", "token_endpoint", "jwks_uri"] {
            let url = field(name)?;
            if !(url.starts_with("https://") || self.allow_insecure) {
                return Err(format!("discovery document's {name} is not https: {url:?}"));
            }
        }
        let methods: Vec<&str> = doc
            .get("token_endpoint_auth_methods_supported")
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let discovery = Discovery {
            issuer: declared,
            authorization_endpoint: field("authorization_endpoint")?,
            token_endpoint: field("token_endpoint")?,
            jwks_uri: field("jwks_uri")?,
            secret_in_body: !methods.is_empty()
                && !methods.contains(&"client_secret_basic")
                && methods.contains(&"client_secret_post"),
        };
        self.discovery
            .lock()
            .unwrap()
            .insert(issuer.to_string(), (Instant::now(), discovery.clone()));
        Ok(discovery)
    }

    fn fetch_jwks(&self, uri: &str, forced: Option<Instant>) -> Result<Vec<Value>, String> {
        let doc = self.get_json(uri, None)?;
        let keys = doc
            .get("keys")
            .and_then(Value::as_array)
            .cloned()
            .ok_or("JWKS has no 'keys'")?;
        self.jwks.lock().unwrap().insert(
            uri.to_string(),
            JwksEntry {
                fetched: Instant::now(),
                keys: keys.clone(),
                forced,
            },
        );
        Ok(keys)
    }

    /// The JWK for `kid`, from the cache or a fresh fetch.
    fn jwk(&self, uri: &str, kid: Option<&str>) -> Result<Value, String> {
        let find = |keys: &[Value]| -> Option<Value> {
            match kid {
                Some(kid) => keys
                    .iter()
                    .find(|k| k.get("kid").and_then(Value::as_str) == Some(kid))
                    .cloned(),
                // No kid: only unambiguous with a single key.
                None if keys.len() == 1 => keys.first().cloned(),
                None => None,
            }
        };
        let missing = || "the token's signing key is not in the provider's JWKS".to_string();
        let cached = self.jwks.lock().unwrap().get(uri).cloned();
        let Some(entry) = cached else {
            return find(&self.fetch_jwks(uri, None)?).ok_or_else(missing);
        };
        if entry.fetched.elapsed() >= CACHE_TTL {
            return find(&self.fetch_jwks(uri, entry.forced)?).ok_or_else(missing);
        }
        if let Some(key) = find(&entry.keys) {
            return Ok(key);
        }
        // An unknown kid: the provider may have rotated its keys. Refetch,
        // but not more than once per interval, so tokens with made-up kids
        // cannot make us hammer the provider.
        if entry
            .forced
            .is_some_and(|at| at.elapsed() < REFETCH_INTERVAL)
        {
            return Err(missing());
        }
        find(&self.fetch_jwks(uri, Some(Instant::now()))?).ok_or_else(missing)
    }

    /// Exchange an authorization code at an OIDC token endpoint; the
    /// response's `id_token`.
    pub(crate) fn exchange_code(
        &self,
        discovery: &Discovery,
        client_id: &str,
        client_secret: &str,
        code: &str,
        redirect_uri: &str,
        verifier: &str,
    ) -> Result<String, String> {
        let mut form = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("code_verifier", verifier),
            ("client_id", client_id),
        ];
        let mut request = self
            .request(reqwest::Method::POST, &discovery.token_endpoint)?
            .header("accept", "application/json");
        if discovery.secret_in_body {
            form.push(("client_secret", client_secret));
        } else {
            // RFC 6749 §2.3.1: form-encode both before the Basic encoding.
            request = request.basic_auth(encode(client_id), Some(encode(client_secret)));
        }
        let response = request
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form_body(&form))
            .send()
            .map_err(|e| format!("token endpoint: {e}"))?;
        let status = response.status();
        let body: Value = response
            .json()
            .map_err(|e| format!("token endpoint: {e}"))?;
        if !status.is_success() {
            return Err(format!(
                "token endpoint: HTTP {status} {}",
                body.get("error").and_then(Value::as_str).unwrap_or("")
            ));
        }
        body.get("id_token")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| "token endpoint returned no id_token".into())
    }

    /// Verify an ID token (see the module docs) and return its user.
    pub(crate) fn verify_id_token(
        &self,
        token: &str,
        discovery: &Discovery,
        accepted_issuers: &[&str],
        client_id: &str,
        nonce: &str,
        now_secs: i64,
    ) -> Result<VerifiedUser, String> {
        let mut parts = token.split('.');
        let (Some(h), Some(p), Some(s), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err("malformed ID token".into());
        };
        let header: Value = b64url_decode(h)
            .and_then(|b| serde_json::from_slice(&b).ok())
            .ok_or("malformed ID token header")?;
        let alg = header.get("alg").and_then(Value::as_str).unwrap_or("");
        if alg != "RS256" && alg != "ES256" {
            return Err(format!("ID token algorithm '{alg}' is not accepted"));
        }
        let kid = header.get("kid").and_then(Value::as_str);
        let key = self.jwk(&discovery.jwks_uri, kid)?;
        let signature = b64url_decode(s).ok_or("malformed ID token signature")?;
        verify_signature(alg, &key, format!("{h}.{p}").as_bytes(), &signature)?;
        let claims: Value = b64url_decode(p)
            .and_then(|b| serde_json::from_slice(&b).ok())
            .ok_or("malformed ID token claims")?;
        check_claims(&claims, accepted_issuers, client_id, nonce, now_secs)
    }

    /// GitHub: exchange the code for an access token, then read the user
    /// and their verified primary address.
    pub(crate) fn github_user(
        &self,
        gh: &GitHub,
        code: &str,
        redirect_uri: &str,
        verifier: &str,
    ) -> Result<VerifiedUser, String> {
        let response = self
            .request(reqwest::Method::POST, &gh.token_url)?
            .header("accept", "application/json")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form_body(&[
                ("client_id", gh.client_id.as_str()),
                ("client_secret", gh.client_secret.as_str()),
                ("code", code),
                ("redirect_uri", redirect_uri),
                ("code_verifier", verifier),
            ]))
            .send()
            .map_err(|e| format!("GitHub token endpoint: {e}"))?;
        let body: Value = response
            .json()
            .map_err(|e| format!("GitHub token endpoint: {e}"))?;
        let token = body
            .get("access_token")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                format!(
                    "GitHub returned no access token ({})",
                    body.get("error").and_then(Value::as_str).unwrap_or("?")
                )
            })?
            .to_string();
        let api = gh.api_url.trim_end_matches('/');
        let user = self.get_json(&format!("{api}/user"), Some(&token))?;
        let id = user
            .get("id")
            .and_then(Value::as_u64)
            .ok_or("GitHub user has no id")?;
        let emails = self.get_json(&format!("{api}/user/emails"), Some(&token))?;
        let email = emails
            .as_array()
            .into_iter()
            .flatten()
            .find(|e| {
                e.get("primary").and_then(Value::as_bool) == Some(true)
                    && e.get("verified").and_then(Value::as_bool) == Some(true)
            })
            .and_then(|e| e.get("email").and_then(Value::as_str))
            .ok_or("your GitHub account has no verified primary email address")?;
        let name = user
            .get("name")
            .and_then(Value::as_str)
            .or_else(|| user.get("login").and_then(Value::as_str))
            .unwrap_or("");
        Ok(VerifiedUser {
            subject: id.to_string(),
            email: email.to_string(),
            name: name.to_string(),
        })
    }
}

/// Check a token's signature with a JWK.
fn verify_signature(
    alg: &str,
    jwk: &Value,
    message: &[u8],
    signature: &[u8],
) -> Result<(), String> {
    use aws_lc_rs::signature;
    let field = |name: &str| {
        jwk.get(name)
            .and_then(Value::as_str)
            .and_then(b64url_decode)
            .ok_or_else(|| format!("JWK has no usable '{name}'"))
    };
    if let Some(use_) = jwk.get("use").and_then(Value::as_str) {
        if use_ != "sig" {
            return Err("the JWK is not a signing key".into());
        }
    }
    let ok = match (alg, jwk.get("kty").and_then(Value::as_str)) {
        ("RS256", Some("RSA")) => {
            let strip = |mut b: Vec<u8>| {
                while b.first() == Some(&0) {
                    b.remove(0);
                }
                b
            };
            let (n, e) = (strip(field("n")?), strip(field("e")?));
            signature::RsaPublicKeyComponents { n: &n, e: &e }
                .verify(&signature::RSA_PKCS1_2048_8192_SHA256, message, signature)
                .is_ok()
        }
        ("ES256", Some("EC")) => {
            if jwk.get("crv").and_then(Value::as_str) != Some("P-256") {
                return Err("ES256 needs a P-256 key".into());
            }
            let (x, y) = (field("x")?, field("y")?);
            if x.len() != 32 || y.len() != 32 {
                return Err("malformed P-256 key".into());
            }
            let mut point = vec![4u8];
            point.extend_from_slice(&x);
            point.extend_from_slice(&y);
            signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, &point)
                .verify(message, signature)
                .is_ok()
        }
        _ => return Err("the JWK does not match the token's algorithm".into()),
    };
    if ok {
        Ok(())
    } else {
        Err("the ID token signature does not verify".into())
    }
}

fn check_claims(
    claims: &Value,
    accepted_issuers: &[&str],
    client_id: &str,
    nonce: &str,
    now: i64,
) -> Result<VerifiedUser, String> {
    let string = |name: &str| claims.get(name).and_then(Value::as_str);
    let iss = string("iss").unwrap_or("");
    if !accepted_issuers.contains(&iss) {
        return Err(format!("ID token issuer '{iss}' is not the configured one"));
    }
    let audiences: Vec<&str> = match claims.get("aud") {
        Some(Value::String(a)) => vec![a.as_str()],
        Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
        _ => vec![],
    };
    if !audiences.contains(&client_id) {
        return Err("the ID token is not for this client".into());
    }
    if audiences.len() > 1 && string("azp").is_some_and(|azp| azp != client_id) {
        return Err("the ID token's authorized party is another client".into());
    }
    let number = |name: &str| claims.get(name).and_then(Value::as_i64);
    match number("exp") {
        Some(exp) if exp + SKEW_SECS > now => {}
        _ => return Err("the ID token has expired".into()),
    }
    if number("iat").is_some_and(|iat| iat > now + SKEW_SECS) {
        return Err("the ID token was issued in the future".into());
    }
    let token_nonce = string("nonce").unwrap_or("");
    if !constant_time_eq(token_nonce.as_bytes(), nonce.as_bytes()) {
        return Err("the ID token's nonce does not match this sign-in".into());
    }
    let subject = string("sub")
        .filter(|s| !s.is_empty())
        .ok_or("ID token has no 'sub'")?;
    let email = string("email").ok_or("the provider did not share an email address")?;
    let verified = match claims.get("email_verified") {
        Some(Value::Bool(v)) => *v,
        // Some providers send the string form.
        Some(Value::String(v)) => v == "true",
        _ => false,
    };
    if !verified {
        return Err("the provider has not verified this email address".into());
    }
    Ok(VerifiedUser {
        subject: subject.to_string(),
        email: email.to_string(),
        name: string("name").unwrap_or("").to_string(),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::console_crypto::b64url;
    use aws_lc_rs::signature::{KeyPair, RsaKeyPair, RSA_PKCS1_SHA256};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    /// A tiny HTTP server for provider endpoints. The handler gets the
    /// method, path (with query), headers (lowercased) and body.
    pub(crate) struct FakeHttp {
        pub url: String,
    }

    type HandlerFn =
        dyn Fn(&str, &str, &HashMap<String, String>, &str) -> (u16, String) + Send + Sync;

    impl FakeHttp {
        pub(crate) fn start(handler: Arc<HandlerFn>) -> FakeHttp {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { continue };
                    let handler = handler.clone();
                    std::thread::spawn(move || {
                        let mut reader = BufReader::new(stream.try_clone().unwrap());
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 {
                            return;
                        }
                        let mut parts = line.split_whitespace();
                        let method = parts.next().unwrap_or("").to_string();
                        let path = parts.next().unwrap_or("").to_string();
                        let mut headers = HashMap::new();
                        loop {
                            let mut h = String::new();
                            reader.read_line(&mut h).unwrap();
                            if h.trim().is_empty() {
                                break;
                            }
                            if let Some((k, v)) = h.split_once(':') {
                                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
                            }
                        }
                        let len: usize = headers
                            .get("content-length")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0);
                        let mut body = vec![0u8; len];
                        reader.read_exact(&mut body).unwrap();
                        let body = String::from_utf8_lossy(&body).to_string();
                        let (status, reply) = handler(&method, &path, &headers, &body);
                        let mut out = stream;
                        let _ = write!(
                            out,
                            "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\nconnection: close\r\n\r\n{reply}",
                            reply.len()
                        );
                    });
                }
            });
            FakeHttp { url }
        }
    }

    /// An RSA key pair, its JWK, and a signer of ID tokens.
    pub(crate) struct TestKey {
        pair: RsaKeyPair,
        pub kid: String,
    }

    /// The (n, e) of a PKCS#1 RSAPublicKey DER.
    fn rsa_components(der: &[u8]) -> (Vec<u8>, Vec<u8>) {
        fn read(der: &[u8], at: &mut usize) -> (u8, Vec<u8>) {
            let tag = der[*at];
            *at += 1;
            let mut len = der[*at] as usize;
            *at += 1;
            if len & 0x80 != 0 {
                let n = len & 0x7f;
                len = 0;
                for _ in 0..n {
                    len = (len << 8) | der[*at] as usize;
                    *at += 1;
                }
            }
            let value = der[*at..*at + len].to_vec();
            *at += len;
            (tag, value)
        }
        let mut at = 0;
        let (_, seq) = read(der, &mut at);
        let mut at = 0;
        let (_, n) = read(&seq, &mut at);
        let (_, e) = read(&seq, &mut at);
        (n, e)
    }

    impl TestKey {
        pub(crate) fn new(kid: &str) -> TestKey {
            TestKey {
                pair: RsaKeyPair::generate(aws_lc_rs::rsa::KeySize::Rsa2048).unwrap(),
                kid: kid.to_string(),
            }
        }

        pub(crate) fn jwk(&self) -> Value {
            let (n, e) = rsa_components(self.pair.public_key().as_ref());
            let strip = |b: Vec<u8>| b.into_iter().skip_while(|x| *x == 0).collect::<Vec<u8>>();
            serde_json::json!({
                "kty": "RSA", "use": "sig", "alg": "RS256", "kid": self.kid,
                "n": b64url(&strip(n)), "e": b64url(&strip(e)),
            })
        }

        pub(crate) fn sign(&self, header: &Value, claims: &Value) -> String {
            let input = format!(
                "{}.{}",
                b64url(header.to_string().as_bytes()),
                b64url(claims.to_string().as_bytes())
            );
            let rng = aws_lc_rs::rand::SystemRandom::new();
            let mut sig = vec![0u8; self.pair.public_modulus_len()];
            self.pair
                .sign(&RSA_PKCS1_SHA256, &rng, input.as_bytes(), &mut sig)
                .unwrap();
            format!("{input}.{}", b64url(&sig))
        }

        pub(crate) fn id_token(&self, claims: &Value) -> String {
            self.sign(
                &serde_json::json!({"alg": "RS256", "kid": self.kid, "typ": "JWT"}),
                claims,
            )
        }
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn claims(issuer: &str) -> Value {
        serde_json::json!({
            "iss": issuer, "aud": "client-1", "sub": "1234", "nonce": "n-1",
            "email": "ada@example.com", "email_verified": true, "name": "Ada",
            "iat": now(), "exp": now() + 300,
        })
    }

    /// An OIDC provider: discovery and JWKS (with a counter of JWKS
    /// fetches), serving whatever keys `keys` holds.
    fn provider(keys: Arc<Mutex<Vec<Value>>>, fetches: Arc<Mutex<u32>>) -> FakeHttp {
        let issuer_cell: Arc<Mutex<String>> = Arc::new(Mutex::new(String::new()));
        let cell = issuer_cell.clone();
        let fake = FakeHttp::start(Arc::new(move |_m, path, _h, _b| {
            let issuer = cell.lock().unwrap().clone();
            match path {
                "/.well-known/openid-configuration" => (
                    200,
                    serde_json::json!({
                        "issuer": issuer,
                        "authorization_endpoint": format!("{issuer}/authorize"),
                        "token_endpoint": format!("{issuer}/token"),
                        "jwks_uri": format!("{issuer}/jwks"),
                    })
                    .to_string(),
                ),
                "/jwks" => {
                    *fetches.lock().unwrap() += 1;
                    (
                        200,
                        serde_json::json!({ "keys": *keys.lock().unwrap() }).to_string(),
                    )
                }
                _ => (404, "{}".into()),
            }
        }));
        *issuer_cell.lock().unwrap() = fake.url.clone();
        fake
    }

    #[test]
    fn id_tokens_are_verified_against_the_jwks() {
        let key = TestKey::new("k1");
        let keys = Arc::new(Mutex::new(vec![key.jwk()]));
        let fetches = Arc::new(Mutex::new(0));
        let idp = provider(keys.clone(), fetches.clone());
        let client = OidcClient::insecure_for_tests();
        let d = client.discover(&idp.url).unwrap();
        assert_eq!(d.jwks_uri, format!("{}/jwks", idp.url));
        let iss = [idp.url.as_str()];
        let verify =
            |token: &str| client.verify_id_token(token, &d, &iss, "client-1", "n-1", now());

        let good = key.id_token(&claims(&idp.url));
        let user = verify(&good).unwrap();
        assert_eq!(user.subject, "1234");
        assert_eq!(user.email, "ada@example.com");

        // Each claim rule.
        let with = |field: &str, value: Value| {
            let mut c = claims(&idp.url);
            c[field] = value;
            key.id_token(&c)
        };
        for (field, value) in [
            ("iss", serde_json::json!("https://evil.example")),
            ("aud", serde_json::json!("client-2")),
            ("aud", serde_json::json!(["client-2", "client-3"])),
            ("exp", serde_json::json!(now() - 3600)),
            ("iat", serde_json::json!(now() + 3600)),
            ("nonce", serde_json::json!("n-2")),
            ("email_verified", serde_json::json!(false)),
            ("sub", serde_json::json!("")),
        ] {
            assert!(
                verify(&with(field, value.clone())).is_err(),
                "{field}={value}"
            );
        }
        assert!(verify(&with("aud", serde_json::json!(["client-1", "other"]))).is_ok());
        let mut c = claims(&idp.url);
        c["aud"] = serde_json::json!(["client-1", "other"]);
        c["azp"] = serde_json::json!("other");
        assert!(verify(&key.id_token(&c)).is_err());

        // Tampering, other algorithms, other keys.
        let mut parts: Vec<String> = good.split('.').map(str::to_string).collect();
        let mut forged = claims(&idp.url);
        forged["email"] = serde_json::json!("eve@example.com");
        parts[1] = b64url(forged.to_string().as_bytes());
        assert!(verify(&parts.join(".")).is_err());
        let none = format!(
            "{}.{}.",
            b64url(br#"{"alg":"none","kid":"k1"}"#),
            b64url(claims(&idp.url).to_string().as_bytes())
        );
        assert!(verify(&none).is_err());
        let hs = key.sign(
            &serde_json::json!({"alg": "HS256", "kid": "k1"}),
            &claims(&idp.url),
        );
        assert!(verify(&hs).is_err());
        let stranger = TestKey::new("k1");
        assert!(verify(&stranger.id_token(&claims(&idp.url))).is_err());

        // Rotation: a token from a new key refetches the JWKS once.
        let rotated = TestKey::new("k2");
        keys.lock().unwrap().push(rotated.jwk());
        let before = *fetches.lock().unwrap();
        assert!(verify(&rotated.id_token(&claims(&idp.url))).is_ok());
        assert_eq!(*fetches.lock().unwrap(), before + 1);
        // An unknown kid right after does not refetch (rate limit).
        let unknown = TestKey::new("k3");
        assert!(verify(&unknown.id_token(&claims(&idp.url))).is_err());
        assert_eq!(*fetches.lock().unwrap(), before + 1);
    }

    #[test]
    fn discovery_must_name_the_configured_issuer() {
        let idp = FakeHttp::start(Arc::new(|_m, _p, _h, _b| {
            (
                200,
                serde_json::json!({
                    "issuer": "https://someone-else.example",
                    "authorization_endpoint": "a", "token_endpoint": "t", "jwks_uri": "j",
                })
                .to_string(),
            )
        }));
        let err = OidcClient::insecure_for_tests()
            .discover(&idp.url)
            .unwrap_err();
        assert!(err.contains("someone-else"), "{err}");
    }

    #[test]
    fn github_accepts_only_a_verified_primary_address() {
        let emails = Arc::new(Mutex::new(serde_json::json!([
            {"email": "old@example.com", "primary": false, "verified": true},
            {"email": "ada@example.com", "primary": true, "verified": true},
        ])));
        let list = emails.clone();
        let gh = FakeHttp::start(Arc::new(move |method, path, headers, body| {
            match (method, path) {
                ("POST", "/login/oauth/access_token") => {
                    assert!(body.contains("code_verifier=v-1"), "{body}");
                    assert!(body.contains("code=c-1"), "{body}");
                    (
                        200,
                        r#"{"access_token":"gho_1","token_type":"bearer"}"#.into(),
                    )
                }
                ("GET", "/user") => {
                    assert_eq!(
                        headers.get("authorization").map(String::as_str),
                        Some("Bearer gho_1")
                    );
                    (200, r#"{"id":42,"login":"ada","name":null}"#.into())
                }
                ("GET", "/user/emails") => (200, list.lock().unwrap().to_string()),
                _ => (404, "{}".into()),
            }
        }));
        let config = GitHub {
            client_id: "gh-client".into(),
            client_secret: "gh-secret".into(),
            authorize_url: format!("{}/login/oauth/authorize", gh.url),
            token_url: format!("{}/login/oauth/access_token", gh.url),
            api_url: gh.url.clone(),
        };
        let client = OidcClient::insecure_for_tests();
        let user = client
            .github_user(&config, "c-1", "https://kosha/cb", "v-1")
            .unwrap();
        assert_eq!(
            user,
            VerifiedUser {
                subject: "42".into(),
                email: "ada@example.com".into(),
                name: "ada".into()
            }
        );
        *emails.lock().unwrap() = serde_json::json!([
            {"email": "ada@example.com", "primary": true, "verified": false},
            {"email": "other@example.com", "primary": false, "verified": true},
        ]);
        assert!(client
            .github_user(&config, "c-1", "https://kosha/cb", "v-1")
            .is_err());
    }

    #[test]
    fn authorize_urls_carry_pkce_state_and_nonce() {
        let url = authorize_url(
            "https://idp.example/auth",
            &AuthRequest {
                client_id: "c 1",
                redirect_uri: "https://kosha.example/v1/console/oauth/google/callback",
                state: "s",
                nonce: Some("n"),
                code_challenge: "ch",
                scope: "openid email profile",
            },
        );
        assert!(url.starts_with("https://idp.example/auth?response_type=code&client_id=c%201&"));
        assert!(url.contains(
            "redirect_uri=https%3A%2F%2Fkosha.example%2Fv1%2Fconsole%2Foauth%2Fgoogle%2Fcallback"
        ));
        assert!(url.contains("&scope=openid%20email%20profile&state=s&code_challenge=ch&code_challenge_method=S256&nonce=n"));
    }
}
