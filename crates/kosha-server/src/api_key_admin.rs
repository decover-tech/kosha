//! The API-key registry's admin routes (#346). All need the dedicated
//! `KOSHA_ADMIN_API_KEY` credential (`route` checks it for `/v1/admin/*`)
//! and a Postgres control plane:
//!
//! - `POST /v1/admin/api-keys` creates a key:
//!   `{"tenant_id": "acme", "description": "...", "scopes": ["read"],
//!   "namespaces": ["docs"], "expires_in_secs": 86400}`. Only `tenant_id`
//!   is required; without `scopes` the key gets all three, without
//!   `namespaces` every namespace of the tenant, without `expires_in_secs`
//!   (or an absolute `expires_at` in unix seconds) it never expires. The
//!   response is the only time the key is shown.
//! - `GET /v1/admin/api-keys[?tenant_id=acme]` lists keys that are not
//!   revoked, without any secret.
//! - `POST /v1/admin/api-keys/revoke` `{"key_id": "..."}`.
//! - `POST /v1/admin/api-keys/rotate`
//!   `{"key_id": "...", "overlap_secs": 86400}` issues a replacement with
//!   the same tenant, scopes and namespaces; the old key keeps working for
//!   `overlap_secs` (default one day), so clients can switch over without
//!   downtime. `expires_in_secs` / `expires_at` set the new key's expiry.
//! - `POST /v1/admin/api-keys/scrub-plaintext` removes the plaintext that
//!   rows created before #346 still hold.
//!
//! A key's `key_id` is the SHA-256 hash the registry stores for it. Where
//! a `key_id` is expected, `{"api_key": "<the key>"}` works too.

use crate::{
    api_error, api_error_status, valid_namespace_component, valid_new_tenant_id, ErrorCode,
};
use kosha_control::{hash_api_key, ApiKeyRecord, NewApiKey, Scopes};

/// How long a rotated key keeps working when the caller does not say.
const DEFAULT_ROTATION_OVERLAP_SECS: u64 = 24 * 60 * 60;
/// Upper bound for an overlap window and for `expires_in_secs`: ten years.
const MAX_LIFETIME_SECS: u64 = 10 * 365 * 24 * 60 * 60;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
enum Action {
    Create(NewApiKey),
    List {
        tenant_id: Option<String>,
    },
    Revoke {
        key_hash: String,
    },
    Rotate {
        key_hash: String,
        overlap_secs: u64,
        expires_at: Option<i64>,
    },
    ScrubPlaintext,
}

/// Answer an `/v1/admin/api-keys` request; `None` for any other route.
pub(crate) fn handle(request_line: &str, body: &[u8]) -> Option<String> {
    let (method, path) = crate::parse_method_and_path(request_line)?;
    let (bare, query) = path.split_once('?').unwrap_or((path, ""));
    let sub = bare.strip_prefix("/v1/admin/api-keys")?;
    if !sub.is_empty() && !sub.starts_with('/') {
        return None;
    }
    let action = match parse(method, sub, query, body, crate::auth::unix_now()) {
        Ok(action) => action,
        Err((status, message)) => return Some(api_error_status(status, &message)),
    };
    Some(execute(action))
}

type Rejection = (u16, String);

fn bad_request(message: impl Into<String>) -> Rejection {
    (400, message.into())
}

fn parse(method: &str, sub: &str, query: &str, body: &[u8], now: i64) -> Result<Action, Rejection> {
    let json = || -> Result<serde_json::Value, Rejection> {
        serde_json::from_slice(body).map_err(|_| bad_request("invalid JSON"))
    };
    match (method, sub) {
        ("POST", "") => parse_create(&json()?, now).map(Action::Create),
        ("GET", "") => Ok(Action::List {
            tenant_id: query
                .split('&')
                .find_map(|pair| pair.strip_prefix("tenant_id="))
                .map(crate::url_decode)
                .filter(|t| !t.is_empty()),
        }),
        ("POST", "/revoke") => Ok(Action::Revoke {
            key_hash: parse_key_ref(&json()?)?,
        }),
        ("POST", "/rotate") => {
            let req = json()?;
            Ok(Action::Rotate {
                key_hash: parse_key_ref(&req)?,
                overlap_secs: match req.get("overlap_secs") {
                    None | Some(serde_json::Value::Null) => DEFAULT_ROTATION_OVERLAP_SECS,
                    Some(v) => v
                        .as_u64()
                        .filter(|secs| *secs <= MAX_LIFETIME_SECS)
                        .ok_or_else(|| {
                            bad_request("'overlap_secs' must be a non-negative number of seconds")
                        })?,
                },
                expires_at: parse_expiry(&req, now)?,
            })
        }
        ("POST", "/scrub-plaintext") => Ok(Action::ScrubPlaintext),
        _ => Err((404, "not found".into())),
    }
}

fn parse_create(req: &serde_json::Value, now: i64) -> Result<NewApiKey, Rejection> {
    let tenant_id = req
        .get("tenant_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| bad_request("missing 'tenant_id'"))?;
    if !valid_new_tenant_id(tenant_id) {
        return Err(bad_request(
            "invalid 'tenant_id' (must be non-empty, without '/', '\\' or control \
             characters, and not start with '_' or '.')",
        ));
    }
    let description = req
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let strings = |field: &str| -> Result<Option<Vec<&str>>, Rejection> {
        match req.get(field) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .map(|item| item.as_str())
                .collect::<Option<Vec<&str>>>()
                .map(Some)
                .ok_or_else(|| bad_request(format!("'{field}' must be an array of strings"))),
            Some(_) => Err(bad_request(format!(
                "'{field}' must be an array of strings"
            ))),
        }
    };
    let scopes = match strings("scopes")? {
        None => Scopes::ALL,
        Some(names) => {
            Scopes::parse(names).map_err(|e| bad_request(format!("invalid 'scopes': {e}")))?
        }
    };
    let namespaces = match strings("namespaces")? {
        None => None,
        Some(names) => {
            if names.is_empty() {
                return Err(bad_request(
                    "'namespaces' must name at least one namespace (omit it for all)",
                ));
            }
            if let Some(bad) = names.iter().find(|n| !valid_namespace_component(n)) {
                return Err(bad_request(format!(
                    "invalid namespace '{bad}' in 'namespaces' (use unqualified namespace names)"
                )));
            }
            let mut names: Vec<String> = names.into_iter().map(str::to_string).collect();
            names.sort_unstable();
            names.dedup();
            Some(names)
        }
    };
    Ok(NewApiKey {
        tenant_id: tenant_id.to_string(),
        description: description.to_string(),
        scopes,
        namespaces,
        expires_at: parse_expiry(req, now)?,
    })
}

/// `expires_in_secs` (relative) or `expires_at` (unix seconds); neither
/// means the key never expires.
fn parse_expiry(req: &serde_json::Value, now: i64) -> Result<Option<i64>, Rejection> {
    let present = |field: &str| req.get(field).filter(|v| !v.is_null());
    match (present("expires_in_secs"), present("expires_at")) {
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(bad_request(
            "give 'expires_in_secs' or 'expires_at', not both",
        )),
        (Some(v), None) => v
            .as_u64()
            .filter(|secs| (1..=MAX_LIFETIME_SECS).contains(secs))
            .map(|secs| Some(now + secs as i64))
            .ok_or_else(|| bad_request("'expires_in_secs' must be a positive number of seconds")),
        (None, Some(v)) => v
            .as_i64()
            .filter(|at| *at > now && *at <= now + MAX_LIFETIME_SECS as i64)
            .map(Some)
            .ok_or_else(|| bad_request("'expires_at' must be a future time in unix seconds")),
    }
}

/// The hash of the key a request refers to: `key_id`, or the key itself.
fn parse_key_ref(req: &serde_json::Value) -> Result<String, Rejection> {
    if let Some(id) = req.get("key_id").and_then(|v| v.as_str()) {
        let is_hash = id.len() == 64
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        return if is_hash {
            Ok(id.to_string())
        } else {
            Err(bad_request(
                "invalid 'key_id' (expected the 64-character id from the key listing)",
            ))
        };
    }
    match req.get("api_key").and_then(|v| v.as_str()) {
        Some(key) if !key.trim().is_empty() => Ok(hash_api_key(key.trim())),
        _ => Err(bad_request("missing 'key_id' (or 'api_key')")),
    }
}

#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
fn record_json(record: &ApiKeyRecord, now: i64) -> serde_json::Value {
    serde_json::json!({
        "key_id": record.key_hash,
        "tenant_id": record.tenant_id,
        "description": record.description,
        "scopes": record.scopes.names(),
        "namespaces": record.namespaces,
        "expires_at": record.expires_at,
        "expired": record.expires_at.is_some_and(|at| now >= at),
        "last_used_at": record.last_used_at,
        "created_at": record.created_at,
    })
}

#[cfg(not(feature = "postgres"))]
fn execute(_action: Action) -> String {
    api_error(
        ErrorCode::InvalidArgument,
        "admin API requires postgres feature (compile with --features postgres)",
    )
}

#[cfg(feature = "postgres")]
fn execute(action: Action) -> String {
    use crate::{auth, json_ok};
    let auth::Auth::Keys(store) = auth::global() else {
        return api_error(
            ErrorCode::InvalidArgument,
            "API keys are disabled (KOSHA_AUTH=disabled)",
        );
    };
    if std::env::var("DATABASE_URL").is_err() {
        return api_error(
            ErrorCode::InvalidArgument,
            "Postgres not configured (DATABASE_URL not set)",
        );
    }
    let Some(pg) = store.pg() else {
        return api_error(ErrorCode::Internal, "failed to connect to postgres");
    };
    let now = auth::unix_now();
    // Held until the change is in this pod's snapshot, so a key refresh
    // that read the table before the change cannot undo it here.
    let _registry = store.registry_guard();
    let issued_json = |issued: &kosha_control::IssuedApiKey| {
        let mut value = record_json(&issued.record, now);
        value["api_key"] = serde_json::json!(issued.api_key);
        value
    };
    match action {
        Action::Create(new) => match pg.create_api_key(&new) {
            Ok(issued) => {
                // Usable on this pod right away; other pods pick it up on
                // their next key refresh.
                store.upsert_db_key(issued.record.clone());
                json_ok(&issued_json(&issued))
            }
            Err(e) => api_error(ErrorCode::Internal, &format!("failed to create key: {e}")),
        },
        Action::List { tenant_id } => match pg.list_api_keys(tenant_id.as_deref()) {
            Ok(keys) => {
                let keys: Vec<_> = keys.iter().map(|k| record_json(k, now)).collect();
                json_ok(&serde_json::json!({ "api_keys": keys }))
            }
            Err(e) => api_error(ErrorCode::Internal, &format!("failed to list keys: {e}")),
        },
        Action::Revoke { key_hash } => match pg.revoke_api_key(&key_hash) {
            Ok(true) => {
                store.remove_db_key(&key_hash);
                json_ok(&serde_json::json!({ "revoked": true, "key_id": key_hash }))
            }
            Ok(false) => api_error(ErrorCode::NotFound, "no active API key with that id"),
            Err(e) => api_error(ErrorCode::Internal, &format!("failed to revoke key: {e}")),
        },
        Action::Rotate {
            key_hash,
            overlap_secs,
            expires_at,
        } => match pg.rotate_api_key(&key_hash, overlap_secs, expires_at) {
            Ok(Some(rotated)) => {
                store.upsert_db_key(rotated.new.record.clone());
                // The old key keeps its attributes and gains the deadline.
                store.upsert_db_key(ApiKeyRecord {
                    key_hash: key_hash.clone(),
                    expires_at: Some(rotated.old_expires_at),
                    ..rotated.new.record.clone()
                });
                let mut value = issued_json(&rotated.new);
                value["replaces"] = serde_json::json!({
                    "key_id": key_hash,
                    "expires_at": rotated.old_expires_at,
                });
                json_ok(&value)
            }
            Ok(None) => api_error(ErrorCode::NotFound, "no active API key with that id"),
            Err(e) => api_error(ErrorCode::Internal, &format!("failed to rotate key: {e}")),
        },
        Action::ScrubPlaintext => match pg.scrub_plaintext_api_keys() {
            Ok(scrubbed) => json_ok(&serde_json::json!({ "scrubbed": scrubbed })),
            Err(e) => api_error(
                ErrorCode::Internal,
                &format!("failed to scrub plaintext keys: {e}"),
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_control::Scope;

    const NOW: i64 = 1_800_000_000;

    fn post(sub: &str, body: &str) -> Result<Action, Rejection> {
        parse("POST", sub, "", body.as_bytes(), NOW)
    }

    fn error(result: Result<Action, Rejection>) -> String {
        let (status, message) = result.unwrap_err();
        assert_eq!(status, 400, "{message}");
        message
    }

    #[test]
    fn create_defaults_to_a_full_access_key_that_never_expires() {
        assert_eq!(
            post("", r#"{"tenant_id":"acme"}"#),
            Ok(Action::Create(NewApiKey {
                tenant_id: "acme".into(),
                description: String::new(),
                scopes: Scopes::ALL,
                namespaces: None,
                expires_at: None,
            }))
        );
        // Explicit nulls mean the same as leaving the fields out.
        assert_eq!(
            post(
                "",
                r#"{"tenant_id":"acme","scopes":null,"namespaces":null,"expires_at":null}"#
            ),
            post("", r#"{"tenant_id":"acme"}"#)
        );
    }

    #[test]
    fn create_reads_scopes_namespaces_and_expiry() {
        let body = r#"{"tenant_id":"acme","description":"ci","scopes":["read","write"],
                       "namespaces":["wiki","docs","wiki"],"expires_in_secs":3600}"#;
        assert_eq!(
            post("", body),
            Ok(Action::Create(NewApiKey {
                tenant_id: "acme".into(),
                description: "ci".into(),
                scopes: Scopes::of(&[Scope::Read, Scope::Write]),
                namespaces: Some(vec!["docs".into(), "wiki".into()]),
                expires_at: Some(NOW + 3600),
            }))
        );
        let Ok(Action::Create(absolute)) = post(
            "",
            &format!(r#"{{"tenant_id":"acme","expires_at":{}}}"#, NOW + 60),
        ) else {
            panic!("expected create");
        };
        assert_eq!(absolute.expires_at, Some(NOW + 60));
    }

    #[test]
    fn create_rejects_bad_input() {
        for (body, needle) in [
            ("not json", "invalid JSON"),
            ("{}", "tenant_id"),
            (r#"{"tenant_id":"_legacy"}"#, "tenant_id"),
            (r#"{"tenant_id":"a","scopes":["root"]}"#, "root"),
            (r#"{"tenant_id":"a","scopes":[]}"#, "at least one scope"),
            (r#"{"tenant_id":"a","scopes":"read"}"#, "array of strings"),
            (r#"{"tenant_id":"a","scopes":[1]}"#, "array of strings"),
            (r#"{"tenant_id":"a","namespaces":[]}"#, "at least one"),
            (r#"{"tenant_id":"a","namespaces":["a/b"]}"#, "a/b"),
            (
                r#"{"tenant_id":"a","namespaces":[""]}"#,
                "invalid namespace",
            ),
            (
                r#"{"tenant_id":"a","namespaces":["_shard-0001"]}"#,
                "_shard-0001",
            ),
            (r#"{"tenant_id":"a","expires_in_secs":0}"#, "positive"),
            (r#"{"tenant_id":"a","expires_in_secs":-5}"#, "positive"),
            (r#"{"tenant_id":"a","expires_in_secs":"1h"}"#, "positive"),
            (r#"{"tenant_id":"a","expires_at":5}"#, "future"),
            (
                r#"{"tenant_id":"a","expires_at":1900000000,"expires_in_secs":5}"#,
                "not both",
            ),
        ] {
            let message = error(post("", body));
            assert!(message.contains(needle), "{body}: {message}");
        }
    }

    #[test]
    fn keys_are_referenced_by_id_or_by_the_key_itself() {
        let id = hash_api_key("sk-abc");
        assert_eq!(
            post("/revoke", &format!(r#"{{"key_id":"{id}"}}"#)),
            Ok(Action::Revoke {
                key_hash: id.clone()
            })
        );
        assert_eq!(
            post("/revoke", r#"{"api_key":" sk-abc "}"#),
            Ok(Action::Revoke {
                key_hash: id.clone()
            })
        );
        for body in [
            "{}",
            r#"{"key_id":"abc"}"#,
            r#"{"key_id":"sk-abc"}"#,
            r#"{"api_key":"  "}"#,
        ] {
            assert!(error(post("/revoke", body)).contains("key_id"), "{body}");
        }
        // An uppercase or over-long id is not a stored hash.
        let upper = id.to_uppercase();
        assert!(post("/revoke", &format!(r#"{{"key_id":"{upper}"}}"#)).is_err());
    }

    #[test]
    fn rotate_defaults_to_a_one_day_overlap() {
        let id = hash_api_key("sk-abc");
        assert_eq!(
            post("/rotate", &format!(r#"{{"key_id":"{id}"}}"#)),
            Ok(Action::Rotate {
                key_hash: id.clone(),
                overlap_secs: 86_400,
                expires_at: None,
            })
        );
        assert_eq!(
            post(
                "/rotate",
                &format!(r#"{{"key_id":"{id}","overlap_secs":0,"expires_in_secs":600}}"#)
            ),
            Ok(Action::Rotate {
                key_hash: id.clone(),
                overlap_secs: 0,
                expires_at: Some(NOW + 600),
            })
        );
        for overlap in ["-1", "\"1d\"", "99999999999"] {
            let body = format!(r#"{{"key_id":"{id}","overlap_secs":{overlap}}}"#);
            assert!(error(post("/rotate", &body)).contains("overlap_secs"));
        }
    }

    #[test]
    fn list_scrub_and_unknown_routes() {
        assert_eq!(
            parse("GET", "", "", b"", NOW),
            Ok(Action::List { tenant_id: None })
        );
        assert_eq!(
            parse("GET", "", "x=1&tenant_id=acme%20corp", b"", NOW),
            Ok(Action::List {
                tenant_id: Some("acme corp".into())
            })
        );
        assert_eq!(post("/scrub-plaintext", ""), Ok(Action::ScrubPlaintext));
        assert_eq!(post("/nope", "{}").unwrap_err().0, 404);
        assert_eq!(parse("DELETE", "", "", b"", NOW).unwrap_err().0, 404);
        // Not an api-keys route at all.
        assert_eq!(handle("POST /v1/admin/api-keysx HTTP/1.1", b"{}"), None);
        assert_eq!(handle("POST /v1/admin/gc HTTP/1.1", b"{}"), None);
    }

    #[test]
    fn listed_keys_show_no_secret_and_flag_expiry() {
        let record = ApiKeyRecord {
            key_hash: hash_api_key("sk-abc"),
            tenant_id: "acme".into(),
            description: "ci".into(),
            scopes: Scopes::of(&[Scope::Read]),
            namespaces: Some(vec!["docs".into()]),
            expires_at: Some(NOW - 1),
            last_used_at: Some(NOW - 50),
            created_at: NOW - 100,
        };
        let json = record_json(&record, NOW);
        assert_eq!(json["key_id"], hash_api_key("sk-abc"));
        assert_eq!(json["scopes"], serde_json::json!(["read"]));
        assert_eq!(json["namespaces"], serde_json::json!(["docs"]));
        assert_eq!(json["expired"], true);
        assert_eq!(json["last_used_at"], NOW - 50);
        assert!(json.get("api_key").is_none());
        assert!(!json.to_string().contains("sk-abc"));
    }
}
