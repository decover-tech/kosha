//! Per-operator admin credentials (#346): named credentials that open
//! `/v1/admin/*` like `KOSHA_ADMIN_API_KEY` does, but belong to one operator
//! each, expire, rotate and can be revoked one by one. Whoever holds one is
//! recorded in the audit log as `operator:<name>` instead of the shared
//! `operator`.
//!
//! They are rows of the tenant-key registry (`kosha_control::api_key_store`),
//! so hashing at rest, expiry, rotation with an overlap, revocation and the
//! refresh that carries all of it to every pod are the tenant-key machinery,
//! not a second copy of it. They need a Postgres control plane.
//!
//! All routes need an admin credential (`route` checks it): the bootstrap
//! `KOSHA_ADMIN_API_KEY`, which is how the first operator credential is
//! made, or an existing operator credential.
//!
//! - `POST /v1/admin/operators` `{"name": "alice", "description": "...",
//!   "expires_in_secs": 7776000}` issues a credential. Only `name` is
//!   required. The response is the only time the credential is shown. 409
//!   when that operator already holds a usable credential: rotate it.
//! - `GET /v1/admin/operators` lists credentials that are not revoked,
//!   without any secret.
//! - `POST /v1/admin/operators/revoke` `{"key_id": "..."}` (or
//!   `{"api_key": "<the credential>"}`) revokes one credential;
//!   `{"name": "alice"}` revokes every credential of that operator.
//! - `POST /v1/admin/operators/rotate`
//!   `{"key_id": "...", "overlap_secs": 86400}` issues a replacement for
//!   the same operator; the old credential keeps working for the overlap.
//!
//! **Lock-out guard.** While the shared bootstrap key is off
//! (`KOSHA_ADMIN_BOOTSTRAP_KEY=disabled`, or `KOSHA_ADMIN_API_KEY` unset),
//! a revoke or rotate that would leave no usable operator credential, or
//! make the last one lapse within a day, is refused with 409 and changes
//! nothing (`kosha_control::api_keys::check_lockout`). `"force": true` in
//! the body overrides it; the way back in is then re-enabling the
//! bootstrap key and restarting.
//!
//! Also here, because it is read-only reporting on the plaintext scrub:
//!
//! - `GET /v1/admin/api-keys/plaintext-count` reports how many registry
//!   rows still hold a plaintext key (the automatic scrub removes it).

use crate::api_error::api_error_status;
use crate::api_key_admin::{bad_request, parse_expiry, parse_key_ref, parse_overlap, Rejection};
use kosha_control::api_keys::{operator_name, valid_operator_name};
use kosha_control::orgs::Actor;
use kosha_control::ApiKeyRecord;

/// Longest description stored with a credential.
const MAX_DESCRIPTION_LEN: usize = 256;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
enum Target {
    /// One credential, by its id.
    KeyHash(String),
    /// Every credential of one operator.
    Operator(String),
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
enum Action {
    Create {
        name: String,
        description: String,
        expires_at: Option<i64>,
    },
    List,
    Revoke {
        target: Target,
        /// Override the lock-out guard.
        force: bool,
    },
    Rotate {
        key_hash: String,
        overlap_secs: u64,
        expires_at: Option<i64>,
        force: bool,
    },
    PlaintextCount,
}

/// The request's `force` flag: `true` overrides the lock-out guard.
fn parse_force(req: &serde_json::Value) -> Result<bool, Rejection> {
    match req.get("force") {
        None | Some(serde_json::Value::Null) => Ok(false),
        Some(serde_json::Value::Bool(force)) => Ok(*force),
        Some(_) => Err(bad_request("'force' must be true or false")),
    }
}

/// Answer an operator-credential request (or the plaintext count); `None`
/// for any other route. `actor` is the admin credential's holder, already
/// authenticated by `route`.
pub(crate) fn handle(method: &str, path: &str, body: &[u8], actor: &Actor) -> Option<String> {
    let bare = path.split('?').next().unwrap_or(path);
    let action = match parse(method, bare, body, crate::auth::unix_now())? {
        Ok(action) => action,
        Err((status, message)) => return Some(api_error_status(status, &message)),
    };
    // `route` refuses a request that names a user before it gets here; a
    // user must never manage operator credentials even if that changes.
    if actor.user_id().is_some() {
        return Some(api_error_status(403, "this route is operator-only"));
    }
    Some(execute(action, &actor.label()))
}

fn parse(method: &str, bare: &str, body: &[u8], now: i64) -> Option<Result<Action, Rejection>> {
    if bare == "/v1/admin/api-keys/plaintext-count" {
        return Some(match method {
            "GET" => Ok(Action::PlaintextCount),
            _ => Err((404, "not found".into())),
        });
    }
    let sub = bare.strip_prefix("/v1/admin/operators")?;
    if !sub.is_empty() && !sub.starts_with('/') {
        return None;
    }
    Some(parse_operators(method, sub, body, now))
}

fn parse_operators(method: &str, sub: &str, body: &[u8], now: i64) -> Result<Action, Rejection> {
    let json = || -> Result<serde_json::Value, Rejection> {
        serde_json::from_slice(body).map_err(|_| bad_request("invalid JSON"))
    };
    match (method, sub) {
        ("POST", "") => {
            let req = json()?;
            let description = match req.get("description") {
                None | Some(serde_json::Value::Null) => "",
                Some(serde_json::Value::String(text)) => text.as_str(),
                Some(_) => return Err(bad_request("'description' must be a string")),
            };
            if description.chars().count() > MAX_DESCRIPTION_LEN
                || description.chars().any(char::is_control)
            {
                return Err(bad_request(format!(
                    "invalid 'description' (at most {MAX_DESCRIPTION_LEN} characters, \
                     no control characters)"
                )));
            }
            Ok(Action::Create {
                name: parse_name(&req)?.ok_or_else(|| bad_request("missing 'name'"))?,
                description: description.to_string(),
                expires_at: parse_expiry(&req, now)?,
            })
        }
        ("GET", "") => Ok(Action::List),
        ("POST", "/revoke") => {
            let req = json()?;
            let by_key = req.get("key_id").is_some() || req.get("api_key").is_some();
            let force = parse_force(&req)?;
            let target = match parse_name(&req)? {
                Some(_) if by_key => {
                    return Err(bad_request(
                        "give 'name' (every credential of that operator) or 'key_id' (one \
                         credential), not both",
                    ))
                }
                Some(name) => Target::Operator(name),
                None => Target::KeyHash(parse_key_ref(&req)?),
            };
            Ok(Action::Revoke { target, force })
        }
        ("POST", "/rotate") => {
            let req = json()?;
            Ok(Action::Rotate {
                key_hash: parse_key_ref(&req)?,
                overlap_secs: parse_overlap(&req)?,
                expires_at: parse_expiry(&req, now)?,
                force: parse_force(&req)?,
            })
        }
        _ => Err((404, "not found".into())),
    }
}

/// The request's `name`, validated; `None` when it has none.
fn parse_name(req: &serde_json::Value) -> Result<Option<String>, Rejection> {
    match req.get("name") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(name)) if valid_operator_name(name) => {
            Ok(Some(name.clone()))
        }
        Some(_) => Err(bad_request(
            "invalid 'name' (1 to 64 ASCII letters, digits, '.', '_', '-', '@' or '+', \
             starting with a letter or digit)",
        )),
    }
}

#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
fn record_json(record: &ApiKeyRecord, now: i64) -> serde_json::Value {
    serde_json::json!({
        "key_id": record.key_hash,
        "name": operator_name(&record.tenant_id),
        "description": record.description,
        "expires_at": record.expires_at,
        "expired": record.expires_at.is_some_and(|at| now >= at),
        "last_used_at": record.last_used_at,
        "created_at": record.created_at,
    })
}

#[cfg(not(feature = "postgres"))]
fn execute(_action: Action, _actor: &str) -> String {
    api_error_status(
        400,
        "admin API requires postgres feature (compile with --features postgres)",
    )
}

/// Whether a change may be refused for locking operators out: only while
/// the shared bootstrap key is not a way in, and only when not forced.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
fn lockout_guard(bootstrap_key_usable: bool, force: bool) -> bool {
    !bootstrap_key_usable && !force
}

#[cfg(feature = "postgres")]
fn credential_error(what: &str, e: kosha_control::CredentialError) -> String {
    match e {
        kosha_control::CredentialError::WouldLockOut(message) => api_error_status(409, &message),
        kosha_control::CredentialError::Store(e) => {
            api_error_status(500, &format!("failed to {what}: {e}"))
        }
    }
}

#[cfg(feature = "postgres")]
fn execute(action: Action, actor: &str) -> String {
    use crate::{auth, json_ok};
    use kosha_control::{LockoutGuard, OperatorCredentials};
    let guard = |force: bool| {
        if lockout_guard(auth::bootstrap_admin_key().is_some(), force) {
            LockoutGuard::On
        } else {
            LockoutGuard::Off
        }
    };
    let auth::Auth::Keys(store) = auth::global() else {
        return api_error_status(501, "API keys are disabled (KOSHA_AUTH=disabled)");
    };
    if std::env::var("DATABASE_URL").is_err() {
        return api_error_status(501, "Postgres not configured (DATABASE_URL not set)");
    }
    let Some(pg) = store.pg() else {
        return api_error_status(500, "failed to connect to postgres");
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
        Action::Create {
            name,
            description,
            expires_at,
        } => match pg.create_operator_credential(&name, &description, expires_at, actor) {
            Ok(Some(issued)) => {
                // Usable on this pod right away; other pods pick it up on
                // their next key refresh.
                store.upsert_db_key(issued.record.clone());
                json_ok(&issued_json(&issued))
            }
            Ok(None) => api_error_status(
                409,
                &format!(
                    "operator '{name}' already has a usable credential; rotate it \
                     (POST /v1/admin/operators/rotate) or revoke it first"
                ),
            ),
            Err(e) => api_error_status(500, &format!("failed to create operator credential: {e}")),
        },
        Action::List => match pg.list_operator_credentials() {
            Ok(credentials) => {
                let operators: Vec<_> = credentials.iter().map(|c| record_json(c, now)).collect();
                json_ok(&serde_json::json!({ "operators": operators }))
            }
            Err(e) => api_error_status(500, &format!("failed to list operator credentials: {e}")),
        },
        Action::Revoke { target, force } => {
            let which = match &target {
                Target::KeyHash(hash) => OperatorCredentials::KeyHash(hash),
                Target::Operator(name) => OperatorCredentials::Operator(name),
            };
            match pg.revoke_operator_credentials(which, actor, guard(force)) {
                Ok(revoked) if revoked.is_empty() => {
                    api_error_status(404, "no active operator credential matches")
                }
                Ok(revoked) => {
                    for (key_hash, _) in &revoked {
                        store.remove_db_key(key_hash);
                    }
                    let revoked: Vec<_> = revoked
                        .iter()
                        .map(|(key_hash, name)| {
                            serde_json::json!({ "key_id": key_hash, "name": name })
                        })
                        .collect();
                    json_ok(&serde_json::json!({ "revoked": revoked }))
                }
                Err(e) => credential_error("revoke operator credential", e),
            }
        }
        Action::Rotate {
            key_hash,
            overlap_secs,
            expires_at,
            force,
        } => match pg.rotate_operator_credential(
            &key_hash,
            overlap_secs,
            expires_at,
            actor,
            guard(force),
        ) {
            Ok(Some(rotated)) => {
                store.upsert_db_key(rotated.new.record.clone());
                // The old credential keeps its operator and gains the deadline.
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
            Ok(None) => api_error_status(404, "no active operator credential with that id"),
            Err(e) => credential_error("rotate operator credential", e),
        },
        Action::PlaintextCount => match pg.count_plaintext_api_keys() {
            Ok(rows) => json_ok(&serde_json::json!({
                "plaintext_rows": rows,
                "scrub_due": rows > 0,
            })),
            Err(e) => api_error_status(500, &format!("failed to count plaintext keys: {e}")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_control::hash_api_key;

    const NOW: i64 = 1_800_000_000;

    fn request(method: &str, path: &str, body: &str) -> Option<Result<Action, Rejection>> {
        parse(method, path, body.as_bytes(), NOW)
    }

    fn ok(method: &str, path: &str, body: &str) -> Action {
        request(method, path, body)
            .expect("an operator route")
            .expect("accepted")
    }

    fn rejected(method: &str, path: &str, body: &str) -> Rejection {
        request(method, path, body)
            .expect("an operator route")
            .expect_err("rejected")
    }

    const OPERATORS: &str = "/v1/admin/operators";

    #[test]
    fn create_needs_only_a_name() {
        assert_eq!(
            ok("POST", OPERATORS, r#"{"name": "alice"}"#),
            Action::Create {
                name: "alice".into(),
                description: String::new(),
                expires_at: None,
            }
        );
        assert_eq!(
            ok(
                "POST",
                OPERATORS,
                r#"{"name": "alice@example.com", "description": "on call", "expires_in_secs": 60}"#
            ),
            Action::Create {
                name: "alice@example.com".into(),
                description: "on call".into(),
                expires_at: Some(NOW + 60),
            }
        );
    }

    #[test]
    fn create_refuses_bad_names_descriptions_and_expiries() {
        for body in [
            r#"{}"#,
            r#"{"name": ""}"#,
            r#"{"name": "al ice"}"#,
            r#"{"name": "a/b"}"#,
            r#"{"name": "_operator/alice"}"#,
            r#"{"name": 7}"#,
            r#"{"name": "alice", "description": 7}"#,
            r#"{"name": "alice", "description": "line\nbreak"}"#,
            r#"{"name": "alice", "expires_in_secs": 0}"#,
            r#"{"name": "alice", "expires_at": 5}"#,
            "not json",
        ] {
            let (status, message) = rejected("POST", OPERATORS, body);
            assert_eq!(status, 400, "{body}: {message}");
        }
        let long = format!(
            r#"{{"name": "alice", "description": "{}"}}"#,
            "d".repeat(MAX_DESCRIPTION_LEN + 1)
        );
        assert_eq!(rejected("POST", OPERATORS, &long).0, 400);
    }

    #[test]
    fn revoke_takes_one_credential_or_one_operator() {
        let id = hash_api_key("sk-alice");
        assert_eq!(
            ok(
                "POST",
                "/v1/admin/operators/revoke",
                &format!(r#"{{"key_id": "{id}"}}"#)
            ),
            Action::Revoke {
                target: Target::KeyHash(id.clone()),
                force: false
            }
        );
        assert_eq!(
            ok(
                "POST",
                "/v1/admin/operators/revoke",
                r#"{"api_key": "sk-alice"}"#
            ),
            Action::Revoke {
                target: Target::KeyHash(id.clone()),
                force: false
            }
        );
        assert_eq!(
            ok(
                "POST",
                "/v1/admin/operators/revoke",
                r#"{"name": "alice", "force": true}"#
            ),
            Action::Revoke {
                target: Target::Operator("alice".into()),
                force: true
            }
        );
        for body in [
            r#"{}"#.to_string(),
            r#"{"key_id": "short"}"#.to_string(),
            r#"{"name": "al ice"}"#.to_string(),
            r#"{"name": "alice", "force": "yes"}"#.to_string(),
            format!(r#"{{"name": "alice", "key_id": "{id}"}}"#),
        ] {
            assert_eq!(
                rejected("POST", "/v1/admin/operators/revoke", &body).0,
                400,
                "{body}"
            );
        }
    }

    #[test]
    fn rotate_defaults_to_a_one_day_overlap() {
        let id = hash_api_key("sk-alice");
        assert_eq!(
            ok(
                "POST",
                "/v1/admin/operators/rotate",
                &format!(r#"{{"key_id": "{id}"}}"#)
            ),
            Action::Rotate {
                key_hash: id.clone(),
                overlap_secs: 24 * 60 * 60,
                expires_at: None,
                force: false,
            }
        );
        assert_eq!(
            ok(
                "POST",
                "/v1/admin/operators/rotate",
                &format!(
                    r#"{{"key_id": "{id}", "overlap_secs": 0, "expires_in_secs": 10,
                         "force": true}}"#
                )
            ),
            Action::Rotate {
                key_hash: id.clone(),
                overlap_secs: 0,
                expires_at: Some(NOW + 10),
                force: true,
            }
        );
        assert_eq!(
            rejected(
                "POST",
                "/v1/admin/operators/rotate",
                &format!(r#"{{"key_id": "{id}", "overlap_secs": -1}}"#)
            )
            .0,
            400
        );
        assert_eq!(rejected("POST", "/v1/admin/operators/rotate", "{}").0, 400);
    }

    #[test]
    fn only_its_own_routes_are_answered() {
        assert_eq!(ok("GET", OPERATORS, ""), Action::List);
        assert_eq!(
            ok("GET", "/v1/admin/api-keys/plaintext-count", ""),
            Action::PlaintextCount
        );
        // The count is read-only.
        assert_eq!(
            rejected("POST", "/v1/admin/api-keys/plaintext-count", "{}").0,
            404
        );
        assert_eq!(rejected("DELETE", OPERATORS, "").0, 404);
        assert_eq!(rejected("POST", "/v1/admin/operators/nope", "{}").0, 404);
        // Everything else is left to the other admin handlers.
        for path in [
            "/v1/admin/api-keys",
            "/v1/admin/api-keys/scrub-plaintext",
            "/v1/admin/operatorsx",
            "/v1/admin/orgs",
            "/v1/admin/gc",
        ] {
            assert!(request("POST", path, "{}").is_none(), "{path}");
            assert!(request("GET", path, "").is_none(), "{path}");
        }
    }

    #[test]
    fn listing_shows_the_operator_and_no_secret() {
        let record = ApiKeyRecord {
            key_hash: hash_api_key("sk-alice"),
            tenant_id: kosha_control::api_keys::operator_tenant_id("alice"),
            description: "on call".into(),
            scopes: kosha_control::Scopes::NONE,
            namespaces: None,
            expires_at: Some(NOW - 1),
            last_used_at: Some(NOW - 5),
            created_at: NOW - 10,
        };
        let shown = record_json(&record, NOW);
        assert_eq!(shown["name"], "alice");
        assert_eq!(shown["key_id"], hash_api_key("sk-alice"));
        assert_eq!(shown["expired"], true);
        assert_eq!(shown["last_used_at"], NOW - 5);
        let text = shown.to_string();
        assert!(!text.contains("sk-alice"), "{text}");
        assert!(!text.contains("_operator/"), "{text}");
        assert!(shown.get("tenant_id").is_none() && shown.get("scopes").is_none());
    }

    #[test]
    fn the_lockout_guard_applies_only_without_a_bootstrap_key_and_without_force() {
        assert!(lockout_guard(false, false));
        assert!(!lockout_guard(false, true));
        assert!(!lockout_guard(true, false));
        assert!(!lockout_guard(true, true));
    }

    #[test]
    fn a_user_cannot_manage_operator_credentials() {
        let response = handle(
            "POST",
            OPERATORS,
            br#"{"name": "alice"}"#,
            &Actor::User("usr_1".into()),
        )
        .unwrap();
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    }
}
