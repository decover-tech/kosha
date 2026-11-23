//! Tenant self-service API-key management (#346): a tenant manages its own
//! keys with one of them, instead of asking an operator.
//!
//! The routes need a tenant key with the `admin` scope (`authz.rs` maps
//! them to it) and a Postgres control plane:
//!
//! - `GET /v1/api-keys` lists the tenant's keys that the calling key could
//!   have issued, without any secret.
//! - `POST /v1/api-keys` `{"description": "...", "scopes": ["read"],
//!   "namespaces": ["docs"], "expires_in_secs": 86400}` creates a key.
//!   Every field is optional: left out, the scopes, namespaces and expiry
//!   are the calling key's own. The response is the only time the key is
//!   shown.
//! - `POST /v1/api-keys/rotate` `{"key_id": "...", "overlap_secs": 86400}`
//!   replaces a key; the old one keeps working for the overlap.
//! - `POST /v1/api-keys/revoke` `{"key_id": "..."}`.
//!
//! Where a `key_id` is expected, `{"api_key": "<the key>"}` works too.
//!
//! **A key never grants more than it holds** ([`KeyAuthority`]): what it
//! creates has a subset of its scopes and namespaces and expires no later
//! than it does; it sees, rotates and revokes only keys of its own tenant
//! that it could have created; a rotation's replacement is bounded by the
//! caller's expiry too. Anything else is a 403 (asking for more) or a 404
//! (a key outside its grant, which it is not told exists). Env keys
//! (`KOSHA_API_KEYS`) are configuration and never listed or changed here;
//! an env key with the `admin` scope can still manage the registry's keys
//! of its tenant.
//!
//! Every change is audited in the registry's transaction, with the calling
//! key as the actor: `api_key:<first 12 characters of its id>`
//! (`api_key:env` for an env key).
//!
//! The admin and internal credentials are not tenant keys and get a 403
//! here: an operator manages any tenant's keys through
//! `/v1/admin/api-keys` (`api_key_admin.rs`).
//!
//! The core functions ([`list_keys`], [`create_key`], [`rotate_key`],
//! [`revoke_key`]) take the granting authority and the audit actor
//! explicitly, so another front end (the organization console) can call
//! them with a user's authority and `user:<id>` as the actor.

use crate::api_key_admin::{
    bad_request, parse_expiry, parse_grant, parse_key_ref, parse_overlap, Rejection,
};
use crate::json_error;
use kosha_control::{KeyAuthority, Scopes};

/// Longest description stored with a key created here.
const MAX_DESCRIPTION_LEN: usize = 256;

/// What a caller asks for when creating a key. `None` fields default to
/// the granting authority's own ([`KeyAuthority::grant`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyRequest {
    pub description: String,
    pub scopes: Option<Scopes>,
    pub namespaces: Option<Vec<String>>,
    /// Unix seconds.
    pub expires_at: Option<i64>,
}

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
enum Action {
    List,
    Create(KeyRequest),
    Rotate {
        key_hash: String,
        overlap_secs: u64,
        expires_at: Option<i64>,
    },
    Revoke {
        key_hash: String,
    },
}

/// Answer a `/v1/api-keys` request; `None` for any other route. The caller
/// is the request's principal (`authz::current_principal`), which
/// `dispatch` has already authorized for the `admin` scope.
pub(crate) fn handle(request_line: &str, body: &[u8]) -> Option<String> {
    let (method, path) = crate::parse_method_and_path(request_line)?;
    let bare = path.split('?').next().unwrap_or(path);
    let sub = bare.strip_prefix("/v1/api-keys")?;
    if !sub.is_empty() && !sub.starts_with('/') {
        return None;
    }
    let action = match parse(method, sub, body, crate::auth::unix_now()) {
        Ok(action) => action,
        Err((status, message)) => return Some(json_error(status, &message)),
    };
    let Some(caller) = crate::authz::current_principal() else {
        return Some(json_error(
            403,
            "these routes take a tenant API key with the 'admin' scope; operators manage \
             any tenant's keys through /v1/admin/api-keys",
        ));
    };
    // `authz` refuses a key without the scope before the request is
    // routed; this keeps a route that skipped it from granting anything.
    if !caller.scopes.contains(kosha_control::Scope::Admin) {
        return Some(json_error(403, "this API key lacks the 'admin' scope"));
    }
    Some(execute(
        action,
        &caller.authority(),
        &caller.audit_label(),
        caller.key_id.as_deref(),
    ))
}

fn parse(method: &str, sub: &str, body: &[u8], now: i64) -> Result<Action, Rejection> {
    let json = || -> Result<serde_json::Value, Rejection> {
        if body.iter().all(u8::is_ascii_whitespace) {
            return Ok(serde_json::json!({}));
        }
        serde_json::from_slice(body).map_err(|_| bad_request("invalid JSON"))
    };
    match (method, sub) {
        ("GET", "") => Ok(Action::List),
        ("POST", "") => {
            let req = json()?;
            if !req.is_object() {
                return Err(bad_request("expected a JSON object"));
            }
            if req.get("tenant_id").is_some_and(|t| !t.is_null()) {
                return Err(bad_request(
                    "'tenant_id' is not accepted here: a key is always created for the \
                     calling key's own tenant",
                ));
            }
            let description = match req.get("description") {
                None | Some(serde_json::Value::Null) => "",
                Some(serde_json::Value::String(text)) => text.as_str(),
                Some(_) => return Err(bad_request("'description' must be a string")),
            };
            if description.chars().count() > MAX_DESCRIPTION_LEN
                || description.chars().any(char::is_control)
            {
                return Err(bad_request(format!(
                    "invalid 'description' (at most {MAX_DESCRIPTION_LEN} characters, no \
                     control characters)"
                )));
            }
            let (scopes, namespaces) = parse_grant(&req)?;
            Ok(Action::Create(KeyRequest {
                description: description.to_string(),
                scopes,
                namespaces,
                expires_at: parse_expiry(&req, now)?,
            }))
        }
        ("POST", "/rotate") => {
            let req = json()?;
            Ok(Action::Rotate {
                key_hash: parse_key_ref(&req)?,
                overlap_secs: parse_overlap(&req)?,
                expires_at: parse_expiry(&req, now)?,
            })
        }
        ("POST", "/revoke") => Ok(Action::Revoke {
            key_hash: parse_key_ref(&json()?)?,
        }),
        _ => Err((404, "not found".into())),
    }
}

/// The answer for a key the caller may not manage, or that does not exist:
/// the two are not told apart.
#[cfg_attr(not(feature = "postgres"), allow(dead_code))]
fn not_manageable() -> Rejection {
    (
        404,
        "no active API key with that id among the keys this key may manage".into(),
    )
}

#[cfg(feature = "postgres")]
mod core {
    //! The registry operations behind the routes, for any front end.

    use super::{not_manageable, KeyRequest};
    use crate::api_key_admin::Rejection;
    use crate::auth::KeyStore;
    use kosha_control::{ApiKeyRecord, IssuedApiKey, KeyAuthority, PgStore, RotatedApiKey};

    fn store_error(what: &str, e: impl std::fmt::Display) -> Rejection {
        (500, format!("failed to {what}: {e}"))
    }

    /// The registry keys of `authority`'s tenant that it could have issued
    /// (not revoked; expired ones included), newest first.
    pub(crate) fn list_keys(
        pg: &PgStore,
        authority: &KeyAuthority,
    ) -> Result<Vec<ApiKeyRecord>, Rejection> {
        let keys = pg
            .list_api_keys(Some(&authority.tenant))
            .map_err(|e| store_error("list keys", e))?;
        Ok(keys
            .into_iter()
            .filter(|k| authority.can_manage(k))
            .collect())
    }

    /// Create a key within `authority` (403 when the request asks for more
    /// than it holds), audited as `actor`, and make it usable on this pod at
    /// once (`keys`; other pods on their next refresh).
    pub(crate) fn create_key(
        pg: &PgStore,
        keys: &KeyStore,
        authority: &KeyAuthority,
        request: KeyRequest,
        actor: &str,
    ) -> Result<IssuedApiKey, Rejection> {
        let new = authority
            .grant(
                request.description,
                request.scopes,
                request.namespaces,
                request.expires_at,
            )
            .map_err(|e| (403, e))?;
        let _registry = keys.registry_guard();
        let issued = pg
            .create_api_key_as(&new, actor)
            .map_err(|e| store_error("create key", e))?;
        keys.upsert_db_key(issued.record.clone());
        Ok(issued)
    }

    /// Rotate a key `authority` could have issued; the replacement expires
    /// no later than the authority (403 when `expires_at` asks for later).
    pub(crate) fn rotate_key(
        pg: &PgStore,
        keys: &KeyStore,
        authority: &KeyAuthority,
        key_hash: &str,
        overlap_secs: u64,
        expires_at: Option<i64>,
        actor: &str,
    ) -> Result<RotatedApiKey, Rejection> {
        let expires_at = authority.bound_expiry(expires_at).map_err(|e| (403, e))?;
        manageable(pg, authority, key_hash)?;
        let _registry = keys.registry_guard();
        let rotated = pg
            .rotate_api_key_as(
                key_hash,
                Some(&authority.tenant),
                overlap_secs,
                expires_at,
                actor,
            )
            .map_err(|e| store_error("rotate key", e))?
            .ok_or_else(not_manageable)?;
        keys.upsert_db_key(rotated.new.record.clone());
        // The old key keeps its attributes and gains the deadline.
        keys.upsert_db_key(ApiKeyRecord {
            key_hash: key_hash.to_string(),
            expires_at: Some(rotated.old_expires_at),
            ..rotated.new.record.clone()
        });
        Ok(rotated)
    }

    /// Revoke a key `authority` could have issued; this pod stops accepting
    /// it at once, the others on their next refresh.
    pub(crate) fn revoke_key(
        pg: &PgStore,
        keys: &KeyStore,
        authority: &KeyAuthority,
        key_hash: &str,
        actor: &str,
    ) -> Result<(), Rejection> {
        manageable(pg, authority, key_hash)?;
        let _registry = keys.registry_guard();
        if !pg
            .revoke_api_key_as(key_hash, Some(&authority.tenant), actor)
            .map_err(|e| store_error("revoke key", e))?
        {
            return Err(not_manageable());
        }
        keys.remove_db_key(key_hash);
        Ok(())
    }

    /// The key, if `authority` may manage it. A key's tenant, scopes and
    /// namespaces never change after it is created, so this check cannot go
    /// stale before the change it guards (which is also bound to the
    /// tenant in SQL).
    fn manageable(
        pg: &PgStore,
        authority: &KeyAuthority,
        key_hash: &str,
    ) -> Result<ApiKeyRecord, Rejection> {
        pg.get_api_key(key_hash)
            .map_err(|e| store_error("read key", e))?
            .filter(|record| authority.can_manage(record))
            .ok_or_else(not_manageable)
    }
}

#[cfg(feature = "postgres")]
pub(crate) use core::{create_key, list_keys, revoke_key, rotate_key};

#[cfg(not(feature = "postgres"))]
fn execute(
    _action: Action,
    _authority: &KeyAuthority,
    _actor: &str,
    _caller_key: Option<&str>,
) -> String {
    json_error(
        400,
        "API key management requires the postgres feature (compile with --features postgres)",
    )
}

#[cfg(feature = "postgres")]
fn execute(
    action: Action,
    authority: &KeyAuthority,
    actor: &str,
    caller_key: Option<&str>,
) -> String {
    use crate::api_key_admin::record_json;
    use crate::{auth, json_ok};
    let auth::Auth::Keys(keys) = auth::global() else {
        return json_error(400, "API keys are disabled (KOSHA_AUTH=disabled)");
    };
    if std::env::var("DATABASE_URL").is_err() {
        return json_error(400, "Postgres not configured (DATABASE_URL not set)");
    }
    let Some(pg) = keys.pg() else {
        return json_error(500, "failed to connect to postgres");
    };
    let now = auth::unix_now();
    let issued_json = |issued: &kosha_control::IssuedApiKey| {
        let mut value = record_json(&issued.record, now);
        value["api_key"] = serde_json::json!(issued.api_key);
        value
    };
    let result = match action {
        Action::List => list_keys(&pg, authority).map(|records| {
            let listed: Vec<_> = records
                .iter()
                .map(|record| {
                    let mut value = record_json(record, now);
                    value["current"] =
                        serde_json::json!(caller_key == Some(record.key_hash.as_str()));
                    value
                })
                .collect();
            serde_json::json!({ "api_keys": listed })
        }),
        Action::Create(request) => {
            create_key(&pg, keys, authority, request, actor).map(|issued| issued_json(&issued))
        }
        Action::Rotate {
            key_hash,
            overlap_secs,
            expires_at,
        } => rotate_key(
            &pg,
            keys,
            authority,
            &key_hash,
            overlap_secs,
            expires_at,
            actor,
        )
        .map(|rotated| {
            let mut value = issued_json(&rotated.new);
            value["replaces"] = serde_json::json!({
                "key_id": key_hash,
                "expires_at": rotated.old_expires_at,
            });
            value
        }),
        Action::Revoke { key_hash } => revoke_key(&pg, keys, authority, &key_hash, actor)
            .map(|()| serde_json::json!({ "revoked": true, "key_id": key_hash })),
    };
    match result {
        Ok(value) => json_ok(&value),
        Err((status, message)) => json_error(status, &message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Principal;
    use kosha_control::{hash_api_key, Scope};

    const NOW: i64 = 1_800_000_000;

    fn post(sub: &str, body: &str) -> Result<Action, Rejection> {
        parse("POST", sub, body.as_bytes(), NOW)
    }

    #[test]
    fn create_leaves_every_attribute_to_the_caller_by_default() {
        let expected = Action::Create(KeyRequest {
            description: String::new(),
            scopes: None,
            namespaces: None,
            expires_at: None,
        });
        assert_eq!(post("", "{}"), Ok(expected));
        // An empty body is an empty request.
        assert_eq!(post("", ""), post("", "{}"));
        assert_eq!(
            post(
                "",
                r#"{"description":"ci","scopes":["read"],"namespaces":["docs","docs"],
                    "expires_in_secs":60}"#
            ),
            Ok(Action::Create(KeyRequest {
                description: "ci".into(),
                scopes: Some(Scopes::of(&[Scope::Read])),
                namespaces: Some(vec!["docs".into()]),
                expires_at: Some(NOW + 60),
            }))
        );
    }

    #[test]
    fn create_refuses_another_tenant_and_bad_input() {
        for (body, needle) in [
            (r#"{"tenant_id":"other"}"#, "tenant_id"),
            (r#"{"scopes":["root"]}"#, "root"),
            (r#"{"scopes":[]}"#, "at least one scope"),
            (r#"{"namespaces":[]}"#, "at least one"),
            (r#"{"namespaces":["a/b"]}"#, "a/b"),
            (r#"{"description":7}"#, "description"),
            (r#"{"description":"a\nb"}"#, "description"),
            (r#"{"expires_in_secs":0}"#, "positive"),
            ("[]", "object"),
            ("not json", "invalid JSON"),
        ] {
            let (status, message) = post("", body).unwrap_err();
            assert_eq!(status, 400, "{body}: {message}");
            assert!(message.contains(needle), "{body}: {message}");
        }
    }

    #[test]
    fn rotate_and_revoke_take_a_key_reference() {
        let id = hash_api_key("sk-abc");
        assert_eq!(
            post(
                "/rotate",
                &format!(r#"{{"key_id":"{id}","overlap_secs":5}}"#)
            ),
            Ok(Action::Rotate {
                key_hash: id.clone(),
                overlap_secs: 5,
                expires_at: None,
            })
        );
        assert_eq!(
            post("/revoke", r#"{"api_key":"sk-abc"}"#),
            Ok(Action::Revoke {
                key_hash: id.clone()
            })
        );
        assert_eq!(post("/revoke", "{}").unwrap_err().0, 400);
        assert_eq!(post("/nope", "{}").unwrap_err().0, 404);
        assert_eq!(parse("DELETE", "", b"", NOW).unwrap_err().0, 404);
        assert_eq!(parse("GET", "", b"", NOW), Ok(Action::List));
    }

    #[test]
    fn only_its_own_routes_and_only_tenant_keys_with_admin() {
        assert_eq!(handle("GET /v1/api-keysx HTTP/1.1", b""), None);
        assert_eq!(handle("GET /v1/admin/api-keys HTTP/1.1", b""), None);
        // No tenant key behind the request (the admin credential).
        let response = handle("GET /v1/api-keys HTTP/1.1", b"").unwrap();
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        assert!(response.contains("/v1/admin/api-keys"), "{response}");
        // A key without the admin scope.
        let read_only = Principal {
            scopes: Scopes::of(&[Scope::Read]),
            ..Principal::full("acme")
        };
        let response = crate::authz::with_principal(Some(read_only), || {
            handle("GET /v1/api-keys HTTP/1.1", b"").unwrap()
        });
        assert!(response.starts_with("HTTP/1.1 403"), "{response}");
        // Malformed requests are refused before anything else.
        let response = handle("POST /v1/api-keys HTTP/1.1", b"nope").unwrap();
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    }

    #[test]
    fn the_caller_is_named_and_bounded_by_its_own_key() {
        let caller = Principal {
            scopes: Scopes::of(&[Scope::Read, Scope::Admin]),
            namespaces: Some(vec!["docs".to_string()].into()),
            expires_at: Some(NOW + 100),
            key_id: Some(hash_api_key("sk-caller")),
            ..Principal::full("acme")
        };
        assert_eq!(
            caller.audit_label(),
            format!("api_key:{}", &hash_api_key("sk-caller")[..12])
        );
        assert_eq!(Principal::full("acme").audit_label(), "api_key:env");
        let authority = caller.authority();
        assert_eq!(authority.tenant, "acme");
        assert_eq!(authority.namespaces, Some(vec!["docs".to_string()]));
        // What a create with no attributes becomes.
        let new = authority.grant(String::new(), None, None, None).unwrap();
        assert_eq!(new.expires_at, Some(NOW + 100));
        assert_eq!(new.scopes, Scopes::of(&[Scope::Read, Scope::Admin]));
        assert!(authority
            .grant(String::new(), Some(Scopes::ALL), None, None)
            .is_err());
        assert!(authority.bound_expiry(Some(NOW + 101)).is_err());
    }
}
