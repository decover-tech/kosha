//! Admin API for organizations, users, memberships, projects, tenants and
//! the audit log (#345). The model and its rules are `kosha_control::orgs`;
//! this is the HTTP surface.
//!
//! Every route is under `/v1/admin/` and so needs an admin credential:
//! `KOSHA_ADMIN_API_KEY` or a named operator credential (`route` checks it
//! before calling [`handle`]). The caller is then one of:
//!   - the operator: no `X-Kosha-Actor` header. Not bound by roles. Audited
//!     as `operator`, or as `operator:<name>` with an operator credential.
//!   - a user: `X-Kosha-Actor: <user id>`. The admin credential's holder (a
//!     console or sign-up service that authenticated the user itself) vouches
//!     for the id; the user's role in the organization is enforced here.
//!
//! ```text
//! POST   /v1/admin/users                                  {email, display_name?}
//! GET    /v1/admin/users/{user}
//! POST   /v1/admin/orgs                                   {slug, name, owner_user_id?}
//! GET    /v1/admin/orgs/{org}
//! GET    /v1/admin/orgs/{org}/members
//! POST   /v1/admin/orgs/{org}/members                     {user_id, role}
//! POST   /v1/admin/orgs/{org}/members/{user}/role         {role}
//! DELETE /v1/admin/orgs/{org}/members/{user}
//! GET    /v1/admin/orgs/{org}/projects
//! POST   /v1/admin/orgs/{org}/projects                    {slug, name}
//! GET    /v1/admin/orgs/{org}/tenants
//! POST   /v1/admin/orgs/{org}/tenants                     {project_id?, tenant_id?}
//! POST   /v1/admin/orgs/{org}/tenants/{tenant}/api-keys   {description?}
//! GET    /v1/admin/orgs/{org}/audit-log?limit=&before=
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use kosha_control::orgs::{self, Actor, MemoryOrgStore, OrgError, OrgStore, Role};
use serde_json::{json, Value};

use crate::auth::KeyStore;
use crate::{json_error, json_ok, url_decode};

/// The header naming the user a request acts for.
const ACTOR_HEADER: &str = "x-kosha-actor";

/// Where the organization model is stored: Postgres when the server has it
/// (`DATABASE_URL` and the `postgres` feature), otherwise in this process,
/// like the in-memory control plane.
pub(crate) struct OrgBackend {
    memory: Arc<MemoryOrgStore>,
    #[cfg(feature = "postgres")]
    database_url: Option<String>,
    /// Used when there is no key store to share a pool with
    /// (`KOSHA_AUTH=disabled`). Connected on first use.
    #[cfg(feature = "postgres")]
    pg: std::sync::Mutex<Option<Arc<kosha_control::PgStore>>>,
}

impl OrgBackend {
    pub(crate) fn in_memory() -> Self {
        Self {
            memory: Arc::new(MemoryOrgStore::new()),
            #[cfg(feature = "postgres")]
            database_url: None,
            #[cfg(feature = "postgres")]
            pg: std::sync::Mutex::new(None),
        }
    }

    /// Every audit row of the in-memory store, oldest first (tests).
    #[cfg(test)]
    pub(crate) fn audit_rows(&self) -> Vec<kosha_control::orgs::AuditEvent> {
        self.memory.audit_log()
    }

    pub(crate) fn from_env() -> Self {
        #[allow(unused_mut)]
        let mut backend = Self::in_memory();
        #[cfg(feature = "postgres")]
        {
            backend.database_url = std::env::var("DATABASE_URL").ok().filter(|u| !u.is_empty());
        }
        backend
    }

    /// The organization store: Postgres when configured, else this
    /// process's memory. Also where `admin_audit` writes its rows.
    pub(crate) fn store(&self) -> Result<Arc<dyn OrgStore>, String> {
        #[cfg(feature = "postgres")]
        if let Some(url) = &self.database_url {
            // One pool per process: the key store's, when there is one.
            if let crate::auth::Auth::Keys(keys) = crate::auth::global() {
                if let Some(pg) = keys.pg() {
                    return Ok(pg);
                }
            }
            let mut own = self.pg.lock().unwrap_or_else(|e| e.into_inner());
            if own.is_none() {
                let store = kosha_control::PgStore::new(url)
                    .map_err(|e| format!("failed to connect to postgres: {e}"))?;
                *own = Some(Arc::new(store));
            }
            if let Some(pg) = own.clone() {
                return Ok(pg);
            }
        }
        Ok(self.memory.clone())
    }
}

fn error_response(e: &OrgError) -> String {
    let status = match e {
        OrgError::Invalid(_) => 400,
        OrgError::Forbidden(_) => 403,
        OrgError::NotFound(_) => 404,
        OrgError::Conflict(_) => 409,
        OrgError::Store(_) => 500,
    };
    json_error(status, &e.to_string())
}

fn respond<T>(result: Result<T, OrgError>, body: impl FnOnce(T) -> Value) -> String {
    match result {
        Ok(value) => json_ok(&body(value)),
        Err(e) => error_response(&e),
    }
}

fn actor_header(headers: &HashMap<String, String>) -> Option<&str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(ACTOR_HEADER))
        .map(|(_, v)| v.trim())
}

/// Whether the request names a user to act for, even an empty one. `route`
/// refuses such a request on every admin route outside this module: those
/// are operator-only and do not check roles.
pub(crate) fn names_actor(headers: &HashMap<String, String>) -> bool {
    actor_header(headers).is_some()
}

/// Who the request acts as: the user it names, else `operator`, the admin
/// credential's holder that `route` authenticated. A user named under a
/// named operator's credential stays tied to it ([`Actor::VouchedUser`]):
/// the audit row says which credential vouched for them.
fn actor(headers: &HashMap<String, String>, operator: &Actor) -> Result<Actor, OrgError> {
    match actor_header(headers) {
        None => Ok(operator.clone()),
        // A present but empty header is a caller bug, and must not be read
        // as "no header": that would turn a user request into an operator's.
        Some("") => Err(OrgError::Invalid(format!("empty '{ACTOR_HEADER}' header"))),
        Some(user_id) => Ok(match operator {
            Actor::NamedOperator(name) => Actor::VouchedUser {
                user_id: user_id.to_string(),
                operator: name.clone(),
            },
            _ => Actor::User(user_id.to_string()),
        }),
    }
}

fn parse_body(body: &[u8]) -> Result<Value, OrgError> {
    // A body-less POST is an empty object, so routes whose fields are all
    // optional need no body.
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!({}));
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(value) if value.is_object() => Ok(value),
        _ => Err(OrgError::Invalid("body must be a JSON object".into())),
    }
}

/// An optional string field; present with another type is an error.
fn optional<'a>(body: &'a Value, field: &str) -> Result<Option<&'a str>, OrgError> {
    match body.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(OrgError::Invalid(format!("'{field}' must be a string"))),
    }
}

fn required<'a>(body: &'a Value, field: &str) -> Result<&'a str, OrgError> {
    optional(body, field)?.ok_or_else(|| OrgError::Invalid(format!("missing '{field}'")))
}

fn role_field(body: &Value) -> Result<Role, OrgError> {
    let role = required(body, "role")?;
    Role::parse(role).ok_or_else(|| {
        OrgError::Invalid("invalid 'role': one of owner, admin, member, read-only".into())
    })
}

fn query_number<T: std::str::FromStr>(query: &str, name: &str) -> Result<Option<T>, OrgError> {
    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            if key == name {
                return value
                    .parse()
                    .map(Some)
                    .map_err(|_| OrgError::Invalid(format!("invalid '{name}'")));
            }
        }
    }
    Ok(None)
}

/// Answer an organization-model route, or `None` when `path` is not one.
///
/// `keys` is the server's API-key table, `None` under `KOSHA_AUTH=disabled`:
/// a created key is added to it so it works on this pod at once (other pods
/// pick it up on their next refresh).
pub(crate) fn handle(
    method: &str,
    path: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
    backend: &OrgBackend,
    keys: Option<&KeyStore>,
    operator: &Actor,
) -> Option<String> {
    let rest = path.strip_prefix("/v1/admin/")?;
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let segments: Vec<String> = rest.split('/').map(url_decode).collect();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    if !matches!(segments.first(), Some(&"users") | Some(&"orgs")) {
        return None;
    }
    let result = backend.store().map_err(OrgError::Store).and_then(|store| {
        let actor = actor(headers, operator)?;
        dispatch(method, &segments, query, &actor, body, &*store, keys)
    });
    Some(result.unwrap_or_else(|e| error_response(&e)))
}

fn dispatch(
    method: &str,
    segments: &[&str],
    query: &str,
    actor: &Actor,
    body: &[u8],
    store: &dyn OrgStore,
    keys: Option<&KeyStore>,
) -> Result<String, OrgError> {
    Ok(match (method, segments) {
        ("POST", ["users"]) => {
            let body = parse_body(body)?;
            let email = required(&body, "email")?;
            let display_name = optional(&body, "display_name")?.unwrap_or("");
            respond(
                orgs::create_user(store, actor, email, display_name),
                |user| json!(user),
            )
        }
        ("GET", ["users", user_id]) => {
            respond(orgs::get_user(store, actor, user_id), |user| json!(user))
        }
        ("POST", ["orgs"]) => {
            let body = parse_body(body)?;
            let slug = required(&body, "slug")?;
            let name = required(&body, "name")?;
            let owner = optional(&body, "owner_user_id")?;
            respond(orgs::create_org(store, actor, slug, name, owner), |org| {
                json!(org)
            })
        }
        ("GET", ["orgs", org]) => respond(orgs::get_org(store, actor, org), |org| json!(org)),
        ("GET", ["orgs", org, "members"]) => respond(
            orgs::list_members(store, actor, org),
            |members| json!({ "members": members }),
        ),
        ("POST", ["orgs", org, "members"]) => {
            let body = parse_body(body)?;
            let user_id = required(&body, "user_id")?;
            let role = role_field(&body)?;
            respond(
                orgs::add_member(store, actor, org, user_id, role),
                |()| json!({ "org_id": org, "user_id": user_id, "role": role }),
            )
        }
        ("POST", ["orgs", org, "members", user_id, "role"]) => {
            let role = role_field(&parse_body(body)?)?;
            respond(
                orgs::set_member_role(store, actor, org, user_id, role),
                |()| json!({ "org_id": org, "user_id": user_id, "role": role }),
            )
        }
        ("DELETE", ["orgs", org, "members", user_id]) => respond(
            orgs::remove_member(store, actor, org, user_id),
            |()| json!({ "org_id": org, "user_id": user_id, "removed": true }),
        ),
        ("GET", ["orgs", org, "projects"]) => respond(
            orgs::list_projects(store, actor, org),
            |projects| json!({ "projects": projects }),
        ),
        ("POST", ["orgs", org, "projects"]) => {
            let body = parse_body(body)?;
            let slug = required(&body, "slug")?;
            let name = required(&body, "name")?;
            respond(
                orgs::create_project(store, actor, org, slug, name),
                |project| json!(project),
            )
        }
        ("GET", ["orgs", org, "tenants"]) => respond(
            orgs::list_tenants(store, actor, org),
            |tenants| json!({ "tenants": tenants }),
        ),
        ("POST", ["orgs", org, "tenants"]) => {
            let body = parse_body(body)?;
            let tenant_id = optional(&body, "tenant_id")?;
            let project_id = optional(&body, "project_id")?;
            respond(
                orgs::link_tenant(store, actor, org, tenant_id, project_id),
                |tenant| json!(tenant),
            )
        }
        ("POST", ["orgs", org, "tenants", tenant_id, "api-keys"]) => {
            let body = parse_body(body)?;
            let description = optional(&body, "description")?.unwrap_or("");
            let Some(keys) = keys else {
                return Err(OrgError::Invalid(
                    "API keys are disabled (KOSHA_AUTH=disabled)".into(),
                ));
            };
            respond(
                orgs::create_api_key(store, actor, org, tenant_id, description),
                |key| {
                    // A full-access key that never expires, as an operator-made
                    // key is by default (#346).
                    keys.upsert_db_key(kosha_control::ApiKeyRecord {
                        key_hash: kosha_control::hash_api_key(&key.api_key),
                        tenant_id: key.tenant_id.clone(),
                        description: key.description.clone(),
                        scopes: kosha_control::Scopes::ALL,
                        namespaces: None,
                        expires_at: None,
                        last_used_at: None,
                        created_at: crate::auth::unix_now(),
                    });
                    json!(key)
                },
            )
        }
        ("GET", ["orgs", org, "audit-log"]) => {
            let limit = query_number::<u32>(query, "limit")?;
            let before = query_number::<i64>(query, "before")?;
            respond(
                orgs::list_audit(store, actor, org, limit, before),
                |events| {
                    // The cursor for the next (older) page; null on the
                    // page that reaches the start of the log.
                    let page = limit
                        .unwrap_or(orgs::DEFAULT_AUDIT_PAGE)
                        .clamp(1, orgs::MAX_LIST);
                    let next = events
                        .last()
                        .filter(|_| events.len() as u32 == page)
                        .map(|e| e.id);
                    json!({ "events": events, "next_before": next })
                },
            )
        }
        _ => json_error(404, "not found"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{route, AppState};

    const ADMIN_KEY: &str = "admin-secret";

    struct Api {
        state: AppState,
    }

    impl Api {
        fn new() -> Self {
            let mut state = AppState::new(crate::tests::test_dir());
            state.admin_auth_key = Some(ADMIN_KEY.into());
            state.orgs = OrgBackend::in_memory();
            Api { state }
        }

        /// One request through the server's `route`, as the operator or as
        /// `actor`. Returns the status and the JSON body.
        fn call(&self, actor: Option<&str>, method: &str, path: &str, body: Value) -> (u16, Value) {
            let mut headers =
                HashMap::from([("authorization".to_string(), format!("Bearer {ADMIN_KEY}"))]);
            if let Some(actor) = actor {
                headers.insert(ACTOR_HEADER.to_string(), actor.to_string());
            }
            let body = if body.is_null() {
                Vec::new()
            } else {
                body.to_string().into_bytes()
            };
            let request_line = format!("{method} {path} HTTP/1.1");
            parse(&route(
                &request_line,
                &headers,
                &body,
                "service",
                &self.state,
            ))
        }

        fn ok(&self, actor: Option<&str>, method: &str, path: &str, body: Value) -> Value {
            let (status, value) = self.call(actor, method, path, body);
            assert_eq!(status, 200, "{method} {path}: {value}");
            value
        }

        fn status(&self, actor: Option<&str>, method: &str, path: &str, body: Value) -> u16 {
            self.call(actor, method, path, body).0
        }

        fn user(&self, name: &str) -> String {
            let email = format!("{name}@example.com");
            let user = self.ok(None, "POST", "/v1/admin/users", json!({ "email": email }));
            user["id"].as_str().unwrap().to_string()
        }
    }

    fn parse(response: &str) -> (u16, Value) {
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let status = head.split(' ').nth(1).unwrap().parse().unwrap();
        (status, serde_json::from_str(body).unwrap())
    }

    #[test]
    fn org_routes_need_the_admin_credential() {
        let api = Api::new();
        for (method, path) in [
            ("POST", "/v1/admin/users"),
            ("GET", "/v1/admin/users/usr_1"),
            ("POST", "/v1/admin/orgs"),
            ("GET", "/v1/admin/orgs/org_1/members"),
            ("DELETE", "/v1/admin/orgs/org_1/members/usr_1"),
            ("GET", "/v1/admin/orgs/org_1/audit-log"),
        ] {
            for headers in [
                HashMap::new(),
                HashMap::from([("authorization".to_string(), "Bearer tenant-key".to_string())]),
                // Naming an actor is not a credential.
                HashMap::from([(ACTOR_HEADER.to_string(), "usr_1".to_string())]),
            ] {
                let line = format!("{method} {path} HTTP/1.1");
                let response = route(&line, &headers, b"{}", "acme", &api.state);
                assert!(response.starts_with("HTTP/1.1 403"), "{line}: {response}");
            }
        }
    }

    #[test]
    fn sign_up_shaped_flow_from_user_to_working_key() {
        let api = Api::new();
        let ada = api.user("ada");
        let grace = api.user("grace");

        // Ada creates her organization and is its owner.
        let org = api.ok(
            Some(&ada),
            "POST",
            "/v1/admin/orgs",
            json!({ "slug": "acme", "name": "Acme" }),
        );
        let org_id = org["id"].as_str().unwrap();
        let base = format!("/v1/admin/orgs/{org_id}");
        assert_eq!(api.ok(Some(&ada), "GET", &base, Value::Null), org);
        let members = api.ok(Some(&ada), "GET", &format!("{base}/members"), Value::Null);
        assert_eq!(members["members"][0]["user_id"], json!(ada));
        assert_eq!(members["members"][0]["role"], json!("owner"));
        assert_eq!(members["members"][0]["email"], json!("ada@example.com"));

        // Grace is nobody there until Ada adds her.
        assert_eq!(api.status(Some(&grace), "GET", &base, Value::Null), 403);
        api.ok(
            Some(&ada),
            "POST",
            &format!("{base}/members"),
            json!({ "user_id": grace, "role": "read-only" }),
        );
        assert_eq!(api.status(Some(&grace), "GET", &base, Value::Null), 200);

        // A project, a tenant under it, and a key for the tenant.
        let project = api.ok(
            Some(&ada),
            "POST",
            &format!("{base}/projects"),
            json!({ "slug": "search", "name": "Search" }),
        );
        let tenant = api.ok(
            Some(&ada),
            "POST",
            &format!("{base}/tenants"),
            json!({ "project_id": project["id"] }),
        );
        let tenant_id = tenant["tenant_id"].as_str().unwrap();
        assert!(tenant_id.starts_with("acme-"), "{tenant_id}");
        assert_eq!(
            api.ok(Some(&grace), "GET", &format!("{base}/tenants"), Value::Null),
            json!({ "tenants": [tenant] })
        );
        assert_eq!(
            api.ok(
                Some(&grace),
                "GET",
                &format!("{base}/projects"),
                Value::Null
            ),
            json!({ "projects": [project] })
        );

        // Read-only cannot mint keys; after a promotion she can. The key
        // route is driven through `handle` with a key table, because unit
        // tests run with authentication disabled (see the test below).
        let keys = KeyStore::with_keys(HashMap::new());
        let key_path = format!("{base}/tenants/{tenant_id}/api-keys");
        let create_key = |actor: &str| {
            let headers = HashMap::from([(ACTOR_HEADER.to_string(), actor.to_string())]);
            let body = br#"{"description": "ci"}"#;
            let response = handle(
                "POST",
                &key_path,
                &headers,
                body,
                &api.state.orgs,
                Some(&keys),
                &Actor::Operator,
            );
            parse(&response.unwrap())
        };
        assert_eq!(create_key(&grace).0, 403);
        assert_eq!(keys.len(), 0);
        api.ok(
            Some(&ada),
            "POST",
            &format!("{base}/members/{grace}/role"),
            json!({ "role": "member" }),
        );
        let (status, key) = create_key(&grace);
        assert_eq!(status, 200, "{key}");
        assert_eq!(key["tenant_id"], json!(tenant_id));
        // The key authenticates as the tenant on this pod straight away.
        let api_key = key["api_key"].as_str().unwrap();
        let tenant = keys.lookup(api_key).ok().map(|p| p.tenant);
        assert_eq!(tenant.as_deref(), Some(tenant_id));

        // Ada removes Grace; the audit log has the whole story and no key.
        api.ok(
            Some(&ada),
            "DELETE",
            &format!("{base}/members/{grace}"),
            Value::Null,
        );
        assert_eq!(api.status(Some(&grace), "GET", &base, Value::Null), 403);
        let log = api.ok(Some(&ada), "GET", &format!("{base}/audit-log"), Value::Null);
        let actions: Vec<(&str, &str)> = log["events"]
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .map(|e| (e["actor"].as_str().unwrap(), e["action"].as_str().unwrap()))
            .collect();
        let (by_ada, by_grace) = (format!("user:{ada}"), format!("user:{grace}"));
        assert_eq!(
            actions,
            vec![
                (by_ada.as_str(), "org.created"),
                (by_ada.as_str(), "member.added"),
                (by_ada.as_str(), "project.created"),
                (by_ada.as_str(), "tenant.linked"),
                (by_ada.as_str(), "member.role_changed"),
                (by_grace.as_str(), "api_key.created"),
                (by_ada.as_str(), "member.removed"),
            ]
        );
        assert_eq!(log["next_before"], Value::Null);
        assert!(!log.to_string().contains(api_key));

        // Paging: two per page, following `next_before`.
        let page = api.ok(
            Some(&ada),
            "GET",
            &format!("{base}/audit-log?limit=2"),
            Value::Null,
        );
        assert_eq!(page["events"].as_array().unwrap().len(), 2);
        assert_eq!(page["events"][0]["action"], json!("member.removed"));
        let next = page["next_before"].as_i64().unwrap();
        let page = api.ok(
            Some(&ada),
            "GET",
            &format!("{base}/audit-log?limit=2&before={next}"),
            Value::Null,
        );
        assert_eq!(page["events"][0]["action"], json!("member.role_changed"));
        assert_eq!(
            api.status(
                Some(&ada),
                "GET",
                &format!("{base}/audit-log?limit=x"),
                Value::Null
            ),
            400
        );
    }

    #[test]
    fn statuses_for_refused_requests() {
        let api = Api::new();
        let ada = api.user("ada");
        let org = api.ok(
            None,
            "POST",
            "/v1/admin/orgs",
            json!({ "slug": "acme", "name": "Acme", "owner_user_id": ada }),
        );
        let base = format!("/v1/admin/orgs/{}", org["id"].as_str().unwrap());
        let members = format!("{base}/members");

        // 400: malformed bodies and fields.
        let users = "/v1/admin/users";
        assert_eq!(api.status(None, "POST", users, json!({})), 400);
        assert_eq!(api.status(None, "POST", users, json!({ "email": 7 })), 400);
        assert_eq!(api.status(None, "POST", users, json!([1])), 400);
        assert_eq!(
            api.status(None, "POST", users, json!({ "email": "nope" })),
            400
        );
        assert_eq!(
            api.status(
                None,
                "POST",
                &members,
                json!({ "user_id": ada, "role": "god" })
            ),
            400
        );
        assert_eq!(
            api.status(
                None,
                "POST",
                "/v1/admin/orgs",
                json!({ "slug": "x", "name": "X" })
            ),
            400,
            "the operator must name the owner"
        );
        assert_eq!(api.status(Some(" "), "GET", &base, Value::Null), 400);
        // 403: a user doing an operator's job, or acting where they have no role.
        assert_eq!(
            api.status(
                Some(&ada),
                "POST",
                users,
                json!({ "email": "x@example.com" })
            ),
            403
        );
        assert_eq!(
            api.status(
                Some(&ada),
                "POST",
                &format!("{base}/tenants"),
                json!({ "tenant_id": "someone-elses-tenant" })
            ),
            403
        );
        assert_eq!(
            api.status(Some("usr_nobody"), "GET", &base, Value::Null),
            403
        );
        // 404: unknown things and unknown routes.
        assert_eq!(
            api.status(None, "GET", "/v1/admin/orgs/org_missing", Value::Null),
            404
        );
        assert_eq!(
            api.status(None, "GET", "/v1/admin/users/usr_missing", Value::Null),
            404
        );
        assert_eq!(
            api.status(None, "GET", &format!("{base}/nope"), Value::Null),
            404
        );
        assert_eq!(api.status(None, "PUT", &base, json!({})), 404);
        // 409: duplicates and the last owner.
        assert_eq!(
            api.status(None, "POST", users, json!({ "email": "ADA@example.com" })),
            409
        );
        assert_eq!(
            api.status(
                None,
                "POST",
                "/v1/admin/orgs",
                json!({ "slug": "acme", "name": "Again", "owner_user_id": ada })
            ),
            409
        );
        assert_eq!(
            api.status(None, "DELETE", &format!("{members}/{ada}"), Value::Null),
            409
        );

        // The operator links an existing tenant id; a second link is refused.
        let tenants = format!("{base}/tenants");
        let linked = api.ok(None, "POST", &tenants, json!({ "tenant_id": "legacy-co" }));
        assert_eq!(linked["tenant_id"], json!("legacy-co"));
        assert_eq!(
            api.status(None, "POST", &tenants, json!({ "tenant_id": "legacy-co" })),
            409
        );
    }

    /// Unit tests run with authentication disabled, where there is no key
    /// table for a new key to join: the key route says so and creates
    /// nothing, like `POST /v1/admin/api-keys`.
    #[test]
    fn key_route_refuses_when_authentication_is_disabled() {
        let api = Api::new();
        let ada = api.user("ada");
        let org = api.ok(
            Some(&ada),
            "POST",
            "/v1/admin/orgs",
            json!({ "slug": "acme", "name": "Acme" }),
        );
        let base = format!("/v1/admin/orgs/{}", org["id"].as_str().unwrap());
        let tenant = api.ok(Some(&ada), "POST", &format!("{base}/tenants"), Value::Null);
        let path = format!(
            "{base}/tenants/{}/api-keys",
            tenant["tenant_id"].as_str().unwrap()
        );
        let (status, body) = api.call(Some(&ada), "POST", &path, Value::Null);
        assert_eq!(status, 400, "{body}");
        let log = api.ok(Some(&ada), "GET", &format!("{base}/audit-log"), Value::Null);
        assert!(!log.to_string().contains("api_key.created"));
    }

    /// A query pod forwards organization writes to the ingest pod, like
    /// every other admin write.
    #[test]
    fn org_writes_are_write_paths() {
        for line in [
            "POST /v1/admin/users HTTP/1.1",
            "POST /v1/admin/orgs/org_1/members HTTP/1.1",
            "DELETE /v1/admin/orgs/org_1/members/usr_1 HTTP/1.1",
        ] {
            assert!(crate::is_write_path(line), "{line}");
        }
        assert!(!crate::is_write_path("GET /v1/admin/orgs/org_1 HTTP/1.1"));
    }

    /// Acting for a user never reaches an operator-only admin route, where
    /// no role would be checked: `POST /v1/admin/api-keys` would mint a key
    /// for any tenant.
    #[test]
    fn operator_only_admin_routes_refuse_an_actor() {
        let api = Api::new();
        let ada = api.user("ada");
        for (method, path) in [
            ("POST", "/v1/admin/api-keys"),
            ("POST", "/v1/admin/gc"),
            ("POST", "/v1/admin/compact-namespace"),
            ("GET", "/v1/admin/compaction-status"),
            ("POST", "/v1/admin/no-such-route"),
        ] {
            let body = json!({ "tenant_id": "someone-elses-tenant" });
            for actor in [ada.as_str(), ""] {
                let (status, value) = api.call(Some(actor), method, path, body.clone());
                assert_eq!(status, 403, "{method} {path} as {actor:?}: {value}");
            }
        }
        // Without the header the operator still reaches them.
        let (status, _) = api.call(None, "GET", "/v1/admin/compaction-status", Value::Null);
        assert_eq!(status, 200);
    }

    #[test]
    fn other_admin_routes_are_not_claimed() {
        let backend = OrgBackend::in_memory();
        for path in [
            "/v1/admin/api-keys",
            "/v1/admin/gc",
            "/v1/admin/usersx",
            "/v1/namespaces/orgs",
        ] {
            let none = handle(
                "POST",
                path,
                &HashMap::new(),
                b"{}",
                &backend,
                None,
                &Actor::Operator,
            );
            assert!(none.is_none());
        }
    }

    /// #346: what an admin credential's holder does is audited under its
    /// own name when it is a named operator credential, and as the shared
    /// `operator` with the bootstrap key. A user the holder vouches for is
    /// still audited (and bound by roles) as that user.
    #[test]
    fn a_named_operator_is_the_audited_actor() {
        let backend = OrgBackend::in_memory();
        let alice = Actor::NamedOperator("alice".into());
        let call = |operator: &Actor, user: Option<&str>, method: &str, path: &str, body: Value| {
            let headers = match user {
                Some(id) => HashMap::from([(ACTOR_HEADER.to_string(), id.to_string())]),
                None => HashMap::new(),
            };
            let body = if body.is_null() {
                Vec::new()
            } else {
                body.to_string().into_bytes()
            };
            parse(&handle(method, path, &headers, &body, &backend, None, operator).unwrap())
        };
        let ok = |operator: &Actor, user: Option<&str>, method: &str, path: &str, body: Value| {
            let (status, value) = call(operator, user, method, path, body);
            assert_eq!(status, 200, "{method} {path}: {value}");
            value
        };

        // A named operator has the operator's powers: creating users, and
        // an organization for someone else.
        let user = ok(
            &alice,
            None,
            "POST",
            "/v1/admin/users",
            json!({ "email": "ada@example.com" }),
        );
        let ada = user["id"].as_str().unwrap();
        let org = ok(
            &alice,
            None,
            "POST",
            "/v1/admin/orgs",
            json!({ "slug": "acme", "name": "Acme", "owner_user_id": ada }),
        );
        let base = format!("/v1/admin/orgs/{}", org["id"].as_str().unwrap());
        // The bootstrap key's holder, and a user alice's credential vouches for.
        ok(
            &Actor::Operator,
            None,
            "POST",
            &format!("{base}/projects"),
            json!({ "slug": "one", "name": "One" }),
        );
        ok(
            &alice,
            Some(ada),
            "POST",
            &format!("{base}/projects"),
            json!({ "slug": "two", "name": "Two" }),
        );
        // Not being a member does not stop a named operator reading the log.
        let log = ok(
            &alice,
            None,
            "GET",
            &format!("{base}/audit-log"),
            Value::Null,
        );
        let actions: Vec<(&str, &str)> = log["events"]
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .map(|e| (e["actor"].as_str().unwrap(), e["action"].as_str().unwrap()))
            .collect();
        let by_ada = format!("user:{ada}");
        assert_eq!(
            actions,
            vec![
                ("operator:alice", "org.created"),
                ("operator", "project.created"),
                (by_ada.as_str(), "project.created"),
            ]
        );
    }

    /// Naming a user must not make a named operator's action unattributable:
    /// the row keeps `user:<id>` as the actor (roles are the user's) and
    /// its detail says which credential made the request.
    #[test]
    fn a_user_vouched_for_by_a_named_operator_is_audited_with_the_credential() {
        let backend = OrgBackend::in_memory();
        let alice = Actor::NamedOperator("alice".into());
        let ok = |operator: &Actor, user: Option<&str>, method: &str, path: &str, body: Value| {
            let headers = match user {
                Some(id) => HashMap::from([(ACTOR_HEADER.to_string(), id.to_string())]),
                None => HashMap::new(),
            };
            let body = if body.is_null() {
                Vec::new()
            } else {
                body.to_string().into_bytes()
            };
            let (status, value) =
                parse(&handle(method, path, &headers, &body, &backend, None, operator).unwrap());
            assert_eq!(status, 200, "{method} {path}: {value}");
            value
        };
        let user = |email: &str| {
            let created = ok(
                &alice,
                None,
                "POST",
                "/v1/admin/users",
                json!({ "email": email }),
            );
            created["id"].as_str().unwrap().to_string()
        };
        let (ada, bob) = (user("ada@example.com"), user("bob@example.com"));
        let org = ok(
            &alice,
            None,
            "POST",
            "/v1/admin/orgs",
            json!({ "slug": "acme", "name": "Acme", "owner_user_id": ada }),
        );
        let base = format!("/v1/admin/orgs/{}", org["id"].as_str().unwrap());
        let members = format!("{base}/members");
        let add = |operator: &Actor, role: &str| {
            ok(
                operator,
                Some(&ada),
                "POST",
                &members,
                json!({ "user_id": bob, "role": role }),
            );
        };
        // The owner, through the shared key (as the console does today).
        add(&Actor::Operator, "member");
        // The owner, named by alice's credential: a role change, a removal.
        ok(
            &alice,
            Some(&ada),
            "POST",
            &format!("{members}/{bob}/role"),
            json!({ "role": "admin" }),
        );
        ok(
            &alice,
            Some(&ada),
            "DELETE",
            &format!("{members}/{bob}"),
            Value::Null,
        );

        let log = ok(
            &Actor::Operator,
            None,
            "GET",
            &format!("{base}/audit-log"),
            Value::Null,
        );
        let by_ada = format!("user:{ada}");
        let rows: Vec<(&str, &str, &Value)> = log["events"]
            .as_array()
            .unwrap()
            .iter()
            .rev()
            .map(|e| {
                (
                    e["actor"].as_str().unwrap(),
                    e["action"].as_str().unwrap(),
                    &e["detail"]["via"],
                )
            })
            .collect();
        let via_alice = json!("operator:alice");
        assert_eq!(
            rows,
            vec![
                // alice acting for herself is the actor; nothing vouched.
                ("operator:alice", "org.created", &Value::Null),
                // The shared key has no name to record.
                (by_ada.as_str(), "member.added", &Value::Null),
                (by_ada.as_str(), "member.role_changed", &via_alice),
                (by_ada.as_str(), "member.removed", &via_alice),
            ]
        );
        // The rest of the detail is untouched.
        let removed = &log["events"][0]["detail"];
        assert_eq!(removed["role"], json!("admin"));
    }
}
