//! Per-request authorization of tenant API keys (#346): which scope a
//! route needs, which namespaces it touches, and whether a key's scopes and
//! namespace allow-list cover them.
//!
//! `dispatch` calls [`authorize`] for every request that authenticated with
//! a tenant key, before the request is routed or forwarded to the ingest
//! pod. The dedicated admin and internal credentials are not tenant keys
//! and never come through here; `/v1/admin/*` and `/internal/*` stay behind
//! them (`route`), whatever a tenant key's scopes.
//!
//! Scopes by route:
//!   - `read`: search, exists, namespace stats, reading a schema, listing
//!     namespaces and branches, the tenant's own usage (`GET /v1/usage`)
//!     and billing state (`GET /v1/billing`), and namespace exports
//!     (`/v1/namespaces/{ns}/exports...`, #347: an export returns what
//!     a search could read).
//!   - `write`: index, replace, delete documents, flush.
//!   - `admin`: create and delete namespaces and branches, change a
//!     namespace's schema or shards, manage the tenant's own API keys
//!     (`/v1/api-keys`, `tenant_key_admin.rs`), and start a self-service
//!     checkout or open the billing portal (`POST /v1/billing/checkout`,
//!     `POST /v1/billing/portal`).
//!
//! Usage and billing answer for the whole tenant (every namespace's usage,
//! the plan, the invoices), so a key restricted to specific namespaces is
//! refused there whatever its scopes. Listing namespaces and the key
//! routes are [`Target::Filtered`] instead: a restricted key may use them,
//! and the handler narrows the answer (only its namespaces are listed;
//! only keys within its own grant are shown or changed). The routes that
//! delete a tenant or change its plan are under `/v1/admin/` and take the
//! admin credential, never a tenant key.
//!
//! The request's principal is also kept for the handlers that narrow their
//! answer ([`with_principal`], [`current_principal`]): `dispatch` sets it for
//! the whole of the request, on the connection's thread.
//!
//! **Keep [`requirement`] in step with `route_inner`'s dispatch table.** A
//! tenant route missing here is not left open: it classifies as
//! [`Requirement::Unknown`], which only a key with every scope and no
//! namespace restriction passes.
//! The tests read `route_inner`'s source and fail for a route it serves
//! that has no scope here, whether it is matched by a `"METHOD /path"`
//! literal, by a predicate helper or by a module it delegates to (the last
//! two are listed in the tests' `DELEGATES`).

use crate::auth::Principal;
use kosha_control::Scope;
use std::cell::RefCell;

thread_local! {
    /// The tenant key the request this thread is serving authenticated
    /// with; `None` for the admin and internal credentials.
    static CURRENT: RefCell<Option<Principal>> = const { RefCell::new(None) };
}

/// Run `f` (the rest of a request) with `principal` as the request's
/// principal, and clear it afterwards, even if `f` panics, so it can never
/// leak into the next request served on this thread.
pub fn with_principal<R>(principal: Option<Principal>, f: impl FnOnce() -> R) -> R {
    struct Reset(Option<Principal>);
    impl Drop for Reset {
        fn drop(&mut self) {
            let previous = self.0.take();
            CURRENT.with(|current| *current.borrow_mut() = previous);
        }
    }
    let previous = CURRENT.with(|current| current.replace(principal));
    let _reset = Reset(previous);
    f()
}

/// The principal [`with_principal`] set for the request this thread is
/// serving.
pub fn current_principal() -> Option<Principal> {
    CURRENT.with(|current| current.borrow().clone())
}

/// Whether the request's key may see namespace `name` (unqualified) in a
/// listing: every key not restricted to namespaces may, and the admin and
/// internal credentials (no principal) may.
pub fn may_see_namespace(name: &str) -> bool {
    CURRENT.with(|current| match current.borrow().as_ref() {
        Some(principal) => principal
            .namespaces
            .as_deref()
            .is_none_or(|allowed| allowed.iter().any(|ns| ns == name)),
        None => true,
    })
}

/// Where a route names the namespace(s) it acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// The tenant as a whole (usage, billing): a key restricted to
    /// namespaces is refused.
    Tenant,
    /// The tenant as a whole, but the handler answers only within the
    /// key's namespaces (listing namespaces, the key routes).
    Filtered,
    /// Named in the path.
    Path(String),
    /// Named by a string field of the JSON body.
    Body(&'static str),
    /// One in the path and one in a body field (creating a branch: the
    /// parent and the new branch).
    PathAndBody(String, &'static str),
    /// `ns=` query parameters (legacy `GET /search`).
    Query(Vec<String>),
}

/// What a request needs from the key that makes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Requirement {
    /// Nothing: health checks, and routes that answer the same for every
    /// tenant key (global stats refuses them all).
    Open,
    /// A route for the admin or internal credential; `route` refuses a
    /// tenant key there regardless of scopes.
    Service,
    Scoped {
        scope: Scope,
        target: Target,
    },
    /// Not a route this table knows.
    Unknown,
}

fn scoped(scope: Scope, target: Target) -> Requirement {
    Requirement::Scoped { scope, target }
}

/// Classify a request by its method and path (query string included).
pub fn requirement(method: &str, path: &str) -> Requirement {
    use Scope::{Admin, Read, Write};
    let (bare, query) = path.split_once('?').unwrap_or((path, ""));
    if bare.starts_with("/v1/admin/") || bare.starts_with("/internal/") {
        return Requirement::Service;
    }
    // The console (#345) authenticates with a session cookie, answered
    // before API keys are looked at; no tenant key is ever accepted there.
    if bare == "/v1/console" || bare.starts_with("/v1/console/") {
        return Requirement::Service;
    }
    // Namespace exports (#347): starting, reading, downloading and
    // deleting one hands back the namespace's documents, so `read`.
    if let Some(route) = crate::exports::parse_route(method, path) {
        use crate::exports::ExportRoute;
        let ns = match route {
            ExportRoute::Create(ns) | ExportRoute::List(ns) => ns,
            ExportRoute::Get(ns, _) | ExportRoute::Download(ns, _) | ExportRoute::Delete(ns, _) => {
                ns
            }
        };
        return scoped(Read, Target::Path(ns));
    }
    if let Some(rest) = bare.strip_prefix("/v1/namespaces/") {
        let Some((ns, action)) = rest.rsplit_once('/') else {
            // The bare `/v1/namespaces/{ns}`.
            return match method {
                "DELETE" => scoped(Admin, Target::Path(crate::path_decode(rest))),
                _ => Requirement::Unknown,
            };
        };
        let ns = crate::path_decode(ns);
        let scope = match (method, action) {
            ("POST", "search" | "read" | "exists") => Read,
            ("GET", "schema" | "branches" | "stats") => Read,
            ("POST", "documents" | "replace" | "flush" | "delete") => Write,
            ("POST", "schema" | "shards") => Admin,
            ("DELETE", "branch") => Admin,
            ("POST", "branch") => return scoped(Admin, Target::PathAndBody(ns, "name")),
            _ => return Requirement::Unknown,
        };
        return scoped(scope, Target::Path(ns));
    }
    match (method, bare) {
        (_, "/healthz" | "/v1/healthz" | "/stats" | "/v1/stats") => Requirement::Open,
        ("GET", "/v1/namespaces") => scoped(Read, Target::Filtered),
        // The tenant's own keys (#346 self-service).
        ("GET" | "POST", "/v1/api-keys")
        | ("POST", "/v1/api-keys/rotate" | "/v1/api-keys/revoke") => {
            scoped(Admin, Target::Filtered)
        }
        // A tenant reading its own usage (#348) and billing state (#349).
        ("GET", "/v1/usage" | "/v1/billing") => scoped(Read, Target::Tenant),
        // Self-service billing (#349): putting a payment method on file
        // moves the tenant to a paid plan, so it takes the admin scope.
        ("POST", "/v1/billing/checkout" | "/v1/billing/portal") => scoped(Admin, Target::Tenant),
        // Stripe's deliveries carry a signature, not a key; `dispatch`
        // answers them before authentication.
        ("POST", "/v1/billing/webhooks/stripe") => Requirement::Open,
        ("POST", "/v1/namespaces") => scoped(Admin, Target::Body("namespace")),
        // Legacy unversioned routes.
        ("POST", "/search" | "/exists") => scoped(Read, Target::Body("namespace")),
        ("POST", "/index" | "/replace" | "/flush" | "/delete") => {
            scoped(Write, Target::Body("namespace"))
        }
        ("GET", "/search") => scoped(
            Read,
            Target::Query(
                query
                    .split('&')
                    .filter_map(|pair| pair.strip_prefix("ns="))
                    .map(crate::url_decode)
                    .collect(),
            ),
        ),
        _ => Requirement::Unknown,
    }
}

/// The namespaces a request touches, or `None` when they cannot be told
/// (tenant-wide route, unreadable body, missing field).
fn target_namespaces(target: &Target, body: &[u8]) -> Option<Vec<String>> {
    // Read the body the way the handlers do (`serde_json::Value`), so the
    // namespace checked here is the namespace acted on.
    let body_field = |field: &str| -> Option<String> {
        let value: serde_json::Value = serde_json::from_slice(body).ok()?;
        value.get(field)?.as_str().map(str::to_string)
    };
    match target {
        Target::Tenant | Target::Filtered => None,
        Target::Path(ns) => Some(vec![ns.clone()]),
        Target::Body(field) => Some(vec![body_field(field)?]),
        Target::PathAndBody(ns, field) => Some(vec![ns.clone(), body_field(field)?]),
        Target::Query(names) if names.is_empty() => None,
        Target::Query(names) => Some(names.clone()),
    }
}

/// Whether `principal`'s key may make this request. `Err` is the message
/// for a 403.
pub fn authorize(
    principal: &Principal,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<(), String> {
    // Env keys, `KOSHA_AUTH=disabled` and every key from before #346.
    if principal.unrestricted() {
        return Ok(());
    }
    let (scope, target) = match requirement(method, path) {
        Requirement::Open | Requirement::Service => return Ok(()),
        Requirement::Unknown => {
            return Err(
                "this API key is restricted (scopes or namespaces) and cannot use this route"
                    .into(),
            )
        }
        Requirement::Scoped { scope, target } => (scope, target),
    };
    if !principal.scopes.contains(scope) {
        return Err(format!(
            "this API key lacks the '{}' scope (it has: {})",
            scope.as_str(),
            principal.scopes.names().join(", ")
        ));
    }
    let Some(allowed) = principal.namespaces.as_deref() else {
        return Ok(());
    };
    if target == Target::Filtered {
        return Ok(());
    }
    let Some(wanted) = target_namespaces(&target, body) else {
        return Err(match target {
            Target::Tenant => {
                "this API key is restricted to specific namespaces and cannot use a route \
                 that covers the whole tenant"
            }
            _ => "this API key is restricted to specific namespaces and the request names none",
        }
        .into());
    };
    match wanted.iter().find(|ns| !allowed.contains(ns)) {
        Some(denied) => Err(format!(
            "this API key is not allowed to access namespace '{denied}'"
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kosha_control::Scopes;

    fn key(scopes: &[Scope], namespaces: Option<&[&str]>) -> Principal {
        Principal {
            tenant: "acme".into(),
            scopes: Scopes::of(scopes),
            namespaces: namespaces.map(|ns| ns.iter().map(|n| n.to_string()).collect()),
            expires_at: None,
            key_id: None,
        }
    }

    /// Every tenant route `route_inner` serves, with the scope it needs.
    /// Path-addressed routes use namespace `docs`; body-addressed ones name
    /// it in `BODY`.
    const BODY: &[u8] = br#"{"namespace":"docs","name":"docs-branch"}"#;
    const ROUTES: &[(&str, &str, Scope)] = &[
        ("POST", "/v1/namespaces/docs/search", Scope::Read),
        ("POST", "/v1/namespaces/docs/read", Scope::Read),
        ("POST", "/v1/namespaces/docs/exists", Scope::Read),
        ("GET", "/v1/namespaces/docs/schema", Scope::Read),
        ("GET", "/v1/namespaces/docs/branches", Scope::Read),
        ("GET", "/v1/namespaces/docs/stats", Scope::Read),
        ("GET", "/v1/namespaces", Scope::Read),
        ("GET", "/v1/usage", Scope::Read),
        ("GET", "/v1/usage?from=1&to=2", Scope::Read),
        ("GET", "/v1/billing", Scope::Read),
        ("POST", "/search", Scope::Read),
        ("POST", "/exists", Scope::Read),
        ("GET", "/search?ns=docs&q=x", Scope::Read),
        ("POST", "/v1/namespaces/docs/documents", Scope::Write),
        ("POST", "/v1/namespaces/docs/replace", Scope::Write),
        ("POST", "/v1/namespaces/docs/flush", Scope::Write),
        ("POST", "/v1/namespaces/docs/delete", Scope::Write),
        ("POST", "/index", Scope::Write),
        ("POST", "/replace", Scope::Write),
        ("POST", "/flush", Scope::Write),
        ("POST", "/delete", Scope::Write),
        ("POST", "/v1/namespaces", Scope::Admin),
        ("DELETE", "/v1/namespaces/docs", Scope::Admin),
        ("POST", "/v1/namespaces/docs/schema", Scope::Admin),
        ("POST", "/v1/namespaces/docs/shards", Scope::Admin),
        ("POST", "/v1/namespaces/docs/branch", Scope::Admin),
        ("DELETE", "/v1/namespaces/docs/branch", Scope::Admin),
        ("GET", "/v1/api-keys", Scope::Admin),
        ("POST", "/v1/api-keys", Scope::Admin),
        ("POST", "/v1/api-keys/rotate", Scope::Admin),
        ("POST", "/v1/api-keys/revoke", Scope::Admin),
        ("POST", "/v1/billing/checkout", Scope::Admin),
        ("POST", "/v1/billing/portal", Scope::Admin),
        ("POST", "/v1/namespaces/docs/exports", Scope::Read),
        ("GET", "/v1/namespaces/docs/exports", Scope::Read),
        ("GET", "/v1/namespaces/docs/exports/exp-1", Scope::Read),
        (
            "GET",
            "/v1/namespaces/docs/exports/exp-1/download",
            Scope::Read,
        ),
        ("DELETE", "/v1/namespaces/docs/exports/exp-1", Scope::Read),
    ];

    /// The routes of `ROUTES` that answer for the whole tenant and refuse a
    /// namespace-restricted key.
    const TENANT_WIDE: &[(&str, &str)] = &[
        ("GET", "/v1/usage"),
        ("GET", "/v1/billing"),
        ("POST", "/v1/billing/checkout"),
        ("POST", "/v1/billing/portal"),
    ];

    /// The routes of `ROUTES` that answer for the whole tenant but narrow
    /// the answer to a restricted key's namespaces.
    const FILTERED: &[(&str, &str)] = &[
        ("GET", "/v1/namespaces"),
        ("GET", "/v1/api-keys"),
        ("POST", "/v1/api-keys"),
        ("POST", "/v1/api-keys/rotate"),
        ("POST", "/v1/api-keys/revoke"),
    ];

    /// The body of the HTTP route dispatcher, after its signature.
    fn route_inner_body() -> &'static str {
        let source = include_str!("http_routes.rs");
        let start = source
            .find("\npub(super) fn route_inner(")
            .expect("route_inner in http_routes.rs");
        let body = &source[start..];
        let body = &body[..body.find("\n}\n").expect("the end of route_inner")];
        let open = ") -> String {\n";
        &body[body.find(open).expect("route_inner's signature") + open.len()..]
    }

    const METHODS: [&str; 5] = ["GET", "POST", "PUT", "PATCH", "DELETE"];

    /// The `"METHOD /path` literals in `text`, as `(method, sample path)`.
    /// `extract_namespace(request_line, "POST /v1/namespaces/", "/search")`
    /// becomes `POST /v1/namespaces/docs/search`.
    fn route_literals(text: &str) -> Vec<(String, String)> {
        let mut found = Vec::new();
        // Every other piece of a split on '"' is the inside of a string
        // literal (the table has no escaped quotes).
        let literals: Vec<&str> = text.split('"').skip(1).step_by(2).collect();
        for (i, literal) in literals.iter().enumerate() {
            let Some((method, path)) = literal.split_once(' ') else {
                continue;
            };
            if !METHODS.contains(&method) || !path.starts_with('/') {
                continue;
            }
            let path = path.trim_end_matches([' ', '?']);
            let path = match path {
                "/v1/namespaces/" => {
                    format!("{path}docs{}", literals.get(i + 1).copied().unwrap_or(""))
                }
                _ => path.to_string(),
            };
            found.push((method.to_string(), path));
        }
        found
    }

    /// A dispatch condition of `route_inner` that carries no
    /// `"METHOD /path"` literal: a predicate helper, or a module it hands
    /// the request line to.
    #[derive(Clone, Copy)]
    struct Delegate {
        /// The call, as written in `route_inner`'s condition.
        call: &'static str,
        /// Every route it serves, as `(method, sample path)`.
        routes: &'static [(&'static str, &'static str)],
        /// When the call dispatches in another file: that file's source and
        /// the opening of the function called.
        source: Option<(&'static str, &'static str)>,
    }

    /// Every such condition. A new helper or delegated module in
    /// `route_inner` gets a line here, with the routes it serves; tenant
    /// routes among them then need a scope in [`requirement`] and a line in
    /// `ROUTES`, like the routes matched by a literal.
    const DELEGATES: &[Delegate] = &[
        Delegate {
            call: "is_create_namespace_route(request_line)",
            routes: &[("POST", "/v1/namespaces")],
            source: None,
        },
        Delegate {
            call: "bare_namespace(request_line, \"DELETE\")",
            routes: &[("DELETE", "/v1/namespaces/docs")],
            source: None,
        },
        Delegate {
            call: "is_list_namespaces_route(request_line)",
            routes: &[("GET", "/v1/namespaces")],
            source: None,
        },
        Delegate {
            call: "billing::webhook_response(state, request_line, headers, body)",
            routes: &[("POST", "/v1/billing/webhooks/stripe")],
            source: None,
        },
        Delegate {
            call: "billing::route(request_line, body, tenant, state)",
            routes: &crate::billing::ROUTES,
            source: Some((include_str!("billing.rs"), "\npub(crate) fn route(")),
        },
        Delegate {
            call: "tenant_key_admin::handle(request_line, body)",
            routes: &[
                ("GET", "/v1/api-keys"),
                ("POST", "/v1/api-keys"),
                ("POST", "/v1/api-keys/rotate"),
                ("POST", "/v1/api-keys/revoke"),
            ],
            source: Some((
                include_str!("tenant_key_admin.rs"),
                "\npub(crate) fn handle(",
            )),
        },
        Delegate {
            call: "exports::route(request_line, body, tenant, state)",
            routes: &[
                ("POST", "/v1/namespaces/docs/exports"),
                ("GET", "/v1/namespaces/docs/exports"),
                ("GET", "/v1/namespaces/docs/exports/exp-1"),
                ("DELETE", "/v1/namespaces/docs/exports/exp-1"),
            ],
            source: Some((include_str!("exports.rs"), "\npub(crate) fn route(")),
        },
        Delegate {
            call: "namespace_deletion::route_admin(request_line, body, state)",
            routes: &[
                ("DELETE", "/v1/admin/tenants/acme"),
                ("POST", "/v1/admin/deletions/resume"),
                ("GET", "/v1/admin/deletions"),
                ("GET", "/v1/admin/deletions/del_1"),
            ],
            source: Some((
                include_str!("namespace_deletion.rs"),
                "\npub(crate) fn route_admin(",
            )),
        },
    ];

    /// What `route_inner`'s body (or a stand-in for it) dispatches on that
    /// these tests cannot account for: a top-level statement that is not an
    /// `if`, or an `if` whose condition has neither a `"METHOD /path"`
    /// literal nor a call listed in `delegates`. Empty when every dispatch
    /// is accounted for.
    fn unrecognised_dispatches(body: &str, delegates: &[Delegate]) -> Vec<String> {
        let mut unknown = Vec::new();
        // Statements of the function itself are indented by four spaces.
        for line in body.lines() {
            let Some(statement) = line.strip_prefix("    ") else {
                continue;
            };
            let fine = statement.starts_with(' ')
                || statement.starts_with("if ")
                || statement.starts_with("//")
                || ["{", "}", "api_error(ErrorCode::NotFound, \"not found\")"].contains(&statement);
            if !fine {
                unknown.push(statement.to_string());
            }
        }
        for chunk in format!("\n{body}").split("\n    if ").skip(1) {
            // A condition ends at the `{` that closes its line, or stands
            // alone on the line after a multi-line condition.
            let condition = chunk.split("{\n").next().unwrap_or(chunk).trim();
            if route_literals(condition).is_empty()
                && !delegates.iter().any(|d| condition.contains(d.call))
            {
                unknown.push(condition.to_string());
            }
        }
        unknown
    }

    /// The paths a delegated module's dispatch function matches on: each
    /// `"/v1/...` or `"METHOD /v1/...` string literal in the body of the
    /// function that opens with `function`.
    fn delegate_paths<'a>(source: &'a str, function: &str) -> Vec<&'a str> {
        let start = source
            .find(function)
            .unwrap_or_else(|| panic!("{function:?} not found; update DELEGATES"));
        let code = &source[start..];
        let code = &code[..code.find("\n}\n").expect("the end of the function")];
        let mut paths = Vec::new();
        for (at, _) in code.match_indices("/v1/") {
            let before = &code[..at];
            let quoted = before.ends_with('"')
                || METHODS
                    .iter()
                    .any(|method| before.ends_with(&format!("\"{method} ")));
            if quoted {
                let path = &code[at..];
                paths.push(&path[..path.find('"').unwrap_or(path.len())]);
            }
        }
        paths
    }

    /// A route added to `route_inner`, in any of the three ways it
    /// dispatches (a `"METHOD /path"` literal, a predicate helper, a
    /// delegated module), without a line in [`requirement`] would be
    /// refused for every restricted key. This fails instead, naming the
    /// route or the dispatch it cannot account for.
    #[test]
    fn every_route_route_inner_matches_is_classified() {
        let body = route_inner_body();
        let mut routes = route_literals(body);
        // The scan found the table, not a fragment of it.
        assert!(routes.len() >= 30, "{routes:?}");
        for expected in [
            ("POST", "/v1/namespaces/docs/search"),
            ("DELETE", "/v1/namespaces/docs/branch"),
            ("GET", "/v1/usage"),
            ("POST", "/index"),
            ("GET", "/stats"),
        ] {
            assert!(
                routes
                    .iter()
                    .any(|(m, p)| (m.as_str(), p.as_str()) == expected),
                "{expected:?} not found in route_inner"
            );
        }
        // Nothing is dispatched in a way the scan does not see.
        let unknown = unrecognised_dispatches(body, DELEGATES);
        assert!(
            unknown.is_empty(),
            "route_inner dispatches on something these tests cannot see; add it to \
             DELEGATES with the routes it serves: {unknown:#?}"
        );
        for delegate in DELEGATES {
            assert!(
                body.contains(delegate.call),
                "'{}' is no longer in route_inner; update DELEGATES",
                delegate.call
            );
            routes.extend(
                delegate
                    .routes
                    .iter()
                    .map(|(method, path)| (method.to_string(), path.to_string())),
            );
            // A tenant path the module matches on is among its routes.
            let matched = delegate
                .source
                .map(|(source, function)| delegate_paths(source, function))
                .unwrap_or_default();
            for path in matched {
                if requirement("GET", path) == Requirement::Service {
                    continue;
                }
                assert!(
                    delegate.routes.iter().any(|(method, route)| {
                        route.starts_with(path)
                            && requirement(method, route) != Requirement::Service
                    }),
                    "'{}' matches on the tenant path {path}, which is not among its \
                     routes in DELEGATES",
                    delegate.call
                );
            }
        }
        for (method, path) in &routes {
            let needs = requirement(method, path);
            assert_ne!(
                needs,
                Requirement::Unknown,
                "{method} {path} is served but has no scope in authz::requirement"
            );
            // A tenant route is in the table the scope tests walk.
            if let Requirement::Scoped { scope, .. } = needs {
                assert!(
                    ROUTES.iter().any(|&(m, p, s)| m == method
                        && p.split('?').next() == Some(path.as_str())
                        && s == scope),
                    "{method} {path} ('{}') is missing from ROUTES",
                    scope.as_str()
                );
            }
        }
    }

    /// The guard above sees all three dispatch styles: a route added
    /// through a helper or a delegated module, with no literal in
    /// `route_inner`, is reported until `DELEGATES` accounts for it.
    #[test]
    fn a_route_dispatched_without_a_literal_is_reported() {
        let known = r#"    if is_list_namespaces_route(request_line) {
        return a();
    }

    // A comment.
    if request_line.starts_with("GET /v1/usage ")
        || request_line.starts_with("GET /v1/usage?")
    {
        return b();
    }

    api_error(ErrorCode::NotFound, "not found")"#;
        assert_eq!(
            unrecognised_dispatches(known, DELEGATES),
            Vec::<String>::new()
        );
        for (added, reported) in [
            (
                r#"    if let Some(ns) = bare_namespace(request_line, "PATCH") {
        return c();
    }"#,
                r#"let Some(ns) = bare_namespace(request_line, "PATCH")"#,
            ),
            (
                r#"    if let Some(r) = snapshots::route(request_line, body, state) {
        return r;
    }"#,
                "let Some(r) = snapshots::route(request_line, body, state)",
            ),
            (
                r#"    match snapshots::route(request_line) {
        Some(r) => return r,
        None => {}
    }"#,
                "match snapshots::route(request_line) {",
            ),
        ] {
            assert_eq!(
                unrecognised_dispatches(&format!("{added}\n{known}"), DELEGATES),
                vec![reported.to_string()],
                "{added}"
            );
        }
        // The real table minus one entry: the helper it covered is reported.
        let without_delete: Vec<Delegate> = DELEGATES
            .iter()
            .copied()
            .filter(|d| !d.call.starts_with("bare_namespace"))
            .collect();
        assert_eq!(
            unrecognised_dispatches(route_inner_body(), &without_delete),
            vec![r#"let Some(ns) = bare_namespace(request_line, "DELETE")"#.to_string()]
        );
        // A delegated module's paths are read from its dispatch function,
        // not from comments or the rest of the file.
        assert_eq!(
            delegate_paths(
                "fn gate() { \"/v1/elsewhere\" }\nfn route() {\n    (\"GET\", \"/v1/billing\");\n    \
                 // `GET /v1/usage`\n    extract(r, \"GET /v1/namespaces/\", \"/x\")\n}\n\
                 fn after() { \"/v1/later\" }\n",
                "\nfn route("
            ),
            vec!["/v1/billing", "/v1/namespaces/"]
        );
    }

    #[test]
    fn each_route_needs_exactly_its_scope() {
        for &(method, path, needed) in ROUTES {
            for have in [Scope::Read, Scope::Write, Scope::Admin] {
                let result = authorize(&key(&[have], None), method, path, BODY);
                assert_eq!(
                    result.is_ok(),
                    have == needed,
                    "{method} {path} with only '{}': {result:?}",
                    have.as_str()
                );
            }
            // Holding the other two scopes is not enough either.
            let others: Vec<Scope> = [Scope::Read, Scope::Write, Scope::Admin]
                .into_iter()
                .filter(|s| *s != needed)
                .collect();
            let denied = authorize(&key(&others, None), method, path, BODY).unwrap_err();
            assert!(denied.contains(needed.as_str()), "{denied}");
        }
    }

    #[test]
    fn a_read_only_key_cannot_write() {
        let ro = key(&[Scope::Read], None);
        assert!(authorize(&ro, "POST", "/v1/namespaces/docs/search", b"{}").is_ok());
        for (method, path) in [
            ("POST", "/v1/namespaces/docs/documents"),
            ("POST", "/v1/namespaces/docs/delete"),
            ("POST", "/v1/namespaces/docs/flush"),
            ("POST", "/index"),
            ("POST", "/replace"),
            ("DELETE", "/v1/namespaces/docs"),
            ("POST", "/v1/namespaces"),
            ("POST", "/v1/namespaces/docs/schema"),
        ] {
            let denied = authorize(&ro, method, path, BODY).unwrap_err();
            assert!(denied.contains("scope"), "{method} {path}: {denied}");
        }
    }

    #[test]
    fn unrestricted_keys_are_never_rejected() {
        let full = Principal::full("acme");
        for &(method, path, _) in ROUTES {
            assert!(authorize(&full, method, path, b"not json").is_ok());
        }
        // Unknown and service routes fall through to `route` unchanged.
        assert!(authorize(&full, "POST", "/no-such-route", b"").is_ok());
        assert!(authorize(&full, "POST", "/v1/admin/api-keys", b"").is_ok());
    }

    #[test]
    fn namespace_allow_list_applies_to_every_way_a_route_names_a_namespace() {
        let all = [Scope::Read, Scope::Write, Scope::Admin];
        let docs_only = key(&all, Some(&["docs", "docs-branch"]));
        let other_only = key(&all, Some(&["other"]));
        for &(method, path, _) in ROUTES {
            let bare = path.split('?').next().unwrap();
            if TENANT_WIDE.contains(&(method, bare)) || FILTERED.contains(&(method, bare)) {
                continue;
            }
            assert!(
                authorize(&docs_only, method, path, BODY).is_ok(),
                "{method} {path}"
            );
            let denied = authorize(&other_only, method, path, BODY).unwrap_err();
            assert!(
                denied.contains("not allowed to access namespace 'docs'"),
                "{method} {path}: {denied}"
            );
        }
    }

    #[test]
    fn namespace_restricted_keys_fail_closed() {
        let all = [Scope::Read, Scope::Write, Scope::Admin];
        let docs = key(&all, Some(&["docs"]));
        // Usage and billing span the tenant: the answer would name
        // namespaces (or money) the key was not given.
        for &(method, path) in TENANT_WIDE {
            let denied = authorize(&docs, method, path, b"").unwrap_err();
            assert!(denied.contains("whole tenant"), "{method} {path}: {denied}");
            // The same key without the namespace restriction may.
            assert!(authorize(&key(&all, None), method, path, b"").is_ok());
        }
        // Listing namespaces and the key routes narrow their answer to the
        // key's namespaces instead, so a restricted key may use them (with
        // the right scope).
        for &(method, path) in FILTERED {
            assert!(
                authorize(&docs, method, path, b"").is_ok(),
                "{method} {path}"
            );
            let read_only_docs = key(&[Scope::Read], Some(&["docs"]));
            let needed = match requirement(method, path) {
                Requirement::Scoped { scope, .. } => scope,
                other => panic!("{method} {path}: {other:?}"),
            };
            assert_eq!(
                authorize(&read_only_docs, method, path, b"").is_ok(),
                needed == Scope::Read,
                "{method} {path}"
            );
        }
        assert!(authorize(&docs, "GET", "/v1/usage?from=1&to=2", b"").is_err());
        // A branch must be allowed by name as well as its parent.
        let branch = "/v1/namespaces/docs/branch";
        assert!(authorize(&docs, "POST", branch, br#"{"name":"scratch"}"#)
            .unwrap_err()
            .contains("'scratch'"));
        assert!(authorize(&docs, "POST", branch, br#"{"name":"docs"}"#).is_ok());
        // No readable namespace in the body or query: refused, not waved on.
        assert!(authorize(&docs, "POST", "/index", b"not json").is_err());
        assert!(authorize(&docs, "POST", "/index", br#"{"namespace":7}"#).is_err());
        assert!(authorize(&docs, "POST", "/v1/namespaces", b"{}").is_err());
        assert!(authorize(&docs, "POST", branch, b"{}").is_err());
        assert!(authorize(&docs, "GET", "/search?q=x", b"").is_err());
        // Every `ns=` must be allowed; names are compared URL-decoded.
        assert!(authorize(&docs, "GET", "/search?ns=docs&ns=other", b"").is_err());
        assert!(authorize(&docs, "GET", "/search?ns=%64ocs&q=x", b"").is_ok());
        assert!(authorize(&docs, "POST", "/v1/namespaces/%64ocs/search", b"").is_ok());
        assert!(authorize(&docs, "POST", "/v1/namespaces/Docs/search", b"").is_err());
        // An empty allow-list allows nothing.
        let none = key(&all, Some(&[]));
        assert!(authorize(&none, "POST", "/v1/namespaces/docs/search", b"").is_err());
    }

    #[test]
    fn restricted_keys_cannot_use_unclassified_routes() {
        let ro = key(&[Scope::Read], None);
        let scoped_ns = key(&[Scope::Read, Scope::Write, Scope::Admin], Some(&["docs"]));
        for principal in [&ro, &scoped_ns] {
            for (method, path) in [
                ("POST", "/v1/namespaces/docs/some-future-action"),
                ("PUT", "/v1/namespaces/docs"),
                ("GET", "/v1/namespaces/docs/documents"),
                ("POST", "/no-such-route"),
            ] {
                assert!(
                    authorize(principal, method, path, b"{}").is_err(),
                    "{method} {path}"
                );
            }
            // Health and service routes are decided elsewhere.
            assert!(authorize(principal, "GET", "/v1/healthz", b"").is_ok());
            assert!(authorize(principal, "GET", "/v1/stats", b"").is_ok());
            assert!(authorize(principal, "POST", "/v1/admin/api-keys", b"").is_ok());
            assert!(authorize(principal, "GET", "/internal/segment/a/b", b"").is_ok());
        }
    }

    #[test]
    fn the_request_principal_narrows_listings_and_never_outlives_its_request() {
        assert_eq!(current_principal(), None);
        assert!(may_see_namespace("anything"));
        let docs = key(&[Scope::Read], Some(&["docs"]));
        with_principal(Some(docs.clone()), || {
            assert_eq!(current_principal(), Some(docs.clone()));
            assert!(may_see_namespace("docs"));
            assert!(!may_see_namespace("other"));
            // Nested (a request handled in a request) restores the outer one.
            with_principal(None, || assert!(may_see_namespace("other")));
            assert!(!may_see_namespace("other"));
        });
        assert_eq!(current_principal(), None);
        // Cleared even when the request panics.
        let _ = std::panic::catch_unwind(|| {
            with_principal(Some(docs.clone()), || panic!("handler failed"))
        });
        assert_eq!(current_principal(), None);
        // Unrestricted keys see everything.
        with_principal(Some(key(&[Scope::Read], None)), || {
            assert!(may_see_namespace("other"))
        });
    }

    #[test]
    fn requirement_reads_method_path_and_query() {
        assert_eq!(
            requirement("POST", "/v1/namespaces/my%20ns/search"),
            scoped(Scope::Read, Target::Path("my ns".into()))
        );
        assert_eq!(
            requirement("DELETE", "/v1/namespaces/docs?force=1"),
            scoped(Scope::Admin, Target::Path("docs".into()))
        );
        assert_eq!(
            requirement("GET", "/search?q=x&ns=a&ns=b"),
            scoped(Scope::Read, Target::Query(vec!["a".into(), "b".into()]))
        );
        assert_eq!(
            requirement("GET", "/v1/admin/compaction-status"),
            Requirement::Service
        );
        assert_eq!(requirement("GET", "/healthz"), Requirement::Open);
        assert_eq!(requirement("PATCH", "/v1/namespaces"), Requirement::Unknown);
    }
}
