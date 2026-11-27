//! The `/v1` route table (#350, ADR-350 §d).
//!
//! One static row per public or admin operation: its method, path template,
//! the `operationId` it has in the checked-in OpenAPI spec
//! (`api/openapi/v1/*.openapi.json`), and how the server treats it (read,
//! search, write, or a read that must run on the writer pod). The table is
//! the single source for:
//!
//! - whether a `/v1` path exists at all (`route` answers 404 for one that
//!   is not here, and 405 with `Allow` for a known path with the wrong
//!   method);
//! - forwarding, write admission and search admission for `/v1` paths, so a new route
//!   cannot be forwarded or limited differently from what it declares;
//! - the spec: `public_routes_match_spec` asserts the table and the spec
//!   describe the same operations, and that every operation reaches a
//!   handler.
//!
//! Legacy unversioned routes, `/internal/*`, the probes, `/metrics` and the
//! dashboard are deliberately not here: they are not part of the public
//! contract.

/// How the server treats a request on a route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
    /// Read-only; every pod answers it itself.
    Read,
    /// A search: tenant qps and concurrency limits apply.
    Search,
    /// Mutates state; a query pod forwards it to the ingest pod.
    Write,
    /// Read-only, but the state it reads lives on the writer pod
    /// (e.g., unflushed documents or compaction jobs), so a query pod forwards it.
    WriterRead,
}

/// Which spec documents the route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Surface {
    /// `api/openapi/v1/kosha.openapi.json`: tenant API keys.
    Public,
    /// `api/openapi/v1/kosha-admin.openapi.json`: the operator credential.
    Admin,
}

/// One `/v1` operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Route {
    pub method: &'static str,
    /// `/v1/...` with `{name}` for one path segment.
    pub template: &'static str,
    /// The spec's `operationId` (`<Service>_<Rpc>`).
    pub operation_id: &'static str,
    pub class: Class,
    pub surface: Surface,
}

const fn public(
    method: &'static str,
    template: &'static str,
    operation_id: &'static str,
    class: Class,
) -> Route {
    Route {
        method,
        template,
        operation_id,
        class,
        surface: Surface::Public,
    }
}

const fn admin(
    method: &'static str,
    template: &'static str,
    operation_id: &'static str,
    class: Class,
) -> Route {
    Route {
        method,
        template,
        operation_id,
        class,
        surface: Surface::Admin,
    }
}

use Class::{Read, Search, Write, WriterRead};

/// Every `/v1` operation the server serves. Order does not matter: no two
/// templates match the same method and path.
pub(crate) static ROUTES: &[Route] = &[
    // ── Public: proto/kosha/v1/kosha.proto `KoshaService` ──────────────────
    public("GET", "/v1/healthz", "KoshaService_HealthCheck", Read),
    public(
        "POST",
        "/v1/namespaces",
        "KoshaService_CreateNamespace",
        Write,
    ),
    public("GET", "/v1/namespaces", "KoshaService_ListNamespaces", Read),
    public(
        "DELETE",
        "/v1/namespaces/{namespace}",
        "KoshaService_DeleteNamespace",
        Write,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/documents",
        "KoshaService_IndexDocuments",
        Write,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/replace",
        "KoshaService_ReplaceDocuments",
        Write,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/search",
        "KoshaService_Search",
        Search,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/read",
        "KoshaService_ReadDocument",
        Search,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/exists",
        "KoshaService_Exists",
        WriterRead,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/flush",
        "KoshaService_Flush",
        Write,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/delete",
        "KoshaService_DeleteByQuery",
        Write,
    ),
    public(
        "GET",
        "/v1/namespaces/{namespace}/stats",
        "KoshaService_GetStats",
        Read,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/schema",
        "KoshaService_PutNamespaceSchema",
        Write,
    ),
    public(
        "GET",
        "/v1/namespaces/{namespace}/schema",
        "KoshaService_GetNamespaceSchema",
        Read,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/shards",
        "KoshaService_PutShards",
        Write,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/branch",
        "KoshaService_BranchNamespace",
        Write,
    ),
    public(
        "GET",
        "/v1/namespaces/{namespace}/branches",
        "KoshaService_ListBranches",
        Read,
    ),
    public(
        "DELETE",
        "/v1/namespaces/{namespace}/branch",
        "KoshaService_DeleteBranch",
        Write,
    ),
    public("GET", "/v1/usage", "KoshaService_GetUsage", Read),
    public("GET", "/v1/billing", "KoshaService_GetBilling", Read),
    public(
        "GET",
        "/v1/api-keys",
        "KoshaService_ListTenantApiKeys",
        Read,
    ),
    public(
        "POST",
        "/v1/api-keys",
        "KoshaService_CreateTenantApiKey",
        Read,
    ),
    public(
        "POST",
        "/v1/api-keys/rotate",
        "KoshaService_RotateTenantApiKey",
        Read,
    ),
    public(
        "POST",
        "/v1/api-keys/revoke",
        "KoshaService_RevokeTenantApiKey",
        Read,
    ),
    public(
        "POST",
        "/v1/billing/checkout",
        "KoshaService_CreateCheckoutSession",
        Read,
    ),
    public(
        "POST",
        "/v1/billing/portal",
        "KoshaService_CreateBillingPortalSession",
        Read,
    ),
    public(
        "POST",
        "/v1/billing/webhooks/stripe",
        "KoshaService_HandleStripeWebhook",
        Read,
    ),
    public(
        "POST",
        "/v1/namespaces/{namespace}/exports",
        "KoshaService_CreateExport",
        Write,
    ),
    public(
        "GET",
        "/v1/namespaces/{namespace}/exports",
        "KoshaService_ListExports",
        Read,
    ),
    public(
        "GET",
        "/v1/namespaces/{namespace}/exports/{id}",
        "KoshaService_GetExport",
        Read,
    ),
    public(
        "DELETE",
        "/v1/namespaces/{namespace}/exports/{id}",
        "KoshaService_DeleteExport",
        Write,
    ),
    public(
        "GET",
        "/v1/namespaces/{namespace}/exports/{id}/download",
        "KoshaService_DownloadExport",
        Read,
    ),
    // ── Admin: proto/kosha/v1/admin.proto `AdminService` ───────────────────
    admin("GET", "/v1/stats", "AdminService_GetServerStats", Read),
    admin(
        "GET",
        "/v1/admin/usage",
        "AdminService_GetTenantUsage",
        Read,
    ),
    admin(
        "GET",
        "/v1/admin/usage/reconcile",
        "AdminService_ReconcileUsage",
        Read,
    ),
    admin(
        "GET",
        "/v1/admin/warmup-status",
        "AdminService_GetWarmupStatus",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/rebuild-filter-blooms",
        "AdminService_RebuildFilterBlooms",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/backfill-offset-tables",
        "AdminService_BackfillOffsetTables",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/compact-namespace",
        "AdminService_CompactNamespace",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/compaction-status",
        "AdminService_GetCompactionStatus",
        WriterRead,
    ),
    admin(
        "GET",
        "/v1/admin/compaction-jobs/{job_id}",
        "AdminService_GetCompactionJob",
        WriterRead,
    ),
    admin(
        "POST",
        "/v1/admin/import-namespace",
        "AdminService_ImportNamespace",
        Write,
    ),
    admin("POST", "/v1/admin/gc", "AdminService_CollectGarbage", Write),
    admin(
        "POST",
        "/v1/admin/api-keys",
        "AdminService_CreateApiKey",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/api-keys",
        "AdminService_ListApiKeys",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/api-keys/revoke",
        "AdminService_RevokeApiKey",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/api-keys/rotate",
        "AdminService_RotateApiKey",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/api-keys/scrub-plaintext",
        "AdminService_ScrubApiKeyPlaintext",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/api-keys/plaintext-count",
        "AdminService_CountApiKeyPlaintext",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/operators",
        "AdminService_CreateOperatorCredential",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/operators",
        "AdminService_ListOperatorCredentials",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/operators/revoke",
        "AdminService_RevokeOperatorCredential",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/operators/rotate",
        "AdminService_RotateOperatorCredential",
        Write,
    ),
    admin(
        "DELETE",
        "/v1/admin/tenants/{tenant}",
        "AdminService_DeleteTenant",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/deletions",
        "AdminService_ListDeletions",
        Read,
    ),
    admin(
        "GET",
        "/v1/admin/deletions/{id}",
        "AdminService_GetDeletion",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/deletions/resume",
        "AdminService_ResumeDeletions",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/billing/accounts",
        "AdminService_ListBillingAccounts",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/billing/accounts",
        "AdminService_PutBillingAccount",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/billing/invoices",
        "AdminService_ListInvoices",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/billing/close-period",
        "AdminService_CloseBillingPeriod",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/billing/sync",
        "AdminService_SyncBilling",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/billing/invoices/mark-paid",
        "AdminService_MarkInvoicePaid",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/billing/dunning",
        "AdminService_RunDunning",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/billing/catalog/reload",
        "AdminService_ReloadBillingCatalog",
        Write,
    ),
    admin("POST", "/v1/admin/users", "AdminService_CreateUser", Write),
    admin(
        "GET",
        "/v1/admin/users/{user_id}",
        "AdminService_GetUser",
        Read,
    ),
    admin("POST", "/v1/admin/orgs", "AdminService_CreateOrg", Write),
    admin("GET", "/v1/admin/orgs/{org}", "AdminService_GetOrg", Read),
    admin(
        "GET",
        "/v1/admin/orgs/{org}/members",
        "AdminService_ListOrgMembers",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/orgs/{org}/members",
        "AdminService_AddOrgMember",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/orgs/{org}/members/{user_id}/role",
        "AdminService_SetOrgMemberRole",
        Write,
    ),
    admin(
        "DELETE",
        "/v1/admin/orgs/{org}/members/{user_id}",
        "AdminService_RemoveOrgMember",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/orgs/{org}/projects",
        "AdminService_ListProjects",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/orgs/{org}/projects",
        "AdminService_CreateProject",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/orgs/{org}/tenants",
        "AdminService_ListOrgTenants",
        Read,
    ),
    admin(
        "POST",
        "/v1/admin/orgs/{org}/tenants",
        "AdminService_CreateOrgTenant",
        Write,
    ),
    admin(
        "POST",
        "/v1/admin/orgs/{org}/tenants/{tenant_id}/api-keys",
        "AdminService_CreateOrgTenantApiKey",
        Write,
    ),
    admin(
        "GET",
        "/v1/admin/orgs/{org}/audit-log",
        "AdminService_ListAuditLog",
        Read,
    ),
];

/// Whether `path` (no query string) fits `template`: same segment count,
/// literal segments equal, and each `{param}` one non-empty segment.
fn template_matches(template: &str, path: &str) -> bool {
    let mut want = template.split('/');
    let mut got = path.split('/');
    loop {
        match (want.next(), got.next()) {
            (None, None) => return true,
            (Some(w), Some(g)) if w.starts_with('{') && w.ends_with('}') => {
                if g.is_empty() {
                    return false;
                }
            }
            (Some(w), Some(g)) if w == g => {}
            _ => return false,
        }
    }
}

/// The path of a request target without its query string.
fn bare(path: &str) -> &str {
    path.split('?').next().unwrap_or(path)
}

/// The route serving `method` + `path` (query string allowed).
pub(crate) fn lookup(method: &str, path: &str) -> Option<&'static Route> {
    let path = bare(path);
    ROUTES
        .iter()
        .find(|route| route.method == method && template_matches(route.template, path))
}

/// The methods the table serves on `path`, for a 405's `Allow` header.
/// Empty when the path is unknown.
pub(crate) fn allowed_methods(path: &str) -> Vec<&'static str> {
    let path = bare(path);
    let mut methods: Vec<&'static str> = ROUTES
        .iter()
        .filter(|route| template_matches(route.template, path))
        .map(|route| route.method)
        .collect();
    methods.sort_unstable();
    methods.dedup();
    methods
}

/// The route for a raw request line (`"METHOD /path HTTP/1.1"`).
pub(crate) fn lookup_line(request_line: &str) -> Option<&'static Route> {
    let (method, path) = crate::parse_method_and_path(request_line)?;
    lookup(method, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_match_one_segment_per_param() {
        assert!(template_matches(
            "/v1/namespaces/{namespace}/search",
            "/v1/namespaces/docs/search"
        ));
        assert!(!template_matches(
            "/v1/namespaces/{namespace}/search",
            "/v1/namespaces//search"
        ));
        assert!(!template_matches(
            "/v1/namespaces/{namespace}/search",
            "/v1/namespaces/a/b/search"
        ));
        assert!(!template_matches(
            "/v1/namespaces/{namespace}",
            "/v1/namespaces"
        ));
        assert_eq!(
            lookup("GET", "/v1/admin/api-keys?tenant_id=acme").map(|r| r.operation_id),
            Some("AdminService_ListApiKeys")
        );
    }

    #[test]
    fn no_two_rows_overlap() {
        for (i, a) in ROUTES.iter().enumerate() {
            for b in &ROUTES[i + 1..] {
                assert!(
                    a.operation_id != b.operation_id,
                    "duplicate operation id {}",
                    a.operation_id
                );
                if a.method != b.method {
                    continue;
                }
                // Instantiate each template with a sample value and make
                // sure the other row does not claim it.
                let sample = |t: &str| {
                    t.split('/')
                        .map(|s| if s.starts_with('{') { "x" } else { s })
                        .collect::<Vec<_>>()
                        .join("/")
                };
                assert!(
                    !template_matches(b.template, &sample(a.template)),
                    "{} {} overlaps {}",
                    a.method,
                    a.template,
                    b.template
                );
                assert!(
                    !template_matches(a.template, &sample(b.template)),
                    "{} {} overlaps {}",
                    b.method,
                    b.template,
                    a.template
                );
            }
        }
    }

    #[test]
    fn allowed_methods_lists_every_method_on_a_path() {
        assert_eq!(allowed_methods("/v1/namespaces"), vec!["GET", "POST"]);
        assert_eq!(
            allowed_methods("/v1/namespaces/docs/schema"),
            vec!["GET", "POST"]
        );
        assert_eq!(allowed_methods("/v1/namespaces/docs/search"), vec!["POST"]);
        assert!(allowed_methods("/v1/namespaces/docs/nope").is_empty());
    }

    /// Pinned routing behavior: `(request line, forwarded to ingest, search route)`.
    const PINNED: &[(&str, bool, bool)] = &[
        ("GET /v1/healthz HTTP/1.1", false, false),
        ("POST /v1/namespaces HTTP/1.1", true, false),
        ("GET /v1/namespaces HTTP/1.1", false, false),
        ("DELETE /v1/namespaces/docs HTTP/1.1", true, false),
        ("POST /v1/namespaces/docs/documents HTTP/1.1", true, false),
        ("POST /v1/namespaces/docs/search HTTP/1.1", false, true),
        ("POST /v1/namespaces/docs/read HTTP/1.1", false, true),
        ("POST /v1/namespaces/docs/exists HTTP/1.1", true, false),
        ("POST /v1/namespaces/docs/flush HTTP/1.1", true, false),
        ("POST /v1/namespaces/docs/delete HTTP/1.1", true, false),
        ("GET /v1/namespaces/docs/stats HTTP/1.1", false, false),
        ("POST /v1/namespaces/docs/schema HTTP/1.1", true, false),
        ("GET /v1/namespaces/docs/schema HTTP/1.1", false, false),
        ("POST /v1/namespaces/docs/shards HTTP/1.1", true, false),
        ("POST /v1/namespaces/docs/branch HTTP/1.1", true, false),
        ("GET /v1/namespaces/docs/branches HTTP/1.1", false, false),
        ("DELETE /v1/namespaces/docs/branch HTTP/1.1", true, false),
        ("GET /v1/usage HTTP/1.1", false, false),
        ("GET /v1/billing HTTP/1.1", false, false),
        ("GET /v1/stats HTTP/1.1", false, false),
        ("GET /v1/admin/usage HTTP/1.1", false, false),
        ("GET /v1/admin/usage/reconcile HTTP/1.1", false, false),
        ("GET /v1/admin/warmup-status HTTP/1.1", false, false),
        ("POST /v1/admin/rebuild-filter-blooms HTTP/1.1", true, false),
        (
            "POST /v1/admin/backfill-offset-tables HTTP/1.1",
            true,
            false,
        ),
        ("POST /v1/admin/compact-namespace HTTP/1.1", true, false),
        ("GET /v1/admin/compaction-status HTTP/1.1", true, false),
        ("GET /v1/admin/compaction-jobs/j1 HTTP/1.1", true, false),
        ("POST /v1/admin/import-namespace HTTP/1.1", true, false),
        ("POST /v1/admin/gc HTTP/1.1", true, false),
        ("POST /v1/admin/api-keys HTTP/1.1", true, false),
        ("GET /v1/admin/api-keys HTTP/1.1", false, false),
        ("POST /v1/admin/api-keys/revoke HTTP/1.1", true, false),
        ("POST /v1/admin/api-keys/rotate HTTP/1.1", true, false),
        (
            "POST /v1/admin/api-keys/scrub-plaintext HTTP/1.1",
            true,
            false,
        ),
        (
            "GET /v1/admin/api-keys/plaintext-count HTTP/1.1",
            false,
            false,
        ),
        ("POST /v1/admin/operators HTTP/1.1", true, false),
        ("GET /v1/admin/operators HTTP/1.1", false, false),
        ("POST /v1/admin/operators/revoke HTTP/1.1", true, false),
        ("POST /v1/admin/operators/rotate HTTP/1.1", true, false),
        ("DELETE /v1/admin/tenants/acme HTTP/1.1", true, false),
        ("GET /v1/admin/deletions HTTP/1.1", false, false),
        ("GET /v1/admin/deletions/d1 HTTP/1.1", false, false),
        ("POST /v1/admin/deletions/resume HTTP/1.1", true, false),
        ("GET /v1/admin/billing/accounts HTTP/1.1", false, false),
        ("POST /v1/admin/billing/accounts HTTP/1.1", true, false),
        ("GET /v1/admin/billing/invoices HTTP/1.1", false, false),
        ("POST /v1/admin/billing/close-period HTTP/1.1", true, false),
        ("POST /v1/admin/billing/sync HTTP/1.1", true, false),
        (
            "POST /v1/admin/billing/invoices/mark-paid HTTP/1.1",
            true,
            false,
        ),
        ("POST /v1/admin/users HTTP/1.1", true, false),
        ("GET /v1/admin/users/u1 HTTP/1.1", false, false),
        ("POST /v1/admin/orgs HTTP/1.1", true, false),
        ("GET /v1/admin/orgs/o1 HTTP/1.1", false, false),
        ("GET /v1/admin/orgs/o1/members HTTP/1.1", false, false),
        ("POST /v1/admin/orgs/o1/members HTTP/1.1", true, false),
        (
            "POST /v1/admin/orgs/o1/members/u1/role HTTP/1.1",
            true,
            false,
        ),
        ("DELETE /v1/admin/orgs/o1/members/u1 HTTP/1.1", true, false),
        ("GET /v1/admin/orgs/o1/projects HTTP/1.1", false, false),
        ("POST /v1/admin/orgs/o1/projects HTTP/1.1", true, false),
        ("GET /v1/admin/orgs/o1/tenants HTTP/1.1", false, false),
        ("POST /v1/admin/orgs/o1/tenants HTTP/1.1", true, false),
        (
            "POST /v1/admin/orgs/o1/tenants/t1/api-keys HTTP/1.1",
            true,
            false,
        ),
        ("GET /v1/admin/orgs/o1/audit-log HTTP/1.1", false, false),
        ("GET /v1/api-keys HTTP/1.1", false, false),
        ("POST /v1/api-keys HTTP/1.1", false, false),
        ("POST /v1/api-keys/rotate HTTP/1.1", false, false),
        ("POST /v1/api-keys/revoke HTTP/1.1", false, false),
        ("POST /v1/billing/checkout HTTP/1.1", false, false),
        ("POST /v1/billing/portal HTTP/1.1", false, false),
        ("POST /v1/billing/webhooks/stripe HTTP/1.1", false, false),
        ("POST /v1/namespaces/docs/exports HTTP/1.1", true, false),
        ("GET /v1/namespaces/docs/exports HTTP/1.1", false, false),
        ("GET /v1/namespaces/docs/exports/e1 HTTP/1.1", false, false),
        (
            "DELETE /v1/namespaces/docs/exports/e1 HTTP/1.1",
            true,
            false,
        ),
        (
            "GET /v1/namespaces/docs/exports/e1/download HTTP/1.1",
            false,
            false,
        ),
        ("POST /v1/admin/billing/dunning HTTP/1.1", true, false),
        (
            "POST /v1/admin/billing/catalog/reload HTTP/1.1",
            true,
            false,
        ),
        // Legacy routes stay hand-classified.
        ("POST /index HTTP/1.1", true, false),
        ("POST /replace HTTP/1.1", true, false),
        ("POST /flush HTTP/1.1", true, false),
        ("POST /delete HTTP/1.1", true, false),
        ("POST /exists HTTP/1.1", true, false),
        ("POST /search HTTP/1.1", false, true),
        ("GET /search?ns=docs&q=x HTTP/1.1", false, true),
        ("GET /stats HTTP/1.1", false, false),
        ("GET /healthz HTTP/1.1", false, false),
    ];

    #[test]
    fn route_table_pins_today() {
        for &(line, ingest, search) in PINNED {
            assert_eq!(
                crate::is_ingest_path(line),
                ingest,
                "is_ingest_path({line})"
            );
            assert_eq!(
                crate::is_search_route(line),
                search,
                "is_search_route({line})"
            );
        }
    }

    #[test]
    fn every_route_is_pinned() {
        for route in ROUTES {
            if route.operation_id == "KoshaService_ReplaceDocuments" {
                continue; // new in #350
            }
            assert!(
                PINNED
                    .iter()
                    .any(|(line, _, _)| lookup_line(line) == Some(route)),
                "{} {} has no pinned classification",
                route.method,
                route.template
            );
        }
    }
}
