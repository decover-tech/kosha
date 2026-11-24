//! An audit row for every state-changing admin route that does not write
//! its own (#346): segment GC, compaction, imports and repairs, tenant
//! deletion, billing and usage administration. Each names the admin
//! credential that made the request, `operator:<name>` for an operator
//! credential and `operator` for the shared bootstrap key, so no admin
//! change is anonymous.
//!
//! `route` wraps those routes in [`audited`]: every non-`GET` request under
//! `/v1/admin/` that the organization, operator-credential and API-key
//! handlers did not answer (those write their own rows, in the transaction
//! of the change). The row is written after the handler returns, with the
//! response status, so a refused or failed attempt is recorded too. It
//! holds the method, the path and the status, never the body (which can
//! carry secrets such as a payment provider's ids). Writing it is best
//! effort: these changes are not database transactions, so an audit store
//! failure is logged and the response is returned unchanged.
//!
//! Rows go to the organization store's `audit_log` (Postgres when
//! configured): under the tenant's organization for a tenant deletion,
//! otherwise with no organization, like the operator-credential rows.

use crate::org_admin::OrgBackend;
use kosha_control::orgs::{Actor, NewAuditEvent};

/// The audit action for an admin route, by path.
fn action(method: &str, bare: &str) -> &'static str {
    let rest = bare.strip_prefix("/v1/admin/").unwrap_or(bare);
    let first = rest.split('/').next().unwrap_or("");
    match (method, first) {
        (_, "gc") => "admin.segment_gc",
        (_, "compact-namespace") => "admin.compaction",
        (_, "tenants" | "deletions") => "admin.tenant_deletion",
        (_, "billing") => "admin.billing",
        (_, "usage") => "admin.usage",
        (_, "import-namespace") => "admin.import_namespace",
        (_, "rebuild-filter-blooms" | "backfill-offset-tables") => "admin.repair",
        _ => "admin.request",
    }
}

/// The tenant a request acts on, when its path names one.
fn target_tenant(method: &str, bare: &str) -> Option<String> {
    let tenant = bare.strip_prefix("/v1/admin/tenants/")?;
    (method == "DELETE" && !tenant.is_empty() && !tenant.contains('/'))
        .then(|| crate::url_decode(tenant))
}

/// The status of a raw HTTP response (`HTTP/1.1 200 OK...`).
fn status(response: &str) -> u16 {
    response
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

/// Run `handler` (a state-changing admin route) and record who did it.
pub(crate) fn audited(
    orgs: &OrgBackend,
    operator: &Actor,
    method: &str,
    path: &str,
    handler: impl FnOnce() -> String,
) -> String {
    let bare = path.split('?').next().unwrap_or(path);
    let tenant = target_tenant(method, bare);
    let store = orgs.store();
    // Read before the handler runs: a tenant deletion removes the link.
    let org_id = match (&store, &tenant) {
        (Ok(store), Some(tenant)) => {
            let mut org = None;
            let _ = store.transact(&mut |tx| {
                org = tx.tenant(tenant)?.map(|link| link.org_id);
                Ok(())
            });
            org
        }
        _ => None,
    };
    let response = handler();
    let (target_type, target_id) = match tenant {
        Some(tenant) => ("tenant", tenant),
        None => ("route", bare.chars().take(256).collect()),
    };
    let event = NewAuditEvent {
        org_id,
        actor: operator.label(),
        action: action(method, bare),
        target_type,
        target_id,
        detail: serde_json::json!({
            "method": method,
            "path": bare.chars().take(256).collect::<String>(),
            "status": status(&response),
        }),
    };
    let written = store.and_then(|store| {
        store
            .transact(&mut |tx| tx.insert_audit(&event))
            .map_err(|e| e.to_string())
    });
    if let Err(e) = written {
        eprintln!(
            "admin audit: failed to record {} {bare} by {}: {e}",
            method, event.actor
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_get_an_action_and_tenant_deletions_a_tenant() {
        assert_eq!(action("POST", "/v1/admin/gc"), "admin.segment_gc");
        assert_eq!(
            action("POST", "/v1/admin/compact-namespace"),
            "admin.compaction"
        );
        assert_eq!(
            action("DELETE", "/v1/admin/tenants/acme"),
            "admin.tenant_deletion"
        );
        assert_eq!(
            action("POST", "/v1/admin/deletions/resume"),
            "admin.tenant_deletion"
        );
        assert_eq!(
            action("POST", "/v1/admin/billing/close-period"),
            "admin.billing"
        );
        assert_eq!(action("POST", "/v1/admin/something-new"), "admin.request");
        assert_eq!(
            target_tenant("DELETE", "/v1/admin/tenants/acme%20co").as_deref(),
            Some("acme co")
        );
        assert_eq!(target_tenant("POST", "/v1/admin/tenants/acme"), None);
        assert_eq!(target_tenant("DELETE", "/v1/admin/tenants/a/b"), None);
        assert_eq!(status("HTTP/1.1 409 Conflict\r\n\r\n{}"), 409);
        assert_eq!(status("garbage"), 0);
    }

    #[test]
    fn the_operator_and_the_outcome_are_recorded_without_the_body() {
        let orgs = OrgBackend::in_memory();
        let alice = Actor::NamedOperator("alice".into());
        let response = audited(&orgs, &alice, "POST", "/v1/admin/gc?dry_run=1", || {
            "HTTP/1.1 200 OK\r\n\r\n{}".to_string()
        });
        assert!(response.starts_with("HTTP/1.1 200"));
        audited(
            &orgs,
            &Actor::Operator,
            "DELETE",
            "/v1/admin/tenants/acme",
            || "HTTP/1.1 404 Not Found\r\n\r\n{}".to_string(),
        );
        let rows = orgs.audit_rows();
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0].actor, "operator:alice");
        assert_eq!(rows[0].action, "admin.segment_gc");
        assert_eq!(rows[0].target_type, "route");
        assert_eq!(rows[0].target_id, "/v1/admin/gc");
        assert_eq!(rows[0].detail["status"], 200);
        assert_eq!(rows[0].org_id, None);
        assert_eq!(rows[1].actor, "operator");
        assert_eq!(rows[1].action, "admin.tenant_deletion");
        assert_eq!(rows[1].target_type, "tenant");
        assert_eq!(rows[1].target_id, "acme");
        assert_eq!(rows[1].detail["status"], 404);
    }
}
