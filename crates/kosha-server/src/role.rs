//! `KOSHA_ROLE` (#254): what this process does.
//!
//! | role         | writes                         | compaction scheduler                  | routes |
//! |--------------|--------------------------------|---------------------------------------|--------|
//! | `query`      | forwarded to `KOSHA_INGEST_HOST` | never                               | all (writes forwarded) |
//! | `ingest`     | executed, under the writer lease | only with `KOSHA_COMPACTION_ON_INGEST` | all |
//! | `indexer`    | none                           | always                                | probes, compaction admin |
//! | `standalone` | executed, under the writer lease | yes (`KOSHA_COMPACTION_ON_INGEST`)   | all |
//!
//! Before #254 the role was a log label and `KOSHA_INGEST_HOST` alone
//! decided whether a pod forwarded writes. An unset `KOSHA_ROLE` keeps that
//! rule (`KOSHA_INGEST_HOST` set: query, otherwise standalone). An explicit
//! role is checked against the rest of the environment, and a pod whose
//! configuration contradicts its role refuses to start instead of quietly
//! becoming a second, uncoordinated writer.

use std::fmt;

/// What this process does. See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    Query,
    Ingest,
    Indexer,
    Standalone,
}

/// The environment `resolve_role` reads, as plain values so it is a pure
/// function (unit tests do not touch the process environment).
#[derive(Debug, Default, Clone)]
pub(crate) struct RoleEnv {
    /// `KOSHA_ROLE`.
    pub role: Option<String>,
    /// `KOSHA_INGEST_HOST`.
    pub ingest_host: Option<String>,
    /// `DATABASE_URL`.
    pub database_url: Option<String>,
}

impl RoleEnv {
    pub(crate) fn from_process() -> Self {
        let var = |name: &str| std::env::var(name).ok();
        Self {
            role: var("KOSHA_ROLE"),
            ingest_host: var("KOSHA_INGEST_HOST"),
            database_url: var("DATABASE_URL"),
        }
    }
}

/// A resolved role, and the warning to log when it was inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resolved {
    pub role: Role,
    pub warning: Option<String>,
}

fn set(value: &Option<String>) -> bool {
    value.as_deref().is_some_and(|v| !v.trim().is_empty())
}

/// Resolve `KOSHA_ROLE` against the rest of the environment. `Err` carries
/// the message to exit with.
pub(crate) fn resolve_role(env: &RoleEnv) -> Result<Resolved, String> {
    let forwards = set(&env.ingest_host);
    let raw = env
        .role
        .as_deref()
        .map(|r| r.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if raw.is_empty() {
        let role = if forwards {
            Role::Query
        } else {
            Role::Standalone
        };
        return Ok(Resolved {
            role,
            warning: Some(format!(
                "KOSHA_ROLE is not set; inferred `{role}` from KOSHA_INGEST_HOST being {}. \
                 Set KOSHA_ROLE explicitly (query, ingest, indexer or standalone)",
                if forwards { "set" } else { "unset" }
            )),
        });
    }
    let role = match raw.as_str() {
        "query" => Role::Query,
        "ingest" => Role::Ingest,
        "indexer" => Role::Indexer,
        "standalone" => Role::Standalone,
        "compaction" => {
            return Err("KOSHA_ROLE=compaction is no longer accepted: it never ran \
                 compaction and started a second, uncoordinated writer. Use \
                 KOSHA_ROLE=indexer for a compaction-only pod"
                .into())
        }
        other => {
            return Err(format!(
                "unknown KOSHA_ROLE {other:?}: use query, ingest, indexer or standalone"
            ))
        }
    };
    match role {
        Role::Query if !forwards => Err(
            "KOSHA_ROLE=query needs KOSHA_INGEST_HOST: a query pod forwards writes to the \
             ingest pod. Without it this pod would execute writes itself as a second writer"
                .into(),
        ),
        Role::Ingest | Role::Indexer | Role::Standalone if forwards => Err(format!(
            "KOSHA_ROLE={role} must not set KOSHA_INGEST_HOST: only a query pod forwards writes"
        )),
        Role::Indexer if !set(&env.database_url) => Err(
            "KOSHA_ROLE=indexer needs DATABASE_URL: it publishes compactions into the \
             control plane it shares with the ingest pod"
                .into(),
        ),
        _ => Ok(Resolved {
            role,
            warning: None,
        }),
    }
}

impl Role {
    /// The `KOSHA_ROLE` string. `query` and `ingest` also name the
    /// persisted warm set (`warm.rs`), so they must not change.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Role::Query => "query",
            Role::Ingest => "ingest",
            Role::Indexer => "indexer",
            Role::Standalone => "standalone",
        }
    }

    /// Executes document writes itself (and so holds writer leases).
    pub(crate) fn accepts_writes(self) -> bool {
        matches!(self, Role::Ingest | Role::Standalone)
    }

    /// Runs the background compaction scheduler, given
    /// `KOSHA_COMPACTION_ON_INGEST`.
    pub(crate) fn runs_compaction_scheduler(self, compaction_on_ingest: bool) -> bool {
        match self {
            Role::Indexer => true,
            Role::Ingest | Role::Standalone => compaction_on_ingest,
            Role::Query => false,
        }
    }

    /// Whether an indexer serves `request_line` (probes are answered before
    /// routing). Only the compaction admin API: everything else is 404.
    pub(crate) fn indexer_serves(request_line: &str) -> bool {
        request_line.starts_with("POST /v1/admin/compact")
            || request_line.starts_with("GET /v1/admin/compaction-")
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `KOSHA_COMPACTION_ON_INGEST`: whether a writer pod (ingest, standalone)
/// also runs the compaction scheduler. Default true, today's behaviour;
/// set it to false once an `indexer` pod compacts instead.
pub(crate) fn compaction_on_ingest(raw: Option<&str>) -> bool {
    match raw.map(|v| v.trim().to_ascii_lowercase()) {
        None => true,
        Some(v) if v.is_empty() => true,
        Some(v) => !matches!(v.as_str(), "false" | "0" | "no" | "off"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(role: Option<&str>, ingest_host: Option<&str>, db: Option<&str>) -> RoleEnv {
        RoleEnv {
            role: role.map(str::to_owned),
            ingest_host: ingest_host.map(str::to_owned),
            database_url: db.map(str::to_owned),
        }
    }

    /// Every role value against every combination of KOSHA_INGEST_HOST and
    /// DATABASE_URL.
    #[test]
    fn resolve_role_covers_every_combination() {
        const HOST: Option<&str> = Some("http://kosha-ingest:8080");
        const DB: Option<&str> = Some("postgres://x");
        use Role::*;
        // (KOSHA_ROLE, host set, db set) -> Ok(role) or Err
        let cases: &[(Option<&str>, bool, bool, Option<Role>)] = &[
            (None, false, false, Some(Standalone)),
            (None, false, true, Some(Standalone)),
            (None, true, false, Some(Query)),
            (None, true, true, Some(Query)),
            (Some(""), true, false, Some(Query)),
            (Some("query"), true, false, Some(Query)),
            (Some("query"), true, true, Some(Query)),
            (Some("query"), false, false, None),
            (Some("query"), false, true, None),
            (Some("ingest"), false, false, Some(Ingest)),
            (Some("ingest"), false, true, Some(Ingest)),
            (Some("ingest"), true, false, None),
            (Some("ingest"), true, true, None),
            (Some("indexer"), false, true, Some(Indexer)),
            (Some("indexer"), false, false, None),
            (Some("indexer"), true, true, None),
            (Some("indexer"), true, false, None),
            (Some("standalone"), false, false, Some(Standalone)),
            (Some("standalone"), false, true, Some(Standalone)),
            (Some("standalone"), true, false, None),
            (Some("standalone"), true, true, None),
            (Some(" Ingest "), false, false, Some(Ingest)),
            (Some("compaction"), false, false, None),
            (Some("compaction"), false, true, None),
            (Some("compaction"), true, true, None),
            (Some("writer"), false, false, None),
        ];
        for &(role, host, db, want) in cases {
            let e = env(
                role,
                host.then_some(HOST.unwrap()),
                db.then_some(DB.unwrap()),
            );
            let got = resolve_role(&e);
            match want {
                Some(want) => {
                    let got = got.unwrap_or_else(|e| panic!("{role:?} host={host} db={db}: {e}"));
                    assert_eq!(got.role, want, "{role:?} host={host} db={db}");
                    // Only an inferred role warns.
                    let inferred = role.is_none_or(|r| r.trim().is_empty());
                    assert_eq!(got.warning.is_some(), inferred, "{role:?}");
                }
                None => assert!(got.is_err(), "{role:?} host={host} db={db}: {got:?}"),
            }
        }
        // An empty KOSHA_INGEST_HOST counts as unset.
        assert_eq!(
            resolve_role(&env(Some("ingest"), Some(""), None))
                .unwrap()
                .role,
            Ingest
        );
        assert!(resolve_role(&env(Some("query"), Some("  "), None)).is_err());
    }

    #[test]
    fn legacy_compaction_role_points_to_indexer() {
        let err = resolve_role(&env(Some("compaction"), None, Some("postgres://x"))).unwrap_err();
        assert!(err.contains("KOSHA_ROLE=indexer"), "{err}");
    }

    #[test]
    fn warm_set_role_names_are_stable() {
        assert_eq!(Role::Query.as_str(), "query");
        assert_eq!(Role::Ingest.as_str(), "ingest");
    }

    #[test]
    fn compaction_placement_per_role() {
        for on_ingest in [true, false] {
            assert!(Role::Indexer.runs_compaction_scheduler(on_ingest));
            assert!(!Role::Query.runs_compaction_scheduler(on_ingest));
            assert_eq!(Role::Ingest.runs_compaction_scheduler(on_ingest), on_ingest);
            assert_eq!(
                Role::Standalone.runs_compaction_scheduler(on_ingest),
                on_ingest
            );
        }
        assert!(compaction_on_ingest(None));
        assert!(compaction_on_ingest(Some("true")));
        assert!(!compaction_on_ingest(Some("false")));
        assert!(!compaction_on_ingest(Some("0")));
        assert!(!compaction_on_ingest(Some(" FALSE ")));
        assert!(Role::Ingest.accepts_writes() && Role::Standalone.accepts_writes());
        assert!(!Role::Query.accepts_writes() && !Role::Indexer.accepts_writes());
    }

    #[test]
    fn indexer_serves_only_the_compaction_admin_api() {
        for line in [
            "POST /v1/admin/compact-namespace HTTP/1.1",
            "GET /v1/admin/compaction-status HTTP/1.1",
            "GET /v1/admin/compaction-jobs/abc HTTP/1.1",
        ] {
            assert!(Role::indexer_serves(line), "{line}");
        }
        for line in [
            "POST /v1/namespaces/docs/documents HTTP/1.1",
            "POST /v1/namespaces/docs/search HTTP/1.1",
            "POST /index HTTP/1.1",
            "POST /v1/admin/gc HTTP/1.1",
            "GET /stats HTTP/1.1",
            "DELETE /v1/namespaces/docs HTTP/1.1",
        ] {
            assert!(!Role::indexer_serves(line), "{line}");
        }
    }
}
