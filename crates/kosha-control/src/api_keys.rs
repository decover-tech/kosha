//! API-key attributes shared by the Postgres key registry and the server's
//! request authorization (#346): scopes, the at-rest hash, and the record
//! the registry returns. No database code lives here, so the server can use
//! these types without the `postgres` feature.

use sha2::{Digest, Sha256};

/// What one scope lets a tenant key do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Search, exists, namespace stats, reading a schema, listing.
    Read,
    /// Index, replace, delete documents, flush.
    Write,
    /// Create and delete namespaces and branches, change schema or shards.
    Admin,
}

impl Scope {
    const ALL: [Scope; 3] = [Scope::Read, Scope::Write, Scope::Admin];

    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Write => "write",
            Scope::Admin => "admin",
        }
    }

    fn bit(self) -> u8 {
        match self {
            Scope::Read => 1,
            Scope::Write => 2,
            Scope::Admin => 4,
        }
    }

    fn from_name(name: &str) -> Option<Scope> {
        Scope::ALL.into_iter().find(|s| s.as_str() == name)
    }
}

/// A set of [`Scope`]s. Scopes are independent: `admin` does not imply
/// `write`, and `write` does not imply `read`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scopes(u8);

impl Scopes {
    /// Every scope: what a key could do before scopes existed.
    pub const ALL: Scopes = Scopes(7);
    pub const NONE: Scopes = Scopes(0);

    pub fn of(scopes: &[Scope]) -> Scopes {
        Scopes(scopes.iter().fold(0, |bits, s| bits | s.bit()))
    }

    pub fn contains(self, scope: Scope) -> bool {
        self.0 & scope.bit() != 0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Whether every scope in `self` is also in `other`.
    pub fn is_subset_of(self, other: Scopes) -> bool {
        self.0 & !other.0 == 0
    }

    /// The scope names, in `read`, `write`, `admin` order.
    pub fn names(self) -> Vec<&'static str> {
        Scope::ALL
            .into_iter()
            .filter(|s| self.contains(*s))
            .map(Scope::as_str)
            .collect()
    }

    /// The stored form: `read,write,admin`.
    pub fn to_column(self) -> String {
        self.names().join(",")
    }

    /// Parse scope names a caller supplied. An unknown name or an empty
    /// set is an error.
    pub fn parse<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<Scopes, String> {
        let mut bits = 0;
        for name in names {
            let name = name.trim();
            match Scope::from_name(name) {
                Some(scope) => bits |= scope.bit(),
                None => {
                    return Err(format!(
                        "unknown scope '{name}' (expected read, write or admin)"
                    ))
                }
            }
        }
        if bits == 0 {
            return Err("at least one scope is required (read, write or admin)".into());
        }
        Ok(Scopes(bits))
    }

    /// Read the stored column. A name this build does not know grants
    /// nothing, so a value written by a newer build fails closed.
    pub fn from_column(column: &str) -> Scopes {
        Scopes(
            column
                .split(',')
                .filter_map(|name| Scope::from_name(name.trim()))
                .fold(0, |bits, s| bits | s.bit()),
        )
    }
}

/// Lowercase hex SHA-256 of an API key: what the registry stores and
/// matches instead of the key. Keys are random (122 bits for the `sk-<uuid>`
/// keys the server issues), so a fast unsalted hash is enough: there is
/// nothing to brute-force, and an unsalted hash lets one indexed lookup
/// find the row.
pub fn hash_api_key(key: &str) -> String {
    use std::fmt::Write;
    let digest = Sha256::digest(key.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// The `tenant_id` prefix of an operator admin credential's row (#346).
///
/// Operator credentials live in the key registry next to tenant keys, so
/// they share its hashing, expiry, rotation and revocation. The prefix is
/// what tells them apart, and it can never be a tenant: a tenant id may not
/// contain `/` (new keys refuse it, and every tenant route answers 403
/// "invalid tenant" for one), so a server build that does not know about
/// operator credentials cannot use such a row as a tenant key either.
pub const OPERATOR_TENANT_PREFIX: &str = "_operator/";

/// Longest operator name accepted.
pub const MAX_OPERATOR_NAME_LEN: usize = 64;

/// An operator's name: 1 to 64 ASCII letters, digits, `.`, `_`, `-`, `@` or
/// `+`, starting with a letter or digit (`alice`, `alice@example.com`,
/// `ci-deploy`). It is written into the audit log as `operator:<name>`.
pub fn valid_operator_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_OPERATOR_NAME_LEN
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'@' | b'+'))
}

/// The registry `tenant_id` of the operator `name`'s credentials.
pub fn operator_tenant_id(name: &str) -> String {
    format!("{OPERATOR_TENANT_PREFIX}{name}")
}

/// The operator a registry row belongs to; `None` for a tenant key (and for
/// an operator row whose name this build would not have issued).
pub fn operator_name(tenant_id: &str) -> Option<&str> {
    tenant_id
        .strip_prefix(OPERATOR_TENANT_PREFIX)
        .filter(|name| valid_operator_name(name))
}

/// Whether a registry row is an operator credential rather than a tenant
/// key, whatever follows the prefix: a malformed operator row must still
/// never be treated as a tenant key.
pub fn is_operator_tenant_id(tenant_id: &str) -> bool {
    tenant_id.starts_with(OPERATOR_TENANT_PREFIX)
}

/// One row of the key registry. Never carries the key itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyRecord {
    /// [`hash_api_key`] of the key; doubles as the key's public id.
    pub key_hash: String,
    pub tenant_id: String,
    pub description: String,
    pub scopes: Scopes,
    /// `None`: every namespace of the tenant. Otherwise the unqualified
    /// namespace names the key may touch.
    pub namespaces: Option<Vec<String>>,
    /// Unix seconds; `None` never expires.
    pub expires_at: Option<i64>,
    /// Unix seconds; `None` if the key was never used (or only before #346).
    pub last_used_at: Option<i64>,
    /// Unix seconds.
    pub created_at: i64,
}

/// The attributes of a key to create.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewApiKey {
    pub tenant_id: String,
    pub description: String,
    pub scopes: Scopes,
    pub namespaces: Option<Vec<String>>,
    /// Unix seconds; `None` never expires.
    pub expires_at: Option<i64>,
}

/// What a key-granting caller may hand out (#346 tenant self-service): the
/// tenant, scopes, namespaces and expiry of the API key making the request,
/// or of whatever else a front end (the organization console) decides a
/// user may grant. A key it creates or rotates never does more than the
/// authority, nor outlives it; a key it lists, rotates or revokes is one it
/// could have created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyAuthority {
    pub tenant: String,
    pub scopes: Scopes,
    /// `None`: every namespace of the tenant.
    pub namespaces: Option<Vec<String>>,
    /// Unix seconds; `None` never expires.
    pub expires_at: Option<i64>,
}

impl KeyAuthority {
    /// Whether this authority could have issued `record`: same tenant, a
    /// subset of its scopes and namespaces. Expiry is not compared: a
    /// longer-lived key may still be revoked, and rotating it gives a
    /// replacement bounded by [`Self::bound_expiry`]. Never an operator
    /// credential.
    pub fn can_manage(&self, record: &ApiKeyRecord) -> bool {
        record.tenant_id == self.tenant
            && !is_operator_tenant_id(&record.tenant_id)
            && record.scopes.is_subset_of(self.scopes)
            && namespaces_within(record.namespaces.as_deref(), self.namespaces.as_deref())
    }

    /// The expiry a key issued under this authority gets: `requested`, or
    /// the authority's own expiry when none is requested. `Err` when the
    /// request would outlive the authority.
    pub fn bound_expiry(&self, requested: Option<i64>) -> Result<Option<i64>, String> {
        match (requested, self.expires_at) {
            (None, limit) => Ok(limit),
            (Some(at), Some(limit)) if at > limit => Err(format!(
                "the key would outlive the key that grants it (which expires at unix time \
                 {limit}); ask for an expiry no later than that"
            )),
            (Some(at), _) => Ok(Some(at)),
        }
    }

    /// Complete and check a key to create under this authority. Scopes and
    /// namespaces the request left out (`None`) default to the
    /// authority's own; the expiry defaults to the authority's. `Err` is
    /// the reason it may not be granted (a 403).
    pub fn grant(
        &self,
        description: String,
        scopes: Option<Scopes>,
        namespaces: Option<Vec<String>>,
        expires_at: Option<i64>,
    ) -> Result<NewApiKey, String> {
        let scopes = scopes.unwrap_or(self.scopes);
        if !scopes.is_subset_of(self.scopes) {
            return Err(format!(
                "cannot grant scopes [{}]: the granting key has only [{}]",
                scopes.names().join(", "),
                self.scopes.names().join(", ")
            ));
        }
        let namespaces = namespaces.or_else(|| self.namespaces.clone());
        if !namespaces_within(namespaces.as_deref(), self.namespaces.as_deref()) {
            return Err(
                "cannot grant namespaces outside those of the granting key (omit 'namespaces' \
                 to get exactly its own)"
                    .into(),
            );
        }
        Ok(NewApiKey {
            tenant_id: self.tenant.clone(),
            description,
            scopes,
            namespaces,
            expires_at: self.bound_expiry(expires_at)?,
        })
    }
}

/// Whether the namespace allow-list `inner` grants no more than `outer`
/// (`None` = every namespace).
fn namespaces_within(inner: Option<&[String]>, outer: Option<&[String]>) -> bool {
    match (inner, outer) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(inner), Some(outer)) => inner.iter().all(|ns| outer.contains(ns)),
    }
}

/// How long the latest usable operator credential must still have to live
/// for the lock-out guard to count it (#346): one day, time enough to
/// issue or rotate another before it lapses.
pub const LOCKOUT_MARGIN_SECS: i64 = 24 * 60 * 60;

/// Whether a set of usable operator credentials (their expiries, unix
/// seconds, `None` = never) keeps `/v1/admin/*` reachable for at least
/// [`LOCKOUT_MARGIN_SECS`] after `now`.
fn keeps_admin_reachable(expiries: &[Option<i64>], now: i64) -> bool {
    expiries
        .iter()
        .any(|at| at.is_none_or(|at| at >= now + LOCKOUT_MARGIN_SECS))
}

/// The lock-out guard (#346): with the shared bootstrap key switched off,
/// a change to operator credentials must not leave the deployment without
/// an admin credential. `before` and `after` are the expiries of the
/// usable operator credentials before and after the change (a credential
/// being rotated out is not counted after it). `Err` explains a refusal:
/// the change leaves none at all, or it is what makes the last one lapse
/// within [`LOCKOUT_MARGIN_SECS`].
pub fn check_lockout(
    before: &[Option<i64>],
    after: &[Option<i64>],
    now: i64,
) -> Result<(), String> {
    if after.is_empty() {
        return Err(
            "this would leave no usable operator credential while the shared admin key is \
             off (KOSHA_ADMIN_BOOTSTRAP_KEY=disabled, or KOSHA_ADMIN_API_KEY unset), locking \
             every operator out of /v1/admin/*; issue another credential first, or repeat \
             with \"force\": true"
                .into(),
        );
    }
    if keeps_admin_reachable(before, now) && !keeps_admin_reachable(after, now) {
        return Err(format!(
            "this would let the last usable operator credential lapse within {} hours while \
             the shared admin key is off (KOSHA_ADMIN_BOOTSTRAP_KEY=disabled, or \
             KOSHA_ADMIN_API_KEY unset); give it a later expiry, issue another credential \
             first, or repeat with \"force\": true",
            LOCKOUT_MARGIN_SECS / 3600
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority(
        scopes: &[Scope],
        namespaces: Option<&[&str]>,
        expires_at: Option<i64>,
    ) -> KeyAuthority {
        KeyAuthority {
            tenant: "acme".into(),
            scopes: Scopes::of(scopes),
            namespaces: namespaces.map(|ns| ns.iter().map(|n| n.to_string()).collect()),
            expires_at,
        }
    }

    fn key(tenant: &str, scopes: &[Scope], namespaces: Option<&[&str]>) -> ApiKeyRecord {
        ApiKeyRecord {
            key_hash: hash_api_key("k"),
            tenant_id: tenant.into(),
            description: String::new(),
            scopes: Scopes::of(scopes),
            namespaces: namespaces.map(|ns| ns.iter().map(|n| n.to_string()).collect()),
            expires_at: None,
            last_used_at: None,
            created_at: 0,
        }
    }

    #[test]
    fn an_authority_grants_at_most_itself() {
        use Scope::{Admin, Read, Write};
        let docs_admin = authority(&[Read, Admin], Some(&["docs", "wiki"]), Some(1_000));
        // Defaults: its own scopes, namespaces and expiry.
        let new = docs_admin.grant("ci".into(), None, None, None).unwrap();
        assert_eq!(new.tenant_id, "acme");
        assert_eq!(new.scopes, Scopes::of(&[Read, Admin]));
        assert_eq!(new.namespaces, Some(vec!["docs".into(), "wiki".into()]));
        assert_eq!(new.expires_at, Some(1_000));
        // Narrower is fine.
        let narrow = docs_admin
            .grant(
                String::new(),
                Some(Scopes::of(&[Read])),
                Some(vec!["docs".into()]),
                Some(500),
            )
            .unwrap();
        assert_eq!(narrow.expires_at, Some(500));
        // Wider is not.
        let more_scopes = docs_admin.grant(String::new(), Some(Scopes::of(&[Write])), None, None);
        assert!(more_scopes.unwrap_err().contains("write"));
        let more_ns = docs_admin.grant(String::new(), None, Some(vec!["other".into()]), None);
        assert!(more_ns.unwrap_err().contains("namespaces"));
        assert!(docs_admin
            .grant(String::new(), None, None, Some(1_001))
            .is_err());
        // An unrestricted, non-expiring authority grants anything in its tenant.
        let full = authority(&[Read, Write, Admin], None, None);
        let any = full.grant(String::new(), None, None, Some(9_999)).unwrap();
        assert_eq!((any.scopes, any.namespaces), (Scopes::ALL, None));
        // A namespace-restricted one cannot grant "every namespace".
        assert_eq!(
            docs_admin
                .grant(String::new(), None, None, None)
                .unwrap()
                .namespaces
                .map(|n| n.len()),
            Some(2)
        );
    }

    #[test]
    fn an_authority_manages_only_keys_it_could_have_issued() {
        use Scope::{Admin, Read, Write};
        let docs_admin = authority(&[Read, Admin], Some(&["docs"]), None);
        assert!(docs_admin.can_manage(&key("acme", &[Read], Some(&["docs"]))));
        assert!(docs_admin.can_manage(&key("acme", &[Read, Admin], Some(&["docs"]))));
        assert!(!docs_admin.can_manage(&key("acme", &[Read, Write], Some(&["docs"]))));
        assert!(!docs_admin.can_manage(&key("acme", &[Read], None)));
        assert!(!docs_admin.can_manage(&key("acme", &[Read], Some(&["docs", "x"]))));
        assert!(!docs_admin.can_manage(&key("other", &[Read], Some(&["docs"]))));
        let full = authority(&[Read, Write, Admin], None, None);
        assert!(full.can_manage(&key("acme", &[Read, Write, Admin], None)));
        assert!(!full.can_manage(&key("other", &[Read], None)));
        let operator = KeyAuthority {
            tenant: operator_tenant_id("alice"),
            ..full
        };
        assert!(!operator.can_manage(&key(&operator_tenant_id("alice"), &[], None)));
    }

    #[test]
    fn the_lockout_guard_refuses_only_what_locks_operators_out() {
        let now = 1_000_000;
        let day = LOCKOUT_MARGIN_SECS;
        // Revoking the last one.
        assert!(check_lockout(&[None], &[], now).is_err());
        // Revoking one of two.
        assert!(check_lockout(&[None, Some(now + 2 * day)], &[Some(now + 2 * day)], now).is_ok());
        // Rotating the last long-lived one into a short-lived one.
        let lapse = check_lockout(&[None], &[Some(now + 600)], now).unwrap_err();
        assert!(lapse.contains("lapse"), "{lapse}");
        // ... into one that lives long enough.
        assert!(check_lockout(&[None], &[Some(now + 90 * day)], now).is_ok());
        // Already short-lived: the change does not make it worse.
        assert!(check_lockout(&[Some(now + 60), Some(now + 70)], &[Some(now + 60)], now).is_ok());
        assert!(check_lockout(&[Some(now + 60)], &[], now).is_err());
    }

    #[test]
    fn scope_subsets() {
        let read = Scopes::of(&[Scope::Read]);
        assert!(read.is_subset_of(Scopes::ALL));
        assert!(Scopes::NONE.is_subset_of(read));
        assert!(!Scopes::ALL.is_subset_of(read));
        assert!(read.is_subset_of(read));
    }

    #[test]
    fn scopes_parse_strictly_and_round_trip() {
        let rw = Scopes::parse(["write", " read "]).unwrap();
        assert!(rw.contains(Scope::Read) && rw.contains(Scope::Write));
        assert!(!rw.contains(Scope::Admin));
        assert_eq!(rw.to_column(), "read,write");
        assert_eq!(Scopes::from_column(&rw.to_column()), rw);
        assert_eq!(Scopes::ALL.to_column(), "read,write,admin");
        assert_eq!(Scopes::parse(["read", "write", "admin"]), Ok(Scopes::ALL));
        assert_eq!(Scopes::of(&[Scope::Admin]).names(), vec!["admin"]);

        assert!(Scopes::parse(["read", "root"])
            .unwrap_err()
            .contains("root"));
        assert!(Scopes::parse([]).is_err());
        assert!(Scopes::parse(["Read"]).is_err());
    }

    #[test]
    fn stored_scopes_fail_closed_on_unknown_names() {
        assert_eq!(
            Scopes::from_column("read,superuser"),
            Scopes::of(&[Scope::Read])
        );
        assert_eq!(Scopes::from_column(""), Scopes::NONE);
        assert!(Scopes::from_column("everything").is_empty());
    }

    #[test]
    fn operator_rows_are_told_apart_by_a_prefix_no_tenant_can_have() {
        assert_eq!(operator_tenant_id("alice"), "_operator/alice");
        assert_eq!(operator_name("_operator/alice"), Some("alice"));
        assert_eq!(
            operator_name("_operator/alice@example.com"),
            Some("alice@example.com")
        );
        assert_eq!(operator_name("acme"), None);
        assert_eq!(operator_name("_operator"), None);
        // A malformed operator row names nobody, and is still not a tenant.
        for bad in ["_operator/", "_operator/a/b", "_operator/ alice"] {
            assert_eq!(operator_name(bad), None, "{bad}");
            assert!(is_operator_tenant_id(bad), "{bad}");
        }
        assert!(!is_operator_tenant_id("acme"));
        assert!(!is_operator_tenant_id("_legacy"));

        for good in ["a", "alice", "ci-deploy", "a.b_c+d@example.com", "9lives"] {
            assert!(valid_operator_name(good), "{good}");
        }
        let long = "a".repeat(MAX_OPERATOR_NAME_LEN + 1);
        for bad in [
            "", "-alice", ".alice", "al ice", "a/b", "a:b", "al\u{e9}", &long,
        ] {
            assert!(!valid_operator_name(bad), "{bad}");
        }
        assert!(valid_operator_name(&"a".repeat(MAX_OPERATOR_NAME_LEN)));
    }

    #[test]
    fn key_hash_is_sha256_hex() {
        // FIPS 180-2 test vector.
        assert_eq!(
            hash_api_key("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!(hash_api_key("sk-a"), hash_api_key("sk-b"));
    }
}
