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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn key_hash_is_sha256_hex() {
        // FIPS 180-2 test vector.
        assert_eq!(
            hash_api_key("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!(hash_api_key("sk-a"), hash_api_key("sk-b"));
    }
}
