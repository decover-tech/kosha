//! Scoped tenant API keys (#346): scopes, namespace restrictions, expiry,
//! rotation with an overlap window, revocation, and hashed storage.
//!
//! A key is `sk-<prefix>_<secret>`: `<prefix>` is 8 random hex digits shown
//! in listings so an operator can tell keys apart, `<secret>` is 32 random
//! bytes in hex. Only `SHA-256(key)` is stored. The keys are high-entropy
//! random strings, so a plain hash is enough: there is nothing to
//! brute-force and no salt or slow KDF is needed. The plaintext is returned
//! once, by [`ApiKeyStore::create`] or [`ApiKeyStore::rotate`], and never
//! again.
//!
//! [`MemoryApiKeyStore`] backs tests and env-only deployments; `PgStore`
//! implements the same trait on `kosha.api_keys` (migration 004).

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kosha_core::KoshaError;
use sha2::{Digest, Sha256};

/// One permission a key may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    /// Search, exists, stats, read schema, list namespaces and branches.
    Read,
    /// Index, flush and delete documents.
    Write,
    /// Create and delete namespaces, change schema and shards, create and
    /// delete branches.
    Admin,
}

impl Scope {
    pub const ALL: [Scope; 3] = [Scope::Read, Scope::Write, Scope::Admin];

    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Read => "read",
            Scope::Write => "write",
            Scope::Admin => "admin",
        }
    }

    pub fn parse(name: &str) -> Option<Scope> {
        match name.trim().to_ascii_lowercase().as_str() {
            "read" => Some(Scope::Read),
            "write" => Some(Scope::Write),
            "admin" => Some(Scope::Admin),
            _ => None,
        }
    }

    fn bit(self) -> u8 {
        match self {
            Scope::Read => 1,
            Scope::Write => 2,
            Scope::Admin => 4,
        }
    }
}

/// A set of [`Scope`]s, as a bitset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Scopes(u8);

impl Scopes {
    pub const NONE: Scopes = Scopes(0);
    pub const ALL: Scopes = Scopes(1 | 2 | 4);

    pub fn of(scopes: &[Scope]) -> Scopes {
        Scopes(scopes.iter().fold(0, |acc, s| acc | s.bit()))
    }

    pub fn contains(self, scope: Scope) -> bool {
        self.0 & scope.bit() != 0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Parse scope names. An unknown name is an error, never ignored.
    pub fn from_names<S: AsRef<str>>(names: &[S]) -> Result<Scopes, String> {
        let mut out = Scopes::NONE;
        for name in names {
            let scope = Scope::parse(name.as_ref())
                .ok_or_else(|| format!("unknown scope '{}'", name.as_ref()))?;
            out.0 |= scope.bit();
        }
        Ok(out)
    }

    /// The scope names, in `read`, `write`, `admin` order.
    pub fn names(self) -> Vec<&'static str> {
        Scope::ALL
            .into_iter()
            .filter(|s| self.contains(*s))
            .map(Scope::as_str)
            .collect()
    }
}

/// A stored key. Holds the hash, never the plaintext. Times are Unix
/// seconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRecord {
    pub id: String,
    pub tenant_id: String,
    /// `sk-<8 hex>`: the non-secret leading part, for display.
    pub key_prefix: String,
    /// Lowercase hex SHA-256 of the full key.
    pub key_hash: String,
    pub scopes: Scopes,
    /// `None`: every namespace of the tenant.
    pub namespaces: Option<Vec<String>>,
    pub description: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub last_used_at: Option<i64>,
    pub revoked_at: Option<i64>,
    /// The key this one replaced, when it came from a rotation.
    pub rotated_from: Option<String>,
}

impl KeyRecord {
    /// Not revoked and not expired at `now`.
    pub fn is_active_at(&self, now: i64) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|t| t > now)
    }
}

/// What a new key may do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewKey {
    pub tenant_id: String,
    pub scopes: Scopes,
    pub namespaces: Option<Vec<String>>,
    pub description: String,
    pub expires_at: Option<i64>,
}

/// Persistent API keys. Every method that returns a plaintext key returns
/// it exactly once; the store keeps only its hash.
pub trait ApiKeyStore: Send + Sync {
    /// Store a new key and return its record and the plaintext.
    fn create(&self, new: NewKey) -> Result<(KeyRecord, String), KoshaError>;

    /// Keys that are neither revoked nor expired, optionally one tenant's.
    fn list_active(&self, tenant_id: Option<&str>) -> Result<Vec<KeyRecord>, KoshaError>;

    /// Revoke an active key. `false` if there is no such active key.
    fn revoke(&self, id: &str) -> Result<bool, KoshaError>;

    /// Replace an active key: a new key with the same tenant, scopes,
    /// namespaces and description (`rotated_from` = `id`, expiring at
    /// `new_expires_at`), while the old one keeps working for `overlap`
    /// (its `expires_at` becomes `now + overlap`, or stays earlier if it
    /// already was). `None` if there is no such active key.
    fn rotate(
        &self,
        id: &str,
        overlap: Duration,
        new_expires_at: Option<i64>,
    ) -> Result<Option<(KeyRecord, String)>, KoshaError>;

    /// Record when keys were last used: `(key id, unix seconds)`. A time
    /// earlier than the stored one is ignored.
    fn touch_last_used(&self, used: &[(String, i64)]) -> Result<(), KoshaError>;
}

/// Unix seconds now.
pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut buf = [0u8; N];
    getrandom::getrandom(&mut buf).expect("OS random number generator unavailable");
    buf
}

/// Lowercase hex SHA-256 of a presented key: the only form keys are
/// stored and looked up in.
pub fn hash_key(key: &str) -> String {
    to_hex(&Sha256::digest(key.as_bytes()))
}

/// A fresh `sk-<prefix>_<secret>` key and its display prefix `sk-<prefix>`.
pub fn generate_key() -> (String, String) {
    let prefix = format!("sk-{}", to_hex(&random_bytes::<4>()));
    let key = format!("{prefix}_{}", to_hex(&random_bytes::<32>()));
    (key, prefix)
}

/// The display prefix of any key, including keys minted before #346
/// (`sk-<uuid>`): its first 11 characters at most.
pub fn key_prefix_of(key: &str) -> String {
    match key.split_once('_') {
        Some((prefix, _)) if prefix.starts_with("sk-") && prefix.len() <= 16 => prefix.into(),
        _ => key.chars().take(11).collect(),
    }
}

/// A random (v4) UUID string, the id of a key.
pub fn new_key_id() -> String {
    let mut b = random_bytes::<16>();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let h = to_hex(&b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// What the legacy plaintext `api_key` column holds once a key is hashed:
/// recognisably not a key (it does not start with `sk-`, and no stored
/// hash matches it), unique per row since the column is the primary key.
pub fn redacted_marker(prefix: &str, id: &str) -> String {
    format!("redacted:{prefix}:{id}")
}

/// In-memory [`ApiKeyStore`]: tests and deployments without Postgres. Keys
/// live as long as the process.
#[derive(Default)]
pub struct MemoryApiKeyStore {
    records: Mutex<Vec<KeyRecord>>,
}

impl MemoryApiKeyStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every record, revoked and expired included (tests).
    pub fn all_records(&self) -> Vec<KeyRecord> {
        self.records.lock().unwrap().clone()
    }

    fn insert(&self, new: NewKey, rotated_from: Option<String>) -> (KeyRecord, String) {
        let (key, prefix) = generate_key();
        let record = KeyRecord {
            id: new_key_id(),
            tenant_id: new.tenant_id,
            key_prefix: prefix,
            key_hash: hash_key(&key),
            scopes: new.scopes,
            namespaces: new.namespaces,
            description: new.description,
            created_at: now_unix(),
            expires_at: new.expires_at,
            last_used_at: None,
            revoked_at: None,
            rotated_from,
        };
        self.records.lock().unwrap().push(record.clone());
        (record, key)
    }
}

impl ApiKeyStore for MemoryApiKeyStore {
    fn create(&self, new: NewKey) -> Result<(KeyRecord, String), KoshaError> {
        Ok(self.insert(new, None))
    }

    fn list_active(&self, tenant_id: Option<&str>) -> Result<Vec<KeyRecord>, KoshaError> {
        let now = now_unix();
        let mut out: Vec<KeyRecord> = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.is_active_at(now))
            .filter(|r| tenant_id.is_none_or(|t| r.tenant_id == t))
            .cloned()
            .collect();
        out.sort_by_key(|r| std::cmp::Reverse(r.created_at));
        Ok(out)
    }

    fn revoke(&self, id: &str) -> Result<bool, KoshaError> {
        let now = now_unix();
        let mut records = self.records.lock().unwrap();
        match records
            .iter_mut()
            .find(|r| r.id == id && r.is_active_at(now))
        {
            Some(r) => {
                r.revoked_at = Some(now);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn rotate(
        &self,
        id: &str,
        overlap: Duration,
        new_expires_at: Option<i64>,
    ) -> Result<Option<(KeyRecord, String)>, KoshaError> {
        let now = now_unix();
        let new = {
            let mut records = self.records.lock().unwrap();
            let Some(old) = records
                .iter_mut()
                .find(|r| r.id == id && r.is_active_at(now))
            else {
                return Ok(None);
            };
            let until = now.saturating_add(overlap.as_secs() as i64);
            old.expires_at = Some(old.expires_at.map_or(until, |t| t.min(until)));
            NewKey {
                tenant_id: old.tenant_id.clone(),
                scopes: old.scopes,
                namespaces: old.namespaces.clone(),
                description: old.description.clone(),
                expires_at: new_expires_at,
            }
        };
        Ok(Some(self.insert(new, Some(id.to_string()))))
    }

    fn touch_last_used(&self, used: &[(String, i64)]) -> Result<(), KoshaError> {
        let mut records = self.records.lock().unwrap();
        for (id, at) in used {
            if let Some(r) = records.iter_mut().find(|r| &r.id == id) {
                if r.last_used_at.is_none_or(|t| t < *at) {
                    r.last_used_at = Some(*at);
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "postgres")]
pub(crate) mod pg {
    //! [`ApiKeyStore`] on `kosha.api_keys` (migrations 001 + 004).

    use super::*;
    use crate::PgStore;

    fn err(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    const COLUMNS: &str = "id::text, tenant_id, key_prefix, key_hash, scopes, namespaces, \
         description, EXTRACT(EPOCH FROM created_at)::BIGINT, \
         EXTRACT(EPOCH FROM expires_at)::BIGINT, EXTRACT(EPOCH FROM last_used_at)::BIGINT, \
         EXTRACT(EPOCH FROM revoked_at)::BIGINT, rotated_from::text";

    type Row = (
        String,
        String,
        String,
        Option<String>,
        Vec<String>,
        Option<Vec<String>>,
        String,
        i64,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<String>,
    );

    fn decode(row: Row) -> Option<KeyRecord> {
        let (id, tenant_id, key_prefix, key_hash, scopes, namespaces, description, created_at) =
            (row.0, row.1, row.2, row.3?, row.4, row.5, row.6, row.7);
        Some(KeyRecord {
            id,
            tenant_id,
            key_prefix,
            key_hash,
            // An unknown name in the column grants nothing.
            scopes: Scopes::of(
                &scopes
                    .iter()
                    .filter_map(|s| Scope::parse(s))
                    .collect::<Vec<_>>(),
            ),
            namespaces,
            description,
            created_at,
            expires_at: row.8,
            last_used_at: row.9,
            revoked_at: row.10,
            rotated_from: row.11,
        })
    }

    async fn insert<'c, E>(
        exec: E,
        new: &NewKey,
        rotated_from: Option<&str>,
    ) -> Result<(KeyRecord, String), KoshaError>
    where
        E: sqlx::PgExecutor<'c>,
    {
        let (key, prefix) = generate_key();
        let id = new_key_id();
        let scopes: Vec<String> = new.scopes.names().into_iter().map(String::from).collect();
        let row: Row = sqlx::query_as(&format!(
            "INSERT INTO kosha.api_keys \
                 (api_key, id, tenant_id, key_prefix, key_hash, scopes, namespaces, \
                  description, expires_at, rotated_from) \
             VALUES ($1, $2::uuid, $3, $4, $5, $6, $7, $8, \
                     to_timestamp(($9::bigint)::float8), $10::uuid) \
             RETURNING {COLUMNS}"
        ))
        .bind(redacted_marker(&prefix, &id))
        .bind(&id)
        .bind(&new.tenant_id)
        .bind(&prefix)
        .bind(hash_key(&key))
        .bind(&scopes)
        .bind(&new.namespaces)
        .bind(&new.description)
        .bind(new.expires_at)
        .bind(rotated_from)
        .fetch_one(exec)
        .await
        .map_err(err)?;
        let record = decode(row).ok_or_else(|| KoshaError::ControlPlane("no key hash".into()))?;
        Ok((record, key))
    }

    /// Hash every row stored before #346 and overwrite its plaintext with a
    /// [`redacted_marker`], so the database holds no usable key. Runs at
    /// every startup after the migrations; a row another process already
    /// converted is skipped (`key_hash IS NULL` in the `WHERE`).
    pub(crate) async fn backfill_key_hashes(pool: &sqlx::PgPool) -> Result<usize, String> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT api_key, id::text FROM kosha.api_keys WHERE key_hash IS NULL")
                .fetch_all(pool)
                .await
                .map_err(|e| format!("api key backfill: {e}"))?;
        let mut done = 0;
        for (plaintext, id) in rows {
            let prefix = key_prefix_of(&plaintext);
            let result = sqlx::query(
                "UPDATE kosha.api_keys \
                 SET key_hash = $2, key_prefix = $3, api_key = $4 \
                 WHERE api_key = $1 AND key_hash IS NULL",
            )
            .bind(&plaintext)
            .bind(hash_key(&plaintext))
            .bind(&prefix)
            .bind(redacted_marker(&prefix, &id))
            .execute(pool)
            .await
            .map_err(|e| format!("api key backfill: {e}"))?;
            done += result.rows_affected() as usize;
        }
        Ok(done)
    }

    impl ApiKeyStore for PgStore {
        fn create(&self, new: NewKey) -> Result<(KeyRecord, String), KoshaError> {
            let (pool, rt) = self.pool_and_runtime();
            rt.block_on(insert(pool, &new, None))
        }

        fn list_active(&self, tenant_id: Option<&str>) -> Result<Vec<KeyRecord>, KoshaError> {
            let (pool, rt) = self.pool_and_runtime();
            rt.block_on(async {
                let rows: Vec<Row> = sqlx::query_as(&format!(
                    "SELECT {COLUMNS} FROM kosha.api_keys \
                     WHERE revoked_at IS NULL AND key_hash IS NOT NULL \
                       AND (expires_at IS NULL OR expires_at > now()) \
                       AND ($1::text IS NULL OR tenant_id = $1) \
                     ORDER BY created_at DESC"
                ))
                .bind(tenant_id)
                .fetch_all(pool)
                .await
                .map_err(err)?;
                Ok(rows.into_iter().filter_map(decode).collect())
            })
        }

        fn revoke(&self, id: &str) -> Result<bool, KoshaError> {
            let (pool, rt) = self.pool_and_runtime();
            rt.block_on(async {
                let result = sqlx::query(
                    "UPDATE kosha.api_keys SET revoked_at = now() \
                     WHERE id = $1::uuid AND revoked_at IS NULL",
                )
                .bind(id)
                .execute(pool)
                .await
                .map_err(err)?;
                Ok(result.rows_affected() > 0)
            })
        }

        fn rotate(
            &self,
            id: &str,
            overlap: Duration,
            new_expires_at: Option<i64>,
        ) -> Result<Option<(KeyRecord, String)>, KoshaError> {
            let (pool, rt) = self.pool_and_runtime();
            rt.block_on(async {
                let mut tx = pool.begin().await.map_err(err)?;
                let old: Option<Row> = sqlx::query_as(&format!(
                    "SELECT {COLUMNS} FROM kosha.api_keys \
                     WHERE id = $1::uuid AND revoked_at IS NULL AND key_hash IS NOT NULL \
                       AND (expires_at IS NULL OR expires_at > now()) \
                     FOR UPDATE"
                ))
                .bind(id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(err)?;
                let Some(old) = old.and_then(decode) else {
                    return Ok(None);
                };
                sqlx::query(
                    "UPDATE kosha.api_keys \
                     SET expires_at = LEAST(COALESCE(expires_at, 'infinity'::timestamptz), \
                                            now() + ($2::float8 * interval '1 second')) \
                     WHERE id = $1::uuid",
                )
                .bind(id)
                .bind(overlap.as_secs_f64())
                .execute(&mut *tx)
                .await
                .map_err(err)?;
                let new = NewKey {
                    tenant_id: old.tenant_id,
                    scopes: old.scopes,
                    namespaces: old.namespaces,
                    description: old.description,
                    expires_at: new_expires_at,
                };
                let created = insert(&mut *tx, &new, Some(id)).await?;
                tx.commit().await.map_err(err)?;
                Ok(Some(created))
            })
        }

        fn touch_last_used(&self, used: &[(String, i64)]) -> Result<(), KoshaError> {
            if used.is_empty() {
                return Ok(());
            }
            let ids: Vec<&str> = used.iter().map(|(id, _)| id.as_str()).collect();
            let times: Vec<i64> = used.iter().map(|(_, t)| *t).collect();
            let (pool, rt) = self.pool_and_runtime();
            rt.block_on(async {
                sqlx::query(
                    "UPDATE kosha.api_keys k \
                     SET last_used_at = to_timestamp(u.at::float8) \
                     FROM unnest($1::text[], $2::bigint[]) AS u(id, at) \
                     WHERE k.id = u.id::uuid \
                       AND (k.last_used_at IS NULL OR k.last_used_at < to_timestamp(u.at::float8))",
                )
                .bind(&ids)
                .bind(&times)
                .execute(pool)
                .await
                .map_err(err)?;
                Ok(())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_key(tenant: &str, scopes: &[Scope]) -> NewKey {
        NewKey {
            tenant_id: tenant.into(),
            scopes: Scopes::of(scopes),
            namespaces: None,
            description: String::new(),
            expires_at: None,
        }
    }

    #[test]
    fn scopes_parse_and_print() {
        let s = Scopes::from_names(&["Read", "admin"]).unwrap();
        assert!(s.contains(Scope::Read) && s.contains(Scope::Admin));
        assert!(!s.contains(Scope::Write));
        assert_eq!(s.names(), vec!["read", "admin"]);
        assert!(Scopes::from_names(&["read", "root"]).is_err());
        assert_eq!(Scopes::ALL.names(), vec!["read", "write", "admin"]);
    }

    #[test]
    fn generated_keys_are_distinct_and_only_their_hash_is_stored() {
        let store = MemoryApiKeyStore::new();
        let (a, key_a) = store.create(new_key("acme", &[Scope::Read])).unwrap();
        let (_, key_b) = store.create(new_key("acme", &[Scope::Read])).unwrap();
        assert_ne!(key_a, key_b);
        assert!(key_a.starts_with(&format!("{}_", a.key_prefix)));
        assert_eq!(key_a.len(), "sk-".len() + 8 + 1 + 64);
        assert_eq!(a.key_hash, hash_key(&key_a));
        assert_eq!(key_prefix_of(&key_a), a.key_prefix);
        let dump = format!("{:?}", store.all_records());
        assert!(!dump.contains(&key_a) && !dump.contains(&key_b));
        assert!(!dump.contains(&key_a[a.key_prefix.len() + 1..]));
    }

    #[test]
    fn legacy_prefix_and_marker() {
        let legacy = "sk-0a1b2c3d-1111-2222-3333-444455556666";
        assert_eq!(key_prefix_of(legacy), "sk-0a1b2c3d");
        let marker = redacted_marker("sk-0a1b2c3d", "id-1");
        assert!(marker.starts_with("redacted:") && !marker.contains("1111-2222"));
        let id = new_key_id();
        assert_eq!(id.len(), 36);
        assert_eq!(&id[14..15], "4");
    }

    #[test]
    fn rotate_keeps_the_old_key_for_the_overlap_and_revoke_removes() {
        let store = MemoryApiKeyStore::new();
        let (old, _) = store.create(new_key("acme", &[Scope::Read])).unwrap();
        let (new, _) = store
            .rotate(&old.id, Duration::from_secs(60), None)
            .unwrap()
            .unwrap();
        assert_eq!(new.rotated_from.as_deref(), Some(old.id.as_str()));
        assert_eq!(new.scopes, old.scopes);
        let active = store.list_active(Some("acme")).unwrap();
        assert_eq!(active.len(), 2);
        let old_now = active.iter().find(|r| r.id == old.id).unwrap();
        let until = old_now.expires_at.unwrap();
        assert!(until > now_unix() && until <= now_unix() + 60);
        // Rotating with no overlap ends the old key now.
        let (_, _) = store
            .rotate(&new.id, Duration::ZERO, None)
            .unwrap()
            .unwrap();
        assert!(!store
            .list_active(None)
            .unwrap()
            .iter()
            .any(|r| r.id == new.id));
        // Revoking an unknown or already-ended key reports false.
        assert!(!store.revoke(&new.id).unwrap());
        assert!(store.revoke(&old.id).unwrap());
        assert!(!store
            .list_active(None)
            .unwrap()
            .iter()
            .any(|r| r.id == old.id));
        assert!(store
            .rotate("missing", Duration::ZERO, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn touch_last_used_only_moves_forward() {
        let store = MemoryApiKeyStore::new();
        let (k, _) = store.create(new_key("acme", &[Scope::Read])).unwrap();
        store.touch_last_used(&[(k.id.clone(), 100)]).unwrap();
        store.touch_last_used(&[(k.id.clone(), 50)]).unwrap();
        assert_eq!(store.all_records()[0].last_used_at, Some(100));
    }
}
