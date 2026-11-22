//! The Postgres API-key registry (`kosha.api_keys`, #346): keys are matched
//! and stored by SHA-256 hash, carry scopes, an optional namespace
//! allow-list and an optional expiry, and can be rotated with an overlap
//! window. Schema: `migrations/003_scoped_api_keys.sql`.
//!
//! Creating, rotating and revoking a key each append a row to
//! `kosha.audit_log` (migration 006, #345) in the transaction that makes
//! the change, with the operator as the actor: these back the operator's
//! `/v1/admin/api-keys` routes. Keys created through the organization
//! console are audited by `orgs::create_api_key`.

use crate::api_keys::{hash_api_key, ApiKeyRecord, NewApiKey, Scopes};
use crate::PgStore;
use kosha_core::KoshaError;
use uuid::Uuid;

/// A row's key hash. Rows an older server build inserts while a rolling
/// deploy is in progress have no `key_hash` yet; their hash is computed
/// from the plaintext they still hold, so such keys work on new pods too.
const KEY_HASH: &str = "COALESCE(key_hash, encode(sha256(convert_to(api_key, 'UTF8')), 'hex'))";

/// What the `api_key` column holds instead of the key. The leading space
/// makes it unpresentable: header values are trimmed, so a build from
/// before #346 (which matches that column against the presented key) can
/// never be handed this string as a credential.
pub(crate) fn placeholder(key_hash: &str) -> String {
    format!(" sha256:{key_hash}")
}

type Row = (
    String,
    String,
    String,
    String,
    Option<Vec<String>>,
    Option<i64>,
    Option<i64>,
    i64,
);

fn select(condition: &str) -> String {
    format!(
        "SELECT {KEY_HASH}, tenant_id, description, scopes, namespaces, \
                EXTRACT(EPOCH FROM expires_at)::BIGINT, \
                EXTRACT(EPOCH FROM last_used_at)::BIGINT, \
                EXTRACT(EPOCH FROM created_at)::BIGINT \
         FROM kosha.api_keys WHERE {condition}"
    )
}

fn record(row: Row) -> ApiKeyRecord {
    let (key_hash, tenant_id, description, scopes, namespaces, expires_at, last_used_at, created) =
        row;
    ApiKeyRecord {
        key_hash,
        tenant_id,
        description,
        scopes: Scopes::from_column(&scopes),
        namespaces,
        expires_at,
        last_used_at,
        created_at: created,
    }
}

fn db_error(e: sqlx::Error) -> KoshaError {
    KoshaError::ControlPlane(e.to_string())
}

/// A key issued by [`PgStore::create_api_key`] or
/// [`PgStore::rotate_api_key`]. `api_key` is the only copy of the key: the
/// database holds its hash.
#[derive(Debug, Clone)]
pub struct IssuedApiKey {
    pub api_key: String,
    pub record: ApiKeyRecord,
}

/// The outcome of [`PgStore::rotate_api_key`].
#[derive(Debug, Clone)]
pub struct RotatedApiKey {
    pub new: IssuedApiKey,
    /// When the replaced key stops working (unix seconds).
    pub old_expires_at: i64,
}

const INSERT: &str = "INSERT INTO kosha.api_keys \
        (api_key, key_hash, tenant_id, description, scopes, namespaces, expires_at) \
     VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7::BIGINT::DOUBLE PRECISION)) \
     RETURNING EXTRACT(EPOCH FROM created_at)::BIGINT";

/// How many characters of a key's id (its hash) an audit row carries:
/// enough to pick the key out of the key listing, not the whole id.
const AUDIT_KEY_ID_PREFIX_LEN: usize = 12;

fn key_id_prefix(key_hash: &str) -> &str {
    key_hash.get(..AUDIT_KEY_ID_PREFIX_LEN).unwrap_or(key_hash)
}

/// The audit detail for a key that was just issued: what the key may do
/// and enough to tell it apart, never the key or its whole hash.
fn issued_detail(issued: &IssuedApiKey) -> serde_json::Value {
    let tail = issued.api_key.len().saturating_sub(4);
    serde_json::json!({
        "key_suffix": issued.api_key.get(tail..).unwrap_or(""),
        "key_id_prefix": key_id_prefix(&issued.record.key_hash),
        "description": issued.record.description,
        "scopes": issued.record.scopes.names(),
        "namespaces": issued.record.namespaces,
        "expires_at": issued.record.expires_at,
    })
}

/// Append an operator action on one of `tenant_id`'s keys to the audit
/// log, under the tenant's organization when it has one (as
/// `orgs::create_api_key` does for the console's keys). Run it on the
/// transaction that makes the change, so neither exists without the other.
async fn audit<'e, E>(
    executor: E,
    action: &str,
    tenant_id: &str,
    detail: &serde_json::Value,
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(
        "INSERT INTO kosha.audit_log (org_id, actor, action, target_type, target_id, detail) \
         VALUES ((SELECT org_id FROM kosha.tenants WHERE tenant_id = $1), \
                 'operator', $2, 'tenant', $1, $3)",
    )
    .bind(tenant_id)
    .bind(action)
    .bind(detail.to_string())
    .execute(executor)
    .await
    .map(|_| ())
}

async fn insert<'e, E>(executor: E, new: &NewApiKey) -> Result<IssuedApiKey, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    // A random UUID prefixed with "sk-" (secret key): 122 bits of entropy.
    let api_key = format!("sk-{}", Uuid::new_v4());
    let key_hash = hash_api_key(&api_key);
    let (created_at,): (i64,) = sqlx::query_as(INSERT)
        .bind(placeholder(&key_hash))
        .bind(&key_hash)
        .bind(&new.tenant_id)
        .bind(&new.description)
        .bind(new.scopes.to_column())
        .bind(&new.namespaces)
        .bind(new.expires_at)
        .fetch_one(executor)
        .await?;
    Ok(IssuedApiKey {
        api_key,
        record: ApiKeyRecord {
            key_hash,
            tenant_id: new.tenant_id.clone(),
            description: new.description.clone(),
            scopes: new.scopes,
            namespaces: new.namespaces.clone(),
            expires_at: new.expires_at,
            last_used_at: None,
            created_at,
        },
    })
}

impl PgStore {
    /// The tenant of a key that is neither revoked nor expired.
    pub fn validate_api_key(&self, api_key: &str) -> Option<String> {
        let (pool, rt) = self.pool_and_runtime();
        let sql = format!(
            "SELECT tenant_id FROM kosha.api_keys \
             WHERE {KEY_HASH} = $1 AND revoked_at IS NULL \
               AND (expires_at IS NULL OR expires_at > NOW())"
        );
        rt.block_on(async {
            let row: Option<(String,)> = sqlx::query_as(&sql)
                .bind(hash_api_key(api_key))
                .fetch_optional(pool)
                .await
                .ok()?;
            row.map(|(tenant,)| tenant)
        })
    }

    /// Create a key. Only its hash is stored; the returned `api_key` is the
    /// one chance to see the key. Audited as `api_key.created` by the
    /// operator, in the same transaction.
    pub fn create_api_key(&self, new: &NewApiKey) -> Result<IssuedApiKey, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        rt.block_on(async {
            let mut tx = pool.begin().await?;
            let issued = insert(&mut *tx, new).await?;
            let detail = issued_detail(&issued);
            audit(&mut *tx, "api_key.created", &new.tenant_id, &detail).await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(issued)
        })
        .map_err(db_error)
    }

    /// Every key that is not revoked (expired keys included, so an operator
    /// can see them), newest first, optionally for one tenant.
    pub fn list_api_keys(&self, tenant_id: Option<&str>) -> Result<Vec<ApiKeyRecord>, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        let sql = select(
            "revoked_at IS NULL AND ($1::TEXT IS NULL OR tenant_id = $1) \
             ORDER BY created_at DESC",
        );
        rt.block_on(async {
            let rows: Vec<Row> = sqlx::query_as(&sql)
                .bind(tenant_id)
                .fetch_all(pool)
                .await
                .map_err(db_error)?;
            Ok(rows.into_iter().map(record).collect())
        })
    }

    /// Revoke the key with this hash. `false` when there is no such active
    /// key. Servers stop accepting it on their next key refresh. Audited
    /// as `api_key.revoked` by the operator, in the same transaction.
    pub fn revoke_api_key(&self, key_hash: &str) -> Result<bool, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        let sql = format!(
            "UPDATE kosha.api_keys SET revoked_at = NOW() \
             WHERE {KEY_HASH} = $1 AND revoked_at IS NULL \
             RETURNING tenant_id, description"
        );
        rt.block_on(async {
            let mut tx = pool.begin().await?;
            let revoked: Option<(String, String)> = sqlx::query_as(&sql)
                .bind(key_hash)
                .fetch_optional(&mut *tx)
                .await?;
            let Some((tenant_id, description)) = revoked else {
                return Ok(false);
            };
            let detail = serde_json::json!({
                "key_id_prefix": key_id_prefix(key_hash),
                "description": description,
            });
            audit(&mut *tx, "api_key.revoked", &tenant_id, &detail).await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(true)
        })
        .map_err(db_error)
    }

    /// Replace a key without downtime: issue a new key with the same
    /// tenant, description, scopes and namespaces (expiring at
    /// `new_expires_at`, if given), and let the old key keep working for
    /// `overlap_secs` more seconds. The overlap never extends the old key:
    /// one that already expires sooner keeps its earlier expiry. `None`
    /// when there is no such key, or it is revoked or already expired.
    /// Audited as `api_key.rotated` by the operator, in the same
    /// transaction.
    pub fn rotate_api_key(
        &self,
        key_hash: &str,
        overlap_secs: u64,
        new_expires_at: Option<i64>,
    ) -> Result<Option<RotatedApiKey>, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        let find = format!(
            "{} FOR UPDATE",
            select(&format!(
                "{KEY_HASH} = $1 AND revoked_at IS NULL \
                 AND (expires_at IS NULL OR expires_at > NOW())"
            ))
        );
        let retire = format!(
            "UPDATE kosha.api_keys \
             SET expires_at = LEAST(COALESCE(expires_at, 'infinity'), \
                                    NOW() + make_interval(secs => $2::DOUBLE PRECISION)) \
             WHERE {KEY_HASH} = $1 \
             RETURNING EXTRACT(EPOCH FROM expires_at)::BIGINT"
        );
        rt.block_on(async {
            let mut tx = pool.begin().await.map_err(db_error)?;
            let old: Option<Row> = sqlx::query_as(&find)
                .bind(key_hash)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db_error)?;
            let Some(old) = old.map(record) else {
                return Ok(None);
            };
            let new = insert(
                &mut *tx,
                &NewApiKey {
                    tenant_id: old.tenant_id,
                    description: old.description,
                    scopes: old.scopes,
                    namespaces: old.namespaces,
                    expires_at: new_expires_at,
                },
            )
            .await
            .map_err(db_error)?;
            let (old_expires_at,): (i64,) = sqlx::query_as(&retire)
                .bind(key_hash)
                .bind(overlap_secs as f64)
                .fetch_one(&mut *tx)
                .await
                .map_err(db_error)?;
            let mut detail = issued_detail(&new);
            detail["replaces"] = serde_json::json!({
                "key_id_prefix": key_id_prefix(key_hash),
                "expires_at": old_expires_at,
            });
            audit(&mut *tx, "api_key.rotated", &new.record.tenant_id, &detail)
                .await
                .map_err(db_error)?;
            tx.commit().await.map_err(db_error)?;
            Ok(Some(RotatedApiKey {
                new,
                old_expires_at,
            }))
        })
    }

    /// Record when keys were last used: `(key_hash, unix seconds)` pairs.
    /// A row's `last_used_at` only moves forward.
    pub fn touch_api_keys(&self, used: &[(String, i64)]) -> Result<(), KoshaError> {
        if used.is_empty() {
            return Ok(());
        }
        let (pool, rt) = self.pool_and_runtime();
        let (hashes, times): (Vec<&str>, Vec<i64>) =
            used.iter().map(|(h, t)| (h.as_str(), *t)).unzip();
        let sql = format!(
            "UPDATE kosha.api_keys \
             SET last_used_at = to_timestamp(u.ts::DOUBLE PRECISION) \
             FROM UNNEST($1::TEXT[], $2::BIGINT[]) AS u(h, ts) \
             WHERE {KEY_HASH} = u.h \
               AND (last_used_at IS NULL OR last_used_at < to_timestamp(u.ts::DOUBLE PRECISION))"
        );
        rt.block_on(async {
            sqlx::query(&sql)
                .bind(hashes)
                .bind(times)
                .execute(pool)
                .await
                .map_err(db_error)?;
            Ok(())
        })
    }

    /// Replace every plaintext key still held in the `api_key` column
    /// (rows from before #346) with an unusable placeholder, keeping the
    /// hash. Returns how many rows held plaintext. The keys keep working
    /// on builds with #346; a build from before it can no longer
    /// authenticate them, so run this only once every pod is upgraded.
    pub fn scrub_plaintext_api_keys(&self) -> Result<u64, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        let sql = format!(
            "UPDATE kosha.api_keys \
             SET key_hash = {KEY_HASH}, api_key = ' sha256:' || {KEY_HASH} \
             WHERE api_key NOT LIKE ' sha256:%'"
        );
        rt.block_on(async {
            let done = sqlx::query(&sql).execute(pool).await.map_err(db_error)?;
            Ok(done.rows_affected())
        })
    }
}
