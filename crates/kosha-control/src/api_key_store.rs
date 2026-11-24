//! The Postgres API-key registry (`kosha.api_keys`, #346): keys are matched
//! and stored by SHA-256 hash, carry scopes, an optional namespace
//! allow-list and an optional expiry, and can be rotated with an overlap
//! window. Schema: `migrations/003_scoped_api_keys.sql`.
//!
//! The same table holds the operator admin credentials (#346): rows whose
//! `tenant_id` is `_operator/<name>` ([`OPERATOR_TENANT_PREFIX`]). They are
//! created, rotated and revoked by the same statements as tenant keys; the
//! tenant-key methods never see them, and the operator methods never see a
//! tenant key. Every change to one is written to `kosha.audit_log` in the
//! same transaction.
//!
//! Creating, rotating and revoking a tenant key each append a row to
//! `kosha.audit_log` (migration 006, #345) in the transaction that makes
//! the change, with the operator as the actor: these back the operator's
//! `/v1/admin/api-keys` routes. Keys created through the organization
//! console are audited by `orgs::create_api_key`.

use crate::api_keys::{
    hash_api_key, operator_name, operator_tenant_id, valid_operator_name, ApiKeyRecord, NewApiKey,
    Scopes, OPERATOR_TENANT_PREFIX,
};
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

/// Which rows of the registry a statement is about.
#[derive(Clone, Copy)]
enum Kind {
    /// Tenant keys: everything that is not an operator credential.
    Tenant,
    /// Operator admin credentials.
    Operator,
}

impl Kind {
    /// The SQL condition selecting this kind of row.
    fn condition(self) -> String {
        let is_operator = format!(
            "left(tenant_id, {}) = '{OPERATOR_TENANT_PREFIX}'",
            OPERATOR_TENANT_PREFIX.len()
        );
        match self {
            Kind::Tenant => format!("NOT ({is_operator})"),
            Kind::Operator => is_operator,
        }
    }
}

/// The operator an operator row belongs to, for its audit entry. A row this
/// build would not have issued is still audited, under whatever follows the
/// prefix.
fn audited_operator(tenant_id: &str) -> &str {
    operator_name(tenant_id)
        .or_else(|| tenant_id.strip_prefix(OPERATOR_TENANT_PREFIX))
        .unwrap_or(tenant_id)
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

/// Replace the active key `key_hash` of the given kind inside `tx`: see
/// [`PgStore::rotate_api_key`]. The caller commits.
async fn rotate(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    kind: Kind,
    key_hash: &str,
    overlap_secs: u64,
    new_expires_at: Option<i64>,
) -> Result<Option<RotatedApiKey>, sqlx::Error> {
    let find = format!(
        "{} FOR UPDATE",
        select(&format!(
            "{KEY_HASH} = $1 AND revoked_at IS NULL \
             AND (expires_at IS NULL OR expires_at > NOW()) AND {}",
            kind.condition()
        ))
    );
    let retire = format!(
        "UPDATE kosha.api_keys \
         SET expires_at = LEAST(COALESCE(expires_at, 'infinity'), \
                                NOW() + make_interval(secs => $2::DOUBLE PRECISION)) \
         WHERE {KEY_HASH} = $1 \
         RETURNING EXTRACT(EPOCH FROM expires_at)::BIGINT"
    );
    let old: Option<Row> = sqlx::query_as(&find)
        .bind(key_hash)
        .fetch_optional(&mut **tx)
        .await?;
    let Some(old) = old.map(record) else {
        return Ok(None);
    };
    let new = insert(
        &mut **tx,
        &NewApiKey {
            tenant_id: old.tenant_id,
            description: old.description,
            scopes: old.scopes,
            namespaces: old.namespaces,
            expires_at: new_expires_at,
        },
    )
    .await?;
    let (old_expires_at,): (i64,) = sqlx::query_as(&retire)
        .bind(key_hash)
        .bind(overlap_secs as f64)
        .fetch_one(&mut **tx)
        .await?;
    Ok(Some(RotatedApiKey {
        new,
        old_expires_at,
    }))
}

/// Serialise changes to one operator's credentials: create, rotate and
/// revoke all take this lock, so a revoke by name cannot run while a
/// rotation has inserted a replacement it would not yet see.
async fn lock_operator(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(tenant_id)
        .execute(&mut **tx)
        .await
        .map(|_| ())
}

/// Lock the operator that the credential `key_hash` belongs to; `false`
/// when no operator credential has that hash.
async fn lock_operator_of(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key_hash: &str,
) -> Result<bool, sqlx::Error> {
    let sql = format!(
        "SELECT tenant_id FROM kosha.api_keys WHERE {KEY_HASH} = $1 AND {}",
        Kind::Operator.condition()
    );
    let owner: Option<(String,)> = sqlx::query_as(&sql)
        .bind(key_hash)
        .fetch_optional(&mut **tx)
        .await?;
    match owner {
        Some((tenant_id,)) => lock_operator(tx, &tenant_id).await.map(|()| true),
        None => Ok(false),
    }
}

/// Append the audit row of a change to the operator `name`'s credentials.
async fn audit_operator(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    actor: &str,
    action: &str,
    name: &str,
    detail: serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(crate::orgs_pg::INSERT_AUDIT)
        .bind(None::<String>)
        .bind(actor)
        .bind(action)
        .bind("operator")
        .bind(name)
        .bind(detail.to_string())
        .execute(&mut **tx)
        .await
        .map(|_| ())
}

/// Which operator credentials [`PgStore::revoke_operator_credentials`]
/// revokes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorCredentials<'a> {
    /// The one credential with this key hash.
    KeyHash(&'a str),
    /// Every credential of this operator (offboarding).
    Operator(&'a str),
}

impl PgStore {
    /// The tenant of a tenant key that is neither revoked nor expired.
    pub fn validate_api_key(&self, api_key: &str) -> Option<String> {
        let (pool, rt) = self.pool_and_runtime();
        let sql = format!(
            "SELECT tenant_id FROM kosha.api_keys \
             WHERE {KEY_HASH} = $1 AND revoked_at IS NULL \
               AND (expires_at IS NULL OR expires_at > NOW()) AND {}",
            Kind::Tenant.condition()
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

    /// Every tenant key that is not revoked (expired keys included, so an
    /// operator can see them), newest first, optionally for one tenant.
    /// Operator credentials are not tenant keys and are never listed here.
    pub fn list_api_keys(&self, tenant_id: Option<&str>) -> Result<Vec<ApiKeyRecord>, KoshaError> {
        self.list_rows(&Kind::Tenant.condition(), tenant_id)
    }

    /// Every operator credential that is not revoked (expired ones
    /// included), newest first. A record's `tenant_id` is
    /// `_operator/<name>`: see [`operator_name`].
    pub fn list_operator_credentials(&self) -> Result<Vec<ApiKeyRecord>, KoshaError> {
        self.list_rows(&Kind::Operator.condition(), None)
    }

    /// Every row that is not revoked, tenant keys and operator credentials
    /// alike: what a server loads to authenticate requests.
    pub fn list_all_credentials(&self) -> Result<Vec<ApiKeyRecord>, KoshaError> {
        self.list_rows("TRUE", None)
    }

    /// The rows that are not revoked and match `kind` (a SQL condition),
    /// newest first, optionally for one `tenant_id`.
    fn list_rows(
        &self,
        kind: &str,
        tenant_id: Option<&str>,
    ) -> Result<Vec<ApiKeyRecord>, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        let sql = select(&format!(
            "revoked_at IS NULL AND ($1::TEXT IS NULL OR tenant_id = $1) AND {kind} \
             ORDER BY created_at DESC"
        ));
        rt.block_on(async {
            let rows: Vec<Row> = sqlx::query_as(&sql)
                .bind(tenant_id)
                .fetch_all(pool)
                .await
                .map_err(db_error)?;
            Ok(rows.into_iter().map(record).collect())
        })
    }

    /// Revoke the tenant key with this hash. `false` when there is no such
    /// active key. Servers stop accepting it on their next key refresh.
    /// Audited as `api_key.revoked` by the operator, in the same
    /// transaction.
    pub fn revoke_api_key(&self, key_hash: &str) -> Result<bool, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        let sql = format!(
            "UPDATE kosha.api_keys SET revoked_at = NOW() \
             WHERE {KEY_HASH} = $1 AND revoked_at IS NULL AND {} \
             RETURNING tenant_id, description",
            Kind::Tenant.condition()
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
        rt.block_on(async {
            let mut tx = pool.begin().await.map_err(db_error)?;
            let Some(rotated) = rotate(
                &mut tx,
                Kind::Tenant,
                key_hash,
                overlap_secs,
                new_expires_at,
            )
            .await
            .map_err(db_error)?
            else {
                return Ok(None);
            };
            let mut detail = issued_detail(&rotated.new);
            detail["replaces"] = serde_json::json!({
                "key_id_prefix": key_id_prefix(key_hash),
                "expires_at": rotated.old_expires_at,
            });
            let tenant_id = &rotated.new.record.tenant_id;
            audit(&mut *tx, "api_key.rotated", tenant_id, &detail)
                .await
                .map_err(db_error)?;
            tx.commit().await.map_err(db_error)?;
            Ok(Some(rotated))
        })
    }

    /// Issue a credential for the operator `name`, recording `actor` (an
    /// audit label such as `operator` or `operator:alice`) as who did it.
    /// `None` when that operator already has a credential that is neither
    /// revoked nor expired: a name identifies one operator, and a
    /// replacement is issued by rotating.
    pub fn create_operator_credential(
        &self,
        name: &str,
        description: &str,
        expires_at: Option<i64>,
        actor: &str,
    ) -> Result<Option<IssuedApiKey>, KoshaError> {
        if !valid_operator_name(name) {
            return Err(KoshaError::ControlPlane(format!(
                "invalid operator name '{name}'"
            )));
        }
        let tenant_id = operator_tenant_id(name);
        let (pool, rt) = self.pool_and_runtime();
        rt.block_on(async {
            let mut tx = pool.begin().await.map_err(db_error)?;
            // Two creates for one name must not both find it free.
            lock_operator(&mut tx, &tenant_id).await.map_err(db_error)?;
            let taken: Option<(i32,)> = sqlx::query_as(
                "SELECT 1 FROM kosha.api_keys \
                 WHERE tenant_id = $1 AND revoked_at IS NULL \
                   AND (expires_at IS NULL OR expires_at > NOW()) LIMIT 1",
            )
            .bind(&tenant_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?;
            if taken.is_some() {
                return Ok(None);
            }
            let issued = insert(
                &mut *tx,
                &NewApiKey {
                    tenant_id,
                    description: description.to_string(),
                    // An operator credential is not a tenant key: it holds
                    // no tenant scope, so a build that does not know the
                    // `_operator/` prefix grants it nothing.
                    scopes: Scopes::NONE,
                    namespaces: None,
                    expires_at,
                },
            )
            .await
            .map_err(db_error)?;
            audit_operator(
                &mut tx,
                actor,
                "operator_credential.created",
                name,
                serde_json::json!({
                    "key_id": issued.record.key_hash,
                    "description": description,
                    "expires_at": expires_at,
                }),
            )
            .await
            .map_err(db_error)?;
            tx.commit().await.map_err(db_error)?;
            Ok(Some(issued))
        })
    }

    /// Revoke operator credentials, recording `actor` as who did it.
    /// Returns the `(key hash, operator name)` of each credential revoked:
    /// empty when nothing active matched. Servers stop accepting them on
    /// their next key refresh.
    pub fn revoke_operator_credentials(
        &self,
        which: OperatorCredentials<'_>,
        actor: &str,
    ) -> Result<Vec<(String, String)>, KoshaError> {
        let (key_hash, tenant_id) = match which {
            OperatorCredentials::KeyHash(hash) => (Some(hash.to_string()), None),
            OperatorCredentials::Operator(name) => (None, Some(operator_tenant_id(name))),
        };
        let (pool, rt) = self.pool_and_runtime();
        let sql = format!(
            "UPDATE kosha.api_keys SET revoked_at = NOW() \
             WHERE revoked_at IS NULL AND {} \
               AND ({KEY_HASH} = $1::TEXT OR tenant_id = $2::TEXT) \
             RETURNING {KEY_HASH}, tenant_id",
            Kind::Operator.condition()
        );
        rt.block_on(async {
            let mut tx = pool.begin().await.map_err(db_error)?;
            // Taken before the UPDATE, whose snapshot then includes any
            // replacement a concurrent rotation committed.
            match (&key_hash, &tenant_id) {
                (Some(hash), _) => {
                    lock_operator_of(&mut tx, hash).await.map_err(db_error)?;
                }
                (None, Some(tenant_id)) => {
                    lock_operator(&mut tx, tenant_id).await.map_err(db_error)?;
                }
                (None, None) => {}
            }
            let rows: Vec<(String, String)> = sqlx::query_as(&sql)
                .bind(key_hash)
                .bind(tenant_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(db_error)?;
            let mut revoked = Vec::with_capacity(rows.len());
            for (hash, tenant_id) in rows {
                let name = audited_operator(&tenant_id).to_string();
                audit_operator(
                    &mut tx,
                    actor,
                    "operator_credential.revoked",
                    &name,
                    serde_json::json!({ "key_id": hash }),
                )
                .await
                .map_err(db_error)?;
                revoked.push((hash, name));
            }
            tx.commit().await.map_err(db_error)?;
            Ok(revoked)
        })
    }

    /// [`Self::rotate_api_key`] for an operator credential: the replacement
    /// belongs to the same operator, and `actor` is recorded as who rotated
    /// it. `None` when there is no such active operator credential.
    pub fn rotate_operator_credential(
        &self,
        key_hash: &str,
        overlap_secs: u64,
        new_expires_at: Option<i64>,
        actor: &str,
    ) -> Result<Option<RotatedApiKey>, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        rt.block_on(async {
            let mut tx = pool.begin().await.map_err(db_error)?;
            if !lock_operator_of(&mut tx, key_hash)
                .await
                .map_err(db_error)?
            {
                return Ok(None);
            }
            let Some(rotated) = rotate(
                &mut tx,
                Kind::Operator,
                key_hash,
                overlap_secs,
                new_expires_at,
            )
            .await
            .map_err(db_error)?
            else {
                return Ok(None);
            };
            audit_operator(
                &mut tx,
                actor,
                "operator_credential.rotated",
                audited_operator(&rotated.new.record.tenant_id),
                serde_json::json!({
                    "key_id": rotated.new.record.key_hash,
                    "expires_at": new_expires_at,
                    "replaces": { "key_id": key_hash, "expires_at": rotated.old_expires_at },
                }),
            )
            .await
            .map_err(db_error)?;
            tx.commit().await.map_err(db_error)?;
            Ok(Some(rotated))
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

    /// How many rows still hold a plaintext key in the `api_key` column:
    /// exactly the rows [`Self::scrub_plaintext_api_keys`] would rewrite.
    /// Read-only.
    pub fn count_plaintext_api_keys(&self) -> Result<u64, KoshaError> {
        let (pool, rt) = self.pool_and_runtime();
        rt.block_on(async {
            let (rows,): (i64,) = sqlx::query_as(
                "SELECT count(*) FROM kosha.api_keys WHERE api_key NOT LIKE ' sha256:%'",
            )
            .fetch_one(pool)
            .await
            .map_err(db_error)?;
            Ok(rows.max(0) as u64)
        })
    }
}
