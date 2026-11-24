//! Writer lease and fencing token per namespace (#254), on Postgres.
//!
//! `kosha.writer_leases` (migration 003) holds one row per namespace: the
//! holder, its fencing token and when the lease expires. A writer takes
//! the lease before it publishes, renews it in the background, and
//! presents the token on every manifest CAS
//! ([`PgStore`](crate::PgStore)'s `compare_and_swap_manifest_fenced`,
//! which reads the row `FOR SHARE` in the CAS transaction).
//!
//! Like [`HydrationLeaseStore`](crate::HydrationLeaseStore), the server's
//! renewal thread uses its own small pool, [`PgWriterLeaseStore`], rather
//! than `PgStore` behind `AppState::controller`'s mutex: a renewal stuck on
//! a slow database must not serialize every manifest read and write in the
//! process behind it. `PgStore` implements the same `ControlStore` methods
//! with these queries for callers that only have the control store.

use std::time::Duration;

use kosha_core::{KoshaError, NamespaceId};

fn control_plane_error(e: sqlx::Error) -> KoshaError {
    KoshaError::ControlPlane(e.to_string())
}

/// Take the lease when there is no row, the row expired, or `holder`
/// already holds it. A takeover by a different holder sets the token to
/// the old one + 1; the same holder keeps its token. No row back means a
/// live lease belongs to someone else.
const ACQUIRE: &str =
    "INSERT INTO kosha.writer_leases AS l (namespace_id, holder, token, expires_at) \
     VALUES ($1, $2, 1, now() + ($3 * interval '1 second')) \
     ON CONFLICT (namespace_id) DO UPDATE \
         SET token = CASE WHEN l.holder = EXCLUDED.holder THEN l.token ELSE l.token + 1 END, \
             holder = EXCLUDED.holder, \
             expires_at = EXCLUDED.expires_at \
         WHERE l.holder = EXCLUDED.holder OR l.expires_at < now() \
     RETURNING token";

const RENEW: &str = "UPDATE kosha.writer_leases \
     SET expires_at = now() + ($4 * interval '1 second') \
     WHERE namespace_id = $1 AND holder = $2 AND token = $3";

const RELEASE: &str = "UPDATE kosha.writer_leases SET expires_at = now() \
     WHERE namespace_id = $1 AND holder = $2 AND token = $3";

pub(crate) async fn acquire(
    pool: &sqlx::PgPool,
    id: &NamespaceId,
    holder: &str,
    ttl: Duration,
) -> Result<Option<u64>, KoshaError> {
    let row: Option<(i64,)> = sqlx::query_as(ACQUIRE)
        .bind(&id.0)
        .bind(holder)
        .bind(ttl.as_secs_f64())
        .fetch_optional(pool)
        .await
        .map_err(control_plane_error)?;
    Ok(row.map(|(token,)| token as u64))
}

pub(crate) async fn renew(
    pool: &sqlx::PgPool,
    id: &NamespaceId,
    holder: &str,
    token: u64,
    ttl: Duration,
) -> Result<bool, KoshaError> {
    let done = sqlx::query(RENEW)
        .bind(&id.0)
        .bind(holder)
        .bind(token as i64)
        .bind(ttl.as_secs_f64())
        .execute(pool)
        .await
        .map_err(control_plane_error)?;
    Ok(done.rows_affected() == 1)
}

pub(crate) async fn release(
    pool: &sqlx::PgPool,
    id: &NamespaceId,
    holder: &str,
    token: u64,
) -> Result<(), KoshaError> {
    sqlx::query(RELEASE)
        .bind(&id.0)
        .bind(holder)
        .bind(token as i64)
        .execute(pool)
        .await
        .map_err(control_plane_error)?;
    Ok(())
}

/// The fencing check of a fenced manifest CAS, inside its transaction:
/// a conflict when a lease row exists for `id` under another token. The
/// `FOR SHARE` lock holds off a concurrent takeover (an `UPDATE` of the
/// row) until the CAS commits or rolls back.
pub(crate) async fn fencing_conflict(
    tx: &mut sqlx::PgConnection,
    id: &NamespaceId,
    token: u64,
) -> Result<Option<KoshaError>, sqlx::Error> {
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT token FROM kosha.writer_leases WHERE namespace_id = $1 FOR SHARE")
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await?;
    Ok(match row {
        Some((current,)) if current as u64 != token => Some(KoshaError::Fenced {
            namespace: id.clone(),
            token,
            current: current as u64,
        }),
        _ => None,
    })
}

pub(crate) async fn check_writable(pool: &sqlx::PgPool) -> Result<(), KoshaError> {
    tokio::time::timeout(Duration::from_millis(500), pool.acquire())
        .await
        .map_err(|_| KoshaError::ControlPlane("Postgres write check timed out".into()))?
        .map_err(control_plane_error)?;
    Ok(())
}

/// Writer-lease client with its own small pool (see the module doc).
pub struct PgWriterLeaseStore {
    pool: sqlx::PgPool,
    rt: tokio::runtime::Runtime,
}

impl PgWriterLeaseStore {
    /// Connect a dedicated pool. Assumes `kosha.writer_leases` exists:
    /// `PgStore::new` applies migration 003 at startup.
    pub fn new(database_url: &str) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| format!("failed to create tokio runtime: {e}"))?;
        let pool = rt
            .block_on(
                crate::postgres::writer_pool_options()
                    .max_connections(2)
                    .acquire_timeout(Duration::from_secs(5))
                    .connect(database_url),
            )
            .map_err(|e| format!("failed to connect to postgres: {e}"))?;
        Ok(Self { pool, rt })
    }

    /// Check the write endpoint without waiting behind the manifest store.
    pub fn check_writable(&self) -> Result<(), KoshaError> {
        self.rt.block_on(check_writable(&self.pool))
    }

    /// See `ControlStore::acquire_writer_lease`.
    pub fn acquire(
        &self,
        id: &NamespaceId,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<u64>, KoshaError> {
        self.rt.block_on(acquire(&self.pool, id, holder, ttl))
    }

    /// See `ControlStore::renew_writer_lease`.
    pub fn renew(
        &self,
        id: &NamespaceId,
        holder: &str,
        token: u64,
        ttl: Duration,
    ) -> Result<bool, KoshaError> {
        self.rt.block_on(renew(&self.pool, id, holder, token, ttl))
    }

    /// See `ControlStore::release_writer_lease`.
    pub fn release(&self, id: &NamespaceId, holder: &str, token: u64) -> Result<(), KoshaError> {
        self.rt.block_on(release(&self.pool, id, holder, token))
    }
}
