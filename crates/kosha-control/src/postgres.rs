//! Postgres-backed namespace registry and manifest store.
//!
//! Activated by setting the `DATABASE_URL` environment variable (along with the
//! `postgres` feature).  Falls back to the in-memory `Controller` otherwise.
//!
//! Tables live in a `kosha` schema — see `migrations/001_create_kosha_tables.sql`.

use kosha_core::{
    ControlStore, Footer, KoshaError, Manifest, ManifestEntry, NamespaceId, SegmentId,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// Postgres-backed store.
///
/// Wraps an async `sqlx` pool behind a long-lived multi-thread Tokio runtime.
/// Creating a fresh current-thread runtime per call is unsafe with sqlx: the
/// pool's connections are tied to the runtime that created them, and dropping
/// that runtime causes subsequent acquires to hang until `acquire_timeout`
/// ("pool timed out while waiting for an open connection").
pub struct PgStore {
    pool: sqlx::PgPool,
    rt: tokio::runtime::Runtime,
}

/// JSON-serialisable segment entry stored inside the manifest text column.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentEntry {
    segment_id: String,
    doc_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    footer: Option<Footer>,
}

fn control_plane_error(e: sqlx::Error) -> KoshaError {
    KoshaError::ControlPlane(e.to_string())
}

fn decode_manifest(version: u64, segments_json: &str) -> Option<Manifest> {
    if let Ok(mut manifest) = serde_json::from_str::<Manifest>(segments_json) {
        manifest.version = version;
        return Some(manifest);
    }
    let entries: Vec<SegmentEntry> = serde_json::from_str(segments_json).ok()?;
    let mut manifest = Manifest {
        version,
        segments: entries
            .iter()
            .map(|e| ManifestEntry {
                segment_id: SegmentId(e.segment_id.clone()),
                doc_count: e.doc_count,
            })
            .collect(),
        segment_footers: Default::default(),
        footer_crc32: Default::default(),
        wal_checkpoint: 0,
        wal_origin: None,
        tombstones: Default::default(),
        deletion_vectors: Default::default(),
        branch: None,
        schema: None,
        retired: Default::default(),
        shards: None,
    };
    for entry in entries {
        if let Some(footer) = entry.footer {
            manifest.remember_segment_footer(footer);
        }
    }
    Some(manifest)
}

/// Insert a namespace's empty manifest row at the version its next
/// incarnation starts at: 0, or one past the version a deleted incarnation
/// retired at (`kosha.retired_namespaces`). Never replaces a row. `$1` is
/// the namespace id.
const INSERT_INITIAL_MANIFEST: &str = "INSERT INTO kosha.manifests (namespace_id, version, segments_json) \
     VALUES ($1, COALESCE((SELECT last_version + 1 FROM kosha.retired_namespaces WHERE id = $1), 0), '[]') \
     ON CONFLICT DO NOTHING";

fn empty_manifest(version: u64) -> Manifest {
    Manifest::empty(version)
}

/// Key of the transaction-scoped advisory lock that serializes the inline
/// schema migration (#330): the ASCII bytes of "kosha_mg" as a big-endian
/// i64. Any fixed value works as long as every migrator uses the same one.
const MIGRATION_LOCK_KEY: i64 = i64::from_be_bytes(*b"kosha_mg");

/// SQLSTATEs a concurrent `CREATE ... IF NOT EXISTS` can still raise when
/// the other migrator does not take `MIGRATION_LOCK_KEY` (an older binary
/// mid-rollout, or `make db-migrate` run by hand through psql): Postgres
/// checks "if not exists" before inserting the catalog row, so the loser of
/// that race gets a unique violation on a catalog index (23505) or a
/// duplicate schema/table (42P06/42P07) instead of a no-op.
const MIGRATION_RACE_SQLSTATES: [&str; 3] = ["23505", "42P06", "42P07"];

fn is_migration_race(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| MIGRATION_RACE_SQLSTATES.contains(&code.as_ref()))
}

/// Apply the embedded migrations (001, then 003) in one transaction on one
/// connection, holding a transaction-scoped advisory lock for its duration.
///
/// Concurrent constructors against a fresh database (several pods booting
/// at once, or the API-key loader and the manifest store in one pod) used
/// to race inside `CREATE SCHEMA IF NOT EXISTS` and fail with a duplicate
/// key on `pg_namespace` (#330). With the lock they queue up: the first
/// creates everything and commits (which releases the lock), the rest find
/// it all present and their statements are no-ops. Every statement in
/// 001/003 is valid inside a transaction (no `CREATE INDEX CONCURRENTLY`,
/// no `VACUUM`), and running both in one transaction also means a failure
/// leaves no half-applied schema behind.
///
/// A migrator that does not take the lock can still race with us, so a
/// failure with one of `MIGRATION_RACE_SQLSTATES` is retried once: by then
/// the other side has committed and the retry is all no-ops.
async fn run_migrations(pool: &sqlx::PgPool) -> Result<(), String> {
    match migrate_once(pool).await {
        Err((e, _)) if is_migration_race(&e) => migrate_once(pool).await,
        other => other,
    }
    .map_err(|(e, which)| format!("{which}: {e}"))
}

/// One locked migration attempt. The error carries the message prefix the
/// caller reports: "migration failed" for 001 and the lock/transaction
/// plumbing, "migration 003 failed" for 003.
async fn migrate_once(pool: &sqlx::PgPool) -> Result<(), (sqlx::Error, &'static str)> {
    const FAILED: &str = "migration failed";
    let mut tx = pool.begin().await.map_err(|e| (e, FAILED))?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut *tx)
        .await
        .map_err(|e| (e, FAILED))?;
    let migration = include_str!("../migrations/001_create_kosha_tables.sql");
    sqlx::raw_sql(migration)
        .execute(&mut *tx)
        .await
        .map_err(|e| (e, FAILED))?;
    // Writer leases (#254): additive, idempotent, same pattern as 001.
    let migration = include_str!("../migrations/003_writer_leases.sql");
    sqlx::raw_sql(migration)
        .execute(&mut *tx)
        .await
        .map_err(|e| (e, "migration 003 failed"))?;
    // Usage events (#348): additive, idempotent, same pattern as 001.
    let migration = include_str!("../migrations/006_usage_events.sql");
    sqlx::raw_sql(migration)
        .execute(&mut *tx)
        .await
        .map_err(|e| (e, "migration 006 failed"))?;
    tx.commit().await.map_err(|e| (e, FAILED))
}

impl PgStore {
    /// Create a new Postgres-backed store.
    ///
    /// Runs the schema migration on construction.
    pub fn new(database_url: &str) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| format!("failed to create tokio runtime: {e}"))?;

        let pool = rt
            .block_on(
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(5)
                    .acquire_timeout(Duration::from_secs(10))
                    .connect(database_url),
            )
            .map_err(|e| format!("failed to connect to postgres: {e}"))?;

        // Run migrations inline (no external migrator dependency), serialized
        // across processes by an advisory lock (#330).
        rt.block_on(run_migrations(&pool))?;

        Ok(Self { pool, rt })
    }

    /// Bounded attempts for the initial connect, mirroring
    /// `s3_storage::FETCH_MAX_ATTEMPTS`'s reasoning: a pod's very first
    /// connect frequently races pod-startup conditions that resolve within
    /// seconds (CNI not yet routable, Postgres briefly out of free
    /// connections during a rolling restart, DNS not yet warm) — a single
    /// attempt turns any of those into a permanent, silent failure (see
    /// `connect_with_retry`'s doc comment for the incident this fixes).
    const CONNECT_MAX_ATTEMPTS: u32 = 5;

    /// The backoff delay before each retry (not the initial attempt) — a
    /// plain `Vec` rather than inlined into `connect_with_retry`'s loop so
    /// the schedule itself has a direct, network-free unit test. Starts at
    /// 500ms, doubles, capped at 5s so a raised `CONNECT_MAX_ATTEMPTS`
    /// later can't silently balloon total startup wait.
    fn connect_retry_backoff_schedule() -> Vec<Duration> {
        let mut delay = Duration::from_millis(500);
        let cap = Duration::from_secs(5);
        let mut schedule = Vec::new();
        for _ in 1..Self::CONNECT_MAX_ATTEMPTS {
            schedule.push(delay);
            delay = (delay * 2).min(cap);
        }
        schedule
    }

    /// `new`, retried with backoff before giving up.
    ///
    /// Fixes a real incident: a single failed connect attempt at boot
    /// (`pool timed out while waiting for an open connection`, most likely
    /// pod-startup network-readiness timing, not Postgres actually being
    /// down) used to be indistinguishable from "Postgres doesn't exist" —
    /// the caller fell back to an empty in-memory control plane
    /// permanently for that pod's lifetime, with `/healthz`/`/readyz`
    /// still reporting healthy, so Kubernetes kept routing real traffic to
    /// a pod that silently had zero namespaces. `CONNECT_MAX_ATTEMPTS`
    /// attempts with the schedule above (~500ms+1s+2s+4s ≈ 7.5s of backoff,
    /// plus each attempt's own `acquire_timeout`) absorbs exactly that
    /// class of transient blip before the caller has to decide what to do
    /// about a *genuinely* unreachable Postgres (see
    /// `main.rs`'s control-plane init, which now treats that as fatal
    /// rather than silently degrading).
    pub fn connect_with_retry(database_url: &str) -> Result<Self, String> {
        let mut backoffs = Self::connect_retry_backoff_schedule().into_iter();
        let mut last_err = String::new();
        for attempt in 1..=Self::CONNECT_MAX_ATTEMPTS {
            match Self::new(database_url) {
                Ok(store) => return Ok(store),
                Err(e) => {
                    last_err = e;
                    if let Some(delay) = backoffs.next() {
                        eprintln!(
                            "WARN: postgres connect failed (attempt {attempt}/{}): {last_err}; \
                             retrying in {delay:?}",
                            Self::CONNECT_MAX_ATTEMPTS
                        );
                        std::thread::sleep(delay);
                    }
                }
            }
        }
        Err(last_err)
    }

    /// Validate an API key against the database.
    /// Returns the tenant_id if the key is valid and not revoked.
    pub fn validate_api_key(&self, api_key: &str) -> Option<String> {
        let key = api_key.to_string();
        self.rt.block_on(async {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT tenant_id FROM kosha.api_keys \
                 WHERE api_key = $1 AND revoked_at IS NULL",
            )
            .bind(&key)
            .fetch_optional(&self.pool)
            .await
            .ok()?;

            row.map(|(tenant,)| tenant)
        })
    }

    /// Create a new API key for a tenant.
    /// The key is a random UUID prefixed with "sk-" (secret key).
    pub fn create_api_key(&self, tenant_id: &str, description: &str) -> Result<String, KoshaError> {
        let api_key = format!("sk-{}", Uuid::new_v4());
        let tid = tenant_id.to_string();
        let desc = description.to_string();
        let key = api_key.clone();

        self.rt.block_on(async {
            sqlx::query(
                "INSERT INTO kosha.api_keys (api_key, tenant_id, description) \
                 VALUES ($1, $2, $3)",
            )
            .bind(&key)
            .bind(&tid)
            .bind(&desc)
            .execute(&self.pool)
            .await
            .map_err(|e| KoshaError::NotFound(e.to_string()))
        })?;

        Ok(api_key)
    }

    /// List all active (non-revoked) API keys, optionally filtered by tenant.
    pub fn list_api_keys(
        &self,
        tenant_id: Option<&str>,
    ) -> Result<Vec<(String, String, String)>, KoshaError> {
        let tid = tenant_id.map(|s| s.to_string());

        self.rt.block_on(async {
            let query = if tid.is_some() {
                "SELECT api_key, tenant_id, description FROM kosha.api_keys \
                 WHERE revoked_at IS NULL AND tenant_id = $1 \
                 ORDER BY created_at DESC"
            } else {
                "SELECT api_key, tenant_id, description FROM kosha.api_keys \
                 WHERE revoked_at IS NULL \
                 ORDER BY created_at DESC"
            };

            let rows: Vec<(String, String, String)> = sqlx::query_as(query)
                .bind(&tid)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| KoshaError::NotFound(e.to_string()))?;

            Ok(rows)
        })
    }
}

impl ControlStore for PgStore {
    fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError> {
        let id_str = id.0.clone();
        self.rt.block_on(async {
            let io_error = |e: sqlx::Error| KoshaError::NotFound(e.to_string());
            let mut tx = self.pool.begin().await.map_err(io_error)?;
            let inserted =
                sqlx::query("INSERT INTO kosha.namespaces (id) VALUES ($1) ON CONFLICT DO NOTHING")
                    .bind(&id_str)
                    .execute(&mut *tx)
                    .await
                    .map_err(io_error)?;
            if inserted.rows_affected() == 0 {
                return Err(KoshaError::NamespaceExists(NamespaceId(id_str)));
            }
            sqlx::query(INSERT_INITIAL_MANIFEST)
                .bind(&id_str)
                .execute(&mut *tx)
                .await
                .map_err(io_error)?;
            tx.commit().await.map_err(io_error)?;
            Ok(())
        })
    }

    fn ensure_namespace(&mut self, id: NamespaceId) {
        let id_str = id.0.clone();
        self.rt.block_on(async {
            let _ =
                sqlx::query("INSERT INTO kosha.namespaces (id) VALUES ($1) ON CONFLICT DO NOTHING")
                    .bind(&id_str)
                    .execute(&self.pool)
                    .await;

            let _ = sqlx::query(INSERT_INITIAL_MANIFEST)
                .bind(&id_str)
                .execute(&self.pool)
                .await;
        });
    }

    fn has_namespace(&self, id: &NamespaceId) -> bool {
        let id_str = id.0.clone();
        self.rt.block_on(async {
            let row: Option<(i64,)> =
                sqlx::query_as("SELECT COUNT(*) FROM kosha.namespaces WHERE id = $1")
                    .bind(&id_str)
                    .fetch_optional(&self.pool)
                    .await
                    .unwrap_or(None);

            matches!(row, Some((count,)) if count > 0)
        })
    }

    fn manifest(&self, _id: &NamespaceId) -> Option<&Manifest> {
        // Cannot return a borrowed reference from a database query — use
        // `manifest_cloned()` below, which is the read path callers take.
        None
    }

    fn manifest_mut(&mut self, _id: &NamespaceId) -> Option<&mut Manifest> {
        // Same limitation as `manifest()` above.
        None
    }

    /// One integer off the primary key, not the `segments_json` text.
    /// Query errors surface as `KoshaError::ControlPlane`, never as a
    /// missing namespace (#232).
    fn manifest_version(&self, id: &NamespaceId) -> Result<Option<u64>, KoshaError> {
        let id_str = id.0.clone();
        self.rt.block_on(async {
            let row: Option<(i64,)> =
                sqlx::query_as("SELECT version FROM kosha.manifests WHERE namespace_id = $1")
                    .bind(&id_str)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(control_plane_error)?;
            Ok(row.map(|(version,)| version as u64))
        })
    }

    /// The query replicas' refresher poll: one small query for every
    /// namespace, independent of how many namespaces the pod serves.
    fn manifest_versions(&self) -> Result<Vec<(NamespaceId, u64)>, KoshaError> {
        self.rt.block_on(async {
            let rows: Vec<(String, i64)> =
                sqlx::query_as("SELECT namespace_id, version FROM kosha.manifests")
                    .fetch_all(&self.pool)
                    .await
                    .map_err(control_plane_error)?;
            Ok(rows
                .into_iter()
                .map(|(id, version)| (NamespaceId(id), version as u64))
                .collect())
        })
    }

    fn manifest_cloned(&self, id: &NamespaceId) -> Option<Manifest> {
        self.try_manifest_cloned(id).ok().flatten()
    }

    fn try_manifest_cloned(&self, id: &NamespaceId) -> Result<Option<Manifest>, KoshaError> {
        let id_str = id.0.clone();
        self.rt.block_on(async {
            let row: Option<(i64, String)> = sqlx::query_as(
                "SELECT version, segments_json FROM kosha.manifests WHERE namespace_id = $1",
            )
            .bind(&id_str)
            .fetch_optional(&self.pool)
            .await
            .map_err(control_plane_error)?;

            let Some((version, segments_json)) = row else {
                return Ok(None);
            };
            decode_manifest(version as u64, &segments_json)
                .map(Some)
                .ok_or_else(|| {
                    KoshaError::ControlPlane(format!(
                        "manifest row for '{id_str}' (version {version}) is not decodable"
                    ))
                })
        })
    }

    fn save_manifest(&mut self, id: &NamespaceId, manifest: &Manifest) -> Result<(), KoshaError> {
        let id_str = id.0.clone();
        let version = manifest.version as i64;
        let segments_json = serde_json::to_string(&manifest)?;

        self.rt.block_on(async {
            // kosha.manifests has a FK to kosha.namespaces — ensure the
            // namespace row exists first.
            sqlx::query("INSERT INTO kosha.namespaces (id) VALUES ($1) ON CONFLICT DO NOTHING")
                .bind(&id_str)
                .execute(&self.pool)
                .await
                .map_err(|e| KoshaError::NotFound(e.to_string()))?;

            sqlx::query(
                "INSERT INTO kosha.manifests (namespace_id, version, segments_json) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (namespace_id) DO UPDATE \
                 SET version = $2, segments_json = $3, updated_at = NOW()",
            )
            .bind(&id_str)
            .bind(version)
            .bind(&segments_json)
            .execute(&self.pool)
            .await
            .map_err(|e| KoshaError::NotFound(e.to_string()))?;

            Ok(())
        })
    }

    /// Both rows go in one transaction (the manifest row first: it holds
    /// the foreign key), so a reader never sees a manifest without its
    /// namespace or the reverse.
    fn remove_namespace(&mut self, id: &NamespaceId) -> Result<Option<Manifest>, KoshaError> {
        let id_str = id.0.clone();
        self.rt.block_on(async {
            let mut tx = self.pool.begin().await.map_err(control_plane_error)?;
            let row: Option<(i64, String)> = sqlx::query_as(
                "DELETE FROM kosha.manifests WHERE namespace_id = $1 \
                 RETURNING version, segments_json",
            )
            .bind(&id_str)
            .fetch_optional(&mut *tx)
            .await
            .map_err(control_plane_error)?;
            let deleted = sqlx::query("DELETE FROM kosha.namespaces WHERE id = $1")
                .bind(&id_str)
                .execute(&mut *tx)
                .await
                .map_err(control_plane_error)?;
            tx.commit().await.map_err(control_plane_error)?;
            if deleted.rows_affected() == 0 && row.is_none() {
                return Ok(None);
            }
            Ok(Some(
                row.and_then(|(version, json)| decode_manifest(version as u64, &json))
                    .unwrap_or_else(|| decode_manifest(0, "[]").expect("empty manifest")),
            ))
        })
    }

    fn compare_and_swap_manifest(
        &mut self,
        id: &NamespaceId,
        expected_version: u64,
        new_manifest: Manifest,
    ) -> Result<(), KoshaError> {
        self.compare_and_swap_manifest_fenced(id, expected_version, new_manifest, None)
    }

    fn acquire_writer_lease(
        &mut self,
        id: &NamespaceId,
        holder: &str,
        ttl: Duration,
    ) -> Result<Option<u64>, KoshaError> {
        self.rt
            .block_on(crate::writer_lease::acquire(&self.pool, id, holder, ttl))
    }

    fn renew_writer_lease(
        &mut self,
        id: &NamespaceId,
        holder: &str,
        token: u64,
        ttl: Duration,
    ) -> Result<bool, KoshaError> {
        self.rt.block_on(crate::writer_lease::renew(
            &self.pool, id, holder, token, ttl,
        ))
    }

    fn release_writer_lease(
        &mut self,
        id: &NamespaceId,
        holder: &str,
        token: u64,
    ) -> Result<(), KoshaError> {
        self.rt
            .block_on(crate::writer_lease::release(&self.pool, id, holder, token))
    }

    /// The plain CAS plus, with a token, the fencing check in the same
    /// transaction, before the `UPDATE` (see `writer_lease::check_fencing`).
    fn compare_and_swap_manifest_fenced(
        &mut self,
        id: &NamespaceId,
        expected_version: u64,
        new_manifest: Manifest,
        token: Option<u64>,
    ) -> Result<(), KoshaError> {
        let id_str = id.0.clone();
        let segments_json = serde_json::to_string(&new_manifest)?;

        self.rt.block_on(async {
            let io_error = |e: sqlx::Error| KoshaError::NotFound(e.to_string());
            let mut tx = self.pool.begin().await.map_err(io_error)?;
            if let Some(token) = token {
                // Dropping `tx` on the error path rolls it back.
                crate::writer_lease::check_fencing(&mut tx, id, token).await?;
            }
            // Bootstrap is transactional and never replaces an existing row.
            // A missing namespace is bootstrapped by a writer that knows
            // nothing (expected 0) or one that knows the version a deleted
            // and re-created namespace starts at (see `delete_namespace`).
            let retired: Option<(i64,)> =
                sqlx::query_as("SELECT last_version FROM kosha.retired_namespaces WHERE id = $1")
                    .bind(&id_str).fetch_optional(&mut *tx).await.map_err(io_error)?;
            let initial = kosha_core::initial_manifest_version(retired.map(|(v,)| v as u64));
            let bootstrap = expected_version == 0 || expected_version == initial;
            if bootstrap {
                sqlx::query("INSERT INTO kosha.namespaces (id) VALUES ($1) ON CONFLICT DO NOTHING")
                    .bind(&id_str).execute(&mut *tx).await.map_err(io_error)?;
                sqlx::query(INSERT_INITIAL_MANIFEST)
                    .bind(&id_str).execute(&mut *tx).await.map_err(io_error)?;
            }
            let result = sqlx::query(
                "UPDATE kosha.manifests SET version = $1, segments_json = $2, updated_at = NOW() \
                 WHERE namespace_id = $3 AND version = $4 AND version < $1"
            ).bind(new_manifest.version as i64).bind(&segments_json).bind(&id_str)
                .bind(expected_version as i64).execute(&mut *tx).await.map_err(io_error)?;
            if result.rows_affected() == 0 {
                // A retry after a lost reply succeeds only for the exact
                // snapshot already committed. Equal version alone is unsafe.
                let row: Option<(i64, String)> = sqlx::query_as(
                    "SELECT version, segments_json FROM kosha.manifests WHERE namespace_id = $1 FOR UPDATE"
                ).bind(&id_str).fetch_optional(&mut *tx).await.map_err(io_error)?;
                let same = row.as_ref().and_then(|(version, json)| decode_manifest(*version as u64, json))
                    .is_some_and(|current| current.same_contents(&new_manifest));
                if !same {
                    // Keep a bootstrap row: a writer with no version (0)
                    // that met a retired namespace rebases onto the empty
                    // manifest at its initial version, which must exist.
                    if bootstrap && row.is_some() {
                        tx.commit().await.map_err(io_error)?;
                    }
                    return Err(KoshaError::ManifestConflict {
                        namespace: id.clone(), expected: expected_version,
                        actual: row.map(|(version, _)| version as u64),
                    });
                }
            }
            tx.commit().await.map_err(io_error)?;
            Ok(())
        })
    }

    fn delete_namespace(
        &mut self,
        id: &NamespaceId,
        retire_at_least: u64,
    ) -> Result<(Manifest, u64), KoshaError> {
        let id_str = id.0.clone();
        self.rt.block_on(async {
            let io_error = |e: sqlx::Error| KoshaError::NotFound(e.to_string());
            let mut tx = self.pool.begin().await.map_err(io_error)?;
            // Lock the manifest row first so a concurrent CAS either lands
            // before the delete (and its segments are reclaimed with the
            // rest) or finds no row afterwards.
            let row: Option<(i64, String)> = sqlx::query_as(
                "SELECT version, segments_json FROM kosha.manifests \
                 WHERE namespace_id = $1 FOR UPDATE",
            )
            .bind(&id_str)
            .fetch_optional(&mut *tx)
            .await
            .map_err(io_error)?;
            sqlx::query("DELETE FROM kosha.manifests WHERE namespace_id = $1")
                .bind(&id_str)
                .execute(&mut *tx)
                .await
                .map_err(io_error)?;
            let deleted = sqlx::query("DELETE FROM kosha.namespaces WHERE id = $1")
                .bind(&id_str)
                .execute(&mut *tx)
                .await
                .map_err(io_error)?;
            if deleted.rows_affected() == 0 {
                // Rolled back on drop: nothing changes.
                return Err(KoshaError::NamespaceNotFound(id.clone()));
            }
            let manifest = match row {
                Some((version, json)) => {
                    decode_manifest(version as u64, &json).ok_or_else(|| {
                        KoshaError::NotFound(format!("undecodable manifest for {id_str}"))
                    })?
                }
                None => empty_manifest(0),
            };
            let floor = manifest.version.max(retire_at_least) as i64;
            let (retired,): (i64,) = sqlx::query_as(
                "INSERT INTO kosha.retired_namespaces AS r (id, last_version) VALUES ($1, $2) \
                 ON CONFLICT (id) DO UPDATE SET \
                 last_version = GREATEST(r.last_version, EXCLUDED.last_version), \
                 retired_at = NOW() \
                 RETURNING last_version",
            )
            .bind(&id_str)
            .bind(floor)
            .fetch_one(&mut *tx)
            .await
            .map_err(io_error)?;
            tx.commit().await.map_err(io_error)?;
            Ok((manifest, retired as u64))
        })
    }

    fn list_namespaces(&self) -> Vec<NamespaceId> {
        self.rt.block_on(async {
            let rows: Vec<(String,)> =
                sqlx::query_as("SELECT id FROM kosha.namespaces ORDER BY created_at DESC")
                    .fetch_all(&self.pool)
                    .await
                    .unwrap_or_default();

            rows.into_iter().map(|(id,)| NamespaceId(id)).collect()
        })
    }

    fn namespace_count(&self) -> usize {
        self.rt.block_on(async {
            let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kosha.namespaces")
                .fetch_one(&self.pool)
                .await
                .unwrap_or((0,));

            row.0 as usize
        })
    }
}

impl PgStore {
    /// The pool and the runtime that owns it, for sibling modules that
    /// run their own transactions (`namespace_rename`).
    pub(crate) fn pool_and_runtime(&self) -> (&sqlx::PgPool, &tokio::runtime::Runtime) {
        (&self.pool, &self.rt)
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    #[test]
    fn connect_retry_backoff_schedule_doubles_from_500ms_capped_at_5s() {
        let schedule = PgStore::connect_retry_backoff_schedule();
        // CONNECT_MAX_ATTEMPTS attempts => CONNECT_MAX_ATTEMPTS - 1 backoff delays.
        assert_eq!(schedule.len(), (PgStore::CONNECT_MAX_ATTEMPTS - 1) as usize);
        assert_eq!(schedule[0], Duration::from_millis(500));
        assert_eq!(schedule[1], Duration::from_secs(1));
        assert_eq!(schedule[2], Duration::from_secs(2));
        assert_eq!(schedule[3], Duration::from_secs(4));
    }

    #[test]
    fn connect_retry_backoff_schedule_never_exceeds_the_5s_cap() {
        // Guards against a future CONNECT_MAX_ATTEMPTS bump silently
        // ballooning total startup wait — the cap must hold regardless of
        // how many attempts are configured.
        for delay in PgStore::connect_retry_backoff_schedule() {
            assert!(
                delay <= Duration::from_secs(5),
                "delay {delay:?} exceeds the 5s cap"
            );
        }
    }

    // `connect_with_retry` itself (the loop against a real/unreachable
    // Postgres) is intentionally not unit-tested here — no network access
    // in this test binary, and DNS-resolution failure timing for an
    // unreachable host isn't hermetic (varies by environment, can hang
    // without egress). Covered instead by the schedule tests above (the
    // retry *shape* is what regresses silently) plus live validation
    // against a real pool-timeout on staging.
}

#[cfg(test)]
mod manifest_encoding_tests {
    use super::*;
    #[test]
    fn legacy_and_durable_manifests_round_trip() {
        let legacy = decode_manifest(7, r#"[{"segment_id":"s","doc_count":2}]"#).unwrap();
        assert_eq!(legacy.segments.len(), 1);
        assert_eq!(legacy.wal_checkpoint, 0);
        assert!(legacy.tombstones.is_empty());
        let mut durable = legacy;
        durable.wal_checkpoint = 42;
        durable
            .tombstones
            .insert(SegmentId("s".into()), std::collections::HashSet::from([1]));
        let encoded = serde_json::to_string(&durable).unwrap();
        let decoded = decode_manifest(8, &encoded).unwrap();
        assert_eq!(decoded.version, 8);
        assert_eq!(decoded.wal_checkpoint, 42);
        assert_eq!(decoded.tombstones, durable.tombstones);
    }
}
