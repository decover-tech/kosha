//! Postgres-backed namespace registry and manifest store.
//!
//! Activated by setting the `DATABASE_URL` environment variable (along with the
//! `postgres` feature).  Falls back to the in-memory `Controller` otherwise.
//!
//! Tables live in a `kosha` schema — see `migrations/001_create_kosha_tables.sql`.

use kosha_core::{ControlStore, KoshaError, Manifest, NamespaceId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Postgres-backed store.
///
/// Uses a synchronous wrapper around `sqlx`'s async pool so it can slot into
/// the current single-threaded server without a runtime refactor.
pub struct PgStore {
    pool: sqlx::PgPool,
}

/// JSON-serialisable segment entry stored inside the manifest text column.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentEntry {
    segment_id: String,
    doc_count: u32,
}

impl PgStore {
    /// Create a new Postgres-backed store.
    ///
    /// Runs the schema migration on construction.
    pub fn new(database_url: &str) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to create tokio runtime: {e}"))?;

        let pool = rt
            .block_on(sqlx::PgPool::connect(database_url))
            .map_err(|e| format!("failed to connect to postgres: {e}"))?;

        // Run migration inline (no external migrator dependency).
        let migration = include_str!("../migrations/001_create_kosha_tables.sql");
        rt.block_on(sqlx::raw_sql(migration).execute(&pool))
            .map_err(|e| format!("migration failed: {e}"))?;

        Ok(Self { pool })
    }

    /// Validate an API key against the database.
    /// Returns the tenant_id if the key is valid and not revoked.
    pub fn validate_api_key(&self, api_key: &str) -> Option<String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;

        let key = api_key.to_string();
        rt.block_on(async move {
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
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KoshaError::NotFound(e.to_string()))?;

        let api_key = format!("sk-{}", Uuid::new_v4());
        let tid = tenant_id.to_string();
        let desc = description.to_string();
        let key = api_key.clone();

        let result: Result<(), KoshaError> = rt.block_on(async move {
            sqlx::query(
                "INSERT INTO kosha.api_keys (api_key, tenant_id, description) \
                 VALUES ($1, $2, $3)",
            )
            .bind(&key)
            .bind(&tid)
            .bind(&desc)
            .execute(&self.pool)
            .await
            .map_err(|e| KoshaError::NotFound(e.to_string()))?;

            Ok(())
        });

        result?;

        Ok(api_key)
    }

    /// List all active (non-revoked) API keys, optionally filtered by tenant.
    pub fn list_api_keys(
        &self,
        tenant_id: Option<&str>,
    ) -> Result<Vec<(String, String, String)>, KoshaError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KoshaError::NotFound(e.to_string()))?;

        let tid = tenant_id.map(|s| s.to_string());

        rt.block_on(async move {
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
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KoshaError::NotFound(e.to_string()))?;

        let id_str = id.0.clone();
        rt.block_on(async move {
            let result =
                sqlx::query("INSERT INTO kosha.namespaces (id) VALUES ($1) ON CONFLICT DO NOTHING")
                    .bind(&id_str)
                    .execute(&self.pool)
                    .await;

            match result {
                Ok(r) if r.rows_affected() == 0 => {
                    Err(KoshaError::NamespaceNotFound(NamespaceId(id_str)))
                }
                Ok(_) => {
                    // Also ensure a manifest row exists.
                    let _ = sqlx::query(
                        "INSERT INTO kosha.manifests (namespace_id, version, segments_json) \
                         VALUES ($1, 0, '[]') ON CONFLICT DO NOTHING",
                    )
                    .bind(&id_str)
                    .execute(&self.pool)
                    .await;
                    Ok(())
                }
                Err(e) => Err(KoshaError::NotFound(e.to_string())),
            }
        })
    }

    fn ensure_namespace(&mut self, id: NamespaceId) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let id_str = id.0.clone();
        rt.block_on(async move {
            let _ =
                sqlx::query("INSERT INTO kosha.namespaces (id) VALUES ($1) ON CONFLICT DO NOTHING")
                    .bind(&id_str)
                    .execute(&self.pool)
                    .await;

            let _ = sqlx::query(
                "INSERT INTO kosha.manifests (namespace_id, version, segments_json) \
                 VALUES ($1, 0, '[]') ON CONFLICT DO NOTHING",
            )
            .bind(&id_str)
            .execute(&self.pool)
            .await;
        });
    }

    fn has_namespace(&self, id: &NamespaceId) -> bool {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let id_str = id.0.clone();
        rt.block_on(async move {
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
        // Simplified: returns None for live queries — the server uses
        // `manifest_cloned()` which calls into `self.manifest()` on the
        // Indexer, not the control store directly.
        //
        // For a full implementation, the PgStore would need an internal
        // cache or return owned values.  Keeping this as a placeholder
        // for now — the Indexer's in-memory manifest is the source of
        // truth for query path until Phase 2.
        None
    }

    fn manifest_mut(&mut self, _id: &NamespaceId) -> Option<&mut Manifest> {
        // Same simplification as `manifest()` above.
        None
    }

    fn compare_and_swap_manifest(
        &mut self,
        id: &NamespaceId,
        expected_version: u64,
        new_manifest: Manifest,
    ) -> Result<(), KoshaError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| KoshaError::NotFound(e.to_string()))?;

        let id_str = id.0.clone();
        let segments_json = serde_json::to_string(
            &new_manifest
                .segments
                .iter()
                .map(|s| SegmentEntry {
                    segment_id: s.segment_id.0.clone(),
                    doc_count: s.doc_count,
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|e| KoshaError::NotFound(e.to_string()))?;

        rt.block_on(async move {
            let result = sqlx::query(
                "UPDATE kosha.manifests \
                 SET version = $1, segments_json = $2, updated_at = NOW() \
                 WHERE namespace_id = $3 AND version = $4",
            )
            .bind(new_manifest.version as i64)
            .bind(&segments_json)
            .bind(&id_str)
            .bind(expected_version as i64)
            .execute(&self.pool)
            .await;

            match result {
                Ok(r) if r.rows_affected() == 0 => Err(KoshaError::NotFound(format!(
                    "manifest CAS failed for {id_str}: version mismatch or namespace not found"
                ))),
                Ok(_) => Ok(()),
                Err(e) => Err(KoshaError::NotFound(e.to_string())),
            }
        })
    }

    fn list_namespaces(&self) -> Vec<NamespaceId> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let rows: Vec<(String,)> =
                sqlx::query_as("SELECT id FROM kosha.namespaces ORDER BY created_at DESC")
                    .fetch_all(&self.pool)
                    .await
                    .unwrap_or_default();

            rows.into_iter().map(|(id,)| NamespaceId(id)).collect()
        })
    }

    fn namespace_count(&self) -> usize {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM kosha.namespaces")
                .fetch_one(&self.pool)
                .await
                .unwrap_or((0,));

            row.0 as usize
        })
    }
}
