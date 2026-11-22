//! Namespace erasure records (#347): the completion record of a hard delete
//! (`DELETE /v1/namespaces/{ns}`), kept after the namespace is gone so an
//! operator can show when an erasure was requested, when it finished and
//! whether storage was verified empty (GDPR / DPA erasure commitments).
//!
//! [`InMemoryErasureStore`] backs the in-memory control plane;
//! [`PgErasureStore`] keeps them in `kosha.namespace_erasures` (migration
//! 005), on its own small pool like the writer-lease store.

use std::sync::Mutex;

use kosha_core::KoshaError;
use serde::{Deserialize, Serialize};

/// One completed namespace hard delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasureRecord {
    /// `er_…`, returned as `erasure_id` by the delete.
    pub id: String,
    /// The namespace name as the tenant addressed it (no tenant prefix).
    pub namespace: String,
    pub tenant: String,
    /// Unix epoch milliseconds the delete request arrived.
    pub requested_at: u64,
    /// Unix epoch milliseconds the delete finished.
    pub completed_at: u64,
    /// Objects deleted under the namespace's S3 prefix (0 without S3).
    pub s3_objects_deleted: u64,
    /// A listing after the delete found nothing left under the namespace's
    /// prefixes (S3 and this node's local directory).
    pub verified_empty: bool,
    /// This node's local segment files and write-ahead log were removed.
    pub local_reclaimed: bool,
}

impl ErasureRecord {
    /// A fresh, unguessable-enough record id: the completion time and a
    /// per-process random component.
    pub fn new_id(now_ms: u64) -> String {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(now_ms);
        hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        hasher.write_u32(std::process::id());
        format!("er_{now_ms:x}{:016x}", hasher.finish())
    }
}

/// Where erasure records live. Records are append-only: a namespace
/// deleted twice has two.
pub trait ErasureStore: Send + Sync {
    fn record(&self, record: &ErasureRecord) -> Result<(), KoshaError>;
    /// The most recent record for `tenant`'s `namespace`.
    fn latest(&self, tenant: &str, namespace: &str) -> Result<Option<ErasureRecord>, KoshaError>;
    /// Records newest first, for one tenant or (`None`) all, at most `limit`.
    fn list(&self, tenant: Option<&str>, limit: usize) -> Result<Vec<ErasureRecord>, KoshaError>;
}

/// Erasure records in process memory (lost on restart, like the in-memory
/// control plane itself).
#[derive(Default)]
pub struct InMemoryErasureStore {
    records: Mutex<Vec<ErasureRecord>>,
}

impl InMemoryErasureStore {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Newest first: by completion time, then id for a stable order.
fn newest_first(a: &ErasureRecord, b: &ErasureRecord) -> std::cmp::Ordering {
    b.completed_at
        .cmp(&a.completed_at)
        .then_with(|| b.requested_at.cmp(&a.requested_at))
}

impl ErasureStore for InMemoryErasureStore {
    fn record(&self, record: &ErasureRecord) -> Result<(), KoshaError> {
        self.records.lock().unwrap().push(record.clone());
        Ok(())
    }

    fn latest(&self, tenant: &str, namespace: &str) -> Result<Option<ErasureRecord>, KoshaError> {
        let records = self.records.lock().unwrap();
        // Later pushes win ties: the last record pushed is the latest.
        Ok(records
            .iter()
            .rev()
            .filter(|r| r.tenant == tenant && r.namespace == namespace)
            .min_by(|a, b| newest_first(a, b))
            .cloned())
    }

    fn list(&self, tenant: Option<&str>, limit: usize) -> Result<Vec<ErasureRecord>, KoshaError> {
        let records = self.records.lock().unwrap();
        let mut out: Vec<ErasureRecord> = records
            .iter()
            .rev()
            .filter(|r| tenant.is_none_or(|t| r.tenant == t))
            .cloned()
            .collect();
        out.sort_by(newest_first);
        out.truncate(limit);
        Ok(out)
    }
}

#[cfg(feature = "postgres")]
pub use pg::PgErasureStore;

#[cfg(feature = "postgres")]
mod pg {
    use std::time::Duration;

    use kosha_core::KoshaError;

    use super::{ErasureRecord, ErasureStore};

    fn control_plane_error(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    const INSERT: &str = "INSERT INTO kosha.namespace_erasures \
         (id, namespace, tenant, requested_at, completed_at, s3_objects_deleted, \
          verified_empty, local_reclaimed) \
         VALUES ($1, $2, $3, to_timestamp($4::double precision / 1000.0), \
                 to_timestamp($5::double precision / 1000.0), $6, $7, $8)";

    const COLUMNS: &str = "id, namespace, tenant, \
         (extract(epoch FROM requested_at) * 1000)::bigint, \
         (extract(epoch FROM completed_at) * 1000)::bigint, \
         s3_objects_deleted, verified_empty, local_reclaimed";

    type Row = (String, String, String, i64, i64, i64, bool, bool);

    fn from_row(row: Row) -> ErasureRecord {
        let (id, namespace, tenant, requested_at, completed_at, objects, verified, local) = row;
        ErasureRecord {
            id,
            namespace,
            tenant,
            requested_at: requested_at.max(0) as u64,
            completed_at: completed_at.max(0) as u64,
            s3_objects_deleted: objects.max(0) as u64,
            verified_empty: verified,
            local_reclaimed: local,
        }
    }

    /// Erasure records in `kosha.namespace_erasures`, on a dedicated pool.
    pub struct PgErasureStore {
        pool: sqlx::PgPool,
        rt: tokio::runtime::Runtime,
    }

    impl PgErasureStore {
        /// Connect a dedicated pool. Assumes the table exists:
        /// `PgStore::new` applies migration 005 at startup.
        pub fn new(database_url: &str) -> Result<Self, String> {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|e| format!("failed to create tokio runtime: {e}"))?;
            let pool = rt
                .block_on(
                    sqlx::postgres::PgPoolOptions::new()
                        .max_connections(2)
                        .acquire_timeout(Duration::from_secs(5))
                        .connect(database_url),
                )
                .map_err(|e| format!("failed to connect to postgres: {e}"))?;
            Ok(Self { pool, rt })
        }
    }

    impl ErasureStore for PgErasureStore {
        fn record(&self, record: &ErasureRecord) -> Result<(), KoshaError> {
            self.rt.block_on(async {
                sqlx::query(INSERT)
                    .bind(&record.id)
                    .bind(&record.namespace)
                    .bind(&record.tenant)
                    .bind(record.requested_at as i64)
                    .bind(record.completed_at as i64)
                    .bind(record.s3_objects_deleted as i64)
                    .bind(record.verified_empty)
                    .bind(record.local_reclaimed)
                    .execute(&self.pool)
                    .await
                    .map_err(control_plane_error)?;
                Ok(())
            })
        }

        fn latest(
            &self,
            tenant: &str,
            namespace: &str,
        ) -> Result<Option<ErasureRecord>, KoshaError> {
            let sql = format!(
                "SELECT {COLUMNS} FROM kosha.namespace_erasures \
                 WHERE tenant = $1 AND namespace = $2 \
                 ORDER BY completed_at DESC, requested_at DESC LIMIT 1"
            );
            self.rt.block_on(async {
                let row: Option<Row> = sqlx::query_as(&sql)
                    .bind(tenant)
                    .bind(namespace)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(control_plane_error)?;
                Ok(row.map(from_row))
            })
        }

        fn list(
            &self,
            tenant: Option<&str>,
            limit: usize,
        ) -> Result<Vec<ErasureRecord>, KoshaError> {
            let sql = format!(
                "SELECT {COLUMNS} FROM kosha.namespace_erasures \
                 WHERE ($1::text IS NULL OR tenant = $1) \
                 ORDER BY completed_at DESC, requested_at DESC LIMIT $2"
            );
            self.rt.block_on(async {
                let rows: Vec<Row> = sqlx::query_as(&sql)
                    .bind(tenant)
                    .bind(limit.min(i64::MAX as usize) as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(control_plane_error)?;
                Ok(rows.into_iter().map(from_row).collect())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(tenant: &str, ns: &str, completed_at: u64) -> ErasureRecord {
        ErasureRecord {
            id: ErasureRecord::new_id(completed_at),
            namespace: ns.into(),
            tenant: tenant.into(),
            requested_at: completed_at.saturating_sub(5),
            completed_at,
            s3_objects_deleted: 3,
            verified_empty: true,
            local_reclaimed: true,
        }
    }

    #[test]
    fn in_memory_store_keeps_every_record_and_scopes_by_tenant() {
        let store = InMemoryErasureStore::new();
        store.record(&record("a", "docs", 100)).unwrap();
        store.record(&record("b", "docs", 150)).unwrap();
        let second = record("a", "docs", 200);
        store.record(&second).unwrap();
        store.record(&record("a", "other", 300)).unwrap();

        assert_eq!(store.latest("a", "docs").unwrap(), Some(second.clone()));
        assert_eq!(store.latest("b", "docs").unwrap().unwrap().tenant, "b");
        assert!(store.latest("c", "docs").unwrap().is_none());

        let a = store.list(Some("a"), 10).unwrap();
        assert_eq!(
            a.iter().map(|r| r.completed_at).collect::<Vec<_>>(),
            [300, 200, 100]
        );
        assert_eq!(store.list(None, 10).unwrap().len(), 4);
        assert_eq!(store.list(None, 2).unwrap().len(), 2);
    }

    #[test]
    fn ids_are_distinct() {
        let a = ErasureRecord::new_id(1);
        let b = ErasureRecord::new_id(1);
        assert_ne!(a, b);
        assert!(a.starts_with("er_"));
    }
}
