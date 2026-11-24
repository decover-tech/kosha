//! Deletion records (#347) for both control stores. See
//! [`kosha_core::deletion`] for what a record means.
//!
//! The in-memory [`DeletionLog`] backs `Controller` (records last as long
//! as the process, like everything else in that store). Postgres keeps them
//! in `kosha.deletion_records` (migration 004).

use std::time::{SystemTime, UNIX_EPOCH};

use std::collections::HashMap;

use kosha_core::deletion::{
    AckStatus, DeletionFilter, DeletionPosition, DeletionRecord, DeletionState, NewDeletion,
    PodHeartbeat, PodInfo,
};
use kosha_core::KoshaError;

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn no_such_record(id: &str) -> KoshaError {
    KoshaError::NotFound(format!("deletion record {id}"))
}

/// The in-memory store's deletion records, oldest first.
#[derive(Debug, Default)]
pub(crate) struct DeletionLog {
    records: Vec<DeletionRecord>,
    issued: u64,
    /// The pod registry (#347): pod id -> its last heartbeat.
    pods: HashMap<String, PodInfo>,
    /// Deletion id -> `(pod id, acknowledged at ms)`.
    acks: HashMap<String, Vec<(String, u64)>>,
}

impl DeletionLog {
    pub(crate) fn begin(&mut self, deletion: &NewDeletion, last_version: u64) -> DeletionRecord {
        self.issued += 1;
        let now = unix_millis();
        let record = DeletionRecord {
            // Unique within the process, which is as far as this store goes.
            id: format!("del-{now:x}-{:x}-{}", std::process::id(), self.issued),
            scope: deletion.scope,
            subject: deletion.subject.clone(),
            tenant_id: deletion.tenant_id.clone(),
            parent_id: deletion.parent_id.clone(),
            last_version,
            state: DeletionState::Pending,
            attempts: 0,
            objects_deleted: 0,
            last_error: None,
            requested_at_ms: now,
            completed_at_ms: None,
            acks_required: deletion.acks_required,
            storage_verified: false,
            exports_deleted: 0,
        };
        self.records.push(record.clone());
        record
    }

    pub(crate) fn record_attempt(
        &mut self,
        id: &str,
        objects_deleted: u64,
        error: Option<&str>,
    ) -> Result<DeletionRecord, KoshaError> {
        let record = self
            .records
            .iter_mut()
            .find(|record| record.id == id)
            .ok_or_else(|| no_such_record(id))?;
        if record.state == DeletionState::Pending {
            record.attempts += 1;
            record.objects_deleted += objects_deleted;
            record.last_error = error.map(str::to_string);
            if error.is_none() {
                record.storage_verified = true;
                // With acknowledgements required, `finalize` completes it.
                if !record.acks_required {
                    record.state = DeletionState::Completed;
                    record.completed_at_ms = Some(unix_millis());
                }
            }
        }
        Ok(record.clone())
    }

    pub(crate) fn note_exports(&mut self, id: &str, count: u64) -> Result<(), KoshaError> {
        let record = self
            .records
            .iter_mut()
            .find(|record| record.id == id)
            .ok_or_else(|| no_such_record(id))?;
        record.exports_deleted += count;
        Ok(())
    }

    pub(crate) fn heartbeat(&mut self, pod: &PodHeartbeat) {
        self.pods.insert(
            pod.pod_id.clone(),
            PodInfo {
                pod_id: pod.pod_id.clone(),
                role: pod.role.clone(),
                purge_enabled: pod.purge_enabled,
                last_heartbeat_ms: unix_millis(),
            },
        );
    }

    pub(crate) fn acknowledge(&mut self, id: &str, pod_id: &str) -> Result<(), KoshaError> {
        if !self.records.iter().any(|record| record.id == id) {
            return Err(no_such_record(id));
        }
        let acks = self.acks.entry(id.to_string()).or_default();
        if !acks.iter().any(|(pod, _)| pod == pod_id) {
            acks.push((pod_id.to_string(), unix_millis()));
        }
        Ok(())
    }

    pub(crate) fn ack_status(&self, id: &str, live_within_ms: u64) -> AckStatus {
        let pods: Vec<PodInfo> = self.pods.values().cloned().collect();
        let acks = self.acks.get(id).cloned().unwrap_or_default();
        AckStatus::compute(&pods, &acks, unix_millis().saturating_sub(live_within_ms))
    }

    pub(crate) fn finalize(
        &mut self,
        id: &str,
        live_within_ms: u64,
    ) -> Result<DeletionRecord, KoshaError> {
        let ready = self.ack_status(id, live_within_ms).complete();
        let record = self
            .records
            .iter_mut()
            .find(|record| record.id == id)
            .ok_or_else(|| no_such_record(id))?;
        if ready && record.awaiting_acks() {
            record.state = DeletionState::Completed;
            record.completed_at_ms = Some(unix_millis());
            record.last_error = None;
        }
        Ok(record.clone())
    }

    pub(crate) fn get(&self, id: &str) -> Option<DeletionRecord> {
        self.records.iter().find(|record| record.id == id).cloned()
    }

    pub(crate) fn list_after(
        &self,
        after: Option<&DeletionPosition>,
        limit: u32,
    ) -> Vec<DeletionRecord> {
        let mut records: Vec<DeletionRecord> = self
            .records
            .iter()
            .filter(|record| after.is_none_or(|after| record.position() > *after))
            .cloned()
            .collect();
        records.sort_by_key(DeletionRecord::position);
        records.truncate(limit.max(1) as usize);
        records
    }

    pub(crate) fn list(&self, filter: &DeletionFilter) -> Vec<DeletionRecord> {
        self.records
            .iter()
            .filter(|record| filter.matches(record))
            .take(filter.effective_limit() as usize)
            .cloned()
            .collect()
    }
}

#[cfg(feature = "postgres")]
pub(crate) use pg::{
    ack_status, acknowledge, finalize, get, heartbeat, insert, list, list_after, note_exports,
    record_attempt,
};

#[cfg(feature = "postgres")]
mod pg {
    use super::{
        no_such_record, AckStatus, DeletionFilter, DeletionPosition, DeletionRecord, DeletionState,
        NewDeletion, PodHeartbeat, PodInfo,
    };
    use kosha_core::deletion::DeletionScope;
    use kosha_core::KoshaError;

    /// `requested_at` truncated to the millisecond, in UTC: the position
    /// records are paged by. `date_trunc` on a `timestamp` and
    /// `AT TIME ZONE 'UTC'` are both immutable, so migration 010 indexes
    /// exactly this expression (`idx_deletion_records_position`) and
    /// [`list_after`] reads it in index order (#347 review nit).
    const POSITION: &str = "date_trunc('milliseconds', requested_at AT TIME ZONE 'UTC')";

    /// `kosha.deletion_records` columns, in [`Row`] order. Timestamps are
    /// read as unix milliseconds; `requested_at` through [`POSITION`], so a
    /// position taken from a record compares the way the rows sort.
    const COLUMNS: &str = "id, scope, subject, tenant_id, parent_id, last_version, state, \
         attempts, objects_deleted, last_error, \
         (EXTRACT(EPOCH FROM date_trunc('milliseconds', requested_at AT TIME ZONE 'UTC')) \
            * 1000)::BIGINT, \
         (EXTRACT(EPOCH FROM completed_at) * 1000)::BIGINT, \
         acks_required, storage_verified, exports_deleted";

    type Row = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        i64,
        String,
        i32,
        i64,
        Option<String>,
        i64,
        Option<i64>,
        bool,
        bool,
        i64,
    );

    fn io_error(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    fn decode(row: Row) -> Result<DeletionRecord, KoshaError> {
        let (
            id,
            scope,
            subject,
            tenant_id,
            parent_id,
            last_version,
            state,
            attempts,
            objects_deleted,
            last_error,
            requested_at_ms,
            completed_at_ms,
            acks_required,
            storage_verified,
            exports_deleted,
        ) = row;
        let bad = |what: &str, value: &str| {
            KoshaError::ControlPlane(format!("deletion record {id}: unknown {what} '{value}'"))
        };
        Ok(DeletionRecord {
            scope: DeletionScope::parse(&scope).ok_or_else(|| bad("scope", &scope))?,
            state: DeletionState::parse(&state).ok_or_else(|| bad("state", &state))?,
            id,
            subject,
            tenant_id,
            parent_id,
            last_version: last_version.max(0) as u64,
            attempts: attempts.max(0) as u32,
            objects_deleted: objects_deleted.max(0) as u64,
            last_error,
            requested_at_ms: requested_at_ms.max(0) as u64,
            completed_at_ms: completed_at_ms.map(|ms| ms.max(0) as u64),
            acks_required,
            storage_verified,
            exports_deleted: exports_deleted.max(0) as u64,
        })
    }

    /// Insert a pending record. Runs on any executor, so the caller can put
    /// it in the transaction that removes the namespace's rows.
    pub(crate) async fn insert<'e, E>(
        executor: E,
        deletion: &NewDeletion,
        last_version: u64,
    ) -> Result<DeletionRecord, KoshaError>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let id = format!("del-{}", uuid::Uuid::new_v4());
        let row: Row = sqlx::query_as(&format!(
            "INSERT INTO kosha.deletion_records \
             (id, scope, subject, tenant_id, parent_id, last_version, acks_required) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {COLUMNS}"
        ))
        .bind(&id)
        .bind(deletion.scope.as_str())
        .bind(&deletion.subject)
        .bind(&deletion.tenant_id)
        .bind(&deletion.parent_id)
        .bind(last_version as i64)
        .bind(deletion.acks_required)
        .fetch_one(executor)
        .await
        .map_err(io_error)?;
        decode(row)
    }

    pub(crate) async fn record_attempt(
        pool: &sqlx::PgPool,
        id: &str,
        objects_deleted: u64,
        error: Option<&str>,
    ) -> Result<DeletionRecord, KoshaError> {
        // Only a pending record changes; a completed one stays as it was.
        // A verified reclaim completes the record unless it waits for the
        // pods' acknowledgements (`finalize` completes those).
        let updated: Option<Row> = sqlx::query_as(&format!(
            "UPDATE kosha.deletion_records SET \
               attempts = attempts + 1, \
               objects_deleted = objects_deleted + $2, \
               last_error = $3, \
               storage_verified = storage_verified OR $3::TEXT IS NULL, \
               state = CASE WHEN $3::TEXT IS NULL AND NOT acks_required \
                            THEN 'completed' ELSE state END, \
               completed_at = CASE WHEN $3::TEXT IS NULL AND NOT acks_required \
                                   THEN NOW() ELSE completed_at END \
             WHERE id = $1 AND state = 'pending' RETURNING {COLUMNS}"
        ))
        .bind(id)
        .bind(objects_deleted as i64)
        .bind(error)
        .fetch_optional(pool)
        .await
        .map_err(io_error)?;
        match updated {
            Some(row) => decode(row),
            None => get(pool, id).await?.ok_or_else(|| no_such_record(id)),
        }
    }

    pub(crate) async fn note_exports(
        pool: &sqlx::PgPool,
        id: &str,
        count: u64,
    ) -> Result<(), KoshaError> {
        let done = sqlx::query(
            "UPDATE kosha.deletion_records SET exports_deleted = exports_deleted + $2 \
             WHERE id = $1",
        )
        .bind(id)
        .bind(count as i64)
        .execute(pool)
        .await
        .map_err(io_error)?;
        if done.rows_affected() == 0 {
            return Err(no_such_record(id));
        }
        Ok(())
    }

    pub(crate) async fn get(
        pool: &sqlx::PgPool,
        id: &str,
    ) -> Result<Option<DeletionRecord>, KoshaError> {
        let row: Option<Row> = sqlx::query_as(&format!(
            "SELECT {COLUMNS} FROM kosha.deletion_records WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(io_error)?;
        row.map(decode).transpose()
    }

    pub(crate) async fn list(
        pool: &sqlx::PgPool,
        filter: &DeletionFilter,
    ) -> Result<Vec<DeletionRecord>, KoshaError> {
        let rows: Vec<Row> = sqlx::query_as(&format!(
            "SELECT {COLUMNS} FROM kosha.deletion_records \
             WHERE ($1::TEXT IS NULL OR state = $1) \
               AND ($2::TEXT IS NULL OR tenant_id = $2) \
             ORDER BY requested_at, id LIMIT $3"
        ))
        .bind(filter.state.map(DeletionState::as_str))
        .bind(&filter.tenant_id)
        .bind(i64::from(filter.effective_limit()))
        .fetch_all(pool)
        .await
        .map_err(io_error)?;
        rows.into_iter().map(decode).collect()
    }

    /// `kosha.deletion_records` in `(requested_at in ms, id)` order, strictly
    /// after `after`. Compares and sorts on [`POSITION`], which
    /// `idx_deletion_records_position` covers, so a page is an index range
    /// scan instead of a sort of the whole table. Two statements rather
    /// than `$1 IS NULL OR ...`, so a generic plan can still use the index.
    pub(crate) async fn list_after(
        pool: &sqlx::PgPool,
        after: Option<&DeletionPosition>,
        limit: u32,
    ) -> Result<Vec<DeletionRecord>, KoshaError> {
        let limit = i64::from(limit.max(1));
        let rows: Vec<Row> = match after {
            None => sqlx::query_as(&format!(
                "SELECT {COLUMNS} FROM kosha.deletion_records \
                 ORDER BY {POSITION}, id LIMIT $1"
            ))
            .bind(limit)
            .fetch_all(pool)
            .await
            .map_err(io_error)?,
            Some(after) => sqlx::query_as(&format!(
                "SELECT {COLUMNS} FROM kosha.deletion_records \
                 WHERE ({POSITION}, id) > \
                       (TIMESTAMP '1970-01-01 00:00:00' + $1::BIGINT * INTERVAL '1 millisecond', \
                        $2::TEXT) \
                 ORDER BY {POSITION}, id LIMIT $3"
            ))
            .bind(after.requested_at_ms as i64)
            .bind(after.id.as_str())
            .bind(limit)
            .fetch_all(pool)
            .await
            .map_err(io_error)?,
        };
        rows.into_iter().map(decode).collect()
    }

    pub(crate) async fn heartbeat(
        pool: &sqlx::PgPool,
        pod: &PodHeartbeat,
    ) -> Result<(), KoshaError> {
        sqlx::query(
            "INSERT INTO kosha.pods (pod_id, role, purge_enabled, last_heartbeat_at) \
             VALUES ($1, $2, $3, NOW()) \
             ON CONFLICT (pod_id) DO UPDATE SET role = EXCLUDED.role, \
               purge_enabled = EXCLUDED.purge_enabled, last_heartbeat_at = NOW()",
        )
        .bind(&pod.pod_id)
        .bind(&pod.role)
        .bind(pod.purge_enabled)
        .execute(pool)
        .await
        .map_err(io_error)?;
        Ok(())
    }

    pub(crate) async fn acknowledge(
        pool: &sqlx::PgPool,
        id: &str,
        pod_id: &str,
    ) -> Result<(), KoshaError> {
        sqlx::query(
            "INSERT INTO kosha.deletion_acks (deletion_id, pod_id) VALUES ($1, $2) \
             ON CONFLICT (deletion_id, pod_id) DO NOTHING",
        )
        .bind(id)
        .bind(pod_id)
        .execute(pool)
        .await
        .map_err(io_error)?;
        Ok(())
    }

    /// Acknowledgements against the registry, judged by the database's
    /// clock so pods' clocks do not matter.
    pub(crate) async fn ack_status(
        pool: &sqlx::PgPool,
        id: &str,
        live_within_ms: u64,
    ) -> Result<AckStatus, KoshaError> {
        let now_ms: i64 = sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT")
            .fetch_one(pool)
            .await
            .map_err(io_error)?;
        let pods: Vec<(String, String, bool, i64)> = sqlx::query_as(
            "SELECT pod_id, role, purge_enabled, \
               (EXTRACT(EPOCH FROM last_heartbeat_at) * 1000)::BIGINT \
             FROM kosha.pods WHERE purge_enabled",
        )
        .fetch_all(pool)
        .await
        .map_err(io_error)?;
        let acks: Vec<(String, i64)> = sqlx::query_as(
            "SELECT pod_id, (EXTRACT(EPOCH FROM acked_at) * 1000)::BIGINT \
             FROM kosha.deletion_acks WHERE deletion_id = $1",
        )
        .bind(id)
        .fetch_all(pool)
        .await
        .map_err(io_error)?;
        let pods: Vec<PodInfo> = pods
            .into_iter()
            .map(|(pod_id, role, purge_enabled, at)| PodInfo {
                pod_id,
                role,
                purge_enabled,
                last_heartbeat_ms: at.max(0) as u64,
            })
            .collect();
        let acks: Vec<(String, u64)> = acks
            .into_iter()
            .map(|(pod, at)| (pod, at.max(0) as u64))
            .collect();
        Ok(AckStatus::compute(
            &pods,
            &acks,
            (now_ms.max(0) as u64).saturating_sub(live_within_ms),
        ))
    }

    /// Complete a record waiting only for acknowledgements, in one
    /// statement with the check that no live purge pod is missing.
    pub(crate) async fn finalize(
        pool: &sqlx::PgPool,
        id: &str,
        live_within_ms: u64,
    ) -> Result<DeletionRecord, KoshaError> {
        let updated: Option<Row> = sqlx::query_as(&format!(
            "UPDATE kosha.deletion_records d SET state = 'completed', completed_at = NOW(), \
               last_error = NULL \
             WHERE d.id = $1 AND d.state = 'pending' AND d.storage_verified \
               AND d.acks_required \
               AND NOT EXISTS ( \
                 SELECT 1 FROM kosha.pods p \
                 WHERE p.purge_enabled \
                   AND p.last_heartbeat_at > NOW() - $2::BIGINT * INTERVAL '1 millisecond' \
                   AND NOT EXISTS ( \
                     SELECT 1 FROM kosha.deletion_acks a \
                     WHERE a.deletion_id = d.id AND a.pod_id = p.pod_id)) \
             RETURNING {COLUMNS}"
        ))
        .bind(id)
        .bind(live_within_ms as i64)
        .fetch_optional(pool)
        .await
        .map_err(io_error)?;
        match updated {
            Some(row) => decode(row),
            None => get(pool, id).await?.ok_or_else(|| no_such_record(id)),
        }
    }
}
