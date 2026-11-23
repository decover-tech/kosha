//! Deletion records (#347) for both control stores. See
//! [`kosha_core::deletion`] for what a record means.
//!
//! The in-memory [`DeletionLog`] backs `Controller` (records last as long
//! as the process, like everything else in that store). Postgres keeps them
//! in `kosha.deletion_records` (migration 004).

use std::time::{SystemTime, UNIX_EPOCH};

use kosha_core::deletion::{
    DeletionFilter, DeletionPosition, DeletionRecord, DeletionState, NewDeletion,
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
                record.state = DeletionState::Completed;
                record.completed_at_ms = Some(unix_millis());
            }
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
pub(crate) use pg::{get, insert, list, list_after, record_attempt};

#[cfg(feature = "postgres")]
mod pg {
    use super::{
        no_such_record, DeletionFilter, DeletionPosition, DeletionRecord, DeletionState,
        NewDeletion,
    };
    use kosha_core::deletion::DeletionScope;
    use kosha_core::KoshaError;

    /// `kosha.deletion_records` columns, in [`Row`] order. Timestamps are
    /// read as unix milliseconds.
    const COLUMNS: &str = "id, scope, subject, tenant_id, parent_id, last_version, state, \
         attempts, objects_deleted, last_error, \
         (EXTRACT(EPOCH FROM requested_at) * 1000)::BIGINT, \
         (EXTRACT(EPOCH FROM completed_at) * 1000)::BIGINT";

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
             (id, scope, subject, tenant_id, parent_id, last_version) \
             VALUES ($1, $2, $3, $4, $5, $6) RETURNING {COLUMNS}"
        ))
        .bind(&id)
        .bind(deletion.scope.as_str())
        .bind(&deletion.subject)
        .bind(&deletion.tenant_id)
        .bind(&deletion.parent_id)
        .bind(last_version as i64)
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
        let updated: Option<Row> = sqlx::query_as(&format!(
            "UPDATE kosha.deletion_records SET \
               attempts = attempts + 1, \
               objects_deleted = objects_deleted + $2, \
               last_error = $3, \
               state = CASE WHEN $3::TEXT IS NULL THEN 'completed' ELSE state END, \
               completed_at = CASE WHEN $3::TEXT IS NULL THEN NOW() ELSE completed_at END \
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
    /// after `after`. The millisecond value is the one [`COLUMNS`] reads,
    /// so a position taken from a record compares the way the rows sort.
    pub(crate) async fn list_after(
        pool: &sqlx::PgPool,
        after: Option<&DeletionPosition>,
        limit: u32,
    ) -> Result<Vec<DeletionRecord>, KoshaError> {
        const MS: &str = "(EXTRACT(EPOCH FROM requested_at) * 1000)::BIGINT";
        let rows: Vec<Row> = sqlx::query_as(&format!(
            "SELECT {COLUMNS} FROM kosha.deletion_records \
             WHERE $1::BIGINT IS NULL OR ({MS}, id) > ($1::BIGINT, $2::TEXT) \
             ORDER BY {MS}, id LIMIT $3"
        ))
        .bind(after.map(|after| after.requested_at_ms as i64))
        .bind(after.map_or("", |after| after.id.as_str()))
        .bind(i64::from(limit.max(1)))
        .fetch_all(pool)
        .await
        .map_err(io_error)?;
        rows.into_iter().map(decode).collect()
    }
}
