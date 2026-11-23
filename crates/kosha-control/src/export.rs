//! Export jobs (#347) for both control stores. See [`kosha_core::export`]
//! for what a record means.
//!
//! The in-memory [`ExportLog`] backs `Controller` (records last as long as
//! the process). Postgres keeps them in `kosha.exports` (migration 010).

use std::time::{SystemTime, UNIX_EPOCH};

use kosha_core::export::{ExportFilter, ExportRecord, ExportState, ExportUpdate, NewExport};

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The host of a validated upload URL, for display.
pub(crate) fn url_host(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let authority = rest.split(['/', '?', '#']).next()?;
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    (!host.is_empty()).then(|| host.to_string())
}

fn versions_of(export: &NewExport) -> Vec<(String, u64)> {
    export
        .pins
        .iter()
        .map(|(ns, manifest)| (ns.0.clone(), manifest.version))
        .collect()
}

/// Apply `update` to `record` (shared by both stores' semantics).
fn apply(record: &mut ExportRecord, update: &ExportUpdate, now: u64) {
    if let Some(state) = update.state {
        record.state = state;
    }
    if let Some(parts) = &update.parts {
        record.parts = parts.clone();
    }
    if let Some(documents) = update.documents {
        record.documents = documents;
    }
    if let Some(bytes) = update.bytes {
        record.bytes = bytes;
    }
    if let Some(error) = &update.error {
        record.error = error.clone();
    }
    if let Some(state) = update.delivery_state {
        record.delivery_state = Some(state);
    }
    if let Some(error) = &update.delivery_error {
        record.delivery_error = error.clone();
    }
    if let Some(at) = update.completed_at_ms {
        record.completed_at_ms = Some(at);
    }
    if let Some(at) = update.expires_at_ms {
        record.expires_at_ms = Some(at);
    }
    if update.clear_upload_url {
        record.upload_url = None;
    }
    if update.clear_pins {
        record.pins.clear();
    }
    record.updated_at_ms = now;
}

/// The in-memory store's exports, oldest first.
#[derive(Debug, Default)]
pub(crate) struct ExportLog {
    records: Vec<ExportRecord>,
    issued: u64,
    /// Export id -> when its claim was last taken or refreshed (unix ms).
    claimed_at: std::collections::HashMap<String, u64>,
}

impl ExportLog {
    pub(crate) fn create(&mut self, export: &NewExport) -> ExportRecord {
        self.issued += 1;
        let now = unix_millis();
        let record = ExportRecord {
            id: format!("exp-{now:x}-{:x}-{}", std::process::id(), self.issued),
            tenant_id: export.tenant_id.clone(),
            namespace: export.namespace.clone(),
            state: ExportState::Pending,
            format: "jsonl.gz".into(),
            include_vectors: export.include_vectors,
            versions: versions_of(export),
            parts: Vec::new(),
            documents: 0,
            bytes: 0,
            error: None,
            delivery_host: export.upload_url.as_deref().and_then(url_host),
            delivery_state: export
                .upload_url
                .as_ref()
                .map(|_| kosha_core::export::DeliveryState::Pending),
            delivery_error: None,
            attempts: 0,
            claimed_by: None,
            created_at_ms: now,
            updated_at_ms: now,
            completed_at_ms: None,
            expires_at_ms: None,
            ttl_ms: export.ttl_ms,
            pins: export.pins.clone(),
            upload_url: export.upload_url.clone(),
        };
        self.records.push(record.clone());
        record
    }

    pub(crate) fn get(&self, id: &str) -> Option<ExportRecord> {
        self.records.iter().find(|r| r.id == id).cloned()
    }

    pub(crate) fn list(&self, filter: &ExportFilter) -> Vec<ExportRecord> {
        self.records
            .iter()
            .filter(|r| filter.matches(r))
            .take(filter.effective_limit() as usize)
            .cloned()
            .collect()
    }

    pub(crate) fn update(&mut self, id: &str, update: &ExportUpdate) -> Option<ExportRecord> {
        let record = self.records.iter_mut().find(|r| r.id == id)?;
        if let Some(holder) = &update.holder {
            if record.claimed_by.as_ref() != Some(holder) || !record.state.is_active() {
                return None;
            }
        }
        apply(record, update, unix_millis());
        Some(record.clone())
    }

    pub(crate) fn claim(
        &mut self,
        id: &str,
        holder: &str,
        stale_after_ms: u64,
    ) -> Option<ExportRecord> {
        let claimed_at = &mut self.claimed_at;
        let now = unix_millis();
        let record = self.records.iter_mut().find(|r| r.id == id)?;
        let mine = record.claimed_by.as_deref() == Some(holder);
        let stale = claimed_at
            .get(id)
            .is_none_or(|at| now.saturating_sub(*at) > stale_after_ms);
        let claimable = match record.state {
            ExportState::Pending => true,
            ExportState::Running => mine || stale,
            _ => false,
        };
        if !claimable {
            return None;
        }
        if !mine || record.state == ExportState::Pending {
            record.attempts += 1;
        }
        record.state = ExportState::Running;
        record.claimed_by = Some(holder.to_string());
        record.updated_at_ms = now;
        claimed_at.insert(id.to_string(), now);
        Some(record.clone())
    }

    pub(crate) fn delete(&mut self, ids: &[String]) -> u64 {
        let before = self.records.len();
        self.records.retain(|r| !ids.contains(&r.id));
        (before - self.records.len()) as u64
    }
}

#[cfg(feature = "postgres")]
pub(crate) use pg::{claim, create, delete, get, list, update};

#[cfg(feature = "postgres")]
mod pg {
    use super::{apply, url_host, versions_of, ExportFilter, ExportRecord, ExportState};
    use super::{ExportUpdate, NewExport};
    use kosha_core::export::{DeliveryState, ExportPart};
    use kosha_core::{KoshaError, Manifest, NamespaceId};
    use sqlx::Row;

    const COLUMNS: &str = "id, tenant_id, namespace, state, format, include_vectors, versions, \
         pins, parts, documents, bytes, error, upload_url, delivery_host, delivery_state, \
         delivery_error, attempts, claimed_by, ttl_ms, \
         (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_ms, \
         (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_ms, \
         (EXTRACT(EPOCH FROM completed_at) * 1000)::BIGINT AS completed_ms, \
         (EXTRACT(EPOCH FROM expires_at) * 1000)::BIGINT AS expires_ms";

    fn io_error(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    fn json_error(id: &str, what: &str, e: serde_json::Error) -> KoshaError {
        KoshaError::ControlPlane(format!("export {id}: unreadable {what}: {e}"))
    }

    fn ms(value: Option<i64>) -> Option<u64> {
        value.map(|v| v.max(0) as u64)
    }

    fn decode(row: sqlx::postgres::PgRow) -> Result<ExportRecord, KoshaError> {
        let get_str = |name: &str| row.try_get::<String, _>(name).map_err(io_error);
        let get_opt = |name: &str| row.try_get::<Option<String>, _>(name).map_err(io_error);
        let id = get_str("id")?;
        let state_raw = get_str("state")?;
        let state = ExportState::parse(&state_raw).ok_or_else(|| {
            KoshaError::ControlPlane(format!("export {id}: unknown state '{state_raw}'"))
        })?;
        let versions: Vec<(String, u64)> = serde_json::from_str(&get_str("versions")?)
            .map_err(|e| json_error(&id, "versions", e))?;
        let pins: Vec<(NamespaceId, Manifest)> = match get_opt("pins")? {
            Some(raw) => serde_json::from_str(&raw).map_err(|e| json_error(&id, "pins", e))?,
            None => Vec::new(),
        };
        let parts: Vec<ExportPart> =
            serde_json::from_str(&get_str("parts")?).map_err(|e| json_error(&id, "parts", e))?;
        let i64_of = |name: &str| row.try_get::<i64, _>(name).map_err(io_error);
        let opt_i64 = |name: &str| row.try_get::<Option<i64>, _>(name).map_err(io_error);
        Ok(ExportRecord {
            tenant_id: get_str("tenant_id")?,
            namespace: get_str("namespace")?,
            state,
            format: get_str("format")?,
            include_vectors: row
                .try_get::<bool, _>("include_vectors")
                .map_err(io_error)?,
            versions,
            parts,
            documents: i64_of("documents")?.max(0) as u64,
            bytes: i64_of("bytes")?.max(0) as u64,
            error: get_opt("error")?,
            delivery_host: get_opt("delivery_host")?,
            delivery_state: get_opt("delivery_state")?
                .as_deref()
                .and_then(DeliveryState::parse),
            delivery_error: get_opt("delivery_error")?,
            attempts: row.try_get::<i32, _>("attempts").map_err(io_error)?.max(0) as u32,
            claimed_by: get_opt("claimed_by")?,
            created_at_ms: i64_of("created_ms")?.max(0) as u64,
            updated_at_ms: i64_of("updated_ms")?.max(0) as u64,
            completed_at_ms: ms(opt_i64("completed_ms")?),
            expires_at_ms: ms(opt_i64("expires_ms")?),
            ttl_ms: i64_of("ttl_ms")?.max(0) as u64,
            pins,
            upload_url: get_opt("upload_url")?,
            id,
        })
    }

    pub(crate) async fn create(
        pool: &sqlx::PgPool,
        export: &NewExport,
    ) -> Result<ExportRecord, KoshaError> {
        let id = format!("exp-{}", uuid::Uuid::new_v4());
        let pins = serde_json::to_string(&export.pins)
            .map_err(|e| KoshaError::ControlPlane(format!("export pins: {e}")))?;
        let versions = serde_json::to_string(&versions_of(export))
            .map_err(|e| KoshaError::ControlPlane(format!("export versions: {e}")))?;
        let row = sqlx::query(&format!(
            "INSERT INTO kosha.exports \
             (id, tenant_id, namespace, include_vectors, versions, pins, upload_url, \
              delivery_host, delivery_state, ttl_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING {COLUMNS}"
        ))
        .bind(&id)
        .bind(&export.tenant_id)
        .bind(&export.namespace)
        .bind(export.include_vectors)
        .bind(versions)
        .bind(pins)
        .bind(&export.upload_url)
        .bind(export.upload_url.as_deref().and_then(url_host))
        .bind(
            export
                .upload_url
                .as_ref()
                .map(|_| DeliveryState::Pending.as_str()),
        )
        .bind(export.ttl_ms as i64)
        .fetch_one(pool)
        .await
        .map_err(io_error)?;
        decode(row)
    }

    pub(crate) async fn get(
        pool: &sqlx::PgPool,
        id: &str,
    ) -> Result<Option<ExportRecord>, KoshaError> {
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM kosha.exports WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(io_error)?;
        row.map(decode).transpose()
    }

    pub(crate) async fn list(
        pool: &sqlx::PgPool,
        filter: &ExportFilter,
    ) -> Result<Vec<ExportRecord>, KoshaError> {
        let states: Vec<String> = filter
            .states
            .iter()
            .map(|s| s.as_str().to_string())
            .collect();
        let rows = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM kosha.exports \
             WHERE ($1::TEXT IS NULL OR tenant_id = $1) \
               AND ($2::TEXT IS NULL OR namespace = $2) \
               AND (cardinality($3::TEXT[]) = 0 OR state = ANY($3)) \
             ORDER BY created_at, id LIMIT $4"
        ))
        .bind(&filter.tenant_id)
        .bind(&filter.namespace)
        .bind(states)
        .bind(i64::from(filter.effective_limit()))
        .fetch_all(pool)
        .await
        .map_err(io_error)?;
        rows.into_iter().map(decode).collect()
    }

    /// Read, apply and write back under a row lock, so the update rules
    /// live in one place ([`apply`]) for both stores.
    pub(crate) async fn update(
        pool: &sqlx::PgPool,
        id: &str,
        update: &ExportUpdate,
    ) -> Result<Option<ExportRecord>, KoshaError> {
        let mut tx = pool.begin().await.map_err(io_error)?;
        let row = sqlx::query(&format!(
            "SELECT {COLUMNS} FROM kosha.exports WHERE id = $1 FOR UPDATE"
        ))
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(io_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut record = decode(row)?;
        if let Some(holder) = &update.holder {
            if record.claimed_by.as_ref() != Some(holder) || !record.state.is_active() {
                return Ok(None);
            }
        }
        apply(&mut record, update, 0);
        let parts = serde_json::to_string(&record.parts)
            .map_err(|e| KoshaError::ControlPlane(format!("export parts: {e}")))?;
        let row = sqlx::query(&format!(
            "UPDATE kosha.exports SET state = $2, parts = $3, documents = $4, bytes = $5, \
               error = $6, delivery_state = $7, delivery_error = $8, \
               completed_at = CASE WHEN $9::BIGINT IS NULL THEN completed_at \
                 ELSE TIMESTAMPTZ 'epoch' + $9::BIGINT * INTERVAL '1 millisecond' END, \
               expires_at = CASE WHEN $10::BIGINT IS NULL THEN expires_at \
                 ELSE TIMESTAMPTZ 'epoch' + $10::BIGINT * INTERVAL '1 millisecond' END, \
               upload_url = CASE WHEN $11 THEN NULL ELSE upload_url END, \
               pins = CASE WHEN $12 THEN NULL ELSE pins END, \
               updated_at = NOW() \
             WHERE id = $1 RETURNING {COLUMNS}"
        ))
        .bind(id)
        .bind(record.state.as_str())
        .bind(parts)
        .bind(record.documents as i64)
        .bind(record.bytes as i64)
        .bind(&record.error)
        .bind(record.delivery_state.map(DeliveryState::as_str))
        .bind(&record.delivery_error)
        .bind(update.completed_at_ms.map(|v| v as i64))
        .bind(update.expires_at_ms.map(|v| v as i64))
        .bind(update.clear_upload_url)
        .bind(update.clear_pins)
        .fetch_one(&mut *tx)
        .await
        .map_err(io_error)?;
        let record = decode(row)?;
        tx.commit().await.map_err(io_error)?;
        Ok(Some(record))
    }

    pub(crate) async fn claim(
        pool: &sqlx::PgPool,
        id: &str,
        holder: &str,
        stale_after_ms: u64,
    ) -> Result<Option<ExportRecord>, KoshaError> {
        let row = sqlx::query(&format!(
            "UPDATE kosha.exports SET state = 'running', \
               attempts = attempts + CASE WHEN claimed_by IS DISTINCT FROM $2 \
                 OR state = 'pending' THEN 1 ELSE 0 END, \
               claimed_by = $2, claimed_at = NOW(), updated_at = NOW() \
             WHERE id = $1 AND (state = 'pending' OR (state = 'running' AND \
               (claimed_by = $2 OR claimed_at IS NULL \
                OR claimed_at < NOW() - $3::BIGINT * INTERVAL '1 millisecond'))) \
             RETURNING {COLUMNS}"
        ))
        .bind(id)
        .bind(holder)
        .bind(stale_after_ms as i64)
        .fetch_optional(pool)
        .await
        .map_err(io_error)?;
        row.map(decode).transpose()
    }

    pub(crate) async fn delete(pool: &sqlx::PgPool, ids: &[String]) -> Result<u64, KoshaError> {
        if ids.is_empty() {
            return Ok(0);
        }
        let done = sqlx::query("DELETE FROM kosha.exports WHERE id = ANY($1)")
            .bind(ids)
            .execute(pool)
            .await
            .map_err(io_error)?;
        Ok(done.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_host_takes_the_authority_without_credentials() {
        assert_eq!(
            url_host("https://bucket.s3.amazonaws.com/key?X-Amz-Signature=abc").as_deref(),
            Some("bucket.s3.amazonaws.com")
        );
        assert_eq!(
            url_host("https://u:p@host:443/x").as_deref(),
            Some("host:443")
        );
        assert_eq!(url_host("nonsense"), None);
    }
}
