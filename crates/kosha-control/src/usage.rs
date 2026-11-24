//! Usage metering store (#348): raw usage events per tenant and namespace,
//! and the summary computed from them.
//!
//! A [`UsageEvent`] is one thing that happened (a search, a document write,
//! an hourly stored-bytes snapshot) with one quantity per meter. Meters are
//! plain strings so a new one needs no schema change; the ones the server
//! emits are in [`meter`].
//!
//! Recording is idempotent on `(event id, meter)`: a pod that crashes
//! between writing a batch and deleting its local spool file sends the batch
//! again after restart, and the second copy changes nothing. That is the
//! whole durability contract of [`UsageStore::record`].
//!
//! A summary is always derived from the stored events ([`summarize`]), never
//! kept as a running total, so a month's usage can be recomputed from the
//! raw rows.

use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

use kosha_core::KoshaError;
use serde::{Deserialize, Serialize};

/// Meter names the server emits.
pub mod meter {
    /// Successful searches, `1` per search (result-cache hits included).
    pub const QUERIES: &str = "queries";
    /// Bytes of the JSON response body of a successful search.
    pub const BYTES_RETURNED: &str = "bytes_returned";
    /// Bytes a search pulled from object storage: whole files and ranged
    /// reads alike. Zero for a search served from local disk or the result
    /// cache, so it measures cache misses, not work done. Cost attribution
    /// only; see `bytes_queried` in [`super::NamespaceUsage`] for the
    /// cache-independent query meter.
    pub const BYTES_FETCHED: &str = "bytes_fetched";
    /// Logical bytes of the documents in an accepted write: for each
    /// document its id, plus each field's name and value as submitted.
    pub const BYTES_WRITTEN: &str = "bytes_written";
    /// Documents in an accepted write.
    pub const DOCUMENTS_WRITTEN: &str = "documents_written";
    /// Scalar coordinates across all vectors in an accepted write.
    pub const VECTOR_DIMENSIONS_WRITTEN: &str = "vector_dimensions_written";
    /// Bytes of the objects of every segment the namespace's manifest
    /// references (segments a branch inherits are counted on the namespace
    /// that owns them). A gauge: one event per namespace per hour, with a
    /// deterministic id, so the hour's sum is the snapshot.
    pub const STORED_BYTES: &str = "stored_bytes";
}

/// One hour in milliseconds: the bucket width of [`UsageStore::hourly`].
pub const HOUR_MS: u64 = 3_600_000;

/// How far before a window [`UsageStore::latest_between`] is asked to look
/// for the stored-bytes snapshot that was current when the window opened.
/// Snapshots are hourly, so two days covers a long writer outage without
/// scanning a namespace's whole history.
pub const STORED_CARRY_LOOKBACK_MS: u64 = 48 * HOUR_MS;

/// One usage event. `id` must be unique per event and stable across
/// retries of the same event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    pub id: String,
    pub tenant: String,
    pub namespace: String,
    /// When it happened, in Unix milliseconds.
    pub at_ms: u64,
    /// Quantity per meter.
    pub meters: BTreeMap<String, u64>,
}

/// The sum of one meter over one hour of one namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageBucket {
    pub tenant: String,
    pub namespace: String,
    /// Start of the hour, in Unix milliseconds (a multiple of [`HOUR_MS`]).
    pub hour_start_ms: u64,
    pub meter: String,
    pub quantity: u64,
}

/// The latest value of a meter for one namespace
/// ([`UsageStore::latest_between`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageGauge {
    pub tenant: String,
    pub namespace: String,
    pub quantity: u64,
}

/// A durable, idempotent sink for usage events, and the raw reads a
/// summary is built from.
pub trait UsageStore: Send + Sync {
    /// Store `events`. A `(event id, meter)` pair already stored is left as
    /// it is. Returns the number of meter rows newly stored.
    fn record(&self, events: &[UsageEvent]) -> Result<u64, KoshaError>;

    /// Per `(tenant, namespace, hour, meter)` sums over events with
    /// `from_ms <= at_ms < to_ms`, for one tenant or (`None`) all of them.
    fn hourly(
        &self,
        tenant: Option<&str>,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<UsageBucket>, KoshaError>;

    /// For each namespace, the quantity of its latest `meter` event with
    /// `from_ms <= at_ms < to_ms`.
    fn latest_between(
        &self,
        tenant: Option<&str>,
        meter: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<UsageGauge>, KoshaError>;
}

/// One namespace's usage over a window.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct NamespaceUsage {
    pub tenant: String,
    pub namespace: String,
    pub queries: u64,
    pub bytes_returned: u64,
    pub bytes_fetched: u64,
    /// Derived, not emitted: for each search, the namespace's stored bytes
    /// at the time (its latest hourly snapshot at or before the search's
    /// hour, or the first one after it for searches older than every
    /// snapshot). It depends only on the search count and the snapshots, so
    /// it is the same whether a search read whole files, byte ranges or
    /// nothing at all (a warm or result-cached search).
    pub bytes_queried: u64,
    pub bytes_written: u64,
    pub documents_written: u64,
    pub vector_dimensions_written: u64,
    /// Sum of the hourly stored-bytes snapshots in the window: byte-hours.
    /// An hour without a snapshot (no writer running) contributes nothing;
    /// `stored_snapshots` says how many hours did.
    pub stored_byte_hours: u64,
    pub stored_snapshots: u64,
    /// The latest snapshot in the window, if any.
    pub stored_bytes_latest: Option<u64>,
    /// Meters this build does not know, summed.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub other: BTreeMap<String, u64>,
}

impl NamespaceUsage {
    /// Add `other`'s totals into `self` (for a tenant or fleet total).
    /// `stored_bytes_latest` adds up the namespaces that have one.
    pub fn add(&mut self, other: &NamespaceUsage) {
        self.queries = self.queries.saturating_add(other.queries);
        self.bytes_returned = self.bytes_returned.saturating_add(other.bytes_returned);
        self.bytes_fetched = self.bytes_fetched.saturating_add(other.bytes_fetched);
        self.bytes_queried = self.bytes_queried.saturating_add(other.bytes_queried);
        self.bytes_written = self.bytes_written.saturating_add(other.bytes_written);
        self.documents_written = self
            .documents_written
            .saturating_add(other.documents_written);
        self.vector_dimensions_written = self
            .vector_dimensions_written
            .saturating_add(other.vector_dimensions_written);
        self.stored_byte_hours = self
            .stored_byte_hours
            .saturating_add(other.stored_byte_hours);
        self.stored_snapshots = self.stored_snapshots.saturating_add(other.stored_snapshots);
        if let Some(latest) = other.stored_bytes_latest {
            self.stored_bytes_latest =
                Some(self.stored_bytes_latest.unwrap_or(0).saturating_add(latest));
        }
        for (meter, quantity) in &other.other {
            let total = self.other.entry(meter.clone()).or_default();
            *total = total.saturating_add(*quantity);
        }
    }
}

/// Summarize a window's hourly buckets per namespace, ordered by tenant and
/// namespace. `carry_in` is each namespace's stored-bytes snapshot that was
/// current when the window opened (see [`STORED_CARRY_LOOKBACK_MS`]); it
/// prices searches made before the window's first snapshot and adds nothing
/// else. Totals saturate at `u64::MAX`.
pub fn summarize(buckets: &[UsageBucket], carry_in: &[UsageGauge]) -> Vec<NamespaceUsage> {
    #[derive(Default)]
    struct Hour {
        queries: u64,
        stored: Option<u64>,
    }
    #[derive(Default)]
    struct Acc {
        usage: NamespaceUsage,
        hours: BTreeMap<u64, Hour>,
    }
    let mut namespaces: BTreeMap<(String, String), Acc> = BTreeMap::new();
    for bucket in buckets {
        let acc = namespaces
            .entry((bucket.tenant.clone(), bucket.namespace.clone()))
            .or_default();
        let usage = &mut acc.usage;
        let q = bucket.quantity;
        match bucket.meter.as_str() {
            meter::QUERIES => {
                usage.queries = usage.queries.saturating_add(q);
                let hour = acc.hours.entry(bucket.hour_start_ms).or_default();
                hour.queries = hour.queries.saturating_add(q);
            }
            meter::BYTES_RETURNED => usage.bytes_returned = usage.bytes_returned.saturating_add(q),
            meter::BYTES_FETCHED => usage.bytes_fetched = usage.bytes_fetched.saturating_add(q),
            meter::BYTES_WRITTEN => usage.bytes_written = usage.bytes_written.saturating_add(q),
            meter::DOCUMENTS_WRITTEN => {
                usage.documents_written = usage.documents_written.saturating_add(q)
            }
            meter::VECTOR_DIMENSIONS_WRITTEN => {
                usage.vector_dimensions_written = usage.vector_dimensions_written.saturating_add(q)
            }
            meter::STORED_BYTES => {
                usage.stored_byte_hours = usage.stored_byte_hours.saturating_add(q);
                usage.stored_snapshots += 1;
                acc.hours.entry(bucket.hour_start_ms).or_default().stored = Some(q);
            }
            other => {
                let total = usage.other.entry(other.to_string()).or_default();
                *total = total.saturating_add(q);
            }
        }
    }
    let carried: BTreeMap<(&str, &str), u64> = carry_in
        .iter()
        .map(|g| ((g.tenant.as_str(), g.namespace.as_str()), g.quantity))
        .collect();
    namespaces
        .into_iter()
        .map(|((tenant, namespace), mut acc)| {
            let first_in_window = acc.hours.values().find_map(|hour| hour.stored);
            let mut size = carried
                .get(&(tenant.as_str(), namespace.as_str()))
                .copied()
                .or(first_in_window)
                .unwrap_or(0);
            for hour in acc.hours.values() {
                if let Some(stored) = hour.stored {
                    size = stored;
                    acc.usage.stored_bytes_latest = Some(stored);
                }
                acc.usage.bytes_queried = acc
                    .usage
                    .bytes_queried
                    .saturating_add(hour.queries.saturating_mul(size));
            }
            acc.usage.tenant = tenant;
            acc.usage.namespace = namespace;
            acc.usage
        })
        .collect()
}

/// Read a window from `store` and summarize it: [`UsageStore::hourly`] over
/// `[from_ms, to_ms)` plus the stored-bytes snapshots current at `from_ms`.
pub fn usage_summary(
    store: &dyn UsageStore,
    tenant: Option<&str>,
    from_ms: u64,
    to_ms: u64,
) -> Result<Vec<NamespaceUsage>, KoshaError> {
    let buckets = store.hourly(tenant, from_ms, to_ms)?;
    let carry_in = store.latest_between(
        tenant,
        meter::STORED_BYTES,
        from_ms.saturating_sub(STORED_CARRY_LOOKBACK_MS),
        from_ms,
    )?;
    Ok(summarize(&buckets, &carry_in))
}

#[derive(Debug, Clone)]
struct Row {
    tenant: String,
    namespace: String,
    meter: String,
    quantity: u64,
    at_ms: u64,
}

#[derive(Default)]
struct MemoryInner {
    seen: HashSet<(String, String)>,
    rows: Vec<Row>,
}

/// In-memory [`UsageStore`]: the default without Postgres, and what tests
/// use. Not durable and not shared between pods.
#[derive(Default)]
pub struct MemoryUsageStore {
    inner: Mutex<MemoryInner>,
}

impl MemoryUsageStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Meter rows stored.
    pub fn row_count(&self) -> usize {
        self.inner.lock().unwrap().rows.len()
    }
}

impl UsageStore for MemoryUsageStore {
    fn record(&self, events: &[UsageEvent]) -> Result<u64, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        let mut stored = 0;
        for event in events {
            for (meter, quantity) in &event.meters {
                if !inner.seen.insert((event.id.clone(), meter.clone())) {
                    continue;
                }
                inner.rows.push(Row {
                    tenant: event.tenant.clone(),
                    namespace: event.namespace.clone(),
                    meter: meter.clone(),
                    quantity: *quantity,
                    at_ms: event.at_ms,
                });
                stored += 1;
            }
        }
        Ok(stored)
    }

    fn hourly(
        &self,
        tenant: Option<&str>,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<UsageBucket>, KoshaError> {
        let inner = self.inner.lock().unwrap();
        let mut sums: BTreeMap<(String, String, u64, String), u64> = BTreeMap::new();
        for row in &inner.rows {
            if row.at_ms < from_ms || row.at_ms >= to_ms || tenant.is_some_and(|t| t != row.tenant)
            {
                continue;
            }
            let key = (
                row.tenant.clone(),
                row.namespace.clone(),
                row.at_ms / HOUR_MS * HOUR_MS,
                row.meter.clone(),
            );
            let total = sums.entry(key).or_default();
            *total = total.saturating_add(row.quantity);
        }
        Ok(sums
            .into_iter()
            .map(
                |((tenant, namespace, hour_start_ms, meter), quantity)| UsageBucket {
                    tenant,
                    namespace,
                    hour_start_ms,
                    meter,
                    quantity,
                },
            )
            .collect())
    }

    fn latest_between(
        &self,
        tenant: Option<&str>,
        meter: &str,
        from_ms: u64,
        to_ms: u64,
    ) -> Result<Vec<UsageGauge>, KoshaError> {
        let inner = self.inner.lock().unwrap();
        let mut latest: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
        for row in &inner.rows {
            if row.meter != meter
                || row.at_ms < from_ms
                || row.at_ms >= to_ms
                || tenant.is_some_and(|t| t != row.tenant)
            {
                continue;
            }
            let entry = latest
                .entry((row.tenant.clone(), row.namespace.clone()))
                .or_insert((row.at_ms, row.quantity));
            if row.at_ms >= entry.0 {
                *entry = (row.at_ms, row.quantity);
            }
        }
        Ok(latest
            .into_iter()
            .map(|((tenant, namespace), (_, quantity))| UsageGauge {
                tenant,
                namespace,
                quantity,
            })
            .collect())
    }
}

#[cfg(feature = "postgres")]
pub use pg::PgUsageStore;

#[cfg(feature = "postgres")]
mod pg {
    use super::{UsageBucket, UsageEvent, UsageGauge, UsageStore, HOUR_MS};
    use kosha_core::KoshaError;
    use std::time::Duration;

    /// Meter rows per `INSERT`. Six array parameters per statement whatever
    /// the batch size, so this only bounds the statement's memory.
    const INSERT_CHUNK_ROWS: usize = 5_000;

    fn control_plane_error(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    /// Postgres clamps: quantities and times are `BIGINT`.
    fn to_i64(value: u64) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }

    fn to_u64(value: i64) -> u64 {
        u64::try_from(value).unwrap_or(0)
    }

    /// `kosha.usage_events` (migration 005) behind its own small pool, like
    /// the writer-lease store: usage writes must not wait behind manifest
    /// traffic, nor the other way round.
    pub struct PgUsageStore {
        pool: sqlx::PgPool,
        rt: tokio::runtime::Runtime,
    }

    impl PgUsageStore {
        /// A store that connects on first use, so a Postgres outage at boot
        /// delays usage delivery (events wait in the pod's spool) instead
        /// of failing startup. Assumes `kosha.usage_events` exists:
        /// `PgStore::new` applies migration 005 at startup.
        pub fn new(database_url: &str) -> Result<Self, String> {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|e| format!("failed to create tokio runtime: {e}"))?;
            let pool = {
                let _guard = rt.enter();
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(2)
                    .acquire_timeout(Duration::from_secs(5))
                    .connect_lazy(database_url)
                    .map_err(|e| format!("invalid postgres url: {e}"))?
            };
            Ok(Self { pool, rt })
        }
    }

    impl UsageStore for PgUsageStore {
        fn record(&self, events: &[UsageEvent]) -> Result<u64, KoshaError> {
            let rows: Vec<(&UsageEvent, &String, u64)> = events
                .iter()
                .flat_map(|event| event.meters.iter().map(move |(m, q)| (event, m, *q)))
                .collect();
            if rows.is_empty() {
                return Ok(0);
            }
            self.rt.block_on(async {
                // One transaction: a batch is stored whole or not at all,
                // so the caller can retry it as a unit.
                let mut tx = self.pool.begin().await.map_err(control_plane_error)?;
                let mut stored = 0;
                for chunk in rows.chunks(INSERT_CHUNK_ROWS) {
                    let ids: Vec<&str> = chunk.iter().map(|(e, _, _)| e.id.as_str()).collect();
                    let meters: Vec<&str> = chunk.iter().map(|(_, m, _)| m.as_str()).collect();
                    let tenants: Vec<&str> =
                        chunk.iter().map(|(e, _, _)| e.tenant.as_str()).collect();
                    let namespaces: Vec<&str> =
                        chunk.iter().map(|(e, _, _)| e.namespace.as_str()).collect();
                    let quantities: Vec<i64> = chunk.iter().map(|(_, _, q)| to_i64(*q)).collect();
                    let times: Vec<i64> = chunk.iter().map(|(e, _, _)| to_i64(e.at_ms)).collect();
                    let result = sqlx::query(
                        "INSERT INTO kosha.usage_events \
                         (event_id, meter, tenant_id, namespace, quantity, occurred_at_ms) \
                         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], \
                                              $5::bigint[], $6::bigint[]) \
                         ON CONFLICT (event_id, meter) DO NOTHING",
                    )
                    .bind(&ids)
                    .bind(&meters)
                    .bind(&tenants)
                    .bind(&namespaces)
                    .bind(&quantities)
                    .bind(&times)
                    .execute(&mut *tx)
                    .await
                    .map_err(control_plane_error)?;
                    stored += result.rows_affected();
                }
                tx.commit().await.map_err(control_plane_error)?;
                Ok(stored)
            })
        }

        fn hourly(
            &self,
            tenant: Option<&str>,
            from_ms: u64,
            to_ms: u64,
        ) -> Result<Vec<UsageBucket>, KoshaError> {
            self.rt.block_on(async {
                let rows: Vec<(String, String, i64, String, i64)> = sqlx::query_as(
                    "SELECT tenant_id, namespace, occurred_at_ms / $4 AS hour, meter, \
                            LEAST(SUM(quantity), 9223372036854775807)::bigint \
                     FROM kosha.usage_events \
                     WHERE occurred_at_ms >= $1 AND occurred_at_ms < $2 \
                       AND ($3::text IS NULL OR tenant_id = $3) \
                     GROUP BY tenant_id, namespace, hour, meter \
                     ORDER BY tenant_id, namespace, hour, meter",
                )
                .bind(to_i64(from_ms))
                .bind(to_i64(to_ms))
                .bind(tenant)
                .bind(to_i64(HOUR_MS))
                .fetch_all(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(rows
                    .into_iter()
                    .map(|(tenant, namespace, hour, meter, quantity)| UsageBucket {
                        tenant,
                        namespace,
                        hour_start_ms: to_u64(hour).saturating_mul(HOUR_MS),
                        meter,
                        quantity: to_u64(quantity),
                    })
                    .collect())
            })
        }

        fn latest_between(
            &self,
            tenant: Option<&str>,
            meter: &str,
            from_ms: u64,
            to_ms: u64,
        ) -> Result<Vec<UsageGauge>, KoshaError> {
            self.rt.block_on(async {
                let rows: Vec<(String, String, i64)> = sqlx::query_as(
                    "SELECT DISTINCT ON (tenant_id, namespace) tenant_id, namespace, quantity \
                     FROM kosha.usage_events \
                     WHERE meter = $1 AND occurred_at_ms >= $2 AND occurred_at_ms < $3 \
                       AND ($4::text IS NULL OR tenant_id = $4) \
                     ORDER BY tenant_id, namespace, occurred_at_ms DESC, event_id DESC",
                )
                .bind(meter)
                .bind(to_i64(from_ms))
                .bind(to_i64(to_ms))
                .bind(tenant)
                .fetch_all(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(rows
                    .into_iter()
                    .map(|(tenant, namespace, quantity)| UsageGauge {
                        tenant,
                        namespace,
                        quantity: to_u64(quantity),
                    })
                    .collect())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: u64 = HOUR_MS;

    fn event(id: &str, ns: &str, at_ms: u64, meters: &[(&str, u64)]) -> UsageEvent {
        UsageEvent {
            id: id.into(),
            tenant: "acme".into(),
            namespace: ns.into(),
            at_ms,
            meters: meters.iter().map(|(m, q)| (m.to_string(), *q)).collect(),
        }
    }

    fn query(id: &str, ns: &str, at_ms: u64) -> UsageEvent {
        event(
            id,
            ns,
            at_ms,
            &[(meter::QUERIES, 1), (meter::BYTES_RETURNED, 100)],
        )
    }

    fn stored(ns: &str, hour: u64, bytes: u64) -> UsageEvent {
        event(
            &format!("stored/acme/{ns}/{hour}"),
            ns,
            hour * H,
            &[(meter::STORED_BYTES, bytes)],
        )
    }

    #[test]
    fn recording_the_same_events_twice_stores_them_once() {
        let store = MemoryUsageStore::new();
        let batch = vec![query("q1", "docs", 10), query("q2", "docs", 20)];
        assert_eq!(store.record(&batch).unwrap(), 4);
        // The replay after a crash: same ids, plus one event that is new.
        let mut replay = batch.clone();
        replay.push(query("q3", "docs", 30));
        assert_eq!(store.record(&replay).unwrap(), 2);
        assert_eq!(store.row_count(), 6);
        let usage = usage_summary(&store, Some("acme"), 0, H).unwrap();
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].queries, 3);
        assert_eq!(usage[0].bytes_returned, 300);
    }

    #[test]
    fn a_window_is_half_open_and_scoped_to_the_tenant() {
        let store = MemoryUsageStore::new();
        let mut other = query("o1", "docs", H);
        other.tenant = "globex".into();
        store
            .record(&[
                query("before", "docs", H - 1),
                query("first", "docs", H),
                query("last", "docs", 2 * H - 1),
                query("after", "docs", 2 * H),
                other,
            ])
            .unwrap();
        let acme = usage_summary(&store, Some("acme"), H, 2 * H).unwrap();
        assert_eq!(acme.len(), 1);
        assert_eq!((acme[0].tenant.as_str(), acme[0].queries), ("acme", 2));
        let all = usage_summary(&store, None, H, 2 * H).unwrap();
        assert_eq!(
            all.iter()
                .map(|u| (u.tenant.as_str(), u.queries))
                .collect::<Vec<_>>(),
            vec![("acme", 2), ("globex", 1)]
        );
    }

    #[test]
    fn bytes_queried_prices_each_search_at_the_snapshot_current_at_its_hour() {
        let store = MemoryUsageStore::new();
        store
            .record(&[
                // Hour 9 is before the window: its snapshot is the carry-in.
                stored("docs", 9, 1_000),
                query("a", "docs", 10 * H + 5),
                query("b", "docs", 10 * H + 6),
                stored("docs", 11, 4_000),
                query("c", "docs", 11 * H + 1),
                // Hour 12 has no snapshot: hour 11's still applies.
                query("d", "docs", 12 * H + 1),
            ])
            .unwrap();
        let usage = usage_summary(&store, Some("acme"), 10 * H, 13 * H).unwrap();
        let docs = &usage[0];
        assert_eq!(docs.queries, 4);
        assert_eq!(docs.bytes_queried, 2 * 1_000 + 4_000 + 4_000);
        // Only snapshots inside the window count as stored byte-hours.
        assert_eq!(docs.stored_byte_hours, 4_000);
        assert_eq!(docs.stored_snapshots, 1);
        assert_eq!(docs.stored_bytes_latest, Some(4_000));
    }

    #[test]
    fn searches_older_than_every_snapshot_use_the_first_one_after_them() {
        let store = MemoryUsageStore::new();
        store
            .record(&[
                query("a", "docs", 10 * H),
                stored("docs", 11, 700),
                query("b", "docs", 11 * H),
                // A namespace never snapshotted contributes no queried bytes.
                query("c", "scratch", 10 * H),
            ])
            .unwrap();
        let usage = usage_summary(&store, Some("acme"), 10 * H, 12 * H).unwrap();
        assert_eq!(usage[0].namespace, "docs");
        assert_eq!(usage[0].bytes_queried, 1_400);
        assert_eq!(usage[1].namespace, "scratch");
        assert_eq!((usage[1].queries, usage[1].bytes_queried), (1, 0));
        assert_eq!(usage[1].stored_bytes_latest, None);
    }

    #[test]
    fn the_carry_in_lookback_is_bounded_and_takes_the_latest_snapshot() {
        let store = MemoryUsageStore::new();
        let lookback_hours = STORED_CARRY_LOOKBACK_MS / H;
        let window = 1_000;
        store
            .record(&[
                stored("docs", window - 3, 10),
                stored("docs", window - 1, 30),
                // Older than the lookback: ignored.
                stored("old", window - lookback_hours - 1, 99),
                query("a", "docs", window * H),
                query("b", "old", window * H),
            ])
            .unwrap();
        let usage = usage_summary(&store, Some("acme"), window * H, (window + 1) * H).unwrap();
        assert_eq!(usage[0].bytes_queried, 30);
        assert_eq!(usage[1].bytes_queried, 0);
    }

    #[test]
    fn a_second_snapshot_of_the_same_hour_is_ignored() {
        let store = MemoryUsageStore::new();
        assert_eq!(store.record(&[stored("docs", 5, 100)]).unwrap(), 1);
        // Another pod (or this one after a restart) snapshots the same hour.
        assert_eq!(store.record(&[stored("docs", 5, 250)]).unwrap(), 0);
        let usage = usage_summary(&store, Some("acme"), 5 * H, 6 * H).unwrap();
        assert_eq!(usage[0].stored_byte_hours, 100);
        assert_eq!(usage[0].stored_snapshots, 1);
    }

    #[test]
    fn unknown_meters_are_kept_and_totals_add_up() {
        let store = MemoryUsageStore::new();
        store
            .record(&[
                event(
                    "w1",
                    "docs",
                    0,
                    &[(meter::BYTES_WRITTEN, 40), ("gpu_ms", 7)],
                ),
                event("w2", "logs", 0, &[(meter::DOCUMENTS_WRITTEN, 2)]),
                stored("docs", 0, 5),
                stored("logs", 0, 6),
            ])
            .unwrap();
        let usage = usage_summary(&store, None, 0, H).unwrap();
        let mut total = NamespaceUsage::default();
        for namespace in &usage {
            total.add(namespace);
        }
        assert_eq!(total.bytes_written, 40);
        assert_eq!(total.documents_written, 2);
        assert_eq!(total.other.get("gpu_ms"), Some(&7));
        assert_eq!(total.stored_bytes_latest, Some(11));
        assert_eq!(total.stored_byte_hours, 11);
    }

    #[test]
    fn totals_saturate_instead_of_wrapping() {
        let buckets = [
            UsageBucket {
                tenant: "acme".into(),
                namespace: "docs".into(),
                hour_start_ms: 0,
                meter: meter::STORED_BYTES.into(),
                quantity: u64::MAX / 2,
            },
            UsageBucket {
                tenant: "acme".into(),
                namespace: "docs".into(),
                hour_start_ms: 0,
                meter: meter::QUERIES.into(),
                quantity: 3,
            },
        ];
        assert_eq!(summarize(&buckets, &[])[0].bytes_queried, u64::MAX);
    }
}
