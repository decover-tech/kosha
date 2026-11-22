//! Usage metering per tenant and namespace (#348): the event model and the
//! durable store behind it.
//!
//! Pods count usage locally (`kosha-server/src/usage.rs`), seal each
//! counting window into [`UsageEvent`]s and [`UsageStore::append`] them.
//! Every event carries a deterministic `event_id` (see
//! [`UsageEvent::derive_id`]), and an append ignores ids it already holds,
//! so a flush retried after an error never counts twice. Monthly usage is
//! then the sum of the raw events, which [`UsageStore::aggregate`] computes
//! and [`UsageStore::raw`] lets anyone reproduce.
//!
//! Times are Unix seconds. An event belongs to a query range `[from, to)`
//! when its `window_start` falls in it.
//!
//! Two implementations: [`MemoryUsageStore`] (tests, single-node dev) and,
//! with the `postgres` feature, [`PgUsageStore`] over `kosha.usage_events`
//! (migration `006_usage_events.sql`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use kosha_core::KoshaError;
use serde::{Deserialize, Serialize};

/// One metered quantity. Counter meters add up whatever happened in their
/// window; snapshot meters sample a level once per snapshot period (hourly),
/// so their sum over a month is a level × hours figure (byte-hours).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Meter {
    /// Logical bytes stored (snapshot).
    StorageBytes,
    /// Request body bytes of accepted index/replace requests.
    WriteBytes,
    /// Documents accepted by index/replace requests.
    WriteDocs,
    /// Search and exists requests answered.
    Queries,
    /// Response body bytes of those queries.
    QueryBytesReturned,
    /// Bytes the queries scanned (an approximation; see the server).
    QueryBytesScanned,
    /// Vector dimensions × vectors stored (snapshot).
    VectorDimDocs,
}

impl Meter {
    pub const ALL: [Meter; 7] = [
        Meter::StorageBytes,
        Meter::WriteBytes,
        Meter::WriteDocs,
        Meter::Queries,
        Meter::QueryBytesReturned,
        Meter::QueryBytesScanned,
        Meter::VectorDimDocs,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Meter::StorageBytes => "storage_bytes",
            Meter::WriteBytes => "write_bytes",
            Meter::WriteDocs => "write_docs",
            Meter::Queries => "queries",
            Meter::QueryBytesReturned => "query_bytes_returned",
            Meter::QueryBytesScanned => "query_bytes_scanned",
            Meter::VectorDimDocs => "vector_dim_docs",
        }
    }

    pub fn parse(s: &str) -> Option<Meter> {
        Meter::ALL.into_iter().find(|m| m.as_str() == s)
    }

    /// A sampled level rather than a count of what happened.
    pub fn is_snapshot(self) -> bool {
        matches!(self, Meter::StorageBytes | Meter::VectorDimDocs)
    }
}

impl std::fmt::Display for Meter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One sealed meter window of one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    pub event_id: String,
    pub tenant: String,
    /// The namespace name within the tenant (no `<tenant>/` prefix).
    pub namespace: String,
    pub meter: Meter,
    pub quantity: u64,
    /// Unix seconds, inclusive.
    pub window_start: i64,
    /// Unix seconds, exclusive.
    pub window_end: i64,
    pub source_pod: String,
}

impl UsageEvent {
    /// The deterministic id of the event `source` emits for `(window,
    /// tenant, namespace, meter)`: 128 bits of two FNV-1a passes, as hex.
    /// Stable across processes and releases (unlike `DefaultHasher`), so a
    /// flush retried by the same pod produces the same ids.
    pub fn derive_id(
        source: &str,
        window_start: i64,
        window_end: i64,
        tenant: &str,
        namespace: &str,
        meter: Meter,
    ) -> String {
        let ws = window_start.to_string();
        let we = window_end.to_string();
        let parts: [&str; 6] = [source, &ws, &we, tenant, namespace, meter.as_str()];
        let hash = |seed: u64| {
            let mut h = seed;
            for part in parts {
                // Length-prefixed, so ("ab","c") and ("a","bc") differ.
                for b in (part.len() as u64)
                    .to_le_bytes()
                    .iter()
                    .chain(part.as_bytes())
                {
                    h ^= u64::from(*b);
                    h = h.wrapping_mul(0x0000_0100_0000_01b3);
                }
            }
            h
        };
        format!(
            "{:016x}{:016x}",
            hash(0xcbf2_9ce4_8422_2325),
            hash(0x6c62_272e_07bb_0142)
        )
    }
}

/// One aggregate total: a meter's summed quantity over a range, per
/// namespace (`namespace: Some`) or for the whole tenant (`None`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub meter: Meter,
    pub quantity: u64,
    /// How many events the total sums.
    pub events: u64,
}

/// Durable usage events.
pub trait UsageStore: Send + Sync {
    /// Insert `events`, skipping any whose `event_id` is already stored.
    /// Returns how many were new. All-or-nothing per call is not promised:
    /// a caller retries the whole batch, which the idempotency makes safe.
    fn append(&self, events: &[UsageEvent]) -> Result<usize, KoshaError>;

    /// Per-meter totals of `tenant`'s events with `window_start` in
    /// `[from, to)`, per namespace when `group_by_namespace`. Sorted by
    /// namespace, then meter.
    fn aggregate(
        &self,
        tenant: &str,
        from: i64,
        to: i64,
        group_by_namespace: bool,
    ) -> Result<Vec<UsageRow>, KoshaError>;

    /// `tenant`'s raw events with `window_start` in `[from, to)`, ordered
    /// by `window_start`, then `event_id`.
    fn raw(&self, tenant: &str, from: i64, to: i64) -> Result<Vec<UsageEvent>, KoshaError>;

    /// The latest event of `meter` per `(tenant, namespace)` (by
    /// `window_start`), for every tenant or just `tenant`. Reconciliation
    /// compares the latest `storage_bytes` snapshot with the bucket.
    fn latest(&self, tenant: Option<&str>, meter: Meter) -> Result<Vec<UsageEvent>, KoshaError>;
}

/// Sum `events` into rows the way [`UsageStore::aggregate`] does.
pub fn aggregate_events<'a>(
    events: impl IntoIterator<Item = &'a UsageEvent>,
    group_by_namespace: bool,
) -> Vec<UsageRow> {
    let mut totals: BTreeMap<(Option<String>, Meter), (u64, u64)> = BTreeMap::new();
    for e in events {
        let ns = group_by_namespace.then(|| e.namespace.clone());
        let t = totals.entry((ns, e.meter)).or_default();
        t.0 = t.0.saturating_add(e.quantity);
        t.1 += 1;
    }
    totals
        .into_iter()
        .map(|((namespace, meter), (quantity, events))| UsageRow {
            namespace,
            meter,
            quantity,
            events,
        })
        .collect()
}

/// In-memory [`UsageStore`]: tests and single-node dev (`KOSHA_USAGE=memory`).
#[derive(Default)]
pub struct MemoryUsageStore {
    inner: Mutex<MemoryInner>,
}

#[derive(Default)]
struct MemoryInner {
    by_id: HashMap<String, usize>,
    events: Vec<UsageEvent>,
}

impl MemoryUsageStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every stored event, in insertion order.
    pub fn all(&self) -> Vec<UsageEvent> {
        self.inner.lock().unwrap().events.clone()
    }
}

impl UsageStore for MemoryUsageStore {
    fn append(&self, events: &[UsageEvent]) -> Result<usize, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        let mut added = 0;
        for e in events {
            if inner.by_id.contains_key(&e.event_id) {
                continue;
            }
            let at = inner.events.len();
            inner.by_id.insert(e.event_id.clone(), at);
            inner.events.push(e.clone());
            added += 1;
        }
        Ok(added)
    }

    fn aggregate(
        &self,
        tenant: &str,
        from: i64,
        to: i64,
        group_by_namespace: bool,
    ) -> Result<Vec<UsageRow>, KoshaError> {
        let inner = self.inner.lock().unwrap();
        Ok(aggregate_events(
            inner
                .events
                .iter()
                .filter(|e| e.tenant == tenant && e.window_start >= from && e.window_start < to),
            group_by_namespace,
        ))
    }

    fn raw(&self, tenant: &str, from: i64, to: i64) -> Result<Vec<UsageEvent>, KoshaError> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<UsageEvent> = inner
            .events
            .iter()
            .filter(|e| e.tenant == tenant && e.window_start >= from && e.window_start < to)
            .cloned()
            .collect();
        out.sort_by(|a, b| (a.window_start, &a.event_id).cmp(&(b.window_start, &b.event_id)));
        Ok(out)
    }

    fn latest(&self, tenant: Option<&str>, meter: Meter) -> Result<Vec<UsageEvent>, KoshaError> {
        let inner = self.inner.lock().unwrap();
        let mut latest: BTreeMap<(String, String), &UsageEvent> = BTreeMap::new();
        for e in &inner.events {
            if e.meter != meter || tenant.is_some_and(|t| t != e.tenant) {
                continue;
            }
            let key = (e.tenant.clone(), e.namespace.clone());
            match latest.get(&key) {
                Some(cur) if (cur.window_start, &cur.event_id) >= (e.window_start, &e.event_id) => {
                }
                _ => {
                    latest.insert(key, e);
                }
            }
        }
        Ok(latest.into_values().cloned().collect())
    }
}

#[cfg(feature = "postgres")]
pub use pg::PgUsageStore;

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use std::time::Duration;

    fn control_plane_error(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    /// Rows per `INSERT`: keeps each statement's arrays modest.
    const APPEND_CHUNK: usize = 1000;

    const APPEND: &str = "INSERT INTO kosha.usage_events \
         (event_id, tenant, namespace, meter, quantity, window_start, window_end, source_pod) \
         SELECT e, t, n, m, q, to_timestamp(ws), to_timestamp(we), p \
         FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[], \
                     $6::bigint[], $7::bigint[], $8::text[]) AS u(e, t, n, m, q, ws, we, p) \
         ON CONFLICT (event_id) DO NOTHING";

    const COLUMNS: &str = "event_id, tenant, namespace, meter, quantity, \
         extract(epoch from window_start)::bigint, extract(epoch from window_end)::bigint, \
         source_pod";

    type Row = (String, String, String, String, i64, i64, i64, String);

    fn event_of(row: Row) -> Option<UsageEvent> {
        let (event_id, tenant, namespace, meter, quantity, ws, we, source_pod) = row;
        Some(UsageEvent {
            event_id,
            tenant,
            namespace,
            meter: Meter::parse(&meter)?,
            quantity: quantity.max(0) as u64,
            window_start: ws,
            window_end: we,
            source_pod,
        })
    }

    /// [`UsageStore`] over `kosha.usage_events`. Like
    /// [`HydrationLeaseStore`](crate::HydrationLeaseStore) it runs its own
    /// small pool rather than sharing `PgStore`'s, which sits behind the
    /// server's control-store mutex: a usage flush stuck on a slow database
    /// must not hold up manifest reads. Assumes the table exists (created by
    /// `PgStore::new`'s migration).
    pub struct PgUsageStore {
        pool: sqlx::PgPool,
        rt: tokio::runtime::Runtime,
    }

    impl PgUsageStore {
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
                        .acquire_timeout(Duration::from_secs(10))
                        .connect(database_url),
                )
                .map_err(|e| format!("failed to connect to postgres: {e}"))?;
            Ok(Self { pool, rt })
        }
    }

    impl UsageStore for PgUsageStore {
        fn append(&self, events: &[UsageEvent]) -> Result<usize, KoshaError> {
            let mut added = 0usize;
            for chunk in events.chunks(APPEND_CHUNK) {
                let col = |f: fn(&UsageEvent) -> String| chunk.iter().map(f).collect::<Vec<_>>();
                let ids = col(|e| e.event_id.clone());
                let tenants = col(|e| e.tenant.clone());
                let namespaces = col(|e| e.namespace.clone());
                let meters = col(|e| e.meter.as_str().to_string());
                let pods = col(|e| e.source_pod.clone());
                let quantities: Vec<i64> = chunk
                    .iter()
                    .map(|e| i64::try_from(e.quantity).unwrap_or(i64::MAX))
                    .collect();
                let starts: Vec<i64> = chunk.iter().map(|e| e.window_start).collect();
                let ends: Vec<i64> = chunk.iter().map(|e| e.window_end).collect();
                let done = self
                    .rt
                    .block_on(
                        sqlx::query(APPEND)
                            .bind(ids)
                            .bind(tenants)
                            .bind(namespaces)
                            .bind(meters)
                            .bind(quantities)
                            .bind(starts)
                            .bind(ends)
                            .bind(pods)
                            .execute(&self.pool),
                    )
                    .map_err(control_plane_error)?;
                added += done.rows_affected() as usize;
            }
            Ok(added)
        }

        fn aggregate(
            &self,
            tenant: &str,
            from: i64,
            to: i64,
            group_by_namespace: bool,
        ) -> Result<Vec<UsageRow>, KoshaError> {
            let ns = if group_by_namespace {
                "namespace"
            } else {
                "NULL::text"
            };
            let sql = format!(
                "SELECT {ns}, meter, COALESCE(SUM(quantity), 0)::bigint, COUNT(*)::bigint \
                 FROM kosha.usage_events \
                 WHERE tenant = $1 AND window_start >= to_timestamp($2) \
                   AND window_start < to_timestamp($3) \
                 GROUP BY 1, 2 ORDER BY 1, 2"
            );
            let rows: Vec<(Option<String>, String, i64, i64)> = self
                .rt
                .block_on(
                    sqlx::query_as(&sql)
                        .bind(tenant)
                        .bind(from as f64)
                        .bind(to as f64)
                        .fetch_all(&self.pool),
                )
                .map_err(control_plane_error)?;
            let mut out: Vec<UsageRow> = rows
                .into_iter()
                .filter_map(|(namespace, meter, quantity, events)| {
                    Some(UsageRow {
                        namespace,
                        meter: Meter::parse(&meter)?,
                        quantity: quantity.max(0) as u64,
                        events: events.max(0) as u64,
                    })
                })
                .collect();
            // Same order as the in-memory store (enum order, not text).
            out.sort_by(|a, b| (&a.namespace, a.meter).cmp(&(&b.namespace, b.meter)));
            Ok(out)
        }

        fn raw(&self, tenant: &str, from: i64, to: i64) -> Result<Vec<UsageEvent>, KoshaError> {
            let sql = format!(
                "SELECT {COLUMNS} FROM kosha.usage_events \
                 WHERE tenant = $1 AND window_start >= to_timestamp($2) \
                   AND window_start < to_timestamp($3) \
                 ORDER BY window_start, event_id"
            );
            let rows: Vec<Row> = self
                .rt
                .block_on(
                    sqlx::query_as(&sql)
                        .bind(tenant)
                        .bind(from as f64)
                        .bind(to as f64)
                        .fetch_all(&self.pool),
                )
                .map_err(control_plane_error)?;
            Ok(rows.into_iter().filter_map(event_of).collect())
        }

        fn latest(
            &self,
            tenant: Option<&str>,
            meter: Meter,
        ) -> Result<Vec<UsageEvent>, KoshaError> {
            let sql = format!(
                "SELECT DISTINCT ON (tenant, namespace) {COLUMNS} FROM kosha.usage_events \
                 WHERE meter = $1 AND ($2::text IS NULL OR tenant = $2) \
                 ORDER BY tenant, namespace, window_start DESC, event_id DESC"
            );
            let rows: Vec<Row> = self
                .rt
                .block_on(
                    sqlx::query_as(&sql)
                        .bind(meter.as_str())
                        .bind(tenant)
                        .fetch_all(&self.pool),
                )
                .map_err(control_plane_error)?;
            Ok(rows.into_iter().filter_map(event_of).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(tenant: &str, ns: &str, meter: Meter, quantity: u64, ws: i64) -> UsageEvent {
        UsageEvent {
            event_id: UsageEvent::derive_id("pod-a", ws, ws + 60, tenant, ns, meter),
            tenant: tenant.into(),
            namespace: ns.into(),
            meter,
            quantity,
            window_start: ws,
            window_end: ws + 60,
            source_pod: "pod-a".into(),
        }
    }

    #[test]
    fn meter_names_round_trip() {
        for m in Meter::ALL {
            assert_eq!(Meter::parse(m.as_str()), Some(m));
            assert_eq!(
                serde_json::to_string(&m).unwrap(),
                format!("\"{}\"", m.as_str())
            );
        }
        assert!(Meter::StorageBytes.is_snapshot());
        assert!(Meter::VectorDimDocs.is_snapshot());
        assert!(!Meter::Queries.is_snapshot());
        assert_eq!(Meter::parse("nope"), None);
    }

    #[test]
    fn derive_id_is_deterministic_and_distinguishes_every_part() {
        let base = UsageEvent::derive_id("p", 0, 60, "t", "n", Meter::Queries);
        assert_eq!(base.len(), 32);
        assert_eq!(
            base,
            UsageEvent::derive_id("p", 0, 60, "t", "n", Meter::Queries)
        );
        let variants = [
            UsageEvent::derive_id("q", 0, 60, "t", "n", Meter::Queries),
            UsageEvent::derive_id("p", 60, 120, "t", "n", Meter::Queries),
            UsageEvent::derive_id("p", 0, 61, "t", "n", Meter::Queries),
            UsageEvent::derive_id("p", 0, 60, "u", "n", Meter::Queries),
            UsageEvent::derive_id("p", 0, 60, "t", "m", Meter::Queries),
            UsageEvent::derive_id("p", 0, 60, "t", "n", Meter::WriteDocs),
            // Concatenation-ambiguous parts.
            UsageEvent::derive_id("p", 0, 60, "tn", "", Meter::Queries),
        ];
        for v in variants {
            assert_ne!(v, base);
        }
    }

    #[test]
    fn re_append_is_idempotent() {
        let store = MemoryUsageStore::new();
        let batch = vec![
            event("acme", "docs", Meter::Queries, 3, 0),
            event("acme", "docs", Meter::WriteBytes, 100, 0),
        ];
        assert_eq!(store.append(&batch).unwrap(), 2);
        // The same batch again (a retried flush), and one overlapping it.
        assert_eq!(store.append(&batch).unwrap(), 0);
        let mut overlap = batch.clone();
        overlap.push(event("acme", "docs", Meter::Queries, 5, 60));
        assert_eq!(store.append(&overlap).unwrap(), 1);
        let rows = store.aggregate("acme", 0, 120, false).unwrap();
        let queries = rows.iter().find(|r| r.meter == Meter::Queries).unwrap();
        assert_eq!((queries.quantity, queries.events), (8, 2));
        assert_eq!(store.all().len(), 3);
    }

    #[test]
    fn month_of_events_aggregates_to_the_sum_of_raw_events() {
        let store = MemoryUsageStore::new();
        let month_start = 1_767_225_600; // 2026-01-01T00:00:00Z
        let month_end = month_start + 31 * 86_400;
        // A deterministic pseudo-random month: two tenants, three
        // namespaces, every meter, hourly windows, plus events either side
        // of the month that must not count.
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut batch = Vec::new();
        for hour in -2..(31 * 24 + 2) {
            let ws = month_start + hour * 3600;
            for tenant in ["acme", "globex"] {
                for ns in ["a", "b", "c"] {
                    for meter in Meter::ALL {
                        if next() % 3 == 0 {
                            continue;
                        }
                        batch.push(event(tenant, ns, meter, next() % 1_000_000, ws));
                    }
                }
            }
            if batch.len() > 5000 {
                store.append(&batch).unwrap();
                // Re-appending part of it changes nothing.
                store.append(&batch[..100]).unwrap();
                batch.clear();
            }
        }
        store.append(&batch).unwrap();

        let raw = store.raw("acme", month_start, month_end).unwrap();
        assert!(raw.iter().all(|e| e.tenant == "acme"
            && e.window_start >= month_start
            && e.window_start < month_end));
        assert!(raw
            .windows(2)
            .all(|w| w[0].window_start <= w[1].window_start));
        for group in [false, true] {
            let rows = store
                .aggregate("acme", month_start, month_end, group)
                .unwrap();
            let mut expected: BTreeMap<(Option<String>, Meter), u64> = BTreeMap::new();
            for e in &raw {
                *expected
                    .entry((group.then(|| e.namespace.clone()), e.meter))
                    .or_default() += e.quantity;
            }
            let got: BTreeMap<(Option<String>, Meter), u64> = rows
                .iter()
                .map(|r| ((r.namespace.clone(), r.meter), r.quantity))
                .collect();
            assert_eq!(got, expected);
            assert_eq!(rows.iter().map(|r| r.events).sum::<u64>(), raw.len() as u64);
        }
        // The other tenant's events never leak in.
        assert!(store
            .raw("acme", month_start, month_end)
            .unwrap()
            .iter()
            .all(|e| e.tenant != "globex"));
    }

    #[test]
    fn latest_picks_the_newest_snapshot_per_namespace() {
        let store = MemoryUsageStore::new();
        store
            .append(&[
                event("acme", "a", Meter::StorageBytes, 10, 0),
                event("acme", "a", Meter::StorageBytes, 30, 7200),
                event("acme", "a", Meter::StorageBytes, 20, 3600),
                event("acme", "b", Meter::StorageBytes, 5, 0),
                event("acme", "b", Meter::Queries, 99, 9000),
                event("globex", "a", Meter::StorageBytes, 1, 0),
            ])
            .unwrap();
        let latest = store.latest(Some("acme"), Meter::StorageBytes).unwrap();
        let got: Vec<(&str, u64)> = latest
            .iter()
            .map(|e| (e.namespace.as_str(), e.quantity))
            .collect();
        assert_eq!(got, vec![("a", 30), ("b", 5)]);
        assert_eq!(store.latest(None, Meter::StorageBytes).unwrap().len(), 3);
    }
}
