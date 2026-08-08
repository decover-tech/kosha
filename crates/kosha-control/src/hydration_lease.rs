//! Cross-replica hydration coordination.
//!
//! The architectural gap this closes: every kosha-query pod has its own
//! fully-independent local cache (in-memory segment cache + on-disk NVMe).
//! `kosha-server`'s per-process `in_flight_segments` registry (see
//! `ensure_segments_local` in main.rs) already coalesces concurrent requests
//! *within one pod* down to a single S3 GET per segment file. It does
//! nothing across pods — so N replicas hitting a cold namespace at once
//! still independently re-fetch the same segments from S3 at the same
//! time. That's what turned "one cold namespace" into "6 pods all hammering
//! the same S3 prefix simultaneously" during the staging outage this exists
//! to fix.
//!
//! This extends the same single-flight idea across the fleet, using a lease
//! row in Postgres — the control plane every query pod is already connected
//! to — as the coordination point:
//!
//!   1. Every pod that misses locally calls [`HydrationLeaseStore::try_claim`].
//!      Exactly one pod gets back [`HydrationLease::Owner`]; the rest get
//!      [`HydrationLease::OwnedBy`] the owner's own address.
//!   2. The owner fetches from S3 as before, then calls
//!      [`HydrationLeaseStore::release`].
//!   3. Waiters fetch the bytes directly from the owner pod over HTTP
//!      (`GET /internal/segment/...`, see kosha-server's main.rs) instead of
//!      touching S3 themselves — this is the actual request-volume win: a
//!      segment that's cold across the whole fleet now costs one S3 GET
//!      total, not one per replica.
//!
//! Fails open throughout: any Postgres error is treated as `Owner`, so a
//! coordination outage degrades to "every pod fetches its own copy" — i.e.
//! exactly today's behavior — never to a stuck or failed request.
//!
//! Deliberately runs its own small connection pool, separate from
//! `PgStore`'s. `PgStore` sits behind `AppState::controller`'s single
//! `Mutex` in kosha-server; a waiter blocked retrying `try_claim` while
//! holding that lock would serialize every manifest read/write in the
//! process behind lease contention, which would be a worse regression than
//! the S3 storm this is meant to fix.

use std::time::Duration;

/// Outcome of [`HydrationLeaseStore::try_claim`].
pub enum HydrationLease {
    /// This caller must fetch the segment itself — either a fresh claim, or
    /// a stale lease it just took over from a replica that crashed or
    /// stalled mid-fetch (see `expires_at`).
    Owner,
    /// Another replica already owns this fetch. Reachable at this address
    /// (`host:port`, as published via `try_claim`'s own `self_addr`).
    OwnedBy(String),
}

/// Postgres-backed cross-replica lease coordinator for segment hydration.
pub struct HydrationLeaseStore {
    pool: sqlx::PgPool,
    rt: tokio::runtime::Runtime,
}

impl HydrationLeaseStore {
    /// Connect a small dedicated pool. Assumes `kosha.hydration_leases`
    /// already exists — created by `PgStore::new`'s migration, which every
    /// query pod already runs at startup against the same database.
    pub fn new(database_url: &str) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| format!("failed to create tokio runtime: {e}"))?;

        // Lease claims are frequent, cheap, single-row upserts — not worth
        // competing with PgStore's own pool (or with other replicas) for
        // connections, hence a small pool of its own.
        let pool = rt
            .block_on(
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(3)
                    .acquire_timeout(Duration::from_secs(5))
                    .connect(database_url),
            )
            .map_err(|e| format!("failed to connect to postgres: {e}"))?;

        Ok(Self { pool, rt })
    }

    /// Atomically claim `segment_key` for `self_addr`, or learn who already
    /// owns it.
    ///
    /// One round trip: the CTE either inserts/refreshes the row (claim) or,
    /// when a live lease already exists, just reads it back — so there's no
    /// window between "check" and "claim" for two replicas to both believe
    /// they're the owner. `ttl` bounds how long a claim survives a crashed
    /// or stalled owner before another replica may take it over.
    pub fn try_claim(&self, segment_key: &str, self_addr: &str, ttl: Duration) -> HydrationLease {
        let key = segment_key.to_string();
        let addr = self_addr.to_string();
        let ttl_secs = ttl.as_secs_f64();

        let result: Result<Option<(String,)>, sqlx::Error> = self.rt.block_on(async {
            sqlx::query_as(
                "WITH upsert AS ( \
                    INSERT INTO kosha.hydration_leases (segment_key, owner_addr, expires_at) \
                    VALUES ($1, $2, now() + ($3 * interval '1 second')) \
                    ON CONFLICT (segment_key) DO UPDATE \
                        SET owner_addr = EXCLUDED.owner_addr, \
                            claimed_at = now(), \
                            expires_at = EXCLUDED.expires_at \
                        WHERE kosha.hydration_leases.expires_at < now() \
                    RETURNING owner_addr \
                 ) \
                 SELECT owner_addr FROM upsert \
                 UNION ALL \
                 SELECT owner_addr FROM kosha.hydration_leases \
                 WHERE segment_key = $1 AND NOT EXISTS (SELECT 1 FROM upsert) \
                 LIMIT 1",
            )
            .bind(&key)
            .bind(&addr)
            .bind(ttl_secs)
            .fetch_optional(&self.pool)
            .await
        });

        match result {
            Ok(Some((owner,))) if owner == self_addr => HydrationLease::Owner,
            Ok(Some((owner,))) => HydrationLease::OwnedBy(owner),
            // Reachable only via a race between the CTE's two branches (the
            // lease got deleted — i.e. released — between them):
            // vanishingly rare, and safe to treat as "nobody owns it right
            // now, so we do."
            Ok(None) => HydrationLease::Owner,
            Err(e) => {
                eprintln!(
                    "WARN: hydration lease claim failed for {segment_key}: {e}; fetching directly"
                );
                HydrationLease::Owner
            }
        }
    }

    /// Release a lease this replica owns, so a future miss on `segment_key`
    /// doesn't wait out the rest of the TTL.
    ///
    /// Best-effort and scoped to `self_addr`: only clears the row if we're
    /// still its recorded owner, so a lease that already expired and was
    /// reclaimed by someone else is never deleted out from under its new
    /// owner. A failure here just means the row expires naturally instead.
    pub fn release(&self, segment_key: &str, self_addr: &str) {
        let key = segment_key.to_string();
        let addr = self_addr.to_string();
        let result = self.rt.block_on(async {
            sqlx::query(
                "DELETE FROM kosha.hydration_leases WHERE segment_key = $1 AND owner_addr = $2",
            )
            .bind(&key)
            .bind(&addr)
            .execute(&self.pool)
            .await
        });
        if let Err(e) = result {
            eprintln!("WARN: failed to release hydration lease for {segment_key}: {e}");
        }
    }
}
