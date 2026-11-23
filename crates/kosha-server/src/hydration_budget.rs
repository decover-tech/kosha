//! Byte-level accounting and backpressure for S3 hydration.
//!
//! Every pre-existing hydration knob bounds *file count*, not volume:
//! `KOSHA_HYDRATE_CONCURRENCY` / `KOSHA_SCORING_HYDRATE_CONCURRENCY` bound
//! GETs in flight per batch, `KOSHA_MAX_CONCURRENT_HYDRATIONS` bounds
//! independent batches. `KOSHA_HYDRATE_BYTE_BUDGET` does bound bytes, but
//! only where sizes are known *before* the fetch — i.e. the
//! `list_with_sizes`-backed full-segment path. The scoring path (the one a
//! cold `/search` actually takes) deliberately skips that LIST round trip
//! and passes size `0` for every file, so `chunk_by_byte_budget` puts the
//! whole batch in one chunk and the budget never binds. That is how a single
//! cold search logged `fetched=177 bytes=4974.1MB concurrency=64`.
//!
//! Two complementary mechanisms now cover the two cases:
//!   * `chunk_by_byte_budget` (in `main.rs`) — **pre-GET**, needs sizes up
//!     front, used where a LIST already supplied them.
//!   * [`HydrationBudget`] here — **post-send**, learns each object's size
//!     from the GET response's `Content-Length`, so it works on exactly the
//!     path that has no sizes a priori.
//!
//! What the in-flight cap bounds, precisely: concurrent transfer + local
//! write volume, not process heap. Heap is bounded by streaming the response
//! body straight to disk (see `s3_storage::fetch_one`) — a bound by
//! construction, which is the actual fix for the OOM. This cap is what keeps
//! 4 hydration ops × 64-way fan-out from queueing an unbounded number of
//! simultaneous multi-hundred-MB transfers against one pod's disk and NIC.
//!
//! Tenants (#342): the budget is also accounted per tenant (the first
//! component of each object's namespace), with a per-tenant cap
//! (`KOSHA_HYDRATE_TENANT_MAX_INFLIGHT_BYTES`, by default half the global
//! budget), so one tenant's cold start cannot hold all of the pod's S3
//! bandwidth while another tenant's fetches wait behind it. The cap is
//! work-conserving: it binds only while some other tenant holds or waits
//! for budget, so a single-tenant pod (or one serving only pre-#262 bare
//! namespaces, which all share `_legacy`) keeps the whole budget.
//!
//! [`RequestHydration`] is the per-request half: a running total that lets a
//! request whose hydration set is absurd stop and shed (HTTP 429) instead of
//! grinding through gigabytes while holding a hydration permit.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Default ceiling on the bytes one search request may hydrate before it is
/// shed. Deliberately well above a healthy cold search's scoring set (tens
/// to low hundreds of MB) and well below the multi-GB pull that motivated
/// this module, so it fires on the pathological case only.
pub const DEFAULT_REQUEST_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// How long a fetch waits for in-flight hydration bytes to free up before
/// its request is shed. Kept short: the waiter is holding an already-sent
/// GET's response open, so blocking for minutes would trade a memory
/// problem for a connection-timeout problem.
pub const DEFAULT_BUDGET_WAIT: Duration = Duration::from_secs(30);

/// Default per-tenant in-flight cap, as a fraction of the global budget:
/// one tenant may hold half the pod's hydration bandwidth.
pub const DEFAULT_TENANT_FRACTION: f64 = 0.5;

/// `KOSHA_HYDRATE_TENANT_MAX_INFLIGHT_BYTES`, else
/// [`DEFAULT_TENANT_FRACTION`] of `capacity`.
pub fn tenant_cap_from_env(capacity: u64) -> u64 {
    std::env::var("KOSHA_HYDRATE_TENANT_MAX_INFLIGHT_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or((capacity as f64 * DEFAULT_TENANT_FRACTION) as u64)
}

/// One tenant's share of [`BudgetState`].
#[derive(Default)]
struct TenantInFlight {
    bytes: u64,
    holders: usize,
}

struct BudgetState {
    /// Bytes reserved by fetches that have been admitted and are still
    /// streaming to disk.
    in_flight: u64,
    /// Number of live permits. The anti-starvation rule keys off this —
    /// see [`HydrationBudget::acquire`].
    holders: usize,
    /// In-flight bytes and permits per tenant; a tenant leaves the map
    /// when its last permit drops.
    tenants: HashMap<String, TenantInFlight>,
    /// Byte requests of the fetches waiting in [`HydrationBudget::acquire`],
    /// per tenant; a tenant leaves the map when its last waiter is admitted
    /// or gives up.
    waiting: HashMap<String, Vec<u64>>,
}

impl BudgetState {
    /// Whether a tenant other than `tenant` holds a permit.
    fn other_tenant_holding(&self, tenant: &str) -> bool {
        self.tenants.keys().any(|t| t != tenant)
    }

    /// Whether a tenant other than `tenant` is waiting for budget.
    fn other_tenant_waiting(&self, tenant: &str) -> bool {
        self.waiting.keys().any(|t| t != tenant)
    }

    fn add_waiter(&mut self, tenant: &str, bytes: u64) {
        self.waiting
            .entry(tenant.to_string())
            .or_default()
            .push(bytes);
    }

    fn remove_waiter(&mut self, tenant: &str, bytes: u64) {
        if let Some(requests) = self.waiting.get_mut(tenant) {
            if let Some(i) = requests.iter().position(|b| *b == bytes) {
                requests.swap_remove(i);
            }
            if requests.is_empty() {
                self.waiting.remove(tenant);
            }
        }
    }

    /// Whether `tenant`'s own cap (`cap`) lets it take `bytes` more. See
    /// [`HydrationBudget::acquire`].
    fn tenant_ok(&self, tenant: &str, bytes: u64, cap: u64) -> bool {
        let (tenant_bytes, tenant_holders) = self
            .tenants
            .get(tenant)
            .map_or((0, 0), |t| (t.bytes, t.holders));
        let others_waiting = self.other_tenant_waiting(tenant);
        tenant_bytes.saturating_add(bytes) <= cap
            || tenant_holders == 0
            || !(others_waiting || self.other_tenant_holding(tenant))
            || (bytes > cap && !others_waiting)
    }

    /// Bytes to keep free for other tenants' waiters before `tenant`, which
    /// already holds budget, may take more: the largest, over the other
    /// waiting tenants, of each one's smallest request its own cap allows
    /// (a waiter held back by its own cap needs no room kept for it).
    fn reserve_for_others(&self, tenant: &str, cap: u64) -> u64 {
        self.waiting
            .iter()
            .filter(|(t, _)| t.as_str() != tenant)
            .filter_map(|(t, requests)| {
                requests
                    .iter()
                    .copied()
                    .filter(|b| self.tenant_ok(t, *b, cap))
                    .min()
            })
            .max()
            .unwrap_or(0)
    }
}

/// Process-wide cap on hydration bytes in flight at once.
pub struct HydrationBudget {
    state: Mutex<BudgetState>,
    /// Wakes waiters in [`Self::acquire`] when budget frees or a waiter
    /// leaves.
    freed: tokio::sync::Notify,
    capacity: u64,
    /// Most bytes one tenant may have in flight (#342).
    tenant_capacity: u64,
    wait_timeout: Duration,
}

impl HydrationBudget {
    /// A budget whose per-tenant cap is
    /// `KOSHA_HYDRATE_TENANT_MAX_INFLIGHT_BYTES` (default half of
    /// `capacity`).
    pub fn new(capacity: u64, wait_timeout: Duration) -> Self {
        Self::with_tenant_cap(capacity, tenant_cap_from_env(capacity), wait_timeout)
    }

    pub fn with_tenant_cap(capacity: u64, tenant_capacity: u64, wait_timeout: Duration) -> Self {
        Self {
            state: Mutex::new(BudgetState {
                in_flight: 0,
                holders: 0,
                tenants: HashMap::new(),
                waiting: HashMap::new(),
            }),
            freed: tokio::sync::Notify::new(),
            capacity: capacity.max(1),
            tenant_capacity: tenant_capacity.max(1),
            wait_timeout,
        }
    }

    /// Reserve `bytes` for `tenant` until the returned permit drops,
    /// waiting while the reservation would exceed the global capacity or
    /// the tenant's cap.
    ///
    /// The wait is async: callers run on the S3 runtime's few worker
    /// threads, and the permit holders whose release would admit a waiter
    /// stream their response bodies on those same workers. A blocking wait
    /// (this used to be a `Condvar`) parked a worker per waiter, so with
    /// as many waiters as workers no holder could make progress, and every
    /// waiter timed out together while the runtime stood still.
    ///
    /// Returns [`BudgetExpired`] if nothing freed within `wait_timeout` —
    /// the caller abandons that fetch and sheds the request rather than
    /// waiting unboundedly with a live S3 response open. Dropping the
    /// future (a cancelled task) withdraws the wait.
    ///
    /// Anti-starvation, mirroring `kosha_query::MemoryLedger::admit`: when
    /// no other permit is outstanding, a reservation is granted however
    /// large. A single object bigger than the whole budget (a
    /// several-hundred-MB `doc_store.bin` under a small configured budget)
    /// must degrade to "fetched alone", never to "never fetched". The same
    /// rule applies per tenant: a tenant with nothing in flight is never
    /// held back by its own cap, only by the global one.
    ///
    /// The tenant cap is work-conserving: it is waived while no other
    /// tenant holds or waits for budget (there is nobody to protect), and a
    /// single object larger than the cap itself — which could otherwise
    /// only ever be admitted with its tenant's in-flight count at zero,
    /// racing that tenant's own smaller fetches — is waived while no other
    /// tenant is waiting.
    ///
    /// Freed budget goes to a waiting tenant first: while another tenant
    /// waits (for a request its own cap allows), a tenant that already
    /// holds budget is admitted only if the global budget still has room
    /// for that waiter afterwards. Without this, a tenant refilling its cap
    /// with many small fetches re-takes every freed byte, and a waiter with
    /// a larger object (or any waiter, when two heavy tenants together fill
    /// the budget) times out and is shed.
    pub async fn acquire(
        self: &Arc<Self>,
        tenant: &str,
        bytes: u64,
    ) -> Result<BudgetPermit, BudgetExpired> {
        let deadline = tokio::time::Instant::now() + self.wait_timeout;
        let mut registration = Registration {
            budget: self,
            tenant,
            bytes,
            active: false,
        };
        loop {
            // Created before the check (one atomic load, nothing
            // registered), enabled under the lock below: a release that
            // frees budget after this check finds us in `waiting` and wakes
            // us, so no wakeup is lost between the check and the await.
            let freed = self.freed.notified();
            tokio::pin!(freed);
            {
                let mut st = self.state.lock().unwrap();
                if self.admissible(&st, tenant, bytes) {
                    if registration.active {
                        registration.active = false;
                        st.remove_waiter(tenant, bytes);
                        // Leaving `waiting` can lift the cap or the reserve
                        // that held another tenant's waiter back.
                        if !st.waiting.is_empty() {
                            self.freed.notify_waiters();
                        }
                    }
                    st.in_flight = st.in_flight.saturating_add(bytes);
                    st.holders += 1;
                    let t = st.tenants.entry(tenant.to_string()).or_default();
                    t.bytes = t.bytes.saturating_add(bytes);
                    t.holders += 1;
                    return Ok(BudgetPermit {
                        budget: Arc::clone(self),
                        tenant: tenant.to_string(),
                        bytes,
                    });
                }
                if tokio::time::Instant::now() >= deadline {
                    if registration.active {
                        registration.active = false;
                        st.remove_waiter(tenant, bytes);
                        // Our wait may have been what held another tenant's
                        // fetch to its cap.
                        if !st.waiting.is_empty() {
                            self.freed.notify_waiters();
                        }
                    }
                    let tenant_bytes = st.tenants.get(tenant).map_or(0, |t| t.bytes);
                    return Err(BudgetExpired {
                        in_flight: st.in_flight,
                        capacity: self.capacity,
                        tenant_in_flight: tenant_bytes,
                        tenant_capacity: self.tenant_capacity,
                        holders: st.holders,
                        waiters: st.waiting.values().map(Vec::len).sum(),
                    });
                }
                if !registration.active {
                    st.add_waiter(tenant, bytes);
                    registration.active = true;
                }
                freed.as_mut().enable();
            }
            // Timing out just re-runs the check once more, then expires.
            let _ = tokio::time::timeout_at(deadline, freed).await;
        }
    }

    /// Whether `bytes` for `tenant` may be admitted now. See
    /// [`Self::acquire`].
    fn admissible(&self, st: &BudgetState, tenant: &str, bytes: u64) -> bool {
        let fits = st.in_flight.saturating_add(bytes) <= self.capacity;
        let tenant_holders = st.tenants.get(tenant).map_or(0, |t| t.holders);
        let tenant_ok = st.tenant_ok(tenant, bytes, self.tenant_capacity);
        let leaves_room = tenant_holders == 0 || {
            let reserve = st.reserve_for_others(tenant, self.tenant_capacity);
            st.in_flight.saturating_add(bytes).saturating_add(reserve) <= self.capacity
        };
        (fits || st.holders == 0) && tenant_ok && leaves_room
    }

    fn release(&self, tenant: &str, bytes: u64) {
        let mut st = self.state.lock().unwrap();
        st.in_flight = st.in_flight.saturating_sub(bytes);
        st.holders = st.holders.saturating_sub(1);
        if let Some(t) = st.tenants.get_mut(tenant) {
            t.bytes = t.bytes.saturating_sub(bytes);
            t.holders = t.holders.saturating_sub(1);
            if t.holders == 0 {
                st.tenants.remove(tenant);
            }
        }
        // Every waiter registers in `waiting` under this lock, so an empty
        // map means nobody to wake — the uncontended release skips it.
        let waiters = !st.waiting.is_empty();
        drop(st);
        if waiters {
            self.freed.notify_waiters();
        }
    }

    /// Bytes currently reserved. Test-only: the runtime reports budget
    /// pressure as `budget_wait_ms` on the `hydrate_files timing:` line,
    /// which is the number worth acting on — an instantaneous reservation
    /// total sampled from outside the lock says little.
    #[cfg(test)]
    pub fn in_flight(&self) -> u64 {
        self.state.lock().unwrap().in_flight
    }

    /// Bytes `tenant` has reserved. Test-only, like [`Self::in_flight`].
    #[cfg(test)]
    pub fn tenant_in_flight(&self, tenant: &str) -> u64 {
        self.state
            .lock()
            .unwrap()
            .tenants
            .get(tenant)
            .map_or(0, |t| t.bytes)
    }
}

/// A wait in [`HydrationBudget::acquire`] still listed in `waiting`;
/// dropping it (the acquiring task was cancelled mid-wait) withdraws it,
/// so a vanished waiter never keeps holding other tenants back.
struct Registration<'a> {
    budget: &'a HydrationBudget,
    tenant: &'a str,
    bytes: u64,
    active: bool,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut st = self.budget.state.lock().unwrap();
        st.remove_waiter(self.tenant, self.bytes);
        let waiters = !st.waiting.is_empty();
        drop(st);
        if waiters {
            self.budget.freed.notify_waiters();
        }
    }
}

/// The budget's state when a wait in [`HydrationBudget::acquire`] expired,
/// for the log line that says which limit held the fetch back.
#[derive(Debug)]
pub struct BudgetExpired {
    pub in_flight: u64,
    pub capacity: u64,
    pub tenant_in_flight: u64,
    pub tenant_capacity: u64,
    pub holders: usize,
    /// Other fetches still waiting.
    pub waiters: usize,
}

/// RAII reservation against a [`HydrationBudget`].
pub struct BudgetPermit {
    budget: Arc<HydrationBudget>,
    tenant: String,
    bytes: u64,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        self.budget.release(&self.tenant, self.bytes);
    }
}

/// One request's hydration accounting: how much it has pulled, how long it
/// spent blocked on the process-wide budget, and whether it blew its
/// ceiling and must be shed.
///
/// Shared (as an `Arc`) by every fetch a single search request issues —
/// footer prefetch, scoring set, and posting blobs alike — so the ceiling
/// covers the request's whole hydration cost rather than each batch's.
pub struct RequestHydration {
    fetched_bytes: AtomicU64,
    /// Ceiling in bytes; `u64::MAX` disables shedding.
    ceiling: u64,
    shed: AtomicBool,
    /// Shed because a fetch waited out the pod's in-flight budget, rather
    /// than because the request went over its own ceiling.
    shed_waiting: AtomicBool,
    wait_micros: AtomicU64,
    /// Fetches whose budget wait expired; the first is logged in full,
    /// the total when the request's accountant drops.
    budget_expiries: AtomicU64,
    /// " rid=<id>" of the request this accounts for (empty for background
    /// work): fetches run on the S3 runtime's threads, which do not carry
    /// the handler thread's request id.
    rid: String,
}

impl RequestHydration {
    pub fn new(ceiling: u64) -> Self {
        Self {
            fetched_bytes: AtomicU64::new(0),
            ceiling,
            shed: AtomicBool::new(false),
            shed_waiting: AtomicBool::new(false),
            wait_micros: AtomicU64::new(0),
            budget_expiries: AtomicU64::new(0),
            rid: crate::request_id(),
        }
    }

    /// An accountant that never sheds — for hydration that is not serving a
    /// user-facing search (warmup, compaction, admin rewrites), where a
    /// partial fetch is worse than a slow one.
    pub fn unlimited() -> Arc<Self> {
        Arc::new(Self::new(u64::MAX))
    }

    /// Charge `bytes` to this request, returning whether it may proceed.
    /// Once over the ceiling the request stays shed: every subsequent fetch
    /// short-circuits, so an oversized hydration stops at the ceiling
    /// instead of at the end of its file list.
    pub fn charge(&self, bytes: u64) -> bool {
        if self.shed.load(Ordering::Relaxed) {
            return false;
        }
        let total = self.fetched_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if total > self.ceiling {
            self.shed.store(true, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Give back bytes charged for a fetch that then failed — an error is
    /// not consumption, and repeated retries of a flaky object should not
    /// walk a healthy request into its ceiling.
    pub fn refund(&self, bytes: u64) {
        self.fetched_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn record_wait(&self, waited: Duration) {
        self.wait_micros
            .fetch_add(waited.as_micros() as u64, Ordering::Relaxed);
    }

    /// Mark the request shed without charging bytes — used when the
    /// process-wide budget wait times out.
    ///
    /// No-op for an [`unlimited`](Self::unlimited) accountant: background
    /// and admin hydration (warmup, compaction, rewrites) must not
    /// short-circuit the rest of its file list because one file waited out
    /// the budget. That fetch still fails and still surfaces through the
    /// caller's own `missing` list — a partial fetch there is worse than a
    /// slow one, which is the whole reason those callers opt out of the
    /// ceiling.
    pub fn mark_shed(&self) {
        if self.ceiling == u64::MAX {
            return;
        }
        // A request already shed keeps its first cause for the log line.
        if !self.shed.swap(true, Ordering::Relaxed) {
            self.shed_waiting.store(true, Ordering::Relaxed);
        }
    }

    /// Why the request was shed, for its log line.
    pub fn shed_reason(&self) -> String {
        let mb = |b: u64| b as f64 / (1024.0 * 1024.0);
        if self.shed_waiting.load(Ordering::Relaxed) {
            format!(
                "a fetch timed out waiting for in-flight budget: the pod's \
                 (KOSHA_HYDRATE_BYTE_BUDGET) or its tenant's \
                 (KOSHA_HYDRATE_TENANT_MAX_INFLIGHT_BYTES), waited_ms={:.0} fetched_mb={:.1}",
                self.wait_ms(),
                mb(self.fetched_bytes())
            )
        } else {
            format!(
                "over the request ceiling (KOSHA_HYDRATE_REQUEST_MAX_BYTES) fetched_mb={:.1} ceiling_mb={:.1}",
                mb(self.fetched_bytes()),
                mb(self.ceiling)
            )
        }
    }

    /// Log that `key`'s wait for `want` bytes of in-flight budget expired
    /// after `waited`: in full for the request's first expiry, counted for
    /// the rest (reported when the accountant drops), so one stalled pod
    /// logs a line per request, not per fetch.
    pub fn note_budget_expired(&self, key: &str, want: u64, waited: Duration, at: &BudgetExpired) {
        if self.budget_expiries.fetch_add(1, Ordering::Relaxed) > 0 {
            return;
        }
        let mb = |b: u64| b as f64 / (1024.0 * 1024.0);
        eprintln!(
            "WARN: hydrate budget wait expired: key={key} want_mb={:.1} waited_ms={} \
             in_flight_mb={:.1}/{:.1} tenant_mb={:.1}/{:.1} holders={} waiters={}{}",
            mb(want),
            waited.as_millis(),
            mb(at.in_flight),
            mb(at.capacity),
            mb(at.tenant_in_flight),
            mb(at.tenant_capacity),
            at.holders,
            at.waiters,
            self.rid
        );
    }

    pub fn is_shed(&self) -> bool {
        self.shed.load(Ordering::Relaxed)
    }

    pub fn wait_ms(&self) -> f64 {
        self.wait_micros.load(Ordering::Relaxed) as f64 / 1e3
    }

    pub fn fetched_bytes(&self) -> u64 {
        self.fetched_bytes.load(Ordering::Relaxed)
    }
}

impl Drop for RequestHydration {
    fn drop(&mut self) {
        let expiries = *self.budget_expiries.get_mut();
        if expiries > 1 {
            eprintln!(
                "WARN: hydrate budget wait expired for {expiries} fetches of this request \
                 (first logged above){}",
                self.rid
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// `HydrationBudget::acquire` from a plain thread, as the threaded
    /// tests below drive it: each call gets its own runtime, so a waiter
    /// here never shares a runtime with the holder it waits on.
    fn acquire(budget: &Arc<HydrationBudget>, tenant: &str, bytes: u64) -> Option<BudgetPermit> {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(budget.acquire(tenant, bytes))
            .ok()
    }

    #[test]
    fn concurrent_reservations_never_exceed_capacity() {
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            100,
            Duration::from_secs(5),
        ));
        let a = acquire(&budget, "t", 60).expect("first fits");
        assert_eq!(budget.in_flight(), 60);

        let b = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            // Must block: 60 + 60 > 100, and a holder exists.
            let permit = acquire(&b, "t", 60).expect("granted once the holder drops");
            assert_eq!(b.in_flight(), 60);
            drop(permit);
        });

        // Give the waiter time to actually block before freeing capacity;
        // if it had been admitted eagerly, in_flight would read 120.
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(budget.in_flight(), 60, "waiter must not have been admitted");
        drop(a);
        handle.join().unwrap();
        assert_eq!(budget.in_flight(), 0);
    }

    #[test]
    fn single_oversized_reservation_is_admitted_when_alone() {
        // A file larger than the whole budget must degrade to "fetched
        // alone", not deadlock — same rule as MemoryLedger's anti-starvation
        // clause.
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            100,
            Duration::from_millis(50),
        ));
        let permit = acquire(&budget, "t", 10_000).expect("no other holder → admit");
        assert_eq!(budget.in_flight(), 10_000);
        drop(permit);
        assert_eq!(budget.in_flight(), 0);
    }

    #[test]
    fn contended_reservation_times_out_instead_of_waiting_forever() {
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            100,
            Duration::from_millis(50),
        ));
        let _held = acquire(&budget, "t", 100).expect("first fits exactly");
        assert!(
            acquire(&budget, "t", 100).is_none(),
            "nothing frees within the wait timeout → shed, don't hang"
        );
    }

    /// #342: tenant A's cold start cannot take more than its in-flight cap,
    /// even with global room left, and tenant B is admitted meanwhile.
    #[test]
    fn a_tenant_cannot_exceed_its_cap_while_another_is_admitted() {
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            150,
            50,
            Duration::from_millis(50),
        ));
        // The cap binds only while another tenant is present.
        let b = acquire(&budget, "beta", 50).expect("B is admitted at once");
        let a1 = acquire(&budget, "acme", 40).expect("under A's cap");
        assert!(
            acquire(&budget, "acme", 20).is_none(),
            "40 + 20 is over A's cap of 50 though the pod has room"
        );
        assert_eq!(budget.tenant_in_flight("acme"), 40);
        assert_eq!(budget.in_flight(), 90);
        assert_eq!(budget.tenant_in_flight("beta"), 50);
        // Anti-starvation per tenant: alone in its tenant, an object larger
        // than the tenant cap is still fetched when the pod has room.
        drop(a1);
        let big = acquire(&budget, "acme", 60).expect("A has nothing in flight");
        assert_eq!(budget.tenant_in_flight("acme"), 60);
        drop(big);
        drop(b);
        assert_eq!(budget.in_flight(), 0);
        assert_eq!(budget.tenant_in_flight("acme"), 0);
    }

    /// The default config's tenant cap (half the budget) must not halve a
    /// single-tenant pod's hydration bandwidth: alone, a tenant may use the
    /// whole budget.
    #[test]
    fn a_lone_tenant_may_use_the_whole_budget() {
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            50,
            Duration::from_millis(50),
        ));
        let a = acquire(&budget, "_legacy", 60).expect("first fits");
        let b = acquire(&budget, "_legacy", 30)
            .expect("60 + 30 is over the tenant cap, but nobody else wants budget");
        assert_eq!(budget.in_flight(), 90);
        drop(a);
        drop(b);
        assert_eq!(budget.in_flight(), 0);
    }

    /// An object larger than the tenant cap (a 600 MB doc_store.bin under
    /// a 512 MiB cap) is admitted while its tenant has smaller fetches in
    /// flight, as long as it fits globally and no other tenant is waiting.
    #[test]
    fn an_object_over_the_tenant_cap_is_admitted_beside_its_tenants_small_fetches() {
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            1_000,
            500,
            Duration::from_millis(50),
        ));
        let small = acquire(&budget, "acme", 100).unwrap();
        let big = acquire(&budget, "acme", 600).expect("fits globally, nobody else present");
        assert_eq!(budget.tenant_in_flight("acme"), 700);
        drop(big);
        // With another tenant holding (but not waiting), the oversized
        // object is still admitted: it can never fit under the cap.
        let beta = acquire(&budget, "beta", 100).unwrap();
        let big = acquire(&budget, "acme", 600).expect("oversized, nobody waiting");
        drop(big);
        // A small fetch over the cap waits while another tenant holds.
        let mid = acquire(&budget, "acme", 300).unwrap();
        assert!(
            acquire(&budget, "acme", 200).is_none(),
            "100 + 300 + 200 is over A's cap while B is present"
        );
        drop((small, mid, beta));
        assert_eq!(budget.in_flight(), 0);
    }

    /// A tenant waiting on its own cap is admitted as soon as its earlier
    /// fetch finishes, and never sees more than its cap in flight.
    #[test]
    fn a_tenant_waiting_on_its_cap_is_woken_by_its_own_release() {
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            50,
            Duration::from_secs(5),
        ));
        // Another tenant is present, so A's cap binds.
        let _other = acquire(&budget, "beta", 10).unwrap();
        let held = acquire(&budget, "acme", 40).unwrap();
        let b2 = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            let p = acquire(&b2, "acme", 20).expect("granted once A frees");
            assert!(b2.tenant_in_flight("acme") <= 50);
            drop(p);
        });
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(budget.tenant_in_flight("acme"), 40, "still waiting");
        drop(held);
        handle.join().unwrap();
        assert_eq!(
            budget.in_flight(),
            10,
            "only the other tenant's permit is left"
        );
    }

    /// #342 review: freed budget goes to a waiting tenant before tenants
    /// that already hold some. Two heavy tenants refill their caps (which
    /// together are the whole budget) with many small fetches; a third
    /// tenant waiting for an object larger than capacity minus one cap is
    /// admitted within a few of their releases instead of timing out.
    #[test]
    fn a_waiting_tenant_is_not_starved_by_tenants_refilling_their_caps() {
        use std::sync::atomic::AtomicBool;
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            50,
            Duration::from_secs(10),
        ));
        let stop = Arc::new(AtomicBool::new(false));
        let mut heavy = Vec::new();
        for tenant in ["acme", "globex"] {
            for _ in 0..8 {
                let budget = Arc::clone(&budget);
                let stop = Arc::clone(&stop);
                heavy.push(std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        if let Some(p) = acquire(&budget, tenant, 10) {
                            std::thread::sleep(Duration::from_millis(2));
                            drop(p);
                        }
                    }
                }));
            }
        }
        // Let both heavy tenants fill the budget.
        std::thread::sleep(Duration::from_millis(100));
        let started = Instant::now();
        let permit = acquire(&budget, "beta", 60);
        let waited = started.elapsed();
        stop.store(true, Ordering::Relaxed);
        assert!(permit.is_some(), "beta timed out behind refilling tenants");
        assert!(
            waited < Duration::from_secs(2),
            "beta waited {waited:?} for 60 bytes"
        );
        drop(permit);
        for h in heavy {
            h.join().unwrap();
        }
        assert_eq!(budget.in_flight(), 0);
    }

    /// A tenant that holds budget does not take freed bytes another
    /// tenant's admissible waiter needs; both are admitted once room frees.
    #[test]
    fn freed_budget_is_kept_for_another_tenants_waiter() {
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            50,
            Duration::from_secs(5),
        ));
        let a1 = acquire(&budget, "acme", 40).unwrap();
        let a2 = acquire(&budget, "acme", 10).unwrap();
        let g = acquire(&budget, "globex", 50).unwrap();
        let admitted = Arc::new(Mutex::new(Vec::new()));
        let spawn = |tenant: &'static str, bytes: u64| {
            let budget = Arc::clone(&budget);
            let admitted = Arc::clone(&admitted);
            std::thread::spawn(move || {
                let p = acquire(&budget, tenant, bytes).expect("admitted");
                admitted.lock().unwrap().push(tenant);
                drop(p);
            })
        };
        let beta = spawn("beta", 30);
        std::thread::sleep(Duration::from_millis(30));
        drop(a2);
        let acme = spawn("acme", 10);
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            admitted.lock().unwrap().is_empty(),
            "90 in flight: acme must not take the freed 10 while beta waits for 30"
        );
        drop(a1);
        beta.join().unwrap();
        acme.join().unwrap();
        assert_eq!(admitted.lock().unwrap().len(), 2);
        drop(g);
        assert_eq!(budget.in_flight(), 0);
    }

    /// D1: a fetch waiting for budget must not occupy a worker of the
    /// runtime its holder releases on. Fetches run on the S3 runtime's few
    /// workers and a holder releases only once its body stream (here a
    /// timer) completes on that runtime; with the waiter parked on a
    /// worker, a runtime with one worker never drove the holder's stream,
    /// so the waiter sat out its whole timeout and was shed — the 30 s
    /// beat of 429s seen on staging.
    #[test]
    fn a_waiter_does_not_stall_the_runtime_its_holder_releases_on() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            100,
            Duration::from_secs(2),
        ));
        let started = Instant::now();
        let admitted = rt.block_on(async {
            let permit = budget.acquire("t", 100).await.expect("first fits");
            let holder = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                drop(permit);
            });
            let waiter = tokio::spawn({
                let budget = Arc::clone(&budget);
                async move { budget.acquire("t", 100).await.is_ok() }
            });
            let admitted = waiter.await.unwrap();
            holder.await.unwrap();
            admitted
        });
        assert!(admitted, "the waiter timed out: its holder never ran");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "admitted only after {:?}; the holder frees at 50 ms",
            started.elapsed()
        );
        assert_eq!(budget.in_flight(), 0);
    }

    /// A waiting fetch whose task is cancelled leaves `waiting`: it must
    /// not go on reserving room another tenant's fetch is then held back
    /// for.
    #[test]
    fn a_cancelled_wait_stops_holding_other_tenants_back() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            50,
            Duration::from_secs(5),
        ));
        rt.block_on(async {
            let _acme = budget.acquire("acme", 50).await.unwrap();
            // beta waits for 60: 50 + 60 is over capacity.
            let cancelled =
                tokio::time::timeout(Duration::from_millis(20), budget.acquire("beta", 60)).await;
            assert!(cancelled.is_err(), "beta cannot fit yet");
            // Had beta's wait stayed listed, acme (already holding) would
            // have to leave 60 free for it: 50 + 10 + 60 > 100.
            let more = tokio::time::timeout(Duration::from_millis(200), budget.acquire("acme", 10))
                .await
                .expect("not held back by a cancelled waiter")
                .expect("admitted");
            drop(more);
        });
    }

    /// An expired wait reports the state that held it back.
    #[test]
    fn an_expired_wait_reports_what_held_it_back() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            50,
            Duration::from_millis(20),
        ));
        rt.block_on(async {
            let _held = budget.acquire("acme", 90).await.unwrap();
            let Err(expired) = budget.acquire("acme", 20).await else {
                panic!("90 + 20 is over capacity");
            };
            assert_eq!(expired.in_flight, 90);
            assert_eq!(expired.capacity, 100);
            assert_eq!(expired.tenant_in_flight, 90);
            assert_eq!(expired.tenant_capacity, 50);
            assert_eq!(expired.holders, 1);
            assert_eq!(expired.waiters, 0, "its own wait is withdrawn");
        });
    }

    #[test]
    fn request_sheds_once_past_its_ceiling_and_stays_shed() {
        let req = RequestHydration::new(1_000);
        assert!(req.charge(600), "under ceiling");
        assert!(!req.charge(600), "crossing the ceiling sheds");
        assert!(req.is_shed());
        assert!(
            !req.charge(1),
            "a shed request short-circuits every later fetch"
        );
    }

    #[test]
    fn failed_fetch_is_refunded_and_does_not_count_toward_the_ceiling() {
        let req = RequestHydration::new(1_000);
        assert!(req.charge(900));
        req.refund(900);
        assert_eq!(req.fetched_bytes(), 0);
        assert!(req.charge(900), "refunded bytes are not held against it");
    }

    #[test]
    fn unlimited_never_sheds() {
        let req = RequestHydration::unlimited();
        assert!(req.charge(u64::MAX / 2));
        assert!(!req.is_shed());
        // Not even on a budget-wait timeout: one slow file must not abandon
        // the rest of a compaction's or warmup's fetch list.
        req.mark_shed();
        assert!(!req.is_shed());
        assert!(req.charge(1));
    }

    /// The log line names the cause that shed the request first: a budget
    /// wait timing out after the ceiling already shed it is still a ceiling
    /// shed, and a wait names both limits it can time out on.
    #[test]
    fn shed_reason_keeps_the_first_cause() {
        let over = RequestHydration::new(1_000);
        assert!(!over.charge(2_000));
        over.mark_shed();
        assert!(over
            .shed_reason()
            .contains("KOSHA_HYDRATE_REQUEST_MAX_BYTES"));

        let waited = RequestHydration::new(1_000);
        waited.mark_shed();
        let reason = waited.shed_reason();
        assert!(reason.contains("KOSHA_HYDRATE_BYTE_BUDGET"), "{reason}");
        assert!(
            reason.contains("KOSHA_HYDRATE_TENANT_MAX_INFLIGHT_BYTES"),
            "{reason}"
        );
    }
}
