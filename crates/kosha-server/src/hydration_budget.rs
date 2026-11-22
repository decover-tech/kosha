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
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

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
    /// Byte requests of the fetches blocked in [`HydrationBudget::acquire`],
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
    cv: Condvar,
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
            cv: Condvar::new(),
            capacity: capacity.max(1),
            tenant_capacity: tenant_capacity.max(1),
            wait_timeout,
        }
    }

    /// Reserve `bytes` for `tenant` until the returned permit drops,
    /// blocking while the reservation would exceed the global capacity or
    /// the tenant's cap.
    ///
    /// Returns `None` if nothing freed within `wait_timeout` — the caller
    /// abandons that fetch and sheds the request rather than waiting
    /// unboundedly with a live S3 response open.
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
    pub fn acquire(self: &Arc<Self>, tenant: &str, bytes: u64) -> Option<BudgetPermit> {
        let deadline = Instant::now() + self.wait_timeout;
        let mut st = self.state.lock().unwrap();
        let mut registered = false;
        loop {
            let fits = st.in_flight.saturating_add(bytes) <= self.capacity;
            let tenant_holders = st.tenants.get(tenant).map_or(0, |t| t.holders);
            let tenant_ok = st.tenant_ok(tenant, bytes, self.tenant_capacity);
            let leaves_room = tenant_holders == 0 || {
                let reserve = st.reserve_for_others(tenant, self.tenant_capacity);
                st.in_flight.saturating_add(bytes).saturating_add(reserve) <= self.capacity
            };
            if (fits || st.holders == 0) && tenant_ok && leaves_room {
                if registered {
                    st.remove_waiter(tenant, bytes);
                    // Leaving `waiting` can lift the cap or the reserve that
                    // held another tenant's waiter back.
                    self.cv.notify_all();
                }
                st.in_flight = st.in_flight.saturating_add(bytes);
                st.holders += 1;
                let t = st.tenants.entry(tenant.to_string()).or_default();
                t.bytes = t.bytes.saturating_add(bytes);
                t.holders += 1;
                return Some(BudgetPermit {
                    budget: Arc::clone(self),
                    tenant: tenant.to_string(),
                    bytes,
                });
            }
            let now = Instant::now();
            if now >= deadline {
                if registered {
                    st.remove_waiter(tenant, bytes);
                    drop(st);
                    // Our wait may have been what held another tenant's
                    // fetch to its cap.
                    self.cv.notify_all();
                }
                return None;
            }
            if !registered {
                st.add_waiter(tenant, bytes);
                registered = true;
            }
            let (guard, _timeout) = self.cv.wait_timeout(st, deadline - now).unwrap();
            st = guard;
        }
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
        drop(st);
        self.cv.notify_all();
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
    wait_micros: AtomicU64,
}

impl RequestHydration {
    pub fn new(ceiling: u64) -> Self {
        Self {
            fetched_bytes: AtomicU64::new(0),
            ceiling,
            shed: AtomicBool::new(false),
            wait_micros: AtomicU64::new(0),
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
        self.shed.store(true, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_reservations_never_exceed_capacity() {
        let budget = Arc::new(HydrationBudget::with_tenant_cap(
            100,
            100,
            Duration::from_secs(5),
        ));
        let a = budget.acquire("t", 60).expect("first fits");
        assert_eq!(budget.in_flight(), 60);

        let b = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            // Must block: 60 + 60 > 100, and a holder exists.
            let permit = b.acquire("t", 60).expect("granted once the holder drops");
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
        let permit = budget
            .acquire("t", 10_000)
            .expect("no other holder → admit");
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
        let _held = budget.acquire("t", 100).expect("first fits exactly");
        assert!(
            budget.acquire("t", 100).is_none(),
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
        let b = budget.acquire("beta", 50).expect("B is admitted at once");
        let a1 = budget.acquire("acme", 40).expect("under A's cap");
        assert!(
            budget.acquire("acme", 20).is_none(),
            "40 + 20 is over A's cap of 50 though the pod has room"
        );
        assert_eq!(budget.tenant_in_flight("acme"), 40);
        assert_eq!(budget.in_flight(), 90);
        assert_eq!(budget.tenant_in_flight("beta"), 50);
        // Anti-starvation per tenant: alone in its tenant, an object larger
        // than the tenant cap is still fetched when the pod has room.
        drop(a1);
        let big = budget.acquire("acme", 60).expect("A has nothing in flight");
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
        let a = budget.acquire("_legacy", 60).expect("first fits");
        let b = budget
            .acquire("_legacy", 30)
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
        let small = budget.acquire("acme", 100).unwrap();
        let big = budget
            .acquire("acme", 600)
            .expect("fits globally, nobody else present");
        assert_eq!(budget.tenant_in_flight("acme"), 700);
        drop(big);
        // With another tenant holding (but not waiting), the oversized
        // object is still admitted: it can never fit under the cap.
        let beta = budget.acquire("beta", 100).unwrap();
        let big = budget
            .acquire("acme", 600)
            .expect("oversized, nobody waiting");
        drop(big);
        // A small fetch over the cap waits while another tenant holds.
        let mid = budget.acquire("acme", 300).unwrap();
        assert!(
            budget.acquire("acme", 200).is_none(),
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
        let _other = budget.acquire("beta", 10).unwrap();
        let held = budget.acquire("acme", 40).unwrap();
        let b2 = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            let p = b2.acquire("acme", 20).expect("granted once A frees");
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
                        if let Some(p) = budget.acquire(tenant, 10) {
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
        let permit = budget.acquire("beta", 60);
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
        let a1 = budget.acquire("acme", 40).unwrap();
        let a2 = budget.acquire("acme", 10).unwrap();
        let g = budget.acquire("globex", 50).unwrap();
        let admitted = Arc::new(Mutex::new(Vec::new()));
        let spawn = |tenant: &'static str, bytes: u64| {
            let budget = Arc::clone(&budget);
            let admitted = Arc::clone(&admitted);
            std::thread::spawn(move || {
                let p = budget.acquire(tenant, bytes).expect("admitted");
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
}
