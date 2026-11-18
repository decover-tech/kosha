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
//! [`RequestHydration`] is the per-request half: a running total that lets a
//! request whose hydration set is absurd stop and shed (HTTP 429) instead of
//! grinding through gigabytes while holding a hydration permit.

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

struct BudgetState {
    /// Bytes reserved by fetches that have been admitted and are still
    /// streaming to disk.
    in_flight: u64,
    /// Number of live permits. The anti-starvation rule keys off this —
    /// see [`HydrationBudget::acquire`].
    holders: usize,
}

/// Process-wide cap on hydration bytes in flight at once.
pub struct HydrationBudget {
    state: Mutex<BudgetState>,
    cv: Condvar,
    capacity: u64,
    wait_timeout: Duration,
}

impl HydrationBudget {
    pub fn new(capacity: u64, wait_timeout: Duration) -> Self {
        Self {
            state: Mutex::new(BudgetState {
                in_flight: 0,
                holders: 0,
            }),
            cv: Condvar::new(),
            capacity: capacity.max(1),
            wait_timeout,
        }
    }

    /// Reserve `bytes` until the returned permit drops, blocking while the
    /// reservation would exceed capacity.
    ///
    /// Returns `None` if nothing freed within `wait_timeout` — the caller
    /// abandons that fetch and sheds the request rather than waiting
    /// unboundedly with a live S3 response open.
    ///
    /// Anti-starvation, mirroring `kosha_query::MemoryLedger::admit`: when
    /// no other permit is outstanding, a reservation is granted however
    /// large. A single object bigger than the whole budget (a
    /// several-hundred-MB `doc_store.bin` under a small configured budget)
    /// must degrade to "fetched alone", never to "never fetched".
    pub fn acquire(self: &Arc<Self>, bytes: u64) -> Option<BudgetPermit> {
        let deadline = Instant::now() + self.wait_timeout;
        let mut st = self.state.lock().unwrap();
        loop {
            let fits = st.in_flight.saturating_add(bytes) <= self.capacity;
            if fits || st.holders == 0 {
                st.in_flight = st.in_flight.saturating_add(bytes);
                st.holders += 1;
                return Some(BudgetPermit {
                    budget: Arc::clone(self),
                    bytes,
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let (guard, _timeout) = self.cv.wait_timeout(st, deadline - now).unwrap();
            st = guard;
        }
    }

    fn release(&self, bytes: u64) {
        let mut st = self.state.lock().unwrap();
        st.in_flight = st.in_flight.saturating_sub(bytes);
        st.holders = st.holders.saturating_sub(1);
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
}

/// RAII reservation against a [`HydrationBudget`].
pub struct BudgetPermit {
    budget: Arc<HydrationBudget>,
    bytes: u64,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
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
    /// Wall time spent inside fetch batches for this request.
    fetch_micros: AtomicU64,
    /// What this request made local, by kind of file: (files, bytes).
    /// Written once per fetch batch, read once when a profile is built.
    fetched_by_kind: std::sync::Mutex<std::collections::BTreeMap<&'static str, (u64, u64)>>,
}

/// The kind of segment file a name denotes, for the search profile. The
/// kinds are what a reader of the profile needs to tell apart: a search that
/// spent its time fetching posting shards is a first-seen term; one that
/// fetched document bodies is a first-seen page.
pub fn file_kind(name: &str) -> &'static str {
    let name = name.rsplit('/').next().unwrap_or(name);
    if name.starts_with("postings") {
        "posting_shards"
    } else if name == "filters.bin" {
        "filter_columns"
    } else if name == "doc_store.bin" {
        "document_bodies"
    } else if name == "doc_store.offsets" {
        "document_offsets"
    } else if name.starts_with("vector.") {
        "vector_index"
    } else if name == "inverted.idx" {
        "term_table"
    } else if name == "footer.json" {
        "footers"
    } else {
        "other"
    }
}

impl RequestHydration {
    pub fn new(ceiling: u64) -> Self {
        Self {
            fetched_bytes: AtomicU64::new(0),
            ceiling,
            shed: AtomicBool::new(false),
            wait_micros: AtomicU64::new(0),
            fetch_micros: AtomicU64::new(0),
            fetched_by_kind: std::sync::Mutex::new(std::collections::BTreeMap::new()),
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

    /// Record one fetch batch: how long it took and what it made local.
    pub fn record_fetch(&self, took: Duration, landed: &[(&'static str, u64)]) {
        self.fetch_micros
            .fetch_add(took.as_micros() as u64, Ordering::Relaxed);
        if landed.is_empty() {
            return;
        }
        let mut by_kind = self.fetched_by_kind.lock().unwrap();
        for (kind, bytes) in landed {
            let entry = by_kind.entry(kind).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += bytes;
        }
    }

    pub fn fetch_ms(&self) -> f64 {
        self.fetch_micros.load(Ordering::Relaxed) as f64 / 1e3
    }

    /// `{kind: {"files": n, "bytes": n}}` for everything this request made
    /// local.
    pub fn fetched_by_kind_json(&self) -> serde_json::Value {
        let by_kind = self.fetched_by_kind.lock().unwrap();
        serde_json::Value::Object(
            by_kind
                .iter()
                .map(|(kind, (files, bytes))| {
                    (
                        kind.to_string(),
                        serde_json::json!({"files": files, "bytes": bytes}),
                    )
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_reservations_never_exceed_capacity() {
        let budget = Arc::new(HydrationBudget::new(100, Duration::from_secs(5)));
        let a = budget.acquire(60).expect("first fits");
        assert_eq!(budget.in_flight(), 60);

        let b = Arc::clone(&budget);
        let handle = std::thread::spawn(move || {
            // Must block: 60 + 60 > 100, and a holder exists.
            let permit = b.acquire(60).expect("granted once the holder drops");
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
        let budget = Arc::new(HydrationBudget::new(100, Duration::from_millis(50)));
        let permit = budget.acquire(10_000).expect("no other holder → admit");
        assert_eq!(budget.in_flight(), 10_000);
        drop(permit);
        assert_eq!(budget.in_flight(), 0);
    }

    #[test]
    fn contended_reservation_times_out_instead_of_waiting_forever() {
        let budget = Arc::new(HydrationBudget::new(100, Duration::from_millis(50)));
        let _held = budget.acquire(100).expect("first fits exactly");
        assert!(
            budget.acquire(100).is_none(),
            "nothing frees within the wait timeout → shed, don't hang"
        );
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
