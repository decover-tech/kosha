//! Fair search admission across tenants (#341).
//!
//! [`FairSemaphore`] bounds concurrent scoring passes like the single FIFO
//! semaphore it replaces (`KOSHA_MAX_CONCURRENT_SEARCHES`), but waiters
//! queue per tenant and a freed permit goes to the next tenant in a round
//! robin over the tenants that have waiters (deficit round robin: a tenant
//! with weight `w` gets up to `w` permits per turn). One tenant's burst of
//! a hundred queued searches therefore delays another tenant's single
//! search by at most one permit turn, instead of queueing it behind all
//! hundred.
//!
//! Overload still sheds instead of piling up: the total queue is bounded
//! (`KOSHA_SEARCH_QUEUE_DEPTH`), so is each tenant's share of it
//! (`KOSHA_SEARCH_QUEUE_DEPTH_PER_TENANT`), and a waiter gives up after
//! `KOSHA_SEARCH_QUEUE_TIMEOUT_MS`. The caller answers a shed with 429.
//!
//! The total bound is shared fairly too. When the queue is full and the
//! arriving tenant holds less of it than the tenant with the most waiters,
//! that tenant's newest waiter is shed and the arrival queues in its place
//! (drop from the longest queue). So a tenant that fills the whole queue
//! sheds its own requests, not another tenant's; an arrival is refused
//! only when its tenant already has as large a share as anyone.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::metrics::QueueShed;

/// One tenant's waiters.
#[derive(Debug)]
struct TenantQueue {
    tickets: VecDeque<u64>,
    /// Permits this tenant may still take in its current turn.
    deficit: u32,
    weight: u32,
}

/// The permit bookkeeping, without blocking: [`FairSemaphore`] wraps it in
/// a mutex and a condition variable. Separate so the scheduling order can
/// be tested deterministically.
#[derive(Debug, Default)]
pub(crate) struct Scheduler {
    permits: usize,
    queues: HashMap<String, TenantQueue>,
    /// Tenants with waiters, in round-robin order; the front is served next.
    ring: VecDeque<String>,
    /// Tickets handed a permit whose waiter has not claimed it yet.
    granted: HashSet<u64>,
    /// Tickets shed to make room for another tenant (see
    /// [`Self::evict_for`]) whose waiter has not noticed yet.
    evicted: HashSet<u64>,
    next_ticket: u64,
    /// Queued plus granted-but-unclaimed tickets.
    waiters: usize,
}

impl Scheduler {
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            permits,
            ..Self::default()
        }
    }

    /// Take a permit at once if one is free and nobody is waiting.
    fn try_take(&mut self) -> bool {
        if self.permits > 0 && self.waiters == 0 {
            self.permits -= 1;
            return true;
        }
        false
    }

    fn tenant_waiters(&self, tenant: &str) -> usize {
        self.queues.get(tenant).map_or(0, |q| q.tickets.len())
    }

    /// Queue a waiter for `tenant`; returns its ticket.
    pub(crate) fn enqueue(&mut self, tenant: &str, weight: u32) -> u64 {
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.waiters += 1;
        match self.queues.get_mut(tenant) {
            Some(q) => {
                q.weight = weight.max(1);
                q.tickets.push_back(ticket);
            }
            None => {
                self.queues.insert(
                    tenant.to_string(),
                    TenantQueue {
                        tickets: VecDeque::from([ticket]),
                        deficit: 0,
                        weight: weight.max(1),
                    },
                );
                self.ring.push_back(tenant.to_string());
            }
        }
        ticket
    }

    /// Hand free permits to waiters, round robin across tenants. Returns
    /// whether any permit was granted.
    pub(crate) fn dispatch(&mut self) -> bool {
        let mut any = false;
        while self.permits > 0 {
            let Some(tenant) = self.ring.front().cloned() else {
                break;
            };
            let Some(queue) = self.queues.get_mut(&tenant) else {
                self.ring.pop_front();
                continue;
            };
            let Some(ticket) = queue.tickets.pop_front() else {
                self.queues.remove(&tenant);
                self.ring.pop_front();
                continue;
            };
            if queue.deficit == 0 {
                queue.deficit = queue.weight;
            }
            queue.deficit -= 1;
            self.permits -= 1;
            self.granted.insert(ticket);
            any = true;
            if queue.tickets.is_empty() {
                self.queues.remove(&tenant);
                self.ring.pop_front();
            } else if queue.deficit == 0 {
                self.ring.rotate_left(1);
            }
        }
        any
    }

    /// Make room in a full queue for an arrival from `tenant`: shed the
    /// newest waiter of the tenant with the most waiters, if that tenant
    /// would still have at least as many waiting as `tenant` afterwards.
    /// Returns whether a place was freed.
    pub(crate) fn evict_for(&mut self, tenant: &str) -> bool {
        let mine = self.tenant_waiters(tenant);
        // Ties go to the tenant earliest in the ring, so the choice does
        // not depend on hash order.
        let mut longest: Option<(&String, usize)> = None;
        for name in &self.ring {
            let len = self.queues.get(name).map_or(0, |q| q.tickets.len());
            if longest.is_none_or(|(_, best)| len > best) {
                longest = Some((name, len));
            }
        }
        let Some((name, len)) = longest else {
            return false;
        };
        // `len - 1 >= mine + 1`: otherwise the two would only swap places.
        if name == tenant || len < mine + 2 {
            return false;
        }
        let name = name.clone();
        let Some(queue) = self.queues.get_mut(&name) else {
            return false;
        };
        let Some(ticket) = queue.tickets.pop_back() else {
            return false;
        };
        self.waiters -= 1;
        self.evicted.insert(ticket);
        true
    }

    /// Whether `ticket` was shed by [`Self::evict_for`].
    fn take_evicted(&mut self, ticket: u64) -> bool {
        self.evicted.remove(&ticket)
    }

    /// Whether `ticket` holds a granted permit; if so the caller owns it.
    pub(crate) fn claim(&mut self, ticket: u64) -> bool {
        if self.granted.remove(&ticket) {
            self.waiters -= 1;
            return true;
        }
        false
    }

    /// Withdraw `ticket` (its waiter timed out). `true` if a permit had
    /// already been granted to it, which the caller now owns.
    fn cancel(&mut self, tenant: &str, ticket: u64) -> bool {
        if self.claim(ticket) {
            return true;
        }
        if let Some(queue) = self.queues.get_mut(tenant) {
            if let Some(pos) = queue.tickets.iter().position(|t| *t == ticket) {
                queue.tickets.remove(pos);
                self.waiters -= 1;
            }
            if queue.tickets.is_empty() {
                self.queues.remove(tenant);
                self.ring.retain(|t| t != tenant);
            }
        }
        false
    }

    /// Return a permit and hand it on. Returns whether it was granted.
    pub(crate) fn release(&mut self) -> bool {
        self.permits += 1;
        self.dispatch()
    }
}

/// A counting semaphore whose waiters are served fairly across tenants.
/// See the module doc.
pub(crate) struct FairSemaphore {
    state: Mutex<Scheduler>,
    /// Permits in all (free + held), for the shed log line.
    permits: usize,
    cv: Condvar,
    /// Max requests waiting in all. `0` = unbounded.
    max_waiters: usize,
    /// Max requests one tenant may have waiting. `0` = only `max_waiters`.
    max_waiters_per_tenant: usize,
    /// How long a request may wait before being shed. `None` = forever.
    wait_timeout: Option<Duration>,
}

impl FairSemaphore {
    pub(crate) fn new(
        permits: usize,
        max_waiters: usize,
        max_waiters_per_tenant: usize,
        wait_timeout: Option<Duration>,
    ) -> Self {
        Self {
            state: Mutex::new(Scheduler::new(permits)),
            permits,
            cv: Condvar::new(),
            max_waiters,
            max_waiters_per_tenant,
            wait_timeout,
        }
    }

    /// Acquire a permit for `tenant`, waiting in its queue, or shed (`Err`)
    /// when the queue (total or the tenant's) is full or the wait times
    /// out. `weight` is the tenant's share per round-robin turn.
    pub(crate) fn try_acquire(
        &self,
        tenant: &str,
        weight: u32,
    ) -> Result<FairPermit<'_>, QueueShed> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.try_take() {
            return Ok(FairPermit { sem: self });
        }
        // Fast-fail on a saturated queue: waiting only to time out later
        // would burn the client's deadline for nothing.
        if self.max_waiters_per_tenant > 0
            && st.tenant_waiters(tenant) >= self.max_waiters_per_tenant
        {
            return Err(QueueShed::TenantFull);
        }
        if self.max_waiters > 0 && st.waiters >= self.max_waiters {
            // Full: take a place from the tenant holding the most of the
            // queue, unless this tenant already holds as much.
            if !st.evict_for(tenant) {
                return Err(QueueShed::Full);
            }
            self.cv.notify_all();
        }
        let ticket = st.enqueue(tenant, weight);
        if st.dispatch() {
            self.cv.notify_all();
        }
        let deadline = self.wait_timeout.map(|t| Instant::now() + t);
        loop {
            if st.claim(ticket) {
                return Ok(FairPermit { sem: self });
            }
            if st.take_evicted(ticket) {
                return Err(QueueShed::Full);
            }
            match deadline {
                None => st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner()),
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        return if st.cancel(tenant, ticket) {
                            Ok(FairPermit { sem: self })
                        } else {
                            Err(QueueShed::Timeout)
                        };
                    }
                    st = self
                        .cv
                        .wait_timeout(st, d - now)
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                }
            }
        }
    }

    /// Max requests one tenant may have waiting (`0` = no per-tenant cap).
    pub(crate) fn max_waiters_per_tenant(&self) -> usize {
        self.max_waiters_per_tenant
    }

    /// The queue as a shed sees it, for its log line (shed path only):
    /// permits in all, permits held, requests waiting in all and in
    /// `tenant`'s queue.
    pub(crate) fn shed_snapshot(&self, tenant: &str) -> ShedSnapshot {
        let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        ShedSnapshot {
            permits: self.permits,
            in_use: self.permits.saturating_sub(st.permits),
            waiters: st.waiters,
            tenant_waiters: st.tenant_waiters(tenant),
        }
    }

    /// How long a request may wait before it is shed (`None` = forever).
    pub(crate) fn wait_timeout(&self) -> Option<Duration> {
        self.wait_timeout
    }

    /// Requests waiting for a permit right now (`kosha_search_queue_depth`).
    pub(crate) fn waiters(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).waiters
    }
}

/// See [`FairSemaphore::shed_snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShedSnapshot {
    pub(crate) permits: usize,
    pub(crate) in_use: usize,
    pub(crate) waiters: usize,
    pub(crate) tenant_waiters: usize,
}

/// A held search permit; dropping it hands the permit to the next tenant.
pub(crate) struct FairPermit<'a> {
    sem: &'a FairSemaphore,
}

impl Drop for FairPermit<'_> {
    fn drop(&mut self) {
        let mut st = self.sem.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.release() {
            self.sem.cv.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Release permits one at a time and return the tickets in the order
    /// they were granted.
    fn drain(s: &mut Scheduler, tickets: &[(u64, &str)]) -> Vec<String> {
        let mut order = Vec::new();
        for _ in 0..tickets.len() {
            s.release();
            let granted: Vec<u64> = s.granted.iter().copied().collect();
            assert_eq!(granted.len(), 1, "one permit, one grant");
            let ticket = granted[0];
            assert!(s.claim(ticket));
            s.permits = 0; // the claimant holds it until the next release
            let tenant = tickets.iter().find(|(t, _)| *t == ticket).unwrap().1;
            order.push(tenant.to_string());
        }
        order
    }

    #[test]
    fn a_flood_does_not_delay_another_tenant_beyond_one_turn() {
        let mut s = Scheduler::new(0);
        let mut tickets = Vec::new();
        for _ in 0..100 {
            tickets.push((s.enqueue("flood", 1), "flood"));
        }
        tickets.push((s.enqueue("quiet", 1), "quiet"));
        let order = drain(&mut s, &tickets);
        let pos = order.iter().position(|t| t == "quiet").unwrap();
        assert!(pos <= 1, "quiet tenant served at position {pos}");
        assert_eq!(order.iter().filter(|t| *t == "flood").count(), 100);
        assert_eq!(s.waiters, 0);
        assert!(s.ring.is_empty() && s.queues.is_empty());
    }

    #[test]
    fn round_robin_alternates_and_weights_scale_turns() {
        let mut s = Scheduler::new(0);
        let mut tickets = Vec::new();
        for _ in 0..3 {
            tickets.push((s.enqueue("a", 1), "a"));
        }
        for _ in 0..3 {
            tickets.push((s.enqueue("b", 1), "b"));
        }
        assert_eq!(drain(&mut s, &tickets), ["a", "b", "a", "b", "a", "b"]);

        let mut s = Scheduler::new(0);
        let mut tickets = Vec::new();
        for _ in 0..6 {
            tickets.push((s.enqueue("heavy", 2), "heavy"));
        }
        for _ in 0..3 {
            tickets.push((s.enqueue("light", 1), "light"));
        }
        assert_eq!(
            drain(&mut s, &tickets),
            ["heavy", "heavy", "light", "heavy", "heavy", "light", "heavy", "heavy", "light"]
        );
    }

    #[test]
    fn cancel_withdraws_a_waiter_and_keeps_the_ring_consistent() {
        let mut s = Scheduler::new(0);
        let a = s.enqueue("a", 1);
        let b = s.enqueue("b", 1);
        assert!(!s.cancel("a", a));
        assert_eq!(s.waiters, 1);
        assert_eq!(s.ring, ["b"]);
        assert!(s.release());
        // Granted before its waiter gave up: cancel hands it over.
        assert!(s.cancel("b", b));
        assert_eq!(s.waiters, 0);
    }

    #[test]
    fn quiet_tenant_is_served_within_one_permit_turn_under_real_threads() {
        let sem = Arc::new(FairSemaphore::new(1, 0, 0, None));
        let held = sem.try_acquire("flood", 1).unwrap();
        let served = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut handles = Vec::new();
        for _ in 0..100 {
            let (sem, served) = (Arc::clone(&sem), Arc::clone(&served));
            handles.push(std::thread::spawn(move || {
                let _p = sem.try_acquire("flood", 1).unwrap();
                served.lock().unwrap().push("flood".into());
            }));
        }
        let t0 = Instant::now();
        while sem.waiters() < 100 && t0.elapsed() < Duration::from_secs(10) {
            std::thread::yield_now();
        }
        assert_eq!(sem.waiters(), 100);
        let quiet = {
            let (sem, served) = (Arc::clone(&sem), Arc::clone(&served));
            std::thread::spawn(move || {
                let _p = sem.try_acquire("quiet", 1).unwrap();
                served.lock().unwrap().push("quiet".into());
            })
        };
        while sem.waiters() < 101 && t0.elapsed() < Duration::from_secs(10) {
            std::thread::yield_now();
        }
        drop(held);
        for h in handles {
            h.join().unwrap();
        }
        quiet.join().unwrap();
        let served = served.lock().unwrap();
        let pos = served.iter().position(|t| t == "quiet").unwrap();
        assert!(pos <= 1, "quiet served at {pos} of {}", served.len());
    }

    #[test]
    fn full_queues_shed_at_once_and_waiters_time_out() {
        // The 10M vector namespace's failure mode: arrivals queued for two
        // minutes behind a saturated disk. Overload must shed, not queue.
        let sem = Arc::new(FairSemaphore::new(1, 2, 0, Some(Duration::from_millis(50))));
        let held = sem.try_acquire("t", 1).expect("first acquire must succeed");
        let mut waiting = Vec::new();
        for _ in 0..2 {
            let s = Arc::clone(&sem);
            waiting.push(std::thread::spawn(move || s.try_acquire("t", 1).is_ok()));
        }
        let t0 = Instant::now();
        while sem.waiters() < 2 && t0.elapsed() < Duration::from_secs(2) {
            std::thread::yield_now();
        }
        assert_eq!(sem.waiters(), 2, "both waiters should be queued");
        let t = Instant::now();
        // Same tenant: nobody holds more of the queue to take a place from.
        assert!(matches!(sem.try_acquire("t", 1), Err(QueueShed::Full)));
        assert!(
            t.elapsed() < Duration::from_millis(40),
            "shed must be immediate"
        );
        for h in waiting {
            assert!(!h.join().unwrap(), "queued waiter should time out");
        }
        assert_eq!(sem.waiters(), 0);
        drop(held);
        assert!(
            sem.try_acquire("t", 1).is_ok(),
            "a freed permit is acquirable"
        );
    }

    #[test]
    fn a_full_queue_evicts_from_the_longest_tenant_queue() {
        let mut s = Scheduler::new(0);
        let flood: Vec<u64> = (0..4).map(|_| s.enqueue("flood", 1)).collect();
        // The flooder itself gets no place from its own queue.
        assert!(!s.evict_for("flood"));
        // Another tenant takes the flooder's newest place.
        assert!(s.evict_for("quiet"));
        assert_eq!(s.waiters, 3);
        assert!(s.take_evicted(flood[3]));
        assert!(!s.take_evicted(flood[3]), "reported once");
        s.enqueue("quiet", 1);
        // flood 3, quiet 1: one more swap leaves 2 and 2.
        assert!(s.evict_for("quiet"));
        assert!(s.take_evicted(flood[2]));
        s.enqueue("quiet", 1);
        // Equal shares: nobody is evicted, in either direction.
        assert!(!s.evict_for("quiet"));
        assert!(!s.evict_for("flood"));
        assert_eq!(s.waiters, 4);
        // The evicted tickets are gone from the queue: only live ones are
        // granted.
        let mut granted = 0;
        while s.release() {
            let t: Vec<u64> = s.granted.iter().copied().collect();
            assert!(!flood[2..].contains(&t[0]));
            assert!(s.claim(t[0]));
            s.permits = 0;
            granted += 1;
        }
        assert_eq!(granted, 4);
        assert_eq!(s.waiters, 0);
        assert!(s.ring.is_empty() && s.queues.is_empty() && s.evicted.is_empty());

        // 2 against 1 would only swap places: no eviction.
        let mut s = Scheduler::new(0);
        s.enqueue("a", 1);
        s.enqueue("a", 1);
        s.enqueue("b", 1);
        assert!(!s.evict_for("b"));
        // 2 against 0 leaves 1 and 1.
        assert!(s.evict_for("c"));
    }

    /// The pod-wide queue bound must not let one tenant starve another
    /// (#341): a tenant that fills every permit and the whole queue sheds
    /// its own waiters when another tenant arrives.
    #[test]
    fn a_tenant_filling_the_whole_queue_does_not_shed_another_tenant() {
        let sem = Arc::new(FairSemaphore::new(2, 4, 0, Some(Duration::from_secs(30))));
        let held: Vec<_> = (0..2)
            .map(|_| sem.try_acquire("flood", 1).unwrap())
            .collect();
        let flooders: Vec<_> = (0..4)
            .map(|_| {
                let s = Arc::clone(&sem);
                std::thread::spawn(move || s.try_acquire("flood", 1).map(drop))
            })
            .collect();
        let t0 = Instant::now();
        while sem.waiters() < 4 && t0.elapsed() < Duration::from_secs(10) {
            std::thread::yield_now();
        }
        assert_eq!(sem.waiters(), 4, "the flooder fills the queue");
        // The flooder's own next request is shed: its share is the largest.
        assert!(matches!(sem.try_acquire("flood", 1), Err(QueueShed::Full)));

        let quiet = {
            let s = Arc::clone(&sem);
            std::thread::spawn(move || s.try_acquire("quiet", 1).map(drop))
        };
        // The quiet tenant queues; one flood waiter is shed for it at once,
        // long before the 30s wait timeout.
        let mut results = Vec::new();
        let mut flooders = flooders;
        while results.is_empty() && t0.elapsed() < Duration::from_secs(10) {
            if let Some(i) = flooders.iter().position(|h| h.is_finished()) {
                results.push(flooders.swap_remove(i).join().unwrap());
            } else {
                std::thread::yield_now();
            }
        }
        assert!(
            matches!(results.first(), Some(Err(QueueShed::Full))),
            "one flood waiter is shed: {results:?}"
        );
        assert_eq!(sem.waiters(), 4, "the queue stays at its bound");
        drop(held);
        assert!(quiet.join().unwrap().is_ok(), "the quiet tenant is served");
        for h in flooders {
            assert!(h.join().unwrap().is_ok(), "the remaining flood waiters run");
        }
        assert_eq!(sem.waiters(), 0);
    }

    #[test]
    fn per_tenant_cap_sheds_only_that_tenant() {
        let sem = Arc::new(FairSemaphore::new(
            1,
            0,
            1,
            Some(Duration::from_millis(200)),
        ));
        let held = sem.try_acquire("a", 1).unwrap();
        let first = {
            let s = Arc::clone(&sem);
            std::thread::spawn(move || s.try_acquire("a", 1).is_ok())
        };
        let t0 = Instant::now();
        while sem.waiters() < 1 && t0.elapsed() < Duration::from_secs(2) {
            std::thread::yield_now();
        }
        assert!(matches!(
            sem.try_acquire("a", 1),
            Err(QueueShed::TenantFull)
        ));
        let other = {
            let s = Arc::clone(&sem);
            std::thread::spawn(move || s.try_acquire("b", 1).is_ok())
        };
        while sem.waiters() < 2 && t0.elapsed() < Duration::from_secs(2) {
            std::thread::yield_now();
        }
        drop(held);
        assert!(first.join().unwrap());
        assert!(other.join().unwrap());
    }

    /// The shed log line's queue figures: permits held and waiting, in all
    /// and for the shed tenant.
    #[test]
    fn shed_snapshot_reports_the_queue_a_shed_saw() {
        let sem = FairSemaphore::new(1, 1, 0, Some(Duration::from_millis(1)));
        let held = sem.try_acquire("a", 1).expect("a free permit");
        assert_eq!(
            sem.shed_snapshot("b"),
            ShedSnapshot {
                permits: 1,
                in_use: 1,
                waiters: 0,
                tenant_waiters: 0
            }
        );
        assert!(matches!(sem.try_acquire("b", 1), Err(QueueShed::Timeout)));
        assert_eq!(sem.wait_timeout(), Some(Duration::from_millis(1)));
        drop(held);
        assert_eq!(sem.shed_snapshot("b").in_use, 0);
    }
}
