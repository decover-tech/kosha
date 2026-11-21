//! Per-namespace writer leases held by this process (#254).
//!
//! A writer pod (`ingest`, `standalone`) takes the lease on a namespace
//! before it accepts writes for it or publishes it, renews every held
//! lease in the background (`AppState::spawn_writer_lease_renewer`, every
//! TTL/3), and presents the lease's fencing token on every manifest CAS
//! (`ControlStore::compare_and_swap_manifest_fenced`). A writer whose CAS
//! or renewal finds another token is *fenced*: it stops accepting writes
//! for that namespace (503 + `Retry-After`) and drops its unpublished
//! local state (`AppState::fence_namespace`). It tries to take the lease
//! again on the next write, which succeeds only once the other writer's
//! lease has expired or been released.
//!
//! `KOSHA_WRITER_LEASE=off` turns all of it off: no lease rows, plain CAS,
//! exactly the pre-#254 behaviour.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use kosha_core::{ControlStore, KoshaError, NamespaceId};

/// Default `KOSHA_WRITER_LEASE_TTL_MS`.
pub(crate) const DEFAULT_TTL: Duration = Duration::from_millis(30_000);

/// `KOSHA_WRITER_LEASE`: anything but `off` (or `false`/`0`) leaves leases on.
pub(crate) fn enabled_from(raw: Option<&str>) -> bool {
    !matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("off" | "false" | "0" | "disabled")
    )
}

/// `KOSHA_WRITER_LEASE_TTL_MS`, at least 300 ms; unparsable values fall
/// back to the default.
pub(crate) fn ttl_from(raw: Option<&str>) -> Duration {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map_or(DEFAULT_TTL, |ms| Duration::from_millis(ms.max(300)))
}

/// Where lease rows live.
pub(crate) enum LeaseBackend {
    /// Through the control store (in-memory `Controller`, or any store
    /// without its own lease client). In-memory calls never block.
    ControlStore,
    /// Postgres, through a small pool of its own, so a renewal never waits
    /// on (or holds) `AppState::controller`'s mutex.
    #[cfg(feature = "postgres")]
    Postgres(kosha_control::PgWriterLeaseStore),
}

#[derive(Debug, Clone, Copy)]
struct Held {
    token: u64,
    /// Conservative local view of the lease's expiry: the TTL counted from
    /// before the acquire or renew call was sent.
    valid_until: Instant,
}

/// Leases this process holds and namespaces it was fenced from.
pub(crate) struct WriterLeases {
    enabled: bool,
    holder: String,
    ttl: Duration,
    backend: LeaseBackend,
    held: Mutex<HashMap<NamespaceId, Held>>,
    fenced: Mutex<HashSet<NamespaceId>>,
}

type Controller = Mutex<Box<dyn ControlStore>>;

impl WriterLeases {
    pub(crate) fn new(enabled: bool, ttl: Duration, backend: LeaseBackend) -> Self {
        let host = std::env::var("HOSTNAME")
            .ok()
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "kosha".into());
        // Unique per `WriterLeases` (two in one process in tests): the
        // clock plus a process-wide counter.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            enabled,
            holder: format!("{host}/{}/{nonce:x}-{seq}", std::process::id()),
            ttl,
            backend,
            held: Mutex::new(HashMap::new()),
            fenced: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn ttl(&self) -> Duration {
        self.ttl
    }

    pub(crate) fn holder(&self) -> &str {
        &self.holder
    }

    /// Seconds a client should wait before retrying a fenced write: about
    /// when a dead holder's lease would have expired.
    pub(crate) fn retry_after_secs(&self) -> u64 {
        self.ttl.as_secs().max(1)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn is_fenced(&self, ns: &NamespaceId) -> bool {
        self.fenced.lock().unwrap().contains(ns)
    }

    /// The token to present for `ns`, taking (or re-taking) the lease when
    /// this process does not hold a live one. `Ok(None)`: leases are off.
    /// `Err(Fenced)`: another holder's lease is live; nothing changes
    /// locally (a fence only follows a lost lease, see `lost`).
    pub(crate) fn token(
        &self,
        ns: &NamespaceId,
        controller: &Controller,
    ) -> Result<Option<u64>, KoshaError> {
        if !self.enabled {
            return Ok(None);
        }
        let now = Instant::now();
        let previous = self.held.lock().unwrap().get(ns).copied();
        if let Some(held) = previous.filter(|h| h.valid_until > now) {
            return Ok(Some(held.token));
        }
        let granted = match &self.backend {
            LeaseBackend::ControlStore => {
                controller
                    .lock()
                    .unwrap()
                    .acquire_writer_lease(ns, &self.holder, self.ttl)?
            }
            #[cfg(feature = "postgres")]
            LeaseBackend::Postgres(store) => store.acquire(ns, &self.holder, self.ttl)?,
        };
        match granted {
            Some(token) => {
                if previous.is_some_and(|h| h.token != token) {
                    eprintln!(
                        "writer lease: ns={} re-taken under token {token} (held {} before)",
                        ns.0,
                        previous.map_or(0, |h| h.token)
                    );
                }
                self.held.lock().unwrap().insert(
                    ns.clone(),
                    Held {
                        token,
                        valid_until: now + self.ttl,
                    },
                );
                if self.fenced.lock().unwrap().remove(ns) {
                    println!(
                        "writer lease: ns={} taken under token {token}; accepting writes again",
                        ns.0
                    );
                }
                Ok(Some(token))
            }
            None => Err(KoshaError::Fenced {
                namespace: ns.clone(),
                token: previous.map_or(0, |h| h.token),
                current: 0,
            }),
        }
    }

    /// Record that this process lost `ns` (fenced CAS, failed renewal or a
    /// lease it held taken over). Returns whether it was not fenced before.
    pub(crate) fn lost(&self, ns: &NamespaceId) -> bool {
        self.held.lock().unwrap().remove(ns);
        self.fenced.lock().unwrap().insert(ns.clone())
    }

    /// Whether this process held `ns` before (so a refused acquire means
    /// someone else took it over).
    pub(crate) fn held_before(&self, ns: &NamespaceId) -> bool {
        self.held.lock().unwrap().contains_key(ns)
    }

    /// Release every held lease, on graceful shutdown, so the next writer
    /// takes them at once instead of waiting out the TTL. Returns how many
    /// were released; a failed release only means that lease lapses on its
    /// own.
    pub(crate) fn release_all(&self, controller: &Controller) -> usize {
        let held: Vec<(NamespaceId, Held)> = self.held.lock().unwrap().drain().collect();
        let mut released = 0;
        for (ns, held) in held {
            let result = match &self.backend {
                LeaseBackend::ControlStore => {
                    controller
                        .lock()
                        .unwrap()
                        .release_writer_lease(&ns, &self.holder, held.token)
                }
                #[cfg(feature = "postgres")]
                LeaseBackend::Postgres(store) => store.release(&ns, &self.holder, held.token),
            };
            match result {
                Ok(()) => released += 1,
                Err(error) => eprintln!(
                    "WARN: writer lease release for {} failed: {error}; it lapses on its own",
                    ns.0
                ),
            }
        }
        released
    }

    /// Renew every held lease once. Returns the namespaces whose lease was
    /// taken over (the caller fences them). A control-plane error leaves
    /// the lease alone; it lapses locally once `valid_until` passes, and
    /// the next write re-takes it.
    pub(crate) fn renew_all(&self, controller: &Controller) -> Vec<NamespaceId> {
        let held: Vec<(NamespaceId, Held)> = self
            .held
            .lock()
            .unwrap()
            .iter()
            .map(|(ns, h)| (ns.clone(), *h))
            .collect();
        let mut lost = Vec::new();
        for (ns, held) in held {
            let started = Instant::now();
            let renewed = match &self.backend {
                LeaseBackend::ControlStore => controller.lock().unwrap().renew_writer_lease(
                    &ns,
                    &self.holder,
                    held.token,
                    self.ttl,
                ),
                #[cfg(feature = "postgres")]
                LeaseBackend::Postgres(store) => {
                    store.renew(&ns, &self.holder, held.token, self.ttl)
                }
            };
            match renewed {
                Ok(true) => {
                    if let Some(h) = self.held.lock().unwrap().get_mut(&ns) {
                        if h.token == held.token {
                            h.valid_until = started + self.ttl;
                        }
                    }
                }
                Ok(false) => lost.push(ns),
                Err(error) => eprintln!(
                    "WARN: writer lease renewal for {} failed: {error}; retrying next tick",
                    ns.0
                ),
            }
        }
        lost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_parsing() {
        assert!(enabled_from(None));
        assert!(enabled_from(Some("on")));
        assert!(!enabled_from(Some("off")));
        assert!(!enabled_from(Some(" OFF ")));
        assert_eq!(ttl_from(None), DEFAULT_TTL);
        assert_eq!(ttl_from(Some("junk")), DEFAULT_TTL);
        assert_eq!(ttl_from(Some("0")), DEFAULT_TTL);
        assert_eq!(ttl_from(Some("5000")), Duration::from_secs(5));
        assert_eq!(ttl_from(Some("10")), Duration::from_millis(300));
    }

    #[test]
    fn a_second_holder_is_refused_until_the_first_lapses() {
        let controller: Controller = Mutex::new(Box::new(kosha_control::Controller::new()));
        let ttl = Duration::from_millis(300);
        let a = WriterLeases::new(true, ttl, LeaseBackend::ControlStore);
        let b = WriterLeases::new(true, ttl, LeaseBackend::ControlStore);
        assert_ne!(a.holder(), b.holder());
        let ns = NamespaceId("n".into());
        assert_eq!(a.token(&ns, &controller).unwrap(), Some(1));
        assert!(matches!(
            b.token(&ns, &controller),
            Err(KoshaError::Fenced { .. })
        ));
        assert!(a.renew_all(&controller).is_empty());
        std::thread::sleep(ttl + Duration::from_millis(50));
        assert_eq!(b.token(&ns, &controller).unwrap(), Some(2));
        // A's renewal now finds B's token: A lost the namespace.
        assert_eq!(a.renew_all(&controller), vec![ns.clone()]);
        assert!(a.lost(&ns));
        assert!(a.is_fenced(&ns));
        let off = WriterLeases::new(false, ttl, LeaseBackend::ControlStore);
        assert_eq!(off.token(&ns, &controller).unwrap(), None);
    }
}
