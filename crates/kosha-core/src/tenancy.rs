//! Tenant identity and fair cache shares (#342).
//!
//! A tenant-scoped namespace id is `<tenant>/<name>` (#262); a branch is a
//! sibling `<tenant>/<branch>` and a shard nests under it as
//! `<tenant>/<name>/_shard-NNNN`. Bare ids written before #262, legacy
//! shards (`<name>/_shard-NNNN`) and internal prefixes (`_wal/...`) all
//! belong to one shared [`LEGACY_TENANT`].
//!
//! [`TenantShares`] is the eviction policy every process-wide cache uses
//! to keep one tenant's cold working set from flushing everybody else's:
//! the cache budget is split across the tenants holding bytes in it, by
//! weight, and a victim is taken from the tenant furthest over its share.
//! A tenant is never taken below a guaranteed floor
//! (`KOSHA_CACHE_TENANT_MIN_BYTES`) while another tenant is over its share.

use std::collections::HashMap;
use std::sync::Arc;

/// The tenant of every namespace id without one (pre-#262 bare ids,
/// legacy shards, internal prefixes).
pub const LEGACY_TENANT: &str = "_legacy";

/// Default for `KOSHA_CACHE_TENANT_MIN_BYTES`: 64 MiB.
pub const DEFAULT_TENANT_MIN_BYTES: u64 = 64 * 1024 * 1024;

/// The tenant a namespace id belongs to: its first component when it has
/// more than one, else [`LEGACY_TENANT`].
pub fn tenant_of_namespace(namespace: &str) -> &str {
    let namespace = namespace.trim_start_matches('/');
    let Some((tenant, rest)) = namespace.split_once('/') else {
        return LEGACY_TENANT;
    };
    if tenant.is_empty() || tenant.starts_with('_') || tenant.starts_with('.') || rest.is_empty() {
        return LEGACY_TENANT;
    }
    // `<name>/_shard-NNNN` is a shard of a bare namespace, not a tenant.
    if !rest.contains('/') && is_shard_component(rest) {
        return LEGACY_TENANT;
    }
    tenant
}

/// The tenant of a segment directory key, `{namespace}/{segment}`.
pub fn tenant_of_segment_dir(key: &str) -> &str {
    match key.trim_matches('/').rsplit_once('/') {
        Some((namespace, _segment)) => tenant_of_namespace(namespace),
        None => LEGACY_TENANT,
    }
}

/// Suffix of a per-file span-cache directory (`{file}.spans/`), mirrored
/// from `kosha_segment::span_cache::SPANS_SUFFIX`.
const SPANS_SUFFIX: &str = ".spans";

/// The tenant of a segment file key, `{namespace}/{segment}/{file}` — the
/// shape of every disk-cache key and S3 object path under a namespace.
/// A span-cache file, `{namespace}/{segment}/{file}.spans/{span}`, belongs
/// to the same namespace as `{file}`. Anything shorter has no namespace and
/// is [`LEGACY_TENANT`].
pub fn tenant_of_file_key(key: &str) -> &str {
    let mut key = key.trim_matches('/');
    // Strip the span name so `{file}.spans` takes the file's place.
    if let Some((parent, _span)) = key.rsplit_once('/') {
        let dir = parent.rsplit_once('/').map_or(parent, |(_, d)| d);
        if dir.len() > SPANS_SUFFIX.len() && dir.ends_with(SPANS_SUFFIX) {
            key = parent;
        }
    }
    let mut parts = key.rsplitn(3, '/');
    let (Some(_file), Some(_segment), Some(namespace)) = (parts.next(), parts.next(), parts.next())
    else {
        return LEGACY_TENANT;
    };
    tenant_of_namespace(namespace)
}

/// True for a tenant id, or a tenant-supplied namespace or branch name,
/// that would be mistaken for an internal or legacy layout: a leading `_`
/// (shard components such as `_shard-0001`, internal prefixes) or `.`.
/// Stored as `<tenant>/_shard-NNNN`, a tenant's namespace would be
/// accounted to [`LEGACY_TENANT`], taking it out of its own cache share and
/// hydration cap, so such names are reserved.
pub fn is_reserved_name(name: &str) -> bool {
    name.starts_with('_') || name.starts_with('.')
}

fn is_shard_component(component: &str) -> bool {
    component
        .strip_prefix("_shard-")
        .is_some_and(|d| d.len() == 4 && d.bytes().all(|b| b.is_ascii_digit()))
}

/// A tenant's relative cache weight (at least 1).
pub type TenantWeightFn = Arc<dyn Fn(&str) -> u32 + Send + Sync>;

/// How a cache budget is split between tenants. See the module doc.
#[derive(Clone)]
pub struct TenantShares {
    /// Guaranteed floor per active tenant, before capping at an equal split.
    min_bytes: u64,
    weight: Option<TenantWeightFn>,
}

impl Default for TenantShares {
    fn default() -> Self {
        Self::new(DEFAULT_TENANT_MIN_BYTES)
    }
}

impl std::fmt::Debug for TenantShares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantShares")
            .field("min_bytes", &self.min_bytes)
            .field("weighted", &self.weight.is_some())
            .finish()
    }
}

impl TenantShares {
    /// Every tenant weighs 1; `min_bytes` is the guaranteed floor.
    pub fn new(min_bytes: u64) -> Self {
        Self {
            min_bytes,
            weight: None,
        }
    }

    /// `KOSHA_CACHE_TENANT_MIN_BYTES` (default 64 MiB), equal weights.
    pub fn from_env() -> Self {
        let min_bytes = std::env::var("KOSHA_CACHE_TENANT_MIN_BYTES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(DEFAULT_TENANT_MIN_BYTES);
        Self::new(min_bytes)
    }

    /// Weigh tenants by `weight` (values below 1 count as 1).
    pub fn with_weights(mut self, weight: Option<TenantWeightFn>) -> Self {
        self.weight = weight;
        self
    }

    /// The same weights with another floor (for a cache bounded in some
    /// other unit, such as entries).
    pub fn with_min_bytes(mut self, min_bytes: u64) -> Self {
        self.min_bytes = min_bytes;
        self
    }

    pub fn min_bytes(&self) -> u64 {
        self.min_bytes
    }

    pub fn weight(&self, tenant: &str) -> u32 {
        self.weight.as_ref().map_or(1, |w| w(tenant).max(1))
    }

    /// The floor each of `active` tenants is guaranteed under `budget`:
    /// `min_bytes`, capped at an equal split so the floors always fit.
    pub fn floor(&self, budget: u64, active: usize) -> u64 {
        self.min_bytes.min(budget / active.max(1) as u64)
    }

    /// Each active tenant's fair share of `budget`, by weight.
    pub fn shares<'a>(&self, budget: u64, usage: &[(&'a str, u64)]) -> Vec<(&'a str, u64)> {
        let active: Vec<(&str, u32)> = usage
            .iter()
            .filter(|(_, bytes)| *bytes > 0)
            .map(|(t, _)| (*t, self.weight(t)))
            .collect();
        let total: u128 = active.iter().map(|(_, w)| *w as u128).sum::<u128>().max(1);
        active
            .into_iter()
            .map(|(t, w)| (t, (budget as u128 * w as u128 / total) as u64))
            .collect()
    }

    /// The tenant a cache over `budget` should evict from next.
    ///
    /// `usage` is each tenant's bytes in the cache. `victim(tenant)` is the
    /// size of the entry that tenant would lose (its least recently used
    /// evictable entry), or `None` when it has nothing evictable.
    ///
    /// Tenants whose eviction keeps them at or above their floor come
    /// first; among those, tenants over their share before tenants under
    /// it; and then the one furthest over (or least under) its share. A
    /// tenant is taken below its floor only when no tenant can give
    /// without dropping below its own, and then the tenant over its share
    /// goes first — so the floor holds whenever anybody else has bytes to
    /// spare, whatever the weights.
    pub fn pick_victim<'a>(
        &self,
        budget: u64,
        usage: &[(&'a str, u64)],
        mut victim: impl FnMut(&str) -> Option<u64>,
    ) -> Option<&'a str> {
        let shares = self.shares(budget, usage);
        let floor = self.floor(budget, shares.len());
        let mut best: Option<((bool, bool), i128, &'a str)> = None;
        for (tenant, share) in shares {
            let bytes = usage
                .iter()
                .find(|(t, _)| *t == tenant)
                .map_or(0, |(_, b)| *b);
            let Some(size) = victim(tenant) else {
                continue;
            };
            let excess = bytes as i128 - share as i128;
            let over = excess > 0;
            let keeps_floor = bytes.saturating_sub(size) >= floor;
            // Ordering key: floor kept first, then over share, then excess.
            let rank = (keeps_floor, over);
            let better = match &best {
                None => true,
                Some((best_rank, best_excess, _)) => (rank, excess) > (*best_rank, *best_excess),
            };
            if better {
                best = Some((rank, excess, tenant));
            }
        }
        best.map(|(_, _, tenant)| tenant)
    }
}

/// One tenant's footprint and traffic in one cache, for
/// `kosha_cache_{bytes,hits,misses,evictions}{cache,tenant}`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantCacheStats {
    pub tenant: String,
    /// Bytes the tenant holds in the cache now.
    pub bytes: u64,
    /// Lookups that found their entry (monotonic).
    pub hits: u64,
    /// Lookups that did not (monotonic).
    pub misses: u64,
    /// Entries removed from the cache (monotonic).
    pub evictions: u64,
}

/// Per-tenant byte and traffic accounting a cache keeps beside its own
/// entries. Counters survive a tenant dropping to zero bytes, so they stay
/// monotonic for Prometheus.
#[derive(Debug, Default)]
pub struct TenantLedger {
    tenants: HashMap<String, TenantCacheStats>,
}

impl TenantLedger {
    fn entry(&mut self, tenant: &str) -> &mut TenantCacheStats {
        if !self.tenants.contains_key(tenant) {
            self.tenants.insert(
                tenant.to_string(),
                TenantCacheStats {
                    tenant: tenant.to_string(),
                    ..Default::default()
                },
            );
        }
        self.tenants.get_mut(tenant).expect("just inserted")
    }

    pub fn add_bytes(&mut self, tenant: &str, bytes: u64) {
        let e = self.entry(tenant);
        e.bytes = e.bytes.saturating_add(bytes);
    }

    pub fn sub_bytes(&mut self, tenant: &str, bytes: u64) {
        let e = self.entry(tenant);
        e.bytes = e.bytes.saturating_sub(bytes);
    }

    pub fn hit(&mut self, tenant: &str) {
        self.entry(tenant).hits += 1;
    }

    pub fn miss(&mut self, tenant: &str) {
        self.entry(tenant).misses += 1;
    }

    /// Count `n` hits at once (no-op for zero).
    pub fn hits(&mut self, tenant: &str, n: u64) {
        if n > 0 {
            self.entry(tenant).hits += n;
        }
    }

    /// Count `n` misses at once (no-op for zero).
    pub fn misses(&mut self, tenant: &str, n: u64) {
        if n > 0 {
            self.entry(tenant).misses += n;
        }
    }

    pub fn evicted(&mut self, tenant: &str) {
        self.entry(tenant).evictions += 1;
    }

    pub fn bytes(&self, tenant: &str) -> u64 {
        self.tenants.get(tenant).map_or(0, |e| e.bytes)
    }

    /// Bytes per tenant holding any, for [`TenantShares::pick_victim`].
    pub fn usage(&self) -> Vec<(&str, u64)> {
        self.tenants
            .values()
            .filter(|e| e.bytes > 0)
            .map(|e| (e.tenant.as_str(), e.bytes))
            .collect()
    }

    /// Zero every tenant's bytes (the cache was cleared); counters stay.
    pub fn clear_bytes(&mut self) {
        for e in self.tenants.values_mut() {
            e.bytes = 0;
        }
    }

    /// Every tenant seen, sorted by tenant id.
    pub fn stats(&self) -> Vec<TenantCacheStats> {
        let mut out: Vec<_> = self.tenants.values().cloned().collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenants_come_from_the_first_namespace_component() {
        assert_eq!(tenant_of_namespace("acme/docs"), "acme");
        assert_eq!(tenant_of_namespace("acme/docs/_shard-0001"), "acme");
        assert_eq!(tenant_of_namespace("docs"), LEGACY_TENANT);
        assert_eq!(tenant_of_namespace("docs/_shard-0001"), LEGACY_TENANT);
        assert_eq!(tenant_of_namespace("_wal/x"), LEGACY_TENANT);
        assert_eq!(tenant_of_file_key("acme/docs/seg-1/a.bin"), "acme");
        assert_eq!(
            tenant_of_file_key("acme/docs/_shard-0002/seg/a.bin"),
            "acme"
        );
        assert_eq!(tenant_of_file_key("docs/seg-1/a.bin"), LEGACY_TENANT);
        assert_eq!(
            tenant_of_file_key("docs/_shard-0002/seg/a.bin"),
            LEGACY_TENANT
        );
        assert_eq!(tenant_of_file_key("first"), LEGACY_TENANT);
        // Span-cache files sit one level below their file.
        assert_eq!(
            tenant_of_file_key("docs/seg/doc_store.bin.spans/0-10"),
            LEGACY_TENANT
        );
        assert_eq!(
            tenant_of_file_key("docs/_shard-0001/seg/x.spans/0-10"),
            LEGACY_TENANT
        );
        assert_eq!(
            tenant_of_file_key("acme/docs/seg/doc_store.bin.spans/0-10"),
            "acme"
        );
        assert_eq!(
            tenant_of_file_key("acme/docs/_shard-0001/seg/postings-00.bin.spans/0-10"),
            "acme"
        );
        assert!(is_reserved_name("_shard-0001"));
        assert!(is_reserved_name(".x"));
        assert!(!is_reserved_name("docs"));
        assert_eq!(tenant_of_segment_dir("acme/docs/seg-1"), "acme");
        assert_eq!(tenant_of_segment_dir("docs/seg-1"), LEGACY_TENANT);
    }

    #[test]
    fn victims_come_from_the_tenant_furthest_over_its_share() {
        let shares = TenantShares::new(100);
        // Budget 1000, two tenants → 500 each. A holds 900, B 300.
        let usage = [("a", 900), ("b", 300)];
        assert_eq!(shares.pick_victim(1000, &usage, |_| Some(10)), Some("a"));
        // B over its share, A under: B goes.
        let usage = [("a", 400), ("b", 700)];
        assert_eq!(shares.pick_victim(1000, &usage, |_| Some(10)), Some("b"));
        // The over-share tenant has nothing evictable (all pinned): the
        // other one must give rather than the cache staying over budget.
        assert_eq!(
            shares.pick_victim(1000, &usage, |t| (t == "a").then_some(10)),
            Some("a")
        );
    }

    #[test]
    fn weights_shift_shares() {
        let shares =
            TenantShares::new(0).with_weights(Some(Arc::new(
                |t: &str| {
                    if t == "big" {
                        3
                    } else {
                        1
                    }
                },
            )));
        let usage = [("big", 700), ("small", 300)];
        assert_eq!(
            shares.shares(1000, &usage),
            vec![("big", 750), ("small", 250)]
        );
        // small (300 > 250) is the one over its share.
        assert_eq!(
            shares.pick_victim(1000, &usage, |_| Some(10)),
            Some("small")
        );
    }

    #[test]
    fn floor_protects_a_low_weight_tenant_while_another_is_over_share() {
        // Weight 9:1 → small's share is 100, but its floor is 200.
        let shares =
            TenantShares::new(200).with_weights(Some(Arc::new(
                |t: &str| {
                    if t == "big" {
                        9
                    } else {
                        1
                    }
                },
            )));
        // big 950 (over 900), small 200 (over 100 but at its floor).
        let usage = [("big", 950), ("small", 200)];
        assert_eq!(shares.pick_victim(1000, &usage, |_| Some(50)), Some("big"));
        // Even when big is under its share, small keeps its floor while big
        // has bytes above its own.
        let usage = [("big", 850), ("small", 200)];
        assert_eq!(shares.pick_victim(1000, &usage, |_| Some(50)), Some("big"));
        // Small above its floor and over its share gives first.
        let usage = [("big", 700), ("small", 400)];
        assert_eq!(
            shares.pick_victim(1000, &usage, |_| Some(50)),
            Some("small")
        );
        // Nobody can give without breaking a floor: the over-share one goes.
        let shares = TenantShares::new(500);
        let usage = [("a", 600), ("b", 500)];
        assert_eq!(shares.pick_victim(1000, &usage, |_| Some(200)), Some("a"));
        // The floor is capped at an equal split so floors always fit.
        assert_eq!(TenantShares::new(10_000).floor(1000, 4), 250);
    }
}
