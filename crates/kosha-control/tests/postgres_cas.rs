//! Opt-in database tests. Use a dedicated disposable database:
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test postgres_cas -- --ignored
#![cfg(feature = "postgres")]
use kosha_control::PgStore;
use kosha_core::{ControlStore, KoshaError, Manifest, ManifestEntry, NamespaceId, SegmentId};
use std::sync::{Arc, Barrier};

fn candidate(segment: &str, version: u64) -> Manifest {
    Manifest {
        version,
        segments: vec![ManifestEntry {
            segment_id: SegmentId(segment.into()),
            doc_count: 1,
        }],
        segment_footers: Default::default(),
        footer_crc32: Default::default(),
        wal_checkpoint: 1,
        wal_origin: Some(segment.into()),
        deletion_vectors: Default::default(),
        branch: None,
        schema: None,
        retired: Default::default(),
        shards: None,
        tombstones: [(SegmentId(segment.into()), (0..256).collect())]
            .into_iter()
            .collect(),
    }
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_cas_race_and_ambiguous_commit_retry() {
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    // Initialize schema before opening independent concurrent connections.
    let observer = PgStore::new(&url).unwrap();
    let a = PgStore::new(&url).unwrap();
    let b = PgStore::new(&url).unwrap();
    let ns = NamespaceId(format!("cas-test-{}", uuid::Uuid::new_v4()));
    let barrier = Arc::new(Barrier::new(2));
    let outcomes = std::thread::scope(|scope| {
        let spawn = |mut store: PgStore, segment: &'static str| {
            let ns = ns.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                let snapshot = candidate(segment, 1);
                barrier.wait();
                let result = store.compare_and_swap_manifest(&ns, 0, snapshot.clone());
                (store, snapshot, result)
            })
        };
        let first = spawn(a, "a");
        let second = spawn(b, "b");
        (first.join().unwrap(), second.join().unwrap())
    });
    let ((mut a, ca, ra), (mut b, cb, rb)) = outcomes;
    assert_eq!(usize::from(ra.is_ok()) + usize::from(rb.is_ok()), 1);
    let (winner, committed, loser, rejected) = if ra.is_ok() {
        (&mut a, ca, &mut b, cb)
    } else {
        (&mut b, cb, &mut a, ca)
    };
    assert!(observer
        .manifest_cloned(&ns)
        .unwrap()
        .same_contents(&committed));
    // Simulate retrying after losing the success reply: the old expected
    // version is accepted only if the full candidate already committed.
    winner
        .compare_and_swap_manifest(&ns, 0, committed.clone())
        .unwrap();
    assert!(matches!(
        loser.compare_and_swap_manifest(&ns, 0, rejected.clone()),
        Err(KoshaError::ManifestConflict { .. })
    ));
    assert!(
        loser.compare_and_swap_manifest(&ns, 1, rejected).is_err(),
        "same version cannot authorize different contents"
    );
    let mut next = committed.clone();
    next.version = 2;
    next.segments.push(ManifestEntry {
        segment_id: SegmentId("next".into()),
        doc_count: 1,
    });
    winner
        .compare_and_swap_manifest(&ns, 1, next.clone())
        .unwrap();
    assert!(
        winner.compare_and_swap_manifest(&ns, 0, committed).is_err(),
        "an old retry cannot rewind a newer commit"
    );
    assert!(observer.manifest_cloned(&ns).unwrap().same_contents(&next));
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_manifest_versions_poll_and_missing_namespace() {
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let mut store = PgStore::new(&url).unwrap();
    let ns = NamespaceId(format!("versions-test-{}", uuid::Uuid::new_v4()));
    store
        .compare_and_swap_manifest(&ns, 0, candidate("versions", 3))
        .unwrap();
    let versions = store.manifest_versions().unwrap();
    assert!(versions.contains(&(ns.clone(), 3)), "{versions:?}");
    assert_eq!(store.manifest_version(&ns).unwrap(), Some(3));
    let missing = NamespaceId(format!("missing-{}", uuid::Uuid::new_v4()));
    assert_eq!(store.manifest_version(&missing).unwrap(), None);
    assert!(store.try_manifest_cloned(&missing).unwrap().is_none());
    assert_eq!(store.try_manifest_cloned(&ns).unwrap().unwrap().version, 3);
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_round_trips_retirement_records_and_gc_prunes_them_by_cas() {
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let mut store = PgStore::new(&url).unwrap();
    let ns = NamespaceId(format!("gc-test-{}", uuid::Uuid::new_v4()));
    let mut first = candidate("a", 1);
    first.tombstones.clear();
    store.compare_and_swap_manifest(&ns, 0, first).unwrap();
    let mut compacted = candidate("merged", 2);
    compacted.tombstones.clear();
    compacted.retired = vec![kosha_core::gc::RetiredObject {
        segment_id: SegmentId("a".into()),
        deletion_generation: None,
        retired_at_ms: 1_700_000_000_000,
        retired_in_version: 2,
    }];
    store
        .compare_and_swap_manifest(&ns, 1, compacted.clone())
        .unwrap();
    let stored = store.manifest_cloned(&ns).unwrap();
    assert_eq!(stored.retired, compacted.retired);
    // An identical retry (lost reply) is still idempotent with records.
    store
        .compare_and_swap_manifest(&ns, 1, compacted.clone())
        .unwrap();
    // GC's prune is an ordinary CAS from the version it planned against.
    let mut pruned = stored.clone();
    pruned.retired.clear();
    pruned.version = 3;
    store.compare_and_swap_manifest(&ns, 2, pruned).unwrap();
    let after = store.manifest_cloned(&ns).unwrap();
    assert!(after.retired.is_empty());
    assert_eq!(after.version, 3);
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_branch_manifest_round_trips_and_remove_namespace() {
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let mut store = PgStore::new(&url).unwrap();
    let parent = NamespaceId(format!("branch-parent-{}", uuid::Uuid::new_v4()));
    let child = NamespaceId(format!("branch-child-{}", uuid::Uuid::new_v4()));
    store
        .compare_and_swap_manifest(&parent, 0, candidate("shared", 4))
        .unwrap();
    let branch = store
        .try_manifest_cloned(&parent)
        .unwrap()
        .unwrap()
        .branch_from(&parent, 1);
    store
        .compare_and_swap_manifest(&child, 0, branch.clone())
        .unwrap();
    let stored = store.try_manifest_cloned(&child).unwrap().unwrap();
    assert!(stored.same_contents(&branch));
    let shared = SegmentId("shared".into());
    assert_eq!(stored.segment_home(&child, &shared), &parent);

    let removed = store.remove_namespace(&child).unwrap().unwrap();
    assert_eq!(removed.version, 1);
    assert!(!store.has_namespace(&child));
    assert!(store.try_manifest_cloned(&child).unwrap().is_none());
    assert!(store.remove_namespace(&child).unwrap().is_none());
    // The parent is untouched.
    assert_eq!(store.manifest_version(&parent).unwrap(), Some(4));
}

/// #254: two writers race for one namespace's lease and publish
/// concurrently with their tokens. Exactly one holds the lease and
/// commits; the other is refused the lease and its fenced CAS is rejected.
/// After the winner's lease expires, a takeover bumps the token and the
/// old holder's CAS is fenced even over the right version.
#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_fenced_cas_race_and_takeover() {
    use kosha_control::PgWriterLeaseStore;
    use std::time::Duration;

    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let observer = PgStore::new(&url).unwrap();
    let a = PgStore::new(&url).unwrap();
    let b = PgStore::new(&url).unwrap();
    let ns = NamespaceId(format!("fence-test-{}", uuid::Uuid::new_v4()));
    let ttl = Duration::from_millis(500);
    let barrier = Arc::new(Barrier::new(2));
    let outcomes = std::thread::scope(|scope| {
        let spawn = |mut store: PgStore, holder: &'static str| {
            let ns = ns.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                let token = store.acquire_writer_lease(&ns, holder, ttl).unwrap();
                // A writer refused the lease still tries with a guess: it
                // must be fenced, not committed.
                let presented = token.unwrap_or(1) + u64::from(token.is_none());
                let result = store.compare_and_swap_manifest_fenced(
                    &ns,
                    0,
                    candidate(holder, 1),
                    Some(presented),
                );
                (store, token, result)
            })
        };
        let first = spawn(a, "a");
        let second = spawn(b, "b");
        (first.join().unwrap(), second.join().unwrap())
    });
    let ((mut a, ta, ra), (mut b, tb, rb)) = outcomes;
    assert_eq!(usize::from(ta.is_some()) + usize::from(tb.is_some()), 1);
    assert_eq!(usize::from(ra.is_ok()) + usize::from(rb.is_ok()), 1);
    let (winner, winner_token, loser, loser_holder) = if ra.is_ok() {
        assert_eq!(ta, Some(1));
        assert!(matches!(rb, Err(KoshaError::Fenced { .. })), "{rb:?}");
        (&mut a, ta.unwrap(), &mut b, "b")
    } else {
        assert_eq!(tb, Some(1));
        assert!(matches!(ra, Err(KoshaError::Fenced { .. })), "{ra:?}");
        (&mut b, tb.unwrap(), &mut a, "a")
    };
    assert_eq!(observer.manifest_cloned(&ns).unwrap().version, 1);

    // The winner stalls past its TTL; the loser takes over through the
    // dedicated lease pool, with token + 1.
    std::thread::sleep(ttl + Duration::from_millis(300));
    let leases = PgWriterLeaseStore::new(&url).unwrap();
    let taken = leases.acquire(&ns, loser_holder, ttl).unwrap().unwrap();
    assert_eq!(taken, winner_token + 1);
    assert!(!leases
        .renew(&ns, "a-stale-holder", winner_token, ttl)
        .unwrap());
    let stale =
        winner.compare_and_swap_manifest_fenced(&ns, 1, candidate("late", 2), Some(winner_token));
    assert!(
        matches!(stale, Err(KoshaError::Fenced { token, current, .. }) if token == winner_token && current == taken),
        "{stale:?}"
    );
    loser
        .compare_and_swap_manifest_fenced(&ns, 1, candidate("new", 2), Some(taken))
        .unwrap();
    // Unfenced CAS (old binaries, KOSHA_WRITER_LEASE=off) is unchanged.
    winner
        .compare_and_swap_manifest(&ns, 2, candidate("plain", 3))
        .unwrap();
    leases.release(&ns, loser_holder, taken).unwrap();
}
