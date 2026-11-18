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
        wal_checkpoint: 1,
        wal_origin: Some(segment.into()),
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
