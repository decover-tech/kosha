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

/// #344: `migrate-namespace`'s control-plane step. The rename moves the
/// namespace and manifest rows in one transaction, carries the lease token,
/// fences the old id and retires it; a collision or a moved version
/// changes nothing.
#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_rename_namespace_rekeys_rows_in_one_transaction() {
    use kosha_control::NamespaceRename;
    use std::time::Duration;

    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let mut store = PgStore::new(&url).unwrap();
    let observer = PgStore::new(&url).unwrap();
    let id = uuid::Uuid::new_v4();
    let from = NamespaceId(format!("bare-{id}"));
    let to = NamespaceId(format!("tenant-{id}/corpus"));
    let ttl = Duration::from_secs(60);

    let mut manifest = candidate("bare-000001", 1);
    manifest.tombstones.clear();
    manifest.deletion_vectors.insert(
        SegmentId("bare-000001".into()),
        kosha_core::deletes::DeletionVector {
            generation: "a".repeat(64),
            count: 1,
            namespace: Some(from.clone()),
        },
    );
    store.compare_and_swap_manifest(&from, 0, manifest).unwrap();
    let token = store
        .acquire_writer_lease(&from, "migrator", ttl)
        .unwrap()
        .unwrap();

    // A moved version is refused and changes nothing.
    assert!(matches!(
        store.rename_namespace(&from, &to, 7),
        Err(KoshaError::ManifestConflict {
            actual: Some(1),
            ..
        })
    ));
    assert!(observer.has_namespace(&from));
    assert!(!observer.has_namespace(&to));

    // A collision is refused and changes nothing.
    let taken = NamespaceId(format!("tenant-{id}/taken"));
    store.create_namespace(taken.clone()).unwrap();
    assert!(matches!(
        store.rename_namespace(&from, &taken, 1),
        Err(KoshaError::NamespaceExists(ref existing)) if existing == &taken
    ));
    assert_eq!(observer.manifest_version(&from).unwrap(), Some(1));
    assert_eq!(observer.manifest_version(&taken).unwrap(), Some(0));

    let outcome = store.rename_namespace(&from, &to, 1).unwrap();
    assert_eq!(outcome.lease_token, Some(token));
    assert!(!observer.has_namespace(&from));
    assert_eq!(observer.manifest_version(&from).unwrap(), None);
    let moved = observer.try_manifest_cloned(&to).unwrap().unwrap();
    assert_eq!(moved.version, 1);
    assert_eq!(moved.segments[0].segment_id.0, "bare-000001");
    assert_eq!(
        moved.deletion_vectors[&SegmentId("bare-000001".into())].namespace,
        Some(to.clone())
    );
    // The holder keeps `to` under the carried token; `from` is fenced.
    assert!(store
        .renew_writer_lease(&to, "migrator", token, ttl)
        .unwrap());
    assert!(!store
        .renew_writer_lease(&from, "migrator", token, ttl)
        .unwrap());
    store.release_writer_lease(&to, "migrator", token).unwrap();
    // Renaming again finds nothing to move.
    assert!(matches!(
        store.rename_namespace(&from, &to, 1),
        Err(KoshaError::NamespaceNotFound(_))
    ));
    // The bare id was retired: re-creating it starts above version 1.
    store.create_namespace(from.clone()).unwrap();
    assert_eq!(observer.manifest_version(&from).unwrap(), Some(2));
}

/// `url` with its database name replaced by `db` (query string kept).
fn with_database(url: &str, db: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let authority_start = base.find("://").map_or(0, |i| i + 3);
    let server = match base[authority_start..].find('/') {
        Some(slash) => &base[..authority_start + slash],
        None => base,
    };
    match query {
        Some(query) => format!("{server}/{db}?{query}"),
        None => format!("{server}/{db}"),
    }
}

/// Run `sql` on `url` over a fresh connection, on its own runtime.
fn admin_exec(url: &str, sql: &str) {
    use sqlx::Connection;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut conn = sqlx::PgConnection::connect(url).await.unwrap();
        sqlx::raw_sql(sql).execute(&mut conn).await.unwrap();
        conn.close().await.unwrap();
    });
}

/// #330: several processes constructing `PgStore` against a database that
/// has never been migrated must all succeed. Before the advisory lock,
/// concurrent `CREATE SCHEMA IF NOT EXISTS kosha` failed with a duplicate
/// key on `pg_namespace`. Uses its own freshly created database on the
/// server of KOSHA_TEST_DATABASE_URL, so it neither depends on nor disturbs
/// the schema the other tests in this file share.
#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn concurrent_first_boot_migrations_all_succeed() {
    const CONSTRUCTORS: usize = 8;
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let db = format!("kosha_migrate_{}", uuid::Uuid::new_v4().simple());
    admin_exec(&url, &format!("CREATE DATABASE {db}"));
    let fresh = with_database(&url, &db);

    let barrier = Arc::new(Barrier::new(CONSTRUCTORS));
    let results: Vec<Result<PgStore, String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..CONSTRUCTORS)
            .map(|_| {
                let barrier = barrier.clone();
                let fresh = fresh.as_str();
                scope.spawn(move || {
                    barrier.wait();
                    PgStore::new(fresh)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let failures: Vec<&String> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert!(failures.is_empty(), "constructors failed: {failures:?}");

    // The migrated schema is usable from every store.
    let mut stores: Vec<PgStore> = results.into_iter().map(Result::unwrap).collect();
    let ns = NamespaceId(format!("migrate-test-{}", uuid::Uuid::new_v4()));
    stores[0]
        .compare_and_swap_manifest(&ns, 0, candidate("first", 1))
        .unwrap();
    for store in &stores {
        assert_eq!(store.manifest_cloned(&ns).unwrap().version, 1);
    }

    drop(stores);
    admin_exec(&url, &format!("DROP DATABASE {db} WITH (FORCE)"));
}

#[test]
fn with_database_replaces_only_the_database_name() {
    assert_eq!(
        with_database("postgres://u:p@h:5432/kosha_cas_test", "x"),
        "postgres://u:p@h:5432/x"
    );
    assert_eq!(
        with_database("postgres://u:p@h/db?sslmode=disable", "x"),
        "postgres://u:p@h/x?sslmode=disable"
    );
    assert_eq!(
        with_database("postgres://h:5432", "x"),
        "postgres://h:5432/x"
    );
}

/// #347: each pod's cache purge pages through every deletion record, of
/// any state, in `(requested_at_ms, id)` order, and re-reads a window
/// behind the newest it has seen.
#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_pages_through_deletion_records_in_position_order() {
    use kosha_core::deletion::{DeletionPosition, DeletionRecord, NewDeletion};
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let mut store = PgStore::new(&url).unwrap();
    let tenant = format!("page-test-{}", uuid::Uuid::new_v4());
    let mut issued: Vec<DeletionRecord> = Vec::new();
    for i in 0..5u64 {
        let subject = format!("{tenant}/ns{i}");
        let request = NewDeletion::namespace(&subject, Some(&tenant), None);
        issued.push(store.begin_deletion(&request, i).unwrap());
    }
    store
        .record_deletion_attempt(&issued[1].id, 0, None)
        .unwrap();
    let ours = |records: Vec<DeletionRecord>| -> Vec<DeletionRecord> {
        records
            .into_iter()
            .filter(|r| r.tenant_id.as_deref() == Some(tenant.as_str()))
            .collect()
    };

    // From the start, two at a time: the whole table in order, each record
    // once (other tests' rows are in it too).
    let mut all: Vec<DeletionRecord> = Vec::new();
    let mut after: Option<DeletionPosition> = None;
    loop {
        let page = store.deletions_after(after.as_ref(), 2).unwrap();
        assert!(page.len() <= 2);
        let Some(last) = page.last() else { break };
        after = Some(last.position());
        all.extend(page);
    }
    let positions: Vec<DeletionPosition> = all.iter().map(DeletionRecord::position).collect();
    let mut sorted = positions.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(positions, sorted, "ordered, no record twice");
    let mine = ours(all);
    assert_eq!(mine.len(), 5);
    assert_eq!(mine.iter().filter(|r| r.is_completed()).count(), 1);

    // A window: everything requested at or after a record's millisecond.
    let oldest = issued.iter().map(|r| r.requested_at_ms).min().unwrap();
    let window = ours(
        store
            .deletions_after(Some(&DeletionPosition::at_ms(oldest)), 1000)
            .unwrap(),
    );
    assert_eq!(window.len(), 5);
    // Strictly after the newest of ours: none of ours.
    let newest = issued.iter().map(DeletionRecord::position).max().unwrap();
    assert!(ours(store.deletions_after(Some(&newest), 1000).unwrap()).is_empty());
}

/// #347: the namespace's rows and its pending deletion record go in one
/// transaction; the record completes only on an attempt without an error,
/// and migration 004 leaves existing rows alone.
#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_records_namespace_and_tenant_deletions() {
    use kosha_core::deletion::{DeletionFilter, DeletionScope, DeletionState, NewDeletion};
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    let mut store = PgStore::new(&url).unwrap();
    // A second constructor re-runs every migration over the existing rows.
    let observer = PgStore::new(&url).unwrap();
    let tenant = format!("del-test-{}", uuid::Uuid::new_v4());
    let ns = NamespaceId(format!("{tenant}/docs"));
    let kept = NamespaceId(format!("{tenant}/kept"));
    let request = NewDeletion::namespace(&ns.0, Some(&tenant), None);

    // No namespace: nothing is recorded.
    assert!(matches!(
        store.delete_namespace_recorded(&ns, 0, &request),
        Err(KoshaError::NamespaceNotFound(_))
    ));
    let of_tenant = DeletionFilter {
        tenant_id: Some(tenant.clone()),
        ..DeletionFilter::default()
    };
    assert!(observer.deletions(&of_tenant).unwrap().is_empty());

    store
        .compare_and_swap_manifest(&ns, 0, candidate("a", 3))
        .unwrap();
    store
        .compare_and_swap_manifest(&kept, 0, candidate("k", 1))
        .unwrap();
    let (manifest, retired, record) = store.delete_namespace_recorded(&ns, 9, &request).unwrap();
    assert_eq!((manifest.version, retired), (3, 9));
    assert!(!observer.has_namespace(&ns));
    assert!(observer.manifest_version(&ns).unwrap().is_none());
    assert!(
        observer.has_namespace(&kept),
        "other namespaces are untouched"
    );
    assert!(record.id.starts_with("del-"));
    assert_eq!(record.scope, DeletionScope::Namespace);
    assert_eq!(record.subject, ns.0);
    assert_eq!(record.tenant_id.as_deref(), Some(tenant.as_str()));
    assert_eq!(record.last_version, 9);
    assert_eq!(record.state, DeletionState::Pending);
    assert_eq!((record.attempts, record.objects_deleted), (0, 0));
    assert!(record.requested_at_ms > 0);
    assert!(record.completed_at_ms.is_none());
    assert_eq!(observer.deletion(&record.id).unwrap(), Some(record.clone()));
    // A re-created namespace starts above the retired version.
    store.ensure_namespace(ns.clone());
    assert_eq!(observer.manifest_version(&ns).unwrap(), Some(10));

    let failed = store
        .record_deletion_attempt(&record.id, 4, Some("S3 delete_objects: throttled"))
        .unwrap();
    assert_eq!(failed.state, DeletionState::Pending);
    assert_eq!((failed.attempts, failed.objects_deleted), (1, 4));
    assert_eq!(
        failed.last_error.as_deref(),
        Some("S3 delete_objects: throttled")
    );
    let pending = DeletionFilter {
        state: Some(DeletionState::Pending),
        ..of_tenant.clone()
    };
    assert_eq!(observer.deletions(&pending).unwrap(), vec![failed]);

    let done = store.record_deletion_attempt(&record.id, 2, None).unwrap();
    assert_eq!(done.state, DeletionState::Completed);
    assert_eq!((done.attempts, done.objects_deleted), (2, 6));
    assert!(done.last_error.is_none());
    assert!(done.completed_at_ms.unwrap() >= done.requested_at_ms);
    assert!(observer.deletions(&pending).unwrap().is_empty());
    // Completed is final.
    let late = store
        .record_deletion_attempt(&record.id, 9, Some("late"))
        .unwrap();
    assert_eq!(late, done);
    assert!(matches!(
        store.record_deletion_attempt("del-missing", 0, None),
        Err(KoshaError::NotFound(_))
    ));
    assert_eq!(observer.deletion("del-missing").unwrap(), None);

    // A tenant record with a child, listed oldest first and by limit.
    let parent = store
        .begin_deletion(&NewDeletion::tenant(&tenant), 0)
        .unwrap();
    assert_eq!(parent.scope, DeletionScope::Tenant);
    let child = store
        .begin_deletion(
            &NewDeletion::namespace(&kept.0, Some(&tenant), Some(&parent.id)),
            1,
        )
        .unwrap();
    assert_eq!(child.parent_id.as_deref(), Some(parent.id.as_str()));
    let all: Vec<String> = observer
        .deletions(&of_tenant)
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(all.len(), 3);
    assert_eq!(all[0], record.id);
    assert!(all.contains(&parent.id) && all.contains(&child.id));
    let first = DeletionFilter {
        limit: Some(1),
        ..of_tenant.clone()
    };
    assert_eq!(observer.deletions(&first).unwrap().len(), 1);

    // Tenant keys: every key of the tenant goes, nobody else's.
    let new_key = |tenant: &str, description: &str| kosha_control::NewApiKey {
        tenant_id: tenant.to_string(),
        description: description.to_string(),
        scopes: kosha_control::Scopes::ALL,
        namespaces: None,
        expires_at: None,
    };
    let key = store
        .create_api_key(&new_key(&tenant, "to delete"))
        .unwrap()
        .api_key;
    let other_tenant = format!("{tenant}-other");
    let other_key = store
        .create_api_key(&new_key(&other_tenant, "kept"))
        .unwrap()
        .api_key;
    assert_eq!(
        observer.validate_api_key(&key).as_deref(),
        Some(tenant.as_str())
    );
    assert_eq!(store.delete_api_keys(&tenant).unwrap(), 1);
    assert_eq!(observer.validate_api_key(&key), None);
    assert_eq!(
        observer.validate_api_key(&other_key).as_deref(),
        Some(other_tenant.as_str())
    );
    assert_eq!(store.delete_api_keys(&tenant).unwrap(), 0);
    store.delete_api_keys(&other_tenant).unwrap();
}
