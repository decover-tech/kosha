//! Opt-in database tests for the usage store (#348). Use a dedicated
//! disposable database:
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test postgres_usage -- --ignored
#![cfg(feature = "postgres")]
use kosha_control::usage::{
    meter, usage_summary, MemoryUsageStore, PgUsageStore, UsageEvent, UsageStore, HOUR_MS,
};
use kosha_control::PgStore;

fn url() -> String {
    std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL")
}

/// A tenant no other run of these tests has used.
fn fresh_tenant() -> String {
    format!("usage-test-{}", uuid::Uuid::new_v4())
}

fn event(tenant: &str, id: &str, ns: &str, at_ms: u64, meters: &[(&str, u64)]) -> UsageEvent {
    UsageEvent {
        id: format!("{tenant}/{id}"),
        tenant: tenant.into(),
        namespace: ns.into(),
        at_ms,
        meters: meters.iter().map(|(m, q)| (m.to_string(), *q)).collect(),
    }
}

/// A month-like mix: writes, searches and hourly snapshots over two
/// namespaces, with a snapshot before the window and a gap inside it.
fn sample(tenant: &str) -> Vec<UsageEvent> {
    let h = HOUR_MS;
    let stored = |ns: &str, hour: u64, bytes: u64| {
        event(
            tenant,
            &format!("stored/{ns}/{hour}"),
            ns,
            hour * h,
            &[(meter::STORED_BYTES, bytes)],
        )
    };
    let query = |id: &str, ns: &str, at_ms: u64, returned: u64, fetched: u64| {
        event(
            tenant,
            id,
            ns,
            at_ms,
            &[
                (meter::QUERIES, 1),
                (meter::BYTES_RETURNED, returned),
                (meter::BYTES_FETCHED, fetched),
            ],
        )
    };
    vec![
        stored("docs", 99, 1_000),
        event(
            tenant,
            "w1",
            "docs",
            100 * h + 1,
            &[(meter::BYTES_WRITTEN, 4_096), (meter::DOCUMENTS_WRITTEN, 3)],
        ),
        query("q1", "docs", 100 * h + 2, 300, 9_000),
        query("q2", "docs", 100 * h + 3, 300, 0),
        stored("docs", 101, 5_000),
        query("q3", "docs", 101 * h + 59 * 60_000, 120, 0),
        query("q4", "docs", 102 * h, 80, 64),
        stored("logs", 100, 77),
        query("q5", "logs", 100 * h + 10, 10, 10),
        // Exactly at the end of the window: excluded.
        query("q6", "docs", 103 * h, 1, 1),
    ]
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_usage_store_is_idempotent_and_matches_the_reference_store() {
    let url = url();
    // Applies migration 005; a second boot re-runs it as a no-op.
    drop(PgStore::new(&url).unwrap());
    drop(PgStore::new(&url).unwrap());
    let pg = PgUsageStore::new(&url).unwrap();
    let tenant = fresh_tenant();
    let events = sample(&tenant);
    let rows: u64 = events.iter().map(|e| e.meters.len() as u64).sum();

    assert_eq!(pg.record(&events).unwrap(), rows);
    // The resend after a crash, from a second pod's connection.
    let other_pod = PgUsageStore::new(&url).unwrap();
    assert_eq!(other_pod.record(&events).unwrap(), 0);
    // A batch that repeats an event inside itself, plus one new event.
    let mut mixed = vec![events[2].clone(), events[2].clone()];
    mixed.push(event(
        &tenant,
        "w2",
        "docs",
        100 * HOUR_MS + 5,
        &[(meter::BYTES_WRITTEN, 4)],
    ));
    assert_eq!(pg.record(&mixed).unwrap(), 1);
    // A second hourly snapshot with the same id but another value loses.
    let late = event(
        &tenant,
        "stored/docs/101",
        "docs",
        101 * HOUR_MS,
        &[(meter::STORED_BYTES, 999_999)],
    );
    assert_eq!(pg.record(&[late]).unwrap(), 0);

    let reference = MemoryUsageStore::new();
    reference.record(&events).unwrap();
    reference.record(&mixed).unwrap();
    let (from, to) = (100 * HOUR_MS, 103 * HOUR_MS);
    let got = usage_summary(&pg, Some(&tenant), from, to).unwrap();
    assert_eq!(
        got,
        usage_summary(&reference, Some(&tenant), from, to).unwrap()
    );
    assert_eq!(
        pg.hourly(Some(&tenant), from, to).unwrap(),
        reference.hourly(Some(&tenant), from, to).unwrap()
    );
    let docs = got.iter().find(|u| u.namespace == "docs").unwrap();
    assert_eq!(docs.queries, 4);
    assert_eq!(docs.bytes_returned, 800);
    assert_eq!(docs.bytes_fetched, 9_064);
    assert_eq!(docs.bytes_written, 4_100);
    assert_eq!(docs.documents_written, 3);
    // q1 and q2 at the carried-in 1,000; q3 and q4 at hour 101's 5,000.
    assert_eq!(docs.bytes_queried, 2 * 1_000 + 2 * 5_000);
    assert_eq!((docs.stored_byte_hours, docs.stored_snapshots), (5_000, 1));
    let logs = got.iter().find(|u| u.namespace == "logs").unwrap();
    assert_eq!((logs.queries, logs.bytes_queried), (1, 77));

    // The unscoped (admin) read sees this tenant's rows among the others.
    let everyone = usage_summary(&pg, None, from, to).unwrap();
    assert_eq!(
        everyone
            .iter()
            .filter(|u| u.tenant == tenant)
            .collect::<Vec<_>>(),
        got.iter().collect::<Vec<_>>()
    );
    // And another tenant sees none of them.
    assert!(usage_summary(&pg, Some(&fresh_tenant()), from, to)
        .unwrap()
        .is_empty());
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_usage_store_takes_a_batch_larger_than_one_insert() {
    let url = url();
    drop(PgStore::new(&url).unwrap());
    let pg = PgUsageStore::new(&url).unwrap();
    let tenant = fresh_tenant();
    // 6,000 events x 2 meters = 12,000 rows: three INSERT chunks.
    let events: Vec<UsageEvent> = (0..6_000u64)
        .map(|i| {
            event(
                &tenant,
                &format!("q{i}"),
                "docs",
                i,
                &[(meter::QUERIES, 1), (meter::BYTES_RETURNED, i)],
            )
        })
        .collect();
    assert_eq!(pg.record(&events).unwrap(), 12_000);
    assert_eq!(pg.record(&events).unwrap(), 0);
    let usage = usage_summary(&pg, Some(&tenant), 0, HOUR_MS).unwrap();
    assert_eq!(usage[0].queries, 6_000);
    assert_eq!(usage[0].bytes_returned, (0..6_000u64).sum::<u64>());
}

/// Usage delivery must not fail a pod's startup: the store connects on
/// first use, and an unreachable database is an error per call.
#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_usage_store_connects_lazily() {
    // A port nothing listens on.
    let pg = PgUsageStore::new("postgres://kosha:kosha@127.0.0.1:1/kosha").unwrap();
    let tenant = fresh_tenant();
    assert!(pg
        .record(&[event(&tenant, "q", "docs", 0, &[(meter::QUERIES, 1)])])
        .is_err());
    assert!(pg.hourly(Some(&tenant), 0, HOUR_MS).is_err());
    // The same events reach a reachable store later.
    drop(PgStore::new(&url()).unwrap());
    let reachable = PgUsageStore::new(&url()).unwrap();
    assert_eq!(
        reachable
            .record(&[event(&tenant, "q", "docs", 0, &[(meter::QUERIES, 1)])])
            .unwrap(),
        1
    );
}
