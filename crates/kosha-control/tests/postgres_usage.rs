//! Opt-in database test for the usage store (#348). Use a dedicated
//! disposable database:
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test postgres_usage -- --ignored
#![cfg(feature = "postgres")]
use kosha_control::{Meter, PgStore, PgUsageStore, UsageEvent, UsageStore};

fn event(tenant: &str, ns: &str, meter: Meter, quantity: u64, ws: i64) -> UsageEvent {
    UsageEvent {
        event_id: UsageEvent::derive_id("test-pod", ws, ws + 60, tenant, ns, meter),
        tenant: tenant.into(),
        namespace: ns.into(),
        meter,
        quantity,
        window_start: ws,
        window_end: ws + 60,
        source_pod: "test-pod".into(),
    }
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn postgres_usage_append_is_idempotent_and_aggregates() {
    let url = std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
    // Runs the migrations (006 creates kosha.usage_events).
    PgStore::new(&url).unwrap();
    let store = PgUsageStore::new(&url).unwrap();
    let tenant = format!("usage-test-{}", uuid::Uuid::new_v4());
    let batch: Vec<UsageEvent> = (0..50)
        .flat_map(|i| {
            [
                event(&tenant, "a", Meter::Queries, i, i as i64 * 60),
                event(&tenant, "b", Meter::StorageBytes, 1000 + i, i as i64 * 60),
            ]
        })
        .collect();
    assert_eq!(store.append(&batch).unwrap(), 100);
    assert_eq!(store.append(&batch).unwrap(), 0);

    let raw = store.raw(&tenant, 0, 50 * 60).unwrap();
    assert_eq!(raw.len(), 100);
    assert_eq!(raw[0].window_start, 0);
    let rows = store.aggregate(&tenant, 0, 50 * 60, true).unwrap();
    let queries = rows.iter().find(|r| r.meter == Meter::Queries).unwrap();
    assert_eq!(queries.namespace.as_deref(), Some("a"));
    assert_eq!(queries.quantity, (0..50).sum::<u64>());
    let latest = store.latest(Some(&tenant), Meter::StorageBytes).unwrap();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].quantity, 1049);
}
