//! Opt-in database test for the automatic plaintext scrub (#346). Its own
//! test binary, because it rewrites every plaintext row of the table and
//! would race the plaintext assertions of `postgres_api_keys`. Use a
//! dedicated disposable database:
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test postgres_api_key_scrub -- --ignored
#![cfg(feature = "postgres")]
use kosha_control::{hash_api_key, PgStore, ScrubStatus};

fn url() -> String {
    std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL")
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Run SQL the registry has no method for: what an older server build or
/// an operator with `psql` would do.
fn sql<T>(query: &str, binds: &[&str]) -> Vec<T>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + Send + Unpin,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let pool = sqlx::PgPool::connect(&url()).await.unwrap();
        let mut q = sqlx::query_as::<_, T>(query);
        for bind in binds {
            q = q.bind(*bind);
        }
        let rows = q.fetch_all(&pool).await.unwrap();
        pool.close().await;
        rows
    })
}

/// What a server build from before hashed keys does to create a key: the
/// key in `api_key`, no `key_hash`.
fn insert_like_an_old_build(key: &str, tenant: &str) {
    sql::<(i32,)>(
        "WITH i AS (INSERT INTO kosha.api_keys (api_key, tenant_id) \
                    VALUES ($1, $2) RETURNING 1) SELECT 1 FROM i",
        &[key, tenant],
    );
}

fn row(tenant: &str) -> (String, Option<String>) {
    sql::<(String, Option<String>)>(
        "SELECT api_key, key_hash FROM kosha.api_keys WHERE tenant_id = $1",
        &[tenant],
    )
    .remove(0)
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn the_scrub_waits_out_old_builds_then_removes_every_plaintext_key() {
    let store = PgStore::new(&url()).unwrap();
    let tenant = format!("scrub-{}", uuid::Uuid::new_v4());
    let key = format!("sk-{}", uuid::Uuid::new_v4());
    let hour = 3600;

    // An old build inserting a key is a sign one is still serving: the
    // row gets its hash, keeps its plaintext, and the clock starts now.
    insert_like_an_old_build(&key, &tenant);
    let started = now();
    let ScrubStatus::Waiting { plaintext, due_at } = store.auto_scrub_plaintext(hour).unwrap()
    else {
        panic!("expected the scrub to wait");
    };
    assert!(plaintext >= 1);
    assert!(
        (started + hour as i64 - 2..=now() + hour as i64 + 2).contains(&due_at),
        "{due_at}"
    );
    assert_eq!(row(&tenant), (key.clone(), Some(hash_api_key(&key))));
    assert_eq!(store.validate_api_key(&key), Some(tenant.clone()));

    // Idempotent: nothing new, same deadline, nothing changed.
    let again = store.auto_scrub_plaintext(hour).unwrap();
    assert!(
        matches!(again, ScrubStatus::Waiting { due_at: d, .. } if (d - due_at).abs() <= 1),
        "{again:?}"
    );
    assert_eq!(row(&tenant).0, key);

    // Past the grace period: every plaintext row is rewritten, the key
    // still works by hash, and the next round finds nothing to do.
    let ScrubStatus::Scrubbed(rows) = store.auto_scrub_plaintext(0).unwrap() else {
        panic!("expected the scrub to run");
    };
    assert!(rows >= 1);
    let (api_key, key_hash) = row(&tenant);
    assert_eq!(api_key, format!(" sha256:{}", hash_api_key(&key)));
    assert_eq!(key_hash.as_deref(), Some(hash_api_key(&key).as_str()));
    assert_eq!(store.count_plaintext_api_keys().unwrap(), 0);
    assert_eq!(store.validate_api_key(&key), Some(tenant.clone()));
    assert_eq!(store.auto_scrub_plaintext(0).unwrap(), ScrubStatus::Clean);
    assert_eq!(
        store.auto_scrub_plaintext(hour).unwrap(),
        ScrubStatus::Clean
    );
    let recorded: Vec<(Option<i64>,)> = sql("SELECT last_scrubbed FROM kosha.api_key_scrub", &[]);
    assert_eq!(recorded, vec![(Some(rows as i64),)]);

    // Another old-build key later restarts the clock rather than being
    // scrubbed under a pod that may still need it.
    let late_tenant = format!("scrub-late-{}", uuid::Uuid::new_v4());
    let late = format!("sk-{}", uuid::Uuid::new_v4());
    insert_like_an_old_build(&late, &late_tenant);
    assert!(matches!(
        store.auto_scrub_plaintext(hour).unwrap(),
        ScrubStatus::Waiting { .. }
    ));
    assert_eq!(row(&late_tenant).0, late);
}
