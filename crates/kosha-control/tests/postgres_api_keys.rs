//! Opt-in database tests for the API-key registry (#346). Use a dedicated
//! disposable database:
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test postgres_api_keys -- --ignored
#![cfg(feature = "postgres")]
use kosha_control::{hash_api_key, NewApiKey, PgStore, Scope, Scopes};

fn url() -> String {
    std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL")
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// A tenant id no other test (or earlier run) uses.
fn tenant(label: &str) -> String {
    format!("{label}-{}", uuid::Uuid::new_v4())
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

fn full_access(tenant_id: &str) -> NewApiKey {
    NewApiKey {
        tenant_id: tenant_id.into(),
        description: "test".into(),
        scopes: Scopes::ALL,
        namespaces: None,
        expires_at: None,
    }
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn created_keys_are_stored_hashed_with_their_attributes() {
    let store = PgStore::new(&url()).unwrap();
    let t = tenant("scoped");
    let expires = now() + 3600;
    let issued = store
        .create_api_key(&NewApiKey {
            tenant_id: t.clone(),
            description: "read only".into(),
            scopes: Scopes::of(&[Scope::Read]),
            namespaces: Some(vec!["docs".into(), "wiki".into()]),
            expires_at: Some(expires),
        })
        .unwrap();
    assert!(issued.api_key.starts_with("sk-"));
    assert_eq!(issued.record.key_hash, hash_api_key(&issued.api_key));

    // No column of the row holds the key.
    let stored: Vec<(String, String)> = sql(
        "SELECT api_key, key_hash FROM kosha.api_keys WHERE tenant_id = $1",
        &[&t],
    );
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].0, format!(" sha256:{}", issued.record.key_hash));
    assert_eq!(stored[0].1, issued.record.key_hash);
    assert!(!stored[0].0.contains(&issued.api_key));

    let listed = store.list_api_keys(Some(&t)).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0], issued.record);
    assert_eq!(listed[0].scopes, Scopes::of(&[Scope::Read]));
    assert_eq!(
        listed[0].namespaces.as_deref(),
        Some(&["docs".to_string(), "wiki".to_string()][..])
    );
    assert_eq!(listed[0].expires_at, Some(expires));
    assert_eq!(listed[0].last_used_at, None);

    assert_eq!(store.validate_api_key(&issued.api_key), Some(t));
    // The placeholder is not a key.
    assert_eq!(store.validate_api_key(&stored[0].0), None);
    assert_eq!(store.validate_api_key(&issued.record.key_hash), None);
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn plaintext_rows_from_before_the_migration_keep_working_and_can_be_scrubbed() {
    let store = PgStore::new(&url()).unwrap();
    let t = tenant("legacy");
    let before = format!("sk-before-{t}");
    let during = format!("sk-during-{t}");

    // A row as it looked before the migration, then the migration again
    // (every server start re-runs it): the row gains a hash and full access.
    sql::<(i32,)>(
        "WITH i AS (INSERT INTO kosha.api_keys (api_key, tenant_id, key_hash) \
                    VALUES ($1, $2, NULL) RETURNING 1) SELECT 1 FROM i",
        &[&before, &t],
    );
    let store2 = PgStore::new(&url()).unwrap();
    // A row an old build inserts after the migration ran (rolling deploy):
    // no hash until the next startup, but it must already authenticate.
    sql::<(i32,)>(
        "WITH i AS (INSERT INTO kosha.api_keys (api_key, tenant_id) \
                    VALUES ($1, $2) RETURNING 1) SELECT 1 FROM i",
        &[&during, &t],
    );

    let mut listed = store2.list_api_keys(Some(&t)).unwrap();
    listed.sort_by(|a, b| a.key_hash.cmp(&b.key_hash));
    let mut expected = vec![hash_api_key(&before), hash_api_key(&during)];
    expected.sort();
    assert_eq!(
        listed
            .iter()
            .map(|k| k.key_hash.clone())
            .collect::<Vec<_>>(),
        expected
    );
    for key in &listed {
        assert_eq!(key.scopes, Scopes::ALL);
        assert_eq!(key.namespaces, None);
        assert_eq!(key.expires_at, None);
    }
    assert_eq!(store.validate_api_key(&before), Some(t.clone()));
    assert_eq!(store.validate_api_key(&during), Some(t.clone()));

    // The plaintext is still there until an operator scrubs it...
    let plain: Vec<(String,)> = sql(
        "SELECT api_key FROM kosha.api_keys WHERE tenant_id = $1 ORDER BY api_key",
        &[&t],
    );
    assert!(plain.iter().any(|(k,)| k == &before));
    // ... and the read-only count says the scrub is due, changing nothing.
    assert!(store.count_plaintext_api_keys().unwrap() >= 2);
    let still_plain: Vec<(String,)> = sql(
        "SELECT api_key FROM kosha.api_keys WHERE tenant_id = $1 ORDER BY api_key",
        &[&t],
    );
    assert_eq!(still_plain, plain);

    // ...after which no row of the table holds a usable key, and the keys
    // still authenticate by hash.
    assert!(store.scrub_plaintext_api_keys().unwrap() >= 2);
    assert_eq!(store.count_plaintext_api_keys().unwrap(), 0);
    let scrubbed: Vec<(String, String)> = sql(
        "SELECT api_key, key_hash FROM kosha.api_keys WHERE tenant_id = $1",
        &[&t],
    );
    assert_eq!(scrubbed.len(), 2);
    for (api_key, key_hash) in &scrubbed {
        assert_eq!(api_key, &format!(" sha256:{key_hash}"));
    }
    let remaining: Vec<(i64,)> = sql(
        "SELECT count(*) FROM kosha.api_keys WHERE api_key NOT LIKE ' sha256:%'",
        &[],
    );
    assert_eq!(remaining[0].0, 0);
    assert_eq!(store.validate_api_key(&before), Some(t.clone()));
    assert_eq!(store.validate_api_key(&during), Some(t.clone()));
    assert_eq!(store.list_api_keys(Some(&t)).unwrap().len(), 2);
    // Scrubbing again finds nothing.
    let again: Vec<(i64,)> = sql(
        "SELECT count(*) FROM kosha.api_keys WHERE tenant_id = $1 AND api_key NOT LIKE ' sha256:%'",
        &[&t],
    );
    assert_eq!(again[0].0, 0);
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn rotation_overlaps_then_the_old_key_expires() {
    let store = PgStore::new(&url()).unwrap();
    let t = tenant("rotate");
    let old = store
        .create_api_key(&NewApiKey {
            scopes: Scopes::of(&[Scope::Read, Scope::Write]),
            namespaces: Some(vec!["docs".into()]),
            ..full_access(&t)
        })
        .unwrap();

    let started = now();
    let rotated = store
        .rotate_api_key(&old.record.key_hash, 600, None)
        .unwrap()
        .expect("active key rotates");
    // The replacement carries the old key's attributes and is a new secret.
    assert_ne!(rotated.new.api_key, old.api_key);
    assert_eq!(rotated.new.record.tenant_id, t);
    assert_eq!(rotated.new.record.scopes, old.record.scopes);
    assert_eq!(rotated.new.record.namespaces, old.record.namespaces);
    assert_eq!(rotated.new.record.expires_at, None);
    // Both keys work during the overlap window.
    assert!((started + 599..=started + 602).contains(&rotated.old_expires_at));
    assert_eq!(store.validate_api_key(&old.api_key), Some(t.clone()));
    assert_eq!(
        store.validate_api_key(&rotated.new.api_key),
        Some(t.clone())
    );
    let listed = store.list_api_keys(Some(&t)).unwrap();
    assert_eq!(listed.len(), 2);
    let old_row = listed
        .iter()
        .find(|k| k.key_hash == old.record.key_hash)
        .unwrap();
    assert_eq!(old_row.expires_at, Some(rotated.old_expires_at));

    // A second rotation of the old key cannot extend its life.
    let again = store
        .rotate_api_key(&old.record.key_hash, 86_400, None)
        .unwrap()
        .unwrap();
    assert_eq!(again.old_expires_at, rotated.old_expires_at);

    // A zero overlap retires the key at once; it can then not be rotated.
    let cut = store
        .rotate_api_key(&rotated.new.record.key_hash, 0, Some(started + 7200))
        .unwrap()
        .unwrap();
    assert_eq!(cut.new.record.expires_at, Some(started + 7200));
    assert_eq!(store.validate_api_key(&rotated.new.api_key), None);
    assert_eq!(store.validate_api_key(&cut.new.api_key), Some(t.clone()));
    assert!(store
        .rotate_api_key(&rotated.new.record.key_hash, 600, None)
        .unwrap()
        .is_none());
    assert!(store
        .rotate_api_key(&hash_api_key("sk-never-issued"), 600, None)
        .unwrap()
        .is_none());
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn revoked_and_expired_keys_stop_validating() {
    let store = PgStore::new(&url()).unwrap();
    let t = tenant("revoke");
    let live = store.create_api_key(&full_access(&t)).unwrap();
    let expired = store
        .create_api_key(&NewApiKey {
            expires_at: Some(now() - 10),
            ..full_access(&t)
        })
        .unwrap();

    assert_eq!(store.validate_api_key(&expired.api_key), None);
    // Expired keys stay listed (an operator can see them); revoked do not.
    assert_eq!(store.list_api_keys(Some(&t)).unwrap().len(), 2);

    assert!(store.revoke_api_key(&live.record.key_hash).unwrap());
    assert_eq!(store.validate_api_key(&live.api_key), None);
    assert!(!store.revoke_api_key(&live.record.key_hash).unwrap());
    assert!(!store.revoke_api_key(&hash_api_key("sk-unknown")).unwrap());
    let listed = store.list_api_keys(Some(&t)).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key_hash, expired.record.key_hash);
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn last_used_only_moves_forward() {
    let store = PgStore::new(&url()).unwrap();
    let t = tenant("used");
    let a = store.create_api_key(&full_access(&t)).unwrap();
    let b = store.create_api_key(&full_access(&t)).unwrap();
    let last_used = |hash: &str| {
        store
            .list_api_keys(Some(&t))
            .unwrap()
            .into_iter()
            .find(|k| k.key_hash == hash)
            .unwrap()
            .last_used_at
    };

    store.touch_api_keys(&[]).unwrap();
    assert_eq!(last_used(&a.record.key_hash), None);

    store
        .touch_api_keys(&[
            (a.record.key_hash.clone(), 1_900_000_000),
            (hash_api_key("sk-unknown"), 1_900_000_000),
        ])
        .unwrap();
    assert_eq!(last_used(&a.record.key_hash), Some(1_900_000_000));
    assert_eq!(last_used(&b.record.key_hash), None);

    // An older report (another pod flushing late) does not move it back.
    store
        .touch_api_keys(&[
            (a.record.key_hash.clone(), 1_800_000_000),
            (b.record.key_hash.clone(), 1_800_000_000),
        ])
        .unwrap();
    assert_eq!(last_used(&a.record.key_hash), Some(1_900_000_000));
    assert_eq!(last_used(&b.record.key_hash), Some(1_800_000_000));
}

/// An operator name no other test (or earlier run) uses.
fn operator(label: &str) -> String {
    format!("{label}-{}", uuid::Uuid::new_v4())
}

/// The audit rows about one operator's credentials, oldest first.
fn operator_audit(name: &str) -> Vec<(String, String, serde_json::Value)> {
    sql::<(String, String, String)>(
        "SELECT actor, action, detail FROM kosha.audit_log \
         WHERE target_type = 'operator' AND target_id = $1 AND org_id IS NULL ORDER BY id",
        &[name],
    )
    .into_iter()
    .map(|(actor, action, detail)| (actor, action, serde_json::from_str(&detail).unwrap()))
    .collect()
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn operator_credentials_are_hashed_named_and_audited() {
    use kosha_control::api_keys::{operator_name, operator_tenant_id};
    let store = PgStore::new(&url()).unwrap();
    let alice = operator("alice");
    let expires = now() + 3600;
    let issued = store
        .create_operator_credential(&alice, "on call", Some(expires), "operator")
        .unwrap()
        .expect("the name is free");
    assert!(issued.api_key.starts_with("sk-"));
    assert_eq!(issued.record.key_hash, hash_api_key(&issued.api_key));
    assert_eq!(
        operator_name(&issued.record.tenant_id),
        Some(alice.as_str())
    );
    assert_eq!(issued.record.expires_at, Some(expires));

    // Stored like any key since #346: hash and placeholder, never the key,
    // and with no tenant scope at all.
    let stored: Vec<(String, String, String)> = sql(
        "SELECT api_key, key_hash, scopes FROM kosha.api_keys WHERE tenant_id = $1",
        &[&operator_tenant_id(&alice)],
    );
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].0, format!(" sha256:{}", issued.record.key_hash));
    assert_eq!(stored[0].1, issued.record.key_hash);
    assert_eq!(stored[0].2, "");

    let listed = store.list_operator_credentials().unwrap();
    let mine: Vec<_> = listed
        .iter()
        .filter(|c| operator_name(&c.tenant_id) == Some(alice.as_str()))
        .collect();
    assert_eq!(mine, vec![&issued.record]);
    assert_eq!(mine[0].description, "on call");
    // Nothing in the operator listing is a tenant key.
    assert!(listed.iter().all(|c| operator_name(&c.tenant_id).is_some()));

    // A name identifies one operator: a second usable credential is refused
    // and changes nothing.
    assert!(store
        .create_operator_credential(&alice, "again", None, "operator:mallory")
        .unwrap()
        .is_none());
    // The store itself refuses a name the routes would have refused.
    for bad in ["", "a/b", "al ice", "_operator/x"] {
        assert!(
            store
                .create_operator_credential(bad, "", None, "operator")
                .is_err(),
            "{bad:?}"
        );
    }

    let audit = operator_audit(&alice);
    assert_eq!(audit.len(), 1, "{audit:?}");
    assert_eq!(audit[0].0, "operator");
    assert_eq!(audit[0].1, "operator_credential.created");
    assert_eq!(audit[0].2["key_id"], issued.record.key_hash.as_str());
    assert_eq!(audit[0].2["expires_at"], expires);
    assert!(!audit[0].2.to_string().contains(&issued.api_key));
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn operator_credentials_and_tenant_keys_never_mix() {
    use kosha_control::OperatorCredentials;
    let store = PgStore::new(&url()).unwrap();
    let alice = operator("alice");
    let credential = store
        .create_operator_credential(&alice, "", None, "operator")
        .unwrap()
        .unwrap();
    let t = tenant("mixed");
    let key = store.create_api_key(&full_access(&t)).unwrap();
    let (credential_id, key_id) = (&credential.record.key_hash, &key.record.key_hash);

    // The tenant-key side does not see, validate, revoke or rotate an
    // operator credential.
    assert_eq!(store.validate_api_key(&credential.api_key), None);
    let tenant_keys = store.list_api_keys(None).unwrap();
    assert!(tenant_keys.iter().any(|k| &k.key_hash == key_id));
    assert!(tenant_keys.iter().all(|k| &k.key_hash != credential_id));
    assert!(tenant_keys
        .iter()
        .all(|k| !k.tenant_id.starts_with("_operator/")));
    assert!(!store.revoke_api_key(credential_id).unwrap());
    assert!(store
        .rotate_api_key(credential_id, 60, None)
        .unwrap()
        .is_none());

    // The operator side does not see, revoke or rotate a tenant key.
    let operators = store.list_operator_credentials().unwrap();
    assert!(operators.iter().any(|c| &c.key_hash == credential_id));
    assert!(operators.iter().all(|c| &c.key_hash != key_id));
    assert!(store
        .revoke_operator_credentials(OperatorCredentials::KeyHash(key_id), "operator")
        .unwrap()
        .is_empty());
    assert!(store
        .rotate_operator_credential(key_id, 60, None, "operator")
        .unwrap()
        .is_none());
    assert_eq!(store.validate_api_key(&key.api_key), Some(t.clone()));

    // What a server loads holds both, each exactly once.
    let all = store.list_all_credentials().unwrap();
    for id in [credential_id, key_id] {
        assert_eq!(all.iter().filter(|k| &k.key_hash == id).count(), 1);
    }
    // None of the refused calls left an audit row or changed a credential.
    assert_eq!(operator_audit(&alice).len(), 1);
    assert!(store
        .list_operator_credentials()
        .unwrap()
        .iter()
        .any(|c| &c.key_hash == credential_id && c.expires_at.is_none()));
}

#[test]
#[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
fn operator_credentials_rotate_and_revoke_with_an_audit_trail() {
    use kosha_control::api_keys::operator_name;
    use kosha_control::OperatorCredentials;
    let store = PgStore::new(&url()).unwrap();
    let alice = operator("alice");
    let by_alice = format!("operator:{alice}");
    let first = store
        .create_operator_credential(&alice, "laptop", None, "operator")
        .unwrap()
        .unwrap();

    // Rotation: same operator and description, a new secret, and the old
    // credential kept only for the overlap.
    let before = now();
    let rotated = store
        .rotate_operator_credential(&first.record.key_hash, 600, None, &by_alice)
        .unwrap()
        .expect("active credential");
    assert_ne!(rotated.new.api_key, first.api_key);
    assert_eq!(rotated.new.record.tenant_id, first.record.tenant_id);
    assert_eq!(rotated.new.record.description, "laptop");
    assert_eq!(rotated.new.record.expires_at, None);
    assert!((before + 599..=now() + 601).contains(&rotated.old_expires_at));
    let mine = |store: &PgStore| -> Vec<kosha_control::ApiKeyRecord> {
        store
            .list_operator_credentials()
            .unwrap()
            .into_iter()
            .filter(|c| operator_name(&c.tenant_id) == Some(alice.as_str()))
            .collect()
    };
    assert_eq!(mine(&store).len(), 2);
    // While a credential of that name still works, the name is taken.
    assert!(store
        .create_operator_credential(&alice, "", None, "operator")
        .unwrap()
        .is_none());

    // Revoking one credential leaves the operator's other one alone.
    let revoked = store
        .revoke_operator_credentials(
            OperatorCredentials::KeyHash(&first.record.key_hash),
            &by_alice,
        )
        .unwrap();
    assert_eq!(
        revoked,
        vec![(first.record.key_hash.clone(), alice.clone())]
    );
    assert_eq!(mine(&store), vec![rotated.new.record.clone()]);
    assert!(store
        .revoke_operator_credentials(
            OperatorCredentials::KeyHash(&first.record.key_hash),
            &by_alice
        )
        .unwrap()
        .is_empty());
    assert!(store
        .rotate_operator_credential(&first.record.key_hash, 600, None, &by_alice)
        .unwrap()
        .is_none());

    // Offboarding: every credential of the operator, by name. Rotate once
    // more first, so there are two to revoke.
    let again = store
        .rotate_operator_credential(&rotated.new.record.key_hash, 600, None, "operator")
        .unwrap()
        .unwrap();
    assert_eq!(mine(&store).len(), 2);
    let mut gone = store
        .revoke_operator_credentials(OperatorCredentials::Operator(&alice), "operator:bob")
        .unwrap();
    gone.sort();
    let mut expected = vec![
        (rotated.new.record.key_hash.clone(), alice.clone()),
        (again.new.record.key_hash.clone(), alice.clone()),
    ];
    expected.sort();
    assert_eq!(gone, expected);
    assert!(mine(&store).is_empty());
    assert!(store
        .list_all_credentials()
        .unwrap()
        .iter()
        .all(|c| operator_name(&c.tenant_id) != Some(alice.as_str())));
    // The name is free again.
    assert!(store
        .create_operator_credential(&alice, "", None, "operator")
        .unwrap()
        .is_some());

    let audit = operator_audit(&alice);
    let trail: Vec<(&str, &str)> = audit
        .iter()
        .map(|(actor, action, _)| (actor.as_str(), action.as_str()))
        .collect();
    assert_eq!(
        trail,
        vec![
            ("operator", "operator_credential.created"),
            (by_alice.as_str(), "operator_credential.rotated"),
            (by_alice.as_str(), "operator_credential.revoked"),
            ("operator", "operator_credential.rotated"),
            ("operator:bob", "operator_credential.revoked"),
            ("operator:bob", "operator_credential.revoked"),
            ("operator", "operator_credential.created"),
        ]
    );
    let rotation = &audit[1].2;
    assert_eq!(rotation["key_id"], rotated.new.record.key_hash.as_str());
    assert_eq!(
        rotation["replaces"]["key_id"],
        first.record.key_hash.as_str()
    );
    assert_eq!(rotation["replaces"]["expires_at"], rotated.old_expires_at);
}
