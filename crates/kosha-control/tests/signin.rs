//! Self-serve sign-in (#345) against both stores: `MemoryOrgStore` always,
//! Postgres when asked.
//!
//! Postgres (a dedicated disposable database):
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test signin -- --ignored

use kosha_control::orgs::{self, Actor, MemoryOrgStore, OrgError, OrgStore, Role};
use kosha_control::signin;

fn unique_email(who: &str) -> String {
    format!(
        "{who}-{}@example.com",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

fn unauthorized<T: std::fmt::Debug>(result: Result<T, OrgError>) {
    assert!(
        matches!(result, Err(OrgError::Unauthorized(_))),
        "{result:?}"
    );
}

fn sign_up_creates_and_verifies_the_user(store: &dyn OrgStore) {
    let email = unique_email("Ada");
    let issued = signin::request_signin(store, &format!("  {email} "), " Ada ").unwrap();
    assert_eq!(issued.email, email.to_lowercase());
    assert!(issued.token.starts_with("kst_"));

    let first = signin::complete_signin(store, &issued.token).unwrap();
    assert!(first.created);
    assert_eq!(first.user.email, email.to_lowercase());
    assert_eq!(first.user.display_name, "Ada");
    assert!(first.session_token.starts_with("kss_"));
    assert_eq!(
        signin::session_user(store, &first.session_token).unwrap(),
        first.user
    );
    // Spent.
    unauthorized(signin::complete_signin(store, &issued.token));
    unauthorized(signin::complete_signin(store, "kst_nope"));
    unauthorized(signin::complete_signin(store, "not-a-token"));

    // A user the operator created signs in as themselves; no new user.
    let other = unique_email("bob");
    let bob = orgs::create_user(store, &Actor::Operator, &other, "Bob").unwrap();
    let issued = signin::request_signin(store, &other, "ignored").unwrap();
    let again = signin::complete_signin(store, &issued.token).unwrap();
    assert!(!again.created);
    assert_eq!(again.user, bob);
}

fn resend_is_throttled(store: &dyn OrgStore) {
    let email = unique_email("carol");
    signin::request_signin(store, &email, "").unwrap();
    let again = signin::request_signin(store, &email, "");
    assert!(matches!(again, Err(OrgError::RateLimited(_))), "{again:?}");
    assert!(matches!(
        signin::request_signin(store, "nope", ""),
        Err(OrgError::Invalid(_))
    ));
}

fn sessions_end_at_sign_out_and_list_orgs(store: &dyn OrgStore) {
    let email = unique_email("dan");
    let issued = signin::request_signin(store, &email, "").unwrap();
    let signed_in = signin::complete_signin(store, &issued.token).unwrap();
    let actor = Actor::User(signed_in.user.id.clone());
    assert!(signin::list_user_orgs(store, &signed_in.user.id)
        .unwrap()
        .is_empty());
    let slug = format!("org-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let org = orgs::create_org(store, &actor, &slug, "Org", None).unwrap();
    let listed = signin::list_user_orgs(store, &signed_in.user.id).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].org, org);
    assert_eq!(listed[0].role, Role::Owner);

    let audit = orgs::list_audit(store, &actor, &org.id, Some(100), None).unwrap();
    assert!(audit
        .iter()
        .all(|e| e.actor == format!("user:{}", signed_in.user.id)));

    signin::sign_out(store, &signed_in.session_token).unwrap();
    unauthorized(signin::session_user(store, &signed_in.session_token));
    // Signing out twice, or of nothing, is fine.
    signin::sign_out(store, &signed_in.session_token).unwrap();
    signin::sign_out(store, "kss_unknown").unwrap();
    unauthorized(signin::session_user(store, "kss_unknown"));
    unauthorized(signin::session_user(store, "admin-key"));
}

fn all(store: &dyn OrgStore) {
    sign_up_creates_and_verifies_the_user(store);
    resend_is_throttled(store);
    sessions_end_at_sign_out_and_list_orgs(store);
}

#[test]
fn memory_store() {
    all(&MemoryOrgStore::default());
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;

    #[test]
    #[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
    fn postgres_store() {
        let url =
            std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
        all(&kosha_control::PgStore::new(&url).unwrap());
    }
}
