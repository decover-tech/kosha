//! The identity model (#345 part 2) against both stores: the same
//! scenarios run on `MemoryOrgStore` always and on Postgres when asked.
//!
//! Postgres (a dedicated disposable database):
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test identity -- --ignored

use kosha_control::identity::{
    self, ExternalIdentity, IdentityStore, LoginError, SignUp, SsoIntent,
};
use kosha_control::orgs::{self, Actor, MemoryOrgStore, OrgError, Role};
use kosha_control::Scopes;

const PASSWORD: &str = "correct horse battery";

fn unique() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

fn signed_up(store: &dyn IdentityStore, email: &str) -> (orgs::User, String) {
    match identity::sign_up(store, email, PASSWORD, "").unwrap() {
        SignUp::Created { user, verify_token } => (user, verify_token),
        other => panic!("{other:?}"),
    }
}

fn sign_up_verify_and_login(store: &dyn IdentityStore) {
    let email = format!("ada-{}@example.com", unique());
    let (user, token) = signed_up(store, &email);
    assert_eq!(
        identity::password_login(store, &email, PASSWORD),
        Err(LoginError::Unverified)
    );
    // A second sign-up changes nothing and says so to the caller only.
    assert!(matches!(
        identity::sign_up(store, &email, "another long password", "").unwrap(),
        SignUp::Existing { .. }
    ));
    identity::verify_email(store, &token).unwrap();
    assert!(matches!(
        identity::verify_email(store, &token),
        Err(OrgError::Invalid(_))
    ));
    assert_eq!(
        identity::password_login(store, &email, PASSWORD)
            .unwrap()
            .id,
        user.id
    );
    assert_eq!(
        identity::password_login(store, &email, "wrong password!!"),
        Err(LoginError::InvalidCredentials)
    );
    assert_eq!(
        identity::password_login(store, "ghost@example.com", PASSWORD),
        Err(LoginError::InvalidCredentials)
    );

    // Sessions: live, then revoked.
    let session = identity::create_session(store, &user.id, "password", 3_600_000).unwrap();
    let (s, u) = identity::session(store, &session, 60_000).unwrap().unwrap();
    assert_eq!(
        (s.auth_method.as_str(), u.id.as_str()),
        ("password", user.id.as_str())
    );
    assert!(identity::session(store, "not-a-session", 60_000)
        .unwrap()
        .is_none());
    // An idle timeout of zero ends it.
    assert!(identity::session(store, &session, 0).unwrap().is_none());
    identity::revoke_session(store, &session).unwrap();
    assert!(identity::session(store, &session, 60_000)
        .unwrap()
        .is_none());

    // Reset: single use, ends sessions, the old password stops working.
    let other = identity::create_session(store, &user.id, "password", 3_600_000).unwrap();
    let (_, reset) = identity::start_password_reset(store, &email)
        .unwrap()
        .unwrap();
    assert!(identity::start_password_reset(store, "ghost@example.com")
        .unwrap()
        .is_none());
    identity::finish_password_reset(store, &reset, "a brand new passphrase").unwrap();
    assert!(identity::finish_password_reset(store, &reset, "again a new passphrase").is_err());
    assert!(identity::session(store, &other, 60_000).unwrap().is_none());
    assert!(identity::password_login(store, &email, PASSWORD).is_err());
    assert!(identity::password_login(store, &email, "a brand new passphrase").is_ok());
}

fn external_identities_link_by_verified_email(store: &dyn IdentityStore) {
    let email = format!("pat-{}@example.com", unique());
    // A squatter registered the address without proving it.
    let (squatter, _) = signed_up(store, &email);
    let google = ExternalIdentity {
        provider: "google".into(),
        subject: format!("g-{}", unique()),
        email: email.clone(),
        display_name: "Pat".into(),
    };
    let user = identity::external_login(store, &google, false).unwrap();
    assert_eq!(user.id, squatter.id);
    // The unproven password is gone.
    assert!(identity::password_login(store, &email, PASSWORD).is_err());
    // The same identity signs in again without matching by address.
    assert_eq!(
        identity::external_login(store, &google, false).unwrap().id,
        user.id
    );
    // Unknown address with sign-up closed: refused; open: created.
    let mut stranger = google.clone();
    stranger.subject = format!("g-{}", unique());
    stranger.email = format!("new-{}@example.com", unique());
    assert!(matches!(
        identity::external_login(store, &stranger, false),
        Err(OrgError::Forbidden(_))
    ));
    assert!(identity::external_login(store, &stranger, true).is_ok());
}

fn keys_invites_and_sso(store: &dyn IdentityStore) {
    let owner_email = format!("own-{}@example.com", unique());
    let (owner, _) = signed_up(store, &owner_email);
    let owner_actor = Actor::User(owner.id.clone());
    let org = orgs::create_org(
        store,
        &owner_actor,
        &format!("org-{}", unique()),
        "Org",
        None,
    )
    .unwrap();
    let tenant = orgs::link_tenant(store, &owner_actor, &org.id, None, None).unwrap();

    // Invite a member: the invite works once, for its address only.
    let member_email = format!("mem-{}@example.com", unique());
    let (member, _) = signed_up(store, &member_email);
    let invite =
        identity::invite_member(store, &owner_actor, &org.id, &member_email, Role::Member).unwrap();
    assert_eq!(
        identity::invite_email(store, &invite.token).unwrap(),
        Some(member_email.clone())
    );
    assert!(matches!(
        identity::accept_invite(store, &owner.id, &invite.token),
        Err(OrgError::Forbidden(_))
    ));
    identity::accept_invite(store, &member.id, &invite.token).unwrap();
    assert!(identity::accept_invite(store, &member.id, &invite.token).is_err());

    // Keys record their creator; removal revokes them.
    let member_actor = Actor::User(member.id.clone());
    let key = identity::create_org_api_key(
        store,
        &member_actor,
        &org.id,
        &tenant.tenant_id,
        "ci",
        Scopes::ALL,
        None,
    )
    .unwrap();
    let keys = identity::list_org_api_keys(store, &owner_actor, &org.id).unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].created_by.as_deref(), Some(member.id.as_str()));
    let removed = identity::remove_member(store, &owner_actor, &org.id, &member.id, true).unwrap();
    assert_eq!(removed.revoked_keys, vec![key.listed.key_id.clone()]);
    assert!(identity::list_org_api_keys(store, &owner_actor, &org.id)
        .unwrap()
        .is_empty());

    // SSO config: admin settings, owner-only requirement, sealed secret kept.
    let settings = identity::SsoSettings {
        issuer: "https://idp.example".into(),
        client_id: "client".into(),
        client_secret_sealed: Some("v1:sealed".into()),
        allowed_domains: vec!["Example.com".into()],
        sso_required: false,
    };
    identity::put_sso_config(store, &owner_actor, &org.id, &settings).unwrap();
    let mut again = settings.clone();
    again.client_secret_sealed = None;
    again.sso_required = true;
    let config = identity::put_sso_config(store, &owner_actor, &org.id, &again).unwrap();
    assert_eq!(config.client_secret_sealed, "v1:sealed");
    assert_eq!(config.allowed_domains, vec!["example.com".to_string()]);

    // A just-in-time SSO member: a new account, address not verified.
    let iss = "https://idp.example";
    let jit = ExternalIdentity {
        provider: format!("sso:{}", org.id),
        subject: format!("okta-{}", unique()),
        email: format!("kim-{}@example.com", unique()),
        display_name: "Kim".into(),
    };
    let kim = identity::sso_login(store, &org.id, iss, &jit, SsoIntent::SignIn).unwrap();
    let kim_role = identity::user_orgs(store, &kim.id).unwrap();
    assert_eq!(kim_role[0].role, Role::ReadOnly);
    assert!(kim_role[0].sso_required);
    assert_eq!(
        identity::sso_login(store, &org.id, iss, &jit, SsoIntent::SignIn)
            .unwrap()
            .id,
        kim.id
    );
    sso_never_takes_over_existing_accounts(store, &org.id, &owner, &owner_email, &jit, &kim);

    assert!(identity::record_tenant_event(
        store,
        &tenant.tenant_id,
        "tenant:x",
        "namespace.deleted",
        "namespace",
        "docs",
        serde_json::json!({}),
    )
    .unwrap());
    let actions: Vec<String> = orgs::list_audit(store, &owner_actor, &org.id, None, None)
        .unwrap()
        .into_iter()
        .map(|e| e.action)
        .collect();
    for expected in [
        "member.invited",
        "api_key.created",
        "api_key.revoked",
        "sso.configured",
        "namespace.deleted",
    ] {
        assert!(
            actions.iter().any(|a| a == expected),
            "{expected}: {actions:?}"
        );
    }
}

/// #345 review findings 1, 2, 3, 4 and 7: an organization's SSO never
/// signs in as, or changes, an account it was not explicitly connected to.
fn sso_never_takes_over_existing_accounts(
    store: &dyn IdentityStore,
    org_id: &str,
    owner: &orgs::User,
    owner_email: &str,
    jit: &ExternalIdentity,
    kim: &orgs::User,
) {
    let iss = "https://idp.example";
    let owner_actor = Actor::User(owner.id.clone());
    // 1/4: the IdP asserts the owner's address: refused, not linked, and
    // the owner's password still works.
    let mut as_owner = jit.clone();
    as_owner.subject = format!("evil-{}", unique());
    as_owner.email = owner_email.to_string();
    let refused = identity::sso_login(store, org_id, iss, &as_owner, SsoIntent::SignIn);
    assert!(
        matches!(refused, Err(OrgError::Forbidden(_))),
        "{refused:?}"
    );
    // (The owner never verified here: the password is still right.)
    assert_eq!(
        identity::password_login(store, owner_email, PASSWORD),
        Err(LoginError::Unverified)
    );
    // An unverified account in an allowed domain is not touched either.
    let pending_email = format!("pending-{}@example.com", unique());
    let (_, _) = signed_up(store, &pending_email);
    as_owner.email = pending_email.clone();
    assert!(identity::sso_login(store, org_id, iss, &as_owner, SsoIntent::SignIn).is_err());
    assert_eq!(
        identity::password_login(store, &pending_email, PASSWORD),
        Err(LoginError::Unverified),
        "the password is kept and the address stays unverified"
    );
    // An explicit connect from the owner's own session links it.
    let connected = identity::sso_login(
        store,
        org_id,
        iss,
        &as_owner,
        SsoIntent::Connect { user_id: &owner.id },
    )
    .unwrap();
    assert_eq!(connected.id, owner.id);
    assert_eq!(
        identity::sso_login(store, org_id, iss, &as_owner, SsoIntent::SignIn)
            .unwrap()
            .id,
        owner.id
    );
    // 2: the same `sub` from another issuer is a different identity.
    let other = identity::sso_login(
        store,
        org_id,
        "https://other.example",
        jit,
        SsoIntent::SignIn,
    );
    assert!(other.is_err(), "{other:?}");

    // 3: a new issuer or client id needs a new secret, and (7) drops the old
    // provider's identities and sessions.
    let kim_session =
        identity::create_session(store, &kim.id, &format!("sso:{org_id}"), 3_600_000).unwrap();
    let kim_session_data = identity::session(store, &kim_session, 60_000)
        .unwrap()
        .unwrap()
        .0;
    identity::session_may_act_in(store, &kim_session_data, org_id).unwrap();
    let moved = identity::SsoSettings {
        issuer: "https://attacker.example".into(),
        client_id: "client".into(),
        client_secret_sealed: None,
        allowed_domains: vec!["example.com".into()],
        sso_required: true,
    };
    assert!(matches!(
        identity::put_sso_config(store, &owner_actor, org_id, &moved),
        Err(OrgError::Invalid(_))
    ));
    let mut moved_with_secret = moved.clone();
    moved_with_secret.client_secret_sealed = Some("v1:new".into());
    identity::put_sso_config(store, &owner_actor, org_id, &moved_with_secret).unwrap();
    assert!(identity::session(store, &kim_session, 60_000)
        .unwrap()
        .is_none());
    // Kim's identity was for the old issuer; under the new one Kim's
    // existing account is not taken over by address either.
    assert!(identity::sso_login(
        store,
        org_id,
        "https://attacker.example",
        jit,
        SsoIntent::SignIn
    )
    .is_err());
    // 7: removing SSO ends the sessions it opened.
    let kim_session =
        identity::create_session(store, &kim.id, &format!("sso:{org_id}"), 3_600_000).unwrap();
    let data = identity::session(store, &kim_session, 60_000)
        .unwrap()
        .unwrap()
        .0;
    identity::delete_sso_config(store, &owner_actor, org_id).unwrap();
    assert!(identity::session(store, &kim_session, 60_000)
        .unwrap()
        .is_none());
    assert!(identity::session_may_act_in(store, &data, org_id).is_err());
}

fn all(store: &dyn IdentityStore) {
    sign_up_verify_and_login(store);
    external_identities_link_by_verified_email(store);
    keys_invites_and_sso(store);
}

#[test]
fn memory_store_runs_the_scenarios() {
    all(&MemoryOrgStore::new());
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use kosha_control::PgStore;

    #[test]
    #[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
    fn postgres_runs_the_same_scenarios() {
        let url =
            std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
        all(&PgStore::new(&url).unwrap());
    }
}
