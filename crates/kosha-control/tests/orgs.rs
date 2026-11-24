//! The organization model (#345) against both stores: the same scenarios
//! run on `MemoryOrgStore` always and on Postgres when asked.
//!
//! Postgres (a dedicated disposable database):
//! KOSHA_TEST_DATABASE_URL=... cargo test -p kosha-control --features postgres --test orgs -- --ignored

use kosha_control::orgs::{self, Actor, MemoryOrgStore, OrgError, OrgStore, Role};

/// Names unique to one run, so the suite can re-run against a database
/// that already holds earlier runs' rows.
struct Names(String);

impl Names {
    fn new() -> Self {
        Names(uuid::Uuid::new_v4().simple().to_string()[..12].to_string())
    }
    fn email(&self, who: &str) -> String {
        format!("{who}-{}@example.com", self.0)
    }
    fn slug(&self, what: &str) -> String {
        format!("{what}-{}", self.0)
    }
}

fn user(id: &str) -> Actor {
    Actor::User(id.to_string())
}

fn forbidden<T: std::fmt::Debug>(result: Result<T, OrgError>) {
    assert!(matches!(result, Err(OrgError::Forbidden(_))), "{result:?}");
}

fn conflict<T: std::fmt::Debug>(result: Result<T, OrgError>) {
    assert!(matches!(result, Err(OrgError::Conflict(_))), "{result:?}");
}

fn not_found<T: std::fmt::Debug>(result: Result<T, OrgError>) {
    assert!(matches!(result, Err(OrgError::NotFound(_))), "{result:?}");
}

fn invalid<T: std::fmt::Debug>(result: Result<T, OrgError>) {
    assert!(matches!(result, Err(OrgError::Invalid(_))), "{result:?}");
}

/// An organization with one user in each role.
struct Fixture {
    org: String,
    owner: Actor,
    admin: Actor,
    member: Actor,
    read_only: Actor,
    outsider: Actor,
}

impl Fixture {
    fn id(actor: &Actor) -> &str {
        actor.user_id().expect("the operator has no user id")
    }
}

fn fixture(store: &dyn OrgStore, names: &Names) -> Fixture {
    let op = Actor::Operator;
    let new_user = |who: &str| {
        orgs::create_user(store, &op, &names.email(who), who)
            .unwrap()
            .id
    };
    let owner = new_user("owner");
    let admin = new_user("admin");
    let member = new_user("member");
    let read_only = new_user("read-only");
    let outsider = new_user("outsider");
    // The owner creates the organization for themselves.
    let org = orgs::create_org(store, &user(&owner), &names.slug("acme"), "Acme", None)
        .unwrap()
        .id;
    for (id, role) in [
        (&admin, Role::Admin),
        (&member, Role::Member),
        (&read_only, Role::ReadOnly),
    ] {
        orgs::add_member(store, &user(&owner), &org, id, role).unwrap();
    }
    Fixture {
        org,
        owner: user(&owner),
        admin: user(&admin),
        member: user(&member),
        read_only: user(&read_only),
        outsider: user(&outsider),
    }
}

fn users_and_organizations(store: &dyn OrgStore) {
    let names = Names::new();
    let op = Actor::Operator;

    let ada =
        orgs::create_user(store, &op, &format!(" Ada-{}@Example.com ", names.0), "Ada").unwrap();
    assert_eq!(ada.email, format!("ada-{}@example.com", names.0));
    assert!(ada.id.starts_with("usr_"));
    // Email is unique case-insensitively; a user cannot register users.
    conflict(orgs::create_user(store, &op, &ada.email.to_uppercase(), ""));
    forbidden(orgs::create_user(
        store,
        &user(&ada.id),
        &names.email("x"),
        "",
    ));
    invalid(orgs::create_user(store, &op, "not-an-email", ""));

    assert_eq!(orgs::get_user(store, &op, &ada.id).unwrap(), ada);
    assert_eq!(orgs::get_user(store, &user(&ada.id), &ada.id).unwrap(), ada);
    forbidden(orgs::get_user(store, &user("usr_other"), &ada.id));
    not_found(orgs::get_user(store, &op, "usr_missing"));

    // The operator must name an owner, and the owner must exist.
    let slug = names.slug("org");
    invalid(orgs::create_org(store, &op, &slug, "Org", None));
    not_found(orgs::create_org(
        store,
        &op,
        &slug,
        "Org",
        Some("usr_missing"),
    ));
    forbidden(orgs::create_org(
        store,
        &user("usr_missing"),
        &slug,
        "Org",
        None,
    ));
    forbidden(orgs::create_org(
        store,
        &user(&ada.id),
        &slug,
        "Org",
        Some("usr_other"),
    ));
    invalid(orgs::create_org(
        store,
        &op,
        "Bad Slug",
        "Org",
        Some(&ada.id),
    ));
    // None of the refused attempts took the slug.
    let org = orgs::create_org(store, &op, &slug, " Org ", Some(&ada.id)).unwrap();
    assert!(org.id.starts_with("org_"));
    assert_eq!(
        (org.slug.as_str(), org.name.as_str()),
        (slug.as_str(), "Org")
    );
    conflict(orgs::create_org(store, &op, &slug, "Again", Some(&ada.id)));

    // Its creator is its only member, as owner.
    let members = orgs::list_members(store, &user(&ada.id), &org.id).unwrap();
    assert_eq!(members.len(), 1);
    assert_eq!(
        (
            members[0].user_id.as_str(),
            members[0].email.as_str(),
            members[0].role
        ),
        (ada.id.as_str(), ada.email.as_str(), Role::Owner)
    );
    assert_eq!(orgs::get_org(store, &user(&ada.id), &org.id).unwrap(), org);
    not_found(orgs::get_org(store, &op, "org_missing"));
}

fn roles_gate_every_operation(store: &dyn OrgStore) {
    let names = Names::new();
    let f = fixture(store, &names);
    let org = f.org.as_str();
    let outsider_id = Fixture::id(&f.outsider);

    // Everyone in the organization reads it; nobody outside does.
    for actor in [
        &f.owner,
        &f.admin,
        &f.member,
        &f.read_only,
        &Actor::Operator,
    ] {
        assert_eq!(orgs::get_org(store, actor, org).unwrap().id, org);
        assert_eq!(orgs::list_members(store, actor, org).unwrap().len(), 4);
        orgs::list_projects(store, actor, org).unwrap();
        orgs::list_tenants(store, actor, org).unwrap();
    }
    forbidden(orgs::get_org(store, &f.outsider, org));
    forbidden(orgs::list_members(store, &f.outsider, org));
    forbidden(orgs::list_projects(store, &f.outsider, org));
    forbidden(orgs::list_tenants(store, &f.outsider, org));
    forbidden(orgs::list_audit(store, &f.outsider, org, None, None));

    // Projects, tenants, members and the audit log need admin or owner.
    for actor in [&f.member, &f.read_only, &f.outsider] {
        forbidden(orgs::create_project(store, actor, org, "nope", "Nope"));
        forbidden(orgs::link_tenant(store, actor, org, None, None));
        forbidden(orgs::add_member(
            store,
            actor,
            org,
            outsider_id,
            Role::ReadOnly,
        ));
        forbidden(orgs::set_member_role(
            store,
            actor,
            org,
            Fixture::id(&f.read_only),
            Role::Admin,
        ));
        forbidden(orgs::remove_member(
            store,
            actor,
            org,
            Fixture::id(&f.admin),
        ));
        forbidden(orgs::list_audit(store, actor, org, None, None));
    }
    let project = orgs::create_project(store, &f.admin, org, "search", "Search").unwrap();
    assert!(project.id.starts_with("prj_"));
    conflict(orgs::create_project(store, &f.owner, org, "search", "Dup"));
    invalid(orgs::create_project(store, &f.owner, org, "search", "  "));
    assert_eq!(
        orgs::list_projects(store, &f.read_only, org).unwrap(),
        vec![project.clone()]
    );

    // A user gets a generated tenant id; only the operator names one.
    let slug = orgs::get_org(store, &f.owner, org).unwrap().slug;
    let tenant = orgs::link_tenant(store, &f.admin, org, None, Some(&project.id)).unwrap();
    assert!(tenant.tenant_id.starts_with(&format!("{slug}-")));
    assert_eq!(tenant.tenant_id.len(), slug.len() + 13);
    assert_eq!(tenant.project_id.as_deref(), Some(project.id.as_str()));
    let legacy = names.slug("legacy-tenant");
    forbidden(orgs::link_tenant(store, &f.owner, org, Some(&legacy), None));
    invalid(orgs::link_tenant(
        store,
        &Actor::Operator,
        org,
        Some("_legacy"),
        None,
    ));
    not_found(orgs::link_tenant(
        store,
        &f.owner,
        org,
        None,
        Some("prj_missing"),
    ));
    let adopted = orgs::link_tenant(store, &Actor::Operator, org, Some(&legacy), None).unwrap();
    assert_eq!(
        (adopted.tenant_id.as_str(), &adopted.project_id),
        (legacy.as_str(), &None)
    );
    assert_eq!(
        orgs::list_tenants(store, &f.member, org).unwrap(),
        vec![tenant.clone(), adopted]
    );

    // A tenant belongs to one organization, and a project only attaches
    // tenants of its own organization.
    let other = orgs::create_org(store, &f.outsider, &names.slug("other"), "Other", None).unwrap();
    conflict(orgs::link_tenant(
        store,
        &Actor::Operator,
        &other.id,
        Some(&legacy),
        None,
    ));
    not_found(orgs::link_tenant(
        store,
        &f.outsider,
        &other.id,
        None,
        Some(&project.id),
    ));

    // Keys: everyone but read-only, and only for the organization's tenants.
    for actor in [&f.owner, &f.admin, &f.member, &Actor::Operator] {
        let key = orgs::create_api_key(store, actor, org, &tenant.tenant_id, "ci").unwrap();
        assert!(key.api_key.starts_with("sk-"));
        assert_eq!(key.tenant_id, tenant.tenant_id);
    }
    forbidden(orgs::create_api_key(
        store,
        &f.read_only,
        org,
        &tenant.tenant_id,
        "",
    ));
    forbidden(orgs::create_api_key(
        store,
        &f.outsider,
        org,
        &tenant.tenant_id,
        "",
    ));
    not_found(orgs::create_api_key(
        store,
        &f.owner,
        org,
        "someone-elses-tenant",
        "",
    ));
    // The other organization's owner cannot mint keys for this one's tenant
    // by addressing it through their own organization.
    not_found(orgs::create_api_key(
        store,
        &f.outsider,
        &other.id,
        &tenant.tenant_id,
        "",
    ));
}

fn membership_rules(store: &dyn OrgStore) {
    let names = Names::new();
    let f = fixture(store, &names);
    let org = f.org.as_str();
    let (owner, admin, member, read_only, outsider) = (
        Fixture::id(&f.owner),
        Fixture::id(&f.admin),
        Fixture::id(&f.member),
        Fixture::id(&f.read_only),
        Fixture::id(&f.outsider),
    );
    let role_of = |user_id: &str| {
        orgs::list_members(store, &Actor::Operator, org)
            .unwrap()
            .into_iter()
            .find(|m| m.user_id == user_id)
            .map(|m| m.role)
    };

    conflict(orgs::add_member(store, &f.owner, org, member, Role::Admin));
    not_found(orgs::add_member(
        store,
        &f.owner,
        org,
        "usr_missing",
        Role::Member,
    ));
    not_found(orgs::set_member_role(
        store,
        &f.owner,
        org,
        outsider,
        Role::Member,
    ));
    not_found(orgs::remove_member(store, &f.owner, org, outsider));
    not_found(orgs::add_member(
        store,
        &f.owner,
        "org_missing",
        outsider,
        Role::Member,
    ));

    // An admin manages everyone below owner, and never the owner role.
    orgs::set_member_role(store, &f.admin, org, read_only, Role::Member).unwrap();
    assert_eq!(role_of(read_only), Some(Role::Member));
    forbidden(orgs::set_member_role(
        store,
        &f.admin,
        org,
        member,
        Role::Owner,
    ));
    forbidden(orgs::set_member_role(
        store,
        &f.admin,
        org,
        owner,
        Role::Member,
    ));
    forbidden(orgs::remove_member(store, &f.admin, org, owner));
    forbidden(orgs::add_member(
        store,
        &f.admin,
        org,
        outsider,
        Role::Owner,
    ));
    assert_eq!(role_of(owner), Some(Role::Owner));
    assert_eq!(role_of(outsider), None);

    // The last owner can neither leave nor be demoted, by anyone.
    conflict(orgs::remove_member(store, &f.owner, org, owner));
    conflict(orgs::set_member_role(
        store,
        &f.owner,
        org,
        owner,
        Role::Admin,
    ));
    conflict(orgs::remove_member(store, &Actor::Operator, org, owner));
    assert_eq!(role_of(owner), Some(Role::Owner));

    // With a second owner the first can step down, and then no longer
    // holds owner powers.
    orgs::set_member_role(store, &f.owner, org, admin, Role::Owner).unwrap();
    orgs::set_member_role(store, &f.owner, org, owner, Role::Admin).unwrap();
    forbidden(orgs::set_member_role(
        store,
        &f.owner,
        org,
        admin,
        Role::Admin,
    ));
    conflict(orgs::remove_member(store, &f.admin, org, admin));

    // Anyone may leave; after leaving they are an outsider.
    orgs::remove_member(store, &f.read_only, org, read_only).unwrap();
    assert_eq!(role_of(read_only), None);
    forbidden(orgs::get_org(store, &f.read_only, org));
    forbidden(orgs::remove_member(store, &f.read_only, org, read_only));
    orgs::remove_member(store, &f.admin, org, member).unwrap();
    assert_eq!(
        orgs::list_members(store, &f.admin, org)
            .unwrap()
            .into_iter()
            .map(|m| (m.user_id, m.role))
            .collect::<Vec<_>>(),
        // Ordered by email: "admin-..." before "owner-...".
        vec![
            (admin.to_string(), Role::Owner),
            (owner.to_string(), Role::Admin)
        ]
    );
}

fn audit_log_records_each_action_once(store: &dyn OrgStore) {
    let names = Names::new();
    let f = fixture(store, &names);
    let org = f.org.as_str();
    let (owner, admin, member, read_only) = (
        Fixture::id(&f.owner),
        Fixture::id(&f.admin),
        Fixture::id(&f.member),
        Fixture::id(&f.read_only),
    );

    let project = orgs::create_project(store, &f.admin, org, "search", "Search").unwrap();
    let tenant = orgs::link_tenant(store, &f.admin, org, None, Some(&project.id)).unwrap();
    let key = orgs::create_api_key(store, &f.member, org, &tenant.tenant_id, "ci").unwrap();
    orgs::set_member_role(store, &f.owner, org, read_only, Role::Member).unwrap();
    // Refused and no-op calls leave nothing behind.
    orgs::set_member_role(store, &f.owner, org, read_only, Role::Member).unwrap();
    forbidden(orgs::create_project(store, &f.member, org, "nope", "Nope"));
    conflict(orgs::remove_member(store, &f.owner, org, owner));
    conflict(orgs::create_project(store, &f.owner, org, "search", "Dup"));
    orgs::remove_member(store, &f.admin, org, read_only).unwrap();

    let log = orgs::list_audit(store, &f.owner, org, None, None).unwrap();
    let got: Vec<(String, String, String, String, serde_json::Value)> = log
        .iter()
        .rev()
        .map(|e| {
            assert_eq!(e.org_id.as_deref(), Some(org));
            assert!(e.created_at_ms > 0);
            (
                e.actor.clone(),
                e.action.clone(),
                e.target_type.clone(),
                e.target_id.clone(),
                e.detail.clone(),
            )
        })
        .collect();
    let row = |actor: &str, action: &str, target_type: &str, target: &str, detail| {
        (
            actor.to_string(),
            action.to_string(),
            target_type.to_string(),
            target.to_string(),
            detail,
        )
    };
    let by = |id: &str| format!("user:{id}");
    let suffix = |key: &str| key[key.len() - 4..].to_string();
    let slug = names.slug("acme");
    use serde_json::json;
    assert_eq!(
        got,
        vec![
            row(
                &by(owner),
                "org.created",
                "organization",
                org,
                json!({"slug": slug, "owner_user_id": owner})
            ),
            row(
                &by(owner),
                "member.added",
                "user",
                admin,
                json!({"role": "admin"})
            ),
            row(
                &by(owner),
                "member.added",
                "user",
                member,
                json!({"role": "member"})
            ),
            row(
                &by(owner),
                "member.added",
                "user",
                read_only,
                json!({"role": "read-only"})
            ),
            row(
                &by(admin),
                "project.created",
                "project",
                &project.id,
                json!({"slug": "search"})
            ),
            row(
                &by(admin),
                "tenant.linked",
                "tenant",
                &tenant.tenant_id,
                json!({"project_id": project.id, "existing_tenant": false})
            ),
            row(
                &by(member),
                "api_key.created",
                "tenant",
                &tenant.tenant_id,
                json!({"key_suffix": suffix(&key.api_key), "description": "ci"})
            ),
            row(
                &by(owner),
                "member.role_changed",
                "user",
                read_only,
                json!({"from": "read-only", "to": "member"})
            ),
            row(
                &by(admin),
                "member.removed",
                "user",
                read_only,
                json!({"role": "member"})
            ),
        ]
    );
    // The log never holds a key.
    let text = serde_json::to_string(&log).unwrap();
    assert!(!text.contains(&key.api_key));

    // Newest first, paged by id.
    assert!(log.windows(2).all(|w| w[0].id > w[1].id));
    let first = orgs::list_audit(store, &f.admin, org, Some(4), None).unwrap();
    assert_eq!(first, log[..4]);
    let second = orgs::list_audit(store, &f.admin, org, Some(4), Some(first[3].id)).unwrap();
    assert_eq!(second, log[4..8]);
    let rest = orgs::list_audit(store, &f.admin, org, Some(0), Some(second[3].id)).unwrap();
    assert_eq!(rest, log[8..9], "a limit of 0 is raised to 1");

    // Another organization's log holds none of it.
    let other = orgs::create_org(store, &f.outsider, &names.slug("other"), "Other", None).unwrap();
    let other_log = orgs::list_audit(store, &f.outsider, &other.id, None, None).unwrap();
    assert_eq!(other_log.len(), 1);
    assert_eq!(other_log[0].action, "org.created");
}

/// A user a named operator's credential vouched for (#346) is that user for
/// every rule, and the audit row keeps the credential in `via`.
fn vouched_users_keep_their_role_and_name_the_credential(store: &dyn OrgStore) {
    let names = Names::new();
    let f = fixture(store, &names);
    let org = f.org.as_str();
    let via = |actor: &Actor| Actor::VouchedUser {
        user_id: Fixture::id(actor).to_string(),
        operator: "alice".to_string(),
    };
    assert_eq!(via(&f.owner).label(), f.owner.label());

    // Bound by the user's role, not lifted to the operator's powers.
    forbidden(orgs::create_project(
        store,
        &via(&f.member),
        org,
        "nope",
        "Nope",
    ));
    forbidden(orgs::create_user(
        store,
        &via(&f.owner),
        &names.email("x"),
        "",
    ));
    forbidden(orgs::get_org(store, &via(&f.outsider), org));
    // Leaving is still leaving, and the last owner still cannot.
    conflict(orgs::remove_member(
        store,
        &via(&f.owner),
        org,
        Fixture::id(&f.owner),
    ));

    let project = orgs::create_project(store, &via(&f.admin), org, "vouched", "V").unwrap();
    orgs::create_project(store, &f.admin, org, "direct", "D").unwrap();
    let member = Fixture::id(&f.member);
    orgs::remove_member(store, &via(&f.owner), org, member).unwrap();

    let log = orgs::list_audit(store, &f.owner, org, None, None).unwrap();
    let got: Vec<_> = log
        .iter()
        .take(3)
        .rev()
        .map(|e| (e.actor.clone(), e.action.as_str(), e.detail.clone()))
        .collect();
    assert_eq!(
        got,
        vec![
            (
                f.admin.label(),
                "project.created",
                serde_json::json!({ "slug": project.slug, "via": "operator:alice" }),
            ),
            (
                f.admin.label(),
                "project.created",
                serde_json::json!({ "slug": "direct" }),
            ),
            (
                f.owner.label(),
                "member.removed",
                serde_json::json!({ "role": "member", "via": "operator:alice" }),
            ),
        ]
    );
}

fn all(store: &dyn OrgStore) {
    users_and_organizations(store);
    roles_gate_every_operation(store);
    membership_rules(store);
    audit_log_records_each_action_once(store);
    vouched_users_keep_their_role_and_name_the_credential(store);
}

#[test]
fn memory_users_and_organizations() {
    users_and_organizations(&MemoryOrgStore::new());
}

#[test]
fn memory_roles_gate_every_operation() {
    roles_gate_every_operation(&MemoryOrgStore::new());
}

#[test]
fn memory_membership_rules() {
    membership_rules(&MemoryOrgStore::new());
}

#[test]
fn memory_audit_log_records_each_action_once() {
    audit_log_records_each_action_once(&MemoryOrgStore::new());
}

#[test]
fn memory_vouched_users_keep_their_role_and_name_the_credential() {
    vouched_users_keep_their_role_and_name_the_credential(&MemoryOrgStore::new());
}

#[test]
fn memory_keys_map_to_their_tenant() {
    let store = MemoryOrgStore::new();
    let names = Names::new();
    let f = fixture(&store, &names);
    let tenant = orgs::link_tenant(&store, &f.owner, &f.org, None, None).unwrap();
    let key = orgs::create_api_key(&store, &f.member, &f.org, &tenant.tenant_id, "").unwrap();
    assert_eq!(store.api_key_tenant(&key.api_key), Some(tenant.tenant_id));
    // One store, several organizations: the whole suite shares it.
    all(&store);
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    use kosha_control::PgStore;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    fn store() -> PgStore {
        let url =
            std::env::var("KOSHA_TEST_DATABASE_URL").expect("set a dedicated test database URL");
        PgStore::new(&url).unwrap()
    }

    #[test]
    #[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
    fn postgres_runs_the_same_scenarios() {
        // Twice on one database: the migration is re-applied by each
        // `PgStore::new` and earlier rows do not disturb a later run.
        all(&store());
        all(&store());
    }

    #[test]
    #[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
    fn postgres_keys_authenticate_and_existing_keys_are_untouched() {
        let store = store();
        let names = Names::new();
        // A key for a tenant no organization knows about, made the old way.
        let legacy_tenant = names.slug("legacy");
        let legacy_key = store
            .create_api_key(
                &kosha_control::NewApiKey {
                    tenant_id: legacy_tenant.clone(),
                    description: "pre-orgs".to_string(),
                    scopes: kosha_control::Scopes::ALL,
                    namespaces: None,
                    expires_at: None,
                },
                "operator",
            )
            .unwrap()
            .api_key;

        let f = fixture(&store, &names);
        let tenant = orgs::link_tenant(&store, &f.owner, &f.org, None, None).unwrap();
        let key = orgs::create_api_key(&store, &f.member, &f.org, &tenant.tenant_id, "").unwrap();
        assert_eq!(store.validate_api_key(&key.api_key), Some(tenant.tenant_id));

        // The unlinked tenant's key still validates, and the operator can
        // link the tenant afterwards without disturbing it.
        assert_eq!(
            store.validate_api_key(&legacy_key),
            Some(legacy_tenant.clone())
        );
        // The operator's `POST /v1/admin/api-keys`: scoped, namespace-limited
        // and expiring, for a tenant with or without an organization.
        let operator_key = kosha_control::NewApiKey {
            tenant_id: legacy_tenant.clone(),
            description: "ops".to_string(),
            scopes: kosha_control::Scopes::of(&[kosha_control::Scope::Read]),
            namespaces: Some(vec!["docs".to_string()]),
            expires_at: Some(4_102_444_800),
        };
        let unlinked = store.create_api_key(&operator_key, "operator").unwrap();
        orgs::link_tenant(&store, &Actor::Operator, &f.org, Some(&legacy_tenant), None).unwrap();
        for key in [&legacy_key, &unlinked.api_key] {
            assert_eq!(store.validate_api_key(key), Some(legacy_tenant.clone()));
        }
        assert!(store
            .list_api_keys(Some(&legacy_tenant))
            .unwrap()
            .iter()
            .any(|k| k.key_hash == kosha_control::hash_api_key(&legacy_key)));
        // The key made before the link was audited with no organization, so
        // it is not in this organization's log; one made after is.
        let linked = store.create_api_key(&operator_key, "operator").unwrap();
        let log = orgs::list_audit(&store, &f.owner, &f.org, None, None).unwrap();
        let created: Vec<_> = log
            .iter()
            .filter(|e| e.action == "api_key.created" && e.target_id == legacy_tenant)
            .collect();
        assert_eq!(created.len(), 1, "{created:?}");
        assert_eq!(created[0].actor, "operator");
        assert_eq!(created[0].target_type, "tenant");
        // What the key may do is in the entry; the key and its id are not.
        assert_eq!(
            created[0].detail,
            serde_json::json!({
                "key_suffix": &linked.api_key[linked.api_key.len() - 4..],
                "key_id_prefix": &linked.record.key_hash[..12],
                "description": "ops",
                "scopes": ["read"],
                "namespaces": ["docs"],
                "expires_at": 4_102_444_800_i64,
            })
        );
        let text = serde_json::to_string(&log).unwrap();
        assert!(!text.contains(&linked.api_key) && !text.contains(&linked.record.key_hash));
    }

    #[test]
    #[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
    fn postgres_refused_operation_rolls_back() {
        let store = store();
        let names = Names::new();
        let op = Actor::Operator;
        let ada = orgs::create_user(&store, &op, &names.email("ada"), "").unwrap();
        let slug = names.slug("rollback");
        // The second insert fails inside its transaction (unique slug).
        let org = orgs::create_org(&store, &op, &slug, "First", Some(&ada.id)).unwrap();
        conflict(orgs::create_org(
            &store,
            &op,
            &slug,
            "Second",
            Some(&ada.id),
        ));
        // The store is usable after the rolled-back transaction.
        let grace = orgs::create_user(&store, &op, &names.email("grace"), "").unwrap();
        conflict(orgs::create_user(&store, &op, &names.email("grace"), ""));
        orgs::add_member(&store, &op, &org.id, &grace.id, Role::Owner).unwrap();
        assert_eq!(orgs::list_members(&store, &op, &org.id).unwrap().len(), 2);
    }

    /// Two owners remove each other at the same moment, on separate
    /// connections. The first transaction is held open after it takes the
    /// organization's row lock, so the second provably overlaps it: without
    /// the lock the second would still count two owners, both removals
    /// would commit, and the organization would have none.
    #[test]
    #[ignore = "requires a disposable KOSHA_TEST_DATABASE_URL"]
    fn postgres_concurrent_owner_removals_keep_one_owner() {
        let observer = store();
        let op = Actor::Operator;
        let names = Names::new();
        let a = orgs::create_user(&observer, &op, &names.email("a"), "").unwrap();
        let b = orgs::create_user(&observer, &op, &names.email("b"), "").unwrap();
        let org =
            orgs::create_org(&observer, &op, &names.slug("race"), "Race", Some(&a.id)).unwrap();
        orgs::add_member(&observer, &op, &org.id, &b.id, Role::Owner).unwrap();

        let (first, second) = (store(), store());
        let (locked_tx, locked_rx) = mpsc::channel();
        let b_removing_a = std::thread::scope(|scope| {
            // A removes B: the steps `remove_member` takes, paused between
            // taking the lock and the delete.
            scope.spawn(|| {
                first
                    .transact(&mut |tx| {
                        assert!(tx.org(&org.id, true)?.is_some());
                        assert_eq!(tx.owner_count(&org.id)?, 2);
                        locked_tx.send(()).unwrap();
                        std::thread::sleep(Duration::from_millis(500));
                        tx.remove_member(&org.id, &b.id)
                    })
                    .unwrap();
            });
            locked_rx.recv().unwrap();
            let started = Instant::now();
            let result = orgs::remove_member(&second, &user(&b.id), &org.id, &a.id);
            (result, started.elapsed())
        });
        // B waited for A's transaction, and by then was no longer a member.
        let (result, waited) = b_removing_a;
        forbidden(result);
        assert!(waited >= Duration::from_millis(300), "{waited:?}");
        let members = orgs::list_members(&observer, &op, &org.id).unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(
            (members[0].user_id.as_str(), members[0].role),
            (a.id.as_str(), Role::Owner)
        );
    }
}
