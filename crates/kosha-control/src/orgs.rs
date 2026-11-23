//! Organizations, users, memberships, projects, tenant links and the
//! control-plane audit log (#345).
//!
//! Before this, a tenant was only a string on a `kosha.api_keys` row. This
//! module adds the model above it:
//!
//! ```text
//! organization ──< membership (owner | admin | member | read-only) >── user
//!      │
//!      ├──< project (optional grouping)
//!      │
//!      └──< tenant link ── tenant id ──< api_keys rows, `{tenant}/...` namespaces
//! ```
//!
//! It is additive. A tenant with no link row (every tenant that existed
//! before this) keeps working exactly as before: request authentication
//! still maps an API key to its tenant id and never reads these tables.
//!
//! Every operation here runs in one [`OrgStore::transact`] transaction that
//! checks the caller's role, makes the change and appends the audit row, so
//! an audited action and its audit entry commit or roll back together. The
//! rules are written once, against [`OrgTx`]; [`MemoryOrgStore`] (default
//! build, tests) and `PgStore` (the `postgres` feature, migration 006) only
//! supply storage.
//!
//! Callers are identified by an [`Actor`]: the operator (the holder of the
//! server's admin credential acting for itself) or a user id the admin
//! credential's holder vouches for. Nothing here authenticates a user; that
//! is the job of the sign-up/login layer that will sit in front of it.

use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// A user's role in one organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    #[serde(rename = "owner")]
    Owner,
    #[serde(rename = "admin")]
    Admin,
    #[serde(rename = "member")]
    Member,
    #[serde(rename = "read-only")]
    ReadOnly,
}

/// What a role may do inside its organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// Read the organization, its members, projects and tenants.
    View,
    /// Create an API key for one of the organization's tenants.
    CreateApiKey,
    /// Create projects.
    ManageProjects,
    /// Create tenants.
    ManageTenants,
    /// Add and remove members and change roles (owner-role changes need an
    /// owner: see `check_member_change`).
    ManageMembers,
    /// Read the audit log.
    ViewAuditLog,
}

impl Role {
    pub const ALL: [Role; 4] = [Role::Owner, Role::Admin, Role::Member, Role::ReadOnly];

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Owner => "owner",
            Role::Admin => "admin",
            Role::Member => "member",
            Role::ReadOnly => "read-only",
        }
    }

    pub fn parse(s: &str) -> Option<Role> {
        Role::ALL.into_iter().find(|r| r.as_str() == s)
    }

    /// The permission matrix.
    pub fn allows(self, permission: Permission) -> bool {
        match permission {
            Permission::View => true,
            Permission::CreateApiKey => self != Role::ReadOnly,
            Permission::ManageProjects
            | Permission::ManageTenants
            | Permission::ManageMembers
            | Permission::ViewAuditLog => matches!(self, Role::Owner | Role::Admin),
        }
    }
}

/// Who is asking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Actor {
    /// The admin credential's holder acting for itself. Not bound by roles.
    Operator,
    /// The holder of a named operator admin credential (#346) acting for
    /// itself. Has the operator's powers; the name is what the audit log
    /// records.
    NamedOperator(String),
    /// A user (by id), bound by their role in the organization.
    User(String),
    /// A user a named operator's credential (#346) vouched for: the request
    /// carried that credential and named the user. Exactly a
    /// [`Actor::User`] for every rule; the audit row still says
    /// `user:<id>`, and its detail gains `"via": "operator:<name>"`, so the
    /// credential that made the request is never lost.
    VouchedUser { user_id: String, operator: String },
}

impl Actor {
    /// How the actor is written into `audit_log.actor`.
    pub fn label(&self) -> String {
        match self {
            Actor::Operator => "operator".to_string(),
            Actor::NamedOperator(name) => format!("operator:{name}"),
            Actor::User(id) | Actor::VouchedUser { user_id: id, .. } => format!("user:{id}"),
        }
    }

    /// The user the actor is, `None` for an operator acting for itself.
    pub fn user_id(&self) -> Option<&str> {
        match self {
            Actor::Operator | Actor::NamedOperator(_) => None,
            Actor::User(id) | Actor::VouchedUser { user_id: id, .. } => Some(id),
        }
    }

    /// The audit label of the named operator credential that vouched for
    /// this user, when there is one: what an audit row's `via` holds.
    pub fn vouched_by(&self) -> Option<String> {
        match self {
            Actor::VouchedUser { operator, .. } => Some(format!("operator:{operator}")),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrgError {
    /// The request itself is malformed (400).
    Invalid(String),
    /// The actor's role does not allow it (403).
    Forbidden(String),
    /// The organization, user, project or tenant does not exist (404).
    NotFound(String),
    /// It collides with existing state (409).
    Conflict(String),
    /// The store failed (500).
    Store(String),
}

impl std::fmt::Display for OrgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrgError::Invalid(m)
            | OrgError::Forbidden(m)
            | OrgError::NotFound(m)
            | OrgError::Conflict(m) => f.write_str(m),
            OrgError::Store(m) => write!(f, "control plane error: {m}"),
        }
    }
}

impl std::error::Error for OrgError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct User {
    pub id: String,
    pub email: String,
    pub display_name: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Organization {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub created_at_ms: i64,
}

/// One membership, joined with the user it belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Member {
    pub user_id: String,
    pub email: String,
    pub display_name: String,
    pub role: Role,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Project {
    pub id: String,
    pub org_id: String,
    pub slug: String,
    pub name: String,
    pub created_at_ms: i64,
}

/// The link from a tenant id (the namespace prefix API keys map to) to the
/// organization, and optionally the project, that owns it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TenantLink {
    pub tenant_id: String,
    pub org_id: String,
    pub project_id: Option<String>,
    pub created_at_ms: i64,
}

/// A newly created API key. The only time the key itself is returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApiKey {
    pub api_key: String,
    pub tenant_id: String,
    pub description: String,
}

/// An audit row to append. `org_id` is `None` for actions outside any
/// organization (a user created, a key for an unlinked tenant).
#[derive(Debug, Clone, PartialEq)]
pub struct NewAuditEvent {
    pub org_id: Option<String>,
    pub actor: String,
    pub action: &'static str,
    pub target_type: &'static str,
    pub target_id: String,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AuditEvent {
    /// Grows with insertion order; the paging cursor.
    pub id: i64,
    pub org_id: Option<String>,
    pub actor: String,
    pub action: String,
    pub target_type: String,
    pub target_id: String,
    pub detail: serde_json::Value,
    pub created_at_ms: i64,
}

/// Storage primitives inside one transaction. No rules live here.
///
/// Inserts fail with [`OrgError::Conflict`] on a uniqueness collision
/// (email, organization slug, project slug within its organization, tenant
/// id). After any error the transaction is unusable and must be abandoned,
/// which every operation in this module does by returning the error.
pub trait OrgTx {
    fn user(&mut self, id: &str) -> Result<Option<User>, OrgError>;
    fn insert_user(&mut self, user: &User) -> Result<(), OrgError>;
    /// `lock` takes the organization's row lock for the rest of the
    /// transaction, serializing membership changes in that organization.
    fn org(&mut self, id: &str, lock: bool) -> Result<Option<Organization>, OrgError>;
    fn insert_org(&mut self, org: &Organization) -> Result<(), OrgError>;
    fn member_role(&mut self, org_id: &str, user_id: &str) -> Result<Option<Role>, OrgError>;
    fn owner_count(&mut self, org_id: &str) -> Result<u64, OrgError>;
    /// Insert the membership, or change its role if it exists.
    fn put_member(
        &mut self,
        org_id: &str,
        user_id: &str,
        role: Role,
        created_at_ms: i64,
    ) -> Result<(), OrgError>;
    fn remove_member(&mut self, org_id: &str, user_id: &str) -> Result<(), OrgError>;
    /// Lists are ordered by a unique key (members by email, projects by
    /// slug, tenants by id) and capped at [`MAX_LIST`].
    fn members(&mut self, org_id: &str) -> Result<Vec<Member>, OrgError>;
    fn project(&mut self, id: &str) -> Result<Option<Project>, OrgError>;
    fn insert_project(&mut self, project: &Project) -> Result<(), OrgError>;
    fn projects(&mut self, org_id: &str) -> Result<Vec<Project>, OrgError>;
    fn tenant(&mut self, tenant_id: &str) -> Result<Option<TenantLink>, OrgError>;
    fn insert_tenant(&mut self, tenant: &TenantLink) -> Result<(), OrgError>;
    fn tenants(&mut self, org_id: &str) -> Result<Vec<TenantLink>, OrgError>;
    fn insert_api_key(&mut self, key: &ApiKey) -> Result<(), OrgError>;
    fn insert_audit(&mut self, event: &NewAuditEvent) -> Result<(), OrgError>;
    /// Newest first, at most `limit`, only ids below `before` when given.
    fn audit(
        &mut self,
        org_id: &str,
        limit: u32,
        before: Option<i64>,
    ) -> Result<Vec<AuditEvent>, OrgError>;
}

/// A store that can run a closure in one all-or-nothing transaction.
pub trait OrgStore: Send + Sync {
    /// Run `f` in a transaction: commit if it returns `Ok`, roll back
    /// everything it did if it returns `Err`.
    fn transact(
        &self,
        f: &mut dyn FnMut(&mut dyn OrgTx) -> Result<(), OrgError>,
    ) -> Result<(), OrgError>;
}

fn run<R>(
    store: &dyn OrgStore,
    mut f: impl FnMut(&mut dyn OrgTx) -> Result<R, OrgError>,
) -> Result<R, OrgError> {
    let mut out = None;
    store.transact(&mut |tx| {
        out = Some(f(tx)?);
        Ok(())
    })?;
    out.ok_or_else(|| OrgError::Store("transaction committed without a result".into()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

// ── Validation ──────────────────────────────────────────────────────────────

const MAX_SLUG_LEN: usize = 63;
const MAX_NAME_LEN: usize = 200;
const MAX_EMAIL_LEN: usize = 254;
const MAX_DESCRIPTION_LEN: usize = 500;
/// The most rows a list call returns (and the audit log's largest page).
pub const MAX_LIST: u32 = 1000;
pub const DEFAULT_AUDIT_PAGE: u32 = 100;

/// Lowercase letters, digits and inner hyphens; starts with a letter or
/// digit. Organization slugs prefix generated tenant ids, so this is also
/// a valid tenant id prefix.
fn check_slug(what: &str, slug: &str) -> Result<(), OrgError> {
    let ok = !slug.is_empty()
        && slug.len() <= MAX_SLUG_LEN
        && slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !slug.starts_with('-')
        && !slug.ends_with('-');
    if ok {
        Ok(())
    } else {
        Err(OrgError::Invalid(format!(
            "invalid {what} slug: 1-{MAX_SLUG_LEN} characters of a-z, 0-9 and '-', \
             not starting or ending with '-'"
        )))
    }
}

fn check_text(what: &str, value: &str, max: usize, required: bool) -> Result<(), OrgError> {
    if required && value.trim().is_empty() {
        return Err(OrgError::Invalid(format!("'{what}' must not be empty")));
    }
    if value.chars().count() > max || value.chars().any(char::is_control) {
        return Err(OrgError::Invalid(format!(
            "invalid '{what}': at most {max} characters, no control characters"
        )));
    }
    Ok(())
}

/// Trimmed and lowercased, so uniqueness is case-insensitive. A shape
/// check only: whether the address is real is email verification's job.
fn normalize_email(email: &str) -> Result<String, OrgError> {
    let email = email.trim().to_lowercase();
    let shape_ok = match email.split_once('@') {
        Some((local, domain)) => !local.is_empty() && !domain.is_empty() && !domain.contains('@'),
        None => false,
    };
    if !shape_ok
        || email.len() > MAX_EMAIL_LEN
        || email.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(OrgError::Invalid("invalid 'email'".into()));
    }
    Ok(email)
}

/// The server's rule for a tenant id on a new API key: one path component
/// that does not start with `_` or `.` (those are accounted to the shared
/// `_legacy` tenant).
fn check_tenant_id(tenant_id: &str) -> Result<(), OrgError> {
    let ok = !tenant_id.is_empty()
        && tenant_id.len() <= MAX_NAME_LEN
        && !tenant_id.starts_with('_')
        && !tenant_id.starts_with('.')
        && !tenant_id
            .chars()
            .any(|c| c == '/' || c == '\\' || c.is_control());
    if ok {
        Ok(())
    } else {
        Err(OrgError::Invalid(
            "invalid 'tenant_id' (must be non-empty, without '/', '\\' or control \
             characters, and not start with '_' or '.')"
                .into(),
        ))
    }
}

// ── Authorization ───────────────────────────────────────────────────────────

fn require_operator(actor: &Actor, what: &str) -> Result<(), OrgError> {
    match actor.user_id() {
        None => Ok(()),
        Some(_) => Err(OrgError::Forbidden(format!("only the operator can {what}"))),
    }
}

fn require_org(tx: &mut dyn OrgTx, org_id: &str, lock: bool) -> Result<Organization, OrgError> {
    tx.org(org_id, lock)?
        .ok_or_else(|| OrgError::NotFound(format!("organization '{org_id}' not found")))
}

/// The actor's role in the organization: `None` for the operator, who is
/// not bound by roles. A user who is not a member is refused.
fn actor_role(tx: &mut dyn OrgTx, actor: &Actor, org_id: &str) -> Result<Option<Role>, OrgError> {
    match actor.user_id() {
        None => Ok(None),
        Some(user_id) => tx
            .member_role(org_id, user_id)?
            .map(Some)
            .ok_or_else(|| OrgError::Forbidden("not a member of this organization".into())),
    }
}

fn authorize(
    tx: &mut dyn OrgTx,
    actor: &Actor,
    org_id: &str,
    permission: Permission,
) -> Result<Option<Role>, OrgError> {
    let role = actor_role(tx, actor, org_id)?;
    match role {
        Some(role) if !role.allows(permission) => Err(OrgError::Forbidden(format!(
            "role '{}' is not allowed to do this",
            role.as_str()
        ))),
        _ => Ok(role),
    }
}

/// The owner rules for a membership change from `current` to `new` (`None`
/// is "not a member"), by an actor with `actor_role` (`None` = operator):
/// only an owner (or the operator) grants, changes or revokes the owner
/// role, and an organization never loses its last owner.
fn check_member_change(
    actor_role: Option<Role>,
    current: Option<Role>,
    new: Option<Role>,
    owner_count: u64,
) -> Result<(), OrgError> {
    let touches_owner = current == Some(Role::Owner) || new == Some(Role::Owner);
    if touches_owner && actor_role.is_some_and(|r| r != Role::Owner) {
        return Err(OrgError::Forbidden(
            "only an owner can grant, change or revoke the owner role".into(),
        ));
    }
    if current == Some(Role::Owner) && new != Some(Role::Owner) && owner_count <= 1 {
        return Err(OrgError::Conflict(
            "an organization must keep at least one owner".into(),
        ));
    }
    Ok(())
}

fn audit(
    tx: &mut dyn OrgTx,
    actor: &Actor,
    org_id: Option<&str>,
    action: &'static str,
    target_type: &'static str,
    target_id: &str,
    mut detail: serde_json::Value,
) -> Result<(), OrgError> {
    // A user's action made through a named operator's credential says so:
    // without it the row could not be told from the user's own.
    if let (Some(via), Some(fields)) = (actor.vouched_by(), detail.as_object_mut()) {
        fields.insert("via".into(), serde_json::Value::String(via));
    }
    tx.insert_audit(&NewAuditEvent {
        org_id: org_id.map(str::to_string),
        actor: actor.label(),
        action,
        target_type,
        target_id: target_id.to_string(),
        detail,
    })
}

// ── Operations ──────────────────────────────────────────────────────────────

/// Register a user. Operator only: self-serve sign-up is not built yet.
pub fn create_user(
    store: &dyn OrgStore,
    actor: &Actor,
    email: &str,
    display_name: &str,
) -> Result<User, OrgError> {
    require_operator(actor, "create users")?;
    let email = normalize_email(email)?;
    check_text("display_name", display_name, MAX_NAME_LEN, false)?;
    run(store, |tx| {
        let user = User {
            id: new_id("usr"),
            email: email.clone(),
            display_name: display_name.trim().to_string(),
            created_at_ms: now_ms(),
        };
        tx.insert_user(&user)?;
        audit(
            tx,
            actor,
            None,
            "user.created",
            "user",
            &user.id,
            serde_json::json!({ "email": user.email }),
        )?;
        Ok(user)
    })
}

/// A user's record: the operator may read any, a user only their own.
pub fn get_user(store: &dyn OrgStore, actor: &Actor, user_id: &str) -> Result<User, OrgError> {
    if let Some(id) = actor.user_id() {
        if id != user_id {
            return Err(OrgError::Forbidden("users can only read themselves".into()));
        }
    }
    run(store, |tx| {
        tx.user(user_id)?
            .ok_or_else(|| OrgError::NotFound(format!("user '{user_id}' not found")))
    })
}

/// Create an organization with its first owner. A user creates one for
/// themselves; the operator names the owner.
pub fn create_org(
    store: &dyn OrgStore,
    actor: &Actor,
    slug: &str,
    name: &str,
    owner_user_id: Option<&str>,
) -> Result<Organization, OrgError> {
    check_slug("organization", slug)?;
    check_text("name", name, MAX_NAME_LEN, true)?;
    let owner = match (actor.user_id(), owner_user_id) {
        (Some(id), Some(owner)) if owner != id => {
            return Err(OrgError::Forbidden(
                "only the operator can create an organization for another user".into(),
            ))
        }
        (Some(id), _) => id.to_string(),
        (None, Some(owner)) => owner.to_string(),
        (None, None) => {
            return Err(OrgError::Invalid("missing 'owner_user_id'".into()));
        }
    };
    run(store, |tx| {
        if tx.user(&owner)?.is_none() {
            return Err(match actor.user_id() {
                Some(_) => OrgError::Forbidden("unknown acting user".into()),
                None => OrgError::NotFound(format!("user '{owner}' not found")),
            });
        }
        let org = Organization {
            id: new_id("org"),
            slug: slug.to_string(),
            name: name.trim().to_string(),
            created_at_ms: now_ms(),
        };
        tx.insert_org(&org)?;
        tx.put_member(&org.id, &owner, Role::Owner, org.created_at_ms)?;
        audit(
            tx,
            actor,
            Some(&org.id),
            "org.created",
            "organization",
            &org.id,
            serde_json::json!({ "slug": org.slug, "owner_user_id": owner }),
        )?;
        Ok(org)
    })
}

pub fn get_org(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
) -> Result<Organization, OrgError> {
    run(store, |tx| {
        let org = require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::View)?;
        Ok(org)
    })
}

pub fn list_members(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
) -> Result<Vec<Member>, OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::View)?;
        tx.members(org_id)
    })
}

/// Add an existing user to the organization.
pub fn add_member(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
    user_id: &str,
    role: Role,
) -> Result<(), OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, true)?;
        let acting = authorize(tx, actor, org_id, Permission::ManageMembers)?;
        if tx.user(user_id)?.is_none() {
            return Err(OrgError::NotFound(format!("user '{user_id}' not found")));
        }
        if tx.member_role(org_id, user_id)?.is_some() {
            return Err(OrgError::Conflict(
                "user is already a member of this organization".into(),
            ));
        }
        let owners = tx.owner_count(org_id)?;
        check_member_change(acting, None, Some(role), owners)?;
        tx.put_member(org_id, user_id, role, now_ms())?;
        audit(
            tx,
            actor,
            Some(org_id),
            "member.added",
            "user",
            user_id,
            serde_json::json!({ "role": role.as_str() }),
        )
    })
}

/// Change a member's role. Setting the role they already have changes
/// nothing and writes no audit row.
pub fn set_member_role(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
    user_id: &str,
    role: Role,
) -> Result<(), OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, true)?;
        let acting = authorize(tx, actor, org_id, Permission::ManageMembers)?;
        let current = tx
            .member_role(org_id, user_id)?
            .ok_or_else(|| OrgError::NotFound(format!("user '{user_id}' is not a member")))?;
        if current == role {
            return Ok(());
        }
        let owners = tx.owner_count(org_id)?;
        check_member_change(acting, Some(current), Some(role), owners)?;
        tx.put_member(org_id, user_id, role, now_ms())?;
        audit(
            tx,
            actor,
            Some(org_id),
            "member.role_changed",
            "user",
            user_id,
            serde_json::json!({ "from": current.as_str(), "to": role.as_str() }),
        )
    })
}

/// Remove a member. Any member may remove themselves (leave); removing
/// someone else needs `ManageMembers`.
pub fn remove_member(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
    user_id: &str,
) -> Result<(), OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, true)?;
        let leaving = actor.user_id() == Some(user_id);
        let acting = if leaving {
            actor_role(tx, actor, org_id)?
        } else {
            authorize(tx, actor, org_id, Permission::ManageMembers)?
        };
        let current = tx
            .member_role(org_id, user_id)?
            .ok_or_else(|| OrgError::NotFound(format!("user '{user_id}' is not a member")))?;
        let owners = tx.owner_count(org_id)?;
        check_member_change(acting, Some(current), None, owners)?;
        tx.remove_member(org_id, user_id)?;
        audit(
            tx,
            actor,
            Some(org_id),
            "member.removed",
            "user",
            user_id,
            serde_json::json!({ "role": current.as_str() }),
        )
    })
}

pub fn create_project(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
    slug: &str,
    name: &str,
) -> Result<Project, OrgError> {
    check_slug("project", slug)?;
    check_text("name", name, MAX_NAME_LEN, true)?;
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::ManageProjects)?;
        let project = Project {
            id: new_id("prj"),
            org_id: org_id.to_string(),
            slug: slug.to_string(),
            name: name.trim().to_string(),
            created_at_ms: now_ms(),
        };
        tx.insert_project(&project)?;
        audit(
            tx,
            actor,
            Some(org_id),
            "project.created",
            "project",
            &project.id,
            serde_json::json!({ "slug": project.slug }),
        )?;
        Ok(project)
    })
}

pub fn list_projects(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
) -> Result<Vec<Project>, OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::View)?;
        tx.projects(org_id)
    })
}

/// Give the organization a tenant, optionally under one of its projects.
///
/// With `tenant_id: None` a new id `{org-slug}-{12 hex}` is generated;
/// this is the only form a user may call. Naming an id links a tenant that
/// already exists (one that predates organizations) and is operator-only:
/// whoever controls a tenant's link can mint keys for its namespaces, so a
/// user must never be able to claim an id of their choosing.
pub fn link_tenant(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
    tenant_id: Option<&str>,
    project_id: Option<&str>,
) -> Result<TenantLink, OrgError> {
    if let Some(tenant_id) = tenant_id {
        require_operator(
            actor,
            "link an existing tenant id (omit 'tenant_id' to create a new tenant)",
        )?;
        check_tenant_id(tenant_id)?;
    }
    run(store, |tx| {
        let org = require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::ManageTenants)?;
        if let Some(project_id) = project_id {
            let in_org = tx.project(project_id)?.is_some_and(|p| p.org_id == org_id);
            if !in_org {
                return Err(OrgError::NotFound(format!(
                    "project '{project_id}' not found in this organization"
                )));
            }
        }
        let generated = || {
            let hex = uuid::Uuid::new_v4().simple().to_string();
            format!("{}-{}", org.slug, &hex[..12])
        };
        let link = TenantLink {
            tenant_id: tenant_id.map(str::to_string).unwrap_or_else(generated),
            org_id: org_id.to_string(),
            project_id: project_id.map(str::to_string),
            created_at_ms: now_ms(),
        };
        tx.insert_tenant(&link)?;
        audit(
            tx,
            actor,
            Some(org_id),
            "tenant.linked",
            "tenant",
            &link.tenant_id,
            serde_json::json!({
                "project_id": link.project_id,
                "existing_tenant": tenant_id.is_some(),
            }),
        )?;
        Ok(link)
    })
}

pub fn list_tenants(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
) -> Result<Vec<TenantLink>, OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::View)?;
        tx.tenants(org_id)
    })
}

/// The audit detail for a created key: enough to tell keys apart, never
/// the key.
fn key_detail(key: &ApiKey) -> serde_json::Value {
    let tail = key.api_key.len().saturating_sub(4);
    serde_json::json!({
        "key_suffix": key.api_key.get(tail..).unwrap_or(""),
        "description": key.description,
    })
}

fn new_api_key(tenant_id: &str, description: &str) -> ApiKey {
    ApiKey {
        api_key: format!("sk-{}", uuid::Uuid::new_v4()),
        tenant_id: tenant_id.to_string(),
        description: description.to_string(),
    }
}

/// Create an API key for one of the organization's tenants.
pub fn create_api_key(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
    tenant_id: &str,
    description: &str,
) -> Result<ApiKey, OrgError> {
    check_text("description", description, MAX_DESCRIPTION_LEN, false)?;
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::CreateApiKey)?;
        let linked = tx.tenant(tenant_id)?.is_some_and(|t| t.org_id == org_id);
        if !linked {
            return Err(OrgError::NotFound(format!(
                "tenant '{tenant_id}' not found in this organization"
            )));
        }
        let key = new_api_key(tenant_id, description);
        tx.insert_api_key(&key)?;
        audit(
            tx,
            actor,
            Some(org_id),
            "api_key.created",
            "tenant",
            tenant_id,
            key_detail(&key),
        )?;
        Ok(key)
    })
}

// The operator's `POST /v1/admin/api-keys` (a key for any tenant id, with
// scopes, namespaces and an expiry) is `PgStore::create_api_key`, which
// writes its own `api_key.created` row: see `api_key_store`.

/// One page of the organization's audit log, newest first. Page with
/// `before` = the last `id` of the previous page.
pub fn list_audit(
    store: &dyn OrgStore,
    actor: &Actor,
    org_id: &str,
    limit: Option<u32>,
    before: Option<i64>,
) -> Result<Vec<AuditEvent>, OrgError> {
    let limit = limit.unwrap_or(DEFAULT_AUDIT_PAGE).clamp(1, MAX_LIST);
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::ViewAuditLog)?;
        tx.audit(org_id, limit, before)
    })
}

// ── In-memory store ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct MemberRow {
    org_id: String,
    user_id: String,
    role: Role,
    created_at_ms: i64,
}

#[derive(Debug, Clone, Default)]
struct MemoryState {
    users: Vec<User>,
    orgs: Vec<Organization>,
    members: Vec<MemberRow>,
    projects: Vec<Project>,
    tenants: Vec<TenantLink>,
    api_keys: Vec<ApiKey>,
    audit: Vec<AuditEvent>,
}

/// The store for a server without Postgres, like the in-memory
/// `Controller`: state lives in this process and is gone on restart.
#[derive(Debug, Default)]
pub struct MemoryOrgStore {
    state: Mutex<MemoryState>,
}

impl MemoryOrgStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// The tenant a key created through this store belongs to.
    pub fn api_key_tenant(&self, api_key: &str) -> Option<String> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .api_keys
            .iter()
            .find(|k| k.api_key == api_key)
            .map(|k| k.tenant_id.clone())
    }
}

impl OrgStore for MemoryOrgStore {
    fn transact(
        &self,
        f: &mut dyn FnMut(&mut dyn OrgTx) -> Result<(), OrgError>,
    ) -> Result<(), OrgError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // Work on a copy and install it only on success: an error part-way
        // through leaves no trace, as a rolled-back transaction would.
        let mut work = state.clone();
        f(&mut work)?;
        *state = work;
        Ok(())
    }
}

fn conflict(message: &str) -> OrgError {
    OrgError::Conflict(message.to_string())
}

pub(crate) const USER_EXISTS: &str = "a user with this email already exists";
pub(crate) const ORG_EXISTS: &str = "an organization with this slug already exists";
pub(crate) const PROJECT_EXISTS: &str =
    "a project with this slug already exists in this organization";
pub(crate) const TENANT_LINKED: &str = "this tenant id already belongs to an organization";

impl OrgTx for MemoryState {
    fn user(&mut self, id: &str) -> Result<Option<User>, OrgError> {
        Ok(self.users.iter().find(|u| u.id == id).cloned())
    }

    fn insert_user(&mut self, user: &User) -> Result<(), OrgError> {
        if self.users.iter().any(|u| u.email == user.email) {
            return Err(conflict(USER_EXISTS));
        }
        self.users.push(user.clone());
        Ok(())
    }

    fn org(&mut self, id: &str, _lock: bool) -> Result<Option<Organization>, OrgError> {
        Ok(self.orgs.iter().find(|o| o.id == id).cloned())
    }

    fn insert_org(&mut self, org: &Organization) -> Result<(), OrgError> {
        if self.orgs.iter().any(|o| o.slug == org.slug) {
            return Err(conflict(ORG_EXISTS));
        }
        self.orgs.push(org.clone());
        Ok(())
    }

    fn member_role(&mut self, org_id: &str, user_id: &str) -> Result<Option<Role>, OrgError> {
        Ok(self
            .members
            .iter()
            .find(|m| m.org_id == org_id && m.user_id == user_id)
            .map(|m| m.role))
    }

    fn owner_count(&mut self, org_id: &str) -> Result<u64, OrgError> {
        Ok(self
            .members
            .iter()
            .filter(|m| m.org_id == org_id && m.role == Role::Owner)
            .count() as u64)
    }

    fn put_member(
        &mut self,
        org_id: &str,
        user_id: &str,
        role: Role,
        created_at_ms: i64,
    ) -> Result<(), OrgError> {
        match self
            .members
            .iter_mut()
            .find(|m| m.org_id == org_id && m.user_id == user_id)
        {
            Some(member) => member.role = role,
            None => self.members.push(MemberRow {
                org_id: org_id.to_string(),
                user_id: user_id.to_string(),
                role,
                created_at_ms,
            }),
        }
        Ok(())
    }

    fn remove_member(&mut self, org_id: &str, user_id: &str) -> Result<(), OrgError> {
        self.members
            .retain(|m| !(m.org_id == org_id && m.user_id == user_id));
        Ok(())
    }

    fn members(&mut self, org_id: &str) -> Result<Vec<Member>, OrgError> {
        let mut members: Vec<Member> = self
            .members
            .iter()
            .filter(|m| m.org_id == org_id)
            .filter_map(|m| {
                let user = self.users.iter().find(|u| u.id == m.user_id)?;
                Some(Member {
                    user_id: m.user_id.clone(),
                    email: user.email.clone(),
                    display_name: user.display_name.clone(),
                    role: m.role,
                    created_at_ms: m.created_at_ms,
                })
            })
            .collect();
        members.sort_by(|a, b| a.email.cmp(&b.email));
        members.truncate(MAX_LIST as usize);
        Ok(members)
    }

    fn project(&mut self, id: &str) -> Result<Option<Project>, OrgError> {
        Ok(self.projects.iter().find(|p| p.id == id).cloned())
    }

    fn insert_project(&mut self, project: &Project) -> Result<(), OrgError> {
        if self
            .projects
            .iter()
            .any(|p| p.org_id == project.org_id && p.slug == project.slug)
        {
            return Err(conflict(PROJECT_EXISTS));
        }
        self.projects.push(project.clone());
        Ok(())
    }

    fn projects(&mut self, org_id: &str) -> Result<Vec<Project>, OrgError> {
        let mut projects: Vec<Project> = self
            .projects
            .iter()
            .filter(|p| p.org_id == org_id)
            .cloned()
            .collect();
        projects.sort_by(|a, b| a.slug.cmp(&b.slug));
        projects.truncate(MAX_LIST as usize);
        Ok(projects)
    }

    fn tenant(&mut self, tenant_id: &str) -> Result<Option<TenantLink>, OrgError> {
        Ok(self
            .tenants
            .iter()
            .find(|t| t.tenant_id == tenant_id)
            .cloned())
    }

    fn insert_tenant(&mut self, tenant: &TenantLink) -> Result<(), OrgError> {
        if self.tenants.iter().any(|t| t.tenant_id == tenant.tenant_id) {
            return Err(conflict(TENANT_LINKED));
        }
        self.tenants.push(tenant.clone());
        Ok(())
    }

    fn tenants(&mut self, org_id: &str) -> Result<Vec<TenantLink>, OrgError> {
        let mut tenants: Vec<TenantLink> = self
            .tenants
            .iter()
            .filter(|t| t.org_id == org_id)
            .cloned()
            .collect();
        tenants.sort_by(|a, b| a.tenant_id.cmp(&b.tenant_id));
        tenants.truncate(MAX_LIST as usize);
        Ok(tenants)
    }

    fn insert_api_key(&mut self, key: &ApiKey) -> Result<(), OrgError> {
        self.api_keys.push(key.clone());
        Ok(())
    }

    fn insert_audit(&mut self, event: &NewAuditEvent) -> Result<(), OrgError> {
        let id = self.audit.last().map_or(1, |e| e.id + 1);
        self.audit.push(AuditEvent {
            id,
            org_id: event.org_id.clone(),
            actor: event.actor.clone(),
            action: event.action.to_string(),
            target_type: event.target_type.to_string(),
            target_id: event.target_id.clone(),
            detail: event.detail.clone(),
            created_at_ms: now_ms(),
        });
        Ok(())
    }

    fn audit(
        &mut self,
        org_id: &str,
        limit: u32,
        before: Option<i64>,
    ) -> Result<Vec<AuditEvent>, OrgError> {
        Ok(self
            .audit
            .iter()
            .rev()
            .filter(|e| e.org_id.as_deref() == Some(org_id))
            .filter(|e| before.is_none_or(|b| e.id < b))
            .take(limit as usize)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_matrix() {
        use Permission::*;
        let allowed = |role: Role| {
            [
                View,
                CreateApiKey,
                ManageProjects,
                ManageTenants,
                ManageMembers,
                ViewAuditLog,
            ]
            .map(|p| role.allows(p))
        };
        assert_eq!(allowed(Role::Owner), [true; 6]);
        assert_eq!(allowed(Role::Admin), [true; 6]);
        assert_eq!(
            allowed(Role::Member),
            [true, true, false, false, false, false]
        );
        assert_eq!(
            allowed(Role::ReadOnly),
            [true, false, false, false, false, false]
        );
    }

    #[test]
    fn role_names_round_trip() {
        for role in Role::ALL {
            assert_eq!(Role::parse(role.as_str()), Some(role));
            assert_eq!(
                serde_json::to_value(role).unwrap(),
                serde_json::json!(role.as_str())
            );
        }
        assert_eq!(Role::parse("read_only"), None);
        assert_eq!(Role::parse("Owner"), None);
    }

    #[test]
    fn owner_rules() {
        let (owner, admin) = (Some(Role::Owner), Some(Role::Admin));
        // An admin manages non-owners but never the owner role.
        assert!(check_member_change(admin, None, Some(Role::Admin), 1).is_ok());
        assert!(check_member_change(admin, Some(Role::Member), None, 1).is_ok());
        for (current, new) in [
            (None, owner),
            (Some(Role::Member), owner),
            (owner, Some(Role::Member)),
            (owner, None),
        ] {
            assert!(matches!(
                check_member_change(admin, current, new, 2),
                Err(OrgError::Forbidden(_))
            ));
        }
        // An owner and the operator may, but not below one owner.
        for actor in [owner, None] {
            assert!(check_member_change(actor, None, owner, 1).is_ok());
            assert!(check_member_change(actor, owner, None, 2).is_ok());
            assert!(matches!(
                check_member_change(actor, owner, None, 1),
                Err(OrgError::Conflict(_))
            ));
            assert!(matches!(
                check_member_change(actor, owner, Some(Role::Admin), 1),
                Err(OrgError::Conflict(_))
            ));
        }
    }

    #[test]
    fn validation() {
        for slug in ["a", "acme-corp", "0x", &"a".repeat(63)] {
            assert!(check_slug("organization", slug).is_ok(), "{slug}");
        }
        for slug in ["", "-a", "a-", "Acme", "a_b", "a/b", "a b", &"a".repeat(64)] {
            assert!(check_slug("organization", slug).is_err(), "{slug}");
        }
        assert_eq!(
            normalize_email(" Ada@Example.COM ").unwrap(),
            "ada@example.com"
        );
        for email in [
            "",
            "ada",
            "@example.com",
            "ada@",
            "a@b@c",
            "a da@example.com",
        ] {
            assert!(normalize_email(email).is_err(), "{email}");
        }
        assert!(check_tenant_id("acme-corp").is_ok());
        for tenant in ["", "_legacy", ".hidden", "a/b", "a\\b", "a\nb"] {
            assert!(check_tenant_id(tenant).is_err(), "{tenant:?}");
        }
    }

    #[test]
    fn a_failed_operation_leaves_no_trace() {
        let store = MemoryOrgStore::new();
        let result = store.transact(&mut |tx| {
            tx.insert_user(&User {
                id: "usr_1".into(),
                email: "a@example.com".into(),
                display_name: String::new(),
                created_at_ms: 0,
            })?;
            Err(OrgError::Store("boom".into()))
        });
        assert_eq!(result, Err(OrgError::Store("boom".into())));
        assert!(matches!(
            get_user(&store, &Actor::Operator, "usr_1"),
            Err(OrgError::NotFound(_))
        ));
    }
}
