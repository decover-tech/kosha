//! Who a console user is (#345 part 2): self-serve sign-up, email
//! verification, passwords, sessions, OAuth/OIDC identities, per-
//! organization single sign-on, invites, and the organization's API keys as
//! the console manages them.
//!
//! This sits on top of [`crate::orgs`]: a user here is a `kosha.users` row,
//! and every organization operation a signed-in user makes still goes
//! through the role checks there, as [`Actor::User`]. What this adds is the
//! proof that the user is who they say, so the console no longer needs an
//! operator to vouch for them.
//!
//! Decisions:
//! - **Passwords** are hashed with argon2id (the `argon2` crate's defaults:
//!   19 MiB, 2 passes, 1 lane, the OWASP minimum), at least
//!   [`MIN_PASSWORD_LEN`] characters. A login for an unknown address still
//!   runs one hash, so timing does not say which addresses exist.
//! - **Tokens** (sessions, verification, reset, invites, OAuth state) are
//!   256 random bits from the OS, shown once, and stored only as their
//!   SHA-256. Email tokens are single use: taking one marks it used in the
//!   same transaction as the change it authorizes, so a failed change leaves
//!   it usable and a replay finds it used.
//! - **Nothing says whether an address is registered**: sign-up, resend and
//!   reset answer the same either way (the mail that goes out differs), and
//!   a failed login is one error whatever the reason, until the password is
//!   right. Only then is "verify your email first" reported.
//! - **Linking by verified email.** A Google or GitHub sign-in whose
//!   provider-verified address matches a user signs in as that user. If that
//!   user's address was never verified, its password is dropped and its
//!   sessions revoked first: whoever registered the address without proving
//!   it must not keep a way in (pre-account takeover).
//! - **Organization SSO is trusted for its organization only.** An
//!   organization's admins configure its OIDC provider, so its assertions
//!   are not proof of an address in general. A session opened through it is
//!   scoped to that organization ([`session_may_act_in`]) and cannot create
//!   organizations or accept invites; just-in-time accounts and memberships
//!   (role `read-only`) are made only for addresses in the organization's
//!   allowed domains.
//! - **"SSO required"** refuses every session of a member that was not
//!   opened through the organization's SSO, except an owner's: owners keep a
//!   way in when the identity provider is broken or misconfigured.

use std::sync::OnceLock;

use serde::Serialize;

use crate::api_keys::{hash_api_key, Scopes};
use crate::orgs::{
    self, audit, authorize, check_member_change, check_text, new_api_key, new_id, normalize_email,
    now_ms, require_org, Actor, ApiKey, OrgError, OrgStore, OrgTx, Organization, Permission, Role,
    User, MAX_DESCRIPTION_LEN, MAX_NAME_LEN,
};

// ── Secrets ─────────────────────────────────────────────────────────────────

/// Shortest password accepted (characters).
pub const MIN_PASSWORD_LEN: usize = 12;
/// Longest password accepted (bytes): bounds the work one request can ask
/// argon2 to do.
pub const MAX_PASSWORD_LEN: usize = 256;

/// 256 random bits from the OS, as 64 lowercase hex characters.
pub fn new_token() -> String {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("the OS random number generator failed");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// What is stored instead of a token: its lowercase hex SHA-256. Tokens
/// are random, so an unsalted fast hash is enough (as for API keys).
pub fn token_hash(token: &str) -> String {
    hash_api_key(token)
}

fn check_password(password: &str) -> Result<(), OrgError> {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(OrgError::Invalid(format!(
            "the password must be at least {MIN_PASSWORD_LEN} characters"
        )));
    }
    if password.len() > MAX_PASSWORD_LEN {
        return Err(OrgError::Invalid(format!(
            "the password must be at most {MAX_PASSWORD_LEN} bytes"
        )));
    }
    Ok(())
}

/// An argon2id PHC string for `password`, with a fresh 128-bit salt.
pub fn hash_password(password: &str) -> Result<String, OrgError> {
    use argon2::password_hash::{PasswordHasher, SaltString};
    let mut salt = [0u8; 16];
    getrandom::getrandom(&mut salt).map_err(|e| OrgError::Store(format!("random salt: {e}")))?;
    let salt = SaltString::encode_b64(&salt).map_err(|e| OrgError::Store(e.to_string()))?;
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|e| OrgError::Store(format!("password hash: {e}")))
}

/// Whether `password` matches the PHC string `phc` (in constant time, by
/// the argon2 crate). A malformed `phc` matches nothing.
pub fn verify_password(password: &str, phc: &str) -> bool {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    if password.len() > MAX_PASSWORD_LEN {
        return false;
    }
    PasswordHash::new(phc).is_ok_and(|parsed| {
        argon2::Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}

/// Burn one password verification's worth of time, for a login whose
/// address has no password, so it takes as long as a wrong password.
fn verify_dummy_password(password: &str) {
    static DUMMY: OnceLock<String> = OnceLock::new();
    let phc = DUMMY.get_or_init(|| hash_password("kosha-dummy-password").unwrap_or_default());
    let _ = verify_password(password, phc);
}

/// The domain of a normalized address.
pub fn email_domain(email: &str) -> &str {
    email.rsplit_once('@').map_or("", |(_, domain)| domain)
}

/// Whether `email`'s domain is one of `domains` (lowercase). An empty list
/// allows every domain.
pub fn domain_allowed(email: &str, domains: &[String]) -> bool {
    domains.is_empty() || domains.iter().any(|d| d == email_domain(email))
}

// ── Records ─────────────────────────────────────────────────────────────────

/// A user's password and verification state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Credentials {
    pub user_id: String,
    /// argon2id PHC string; `None` for a user without a password (signed
    /// up with a provider, or whose unproven password was dropped).
    pub password_hash: Option<String>,
    pub email_verified: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    VerifyEmail,
    ResetPassword,
    Invite,
}

impl TokenKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TokenKind::VerifyEmail => "verify_email",
            TokenKind::ResetPassword => "reset_password",
            TokenKind::Invite => "invite",
        }
    }

    pub fn parse(s: &str) -> Option<TokenKind> {
        [
            TokenKind::VerifyEmail,
            TokenKind::ResetPassword,
            TokenKind::Invite,
        ]
        .into_iter()
        .find(|k| k.as_str() == s)
    }
}

/// A token sent by email. Stored under its [`token_hash`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmailToken {
    pub kind: TokenKind,
    /// The user it is for (verification, reset).
    pub user_id: Option<String>,
    /// The address it was sent to.
    pub email: String,
    /// The organization and role an invite grants.
    pub org_id: Option<String>,
    pub role: Option<Role>,
    /// Audit label of whoever caused it.
    pub created_by: String,
    pub expires_at_ms: i64,
    pub used: bool,
}

/// A console session. Stored under the [`token_hash`] of its cookie.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub user_id: String,
    /// `password`, `google`, `github` or `sso:<org id>`.
    pub auth_method: String,
    pub created_at_ms: i64,
    pub last_seen_at_ms: i64,
    pub expires_at_ms: i64,
    pub revoked: bool,
}

/// An organization's OIDC single sign-on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsoConfig {
    pub org_id: String,
    pub issuer: String,
    pub client_id: String,
    /// The client secret sealed by the server (`KOSHA_CONSOLE_SECRET_KEY`).
    /// This module never sees it in the clear.
    pub client_secret_sealed: String,
    /// Lowercase; empty: no just-in-time accounts or memberships.
    pub allowed_domains: Vec<String>,
    pub sso_required: bool,
    pub updated_at_ms: i64,
}

/// An OAuth/OIDC sign-in between the redirect and the callback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginFlow {
    /// `google`, `github` or `sso`.
    pub provider: String,
    /// The organization, for `sso`.
    pub org_id: Option<String>,
    pub pkce_verifier: String,
    pub nonce: String,
    pub expires_at_ms: i64,
}

/// A key to insert for an organization's tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrgKey {
    pub key_hash: String,
    pub tenant_id: String,
    pub org_id: String,
    pub description: String,
    pub scopes: Scopes,
    /// Unix seconds; `None` never expires.
    pub expires_at: Option<i64>,
    /// `None`: the operator.
    pub created_by: Option<String>,
}

/// One of an organization's API keys, as the console lists it. Never the
/// key itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrgApiKey {
    /// The key's hash: its public id.
    pub key_id: String,
    pub tenant_id: String,
    pub description: String,
    pub scopes: Vec<String>,
    /// Unix seconds.
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub last_used_at: Option<i64>,
    /// The user who created it through the console; `None` for a key made
    /// by the operator or before creators were recorded.
    pub created_by: Option<String>,
}

/// A user's membership as the console lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UserOrg {
    #[serde(flatten)]
    pub org: Organization,
    pub role: Role,
    pub sso_configured: bool,
    pub sso_required: bool,
}

// ── Storage ─────────────────────────────────────────────────────────────────

/// Storage primitives for this module, inside the same transaction as the
/// organization primitives. No rules live here.
pub trait IdentityTx: OrgTx {
    fn user_by_email(&mut self, email: &str) -> Result<Option<User>, OrgError>;
    fn credentials(&mut self, user_id: &str) -> Result<Option<Credentials>, OrgError>;
    fn put_credentials(&mut self, credentials: &Credentials) -> Result<(), OrgError>;
    fn insert_token(&mut self, token_hash: &str, token: &EmailToken) -> Result<(), OrgError>;
    /// Mark the token used and return it, if it exists and was unused.
    /// Expiry is the caller's to check.
    fn take_token(&mut self, token_hash: &str) -> Result<Option<EmailToken>, OrgError>;
    /// Mark every unused token of this kind for this user used.
    fn retire_tokens(&mut self, user_id: &str, kind: TokenKind) -> Result<(), OrgError>;
    fn insert_session(&mut self, token_hash: &str, session: &Session) -> Result<(), OrgError>;
    fn session(&mut self, token_hash: &str) -> Result<Option<Session>, OrgError>;
    fn touch_session(&mut self, token_hash: &str, now_ms: i64) -> Result<(), OrgError>;
    fn revoke_session(&mut self, token_hash: &str) -> Result<(), OrgError>;
    fn revoke_user_sessions(&mut self, user_id: &str) -> Result<(), OrgError>;
    fn identity_user(&mut self, provider: &str, subject: &str) -> Result<Option<String>, OrgError>;
    fn insert_identity(
        &mut self,
        provider: &str,
        subject: &str,
        user_id: &str,
        email: &str,
    ) -> Result<(), OrgError>;
    /// The user's organizations and roles, by slug, capped at
    /// [`orgs::MAX_LIST`].
    fn memberships(&mut self, user_id: &str) -> Result<Vec<(Organization, Role)>, OrgError>;
    fn org_by_slug(&mut self, slug: &str) -> Result<Option<Organization>, OrgError>;
    fn sso_config(&mut self, org_id: &str) -> Result<Option<SsoConfig>, OrgError>;
    fn put_sso_config(&mut self, config: &SsoConfig) -> Result<(), OrgError>;
    fn delete_sso_config(&mut self, org_id: &str) -> Result<bool, OrgError>;
    fn insert_login_flow(&mut self, state_hash: &str, flow: &LoginFlow) -> Result<(), OrgError>;
    /// Remove the flow and return it (single use). Expiry is the caller's.
    fn take_login_flow(&mut self, state_hash: &str) -> Result<Option<LoginFlow>, OrgError>;
    /// Insert the key (hashed) and who created it.
    fn insert_org_api_key(&mut self, key: &NewOrgKey) -> Result<(), OrgError>;
    /// The organization's tenants' keys that are not revoked, newest first,
    /// capped at [`orgs::MAX_LIST`].
    fn org_api_keys(&mut self, org_id: &str) -> Result<Vec<OrgApiKey>, OrgError>;
    /// Revoke a tenant key by hash; `false` if no active key has it.
    fn revoke_api_key(&mut self, key_hash: &str) -> Result<bool, OrgError>;
}

/// A store that runs identity operations in one transaction.
pub trait IdentityStore: OrgStore {
    fn transact_identity(
        &self,
        f: &mut dyn FnMut(&mut dyn IdentityTx) -> Result<(), OrgError>,
    ) -> Result<(), OrgError>;
}

fn run<R>(
    store: &dyn IdentityStore,
    mut f: impl FnMut(&mut dyn IdentityTx) -> Result<R, OrgError>,
) -> Result<R, OrgError> {
    let mut out = None;
    store.transact_identity(&mut |tx| {
        out = Some(f(tx)?);
        Ok(())
    })?;
    out.ok_or_else(|| OrgError::Store("transaction committed without a result".into()))
}

fn user_actor(user_id: &str) -> Actor {
    Actor::User(user_id.to_string())
}

fn invalid_token() -> OrgError {
    OrgError::Invalid("this link is invalid, already used or expired".into())
}

/// Take a token of `kind`: it must exist, be unused, unexpired and of that
/// kind, or nothing is consumed (the transaction rolls back).
fn take(tx: &mut dyn IdentityTx, token: &str, kind: TokenKind) -> Result<EmailToken, OrgError> {
    match tx.take_token(&token_hash(token.trim()))? {
        Some(t) if t.kind == kind && t.expires_at_ms > now_ms() => Ok(t),
        _ => Err(invalid_token()),
    }
}

fn issue_token(tx: &mut dyn IdentityTx, token: EmailToken) -> Result<String, OrgError> {
    let secret = new_token();
    tx.insert_token(&token_hash(&secret), &token)?;
    Ok(secret)
}

// ── Sign-up and email verification ──────────────────────────────────────────

/// How long an email verification link works.
pub const VERIFY_TTL_MS: i64 = 24 * 3600 * 1000;
/// How long a password reset link works.
pub const RESET_TTL_MS: i64 = 3600 * 1000;
/// How long an invite works.
pub const INVITE_TTL_MS: i64 = 7 * 24 * 3600 * 1000;

/// What a sign-up did. The HTTP answer is the same for both; only the mail
/// differs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignUp {
    /// A new user, and the verification token to mail them.
    Created { user: User, verify_token: String },
    /// The address is already registered. Nothing changed; tell its owner.
    Existing { user: User },
}

/// Register `email` with a password. Whether the address may sign up at
/// all (sign-up switched on, its domain allowed) is the caller's policy.
pub fn sign_up(
    store: &dyn IdentityStore,
    email: &str,
    password: &str,
    display_name: &str,
) -> Result<SignUp, OrgError> {
    let email = normalize_email(email)?;
    check_password(password)?;
    check_text("display_name", display_name, MAX_NAME_LEN, false)?;
    // Hashed outside the transaction: it is the slow part.
    let password_hash = hash_password(password)?;
    run(store, |tx| {
        if let Some(user) = tx.user_by_email(&email)? {
            return Ok(SignUp::Existing { user });
        }
        let user = User {
            id: new_id("usr"),
            email: email.clone(),
            display_name: display_name.trim().to_string(),
            created_at_ms: now_ms(),
        };
        tx.insert_user(&user)?;
        tx.put_credentials(&Credentials {
            user_id: user.id.clone(),
            password_hash: Some(password_hash.clone()),
            email_verified: false,
        })?;
        audit(
            tx,
            &user_actor(&user.id),
            None,
            "user.signed_up",
            "user",
            &user.id,
            serde_json::json!({ "email": user.email, "method": "password" }),
        )?;
        let verify_token = issue_token(
            tx,
            EmailToken {
                kind: TokenKind::VerifyEmail,
                user_id: Some(user.id.clone()),
                email: user.email.clone(),
                org_id: None,
                role: None,
                created_by: format!("user:{}", user.id),
                expires_at_ms: now_ms() + VERIFY_TTL_MS,
                used: false,
            },
        )?;
        Ok(SignUp::Created { user, verify_token })
    })
}

/// A fresh verification token for `email`, if it belongs to a user whose
/// address is not verified yet. `None` otherwise (unknown, or verified):
/// the caller answers the same either way.
pub fn resend_verification(
    store: &dyn IdentityStore,
    email: &str,
) -> Result<Option<(User, String)>, OrgError> {
    let Ok(email) = normalize_email(email) else {
        return Ok(None);
    };
    run(store, |tx| {
        let Some(user) = tx.user_by_email(&email)? else {
            return Ok(None);
        };
        let pending = tx
            .credentials(&user.id)?
            .is_some_and(|c| !c.email_verified && c.password_hash.is_some());
        if !pending {
            return Ok(None);
        }
        let token = issue_token(
            tx,
            EmailToken {
                kind: TokenKind::VerifyEmail,
                user_id: Some(user.id.clone()),
                email: user.email.clone(),
                org_id: None,
                role: None,
                created_by: format!("user:{}", user.id),
                expires_at_ms: now_ms() + VERIFY_TTL_MS,
                used: false,
            },
        )?;
        Ok(Some((user, token)))
    })
}

/// Mark the address of the token's user verified.
pub fn verify_email(store: &dyn IdentityStore, token: &str) -> Result<User, OrgError> {
    run(store, |tx| {
        let t = take(tx, token, TokenKind::VerifyEmail)?;
        let user_id = t.user_id.clone().ok_or_else(invalid_token)?;
        let user = tx.user(&user_id)?.ok_or_else(invalid_token)?;
        // A token sent to an address the user no longer has proves nothing.
        if user.email != t.email {
            return Err(invalid_token());
        }
        let mut credentials = tx.credentials(&user_id)?.unwrap_or(Credentials {
            user_id: user_id.clone(),
            password_hash: None,
            email_verified: false,
        });
        if !credentials.email_verified {
            credentials.email_verified = true;
            tx.put_credentials(&credentials)?;
            audit(
                tx,
                &user_actor(&user_id),
                None,
                "user.email_verified",
                "user",
                &user_id,
                serde_json::json!({ "email": user.email }),
            )?;
        }
        Ok(user)
    })
}

// ── Passwords ───────────────────────────────────────────────────────────────

/// Why a password login failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoginError {
    /// Unknown address, no password, or the wrong one: one answer for all.
    InvalidCredentials,
    /// The password is right but the address is not verified yet.
    Unverified,
    Store(String),
}

/// The user `email` and `password` identify.
pub fn password_login(
    store: &dyn IdentityStore,
    email: &str,
    password: &str,
) -> Result<User, LoginError> {
    let Ok(email) = normalize_email(email) else {
        verify_dummy_password(password);
        return Err(LoginError::InvalidCredentials);
    };
    let found = run(store, |tx| {
        let Some(user) = tx.user_by_email(&email)? else {
            return Ok(None);
        };
        let credentials = tx.credentials(&user.id)?;
        Ok(Some((user, credentials)))
    })
    .map_err(|e| LoginError::Store(e.to_string()))?;
    let (user, credentials) = match found {
        Some((user, Some(c))) if c.password_hash.is_some() => (user, c),
        _ => {
            verify_dummy_password(password);
            return Err(LoginError::InvalidCredentials);
        }
    };
    let phc = credentials.password_hash.as_deref().unwrap_or_default();
    if !verify_password(password, phc) {
        return Err(LoginError::InvalidCredentials);
    }
    if !credentials.email_verified {
        return Err(LoginError::Unverified);
    }
    Ok(user)
}

/// A password reset token for `email`'s user, `None` for an unknown
/// address (the caller answers the same either way). Works for a user
/// without a password too: proving the address by mail is how a user the
/// operator created, or one who signed up with a provider, sets one.
pub fn start_password_reset(
    store: &dyn IdentityStore,
    email: &str,
) -> Result<Option<(User, String)>, OrgError> {
    let Ok(email) = normalize_email(email) else {
        return Ok(None);
    };
    run(store, |tx| {
        let Some(user) = tx.user_by_email(&email)? else {
            return Ok(None);
        };
        let token = issue_token(
            tx,
            EmailToken {
                kind: TokenKind::ResetPassword,
                user_id: Some(user.id.clone()),
                email: user.email.clone(),
                org_id: None,
                role: None,
                created_by: format!("user:{}", user.id),
                expires_at_ms: now_ms() + RESET_TTL_MS,
                used: false,
            },
        )?;
        Ok(Some((user, token)))
    })
}

/// Set a new password with a reset token. The address is then verified
/// (the token came through it), every other reset link and every session
/// of the user stop working.
pub fn finish_password_reset(
    store: &dyn IdentityStore,
    token: &str,
    new_password: &str,
) -> Result<User, OrgError> {
    check_password(new_password)?;
    let password_hash = hash_password(new_password)?;
    run(store, |tx| {
        let t = take(tx, token, TokenKind::ResetPassword)?;
        let user_id = t.user_id.clone().ok_or_else(invalid_token)?;
        let user = tx.user(&user_id)?.ok_or_else(invalid_token)?;
        if user.email != t.email {
            return Err(invalid_token());
        }
        tx.put_credentials(&Credentials {
            user_id: user_id.clone(),
            password_hash: Some(password_hash.clone()),
            email_verified: true,
        })?;
        tx.retire_tokens(&user_id, TokenKind::ResetPassword)?;
        tx.revoke_user_sessions(&user_id)?;
        audit(
            tx,
            &user_actor(&user_id),
            None,
            "user.password_reset",
            "user",
            &user_id,
            serde_json::json!({}),
        )?;
        Ok(user)
    })
}

// ── Sessions ────────────────────────────────────────────────────────────────

/// How often a session's `last_seen` is written: a busy console must not
/// write a row on every request.
pub const SESSION_TOUCH_INTERVAL_MS: i64 = 60 * 1000;

/// Open a session for `user_id`; returns the cookie value (the only copy).
pub fn create_session(
    store: &dyn IdentityStore,
    user_id: &str,
    auth_method: &str,
    lifetime_ms: i64,
) -> Result<String, OrgError> {
    let token = new_token();
    let now = now_ms();
    run(store, |tx| {
        tx.insert_session(
            &token_hash(&token),
            &Session {
                user_id: user_id.to_string(),
                auth_method: auth_method.to_string(),
                created_at_ms: now,
                last_seen_at_ms: now,
                expires_at_ms: now + lifetime_ms,
                revoked: false,
            },
        )
    })?;
    Ok(token)
}

/// The live session for a cookie value: not revoked, within its lifetime
/// and used within `idle_timeout_ms`. Refreshes its last-seen time.
pub fn session(
    store: &dyn IdentityStore,
    token: &str,
    idle_timeout_ms: i64,
) -> Result<Option<(Session, User)>, OrgError> {
    let hash = token_hash(token.trim());
    let now = now_ms();
    run(store, |tx| {
        let Some(session) = tx.session(&hash)? else {
            return Ok(None);
        };
        let live = !session.revoked
            && session.expires_at_ms > now
            && session.last_seen_at_ms + idle_timeout_ms > now;
        if !live {
            return Ok(None);
        }
        let Some(user) = tx.user(&session.user_id)? else {
            return Ok(None);
        };
        if now - session.last_seen_at_ms >= SESSION_TOUCH_INTERVAL_MS {
            tx.touch_session(&hash, now)?;
        }
        Ok(Some((session.clone(), user)))
    })
}

/// End the session for a cookie value (logout). Unknown values are fine.
pub fn revoke_session(store: &dyn IdentityStore, token: &str) -> Result<(), OrgError> {
    let hash = token_hash(token.trim());
    run(store, |tx| tx.revoke_session(&hash))
}

/// Whether a session may act in `org_id`: see the module docs on SSO.
pub fn session_may_act_in(
    store: &dyn IdentityStore,
    session: &Session,
    org_id: &str,
) -> Result<(), OrgError> {
    let via_this_sso = session.auth_method == format!("sso:{org_id}");
    if session.auth_method.starts_with("sso:") && !via_this_sso {
        return Err(OrgError::Forbidden(
            "this session was opened through another organization's single sign-on; \
             sign in again to use this organization"
                .into(),
        ));
    }
    if via_this_sso {
        return Ok(());
    }
    run(store, |tx| {
        let Some(config) = tx.sso_config(org_id)? else {
            return Ok(());
        };
        if !config.sso_required {
            return Ok(());
        }
        match tx.member_role(org_id, &session.user_id)? {
            // Owners keep a way in when the identity provider fails.
            Some(Role::Owner) | None => Ok(()),
            Some(_) => Err(OrgError::Forbidden(
                "this organization requires signing in with its single sign-on".into(),
            )),
        }
    })
}

/// Whether a session may make account-wide changes (create an
/// organization, accept an invite): not one opened through an
/// organization's SSO.
pub fn session_is_global(session: &Session) -> Result<(), OrgError> {
    if session.auth_method.starts_with("sso:") {
        return Err(OrgError::Forbidden(
            "a session opened through an organization's single sign-on cannot do this; \
             sign in with a password, Google or GitHub"
                .into(),
        ));
    }
    Ok(())
}

/// The user's organizations with their role and SSO state.
pub fn user_orgs(store: &dyn IdentityStore, user_id: &str) -> Result<Vec<UserOrg>, OrgError> {
    run(store, |tx| {
        let memberships = tx.memberships(user_id)?;
        let mut out = Vec::with_capacity(memberships.len());
        for (org, role) in memberships {
            let sso = tx.sso_config(&org.id)?;
            out.push(UserOrg {
                sso_configured: sso.is_some(),
                sso_required: sso.is_some_and(|c| c.sso_required),
                org,
                role,
            });
        }
        Ok(out)
    })
}

// ── External identities (Google, GitHub, organization SSO) ──────────────────

/// A user as an external provider vouches for them. `email` must be an
/// address the provider verified; the caller refuses unverified ones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalIdentity {
    /// `google`, `github` or `sso:<org id>`.
    pub provider: String,
    /// The provider's stable id for the account.
    pub subject: String,
    pub email: String,
    pub display_name: String,
}

/// Find, link or create the user an external identity signs in as.
///
/// 1. An identity linked before signs in as its user.
/// 2. Otherwise a user with the same address is linked to it. If that
///    user's address was never verified, their password is dropped and
///    their sessions revoked first (see the module docs).
/// 3. Otherwise a user is created when `allow_create`, else `Forbidden`.
pub fn external_login(
    store: &dyn IdentityStore,
    identity: &ExternalIdentity,
    allow_create: bool,
) -> Result<User, OrgError> {
    let email = normalize_email(&identity.email)?;
    check_text("subject", &identity.subject, MAX_NAME_LEN, true)?;
    let display_name = clean_name(&identity.display_name);
    run(store, |tx| {
        external_login_tx(tx, identity, &email, &display_name, allow_create)
    })
}

/// A provider's display name, made safe to store: no control characters,
/// at most [`MAX_NAME_LEN`] characters.
fn clean_name(name: &str) -> String {
    name.chars()
        .filter(|c| !c.is_control())
        .take(MAX_NAME_LEN)
        .collect()
}

fn external_login_tx(
    tx: &mut dyn IdentityTx,
    identity: &ExternalIdentity,
    email: &str,
    display_name: &str,
    allow_create: bool,
) -> Result<User, OrgError> {
    if let Some(user_id) = tx.identity_user(&identity.provider, &identity.subject)? {
        return tx
            .user(&user_id)?
            .ok_or_else(|| OrgError::Store("identity linked to a missing user".into()));
    }
    let user = match tx.user_by_email(email)? {
        Some(user) => {
            let credentials = tx.credentials(&user.id)?;
            let verified = credentials.as_ref().is_some_and(|c| c.email_verified);
            if !verified {
                // Whoever registered the address without proving it loses
                // the password they chose and any session they hold.
                tx.put_credentials(&Credentials {
                    user_id: user.id.clone(),
                    password_hash: None,
                    email_verified: true,
                })?;
                tx.revoke_user_sessions(&user.id)?;
            }
            audit(
                tx,
                &user_actor(&user.id),
                None,
                "user.identity_linked",
                "user",
                &user.id,
                serde_json::json!({
                    "provider": identity.provider,
                    "unverified_password_dropped": !verified
                        && credentials.is_some_and(|c| c.password_hash.is_some()),
                }),
            )?;
            user
        }
        None if allow_create => {
            let user = User {
                id: new_id("usr"),
                email: email.to_string(),
                display_name: display_name.trim().to_string(),
                created_at_ms: now_ms(),
            };
            tx.insert_user(&user)?;
            tx.put_credentials(&Credentials {
                user_id: user.id.clone(),
                password_hash: None,
                email_verified: true,
            })?;
            audit(
                tx,
                &user_actor(&user.id),
                None,
                "user.signed_up",
                "user",
                &user.id,
                serde_json::json!({ "email": user.email, "method": identity.provider }),
            )?;
            user
        }
        None => {
            return Err(OrgError::Forbidden(
                "no account uses this address, and sign-up is not open".into(),
            ))
        }
    };
    tx.insert_identity(&identity.provider, &identity.subject, &user.id, email)?;
    Ok(user)
}

/// The role a just-in-time SSO membership gets.
pub const SSO_JIT_ROLE: Role = Role::ReadOnly;

/// Sign in through `org_id`'s SSO: [`external_login`] restricted to that
/// organization's members, plus just-in-time accounts and memberships for
/// addresses in its allowed domains.
pub fn sso_login(
    store: &dyn IdentityStore,
    org_id: &str,
    identity: &ExternalIdentity,
) -> Result<User, OrgError> {
    let email = normalize_email(&identity.email)?;
    check_text("subject", &identity.subject, MAX_NAME_LEN, true)?;
    run(store, |tx| {
        let config = tx
            .sso_config(org_id)?
            .ok_or_else(|| OrgError::NotFound("single sign-on is not configured".into()))?;
        let in_domain =
            !config.allowed_domains.is_empty() && domain_allowed(&email, &config.allowed_domains);
        let name = clean_name(&identity.display_name);
        let user = external_login_tx(tx, identity, &email, &name, in_domain)?;
        if tx.member_role(org_id, &user.id)?.is_none() {
            if !in_domain {
                return Err(OrgError::Forbidden(
                    "this account is not a member of the organization".into(),
                ));
            }
            require_org(tx, org_id, true)?;
            tx.put_member(org_id, &user.id, SSO_JIT_ROLE, now_ms())?;
            audit(
                tx,
                &user_actor(&user.id),
                Some(org_id),
                "member.added",
                "user",
                &user.id,
                serde_json::json!({ "role": SSO_JIT_ROLE.as_str(), "via": "sso" }),
            )?;
        }
        Ok(user)
    })
}

/// Record a login flow; returns nothing (the caller holds the state).
pub fn begin_login_flow(
    store: &dyn IdentityStore,
    state: &str,
    flow: &LoginFlow,
) -> Result<(), OrgError> {
    let hash = token_hash(state);
    run(store, |tx| tx.insert_login_flow(&hash, flow))
}

/// Take the flow a callback's `state` belongs to: single use, unexpired.
pub fn finish_login_flow(store: &dyn IdentityStore, state: &str) -> Result<LoginFlow, OrgError> {
    let hash = token_hash(state.trim());
    run(store, |tx| match tx.take_login_flow(&hash)? {
        Some(flow) if flow.expires_at_ms > now_ms() => Ok(flow),
        _ => Err(OrgError::Invalid(
            "this sign-in has expired or was already used; start again".into(),
        )),
    })
}

/// An organization by slug, for starting its SSO.
pub fn org_by_slug(store: &dyn IdentityStore, slug: &str) -> Result<Organization, OrgError> {
    let slug = slug.trim().to_ascii_lowercase();
    run(store, |tx| {
        tx.org_by_slug(&slug)?
            .ok_or_else(|| OrgError::NotFound("no such organization".into()))
    })
}

// ── SSO configuration ───────────────────────────────────────────────────────

/// The SSO settings an owner or admin submits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsoSettings {
    pub issuer: String,
    pub client_id: String,
    /// Sealed client secret; `None` keeps the stored one.
    pub client_secret_sealed: Option<String>,
    pub allowed_domains: Vec<String>,
    pub sso_required: bool,
}

fn check_sso_settings(settings: &SsoSettings) -> Result<Vec<String>, OrgError> {
    let issuer = settings.issuer.trim();
    let https = issuer.starts_with("https://");
    let local = issuer.starts_with("http://127.0.0.1") || issuer.starts_with("http://localhost");
    if !(https || local) || issuer.len() > 500 || issuer.chars().any(char::is_whitespace) {
        return Err(OrgError::Invalid(
            "'issuer' must be an https:// URL (the OIDC issuer identifier)".into(),
        ));
    }
    check_text("client_id", &settings.client_id, 500, true)?;
    let mut domains = Vec::new();
    for domain in &settings.allowed_domains {
        let domain = domain.trim().to_ascii_lowercase();
        let ok = !domain.is_empty()
            && domain.len() <= 253
            && domain.contains('.')
            && domain
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-');
        if !ok {
            return Err(OrgError::Invalid(format!(
                "invalid domain in 'allowed_domains': '{domain}'"
            )));
        }
        if !domains.contains(&domain) {
            domains.push(domain);
        }
    }
    if domains.len() > 50 {
        return Err(OrgError::Invalid("at most 50 'allowed_domains'".into()));
    }
    Ok(domains)
}

/// The organization's SSO config (owners and admins).
pub fn get_sso_config(
    store: &dyn IdentityStore,
    actor: &Actor,
    org_id: &str,
) -> Result<Option<SsoConfig>, OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::ManageSso)?;
        tx.sso_config(org_id)
    })
}

/// The SSO config a sign-in uses (no actor: the user is not known yet).
pub fn sso_config_for_login(
    store: &dyn IdentityStore,
    org_id: &str,
) -> Result<Option<SsoConfig>, OrgError> {
    run(store, |tx| tx.sso_config(org_id))
}

/// Create or replace the organization's SSO config. Requiring SSO needs an
/// owner (it can lock admins out of password sign-in).
pub fn put_sso_config(
    store: &dyn IdentityStore,
    actor: &Actor,
    org_id: &str,
    settings: &SsoSettings,
) -> Result<SsoConfig, OrgError> {
    let domains = check_sso_settings(settings)?;
    run(store, |tx| {
        require_org(tx, org_id, true)?;
        let role = authorize(tx, actor, org_id, Permission::ManageSso)?;
        let current = tx.sso_config(org_id)?;
        let turning_on = settings.sso_required && !current.as_ref().is_some_and(|c| c.sso_required);
        if turning_on && role.is_some_and(|r| r != Role::Owner) {
            return Err(OrgError::Forbidden(
                "only an owner can require single sign-on".into(),
            ));
        }
        let secret = match (&settings.client_secret_sealed, &current) {
            (Some(sealed), _) => sealed.clone(),
            (None, Some(current)) => current.client_secret_sealed.clone(),
            (None, None) => return Err(OrgError::Invalid("missing 'client_secret'".into())),
        };
        let config = SsoConfig {
            org_id: org_id.to_string(),
            issuer: settings.issuer.trim().trim_end_matches('/').to_string(),
            client_id: settings.client_id.trim().to_string(),
            client_secret_sealed: secret,
            allowed_domains: domains.clone(),
            sso_required: settings.sso_required,
            updated_at_ms: now_ms(),
        };
        tx.put_sso_config(&config)?;
        audit(
            tx,
            actor,
            Some(org_id),
            "sso.configured",
            "organization",
            org_id,
            serde_json::json!({
                "issuer": config.issuer,
                "client_id": config.client_id,
                "allowed_domains": config.allowed_domains,
                "sso_required": config.sso_required,
                "client_secret_changed": settings.client_secret_sealed.is_some(),
            }),
        )?;
        Ok(config)
    })
}

/// Remove the organization's SSO config (owners only when it was required).
pub fn delete_sso_config(
    store: &dyn IdentityStore,
    actor: &Actor,
    org_id: &str,
) -> Result<bool, OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, true)?;
        let role = authorize(tx, actor, org_id, Permission::ManageSso)?;
        let required = tx.sso_config(org_id)?.is_some_and(|c| c.sso_required);
        if required && role.is_some_and(|r| r != Role::Owner) {
            return Err(OrgError::Forbidden(
                "only an owner can remove a required single sign-on".into(),
            ));
        }
        let removed = tx.delete_sso_config(org_id)?;
        if removed {
            audit(
                tx,
                actor,
                Some(org_id),
                "sso.removed",
                "organization",
                org_id,
                serde_json::json!({}),
            )?;
        }
        Ok(removed)
    })
}

// ── Invites ─────────────────────────────────────────────────────────────────

/// An invite to mail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invite {
    pub token: String,
    pub email: String,
    pub org: Organization,
    pub role: Role,
    pub expires_at_ms: i64,
}

/// Invite `email` to the organization with `role`. Same rules as adding a
/// member: `ManageMembers`, and only an owner grants the owner role.
pub fn invite_member(
    store: &dyn IdentityStore,
    actor: &Actor,
    org_id: &str,
    email: &str,
    role: Role,
) -> Result<Invite, OrgError> {
    let email = normalize_email(email)?;
    run(store, |tx| {
        let org = require_org(tx, org_id, true)?;
        let acting = authorize(tx, actor, org_id, Permission::ManageMembers)?;
        let owners = tx.owner_count(org_id)?;
        check_member_change(acting, None, Some(role), owners)?;
        if let Some(user) = tx.user_by_email(&email)? {
            if tx.member_role(org_id, &user.id)?.is_some() {
                return Err(OrgError::Conflict(
                    "this address already belongs to a member".into(),
                ));
            }
        }
        let expires_at_ms = now_ms() + INVITE_TTL_MS;
        let token = issue_token(
            tx,
            EmailToken {
                kind: TokenKind::Invite,
                user_id: None,
                email: email.clone(),
                org_id: Some(org_id.to_string()),
                role: Some(role),
                created_by: actor.label(),
                expires_at_ms,
                used: false,
            },
        )?;
        audit(
            tx,
            actor,
            Some(org_id),
            "member.invited",
            "email",
            &email,
            serde_json::json!({ "role": role.as_str() }),
        )?;
        Ok(Invite {
            token,
            email: email.clone(),
            org,
            role,
            expires_at_ms,
        })
    })
}

/// Accept an invite as the signed-in `user_id`, whose (verified) address
/// must be the one invited.
pub fn accept_invite(
    store: &dyn IdentityStore,
    user_id: &str,
    token: &str,
) -> Result<(Organization, Role), OrgError> {
    run(store, |tx| {
        let invite = take(tx, token, TokenKind::Invite)?;
        let user = tx
            .user(user_id)?
            .ok_or_else(|| OrgError::Forbidden("unknown user".into()))?;
        if user.email != invite.email {
            return Err(OrgError::Forbidden(
                "this invite was sent to another address".into(),
            ));
        }
        let (Some(org_id), Some(role)) = (invite.org_id.clone(), invite.role) else {
            return Err(invalid_token());
        };
        let org = require_org(tx, &org_id, true)?;
        if tx.member_role(&org_id, user_id)?.is_some() {
            return Err(OrgError::Conflict(
                "you are already a member of this organization".into(),
            ));
        }
        tx.put_member(&org_id, user_id, role, now_ms())?;
        audit(
            tx,
            &user_actor(user_id),
            Some(&org_id),
            "member.added",
            "user",
            user_id,
            serde_json::json!({ "role": role.as_str(), "invited_by": invite.created_by }),
        )?;
        Ok((org, role))
    })
}

/// The address an unused, unexpired invite was sent to, without using it
/// (sign-up with an invite while open sign-up is off). `None` for any
/// other token.
pub fn invite_email(store: &dyn IdentityStore, token: &str) -> Result<Option<String>, OrgError> {
    let hash = token_hash(token.trim());
    let mut found = None;
    // Take the token and roll back: a read that leaves it unused.
    let peeked = store.transact_identity(&mut |tx| {
        if let Some(t) = tx.take_token(&hash)? {
            if t.kind == TokenKind::Invite && t.expires_at_ms > now_ms() {
                found = Some(t.email.clone());
            }
        }
        Err(OrgError::Store(PEEK.into()))
    });
    match peeked {
        Err(OrgError::Store(m)) if m == PEEK => Ok(found),
        Err(e) => Err(e),
        Ok(()) => Ok(found),
    }
}

const PEEK: &str = "peek: rolled back on purpose";

// ── Members ─────────────────────────────────────────────────────────────────

/// What removing a member did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Removed {
    /// Hashes (ids) of the keys revoked with the member.
    pub revoked_keys: Vec<String>,
}

/// [`orgs::remove_member`], and in the same transaction, when `revoke_keys`,
/// revoke every key of the organization the member created through the
/// console. The caller must also drop revoked keys from its pods' key
/// tables.
pub fn remove_member(
    store: &dyn IdentityStore,
    actor: &Actor,
    org_id: &str,
    user_id: &str,
    revoke_keys: bool,
) -> Result<Removed, OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, true)?;
        let leaving = actor.user_id() == Some(user_id);
        let acting = if leaving {
            orgs::actor_role(tx, actor, org_id)?
        } else {
            authorize(tx, actor, org_id, Permission::ManageMembers)?
        };
        let current = tx
            .member_role(org_id, user_id)?
            .ok_or_else(|| OrgError::NotFound(format!("user '{user_id}' is not a member")))?;
        let owners = tx.owner_count(org_id)?;
        check_member_change(acting, Some(current), None, owners)?;
        tx.remove_member(org_id, user_id)?;
        let mut revoked_keys = Vec::new();
        if revoke_keys {
            for key in tx.org_api_keys(org_id)? {
                if key.created_by.as_deref() == Some(user_id) && tx.revoke_api_key(&key.key_id)? {
                    revoked_keys.push(key.key_id);
                }
            }
        }
        audit(
            tx,
            actor,
            Some(org_id),
            "member.removed",
            "user",
            user_id,
            serde_json::json!({ "role": current.as_str(), "revoked_keys": revoked_keys.len() }),
        )?;
        for key_id in &revoked_keys {
            audit(
                tx,
                actor,
                Some(org_id),
                "api_key.revoked",
                "api_key",
                &key_prefix(key_id),
                serde_json::json!({ "reason": "member removed", "member": user_id }),
            )?;
        }
        Ok(Removed { revoked_keys })
    })
}

// ── API keys ────────────────────────────────────────────────────────────────

/// How much of a key id the audit log keeps (as the operator routes do).
fn key_prefix(key_id: &str) -> String {
    key_id.chars().take(12).collect()
}

/// A key created by [`create_org_api_key`]: the key itself (shown once)
/// and its listing entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedKey {
    pub key: ApiKey,
    pub listed: OrgApiKey,
}

/// Create a key for one of the organization's tenants (`CreateApiKey`:
/// member and up), recording who made it.
pub fn create_org_api_key(
    store: &dyn IdentityStore,
    actor: &Actor,
    org_id: &str,
    tenant_id: &str,
    description: &str,
    scopes: Scopes,
    expires_at: Option<i64>,
) -> Result<CreatedKey, OrgError> {
    check_text("description", description, MAX_DESCRIPTION_LEN, false)?;
    if scopes.is_empty() {
        return Err(OrgError::Invalid("a key needs at least one scope".into()));
    }
    if expires_at.is_some_and(|at| at * 1000 <= now_ms()) {
        return Err(OrgError::Invalid("'expires_at' is in the past".into()));
    }
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::CreateApiKey)?;
        let linked = tx.tenant(tenant_id)?.is_some_and(|t| t.org_id == org_id);
        if !linked {
            return Err(OrgError::NotFound(format!(
                "tenant '{tenant_id}' not found in this organization"
            )));
        }
        let key = new_api_key(tenant_id, description.trim());
        let key_hash = hash_api_key(&key.api_key);
        let new = NewOrgKey {
            key_hash: key_hash.clone(),
            tenant_id: tenant_id.to_string(),
            org_id: org_id.to_string(),
            description: key.description.clone(),
            scopes,
            expires_at,
            created_by: actor.user_id().map(str::to_string),
        };
        tx.insert_org_api_key(&new)?;
        let mut detail = orgs::key_detail(&key);
        detail["key_id_prefix"] = serde_json::json!(key_prefix(&key_hash));
        detail["scopes"] = serde_json::json!(scopes.names());
        audit(
            tx,
            actor,
            Some(org_id),
            "api_key.created",
            "tenant",
            tenant_id,
            detail,
        )?;
        Ok(CreatedKey {
            listed: OrgApiKey {
                key_id: key_hash,
                tenant_id: tenant_id.to_string(),
                description: key.description.clone(),
                scopes: scopes.names().into_iter().map(str::to_string).collect(),
                created_at: now_ms() / 1000,
                expires_at,
                last_used_at: None,
                created_by: new.created_by.clone(),
            },
            key,
        })
    })
}

/// The organization's keys that are not revoked (any member may list).
pub fn list_org_api_keys(
    store: &dyn IdentityStore,
    actor: &Actor,
    org_id: &str,
) -> Result<Vec<OrgApiKey>, OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        authorize(tx, actor, org_id, Permission::View)?;
        tx.org_api_keys(org_id)
    })
}

/// Revoke one of the organization's keys: owners and admins any key, a
/// member only one they created. Returns the revoked key.
pub fn revoke_org_api_key(
    store: &dyn IdentityStore,
    actor: &Actor,
    org_id: &str,
    key_id: &str,
) -> Result<OrgApiKey, OrgError> {
    run(store, |tx| {
        require_org(tx, org_id, false)?;
        let role = authorize(tx, actor, org_id, Permission::View)?;
        let key = tx
            .org_api_keys(org_id)?
            .into_iter()
            .find(|k| k.key_id == key_id)
            .ok_or_else(|| OrgError::NotFound("no such active key in this organization".into()))?;
        let own = key.created_by.is_some() && key.created_by.as_deref() == actor.user_id();
        let may = match role {
            None => true,
            Some(role) => role.allows(Permission::ManageApiKeys) || (own && role != Role::ReadOnly),
        };
        if !may {
            return Err(OrgError::Forbidden(
                "only owners, admins and the key's creator can revoke it".into(),
            ));
        }
        if !tx.revoke_api_key(key_id)? {
            return Err(OrgError::NotFound("no such active key".into()));
        }
        audit(
            tx,
            actor,
            Some(org_id),
            "api_key.revoked",
            "api_key",
            &key_prefix(key_id),
            serde_json::json!({ "tenant_id": key.tenant_id, "description": key.description }),
        )?;
        Ok(key)
    })
}

// ── Data-plane events ───────────────────────────────────────────────────────

/// Append an audit row for something that happened to a tenant outside the
/// console (a namespace deleted with a tenant key, a tenant deleted by the
/// operator), under the organization the tenant is linked to. Nothing is
/// written for a tenant no organization owns. `actor` is an audit label.
pub fn record_tenant_event(
    store: &dyn OrgStore,
    tenant_id: &str,
    actor: &str,
    action: &'static str,
    target_type: &'static str,
    target_id: &str,
    detail: serde_json::Value,
) -> Result<bool, OrgError> {
    orgs::run(store, |tx| {
        let Some(link) = tx.tenant(tenant_id)? else {
            return Ok(false);
        };
        tx.insert_audit(&orgs::NewAuditEvent {
            org_id: Some(link.org_id.clone()),
            actor: actor.to_string(),
            action,
            target_type,
            target_id: target_id.to_string(),
            detail: detail.clone(),
        })?;
        Ok(true)
    })
}

// ── In-memory store ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct MemKey {
    listed: OrgApiKey,
    org_id: String,
    revoked: bool,
}

/// The identity tables of [`orgs::MemoryOrgStore`].
#[derive(Debug, Clone, Default)]
pub struct MemoryIdentity {
    credentials: Vec<Credentials>,
    tokens: Vec<(String, EmailToken)>,
    sessions: Vec<(String, Session)>,
    identities: Vec<(String, String, String)>,
    sso: Vec<SsoConfig>,
    flows: Vec<(String, LoginFlow)>,
    keys: Vec<MemKey>,
}

impl orgs::MemoryOrgStore {
    /// The tenant of an active key created through the console (tests).
    pub fn console_key_tenant(&self, key_hash: &str) -> Option<String> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .identity
            .keys
            .iter()
            .find(|k| k.listed.key_id == key_hash && !k.revoked)
            .map(|k| k.listed.tenant_id.clone())
    }
}

impl IdentityStore for orgs::MemoryOrgStore {
    fn transact_identity(
        &self,
        f: &mut dyn FnMut(&mut dyn IdentityTx) -> Result<(), OrgError>,
    ) -> Result<(), OrgError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut work = state.clone();
        f(&mut work)?;
        *state = work;
        Ok(())
    }
}

impl IdentityTx for orgs::MemoryState {
    fn user_by_email(&mut self, email: &str) -> Result<Option<User>, OrgError> {
        Ok(self.users.iter().find(|u| u.email == email).cloned())
    }

    fn credentials(&mut self, user_id: &str) -> Result<Option<Credentials>, OrgError> {
        Ok(self
            .identity
            .credentials
            .iter()
            .find(|c| c.user_id == user_id)
            .cloned())
    }

    fn put_credentials(&mut self, credentials: &Credentials) -> Result<(), OrgError> {
        let list = &mut self.identity.credentials;
        match list.iter_mut().find(|c| c.user_id == credentials.user_id) {
            Some(c) => *c = credentials.clone(),
            None => list.push(credentials.clone()),
        }
        Ok(())
    }

    fn insert_token(&mut self, token_hash: &str, token: &EmailToken) -> Result<(), OrgError> {
        self.identity
            .tokens
            .push((token_hash.to_string(), token.clone()));
        Ok(())
    }

    fn take_token(&mut self, token_hash: &str) -> Result<Option<EmailToken>, OrgError> {
        Ok(self
            .identity
            .tokens
            .iter_mut()
            .find(|(h, t)| h == token_hash && !t.used)
            .map(|(_, t)| {
                t.used = true;
                t.clone()
            }))
    }

    fn retire_tokens(&mut self, user_id: &str, kind: TokenKind) -> Result<(), OrgError> {
        for (_, t) in &mut self.identity.tokens {
            if t.kind == kind && t.user_id.as_deref() == Some(user_id) {
                t.used = true;
            }
        }
        Ok(())
    }

    fn insert_session(&mut self, token_hash: &str, session: &Session) -> Result<(), OrgError> {
        self.identity
            .sessions
            .push((token_hash.to_string(), session.clone()));
        Ok(())
    }

    fn session(&mut self, token_hash: &str) -> Result<Option<Session>, OrgError> {
        Ok(self
            .identity
            .sessions
            .iter()
            .find(|(h, _)| h == token_hash)
            .map(|(_, s)| s.clone()))
    }

    fn touch_session(&mut self, token_hash: &str, now_ms: i64) -> Result<(), OrgError> {
        for (h, s) in &mut self.identity.sessions {
            if h == token_hash {
                s.last_seen_at_ms = now_ms;
            }
        }
        Ok(())
    }

    fn revoke_session(&mut self, token_hash: &str) -> Result<(), OrgError> {
        for (h, s) in &mut self.identity.sessions {
            if h == token_hash {
                s.revoked = true;
            }
        }
        Ok(())
    }

    fn revoke_user_sessions(&mut self, user_id: &str) -> Result<(), OrgError> {
        for (_, s) in &mut self.identity.sessions {
            if s.user_id == user_id {
                s.revoked = true;
            }
        }
        Ok(())
    }

    fn identity_user(&mut self, provider: &str, subject: &str) -> Result<Option<String>, OrgError> {
        Ok(self
            .identity
            .identities
            .iter()
            .find(|(p, s, _)| p == provider && s == subject)
            .map(|(_, _, u)| u.clone()))
    }

    fn insert_identity(
        &mut self,
        provider: &str,
        subject: &str,
        user_id: &str,
        _email: &str,
    ) -> Result<(), OrgError> {
        if self.identity_user(provider, subject)?.is_some() {
            return Err(OrgError::Conflict("identity already linked".into()));
        }
        self.identity.identities.push((
            provider.to_string(),
            subject.to_string(),
            user_id.to_string(),
        ));
        Ok(())
    }

    fn memberships(&mut self, user_id: &str) -> Result<Vec<(Organization, Role)>, OrgError> {
        Ok(self.user_memberships(user_id))
    }

    fn org_by_slug(&mut self, slug: &str) -> Result<Option<Organization>, OrgError> {
        Ok(self.orgs.iter().find(|o| o.slug == slug).cloned())
    }

    fn sso_config(&mut self, org_id: &str) -> Result<Option<SsoConfig>, OrgError> {
        Ok(self
            .identity
            .sso
            .iter()
            .find(|c| c.org_id == org_id)
            .cloned())
    }

    fn put_sso_config(&mut self, config: &SsoConfig) -> Result<(), OrgError> {
        self.identity.sso.retain(|c| c.org_id != config.org_id);
        self.identity.sso.push(config.clone());
        Ok(())
    }

    fn delete_sso_config(&mut self, org_id: &str) -> Result<bool, OrgError> {
        let before = self.identity.sso.len();
        self.identity.sso.retain(|c| c.org_id != org_id);
        Ok(self.identity.sso.len() != before)
    }

    fn insert_login_flow(&mut self, state_hash: &str, flow: &LoginFlow) -> Result<(), OrgError> {
        // Expired flows go here, so abandoned sign-ins do not pile up.
        let now = now_ms();
        self.identity.flows.retain(|(_, f)| f.expires_at_ms > now);
        self.identity
            .flows
            .push((state_hash.to_string(), flow.clone()));
        Ok(())
    }

    fn take_login_flow(&mut self, state_hash: &str) -> Result<Option<LoginFlow>, OrgError> {
        let at = self
            .identity
            .flows
            .iter()
            .position(|(h, _)| h == state_hash);
        Ok(at.map(|i| self.identity.flows.remove(i).1))
    }

    fn insert_org_api_key(&mut self, key: &NewOrgKey) -> Result<(), OrgError> {
        self.identity.keys.push(MemKey {
            listed: OrgApiKey {
                key_id: key.key_hash.clone(),
                tenant_id: key.tenant_id.clone(),
                description: key.description.clone(),
                scopes: key.scopes.names().into_iter().map(str::to_string).collect(),
                created_at: now_ms() / 1000,
                expires_at: key.expires_at,
                last_used_at: None,
                created_by: key.created_by.clone(),
            },
            org_id: key.org_id.clone(),
            revoked: false,
        });
        Ok(())
    }

    fn org_api_keys(&mut self, org_id: &str) -> Result<Vec<OrgApiKey>, OrgError> {
        let linked: Vec<&str> = self
            .tenants
            .iter()
            .filter(|t| t.org_id == org_id)
            .map(|t| t.tenant_id.as_str())
            .collect();
        let mut keys: Vec<OrgApiKey> = self
            .identity
            .keys
            .iter()
            .filter(|k| !k.revoked && k.org_id == org_id)
            .filter(|k| linked.contains(&k.listed.tenant_id.as_str()))
            .map(|k| k.listed.clone())
            .collect();
        keys.reverse();
        keys.truncate(orgs::MAX_LIST as usize);
        Ok(keys)
    }

    fn revoke_api_key(&mut self, key_hash: &str) -> Result<bool, OrgError> {
        Ok(self
            .identity
            .keys
            .iter_mut()
            .find(|k| k.listed.key_id == key_hash && !k.revoked)
            .map(|k| k.revoked = true)
            .is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passwords_hash_with_argon2id_and_verify() {
        let phc = hash_password("correct horse battery").unwrap();
        assert!(phc.starts_with("$argon2id$"), "{phc}");
        assert!(verify_password("correct horse battery", &phc));
        assert!(!verify_password("correct horse batterY", &phc));
        assert!(!verify_password("correct horse battery", "not a hash"));
        // A fresh salt every time.
        assert_ne!(phc, hash_password("correct horse battery").unwrap());
        assert!(check_password("short").is_err());
        assert!(check_password(&"x".repeat(MAX_PASSWORD_LEN + 1)).is_err());
        assert!(check_password("twelve chars").is_ok());
    }

    #[test]
    fn tokens_are_random_and_stored_hashed() {
        let a = new_token();
        assert_eq!(a.len(), 64);
        assert_ne!(a, new_token());
        assert_eq!(token_hash(&a), hash_api_key(&a));
        assert_ne!(token_hash(&a), a);
    }

    #[test]
    fn domains() {
        assert!(domain_allowed("a@x.com", &[]));
        assert!(domain_allowed("a@x.com", &["x.com".into()]));
        assert!(!domain_allowed("a@evilx.com", &["x.com".into()]));
        assert!(!domain_allowed("a@x.com.evil", &["x.com".into()]));
    }
}
