//! Self-serve sign-up and sign-in for the console (#345 part 2).
//!
//! Passwordless: [`request_signin`] mints a one-time link token for an
//! email address, the caller emails it, and [`complete_signin`] redeems it.
//! Redeeming a link proves the address, so it verifies the email and, the
//! first time, creates the user. It returns a session token the console
//! sends on every call; [`session_user`] resolves it and [`sign_out`]
//! revokes it. Only SHA-256 hashes of either token are stored.
//!
//! A signed-in user then acts as [`Actor::User`] through the `orgs`
//! operations: creating an organization makes them its owner, and the
//! role checks there decide everything else.

use crate::hash_api_key;
use crate::orgs::{
    audit, new_id, normalize_email, now_ms, run, Actor, OrgError, OrgStore, OrgTx, Organization,
    Role, User, MAX_NAME_LEN,
};
use serde::Serialize;

/// A link is good for 15 minutes, once.
pub const SIGNIN_TOKEN_TTL_MS: i64 = 15 * 60 * 1000;
/// A console session lasts 7 days.
pub const SESSION_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// At most one link per address per minute.
pub const SIGNIN_RESEND_MS: i64 = 60 * 1000;

const SIGNIN_PREFIX: &str = "kst_";
const SESSION_PREFIX: &str = "kss_";

/// A stored sign-in link: the hash of its token, never the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SigninToken {
    pub token_hash: String,
    pub email: String,
    pub display_name: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
}

/// A stored console session: the hash of its token, never the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserSession {
    pub token_hash: String,
    pub user_id: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
}

/// A link just minted by [`request_signin`]: `token` goes in the email.
#[derive(Debug, Clone)]
pub struct IssuedSignin {
    pub email: String,
    pub token: String,
    pub expires_at_ms: i64,
}

/// What [`complete_signin`] returns: the user, whether this link created
/// them, and the session token (shown once) with its expiry.
#[derive(Debug, Clone, Serialize)]
pub struct SignedIn {
    pub user: User,
    pub created: bool,
    pub session_token: String,
    pub expires_at_ms: i64,
}

/// One organization a user belongs to, with their role in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UserOrg {
    #[serde(flatten)]
    pub org: Organization,
    pub role: Role,
}

fn new_token(prefix: &str) -> String {
    // Two v4 UUIDs: 244 random bits.
    format!(
        "{prefix}{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

fn invalid_link() -> OrgError {
    OrgError::Unauthorized("this sign-in link is invalid, used or expired".into())
}

/// Mint a sign-in link for `email`. Anyone may ask; the link only works
/// for whoever reads that inbox. `display_name` is kept for the user the
/// link creates if the address is new, and ignored otherwise.
///
/// Refuses with `RateLimited` if a link went to this address in the last
/// [`SIGNIN_RESEND_MS`].
pub fn request_signin(
    store: &dyn OrgStore,
    email: &str,
    display_name: &str,
) -> Result<IssuedSignin, OrgError> {
    let email = normalize_email(email)?;
    crate::orgs::check_text("display_name", display_name, MAX_NAME_LEN, false)?;
    run(store, |tx| {
        let now = now_ms();
        if let Some(last) = tx.last_signin_token_ms(&email)? {
            if now - last < SIGNIN_RESEND_MS {
                return Err(OrgError::RateLimited(
                    "a sign-in link was just sent to this address; try again in a minute".into(),
                ));
            }
        }
        let token = new_token(SIGNIN_PREFIX);
        let expires_at_ms = now + SIGNIN_TOKEN_TTL_MS;
        tx.insert_signin_token(&SigninToken {
            token_hash: hash_api_key(&token),
            email: email.clone(),
            display_name: display_name.trim().to_string(),
            created_at_ms: now,
            expires_at_ms,
        })?;
        Ok(IssuedSignin {
            email: email.clone(),
            token,
            expires_at_ms,
        })
    })
}

/// Redeem a sign-in link: verify its email, create the user the first
/// time, and open a session. The link cannot be used again.
pub fn complete_signin(store: &dyn OrgStore, token: &str) -> Result<SignedIn, OrgError> {
    let token = token.trim();
    if !token.starts_with(SIGNIN_PREFIX) {
        return Err(invalid_link());
    }
    let token_hash = hash_api_key(token);
    run(store, |tx| {
        let now = now_ms();
        let link = tx
            .take_signin_token(&token_hash, now)?
            .ok_or_else(invalid_link)?;
        let (user, created) = match tx.user_by_email(&link.email)? {
            Some(user) => (user, false),
            None => {
                let user = User {
                    id: new_id("usr"),
                    email: link.email.clone(),
                    display_name: link.display_name.clone(),
                    created_at_ms: now,
                };
                tx.insert_user(&user)?;
                (user, true)
            }
        };
        let actor = Actor::User(user.id.clone());
        if created {
            audit(
                tx,
                &actor,
                None,
                "user.signed_up",
                "user",
                &user.id,
                serde_json::json!({ "email": user.email }),
            )?;
        }
        if tx.mark_email_verified(&user.id, now)? {
            audit(
                tx,
                &actor,
                None,
                "user.email_verified",
                "user",
                &user.id,
                serde_json::json!({ "email": user.email }),
            )?;
        }
        let session_token = new_token(SESSION_PREFIX);
        let expires_at_ms = now + SESSION_TTL_MS;
        tx.insert_session(&UserSession {
            token_hash: hash_api_key(&session_token),
            user_id: user.id.clone(),
            created_at_ms: now,
            expires_at_ms,
        })?;
        audit(
            tx,
            &actor,
            None,
            "user.signed_in",
            "user",
            &user.id,
            serde_json::json!({}),
        )?;
        Ok(SignedIn {
            user,
            created,
            session_token,
            expires_at_ms,
        })
    })
}

/// The user a live session token belongs to, or `Unauthorized`.
pub fn session_user(store: &dyn OrgStore, session_token: &str) -> Result<User, OrgError> {
    let unauthorized = || OrgError::Unauthorized("sign in again: the session is not valid".into());
    let token = session_token.trim();
    if !token.starts_with(SESSION_PREFIX) {
        return Err(unauthorized());
    }
    let token_hash = hash_api_key(token);
    run(store, |tx| {
        let user_id = tx
            .session_user(&token_hash, now_ms())?
            .ok_or_else(unauthorized)?;
        tx.user(&user_id)?.ok_or_else(unauthorized)
    })
}

/// Revoke a session. Signing out of a session that is already gone is
/// not an error.
pub fn sign_out(store: &dyn OrgStore, session_token: &str) -> Result<(), OrgError> {
    let token_hash = hash_api_key(session_token.trim());
    run(store, |tx| tx.revoke_session(&token_hash, now_ms()))
}

/// The organizations `user_id` belongs to, by slug, with their role.
pub fn list_user_orgs(store: &dyn OrgStore, user_id: &str) -> Result<Vec<UserOrg>, OrgError> {
    run(store, |tx| user_orgs(tx, user_id))
}

fn user_orgs(tx: &mut dyn OrgTx, user_id: &str) -> Result<Vec<UserOrg>, OrgError> {
    Ok(tx
        .user_orgs(user_id)?
        .into_iter()
        .map(|(org, role)| UserOrg { org, role })
        .collect())
}
