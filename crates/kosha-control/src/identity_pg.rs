//! Postgres storage for the identity model (#345 part 2): the
//! [`IdentityTx`] primitives over the tables of migration 008, on the same
//! transaction type as the organization primitives (`orgs_pg`). The rules
//! are in `identity`; nothing here decides anything.

use crate::api_keys::Scopes;
use crate::identity::{
    Credentials, EmailToken, IdentityStore, IdentityTx, LoginFlow, NewOrgKey, OrgApiKey, Session,
    SsoConfig, TokenKind,
};
use crate::orgs::{OrgError, Organization, Role, User, MAX_LIST};
use crate::orgs_pg::{insert_error, parse_role, seconds, store_error, PgTx};
use crate::PgStore;

/// `created_at`-style columns as Unix milliseconds.
fn ms(column: &str) -> String {
    format!("ROUND(EXTRACT(EPOCH FROM {column}) * 1000)::BIGINT")
}

impl IdentityStore for PgStore {
    fn transact_identity(
        &self,
        f: &mut dyn FnMut(&mut dyn IdentityTx) -> Result<(), OrgError>,
    ) -> Result<(), OrgError> {
        let (pool, rt) = self.pool_and_runtime();
        let _context = rt.enter();
        let tx = rt.block_on(pool.begin()).map_err(store_error)?;
        let mut pg = PgTx { tx, rt };
        match f(&mut pg) {
            Ok(()) => rt.block_on(pg.tx.commit()).map_err(store_error),
            Err(e) => {
                let _ = rt.block_on(pg.tx.rollback());
                Err(e)
            }
        }
    }
}

type TokenRow = (
    String,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    String,
    i64,
    bool,
);

fn token(row: TokenRow) -> Result<EmailToken, OrgError> {
    let (kind, user_id, email, org_id, role, created_by, expires_at_ms, used) = row;
    Ok(EmailToken {
        kind: TokenKind::parse(&kind)
            .ok_or_else(|| OrgError::Store(format!("unknown token kind '{kind}'")))?,
        user_id,
        email,
        org_id,
        role: role.as_deref().map(parse_role).transpose()?,
        created_by,
        expires_at_ms,
        used,
    })
}

type SessionRow = (String, String, i64, i64, i64, bool);

fn session(row: SessionRow) -> Session {
    let (user_id, auth_method, created_at_ms, last_seen_at_ms, expires_at_ms, revoked) = row;
    Session {
        user_id,
        auth_method,
        created_at_ms,
        last_seen_at_ms,
        expires_at_ms,
        revoked,
    }
}

fn split_domains(column: &str) -> Vec<String> {
    column
        .split(',')
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .map(str::to_string)
        .collect()
}

impl IdentityTx for PgTx<'_> {
    fn user_by_email(&mut self, email: &str) -> Result<Option<User>, OrgError> {
        let sql = format!(
            "SELECT id, email, display_name, {} FROM kosha.users WHERE email = $1",
            ms("created_at")
        );
        let row: Option<(String, String, String, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(&sql)
                    .bind(email)
                    .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(|(id, email, display_name, created_at_ms)| User {
            id,
            email,
            display_name,
            created_at_ms,
        }))
    }

    fn credentials(&mut self, user_id: &str) -> Result<Option<Credentials>, OrgError> {
        let row: Option<(Option<String>, bool)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT password_hash, email_verified_at IS NOT NULL \
                     FROM kosha.user_credentials WHERE user_id = $1 FOR UPDATE",
                )
                .bind(user_id)
                .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(|(password_hash, email_verified)| Credentials {
            user_id: user_id.to_string(),
            password_hash,
            email_verified,
        }))
    }

    fn put_credentials(&mut self, credentials: &Credentials) -> Result<(), OrgError> {
        // A verified address keeps its first verification time.
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.user_credentials \
                         (user_id, password_hash, email_verified_at, updated_at) \
                     VALUES ($1, $2, CASE WHEN $3 THEN NOW() END, NOW()) \
                     ON CONFLICT (user_id) DO UPDATE SET \
                         password_hash = EXCLUDED.password_hash, \
                         email_verified_at = CASE WHEN $3 \
                             THEN COALESCE(kosha.user_credentials.email_verified_at, NOW()) \
                             END, \
                         updated_at = NOW()",
                )
                .bind(&credentials.user_id)
                .bind(&credentials.password_hash)
                .bind(credentials.email_verified)
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn insert_token(&mut self, token_hash: &str, token: &EmailToken) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.auth_tokens \
                         (token_hash, kind, user_id, email, org_id, role, created_by, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8))",
                )
                .bind(token_hash)
                .bind(token.kind.as_str())
                .bind(&token.user_id)
                .bind(&token.email)
                .bind(&token.org_id)
                .bind(token.role.map(Role::as_str))
                .bind(&token.created_by)
                .bind(seconds(token.expires_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(|e| insert_error(e, "token collision"))
    }

    fn take_token(&mut self, token_hash: &str) -> Result<Option<EmailToken>, OrgError> {
        // One statement: two concurrent uses cannot both see it unused.
        let sql = format!(
            "UPDATE kosha.auth_tokens SET used_at = NOW() \
             WHERE token_hash = $1 AND used_at IS NULL \
             RETURNING kind, user_id, email, org_id, role, created_by, {}, FALSE",
            ms("expires_at")
        );
        let row: Option<TokenRow> = self
            .rt
            .block_on(
                sqlx::query_as(&sql)
                    .bind(token_hash)
                    .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        row.map(token).transpose()
    }

    fn retire_tokens(&mut self, user_id: &str, kind: TokenKind) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "UPDATE kosha.auth_tokens SET used_at = NOW() \
                     WHERE user_id = $1 AND kind = $2 AND used_at IS NULL",
                )
                .bind(user_id)
                .bind(kind.as_str())
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn insert_session(&mut self, token_hash: &str, s: &Session) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.sessions \
                         (token_hash, user_id, auth_method, created_at, last_seen_at, expires_at) \
                     VALUES ($1, $2, $3, to_timestamp($4), to_timestamp($5), to_timestamp($6))",
                )
                .bind(token_hash)
                .bind(&s.user_id)
                .bind(&s.auth_method)
                .bind(seconds(s.created_at_ms))
                .bind(seconds(s.last_seen_at_ms))
                .bind(seconds(s.expires_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(|e| insert_error(e, "session collision"))
    }

    fn session(&mut self, token_hash: &str) -> Result<Option<Session>, OrgError> {
        let sql = format!(
            "SELECT user_id, auth_method, {}, {}, {}, revoked_at IS NOT NULL \
             FROM kosha.sessions WHERE token_hash = $1",
            ms("created_at"),
            ms("last_seen_at"),
            ms("expires_at")
        );
        let row: Option<SessionRow> = self
            .rt
            .block_on(
                sqlx::query_as(&sql)
                    .bind(token_hash)
                    .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(session))
    }

    fn touch_session(&mut self, token_hash: &str, now_ms: i64) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "UPDATE kosha.sessions SET last_seen_at = to_timestamp($2) \
                     WHERE token_hash = $1",
                )
                .bind(token_hash)
                .bind(seconds(now_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn revoke_session(&mut self, token_hash: &str) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "UPDATE kosha.sessions SET revoked_at = NOW() \
                     WHERE token_hash = $1 AND revoked_at IS NULL",
                )
                .bind(token_hash)
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn revoke_user_sessions(&mut self, user_id: &str) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "UPDATE kosha.sessions SET revoked_at = NOW() \
                     WHERE user_id = $1 AND revoked_at IS NULL",
                )
                .bind(user_id)
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn identity_user(&mut self, provider: &str, subject: &str) -> Result<Option<String>, OrgError> {
        let row: Option<(String,)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT user_id FROM kosha.user_identities \
                     WHERE provider = $1 AND subject = $2",
                )
                .bind(provider)
                .bind(subject)
                .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(|(user_id,)| user_id))
    }

    fn insert_identity(
        &mut self,
        provider: &str,
        subject: &str,
        user_id: &str,
        email: &str,
    ) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.user_identities (provider, subject, user_id, email) \
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(provider)
                .bind(subject)
                .bind(user_id)
                .bind(email)
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(|e| insert_error(e, "identity already linked"))
    }

    fn memberships(&mut self, user_id: &str) -> Result<Vec<(Organization, Role)>, OrgError> {
        let sql = format!(
            "SELECT o.id, o.slug, o.name, {}, m.role \
             FROM kosha.org_members m JOIN kosha.organizations o ON o.id = m.org_id \
             WHERE m.user_id = $1 ORDER BY o.slug LIMIT $2",
            ms("o.created_at")
        );
        let rows: Vec<(String, String, String, i64, String)> = self
            .rt
            .block_on(
                sqlx::query_as(&sql)
                    .bind(user_id)
                    .bind(i64::from(MAX_LIST))
                    .fetch_all(&mut *self.tx),
            )
            .map_err(store_error)?;
        rows.into_iter()
            .map(|(id, slug, name, created_at_ms, role)| {
                Ok((
                    Organization {
                        id,
                        slug,
                        name,
                        created_at_ms,
                    },
                    parse_role(&role)?,
                ))
            })
            .collect()
    }

    fn org_by_slug(&mut self, slug: &str) -> Result<Option<Organization>, OrgError> {
        let sql = format!(
            "SELECT id, slug, name, {} FROM kosha.organizations WHERE slug = $1",
            ms("created_at")
        );
        let row: Option<(String, String, String, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(&sql)
                    .bind(slug)
                    .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(|(id, slug, name, created_at_ms)| Organization {
            id,
            slug,
            name,
            created_at_ms,
        }))
    }

    fn sso_config(&mut self, org_id: &str) -> Result<Option<SsoConfig>, OrgError> {
        let sql = format!(
            "SELECT issuer, client_id, client_secret, allowed_domains, sso_required, {} \
             FROM kosha.org_sso WHERE org_id = $1",
            ms("updated_at")
        );
        let row: Option<(String, String, String, String, bool, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(&sql)
                    .bind(org_id)
                    .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(
            |(issuer, client_id, secret, domains, sso_required, updated_at_ms)| SsoConfig {
                org_id: org_id.to_string(),
                issuer,
                client_id,
                client_secret_sealed: secret,
                allowed_domains: split_domains(&domains),
                sso_required,
                updated_at_ms,
            },
        ))
    }

    fn put_sso_config(&mut self, c: &SsoConfig) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.org_sso (org_id, issuer, client_id, client_secret, \
                         allowed_domains, sso_required, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, NOW()) \
                     ON CONFLICT (org_id) DO UPDATE SET issuer = EXCLUDED.issuer, \
                         client_id = EXCLUDED.client_id, \
                         client_secret = EXCLUDED.client_secret, \
                         allowed_domains = EXCLUDED.allowed_domains, \
                         sso_required = EXCLUDED.sso_required, updated_at = NOW()",
                )
                .bind(&c.org_id)
                .bind(&c.issuer)
                .bind(&c.client_id)
                .bind(&c.client_secret_sealed)
                .bind(c.allowed_domains.join(","))
                .bind(c.sso_required)
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn delete_sso_config(&mut self, org_id: &str) -> Result<bool, OrgError> {
        self.rt
            .block_on(
                sqlx::query("DELETE FROM kosha.org_sso WHERE org_id = $1")
                    .bind(org_id)
                    .execute(&mut *self.tx),
            )
            .map(|r| r.rows_affected() > 0)
            .map_err(store_error)
    }

    fn insert_login_flow(&mut self, state_hash: &str, flow: &LoginFlow) -> Result<(), OrgError> {
        // Abandoned sign-ins are cleared here, so the table stays small.
        self.rt
            .block_on(
                sqlx::query("DELETE FROM kosha.login_flows WHERE expires_at < NOW()")
                    .execute(&mut *self.tx),
            )
            .map_err(store_error)?;
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.login_flows \
                         (state_hash, provider, org_id, pkce_verifier, nonce, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, to_timestamp($6))",
                )
                .bind(state_hash)
                .bind(&flow.provider)
                .bind(&flow.org_id)
                .bind(&flow.pkce_verifier)
                .bind(&flow.nonce)
                .bind(seconds(flow.expires_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(|e| insert_error(e, "state collision"))
    }

    fn take_login_flow(&mut self, state_hash: &str) -> Result<Option<LoginFlow>, OrgError> {
        let sql = format!(
            "DELETE FROM kosha.login_flows WHERE state_hash = $1 \
             RETURNING provider, org_id, pkce_verifier, nonce, {}",
            ms("expires_at")
        );
        let row: Option<(String, Option<String>, String, String, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(&sql)
                    .bind(state_hash)
                    .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(
            |(provider, org_id, pkce_verifier, nonce, expires_at_ms)| LoginFlow {
                provider,
                org_id,
                pkce_verifier,
                nonce,
                expires_at_ms,
            },
        ))
    }

    fn insert_org_api_key(&mut self, key: &NewOrgKey) -> Result<(), OrgError> {
        // Hashed at rest like every key since #346.
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.api_keys \
                         (api_key, key_hash, tenant_id, description, scopes, expires_at) \
                     VALUES ($1, $2, $3, $4, $5, to_timestamp($6::BIGINT::DOUBLE PRECISION))",
                )
                .bind(crate::api_key_store::placeholder(&key.key_hash))
                .bind(&key.key_hash)
                .bind(&key.tenant_id)
                .bind(&key.description)
                .bind(key.scopes.to_column())
                .bind(key.expires_at)
                .execute(&mut *self.tx),
            )
            .map_err(store_error)?;
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.api_key_creators (key_hash, org_id, tenant_id, user_id) \
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(&key.key_hash)
                .bind(&key.org_id)
                .bind(&key.tenant_id)
                .bind(&key.created_by)
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn org_api_keys(&mut self, org_id: &str) -> Result<Vec<OrgApiKey>, OrgError> {
        type Row = (
            String,
            String,
            String,
            String,
            i64,
            Option<i64>,
            Option<i64>,
            Option<String>,
        );
        let rows: Vec<Row> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT k.key_hash, k.tenant_id, k.description, k.scopes, \
                            EXTRACT(EPOCH FROM k.created_at)::BIGINT, \
                            EXTRACT(EPOCH FROM k.expires_at)::BIGINT, \
                            EXTRACT(EPOCH FROM k.last_used_at)::BIGINT, \
                            c.user_id \
                     FROM kosha.api_keys k \
                     JOIN kosha.tenants t ON t.tenant_id = k.tenant_id \
                     LEFT JOIN kosha.api_key_creators c ON c.key_hash = k.key_hash \
                     WHERE t.org_id = $1 AND k.revoked_at IS NULL AND k.key_hash IS NOT NULL \
                     ORDER BY k.created_at DESC LIMIT $2",
                )
                .bind(org_id)
                .bind(i64::from(MAX_LIST))
                .fetch_all(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    key_id,
                    tenant_id,
                    description,
                    scopes,
                    created_at,
                    expires_at,
                    last_used_at,
                    created_by,
                )| {
                    OrgApiKey {
                        key_id,
                        tenant_id,
                        description,
                        scopes: Scopes::from_column(&scopes)
                            .names()
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                        created_at,
                        expires_at,
                        last_used_at,
                        created_by,
                    }
                },
            )
            .collect())
    }

    fn revoke_api_key(&mut self, key_hash: &str) -> Result<bool, OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "UPDATE kosha.api_keys SET revoked_at = NOW() \
                     WHERE key_hash = $1 AND revoked_at IS NULL",
                )
                .bind(key_hash)
                .execute(&mut *self.tx),
            )
            .map(|r| r.rows_affected() > 0)
            .map_err(store_error)
    }
}
