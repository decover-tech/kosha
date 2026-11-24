//! Postgres storage for the organization model (#345): the [`OrgTx`]
//! primitives over the tables of migrations 006 and 008, and [`OrgStore`] for
//! [`PgStore`]. The rules are in `orgs`; nothing here decides anything.

use crate::orgs::{
    ApiKey, AuditEvent, Member, NewAuditEvent, OrgError, OrgStore, OrgTx, Organization, Project,
    Role, TenantLink, User, MAX_LIST, ORG_EXISTS, PROJECT_EXISTS, TENANT_LINKED, USER_EXISTS,
};
use crate::signin::{SigninToken, UserSession};
use crate::PgStore;

fn store_error(e: sqlx::Error) -> OrgError {
    OrgError::Store(e.to_string())
}

/// A unique violation (SQLSTATE 23505) on an insert is the collision
/// `OrgTx` documents; anything else is a store failure.
fn insert_error(e: sqlx::Error, conflict: &str) -> OrgError {
    let unique_violation = e
        .as_database_error()
        .and_then(|db| db.code())
        .is_some_and(|code| code == "23505");
    if unique_violation {
        OrgError::Conflict(conflict.to_string())
    } else {
        store_error(e)
    }
}

fn parse_role(role: &str) -> Result<Role, OrgError> {
    Role::parse(role)
        .ok_or_else(|| OrgError::Store(format!("unknown role '{role}' in org_members")))
}

/// Timestamps cross the boundary as Unix milliseconds: bound as seconds for
/// `to_timestamp`, read back with `CREATED_MS`-style expressions.
fn seconds(ms: i64) -> f64 {
    ms as f64 / 1000.0
}

/// Appends one `kosha.audit_log` row. Binds, in order: `org_id`, `actor`,
/// `action`, `target_type`, `target_id`, `detail` (JSON text). Shared with
/// the operator-credential changes in `api_key_store`, which audit inside
/// their own transaction.
pub(crate) const INSERT_AUDIT: &str = "INSERT INTO kosha.audit_log \
         (org_id, actor, action, target_type, target_id, detail) \
     VALUES ($1, $2, $3, $4, $5, $6)";

struct PgTx<'a> {
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
    rt: &'a tokio::runtime::Runtime,
}

impl OrgStore for PgStore {
    fn transact(
        &self,
        f: &mut dyn FnMut(&mut dyn OrgTx) -> Result<(), OrgError>,
    ) -> Result<(), OrgError> {
        let (pool, rt) = self.pool_and_runtime();
        // A connection going back to the pool needs the runtime's context,
        // including when `f` panics and the transaction is dropped.
        let _context = rt.enter();
        let tx = rt.block_on(pool.begin()).map_err(store_error)?;
        let mut pg = PgTx { tx, rt };
        match f(&mut pg) {
            Ok(()) => rt.block_on(pg.tx.commit()).map_err(store_error),
            Err(e) => {
                // The error being returned is the one that matters; a
                // failed rollback still ends the transaction when the
                // connection is dropped.
                let _ = rt.block_on(pg.tx.rollback());
                Err(e)
            }
        }
    }
}

impl OrgTx for PgTx<'_> {
    fn user(&mut self, id: &str) -> Result<Option<User>, OrgError> {
        let row: Option<(String, String, String, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT id, email, display_name, \
                            ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
                     FROM kosha.users WHERE id = $1",
                )
                .bind(id)
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

    fn insert_user(&mut self, user: &User) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.users (id, email, display_name, created_at) \
                     VALUES ($1, $2, $3, to_timestamp($4))",
                )
                .bind(&user.id)
                .bind(&user.email)
                .bind(&user.display_name)
                .bind(seconds(user.created_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(|e| insert_error(e, USER_EXISTS))
    }

    fn org(&mut self, id: &str, lock: bool) -> Result<Option<Organization>, OrgError> {
        let query = if lock {
            "SELECT id, slug, name, ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
             FROM kosha.organizations WHERE id = $1 FOR UPDATE"
        } else {
            "SELECT id, slug, name, ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
             FROM kosha.organizations WHERE id = $1"
        };
        let row: Option<(String, String, String, i64)> = self
            .rt
            .block_on(sqlx::query_as(query).bind(id).fetch_optional(&mut *self.tx))
            .map_err(store_error)?;
        Ok(row.map(|(id, slug, name, created_at_ms)| Organization {
            id,
            slug,
            name,
            created_at_ms,
        }))
    }

    fn insert_org(&mut self, org: &Organization) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.organizations (id, slug, name, created_at) \
                     VALUES ($1, $2, $3, to_timestamp($4))",
                )
                .bind(&org.id)
                .bind(&org.slug)
                .bind(&org.name)
                .bind(seconds(org.created_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(|e| insert_error(e, ORG_EXISTS))
    }

    fn member_role(&mut self, org_id: &str, user_id: &str) -> Result<Option<Role>, OrgError> {
        let row: Option<(String,)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT role FROM kosha.org_members WHERE org_id = $1 AND user_id = $2",
                )
                .bind(org_id)
                .bind(user_id)
                .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        row.map(|(role,)| parse_role(&role)).transpose()
    }

    fn owner_count(&mut self, org_id: &str) -> Result<u64, OrgError> {
        let (count,): (i64,) = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT COUNT(*) FROM kosha.org_members \
                     WHERE org_id = $1 AND role = 'owner'",
                )
                .bind(org_id)
                .fetch_one(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(count.max(0) as u64)
    }

    fn put_member(
        &mut self,
        org_id: &str,
        user_id: &str,
        role: Role,
        created_at_ms: i64,
    ) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.org_members (org_id, user_id, role, created_at) \
                     VALUES ($1, $2, $3, to_timestamp($4)) \
                     ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role",
                )
                .bind(org_id)
                .bind(user_id)
                .bind(role.as_str())
                .bind(seconds(created_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn remove_member(&mut self, org_id: &str, user_id: &str) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query("DELETE FROM kosha.org_members WHERE org_id = $1 AND user_id = $2")
                    .bind(org_id)
                    .bind(user_id)
                    .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn members(&mut self, org_id: &str) -> Result<Vec<Member>, OrgError> {
        let rows: Vec<(String, String, String, String, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT m.user_id, u.email, u.display_name, m.role, \
                            ROUND(EXTRACT(EPOCH FROM m.created_at) * 1000)::BIGINT \
                     FROM kosha.org_members m JOIN kosha.users u ON u.id = m.user_id \
                     WHERE m.org_id = $1 \
                     ORDER BY u.email LIMIT $2",
                )
                .bind(org_id)
                .bind(i64::from(MAX_LIST))
                .fetch_all(&mut *self.tx),
            )
            .map_err(store_error)?;
        rows.into_iter()
            .map(|(user_id, email, display_name, role, created_at_ms)| {
                Ok(Member {
                    user_id,
                    email,
                    display_name,
                    role: parse_role(&role)?,
                    created_at_ms,
                })
            })
            .collect()
    }

    fn project(&mut self, id: &str) -> Result<Option<Project>, OrgError> {
        let row: Option<(String, String, String, String, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT id, org_id, slug, name, \
                            ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
                     FROM kosha.projects WHERE id = $1",
                )
                .bind(id)
                .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(|(id, org_id, slug, name, created_at_ms)| Project {
            id,
            org_id,
            slug,
            name,
            created_at_ms,
        }))
    }

    fn insert_project(&mut self, project: &Project) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.projects (id, org_id, slug, name, created_at) \
                     VALUES ($1, $2, $3, $4, to_timestamp($5))",
                )
                .bind(&project.id)
                .bind(&project.org_id)
                .bind(&project.slug)
                .bind(&project.name)
                .bind(seconds(project.created_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(|e| insert_error(e, PROJECT_EXISTS))
    }

    fn projects(&mut self, org_id: &str) -> Result<Vec<Project>, OrgError> {
        let rows: Vec<(String, String, String, String, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT id, org_id, slug, name, \
                            ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
                     FROM kosha.projects WHERE org_id = $1 \
                     ORDER BY slug LIMIT $2",
                )
                .bind(org_id)
                .bind(i64::from(MAX_LIST))
                .fetch_all(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(rows
            .into_iter()
            .map(|(id, org_id, slug, name, created_at_ms)| Project {
                id,
                org_id,
                slug,
                name,
                created_at_ms,
            })
            .collect())
    }

    fn tenant(&mut self, tenant_id: &str) -> Result<Option<TenantLink>, OrgError> {
        let row: Option<(String, String, Option<String>, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT tenant_id, org_id, project_id, \
                            ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
                     FROM kosha.tenants WHERE tenant_id = $1",
                )
                .bind(tenant_id)
                .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(
            |(tenant_id, org_id, project_id, created_at_ms)| TenantLink {
                tenant_id,
                org_id,
                project_id,
                created_at_ms,
            },
        ))
    }

    fn insert_tenant(&mut self, tenant: &TenantLink) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.tenants (tenant_id, org_id, project_id, created_at) \
                     VALUES ($1, $2, $3, to_timestamp($4))",
                )
                .bind(&tenant.tenant_id)
                .bind(&tenant.org_id)
                .bind(&tenant.project_id)
                .bind(seconds(tenant.created_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(|e| insert_error(e, TENANT_LINKED))
    }

    fn tenants(&mut self, org_id: &str) -> Result<Vec<TenantLink>, OrgError> {
        let rows: Vec<(String, String, Option<String>, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT tenant_id, org_id, project_id, \
                            ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
                     FROM kosha.tenants WHERE org_id = $1 \
                     ORDER BY tenant_id LIMIT $2",
                )
                .bind(org_id)
                .bind(i64::from(MAX_LIST))
                .fetch_all(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(rows
            .into_iter()
            .map(
                |(tenant_id, org_id, project_id, created_at_ms)| TenantLink {
                    tenant_id,
                    org_id,
                    project_id,
                    created_at_ms,
                },
            )
            .collect())
    }

    fn insert_api_key(&mut self, key: &ApiKey) -> Result<(), OrgError> {
        // Hashed at rest like every key since #346: the row holds the hash and
        // a placeholder, never the key itself.
        let key_hash = crate::api_keys::hash_api_key(&key.api_key);
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.api_keys (api_key, key_hash, tenant_id, description) \
                     VALUES ($1, $2, $3, $4)",
                )
                .bind(crate::api_key_store::placeholder(&key_hash))
                .bind(&key_hash)
                .bind(&key.tenant_id)
                .bind(&key.description)
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn insert_audit(&mut self, event: &NewAuditEvent) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(INSERT_AUDIT)
                    .bind(&event.org_id)
                    .bind(&event.actor)
                    .bind(event.action)
                    .bind(event.target_type)
                    .bind(&event.target_id)
                    .bind(event.detail.to_string())
                    .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn audit(
        &mut self,
        org_id: &str,
        limit: u32,
        before: Option<i64>,
    ) -> Result<Vec<AuditEvent>, OrgError> {
        type Row = (
            i64,
            Option<String>,
            String,
            String,
            String,
            String,
            String,
            i64,
        );
        let rows: Vec<Row> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT id, org_id, actor, action, target_type, target_id, detail, \
                            ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
                     FROM kosha.audit_log \
                     WHERE org_id = $1 AND ($2::BIGINT IS NULL OR id < $2) \
                     ORDER BY id DESC LIMIT $3",
                )
                .bind(org_id)
                .bind(before)
                .bind(i64::from(limit))
                .fetch_all(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(rows
            .into_iter()
            .map(
                |(id, org_id, actor, action, target_type, target_id, detail, created_at_ms)| {
                    AuditEvent {
                        id,
                        org_id,
                        actor,
                        action,
                        target_type,
                        target_id,
                        // Written by `insert_audit` as JSON; a row edited by
                        // hand into something else is shown as its text.
                        detail: serde_json::from_str(&detail)
                            .unwrap_or(serde_json::Value::String(detail)),
                        created_at_ms,
                    }
                },
            )
            .collect())
    }

    fn user_by_email(&mut self, email: &str) -> Result<Option<User>, OrgError> {
        let row: Option<(String, String, String, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT id, email, display_name, \
                            ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
                     FROM kosha.users WHERE email = $1",
                )
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

    fn mark_email_verified(&mut self, user_id: &str, at_ms: i64) -> Result<bool, OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.email_verifications (user_id, verified_at) \
                     VALUES ($1, to_timestamp($2)) ON CONFLICT (user_id) DO NOTHING",
                )
                .bind(user_id)
                .bind(seconds(at_ms))
                .execute(&mut *self.tx),
            )
            .map(|r| r.rows_affected() > 0)
            .map_err(store_error)
    }

    fn user_orgs(&mut self, user_id: &str) -> Result<Vec<(Organization, Role)>, OrgError> {
        let rows: Vec<(String, String, String, i64, String)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "SELECT o.id, o.slug, o.name, \
                            ROUND(EXTRACT(EPOCH FROM o.created_at) * 1000)::BIGINT, m.role \
                     FROM kosha.org_members m \
                     JOIN kosha.organizations o ON o.id = m.org_id \
                     WHERE m.user_id = $1 ORDER BY o.slug LIMIT $2",
                )
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

    fn insert_signin_token(&mut self, token: &SigninToken) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.signin_tokens \
                         (token_hash, email, display_name, created_at, expires_at) \
                     VALUES ($1, $2, $3, to_timestamp($4), to_timestamp($5))",
                )
                .bind(&token.token_hash)
                .bind(&token.email)
                .bind(&token.display_name)
                .bind(seconds(token.created_at_ms))
                .bind(seconds(token.expires_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn last_signin_token_ms(&mut self, email: &str) -> Result<Option<i64>, OrgError> {
        self.rt
            .block_on(
                sqlx::query_scalar(
                    "SELECT ROUND(EXTRACT(EPOCH FROM MAX(created_at)) * 1000)::BIGINT \
                     FROM kosha.signin_tokens WHERE email = $1",
                )
                .bind(email)
                .fetch_one(&mut *self.tx),
            )
            .map_err(store_error)
    }

    fn take_signin_token(
        &mut self,
        token_hash: &str,
        now_ms: i64,
    ) -> Result<Option<SigninToken>, OrgError> {
        // One UPDATE both checks and spends the link, so two redemptions
        // racing cannot both succeed.
        let row: Option<(String, String, String, i64, i64)> = self
            .rt
            .block_on(
                sqlx::query_as(
                    "UPDATE kosha.signin_tokens SET used_at = to_timestamp($2) \
                     WHERE token_hash = $1 AND used_at IS NULL \
                       AND expires_at > to_timestamp($2) \
                     RETURNING token_hash, email, display_name, \
                       ROUND(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT, \
                       ROUND(EXTRACT(EPOCH FROM expires_at) * 1000)::BIGINT",
                )
                .bind(token_hash)
                .bind(seconds(now_ms))
                .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)?;
        Ok(row.map(
            |(token_hash, email, display_name, created_at_ms, expires_at_ms)| SigninToken {
                token_hash,
                email,
                display_name,
                created_at_ms,
                expires_at_ms,
            },
        ))
    }

    fn insert_session(&mut self, session: &UserSession) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "INSERT INTO kosha.user_sessions \
                         (token_hash, user_id, created_at, expires_at) \
                     VALUES ($1, $2, to_timestamp($3), to_timestamp($4))",
                )
                .bind(&session.token_hash)
                .bind(&session.user_id)
                .bind(seconds(session.created_at_ms))
                .bind(seconds(session.expires_at_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }

    fn session_user(&mut self, token_hash: &str, now_ms: i64) -> Result<Option<String>, OrgError> {
        self.rt
            .block_on(
                sqlx::query_scalar(
                    "SELECT user_id FROM kosha.user_sessions \
                     WHERE token_hash = $1 AND revoked_at IS NULL \
                       AND expires_at > to_timestamp($2)",
                )
                .bind(token_hash)
                .bind(seconds(now_ms))
                .fetch_optional(&mut *self.tx),
            )
            .map_err(store_error)
    }

    fn revoke_session(&mut self, token_hash: &str, now_ms: i64) -> Result<(), OrgError> {
        self.rt
            .block_on(
                sqlx::query(
                    "UPDATE kosha.user_sessions SET revoked_at = to_timestamp($2) \
                     WHERE token_hash = $1 AND revoked_at IS NULL",
                )
                .bind(token_hash)
                .bind(seconds(now_ms))
                .execute(&mut *self.tx),
            )
            .map(|_| ())
            .map_err(store_error)
    }
}
