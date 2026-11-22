-- Organizations, users, memberships, projects, tenant links and the
-- control-plane audit log (#345). See kosha-control's `orgs` module.
--
-- Additive: no existing table is altered and no existing row is touched.
-- `kosha.api_keys` is deliberately left without a foreign key to
-- `kosha.tenants`: a tenant with no row here (every tenant created before
-- this migration) keeps working, and request authentication never reads
-- these tables.
--
-- Embedded in the server binary and re-run on every startup after 001/003
-- (see kosha-control/src/postgres.rs), so every statement is idempotent.
-- `make db-migrate` also runs it as the RDS admin before 002 hands
-- ownership of the tables to the `kosha` role.

CREATE TABLE IF NOT EXISTS kosha.users (
    id           TEXT PRIMARY KEY,          -- "usr_<uuid>"
    email        TEXT NOT NULL UNIQUE,      -- stored trimmed and lowercased
    display_name TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kosha.organizations (
    id         TEXT PRIMARY KEY,            -- "org_<uuid>"
    slug       TEXT NOT NULL UNIQUE,        -- a-z, 0-9, '-'; prefixes generated tenant ids
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per (organization, user). Every organization keeps at least one
-- 'owner' row; the application enforces that under the organization's row
-- lock.
CREATE TABLE IF NOT EXISTS kosha.org_members (
    org_id     TEXT NOT NULL REFERENCES kosha.organizations(id),
    user_id    TEXT NOT NULL REFERENCES kosha.users(id),
    role       TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'member', 'read-only')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_id, user_id)
);

-- "Which organizations is this user in".
CREATE INDEX IF NOT EXISTS idx_org_members_user
    ON kosha.org_members (user_id);

CREATE TABLE IF NOT EXISTS kosha.projects (
    id         TEXT PRIMARY KEY,            -- "prj_<uuid>"
    org_id     TEXT NOT NULL REFERENCES kosha.organizations(id),
    slug       TEXT NOT NULL,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (org_id, slug)
);

-- The link from a tenant id (`kosha.api_keys.tenant_id`, the `{tenant}/...`
-- namespace prefix) to the organization, and optionally the project, that
-- owns it. A tenant belongs to at most one organization.
CREATE TABLE IF NOT EXISTS kosha.tenants (
    tenant_id  TEXT PRIMARY KEY,
    org_id     TEXT NOT NULL REFERENCES kosha.organizations(id),
    project_id TEXT REFERENCES kosha.projects(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tenants_org
    ON kosha.tenants (org_id);

-- Append-only log of control-plane actions. Written in the same
-- transaction as the action it records. `org_id` is NULL for actions
-- outside any organization and has no foreign key, so the log outlives
-- whatever it describes. `detail` is a JSON object; it never holds an API
-- key, only its last four characters.
CREATE TABLE IF NOT EXISTS kosha.audit_log (
    id          BIGSERIAL PRIMARY KEY,
    org_id      TEXT,
    actor       TEXT NOT NULL,              -- "operator" or "user:<user id>"
    action      TEXT NOT NULL,              -- e.g. "member.added", "api_key.created"
    target_type TEXT NOT NULL,
    target_id   TEXT NOT NULL,
    detail      TEXT NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One organization's log, newest first.
CREATE INDEX IF NOT EXISTS idx_audit_log_org
    ON kosha.audit_log (org_id, id DESC);
