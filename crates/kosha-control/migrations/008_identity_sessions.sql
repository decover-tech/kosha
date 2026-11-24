-- Sign-up, sessions, OAuth/OIDC identities, per-organization SSO and the
-- creator of console-made API keys (#345 part 2). See kosha-control's
-- `identity` module.
--
-- Additive: no existing table is altered and no existing row is touched. A
-- user created through the admin API (006) has no `user_credentials` row
-- until they set a password with a reset link or sign in with a provider.
--
-- Secrets are never stored in a usable form: passwords as argon2id PHC
-- strings, every token (session, email verification, password reset,
-- invite, OAuth state) as the lowercase hex SHA-256 of the token, and an
-- organization's OIDC client secret sealed with the server's
-- KOSHA_CONSOLE_SECRET_KEY (AES-256-GCM).
--
-- Embedded in the server binary and re-run on every startup after 007
-- (see kosha-control/src/postgres.rs), so every statement is idempotent.
-- `make db-migrate` also runs it as the RDS admin before 002 hands
-- ownership of the tables to the `kosha` role.

-- A user's password and whether their email address is proven.
CREATE TABLE IF NOT EXISTS kosha.user_credentials (
    user_id           TEXT PRIMARY KEY REFERENCES kosha.users(id),
    password_hash     TEXT,                 -- argon2id PHC string; NULL = no password
    email_verified_at TIMESTAMPTZ,          -- NULL = not verified yet
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Single-use, expiring tokens sent by email.
CREATE TABLE IF NOT EXISTS kosha.auth_tokens (
    token_hash TEXT PRIMARY KEY,            -- SHA-256 hex of the token
    kind       TEXT NOT NULL CHECK (kind IN ('verify_email', 'reset_password', 'invite')),
    user_id    TEXT,                        -- verify_email, reset_password
    email      TEXT NOT NULL,               -- the address it was sent to
    org_id     TEXT,                        -- invite
    role       TEXT,                        -- invite
    created_by TEXT NOT NULL DEFAULT '',    -- audit label of who made it
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_auth_tokens_user
    ON kosha.auth_tokens (user_id);

-- Console sessions. Only ever authenticate /v1/console routes, never the
-- data plane.
CREATE TABLE IF NOT EXISTS kosha.sessions (
    token_hash   TEXT PRIMARY KEY,          -- SHA-256 hex of the cookie value
    user_id      TEXT NOT NULL REFERENCES kosha.users(id),
    auth_method  TEXT NOT NULL,             -- password | google | github | sso:<org id>
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,      -- absolute lifetime
    revoked_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sessions_user
    ON kosha.sessions (user_id);

-- An external account (Google, GitHub, an organization's OIDC provider)
-- linked to a user.
CREATE TABLE IF NOT EXISTS kosha.user_identities (
    provider   TEXT NOT NULL,               -- google | github | sso:<org id>
    subject    TEXT NOT NULL,               -- the provider's stable user id
    user_id    TEXT NOT NULL REFERENCES kosha.users(id),
    email      TEXT NOT NULL,               -- verified address at link time
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, subject)
);

CREATE INDEX IF NOT EXISTS idx_user_identities_user
    ON kosha.user_identities (user_id);

-- One organization's OIDC single sign-on.
CREATE TABLE IF NOT EXISTS kosha.org_sso (
    org_id          TEXT PRIMARY KEY REFERENCES kosha.organizations(id),
    issuer          TEXT NOT NULL,
    client_id       TEXT NOT NULL,
    client_secret   TEXT NOT NULL,          -- sealed: "v1:<nonce>:<ciphertext>" (base64)
    allowed_domains TEXT NOT NULL DEFAULT '', -- comma-separated, lowercase
    sso_required    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- An OAuth/OIDC sign-in in progress (between the redirect to the provider
-- and its callback). Single use; expires after minutes.
CREATE TABLE IF NOT EXISTS kosha.login_flows (
    state_hash    TEXT PRIMARY KEY,         -- SHA-256 hex of the `state` parameter
    provider      TEXT NOT NULL,
    org_id        TEXT,                     -- set for an organization's SSO
    connect_user_id TEXT,                   -- SSO "connect" started by this signed-in user
    pkce_verifier TEXT NOT NULL,
    nonce         TEXT NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- For a database that ran an earlier draft of this file.
ALTER TABLE kosha.login_flows ADD COLUMN IF NOT EXISTS connect_user_id TEXT;

CREATE INDEX IF NOT EXISTS idx_sessions_auth_method
    ON kosha.sessions (auth_method) WHERE revoked_at IS NULL;

-- Who created a key through the console, so removing a member can revoke
-- the keys they made. No foreign key: the key row may be deleted with its
-- tenant (#347) and this row is then ignored.
CREATE TABLE IF NOT EXISTS kosha.api_key_creators (
    key_hash   TEXT PRIMARY KEY,            -- kosha.api_keys.key_hash
    org_id     TEXT NOT NULL,
    tenant_id  TEXT NOT NULL,
    user_id    TEXT,                        -- NULL = the operator
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_key_creators_org_user
    ON kosha.api_key_creators (org_id, user_id);
