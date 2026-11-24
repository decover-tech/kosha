-- Self-serve sign-in for the console (#345 part 2): emailed one-time
-- sign-in links, the sessions they open, and when each user's email was
-- verified. See kosha-control's `signin` module.
--
-- Additive and idempotent, like 006: re-run on every startup.

-- A table, not a column on `users`: an ALTER TABLE on every startup takes
-- an exclusive lock that deadlocks with a concurrent first boot.
CREATE TABLE IF NOT EXISTS kosha.email_verifications (
    user_id     TEXT PRIMARY KEY REFERENCES kosha.users(id),
    verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per emailed link. Only the SHA-256 of the token is stored. A
-- link is single-use (`used_at`) and short-lived (`expires_at`).
CREATE TABLE IF NOT EXISTS kosha.signin_tokens (
    token_hash   TEXT PRIMARY KEY,
    email        TEXT NOT NULL,              -- trimmed and lowercased
    display_name TEXT NOT NULL DEFAULT '',   -- used if the link creates the user
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at   TIMESTAMPTZ NOT NULL,
    used_at      TIMESTAMPTZ
);

-- "When was the last link sent to this address": the resend throttle.
CREATE INDEX IF NOT EXISTS idx_signin_tokens_email
    ON kosha.signin_tokens (email, created_at DESC);

-- One row per console session. Only the SHA-256 of the token is stored.
CREATE TABLE IF NOT EXISTS kosha.user_sessions (
    token_hash TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES kosha.users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_user
    ON kosha.user_sessions (user_id);
