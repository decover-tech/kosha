-- Billing ledger (#349): plan changes (proration), processed Stripe webhook
-- events (idempotency), payment failures and retries, dunning notices sent,
-- payment methods on file, finished scheduled jobs and the billing
-- scheduler's lease.
--
-- Additive: new tables only, no existing table changes, nothing
-- backfilled. A binary without #349's second part ignores them. Embedded in
-- the server binary and re-run on every startup after 007 (see
-- kosha-control/src/postgres.rs), so every statement is idempotent.
-- `make db-migrate` also runs it as the RDS admin before 002 hands ownership
-- of the tables to the `kosha` role.

-- Every change of a tenant's plan, for proration. A tenant whose plan never
-- changed has no row and is rated on one plan, as before.
CREATE TABLE IF NOT EXISTS kosha.billing_plan_changes (
    id            BIGSERIAL PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    changed_at_ms BIGINT NOT NULL,
    from_plan     TEXT,                 -- NULL: the tenant had no account
    to_plan       TEXT NOT NULL,
    source        TEXT NOT NULL         -- admin | checkout
);
CREATE INDEX IF NOT EXISTS idx_billing_plan_changes_tenant
    ON kosha.billing_plan_changes (tenant_id, changed_at_ms);

-- Stripe event ids already processed: a redelivery is acknowledged and not
-- applied again.
CREATE TABLE IF NOT EXISTS kosha.billing_webhook_events (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    outcome         TEXT NOT NULL,
    processed_at_ms BIGINT NOT NULL
);

-- Failed collections of an invoice and Kosha's own retries of it.
CREATE TABLE IF NOT EXISTS kosha.billing_payment_failures (
    invoice_id         TEXT PRIMARY KEY,
    tenant_id          TEXT NOT NULL,
    failures           INTEGER NOT NULL,
    first_failed_at_ms BIGINT NOT NULL,
    last_failed_at_ms  BIGINT NOT NULL,
    retries            INTEGER NOT NULL DEFAULT 0,
    last_retry_at_ms   BIGINT,
    last_reason        TEXT
);

-- Dunning notices, claimed before sending: `<invoice id>/<kind>` goes out
-- once.
CREATE TABLE IF NOT EXISTS kosha.billing_notices (
    notice_id  TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    invoice_id TEXT,
    kind       TEXT NOT NULL,
    sent_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_billing_notices_sent
    ON kosha.billing_notices (sent_at_ms);

-- The provider's default payment method of each tenant, as last reported
-- (an id such as pm_..., never card data).
CREATE TABLE IF NOT EXISTS kosha.billing_payment_methods (
    tenant_id      TEXT PRIMARY KEY,
    payment_method TEXT,                -- NULL: none on file any more
    updated_at_ms  BIGINT NOT NULL
);

-- Scheduled jobs that completed (`close-period/2026-10`).
CREATE TABLE IF NOT EXISTS kosha.billing_jobs (
    job        TEXT PRIMARY KEY,
    done_at_ms BIGINT NOT NULL,
    detail     TEXT NOT NULL
);

-- The billing scheduler's lease: one pod runs the jobs at a time.
CREATE TABLE IF NOT EXISTS kosha.billing_leases (
    name          TEXT PRIMARY KEY,
    holder        TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL
);
