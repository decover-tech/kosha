-- Billing accounts and invoices (#349).
--
-- Additive: two new tables, no existing table changes, nothing backfilled.
-- A tenant without a row in billing_accounts behaves exactly as before
-- (it is on the catalog's default plan, if one is configured). Embedded in
-- the server binary and re-run on every startup after 001, 003 and 005 (see
-- kosha-control/src/postgres.rs), so every statement is idempotent.
-- `make db-migrate` also runs it as the RDS admin before 002 hands ownership
-- of the tables to the `kosha` role.

-- Which plan a tenant is on. Plans themselves (prices, included amounts,
-- quota defaults) are configuration, not rows: `plan` names an entry of the
-- KOSHA_BILLING_PLANS_FILE catalog.
CREATE TABLE IF NOT EXISTS kosha.billing_accounts (
    tenant_id            TEXT PRIMARY KEY,
    plan                 TEXT NOT NULL,
    provider_customer_id TEXT,              -- the tenant's customer at the payment provider
    created_at_ms        BIGINT NOT NULL,   -- Unix milliseconds; never changed by an update
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One invoice per tenant per calendar month (UTC). `invoice_id` is
-- `<tenant>/<YYYY-MM>`; the monthly job inserts with ON CONFLICT DO NOTHING,
-- so running it twice cannot bill a month twice. An invoice's amounts are
-- never updated after insert: only its status, paid time and provider id.
CREATE TABLE IF NOT EXISTS kosha.billing_invoices (
    invoice_id          TEXT PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    period              TEXT NOT NULL,      -- YYYY-MM
    period_start_ms     BIGINT NOT NULL,
    period_end_ms       BIGINT NOT NULL,    -- exclusive
    plan                TEXT NOT NULL,      -- the plan it was rated on
    line_items          TEXT NOT NULL,      -- JSON array of rated lines
    total_cents         BIGINT NOT NULL,    -- sum of the lines
    issued_at_ms        BIGINT NOT NULL,
    due_at_ms           BIGINT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('open', 'paid', 'void')),
    paid_at_ms          BIGINT,
    provider_invoice_id TEXT,               -- set once handed to the payment provider
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, period)
);

-- The account-standing refresh reads every unpaid invoice.
CREATE INDEX IF NOT EXISTS idx_billing_invoices_open
    ON kosha.billing_invoices (tenant_id) WHERE status = 'open';
