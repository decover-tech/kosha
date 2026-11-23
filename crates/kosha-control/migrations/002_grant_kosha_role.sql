-- 001 runs as the RDS admin (see 000_bootstrap_database.sql and Makefile's
-- db-migrate), so the schema/tables it creates end up owned by that admin —
-- not the `kosha` role the server actually connects as. But 001 is also
-- embedded into the server binary and re-run on every startup (see
-- kosha-control/src/postgres.rs), executed as `kosha`. A non-owner role
-- can't run privilege-changing statements on objects it doesn't own, so
-- ownership has to move to `kosha` itself rather than just granting it
-- access. Run once per RDS instance as admin, after 001. Safe to re-run.

ALTER SCHEMA kosha OWNER TO kosha;
ALTER TABLE kosha.namespaces OWNER TO kosha;
ALTER TABLE kosha.manifests OWNER TO kosha;
ALTER TABLE kosha.api_keys OWNER TO kosha;
ALTER TABLE kosha.retired_namespaces OWNER TO kosha;
-- Also from 001, and missing here until #426's migrations were first run
-- together: without it the server's role could not touch the table on an
-- instance migrated with `make db-migrate`, and deleting a namespace failed
-- with "permission denied for table hydration_leases".
-- An instance that ran the earlier 002 needs this file run again (the
-- server cannot do it; it warns at startup instead, see README "Upgrading").
ALTER TABLE IF EXISTS kosha.hydration_leases OWNER TO kosha;
-- 003_writer_leases.sql (#254). IF EXISTS: 002 may run before 003 on an
-- instance that has not been migrated past 001 yet.
ALTER TABLE IF EXISTS kosha.writer_leases OWNER TO kosha;
-- 004_deletion_records.sql (#347). IF EXISTS, as for 003.
ALTER TABLE IF EXISTS kosha.deletion_records OWNER TO kosha;
-- 005_usage_events.sql (#348). IF EXISTS for the same reason.
ALTER TABLE IF EXISTS kosha.usage_events OWNER TO kosha;
-- 006_orgs_users_projects.sql (#345). IF EXISTS for the same reason. Moving
-- audit_log also moves the sequence its BIGSERIAL id owns.
ALTER TABLE IF EXISTS kosha.users OWNER TO kosha;
ALTER TABLE IF EXISTS kosha.organizations OWNER TO kosha;
ALTER TABLE IF EXISTS kosha.org_members OWNER TO kosha;
ALTER TABLE IF EXISTS kosha.projects OWNER TO kosha;
ALTER TABLE IF EXISTS kosha.tenants OWNER TO kosha;
ALTER TABLE IF EXISTS kosha.audit_log OWNER TO kosha;
-- 007_billing.sql (#349). IF EXISTS for the same reason.
ALTER TABLE IF EXISTS kosha.billing_accounts OWNER TO kosha;
ALTER TABLE IF EXISTS kosha.billing_invoices OWNER TO kosha;
