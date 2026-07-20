-- Bootstraps an isolated `kosha` role + database on a shared RDS instance,
-- so Kosha's control-plane tables (see 001_create_kosha_tables.sql) live in
-- their own database rather than sharing one with the rest of the Decover
-- backend. Run once per RDS instance, as an admin user, against the
-- instance's default maintenance database (e.g. `postgres`). Safe to re-run.
--
-- Requires a psql variable `pass` (-v pass=...) with the password to set for
-- the `kosha` role on first creation.

SELECT format('CREATE ROLE kosha WITH LOGIN PASSWORD %L', :'pass')
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'kosha')
\gexec

SELECT 'CREATE DATABASE kosha OWNER kosha'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'kosha')
\gexec
