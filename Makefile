# ── Kosha build & codegen ──────────────────────────────────────────────────
#
#   make proto-lint       — lint proto files
#   make proto-gen        — generate all stubs + OpenAPI spec from proto
#   make openapi          — generate OpenAPI spec only
#   make gen/python       — generate Python gRPC stubs
#   make gen/go           — generate Go gRPC stubs
#   make gen              — generate everything
#   make rust-test        — run Rust tests
#   make rust-build       — build Rust binary
#   make all              — lint + gen + build + test
#

.PHONY: proto-lint proto-gen openapi gen gen/python gen/go rust-test rust-build all

# ── Proto ──────────────────────────────────────────────────────────────────

proto-lint:
	cd proto && buf lint

proto-gen:
	cd proto && buf generate

openapi:
	cd proto && buf generate --template buf.gen.yaml --path kosha/v1 --filter proto.lint

# ── Generated stubs ────────────────────────────────────────────────────────

gen: proto-gen

gen/python: proto-gen  # for now, proto-gen includes python; isolate later

gen/go: proto-gen      # for now, proto-gen includes go; isolate later

# ── Quickstart ────────────────────────────────────────────────────────────

.PHONY: quickstart

quickstart:
	KOSHA_HOST=http://localhost:8080 KOSHA_API_KEY=sk-kosha-dev python scripts/quickstart.py

# ── Python client ──────────────────────────────────────────────────────────

.PHONY: client-python-build client-python-publish

client-python-build:
	python -m build clients/python

client-python-publish:
	python -m twine upload clients/python/dist/*

# ── Rust ───────────────────────────────────────────────────────────────────

rust-test:
	cargo test --all-features --locked

rust-build:
	cargo build --release

# ── Database ──────────────────────────────────────────────────────────────
#
# Kosha gets its own `kosha` database + role on the shared RDS instance
# (rather than just a schema) so it's isolated from the rest of the Decover
# backend. DATABASE_URL should point at the instance's admin/maintenance
# database (e.g. .../postgres); db-migrate derives the kosha-specific URL
# from it after bootstrapping.

.PHONY: db-bootstrap db-migrate

db-bootstrap:
	@test -n "$$KOSHA_DB_PASSWORD" || (echo "KOSHA_DB_PASSWORD must be set" && exit 1)
	@echo "Bootstrapping isolated kosha role + database on $$DATABASE_URL ..."
	psql "$$DATABASE_URL" -v ON_ERROR_STOP=1 -v pass="$$KOSHA_DB_PASSWORD" \
		-f crates/kosha-control/migrations/000_bootstrap_database.sql

db-migrate: db-bootstrap
	@echo "Applying Kosha migrations to the kosha database ..."
	@KOSHA_DB_URL="$$(echo "$$DATABASE_URL" | sed -E 's#(/[^/?]+)(\?.*)?$$#/kosha\2#')"; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/001_create_kosha_tables.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/002_grant_kosha_role.sql

# ── All ────────────────────────────────────────────────────────────────────

all: proto-lint proto-gen rust-build rust-test
