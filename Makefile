# ── Kosha build & codegen ──────────────────────────────────────────────────
#
#   make proto-lint       — lint proto files (buf, pinned in tools/codegen)
#   make openapi          — regenerate api/openapi/v1/*.openapi.json from proto/
#   make proto-gen        — alias of openapi (the only thing generated from proto)
#   make api-check        — openapi, then fail if api/ or proto/ drifted from git
#   make rust-test        — run Rust tests
#   make rust-build       — build Rust binary
#   make cli-assets       — kosha shell completions + man pages into target/cli-assets
#   make client-python-test — run Python client unit tests (+ the copy script smoke test)
#   make clients-gen      — regenerate the TypeScript and Go SDK types from api/openapi
#   make clients-gen-check — clients-gen, then fail on drift (+ conformance coverage)
#   make clients-test     — TypeScript and Go SDK unit tests
#   make clients-conformance — compose stack + the shared SDK conformance suite
#   make bench-hook-test   — run commit_bench_section.sh regression tests
#   make migration-job-manifest MIGRATION_INDICES="a b" — regen the ES→Kosha backfill Job YAML
#   make compat-matrix    — regen docs/compat/opensearch.{md,json} (KOSHA_BIN=... runs it live)
#   make all              — lint + openapi + build + test
#

.PHONY: proto-lint proto-gen openapi api-tools api-check rust-test rust-build client-python-test bench-hook-test all

# ── Proto and the OpenAPI contract (#350) ──────────────────────────────────
#
# buf, protoc-gen-openapiv2 and apispec are built from tools/codegen/go.mod,
# so every machine and CI job runs the same pinned versions. Needs Go.

GO ?= go
# The Python interpreter for the client and quickstart targets: `python3`,
# which every supported platform ships (many have no bare `python`).
# Override with PYTHON=... (e.g. a virtualenv's python).
PYTHON ?= python3
API_BIN := $(CURDIR)/build/bin
API_SPEC_DIR := api/openapi/v1

api-tools:
	cd tools/codegen && $(GO) build -o $(API_BIN)/ \
		github.com/bufbuild/buf/cmd/buf \
		github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2 \
		./cmd/apispec

proto-lint: api-tools
	cd proto && $(API_BIN)/buf lint

openapi: api-tools
	python3 scripts/api/gen_admin_proto.py
	cd proto && $(API_BIN)/buf lint
	rm -rf build/openapi
	cd proto && PATH="$(API_BIN):$$PATH" $(API_BIN)/buf generate
	mkdir -p $(API_SPEC_DIR)
	$(API_BIN)/apispec convert build/openapi/kosha/v1/kosha.swagger.json $(API_SPEC_DIR)/kosha.openapi.json
	$(API_BIN)/apispec convert build/openapi/kosha/v1/admin.swagger.json $(API_SPEC_DIR)/kosha-admin.openapi.json
	$(API_BIN)/apispec lint --public $(API_SPEC_DIR)/kosha.openapi.json
	$(API_BIN)/apispec lint $(API_SPEC_DIR)/kosha-admin.openapi.json

proto-gen: openapi

# The spec-sync gate CI runs: regenerate, then require a clean tree. The
# version check compares against origin/main when that ref exists.
api-check: openapi
	@git diff --exit-code --stat -- proto/ $(API_SPEC_DIR) && \
		test -z "$$(git ls-files --others --exclude-standard -- proto/ $(API_SPEC_DIR))" || \
		(echo "api/ or proto/ is stale: run 'make openapi' and commit the result" && exit 1)
	@if git rev-parse --verify -q origin/main >/dev/null && \
		git cat-file -e origin/main:$(API_SPEC_DIR)/kosha.openapi.json 2>/dev/null; then \
		git show origin/main:$(API_SPEC_DIR)/kosha.openapi.json > build/openapi/base.openapi.json && \
		$(API_BIN)/apispec check-version build/openapi/base.openapi.json \
			$(API_SPEC_DIR)/kosha.openapi.json api/CHANGELOG.md; \
	else echo "api-check: no spec on origin/main yet; version check skipped"; fi

# ── CLI completions + man pages ───────────────────────────────────────────
# Packaging input for the `kosha` CLI: completion scripts for every shell clap
# supports and one man page per command. Install, e.g.:
#   cp target/cli-assets/completions/_kosha "${fpath[1]}/"
#   cp target/cli-assets/man/*.1 /usr/local/share/man/man1/

.PHONY: cli-assets

CLI_ASSETS := target/cli-assets

cli-assets:
	cargo build --release --locked -p kosha-cli
	mkdir -p $(CLI_ASSETS)/completions $(CLI_ASSETS)/man
	target/release/kosha completions bash > $(CLI_ASSETS)/completions/kosha.bash
	target/release/kosha completions zsh > $(CLI_ASSETS)/completions/_kosha
	target/release/kosha completions fish > $(CLI_ASSETS)/completions/kosha.fish
	target/release/kosha completions powershell > $(CLI_ASSETS)/completions/_kosha.ps1
	target/release/kosha completions elvish > $(CLI_ASSETS)/completions/kosha.elv
	target/release/kosha man --out-dir $(CLI_ASSETS)/man

# ── Quickstart ────────────────────────────────────────────────────────────

.PHONY: quickstart

quickstart:
	KOSHA_HOST=http://localhost:8080 KOSHA_API_KEY=sk-kosha-dev $(PYTHON) scripts/quickstart.py

# ── Python client ──────────────────────────────────────────────────────────

.PHONY: client-python-build client-python-publish client-python-test

client-python-build:
	$(PYTHON) -m build clients/python

# Releases go out from CI: push a clients/python/vX.Y.Z tag and
# .github/workflows/release-clients.yml publishes to PyPI with trusted
# publishing. This manual twine upload is the fallback only.
client-python-publish:
	$(PYTHON) -m twine upload clients/python/dist/*

client-python-test:
	$(PYTHON) -m pip install -e clients/python pytest pyyaml
	$(PYTHON) -m pytest clients/python/tests/ scripts/tests/ -q

# ── TypeScript and Go SDKs (#351, ADR-351) ─────────────────────────────────
#
# Types are generated from the checked-in OpenAPI 3 document; the generated
# files are committed and `clients-gen-check` (CI: clients.yml) fails on
# drift. The Go generator is pinned in clients/go/tools/go.mod, the TS one in
# clients/typescript/package-lock.json.

.PHONY: clients-gen clients-gen-check clients-test clients-conformance clients-conformance-run

CLIENTS_COMPOSE := docker compose -f docker-compose.yml \
	-f scripts/integration/docker-compose.integration.yml \
	-f clients/conformance/docker-compose.clients.yml

clients-gen:
	cd clients/typescript && npm ci --no-audit --no-fund && npm run gen
	cd clients/go && $(GO) tool -modfile=tools/go.mod oapi-codegen -config oapi-codegen.yaml ../../$(API_SPEC_DIR)/kosha.openapi.json

clients-gen-check: clients-gen
	@git diff --exit-code --stat -- clients/typescript/src/generated clients/go/zz_generated_types.go || \
		(echo "generated SDK types are stale: run 'make clients-gen' and commit the result" && exit 1)
	$(PYTHON) clients/conformance/check_coverage.py
	@test "$$(cd clients/go && $(GO) list -m all)" = "github.com/decover-tech/kosha/clients/go" || \
		(echo "clients/go must have no dependencies outside the standard library" && exit 1)

clients-test:
	cd clients/typescript && npm ci --no-audit --no-fund && npm run typecheck && npm test
	cd clients/go && $(GO) vet ./... && $(GO) test -race ./...

# Brings up the read/write split stack with the conformance overlay, then
# runs the Python, TypeScript and Go runners through faultproxy.py.
clients-conformance:
	$(CLIENTS_COMPOSE) up --build -d
	scripts/integration/wait_ready.sh 8080 8081
	$(MAKE) clients-conformance-run

# Against an already-running stack (KOSHA_CONFORMANCE_UPSTREAM, default the
# query pod on :8081).
clients-conformance-run:
	PYTHON=$(PYTHON) GO=$(GO) clients/conformance/run.sh

# ── ES → Kosha migration ───────────────────────────────────────────────────

.PHONY: migration-job-manifest

# Regenerate the staging backfill Job manifest (it runs `kosha-server migrate`;
# see k8s/README.md). The index names belong to the deployment, not the repo:
#   make migration-job-manifest MIGRATION_INDICES="my_index other_index"
migration-job-manifest:
	@test -n "$(MIGRATION_INDICES)" || { echo 'set MIGRATION_INDICES="<index> [<index> ...]"' >&2; exit 1; }
	python3 scripts/gen_es_migration_job.py $(foreach i,$(MIGRATION_INDICES),--index $(i)) > k8s/stage/es-to-kosha-migration-job.yaml

# ── OpenSearch / Elasticsearch compatibility matrix ────────────────────────

.PHONY: compat-matrix

# Regenerate docs/compat/opensearch.{md,json} from scripts/conformance/dsl/cases.yaml.
# With KOSHA_BIN (a kosha-server binary) every case also runs against a live
# server, as CI does; without it only the client-side translation is checked.
# Needs PyYAML (pip install pyyaml).
compat-matrix:
ifdef KOSHA_BIN
	python3 scripts/conformance/dsl/run.py --spawn $(KOSHA_BIN)
else
	python3 scripts/conformance/dsl/run.py --offline
endif

# ── Rust ───────────────────────────────────────────────────────────────────

rust-test:
	cargo test --all-features --locked

rust-build:
	cargo build --release

# ── Bench commit-msg hook tests ─────────────────────────────────────────────

bench-hook-test:
	bash scripts/test_commit_bench_section.sh

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
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/003_writer_leases.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/003_scoped_api_keys.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/004_deletion_records.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/005_usage_events.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/006_orgs_users_projects.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/007_billing.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/008_tenant_kms_keys.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/009_console_signin.sql; \
	psql "$$KOSHA_DB_URL" -v ON_ERROR_STOP=1 -f crates/kosha-control/migrations/002_grant_kosha_role.sql

# ── All ────────────────────────────────────────────────────────────────────

all: proto-lint openapi rust-build rust-test client-python-test
