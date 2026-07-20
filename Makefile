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

# ── All ────────────────────────────────────────────────────────────────────

all: proto-lint proto-gen rust-build rust-test
