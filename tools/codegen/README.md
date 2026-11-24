# API code generation

## Source of truth

The public HTTP/JSON API is described by `proto/kosha/v1/kosha.proto`
(tenant routes) and `proto/kosha/v1/admin.proto` (operator routes; generated
from the route table by `scripts/api/gen_admin_proto.py`), with the shared
`ErrorResponse` in `proto/kosha/v1/errors.proto`.

The proto is written to match the wire exactly: snake_case field names,
string enums with the server's spellings, counts as JSON integers. The
generated OpenAPI 3 documents are the published contract:

- `api/openapi/v1/kosha.openapi.json`: the public API
- `api/openapi/v1/kosha-admin.openapi.json`: the operator API (beta)
- `api/CHANGELOG.md`: what changed in each contract revision

Proto3 canonical JSON is **not** the wire format; the OpenAPI documents are
(see `docs/api/versioning.md`).

## Toolchain

`tools/codegen/go.mod` pins every tool, so all machines and CI produce the
same bytes with no plugin registry involved:

- `buf` (lint, generate, breaking)
- `protoc-gen-openapiv2` (grpc-gateway), run as a local plugin
- `apispec` (`cmd/apispec`): Swagger 2 to OpenAPI 3.0.3 conversion
  (kin-openapi), stable JSON encoding, and the spec lint

You need Go (the version in `go.mod`). `make api-tools` builds them into
`build/bin/`.

## Workflow

```
# 1. Edit proto/kosha/v1/kosha.proto (and the row in
#    crates/kosha-server/src/routes.rs for a new route)
# 2. Regenerate and lint
make openapi
# 3. Bump info.version in kosha.proto, add an api/CHANGELOG.md entry
# 4. Check what CI checks
make api-check
cargo test -p kosha-server public_routes_match_spec
python3 scripts/api/check_wire.py --spawn target/debug/kosha-server
```

CI (`api contract` job) runs `buf lint`, `make api-check` (regenerate and
require a clean tree, plus the version and changelog check) and
`buf breaking` against main. The `test` job runs `public_routes_match_spec`
(route table against the spec, every operation reaches a handler), and the
conformance workflow runs the wire check against a live stack.

## Clients from the spec

The TypeScript and Go SDKs (#351, ADR-351) take their **types** from the
public OpenAPI 3 document; their transport, retries, pagination and bulk
helpers are hand-written:

```
proto/kosha/v1/*.proto
  -> make openapi   -> api/openapi/v1/kosha.openapi.json (checked in)
  -> make clients-gen
       -> clients/typescript/src/generated/schema.ts   (openapi-typescript, pinned in package-lock.json)
       -> clients/go/zz_generated_types.go             (oapi-codegen models only, pinned in clients/go/tools/go.mod)
```

Both outputs are committed; `make clients-gen-check` (CI: `clients.yml`)
regenerates them and fails on drift. A proto change that renames a schema
therefore breaks the SDKs' hand-written alias files (`src/types.ts`,
`types.go`) at build time instead of changing their public API silently.
The shared conformance suite in `clients/conformance` pins behaviour across
the Python, TypeScript and Go SDKs.

Other languages can generate from the same document, for example
`npx openapi-typescript api/openapi/v1/kosha.openapi.json -o kosha.d.ts`.

## Versioning

See `docs/api/versioning.md`. In short: the URL prefix (`/v1`) and proto
package (`kosha.v1`) are the major version; `info.version` is the contract
revision, and inside v1 only additive changes ship.
