# Code generation

## Source of truth

All API definitions live in `proto/kosha/v1/kosha.proto`. This single file is the
canonical contract for the Kosha service — every client and server implements it.

## Prerequisites

- [buf](https://buf.build/docs/installation) CLI (`brew install buf`)
- protoc plugins for your target languages:
  - Go: `protoc-gen-go`, `protoc-gen-go-grpc`
  - Python: `grpcio-tools` (`pip install grpcio-tools`)
  - OpenAPI: included via buf remote plugin

## Workflow

```
# 1. Edit the proto → proto/kosha/v1/kosha.proto
# 2. Lint
make proto-lint

# 3. Check breaking changes against main
cd proto && buf breaking --against .git#branch=main

# 4. Generate all stubs and specs
make gen

# 5. Generated output lands in:
#    gen/go/          — Go client/server stubs
#    gen/python/      — Python gRPC stubs
#    gen/openapi/     — OpenAPI JSON spec
```

## OpenAPI → more clients

The generated OpenAPI spec (`gen/openapi/kosha.yaml`) can drive
[openapi-generator](https://openapi-generator.tech/) to produce clients in any
supported language (TypeScript, Java, C#, Ruby, etc.):

```bash
npx @openapitools/openapi-generator-cli generate \
  -i gen/openapi/kosha.yaml \
  -g typescript-fetch \
  -o clients/typescript
```

## Versioning

- The proto package is `kosha.v1`. Backward-incompatible changes increment the
  package to `kosha.v2`, etc.
- The proto file itself carries a `google.api.http` annotation on every RPC so
  HTTP/JSON clients never need the protobuf binary format.
