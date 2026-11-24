# Changelog

## [0.1.0]

First public preview (#351).

- `Kosha` client for the `/v1` API: namespaces (create, list, delete), and per
  namespace `putSchema`, `getSchema`, `upsert`, `merge`, `search`, `exists`,
  `deleteByQuery`, `flush`, `stats`; `request()` for any other route.
- Types generated from `api/openapi/v1/kosha.openapi.json`.
- Retries under the shared policy (clients/conformance/RETRY.md).
- `searchAll` (`search_after` pagination) and `bulkIndex`.
- Typed errors: `KoshaError` and subclasses.
