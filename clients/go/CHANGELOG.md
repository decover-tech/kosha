# Changelog

## [0.1.0]

First public preview (#351).

- `kosha.Client` for the `/v1` API: namespaces (create, list, delete), and per
  namespace `PutSchema`, `GetSchema`, `Upsert`, `Merge`, `Search`, `Exists`,
  `DeleteByQuery`, `Flush`, `Stats`; `Do` for any other route.
- Types generated from `api/openapi/v1/kosha.openapi.json` (models only;
  standard library only).
- Retries under the shared policy (clients/conformance/RETRY.md).
- `SearchAll` (`search_after` pagination, `iter.Seq2`) and `BulkIndex` /
  `BulkIndexSeq`.
- Typed errors: `*Error` with status-class sentinels, `*TransportError`,
  `*ConfigError`, `*BulkError`.
