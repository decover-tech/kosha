# Kosha API changelog

Changes to the public HTTP/JSON contract, keyed by the contract revision
(`info.version` in `api/openapi/v1/kosha.openapi.json`). The revision is
independent of the server's crate version; see `docs/api/versioning.md`.
`make api-check` fails when the spec changes without a new heading here.

Sections: Added, Changed, Deprecated, Removed.

## [0.9.0]

The first published contract (#350). Pre-1.0: corrections that make the
spec match the wire may still change it.

### Added

- `api/openapi/v1/kosha.openapi.json` (public) and
  `api/openapi/v1/kosha-admin.openapi.json` (operator, beta), generated from
  `proto/kosha/v1/*.proto` by `make openapi`.
- `POST /v1/namespaces/{namespace}/replace`: the `/v1` form of legacy
  `POST /replace` (replace by id, merging partial documents).
- `POST /v1/namespaces/{namespace}/exists`, `.../shards`, `GET /v1/usage`
  and `GET /v1/billing` are now in the spec (they were served but
  undocumented).

### Changed

- The proto describes the JSON the server speaks: `aggs` (not
  `aggregations`), `bm25_params: {k1, b, stats}` (not `bm25_k1`/`bm25_b`),
  the search fields that were missing (`search_after`, `operator`,
  `exact_total_hits`, `total_hits_cap`, `no_cache`, `collapse`,
  `source_includes`/`source_excludes`, `max_staleness_seconds`, ...),
  lowercase enum strings (`"strong"`, `"lexical"`), PascalCase document
  field types (`"Text"`), and counts as JSON integers.
- A known `/v1` path called with a method it does not take answers
  `405 Method Not Allowed` with an `Allow` header (was 404).
- `GET /v1/stats` moved to the admin document; it always required the
  admin credential.
