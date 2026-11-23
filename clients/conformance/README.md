# SDK conformance suite

One set of language-neutral cases that the Python (`kosha-client`),
TypeScript (`@decover/kosha`) and Go SDKs all run against a live Kosha
server (ADR-351 §d). A behaviour the cases pin cannot drift between the
three SDKs without a red check.

| file | |
|---|---|
| `cases/*.json` | the cases (namespaces, documents, search, pagination, bulk, errors, retry, compat) |
| `case.schema.json` | the case format: the closed sets of ops and matchers every runner implements |
| `RETRY.md` | the normative retry policy the `retry.*` cases pin |
| `faultproxy.py` | a stdlib reverse proxy that injects faults and counts attempts |
| `docker-compose.clients.yml`, `limits.json` | the overlay that adds a body-limited and a rate-limited tenant |
| `check_coverage.py` | fails when a public operationId has no case and is not excluded with a reason |
| `run.sh` | starts faultproxy, then runs the three runners |

Runners: `clients/python/tests/conformance/test_conformance.py` (pytest),
`clients/typescript/test/conformance.test.ts` (vitest),
`clients/go/conformance_test.go` (`go test -tags conformance`). Each skips
unless `KOSHA_CONFORMANCE_URL` is set, and checks that it implements exactly
the schema's ops and matchers: an unknown op or matcher fails.

## Run it

```sh
make clients-conformance        # compose stack (ingest + query pods) + overlay, then all three runners
make clients-conformance-run    # against a stack that is already up (query pod on :8081)
```

Or by hand against any server (cases with faults or attempt counts need the proxy):

```sh
python3 clients/conformance/faultproxy.py --upstream http://localhost:8081 &
export KOSHA_CONFORMANCE_URL=http://127.0.0.1:18081 KOSHA_CONFORMANCE_CONTROL=http://127.0.0.1:18091
python3 -m pytest clients/python/tests/conformance
(cd clients/typescript && npm run test:conformance)
(cd clients/go && go test -tags conformance -run Conformance ./...)
```

| variable | default | |
|---|---|---|
| `KOSHA_CONFORMANCE_URL` | (unset: skip) | the server, normally faultproxy |
| `KOSHA_CONFORMANCE_CONTROL` | (unset: skip proxy cases) | faultproxy's control API |
| `KOSHA_CONFORMANCE_API_KEY` | `sk-kosha-dev` | tenant `default` |
| `KOSHA_CONFORMANCE_SMALL_KEY` | `sk-conformance-small` | tenant with `max_body_bytes: 4096` |
| `KOSHA_CONFORMANCE_LIMITED_KEY` | `sk-conformance-limited` | tenant with `qps: 1` |
| `KOSHA_CONFORMANCE_RUN_ID` | time-based | keeps namespaces of concurrent runs apart |

CI: the `conformance` job in `.github/workflows/clients.yml`, on every PR
that touches the server, the API or a client.

## Case format

```json
{"id": "documents.upsert_visible", "covers": ["KoshaService_IndexDocuments"],
 "tenant": "default", "setup": [{"op": "create_namespace", "args": {...}}],
 "faults": [...], "steps": [{"op": "search", "args": {...}, "client": {...}, "expect": {...}}]}
```

- **Namespace:** `$NS` is `conf-<lang>-<run id>-<case id slug>`, so runners
  never collide; ops default to it, and `args.namespace` overrides it
  (`"$NS-missing"`). The runner deletes `$NS` after the case.
- **Placeholders:** `$NS` and `$BIG` (5000 `x` characters) are replaced in
  every string of `args` and `expect`.
- **Generator:** `documents` (and the `ids` matcher) may be
  `{"generate": {"count": 250, "id": "d{i:04}", "fields": {"tag": {"type": "Keyword", "value": "t{i%7}"}}}}`:
  `{i}` is the index, `{i:0W}` zero-padded to W digits, `{i%N}` the index mod
  N, `{i%N:0W}` both.
- **`client`:** settings for one step (`timeout_ms`, `max_attempts`,
  `total_budget_ms`, `retry_non_idempotent`); the runner builds a client
  with them.
- **Tags:** every request carries `X-Conformance-Case: <lang>-<run>-<case>#<step>`
  (`#setup`, `#cleanup` outside steps). faultproxy applies a case's faults
  only to its numbered steps and logs every request, which is what the
  `attempts` matchers count.

### What each op returns

| op | result |
|---|---|
| `search` | `ids` (hit ids in order), `total_hits`, `count` |
| `search_all` | `ids` (every yielded id in order), `count` |
| `exists` | `ids` (the existing ones), `count` |
| `list_namespaces` | `ids` (names), `count` |
| `get_schema` | `ids` (field names, sorted), `count` |
| `stats` | `count` (documents) |
| `upsert`, `merge` | `indexed` |
| `bulk_index` | `indexed`, `batches`, `failed_ids` (also on a `BulkError`) |
| `delete_by_query` | `count` (deleted) |
| `create_namespace`, `delete_namespace`, `put_schema`, `flush`, `raw_request` | nothing beyond success |

### Matchers

Without an `error` matcher the step must succeed. `ids`, `ids_unordered`,
`ids_include`, `total_hits`, `count`, `unique`, `indexed`, `batches`,
`failed_ids` compare the result; `error` checks `kind` (`http`, `transport`,
`config`), `status`, `code`, `retryable`, `has_request_id`,
`retry_after_ms`; `attempts` / `attempts_min` count the step's requests at
faultproxy; `min_elapsed_ms` / `max_elapsed_ms` time the call. Live tenant
limits use lower bounds only.

## Adding a case

1. Add it to a `cases/*.json` file; `covers` lists the operationIds it
   exercises.
2. `python3 clients/conformance/check_coverage.py`.
3. Run all three runners. A new op or matcher means a schema change and an
   implementation in all three runners in the same PR.
