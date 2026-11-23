# OpenSearch / Elasticsearch DSL compatibility cases

This directory produces the compatibility matrix in
[`docs/compat/opensearch.md`](../../../docs/compat/opensearch.md) and
[`docs/compat/opensearch.json`](../../../docs/compat/opensearch.json).

Kosha has no server-side OpenSearch endpoint. kosha-client (Python) translates
OpenSearch and Elasticsearch request bodies into the native `/v1` API, so the
matrix describes that translation and how `/v1` answers it. The TypeScript and
Go SDKs do not translate the DSL.

- `cases.yaml` has one case per matrix row: the construct, an example body (or
  a client method), the outcome (`supported`, `partial` or `rejected`), a note,
  and for a partial row the divergence. The header of the file documents every
  field.
- `run.py` checks every case and, if all of them hold, writes both matrix
  files. For a search body it translates `es_body` with
  `KoshaClient._translate_search`, sends the result to
  `POST /v1/namespaces/{ns}/search` on a live server seeded with the
  conformance corpus (`../corpus.py`), and checks the outcome and the case's
  `expect` block. When the translated body is one the reference model covers,
  it also compares ids, totals and BM25 scores with `../oracle.py` exactly.
  Index-API cases call the named `KoshaClient` method; each has a check
  function in `API_CHECKS`.

The rendered matrix depends only on `cases.yaml`, never on the run, so CI
(`.github/workflows/conformance.yml`) regenerates it and fails when the
committed copy differs.

## Run it

Needs Python 3.10+ and PyYAML (`pip install pyyaml`). kosha-client is imported
from `clients/python` in this checkout.

```bash
# Live, against a server the runner starts (what CI does, minus compose):
cargo build --release -p kosha-server
python3 scripts/conformance/dsl/run.py --spawn target/release/kosha-server
# or: make compat-matrix KOSHA_BIN=target/release/kosha-server

# Against a running server:
python3 scripts/conformance/dsl/run.py --host http://localhost:8080 --api-key sk-kosha-dev

# Translation checks only, no server (fast; writes the same files):
python3 scripts/conformance/dsl/run.py --offline     # or: make compat-matrix
```

`--no-write` checks without writing; `--out report.json` saves the per-case
results.

## Adding a case

1. Add the case to `cases.yaml`. Name the registry entries it covers in
   `kinds`.
2. Run `make compat-matrix KOSHA_BIN=...` and commit `cases.yaml` together with
   both files under `docs/compat/`.

When the translator learns a new clause, aggregation, body key or client
method, add it to the registry at the top of
`clients/python/kosha_client/client.py` and add a case;
`clients/python/tests/test_dsl_registry.py` fails until both are done. When a
behaviour changes, the case that pinned it fails: update its outcome, note and
checks, and regenerate.
