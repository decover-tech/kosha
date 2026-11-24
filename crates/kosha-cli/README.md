# kosha CLI

Remote HTTP client for exploring Kosha namespaces — the Elastic/OpenSearch-style
companion to `kosha-server`.

```bash
cargo install --path crates/kosha-cli
# or: cargo run -p kosha-cli -- <args>
```

## Quickstart

```bash
# 1. Start Kosha locally
docker compose up --build

# 2. Point the CLI at it (env vars, or a profile — see below)
export KOSHA_HOST=http://localhost:8080
export KOSHA_API_KEY=sk-kosha-dev

# 3. Index → flush → search → stats
kosha health
kosha index -n quickstart-demo --file crates/kosha-cli/examples/docs.jsonl
kosha flush -n quickstart-demo
kosha search -n quickstart-demo "breach"
kosha stats -n quickstart-demo
```

## Profiles

Config file: `~/.kosha/config.toml`

```toml
default_profile = "local"

[profiles.local]
host = "http://localhost:8080"
api_key = "sk-kosha-dev"

[profiles.staging]
host = "https://kosha.example"
api_key_env = "KOSHA_STAGING_API_KEY"
```

```bash
kosha profile list
kosha profile show local
kosha profile set-default local
kosha --profile staging stats
```

Precedence: `--host` / `--api-key` flags → `KOSHA_HOST` / `KOSHA_API_KEY` env →
selected profile → default `http://localhost:8080`.

## Commands

| Command | Purpose |
|---------|---------|
| `kosha health` | `GET /v1/healthz` |
| `kosha stats [-n NS]` | Global or per-namespace stats |
| `kosha index -n NS --file docs.jsonl` | Bulk index (JSONL or `--doc '{…}'`) |
| `kosha search -n NS "query" [flags]` | BM25, kNN or hybrid search (see [Search](#search)) |
| `kosha search -n NS --body @query.json` | Full `SearchQuery` JSON |
| `kosha exists -n NS DOC_ID…` | `POST /v1/namespaces/{ns}/exists`; exit status 1 if any id is missing |
| `kosha flush [-n NS]` | Flush buffer (omit ns = flush every namespace `GET /v1/namespaces` lists) |
| `kosha delete -n NS --filter '{…}'` | Delete by filter |
| `kosha compact -n TENANT/NS [--mode tiered\|full] [--wait]` | Start a compaction job (admin) |
| `kosha compaction-status [--job ID]` | Scheduler overview, or one job (admin) |
| `kosha import -n TENANT/NS --source s3 [--overwrite]` | Rebuild a manifest from S3 segment footers (admin) |
| `kosha admin create-api-key --tenant T [--description D] [--scope read,write,admin] [--namespace NS]... [--expires-in-secs N]` | Create a tenant API key (admin, Postgres only) |
| `kosha admin list-api-keys [--tenant T]` | List keys: id, scopes, namespaces, expiry, last use (admin) |
| `kosha admin rotate-api-key KEY_ID [--overlap-secs N] [--expires-in-secs N]` | Replace a key; the old one keeps working for the overlap (admin) |
| `kosha admin revoke-api-key KEY_ID` | Revoke a key (admin) |
| `kosha admin scrub-plaintext-api-keys` | Remove plaintext held by keys created before scoped keys (admin) |
| `kosha admin count-plaintext-api-keys` | How many keys still hold plaintext, i.e. what the scrub would rewrite (admin, read-only) |
| `kosha admin create-operator NAME [--description D] [--expires-in-secs N]` | Issue a named admin credential for one operator (admin, Postgres only) |
| `kosha admin list-operators` | List operator credentials: id, name, expiry, last use (admin) |
| `kosha admin rotate-operator KEY_ID [--overlap-secs N] [--expires-in-secs N]` | Replace an operator credential; the old one keeps working for the overlap (admin) |
| `kosha admin revoke-operator KEY_ID` / `--name NAME` | Revoke one operator credential, or every credential of an operator (admin) |
| `kosha rebuild-filter-blooms -n NS` | Rebuild segment filter blooms (admin) |
| `kosha backfill-offset-tables -n NS` | Backfill `doc_store.offsets` (admin) |
| `kosha curl METHOD PATH [--body @file]` | Raw REST escape hatch |
| `kosha profile list\|show\|set-default` | Manage connection profiles |

Add `--json` to any command for machine-readable output: it prints the
server's response as is (for `compact --wait`, the finished job record).
Usage mistakes — conflicting flags, a bad `--sort` order, a cursor that does
not match the sort keys — are reported by the CLI with exit status 2 before
any request is sent.

### Search

Every flag below except `--max` is an alternative to setting the same
`SearchQuery` field in `--body`, so each one conflicts with `--body`. JSON
flags accept inline JSON or `@file.json`, the convention `--body` uses.

| Flag | `SearchQuery` field |
|------|---------------------|
| `--max N` (default 10) | `max_results` |
| `--from N` | `from` (ignored by the server when `--search-after` is set) |
| `--filter JSON` | `filter` |
| `--exact` | `exact_total_hits: true` |
| `--or` | `operator: "or"` |
| `--sort FIELD[:asc\|:desc]` (repeatable) | `sort`, in flag order; a bare field sorts ascending; `_score` and `_id` work too |
| `--search-after VALUE` (repeatable) | `search_after`; one value per `--sort`, or a single `_id` without `--sort` |
| `--knn-field F --knn-vector JSON\|@FILE` | `knn.field`, `knn.vector` |
| `--k N` | `knn.k` (default `--from` + `--max`) |
| `--num-candidates N` | `knn.num_candidates` (default max(100, k)) |
| `--knn-filter JSON` | `knn.filter` (applies to the kNN leg only) |
| `--highlight FIELD [--pre-tag T] [--post-tag T]` | `highlight` (tags default to `<b>` / `</b>`) |
| `--aggs JSON` | `aggs`, e.g. `'{"by_year":{"terms":{"field":"year"}}}'` |
| `--no-cache` | `no_cache: true` (bypass the server's result cache) |

A query string with no `--knn-*` flags is lexical (BM25); `--knn-*` with no
query string is a pure vector search; both together is **hybrid** — the two
result lists are fused with Reciprocal Rank Fusion.

```bash
# Vector search (examples/vectors.jsonl has a 4-dimensional `embedding` field)
kosha index -n vec-demo --file crates/kosha-cli/examples/vectors.jsonl
kosha flush -n vec-demo
kosha search -n vec-demo --knn-field embedding --knn-vector '[0.85,0.15,0,0.05]' --max 3

# Hybrid: BM25 on "contract" fused with kNN; the vector read from a file
echo '[0.1,0.9,0.1,0]' > q.json
kosha search -n vec-demo "contract" --knn-field embedding --knn-vector @q.json

# Sorted pages with a cursor. Plain output ends with the flags for the next page:
kosha search -n vec-demo "the" --or --sort year:desc --sort _id --max 2
#   …
#   next page: --search-after 2020 --search-after vec-3
kosha search -n vec-demo "the" --or --sort year:desc --sort _id --max 2 \
  --search-after 2020 --search-after vec-3

# Highlighting and aggregations
kosha search -n vec-demo "contract" --highlight content --pre-tag '[' --post-tag ']'
kosha search -n vec-demo "the" --or --aggs '{"by_year":{"terms":{"field":"year"}}}'
```

The `next page:` line appears only for a sorted, full page; a score-ordered
page has no stable cursor, so page it with `--from`.

### Exists

```bash
kosha exists -n quickstart-demo doc-1           # prints "exists", exit 0
kosha exists -n quickstart-demo doc-1 doc-404   # one line per id, exit 1
kosha --json exists -n quickstart-demo doc-404  # {"existing":[],"missing":["doc-404"],…}, exit 1
```

### Admin commands

`compact`, `compaction-status`, `import`, the `admin *-api-key(s)` commands,
`rebuild-filter-blooms` and `backfill-offset-tables` call `/v1/admin/*`, which
the server only accepts with its `KOSHA_ADMIN_API_KEY`. Pass that key the same
way as any other (`--api-key`, `KOSHA_API_KEY`, or a profile — a separate
`admin` profile works well). The local `docker compose` stack sets it to
`sk-kosha-admin-dev`. A refused key, or a server without the backend a route
needs (Postgres for API keys, S3 for import), is reported in plain words
rather than as a bare status code.

Admin requests do not run as the tenant that owns the data, so name the
namespace as it is stored, with its tenant prefix: `default/my-ns` for a
namespace created with the bootstrap `KOSHA_API_KEY`.

```bash
export KOSHA_API_KEY=sk-kosha-admin-dev

# Compaction runs as a background job on the server
kosha compact -n default/quickstart-demo                # prints the job id
kosha compaction-status --job cj-1795125506152-1        # one job
kosha compaction-status                                 # scheduler + every namespace
kosha compact -n default/quickstart-demo --mode full --wait --timeout 1800

# Disaster recovery: rebuild a manifest from the segment footers in S3
kosha import -n default/quickstart-demo --source s3
kosha import -n default/quickstart-demo --source s3 --overwrite   # replace a live manifest

# Create a tenant key. The key alone goes to stdout (notes go to stderr),
# and it is shown only this once:
NEW_KEY=$(kosha admin create-api-key --tenant acme --description "staging")

# A read-only key for one namespace that expires in 30 days:
RO_KEY=$(kosha admin create-api-key --tenant acme --scope read \
  --namespace docs --expires-in-secs 2592000)

# Rotate without downtime: the old key keeps working for an hour.
kosha admin list-api-keys --tenant acme          # first column is the key id
ROTATED=$(kosha admin rotate-api-key "$KEY_ID" --overlap-secs 3600)
kosha admin revoke-api-key "$KEY_ID"             # or cut it off now
```

Without `--scope`, `--namespace` or `--expires-in-secs` a key has every scope,
reaches every namespace of its tenant and never expires. `rotate-api-key`
prints the replacement key the same way `create-api-key` does.

Operator credentials replace the shared `KOSHA_ADMIN_API_KEY` with one named
admin credential per person or job (see
[Operator admin credentials](../../README.md#operator-admin-credentials)).
They print the same way:

```bash
# Issue a credential for alice, valid for 90 days; it is shown only once.
ALICE=$(kosha admin create-operator alice@example.com --expires-in-secs 7776000)

kosha admin list-operators                       # first column is the credential id
kosha --api-key "$ALICE" admin rotate-operator "$CRED_ID" --overlap-secs 3600
kosha admin revoke-operator --name alice@example.com   # offboarding: every credential
```

`compact --wait` polls `GET /v1/admin/compaction-jobs/{id}` every
`--poll-interval` seconds (default 2) and gives up after `--timeout` seconds
(default 3600); the job itself keeps running on the server. It exits non-zero
if the job failed. `import` waits up to `--timeout` seconds (default 600) for
the server, since it fetches every segment footer first.

`scripts/create_api_key.py` is now a thin wrapper around
`kosha admin create-api-key`.

## Shell completions

`kosha completions <bash|zsh|fish|powershell|elvish>` prints a completion
script to stdout (the command is hidden from `--help`).

```bash
# bash (needs bash-completion; or source it from ~/.bashrc)
kosha completions bash > ~/.local/share/bash-completion/completions/kosha
# zsh (any directory on $fpath, before compinit runs)
kosha completions zsh > "${fpath[1]}/_kosha"
# fish
kosha completions fish > ~/.config/fish/completions/kosha.fish
# PowerShell
kosha completions powershell >> $PROFILE
# elvish
kosha completions elvish >> ~/.config/elvish/rc.elv
```

## Man page

`kosha man` prints the `kosha(1)` page (roff) to stdout;
`kosha man --out-dir DIR` writes it plus one `kosha-<command>(1)` page per
subcommand, which is where each command's flags are documented.

```bash
man -l <(kosha man)                        # Linux (man-db)
kosha man | mandoc -a                      # macOS / BSD
kosha man --out-dir /usr/local/share/man/man1
```

`make cli-assets` builds the release binary and writes every completion
script and man page into `target/cli-assets/`, for packaging.

### Document formats

JSONL lines may be either native Kosha documents:

```json
{"id":"d1","fields":[{"name":"title","field_type":"Text","value":"hello"}]}
```

or shorthand (strings→Text, ints→Integer, floats→Float, bools→Boolean):

```json
{"id":"d1","title":"hello","count":3}
```

## Not in this binary

- ES → Kosha migration stays on `kosha-server migrate` (node/ops tool)
- Local disk `--data-dir` mode is out of scope for MVP
