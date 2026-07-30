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
| `kosha search -n NS "query" [--max 10] [--filter '…']` | BM25 search |
| `kosha search -n NS --body @query.json` | Full `SearchQuery` JSON |
| `kosha flush [-n NS]` | Flush buffer (omit ns = flush all via legacy `/flush`) |
| `kosha delete -n NS --filter '{…}'` | Delete by filter |
| `kosha curl METHOD PATH [--body @file]` | Raw REST escape hatch |

Add `--json` for machine-readable output.

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
