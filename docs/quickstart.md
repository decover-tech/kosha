# CLI and Python quickstart

The shortest path, with Docker and curl, is the [README](../README.md#60-second-quickstart).
This page is the CLI and Python-client walkthrough that used to sit at the bottom of the README.

## Quickstart (test as a customer would)

```bash
# 1. Start Kosha locally
docker compose up --build

# 2. Install the CLI (or use cargo run -p kosha-cli -- …)
cargo install --path crates/kosha-cli

# 3. In another terminal, explore the index
export KOSHA_HOST=http://localhost:8080
export KOSHA_API_KEY=sk-kosha-dev

kosha health
kosha index -n quickstart-demo --file crates/kosha-cli/examples/docs.jsonl
kosha flush -n quickstart-demo
kosha search -n quickstart-demo "breach"
kosha stats -n quickstart-demo

# Vector search (the example docs carry a 4-dimensional `embedding`)
kosha index -n quickstart-demo --file crates/kosha-cli/examples/vectors.jsonl
kosha flush -n quickstart-demo
kosha search -n quickstart-demo --knn-field embedding --knn-vector '[0.85,0.15,0,0.05]'
```

See [crates/kosha-cli/README.md](../crates/kosha-cli/README.md) for profiles
(`~/.kosha/config.toml`), `--json` output, hybrid search / sort / cursor
flags, admin commands, shell completions, and the `kosha curl` escape hatch.

The Python client ([`kosha-client` on PyPI](https://pypi.org/project/kosha-client/))
still works the same way for application code:

```bash
pip install kosha-client
```

```python
from kosha_client import KoshaClient

client = KoshaClient(
    hosts="https://app.kosha.io",
    api_key="sk-acme-corp-xxx",
)
client.ping()  # True
```

Both the CLI and the Python client respect `KOSHA_HOST` and `KOSHA_API_KEY`.
`scripts/quickstart.py` remains as a scripted alternative to the CLI flow.
