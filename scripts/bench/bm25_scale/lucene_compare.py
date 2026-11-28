#!/usr/bin/env python3
"""Shared settings for the single-node OpenSearch / Elasticsearch BM25 scale run.

The measured client is query_bench_opensearch.py, which imports
`percentile` and `query_order` from query_bench.py so a Kosha phase and a
Lucene phase reduce latency the same way. This module only builds the
index, the query, the container, and the list-price assumptions. Nothing
here is a measured latency.

Pinned images (current stable releases as of 2026-11-28):

* OpenSearch 3.10.1, released 2026-11-24, Apache-2.0,
  ``opensearchproject/opensearch:3.10.1``.
* Elasticsearch 9.5.7, released 2026-11-23, Elastic License 2.0. The image
  is pulled when the workflow runs. It is not vendored in this repo.
"""

from __future__ import annotations

import json
import shlex
import sys
from pathlib import Path

# char::is_ascii_punctuation, the edge trim Kosha's `standard` analyzer uses
# (whitespace split, then trim ASCII punctuation from each token, then
# lowercase). A comma in the middle of a token stays, so "hello,world" is
# one token on both sides. Java sees these escapes inside the pattern_replace
# regex; json.dumps keeps them as a regex string, not as raw control bytes.
_EDGE = r"[\u0021-\u002F\u003A-\u0040\u005B-\u0060\u007B-\u007E]"
EDGE_PUNCT_PATTERN = f"^{_EDGE}+|{_EDGE}+$"

HEAP = "31g"
DEFAULT_SHARDS = 8
DEFAULT_REPLICAS = 0
DEFAULT_MAX_SEGMENTS = 1
BM25_K1 = 1.2
BM25_B = 0.75
# Same cap Kosha's KOSHA_TOTAL_HITS_CAP default uses, and the engine default.
TRACK_TOTAL_HITS = 10_000
CORPUS_S3 = "s3://decoverai-bench-corpus-cache/msmarco-10m"
ANALYZER_NAME = "kosha_standard"

# Public list prices, not a DecoverAI invoice. The hourly rate is the
# published Linux on-demand price for m7i.8xlarge in us-east-1 on 2026-11-28.
# gp3's $0.08/GB-month is the figure already cited in
# docs/research/389-cost-tiering.md. The 1,000 GB root volume is the size
# the scale harness documents for this VM.
COST_ASSUMPTIONS = {
    "instance_type": "m7i.8xlarge",
    "vcpus": 32,
    "memory_gib": 128,
    "region": "us-east-1",
    "purchase": "linux_on_demand",
    "hourly_usd": 1.6128,
    "hourly_note": (
        "Public on-demand list price for m7i.8xlarge, Linux, us-east-1, "
        "as published on 2026-11-28 ($1.6128/hour). Not an invoice."
    ),
    "root_volume_gb": 1000,
    "root_volume_note": (
        "The scale harness documents a 1,000 GB root volume on the bench VM."
    ),
    "gp3_usd_per_gb_month": 0.08,
    "gp3_note": (
        "gp3 list price cited in docs/research/389-cost-tiering.md. "
        "1,000 GB at that rate is $80/month, list price only. "
        "Index bytes times this rate is the on-disk holding cost once a "
        "run has measured the store size; this module does not invent that size."
    ),
}

ENGINES = {
    "opensearch": {
        "image": "opensearchproject/opensearch:3.10.1",
        "version": "3.10.1",
        "released": "2026-11-24",
        "heap_env": "OPENSEARCH_JAVA_OPTS",
        "data_path": "/usr/share/opensearch/data",
        "license": "Apache-2.0",
        # OpenSearch 3 defaults this to `auto`, which does not parallelize a
        # plain match query. `all` is the explicit "every request" mode.
        # With one segment per shard it has nothing to slice inside a shard;
        # the shard count is the parallelism. Recorded so a re-run that
        # raises max_num_segments does not silently stay on `auto`.
        "concurrent_segment_search": {
            "persistent": {"search.concurrent_segment_search.mode": "all"}
        },
    },
    "elasticsearch": {
        "image": "docker.elastic.co/elasticsearch/elasticsearch:9.5.7",
        "version": "9.5.7",
        "released": "2026-11-23",
        "heap_env": "ES_JAVA_OPTS",
        "data_path": "/usr/share/elasticsearch/data",
        "license": "Elastic License 2.0 (pulled at runtime, not vendored)",
        "concurrent_segment_search": {
            "persistent": {"search.concurrent_segment_search.enabled": True}
        },
    },
}


def to_bulk_ndjson(lines: list[str], index: str) -> tuple[str, int]:
    parts: list[str] = []
    n = 0
    for line in lines:
        rec = json.loads(line)
        parts.append(json.dumps({"index": {"_index": index, "_id": str(rec["id"])}}))
        parts.append(json.dumps({"text": rec["text"]}))
        n += 1
    return "\n".join(parts) + "\n", n


def index_body(shards: int = DEFAULT_SHARDS, replicas: int = DEFAULT_REPLICAS) -> dict:
    """Single-node index. Replicas stay 0: one node cannot assign a replica,
    and the August 1-replica figure was a 2-node domain. refresh_interval -1
    for the life of the index (static corpus). Translog durability and the
    codec are left at the engine defaults; this body does not set them."""
    if shards < 1:
        raise ValueError(f"shards must be >= 1, got {shards}")
    if replicas != 0:
        raise ValueError(
            "this harness is single-node; replicas must be 0 "
            f"(a replica cannot be assigned), got {replicas}"
        )
    return {
        "settings": {
            "number_of_shards": shards,
            "number_of_replicas": replicas,
            "refresh_interval": "-1",
            "analysis": {
                "filter": {
                    "strip_edge_punct": {
                        "type": "pattern_replace",
                        "pattern": EDGE_PUNCT_PATTERN,
                        "replacement": "",
                    },
                    "drop_empty": {"type": "length", "min": 1},
                },
                "analyzer": {
                    ANALYZER_NAME: {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": ["lowercase", "strip_edge_punct", "drop_empty"],
                    }
                },
            },
            "similarity": {
                "default": {"type": "BM25", "k1": BM25_K1, "b": BM25_B}
            },
        },
        "mappings": {
            "properties": {
                "text": {"type": "text", "analyzer": ANALYZER_NAME}
            }
        },
    }


def cache_index_settings(mode: str) -> dict:
    """Flat index settings for one measured phase.

    ``off`` is the Kosha ``no_cache`` analogue: request cache and query
    cache both disabled. ``defaults`` puts both back to the engine default
    (enabled). On a ``size > 0`` search the default request cache still does
    not store the hit list; the phase records cache hit/miss deltas so the
    run shows whether either cache engaged.
    """
    if mode == "off":
        enabled = False
    elif mode == "defaults":
        enabled = True
    else:
        raise ValueError(f"cache mode must be 'off' or 'defaults', got {mode!r}")
    return {
        "index.requests.cache.enable": enabled,
        "index.queries.cache.enabled": enabled,
    }


def search_params(mode: str) -> dict:
    """URL parameters. ``off`` forces ``request_cache=false``. ``defaults``
    omits the parameter, which is the engine default (size=10 is not cached)."""
    if mode == "off":
        return {"request_cache": "false"}
    if mode == "defaults":
        return {}
    raise ValueError(f"cache mode must be 'off' or 'defaults', got {mode!r}")


def search_body(text: str, topk: int, return_fields: str) -> dict:
    """OR match over the single text field. ``ids`` asks for no stored
    source, matching Kosha ``return_fields=ids`` (doc id + score).
    ``source`` returns the stored passage, matching Kosha ``return_fields=all``
    on this text-only index."""
    if return_fields not in ("ids", "source"):
        raise ValueError(f"return_fields must be ids or source, got {return_fields!r}")
    return {
        "size": topk,
        "track_total_hits": TRACK_TOTAL_HITS,
        "_source": return_fields == "source",
        "query": {"match": {"text": {"query": text, "operator": "or"}}},
    }


def docker_argv(engine: str, data_dir: str) -> list[str]:
    """``docker run`` argv for one single-node container. Heap is 31g, the
    compressed-ordinary-object-pointer ceiling, not half of the 128 GiB box.
    The rest of RAM is the OS page cache, which is Lucene's file cache."""
    spec = ENGINES[engine]
    env = [
        "discovery.type=single-node",
        "bootstrap.memory_lock=true",
        # A kept VM can already hold the Kosha cache on the root volume.
        # The watermark must not mark this index read-only because of that.
        "cluster.routing.allocation.disk.threshold_enabled=false",
        "ingest.geoip.downloader.enabled=false",
        f"{spec['heap_env']}=-Xms{HEAP} -Xmx{HEAP}",
    ]
    if engine == "opensearch":
        env += ["DISABLE_SECURITY_PLUGIN=true", "DISABLE_INSTALL_DEMO_CONFIG=true"]
    else:
        env += [
            "xpack.security.enabled=false",
            "xpack.security.http.ssl.enabled=false",
            "xpack.security.transport.ssl.enabled=false",
            "xpack.ml.enabled=false",
        ]
    argv = [
        "sudo", "docker", "run", "-d",
        "--name", engine,
        "--network", "host",
        "--ulimit", "memlock=-1:-1",
        "--ulimit", "nofile=65536:65536",
    ]
    for item in env:
        argv += ["-e", item]
    argv += ["-v", f"{data_dir}:{spec['data_path']}", spec["image"]]
    return argv


def format_load_reports(root: Path) -> str:
    """Markdown for every ``load.json`` under a results directory. Empty
    string when there are none. List prices stay labeled as list prices."""
    if not root.is_dir():
        return ""
    lines: list[str] = []
    for path in sorted(root.rglob("load.json")):
        try:
            d = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(d, dict) or d.get("engine") not in ENGINES:
            continue
        rel = path.relative_to(root).as_posix()
        store = d.get("store_size_bytes")
        store_s = f"{store:,} bytes" if isinstance(store, int) else "not recorded"
        disk = d.get("data_dir_bytes")
        disk_s = f"{disk:,} bytes" if isinstance(disk, int) else "not recorded"
        lines += [
            f"### Load {rel}",
            "",
            f"- engine: {d.get('engine')} {d.get('version_pinned')} "
            f"(image `{d.get('image')}`)",
            f"- node reported: {d.get('node_version') or 'not recorded'}, "
            f"nodes: {d.get('node_count')}",
            f"- heap: {d.get('heap')}, shards: {d.get('shards')}, "
            f"replicas: {d.get('replicas')}, "
            f"force-merge max segments/shard: {d.get('max_num_segments')}",
            f"- refresh_interval: {d.get('refresh_interval')} "
            "(off during load and during the measured window; one explicit refresh before force-merge)",
            f"- docs sent: {d.get('docs_sent')}, index count: {d.get('docs_count')}",
            f"- bulk: {d.get('bulk_s')} s, refresh: {d.get('refresh_s')} s, "
            f"force-merge: {d.get('forcemerge_s')} s",
            f"- store size (primaries): {store_s}",
            f"- data directory: {disk_s}",
            f"- segments: {d.get('segments_count')}",
            f"- concurrent segment search put: {d.get('concurrent_segment_search')}",
            "",
        ]
    if not lines:
        return ""
    return "## Lucene load\n\n" + "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "docker-cmd":
        if len(argv) != 4:
            print("usage: lucene_compare.py docker-cmd <engine> <data-dir>", file=sys.stderr)
            return 2
        quoted = " ".join(shlex.quote(a) for a in docker_argv(argv[2], argv[3]))
        sys.stdout.write(f"docker_cmd=({quoted})\n")
        return 0
    if len(argv) >= 2 and argv[1] == "report-load":
        root = Path(argv[2] if len(argv) > 2 else "results")
        sys.stdout.write(format_load_reports(root))
        return 0
    print(
        "usage: lucene_compare.py docker-cmd <engine> <data-dir> | report-load [results-dir]",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
