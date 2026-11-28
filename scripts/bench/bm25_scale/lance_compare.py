#!/usr/bin/env python3
"""Shared settings for the Lance-over-Iceberg BM25 scale run.

Lance is the table and index format. An Iceberg REST catalog is the
catalog: the table is declared through lance-namespace's Iceberg REST
implementation, and both the loader and the query client resolve the
table's S3 location through that catalog (``lance.dataset(namespace_client=,
table_id=)``), not from a hard-coded URI. Iceberg's spec only allows
Parquet/ORC/Avro data files, so the catalog entry is a Lance table
(``table_type=lance``) whose data and full-text index are Lance files.

Data lives on S3 under LANCE_ROOT, which the workflow sets to the bench
stack's own bucket: the same storage Kosha reads in this harness, destroyed
with the VM. Nothing is
on local disk except Lance's in-process caches.

Pinned versions (current releases as of 2026-11-28):

* pylance 14.0.3 (Apache-2.0)
* lance-namespace 0.13.0 and lance-namespace-impls 0.4.2 (Apache-2.0)
* apache/iceberg-rest-fixture:1.10.1 (Apache-2.0), the Iceberg project's
  reference REST catalog, JDBC/SQLite in memory for the life of the run

Nothing here is a measured latency.
"""

from __future__ import annotations

import json
import shlex
import sys
from pathlib import Path

PYLANCE_VERSION = "14.0.3"
LANCE_NAMESPACE_VERSION = "0.13.0"
LANCE_NAMESPACE_IMPLS_VERSION = "0.4.2"
PIP_REQUIREMENTS = [
    f"pylance=={PYLANCE_VERSION}",
    f"lance-namespace=={LANCE_NAMESPACE_VERSION}",
    f"lance-namespace-impls=={LANCE_NAMESPACE_IMPLS_VERSION}",
    "pyarrow",
    "numpy",
]

CATALOG_IMAGE = "apache/iceberg-rest-fixture:1.10.1"
CATALOG_CONTAINER = "iceberg-rest"
CATALOG_PORT = 8181
CATALOG_ENDPOINT = f"http://127.0.0.1:{CATALOG_PORT}"

# Table id for lance-namespace's Iceberg impl: [warehouse, namespace..., table].
WAREHOUSE = "bench"
NAMESPACE = "kosha_bench"
TABLE = "msmarco_10m"
TABLE_ID = [WAREHOUSE, NAMESPACE, TABLE]

# The S3 root (LANCE_ROOT, s3://bucket/prefix) and the corpus location come
# from the workflow; no bucket name is hard-coded here. The workflow points
# the root at the stack's segment bucket, the same S3 storage Kosha uses on
# this VM, torn down with the stack.
CORPUS_NAME = "msmarco-10m"

TEXT_COLUMN = "text"
# Indexed column: the passage with Kosha's edge-punctuation trim already
# applied. Lance's "whitespace" tokenizer does not trim punctuation and
# "simple" also splits inside a token, so neither alone matches Kosha's
# `standard` analyzer. Trimming before indexing (and the same trim on the
# query) gives the token stream Kosha and the Lucene runs see.
INDEX_COLUMN = "text_norm"

# INVERTED index options. Lance's BM25 uses k1=1.2, b=0.75 (not
# configurable), the same as Kosha and lucene_compare.
INDEX_PARAMS = {
    "base_tokenizer": "whitespace",
    "lower_case": True,
    "stem": False,
    "remove_stop_words": False,
    "ascii_folding": False,
    "max_token_length": None,
    "with_position": False,
}

# Lance's in-process index cache plays the role Lucene's OS page cache
# plays: the hot part of the index in RAM. 64 GiB leaves the rest of the
# 128 GiB box for the process and the OS. Recorded in every phase JSON.
DEFAULT_INDEX_CACHE_BYTES = 64 * 1024**3
DEFAULT_METADATA_CACHE_BYTES = 4 * 1024**3


def _is_ascii_punct(ch: str) -> bool:
    # Rust's char::is_ascii_punctuation.
    o = ord(ch)
    return 0x21 <= o <= 0x2F or 0x3A <= o <= 0x40 or 0x5B <= o <= 0x60 or 0x7B <= o <= 0x7E


def normalize(text: str) -> str:
    """Kosha ``standard@1`` words (whitespace split, ASCII punctuation
    trimmed from each word's edges, empty words dropped), re-joined with a
    single space. Lowercasing is left to the index (``lower_case=True``)."""
    words = []
    for word in text.split():
        start, end = 0, len(word)
        while start < end and _is_ascii_punct(word[start]):
            start += 1
        while end > start and _is_ascii_punct(word[end - 1]):
            end -= 1
        if start < end:
            words.append(word[start:end])
    return " ".join(words)


def namespace_properties(root: str) -> dict:
    return {"endpoint": CATALOG_ENDPOINT, "root": root.rstrip("/")}


def table_location(root: str) -> str:
    """Where the Iceberg impl puts the table when declare_table has no
    location: ``<root>/<warehouse>/<namespace>/<table>``."""
    return f"{root.rstrip('/')}/{'/'.join(TABLE_ID)}"


def projected_columns(return_fields: str) -> list[str]:
    if return_fields == "ids":
        return ["id", "_score"]
    if return_fields == "source":
        return ["id", TEXT_COLUMN, "_score"]
    raise ValueError(f"return_fields must be ids or source, got {return_fields!r}")


def catalog_docker_argv(region: str, root: str) -> list[str]:
    """``docker run`` argv for the Iceberg REST catalog. It writes Iceberg
    metadata for the declared table under the table location, so it gets
    S3FileIO and the instance profile (host network reaches IMDS)."""
    env = [
        "CATALOG_WAREHOUSE=" + root.rstrip("/") + "/warehouse",
        "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
        f"AWS_REGION={region}",
        f"CATALOG_CLIENT_REGION={region}",
    ]
    argv = [
        "sudo", "docker", "run", "-d",
        "--name", CATALOG_CONTAINER,
        "--network", "host",
    ]
    for item in env:
        argv += ["-e", item]
    argv.append(CATALOG_IMAGE)
    return argv


def format_load_reports(root: Path) -> str:
    """Markdown for every Lance ``load.json`` under a results directory."""
    if not root.is_dir():
        return ""
    lines: list[str] = []
    for path in sorted(root.rglob("load.json")):
        try:
            d = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(d, dict) or d.get("engine") != "lance":
            continue
        rel = path.relative_to(root).as_posix()
        size = d.get("s3_bytes")
        size_s = f"{size:,} bytes" if isinstance(size, int) else "not recorded"
        lines += [
            f"### Load {rel}",
            "",
            f"- engine: lance (pylance {d.get('pylance_version')}), catalog: "
            f"Iceberg REST `{d.get('catalog_image')}`",
            f"- table: `{'.'.join(d.get('table_id') or [])}` at `{d.get('location')}`",
            f"- rows written: {d.get('rows_written')}, dataset rows: {d.get('rows_count')}, "
            f"fragments: {d.get('fragments')}",
            f"- write: {d.get('write_s')} s, FTS index build: {d.get('index_s')} s",
            f"- index params: {json.dumps(d.get('index_params'))}",
            f"- S3 size (data + index + Iceberg metadata): {size_s}",
            "",
        ]
    if not lines:
        return ""
    return "## Lance load\n\n" + "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "catalog-cmd":
        if len(argv) != 4:
            print("usage: lance_compare.py catalog-cmd <region> <root>", file=sys.stderr)
            return 2
        quoted = " ".join(shlex.quote(a) for a in catalog_docker_argv(argv[2], argv[3]))
        sys.stdout.write(f"catalog_cmd=({quoted})\n")
        return 0
    if len(argv) >= 2 and argv[1] == "pip-requirements":
        sys.stdout.write(" ".join(PIP_REQUIREMENTS) + "\n")
        return 0
    if len(argv) >= 2 and argv[1] == "report-load":
        root = Path(argv[2] if len(argv) > 2 else "results")
        sys.stdout.write(format_load_reports(root))
        return 0
    print(
        "usage: lance_compare.py catalog-cmd <region> <root> | pip-requirements "
        "| report-load [results-dir]",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
